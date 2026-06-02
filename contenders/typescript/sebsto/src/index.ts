// TypeScript contender Lambda for demo-s3-archiving.
//
// Three-stage pipeline (downloader → zipper → uploader) connected by
// in-memory async channels with explicit backpressure:
//
//   * Downloader fans out parallel `GetObjectCommand` calls, gated by a
//     byte-budget semaphore. Bytes stream directly into a pre-sized
//     Buffer; CRC32 is computed in the same task (parallelism per file).
//   * Zipper consumes downloads in arrival order, emits a STORED ZIP64
//     stream (LFH + body + DataDescriptor per entry, then central
//     directory + ZIP64 EOCD). It pushes bytes to a ChunkProducer that
//     buckets into fixed-size multipart chunks.
//   * Uploader consumes sealed chunks and issues UploadPart calls with
//     a fixed concurrency cap. Returns the CompletedPart array used to
//     finalize the multipart upload.
//
// All state lives in a single Buffer per chunk (10 MiB). No Data /
// Uint8Array conversions are made on the hot path: file bodies and ZIP
// header records are appended via `Buffer.copy` straight into the
// active chunk Buffer.

import { Readable } from "node:stream";
import { crc32 } from "node:zlib";
import { Agent as HttpsAgent } from "node:https";
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  type CompletedPart,
} from "@aws-sdk/client-s3";
import { NodeHttpHandler } from "@smithy/node-http-handler";

// ---------- Tunables ----------

// Total bytes of in-flight downloads. With files averaging ~5 MiB this
// admits ~12 simultaneous downloads. The S3 socket pool is sized to
// match (see `MAX_SOCKETS`) so no download queues behind socket
// allocation.
const MAX_DOWNLOADS_MEMORY = 60 * 1024 * 1024;
// Cap on simultaneous UploadPart calls in flight. S3's per-prefix limit
// is much higher; the cap is purely a memory ceiling for the upload
// path (`MAX_CONCURRENT_UPLOADS × CHUNK_SIZE_BYTES` worth of buffers).
const MAX_CONCURRENT_UPLOADS = 4;
// Multipart-upload part size. S3 allows 5 MiB–5 GiB per part. Smaller
// parts → more requests; larger parts → more memory + a longer tail
// for the final part to flush.
const CHUNK_SIZE_BYTES = 10 * 1024 * 1024;
// In-flight chunk slots in the producer→uploader path (excluding parts
// currently being sent). Caps producer-side memory at
// `BUFFER_CHUNKS_COUNT × CHUNK_SIZE_BYTES`.
const BUFFER_CHUNKS_COUNT = 2;
// Per-host HTTPS connection pool. Sized for the combined download +
// upload concurrency; smaller pools serialize requests behind socket
// allocation and erase the pipeline's parallelism.
const MAX_SOCKETS = 64;

// ---------- Module-level S3 client (cold start once) ----------

// Singleton `https.Agent` with keep-alive so socket setup (TCP + TLS)
// is paid once across all download GETs and upload PUTs to the same
// S3 host.
const httpsAgent = new HttpsAgent({
  keepAlive: true,
  maxSockets: MAX_SOCKETS,
  maxFreeSockets: MAX_SOCKETS,
  scheduling: "lifo",
});

const s3 = new S3Client({
  requestHandler: new NodeHttpHandler({
    httpsAgent,
    connectionTimeout: 5_000,
    socketTimeout: 120_000,
  }),
  // SDK adaptive retries: S3 keep-alive sockets are routinely closed
  // on the server side after ~20 s idle. Reusing a just-closed socket
  // surfaces as `TimeoutError: socket hang up`. The SDK retries those
  // transparently — disabling retries (`maxAttempts: 1`) crashes the
  // Lambda on the first lost socket of a 10-minute run.
  maxAttempts: 5,
  retryMode: "adaptive",
});

// ---------- Types ----------

interface JobInfo {
  bucket_name: string;
  files_prefix: string;
  archive_key: string;
}

interface FileInfo {
  name: string;
  key: string;
  size: number;
}

interface DownloadedFile {
  name: string;
  body: Buffer;
  crc32: number;
  releaseBytes: number;
}

interface ZipEntry {
  nameBytes: Buffer;
  crc32: number;
  size: number; // < 2 GiB per file in practice; full ZIP64 still emitted
  localHeaderOffset: bigint;
}

interface UploadChunk {
  partNumber: number;
  data: Buffer;
}

// ---------- Async primitives ----------

// Counting semaphore over a byte budget. Throttles total bytes of
// in-flight downloads. `acquire` resolves immediately when capacity is
// available, otherwise queues a waiter that wakes in FIFO order. A
// single file larger than `capacity` is admitted alone (it would
// otherwise deadlock).
class ByteSemaphore {
  private available: number;
  private waiters: Array<{ needed: number; resolve: () => void }> = [];
  constructor(private capacity: number) {
    this.available = capacity;
  }
  async acquire(amount: number): Promise<void> {
    const needed = Math.min(amount, this.capacity);
    if (this.available >= needed) {
      this.available -= needed;
      return;
    }
    return new Promise<void>((resolve) => {
      this.waiters.push({ needed, resolve });
    }).then(() => {
      this.available -= needed;
    });
  }
  release(amount: number): void {
    this.available += Math.min(amount, this.capacity);
    while (this.waiters.length > 0 && this.available >= this.waiters[0]!.needed) {
      this.waiters.shift()!.resolve();
    }
  }
}

// Single-producer / single-consumer FIFO with async receive. The
// downloader's byte-budget semaphore already bounds memory upstream;
// this channel is just an arrival-order queue.
class FileChannel {
  private queue: DownloadedFile[] = [];
  private waiter: ((value: DownloadedFile | null) => void) | null = null;
  private closed = false;
  send(item: DownloadedFile): void {
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w(item);
    } else {
      this.queue.push(item);
    }
  }
  finish(): void {
    this.closed = true;
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w(null);
    }
  }
  recv(): Promise<DownloadedFile | null> {
    if (this.queue.length > 0) return Promise.resolve(this.queue.shift()!);
    if (this.closed) return Promise.resolve(null);
    return new Promise((resolve) => {
      this.waiter = resolve;
    });
  }
}

// ---------- Chunk producer ----------

// Buckets writes into fixed-size `Buffer` chunks and emits them on a
// channel for the uploader. Decouples the synchronous ZIP writer from
// async upload calls, with an in-flight-chunks ceiling that
// backpressures the zipper when the uploader can't keep up.
//
// Memory model: at most `maxInFlight + 1` chunks of `chunkSize` bytes
// are alive at any time (the +1 is the chunk currently being filled).
class ChunkProducer {
  private buffer: Buffer;
  private cursor = 0;
  private nextPartNumber = 1;
  private inFlight = 0;
  private slotWaiters: Array<() => void> = [];
  private chunkQueue: UploadChunk[] = [];
  private chunkWaiter: ((value: UploadChunk | null) => void) | null = null;
  private closed = false;

  constructor(
    private chunkSize: number,
    private maxInFlight: number,
  ) {
    this.buffer = Buffer.allocUnsafe(chunkSize);
  }

  // Append `src` (Buffer) bytes to the current chunk, sealing and
  // emitting full chunks as they fill. Backpressure happens inside
  // `emitFullChunk` (waits for an in-flight slot).
  async append(src: Buffer): Promise<void> {
    let offset = 0;
    const total = src.length;
    while (offset < total) {
      const room = this.chunkSize - this.cursor;
      const take = Math.min(total - offset, room);
      src.copy(this.buffer, this.cursor, offset, offset + take);
      this.cursor += take;
      offset += take;
      if (this.cursor === this.chunkSize) {
        await this.emitFullChunk();
      }
    }
  }

  // Append three byte ranges (LFH + body + data descriptor) for one
  // ZIP entry in order. Just three sequential `append` calls.
  async appendCompound(lfh: Buffer, body: Buffer, dd: Buffer): Promise<void> {
    await this.append(lfh);
    await this.append(body);
    await this.append(dd);
  }

  // Mark the producer as done. Emits any remaining partial chunk and
  // closes the chunk channel so the uploader's `recv` loop terminates.
  async finish(): Promise<void> {
    if (this.cursor > 0) {
      await this.emitFullChunk();
    }
    this.closed = true;
    if (this.chunkWaiter) {
      const w = this.chunkWaiter;
      this.chunkWaiter = null;
      w(null);
    }
  }

  // Called by the uploader after a chunk has been put. Frees an
  // in-flight slot so the producer can build the next chunk.
  releaseSlot(): void {
    if (this.inFlight > 0) this.inFlight -= 1;
    if (this.slotWaiters.length > 0) this.slotWaiters.shift()!();
  }

  recv(): Promise<UploadChunk | null> {
    if (this.chunkQueue.length > 0) return Promise.resolve(this.chunkQueue.shift()!);
    if (this.closed) return Promise.resolve(null);
    return new Promise((resolve) => {
      this.chunkWaiter = resolve;
    });
  }

  private async emitFullChunk(): Promise<void> {
    await this.waitForSlot();
    // Slice (zero-copy view) of just the filled portion. The next
    // chunk's Buffer is freshly allocated so the uploader can hold its
    // chunk independently.
    const data = this.buffer.subarray(0, this.cursor);
    const partNumber = this.nextPartNumber++;
    this.inFlight += 1;
    this.buffer = Buffer.allocUnsafe(this.chunkSize);
    this.cursor = 0;
    const chunk: UploadChunk = { partNumber, data };
    if (this.chunkWaiter) {
      const w = this.chunkWaiter;
      this.chunkWaiter = null;
      w(chunk);
    } else {
      this.chunkQueue.push(chunk);
    }
  }

  private async waitForSlot(): Promise<void> {
    if (this.inFlight < this.maxInFlight) return;
    return new Promise((resolve) => {
      this.slotWaiters.push(resolve);
    });
  }
}

// ---------- ZIP header builders ----------

// All records are little-endian. STORED method (no compression). GP
// flag bit 3 is set so CRC + sizes are written in a data descriptor
// *after* the body (we don't know the CRC until streaming is done).
// ZIP64 records are emitted unconditionally to handle archives > 4 GiB
// and to keep the header layout deterministic.

const ZIP_VERSION_NEEDED = 45;
const ZIP_VERSION_MADE_BY = (3 << 8) | 45; // host = unix
const ZIP_GP_FLAG = 0x0008;
const ZIP_METHOD_STORED = 0;
// Fixed mtime (2010-01-01 00:00:00) so two runs over the same input
// produce byte-identical archives.
const ZIP_DOS_TIME = 0;
const ZIP_DOS_DATE = (30 << 9) | (1 << 5) | 1;

function localFileHeader(nameBytes: Buffer): Buffer {
  const buf = Buffer.allocUnsafe(30 + nameBytes.length);
  buf.writeUInt32LE(0x04034b50, 0);
  buf.writeUInt16LE(ZIP_VERSION_NEEDED, 4);
  buf.writeUInt16LE(ZIP_GP_FLAG, 6);
  buf.writeUInt16LE(ZIP_METHOD_STORED, 8);
  buf.writeUInt16LE(ZIP_DOS_TIME, 10);
  buf.writeUInt16LE(ZIP_DOS_DATE, 12);
  buf.writeUInt32LE(0, 14); // crc32 (data descriptor)
  buf.writeUInt32LE(0, 18); // compressed size (data descriptor)
  buf.writeUInt32LE(0, 22); // uncompressed size (data descriptor)
  buf.writeUInt16LE(nameBytes.length, 26);
  buf.writeUInt16LE(0, 28); // extra field length
  nameBytes.copy(buf, 30);
  return buf;
}

// 24 bytes including signature. Sizes are 8-byte LE because GP flag
// bit 3 + ZIP64 implies 64-bit data descriptor sizes.
function dataDescriptor(crc: number, size: number): Buffer {
  const buf = Buffer.allocUnsafe(24);
  buf.writeUInt32LE(0x08074b50, 0);
  buf.writeUInt32LE(crc >>> 0, 4);
  buf.writeBigUInt64LE(BigInt(size), 8);
  buf.writeBigUInt64LE(BigInt(size), 16);
  return buf;
}

// Central directory header for one entry, with mandatory ZIP64 extra
// field (uncompressed size, compressed size, LFH offset — total 24
// bytes after the 4-byte tag/size header).
function centralDirectoryHeader(entry: ZipEntry): Buffer {
  const nameLen = entry.nameBytes.length;
  const extraLen = 28; // tag(2)+size(2)+uncomp(8)+comp(8)+offset(8)
  const buf = Buffer.allocUnsafe(46 + nameLen + extraLen);
  buf.writeUInt32LE(0x02014b50, 0);
  buf.writeUInt16LE(ZIP_VERSION_MADE_BY, 4);
  buf.writeUInt16LE(ZIP_VERSION_NEEDED, 6);
  buf.writeUInt16LE(ZIP_GP_FLAG, 8);
  buf.writeUInt16LE(ZIP_METHOD_STORED, 10);
  buf.writeUInt16LE(ZIP_DOS_TIME, 12);
  buf.writeUInt16LE(ZIP_DOS_DATE, 14);
  buf.writeUInt32LE(entry.crc32 >>> 0, 16);
  buf.writeUInt32LE(0xffffffff, 20); // compressed size → ZIP64 extra
  buf.writeUInt32LE(0xffffffff, 24); // uncompressed size → ZIP64 extra
  buf.writeUInt16LE(nameLen, 28);
  buf.writeUInt16LE(extraLen, 30);
  buf.writeUInt16LE(0, 32); // file comment length
  buf.writeUInt16LE(0, 34); // disk number start
  buf.writeUInt16LE(0, 36); // internal file attrs
  buf.writeUInt32LE(0o644 << 16, 38); // external file attrs (unix 0644)
  buf.writeUInt32LE(0xffffffff, 42); // local header offset → ZIP64 extra
  entry.nameBytes.copy(buf, 46);

  // ZIP64 extended information extra field — order: uncompressed,
  // compressed, offset (matches the order the LFH/CD masks declared).
  let p = 46 + nameLen;
  buf.writeUInt16LE(0x0001, p); p += 2;
  buf.writeUInt16LE(24, p); p += 2;
  buf.writeBigUInt64LE(BigInt(entry.size), p); p += 8;
  buf.writeBigUInt64LE(BigInt(entry.size), p); p += 8;
  buf.writeBigUInt64LE(entry.localHeaderOffset, p);
  return buf;
}

function zip64EndOfCentralDirectory(
  entryCount: bigint,
  cdSize: bigint,
  cdOffset: bigint,
): Buffer {
  const buf = Buffer.allocUnsafe(56);
  buf.writeUInt32LE(0x06064b50, 0);
  buf.writeBigUInt64LE(BigInt(44), 4); // size of this record - 12
  buf.writeUInt16LE(ZIP_VERSION_MADE_BY, 12);
  buf.writeUInt16LE(ZIP_VERSION_NEEDED, 14);
  buf.writeUInt32LE(0, 16); // disk number
  buf.writeUInt32LE(0, 20); // disk with central directory
  buf.writeBigUInt64LE(entryCount, 24); // entries on this disk
  buf.writeBigUInt64LE(entryCount, 32); // total entries
  buf.writeBigUInt64LE(cdSize, 40);
  buf.writeBigUInt64LE(cdOffset, 48);
  return buf;
}

function zip64EndOfCentralDirectoryLocator(zip64EocdOffset: bigint): Buffer {
  const buf = Buffer.allocUnsafe(20);
  buf.writeUInt32LE(0x07064b50, 0);
  buf.writeUInt32LE(0, 4); // disk with ZIP64 EOCD
  buf.writeBigUInt64LE(zip64EocdOffset, 8);
  buf.writeUInt32LE(1, 16); // total disks
  return buf;
}

function endOfCentralDirectory(): Buffer {
  const buf = Buffer.allocUnsafe(22);
  buf.writeUInt32LE(0x06054b50, 0);
  buf.writeUInt16LE(0, 4); // disk number
  buf.writeUInt16LE(0, 6); // disk with central directory
  buf.writeUInt16LE(0xffff, 8); // entries on this disk → ZIP64
  buf.writeUInt16LE(0xffff, 10); // total entries → ZIP64
  buf.writeUInt32LE(0xffffffff, 12); // central directory size → ZIP64
  buf.writeUInt32LE(0xffffffff, 16); // central directory offset → ZIP64
  buf.writeUInt16LE(0, 20); // comment length
  return buf;
}

// ---------- S3 helpers ----------

async function listFiles(bucket: string, filesPrefix: string): Promise<FileInfo[]> {
  const prefix = filesPrefix + "/";
  const out: FileInfo[] = [];
  let token: string | undefined;
  do {
    const page = await s3.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: token,
      }),
    );
    for (const obj of page.Contents ?? []) {
      const key = obj.Key;
      const size = obj.Size;
      if (!key || size == null) continue;
      if (key === prefix) continue;
      if (!key.startsWith(prefix)) continue;
      const name = key.slice(prefix.length);
      if (!name) continue;
      out.push({ name, key, size });
    }
    token = page.IsTruncated ? page.NextContinuationToken : undefined;
  } while (token);
  return out;
}

// Stream the GetObject body into a single pre-allocated Buffer of
// exactly `expectedSize`. The Body chunks are appended via
// `Buffer.copy`, avoiding the doubling-growth reallocations the
// `node:stream` `consumers.buffer()` helper does internally. CRC32 is
// computed once over the full buffer at EOF.
async function downloadFile(
  bucket: string,
  key: string,
  expectedSize: number,
): Promise<{ body: Buffer; crc: number }> {
  const response = await s3.send(
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
  const stream = response.Body as Readable;
  const out = Buffer.allocUnsafe(expectedSize);
  let cursor = 0;
  for await (const chunk of stream) {
    const c = chunk as Buffer;
    c.copy(out, cursor);
    cursor += c.length;
  }
  if (cursor !== expectedSize) {
    throw new Error(
      `short read on ${key}: expected ${expectedSize}, got ${cursor}`,
    );
  }
  // Native CRC32 from `node:zlib` (added in Node 22.2). Significantly
  // faster than any pure-JS implementation; processes the buffer in
  // one C++ call without crossing the JS/native boundary per byte.
  const crc = crc32(out);
  return { body: out, crc };
}

// ---------- Pipeline stages ----------

async function runDownloadStage(
  bucket: string,
  files: FileInfo[],
  byteBudget: ByteSemaphore,
  out: FileChannel,
): Promise<void> {
  // Spawn one promise per file. The byte-budget semaphore is acquired
  // before the GET so the for-loop itself applies file-count
  // backpressure: at most ~MAX_DOWNLOADS_MEMORY worth of files are
  // concurrently live.
  const inflight: Promise<void>[] = [];
  try {
    for (const file of files) {
      await byteBudget.acquire(file.size);
      const p = (async () => {
        const { body, crc } = await downloadFile(bucket, file.key, file.size);
        out.send({ name: file.name, body, crc32: crc, releaseBytes: file.size });
      })();
      inflight.push(p);
    }
    await Promise.all(inflight);
  } finally {
    // Always close the channel — otherwise the zipper would block
    // forever on `recv()`, leaking the orchestrator's other tasks
    // after a download rejection.
    out.finish();
  }
}

async function runZipStage(
  files: FileInfo[],
  fileChannel: FileChannel,
  producer: ChunkProducer,
  byteBudget: ByteSemaphore,
): Promise<void> {
  const entries: ZipEntry[] = [];
  let offset = 0n;

  while (true) {
    const file = await fileChannel.recv();
    if (file === null) break;

    const nameBytes = Buffer.from(file.name, "utf8");
    const lfh = localFileHeader(nameBytes);
    const body = file.body;
    const dd = dataDescriptor(file.crc32, body.length);
    const lfhOffset = offset;

    await producer.appendCompound(lfh, body, dd);
    offset += BigInt(lfh.length + body.length + dd.length);
    byteBudget.release(file.releaseBytes);

    entries.push({
      nameBytes,
      crc32: file.crc32,
      size: body.length,
      localHeaderOffset: lfhOffset,
    });
  }

  const cdOffset = offset;
  let cdSize = 0n;
  for (const e of entries) {
    const h = centralDirectoryHeader(e);
    await producer.append(h);
    cdSize += BigInt(h.length);
  }
  offset += cdSize;

  const z64 = zip64EndOfCentralDirectory(BigInt(entries.length), cdSize, cdOffset);
  const z64Off = offset;
  await producer.append(z64);
  offset += BigInt(z64.length);

  await producer.append(zip64EndOfCentralDirectoryLocator(z64Off));
  await producer.append(endOfCentralDirectory());

  await producer.finish();
  void files; // silence unused under noUnusedParameters
}

async function runUploadStage(
  bucket: string,
  key: string,
  uploadId: string,
  producer: ChunkProducer,
): Promise<CompletedPart[]> {
  const completed: CompletedPart[] = [];
  // Manual concurrency window: at most MAX_CONCURRENT_UPLOADS UploadPart
  // calls in flight. We track them in a Set of promises and `await
  // Promise.race` when full, removing the settled one.
  const inflight = new Set<Promise<void>>();

  while (true) {
    const chunk = await producer.recv();
    if (chunk === null) break;

    if (inflight.size >= MAX_CONCURRENT_UPLOADS) {
      await Promise.race(inflight);
    }

    const p = (async () => {
      const partNumber = chunk.partNumber;
      try {
        const resp = await s3.send(
          new UploadPartCommand({
            Bucket: bucket,
            Key: key,
            UploadId: uploadId,
            PartNumber: partNumber,
            Body: chunk.data,
            ContentLength: chunk.data.length,
          }),
        );
        if (!resp.ETag) throw new Error(`UploadPart ${partNumber}: no ETag`);
        completed.push({ ETag: resp.ETag, PartNumber: partNumber });
      } finally {
        producer.releaseSlot();
      }
    })();
    inflight.add(p);
    // `then(cleanup, cleanup)` (instead of `.finally`) so the rejection
    // path doesn't create a new unhandled promise. The original `p`
    // rejection propagates through the `Promise.race` / `Promise.all`
    // below.
    const cleanup = () => inflight.delete(p);
    p.then(cleanup, cleanup);
  }

  await Promise.all(inflight);
  // S3 requires CompletedMultipartUpload.Parts in ascending PartNumber
  // order; concurrent uploads finish out of order.
  completed.sort((a, b) => (a.PartNumber ?? 0) - (b.PartNumber ?? 0));
  return completed;
}

// ---------- Job orchestration ----------

async function runArchiveJob(job: JobInfo): Promise<void> {
  const files = await listFiles(job.bucket_name, job.files_prefix);

  const create = await s3.send(
    new CreateMultipartUploadCommand({
      Bucket: job.bucket_name,
      Key: job.archive_key,
      ContentType: "application/zip",
    }),
  );
  const uploadId = create.UploadId;
  if (!uploadId) throw new Error("CreateMultipartUpload returned no UploadId");

  const byteBudget = new ByteSemaphore(MAX_DOWNLOADS_MEMORY);
  const fileChannel = new FileChannel();
  const producer = new ChunkProducer(CHUNK_SIZE_BYTES, BUFFER_CHUNKS_COUNT);

  try {
    const dl = runDownloadStage(job.bucket_name, files, byteBudget, fileChannel);
    const zp = runZipStage(files, fileChannel, producer, byteBudget);
    const ul = runUploadStage(job.bucket_name, job.archive_key, uploadId, producer);

    // `Promise.all` rejects on the first stage failure, but the other
    // two stages keep running and may reject later. With no handler
    // attached, those late rejections surface as
    // `Runtime.UnhandledPromiseRejection` and crash the Lambda.
    // `allSettled` waits for every stage to settle, so each rejection
    // gets a consumer; we then re-throw the first failure ourselves.
    const settled = await Promise.allSettled([dl, zp, ul]);
    const firstFailure = settled.find((r) => r.status === "rejected");
    if (firstFailure && firstFailure.status === "rejected") {
      throw firstFailure.reason;
    }
    const parts = (settled[2] as PromiseFulfilledResult<CompletedPart[]>).value;

    await s3.send(
      new CompleteMultipartUploadCommand({
        Bucket: job.bucket_name,
        Key: job.archive_key,
        UploadId: uploadId,
        MultipartUpload: { Parts: parts },
      }),
    );
  } catch (err) {
    // Abort the multipart upload to release any uploaded parts so we
    // don't accrue storage fees on failed runs.
    try {
      await s3.send(
        new AbortMultipartUploadCommand({
          Bucket: job.bucket_name,
          Key: job.archive_key,
          UploadId: uploadId,
        }),
      );
    } catch {
      // ignore — original error wins
    }
    throw err;
  }
}

// ---------- Lambda entry point ----------

export const handler = async (event: JobInfo): Promise<string> => {
  await runArchiveJob(event);
  return "ok";
};
