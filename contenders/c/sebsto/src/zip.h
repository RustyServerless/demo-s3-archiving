#ifndef DSA_ZIP_H
#define DSA_ZIP_H

#include "ring.h"
#include "util.h"

/*
 * Streaming ZIP encoder. Compression method = STORE (0) — source files are
 * already random/incompressible, matches the Rust reference contender.
 *
 * Output is written via ring_write(); each entry is written as a Local File
 * Header followed by the raw data (CRC32 known after read), and the central
 * directory is appended at finish().
 *
 * For files >= 4 GiB the spec mandates ZIP64 — we don't bother since each
 * source object is at most 8 MB, but we use 64-bit central-directory if the
 * total archive size exceeds 4 GiB (it will, ~15 GB total).
 */

typedef struct {
    char    *name;
    uint32_t crc32;
    uint64_t size;             /* == uncompressed == compressed (STORE) */
    uint64_t local_header_off;
} zip_entry_t;

typedef struct {
    ring_t      *out;
    zip_entry_t *entries;
    size_t       n_entries;
    size_t       cap_entries;
    uint64_t     offset;       /* total bytes written so far */
} zip_writer_t;

void zip_writer_init(zip_writer_t *z, ring_t *out);
void zip_writer_free(zip_writer_t *z);

/* Append one stored entry. `data` must be exactly `n` bytes. */
void zip_writer_add(zip_writer_t *z, const char *name, const uint8_t *data, size_t n);

/* Write central directory + EOCD (and ZIP64 if needed). */
void zip_writer_finish(zip_writer_t *z);

#endif
