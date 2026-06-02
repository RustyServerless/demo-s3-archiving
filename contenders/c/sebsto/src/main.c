/*
 * C contender for the demo-s3-archiving benchmark — built on the AWS Common Runtime
 * (aws-c-s3 etc.). Mirrors the Rust reference contender's three-stage pipeline:
 *
 *   download (CRT meta-requests, parallel)
 *      → zipper thread (STORE-only, CRC32 via zlib)
 *      → upload (CRT multipart PUT consuming bytes via aws_s3_meta_request_write)
 *
 * No tracing / OTel — Lambda CloudWatch logs are sufficient for the benchmark.
 */

#include "json.h"
#include "runtime.h"
#include "util.h"
#include "zip.h"

#include <aws/auth/credentials.h>
#include <aws/common/byte_buf.h>
#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/string.h>
#include <aws/common/zero.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/future.h>
#include <aws/io/host_resolver.h>
#include <aws/io/logging.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/io/uri.h>
#include <aws/s3/s3.h>
#include <aws/s3/s3_client.h>

#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---------- Tunables ---------- */

/* Concurrent in-flight downloads. CRT does ranged reads internally per object,
 * but each object is small (~5 MB) so we just do "one meta-request per object,
 * many at a time". */
#define MAX_CONCURRENT_DOWNLOADS 64

/* Memory cap on pending downloaded data waiting for the zipper. */
#define MAX_DOWNLOAD_INFLIGHT_BYTES (32u * 1024 * 1024)

/* CRT throughput hint (Gb/s). The Rust SDK on Lambda configures around 10 Gb/s
 * by default; bumping higher tells CRT to keep more parallel connections to S3. */
#define CRT_THROUGHPUT_GBPS 100.0

/* Multipart part size for the final PUT (CRT splits on this). */
#define CRT_PART_SIZE_BYTES (8u * 1024 * 1024)

/* ---------- Globals (one-time per-cold-start init) ---------- */

static struct aws_allocator           *g_alloc = NULL;
static struct aws_logger               g_logger;
static struct aws_event_loop_group    *g_elg = NULL;
static struct aws_host_resolver       *g_resolver = NULL;
static struct aws_client_bootstrap    *g_bootstrap = NULL;
static struct aws_tls_ctx             *g_tls_ctx = NULL;
static struct aws_credentials_provider *g_creds_provider = NULL;
static struct aws_s3_client           *g_s3 = NULL;
static struct aws_string              *g_region = NULL;
static struct aws_string              *g_endpoint_host = NULL;  /* "<bucket>.s3.<region>.amazonaws.com" */
static char                           *g_bucket = NULL;

static int crt_init(const char *bucket) {
    aws_s3_library_init(g_alloc);

    /* Logger -> stderr (Lambda CloudWatch). */
    struct aws_logger_standard_options lopts = {
        .level = AWS_LL_WARN,
        .file = stderr,
    };
    aws_logger_init_standard(&g_logger, g_alloc, &lopts);
    aws_logger_set(&g_logger);

    g_elg = aws_event_loop_group_new_default(g_alloc, 0, NULL);
    if (!g_elg) return -1;

    struct aws_host_resolver_default_options ropts = {
        .el_group = g_elg,
        .max_entries = 32,
    };
    g_resolver = aws_host_resolver_new_default(g_alloc, &ropts);
    if (!g_resolver) return -1;

    struct aws_client_bootstrap_options bopts = {
        .event_loop_group = g_elg,
        .host_resolver = g_resolver,
    };
    g_bootstrap = aws_client_bootstrap_new(g_alloc, &bopts);
    if (!g_bootstrap) return -1;

    struct aws_tls_ctx_options tls_opts;
    aws_tls_ctx_options_init_default_client(&tls_opts, g_alloc);
    g_tls_ctx = aws_tls_client_ctx_new(g_alloc, &tls_opts);
    aws_tls_ctx_options_clean_up(&tls_opts);
    if (!g_tls_ctx) return -1;

    /* Default credentials chain — picks up Lambda's IAM role from environment. */
    struct aws_credentials_provider_chain_default_options copts = {
        .bootstrap = g_bootstrap,
        .tls_ctx = g_tls_ctx,
    };
    g_creds_provider = aws_credentials_provider_new_chain_default(g_alloc, &copts);
    if (!g_creds_provider) return -1;

    const char *region = getenv("AWS_REGION");
    if (!region) region = getenv("AWS_DEFAULT_REGION");
    if (!region) {
        LOG("AWS_REGION not set");
        return -1;
    }
    g_region = aws_string_new_from_c_str(g_alloc, region);
    g_bucket = xstrdup(bucket);

    char host[512];
    snprintf(host, sizeof(host), "%s.s3.%s.amazonaws.com", bucket, region);
    g_endpoint_host = aws_string_new_from_c_str(g_alloc, host);

    /* Build S3 client. */
    struct aws_signing_config_aws signing = {0};
    signing.config_type = AWS_SIGNING_CONFIG_AWS;
    signing.algorithm = AWS_SIGNING_ALGORITHM_V4;
    signing.signature_type = AWS_ST_HTTP_REQUEST_HEADERS;
    signing.region = aws_byte_cursor_from_string(g_region);
    signing.service = aws_byte_cursor_from_c_str("s3");
    signing.signed_body_header = AWS_SBHT_X_AMZ_CONTENT_SHA256;
    signing.signed_body_value = g_aws_signed_body_value_unsigned_payload;
    signing.credentials_provider = g_creds_provider;

    struct aws_s3_client_config s3cfg = {0};
    s3cfg.region = aws_byte_cursor_from_string(g_region);
    s3cfg.client_bootstrap = g_bootstrap;
    s3cfg.signing_config = &signing;
    s3cfg.tls_mode = AWS_MR_TLS_ENABLED;
    s3cfg.tls_connection_options = NULL;  /* CRT builds default per-region */
    s3cfg.throughput_target_gbps = CRT_THROUGHPUT_GBPS;
    s3cfg.part_size = CRT_PART_SIZE_BYTES;
    /* Prefer host header that uses our virtual-hosted style. CRT will do the right
     * thing as long as we set the correct Host header on each request below. */

    g_s3 = aws_s3_client_new(g_alloc, &s3cfg);
    if (!g_s3) {
        LOG("aws_s3_client_new failed: %s", aws_error_str(aws_last_error()));
        return -1;
    }
    return 0;
}

static void crt_shutdown(void) {
    if (g_s3) { aws_s3_client_release(g_s3); g_s3 = NULL; }
    if (g_creds_provider) { aws_credentials_provider_release(g_creds_provider); g_creds_provider = NULL; }
    if (g_tls_ctx) { aws_tls_ctx_release(g_tls_ctx); g_tls_ctx = NULL; }
    if (g_bootstrap) { aws_client_bootstrap_release(g_bootstrap); g_bootstrap = NULL; }
    if (g_resolver) { aws_host_resolver_release(g_resolver); g_resolver = NULL; }
    if (g_elg) { aws_event_loop_group_release(g_elg); g_elg = NULL; }
    if (g_region) { aws_string_destroy(g_region); g_region = NULL; }
    if (g_endpoint_host) { aws_string_destroy(g_endpoint_host); g_endpoint_host = NULL; }
    free(g_bucket); g_bucket = NULL;
    aws_logger_clean_up(&g_logger);
    aws_s3_library_clean_up();
}

/* ---------- Object listing (via ListObjectsV2 paging using CRT HTTP request) ---------- */

typedef struct {
    char    *key;
    char    *name;
    size_t   size;
} obj_t;

typedef struct {
    obj_t   *items;
    size_t   n;
    size_t   cap;
} obj_list_t;

static void obj_list_free(obj_list_t *o) {
    for (size_t i = 0; i < o->n; i++) { free(o->items[i].key); free(o->items[i].name); }
    free(o->items);
    memset(o, 0, sizeof(*o));
}

/* List response collector: we issue ListObjectsV2 GETs as plain HTTP meta-requests
 * (CRT signs them automatically) and accumulate the body for XML parsing. */
typedef struct {
    buf_t buf;
    int   done;
    int   error_code;
    pthread_mutex_t m;
    pthread_cond_t  cv;
} list_call_t;

static int list_recv_body(struct aws_s3_meta_request *mr,
                          const struct aws_byte_cursor *body,
                          uint64_t range_start, void *user_data) {
    (void)mr; (void)range_start;
    list_call_t *c = (list_call_t *)user_data;
    buf_append(&c->buf, body->ptr, body->len);
    return AWS_OP_SUCCESS;
}

static void list_finish(struct aws_s3_meta_request *mr,
                        const struct aws_s3_meta_request_result *res,
                        void *user_data) {
    (void)mr;
    list_call_t *c = (list_call_t *)user_data;
    pthread_mutex_lock(&c->m);
    c->error_code = res->error_code;
    c->done = 1;
    pthread_cond_signal(&c->cv);
    pthread_mutex_unlock(&c->m);
}

/* Helper: percent-encode for query-string value. */
static void qs_encode(buf_t *b, const char *s) {
    static const char *hex = "0123456789ABCDEF";
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        unsigned char c = *p;
        int unreserved = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                         (c >= '0' && c <= '9') || c == '-' || c == '_' ||
                         c == '.' || c == '~';
        if (unreserved) buf_append_u8(b, c);
        else {
            char e[3] = {'%', hex[c >> 4], hex[c & 0xf]};
            buf_append(b, e, 3);
        }
    }
}

/* Find first <tag>...</tag> after *cursor; advance cursor past closing tag. */
static char *xml_next(const char **cursor, const char *end, const char *tag) {
    char open[64], close[64];
    int o = snprintf(open, sizeof(open), "<%s>", tag);
    int cl = snprintf(close, sizeof(close), "</%s>", tag);
    const char *p = (const char *)memmem(*cursor, (size_t)(end - *cursor), open, (size_t)o);
    if (!p) return NULL;
    p += o;
    const char *e = (const char *)memmem(p, (size_t)(end - p), close, (size_t)cl);
    if (!e) return NULL;
    size_t n = (size_t)(e - p);
    char *r = (char *)xmalloc(n + 1);
    memcpy(r, p, n);
    r[n] = 0;
    *cursor = e + cl;
    return r;
}

static int list_objects(const char *files_prefix, obj_list_t *out) {
    memset(out, 0, sizeof(*out));

    char *cont_token = NULL;
    int rc = -1;

    /* prefix with trailing slash. */
    size_t pn = strlen(files_prefix);
    char *prefix_slash = (char *)xmalloc(pn + 2);
    memcpy(prefix_slash, files_prefix, pn);
    prefix_slash[pn] = '/';
    prefix_slash[pn + 1] = 0;

    for (;;) {
        /* Build path "/?list-type=2&max-keys=1000&prefix=...&[continuation-token=...]" */
        buf_t path; buf_init(&path);
        buf_append_str(&path, "/?list-type=2&max-keys=1000&prefix=");
        qs_encode(&path, prefix_slash);
        if (cont_token) {
            buf_append_str(&path, "&continuation-token=");
            qs_encode(&path, cont_token);
        }
        buf_append_u8(&path, 0);

        struct aws_http_message *req = aws_http_message_new_request(g_alloc);
        aws_http_message_set_request_method(req, aws_byte_cursor_from_c_str("GET"));
        aws_http_message_set_request_path(req, aws_byte_cursor_from_c_str((const char *)path.data));

        struct aws_http_header h_host = {
            .name = aws_byte_cursor_from_c_str("Host"),
            .value = aws_byte_cursor_from_string(g_endpoint_host),
        };
        aws_http_message_add_header(req, h_host);

        list_call_t lc = {0};
        buf_init(&lc.buf);
        pthread_mutex_init(&lc.m, NULL);
        pthread_cond_init(&lc.cv, NULL);

        struct aws_s3_meta_request_options mopt = {0};
        mopt.type = AWS_S3_META_REQUEST_TYPE_DEFAULT;
        mopt.operation_name = aws_byte_cursor_from_c_str("ListObjectsV2");
        mopt.message = req;
        mopt.user_data = &lc;
        mopt.body_callback = list_recv_body;
        mopt.finish_callback = list_finish;

        struct aws_s3_meta_request *mr = aws_s3_client_make_meta_request(g_s3, &mopt);
        if (!mr) {
            LOG("ListObjectsV2: make_meta_request failed: %s",
                aws_error_str(aws_last_error()));
            buf_free(&path);
            buf_free(&lc.buf);
            pthread_mutex_destroy(&lc.m);
            pthread_cond_destroy(&lc.cv);
            aws_http_message_release(req);
            goto out;
        }

        pthread_mutex_lock(&lc.m);
        while (!lc.done) pthread_cond_wait(&lc.cv, &lc.m);
        pthread_mutex_unlock(&lc.m);

        aws_s3_meta_request_release(mr);
        aws_http_message_release(req);
        buf_free(&path);

        if (lc.error_code) {
            LOG("ListObjectsV2 failed: %s", aws_error_str(lc.error_code));
            buf_free(&lc.buf);
            pthread_mutex_destroy(&lc.m);
            pthread_cond_destroy(&lc.cv);
            goto out;
        }

        /* Parse <Contents><Key>..</Key><Size>..</Size> ...</Contents> repeats. */
        const char *cur = (const char *)lc.buf.data;
        const char *end = cur + lc.buf.len;
        for (;;) {
            const char *contents_open = (const char *)memmem(cur, (size_t)(end - cur), "<Contents>", 10);
            if (!contents_open) break;
            const char *contents_inner = contents_open + 10;
            const char *contents_close = (const char *)memmem(contents_inner, (size_t)(end - contents_inner), "</Contents>", 11);
            if (!contents_close) break;
            const char *block_end = contents_close;
            const char *p = contents_inner;

            char *key = xml_next(&p, block_end, "Key");
            char *size_s = xml_next(&p, block_end, "Size");
            if (key && size_s) {
                /* Strip prefix to get name. */
                const char *name = key;
                if (strncmp(key, prefix_slash, pn + 1) == 0) name = key + pn + 1;
                if (*name) {
                    if (out->n == out->cap) {
                        out->cap = out->cap ? out->cap * 2 : 1024;
                        out->items = (obj_t *)xrealloc(out->items, out->cap * sizeof(*out->items));
                    }
                    out->items[out->n].key = key;
                    out->items[out->n].name = xstrdup(name);
                    out->items[out->n].size = (size_t)strtoull(size_s, NULL, 10);
                    out->n++;
                    key = NULL;
                }
            }
            free(key);
            free(size_s);
            cur = contents_close + 11;
        }

        /* Continuation? */
        free(cont_token);
        cont_token = NULL;
        if (memmem(lc.buf.data, lc.buf.len, "<IsTruncated>true</IsTruncated>", 31)) {
            const char *cur2 = (const char *)lc.buf.data;
            const char *end2 = cur2 + lc.buf.len;
            char *t = NULL, *prev_t = NULL;
            for (;;) {
                t = xml_next(&cur2, end2, "NextContinuationToken");
                if (!t) break;
                free(prev_t); prev_t = t;
            }
            cont_token = prev_t;
        }
        buf_free(&lc.buf);
        pthread_mutex_destroy(&lc.m);
        pthread_cond_destroy(&lc.cv);

        if (!cont_token) break;
    }

    rc = 0;

out:
    free(prefix_slash);
    free(cont_token);
    if (rc != 0) obj_list_free(out);
    return rc;
}

/* ---------- Download stage ---------- */

typedef struct {
    obj_t   *obj;            /* not owned */
    buf_t    body;           /* downloaded bytes */
    int      error_code;
    int      done;
    pthread_mutex_t m;
    pthread_cond_t  cv;
} download_t;

static int dl_recv_body(struct aws_s3_meta_request *mr,
                        const struct aws_byte_cursor *body,
                        uint64_t range_start, void *user_data) {
    (void)mr; (void)range_start;
    download_t *d = (download_t *)user_data;
    buf_append(&d->body, body->ptr, body->len);
    return AWS_OP_SUCCESS;
}

static void dl_finish(struct aws_s3_meta_request *mr,
                      const struct aws_s3_meta_request_result *res,
                      void *user_data) {
    (void)mr;
    download_t *d = (download_t *)user_data;
    pthread_mutex_lock(&d->m);
    d->error_code = res->error_code;
    d->done = 1;
    pthread_cond_signal(&d->cv);
    pthread_mutex_unlock(&d->m);
}

/* Build a GET meta-request for one key. */
static struct aws_s3_meta_request *start_download(download_t *d, const char *key) {
    /* Path is "/<key>" with key URL-encoded except slashes. */
    buf_t path; buf_init(&path);
    buf_append_u8(&path, '/');
    static const char *hex = "0123456789ABCDEF";
    for (const unsigned char *p = (const unsigned char *)key; *p; p++) {
        unsigned char c = *p;
        int unreserved = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                         (c >= '0' && c <= '9') || c == '-' || c == '_' ||
                         c == '.' || c == '~' || c == '/';
        if (unreserved) buf_append_u8(&path, c);
        else { char e[3] = {'%', hex[c >> 4], hex[c & 0xf]}; buf_append(&path, e, 3); }
    }
    buf_append_u8(&path, 0);

    struct aws_http_message *req = aws_http_message_new_request(g_alloc);
    aws_http_message_set_request_method(req, aws_byte_cursor_from_c_str("GET"));
    aws_http_message_set_request_path(req, aws_byte_cursor_from_c_str((const char *)path.data));
    struct aws_http_header h_host = {
        .name = aws_byte_cursor_from_c_str("Host"),
        .value = aws_byte_cursor_from_string(g_endpoint_host),
    };
    aws_http_message_add_header(req, h_host);

    struct aws_s3_meta_request_options mopt = {0};
    mopt.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    mopt.message = req;
    mopt.user_data = d;
    mopt.body_callback = dl_recv_body;
    mopt.finish_callback = dl_finish;

    struct aws_s3_meta_request *mr = aws_s3_client_make_meta_request(g_s3, &mopt);
    aws_http_message_release(req);
    buf_free(&path);
    return mr;
}

/* ---------- Pipeline: download -> zipper -> upload ----------
 *
 * Upload uses aws_s3_meta_request_write (send_using_async_writes=true). The
 * zipper thread emits ZIP bytes synchronously; each emission becomes one
 * write() call that we wait on via aws_future_void_wait. CRT's internal
 * buffering replaces the ring buffer.
 */

/* Size of each write() pushed to CRT. Per CRT docs, when a write is smaller
 * than `part_size` (CRT_PART_SIZE_BYTES below), the data is *copied* into
 * CRT's internal buffer and the returned future completes immediately. Choose
 * 1 MB so the zipper thread is never blocked waiting for an actual S3
 * round-trip — CRT continues uploading parts in the background while we keep
 * dispatching downloads and zipping. Avoids the 9000-tiny-write overhead by
 * still being decisively bigger than typical zip headers (~30 B). */
#define UPLOAD_BATCH_BYTES (1u * 1024 * 1024)

typedef struct {
    obj_list_t                 *objs;
    struct aws_s3_meta_request *upload_mr;
    /* Write batching buffer — accumulate ZIP bytes here and flush in big
     * chunks to amortize the per-write future round-trip with CRT. */
    uint8_t                    *batch;
    size_t                      batch_len;
    size_t                      batch_cap;
    int                         write_failed;
    int                         ok;
} zipper_args_t;

/* Issue one write to CRT and block on its future. Used internally by the
 * batching layer. */
static int crt_write_sync(zipper_args_t *a, const uint8_t *data, size_t n, int eof) {
    if (a->write_failed) return -1;
    struct aws_byte_cursor c = { .ptr = (uint8_t *)data, .len = n };
    struct aws_future_void *f = aws_s3_meta_request_write(a->upload_mr, c, eof != 0);
    if (!f) {
        LOG("aws_s3_meta_request_write returned null");
        a->write_failed = 1;
        return -1;
    }
    aws_future_void_wait(f, UINT64_MAX);
    int err = aws_future_void_get_error(f);
    aws_future_void_release(f);
    if (err) {
        LOG("upload write failed: %s", aws_error_str(err));
        a->write_failed = 1;
        return -1;
    }
    return 0;
}

/* Buffered write callback for zip_writer. Flushes the batch buffer when it
 * reaches UPLOAD_BATCH_BYTES, or eagerly when eof is set. */
static int upload_write_sync(void *user, const uint8_t *data, size_t n, int eof) {
    zipper_args_t *a = (zipper_args_t *)user;
    if (a->write_failed) return -1;

    /* Append to batch. Resize lazily — capped at UPLOAD_BATCH_BYTES + one
     * worst-case incoming chunk. */
    if (n) {
        if (a->batch_len + n > a->batch_cap) {
            size_t need = a->batch_len + n;
            size_t cap = a->batch_cap ? a->batch_cap : UPLOAD_BATCH_BYTES;
            while (cap < need) cap *= 2;
            a->batch = (uint8_t *)xrealloc(a->batch, cap);
            a->batch_cap = cap;
        }
        memcpy(a->batch + a->batch_len, data, n);
        a->batch_len += n;
    }

    /* Flush full batches. */
    while (a->batch_len >= UPLOAD_BATCH_BYTES && !eof) {
        if (crt_write_sync(a, a->batch, UPLOAD_BATCH_BYTES, 0) != 0) return -1;
        if (a->batch_len > UPLOAD_BATCH_BYTES) {
            memmove(a->batch, a->batch + UPLOAD_BATCH_BYTES,
                    a->batch_len - UPLOAD_BATCH_BYTES);
        }
        a->batch_len -= UPLOAD_BATCH_BYTES;
    }

    /* On eof: flush whatever's left in the batch (with eof=true). */
    if (eof) {
        if (crt_write_sync(a, a->batch, a->batch_len, 1) != 0) return -1;
        a->batch_len = 0;
    }
    return 0;
}

static void *zipper_thread(void *user) {
    zipper_args_t *a = (zipper_args_t *)user;
    zip_writer_t z;
    zip_writer_init(&z, upload_write_sync, a);

    /* Drive parallel downloads with at most MAX_CONCURRENT_DOWNLOADS in flight,
     * capped by MAX_DOWNLOAD_INFLIGHT_BYTES of pending data. As each download
     * completes (in order is NOT required by the contract — the ZIP just needs
     * each entry once), feed it to the zipper. To keep latency low we feed
     * results out-of-order as they finish. */
    size_t inflight = 0;
    size_t inflight_bytes = 0;
    size_t next_to_start = 0;
    size_t completed = 0;

    download_t *dls = (download_t *)xcalloc(a->objs->n, sizeof(*dls));
    struct aws_s3_meta_request **mrs =
        (struct aws_s3_meta_request **)xcalloc(a->objs->n, sizeof(*mrs));

    for (size_t i = 0; i < a->objs->n; i++) {
        pthread_mutex_init(&dls[i].m, NULL);
        pthread_cond_init(&dls[i].cv, NULL);
        dls[i].obj = &a->objs->items[i];
        buf_init(&dls[i].body);
    }

    int *finished_order = (int *)xcalloc(a->objs->n, sizeof(int));

    while (completed < a->objs->n) {
        /* Saturate while we have headroom. */
        while (next_to_start < a->objs->n &&
               inflight < MAX_CONCURRENT_DOWNLOADS &&
               inflight_bytes + a->objs->items[next_to_start].size <= MAX_DOWNLOAD_INFLIGHT_BYTES) {
            mrs[next_to_start] = start_download(&dls[next_to_start], a->objs->items[next_to_start].key);
            if (!mrs[next_to_start]) {
                LOG("start_download failed for %s", a->objs->items[next_to_start].key);
                a->ok = 0;
                goto end;
            }
            inflight++;
            inflight_bytes += a->objs->items[next_to_start].size;
            next_to_start++;
        }

        /* Wait for at least one download to finish. */
        int idx = -1;
        for (size_t i = 0; i < a->objs->n; i++) {
            if (mrs[i] && !finished_order[i]) {
                pthread_mutex_lock(&dls[i].m);
                int done = dls[i].done;
                pthread_mutex_unlock(&dls[i].m);
                if (done) { idx = (int)i; break; }
            }
        }
        if (idx < 0) {
            /* None ready yet — wait on the lowest in-flight one. */
            for (size_t i = 0; i < a->objs->n; i++) {
                if (mrs[i] && !finished_order[i]) {
                    pthread_mutex_lock(&dls[i].m);
                    while (!dls[i].done) pthread_cond_wait(&dls[i].cv, &dls[i].m);
                    pthread_mutex_unlock(&dls[i].m);
                    idx = (int)i;
                    break;
                }
            }
        }
        if (idx < 0) break;  /* shouldn't happen */

        if (dls[idx].error_code) {
            LOG("download %s failed: %s", a->objs->items[idx].key,
                aws_error_str(dls[idx].error_code));
            a->ok = 0;
            goto end;
        }

        /* Feed to zipper. */
        if (zip_writer_add(&z, a->objs->items[idx].name,
                           dls[idx].body.data, dls[idx].body.len) != 0) {
            a->ok = 0;
            inflight_bytes -= a->objs->items[idx].size;
            inflight--;
            completed++;
            finished_order[idx] = 1;
            buf_free(&dls[idx].body);
            aws_s3_meta_request_release(mrs[idx]);
            mrs[idx] = NULL;
            goto end;
        }

        inflight_bytes -= a->objs->items[idx].size;
        inflight--;
        completed++;
        finished_order[idx] = 1;
        buf_free(&dls[idx].body);
        aws_s3_meta_request_release(mrs[idx]);
        mrs[idx] = NULL;
    }

    if (zip_writer_finish(&z) == 0) a->ok = 1;
    else a->ok = 0;
    /* zip_writer_finish() already issued the eof=true write. Skip the cleanup
     * eof in the normal success path; the early-exit path falls through to
     * the eof signal below. */
    goto cleanup;

end:
    /* Early-exit path: always signal eof so the uploader unblocks. */
    if (!a->write_failed) {
        upload_write_sync(a, NULL, 0, 1);
    }

cleanup:

    for (size_t i = 0; i < a->objs->n; i++) {
        if (mrs[i]) {
            pthread_mutex_lock(&dls[i].m);
            while (!dls[i].done) pthread_cond_wait(&dls[i].cv, &dls[i].m);
            pthread_mutex_unlock(&dls[i].m);
            aws_s3_meta_request_release(mrs[i]);
        }
        buf_free(&dls[i].body);
        pthread_mutex_destroy(&dls[i].m);
        pthread_cond_destroy(&dls[i].cv);
    }
    free(dls);
    free(mrs);
    free(finished_order);
    zip_writer_free(&z);
    free(a->batch);
    a->batch = NULL;
    a->batch_len = a->batch_cap = 0;
    return NULL;
}

/* Upload meta-request finish state. */
typedef struct {
    int done;
    int error_code;
    pthread_mutex_t m;
    pthread_cond_t  cv;
} upload_call_t;

static void up_finish(struct aws_s3_meta_request *mr,
                      const struct aws_s3_meta_request_result *res,
                      void *user_data) {
    (void)mr;
    upload_call_t *c = (upload_call_t *)user_data;
    pthread_mutex_lock(&c->m);
    c->error_code = res->error_code;
    c->done = 1;
    pthread_cond_signal(&c->cv);
    pthread_mutex_unlock(&c->m);
}

/* Build the PUT meta-request configured for async writes. The caller drives
 * the body via aws_s3_meta_request_write (see upload_write_sync). */
static struct aws_s3_meta_request *start_upload(const char *archive_key,
                                                upload_call_t *uc) {
    buf_t path; buf_init(&path);
    buf_append_u8(&path, '/');
    static const char *hex = "0123456789ABCDEF";
    for (const unsigned char *p = (const unsigned char *)archive_key; *p; p++) {
        unsigned char c = *p;
        int unreserved = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                         (c >= '0' && c <= '9') || c == '-' || c == '_' ||
                         c == '.' || c == '~' || c == '/';
        if (unreserved) buf_append_u8(&path, c);
        else { char e[3] = {'%', hex[c >> 4], hex[c & 0xf]}; buf_append(&path, e, 3); }
    }
    buf_append_u8(&path, 0);

    struct aws_http_message *req = aws_http_message_new_request(g_alloc);
    aws_http_message_set_request_method(req, aws_byte_cursor_from_c_str("PUT"));
    aws_http_message_set_request_path(req, aws_byte_cursor_from_c_str((const char *)path.data));

    /* Host + Content-Type only — Content-Length is handled by CRT for async writes. */
    struct aws_http_header h_host = {
        .name = aws_byte_cursor_from_c_str("Host"),
        .value = aws_byte_cursor_from_string(g_endpoint_host),
    };
    struct aws_http_header h_type = {
        .name = aws_byte_cursor_from_c_str("Content-Type"),
        .value = aws_byte_cursor_from_c_str("application/zip"),
    };
    aws_http_message_add_header(req, h_host);
    aws_http_message_add_header(req, h_type);

    struct aws_s3_meta_request_options mopt = {0};
    mopt.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
    mopt.send_using_async_writes = true;
    mopt.message = req;
    mopt.user_data = uc;
    mopt.finish_callback = up_finish;

    struct aws_s3_meta_request *mr = aws_s3_client_make_meta_request(g_s3, &mopt);
    if (!mr) {
        LOG("start_upload: make_meta_request failed: %s", aws_error_str(aws_last_error()));
    }
    aws_http_message_release(req);
    buf_free(&path);
    return mr;
}

/* ---------- Per-invocation handler ---------- */

static int handle_invocation(const char *bucket_name, const char *files_prefix,
                             const char *archive_key) {
    (void)bucket_name;  /* g_bucket is set at cold start */

    obj_list_t objs = {0};
    if (list_objects(files_prefix, &objs) != 0) return -1;
    LOG("Listed %zu objects under %s/", objs.n, files_prefix);

    upload_call_t uc = {0};
    pthread_mutex_init(&uc.m, NULL);
    pthread_cond_init(&uc.cv, NULL);

    struct aws_s3_meta_request *mr = start_upload(archive_key, &uc);
    if (!mr) {
        obj_list_free(&objs);
        pthread_mutex_destroy(&uc.m);
        pthread_cond_destroy(&uc.cv);
        return -1;
    }

    zipper_args_t zargs = { .objs = &objs, .upload_mr = mr, .write_failed = 0, .ok = 0 };

    pthread_t z_tid;
    pthread_create(&z_tid, NULL, zipper_thread, &zargs);
    pthread_join(z_tid, NULL);

    /* Wait for CRT to finalize the multipart upload. */
    pthread_mutex_lock(&uc.m);
    while (!uc.done) pthread_cond_wait(&uc.cv, &uc.m);
    pthread_mutex_unlock(&uc.m);

    aws_s3_meta_request_release(mr);

    int rc = (zargs.ok && uc.error_code == 0) ? 0 : -1;
    if (uc.error_code) {
        LOG("upload finished with error: %s", aws_error_str(uc.error_code));
    }

    obj_list_free(&objs);
    pthread_mutex_destroy(&uc.m);
    pthread_cond_destroy(&uc.cv);
    return rc;
}

/* ---------- main: cold-start init then runtime API loop ---------- */

int main(void) {
    g_alloc = aws_default_allocator();

    lambda_rt_t rt;
    if (lambda_rt_init(&rt) != 0) {
        LOG("lambda_rt_init failed");
        return 1;
    }

    int crt_ready = 0;

    for (;;) {
        char *request_id = NULL;
        buf_t payload; buf_init(&payload);
        if (lambda_rt_next(&rt, &request_id, &payload) != 0) {
            LOG("lambda_rt_next failed");
            buf_free(&payload);
            free(request_id);
            continue;
        }

        char *bucket = json_extract_string((const char *)payload.data, payload.len, "bucket_name");
        char *prefix = json_extract_string((const char *)payload.data, payload.len, "files_prefix");
        char *akey   = json_extract_string((const char *)payload.data, payload.len, "archive_key");

        if (!bucket || !prefix || !akey) {
            lambda_rt_error(&rt, request_id, "Runtime.InvalidPayload",
                            "missing bucket_name/files_prefix/archive_key");
            free(bucket); free(prefix); free(akey);
            buf_free(&payload); free(request_id);
            continue;
        }

        if (!crt_ready) {
            if (crt_init(bucket) != 0) {
                lambda_rt_error(&rt, request_id, "Runtime.CrtInit", "crt_init failed");
                free(bucket); free(prefix); free(akey);
                buf_free(&payload); free(request_id);
                continue;
            }
            crt_ready = 1;
        } else if (strcmp(bucket, g_bucket) != 0) {
            /* Bucket changed across invocations — reinit endpoint host. */
            free(g_bucket);
            g_bucket = xstrdup(bucket);
            char host[512];
            snprintf(host, sizeof(host), "%s.s3.%s.amazonaws.com",
                     bucket, (const char *)aws_string_c_str(g_region));
            aws_string_destroy(g_endpoint_host);
            g_endpoint_host = aws_string_new_from_c_str(g_alloc, host);
        }

        int rc = handle_invocation(bucket, prefix, akey);
        if (rc == 0) {
            lambda_rt_respond(&rt, request_id, "{}", 2);
        } else {
            lambda_rt_error(&rt, request_id, "Runtime.HandlerError",
                            "archive creation failed");
        }

        free(bucket); free(prefix); free(akey);
        buf_free(&payload); free(request_id);
    }

    /* unreachable */
    crt_shutdown();
    lambda_rt_free(&rt);
    return 0;
}
