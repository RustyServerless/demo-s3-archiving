/*
 * C contender for the demo-s3-archiving benchmark — built on the AWS Common Runtime
 * (aws-c-s3 etc.). Mirrors the Rust reference contender's three-stage pipeline:
 *
 *   download (CRT meta-requests, parallel)
 *      → zipper thread (STORE-only, CRC32 via zlib)
 *      → upload (CRT multipart PUT reading from a ring buffer via aws_input_stream)
 *
 * No tracing / OTel — Lambda CloudWatch logs are sufficient for the benchmark.
 */

#include "json.h"
#include "ring.h"
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
#include <aws/io/host_resolver.h>
#include <aws/io/logging.h>
#include <aws/io/stream.h>
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

/* Ring buffer size between zipper and uploader — sized to hold a few CRT parts. */
#define RING_CAPACITY               (32u * 1024 * 1024)

/* CRT throughput hint (Gb/s). Higher = more connections. */
#define CRT_THROUGHPUT_GBPS 25.0

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
        mopt.message = req;
        mopt.user_data = &lc;
        mopt.body_callback = list_recv_body;
        mopt.finish_callback = list_finish;
        /* GET-style; CRT will sign as a normal HTTP request. */

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

/* ---------- Upload stage: PUT meta-request reading from a ring ----------
 *
 * CRT calls aws_input_stream_read repeatedly; we block on ring_read until
 * either bytes arrive or the producer closes the ring.
 */

typedef struct {
    struct aws_input_stream base;
    struct aws_allocator   *alloc;
    ring_t                 *ring;
    int64_t                 length;     /* total length, known up-front */
    int64_t                 position;
} ring_input_stream_t;

static int ris_seek(struct aws_input_stream *s, int64_t off, enum aws_stream_seek_basis basis) {
    /* CRT shouldn't seek a streaming source; reject. */
    (void)s; (void)off; (void)basis;
    return aws_raise_error(AWS_ERROR_UNIMPLEMENTED);
}

static int ris_read(struct aws_input_stream *s, struct aws_byte_buf *dest) {
    ring_input_stream_t *r = (ring_input_stream_t *)s;
    size_t avail = dest->capacity - dest->len;
    if (!avail) return AWS_OP_SUCCESS;
    size_t got = ring_read(r->ring, dest->buffer + dest->len, avail);
    dest->len += got;
    r->position += (int64_t)got;
    return AWS_OP_SUCCESS;
}

static int ris_status(struct aws_input_stream *s, struct aws_stream_status *st) {
    ring_input_stream_t *r = (ring_input_stream_t *)s;
    st->is_end_of_stream = (r->position >= r->length);
    st->is_valid = true;
    return AWS_OP_SUCCESS;
}

static int ris_get_length(struct aws_input_stream *s, int64_t *out) {
    ring_input_stream_t *r = (ring_input_stream_t *)s;
    *out = r->length;
    return AWS_OP_SUCCESS;
}

static struct aws_input_stream_vtable g_ris_vtable = {
    .seek = ris_seek,
    .read = ris_read,
    .get_status = ris_status,
    .get_length = ris_get_length,
};

/* ---------- Pipeline: download -> zipper -> upload ---------- */

typedef struct {
    obj_list_t  *objs;
    ring_t      *ring;
    int          ok;
} zipper_args_t;

static void *zipper_thread(void *user) {
    zipper_args_t *a = (zipper_args_t *)user;
    zip_writer_t z;
    zip_writer_init(&z, a->ring);

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
        zip_writer_add(&z, a->objs->items[idx].name, dls[idx].body.data, dls[idx].body.len);

        inflight_bytes -= a->objs->items[idx].size;
        inflight--;
        completed++;
        finished_order[idx] = 1;
        buf_free(&dls[idx].body);
        aws_s3_meta_request_release(mrs[idx]);
        mrs[idx] = NULL;
    }

    zip_writer_finish(&z);
    a->ok = 1;

end:
    /* Make sure we close even on early-exit so the uploader unblocks. */
    ring_close(a->ring);

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
    return NULL;
}

/* Pre-compute total ZIP size so we can give CRT a Content-Length. STORE-only,
 * so size is sum(local_header + name + payload) + sum(central + name + extra)
 * + EOCD (+ ZIP64 records if needed). */
static uint64_t compute_zip_size(const obj_list_t *o) {
    uint64_t total = 0;
    uint64_t running = 0;
    /* Local headers + payload. */
    for (size_t i = 0; i < o->n; i++) {
        size_t name_len = strlen(o->items[i].name);
        total += 30 + name_len + o->items[i].size;
        running += 30 + name_len + o->items[i].size;
    }
    /* Central directory entries (size depends on running local-header offset). */
    uint64_t cd_size = 0;
    uint64_t r2 = 0;
    for (size_t i = 0; i < o->n; i++) {
        size_t name_len = strlen(o->items[i].name);
        int z64_size = (o->items[i].size >= 0xFFFFFFFFULL);
        int z64_off  = (r2 >= 0xFFFFFFFFULL);
        size_t extra = (z64_size || z64_off) ? (4 + (z64_size ? 16 : 0) + (z64_off ? 8 : 0)) : 0;
        cd_size += 46 + name_len + extra;
        r2 += 30 + name_len + o->items[i].size;
    }
    total += cd_size;
    /* EOCD always; ZIP64 EOCD+locator if needed. */
    int need64 = (o->n >= 0xFFFF) ||
                 (running >= 0xFFFFFFFFULL) ||
                 (cd_size >= 0xFFFFFFFFULL);
    total += 22;
    if (need64) total += 56 + 20;
    return total;
}

/* Upload a streaming PUT of total `total_size` bytes from `ring`. */
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

static int upload_zip(const char *archive_key, ring_t *ring, uint64_t total_size) {
    /* path = "/<archive_key>" */
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

    char clen[32]; snprintf(clen, sizeof(clen), "%" PRIu64, total_size);
    struct aws_http_header headers[] = {
        { aws_byte_cursor_from_c_str("Host"), aws_byte_cursor_from_string(g_endpoint_host) },
        { aws_byte_cursor_from_c_str("Content-Type"), aws_byte_cursor_from_c_str("application/zip") },
        { aws_byte_cursor_from_c_str("Content-Length"), aws_byte_cursor_from_c_str(clen) },
    };
    for (size_t i = 0; i < sizeof(headers) / sizeof(headers[0]); i++) {
        aws_http_message_add_header(req, headers[i]);
    }

    /* Build our streaming input stream. */
    ring_input_stream_t ris = {0};
    ris.base.vtable = &g_ris_vtable;
    ris.base.impl = &ris;
    ris.alloc = g_alloc;
    ris.ring = ring;
    ris.length = (int64_t)total_size;
    aws_http_message_set_body_stream(req, &ris.base);

    upload_call_t uc = {0};
    pthread_mutex_init(&uc.m, NULL);
    pthread_cond_init(&uc.cv, NULL);

    struct aws_s3_meta_request_options mopt = {0};
    mopt.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
    mopt.message = req;
    mopt.user_data = &uc;
    mopt.finish_callback = up_finish;

    struct aws_s3_meta_request *mr = aws_s3_client_make_meta_request(g_s3, &mopt);
    if (!mr) {
        LOG("upload_zip: make_meta_request failed: %s", aws_error_str(aws_last_error()));
        aws_http_message_release(req);
        buf_free(&path);
        ring_close(ring);
        pthread_mutex_destroy(&uc.m);
        pthread_cond_destroy(&uc.cv);
        return -1;
    }

    pthread_mutex_lock(&uc.m);
    while (!uc.done) pthread_cond_wait(&uc.cv, &uc.m);
    pthread_mutex_unlock(&uc.m);

    aws_s3_meta_request_release(mr);
    aws_http_message_release(req);
    buf_free(&path);
    pthread_mutex_destroy(&uc.m);
    pthread_cond_destroy(&uc.cv);

    if (uc.error_code) {
        LOG("upload_zip failed: %s", aws_error_str(uc.error_code));
        return -1;
    }
    return 0;
}

/* ---------- Per-invocation handler ---------- */

static int handle_invocation(const char *bucket_name, const char *files_prefix,
                             const char *archive_key) {
    (void)bucket_name;  /* set via global g_bucket once at cold start */

    /* List the source files. */
    obj_list_t objs = {0};
    if (list_objects(files_prefix, &objs) != 0) return -1;
    LOG("Listed %zu objects under %s/", objs.n, files_prefix);

    /* Compute total ZIP size for Content-Length. */
    uint64_t total = compute_zip_size(&objs);
    LOG("Computed ZIP size: %" PRIu64 " bytes", total);

    ring_t ring;
    ring_init(&ring, RING_CAPACITY);

    zipper_args_t zargs = { .objs = &objs, .ring = &ring, .ok = 0 };
    pthread_t z_tid;
    pthread_create(&z_tid, NULL, zipper_thread, &zargs);

    /* Upload reads from the ring on this thread. */
    int up_rc = upload_zip(archive_key, &ring, total);

    pthread_join(z_tid, NULL);
    ring_free(&ring);

    int rc = (zargs.ok && up_rc == 0) ? 0 : -1;

    obj_list_free(&objs);
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
