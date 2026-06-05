#include "util.h"

void *xmalloc(size_t n) {
    void *p = malloc(n);
    if (!p && n) DIE("malloc(%zu) failed", n);
    return p;
}

void *xcalloc(size_t n, size_t sz) {
    void *p = calloc(n, sz);
    if (!p && n && sz) DIE("calloc(%zu,%zu) failed", n, sz);
    return p;
}

void *xrealloc(void *p, size_t n) {
    void *q = realloc(p, n);
    if (!q && n) DIE("realloc(%zu) failed", n);
    return q;
}

char *xstrdup(const char *s) {
    char *r = strdup(s);
    if (!r) DIE("strdup failed");
    return r;
}

void buf_init(buf_t *b) {
    b->data = NULL;
    b->len = 0;
    b->cap = 0;
}

void buf_free(buf_t *b) {
    free(b->data);
    b->data = NULL;
    b->len = b->cap = 0;
}

void buf_reserve(buf_t *b, size_t need) {
    if (b->cap >= need) return;
    size_t cap = b->cap ? b->cap : 64;
    while (cap < need) cap *= 2;
    b->data = (uint8_t *)xrealloc(b->data, cap);
    b->cap = cap;
}

void buf_append(buf_t *b, const void *data, size_t n) {
    buf_reserve(b, b->len + n);
    memcpy(b->data + b->len, data, n);
    b->len += n;
}

void buf_append_str(buf_t *b, const char *s) {
    buf_append(b, s, strlen(s));
}

void buf_append_u8(buf_t *b, uint8_t v) {
    buf_append(b, &v, 1);
}

void buf_append_u16le(buf_t *b, uint16_t v) {
    uint8_t x[2] = {(uint8_t)(v), (uint8_t)(v >> 8)};
    buf_append(b, x, 2);
}

void buf_append_u32le(buf_t *b, uint32_t v) {
    uint8_t x[4] = {(uint8_t)(v), (uint8_t)(v >> 8), (uint8_t)(v >> 16), (uint8_t)(v >> 24)};
    buf_append(b, x, 4);
}

void buf_append_u64le(buf_t *b, uint64_t v) {
    uint8_t x[8];
    for (int i = 0; i < 8; i++) x[i] = (uint8_t)(v >> (8 * i));
    buf_append(b, x, 8);
}
