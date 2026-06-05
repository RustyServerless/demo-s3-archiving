#ifndef DSA_UTIL_H
#define DSA_UTIL_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LOG(fmt, ...) fprintf(stderr, fmt "\n", ##__VA_ARGS__)
#define DIE(fmt, ...) do { LOG("FATAL: " fmt, ##__VA_ARGS__); exit(1); } while (0)

void *xmalloc(size_t n);
void *xcalloc(size_t n, size_t sz);
void *xrealloc(void *p, size_t n);
char *xstrdup(const char *s);

/* Resizable byte buffer. */
typedef struct {
    uint8_t *data;
    size_t   len;
    size_t   cap;
} buf_t;

void buf_init(buf_t *b);
void buf_free(buf_t *b);
void buf_reserve(buf_t *b, size_t need);
void buf_append(buf_t *b, const void *data, size_t n);
void buf_append_str(buf_t *b, const char *s);
void buf_append_u8(buf_t *b, uint8_t v);
void buf_append_u16le(buf_t *b, uint16_t v);
void buf_append_u32le(buf_t *b, uint32_t v);
void buf_append_u64le(buf_t *b, uint64_t v);

#endif
