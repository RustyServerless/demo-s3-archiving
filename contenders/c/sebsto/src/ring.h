#ifndef DSA_RING_H
#define DSA_RING_H

#include "util.h"

#include <pthread.h>

/*
 * Single-producer / single-consumer byte ring buffer. The producer is the
 * zipper thread (writes the ZIP stream); the consumer is the CRT input-stream
 * read callback (called by aws-c-s3 to pull body bytes for multipart upload).
 *
 * Capacity is the total ring size in bytes. The consumer's read() will block
 * until at least 1 byte is available or the ring is closed by the producer.
 * The producer's write() will block when the ring is full until the consumer
 * drains some.
 *
 * Calling ring_close() signals end-of-stream: consumer drains remaining bytes
 * then sees EOF (read returns 0 bytes after close + drained).
 */

typedef struct {
    uint8_t *buf;
    size_t   cap;
    size_t   head;          /* read pointer */
    size_t   tail;          /* write pointer */
    size_t   used;
    int      closed;        /* producer signaled EOF */
    pthread_mutex_t m;
    pthread_cond_t  not_full;
    pthread_cond_t  not_empty;
} ring_t;

void ring_init(ring_t *r, size_t cap);
void ring_free(ring_t *r);

/* Block until all `n` bytes are written (or never returns if no consumer). */
void ring_write(ring_t *r, const uint8_t *data, size_t n);

/* Read up to `n` bytes, blocking until at least 1 byte is available OR the
 * ring is closed. Returns number of bytes actually written into `out` (0 means
 * EOF after close). */
size_t ring_read(ring_t *r, uint8_t *out, size_t n);

/* Producer side: signal end-of-stream. */
void ring_close(ring_t *r);

#endif
