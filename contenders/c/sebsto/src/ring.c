#include "ring.h"

#include <string.h>

void ring_init(ring_t *r, size_t cap) {
    memset(r, 0, sizeof(*r));
    r->buf = (uint8_t *)xmalloc(cap);
    r->cap = cap;
    pthread_mutex_init(&r->m, NULL);
    pthread_cond_init(&r->not_full, NULL);
    pthread_cond_init(&r->not_empty, NULL);
}

void ring_free(ring_t *r) {
    free(r->buf);
    pthread_mutex_destroy(&r->m);
    pthread_cond_destroy(&r->not_full);
    pthread_cond_destroy(&r->not_empty);
    memset(r, 0, sizeof(*r));
}

void ring_write(ring_t *r, const uint8_t *data, size_t n) {
    pthread_mutex_lock(&r->m);
    while (n) {
        while (r->used == r->cap && !r->closed) {
            pthread_cond_wait(&r->not_full, &r->m);
        }
        if (r->closed) break;  /* shouldn't happen on producer side */
        size_t free_bytes = r->cap - r->used;
        size_t copy = n < free_bytes ? n : free_bytes;
        size_t until_end = r->cap - r->tail;
        size_t first = copy < until_end ? copy : until_end;
        memcpy(r->buf + r->tail, data, first);
        if (copy > first) {
            memcpy(r->buf, data + first, copy - first);
            r->tail = copy - first;
        } else {
            r->tail = (r->tail + copy) % r->cap;
        }
        r->used += copy;
        data += copy;
        n -= copy;
        pthread_cond_signal(&r->not_empty);
    }
    pthread_mutex_unlock(&r->m);
}

size_t ring_read(ring_t *r, uint8_t *out, size_t n) {
    pthread_mutex_lock(&r->m);
    while (r->used == 0 && !r->closed) {
        pthread_cond_wait(&r->not_empty, &r->m);
    }
    size_t got = 0;
    if (r->used) {
        size_t copy = n < r->used ? n : r->used;
        size_t until_end = r->cap - r->head;
        size_t first = copy < until_end ? copy : until_end;
        memcpy(out, r->buf + r->head, first);
        if (copy > first) {
            memcpy(out + first, r->buf, copy - first);
            r->head = copy - first;
        } else {
            r->head = (r->head + copy) % r->cap;
        }
        r->used -= copy;
        got = copy;
        pthread_cond_signal(&r->not_full);
    }
    pthread_mutex_unlock(&r->m);
    return got;  /* 0 means EOF after close */
}

void ring_close(ring_t *r) {
    pthread_mutex_lock(&r->m);
    r->closed = 1;
    pthread_cond_broadcast(&r->not_empty);
    pthread_cond_broadcast(&r->not_full);
    pthread_mutex_unlock(&r->m);
}
