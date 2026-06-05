#ifndef DSA_RUNTIME_H
#define DSA_RUNTIME_H

#include "util.h"

/*
 * Tiny Lambda Runtime API client (HTTP only — runtime API endpoint speaks
 * plaintext over the in-instance link). One blocking call per loop iteration.
 *
 *   GET http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime/invocation/next
 *   POST http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime/invocation/${REQUEST_ID}/response
 *   POST http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime/invocation/${REQUEST_ID}/error
 *   POST http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime/init/error
 */

typedef struct {
    char *runtime_api;     /* host:port from $AWS_LAMBDA_RUNTIME_API */
    int   sock;            /* persistent TCP socket */
} lambda_rt_t;

int  lambda_rt_init(lambda_rt_t *rt);
void lambda_rt_free(lambda_rt_t *rt);

/* Block until the next invocation arrives. Returns malloc'd request_id and
 * malloc'd payload (caller frees). Returns 0 on success. */
int lambda_rt_next(lambda_rt_t *rt, char **out_request_id, buf_t *out_payload);

/* Post a successful response (we always return JSON object {} when done). */
int lambda_rt_respond(lambda_rt_t *rt, const char *request_id,
                      const char *body, size_t body_n);

/* Post an error. */
int lambda_rt_error(lambda_rt_t *rt, const char *request_id,
                    const char *error_type, const char *message);

#endif
