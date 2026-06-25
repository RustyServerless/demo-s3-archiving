#include "runtime.h"

#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

static int connect_runtime(const char *host_port) {
    char host[256];
    const char *colon = strchr(host_port, ':');
    const char *port = "9001";
    if (colon) {
        size_t hl = (size_t)(colon - host_port);
        if (hl >= sizeof(host)) return -1;
        memcpy(host, host_port, hl);
        host[hl] = 0;
        port = colon + 1;
    } else {
        strncpy(host, host_port, sizeof(host) - 1);
        host[sizeof(host) - 1] = 0;
    }

    struct addrinfo hints = {0}, *res = NULL;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, port, &hints, &res) != 0) return -1;

    int s = -1;
    for (struct addrinfo *p = res; p; p = p->ai_next) {
        s = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (s < 0) continue;
        if (connect(s, p->ai_addr, p->ai_addrlen) == 0) break;
        close(s);
        s = -1;
    }
    freeaddrinfo(res);
    if (s < 0) return -1;

    int one = 1;
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return s;
}

int lambda_rt_init(lambda_rt_t *rt) {
    memset(rt, 0, sizeof(*rt));
    const char *api = getenv("AWS_LAMBDA_RUNTIME_API");
    if (!api) {
        LOG("AWS_LAMBDA_RUNTIME_API not set");
        return -1;
    }
    rt->runtime_api = xstrdup(api);
    rt->sock = -1;
    return 0;
}

void lambda_rt_free(lambda_rt_t *rt) {
    if (rt->sock >= 0) close(rt->sock);
    free(rt->runtime_api);
    memset(rt, 0, sizeof(*rt));
    rt->sock = -1;
}

static int ensure_connected(lambda_rt_t *rt) {
    if (rt->sock >= 0) return 0;
    rt->sock = connect_runtime(rt->runtime_api);
    if (rt->sock < 0) {
        LOG("connect to runtime API %s failed: %s", rt->runtime_api, strerror(errno));
        return -1;
    }
    return 0;
}

static void close_connection(lambda_rt_t *rt) {
    if (rt->sock >= 0) { close(rt->sock); rt->sock = -1; }
}

static int send_all(int s, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    while (n) {
        ssize_t k = send(s, p, n, 0);
        if (k < 0) { if (errno == EINTR) continue; return -1; }
        p += k; n -= (size_t)k;
    }
    return 0;
}

/* Read up to one HTTP response. Returns 0 on success and fills:
 *   - status
 *   - request_id_hdr (malloc'd or NULL)
 *   - body (buf_t, caller initialised)
 * Reads the body using Content-Length only (Lambda runtime API always sets it).
 */
static int read_response(int s, int *status, char **request_id_hdr, buf_t *body) {
    *status = -1;
    *request_id_hdr = NULL;

    /* Slurp the headers (read byte-by-byte until \r\n\r\n; small headers, fine). */
    buf_t header; buf_init(&header);
    int seen = 0;
    for (;;) {
        char c;
        ssize_t k = recv(s, &c, 1, 0);
        if (k <= 0) {
            if (k < 0 && errno == EINTR) continue;
            buf_free(&header);
            return -1;
        }
        buf_append_u8(&header, (uint8_t)c);
        if (c == '\r') seen = 1;
        else if (c == '\n' && seen) {
            if (header.len >= 4 &&
                memcmp(header.data + header.len - 4, "\r\n\r\n", 4) == 0) break;
            seen = 0;
        } else seen = 0;
        if (header.len > 32 * 1024) {
            buf_free(&header);
            return -1;
        }
    }

    /* Parse status line. */
    {
        const char *h = (const char *)header.data;
        while (*h && *h != ' ') h++;
        while (*h == ' ') h++;
        *status = atoi(h);
    }

    /* Parse Content-Length and Lambda-Runtime-Aws-Request-Id headers. */
    long content_length = -1;
    {
        const char *h = (const char *)header.data;
        const char *end = h + header.len;
        while (h < end) {
            const char *eol = memchr(h, '\n', (size_t)(end - h));
            if (!eol) break;
            size_t llen = (size_t)(eol - h);
            if (llen && h[llen - 1] == '\r') llen--;

            if (llen > 15 && strncasecmp(h, "Content-Length:", 15) == 0) {
                const char *v = h + 15;
                while (*v == ' ') v++;
                content_length = strtol(v, NULL, 10);
            } else if (llen > 31 &&
                       strncasecmp(h, "Lambda-Runtime-Aws-Request-Id:", 30) == 0) {
                const char *v = h + 30;
                while (*v == ' ') v++;
                size_t vlen = llen - (size_t)(v - h);
                while (vlen && (v[vlen - 1] == ' ' || v[vlen - 1] == '\t')) vlen--;
                char *r = (char *)xmalloc(vlen + 1);
                memcpy(r, v, vlen);
                r[vlen] = 0;
                free(*request_id_hdr);
                *request_id_hdr = r;
            }
            h = eol + 1;
        }
    }
    buf_free(&header);

    if (content_length < 0) return -1;
    buf_reserve(body, (size_t)content_length);
    body->len = 0;
    while (body->len < (size_t)content_length) {
        ssize_t k = recv(s, body->data + body->len,
                         (size_t)content_length - body->len, 0);
        if (k <= 0) {
            if (k < 0 && errno == EINTR) continue;
            return -1;
        }
        body->len += (size_t)k;
    }
    return 0;
}

int lambda_rt_next(lambda_rt_t *rt, char **out_request_id, buf_t *out_payload) {
    *out_request_id = NULL;
    out_payload->len = 0;

    if (ensure_connected(rt) != 0) return -1;

    char req[512];
    int n = snprintf(req, sizeof(req),
        "GET /2018-06-01/runtime/invocation/next HTTP/1.1\r\n"
        "Host: %s\r\n"
        "Connection: keep-alive\r\n"
        "\r\n", rt->runtime_api);
    if (send_all(rt->sock, req, (size_t)n) != 0) {
        close_connection(rt);
        return -1;
    }

    int status = 0;
    if (read_response(rt->sock, &status, out_request_id, out_payload) != 0 ||
        status / 100 != 2) {
        close_connection(rt);
        return -1;
    }
    return 0;
}

static int post_helper(lambda_rt_t *rt, const char *path,
                       const char *body, size_t body_n) {
    if (ensure_connected(rt) != 0) return -1;

    char head[512];
    int n = snprintf(head, sizeof(head),
        "POST %s HTTP/1.1\r\n"
        "Host: %s\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %zu\r\n"
        "Connection: keep-alive\r\n"
        "\r\n", path, rt->runtime_api, body_n);
    if (send_all(rt->sock, head, (size_t)n) != 0) goto err;
    if (body_n && send_all(rt->sock, body, body_n) != 0) goto err;

    int status = 0;
    char *rid = NULL;
    buf_t resp; buf_init(&resp);
    if (read_response(rt->sock, &status, &rid, &resp) != 0) {
        buf_free(&resp); free(rid); goto err;
    }
    free(rid);
    buf_free(&resp);
    if (status / 100 != 2) {
        LOG("runtime POST %s -> %d", path, status);
        return -1;
    }
    return 0;
err:
    close_connection(rt);
    return -1;
}

int lambda_rt_respond(lambda_rt_t *rt, const char *request_id,
                      const char *body, size_t body_n) {
    char path[256];
    snprintf(path, sizeof(path), "/2018-06-01/runtime/invocation/%s/response", request_id);
    return post_helper(rt, path, body, body_n);
}

int lambda_rt_error(lambda_rt_t *rt, const char *request_id,
                    const char *error_type, const char *message) {
    char path[256];
    if (request_id) {
        snprintf(path, sizeof(path), "/2018-06-01/runtime/invocation/%s/error", request_id);
    } else {
        snprintf(path, sizeof(path), "/2018-06-01/runtime/init/error");
    }
    char body[1024];
    int n = snprintf(body, sizeof(body),
        "{\"errorType\":\"%s\",\"errorMessage\":\"%s\"}",
        error_type ? error_type : "Runtime.Error",
        message ? message : "");
    return post_helper(rt, path, body, (size_t)n);
}
