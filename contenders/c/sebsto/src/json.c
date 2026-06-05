#include "json.h"

#include <ctype.h>
#include <string.h>

static const char *skip_ws(const char *p, const char *end) {
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    return p;
}

char *json_extract_string(const char *body, size_t n, const char *key) {
    /* The Lambda runtime sends us a flat object like
     * {"bucket_name":"...","files_prefix":"files","archive_key":"archives/..."}.
     * A naive scan is sufficient here. */
    char pat[128];
    int kn = snprintf(pat, sizeof(pat), "\"%s\"", key);
    if (kn <= 0 || kn >= (int)sizeof(pat)) return NULL;

    const char *end = body + n;
    const char *p = body;
    while (p + (size_t)kn <= end) {
        const char *m = (const char *)memmem(p, (size_t)(end - p), pat, (size_t)kn);
        if (!m) return NULL;
        p = m + kn;
        p = skip_ws(p, end);
        if (p < end && *p == ':') break;
    }
    if (p >= end || *p != ':') return NULL;
    p++;
    p = skip_ws(p, end);
    if (p >= end || *p != '"') return NULL;
    p++;

    buf_t out; buf_init(&out);
    while (p < end && *p != '"') {
        if (*p == '\\' && p + 1 < end) {
            char esc = p[1];
            if (esc == '"' || esc == '\\' || esc == '/') {
                buf_append_u8(&out, (uint8_t)esc);
            } else if (esc == 'n') buf_append_u8(&out, '\n');
            else if (esc == 't') buf_append_u8(&out, '\t');
            else buf_append_u8(&out, (uint8_t)esc);
            p += 2;
            continue;
        }
        buf_append_u8(&out, (uint8_t)*p);
        p++;
    }
    buf_append_u8(&out, 0);
    return (char *)out.data;
}
