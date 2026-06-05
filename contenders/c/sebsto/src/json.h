#ifndef DSA_JSON_H
#define DSA_JSON_H

#include "util.h"

/* Extract the string value of `key` from a flat JSON object. Returns malloc'd
 * string (caller frees) or NULL if missing. */
char *json_extract_string(const char *body, size_t n, const char *key);

#endif
