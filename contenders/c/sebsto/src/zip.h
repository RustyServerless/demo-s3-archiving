#ifndef DSA_ZIP_H
#define DSA_ZIP_H

#include "util.h"

/*
 * Streaming ZIP encoder. Compression method = STORE (0) since the source
 * files are already random/incompressible (matches the Rust reference).
 *
 * Output is delivered via a caller-provided callback. The encoder calls
 * write_fn(user, data, n, eof=false) zero-or-more times during entry
 * emission and then exactly once with eof=true from zip_writer_finish.
 *
 * Per-entry size fits in u32 (each source object is <= 8 MB), but the
 * archive total exceeds 4 GiB so the central directory uses ZIP64 fields
 * once running offset crosses the boundary.
 */

typedef int (*zip_write_fn)(void *user, const uint8_t *data, size_t n, int eof);

typedef struct {
    char    *name;
    uint32_t crc32;
    uint64_t size;             /* == uncompressed == compressed (STORE) */
    uint64_t local_header_off;
} zip_entry_t;

typedef struct {
    zip_write_fn  write_fn;
    void         *user;
    zip_entry_t  *entries;
    size_t        n_entries;
    size_t        cap_entries;
    uint64_t      offset;       /* total bytes written so far */
    int           write_error;
} zip_writer_t;

void zip_writer_init(zip_writer_t *z, zip_write_fn fn, void *user);
void zip_writer_free(zip_writer_t *z);

/* Append one stored entry. `data` must be exactly `n` bytes.
 * Returns 0 on success, non-zero if the underlying writer reported an error
 * (subsequent calls are no-ops). */
int zip_writer_add(zip_writer_t *z, const char *name, const uint8_t *data, size_t n);

/* Write central directory + EOCD (and ZIP64 if needed). Calls write_fn one
 * last time with eof=true. Returns 0 on success. */
int zip_writer_finish(zip_writer_t *z);

#endif
