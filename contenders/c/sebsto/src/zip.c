#include "zip.h"

#include <string.h>
#include <zlib.h>

void zip_writer_init(zip_writer_t *z, zip_write_fn fn, void *user) {
    memset(z, 0, sizeof(*z));
    z->write_fn = fn;
    z->user = user;
}

void zip_writer_free(zip_writer_t *z) {
    for (size_t i = 0; i < z->n_entries; i++) free(z->entries[i].name);
    free(z->entries);
    memset(z, 0, sizeof(*z));
}

static int z_write(zip_writer_t *z, const uint8_t *data, size_t n) {
    if (z->write_error) return z->write_error;
    int rc = z->write_fn(z->user, data, n, 0);
    if (rc) z->write_error = rc;
    else z->offset += n;
    return rc;
}

static int emit_local_header(zip_writer_t *z, const zip_entry_t *e) {
    /*
     *   PK\x03\x04
     *   version-needed   (u16) 20  (no ZIP64 here — entry sizes fit in u32)
     *   gp flags         (u16) 0
     *   method           (u16) 0  (STORE)
     *   mod time/date    (u16+u16) 0
     *   crc32            (u32)
     *   compressed size  (u32)
     *   uncompressed size(u32)
     *   name length      (u16)
     *   extra length     (u16) 0
     *   <name bytes>
     */
    uint8_t hdr[30];
    memcpy(hdr, "PK\x03\x04", 4);
    hdr[4] = 20; hdr[5] = 0;
    hdr[6] = 0;  hdr[7] = 0;
    hdr[8] = 0;  hdr[9] = 0;
    hdr[10] = hdr[11] = hdr[12] = hdr[13] = 0;
    uint32_t crc = e->crc32;
    hdr[14] = (uint8_t)crc; hdr[15] = (uint8_t)(crc >> 8);
    hdr[16] = (uint8_t)(crc >> 16); hdr[17] = (uint8_t)(crc >> 24);
    uint32_t sz = (uint32_t)e->size;
    hdr[18] = (uint8_t)sz; hdr[19] = (uint8_t)(sz >> 8);
    hdr[20] = (uint8_t)(sz >> 16); hdr[21] = (uint8_t)(sz >> 24);
    hdr[22] = (uint8_t)sz; hdr[23] = (uint8_t)(sz >> 8);
    hdr[24] = (uint8_t)(sz >> 16); hdr[25] = (uint8_t)(sz >> 24);
    uint16_t nl = (uint16_t)strlen(e->name);
    hdr[26] = (uint8_t)nl; hdr[27] = (uint8_t)(nl >> 8);
    hdr[28] = 0; hdr[29] = 0;
    if (z_write(z, hdr, 30)) return z->write_error;
    if (z_write(z, (const uint8_t *)e->name, nl)) return z->write_error;
    return 0;
}

int zip_writer_add(zip_writer_t *z, const char *name, const uint8_t *data, size_t n) {
    if (z->write_error) return z->write_error;
    if (z->n_entries == z->cap_entries) {
        z->cap_entries = z->cap_entries ? z->cap_entries * 2 : 4096;
        z->entries = (zip_entry_t *)xrealloc(z->entries, z->cap_entries * sizeof(*z->entries));
    }
    zip_entry_t *e = &z->entries[z->n_entries++];
    e->name = xstrdup(name);
    e->size = n;
    e->crc32 = crc32(0L, data, (uInt)n);
    e->local_header_off = z->offset;

    if (emit_local_header(z, e)) return z->write_error;
    if (n) {
        if (z_write(z, data, n)) return z->write_error;
    }
    return 0;
}

static int need_zip64(uint64_t v) { return v >= 0xFFFFFFFFULL; }

static int emit_central_dir_entry(zip_writer_t *z, const zip_entry_t *e) {
    int z64_size = need_zip64(e->size);
    int z64_off  = need_zip64(e->local_header_off);
    int any_z64  = z64_size || z64_off;

    uint16_t name_len = (uint16_t)strlen(e->name);
    uint16_t extra_len = 0;
    if (any_z64) {
        extra_len = (uint16_t)(4 + (z64_size ? 16 : 0) + (z64_off ? 8 : 0));
    }

    uint8_t hdr[46];
    memcpy(hdr, "PK\x01\x02", 4);
    hdr[4] = 45; hdr[5] = 0;
    hdr[6] = (uint8_t)(any_z64 ? 45 : 20); hdr[7] = 0;
    hdr[8] = 0;  hdr[9]  = 0;
    hdr[10] = 0; hdr[11] = 0;
    hdr[12] = 0; hdr[13] = 0; hdr[14] = 0; hdr[15] = 0;

    uint32_t crc = e->crc32;
    hdr[16] = (uint8_t)crc; hdr[17] = (uint8_t)(crc >> 8);
    hdr[18] = (uint8_t)(crc >> 16); hdr[19] = (uint8_t)(crc >> 24);

    uint32_t csize_w = z64_size ? 0xFFFFFFFFu : (uint32_t)e->size;
    uint32_t usize_w = csize_w;
    hdr[20] = (uint8_t)csize_w; hdr[21] = (uint8_t)(csize_w >> 8);
    hdr[22] = (uint8_t)(csize_w >> 16); hdr[23] = (uint8_t)(csize_w >> 24);
    hdr[24] = (uint8_t)usize_w; hdr[25] = (uint8_t)(usize_w >> 8);
    hdr[26] = (uint8_t)(usize_w >> 16); hdr[27] = (uint8_t)(usize_w >> 24);

    hdr[28] = (uint8_t)name_len; hdr[29] = (uint8_t)(name_len >> 8);
    hdr[30] = (uint8_t)extra_len; hdr[31] = (uint8_t)(extra_len >> 8);
    hdr[32] = 0; hdr[33] = 0;
    hdr[34] = 0; hdr[35] = 0;
    hdr[36] = 0; hdr[37] = 0;
    hdr[38] = 0; hdr[39] = 0; hdr[40] = 0; hdr[41] = 0;

    uint32_t off_w = z64_off ? 0xFFFFFFFFu : (uint32_t)e->local_header_off;
    hdr[42] = (uint8_t)off_w; hdr[43] = (uint8_t)(off_w >> 8);
    hdr[44] = (uint8_t)(off_w >> 16); hdr[45] = (uint8_t)(off_w >> 24);

    if (z_write(z, hdr, 46)) return z->write_error;
    if (z_write(z, (const uint8_t *)e->name, name_len)) return z->write_error;

    if (any_z64) {
        uint8_t extra[28];
        size_t off = 0;
        uint16_t pl_size = (uint16_t)((z64_size ? 16 : 0) + (z64_off ? 8 : 0));
        extra[off++] = 0x01; extra[off++] = 0x00;
        extra[off++] = (uint8_t)pl_size; extra[off++] = (uint8_t)(pl_size >> 8);
        if (z64_size) {
            uint64_t s = e->size;
            for (int i = 0; i < 8; i++) extra[off++] = (uint8_t)(s >> (8 * i));
            for (int i = 0; i < 8; i++) extra[off++] = (uint8_t)(s >> (8 * i));
        }
        if (z64_off) {
            uint64_t o = e->local_header_off;
            for (int i = 0; i < 8; i++) extra[off++] = (uint8_t)(o >> (8 * i));
        }
        if (z_write(z, extra, off)) return z->write_error;
    }
    return 0;
}

int zip_writer_finish(zip_writer_t *z) {
    if (z->write_error) {
        z->write_fn(z->user, NULL, 0, 1);
        return z->write_error;
    }

    uint64_t cd_offset = z->offset;
    for (size_t i = 0; i < z->n_entries; i++) {
        if (emit_central_dir_entry(z, &z->entries[i])) {
            z->write_fn(z->user, NULL, 0, 1);
            return z->write_error;
        }
    }
    uint64_t cd_size = z->offset - cd_offset;

    int z64 = need_zip64(z->n_entries) || need_zip64(cd_offset) || need_zip64(cd_size);
    if (z64) {
        uint8_t z64eocd[56];
        memcpy(z64eocd, "PK\x06\x06", 4);
        uint64_t rec_size = 44;
        for (int i = 0; i < 8; i++) z64eocd[4 + i] = (uint8_t)(rec_size >> (8 * i));
        z64eocd[12] = 45; z64eocd[13] = 0;
        z64eocd[14] = 45; z64eocd[15] = 0;
        memset(z64eocd + 16, 0, 8);
        uint64_t cnt = z->n_entries;
        for (int i = 0; i < 8; i++) z64eocd[24 + i] = (uint8_t)(cnt >> (8 * i));
        for (int i = 0; i < 8; i++) z64eocd[32 + i] = (uint8_t)(cnt >> (8 * i));
        for (int i = 0; i < 8; i++) z64eocd[40 + i] = (uint8_t)(cd_size >> (8 * i));
        for (int i = 0; i < 8; i++) z64eocd[48 + i] = (uint8_t)(cd_offset >> (8 * i));
        if (z_write(z, z64eocd, 56)) {
            z->write_fn(z->user, NULL, 0, 1);
            return z->write_error;
        }

        uint8_t loc[20];
        memcpy(loc, "PK\x06\x07", 4);
        loc[4] = 0; loc[5] = 0; loc[6] = 0; loc[7] = 0;
        uint64_t z64eocd_off = z->offset - 56;
        for (int i = 0; i < 8; i++) loc[8 + i] = (uint8_t)(z64eocd_off >> (8 * i));
        loc[16] = 1; loc[17] = 0; loc[18] = 0; loc[19] = 0;
        if (z_write(z, loc, 20)) {
            z->write_fn(z->user, NULL, 0, 1);
            return z->write_error;
        }
    }

    uint8_t eocd[22];
    memcpy(eocd, "PK\x05\x06", 4);
    eocd[4] = 0; eocd[5] = 0; eocd[6] = 0; eocd[7] = 0;
    uint16_t cnt16 = z->n_entries > 0xFFFF ? 0xFFFF : (uint16_t)z->n_entries;
    eocd[8]  = (uint8_t)cnt16; eocd[9]  = (uint8_t)(cnt16 >> 8);
    eocd[10] = (uint8_t)cnt16; eocd[11] = (uint8_t)(cnt16 >> 8);
    uint32_t cd_size32  = need_zip64(cd_size)  ? 0xFFFFFFFFu : (uint32_t)cd_size;
    uint32_t cd_off32   = need_zip64(cd_offset) ? 0xFFFFFFFFu : (uint32_t)cd_offset;
    eocd[12] = (uint8_t)cd_size32; eocd[13] = (uint8_t)(cd_size32 >> 8);
    eocd[14] = (uint8_t)(cd_size32 >> 16); eocd[15] = (uint8_t)(cd_size32 >> 24);
    eocd[16] = (uint8_t)cd_off32; eocd[17] = (uint8_t)(cd_off32 >> 8);
    eocd[18] = (uint8_t)(cd_off32 >> 16); eocd[19] = (uint8_t)(cd_off32 >> 24);
    eocd[20] = 0; eocd[21] = 0;

    /* Last write: signal EOF to the consumer along with the EOCD bytes. */
    if (z->write_fn(z->user, eocd, 22, 1) != 0) {
        if (!z->write_error) z->write_error = -1;
        return z->write_error;
    }
    z->offset += 22;
    return 0;
}
