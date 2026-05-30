#include "include/ccrc32.h"

#include <stdint.h>
#include <stddef.h>

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#define CCRC32_HW 1
#endif

static uint32_t crc_table[256];
static int crc_table_inited = 0;

static void init_crc_table(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++) {
            c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        crc_table[i] = c;
    }
    crc_table_inited = 1;
}

uint32_t ccrc32_update(uint32_t crc, const uint8_t *data, size_t len) {
    crc = ~crc;

#if defined(CCRC32_HW)
    while (len >= 8) {
        uint64_t v;
        __builtin_memcpy(&v, data, 8);
        crc = __crc32d(crc, v);
        data += 8;
        len  -= 8;
    }
    if (len >= 4) {
        uint32_t v;
        __builtin_memcpy(&v, data, 4);
        crc = __crc32w(crc, v);
        data += 4;
        len  -= 4;
    }
    if (len >= 2) {
        uint16_t v;
        __builtin_memcpy(&v, data, 2);
        crc = __crc32h(crc, v);
        data += 2;
        len  -= 2;
    }
    if (len) {
        crc = __crc32b(crc, *data);
    }
#else
    if (!crc_table_inited) init_crc_table();
    while (len--) {
        crc = crc_table[(crc ^ *data++) & 0xFF] ^ (crc >> 8);
    }
#endif

    return ~crc;
}

#if defined(__linux__)
#include <malloc.h>
size_t ccrc32_mallinfo_uordblks(void) {
#if defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 33))
    struct mallinfo2 mi = mallinfo2();
    return mi.uordblks;
#else
    return 0;
#endif
}
#else
size_t ccrc32_mallinfo_uordblks(void) { return 0; }
#endif
