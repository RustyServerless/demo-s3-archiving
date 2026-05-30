#ifndef CCRC32_H
#define CCRC32_H

#include <stddef.h>
#include <stdint.h>

// Updates a CRC32 (IEEE polynomial 0xEDB88320, the polynomial ZIP uses).
// The caller passes the previous CRC value (or 0 to start) and gets back
// the updated CRC. The bit-flip wrapping that ZIP/zlib expect is handled
// inside.
//
// On aarch64 this uses the ARMv8 CRC32 instructions (__crc32{b,h,w,d})
// when available. Otherwise falls back to a Slicing-by-8 software
// implementation.
uint32_t ccrc32_update(uint32_t crc, const uint8_t *data, size_t len);

#endif
