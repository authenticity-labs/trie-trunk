#ifndef TRUNK_NATIVE_MEMORY_H
#define TRUNK_NATIVE_MEMORY_H
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
void *trunk_memory_new(uint64_t);
void trunk_memory_free(void *);
uint64_t trunk_memory_used(const void *);
uint64_t trunk_memory_peak(const void *);
void *trunk_buffer_new(const void *);
uint8_t *trunk_buffer_resize(void *, size_t);
void trunk_buffer_free(void *);
void *trunk_arena_new(void);
void *trunk_arena_pin(const void *);
void trunk_arena_free(void *);
bool trunk_arena_adopt(const void *, const void *);
void *trunk_buffer_seal(const void *, const uint8_t *, size_t, uint32_t, uint32_t);
uint8_t *trunk_buffer_data(const void *);
size_t trunk_buffer_len(const void *);
#endif
