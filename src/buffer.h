#pragma once

#include "typing.h"

struct buffer {
  char *data;
  u64 size;
};

/// @brief Initializes a buffer with given data and size.
/// @param buffer Pointer to the buffer structure.
/// @param data Pointer to the data to be stored in the buffer.
/// @param size Size of the data in bytes.
extern void buffer_init(struct buffer *buffer, char *data, u64 size);

/// @brief Advances the buffer by a given size.
/// @param buffer Pointer to the buffer structure.
/// @param size Size to advance the buffer by.
extern void buffer_advance(struct buffer *buffer, u64 size);
