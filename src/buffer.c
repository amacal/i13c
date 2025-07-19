#include "buffer.h"
#include "typing.h"

void buffer_init(struct buffer *buffer, char *data, u64 size) {
  buffer->data = data;
  buffer->size = size;
}

void buffer_advance(struct buffer *buffer, u64 size) {
  buffer->data += size;
  buffer->size -= size;
}
