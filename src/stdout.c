#include "stdout.h"
#include "typing.h"
#include "vargs.h"

#define SUBSTITUTION_BUFFER_SIZE 256
#define SUBSTITUTION_MARKER '%'
#define SUBSTITUTION_STRING 's'
#define SUBSTITUTION_HEX 'x'
#define SUBSTITUTION_HEX_LENGTH 18

void substitute_string(u64 *offset, char *buffer, const char *src) {
  // copy the string until EOS or buffer is full
  while (*src != EOS && *offset < SUBSTITUTION_BUFFER_SIZE) {
    buffer[(*offset)++] = *src++;
  }
}

void substitute_hex(u64 *offset, char *buffer, u64 value) {
  const char *chars = "0123456789abcdef";

  // copy the hex representation only if there's enough space
  if (*offset + SUBSTITUTION_HEX_LENGTH < SUBSTITUTION_BUFFER_SIZE) {
    buffer[(*offset)++] = '0';
    buffer[(*offset)++] = 'x';

    for (int i = 15; i >= 0; i--) {
      buffer[(*offset)++] = chars[(value >> (i * 4)) & 0xf];
    }
  }
}

i64 stdout_printf(const char *fmt, ...) {
  u8 vargs_offset = 0;
  void *vargs[VARGS_MAX];

  u64 buffer_offset = 0;
  char buffer[SUBSTITUTION_BUFFER_SIZE];

  // initialize variable argument list
  vargs_init(vargs);

  // handle the format string
  while (*fmt != EOS && buffer_offset < SUBSTITUTION_BUFFER_SIZE) {
    if (*fmt == SUBSTITUTION_MARKER) {
      switch (*++fmt) {
        case SUBSTITUTION_STRING:
          substitute_string(&buffer_offset, buffer, vargs[vargs_offset++]);
          break;
        case SUBSTITUTION_HEX:
          substitute_hex(&buffer_offset, buffer, vargs[vargs_offset++]);
          break;
      }

      // continue looping
      fmt++;
      continue;
    }

    // append regular character
    buffer[buffer_offset++] = *fmt++;
  }

  // print the final buffer
  return stdout_print(buffer_offset, buffer);
}
