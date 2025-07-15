#include "stdout.h"
#include "typing.h"
#include "vargs.h"
#include "sys.h"

#define SUBSTITUTION_BUFFER_SIZE 256
#define SUBSTITUTION_MARKER '%'
#define SUBSTITUTION_STRING 's'

#define SUBSTITUTION_HEX 'x'
#define SUBSTITUTION_HEX_LEN 18
#define SUBSTITUTION_HEX_ALPHABET_LEN 16
#define SUBSTITUTION_HEX_ALPHABET "0123456789abcdef"

static void substitute_string(u64 *offset, char *buffer, const char *src) {
  // copy the string until EOS or buffer is full
  while (*src != EOS && *offset < SUBSTITUTION_BUFFER_SIZE) {
    buffer[(*offset)++] = *src++;
  }
}

static void substitute_hex(u64 *offset, char *buffer, u64 value) {
  const char *chars = SUBSTITUTION_HEX_ALPHABET;

  // copy the hex representation only if there's enough space
  if (*offset + SUBSTITUTION_HEX_LEN < SUBSTITUTION_BUFFER_SIZE) {
    buffer[(*offset)++] = '0';
    buffer[(*offset)++] = 'x';

    for (i32 i = SUBSTITUTION_HEX_ALPHABET_LEN - 1; i >= 0; i--) {
      buffer[(*offset)++] = chars[(value >> (i * 4)) & 0x0f];
    }
  }
}

static i64 flush_buffer(const char *buffer, u64 length) {
  i64 written;

  while (length > 0) {
    // write the buffer to stdout
    if ((written = sys_write(1, buffer, length)) < 0) {
      return written; // return error if write failed
    }

    // adjust the length and buffer pointer
    length -= written;
    buffer += written;
  }

  // success
  return 0;
}

void printf(const char *fmt, ...) {
  u8 vargs_offset = 0;
  void *vargs[VARGS_MAX];

  u64 buffer_offset = 0;
  char buffer[SUBSTITUTION_BUFFER_SIZE];

  // initialize variable argument list
  vargs_init(vargs);

  // handle the format string
  while (*fmt != EOS && buffer_offset < SUBSTITUTION_BUFFER_SIZE) {
    if (*fmt == SUBSTITUTION_MARKER && vargs_offset < VARGS_MAX) {
      switch (*++fmt) {
        case SUBSTITUTION_STRING:
          substitute_string(&buffer_offset, buffer, vargs[vargs_offset++]);
          break;
        case SUBSTITUTION_HEX:
          substitute_hex(&buffer_offset, buffer, (u64)vargs[vargs_offset++]);
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
  if (flush_buffer(buffer, buffer_offset) < 0) {
    sys_exit(1);
  }
}
