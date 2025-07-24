#include "stdout.h"
#include "sys.h"
#include "typing.h"
#include "vargs.h"

#define SUBSTITUTION_BUFFER_SIZE 256
#define SUBSTITUTION_MARKER '%'
#define SUBSTITUTION_STRING 's'
#define SUBSTITUTION_INTENT 'i'

#define SUBSTITUTION_ASCII 'a'
#define SUBSTITUTION_ASCII_MIN 0x20
#define SUBSTITUTION_ASCII_MAX 0x7e
#define SUBSTITUTION_ASCII_FALLBACK 0x2e

#define SUBSTITUTION_DECIMAL 'd'
#define SUBSTITUTION_DECIMAL_LEN 21
#define SUBSTITUTION_DECIMAL_ALPHABET "0123456789"
#define SUBSTITUTION_DECIMAL_ALPHABET_LEN 10

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
  i32 index;
  const char *chars = SUBSTITUTION_HEX_ALPHABET;

  // copy the hex representation only if there's enough space
  if (*offset + SUBSTITUTION_HEX_LEN < SUBSTITUTION_BUFFER_SIZE) {
    buffer[(*offset)++] = '0';
    buffer[(*offset)++] = 'x';

    for (index = SUBSTITUTION_HEX_ALPHABET_LEN - 1; index >= 0; index--) {
      buffer[(*offset)++] = chars[(value >> (index * 4)) & 0x0f];
    }
  }
}

static void substitute_intent(u64 *offset, char *buffer, u64 intent) {
  u64 index;

  // append intent spaces
  for (index = 0; index < intent && *offset < SUBSTITUTION_BUFFER_SIZE; index++) {
    buffer[(*offset)++] = ' ';
  }
}

static void substitute_decimal(u64 *offset, char *buffer, i64 value) {
  i32 index;
  i64 value_abs;

  char tmp[SUBSTITUTION_DECIMAL_LEN];
  const char *chars = SUBSTITUTION_DECIMAL_ALPHABET;

  // default
  index = 0;
  value_abs = value < 0 ? -value : value;

  // if the value is negative, add a minus sign
  if (value < 0 && *offset < SUBSTITUTION_BUFFER_SIZE) {
    buffer[(*offset)++] = '-';
  }

  // extract digit by digit
  while (value_abs > 0 && *offset < SUBSTITUTION_BUFFER_SIZE) {
    tmp[index++] = chars[value_abs % SUBSTITUTION_DECIMAL_ALPHABET_LEN];
    value_abs /= SUBSTITUTION_DECIMAL_ALPHABET_LEN;
  }

  // if no digits were extracted, add a zero
  if (index == 0) {
    tmp[index++] = '0';
  }

  // copy the decimal representation only if there's enough space
  while (index > 0 && *offset < SUBSTITUTION_BUFFER_SIZE) {
    buffer[(*offset)++] = tmp[--index];
  }
}

static void substitute_ascii(u64 *offset, char *buffer, const char *src, u64 size) {
  char ch;

  // copy the string until size or buffer is full
  while (*offset < SUBSTITUTION_BUFFER_SIZE && size > 0) {
    size--;
    ch = *src++;

    // if the character is not in the ASCII range, use fallback
    if (ch < SUBSTITUTION_ASCII_MIN || ch > SUBSTITUTION_ASCII_MAX) {
      ch = SUBSTITUTION_ASCII_FALLBACK;
    }

    // append the character to the buffer
    buffer[(*offset)++] = ch;
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

void writef(const char *fmt, ...) {
  u8 vargs_offset = 0;
  void *vargs[VARGS_MAX];

  u64 buffer_offset = 0;
  char buffer[SUBSTITUTION_BUFFER_SIZE];

  // initialize variable argument list
  vargs_init(vargs);

  // handle the format string
  while (*fmt != EOS && buffer_offset < SUBSTITUTION_BUFFER_SIZE) {

    // handle two consecutive substitution markers
    if (*fmt == SUBSTITUTION_MARKER && vargs_offset + 1 < VARGS_MAX) {
      switch (*(fmt + 1)) {
        case SUBSTITUTION_ASCII:
          substitute_ascii(&buffer_offset, buffer, (const char *)vargs[vargs_offset], (u64)vargs[vargs_offset + 1]);
          vargs_offset += 2;
          fmt += 2;
          continue;
      }
    }

    // handle single substitution markers
    if (*fmt == SUBSTITUTION_MARKER && vargs_offset < VARGS_MAX) {
      switch (*++fmt) {
        case SUBSTITUTION_STRING:
          substitute_string(&buffer_offset, buffer, vargs[vargs_offset++]);
          break;
        case SUBSTITUTION_HEX:
          substitute_hex(&buffer_offset, buffer, (u64)vargs[vargs_offset++]);
          break;
        case SUBSTITUTION_INTENT:
          substitute_intent(&buffer_offset, buffer, (u64)vargs[vargs_offset++]);
          break;
        case SUBSTITUTION_DECIMAL:
          substitute_decimal(&buffer_offset, buffer, (i64)vargs[vargs_offset++]);
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
