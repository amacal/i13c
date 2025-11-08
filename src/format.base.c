#include "format.base.h"
#include "error.h"
#include "typing.h"
#include "vargs.h"

#define SUBSTITUTION_MARKER '%'
#define SUBSTITUTION_STRING 's'
#define SUBSTITUTION_RESULT 'r'

#define SUBSTITUTION_ENDLESS 'e'
#define SUBSTITUTION_ENDLESS_MAX 64

#define SUBSTITUTION_INDENT 'i'
#define SUBSTITUTION_INDENT_CHARACTER ' '

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

static i64 substitute_string(struct format_context *ctx, const char **src) {
  u64 available;

  // calculate available space in the buffer
  available = ctx->buffer_size - ctx->buffer_offset;

  // copy the string until EOS or buffer is full
  while (**src != EOS && ctx->buffer_offset < ctx->buffer_size && available > 0) {
    // append the character to the buffer
    ctx->buffer[ctx->buffer_offset++] = *(*src)++;
    available--;
  }

  // report an error if the string was not fully copied
  if (**src != EOS && ctx->buffer_offset >= ctx->buffer_size) {
    return FORMAT_ERROR_BUFFER_TOO_SMALL;
  }

  // success
  return 0;
}

static i64 substitute_unknown(struct format_context *ctx, char symbol) {
  // check if there's enough space for the substitution
  if (ctx->buffer_offset + 1 >= ctx->buffer_size) {
    return FORMAT_ERROR_BUFFER_TOO_SMALL;
  }

  // append the substitution marker and the unknown symbol
  ctx->buffer[ctx->buffer_offset++] = SUBSTITUTION_MARKER;
  ctx->buffer[ctx->buffer_offset++] = symbol;

  // success
  return 0;
}

static i64 substitute_hex(struct format_context *ctx, u64 value) {
  i32 index;
  const char *chars = SUBSTITUTION_HEX_ALPHABET;

  // check if there's enough space for the substitution
  if (ctx->buffer_offset + SUBSTITUTION_HEX_LEN >= ctx->buffer_size) {
    return FORMAT_ERROR_BUFFER_TOO_SMALL;
  }

  // copy the hex header
  ctx->buffer[ctx->buffer_offset++] = '0';
  ctx->buffer[ctx->buffer_offset++] = 'x';

  // copy the hex representation
  for (index = SUBSTITUTION_HEX_ALPHABET_LEN - 1; index >= 0; index--) {
    ctx->buffer[ctx->buffer_offset++] = chars[(value >> (index * 4)) & 0x0f];
  }

  // success
  return 0;
}

static i64 substitute_indent(struct format_context *ctx, u64 indent) {
  // check if there's enough space for the substitution
  if (ctx->buffer_offset + indent >= ctx->buffer_size) {
    return FORMAT_ERROR_BUFFER_TOO_SMALL;
  }

  // append indent spaces
  while (indent-- > 0) {
    ctx->buffer[ctx->buffer_offset++] = SUBSTITUTION_INDENT_CHARACTER;
  }

  // success
  return 0;
}

static i64 substitute_marker(struct format_context *ctx) {
  // check if there's enough space for the substitution
  if (ctx->buffer_offset >= ctx->buffer_size) {
    return FORMAT_ERROR_BUFFER_TOO_SMALL;
  }

  // append the substitution marker
  ctx->buffer[ctx->buffer_offset++] = SUBSTITUTION_MARKER;

  // success
  return 0;
}

static i64 substitute_decimal(struct format_context *ctx, i64 value) {
  i32 index;
  u64 value_abs;

  char tmp[SUBSTITUTION_DECIMAL_LEN];
  const char *chars = SUBSTITUTION_DECIMAL_ALPHABET;

  // check if there's enough space for the substitution
  if (ctx->buffer_offset + SUBSTITUTION_DECIMAL_LEN >= ctx->buffer_size) {
    return FORMAT_ERROR_BUFFER_TOO_SMALL;
  }

  // default
  index = 0;
  value_abs = value < 0 ? -(u64)value : (u64)value;

  // if the value is negative, add a minus sign
  if (value < 0) {
    ctx->buffer[ctx->buffer_offset++] = '-';
  }

  // extract digit by digit
  while (value_abs > 0) {
    tmp[index++] = chars[value_abs % SUBSTITUTION_DECIMAL_ALPHABET_LEN];
    value_abs /= SUBSTITUTION_DECIMAL_ALPHABET_LEN;
  }

  // if no digits were extracted, add a zero
  if (index == 0) {
    tmp[index++] = '0';
  }

  // copy the decimal representation only if there's enough space
  while (index > 0) {
    ctx->buffer[ctx->buffer_offset++] = tmp[--index];
  }

  // success
  return 0;
}

static i64 substitute_result(struct format_context *ctx, i64 result) {
  const char *text;

  // check if there's enough space for the substitution
  if (ctx->buffer_offset + ERROR_NAME_MAX_LENGTH + 1 + SUBSTITUTION_DECIMAL_LEN >= ctx->buffer_size) {
    return FORMAT_ERROR_BUFFER_TOO_SMALL;
  }

  // delegate calls to other substitutions
  if (result < 0 && result <= ERROR_BASE) {
    // output the error name
    text = res2str(result);
    substitute_string(ctx, &text);

    // followed by the error code
    text = "#";
    substitute_string(ctx, &text);
    substitute_decimal(ctx, res2off(result));
  } else {
    substitute_decimal(ctx, result);
  }

  // success
  return 0;
}

static i64 substitute_ascii(struct format_context *ctx, const char **src, u64 *size) {
  char ch;
  u64 available;

  // calculate available space in the buffer
  available = ctx->buffer_size - ctx->buffer_offset;

  // copy the string until EOS
  while (*size > 0 && available > 0) {
    (*size)--;
    ch = *(*src)++;

    // if the character is not in the ASCII range, use fallback
    if (ch < SUBSTITUTION_ASCII_MIN || ch > SUBSTITUTION_ASCII_MAX) {
      ch = SUBSTITUTION_ASCII_FALLBACK;
    }

    // append the character to the buffer
    ctx->buffer[ctx->buffer_offset++] = ch;
    available--;
  }

  // report an error if the string was not fully copied
  if (*size > 0 && ctx->buffer_offset >= ctx->buffer_size) {
    return FORMAT_ERROR_BUFFER_TOO_SMALL;
  }

  // success
  return 0;
}

static i64 substitute_endless(struct format_context *ctx, const char *src, i64 *count) {
  char ch;
  u64 available;
  const char *current;

  while (*count > 0) {
    // calculate available space in the buffer
    current = src;
    available = ctx->buffer_size - ctx->buffer_offset;

    // if there's not enough space for the substitution
    if (available < SUBSTITUTION_ENDLESS_MAX) {
      return FORMAT_ERROR_BUFFER_TOO_SMALL;
    }

    // copy the string until EOS
    while (*current && available > 0) {
      ch = *(current++);

      // append the character to the buffer
      ctx->buffer[ctx->buffer_offset++] = ch;
      available--;
    }

    // decrease the count
    (*count)--;
  }

  // success
  return 0;
}

i64 format(struct format_context *ctx) {
  i64 result;

  // default values
  result = 0;

  // handle the format string
  while (*ctx->fmt != EOS && result == 0) {

    // handle two consecutive substitution markers
    if (*ctx->fmt == SUBSTITUTION_MARKER && ctx->vargs_offset + 1 < ctx->vargs_max) {
      switch (*(ctx->fmt + 1)) {
        case SUBSTITUTION_ASCII:
          result = substitute_ascii(ctx, (const char **)ctx->vargs + ctx->vargs_offset,
                                    (u64 *)ctx->vargs + ctx->vargs_offset + 1);
          if (result == FORMAT_ERROR_BUFFER_TOO_SMALL) continue;

          ctx->vargs_offset += 2;
          ctx->fmt += 2;
          continue;

        case SUBSTITUTION_ENDLESS:
          result = substitute_endless(ctx, (const char *)ctx->vargs[ctx->vargs_offset],
                                      (i64 *)ctx->vargs + ctx->vargs_offset + 1);
          if (result == FORMAT_ERROR_BUFFER_TOO_SMALL) continue;

          ctx->vargs_offset += 2;
          ctx->fmt += 2;
          continue;
      }
    }

    // handle single substitution markers
    if (*ctx->fmt == SUBSTITUTION_MARKER && ctx->vargs_offset < ctx->vargs_max) {
      switch (*(ctx->fmt + 1)) {
        case SUBSTITUTION_STRING:
          result = substitute_string(ctx, (const char **)ctx->vargs + ctx->vargs_offset);
          if (result == FORMAT_ERROR_BUFFER_TOO_SMALL) continue;
          break;

        case SUBSTITUTION_HEX:
          result = substitute_hex(ctx, (u64)ctx->vargs[ctx->vargs_offset]);
          if (result == FORMAT_ERROR_BUFFER_TOO_SMALL) continue;
          break;

        case SUBSTITUTION_INDENT:
          result = substitute_indent(ctx, (u64)ctx->vargs[ctx->vargs_offset]);
          if (result == FORMAT_ERROR_BUFFER_TOO_SMALL) continue;
          break;

        case SUBSTITUTION_DECIMAL:
          result = substitute_decimal(ctx, (i64)ctx->vargs[ctx->vargs_offset]);
          if (result == FORMAT_ERROR_BUFFER_TOO_SMALL) continue;
          break;

        case SUBSTITUTION_RESULT:
          result = substitute_result(ctx, (i64)ctx->vargs[ctx->vargs_offset]);
          if (result == FORMAT_ERROR_BUFFER_TOO_SMALL) continue;
          break;

        case SUBSTITUTION_MARKER:
          result = substitute_marker(ctx);
          if (result == FORMAT_ERROR_BUFFER_TOO_SMALL) continue;
          break;

        case EOS:
          result = substitute_marker(ctx);
          if (result == FORMAT_ERROR_BUFFER_TOO_SMALL) continue;
          goto result;

        default:
          result = substitute_unknown(ctx, *(ctx->fmt + 1));
          if (result == FORMAT_ERROR_BUFFER_TOO_SMALL) continue;
          break;
      }

      // move context
      ctx->fmt += 2;
      ctx->vargs_offset += 1;

      // continue looping
      continue;
    }

    // check if there's enough space for a regular character
    if (ctx->buffer_offset >= ctx->buffer_size) {
      result = FORMAT_ERROR_BUFFER_TOO_SMALL;
    }

    // append regular character
    if (result == 0) {
      ctx->buffer[ctx->buffer_offset++] = *ctx->fmt++;
    }
  }

result:

  // append EOS to the buffer, there's space
  ctx->buffer[ctx->buffer_offset] = EOS;

  // return error or success
  return result < 0 ? result : ctx->buffer_offset;
}

#if defined(I13C_TESTS)

static void can_format_without_substitutions() {
  char buffer[32];
  struct format_context ctx;
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "Hello, World!";
  ctx.vargs = NULL;
  ctx.buffer_offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_size = sizeof(buffer) - 2;

  // format a simple string
  offset = format(&ctx);

  // assert the result
  assert(offset == 13, "should write 13 bytes");
  assert_eq_str(buffer, "Hello, World!", "should format 'Hello, World!'");
}

static void can_format_with_string_substitution() {
  char buffer[32];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "Hello, %s!";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // not initialize all vargs
  vargs[0] = "World";

  // format a string with substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 13, "should write 13 bytes");
  assert_eq_str(buffer, "Hello, World!", "should format 'Hello, World!'");
}

static void can_format_with_hex_substitution() {
  char buffer[32];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "Value: %x";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // not initialize all vargs
  vargs[0] = (void *)(u64)0x1234abcd01020304;

  // format a string with hex substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 25, "should write 25 bytes");
  assert_eq_str(buffer, "Value: 0x1234abcd01020304", "should format 'Value: 0x1234abcd01020304'");
}

static void can_format_with_decimal_positive() {
  char buffer[32];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "Value: %d";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // not initialize all vargs
  vargs[0] = (void *)(i64)123456789;

  // format a string with decimal substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 16, "should write 16 bytes");
  assert_eq_str(buffer, "Value: 123456789", "should format 'Value: 123456789'");
}

static void can_format_with_decimal_negative() {
  char buffer[32];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "Value: %d";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // not initialize all vargs
  vargs[0] = (void *)(i64)-123456789;

  // format a string with decimal substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 17, "should write 17 bytes");
  assert_eq_str(buffer, "Value: -123456789", "should format 'Value: -123456789'");
}

static void can_format_with_decimal_int64_min() {
  char buffer[32];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "Value: %d";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // not initialize all vargs
  vargs[0] = (void *)(i64)(-9223372036854775807ll - 1);

  // format a string with decimal substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 27, "should write 27 bytes");
  assert_eq_str(buffer, "Value: -9223372036854775808", "should format 'Value: -9223372036854775808'");
}

static void can_format_with_indent_substitution() {
  char buffer[32];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "%iabcdef";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // not initialize all vargs
  vargs[0] = (void *)(u64)4;

  // format a string with indent substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 10, "should write 10 bytes");
  assert_eq_str(buffer, "    abcdef", "should format '    abcdef'");
}

static void can_format_with_ascii_substitution() {
  char buffer[32];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "ASCII: %a";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // not initialize all vargs
  vargs[0] = "Hello, Ślimak!";
  vargs[1] = (void *)(u64)15;

  // format a string with ascii substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 22, "should write 22 bytes");
  assert_eq_str(buffer, "ASCII: Hello, ..limak!", "should format 'ASCII: Hello, ..limak!'");
}

static void can_format_with_endless_substitution() {
  char buffer[512];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "Endless: %e";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // not initialize all vargs
  vargs[0] = "Hello!";
  vargs[1] = (void *)(u64)3;

  // format a string with endless substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 27, "should write 27 bytes");
  assert_eq_str(buffer, "Endless: Hello!Hello!Hello!", "should format 'Endless: Hello!x3'");
}

static void can_format_with_result_substitution() {
  char buffer[64];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "Result: %r";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // not initialize all vargs
  vargs[0] = (void *)(MALLOC_ERROR_BASE - 0x05);

  // format a string with result substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 16, "should write 16 bytes");
  assert_eq_str(buffer, "Result: malloc#5", "should format 'Result: malloc#5'");
}

static void can_format_with_unknown_substitution() {
  char buffer[32];
  struct format_context ctx;
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "Unknown: %z";
  ctx.vargs = NULL;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // format a string with unknown substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 11, "should write 11 bytes");
  assert_eq_str(buffer, "Unknown: %z", "should format 'Unknown: %z'");
}

static void can_format_with_percent_escape() {
  char buffer[32];
  struct format_context ctx;
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "50%% done%";
  ctx.vargs = NULL;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // format a string
  offset = format(&ctx);

  // assert the result
  assert(offset == 9, "should write 9 bytes");
  assert_eq_str(buffer, "50% done%", "should format '50% done%'");
}

static void can_detect_overflow_in_plain_text() {
  char buffer[16];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "This is a very long string.";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // format a string that exceeds the buffer size
  offset = format(&ctx);

  // assert the result
  assert(offset == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return an error for buffer too small");
  assert(ctx.buffer_offset == sizeof(buffer) - 2, "should write up to the buffer size");
  assert_eq_str(buffer, "This is a very", "should format 'This is a very'");

  // clear the offset
  ctx.buffer_offset = 0;

  // second round should continue formatting
  offset = format(&ctx);

  // assert the result
  assert(offset == 13, "should return number of bytes written");
  assert(ctx.buffer_offset == 13, "should write up to the buffer size");
  assert_eq_str(buffer, " long string.", "should format ' long string.'");
}

static void can_detect_overflow_in_string_substitution() {
  char buffer[16];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "This is a %s.";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // initialize vargs
  vargs[0] = "very long string";

  // format a string that exceeds the buffer size
  offset = format(&ctx);

  // assert the result
  assert(offset == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return an error for buffer too small");
  assert(ctx.buffer_offset == sizeof(buffer) - 2, "should write up to the buffer size");
  assert_eq_str(buffer, "This is a very", "should format 'This is a very'");

  // clear the offset
  ctx.buffer_offset = 0;

  // second round should continue formatting
  offset = format(&ctx);

  // assert the result
  assert(offset == 13, "should return number of bytes written");
  assert(ctx.buffer_offset == 13, "should write up to the buffer size");
  assert_eq_str(buffer, " long string.", "should format ' long string.'");
}

static void can_detect_overflow_in_hex_substitution() {
  char buffer[22];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "Value: %x";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // not initialize all vargs
  vargs[0] = (void *)(u64)0x1234abcd01020304;

  // format a string with hex substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return an error for buffer too small");
  assert(ctx.buffer_offset == 7, "should write up to the buffer size");
  assert_eq_str(buffer, "Value: ", "should format 'Value: '");

  // clear the offset
  ctx.buffer_offset = 0;

  // second round should continue formatting
  offset = format(&ctx);

  // assert the result
  assert(offset == 18, "should return number of bytes written");
  assert(ctx.buffer_offset == 18, "should write up to the buffer size");
  assert_eq_str(buffer, "0x1234abcd01020304", "should format '0x1234abcd01020304'");
}

static void can_detect_overflow_with_two_vargs() {
  char buffer[16];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "This is %s and %s.";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // initialize vargs
  vargs[0] = "ABC";
  vargs[1] = "CDE";

  // format a string that exceeds the buffer size
  offset = format(&ctx);

  // assert the result
  assert(offset == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return an error for buffer too small");
  assert(ctx.buffer_offset == sizeof(buffer) - 2, "should write up to the buffer size");
  assert_eq_str(buffer, "This is ABC an", "should format 'This is ABC an'");

  // clear the offset
  ctx.buffer_offset = 0;

  // second round should continue formatting
  offset = format(&ctx);

  // assert the result
  assert(offset == 6, "should write 6 bytes");
  assert(ctx.buffer_offset == 6, "should write up to the buffer size");
  assert_eq_str(buffer, "d CDE.", "should format 'd CDE.'");
}

static void can_detect_overflow_in_long_substitution_1() {
  char buffer[16];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "Value: %s";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // not initialize all vargs
  vargs[0] = "This is a very long string.";

  // format a string with long substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return an error for buffer too small");
  assert(ctx.buffer_offset == sizeof(buffer) - 2, "should write up to the buffer size");
  assert_eq_str(buffer, "Value: This is", "should format 'Value: This is'");

  // clear the offset
  ctx.buffer_offset = 0;

  // second round should continue formatting
  offset = format(&ctx);

  // assert the result
  assert(offset == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return an error for buffer too small");
  assert(ctx.buffer_offset == sizeof(buffer) - 2, "should write up to the buffer size");
  assert_eq_str(buffer, " a very long s", "should format ' a very long s'");

  // clear the offset
  ctx.buffer_offset = 0;

  // third round should continue formatting
  offset = format(&ctx);

  // assert the result
  assert(offset == 6, "should write 6 bytes");
  assert(ctx.buffer_offset == 6, "should write up to the buffer size");
  assert_eq_str(buffer, "tring.", "should format 'tring.'");
}

static void can_detect_overflow_in_long_substitution_2() {
  char buffer[16];
  struct format_context ctx;
  void *vargs[VARGS_MAX];
  i64 offset = 0;

  // initialize the context
  ctx.fmt = "Value: %a";
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.vargs_max = VARGS_MAX;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = sizeof(buffer) - 2;

  // not initialize all vargs
  vargs[0] = "This is a very long string.";
  vargs[1] = (void *)(u64)26;

  // format a string with long substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return an error for buffer too small");
  assert(ctx.buffer_offset == sizeof(buffer) - 2, "should write up to the buffer size");
  assert_eq_str(buffer, "Value: This is", "should format 'Value: This is'");

  // clear the offset
  ctx.buffer_offset = 0;

  // second round should continue formatting
  offset = format(&ctx);

  // assert the result
  assert(offset == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return an error for buffer too small");
  assert(ctx.buffer_offset == sizeof(buffer) - 2, "should write up to the buffer size");
  assert_eq_str(buffer, " a very long s", "should format ' a very long s'");

  // clear the offset
  ctx.buffer_offset = 0;

  // third round should continue formatting
  offset = format(&ctx);

  // assert the result
  assert(offset == 5, "should write 5 bytes");
  assert(ctx.buffer_offset == 5, "should write up to the buffer size");
  assert_eq_str(buffer, "tring", "should format 'tring'");
}

void format_test_cases_base(struct runner_context *ctx) {
  // formatting cases
  test_case(ctx, "can format without substitutions", can_format_without_substitutions);
  test_case(ctx, "can format with string substitution", can_format_with_string_substitution);
  test_case(ctx, "can format with hex substitution", can_format_with_hex_substitution);
  test_case(ctx, "can format with decimal positive", can_format_with_decimal_positive);
  test_case(ctx, "can format with decimal negative", can_format_with_decimal_negative);
  test_case(ctx, "can format with decimal int64 min", can_format_with_decimal_int64_min);
  test_case(ctx, "can format with indent substitution", can_format_with_indent_substitution);
  test_case(ctx, "can format with ascii substitution", can_format_with_ascii_substitution);
  test_case(ctx, "can format with endless substitution", can_format_with_endless_substitution);
  test_case(ctx, "can format with result substitution", can_format_with_result_substitution);
  test_case(ctx, "can format with unknown substitution", can_format_with_unknown_substitution);
  test_case(ctx, "can format with percent escape", can_format_with_percent_escape);

  // buffer too small cases
  test_case(ctx, "can detect overflow in plain text", can_detect_overflow_in_plain_text);
  test_case(ctx, "can detect overflow in string substitution", can_detect_overflow_in_string_substitution);
  test_case(ctx, "can detect overflow in hex substitution", can_detect_overflow_in_hex_substitution);
  test_case(ctx, "can detect overflow with two vargs", can_detect_overflow_with_two_vargs);
  test_case(ctx, "can detect overflow in long substitution 1", can_detect_overflow_in_long_substitution_1);
  test_case(ctx, "can detect overflow in long substitution 2", can_detect_overflow_in_long_substitution_2);
}

#endif
