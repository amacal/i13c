#include "stdout.h"
#include "error.h"
#include "sys.h"
#include "typing.h"
#include "vargs.h"

#define SUBSTITUTION_BUFFER_SIZE 256
#define SUBSTITUTION_BUFFER_TRIGGER 192

#define SUBSTITUTION_MARKER '%'
#define SUBSTITUTION_STRING 's'
#define SUBSTITUTION_RESULT 'r'

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

/// @brief Flush the output buffer
/// @param buffer The buffer to flush
/// @param length The length of the buffer
/// @return The number of bytes written
typedef i64 (*flush_buffer_fn)(const char *buffer, u64 length);

struct stdout_context {
  const char *fmt; // format string
  char *buffer;    // output buffer
  u32 offset;      // current offset in the buffer
  u32 buffer_size; // size of the output buffer
  void **vargs;    // variable arguments
};

static i64 substitute_string(struct stdout_context *ctx, const char **src) {
  u64 available;

  // calculate available space in the buffer
  available = ctx->buffer_size - ctx->offset;

  // copy the string until EOS or buffer is full
  while (**src != EOS && ctx->offset < ctx->buffer_size && available > 0) {
    // append the character to the buffer
    ctx->buffer[ctx->offset++] = *(*src)++;
    available--;
  }

  // report an error if the string was not fully copied
  if (**src != EOS && ctx->offset >= ctx->buffer_size) {
    return STDOUT_ERROR_BUFFER_TOO_SMALL;
  }

  // success
  return 0;
}

static i64 substitute_unknown(struct stdout_context *ctx, char symbol) {
  // check if there's enough space for the substitution
  if (ctx->offset + 1 >= ctx->buffer_size) {
    return STDOUT_ERROR_BUFFER_TOO_SMALL;
  }

  // append the substitution marker and the unknown symbol
  ctx->buffer[ctx->offset++] = SUBSTITUTION_MARKER;
  ctx->buffer[ctx->offset++] = symbol;

  // success
  return 0;
}

static i64 substitute_hex(struct stdout_context *ctx, u64 value) {
  i32 index;
  const char *chars = SUBSTITUTION_HEX_ALPHABET;

  // check if there's enough space for the substitution
  if (ctx->offset + SUBSTITUTION_HEX_LEN >= ctx->buffer_size) {
    return STDOUT_ERROR_BUFFER_TOO_SMALL;
  }

  // copy the hex header
  ctx->buffer[ctx->offset++] = '0';
  ctx->buffer[ctx->offset++] = 'x';

  // copy the hex representation
  for (index = SUBSTITUTION_HEX_ALPHABET_LEN - 1; index >= 0; index--) {
    ctx->buffer[ctx->offset++] = chars[(value >> (index * 4)) & 0x0f];
  }

  // success
  return 0;
}

static i64 substitute_indent(struct stdout_context *ctx, u64 indent) {
  // check if there's enough space for the substitution
  if (ctx->offset + indent >= ctx->buffer_size) {
    return STDOUT_ERROR_BUFFER_TOO_SMALL;
  }

  // append indent spaces
  while (indent-- > 0) {
    ctx->buffer[ctx->offset++] = SUBSTITUTION_INDENT_CHARACTER;
  }

  // success
  return 0;
}

static i64 substitute_marker(struct stdout_context *ctx) {
  // check if there's enough space for the substitution
  if (ctx->offset >= ctx->buffer_size) {
    return STDOUT_ERROR_BUFFER_TOO_SMALL;
  }

  // append the substitution marker
  ctx->buffer[ctx->offset++] = SUBSTITUTION_MARKER;

  // success
  return 0;
}

static i64 substitute_decimal(struct stdout_context *ctx, i64 value) {
  i32 index;
  u64 value_abs;

  char tmp[SUBSTITUTION_DECIMAL_LEN];
  const char *chars = SUBSTITUTION_DECIMAL_ALPHABET;

  // check if there's enough space for the substitution
  if (ctx->offset + SUBSTITUTION_DECIMAL_LEN >= ctx->buffer_size) {
    return STDOUT_ERROR_BUFFER_TOO_SMALL;
  }

  // default
  index = 0;
  value_abs = value < 0 ? -(u64)value : (u64)value;

  // if the value is negative, add a minus sign
  if (value < 0) {
    ctx->buffer[ctx->offset++] = '-';
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
    ctx->buffer[ctx->offset++] = tmp[--index];
  }

  // success
  return 0;
}

static i64 substitute_result(struct stdout_context *ctx, i64 result) {
  const char *text;

  // check if there's enough space for the substitution
  if (ctx->offset + ERROR_NAME_MAX_LENGTH >= ctx->buffer_size) {
    return STDOUT_ERROR_BUFFER_TOO_SMALL;
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

static i64 substitute_ascii(struct stdout_context *ctx, const char **src, u64 *size) {
  char ch;
  u64 available;

  // calculate available space in the buffer
  available = ctx->buffer_size - ctx->offset;

  // copy the string until EOS
  while (*size > 0 && available > 0) {
    (*size)--;
    ch = *(*src)++;

    // if the character is not in the ASCII range, use fallback
    if (ch < SUBSTITUTION_ASCII_MIN || ch > SUBSTITUTION_ASCII_MAX) {
      ch = SUBSTITUTION_ASCII_FALLBACK;
    }

    // append the character to the buffer
    ctx->buffer[ctx->offset++] = ch;
    available--;
  }

  // report an error if the string was not fully copied
  if (*size > 0 && ctx->offset >= ctx->buffer_size) {
    return STDOUT_ERROR_BUFFER_TOO_SMALL;
  }

  // success
  return 0;
}

static i64 flush_buffer(struct stdout_context *ctx) {
  i64 result;

  while (ctx->offset > 0) {
    // write the buffer to stdout
    result = sys_write(1, ctx->buffer, ctx->offset);
    if (result < 0) return result;

    // adjust the length and buffer pointer
    ctx->offset -= result;
    ctx->buffer += result;
  }

  // success
  return 0;
}

static i64 format(struct stdout_context *ctx) {
  u8 vargs_offset;
  i64 result;

  // default values
  vargs_offset = 0;
  result = 0;

  // handle the format string
  while (*ctx->fmt != EOS && result == 0) {

    // handle two consecutive substitution markers
    if (*ctx->fmt == SUBSTITUTION_MARKER && vargs_offset + 1 < VARGS_MAX) {
      switch (*(ctx->fmt + 1)) {
        case SUBSTITUTION_ASCII:
          result =
            substitute_ascii(ctx, (const char **)ctx->vargs + vargs_offset, (u64 *)ctx->vargs + vargs_offset + 1);
          if (result == STDOUT_ERROR_BUFFER_TOO_SMALL) continue;

          vargs_offset += 2;
          ctx->fmt += 2;
          continue;
      }
    }

    // handle single substitution markers
    if (*ctx->fmt == SUBSTITUTION_MARKER && vargs_offset < VARGS_MAX) {
      switch (*(ctx->fmt + 1)) {
        case SUBSTITUTION_STRING:
          result = substitute_string(ctx, (const char **)ctx->vargs + vargs_offset);
          if (result == STDOUT_ERROR_BUFFER_TOO_SMALL) continue;
          break;

        case SUBSTITUTION_HEX:
          result = substitute_hex(ctx, (u64)ctx->vargs[vargs_offset]);
          if (result == STDOUT_ERROR_BUFFER_TOO_SMALL) continue;
          break;

        case SUBSTITUTION_INDENT:
          result = substitute_indent(ctx, (u64)ctx->vargs[vargs_offset]);
          if (result == STDOUT_ERROR_BUFFER_TOO_SMALL) continue;
          break;

        case SUBSTITUTION_DECIMAL:
          result = substitute_decimal(ctx, (i64)ctx->vargs[vargs_offset]);
          if (result == STDOUT_ERROR_BUFFER_TOO_SMALL) continue;
          break;

        case SUBSTITUTION_RESULT:
          result = substitute_result(ctx, (i64)ctx->vargs[vargs_offset]);
          if (result == STDOUT_ERROR_BUFFER_TOO_SMALL) continue;
          break;

        case SUBSTITUTION_MARKER:
          result = substitute_marker(ctx);
          if (result == STDOUT_ERROR_BUFFER_TOO_SMALL) continue;
          break;

        case EOS:
          result = substitute_marker(ctx);
          if (result == STDOUT_ERROR_BUFFER_TOO_SMALL) continue;
          goto result;

        default:
          result = substitute_unknown(ctx, *(ctx->fmt + 1));
          if (result == STDOUT_ERROR_BUFFER_TOO_SMALL) continue;
          break;
      }

      // move context
      vargs_offset += 1;
      ctx->fmt += 2;

      // continue looping
      continue;
    }

    // check if there's enough space for a regular character
    if (ctx->offset >= SUBSTITUTION_BUFFER_SIZE) {
      result = STDOUT_ERROR_BUFFER_TOO_SMALL;
    }

    // append regular character
    if (result == 0) {
      ctx->buffer[ctx->offset++] = *ctx->fmt++;
    }

    if (result < 0) {
      return result;
    }
  }

result:

  // append EOS to the buffer if there's space
  if (ctx->offset < SUBSTITUTION_BUFFER_SIZE) {
    ctx->buffer[ctx->offset] = EOS;
  }

  return ctx->offset;
}

void writef(const char *fmt, ...) {
  void *vargs[VARGS_MAX];
  char buffer[SUBSTITUTION_BUFFER_SIZE];

  i64 result;
  struct stdout_context ctx;

  // collect argument list
  vargs_init(vargs);

  // initialize the context
  ctx.fmt = fmt;
  ctx.offset = 0;
  ctx.vargs = vargs;
  ctx.buffer = buffer;
  ctx.buffer_size = SUBSTITUTION_BUFFER_SIZE;

  do {
    // format the string
    result = format(&ctx);

    // ignore the result
    flush_buffer(&ctx);
  } while (result == STDOUT_ERROR_BUFFER_TOO_SMALL);
}

#if defined(I13C_TESTS)

static void can_format_without_substitutions() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "Hello, World!";
  ctx.vargs = NULL;
  ctx.offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_size = sizeof(buffer);

  // format a simple string
  offset = format(&ctx);

  // assert the result
  assert(offset == 13, "should write 13 bytes");
  assert_eq_str(buffer, "Hello, World!", "should format 'Hello, World!'");
}

static void can_format_with_string_substitution() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  void *vargs[VARGS_MAX];
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "Hello, %s!";
  ctx.vargs = vargs;
  ctx.offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_size = sizeof(buffer);

  // not initialize all vargs
  vargs[0] = "World";

  // format a string with substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 13, "should write 13 bytes");
  assert_eq_str(buffer, "Hello, World!", "should format 'Hello, World!'");
}

static void can_format_with_hex_substitution() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  void *vargs[VARGS_MAX];
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "Value: %x";
  ctx.vargs = vargs;
  ctx.offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_size = sizeof(buffer);

  // not initialize all vargs
  vargs[0] = (void *)(u64)0x1234abcd01020304;

  // format a string with hex substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 25, "should write 25 bytes");
  assert_eq_str(buffer, "Value: 0x1234abcd01020304", "should format 'Value: 0x1234abcd01020304'");
}

static void can_format_with_decimal_positive() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  void *vargs[VARGS_MAX];
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "Value: %d";
  ctx.vargs = vargs;
  ctx.offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_size = sizeof(buffer);

  // not initialize all vargs
  vargs[0] = (void *)(i64)123456789;

  // format a string with decimal substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 16, "should write 16 bytes");
  assert_eq_str(buffer, "Value: 123456789", "should format 'Value: 123456789'");
}

static void can_format_with_decimal_negative() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  void *vargs[VARGS_MAX];
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "Value: %d";
  ctx.vargs = vargs;
  ctx.offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_size = sizeof(buffer);

  // not initialize all vargs
  vargs[0] = (void *)(i64)-123456789;

  // format a string with decimal substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 17, "should write 17 bytes");
  assert_eq_str(buffer, "Value: -123456789", "should format 'Value: -123456789'");
}

static void can_format_with_decimal_int64_min() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  void *vargs[VARGS_MAX];
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "Value: %d";
  ctx.vargs = vargs;
  ctx.offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_size = sizeof(buffer);

  // not initialize all vargs
  vargs[0] = (void *)(i64)(-9223372036854775807ll - 1);

  // format a string with decimal substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 27, "should write 27 bytes");
  assert_eq_str(buffer, "Value: -9223372036854775808", "should format 'Value: -9223372036854775808'");
}

static void can_format_with_indent_substitution() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  void *vargs[VARGS_MAX];
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "%iabcdef";
  ctx.vargs = vargs;
  ctx.offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_size = sizeof(buffer);

  // not initialize all vargs
  vargs[0] = (void *)(u64)4;

  // format a string with indent substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 10, "should write 10 bytes");
  assert_eq_str(buffer, "    abcdef", "should format '    abcdef'");
}

static void can_format_with_ascii_substitution() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  void *vargs[VARGS_MAX];
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "ASCII: %a";
  ctx.vargs = vargs;
  ctx.offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_size = sizeof(buffer);

  // not initialize all vargs
  vargs[0] = "Hello, Ślimak!";
  vargs[1] = (void *)(u64)15;

  // format a string with ascii substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 22, "should write 22 bytes");
  assert_eq_str(buffer, "ASCII: Hello, ..limak!", "should format 'ASCII: Hello, ..limak!'");
}

static void can_format_with_result_substitution() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  void *vargs[VARGS_MAX];
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "Result: %r";
  ctx.vargs = vargs;
  ctx.offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_size = sizeof(buffer);

  // not initialize all vargs
  vargs[0] = (void *)(MALLOC_ERROR_BASE - 0x05);

  // format a string with result substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 16, "should write 16 bytes");
  assert_eq_str(buffer, "Result: malloc#5", "should format 'Result: malloc#5'");
}

static void can_format_with_unknown_substitution() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "Unknown: %z";
  ctx.vargs = NULL;
  ctx.offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_size = sizeof(buffer);

  // format a string with unknown substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 11, "should write 11 bytes");
  assert_eq_str(buffer, "Unknown: %z", "should format 'Unknown: %z'");
}

static void can_format_with_percent_escape() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "50%% done%";
  ctx.vargs = NULL;
  ctx.offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_size = sizeof(buffer);

  // format a string
  offset = format(&ctx);

  // assert the result
  assert(offset == 9, "should write 9 bytes");
  assert_eq_str(buffer, "50% done%", "should format '50% done%'");
}

void stdout_test_cases(struct runner_context *ctx) {
  test_case(ctx, "can format without substitutions", can_format_without_substitutions);
  test_case(ctx, "can format with string substitution", can_format_with_string_substitution);
  test_case(ctx, "can format with hex substitution", can_format_with_hex_substitution);
  test_case(ctx, "can format with decimal positive", can_format_with_decimal_positive);
  test_case(ctx, "can format with decimal negative", can_format_with_decimal_negative);
  test_case(ctx, "can format with decimal int64 min", can_format_with_decimal_int64_min);
  test_case(ctx, "can format with indent substitution", can_format_with_indent_substitution);
  test_case(ctx, "can format with ascii substitution", can_format_with_ascii_substitution);
  test_case(ctx, "can format with result substitution", can_format_with_result_substitution);
  test_case(ctx, "can format with unknown substitution", can_format_with_unknown_substitution);
  test_case(ctx, "can format with percent escape", can_format_with_percent_escape);
}

#endif
