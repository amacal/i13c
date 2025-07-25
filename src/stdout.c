#include "stdout.h"
#include "sys.h"
#include "typing.h"
#include "vargs.h"

#define SUBSTITUTION_BUFFER_SIZE 256
#define SUBSTITUTION_MARKER '%'
#define SUBSTITUTION_STRING 's'
#define SUBSTITUTION_INDENT 'i'

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

struct stdout_context {
  const char *fmt; // format string
  char *buffer;    // output buffer
  void **vargs;    // variable arguments
};

static void substitute_string(u64 *offset, char *buffer, const char *src) {
  // copy the string until EOS or buffer is full
  while (*src != EOS && *offset < SUBSTITUTION_BUFFER_SIZE) {
    buffer[(*offset)++] = *src++;
  }
}

static void substitute_unknown(u64 *offset, char *buffer, char symbol) {
  if (*offset < SUBSTITUTION_BUFFER_SIZE) {
    buffer[(*offset)++] = SUBSTITUTION_MARKER;
  }

  // append an unknown substitution
  if (*offset < SUBSTITUTION_BUFFER_SIZE) {
    buffer[(*offset)++] = symbol;
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

static void substitute_indent(u64 *offset, char *buffer, u64 indent) {
  u64 index;

  // append indent spaces
  for (index = 0; index < indent && *offset < SUBSTITUTION_BUFFER_SIZE; index++) {
    buffer[(*offset)++] = ' ';
  }
}

static void substitute_marker(u64 *offset, char *buffer) {
  // append the substitution marker
  if (*offset < SUBSTITUTION_BUFFER_SIZE) {
    buffer[(*offset)++] = SUBSTITUTION_MARKER;
  }
}

static void substitute_decimal(u64 *offset, char *buffer, i64 value) {
  i32 index;
  u64 value_abs;

  char tmp[SUBSTITUTION_DECIMAL_LEN];
  const char *chars = SUBSTITUTION_DECIMAL_ALPHABET;

  // default
  index = 0;
  value_abs = value < 0 ? -(u64)value : (u64)value;

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

static u64 format(struct stdout_context *ctx) {
  u8 vargs_offset;
  u64 buffer_offset;

  // default values
  vargs_offset = 0;
  buffer_offset = 0;

  // handle the format string
  while (*ctx->fmt != EOS && buffer_offset < SUBSTITUTION_BUFFER_SIZE) {

    // handle two consecutive substitution markers
    if (*ctx->fmt == SUBSTITUTION_MARKER && vargs_offset + 1 < VARGS_MAX) {
      switch (*(ctx->fmt + 1)) {
        case SUBSTITUTION_ASCII:
          substitute_ascii(&buffer_offset, ctx->buffer, (const char *)ctx->vargs[vargs_offset],
                           (u64)ctx->vargs[vargs_offset + 1]);
          vargs_offset += 2;
          ctx->fmt += 2;
          continue;
      }
    }

    // handle single substitution markers
    if (*ctx->fmt == SUBSTITUTION_MARKER && vargs_offset < VARGS_MAX) {
      switch (*++ctx->fmt) {
        case SUBSTITUTION_STRING:
          substitute_string(&buffer_offset, ctx->buffer, ctx->vargs[vargs_offset++]);
          break;
        case SUBSTITUTION_HEX:
          substitute_hex(&buffer_offset, ctx->buffer, (u64)ctx->vargs[vargs_offset++]);
          break;
        case SUBSTITUTION_INDENT:
          substitute_indent(&buffer_offset, ctx->buffer, (u64)ctx->vargs[vargs_offset++]);
          break;
        case SUBSTITUTION_DECIMAL:
          substitute_decimal(&buffer_offset, ctx->buffer, (i64)ctx->vargs[vargs_offset++]);
          break;
        case SUBSTITUTION_MARKER:
          substitute_marker(&buffer_offset, ctx->buffer);
          break;
        case EOS:
          substitute_marker(&buffer_offset, ctx->buffer);
          goto result;
        default:
          substitute_unknown(&buffer_offset, ctx->buffer, *ctx->fmt);
          break;
      }

      // continue looping
      ctx->fmt++;
      continue;
    }

    // append regular character
    ctx->buffer[buffer_offset++] = *ctx->fmt++;
  }

result:

  // append EOS to the buffer if there's space
  if (buffer_offset < SUBSTITUTION_BUFFER_SIZE) {
    ctx->buffer[buffer_offset] = EOS;
  }

  return buffer_offset;
}

void writef(const char *fmt, ...) {
  void *vargs[VARGS_MAX];
  char buffer[SUBSTITUTION_BUFFER_SIZE];

  u64 buffer_offset;
  struct stdout_context ctx;

  // collect argument list
  vargs_init(vargs);

  // initialize the context
  ctx.fmt = fmt;
  ctx.vargs = vargs;
  ctx.buffer = buffer;

  // format the string
  buffer_offset = format(&ctx);

  // print the final buffer and ignore the result
  flush_buffer(buffer, buffer_offset);
}

#if defined(I13C_TESTS)

static void can_format_without_substitutions() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "Hello, World!";
  ctx.vargs = NULL;
  ctx.buffer = buffer;

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
  ctx.buffer = buffer;

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
  ctx.buffer = buffer;

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
  ctx.buffer = buffer;

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
  ctx.buffer = buffer;

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
  ctx.buffer = buffer;

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
  ctx.buffer = buffer;

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
  ctx.buffer = buffer;

  // not initialize all vargs
  vargs[0] = "Hello, Ślimak!";
  vargs[1] = (void *)(u64)15;

  // format a string with ascii substitution
  offset = format(&ctx);

  // assert the result
  assert(offset == 22, "should write 22 bytes");
  assert_eq_str(buffer, "ASCII: Hello, ..limak!", "should format 'ASCII: Hello, ..limak!'");
}

static void can_format_with_unknown_substitution() {
  char buffer[SUBSTITUTION_BUFFER_SIZE];
  struct stdout_context ctx;
  u64 offset = 0;

  // initialize the context
  ctx.fmt = "Unknown: %z";
  ctx.vargs = NULL;
  ctx.buffer = buffer;

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
  ctx.buffer = buffer;

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
  test_case(ctx, "can format with unknown substitution", can_format_with_unknown_substitution);
  test_case(ctx, "can format with percent escape", can_format_with_percent_escape);
}

#endif
