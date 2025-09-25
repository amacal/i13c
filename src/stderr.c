#include "stderr.h"
#include "format.h"
#include "sys.h"
#include "typing.h"
#include "vargs.h"

#define BUFFER_SIZE 256
#define BUFFER_TRIGGER 192

i64 stderr_flush(struct format_context *ctx) {
  i64 result;
  u32 offset;
  char *buffer;

  // default values
  result = 0;
  buffer = ctx->buffer;
  offset = ctx->buffer_offset;

  while (offset > 0) {
    // write the buffer to stderr
    result = sys_write(2, buffer, offset);
    if (result < 0) break;

    // adjust the length and buffer pointer
    buffer += result;
    offset -= result;

    // forget last result
    result = 0;
  }

  // reset the buffer offset
  ctx->buffer_offset = offset;

  // success
  return result;
}

void errorf(const char *fmt, ...) {
  void *vargs[VARGS_MAX];
  char buffer[BUFFER_SIZE];

  i64 result;
  struct format_context ctx;

  // collect argument list
  vargs_init(vargs);

  // initialize the context
  ctx.fmt = fmt;
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = BUFFER_TRIGGER;

  do {
    // format the string
    result = format(&ctx);

    // ignore the result
    stderr_flush(&ctx);
  } while (result == FORMAT_ERROR_BUFFER_TOO_SMALL);
}
