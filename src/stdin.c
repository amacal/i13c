#include "stdin.h"
#include "sys.h"
#include "typing.h"

i64 stdin_read(void *buffer, u64 size) {
  return sys_read(0, (char *)buffer, size);
}
