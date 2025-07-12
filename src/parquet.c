#include "parquet.h"
#include "sys.h"
#include "typing.h"

void parquet_init(parquet_file *file) {
  file->fd = 0;
}

i64 parquet_open(parquet_file *file, const char* path) {
  i64 fd;

  if ((fd = sys_open(path, O_RDONLY, 0)) < 0) {
    return fd;
  }

  file->fd = fd;
  return 0;
}
