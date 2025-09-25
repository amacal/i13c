#include "argv.h"
#include "parquet.extract.h"
#include "parquet.show.h"
#include "stdout.h"
#include "typing.h"

#if defined(I13C_PARQUET)

#define CMD_SHOW_ID 0
#define CMD_SHOW "show"

#define CMD_EXTRACT_ID 1
#define CMD_EXTRACT "extract"

#define CMD_LAST_ID 2

i32 parquet_main(u32 argc, const char **argv) {
  i64 result;
  u64 selected;

  // prepare commands and their names
  const char *names[CMD_LAST_ID + 1];
  argv_match_fn commands[CMD_LAST_ID];

  // first, names
  names[CMD_SHOW_ID] = CMD_SHOW;
  names[CMD_EXTRACT_ID] = CMD_EXTRACT;
  names[CMD_LAST_ID] = NULL;

  // then, commands
  commands[CMD_SHOW_ID] = parquet_show;
  commands[CMD_EXTRACT_ID] = parquet_extract;

  // match the command
  result = argv_match(argc, argv, names, &selected);
  if (result < 0) goto cleanup;

  // execute the command
  result = commands[selected](argc - 2, argv + 2);
  if (result < 0) goto cleanup;

cleanup:
  if (result == 0) return 0;

  writef("Something wrong happened; error=%r\n", result);
  return result;
}

#endif
