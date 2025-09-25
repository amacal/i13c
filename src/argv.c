#include "argv.h"
#include "typing.h"

static bool strcmp(const char *restrict left, const char *restrict right) {
  // compare each character
  while (*left != '\0' && *right != '\0') {
    if (*left != *right) return FALSE;

    left++;
    right++;
  }

  // both should reach the end
  return *left == *right;
}

i64 argv_match(u32 argc, const char **argv, const char **commands, u64 *selected) {
  u64 idx;

  // default
  idx = 0;

  // check for required arguments
  if (argc < 2) return ARGV_ERROR_NO_MATCH;

  // try to match each command
  while (commands[idx] != NULL) {
    if (strcmp(argv[1], commands[idx]) == TRUE) {
      *selected = idx;
      return 0;
    }

    idx++;
  }

  return ARGV_ERROR_NO_MATCH;
}
