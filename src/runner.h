#pragma once

#include "typing.h"

struct runner_context;
struct runner_entry;

typedef struct {
  const char *name;
  void (*execute)(struct runner_context *ctx);
} runner_entry;

typedef struct {
  u64 offset;                // holds number of occupied entries
  runner_entry entries[256]; // holds only 256 entries for now
} runner_context;

/// @brief Appends a test case to the runner context.
/// @param ctx Runner context.
/// @param name Test case name.
/// @param execute Test case function pointer.
void test_case(runner_context *ctx, const char *name, void (*execute)(runner_context *ctx));

/// @brief Asserts a condition and prints a message if it fails.
/// @param condition Condition to check.
/// @param msg Message to print on failure.
void assert(bool condition, const char *msg);
