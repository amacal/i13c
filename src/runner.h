#pragma once

#include "typing.h"

struct runner_context;

struct runner_entry {
  const char *name;
  void (*execute)(struct runner_context *ctx);
};

struct runner_context {
  u64 offset;                       // holds number of occupied entries
  struct runner_entry entries[256]; // holds only 256 entries for now
};

/// @brief Appends a test case to the runner context.
/// @param ctx Runner context.
/// @param name Test case name.
/// @param execute Test case function pointer.
void test_case(struct runner_context *ctx, const char *name, void (*execute)(struct runner_context *ctx));

/// @brief Asserts a condition and prints a message if it fails.
/// @param condition Condition to check.
/// @param msg Message to print on failure.
void assert(bool condition, const char *msg);

/// @brief Asserts that two strings are equal.
/// @param actual Actual string.
/// @param expected Expected string.
/// @param msg Message to print on failure.
void assert_eq_str(const char *actual, const char *expected, const char *msg);
