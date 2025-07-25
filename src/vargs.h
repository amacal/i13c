#pragma once

#include "typing.h"

// we can store only 5 System ABI V registers
// without RDI because it is used in vargs_init
#define VARGS_MAX 5

/// @brief Initialize variable argument list
/// @param area Pointer to the memory area for the argument list
extern void vargs_init(void *area);
