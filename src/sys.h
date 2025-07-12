#pragma once

#include "typing.h"

#define O_RDONLY  0

#define PROT_READ 0x01
#define PROT_WRITE 0x02

#define MAP_PRIVATE 0x02
#define MAP_ANONYMOUS 0x20

/// @brief Opens a file.
/// @param path Path to the file.
/// @param flags File access mode and flags (e.g., O_RDONLY, O_WRONLY).
/// @param mode File mode (used only when creating a file).
/// @return File descriptor on success, or negative error code.
extern i32 sys_open(const char *path, u32 flags, u16 mode);

/// @brief Reads data from a file descriptor.
/// @param fd File descriptor to read from.
/// @param buf Buffer to store read data.
/// @param count Number of bytes to read.
/// @return Number of bytes read on success, or negative error code.
extern i64 sys_read(i32 fd, char *buf, u64 count);

/// @brief Writes data to a file descriptor.
/// @param fd File descriptor to write to.
/// @param buf Buffer containing data to write.
/// @param count Number of bytes to write.
extern i64 sys_write(i32 fd, const char *buf, u64 count);

/// @brief Exits the program with the given status code.
/// @param status Exit status code.
extern void sys_exit(i32 status);

/// @brief Allocates memory.
/// @param addr Desired starting address (or NULL).
/// @param length Length of the mapping.
/// @param prot Desired memory protection (e.g., PROT_READ).
/// @param flags Mapping flags (e.g., MAP_SHARED).
/// @param fd File descriptor to map.
/// @param offset Offset within the file.
/// @return Pointer to the mapped memory on success, or negative on failure.
extern i64 sys_mmap(void *addr, u64 length, u32 prot, u32 flags, i64 fd, u64 offset);

/// @brief Deallocates memory previously allocated with mmap.
/// @param addr Address of the memory to deallocate.
/// @param length Length of the memory to deallocate.
/// @return 0 on success, or negative on failure.
extern i64 sys_munmap(void *addr, u64 length);
