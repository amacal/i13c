typedef struct {
    long data[33];
} coop_info;

/// @brief initializes cooperative preemption
/// @param coop ptr to the uninitialized structure
/// @param submissions size (in slots) of the completion queue
/// @return 0 if no error, or negative value indicating an error
extern long coop_init(coop_info* coop, unsigned int submissions);

/// @brief frees cooperative preemption
/// @param coop ptr to the initialized structure
/// @return 0 if no error, or negative value indicating the first error
extern long coop_free(const coop_info* coop);

/// @brief spawns a cooperative preemption task
/// @param coop ptr to the initialized structure
/// @param fn ptr to the function to be executed
/// @param ctx ptr to the function argument
/// @return 0 if no error, or negative value indicating an error
extern long coop_spawn(const coop_info* coop, long (*fn)(const void*), const void* ctx);

/// @brief runs a cooperative preemption loop
/// @param coop ptr to the initialized coop structure
/// @return 0 if no error, or negative value indicating an error
extern long coop_loop(const coop_info* coop);

/// @brief performs a noop operation
/// @param coop ptr to the initialized coop structure
/// @return 0 if no error, or negative value indicating an error
extern long coop_noop(const coop_info* coop);

/// @brief performs a timeout operation
/// @param coop ptr to the initialized coop structure
/// @param seconds number of seconds to wait
/// @return 0 if no error, or negative value indicating an error
extern long coop_timeout(const coop_info* coop, unsigned int seconds);

/// @brief performs an open file operation
/// @param coop ptr to the initialized coop structure
/// @param file_name ptr to the file name
/// @param flags file open flags
/// @param mode file open mode
/// @return 0 if no error, or negative value indicating an error
extern long coop_openat(const coop_info* coop, const char* file_name, unsigned int flags, unsigned int mode);

/// @brief performs a close file operation
/// @param coop ptr to the initialized coop structure
/// @param fd file descriptor to close
/// @return 0 if no error, or negative value indicating an error
extern long coop_close(const coop_info* coop, unsigned int fd);

/// @brief performs a read operation
/// @param coop ptr to the initialized coop structure
/// @param fd file descriptor to read from
/// @param buffer ptr to the buffer to read into
/// @param size size of the buffer
/// @param offset offset to read from
/// @return 0 if no error, or negative value indicating an error
extern long coop_read(const coop_info* coop, unsigned int fd, char* buffer, unsigned int size, unsigned long offset);
