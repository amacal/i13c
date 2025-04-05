typedef struct {
    long data[33];
} coop_info;

typedef struct {
    coop_info* coop;
    char* file_name;

} coop_task;

/// @brief prints a text in the stdout
/// @param len length of the text
/// @param data ptr to the text
/// @return negative error or 0 on success
extern long stdout_print(const long len, const char* data);

/// @brief print a formatted text in the stdout
/// @param fmt ptr to the format
/// @return a negative error or 0 on succcess
extern long stdout_printf(const char* fmt, ...);

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
extern long coop_spawn(const coop_info* coop, long (*fn)(const coop_task*), const coop_task* ctx);

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

/// @brief performs a read operation
/// @param coop ptr to the initialized coop structure
/// @param fd file descriptor to read from
/// @param buffer ptr to the buffer to read into
/// @param size size of the buffer
/// @param offset offset to read from
/// @return 0 if no error, or negative value indicating an error
extern long coop_read(const coop_info* coop, unsigned int fd, char* buffer, unsigned int size, unsigned long offset);

long task_one(const coop_task* task) {
    char buffer[64];

    stdout_printf("Hello from the task one before noop!\n", "");
    coop_noop(task->coop);

    stdout_printf("Hello from the task one after noop! %s\n", task->file_name);
    coop_timeout(task->coop, 3);

    stdout_printf("Hello from the task one after timeout!\n", "");
    coop_read(task->coop, 0, buffer, sizeof(buffer), 0);

    buffer[3] = '\0';
    stdout_printf("Hello from the task one after read! %s\n", buffer);

    return 0;
}

long task_two(const coop_task* task) {
    char buffer[64];

    stdout_printf("Hello from the task two before noop!\n", "");
    coop_noop(task->coop);

    stdout_printf("Hello from the task two after noop! %s\n", task->file_name);
    coop_timeout(task->coop, 2);

    stdout_printf("Hello from the task two after timeout!\n", "");
    unsigned int fd = coop_openat(task->coop, task->file_name, 0, 0);

    stdout_printf("Hello from the task two after open!\n", "");
    coop_read(task->coop, fd, buffer, sizeof(buffer), 0);

    buffer[3] = '\0';
    stdout_printf("Hello from the task two after read! %s\n", buffer);

    return 0;
}

int main() {
    coop_info coop;
    coop_task task_one_ctx, task_two_ctx;

    task_one_ctx.coop = &coop;
    task_one_ctx.file_name = "task_one.txt";

    task_two_ctx.coop = &coop;
    task_two_ctx.file_name = "Makefile";

    if (coop_init(&coop, 4) < 0) {
        stdout_printf("coop_init\n", "");
        return -1;
    }

    if (coop_spawn(&coop, task_one, &task_one_ctx) < 0) {
        stdout_printf("coop_spawn\n", "");
        return -1;
    }

    if (coop_spawn(&coop, task_two, &task_two_ctx) < 0) {
        stdout_printf("coop_spawn\n", "");
        return -1;
    }

    if (coop_loop(&coop) < 0) {
        stdout_printf("coop_run\n", "");
        return -1;
    }

    if (coop_free(&coop) < 0) {
        stdout_printf("coop_free\n", "");
        return -1;
    }

    stdout_printf("All tasks completed.\n", "");
}
