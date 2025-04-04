typedef struct {
    long data[33];
} coop_info;

/// @brief prints a text in the stdout
/// @param len the length of the text
/// @param data the ptr to the text
/// @return a negative error or 0 on success
extern long stdout_print(const long len, const char* data);

/// @brief print a formatted text in the stdout
/// @param fmt the ptr to the format
/// @param arg1 the first substitute
/// @return a negative error or 0 on succcess
extern long stdout_printf(const char* fmt, const char* arg1);

extern long coop_init(coop_info* coop, unsigned int submissions);
extern long coop_free(const coop_info* coop);
extern long coop_spawn(const coop_info* coop, long (*fn)(const coop_info*), const coop_info* ctx);
extern long coop_loop(const coop_info* coop);
extern long coop_noop(const coop_info* coop);

long task_one(const coop_info* coop) {
    stdout_printf("Hello from the task one before noop!\n", "");
    coop_noop(coop);

    stdout_printf("Hello from the task one after noop!\n", "");
    return 0;
}

long task_two(const coop_info* coop) {
    stdout_printf("Hello from the task two before noop!\n", "");
    coop_noop(coop);

    stdout_printf("Hello from the task two after noop!\n", "");
    return 0;
}

int main() {
    coop_info coop;

    if (coop_init(&coop, 4) < 0) {
        stdout_printf("coop_init\n", "");
        return -1;
    }

    if (coop_spawn(&coop, task_one, &coop) < 0) {
        stdout_printf("coop_spawn\n", "");
        return -1;
    }

    if (coop_spawn(&coop, task_two, &coop) < 0) {
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
