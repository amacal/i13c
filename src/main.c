#include "coop.h"
#include "stdout.h"

typedef struct {
    coop_info* coop;
    char* file_name;

} coop_task;

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

    coop_close(task->coop, fd);
    stdout_printf("Hello from the task two after close!\n", "");

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

    if (coop_spawn(&coop, task_one, &task_one_ctx, sizeof(coop_task)) < 0) {
        stdout_printf("coop_spawn\n", "");
        return -1;
    }

    if (coop_spawn(&coop, task_two, &task_two_ctx, sizeof(coop_task)) < 0) {
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
