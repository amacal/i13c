#include "coop.h"
#include "channel.h"
#include "stdout.h"

typedef struct {
    coop_info* coop;
    channel_info* channel;
    char* file_name;

} coop_task;

long task_one(const coop_task* task) {
    char buffer[64];
    char *msg;

    stdout_printf("Hello from the task one before noop!\n");
    coop_noop(task->coop);

    stdout_printf("Hello from the task one after noop! %s\n", task->file_name);
    coop_timeout(task->coop, 3);

    stdout_printf("Hello from the task one after timeout!\n");
    coop_read(task->coop, 0, buffer, sizeof(buffer), 0);

    buffer[3] = '\0';
    stdout_printf("Hello from the task one after read! %s\n", buffer);

    for (int j = 0; j < 1000; j++) {
        for (int i = 0; i < 10000000; i++) {
            channel_recv(task->channel, &msg);
        }

        stdout_printf("Hello from the task one after recv! %s\n", msg);
    }

    channel_free(task->channel, 0);
    return 0;
}

long task_two(const coop_task* task) {
    char buffer[64];

    stdout_printf("Hello from the task two before noop!\n");
    coop_noop(task->coop);

    stdout_printf("Hello from the task two after noop! %s\n", task->file_name);
    coop_timeout(task->coop, 2);

    stdout_printf("Hello from the task two after timeout!\n");
    unsigned int fd = coop_openat(task->coop, task->file_name, 0, 0);

    stdout_printf("Hello from the task two after open!\n");
    coop_read(task->coop, fd, buffer, sizeof(buffer), 0);

    buffer[3] = '\0';
    stdout_printf("Hello from the task two after read! %s\n", buffer);

    coop_close(task->coop, fd);
    stdout_printf("Hello from the task two after close!\n");

    for (int j = 0; j < 1000; j++) {
        for (int i = 0; i < 10000000; i++) {
            channel_send(task->channel, buffer);
        }

        stdout_printf("Hello from the task two after send!\n");
    }

    channel_free(task->channel, 1);
    return 0;
}

long coordinator(const coop_task* task) {
    channel_info channel;
    coop_task task_one_ctx, task_two_ctx;

    task_one_ctx.coop = task->coop;
    task_one_ctx.channel = &channel;
    task_one_ctx.file_name = "build.rs";

    task_two_ctx.coop = task->coop;
    task_two_ctx.channel = &channel;
    task_two_ctx.file_name = "Makefile";

    if (channel_init(&channel, task->coop, 3) < 0) {
        stdout_printf("channel_init failed\n");
        return -1;
    }

    if (coop_spawn(task->coop, task_one, &task_one_ctx, sizeof(coop_task)) < 0) {
        stdout_printf("coop_spawn failed\n");
        return -1;
    }

    if (coop_spawn(task->coop, task_two, &task_two_ctx, sizeof(coop_task)) < 0) {
        stdout_printf("coop_spawn failed\n");
        return -1;
    }

    if (channel_free(&channel, 1) < 0) {
        stdout_printf("channel_free failed\n");
        return -1;
    }

    stdout_printf("Coordinator completed.\n");
    return 0;
}

int main() {
    coop_info coop;
    coop_task task;

    task.coop = &coop;

    if (coop_init(&coop, 4) < 0) {
        stdout_printf("coop_init failed\n");
        return -1;
    }

    if (coop_spawn(&coop, coordinator, &task, sizeof(coop_task)) < 0) {
        stdout_printf("coop_spawn failed\n");
        return -1;
    }

    if (coop_loop(&coop) < 0) {
        stdout_printf("coop_loop failed\n");
        return -1;
    }

    if (coop_free(&coop) < 0) {
        stdout_printf("coop_free failed\n");
        return -1;
    }

    stdout_printf("All tasks completed.\n");
}
