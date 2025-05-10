#include "coop.h"
#include "channel.h"
#include "stdout.h"

typedef struct
{
    coop_info *coop;
    channel_info *in0;
    channel_info *in1;
    channel_info *in2;

} coop_task;

long worker(const coop_task *task)
{
    unsigned long res, val1, val2;
    channel_info request, *response;

    // stdout_printf("Worker started.\n");
    channel_init(&request, task->coop, 2);

    while (channel_recv(task->in0, &response) == 0)
    {
        res = 1;
        // stdout_printf("Worker received.\n");

        if (task->in1 && task->in2)
        {
            // stdout_printf("Worker asking.\n");
            channel_send(task->in1, &request);
            channel_recv(&request, &val1);

            // stdout_printf("Worker asking.\n");
            channel_send(task->in2, &request);
            channel_recv(&request, &val2);

            res = val1 + val2;
        };

        // stdout_printf("Worker sending.\n");
        channel_send(response, res);
        // stdout_printf("Worker sent.\n");
    }

    // stdout_printf("Worker completed.\n");

    channel_free(&request, 0);
    channel_free(task->in0, 0);

    if (task->in1 && task->in2)
    {
        channel_free(task->in1, 0);
        channel_free(task->in2, 0);
    }

    // stdout_printf("Worker freed.\n");
    return 0;
}

long coordinator(const coop_task *task)
{
    channel_info input[32];
    coop_task fib_task;
    unsigned long size, res;
    channel_info target;

    // stdout_printf("Coordinator started.\n");

    for (int i = 1; i < 32; i++)
    {
        switch (i)
        {
        case 1:
            size = 3;
            break;
        case 30:
            size = 3;
            break;
        case 31:
            size = 2;
            break;
        default:
            size = 4;
            break;
        }

        channel_init(&input[i], task->coop, size);

        if (i >= 3)
        {
            fib_task.in1 = &input[i - 1];
            fib_task.in2 = &input[i - 2];
        }
        else
        {
            fib_task.in1 = 0;
            fib_task.in2 = 0;
        }

        fib_task.coop = task->coop;
        fib_task.in0 = &input[i];

        // stdout_printf("Worker spawned.\n");
        coop_spawn(task->coop, worker, &fib_task, sizeof(coop_task));
    }

    channel_init(&target, task->coop, 2);
    // stdout_printf("Coordinator sending.\n");

    channel_send(&input[31], &target);
    // stdout_printf("Coordinator sent.\n");

    channel_recv(&target, &res);
    // stdout_printf("Target received.\n");

    channel_free(&target, 0);
    channel_free(&target, 1);
    // stdout_printf("Target freed.\n");

    for (int i = 31; i > 0; i--)
    {
        channel_free(&input[i], 1);
    }

    // stdout_printf("Coordinator completed.\n", res);
    return 0;
}

int main()
{
    coop_info coop;
    coop_task task;

    task.coop = &coop;

    if (coop_init(&coop, 4) < 0)
    {
        // stdout_printf("coop_init failed\n");
        return -1;
    }

    if (coop_spawn(&coop, coordinator, &task, sizeof(coop_task)) < 0)
    {
        // stdout_printf("coop_spawn failed\n");
        return -1;
    }

    if (coop_loop(&coop) < 0)
    {
        // stdout_printf("coop_loop failed\n");
        return -1;
    }

    if (coop_free(&coop) < 0)
    {
        // stdout_printf("coop_free failed\n");
        return -1;
    }

    // stdout_printf("All tasks completed.\n");
}
