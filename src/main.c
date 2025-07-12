#include "stdout.h"

i32 main()
{
    stdout_printf("Hello, %x %x!\n", "World", 1 << 31);

    return 0;
}
