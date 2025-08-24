    %ifdef I13C_PARQUET

    section .text
    global parquet_start
    extern parquet_main

parquet_start:
    call parquet_main
    mov edi, eax
    mov rax, 60
    syscall

    %endif
