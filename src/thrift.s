    %ifdef I13C_THRIFT

    section .text
    global thrift_start
    extern thrift_main

thrift_start:
    call thrift_main
    mov edi, eax
    mov rax, 60
    syscall

    %endif
