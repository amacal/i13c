    section .text
    global _start
    extern main

_start:
    call main
    mov edi, eax
    mov rax, 60
    syscall
