    section .text
    global _runner
    extern runner

_runner:
    call runner
    mov edi, eax
    mov rax, 60
    syscall
