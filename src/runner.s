%ifdef I13C_TESTS

    section .text
    global _runner
    extern runner_execute

_runner:
    call runner_execute
    mov edi, eax
    mov rax, 60
    syscall

%endif
