    section .text
    global sys_exit

sys_exit:
    mov rax, 60
    syscall
