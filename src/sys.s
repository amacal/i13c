    section .text
    global sys_exit, sys_write

; exits the program with the given exit code
; edi - exit code (0 for success, non-zero for error)
; rax - returns 0 if no error, or negative value indicating an error
sys_exit:
    mov rax, 60
    syscall

; writes data to the stdout
; rdi - file descriptor (1 for stdout)
; rsi - buffer containing data to write
; rdx - number of bytes to write
sys_write:
    mov rax, 1
    syscall
    ret
