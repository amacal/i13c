    section .text
    global vargs_init

vargs_init:
    mov [rdi], rsi
    mov [rdi + 8], rdx
    mov [rdi + 16], rcx
    mov [rdi + 24], r8
    mov [rdi + 32], r9
    ret
