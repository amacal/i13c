; error codes
    ERR_ZERO_WRITE equ -33                                 ; error when no bytes are written
    ERR_INVALID_FMT equ -34                                ; error for invalid format in printf
    ERR_BUFFER_OVFLW equ -35                               ; error for buffer overflow
    ERR_STACK_ALIGN equ -36                                ; error when stack is not aligned

; assumptions
    PRINTF_BUFFER_SIZE equ 248                             ; size of the printf output buffer

    section .text
    global stdout_print

; prints a specified number of characters from a string to the stdout
; rdi - a length of the next argument string
; rsi - pointer to a string (ascii, not null-terminated)
; rax - returns 0 if no error, or negative value indicating an error
stdout_print:
    lea rax, [rsp + 8]                                     ; compute RSP + 8
    test rax, 15                                           ; check stack alignment
    jnz .unaligned                                         ; jump to .unaligned as error

    mov rdx, rdi                                           ; load number of bytes
    mov rdi, 1                                             ; stdout file descriptor

    test rdx, rdx                                          ; check number of bytes to write
    jz .completed                                          ; if zero just complete it without syscall

.loop:
    mov rax, 1                                             ; sys_write syscall
    syscall                                                ; execute syscall

    test rax, rax                                          ; compare syscall result in RAX to 0
    jz .zero                                               ; jump to .zero if no progress made
    js .end                                                ; jump to .end if kernel failed

    sub rdx, rax                                           ; reduce remaining bytes
    add rsi, rax                                           ; advance the buffer pointer

    test rdx, rdx                                          ; check if remaining bytes are available
    jnz .loop                                              ; doesn't jump if everything was written

.completed:
    xor rax, rax                                           ; no error
    jmp .end

.unaligned:
    mov rax, ERR_STACK_ALIGN                               ; set custom error indicating not aligned stack
    jmp .end

.zero:
    mov rax, ERR_ZERO_WRITE                                ; set custom error indicating zero-write
    jmp .end

.end:
    ret                                                    ; returns syscall error already available in RAX
