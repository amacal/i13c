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

; prints a formatted string using % substitutions
; rdi - pointer to a format string (ascii, null-terminated)
; rsi - pointer to %s substitution, (ascii, null-terminated)
; rsi - pointer to %x substitution, (8 bytes, zero-extended)
; rax - returns 0 if no error, or negative value indicating an error
stdout_printf:
    lea rax, [rsp + 8]                                     ; compute RSP + 8
    sub rsp, PRINTF_BUFFER_SIZE                            ; reserve bytes on the stack for a buffer

    test rax, 15                                           ; check stack alignment
    jnz .unaligned                                         ; jump to .unaligned as error
    mov r8, rdi                                            ; pointer to a template string

    lea r9, [rsp]                                          ; pointer to output buffer
    xor r10, r10                                           ; output buffer offset

.next_char:
    mov al, byte [r8]                                      ; load the current template byte
    test al, al                                            ; check if we've reached the end of the template
    je .write_output                                       ; if no more bytes, write the output buffer

    inc r8                                                 ; advance format string pointer
    cmp al, '%'                                            ; check if the current byte is '%'
    jne .copy_char                                         ; if not, just copy character as is

    mov al, byte [r8]                                      ; load the next byte of substitution
    test rax, rax                                          ; check if we've reached the end of the template
    je .invalid                                            ; if no more bytes, write the output buffer

    inc r8                                                 ; advance format string pointer
    cmp al, 's'                                            ; check if the template is 's'
    je .copy_string                                        ; if yes, just append string

    cmp al, 'x'                                            ; check if the template is 'x'
    je .copy_hex                                           ; if yes, just append hex

.unaligned:
    mov rax, ERR_STACK_ALIGN                               ; set custom error indicating not aligned stack
    jmp .release_buffer                                    ; and jump to clean up routine

.invalid:
    mov rax, ERR_INVALID_FMT                               ; set custom error indicating invalid format
    jmp .release_buffer                                    ; and jump to clean up routine

.overflow:
    mov rax, ERR_BUFFER_OVFLW                              ; set custom error indicating buffer overflow
    jmp .release_buffer                                    ; and jump to clean up routine

.copy_string:
    xor rcx, rcx                                           ; clear offset within substitution string

.copy_string_loop:
    cmp r10, PRINTF_BUFFER_SIZE                            ; check if we are at the end of the buffer
    jae .overflow                                          ; report overflow

    mov al, byte [rsi + rcx]                               ; load a byte from substitution string
    test al, al                                            ; check for null-termination
    je .next_char                                          ; if null, continue processing template

    mov byte [r9 + r10], al                                ; copy single character to a buffer
    inc r10                                                ; increment output buffer offset
    inc rcx                                                ; increment substitution string offset
    jmp .copy_string_loop                                  ; continue copying substitution string

.copy_hex:
    mov rcx, 16                                            ; number of nibbles (8 bytes = 16 nibbles)

.write_hex_loop:
    cmp r10, PRINTF_BUFFER_SIZE                            ; check if we are at the end of the buffer
    jae .overflow                                          ; report overflow

    mov rax, 0xf000000000000000                            ; mask to get the highest nibble
    and rax, rsi                                           ; get the highest nibble of the substitution value
    shr rax, 60                                            ; shift it to the lowest nibble

    cmp rax, 9                                             ; check if it is less than or equal to 9
    jbe .write_hex_loop.digit                              ; if yes, convert to '0'-'9'

    add rax, 87                                            ; convert 0x0a - 0x0f to 'a'-'f'
    jmp .write_hex_loop.store                              ; store the character

.write_hex_loop.digit:
    add rax, 48                                            ; convert 0x00 - 0x09 to '0'-'9'

.write_hex_loop.store:
    mov byte [r9 + r10], al                                ; copy single character to a buffer
    shl rsi, 4                                             ; shift the substitution value to the left by 4 bits
    inc r10                                                ; increment output buffer offset
    dec rcx                                                ; increment substitution string offset
    jnz .write_hex_loop                                    ; continue processing nibbles

.copy_char:
    mov byte [r9 + r10], al                                ; copy single character to a buffer
    inc r10                                                ; increment buffer offset
    jmp .next_char                                         ; continue processing template

.write_output:
    mov rdi, r10                                           ; pass the first arg the length of constructed string
    mov rsi, r9                                            ; pass the second arg as ptr to the constucted string
    call stdout_print                                      ; call print which accepts prepared struct

.release_buffer:
    add rsp, PRINTF_BUFFER_SIZE                            ; release the allocated buffer
    ret
