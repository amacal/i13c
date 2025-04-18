    struc channel_node
    .val resq 1                                            ; value of the message
    .ptr resq 1                                            ; pointer to the resume
    .next resq 1                                           ; pointer to the next node
    .last resq 1                                           ; pointer to the last node
    endstruc

    struc channel_info
    .coop resq 1                                           ; pointer to the coop info
    .size resq 1                                           ; number of participants
    .free resq 1                                           ; pointer to the free node
    .send resq 1                                           ; pointer to the send node
    .recv resq 1                                           ; pointer to the recv node
    endstruc

    section .text
    global channel_init, channel_free, channel_send, channel_recv
    extern coop_noop_ex, coop_pull, coop_push, coop_switch

; initializes a hand-off channel
; rdi - ptr to uninitialized structure
; rsi - ptr to already initialized coop info structure
; rdx - number of participants in the channel
; rax - returns 0 if no error, or negative value indicating an error
channel_init:
    mov rcx, 5                                             ; 5 iterations, each 8 bytes
    xor rax, rax                                           ; source value is 0
    rep stosq                                              ; fill 48 bytes with 0
    sub rdi, 40                                            ; rewind the pointer

    mov [rdi + channel_info.coop], rsi                     ; set the coop info pointer
    mov [rdi + channel_info.size], rdx                     ; set the number of participants

    ret

; frees the hand-off channel instance
; rdi - ptr to channel structure
; rsi - 0 or 1 indicating whether to wait for all participants
; rax - returns 0 if no error, or negative value indicating an error
channel_free:
    ret

; sends a message to the channel
; rdi - ptr to channel structure
; rsi - ptr to message or just a message
; rax - returns 0 if no error, or negative value indicating an error
channel_send:
    push rsi                                               ; save the message pointer
    push rdi                                               ; save the channel pointer

    mov rax, [rdi + channel_info.recv]                     ; get the current rx pointer
    test rax, rax                                          ; check if it is null
    jz .insert                                             ; if null, insert the message

    mov rsi, 1                                             ; just schedule and no loop
    mov rdi, [rdi + channel_info.coop]                     ; get the coop info pointer

    lea rdx, .resume                                       ; set the .resume function address
    call coop_noop_ex                                      ; pretend to noop

    pop rdi                                                ; get the channel pointer
    pop rsi                                                ; get the message pointer

    mov rax, [rdi + channel_info.recv]                     ; get the current rx pointer
    mov rcx, [rax + channel_node.ptr]                      ; get the resume pointer
    push rcx                                               ; set where pull will resume

    mov rcx, [rax + channel_node.val]                      ; get the message slot
    mov [rcx], rsi                                         ; set the message value

    mov rcx, [rax + channel_node.next]                     ; get the next node
    mov [rdi + channel_info.recv], rcx                     ; set the recv pointer

    and rax, ~0x0fff                                       ; find the dump area
    jmp coop_pull                                          ; restore the context and resume

.resume:
    xor rax, rax                                           ; set the return value to 0
    add rsp, 16                                            ; clean the stack
    ret

.insert:
    ud2

; recives a message from the channel
; rdi - ptr to channel structure
; rsi - ptr to a slot where the message will be stored
; rax - returns a message if no error, or negative value indicating an error
channel_recv:
    mov rax, [rdi + channel_info.send]                     ; get the current tx pointer
    test rax, rax                                          ; check if it is null
    jz .insert                                             ; if null, insert the message
    ud2

.insert:
    sub rsp, 32                                            ; reserve slot for a node
    mov [rsp + channel_node.val], rsi                      ; set the message holder

    lea rax, .done                                         ; get the resume address
    mov [rsp + channel_node.ptr], rax                      ; set the message pointer

    mov qword [rsp + channel_node.next], 0                 ; set the next pointer
    mov qword [rsp + channel_node.last], 0                 ; set the last pointer

    mov rcx, [rdi + channel_info.recv]                     ; get the current rx pointer
    test rcx, rcx                                          ; check if it is null

    jz .insert.link                                        ; if null, skip relinking
    mov [rsp + channel_node.next], rcx                     ; set the next pointer

.insert.link:
    mov [rdi + channel_info.recv], rsp                     ; set the recv pointer

    mov rax, rsp                                           ; get the current stack
    and rax, ~0x0fff                                       ; find the dump area

    mov rcx, [rdi + channel_info.coop]                     ; get the coop info pointer
    lea rsi, .done                                         ; set the .done function address
    xor rdi, rdi                                           ; set the .done function context
    mov r11, [rsp + 32]                                    ; set the code resumption address
    lea r8, [rsp + 40]                                     ; set the old stack ptr

    call coop_push                                         ; dump task registers

    xor rdx, rdx                                           ; the main thread dump area is not known
    jmp coop_switch                                        ; switch to the main thread

.done:
    xor rax, rax                                           ; set the return value to 0
    jmp r11                                                ; simply resume after channel_recv
