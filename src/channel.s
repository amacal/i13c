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
    mov rax, [rdi + channel_info.recv]                     ; get the current rx pointer
    test rax, rax                                          ; check if it is null
    jz .insert                                             ; if null, insert the message

    push rsi                                               ; save the message pointer
    push rdi                                               ; save the channel pointer

.direct:
    mov rsi, 1                                             ; just schedule and no loop
    mov rdi, [rdi + channel_info.coop]                     ; get the coop info pointer

    xor rcx, rcx                                           ; set the current stack context
    lea rdx, .direct.resume                                ; set the .resume function address
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

.direct.resume:
    xor rax, rax                                           ; set the return value to 0
    add rsp, 16                                            ; clean the stack
    ret

.insert:
    sub rsp, 32                                            ; reserve slot for a node
    mov [rsp + channel_node.val], rsi                      ; set the message payload

    mov rax, [rsp + 32]                                    ; get the resume address
    mov [rsp + channel_node.ptr], rax                      ; set the message pointer

    mov qword [rsp + channel_node.next], 0                 ; set the next pointer
    mov qword [rsp + channel_node.last], 0                 ; set the last pointer

    mov rcx, [rdi + channel_info.send]                     ; get the current send pointer
    test rcx, rcx                                          ; check if it is null

    jz .insert.link                                        ; if null, skip relinking
    mov [rsp + channel_node.next], rcx                     ; set the next pointer

.insert.link:
    mov [rdi + channel_info.send], rsp                     ; set the send pointer

    mov rax, rsp                                           ; get the current stack
    and rax, ~0x0fff                                       ; find the dump area

    mov rcx, [rdi + channel_info.coop]                     ; get the coop info pointer
    xor rsi, rsi                                           ; set the .done function address
    xor rdi, rdi                                           ; set the .done function context
    mov r11, [rsp + 32]                                    ; set the code resumption address
    lea r8, [rsp + 40]                                     ; set the old stack ptr

    call coop_push                                         ; dump task registers

    xor rdx, rdx                                           ; the main thread dump area is not known
    jmp coop_switch                                        ; switch to the main thread

; receives a message from the channel
; rdi - ptr to channel structure
; rsi - ptr to a slot where the message will be stored
; rax - returns a message if no error, or negative value indicating an error
channel_recv:
    mov rax, [rdi + channel_info.send]                     ; get the current send pointer
    test rax, rax                                          ; check if it is null
    jz .insert                                             ; if null, insert the message

    push rsi                                               ; save the message slot
    push rdi                                               ; save the channel pointer

.direct:
    mov rsi, 1                                             ; just schedule and no loop
    mov rdi, [rdi + channel_info.coop]                     ; get the coop info pointer
    mov rcx, rax                                           ; set the current stack context
    lea rdx, .direct.done                                  ; set the .done function address
; mov r9, [rax + channel_node.ptr]                       ; get the resume pointer
    call coop_noop_ex                                      ; pretend to noop

    pop rdi                                                ; get the channel pointer
    pop rsi                                                ; get the message pointer

    mov rax, [rdi + channel_info.send]                     ; get the current send pointer
    mov rcx, [rax + channel_node.val]                      ; get the message value
    mov [rsi], rcx                                         ; set the message into the slot

    mov rcx, [rax + channel_node.next]                     ; get the next node
    mov [rdi + channel_info.send], rcx                     ; set the send pointer

    xor rax, rax                                           ; set the return value to 0
    ret

.direct.done:
    add rsp, 32                                            ; clean the sender stack by removing node from the stack
    ret                                                    ; pretend returned, expected sender to be resumed

.insert:
    sub rsp, 32                                            ; reserve slot for a node
    mov [rsp + channel_node.val], rsi                      ; set the message holder

    lea rax, .insert.done                                  ; get the resume address
    mov [rsp + channel_node.ptr], rax                      ; set the message pointer

    mov qword [rsp + channel_node.next], 0                 ; set the next pointer
    mov qword [rsp + channel_node.last], 0                 ; set the last pointer

    mov rcx, [rdi + channel_info.recv]                     ; get the current recv pointer
    test rcx, rcx                                          ; check if it is null

    jz .insert.link                                        ; if null, skip relinking
    mov [rsp + channel_node.next], rcx                     ; set the next pointer

.insert.link:
    mov [rdi + channel_info.recv], rsp                     ; set the recv pointer

    mov rax, rsp                                           ; get the current stack
    and rax, ~0x0fff                                       ; find the dump area

    mov rcx, [rdi + channel_info.coop]                     ; get the coop info pointer
    xor rsi, rsi                                           ; set the .done function address
    xor rdi, rdi                                           ; set the .done function context
    mov r11, [rsp + 32]                                    ; set the code resumption address
    lea r8, [rsp + 40]                                     ; set the old stack ptr

    call coop_push                                         ; dump task registers

    xor rdx, rdx                                           ; the main thread dump area is not known
    jmp coop_switch                                        ; switch to the main thread

.insert.done:
    xor rax, rax                                           ; set the return value to 0
    jmp r11                                                ; simply resume after channel_recv
