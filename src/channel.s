; contains the hand-off channel implementation working with the coop in a non-blocking
; single-threaded environment; the channel is a FIFO queue, solely based on the linked list
; backed up by multiple stacks of each enqueued sender and receiver

    CHANNEL_NODE_SIZE equ 24
    CHANNEL_INFO_SIZE equ 56

    struc channel_node
    .val resq 1                                            ; value of the message
    .ptr resq 1                                            ; pointer to the resume
    .next resq 1                                           ; pointer to the next node
    endstruc

    struc channel_info
    .coop resq 1                                           ; pointer to the coop info
    .size resq 1                                           ; number of participants
    .free resq 1                                           ; pointer to the free node
    .send_head resq 1                                      ; pointer to the send head
    .send_tail resq 1                                      ; pointer to the send tail
    .recv_head resq 1                                      ; pointer to the recv head
    .recv_tail resq 1                                      ; pointer to the recv tail
    endstruc

    section .text
    global channel_init, channel_free, channel_send, channel_recv
    extern coop_noop_ex, coop_pull, coop_push, coop_switch, stdout_printf

; initializes a hand-off channel
; rdi - ptr to uninitialized structure
; rsi - ptr to already initialized coop info structure
; rdx - number of participants in the channel
; rax - returns 0 if no error, or negative value indicating an error
channel_init:

; the channel structure is 40 bytes long, so we need to allocate zeroed memory
; which has to be aligned to 8 bytes boundary

    mov rcx, CHANNEL_INFO_SIZE / 8                         ; x iterations, each 8 bytes
    xor rax, rax                                           ; source value is 0
    rep stosq                                              ; fill all bytes with 0
    sub rdi, CHANNEL_INFO_SIZE                             ; rewind the pointer

; the coop info and size are known at this point, so we can set them
; the other pointers are set to 0 and will be popullated later when needed

    mov [rdi + channel_info.coop], rsi                     ; set the coop info pointer
    mov [rdi + channel_info.size], rdx                     ; set the number of participants

; simply return, the RAX is already set to 0

    ret

; frees the hand-off channel instance
; rdi - ptr to channel structure
; rsi - 0 or 1 indicating whether to wait for all participants
; rax - returns 0 if no error, or negative value indicating an error
channel_free:

; initially decrements the number of participants and checks if it is 0
; only the last participant can free the channel or unblock the waiting task

    dec qword [rdi + channel_info.size]                    ; decrement the number of participants
    mov rax, [rdi + channel_info.size]                     ; get the number of participants

    test rax, rax                                          ; check if it is 0
    jz .zero                                               ; if 0, free the channel

    test rsi, rsi                                          ; check if we need to wait
    jz .exit                                               ; if not, free the channel

; the channel_info.free will contain a task that is waiting for all participants
; to finish, but it cannot complete now; it will enter the coop preemptive loop

    mov [rdi + channel_info.free], rsp                     ; set the free pointer

; to push the current task registers, we need to find the dump area
; the dump area is aligned to 0x1000, so we can use the stack pointer

    mov rax, rsp                                           ; get the current stack
    and rax, ~0x0fff                                       ; find the dump area

; finally we need to prepare the coop info, resumption address (used in .done)
; we don't need to pass r11 and r8, because they will be overriden by the noop

    mov rcx, [rdi + channel_info.coop]                     ; get the coop info pointer
    call coop_push                                         ; dump task registers, never fails

    xor rdx, rdx                                           ; the main thread dump area is not known
    jmp coop_switch                                        ; switch to the main thread

; the zero code has to successfully return to the caller, but also trigger
; the channel_info.free function pointer, so the caller will be resumed

.zero:
    mov rax, [rdi + channel_info.free]                     ; get the free pointer
    test rax, rax                                          ; check if it is null
    jz .exit                                               ; if null, exit

; the channel_info.free identifies the stack of the task we need to switch to
; behind the channel_info.free there is a pointer to the resume address after

    mov rsi, 1                                             ; just schedule and no loop
    push rdi                                               ; save the channel pointer
    mov rdi, [rdi + channel_info.coop]                     ; get the coop info pointer
    mov rcx, rax                                           ; set the expected stack context
    lea rdx, .done                                         ; set the .done function address
    call coop_noop_ex                                      ; pretend to noop

; the noop_ex in theory may fail, the panic will be used only
; to log the reason for further troubleshooting

    test rax, rax                                          ; check if the noop was successful
    js channel_panic                                       ; if not, panic

; to prevent any accidental references to the channel at all, the channel
; structure is filled with 0s, so the caller will not be able to use it

    pop rdi                                                ; get the channel pointer
    mov rcx, CHANNEL_INFO_SIZE / 8                         ; x iterations, each 8 bytes
    xor rax, rax                                           ; source value is 0
    rep stosq                                              ; fill all bytes with 0

; now we can naturally complete the current task and return to the caller
; the noop should trigger the .done function in the correct context

.exit:
    xor rax, rax                                           ; set the return value to 0
    ret

; the .done function is called when the noop is completed
; it executes in the corrent context, so the code just returns
; we don't assume any registers are preserved, just the callee saved ones
; but it is guaranteed by the coop_noop_ex behaviour

.done:
    xor rax, rax                                           ; set the return value to 0
    ret

; sends a message to the channel
; rdi - ptr to channel structure
; rsi - ptr to message or just a message
; rax - returns 0 if no error, or negative value indicating an error
channel_send:

; initially distinguishes between the direct and insert model

    mov rax, [rdi + channel_info.recv_head]                ; get the current rx pointer
    test rax, rax                                          ; check if it is null
    jz .insert                                             ; if null, insert the message

.direct:

; in the direct mode we need to remember passed parameters
; so that after calling the coop_noop_ex we can restore them

    push rsi                                               ; save the message pointer
    push rdi                                               ; save the channel pointer

; the coop_noop_ex will be called with the current stack
; without running th event loop, the callback will be picked
; in the near future, when the actual receiver alredy consumed
; passed message from the sender stack

    mov rsi, 1                                             ; just schedule and no loop
    mov rdi, [rdi + channel_info.coop]                     ; get the coop info pointer

    xor rcx, rcx                                           ; set the current stack context
    lea rdx, .direct.resume                                ; set the .resume function address
    call coop_noop_ex                                      ; pretend to noop

; the noop_ex in theory may fail, the panic will be used only
; to log the reason for further troubleshooting

    test rax, rax                                          ; check if the noop was successful
    js channel_panic                                       ; if not, panic

; now we can restore the parameters and set the message
; in order to use them when waking up the receiver

    pop rdi                                                ; get the channel pointer
    pop rsi                                                ; get the message pointer

; the channel_node.ptr behind the channel_info.recv_head is the resume address
; needed to be placed on the stack, picked up automatically by the coop_pull

    mov rax, [rdi + channel_info.recv_head]                ; get the current rx pointer
    mov rcx, [rax + channel_node.ptr]                      ; get the resume pointer
    push rcx                                               ; set where pull will resume

; the channel_node.val contains a slot where the message will be stored
; on the receiver stack, so we need to place inside the message value (RSI)

    mov rcx, [rax + channel_node.val]                      ; get the message slot
    mov [rcx], rsi                                         ; set the message value

; the channel_node.next contains a pointer to the next node
; we need to shift the linked list to consume the message

    mov rcx, [rax + channel_node.next]                     ; get the next node
    mov [rdi + channel_info.recv_head], rcx                ; set the recv pointer

; the node can be the last one, in such case we need to clear the tail

    test rcx, rcx                                          ; check if it is null
    jnz .direct.exit                                       ; if not null, skip the cleanup
    mov [rdi + channel_info.recv_tail], rcx                ; if null, set the tail pointer

; finally we need to pull the receiver registers, but we are the sender
; so we need to find the dump area and jump there, the coop_pull will
; restore the context and resume the receiver already placed in RCX

.direct.exit:
    and rax, ~0x0fff                                       ; find the dump area
    jmp coop_pull                                          ; restore the context and resume

; the .resume function is called when the noop_ex is completed, already
; when the receiver consumed the message, so we can just clean the stack
; and return to the caller, the sendr will be resumed in the correct context

.direct.resume:
    xor rax, rax                                           ; set the return value to 0
    add rsp, 16                                            ; clean the stack
    ret

; the insert mode handles the case when the receiver is not ready
; the message is inserted into the channel queue

.insert:

; reserve space for the message node, which is 8x bytes long
; and set the message value to the passed message, the ptr
; will point to the resume address, just above the node

    sub rsp, CHANNEL_NODE_SIZE                             ; reserve slot for a node
    mov [rsp + channel_node.val], rsi                      ; set the message payload

    mov rax, [rsp + CHANNEL_NODE_SIZE]                     ; get the resume address
    mov [rsp + channel_node.ptr], rax                      ; set the message pointer

    mov qword [rsp + channel_node.next], 0                 ; set the next pointer

; the previous send node will be extracted

    mov rcx, [rdi + channel_info.send_tail]                ; get the current tail pointer
    test rcx, rcx                                          ; check if it is null

; optionally the next node has to be linked within FIFO linked list

    jz .insert.create                                      ; if null, skip linking
    mov [rcx + channel_node.next], rsp                     ; set the next pointer
    mov [rdi + channel_info.send_tail], rsp                ; set the tail pointer
    jmp .insert.dump                                       ; skip creating a new node

; the channel_info.send_head will contain the curret node

.insert.create:
    mov [rdi + channel_info.send_head], rsp                ; set the head pointer
    mov [rdi + channel_info.send_tail], rsp                ; set the tail pointer

; we need to dump the current task registers, because we block and jump
; later to the coop event loop in the main task thread

.insert.dump:
    mov rax, rsp                                           ; get the current stack
    and rax, ~0x0fff                                       ; find the dump area

; critical values are stored in R11 and R8, pointing at the resume address
; and the old stack pointer, so we need to set them before calling the push

    mov rcx, [rdi + channel_info.coop]                     ; get the coop info pointer
    mov r11, [rsp + CHANNEL_NODE_SIZE]                     ; set the code resumption address
    lea r8, [rsp + CHANNEL_NODE_SIZE + 8]                  ; set the old stack ptr
    call coop_push                                         ; dump task registers, never fails

; finally we need to jump into the main thread for event loop

    xor rdx, rdx                                           ; the main thread dump area is not known
    jmp coop_switch                                        ; switch to the main thread

; receives a message from the channel
; rdi - ptr to channel structure
; rsi - ptr to a slot where the message will be stored
; rax - returns a message if no error, or negative value indicating an error
channel_recv:

; initially distinguishes between the direct and insert model

    mov rax, [rdi + channel_info.send_head]                ; get the current send pointer
    test rax, rax                                          ; check if it is null
    jz .insert                                             ; if null, insert the message

.direct:

; in the direct mode we need to remember passed parameters
; so that after calling the coop_noop_ex we can restore them

    push rsi                                               ; save the message slot
    push rdi                                               ; save the channel pointer

; the coop_noop_ex will be called with the sender stack
; without running th event loop, the callback will be picked
; in the near future, when the actual receiver alredy consumed
; passed message from the sender stack

    mov rsi, 1                                             ; just schedule and no loop
    mov rdi, [rdi + channel_info.coop]                     ; get the coop info pointer
    mov rcx, rax                                           ; set the current stack context
    lea rdx, .direct.done                                  ; set the .done function address
    call coop_noop_ex                                      ; pretend to noop

; the noop_ex in theory may fail, the panic will be used only
; to log the reason for further troubleshooting

    test rax, rax                                          ; check if the noop was successful
    js channel_panic                                       ; if not, panic

; now we can restore the parameters and set the message
; in order to use them when waking up the receiver

    pop rdi                                                ; get the channel pointer
    pop rsi                                                ; get the message slot

; the channel_node.val behind the channel_info.send_head is the message value
; which needs to be placed in the slot

    mov rax, [rdi + channel_info.send_head]                ; get the current send pointer
    mov rcx, [rax + channel_node.val]                      ; get the message value
    mov [rsi], rcx                                         ; set the message into the slot

; the channel_info.send_head nodes are shifted, consuming the node

    mov rcx, [rax + channel_node.next]                     ; get the next node
    mov [rdi + channel_info.send_head], rcx                ; set the send pointer

; the node can be the last one, in such case we need to clear the tail

    test rcx, rcx                                          ; check if it is null
    jnz .direct.exit                                       ; if not null, skip the cleanup
    mov [rdi + channel_info.send_tail], rcx                ; if null, set the tail pointer

; finally we report the success to the receiver, letting it consume the message
; while the sender still blocks and waits for the quick noop completion

.direct.exit:
    xor rax, rax                                           ; set the return value to 0
    ret

; when the noop_ex is completed, the sender will be resumed (not the sender)
; and it means that we need to clean the stack and return the succeed value

.direct.done:
    add rsp, CHANNEL_NODE_SIZE                             ; clean the sender stack by removing node from the stack
    ret                                                    ; pretend returned, expected sender to be resumed

; the insert mode handles the case when the sender is not ready
; the message slot is inserted into the channel queue

.insert:

; reserve space for the message node, which is 8x bytes long
; and set the message value to the passed slot, the ptr
; will point to the resume address, just above the node

    sub rsp, CHANNEL_NODE_SIZE                             ; reserve slot for a node
    mov [rsp + channel_node.val], rsi                      ; set the message holder

    lea rax, .insert.done                                  ; get the resume address
    mov [rsp + channel_node.ptr], rax                      ; set the message pointer

    mov qword [rsp + channel_node.next], 0                 ; set the next pointer

; the previous recv node will be extracted

    mov rcx, [rdi + channel_info.recv_tail]                ; get the current head pointer
    test rcx, rcx                                          ; check if it is null

; optionally the next node will point at the previous recv

    jz .insert.create                                      ; if null, skip relinking
    mov [rcx + channel_node.next], rsp                     ; set the next pointer
    mov [rdi + channel_info.recv_tail], rsp                ; set the tail pointer
    jmp .insert.dump                                       ; skip creating a new node

; the channel_info.recv_head will contain the curret node

.insert.create:
    mov [rdi + channel_info.recv_head], rsp                ; set the recv pointer
    mov [rdi + channel_info.recv_tail], rsp                ; set the tail pointer

; we need to dump the current task registers, because we block and jump
; later to the coop event loop in the main task thread

.insert.dump:
    mov rax, rsp                                           ; get the current stack
    and rax, ~0x0fff                                       ; find the dump area

; critical values are stored in R11 and R8, pointing at the resume address
; and the old stack pointer, so we need to set them before calling the push

    mov rcx, [rdi + channel_info.coop]                     ; get the coop info pointer
    mov r11, [rsp + CHANNEL_NODE_SIZE]                     ; set the code resumption address
    lea r8, [rsp + CHANNEL_NODE_SIZE + 8]                  ; set the old stack ptr
    call coop_push                                         ; dump task registers, never fails

; finally we need to jump into the main thread for event loop

    xor rdx, rdx                                           ; the main thread dump area is not known
    jmp coop_switch                                        ; switch to the main thread

; the .done function is called when the sender completes the coop_pull
; from the receiver stack, so R11 holds the receiver resume address
; the receiver will be resumed first, and the sender still blocks in the noop
; the receiver stack is already cleaned by the coop_push with R8 + R11

.insert.done:
    xor rax, rax                                           ; set the return value to 0
    jmp r11                                                ; simply resume after channel_recv

; panics when non-recoverable error occurs, never returns
; rax - error code
channel_panic:
    ud2
