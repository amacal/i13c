; contains the hand-off channel implementation working with the coop in a non-blocking
; single-threaded environment; the channel is a FIFO queue, solely based on the linked list
; backed up by multiple stacks of each enqueued sender and receiver; the coop is a single-threaded
; cooperative scheduler, and it's not going to be used in a multi-threaded environment at all!

    EDEADLK EQU -35
    COOP_ALIGNMENT_MASK equ ~0x0fff

    CHANNEL_NODE_SIZE equ 48
    CHANNEL_INFO_SIZE equ 72

    struc channel_node
    .up resq 1                                             ; pointer to the channel
    .val resq 1                                            ; message value or a slot
    .ptr resq 1                                            ; pointer to the resume
    .res resq 1                                            ; node result
    .prev resq 1                                           ; pointer to the prev node
    .next resq 1                                           ; pointer to the next node
    endstruc

    struc channel_info
    .coop resq 1                                           ; pointer to the coop info
    .size resq 1                                           ; number of participants
    .free resq 1                                           ; pointer to the free node
    .send_head resq 1                                      ; pointer to the send head
    .send_tail resq 1                                      ; pointer to the send tail
    .send_size resq 1                                      ; size of the send queue
    .recv_head resq 1                                      ; pointer to the recv head
    .recv_tail resq 1                                      ; pointer to the recv tail
    .recv_size resq 1                                      ; size of the recv queue
    endstruc

    section .text
    global channel_init, channel_free, channel_send, channel_recv, channel_select
    extern coop_noop_ex, coop_pull, coop_push, coop_switch, stdout_printf

; initializes a hand-off channel
; rdi - ptr to uninitialized structure
; rsi - ptr to already initialized coop info structure
; rdx - number of participants in the channel
; rax - returns 0 if no error, or negative value indicating an error
channel_init:

; the channel structure is 8x bytes long, so we need to allocate zeroed memory
; which must be aligned to an 8-byte boundary

    mov rcx, CHANNEL_INFO_SIZE / 8                         ; x iterations, each 8 bytes
    xor rax, rax                                           ; source value is 0
    rep stosq                                              ; fill all bytes with 0
    sub rdi, CHANNEL_INFO_SIZE                             ; rewind the pointer

; the coop info and size are known at this point, so we can set them
; the other pointers are set to 0 and will be populated later when needed

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

; if the number of participants is 0, we can free the channel
; without waiting for the other participants because they are already gone

    test rax, rax                                          ; check if it is 0
    jz .zero                                               ; if 0, free the channel

.check.sender:

; let's check if the any sender will be in the deadlock

    mov rcx, [rdi + channel_info.send_size]                ; get the count of senders
    cmp rax, rcx                                           ; check if we have all senders
    jne .check.receiver                                    ; if not 0, go further

    push rdi                                               ; remember channel_info
    push rsi                                               ; remember channel_free flag
    mov rax, [rdi + channel_info.send_head]                ; get the current send pointer

.check.sender.loop:

    test rax, rax                                          ; check if it is null
    jz .check.sender.loop.exit                             ; if null, go further
    push rax                                               ; remember current node

; the coop_noop_ex will be called with the sender stack
; without running the event loop, the callback will be picked
; in the near future

    mov rsi, 1                                             ; just schedule and no loop
    mov rcx, [rax + channel_node.ptr]                      ; set the sender stack context
    mov rdi, [rdi + channel_info.coop]                     ; get the coop info pointer
    lea rdx, .check.sender.done                            ; set the .done function address
    call coop_noop_ex                                      ; pretend to noop

    pop rax
    mov rdi, [rsp + 8]                                     ; restore channel_info
    mov rax, [rax + channel_node.next]                     ; get the current send pointer
    jmp .check.sender.loop                                 ; continue looping

.check.sender.done:
    mov rax, EDEADLK                                       ; set the deadlock error
    ret

.check.sender.loop.exit:
    pop rsi                                                ; restore channel_free flag
    pop rdi                                                ; restore channel_info
    mov qword [rdi + channel_info.send_size], 0            ; zero the count of senders
    mov qword [rdi + channel_info.send_head], 0            ; zero the send head pointer
    mov qword [rdi + channel_info.send_tail], 0            ; zero the send tail pointer

.check.receiver:

; let's check if the any receiver will be in the deadlock

    mov rcx, [rdi + channel_info.recv_size]                ; get the count of receivers
    cmp rax, rcx                                           ; check if we have all receivers
    jne .check.waiting                                     ; if not 0, go further

    push rdi                                               ; remember channel_info
    push rsi                                               ; remember channel_free flag
    mov rax, [rdi + channel_info.recv_head]                ; get the current recv pointer

.check.receiver.loop:

    test rax, rax                                          ; check if it is null
    jz .check.receiver.loop.exit                           ; if null, go further
    push rax                                               ; remember current node

; the coop_noop_ex will be called with the receiver stack
; without running the event loop, the callback will be picked
; in the near future

    mov rsi, 1                                             ; just schedule and no loop
    mov rcx, rax                                           ; set the sender stack context
    mov rdi, [rdi + channel_info.coop]                     ; get the coop info pointer
    lea rdx, .check.receiver.done                          ; set the .done function address
    call coop_noop_ex                                      ; pretend to noop

    pop rax
    mov rdi, [rsp + 8]                                     ; restore channel_info
    mov rax, [rax + channel_node.next]                     ; get the current recv pointer
    jmp .check.receiver.loop                               ; continue looping

.check.receiver.done:
    mov r11, r10                                           ; prepare next resumption
    mov r8, [rsp + channel_node.ptr]                       ; get the channel_node.ptr
    mov qword [rsp + channel_node.ptr], 0                  ; zero the channel_node.ptr
    mov qword [rsp + channel_node.res], EDEADLK            ; set the result value
    mov rdx, [rsp + channel_node.up]                       ; get the channel_info ptr
    mov rsp, r9                                            ; restore the stack pointer
    jmp r8                                                 ; go into channel_node.ptr

.check.receiver.loop.exit:
    pop rsi                                                ; restore channel_free flag
    pop rdi                                                ; restore channel_info
    mov qword [rdi + channel_info.recv_size], 0            ; zero the count of receivers
    mov qword [rdi + channel_info.recv_head], 0            ; zero the recv head pointer
    mov qword [rdi + channel_info.recv_tail], 0            ; zero the recv tail pointer

.check.waiting:
; if the number of participants is not 0, we need to check if we need to wait
; the channel_info.free will contain a pointer executed by the last participant

    test rsi, rsi                                          ; check if we need to wait
    jz .exit                                               ; if not, free the channel

; the channel_info.free will contain a task that is waiting for all participants
; to finish, but it cannot complete now; it will enter the coop preemptive loop

    mov [rdi + channel_info.free], rsp                     ; set the free pointer

; to push the current task registers, we need to find the dump area
; the dump area is aligned to 0x1000, so we can use the stack pointer

    mov rax, rsp                                           ; get the current stack
    and rax, COOP_ALIGNMENT_MASK                           ; find the dump area

; finally we need to prepare the coop info, resumption address (used in .done)
; we don't need to pass r11 and r8, because they will be overridden by the noop

    mov rcx, [rdi + channel_info.coop]                     ; get the coop info pointer
    call coop_push                                         ; dump task registers, never fails

    xor rdx, rdx                                           ; the main thread dump area is not known
    jmp coop_switch                                        ; switch to the main thread

; the .zero case has to successfully return to the caller, but also trigger
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
; without running the event loop, the callback will be picked
; in the near future, when the actual receiver already consumed
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
    dec qword [rdi + channel_info.recv_size]               ; decrement the recv size

; the node can be the last one, in such case we need to clear the tail

    test rcx, rcx                                          ; check if it is null
    jnz .direct.prev                                       ; if not null, skip the cleanup

    mov [rdi + channel_info.recv_tail], rcx                ; if null, set the tail pointer
    jmp .direct.exit                                       ; skip the prev node cleanup

; if node is not the last one, the newly shifted node
; won't have any prev pointer, so we need to set it to null

.direct.prev:
    mov qword [rcx + channel_node.prev], 0                 ; set the prev pointer

; finally we need to pull the receiver registers, but we are the sender
; so we need to find the dump area and jump there, the coop_pull will
; restore the context and resume the receiver already placed in RCX
; we don't need to clean the stack, anyway it will be restored properly
; RDX will contain the channel pointer, so we can use it later

.direct.exit:
    mov rsi, rdi                                           ; preserve the channel pointer
    and rax, COOP_ALIGNMENT_MASK                           ; find the dump area
    jmp coop_pull                                          ; restore the context and resume

; the .resume function is called when the noop_ex is completed, already
; when the receiver consumed the message, so we can just clean the stack
; and return to the caller, the sender will be resumed in the correct context

.direct.resume:
    xor rax, rax                                           ; set the return value to 0
    add rsp, 16                                            ; clean the stack
    ret

; the insert mode handles the case when the receiver is not ready
; the message is inserted into the channel queue
;
;  ├── sees recv_head == 0
;  │   ├── alloc channel_node on sender's stack
;  │   ├── set val (message), ptr (resume addr)
;  │   ├── link node (head/tail/next)
;  │   ├── call coop_push -> dumps task regs
;  │   └── jmp coop_switch -> resumes event loop

.insert:

; is it possible to send a message to the channel, let's compare counters

    mov rax, [rdi + channel_info.send_size]                ; get the count of senders
    inc rax                                                ; increment the count to account us
    cmp rax, [rdi + channel_info.size]                     ; compare with the size
    jge .insert.deadlock                                   ; report deadlock

; reserve space for the message node, which is 8x bytes long
; and set the message value to the passed message, the ptr
; will point to the resume address, just above the node

    sub rsp, CHANNEL_NODE_SIZE                             ; reserve slot for a node
    mov [rsp + channel_node.val], rsi                      ; set the message payload

; the channel_node.ptr will contain the resume address, it means
; the address of the next instruction after channel_send

    lea rax, [rsp + CHANNEL_NODE_SIZE]                     ; get the resume address
    mov [rsp + channel_node.ptr], rax                      ; set the resume address
    mov [rsp + channel_node.up], rdi                       ; set the channel pointer

    mov qword [rsp + channel_node.res], 0                  ; set the result value
    mov qword [rsp + channel_node.prev], 0                 ; set the prev pointer
    mov qword [rsp + channel_node.next], 0                 ; set the next pointer

; the previous send tail will be replaced

    inc qword [rdi + channel_info.send_size]               ; increment the send size
    mov rcx, [rdi + channel_info.send_tail]                ; get the current tail pointer
    test rcx, rcx                                          ; check if it is null
    jz .insert.create                                      ; if null, skip linking

; optionally the next node has to be linked within FIFO linked list

    mov [rcx + channel_node.next], rsp                     ; set the next pointer
    mov [rdi + channel_info.send_tail], rsp                ; set the tail pointer
    mov [rsp + channel_node.prev], rcx                     ; set the prev pointer
    jmp .insert.dump                                       ; skip creating a new node

; the channel_info.send_head will contain the current node

.insert.create:
    mov [rdi + channel_info.send_head], rsp                ; set the head pointer
    mov [rdi + channel_info.send_tail], rsp                ; set the tail pointer

; we need to dump the current task registers, because we block and jump
; later to the coop event loop in the main task thread

.insert.dump:
    mov rax, rsp                                           ; get the current stack
    and rax, COOP_ALIGNMENT_MASK                           ; find the dump area

; critical values are stored in R11 and R8, pointing at the resume address
; and the old stack pointer, so we need to set them before calling the push

    mov rcx, [rdi + channel_info.coop]                     ; get the coop info pointer
    mov r11, [rsp + CHANNEL_NODE_SIZE]                     ; set the code resumption address
    lea r8, [rsp + CHANNEL_NODE_SIZE + 8]                  ; set the old stack ptr
    call coop_push                                         ; dump task registers, never fails

; finally we need to jump into the main thread for event loop

    xor rdx, rdx                                           ; the main thread dump area is not known
    jmp coop_switch                                        ; switch to the main thread

.insert.deadlock:
    mov rax, EDEADLK                                       ; set the deadlock error
    ret                                                    ; return to the caller

; receives a message from the channel
; rdi - ptr to channel structure
; rsi - ptr to a slot where the message will be stored
; rax - 0 if no error, or negative value indicating an error
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
; in the near future, when the actual receiver already consumed
; passed message from the sender stack

    mov rsi, 1                                             ; just schedule and no loop
    mov rdi, [rdi + channel_info.coop]                     ; get the coop info pointer
    mov rcx, rax                                           ; set the sender stack context
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
    dec qword [rdi + channel_info.send_size]               ; decrement the send size

; the node can be the last one, in such case we need to clear the tail

    test rcx, rcx                                          ; check if it is null
    jnz .direct.prev                                       ; if not null, skip the cleanup

    mov [rdi + channel_info.send_tail], rcx                ; if null, set the tail pointer
    jmp .direct.exit                                       ; skip the prev node cleanup

; if node is not the last one, the newly shifted node
; won't have any prev pointer, so we need to set it to null

.direct.prev:

    mov qword [rcx + channel_node.prev], 0                 ; set the prev pointer

; finally we report the success to the receiver, letting it consume the message
; while the sender still blocks and waits for the quick noop completion

.direct.exit:
    xor rax, rax                                           ; set the return value to 0
    ret

; when the noop_ex is completed, the sender will be resumed (not the receiver)
; and it means that we need to clean the stack and return the succeed value

.direct.done:
    add rsp, CHANNEL_NODE_SIZE                             ; clean the sender stack by removing node from the stack
    ret                                                    ; pretend returned, expected sender to be resumed

; the insert mode handles the case when the sender is not ready
; the message slot is inserted into the channel queue

.insert:

; is it possible to receive a message from the channel, let's compare counters

    mov rax, [rdi + channel_info.recv_size]                ; get the count of receivers
    inc rax                                                ; increment the count to account us
    cmp rax, [rdi + channel_info.size]                     ; compare with the size
    jge .insert.deadlock                                   ; report deadlock

; reserve space for the message node, which is 8x bytes long
; and set the message value to the passed slot, the ptr
; will point to the resume address, just above the node

    sub rsp, CHANNEL_NODE_SIZE                             ; reserve slot for a node
    mov [rsp + channel_node.val], rsi                      ; set the message holder

    lea rax, .insert.done                                  ; get the resume address
    mov [rsp + channel_node.ptr], rax                      ; set the message pointer
    mov [rsp + channel_node.up], rdi                       ; set the channel pointer

    mov qword [rsp + channel_node.res], 0                  ; set the result value
    mov qword [rsp + channel_node.prev], 0                 ; set the prev pointer
    mov qword [rsp + channel_node.next], 0                 ; set the next pointer

; the previous recv tail will be replaced

    inc qword [rdi + channel_info.recv_size]               ; increment the recv size
    mov rcx, [rdi + channel_info.recv_tail]                ; get the current head pointer
    test rcx, rcx                                          ; check if it is null

; optionally the next node will point at the previous recv

    jz .insert.create                                      ; if null, skip relinking
    mov [rcx + channel_node.next], rsp                     ; set the next pointer
    mov [rdi + channel_info.recv_tail], rsp                ; set the tail pointer
    mov [rsp + channel_node.prev], rcx                     ; set the prev pointer
    jmp .insert.dump                                       ; skip creating a new node

; the channel_info.recv_head will contain the current node

.insert.create:
    mov [rdi + channel_info.recv_head], rsp                ; set the recv pointer
    mov [rdi + channel_info.recv_tail], rsp                ; set the tail pointer

; we need to dump the current task registers, because we block and jump
; later to the coop event loop in the main task thread

.insert.dump:
    mov rax, rsp                                           ; get the current stack
    and rax, COOP_ALIGNMENT_MASK                           ; find the dump area

; critical values are stored in R11 and R8, pointing at the resume address
; and the old stack pointer, so we need to set them before calling the push

    mov rcx, [rdi + channel_info.coop]                     ; get the coop info pointer
    mov r8, rsp                                            ; set the old stack ptr
    call coop_push                                         ; dump task registers, never fails

; finally we need to jump into the main thread for event loop

    xor rdx, rdx                                           ; the main thread dump area is not known
    jmp coop_switch                                        ; switch to the main thread

; the .done function is called when the sender completes the coop_pull
; from the receiver stack, so R11 holds the receiver resume address
; the receiver will be resumed first, and the sender still blocks in the noop
; the receiver stack is not cleaned, but left with node + ret address

.insert.done:
    mov rax, [rsp + channel_node.res]                      ; get the result value
    add rsp, CHANNEL_NODE_SIZE                             ; clean the receiver stack
    ret                                                    ; simply resume after channel_recv

.insert.deadlock:
    mov rax, EDEADLK                                       ; set the deadlock error
    ret                                                    ; return to the caller

; selects a message from multiple channels
; rdi - ptr to a null-terminated array of channel pointers
; rsi - ptr to a slot where the message will be stored
; rax - index of a selected channel if no error, or negative value indicating an error
channel_select:

    xor rcx, rcx                                           ; zero the channel index

; iterate over the channels and check if any of them is ready
; it will help us to decide which path to take, the direct or insert

.direct.loop:
    mov rax, [rdi + rcx * 8]                               ; get the channel pointer
    test rax, rax                                          ; check if it is null
    jz .insert                                             ; if null, follow the insert path

    mov rax, [rax + channel_info.send_head]                ; get the current send pointer
    test rax, rax                                          ; check if it is null
    jnz .direct                                            ; if not null, follow the direct path

    inc rcx                                                ; increment the channel index
    jmp .direct.loop                                       ; check the next channel

.direct:
    push rcx                                               ; save the channel index

    mov rdi, [rdi + rcx * 8]                               ; get the channel pointer
    call channel_recv.direct                               ; the function won't block nor fail

    pop rax                                                ; return the channel index
    ret

; the direct path is used when no sender is ready and we need to
; enqueue the message slot in all channels to be one day resumed
; currently RCX contains number of channels, it will be used to
; find number of needed bytes to reserve on the stack

.insert:
    push rdi                                               ; save the array pointer
    push rcx                                               ; save the node counter

    imul rcx, CHANNEL_NODE_SIZE                            ; size of all nodes
    sub rsp, rcx                                           ; reserve space for all nodes
    mov r8, rsp                                            ; remember the array of nodes

    xor r11, r11                                           ; zero the deadlock counter
    xor rcx, rcx                                           ; zero the channel index
    lea rdx, .insert.done                                  ; set the resume address

.insert.loop:
    mov rax, [rdi + rcx * 8]                               ; get the channel pointer
    test rax, rax                                          ; check if it is null
    jz .insert.completed                                   ; if null, complete all inserts

; check if the channel won't be deadlocked

    mov r9, [rax + channel_info.recv_size]                 ; get the count of receivers
    inc r9                                                 ; increment the count to account us
    cmp r9, [rax + channel_info.size]                      ; compare with the size
    jge .insert.deadlock                                   ; report deadlock

; prepare the channel node for the current channel

    mov [r8 + channel_node.val], rsi                       ; set the message holder
    mov [r8 + channel_node.ptr], rdx                       ; set the .done pointer
    mov [r8 + channel_node.up], rax                        ; set the channel pointer

    mov qword [r8 + channel_node.res], 0                   ; set the result value
    mov qword [r8 + channel_node.prev], 0                  ; set the prev pointer
    mov qword [r8 + channel_node.next], 0                  ; set the next pointer

; the previous recv node will be extracted

    inc qword [rax + channel_info.recv_size]               ; increment the recv size
    mov r9, [rax + channel_info.recv_tail]                 ; get the current head pointer
    test r9, r9                                            ; check if it is null

; optionally the next node will point at the previous recv

    jz .insert.create                                      ; if null, skip relinking
    mov [r9 + channel_node.next], r8                       ; set the next pointer
    mov [rax + channel_info.recv_tail], r8                 ; set the tail pointer
    mov [r8 + channel_node.prev], r9                       ; set the prev pointer
    jmp .insert.next                                       ; skip creating a new node

; the channel_info.recv_head will contain the current node

.insert.create:
    mov [rax + channel_info.recv_head], r8                 ; set the recv pointer
    mov [rax + channel_info.recv_tail], r8                 ; set the tail pointer

.insert.next:
    add r8, CHANNEL_NODE_SIZE                              ; move to the next node
    inc rcx                                                ; increment the channel index

    jmp .insert.loop                                       ; check the next channel

.insert.deadlock:
    mov qword [r8 + channel_node.val], 0                   ; set the message holder
    mov qword [r8 + channel_node.ptr], 0                   ; set the .done pointer

    inc r11                                                ; increment the deadlock counter
    jmp .insert.next                                       ; continue the loop

; it may happen that all channels are deadlocked, so we need to
; release the allocated memory and return the error code; luckily
; R8 contains the RSP pointer just before the array of nodes

.insert.completed:

; first compare the deadlock counter with the number of channels

    cmp r11, rcx                                           ; check if we have all deadlocked channels
    jnz .insert.push                                       ; if not, push the nodes

; all channels are deadlocked, so we need to clean the stack

    lea rsp, [r8 + 16]                                     ; rewind the stack pointer
    mov rax, EDEADLK                                       ; set the deadlock error
    ret                                                    ; return to the caller

; now we can safely switch to the main thread, when one sender will
; wake up and .insert.done will be called

.insert.push:

; critical values are stored in R11 and R8, pointing at the resume address
; and the old stack pointer, so we need to set them before calling the push

    mov rax, [rdi]                                         ; get the channel pointer
    mov rcx, [rax + channel_info.coop]                     ; get the coop info pointer
    mov r11, [r8 + 16]                                     ; set the code resumption address
    add r8, 24                                             ; set the old stack ptr

    mov rax, rsp                                           ; get the current stack
    and rax, COOP_ALIGNMENT_MASK                           ; find the dump area
    call coop_push                                         ; dump task registers, never fails

    xor rdx, rdx                                           ; the main thread dump area is not known
    jmp coop_switch                                        ; switch to the main thread

; here is the most complicated part, the .done function is called and
; we need to detect the woken up channel, followed by cleaning up all
; the other channels, including their stacks; luckily the RDX will contain
; the channel pointer, so we can use it to find the channel index

.insert.done:
    xor r10, r10                                           ; zero the channel index
    mov rdi, [rsp - 16]                                    ; get the array pointer
    lea r9, [rsp - 24]                                     ; go at the end of the array
    mov rcx, [r9]                                          ; get the number of nodes
    imul rcx, CHANNEL_NODE_SIZE                            ; size of all nodes
    sub r9, rcx                                            ; find the nodes beginning

; when resume address is on the stack, we can later just call RET

    push r11                                               ; save the resume address

.insert.done.loop:
    mov rcx, [rdi + r10 * 8]                               ; get the channel pointer

    test rcx, rcx                                          ; check if it is null
    jz .insert.exit                                        ; if null, exit

    cmp rdx, rcx                                           ; check if are equal
    jne .insert.done.unlink                                ; if not equal, unlink node

    mov rax, [r9 + channel_node.res]                       ; prepare the return value
    test rax, rax                                          ; check if the value is negative
    cmovns rax, r10                                        ; if not negative, copy index
    jmp .insert.done.continue                              ; continue looping

; the channel node is not resumed, and it has to be unlinked
; from the linked list to nor create any resurrection, but situation
; is more complicated, because the node may be now in the middle

.insert.done.unlink:

; verify if the channel entry was not registered

    mov rsi, [r9 + channel_node.ptr]                       ; get the resume address
    test rsi, rsi                                          ; check if it is null
    jz .insert.done.continue                               ; if null, channel was not added

; fetch prev and next pointers from the current node

    mov rsi, [r9 + channel_node.prev]                      ; get the previous node
    mov r8, [r9 + channel_node.next]                       ; get the next node
    dec qword [rcx + channel_info.recv_size]               ; decrement the recv size

; if prev node exists, prev node has to skip the current node

    test rsi, rsi                                          ; check prev node exists
    jz .insert.done.unlink.prev                            ; if not, skip the cleanup
    mov [rsi + channel_node.next], r8                      ; set the next pointer

; if next node exists, next node has to skip the current node

.insert.done.unlink.prev:
    test r8, r8                                            ; check next node exists
    jz .insert.done.unlink.head                            ; if not, skip the cleanup
    mov [r8 + channel_node.prev], rsi                      ; set the prev pointer

; if recv_head is the current node, we need to point it to the next node

.insert.done.unlink.head:
    mov r11, [rcx + channel_info.recv_head]                ; get the current head pointer
    cmp r11, r9                                            ; check if it is the current node
    jne .insert.done.unlink.tail                           ; if not, skip the cleanup
    mov [rcx + channel_info.recv_head], r8                 ; set the recv pointer to the next node

; if recv_tail is the current node, we need to point it to the prev node

.insert.done.unlink.tail:
    mov r11, [rcx + channel_info.recv_tail]                ; get the current tail pointer
    cmp r11, r9                                            ; check if it is the current node
    jne .insert.done.continue                              ; if not, skip the cleanup
    mov [rcx + channel_info.recv_tail], rsi                ; set the recv pointer to the prev node

; just increase both counters and continue the loop

.insert.done.continue:
    inc r10                                                ; increment the channel index
    add r9, CHANNEL_NODE_SIZE                              ; move to the next node
    jmp .insert.done.loop                                  ; check the next channel

; we did everything, hurrah!

.insert.exit:
    ret                                                    ; simply resume after channel_select

; panics when non-recoverable error occurs, never returns
; rax - error code
channel_panic:
    ud2
