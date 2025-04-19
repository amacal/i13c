; io_uring_params structure - used to initialize and manage the I/O ring
; Size - 120 bytes

    struc io_uring_params
; Submission Queue (SQ) Configuration - 40 bytes
    .sq_entries resd 1                                     ; Number of submission queue entries
    .cq_entries resd 1                                     ; Number of completion queue entries
    .flags resd 1                                          ; Flags for setup
    .sq_thread_cpu resd 1                                  ; CPU for SQ thread
    .sq_thread_idle resd 1                                 ; Idle time for SQ thread
    .resv resd 5                                           ; Reserved for future use

; Submission Queue Offsets - 40 bytes
    .sq_off_head resd 1                                    ; Offset of SQ head
    .sq_off_tail resd 1                                    ; Offset of SQ tail
    .sq_off_ring_mask resd 1                               ; Ring mask for SQ
    .sq_off_ring_entries resd 1                            ; Number of entries in SQ ring
    .sq_off_flags resd 1                                   ; Flags related to SQ
    .sq_off_dropped resd 1                                 ; Dropped submissions counter
    .sq_off_array resd 1                                   ; Offset of submission array
    .sq_off_resv1 resd 1                                   ; Reserved
    .sq_off_user_data resq 1                               ; User-defined data

; Completion Queue (CQ) Offsets - 40 bytes
    .cq_off_head resd 1                                    ; Offset of CQ head
    .cq_off_tail resd 1                                    ; Offset of CQ tail
    .cq_off_ring_mask resd 1                               ; Ring mask for CQ
    .cq_off_ring_entries resd 1                            ; Number of entries in CQ ring
    .cq_off_overflow resd 1                                ; Overflow counter for CQ
    .cq_off_cqes resd 1                                    ; Offset to CQEs (Completion Queue Entries)
    .cq_off_flags resd 1                                   ; CQ-related flags
    .cq_off_resv1 resd 1                                   ; Reserved
    .cq_off_user_data resq 1                               ; User-defined data
    endstruc

; io_uring_sqe structure - Single Submission Queue Entry (SQE)
; Used to describe a single I/O operation to be submitted
; Size - 64 bytes

    struc io_uring_sqe
; Operation and I/O Parameters - 8 bytes
    .opcode resb 1                                         ; Operation type (e.g., read, write)
    .flags resb 1                                          ; SQE-specific flags
    .ioprio resw 1                                         ; I/O priority
    .fd resd 1                                             ; File descriptor for the I/O operation

; I/O Data and Addressing - 32 bytes
    .offset resq 1                                         ; Offset within the file
    .addr resq 1                                           ; Address of the buffer
    .len resd 1                                            ; Length of the I/O operation (e.g., buffer size)
    .opflags resd 1                                        ; Operaion flags
    .user_data resq 1                                      ; User-defined data (returned on completion)

; Padding - 24 bytes
    .padding resb 24                                       ; Reserved for alignment and future use
    endstruc

; io_uring_cqe structure - Single Completion Queue Entry (CQE)
; Contains information about the result of a completed I/O operation
; Size - 16 bytes

    struc io_uring_cqe
; Completion Information - 16 bytes
    .user_data resq 1                                      ; User-defined data from the SQE
    .result resd 1                                         ; Result of the I/O operation (return value or error code)
    .flags resd 1                                          ; Completion-specific flags
    .reserved resq 2                                       ; Reserved for future extensions
    endstruc

; timespec structure - Time Specification
; Represents a time value with seconds and nanoseconds
; Size - 16 bytes
    struc timespec
    .tv_sec resq 1                                         ; Seconds since the epoch
    .tv_nsec resq 1                                        ; Nanoseconds within the second
    endstruc

; coop_info structure - Cooperative Preemption Management
; Manages I/O ring and task scheduling data
; Size - 264 bytes

    struc coop_info
; File Descriptor and Registers - 136 bytes
    .fd resq 1                                             ; I/O ring file descriptor
    .regs resq 16                                          ; Saved registers for context switching

; Transmit Queue (TX) - 56 bytes
    .tx_ptr resq 1                                         ; Mapped address of the TX ring
    .tx_len resq 1                                         ; Length of the TX ring mapping
    .tx_head resq 1                                        ; Pointer to the TX head
    .tx_tail resq 1                                        ; Pointer to the TX tail
    .tx_mask resq 1                                        ; Pointer to the TX ring mask
    .tx_indx resq 1                                        ; Pointer to the TX index array
    .tx_loop resq 1                                        ; Number of entries in flight

; Receive Queue (RX) - 56 bytes
    .rx_ptr resq 1                                         ; Mapped address of the RX ring
    .rx_len resq 1                                         ; Length of the RX ring mapping
    .rx_head resq 1                                        ; Pointer to the RX head
    .rx_tail resq 1                                        ; Pointer to the RX tail
    .rx_mask resq 1                                        ; Pointer to the RX ring mask
    .rx_indx resq 1                                        ; Pointer to the RX index array
    .rx_cqes resq 1                                        ; Pointer to the RX CQE entries

; Transmit Queue Entries (SQEs) - 16 bytes
    .sq_ptr resq 1                                         ; Mapped address of the SQE array
    .sq_len resq 1                                         ; Length of the SQE array
    endstruc

    section .text
    global coop_init, coop_free, coop_spawn, coop_loop, coop_pull, coop_push, coop_switch, coop_noop, coop_noop_ex, coop_timeout, coop_openat, coop_close, coop_read

; initializes cooperative preemption
; rdi - ptr to the uninitialized structure
; rsi - size (in slots) of the completion queue
; rax - returns 0 if no error, or negative value indicating an error
coop_init:
    sub rsp, 8                                             ; align the stack
    push rdi                                               ; remember ptr to a coop struct

; prepare io_uring_params
    sub rsp, 120                                           ; reserve 120 bytes on stack
    mov rcx, 15                                            ; 15 iterations, each 8 bytes
    xor rax, rax                                           ; source value is 0
    mov rdi, rsp                                           ; destination pointer
    rep stosq                                              ; fill 120 bytes with 0

    mov rdi, rsi                                           ; submission queue size
    mov rsi, rsp                                           ; ptr to io_uring_params
    mov rax, 425                                           ; io_uring_setup syscall
    syscall

    test rax, rax                                          ; check for error
    js .exit                                               ; if error, exit

    mov rdx, [rsp + 120]                                   ; restore ptr to a coop struct
    mov [rdx + coop_info.fd], rax                          ; store obtained uring file descriptor
    mov qword [rdx + coop_info.tx_loop], 0                 ; zero the number of entries in flight

; map IORING_OFF_SQ_RING

    mov esi, [rsp + io_uring_params.sq_entries]            ; sq_entries
    shl esi, 2                                             ; * size of SQE slot (4 bytes)
    add esi, [rsp + io_uring_params.sq_off_array]          ; + sq_off.array
    mov [rdx + coop_info.tx_len], rsi                      ; remember it in coop_info

    xor rdi, rdi                                           ; addr = NULL
    mov rdx, 0x0003                                        ; PROT_READ | PROT_WRITE
    mov r10, 0x8001                                        ; MAP_SHARED | MAP_POPULATE
    mov r8, rax                                            ; uring file descriptor
    mov r9, 0x00000000                                     ; IORING_OFF_SQ_RING
    mov rax, 9                                             ; mmap syscall
    syscall

    test rax, rax                                          ; check for error
    js .fail_sq_ring                                       ; if error, clean and exit

; copy IORING_OFF_SQ_RING pointers

    mov rdx, [rsp + 120]                                   ; copy from stack ptr to uring
    mov [rdx + coop_info.tx_ptr], rax                      ; store pointer to mapped TX ring

    mov ebx, [rsp + io_uring_params.sq_off_head]           ; load SQ head offset
    add rbx, rax                                           ; make TX head pointer
    mov [rdx + coop_info.tx_head], rbx                     ; store TX head pointer

    mov ebx, [rsp + io_uring_params.sq_off_tail]           ; load SQ tail offset
    add rbx, rax                                           ; make TX tail pointer
    mov [rdx + coop_info.tx_tail], rbx                     ; store TX tail pointer

    mov ebx, [rsp + io_uring_params.sq_off_ring_mask]      ; load SQ ring mask offset
    add rbx, rax                                           ; make TX ring mask pointer
    mov [rdx + coop_info.tx_mask], rbx                     ; store TX ring mask pointer

    mov ebx, [rsp + io_uring_params.sq_off_array]          ; load SQ array offset
    add rbx, rax                                           ; make TX index pointer
    mov [rdx + coop_info.tx_indx], rbx                     ; store TX index pointer

; map IORING_OFF_CQ_RING

    mov esi, [rsp + io_uring_params.cq_entries]            ; cq_entries
    shl esi, 4                                             ; * size of CQE slot (16 bytes)
    add esi, [rsp + io_uring_params.cq_off_cqes]           ; + sq_off.cqes
    mov [rdx + coop_info.rx_len], rsi                      ; remember it in coop_info

    mov r8, [rdx + coop_info.fd]                           ; restore uring fd
    xor rdi, rdi                                           ; addr = NULL
    mov rdx, 0x0003                                        ; PROT_READ | PROT_WRITE
    mov r10, 0x8001                                        ; MAP_SHARED | MAP_POPULATE
    mov r9, 0x08000000                                     ; IORING_OFF_CQ_RING
    mov rax, 9                                             ; mmap syscall
    syscall

    test rax, rax                                          ; check for error
    js .fail_cq_ring                                       ; if error, clean and exit

; copy IORING_OFF_CQ_RING pointers

    mov rdx, [rsp + 120]                                   ; copy from stack ptr to uring
    mov [rdx + coop_info.rx_ptr], rax                      ; store pointer to mapped RX ring

    mov ebx, [rsp + io_uring_params.cq_off_head]           ; load CQ head offset
    add rbx, rax                                           ; make RX head pointer
    mov [rdx + coop_info.rx_head], rbx                     ; store RX head pointer

    mov ebx, [rsp + io_uring_params.cq_off_tail]           ; load CQ tail offset
    add rbx, rax                                           ; make RX tail pointer
    mov [rdx + coop_info.rx_tail], rbx                     ; store RX tail pointer

    mov ebx, [rsp + io_uring_params.cq_off_ring_mask]      ; load CQ ring mask offset
    add rbx, rax                                           ; make RX ring mask pointer
    mov [rdx + coop_info.rx_mask], rbx                     ; store RX ring mask pointer

    mov ebx, [rsp + io_uring_params.cq_off_cqes]           ; load CQ entries offset
    add rbx, rax                                           ; make RX entries pointer
    mov [rdx + coop_info.rx_cqes], rbx                     ; store RX entries pointer

    mov ebx, [rsp + io_uring_params.cq_off_ring_entries]   ; load CQ ring entries offset
    add rbx, rax                                           ; make RX ring entries pointer
    mov [rdx + coop_info.rx_indx], rbx                     ; store RX ring entries pointer

; map IORING_OFF_SQES

    mov esi, [rsp + io_uring_params.sq_entries]            ; sq_entries
    imul rsi, rsi, 64                                      ; * size of SQE slot (64 bytes)
    mov [rdx + coop_info.sq_len], rsi                      ; remember it in coop_info

    mov r8, [rdx + coop_info.fd]                           ; restore uring fd
    xor rdi, rdi                                           ; addr = NULL
    mov rdx, 0x0003                                        ; PROT_READ | PROT_WRITE
    mov r10, 0x8001                                        ; MAP_SHARED | MAP_POPULATE
    mov r9, 0x10000000                                     ; IORING_OFF_SQES
    mov rax, 9                                             ; mmap syscall
    syscall

    test rax, rax                                          ; check for error
    js .fail_cq_entries                                    ; if error, clean and exit

; copy IORING_OFF_SQES pointers

    mov rdx, [rsp + 120]                                   ; copy from stack ptr to uring
    mov [rdx + coop_info.sq_ptr], rax                      ; store pointer to mapped TX entries

; successful exit

    mov rax, 0
    jmp .exit

.fail_cq_entries:
    push r12
    mov r12, rax                                           ; save error code

    mov rdx, [rsp + 120]                                   ; copy from stack ptr to uring
    mov rdi, [rdx + coop_info.rx_ptr]                      ; load RX ring pointer
    mov rsi, [rdx + coop_info.rx_len]                      ; load RX ring size
    mov rax, 11                                            ; munmap syscall
    syscall

    mov rax, r12                                           ; restore error code
    pop r12

.fail_cq_ring:
    push r12
    mov r12, rax                                           ; save error code

    mov rdx, [rsp + 120]                                   ; copy from stack ptr to uring
    mov rdi, [rdx + coop_info.tx_ptr]                      ; load TX ring pointer
    mov rsi, [rdx + coop_info.tx_len]                      ; load TX ring size
    mov rax, 11                                            ; munmap syscall
    syscall

    mov rax, r12                                           ; restore error code
    pop r12

.fail_sq_ring:
    push r12
    mov r12, rax                                           ; save error code

    mov rdx, [rsp + 120]                                   ; copy from stack ptr to uring
    mov rdi, [rdx + coop_info.fd]                          ; load uring fd from the coop struct

    mov rax, 3                                             ; close syscall
    syscall

    mov rax, r12                                           ; restore error code
    pop r12

.exit:
    add rsp, 136
    ret

; frees cooperative preemption
; rdi - ptr to the initialized structure
; rax - returns 0 if no error, or negative value indicating the first error
coop_free:
    mov rdx, rdi                                           ; remember in not affected register
    xor r12, r12                                           ; clear rcx to store any error code

    mov rdi, [rdx + coop_info.sq_ptr]                      ; load TX entries pointer
    mov rsi, [rdx + coop_info.sq_len]                      ; load TX ring size
    mov rax, 11                                            ; munmap syscall
    syscall

    test rax, rax                                          ; check for error
    jns .unmap_rx                                          ; if no error, unmap RX ring

    mov r12, rax                                           ; save error code

.unmap_rx:
    mov rdi, [rdx + coop_info.rx_ptr]                      ; load RX ring pointer
    mov rsi, [rdx + coop_info.rx_len]                      ; load RX ring size
    mov rax, 11                                            ; munmap syscall
    syscall

    test rax, rax                                          ; check for error
    jns .unmap_tx                                          ; if no error, unmap TX ring

    test r12, r12                                          ; check for previous error
    jnz .unmap_tx                                          ; if no error, unmap TX ring

    mov r12, rax                                           ; save error code

.unmap_tx:
    mov rdi, [rdx + coop_info.tx_ptr]                      ; load TX ring pointer
    mov rsi, [rdx + coop_info.tx_len]                      ; load TX ring size
    mov rax, 11                                            ; munmap syscall
    syscall

    test rax, rax                                          ; check for error
    jns .close_uring                                       ; if no error, close uring fd

    test r12, r12                                          ; check for previous error
    jnz .close_uring                                       ; if no error, close uring fd

    mov r12, rax                                           ; save error code

.close_uring:
    mov rax, 3                                             ; close syscall
    mov rdi, [rdx + coop_info.fd]                          ; load file descriptor from the coop struct
    syscall

    test rax, rax                                          ; check for error
    jns .exit                                              ; if no error, exit

    test r12, r12                                          ; check for previous error
    jnz .exit                                              ; if no error, exit

    mov r12, rax                                           ; save error code

.exit:
    mov rax, r12                                           ; restore the first error code
    ret

; spawns a cooperative preemption task
; rdi - ptr to the initialized structure
; rsi - ptr to the function to be executed
; rdx - ptr to the function argument
; rcx - size of the function argument
; rax - returns 0 if no error, or negative value indicating an error
coop_spawn:
    push rdi                                               ; remember ptr to a coop struct
    push rsi                                               ; remember ptr to a function
    push rdx                                               ; remember ptr to a function argument
    push rcx                                               ; remember size of the function argument
    sub rsp, 8                                             ; make 16 bytes space on stack

; allocate task stack

    xor rdi, rdi                                           ; addr = NULL
    mov rsi, 0x1000                                        ; 4096 bytes
    mov rdx, 0x0003                                        ; PROT_READ | PROT_WRITE
    mov r10, 0x8022                                        ; MAP_PRIVATE | MAP_ANONYMOUS
    mov r8, -1                                             ; fd = -1
    mov r9, 0                                              ; offset = 0
    mov rax, 9                                             ; mmap syscall
    syscall

    test rax, rax                                          ; check for error
    js .exit                                               ; if error, exit

    mov [rsp], rax                                         ; save new stack pointer

; copy function context

    mov rcx, [rsp + 8]                                     ; load size of the function argument
    test rcx, rcx                                          ; check if size is 0
    jz .skip_copy                                          ; if 0, skip copy

    mov rsi, [rsp + 16]                                    ; load function context
    lea rdi, [rax + 0x0080]                                ; just after dumped registers
    mov [rsp + 16], rdi                                    ; save new function context
    rep movsb                                              ; copy function argument

.skip_copy:

; pull TX offsets

    mov rdi, [rsp + 32]                                    ; restore ptr to a coop struct
    mov r10, [rdi + coop_info.fd]                          ; load uring file descriptor
    mov rdx, [rdi + coop_info.tx_tail]                     ; load TX tail pointer
    mov rcx, [rdi + coop_info.tx_mask]                     ; load TX mask pointer
    mov rsi, [rdi + coop_info.tx_indx]                     ; load TX index pointer
    mov rdi, [rdi + coop_info.sq_ptr]                      ; load TX entries pointer

; find next SQE slot

    mov ecx, [rcx]                                         ; load TX mask value
    mov eax, [rdx]                                         ; load TX tail value

    and rax, rcx                                           ; mask TX tail value
    mov r11, rax                                           ; save current TX tail
    shl rax, 6                                             ; * size of SQE slot (64 bytes)
    add rdi, rax                                           ; add TX entries pointer

; clear SQE slot

    xor rax, rax                                           ; clear rax
    mov rcx, 8                                             ; 64 iterations, each 8 bytes
    rep stosq                                              ; fill 64 bytes with 0
    sub rdi, 64                                            ; move back to the SQE beginning

; prepare stack

    mov rax, [rsp]                                         ; load addr of the regs
    lea rcx, coop_end                                      ; load return address
    lea r8, [rax + 0x0ff8]                                 ; remember new stack pointer
    mov [rax + 0x0ff8], rcx                                ; set task return address
    mov rcx, [rsp + 32]                                    ; load addr of the coop struct

    mov qword [rdi + io_uring_sqe.user_data], rax          ; set user data
    mov [rsi + r11 * 4], r11d                              ; set TX index

    mov rdi, [rsp + 16]                                    ; load function argument
    mov r11, [rsp + 24]                                    ; load function pointer
    lea rsi, .done                                         ; load function callback

    call coop_push                                         ; dump task registers
    inc dword [rdx]                                        ; increment TX tail

; call uring submission with noop (0x00) operation

    mov rdi, r10                                           ; uring file descriptor
    mov rsi, 1                                             ; 1 SQE
    xor rdx, rdx                                           ; 0 CQE
    xor r10, r10                                           ; no flags
    xor r8, r8                                             ; no sigset
    xor r9, r9                                             ; no sigset
    mov rax, 426                                           ; io_uring_enter syscall
    syscall

    test rax, rax                                          ; check for error
    js .fail_enter                                         ; if error, clean and exit

    cmp rax, 0                                             ; check for error
    je .fail_one                                           ; if 0, clean and exit

    mov rdi, [rsp + 32]                                    ; load ptr to a coop struct
    inc qword [rdi + coop_info.tx_loop]                    ; increment number of entries in flight

    mov rax, 0                                             ; success
    jmp .exit                                              ; exit

.fail_one:
    mov rax, -33

.fail_enter:
    push r12
    mov r12, rax                                           ; save error code

    mov rdi, [rsp]                                         ; the allocated stack
    mov rsi, 0x1000                                        ; the size of the stack
    mov rax, 11                                            ; munmap syscall
    syscall

    mov rax, r12                                           ; restore error code
    pop r12

.exit:
    add rsp, 40                                            ; clean stack usage
    ret

.done:
    mov rax, 0                                             ; success
    jmp r11                                                ; continue

; runs a cooperative preemption loop
; rdi - ptr to the initialized coop structure
; rax - returns 0 if no error, or negative value indicating an error
coop_loop:
    push rdi                                               ; remember ptr to a coop struct

    mov rax, [rdi + coop_info.tx_loop]                     ; load number of entries in flight
    cmp rax, 0                                             ; check if there are any entries in flight
    jng .exit                                              ; if not, exit

.loop:
    mov rdi, [rsp]                                         ; load ptr to a coop struct

; call uring submission

    mov rdi, [rdi + coop_info.fd]                          ; load uring fd
    xor rsi, rsi                                           ; 0 SQE
    mov rdx, 0x01                                          ; 1 CQE
    mov r10, 0x01                                          ; IORING_ENTER_GETEVENTS
    xor r8, r8                                             ; no sigset
    xor r9, r9                                             ; no sigset
    mov rax, 426                                           ; io_uring_enter syscall
    syscall

    test rax, rax                                          ; check for error
    js .exit                                               ; if error, exit

; compare RX head and tail

    mov rdi, [rsp]                                         ; load ptr to a coop struct
    mov rdx, [rdi + coop_info.rx_head]                     ; load RX head pointer
    mov r11, [rdi + coop_info.rx_tail]                     ; load RX tail pointer
    mov rcx, [rdi + coop_info.rx_mask]                     ; load RX mask pointer
    mov rsi, [rdi + coop_info.rx_cqes]                     ; load RX entries pointer

    mov edx, [rdx]                                         ; load RX head value
    mov r11d, [r11]                                        ; load RX tail value
    mov ecx, [rcx]                                         ; load RX mask value

    cmp rdx, r11                                           ; compare RX head and tail
    je .loop                                               ; if equal, loop

; find next CQE slot

    and rdx, rcx                                           ; mask RX head value
    shl rdx, 4                                             ; * size of CQE slot (16 bytes)
    add rsi, rdx                                           ; now we have CQE entry

; move RX head to the next entry

    mov rdx, [rdi + coop_info.rx_head]                     ; load RX head pointer
    inc dword [rdx]                                        ; increment RX head

; dump all main registers to the stack

    pop rdi
    mov r11, [rsp]                                         ; return address
    lea rdx, [rdi + coop_info.regs]                        ; load addr of the regs

    mov [rdx + 0*8], rax                                   ; rax
    mov [rdx + 1*8], rbx                                   ; rbx
    mov [rdx + 2*8], rcx                                   ; rcx
    mov [rdx + 3*8], rdx                                   ; rdx = registers
    mov [rdx + 4*8], rsi                                   ; rsi = CQE entry
    mov [rdx + 5*8], rdi                                   ; rdi = coop info
    mov [rdx + 6*8], r8                                    ; r8
    mov [rdx + 7*8], r9                                    ; r9
    mov [rdx + 8*8], r10                                   ; r10
    mov [rdx + 9*8], r11                                   ; r11 = return address
    mov [rdx + 10*8], r12                                  ; r12
    mov [rdx + 11*8], r13                                  ; r13
    mov [rdx + 12*8], r14                                  ; r14
    mov [rdx + 13*8], r15                                  ; r15
    mov [rdx + 14*8], rbp                                  ; rbp
    mov [rdx + 15*8], rsp                                  ; rsp

; extract stack pointer from the CQE entry

    mov rax, rsi                                           ; copy CQE entry pointer
    mov rax, [rsi + io_uring_cqe.user_data]                ; load stack pointer of the task

; now we can switch to the task stack

    call coop_pull                                         ; restore task registers

; to continue execution, we need to jump to the .done first

    dec qword [rcx + coop_info.tx_loop]                    ; decrement number of entries in flight
    jmp rsi                                                ; call .done within the task stack

.exit:
    pop rdi
    ret

; performs a noop operation
; rdi - ptr to the initialized coop structure
; rsi - flags - 0 to continue looping, 1 to exit
; rdx - ptr to the return address or 0 to use the default
; rcx - ptr to the stack context or 0 to use the default
; rax - returns 0 if no error, or negative value indicating an error
coop_noop:
    xor rcx, rcx                                           ; set default value to 0
    xor rsi, rsi                                           ; set default value to 0
    mov rdx, [rsp]                                         ; set default return address

coop_noop_ex:
    push rdx                                               ; remember return address
    push rsi                                               ; remember flags
    push rdi                                               ; remember ptr to a coop struct
    push rcx                                               ; remember ptr to the stack context

; pull TX offsets

    mov r10, [rdi + coop_info.fd]                          ; load uring file descriptor
    mov rdx, [rdi + coop_info.tx_tail]                     ; load TX tail pointer
    mov rcx, [rdi + coop_info.tx_mask]                     ; load TX mask pointer
    mov rsi, [rdi + coop_info.tx_indx]                     ; load TX index pointer
    mov rdi, [rdi + coop_info.sq_ptr]                      ; load TX entries pointer

; find next SQE slot

    mov ecx, [rcx]                                         ; load TX mask value
    mov eax, [rdx]                                         ; load TX tail value

    and rax, rcx                                           ; mask TX tail value
    mov r11, rax                                           ; save current TX tail
    imul rax, rax, 64                                      ; * size of SQE slot (64 bytes)
    add rdi, rax                                           ; add TX entries pointer

; clear SQE slot

    xor rax, rax                                           ; clear rax
    mov rcx, 8                                             ; 64 iterations, each 8 bytes
    rep stosq                                              ; fill 64 bytes with 0
    sub rdi, 64                                            ; move back to the SQE beginning

; prepare stack

    pop rax                                                ; load addr of th current stack
    mov r8, rax                                            ; save addr of the current stack
    test rax, rax                                          ; check if we need to use the default
    jnz .skip_stack                                        ; if not 0, skip stack setup

    mov rax, rsp                                           ; load addr of the stack
    lea r8, [rsp + 32]                                     ; remember old stack ptr

.skip_stack:
    and rax, ~0x0fff                                       ; compute addr of the regs
    mov rcx, [rsp]                                         ; load addr of the coop struct

    mov qword [rdi + io_uring_sqe.user_data], rax          ; set user data
    mov [rsi + r11 * 4], r11d                              ; set TX index

    mov r11, [rsp + 16]                                    ; load function callback
    lea rsi, .done                                         ; load function pointer

    call coop_push                                         ; dump task registers
    inc dword [rdx]                                        ; increment TX tail

; call uring submission with noop (0x00) operation

    mov rdi, r10                                           ; uring file descriptor
    mov rsi, 1                                             ; 1 SQE
    xor rdx, rdx                                           ; 0 CQE
    xor r10, r10                                           ; no flags
    xor r8, r8                                             ; no sigset
    xor r9, r9                                             ; no sigset
    mov rax, 426                                           ; io_uring_enter syscall
    syscall

    test rax, rax                                          ; check for error
    js .exit                                               ; if error, clean and exit

    cmp rax, 0                                             ; check for error
    je .fail_one                                           ; if 0, clean and exit

    mov rdi, [rsp]                                         ; load ptr to a coop struct
    inc qword [rdi + coop_info.tx_loop]                    ; increment number of entries in flight

    mov rdx, [rsp + 8]                                     ; load flags
    test rdx, rdx                                          ; check if we need to exit
    jnz .exit                                              ; if not zero, then exit

    xor rdx, rdx                                           ; clean the main dump location
    add rsp, 32                                            ; clean the local stack usage
    jmp coop_switch                                        ; switch to the main thread

.fail_one:
    mov rax, -33

.exit:
    add rsp, 24                                            ; clean stack usage
    ret

.done:
    mov eax, [rdx + io_uring_cqe.result]                   ; success
    cdqe                                                   ; sign extend
    jmp r11                                                ; continue task

; performs an open file operation
; rdi - ptr to the initialized coop structure
; rsi - ptr to the file path
; rdx - file open flags
; rcx - file open mode
; rax - returns 0 if no error, or negative value indicating an error
coop_openat:
    sub rsp, 8                                             ; align the stack
    push rsi                                               ; save file path
    push rdx                                               ; save file open flags
    push rcx                                               ; save file open mode
    push rdi                                               ; remember ptr to a coop struct

; pull TX offsets

    mov r10, [rdi + coop_info.fd]                          ; load uring file descriptor
    mov rdx, [rdi + coop_info.tx_tail]                     ; load TX tail pointer
    mov rcx, [rdi + coop_info.tx_mask]                     ; load TX mask pointer
    mov rsi, [rdi + coop_info.tx_indx]                     ; load TX index pointer
    mov rdi, [rdi + coop_info.sq_ptr]                      ; load TX entries pointer

; find next SQE slot

    mov ecx, [rcx]                                         ; load TX mask value
    mov eax, [rdx]                                         ; load TX tail value

    and rax, rcx                                           ; mask TX tail value
    mov r11, rax                                           ; save current TX tail
    shl rax, 6                                             ; * size of SQE slot (64 bytes)
    add rdi, rax                                           ; add TX entries pointer

; clear SQE slot

    xor rax, rax                                           ; clear rax
    mov rcx, 8                                             ; 64 iterations, each 8 bytes
    rep stosq                                              ; fill 64 bytes with 0
    sub rdi, 64                                            ; move back to the SQE beginning

    mov byte [rdi + io_uring_sqe.opcode], 18               ; IORING_OP_OPENAT
    mov dword [rdi + io_uring_sqe.fd], -100                ; AT_FDCWD
    mov rax, [rsp + 8]                                     ; load file mode
    mov [rdi + io_uring_sqe.offset], rax                   ; set file mode
    mov rax, [rsp + 16]                                    ; load file open flags
    mov [rdi + io_uring_sqe.opflags], eax                  ; set fil open flags
    mov rax, [rsp + 24]                                    ; load address of the buffer
    mov [rdi + io_uring_sqe.addr], rax                     ; set addr of the file path

; prepare stack

    mov rax, rsp                                           ; load addr of the stack
    and rax, ~0x0fff                                       ; compute addr of the regs
    mov rcx, [rsp]                                         ; load addr of the coop struct

    mov qword [rdi + io_uring_sqe.user_data], rax          ; set user data
    mov [rsi + r11 * 4], r11d                              ; set TX index

    lea r8, [rsp + 48]
    mov r11, [rsp + 40]                                    ; load function callback
    lea rsi, .done                                         ; load function pointer

    call coop_push                                         ; dump task registers
    inc dword [rdx]                                        ; increment TX tail

; call uring submission with noop (0x00) operation

    mov rdi, r10                                           ; uring file descriptor
    mov rsi, 1                                             ; 1 SQE
    xor rdx, rdx                                           ; 0 CQE
    xor r10, r10                                           ; no flags
    xor r8, r8                                             ; no sigset
    xor r9, r9                                             ; no sigset
    mov rax, 426                                           ; io_uring_enter syscall
    syscall

    test rax, rax                                          ; check for error
    js .exit                                               ; if error, clean and exit

    cmp rax, 0                                             ; check for error
    je .fail_one                                           ; if 0, clean and exit

    mov rdi, [rsp]                                         ; load ptr to a coop struct
    inc qword [rdi + coop_info.tx_loop]                    ; increment number of entries in flight

    xor rdx, rdx                                           ; clean the main dump location
    add rsp, 48                                            ; clean the local stack usage
    jmp coop_switch                                        ; switch to the main thread

.fail_one:
    mov rax, -33

.exit:
    add rsp, 40                                            ; clean stack usage
    ret

.done:
    mov eax, [rdx + io_uring_cqe.result]                   ; success
    cdqe                                                   ; sign extend
    jmp r11                                                ; continue task

; performs a close operation
; rdi - ptr to the initialized coop structure
; rsi - file descriptor to close
; rax - returns 0 if no error, or negative value indicating an error
coop_close:
    sub rsp, 8                                             ; align the stack
    push rsi                                               ; save file descriptor
    push rdi                                               ; remember ptr to a coop struct

; pull TX offsets

    mov r10, [rdi + coop_info.fd]                          ; load uring file descriptor
    mov rdx, [rdi + coop_info.tx_tail]                     ; load TX tail pointer
    mov rcx, [rdi + coop_info.tx_mask]                     ; load TX mask pointer
    mov rsi, [rdi + coop_info.tx_indx]                     ; load TX index pointer
    mov rdi, [rdi + coop_info.sq_ptr]                      ; load TX entries pointer

; find next SQE slot

    mov ecx, [rcx]                                         ; load TX mask value
    mov eax, [rdx]                                         ; load TX tail value

    and rax, rcx                                           ; mask TX tail value
    mov r11, rax                                           ; save current TX tail
    imul rax, rax, 64                                      ; * size of SQE slot (64 bytes)
    add rdi, rax                                           ; add TX entries pointer

; clear SQE slot

    xor rax, rax                                           ; clear rax
    mov rcx, 8                                             ; 64 iterations, each 8 bytes
    rep stosq                                              ; fill 64 bytes with 0
    sub rdi, 64                                            ; move back to the SQE beginning

    mov byte [rdi + io_uring_sqe.opcode], 19               ; IORING_OP_CLOSE
    mov eax, [rsp + 8]                                     ; load file descriptor
    mov [rdi + io_uring_sqe.fd], rax                       ; set file descriptor

; prepare stack

    mov rax, rsp                                           ; load addr of the stack
    and rax, ~0x0fff                                       ; compute addr of the regs
    mov rcx, [rsp]                                         ; load addr of the coop struct

    mov qword [rdi + io_uring_sqe.user_data], rax          ; set user data
    mov [rsi + r11 * 4], r11d                              ; set TX index

    lea r8, [rsp + 32]
    mov r11, [rsp + 24]                                    ; load function callback
    lea rsi, .done                                         ; load function pointer

    call coop_push                                         ; dump task registers
    inc dword [rdx]                                        ; increment TX tail

; call uring submission with noop (0x00) operation

    mov rdi, r10                                           ; uring file descriptor
    mov rsi, 1                                             ; 1 SQE
    xor rdx, rdx                                           ; 0 CQE
    xor r10, r10                                           ; no flags
    xor r8, r8                                             ; no sigset
    xor r9, r9                                             ; no sigset
    mov rax, 426                                           ; io_uring_enter syscall
    syscall

    test rax, rax                                          ; check for error
    js .exit                                               ; if error, clean and exit

    cmp rax, 0                                             ; check for error
    je .fail_one                                           ; if 0, clean and exit

    mov rdi, [rsp]                                         ; load ptr to a coop struct
    inc qword [rdi + coop_info.tx_loop]                    ; increment number of entries in flight

    xor rdx, rdx                                           ; clean the main dump location
    add rsp, 32                                            ; clean the local stack usage
    jmp coop_switch                                        ; switch to the main thread

.fail_one:
    mov rax, -33

.exit:
    add rsp, 24                                            ; clean stack usage
    ret

.done:
    mov eax, [rdx + io_uring_cqe.result]                   ; success
    cdqe                                                   ; sign extend
    jmp r11                                                ; continue task

; performs a read operation
; rdi - ptr to the initialized coop structure
; rsi - file descriptor to read from
; rdx - ptr to the buffer to read into
; rcx - size of the buffer
; r8 - offset to read from
; rax - returns 0 if no error, or negative value indicating an error
coop_read:
    push r8                                                ; save file offset
    push rsi                                               ; save file descriptor
    push rdx                                               ; save buffer pointer
    push rcx                                               ; save buffer size
    push rdi                                               ; remember ptr to a coop struct

; pull TX offsets

    mov r10, [rdi + coop_info.fd]                          ; load uring file descriptor
    mov rdx, [rdi + coop_info.tx_tail]                     ; load TX tail pointer
    mov rcx, [rdi + coop_info.tx_mask]                     ; load TX mask pointer
    mov rsi, [rdi + coop_info.tx_indx]                     ; load TX index pointer
    mov rdi, [rdi + coop_info.sq_ptr]                      ; load TX entries pointer

; find next SQE slot

    mov ecx, [rcx]                                         ; load TX mask value
    mov eax, [rdx]                                         ; load TX tail value

    and rax, rcx                                           ; mask TX tail value
    mov r11, rax                                           ; save current TX tail
    imul rax, rax, 64                                      ; * size of SQE slot (64 bytes)
    add rdi, rax                                           ; add TX entries pointer

; clear SQE slot

    xor rax, rax                                           ; clear rax
    mov rcx, 8                                             ; 64 iterations, each 8 bytes
    rep stosq                                              ; fill 64 bytes with 0
    sub rdi, 64                                            ; move back to the SQE beginning

    mov byte [rdi + io_uring_sqe.opcode], 22               ; IORING_OP_READ
    mov rax, [rsp + 8]                                     ; load length of the buffer
    mov [rdi + io_uring_sqe.len], eax                      ; required one structure
    mov rax, [rsp + 16]                                    ; load address of the buffer
    mov [rdi + io_uring_sqe.addr], rax                     ; set addr of the buffer
    mov rax, [rsp + 24]                                    ; load file descriptor
    mov [rdi + io_uring_sqe.fd], eax                       ; set file descriptor
    mov rax, [rsp + 32]                                    ; load offset
    mov [rdi + io_uring_sqe.offset], rax                   ; set offset

; prepare stack

    mov rax, rsp                                           ; load addr of the stack
    and rax, ~0x0fff                                       ; compute addr of the regs
    mov rcx, [rsp]                                         ; load addr of the coop struct

    mov qword [rdi + io_uring_sqe.user_data], rax          ; set user data
    mov [rsi + r11 * 4], r11d                              ; set TX index

    lea r8, [rsp + 48]
    mov r11, [rsp + 40]                                    ; load function callback
    lea rsi, .done                                         ; load function pointer

    call coop_push                                         ; dump task registers
    inc dword [rdx]                                        ; increment TX tail

; call uring submission with noop (0x00) operation

    mov rdi, r10                                           ; uring file descriptor
    mov rsi, 1                                             ; 1 SQE
    xor rdx, rdx                                           ; 0 CQE
    xor r10, r10                                           ; no flags
    xor r8, r8                                             ; no sigset
    xor r9, r9                                             ; no sigset
    mov rax, 426                                           ; io_uring_enter syscall
    syscall

    test rax, rax                                          ; check for error
    js .exit                                               ; if error, clean and exit

    cmp rax, 0                                             ; check for error
    je .fail_one                                           ; if 0, clean and exit

    mov rdi, [rsp]                                         ; load ptr to a coop struct
    inc qword [rdi + coop_info.tx_loop]                    ; increment number of entries in flight

    xor rdx, rdx                                           ; clean the main dump location
    add rsp, 48                                            ; clean the local stack usage
    jmp coop_switch                                        ; switch to the main thread

.fail_one:
    mov rax, -33

.exit:
    add rsp, 40                                            ; clean stack usage
    ret

.done:
    mov eax, [rdx + io_uring_cqe.result]                   ; success
    cdqe                                                   ; sign extend
    jmp r11                                                ; continue task

; performs a timeout operation
; rdi - ptr to the initialized coop structure
; rsi - number of seconds to wait
; rax - returns 0 if no error, or negative value indicating an error
coop_timeout:
    sub rsp, 16                                            ; space for a timespec struct
    push rdi                                               ; remember ptr to a coop struct

    mov [rsp + 8 + timespec.tv_sec], rsi                   ; set seconds
    mov qword [rsp + 8 + timespec.tv_nsec], 0x00000000     ; set nanoseconds

; pull TX offsets

    mov r10, [rdi + coop_info.fd]                          ; load uring file descriptor
    mov rdx, [rdi + coop_info.tx_tail]                     ; load TX tail pointer
    mov rcx, [rdi + coop_info.tx_mask]                     ; load TX mask pointer
    mov rsi, [rdi + coop_info.tx_indx]                     ; load TX index pointer
    mov rdi, [rdi + coop_info.sq_ptr]                      ; load TX entries pointer

; find next SQE slot

    mov ecx, [rcx]                                         ; load TX mask value
    mov eax, [rdx]                                         ; load TX tail value

    and rax, rcx                                           ; mask TX tail value
    mov r11, rax                                           ; save current TX tail
    imul rax, rax, 64                                      ; * size of SQE slot (64 bytes)
    add rdi, rax                                           ; add TX entries pointer

; clear SQE slot

    xor rax, rax                                           ; clear rax
    mov rcx, 8                                             ; 64 iterations, each 8 bytes
    rep stosq                                              ; fill 64 bytes with 0
    sub rdi, 64                                            ; move back to the SQE beginning

    mov byte [rdi + io_uring_sqe.opcode], 11               ; IORING_OP_TIMEOUT
    mov dword [rdi + io_uring_sqe.len], 1                  ; required one structure

    lea rax, [rsp + 8]                                     ; load addr of the timespec struct
    mov [rdi + io_uring_sqe.addr], rax                     ; set addr of the timespec struct

; prepare stack

    mov rax, rsp                                           ; load addr of the stack
    and rax, ~0x0fff                                       ; compute addr of the regs
    mov rcx, [rsp]                                         ; load addr of the coop struct

    mov qword [rdi + io_uring_sqe.user_data], rax          ; set user data
    mov [rsi + r11 * 4], r11d                              ; set TX index

    lea r8, [rsp + 32]
    mov r11, [rsp + 24]                                    ; load function callback
    lea rsi, .done                                         ; load function pointer

    call coop_push                                         ; dump task registers
    inc dword [rdx]                                        ; increment TX tail

; call uring submission with noop (0x00) operation

    mov rdi, r10                                           ; uring file descriptor
    mov rsi, 1                                             ; 1 SQE
    xor rdx, rdx                                           ; 0 CQE
    xor r10, r10                                           ; no flags
    xor r8, r8                                             ; no sigset
    xor r9, r9                                             ; no sigset
    mov rax, 426                                           ; io_uring_enter syscall
    syscall

    test rax, rax                                          ; check for error
    js .exit                                               ; if error, clean and exit

    cmp rax, 0                                             ; check for error
    je .fail_one                                           ; if 0, clean and exit

    mov rdi, [rsp]                                         ; load ptr to a coop struct
    inc qword [rdi + coop_info.tx_loop]                    ; increment number of entries in flight

    xor rdx, rdx                                           ; clean the main dump location
    add rsp, 32                                            ; clean the local stack usage
    jmp coop_switch                                        ; switch to the main thread

.fail_one:
    mov rax, -33

.exit:
    add rsp, 24                                            ; clean stack usage
    ret

.done:
    mov eax, [rdx + io_uring_cqe.result]                   ; success
    cdqe                                                   ; sign extend
    jmp r11                                                ; continue task

; completes a cooperative preemption task
; rsp - ptr to the stack of the task
; rax - returns 0 if no error, or negative value indicating an error
coop_end:
; find the allocated task and the registers of the main thread

    mov rax, rsp                                           ; current stack
    sub rax, 8
    and rax, ~0x0fff                                       ; load addr of the regs
    mov rdi, [rax + 2*8]                                   ; rcx = coop info
    lea rdx, [rdi + coop_info.regs]                        ; load addr of the regs

; deallocate stack of the minor task

    mov r12, rdx                                           ; remember registers pointer

    mov rdi, rax                                           ; the allocated stack
    mov rsi, 0x1000                                        ; the size of the stack
    mov rax, 11                                            ; munmap syscall
    syscall

    mov rdx, r12                                           ; revert registers pointer
    jmp coop_switch                                        ; switch to the main thread

; switches to the main thread
; rsp - ptr to the stack of the task
; rdx - optional ptr to the main dump
coop_switch:

    test rdx, rdx                                          ; check if dump is provided
    jnz .jump                                              ; if set then jump to the main thread

; find the allocated task and the registers of the main thread

    mov rax, rsp                                           ; current stack
    sub rax, 8
    and rax, ~0x0fff                                       ; load addr of the regs
    mov rdi, [rax + 2*8]                                   ; rcx = coop info
    lea rdx, [rdi + coop_info.regs]                        ; load addr of the regs

; switch back to the main thread

.jump:
    mov rax, [rdx + 0*8]                                   ; rax
    mov rbx, [rdx + 1*8]                                   ; rbx
    mov rcx, [rdx + 2*8]                                   ; rcx
    mov rdx, [rdx + 3*8]                                   ; rdx = registers
    mov rsi, [rdx + 4*8]                                   ; rsi
    mov rdi, [rdx + 5*8]                                   ; rdi = coop info
    mov r8, [rdx + 6*8]                                    ; r8
    mov r9, [rdx + 7*8]                                    ; r9
    mov r10, [rdx + 8*8]                                   ; r10
    mov r11, [rdx + 9*8]                                   ; r11
    mov r12, [rdx + 10*8]                                  ; r12
    mov r13, [rdx + 11*8]                                  ; r13
    mov r14, [rdx + 12*8]                                  ; r14
    mov r15, [rdx + 13*8]                                  ; r15
    mov rbp, [rdx + 14*8]                                  ; rbp
    mov rsp, [rdx + 15*8]                                  ; rsp

    jmp coop_loop                                          ; continue looping

; pushes task registers to the stack
; rax - ptr to the dump area
; rcx - ptr to the coop info
; rsi - ptr to the .done function address
; rdi - ptr to the .done function context
; r11 - ptr to the resumption code
; r8 - ptr to the RSP after resumption
coop_push:
    mov [rax + 0*8], rax                                   ; rax = registers
    mov [rax + 1*8], rbx                                   ; rbx
    mov [rax + 2*8], rcx                                   ; rcx = coop info
    mov [rax + 3*8], rdx                                   ; rdx
    mov [rax + 4*8], rsi                                   ; rsi = .done address
    mov [rax + 5*8], rdi                                   ; rdi = .done context
    mov [rax + 6*8], r8                                    ; r8
    mov [rax + 7*8], r9                                    ; r9
    mov [rax + 8*8], r10                                   ; r10
    mov [rax + 9*8], r11                                   ; r11 = resumption
    mov [rax + 10*8], r12                                  ; r12
    mov [rax + 11*8], r13                                  ; r13
    mov [rax + 12*8], r14                                  ; r14
    mov [rax + 13*8], r15                                  ; r15
    mov [rax + 14*8], rbp                                  ; rbp
    mov [rax + 15*8], r8                                   ; rsp = stack
    ret

; switches to the task stack
; rax - ptr to the dump area
; rsp - contains the return address at the top
; rsi - optional ptr preserved in rdx
; r10 - optional ptr preserved in r10
coop_pull:
    pop r8                                                 ; remember return address

    mov rax, [rax + 0*8]                                   ; rax = registers
    mov rbx, [rax + 1*8]                                   ; rbx
    mov rcx, [rax + 2*8]                                   ; rcx = coop info
    mov rdx, rsi                                           ; rdx = preserved rsi
    mov rsi, [rax + 4*8]                                   ; rsi = .done function
    mov rdi, [rax + 5*8]                                   ; rdi = .done context
    mov r8, r8                                             ; r8
    mov r9, [rax + 7*8]                                    ; r9
    mov r10, r10                                           ; r10 = preserved r10
    mov r11, [rax + 9*8]                                   ; r11 = resumption
    mov r12, [rax + 10*8]                                  ; r12
    mov r13, [rax + 11*8]                                  ; r13
    mov r14, [rax + 12*8]                                  ; r14
    mov r15, [rax + 13*8]                                  ; r15
    mov rbp, [rax + 14*8]                                  ; rbp
    mov rsp, [rax + 15*8]                                  ; rsp = stack

    jmp r8                                                 ; jump back
