    section .text
    global sys_read, sys_write, sys_open, sys_close, sys_fstat, sys_mmap, sys_munmap, sys_pread, sys_exit

; reads data from the file descriptor
; rdi - file descriptor (0 for stdin)
; rsi - buffer to store the read data
; rdx - number of bytes to read
; returns the number of bytes read in rax, or negative on error
sys_read:
    mov rax, 0
    syscall
    ret

; writes data to the file descriptor
; rdi - file descriptor (1 for stdout)
; rsi - buffer containing data to write
; rdx - number of bytes to write
sys_write:
    mov rax, 1
    syscall
    ret

; opens a file and returns its file descriptor
; rdi - pointer to the filename (null-terminated string)
; rsi - flags (0 for read-only)
; rdx - mode (permissions, only used if creating a file)
; returns the file descriptor in rax, or negative on error
sys_open:
    mov rax, 2
    syscall
    ret

; closes a file descriptor
; rdi - file descriptor to close
; returns 0 on success, or negative on error
sys_close:
    mov rax, 3
    syscall
    ret

; retrieves file status information
; rdi - file descriptor (0 for stdin, 1 for stdout, etc.)
; rsi - pointer to a struct stat where the file status will be stored
; returns 0 on success, or negative on error
sys_fstat:
    mov rax, 5
    syscall
    ret

; allocates memory using mmap
; rdi - address hint (0 for any address)
; rsi - length of the memory to allocate
; rdx - protection flags (PROT_READ | PROT_WRITE)
; rcx - flags (MAP_PRIVATE | MAP_ANONYMOUS)
; r8 - file descriptor (0 for anonymous mapping)
; r9 - offset (0 for anonymous mapping)
; returns the address of the allocated memory in rax, or negative on error
sys_mmap:
    mov r10, rcx
    mov rax, 9
    syscall
    ret

; deallocates memory using munmap
; rdi - address of the memory to deallocate
; rsi - length of the memory to deallocate
; returns 0 on success, or negative on error
sys_munmap:
    mov rax, 11
    syscall
    ret

; reads data from a file descriptor into a buffer at a specific offset
; rdi - file descriptor (0 for stdin)
; rsi - buffer to store the read data
; rdx - number of bytes to read
; rcx - offset in the file (0 for beginning)
; returns the number of bytes read in rax, or negative on error
sys_pread:
    mov r10, rcx
    mov rax, 17
    syscall
    ret

; exits the program with the given exit code
; edi - exit code (0 for success, non-zero for error)
; rax - returns 0 if no error, or negative value indicating an error
sys_exit:
    mov rax, 60
    syscall
