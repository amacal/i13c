    section .text
    global _main
    extern main, sys_exit

_main:
    call main
    mov edi, eax
    call sys_exit
