SECTIONS
{
    ENTRY(thrift_start)
    . = 0x400000;

    .text 0x400000 : { *(.text) }
    .rodata 0x500000 : { *(.rodata) }
    .data 0x600000 : { *(.data) }
    .bss 0x700000 : { *(.bss) *(COMMON) }

    /DISCARD/ : {
        *(.comment)
        *(.note*)
        *(.eh_frame*)
        *(.eh_frame_hdr*)
    }
}
