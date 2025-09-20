SECTIONS
{
    ENTRY(parquet_start)
    . = 0x400000;

    .text 0x400000 : { *(.text .text.*) }
    .rodata 0x500000 : { *(.rodata .rodata.*) }
    .data 0x600000 : { *(.data .data.*) }
    .bss 0x700000 : { *(.bss) *(COMMON) }

    /DISCARD/ : {
        *(.comment)
        *(.note*)
        *(.eh_frame*)
        *(.eh_frame_hdr*)
        *(.debug*)
    }
}
