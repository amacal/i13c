AR=ar
LD=ld
CC=gcc
NASM=nasm

CFLAGS=-Wall -Wextra -Wno-incompatible-pointer-types -Wno-int-conversion \
	   -static -g -O1 -fno-pic -fno-pie -mno-shstk -fcf-protection=none \
	   -mno-red-zone -fno-merge-constants -fno-stack-protector -MMD -MP

LDFLAGS=-static -no-pie -z noexecstack -nostdlib
NASMFLAGS=-f elf64
RUSTFLAGS=-C relocation-model=static

BINOUTPUT=bin/i13c
LIBOUTPUT=bin/libi13c.a

BINDIR=bin
SRCDIR=src
OBJDIR=obj
TMPDIR=tmp

SRCS_C=$(wildcard $(SRCDIR)/*.c)
SRCS_ASM=$(wildcard $(SRCDIR)/*.s)

OBJS=$(patsubst $(SRCDIR)/%.c, $(OBJDIR)/%.c.o, $(SRCS_C)) \
     $(patsubst $(SRCDIR)/%.s, $(OBJDIR)/%.s.o, $(SRCS_ASM))

-include $(OBJS:.o=.d)

build: $(BINOUTPUT) $(LIBOUTPUT)

run: $(BINOUTPUT)
	@$(BINOUTPUT)

debug: $(BINOUTPUT)
	@edb --run $(BINOUTPUT) 2> /dev/null

test: $(LIBOUTPUT)
	@CARGO_TARGET_DIR=$(TMPDIR) RUSTFLAGS="$(RUSTFLAGS)" cargo test --all-targets -- --test-threads=1 | tr '\n' '@' | sed 's/-- [^@]*@//g' | tr '@' '\n'

$(LIBOUTPUT): $(OBJS)
	@mkdir -p bin
	$(AR) rcs $@ $^

$(BINOUTPUT): $(OBJDIR)/main.s.o $(OBJS)
	@mkdir -p bin
	$(LD) -T src/main.ld $(LDFLAGS) -o $@ $^

$(OBJDIR)/%.c.o: $(SRCDIR)/%.c
	@mkdir -p $(OBJDIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(OBJDIR)/%.s.o: $(SRCDIR)/%.s
	@mkdir -p $(OBJDIR)
	$(NASM) $(NASMFLAGS) $< -o $@

clean:
	@rm -rf $(OBJDIR) $(TMPDIR) $(BINDIR)

.PHONY: build run test debug clean
