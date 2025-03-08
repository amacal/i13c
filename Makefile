AR=ar
LD=ld
CC=gcc
NASM=nasm

CFLAGS=-Wall -Wextra -g -O1 -fno-pic -fno-pie -mno-shstk -fcf-protection=none -mno-red-zone -fno-merge-constants
LDFLAGS=-static -no-pie -z noexecstack -nostdlib
NASMFLAGS=-f elf64

BINOUTPUT=bin/admac
LIBOUTPUT=bin/libadmac.a

BINDIR=bin
SRCDIR=src
OBJDIR=obj
TMPDIR=tmp

SRCS_C=$(wildcard $(SRCDIR)/*.c)
SRCS_ASM=$(wildcard $(SRCDIR)/*.s)
OBJS=$(patsubst $(SRCDIR)/%.c, $(OBJDIR)/%.c.o, $(SRCS_C)) \
     $(patsubst $(SRCDIR)/%.s, $(OBJDIR)/%.s.o, $(SRCS_ASM))

build: $(BINOUTPUT) $(LIBOUTPUT)

run: $(BINOUTPUT)
	@$(BINOUTPUT)

debug: $(BINOUTPUT)
	@edb --run $(BINOUTPUT) 2> /dev/null

test: $(LIBOUTPUT)
	@CARGO_TARGET_DIR=$(TMPDIR) cargo test --all-targets | grep -v '^-- '

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
