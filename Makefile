AR=ar
LD=ld
CC=gcc
NASM=nasm

CFLAGS=-Wall -Wextra -Wno-incompatible-pointer-types -Wno-int-conversion \
	   -static -g -O1 -fno-pic -fno-pie -mno-shstk -fcf-protection=none \
	   -mno-red-zone -fno-merge-constants -fno-stack-protector \
	   -Wno-maybe-uninitialized -Wno-builtin-declaration-mismatch -MMD -MP

LDFLAGS=-static -no-pie -z noexecstack -nostdlib
NASMFLAGS=-f elf64

BINOUTPUT=bin/i13c
TESTOUTPUT=bin/test

BINDIR=bin
SRCDIR=src
OBJDIR=obj
TMPDIR=tmp

SRCS_C=$(wildcard $(SRCDIR)/*.c)
SRCS_ASM=$(wildcard $(SRCDIR)/*.s)

OBJS=$(patsubst $(SRCDIR)/%.c, $(OBJDIR)/%.c.o, $(SRCS_C)) \
     $(patsubst $(SRCDIR)/%.s, $(OBJDIR)/%.s.o, $(SRCS_ASM))

-include $(OBJS:.o=.d)

build: $(BINOUTPUT) $(TESTOUTPUT)

run: $(BINOUTPUT)
	@$(BINOUTPUT)

test: $(TESTOUTPUT)
	@$(TESTOUTPUT)

debug: $(BINOUTPUT)
	@edb --run $(BINOUTPUT) 2> /dev/null

$(BINOUTPUT): $(OBJDIR)/main.s.o $(OBJS)
	@mkdir -p bin
	@$(LD) -T src/main.ld $(LDFLAGS) -o $@ $^

$(TESTOUTPUT): $(OBJDIR)/runner.s.o $(OBJS)
	@mkdir -p bin
	@$(LD) -T src/runner.ld $(LDFLAGS) -o $@ $^

$(OBJDIR)/%.c.o: $(SRCDIR)/%.c
	@mkdir -p $(OBJDIR)
	@$(CC) $(CFLAGS) -c $< -o $@

$(OBJDIR)/%.s.o: $(SRCDIR)/%.s
	@mkdir -p $(OBJDIR)
	@$(NASM) $(NASMFLAGS) $< -o $@

clean:
	@rm -rf $(OBJDIR) $(TMPDIR) $(BINDIR)

.PHONY: build run debug clean
