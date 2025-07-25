VERSION = 0.1.0
RELEASE_DIR = release
TARBALL_DIR = $(RELEASE_DIR)/i13c-$(VERSION)
DEB_DIR = $(RELEASE_DIR)/i13c-$(VERSION)-deb
DEB_NAME = i13c-$(VERSION)-amd64.deb
TARBALL_NAME = i13c-$(VERSION)-linux-x86_64.tar.gz

AR=ar
LD=ld
CC=gcc
NASM=nasm

CFLAGS_COMMON=-Wall -Wextra -ffreestanding -march=native \
	   -static -g -O1 -fno-pic -fno-pie -mno-shstk -fcf-protection=none \
	   -mno-red-zone -fno-merge-constants -fno-stack-protector \
	   -Wno-maybe-uninitialized -Wno-builtin-declaration-mismatch -MMD -MP \
		 -ffunction-sections -fdata-sections

CFLAGS_MAIN   = $(CFLAGS_COMMON)
CFLAGS_TEST   = $(CFLAGS_COMMON) -DI13C_TESTS
CFLAGS_THRIFT = $(CFLAGS_COMMON) -DI13C_THRIFT

LDFLAGS=-static -no-pie -z noexecstack -nostdlib --gc-sections
NASMFLAGS_COMMON=-f elf64

NASMFLAGS_MAIN   = $(NASMFLAGS_COMMON)
NASMFLAGS_TEST   = $(NASMFLAGS_COMMON) -D I13C_TESTS
NASMFLAGS_THRIFT = $(NASMFLAGS_COMMON) -D I13C_THRIFT

BINOUTPUT=bin/i13c
TESTOUTPUT=bin/i13c-tests
THRIFTOUTPUT=bin/i13c-thrift

BINDIR=bin
SRCDIR=src
TMPDIR=tmp

OBJDIR_MAIN   = obj/i13c-main
OBJDIR_TESTS  = obj/i13c-tests
OBJDIR_THRIFT = obj/i13c-thrift

SRCS_C=$(wildcard $(SRCDIR)/*.c)
SRCS_ASM=$(wildcard $(SRCDIR)/*.s)

OBJS_MAIN   := $(patsubst $(SRCDIR)/%.c, $(OBJDIR_MAIN)/%.c.o, $(SRCS_C)) \
               $(patsubst $(SRCDIR)/%.s, $(OBJDIR_MAIN)/%.s.o, $(SRCS_ASM))

OBJS_TEST   := $(patsubst $(SRCDIR)/%.c, $(OBJDIR_TESTS)/%.c.o, $(SRCS_C)) \
               $(patsubst $(SRCDIR)/%.s, $(OBJDIR_TESTS)/%.s.o, $(SRCS_ASM))

OBJS_THRIFT := $(patsubst $(SRCDIR)/%.c, $(OBJDIR_THRIFT)/%.c.o, $(SRCS_C)) \
               $(patsubst $(SRCDIR)/%.s, $(OBJDIR_THRIFT)/%.s.o, $(SRCS_ASM))

-include $(OBJS_MAIN:.o=.d)
-include $(OBJS_TEST:.o=.d)
-include $(OBJS_THRIFT:.o=.d)

$(BINOUTPUT): $(OBJDIR_MAIN)/main.s.o $(OBJS_MAIN)
	@mkdir -p bin
	@$(LD) -T src/main.ld $(LDFLAGS) -o $@ $^

$(TESTOUTPUT): $(OBJDIR_TESTS)/runner.s.o $(OBJS_TEST)
	@mkdir -p bin
	@$(LD) -T src/runner.ld $(LDFLAGS) -o $@ $^

$(THRIFTOUTPUT): $(OBJDIR_THRIFT)/thrift.s.o $(OBJS_THRIFT)
	@mkdir -p bin
	@$(LD) -T src/thrift.ld $(LDFLAGS) -o $@ $^

$(OBJDIR_MAIN)/%.c.o: $(SRCDIR)/%.c
	@mkdir -p $(dir $@)
	@$(CC) $(CFLAGS_MAIN) -c $< -o $@

$(OBJDIR_TESTS)/%.c.o: $(SRCDIR)/%.c
	@mkdir -p $(dir $@)
	@$(CC) $(CFLAGS_TEST) -c $< -o $@

$(OBJDIR_THRIFT)/%.c.o: $(SRCDIR)/%.c
	@mkdir -p $(dir $@)
	@$(CC) $(CFLAGS_THRIFT) -c $< -o $@

$(OBJDIR_MAIN)/%.s.o: $(SRCDIR)/%.s
	@mkdir -p $(dir $@)
	@$(NASM) $(NASMFLAGS_MAIN) $< -o $@

$(OBJDIR_TESTS)/%.s.o: $(SRCDIR)/%.s
	@mkdir -p $(dir $@)
	@$(NASM) $(NASMFLAGS_TEST) $< -o $@

$(OBJDIR_THRIFT)/%.s.o: $(SRCDIR)/%.s
	@mkdir -p $(dir $@)
	@$(NASM) $(NASMFLAGS_THRIFT) $< -o $@

.PHONY: clean
clean:
	@rm -rf $(OBJDIR_MAIN) $(OBJDIR_TESTS) $(OBJDIR_THRIFT) $(TMPDIR) $(BINDIR) $(RELEASE_DIR)

.PHONY: build
build: $(BINOUTPUT) $(TESTOUTPUT) $(THRIFTOUTPUT)

.PHONY: release
release: $(RELEASE_DIR)/$(TARBALL_NAME) $(RELEASE_DIR)/$(DEB_NAME)

$(RELEASE_DIR)/$(TARBALL_NAME):
	@mkdir -p $(TARBALL_DIR)
	@cp bin/i13c-thrift $(TARBALL_DIR)/
	@cp LICENSE README.md $(TARBALL_DIR)/
	@tar -czvf $@ -C $(RELEASE_DIR) $(notdir $(TARBALL_DIR))

$(RELEASE_DIR)/$(DEB_NAME):
	@mkdir -p $(DEB_DIR)/DEBIAN
	@mkdir -p $(DEB_DIR)/usr/bin
	cp bin/i13c-thrift $(DEB_DIR)/usr/bin/
	chmod 755 $(DEB_DIR)/usr/bin/i13c-thrift
	echo "Package: i13c" >  $(DEB_DIR)/DEBIAN/control
	echo "Version: $(VERSION)-1" >> $(DEB_DIR)/DEBIAN/control
	echo "Section: utils" >>       $(DEB_DIR)/DEBIAN/control
	echo "Priority: optional" >>  $(DEB_DIR)/DEBIAN/control
	echo "Architecture: amd64" >> $(DEB_DIR)/DEBIAN/control
	echo "Maintainer: Adrian Macal <adma@amacal.pl>" >> $(DEB_DIR)/DEBIAN/control
	echo "Description: Ultra-lightweight x86_64 tools" >> $(DEB_DIR)/DEBIAN/control
	dpkg-deb --build $(DEB_DIR) $(RELEASE_DIR)/$(DEB_NAME)

.PHONY: lint
lint:
	@clang-format --dry-run --Werror $(SRCDIR)/*.c $(SRCDIR)/*.h
	@bash -c '\
		nasmfmt -ii 4 -ci 60 $(SRCDIR)/*.s; \
		git diff --exit-code $(SRCDIR)/*.s; \
		ret=$$?; \
		git checkout -- $(SRCDIR)/*.s; \
		exit $$ret'

.PHONY: fix
fix:
	@clang-format -i $(SRCDIR)/*.c $(SRCDIR)/*.h
	@nasmfmt -ii 4 -ci 60 $(SRCDIR)/*.s

.PHONY: run
run: $(BINOUTPUT)
	@$(BINOUTPUT)

.PHONY: test
test: $(TESTOUTPUT)
	@$(TESTOUTPUT)

.PHONY: thrift
thrift: $(THRIFTOUTPUT)
	@$(THRIFTOUTPUT)

.PHONY: debug
debug: $(BINOUTPUT)
	@edb --run $(BINOUTPUT) 2> /dev/null

.PHONY: thrift-dump-01
thrift-dump-01: $(THRIFTOUTPUT)
	@dd if=data/test01.parquet skip=18152 bs=1 count=579 status=none | $(THRIFTOUTPUT) > data/test01.thrift

.PHONY: thrift-dump-02
thrift-dump-02: $(THRIFTOUTPUT)
	@dd if=data/test02.parquet skip=5939 bs=1 count=14110 status=none | $(THRIFTOUTPUT) > data/test02.thrift

.PHONY: thrift-dump-03
thrift-dump-03: $(THRIFTOUTPUT)
	@dd if=data/test03.parquet skip=3408 bs=1 count=6841 status=none | $(THRIFTOUTPUT) > data/test03.thrift

.PHONY: thrift-dump-04
thrift-dump-04: $(THRIFTOUTPUT)
	@dd if=data/test04.parquet skip=38843 bs=1 count=1160 status=none | $(THRIFTOUTPUT) > data/test04.thrift

.PHONY: thrift-dump-05
thrift-dump-05: $(THRIFTOUTPUT)
	@dd if=data/test05.parquet skip=590 bs=1 count=1321 status=none | $(THRIFTOUTPUT) > data/test05.thrift
