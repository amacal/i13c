VERSION ?= $(shell git describe --tags --abbrev=0 | sed 's/^v//')
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

CFLAGS_MAIN    = $(CFLAGS_COMMON)
CFLAGS_TEST    = $(CFLAGS_COMMON) -DI13C_TESTS
CFLAGS_THRIFT  = $(CFLAGS_COMMON) -DI13C_THRIFT
CFLAGS_PARQUET = $(CFLAGS_COMMON) -DI13C_PARQUET

LDFLAGS=-static -no-pie -z noexecstack -nostdlib --gc-sections
NASMFLAGS_COMMON=-f elf64

NASMFLAGS_MAIN    = $(NASMFLAGS_COMMON)
NASMFLAGS_TEST    = $(NASMFLAGS_COMMON) -D I13C_TESTS
NASMFLAGS_THRIFT  = $(NASMFLAGS_COMMON) -D I13C_THRIFT
NASMFLAGS_PARQUET = $(NASMFLAGS_COMMON) -D I13C_PARQUET

BIN_OUTPUT=bin/i13c
TEST_OUTPUT=bin/i13c-tests
THRIFT_OUTPUT=bin/i13c-thrift
PARQUET_OUTPUT=bin/i13c-parquet

BINDIR=bin
SRCDIR=src
TMPDIR=tmp

OBJDIR_MAIN   = obj/i13c-main
OBJDIR_TESTS  = obj/i13c-tests
OBJDIR_THRIFT = obj/i13c-thrift
OBJDIR_PARQUET = obj/i13c-parquet

SRCS_C=$(wildcard $(SRCDIR)/*.c)
SRCS_ASM=$(wildcard $(SRCDIR)/*.s)

OBJS_MAIN   := $(patsubst $(SRCDIR)/%.c, $(OBJDIR_MAIN)/%.c.o, $(SRCS_C)) \
               $(patsubst $(SRCDIR)/%.s, $(OBJDIR_MAIN)/%.s.o, $(SRCS_ASM))

OBJS_TEST   := $(patsubst $(SRCDIR)/%.c, $(OBJDIR_TESTS)/%.c.o, $(SRCS_C)) \
               $(patsubst $(SRCDIR)/%.s, $(OBJDIR_TESTS)/%.s.o, $(SRCS_ASM))

OBJS_THRIFT := $(patsubst $(SRCDIR)/%.c, $(OBJDIR_THRIFT)/%.c.o, $(SRCS_C)) \
               $(patsubst $(SRCDIR)/%.s, $(OBJDIR_THRIFT)/%.s.o, $(SRCS_ASM))

OBJS_PARQUET := $(patsubst $(SRCDIR)/%.c, $(OBJDIR_PARQUET)/%.c.o, $(SRCS_C)) \
							 $(patsubst $(SRCDIR)/%.s, $(OBJDIR_PARQUET)/%.s.o, $(SRCS_ASM))

-include $(OBJS_MAIN:.o=.d)
-include $(OBJS_TEST:.o=.d)
-include $(OBJS_THRIFT:.o=.d)
-include $(OBJS_PARQUET:.o=.d)

$(BIN_OUTPUT): $(OBJDIR_MAIN)/main.s.o $(OBJS_MAIN)
	@mkdir -p bin
	@$(LD) -T src/main.ld $(LDFLAGS) -o $@ $^
	@strip --strip-all $(BIN_OUTPUT) -o $(BIN_OUTPUT)

$(TEST_OUTPUT): $(OBJDIR_TESTS)/runner.s.o $(OBJS_TEST)
	@mkdir -p bin
	@$(LD) -T src/runner.ld $(LDFLAGS) -o $@ $^

$(THRIFT_OUTPUT): $(OBJDIR_THRIFT)/thrift.s.o $(OBJS_THRIFT)
	@mkdir -p bin
	@$(LD) -T src/thrift.ld $(LDFLAGS) -o $@ $^
	@strip --strip-all $(THRIFT_OUTPUT)

$(PARQUET_OUTPUT): $(OBJDIR_PARQUET)/parquet.s.o $(OBJS_PARQUET)
	@mkdir -p bin
	@$(LD) -T src/parquet.ld $(LDFLAGS) -o $@ $^
	@strip --strip-all $(PARQUET_OUTPUT)

$(OBJDIR_MAIN)/%.c.o: $(SRCDIR)/%.c
	@mkdir -p $(dir $@)
	@$(CC) $(CFLAGS_MAIN) -c $< -o $@

$(OBJDIR_TESTS)/%.c.o: $(SRCDIR)/%.c
	@mkdir -p $(dir $@)
	@$(CC) $(CFLAGS_TEST) -c $< -o $@

$(OBJDIR_THRIFT)/%.c.o: $(SRCDIR)/%.c
	@mkdir -p $(dir $@)
	@$(CC) $(CFLAGS_THRIFT) -c $< -o $@

$(OBJDIR_PARQUET)/%.c.o: $(SRCDIR)/%.c
	@mkdir -p $(dir $@)
	@$(CC) $(CFLAGS_PARQUET) -c $< -o $@

$(OBJDIR_MAIN)/%.s.o: $(SRCDIR)/%.s
	@mkdir -p $(dir $@)
	@$(NASM) $(NASMFLAGS_MAIN) $< -o $@

$(OBJDIR_TESTS)/%.s.o: $(SRCDIR)/%.s
	@mkdir -p $(dir $@)
	@$(NASM) $(NASMFLAGS_TEST) $< -o $@

$(OBJDIR_THRIFT)/%.s.o: $(SRCDIR)/%.s
	@mkdir -p $(dir $@)
	@$(NASM) $(NASMFLAGS_THRIFT) $< -o $@

$(OBJDIR_PARQUET)/%.s.o: $(SRCDIR)/%.s
	@mkdir -p $(dir $@)
	@$(NASM) $(NASMFLAGS_PARQUET) $< -o $@

.PHONY: clean
clean:
	@rm -rf $(OBJDIR_MAIN) $(OBJDIR_TESTS) $(OBJDIR_THRIFT) $(OBJDIR_PARQUET) $(TMPDIR) $(BINDIR) $(RELEASE_DIR)

.PHONY: build
build: $(BIN_OUTPUT) $(TEST_OUTPUT) $(THRIFT_OUTPUT) $(PARQUET_OUTPUT)

.PHONY: release
release: $(RELEASE_DIR)/$(TARBALL_NAME) $(RELEASE_DIR)/$(DEB_NAME)

$(RELEASE_DIR)/$(TARBALL_NAME):
	@mkdir -p $(TARBALL_DIR)
	@cp bin/i13c-thrift $(TARBALL_DIR)/
	@cp bin/i13c-parquet $(TARBALL_DIR)/
	@cp LICENSE README.md $(TARBALL_DIR)/
	@tar -czvf $@ -C $(RELEASE_DIR) $(notdir $(TARBALL_DIR))

$(RELEASE_DIR)/$(DEB_NAME):
	@mkdir -p $(DEB_DIR)/DEBIAN
	@mkdir -p $(DEB_DIR)/usr/bin
	@cp bin/i13c-thrift $(DEB_DIR)/usr/bin/
	@cp bin/i13c-parquet $(DEB_DIR)/usr/bin/
	@chmod 755 $(DEB_DIR)/usr/bin/i13c-thrift
	@chmod 755 $(DEB_DIR)/usr/bin/i13c-parquet
	@echo "Package: i13c" >  $(DEB_DIR)/DEBIAN/control
	@echo "Version: $(VERSION)-1" >> $(DEB_DIR)/DEBIAN/control
	@echo "Section: utils" >>       $(DEB_DIR)/DEBIAN/control
	@echo "Priority: optional" >>  $(DEB_DIR)/DEBIAN/control
	@echo "Architecture: amd64" >> $(DEB_DIR)/DEBIAN/control
	@echo "Maintainer: Adrian Macal <adma@amacal.pl>" >> $(DEB_DIR)/DEBIAN/control
	@echo "Description: Ultra-lightweight x86_64 tools" >> $(DEB_DIR)/DEBIAN/control
	@dpkg-deb --build $(DEB_DIR) $(RELEASE_DIR)/$(DEB_NAME)

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

.PHONY: check-commit
check-commit: lint

.PHONY: run
run: $(BIN_OUTPUT)
	@$(BIN_OUTPUT)

.PHONY: test
test: $(TEST_OUTPUT)
	@$(TEST_OUTPUT)

.PHONY: thrift
thrift: $(THRIFT_OUTPUT)
	@$(THRIFT_OUTPUT)

parquet: $(PARQUET_OUTPUT)
	@$(PARQUET_OUTPUT) $(ARGS)

parquet-01: $(PARQUET_OUTPUT)
	@$(PARQUET_OUTPUT) data/test01.parquet

.PHONY: debug
debug: $(BIN_OUTPUT)
	@edb --run $(BIN_OUTPUT) 2> /dev/null

.PHONY: thrift-dump-01
thrift-dump-01: $(THRIFT_OUTPUT)
	@dd if=data/test01.parquet skip=18152 bs=1 count=579 status=none | $(THRIFT_OUTPUT) > data/test01.thrift

.PHONY: thrift-dump-02
thrift-dump-02: $(THRIFT_OUTPUT)
	@dd if=data/test02.parquet skip=5939 bs=1 count=14110 status=none | $(THRIFT_OUTPUT) > data/test02.thrift

.PHONY: thrift-dump-03
thrift-dump-03: $(THRIFT_OUTPUT)
	@dd if=data/test03.parquet skip=3408 bs=1 count=6841 status=none | $(THRIFT_OUTPUT) > data/test03.thrift

.PHONY: thrift-dump-04
thrift-dump-04: $(THRIFT_OUTPUT)
	@dd if=data/test04.parquet skip=38843 bs=1 count=1160 status=none | $(THRIFT_OUTPUT) > data/test04.thrift

.PHONY: thrift-dump-05
thrift-dump-05: $(THRIFT_OUTPUT)
	@dd if=data/test05.parquet skip=590 bs=1 count=1321 status=none | $(THRIFT_OUTPUT) > data/test05.thrift

.PHONY: parquet-dump-01
parquet-dump-01: $(PARQUET_OUTPUT)
	@$(PARQUET_OUTPUT) data/test01.parquet > data/test01.metadata

.PHONY: parquet-dump-02
parquet-dump-02: $(PARQUET_OUTPUT)
	@$(PARQUET_OUTPUT) data/test02.parquet > data/test02.metadata

.PHONY: parquet-dump-03
parquet-dump-03: $(PARQUET_OUTPUT)
	@$(PARQUET_OUTPUT) data/test03.parquet > data/test03.metadata

.PHONY: parquet-dump-04
parquet-dump-04: $(PARQUET_OUTPUT)
	@$(PARQUET_OUTPUT) data/test04.parquet > data/test04.metadata

.PHONY: parquet-dump-05
parquet-dump-05: $(PARQUET_OUTPUT)
	@$(PARQUET_OUTPUT) data/test05.parquet > data/test05.metadata
