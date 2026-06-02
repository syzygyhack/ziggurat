# Ziggurat Makefile
# Requires: Go 1.24+
# Cross-compile for Windows: make windows

BINARY := ziggurat

# Platform/shell detection. On Windows, make runs recipes through cmd.exe
# (when invoked from PowerShell/cmd with no POSIX sh on PATH) or through a POSIX
# shell (Git Bash / MSYS2); each needs different commands, path separators, and
# null device. $(SHELL) is NOT a reliable indicator here — it defaults to
# /bin/sh even when make actually uses cmd.exe. Instead we probe the real recipe
# shell, which $(shell ...) shares: cmd.exe's echo preserves the quotes, while a
# POSIX shell strips them.
ifeq ($(OS),Windows_NT)
  BINARY_EXT := .exe
  # cmd.exe echo keeps the quotes (output: "probe"); a POSIX shell strips them
  # (output: probe). Test for the quote character rather than exact equality so
  # a trailing CR from cmd.exe doesn't throw off the comparison.
  ifneq (,$(findstring ",$(shell echo "probe")))
    # cmd.exe: native builtins, backslash paths, NUL device.
    WIN_CMD := 1
    INSTALL_DIR := $(USERPROFILE)\.local\bin
    DEVNULL := NUL
  else
    # POSIX shell on Windows (Git Bash / MSYS2).
    INSTALL_DIR := $(USERPROFILE)/.local/bin
    DEVNULL := /dev/null
  endif
else
  BINARY_EXT :=
  INSTALL_DIR := $(HOME)/.local/bin
  DEVNULL := /dev/null
endif

VERSION ?= $(shell git describe --tags --always --dirty 2>$(DEVNULL) || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>$(DEVNULL) || echo unknown)
VERSION_PKG := github.com/syzygyhack/ziggurat/internal/version
LDFLAGS := -ldflags "-X $(VERSION_PKG).Version=$(VERSION) -X $(VERSION_PKG).Commit=$(COMMIT)"

.PHONY: build install test test-race coverage fmt vet lint tidy proto clean windows \
	dist dist-linux-amd64 dist-linux-arm64 dist-darwin-arm64 dist-windows-amd64 help

build:
	go build $(LDFLAGS) -o $(BINARY)$(BINARY_EXT) ./cmd/ziggurat/

install: build
ifdef WIN_CMD
	@if not exist "$(INSTALL_DIR)" mkdir "$(INSTALL_DIR)"
	copy /Y "$(BINARY)$(BINARY_EXT)" "$(INSTALL_DIR)"
	@echo Installed $(BINARY)$(BINARY_EXT) to $(INSTALL_DIR)
else
	mkdir -p "$(INSTALL_DIR)"
	cp "$(BINARY)$(BINARY_EXT)" "$(INSTALL_DIR)/"
	@echo Installed $(BINARY)$(BINARY_EXT) to $(INSTALL_DIR)
endif

test:
	go test ./... $(ARGS)

test-race:
	go test -race ./... $(ARGS)

coverage:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

fmt:
	go fmt ./...
	@echo "Formatted all Go files"

vet:
	go vet ./...

lint:
ifdef WIN_CMD
	@where golangci-lint >NUL 2>NUL && golangci-lint run ./... || (echo golangci-lint not installed, running go vet only & go vet ./...)
else
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, running go vet only"; \
		go vet ./...; \
	fi
endif

tidy:
	go mod tidy

proto:
	protoc --go_out=internal/transport/pb --go_opt=paths=source_relative \
		--go-grpc_out=internal/transport/pb --go-grpc_opt=paths=source_relative \
		-I proto proto/ziggurat.proto
	@echo "Regenerated protobuf code"

windows:
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY).exe ./cmd/ziggurat/

# Cross-compile release binaries for all supported targets into dist/.
# Intended for a POSIX/CI host (the GitHub Actions release matrix uses the same
# GOOS/GOARCH set). No CGo, so cross-compilation is pure `go build`.
DIST := dist
dist: dist-linux-amd64 dist-linux-arm64 dist-darwin-arm64 dist-windows-amd64

dist-linux-amd64:
	@mkdir -p $(DIST)
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(DIST)/$(BINARY)-linux-amd64 ./cmd/ziggurat/

dist-linux-arm64:
	@mkdir -p $(DIST)
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(DIST)/$(BINARY)-linux-arm64 ./cmd/ziggurat/

dist-darwin-arm64:
	@mkdir -p $(DIST)
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o $(DIST)/$(BINARY)-darwin-arm64 ./cmd/ziggurat/

dist-windows-amd64:
	@mkdir -p $(DIST)
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(DIST)/$(BINARY)-windows-amd64.exe ./cmd/ziggurat/

clean:
ifdef WIN_CMD
	@if exist "$(BINARY)" del /q "$(BINARY)"
	@if exist "$(BINARY).exe" del /q "$(BINARY).exe"
	@if exist coverage.out del /q coverage.out
	@if exist coverage.html del /q coverage.html
	go clean
else
	rm -f $(BINARY) $(BINARY).exe
	rm -f coverage.out coverage.html
	go clean
endif

help:
	@echo "Usage: make [target] [ARGS=...]"
	@echo ""
	@echo "Targets:"
	@echo "  build      Build the ziggurat binary"
	@echo "  install    Build and install to ~/.local/bin"
	@echo "  test       Run all tests (pass ARGS for flags, e.g., ARGS='-v')"
	@echo "  test-race  Run tests with race detector"
	@echo "  coverage   Run tests with coverage report"
	@echo "  fmt        Format all Go source files"
	@echo "  vet        Run go vet"
	@echo "  lint       Run golangci-lint (falls back to go vet)"
	@echo "  tidy       Tidy go.mod dependencies"
	@echo "  proto      Regenerate protobuf Go code"
	@echo "  windows    Cross-compile ziggurat.exe for Windows amd64"
	@echo "  clean      Remove build artifacts"
	@echo "  help       Show this help"
