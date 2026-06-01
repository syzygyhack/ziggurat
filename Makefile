# Ziggurat Makefile
# Requires: Go 1.24+
# Cross-compile for Windows: make windows

BINARY := ziggurat

# Use USERPROFILE on Windows (PowerShell), HOME on Unix.
ifeq ($(OS),Windows_NT)
  INSTALL_DIR := $(USERPROFILE)/.local/bin
  BINARY_EXT := .exe
else
  INSTALL_DIR := $(HOME)/.local/bin
  BINARY_EXT :=
endif

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT)"

.PHONY: build install test test-race coverage fmt vet lint tidy proto clean windows help

build:
	go build $(LDFLAGS) -o $(BINARY)$(BINARY_EXT) ./cmd/ziggurat/

install: build
	@if [ ! -d "$(INSTALL_DIR)" ]; then mkdir -p "$(INSTALL_DIR)"; fi
	cp $(BINARY)$(BINARY_EXT) "$(INSTALL_DIR)/"
	@echo "Installed $(BINARY)$(BINARY_EXT) to $(INSTALL_DIR)"

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
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, running go vet only"; \
		go vet ./...; \
	fi

tidy:
	go mod tidy

proto:
	protoc --go_out=internal/transport/pb --go_opt=paths=source_relative \
		--go-grpc_out=internal/transport/pb --go-grpc_opt=paths=source_relative \
		-I proto proto/ziggurat.proto
	@echo "Regenerated protobuf code"

windows:
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY).exe ./cmd/ziggurat/

clean:
	rm -f $(BINARY) $(BINARY).exe
	rm -f coverage.out coverage.html
	go clean

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
