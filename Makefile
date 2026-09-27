.PHONY: build install test check smoke clean
CODEX_HOME ?= $(HOME)/.codex
export CODEX_HOME

build:
	go build -trimpath -o bin/cronex ./cmd/cronex
install: build
	./bin/cronex install
test:
	go test -race ./...
check:
	go vet ./...
smoke: build
	node scripts/smoke-codex.mjs
clean:
	rm -rf bin
