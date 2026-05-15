BINARY := timberdb
MODULE := github.com/ngaddam369/timberdb

.PHONY: build fmt lint test bench crash-test verify clean tidy

## build: compile the timberdb CLI binary
build:
	go build -o bin/$(BINARY) ./cmd/timberdb

## fmt: format all Go source files in place
fmt:
	gofmt -w .

## lint: run golangci-lint
lint:
	golangci-lint run ./...

## test: run all tests with race detector and show coverage summary
test:
	go test -v -race -count=1 -timeout=120s -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | grep -E "^total|^github"

## bench: run the benchmark suite
bench:
	go run ./cmd/timberdb bench \
	  --db /tmp/timberdb-bench \
	  --duration 60s \
	  --sources 100 \
	  --record-size 512 \
	  --append-rate 100000

## crash-test: run crash recovery tests
crash-test:
	go test -race -count=20 -timeout=300s ./test/crash/...

## verify: run the full checklist (fmt → build → lint → mod verify → vulncheck → test)
verify: fmt build lint test
	go mod verify
	govulncheck ./...

## clean: remove build artifacts
clean:
	rm -rf bin/ coverage.out

## tidy: tidy and verify go modules
tidy:
	go mod tidy
	go mod verify
