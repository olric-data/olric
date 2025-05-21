.PHONY: test
test:
	go test -p 1 ./...

.PHONY: test-quick
test-quick:
	go test -p 1 -count=1 ./...

.PHONY: test-race
test-race:
	go test -p 1 -race ./...

.PHONY: format
format:
	go fmt ./...

.PHONY: prepare-merge
prepare-merge: format test

.PHONY: ci
ci: test

.PHONY: ci-quick
ci-full: test-quick

.PHONY: install
install:
	go install -ldflags="-s -w" -v ./cmd/*