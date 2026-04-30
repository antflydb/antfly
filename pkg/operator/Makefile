GO := GOWORK=off GOEXPERIMENT=simd go

.PHONY: build
build:
	$(GO) build -o bin/manager ./cmd/antfly-operator

.PHONY: test
test:
	$(GO) test ./...

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: fmt
fmt:
	$(GO) fmt ./...

.PHONY: run
run:
	$(GO) run ./cmd/antfly-operator

.PHONY: clean
clean:
	rm -rf bin/
