ANTFLY_DIR ?= ../antfly

# macOS: Postgres symbols (palloc, pfree, etc.) live in the server binary, not
# a library. Tell the linker to allow unresolved symbols at link time — they
# resolve when Postgres loads the extension or pgrx starts a test instance.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  export RUSTFLAGS ?= -C link-arg=-undefined -C link-arg=dynamic_lookup
endif

.PHONY: test test-e2e start-antfly stop-antfly build

build:
	cargo pgrx package --pg-config /opt/homebrew/opt/postgresql@18/bin/pg_config

test:
	cargo pgrx test pg18

test-e2e: start-antfly
	@echo "Running e2e tests..."
	@ANTFLY_TEST_URL=http://localhost:8080/api/v1/ cargo pgrx test pg18 2>&1; \
	status=$$?; \
	$(MAKE) stop-antfly; \
	exit $$status

start-antfly:
	@echo "Starting Antfly swarm..."
	@cd $(ANTFLY_DIR) && GOEXPERIMENT=simd go run ./cmd/antfly swarm > /tmp/antfly-test.log 2>&1 &
	@echo "Waiting for Antfly to be ready..."
	@for i in $$(seq 1 30); do \
		curl -sf http://localhost:4200/healthz > /dev/null 2>&1 && { echo "Antfly is ready."; break; }; \
		sleep 1; \
	done

stop-antfly:
	@echo "Stopping Antfly..."
	@-pkill -f "antfly swarm" 2>/dev/null || true
