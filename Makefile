all: build

GO ?= go
GOLANGCI_LINT_VERSION ?= v2.1.6

.PHONY: all build test unit fmt fmt-check lint clean \
	integration-up integration-test integration-down \
	integration-scale-normal integration-scale-heavy

build:
	$(GO) build -o bin/go-cdc ./cmd/go-cdc

# Unit tests
test unit:
	$(GO) test -race -timeout 2m ./...

# Apply formatters from .golangci.yml (goimports).
fmt: ensure-golangci-lint
	@export PATH="$$(go env GOPATH)/bin:$$PATH"; golangci-lint run --fix ./...

# Fail if module packages are not gofmt-clean (_legacy/ excluded via go list).
fmt-check:
	@dirs=$$($(GO) list -f '{{.Dir}}' ./...); \
	unformatted=$$(gofmt -l $$dirs); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt-clean:"; echo "$$unformatted"; exit 1; \
	fi; \
	echo "gofmt OK"

lint: ensure-golangci-lint
	@export PATH="$$(go env GOPATH)/bin:$$PATH"; golangci-lint run ./...

ensure-golangci-lint:
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "==> installing golangci-lint $(GOLANGCI_LINT_VERSION)"; \
		$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); \
	fi

# Docker MySQL e2e (project-specific; analogous to go-mysql test-local).
integration-up:
	bash test/integration/scripts/mysql-up.sh

integration-test:
	bash test/integration/scripts/run-e2e.sh

integration-down:
	bash test/integration/scripts/mysql-down.sh

integration-scale-normal:
	bash test/integration/scale/run-scale.sh normal

integration-scale-heavy:
	bash test/integration/scale/run-scale.sh heavy

clean:
	$(GO) clean -i ./...
	@rm -rf ./bin
