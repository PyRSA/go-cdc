all: build

GO ?= go
GOLANGCI_LINT_VERSION ?= v2.1.6

.PHONY: all build test unit fmt fmt-check lint clean \
	integration-up integration-test integration-down \
	integration-kafka-up integration-kafka-test \
	integration-doris-up integration-doris-test integration-doris-down \
	integration-doris-scale-normal integration-doris-scale-heavy \
	integration-scale-normal integration-scale-heavy \
	integration-es-up integration-es-down integration-es-integration \
	integration-es-e2e integration-es-load \
	integration-es-scale-normal integration-es-scale-heavy \
	integration-es7-up integration-es7-down integration-es7-integration \
	integration-es7-e2e-up integration-es7-e2e

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

# MySQL + Kafka sink IT (compose kafka service; report docs/test/reports/mysql-kafka.md).
integration-kafka-up: integration-up
	bash test/integration/scripts/kafka-up.sh

integration-kafka-test:
	bash test/integration/scripts/run-kafka-e2e.sh

# MySQL → Doris pipeline + sink Go IT (Linux host network; see test/integration/README.md).
# Requires kernel knobs (inotify watches + vm.max_map_count); doris-up.sh prints the sudo lines.
integration-doris-up: integration-up
	bash test/integration/scripts/doris-up.sh

integration-doris-test:
	bash test/integration/scripts/run-doris-e2e.sh

integration-doris-down:
	bash test/integration/scripts/doris-down.sh
	bash test/integration/scripts/mysql-down.sh

integration-doris-scale-normal:
	bash test/integration/scripts/run-doris-scale.sh normal

integration-doris-scale-heavy:
	bash test/integration/scripts/run-doris-scale.sh heavy

integration-scale-normal:
	bash test/integration/scale/run-scale.sh normal

integration-scale-heavy:
	bash test/integration/scale/run-scale.sh heavy

# Elasticsearch 8 sink suites. All of them need `integration-es-up` (compose ES8)
# plus the MySQL service; reports land in docs/test/reports/.
integration-es-up: integration-up
	bash test/integration/scripts/es-up.sh

integration-es-down:
	bash test/integration/scripts/es-down.sh

# Go-level sink/runner IT against a real cluster (build tag esintegration).
integration-es-integration:
	bash test/integration/scripts/run-es-integration.sh

integration-es-e2e:
	bash test/integration/scripts/run-es-e2e.sh

integration-es-load:
	bash test/integration/scripts/run-es-load.sh

integration-es-scale-normal:
	bash test/integration/scripts/run-es-scale.sh normal

# Stress profile: 10 source DBs + 50k rows/hotspot + 45s peak. Explicit opt-in.
integration-es-scale-heavy:
	bash test/integration/scripts/run-es-scale.sh heavy

# Elasticsearch 7 (v7 client adapter): the Go-level sink IT needs only ES7.
integration-es7-up:
	bash test/integration/scripts/es7-up.sh

integration-es7-down:
	bash test/integration/scripts/es7-down.sh

integration-es7-integration:
	bash test/integration/scripts/run-es7-integration.sh

# ES7 CDC e2e: the ES8 harness with sink.version=7, so it needs MySQL as well.
# MYSQL_UP_SERVICES keeps the ES8 node and Kafka out of this run.
integration-es7-e2e-up:
	MYSQL_UP_SERVICES=mysql bash test/integration/scripts/mysql-up.sh
	bash test/integration/scripts/es7-up.sh

integration-es7-e2e:
	bash test/integration/scripts/run-es7-e2e.sh

clean:
	$(GO) clean -i ./...
	@rm -rf ./bin
