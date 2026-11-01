all: build

GO ?= go
GOLANGCI_LINT_VERSION ?= v2.1.6

.PHONY: all build test unit fmt fmt-check lint clean \
	it-mysql it-stdout it-kafka it-doris it-es it-es7 it-es9 it-pg \
	it-pg14 it-pg15 it-pg16 it-pg17 it-pg18 \
	it-down it-check it-plan

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

# --------------------------------------------------------------- integration
# Docker-backed suites. Every sink goes through ONE entry point, which owns the
# stack (setup + wait), the build, the seed and the report:
#
#   bash tests/integration/scripts/run.sh <sink> [profile] [variant] [suites]
#
#   make it-mysql                        # correctness tier
#   make it-mysql PROFILE=heavy          # + load/scale
#   make it-mysql VARIANT=57             # the other side of the version matrix
#   make it-es VARIANT=es9 SUITES=go,func
#   make it-pg14 PROFILE=heavy
#   make it-down SINK=postgresql         # tear one stack down
#
# PROFILE  normal (default) | heavy
# VARIANT  57|80 · pg14..pg18 · es7|es8|es9 · 2.1.x   (empty = sink default)
# SUITES   comma list that overrides the tier default (e.g. SUITES=func)
#
# The scripts live in tests/integration/scripts/ (setup.sh, wait.sh,
# teardown.sh, run.sh); CI calls exactly the same entry point, so local and CI
# cannot drift.
IT       ?= tests/integration
PROFILE  ?= normal
VARIANT  ?=
SUITES   ?=
SINK     ?= mysql
PATHS    ?= connector/sink/mysql/sink.go
RUN      ?= bash $(IT)/scripts/run.sh
DOWN     ?= bash $(IT)/scripts/teardown.sh
# The variant is quoted so an empty VARIANT still occupies the third slot; the
# suite list is positional #4 of run.sh and must not slide into it.
RUN_IT   ?= $(RUN) "$(1)" "$(PROFILE)" "$(VARIANT)" $(SUITES)

it-mysql:
	$(call RUN_IT,mysql)

it-stdout:
	$(call RUN_IT,stdout)

# kafka_it covers the load/peak test points itself; the tier only resizes them.
it-kafka:
	$(call RUN_IT,kafka)

# Doris FE/BE run on the host network and need the kernel knobs setup.sh raises.
it-doris:
	$(call RUN_IT,doris)

# ES8 is the default variant; the adapter nodes are es7/es9.
it-es:
	$(RUN) elasticsearch "$(PROFILE)" "$(if $(VARIANT),$(VARIANT),es8)" $(SUITES)

# v7/v9 adapter nodes are correctness-only (go + func), whatever the tier.
it-es7:
	$(RUN) elasticsearch "$(PROFILE)" es7 $(SUITES)

it-es9:
	$(RUN) elasticsearch "$(PROFILE)" es9 $(SUITES)

it-pg:
	$(RUN) postgresql "$(PROFILE)" "$(if $(VARIANT),$(VARIANT),pg16)" $(SUITES)

# One target per supported major; heavy also runs the 14..18 compat matrix.
it-pg14:
	$(RUN) postgresql "$(PROFILE)" pg14 $(SUITES)

it-pg15:
	$(RUN) postgresql "$(PROFILE)" pg15 $(SUITES)

it-pg16:
	$(RUN) postgresql "$(PROFILE)" pg16 $(SUITES)

it-pg17:
	$(RUN) postgresql "$(PROFILE)" pg17 $(SUITES)

it-pg18:
	$(RUN) postgresql "$(PROFILE)" pg18 $(SUITES)

# Containers + volumes of one sink; the harness's own trap already does this
# after a local run, so this is the escape hatch for an interrupted one.
it-down:
	$(DOWN) "$(SINK)" "$(VARIANT)"

# Offline: harness unit tests (pytest walks tests/integration as one tree) plus
# the diff -> matrix planner, which is what decides what CI runs.
it-check:
	python3 -m pytest -q $(IT) tools/ci/test_plan.py

# Print the matrix a change would produce, e.g.
#   make it-plan PATHS="connector/sink/kafka/sink.go"
it-plan:
	python3 tools/ci/plan.py --paths $(PATHS) --profile $(PROFILE)

clean:
	$(GO) clean -i ./...
	@rm -rf ./bin
