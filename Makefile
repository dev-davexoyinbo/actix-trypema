.PHONY: help redis-up redis-down test lint doc msrv check

REDIS_PORT ?= 16379
REDIS_URL ?= redis://127.0.0.1:$(REDIS_PORT)/

# Every combination of the redis, json and metrics features.
FEATURE_SETS := none redis json metrics redis,json redis,metrics json,metrics redis,json,metrics

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "} /^[a-z-]+:.*## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

redis-up: ## Start the test Redis from compose.yaml
	@docker info >/dev/null 2>&1 || (echo "docker daemon not running" >&2; exit 1)
	@REDIS_PORT="$(REDIS_PORT)" docker compose up -d --wait

redis-down: ## Stop the test Redis and remove its volume
	@docker compose down -v --remove-orphans

test: ## Run every test on all 8 feature sets, with Redis
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	for features in $(FEATURE_SETS); do \
		if [ "$$features" = none ]; then flags=""; else flags="--features $$features"; fi; \
		echo "== cargo test $$flags"; \
		REDIS_URL="$(REDIS_URL)" cargo test $$flags; \
	done

lint: ## Check formatting, clippy on all 8 feature sets, and the no-feature dependency tree
	cargo fmt --all -- --check
	@set -e; \
	for features in $(FEATURE_SETS); do \
		if [ "$$features" = none ]; then flags=""; else flags="--features $$features"; fi; \
		echo "== cargo clippy --all-targets $$flags"; \
		cargo clippy --all-targets $$flags -- -D warnings; \
	done
	@if cargo tree -e normal --prefix none | grep -E '^redis '; then \
		echo "redis crates leaked into the no-feature tree" >&2; exit 1; \
	fi

doc: ## Build the docs with warnings as errors
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --features redis,json,metrics

msrv: ## Check the build on the MSRV toolchain (Rust 1.88)
	cargo +1.88 check --locked
	cargo +1.88 check --locked --features redis,json,metrics

check: lint doc msrv test ## Run lint, doc, msrv and test
