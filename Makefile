.PHONY: build release test fmt fmt-check clippy check clean docker-build docker-run \
       release-patch release-minor release-major github-release \
       publish publish-dry-run bench bench-core bench-protocol bench-compare bench-quick \
       helm-lint helm-template

# extract the workspace version from the root Cargo.toml
VERSION = $(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml)

build:
	cargo build

release:
	cargo build --release

test:
	cargo test --workspace --features protobuf

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --features protobuf -- -D warnings

check: fmt-check clippy test

clean:
	cargo clean

docker-build:
	docker build -t ember:latest .

docker-run: docker-build
	docker run --rm -p 6379:6379 ember:latest

bench:
	cargo bench --workspace

bench-core:
	cargo bench -p emberkv-core

bench-protocol:
	cargo bench -p ember-protocol

bench-compare:
	cargo build --release -p ember-server --features jemalloc
	bash bench/bench.sh

bench-quick:
	cargo build --release -p ember-server --features jemalloc
	bash bench/bench-quick.sh

bench-memory:
	cargo build --release -p ember-server --features jemalloc
	bash bench/bench-memory.sh

bench-full:
	cargo build --release -p ember-server --features jemalloc
	bash bench/compare-redis.sh

# --- versioning & releases ---
#
# usage:
#   make release-patch   # 0.1.0 -> 0.1.1
#   make release-minor   # 0.1.0 -> 0.2.0
#   make release-major   # 0.1.0 -> 1.0.0
#   make github-release  # create a github release for the current version

define do-release
	@echo "bumping version: $(VERSION) -> $(1)"
	@sed -i '' 's/^version = "$(VERSION)"/version = "$(1)"/' Cargo.toml
	@$(MAKE) check
	git add Cargo.toml Cargo.lock
	git commit -m "release: v$(1)"
	git tag -a "v$(1)" -m "v$(1)"
	@echo ""
	@echo "tagged v$(1). push with:"
	@echo "  git push && git push --tags"
endef

release-patch:
	$(eval V := $(subst ., ,$(VERSION)))
	$(call do-release,$(word 1,$(V)).$(word 2,$(V)).$(shell expr $(word 3,$(V)) + 1))

release-minor:
	$(eval V := $(subst ., ,$(VERSION)))
	$(call do-release,$(word 1,$(V)).$(shell expr $(word 2,$(V)) + 1).0)

release-major:
	$(eval V := $(subst ., ,$(VERSION)))
	$(call do-release,$(shell expr $(word 1,$(V)) + 1).0.0)

github-release:
	gh release create "v$(VERSION)" --title "v$(VERSION)" --generate-notes

# --- crates.io publishing ---
#
# usage:
#   make publish-dry-run  # verify all crates can be packaged
#   make publish          # publish all crates to crates.io (requires login)
#
# crates are published in dependency order:
#   1. ember-persistence, ember-protocol, ember-cluster (no internal deps)
#   2. emberkv-core (depends on persistence, protocol)
#   3. ember-server (depends on core, protocol, persistence)
#   4. emberkv-cli (standalone)

CRATES_ORDER = ember-persistence ember-protocol ember-cluster emberkv-core ember-server emberkv-cli

publish-dry-run:
	@echo "dry run: checking all crates can be packaged..."
	@for crate in $(CRATES_ORDER); do \
		echo "  checking $$crate..."; \
		cargo publish --dry-run -p $$crate || exit 1; \
	done
	@echo "all crates passed dry run"

publish:
	@echo "publishing crates to crates.io..."
	@echo "note: each crate needs time to index before dependents can be published"
	@for crate in $(CRATES_ORDER); do \
		echo ""; \
		echo "publishing $$crate..."; \
		cargo publish -p $$crate || exit 1; \
		echo "waiting for crates.io to index $$crate..."; \
		sleep 30; \
	done
	@echo ""
	@echo "all crates published successfully"

# --- helm ---

helm-lint:
	helm lint helm/ember

helm-template:
	helm template ember helm/ember
