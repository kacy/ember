.PHONY: build release test fmt fmt-check clippy check clean docker-build \
       release-patch release-minor release-major github-release

# extract the workspace version from the root Cargo.toml
VERSION = $(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml)

build:
	cargo build

release:
	cargo build --release

test:
	cargo test --workspace

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace -- -D warnings

check: fmt-check clippy test

clean:
	cargo clean

docker-build:
	docker build -t ember:latest .

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
