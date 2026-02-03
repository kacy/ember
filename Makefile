.PHONY: build test fmt clippy check clean docker-build

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
