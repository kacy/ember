.PHONY: build test fmt clippy check clean docker-build

build:
	cargo build

release:
	cargo build --release

test:
	cargo test

fmt:
	cargo fmt

fmt-check:
	cargo fmt --check

clippy:
	cargo clippy -- -D warnings

check: fmt-check clippy test

clean:
	cargo clean

docker-build:
	docker build -t ember:latest .
