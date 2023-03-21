.PHONY: all local build test docker

local:
	cargo watch -x run

build:
	cargo build --release

test:
	cargo nextest run

docker:
	docker build -t app-backend .

init:
	cargo install cargo-watch
	cargo install cargo-nextest
	cp .env.example .env