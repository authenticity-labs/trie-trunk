.PHONY: build test rust site schema

build: rust definition-tools
	cp rust/target/release/trunk-search trunk-search
	cd go && CGO_ENABLED=1 go build -o ../trunk ./cmd/trunk

rust:
	cargo build --release --locked --manifest-path rust/Cargo.toml --target-dir rust/target

test: rust
	cd ../pill-packer/go && go test ./codec
	cargo test --locked --all-features --manifest-path ../pill-packer/rust/Cargo.toml --target-dir ../pill-packer/rust/target
	node scripts/check-definition-schema.mjs
	cargo test --locked --manifest-path rust/Cargo.toml --target-dir rust/target
	cargo test --locked --manifest-path rust/uuid-index/Cargo.toml
	cd go && PATH="$(CURDIR)/rust/target/release:$$PATH" go test -count=1 ./...

site:
	node site/build.mjs

schema:
	node scripts/check-definition-schema.mjs

# Copy the already-built shared codec tools alongside the server for datastore setup.
.PHONY: definition-tools
definition-tools:
	cp ../pill-packer/rust/target/release/doc doc
	cp ../pill-packer/rust/target/release/rx rx
