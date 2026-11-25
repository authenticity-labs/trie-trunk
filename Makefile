.PHONY: build test test-object-index test-store-schema test-pill-go rust site schema

build: rust definition-tools
	cp rust/target/release/trunk-search trunk-search
	cd go && CGO_ENABLED=1 go build -o ../trunk ./cmd/trunk

rust:
	cargo build --release --locked --manifest-path rust/Cargo.toml --target-dir rust/target

test: rust schema
	cargo test --locked --all-features --manifest-path ../pill-packer/rust/Cargo.toml --target-dir ../pill-packer/rust/target
	cargo test --locked --manifest-path rust/Cargo.toml --target-dir rust/target
	cargo test --locked --manifest-path rust/seeds/Cargo.toml --features query-language --test reader --test reader_index --test replication --test storage_publication --test summary_publication --test registration --test writer --test ingress --test identity --test summary_arrays --test summary_classes --test summary_accumulator
	cargo test --locked --manifest-path rust/node/Cargo.toml
	cargo test --locked --manifest-path rust/uuid-index/Cargo.toml
	cd go && PATH="$(CURDIR)/rust/target/release:$$PATH" go test -count=1 ./...

.PHONY: test-revisions
test-revisions: rust
	cargo build --release --locked --manifest-path ../pill-packer/rust/ffi/Cargo.toml --target-dir ../pill-packer/rust/ffi/target
	cargo test --locked --manifest-path ../pill-packer/rust/Cargo.toml --test record_binding
	cargo test --locked --manifest-path rust/uuid-index/Cargo.toml --lib collection::
	cargo test --locked --manifest-path rust/node/Cargo.toml --test object_revisions
	cd go && go test -a -count=1 ./server/cache -run '^TestRegistered(ObjectRevisionTransport|HTTPIngressRecoveryAndPublication)$$'

test-object-index: rust
	cd go && go test -count=1 -tags trunk_prototype ./server/cache -run '^TestObjectIndex'

site:
	node site/build.mjs

schema:
	cargo test --locked --manifest-path rust/seeds/Cargo.toml --features query-language --test http_definition --test metadata
	cargo test --locked --manifest-path rust/seeds/Cargo.toml --features query-language --test registration shipped_event_lineages_bind_to_their_registered_branches

# Historical Go wire-layout checks are available independently of the registered node.
test-store-schema:
	node scripts/check-definition-schema.mjs

test-pill-go:
	cd ../pill-packer/go && go test -count=1 ./codec

# Copy the already-built shared codec tools alongside the server for datastore setup.
.PHONY: definition-tools
definition-tools:
	cp ../pill-packer/rust/target/release/doc doc
	cp ../pill-packer/rust/target/release/rx rx
