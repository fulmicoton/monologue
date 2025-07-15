test:
	cargo test --tests --lib
	RUSTFLAGS="--cfg loom" BACKTRACE=all cargo test test_concurrency  --lib -- --nocapture
	cargo +nightly miri test

fmt:
	cargo +nightly fmt --all
