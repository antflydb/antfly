INDEX_DIR ?= idx
SEGMENT_MODE ?= single
MANIFEST ?= $(INDEX_DIR)-manifest.json
BM25_K1 ?= 1.2
BM25_B ?= 0.75

clean:
	rm -fr $(INDEX_DIR)
	cargo clean

compile:
	cargo build --release --locked

index: compile
	mkdir -p $(INDEX_DIR)
	target/release/tantivy-kernel-benchmark index $(INDEX_DIR) --segment-mode $(SEGMENT_MODE) --manifest $(MANIFEST) --bm25-k1 $(BM25_K1) --bm25-b $(BM25_B) < $(CORPUS)

serve: compile
	target/release/tantivy-kernel-benchmark query $(INDEX_DIR) --bm25-k1 $(BM25_K1) --bm25-b $(BM25_B)
