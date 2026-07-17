ANTFLY_ROOT ?= ../../..
INDEX_DIR ?= idx
SEGMENT_MODE ?= single
MANIFEST ?= $(INDEX_DIR)-manifest.json
BM25_K1 ?= 1.2
BM25_B ?= 0.75

clean:
	rm -fr $(INDEX_DIR)
	rm -fr $(ANTFLY_ROOT)/zig-out/bin/search_benchmark_index
	rm -fr $(ANTFLY_ROOT)/zig-out/bin/search_benchmark_query

compile:
	cd $(ANTFLY_ROOT) && zig build search-bench-build -Doptimize=ReleaseFast

index: $(INDEX_DIR)

$(INDEX_DIR): compile
	mkdir -p $(INDEX_DIR)
	$(ANTFLY_ROOT)/zig-out/bin/search_benchmark_index "$@" --segment-mode $(SEGMENT_MODE) --manifest $(MANIFEST) --bm25-k1 $(BM25_K1) --bm25-b $(BM25_B) < $(CORPUS)

serve: compile
	$(ANTFLY_ROOT)/zig-out/bin/search_benchmark_query $(INDEX_DIR) --allow-legacy-query-syntax --bm25-k1 $(BM25_K1) --bm25-b $(BM25_B)
