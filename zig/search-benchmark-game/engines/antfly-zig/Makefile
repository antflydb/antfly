ANTFLY_ROOT ?= ../../..
INDEX_DIR ?= idx

clean:
	rm -fr $(INDEX_DIR)
	rm -fr $(ANTFLY_ROOT)/zig-out/bin/search_benchmark_index
	rm -fr $(ANTFLY_ROOT)/zig-out/bin/search_benchmark_query

compile:
	cd $(ANTFLY_ROOT) && zig build search-bench-build -Doptimize=ReleaseFast

index: $(INDEX_DIR)

$(INDEX_DIR): compile
	mkdir -p $(INDEX_DIR)
	$(ANTFLY_ROOT)/zig-out/bin/search_benchmark_index "$@" < $(CORPUS)

serve: compile
	$(ANTFLY_ROOT)/zig-out/bin/search_benchmark_query $(INDEX_DIR)
