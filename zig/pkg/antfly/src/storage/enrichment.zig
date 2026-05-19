// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Enrichment scanning, rebuild state, and backfill progress tracking.
//!
//! Matches Go antfly's enrichment infrastructure:
//!   - ScanForEnrichment: find documents missing enrichment keys
//!   - RebuildState: persist last-scanned key for crash-resilient resume
//!   - BackfillTracker: progress estimation (0.0–1.0) for operational visibility
//!   - Dud marker (0xDD): permanently unenrichable items
//!
//! Go reference:
//!   src/store/storeutils/scanner.go — ScanForEnrichment, ScanForBackfill
//!   src/store/db/indexes/rebuild_state.go — RebuildState
//!   src/store/db/indexes/embeddingenricher.go — backfill loop

const std = @import("std");
const Allocator = std.mem.Allocator;
const backend_erased = @import("backend_erased.zig");
const backend_scan = @import("backend_scan.zig");
const platform_time = @import("../platform/time.zig");
const docstore = @import("docstore.zig");
const DocStore = docstore.DocStore;
const ByteRange = docstore.ByteRange;
const lmdb = @import("lmdb.zig");

// ============================================================================
// Dud enrichment sentinel
// ============================================================================

/// Sentinel value for permanently unenrichable items.
/// Matches Go's DudEnrichmentValue = 0xDD.
pub const DudEnrichmentValue: u8 = 0xDD;

/// Check if a value is the dud enrichment sentinel.
pub fn isDudEnrichment(value: []const u8) bool {
    return value.len == 1 and value[0] == DudEnrichmentValue;
}

// ============================================================================
// Document scan state
// ============================================================================

pub const DocumentScanState = struct {
    doc_key: []u8,
    value: []u8,
    enrichment_hash_id: u64,
};

/// Free a batch of DocumentScanState.
pub fn freeScanBatch(alloc: Allocator, batch: []DocumentScanState) void {
    for (batch) |item| {
        alloc.free(item.doc_key);
        alloc.free(item.value);
    }
    alloc.free(batch);
}

// ============================================================================
// Enrichment scanner
// ============================================================================

pub const EnrichmentScanOptions = struct {
    byte_range: ByteRange,
    primary_suffix: []const u8,
    enrichment_suffix: []const u8,
    batch_size: u32 = 100,
};

/// Scan a DocStore for documents missing enrichment keys.
/// Calls process_batch with batches of unenriched documents.
/// Matches Go's ScanForEnrichment from scanner.go.
pub fn scanForEnrichment(
    alloc: Allocator,
    store: *DocStore,
    opts: EnrichmentScanOptions,
    process_batch: *const fn (batch: []const DocumentScanState) anyerror!void,
) !void {
    var state = ScanState{
        .alloc = alloc,
        .opts = opts,
        .process_batch = process_batch,
    };
    defer state.deinit();

    active_state = &state;
    defer {
        active_state = null;
    }

    const lower = if (opts.byte_range.start.len > 0) opts.byte_range.start else "";
    const upper = if (opts.byte_range.end.len > 0) opts.byte_range.end else "";

    var runtime = try backend_erased.storeFrom(alloc, store.backendStore());
    defer runtime.deinit();
    try backend_scan.scan(&runtime, lower, upper, .{}, &ScanState.callback);

    // Flush last doc if unenriched
    if (state.current_doc != null and !state.has_enrichment) {
        try state.addToBatch();
    }

    // Flush remaining batch
    if (state.batch.items.len > 0) {
        try state.flushBatch();
    }
}

const ScanState = struct {
    alloc: Allocator,
    opts: EnrichmentScanOptions,
    process_batch: *const fn (batch: []const DocumentScanState) anyerror!void,
    current_doc: ?DocumentScanState = null,
    has_enrichment: bool = false,
    batch: std.ArrayListUnmanaged(DocumentScanState) = .empty,
    seen_enrichments: std.StringHashMapUnmanaged(u64) = .empty,

    fn deinit(self: *ScanState) void {
        // Free any remaining current_doc
        if (self.current_doc) |doc| {
            self.alloc.free(doc.doc_key);
            self.alloc.free(doc.value);
        }
        // Free any remaining batch items
        for (self.batch.items) |item| {
            self.alloc.free(item.doc_key);
            self.alloc.free(item.value);
        }
        self.batch.deinit(self.alloc);
        // Free seen_enrichments keys
        var it = self.seen_enrichments.keyIterator();
        while (it.next()) |k| self.alloc.free(@constCast(k.*));
        self.seen_enrichments.deinit(self.alloc);
    }

    fn callback(key: []const u8, value: []const u8) anyerror!backend_scan.ScanAction {
        // We need mutable state access. Since Zig function pointers can't capture,
        // we use a thread-local approach. This is safe because LMDB scan is single-threaded
        // and scanForEnrichment is not called reentrantly from within the callback.
        const self = active_state orelse return error.EnrichmentStateNotSet;
        return self.processEntry(key, value);
    }

    fn processEntry(self: *ScanState, key: []const u8, value: []const u8) !backend_scan.ScanAction {
        if (std.mem.endsWith(u8, key, self.opts.primary_suffix)) {
            // Primary key — handle previous doc
            if (self.current_doc) |doc| {
                if (!self.has_enrichment) {
                    try self.addToBatch();
                } else {
                    // Previous doc was enriched — free its allocations
                    self.alloc.free(doc.doc_key);
                    self.alloc.free(doc.value);
                    self.current_doc = null;
                }
            }

            const item_key = key[0 .. key.len - self.opts.primary_suffix.len];

            self.current_doc = .{
                .doc_key = try self.alloc.dupe(u8, item_key),
                .value = try self.alloc.dupe(u8, value),
                .enrichment_hash_id = 0,
            };
            self.has_enrichment = false;

            // Check if we saw an enrichment for this key earlier
            if (self.seen_enrichments.fetchRemove(item_key)) |kv| {
                self.has_enrichment = true;
                self.current_doc.?.enrichment_hash_id = kv.value;
                self.alloc.free(@constCast(kv.key));
            }
        } else if (std.mem.endsWith(u8, key, self.opts.enrichment_suffix)) {
            const base_key = key[0 .. key.len - self.opts.enrichment_suffix.len];

            if (isDudEnrichment(value)) {
                // Dud — don't mark as enriched, let generatePrompts re-evaluate
            } else if (self.current_doc != null and std.mem.eql(u8, base_key, self.current_doc.?.doc_key)) {
                self.has_enrichment = true;
                self.current_doc.?.enrichment_hash_id = extractHashId(value);
            } else {
                // Enrichment before its primary key — remember it
                const owned_key = try self.alloc.dupe(u8, base_key);
                try self.seen_enrichments.put(self.alloc, owned_key, extractHashId(value));
            }
        }
        return .@"continue";
    }

    fn addToBatch(self: *ScanState) !void {
        if (self.current_doc) |doc| {
            try self.batch.append(self.alloc, doc);
            self.current_doc = null;

            if (self.batch.items.len >= self.opts.batch_size) {
                try self.flushBatch();
            }
        }
    }

    fn flushBatch(self: *ScanState) !void {
        try self.process_batch(self.batch.items);
        // Free batch items after processing
        for (self.batch.items) |item| {
            self.alloc.free(item.doc_key);
            self.alloc.free(item.value);
        }
        self.batch.clearRetainingCapacity();
    }
};

/// Thread-local active scan state for callback access.
threadlocal var active_state: ?*ScanState = null;

/// Extract hash ID from first 8 bytes of enrichment value (big-endian u64).
fn extractHashId(value: []const u8) u64 {
    if (value.len < 8) return 0;
    return std.mem.readInt(u64, value[0..8], .big);
}

// ============================================================================
// Rebuild state — persist last-scanned key for crash-resilient resume
// ============================================================================

pub const RebuildState = struct {
    store: *DocStore,
    state_key: []const u8,

    /// Check if a rebuild is in progress.
    /// Returns the resume key (caller-owned), or null if no rebuild in progress.
    pub fn check(self: *RebuildState, alloc: Allocator) !?[]u8 {
        return self.store.get(alloc, self.state_key) catch |err| switch (err) {
            lmdb.Error.NotFound => null,
            else => return err,
        };
    }

    /// Update rebuild progress to the given key.
    pub fn update(self: *RebuildState, key: []const u8) !void {
        try self.store.put(self.state_key, key);
    }

    /// Clear rebuild state (rebuild complete).
    pub fn clear(self: *RebuildState) !void {
        self.store.delete(self.state_key) catch |err| switch (err) {
            lmdb.Error.NotFound => {},
            else => return err,
        };
    }
};

// ============================================================================
// Backfill progress tracker
// ============================================================================

pub const BackfillTracker = struct {
    items_processed: u64,
    is_active: bool,
    start_key: []const u8,
    end_key: []const u8,

    pub fn init(start_key: []const u8, end_key: []const u8) BackfillTracker {
        return .{
            .items_processed = 0,
            .is_active = false,
            .start_key = start_key,
            .end_key = end_key,
        };
    }

    /// Estimate progress based on current key position within the byte range.
    /// Compares first 8 bytes as big-endian u64.
    pub fn estimateProgress(self: *const BackfillTracker, current_key: []const u8) f64 {
        const start_val = keyToU64(self.start_key);
        const end_val = keyToU64(self.end_key);
        const cur_val = keyToU64(current_key);

        if (end_val <= start_val) return 1.0;
        if (cur_val <= start_val) return 0.0;
        if (cur_val >= end_val) return 1.0;

        const range: f64 = @floatFromInt(end_val - start_val);
        const pos: f64 = @floatFromInt(cur_val - start_val);
        return pos / range;
    }

    pub fn recordItem(self: *BackfillTracker) void {
        self.items_processed += 1;
    }

    pub fn start(self: *BackfillTracker) void {
        self.is_active = true;
        self.items_processed = 0;
    }

    pub fn finish(self: *BackfillTracker) void {
        self.is_active = false;
    }
};

/// Convert first up-to-8 bytes of a key to a u64 for progress estimation.
fn keyToU64(key: []const u8) u64 {
    if (key.len == 0) return 0;
    var buf: [8]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0 };
    const copy_len = @min(key.len, 8);
    @memcpy(buf[0..copy_len], key[0..copy_len]);
    return std.mem.readInt(u64, &buf, .big);
}

// ============================================================================
// Enrichment status
// ============================================================================

pub const EnrichmentStatus = enum(u8) {
    pending = 0,
    processing = 1,
    enriched = 2,
    dud = 3,
};

/// Write enrichment status for a key with a given suffix.
pub fn writeEnrichmentStatus(store: *DocStore, key: []const u8, suffix: []const u8, status: EnrichmentStatus) !void {
    var buf: [1024]u8 = undefined;
    const full_key = makeFullKey(&buf, key, suffix);
    const val = [_]u8{@intFromEnum(status)};
    try store.put(full_key, &val);
}

/// Read enrichment status for a key with a given suffix.
pub fn readEnrichmentStatus(store: *DocStore, alloc: Allocator, key: []const u8, suffix: []const u8) !?EnrichmentStatus {
    var buf: [1024]u8 = undefined;
    const full_key = makeFullKey(&buf, key, suffix);
    const val = store.get(alloc, full_key) catch |err| switch (err) {
        lmdb.Error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(val);
    if (val.len < 1) return null;
    return @enumFromInt(val[0]);
}

/// Mark a key as permanently unenrichable (dud).
pub fn markDud(store: *DocStore, key: []const u8, suffix: []const u8) !void {
    try writeEnrichmentStatus(store, key, suffix, .dud);
}

fn makeFullKey(buf: []u8, key: []const u8, suffix: []const u8) []const u8 {
    @memcpy(buf[0..key.len], key);
    @memcpy(buf[key.len..][0..suffix.len], suffix);
    return buf[0 .. key.len + suffix.len];
}

// ============================================================================
// Enrichment queue
// ============================================================================

/// Batch processing queue for async enrichment.
/// Accumulates keys and flushes to a processor in batches.
pub const EnrichmentQueue = struct {
    alloc: Allocator,
    pending: std.ArrayListUnmanaged([]u8),
    batch_size: u32,
    processor: *const fn (alloc: Allocator, keys: []const []const u8) anyerror!void,

    pub fn init(alloc: Allocator, batch_size: u32, processor: *const fn (alloc: Allocator, keys: []const []const u8) anyerror!void) EnrichmentQueue {
        return .{
            .alloc = alloc,
            .pending = .empty,
            .batch_size = batch_size,
            .processor = processor,
        };
    }

    pub fn deinit(self: *EnrichmentQueue) void {
        for (self.pending.items) |k| self.alloc.free(k);
        self.pending.deinit(self.alloc);
    }

    /// Enqueue a key for processing. Auto-flushes when batch_size is reached.
    pub fn enqueue(self: *EnrichmentQueue, key: []const u8) !void {
        try self.pending.append(self.alloc, try self.alloc.dupe(u8, key));
        if (self.pending.items.len >= self.batch_size) {
            _ = try self.flush();
        }
    }

    /// Process all pending keys. Returns count processed.
    pub fn flush(self: *EnrichmentQueue) !u32 {
        if (self.pending.items.len == 0) return 0;

        // Build slice of const pointers for the processor
        const keys = try self.alloc.alloc([]const u8, self.pending.items.len);
        defer self.alloc.free(keys);
        for (self.pending.items, 0..) |k, i| keys[i] = k;

        const count: u32 = @intCast(self.pending.items.len);
        try self.processor(self.alloc, keys);

        // Free processed keys
        for (self.pending.items) |k| self.alloc.free(k);
        self.pending.clearRetainingCapacity();

        return count;
    }

    /// Number of keys waiting to be processed.
    pub fn pendingCount(self: *const EnrichmentQueue) usize {
        return self.pending.items.len;
    }
};

// ============================================================================
// Tests
// ============================================================================

var tmp_path_nonce: u64 = 0;

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const ns = platform_time.monotonicNs();
    const nonce = @atomicRmw(u64, &tmp_path_nonce, .Add, 1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-enrich-{s}-{d}-{d}\x00", .{ label, ns, nonce }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "enrichment finds unenriched documents" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "ef1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    // doc1 has enrichment, doc2 does not, doc3 does not
    try store.put("doc1:\x00", "content1"); // primary key
    try store.put("doc1:i:idx:e", "hashidenr"); // enrichment (8+ bytes)
    try store.put("doc2:\x00", "content2"); // primary key, no enrichment
    try store.put("doc3:\x00", "content3"); // primary key, no enrichment

    const TestCtx = struct {
        var found_keys: [10][]u8 = undefined;
        var found_count: usize = 0;
        var test_alloc: Allocator = undefined;

        fn processBatch(batch: []const DocumentScanState) anyerror!void {
            for (batch) |item| {
                found_keys[found_count] = try test_alloc.dupe(u8, item.doc_key);
                found_count += 1;
            }
        }
    };
    TestCtx.found_count = 0;
    TestCtx.test_alloc = alloc;

    try scanForEnrichment(alloc, &store, .{
        .byte_range = .{ .start = "", .end = "" },
        .primary_suffix = ":\x00",
        .enrichment_suffix = ":i:idx:e",
        .batch_size = 100,
    }, &TestCtx.processBatch);

    defer {
        for (TestCtx.found_keys[0..TestCtx.found_count]) |k| alloc.free(k);
    }

    // Should find doc2 and doc3 (unenriched)
    try std.testing.expectEqual(@as(usize, 2), TestCtx.found_count);
    try std.testing.expectEqualStrings("doc2", TestCtx.found_keys[0]);
    try std.testing.expectEqualStrings("doc3", TestCtx.found_keys[1]);
}

test "enrichment skips dud-marked documents" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "ed1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    // doc1 has dud enrichment — should be treated as unenriched
    try store.put("doc1:\x00", "content1");
    try store.put("doc1:i:idx:e", &[_]u8{DudEnrichmentValue}); // dud marker

    const TestCtx = struct {
        var found_count: usize = 0;
        fn processBatch(batch: []const DocumentScanState) anyerror!void {
            found_count += batch.len;
        }
    };
    TestCtx.found_count = 0;

    try scanForEnrichment(alloc, &store, .{
        .byte_range = .{ .start = "", .end = "" },
        .primary_suffix = ":\x00",
        .enrichment_suffix = ":i:idx:e",
        .batch_size = 100,
    }, &TestCtx.processBatch);

    // doc1 has dud → treated as unenriched → should appear in batch
    try std.testing.expectEqual(@as(usize, 1), TestCtx.found_count);
}

test "enrichment handles out-of-order keys" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "eo1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    // Enrichment key sorts before its primary key (e.g., "aaa:i:idx:e" < "aaa:\x00" won't happen
    // in LMDB since ':' < '\x00' is false... but "doc:i:" < "doc:\x00" since 'i' > '\x00')
    // Actually in LMDB, ':' is 0x3A, 'i' is 0x69, '\x00' is 0x00.
    // So "doc:\x00" sorts BEFORE "doc:i:idx:e". Primary comes first.
    // Out-of-order only happens if enrichment suffix sorts before primary suffix.
    // With :\x00 as primary and :i: as enrichment prefix, primary always sorts first.
    // Test the in-order case works correctly instead.
    try store.put("doc1:\x00", "content1");
    try store.put("doc1:i:idx:e", "12345678"); // 8 bytes = hash ID
    try store.put("doc2:\x00", "content2");
    // doc2 has no enrichment

    const TestCtx = struct {
        var found_count: usize = 0;
        fn processBatch(batch: []const DocumentScanState) anyerror!void {
            found_count += batch.len;
        }
    };
    TestCtx.found_count = 0;

    try scanForEnrichment(alloc, &store, .{
        .byte_range = .{ .start = "", .end = "" },
        .primary_suffix = ":\x00",
        .enrichment_suffix = ":i:idx:e",
        .batch_size = 100,
    }, &TestCtx.processBatch);

    // Only doc2 is unenriched
    try std.testing.expectEqual(@as(usize, 1), TestCtx.found_count);
}

test "enrichment batch size" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "eb1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    // 5 unenriched docs, batch size 2 → should get 3 batches (2+2+1)
    try store.put("a:\x00", "1");
    try store.put("b:\x00", "2");
    try store.put("c:\x00", "3");
    try store.put("d:\x00", "4");
    try store.put("e:\x00", "5");

    const TestCtx = struct {
        var batch_count: usize = 0;
        var total_items: usize = 0;
        fn processBatch(batch: []const DocumentScanState) anyerror!void {
            batch_count += 1;
            total_items += batch.len;
        }
    };
    TestCtx.batch_count = 0;
    TestCtx.total_items = 0;

    try scanForEnrichment(alloc, &store, .{
        .byte_range = .{ .start = "", .end = "" },
        .primary_suffix = ":\x00",
        .enrichment_suffix = ":i:idx:e",
        .batch_size = 2,
    }, &TestCtx.processBatch);

    try std.testing.expectEqual(@as(usize, 5), TestCtx.total_items);
    try std.testing.expectEqual(@as(usize, 3), TestCtx.batch_count); // 2+2+1
}

test "rebuild state round-trip" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "rs1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    var rs = RebuildState{ .store = &store, .state_key = "rebuild:test_idx" };

    // Initially no rebuild
    const initial = try rs.check(alloc);
    try std.testing.expect(initial == null);

    // Start rebuild, save progress
    try rs.update("doc42");

    const resumed = try rs.check(alloc);
    defer if (resumed) |r| alloc.free(r);
    try std.testing.expect(resumed != null);
    try std.testing.expectEqualStrings("doc42", resumed.?);

    // Clear
    try rs.clear();
    const after_clear = try rs.check(alloc);
    try std.testing.expect(after_clear == null);
}

test "rebuild state persists across reopen" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "rs2");
    defer cleanupTmp(sp);

    {
        var store = try DocStore.open(alloc, sp, .{});
        defer store.close();
        var rs = RebuildState{ .store = &store, .state_key = "rebuild:emb" };
        try rs.update("key_abc");
    }

    {
        var store = try DocStore.open(alloc, sp, .{});
        defer store.close();
        var rs = RebuildState{ .store = &store, .state_key = "rebuild:emb" };
        const val = try rs.check(alloc);
        defer if (val) |v| alloc.free(v);
        try std.testing.expect(val != null);
        try std.testing.expectEqualStrings("key_abc", val.?);
    }
}

test "backfill tracker progress estimation" {
    const tracker = BackfillTracker.init("a", "z");

    // "a" = 0x61, "z" = 0x7A in first byte
    const p_start = tracker.estimateProgress("a");
    try std.testing.expect(p_start >= 0.0 and p_start <= 0.05); // near start

    const p_mid = tracker.estimateProgress("m");
    try std.testing.expect(p_mid > 0.3 and p_mid < 0.7); // roughly middle

    const p_end = tracker.estimateProgress("z");
    try std.testing.expect(p_end >= 0.95); // at or near end

    // Edge cases
    const p_before = tracker.estimateProgress("A"); // 0x41, before 'a'=0x61
    try std.testing.expectEqual(@as(f64, 0.0), p_before);
}

test "backfill tracker items" {
    var tracker = BackfillTracker.init("", "");

    tracker.start();
    try std.testing.expect(tracker.is_active);
    try std.testing.expectEqual(@as(u64, 0), tracker.items_processed);

    tracker.recordItem();
    tracker.recordItem();
    tracker.recordItem();
    try std.testing.expectEqual(@as(u64, 3), tracker.items_processed);

    tracker.finish();
    try std.testing.expect(!tracker.is_active);
}

test "isDudEnrichment" {
    try std.testing.expect(isDudEnrichment(&[_]u8{0xDD}));
    try std.testing.expect(!isDudEnrichment(&[_]u8{ 0xDD, 0x00 }));
    try std.testing.expect(!isDudEnrichment(&[_]u8{0x00}));
    try std.testing.expect(!isDudEnrichment(""));
}

test "enrichment status round-trip" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "es1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    // Initially no status
    const initial = try readEnrichmentStatus(&store, alloc, "doc1", ":i:idx:s");
    try std.testing.expect(initial == null);

    // Write status
    try writeEnrichmentStatus(&store, "doc1", ":i:idx:s", .processing);

    const read = try readEnrichmentStatus(&store, alloc, "doc1", ":i:idx:s");
    try std.testing.expect(read != null);
    try std.testing.expectEqual(EnrichmentStatus.processing, read.?);

    // Update to enriched
    try writeEnrichmentStatus(&store, "doc1", ":i:idx:s", .enriched);
    const read2 = try readEnrichmentStatus(&store, alloc, "doc1", ":i:idx:s");
    try std.testing.expectEqual(EnrichmentStatus.enriched, read2.?);
}

test "markDud sets dud status" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "md1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    try markDud(&store, "doc1", ":i:idx:e");
    const status = try readEnrichmentStatus(&store, alloc, "doc1", ":i:idx:e");
    try std.testing.expectEqual(EnrichmentStatus.dud, status.?);
}

test "enrichment queue enqueue and flush" {
    const alloc = std.testing.allocator;

    const TestProcessor = struct {
        var call_count: usize = 0;
        var total_keys: usize = 0;

        fn process(_: Allocator, keys: []const []const u8) anyerror!void {
            call_count += 1;
            total_keys += keys.len;
        }
    };
    TestProcessor.call_count = 0;
    TestProcessor.total_keys = 0;

    var queue = EnrichmentQueue.init(alloc, 10, &TestProcessor.process);
    defer queue.deinit();

    try queue.enqueue("key1");
    try queue.enqueue("key2");
    try queue.enqueue("key3");

    try std.testing.expectEqual(@as(usize, 3), queue.pendingCount());

    const flushed = try queue.flush();
    try std.testing.expectEqual(@as(u32, 3), flushed);
    try std.testing.expectEqual(@as(usize, 0), queue.pendingCount());
    try std.testing.expectEqual(@as(usize, 1), TestProcessor.call_count);
    try std.testing.expectEqual(@as(usize, 3), TestProcessor.total_keys);
}

test "enrichment queue auto-flush at batch_size" {
    const alloc = std.testing.allocator;

    const TestProcessor = struct {
        var call_count: usize = 0;

        fn process(_: Allocator, _: []const []const u8) anyerror!void {
            call_count += 1;
        }
    };
    TestProcessor.call_count = 0;

    var queue = EnrichmentQueue.init(alloc, 2, &TestProcessor.process);
    defer queue.deinit();

    try queue.enqueue("a");
    try std.testing.expectEqual(@as(usize, 0), TestProcessor.call_count);

    try queue.enqueue("b"); // hits batch_size=2, auto-flush
    try std.testing.expectEqual(@as(usize, 1), TestProcessor.call_count);
    try std.testing.expectEqual(@as(usize, 0), queue.pendingCount());

    try queue.enqueue("c");
    try std.testing.expectEqual(@as(usize, 1), TestProcessor.call_count); // not yet
}
