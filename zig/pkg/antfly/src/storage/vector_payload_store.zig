// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Exact source payload ownership independent of ANN index lifetime.
//! Immutable identities include the complete logical artifact key and envelope.
//! Primary commit, not preparation in this store, selects visible artifacts.
const std = @import("std");
const payload = @import("artifact_payload.zig");
const native = @import("vector_block_store.zig");
const lsm = @import("lsm_backend/mod.zig");
const codec = @import("db/enrichment/artifact_codec.zig");
const erased = @import("backend_erased.zig");
const vector_block = @import("antfly_vectorindex").vector_block;
const Allocator = std.mem.Allocator;
const resources = @import("resource_manager.zig");
const time = @import("antfly_platform").time;
const LiveSet = @import("source_vector_live_set.zig").LiveSet;
const generation_publication = @import("generation_publication.zig");

pub const Stats = payload.Stats;

// Filled only by the deterministic interleaving test below.
const MarkInterleaving = if (@import("builtin").is_test) struct {
    var entered: std.atomic.Value(bool) = .init(false);
    var resume_scan: std.atomic.Value(bool) = .init(false);
    var cancel_entered: std.atomic.Value(bool) = .init(false);
    var cancel_done: std.atomic.Value(bool) = .init(false);
    var scan_error: ?anyerror = null;

    fn pause(_: *Store) void {
        entered.store(true, .release);
        while (!resume_scan.load(.acquire)) std.Thread.yield() catch {};
    }
    fn scan(source: *Store) void {
        source.advanceMarkingSnapshot() catch |err| {
            scan_error = err;
        };
    }
    fn cancel(source: *Store) void {
        cancel_entered.store(true, .release);
        source.cancelMarking();
        cancel_done.store(true, .release);
    }
    fn awaitFlag(flag: *std.atomic.Value(bool)) !void {
        const start = time.monotonicNs();
        while (!flag.load(.acquire)) {
            if (time.monotonicNs() -| start > 10 * std.time.ns_per_s) return error.InterleavingTimedOut;
            std.Thread.yield() catch {};
        }
    }
} else void;

pub const SegmentSizing = struct {
    /// Zero preserves the fixed-shard control for same-binary experiments.
    target_bytes: u64 = 0,
    min_shards: u32 = 16,
    max_shards: u32 = 1024,

    fn fromEnvironment() !SegmentSizing {
        const raw = if (@import("builtin").link_libc) std.c.getenv("ANTFLY_SOURCE_VECTOR_TARGET_SEGMENT_BYTES") else null;
        const text = if (raw) |value| std.mem.span(value) else return .{};
        const target = std.fmt.parseInt(u64, text, 10) catch return error.InvalidArgument;
        if (target != 0 and target < 1024 * 1024) return error.InvalidArgument;
        return .{ .target_bytes = target };
    }

    fn shardCount(self: SegmentSizing, bytes: u64, current: u32) u32 {
        if (self.target_bytes == 0) return current;
        var count = self.min_shards;
        while (count < self.max_shards and bytes / count > self.target_bytes) count *= 2;
        return count;
    }
};

pub const Store = struct {
    alloc: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    opened: native.Opened,
    stats: Stats = .{},
    read_only: bool,
    poisoned: bool = false,
    budget: ?*resources.BudgetedAllocator = null,
    wal_admission_bytes: u64 = 64 * 1024 * 1024,
    ann_reference_root: ?[]u8 = null,
    ann_scopes: ?[]u64 = null,
    location_cache: ?*native.ReferenceLocationCache = null,
    segment_sizing: SegmentSizing = .{},
    collection: ?*Collection = null,
    marking: ?*Marking = null,
    mark_step_rows: usize = 0,
    mark_step_ns: u64 = 0,
    mark_outside_lock: bool = false,
    scan_duty_percent: u8 = 0,
    last_scan_ns: u64 = 0,
    rescue_reappends: bool = false,
    shared_catalog: bool = false,
    independent_scan: bool = false,
    incremental_inventory: bool = false,
    delta_inventory: bool = false,
    debt_scheduling: bool = false,
    bitmap_marking: bool = false,
    bitmap_locator: bool = false,
    incremental_planning: bool = false,
    sparse_gc_copy_bytes: u64 = 0,
    inventory_min_payloads: u64 = 0,
    inventory_requested: bool = false,
    last_mark_completed_ns: u64 = 0,
    obsolete_debt: u64 = 0,
    inventory: Inventory = .{},
    // Test-only pause/clock injection exercises an actual in-flight scan.
    mark_test_hook: if (@import("builtin").is_test) ?*const fn (*Store) void else void = if (@import("builtin").is_test) null else {},
    coalesce_directory: bool = false,
    receipt: ?CheckpointReceipt = null,
    checkpoint_receipts: bool = false,
    snapshot_reads: bool = false,
    append_only: bool = false,
    selective_gc: bool = false,
    group_commit: bool = false,
    preparation_alloc: Allocator = undefined,
    preparation_manager: ?*resources.ResourceManager = null,
    prepare_queue_mutex: std.atomic.Mutex = .unlocked,
    prepare_head: ?*PrepareRequest = null,
    prepare_tail: ?*PrepareRequest = null,
    prepare_running: bool = false,
    prepare_requests: std.atomic.Value(u64) = .init(0),

    directory: ?*@import("source_location_directory.zig").Directory = null,

    /// Rebuildable physical occurrence cache, never commit/ownership authority.
    /// WAL membership is refreshed at installation, not on every preparation.
    /// Between installations the normal append counters track new payloads.
    const Inventory = struct {
        const WalEvent = struct {
            digest: payload.Digest,
            batch: u64,
            fn less(_: void, a: @This(), b: @This()) bool {
                return a.batch < b.batch;
            }
        };
        const Occurrence = struct { dims: u32, count: u64 };
        counts: std.AutoHashMapUnmanaged(payload.Digest, Occurrence) = .empty,
        wal: std.AutoHashMapUnmanaged(payload.Digest, u64) = .empty,
        wal_events: std.ArrayListUnmanaged(WalEvent) = .empty,
        wal_head: usize = 0,
        delta: bool = false,
        wal_rows: u64 = 0,
        wal_retirements: u64 = 0,
        delta_installs: u64 = 0,
        fallback_installs: u64 = 0,
        segments: std.AutoHashMapUnmanaged(u128, void) = .empty,
        bytes: u64 = 0,
        initialized: bool = false,

        fn deinit(self: *@This(), alloc: Allocator) void {
            self.counts.deinit(alloc);
            self.wal.deinit(alloc);
            self.wal_events.deinit(alloc);
            self.segments.deinit(alloc);
            self.* = .{ .delta = self.delta };
        }
        fn id(reader: vector_block.Reader) u128 {
            return (@as(u128, reader.generation) << 64) | reader.shard_id;
        }
        fn add(self: *@This(), alloc: Allocator, digest: payload.Digest, dims: u32) !void {
            const entry = try self.counts.getOrPut(alloc, digest);
            if (!entry.found_existing) {
                entry.value_ptr.* = .{ .dims = dims, .count = 1 };
                self.bytes += @as(u64, dims) * 4;
            } else {
                if (entry.value_ptr.dims != dims) return error.VectorReferenceIdentityMismatch;
                entry.value_ptr.count = try std.math.add(u64, entry.value_ptr.count, 1);
            }
        }
        fn remove(self: *@This(), digest: payload.Digest) !void {
            const entry = self.counts.getPtr(digest) orelse return error.InvalidVectorInventory;
            if (entry.count > 1) entry.count -= 1 else {
                self.bytes -= @as(u64, entry.dims) * 4;
                _ = self.counts.remove(digest);
            }
        }
        fn addWal(self: *@This(), alloc: Allocator, key: []const u8, dims: u32, batch: u64) !void {
            if (key.len != 32) return error.InvalidVectorReference;
            self.wal_rows += 1;
            const entry = try self.wal.getOrPut(alloc, key[0..32].*);
            if (!entry.found_existing) {
                entry.value_ptr.* = batch;
                try self.add(alloc, key[0..32].*, dims);
            } else if (entry.value_ptr.* >= batch) return else entry.value_ptr.* = batch;
            if (self.delta) try self.wal_events.append(alloc, .{ .digest = key[0..32].*, .batch = batch });
        }
        fn addTree(self: *@This(), alloc: Allocator, node: ?*@import("vector_wal_view.zig").Node) anyerror!void {
            if (node) |n| {
                try self.addTree(alloc, n.left);
                if (n.record.kind == .upsert) try self.addWal(alloc, n.record.key, n.record.dims, n.record.batch_id);
                try self.addTree(alloc, n.right);
            }
        }
        fn retireWal(self: *@This(), through: u64) !void {
            while (self.wal_head < self.wal_events.items.len) {
                const event = self.wal_events.items[self.wal_head];
                if (event.batch > through) break;
                self.wal_head += 1;
                if (self.wal.get(event.digest)) |latest| {
                    if (latest != event.batch) continue;
                    try self.remove(event.digest);
                    _ = self.wal.remove(event.digest);
                    self.wal_retirements += 1;
                }
            }
            if (self.wal_head >= self.wal_events.items.len / 2) {
                const remaining = self.wal_events.items.len - self.wal_head;
                std.mem.copyForwards(WalEvent, self.wal_events.items[0..remaining], self.wal_events.items[self.wal_head..]);
                self.wal_events.items.len = remaining;
                self.wal_head = 0;
            }
        }
        fn sync(self: *@This(), alloc: Allocator, previous: ?*const native.Opened, next: *const native.Opened, rows: *u64) !void {
            // Any partial cache edit is discarded. A failed post-publication
            // install must reopen durable authority before accepting writes.
            errdefer self.deinit(alloc);
            var next_segments: std.AutoHashMapUnmanaged(u128, void) = .empty;
            defer next_segments.deinit(alloc);
            try next_segments.ensureTotalCapacity(alloc, @intCast(next.readers.len));
            for (next.readers) |reader| next_segments.putAssumeCapacity(id(reader), {});
            // The prepared successor carries a validated WAL-prefix delta.
            // Normal checkpoints empty the WAL; unknown transitions rebuild.
            const empty_wal = next.wal_tree == null and next.wal.records.items.len == 0;
            const reuse = if (empty_wal) native.Store.WalReuse{ .after_batch = std.math.maxInt(u64) } else next.wal_inventory_delta;
            const use_delta = self.delta and self.initialized and reuse != null;
            if (use_delta) self.delta_installs += 1 else self.fallback_installs += 1;
            if (self.initialized) {
                if (use_delta) {
                    switch (reuse.?) {
                        .all => {},
                        .after_batch => |batch| try self.retireWal(batch),
                    }
                } else {
                    var wal = self.wal.keyIterator();
                    while (wal.next()) |digest| try self.remove(digest.*);
                    self.wal.clearRetainingCapacity();
                    self.wal_events.clearRetainingCapacity();
                    self.wal_head = 0;
                }
                for (previous.?.readers) |reader| {
                    if (next_segments.contains(id(reader))) continue;
                    for (0..reader.count) |i| {
                        const row = reader.sourceIdentityAt(i);
                        rows.* += 1;
                        if (!row.vector or row.key.len != 32) return error.InvalidVectorReference;
                        try self.remove(row.key[0..32].*);
                    }
                }
            }
            for (next.readers) |reader| {
                if (self.segments.contains(id(reader))) continue;
                for (0..reader.count) |i| {
                    const row = reader.sourceIdentityAt(i);
                    rows.* += 1;
                    if (!row.vector or row.key.len != 32) return error.InvalidVectorReference;
                    try self.add(alloc, row.key[0..32].*, row.dims);
                }
            }
            if (!use_delta) {
                for (next.wal.records.items) |record| {
                    if (record.kind == .upsert) try self.addWal(alloc, record.key, record.dims, record.batch_id);
                }
                try self.addTree(alloc, next.wal_tree);
                if (self.delta) std.mem.sort(WalEvent, self.wal_events.items, {}, WalEvent.less);
            }
            self.segments.deinit(alloc);
            self.segments = next_segments;
            next_segments = .empty;
            self.initialized = true;
        }
    };

    fn prepareOpened(self: *Store, next: *native.Opened) !void {
        if (self.inventory_min_payloads != 0) {
            const desired = self.inventory_requested and self.stats.retained_payloads >= self.inventory_min_payloads;
            if (desired != self.incremental_inventory) {
                self.inventory.deinit(self.alloc);
                self.incremental_inventory = desired;
                self.stats.inventory_policy_switches += 1;
            }
        }
        if (self.shared_catalog) try next.shareSegmentCatalog();
        if (self.incremental_inventory) {
            const started = time.monotonicNs();
            defer self.stats.inventory_update_ns += time.monotonicNs() -| started;
            try self.inventory.sync(self.alloc, &self.opened, next, &self.stats.inventory_rows_scanned);
            self.stats.inventory_updates += 1;
        }
    }

    fn installOpened(self: *Store, next: *native.Opened) !void {
        try self.prepareOpened(next);
        self.opened.deinit();
        self.opened = next.*;
    }

    const PrepareRequest = struct {
        prepared: []const payload.Prepared,
        next: ?*PrepareRequest = null,
        state: std.atomic.Value(u8) = .init(0), // queued, elected leader, complete
        err: ?anyerror = null,
    };

    fn experimentEnabled(name: [*:0]const u8) bool {
        const raw = if (@import("builtin").link_libc) std.c.getenv(name) else null;
        return if (raw) |value| std.mem.eql(u8, std.mem.span(value), "1") else false;
    }

    const CheckpointReceipt = struct {
        source: payload.Digest,
        retained_payloads: u64,
        retained_payload_bytes: u64,
        primary_epoch: ?u64 = null,
        ann: ?payload.Digest = null,
    };

    fn sourceAuthorityDigest(self: *Store) !payload.Digest {
        const path = try std.fs.path.join(self.alloc, &.{ self.opened.store.root_dir, "CURRENT" });
        defer self.alloc.free(path);
        const current = try self.opened.store.storage.readFileAlloc(self.alloc, path, 4 * 1024 * 1024);
        defer self.alloc.free(current);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(current);
        for ([_]u64{ self.opened.store.wal_generation, self.opened.store.wal_committed_bytes, self.opened.store.last_committed_batch orelse 0, self.opened.store.covered_source_sequence }) |value| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, value, .little);
            hash.update(&bytes);
        }
        return hash.finalResult();
    }

    pub fn setAnnScopes(self: *Store, scopes: []const u64) !void {
        self.lock();
        defer self.mutex.unlock();
        const copy = try self.alloc.dupe(u64, scopes);
        if (self.ann_scopes) |old| self.alloc.free(old);
        self.ann_scopes = copy;
    }

    fn annScopeIsLive(self: *Store, key: []const u8) bool {
        const scopes = self.ann_scopes orelse return true;
        const scope = @import("internal_keys.zig").embeddingArtifactScopeHash(key) orelse return true;
        return std.mem.indexOfScalar(u64, scopes, scope) != null;
    }

    fn annAuthorityDigest(self: *Store) !payload.Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        if (self.ann_scopes) |scopes| {
            hash.update("catalog-scopes");
            hash.update(std.mem.sliceAsBytes(scopes));
        }
        const root = self.ann_reference_root orelse return hash.finalResult();
        const names = self.opened.store.storage.listFileNamesAlloc(self.alloc, root) catch |err| switch (err) {
            error.FileNotFound => return hash.finalResult(),
            else => return err,
        };
        defer lsm.Storage.freeFileNames(self.alloc, names);
        const Order = struct {
            fn less(_: void, a: []u8, b: []u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        };
        std.mem.sort([]u8, names, {}, Order.less);
        for (names) |name| {
            if (!std.mem.eql(u8, name, "CURRENT") and !std.mem.endsWith(u8, name, ".afvw")) continue;
            const path = try std.fs.path.join(self.alloc, &.{ root, name });
            defer self.alloc.free(path);
            // Hash actual WAL contents: size/mtime alone is not a durable
            // identity after torn-tail repair. Immutable vector blocks are
            // already identified by CURRENT, so this does not read the corpus.
            const bytes = try self.opened.store.storage.readFileAlloc(self.alloc, path, 512 * 1024 * 1024);
            defer self.alloc.free(bytes);
            hash.update(name);
            hash.update(bytes);
        }
        return hash.finalResult();
    }

    fn primaryReferenceEpoch(txn: anytype) !u64 {
        const bytes = txn.get(payload.reference_epoch_key) catch |err| switch (err) {
            error.NotFound => return 0,
            else => return err,
        };
        if (bytes.len != 8) return error.InvalidVectorReferenceEpoch;
        return std.mem.readInt(u64, bytes[0..8], .little);
    }

    fn loadCheckpointReceipt(self: *Store) !bool {
        if (!self.checkpoint_receipts) return false;
        const path = try std.fs.path.join(self.alloc, &.{ self.opened.store.root_dir, "SOURCE_CHECKPOINT" });
        defer self.alloc.free(path);
        const bytes = self.opened.store.storage.readFileAlloc(self.alloc, path, 4096) catch return false;
        defer self.alloc.free(bytes);
        if (bytes.len < 40 or !std.mem.eql(u8, bytes[0..8], "AFVSCP01")) return false;
        var checksum: payload.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes[40..], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[8..40])) return false;
        const parsed = std.json.parseFromSlice(CheckpointReceipt, self.alloc, bytes[40..], .{}) catch return false;
        defer parsed.deinit();
        const receipt = parsed.value;
        if (!std.mem.eql(u8, &receipt.source, &try self.sourceAuthorityDigest())) return false;
        self.receipt = receipt;
        self.stats.retained_payloads = receipt.retained_payloads;
        self.stats.retained_payload_bytes = receipt.retained_payload_bytes;
        self.stats.checkpoint_inventory_restores += 1;
        return true;
    }

    fn saveCheckpointReceipt(self: *Store, epoch: ?u64, ann: ?payload.Digest) !void {
        if (!self.checkpoint_receipts) return;
        const receipt = CheckpointReceipt{
            .source = try self.sourceAuthorityDigest(),
            .retained_payloads = self.stats.retained_payloads,
            .retained_payload_bytes = self.stats.retained_payload_bytes,
            .primary_epoch = epoch,
            .ann = ann,
        };
        const json = try std.json.Stringify.valueAlloc(self.alloc, receipt, .{});
        defer self.alloc.free(json);
        const bytes = try self.alloc.alloc(u8, 40 + json.len);
        defer self.alloc.free(bytes);
        @memcpy(bytes[0..8], "AFVSCP01");
        std.crypto.hash.sha2.Sha256.hash(json, bytes[8..40], .{});
        @memcpy(bytes[40..], json);
        const path = try std.fs.path.join(self.alloc, &.{ self.opened.store.root_dir, "SOURCE_CHECKPOINT" });
        defer self.alloc.free(path);
        try generation_publication.publishControlFile(self.alloc, self.opened.store.storage, path, bytes);
        self.stats.checkpoint_receipt_bytes_written += bytes.len;
        self.receipt = receipt;
    }

    const CollectionItem = struct {
        digest: payload.Digest,
        dims: u32,
        hash: u64,
        shard: u32,
        fn less(_: void, a: @This(), b: @This()) bool {
            if (a.shard != b.shard) return a.shard < b.shard;
            if (a.hash != b.hash) return a.hash < b.hash;
            return std.mem.order(u8, &a.digest, &b.digest) == .lt;
        }
    };

    const Collection = struct {
        input: native.Opened,
        live: LiveSet,
        tail: std.AutoHashMap(payload.Digest, u32),
        items: []CollectionItem,
        shards: u32,
        generation: u64,
        boundary: native.WalPrefixBoundary,
        staged: std.ArrayListUnmanaged(native.StagedBlock) = .empty,
        output: ?native.Store.StreamingBlock = null,
        scratch: std.ArrayListUnmanaged(f32) = .empty,
        pos: usize = 0,
        shard: u32 = 0,
        bytes_read: u64 = 0,
        total_live_bytes: u64,
        marked_live_bytes: u64,
        bytes_written: u64 = 0,
        publication_attempted: bool = false,
        primary_epoch: u64,
        ann: payload.Digest,
        rescued: bool = false,
        debt_at_cut: u64 = 0,
        selected: ?[]@import("antfly_vectorindex").vector_block_manifest.Segment = null,

        fn deinit(self: *@This(), alloc: Allocator) void {
            if (self.output) |*output| output.deinit();
            if (!self.publication_attempted) self.input.store.discardStagedBlocks(self.staged.items);
            self.staged.deinit(alloc);
            self.scratch.deinit(alloc);
            alloc.free(self.items);
            if (self.selected) |selected| alloc.free(selected);
            self.live.deinit();
            self.tail.deinit();
            self.input.deinit();
            alloc.destroy(self);
        }
    };

    // Own the primary snapshot and ANN generation across maintenance turns.
    // No cursor key/value is retained after advancing its cursor.
    const Marking = struct {
        txn: erased.ReadTxn,
        cursor: erased.Cursor,
        indexed: bool,
        started: bool = false,
        primary_done: bool = false,
        ann: ?native.Opened,
        ann_reader: usize = 0,
        ann_row: usize = 0,
        ann_wal: usize = 0,
        verification: ?LiveSet.Iterator = null,
        verified_bytes: u64 = 0,
        epoch: u64,
        ann_digest: payload.Digest,
        boundary: native.WalPrefixBoundary,
        live: LiveSet,
        tail: std.AutoHashMap(payload.Digest, u32),
        source: native.Opened,
        retained_at_cut: u64,
        debt_at_cut: u64,
        scopes: ?[]u64,
        outside_lock: bool,
        running: bool = false, // source mutex protects lifetime and scan admission
        cancel_requested: bool = false,
        scan_done: bool = false,
        verification_done: bool = false,
        // Scanner-owned density summaries over the pinned physical cut. The
        // checkpoint path cannot replace segments while this mark is active.
        segment_stats: ?[]SegmentStats = null,
        planning_reader: usize = 0,
        planning_row: usize = 0,
        planning_required: bool = false,
        // Scanner-owned live map; only these bounded discoveries cross back
        // into the writer-owned tail map at the end of a scan turn.
        discovered: std.ArrayListUnmanaged(payload.Digest) = .empty,
        pending_tail: std.ArrayListUnmanaged(payload.Digest) = .empty,
        // Writer-owned protection requests for payloads durable before this cut.
        // Merge into live only after the scanner rejoins. No location may retire
        // until all these requests have been included in the copy plan.
        rescued: std.AutoHashMapUnmanaged(payload.Digest, u32) = .empty,
        rescued_any: bool = false,

        fn putLive(self: *@This(), digest: payload.Digest, dims: u32) !void {
            try self.live.put(digest, dims);
            // A retry can enter the suffix before the scan reaches its old
            // owner. Count the immutable payload once when planning finishes.
            if (self.outside_lock) {
                try self.discovered.append(self.live.allocator, digest);
            } else _ = self.tail.remove(digest);
        }

        fn deinit(self: *@This(), alloc: Allocator) void {
            std.debug.assert(!self.running);
            if (self.segment_stats) |stats| alloc.free(stats);
            self.source.deinit();
            if (self.scopes) |scopes| alloc.free(scopes);
            self.discovered.deinit(alloc);
            self.pending_tail.deinit(alloc);
            self.rescued.deinit(alloc);
            self.cursor.close();
            self.txn.abort();
            if (self.ann) |*ann| ann.deinit();
            self.live.deinit();
            self.tail.deinit();
            alloc.destroy(self);
        }
    };

    const ScanProgress = struct {
        rows: u64 = 0,
        owners: u64 = 0,
        retired: u64 = 0,
        budget_yield: bool = false,
    };

    fn markAnnKey(marking: *Marking, key: []const u8, progress: *ScanProgress) !void {
        if (marking.scopes) |scopes| {
            if (@import("internal_keys.zig").embeddingArtifactScopeHash(key)) |scope| {
                if (std.mem.indexOfScalar(u64, scopes, scope) == null) {
                    progress.retired += 1;
                    return;
                }
            }
        }
        const opened = &marking.ann.?;
        const row = try opened.get(key, opened.store.covered_source_sequence, null);
        if (row != .vector or row.vector.encoding != .artifact_reference) return;
        if (row.vector.bytes.len != 32) return error.InvalidVectorReference;
        try marking.putLive(row.vector.bytes[0..32].*, row.vector.dims);
    }

    fn markBudgetExpired(started: u64, budget_ns: u64, limit: usize, progress: *ScanProgress) bool {
        if (progress.rows >= limit) return true;
        // Always admit one row. The elapsed limit is cooperative: one backend
        // operation or scheduler stall may exceed it, but no next row starts.
        if (progress.rows != 0 and budget_ns != 0 and time.monotonicNs() -| started >= budget_ns) {
            progress.budget_yield = true;
            return true;
        }
        return false;
    }

    fn scanMarking(marking: *Marking, started: u64, budget_ns: u64, limit: usize, allow_verification: bool, progress: *ScanProgress) !void {
        while (marking.live.locatorPending()) {
            if (markBudgetExpired(started, budget_ns, limit, progress)) return;
            if (try marking.live.advanceLocator()) progress.rows += 1;
        }
        while (!marking.primary_done) {
            if (markBudgetExpired(started, budget_ns, limit, progress)) return;
            const entry = if (!marking.started)
                (if (marking.indexed) try marking.cursor.seekAtOrAfter(payload.ownership_prefix) else try marking.cursor.first())
            else
                try marking.cursor.next();
            marking.started = true;
            const row = entry orelse {
                marking.primary_done = true;
                break;
            };
            progress.rows += 1;
            if (marking.indexed) {
                if (!std.mem.startsWith(u8, row.key, payload.ownership_prefix)) {
                    marking.primary_done = true;
                    break;
                }
                progress.owners += 1;
                if (row.value.len != 36) return error.InvalidVectorOwnershipRecord;
                try marking.putLive(row.value[0..32].*, std.mem.readInt(u32, row.value[32..36], .little));
            } else if (payload.isEmbeddingKey(row.key) and payload.isReference(row.value)) {
                const ref = try payload.Reference.decode(row.value);
                try marking.putLive(ref.digest, ref.dims);
            }
        }
        if (marking.ann) |*ann| {
            while (marking.ann_reader < ann.readers.len) {
                const reader = ann.readers[marking.ann_reader];
                while (marking.ann_row < reader.count) : (marking.ann_row += 1) {
                    if (markBudgetExpired(started, budget_ns, limit, progress)) return;
                    progress.rows += 1;
                    const row = reader.sourceIdentityAt(marking.ann_row);
                    try markAnnKey(marking, row.key, progress);
                }
                marking.ann_row = 0;
                marking.ann_reader += 1;
            }
            while (marking.ann_wal < ann.wal.records.items.len) : (marking.ann_wal += 1) {
                if (markBudgetExpired(started, budget_ns, limit, progress)) return;
                progress.rows += 1;
                const record = ann.wal.records.items[marking.ann_wal];
                if (record.kind == .reference or record.kind == .tombstone) try markAnnKey(marking, record.key, progress);
            }
        }
        if (allow_verification and marking.live.count() == marking.retained_at_cut) {
            if (marking.verification == null) marking.verification = marking.live.iterator();
            while (true) {
                if (markBudgetExpired(started, budget_ns, limit, progress)) return;
                const item = marking.verification.?.next() orelse break;
                progress.rows += 1;
                const found = try marking.source.get(&item.key_ptr.*, std.math.maxInt(u64), 1);
                if (found != .vector or found.vector.dims != item.value_ptr.*) return error.MissingCommittedVectorPayload;
                marking.verified_bytes += @as(u64, item.value_ptr.*) * 4;
            }
            marking.verification_done = true;
        }
        if (marking.segment_stats) |stats| {
            if (!marking.verification_done or marking.planning_required) while (marking.planning_reader < marking.source.readers.len) {
                const reader = marking.source.readers[marking.planning_reader];
                while (marking.planning_row < reader.count) : (marking.planning_row += 1) {
                    if (markBudgetExpired(started, budget_ns, limit, progress)) return;
                    progress.rows += 1;
                    try stats[marking.planning_reader].add(reader, marking.planning_row, &marking.live);
                }
                marking.planning_row = 0;
                marking.planning_reader += 1;
            };
        }
        marking.scan_done = true;
    }

    // Enter and return with the source mutex held, including on scan failure.
    // A running mark owns its snapshots until this function rejoins the writer.
    fn scanMarkingLocked(self: *Store, outside_lock: bool) !bool {
        const marking = self.marking orelse return false;
        if (marking.running) {
            self.stats.collection_mark_busy_deferrals += 1;
            return false;
        }
        if (marking.scan_done) return true;
        marking.running = true;
        const limit = if (self.mark_step_rows == 0) std.math.maxInt(usize) else self.mark_step_rows;
        const budget_ns = self.mark_step_ns;
        const allow_verification = marking.tail.count() == 0;
        if (outside_lock) self.mutex.unlock();
        const started = time.monotonicNs();
        if (comptime @import("builtin").is_test) {
            if (self.mark_test_hook) |hook| hook(self);
        }
        var progress: ScanProgress = .{};
        const result = scanMarking(marking, started, budget_ns, limit, allow_verification, &progress);
        const elapsed = time.monotonicNs() -| started;
        if (outside_lock) self.lock();
        marking.running = false;
        self.last_scan_ns = elapsed;
        self.stats.collection_mark_ns += elapsed;
        self.stats.collection_mark_steps += 1;
        self.stats.collection_mark_rows += progress.rows;
        self.stats.collection_mark_max_step_rows = @max(self.stats.collection_mark_max_step_rows, progress.rows);
        self.stats.collection_mark_max_step_ns = @max(self.stats.collection_mark_max_step_ns, elapsed);
        self.stats.ownership_index_entries_scanned += progress.owners;
        self.stats.retired_ann_references_skipped += progress.retired;
        if (progress.budget_yield) self.stats.collection_mark_budget_yields += 1;
        if (outside_lock) self.stats.collection_mark_outside_lock_ns += elapsed;
        if (marking.cancel_requested) {
            marking.deinit(self.alloc);
            self.marking = null;
            self.stats.collection_deferrals += 1;
            try result;
            return false;
        }
        errdefer {
            marking.deinit(self.alloc);
            self.marking = null;
        }
        try result;
        if (self.poisoned) return error.VectorPayloadStorePoisoned;
        const merge_started = time.monotonicNs();
        try self.mergeRescuedLocked(marking);
        for (marking.discovered.items) |digest| _ = marking.tail.remove(digest);
        marking.discovered.clearRetainingCapacity();
        for (marking.pending_tail.items) |digest| {
            if (marking.live.contains(digest)) _ = marking.tail.remove(digest);
        }
        marking.pending_tail.clearRetainingCapacity();
        // A tail consisting only of retries may become empty after this
        // merge. Verify on a subsequent turn before using the all-live fast
        // path; genuine post-cut additions make that verification unnecessary.
        if (marking.scan_done and !marking.verification_done and marking.tail.count() == 0 and
            marking.live.count() == self.stats.retained_payloads)
        {
            marking.scan_done = false;
        }
        const merge_ns = time.monotonicNs() -| merge_started;
        self.stats.collection_mark_merge_ns += merge_ns;
        self.stats.collection_mark_max_merge_ns = @max(self.stats.collection_mark_max_merge_ns, merge_ns);
        return marking.scan_done;
    }

    fn mergeRescuedLocked(self: *Store, marking: *Marking) !void {
        std.debug.assert(!marking.running);
        if (marking.rescued.count() == 0) return;
        var rescued = marking.rescued.iterator();
        while (rescued.next()) |item| {
            if (!marking.live.contains(item.key_ptr.*)) {
                // Only a newly protected owner changes reachability. Retrying
                // an already-live digest must not rewind verification forever.
                // End the iterator before a possible live-map reallocation.
                marking.verification = null;
                marking.verified_bytes = 0;
                marking.verification_done = false;
                marking.scan_done = false;
                // A retry can resurrect a row already classified as garbage.
                // Recompute summaries before selecting any retirement; the
                // writer queue never mutates scanner-owned state in flight.
                if (marking.segment_stats) |stats| @memset(stats, .{});
                marking.planning_reader = 0;
                marking.planning_row = 0;
                try marking.live.put(item.key_ptr.*, item.value_ptr.*);
                marking.rescued_any = true;
            }
            _ = marking.tail.remove(item.key_ptr.*);
        }
        self.stats.collection_rescued_payloads += marking.rescued.count();
        marking.rescued.clearRetainingCapacity();
    }

    fn scanPauseNs(elapsed_ns: u64, duty_percent: u8) u64 {
        std.debug.assert(duty_percent > 0 and duty_percent <= 100);
        // Wall-time duty bound, not a hard CPU quota. Even a 100% setting yields
        // briefly so shutdown and other work are not starved by tiny scans.
        return @max(100 * std.time.ns_per_us, (elapsed_ns / duty_percent) *| (100 - duty_percent));
    }

    pub fn activeScanPauseNs(self: *Store) ?u64 {
        self.lock();
        defer self.mutex.unlock();
        if (self.scan_duty_percent == 0 or !self.mark_outside_lock or self.poisoned or
            self.stats.unresolved_primary_commits != 0) return null;
        const marking = self.marking orelse return null;
        if (marking.running or marking.scan_done) return null;
        const pause = scanPauseNs(self.last_scan_ns, self.scan_duty_percent);
        self.stats.collection_active_scan_turns += 1;
        self.stats.collection_active_scan_pause_ns += pause;
        return pause;
    }

    /// An incomplete immutable scan has no DB apply transition to perform.
    /// Repair metadata can still request its own maintenance pass separately.
    pub fn continueScanWithoutApply(self: *Store) bool {
        self.lock();
        defer self.mutex.unlock();
        if (!self.independent_scan or !self.mark_outside_lock or self.poisoned or self.stats.unresolved_primary_commits != 0) return false;
        const marking = self.marking orelse return false;
        if (marking.scan_done) return false;
        self.stats.collection_apply_visits_avoided += 1;
        return true;
    }

    /// No primary publication or catalog access: safe before taking DB.apply.
    /// Only immutable leases and scanner-private state are accessed unlocked.
    pub fn advanceMarkingSnapshot(self: *Store) !void {
        if (!self.mark_outside_lock) return;
        self.lock();
        defer self.mutex.unlock();
        if (self.poisoned) return error.VectorPayloadStorePoisoned;
        if (self.stats.unresolved_primary_commits != 0) return;
        _ = try self.scanMarkingLocked(true);
    }

    fn advanceMarkingLocked(self: *Store, budget_bytes: u64) !bool {
        if (self.marking.?.running) return false;
        try self.mergeRescuedLocked(self.marking.?);
        if (self.mark_outside_lock) {
            if (!self.marking.?.scan_done) return false;
        } else if (!try self.scanMarkingLocked(false)) return false;
        return self.finishMarkingLocked(budget_bytes);
    }

    pub fn collectionStepBytes() u64 {
        const raw = if (@import("builtin").link_libc) std.c.getenv("ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES") else null;
        const value = if (raw) |text| std.mem.span(text) else return 0;
        return std.fmt.parseInt(u64, value, 10) catch 0;
    }

    /// Stop borrowing the primary backend before its owner closes it.
    /// Marking publishes nothing; all post-cut preparations remain in the WAL.
    pub fn cancelMarking(self: *Store) void {
        while (true) {
            self.lock();
            if (self.marking) |marking| {
                if (marking.running) {
                    self.mutex.unlock();
                    std.Thread.yield() catch {};
                    continue;
                }
                marking.deinit(self.alloc);
            }
            self.marking = null;
            self.mutex.unlock();
            return;
        }
    }

    pub fn recordBatchLockWait(self: *Store, elapsed_ns: u64) void {
        self.lock();
        defer self.mutex.unlock();
        self.stats.outer_db_batch_lock_wait_ns += elapsed_ns;
    }

    pub fn collectionPending(self: *Store) bool {
        self.lock();
        defer self.mutex.unlock();
        return self.collection != null or self.marking != null;
    }

    pub fn currentGeneration(self: *Store) u64 {
        self.lock();
        defer self.mutex.unlock();
        return self.opened.store.manifest.?.latest_generation;
    }

    fn initializeInventory(self: *Store, defer_from_receipt: bool) !void {
        // loadCheckpointReceipt already authenticated the exact durable source
        // identity and restored its physical inventory totals. Readers and
        // unchanged writers need no occurrence map. installOpened builds one
        // from the new durable view before segment installation uses it.
        std.debug.assert(!self.inventory.initialized);
        if (defer_from_receipt and self.receipt != null) return;
        const started = time.monotonicNs();
        defer self.stats.inventory_update_ns += time.monotonicNs() -| started;
        try self.inventory.sync(self.alloc, null, &self.opened, &self.stats.inventory_rows_scanned);
        self.stats.inventory_updates += 1;
        self.stats.retained_payloads = self.inventory.counts.count();
        self.stats.retained_payload_bytes = self.inventory.bytes;
    }

    fn configureDirectory(self: *Store) !void {
        self.preparation_alloc = self.alloc;
        self.coalesce_directory = experimentEnabled("ANTFLY_SOURCE_VECTOR_COALESCE_DIRECTORY");
        self.mark_outside_lock = experimentEnabled("ANTFLY_SOURCE_VECTOR_MARK_OUTSIDE_LOCK");
        self.rescue_reappends = experimentEnabled("ANTFLY_SOURCE_VECTOR_RESCUE_REAPPENDS");
        self.shared_catalog = experimentEnabled("ANTFLY_SOURCE_VECTOR_SHARED_CATALOG");
        self.independent_scan = experimentEnabled("ANTFLY_SOURCE_VECTOR_INDEPENDENT_SCAN");
        self.incremental_inventory = experimentEnabled("ANTFLY_SOURCE_VECTOR_INCREMENTAL_INVENTORY");
        self.delta_inventory = experimentEnabled("ANTFLY_SOURCE_VECTOR_DELTA_INVENTORY");
        self.inventory.delta = self.delta_inventory;
        self.bitmap_marking = experimentEnabled("ANTFLY_SOURCE_VECTOR_BITMAP_MARKING");
        self.bitmap_locator = experimentEnabled("ANTFLY_SOURCE_VECTOR_BITMAP_LOCATOR");
        self.incremental_planning = experimentEnabled("ANTFLY_SOURCE_VECTOR_INCREMENTAL_PLANNING");
        if (@import("builtin").link_libc) {
            if (std.c.getenv("ANTFLY_SOURCE_VECTOR_SPARSE_GC_COPY_BYTES")) |raw|
                self.sparse_gc_copy_bytes = try std.fmt.parseInt(u64, std.mem.span(raw), 10);
        }
        self.inventory_requested = self.incremental_inventory;
        if (@import("builtin").link_libc) {
            if (std.c.getenv("ANTFLY_SOURCE_VECTOR_INVENTORY_MIN_PAYLOADS")) |raw|
                self.inventory_min_payloads = try std.fmt.parseInt(u64, std.mem.span(raw), 10);
        }
        if (self.inventory_min_payloads != 0 and self.inventory_requested and self.receipt == null) try self.inventoryRetainedPayloads();
        if (self.stats.retained_payloads < self.inventory_min_payloads) self.incremental_inventory = false;
        self.debt_scheduling = experimentEnabled("ANTFLY_SOURCE_VECTOR_DEBT_SCHEDULING") and payload.ownershipEnabled();
        if (self.shared_catalog) try self.opened.shareSegmentCatalog();
        if (self.incremental_inventory) try self.initializeInventory(experimentEnabled("ANTFLY_SOURCE_VECTOR_LAZY_INVENTORY"));
        // Existing tables always verify on their first maintenance turn.
        if (self.stats.retained_payloads == 0) self.last_mark_completed_ns = time.monotonicNs();
        if (@import("builtin").link_libc) {
            if (std.c.getenv("ANTFLY_SOURCE_VECTOR_SCAN_DUTY_PERCENT")) |raw| {
                const duty = std.fmt.parseInt(u8, std.mem.span(raw), 10) catch 0;
                if (duty <= 100) self.scan_duty_percent = duty;
            }
        }
        if (@import("builtin").link_libc) {
            if (std.c.getenv("ANTFLY_SOURCE_VECTOR_MARK_STEP_ROWS")) |raw|
                self.mark_step_rows = try std.fmt.parseInt(usize, std.mem.span(raw), 10);
            if (std.c.getenv("ANTFLY_SOURCE_VECTOR_MARK_STEP_US")) |raw|
                self.mark_step_ns = try std.math.mul(u64, try std.fmt.parseInt(u64, std.mem.span(raw), 10), std.time.ns_per_us);
        }
        self.group_commit = experimentEnabled("ANTFLY_SOURCE_VECTOR_GROUP_COMMIT");
        self.append_only = experimentEnabled("ANTFLY_SOURCE_VECTOR_APPEND_ONLY");
        self.selective_gc = self.append_only and experimentEnabled("ANTFLY_SOURCE_VECTOR_SELECTIVE_GC");
        if (!self.append_only) return;
        const directory = try @import("source_location_directory.zig").Directory.create(self.alloc);
        self.directory = directory;
        directory.load(self.opened.store.storage, self.opened.store.root_dir) catch {};
        try directory.update(&self.opened, directory.generation > self.opened.store.manifest.?.latest_generation);
        self.opened.source_directory = directory;
    }

    fn refreshDirectory(self: *Store, rebuild: bool, persist: bool) void {
        const directory = self.directory orelse return;
        self.opened.source_directory = directory;
        directory.update(&self.opened, rebuild) catch return;
        if (persist and !self.read_only) {
            if (self.coalesce_directory) {
                directory.saveCoalesced(self.opened.store.storage, self.opened.store.root_dir) catch {};
            } else directory.save(self.opened.store.storage, self.opened.store.root_dir) catch {};
        }
    }

    fn configureLocationCache(self: *Store) !void {
        const raw = if (@import("builtin").link_libc) std.c.getenv("ANTFLY_SOURCE_VECTOR_LOCATION_CACHE_ENTRIES") else null;
        const text = if (raw) |value| std.mem.span(value) else return;
        const count = std.fmt.parseInt(usize, text, 10) catch return error.InvalidArgument;
        if (count != 0) self.location_cache = try native.ReferenceLocationCache.createWithPolicy(self.alloc, count, experimentEnabled("ANTFLY_SOURCE_VECTOR_ADAPTIVE_CACHE"));
    }

    pub fn openManaged(alloc: Allocator, manager: ?*resources.ResourceManager, storage: lsm.Storage, root: []const u8, read_only: bool) !Store {
        const resource_manager = manager orelse return openWithEncoding(alloc, storage, root, read_only, preferredEncoding());
        const budget = try alloc.create(resources.BudgetedAllocator);
        errdefer alloc.destroy(budget);
        budget.* = resources.BudgetedAllocator.initReclaiming(resource_manager, .dense_source_payload_state, alloc, 1);
        errdefer budget.deinit();
        var store = try openWithEncoding(budget.threadSafeAllocator(), storage, root, read_only, preferredEncoding());
        errdefer store.deinit();
        if (store.location_cache) |cache| try cache.attachManager(alloc, resource_manager);
        store.preparation_alloc = alloc;
        store.preparation_manager = resource_manager;
        store.budget = budget;
        const limit = resource_manager.sliceStats(.dense_source_payload_state).hard_limit_bytes;
        if (limit != 0) store.wal_admission_bytes = @min(store.wal_admission_bytes, @max(256 * 1024, limit / 8));
        return store;
    }

    fn preferredEncoding() vector_block.Encoding {
        const raw = if (@import("builtin").link_libc) std.c.getenv("ANTFLY_HBC_VECTOR_BLOCK_ENCODING") else null;
        const value = if (raw) |z| std.mem.span(z) else return .float16;
        return if (std.ascii.eqlIgnoreCase(value, "float16") or std.ascii.eqlIgnoreCase(value, "f16")) .float16 else .float32;
    }

    pub fn open(alloc: Allocator, storage: lsm.Storage, root: []const u8, read_only: bool) !Store {
        return openWithEncoding(alloc, storage, root, read_only, .float32);
    }

    fn openWithEncoding(alloc: Allocator, storage: lsm.Storage, root: []const u8, read_only: bool, encoding: vector_block.Encoding) !Store {
        const sizing = try SegmentSizing.fromEnvironment();
        const snapshot_reads = experimentEnabled("ANTFLY_SOURCE_VECTOR_SNAPSHOT_READS");
        if (read_only) {
            var result: Store = .{ .alloc = alloc, .opened = try native.Store.openReadOnlyWithBlocks(alloc, storage, root), .read_only = true, .segment_sizing = sizing, .snapshot_reads = snapshot_reads, .checkpoint_receipts = collectionStepBytes() != 0 };
            errdefer result.deinit();
            if (!try result.loadCheckpointReceipt() and !experimentEnabled("ANTFLY_SOURCE_VECTOR_INCREMENTAL_INVENTORY")) try result.inventoryRetainedPayloads();
            try result.configureLocationCache();
            try result.configureDirectory();
            return result;
        }
        var writer = try native.Store.open(alloc, storage, root);
        defer writer.deinit();
        if (writer.manifest == null) {
            if (read_only) return error.MissingVectorPayloadStore;
            try writer.publishEmptyBase(1, 0, .{ .shard_count = if (sizing.target_bytes == 0) 128 else sizing.min_shards, .encoding = encoding });
        }
        var result: Store = .{ .alloc = alloc, .opened = try native.Store.openWithBlocks(alloc, storage, root), .read_only = read_only, .segment_sizing = sizing, .snapshot_reads = snapshot_reads, .checkpoint_receipts = collectionStepBytes() != 0 };
        errdefer result.deinit();
        if (!try result.loadCheckpointReceipt() and !experimentEnabled("ANTFLY_SOURCE_VECTOR_INCREMENTAL_INVENTORY")) try result.inventoryRetainedPayloads();
        // This is the table's writable startup owner, before any builder can
        // reserve a generation. A clean liveness receipt must not hide files
        // left by a crash during an unpublished incremental copy.
        _ = try result.opened.store.reclaimUnreferencedFiles();
        try result.reclaimStartupTemporaryFiles();
        try result.configureLocationCache();
        try result.configureDirectory();
        return result;
    }

    fn reclaimStartupTemporaryFiles(self: *Store) !void {
        const names = try self.opened.store.storage.listFileNamesAlloc(self.alloc, self.opened.store.root_dir);
        defer lsm.Storage.freeFileNames(self.alloc, names);
        for (names) |name| {
            if (!native.isTemporaryArtifactName(name)) continue;
            const path = try std.fs.path.join(self.alloc, &.{ self.opened.store.root_dir, name });
            defer self.alloc.free(path);
            self.opened.store.storage.deleteFileAbsolute(path) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
        }
    }

    const InventoryTotals = struct { count: u64, bytes: u64 };

    fn inventoryRetainedPayloads(self: *Store) !void {
        var unique = std.AutoHashMap(payload.Digest, u32).init(self.alloc);
        defer unique.deinit();
        const totals = try self.inventoryInto(&self.opened, &unique);
        self.stats.retained_payloads = totals.count;
        self.stats.retained_payload_bytes = totals.bytes;
    }

    fn inventoryInto(self: *Store, opened: *const native.Opened, unique: anytype) !InventoryTotals {
        const started = time.monotonicNs();
        defer self.stats.inventory_update_ns += time.monotonicNs() -| started;
        self.stats.inventory_updates += 1;
        const FullInventory = struct {
            fn put(map: @TypeOf(unique), key: []const u8, dims: u32) !void {
                if (key.len != 32) return error.InvalidVectorReference;
                try map.put(key[0..32].*, dims);
            }
            fn tree(map: @TypeOf(unique), node: ?*@import("vector_wal_view.zig").Node) anyerror!void {
                if (node) |n| {
                    try tree(map, n.left);
                    if (n.record.kind == .upsert) try put(map, n.record.key, n.record.dims);
                    try tree(map, n.right);
                }
            }
        };
        unique.clearRetainingCapacity();
        if (@TypeOf(unique) == *LiveSet) {
            if (self.bitmap_marking) try unique.enableBitmapsMode(opened, self.bitmap_locator);
        }
        // Inventory counts identities. Reader admission has already validated
        // the immutable index and every key; payload checksums are verified
        // at the read, verification, and copy boundaries that touch payload
        // bytes, so this scan must not fault in or hash the vector planes.
        for (opened.readers) |reader| for (0..reader.count) |i| {
            const entry = reader.sourceIdentityAt(i);
            self.stats.inventory_rows_scanned += 1;
            if (entry.vector) try FullInventory.put(unique, entry.key, entry.dims);
        };
        for (opened.wal.records.items) |record| {
            if (record.kind == .upsert) try FullInventory.put(unique, record.key, record.dims);
        }
        try FullInventory.tree(unique, opened.wal_tree);
        var bytes: u64 = 0;
        var it = unique.valueIterator();
        while (it.next()) |dims| bytes += @as(u64, dims.*) * 4;
        return .{ .count = unique.count(), .bytes = bytes };
    }

    pub fn deinit(self: *Store) void {
        std.debug.assert(self.stats.active_sessions == 0);
        if (self.marking) |marking| marking.deinit(self.alloc);
        if (self.collection) |collection| collection.deinit(self.alloc);
        self.inventory.deinit(self.alloc);
        self.opened.deinit();
        if (self.directory) |directory| directory.deinit();
        if (self.location_cache) |cache| cache.deinit();
        if (self.ann_reference_root) |root| self.alloc.free(root);
        if (self.ann_scopes) |scopes| self.alloc.free(scopes);
        if (self.budget) |budget| {
            const backing = budget.backing;
            budget.deinit();
            backing.destroy(budget);
        }
    }

    /// Immutable ANN leases share native blocks and persistent WAL nodes.
    /// Queries never acquire the source writer mutex or reassemble envelopes.
    pub fn snapshot(self: *Store, alloc: Allocator) !native.Opened {
        self.lock();
        defer self.mutex.unlock();
        if (self.poisoned) return error.VectorPayloadStorePoisoned;
        var lease = try self.opened.clone(alloc);
        self.recordCatalogSuccessor();
        lease.reference_location_cache = self.location_cache;
        return lease;
    }

    fn recordCatalogSuccessor(self: *Store) void {
        const bytes = self.opened.catalogMetadataBytes();
        if (self.opened.shared_catalog != null) {
            self.stats.catalog_metadata_bytes_shared += bytes;
        } else self.stats.catalog_metadata_bytes_copied += bytes;
    }

    pub fn interface(self: *Store) payload.Store {
        const normal: payload.Store.VTable = .{ .retain = retain, .release = release, .prepare = prepare, .resolve = resolve, .unresolved_commit = unresolvedCommit };
        const scheduled: payload.Store.VTable = .{ .retain = retain, .release = release, .prepare = prepare, .resolve = resolve, .unresolved_commit = unresolvedCommit, .retired_payloads = retiredPayloads };
        return .{ .ptr = self, .vtable = if (self.debt_scheduling) &scheduled else &normal };
    }

    fn cast(ptr: *anyopaque) *Store {
        return @ptrCast(@alignCast(ptr));
    }
    fn lock(self: *Store) void {
        while (!self.mutex.tryLock()) std.Thread.yield() catch {};
    }
    fn retain(ptr: *anyopaque) void {
        const self = cast(ptr);
        self.lock();
        defer self.mutex.unlock();
        self.stats.active_sessions += 1;
    }
    fn release(ptr: *anyopaque) void {
        const self = cast(ptr);
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.stats.active_sessions > 0);
        self.stats.active_sessions -= 1;
    }

    fn retiredPayloads(ptr: *anyopaque, bytes: u64) void {
        const self = cast(ptr);
        self.lock();
        defer self.mutex.unlock();
        self.obsolete_debt +|= bytes;
    }

    fn unresolvedCommit(ptr: *anyopaque) void {
        const self = cast(ptr);
        self.lock();
        defer self.mutex.unlock();
        // Primary replay on reopen resolves the outcome. Until then, today's
        // visible primary tip is not proof that this payload is unreachable.
        self.stats.unresolved_primary_commits += 1;
    }

    fn prepare(ptr: *anyopaque, prepared: []const payload.Prepared) !void {
        const self = cast(ptr);
        _ = self.prepare_requests.fetchAdd(1, .monotonic);
        if (!self.group_commit) return self.prepareBatch(prepared);
        var request: PrepareRequest = .{ .prepared = prepared };
        self.queueLock();
        if (self.prepare_tail) |tail| tail.next = &request else self.prepare_head = &request;
        self.prepare_tail = &request;
        if (!self.prepare_running) {
            self.prepare_running = true;
            request.state.store(1, .release);
        }
        self.prepare_queue_mutex.unlock();
        while (true) {
            switch (request.state.load(.acquire)) {
                1 => {
                    // No timer weakens latency or durability. Arrivals during
                    // the prior durable append share the next bounded batch.
                    self.queueLock();
                    const first = self.prepare_head.?;
                    var last = first;
                    var count = first.prepared.len;
                    var requests: usize = 1;
                    while (last.next) |next| {
                        if (requests >= 32 or count + next.prepared.len > 256) break;
                        count += next.prepared.len;
                        requests += 1;
                        last = next;
                    }
                    self.prepare_head = last.next;
                    last.next = null;
                    if (self.prepare_head == null) self.prepare_tail = null;
                    self.prepare_queue_mutex.unlock();
                    const outcome: ?anyerror = batch: {
                        const items = self.preparation_alloc.alloc(payload.Prepared, count) catch |err| break :batch err;
                        defer self.preparation_alloc.free(items);
                        var pos: usize = 0;
                        var current: ?*PrepareRequest = first;
                        while (current) |part| : (current = part.next) {
                            @memcpy(items[pos..][0..part.prepared.len], part.prepared);
                            pos += part.prepared.len;
                        }
                        self.prepareBatch(items) catch |err| break :batch err;
                        break :batch null;
                    };
                    // Detach all waiter pointers before waking them; their
                    // stack frames may disappear immediately on notification.
                    var current: ?*PrepareRequest = first;
                    while (current) |part| {
                        const next = part.next;
                        part.err = outcome;
                        part.state.store(2, .release);
                        current = next;
                    }
                    self.queueLock();
                    if (self.prepare_head) |next| next.state.store(1, .release) else self.prepare_running = false;
                    self.prepare_queue_mutex.unlock();
                    if (outcome) |err| return err;
                    return;
                },
                2 => return if (request.err) |err| err else {},
                else => std.Thread.yield() catch {},
            }
        }
    }

    fn queueLock(self: *Store) void {
        while (!self.prepare_queue_mutex.tryLock()) std.Thread.yield() catch {};
    }

    /// Cancelling unpublished work retains every preparation in the WAL.
    /// An active scanner owns its cursor until it rejoins; callers must defer
    /// admission rather than free its state or wait while holding this lock.
    fn discardCollectionForPressureLocked(self: *Store) bool {
        if (self.marking) |marking| {
            if (marking.running) {
                marking.cancel_requested = true;
                return false;
            }
            marking.deinit(self.alloc);
            self.marking = null;
            self.stats.collection_deferrals += 1;
        }
        if (self.collection) |collection| {
            if (collection.publication_attempted) return false;
            collection.deinit(self.alloc);
            self.collection = null;
            self.stats.collection_deferrals += 1;
        }
        return true;
    }

    fn reservePreparationLocked(self: *Store, prepared: []const payload.Prepared) !?resources.BudgetedAllocator.ScratchReservation {
        const budget = self.budget orelse return null;
        // Decode/encode, immutable WAL successor and metadata coexist until
        // append succeeds. Maintenance needs a page plus directory scratch.
        var bytes: usize = 2 * 1024 * 1024;
        for (prepared) |item| bytes = try std.math.add(usize, bytes, try std.math.add(usize, try std.math.mul(usize, item.reference.dims, 12), 2048));
        return budget.reserveScratch(bytes) catch |err| {
            if (err != error.ResourceBudgetExceeded or !self.discardCollectionForPressureLocked()) return err;
            // A cancelled mark no longer prevents flushing its growing WAL.
            if (self.walNeedsAdmissionCheckpoint()) try self.checkpointLocked();
            return try budget.reserveScratch(bytes);
        };
    }

    fn walNeedsAdmissionCheckpoint(self: *const Store) bool {
        return self.opened.store.wal_has_mutations and self.opened.store.wal_committed_bytes >= self.wal_admission_bytes;
    }

    fn prepareBatch(self: *Store, prepared: []const payload.Prepared) !void {
        if (self.read_only) return error.ReadOnly;
        // Decode independent artifact envelopes before entering source writer
        // exclusion. Each request has its own allocator reservation.
        var local_budget: ?resources.BudgetedAllocator = if (self.group_commit and self.preparation_manager != null)
            resources.BudgetedAllocator.initReclaiming(self.preparation_manager.?, .dense_source_payload_state, self.preparation_alloc, 1)
        else
            null;
        defer if (local_budget) |*budget| budget.deinit();
        var decode_arena = std.heap.ArenaAllocator.init(if (local_budget) |*budget| budget.allocator() else self.preparation_alloc);
        defer decode_arena.deinit();
        const decode_started = time.monotonicNs();
        const decoded = if (self.group_commit) try decode_arena.allocator().alloc([]const f32, prepared.len) else null;
        if (decoded) |vectors| for (prepared, vectors) |item, *vector| {
            vector.* = (try codec.denseEmbeddingVectorView(item.artifact)) orelse try codec.decodeDenseEmbeddingAlloc(decode_arena.allocator(), item.artifact);
        };
        const lock_started = time.monotonicNs();
        self.lock();
        defer self.mutex.unlock();
        if (self.poisoned) return error.VectorPayloadStorePoisoned;
        const started = time.monotonicNs();
        self.stats.prepare_lock_wait_ns += started -| lock_started;
        if (self.group_commit) self.stats.decode_outside_lock_ns += lock_started -| decode_started;
        self.stats.prepare_batches += 1;
        defer self.stats.preparation_ns += time.monotonicNs() -| started;
        // The checkpoint threshold is a bound during marking as well. A mark
        // is retryable from a newer cut; an ever-growing resident WAL is not.
        if (self.walNeedsAdmissionCheckpoint() and (self.marking != null or self.collection != null)) {
            if (!self.discardCollectionForPressureLocked()) {
                // The scanner rejoins without DB.apply. Give it one bounded
                // suffix window to cancel; do not wait under a primary write
                // transaction or reject ordinary overlap at the soft bound.
                const hard_wal_limit = self.wal_admission_bytes + @max(16 * 1024, self.wal_admission_bytes / 4);
                if (self.opened.store.wal_committed_bytes >= hard_wal_limit) return error.ResourceBudgetExceeded;
            }
        }
        const scratch_reservation = try self.reservePreparationLocked(prepared);
        defer if (scratch_reservation) |reservation| reservation.release();
        if (self.walNeedsAdmissionCheckpoint()) try self.checkpointLocked();
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var records = std.ArrayListUnmanaged(native.BatchRecord).empty;
        var seen = std.AutoHashMap(payload.Digest, void).init(scratch);
        const sequence = try std.math.add(u64, self.opened.store.covered_source_sequence, 1);
        var bytes: u64 = 0;
        var added_payloads: u64 = 0;
        var added_bytes: u64 = 0;
        if (self.collection) |collection| try collection.tail.ensureUnusedCapacity(@intCast(prepared.len));
        if (self.marking) |marking| {
            try marking.tail.ensureUnusedCapacity(@intCast(prepared.len));
            if (self.rescue_reappends) try marking.rescued.ensureUnusedCapacity(self.alloc, @intCast(prepared.len));
            if (marking.running) try marking.pending_tail.ensureUnusedCapacity(self.alloc, prepared.len);
        }
        for (prepared, 0..) |*item, item_index| {
            // A retry can reuse an already durable immutable payload.
            const found = try self.opened.get(&item.reference.digest, std.math.maxInt(u64), null);
            if (found == .vector) {
                // A retry can select an orphan omitted from the initial mark.
                // Re-append it once after the cut so base+WAL retains it.
                if (self.collection) |collection| {
                    if (collection.live.contains(item.reference.digest) or collection.tail.contains(item.reference.digest)) continue;
                } else if (self.marking) |marking| {
                    if (marking.tail.contains(item.reference.digest) or (!marking.running and marking.live.contains(item.reference.digest))) continue;
                    if (self.rescue_reappends) {
                        if (marking.rescued.contains(item.reference.digest)) continue;
                        if (found.vector.dims != item.reference.dims) return error.VectorReferenceIdentityMismatch;
                        marking.rescued.putAssumeCapacity(item.reference.digest, item.reference.dims);
                        self.stats.deduplicated_reappend_payloads += 1;
                        self.stats.deduplicated_reappend_bytes += @as(u64, item.reference.dims) * 4;
                        continue;
                    }
                } else continue;
            }
            if ((try seen.getOrPut(item.reference.digest)).found_existing) continue;
            const vector = if (decoded) |vectors| vectors[item_index] else (try codec.denseEmbeddingVectorView(item.artifact)) orelse try codec.decodeDenseEmbeddingAlloc(scratch, item.artifact);
            try records.append(scratch, .{
                .kind = .upsert,
                .key = &item.reference.digest,
                .source_sequence = sequence,
                .revision = 1,
                .vector = vector,
            });
            bytes += vector.len * 4;
            if (found != .vector) {
                added_payloads += 1;
                added_bytes += vector.len * 4;
            }
        }
        if (records.items.len == 0) return;
        var encoded = try self.opened.store.encodeBatch(try self.opened.store.nextBatchId(), records.items, sequence);
        defer encoded.deinit();
        var successor = try self.opened.prepareWalSuccessor(self.alloc, &encoded, true);
        errdefer successor.deinit();
        self.recordCatalogSuccessor();
        // Update this rebuildable cache before durability too. If admission or
        // publication fails, discard it and rebuild from the authoritative view.
        errdefer if (self.delta_inventory) self.inventory.deinit(self.alloc);
        if (self.incremental_inventory and self.delta_inventory and self.inventory.initialized) {
            const inventory_started = time.monotonicNs();
            defer self.stats.inventory_update_ns += time.monotonicNs() -| inventory_started;
            for (records.items) |record| try self.inventory.addWal(self.alloc, record.key, @intCast(record.vector.len), encoded.last_committed_batch.?);
        }
        // Prepare all reader allocations before the durable append. This sync
        // establishes payload durability before ANY subsequent primary commit
        // or asynchronous primary checkpoint can persist its reference.
        const append_started = time.monotonicNs();
        self.opened.store.appendEncodedBatch(&encoded, records.items, .{ .sync = true }) catch |err| {
            self.poisoned = self.opened.store.poisoned;
            return err;
        };
        self.stats.durable_append_ns += time.monotonicNs() -| append_started;
        self.opened.deinit();
        self.opened = successor;
        if (self.collection) |collection| {
            for (records.items) |record| collection.tail.putAssumeCapacity(record.key[0..32].*, @intCast(record.vector.len));
        }
        if (self.marking) |marking| {
            for (records.items) |record| {
                marking.tail.putAssumeCapacity(record.key[0..32].*, @intCast(record.vector.len));
                if (marking.running) marking.pending_tail.appendAssumeCapacity(record.key[0..32].*);
            }
        }
        self.stats.retained_payloads += added_payloads;
        self.stats.retained_payload_bytes += added_bytes;
        self.stats.prepared_payloads += records.items.len;
        self.stats.prepared_payload_bytes += bytes;
        self.stats.wal_bytes_written += encoded.bytes().len;
    }

    fn resolve(ptr: *anyopaque, alloc: Allocator, key: []const u8, reference: payload.Reference) ![]u8 {
        const self = cast(ptr);
        if (self.snapshot_reads) {
            const started = time.monotonicNs();
            // Acquire after selecting the primary value: a lease taken before
            // the primary snapshot could miss a concurrently committed ref.
            // The result allocator may be a long-lived transaction arena;
            // transient lease metadata must be freed after this one read.
            var lease_budget: ?resources.BudgetedAllocator = if (self.preparation_manager) |manager|
                resources.BudgetedAllocator.initReclaiming(manager, .dense_source_payload_state, self.preparation_alloc, 1)
            else
                null;
            defer if (lease_budget) |*budget| budget.deinit();
            var lease = try self.snapshot(if (lease_budget) |*budget| budget.allocator() else self.preparation_alloc);
            defer lease.deinit();
            const result = try resolveFrom(&lease, alloc, key, reference);
            self.lock();
            self.stats.snapshot_read_ns += time.monotonicNs() -| started;
            self.stats.resolved_payloads += 1;
            self.stats.resolved_bytes += @as(u64, reference.dims) * 4;
            self.mutex.unlock();
            return result;
        }
        self.lock();
        defer self.mutex.unlock();
        if (self.poisoned) return error.VectorPayloadStorePoisoned;
        const result = try resolveFrom(&self.opened, alloc, key, reference);
        self.stats.resolved_payloads += 1;
        self.stats.resolved_bytes += @as(u64, reference.dims) * 4;
        return result;
    }

    fn resolveFrom(opened: *const native.Opened, alloc: Allocator, key: []const u8, reference: payload.Reference) ![]u8 {
        const found = try opened.get(&reference.digest, std.math.maxInt(u64), 1);
        if (found != .vector) return error.MissingCommittedVectorPayload;
        const value = found.vector;
        if (value.dims != reference.dims) return error.VectorReferenceIdentityMismatch;
        const result = try alloc.alloc(u8, codec.header_len + 4 + @as(usize, reference.dims) * 4);
        errdefer alloc.free(result);
        @memcpy(result[0..codec.header_len], &reference.header);
        std.mem.writeInt(u32, result[codec.header_len..][0..4], reference.dims, .little);
        const bytes = result[codec.header_len + 4 ..];
        if (value.encoding == .float32 and value.scale == 1) {
            @memcpy(bytes, value.bytes);
        } else {
            const scratch = try alloc.alloc(f32, reference.dims);
            defer alloc.free(scratch);
            const exact = try value.decodeExactInto(scratch);
            for (exact, 0..) |component, i| std.mem.writeInt(u32, bytes[i * 4 ..][0..4], @bitCast(component), .little);
        }
        const actual = try payload.Reference.forArtifact(key, result);
        if (!std.mem.eql(u8, &actual.digest, &reference.digest)) return error.VectorReferenceIdentityMismatch;
        return result;
    }

    pub fn checkpoint(self: *Store) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.collection != null or self.marking != null) return;
        try self.checkpointLocked();
        // Initial ingest deliberately accumulates disjoint runs. Once ANN
        // publishes a stable tip, establish the source base as well so every
        // serving lookup does not search the whole bootstrap delta chain.
        // Ordinary preparation keeps the append path, and established bases
        // retain native bounded delta maintenance instead of being rewritten
        // at each enrichment completion.
        if (self.append_only) {
            self.refreshDirectory(false, true);
            return;
        }
        if (self.opened.readers.len == 0) return;
        var input_bytes: u64 = 0;
        for (self.opened.blocks) |block| input_bytes += block.bytes().len;
        const current_shards = self.opened.store.manifest.?.shard_count;
        const target_shards = self.segment_sizing.shardCount(input_bytes, current_shards);
        const bootstrap = self.opened.baseVectorCount() == 0;
        // Grow geometrically at stable publication boundaries. Shrinking an
        // established source belongs to collection, not every small update.
        if (!bootstrap and target_shards <= current_shards) return;
        const started = time.monotonicNs();
        defer self.stats.checkpoint_ns += time.monotonicNs() -| started;
        const prior_generation = self.opened.store.manifest.?.latest_generation;
        errdefer if (self.opened.store.poisoned or self.opened.store.manifest.?.latest_generation != prior_generation) {
            self.poisoned = true;
        };
        if (!try self.opened.compactDeltasToBaseWithShardCount(target_shards, 1024 * 1024)) return;
        var next = try native.Store.openWithBlocksReusing(self.alloc, self.opened.store.storage, self.opened.store.root_dir, &self.opened);
        self.installOpened(&next) catch |err| {
            next.deinit();
            return err;
        };
        self.refreshDirectory(false, false);
        self.stats.checkpoint_bytes_read += input_bytes;
        for (self.opened.blocks) |block| self.stats.checkpoint_bytes_written += block.bytes().len;
    }

    fn checkpointLocked(self: *Store) !void {
        if (self.read_only) return error.ReadOnly;
        if (self.poisoned) return error.VectorPayloadStorePoisoned;
        if (self.marking != null) {
            if (self.stats.unresolved_primary_commits != 0) return;
            _ = try self.advanceMarkingLocked(@max(1024 * 1024, collectionStepBytes()));
            if (self.marking != null) return;
        }
        if (self.collection != null) {
            if (self.stats.unresolved_primary_commits != 0) return;
            // Writers help finish a reserved generation under WAL pressure,
            // bounding how long ordinary checkpointing can be deferred.
            _ = try self.advanceCollectionLocked(@max(1024 * 1024, collectionStepBytes()));
            if (self.collection != null) return;
        }
        const started = time.monotonicNs();
        defer self.stats.checkpoint_ns += time.monotonicNs() -| started;
        const prior_generation = self.opened.store.manifest.?.latest_generation;
        errdefer if (self.opened.store.poisoned or self.opened.store.manifest.?.latest_generation != prior_generation) {
            self.poisoned = true;
        };
        const input_bytes = self.opened.store.wal_committed_bytes;
        if (!try self.opened.checkpointWalToDeltaWithPolicy(true, self.append_only)) return;
        self.stats.checkpoint_bytes_read += input_bytes;
        var next = try native.Store.openWithBlocksReusing(self.alloc, self.opened.store.storage, self.opened.store.root_dir, &self.opened);
        self.installOpened(&next) catch |err| {
            next.deinit();
            return err;
        };
        self.refreshDirectory(false, false);
        for (self.opened.readers, self.opened.blocks) |reader, block| {
            if (reader.generation > prior_generation) self.stats.checkpoint_bytes_written += block.bytes().len;
        }
    }

    pub fn statsSnapshot(self: *Store) Stats {
        self.lock();
        defer self.mutex.unlock();
        return self.statsLocked();
    }

    pub fn tryStatsSnapshot(self: *Store) ?Stats {
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        return self.statsLocked();
    }

    fn statsLocked(self: *Store) Stats {
        var stats = self.stats;
        stats.prepare_requests = self.prepare_requests.load(.monotonic);
        stats.source_segments = self.opened.readers.len;
        stats.inventory_incremental_active = @intFromBool(self.incremental_inventory);
        if (self.marking) |marking| {
            stats.mark_bitmap_bytes = marking.live.bitmapBytes();
            stats.mark_fallback_entries = marking.live.map.count();
        }
        if (self.collection) |collection| {
            stats.mark_bitmap_bytes = collection.live.bitmapBytes();
            stats.mark_fallback_entries = collection.live.map.count();
        }
        stats.obsolete_payload_debt_bytes = self.obsolete_debt;
        stats.inventory_wal_rows = self.inventory.wal_rows;
        stats.inventory_wal_retirements = self.inventory.wal_retirements;
        stats.inventory_delta_installs = self.inventory.delta_installs;
        stats.inventory_fallback_installs = self.inventory.fallback_installs;
        if (self.directory) |directory| {
            stats.directory_bytes_written = directory.bytes_written;
            stats.directory_publications = directory.publications;
            stats.directory_publication_deferrals = directory.publication_deferrals;
            stats.directory_entries = directory.entries.count();
            stats.directory_hits = directory.hits.load(.monotonic);
            stats.directory_misses = directory.misses.load(.monotonic);
        }
        if (self.location_cache) |cache| {
            stats.cache_reclaimed_bytes = cache.reclaimed_bytes.load(.monotonic);
            stats.location_cache_hits = cache.hits.load(.monotonic);
            stats.location_cache_misses = cache.misses.load(.monotonic);
            stats.location_cache_bytes = @sizeOf(native.ReferenceLocationCache) + cache.resident_bytes.load(.monotonic);
        }
        stats.source_shards = self.opened.store.manifest.?.shard_count;
        if (self.marking != null) stats.collection_pending_bytes = @max(1, stats.retained_payload_bytes);
        if (self.collection) |collection| {
            stats.collection_pending_bytes = collection.total_live_bytes -| collection.bytes_read;
        }
        if (self.budget) |budget| stats.heap_bytes = budget.liveBytesThreadSafe();
        if (self.location_cache) |cache| if (cache.adaptive and cache.manager != null) {
            stats.heap_bytes += cache.resident_bytes.load(.monotonic);
        };
        stats.active_wal_bytes = self.opened.store.wal_committed_bytes;
        for (self.opened.blocks) |block| stats.immutable_block_bytes += block.bytes().len;
        return stats;
    }

    /// Explicit quiescent collection for initial qualification. A session is
    /// retained BEFORE taking its primary snapshot, so zero sessions proves
    /// there is no old snapshot or prepared primary commit using this owner.
    /// Retained readers from other physical opens own their old native files.
    /// This conservative collector reports deferral instead of waiting for
    /// readers. Its serialized cost is included in experiment measurements.
    pub fn collectDeferredMark(self: *Store, primary: *erased.Store) !bool {
        const budget = collectionStepBytes();
        return self.collectStepDeferredMark(primary, if (budget == 0) std.math.maxInt(u64) else budget);
    }

    /// Scheduling never certifies liveness. Explicit collect/collectStep calls
    /// bypass this policy; background verification becomes eligible again
    /// after 30 seconds, subject to the normal reader and memory fences.
    pub fn collectBackgroundStepDeferredMark(self: *Store, primary: *erased.Store, budget_bytes: u64) !bool {
        self.lock();
        const defer_scan = self.shouldDeferMark(time.monotonicNs());
        if (defer_scan) self.stats.collection_debt_deferrals += 1;
        self.mutex.unlock();
        if (defer_scan) return false;
        return self.collectStepDeferredMark(primary, budget_bytes);
    }

    fn shouldDeferMark(self: *const Store, now: u64) bool {
        return self.debt_scheduling and !self.poisoned and self.marking == null and self.collection == null and
            self.stats.unresolved_primary_commits == 0 and self.last_mark_completed_ns != 0 and
            now -| self.last_mark_completed_ns < 30 * std.time.ns_per_s and
            self.obsolete_debt < @max(8 * 1024 * 1024, self.stats.retained_payload_bytes / 20) and
            self.stats.unreferenced_payload_bytes_at_collection == 0;
    }

    pub fn collect(self: *Store, primary: *erased.Store) !bool {
        const budget = collectionStepBytes();
        return self.collectStep(primary, if (budget == 0) std.math.maxInt(u64) else budget);
    }

    pub fn collectStep(self: *Store, primary: *erased.Store, budget_bytes: u64) !bool {
        if (try self.collectStepDeferredMark(primary, budget_bytes)) return true;
        if (!self.mark_outside_lock) return false;
        try self.advanceMarkingSnapshot();
        self.lock();
        defer self.mutex.unlock();
        if (self.marking == null or self.stats.unresolved_primary_commits != 0) return false;
        return self.advanceMarkingLocked(@max(1, budget_bytes));
    }

    /// Caller may hold DB.apply. The experimental scan must be advanced by
    /// advanceMarkingSnapshot before that outer lock is acquired.
    pub fn collectStepDeferredMark(self: *Store, primary: *erased.Store, budget_bytes: u64) !bool {
        self.lock();
        defer self.mutex.unlock();
        const locked_started = time.monotonicNs();
        defer recordDuration(&self.stats.collection_locked_ns, &self.stats.collection_max_locked_ns, time.monotonicNs() -| locked_started);
        const had_mark = self.marking != null;
        const done = try self.collectStepLocked(primary, budget_bytes);
        if (!had_mark and self.marking != null) return self.advanceMarkingLocked(@max(1, budget_bytes));
        return done;
    }

    fn collectStepLocked(self: *Store, primary: *erased.Store, budget_bytes: u64) !bool {
        if (self.read_only) return error.ReadOnly;
        if (self.poisoned) return error.VectorPayloadStorePoisoned;
        if ((self.collection == null and self.marking == null and self.stats.active_sessions != 0) or self.stats.unresolved_primary_commits != 0) {
            self.stats.collection_deferrals += 1;
            return false;
        }
        if (self.collection != null) return self.advanceCollectionLocked(@max(1, budget_bytes));
        if (self.marking != null) return self.advanceMarkingLocked(@max(1, budget_bytes));
        return self.startMarkingLocked(primary) catch |err| switch (err) {
            // Setup has released every temporary snapshot before we defer.
            // A budget rejection is scheduling pressure; backing allocation
            // failures and durable I/O errors still propagate to the caller.
            error.CollectionWorkspaceUnavailable => {
                self.stats.collection_deferrals += 1;
                return false;
            },
            else => return err,
        };
    }

    fn markWorkspaceBytes(self: *const Store) !usize {
        const planning_bytes = if (self.incremental_planning and self.selective_gc)
            try std.math.mul(usize, self.opened.readers.len, @sizeOf(SegmentStats))
        else
            0;
        // AutoHashMap's 80% load factor rounds to a power-of-two capacity.
        // Forty bytes per bucket covers a digest, dimension and metadata;
        // fixed slack covers the header/alignment and the Marking itself.
        // Reserve also for the source lease (and full-GC sealing successor).
        if (self.bitmap_marking) {
            const locator_workspace = if (self.bitmap_locator) try LiveSet.locatorWorkspaceBytes(&self.opened) else 0;
            return std.math.add(usize, locator_workspace, try std.math.add(usize, try LiveSet.workspaceBytes(&self.opened), try std.math.add(usize, @sizeOf(Marking) + 4096 + planning_bytes, try std.math.mul(usize, @intCast(self.opened.catalogMetadataBytes()), 3))));
        }
        const count = std.math.cast(u32, self.stats.retained_payloads) orelse return error.VectorPayloadCountOverflow;
        const load_capacity = (try std.math.mul(usize, count, 5)) / 4;
        const slots = try std.math.ceilPowerOfTwo(usize, @max(8, try std.math.add(usize, load_capacity, 1)));
        return std.math.add(usize, try std.math.add(usize, try std.math.mul(usize, slots, 40), @sizeOf(Marking) + 4096 + planning_bytes), try std.math.mul(usize, @intCast(self.opened.catalogMetadataBytes()), 2));
    }

    fn startMarkingLocked(self: *Store, primary: *erased.Store) !bool {
        const setup_started = time.monotonicNs();
        defer recordDuration(&self.stats.collection_setup_ns, &self.stats.collection_max_setup_ns, time.monotonicNs() -| setup_started);
        // Reclamation must follow durable primary publication. An in-memory
        // update alone cannot retire the payload selected by the older WAL
        // tip: recovery might still need it after a power loss.
        // Source mode is gated to the local LSM backend. Its replay-state sync
        // fsyncs the same primary WAL that commits artifact references, without
        // forcing hot scalar documents out of the memtable during background
        // collection. Backends without that boundary retain the full sync.
        if (primary.vtable.sync_replay_state != null) {
            try primary.syncReplayState();
        } else {
            try primary.sync(true);
        }
        // Seal the cut before selecting segments; later preparations remain
        // in the WAL suffix preserved by publication.
        if (self.selective_gc) try self.checkpointLocked();
        var txn = try primary.beginRead();
        var transferred = false;
        defer if (!transferred) txn.abort();
        const epoch = try primaryReferenceEpoch(&txn);
        const ann_digest = if (self.checkpoint_receipts) try self.annAuthorityDigest() else [_]u8{0} ** 32;
        if (self.receipt) |receipt| {
            if (receipt.primary_epoch == epoch and receipt.ann != null and
                std.mem.eql(u8, &receipt.ann.?, &ann_digest) and
                std.mem.eql(u8, &receipt.source, &try self.sourceAuthorityDigest()))
            {
                self.stats.checkpoint_receipt_hits += 1;
                self.stats.live_payloads_at_collection = receipt.retained_payloads;
                self.stats.live_payload_bytes_at_collection = receipt.retained_payload_bytes;
                self.stats.unreferenced_payload_bytes_at_collection = 0;
                return true;
            }
        }
        const owner_epoch_raw = if (payload.ownershipEnabled()) txn.get(payload.ownership_epoch_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        } else null;
        const indexed = if (owner_epoch_raw) |raw| raw.len == 8 and std.mem.readInt(u64, raw[0..8], .little) == epoch else false;
        var cursor = try txn.openCursor();
        errdefer cursor.close();
        var ann: ?native.Opened = if (self.ann_reference_root) |root|
            native.Store.openReadOnlyWithBlocks(self.alloc, self.opened.store.storage, root) catch |err| switch (err) {
                error.FileNotFound, error.MissingVectorBlockManifest => null,
                else => return err,
            }
        else
            null;
        errdefer if (ann) |*opened| opened.deinit();
        if (ann) |*opened| if (opened.baseEncoding() != .artifact_reference) return error.VectorStoreReferenceFormatRequired;
        // The ANN lease may include a large reference WAL during ingestion.
        // Admit the complete mark map against that resident lease before
        // allocating it; otherwise a corpus-sized allocation can cross the
        // slice limit even with an empty source WAL. Retry after the ANN WAL
        // checkpoints or other leases release, without pinning this attempt.
        const mark_scratch = if (self.budget) |budget|
            budget.reserveScratch(try self.markWorkspaceBytes()) catch return error.CollectionWorkspaceUnavailable
        else
            null;
        defer if (mark_scratch) |reservation| reservation.release();
        if (!self.selective_gc and self.opened.store.wal_committed_bytes != 0) {
            // Retain the cut as a sealed extent so full GC can share its
            // post-cut WAL view too, without rereading or copying that WAL.
            var successor = try self.opened.clone(self.alloc);
            var successor_owned = true;
            defer if (successor_owned) successor.deinit();
            const sealed = successor.store.sealWal() catch |err| {
                self.poisoned = successor.store.poisoned;
                return err;
            };
            if (sealed) {
                self.opened.deinit();
                self.opened = successor;
                successor_owned = false;
            } else try self.checkpointLocked();
        }
        var source_snapshot = try self.opened.clone(self.alloc);
        errdefer source_snapshot.deinit();
        const scopes = if (self.ann_scopes) |scopes| try self.alloc.dupe(u64, scopes) else null;
        errdefer if (scopes) |copy| self.alloc.free(copy);
        // The physical inventory bounds the live set at this cut. Allocate
        // its map once before scanning, avoiding old+new hash-table peaks
        // midway through a mark while the post-cut WAL is also growing.
        var live = LiveSet.init(self.alloc);
        errdefer live.deinit();
        if (self.bitmap_marking) {
            try live.enableBitmapsDeferred(&source_snapshot, self.bitmap_locator);
        } else try live.ensureTotalCapacity(std.math.cast(u32, self.stats.retained_payloads) orelse return error.ResourceBudgetExceeded);
        const segment_stats = if (self.incremental_planning and self.selective_gc and !source_snapshot.store.manifest.?.hasPhysicalBase())
            try self.alloc.alloc(SegmentStats, source_snapshot.readers.len)
        else
            null;
        errdefer if (segment_stats) |stats| self.alloc.free(stats);
        if (segment_stats) |stats| @memset(stats, .{});
        const marking = try self.alloc.create(Marking);
        marking.* = .{
            .source = source_snapshot,
            .segment_stats = segment_stats,
            .retained_at_cut = self.stats.retained_payloads,
            .debt_at_cut = self.obsolete_debt,
            .scopes = scopes,
            .outside_lock = self.mark_outside_lock,
            .txn = txn,
            .cursor = cursor,
            .indexed = indexed,
            .ann = ann,
            .epoch = epoch,
            .ann_digest = ann_digest,
            .live = live,
            .tail = .init(self.alloc),
            .boundary = .{
                .generation = self.opened.store.wal_generation,
                .committed_bytes = self.opened.store.wal_committed_bytes,
                .covered_source_sequence = self.opened.store.covered_source_sequence,
            },
        };
        self.marking = marking;
        transferred = true;
        if (indexed) self.stats.ownership_index_collections += 1;
        // Advance in the caller after setup's allocation cleanup scope ends.
        return false;
    }

    fn recordDuration(total: *u64, maximum: *u64, elapsed: u64) void {
        total.* += elapsed;
        maximum.* = @max(maximum.*, elapsed);
    }

    const SegmentStats = struct {
        total: u64 = 0,
        dead: u64 = 0,
        live_rows: u64 = 0,

        fn add(self: *@This(), reader: anytype, index: usize, live: *const LiveSet) !void {
            const row = reader.sourceIdentityAt(index);
            if (!row.vector or row.key.len != 32) return;
            const bytes = @as(u64, row.dims) * 4;
            self.total += bytes;
            if (!live.contains(row.key[0..32].*)) self.dead += bytes else self.live_rows += 1;
        }
    };

    const SparseSegment = struct {
        index: usize,
        live_bytes: u64,
        live_rows: u64,
        ratio: f64,
        fn less(_: void, a: @This(), b: @This()) bool {
            return if (a.ratio == b.ratio) a.index < b.index else a.ratio > b.ratio;
        }
    };

    fn deferMarkPlanning(self: *Store) void {
        self.stats.collection_deferrals += 1;
        self.marking.?.deinit(self.alloc);
        self.marking = null;
    }

    fn finishMarkingLocked(self: *Store, budget_bytes: u64) !bool {
        const started = time.monotonicNs();
        var plan_finished: ?u64 = null;
        defer recordDuration(&self.stats.collection_plan_ns, &self.stats.collection_max_plan_ns, (plan_finished orelse time.monotonicNs()) -| started);
        const marking = self.marking.?;
        // Planning consumes the mark's live map. On failure, retry from a new
        // snapshot rather than allowing a completed mark with an empty map.
        errdefer if (self.marking) |pending| {
            pending.deinit(self.alloc);
            self.marking = null;
        };
        var live = marking.live;
        marking.live = .init(self.alloc);
        defer live.deinit();
        const epoch = marking.epoch;
        const ann_digest = marking.ann_digest;
        // Cardinality alone is insufficient: verify every selected digest
        // before declaring the source inventory fully live. Clean restarts
        // then avoid rewriting the entire corpus just to collect zero bytes.
        if (marking.verification_done and marking.tail.count() == 0 and live.count() == self.stats.retained_payloads) {
            const live_bytes = marking.verified_bytes;
            self.stats.live_payloads_at_collection = live.count();
            self.stats.live_payload_bytes_at_collection = live_bytes;
            self.stats.unreferenced_payload_bytes_at_collection = 0;
            self.stats.collections += 1;
            try self.saveCheckpointReceipt(if (marking.rescued_any) null else epoch, if (marking.rescued_any) null else ann_digest);
            self.last_mark_completed_ns = time.monotonicNs();
            self.obsolete_debt -|= marking.debt_at_cut;
            marking.deinit(self.alloc);
            self.marking = null;
            return true;
        }
        if (marking.segment_stats != null and marking.planning_reader != marking.source.readers.len) {
            // Verification skipped density work for an all-live cut, but a
            // post-cut preparation may now prevent that fast path. Resume the
            // bounded scan instead of doing a surprise corpus scan locked.
            marking.live = live;
            live = .init(self.alloc);
            marking.planning_required = true;
            marking.scan_done = false;
            return false;
        }
        const manifest = self.opened.store.manifest orelse return error.MissingVectorBlockManifest;
        var live_bytes: u64 = 0;
        var dimensions = live.valueIterator();
        while (dimensions.next()) |dims| live_bytes += @as(u64, dims.*) * 4;
        // Reclamation is also the stable boundary at which an adaptive layout
        // may shrink after churn. The target is a bound, not equal-sized shards.
        const shards = if (self.selective_gc) manifest.shard_count else self.segment_sizing.shardCount(live_bytes, manifest.shard_count);
        var constructing = true;
        var selected = std.ArrayListUnmanaged(@import("antfly_vectorindex").vector_block_manifest.Segment).empty;
        defer selected.deinit(self.alloc);
        var copy_live = LiveSet.init(self.alloc);
        defer copy_live.deinit();
        const partial = self.selective_gc and !manifest.hasPhysicalBase();
        const bounded_plan = (self.bitmap_marking and self.bitmap_locator) or (partial and self.sparse_gc_copy_bytes != 0);
        const selection_scratch = if (self.budget) |budget| if (partial and self.sparse_gc_copy_bytes != 0)
            budget.reserveScratch(try std.math.mul(usize, self.opened.readers.len, 2 * @sizeOf(SparseSegment))) catch {
                self.deferMarkPlanning();
                return false;
            }
        else
            null else null;
        defer if (selection_scratch) |reservation| reservation.release();
        var selected_indices = std.ArrayListUnmanaged(usize).empty;
        defer selected_indices.deinit(self.alloc);
        var selected_rows: u64 = 0;
        if (partial) {
            // Retain cold segments byte-for-byte. Updated versions arrive in
            // newer append runs; prioritize segments with reclaimable bytes.
            var best: ?usize = null;
            var best_ratio: f64 = 0;
            var best_live_rows: u64 = 0;
            var selected_reclaimable = false;
            var selected_copy_bytes: u64 = 0;
            var selected_sparse_rows: u64 = 0;
            var sparse = std.ArrayListUnmanaged(SparseSegment).empty;
            defer sparse.deinit(self.alloc);
            for (self.opened.readers, 0..) |reader, index| {
                var counts: SegmentStats = .{};
                if (marking.segment_stats) |stats| {
                    std.debug.assert(marking.planning_reader == self.opened.readers.len);
                    std.debug.assert(reader.generation == marking.source.readers[index].generation and reader.shard_id == marking.source.readers[index].shard_id);
                    counts = stats[index];
                } else for (0..reader.count) |i| try counts.add(reader, i, &live);
                const total = counts.total;
                const dead = counts.dead;
                const live_rows = counts.live_rows;
                const ratio = @as(f64, @floatFromInt(dead)) / @as(f64, @floatFromInt(@max(1, total)));
                if (ratio > best_ratio) {
                    best_ratio = ratio;
                    best = index;
                    best_live_rows = live_rows;
                }
                if (ratio >= 0.25 or reader.count == 0) {
                    try selected.append(self.alloc, manifest.segments[index]);
                    try selected_indices.append(self.alloc, index);
                    selected_reclaimable = selected_reclaimable or dead != 0;
                    selected_copy_bytes +|= total - dead;
                    selected_rows +|= live_rows;
                } else if (self.sparse_gc_copy_bytes != 0 and dead != 0) {
                    try sparse.append(self.alloc, .{ .index = index, .live_bytes = total - dead, .live_rows = live_rows, .ratio = ratio });
                }
            }
            // Empty output segments are cheap to retire, but selecting one
            // must not starve real garbage below the density threshold.
            // Sparse batching is a fallback, not extra cold work appended to
            // an already useful dense collection. Empty segments do not count
            // as reclaimable work and must not suppress sparse progress.
            if (self.sparse_gc_copy_bytes != 0 and !selected_reclaimable) {
                std.mem.sort(SparseSegment, sparse.items, {}, SparseSegment.less);
                for (sparse.items) |candidate| {
                    // Always admit the best nonempty candidate to make progress
                    // even when one segment exceeds the target. Copying still
                    // yields at the existing per-step byte budget.
                    if (selected_reclaimable and (candidate.live_bytes > self.sparse_gc_copy_bytes -| selected_copy_bytes or candidate.live_rows > 65536 -| selected_sparse_rows)) continue;
                    try selected.append(self.alloc, manifest.segments[candidate.index]);
                    try selected_indices.append(self.alloc, candidate.index);
                    selected_copy_bytes +|= candidate.live_bytes;
                    selected_sparse_rows +|= candidate.live_rows;
                    selected_rows +|= candidate.live_rows;
                    selected_reclaimable = true;
                }
            } else if (!selected_reclaimable) {
                if (best) |index| {
                    try selected.append(self.alloc, manifest.segments[index]);
                    try selected_indices.append(self.alloc, index);
                    selected_rows +|= best_live_rows;
                }
            }
        }
        const plan_count: u32 = if (partial) @intCast(@min(live.count(), selected_rows)) else live.count();
        const planning_scratch = if (self.budget) |budget| if (bounded_plan) blk: {
            var bytes = try std.math.add(usize, try std.math.mul(usize, plan_count, @sizeOf(CollectionItem)), try std.math.mul(usize, @intCast(self.opened.catalogMetadataBytes()), 3));
            if (partial) {
                if (self.bitmap_marking) {
                    bytes = try std.math.add(usize, bytes, try LiveSet.workspaceBytes(if (self.bitmap_locator) &live.source.? else &self.opened));
                } else {
                    const slots = try std.math.ceilPowerOfTwo(usize, @max(8, (@as(usize, plan_count) * 5) / 4 + 1));
                    bytes = try std.math.add(usize, bytes, try std.math.mul(usize, slots, 40));
                }
            }
            break :blk budget.reserveScratch(bytes) catch {
                self.deferMarkPlanning();
                return false;
            };
        } else null else null;
        defer if (planning_scratch) |reservation| reservation.release();
        if (partial) {
            if (self.bitmap_marking) {
                if (self.bitmap_locator) try copy_live.enableSubset(&live) else try copy_live.enableBitmaps(&self.opened);
            } else if (bounded_plan) try copy_live.ensureTotalCapacity(plan_count);
            for (selected_indices.items) |index| {
                const reader = self.opened.readers[index];
                for (0..reader.count) |i| {
                    const row = reader.sourceIdentityAt(i);
                    if (row.key.len != 32) continue;
                    if (live.get(row.key[0..32].*)) |dims| try copy_live.put(row.key[0..32].*, dims);
                }
            }
        }
        const copying = if (partial) &copy_live else &live;
        var copy_bytes: u64 = 0;
        var copy_dims = copying.valueIterator();
        while (copy_dims.next()) |dims| copy_bytes += @as(u64, dims.*) * 4;
        const items = try self.alloc.alloc(CollectionItem, copying.count());
        errdefer if (constructing) self.alloc.free(items);
        var it = copying.iterator();
        var pos: usize = 0;
        while (it.next()) |item| : (pos += 1) {
            const hash = vector_block.keyHash(&item.key_ptr.*);
            items[pos] = .{ .digest = item.key_ptr.*, .dims = item.value_ptr.*, .hash = hash, .shard = @intCast(hash & (shards - 1)) };
        }
        std.mem.sort(CollectionItem, items, {}, CollectionItem.less);
        const generation = try std.math.add(u64, manifest.latest_generation, 1);
        const selected_owned = if (partial) try selected.toOwnedSlice(self.alloc) else null;
        errdefer if (constructing) {
            if (selected_owned) |owned| self.alloc.free(owned);
        };
        const collection = try self.alloc.create(Collection);
        errdefer if (constructing) self.alloc.destroy(collection);
        collection.* = .{
            .input = try self.opened.clone(self.alloc),
            .live = live,
            .tail = marking.tail,
            .items = items,
            .total_live_bytes = copy_bytes,
            .marked_live_bytes = live_bytes,
            .shards = shards,
            .generation = generation,
            .primary_epoch = epoch,
            .ann = ann_digest,
            .rescued = marking.rescued_any,
            .debt_at_cut = marking.debt_at_cut,
            .selected = selected_owned,
            .boundary = marking.boundary,
        };
        live = LiveSet.init(self.alloc);
        self.collection = collection;
        marking.tail = .init(self.alloc);
        marking.deinit(self.alloc);
        self.marking = null;
        constructing = false;
        plan_finished = time.monotonicNs();
        return self.advanceCollectionLocked(@max(1, budget_bytes));
    }

    fn advanceCollectionLocked(self: *Store, budget_bytes: u64) !bool {
        self.stats.collection_steps += 1;
        const copy_started = time.monotonicNs();
        var publication_started: ?u64 = null;
        defer {
            recordDuration(&self.stats.collection_copy_ns, &self.stats.collection_max_copy_ns, (publication_started orelse time.monotonicNs()) -| copy_started);
            if (publication_started) |start| recordDuration(&self.stats.collection_publish_ns, &self.stats.collection_max_publish_ns, time.monotonicNs() -| start);
        }
        const collection = self.collection.?;
        const live_count = collection.live.count();
        // After a failed step, discard unpublished files and restart marking.
        // An ambiguous CURRENT is instead fenced and its files are preserved.
        errdefer {
            if (collection.publication_attempted) self.poisoned = true;
            collection.deinit(self.alloc);
            self.collection = null;
        }
        const initial_bytes = collection.bytes_read;
        while (collection.shard < collection.shards) {
            if (collection.selected != null and collection.output == null and
                (collection.pos == collection.items.len or collection.items[collection.pos].shard != collection.shard) and
                !(collection.items.len == 0 and collection.shard == 0))
            {
                collection.shard += 1;
                continue;
            }
            if (collection.output == null) collection.output = try collection.input.store.beginStreamingBlock(
                collection.generation,
                collection.boundary.covered_source_sequence,
                collection.shard,
                collection.shards,
                collection.input.baseEncoding() orelse return error.MissingVectorPayloadStore,
            );
            const output = &collection.output.?;
            while (collection.pos < collection.items.len and collection.items[collection.pos].shard == collection.shard) {
                const item = collection.items[collection.pos];
                const found = try collection.input.get(&item.digest, std.math.maxInt(u64), 1);
                if (found != .vector or found.vector.dims != item.dims) return error.MissingCommittedVectorPayload;
                try collection.scratch.resize(self.alloc, item.dims);
                const exact = try found.vector.decodeExactInto(collection.scratch.items);
                try output.writer.page.appendVector(&item.digest, collection.boundary.covered_source_sequence, 1, exact);
                try output.flushIfNeeded();
                collection.bytes_read += exact.len * 4;
                self.stats.collection_bytes_read += exact.len * 4;
                collection.pos += 1;
                if (collection.bytes_read - initial_bytes >= budget_bytes) return false;
            }
            try collection.staged.ensureUnusedCapacity(self.alloc, 1);
            const receipt = try output.finish();
            collection.staged.appendAssumeCapacity(receipt);
            collection.bytes_written += receipt.bytes;
            self.stats.collection_bytes_written += receipt.bytes;
            output.deinit();
            collection.output = null;
            collection.shard += 1;
        }
        publication_started = time.monotonicNs();
        const publication_scratch = if (self.budget) |budget|
            try budget.reserveScratch(try std.math.add(usize, 64 * 1024, try std.math.add(usize, try std.math.mul(usize, @intCast(self.opened.catalogMetadataBytes()), 2), try std.math.mul(usize, collection.staged.items.len, 1024))))
        else
            null;
        defer if (publication_scratch) |reservation| reservation.release();
        var prepared = try self.opened.store.prepareSourceCollection(collection.generation, collection.staged.items, collection.selected, collection.boundary);
        defer prepared.deinit();
        var next = try prepared.openReaders(self.alloc, &self.opened);
        var next_owned = true;
        defer if (next_owned) next.deinit();
        // The mark is no longer needed once copying completes. Reuse its
        // capacity for control-mode physical accounting rather than allocate
        // another corpus-sized map while retaining the original one.
        const totals = if (!self.incremental_inventory and collection.selected != null)
            try self.inventoryInto(&next, &collection.live)
        else
            null;
        try self.prepareOpened(&next);
        // Inventory is a cache: a failed commit leaves old authority intact
        // and must discard any precomputed cache for the unpublished view.
        errdefer if (self.incremental_inventory) self.inventory.deinit(self.alloc);
        collection.publication_attempted = true;
        self.opened.store.commitPrepared(&prepared) catch |err| {
            collection.publication_attempted = self.opened.store.poisoned;
            return err;
        };
        if (self.coalesce_directory) {
            if (self.directory) |directory| directory.removeRetired(&self.opened, &next) catch {};
        }
        self.opened.deinit();
        self.opened = next;
        next_owned = false;
        if (self.coalesce_directory) {
            self.refreshDirectory(false, false);
            if (self.directory) |directory| directory.saveCoalesced(self.opened.store.storage, self.opened.store.root_dir) catch {};
        } else self.refreshDirectory(true, true);
        var tail_bytes: u64 = 0;
        var tail = collection.tail.valueIterator();
        while (tail.next()) |dims| tail_bytes += @as(u64, dims.*) * 4;
        const retained_bytes = collection.bytes_read + tail_bytes;
        const marked_live_bytes = collection.marked_live_bytes + tail_bytes;
        // A selective pass copies only selected segments. Untouched live
        // payloads are still live; excluding them would report the cold corpus
        // as garbage and include unreclaimed garbage in the live counters.
        self.stats.unreferenced_payload_bytes_at_collection = self.stats.retained_payload_bytes -| marked_live_bytes;
        self.stats.retained_payloads = collection.items.len + collection.tail.count();
        self.stats.retained_payload_bytes = retained_bytes;
        if (self.incremental_inventory) {
            self.stats.retained_payloads = self.inventory.counts.count();
            self.stats.retained_payload_bytes = self.inventory.bytes;
        } else if (totals) |physical| {
            self.stats.retained_payloads = physical.count;
            self.stats.retained_payload_bytes = physical.bytes;
        }
        if (self.inventory_min_payloads != 0 and self.incremental_inventory and self.stats.retained_payloads < self.inventory_min_payloads) {
            self.inventory.deinit(self.alloc);
            self.incremental_inventory = false;
            self.stats.inventory_policy_switches += 1;
        }
        // Includes post-cut preparations conservatively; the next mark decides
        // whether those transactions committed or became orphans.
        self.stats.live_payloads_at_collection = live_count + collection.tail.count();
        self.stats.live_payload_bytes_at_collection = marked_live_bytes;
        self.stats.collections += 1;
        self.last_mark_completed_ns = time.monotonicNs();
        self.obsolete_debt -|= collection.debt_at_cut;
        const receipt_epoch: ?u64 = if (collection.tail.count() == 0 and collection.selected == null and !collection.rescued) collection.primary_epoch else null;
        const receipt_ann: ?payload.Digest = if (collection.tail.count() == 0 and collection.selected == null and !collection.rescued) collection.ann else null;
        // Authority and its serving view are installed. Cleanup and receipt
        // caching may retry later; neither can invalidate a healthy writer.
        _ = self.opened.store.reclaimUnreferencedFiles() catch |err| blk: {
            std.log.warn("source collection cleanup deferred: {s}", .{@errorName(err)});
            break :blk 0;
        };
        self.saveCheckpointReceipt(receipt_epoch, receipt_ann) catch |err| {
            self.receipt = null;
            std.log.warn("source collection receipt deferred: {s}", .{@errorName(err)});
        };
        collection.deinit(self.alloc);
        self.collection = null;
        return true;
    }
};

test "source vector payloads collection syncs primary WAL without flushing hot documents" {
    const alloc = std.testing.allocator;
    const backend_mod = @import("lsm_backend.zig");
    const docs = @import("docstore.zig");
    const keys = @import("internal_keys.zig");
    var primary_memory = lsm.MemoryStorage.init(alloc);
    defer primary_memory.deinit();
    const SyncProbe = struct {
        var syncs: usize = 0;
        fn append(ptr: *anyopaque, path: []const u8, bytes: []const u8, sync: bool) !void {
            const memory: *lsm.MemoryStorage = @ptrCast(@alignCast(ptr));
            if (sync and bytes.len == 0) syncs += 1;
            try memory.storage().vtable.append_file_absolute.?(ptr, path, bytes, sync);
        }
    };
    var primary_vtable = primary_memory.storage().vtable.*;
    primary_vtable.append_file_absolute = SyncProbe.append;
    var primary_storage = primary_memory.storage();
    primary_storage.vtable = &primary_vtable;
    var source_memory = lsm.MemoryStorage.init(alloc);
    defer source_memory.deinit();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-a");
    defer alloc.free(key);
    const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
    defer alloc.free(first);
    const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 4, 5, 6 });
    defer alloc.free(second);
    {
        var backend = try backend_mod.Backend.open(alloc, "/primary", .{
            .storage = primary_storage,
            .flush_threshold = 10000,
        });
        defer backend.abandonAfterCrash();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        var source = try Store.open(alloc, source_memory.storage(), "/source", false);
        defer source.deinit();
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        try store.put("doc", "current document");
        try store.put(key, first);
        try store.put(key, second);
        const mutable_before = backend.mutable.entries.items.len;
        try std.testing.expect(mutable_before > 0);
        SyncProbe.syncs = 0;
        try std.testing.expect(try source.collectStep(&raw, std.math.maxInt(u64)));
        try std.testing.expectEqual(@as(usize, 2), SyncProbe.syncs);
        try std.testing.expectEqual(mutable_before, backend.mutable.entries.items.len);
        try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
        try std.testing.expectEqual(@as(u64, 1), source.stats.retained_payloads);
    }
    var reopened = try backend_mod.Backend.open(alloc, "/primary", .{
        .storage = primary_memory.storage(),
        .flush_threshold = 10000,
    });
    defer reopened.close();
    var raw = try reopened.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var source = try Store.open(alloc, source_memory.storage(), "/source", false);
    defer source.deinit();
    var store = try docs.DocStore.openRuntime(alloc, &raw);
    defer store.close();
    store.payload_store = source.interface();
    const document = try store.get(alloc, "doc");
    defer alloc.free(document);
    try std.testing.expectEqualStrings("current document", document);
    const artifact = try store.get(alloc, key);
    defer alloc.free(artifact);
    try std.testing.expectEqualSlices(u8, second, artifact);
}

test "source vector payloads survive restart and preserve independent model versions" {
    const alloc = std.testing.allocator;
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    const keys = @import("internal_keys.zig");
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-a");
    defer alloc.free(key);
    const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 17, &.{ 1, -0.125, 3 });
    defer alloc.free(artifact);
    var reference: [payload.reference_len]u8 = undefined;
    {
        var source = try Store.open(alloc, memory.storage(), "/source", false);
        defer source.deinit();
        const session = try payload.Session.create(alloc, source.interface());
        defer session.release();
        const value = try session.put(key, artifact);
        @memcpy(&reference, value);
        try std.testing.expectEqualSlices(u8, artifact, try session.get(key, value));
        try session.prepareCommit();
        try session.prepareCommit();
        try std.testing.expectEqual(@as(u64, 1), source.statsSnapshot().prepared_payloads);
        try source.checkpoint();
        try std.testing.expectEqual(@as(u64, if (source.append_only) 0 else 1), source.opened.baseVectorCount());
    }
    var reopened = try Store.open(alloc, memory.storage(), "/source", true);
    defer reopened.deinit();
    const session = try payload.Session.create(alloc, reopened.interface());
    defer session.release();
    try std.testing.expectEqualSlices(u8, artifact, try session.get(key, &reference));
    const other_key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-b");
    defer alloc.free(other_key);
    try std.testing.expectError(error.VectorReferenceIdentityMismatch, session.get(other_key, &reference));
}

test "source vector payloads primary references preserve snapshot and cursor isolation" {
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    const docs = @import("docstore.zig");
    const keys = @import("internal_keys.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var source = try Store.open(alloc, memory.storage(), "/source-isolation", false);
    defer source.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw_store = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw_store.deinit();
    var store = try docs.DocStore.openRuntime(alloc, &raw_store);
    defer store.close();
    store.payload_store = source.interface();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-a");
    defer alloc.free(key);
    const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
    defer alloc.free(first);
    const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 4, 5, 6 });
    defer alloc.free(second);
    try store.put(key, first);
    {
        var physical = try raw_store.beginRead();
        defer physical.abort();
        try std.testing.expectEqual(@as(usize, payload.reference_len), (try physical.get(key)).len);
        try std.testing.expect(payload.isReference(try physical.get(key)));
    }
    var old = try store.beginReadTxn();
    defer old.abort();
    try store.put(key, second);
    const resolved_before_metadata = source.stats.resolved_payloads;
    const old_metadata = try old.getArtifactMetadata(key);
    const current_metadata = try store.getArtifactMetadata(key);
    try std.testing.expectEqual(@as(?u64, 1), old_metadata.sourceHash());
    try std.testing.expectEqual(@as(?u64, 2), current_metadata.sourceHash());
    try std.testing.expectEqual(@as(?u32, 3), current_metadata.dense_dimensions);
    const prefix_keys = try store.scanPrefixKeysPage(alloc, key, null, 10);
    defer {
        for (prefix_keys) |owned| alloc.free(owned);
        alloc.free(prefix_keys);
    }
    const range_keys = try store.scanRangeKeys(alloc, key, "");
    defer {
        for (range_keys) |owned| alloc.free(owned);
        alloc.free(range_keys);
    }
    try std.testing.expectEqual(@as(usize, 1), prefix_keys.len);
    try std.testing.expectEqual(@as(usize, 1), range_keys.len);
    try std.testing.expectEqualStrings(key, prefix_keys[0]);
    try std.testing.expectEqual(resolved_before_metadata, source.stats.resolved_payloads);
    try std.testing.expectEqualSlices(u8, first, try old.get(key));
    {
        var current = try store.beginProbeTxn();
        defer current.abort();
        var values: [1]?[]const u8 = undefined;
        try current.getManySortedTransient(&.{key}, &values);
        try std.testing.expectEqualSlices(u8, second, values[0].?);
    }
    {
        var batch = try store.beginWriteBatch();
        defer batch.abort();
        try batch.put(key, first);
        try std.testing.expectEqualSlices(u8, first, try batch.get(key));
        // Simulate a crash/abort after durable preparation but before primary
        // commit. The prepared value must not replace the committed reference.
        try batch.payload_session.?.prepareCommit();
    }
    var current = try store.beginReadTxn();
    var cursor = try current.openCursor();
    defer cursor.close();
    current.abort();
    try std.testing.expectEqualSlices(u8, second, (try cursor.seekAtOrAfter(key)).?.value);
    try source.checkpoint();
    try std.testing.expectEqualSlices(u8, first, try old.get(key));
}

test "source vector payloads collect obsolete versions only after readers retire" {
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    const docs = @import("docstore.zig");
    const keys = @import("internal_keys.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var source = try Store.open(alloc, memory.storage(), "/source-gc", false);
    defer source.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var store = try docs.DocStore.openRuntime(alloc, &raw);
    defer store.close();
    store.payload_store = source.interface();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
    defer alloc.free(first);
    const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 4, 5, 6, 7 });
    defer alloc.free(second);
    try store.put(key, first);
    var old = try store.beginReadTxn();
    try store.put(key, second);
    try std.testing.expect(!try source.collect(&raw));
    try std.testing.expectEqualSlices(u8, first, try old.get(key));
    old.abort();
    try std.testing.expect(try source.collect(&raw));
    try std.testing.expectEqual(@as(u64, 1), source.statsSnapshot().live_payloads_at_collection);
    try std.testing.expectEqual(@as(u64, 16), source.statsSnapshot().live_payload_bytes_at_collection);
    const collected_generation = source.opened.store.manifest.?.latest_generation;
    const collected_bytes_written = source.statsSnapshot().collection_bytes_written;
    try std.testing.expect(try source.collect(&raw));
    try std.testing.expectEqual(collected_generation, source.opened.store.manifest.?.latest_generation);
    try std.testing.expectEqual(collected_bytes_written, source.statsSnapshot().collection_bytes_written);
    const old_reference = try payload.Reference.forArtifact(key, first);
    try std.testing.expect((try source.opened.get(&old_reference.digest, std.math.maxInt(u64), null)) == .missing);
    var txn = try store.beginWriteTxn();
    try txn.delete(key);
    try txn.commit();
    try std.testing.expect(try source.collect(&raw));
    try std.testing.expectEqual(@as(u64, 0), source.statsSnapshot().live_payloads_at_collection);
}

test "source vector payloads fence ambiguous durable preparations and recover retries" {
    const alloc = std.testing.allocator;
    const keys = @import("internal_keys.zig");
    const Fault = struct {
        fn appendThenFail(ptr: *anyopaque, path: []const u8, bytes: []const u8, sync: bool) !void {
            const memory: *lsm.MemoryStorage = @ptrCast(@alignCast(ptr));
            try memory.storage().vtable.append_file_absolute.?(ptr, path, bytes, sync);
            return error.InjectedLostAcknowledgement;
        }
    };
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 23, &.{ 1, 2, 3 });
    defer alloc.free(artifact);
    var reference: [payload.reference_len]u8 = undefined;
    {
        var source = try Store.open(alloc, memory.storage(), "/ambiguous", false);
        defer source.deinit();
        var faulty_vtable = memory.storage().vtable.*;
        faulty_vtable.append_file_absolute = Fault.appendThenFail;
        source.opened.store.storage.vtable = &faulty_vtable;
        const session = try payload.Session.create(alloc, source.interface());
        defer session.release();
        @memcpy(&reference, try session.put(key, artifact));
        try std.testing.expectError(error.InjectedLostAcknowledgement, session.prepareCommit());
        try std.testing.expect(source.poisoned);
        try std.testing.expectError(error.VectorPayloadStorePoisoned, session.prepareCommit());
    }
    // A successful durable append with a lost acknowledgement is recovered;
    // retry reuses its identity without another append or revision change.
    var reopened = try Store.open(alloc, memory.storage(), "/ambiguous", false);
    defer reopened.deinit();
    const session = try payload.Session.create(alloc, reopened.interface());
    defer session.release();
    try std.testing.expectEqualSlices(u8, artifact, try session.get(key, &reference));
    _ = try session.put(key, artifact);
    try session.prepareCommit();
    session.committed = true;
    try std.testing.expectEqual(@as(u64, 0), reopened.statsSnapshot().prepared_payloads);
}

test "source vector payloads readonly open cannot initialize missing authority" {
    var memory = lsm.MemoryStorage.init(std.testing.allocator);
    defer memory.deinit();
    try std.testing.expectError(error.MissingVectorBlockManifest, Store.open(std.testing.allocator, memory.storage(), "/missing", true));
    try std.testing.expectEqual(@as(usize, 0), memory.files.count());
}

test "source vector payloads failed prepare leaves primary unchanged and orphan is reclaimable after recovery" {
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    const docs = @import("docstore.zig");
    const keys = @import("internal_keys.zig");
    const Fault = struct {
        fn appendThenFail(ptr: *anyopaque, path: []const u8, bytes: []const u8, sync: bool) !void {
            const memory: *lsm.MemoryStorage = @ptrCast(@alignCast(ptr));
            try memory.storage().vtable.append_file_absolute.?(ptr, path, bytes, sync);
            return error.InjectedLostAcknowledgement;
        }
    };
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 31, &.{ 1, 2, 3 });
    defer alloc.free(artifact);
    {
        var source = try Store.open(alloc, memory.storage(), "/failed-prepare", false);
        defer source.deinit();
        var faulty_vtable = memory.storage().vtable.*;
        faulty_vtable.append_file_absolute = Fault.appendThenFail;
        source.opened.store.storage.vtable = &faulty_vtable;
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        try std.testing.expectError(error.InjectedLostAcknowledgement, store.put(key, artifact));
        var primary = try raw.beginRead();
        defer primary.abort();
        try std.testing.expectError(error.NotFound, primary.get(key));
    }
    var source = try Store.open(alloc, memory.storage(), "/failed-prepare", false);
    defer source.deinit();
    try std.testing.expectEqual(@as(u64, 12), source.statsSnapshot().retained_payload_bytes);
    try std.testing.expect(try source.collect(&raw));
    try std.testing.expectEqual(@as(u64, 0), source.statsSnapshot().retained_payload_bytes);
    try std.testing.expectEqual(@as(u64, 12), source.statsSnapshot().unreferenced_payload_bytes_at_collection);
    var store = try docs.DocStore.openRuntime(alloc, &raw);
    defer store.close();
    store.payload_store = source.interface();
    try store.put(key, artifact);
    const restored = try store.get(alloc, key);
    defer alloc.free(restored);
    try std.testing.expectEqualSlices(u8, artifact, restored);
}

test "source vector payloads recover both outcomes of an ambiguous primary commit" {
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    const docs = @import("docstore.zig");
    const keys = @import("internal_keys.zig");
    for ([_]bool{ false, true }) |primary_committed| {
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var backend = mem.Backend.init(alloc, .{});
        defer backend.close();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
        defer alloc.free(key);
        const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 41, &.{ 1, 2, 3 });
        defer alloc.free(artifact);
        {
            var source = try Store.open(alloc, memory.storage(), "/primary-outcome", false);
            defer source.deinit();
            var store = try docs.DocStore.openRuntime(alloc, &raw);
            defer store.close();
            store.payload_store = source.interface();
            var txn = try store.beginWriteTxn();
            try txn.put(key, artifact);
            try txn.payload_session.?.prepareCommit();
            if (primary_committed) {
                // Commit the actual primary reference but lose acknowledgement
                // before DocStore can mark its payload session committed.
                try txn.write.?.commit();
                txn.write = null;
            }
            txn.abort();
            try std.testing.expectEqual(@as(u64, 1), source.statsSnapshot().unresolved_primary_commits);
            try std.testing.expect(!try source.collect(&raw));
            try std.testing.expectEqual(@as(u64, 12), source.statsSnapshot().retained_payload_bytes);
        }
        var source = try Store.open(alloc, memory.storage(), "/primary-outcome", false);
        defer source.deinit();
        try std.testing.expect(try source.collect(&raw));
        try std.testing.expectEqual(@as(u64, if (primary_committed) 12 else 0), source.statsSnapshot().retained_payload_bytes);
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        if (primary_committed) {
            const restored = try store.get(alloc, key);
            defer alloc.free(restored);
            try std.testing.expectEqualSlices(u8, artifact, restored);
        } else {
            try std.testing.expectError(error.NotFound, store.get(alloc, key));
        }
        try store.put(key, artifact);
        try std.testing.expectEqual(@as(u64, if (primary_committed) 0 else 12), source.statsSnapshot().prepared_payload_bytes);
        const retried = try store.get(alloc, key);
        defer alloc.free(retried);
        try std.testing.expectEqualSlices(u8, artifact, retried);
    }
}

test "source vector payloads ANN references share bytes across WAL checkpoint and immutable leases" {
    for ([_]vector_block.Encoding{ .float32, .float16 }) |encoding| {
        const alloc = std.testing.allocator;
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var source = try Store.openWithEncoding(alloc, memory.storage(), "/shared-source", false, encoding);
        defer source.deinit();
        if (source.location_cache) |cache| cache.deinit();
        source.location_cache = try native.ReferenceLocationCache.create(alloc, 1);
        const keys = @import("internal_keys.zig");
        const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-a");
        defer alloc.free(key);
        var vector: [128]f32 = @splat(0.25);
        const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 17, &vector);
        defer alloc.free(artifact);
        const session = try payload.Session.create(alloc, source.interface());
        const ref = try payload.Reference.decode(try session.put(key, artifact));
        try session.prepareCommit();
        session.committed = true;
        session.release();
        var snapshot = try source.snapshot(alloc);
        defer snapshot.deinit();
        var ann = try native.Store.open(alloc, memory.storage(), "/shared-ann");
        defer ann.deinit();
        try ann.publishEmptyBase(1, 0, .{ .shard_count = 16, .encoding = .artifact_reference });
        try ann.appendBatch(1, &.{.{ .kind = .upsert, .key = key, .source_sequence = 1, .revision = 71, .reference = .{ .digest = ref.digest, .dims = 128 } }}, 1, .{});
        try std.testing.expect(ann.wal_committed_bytes < 512);
        var opened = try native.Store.openWithBlocks(alloc, memory.storage(), "/shared-ann");
        defer opened.deinit();
        opened.external_payloads = &snapshot;
        const first = try opened.get(key, 1, 71);
        var scratch: [128]f32 = undefined;
        try std.testing.expectEqualSlices(f32, &vector, try first.vector.decodeExactInto(&scratch));
        try std.testing.expectEqual(@as(u64, 71), first.vector.revision);
        try std.testing.expect(try opened.checkpointWalToDelta(true));
        var checkpointed = try native.Store.openWithBlocks(alloc, memory.storage(), "/shared-ann");
        defer checkpointed.deinit();
        // Raw serving files contain only the immutable digest, never the vector.
        const raw = try checkpointed.get(key, 1, 71);
        try std.testing.expectEqual(.artifact_reference, raw.vector.encoding);
        try std.testing.expectEqualSlices(u8, &ref.digest, raw.vector.bytes);
        checkpointed.external_payloads = &snapshot;
        const location = try checkpointed.locateHashed(key, vector_block.keyHash(key), 1, 71);
        var bytes: [512]u8 = undefined;
        var requests = [_]native.ExactReadRequest{.{ .located = location.vector, .scratch = &bytes }};
        _ = try checkpointed.readExactIntoBatch(null, &requests);
        try std.testing.expectEqualSlices(f32, &vector, try requests[0].value.?.decodeExactInto(&scratch));
        try source.checkpoint();
        // The old ANN source lease remains valid after source WAL rotation.
        const after = try checkpointed.get(key, 1, 71);
        try std.testing.expectEqualSlices(f32, &vector, try after.vector.decodeExactInto(&scratch));
        var source_blocks = try source.snapshot(alloc);
        defer source_blocks.deinit();
        checkpointed.external_payloads = &source_blocks;
        try std.testing.expectEqual(encoding, checkpointed.payloadEncoding());
        const block_location = try checkpointed.locateHashed(key, vector_block.keyHash(key), 1, 71);
        try std.testing.expect(block_location.vector == .block);
        const hits_before = source.location_cache.?.hits.load(.monotonic);
        _ = try checkpointed.locateHashed(key, vector_block.keyHash(key), 1, 71);
        try std.testing.expectEqual(hits_before + 1, source.location_cache.?.hits.load(.monotonic));
        requests[0] = .{ .located = block_location.vector, .scratch = &bytes };
        _ = try checkpointed.readExactIntoBatch(null, &requests);
        try std.testing.expectEqualSlices(f32, &vector, try requests[0].value.?.decodeExactInto(&scratch));
        try std.testing.expect(try checkpointed.compactDeltasToBase());
        // The shared hint must rebind after relocation, while an old query
        // can still resolve through its pinned source files independently.
        try std.testing.expect(try source.opened.compactDeltasToBaseWithShardCount(64, 4096));
        const relocated = try native.Store.openWithBlocksReusing(alloc, memory.storage(), "/shared-source", &source.opened);
        source.opened.deinit();
        source.opened = relocated;
        var new_source = try source.snapshot(alloc);
        defer new_source.deinit();
        checkpointed.external_payloads = &new_source;
        const misses_before = source.location_cache.?.misses.load(.monotonic);
        const new_value = try checkpointed.viewExact((try checkpointed.locateHashed(key, vector_block.keyHash(key), 1, 71)).vector);
        try std.testing.expectEqualSlices(f32, &vector, try new_value.decodeExactInto(&scratch));
        try std.testing.expectEqual(misses_before + 1, source.location_cache.?.misses.load(.monotonic));
        checkpointed.external_payloads = &source_blocks;
        const old_value = try checkpointed.viewExact((try checkpointed.locateHashed(key, vector_block.keyHash(key), 1, 71)).vector);
        try std.testing.expectEqualSlices(f32, &vector, try old_value.decodeExactInto(&scratch));
        try std.testing.expectEqual(misses_before + 2, source.location_cache.?.misses.load(.monotonic));
    }
}

test "source vector payloads collection retains lagging durable ANN versions and old query leases" {
    for ([_]bool{ false, true }) |outside_lock| {
        const alloc = std.testing.allocator;
        const mem = @import("mem_backend.zig");
        const docs = @import("docstore.zig");
        const keys = @import("internal_keys.zig");
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var source = try Store.open(alloc, memory.storage(), "/source-ann-gc", false);
        defer source.deinit();
        source.mark_step_rows = 1;
        source.mark_outside_lock = outside_lock;
        source.ann_reference_root = try alloc.dupe(u8, "/ann-gc");
        var backend = mem.Backend.init(alloc, .{});
        defer backend.close();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
        defer alloc.free(key);
        const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
        defer alloc.free(first);
        const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 4, 5, 6 });
        defer alloc.free(second);
        try store.put(key, first);
        const ref = try payload.Reference.forArtifact(key, first);
        var ann = try native.Store.open(alloc, memory.storage(), "/ann-gc");
        defer ann.deinit();
        try ann.publishEmptyBase(1, 0, .{ .shard_count = 16, .encoding = .artifact_reference });
        try ann.appendBatch(1, &.{.{ .kind = .upsert, .key = key, .source_sequence = 1, .revision = 71, .reference = .{ .digest = ref.digest, .dims = 3 } }}, 1, .{});
        var old_query = try source.snapshot(alloc);
        defer old_query.deinit();
        try store.put(key, second);
        // A durable lagging ANN version remains reachable after primary overwrite.
        while (!try source.collect(&raw)) {}
        try std.testing.expectEqual(@as(u64, 2), source.statsSnapshot().retained_payloads);
        try ann.appendBatch(2, &.{.{ .kind = .tombstone, .key = key, .source_sequence = 2, .revision = 72 }}, 2, .{});
        var opened = try native.Store.openWithBlocks(alloc, memory.storage(), "/ann-gc");
        defer opened.deinit();
        try std.testing.expect(try opened.checkpointWalToDelta(true));
        try std.testing.expect(try opened.compactDeltasToBase());
        while (!try source.collect(&raw)) {}
        try std.testing.expectEqual(@as(u64, 1), source.statsSnapshot().retained_payloads);
        // A pinned old query still sees its immutable bytes after both owners collect.
        const retained = try old_query.get(&ref.digest, std.math.maxInt(u64), 1);
        var scratch: [3]f32 = undefined;
        try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3 }, try retained.vector.decodeExactInto(&scratch));
        // Restart recovers current ownership; no old live source reference is needed.
        var recovered = try Store.open(alloc, memory.storage(), "/source-ann-gc", false);
        defer recovered.deinit();
        try std.testing.expectEqual(@as(u64, 1), recovered.statsSnapshot().retained_payloads);
        try std.testing.expectEqual(@as(u64, 1), source.stats.collection_mark_max_step_rows);
    }
}

test "source vector payloads adaptive segments retain mixed dimensions through growth and restart" {
    const alloc = std.testing.allocator;
    const keys = @import("internal_keys.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var source = try Store.open(alloc, memory.storage(), "/adaptive-source", false);
    defer source.deinit();
    source.append_only = false;
    source.selective_gc = false;
    source.segment_sizing = .{ .target_bytes = 512, .min_shards = 1, .max_shards = 32 };
    const key_a = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-a");
    defer alloc.free(key_a);
    const key_b = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-b");
    defer alloc.free(key_b);
    const a = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
    defer alloc.free(a);
    var medium: [128]f32 = @splat(0.125);
    const b = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &medium);
    defer alloc.free(b);
    const first = try payload.Session.create(alloc, source.interface());
    const ref_a = try payload.Reference.decode(try first.put(key_a, a));
    const ref_b = try payload.Reference.decode(try first.put(key_b, b));
    try first.prepareCommit();
    first.committed = true;
    first.release();
    var old_reader = try source.snapshot(alloc);
    defer old_reader.deinit();
    try source.checkpoint();
    const first_shards = source.opened.store.manifest.?.shard_count;
    try std.testing.expect(first_shards < 128);
    var wide: [1024]f32 = @splat(0.75);
    const replacement = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &wide);
    defer alloc.free(replacement);
    const update = try payload.Session.create(alloc, source.interface());
    const ref_new = try payload.Reference.decode(try update.put(key_b, replacement));
    try update.prepareCommit();
    update.committed = true;
    update.release();
    try source.checkpoint();
    try std.testing.expect(source.opened.store.manifest.?.shard_count > first_shards);
    try std.testing.expectEqual(@as(u32, 128), (try old_reader.get(&ref_b.digest, std.math.maxInt(u64), 1)).vector.dims);
    var reopened = try Store.open(alloc, memory.storage(), "/adaptive-source", false);
    defer reopened.deinit();
    try std.testing.expectEqual(@as(u64, 3), reopened.stats.retained_payloads);
    try std.testing.expectEqual(@as(u32, 3), (try reopened.opened.get(&ref_a.digest, std.math.maxInt(u64), 1)).vector.dims);
    try std.testing.expectEqual(@as(u32, 128), (try reopened.opened.get(&ref_b.digest, std.math.maxInt(u64), 1)).vector.dims);
    const current = (try reopened.opened.get(&ref_new.digest, std.math.maxInt(u64), 1)).vector;
    var scratch: [1024]f32 = undefined;
    try std.testing.expectEqualSlices(f32, &wide, try current.decodeExactInto(&scratch));
}

test "source vector payloads incremental collection retains updates retries deletes and old readers" {
    for ([_]usize{ 0, 1, 2 }) |mark_rows| {
        const alloc = std.testing.allocator;
        const docs = @import("docstore.zig");
        const backend_mod = @import("lsm_backend.zig");
        const keys = @import("internal_keys.zig");
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var backend = try backend_mod.Backend.open(alloc, "/incremental-primary", .{ .storage = memory.storage() });
        defer backend.close();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        var source = try Store.open(alloc, memory.storage(), "/incremental", false);
        defer source.deinit();
        source.mark_step_rows = @min(mark_rows, 1);
        source.mark_outside_lock = mark_rows == 2;
        source.append_only = false;
        source.selective_gc = false;
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-a");
        defer alloc.free(key);
        const other = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-b");
        defer alloc.free(other);
        const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
        defer alloc.free(first);
        const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 4, 5, 6, 7 });
        defer alloc.free(second);
        try store.put(key, first);
        try store.put(key, second);
        // The first version is now an orphan, excluded from the mark.
        try std.testing.expect(!try source.collectStep(&raw, 1));
        try std.testing.expect(source.collectionPending());
        try store.put(key, second); // retry before the scan reaches its existing owner
        try store.put(key, first); // retry resurrects an excluded digest
        try store.put(other, second); // independent model with another dimension
        var old = try store.beginReadTxn();
        var old_active = true;
        defer if (old_active) old.abort();
        var mutation = try store.beginWriteTxn();
        try mutation.delete(key);
        try mutation.commit();
        var passes: usize = 0;
        while (!try source.collectStep(&raw, 1)) : (passes += 1) {
            try std.testing.expect(passes < 1024);
        }
        try std.testing.expectEqual(@as(u64, 3), source.stats.retained_payloads);
        try std.testing.expectEqual(@as(u64, 44), source.stats.retained_payload_bytes);
        try std.testing.expectEqualSlices(u8, first, try old.get(key));
        try std.testing.expectError(error.NotFound, store.get(alloc, key));
        const value = try store.get(alloc, other);
        defer alloc.free(value);
        try std.testing.expectEqualSlices(u8, second, value);
        // A post-cut reader pins both pre-cut live objects and all WAL suffixes.
        try std.testing.expect(!try source.collectStep(&raw, std.math.maxInt(u64)));
        old.abort();
        old_active = false;
        if (mark_rows != 0) try std.testing.expectEqual(@as(u64, 1), source.stats.collection_mark_max_step_rows);
        source.mark_step_rows = 0;
        try std.testing.expect(try source.collectStep(&raw, std.math.maxInt(u64)));
        try std.testing.expectEqual(@as(u64, 1), source.stats.retained_payloads);
        try std.testing.expectEqual(@as(u64, 16), source.stats.retained_payload_bytes);
        var reopened = try Store.open(alloc, memory.storage(), "/incremental", false);
        defer reopened.deinit();
        try std.testing.expectEqual(@as(u64, 1), reopened.stats.retained_payloads);
        const ref = try payload.Reference.forArtifact(other, second);
        const restored = try Store.resolve(&reopened, alloc, other, ref);
        defer alloc.free(restored);
        try std.testing.expectEqualSlices(u8, second, restored);
    }
}

test "source vector payloads abandoned incremental copy leaves recoverable authority" {
    for ([_]usize{ 0, 1, 2 }) |mark_rows| {
        const alloc = std.testing.allocator;
        const docs = @import("docstore.zig");
        const mem = @import("mem_backend.zig");
        const keys = @import("internal_keys.zig");
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var backend = mem.Backend.init(alloc, .{});
        defer backend.close();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
        defer alloc.free(key);
        const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
        defer alloc.free(first);
        const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 4, 5, 6 });
        defer alloc.free(second);
        {
            var source = try Store.open(alloc, memory.storage(), "/abandoned-copy", false);
            defer source.deinit();
            source.mark_step_rows = @min(mark_rows, 1);
            source.mark_outside_lock = mark_rows == 2;
            source.append_only = false;
            source.selective_gc = false;
            var store = try docs.DocStore.openRuntime(alloc, &raw);
            defer store.close();
            store.payload_store = source.interface();
            try store.put(key, first);
            try store.put(key, second);
            try std.testing.expect(!try source.collectStep(&raw, 1));
            try store.put(key, first);
            // DB shutdown cancels the primary snapshot before closing core.
            source.cancelMarking();
            try std.testing.expect(source.marking == null);
            // No new CURRENT was published; reopen must use the old base+WAL.
        }
        var source = try Store.open(alloc, memory.storage(), "/abandoned-copy", false);
        defer source.deinit();
        source.append_only = false;
        source.selective_gc = false;
        try std.testing.expect(try source.collectStep(&raw, std.math.maxInt(u64)));
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        const restored = try store.get(alloc, key);
        defer alloc.free(restored);
        try std.testing.expectEqualSlices(u8, first, restored);
        try std.testing.expectEqual(@as(u64, 1), source.stats.retained_payloads);
    }
}

test "source vector payloads checkpoint receipt invalidates on delete and source preparation" {
    const alloc = std.testing.allocator;
    const docs = @import("docstore.zig");
    const mem = @import("mem_backend.zig");
    const keys = @import("internal_keys.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var source = try Store.open(alloc, memory.storage(), "/receipt", false);
    defer source.deinit();
    source.checkpoint_receipts = true;
    var store = try docs.DocStore.openRuntime(alloc, &raw);
    defer store.close();
    store.payload_store = source.interface();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
    defer alloc.free(artifact);
    try store.put(key, artifact);
    try std.testing.expect(try source.collectStep(&raw, std.math.maxInt(u64)));
    const collections = source.stats.collections;
    try std.testing.expect(try source.loadCheckpointReceipt());
    try std.testing.expect(try source.collectStep(&raw, std.math.maxInt(u64)));
    try std.testing.expectEqual(collections, source.stats.collections);
    // Aborting a deletion must leave the epoch and checkpoint proof intact.
    var aborted = try store.beginWriteTxn();
    try aborted.delete(key);
    aborted.abort();
    try std.testing.expect(try source.collectStep(&raw, std.math.maxInt(u64)));
    try std.testing.expectEqual(collections, source.stats.collections);
    var txn = try store.beginWriteTxn();
    try txn.delete(key);
    try txn.commit();
    try std.testing.expect(try source.collectStep(&raw, std.math.maxInt(u64)));
    try std.testing.expectEqual(@as(u64, 0), source.stats.retained_payloads);
    try store.put(key, artifact);
    try std.testing.expect(!try source.loadCheckpointReceipt());
    try std.testing.expect(try source.collectStep(&raw, std.math.maxInt(u64)));
    try std.testing.expectEqual(@as(u64, 1), source.stats.retained_payloads);
    try memory.storage().writeFileAbsolute("/receipt/SOURCE_CHECKPOINT", "corrupt");
    try std.testing.expect(!try source.loadCheckpointReceipt());
}

test "source vector payloads incremental collection fences ambiguous CURRENT and recovers" {
    @import("../test_error_logs.zig").expectErrorLogs(1);
    const alloc = std.testing.allocator;
    const docs = @import("docstore.zig");
    const mem = @import("mem_backend.zig");
    const keys = @import("internal_keys.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var source = try Store.open(alloc, memory.storage(), "/ambiguous-collection", false);
    defer source.deinit();
    source.append_only = false;
    source.selective_gc = false;
    var store = try docs.DocStore.openRuntime(alloc, &raw);
    defer store.close();
    store.payload_store = source.interface();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
    defer alloc.free(first);
    const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 4, 5, 6 });
    defer alloc.free(second);
    try store.put(key, first);
    try store.put(key, second);
    try std.testing.expect(!try source.collectStep(&raw, 1));
    try store.put(key, first);
    generation_publication.injectPostPublishFailuresForTest(2);
    defer generation_publication.injectPostPublishFailuresForTest(0);
    try std.testing.expectError(error.GenerationPublicationDurabilityUncertain, source.collectStep(&raw, std.math.maxInt(u64)));
    try std.testing.expect(source.poisoned);
    try std.testing.expectError(error.VectorPayloadStorePoisoned, store.get(alloc, key));
    var reopened = try Store.open(alloc, memory.storage(), "/ambiguous-collection", false);
    defer reopened.deinit();
    try std.testing.expect(try reopened.collectStep(&raw, std.math.maxInt(u64)));
    const ref = try payload.Reference.forArtifact(key, first);
    const value = try Store.resolve(&reopened, alloc, key, ref);
    defer alloc.free(value);
    try std.testing.expectEqualSlices(u8, first, value);
}

test "source vector payloads delete-only ambiguous primary commit fences collection" {
    const alloc = std.testing.allocator;
    const docs = @import("docstore.zig");
    const mem = @import("mem_backend.zig");
    const keys = @import("internal_keys.zig");
    for ([_]bool{ false, true }) |committed| {
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var backend = mem.Backend.init(alloc, .{});
        defer backend.close();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
        defer alloc.free(key);
        const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
        defer alloc.free(artifact);
        {
            var source = try Store.open(alloc, memory.storage(), "/ambiguous-delete", false);
            defer source.deinit();
            var store = try docs.DocStore.openRuntime(alloc, &raw);
            defer store.close();
            store.payload_store = source.interface();
            try store.put(key, artifact);
            var txn = try store.beginWriteTxn();
            try txn.delete(key);
            try txn.payload_session.?.stageReferenceEpoch(&txn);
            txn.payload_session.?.primary_commit_attempted = true;
            if (committed) {
                try txn.write.?.commit();
                txn.write = null;
            }
            txn.abort();
            try std.testing.expectEqual(@as(u64, 1), source.stats.unresolved_primary_commits);
            try std.testing.expect(!try source.collectStep(&raw, std.math.maxInt(u64)));
        }
        var reopened = try Store.open(alloc, memory.storage(), "/ambiguous-delete", false);
        defer reopened.deinit();
        try std.testing.expect(try reopened.collectStep(&raw, std.math.maxInt(u64)));
        try std.testing.expectEqual(@as(u64, if (committed) 0 else 1), reopened.stats.retained_payloads);
        var txn = try raw.beginRead();
        defer txn.abort();
        try std.testing.expectEqual(@as(u64, if (committed) 2 else 1), try Store.primaryReferenceEpoch(&txn));
    }
}

test "source vector payloads writable startup reclaims abandoned temporary outputs" {
    const alloc = std.testing.allocator;
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    {
        var source = try Store.open(alloc, memory.storage(), "/temporary-outputs", false);
        defer source.deinit();
    }
    for ([_][]const u8{ "block-2-0.afvb.tmp-123", "wal-2.afvw.tmp-456", "SOURCE_CHECKPOINT.tmp-9", "CURRENT.tmp-7" }) |name| {
        const path = try std.fs.path.join(alloc, &.{ "/temporary-outputs", name });
        defer alloc.free(path);
        try memory.storage().writeFileAbsolute(path, "unpublished bytes");
    }
    try memory.storage().writeFileAbsolute("/temporary-outputs/notes.tmp-1", "unrelated");
    {
        var readonly = try Store.open(alloc, memory.storage(), "/temporary-outputs", true);
        defer readonly.deinit();
        try std.testing.expectEqual(@as(u64, 17), try memory.storage().fileSize("/temporary-outputs/block-2-0.afvb.tmp-123"));
    }
    var reopened = try Store.open(alloc, memory.storage(), "/temporary-outputs", false);
    defer reopened.deinit();
    for ([_][]const u8{ "block-2-0.afvb.tmp-123", "wal-2.afvw.tmp-456", "SOURCE_CHECKPOINT.tmp-9", "CURRENT.tmp-7" }) |name| {
        const path = try std.fs.path.join(alloc, &.{ "/temporary-outputs", name });
        defer alloc.free(path);
        try std.testing.expectError(error.FileNotFound, memory.storage().fileSize(path));
    }
    try std.testing.expectEqual(@as(u64, 9), try memory.storage().fileSize("/temporary-outputs/notes.tmp-1"));
}

test "source vector payloads append segments beyond legacy chain and ignore stale directory hints" {
    const alloc = std.testing.allocator;
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var source = try Store.open(alloc, memory.storage(), "/append-source", false);
    defer source.deinit();
    source.append_only = true;
    if (source.directory == null) source.directory = try @import("source_location_directory.zig").Directory.create(alloc);
    source.opened.source_directory = source.directory;
    var first: payload.Reference = undefined;
    for (0..70) |i| {
        const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, i, &.{ @floatFromInt(i), 0.25 });
        defer alloc.free(artifact);
        const ref = try payload.Reference.forArtifact("model-a", artifact);
        if (i == 0) first = ref;
        try Store.prepare(&source, &.{.{ .reference = ref, .artifact = artifact }});
        try source.checkpoint();
    }
    try std.testing.expectEqual(@as(?u64, 0), source.opened.baseVectorCount());
    try std.testing.expectEqual(@as(usize, 70), source.opened.readers.len);
    try std.testing.expectEqual(@as(u64, 2), source.opened.readers[0].generation);
    // A directory hint is disposable even when its checksum is intact.
    try source.directory.?.entries.put(first.digest, .{ .generation = 99999, .shard = 0 });
    const found = try source.opened.get(&first.digest, std.math.maxInt(u64), 1);
    try std.testing.expect(found == .vector);
    try source.directory.?.save(memory.storage(), "/append-source");
    var reopened = try Store.open(alloc, memory.storage(), "/append-source", true);
    defer reopened.deinit();
    const after = try reopened.opened.get(&first.digest, std.math.maxInt(u64), 1);
    try std.testing.expect(after == .vector);
    const current = try memory.storage().readFileAlloc(alloc, "/append-source/CURRENT", 1024 * 1024);
    defer alloc.free(current);
    try std.testing.expectEqual(@as(u16, 6), std.mem.readInt(u16, current[8..10], .big));
}

test "source vector payloads selective collection preserves cold files old leases and post cut changes" {
    for ([_]usize{ 0, 1, 2 }) |mark_rows| {
        const alloc = std.testing.allocator;
        const mem = @import("mem_backend.zig");
        const docs = @import("docstore.zig");
        const keys = @import("internal_keys.zig");
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var source = try Store.open(alloc, memory.storage(), "/selective-source", false);
        defer source.deinit();
        source.mark_step_rows = @min(mark_rows, 1);
        source.mark_outside_lock = mark_rows == 2;
        source.append_only = true;
        source.coalesce_directory = mark_rows != 0;
        source.selective_gc = true;
        if (source.directory == null) source.directory = try @import("source_location_directory.zig").Directory.create(alloc);
        source.opened.source_directory = source.directory;
        var backend = mem.Backend.init(alloc, .{});
        defer backend.close();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
        defer alloc.free(first);
        const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 4, 5, 6 });
        defer alloc.free(second);
        var selected_keys: [4][]u8 = undefined;
        var count: usize = 0;
        defer for (selected_keys[0..count]) |key| alloc.free(key);
        var attempt: usize = 0;
        // Force a mixed live/dead segment so budget=1 suspends actual copying.
        while (count < 4) : (attempt += 1) {
            var doc_buf: [64]u8 = undefined;
            const doc = try std.fmt.bufPrint(&doc_buf, "doc-{d}", .{attempt});
            const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc, "model-a");
            const ref = try payload.Reference.forArtifact(key, first);
            if (vector_block.keyHash(&ref.digest) & 127 != 0) {
                alloc.free(key);
                continue;
            }
            selected_keys[count] = key;
            count += 1;
            try store.put(key, first);
        }
        try source.checkpoint();
        const old_ref = try payload.Reference.forArtifact(selected_keys[0], first);
        var old = try source.snapshot(alloc);
        defer old.deinit();
        try store.put(selected_keys[0], second);
        try store.put(selected_keys[1], second);
        try source.checkpoint();
        const cold_generation = source.opened.store.manifest.?.latest_generation;
        try std.testing.expect(!try source.collectStep(&raw, 1));
        try store.put(selected_keys[2], second);
        try store.delete(selected_keys[3]);
        while (!try source.collectStep(&raw, 1)) {}
        var retained_cold = false;
        for (source.opened.readers) |reader| if (reader.generation == cold_generation) {
            retained_cold = true;
        };
        try std.testing.expect(retained_cold);
        try std.testing.expect((try old.get(&old_ref.digest, std.math.maxInt(u64), 1)) == .vector);
        for (selected_keys[0..3]) |key| {
            const value = try store.get(alloc, key);
            defer alloc.free(value);
            try std.testing.expectEqualSlices(u8, second, value);
        }
        try std.testing.expectError(error.NotFound, store.get(alloc, selected_keys[3]));
        for (0..1024) |_| {
            if (source.stats.retained_payloads == 3) break;
            _ = try source.collect(&raw);
        }
        try std.testing.expectEqual(@as(u64, 3), source.stats.retained_payloads);
        try std.testing.expect((try old.get(&old_ref.digest, std.math.maxInt(u64), 1)) == .vector);
        if (mark_rows != 0) {
            try std.testing.expectEqual(@as(u64, 1), source.stats.collection_mark_max_step_rows);
            try std.testing.expect(source.directory.?.publication_deferrals > 0);
            try std.testing.expectEqual(@as(u64, 0), source.directory.?.bytes_written);
            try std.testing.expectEqual(@as(usize, 3), source.directory.?.entries.count());
        }
    }
}

test "source vector payloads group commit batches waiting preparations without acknowledging early" {
    const alloc = std.testing.allocator;
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var source = try Store.open(alloc, memory.storage(), "/group-source", false);
    defer source.deinit();
    source.group_commit = true;
    source.prepare_running = true;
    const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
    defer alloc.free(artifact);
    const Context = struct {
        source: *Store,
        item: payload.Prepared,
        done: std.atomic.Value(bool) = .init(false),
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            Store.prepare(self.source, &.{self.item}) catch |err| {
                self.err = err;
            };
            self.done.store(true, .release);
        }
    };
    var contexts: [8]Context = undefined;
    var threads: [8]std.Thread = undefined;
    source.lock();
    var locked = true;
    defer if (locked) source.mutex.unlock();
    var spawned: usize = 0;
    defer {
        if (locked) {
            source.queueLock();
            if (source.prepare_head) |head| head.state.store(1, .release);
            source.prepare_queue_mutex.unlock();
            source.mutex.unlock();
            locked = false;
        }
        for (threads[0..spawned]) |thread| thread.join();
    }
    for (&contexts, 0..) |*ctx, i| {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "key-{d}", .{i});
        ctx.* = .{ .source = &source, .item = .{ .reference = try payload.Reference.forArtifact(key, artifact), .artifact = artifact } };
        threads[i] = try std.Thread.spawn(.{}, Context.run, .{ctx});
        spawned += 1;
    }
    // Elect a leader only once every request has queued, then release the
    // writer boundary. No preparation can be acknowledged before this point.
    const deadline = time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        source.queueLock();
        var queued: usize = 0;
        var request = source.prepare_head;
        while (request) |part| : (request = part.next) queued += 1;
        source.prepare_queue_mutex.unlock();
        if (queued == 8) break;
        if (time.monotonicNs() > deadline) return error.TestUnexpectedResult;
        std.Thread.yield() catch {};
    }
    for (&contexts) |*ctx| try std.testing.expect(!ctx.done.load(.acquire));
    source.queueLock();
    source.prepare_head.?.state.store(1, .release);
    source.prepare_queue_mutex.unlock();
    source.mutex.unlock();
    locked = false;
    for (threads) |thread| thread.join();
    spawned = 0;
    for (&contexts) |*ctx| {
        if (ctx.err) |err| return err;
        try std.testing.expect((try source.opened.get(&ctx.item.reference.digest, std.math.maxInt(u64), 1)) == .vector);
    }
    try std.testing.expect(source.stats.prepare_batches <= 2);
    try std.testing.expectEqual(@as(u64, 8), source.stats.prepared_payloads);
}

test "source vector payloads concurrent lease retirement preserves shared budget accounting" {
    const alloc = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(alloc);
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    {
        var source = try Store.openManaged(alloc, &manager, memory.storage(), "/lease-retirement", false);
        defer source.deinit();
        const Retire = struct {
            fn run(lease: *native.Opened) void {
                lease.deinit();
            }
        };
        for (0..50) |i| {
            const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, i, &.{ @floatFromInt(i), 0.25 });
            defer alloc.free(artifact);
            const ref = try payload.Reference.forArtifact("model-a", artifact);
            var lease = try source.snapshot(alloc);
            const thread = try std.Thread.spawn(.{}, Retire.run, .{&lease});
            defer thread.join();
            try Store.prepare(&source, &.{.{ .reference = ref, .artifact = artifact }});
            if (i % 10 == 0) try source.checkpoint();
        }
        try std.testing.expect(source.statsSnapshot().heap_bytes > 0);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "source vector payloads selective collection cannot starve sparse garbage behind empty segments" {
    for ([_]usize{ 0, 1, 2 }) |mark_rows| {
        const alloc = std.testing.allocator;
        const mem = @import("mem_backend.zig");
        const docs = @import("docstore.zig");
        const keys = @import("internal_keys.zig");
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var source = try Store.open(alloc, memory.storage(), "/sparse-garbage", false);
        defer source.deinit();
        source.append_only = true;
        source.selective_gc = true;
        source.coalesce_directory = true;
        source.mark_step_rows = @min(mark_rows, 1);
        source.mark_outside_lock = mark_rows == 2;
        var backend = mem.Backend.init(alloc, .{});
        defer backend.close();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
        defer alloc.free(artifact);
        var selected_keys: [8][]u8 = undefined;
        var count: usize = 0;
        defer for (selected_keys[0..count]) |key| alloc.free(key);
        var attempt: usize = 0;
        // One obsolete row in this segment is below the 25% selection threshold.
        while (count < selected_keys.len) : (attempt += 1) {
            var buffer: [64]u8 = undefined;
            const doc = try std.fmt.bufPrint(&buffer, "cold-{d}", .{attempt});
            const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc, "model-a");
            const ref = try payload.Reference.forArtifact(key, artifact);
            if (vector_block.keyHash(&ref.digest) & 127 != 0) {
                alloc.free(key);
                continue;
            }
            selected_keys[count] = key;
            count += 1;
            try store.put(key, artifact);
        }
        try source.checkpoint();
        const gone = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "gone", "model-a");
        defer alloc.free(gone);
        try store.put(gone, artifact);
        try source.checkpoint();
        var old = try source.snapshot(alloc);
        defer old.deinit();
        const old_ref = try payload.Reference.forArtifact(selected_keys[0], artifact);
        try store.delete(gone);
        while (!try source.collectStep(&raw, std.math.maxInt(u64))) {}
        try std.testing.expectEqual(@as(u64, 8), source.stats.retained_payloads);
        try std.testing.expectEqual(@as(u64, 8), source.stats.live_payloads_at_collection);
        try std.testing.expectEqual(@as(u64, 12), source.stats.unreferenced_payload_bytes_at_collection);
        var has_empty = false;
        for (source.opened.readers) |reader| if (reader.count == 0) {
            has_empty = true;
        };
        try std.testing.expect(has_empty);
        try store.delete(selected_keys[0]);
        while (!try source.collectStep(&raw, std.math.maxInt(u64))) {}
        // An empty segment must not suppress selection of the best nonempty
        // segment when all remaining garbage is below the density threshold.
        try std.testing.expectEqual(@as(u64, 7), source.stats.retained_payloads);
        try std.testing.expectEqual(@as(u64, 7), source.stats.live_payloads_at_collection);
        try std.testing.expectEqual(@as(u64, 12), source.stats.unreferenced_payload_bytes_at_collection);
        try std.testing.expect((try old.get(&old_ref.digest, std.math.maxInt(u64), 1)) == .vector);
        while (!try source.collectStep(&raw, std.math.maxInt(u64))) {}
        try std.testing.expectEqual(@as(u64, 0), source.stats.unreferenced_payload_bytes_at_collection);
        for (selected_keys[1..]) |key| {
            const value = try store.get(alloc, key);
            defer alloc.free(value);
            try std.testing.expectEqualSlices(u8, artifact, value);
        }
    }
}

test "source vector payloads unlocked marking admits writers fences cancellation and reconciles retries" {
    for ([_]bool{ false, true }) |planning_phase| {
        for ([_]bool{ false, true }) |rescue| {
            for ([_]enum { finish, cancel, ambiguous, poisoned }{ .finish, .cancel, .ambiguous, .poisoned }) |outcome| {
                const alloc = std.testing.allocator;
                const docs = @import("docstore.zig");
                const backend_mod = @import("lsm_backend.zig");
                const keys = @import("internal_keys.zig");
                var memory = lsm.MemoryStorage.init(alloc);
                defer memory.deinit();
                var backend = try backend_mod.Backend.open(alloc, "/unlocked-primary", .{ .storage = memory.storage() });
                defer backend.close();
                var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
                defer raw.deinit();
                var source = try Store.open(alloc, memory.storage(), "/unlocked-source", false);
                defer source.deinit();
                source.mark_outside_lock = true;
                source.incremental_planning = planning_phase;
                source.rescue_reappends = rescue;
                source.mark_step_rows = 1;
                source.append_only = true;
                source.selective_gc = true;
                var store = try docs.DocStore.openRuntime(alloc, &raw);
                defer store.close();
                store.payload_store = source.interface();
                const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "a", "model-a");
                defer alloc.free(key);
                const other = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "z", "model-b");
                defer alloc.free(other);
                const old_value = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
                defer alloc.free(old_value);
                const current = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 4, 5, 6, 7 });
                defer alloc.free(current);
                try store.put(key, old_value);
                try store.put(key, current);
                try store.put(other, current);
                const known = try payload.Reference.forArtifact(key, current);
                try std.testing.expect(!try source.collectStepDeferredMark(&raw, 1));
                while (!source.marking.?.live.contains(known.digest)) try source.advanceMarkingSnapshot();
                if (planning_phase) {
                    while (true) {
                        var classified: u64 = 0;
                        for (source.marking.?.segment_stats.?) |stats| classified += stats.total;
                        if (classified != 0) break;
                        try source.advanceMarkingSnapshot();
                    }
                }
                try std.testing.expect(!source.marking.?.scan_done);
                var old_reader = try store.beginReadTxn();
                var reader_active = true;
                defer if (reader_active) old_reader.abort();
                MarkInterleaving.entered.store(false, .release);
                MarkInterleaving.resume_scan.store(false, .release);
                MarkInterleaving.cancel_entered.store(false, .release);
                MarkInterleaving.cancel_done.store(false, .release);
                MarkInterleaving.scan_error = null;
                source.mark_test_hook = MarkInterleaving.pause;
                const thread = try std.Thread.spawn(.{}, MarkInterleaving.scan, .{&source});
                var joined = false;
                defer if (!joined) {
                    MarkInterleaving.resume_scan.store(true, .release);
                    thread.join();
                };
                try MarkInterleaving.awaitFlag(&MarkInterleaving.entered);
                // Fail promptly if a regression keeps the foreground lock held.
                const admitted = source.mutex.tryLock();
                if (admitted) source.mutex.unlock();
                try std.testing.expect(admitted);
                const generation = source.currentGeneration();
                try source.advanceMarkingSnapshot(); // no parallel access to cursor/live
                try std.testing.expect(source.statsSnapshot().collection_mark_busy_deferrals > 0);
                const wal_before_retry = source.statsSnapshot().wal_bytes_written;
                try store.put(key, current); // already scanned: reconcile pending tail
                try store.put(key, old_value); // resurrect an orphan after the cut
                if (rescue) {
                    try std.testing.expectEqual(wal_before_retry, source.statsSnapshot().wal_bytes_written);
                    try std.testing.expectEqual(@as(u64, 2), source.statsSnapshot().deduplicated_reappend_payloads);
                } else try std.testing.expect(source.statsSnapshot().wal_bytes_written > wal_before_retry);
                try store.delete(other);
                try source.checkpoint(); // cannot retire the scanner's source cut
                try std.testing.expectEqual(generation, source.currentGeneration());
                try source.setAnnScopes(&.{123}); // scanner owns the old scope snapshot
                var cancellation: ?std.Thread = null;
                defer if (cancellation) |cancel_thread| {
                    MarkInterleaving.resume_scan.store(true, .release);
                    cancel_thread.join();
                };
                if (outcome == .cancel) {
                    cancellation = try std.Thread.spawn(.{}, MarkInterleaving.cancel, .{&source});
                    try MarkInterleaving.awaitFlag(&MarkInterleaving.cancel_entered);
                    try std.testing.expect(!MarkInterleaving.cancel_done.load(.acquire));
                } else if (outcome == .ambiguous) {
                    Store.unresolvedCommit(&source);
                } else if (outcome == .poisoned) {
                    source.lock();
                    source.poisoned = true;
                    source.mutex.unlock();
                }
                MarkInterleaving.resume_scan.store(true, .release);
                thread.join();
                joined = true;
                source.mark_test_hook = null;
                if (cancellation) |cancel_thread| {
                    cancel_thread.join();
                    cancellation = null;
                    try std.testing.expect(source.marking == null);
                }
                if (outcome == .poisoned) {
                    try std.testing.expectEqual(error.VectorPayloadStorePoisoned, MarkInterleaving.scan_error.?);
                    try std.testing.expect(source.marking == null);
                } else {
                    try std.testing.expect(MarkInterleaving.scan_error == null);
                    if (source.marking) |marking| try std.testing.expect(!marking.tail.contains(known.digest));
                }
                if (outcome == .finish) {
                    while (!try source.collectStep(&raw, 1)) {}
                    try std.testing.expectEqual(@as(u64, 3), source.statsSnapshot().retained_payloads);
                } else if (outcome == .ambiguous) {
                    try std.testing.expect(!try source.collectStep(&raw, std.math.maxInt(u64)));
                    try std.testing.expectEqual(generation, source.currentGeneration());
                }
                if (outcome == .poisoned) {
                    try std.testing.expectError(error.VectorPayloadStorePoisoned, old_reader.get(other));
                } else try std.testing.expectEqualSlices(u8, current, try old_reader.get(other));
                old_reader.abort();
                reader_active = false;
                source.cancelMarking();
                var reopened = try Store.open(alloc, memory.storage(), "/unlocked-source", false);
                defer reopened.deinit();
                while (!try reopened.collectStep(&raw, std.math.maxInt(u64))) {}
                try std.testing.expectEqual(@as(u64, 1), reopened.statsSnapshot().retained_payloads);
                const ref = try payload.Reference.forArtifact(key, old_value);
                const value = try Store.resolve(&reopened, alloc, key, ref);
                defer alloc.free(value);
                try std.testing.expectEqualSlices(u8, old_value, value);
            }
        }
    }
}

test "source vector payloads elapsed marking budget yields before the row cap and completes verification" {
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    const docs = @import("docstore.zig");
    const keys = @import("internal_keys.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var source = try Store.open(alloc, memory.storage(), "/elapsed-mark", false);
    defer source.deinit();
    source.mark_outside_lock = true;
    source.mark_step_rows = 16384;
    source.mark_step_ns = 1;
    source.rescue_reappends = true;
    // Ensure the one-nanosecond deadline has elapsed before scanning. A fast
    // optimized scan can otherwise finish verification within one clock tick,
    // leaving no partial verification for the concurrent-retry assertion.
    const expire_budget = struct {
        fn hook(_: *Store) void {
            const started = time.monotonicNs();
            while (time.monotonicNs() == started) std.atomic.spinLoopHint();
        }
    }.hook;
    source.mark_test_hook = expire_budget;
    var store = try docs.DocStore.openRuntime(alloc, &raw);
    defer store.close();
    store.payload_store = source.interface();
    const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
    defer alloc.free(artifact);
    for (0..8) |i| {
        var buf: [32]u8 = undefined;
        const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, try std.fmt.bufPrint(&buf, "doc-{d}", .{i}), "model");
        defer alloc.free(key);
        try store.put(key, artifact);
    }
    var steps: usize = 0;
    var retried_during_verification = false;
    while (!try source.collectStep(&raw, std.math.maxInt(u64))) : (steps += 1) {
        try std.testing.expect(steps < 256);
        if (!retried_during_verification and source.marking.?.verified_bytes > 0) {
            const verified_before = source.marking.?.verified_bytes;
            const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc-0", "model");
            defer alloc.free(key);
            const ref = try payload.Reference.forArtifact(key, artifact);
            MarkInterleaving.entered.store(false, .release);
            MarkInterleaving.resume_scan.store(false, .release);
            MarkInterleaving.scan_error = null;
            source.mark_test_hook = MarkInterleaving.pause;
            const thread = try std.Thread.spawn(.{}, MarkInterleaving.scan, .{&source});
            var joined = false;
            defer if (!joined) {
                MarkInterleaving.resume_scan.store(true, .release);
                thread.join();
            };
            try MarkInterleaving.awaitFlag(&MarkInterleaving.entered);
            const wal_before = source.statsSnapshot().wal_bytes_written;
            try Store.prepare(&source, &.{.{ .reference = ref, .artifact = artifact }});
            try std.testing.expectEqual(wal_before, source.statsSnapshot().wal_bytes_written);
            MarkInterleaving.resume_scan.store(true, .release);
            thread.join();
            joined = true;
            source.mark_test_hook = expire_budget;
            try std.testing.expect(MarkInterleaving.scan_error == null);
            // This digest was already in the immutable cut's live set. A retry
            // must neither rewind verification nor invalidate the cut receipt.
            try std.testing.expect(source.marking.?.verified_bytes > verified_before);
            try std.testing.expect(!source.marking.?.rescued_any);
            retried_during_verification = true;
        }
    }
    try std.testing.expect(retried_during_verification);
    const stats = source.statsSnapshot();
    try std.testing.expect(stats.collection_mark_budget_yields > 0);
    try std.testing.expect(stats.collection_mark_max_step_rows < 16384);
    try std.testing.expect(stats.collection_mark_outside_lock_ns > 0);
    try std.testing.expectEqual(@as(u64, 8), stats.live_payloads_at_collection);
    try std.testing.expectEqual(@as(u64, 96), stats.live_payload_bytes_at_collection);
    try std.testing.expectEqual(@as(u64, 0), stats.unreferenced_payload_bytes_at_collection);

    // This phase deliberately requests a new cut after the preceding pass
    // may have certified the unchanged source when receipts are enabled.
    source.receipt = null;
    // Real post-cut additions rule out the all-live shortcut. Do not scan the
    // complete source inventory merely to reject that shortcut afterward.
    try std.testing.expect(!try source.collectStepDeferredMark(&raw, std.math.maxInt(u64)));
    const extra = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "extra", "another-model");
    defer alloc.free(extra);
    try store.put(extra, artifact);
    steps = 0;
    while (!source.marking.?.scan_done) : (steps += 1) {
        try std.testing.expect(steps < 256);
        try source.advanceMarkingSnapshot();
    }
    try std.testing.expectEqual(@as(u64, 0), source.marking.?.verified_bytes);
    try std.testing.expect(!source.marking.?.verification_done);
    try std.testing.expect(try source.collectStepDeferredMark(&raw, std.math.maxInt(u64)));
    try std.testing.expectEqual(@as(u64, 9), source.statsSnapshot().retained_payloads);
    const restored = try store.get(alloc, extra);
    defer alloc.free(restored);
    try std.testing.expectEqualSlices(u8, artifact, restored);
}

test "source vector payloads rescued preparations cannot certify an unchanged primary as fully live" {
    for ([_]bool{ false, true }) |extra_orphan| {
        const alloc = std.testing.allocator;
        const mem = @import("mem_backend.zig");
        const docs = @import("docstore.zig");
        const keys = @import("internal_keys.zig");
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var backend = mem.Backend.init(alloc, .{});
        defer backend.close();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        var source = try Store.open(alloc, memory.storage(), "/rescue-receipt", false);
        defer source.deinit();
        source.rescue_reappends = true;
        source.mark_outside_lock = true;
        source.mark_step_rows = 1;
        source.checkpoint_receipts = true;
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "a", "model");
        defer alloc.free(key);
        const old = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2 });
        defer alloc.free(old);
        const current = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 3, 4 });
        defer alloc.free(current);
        try store.put(key, old);
        try store.put(key, current);
        const ref = try payload.Reference.forArtifact(key, old);
        if (extra_orphan) {
            const unused = try payload.Reference.forArtifact("unused", old);
            try Store.prepare(&source, &.{.{ .reference = unused, .artifact = old }});
        }
        try std.testing.expect(!try source.collectStepDeferredMark(&raw, 1));
        const bytes = source.stats.wal_bytes_written;
        try Store.prepare(&source, &.{.{ .reference = ref, .artifact = old }});
        try std.testing.expectEqual(bytes, source.stats.wal_bytes_written);
        // Do not commit the prepared reference. It remains conservatively live
        // for this cut; a clean receipt for the old epoch would leak it forever.
        while (!try source.collectStep(&raw, 1)) {}
        try std.testing.expectEqual(@as(u64, 2), source.stats.retained_payloads);
        try std.testing.expect(source.receipt.?.primary_epoch == null);
        const rescued = try Store.resolve(&source, alloc, key, ref);
        defer alloc.free(rescued);
        try std.testing.expectEqualSlices(u8, old, rescued);
        while (!try source.collectStep(&raw, 1)) {}
        try std.testing.expectEqual(@as(u64, 1), source.stats.retained_payloads);
        try std.testing.expectError(error.MissingCommittedVectorPayload, Store.resolve(&source, alloc, key, ref));
    }
}

test "source vector payloads active scan scheduling preserves duty and fences" {
    try std.testing.expectEqual(@as(u64, 2_000_000), Store.scanPauseNs(2_000_000, 50));
    try std.testing.expectEqual(@as(u64, 6_000_000), Store.scanPauseNs(2_000_000, 25));
    try std.testing.expectEqual(@as(u64, 100_000), Store.scanPauseNs(2_000_000, 100));
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var source = try Store.open(alloc, memory.storage(), "/scan-duty", false);
    defer source.deinit();
    source.mark_outside_lock = true;
    source.scan_duty_percent = 50;
    source.last_scan_ns = 2_000_000;
    try std.testing.expect(source.activeScanPauseNs() == null);
    try std.testing.expect(!try source.collectStepDeferredMark(&raw, 1));
    try std.testing.expectEqual(@as(?u64, 2_000_000), source.activeScanPauseNs());
    source.marking.?.running = true;
    try std.testing.expect(source.activeScanPauseNs() == null);
    source.marking.?.running = false;
    source.stats.unresolved_primary_commits = 1;
    try std.testing.expect(source.activeScanPauseNs() == null);
    source.stats.unresolved_primary_commits = 0;
    source.marking.?.scan_done = true;
    try std.testing.expect(source.activeScanPauseNs() == null);
    source.cancelMarking();
}

test "source vector payloads failed planning discards the consumed mark before retry" {
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    const docs = @import("docstore.zig");
    const keys = @import("internal_keys.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var source = try Store.open(alloc, memory.storage(), "/plan-failure", false);
    defer source.deinit();
    source.mark_outside_lock = true;
    source.mark_step_rows = 1;
    var store = try docs.DocStore.openRuntime(alloc, &raw);
    defer store.close();
    store.payload_store = source.interface();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "a", "model");
    defer alloc.free(key);
    const old = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2 });
    defer alloc.free(old);
    const current = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 3, 4 });
    defer alloc.free(current);
    try store.put(key, old);
    try store.put(key, current);
    try std.testing.expect(!try source.collectStepDeferredMark(&raw, 1));
    while (!source.marking.?.scan_done) try source.advanceMarkingSnapshot();
    try std.testing.expectEqual(@as(u32, 1), source.marking.?.live.count());
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    {
        source.alloc = failing.allocator();
        defer source.alloc = alloc;
        try std.testing.expectError(error.OutOfMemory, source.collectStepDeferredMark(&raw, 1));
    }
    try std.testing.expect(source.marking == null);
    try std.testing.expect(source.collection == null);
    try std.testing.expect(!source.poisoned);
    // Retry must take a new primary snapshot, not plan from the emptied map.
    while (!try source.collectStep(&raw, 1)) {}
    try std.testing.expectEqual(@as(u64, 1), source.stats.retained_payloads);
    const value = try store.get(alloc, key);
    defer alloc.free(value);
    try std.testing.expectEqualSlices(u8, current, value);
    var reopened = try Store.open(alloc, memory.storage(), "/plan-failure", false);
    defer reopened.deinit();
    const ref = try payload.Reference.forArtifact(key, current);
    const durable = try Store.resolve(&reopened, alloc, key, ref);
    defer alloc.free(durable);
    try std.testing.expectEqualSlices(u8, current, durable);
}

test "source vector payloads shared catalogs preserve old leases across WAL and segment publication" {
    const alloc = std.testing.allocator;
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var source = try Store.open(alloc, memory.storage(), "/shared-catalog", false);
    var source_live = true;
    defer if (source_live) source.deinit();
    source.shared_catalog = true;
    try source.opened.shareSegmentCatalog();
    const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2 });
    defer alloc.free(first);
    const first_ref = try payload.Reference.forArtifact("model-a", first);
    try Store.prepare(&source, &.{.{ .reference = first_ref, .artifact = first }});
    try source.checkpoint();
    var lease = try source.snapshot(alloc);
    defer lease.deinit();
    try std.testing.expect(lease.shared_catalog == source.opened.shared_catalog);
    try std.testing.expect(lease.readers.ptr == source.opened.readers.ptr);
    try std.testing.expect(lease.store.manifest_segments.ptr == source.opened.store.manifest_segments.ptr);
    const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 3, 4, 5 });
    defer alloc.free(second);
    const second_ref = try payload.Reference.forArtifact("model-b", second);
    try Store.prepare(&source, &.{.{ .reference = second_ref, .artifact = second }});
    try std.testing.expect(lease.shared_catalog == source.opened.shared_catalog);
    try std.testing.expect((try lease.get(&second_ref.digest, std.math.maxInt(u64), null)) == .missing);
    try source.checkpoint();
    try std.testing.expect(lease.shared_catalog != source.opened.shared_catalog);
    source.deinit();
    source_live = false;
    // Source hints are table-owned; this raw lease test has no directory.
    lease.source_directory = null;
    lease.reference_location_cache = null;
    const old = try lease.get(&first_ref.digest, std.math.maxInt(u64), null);
    var decoded: [2]f32 = undefined;
    try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, try old.vector.decodeInto(&decoded));
    var reopened = try Store.open(alloc, memory.storage(), "/shared-catalog", false);
    defer reopened.deinit();
    const current = try Store.resolve(&reopened, alloc, "model-b", second_ref);
    defer alloc.free(current);
    try std.testing.expectEqualSlices(u8, second, current);
}

test "source vector payloads shared catalog allocation failures preserve original owners" {
    const alloc = std.testing.allocator;
    for (0..2) |fail_index| {
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var source = try Store.open(alloc, memory.storage(), "/catalog-failure", false);
        defer source.deinit();
        // Environment-enabled sources already own a catalog; a fresh native
        // open gives this test the unshared ownership transition explicitly.
        var opened = try native.Store.openWithBlocks(alloc, memory.storage(), "/catalog-failure");
        defer opened.deinit();
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = fail_index });
        opened.store.alloc = failing.allocator();
        const outcome = opened.shareSegmentCatalog();
        opened.store.alloc = alloc;
        try std.testing.expectError(error.OutOfMemory, outcome);
        try std.testing.expect(opened.shared_catalog == null);
        try std.testing.expect(opened.store.shared_manifest == null);
        try opened.shareSegmentCatalog();
        var failed_clone = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
        try std.testing.expectError(error.OutOfMemory, opened.clone(failed_clone.allocator()));
        var clone = try opened.clone(alloc);
        defer clone.deinit();
        try std.testing.expect(clone.shared_catalog == opened.shared_catalog);
    }
}

test "source vector payloads incremental inventory matches full inventory after duplicate rescue updates and deletes" {
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    const docs = @import("docstore.zig");
    const keys = @import("internal_keys.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var source = try Store.open(alloc, memory.storage(), "/incremental-inventory", false);
    defer source.deinit();
    source.incremental_inventory = true;
    source.append_only = true;
    source.selective_gc = true;
    source.mark_outside_lock = true;
    source.mark_step_rows = 1;
    source.rescue_reappends = false;
    if (!source.inventory.initialized) try source.inventory.sync(alloc, null, &source.opened, &source.stats.inventory_rows_scanned);
    var store = try docs.DocStore.openRuntime(alloc, &raw);
    defer store.close();
    store.payload_store = source.interface();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-a");
    defer alloc.free(key);
    const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2 });
    defer alloc.free(first);
    const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 3, 4, 5 });
    defer alloc.free(second);
    const first_ref = try payload.Reference.forArtifact(key, first);
    try store.put(key, first);
    try source.checkpoint();
    try store.put(key, second);
    try std.testing.expect(!try source.collectStepDeferredMark(&raw, 1));
    // The abandoned old version occurs in a segment and in the post-cut WAL.
    try Store.prepare(&source, &.{.{ .reference = first_ref, .artifact = first }});
    for (0..3) |_| {
        var steps: usize = 0;
        while (!try source.collectStep(&raw, 1)) : (steps += 1) try std.testing.expect(steps < 2048);
        const expected_count = source.stats.retained_payloads;
        const expected_bytes = source.stats.retained_payload_bytes;
        try source.inventoryRetainedPayloads();
        try std.testing.expectEqual(expected_count, source.stats.retained_payloads);
        try std.testing.expectEqual(expected_bytes, source.stats.retained_payload_bytes);
    }
    try std.testing.expectEqual(@as(u64, 1), source.stats.retained_payloads);
    try store.delete(key);
    while (!try source.collectStep(&raw, 1)) {}
    while (source.stats.retained_payloads != 0) {
        while (!try source.collectStep(&raw, 1)) {}
    }
    try std.testing.expectEqual(@as(u64, 0), source.inventory.bytes);
    var reopened = try Store.open(alloc, memory.storage(), "/incremental-inventory", false);
    defer reopened.deinit();
    try std.testing.expectEqual(@as(u64, 0), reopened.stats.retained_payloads);
}

test "source vector payloads publication reuses WAL under memory pressure" {
    try testPublicationMemoryPressure(false);
    try testPublicationMemoryPressure(true);
}

test "source vector payloads mark workspace admission releases snapshots and resumes reclamation" {
    const alloc = std.testing.allocator;
    const docs = @import("docstore.zig");
    const mem = @import("mem_backend.zig");
    const keys = @import("internal_keys.zig");
    for ([_]bool{ false, true }) |incremental| {
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var backend = mem.Backend.init(alloc, .{});
        defer backend.close();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        var budgets = resources.Options.defaultBudgets();
        const limit = 16 * 1024 * 1024;
        budgets[@intFromEnum(resources.Slice.dense_source_payload_state)] = .{ .hard_limit_bytes = limit };
        var manager = resources.ResourceManager.init(.{ .budgets = budgets });
        defer manager.deinit(alloc);
        var source = try Store.openManaged(alloc, &manager, memory.storage(), "/mark-admission", false);
        defer source.deinit();
        source.append_only = true;
        source.selective_gc = true;
        source.incremental_inventory = incremental;
        source.shared_catalog = incremental;
        source.mark_outside_lock = incremental;
        source.mark_step_rows = 64;
        source.ann_reference_root = try source.alloc.dupe(u8, "/mark-admission-ann");
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
        defer alloc.free(key);
        const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
        defer alloc.free(artifact);
        try store.put(key, artifact);
        const ref = try payload.Reference.forArtifact(key, artifact);
        for (0..2048) |i| {
            var identity: [8]u8 = undefined;
            std.mem.writeInt(u64, &identity, i, .little);
            const orphan = try payload.Reference.forArtifact(&identity, artifact);
            try Store.prepare(&source, &.{.{ .reference = orphan, .artifact = artifact }});
        }
        try source.checkpoint();
        var ann = try native.Store.open(alloc, memory.storage(), "/mark-admission-ann");
        defer ann.deinit();
        try ann.publishEmptyBase(1, 0, .{ .shard_count = 16, .encoding = .artifact_reference });
        try ann.appendBatch(1, &.{.{ .kind = .upsert, .key = key, .source_sequence = 1, .revision = 1, .reference = .{ .digest = ref.digest, .dims = 3 } }}, 1, .{});
        var old_query = try source.snapshot(alloc);
        defer old_query.deinit();
        const budget = source.budget.?;
        budget.reservation.shrink(budget.reservation.bytes - budget.live_bytes);
        const before_live = budget.live_bytes;
        const used = manager.sliceStats(.dense_source_payload_state).used_bytes;
        const workspace = try source.markWorkspaceBytes();
        var held = try manager.reserve(.dense_source_payload_state, limit - used - workspace / 2);
        defer held.release();
        const generation = source.currentGeneration();
        for (0..3) |_| {
            try std.testing.expect(!try source.collectStepDeferredMark(&raw, 4096));
            try std.testing.expect(source.marking == null and source.collection == null);
            try std.testing.expect(!source.poisoned);
            try std.testing.expectEqual(generation, source.currentGeneration());
            // Each denied attempt releases its primary cursor and ANN/source
            // leases rather than accumulating memory or blocking publication.
            try std.testing.expectEqual(before_live, budget.live_bytes);
            const denial = budget.allocationFailureThreadSafe().?;
            try std.testing.expect(denial.cause == .admission);
            try std.testing.expectEqual(workspace, denial.requested_bytes);
        }
        try std.testing.expect(source.stats.collection_deferrals >= 3);
        held.release();
        // Backing allocation failure remains an error, even after a prior
        // admission denial; a stale receipt must not swallow real OOMs.
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
        {
            budget.backing = failing.allocator();
            defer budget.backing = alloc;
            try std.testing.expectError(error.OutOfMemory, source.collectStepDeferredMark(&raw, 4096));
        }
        // Admission can change between the cut and copy planning. Reject the
        // new plan before allocation, release its leases, and retry cleanly.
        source.bitmap_marking = true;
        source.bitmap_locator = true;
        source.sparse_gc_copy_bytes = 64 * 1024 * 1024;
        try std.testing.expect(!try source.startMarkingLocked(&raw));
        var progress: Store.ScanProgress = .{};
        try Store.scanMarking(source.marking.?, time.monotonicNs(), 0, std.math.maxInt(usize), true, &progress);
        try std.testing.expect(source.marking.?.scan_done);
        budget.reservation.shrink(budget.reservation.bytes - budget.live_bytes);
        var plan_pressure = try manager.reserve(.dense_source_payload_state, limit - manager.sliceStats(.dense_source_payload_state).used_bytes);
        defer plan_pressure.release();
        const before_deferred_plan = source.stats.collection_deferrals;
        try std.testing.expect(!try source.finishMarkingLocked(4096));
        try std.testing.expect(source.marking == null and source.collection == null);
        try std.testing.expect(!source.poisoned);
        try std.testing.expectEqual(before_deferred_plan + 1, source.stats.collection_deferrals);
        try std.testing.expectEqual(generation, source.currentGeneration());
        plan_pressure.release();
        var steps: usize = 0;
        while (!try source.collectStep(&raw, 4096)) : (steps += 1) try std.testing.expect(steps < 4096);
        try std.testing.expectEqual(@as(u64, 1), source.stats.retained_payloads);
        const old = try old_query.get(&ref.digest, std.math.maxInt(u64), 1);
        var decoded: [3]f32 = undefined;
        try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3 }, try old.vector.decodeExactInto(&decoded));
        for (0..2) |_| {
            var reopened = try Store.open(alloc, memory.storage(), "/mark-admission", false);
            defer reopened.deinit();
            const value = try Store.resolve(&reopened, alloc, key, ref);
            defer alloc.free(value);
            try std.testing.expectEqualSlices(u8, artifact, value);
            try std.testing.expectEqual(@as(u64, 1), reopened.stats.retained_payloads);
        }
    }
}

fn testPublicationMemoryPressure(selective: bool) !void {
    const alloc = std.testing.allocator;
    const docs = @import("docstore.zig");
    const mem = @import("mem_backend.zig");
    const keys = @import("internal_keys.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var budgets = resources.Options.defaultBudgets();
    const limit = 32 * 1024 * 1024;
    budgets[@intFromEnum(resources.Slice.dense_source_payload_state)] = .{ .hard_limit_bytes = limit };
    var manager = resources.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var source = try Store.openManaged(alloc, &manager, memory.storage(), "/publication-memory", false);
    defer source.deinit();
    source.append_only = true;
    source.selective_gc = selective;
    source.mark_outside_lock = true;
    source.mark_step_rows = 1;
    var store = try docs.DocStore.openRuntime(alloc, &raw);
    defer store.close();
    store.payload_store = source.interface();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
    defer alloc.free(first);
    const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 4, 5, 6 });
    defer alloc.free(second);
    try store.put(key, first);
    try store.put(key, second);
    try std.testing.expect(!try source.collectStepDeferredMark(&raw, 1));
    while (!source.marking.?.scan_done) try source.advanceMarkingSnapshot();
    const prefix_bytes = source.marking.?.boundary.committed_bytes;
    const vector = [_]f32{1} ** 256;
    const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 3, &vector);
    defer alloc.free(artifact);
    for (0..1024) |i| {
        var identity: [8]u8 = undefined;
        std.mem.writeInt(u64, &identity, i, .little);
        const ref = try payload.Reference.forArtifact(&identity, artifact);
        try Store.prepare(&source, &.{.{ .reference = ref, .artifact = artifact }});
    }
    const wal_bytes = source.opened.store.wal_committed_bytes;
    const budget = source.budget.?;
    budget.reservation.shrink(budget.reservation.bytes - budget.live_bytes);
    const used = manager.sliceStats(.dense_source_payload_state).used_bytes;
    var held = try manager.reserve(.dense_source_payload_state, limit - used - wal_bytes / 2);
    defer held.release();
    try std.testing.expect(try source.collectStep(&raw, std.math.maxInt(u64)));
    try std.testing.expect(!source.poisoned);
    try std.testing.expectEqual(@as(usize, 0), source.opened.wal_bytes.len);
    try std.testing.expectEqual(wal_bytes - prefix_bytes, source.opened.store.wal_committed_bytes);
    try std.testing.expect(budget.allocationFailureThreadSafe() == null);
    held.release();
    for (0..2) |_| {
        var reopened = try Store.open(alloc, memory.storage(), "/publication-memory", false);
        defer reopened.deinit();
        var identity: [8]u8 = undefined;
        std.mem.writeInt(u64, &identity, 1023, .little);
        const ref = try payload.Reference.forArtifact(&identity, artifact);
        const resolved = try Store.resolve(&reopened, alloc, &identity, ref);
        defer alloc.free(resolved);
        try std.testing.expectEqualSlices(u8, artifact, resolved);
    }
    // Actual admission pressure must precede append and leave the current
    // generation usable. Cancelling the pending mark cannot lose its tail.
    try std.testing.expect(!try source.collectStepDeferredMark(&raw, 1));
    budget.reservation.shrink(budget.reservation.bytes - budget.live_bytes);
    const pressure_used = manager.sliceStats(.dense_source_payload_state).used_bytes;
    var pressure = try manager.reserve(.dense_source_payload_state, limit - pressure_used - 1024);
    defer pressure.release();
    const before_batch = source.opened.store.last_committed_batch;
    const pressure_ref = try payload.Reference.forArtifact("pressure-retry", artifact);
    try std.testing.expectError(error.ResourceBudgetExceeded, Store.prepare(&source, &.{.{ .reference = pressure_ref, .artifact = artifact }}));
    try std.testing.expectEqual(before_batch, source.opened.store.last_committed_batch);
    try std.testing.expect(!source.poisoned);
    try std.testing.expect(source.marking == null);
    pressure.release();
    try Store.prepare(&source, &.{.{ .reference = pressure_ref, .artifact = artifact }});
}

test "source vector payloads publication allocation failures preserve usable authority" {
    const alloc = std.testing.allocator;
    const docs = @import("docstore.zig");
    const mem = @import("mem_backend.zig");
    const keys = @import("internal_keys.zig");
    var failures: usize = 0;
    var successes: usize = 0;
    for ([_]bool{ false, true }) |incremental| for (0..128) |fail_index| {
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = fail_index });
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var backend = mem.Backend.init(alloc, .{});
        defer backend.close();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        var source = try Store.open(alloc, memory.storage(), "/publication-failure", false);
        defer source.deinit();
        source.append_only = true;
        source.selective_gc = true;
        source.mark_outside_lock = true;
        source.mark_step_rows = 1;
        source.shared_catalog = incremental;
        source.incremental_inventory = incremental;
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
        defer alloc.free(key);
        const old = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2 });
        defer alloc.free(old);
        const current = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 3, 4 });
        defer alloc.free(current);
        try store.put(key, old);
        try store.put(key, current);
        var old_view = try source.snapshot(alloc);
        defer old_view.deinit();
        try std.testing.expect(!try source.collectStepDeferredMark(&raw, 1));
        while (!source.marking.?.scan_done) try source.advanceMarkingSnapshot();
        source.alloc = failing.allocator();
        source.opened.store.alloc = failing.allocator();
        const result = source.collectStep(&raw, std.math.maxInt(u64));
        source.alloc = alloc;
        source.opened.store.alloc = alloc;
        if (result) |complete| {
            try std.testing.expect(complete);
            successes += 1;
        } else |err| {
            failures += 1;
            if (err == error.GenerationPublicationDurabilityUncertain) {
                @import("../test_error_logs.zig").expectErrorLogs(1);
                try std.testing.expect(source.poisoned);
            } else {
                try std.testing.expectEqual(error.OutOfMemory, err);
                try std.testing.expect(!source.poisoned);
                // Foreground reads and the next collection remain usable.
                const value = try store.get(alloc, key);
                defer alloc.free(value);
                try std.testing.expectEqualSlices(u8, current, value);
                while (!try source.collectStep(&raw, 1)) {}
            }
        }
        const old_ref = try payload.Reference.forArtifact(key, old);
        try std.testing.expect((try old_view.get(&old_ref.digest, std.math.maxInt(u64), null)) == .vector);
        for (0..2) |_| {
            var reopened = try Store.open(alloc, memory.storage(), "/publication-failure", false);
            defer reopened.deinit();
            const ref = try payload.Reference.forArtifact(key, current);
            const value = try Store.resolve(&reopened, alloc, key, ref);
            defer alloc.free(value);
            try std.testing.expectEqualSlices(u8, current, value);
        }
    };
    try std.testing.expect(failures != 0 and successes != 0);
}

test "source vector payloads WAL admission cancels unpublished marks and retains every append" {
    const alloc = std.testing.allocator;
    const docs = @import("docstore.zig");
    const mem = @import("mem_backend.zig");
    const keys = @import("internal_keys.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var source = try Store.open(alloc, memory.storage(), "/bounded-wal", false);
    defer source.deinit();
    source.append_only = true;
    source.selective_gc = true;
    source.mark_outside_lock = true;
    source.mark_step_rows = 1;
    source.wal_admission_bytes = 64 * 1024;
    var store = try docs.DocStore.openRuntime(alloc, &raw);
    defer store.close();
    store.payload_store = source.interface();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    const value = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2 });
    defer alloc.free(value);
    try store.put(key, value);
    try std.testing.expect(!try source.collectStepDeferredMark(&raw, 1));
    const vector = [_]f32{2} ** 2048;
    const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &vector);
    defer alloc.free(artifact);
    MarkInterleaving.entered.store(false, .release);
    MarkInterleaving.resume_scan.store(false, .release);
    MarkInterleaving.scan_error = null;
    source.mark_test_hook = MarkInterleaving.pause;
    const scanner = try std.Thread.spawn(.{}, MarkInterleaving.scan, .{&source});
    var joined = false;
    defer if (!joined) {
        MarkInterleaving.resume_scan.store(true, .release);
        scanner.join();
    };
    try MarkInterleaving.awaitFlag(&MarkInterleaving.entered);
    for (0..12) |i| {
        if (i == 9) {
            try std.testing.expect(source.marking.?.cancel_requested);
            MarkInterleaving.resume_scan.store(true, .release);
            scanner.join();
            joined = true;
            source.mark_test_hook = null;
            try std.testing.expect(MarkInterleaving.scan_error == null);
            try std.testing.expect(source.marking == null);
        }
        var identity: [8]u8 = undefined;
        std.mem.writeInt(u64, &identity, i, .little);
        const ref = try payload.Reference.forArtifact(&identity, artifact);
        try Store.prepare(&source, &.{.{ .reference = ref, .artifact = artifact }});
        try std.testing.expect(source.opened.store.wal_committed_bytes < source.wal_admission_bytes + 16 * 1024);
    }
    try std.testing.expect(source.marking == null and source.collection == null);
    try std.testing.expect(source.stats.collection_deferrals > 0);
    try std.testing.expect(!source.poisoned);
    var reopened = try Store.open(alloc, memory.storage(), "/bounded-wal", false);
    defer reopened.deinit();
    for (0..12) |i| {
        var identity: [8]u8 = undefined;
        std.mem.writeInt(u64, &identity, i, .little);
        const ref = try payload.Reference.forArtifact(&identity, artifact);
        const resolved = try Store.resolve(&reopened, alloc, &identity, ref);
        defer alloc.free(resolved);
        try std.testing.expectEqualSlices(u8, artifact, resolved);
    }
    try store.delete(key);
    while (!try source.collectStep(&raw, std.math.maxInt(u64))) {}
    while (source.stats.retained_payloads != 0) {
        while (!try source.collectStep(&raw, std.math.maxInt(u64))) {}
    }
}

test "source vector payloads inventory allocation failure discards partial cache and rebuilds" {
    const alloc = std.testing.allocator;
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var source = try Store.open(alloc, memory.storage(), "/inventory-failure", false);
    defer source.deinit();
    const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2 });
    defer alloc.free(artifact);
    const ref = try payload.Reference.forArtifact("model-a", artifact);
    try Store.prepare(&source, &.{.{ .reference = ref, .artifact = artifact }});
    try source.checkpoint();
    var reached_success = false;
    for (0..32) |fail_index| {
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = fail_index });
        var inventory: Store.Inventory = .{};
        defer inventory.deinit(alloc);
        var rows: u64 = 0;
        inventory.sync(failing.allocator(), null, &source.opened, &rows) catch |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            try std.testing.expect(!inventory.initialized);
            try std.testing.expectEqual(@as(u32, 0), inventory.counts.count());
            try inventory.sync(alloc, null, &source.opened, &rows);
            try std.testing.expectEqual(@as(u64, 8), inventory.bytes);
            continue;
        };
        reached_success = true;
        break;
    }
    try std.testing.expect(reached_success);
}

test "source vector payloads deferred inventory preserves receipt totals and builds on installation" {
    const alloc = std.testing.allocator;
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2 });
    defer alloc.free(first);
    const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 3, 4, 5 });
    defer alloc.free(second);
    const first_ref = try payload.Reference.forArtifact("model-a", first);
    const second_ref = try payload.Reference.forArtifact("model-b", second);
    {
        var source = try Store.open(alloc, memory.storage(), "/deferred-inventory", false);
        defer source.deinit();
        source.checkpoint_receipts = true;
        try Store.prepare(&source, &.{.{ .reference = first_ref, .artifact = first }});
        try source.checkpoint();
        try source.saveCheckpointReceipt(null, null);
    }
    var source = try Store.open(alloc, memory.storage(), "/deferred-inventory", false);
    defer source.deinit();
    source.checkpoint_receipts = true;
    try std.testing.expect(try source.loadCheckpointReceipt());
    // This fixture exercises deferred cache construction, regardless of a
    // process-wide small-table policy. The cutoff has separate transition tests.
    source.inventory_min_payloads = 0;
    source.inventory_requested = true;
    source.incremental_inventory = true;
    source.inventory.deinit(alloc);
    const updates = source.stats.inventory_updates;
    try source.initializeInventory(true);
    try std.testing.expect(!source.inventory.initialized);
    try std.testing.expectEqual(updates, source.stats.inventory_updates);
    try std.testing.expectEqual(@as(u64, 1), source.stats.retained_payloads);
    try std.testing.expectEqual(@as(u64, 8), source.stats.retained_payload_bytes);
    var lease = try source.snapshot(alloc);
    defer lease.deinit();
    const value = try Store.resolve(&source, alloc, "model-a", first_ref);
    defer alloc.free(value);
    try std.testing.expectEqualSlices(u8, first, value);
    try Store.prepare(&source, &.{.{ .reference = second_ref, .artifact = second }});
    try std.testing.expect(!source.inventory.initialized);
    try std.testing.expectEqual(@as(u64, 2), source.stats.retained_payloads);
    // A failed first map construction consumes neither the old source nor next.
    var next = try source.opened.clone(alloc);
    var next_owned = true;
    defer if (next_owned) next.deinit();
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    {
        source.alloc = failing.allocator();
        defer source.alloc = alloc;
        const installation = source.installOpened(&next);
        if (installation) |_| {
            // Preserve single ownership even if the expected failure regresses.
            next_owned = false;
        } else |_| {}
        try std.testing.expectError(error.OutOfMemory, installation);
    }
    try std.testing.expect(!source.inventory.initialized);
    try source.checkpoint();
    try std.testing.expect(source.inventory.initialized);
    try std.testing.expectEqual(@as(u32, 2), source.inventory.counts.count());
    try std.testing.expectEqual(@as(u64, 20), source.inventory.bytes);
    try source.inventoryRetainedPayloads();
    try std.testing.expectEqual(source.inventory.bytes, source.stats.retained_payload_bytes);
    try std.testing.expectEqual(@as(u64, source.inventory.counts.count()), source.stats.retained_payloads);
    try std.testing.expect((try lease.get(&first_ref.digest, std.math.maxInt(u64), null)) == .vector);
    try std.testing.expect((try lease.get(&second_ref.digest, std.math.maxInt(u64), null)) == .missing);
}

test "source vector payloads deferred inventory rejects corrupt and stale receipts" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |corrupt| {
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2 });
        defer alloc.free(artifact);
        const first_ref = try payload.Reference.forArtifact("model-a", artifact);
        const second_ref = try payload.Reference.forArtifact("model-b", artifact);
        {
            var source = try Store.open(alloc, memory.storage(), "/invalid-receipt-inventory", false);
            defer source.deinit();
            source.checkpoint_receipts = true;
            try Store.prepare(&source, &.{.{ .reference = first_ref, .artifact = artifact }});
            try source.checkpoint();
            try source.saveCheckpointReceipt(null, null);
            if (corrupt) {
                try memory.storage().writeFileAbsolute("/invalid-receipt-inventory/SOURCE_CHECKPOINT", "corrupt");
            } else {
                try Store.prepare(&source, &.{.{ .reference = second_ref, .artifact = artifact }});
            }
        }
        var reopened = try Store.open(alloc, memory.storage(), "/invalid-receipt-inventory", false);
        defer reopened.deinit();
        reopened.checkpoint_receipts = true;
        try std.testing.expect(!try reopened.loadCheckpointReceipt());
        try std.testing.expect(reopened.receipt == null);
        reopened.inventory.deinit(alloc);
        try reopened.initializeInventory(true);
        try std.testing.expect(reopened.inventory.initialized);
        try std.testing.expectEqual(@as(u64, if (corrupt) 1 else 2), reopened.stats.retained_payloads);
        try std.testing.expectEqual(@as(u64, if (corrupt) 8 else 16), reopened.stats.retained_payload_bytes);
    }
}

test "source vector payloads independent scan skips apply only before planning and outside fences" {
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var source = try Store.open(alloc, memory.storage(), "/independent-scan", false);
    defer source.deinit();
    source.mark_outside_lock = true;
    source.independent_scan = true;
    try std.testing.expect(!source.continueScanWithoutApply());
    try std.testing.expect(!try source.collectStepDeferredMark(&raw, 1));
    try std.testing.expect(source.continueScanWithoutApply());
    source.stats.unresolved_primary_commits = 1;
    try std.testing.expect(!source.continueScanWithoutApply());
    source.stats.unresolved_primary_commits = 0;
    try source.advanceMarkingSnapshot();
    try std.testing.expect(!source.continueScanWithoutApply());
    try std.testing.expect(try source.collectStepDeferredMark(&raw, 1));
    try std.testing.expectEqual(@as(u64, 1), source.stats.collection_apply_visits_avoided);
}

test "source vector payloads delta inventory retires only cut WAL events and preserves duplicate suffix" {
    const alloc = std.testing.allocator;
    var inventory: Store.Inventory = .{ .delta = true };
    defer inventory.deinit(alloc);
    const first: payload.Digest = [_]u8{1} ** 32;
    const second: payload.Digest = [_]u8{2} ** 32;
    // Batch zero is real. Reappearance in a later batch must keep the single
    // WAL contribution when an earlier occurrence crosses the checkpoint cut.
    try inventory.addWal(alloc, &first, 2, 0);
    try inventory.addWal(alloc, &second, 3, 1);
    try inventory.addWal(alloc, &first, 2, 2);
    try inventory.add(alloc, first, 2); // same identity also exists in a segment
    try inventory.retireWal(0);
    try std.testing.expectEqual(@as(u32, 2), inventory.wal.count());
    try std.testing.expectEqual(@as(u64, 2), inventory.counts.get(first).?.count);
    try inventory.retireWal(1);
    try std.testing.expect(!inventory.counts.contains(second));
    try inventory.retireWal(2);
    try std.testing.expectEqual(@as(u64, 1), inventory.counts.get(first).?.count);
    try std.testing.expectEqual(@as(u64, 8), inventory.bytes);
    try std.testing.expectEqual(@as(u32, 0), inventory.wal.count());
    try std.testing.expectEqual(@as(usize, 0), inventory.wal_events.items.len);
}

test "source vector payloads cost experiments preserve updates deletes old leases and independent inventory" {
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    const docs = @import("docstore.zig");
    const keys = @import("internal_keys.zig");
    for ([_]bool{ false, true }) |bitmap| for ([_]bool{ false, true }) |delta| for ([_]u64{ 0, 2 }) |cutoff| for ([_]bool{ false, true }) |locator| {
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var backend = mem.Backend.init(alloc, .{});
        defer backend.close();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        var source = try Store.open(alloc, memory.storage(), "/cost-experiments", false);
        defer source.deinit();
        source.bitmap_marking = bitmap;
        source.bitmap_locator = locator;
        source.delta_inventory = delta;
        source.inventory.delta = delta;
        source.incremental_inventory = true;
        source.inventory_requested = true;
        source.inventory_min_payloads = cutoff;
        source.append_only = true;
        source.selective_gc = true;
        source.mark_outside_lock = true;
        source.mark_step_rows = 1;
        source.rescue_reappends = false;
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        const a = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-a");
        defer alloc.free(a);
        const b = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-b");
        defer alloc.free(b);
        const old = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2 });
        defer alloc.free(old);
        const new = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 3, 4, 5 });
        defer alloc.free(new);
        const ref = try payload.Reference.forArtifact(a, old);
        try store.put(a, old);
        try source.checkpoint();
        if (cutoff != 0) try std.testing.expect(!source.incremental_inventory);
        try store.put(b, new);
        try source.checkpoint();
        try std.testing.expect(source.incremental_inventory);
        var lease = try source.snapshot(alloc);
        defer lease.deinit();
        try store.put(a, new);
        try std.testing.expect(!try source.collectStepDeferredMark(&raw, 1));
        // Force a post-cut reappend of the old version; it is protected for
        // this pass but must disappear after the following mark.
        try Store.prepare(&source, &.{.{ .reference = ref, .artifact = old }});
        for (0..3) |_| {
            var turns: usize = 0;
            while (!try source.collectStep(&raw, 1)) : (turns += 1) try std.testing.expect(turns < 2048);
            const expected_count = source.stats.retained_payloads;
            const expected_bytes = source.stats.retained_payload_bytes;
            try source.inventoryRetainedPayloads();
            try std.testing.expectEqual(expected_count, source.stats.retained_payloads);
            try std.testing.expectEqual(expected_bytes, source.stats.retained_payload_bytes);
        }
        try std.testing.expectEqual(@as(u64, 2), source.stats.retained_payloads);
        try store.delete(a);
        try store.delete(b);
        for (0..2048) |_| {
            if (source.stats.retained_payloads == 0) break;
            _ = try source.collectStep(&raw, 1);
        }
        try std.testing.expectEqual(@as(u64, 0), source.stats.retained_payloads);
        try source.checkpoint();
        if (cutoff != 0) try std.testing.expect(!source.incremental_inventory);
        try std.testing.expect((try lease.get(&ref.digest, std.math.maxInt(u64), 1)) == .vector);
        for (0..2) |_| {
            var reopened = try Store.open(alloc, memory.storage(), "/cost-experiments", false);
            defer reopened.deinit();
            try reopened.inventoryRetainedPayloads();
            try std.testing.expectEqual(@as(u64, 0), reopened.stats.retained_payloads);
        }
    };
}

test "source vector payloads debt policy bounds background deferral and explicit collection bypasses it" {
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var source = try Store.open(alloc, memory.storage(), "/debt-policy", false);
    defer source.deinit();
    source.debt_scheduling = true;
    source.last_mark_completed_ns = 100;
    try std.testing.expect(source.shouldDeferMark(100 + 29 * std.time.ns_per_s));
    try std.testing.expect(!source.shouldDeferMark(100 + 30 * std.time.ns_per_s));
    Store.retiredPayloads(&source, 8 * 1024 * 1024);
    try std.testing.expect(!source.shouldDeferMark(101));
    source.obsolete_debt = 0;
    source.last_mark_completed_ns = time.monotonicNs();
    try std.testing.expect(!try source.collectBackgroundStepDeferredMark(&raw, 1));
    try std.testing.expect(source.marking == null);
    while (!try source.collectStep(&raw, 1)) {}
    try std.testing.expect(source.stats.collections > 0);
    source.stats.unresolved_primary_commits = 1;
    try std.testing.expect(!source.shouldDeferMark(time.monotonicNs()));
    try std.testing.expect(!try source.collectStep(&raw, 1));
}

test "source vector payloads debt notification counts committed replacements and deletes only" {
    if (!payload.ownershipEnabled()) return;
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    const docs = @import("docstore.zig");
    const keys = @import("internal_keys.zig");
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = mem.Backend.init(alloc, .{});
    defer backend.close();
    var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer raw.deinit();
    var source = try Store.open(alloc, memory.storage(), "/debt-commit", false);
    defer source.deinit();
    source.debt_scheduling = true;
    var store = try docs.DocStore.openRuntime(alloc, &raw);
    defer store.close();
    store.payload_store = source.interface();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    const first = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2 });
    defer alloc.free(first);
    const second = try codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 3, 4, 5 });
    defer alloc.free(second);
    try store.put(key, first);
    try std.testing.expectEqual(@as(u64, 0), source.obsolete_debt);
    try store.put(key, first);
    try std.testing.expectEqual(@as(u64, 0), source.obsolete_debt);
    {
        var txn = try store.beginWriteTxn();
        try txn.put(key, second);
        txn.abort();
    }
    try std.testing.expectEqual(@as(u64, 0), source.obsolete_debt);
    try store.put(key, second);
    try std.testing.expectEqual(@as(u64, 8), source.obsolete_debt);
    try store.delete(key);
    try std.testing.expectEqual(@as(u64, 20), source.obsolete_debt);
    while (!try source.collectStep(&raw, 1)) {}
    try std.testing.expectEqual(@as(u64, 0), source.obsolete_debt);
}

fn testBitmapLocatorAllocation(alloc: Allocator, opened: *const native.Opened) !void {
    var live = LiveSet.init(alloc);
    defer live.deinit();
    try live.enableBitmapsMode(opened, true);
    var subset = LiveSet.init(alloc);
    defer subset.deinit();
    try subset.enableSubset(&live);
}

test "source vector payloads bitmap locator verifies collisions and owns immutable subset leases" {
    const alloc = std.testing.allocator;
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var source = try Store.open(alloc, memory.storage(), "/bitmap-locator", false);
    defer source.deinit();
    source.append_only = true;
    const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
    defer alloc.free(artifact);
    var references: [64]payload.Reference = undefined;
    for (&references, 0..) |*reference, i| {
        var buffer: [32]u8 = undefined;
        reference.* = try payload.Reference.forArtifact(try std.fmt.bufPrint(&buffer, "model-{d}", .{i}), artifact);
        try Store.prepare(&source, &.{.{ .reference = reference.*, .artifact = artifact }});
    }
    try source.checkpoint();
    try std.testing.checkAllAllocationFailures(alloc, testBitmapLocatorAllocation, .{&source.opened});
    var control = LiveSet.init(alloc);
    defer control.deinit();
    try control.enableBitmaps(&source.opened);
    var indexed = LiveSet.init(alloc);
    defer indexed.deinit();
    try indexed.enableBitmapsDeferred(&source.opened, true);
    try std.testing.expect(indexed.locatorPending());
    try indexed.put(references[0].digest, references[0].dims);
    while (try indexed.advanceLocator()) {
        try std.testing.expectEqual(@as(?u32, 3), indexed.get(references[0].digest));
    }
    try std.testing.expect(!indexed.locatorPending());
    for (references) |reference| {
        try control.put(reference.digest, reference.dims);
        try indexed.put(reference.digest, reference.dims);
    }
    try std.testing.expectEqual(control.count(), indexed.count());
    for (references) |reference| try std.testing.expectEqual(control.get(reference.digest), indexed.get(reference.digest));
    var collision = references[0].digest;
    collision[31] ^= 1; // identical bucket hash, different full identity
    try std.testing.expect(indexed.get(collision) == null);
    try std.testing.expectError(error.MissingCommittedVectorPayload, indexed.put(collision, 3));
    try std.testing.expectError(error.VectorReferenceIdentityMismatch, indexed.put(references[0].digest, 4));
    var subset = LiveSet.init(alloc);
    defer subset.deinit();
    try subset.enableSubset(&indexed);
    for (references[0..32]) |reference| try subset.put(reference.digest, reference.dims);
    indexed.clearRetainingCapacity();
    const late = try payload.Reference.forArtifact("late-model", artifact);
    try Store.prepare(&source, &.{.{ .reference = late, .artifact = artifact }});
    try source.checkpoint();
    try std.testing.expect(subset.get(late.digest) == null);
    for (references[0..32]) |reference| try std.testing.expectEqual(@as(?u32, 3), subset.get(reference.digest));
    var iterated: usize = 0;
    var it = subset.iterator();
    while (it.next()) |entry| {
        try std.testing.expectEqual(@as(?u32, 3), control.get(entry.key_ptr.*));
        iterated += 1;
    }
    try std.testing.expectEqual(@as(usize, 32), iterated);
}

test "source vector payloads sparse batches amortize one mark with bounded selection and old leases" {
    const alloc = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    const docs = @import("docstore.zig");
    const keys = @import("internal_keys.zig");
    for ([_]struct { target: u64, dense: bool, planning: bool }{
        .{ .target = 0, .dense = false, .planning = false },
        .{ .target = 84, .dense = false, .planning = true },
        .{ .target = 64 * 1024 * 1024, .dense = false, .planning = true },
        .{ .target = 64 * 1024 * 1024, .dense = true, .planning = false },
        .{ .target = 64 * 1024 * 1024, .dense = true, .planning = true },
    }) |case| {
        const target = case.target;
        var memory = lsm.MemoryStorage.init(alloc);
        defer memory.deinit();
        var source = try Store.open(alloc, memory.storage(), "/sparse-batches", false);
        defer source.deinit();
        source.append_only = true;
        source.selective_gc = true;
        source.sparse_gc_copy_bytes = target;
        source.incremental_planning = case.planning;
        source.mark_outside_lock = true;
        source.mark_step_rows = 1;
        var backend = mem.Backend.init(alloc, .{});
        defer backend.close();
        var raw = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer raw.deinit();
        var store = try docs.DocStore.openRuntime(alloc, &raw);
        defer store.close();
        store.payload_store = source.interface();
        const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
        defer alloc.free(artifact);
        var counts: [3]usize = @splat(0);
        var names: [3][8][]u8 = undefined;
        defer for (0..3) |shard| for (names[shard][0..counts[shard]]) |name| alloc.free(name);
        var attempt: usize = 0;
        while (counts[0] + counts[1] + counts[2] != 24) : (attempt += 1) {
            var buffer: [40]u8 = undefined;
            const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, try std.fmt.bufPrint(&buffer, "sparse-{d}", .{attempt}), "model-a");
            const reference = try payload.Reference.forArtifact(key, artifact);
            const shard = vector_block.keyHash(&reference.digest) & 127;
            if (shard >= 3 or counts[shard] == 8) {
                alloc.free(key);
                continue;
            }
            names[shard][counts[shard]] = key;
            counts[shard] += 1;
            try store.put(key, artifact);
        }
        try source.checkpoint();
        var old = try source.snapshot(alloc);
        defer old.deinit();
        for (0..3) |shard| try store.delete(names[shard][0]);
        if (case.dense) try store.delete(names[0][1]);
        const collections = source.stats.collections;
        while (!try source.collectStep(&raw, 12)) {}
        try std.testing.expectEqual(collections + 1, source.stats.collections);
        // Each segment has seven live 12-byte vectors and one dead vector.
        try std.testing.expectEqual(@as(u64, if (case.dense) 22 else if (target > 84) 21 else 23), source.stats.retained_payloads);
        for (0..3) |shard| {
            const old_reference = try payload.Reference.forArtifact(names[shard][0], artifact);
            try std.testing.expect((try old.get(&old_reference.digest, std.math.maxInt(u64), 1)) == .vector);
            for (names[shard][if (case.dense and shard == 0) 2 else 1..]) |key| {
                const actual = try store.get(alloc, key);
                defer alloc.free(actual);
                try std.testing.expectEqualSlices(u8, artifact, actual);
            }
        }
        const expected: u64 = if (case.dense) 20 else 21;
        while (source.stats.retained_payloads != expected) _ = try source.collectStep(&raw, 12);
        try source.inventoryRetainedPayloads();
        try std.testing.expectEqual(expected, source.stats.retained_payloads);
        try std.testing.expectEqual(expected * 12, source.stats.retained_payload_bytes);
    }
}
