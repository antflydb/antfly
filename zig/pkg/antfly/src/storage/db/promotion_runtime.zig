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

//! Promoter stage (see zig/RESOLUTION.md).
//!
//! The promoter turns resolution decisions into durable entity state: for each
//! resolution artifact the resolution stage writes, it upserts the canonical
//! entity document into the entity table. Entities are sharded by entity key and
//! generally live on a *different* shard than the source document, so the actual
//! write goes through an injected `EntitySink` (the write-side analog of the
//! resolver's `CandidateSource`) which the api/serving layer implements over the
//! routing-aware table write path. In raft deployments promotion is additionally
//! guarded by an injected `PromotionOwner` so only the source shard's current
//! leader turns replay into public entity writes. With no sink the stage waits by
//! default; callers can explicitly disable promotion when that is intended.
//!
//! Replay stability: re-promoting the same resolution re-issues an idempotent
//! upsert (the sink merges canonical fields and unions aliases), so replay is a
//! no-op. The stage advances `applied_sequence` only after the upserts return.

const std = @import("std");
const builtin = @import("builtin");
const resolver_lib = @import("antfly_resolver");
const internal_keys = @import("../internal_keys.zig");
const change_journal_mod = @import("derived/change_journal.zig");
const replay_source_mod = @import("derived/replay_source.zig");
const enrichment_state = @import("enrichment/enrichment_state.zig");
const backend_erased = @import("../backend_erased.zig");
const background_runtime_mod = @import("../background_runtime.zig");
const resolution_runtime = @import("resolution_runtime.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;

/// applied-sequence checkpoint scope; also used by the replay prune watermark so
/// resolution-artifact records survive until the promoter consumes them.
pub const scope_name = "promotion";

/// Sink that upserts a canonical entity document. Abstracted over locality: a
/// local sink writes the worker's own store (co-located entity table); the api
/// layer implements a cross-shard sink over the table write path, applying an
/// idempotent merge transform (set canonical fields, union aliases). The
/// promoter calls this once per resolved mention.
/// One canonical entity upsert: the document `doc_json` at `key` in `table`.
pub const EntityUpsert = struct {
    table: []const u8,
    key: []const u8,
    doc_json: []const u8,
};

pub const MissingSinkPolicy = enum {
    /// Hold the applied sequence until a sink is injected; production routing or
    /// metadata gaps must not permanently drop promotion work.
    wait,
    /// Explicitly disable promotion and advance applied sequence without writes.
    disabled,
};

pub const EntitySink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Upsert the entity at `key` in `table` with the canonical document
        /// `doc_json`. Must be idempotent under replay.
        upsert: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, key: []const u8, doc_json: []const u8) anyerror!void,
        /// Upsert all entities resolved from one document atomically (a single
        /// multi-participant transaction), so a document never lands a partial
        /// set of its entities. Optional: when absent, `upsertBatch` falls back
        /// to per-entity upserts.
        upsert_batch: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, entries: []const EntityUpsert) anyerror!void = null,
    };

    pub fn upsert(self: EntitySink, allocator: std.mem.Allocator, table: []const u8, key: []const u8, doc_json: []const u8) anyerror!void {
        return self.vtable.upsert(self.ptr, allocator, table, key, doc_json);
    }

    pub fn upsertBatch(self: EntitySink, allocator: std.mem.Allocator, entries: []const EntityUpsert) anyerror!void {
        if (self.vtable.upsert_batch) |f| return f(self.ptr, allocator, entries);
        for (entries) |e| try self.upsert(allocator, e.table, e.key, e.doc_json);
    }
};

/// Dynamic ownership predicate for promotion work belonging to this DB's source
/// shard. Standalone/local DBs leave this unset and are always owners; raft
/// apply-side DBs inject a leadership-backed owner so followers keep the
/// promotion checkpoint unapplied until they become leader.
pub const PromotionOwner = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        is_local_owner: *const fn (ptr: *anyopaque) bool,
    };

    pub fn isLocalOwner(self: PromotionOwner) bool {
        return self.vtable.is_local_owner(self.ptr);
    }
};

/// The canonical entity document shape (RESOLUTION.md). `aliases` seeds the
/// union the sink's merge transform grows as more mentions resolve to the same
/// entity; provenance ("which documents mention this") lives as graph edges, not
/// here.
const EntityDoc = struct {
    entity_type: []const u8,
    canonical_name: []const u8,
    aliases: []const []const u8,
};

fn buildEntityDocAlloc(alloc: std.mem.Allocator, e: resolver_lib.ResolvedEntity) ![]u8 {
    const alias = if (e.surface_form.len > 0) e.surface_form else e.canonical_name;
    const aliases = [_][]const u8{alias};
    const doc = EntityDoc{
        .entity_type = e.label,
        .canonical_name = e.canonical_name,
        .aliases = aliases[0..],
    };
    return try std.json.Stringify.valueAlloc(alloc, doc, .{});
}

fn isPromotableDecision(decision: resolver_lib.Decision) bool {
    return switch (decision) {
        .new, .match => true,
        .review => false,
    };
}

/// Read the resolution artifact at `resolution_key` and upsert a canonical
/// entity document for each canonical decision through `sink`. Review-band
/// decisions stay durable in the resolution artifact/review queue but are not
/// promoted until a curator override re-resolves them to a canonical decision.
/// Returns the number of entities promoted. Pure of threading/state so it is
/// unit-testable with a fake store and sink.
pub fn processResolutionArtifact(
    gpa: std.mem.Allocator,
    store: resolver_lib.ArtifactStore,
    resolution_key: []const u8,
    sink: EntitySink,
) !usize {
    const raw = (try store.get(gpa, resolution_key)) orelse return 0;
    defer gpa.free(raw);

    var parsed = try resolver_lib.parseResolution(gpa, raw);
    defer parsed.deinit();

    // Collect every resolvable entity, then commit them in one batch so a
    // document's entities promote atomically (the sink uses a multi-participant
    // transaction when it supports one).
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    var entries = std.ArrayListUnmanaged(EntityUpsert).empty;
    for (parsed.entities) |e| {
        if (!isPromotableDecision(e.decision)) continue;
        // Need at least a canonical name to mint/merge a meaningful entity.
        if (e.canonical_name.len == 0) continue;
        try entries.append(a, .{
            .table = e.doc_ref.table,
            .key = e.doc_ref.key,
            .doc_json = try buildEntityDocAlloc(a, e),
        });
    }
    if (entries.items.len == 0) return 0;
    try sink.upsertBatch(gpa, entries.items);
    return entries.items.len;
}

/// Promote every changed resolution-artifact key in a replay record.
pub fn processRecordKeys(
    gpa: std.mem.Allocator,
    store: resolver_lib.ArtifactStore,
    changed_artifact_keys: []const []const u8,
    sink: EntitySink,
) !void {
    for (changed_artifact_keys) |key| {
        if (!internal_keys.isResolutionArtifactKey(key)) continue;
        _ = try processResolutionArtifact(gpa, store, key, sink);
    }
}

pub const default_max_records_per_window: usize = 1024;

/// Iterate replay records matching the promotion hint from `from_sequence`,
/// promoting each record's changed resolution artifacts. Returns the highest
/// sequence processed (or `from_sequence` if none, since `from_sequence` is
/// exclusive). Pure of the runtime's threading/state so it is unit-testable.
pub fn catchUpWindow(
    gpa: Allocator,
    replay_source: replay_source_mod.Source,
    store: resolver_lib.ArtifactStore,
    sink: EntitySink,
    from_sequence: u64,
    max_records: usize,
) !u64 {
    const Ctx = struct {
        gpa: Allocator,
        store: resolver_lib.ArtifactStore,
        sink: EntitySink,
        max_seen: u64,

        fn consume(ptr: *anyopaque, sequence: u64, payload: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var decoded = try change_journal_mod.decodeRecord(self.gpa, payload);
            defer decoded.deinit();
            try processRecordKeys(self.gpa, self.store, decoded.record.changed_artifact_keys, self.sink);
            if (sequence > self.max_seen) self.max_seen = sequence;
        }
    };

    var ctx = Ctx{
        .gpa = gpa,
        .store = store,
        .sink = sink,
        .max_seen = from_sequence,
    };
    _ = try replay_source.forEachMatchingRecord(gpa, from_sequence, .promotion, max_records, &ctx, Ctx.consume);
    return ctx.max_seen;
}

/// Managed worker that catches the promoter up on changed resolution artifacts.
/// Mirrors `ResolutionRuntime`: it wraps the shard store and replay source,
/// drains `applied_sequence` toward `target_sequence`, and persists the applied
/// sequence only after the entity upserts are durable, or when promotion is
/// explicitly disabled by policy.
pub const PromotionRuntime = struct {
    alloc: Allocator,
    store_handle: resolution_runtime.RuntimeStoreHandle,
    replay_source: replay_source_mod.Source,
    owner: ?PromotionOwner,
    /// Cross-shard entity write sink injected by the api/serving layer; null
    /// means promotion waits or is explicitly disabled, depending on
    /// `missing_sink_policy`. Must outlive the runtime.
    sink: ?EntitySink,
    sink_available: std.atomic.Value(bool),
    missing_sink_policy: MissingSinkPolicy,
    applied_sequence: std.atomic.Value(u64),
    target_sequence: std.atomic.Value(u64),
    error_count: std.atomic.Value(u64),
    shutdown_flag: std.atomic.Value(bool),
    catch_up_mutex: std.atomic.Mutex = .unlocked,
    io_impl: ?*background_runtime_mod.IoImpl,
    future: ?Io.Future(void),

    pub fn init(
        alloc: Allocator,
        store: anytype,
        replay_source: replay_source_mod.Source,
        backend_runtime: *background_runtime_mod.BackendRuntime,
        owner: ?PromotionOwner,
        sink: ?EntitySink,
        missing_sink_policy: MissingSinkPolicy,
    ) !PromotionRuntime {
        var store_handle = try resolution_runtime.initRuntimeStore(alloc, store);
        errdefer store_handle.deinit();
        const applied = try enrichment_state.loadAppliedSequence(alloc, store_handle.store, scope_name);
        return .{
            .alloc = alloc,
            .store_handle = store_handle,
            .replay_source = replay_source,
            .owner = owner,
            .sink = sink,
            .sink_available = .init(sink != null),
            .missing_sink_policy = missing_sink_policy,
            .applied_sequence = .init(applied),
            .target_sequence = .init(applied),
            .error_count = .init(0),
            .shutdown_flag = .init(false),
            .io_impl = backend_runtime.io_impl,
            .future = null,
        };
    }

    pub fn deinit(self: *PromotionRuntime) void {
        self.stop();
        self.store_handle.deinit();
        self.* = undefined;
    }

    /// Raise the catch-up target; the worker loop drains toward it.
    pub fn notifySequence(self: *PromotionRuntime, sequence: u64) void {
        var cur = self.target_sequence.load(.monotonic);
        while (sequence > cur) {
            cur = self.target_sequence.cmpxchgWeak(cur, sequence, .monotonic, .monotonic) orelse break;
        }
    }

    pub fn stats(self: *PromotionRuntime) types.ReplayStageStats {
        const target = self.target_sequence.load(.acquire);
        const applied = self.applied_sequence.load(.acquire);
        const owner_blocked = applied < target and if (self.owner) |owner| !owner.isLocalOwner() else false;
        const sink_blocked = applied < target and !owner_blocked and !self.sink_available.load(.acquire) and self.missing_sink_policy == .wait;
        const blocked = owner_blocked or sink_blocked;
        return .{
            .enabled = target > 0 or applied < target,
            .target_sequence = target,
            .applied_sequence = applied,
            .catch_up_required = applied < target,
            .blocked = blocked,
            .blocked_reason = if (owner_blocked) "not_source_group_leader" else if (sink_blocked) "missing_entity_sink" else "",
            .error_count = self.error_count.load(.monotonic),
        };
    }

    /// Inject (or clear) the source-shard promotion owner. Serialized with
    /// catch-up so ownership cannot change midway through a promotion window.
    pub fn setOwner(self: *PromotionRuntime, owner: ?PromotionOwner) void {
        lockMutex(&self.catch_up_mutex);
        defer self.catch_up_mutex.unlock();
        self.owner = owner;
    }

    /// Inject (or clear) the entity sink after construction, taken under
    /// `catch_up_mutex` so it cannot tear against an in-flight catch-up.
    pub fn setSink(self: *PromotionRuntime, sink: ?EntitySink) void {
        lockMutex(&self.catch_up_mutex);
        defer self.catch_up_mutex.unlock();
        self.sink = sink;
        self.sink_available.store(sink != null, .release);
    }

    pub fn start(self: *PromotionRuntime) !void {
        const io_impl = self.io_impl orelse return;
        self.future = try io_impl.io().concurrent(workerMain, .{self});
    }

    pub fn stop(self: *PromotionRuntime) void {
        self.shutdown_flag.store(true, .release);
        if (self.future) |*future| {
            if (self.io_impl) |io_impl| {
                _ = future.await(io_impl.io());
            }
            self.future = null;
        }
    }

    /// Drain applied -> target. Serialized so the background worker and a
    /// synchronous driver (runUntilIdle) cannot process the same records at once;
    /// idempotent and safe to retry. `applied_sequence` is persisted only after
    /// durable upserts.
    pub fn catchUp(self: *PromotionRuntime) !void {
        lockMutex(&self.catch_up_mutex);
        defer self.catch_up_mutex.unlock();
        errdefer _ = self.error_count.fetchAdd(1, .monotonic);

        while (true) {
            const target = self.target_sequence.load(.acquire);
            const applied = self.applied_sequence.load(.acquire);
            if (applied >= target) return;

            if (self.owner) |owner| {
                if (!owner.isLocalOwner()) return;
            }

            // No sink is either an explicit disabled mode or a temporary wiring /
            // routing gap. Only the disabled mode is allowed to mark work applied.
            const sink = self.sink orelse {
                if (self.missing_sink_policy == .wait) return;
                try enrichment_state.saveAppliedSequence(self.store_handle.store, scope_name, target);
                self.applied_sequence.store(target, .release);
                return;
            };

            var das = resolution_runtime.DbArtifactStore(backend_erased.Store){ .store = &self.store_handle.store };
            const max_seen = try catchUpWindow(
                self.alloc,
                self.replay_source,
                das.artifactStore(),
                sink,
                // from_sequence is exclusive (records with seq > from), matching
                // the derived workers, which pass their applied_sequence.
                applied,
                default_max_records_per_window,
            );
            if (max_seen <= applied) {
                try enrichment_state.saveAppliedSequence(self.store_handle.store, scope_name, target);
                self.applied_sequence.store(target, .release);
                return;
            }
            try enrichment_state.saveAppliedSequence(self.store_handle.store, scope_name, max_seen);
            self.applied_sequence.store(max_seen, .release);
        }
    }

    fn workerMain(self: *PromotionRuntime) void {
        const io = (self.io_impl orelse return).io();
        while (!self.shutdown_flag.load(.acquire)) {
            if (self.applied_sequence.load(.acquire) < self.target_sequence.load(.acquire)) {
                self.catchUp() catch |err| {
                    std.log.warn("promotion catch-up failed: {s}", .{@errorName(err)});
                    io.sleep(Io.Duration.fromMilliseconds(50), .awake) catch {};
                    continue;
                };
                if (self.applied_sequence.load(.acquire) < self.target_sequence.load(.acquire)) {
                    io.sleep(Io.Duration.fromMilliseconds(50), .awake) catch {};
                }
            } else {
                io.sleep(Io.Duration.fromMilliseconds(50), .awake) catch {};
            }
        }
        self.catchUp() catch {};
    }
};

fn lockMutex(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        if (builtin.single_threaded) {
            std.atomic.spinLoopHint();
            continue;
        }
        std.Thread.yield() catch {};
    }
}

const testing = std.testing;

/// In-memory ArtifactStore for tests (holds resolution artifacts).
const MapStore = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMapUnmanaged([]u8) = .empty,

    fn deinit(self: *MapStore) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.alloc.free(e.key_ptr.*);
            self.alloc.free(e.value_ptr.*);
        }
        self.map.deinit(self.alloc);
    }

    fn put(self: *MapStore, key: []const u8, value: []const u8) !void {
        const owned_value = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(owned_value);
        const gop = try self.map.getOrPut(self.alloc, key);
        if (gop.found_existing) self.alloc.free(gop.value_ptr.*) else gop.key_ptr.* = try self.alloc.dupe(u8, key);
        gop.value_ptr.* = owned_value;
    }

    fn backendStore(self: *MapStore) BackendStore {
        return .{ .store_ptr = self };
    }

    fn store(self: *MapStore) resolver_lib.ArtifactStore {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = resolver_lib.ArtifactStore.VTable{ .get = get, .put = putFn, .delete = deleteFn };

    fn get(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?[]u8 {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        const v = self.map.get(key) orelse return null;
        return try allocator.dupe(u8, v);
    }
    fn putFn(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        try self.put(key, value);
    }
    fn deleteFn(ptr: *anyopaque, key: []const u8) anyerror!void {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        if (self.map.fetchRemove(key)) |kv| {
            self.alloc.free(kv.key);
            self.alloc.free(kv.value);
        }
    }

    const BackendStore = struct {
        store_ptr: *MapStore,

        pub fn capabilities(_: BackendStore) backend_erased.types.Capabilities {
            return .{ .cursors = false };
        }

        pub fn beginRead(self: BackendStore) !ReadTxn {
            return .{ .store_ptr = self.store_ptr };
        }

        pub fn beginWrite(self: BackendStore) !WriteTxn {
            return .{ .store_ptr = self.store_ptr };
        }

        pub fn beginBatch(self: BackendStore) !WriteTxn {
            return self.beginWrite();
        }
    };

    const ReadTxn = struct {
        store_ptr: *MapStore,

        pub fn abort(_: *ReadTxn) void {}

        pub fn get(self: *ReadTxn, key: []const u8) ![]const u8 {
            return self.store_ptr.map.get(key) orelse error.NotFound;
        }

        pub fn openCursor(_: *ReadTxn) !EmptyCursor {
            return error.Unsupported;
        }
    };

    const WriteTxn = struct {
        store_ptr: *MapStore,

        pub fn abort(_: *WriteTxn) void {}

        pub fn commit(_: *WriteTxn) !void {}

        pub fn get(self: *WriteTxn, key: []const u8) ![]const u8 {
            return self.store_ptr.map.get(key) orelse error.NotFound;
        }

        pub fn put(self: *WriteTxn, key: []const u8, value: []const u8) !void {
            try self.store_ptr.put(key, value);
        }

        pub fn delete(self: *WriteTxn, key: []const u8) !void {
            if (self.store_ptr.map.fetchRemove(key)) |kv| {
                self.store_ptr.alloc.free(kv.key);
                self.store_ptr.alloc.free(kv.value);
            } else return error.NotFound;
        }

        pub fn openCursor(_: *WriteTxn) !EmptyCursor {
            return error.Unsupported;
        }
    };

    const EmptyCursor = struct {
        pub fn close(_: *EmptyCursor) void {}
        pub fn first(_: *EmptyCursor) !backend_erased.Entry {
            return error.NotFound;
        }
        pub fn last(_: *EmptyCursor) !backend_erased.Entry {
            return error.NotFound;
        }
        pub fn next(_: *EmptyCursor) !backend_erased.Entry {
            return error.NotFound;
        }
        pub fn prev(_: *EmptyCursor) !backend_erased.Entry {
            return error.NotFound;
        }
        pub fn seekAtOrAfter(_: *EmptyCursor, _: []const u8) !backend_erased.Entry {
            return error.NotFound;
        }
        pub fn seekAtOrBefore(_: *EmptyCursor, _: []const u8) !backend_erased.Entry {
            return error.NotFound;
        }
    };
};

/// Capturing entity sink for tests.
const CaptureSink = struct {
    alloc: std.mem.Allocator,
    keys: std.ArrayListUnmanaged([]u8) = .empty,
    tables: std.ArrayListUnmanaged([]u8) = .empty,
    docs: std.ArrayListUnmanaged([]u8) = .empty,
    batch_calls: usize = 0,

    fn deinit(self: *CaptureSink) void {
        for (self.keys.items) |k| self.alloc.free(k);
        for (self.tables.items) |t| self.alloc.free(t);
        for (self.docs.items) |d| self.alloc.free(d);
        self.keys.deinit(self.alloc);
        self.tables.deinit(self.alloc);
        self.docs.deinit(self.alloc);
    }

    fn sink(self: *CaptureSink) EntitySink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = EntitySink.VTable{ .upsert = upsert, .upsert_batch = upsertBatch };

    fn record(self: *CaptureSink, table: []const u8, key: []const u8, doc_json: []const u8) anyerror!void {
        try self.tables.append(self.alloc, try self.alloc.dupe(u8, table));
        try self.keys.append(self.alloc, try self.alloc.dupe(u8, key));
        try self.docs.append(self.alloc, try self.alloc.dupe(u8, doc_json));
    }

    fn upsert(ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, key: []const u8, doc_json: []const u8) anyerror!void {
        _ = allocator;
        const self: *CaptureSink = @ptrCast(@alignCast(ptr));
        try self.record(table, key, doc_json);
    }

    fn upsertBatch(ptr: *anyopaque, allocator: std.mem.Allocator, entries: []const EntityUpsert) anyerror!void {
        _ = allocator;
        const self: *CaptureSink = @ptrCast(@alignCast(ptr));
        self.batch_calls += 1;
        for (entries) |e| try self.record(e.table, e.key, e.doc_json);
    }
};

const ToggleOwner = struct {
    local_owner: bool,

    fn owner(self: *ToggleOwner) PromotionOwner {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = PromotionOwner.VTable{ .is_local_owner = isLocalOwner };

    fn isLocalOwner(ptr: *anyopaque) bool {
        const self: *ToggleOwner = @ptrCast(@alignCast(ptr));
        return self.local_owner;
    }
};

const sample_resolution =
    \\{"config_generation":3,"entities":[
    \\  {"local_id":"e0","doc_ref":{"table":"entities","key":"person/ada_lovelace"},"confidence":0.98,"decision":"new","label":"person","canonical_name":"Ada Lovelace","surface_form":"Ada Lovelace"},
    \\  {"local_id":"e1","doc_ref":{"table":"entities","key":"org/antfly"},"confidence":1.0,"decision":"match","label":"org","canonical_name":"Antfly","surface_form":"Antfly DB"}
    \\]}
;

test "processResolutionArtifact upserts a canonical entity per resolved mention" {
    const alloc = testing.allocator;
    var map = MapStore{ .alloc = alloc };
    defer map.deinit();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    try map.put(resolution_key, sample_resolution);

    var capture = CaptureSink{ .alloc = alloc };
    defer capture.deinit();

    const promoted = try processResolutionArtifact(alloc, map.store(), resolution_key, capture.sink());
    try testing.expectEqual(@as(usize, 2), promoted);

    // Both entities promoted in a single atomic batch (one transaction).
    try testing.expectEqual(@as(usize, 1), capture.batch_calls);
    try testing.expectEqual(@as(usize, 2), capture.keys.items.len);
    try testing.expectEqualStrings("entities", capture.tables.items[0]);
    try testing.expectEqualStrings("person/ada_lovelace", capture.keys.items[0]);
    try testing.expect(std.mem.indexOf(u8, capture.docs.items[0], "\"canonical_name\":\"Ada Lovelace\"") != null);
    try testing.expect(std.mem.indexOf(u8, capture.docs.items[0], "\"aliases\":[\"Ada Lovelace\"]") != null);
    try testing.expect(std.mem.indexOf(u8, capture.docs.items[0], "\"entity_type\":\"person\"") != null);
    try testing.expectEqualStrings("org/antfly", capture.keys.items[1]);
    try testing.expect(std.mem.indexOf(u8, capture.docs.items[1], "\"canonical_name\":\"Antfly\"") != null);
    try testing.expect(std.mem.indexOf(u8, capture.docs.items[1], "\"aliases\":[\"Antfly DB\"]") != null);

    // A missing artifact promotes nothing.
    try testing.expectEqual(@as(usize, 0), try processResolutionArtifact(alloc, map.store(), "no-such-key", capture.sink()));
}

test "processResolutionArtifact leaves review-band mentions unpromoted" {
    const alloc = testing.allocator;
    var map = MapStore{ .alloc = alloc };
    defer map.deinit();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:review", "resolution_v1");
    defer alloc.free(resolution_key);
    try map.put(resolution_key,
        \\{"config_generation":3,"entities":[
        \\  {"local_id":"e0","doc_ref":{"table":"entities","key":"person/ada_review"},"confidence":0.72,"decision":"review","label":"person","canonical_name":"Ada Lovelace","surface_form":"Ada Lovelace"},
        \\  {"local_id":"e1","doc_ref":{"table":"entities","key":"org/antfly"},"confidence":1.0,"decision":"match","label":"org","canonical_name":"Antfly","surface_form":"Antfly DB"}
        \\]}
    );

    var capture = CaptureSink{ .alloc = alloc };
    defer capture.deinit();

    const promoted = try processResolutionArtifact(alloc, map.store(), resolution_key, capture.sink());
    try testing.expectEqual(@as(usize, 1), promoted);
    try testing.expectEqual(@as(usize, 1), capture.batch_calls);
    try testing.expectEqual(@as(usize, 1), capture.keys.items.len);
    try testing.expectEqualStrings("org/antfly", capture.keys.items[0]);
}

test "processResolutionArtifact fails closed on malformed resolution artifacts" {
    const alloc = testing.allocator;
    var map = MapStore{ .alloc = alloc };
    defer map.deinit();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:bad", "resolution_v1");
    defer alloc.free(resolution_key);
    try map.put(resolution_key, "{}");

    var capture = CaptureSink{ .alloc = alloc };
    defer capture.deinit();

    try testing.expectError(error.InvalidResolution, processResolutionArtifact(alloc, map.store(), resolution_key, capture.sink()));
    try testing.expectEqual(@as(usize, 0), capture.keys.items.len);
}

/// Minimal replay Source for tests: replays a fixed list of encoded records.
const FakeSource = struct {
    const Rec = struct { sequence: u64, payload: []const u8 };
    records: []const Rec,

    fn source(self: *FakeSource) replay_source_mod.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = replay_source_mod.Source.VTable{
        .open_matching_cursor = openCursor,
        .for_each_matching_record = forEach,
        .latest_matching_sequence = latest,
        .collect_enrichment_document_groups = collectGroups,
        .is_sequence_visible = isVisible,
    };

    fn forEach(
        ptr: *anyopaque,
        alloc: Allocator,
        from_sequence: u64,
        hint: replay_source_mod.TargetHint,
        max_matched_entries: usize,
        ctx: *anyopaque,
        consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
    ) anyerror!replay_source_mod.MatchingRecordStats {
        _ = alloc;
        _ = hint;
        const self: *FakeSource = @ptrCast(@alignCast(ptr));
        var matched: usize = 0;
        var last: u64 = 0;
        for (self.records) |rec| {
            if (rec.sequence <= from_sequence) continue; // exclusive, matching the real source
            if (max_matched_entries != 0 and matched >= max_matched_entries) break;
            try consume(ctx, rec.sequence, rec.payload);
            matched += 1;
            last = rec.sequence;
        }
        return .{ .matched_entries = matched, .last_sequence = last };
    }

    fn openCursor(_: *anyopaque, _: Allocator, _: u64, _: replay_source_mod.TargetHint) anyerror!replay_source_mod.MatchingCursor {
        return error.Unsupported;
    }
    fn latest(_: *anyopaque, _: Allocator, _: u64, _: replay_source_mod.TargetHint) anyerror!u64 {
        return error.Unsupported;
    }
    fn collectGroups(_: *anyopaque, _: Allocator, _: u64) anyerror![]replay_source_mod.PendingDocumentGroup {
        return error.Unsupported;
    }
    fn isVisible(_: *anyopaque, _: u64) anyerror!bool {
        return error.Unsupported;
    }
};

test "catchUpWindow promotes resolution artifacts referenced by a replay record" {
    const alloc = testing.allocator;
    var map = MapStore{ .alloc = alloc };
    defer map.deinit();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    try map.put(resolution_key, sample_resolution);

    const payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 9,
        .changed_artifact_keys = &.{resolution_key},
        .target_hints = &.{.promotion},
    });
    defer alloc.free(payload);

    var fake_source = FakeSource{ .records = &.{.{ .sequence = 9, .payload = payload }} };
    var capture = CaptureSink{ .alloc = alloc };
    defer capture.deinit();

    const max_seen = try catchUpWindow(alloc, fake_source.source(), map.store(), capture.sink(), 1, 0);
    try testing.expectEqual(@as(u64, 9), max_seen);
    try testing.expectEqual(@as(usize, 2), capture.keys.items.len);
}

test "catchUpWindow leaves replay unapplied when a resolution artifact is malformed" {
    const alloc = testing.allocator;
    var map = MapStore{ .alloc = alloc };
    defer map.deinit();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:bad", "resolution_v1");
    defer alloc.free(resolution_key);
    try map.put(resolution_key, "{}");

    const payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 9,
        .changed_artifact_keys = &.{resolution_key},
        .target_hints = &.{.promotion},
    });
    defer alloc.free(payload);

    var fake_source = FakeSource{ .records = &.{.{ .sequence = 9, .payload = payload }} };
    var capture = CaptureSink{ .alloc = alloc };
    defer capture.deinit();

    try testing.expectError(error.InvalidResolution, catchUpWindow(alloc, fake_source.source(), map.store(), capture.sink(), 1, 0));
    try testing.expectEqual(@as(usize, 0), capture.keys.items.len);
}

test "PromotionRuntime waits on source-shard leadership before promoting" {
    const alloc = testing.allocator;
    var map = MapStore{ .alloc = alloc };
    defer map.deinit();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    try map.put(resolution_key, sample_resolution);

    const payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 9,
        .changed_artifact_keys = &.{resolution_key},
        .target_hints = &.{.promotion},
    });
    defer alloc.free(payload);

    var fake_source = FakeSource{ .records = &.{.{ .sequence = 9, .payload = payload }} };
    var capture = CaptureSink{ .alloc = alloc };
    defer capture.deinit();
    var owner = ToggleOwner{ .local_owner = false };
    var store_handle = try resolution_runtime.initRuntimeStore(alloc, map.backendStore());
    defer store_handle.deinit();

    var runtime = PromotionRuntime{
        .alloc = alloc,
        .store_handle = store_handle,
        .replay_source = fake_source.source(),
        .owner = owner.owner(),
        .sink = capture.sink(),
        .sink_available = .init(true),
        .missing_sink_policy = .wait,
        .applied_sequence = .init(1),
        .target_sequence = .init(9),
        .error_count = .init(0),
        .shutdown_flag = .init(false),
        .io_impl = null,
        .future = null,
    };

    try runtime.catchUp();
    try testing.expectEqual(@as(u64, 1), runtime.applied_sequence.load(.acquire));
    try testing.expectEqual(@as(usize, 0), capture.keys.items.len);
    const follower_stats = runtime.stats();
    try testing.expect(follower_stats.blocked);
    try testing.expectEqualStrings("not_source_group_leader", follower_stats.blocked_reason);

    owner.local_owner = true;
    try runtime.catchUp();
    try testing.expectEqual(@as(u64, 9), runtime.applied_sequence.load(.acquire));
    try testing.expectEqual(@as(usize, 2), capture.keys.items.len);
    const leader_stats = runtime.stats();
    try testing.expect(!leader_stats.blocked);

    runtime.store_handle = undefined;
}

test "PromotionRuntime stats are nonblocking while catch-up owns the mutex" {
    var runtime = PromotionRuntime{
        .alloc = testing.allocator,
        .store_handle = undefined,
        .replay_source = undefined,
        .owner = null,
        .sink = null,
        .sink_available = .init(false),
        .missing_sink_policy = .wait,
        .applied_sequence = .init(1),
        .target_sequence = .init(2),
        .error_count = .init(3),
        .shutdown_flag = .init(false),
        .io_impl = null,
        .future = null,
    };
    lockMutex(&runtime.catch_up_mutex);
    defer runtime.catch_up_mutex.unlock();

    const stats_snapshot = runtime.stats();
    try testing.expect(stats_snapshot.catch_up_required);
    try testing.expect(stats_snapshot.blocked);
    try testing.expectEqual(@as(u64, 3), stats_snapshot.error_count);
}

test "catchUpWindow with no matching records returns from_sequence" {
    const alloc = testing.allocator;
    var map = MapStore{ .alloc = alloc };
    defer map.deinit();
    var capture = CaptureSink{ .alloc = alloc };
    defer capture.deinit();
    var fake_source = FakeSource{ .records = &.{} };

    const max_seen = try catchUpWindow(alloc, fake_source.source(), map.store(), capture.sink(), 5, 0);
    try testing.expectEqual(@as(u64, 5), max_seen);
    try testing.expectEqual(@as(usize, 0), capture.keys.items.len);
}

test "PromotionRuntime compiles end-to-end" {
    testing.refAllDecls(PromotionRuntime);
}
