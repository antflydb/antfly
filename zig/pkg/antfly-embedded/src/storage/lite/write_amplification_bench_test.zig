// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Lite write-amplification counting harness.
//!
//! `zig build lite-write-amplification` runs this file's report test and
//! prints a JSON report to stderr. See
//! `docs/design/lite-performance/README.md` for how to run it, how it
//! relates to `scripts/bench/lite_sqlite_comparison.py`, and how to read the
//! output. This file supplies its own instrumented `std.Io` (see
//! `CountingIo` below) to the public embedding seam
//! (`capi.openLiteHandleWithRuntime`) so no production file needs to know
//! about counting bytes/syncs; the small number of `write_amplification_trace`
//! stage-tag scopes elsewhere in `storage/` only mark *which* logical stage
//! is doing durable I/O right now.

const std = @import("std");
const api = @import("../../capi/db.zig");
const capi_types = @import("../../capi/types.zig");
const db_mod = api.db_mod;
const wal_trace = @import("../write_amplification_trace.zig");
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;

const default_updates: usize = 128;
const default_writers: usize = 8;
const text_bytes: usize = 1024;
const graph_index_name = "wal_graph";
// Caller-supplied vector dimensions for the dense/sparse write-amplification
// modes below (no embedding inference: see `denseVectorPayload`/
// `sparseVectorPayload`). Chosen to be representative of a small embedding
// model's output size while keeping per-update JSON bodies small.
const dense_dims: usize = 128;
const sparse_terms: usize = 32;
const dense_index_name = "dense_vec_wamp_v0";
const sparse_index_name = "sparse_vec_wamp_v0";

// ---------------------------------------------------------------------------
// Instrumented std.Io: the harness's own leaf IO counting layer. Delegates
// every call to the real `std.testing.io` implementation and additionally
// counts durable syncs and write/pwrite submissions, attributed to whatever
// `write_amplification_trace.Stage` is current on the calling thread.
// ---------------------------------------------------------------------------

var counted_vtable: std.Io.VTable = undefined;
var counted_vtable_ready = std.atomic.Value(bool).init(false);

const CountingShim = struct {
    fn fileSync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
        _ = userdata;
        try std.testing.io.vtable.fileSync(std.testing.io.userdata, file);
        wal_trace.recordSync();
    }
    fn fileWritePositional(
        userdata: ?*anyopaque,
        file: std.Io.File,
        header: []const u8,
        data: []const []const u8,
        splat: usize,
        offset: u64,
    ) std.Io.File.WritePositionalError!usize {
        _ = userdata;
        const written = try std.testing.io.vtable.fileWritePositional(std.testing.io.userdata, file, header, data, splat, offset);
        wal_trace.recordWrite(written);
        return written;
    }
};

// Only ever called from the harness's own single setup thread, before any
// writer threads are spawned (see Fixture.open / measureConcurrent), so a
// plain flag is enough: no concurrent first-call race to guard against.
fn countingIo() std.Io {
    if (!counted_vtable_ready.load(.monotonic)) {
        counted_vtable = std.testing.io.vtable.*;
        counted_vtable.fileSync = CountingShim.fileSync;
        counted_vtable.fileWritePositional = CountingShim.fileWritePositional;
        counted_vtable_ready.store(true, .monotonic);
    }
    return .{ .userdata = std.testing.io.userdata, .vtable = &counted_vtable };
}

// ---------------------------------------------------------------------------
// Fixture: one durable Lite handle, driven through the same public embedding
// seam (`capi.openLiteHandleWithRuntime`) the Go/Python/Rust/TS SDKs reach
// through the C ABI, but with a `.manual` backend runtime and our counting
// `std.Io` so catch-up/maintenance run deterministically and inline on the
// calling thread, matching the synchronous `sync_level` the SDK bench
// (`scripts/bench/lite_sqlite_comparison.py`) exercises.
// ---------------------------------------------------------------------------

/// Which managed index (if any) a `Fixture` provisions before the measured
/// writes begin. `dense_vector`/`sparse_vector` are `external: true`
/// embeddings indexes: the caller supplies the vector directly through
/// `_embeddings` (see `insertOneDenseVector`/`insertOneSparseVector`), so no
/// embedder runs and no inference happens on the write path -- this harness
/// measures only the index-storage write amplification, not model latency.
const Mode = enum { primary_only, full_text, dense_vector, sparse_vector };

const Fixture = struct {
    alloc: std.mem.Allocator,
    directory: TestDirectory,
    path: []const u8,
    runtime: db_mod.background_runtime.BackendRuntimeHandle,
    handle: *anyopaque,

    fn open(alloc: std.mem.Allocator, comptime name: []const u8, mode: Mode) !Fixture {
        var directory = try TestDirectory.init(name);
        errdefer directory.cleanup();
        try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
        const path = try std.fmt.allocPrint(alloc, "{s}/db.aflite", .{directory.path()});
        errdefer alloc.free(path);

        var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{
            .backend = .manual,
            .borrowed_io = .{ .general = countingIo() },
        });
        errdefer runtime.deinit();

        const handle = try api.openLiteHandleWithRuntime(alloc, path, countingIo(), runtime.ptr(), .{
            .create = true,
            .hosted = false,
            .no_sync = false,
        });

        // Every mode but `full_text` drops the default full-text index so
        // its own writes/catch-up never show up in this mode's attribution.
        if (mode != .full_text) {
            var removed = false;
            try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_delete_index(handle, .fromSlice("full_text_index_v0"), &removed));
        }
        switch (mode) {
            .primary_only, .full_text => {},
            .dense_vector => try addExternalVectorIndex(alloc, handle, dense_index_name, false),
            .sparse_vector => try addExternalVectorIndex(alloc, handle, sparse_index_name, true),
        }

        return .{ .alloc = alloc, .directory = directory, .path = path, .runtime = runtime, .handle = handle };
    }

    /// Graph mode: no full-text index (isolates graph forward/reverse store
    /// cost from text's own), plus one caller-supplied graph index with no
    /// metrics/resolvers/artifact sources configured ("no inference" --
    /// task asks for edges the caller supplies directly via `graph_writes`,
    /// not derived through a projection/extraction pipeline).
    fn openGraph(alloc: std.mem.Allocator, comptime name: []const u8) !Fixture {
        var directory = try TestDirectory.init(name);
        errdefer directory.cleanup();
        try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
        const path = try std.fmt.allocPrint(alloc, "{s}/db.aflite", .{directory.path()});
        errdefer alloc.free(path);

        var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{
            .backend = .manual,
            .borrowed_io = .{ .general = countingIo() },
        });
        errdefer runtime.deinit();

        const handle = try api.openLiteHandleWithRuntime(alloc, path, countingIo(), runtime.ptr(), .{
            .create = true,
            .hosted = false,
            .no_sync = false,
        });

        var removed = false;
        try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_delete_index(handle, .fromSlice("full_text_index_v0"), &removed));
        try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_add_index_json(handle, .fromSlice(
            "{\"name\":\"" ++ graph_index_name ++ "\",\"kind\":\"graph\",\"config_json\":{}}",
        )));

        return .{ .alloc = alloc, .directory = directory, .path = path, .runtime = runtime, .handle = handle };
    }

    fn close(self: *Fixture) void {
        api.antfly_db_close(self.handle);
        self.runtime.deinit();
        self.alloc.free(self.path);
        self.directory.cleanup();
    }
};

fn payload(buffer: []u8, worker: usize, sequence: usize) []const u8 {
    const prefix = std.fmt.bufPrint(buffer, "matchedworker{d}sequence{d} ", .{ worker, sequence }) catch unreachable;
    var offset = prefix.len;
    const filler = "searchable churn alpha beta gamma delta ";
    while (offset < text_bytes) {
        const remaining = text_bytes - offset;
        const chunk = if (remaining < filler.len) filler[0..remaining] else filler;
        @memcpy(buffer[offset .. offset + chunk.len], chunk);
        offset += chunk.len;
    }
    return buffer[0..text_bytes];
}

fn insertOne(alloc: std.mem.Allocator, handle: *anyopaque, key: []const u8, text: []const u8, sync_level: []const u8) !void {
    const key_json = try jsonString(alloc, key);
    defer alloc.free(key_json);
    const text_json = try jsonString(alloc, text);
    defer alloc.free(text_json);
    const sync_level_json = try jsonString(alloc, sync_level);
    defer alloc.free(sync_level_json);
    const body = try std.fmt.allocPrint(alloc, "{{\"inserts\":{{{s}:{{\"body\":{s}}}}},\"sync_level\":{s}}}", .{
        key_json,
        text_json,
        sync_level_json,
    });
    defer alloc.free(body);
    var output: capi_types.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_batch_json(handle, .fromSlice(body), &output));
}

fn jsonString(alloc: std.mem.Allocator, value: []const u8) ![]const u8 {
    return std.json.Stringify.valueAlloc(alloc, value, .{});
}

/// One single-doc write that adds (first occurrence of `source`/`target`
/// pair) or changes (later occurrences; weight varies by `sequence`) a
/// caller-supplied graph edge -- no projection/extraction, matching the
/// `graph_writes` wire field `GraphEdgeWrite` maps to
/// (`storage/db/graph_edge_types.zig`). `sync_level` "full_index" blocks
/// until the edge is visible through the graph index, the sync level this
/// harness's graph modes measure at.
fn insertEdge(alloc: std.mem.Allocator, handle: *anyopaque, source: []const u8, target: []const u8, sequence: usize, sync_level: []const u8) !void {
    const source_json = try jsonString(alloc, source);
    defer alloc.free(source_json);
    const target_json = try jsonString(alloc, target);
    defer alloc.free(target_json);
    const sync_level_json = try jsonString(alloc, sync_level);
    defer alloc.free(sync_level_json);
    const body = try std.fmt.allocPrint(
        alloc,
        "{{\"graph_writes\":[{{\"index_name\":\"" ++ graph_index_name ++ "\",\"source\":{s},\"target\":{s},\"edge_type\":\"rel\",\"weight\":{d}}}],\"sync_level\":{s}}}",
        .{ source_json, target_json, @as(f64, @floatFromInt(1 + sequence % 128)), sync_level_json },
    );
    defer alloc.free(body);
    var output: capi_types.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_batch_json(handle, .fromSlice(body), &output));
}

/// Creates an `external: true` dense/sparse vector index through the same
/// public `antfly_db_add_index_json` seam the SDKs use, matching the exact
/// JSON shape `capi-conformance/cases/dense_vector_search.json` exercises
/// in CI (`"kind":"dense_vector"`/`"sparse_vector"` with a nested
/// `config_json` string -- NOT the `"type":"embeddings"` shorthand, whose
/// flat-object translation does not supply the `"field"`/`"dims"` keys
/// `parseDenseConfig`/`parseSparseConfig` in `catalog/index_manager.zig`
/// require). `external: true` means vectors are supplied directly by the
/// caller through `_embeddings` on each write -- no embedder, no generator,
/// no inference -- matching this harness's "caller-supplied vectors"
/// measurement goal.
fn addExternalVectorIndex(alloc: std.mem.Allocator, handle: *anyopaque, comptime index_name: []const u8, sparse: bool) !void {
    const inner = if (sparse)
        try std.fmt.allocPrint(alloc, "{{\"field\":\"embedding\",\"external\":true}}", .{})
    else
        try std.fmt.allocPrint(alloc, "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"l2_squared\",\"external\":true}}", .{dense_dims});
    defer alloc.free(inner);
    const inner_json = try jsonString(alloc, inner);
    defer alloc.free(inner_json);
    const kind = if (sparse) "sparse_vector" else "dense_vector";
    const body = try std.fmt.allocPrint(alloc, "{{\"name\":\"{s}\",\"kind\":\"{s}\",\"config_json\":{s}}}", .{ index_name, kind, inner_json });
    defer alloc.free(body);
    try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_add_index_json(handle, .fromSlice(body)));
}

/// Deterministic, cheap-to-generate dense vector: no embedder runs, so the
/// exact values are irrelevant to this harness -- only their count
/// (`dense_dims`) and the fact that they are caller-supplied matter.
fn denseVectorJsonAlloc(alloc: std.mem.Allocator, seed: usize) ![]const u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(alloc);
    try buf.append(alloc, '[');
    var number_buf: [32]u8 = undefined;
    for (0..dense_dims) |i| {
        if (i != 0) try buf.append(alloc, ',');
        const v = @as(f64, @floatFromInt((seed + i) % 1000)) / 1000.0;
        const text = try std.fmt.bufPrint(&number_buf, "{d:.4}", .{v});
        try buf.appendSlice(alloc, text);
    }
    try buf.append(alloc, ']');
    return try buf.toOwnedSlice(alloc);
}

/// Deterministic sparse (token id -> weight) map with `sparse_terms`
/// entries, matching the wire shape documented in
/// `specs/openapi/antfly/metadata.yaml` for a manual `_embeddings` sparse
/// write: `{"<token id>": <weight>, ...}`.
fn sparseVectorJsonAlloc(alloc: std.mem.Allocator, seed: usize) ![]const u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(alloc);
    try buf.append(alloc, '{');
    var entry_buf: [48]u8 = undefined;
    for (0..sparse_terms) |i| {
        if (i != 0) try buf.append(alloc, ',');
        const token_id = (seed * sparse_terms + i) % 50_000;
        const weight = @as(f64, @floatFromInt(1 + (seed + i) % 9)) / 2.0;
        const text = try std.fmt.bufPrint(&entry_buf, "\"{d}\":{d:.2}", .{ token_id, weight });
        try buf.appendSlice(alloc, text);
    }
    try buf.append(alloc, '}');
    return try buf.toOwnedSlice(alloc);
}

fn insertOneVector(
    alloc: std.mem.Allocator,
    handle: *anyopaque,
    key: []const u8,
    comptime index_name: []const u8,
    vector_json: []const u8,
    sync_level: []const u8,
) !void {
    const key_json = try jsonString(alloc, key);
    defer alloc.free(key_json);
    const sync_level_json = try jsonString(alloc, sync_level);
    defer alloc.free(sync_level_json);
    const body = try std.fmt.allocPrint(
        alloc,
        "{{\"inserts\":{{{s}:{{\"_embeddings\":{{\"{s}\":{s}}}}}}},\"sync_level\":{s}}}",
        .{ key_json, index_name, vector_json, sync_level_json },
    );
    defer alloc.free(body);
    var output: capi_types.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_batch_json(handle, .fromSlice(body), &output));
}

// ---------------------------------------------------------------------------
// Measurement modes
// ---------------------------------------------------------------------------

const ModeReport = struct {
    updates: usize,
    report: wal_trace.Report,
    /// Wall-clock duration of the measured loop. Zero means "not timed".
    /// Sync/write/op counts never depend on build mode (see the README);
    /// this is the one field in the report that does, which is why every
    /// consumer of it is labeled "indicative only, Debug".
    elapsed_ns: u64 = 0,

    fn perUpdate(self: ModeReport, lane: wal_trace.Lane) f64 {
        return @as(f64, @floatFromInt(self.report.totalSyncs(lane))) / @as(f64, @floatFromInt(self.updates));
    }

    fn totalCommitsOverall(self: ModeReport) u64 {
        var total: u64 = 0;
        for (std.meta.tags(wal_trace.Stage)) |s| {
            const counts = self.report.stage(s);
            total += counts.commits_calling_thread + counts.commits_background;
        }
        return total;
    }

    /// Every durable fsync this run issued, calling-thread and background
    /// combined, per logical update -- the harness's single "total I/O
    /// durability cost" number, as opposed to `perUpdate`'s calling-thread-
    /// only rate.
    fn fsyncsPerWriteOverall(self: ModeReport) f64 {
        const total = self.report.totalSyncs(.calling_thread) + self.report.totalSyncs(.background);
        return @as(f64, @floatFromInt(total)) / @as(f64, @floatFromInt(self.updates));
    }

    /// Every durable commit (native commit_sequence advance) this run
    /// issued, per logical update. The inverse of group size when every
    /// commit groups the same number of updates; reported separately
    /// because real runs mix grouped and ungrouped commits across stages.
    fn durableCommitsPerWrite(self: ModeReport) f64 {
        return @as(f64, @floatFromInt(self.totalCommitsOverall())) / @as(f64, @floatFromInt(self.updates));
    }

    /// How many logical updates, on average, one durable primary-commit
    /// transaction covered. This is the group-commit metric: it reads 1.0
    /// when every update gets its own commit (no grouping, e.g. one
    /// writer with no contention) and rises above 1.0 only when concurrent
    /// writers' intents actually got bundled into shared commits.
    fn meanPrimaryGroupSize(self: ModeReport) f64 {
        const commits = self.report.stage(.primary_commit).commits_calling_thread;
        if (commits == 0) return 0;
        return @as(f64, @floatFromInt(self.updates)) / @as(f64, @floatFromInt(commits));
    }

    /// Indicative only: wall-clock throughput is sensitive to build mode,
    /// machine load and scheduling, unlike every other field here. Debug
    /// by default; see the README for a ReleaseFast run.
    fn writesPerSecondIndicative(self: ModeReport) f64 {
        if (self.elapsed_ns == 0) return 0;
        const seconds = @as(f64, @floatFromInt(self.elapsed_ns)) / 1_000_000_000.0;
        if (seconds <= 0) return 0;
        return @as(f64, @floatFromInt(self.updates)) / seconds;
    }
};

fn measureSingleWriter(alloc: std.mem.Allocator, comptime name: []const u8, indexed: bool, updates: usize) !ModeReport {
    var fixture = try Fixture.open(alloc, name, if (indexed) .full_text else .primary_only);
    defer fixture.close();

    var buffer: [text_bytes]u8 = undefined;
    var key_buffer: [32]u8 = undefined;
    wal_trace.reset();
    wal_trace.registerCallingThread();
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..updates) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "doc-{d}", .{i % 128});
        const text = payload(&buffer, 0, i);
        try insertOne(alloc, fixture.handle, key, text, if (indexed) "full_index" else "write");
    }
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    return .{ .updates = updates, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

/// Graph counterpart to `measureSingleWriter`'s indexed mode: single writer,
/// `updates` single-doc writes that each add or change one caller-supplied
/// edge, at `sync_level: "full_index"` (graph visibility required before
/// ack). Only `graph_edge_cardinality` distinct (source, target) pairs
/// exist, so with `default_updates` (128) this exercises both "add" (the
/// first `graph_edge_cardinality` writes) and "change" (every write after
/// that touches an already-existing edge, varying its weight).
const graph_edge_cardinality: usize = 16;

fn measureSingleWriterGraph(alloc: std.mem.Allocator, comptime name: []const u8, updates: usize) !ModeReport {
    var fixture = try Fixture.openGraph(alloc, name);
    defer fixture.close();

    var source_buffer: [32]u8 = undefined;
    var target_buffer: [32]u8 = undefined;
    wal_trace.reset();
    wal_trace.registerCallingThread();
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..updates) |i| {
        const source = try std.fmt.bufPrint(&source_buffer, "doc-{d}", .{i % graph_edge_cardinality});
        const target = try std.fmt.bufPrint(&target_buffer, "doc-{d}", .{(i + 1) % graph_edge_cardinality});
        try insertEdge(alloc, fixture.handle, source, target, i, "full_index");
    }
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    return .{ .updates = updates, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

/// Single-writer dense/sparse vector mode: one document per transaction,
/// each carrying a caller-supplied vector (no embedder/inference), acked at
/// `full_index` -- the sync level that requires this index's own write to be
/// visible before the batch call returns, mirroring `measureSingleWriter`'s
/// `indexed` mode but for a vector index instead of full-text.
fn measureSingleWriterVector(alloc: std.mem.Allocator, comptime name: []const u8, sparse: bool, updates: usize) !ModeReport {
    var fixture = try Fixture.open(alloc, name, if (sparse) .sparse_vector else .dense_vector);
    defer fixture.close();

    var key_buffer: [32]u8 = undefined;
    wal_trace.reset();
    wal_trace.registerCallingThread();
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..updates) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "doc-{d}", .{i % 128});
        const vector_json = if (sparse) try sparseVectorJsonAlloc(alloc, i) else try denseVectorJsonAlloc(alloc, i);
        defer alloc.free(vector_json);
        if (sparse) {
            try insertOneVector(alloc, fixture.handle, key, sparse_index_name, vector_json, "full_index");
        } else {
            try insertOneVector(alloc, fixture.handle, key, dense_index_name, vector_json, "full_index");
        }
    }
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    return .{ .updates = updates, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

/// A plain spin barrier over a fixed, known set of OS threads. Every writer
/// thread calls `wait` immediately before each of its updates, so all
/// `participants` threads arrive at Lite's single-writer admission queue
/// for a given round at (as close to) the same moment as the OS scheduler
/// allows: real, deterministic contention every round, rather than leaving
/// overlap to chance. Before this, the 8-writer modes spawned real OS
/// threads but gave them no reason to ever be waiting at the same time, so
/// each update's commit reliably finished before the next one arrived and
/// group commit never had a window to bundle anything -- the harness
/// reported numbers identical to one writer doing the same total work
/// serially (see the former "Known limitations" note in the README).
const Barrier = struct {
    participants: usize,
    count: std.atomic.Value(usize) = .init(0),
    generation: std.atomic.Value(usize) = .init(0),

    fn wait(self: *Barrier) void {
        const gen = self.generation.load(.acquire);
        const arrived = self.count.fetchAdd(1, .acq_rel) + 1;
        if (arrived == self.participants) {
            self.count.store(0, .release);
            _ = self.generation.fetchAdd(1, .release);
        } else {
            while (self.generation.load(.acquire) == gen) std.Thread.yield() catch {};
        }
    }
};

const ConcurrentWorker = struct {
    alloc: std.mem.Allocator,
    handle: *anyopaque,
    worker: usize,
    updates_per_writer: usize,
    sync_level: []const u8,
    barrier: *Barrier,
    err: ?anyerror = null,

    fn run(self: *ConcurrentWorker) void {
        wal_trace.registerCallingThread();
        var buffer: [text_bytes]u8 = undefined;
        var key_buffer: [32]u8 = undefined;
        for (0..self.updates_per_writer) |i| {
            self.barrier.wait();
            const key = std.fmt.bufPrint(&key_buffer, "w{d}-doc-{d}", .{ self.worker, i % 128 }) catch |err| {
                self.err = err;
                return;
            };
            const text = payload(&buffer, self.worker, i);
            insertOne(self.alloc, self.handle, key, text, self.sync_level) catch |err| {
                self.err = err;
                return;
            };
        }
    }
};

/// Real concurrent threads issuing writes through the same public path as
/// the single-writer modes (`insertOne` -> `antfly_db_batch_json`), with
/// `indexed` selecting the same primary-only/full_index split
/// `measureSingleWriter` uses. Every round is barrier-synchronized (see
/// `Barrier`) so the comparison against the matching single-writer mode
/// actually reflects contention, not scheduling luck.
fn measureConcurrent(alloc: std.mem.Allocator, comptime name: []const u8, indexed: bool, writers: usize, updates_per_writer: usize) !ModeReport {
    var fixture = try Fixture.open(alloc, name, if (indexed) .full_text else .primary_only);
    defer fixture.close();

    wal_trace.reset();
    var barrier = Barrier{ .participants = writers };
    var workers = try alloc.alloc(ConcurrentWorker, writers);
    defer alloc.free(workers);
    var threads = try alloc.alloc(std.Thread, writers);
    defer alloc.free(threads);
    const sync_level: []const u8 = if (indexed) "full_index" else "write";
    for (0..writers) |w| workers[w] = .{
        .alloc = alloc,
        .handle = fixture.handle,
        .worker = w,
        .updates_per_writer = updates_per_writer,
        .sync_level = sync_level,
        .barrier = &barrier,
    };
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..writers) |w| threads[w] = try std.Thread.spawn(.{}, ConcurrentWorker.run, .{&workers[w]});
    for (threads) |thread| thread.join();
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    for (workers) |worker| if (worker.err) |err| return err;
    return .{ .updates = writers * updates_per_writer, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

const ConcurrentGraphWorker = struct {
    alloc: std.mem.Allocator,
    handle: *anyopaque,
    worker: usize,
    updates_per_writer: usize,
    barrier: *Barrier,
    err: ?anyerror = null,

    fn run(self: *ConcurrentGraphWorker) void {
        wal_trace.registerCallingThread();
        var source_buffer: [32]u8 = undefined;
        var target_buffer: [32]u8 = undefined;
        for (0..self.updates_per_writer) |i| {
            self.barrier.wait();
            // Per-worker node namespace, like the text/primary modes' "w{d}-
            // doc-{d}" keys: isolates one worker's edges from another's so
            // this measures contention on Lite's single-writer admission
            // queue, not a second-order graph identity race.
            const source = std.fmt.bufPrint(&source_buffer, "w{d}-doc-{d}", .{ self.worker, i % graph_edge_cardinality }) catch |err| {
                self.err = err;
                return;
            };
            const target = std.fmt.bufPrint(&target_buffer, "w{d}-doc-{d}", .{ self.worker, (i + 1) % graph_edge_cardinality }) catch |err| {
                self.err = err;
                return;
            };
            insertEdge(self.alloc, self.handle, source, target, i, "full_index") catch |err| {
                self.err = err;
                return;
            };
        }
    }
};

const ConcurrentVectorWorker = struct {
    alloc: std.mem.Allocator,
    handle: *anyopaque,
    worker: usize,
    updates_per_writer: usize,
    sparse: bool,
    barrier: *Barrier,
    err: ?anyerror = null,

    fn run(self: *ConcurrentVectorWorker) void {
        wal_trace.registerCallingThread();
        var key_buffer: [32]u8 = undefined;
        for (0..self.updates_per_writer) |i| {
            self.barrier.wait();
            const key = std.fmt.bufPrint(&key_buffer, "w{d}-doc-{d}", .{ self.worker, i % 128 }) catch |err| {
                self.err = err;
                return;
            };
            const seed = self.worker * 1_000_003 + i;
            if (self.sparse) {
                const vj = sparseVectorJsonAlloc(self.alloc, seed) catch |err| {
                    self.err = err;
                    return;
                };
                defer self.alloc.free(vj);
                insertOneVector(self.alloc, self.handle, key, sparse_index_name, vj, "full_index") catch |err| {
                    self.err = err;
                    return;
                };
            } else {
                const vj = denseVectorJsonAlloc(self.alloc, seed) catch |err| {
                    self.err = err;
                    return;
                };
                defer self.alloc.free(vj);
                insertOneVector(self.alloc, self.handle, key, dense_index_name, vj, "full_index") catch |err| {
                    self.err = err;
                    return;
                };
            }
        }
    }
};

/// Graph counterpart to `measureConcurrent`: `writers` real OS threads, each
/// writing to its own node namespace, barrier-synchronized the same way.
fn measureConcurrentGraph(alloc: std.mem.Allocator, comptime name: []const u8, writers: usize, updates_per_writer: usize) !ModeReport {
    var fixture = try Fixture.openGraph(alloc, name);
    defer fixture.close();

    wal_trace.reset();
    var barrier = Barrier{ .participants = writers };
    var workers = try alloc.alloc(ConcurrentGraphWorker, writers);
    defer alloc.free(workers);
    var threads = try alloc.alloc(std.Thread, writers);
    defer alloc.free(threads);
    for (0..writers) |w| workers[w] = .{
        .alloc = alloc,
        .handle = fixture.handle,
        .worker = w,
        .updates_per_writer = updates_per_writer,
        .barrier = &barrier,
    };
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..writers) |w| threads[w] = try std.Thread.spawn(.{}, ConcurrentGraphWorker.run, .{&workers[w]});
    for (threads) |thread| thread.join();
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    for (workers) |worker| if (worker.err) |err| return err;
    return .{ .updates = writers * updates_per_writer, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

/// Real concurrent threads issuing dense/sparse vector writes, mirroring
/// `measureConcurrent`'s barrier-synchronized contention shape but for
/// `measureSingleWriterVector`'s caller-supplied-vector workload.
fn measureConcurrentVector(alloc: std.mem.Allocator, comptime name: []const u8, sparse: bool, writers: usize, updates_per_writer: usize) !ModeReport {
    var fixture = try Fixture.open(alloc, name, if (sparse) .sparse_vector else .dense_vector);
    defer fixture.close();

    wal_trace.reset();
    var barrier = Barrier{ .participants = writers };
    var workers = try alloc.alloc(ConcurrentVectorWorker, writers);
    defer alloc.free(workers);
    var threads = try alloc.alloc(std.Thread, writers);
    defer alloc.free(threads);
    for (0..writers) |w| workers[w] = .{
        .alloc = alloc,
        .handle = fixture.handle,
        .worker = w,
        .updates_per_writer = updates_per_writer,
        .sparse = sparse,
        .barrier = &barrier,
    };
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..writers) |w| threads[w] = try std.Thread.spawn(.{}, ConcurrentVectorWorker.run, .{&workers[w]});
    for (threads) |thread| thread.join();
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    for (workers) |worker| if (worker.err) |err| return err;
    return .{ .updates = writers * updates_per_writer, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

// ---------------------------------------------------------------------------
// JSON report
// ---------------------------------------------------------------------------

fn printStageTable(mode_name: []const u8, mode: ModeReport) void {
    std.debug.print(
        "  {s}: updates={d} calling_thread_syncs_per_update={d:.3} background_syncs_per_update={d:.3} fsyncs_per_write_overall={d:.3} durable_commits_per_write={d:.3} mean_primary_group_size={d:.3} writes_per_second_indicative_debug={d:.1}\n",
        .{
            mode_name,
            mode.updates,
            mode.perUpdate(.calling_thread),
            mode.perUpdate(.background),
            mode.fsyncsPerWriteOverall(),
            mode.durableCommitsPerWrite(),
            mode.meanPrimaryGroupSize(),
            mode.writesPerSecondIndicative(),
        },
    );
    for (std.meta.tags(wal_trace.Stage)) |stage| {
        const counts = mode.report.stage(stage);
        if (counts.sync_calling_thread + counts.sync_background + counts.write_calls_calling_thread + counts.write_calls_background == 0) continue;
        std.debug.print("    {s}: sync(calling={d},background={d}) write_bytes(calling={d},background={d}) commits(calling={d},background={d})\n", .{
            @tagName(stage),
            counts.sync_calling_thread,
            counts.sync_background,
            counts.write_bytes_calling_thread,
            counts.write_bytes_background,
            counts.commits_calling_thread,
            counts.commits_background,
        });
    }
    for (std.meta.tags(wal_trace.Op)) |op| {
        const count = mode.report.op(op);
        if (count == 0) continue;
        std.debug.print("    op {s}: {d}\n", .{ @tagName(op), count });
    }
}

const StageJson = struct {
    sync_calling_thread: u64,
    sync_background: u64,
    write_calls_calling_thread: u64,
    write_calls_background: u64,
    write_bytes_calling_thread: u64,
    write_bytes_background: u64,
    commits_calling_thread: u64,
    commits_background: u64,
};

const StagesJson = struct {
    primary_commit: StageJson,
    artifact_stage: StageJson,
    artifact_link: StageJson,
    artifact_pin: StageJson,
    lsm_wal: StageJson,
    progress: StageJson,
    status_snapshot: StageJson,
    replay_retirement: StageJson,
    artifact_cleanup: StageJson,
    vfs_write: StageJson,
    vfs_append: StageJson,
    vfs_rename: StageJson,
    vfs_delete: StageJson,
    vfs_sync: StageJson,
    vfs_finish: StageJson,
    artifact_lease: StageJson,
    main_txn_commit: StageJson,
    lsm_backend_wal: StageJson,
    graph_forward_commit: StageJson,
    graph_reverse_commit: StageJson,
    graph_metric_commit: StageJson,
    graph_artifact: StageJson,
    other: StageJson,
};

const BackgroundOpsJson = struct {
    source_turn: u64,
    maintenance_lease_acquired: u64,
    serving_runtime_refresh: u64,
    derived_worker_turn: u64,
};

const ModeJson = struct {
    updates: usize,
    calling_thread_syncs_per_update: f64,
    background_syncs_per_update: f64,
    fsyncs_per_write_overall: f64,
    durable_commits_per_write: f64,
    mean_primary_group_size: f64,
    /// Wall-clock writes/s for this mode's run. Indicative only: unlike
    /// every other field in this report, it is sensitive to build mode,
    /// machine load, and scheduling (see the README).
    indicative_writes_per_second_debug: f64,
    stages: StagesJson,
    background_ops: BackgroundOpsJson,
};

fn stageJson(mode: ModeReport, stage: wal_trace.Stage) StageJson {
    const counts = mode.report.stage(stage);
    return .{
        .sync_calling_thread = counts.sync_calling_thread,
        .sync_background = counts.sync_background,
        .write_calls_calling_thread = counts.write_calls_calling_thread,
        .write_calls_background = counts.write_calls_background,
        .write_bytes_calling_thread = counts.write_bytes_calling_thread,
        .write_bytes_background = counts.write_bytes_background,
        .commits_calling_thread = counts.commits_calling_thread,
        .commits_background = counts.commits_background,
    };
}

fn modeJson(mode: ModeReport) ModeJson {
    return .{
        .updates = mode.updates,
        .calling_thread_syncs_per_update = mode.perUpdate(.calling_thread),
        .background_syncs_per_update = mode.perUpdate(.background),
        .fsyncs_per_write_overall = mode.fsyncsPerWriteOverall(),
        .durable_commits_per_write = mode.durableCommitsPerWrite(),
        .mean_primary_group_size = mode.meanPrimaryGroupSize(),
        .indicative_writes_per_second_debug = mode.writesPerSecondIndicative(),
        .stages = .{
            .primary_commit = stageJson(mode, .primary_commit),
            .artifact_stage = stageJson(mode, .artifact_stage),
            .artifact_link = stageJson(mode, .artifact_link),
            .artifact_pin = stageJson(mode, .artifact_pin),
            .lsm_wal = stageJson(mode, .lsm_wal),
            .progress = stageJson(mode, .progress),
            .status_snapshot = stageJson(mode, .status_snapshot),
            .replay_retirement = stageJson(mode, .replay_retirement),
            .artifact_cleanup = stageJson(mode, .artifact_cleanup),
            .vfs_write = stageJson(mode, .vfs_write),
            .vfs_append = stageJson(mode, .vfs_append),
            .vfs_rename = stageJson(mode, .vfs_rename),
            .vfs_delete = stageJson(mode, .vfs_delete),
            .vfs_sync = stageJson(mode, .vfs_sync),
            .vfs_finish = stageJson(mode, .vfs_finish),
            .artifact_lease = stageJson(mode, .artifact_lease),
            .main_txn_commit = stageJson(mode, .main_txn_commit),
            .lsm_backend_wal = stageJson(mode, .lsm_backend_wal),
            .graph_forward_commit = stageJson(mode, .graph_forward_commit),
            .graph_reverse_commit = stageJson(mode, .graph_reverse_commit),
            .graph_metric_commit = stageJson(mode, .graph_metric_commit),
            .graph_artifact = stageJson(mode, .graph_artifact),
            .other = stageJson(mode, .other),
        },
        .background_ops = .{
            .source_turn = mode.report.op(.source_turn),
            .maintenance_lease_acquired = mode.report.op(.maintenance_lease_acquired),
            .serving_runtime_refresh = mode.report.op(.serving_runtime_refresh),
            .derived_worker_turn = mode.report.op(.derived_worker_turn),
        },
    };
}

const ReportJson = struct {
    text_bytes: usize,
    dense_dims: usize,
    sparse_terms: usize,
    primary_only: ModeJson,
    primary_only_8_writers: ModeJson,
    indexed: ModeJson,
    indexed_8_writers: ModeJson,
    /// Single-doc writes that add/change a caller-supplied graph edge (no
    /// projection/inference), at a sync level requiring graph visibility.
    /// See docs/design/lite-performance/lite-single-commit-write-path.md's
    /// graph extension.
    graph: ModeJson,
    graph_8_writers: ModeJson,
    /// Dense/sparse modes: single-doc writes with caller-supplied vectors
    /// (no embedding inference), acked at `full_index` -- the sync level
    /// that requires the vector index's own write to be visible. See
    /// docs/design/lite-performance/lite-single-commit-write-path.md's WS0
    /// row and the dense/sparse-storage design note for how these compare
    /// against `indexed` (full-text) today: both still run on the `.lsm`
    /// backend (lite/backend.zig's `dense_storage_backend`/`sparse_backend`),
    /// so these numbers are the WS0 "before" baseline this harness exists
    /// to capture, not yet a native-catalog "after".
    dense_vector: ModeJson,
    dense_vector_8_writers: ModeJson,
    sparse_vector: ModeJson,
    sparse_vector_8_writers: ModeJson,
};

test "lite write amplification report" {
    const alloc = std.testing.allocator;
    const updates = default_updates;

    const primary = try measureSingleWriter(alloc, "lite-write-amp-primary", false, updates);
    const indexed = try measureSingleWriter(alloc, "lite-write-amp-indexed", true, updates);
    const primary_concurrent = try measureConcurrent(alloc, "lite-write-amp-primary-concurrent", false, default_writers, updates / default_writers);
    const concurrent = try measureConcurrent(alloc, "lite-write-amp-concurrent", true, default_writers, updates / default_writers);
    const graph = try measureSingleWriterGraph(alloc, "lite-write-amp-graph", updates);
    const graph_concurrent = try measureConcurrentGraph(alloc, "lite-write-amp-graph-concurrent", default_writers, updates / default_writers);
    const dense = try measureSingleWriterVector(alloc, "lite-write-amp-dense", false, updates);
    const dense_concurrent = try measureConcurrentVector(alloc, "lite-write-amp-dense-concurrent", false, default_writers, updates / default_writers);
    const sparse = try measureSingleWriterVector(alloc, "lite-write-amp-sparse", true, updates);
    const sparse_concurrent = try measureConcurrentVector(alloc, "lite-write-amp-sparse-concurrent", true, default_writers, updates / default_writers);

    std.debug.print("lite write amplification report (updates={d}, text_bytes={d}, dense_dims={d}, sparse_terms={d}):\n", .{ updates, text_bytes, dense_dims, sparse_terms });
    printStageTable("primary-only", primary);
    printStageTable("primary-only, 8 writers", primary_concurrent);
    printStageTable("full-text-visible (indexed)", indexed);
    printStageTable("full-text-visible, 8 writers", concurrent);
    printStageTable("graph-visible (caller-supplied edges)", graph);
    printStageTable("graph-visible, 8 writers", graph_concurrent);
    printStageTable("dense-vector-visible", dense);
    printStageTable("dense-vector-visible, 8 writers", dense_concurrent);
    printStageTable("sparse-vector-visible", sparse);
    printStageTable("sparse-vector-visible, 8 writers", sparse_concurrent);

    const report: ReportJson = .{
        .text_bytes = text_bytes,
        .dense_dims = dense_dims,
        .sparse_terms = sparse_terms,
        .primary_only = modeJson(primary),
        .primary_only_8_writers = modeJson(primary_concurrent),
        .indexed = modeJson(indexed),
        .indexed_8_writers = modeJson(concurrent),
        .graph = modeJson(graph),
        .graph_8_writers = modeJson(graph_concurrent),
        .dense_vector = modeJson(dense),
        .dense_vector_8_writers = modeJson(dense_concurrent),
        .sparse_vector = modeJson(sparse),
        .sparse_vector_8_writers = modeJson(sparse_concurrent),
    };
    const json_text = try std.json.Stringify.valueAlloc(alloc, report, .{ .whitespace = .indent_2 });
    defer alloc.free(json_text);
    std.debug.print("lite_write_amplification_json_begin\n{s}\nlite_write_amplification_json_end\n", .{json_text});
}

// Regression gate: ceilings start at the measured baseline plus a small
// slack factor. TODO(write-amplification): tighten these once the
// publication-shape work in docs/design/lite-performance/README.md lands;
// today they only catch a large regression, not restore the SQLite-matched
// floor.
test "lite write amplification ceilings" {
    const alloc = std.testing.allocator;
    const updates = default_updates;

    const primary = try measureSingleWriter(alloc, "lite-write-amp-ceil-primary", false, updates);
    const indexed = try measureSingleWriter(alloc, "lite-write-amp-ceil-indexed", true, updates);

    // Slack factor over the measured main@6c0b004e95 baseline (see
    // docs/design/lite-performance/write-amplification-baseline.json):
    // primary-only 3.000, indexed 66.844 calling-thread syncs/update.
    const slack = 1.25;
    try expectAtMost(primary.perUpdate(.calling_thread), 3.0 * slack, "primary-only calling-thread syncs/update");
    try expectAtMost(indexed.perUpdate(.calling_thread), 67.0 * slack, "indexed calling-thread syncs/update");

    // Extend the same ceilings to the 8-writer modes: the writer slot, the
    // DB apply lock and the per-call cross-process path lease are all held
    // across the fsync (see docs/design/lite-performance/
    // lite-single-commit-write-path.md's "throughput falls as writers are
    // added" note), so the existing group-commit queue cannot yet bundle
    // concurrent foreground writers' commits -- measured mean_primary_group_
    // size is 1.0 today, confirmed by this same harness's report test.
    // Concurrency must not make amplification *worse* than the matching
    // single-writer mode, even though it cannot yet make it better; this
    // gate exists so a future regression in the concurrent path specifically
    // (as opposed to the primary/indexed path in general) is still caught.
    const primary_concurrent = try measureConcurrent(alloc, "lite-write-amp-ceil-primary-concurrent", false, default_writers, updates / default_writers);
    const indexed_concurrent = try measureConcurrent(alloc, "lite-write-amp-ceil-indexed-concurrent", true, default_writers, updates / default_writers);
    try expectAtMost(primary_concurrent.perUpdate(.calling_thread), 3.0 * slack, "primary-only 8-writer calling-thread syncs/update");
    try expectAtMost(indexed_concurrent.perUpdate(.calling_thread), 67.0 * slack, "indexed 8-writer calling-thread syncs/update");

    // Graph ceiling over the measured `.native_catalog` baseline (see
    // docs/design/lite-performance/write-amplification-graph-after.json):
    // graph-visible 6.000, 8-writer 6.000 calling-thread syncs/update --
    // down from `.lsm`'s 37.125/39.578 (write-amplification-graph-
    // before.json), but not yet at text's inline-publication 3.1: this
    // harness's graph mode has no primary document write at all (pure
    // `graph_writes`), yet still shows `background_ops.source_turn` and
    // `.derived_worker_turn` incrementing once per update, meaning each
    // edge write is still going through the async journal-append +
    // catch-up pipeline for its *own* synchronous ack instead of an inline
    // same-commit publish. `graph_forward_commit` reads 0 syncs (the
    // ride-along collapsed into `graph_reverse_commit`'s single durable
    // commit, 2 syncs for 1 commit per update -- the native commit
    // protocol's data+slot sync pair), so the remaining ~4 syncs/update
    // (the `other` bucket) are that catch-up path, not forward/reverse
    // adjacency. Closing that gap is the inline prepare/install publication
    // API (design doc §3.3's graph extension; see `GraphIndex.
    // prepareInlineEdgePublication`/`installInlineEdgePublication` below),
    // mirrored on text's already-landed `prepareInlineTextPublication`/
    // `installInlineTextPublication` -- intentionally not wired into the
    // ack path yet (API + tests only; see that section's doc comment).
    const graph = try measureSingleWriterGraph(alloc, "lite-write-amp-ceil-graph", updates);
    const graph_concurrent = try measureConcurrentGraph(alloc, "lite-write-amp-ceil-graph-concurrent", default_writers, updates / default_writers);
    try expectAtMost(graph.perUpdate(.calling_thread), 6.0 * slack, "graph calling-thread syncs/update");
    try expectAtMost(graph_concurrent.perUpdate(.calling_thread), 6.0 * slack, "graph 8-writer calling-thread syncs/update");
}

fn expectAtMost(actual: f64, ceiling: f64, comptime label: []const u8) !void {
    if (actual > ceiling) {
        std.debug.print("{s}: {d:.3} exceeds ceiling {d:.3}\n", .{ label, actual, ceiling });
        return error.WriteAmplificationCeilingExceeded;
    }
}
