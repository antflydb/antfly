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
const docstore_mod = @import("docstore.zig");

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
const algebraic_index_name = "algebraic_wamp_v0";
// Representative algebraic config: one group-by field plus an HLL
// cardinality materialization over it, matching the shape used by
// `storage/db/algebraic/index.zig`'s own "materializes approximate
// per-group cardinality" test. `full_index` sync level (the only level
// that forces synchronous algebraic visibility: see
// `syncLevelRequiresDerivedVisibility` in `storage/db/mutation_commit.zig`)
// requires a write's algebraic facts and this sketch to be visible before
// the batch call returns.
// `max_hll_maintenance_rows_per_tick` bounds each ride-along maintenance
// commit's own rescan/rewrite cost (see `index.zig`'s own doc comment and
// its `"max_hll_maintenance_rows_per_tick": 1` test example). Left at this
// config's default (10,000) a single dirty-group rebuild can rescan far
// more rows than the harness's own tiny groups actually have pending at
// once, pushing a single ride-along commit's own estimated bytes over
// `ride_along_byte_threshold` (1 MiB) and forcing its own self-drain
// instead of truly riding along -- see `measureSingleWriterAlgebraicOverwrite`.
// A small per-tick budget keeps each commit's cost bounded and incremental,
// exactly like WS5 retirement's own "bounded by keys and bytes per turn".
const algebraic_config_json =
    \\{"version":1,"table":"docs","group_fields":[{"name":"region","path":"region","type":"string"},{"name":"customer","path":"customer","type":"string"}],"hll_cardinalities":[{"name":"customers_by_region","group_by":["region"],"value_field":"customer"}],"max_hll_maintenance_rows_per_tick":8}
;
// `measureSingleWriterAlgebraicOverwrite`'s key cycle: small enough (vs.
// `measureSingleWriterAlgebraic`'s 128-key cycle, which at `default_updates
// = 128` never repeats a key and so never overwrites one) that most writes
// in a `default_updates`-sized run land on a key already written earlier in
// the same run with a *different* seed -- and so a different `customer`
// value -- retracting the old fact and marking the HLL cardinality sketch
// dirty (`Index.hllCardinalityDirtyTxn`). `runHllMaintenance`'s own doc
// comment: "Rebuilds any HLL cardinality materialization marked dirty by a
// delete or overwrite" -- only an overwrite (or delete) workload exercises
// its ride-along commit at all; the plain insert-only `algebraic` mode above
// never dirties it.
const algebraic_overwrite_key_cycle: usize = 8;

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
const Mode = enum { primary_only, full_text, dense_vector, sparse_vector, algebraic };

const Fixture = struct {
    alloc: std.mem.Allocator,
    directory: TestDirectory,
    path: []const u8,
    runtime: db_mod.background_runtime.BackendRuntimeHandle,
    handle: *anyopaque,

    fn open(alloc: std.mem.Allocator, comptime name: []const u8, mode: Mode) !Fixture {
        return openWithRideAlongIdle(alloc, name, mode, null);
    }

    /// `ride_along_idle_ns`: see `docstore.Store.ride_along_idle_ns`'s own
    /// doc comment -- `null` keeps the production default (50ms); a mode
    /// that wants to measure ride-along work being drained for free by the
    /// next foreground write, rather than by this barrier, passes a large
    /// value here instead (`measureSingleWriterAlgebraicOverwrite`/
    /// `measureConcurrentAlgebraicOverwrite` below).
    fn openWithRideAlongIdle(alloc: std.mem.Allocator, comptime name: []const u8, mode: Mode, ride_along_idle_ns: ?u64) !Fixture {
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
            .ride_along_idle_ns = ride_along_idle_ns,
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
            .algebraic => try addAlgebraicIndex(alloc, handle),
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

/// Creates the representative algebraic index (`algebraic_config_json`)
/// through the same public `antfly_db_add_index_json` seam the SDKs use.
fn addAlgebraicIndex(alloc: std.mem.Allocator, handle: *anyopaque) !void {
    const inner_json = try jsonString(alloc, algebraic_config_json);
    defer alloc.free(inner_json);
    const body = try std.fmt.allocPrint(alloc, "{{\"name\":\"{s}\",\"kind\":\"algebraic\",\"config_json\":{s}}}", .{ algebraic_index_name, inner_json });
    defer alloc.free(body);
    try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_add_index_json(handle, .fromSlice(body)));
}

/// One document write exercising the algebraic index's group fields --
/// deterministic region/customer pair so repeated writes (same 128-key
/// cycle as `payload`/`insertOne`) touch a bounded, repeating set of HLL
/// cardinality groups instead of growing one unboundedly.
fn insertOneAlgebraic(alloc: std.mem.Allocator, handle: *anyopaque, key: []const u8, seed: usize, sync_level: []const u8) !void {
    const key_json = try jsonString(alloc, key);
    defer alloc.free(key_json);
    const regions = [_][]const u8{ "west", "east", "north", "south" };
    const region = regions[seed % regions.len];
    var customer_buf: [32]u8 = undefined;
    const customer = try std.fmt.bufPrint(&customer_buf, "cust{d}", .{seed % 37});
    const sync_level_json = try jsonString(alloc, sync_level);
    defer alloc.free(sync_level_json);
    const body = try std.fmt.allocPrint(
        alloc,
        "{{\"inserts\":{{{s}:{{\"region\":\"{s}\",\"customer\":\"{s}\"}}}},\"sync_level\":{s}}}",
        .{ key_json, region, customer, sync_level_json },
    );
    defer alloc.free(body);
    var output: capi_types.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_batch_json(handle, .fromSlice(body), &output));
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

/// One single-doc write that carries BOTH a text `body` field (for the
/// default full-text index) and a caller-supplied sparse embedding (for
/// `sparse_index_name`) in the SAME `inserts` entry -- the mixed-kind case
/// lite-single-commit-write-path.md §3.3a asks for: each eligible index
/// (text's `InlineTextPublication`, sparse's `InlineSparsePublication`)
/// must contribute its own commit_participant to the SAME native
/// transaction as the primary row, not fall back to a slower path just
/// because another index is also present in the write.
fn insertOneMixedTextSparse(alloc: std.mem.Allocator, handle: *anyopaque, key: []const u8, text: []const u8, vector_json: []const u8, sync_level: []const u8) !void {
    const key_json = try jsonString(alloc, key);
    defer alloc.free(key_json);
    const text_json = try jsonString(alloc, text);
    defer alloc.free(text_json);
    const sync_level_json = try jsonString(alloc, sync_level);
    defer alloc.free(sync_level_json);
    const body = try std.fmt.allocPrint(
        alloc,
        "{{\"inserts\":{{{s}:{{\"body\":{s},\"_embeddings\":{{\"" ++ sparse_index_name ++ "\":{s}}}}}}},\"sync_level\":{s}}}",
        .{ key_json, text_json, vector_json, sync_level_json },
    );
    defer alloc.free(body);
    var output: capi_types.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    const code = api.antfly_db_batch_json(handle, .fromSlice(body), &output);
    if (code != .ok) std.debug.print("mixed text+sparse batch for {s} failed: {s}\n", .{ key, std.mem.span(api.antfly_last_error_detail()) });
    try std.testing.expectEqual(capi_types.ErrorCode.ok, code);
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

    /// Both `rideAlongFlushLoop`-driven idle-flush stages: `.ride_along_flush`
    /// (forcing a genuinely-queued ride-along `MutationPlan` member through)
    /// and `.redo_checkpoint_fold` (`maybeCheckpointRedoLogOnIdleTurn`'s own
    /// opportunistic redo-log overlay fold) -- the same background loop, the
    /// same wake, two different things it can fold. Kept as two Stage tags
    /// so a report can tell them apart, but treated as one kind of cost
    /// (the documented safety-net idle-flush backstop, not foreground
    /// amplification) everywhere that distinction matters below.
    const redo_checkpoint_fold_stages = [_]wal_trace.Stage{ .ride_along_flush, .redo_checkpoint_fold };

    fn totalCommitsExcluding(self: ModeReport, excluded: []const wal_trace.Stage) u64 {
        var total: u64 = 0;
        for (std.meta.tags(wal_trace.Stage)) |s| {
            if (std.mem.indexOfScalar(wal_trace.Stage, excluded, s) != null) continue;
            const counts = self.report.stage(s);
            total += counts.commits_calling_thread + counts.commits_background;
        }
        return total;
    }

    fn totalSyncsExcluding(self: ModeReport, lane: wal_trace.Lane, excluded: []const wal_trace.Stage) u64 {
        var total: u64 = 0;
        for (std.meta.tags(wal_trace.Stage)) |s| {
            if (std.mem.indexOfScalar(wal_trace.Stage, excluded, s) != null) continue;
            const counts = self.report.stage(s);
            total += switch (lane) {
                .calling_thread => counts.sync_calling_thread,
                .background => counts.sync_background,
            };
        }
        return total;
    }

    /// Durable commits/write, excluding any commit `rideAlongFlushLoop`'s
    /// idle-flush timer itself led (`redo_checkpoint_fold_stages`). A
    /// production commit the idle timer leads is still a real, useful
    /// commit (it makes otherwise-stranded ride-along work durable when no
    /// foreground writer follows) -- it just isn't the single-commit write
    /// path's own per-write amplification, so it gets its own separate
    /// bound (`idleFlushDrains`) instead of tripping this one.
    fn durableCommitsPerWriteExcludingIdleFlush(self: ModeReport) f64 {
        return @as(f64, @floatFromInt(self.totalCommitsExcluding(&redo_checkpoint_fold_stages))) / @as(f64, @floatFromInt(self.updates));
    }

    /// Background syncs/update, excluding the idle-flush timer's own
    /// syncs. See `durableCommitsPerWriteExcludingIdleFlush`.
    fn backgroundSyncsPerUpdateExcludingIdleFlush(self: ModeReport) f64 {
        return @as(f64, @floatFromInt(self.totalSyncsExcluding(.background, &redo_checkpoint_fold_stages))) / @as(f64, @floatFromInt(self.updates));
    }

    /// How many times `rideAlongFlushLoop`'s idle-flush timer itself led a
    /// drain during this run. Unavoidable without a foreground write
    /// following within `ride_along_idle_ns` of the last ride-along
    /// enqueue -- bounded by how many times the timer can physically fire
    /// over the run's own wall-clock duration (`elapsed_ns`), not by write
    /// traffic, so the gate on this is `ceil(elapsed_ns / idle_window) + 1`
    /// rather than zero.
    fn idleFlushDrains(self: ModeReport) u64 {
        var total: u64 = 0;
        for (redo_checkpoint_fold_stages) |s| total += self.report.stage(s).commits_background;
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
    return measureSingleWriterWithSyncLevel(alloc, name, indexed, if (indexed) "full_index" else "write", updates);
}

/// Like `measureSingleWriter`, but with an explicit `sync_level` instead of
/// the "write"/"full_index" default. WS4 phase 2's inline text publication
/// fast path (`docs/design/lite-performance/lite-single-commit-write-path.md`
/// §3.3) is gated on `sync_level == .full_text` specifically: `full_index`
/// additionally waits for every other derived consumer (enrichments, dense
/// vectors, LEANN graph coverage, scheduled text merges -- see
/// `DB.waitForSyncLevelWithPublicationCut`'s `.full_index` branch), which is
/// out of scope for this change and still takes the pre-existing
/// notify-workers-and-wait path. Measuring the fast path's own effect
/// therefore requires this direct `full_text` scenario rather than the
/// `indexed` (`full_index`) one above, which this harness's "ceilings"
/// regression gate already covers and which this function deliberately does
/// not change.
fn measureSingleWriterWithSyncLevel(alloc: std.mem.Allocator, comptime name: []const u8, indexed: bool, sync_level: []const u8, updates: usize) !ModeReport {
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
        try insertOne(alloc, fixture.handle, key, text, sync_level);
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

/// `test_commit_key_hook` classifier: by key kind, not by code stage, so a
/// single physical commit's bytes can be broken down into what it actually
/// carries (primary document row, algebraic facts, progress-v2 record,
/// journal/replay record, everything else) regardless of which stage tag
/// happened to be active when the write was staged.
const CommitKeyKindDiagnostic = struct {
    var primary_bytes: usize = 0;
    var algebraic_fact_bytes: usize = 0;
    var progress_bytes: usize = 0;
    var journal_bytes: usize = 0;
    var other_bytes: usize = 0;
    var keys_seen: usize = 0;

    fn reset() void {
        primary_bytes = 0;
        algebraic_fact_bytes = 0;
        progress_bytes = 0;
        journal_bytes = 0;
        other_bytes = 0;
        keys_seen = 0;
    }

    fn record(key: []const u8, bytes: usize) void {
        keys_seen += 1;
        // `storage/db/algebraic/index.zig`'s `namespace_prefix`.
        if (std.mem.startsWith(u8, key, "\x00\x00__algebraic__:")) {
            algebraic_fact_bytes += bytes;
            return;
        }
        // `storage/db/derived/apply_state.zig`'s `progress_prefix_v2`.
        if (std.mem.startsWith(u8, key, "\x00\x00__metadata__:progress:v2:")) {
            progress_bytes += bytes;
            return;
        }
        // `storage/internal_keys.zig`'s `replay_namespace` (0x02) prefixes
        // every journal/replay-lane key (the "all" lane and every hinted
        // lane).
        if (key.len > 0 and key[0] == 0x02) {
            journal_bytes += bytes;
            return;
        }
        // Every other private-metadata key (`\x00\x00...`) this write might
        // touch (status snapshots, index catalog, consumer registry, ...).
        if (key.len >= 2 and key[0] == 0 and key[1] == 0) {
            other_bytes += bytes;
            return;
        }
        // No `\x00\x00`/`\x02` prefix: the user-supplied primary document
        // key itself.
        primary_bytes += bytes;
    }

    fn printReport(comptime label: []const u8, updates: usize) void {
        const u: f64 = @floatFromInt(@max(updates, 1));
        std.debug.print(
            "  {s}: keys/write={d:.1} primary_bytes/write={d:.0} algebraic_fact_bytes/write={d:.0} progress_bytes/write={d:.0} journal_bytes/write={d:.0} other_bytes/write={d:.0}\n",
            .{
                label,
                @as(f64, @floatFromInt(keys_seen)) / u,
                @as(f64, @floatFromInt(primary_bytes)) / u,
                @as(f64, @floatFromInt(algebraic_fact_bytes)) / u,
                @as(f64, @floatFromInt(progress_bytes)) / u,
                @as(f64, @floatFromInt(journal_bytes)) / u,
                @as(f64, @floatFromInt(other_bytes)) / u,
            },
        );
    }
};

/// Single-writer algebraic mode: one document per transaction, acked at
/// `full_index` -- the sync level that requires this index's own facts (and
/// its HLL cardinality sketch) to be visible before the batch call returns.
fn measureSingleWriterAlgebraic(alloc: std.mem.Allocator, comptime name: []const u8, updates: usize) !ModeReport {
    var fixture = try Fixture.open(alloc, name, .algebraic);
    defer fixture.close();

    var key_buffer: [32]u8 = undefined;
    wal_trace.reset();
    wal_trace.registerCallingThread();
    CommitKeyKindDiagnostic.reset();
    docstore_mod.Store.test_commit_key_hook = CommitKeyKindDiagnostic.record;
    defer docstore_mod.Store.test_commit_key_hook = null;
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..updates) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "doc-{d}", .{i % 128});
        try insertOneAlgebraic(alloc, fixture.handle, key, i, "full_index");
    }
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    CommitKeyKindDiagnostic.printReport("algebraic bytes by record kind", updates);
    return .{ .updates = updates, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

const ConcurrentAlgebraicWorker = struct {
    alloc: std.mem.Allocator,
    handle: *anyopaque,
    worker: usize,
    updates_per_writer: usize,
    barrier: *Barrier,
    err: ?anyerror = null,

    fn run(self: *ConcurrentAlgebraicWorker) void {
        wal_trace.registerCallingThread();
        var key_buffer: [32]u8 = undefined;
        for (0..self.updates_per_writer) |i| {
            self.barrier.wait();
            const key = std.fmt.bufPrint(&key_buffer, "w{d}-doc-{d}", .{ self.worker, i % 128 }) catch |err| {
                self.err = err;
                return;
            };
            const seed = self.worker * 1_000_003 + i;
            insertOneAlgebraic(self.alloc, self.handle, key, seed, "full_index") catch |err| {
                self.err = err;
                return;
            };
        }
    }
};

/// Real concurrent threads issuing algebraic writes, mirroring
/// `measureConcurrentVector`'s barrier-synchronized contention shape but for
/// `measureSingleWriterAlgebraic`'s workload.
fn measureConcurrentAlgebraic(alloc: std.mem.Allocator, comptime name: []const u8, writers: usize, updates_per_writer: usize) !ModeReport {
    var fixture = try Fixture.open(alloc, name, .algebraic);
    defer fixture.close();

    wal_trace.reset();
    var barrier = Barrier{ .participants = writers };
    var workers = try alloc.alloc(ConcurrentAlgebraicWorker, writers);
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
    for (0..writers) |w| threads[w] = try std.Thread.spawn(.{}, ConcurrentAlgebraicWorker.run, .{&workers[w]});
    for (threads) |thread| thread.join();
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    for (workers) |worker| if (worker.err) |err| return err;
    return .{ .updates = writers * updates_per_writer, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

// Diagnostic accumulator for `docstore_mod.Store.test_ride_along_hook`:
/// every `stageRideAlong` call during the measured write loop below,
/// regardless of which caller/kind staged it (the stage tag active when a
/// self-forced drain's fsync actually happens belongs to whatever OTHER
/// write is running at that moment, never necessarily the ride-along
/// member whose own bytes tipped the threshold -- see the hook's own doc
/// comment in docstore.zig).
const RideAlongDiagnostic = struct {
    var calls: usize = 0;
    var total_bytes: usize = 0;
    var max_bytes: usize = 0;
    var crossings: usize = 0; // calls whose own bytes alone exceed the threshold
    var pending_before_sum: usize = 0;

    fn reset() void {
        calls = 0;
        total_bytes = 0;
        max_bytes = 0;
        crossings = 0;
        pending_before_sum = 0;
    }

    fn record(estimated_bytes: usize, pending_before: usize) void {
        calls += 1;
        total_bytes += estimated_bytes;
        pending_before_sum += pending_before;
        if (estimated_bytes > max_bytes) max_bytes = estimated_bytes;
        if (estimated_bytes >= 1024 * 1024) crossings += 1;
    }

    fn printReport(comptime label: []const u8) void {
        const mean_bytes: f64 = if (calls == 0) 0 else @as(f64, @floatFromInt(total_bytes)) / @as(f64, @floatFromInt(calls));
        const mean_pending_before: f64 = if (calls == 0) 0 else @as(f64, @floatFromInt(pending_before_sum)) / @as(f64, @floatFromInt(calls));
        std.debug.print(
            "  {s}: stageRideAlong calls={d} mean_estimated_bytes={d:.0} max_estimated_bytes={d} single-call-exceeds-1MiB={d} mean_pending_before={d:.0}\n",
            .{ label, calls, mean_bytes, max_bytes, crossings, mean_pending_before },
        );
    }
};

/// Single-writer algebraic *overwrite* mode: same fixture and sync level as
/// `measureSingleWriterAlgebraic`, but a small key cycle
/// (`algebraic_overwrite_key_cycle`) so most writes overwrite an existing
/// doc's group-by/customer value instead of inserting a fresh one -- the
/// only way to dirty and so exercise the HLL cardinality sketch's ride-along
/// maintenance path (`runHllMaintenance`/`flushPendingHllObservations`),
/// which the insert-only `algebraic` mode above never triggers. Opens with
/// the production default `ride_along_idle_ns` (not overridden -- a prior
/// version of this mode widened it to make the idle-flush timer never fire
/// during the loop at all; that just hid a real, if rare, design flaw in
/// the gate instead of fixing it, and the idle timer's own commits are a
/// real barrier this mode should exercise, not route around). See
/// `durableCommitsPerWriteExcludingIdleFlush`/`idleFlushDrains` for how the
/// gates below separate "the idle timer did its job" from "something is
/// actually amplifying." `antfly_db_force_sync_for_test` after the loop
/// (not measured -- `wal_trace.snapshot()` runs first) drains whatever the
/// very last write's own ride-along job left pending, with no later
/// foreground write of its own to piggyback on, so the fixture closes clean.
fn measureSingleWriterAlgebraicOverwrite(alloc: std.mem.Allocator, comptime name: []const u8, updates: usize) !ModeReport {
    var fixture = try Fixture.open(alloc, name, .algebraic);
    defer fixture.close();

    var key_buffer: [32]u8 = undefined;
    wal_trace.reset();
    wal_trace.registerCallingThread();
    RideAlongDiagnostic.reset();
    docstore_mod.Store.test_ride_along_hook = RideAlongDiagnostic.record;
    defer docstore_mod.Store.test_ride_along_hook = null;
    CommitKeyKindDiagnostic.reset();
    docstore_mod.Store.test_commit_key_hook = CommitKeyKindDiagnostic.record;
    defer docstore_mod.Store.test_commit_key_hook = null;
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..updates) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "doc-{d}", .{i % algebraic_overwrite_key_cycle});
        try insertOneAlgebraic(alloc, fixture.handle, key, i, "full_index");
    }
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    const report = wal_trace.snapshot();
    RideAlongDiagnostic.printReport("algebraic overwrite ride-along diagnostic");
    CommitKeyKindDiagnostic.printReport("algebraic overwrite bytes by record kind", updates);
    // Not measured (the snapshot above already captured the loop):
    // explicit cleanup so the fixture's own `close()` never has to drain a
    // still-pending ride-along job under this mode's deliberately huge
    // idle window.
    try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_force_sync_for_test(fixture.handle));
    return .{ .updates = updates, .report = report, .elapsed_ns = elapsed_ns };
}

const ConcurrentAlgebraicOverwriteWorker = struct {
    alloc: std.mem.Allocator,
    handle: *anyopaque,
    worker: usize,
    updates_per_writer: usize,
    err: ?anyerror = null,

    /// No per-round barrier (unlike `ConcurrentAlgebraicWorker` above):
    /// the coordinator asked for continuous, staggered traffic here
    /// specifically, not lockstep rounds with a gap between them, so each
    /// worker just runs its own loop as fast as it can -- natural
    /// scheduling jitter across 8 independent threads is what approximates
    /// real concurrent write arrival, not an explicit stagger delay.
    fn run(self: *ConcurrentAlgebraicOverwriteWorker) void {
        wal_trace.registerCallingThread();
        var key_buffer: [32]u8 = undefined;
        for (0..self.updates_per_writer) |i| {
            // Own small key cycle per worker (never shared across workers,
            // same as `ConcurrentAlgebraicWorker` above) so each worker's
            // own stream overwrites its own keys repeatedly, same shape as
            // `measureSingleWriterAlgebraicOverwrite`.
            const key = std.fmt.bufPrint(&key_buffer, "w{d}-doc-{d}", .{ self.worker, i % algebraic_overwrite_key_cycle }) catch |err| {
                self.err = err;
                return;
            };
            const seed = self.worker * 1_000_003 + i;
            insertOneAlgebraic(self.alloc, self.handle, key, seed, "full_index") catch |err| {
                self.err = err;
                return;
            };
        }
    }
};

/// 8-writer counterpart to `measureSingleWriterAlgebraicOverwrite`: same
/// production-default idle window and explicit final drain, but continuous
/// staggered traffic (no per-round barrier -- see
/// `ConcurrentAlgebraicOverwriteWorker`) instead of a single sequential
/// writer, so a ride-along HLL maintenance commit almost always finds
/// another worker's own group forming to join well before the idle timer
/// would ever need to step in.
fn measureConcurrentAlgebraicOverwrite(alloc: std.mem.Allocator, comptime name: []const u8, writers: usize, updates_per_writer: usize) !ModeReport {
    var fixture = try Fixture.open(alloc, name, .algebraic);
    defer fixture.close();

    wal_trace.reset();
    RideAlongDiagnostic.reset();
    docstore_mod.Store.test_ride_along_hook = RideAlongDiagnostic.record;
    defer docstore_mod.Store.test_ride_along_hook = null;
    var workers = try alloc.alloc(ConcurrentAlgebraicOverwriteWorker, writers);
    defer alloc.free(workers);
    var threads = try alloc.alloc(std.Thread, writers);
    defer alloc.free(threads);
    for (0..writers) |w| workers[w] = .{
        .alloc = alloc,
        .handle = fixture.handle,
        .worker = w,
        .updates_per_writer = updates_per_writer,
    };
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..writers) |w| threads[w] = try std.Thread.spawn(.{}, ConcurrentAlgebraicOverwriteWorker.run, .{&workers[w]});
    for (threads) |thread| thread.join();
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    for (workers) |worker| if (worker.err) |err| return err;
    const report = wal_trace.snapshot();
    RideAlongDiagnostic.printReport("algebraic overwrite ride-along diagnostic, 8 writers");
    try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_force_sync_for_test(fixture.handle));
    return .{ .updates = writers * updates_per_writer, .report = report, .elapsed_ns = elapsed_ns };
}

// ---------------------------------------------------------------------------
// Relational write-surface modes (lite-single-commit-write-path.md §3.3c):
// secondary indexes, unique constraints, foreign keys, and SQL INSERT/UPDATE
// all run through `antfly_db_sql_json`, the same embedded SQL seam the
// Go/Python/Rust/TS SDKs' SQL surface reaches, matching how a real relational
// write gets its unique/FK coordination (`sql.zig`'s `Adapter.mutate`
// routes a table with `hasCoordinatedConstraints()` through
// `beginTransaction`/`writeTransaction`/`commitTransaction` --
// `opts.transaction_resolution` in `mutation_commit.zig` -- the exact path
// this harness's modes below exercise for `unique_index`/`foreign_key`/
// `transaction`; `secondary_index` and `plain` have no coordinated
// constraint and so stay on the plain batch path,
// `trackedBatchWithWriterLaneHook`).
// ---------------------------------------------------------------------------

const relational_cardinality: usize = 128;
const relational_fk_child_cardinality: usize = 32;
const relational_fk_parent_cardinality: usize = 16;

fn execSql(alloc: std.mem.Allocator, handle: *anyopaque, statement: []const u8) !void {
    try execSqlSession(alloc, handle, statement, null);
}

fn execSqlSession(alloc: std.mem.Allocator, handle: *anyopaque, statement: []const u8, session_id: ?u64) !void {
    const stmt_json = try jsonString(alloc, statement);
    defer alloc.free(stmt_json);
    const body = if (session_id) |id|
        try std.fmt.allocPrint(alloc, "{{\"statement\":{s},\"session_id\":{d}}}", .{ stmt_json, id })
    else
        try std.fmt.allocPrint(alloc, "{{\"statement\":{s}}}", .{stmt_json});
    defer alloc.free(body);
    var output: capi_types.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    const code = api.antfly_db_sql_json(handle, .fromSlice(body), &output);
    if (code != .ok) {
        std.debug.print("lite-write-amp relational SQL failed ({s}): {s}\n", .{
            statement,
            if (output.ptr) |p| p[0..output.len] else "<no output>",
        });
        return error.RelationalSqlExecFailed;
    }
}

/// Like `execSql`, but never raises on a non-`ok` result: returns the
/// result code so a concurrent-conflict test can tell a winner (`.ok`)
/// from a loser (anything else) without treating the loss itself as a
/// test failure.
fn execSqlOutcome(alloc: std.mem.Allocator, handle: *anyopaque, statement: []const u8) !capi_types.ErrorCode {
    const stmt_json = try jsonString(alloc, statement);
    defer alloc.free(stmt_json);
    const body = try std.fmt.allocPrint(alloc, "{{\"statement\":{s}}}", .{stmt_json});
    defer alloc.free(body);
    var output: capi_types.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    return api.antfly_db_sql_json(handle, .fromSlice(body), &output);
}

/// Count of rows via `SELECT COUNT(*) ... WHERE ...`-style aggregation is
/// not exercised elsewhere in this harness; counting matching rows by
/// scanning the returned array keeps this independent of SQL aggregate
/// support. Raises on a non-`ok` result: callers use this only for the
/// post-race assertion, where the SELECT itself is never expected to fail.
fn countMatchingRows(alloc: std.mem.Allocator, handle: *anyopaque, statement: []const u8) !usize {
    const stmt_json = try jsonString(alloc, statement);
    defer alloc.free(stmt_json);
    const body = try std.fmt.allocPrint(alloc, "{{\"statement\":{s}}}", .{stmt_json});
    defer alloc.free(body);
    var output: capi_types.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    const code = api.antfly_db_sql_json(handle, .fromSlice(body), &output);
    if (code != .ok) {
        std.debug.print("lite-write-amp relational SELECT failed ({s}): {s}\n", .{
            statement,
            if (output.ptr) |p| p[0..output.len] else "<no output>",
        });
        return error.RelationalSqlExecFailed;
    }
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, output.ptr.?[0..output.len], .{});
    defer parsed.deinit();
    return parsed.value.object.get("rows").?.array.items.len;
}

const RelationalKind = enum { plain, secondary_index, unique_index, foreign_key };

/// A relational SQL Fixture: the primary-only Fixture (no default full-text
/// index, isolating this mode's own write cost) plus DDL for `kind`, run
/// once through the same `antfly_db_sql_json` seam the measured writes use.
fn openRelationalFixture(alloc: std.mem.Allocator, comptime name: []const u8, kind: RelationalKind) !Fixture {
    var fixture = try Fixture.open(alloc, name, .primary_only);
    errdefer fixture.close();
    switch (kind) {
        .plain => try execSql(alloc, fixture.handle, "CREATE TABLE items (label TEXT)"),
        .secondary_index => {
            try execSql(alloc, fixture.handle, "CREATE TABLE items (label TEXT)");
            try execSql(alloc, fixture.handle, "CREATE INDEX items_label ON items (label)");
        },
        .unique_index => {
            try execSql(alloc, fixture.handle, "CREATE TABLE items (label TEXT)");
            try execSql(alloc, fixture.handle, "CREATE UNIQUE INDEX items_label_unique ON items (label)");
        },
        .foreign_key => {
            try execSql(alloc, fixture.handle, "CREATE TABLE parents (id BIGINT PRIMARY KEY)");
            try execSql(alloc, fixture.handle, "CREATE TABLE children (id BIGINT, parent_id BIGINT REFERENCES parents (id))");
            for (0..relational_fk_parent_cardinality) |p| {
                var stmt_buf: [128]u8 = undefined;
                const stmt = try std.fmt.bufPrint(&stmt_buf, "INSERT INTO parents (_id, id) VALUES ('parent-{d}', {d})", .{ p, p });
                try execSql(alloc, fixture.handle, stmt);
            }
        },
    }
    // DDL (table/index creation) leaves background settle work in flight
    // on its own worker thread (root-incarnation catalog writes, default
    // full-text index provisioning/cleanup) that can still be mid-commit
    // when this returns. Drain it here, before the caller's `wal_trace.reset()`,
    // so at least those setup-time commits are accounted to setup, not
    // the measured write loop -- see the enrichment mode above for the
    // same `runUntilIdle` pattern. This narrows but does not close the
    // ~0.6-1.5 background commits/write every mode in this harness still
    // shows (stack-trace attribution during the write loop itself, not
    // setup, did not land a conclusive cause before time ran out on this
    // pass; see the design doc changelog for what's known and what a
    // follow-up pass should check first).
    _ = api.antfly_db_run_until_idle(fixture.handle);
    return fixture;
}

/// One relational write to the `items` table: INSERT the first time a given
/// key is written, UPDATE every later time -- mirroring `insertOne`'s/
/// `insertEdge`'s add-then-change shape, and literally exercising "SQL
/// INSERT/UPDATE via the C API". `label_seed` is always a value this call
/// has never used before, so a `unique_index` fixture's uniqueness check
/// scans the whole live set on every write, not a repeated no-op value.
fn writeRelationalRow(alloc: std.mem.Allocator, handle: *anyopaque, key: []const u8, is_first: bool, label_seed: usize) !void {
    var stmt_buf: [192]u8 = undefined;
    const stmt = if (is_first)
        try std.fmt.bufPrint(&stmt_buf, "INSERT INTO items (_id, label) VALUES ('{s}', 'label-{d}')", .{ key, label_seed })
    else
        try std.fmt.bufPrint(&stmt_buf, "UPDATE items SET label = 'label-{d}' WHERE _id = '{s}'", .{ label_seed, key });
    try execSql(alloc, handle, stmt);
}

/// One foreign-key write to the `children` table: the first
/// `relational_fk_child_cardinality` occurrences INSERT a new child row
/// referencing a parent (round-robin); every later occurrence UPDATEs an
/// existing child's `parent_id`, so FK re-validation runs on every write,
/// not just the first.
fn writeRelationalChild(alloc: std.mem.Allocator, handle: *anyopaque, key: []const u8, index: usize) !void {
    const child_ordinal = index % relational_fk_child_cardinality;
    const parent_ordinal = index % relational_fk_parent_cardinality;
    var stmt_buf: [192]u8 = undefined;
    const stmt = if (index < relational_fk_child_cardinality)
        try std.fmt.bufPrint(&stmt_buf, "INSERT INTO children (_id, id, parent_id) VALUES ('{s}', {d}, {d})", .{ key, child_ordinal, parent_ordinal })
    else
        try std.fmt.bufPrint(&stmt_buf, "UPDATE children SET parent_id = {d} WHERE _id = '{s}'", .{ parent_ordinal, key });
    try execSql(alloc, handle, stmt);
}

fn measureSingleWriterRelational(alloc: std.mem.Allocator, comptime name: []const u8, kind: RelationalKind, updates: usize) !ModeReport {
    var fixture = try openRelationalFixture(alloc, name, kind);
    defer fixture.close();

    var key_buffer: [32]u8 = undefined;
    wal_trace.reset();
    wal_trace.registerCallingThread();
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..updates) |i| {
        if (kind == .foreign_key) {
            const key = try std.fmt.bufPrint(&key_buffer, "child-{d}", .{i % relational_fk_child_cardinality});
            try writeRelationalChild(alloc, fixture.handle, key, i);
        } else {
            const key = try std.fmt.bufPrint(&key_buffer, "doc-{d}", .{i % relational_cardinality});
            try writeRelationalRow(alloc, fixture.handle, key, i < relational_cardinality, i);
        }
    }
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    return .{ .updates = updates, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

/// Explicit multi-document SQL transaction mode: each "update" is one
/// `BEGIN`/2x `INSERT-or-UPDATE`/`COMMIT` transaction touching two distinct
/// rows of a `plain` table (no coordinated constraint, isolating the
/// transaction-resolution commit's own cost from unique/FK re-validation,
/// which the modes above already measure separately). Exercises
/// `opts.transaction_resolution` in `mutation_commit.zig` exactly like
/// `commitTransaction` on any multi-statement SQL session.
fn measureSingleWriterRelationalTransaction(alloc: std.mem.Allocator, comptime name: []const u8, updates: usize) !ModeReport {
    var fixture = try openRelationalFixture(alloc, name, .plain);
    defer fixture.close();

    var session_id: u64 = 0;
    try std.testing.expectEqual(capi_types.ErrorCode.ok, @import("../../capi/sql_session.zig").antfly_db_sql_session_open(fixture.handle, &session_id));
    defer _ = @import("../../capi/sql_session.zig").antfly_db_sql_session_close(fixture.handle, session_id);

    var key_buffer_a: [32]u8 = undefined;
    var key_buffer_b: [32]u8 = undefined;
    wal_trace.reset();
    wal_trace.registerCallingThread();
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..updates) |i| {
        const first = i < relational_cardinality;
        const key_a = try std.fmt.bufPrint(&key_buffer_a, "doc-{d}-a", .{i % relational_cardinality});
        const key_b = try std.fmt.bufPrint(&key_buffer_b, "doc-{d}-b", .{i % relational_cardinality});
        try execSqlSession(alloc, fixture.handle, "BEGIN", session_id);
        var stmt_buf_a: [160]u8 = undefined;
        const stmt_a = if (first)
            try std.fmt.bufPrint(&stmt_buf_a, "INSERT INTO items (_id, label) VALUES ('{s}', 'label-{d}')", .{ key_a, i })
        else
            try std.fmt.bufPrint(&stmt_buf_a, "UPDATE items SET label = 'label-{d}' WHERE _id = '{s}'", .{ i, key_a });
        try execSqlSession(alloc, fixture.handle, stmt_a, session_id);
        var stmt_buf_b: [160]u8 = undefined;
        const stmt_b = if (first)
            try std.fmt.bufPrint(&stmt_buf_b, "INSERT INTO items (_id, label) VALUES ('{s}', 'label-{d}')", .{ key_b, i })
        else
            try std.fmt.bufPrint(&stmt_buf_b, "UPDATE items SET label = 'label-{d}' WHERE _id = '{s}'", .{ i, key_b });
        try execSqlSession(alloc, fixture.handle, stmt_b, session_id);
        try execSqlSession(alloc, fixture.handle, "COMMIT", session_id);
    }
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    return .{ .updates = updates, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

const ConcurrentRelationalWorker = struct {
    alloc: std.mem.Allocator,
    handle: *anyopaque,
    worker: usize,
    updates_per_writer: usize,
    kind: RelationalKind,
    barrier: *Barrier,
    err: ?anyerror = null,

    fn run(self: *ConcurrentRelationalWorker) void {
        wal_trace.registerCallingThread();
        var key_buffer: [32]u8 = undefined;
        for (0..self.updates_per_writer) |i| {
            self.barrier.wait();
            if (self.kind == .foreign_key) {
                const key = std.fmt.bufPrint(&key_buffer, "w{d}-child-{d}", .{ self.worker, i % relational_fk_child_cardinality }) catch |err| {
                    self.err = err;
                    return;
                };
                writeRelationalChild(self.alloc, self.handle, key, i) catch |err| {
                    self.err = err;
                    return;
                };
            } else {
                const key = std.fmt.bufPrint(&key_buffer, "w{d}-doc-{d}", .{ self.worker, i % relational_cardinality }) catch |err| {
                    self.err = err;
                    return;
                };
                const label_seed = self.worker * 1_000_003 + i;
                writeRelationalRow(self.alloc, self.handle, key, i < relational_cardinality, label_seed) catch |err| {
                    self.err = err;
                    return;
                };
            }
        }
    }
};

/// Real concurrent threads issuing relational SQL writes, mirroring
/// `measureConcurrent`'s/`measureConcurrentGraph`'s barrier-synchronized
/// contention shape. Each worker uses its own key (and, for `foreign_key`,
/// its own child-row) namespace, like every other concurrent mode here.
fn measureConcurrentRelational(alloc: std.mem.Allocator, comptime name: []const u8, kind: RelationalKind, writers: usize, updates_per_writer: usize) !ModeReport {
    var fixture = try openRelationalFixture(alloc, name, kind);
    defer fixture.close();

    wal_trace.reset();
    var barrier = Barrier{ .participants = writers };
    var workers = try alloc.alloc(ConcurrentRelationalWorker, writers);
    defer alloc.free(workers);
    var threads = try alloc.alloc(std.Thread, writers);
    defer alloc.free(threads);
    for (0..writers) |w| workers[w] = .{
        .alloc = alloc,
        .handle = fixture.handle,
        .worker = w,
        .updates_per_writer = updates_per_writer,
        .kind = kind,
        .barrier = &barrier,
    };
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..writers) |w| threads[w] = try std.Thread.spawn(.{}, ConcurrentRelationalWorker.run, .{&workers[w]});
    for (threads) |thread| thread.join();
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    for (workers) |worker| if (worker.err) |err| return err;
    return .{ .updates = writers * updates_per_writer, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

/// Concurrent counterpart to `measureSingleWriterRelationalTransaction`:
/// `writers` real OS threads, each with its own SQL session, each running
/// its own 2-document transactions against its own key namespace.
fn measureConcurrentRelationalTransaction(alloc: std.mem.Allocator, comptime name: []const u8, writers: usize, updates_per_writer: usize) !ModeReport {
    var fixture = try openRelationalFixture(alloc, name, .plain);
    defer fixture.close();

    const sql_session = @import("../../capi/sql_session.zig");
    const sessions = try alloc.alloc(u64, writers);
    defer alloc.free(sessions);
    for (sessions) |*session_id| try std.testing.expectEqual(capi_types.ErrorCode.ok, sql_session.antfly_db_sql_session_open(fixture.handle, session_id));
    defer {
        for (sessions) |session_id| _ = sql_session.antfly_db_sql_session_close(fixture.handle, session_id);
    }

    wal_trace.reset();
    var barrier = Barrier{ .participants = writers };
    const Worker = struct {
        alloc: std.mem.Allocator,
        handle: *anyopaque,
        worker: usize,
        session_id: u64,
        updates_per_writer: usize,
        barrier: *Barrier,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            wal_trace.registerCallingThread();
            var key_buffer_a: [40]u8 = undefined;
            var key_buffer_b: [40]u8 = undefined;
            for (0..self.updates_per_writer) |i| {
                self.barrier.wait();
                const first = i < relational_cardinality;
                const key_a = std.fmt.bufPrint(&key_buffer_a, "w{d}-doc-{d}-a", .{ self.worker, i % relational_cardinality }) catch |err| {
                    self.err = err;
                    return;
                };
                const key_b = std.fmt.bufPrint(&key_buffer_b, "w{d}-doc-{d}-b", .{ self.worker, i % relational_cardinality }) catch |err| {
                    self.err = err;
                    return;
                };
                execSqlSession(self.alloc, self.handle, "BEGIN", self.session_id) catch |err| {
                    self.err = err;
                    return;
                };
                var stmt_buf: [160]u8 = undefined;
                for ([_][]const u8{ key_a, key_b }) |key| {
                    const stmt = if (first)
                        std.fmt.bufPrint(&stmt_buf, "INSERT INTO items (_id, label) VALUES ('{s}', 'label-{d}')", .{ key, i }) catch |err| {
                            self.err = err;
                            return;
                        }
                    else
                        std.fmt.bufPrint(&stmt_buf, "UPDATE items SET label = 'label-{d}' WHERE _id = '{s}'", .{ i, key }) catch |err| {
                            self.err = err;
                            return;
                        };
                    execSqlSession(self.alloc, self.handle, stmt, self.session_id) catch |err| {
                        self.err = err;
                        return;
                    };
                }
                execSqlSession(self.alloc, self.handle, "COMMIT", self.session_id) catch |err| {
                    self.err = err;
                    return;
                };
            }
        }
    };
    var workers = try alloc.alloc(Worker, writers);
    defer alloc.free(workers);
    var threads = try alloc.alloc(std.Thread, writers);
    defer alloc.free(threads);
    for (0..writers) |w| workers[w] = .{
        .alloc = alloc,
        .handle = fixture.handle,
        .worker = w,
        .session_id = sessions[w],
        .updates_per_writer = updates_per_writer,
        .barrier = &barrier,
    };
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..writers) |w| threads[w] = try std.Thread.spawn(.{}, Worker.run, .{&workers[w]});
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
        if (counts.sync_calling_thread + counts.sync_background + counts.write_calls_calling_thread + counts.write_calls_background + counts.commits_calling_thread + counts.commits_background == 0) continue;
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
    relational_txn_begin: StageJson,
    relational_intent_write: StageJson,
    relational_intent_resolve: StageJson,
    relational_coordinator_hint: StageJson,
    algebraic_apply: StageJson,
    algebraic_bulk_finish: StageJson,
    algebraic_hll_maintenance: StageJson,
    algebraic_adaptive: StageJson,
    ride_along_flush: StageJson,
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
            .relational_txn_begin = stageJson(mode, .relational_txn_begin),
            .relational_intent_write = stageJson(mode, .relational_intent_write),
            .relational_intent_resolve = stageJson(mode, .relational_intent_resolve),
            .relational_coordinator_hint = stageJson(mode, .relational_coordinator_hint),
            .algebraic_apply = stageJson(mode, .algebraic_apply),
            .algebraic_bulk_finish = stageJson(mode, .algebraic_bulk_finish),
            .algebraic_hll_maintenance = stageJson(mode, .algebraic_hll_maintenance),
            .algebraic_adaptive = stageJson(mode, .algebraic_adaptive),
            .ride_along_flush = stageJson(mode, .ride_along_flush),
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
    /// Algebraic mode: one document per transaction against a representative
    /// group-by + HLL-cardinality algebraic index config, acked at
    /// `full_index`. Unlike full-text/dense/sparse, algebraic facts already
    /// live in the primary DocStore (see `storage/db/algebraic/index.zig`'s
    /// `storagePrefixAlloc`'s `\x00\x00__algebraic__:` prefix) -- there is no
    /// separate storage backend here, only the single-commit write-path
    /// question of whether the fact apply and the primary write share one
    /// native commit.
    algebraic: ModeJson,
    algebraic_8_writers: ModeJson,
    /// Same algebraic config, but overwriting a small cycle of keys instead
    /// of inserting fresh ones every time -- the only workload shape that
    /// dirties the HLL cardinality sketch and so exercises its ride-along
    /// maintenance commit (`algebraic_hll_maintenance`'s background syncs),
    /// which the insert-only `algebraic` mode above always reads as zero.
    algebraic_overwrite: ModeJson,
    algebraic_overwrite_8_writers: ModeJson,
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
    const algebraic = try measureSingleWriterAlgebraic(alloc, "lite-write-amp-algebraic", updates);
    const algebraic_concurrent = try measureConcurrentAlgebraic(alloc, "lite-write-amp-algebraic-concurrent", default_writers, updates / default_writers);
    const algebraic_overwrite = try measureSingleWriterAlgebraicOverwrite(alloc, "lite-write-amp-algebraic-overwrite", updates);
    const algebraic_overwrite_concurrent = try measureConcurrentAlgebraicOverwrite(alloc, "lite-write-amp-algebraic-overwrite-concurrent", default_writers, updates / default_writers);

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
    printStageTable("algebraic-visible", algebraic);
    printStageTable("algebraic-visible, 8 writers", algebraic_concurrent);
    printStageTable("algebraic-visible, overwrite", algebraic_overwrite);
    printStageTable("algebraic-visible, overwrite, 8 writers", algebraic_overwrite_concurrent);

    // Journal suppression + ride-along HLL maintenance TARGET (lite-single-
    // commit-write-path.md §3.2a/§3.3b): a workload that dirties and
    // rebuilds the HLL sketch on most writes still reaches 1.0 durable
    // commits/write with 0 background syncs/update -- the dirty rebuild
    // rides along in the SAME native transaction as the write that
    // dirtied it (or a later one), never forcing a sync of its own.
    //
    // ROOT-CAUSED (see docs/design/lite-performance/lite-single-commit-
    // write-path.md §3.3b): the original ~1.9/~0.94 numbers were never a
    // byte-threshold or oversized-sketch problem --
    // `RideAlongDiagnostic`'s own `stageRideAlong` size log showed every
    // call at ~16.9KB with `pending_before=0` every time, nowhere near
    // `ride_along_byte_threshold` (1 MiB). The actual mechanism was
    // `rideAlongFlushLoop`'s idle-flush timer (default `ride_along_idle_ns`
    // = 50ms) racing this Debug-build harness's own much slower (~250-
    // 300ms apart) write cadence and winning almost every time -- the
    // documented safety net working as designed, just triggered far more
    // often than realistic traffic would hit it.
    //
    // GATE REDESIGNED: a prior version of this gate widened both modes'
    // `ride_along_idle_ns` to 24 hours so the idle timer could never fire
    // during the loop at all. That hid a real design flaw rather than
    // fixing anything: with a wide-open idle window, a synchronous
    // ride-along commit (HLL maintenance waiting on its own ticket) blocks
    // until the window elapses, and with no foreground write to drain it
    // in the meantime, nothing ever does -- a hang, not a product
    // deadlock, but a hang. Both modes now open with the production
    // default `ride_along_idle_ns` (`Fixture.open`, not overridden) and
    // the gate instead tells "the idle timer did its job" apart from
    // "something is actually amplifying": `rideAlongFlushLoop` tags every
    // drain it leads as `.ride_along_flush`
    // (`write_amplification_trace.Stage`), so
    // `durableCommitsPerWriteExcludingIdleFlush`/
    // `backgroundSyncsPerUpdateExcludingIdleFlush` hard-gate at 1.0/0 same
    // as before, excluding exactly those drains, and `idleFlushDrains`
    // gets its own separate ceiling -- the timer can fire at most
    // `ceil(elapsed_ns / ride_along_idle_ns) + 1` times over the run's own
    // wall-clock duration, regardless of write traffic, so a count above
    // that bound is a real bug (the timer firing far more often than its
    // own period allows), not legitimate idle-flush activity. The
    // 8-writer mode keeps its staggered (no per-round barrier) traffic
    // shape so a ride-along HLL commit almost always finds another
    // worker's own group forming to join well before the idle timer would
    // ever need to step in; see the idle-flush-still-fires test in
    // `storage/lite/docstore.zig`, which proves the opposite case (no
    // foreground writer at all) still works.
    try expectAtMost(algebraic_overwrite.durableCommitsPerWriteExcludingIdleFlush(), 1.0, "algebraic overwrite: durable commits/write (excluding idle-flush drains)");
    try expectAtMost(algebraic_overwrite.backgroundSyncsPerUpdateExcludingIdleFlush(), 0.0, "algebraic overwrite: background syncs/update (excluding idle-flush drains)");
    try expectAtMost(
        @as(f64, @floatFromInt(algebraic_overwrite.idleFlushDrains())),
        @as(f64, @floatFromInt(ceilDiv(algebraic_overwrite.elapsed_ns, default_ride_along_idle_ns) + 1)),
        "algebraic overwrite: idle-flush drains",
    );
    try expectAtMost(algebraic_overwrite_concurrent.durableCommitsPerWriteExcludingIdleFlush(), 1.0, "algebraic overwrite, 8 writers: durable commits/write (excluding idle-flush drains)");
    try expectAtMost(algebraic_overwrite_concurrent.backgroundSyncsPerUpdateExcludingIdleFlush(), 0.0, "algebraic overwrite, 8 writers: background syncs/update (excluding idle-flush drains)");
    try expectAtMost(
        @as(f64, @floatFromInt(algebraic_overwrite_concurrent.idleFlushDrains())),
        @as(f64, @floatFromInt(ceilDiv(algebraic_overwrite_concurrent.elapsed_ns, default_ride_along_idle_ns) + 1)),
        "algebraic overwrite, 8 writers: idle-flush drains",
    );

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
        .algebraic = modeJson(algebraic),
        .algebraic_8_writers = modeJson(algebraic_concurrent),
        .algebraic_overwrite = modeJson(algebraic_overwrite),
        .algebraic_overwrite_8_writers = modeJson(algebraic_overwrite_concurrent),
    };
    const json_text = try std.json.Stringify.valueAlloc(alloc, report, .{ .whitespace = .indent_2 });
    defer alloc.free(json_text);
    std.debug.print("lite_write_amplification_json_begin\n{s}\nlite_write_amplification_json_end\n", .{json_text});
}

// Diagnostic-only (not part of the regular report/ceilings tests): isolates
// dense's own write path from every other mode in "lite write amplification
// report", to narrow down a suspected `error.UnexpectedNativePageKind`
// surfaced on an unrelated index when dense_storage_backend is flipped to
// `.native_catalog` in lite/backend.zig's configureDbOpenOptions. See the
// handback's dense/open-risk section. Requires dense_storage_backend to
// actually be `.native_catalog` to reproduce anything -- with it left at
// the shipped default (`.lsm`) this test is expected to pass cleanly, same
// as the "indexed" and "dense-vector-visible" modes in the main report.
test "lite write amplification diagnostic: dense vector mode alone" {
    const alloc = std.testing.allocator;
    const dense = try measureSingleWriterVector(alloc, "lite-write-amp-diag-dense-alone", false, default_updates);
    printStageTable("diagnostic: dense-vector-visible alone", dense);
}

// Same diagnostic, but with a full_text ("indexed") mode run in the SAME
// process immediately beforehand, matching the main report's mode order
// (indexed runs second, well before any dense mode) -- isolates whether
// the corruption needs a prior, unrelated full_text-indexed Fixture to
// have run in-process first, or reproduces with dense alone (the test
// above) just as readily.
test "lite write amplification diagnostic: full text then dense vector mode" {
    const alloc = std.testing.allocator;
    const indexed = try measureSingleWriter(alloc, "lite-write-amp-diag-indexed-first", true, default_updates);
    printStageTable("diagnostic: full-text-visible (indexed)", indexed);
    const dense = try measureSingleWriterVector(alloc, "lite-write-amp-diag-dense-after-indexed", false, default_updates);
    printStageTable("diagnostic: dense-vector-visible after indexed", dense);
}

// Same diagnostic family, extended to the full single-process mode lineup
// the main report test runs (minus its own 8-writer variants other than
// dense's): primary, indexed, graph, dense, sparse in sequence, then
// dense's own 8-writer concurrent mode last. Narrows whether a suspected
// corruption needs the full report's exact mode sequence/concurrency to
// reproduce, or reproduces already in the two simpler diagnostics above.
test "lite write amplification diagnostic: full mode lineup plus dense concurrent" {
    const alloc = std.testing.allocator;
    const updates = default_updates;
    const primary = try measureSingleWriter(alloc, "lite-write-amp-diag2-primary", false, updates);
    printStageTable("diagnostic2: primary-only", primary);
    const indexed = try measureSingleWriter(alloc, "lite-write-amp-diag2-indexed", true, updates);
    printStageTable("diagnostic2: indexed", indexed);
    const graph = try measureSingleWriterGraph(alloc, "lite-write-amp-diag2-graph", updates);
    printStageTable("diagnostic2: graph", graph);
    const dense = try measureSingleWriterVector(alloc, "lite-write-amp-diag2-dense", false, updates);
    printStageTable("diagnostic2: dense", dense);
    const sparse = try measureSingleWriterVector(alloc, "lite-write-amp-diag2-sparse", true, updates);
    printStageTable("diagnostic2: sparse", sparse);
    const dense_concurrent = try measureConcurrentVector(alloc, "lite-write-amp-diag2-dense-concurrent", false, default_writers, updates / default_writers);
    printStageTable("diagnostic2: dense 8 writers", dense_concurrent);
}

// Narrows the diagnostic2 failure (UnexpectedNativePageKind surfaced while
// closing the "indexed" Fixture, immediately before the "graph" Fixture's
// first edge write fails) to whether dense's activation is even involved:
// this test never touches a dense (or sparse) index at all.
test "lite write amplification diagnostic: primary then indexed then graph only" {
    const alloc = std.testing.allocator;
    const updates = default_updates;
    const primary = try measureSingleWriter(alloc, "lite-write-amp-diag3-primary", false, updates);
    printStageTable("diagnostic3: primary-only", primary);
    const indexed = try measureSingleWriter(alloc, "lite-write-amp-diag3-indexed", true, updates);
    printStageTable("diagnostic3: indexed", indexed);
    const graph = try measureSingleWriterGraph(alloc, "lite-write-amp-diag3-graph", updates);
    printStageTable("diagnostic3: graph", graph);
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

// ---------------------------------------------------------------------------
// "Other write-path surfaces" harness modes: TTL'd document writes and an
// enrichment-backed index driven by a deterministic non-inference producer
// (lite-single-commit-write-path.md §3.2a/§3.4). Deliberately separate from
// the report/ceilings tests above (own JSON markers, no ceiling gate yet):
// these exist to show the TTL-schedule write and the enrichment status/
// progress writes this change touched cost no *extra* durable commit beyond
// an ordinary write, not to gate the unrelated primary/indexed/graph modes.
//
// No new harness mode exists yet for "a table with a replication/CDC source
// configured so outbox rows are written": every durable-outbox write path
// (`mutation_commit.zig`'s replication_async_batch_mirror/
// replication_async_effect_mirror) is configured through internal `DB`
// fields with no public capi entry point, unlike schema/index/enrichment
// configuration -- this harness only opens through the public embedding
// seam (`capi.openLiteHandleWithRuntime`), by design (see this file's top
// comment), so it cannot reach that configuration without adding new capi
// surface. The TTL/outbox ride-along conversions are instead covered
// directly by `storage/lite/crash_consistency_test.zig`'s "TTL/outbox
// ride-along bookkeeping..." test.
const ttl_doc_count: usize = 24;

fn measureSingleWriterTtl(alloc: std.mem.Allocator, comptime name: []const u8, updates: usize) !ModeReport {
    var fixture = try Fixture.open(alloc, name, .primary_only);
    defer fixture.close();

    // A short TTL so the schema-level write-path cost (the per-write
    // timestamp key, already folded into the primary commit -- see
    // mutation_commit.zig's shouldWriteTimestamp/timestamp_writes) is
    // exercised on every insert, matching a document TTL configuration.
    try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_set_schema_json(fixture.handle, .fromSlice("{\"ttl_duration_ns\":3600000000000}")));

    var buffer: [text_bytes]u8 = undefined;
    var key_buffer: [32]u8 = undefined;
    wal_trace.reset();
    wal_trace.registerCallingThread();
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..updates) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "doc-{d}", .{i % ttl_doc_count});
        const text = payload(&buffer, 0, i);
        try insertOne(alloc, fixture.handle, key, text, "write");
    }
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    return .{ .updates = updates, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

/// Public-capi JSON shape for `types.EnrichmentConfig` (see
/// `capi/db.zig`'s `antfly_db_add_enrichment_json`): an empty/omitted
/// `producer_json` on an `asset` enrichment resolves to the deterministic
/// field-copy producer (no embedder, no generator, no inference -- see
/// `config_validation.zig`'s `.copy` producer kind), matching this
/// harness's "no inference on the write path" measurement goal.
fn addCopyEnrichment(handle: *anyopaque) !void {
    try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_add_enrichment_json(handle, .fromSlice(
        "{\"name\":\"wal_copy\",\"kind\":\"asset\",\"field\":\"body\",\"content_type\":\"text/plain\"}",
    )));
}

/// Enrichment-backed mode: single writer, `updates` single-doc writes each
/// with a `body` field a deterministic `.copy` producer (no inference)
/// materializes into its own asset artifact. Writes ack at `sync_level:
/// "write"` (generated artifacts are asynchronous by design -- §3.3a:
/// "Inference never runs inside a commit"); `db.runUntilIdle()` once after
/// the whole batch deterministically drains the enrichment worker under
/// this harness's `.manual` BackendRuntime, so the worker's own durable
/// commits (artifact publication, `enrichment_state.saveRuntimeStatus`/
/// `saveProjectionCheckpoint` progress -- this change's ride-along
/// conversions) are fully captured in one snapshot.
fn measureSingleWriterEnrichment(alloc: std.mem.Allocator, comptime name: []const u8, updates: usize) !ModeReport {
    var fixture = try Fixture.open(alloc, name, .primary_only);
    defer fixture.close();
    try addCopyEnrichment(fixture.handle);

    var buffer: [text_bytes]u8 = undefined;
    var key_buffer: [32]u8 = undefined;
    wal_trace.reset();
    wal_trace.registerCallingThread();
    const start_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..updates) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "doc-{d}", .{i});
        const text = payload(&buffer, 0, i);
        try insertOne(alloc, fixture.handle, key, text, "write");
    }
    try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_run_until_idle(fixture.handle));
    const elapsed_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - start_ns));
    return .{ .updates = updates, .report = wal_trace.snapshot(), .elapsed_ns = elapsed_ns };
}

test "lite write amplification: TTL-scheduled document writes" {
    const alloc = std.testing.allocator;
    const ttl = try measureSingleWriterTtl(alloc, "lite-write-amp-ttl", default_updates);
    printStageTable("TTL-scheduled (sync_level write)", ttl);
    const json_text = try std.json.Stringify.valueAlloc(alloc, modeJson(ttl), .{ .whitespace = .indent_2 });
    defer alloc.free(json_text);
    std.debug.print("lite_write_amplification_ttl_json_begin\n{s}\nlite_write_amplification_ttl_json_end\n", .{json_text});
}

test "lite write amplification: enrichment-backed index with a deterministic copy producer" {
    const alloc = std.testing.allocator;
    const enrichment = try measureSingleWriterEnrichment(alloc, "lite-write-amp-enrichment", default_updates);
    printStageTable("enrichment-backed (deterministic copy producer, drained via runUntilIdle)", enrichment);
    const json_text = try std.json.Stringify.valueAlloc(alloc, modeJson(enrichment), .{ .whitespace = .indent_2 });
    defer alloc.free(json_text);
    std.debug.print("lite_write_amplification_enrichment_json_begin\n{s}\nlite_write_amplification_enrichment_json_end\n", .{json_text});
}

// WS4 phase 2 round 2: measures the inline text publication fast path
// directly, through `sync_level: "full_text"` (not `"full_index"`; see
// `measureSingleWriterWithSyncLevel`'s comment). Runs with the default
// Lite configuration -- background index workers running -- which is
// exactly the configuration the no-worker-only `!hasWorkers()` gate used
// to exclude.
//
// This is also WS4 phase 2 round 3's regression gate for the inline
// fast path's steady-state commit count: `durableCommitsPerWrite` must
// stay close to 1.0 (one native commit per write, workers on). A real
// regression here previously measured 2.99 commits/write because
// `inline_text_handled_names` (mutation_commit.zig) stored a name that
// aliased `sync_targets.full_text_indexes`, which the "filter once" block
// a few lines later frees as soon as it learns the name was handled --
// well before the `install` loop at the bottom of
// `batchInternalPreparedOwned` reads that same slot to call
// `Executor.advanceAppliedSequenceIfContiguous`. The comparison inside
// that call read already-freed memory, so it never matched the
// registered worker and the fast-forward silently failed on every single
// write, forcing every other write back onto the slow, worker-driven
// catch-up path (worker.applied_sequence traced 0, 1, 1, 3, 3, 5, 5, ...
// instead of 0, 1, 2, 3, 4, 5, 6, ...). The fix gives
// `inline_text_handled_names` its own owned copy of the name instead of
// aliasing `sync_targets.full_text_indexes`.
//
// `calling_thread_syncs_per_update`/`fsyncsPerWriteOverall` are reported
// for comparison but not gated here: today's native commit protocol
// issues more than one physical fsync per logical commit (see
// `docs/design/lite-performance/lite-single-commit-write-path.md` §3.1's
// commit protocol), which is `native.zig` commit/manifest territory, out
// of WS4's scope.
test "lite write amplification report: full_text sync level (WS4 phase 2 round 2)" {
    const alloc = std.testing.allocator;
    const updates = default_updates;

    const full_text = try measureSingleWriterWithSyncLevel(alloc, "lite-write-amp-fulltext", true, "full_text", updates);
    std.debug.print("lite write amplification report, full_text sync level (updates={d}, text_bytes={d}):\n", .{ updates, text_bytes });
    printStageTable("full-text-visible (sync_level=full_text, workers on)", full_text);

    const report = modeJson(full_text);
    const json_text = try std.json.Stringify.valueAlloc(alloc, report, .{ .whitespace = .indent_2 });
    defer alloc.free(json_text);
    std.debug.print("lite_write_amplification_full_text_json_begin\n{s}\nlite_write_amplification_full_text_json_end\n", .{json_text});

    // Steady state (every write but the very first one, which legitimately
    // defers to the slow worker-driven path -- see
    // `batchInternalPreparedOwned`'s comment on "the very first write this
    // worker has ever been asked to apply") must cost one native commit.
    // 1.25 leaves headroom for that one amortized startup write (1/128 =
    // 0.0078) plus slack, while still catching the alternation regression
    // (2.99) by more than 2x.
    try expectAtMost(full_text.durableCommitsPerWrite(), 1.25, "full_text sync-level durable commits/write (workers on)");
}

// WS4 phase 2 round 3 regression test: the inline fast path must engage on
// every write after the first, not every other write. Uses a small update
// count so the specific alternation signature (worker.applied_sequence
// advancing by 2 every other write instead of by 1 every write) cannot
// hide inside an averaged ceiling the way `durableCommitsPerWrite` alone
// might if some other change shifted the ratio without reintroducing a
// literal alternation.
test "lite write amplification: full_text inline fast path engages every write in steady state" {
    const alloc = std.testing.allocator;
    const updates: usize = 20;

    const full_text = try measureSingleWriterWithSyncLevel(alloc, "lite-write-amp-fulltext-steady", true, "full_text", updates);
    // Every write but the first costs exactly one native commit when the
    // fast path engages every time; the alternation bug this guards
    // against made every *other* write cost roughly three (primary +
    // worker-driven index commit + worker-driven progress commit),
    // averaging close to 3x this ceiling over only 20 writes.
    try expectAtMost(full_text.durableCommitsPerWrite(), 1.35, "full_text inline fast path steady-state durable commits/write");
}

// lite-single-commit-write-path.md §3.2a/§3.3/§3.3a (full_index text inline
// publication): `prepareInlineTextPublication`'s fast path used to be
// gated on `sync_level == .full_text` exactly, so an `indexed`
// (`full_index`) write with a text index -- the harness's own "indexed"
// mode above, and the common case for a write that must also wait for
// enrichments/dense/graph/LEANN visibility before acking -- never took it,
// even though `.full_index` is `.full_text`'s superset and already
// requires this text index's own visibility. Measures that `.full_index`
// write directly (`measureSingleWriter`'s `indexed` mode, same Fixture and
// text payload as the main report's "indexed" row) to confirm the fast
// path now engages there too: before this change this mode cost one
// native commit for the primary write plus the worker-driven index
// segment/seal/progress commits (durable_commits_per_write well above 1,
// calling-thread syncs/update in the tens -- see the `measured
// main@6c0b004e95 baseline` cited in "lite write amplification ceilings"),
// after it the same ~1.03 commits/write steady state as the `full_text`
// sync-level test above.
test "lite write amplification report: full_index with text index (sparse-text-inline round 1)" {
    const alloc = std.testing.allocator;
    const updates = default_updates;

    const indexed = try measureSingleWriter(alloc, "lite-write-amp-fullindex-text", true, updates);
    std.debug.print("lite write amplification report, full_index sync level with text index (updates={d}, text_bytes={d}):\n", .{ updates, text_bytes });
    printStageTable("full-index-visible with text index (sync_level=full_index, workers on)", indexed);

    const report = modeJson(indexed);
    const json_text = try std.json.Stringify.valueAlloc(alloc, report, .{ .whitespace = .indent_2 });
    defer alloc.free(json_text);
    std.debug.print("lite_write_amplification_full_index_text_json_begin\n{s}\nlite_write_amplification_full_index_text_json_end\n", .{json_text});

    // Same headroom as the `full_text` sync-level test above: one amortized
    // startup write (the very first write for this index, which always
    // defers to the slow worker-driven path -- see
    // `batchInternalPreparedOwned`'s comment on "the very first write this
    // worker has ever been asked to apply") plus slack.
    try expectAtMost(indexed.durableCommitsPerWrite(), 1.25, "full_index-with-text durable commits/write (workers on)");
}

// Steady-state counterpart to the `full_text` alternation regression test
// above, but for `.full_index`: guards against the same class of bug
// (a use-after-free or a stale filtered-name comparison) reappearing
// specifically on the `.full_index` branch of the fast-path gate and its
// own `sync_targets.all_indexes` filter, which the `.full_text` steady-
// state test above cannot see since it never takes that branch.
test "lite write amplification: full_index text inline fast path engages every write in steady state" {
    const alloc = std.testing.allocator;
    const updates: usize = 20;

    const indexed = try measureSingleWriter(alloc, "lite-write-amp-fullindex-text-steady", true, updates);
    try expectAtMost(indexed.durableCommitsPerWrite(), 1.35, "full_index-with-text inline fast path steady-state durable commits/write");
}

// lite-single-commit-write-path.md §3.3a's sparse extension: measures the
// inline sparse publication fast path directly with `measureSingleWriterVector`
// (sparse=true), the same single-writer, caller-supplied-vector mode the
// main report's "sparse-vector-visible" row already uses, which never
// touches the 8-writer concurrent sparse mode (see `measureConcurrentVector`)
// -- that mode currently crashes in the pre-existing, unrelated
// `encodeChunkOrdinalRange`/`addToPostings` worker-driven apply path under
// real concurrent writers (a data race in `sparse.zig`'s posting-chunk
// split/merge logic, reproducible with or without this change, since the
// inline fast path never engages for a worker-driven catch-up write at all
// -- only for this write's OWN synchronous ack). Not chased here; see the
// handback notes.
test "lite write amplification report: sparse_vector with inline publication (sparse-text-inline round 2)" {
    const alloc = std.testing.allocator;
    const updates = default_updates;

    const sparse = try measureSingleWriterVector(alloc, "lite-write-amp-sparse-inline", true, updates);
    std.debug.print("lite write amplification report, sparse_vector with inline publication (updates={d}, sparse_terms={d}):\n", .{ updates, sparse_terms });
    printStageTable("sparse-vector-visible with inline publication (sync_level=full_index)", sparse);

    const report = modeJson(sparse);
    const json_text = try std.json.Stringify.valueAlloc(alloc, report, .{ .whitespace = .indent_2 });
    defer alloc.free(json_text);
    std.debug.print("lite_write_amplification_sparse_inline_json_begin\n{s}\nlite_write_amplification_sparse_inline_json_end\n", .{json_text});

    // Same headroom as the text/graph inline fast paths: one amortized
    // startup write (this index's very first write, which the eligibility
    // gate's background-worker-catchup check -- mirrored from text's own
    // -- always defers to the slow path) plus slack.
    try expectAtMost(sparse.durableCommitsPerWrite(), 1.25, "sparse_vector inline fast path durable commits/write");
}

// Steady-state counterpart to the text/graph alternation regression tests
// above, but for sparse: guards against the same class of bug (a stale
// filtered-name comparison, or a counter left mutated between prepare and
// install/abort) reappearing on the sparse branch specifically.
test "lite write amplification: sparse_vector inline fast path engages every write in steady state" {
    const alloc = std.testing.allocator;
    const updates: usize = 20;

    const sparse = try measureSingleWriterVector(alloc, "lite-write-amp-sparse-inline-steady", true, updates);
    try expectAtMost(sparse.durableCommitsPerWrite(), 1.35, "sparse_vector inline fast path steady-state durable commits/write");
}

// lite-single-commit-write-path.md §3.3a: a mixed-kind write -- one doc
// carrying both a text `body` field and a caller-supplied sparse
// embedding -- must still cost one native commit: each eligible index
// (InlineTextPublication, InlineSparsePublication) contributes its own
// commit_participant to the SAME transaction, and the other's presence
// must never push either one onto its own slower, separate-commit path.
test "lite write amplification: mixed text+sparse write engages both inline fast paths in one commit" {
    const alloc = std.testing.allocator;
    const updates = default_updates;

    var fixture = try Fixture.open(alloc, "lite-write-amp-mixed-text-sparse", .full_text);
    defer fixture.close();
    try addExternalVectorIndex(alloc, fixture.handle, sparse_index_name, true);

    var buffer: [text_bytes]u8 = undefined;
    var key_buffer: [32]u8 = undefined;
    wal_trace.reset();
    wal_trace.registerCallingThread();
    for (0..updates) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "doc-{d}", .{i % 128});
        const text = payload(&buffer, 0, i);
        const vector_json = try sparseVectorJsonAlloc(alloc, i);
        defer alloc.free(vector_json);
        try insertOneMixedTextSparse(alloc, fixture.handle, key, text, vector_json, "full_index");
    }
    const mixed: ModeReport = .{ .updates = updates, .report = wal_trace.snapshot() };

    std.debug.print("lite write amplification report, mixed text+sparse (updates={d}):\n", .{updates});
    printStageTable("mixed text+sparse-visible (sync_level=full_index, workers on)", mixed);

    // Same headroom as every other inline fast-path report test: one
    // amortized startup write per eligible index (each index's own
    // worker-catchup precheck defers its very first write to the slow
    // path) plus slack. Two indexes' startup writes can land on the SAME
    // update (doc-0's very first write touches both), so this stays at
    // the single-index ceiling, not double it.
    try expectAtMost(mixed.durableCommitsPerWrite(), 1.25, "mixed text+sparse durable commits/write (workers on)");

    // The sparse index's large catalog values fold through the direct
    // large-batch path; their chain pages must be counted, or retiring an
    // overwritten value fails every later fold.
    const guard = api.enterHandle(fixture.handle, .read) orelse return error.TestUnexpectedResult;
    defer guard.leave();
    const root = guard.handle.parent_handle orelse guard.handle;
    const status = try root.owned_lite_backend.?.native_docstore.?.reclamationStatus();
    try std.testing.expectEqual(@as(u64, 0), status.allocator_work.redo_checkpoint_failures);
}

// lite-single-commit-write-path.md §3.3c: the relational write surface --
// secondary indexes, unique constraints, foreign keys, SQL INSERT/UPDATE
// through the C API, and multi-document transactions. A fresh schema/table
// per mode isolates each concern's own cost, exactly like the graph/dense/
// sparse sections above isolate theirs.
test "lite write amplification report: relational write surface" {
    const alloc = std.testing.allocator;
    const updates = default_updates;

    const secondary_index = try measureSingleWriterRelational(alloc, "lite-write-amp-rel-secondary", .secondary_index, updates);
    const secondary_index_concurrent = try measureConcurrentRelational(alloc, "lite-write-amp-rel-secondary-concurrent", .secondary_index, default_writers, updates / default_writers);
    const unique_index = try measureSingleWriterRelational(alloc, "lite-write-amp-rel-unique", .unique_index, updates);
    const unique_index_concurrent = try measureConcurrentRelational(alloc, "lite-write-amp-rel-unique-concurrent", .unique_index, default_writers, updates / default_writers);
    const foreign_key = try measureSingleWriterRelational(alloc, "lite-write-amp-rel-fk", .foreign_key, updates);
    const foreign_key_concurrent = try measureConcurrentRelational(alloc, "lite-write-amp-rel-fk-concurrent", .foreign_key, default_writers, updates / default_writers);
    const sql_insert_update = try measureSingleWriterRelational(alloc, "lite-write-amp-rel-sql", .plain, updates);
    const sql_insert_update_concurrent = try measureConcurrentRelational(alloc, "lite-write-amp-rel-sql-concurrent", .plain, default_writers, updates / default_writers);
    const transaction = try measureSingleWriterRelationalTransaction(alloc, "lite-write-amp-rel-txn", updates);
    const transaction_concurrent = try measureConcurrentRelationalTransaction(alloc, "lite-write-amp-rel-txn-concurrent", default_writers, updates / default_writers);

    std.debug.print("lite write amplification report, relational write surface (updates={d}):\n", .{updates});
    printStageTable("relational secondary-index", secondary_index);
    printStageTable("relational secondary-index, 8 writers", secondary_index_concurrent);
    printStageTable("relational unique-constraint", unique_index);
    printStageTable("relational unique-constraint, 8 writers", unique_index_concurrent);
    printStageTable("relational foreign-key child insert/update", foreign_key);
    printStageTable("relational foreign-key child insert/update, 8 writers", foreign_key_concurrent);
    printStageTable("SQL INSERT/UPDATE via C API", sql_insert_update);
    printStageTable("SQL INSERT/UPDATE via C API, 8 writers", sql_insert_update_concurrent);
    printStageTable("2-document SQL transaction", transaction);
    printStageTable("2-document SQL transaction, 8 writers", transaction_concurrent);

    const RelationalReportJson = struct {
        updates: usize,
        secondary_index: ModeJson,
        secondary_index_8_writers: ModeJson,
        unique_index: ModeJson,
        unique_index_8_writers: ModeJson,
        foreign_key: ModeJson,
        foreign_key_8_writers: ModeJson,
        sql_insert_update: ModeJson,
        sql_insert_update_8_writers: ModeJson,
        transaction_2doc: ModeJson,
        transaction_2doc_8_writers: ModeJson,
    };
    const report: RelationalReportJson = .{
        .updates = updates,
        .secondary_index = modeJson(secondary_index),
        .secondary_index_8_writers = modeJson(secondary_index_concurrent),
        .unique_index = modeJson(unique_index),
        .unique_index_8_writers = modeJson(unique_index_concurrent),
        .foreign_key = modeJson(foreign_key),
        .foreign_key_8_writers = modeJson(foreign_key_concurrent),
        .sql_insert_update = modeJson(sql_insert_update),
        .sql_insert_update_8_writers = modeJson(sql_insert_update_concurrent),
        .transaction_2doc = modeJson(transaction),
        .transaction_2doc_8_writers = modeJson(transaction_concurrent),
    };
    const json_text = try std.json.Stringify.valueAlloc(alloc, report, .{ .whitespace = .indent_2 });
    defer alloc.free(json_text);
    std.debug.print("lite_write_amplification_relational_json_begin\n{s}\nlite_write_amplification_relational_json_end\n", .{json_text});

    // Regression gates (lite-single-commit-write-path.md §3.3c). A
    // single-statement coordinated write (UNIQUE) and an explicit
    // multi-statement session transaction used to always take the full
    // cross-database two-phase commit protocol in `sql_commit.zig` --
    // store the recovery hint, begin the root as coordinator, begin the
    // one participant, write its intents, decide on the root, resolve the
    // participant, clear the hint -- about 11-12 native commits for one
    // write. `sql.zig`'s `mutate` and (task 3: `sql_session.zig`'s
    // `commit`, now also `allow_local_decision = true`) both route a
    // write whose prepared plan touches exactly one table on a Lite
    // single-file store through `commitCoordinatedLocal` instead: one
    // native commit, no begin record, no durable intent record -- an
    // ambiguous outcome resolves through the bounded outcome-receipt
    // marker scheme instead (`DB.outcomeMarkerFallbackStatus`). Task 4
    // (lite-single-commit-write-path.md §3.3c) converted the columnar
    // maintenance scheduler's own standalone background commits
    // (`relational_columns.zig`'s `selectReady`/`compact`/`prune`/
    // `drainCleanup`/`drainGarbage` -- all redoable bookkeeping over a
    // derived secondary index) to ride along the next foreground commit
    // instead of forcing their own fsync, which moved the measured
    // baseline down from ~1.6-1.9 to ~1.3-1.6. What is left in that
    // residual is `write_amplification_trace.zig`'s own `.ride_along_flush`
    // tag (the bounded idle-flush timer that keeps a ride-along write from
    // sitting undurable indefinitely when no foreground commit comes along
    // to carry it -- see its doc comment) plus one-time fixture/schema/
    // index-provisioning setup, neither a per-write cost. 2.0 leaves
    // headroom above that ~1.3-1.6 baseline (timer-firing jitter under
    // load) while still catching a regression back toward the old
    // two-phase-commit baseline (~4.5 for the transaction case, ~11-12 for
    // a cross-database one) by a wide margin.
    try expectAtMost(unique_index.durableCommitsPerWrite(), 2.0, "relational unique-constraint durable commits/write");
    try expectAtMost(unique_index_concurrent.durableCommitsPerWrite(), 2.0, "relational unique-constraint 8-writer durable commits/write");
    try expectAtMost(transaction.durableCommitsPerWrite(), 2.0, "2-document SQL transaction durable commits/write");
    try expectAtMost(transaction_concurrent.durableCommitsPerWrite(), 2.0, "2-document SQL transaction 8-writer durable commits/write");
    // Unaffected by the above: `secondary_index`/`sql_insert_update` never
    // had a coordinated constraint to route around `sql_commit.zig` in the
    // first place (no UNIQUE/FK on this mode's table), so their own,
    // separate baseline (now ~1.3-1.6, see the task 4 comment above) is
    // gated at its existing level, loosely, just to catch an unrelated
    // regression.
    try expectAtMost(secondary_index.durableCommitsPerWrite(), 2.0, "relational secondary-index durable commits/write");
    try expectAtMost(sql_insert_update.durableCommitsPerWrite(), 2.0, "SQL INSERT/UPDATE via C API durable commits/write");
    // `foreign_key` still spans two physical tables (the child row plus an
    // `.attach` inbound-reference record against the parent -- see
    // `Plan.expand` in `api/relational_integrity.zig`), but
    // `capi/sql.zig`'s `commitCoordinatedFusedMultiTable` (lite-single-
    // commit-write-path.md §7.5's FK multi-table coordinator) now drives
    // this case too instead of the full cross-database two-phase
    // protocol: it locks the parent's `core.apply_mutex` directly
    // (ascending `table_id` order, parent before child), validates and
    // stages the `.attach` record as a ride-along commit against the
    // parent's own store, then drives the child's row insert through its
    // existing, unmodified single-table `commitCoordinatedLocal`. Measured
    // ~16 -> ~1.9 (1 writer) / ~2.3 (8 writers), down from the untouched
    // baseline this gate used to allow. The residual above 1.0 is the
    // same `ride_along_flush` idle-timer dynamic already documented for
    // the algebraic overwrite case (§3.3b): the parent's ride-along
    // `.attach` write usually, but not always, gets swept into the SAME
    // fsync as the child's own foreground commit; when it loses that
    // race it becomes durable on the next foreground commit or the
    // idle-flush timer instead, costing close to a second commit. Not
    // addressed here (closing that gap needs the same cross-cutting
    // ride-along-timing work §3.3b/§7.5 already flagged as follow-up, not
    // a change specific to FK).
    try expectAtMost(foreign_key.durableCommitsPerWrite(), 3.0, "relational foreign-key durable commits/write");
    try expectAtMost(foreign_key_concurrent.durableCommitsPerWrite(), 3.5, "relational foreign-key 8-writer durable commits/write");
}

// ---------------------------------------------------------------------------
// Concurrency correctness: the single-store fast path in `sql.zig`'s
// `mutate`/`sql_session.zig`'s `commit` (lite-single-commit-write-path.md
// §3.3c) must keep exactly the same OCC/integrity guarantees the full
// cross-database two-phase commit protocol gave -- it only changes which
// commit path a single-table coordinated write takes, not the validation
// itself (`commitCoordinated`'s own begin/writeTransaction/commitTransaction
// body is untouched). These tests exercise that directly: real concurrent
// threads racing a UNIQUE value, and a foreign key referencing a parent
// that was never created.
// ---------------------------------------------------------------------------

const race_writers: usize = 8;

// `race_writers` real OS threads each try to INSERT a distinct row with
// the SAME UNIQUE `label` value, all through `commitCoordinated`'s
// single-store fast path. Exactly one must win; the rest must see a
// conflict, never a silently-dropped write or a duplicate committed row.
test "lite write amplification concurrency: concurrent conflicting UNIQUE inserts, exactly one wins" {
    const alloc = std.testing.allocator;
    var fixture = try openRelationalFixture(alloc, "lite-write-amp-rel-unique-race", .unique_index);
    defer fixture.close();

    const Worker = struct {
        alloc: std.mem.Allocator,
        handle: *anyopaque,
        id: usize,
        outcome: capi_types.ErrorCode = .internal,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            var stmt_buf: [160]u8 = undefined;
            const stmt = std.fmt.bufPrint(&stmt_buf, "INSERT INTO items (_id, label) VALUES ('race-{d}', 'contested')", .{self.id}) catch |err| {
                self.err = err;
                return;
            };
            self.outcome = execSqlOutcome(self.alloc, self.handle, stmt) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    var workers: [race_writers]Worker = undefined;
    for (&workers, 0..) |*worker, i| worker.* = .{ .alloc = alloc, .handle = fixture.handle, .id = i };
    var threads: [race_writers]std.Thread = undefined;
    for (&threads, &workers) |*thread, *worker| thread.* = try std.Thread.spawn(.{}, Worker.run, .{worker});
    for (&threads) |*thread| thread.join();

    var wins: usize = 0;
    for (workers) |worker| {
        if (worker.err) |err| return err;
        if (worker.outcome == .ok) wins += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), wins);
    try std.testing.expectEqual(@as(usize, 1), try countMatchingRows(alloc, fixture.handle, "SELECT label FROM items WHERE label = 'contested'"));
}

// `race_writers` real OS threads each try to INSERT a distinct child row
// referencing a parent that does not exist, all through `commitCoordinated`'s
// single-store fast path. None may win: a child row must never become
// durable without its parent foreign-key check holding, concurrently or
// otherwise.
test "lite write amplification concurrency: concurrent FK child inserts against a missing parent all fail" {
    const alloc = std.testing.allocator;
    var fixture = try openRelationalFixture(alloc, "lite-write-amp-rel-fk-race", .foreign_key);
    defer fixture.close();
    const missing_parent: usize = relational_fk_parent_cardinality + 1000;

    const Worker = struct {
        alloc: std.mem.Allocator,
        handle: *anyopaque,
        id: usize,
        outcome: capi_types.ErrorCode = .internal,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            var stmt_buf: [192]u8 = undefined;
            const stmt = std.fmt.bufPrint(&stmt_buf, "INSERT INTO children (_id, id, parent_id) VALUES ('race-child-{d}', {d}, {d})", .{ self.id, self.id, missing_parent }) catch |err| {
                self.err = err;
                return;
            };
            self.outcome = execSqlOutcome(self.alloc, self.handle, stmt) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    var workers: [race_writers]Worker = undefined;
    for (&workers, 0..) |*worker, i| worker.* = .{ .alloc = alloc, .handle = fixture.handle, .id = i };
    var threads: [race_writers]std.Thread = undefined;
    for (&threads, &workers) |*thread, *worker| thread.* = try std.Thread.spawn(.{}, Worker.run, .{worker});
    for (&threads) |*thread| thread.join();

    var wins: usize = 0;
    for (workers) |worker| {
        if (worker.err) |err| return err;
        if (worker.outcome == .ok) wins += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), wins);
    try std.testing.expectEqual(@as(usize, 0), try countMatchingRows(alloc, fixture.handle, "SELECT _id FROM children"));
}

// lite-single-commit-write-path.md §7.5: `commitCoordinatedFusedMultiTable`
// locks the parent's `core.apply_mutex` directly (never through its own
// `batchInternal` re-entry) for the whole duration of validating and
// staging a child insert's `.attach` record, exactly where a parent
// DELETE's own single-table `commitCoordinatedLocal` call also takes that
// same lock to check for live references before deleting. Half of
// `race_writers` threads race to DELETE the one seeded parent this test
// targets; the other half race to INSERT a new child referencing it. The
// correctness invariant is "no orphan": a parent that ends up durably
// deleted may never leave a live child row still referencing it,
// regardless of how the two kinds of operations interleaved.
test "lite write amplification concurrency: parent DELETE races child INSERT, no orphan and no lost reject" {
    const alloc = std.testing.allocator;
    var fixture = try openRelationalFixture(alloc, "lite-write-amp-rel-fk-parent-race", .foreign_key);
    defer fixture.close();
    const parent_id: usize = 0;

    const Role = enum { delete_parent, insert_child };
    const Worker = struct {
        alloc: std.mem.Allocator,
        handle: *anyopaque,
        id: usize,
        role: Role,
        outcome: capi_types.ErrorCode = .internal,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            var stmt_buf: [192]u8 = undefined;
            const stmt = switch (self.role) {
                .delete_parent => std.fmt.bufPrint(&stmt_buf, "DELETE FROM parents WHERE id = {d}", .{parent_id}) catch |err| {
                    self.err = err;
                    return;
                },
                .insert_child => std.fmt.bufPrint(&stmt_buf, "INSERT INTO children (_id, id, parent_id) VALUES ('race-parent-child-{d}', {d}, {d})", .{ self.id, self.id, parent_id }) catch |err| {
                    self.err = err;
                    return;
                },
            };
            self.outcome = execSqlOutcome(self.alloc, self.handle, stmt) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    var workers: [race_writers]Worker = undefined;
    for (&workers, 0..) |*worker, i| worker.* = .{
        .alloc = alloc,
        .handle = fixture.handle,
        .id = i,
        .role = if (i % 2 == 0) .delete_parent else .insert_child,
    };
    var threads: [race_writers]std.Thread = undefined;
    for (&threads, &workers) |*thread, *worker| thread.* = try std.Thread.spawn(.{}, Worker.run, .{worker});
    for (&threads) |*thread| thread.join();

    // A DELETE whose WHERE clause matches zero rows still reports `.ok`
    // (standard SQL semantics: it is a no-op, not an error), so more than
    // one `delete_parent` worker legitimately reporting success is
    // expected whenever a single winner deletes the row first and every
    // later DELETE then matches nothing -- outcome codes alone cannot
    // distinguish "I deleted it" from "it was already gone", only the
    // final durable state can. Check that instead.
    for (workers) |worker| if (worker.err) |err| return err;
    var parent_stmt_buf: [128]u8 = undefined;
    const parent_count_stmt = try std.fmt.bufPrint(&parent_stmt_buf, "SELECT id FROM parents WHERE id = {d}", .{parent_id});
    const parent_still_present = (try countMatchingRows(alloc, fixture.handle, parent_count_stmt)) == 1;
    var child_stmt_buf: [128]u8 = undefined;
    const child_count_stmt = try std.fmt.bufPrint(&child_stmt_buf, "SELECT _id FROM children WHERE parent_id = {d}", .{parent_id});
    const live_children = try countMatchingRows(alloc, fixture.handle, child_count_stmt);
    // No orphan: a deleted parent can never leave a live (durable) child
    // row still referencing it. This also IS the no-lost-reject
    // invariant restated: a child reference can only be durably live if
    // its parent is durably present, i.e. RESTRICT never let an attach
    // through against a parent that was (or became) absent.
    if (!parent_still_present) try std.testing.expectEqual(@as(usize, 0), live_children);
    // At least one side of the race must have made progress: either the
    // parent is gone, or at least one child successfully attached (ruling
    // out a lock-ordering bug that makes every concurrent attempt on this
    // table pair spuriously fail).
    try std.testing.expect(!parent_still_present or live_children > 0);
}

// lite-single-commit-write-path.md §7.5: two tables with FOREIGN KEYs
// pointing at each other in opposite directions. `eligibleForFusedMultiTable`
// in `capi/sql.zig` only drives the direction whose leader (the table
// receiving the row write) has the LARGER `table_id` of the two through
// the fused coordinator; the opposite direction falls back to
// `sql_commit.zig`'s cross-database two-phase coordinator, which already
// serializes through `handle.sql_commit_mutex`. Concurrent writers in BOTH
// directions must still complete without a deadlock between the fused
// path's direct `core.apply_mutex` locking (ascending `table_id`) and the
// two-phase path's own lock acquisition.
test "lite write amplification concurrency: mutual FK tables, concurrent writers in both directions complete without deadlock" {
    const alloc = std.testing.allocator;
    var fixture = try Fixture.open(alloc, "lite-write-amp-rel-fk-mutual", .primary_only);
    defer fixture.close();

    try execSql(alloc, fixture.handle, "CREATE TABLE node_a (id BIGINT PRIMARY KEY, b_ref BIGINT)");
    try execSql(alloc, fixture.handle, "CREATE TABLE node_b (id BIGINT PRIMARY KEY, a_ref BIGINT)");
    try execSql(alloc, fixture.handle, "INSERT INTO node_a (_id, id) VALUES ('seed-a', 0)");
    try execSql(alloc, fixture.handle, "INSERT INTO node_b (_id, id) VALUES ('seed-b', 0)");
    try execSql(alloc, fixture.handle, "ALTER TABLE node_a ADD CONSTRAINT node_a_b_fkey FOREIGN KEY (b_ref) REFERENCES node_b (id)");
    try execSql(alloc, fixture.handle, "ALTER TABLE node_b ADD CONSTRAINT node_b_a_fkey FOREIGN KEY (a_ref) REFERENCES node_a (id)");
    _ = api.antfly_db_run_until_idle(fixture.handle);

    const Direction = enum { a_references_b, b_references_a };
    const Worker = struct {
        alloc: std.mem.Allocator,
        handle: *anyopaque,
        id: usize,
        direction: Direction,
        outcome: capi_types.ErrorCode = .internal,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            var stmt_buf: [192]u8 = undefined;
            const stmt = switch (self.direction) {
                .a_references_b => std.fmt.bufPrint(&stmt_buf, "INSERT INTO node_a (_id, id, b_ref) VALUES ('mutual-a-{d}', {d}, 0)", .{ self.id, self.id + 1 }) catch |err| {
                    self.err = err;
                    return;
                },
                .b_references_a => std.fmt.bufPrint(&stmt_buf, "INSERT INTO node_b (_id, id, a_ref) VALUES ('mutual-b-{d}', {d}, 0)", .{ self.id, self.id + 1 }) catch |err| {
                    self.err = err;
                    return;
                },
            };
            self.outcome = execSqlOutcome(self.alloc, self.handle, stmt) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    var workers: [race_writers]Worker = undefined;
    for (&workers, 0..) |*worker, i| worker.* = .{
        .alloc = alloc,
        .handle = fixture.handle,
        .id = i,
        .direction = if (i % 2 == 0) .a_references_b else .b_references_a,
    };
    var threads: [race_writers]std.Thread = undefined;
    for (&threads, &workers) |*thread, *worker| thread.* = try std.Thread.spawn(.{}, Worker.run, .{worker});
    // `thread.join()` below hangs forever on a genuine deadlock; this
    // test's own bound is the harness's outer test timeout, exactly like
    // every other concurrency test in this file.
    for (&threads) |*thread| thread.join();

    for (workers) |worker| {
        if (worker.err) |err| return err;
        // Every insert references a parent row that was live the whole
        // time (never deleted by this test), so RESTRICT/existence
        // checks have no reason to reject any of them.
        try std.testing.expectEqual(capi_types.ErrorCode.ok, worker.outcome);
    }
    try std.testing.expectEqual(race_writers / 2, try countMatchingRows(alloc, fixture.handle, "SELECT _id FROM node_a WHERE b_ref = 0"));
    try std.testing.expectEqual(race_writers / 2, try countMatchingRows(alloc, fixture.handle, "SELECT _id FROM node_b WHERE a_ref = 0"));
}

fn expectAtMost(actual: f64, ceiling: f64, comptime label: []const u8) !void {
    if (actual > ceiling) {
        std.debug.print("{s}: {d:.3} exceeds ceiling {d:.3}\n", .{ label, actual, ceiling });
        return error.WriteAmplificationCeilingExceeded;
    }
}

/// Mirrors `Store.ride_along_idle_ns`'s production default (50ms). Not read
/// from a live `Store` -- the algebraic overwrite modes above no longer
/// override it, so this is the window the real idle-flush timer is using,
/// needed here only to bound how many times it could possibly have fired
/// over one measured run's `elapsed_ns`.
const default_ride_along_idle_ns: u64 = 50 * std.time.ns_per_ms;

fn ceilDiv(numerator: u64, denominator: u64) u64 {
    if (denominator == 0) return 0;
    return (numerator + denominator - 1) / denominator;
}
