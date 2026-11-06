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

const Fixture = struct {
    alloc: std.mem.Allocator,
    directory: TestDirectory,
    path: []const u8,
    runtime: db_mod.background_runtime.BackendRuntimeHandle,
    handle: *anyopaque,

    fn open(alloc: std.mem.Allocator, comptime name: []const u8, indexed: bool) !Fixture {
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

        if (!indexed) {
            var removed = false;
            try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_delete_index(handle, .fromSlice("full_text_index_v0"), &removed));
        }

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

// ---------------------------------------------------------------------------
// Measurement modes
// ---------------------------------------------------------------------------

const ModeReport = struct {
    updates: usize,
    report: wal_trace.Report,

    fn perUpdate(self: ModeReport, lane: wal_trace.Lane) f64 {
        return @as(f64, @floatFromInt(self.report.totalSyncs(lane))) / @as(f64, @floatFromInt(self.updates));
    }
};

fn measureSingleWriter(alloc: std.mem.Allocator, comptime name: []const u8, indexed: bool, updates: usize) !ModeReport {
    var fixture = try Fixture.open(alloc, name, indexed);
    defer fixture.close();

    var buffer: [text_bytes]u8 = undefined;
    var key_buffer: [32]u8 = undefined;
    wal_trace.reset();
    wal_trace.registerCallingThread();
    for (0..updates) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "doc-{d}", .{i % 128});
        const text = payload(&buffer, 0, i);
        try insertOne(alloc, fixture.handle, key, text, if (indexed) "full_index" else "write");
    }
    return .{ .updates = updates, .report = wal_trace.snapshot() };
}

const ConcurrentWorker = struct {
    alloc: std.mem.Allocator,
    handle: *anyopaque,
    worker: usize,
    updates_per_writer: usize,
    err: ?anyerror = null,

    fn run(self: *ConcurrentWorker) void {
        wal_trace.registerCallingThread();
        var buffer: [text_bytes]u8 = undefined;
        var key_buffer: [32]u8 = undefined;
        for (0..self.updates_per_writer) |i| {
            const key = std.fmt.bufPrint(&key_buffer, "w{d}-doc-{d}", .{ self.worker, i % 128 }) catch |err| {
                self.err = err;
                return;
            };
            const text = payload(&buffer, self.worker, i);
            insertOne(self.alloc, self.handle, key, text, "full_index") catch |err| {
                self.err = err;
                return;
            };
        }
    }
};

fn measureConcurrent(alloc: std.mem.Allocator, writers: usize, updates_per_writer: usize) !ModeReport {
    var fixture = try Fixture.open(alloc, "lite-write-amp-concurrent", true);
    defer fixture.close();

    wal_trace.reset();
    var workers = try alloc.alloc(ConcurrentWorker, writers);
    defer alloc.free(workers);
    var threads = try alloc.alloc(std.Thread, writers);
    defer alloc.free(threads);
    for (0..writers) |w| workers[w] = .{ .alloc = alloc, .handle = fixture.handle, .worker = w, .updates_per_writer = updates_per_writer };
    for (0..writers) |w| threads[w] = try std.Thread.spawn(.{}, ConcurrentWorker.run, .{&workers[w]});
    for (threads) |thread| thread.join();
    for (workers) |worker| if (worker.err) |err| return err;
    return .{ .updates = writers * updates_per_writer, .report = wal_trace.snapshot() };
}

// ---------------------------------------------------------------------------
// JSON report
// ---------------------------------------------------------------------------

fn printStageTable(mode_name: []const u8, mode: ModeReport) void {
    std.debug.print("  {s}: updates={d} calling_thread_syncs_per_update={d:.3} background_syncs_per_update={d:.3}\n", .{
        mode_name,
        mode.updates,
        mode.perUpdate(.calling_thread),
        mode.perUpdate(.background),
    });
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
    primary_only: ModeJson,
    indexed: ModeJson,
    indexed_8_writers: ModeJson,
};

test "lite write amplification report" {
    const alloc = std.testing.allocator;
    const updates = default_updates;

    const primary = try measureSingleWriter(alloc, "lite-write-amp-primary", false, updates);
    const indexed = try measureSingleWriter(alloc, "lite-write-amp-indexed", true, updates);
    const concurrent = try measureConcurrent(alloc, default_writers, updates / default_writers);

    std.debug.print("lite write amplification report (updates={d}, text_bytes={d}):\n", .{ updates, text_bytes });
    printStageTable("primary-only", primary);
    printStageTable("full-text-visible (indexed)", indexed);
    printStageTable("full-text-visible, 8 writers", concurrent);

    const report: ReportJson = .{
        .text_bytes = text_bytes,
        .primary_only = modeJson(primary),
        .indexed = modeJson(indexed),
        .indexed_8_writers = modeJson(concurrent),
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
    // docs/design/lite-performance/write-amplification-baseline.json).
    const slack = 1.25;
    try expectAtMost(primary.perUpdate(.calling_thread), 3.0 * slack, "primary-only calling-thread syncs/update");
    try expectAtMost(indexed.perUpdate(.calling_thread), 40.0 * slack, "indexed calling-thread syncs/update");
}

fn expectAtMost(actual: f64, ceiling: f64, comptime label: []const u8) !void {
    if (actual > ceiling) {
        std.debug.print("{s}: {d:.3} exceeds ceiling {d:.3}\n", .{ label, actual, ceiling });
        return error.WriteAmplificationCeilingExceeded;
    }
}
