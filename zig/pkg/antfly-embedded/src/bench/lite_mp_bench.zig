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

//! Multi-writer Lite benchmark through the public C API, without the
//! inference runtime (`zig build lite-mp-bench`, installed as
//! `zig-out/bin/lite-mp-bench`). The storage-only analogue
//! of `scripts/bench/lite_sqlite_comparison.py`: N writers, each its own
//! process or a thread of one process with its own connection, overwrite
//! their own keys for a fixed time, with or without a full-text index. A
//! fresh handle then checks the file and, when indexed, that each key's
//! newest token is found and the token it replaced is not. One JSON line per
//! configuration on stdout; exit status 1 if any configuration failed.
//!
//!   lite-mp-bench --dir DIR [--workers 8] [--seconds 10] [--slots 16]
//!                 [--mode process|thread|both] [--indexed yes|no|both]

const std = @import("std");

/// The C API (`include/antfly.h`), linked from the storage engine archive.
const c = struct {
    const ErrorCode = enum(c_int) { ok = 0, busy = 6, stalled = 9, _ };
    const Slice = extern struct {
        ptr: ?[*]const u8 = null,
        len: usize = 0,
        fn fromSlice(value: []const u8) Slice {
            return .{ .ptr = value.ptr, .len = value.len };
        }
    };
    const Buffer = extern struct { ptr: ?[*]u8 = null, len: usize = 0 };
    const storage_kind_lite: u32 = 1;
    const OpenOptions = extern struct {
        abi_size: u32 = @sizeOf(OpenOptions),
        storage_kind: u32 = 0,
        open_mode: u32 = 0,
        profile: u32 = 0,
        flags: u32 = 0,
        reserved0: u32 = 0,
        map_size: u64 = 0,
        ttl_cleanup_enabled: bool = false,
        ttl_cleanup_lease_owned: bool = false,
        ttl_cleanup_batch_size: u32 = 0,
        ttl_cleanup_owner_id: Slice = .{},
        ttl_cleanup_lease_ttl_ms: u64 = 0,
        ttl_cleanup_interval_ms: u64 = 0,
        ttl_cleanup_grace_period_ns: u64 = 0,
        inference_host_budget_mb: u32 = 0,
        inference_backend_budget_mb: u32 = 0,
        inference_process_memory_budget_mb: u32 = 0,
        inference_combined_budget_mb: u32 = 0,
        inference_kv_budget_mb: u32 = 0,
        inference_scratch_budget_mb: u32 = 0,
        busy_timeout_ms: u64 = 0,
        reserved: [6]u64 = @splat(0),
        models_dir: Slice = .{},
    };
    extern fn antfly_error_code_name(code: c_int) [*:0]const u8;
    fn errorCodeName(code: c_int) [*:0]const u8 {
        return antfly_error_code_name(code);
    }
};

const api = struct {
    extern fn antfly_db_create_with_options(path: ?[*:0]const u8, options: ?*const c.OpenOptions, out_handle: ?*?*anyopaque) c.ErrorCode;
    extern fn antfly_db_open_with_options(path: ?[*:0]const u8, options: ?*const c.OpenOptions, out_handle: ?*?*anyopaque) c.ErrorCode;
    extern fn antfly_db_close(handle: ?*anyopaque) void;
    extern fn antfly_db_batch_json(handle: ?*anyopaque, request: c.Slice, out: *c.Buffer) c.ErrorCode;
    extern fn antfly_db_search_json(handle: ?*anyopaque, request: c.Slice, out: *c.Buffer) c.ErrorCode;
    extern fn antfly_db_add_index_json(handle: ?*anyopaque, config: c.Slice) c.ErrorCode;
    extern fn antfly_db_delete_index(handle: ?*anyopaque, name: c.Slice, out_deleted: ?*bool) c.ErrorCode;
    extern fn antfly_db_run_until_idle(handle: ?*anyopaque) c.ErrorCode;
    extern fn antfly_lite_check_json(handle: ?*anyopaque, out: ?*c.Buffer) c.ErrorCode;
    extern fn antfly_buffer_free_zero(buffer: ?*c.Buffer) void;
};

const Mode = enum { process, thread };

const Config = struct {
    dir: []const u8 = "",
    workers: usize = 8,
    seconds: f64 = 10,
    slots: usize = 16,
    busy_timeout_ms: u64 = 10_000,
    modes: []const Mode = &.{ .process, .thread },
    indexed: []const bool = &.{ false, true },
};

/// What one writer reports. Fixed size so a forked child can send it whole.
const max_slots = 256;
const WorkerResult = extern struct {
    ok: u64 = 0,
    busy: u64 = 0,
    stalled: u64 = 0,
    other: u64 = 0,
    first_error: i32 = 0,
    elapsed_ns: u64 = 0,
    /// Per slot: 1 + the sequence of the last acknowledged write (0: none).
    last: [max_slots]u64 = @splat(0),
};

fn nowNs() i128 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec;
}

fn openHandle(path: [:0]const u8, timeout_ms: u64) !?*anyopaque {
    return openOrCreate(path, timeout_ms, false);
}

fn openOrCreate(path: [:0]const u8, timeout_ms: u64, create: bool) !?*anyopaque {
    var options: c.OpenOptions = .{ .storage_kind = c.storage_kind_lite, .busy_timeout_ms = timeout_ms };
    var handle: ?*anyopaque = null;
    const code = if (create)
        api.antfly_db_create_with_options(path.ptr, &options, &handle)
    else
        api.antfly_db_open_with_options(path.ptr, &options, &handle);
    if (code != .ok) {
        std.debug.print("open failed: {s}\n", .{c.errorCodeName(@backingInt(code))});
        return error.OpenFailed;
    }
    return handle;
}

fn batch(handle: ?*anyopaque, request: []const u8) c.ErrorCode {
    var out: c.Buffer = .{};
    defer api.antfly_buffer_free_zero(&out);
    return api.antfly_db_batch_json(handle, .fromSlice(request), &out);
}

fn token(buf: []u8, worker: usize, sequence: u64) []const u8 {
    return std.fmt.bufPrint(buf, "tokw{d}s{d}", .{ worker, sequence }) catch unreachable;
}

fn setup(path: [:0]const u8, cfg: Config, indexed: bool) !void {
    const handle = try openOrCreate(path, cfg.busy_timeout_ms, true);
    defer api.antfly_db_close(handle);
    var deleted = false;
    _ = api.antfly_db_delete_index(handle, .fromSlice("full_text_index_v0"), &deleted);
    if (indexed) {
        const code = api.antfly_db_add_index_json(handle, .fromSlice(
            "{\"name\":\"matched_text\",\"kind\":\"full_text\",\"config_json\":\"{}\"}",
        ));
        if (code != .ok) return error.AddIndexFailed;
    }
    var request: std.ArrayList(u8) = .empty;
    defer request.deinit(std.heap.page_allocator);
    for (0..cfg.workers) |worker| {
        request.clearRetainingCapacity();
        try request.appendSlice(std.heap.page_allocator, "{\"inserts\":{");
        for (0..cfg.slots) |slot| {
            if (slot != 0) try request.append(std.heap.page_allocator, ',');
            try request.print(std.heap.page_allocator, "\"w{d}-{d}\":{{\"text\":\"seed\"}}", .{ worker, slot });
        }
        try request.appendSlice(std.heap.page_allocator, "},\"sync_level\":\"write\"}");
        if (batch(handle, request.items) != .ok) return error.SeedFailed;
    }
    if (api.antfly_db_run_until_idle(handle) != .ok) return error.SeedDrainFailed;
}

fn runWorker(path: [:0]const u8, cfg: Config, indexed: bool, worker: usize) WorkerResult {
    var result: WorkerResult = .{};
    const handle = openHandle(path, cfg.busy_timeout_ms) catch {
        result.other = 1;
        result.first_error = -1;
        return result;
    };
    defer api.antfly_db_close(handle);
    const sync_level = if (indexed) "full_index" else "write";
    const started = nowNs();
    const deadline = started + @as(i128, @intFromFloat(cfg.seconds * std.time.ns_per_s));
    var sequence: u64 = 0;
    var request_buf: [256]u8 = undefined;
    var token_buf: [64]u8 = undefined;
    while (nowNs() < deadline) : (sequence += 1) {
        const slot = sequence % cfg.slots;
        const request = std.fmt.bufPrint(&request_buf, "{{\"inserts\":{{\"w{d}-{d}\":{{\"text\":\"{s}\"}}}},\"sync_level\":\"{s}\"}}", .{ worker, slot, token(&token_buf, worker, sequence), sync_level }) catch unreachable;
        const code = batch(handle, request);
        switch (code) {
            .ok => {
                result.ok += 1;
                result.last[slot] = sequence + 1;
            },
            .busy => result.busy += 1,
            .stalled => result.stalled += 1,
            else => result.other += 1,
        }
        if (code != .ok and result.first_error == 0) result.first_error = @backingInt(code);
        // A write whose outcome is unknown leaves its slot's expectations
        // undefined; stop rather than verify against a guess.
        if (code != .ok and code != .busy) break;
    }
    result.elapsed_ns = @intCast(nowNs() - started);
    return result;
}

fn runProcesses(path: [:0]const u8, cfg: Config, indexed: bool, results: []WorkerResult) !void {
    var fds = try std.heap.page_allocator.alloc([2]std.c.fd_t, cfg.workers);
    defer std.heap.page_allocator.free(fds);
    var pids = try std.heap.page_allocator.alloc(std.c.pid_t, cfg.workers);
    defer std.heap.page_allocator.free(pids);
    for (0..cfg.workers) |worker| {
        if (std.c.pipe(&fds[worker]) != 0) return error.PipeFailed;
        const pid = std.c.fork();
        if (pid == 0) {
            _ = std.c.close(fds[worker][0]);
            const result = runWorker(path, cfg, indexed, worker);
            const bytes = std.mem.asBytes(&result);
            var written: usize = 0;
            while (written < bytes.len) {
                const n = std.c.write(fds[worker][1], bytes[written..].ptr, bytes.len - written);
                if (n <= 0) break;
                written += @intCast(n);
            }
            std.c._exit(0);
        }
        if (pid < 0) return error.ForkFailed;
        pids[worker] = pid;
        _ = std.c.close(fds[worker][1]);
    }
    for (0..cfg.workers) |worker| {
        const bytes = std.mem.asBytes(&results[worker]);
        var read: usize = 0;
        while (read < bytes.len) {
            const n = std.c.read(fds[worker][0], bytes[read..].ptr, bytes.len - read);
            if (n <= 0) break;
            read += @intCast(n);
        }
        _ = std.c.close(fds[worker][0]);
        var status: c_int = 0;
        _ = std.c.waitpid(pids[worker], &status, 0);
        if (read != bytes.len) {
            results[worker] = .{ .other = 1, .first_error = -2 };
        }
    }
}

fn runThreads(path: [:0]const u8, cfg: Config, indexed: bool, results: []WorkerResult) !void {
    const Ctx = struct {
        fn run(p: [:0]const u8, k: Config, i: bool, worker: usize, out: *WorkerResult) void {
            out.* = runWorker(p, k, i, worker);
        }
    };
    var threads = try std.heap.page_allocator.alloc(std.Thread, cfg.workers);
    defer std.heap.page_allocator.free(threads);
    for (0..cfg.workers) |worker| threads[worker] = try std.Thread.spawn(.{}, Ctx.run, .{ path, cfg, indexed, worker, &results[worker] });
    for (threads) |thread| thread.join();
}

const Verification = struct {
    valid: bool = false,
    missing: u64 = 0,
    stale: u64 = 0,
    searches_failed: u64 = 0,
};

fn found(handle: ?*anyopaque, search_token: []const u8, key: []const u8) !bool {
    var query_buf: [256]u8 = undefined;
    const query = try std.fmt.bufPrint(&query_buf, "{{\"full_text_search\":{{\"match\":{{\"field\":\"text\",\"text\":\"{s}\"}}}},\"limit\":16}}", .{search_token});
    var response: c.Buffer = .{};
    defer api.antfly_buffer_free_zero(&response);
    if (api.antfly_db_search_json(handle, .fromSlice(query), &response) != .ok) return error.SearchFailed;
    var quoted_buf: [64]u8 = undefined;
    const quoted = try std.fmt.bufPrint(&quoted_buf, "\"{s}\"", .{key});
    return std.mem.indexOf(u8, response.ptr.?[0..response.len], quoted) != null;
}

fn verify(path: [:0]const u8, cfg: Config, indexed: bool, results: []const WorkerResult) !Verification {
    var out: Verification = .{};
    const handle = try openHandle(path, cfg.busy_timeout_ms);
    defer api.antfly_db_close(handle);
    {
        var report: c.Buffer = .{};
        defer api.antfly_buffer_free_zero(&report);
        if (api.antfly_lite_check_json(handle, &report) == .ok and report.ptr != null)
            out.valid = std.mem.indexOf(u8, report.ptr.?[0..report.len], "\"valid\":true") != null;
        if (!out.valid and report.ptr != null) std.debug.print("check report: {s}\n", .{report.ptr.?[0..@min(report.len, 4000)]});
    }
    if (!indexed) return out;
    var key_buf: [32]u8 = undefined;
    var token_buf: [64]u8 = undefined;
    for (results, 0..) |result, worker| {
        for (0..cfg.slots) |slot| {
            const last = result.last[slot];
            if (last == 0) continue;
            const key = try std.fmt.bufPrint(&key_buf, "w{d}-{d}", .{ worker, slot });
            const newest = last - 1;
            if (!(found(handle, token(&token_buf, worker, newest), key) catch blk: {
                out.searches_failed += 1;
                break :blk true;
            })) out.missing += 1;
            if (newest >= cfg.slots) {
                if (found(handle, token(&token_buf, worker, newest - cfg.slots), key) catch blk: {
                    out.searches_failed += 1;
                    break :blk false;
                }) out.stale += 1;
            }
        }
    }
    return out;
}

fn runConfig(cfg: Config, mode: Mode, indexed: bool, ordinal: usize) !bool {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrintSentinel(&path_buf, "{s}/bench-{d}-{s}-{s}.aflite", .{ cfg.dir, ordinal, @tagName(mode), if (indexed) "indexed" else "plain" }, 0);
    try setup(path, cfg, indexed);
    const results = try std.heap.page_allocator.alloc(WorkerResult, cfg.workers);
    defer std.heap.page_allocator.free(results);
    @memset(results, .{});
    const started = nowNs();
    switch (mode) {
        .process => try runProcesses(path, cfg, indexed, results),
        .thread => try runThreads(path, cfg, indexed, results),
    }
    const elapsed_s = @as(f64, @floatFromInt(nowNs() - started)) / std.time.ns_per_s;
    var total: WorkerResult = .{};
    for (results) |result| {
        total.ok += result.ok;
        total.busy += result.busy;
        total.stalled += result.stalled;
        total.other += result.other;
        if (total.first_error == 0) total.first_error = result.first_error;
    }
    const v = try verify(path, cfg, indexed, results);
    const passed = v.valid and v.missing == 0 and v.stale == 0 and v.searches_failed == 0 and total.stalled == 0 and total.other == 0 and total.busy == 0;
    const first_error: [*:0]const u8 = if (total.first_error > 0) c.errorCodeName(total.first_error) else if (total.first_error < 0) "WORKER_FAILED" else "";
    std.debug.print(
        "{{\"mode\":\"{s}\",\"indexed\":{},\"workers\":{d},\"seconds\":{d:.1},\"writes\":{d},\"writes_per_sec\":{d:.0},\"busy\":{d},\"stalled\":{d},\"other\":{d},\"first_error\":\"{s}\",\"valid\":{},\"missing\":{d},\"stale\":{d},\"search_failures\":{d},\"passed\":{}}}\n",
        .{ @tagName(mode), indexed, cfg.workers, elapsed_s, total.ok, @as(f64, @floatFromInt(total.ok)) / elapsed_s, total.busy, total.stalled, total.other, first_error, v.valid, v.missing, v.stale, v.searches_failed, passed },
    );
    return passed;
}

pub fn main(init: std.process.Init) !u8 {
    var cfg: Config = .{};
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.skip();
    while (args.next()) |arg| {
        const value = args.next() orelse return error.MissingArgumentValue;
        if (std.mem.eql(u8, arg, "--dir")) {
            cfg.dir = value;
        } else if (std.mem.eql(u8, arg, "--workers")) {
            cfg.workers = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--seconds")) {
            cfg.seconds = try std.fmt.parseFloat(f64, value);
        } else if (std.mem.eql(u8, arg, "--slots")) {
            cfg.slots = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--mode")) {
            cfg.modes = if (std.mem.eql(u8, value, "process")) &.{.process} else if (std.mem.eql(u8, value, "thread")) &.{.thread} else &.{ .process, .thread };
        } else if (std.mem.eql(u8, arg, "--indexed")) {
            cfg.indexed = if (std.mem.eql(u8, value, "yes")) &.{true} else if (std.mem.eql(u8, value, "no")) &.{false} else &.{ false, true };
        } else return error.UnknownArgument;
    }
    if (cfg.dir.len == 0 or cfg.workers == 0 or cfg.slots == 0 or cfg.slots > max_slots) return error.InvalidArguments;
    var all_passed = true;
    var ordinal: usize = 0;
    for (cfg.indexed) |indexed| for (cfg.modes) |mode| {
        all_passed = (runConfig(cfg, mode, indexed, ordinal) catch |err| blk: {
            std.debug.print("{{\"mode\":\"{s}\",\"indexed\":{},\"error\":\"{s}\",\"passed\":false}}\n", .{ @tagName(mode), indexed, @errorName(err) });
            break :blk false;
        }) and all_passed;
        ordinal += 1;
    };
    return if (all_passed) 0 else 1;
}
