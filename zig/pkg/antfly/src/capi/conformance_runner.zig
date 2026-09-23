// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

//! Reference runner for the shared libantfly conformance cases
//! (pkg/antfly/capi-conformance). It links libantfly and calls the C ABI
//! through the public antfly.h header only, so the cases are validated
//! against the ABI itself before any binding runs them.
//!
//! Usage: antfly-capi-conformance <cases-dir> <work-dir>

const std = @import("std");
const c = @cImport(@cInclude("antfly.h"));

const Allocator = std.mem.Allocator;
const Value = std.json.Value;

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    const io = init.io;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();
    _ = args.next();
    const cases_dir = args.next() orelse return usage();
    const work_dir = args.next() orelse return usage();

    if (c.antfly_threading_mode() != c.ANTFLY_THREADING_SERIALIZED) {
        std.debug.print("unexpected threading mode {d}\n", .{c.antfly_threading_mode()});
        return error.ConformanceFailed;
    }

    var names: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (names.items) |name| alloc.free(name);
        names.deinit(alloc);
    }
    {
        var dir = try std.Io.Dir.cwd().openDir(io, cases_dir, .{ .iterate = true });
        defer dir.close(io);
        var it = dir.iterate();
        while (try it.next(io)) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".json")) continue;
            try names.append(alloc, try alloc.dupe(u8, entry.name));
        }
    }
    std.mem.sort([]u8, names.items, {}, struct {
        fn lessThan(_: void, a: []u8, b: []u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);
    if (names.items.len == 0) {
        std.debug.print("no conformance cases in {s}\n", .{cases_dir});
        return error.ConformanceFailed;
    }

    var failures: usize = 0;
    for (names.items) |name| {
        const case_path = try std.fs.path.join(alloc, &.{ cases_dir, name });
        defer alloc.free(case_path);
        const stem = name[0 .. name.len - ".json".len];
        const case_dir = try std.fs.path.join(alloc, &.{ work_dir, stem });
        defer alloc.free(case_dir);
        std.Io.Dir.cwd().deleteTree(io, case_dir) catch {};
        try std.Io.Dir.cwd().createDirPath(io, case_dir);

        var arena_state = std.heap.ArenaAllocator.init(alloc);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var runner = Runner{ .arena = arena, .io = io, .dir = case_dir };
        defer runner.closeCurrent();
        runner.runCase(case_path) catch |err| {
            failures += 1;
            std.debug.print("FAIL {s}: {s}\n", .{ stem, runner.failure orelse @errorName(err) });
            continue;
        };
        std.debug.print("ok   {s}\n", .{stem});
    }
    std.debug.print("{d} cases, {d} failed\n", .{ names.items.len, failures });
    if (failures != 0) return error.ConformanceFailed;
}

fn usage() error{InvalidArguments} {
    std.debug.print("usage: antfly-capi-conformance <cases-dir> <work-dir>\n", .{});
    return error.InvalidArguments;
}

const StepError = error{ StepFailed, OutOfMemory };

const Outcome = struct {
    code: c.antfly_error_code = c.ANTFLY_OK,
    /// Raw result text: JSON output or a JSON-encoded scalar.
    text: ?[]const u8 = null,
};

const Runner = struct {
    arena: Allocator,
    io: std.Io,
    dir: []const u8,
    handle: ?*anyopaque = null,
    path: []const u8 = "",
    backup: []const u8 = "",
    failure: ?[]const u8 = null,

    fn fail(self: *Runner, comptime fmt: []const u8, args: anytype) StepError {
        self.failure = std.fmt.allocPrint(self.arena, fmt, args) catch "out of memory";
        return error.StepFailed;
    }

    fn runCase(self: *Runner, case_path: []const u8) !void {
        const raw = try std.Io.Dir.cwd().readFileAlloc(self.io, case_path, self.arena, .limited(1 << 20));
        const parsed = try std.json.parseFromSliceLeaky(Value, self.arena, raw, .{});
        const case = parsed.object;
        const open = if (case.get("open")) |o| o else Value{ .object = .empty };
        const opened = try self.openAt(open, null);
        if (opened.code != c.ANTFLY_OK) return self.fail("open: {s}", .{codeName(opened.code)});
        const steps = (case.get("steps") orelse return self.fail("case has no steps", .{})).array;
        for (steps.items, 0..) |step, i| {
            self.runStep(step) catch |err| {
                const detail = self.failure orelse @errorName(err);
                return self.fail("step {d} ({s}): {s}", .{ i, str(step, "op") orelse "?", detail });
            };
        }
    }

    fn runStep(self: *Runner, step: Value) StepError!void {
        const outcome = try self.execute(step);
        const expect = step.object.get("expect") orelse {
            if (outcome.code != c.ANTFLY_OK) return self.fail("failed with {s}", .{codeName(outcome.code)});
            return;
        };
        if (str(expect, "error")) |want| {
            if (outcome.code == c.ANTFLY_OK) return self.fail("succeeded, want {s}", .{want});
            if (!std.mem.eql(u8, codeName(outcome.code), want))
                return self.fail("error {s}, want {s}", .{ codeName(outcome.code), want });
            return;
        }
        if (outcome.code != c.ANTFLY_OK) return self.fail("failed with {s}", .{codeName(outcome.code)});
        const text = outcome.text orelse "";
        if (expect.object.get("json_subset")) |want| {
            const got = std.json.parseFromSliceLeaky(Value, self.arena, text, .{}) catch
                return self.fail("result is not JSON: {s}", .{text});
            if (!jsonSubset(want, got)) return self.fail("result {s} does not contain the expected subset", .{text});
        }
        if (expect.object.get("contains")) |list| for (list.array.items) |s| {
            if (std.mem.indexOf(u8, text, s.string) == null) return self.fail("result {s} does not contain \"{s}\"", .{ text, s.string });
        };
        if (expect.object.get("not_contains")) |list| for (list.array.items) |s| {
            if (std.mem.indexOf(u8, text, s.string) != null) return self.fail("result {s} unexpectedly contains \"{s}\"", .{ text, s.string });
        };
        if (expect.object.get("equals")) |want| {
            const got = std.json.parseFromSliceLeaky(Value, self.arena, text, .{}) catch
                return self.fail("scalar result {s} is not JSON", .{text});
            if (!jsonSubset(want, got)) return self.fail("result {s} is not equal to the expected value", .{text});
        }
    }

    fn execute(self: *Runner, step: Value) StepError!Outcome {
        const op = str(step, "op") orelse return self.fail("step has no op", .{});
        const h = self.handle;
        if (eql(op, "batch")) {
            const writes = try self.writeIntents(step);
            return .{ .code = c.antfly_db_batch(h, writes.ptr, writes.len, null, 0, uint(step, "timestamp"), 0) };
        }
        if (eql(op, "batch_json")) return self.withInput(c.antfly_db_batch_json, try self.encoded(step, "request"));
        if (eql(op, "lookup")) return self.withInput(c.antfly_db_lookup_json, str(step, "key") orelse "");
        if (eql(op, "scan")) return self.withInput(c.antfly_db_scan_json, try self.encoded(step, "request"));
        if (eql(op, "search")) return self.withInput(c.antfly_db_search_json, try self.encoded(step, "request"));
        if (eql(op, "stats")) return self.output(c.antfly_db_stats_json);
        if (eql(op, "status")) return self.output(c.antfly_lite_status_json);
        if (eql(op, "capabilities")) return self.output(c.antfly_lite_capabilities_json);
        if (eql(op, "check")) return self.output(c.antfly_lite_check_json);
        if (eql(op, "pending_work_stats")) return self.output(c.antfly_db_pending_work_stats_json);
        if (eql(op, "run_until_idle")) return .{ .code = c.antfly_db_run_until_idle(h) };
        if (eql(op, "get_schema")) return self.output(c.antfly_db_get_schema_json);
        if (eql(op, "set_schema")) return .{ .code = c.antfly_db_set_schema_json(h, slice(try self.encoded(step, "schema"))) };
        if (eql(op, "list_indexes")) return self.output(c.antfly_db_list_indexes_json);
        if (eql(op, "add_index")) return .{ .code = c.antfly_db_add_index_json(h, slice(try self.encoded(step, "config"))) };
        if (eql(op, "delete_index")) {
            var deleted = false;
            const code = c.antfly_db_delete_index(h, slice(str(step, "name") orelse ""), &deleted);
            return .{ .code = code, .text = if (deleted) "true" else "false" };
        }
        if (eql(op, "get_edges")) {
            const direction_name = str(step, "direction") orelse "out";
            const direction: u8 = if (eql(direction_name, "out"))
                c.ANTFLY_GRAPH_DIRECTION_OUT
            else if (eql(direction_name, "in"))
                c.ANTFLY_GRAPH_DIRECTION_IN
            else if (eql(direction_name, "both"))
                c.ANTFLY_GRAPH_DIRECTION_BOTH
            else
                return self.fail("unknown direction {s}", .{direction_name});
            var buf: c.antfly_buffer = .{ .ptr = null, .len = 0 };
            const code = c.antfly_db_get_edges_json(
                h,
                slice(str(step, "index") orelse ""),
                slice(str(step, "key") orelse ""),
                slice(str(step, "edge_type") orelse ""),
                direction,
                &buf,
            );
            if (code != c.ANTFLY_OK) return .{ .code = code };
            return .{ .text = try self.take(buf) };
        }
        if (eql(op, "list_enrichments")) return self.output(c.antfly_db_list_enrichments_json);
        if (eql(op, "add_enrichment")) return .{ .code = c.antfly_db_add_enrichment_json(h, slice(try self.encoded(step, "config"))) };
        if (eql(op, "delete_enrichment")) {
            var deleted = false;
            const code = c.antfly_db_delete_enrichment(h, slice(str(step, "kind") orelse ""), slice(str(step, "name") orelse ""), &deleted);
            return .{ .code = code, .text = if (deleted) "true" else "false" };
        }
        if (eql(op, "begin_transaction")) {
            var id = try self.txnId(step);
            return .{ .code = c.antfly_db_begin_transaction_with_id(h, &id, uint(step, "timestamp"), null, 0) };
        }
        if (eql(op, "write_transaction")) {
            var id = try self.txnId(step);
            const writes = try self.writeIntents(step);
            return .{ .code = c.antfly_db_write_transaction(h, &id, writes.ptr, writes.len, null, 0) };
        }
        if (eql(op, "resolve_transaction")) {
            var id = try self.txnId(step);
            const status_name = str(step, "status") orelse "";
            const status: u8 = if (eql(status_name, "committed"))
                c.ANTFLY_TXN_COMMITTED
            else if (eql(status_name, "aborted"))
                c.ANTFLY_TXN_ABORTED
            else
                return self.fail("unknown transaction status {s}", .{status_name});
            return .{ .code = c.antfly_db_resolve_intents(h, &id, status, uint(step, "commit_version")) };
        }
        if (eql(op, "transaction_status")) {
            var id = try self.txnId(step);
            var status: u8 = 0;
            const code = c.antfly_db_get_transaction_status(h, &id, &status);
            const name: []const u8 = switch (status) {
                c.ANTFLY_TXN_PENDING => "\"pending\"",
                c.ANTFLY_TXN_COMMITTED => "\"committed\"",
                c.ANTFLY_TXN_ABORTED => "\"aborted\"",
                else => "\"unknown\"",
            };
            return .{ .code = code, .text = name };
        }
        if (eql(op, "commit_version")) {
            var id = try self.txnId(step);
            var version: u64 = 0;
            const code = c.antfly_db_get_commit_version(h, &id, &version);
            return .{ .code = code, .text = try std.fmt.allocPrint(self.arena, "{d}", .{version}) };
        }
        if (eql(op, "backup")) {
            var buf: c.antfly_buffer = .{ .ptr = null, .len = 0 };
            const code = c.antfly_lite_backup(h, &buf);
            if (code == c.ANTFLY_OK) self.backup = try self.take(buf);
            return .{ .code = code };
        }
        if (eql(op, "restore_open")) {
            const path = try self.resolvePath(str(step, "path"));
            var buf: c.antfly_buffer = .{ .ptr = null, .len = 0 };
            const code = c.antfly_lite_restore_backup_json(path.ptr, slice(self.backup), false, &buf);
            if (code != c.ANTFLY_OK) return .{ .code = code };
            _ = try self.take(buf);
            self.closeCurrent();
            return self.openAt(step, null);
        }
        if (eql(op, "reopen")) {
            const current = self.path;
            self.closeCurrent();
            return self.openAt(step, current);
        }
        if (eql(op, "open_second")) {
            const path = try self.resolvePath(str(step, "path"));
            var second: ?*anyopaque = null;
            const code = try self.openRaw(step, path, &second);
            if (code == c.ANTFLY_OK) c.antfly_db_close(second);
            return .{ .code = code };
        }
        if (eql(op, "close")) {
            self.closeCurrent();
            return .{};
        }
        return self.fail("unknown op {s}", .{op});
    }

    fn resolvePath(self: *Runner, name: ?[]const u8) StepError![:0]const u8 {
        return std.fs.path.joinZ(self.arena, &.{ self.dir, name orelse "db.aflite" });
    }

    fn openAt(self: *Runner, fields: Value, default_path: ?[]const u8) StepError!Outcome {
        const path: [:0]const u8 = if (str(fields, "path")) |name|
            try self.resolvePath(name)
        else if (default_path) |p|
            try self.arena.dupeZ(u8, p)
        else
            try self.resolvePath(null);
        const code = try self.openRaw(fields, path, &self.handle);
        if (code == c.ANTFLY_OK) self.path = path;
        return .{ .code = code };
    }

    fn openRaw(self: *Runner, fields: Value, path: [:0]const u8, out: *?*anyopaque) StepError!c.antfly_error_code {
        var opts: c.antfly_lite_open_options = undefined;
        if (c.antfly_lite_open_options_init(&opts) != c.ANTFLY_OK) return self.fail("open options init failed", .{});
        const mode = str(fields, "mode") orelse "writer";
        opts.open_mode = if (eql(mode, "writer"))
            c.ANTFLY_LITE_OPEN_MODE_WRITER
        else if (eql(mode, "readonly"))
            c.ANTFLY_LITE_OPEN_MODE_READONLY
        else if (eql(mode, "status_only"))
            c.ANTFLY_LITE_OPEN_MODE_STATUS_ONLY
        else
            return self.fail("unknown mode {s}", .{mode});
        const profile = str(fields, "profile") orelse "native";
        opts.profile = if (eql(profile, "native"))
            c.ANTFLY_LITE_PROFILE_NATIVE
        else if (eql(profile, "hosted"))
            c.ANTFLY_LITE_PROFILE_HOSTED
        else
            return self.fail("unknown profile {s}", .{profile});
        if (boolean(fields, "no_sync")) opts.flags |= c.ANTFLY_LITE_OPEN_FLAG_NO_SYNC;
        opts.busy_timeout_ms = uint(fields, "busy_timeout_ms");
        return if (boolean(fields, "create"))
            c.antfly_lite_create_with_options(path.ptr, &opts, out)
        else
            c.antfly_lite_open_with_options(path.ptr, &opts, out);
    }

    fn closeCurrent(self: *Runner) void {
        if (self.handle) |h| c.antfly_db_close(h);
        self.handle = null;
    }

    fn output(self: *Runner, comptime f: anytype) StepError!Outcome {
        var buf: c.antfly_buffer = .{ .ptr = null, .len = 0 };
        const code = f(self.handle, &buf);
        if (code != c.ANTFLY_OK) return .{ .code = code };
        return .{ .text = try self.take(buf) };
    }

    fn withInput(self: *Runner, comptime f: anytype, input: []const u8) StepError!Outcome {
        var buf: c.antfly_buffer = .{ .ptr = null, .len = 0 };
        const code = f(self.handle, slice(input), &buf);
        if (code != c.ANTFLY_OK) return .{ .code = code };
        return .{ .text = try self.take(buf) };
    }

    /// Copies a returned buffer into the arena and frees it.
    fn take(self: *Runner, buf: c.antfly_buffer) StepError![]const u8 {
        defer {
            var owned = buf;
            c.antfly_buffer_free(&owned);
        }
        if (buf.ptr == null) return "";
        return self.arena.dupe(u8, buf.ptr[0..buf.len]);
    }

    fn encoded(self: *Runner, step: Value, field: []const u8) StepError![]const u8 {
        const v = step.object.get(field) orelse return self.fail("step has no {s}", .{field});
        return std.json.Stringify.valueAlloc(self.arena, v, .{});
    }

    fn writeIntents(self: *Runner, step: Value) StepError![]c.antfly_write_intent {
        const list = (step.object.get("writes") orelse return self.fail("step has no writes", .{})).array;
        const out = try self.arena.alloc(c.antfly_write_intent, list.items.len);
        for (list.items, out) |w, *intent| {
            const delete = boolean(w, "delete");
            intent.* = .{
                .key = slice(str(w, "key") orelse ""),
                .value = if (delete) slice("") else slice(try self.encoded(w, "value")),
                .is_delete = delete,
            };
        }
        return out;
    }

    fn txnId(self: *Runner, step: Value) StepError![16]u8 {
        const hex = str(step, "txn_id") orelse return self.fail("step has no txn_id", .{});
        var id: [16]u8 = undefined;
        if (hex.len != 32) return self.fail("txn_id must be 32 hex characters", .{});
        _ = std.fmt.hexToBytes(&id, hex) catch return self.fail("txn_id is not hex", .{});
        return id;
    }
};

fn slice(bytes: []const u8) c.antfly_slice {
    return .{ .ptr = bytes.ptr, .len = bytes.len };
}

fn codeName(code: c.antfly_error_code) []const u8 {
    return std.mem.span(c.antfly_error_code_name(code));
}

fn eql(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

fn str(v: Value, field: []const u8) ?[]const u8 {
    const f = v.object.get(field) orelse return null;
    return switch (f) {
        .string => |s| s,
        else => null,
    };
}

fn uint(v: Value, field: []const u8) u64 {
    const f = v.object.get(field) orelse return 0;
    return switch (f) {
        .integer => |i| @intCast(i),
        else => 0,
    };
}

fn boolean(v: Value, field: []const u8) bool {
    const f = v.object.get(field) orelse return false;
    return switch (f) {
        .bool => |b| b,
        else => false,
    };
}

/// Recursive subset match per the case format: objects match when every
/// expected key matches, arrays element-wise with equal length, scalars
/// exactly (integers and floats compare numerically).
fn jsonSubset(want: Value, got: Value) bool {
    switch (want) {
        .object => |w| {
            if (got != .object) return false;
            var it = w.iterator();
            while (it.next()) |entry| {
                const g = got.object.get(entry.key_ptr.*) orelse return false;
                if (!jsonSubset(entry.value_ptr.*, g)) return false;
            }
            return true;
        },
        .array => |w| {
            if (got != .array or got.array.items.len != w.items.len) return false;
            for (w.items, got.array.items) |a, b| if (!jsonSubset(a, b)) return false;
            return true;
        },
        .string => |s| return got == .string and std.mem.eql(u8, s, got.string),
        .bool => |b| return got == .bool and got.bool == b,
        .null => return got == .null,
        .integer, .float, .number_string => return numberEq(want, got),
    }
}

fn numberEq(a: Value, b: Value) bool {
    const x = asF64(a) orelse return false;
    const y = asF64(b) orelse return false;
    return x == y;
}

fn asF64(v: Value) ?f64 {
    return switch (v) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        .number_string => |s| std.fmt.parseFloat(f64, s) catch null,
        else => null,
    };
}
