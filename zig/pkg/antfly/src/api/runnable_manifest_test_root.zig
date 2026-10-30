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

//! Normal manifest load entry points under actual API-owned runnable capacity.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const inference = @import("inference_server");
const manifest = inference.models.manifest;
const execution = inference.execution_control;
const owner_module = @import("runnable_owner_execution.zig");
const runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const Entry = enum { full, listing, candidate, capability, plan };

fn fixture(alloc: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, entry: Entry, large: bool) !void {
    const padding = try alloc.alloc(u8, if (large) 64 * 1024 else 0);
    defer alloc.free(padding);
    @memset(padding, 'x');
    const config = try std.fmt.allocPrint(alloc, "{{\"hidden_size\":24,\"_padding\":\"{s}\"}}", .{padding});
    defer alloc.free(config);
    const declaration = try std.fmt.allocPrint(alloc, "{{\"type\":\"classifier\",\"capabilities\":[\"classification\"],\"_padding\":\"{s}\"}}", .{padding});
    defer alloc.free(declaration);
    try dir.writeFile(io, .{ .sub_path = "config.json", .data = config });
    try dir.writeFile(io, .{ .sub_path = "model_manifest.json", .data = declaration });
    try dir.writeFile(io, .{ .sub_path = "model.onnx", .data = "onnx" });
    const tokenizer = try std.fmt.allocPrint(alloc, "{{\"model\":{{\"vocab\":[\"{s}\"]}},\"added_tokens\":[{{\"id\":1,\"content\":\"<s>\"}}]}}", .{padding});
    defer alloc.free(tokenizer);
    const tokenizer_config = "{\"bos_token\":\"<s>\"}";
    try dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = tokenizer });
    try dir.writeFile(io, .{ .sub_path = "tokenizer_config.json", .data = tokenizer_config });
    if (entry == .plan) {
        const path = try dir.realPathFileAlloc(io, ".", alloc);
        defer alloc.free(path);
        try inference.registry.download.beginManagedDownload(alloc, io, path);
        const plan = try std.fmt.allocPrint(alloc, "{{\"version\":2,\"artifacts\":[{{\"path\":\"config.json\",\"size\":{d}}},{{\"path\":\"model_manifest.json\",\"size\":{d}}},{{\"path\":\"model.onnx\",\"size\":4}},{{\"path\":\"tokenizer.json\",\"size\":{d}}},{{\"path\":\"tokenizer_config.json\",\"size\":{d}}}]}}", .{ config.len, declaration.len, tokenizer.len, tokenizer_config.len });
        defer alloc.free(plan);
        try dir.writeFile(io, .{ .sub_path = ".antfly-download-plan.json", .data = plan });
    }
}

fn load(alloc: std.mem.Allocator, path: []const u8, entry: Entry) !void {
    if (entry == .capability) {
        try std.testing.expect(try manifest.hasDeclaredCapability(alloc, path, "classification"));
        return;
    }
    var result = switch (entry) {
        .full => try manifest.loadFromDir(alloc, path),
        .listing => try manifest.loadListingFromDir(alloc, path),
        .candidate => (try manifest.loadListingCandidateFromDir(alloc, path)) orelse return error.MissingCandidate,
        .plan => try manifest.loadFromManagedPlanDir(alloc, path),
        .capability => unreachable,
    };
    defer result.deinit();
    try std.testing.expectEqual(manifest.ModelType.classifier, result.model_type);
    if (entry == .full or entry == .plan) {
        try std.testing.expectEqual(@as(u32, 24), result.hidden_size);
        try std.testing.expectEqualStrings("<s>", result.bos_token);
    }
    try std.testing.expect(result.hasCapability("classification"));
    try std.testing.expect(result.onnx_path != null);
}

test "model manifest owner normal load listing capability and private plan yield and preserve refusal" {
    const Clock = struct {
        var instant: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return instant.load(.acquire);
        }
    };
    const Mode = enum { normal, canceled, deadline, lease, schema };
    const Probe = struct {
        owner: runtime.runnable_abi.Owner,
        parent: *anyopaque = undefined,
        io: *const runtime.native_abi.IoBorrow,
        canceled: std.atomic.Value(bool) = .init(false),
        query_done: std.atomic.Value(bool) = .init(false),
        checks: usize = 0,
        reject_at: usize,
        mode: Mode,
        fn checked(status: runtime.error_abi.Status) !void {
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
        fn cancellation(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return @intFromBool(self.canceled.load(.acquire));
        }
        fn query(self: *@This()) anyerror!void {
            var work: ?*anyopaque = null;
            try checked(self.owner.start(self.owner.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.query), .out_handle = &work }));
            defer self.owner.finish(work.?);
            self.query_done.store(true, .release);
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == 1) {
                var control: ?*anyopaque = null;
                try checked(self.owner.start(self.owner.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.control), .allow_wait = 0, .out_handle = &control }));
                self.owner.finish(control.?);
            }
            if (self.checks == self.reject_at) switch (self.mode) {
                .normal => unreachable,
                .canceled => self.canceled.store(true, .release),
                .deadline => Clock.instant.store(1000, .release),
                .lease => return error.ManifestLeaseLost,
                .schema => return error.InvalidModelManifest,
            };
            try checked(self.owner.checkpoint(self.parent));
        }
    };
    const io = std.testing.io;
    const alloc = std.testing.allocator;
    for (std.enums.values(Entry)) |entry| {
        var large_checks: usize = 0;
        for ([_]bool{ true, false }) |large| {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try tmp.dir.realPathFileAlloc(io, ".", alloc);
            defer alloc.free(path);
            var download_lock: ?std.Io.File = null;
            var download_lock_path: ?[]u8 = null;
            defer {
                if (download_lock) |file| file.close(io);
                if (download_lock_path) |lock_path| {
                    std.Io.Dir.cwd().deleteFile(io, lock_path) catch {};
                    alloc.free(lock_path);
                }
            }
            if (entry == .plan) {
                download_lock_path = try inference.registry.download.managedModelLockPathAlloc(alloc, path);
                download_lock = try std.Io.Dir.cwd().createFile(io, download_lock_path.?, .{ .truncate = false, .lock = .exclusive });
            }
            try fixture(alloc, io, tmp.dir, entry, large);
            var total_checks: usize = 0;
            for (std.enums.values(Mode)) |mode| {
                if (large and mode != .normal) continue;
                var reject_at: usize = if (mode == .normal) 0 else 1;
                while (reject_at <= total_checks) : (reject_at += 1) {
                    Clock.instant.store(0, .release);
                    var state: owner_module.State = .{ .owner_alloc = alloc, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .checkpoint_budget = 1 }), .native_now_ns = Clock.now };
                    defer state.owner.tryDeinit() catch @panic("manifest preparation retained execution");
                    const owner = state.port();
                    const borrow = runtime.native_abi.IoBorrow.init(&io);
                    var probe: Probe = .{ .owner = owner, .io = &borrow, .mode = mode, .reject_at = reject_at };
                    var parent: ?*anyopaque = null;
                    try Probe.checked(owner.start(owner.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.background), .slots = 2, .retained_bytes = 123, .has_deadline = 1, .native_deadline = 1, .deadline_ns = 1000, .cancellation = .{ .context = &probe, .is_cancelled = Probe.cancellation }, .out_handle = &parent }));
                    defer if (parent) |value| owner.finish(value);
                    probe.parent = parent.?;
                    const retained = state.owner.scheduler.stats().retained_bytes;
                    var query: ?std.Io.Future(anyerror!void) = null;
                    defer {
                        if (query) |*task| _ = task.cancel(io) catch {};
                    }
                    if (mode == .normal) {
                        query = try io.concurrent(Probe.query, .{&probe});
                        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
                        while (state.owner.scheduler.stats().queued == 0) {
                            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.MissingQueuedQuery;
                            try io.sleep(.fromMilliseconds(1), .awake);
                        }
                    }
                    // Parser-only scope: physical I/O cannot lend this grant in
                    // the fixture, so query progress proves CPU cooperation.
                    var scope = execution.CheckpointScope.init(.{ .context = &probe, .check_fn = Probe.check });
                    const outcome = load(alloc, path, entry);
                    scope.deinit();
                    if (mode == .normal) {
                        try outcome;
                        try query.?.await(io);
                        query = null;
                        try std.testing.expect(probe.query_done.load(.acquire));
                        total_checks = probe.checks;
                        try std.testing.expect(total_checks > 0);
                        if (large) large_checks = probe.checks else try std.testing.expect(large_checks > probe.checks);
                        try std.testing.expectEqual(@as(usize, 2), state.owner.scheduler.stats().runnable_slots);
                    } else {
                        try std.testing.expectError(switch (mode) {
                            .canceled => error.Canceled,
                            .deadline => error.DeadlineExceeded,
                            .lease => error.ManifestLeaseLost,
                            .schema => error.InvalidModelManifest,
                            .normal => unreachable,
                        }, outcome);
                        try std.testing.expectEqual(reject_at, probe.checks);
                    }
                    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
                    try std.testing.expectEqual(retained, state.owner.scheduler.stats().retained_bytes);
                    owner.finish(parent.?);
                    parent = null;
                    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
                    try std.testing.expectEqual(@as(u64, 0), state.owner.scheduler.stats().retained_bytes);
                    if (mode == .normal) break;
                }
            }
        }
    }
}

test "model manifest owner retained refusal cannot become absent optional metadata" {
    const Probe = struct {
        calls: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == 1) return error.ManifestLeaseLost;
        }
    };
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(path);
    var probe: Probe = .{};
    var scope = execution.CheckpointScope.init(.{ .context = &probe, .check_fn = Probe.check });
    defer if (scope.active) scope.deinit();
    try std.testing.expectError(error.ManifestLeaseLost, (execution.InferenceExecutionControl{}).check());
    for (std.enums.values(Entry)) |entry| try std.testing.expectError(error.ManifestLeaseLost, load(a, path, entry));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    scope.deinit();
    // The retired scope conveys no authority or refusal to a fresh call.
    try std.testing.expect(!try manifest.hasDeclaredCapability(a, path, "classification"));
    var candidate = (try manifest.loadListingCandidateFromDir(a, path)) orelse return error.MissingCandidate;
    defer candidate.deinit();
}
