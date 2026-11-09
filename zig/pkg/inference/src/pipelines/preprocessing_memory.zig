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

//! Allocation ownership for serial media preparation. Initial process-wide
//! credit is borrowed from a permit that must outlive this scope. Codec scratch,
//! tensors, and allocation bookkeeping beyond that forecast acquire their own
//! leases before allocating. Do not move the scope after obtaining its allocator.
const std = @import("std");
const sessions = @import("../backends/session.zig");
const memory = @import("../runtime/tier/memory.zig");
const AdmittedAllocator = @import("../backends/admitted_allocator.zig").AdmittedAllocator;

pub const Scope = struct {
    owner: AdmittedAllocator,
    session: ?sessions.Session,
    budget: ?*memory.RunBudget,
    budget_bytes: usize = 0,
    local_controller: memory.AdmissionController = .{},
    started: bool = false,

    pub fn init(backing: std.mem.Allocator, session: ?sessions.Session, prepaid_host_bytes: usize, budget: ?*memory.RunBudget) @This() {
        return .{ .owner = .{ .backing = backing, .prepaid_host_bytes = prepaid_host_bytes }, .session = session, .budget = budget };
    }

    pub fn allocator(self: *@This()) !std.mem.Allocator {
        if (!self.started) {
            // Process credit is already held by the caller; the per-run ledger
            // separately accounts media preparation alongside model execution.
            if (self.budget) |budget| try budget.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_tier = .host, .scratch_bytes = self.owner.prepaid_host_bytes });
            self.budget_bytes = self.owner.prepaid_host_bytes;
            self.owner.admission_context = self;
            self.owner.admit = acquire;
            self.started = true;
        }
        return self.owner.allocator();
    }

    fn acquire(raw: *anyopaque, amount: usize) !memory.AdmissionLease {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const owned = try std.math.add(usize, self.owner.prepaid_host_bytes, self.owner.reserved_bytes);
        if (self.session) |session| if (session.run_admission) |admission| {
            // Credit and growth belong to one request. Its own baseline must
            // not make an intrinsically oversized request look like contention.
            try admission.validateOwnedHostScratch(try std.math.add(usize, owned, amount));
        };
        if (self.budget) |budget| try budget.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_tier = .host, .scratch_bytes = amount });
        errdefer if (self.budget) |budget| budget.release(.{ .kind = .scratch, .tier = .host, .bytes = amount });
        const lease = if (self.session) |session| (try session.admitHostPreprocess(amount)).lease else null;
        // Offline callers may have only a local run budget and no session.
        // A local lease gives those reservations the same owned cleanup path.
        const result = lease orelse try self.local_controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = amount }, false);
        self.budget_bytes += amount;
        return result;
    }

    /// Return unused growth at a phase boundary while preserving output leases.
    /// Serial image cohorts otherwise reuse capacity to avoid per-buffer IPC.
    pub fn trim(self: *@This()) !void {
        const before = self.owner.reserved_bytes;
        defer {
            const released = before - self.owner.reserved_bytes;
            if (self.budget) |budget| budget.release(.{ .kind = .scratch, .tier = .host, .bytes = released });
            self.budget_bytes -= released;
        }
        try self.owner.trim();
    }

    pub fn failure(self: *const @This(), err: anyerror) anyerror {
        return if (err == error.OutOfMemory) self.owner.admission_error orelse err else err;
    }

    pub fn deinit(self: *@This()) void {
        self.owner.deinit();
        if (self.started) if (self.budget) |budget| budget.release(.{ .kind = .scratch, .tier = .host, .bytes = self.budget_bytes });
        self.budget_bytes = 0;
        self.started = false;
    }
};

const Test = struct {
    const qwen = @import("qwen2vl_multimodal.zig");
    const config = qwen.PreprocessorConfig{ .resample = .nearest, .min_pixels = 784, .max_pixels = 784 };
    const png8 = @embedFile("testdata/wide-rgba8.png");
    const png16 = @embedFile("testdata/wide-rgba16.png");

    fn session(controller: *memory.AdmissionController, limit: usize) sessions.Session {
        return .{ .ptr = undefined, .vtable = undefined, .run_admission = .{ .controller = controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = limit, .scratch_limit_bytes = limit }, .static_workspace_bytes = 0, .check_live_memory = false } };
    }
};

test "image decode admission charges PNG8 and PNG16 scanlines beyond prepaid geometry" {
    const a = std.testing.allocator;
    for ([_][]const u8{ Test.png8, Test.png16 }) |bytes| {
        var controller: memory.AdmissionController = .{};
        const session = Test.session(&controller, 256 * 1024);
        const initial = try Test.qwen.imageWorkingBytes(&.{bytes}, Test.config);
        var permit = try session.admitHostPreprocess(initial);
        defer permit.deinit();
        {
            var budget = memory.RunBudget.init(.{ .host_limit_bytes = 256 * 1024, .scratch_limit_bytes = 256 * 1024 });
            var scope = Scope.init(a, session, initial, &budget);
            defer scope.deinit();
            const alloc = try scope.allocator();
            var prepared = Test.qwen.prepareImage(alloc, bytes, Test.config) catch |err| return scope.failure(err);
            defer prepared.deinit();
            try std.testing.expectEqual(@as(u32, 28), prepared.resized_width);
            try std.testing.expectEqual(@as(u32, 28), prepared.resized_height);
            try std.testing.expect(scope.owner.reserved_bytes > 0);
            try std.testing.expectEqual(initial + scope.owner.reserved_bytes, controller.snapshot().host_scratch_bytes);
            try std.testing.expectEqual(controller.snapshot().host_scratch_bytes, budget.host_scratch_bytes);
            try std.testing.expectEqual(@as(usize, 28 * 28 * 3), prepared.pixel_values.len);
            const fingerprint = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(prepared.pixel_values));
            try scope.trim();
            try std.testing.expectEqual(initial, controller.snapshot().host_scratch_bytes);
            try std.testing.expectEqual(initial, budget.host_scratch_bytes);
            try std.testing.expectEqual(fingerprint, std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(prepared.pixel_values)));
            // Output deinit must precede the allocator owner and borrowed permit.
        }
        try std.testing.expectEqual(initial, controller.snapshot().host_scratch_bytes);
    }
}

test "image decode admission retains earlier image tensors while another PNG decodes" {
    var controller: memory.AdmissionController = .{};
    const session = Test.session(&controller, 512 * 1024);
    const initial = try Test.qwen.imageWorkingBytes(&.{ Test.png8, Test.png16 }, Test.config);
    var permit = try session.admitHostPreprocess(initial);
    defer permit.deinit();
    {
        var scope = Scope.init(std.testing.allocator, session, initial, null);
        defer scope.deinit();
        const a = try scope.allocator();
        var first = try Test.qwen.prepareImage(a, Test.png8, Test.config);
        defer first.deinit();
        const before = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(first.pixel_values));
        var second = try Test.qwen.prepareImage(a, Test.png16, Test.config);
        defer second.deinit();
        try std.testing.expectEqual(before, std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(first.pixel_values)));
        try std.testing.expect(scope.owner.reserved_bytes > 0);
        try std.testing.expect(controller.snapshot().host_scratch_bytes >= scope.owner.live_bytes + scope.owner.metadata_bytes);
    }
    try std.testing.expectEqual(initial, controller.snapshot().host_scratch_bytes);
}

test "image decode admission distinguishes intrinsic limits and peer contention" {
    const initial = try Test.qwen.imageWorkingBytes(&.{Test.png16}, Test.config);
    for ([_]bool{ false, true }) |contended| {
        var controller: memory.AdmissionController = .{};
        const session = Test.session(&controller, if (contended) 256 * 1024 else initial + 32 * 1024);
        var permit = try session.admitHostPreprocess(initial);
        defer permit.deinit();
        var peer = try session.admitHostPreprocess(if (contended) 180 * 1024 else 0);
        defer peer.deinit();
        const before = controller.snapshot().host_scratch_bytes;
        {
            var scope = Scope.init(std.testing.allocator, session, initial, null);
            defer scope.deinit();
            const result = Test.qwen.prepareImage(try scope.allocator(), Test.png16, Test.config);
            if (result) |value| {
                var prepared = value;
                prepared.deinit();
                return error.UnexpectedSuccess;
            } else |err| {
                try std.testing.expectEqual(if (contended) error.ResourceTemporarilyUnavailable else error.ResourceLimitExceeded, scope.failure(err));
                try std.testing.expectEqual(@as(usize, 0), scope.owner.live_bytes);
            }
        }
        try std.testing.expectEqual(before, controller.snapshot().host_scratch_bytes);
    }
}

test "image decode admission honors offline budgets and does not reject fitting quantum tails" {
    var budget = memory.RunBudget.init(.{ .host_limit_bytes = 2048 });
    {
        var scope = Scope.init(std.testing.allocator, null, 0, &budget);
        defer scope.deinit();
        const a = try scope.allocator();
        const small = try a.alloc(u8, 1024);
        defer a.free(small);
        try std.testing.expect(budget.host_scratch_bytes <= 2048);
        try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 4096));
        try std.testing.expectEqual(error.MemoryBudgetExceeded, scope.failure(error.OutOfMemory));
    }
    try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
}

test "image decode admission preserves true backing allocator failure" {
    var controller: memory.AdmissionController = .{};
    const session = Test.session(&controller, 8192);
    var permit = try session.admitHostPreprocess(4096);
    defer permit.deinit();
    var scope = Scope.init(std.testing.failing_allocator, session, 4096, null);
    defer scope.deinit();
    const a = try scope.allocator();
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 128));
    try std.testing.expectEqual(error.OutOfMemory, scope.failure(error.OutOfMemory));
}

test "image decode admission unwinds every decoder allocation failure without losing prepaid credit" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var controller: memory.AdmissionController = .{};
            const session = Test.session(&controller, 256 * 1024);
            const initial = try Test.qwen.imageWorkingBytes(&.{Test.png16}, Test.config);
            var permit = try session.admitHostPreprocess(initial);
            defer permit.deinit();
            defer std.debug.assert(controller.snapshot().host_scratch_bytes == initial);
            var scope = Scope.init(a, session, initial, null);
            defer scope.deinit();
            var prepared = Test.qwen.prepareImage(try scope.allocator(), Test.png16, Test.config) catch |err| return scope.failure(err);
            defer prepared.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "image decode admission generation preparation releases each image once after prompt failure" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var cb: @import("../ops/ops.zig").ComputeBackend = undefined;
            const result = Test.qwen.prepareExpandedPromptEmbeddings(&cb, a, .{ .hidden_size = 1, .image_token_index = 42 }, Test.config, &.{}, &.{Test.png8});
            if (result) |_| return error.UnexpectedSuccess else |err| if (err != error.ImagePlaceholderCountMismatch) return err;
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "image decode admission cancellation releases decoder scratch and preserves its error" {
    const Probe = struct {
        count: usize = 0,
        fn check(raw: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.count += 1;
            if (self.count == 2) return error.Canceled;
        }
    };
    var controller: memory.AdmissionController = .{};
    const session = Test.session(&controller, 256 * 1024);
    const initial = try Test.qwen.imageWorkingBytes(&.{Test.png16}, Test.config);
    var permit = try session.admitHostPreprocess(initial);
    defer permit.deinit();
    {
        var scope = Scope.init(std.testing.allocator, session, initial, null);
        defer scope.deinit();
        var probe = Probe{};
        const control = @import("antfly_image").work_control.Scope.enter(.{ .context = &probe, .check_fn = Probe.check });
        defer control.deinit();
        const result = Test.qwen.prepareImage(try scope.allocator(), Test.png16, Test.config);
        if (result) |value| {
            var prepared = value;
            prepared.deinit();
            return error.UnexpectedSuccess;
        } else |err| try std.testing.expectEqual(error.Canceled, scope.failure(err));
        try std.testing.expectEqual(@as(usize, 0), scope.owner.live_bytes);
    }
    try std.testing.expectEqual(initial, controller.snapshot().host_scratch_bytes);
}

test "image decode admission default Qwen limits charge the original short wide PNG reproduction" {
    const a = std.testing.allocator;
    const width = 4_000_000;
    const rgba = try a.alloc(u8, width * 4);
    defer a.free(rgba);
    for (rgba, 0..) |*byte, i| byte.* = @truncate(i * 71 + i / 11 * 83);
    const png = try @import("antfly_image").png.encodeRgba(a, width, 1, rgba);
    defer a.free(png);
    const config = Test.qwen.PreprocessorConfig{ .resample = .nearest };
    const geometry = try Test.qwen.imageGeometryForEncoded(png, config);
    try std.testing.expectEqual(@as(u32, 35840), geometry.resized_width);
    try std.testing.expectEqual(@as(u32, 28), geometry.resized_height);
    const initial = try Test.qwen.imageWorkingBytes(&.{png}, config);
    // Raw scanlines, RGBA and two unfilter rows alone exceed this forecast.
    try std.testing.expect(initial < width * 16);
    var controller: memory.AdmissionController = .{};
    const session = Test.session(&controller, 128 * 1024 * 1024);
    var permit = try session.admitHostPreprocess(initial);
    defer permit.deinit();
    {
        var scope = Scope.init(a, session, initial, null);
        defer scope.deinit();
        var prepared = Test.qwen.prepareImage(try scope.allocator(), png, config) catch |err| return scope.failure(err);
        defer prepared.deinit();
        try std.testing.expect(scope.owner.reserved_bytes > 0);
        try std.testing.expect(controller.snapshot().host_scratch_bytes >= width * 16);
        try std.testing.expectEqual(geometry.resized_width, prepared.resized_width);
    }
    try std.testing.expectEqual(initial, controller.snapshot().host_scratch_bytes);
}
