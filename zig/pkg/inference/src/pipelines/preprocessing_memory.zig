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

//! Allocation ownership for serial media preparation. Independent process and
//! local-budget credits are borrowed from owners that must outlive this scope.
//! Codec scratch,
//! tensors, and allocation bookkeeping beyond that forecast acquire their own
//! leases before allocating. Do not move the scope after obtaining its allocator.
const std = @import("std");
const sessions = @import("../backends/session.zig");
const memory = @import("../runtime/tier/memory.zig");
const AdmittedAllocator = @import("../backends/admitted_allocator.zig").AdmittedAllocator;

/// Both credits are borrowed. Their respective owners must outlive Scope.
/// They can differ: offline planning can reserve local budget before acquiring
/// process capacity, whereas serving normally prepays both ledgers together.
pub const Credit = struct {
    process_bytes: usize = 0,
    budget_bytes: usize = 0,
};

/// Reserve known media work before model prefill planning chooses its chunk.
pub const BudgetCredit = struct {
    budget: *memory.RunBudget,
    bytes: usize,

    pub fn reserve(budget: *memory.RunBudget, bytes: usize) !@This() {
        try reserveBudget(budget, bytes);
        return .{ .budget = budget, .bytes = bytes };
    }

    pub fn deinit(self: *@This()) void {
        self.budget.release(.{ .kind = .scratch, .tier = .host, .bytes = self.bytes });
        self.bytes = 0;
    }
};

fn reserveBudget(budget: *memory.RunBudget, bytes: usize) !void {
    try budget.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_tier = .host, .scratch_bytes = bytes });
}

pub const Scope = struct {
    owner: AdmittedAllocator,
    budget: ?*memory.RunBudget,
    borrowed_budget_bytes: usize,
    budget_bytes: usize = 0,
    local_controller: memory.AdmissionController = .{},
    started: bool = false,
    /// Synchronous ancestor preparation remains live while a child executes.
    parent: ?*const Scope = null,
    /// External host output leases retained during synchronous post-processing.
    /// Feasibility only: these bytes are already charged and never allocator
    /// credit. Their owning tensors must outlive this borrowed residency.
    required_host_bytes: usize = 0,

    pub fn init(backing: std.mem.Allocator, session: ?sessions.Session, credit: Credit, budget: ?*memory.RunBudget) @This() {
        return .{ .owner = .{ .backing = backing, .session = session, .prepaid_host_bytes = credit.process_bytes }, .budget = budget, .borrowed_budget_bytes = credit.budget_bytes };
    }

    pub fn child(backing: std.mem.Allocator, session: sessions.Session, parent: ?*const Scope) @This() {
        var result = init(backing, session, .{}, null);
        result.parent = parent;
        return result;
    }

    pub fn ownedHostBytesFor(self: *const @This(), session: sessions.Session) !usize {
        const admission = session.run_admission orelse return 0;
        var owned: usize = if (self.owner.session) |source| if (source.run_admission) |source_admission|
            if (source_admission.controller == admission.controller) try std.math.add(usize, self.required_host_bytes, try std.math.add(usize, self.owner.prepaid_host_bytes, self.owner.reserved_bytes)) else 0
        else
            0 else 0;
        if (self.parent) |parent| owned = try std.math.add(usize, owned, try parent.ownedHostBytesFor(session));
        return owned;
    }

    /// Borrow proven, admitted storage for one synchronous model invocation.
    /// Shape and payload remain owned by the caller; no tensor copy is made.
    pub fn tensor(self: *const @This(), comptime T: type, name: []const u8, shape: []const i64, values: []const T) !@import("../backends/tensor.zig").Tensor {
        const bytes = std.mem.sliceAsBytes(values);
        if (!self.owner.ownsRange(bytes)) return error.InvalidAdmissionLeaseReduction;
        const Tensor = @import("../backends/tensor.zig").Tensor;
        const result = Tensor{ .data = @constCast(bytes), .dtype = if (T == f32) .f32 else if (T == i64) .i64 else @compileError("unsupported prepared input dtype"), .shape = shape, .name = name, .allocator = self.owner.backing, .owns_data = false, .owns_shape = false, .shared_storage = @constCast(bytes), .admitted_storage_domain = if (self.owner.session) |session| if (session.run_admission) |admission| admission.controller else null else null };
        _ = try result.denseElementCount();
        return result;
    }

    pub fn admitRun(self: *@This(), session: sessions.Session, request: sessions.RunRequest) !sessions.RunPermit {
        try self.trim();
        return session.admitPrepared(request, try self.ownedHostBytesFor(session));
    }

    /// Direct native executors consume the prepared host arrays themselves.
    pub fn admitInputRun(self: *@This(), session: sessions.Session, request: sessions.RunRequest) !sessions.RunPermit {
        if (request.input_bytes > self.owner.live_bytes) return error.InvalidAdmissionLeaseReduction;
        var credited = request;
        credited.pre_admitted_host_bytes = request.input_bytes;
        return self.admitRun(session, credited);
    }

    pub fn run(self: *@This(), session: sessions.Session, inputs: []const @import("../backends/tensor.zig").Tensor, allocator_: std.mem.Allocator, control: ?@import("../execution_control.zig").InferenceExecutionControl) ![]@import("../backends/tensor.zig").Tensor {
        const active = session.withAdmissionControl(control);
        var permit = try self.admitRun(active, try active.planRun(inputs, null));
        defer permit.deinit();
        return permit.runTransferringOutputs(inputs, allocator_, control);
    }

    pub fn allocator(self: *@This()) !std.mem.Allocator {
        if (!self.started) {
            const own_budget = self.owner.prepaid_host_bytes -| self.borrowed_budget_bytes;
            if (self.budget) |budget| try reserveBudget(budget, own_budget);
            self.budget_bytes = own_budget;
            self.owner.admission_context = self;
            self.owner.admit = acquire;
            self.owner.rollback_admission = rollback;
            self.started = true;
        }
        return self.owner.allocator();
    }

    fn acquire(raw: *anyopaque, amount: usize) !memory.AdmissionLease {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const owned = try std.math.add(usize, self.required_host_bytes, try std.math.add(usize, self.owner.prepaid_host_bytes, self.owner.reserved_bytes));
        const required_budget = (try std.math.add(usize, owned - self.required_host_bytes, amount)) -| self.borrowed_budget_bytes;
        const budget_growth = required_budget -| self.budget_bytes;
        if (self.budget) |budget| try reserveBudget(budget, budget_growth);
        errdefer if (self.budget) |budget| budget.release(.{ .kind = .scratch, .tier = .host, .bytes = budget_growth });
        const lease = if (self.owner.session) |session| (try session.admitHostPreprocessOwned(amount, try std.math.add(usize, owned, if (self.parent) |parent| try parent.ownedHostBytesFor(session) else 0))).lease else null;
        // Offline callers may have only a local run budget and no session.
        // A local lease gives those reservations the same owned cleanup path.
        const result = lease orelse try self.local_controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = amount }, false);
        self.budget_bytes += budget_growth;
        return result;
    }

    /// Return unused growth at a phase boundary while preserving output leases.
    /// Serial image cohorts otherwise reuse capacity to avoid per-buffer IPC.
    pub fn trim(self: *@This()) !void {
        defer self.refundBudget();
        try self.owner.trim();
    }

    fn rollback(raw: *anyopaque, _: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.refundBudget();
    }

    fn refundBudget(self: *@This()) void {
        const needed = (self.owner.prepaid_host_bytes + self.owner.reserved_bytes) -| self.borrowed_budget_bytes;
        const released = self.budget_bytes -| needed;
        if (self.budget) |budget| budget.release(.{ .kind = .scratch, .tier = .host, .bytes = released });
        self.budget_bytes -= released;
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
            var scope = Scope.init(a, session, .{ .process_bytes = initial }, &budget);
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
        var scope = Scope.init(std.testing.allocator, session, .{ .process_bytes = initial }, null);
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
            var scope = Scope.init(std.testing.allocator, session, .{ .process_bytes = initial }, null);
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
        var scope = Scope.init(std.testing.allocator, null, .{}, &budget);
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
    var scope = Scope.init(std.testing.failing_allocator, session, .{ .process_bytes = 4096 }, null);
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
            var budget = memory.RunBudget.init(.{ .host_limit_bytes = 256 * 1024 });
            var credit = try BudgetCredit.reserve(&budget, initial);
            defer credit.deinit();
            defer std.debug.assert(budget.host_scratch_bytes == initial);
            defer std.debug.assert(controller.snapshot().host_scratch_bytes == initial);
            var scope = Scope.init(a, session, .{ .process_bytes = initial, .budget_bytes = credit.bytes }, &budget);
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
        var scope = Scope.init(std.testing.allocator, session, .{ .process_bytes = initial }, null);
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
        var scope = Scope.init(a, session, .{ .process_bytes = initial }, null);
        defer scope.deinit();
        var prepared = Test.qwen.prepareImage(try scope.allocator(), png, config) catch |err| return scope.failure(err);
        defer prepared.deinit();
        try std.testing.expect(scope.owner.reserved_bytes > 0);
        try std.testing.expect(controller.snapshot().host_scratch_bytes >= width * 16);
        try std.testing.expectEqual(geometry.resized_width, prepared.resized_width);
    }
    try std.testing.expectEqual(initial, controller.snapshot().host_scratch_bytes);
}

test "image decode admission media credit participates in prefill planning and is borrowed once" {
    const cfg = @import("../models/gpt.zig").Config{ .hidden_size = 8, .num_hidden_layers = 1, .num_attention_heads = 1, .attention_head_dim = 8, .vocab_size = 1 };
    const components = [_]memory.GptGenerationBudgetComponent{.{ .backend = .cuda, .kv_dtype = .f16, .config = cfg }};
    const full = try components[0].estimate(512, 1, 512);
    const small = try components[0].estimate(512, 1, 32);
    const media_bytes = (full.scratch_bytes - small.scratch_bytes) / 2;
    var budget = memory.RunBudget.init(.{ .scratch_limit_bytes = full.scratch_bytes });
    var credit = try BudgetCredit.reserve(&budget, media_bytes);
    defer credit.deinit();
    const chunk = try memory.reserveGptGenerationAtLargestChunk(&budget, &components, 512, 1, 512);
    try std.testing.expectEqual(@as(usize, 256), chunk);
    var controller: memory.AdmissionController = .{};
    const session = Test.session(&controller, full.scratch_bytes);
    var permit = try session.admitHostPreprocess(media_bytes);
    defer permit.deinit();
    const before = budget.scratchTotalBytes();
    {
        var scope = Scope.init(std.testing.allocator, session, .{ .process_bytes = media_bytes, .budget_bytes = credit.bytes }, &budget);
        defer scope.deinit();
        const a = try scope.allocator();
        const output = try a.alloc(u8, 128);
        defer a.free(output);
        try std.testing.expectEqual(before, budget.scratchTotalBytes());
        try std.testing.expectEqual(media_bytes, controller.snapshot().host_scratch_bytes);
    }
    try std.testing.expectEqual(before, budget.scratchTotalBytes());
}

test "image decode admission budget-only credit does not fabricate prepaid process capacity" {
    var budget = memory.RunBudget.init(.{ .host_limit_bytes = 4096 });
    var credit = try BudgetCredit.reserve(&budget, 4096);
    defer credit.deinit();
    var scope = Scope.init(std.testing.allocator, null, .{ .budget_bytes = credit.bytes }, &budget);
    defer scope.deinit();
    const a = try scope.allocator();
    const output = try a.alloc(u8, 128);
    a.free(output);
    try std.testing.expect(scope.local_controller.snapshot().host_scratch_bytes > 0);
    try std.testing.expectEqual(credit.bytes, budget.host_scratch_bytes);
    try scope.trim();
    try std.testing.expectEqual(@as(usize, 0), scope.local_controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(credit.bytes, budget.host_scratch_bytes);
}

test "image decode admission failed reservation metadata refunds both ledgers before retry" {
    for ([_]usize{ 0, 4096 }) |prepaid| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
        var budget = memory.RunBudget.init(.{ .host_limit_bytes = 128 * 1024 });
        var credit = try BudgetCredit.reserve(&budget, prepaid);
        defer credit.deinit();
        var scope = Scope.init(failing.allocator(), null, .{ .budget_bytes = credit.bytes }, &budget);
        defer scope.deinit();
        const a = try scope.allocator();
        try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 1024));
        try std.testing.expectEqual(@as(usize, 0), scope.local_controller.snapshot().host_scratch_bytes);
        try std.testing.expectEqual(@as(usize, 0), scope.owner.reserved_bytes);
        try std.testing.expectEqual(@as(usize, 0), scope.budget_bytes);
        try std.testing.expectEqual(prepaid, budget.host_scratch_bytes);
        failing.fail_index = std.math.maxInt(usize);
        const retry = try a.alloc(u8, 80 * 1024);
        a.free(retry);
        try scope.trim();
        try std.testing.expectEqual(prepaid, budget.host_scratch_bytes);
        try std.testing.expectEqual(@as(usize, 0), scope.local_controller.snapshot().host_scratch_bytes);
    }
}

test "image decode admission shared denial refunds local budget and permits same-scope retry" {
    var controller: memory.AdmissionController = .{};
    const session = Test.session(&controller, 128 * 1024);
    var permit = try session.admitHostPreprocess(4096);
    defer permit.deinit();
    var budget = memory.RunBudget.init(.{ .host_limit_bytes = 256 * 1024 });
    var credit = try BudgetCredit.reserve(&budget, 4096);
    defer credit.deinit();
    var peer = try session.admitHostPreprocess(120 * 1024);
    defer peer.deinit();
    var scope = Scope.init(std.testing.allocator, session, .{ .process_bytes = 4096, .budget_bytes = credit.bytes }, &budget);
    defer scope.deinit();
    const a = try scope.allocator();
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 16 * 1024));
    try std.testing.expectEqual(error.ResourceTemporarilyUnavailable, scope.failure(error.OutOfMemory));
    try std.testing.expectEqual(credit.bytes, budget.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 124 * 1024), controller.snapshot().host_scratch_bytes);
    peer.deinit();
    const retry = try a.alloc(u8, 16 * 1024);
    a.free(retry);
    try scope.trim();
    try std.testing.expectEqual(credit.bytes, budget.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 4096), controller.snapshot().host_scratch_bytes);
}

test "preprocessing admission growth refunds both budgets after a resident working set denial" {
    var controller = memory.AdmissionController{};
    const limits = memory.Limits{ .host_limit_bytes = 100 * 1024 };
    var weights = try controller.tryAcquire(.cpu, limits, .{ .host_weight_bytes = 90 * 1024 }, false);
    defer weights.release();
    const session = sessions.Session{ .ptr = &controller, .vtable = undefined, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = limits, .static_workspace_bytes = 0, .required_residency = weights.amounts, .check_live_memory = false } };
    var failure = memory.AdmissionFailure{};
    const request_session = session.withAdmissionControl(.{ .admission_failure = &failure });
    var budget = memory.RunBudget.init(limits);
    var scope = Scope.init(std.testing.allocator, request_session, .{}, &budget);
    defer scope.deinit();
    const alloc = try scope.allocator();
    const first = try alloc.alloc(u8, 6 * 1024);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 6 * 1024));
    try std.testing.expectEqual(error.ResourceLimitExceeded, scope.failure(error.OutOfMemory));
    try std.testing.expect(session.run_admission.?.failure == null);
    const exceeded = failure.snapshot().?;
    try std.testing.expect(exceeded.limit == .host);
    try std.testing.expectEqual(limits.host_limit_bytes, exceeded.limit_bytes);
    try std.testing.expect(exceeded.requested_bytes > exceeded.limit_bytes);
    try std.testing.expectEqual(scope.owner.reserved_bytes, budget.host_scratch_bytes);
    alloc.free(first);
    try scope.trim();
    try std.testing.expectEqualDeep(weights.amounts, controller.snapshot());
    try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
}

test "preprocessing admission discards recovered proposals without losing terminal diagnostics" {
    var controller = memory.AdmissionController{};
    const limits = memory.Limits{ .host_limit_bytes = 100 * 1024 };
    var weights = try controller.tryAcquire(.cpu, limits, .{ .host_weight_bytes = 90 * 1024 }, false);
    defer weights.release();
    var failure = memory.AdmissionFailure{};
    const session = sessions.Session{ .ptr = &controller, .vtable = undefined, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = limits, .static_workspace_bytes = 0, .required_residency = weights.amounts, .failure = &failure, .check_live_memory = false } };
    var scope = Scope.init(std.testing.allocator, session, .{}, null);
    defer scope.deinit();
    const a = try scope.allocator();
    const first = try a.alloc(u8, 512);
    try std.testing.expect(failure.snapshot() == null);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 20 * 1024));
    try std.testing.expectEqual(error.ResourceLimitExceeded, scope.failure(error.OutOfMemory));
    try std.testing.expect(failure.snapshot().?.requested_bytes > limits.host_limit_bytes);
    try std.testing.expectEqual(limits.host_limit_bytes, failure.snapshot().?.limit_bytes);
    a.free(first);
    try scope.trim();
    try std.testing.expectEqualDeep(weights.amounts, controller.snapshot());
}

test "prepared tensor borrowing preserves pointer admission and independent output lifetime" {
    const Tensor = @import("../backends/tensor.zig").Tensor;
    const Fake = struct {
        expected: [*]const f32,
        calls: usize = 0,
        fn info(_: *anyopaque) []const @import("../backends/tensor.zig").TensorInfo {
            return &.{.{ .name = "out", .dtype = .f32, .shape = &.{ 1, 2 } }};
        }
        fn run(raw: *anyopaque, inputs: []const Tensor, a: std.mem.Allocator) ![]Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(inputs[0].asFloat32().ptr == self.expected);
            self.calls += 1;
            const outputs = try a.alloc(Tensor, 1);
            errdefer a.free(outputs);
            outputs[0] = try Tensor.initFloat32(a, "out", &.{ 1, 2 }, &.{ 7, 9 });
            return outputs;
        }
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
    };
    var controller = memory.AdmissionController{};
    var fake = Fake{ .expected = undefined };
    const session = sessions.Session{ .ptr = &fake, .vtable = &.{ .run = Fake.run, .inputInfo = Fake.info, .outputInfo = Fake.info, .backend = Fake.backend, .close = Fake.close }, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 4096, .scratch_limit_bytes = 4096 }, .static_workspace_bytes = 1024, .check_live_memory = false } };
    var parent = Scope.child(std.testing.allocator, session, null);
    defer parent.deinit();
    const parent_a = try parent.allocator();
    _ = try parent_a.alloc(u8, 128);
    try parent.trim();
    var child_scope = Scope.child(std.testing.allocator, session, &parent);
    defer child_scope.deinit();
    const a = try child_scope.allocator();
    const values = try a.alloc(f32, 16);
    @memset(values, 3);
    fake.expected = values.ptr;
    try child_scope.trim();
    var input = try child_scope.tensor(f32, "in", &.{ 1, 16 }, values);
    defer input.deinit();
    try std.testing.expectEqual(@as(usize, 64), try session.inputResidencyCredit(&.{input}));
    try std.testing.expectError(error.InvalidAdmissionLeaseReduction, child_scope.tensor(f32, "foreign", &.{1}, &.{1}));
    const outputs = try child_scope.run(session, &.{input}, std.testing.allocator, null);
    defer std.testing.allocator.free(outputs);
    defer for (outputs) |*output| output.deinit();
    child_scope.deinit();
    parent.deinit();
    try std.testing.expectEqualSlices(f32, &.{ 7, 9 }, outputs[0].asFloat32());
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expect(controller.snapshot().host_scratch_bytes < 512);
}

test "prepared child scopes distinguish aggregate policy failure from peer contention" {
    for ([_]bool{ false, true }) |contended| {
        var controller = memory.AdmissionController{};
        const session = Test.session(&controller, if (contended) 80 * 1024 else 40 * 1024);
        var parent = Scope.child(std.testing.allocator, session, null);
        defer parent.deinit();
        const a = try parent.allocator();
        _ = try a.alloc(u8, 20 * 1024);
        try parent.trim();
        var peer = try session.admitHostPreprocess(if (contended) 40 * 1024 else 0);
        defer peer.deinit();
        var child_scope = Scope.child(std.testing.allocator, session, &parent);
        defer child_scope.deinit();
        const child_a = try child_scope.allocator();
        try std.testing.expectError(error.OutOfMemory, child_a.alloc(u8, 24 * 1024));
        try std.testing.expectEqual(if (contended) error.ResourceTemporarilyUnavailable else error.ResourceLimitExceeded, child_scope.failure(error.OutOfMemory));
    }
}
