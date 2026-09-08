// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One admitted token owner for validation, bounded stage execution and usage.
//! Preparation completes before any forward, preserving whole-request rejection.
const std = @import("std");
const session_mod = @import("../backends/session.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const AdmittedAllocator = @import("../backends/admitted_allocator.zig").AdmittedAllocator;

pub const PreparedTextBatch = struct {
    allocator: std.mem.Allocator,
    ids: [][]i32,
    permit: session_mod.RunPermit,
    token_owner: *AdmittedAllocator,
    reserved_bytes: usize,
    tokenizer: Tokenizer,
    max_sequence: usize,
    total_tokens: usize = 0,
    max_tokens: usize = 0,

    pub fn init(allocator: std.mem.Allocator, session: session_mod.Session, tokenizer: Tokenizer, texts: []const []const u8, max_sequence: usize, control: ?Control) !@This() {
        if (control) |active| try active.check();
        // The caller's item/text contract bounds this queue. Keep all IDs to
        // validate the complete request before forwarding; only tensors and
        // model outputs are window-scoped. Reject capacity before tokenization.
        const mul = std.math.mul;
        const add = std.math.add;
        // Reserve bounded tensor staging separately. Tokenizer allocation is
        // measured/enforced, not estimated from UTF-8 bytes or token limits.
        const bytes = try add(usize, @sizeOf(AdmittedAllocator), try mul(usize, @min(texts.len, 8), try mul(usize, max_sequence, 32)));
        var permit = try session.admitHostPreprocess(bytes);
        errdefer permit.deinit();
        const owner = try allocator.create(AdmittedAllocator);
        errdefer allocator.destroy(owner);
        owner.* = .{ .backing = allocator, .session = session };
        const token_alloc = owner.allocator();
        const ids = token_alloc.alloc([]i32, texts.len) catch |err| return owner.admission_error orelse err;
        var initialized: usize = 0;
        errdefer {
            for (ids[0..initialized]) |item| token_alloc.free(item);
            token_alloc.free(ids);
        }
        var total: usize = 0;
        var maximum: usize = 0;
        for (texts, ids) |text, *item| {
            if (control) |active| try active.check();
            item.* = tokenizer.encode(token_alloc, text) catch |err| return owner.admission_error orelse err;
            initialized += 1;
            total = try add(usize, total, item.len);
            maximum = @max(maximum, item.len);
        }
        if (control) |active| try active.check();
        return .{ .allocator = allocator, .ids = ids, .permit = permit, .token_owner = owner, .reserved_bytes = try add(usize, bytes, owner.live_bytes), .tokenizer = tokenizer, .max_sequence = max_sequence, .total_tokens = total, .max_tokens = maximum };
    }

    pub fn validateFor(self: *const @This(), session: session_mod.Session, tokenizer: Tokenizer, max_sequence: usize) !void {
        if (self.permit.session.ptr != session.ptr or self.permit.session.vtable != session.vtable or
            self.tokenizer.ptr != tokenizer.ptr or self.tokenizer.vtable != tokenizer.vtable or self.max_sequence != max_sequence)
            return error.InvalidPreparedTextInputs;
        const owner = self.permit.session.run_admission;
        const consumer = session.run_admission;
        if ((owner == null) != (consumer == null)) return error.InvalidPreparedTextInputs;
        if (owner) |domain| {
            if (domain.controller != consumer.?.controller or domain.backend_class != consumer.?.backend_class or
                !std.meta.eql(domain.limits, consumer.?.limits)) return error.InvalidPreparedTextInputs;
        }
    }

    pub fn deinit(self: *@This()) void {
        const token_alloc = self.token_owner.allocator();
        for (self.ids) |ids| token_alloc.free(ids);
        token_alloc.free(self.ids);
        std.debug.assert(self.token_owner.live_bytes == 0);
        self.allocator.destroy(self.token_owner);
        self.permit.deinit();
    }
};

fn checkPreparedOwnership(allocator: std.mem.Allocator) !void {
    const memory = @import("../runtime/tier/memory.zig");
    const Probe = struct {
        controller: *memory.AdmissionController,
        fn encode(raw: *anyopaque, alloc: std.mem.Allocator, text: []const u8) ![]i32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.controller.snapshot().hostTotalBytes() > 0);
            var scratch = try alloc.alignedAlloc(u8, .@"64", text.len);
            defer alloc.free(scratch);
            scratch = try alloc.realloc(scratch, text.len * 2);
            scratch = try alloc.realloc(scratch, text.len);
            try std.testing.expectEqual(@as(usize, 0), @intFromPtr(scratch.ptr) % 64);
            const ids = try alloc.alloc(i32, text.len);
            @memset(ids, 1);
            return ids;
        }
    };
    var controller = memory.AdmissionController{};
    defer std.debug.assert(controller.snapshot().hostTotalBytes() == 0);
    var probe = Probe{ .controller = &controller };
    const session = session_mod.Session{ .ptr = &probe, .vtable = undefined, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 1, .check_live_memory = false } };
    const tokenizer = Tokenizer{ .ptr = &probe, .vtable = &.{ .encode = Probe.encode, .decode = undefined, .encodeInto = undefined, .encodeForModel = undefined, .encodeGeneration = undefined, .specialTokens = undefined, .vocabSize = undefined, .deinit = undefined } };
    var prepared = try PreparedTextBatch.init(allocator, session, tokenizer, &.{ "one", "second" }, 16, null);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 9), prepared.total_tokens);
    try std.testing.expectEqual(@as(usize, 6), prepared.max_tokens);
}

test "prepared text admits before tokenization and unwinds every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkPreparedOwnership, .{});
}

test "prepared text charges tokenizer scratch and denies growth before allocation" {
    const memory = @import("../runtime/tier/memory.zig");
    const Probe = struct {
        controller: *memory.AdmissionController,
        observed_peak: usize = 0,
        fn encode(raw: *anyopaque, alloc: std.mem.Allocator, text: []const u8) ![]i32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Model Unigram's Viterbi arrays: independent of max_sequence.
            const scores = try alloc.alloc(f32, text.len + 1);
            defer alloc.free(scores);
            const positions = try alloc.alloc(usize, text.len + 1);
            defer alloc.free(positions);
            self.observed_peak = self.controller.snapshot().hostTotalBytes();
            try std.testing.expect(self.observed_peak >= (text.len + 1) * 12);
            return alloc.dupe(i32, &.{1});
        }
    };
    var controller = memory.AdmissionController{};
    var probe = Probe{ .controller = &controller };
    var session = session_mod.Session{ .ptr = &probe, .vtable = undefined, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{ .host_limit_bytes = 160 * 1024 },
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    const tokenizer = Tokenizer{ .ptr = &probe, .vtable = &.{ .encode = Probe.encode, .decode = undefined, .encodeInto = undefined, .encodeForModel = undefined, .encodeGeneration = undefined, .specialTokens = undefined, .vocabSize = undefined, .deinit = undefined } };
    const text = [_]u8{'a'} ** (16 * 1024);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, PreparedTextBatch.init(std.testing.allocator, session, tokenizer, &.{&text}, 512, null));
    try std.testing.expectEqual(@as(usize, 0), probe.observed_peak);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
    session.run_admission.?.limits.host_limit_bytes = 1024 * 1024;
    var prepared = try PreparedTextBatch.init(std.testing.allocator, session, tokenizer, &.{&text}, 512, null);
    try std.testing.expect(prepared.reserved_bytes < probe.observed_peak);
    try std.testing.expectEqual(prepared.reserved_bytes, controller.snapshot().hostTotalBytes());
    prepared.deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}
