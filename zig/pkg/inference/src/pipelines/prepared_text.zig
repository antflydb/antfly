// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One admitted token owner for validation, bounded stage execution and usage.
//! Preparation completes before any forward, preserving whole-request rejection.
const std = @import("std");
const session_mod = @import("../backends/session.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Tokenizer = @import("inference_tokenizer").Tokenizer;

pub const PreparedTextBatch = struct {
    allocator: std.mem.Allocator,
    ids: [][]i32,
    permit: session_mod.RunPermit,
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
        var bytes = try add(usize, try mul(usize, texts.len, @sizeOf([]i32)), try mul(usize, @min(texts.len, 8), try mul(usize, max_sequence, 32)));
        for (texts) |text| bytes = try add(usize, bytes, try mul(usize, text.len, 8));
        var permit = try session.admitHostPreprocess(bytes);
        errdefer permit.deinit();
        const ids = try allocator.alloc([]i32, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (ids[0..initialized]) |item| allocator.free(item);
            allocator.free(ids);
        }
        var total: usize = 0;
        var maximum: usize = 0;
        for (texts, ids) |text, *item| {
            if (control) |active| try active.check();
            item.* = try tokenizer.encode(allocator, text);
            initialized += 1;
            total = try add(usize, total, item.len);
            maximum = @max(maximum, item.len);
        }
        if (control) |active| try active.check();
        return .{ .allocator = allocator, .ids = ids, .permit = permit, .reserved_bytes = bytes, .tokenizer = tokenizer, .max_sequence = max_sequence, .total_tokens = total, .max_tokens = maximum };
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
        for (self.ids) |ids| self.allocator.free(ids);
        self.allocator.free(self.ids);
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
