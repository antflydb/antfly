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

//! Transactional, provider-neutral text preparation. Storage is caller-arena
//! owned; authoritative history is never mutated. Publish `next` only after
//! this function succeeds. Summaries have no instruction authority.
const std = @import("std");
const p = @import("root.zig");

pub const Mode = enum { inspect, reject, window, compact };
pub const Options = struct {
    mode: Mode = .compact,
    target_input_tokens: usize,
    reserved_output_tokens: usize = 1024,
    keep_recent_turns: usize = 2,
    summary_input_tokens: usize = 4096,
    summary_output_tokens: usize = 1024,
    max_batches: usize = 8,
    limits: p.Limits = .{},
};
pub const Control = struct {
    ptr: *anyopaque = undefined,
    check_fn: ?*const fn (*anyopaque) anyerror!void = null,
    pub fn check(self: Control) !void {
        if (self.check_fn) |f| try f(self.ptr);
    }
};
pub const Summary = struct { text: []const u8, input_tokens: usize = 0, output_tokens: usize = 0 };
pub const Runner = struct {
    ptr: *anyopaque,
    run_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, usize) anyerror!Summary,
};
/// Request-local derived state. Continuations must supply full authoritative
/// history. Changing providers does not invalidate text; every preparation
/// recounts with the destination counter. Never accept this struct from wire.
pub const State = struct {
    source_messages: usize = 0,
    source_digest: p.Digest = @splat(0),
    keep: []const bool = &.{},
    memory: ?p.StructuredMemory = null,
};
pub const Diagnostics = struct {
    contract_version: usize = p.summary_contract_version,
    original_input_tokens: usize = 0,
    effective_input_tokens: usize = 0,
    retained_messages: usize = 0,
    compacted_messages: usize = 0,
    batches: usize = 0,
    retries: usize = 0,
    summary_input_tokens: usize = 0,
    summary_output_tokens: usize = 0,
};
pub const Prepared = struct {
    next: State,
    rendered_memory: ?[]const u8,
    diagnostics: Diagnostics,
};

pub fn prepare(a: std.mem.Allocator, source: []const p.MessageView, previous: State, options: Options, counter: p.ExactCounter, summary_counter: p.ExactCounter, runner: ?Runner, control: Control) !Prepared {
    try control.check();
    if (options.target_input_tokens == 0 or options.summary_input_tokens == 0 or options.summary_output_tokens == 0 or
        options.max_batches == 0 or options.max_batches > 32 or source.len > 4096) return error.InvalidContextRequest;
    if (previous.source_messages > source.len or previous.keep.len != previous.source_messages) return error.CheckpointHistoryMismatch;
    if (previous.source_messages > 0 and !std.mem.eql(u8, &previous.source_digest, &try p.digestPrefix(source, previous.source_messages))) return error.CheckpointHistoryMismatch;
    const keep = try a.alloc(bool, source.len);
    @memset(keep, true);
    @memcpy(keep[0..previous.keep.len], previous.keep);
    var memory = previous.memory;
    var rendered: ?[]const u8 = if (memory) |m| try p.renderMemory(a, m, options.limits) else null;
    var diagnostics = Diagnostics{};
    const original = try counter.count(.{ .messages = source, .keep = keep, .rendered_memory = rendered });
    diagnostics.original_input_tokens = original;
    var effective = original;
    if (original > options.target_input_tokens and options.mode != .inspect) {
        if (options.mode == .reject) return error.AgentContextBudgetExceeded;
        var inspection = try inspectExchanges(a, source, options.keep_recent_turns);
        defer inspection.deinit();
        // Each unit is a completed turn or a completed current-turn tool
        // exchange. Pinned user questions, instructions and open calls survive.
        for (inspection.units) |unit| {
            if (effective <= options.target_input_tokens) break;
            if (unit.pinned or !unit.complete) continue;
            var selected_any = false;
            for (keep[unit.start..unit.end]) |retained| selected_any = selected_any or retained;
            if (!selected_any) continue;
            if (options.mode == .compact) {
                const run = runner orelse return error.ContextCompactionRunnerRequired;
                if (diagnostics.batches >= options.max_batches) return error.ContextCompactionBatchLimitExceeded;
                const selected = try a.alloc(bool, source.len);
                @memset(selected, false);
                @memcpy(selected[unit.start..unit.end], keep[unit.start..unit.end]);
                const input = p.CountInput{ .messages = source, .keep = selected, .rendered_memory = rendered };
                if (try summary_counter.count(input) > options.summary_input_tokens) return error.CompactionUnitTooLarge;
                const frame = try p.buildCompactionFrame(a, source, selected, rendered);
                try control.check();
                const output = try run.run_fn(run.ptr, a, frame, options.summary_output_tokens);
                try control.check();
                if (output.text.len > options.limits.max_memory_bytes) return error.InvalidContextSummary;
                const parsed = try p.parseSummary(a, output.text);
                if (parsed.unresolved_tool_calls.len != 0) return error.InvalidContextSummary;
                try p.validateSummaryEvidence(parsed, memory, source, selected);
                memory = try p.retainSmallToolResults(a, parsed, memory, source, selected, options.limits);
                rendered = try p.renderMemory(a, memory.?, options.limits);
                diagnostics.batches += 1;
                diagnostics.summary_input_tokens += output.input_tokens;
                diagnostics.summary_output_tokens += output.output_tokens;
            }
            @memset(keep[unit.start..unit.end], false);
            effective = try counter.count(.{ .messages = source, .keep = keep, .rendered_memory = rendered });
        }
        if (effective > options.target_input_tokens) return error.AgentContextBudgetExceeded;
        if (options.mode == .compact and effective >= original) return error.InsufficientContextReduction;
    }
    try control.check();
    diagnostics.effective_input_tokens = effective;
    for (keep) |retained| {
        if (retained) diagnostics.retained_messages += 1 else diagnostics.compacted_messages += 1;
    }
    return .{
        .next = .{ .source_messages = source.len, .source_digest = try p.digestPrefix(source, source.len), .keep = keep, .memory = memory },
        .rendered_memory = rendered,
        .diagnostics = diagnostics,
    };
}

/// The general conversation planner retains whole user turns. Agent loops can
/// also reduce completed exchanges within their current turn: retain the user
/// question, the most recent exchange, and any unresolved tool transaction.
fn inspectExchanges(a: std.mem.Allocator, source: []const p.MessageView, recent: usize) !p.Inspection {
    var result = try p.inspect(a, source);
    errdefer result.deinit();
    var units: std.ArrayListUnmanaged(p.Unit) = .empty;
    errdefer units.deinit(a);
    var current: usize = 0;
    for (source, 0..) |m, i| if (m.role == .user) {
        current = i;
    };
    var recent_left = recent;
    var i = result.units.len;
    while (i > 0) {
        i -= 1;
        var unit = result.units[i];
        if (unit.start >= current) continue;
        if (unit.is_turn and recent_left > 0) {
            unit.pinned = true;
            recent_left -= 1;
        }
        result.units[i] = unit;
    }
    for (result.units) |unit| {
        if (unit.start < current or source[unit.start].role == .system or source[unit.start].role == .developer) {
            try units.append(a, unit);
            continue;
        }
        var start = unit.start;
        while (start < unit.end) {
            var end = start + 1;
            const message = source[start];
            if (message.role == .assistant and message.tool_calls.len > 0) {
                while (end < unit.end and source[end].role == .tool) : (end += 1) {}
            }
            var pinned = message.role == .user or message.role == .tool;
            for (result.open_or_orphan[start..end]) |open| pinned = pinned or open;
            try units.append(a, .{ .start = start, .end = end, .pinned = pinned, .is_turn = false, .complete = !pinned });
            start = end;
        }
    }
    // Retain the latest assistant/result exchange for immediate continuation.
    if (units.items.len > 0) units.items[units.items.len - 1].pinned = true;
    a.free(result.units);
    result.units = try units.toOwnedSlice(a);
    return result;
}

const Fake = struct {
    calls: usize = 0,
    fail: bool = false,
    cancelled: bool = false,
    cancel_after_output: bool = false,
    summary: []const u8 = "{\"conversation\":\"deadline Friday; cite doc:42\",\"tool_evidence\":[{\"tool_call_id\":\"c1\",\"tool_name\":\"search\",\"facts\":\"doc:42 deadline Friday\",\"source_ids\":[\"doc:42\"]}],\"unresolved_tool_calls\":[]}",
    fn count(_: *anyopaque, input: p.CountInput) !usize {
        var total: usize = if (input.rendered_memory) |m| m.len else 0;
        for (input.messages, input.keep) |message, keep| if (keep) {
            total += message.content.len + 8;
        };
        return total;
    }
    fn summaryCount(_: *anyopaque, _: p.CountInput) !usize {
        return 100;
    }
    fn run(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: usize) !Summary {
        const self: *Fake = @ptrCast(@alignCast(raw));
        self.calls += 1;
        if (self.fail) return error.ProviderUnavailable;
        if (self.cancel_after_output) self.cancelled = true;
        return .{ .text = self.summary, .input_tokens = 100, .output_tokens = 20 };
    }
    fn check(raw: *anyopaque) !void {
        const self: *Fake = @ptrCast(@alignCast(raw));
        if (self.cancelled or (self.fail and self.calls > 0)) return error.Cancelled;
    }
    fn prepare(self: *Fake, a: std.mem.Allocator, source: []const p.MessageView, state: State, mode: Mode, target: usize) !Prepared {
        return @import("preparation.zig").prepare(a, source, state, .{ .mode = mode, .target_input_tokens = target, .keep_recent_turns = 0 }, .{ .ptr = self, .count_fn = count }, .{ .ptr = self, .count_fn = summaryCount }, .{ .ptr = self, .run_fn = run }, .{ .ptr = self, .check_fn = check });
    }
};
fn history(a: std.mem.Allocator) ![]const p.MessageView {
    const messages = try a.dupe(p.MessageView, &.{
        .{ .role = .system, .content = "Never reveal secrets" },
        .{ .role = .user, .content = "When is doc:42 due?" },
        .{ .role = .assistant, .tool_calls = &.{.{ .id = "c1", .name = "search" }} },
        .{ .role = .tool, .tool_call_id = "c1" },
        .{ .role = .assistant, .content = "Need to confirm" },
    });
    messages[3].source_ids = &.{"doc:42"};
    messages[3].content = try a.alloc(u8, 3000);
    @memset(@constCast(messages[3].content), 'x');
    return messages;
}

test "agent compaction preserves question instructions evidence and authoritative history" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const source = try history(a);
    var fake = Fake{};
    const result = try fake.prepare(a, source, .{}, .compact, 1000);
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expect(result.next.keep[0] and result.next.keep[1] and result.next.keep[4]);
    try std.testing.expect(!result.next.keep[2] and !result.next.keep[3]);
    try std.testing.expectEqual(@as(usize, 3000), source[3].content.len);
    try std.testing.expectEqualStrings("c1", result.next.memory.?.tool_evidence[0].tool_call_id);
    try std.testing.expect(std.mem.indexOf(u8, result.rendered_memory.?, "doc:42") != null);
    try std.testing.expect(result.diagnostics.effective_input_tokens <= 1000);
    // A different destination budget recounts portable memory without inference.
    const continued = try fake.prepare(a, source, result.next, .compact, 1100);
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(result.next.memory.?.conversation, continued.next.memory.?.conversation);
    const altered = try a.dupe(p.MessageView, source);
    altered[1].content = "tampered";
    try std.testing.expectError(error.CheckpointHistoryMismatch, fake.prepare(a, altered, result.next, .compact, 1100));
}

test "fitting and non-generative agent policies never invoke summary runner" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const source = try history(a);
    var fake = Fake{};
    _ = try fake.prepare(a, source, .{}, .compact, 5000);
    _ = try fake.prepare(a, source, .{}, .inspect, 1000);
    _ = try fake.prepare(a, source, .{}, .window, 1000);
    try std.testing.expectError(error.AgentContextBudgetExceeded, fake.prepare(a, source, .{}, .reject, 1000));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
}

test "malformed missing or invented evidence cannot publish agent memory" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const source = try history(a);
    for ([_][]const u8{
        "{",                                                                                                                                                       "{\"conversation\":\"ok\",\"tool_evidence\":[],\"unresolved_tool_calls\":[]}",
        "{\"conversation\":\"ok\",\"tool_evidence\":[{\"tool_call_id\":\"invented\",\"tool_name\":\"search\",\"facts\":\"claim\"}],\"unresolved_tool_calls\":[]}", "{\"conversation\":\"ok\",\"conversation\":\"duplicate\"}",
    }) |invalid| {
        var fake = Fake{ .summary = invalid };
        try std.testing.expectError(error.InvalidContextSummary, fake.prepare(a, source, .{}, .compact, 1000));
        try std.testing.expectEqual(@as(usize, 3000), source[3].content.len);
    }
}

test "provider failures cancellation insufficient reduction and open exchanges fail atomically" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const source = try history(a);
    var failed = Fake{ .fail = true };
    try std.testing.expectError(error.ProviderUnavailable, failed.prepare(a, source, .{}, .compact, 1000));
    var cancelled = Fake{ .cancelled = true };
    try std.testing.expectError(error.Cancelled, cancelled.prepare(a, source, .{}, .compact, 1000));
    try std.testing.expectEqual(@as(usize, 0), cancelled.calls);
    var fake = Fake{};
    try std.testing.expectError(error.AgentContextBudgetExceeded, fake.prepare(a, source, .{}, .compact, 100));
    const open = try a.dupe(p.MessageView, source);
    open[3].tool_call_id = "orphan";
    fake.calls = 0;
    try std.testing.expectError(error.AgentContextBudgetExceeded, fake.prepare(a, open, .{}, .compact, 1000));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
}

test "exact small tool records survive lossy summaries" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const source = try a.dupe(p.MessageView, try history(a));
    source[3].content = "{\"deadline\":\"Friday\",\"id\":\"doc:42\"}";
    source[2].content = try a.alloc(u8, 3000);
    @memset(@constCast(source[2].content), 'x');
    var fake = Fake{};
    const result = try fake.prepare(a, source, .{}, .compact, 1000);
    try std.testing.expectEqualStrings(source[3].content, result.next.memory.?.tool_evidence[0].facts);
}

test "agent summary capacity and cancellation after output cannot publish state" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const source = try history(a);
    var fake = Fake{};
    try std.testing.expectError(error.CompactionUnitTooLarge, prepare(a, source, .{}, .{ .target_input_tokens = 1000, .summary_input_tokens = 50 }, .{ .ptr = &fake, .count_fn = Fake.count }, .{ .ptr = &fake, .count_fn = Fake.summaryCount }, .{ .ptr = &fake, .run_fn = Fake.run }, .{}));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    fake.cancel_after_output = true;
    try std.testing.expectError(error.Cancelled, fake.prepare(a, source, .{}, .compact, 1000));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(usize, 3000), source[3].content.len);
}

test "repeated agent compaction preserves previous evidence and validates cumulative history" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const source = try history(a);
    var first_runner = Fake{};
    const first = try first_runner.prepare(a, source, .{}, .compact, 1000);
    // Provenance is part of authoritative history even when rendered text is unchanged.
    const altered = try a.dupe(p.MessageView, source);
    for (altered) |*message| if (message.source_ids != null) {
        message.source_ids = &.{"doc:42-alias"};
        break;
    };
    try std.testing.expectError(error.CheckpointHistoryMismatch, first_runner.prepare(a, altered, first.next, .compact, 1000));
    const more = try a.alloc(u8, 3000);
    @memset(more, 'y');
    const continued = try std.mem.concat(a, p.MessageView, &.{ source, &.{
        .{ .role = .assistant, .tool_calls = &.{.{ .id = "c2", .name = "fetch" }} },
        .{ .role = .tool, .tool_call_id = "c2", .content = more, .source_ids = &.{"doc:43"} },
        .{ .role = .assistant, .content = "current" },
    } });
    const Repeat = struct {
        fn run(_: *anyopaque, _: std.mem.Allocator, frame: []const u8, _: usize) !Summary {
            return .{ .text = if (std.mem.indexOf(u8, frame, "\"id\":\"c2\"") != null)
                "{\"conversation\":\"doc:42 deadline Friday; fetched confirmation\",\"tool_evidence\":[{\"tool_call_id\":\"c1\",\"tool_name\":\"search\",\"facts\":\"doc:42 deadline Friday\",\"source_ids\":[\"doc:42\"]},{\"tool_call_id\":\"c2\",\"tool_name\":\"fetch\",\"facts\":\"confirmed Friday\",\"source_ids\":[\"doc:43\"]}],\"unresolved_tool_calls\":[]}"
            else
                (Fake{}).summary };
        }
    };
    var fake: u8 = 0;
    const second = try prepare(a, continued, first.next, .{ .target_input_tokens = 1200, .keep_recent_turns = 0 }, .{ .ptr = &fake, .count_fn = Fake.count }, .{ .ptr = &fake, .count_fn = Fake.summaryCount }, .{ .ptr = &fake, .run_fn = Repeat.run }, .{});
    try std.testing.expectEqual(@as(usize, 2), second.next.memory.?.tool_evidence.len);
    try std.testing.expectEqualStrings("c1", second.next.memory.?.tool_evidence[0].tool_call_id);
    try std.testing.expectEqualStrings("c2", second.next.memory.?.tool_evidence[1].tool_call_id);
    try std.testing.expectEqual(@as(usize, 1), first.next.memory.?.tool_evidence.len);
    try std.testing.expect(second.diagnostics.effective_input_tokens <= 1200);
}
