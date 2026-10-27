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

//! Normalize provider responses once, independently of completion policy.
//! Retain measured counters without estimates and copy only returned text.
const std = @import("std");
const readers = @import("mod.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
fn field(value: V, key: []const u8) ?V {
    return if (value == .object) value.object.get(key) else null;
}
fn parse(comptime T: type, a: A, bytes: []const u8) !std.json.Parsed(T) {
    return std.json.parseFromSlice(T, a, bytes, .{ .ignore_unknown_fields = true, .duplicate_field_behavior = .@"error", .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidReadResultCount,
    };
}
const Usage = struct { prompt: usize, completion: usize, total: usize };
fn count(value: V) !usize {
    return switch (value) {
        .integer => std.math.cast(usize, value.integer) orelse error.InvalidReadExecutionReport,
        .number_string => std.fmt.parseUnsigned(usize, value.number_string, 10) catch error.InvalidReadExecutionReport,
        else => error.InvalidReadExecutionReport,
    };
}
fn optionalCount(value: V, name: []const u8) !usize {
    return if (field(value, name)) |n| try count(n) else 0;
}
fn add(left: usize, right: usize) !usize {
    return std.math.add(usize, left, right) catch error.InvalidReadExecutionReport;
}
fn usage(value: ?V, comptime vertex_format: bool) !?Usage {
    const root = value orelse return null;
    if (root == .null) return null;
    if (root != .object) return error.InvalidReadExecutionReport;
    if (vertex_format and field(root, "promptTokenCount") == null and field(root, "candidatesTokenCount") == null and field(root, "totalTokenCount") == null and field(root, "thoughtsTokenCount") == null and field(root, "toolUsePromptTokenCount") == null) return error.InvalidReadExecutionReport;
    const prompt = if (vertex_format)
        try add(try optionalCount(root, "promptTokenCount"), try optionalCount(root, "toolUsePromptTokenCount"))
    else
        try count(field(root, "prompt_tokens") orelse return error.InvalidReadExecutionReport);
    const completion = if (vertex_format)
        try add(try optionalCount(root, "candidatesTokenCount"), try optionalCount(root, "thoughtsTokenCount"))
    else
        try count(field(root, "completion_tokens") orelse return error.InvalidReadExecutionReport);
    const total = try add(prompt, completion);
    if (field(root, if (vertex_format) "totalTokenCount" else "total_tokens")) |n| if (try count(n) != total) return error.InvalidReadExecutionReport;
    if (std.math.cast(i64, prompt) == null or std.math.cast(i64, completion) == null or std.math.cast(i64, total) == null) return error.InvalidReadExecutionReport;
    return .{ .prompt = prompt, .completion = completion, .total = total };
}
fn report(a: A, text: []const u8, completion: readers.Completion, counts: ?Usage) !readers.BatchResult {
    const items = try a.alloc(readers.Result, 1);
    errdefer a.free(items);
    items[0] = .{ .text = text, .completion = completion, .prompt_tokens = if (counts) |n| n.prompt else null, .completion_tokens = if (counts) |n| n.completion else null };
    return .{
        .items = items,
        .execution = .{ .requested_items = 1, .serial_items = 1 },
        .usage = if (counts) |n| .{ .prompt_tokens = @intCast(n.prompt), .completion_tokens = @intCast(n.completion), .total_tokens = @intCast(n.total) } else null,
    };
}

/// OpenAI-compatible vision must supply one text choice with an explicit stop.
/// Missing or unfamiliar finish reasons remain unresolved, never complete.
pub fn chat(a: A, bytes: []const u8) !readers.BatchResult {
    const Response = struct {
        choices: []const struct {
            message: ?struct { content: ?[]const u8 = null, refusal: ?[]const u8 = null, tool_calls: ?[]const V = null } = null,
            finish_reason: ?[]const u8 = null,
        } = &.{},
        usage: ?V = null,
    };
    var parsed = try parse(Response, a, bytes);
    defer parsed.deinit();
    if (parsed.value.choices.len != 1) return error.InvalidReadResultCount;
    const choice = parsed.value.choices[0];
    const message = choice.message orelse return error.InvalidReadResultCount;
    var completion: readers.Completion = .unresolved;
    if (choice.finish_reason) |reason| {
        if (std.mem.eql(u8, reason, "stop")) completion = .complete;
        if (std.mem.eql(u8, reason, "length")) completion = .length;
    }
    if ((message.refusal != null and message.refusal.?.len > 0) or (message.tool_calls != null and message.tool_calls.?.len > 0)) completion = .unresolved;
    if (completion == .complete and message.content == null) return error.EmptyResponse;
    const counts = try usage(parsed.value.usage, false);
    const text = try a.dupe(u8, message.content orelse "");
    errdefer a.free(text);
    return report(a, text, completion, counts);
}

/// Vertex returns ordered text parts; thought/tool parts cannot become OCR.
/// Usage categories follow GenerateContentResponse.UsageMetadata:
/// https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/GenerateContentResponse
pub fn vertex(a: A, bytes: []const u8) !readers.BatchResult {
    const Response = struct {
        candidates: []const struct {
            content: ?struct { parts: []const struct { text: ?[]const u8 = null, thought: bool = false } = &.{} } = null,
            finishReason: ?[]const u8 = null,
        } = &.{},
        usageMetadata: ?V = null,
    };
    var parsed = try parse(Response, a, bytes);
    defer parsed.deinit();
    if (parsed.value.candidates.len != 1) return error.InvalidReadResultCount;
    const candidate = parsed.value.candidates[0];
    var completion: readers.Completion = .unresolved;
    if (candidate.finishReason) |reason| {
        if (std.mem.eql(u8, reason, "STOP")) completion = .complete;
        if (std.mem.eql(u8, reason, "MAX_TOKENS")) completion = .length;
    }
    var length: usize = 0;
    var visible = false;
    if (candidate.content) |content| for (content.parts) |part| {
        if (part.thought) continue;
        if (part.text) |value| {
            visible = true;
            length = std.math.add(usize, length, value.len) catch return error.InvalidReadResultCount;
        } else completion = .unresolved;
    };
    if (completion == .complete and !visible) return error.EmptyResponse;
    const counts = try usage(parsed.value.usageMetadata, true);
    const owned = try a.alloc(u8, length);
    errdefer a.free(owned);
    var cursor: usize = 0;
    if (candidate.content) |content| for (content.parts) |part| {
        if (!part.thought) if (part.text) |value| {
            @memcpy(owned[cursor..][0..value.len], value);
            cursor += value.len;
        };
    };
    return report(a, owned, completion, counts);
}

test "hosted OCR chat completion requires explicit stop and rejects refusal tool and unknown endings" {
    const a = std.testing.allocator;
    const cases = [_]struct { json: []const u8, completion: readers.Completion }{
        .{ .json = "{\"choices\":[{\"message\":{\"content\":\"text\"},\"finish_reason\":\"stop\"}]}", .completion = .complete },
        .{ .json = "{\"choices\":[{\"message\":{\"content\":\"partial\"},\"finish_reason\":\"length\"}]}", .completion = .length },
        .{ .json = "{\"choices\":[{\"message\":{\"content\":\"text\"}}]}", .completion = .unresolved },
        .{ .json = "{\"choices\":[{\"message\":{\"content\":\"text\"},\"finish_reason\":null}]}", .completion = .unresolved },
        .{ .json = "{\"choices\":[{\"message\":{\"content\":null},\"finish_reason\":\"content_filter\"}]}", .completion = .unresolved },
        .{ .json = "{\"choices\":[{\"message\":{\"content\":\"text\"},\"finish_reason\":\"future_reason\"}]}", .completion = .unresolved },
        .{ .json = "{\"choices\":[{\"message\":{\"content\":\"text\",\"refusal\":\"refused\"},\"finish_reason\":\"stop\"}]}", .completion = .unresolved },
        .{ .json = "{\"choices\":[{\"message\":{\"content\":\"text\",\"tool_calls\":[{}]},\"finish_reason\":\"stop\"}]}", .completion = .unresolved },
    };
    for (cases) |case| {
        var result = try chat(a, case.json);
        defer result.deinit(a);
        try std.testing.expectEqual(case.completion, result.items[0].completion);
        if (case.completion == .complete) try readers.requireComplete(result.items) else if (case.completion == .length)
            try std.testing.expectError(error.ReadOutputTruncated, readers.requireComplete(result.items))
        else
            try std.testing.expectError(error.DocumentUnresolved, readers.requireComplete(result.items));
        try std.testing.expect(result.usage == null);
    }
}

test "hosted OCR usage preserves exact measured counts and distinguishes absent from zero" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "{\"prompt_tokens\":7,\"completion_tokens\":2,\"total_tokens\":9}",
        "{\"prompt_tokens\":7,\"completion_tokens\":2}",
        "{\"prompt_tokens\":0,\"completion_tokens\":0,\"total_tokens\":0}",
    }, [_]usize{ 9, 9, 0 }) |counters, total| {
        const bytes = try std.fmt.allocPrint(a, "{{\"choices\":[{{\"message\":{{\"content\":\"text\"}},\"finish_reason\":\"stop\"}}],\"usage\":{s}}}", .{counters});
        defer a.free(bytes);
        var result = try chat(a, bytes);
        defer result.deinit(a);
        try std.testing.expectEqual(@as(i64, @intCast(total)), result.usage.?.total_tokens);
        try std.testing.expect(result.items[0].prompt_tokens != null and result.items[0].completion_tokens != null);
        try std.testing.expectEqualDeep(result.usage.?, (try readers.measuredUsage(result.items)).?);
    }
    for ([_][]const u8{
        "{}",                                                              "[]",                                              "{\"prompt_tokens\":1}",                                            "{\"prompt_tokens\":-1,\"completion_tokens\":0}",
        "{\"prompt_tokens\":1.5,\"completion_tokens\":0}",                 "{\"prompt_tokens\":1e1,\"completion_tokens\":0}", "{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":2}", "{\"prompt_tokens\":18446744073709551615,\"completion_tokens\":1}",
        "{\"prompt_tokens\":9223372036854775807,\"completion_tokens\":1}",
    }) |counters| {
        const bytes = try std.fmt.allocPrint(a, "{{\"choices\":[{{\"message\":{{\"content\":\"text\"}},\"finish_reason\":\"stop\"}}],\"usage\":{s}}}", .{counters});
        defer a.free(bytes);
        try std.testing.expectError(error.InvalidReadExecutionReport, chat(a, bytes));
    }
}

test "hosted OCR response validation rejects malformed or ambiguous choices" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "{}",                                                                       "[]",                                                                       "{\"choices\":[]}",                "{\"choices\":[{},{}]}", "{\"choices\":[{}]}",
        "{\"choices\":[{\"message\":{\"content\":1},\"finish_reason\":\"stop\"}]}", "{\"choices\":[{\"message\":{\"content\":\"text\"},\"finish_reason\":1}]}", "{\"choices\":[],\"choices\":[]}",
    }) |bytes| try std.testing.expectError(error.InvalidReadResultCount, chat(a, bytes));
    try std.testing.expectError(error.EmptyResponse, chat(a, "{\"choices\":[{\"message\":{},\"finish_reason\":\"stop\"}]}"));
}

test "hosted OCR Vertex stitches visible parts and counts prompt tool and thought usage" {
    const a = std.testing.allocator;
    var result = try vertex(a,
        \\{"candidates":[{"content":{"parts":[{"text":"private reasoning","thought":true},{"text":"Receipt "},{"text":"12.50"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":7,"toolUsePromptTokenCount":3,"candidatesTokenCount":2,"thoughtsTokenCount":4,"totalTokenCount":16}}
    );
    defer result.deinit(a);
    try readers.requireComplete(result.items);
    try std.testing.expectEqualStrings("Receipt 12.50", result.items[0].text);
    try std.testing.expectEqual(@as(i64, 10), result.usage.?.prompt_tokens);
    try std.testing.expectEqual(@as(i64, 6), result.usage.?.completion_tokens);
    try std.testing.expectEqual(@as(i64, 16), result.usage.?.total_tokens);
    try std.testing.expectEqualDeep(result.usage.?, (try readers.measuredUsage(result.items)).?);
    for ([_][]const u8{ "MAX_TOKENS", "SAFETY", "FINISH_REASON_UNSPECIFIED", "future_reason" }) |reason| {
        const bytes = try std.fmt.allocPrint(a, "{{\"candidates\":[{{\"content\":{{\"parts\":[{{\"text\":\"partial\"}}]}},\"finishReason\":\"{s}\"}}]}}", .{reason});
        defer a.free(bytes);
        var incomplete = try vertex(a, bytes);
        defer incomplete.deinit(a);
        try std.testing.expect(incomplete.items[0].completion != .complete);
    }
    var tool = try vertex(a, "{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{}}]},\"finishReason\":\"STOP\"}]}");
    defer tool.deinit(a);
    try std.testing.expectEqual(readers.Completion.unresolved, tool.items[0].completion);
    try std.testing.expectError(error.EmptyResponse, vertex(a, "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"private\",\"thought\":true}]},\"finishReason\":\"STOP\"}]}"));
}

test "hosted OCR decoders release every partial allocation for complete and incomplete responses" {
    const Runner = struct {
        fn run(a: A) !void {
            var first = try chat(a, "{\"choices\":[{\"message\":{\"content\":\"Receipt\"},\"finish_reason\":\"length\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5}}");
            defer first.deinit(a);
            var second = try vertex(a, "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Receipt \"},{\"text\":\"12.50\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}");
            defer second.deinit(a);
            try readers.requireComplete(second.items);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "hosted OCR measured counts retain integers beyond floating point precision" {
    if (@bitSizeOf(usize) < 64) return error.SkipZigTest;
    const a = std.testing.allocator;
    var result = try chat(a,
        \\{"choices":[{"message":{"content":"text"},"finish_reason":"stop"}],"usage":{"prompt_tokens":9007199254740993,"completion_tokens":2,"total_tokens":9007199254740995}}
    );
    defer result.deinit(a);
    try std.testing.expectEqual(@as(i64, 9007199254740993), result.usage.?.prompt_tokens);
    try std.testing.expectEqual(@as(i64, 9007199254740995), result.usage.?.total_tokens);
}

test "hosted OCR Vertex refuses malformed counters and unknown completion metadata" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "{}", "{\"promptTokenCount\":-1}", "{\"promptTokenCount\":1.5}", "{\"promptTokenCount\":7,\"candidatesTokenCount\":2,\"totalTokenCount\":10}" }) |counters| {
        const bytes = try std.fmt.allocPrint(a, "{{\"candidates\":[{{\"content\":{{\"parts\":[{{\"text\":\"text\"}}]}},\"finishReason\":\"STOP\"}}],\"usageMetadata\":{s}}}", .{counters});
        defer a.free(bytes);
        try std.testing.expectError(error.InvalidReadExecutionReport, vertex(a, bytes));
    }
    var missing = try vertex(a, "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"text\"}]}}]}");
    defer missing.deinit(a);
    try std.testing.expectEqual(readers.Completion.unresolved, missing.items[0].completion);
    try std.testing.expect(missing.usage == null);
    var zero = try vertex(a, "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":0,\"candidatesTokenCount\":0,\"totalTokenCount\":0}}");
    defer zero.deinit(a);
    try readers.requireComplete(zero.items);
    try std.testing.expectEqual(@as(i64, 0), zero.usage.?.total_tokens);
}
