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

//! Bounded token evidence from actual grammar-masked decoder logits. The
//! collector owns token bytes because tokenizer scratch ends before serving
//! serializes the response. It never asks a model to report confidence.
const std = @import("std");
const TokenByteTable = @import("grammar.zig").TokenByteTable;
const A = std.mem.Allocator;
pub const Observer = struct {
    ptr: *anyopaque,
    record: *const fn (*anyopaque, *const TokenByteTable, []const f32, i32) anyerror!void,
};
fn writeToken(token: []const u8, logprob: f64, writer: anytype) !void {
    try writer.objectField("token");
    // Token boundaries may split UTF-8. The display string is advisory; bytes
    // carry the exact decoder witness for both selected and alternate tokens.
    try writer.write(if (std.unicode.utf8ValidateSlice(token)) token else "\xef\xbf\xbd");
    try writer.objectField("bytes");
    try writer.beginArray();
    for (token) |byte| try writer.write(byte);
    try writer.endArray();
    try writer.objectField("logprob");
    try writer.write(logprob);
}
pub const Alternative = struct {
    token: []const u8,
    logprob: f64,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        try writeToken(self.token, self.logprob, writer);
        try writer.endObject();
    }
};
pub const Entry = struct {
    token: []const u8,
    logprob: f64,
    top_logprobs: []Alternative,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        try writeToken(self.token, self.logprob, writer);
        try writer.objectField("top_logprobs");
        try writer.write(self.top_logprobs);
        try writer.endObject();
    }
};
pub const Collector = struct {
    allocator: A,
    entries: std.ArrayList(Entry) = .empty,
    max_tokens: usize,
    max_alternatives: usize,
    max_bytes: usize = 8 * 1024 * 1024,
    bytes: usize = 0,
    pub fn observer(self: *Collector) Observer {
        return .{ .ptr = self, .record = record };
    }
    pub fn deinit(self: *Collector) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.token);
            for (entry.top_logprobs) |alternative| self.allocator.free(alternative.token);
            self.allocator.free(entry.top_logprobs);
        }
        self.entries.deinit(self.allocator);
    }
    fn copy(self: *Collector, value: []const u8) ![]u8 {
        if (value.len > self.max_bytes -| self.bytes) return error.TokenEvidenceLimitExceeded;
        self.bytes += value.len;
        return self.allocator.dupe(u8, value);
    }
    fn record(ptr: *anyopaque, table: *const TokenByteTable, logits: []const f32, selected: i32) !void {
        const self: *Collector = @ptrCast(@alignCast(ptr));
        if (self.max_alternatives == 0 or self.max_alternatives > 64 or self.entries.items.len >= self.max_tokens) return error.TokenEvidenceLimitExceeded;
        if (selected < 0 or selected >= logits.len) return error.InvalidTokenEvidence;
        const Candidate = struct { id: usize, logit: f64 };
        var top: [64]Candidate = undefined;
        var count: usize = 0;
        var allowed_count: usize = 0;
        var maximum: f64 = -std.math.inf(f64);
        for (logits, 0..) |raw, id| {
            if (std.math.isNan(raw) or raw == std.math.inf(f32)) return error.InvalidTokenEvidence;
            if (!std.math.isFinite(raw)) continue;
            const value: f64 = raw;
            allowed_count += 1;
            maximum = @max(maximum, value);
            // Only complete finite distributions are retained. Once more
            // than k tokens are allowed, a partial top-k cannot prove a value
            // distribution, so avoid heap work for free-form vocabulary.
            if (count < self.max_alternatives) {
                top[count] = .{ .id = id, .logit = value };
                count += 1;
            }
        }
        if (!std.math.isFinite(maximum) or !std.math.isFinite(logits[@intCast(selected)])) return error.InvalidTokenEvidence;
        var total: f64 = 0;
        for (logits) |value| if (std.math.isFinite(value)) {
            total += @exp(@as(f64, value) - maximum);
        };
        const log_normalizer = maximum + @log(total);
        // A free-form field can admit the entire vocabulary. Preserve its
        // chosen token for byte alignment, but never retain a partial finite
        // distribution that a consumer could mistake for complete evidence.
        if (allowed_count > self.max_alternatives) count = 0;
        const alternatives = try self.allocator.alloc(Alternative, count);
        var initialized: usize = 0;
        errdefer {
            for (alternatives[0..initialized]) |alternative| self.allocator.free(alternative.token);
            self.allocator.free(alternatives);
        }
        if (count * @sizeOf(Alternative) + @sizeOf(Entry) > self.max_bytes -| self.bytes) return error.TokenEvidenceLimitExceeded;
        self.bytes += count * @sizeOf(Alternative) + @sizeOf(Entry);
        for (top[0..count], alternatives) |candidate, *alternative| {
            alternative.* = .{ .token = try self.copy(table.getTokenBytes(candidate.id)), .logprob = candidate.logit - log_normalizer };
            initialized += 1;
        }
        const token = try self.copy(table.getTokenBytes(@intCast(selected)));
        errdefer self.allocator.free(token);
        try self.entries.append(self.allocator, .{ .token = token, .logprob = @as(f64, logits[@intCast(selected)]) - log_normalizer, .top_logprobs = alternatives });
    }
};

test "grammar token evidence records real normalized logits and enforces output bounds" {
    const a = std.testing.allocator;
    const bytes = "sameother";
    const offsets = [_]u32{ 0, 4 };
    const lengths = [_]u16{ 4, 5 };
    const table = TokenByteTable{ .byte_data = @constCast(bytes), .token_offsets = @constCast(&offsets), .token_lengths = @constCast(&lengths) };
    var collector = Collector{ .allocator = a, .max_tokens = 1, .max_alternatives = 2 };
    defer collector.deinit();
    const observer = collector.observer();
    try observer.record(observer.ptr, &table, &.{ 3, 1 }, 0);
    try std.testing.expectEqualStrings("same", collector.entries.items[0].token);
    try std.testing.expectApproxEqAbs(@as(f64, 0.8807970779778823), @exp(collector.entries.items[0].logprob), 1e-8);
    try std.testing.expectError(error.TokenEvidenceLimitExceeded, observer.record(observer.ptr, &table, &.{ 3, 1 }, 0));
}

test "grammar token evidence serializes split UTF8 tokens as exact bytes" {
    const a = std.testing.allocator;
    const encoded = try std.json.Stringify.valueAlloc(a, Entry{ .token = &.{0xc3}, .logprob = -0.25, .top_logprobs = &.{} }, .{});
    defer a.free(encoded);
    try std.testing.expect(std.unicode.utf8ValidateSlice(encoded));
    var parsed = try std.json.parseFromSlice(std.json.Value, a, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 195), parsed.value.object.get("bytes").?.array.items[0].integer);
}
