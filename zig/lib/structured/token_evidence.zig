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

//! Map actual decoder token distributions to finite structured values. A
//! multi-token alternative is not guessed from its first token: absent exact
//! counterfactual token evidence fails closed.
const std = @import("std");
const constrained = @import("root.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
const Span = struct {
    start: usize,
    end: usize,
    fields: std.StringHashMapUnmanaged(*Span) = .empty,
    items: std.ArrayList(*Span) = .empty,
};

pub fn distribution(a: A, proof: constrained.TokenEvidence, item: usize, path: []const []const u8, values: []const V) ![]f64 {
    return proof.distribution(a, item, path, values);
}
const Alternative = struct { bytes: []const u8, logprob: f64 };
const Token = struct { start: usize, end: usize, bytes: []const u8, alternatives: []const Alternative };
fn tokenBytes(a: A, value: V) ![]const u8 {
    if (value != .object) return error.InvalidConstrainedProbabilities;
    if (value.object.get("bytes")) |raw| if (raw != .null) {
        if (raw != .array or raw.array.items.len > 4096) return error.InvalidConstrainedProbabilities;
        const bytes = try a.alloc(u8, raw.array.items.len);
        for (raw.array.items, bytes) |entry, *byte| {
            if (entry != .integer) return error.InvalidConstrainedProbabilities;
            byte.* = std.math.cast(u8, entry.integer) orelse return error.InvalidConstrainedProbabilities;
        }
        return bytes;
    };
    const token = value.object.get("token") orelse return error.InvalidConstrainedProbabilities;
    if (token != .string or token.string.len > 4096) return error.InvalidConstrainedProbabilities;
    return token.string;
}
fn logprob(value: V) !f64 {
    const raw = value.object.get("logprob") orelse return error.InvalidConstrainedProbabilities;
    const result: f64 = switch (raw) {
        .float => raw.float,
        .integer => @floatFromInt(raw.integer),
        else => return error.InvalidConstrainedProbabilities,
    };
    if (!std.math.isFinite(result)) return error.InvalidConstrainedProbabilities;
    return result;
}
/// The index belongs to the request arena. Parsing and positional attribution
/// happen once per physical response, independent of mention/question count.
pub const Index = struct {
    text: []const u8,
    items: std.AutoHashMapUnmanaged(usize, ?*Span) = .empty,
    tokens: []Token,
    pub fn init(a: A, raw: []const u8, logprobs: V) !*Index {
        if (logprobs != .object) return error.InvalidConstrainedProbabilities;
        const content = logprobs.object.get("content") orelse return error.InvalidConstrainedProbabilities;
        if (content != .array or content.array.items.len > 65536) return error.InvalidConstrainedProbabilities;
        const self = try a.create(Index);
        self.* = .{ .text = try a.dupe(u8, raw), .tokens = &.{} };
        var scanner = std.json.Scanner.initCompleteInput(a, self.text);
        defer scanner.deinit();
        const root = try parseSpan(a, &scanner, self.text, 0);
        const rows = root.fields.get("items") orelse return error.InvalidConstrainedProbabilities;
        for (rows.items.items) |row| {
            const id_span = row.fields.get("item") orelse continue;
            const id = std.fmt.parseInt(usize, self.text[id_span.start..id_span.end], 10) catch continue;
            if (self.items.contains(id)) try self.items.put(a, id, null) else try self.items.put(a, id, row.fields.get("value"));
        }
        var tokens: std.ArrayList(Token) = .empty;
        var offset: usize = 0;
        for (content.array.items) |entry| {
            if (offset == self.text.len) break; // EOS is outside the JSON answer.
            if (entry != .object) return error.InvalidConstrainedProbabilities;
            const bytes = try tokenBytes(a, entry);
            if (bytes.len > self.text.len - offset or !std.mem.eql(u8, bytes, self.text[offset .. offset + bytes.len])) return error.InvalidConstrainedProbabilities;
            if (bytes.len == 0) continue;
            const top = entry.object.get("top_logprobs");
            if (top) |value| if (value != .array or value.array.items.len > 64) return error.InvalidConstrainedProbabilities;
            const alternatives = try a.alloc(Alternative, if (top) |value| value.array.items.len else 0);
            if (top) |value| for (alternatives, value.array.items) |*alternative, candidate| {
                alternative.* = .{ .bytes = try tokenBytes(a, candidate), .logprob = try logprob(candidate) };
            };
            try tokens.append(a, .{ .start = offset, .end = offset + bytes.len, .bytes = self.text[offset .. offset + bytes.len], .alternatives = alternatives });
            offset += bytes.len;
        }
        if (offset != self.text.len) return error.InvalidConstrainedProbabilities;
        self.tokens = try tokens.toOwnedSlice(a);
        return self;
    }
    pub fn evidence(self: *const Index) constrained.TokenEvidence {
        return .{ .ptr = self, .get_distribution = get };
    }
    fn get(ptr: *const anyopaque, a: A, item: usize, path: []const []const u8, alternatives: []const V) ![]f64 {
        const self: *const Index = @ptrCast(@alignCast(ptr));
        var span = (self.items.get(item) orelse return error.InvalidConstrainedProbabilities) orelse return error.InvalidConstrainedProbabilities;
        for (path) |part| {
            if (span.fields.get(part)) |field| span = field else {
                const index = std.fmt.parseInt(usize, part, 10) catch return error.InvalidConstrainedProbabilities;
                if (index >= span.items.items.len) return error.InvalidConstrainedProbabilities;
                span = span.items.items[index];
            }
        }
        const quoted = self.text[span.start] == '"';
        const start = span.start + @as(usize, if (quoted) 1 else 0);
        const end = span.end - @as(usize, if (quoted) 1 else 0);
        if (start >= end) return error.InvalidConstrainedProbabilities;
        var low: usize = 0;
        var high = self.tokens.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (self.tokens[mid].end < end) low = mid + 1 else high = mid;
        }
        if (low == self.tokens.len) return error.InvalidConstrainedProbabilities;
        const token = self.tokens[low];
        if (token.start > start or token.end < end) return error.InvalidConstrainedProbabilities;
        const values = try a.alloc(f64, alternatives.len);
        for (alternatives, values) |alternative, *value| {
            const encoded = try std.json.Stringify.valueAlloc(a, alternative, .{});
            if ((alternative == .string) != quoted) return error.InvalidConstrainedProbabilities;
            const inner = if (quoted) encoded[1 .. encoded.len - 1] else encoded;
            const expected = try std.mem.concat(a, u8, &.{ token.bytes[0 .. start - token.start], inner, token.bytes[end - token.start ..] });
            var found = false;
            for (token.alternatives) |candidate| {
                if (!std.mem.eql(u8, candidate.bytes, expected)) continue;
                if (found) return error.InvalidConstrainedProbabilities;
                found = true;
                value.* = candidate.logprob;
            }
            if (!found) return error.InvalidConstrainedProbabilities;
        }
        return constrained.normalizeLogprobs(a, values);
    }
};

fn parseSpan(a: A, scanner: *std.json.Scanner, text: []const u8, depth: usize) anyerror!*Span {
    if (depth > 32) return error.InvalidConstrainedProbabilities;
    var start = scanner.cursor;
    const token = try scanner.nextAlloc(a, .alloc_always);
    while (start < text.len and std.mem.indexOfScalar(u8, " \t\r\n,:", text[start]) != null) start += 1;
    return parseSpanToken(a, scanner, text, depth, token, start);
}
fn parseSpanToken(a: A, scanner: *std.json.Scanner, text: []const u8, depth: usize, token: std.json.Token, start: usize) anyerror!*Span {
    const span = try a.create(Span);
    span.* = .{ .start = start, .end = scanner.cursor };
    switch (token) {
        .object_begin => {
            while (true) {
                const key = try scanner.nextAlloc(a, .alloc_always);
                if (key == .object_end) break;
                const name = switch (key) {
                    .string, .allocated_string => |name| name,
                    else => return error.InvalidConstrainedProbabilities,
                };
                const child = try parseSpan(a, scanner, text, depth + 1);
                if (span.fields.contains(name)) return error.InvalidConstrainedProbabilities;
                try span.fields.put(a, name, child);
            }
            span.end = scanner.cursor;
        },
        .array_begin => {
            while (true) {
                var child_start = scanner.cursor;
                const next = try scanner.nextAlloc(a, .alloc_always);
                if (next == .array_end) break;
                while (child_start < text.len and std.mem.indexOfScalar(u8, " \t\r\n,:", text[child_start]) != null) child_start += 1;
                if (depth > 32) return error.InvalidConstrainedProbabilities;
                try span.items.append(a, try parseSpanToken(a, scanner, text, depth + 1, next, child_start));
            }
            span.end = scanner.cursor;
        },
        .string, .allocated_string, .number, .allocated_number, .true, .false, .null => {},
        else => return error.InvalidConstrainedProbabilities,
    }
    return span;
}

test "asset producer runtime constrained token evidence binds the correct batched item" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const raw = try std.json.parseFromSliceLeaky(V, a,
        \\{"text":"{\"items\":[{\"item\":0,\"value\":{\"relation\":\"same\"}}]}","logprobs":{"content":[{"token":"{\"items\":[{\"item\":0,\"value\":{\"relation\":\""},{"token":"same","top_logprobs":[{"token":"same","logprob":-0.1},{"token":"different","logprob":-2.4}]},{"token":"\"}}]}"}]}}
    , .{});
    const index = try Index.init(a, raw.object.get("text").?.string, raw.object.get("logprobs").?);
    const probabilities = try distribution(a, index.evidence(), 0, &.{"relation"}, &.{ .{ .string = "same" }, .{ .string = "different" } });
    try std.testing.expect(probabilities[0] > 0.9);
    try std.testing.expectApproxEqAbs(@as(f64, 1), probabilities[0] + probabilities[1], 1e-9);
    try std.testing.expectError(error.InvalidConstrainedProbabilities, distribution(a, index.evidence(), 1, &.{"relation"}, &.{ .{ .string = "same" }, .{ .string = "different" } }));
}

test "constrained token evidence uses byte witnesses rather than lossy UTF8 display tokens" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const raw = "{\"items\":[{\"item\":0,\"value\":{\"label\":\"é\"}}]}";
    const proof = try std.json.parseFromSliceLeaky(V, a,
        \\{"content":[{"token":"{\"items\":[{\"item\":0,\"value\":{\"label\":\""},{"token":"�","bytes":[195,169],"top_logprobs":[{"token":"�","bytes":[195,169],"logprob":-0.1},{"token":"�","bytes":[195,168],"logprob":-2.3}]},{"token":"\"}}]}"}]}
    , .{});
    const index = try Index.init(a, raw, proof);
    const measured = try index.evidence().distribution(a, 0, &.{"label"}, &.{ .{ .string = "é" }, .{ .string = "è" } });
    try std.testing.expect(measured[0] > 0.89);
    try std.testing.expectApproxEqAbs(@as(f64, 1), measured[0] + measured[1], 1e-12);
}
