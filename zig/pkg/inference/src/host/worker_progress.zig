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

//! Advisory worker telemetry. A report is not a native-team stop boundary.
//! Never park the team's execution loan to publish it. One report per request
//! may enter the transport at a time, at most ten times per native second.
const std = @import("std");

pub const interval_ns = 100 * std.time.ns_per_ms;
pub const max_model_bytes = 256;
pub const max_backend_bytes = 32;
// JSON escaping needs at most six bytes per input byte. Leave room for every
// scalar at its maximum width; no allocator or caller-sized scratch is used.
pub const encoded_capacity = 2048;

pub const Gate = struct {
    busy: std.atomic.Value(bool) = .init(false),
    last_ns: ?u64 = null,

    pub fn enter(self: *Gate, now_ns: u64) bool {
        // A reporting worker never waits for another reporting worker, even
        // while its pipe offer is backpressured. No retry loop or task spawn.
        if (self.busy.swap(true, .acquire)) return false;
        if (self.last_ns) |last| if (now_ns -| last < interval_ns) {
            self.leave();
            return false;
        };
        self.last_ns = now_ns;
        return true;
    }

    pub fn leave(self: *Gate) void {
        self.busy.store(false, .release);
    }
};

pub const Update = struct {
    kind: enum { progress } = .progress,
    phase: u8,
    completed: u64,
    total: u64,
    model: []const u8,
    backend: []const u8,
};

fn label(value: []const u8, max_bytes: usize) []const u8 {
    var end = @min(value.len, max_bytes);
    // Do not split a UTF-8 codepoint at the byte ceiling. Inspect only the
    // bounded prefix; invalid input is advisory and can be omitted entirely.
    if (end < value.len) while (end > 0 and value[end] & 0xc0 == 0x80) {
        end -= 1;
    };
    const prefix = value[0..end];
    return if (std.unicode.utf8ValidateSlice(prefix)) prefix else "";
}

pub fn encode(storage: *[encoded_capacity]u8, update: Update) ![]const u8 {
    var value = update;
    value.model = label(value.model, max_model_bytes);
    value.backend = label(value.backend, max_backend_bytes);
    var writer = std.Io.Writer.fixed(storage);
    try std.json.Stringify.value(value, .{}, &writer);
    return writer.buffered();
}

test "inference worker progress gate bounds frequency without waiting for an active reporter" {
    var gate: Gate = .{};
    try std.testing.expect(gate.enter(0));
    // A later clock cannot admit a second reporter while the first is active.
    try std.testing.expect(!gate.enter(10 * interval_ns));
    gate.leave();
    try std.testing.expect(!gate.enter(interval_ns - 1));
    try std.testing.expect(gate.enter(interval_ns));
    gate.leave();
    // Separate requests have independent telemetry capacity.
    var other: Gate = .{};
    try std.testing.expect(other.enter(0));
    other.leave();
    try std.testing.expect(!gate.enter(0));
    try std.testing.expect(gate.enter(2 * interval_ns));
    gate.leave();
    try std.testing.expect(gate.enter(std.math.maxInt(u64)));
    gate.leave();
    try std.testing.expect(!gate.enter(std.math.maxInt(u64)));
}

test "inference worker progress encoding fits maximum escaped labels and integer widths" {
    const model: [max_model_bytes * 4]u8 = @splat(0x01);
    const backend: [max_backend_bytes * 4]u8 = @splat(0x02);
    var storage: [encoded_capacity]u8 = undefined;
    const bytes = try encode(&storage, .{ .phase = 255, .completed = std.math.maxInt(u64), .total = std.math.maxInt(u64), .model = &model, .backend = &backend });
    var parsed = try std.json.parseFromSlice(Update, std.testing.allocator, bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 255), parsed.value.phase);
    try std.testing.expectEqual(std.math.maxInt(u64), parsed.value.completed);
    try std.testing.expectEqual(std.math.maxInt(u64), parsed.value.total);
    try std.testing.expectEqualSlices(u8, model[0..max_model_bytes], parsed.value.model);
    try std.testing.expectEqualSlices(u8, backend[0..max_backend_bytes], parsed.value.backend);
    try std.testing.expect(bytes.len <= encoded_capacity);
}

test "inference worker progress labels preserve UTF8 boundaries and bound invalid input" {
    var storage: [encoded_capacity]u8 = undefined;
    var model: [max_model_bytes + 2]u8 = @splat('a');
    @memcpy(model[max_model_bytes - 1 ..], "\xe2\x82\xac");
    const bytes = try encode(&storage, .{ .phase = 1, .completed = 0, .total = 1, .model = &model, .backend = "\xff" });
    var parsed = try std.json.parseFromSlice(Update, std.testing.allocator, bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings(model[0 .. max_model_bytes - 1], parsed.value.model);
    try std.testing.expectEqualStrings("", parsed.value.backend);
}
