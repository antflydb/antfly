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

//! State-independent mutation admission; no physical document executor.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("mutation_values.zig");
pub fn validateDocumentTransform(alloc: Allocator, transform: types.DocumentTransform) !void {
    for (transform.operations) |op| {
        switch (op.op) {
            .set, .set_on_insert, .unset, .inc, .push, .pull, .add_to_set, .min, .max => {},
            else => return error.UnsupportedTransformOperation,
        }
        const path = try normalizeJsonPath(op.path);
        if (path.len == 0) return error.InvalidArgument;
        if (op.op == .unset) {
            if (op.value_json != null) return error.InvalidArgument;
            continue;
        }
        const bytes = op.value_json orelse return error.InvalidArgument;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, bytes, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidArgument,
        };
        defer parsed.deinit();
        switch (op.op) {
            .inc, .min, .max => switch (parsed.value) {
                .integer => {},
                .float => |f| if (!std.math.isFinite(f)) return error.InvalidArgument,
                else => return error.InvalidArgument,
            },
            else => {},
        }
    }
}
const NormalizedJsonPath = struct {
    parts: [32][]const u8 = undefined,
    len: usize = 0,

    fn slice(self: *const NormalizedJsonPath) []const []const u8 {
        return self.parts[0..self.len];
    }
};

fn normalizeJsonPath(path: []const u8) !NormalizedJsonPath {
    var normalized = path;
    if (normalized.len > 0 and normalized[0] == '$') {
        if (normalized.len == 1) return .{};
        if (normalized.len < 2 or normalized[1] != '.') return error.InvalidArgument;
        normalized = normalized[2..];
    }
    if (normalized.len == 0) return .{};
    var count: usize = 1;
    for (normalized) |ch| {
        if (ch == '.') count += 1;
    }
    if (count > 32) return error.InvalidArgument;
    var parts = NormalizedJsonPath{};
    var it = std.mem.splitScalar(u8, normalized, '.');
    while (it.next()) |part| {
        if (part.len == 0) return error.InvalidArgument;
        parts.parts[parts.len] = part;
        parts.len += 1;
    }
    if (parts.len != count) return error.InvalidArgument;
    return parts;
}
