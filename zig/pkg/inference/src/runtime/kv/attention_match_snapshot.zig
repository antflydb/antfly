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

//! Authoritative, bounded readback of an exact device prefix. The caller holds
//! the source immutable throughout capture. Readers must never fall back to a
//! logical pool's host bytes. Compacted/reordered layouts require another reader.
const std = @import("std");
const checkpoint = @import("attention_match_checkpoint.zig");
const Control = @import("attention_match.zig").Control;

pub const Donor = struct { layer: u32, window: u32, heads: u32, dim: u32 };
pub const Range = struct {
    sequence: u32,
    layer: u32,
    logical_length: usize,
    start: usize,
    count: usize,
    heads: u32,
    dim: u32,
};
pub const Reader = struct {
    context: *anyopaque,
    /// Fenced, token-major [row, head, dimension] f32 output. A failure may
    /// partially fill the outputs, but must finish using them before returning.
    read_fn: *const fn (*anyopaque, Range, []f32, []f32) anyerror!void,
};
pub const Request = struct {
    identity: checkpoint.Identity,
    sequence: u32,
    tokens: []const u32,
    logits: []const f32,
    layer_donors: []const u32,
    donors: []const Donor,
    max_host_bytes: usize,
    limits: checkpoint.Limits = .{},
    control: Control = .{},
};

pub const chunk_elements = 65536;
// CUDA expands half readback in place; no separate conversion buffer exists.
pub const workspace_bytes = chunk_elements * 2 * @sizeOf(f32);
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.KvCheckpointLimit;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.KvCheckpointLimit;
}

/// Exact payload size from authenticated model geometry, before source tokens or
/// logits are materialized. Content validation remains in allocationBytes.
pub fn geometryBytes(n: usize, vocab: usize, layer_donors: []const u32, donors: []const Donor, limits: checkpoint.Limits) !usize {
    if (n == 0 or n > limits.max_tokens or n > std.math.maxInt(u32) or vocab == 0 or vocab > limits.max_vocab or
        layer_donors.len == 0 or layer_donors.len > limits.max_layers or donors.len == 0 or donors.len > layer_donors.len)
        return error.InvalidKvCheckpoint;
    for (layer_donors, 0..) |index, layer| {
        if (index >= donors.len or donors[index].layer > layer) return error.InvalidKvCheckpoint;
    }
    var bytes = try mul(try add(try add(n, vocab), layer_donors.len), 4);
    bytes = try add(bytes, try mul(donors.len, @sizeOf(checkpoint.Donor)));
    for (donors, 0..) |donor, i| {
        if (donor.layer >= layer_donors.len or layer_donors[donor.layer] != i or
            (i > 0 and donor.layer <= donors[i - 1].layer) or donor.window > limits.max_tokens or
            donor.heads == 0 or donor.heads > limits.max_kv_heads or donor.dim == 0 or donor.dim > limits.max_head_dim)
            return error.InvalidKvCheckpoint;
        if (try mul(donor.heads, donor.dim) > chunk_elements) return error.UnsupportedKvSnapshotLayout;
        const rows = if (donor.window == 0) n else @min(n, donor.window);
        bytes = try add(bytes, try mul(donor.heads, try add(@sizeOf(checkpoint.Head), try mul(rows, try add(20, try mul(donor.dim, 8))))));
    }
    if (bytes > limits.max_state_bytes) return error.KvCheckpointLimit;
    return bytes;
}

pub fn allocationBytes(request: Request) !usize {
    const bytes = try geometryBytes(request.tokens.len, request.logits.len, request.layer_donors, request.donors, request.limits);
    for (request.tokens) |token| if (token >= request.logits.len) return error.InvalidKvCheckpoint;
    for (request.logits) |value| if (!std.math.isFinite(value)) return error.InvalidKvCheckpoint;
    if (try add(bytes, workspace_bytes) > request.max_host_bytes) return error.KvCheckpointLimit;
    return bytes;
}

pub fn capture(allocator: std.mem.Allocator, request: Request, reader: Reader) !checkpoint.Owned {
    try request.control.check();
    _ = try allocationBytes(request);
    var result: checkpoint.Owned = .{ .allocator = allocator, .state = .{ .identity = request.identity, .logical_length = request.tokens.len } };
    errdefer result.deinit();
    result.state.tokens = try allocator.dupe(u32, request.tokens);
    result.state.logits = try allocator.dupe(f32, request.logits);
    result.state.layer_donors = try allocator.dupe(u32, request.layer_donors);
    const donors = try allocator.alloc(checkpoint.Donor, request.donors.len);
    @memset(donors, .{});
    result.state.donors = donors;
    const key_buffer = try allocator.alloc(f32, chunk_elements);
    defer allocator.free(key_buffer);
    const value_buffer = try allocator.alloc(f32, chunk_elements);
    defer allocator.free(value_buffer);
    for (request.donors, donors) |geometry, *donor| {
        try request.control.check();
        const rows = if (geometry.window == 0) request.tokens.len else @min(request.tokens.len, geometry.window);
        const start = request.tokens.len - rows;
        const heads = try allocator.alloc(checkpoint.Head, geometry.heads);
        @memset(heads, .{});
        donor.* = .{ .layer = geometry.layer, .window = geometry.window, .heads = heads };
        for (heads) |*head| {
            head.dim = geometry.dim;
            const positions = try allocator.alloc(u64, rows);
            head.positions = positions;
            const ends = try allocator.alloc(u64, rows);
            head.coverage_ends = ends;
            for (positions, ends, 0..) |*pos, *end, row| {
                pos.* = start + row;
                end.* = start + row + 1;
            }
            head.keys = try allocator.alloc(f32, rows * geometry.dim);
            head.values = try allocator.alloc(f32, rows * geometry.dim);
            const bias = try allocator.alloc(f32, rows);
            @memset(bias, 0);
            head.bias = bias;
        }
        const width = geometry.heads * geometry.dim;
        var offset: usize = 0;
        while (offset < rows) {
            try request.control.check();
            const count = @min(rows - offset, chunk_elements / width);
            const k = key_buffer[0 .. count * width];
            const v = value_buffer[0 .. count * width];
            try reader.read_fn(reader.context, .{ .sequence = request.sequence, .layer = geometry.layer, .logical_length = request.tokens.len, .start = start + offset, .count = count, .heads = geometry.heads, .dim = geometry.dim }, k, v);
            try request.control.check();
            for (0..count) |row| for (heads, 0..) |head, h| {
                const from = row * width + h * geometry.dim;
                const to = (offset + row) * geometry.dim;
                @memcpy(@constCast(head.keys[to..][0..geometry.dim]), k[from..][0..geometry.dim]);
                @memcpy(@constCast(head.values[to..][0..geometry.dim]), v[from..][0..geometry.dim]);
            };
            offset += count;
        }
    }
    try result.state.validate(request.limits);
    try request.control.check();
    return result;
}

const fixture: Request = .{
    .identity = .{ .weights = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4) },
    .sequence = 7,
    .tokens = &.{ 0, 1, 0, 1 },
    .logits = &.{ 0.1, 0.2 },
    .layer_donors = &.{ 0, 1, 0 },
    .donors = &.{ .{ .layer = 0, .window = 0, .heads = 2, .dim = 2 }, .{ .layer = 1, .window = 2, .heads = 2, .dim = 2 } },
    .max_host_bytes = 1024 * 1024,
};
const TestReader = struct {
    calls: usize = 0,
    fail: bool = false,
    fn read(raw: *anyopaque, range: Range, k: []f32, v: []f32) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.calls += 1;
        if (self.fail) return error.DeviceReadUnsupported;
        for (k, v, 0..) |*key, *value, i| {
            key.* = @floatFromInt(range.start * range.heads * range.dim + i + range.layer * 100);
            value.* = key.* + 10;
        }
    }
    fn reader(self: *@This()) Reader {
        return .{ .context = self, .read_fn = read };
    }
};

test "attention_match snapshot reads each shared donor once and preserves local logical positions" {
    var backend: TestReader = .{};
    var result = try capture(std.testing.allocator, fixture, backend.reader());
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), backend.calls);
    try std.testing.expectEqualSlices(f32, &.{ 2, 3, 6, 7, 10, 11, 14, 15 }, result.state.donors[0].heads[1].keys);
    try std.testing.expectEqualSlices(u64, &.{ 2, 3 }, result.state.donors[1].heads[0].positions);
    try std.testing.expectEqualSlices(f32, &.{ 108, 109, 112, 113 }, result.state.donors[1].heads[0].keys);
    try std.testing.expectEqual(try allocationBytes(fixture), try result.state.residentBytes());
    var denied = fixture;
    denied.max_host_bytes = try allocationBytes(fixture) + workspace_bytes - 1;
    try std.testing.expectError(error.KvCheckpointLimit, capture(std.testing.allocator, denied, backend.reader()));
    try std.testing.expectEqual(@as(usize, 2), backend.calls);
    backend.fail = true;
    try std.testing.expectError(error.DeviceReadUnsupported, capture(std.testing.allocator, fixture, backend.reader()));
}
fn allocationFailure(allocator: std.mem.Allocator) !void {
    var backend: TestReader = .{};
    var result = try capture(allocator, fixture, backend.reader());
    defer result.deinit();
}
test "attention_match snapshot unwinds every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailure, .{});
}

test "attention_match snapshot cancels after fenced read and rejects nonfinite device data" {
    const Backend = struct {
        checks: usize = 0,
        reads: usize = 0,
        cancel: bool = false,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.cancel and self.reads > 0) return error.CompactionCancelled;
        }
        fn read(raw: *anyopaque, _: Range, k: []f32, v: []f32) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            @memset(k, std.math.nan(f32));
            @memset(v, 1);
        }
    };
    var backend: Backend = .{ .cancel = true };
    var request = fixture;
    request.control = .{ .context = &backend, .check_fn = Backend.check };
    const reader: Reader = .{ .context = &backend, .read_fn = Backend.read };
    try std.testing.expectError(error.CompactionCancelled, capture(std.testing.allocator, request, reader));
    try std.testing.expectEqual(@as(usize, 1), backend.reads);
    backend = .{};
    try std.testing.expectError(error.InvalidKvCheckpoint, capture(std.testing.allocator, request, reader));
}

test "attention_match snapshot reads a final partial chunk at its original logical offset" {
    const tokens: [4097]u32 = @splat(0);
    var request = fixture;
    request.tokens = &tokens;
    request.layer_donors = &.{0};
    request.donors = &.{.{ .layer = 0, .window = 0, .heads = 1, .dim = 16 }};
    request.max_host_bytes = 2 * 1024 * 1024;
    var backend: TestReader = .{};
    var captured = try capture(std.testing.allocator, request, backend.reader());
    defer captured.deinit();
    try std.testing.expectEqual(@as(usize, 2), backend.calls);
    try std.testing.expectEqual(@as(f32, 65536), captured.state.donors[0].heads[0].keys[65536]);
    try std.testing.expectEqual(@as(u64, 4097), captured.state.donors[0].heads[0].coverage_ends[4096]);
}
