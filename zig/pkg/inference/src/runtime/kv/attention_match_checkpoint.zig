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

//! Portable attention_match_v1 state, separate from JSON text-memory checkpoints.
//! The format is explicitly little endian; no native structs or pointers go on
//! disk. Integrity is SHA-256, not authentication. Import requires trusted model
//! fingerprints and bounded allocation before any state may replace a cache.
const std = @import("std");
const Control = @import("attention_match.zig").Control;
pub const codec = @import("attention_match_codec.zig");
pub const Encoding = codec.Encoding;
pub const packed_storage = @import("attention_match_packed.zig");

/// Qualified logical context ceiling shared by checkpoint admission, fitting,
/// and decode. Physical compaction never increases this context window.
pub const max_context_tokens: usize = 32768;

pub const Identity = struct {
    weights: [32]u8,
    tokenizer: [32]u8,
    template: [32]u8,
    layout: [32]u8,

    pub fn eql(a: Identity, b: Identity) bool {
        return std.mem.eql(u8, &a.weights, &b.weights) and std.mem.eql(u8, &a.tokenizer, &b.tokenizer) and
            std.mem.eql(u8, &a.template, &b.template) and std.mem.eql(u8, &a.layout, &b.layout);
    }
};

pub const Head = struct {
    dim: u32 = 0,
    /// Exclusive end of the fitted prefix. Rows at and after this boundary
    /// must be a contiguous exact suffix; widening the recent window behind
    /// this point requires replay, not another compaction.
    compacted_until: u64 = 0,
    /// Post-RoPE keys retain their original positions, independently per KV head.
    positions: []const u64 = &.{},
    /// Exclusive logical end of the history represented by each row. A query
    /// at p may see this row only when coverage_end <= p + 1.
    coverage_ends: []const u64 = &.{},
    keys: []const f32 = &.{},
    values: []const f32 = &.{},
    bias: []const f32 = &.{},
    /// Storage-only annotation; these values have already passed attention validation.
    fp8: codec.Fp8 = .{},
    compressed: packed_storage.Packed = .{},

    pub fn visible(self: Head, row: usize, query_position: u64) bool {
        return self.coverage_ends[row] != 0 and self.coverage_ends[row] - 1 <= query_position;
    }
};

pub const Donor = struct {
    layer: u32 = 0,
    /// Zero for global attention. Local donors must contain an exact contiguous
    /// suffix of min(window, logical_length) rows, never fitted values or bias.
    window: u32 = 0,
    heads: []const Head = &.{},
};

pub const State = struct {
    identity: Identity,
    /// Never reset to the physical row count after compaction.
    logical_length: u64,
    tokens: []const u32 = &.{},
    /// Next-token logits at the ingestion boundary. Replaying the last token
    /// would mutate the compacted history, so a resumable checkpoint saves these.
    logits: []const f32 = &.{},
    /// One donor index per model layer. Shared layers do not duplicate K/V.
    layer_donors: []const u32 = &.{},
    donors: []const Donor = &.{},

    pub fn validate(self: State, limits: Limits) !void {
        return self.validateWithControl(limits, .{});
    }

    pub fn validateWithControl(self: State, limits: Limits, control: Control) !void {
        try control.check();
        if (self.logical_length == 0 or self.logical_length > limits.max_tokens or self.tokens.len != self.logical_length or
            self.tokens.len > std.math.maxInt(u32) or
            self.logits.len == 0 or self.logits.len > limits.max_vocab or
            self.layer_donors.len == 0 or self.layer_donors.len > limits.max_layers or
            self.donors.len == 0 or self.donors.len > self.layer_donors.len) return error.InvalidKvCheckpoint;
        for (self.tokens, 0..) |token, i| {
            if (i % 4096 == 0) try control.check();
            if (token >= self.logits.len) return error.InvalidKvCheckpoint;
        }
        try finiteWithControl(self.logits, control);
        for (self.layer_donors, 0..) |idx, layer| {
            if (idx >= self.donors.len or self.donors[idx].layer > layer) return error.InvalidKvCheckpoint;
        }
        for (self.donors, 0..) |donor, idx| {
            if (donor.layer >= self.layer_donors.len or self.layer_donors[donor.layer] != idx or
                (idx > 0 and donor.layer <= self.donors[idx - 1].layer) or donor.window > limits.max_tokens or
                donor.heads.len == 0 or donor.heads.len > limits.max_kv_heads) return error.InvalidKvCheckpoint;
            for (donor.heads) |head| {
                const rows = head.positions.len;
                try validateFp8(head, donor.window);
                if (head.dim == 0 or head.dim > limits.max_head_dim or head.dim != donor.heads[0].dim or
                    head.compacted_until > self.logical_length or (donor.window != 0 and head.compacted_until != 0) or
                    rows == 0 or rows > self.logical_length or head.coverage_ends.len != rows or head.bias.len != rows or
                    head.keys.len != try mul(rows, head.dim) or head.values.len != head.keys.len) return error.InvalidKvCheckpoint;
                try validatePacked(head, donor.window, control);
                try finiteWithControl(head.keys, control);
                try finiteWithControl(head.values, control);
                try finiteWithControl(head.bias, control);
                const local_rows = @min(donor.window, self.logical_length);
                if (donor.window > 0 and rows != local_rows) return error.InvalidKvCheckpoint;
                var next_exact = head.compacted_until;
                var prefix_end: u64 = 0;
                for (head.positions, head.coverage_ends, head.bias, 0..) |pos, end, bias, row| {
                    if (row % 1024 == 0) try control.check();
                    if (pos >= self.logical_length or end <= pos or end > self.logical_length or
                        (row > 0 and (pos <= head.positions[row - 1] or end < head.coverage_ends[row - 1])))
                        return error.InvalidKvCheckpoint;
                    if (donor.window > 0 and (pos != self.logical_length - local_rows + row or end != pos + 1 or bias != 0))
                        return error.InvalidKvCheckpoint;
                    if (donor.window == 0) {
                        if (pos < head.compacted_until) {
                            if (end > head.compacted_until) return error.InvalidKvCheckpoint;
                            prefix_end = end;
                        } else {
                            if (pos != next_exact or end != pos + 1 or bias != 0) return error.InvalidKvCheckpoint;
                            next_exact += 1;
                        }
                    }
                }
                if (donor.window == 0 and (next_exact != self.logical_length or prefix_end != head.compacted_until))
                    return error.InvalidKvCheckpoint;
            }
        }
    }

    /// Payload allocations only; excludes the caller's input file and any
    /// resident device allocation. Used to reserve original plus candidate state.
    pub fn residentBytes(self: State) !usize {
        var size = try mul(try add(try add(self.tokens.len, self.logits.len), self.layer_donors.len), 4);
        size = try add(size, try mul(self.donors.len, @sizeOf(Donor)));
        for (self.donors) |donor| {
            size = try add(size, try mul(donor.heads.len, @sizeOf(Head)));
            for (donor.heads) |head| {
                size = try add(size, try mul(head.positions.len, 20));
                size = try add(size, try mul(head.keys.len, 8));
                size = try add(size, head.compressed.bytes());
            }
        }
        return size;
    }
};

pub const Limits = struct {
    max_file_bytes: usize = 512 * 1024 * 1024,
    max_state_bytes: usize = 512 * 1024 * 1024,
    max_tokens: u64 = 131072,
    max_vocab: u32 = 262144,
    max_layers: u32 = 256,
    max_kv_heads: u32 = 64,
    max_head_dim: u32 = 1024,
};

/// Production transport/state limits. The generic codec can be used with other
/// explicit limits, but native serving always enforces the qualified ceiling.
pub const serving_limits: Limits = .{
    .max_file_bytes = 2 * 1024 * 1024 * 1024,
    .max_state_bytes = 2 * 1024 * 1024 * 1024,
    .max_tokens = max_context_tokens,
};

pub const Owned = struct {
    allocator: std.mem.Allocator,
    state: State,

    pub fn deinit(self: *Owned) void {
        const a = self.allocator;
        for (self.state.donors) |donor| {
            for (donor.heads) |head| {
                a.free(head.positions);
                a.free(head.coverage_ends);
                a.free(head.keys);
                a.free(head.values);
                a.free(head.bias);
                var compressed = head.compressed;
                compressed.deinit(a);
            }
            a.free(donor.heads);
        }
        a.free(self.state.donors);
        a.free(self.state.tokens);
        a.free(self.state.logits);
        a.free(self.state.layer_donors);
        self.* = undefined;
    }
};

const magic = "ANTFLYKV";
const version: u32 = 1;
const header_bytes = 8 + 4 + 4 + 8 + 128 + 32;
const digest_offset = header_bytes - 32;

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidKvCheckpoint;
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.InvalidKvCheckpoint;
}
fn finiteWithControl(values: []const f32, control: Control) !void {
    var offset: usize = 0;
    while (offset < values.len) {
        try control.check();
        const end = offset + @min(4096, values.len - offset);
        for (values[offset..end]) |v| if (!std.math.isFinite(v)) return error.InvalidKvCheckpoint;
        offset = end;
    }
}

fn hashWithControl(hash: *std.crypto.hash.sha2.Sha256, bytes: []const u8, control: Control) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        try control.check();
        const end = offset + @min(64 * 1024, bytes.len - offset);
        hash.update(bytes[offset..end]);
        offset = end;
    }
    try control.check();
}

fn fileBytes(state: State) !usize {
    return fileBytesEncoded(state, .f32);
}

fn fileBytesEncoded(state: State, encoding: Encoding) !usize {
    // logical_length; token, logit, layer, donor counts
    var size = try add(header_bytes, 8 + 4 * 4);
    size = try add(size, try mul(try add(try add(state.tokens.len, state.logits.len), state.layer_donors.len), 4));
    for (state.donors) |donor| {
        size = try add(size, 12);
        for (donor.heads) |head| {
            if (encoding != .rotated4 and head.compressed.spans.len != 0) return error.KvStoragePrecisionMismatch;
            size = try add(size, 16);
            size = try add(size, try mul(head.positions.len, 20)); // pos, end, bias
            if (encoding == .rotated4) {
                size = try add(size, 16);
                size = try add(size, head.compressed.bytes());
                size = try add(size, try mul(try mul(head.positions.len - head.compressed.rows(), head.dim), 4));
            } else if (encoding == .f32) {
                size = try add(size, try mul(head.keys.len, 8)); // K, V
            } else {
                const fp8 = if (encoding == .fp8) head.fp8 else codec.Fp8{};
                size = try add(size, 16); // range and K/V scales
                const dense = try mul(head.keys.len, 4);
                const saved = try mul(try mul(fp8.rows, head.dim), 2);
                if (saved > dense) return error.InvalidKvCheckpoint;
                size = try add(size, dense - saved);
            }
        }
    }
    return size;
}

const Encoder = struct {
    bytes: []u8,
    offset: usize = 0,
    fn raw(self: *Encoder, value: []const u8) !void {
        @memcpy(self.bytes[self.offset..][0..value.len], value);
        self.offset += value.len;
    }
    fn int(self: *Encoder, comptime T: type, value: T) !void {
        std.mem.writeInt(T, self.bytes[self.offset..][0..@sizeOf(T)], value, .little);
        self.offset += @sizeOf(T);
    }
    fn slice(self: *Encoder, comptime T: type, values: []const T) !void {
        if (comptime @import("builtin").target.cpu.arch.endian() == .little)
            return self.raw(std.mem.sliceAsBytes(values));
        for (values) |value| {
            if (T == f32) try self.int(u32, @bitCast(value)) else try self.int(T, value);
        }
    }
};

fn checksum(bytes: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(bytes[0..digest_offset]);
    hash.update(bytes[header_bytes..]);
    return hash.finalResult();
}

pub fn encode(allocator: std.mem.Allocator, state: State, limits: Limits) ![]u8 {
    try state.validate(limits);
    if (try state.residentBytes() > limits.max_state_bytes) return error.KvCheckpointLimit;
    const size = try fileBytes(state);
    if (size > limits.max_file_bytes) return error.KvCheckpointLimit;
    const bytes = try allocator.alloc(u8, size);
    errdefer allocator.free(bytes);
    var w: Encoder = .{ .bytes = bytes };
    try w.raw(magic);
    try w.int(u32, version);
    try w.int(u32, 0); // flags: f32, little endian, attention_match_v1
    try w.int(u64, size - header_bytes);
    inline for (.{ "weights", "tokenizer", "template", "layout" }) |field| try w.raw(&@field(state.identity, field));
    try w.raw(&@as([32]u8, @splat(0)));
    try writePayload(&w, state);
    std.debug.assert(w.offset == bytes.len);
    @memcpy(bytes[digest_offset..header_bytes], &checksum(bytes));
    return bytes;
}

pub const ExportDigest = struct { integrity_sha256: [32]u8, file_sha256: [32]u8, bytes: usize };

fn validateFp8(head: Head, window: u32) !void {
    const q = head.fp8;
    if (!codec.validScale(q.key_scale) or !codec.validScale(q.value_scale)) return error.InvalidKvCheckpoint;
    if (q.rows == 0) {
        if (q.first_row != 0 or q.key_scale != 1 or q.value_scale != 1) return error.InvalidKvCheckpoint;
        return;
    }
    if (window != 0 or q.first_row > head.positions.len or q.rows > head.positions.len - q.first_row or
        head.coverage_ends.len != head.positions.len) return error.InvalidKvCheckpoint;
    const last = @as(usize, q.first_row) + q.rows - 1;
    if (head.positions[last] >= head.compacted_until or head.coverage_ends[last] > head.compacted_until)
        return error.InvalidKvCheckpoint;
}

fn encodedValue(value: f32, scale: f32, fp8: bool) !u16 {
    if (!codec.halfExact(value)) return error.KvStoragePrecisionMismatch;
    if (!fp8) return @bitCast(@as(f16, @floatCast(value)));
    const bits = try codec.encode(value, scale);
    // Export never introduces unvalidated rounding, including repeated exports.
    if (@as(u32, @bitCast(try codec.decode(bits, scale))) != @as(u32, @bitCast(value)))
        return error.KvStoragePrecisionMismatch;
    return bits;
}

pub fn encodedSize(state: State, limits: Limits, encoding: Encoding, control: Control) !usize {
    if (encoding == .f32) return encodedSizeWithControl(state, limits, control);
    try state.validateWithControl(limits, control);
    if (try state.residentBytes() > limits.max_state_bytes) return error.KvCheckpointLimit;
    for (state.donors) |donor| for (donor.heads) |head| {
        const q = if (encoding == .fp8) head.fp8 else codec.Fp8{};
        const begin = @as(usize, q.first_row) * head.dim;
        const end = begin + @as(usize, q.rows) * head.dim;
        var compressed_row = false;
        for (head.keys, head.values, 0..) |k, v, i| {
            if (i % 4096 == 0) try control.check();
            if (encoding == .rotated4 and i % head.dim == 0) compressed_row = head.compressed.index(i / head.dim) == .compressed;
            if (compressed_row) continue;
            _ = try encodedValue(k, q.key_scale, i >= begin and i < end);
            _ = try encodedValue(v, q.value_scale, i >= begin and i < end);
        }
    };
    const size = try fileBytesEncoded(state, encoding);
    if (size > limits.max_file_bytes) return error.KvCheckpointLimit;
    try control.check();
    return size;
}

/// V3 retains V2 framing/integrity and stores K/V as exact f16 or validated
/// E4M3FN ranges. Metadata, biases and saved logits retain their full precision.
pub fn exportEncoded(state: State, limits: Limits, encoding: Encoding, sink: anytype, control: Control) !ExportDigest {
    if (encoding == .f32) return exportV2(state, limits, sink, control);
    const size = try encodedSize(state, limits, encoding, control);
    var header: [digest_offset]u8 = undefined;
    var h: Encoder = .{ .bytes = &header };
    try h.raw(magic);
    try h.int(u32, encoding.wireVersion());
    try h.int(u32, @intFromBool(encoding == .fp8));
    try h.int(u64, size - header_bytes);
    inline for (.{ "weights", "tokenizer", "template", "layout" }) |field| try h.raw(&@field(state.identity, field));
    var w: StreamEncoder(@TypeOf(sink)) = .{ .sink = sink, .control = control, .streaming_hash = true };
    try w.slice(u8, &header);
    try writePayloadEncoded(&w, state, encoding);
    try w.flush();
    try control.check();
    const digest = w.hash.peek();
    try sink.writeAll(&digest);
    w.hash.update(&digest);
    try control.check();
    return .{ .integrity_sha256 = digest, .file_sha256 = w.hash.finalResult(), .bytes = size };
}

pub fn encodeWithEncoding(allocator: std.mem.Allocator, state: State, limits: Limits, encoding: Encoding, control: Control) ![]u8 {
    const bytes = try allocator.alloc(u8, try encodedSize(state, limits, encoding, control));
    errdefer allocator.free(bytes);
    const Sink = struct {
        bytes: []u8,
        offset: usize = 0,
        pub fn writeAll(self: *@This(), data: []const u8) !void {
            if (data.len > self.bytes.len - self.offset) return error.InvalidKvCheckpoint;
            @memcpy(self.bytes[self.offset..][0..data.len], data);
            self.offset += data.len;
        }
    };
    var sink: Sink = .{ .bytes = bytes };
    const result = try exportEncoded(state, limits, encoding, &sink, control);
    if (result.bytes != sink.offset or sink.offset != bytes.len) return error.InvalidKvCheckpoint;
    return bytes;
}

/// Validate before emitting transport headers, without allocating a wire copy.
/// Both supported checkpoint versions have the same encoded length. Callers
/// must keep the state immutable through the subsequent export.
pub fn encodedSizeWithControl(state: State, limits: Limits, control: Control) !usize {
    try control.check();
    try state.validateWithControl(limits, control);
    if (try state.residentBytes() > limits.max_state_bytes) return error.KvCheckpointLimit;
    const size = try fileBytes(state);
    if (size > limits.max_file_bytes) return error.KvCheckpointLimit;
    try control.check();
    return size;
}

/// Version 2 keeps the same total overhead and puts its integrity digest in a
/// footer. A sink is private until this function succeeds. Reusing the running
/// hash for the final file digest avoids a second scan of the large payload.
pub fn exportV2(state: State, limits: Limits, sink: anytype, control: @import("attention_match.zig").Control) !ExportDigest {
    const size = try encodedSizeWithControl(state, limits, control);
    var header: [digest_offset]u8 = undefined;
    var h: Encoder = .{ .bytes = &header };
    try h.raw(magic);
    try h.int(u32, 2);
    try h.int(u32, 0);
    try h.int(u64, size - header_bytes);
    inline for (.{ "weights", "tokenizer", "template", "layout" }) |field| try h.raw(&@field(state.identity, field));
    var w: StreamEncoder(@TypeOf(sink)) = .{ .sink = sink, .control = control, .streaming_hash = true };
    try w.slice(u8, &header);
    try writePayload(&w, state);
    try w.flush();
    try control.check();
    const digest = w.hash.peek();
    try sink.writeAll(&digest);
    w.hash.update(&digest);
    try control.check();
    return .{ .integrity_sha256 = digest, .file_sha256 = w.hash.finalResult(), .bytes = size };
}

pub fn encodeV2(allocator: std.mem.Allocator, state: State, limits: Limits) ![]u8 {
    try state.validate(limits);
    if (try state.residentBytes() > limits.max_state_bytes) return error.KvCheckpointLimit;
    const size = try fileBytes(state);
    if (size > limits.max_file_bytes) return error.KvCheckpointLimit;
    const bytes = try allocator.alloc(u8, size);
    errdefer allocator.free(bytes);
    var w: Encoder = .{ .bytes = bytes };
    try w.raw(magic);
    try w.int(u32, 2);
    try w.int(u32, 0);
    try w.int(u64, size - header_bytes);
    inline for (.{ "weights", "tokenizer", "template", "layout" }) |field| try w.raw(&@field(state.identity, field));
    try writePayload(&w, state);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes[0..w.offset], &digest, .{});
    try w.raw(&digest);
    std.debug.assert(w.offset == bytes.len);
    return bytes;
}

pub fn encodeV2WithControl(allocator: std.mem.Allocator, state: State, limits: Limits, control: Control) ![]u8 {
    try state.validateWithControl(limits, control);
    if (try state.residentBytes() > limits.max_state_bytes) return error.KvCheckpointLimit;
    const size = try fileBytes(state);
    if (size > limits.max_file_bytes) return error.KvCheckpointLimit;
    const bytes = try allocator.alloc(u8, size);
    errdefer allocator.free(bytes);
    const Sink = struct {
        bytes: []u8,
        offset: usize = 0,
        pub fn writeAll(self: *@This(), data: []const u8) !void {
            if (data.len > self.bytes.len - self.offset) return error.InvalidKvCheckpoint;
            @memcpy(self.bytes[self.offset..][0..data.len], data);
            self.offset += data.len;
        }
    };
    var sink: Sink = .{ .bytes = bytes };
    const exported = try exportV2(state, limits, &sink, control);
    if (sink.offset != size or exported.bytes != size) return error.InvalidKvCheckpoint;
    try control.check();
    return bytes;
}

fn writePayload(w: anytype, state: State) !void {
    return writePayloadEncoded(w, state, .f32);
}

fn writePayloadEncoded(w: anytype, state: State, encoding: Encoding) !void {
    try w.int(u64, state.logical_length);
    try w.int(u32, @intCast(state.tokens.len));
    try w.int(u32, @intCast(state.logits.len));
    try w.int(u32, @intCast(state.layer_donors.len));
    try w.int(u32, @intCast(state.donors.len));
    try w.slice(u32, state.tokens);
    try w.slice(f32, state.logits);
    try w.slice(u32, state.layer_donors);
    for (state.donors) |donor| {
        try w.int(u32, donor.layer);
        try w.int(u32, donor.window);
        try w.int(u32, @intCast(donor.heads.len));
        for (donor.heads) |head| {
            try w.int(u32, head.dim);
            try w.int(u32, @intCast(head.positions.len));
            try w.int(u64, head.compacted_until);
            const q = if (encoding == .fp8) head.fp8 else codec.Fp8{};
            if (encoding == .rotated4) {
                try w.int(u32, head.compressed.version);
                try w.int(u32, @intCast(head.compressed.spans.len));
                try w.int(u32, @intCast(head.compressed.rows()));
                try w.int(u32, 0);
                for (head.compressed.spans) |span| {
                    try w.int(u32, span.first);
                    try w.int(u32, span.rows);
                    try w.int(u32, span.offset);
                }
            } else if (encoding != .f32) {
                try w.int(u32, q.first_row);
                try w.int(u32, q.rows);
                try w.int(u32, @bitCast(q.key_scale));
                try w.int(u32, @bitCast(q.value_scale));
            }
            try w.slice(u64, head.positions);
            try w.slice(u64, head.coverage_ends);
            if (encoding == .rotated4) {
                try w.slice(u8, head.compressed.keys);
                try w.slice(u8, head.compressed.values);
                for ([_][]const f32{ head.keys, head.values }) |values| {
                    for (0..head.positions.len) |row| {
                        if (head.compressed.index(row) == .compressed) continue;
                        for (values[row * head.dim ..][0..head.dim]) |value|
                            try w.int(u16, try encodedValue(value, 1, false));
                    }
                }
            } else if (encoding == .f32) {
                try w.slice(f32, head.keys);
                try w.slice(f32, head.values);
            } else {
                try writeValues(w, head.keys, head.dim, q, q.key_scale);
                try writeValues(w, head.values, head.dim, q, q.value_scale);
            }
            try w.slice(f32, head.bias);
        }
    }
}

fn writeValues(w: anytype, values: []const f32, dim: u32, q: codec.Fp8, scale: f32) !void {
    const begin = @as(usize, q.first_row) * dim;
    const end = begin + @as(usize, q.rows) * dim;
    for (values, 0..) |value, i| {
        const fp8 = i >= begin and i < end;
        const bits = try encodedValue(value, scale, fp8);
        if (fp8) try w.int(u8, @intCast(bits)) else try w.int(u16, bits);
    }
}

fn readValues(r: anytype, elements: usize, head: Head, scale: f32) ![]f32 {
    const begin = try mul(head.fp8.first_row, head.dim);
    const quantized = try mul(head.fp8.rows, head.dim);
    if (try mul(elements, 2) - quantized > r.wireRemaining()) return error.InvalidKvCheckpoint;
    const values = try r.alloc(f32, elements);
    errdefer r.allocator.free(values);
    var buffer: [16384]u8 = undefined;
    var offset: usize = 0;
    while (offset < elements) {
        try r.control.check();
        const fp8 = offset >= begin and offset < begin + quantized;
        const stop = if (offset < begin) begin else if (fp8) begin + quantized else elements;
        const width: usize = if (fp8) 1 else 2;
        const count = @min(stop - offset, buffer.len / width);
        try r.read(buffer[0 .. count * width]);
        for (values[offset..][0..count], 0..) |*value, i| {
            value.* = if (fp8) try codec.decode(buffer[i], scale) else @as(f16, @bitCast(std.mem.readInt(u16, buffer[i * 2 ..][0..2], .little)));
            if (!std.math.isFinite(value.*)) return error.InvalidKvCheckpoint;
        }
        offset += count;
    }
    return values;
}

/// Bounded two-pass export of the v1 wire format. The caller holds the state
/// immutable for both passes and publishes the destination only on success.
/// Sink.writeAll must consume the bytes before returning. Cancellation is checked
/// every 16 KiB in both the checksum and output pass. No file-sized allocation.
pub fn exportTo(state: State, limits: Limits, sink: anytype, control: @import("attention_match.zig").Control) !void {
    try control.check();
    try state.validateWithControl(limits, control);
    if (try state.residentBytes() > limits.max_state_bytes) return error.KvCheckpointLimit;
    const size = try fileBytes(state);
    if (size > limits.max_file_bytes) return error.KvCheckpointLimit;
    var header: [header_bytes]u8 = @splat(0);
    var h: Encoder = .{ .bytes = &header };
    try h.raw(magic);
    try h.int(u32, version);
    try h.int(u32, 0);
    try h.int(u64, size - header_bytes);
    inline for (.{ "weights", "tokenizer", "template", "layout" }) |field| try h.raw(&@field(state.identity, field));
    var w: StreamEncoder(@TypeOf(sink)) = .{ .sink = sink, .control = control };
    w.hash.update(header[0..digest_offset]);
    try writePayload(&w, state);
    try w.flush();
    @memcpy(header[digest_offset..], &w.hash.finalResult());
    try control.check();
    try sink.writeAll(&header);
    w.hashing = false;
    try writePayload(&w, state);
    try w.flush();
    try control.check();
}

fn StreamEncoder(comptime Sink: type) type {
    return struct {
        sink: Sink,
        control: @import("attention_match.zig").Control,
        hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
        hashing: bool = true,
        streaming_hash: bool = false,
        buffer: [16 * 1024]u8 = undefined,
        used: usize = 0,
        fn flush(self: *@This()) !void {
            try self.control.check();
            if (self.used == 0) return;
            if (self.streaming_hash) {
                self.hash.update(self.buffer[0..self.used]);
                try self.sink.writeAll(self.buffer[0..self.used]);
            } else if (self.hashing) self.hash.update(self.buffer[0..self.used]) else try self.sink.writeAll(self.buffer[0..self.used]);
            self.used = 0;
        }
        fn int(self: *@This(), comptime T: type, value: T) !void {
            if (self.used + @sizeOf(T) > self.buffer.len) try self.flush();
            std.mem.writeInt(T, self.buffer[self.used..][0..@sizeOf(T)], value, .little);
            self.used += @sizeOf(T);
        }
        fn slice(self: *@This(), comptime T: type, values: []const T) !void {
            if (comptime @import("builtin").target.cpu.arch.endian() == .little) {
                const bytes = std.mem.sliceAsBytes(values);
                var offset: usize = 0;
                while (offset < bytes.len) {
                    if (self.used == self.buffer.len) try self.flush();
                    const count = @min(self.buffer.len - self.used, bytes.len - offset);
                    @memcpy(self.buffer[self.used..][0..count], bytes[offset..][0..count]);
                    self.used += count;
                    offset += count;
                }
                return;
            }
            for (values) |value| {
                if (T == f32) try self.int(u32, @bitCast(value)) else try self.int(T, value);
            }
        }
    };
}

/// Copy only resume metadata and allocate empty donor/head slots. This enables
/// fitting directly into the candidate without copying the full source K/V or
/// constructing a serialized intermediary. Caller validates/admit source first.
pub fn cloneMetadata(allocator: std.mem.Allocator, source: State) !Owned {
    var result: Owned = .{ .allocator = allocator, .state = .{ .identity = source.identity, .logical_length = source.logical_length } };
    errdefer result.deinit();
    result.state.tokens = try allocator.dupe(u32, source.tokens);
    result.state.logits = try allocator.dupe(f32, source.logits);
    result.state.layer_donors = try allocator.dupe(u32, source.layer_donors);
    const donors = try allocator.alloc(Donor, source.donors.len);
    @memset(donors, .{});
    result.state.donors = donors;
    for (source.donors, donors) |original, *donor| {
        const heads = try allocator.alloc(Head, original.heads.len);
        @memset(heads, .{});
        donor.* = .{ .layer = original.layer, .window = original.window, .heads = heads };
    }
    return result;
}

pub fn cloneHead(allocator: std.mem.Allocator, source: Head) !Head {
    const positions = try allocator.dupe(u64, source.positions);
    errdefer allocator.free(positions);
    const ends = try allocator.dupe(u64, source.coverage_ends);
    errdefer allocator.free(ends);
    const keys = try allocator.dupe(f32, source.keys);
    errdefer allocator.free(keys);
    const values = try allocator.dupe(f32, source.values);
    errdefer allocator.free(values);
    const bias = try allocator.dupe(f32, source.bias);
    errdefer allocator.free(bias);
    const compressed = try source.compressed.clone(allocator);
    return .{ .compressed = compressed, .fp8 = source.fp8, .dim = source.dim, .compacted_until = source.compacted_until, .positions = positions, .coverage_ends = ends, .keys = keys, .values = values, .bias = bias };
}

const Decoder = struct {
    control: Control = .{},
    bytes: []const u8,
    allocator: std.mem.Allocator,
    remaining_allocation: usize,
    offset: usize = 0,
    fn wireRemaining(self: *Decoder) usize {
        return self.bytes.len - self.offset;
    }
    fn read(self: *Decoder, bytes: []u8) !void {
        @memcpy(bytes, try self.raw(bytes.len));
    }
    fn raw(self: *Decoder, count: usize) ![]const u8 {
        if (count > self.bytes.len - self.offset) return error.InvalidKvCheckpoint;
        const value = self.bytes[self.offset..][0..count];
        self.offset += count;
        return value;
    }
    fn int(self: *Decoder, comptime T: type) !T {
        const value = try self.raw(@sizeOf(T));
        return std.mem.readInt(T, value[0..@sizeOf(T)], .little);
    }
    fn alloc(self: *Decoder, comptime T: type, count: usize) ![]T {
        try self.control.check();
        const size = try mul(count, @sizeOf(T));
        if (size > self.remaining_allocation) return error.KvCheckpointLimit;
        self.remaining_allocation -= size;
        return self.allocator.alloc(T, count);
    }
    fn slice(self: *Decoder, comptime T: type, count: usize) ![]T {
        // Check wire bounds before allocation, even for checksummed hostile data.
        const data = try self.raw(try mul(count, @sizeOf(T)));
        const values = try self.alloc(T, count);
        errdefer self.allocator.free(values);
        var offset: usize = 0;
        while (offset < count) {
            try self.control.check();
            const end = offset + @min(16384 / @sizeOf(T), count - offset);
            if (comptime @import("builtin").target.cpu.arch.endian() == .little) {
                @memcpy(std.mem.sliceAsBytes(values[offset..end]), data[offset * @sizeOf(T) .. end * @sizeOf(T)]);
            } else {
                for (values[offset..end], offset..) |*value, i| {
                    const U = if (T == f32) u32 else T;
                    const bits = std.mem.readInt(U, data[i * @sizeOf(T) ..][0..@sizeOf(T)], .little);
                    value.* = if (T == f32) @bitCast(bits) else bits;
                }
            }
            offset = end;
        }
        try self.control.check();
        return values;
    }
};

pub fn decode(allocator: std.mem.Allocator, bytes: []const u8, expected: Identity, limits: Limits) !Owned {
    return decodeWithControl(allocator, bytes, expected, limits, .{});
}

pub fn decodeWithControl(allocator: std.mem.Allocator, bytes: []const u8, expected: Identity, limits: Limits, control: Control) !Owned {
    try control.check();
    if (bytes.len > limits.max_file_bytes) return error.KvCheckpointLimit;
    if (bytes.len < header_bytes or !std.mem.eql(u8, bytes[0..8], magic)) return error.InvalidKvCheckpoint;
    var r: Decoder = .{ .bytes = bytes, .allocator = allocator, .remaining_allocation = limits.max_state_bytes, .offset = 8, .control = control };
    const wire_version = try r.int(u32);
    const flags = try r.int(u32);
    if ((wire_version != version and wire_version != 2 and wire_version != 3 and wire_version != 4) or
        (wire_version < 3 and flags != 0) or (wire_version == 3 and flags > 1) or (wire_version == 4 and flags != 0)) return error.UnsupportedKvCheckpoint;
    if (try r.int(u64) != bytes.len - header_bytes) return error.InvalidKvCheckpoint;
    var identity: Identity = undefined;
    inline for (.{ "weights", "tokenizer", "template", "layout" }) |field| @memcpy(&@field(identity, field), try r.raw(32));
    if (!identity.eql(expected)) return error.KvCheckpointModelMismatch;
    if (wire_version == 1) {
        var hash: std.crypto.hash.sha2.Sha256 = .init(.{});
        try hashWithControl(&hash, bytes[0..digest_offset], control);
        try hashWithControl(&hash, bytes[header_bytes..], control);
        if (!std.mem.eql(u8, try r.raw(32), &hash.finalResult())) return error.KvCheckpointChecksum;
    } else {
        const end = bytes.len - 32;
        var hash: std.crypto.hash.sha2.Sha256 = .init(.{});
        try hashWithControl(&hash, bytes[0..end], control);
        const digest = hash.finalResult();
        if (!std.mem.eql(u8, bytes[end..], &digest)) return error.KvCheckpointChecksum;
        r.bytes = bytes[0..end];
    }
    var owned = try decodePayload(&r, allocator, identity, limits, wire_version, flags);
    errdefer owned.deinit();
    if (r.offset != r.bytes.len) return error.InvalidKvCheckpoint;
    try owned.state.validateWithControl(limits, control);
    try control.check();
    return owned;
}

fn decodePayload(r: anytype, allocator: std.mem.Allocator, identity: Identity, limits: Limits, wire_version: u32, flags: u32) !Owned {
    var owned: Owned = .{ .allocator = allocator, .state = .{ .identity = identity, .logical_length = try r.int(u64) } };
    errdefer owned.deinit();
    const nt = try r.int(u32);
    const nv = try r.int(u32);
    const nl = try r.int(u32);
    const nd = try r.int(u32);
    if (nt == 0 or nt > limits.max_tokens or nt != owned.state.logical_length or nv == 0 or nv > limits.max_vocab or
        nl == 0 or nl > limits.max_layers or nd == 0 or nd > nl) return error.InvalidKvCheckpoint;
    owned.state.tokens = try r.slice(u32, nt);
    owned.state.logits = try r.slice(f32, nv);
    owned.state.layer_donors = try r.slice(u32, nl);
    const donors = try r.alloc(Donor, nd);
    @memset(donors, .{});
    owned.state.donors = donors;
    for (donors) |*donor| {
        donor.layer = try r.int(u32);
        donor.window = try r.int(u32);
        const nh = try r.int(u32);
        if (nh == 0 or nh > limits.max_kv_heads) return error.InvalidKvCheckpoint;
        const heads = try r.alloc(Head, nh);
        @memset(heads, .{});
        donor.heads = heads;
        for (heads) |*head| {
            head.dim = try r.int(u32);
            const nr = try r.int(u32);
            head.compacted_until = try r.int(u64);
            if (head.dim == 0 or head.dim > limits.max_head_dim or nr == 0 or nr > nt) return error.InvalidKvCheckpoint;
            if (wire_version == 3) {
                head.fp8 = .{ .first_row = try r.int(u32), .rows = try r.int(u32), .key_scale = @bitCast(try r.int(u32)), .value_scale = @bitCast(try r.int(u32)) };
                if (head.fp8.first_row > nr or head.fp8.rows > nr - head.fp8.first_row or
                    (flags == 0 and head.fp8.rows != 0)) return error.InvalidKvCheckpoint;
            }
            var compressed_rows: usize = 0;
            if (wire_version == 4) compressed_rows = try readPackedHeader(r, head, nr);
            head.positions = try r.slice(u64, nr);
            head.coverage_ends = try r.slice(u64, nr);
            const elements = try mul(nr, head.dim);
            if (wire_version == 4) {
                try readPackedValues(r, head, nr, compressed_rows);
            } else if (wire_version == 3) {
                try validateFp8(head.*, donor.window);
                head.keys = try readValues(r, elements, head.*, head.fp8.key_scale);
                head.values = try readValues(r, elements, head.*, head.fp8.value_scale);
            } else {
                head.keys = try r.slice(f32, elements);
                head.values = try r.slice(f32, elements);
            }
            head.bias = try r.slice(f32, nr);
        }
    }
    return owned;
}

/// Atomic ownership transfer after all fitting/import/upload work has succeeded.
/// The serving owner must serialize commits with decode; generation guards catch
/// stale work. On failure both old and candidate state remain owned and intact.
pub const Store = struct {
    current: ?Owned = null,
    generation: u64 = 0,

    pub fn deinit(self: *Store) void {
        if (self.current) |*current| current.deinit();
        self.* = .{};
    }

    pub fn validateCompaction(self: *const Store, next: State, expected_generation: u64, limits: Limits) !void {
        return self.validateCompactionWithControl(next, expected_generation, limits, .{});
    }

    pub fn validateCompactionWithControl(self: *const Store, next: State, expected_generation: u64, limits: Limits, control: Control) !void {
        try control.check();
        if (expected_generation != self.generation or self.generation == std.math.maxInt(u64)) return error.StaleKvCompaction;

        try next.validateWithControl(limits, control);
        if (try next.residentBytes() > limits.max_state_bytes) return error.KvCheckpointLimit;
        if (self.current) |old| {
            const previous = old.state;
            try previous.validateWithControl(limits, control);
            if (!next.identity.eql(previous.identity) or next.logical_length != previous.logical_length or
                !std.mem.eql(u32, next.tokens, previous.tokens) or !std.mem.eql(f32, next.logits, previous.logits) or
                !std.mem.eql(u32, next.layer_donors, previous.layer_donors) or next.donors.len != previous.donors.len)
                return error.KvCompactionStateMismatch;
            for (previous.donors, next.donors) |a, b| {
                if (a.layer != b.layer or a.window != b.window or a.heads.len != b.heads.len) return error.KvCompactionStateMismatch;
                for (a.heads, b.heads) |x, y| {
                    try control.check();
                    if (x.dim != y.dim or y.compacted_until < x.compacted_until or y.positions.len > x.positions.len)
                        return error.KvCompactionStateMismatch;
                    if (a.window == 0) {
                        const tail: usize = @intCast(next.logical_length - y.compacted_until);
                        const elements = tail * x.dim;
                        if (!std.mem.eql(f32, x.keys[x.keys.len - elements ..], y.keys[y.keys.len - elements ..]) or
                            !std.mem.eql(f32, x.values[x.values.len - elements ..], y.values[y.values.len - elements ..]))
                            return error.KvCompactionStateMismatch;
                    }
                    if (a.window != 0 and (!std.mem.eql(u64, x.positions, y.positions) or
                        !std.mem.eql(u64, x.coverage_ends, y.coverage_ends) or !std.mem.eql(f32, x.keys, y.keys) or
                        !std.mem.eql(f32, x.values, y.values) or !std.mem.eql(f32, x.bias, y.bias))) return error.KvCompactionStateMismatch;
                }
            }
        }
    }

    pub fn commitCompaction(self: *Store, candidate: *?Owned, expected_generation: u64, limits: Limits) !void {
        if (candidate == &self.current) return error.InvalidKvCheckpoint;
        if (expected_generation != self.generation) return error.StaleKvCompaction;
        if (self.generation == std.math.maxInt(u64)) return error.StaleKvCompaction;
        const next = if (candidate.*) |value| value.state else return error.InvalidKvCheckpoint;
        try self.validateCompaction(next, expected_generation, limits);
        if (self.current) |*old| old.deinit();
        self.current = candidate.*;
        candidate.* = null;
        self.generation += 1;
    }
};

const test_identity: Identity = .{ .weights = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4) };
const test_head: Head = .{ .dim = 1, .compacted_until = 2, .positions = &.{0}, .coverage_ends = &.{2}, .keys = &.{0}, .values = &.{3}, .bias = &.{0.6931472} };
const test_local: Head = .{ .dim = 1, .positions = &.{1}, .coverage_ends = &.{2}, .keys = &.{2}, .values = &.{4}, .bias = &.{0} };
const test_state: State = .{ .identity = test_identity, .logical_length = 2, .tokens = &.{ 0, 1 }, .logits = &.{ 0.5, -0.5 }, .layer_donors = &.{ 0, 1, 0 }, .donors = &.{ .{ .layer = 0, .heads = &.{test_head} }, .{ .layer = 1, .window = 1, .heads = &.{test_local} } } };

test "attention_match checkpoint round trip preserves logical state and shared donors" {
    const bytes = try encode(std.testing.allocator, test_state, .{});
    defer std.testing.allocator.free(bytes);
    try std.testing.expectEqualStrings("ANTFLYKV", bytes[0..8]);
    var restored = try decode(std.testing.allocator, bytes, test_identity, .{});
    defer restored.deinit();
    const again = try encode(std.testing.allocator, restored.state, .{});
    defer std.testing.allocator.free(again);
    try std.testing.expectEqualSlices(u8, bytes, again);
    try std.testing.expectEqual(@as(u64, 2), restored.state.logical_length);
    try std.testing.expect(!restored.state.donors[0].heads[0].visible(0, 0));
    try std.testing.expect(restored.state.donors[0].heads[0].visible(0, 1));
}

test "attention_match checkpoint rejects corruption truncation wrong model and allocation limits" {
    const bytes = try encode(std.testing.allocator, test_state, .{});
    defer std.testing.allocator.free(bytes);
    for (0..bytes.len) |length| try std.testing.expectError(error.InvalidKvCheckpoint, decode(std.testing.allocator, bytes[0..length], test_identity, .{}));
    var wrong = test_identity;
    wrong.weights[0] ^= 1;
    try std.testing.expectError(error.KvCheckpointModelMismatch, decode(std.testing.allocator, bytes, wrong, .{}));
    try std.testing.expectError(error.KvCheckpointLimit, decode(std.testing.allocator, bytes, test_identity, .{ .max_state_bytes = 1 }));
    bytes[bytes.len - 1] ^= 1;
    try std.testing.expectError(error.KvCheckpointChecksum, decode(std.testing.allocator, bytes, test_identity, .{}));
}

test "attention_match checkpoint rejects fitted local rows and invalid coverage" {
    var bad = test_local;
    bad.bias = &.{1};
    var state = test_state;
    state.donors = &.{ .{ .layer = 0, .heads = &.{test_head} }, .{ .layer = 1, .window = 1, .heads = &.{bad} } };
    try std.testing.expectError(error.InvalidKvCheckpoint, state.validate(.{}));
    bad = test_head;
    bad.coverage_ends = &.{3};
    state.donors = &.{ .{ .layer = 0, .heads = &.{bad} }, .{ .layer = 1, .window = 1, .heads = &.{test_local} } };
    try std.testing.expectError(error.InvalidKvCheckpoint, state.validate(.{}));
}

test "attention_match checkpoint checks hostile counts and nonfinite payloads even with valid checksum" {
    const bytes = try encode(std.testing.allocator, test_state, .{});
    defer std.testing.allocator.free(bytes);
    // The last wire value is the local row's bias. A checksum is not a promise
    // of semantic validity: untrusted uploads can recompute it themselves.
    std.mem.writeInt(u32, bytes[bytes.len - 4 ..][0..4], @bitCast(std.math.inf(f32)), .little);
    @memcpy(bytes[digest_offset..header_bytes], &checksum(bytes));
    try std.testing.expectError(error.InvalidKvCheckpoint, decode(std.testing.allocator, bytes, test_identity, .{}));
    std.mem.writeInt(u32, bytes[bytes.len - 4 ..][0..4], 0, .little);
    std.mem.writeInt(u32, bytes[header_bytes + 8 ..][0..4], std.math.maxInt(u32), .little);
    @memcpy(bytes[digest_offset..header_bytes], &checksum(bytes));
    try std.testing.expectError(error.InvalidKvCheckpoint, decode(std.testing.allocator, bytes, test_identity, .{}));
}

test "attention_match checkpoint exact suffix cannot contain gaps or fitted rows" {
    var bad = test_head;
    bad.compacted_until = 0;
    var state = test_state;
    state.donors = &.{ .{ .layer = 0, .heads = &.{bad} }, .{ .layer = 1, .window = 1, .heads = &.{test_local} } };
    try std.testing.expectError(error.InvalidKvCheckpoint, state.validate(.{}));
    bad = test_head;
    bad.positions = &.{1};
    bad.bias = &.{0};
    bad.compacted_until = 0;
    state.donors = &.{ .{ .layer = 0, .heads = &.{bad} }, .{ .layer = 1, .window = 1, .heads = &.{test_local} } };
    try std.testing.expectError(error.InvalidKvCheckpoint, state.validate(.{}));
}

test "attention_match cache transaction rejects stale work without dropping either state" {
    const bytes = try encode(std.testing.allocator, test_state, .{});
    defer std.testing.allocator.free(bytes);
    var store: Store = .{};
    defer store.deinit();
    var candidate: ?Owned = try decode(std.testing.allocator, bytes, test_identity, .{});
    defer if (candidate) |*owned| owned.deinit();
    try store.commitCompaction(&candidate, 0, .{});
    try std.testing.expect(candidate == null);
    candidate = try decode(std.testing.allocator, bytes, test_identity, .{});
    try std.testing.expectError(error.StaleKvCompaction, store.commitCompaction(&candidate, 0, .{}));
    try std.testing.expect(candidate != null and store.current != null);
    try store.commitCompaction(&candidate, 1, .{});
    try std.testing.expectEqual(@as(u64, 2), store.generation);
}

fn allocationFailure(allocator: std.mem.Allocator, bytes: []const u8) !void {
    var owned = try decode(allocator, bytes, test_identity, .{});
    defer owned.deinit();
}
test "attention_match checkpoint import unwinds every partial allocation" {
    const bytes = try encode(std.testing.allocator, test_state, .{});
    defer std.testing.allocator.free(bytes);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailure, .{bytes});
}

const TestSink = struct {
    bytes: []u8,
    used: usize = 0,
    largest_write: usize = 0,
    fail: bool = false,
    pub fn writeAll(self: *@This(), data: []const u8) !void {
        if (self.fail) return error.ExportFailed;
        if (data.len > self.bytes.len - self.used) return error.NoSpaceLeft;
        @memcpy(self.bytes[self.used..][0..data.len], data);
        self.used += data.len;
        self.largest_write = @max(self.largest_write, data.len);
    }
};

test "attention_match bounded export matches v1 encoding and propagates cancellation and sink errors" {
    const a = std.testing.allocator;
    const expected = try encode(a, test_state, .{});
    defer a.free(expected);
    const output = try a.alloc(u8, expected.len);
    defer a.free(output);
    var sink: TestSink = .{ .bytes = output };
    try exportTo(test_state, .{}, &sink, .{});
    try std.testing.expectEqualSlices(u8, expected, output[0..sink.used]);
    try std.testing.expect(sink.largest_write <= 16 * 1024);
    var restored = try decode(a, output, test_identity, .{});
    defer restored.deinit();
    sink.fail = true;
    try std.testing.expectError(error.ExportFailed, exportTo(test_state, .{}, &sink, .{}));
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.CompactionCancelled;
        }
    };
    try std.testing.expectError(error.CompactionCancelled, exportTo(test_state, .{}, &sink, .{ .check_fn = Cancel.check }));
}

test "attention_match export flushes bounded chunks and can cancel before publishing the header" {
    const a = std.testing.allocator;
    const logits = try a.alloc(f32, 20000);
    defer a.free(logits);
    @memset(logits, 0.25);
    var state = test_state;
    state.logits = logits;
    const expected = try encode(a, state, .{});
    defer a.free(expected);
    const output = try a.alloc(u8, expected.len);
    defer a.free(output);
    var sink: TestSink = .{ .bytes = output };
    try exportTo(state, .{}, &sink, .{});
    try std.testing.expectEqualSlices(u8, expected, output);
    try std.testing.expectEqual(@as(usize, 16384), sink.largest_write);
    const Cancel = struct {
        count: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.count += 1;
            if (self.count == 3) return error.CompactionCancelled;
        }
    };
    var cancel: Cancel = .{};
    sink.used = 0;
    try std.testing.expectError(error.CompactionCancelled, exportTo(state, .{}, &sink, .{ .context = &cancel, .check_fn = Cancel.check }));
    try std.testing.expectEqual(@as(usize, 0), sink.used);
}

test "checkpoint streaming keeps little-endian u64 bytes across an unaligned flush boundary" {
    const Sink = struct {
        bytes: [16 * 1024 + 16]u8 = undefined,
        used: usize = 0,
        fn writeAll(self: *@This(), bytes: []const u8) !void {
            @memcpy(self.bytes[self.used..][0..bytes.len], bytes);
            self.used += bytes.len;
        }
    };
    var sink: Sink = .{};
    var writer: StreamEncoder(*Sink) = .{ .sink = &sink, .control = .{}, .hashing = false };
    const prefix: [4095]u32 = @splat(0);
    try writer.slice(u32, &prefix);
    try writer.slice(u64, &.{ 0x0807060504030201, 0x1817161514131211 });
    try writer.flush();
    try std.testing.expectEqual(@as(usize, 16 * 1024 + 12), sink.used);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18 }, sink.bytes[16 * 1024 - 4 ..][0..16]);
}

test "checkpoint v2 streams once with independently checked file and payload digests" {
    const a = std.testing.allocator;
    const logits = try a.alloc(f32, 20000);
    defer a.free(logits);
    @memset(logits, 0.25);
    var state = test_state;
    state.logits = logits;
    const expected = try encodeV2(a, state, .{});
    defer a.free(expected);
    const output = try a.alloc(u8, expected.len);
    defer a.free(output);
    var sink: TestSink = .{ .bytes = output };
    const result = try exportV2(state, .{}, &sink, .{});
    try std.testing.expectEqual(expected.len, result.bytes);
    try std.testing.expectEqualSlices(u8, expected, output[0..sink.used]);
    try std.testing.expect(sink.largest_write <= 16384);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(output, &digest, .{});
    try std.testing.expectEqualSlices(u8, &digest, &result.file_sha256);
    try std.testing.expectEqualSlices(u8, output[output.len - 32 ..], &result.integrity_sha256);
    var restored = try decode(a, output, test_identity, .{});
    defer restored.deinit();
    const legacy = try encode(a, restored.state, .{});
    defer a.free(legacy);
    const original = try encode(a, state, .{});
    defer a.free(original);
    try std.testing.expectEqualSlices(u8, original, legacy);
    sink.used = 0;
    sink.fail = true;
    try std.testing.expectError(error.ExportFailed, exportV2(state, .{}, &sink, .{}));
    sink.fail = false;
    const Cancel = struct {
        sink: *TestSink,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.sink.used > 0) return error.CompactionCancelled;
        }
    };
    var cancel: Cancel = .{ .sink = &sink };
    try std.testing.expectError(error.CompactionCancelled, exportV2(state, .{}, &sink, .{ .context = &cancel, .check_fn = Cancel.check }));
    try std.testing.expect(sink.used > 0 and sink.used < output.len);
    try std.testing.expectError(error.InvalidKvCheckpoint, decode(a, output[0..sink.used], test_identity, .{}));
}

test "checkpoint transport sizing validates state and limits before publication" {
    const a = std.testing.allocator;
    const size = try encodedSizeWithControl(test_state, .{}, .{});
    const v1 = try encode(a, test_state, .{});
    defer a.free(v1);
    const v2 = try encodeV2(a, test_state, .{});
    defer a.free(v2);
    try std.testing.expectEqual(v1.len, size);
    try std.testing.expectEqual(v2.len, size);
    try std.testing.expectError(error.KvCheckpointLimit, encodedSizeWithControl(test_state, .{ .max_file_bytes = size - 1 }, .{}));
    try std.testing.expectError(error.KvCheckpointLimit, encodedSizeWithControl(test_state, .{ .max_state_bytes = 0 }, .{}));
    var invalid = test_state;
    invalid.logits = &.{std.math.nan(f32)};
    try std.testing.expectError(error.InvalidKvCheckpoint, encodedSizeWithControl(invalid, .{}, .{}));
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.CompactionCancelled;
        }
    };
    try std.testing.expectError(error.CompactionCancelled, encodedSizeWithControl(test_state, .{}, .{ .check_fn = Cancel.check }));
}

fn v2DecodeAllocationFailure(allocator: std.mem.Allocator, bytes: []const u8) !void {
    var result = try decode(allocator, bytes, test_identity, .{});
    defer result.deinit();
}
test "checkpoint v2 rejects truncation corruption model mismatch and allocation limits" {
    const a = std.testing.allocator;
    const bytes = try encodeV2(a, test_state, .{});
    defer a.free(bytes);
    for (0..bytes.len) |length| try std.testing.expectError(error.InvalidKvCheckpoint, decode(a, bytes[0..length], test_identity, .{}));
    var wrong = test_identity;
    wrong.weights[0] ^= 1;
    try std.testing.expectError(error.KvCheckpointModelMismatch, decode(a, bytes, wrong, .{}));
    try std.testing.expectError(error.KvCheckpointLimit, decode(a, bytes, test_identity, .{ .max_state_bytes = 1 }));
    try std.testing.checkAllAllocationFailures(a, v2DecodeAllocationFailure, .{bytes});
    for ([_]usize{ digest_offset, bytes.len - 33, bytes.len - 1 }) |index| {
        bytes[index] ^= 1;
        try std.testing.expectError(error.KvCheckpointChecksum, decode(a, bytes, test_identity, .{}));
        bytes[index] ^= 1;
    }
}
const CancelAt = struct {
    calls: usize = 0,
    at: usize = std.math.maxInt(usize),
    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.calls += 1;
        if (self.calls == self.at) return error.CompactionCancelled;
    }
    fn control(self: *@This()) Control {
        return .{ .context = self, .check_fn = check };
    }
};

test "checkpoint cancellable export preserves bytes and unwinds every cancellation boundary" {
    const a = std.testing.allocator;
    const expected = try encodeV2(a, test_state, .{});
    defer a.free(expected);
    var counter: CancelAt = .{};
    const actual = try encodeV2WithControl(a, test_state, .{}, counter.control());
    defer a.free(actual);
    try std.testing.expectEqualSlices(u8, expected, actual);
    try std.testing.expect(counter.calls > 10);
    for (1..counter.calls + 1) |at| {
        var cancel: CancelAt = .{ .at = at };
        try std.testing.expectError(error.CompactionCancelled, encodeV2WithControl(a, test_state, .{}, cancel.control()));
        try std.testing.expectEqual(at, cancel.calls);
    }
}

test "checkpoint v1 and v2 cancel during hashing copy and validation without leaking partial arrays" {
    const a = std.testing.allocator;
    const rows = 1025;
    const dim = 16;
    var positions: [rows]u64 = undefined;
    var ends: [rows]u64 = undefined;
    for (&positions, &ends, 0..) |*pos, *end, i| {
        pos.* = i;
        end.* = i + 1;
    }
    const tokens: [rows]u32 = @splat(0);
    const keys: [rows * dim]f32 = @splat(0.5);
    const values: [rows * dim]f32 = @splat(-2);
    const bias: [rows]f32 = @splat(0);
    const head: Head = .{ .dim = dim, .positions = &positions, .coverage_ends = &ends, .keys = &keys, .values = &values, .bias = &bias };
    const state: State = .{ .identity = test_identity, .logical_length = rows, .tokens = &tokens, .logits = &.{0.25}, .layer_donors = &.{0}, .donors = &.{.{ .layer = 0, .heads = &.{head} }} };
    for ([_]bool{ false, true }) |v2| {
        const bytes = if (v2) try encodeV2(a, state, .{}) else try encode(a, state, .{});
        defer a.free(bytes);
        var counter: CancelAt = .{};
        var decoded = try decodeWithControl(a, bytes, test_identity, .{}, counter.control());
        defer decoded.deinit();
        try std.testing.expectEqualSlices(f32, &values, decoded.state.donors[0].heads[0].values);
        try std.testing.expect(counter.calls > 40);
        for (1..counter.calls + 1) |at| {
            var cancel: CancelAt = .{ .at = at };
            try std.testing.expectError(error.CompactionCancelled, decodeWithControl(a, bytes, test_identity, .{}, cancel.control()));
            try std.testing.expectEqual(at, cancel.calls);
        }
        var unchanged = try decode(a, bytes, test_identity, .{});
        defer unchanged.deinit();
        try std.testing.expectEqualSlices(u64, &positions, unchanged.state.donors[0].heads[0].positions);
        try std.testing.expectEqualSlices(f32, &keys, unchanged.state.donors[0].heads[0].keys);
    }
}

test "checkpoint replacement validation can cancel without changing owner" {
    const a = std.testing.allocator;
    const bytes = try encodeV2(a, test_state, .{});
    defer a.free(bytes);
    var store: Store = .{ .current = try decode(a, bytes, test_identity, .{}), .generation = 7 };
    defer store.deinit();
    const original_ptr = store.current.?.state.donors[0].heads[0].keys.ptr;
    const Cancel = struct {
        count: usize = 0,
        at: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.count += 1;
            if (self.count == self.at) return error.CompactionCancelled;
        }
    };
    var reference: Cancel = .{};
    try store.validateCompactionWithControl(test_state, 7, .{}, .{ .context = &reference, .check_fn = Cancel.check });
    for (1..reference.count + 1) |at| {
        var cancel: Cancel = .{ .at = at };
        try std.testing.expectError(error.CompactionCancelled, store.validateCompactionWithControl(test_state, 7, .{}, .{ .context = &cancel, .check_fn = Cancel.check }));
        try std.testing.expectEqual(@as(u64, 7), store.generation);
        try std.testing.expectEqual(original_ptr, store.current.?.state.donors[0].heads[0].keys.ptr);
    }
}

/// Decode exactly one bounded file from a reader with read([]u8)!usize. The
/// reader may contain following envelope data, which is left untouched. The
/// caller owns transport EOF/framing checks. No state is returned before its
/// checksum and semantic checks pass, and wire bytes are not buffered twice.
pub const Decoded = struct {
    checkpoint: Owned,
    encoding: Encoding,

    pub fn deinit(self: *Decoded) void {
        self.checkpoint.deinit();
        self.* = undefined;
    }
};

fn wireEncoding(wire_version: u32, flags: u32) !Encoding {
    return switch (wire_version) {
        1, 2 => .f32,
        3 => if (flags == 0) .f16 else if (flags == 1) .fp8 else error.UnsupportedKvCheckpoint,
        4 => .rotated4,
        else => error.UnsupportedKvCheckpoint,
    };
}

test "checkpoint wire provenance distinguishes release encodings" {
    try std.testing.expectEqual(Encoding.f32, try wireEncoding(1, 0));
    try std.testing.expectEqual(Encoding.f32, try wireEncoding(2, 0));
    try std.testing.expectEqual(Encoding.f16, try wireEncoding(3, 0));
    try std.testing.expectEqual(Encoding.fp8, try wireEncoding(3, 1));
    try std.testing.expectEqual(Encoding.rotated4, try wireEncoding(4, 0));
    try std.testing.expectError(error.UnsupportedKvCheckpoint, wireEncoding(3, 2));
}

pub fn decodeReaderWithEncodingAndControl(allocator: std.mem.Allocator, reader: anytype, file_length: usize, expected: Identity, limits: Limits, control: Control) !Decoded {
    try control.check();
    if (file_length > limits.max_file_bytes) return error.KvCheckpointLimit;
    if (file_length < header_bytes) return error.InvalidKvCheckpoint;
    var header: [digest_offset]u8 = undefined;
    try readCheckpointExact(reader, &header, control);
    if (!std.mem.eql(u8, header[0..8], magic)) return error.InvalidKvCheckpoint;
    var h: Decoder = .{ .bytes = &header, .allocator = allocator, .remaining_allocation = 0, .offset = 8, .control = control };
    const wire_version = try h.int(u32);
    const flags = try h.int(u32);
    if ((wire_version != 1 and wire_version != 2 and wire_version != 3 and wire_version != 4) or
        (wire_version < 3 and flags != 0) or (wire_version == 3 and flags > 1) or (wire_version == 4 and flags != 0)) return error.UnsupportedKvCheckpoint;
    if (try h.int(u64) != file_length - header_bytes) return error.InvalidKvCheckpoint;
    var identity: Identity = undefined;
    inline for (.{ "weights", "tokenizer", "template", "layout" }) |field| @memcpy(&@field(identity, field), try h.raw(32));
    if (!identity.eql(expected)) return error.KvCheckpointModelMismatch;
    var expected_digest: [32]u8 = undefined;
    if (wire_version == 1) try readCheckpointExact(reader, &expected_digest, control);
    var r: StreamingDecoder(@TypeOf(reader)) = .{
        .reader = reader,
        .allocator = allocator,
        .control = control,
        .remaining_wire_bytes = file_length - header_bytes,
        .remaining_allocation = limits.max_state_bytes,
    };
    r.hash.update(&header);
    var owned = try decodePayload(&r, allocator, identity, limits, wire_version, flags);
    errdefer owned.deinit();
    if (r.remaining_wire_bytes != 0) return error.InvalidKvCheckpoint;
    if (wire_version >= 2) try readCheckpointExact(reader, &expected_digest, control);
    const digest = r.hash.finalResult();
    if (!std.mem.eql(u8, &expected_digest, &digest)) return error.KvCheckpointChecksum;
    try owned.state.validateWithControl(limits, control);
    try control.check();
    return .{ .checkpoint = owned, .encoding = try wireEncoding(wire_version, flags) };
}

pub fn decodeReaderWithControl(allocator: std.mem.Allocator, reader: anytype, file_length: usize, expected: Identity, limits: Limits, control: Control) !Owned {
    var decoded = try decodeReaderWithEncodingAndControl(allocator, reader, file_length, expected, limits, control);
    const checkpoint = decoded.checkpoint;
    decoded = undefined;
    return checkpoint;
}

fn readCheckpointExact(reader: anytype, destination: []u8, control: Control) !void {
    var offset: usize = 0;
    while (offset < destination.len) {
        try control.check();
        const count = try reader.read(destination[offset..][0..@min(16384, destination.len - offset)]);
        if (count == 0 or count > @min(16384, destination.len - offset)) return error.InvalidKvCheckpoint;
        offset += count;
    }
}

fn StreamingDecoder(comptime Reader: type) type {
    return struct {
        reader: Reader,
        allocator: std.mem.Allocator,
        control: Control,
        remaining_wire_bytes: usize,
        remaining_allocation: usize,
        hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
        fn wireRemaining(self: *@This()) usize {
            return self.remaining_wire_bytes;
        }
        fn read(self: *@This(), bytes: []u8) !void {
            if (bytes.len > self.remaining_wire_bytes) return error.InvalidKvCheckpoint;
            var offset: usize = 0;
            while (offset < bytes.len) {
                const chunk = bytes[offset..][0..@min(16384, bytes.len - offset)];
                try readCheckpointExact(self.reader, chunk, self.control);
                self.hash.update(chunk);
                offset += chunk.len;
                self.remaining_wire_bytes -= chunk.len;
            }
        }
        fn int(self: *@This(), comptime T: type) !T {
            var bytes: [@sizeOf(T)]u8 = undefined;
            try self.read(&bytes);
            return std.mem.readInt(T, &bytes, .little);
        }
        fn alloc(self: *@This(), comptime T: type, count: usize) ![]T {
            try self.control.check();
            const size = try mul(count, @sizeOf(T));
            if (size > self.remaining_allocation) return error.KvCheckpointLimit;
            self.remaining_allocation -= size;
            return self.allocator.alloc(T, count);
        }
        fn slice(self: *@This(), comptime T: type, count: usize) ![]T {
            const bytes = try mul(count, @sizeOf(T));
            if (bytes > self.remaining_wire_bytes) return error.InvalidKvCheckpoint;
            const values = try self.alloc(T, count);
            errdefer self.allocator.free(values);
            try self.read(std.mem.sliceAsBytes(values));
            if (comptime @import("builtin").target.cpu.arch.endian() != .little) {
                for (values, 0..) |*value, i| {
                    if (i % 2048 == 0) try self.control.check();
                    const U = if (T == f32) u32 else T;
                    const bits = std.mem.readInt(U, std.mem.asBytes(value), .little);
                    value.* = if (T == f32) @bitCast(bits) else bits;
                }
            }
            return values;
        }
    };
}

const CheckpointChunkReader = struct {
    bytes: []const u8,
    offset: usize = 0,
    chunk: usize = 3,
    calls: usize = 0,
    maximum_requested: usize = 0,
    fail_at: usize = std.math.maxInt(usize),
    pub fn read(self: *@This(), output: []u8) !usize {
        self.calls += 1;
        if (self.calls == self.fail_at) return error.InjectedReadFailure;
        self.maximum_requested = @max(self.maximum_requested, output.len);
        const count = @min(@min(output.len, self.chunk), self.bytes.len - self.offset);
        @memcpy(output[0..count], self.bytes[self.offset..][0..count]);
        self.offset += count;
        return count;
    }
};

test "checkpoint streaming import preserves both wire versions and following envelope bytes" {
    const a = std.testing.allocator;
    for ([_]u32{ 1, 2 }) |v| {
        const encoded = if (v == 1) try encode(a, test_state, .{}) else try encodeV2(a, test_state, .{});
        defer a.free(encoded);
        const framed = try a.alloc(u8, encoded.len + 3);
        defer a.free(framed);
        @memcpy(framed[0..encoded.len], encoded);
        @memcpy(framed[encoded.len..], "END");
        for ([_]usize{ 1, 3, 17, 65536 }) |chunk| {
            var reader: CheckpointChunkReader = .{ .bytes = framed, .chunk = chunk };
            var restored = try decodeReaderWithControl(a, &reader, encoded.len, test_identity, .{}, .{});
            defer restored.deinit();
            try std.testing.expectEqual(encoded.len, reader.offset);
            const copy = try encode(a, restored.state, .{});
            defer a.free(copy);
            const expected = try encode(a, test_state, .{});
            defer a.free(expected);
            try std.testing.expectEqualSlices(u8, expected, copy);
            try std.testing.expect(reader.maximum_requested <= 16384);
        }
    }
}

test "checkpoint streaming import rejects every truncated prefix and bad trailer" {
    const a = std.testing.allocator;
    for ([_]u32{ 1, 2 }) |v| {
        const encoded = if (v == 1) try encode(a, test_state, .{}) else try encodeV2(a, test_state, .{});
        defer a.free(encoded);
        for (0..encoded.len) |length| {
            var reader: CheckpointChunkReader = .{ .bytes = encoded[0..length], .chunk = 17 };
            try std.testing.expectError(error.InvalidKvCheckpoint, decodeReaderWithControl(a, &reader, encoded.len, test_identity, .{}, .{}));
        }
        encoded[if (v == 1) digest_offset else encoded.len - 1] ^= 1;
        var corrupted: CheckpointChunkReader = .{ .bytes = encoded };
        try std.testing.expectError(error.KvCheckpointChecksum, decodeReaderWithControl(a, &corrupted, encoded.len, test_identity, .{}, .{}));
    }
}

fn checkpointStreamingAllocationFailure(a: std.mem.Allocator, encoded: []const u8) !void {
    var reader: CheckpointChunkReader = .{ .bytes = encoded };
    var restored = try decodeReaderWithControl(a, &reader, encoded.len, test_identity, .{}, .{});
    defer restored.deinit();
}

test "checkpoint streaming import unwinds allocations IO failures and cancellation at every boundary" {
    const a = std.testing.allocator;
    const Cancel = struct {
        calls: usize = 0,
        at: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == self.at) return error.CompactionCancelled;
        }
    };
    for ([_]u32{ 1, 2 }) |v| {
        const encoded = if (v == 1) try encode(a, test_state, .{}) else try encodeV2(a, test_state, .{});
        defer a.free(encoded);
        try std.testing.checkAllAllocationFailures(a, checkpointStreamingAllocationFailure, .{encoded});
        var reference: CheckpointChunkReader = .{ .bytes = encoded };
        var counter: Cancel = .{};
        var restored = try decodeReaderWithControl(a, &reference, encoded.len, test_identity, .{}, .{ .context = &counter, .check_fn = Cancel.check });
        restored.deinit();
        for (1..reference.calls + 1) |at| {
            var reader: CheckpointChunkReader = .{ .bytes = encoded, .fail_at = at };
            try std.testing.expectError(error.InjectedReadFailure, decodeReaderWithControl(a, &reader, encoded.len, test_identity, .{}, .{}));
        }
        for (1..counter.calls + 1) |at| {
            var reader: CheckpointChunkReader = .{ .bytes = encoded };
            var cancel: Cancel = .{ .at = at };
            try std.testing.expectError(error.CompactionCancelled, decodeReaderWithControl(a, &reader, encoded.len, test_identity, .{}, .{ .context = &cancel, .check_fn = Cancel.check }));
        }
    }
}

test "checkpoint streaming import bounds file state and model before publication" {
    const a = std.testing.allocator;
    const encoded = try encodeV2(a, test_state, .{});
    defer a.free(encoded);
    var reader: CheckpointChunkReader = .{ .bytes = encoded };
    try std.testing.expectError(error.KvCheckpointLimit, decodeReaderWithControl(a, &reader, encoded.len, test_identity, .{ .max_file_bytes = encoded.len - 1 }, .{}));
    try std.testing.expectEqual(@as(usize, 0), reader.calls);
    var wrong_identity = test_identity;
    wrong_identity.weights[0] ^= 1;
    try std.testing.expectError(error.KvCheckpointModelMismatch, decodeReaderWithControl(a, &reader, encoded.len, wrong_identity, .{}, .{}));
    try std.testing.expectEqual(@as(usize, digest_offset), reader.offset);
    reader.offset = 0;
    try std.testing.expectError(error.KvCheckpointLimit, decodeReaderWithControl(a, &reader, encoded.len, test_identity, .{ .max_state_bytes = 1 }, .{}));
    reader.offset = 0;
    try std.testing.expectError(error.InvalidKvCheckpoint, decodeReaderWithControl(a, &reader, encoded.len - 1, test_identity, .{}, .{}));
}

test "checkpoint streaming import bounds large array reads and unwinds a partial array" {
    const a = std.testing.allocator;
    const rows = 1025;
    const dim = 16;
    const positions = try a.alloc(u64, rows);
    defer a.free(positions);
    const ends = try a.alloc(u64, rows);
    defer a.free(ends);
    const values = try a.alloc(f32, rows * dim);
    defer a.free(values);
    const bias = try a.alloc(f32, rows);
    defer a.free(bias);
    const tokens = try a.alloc(u32, rows);
    defer a.free(tokens);
    for (positions, ends, 0..) |*position, *end, i| {
        position.* = i;
        end.* = i + 1;
    }
    for (values, 0..) |*value, i| value.* = if (i % 2 == 0) -@as(f32, 0.0) else @as(f32, @floatFromInt(i % 997));
    @memset(bias, 0);
    @memset(tokens, 0);
    const heads = [_]Head{.{ .dim = dim, .positions = positions, .coverage_ends = ends, .keys = values, .values = values, .bias = bias }};
    const donors = [_]Donor{.{ .heads = &heads }};
    const source: State = .{ .identity = test_identity, .logical_length = rows, .tokens = tokens, .logits = &.{0}, .layer_donors = &.{0}, .donors = &donors };
    const bytes = try encodeV2(a, source, .{});
    defer a.free(bytes);
    var reader: CheckpointChunkReader = .{ .bytes = bytes, .chunk = 8191 };
    var restored = try decodeReaderWithControl(a, &reader, bytes.len, test_identity, .{}, .{});
    defer restored.deinit();
    try std.testing.expectEqual(@as(usize, 16384), reader.maximum_requested);
    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(values), std.mem.sliceAsBytes(restored.state.donors[0].heads[0].values));
    try std.testing.expect(reader.calls > 22);
    var failure: CheckpointChunkReader = .{ .bytes = bytes, .chunk = 8191, .fail_at = 22 };
    try std.testing.expectError(error.InjectedReadFailure, decodeReaderWithControl(a, &failure, bytes.len, test_identity, .{}, .{}));
}

const codec_test_values: [24]f32 = .{ -0.0, 0.000000059604644775390625, 65504, -65504, 1.0625, 2, 3, 4 } ++ @as([8]f32, @splat(1.125)) ++ @as([8]f32, @splat(1.0625));
const codec_test_head: Head = .{ .dim = 8, .compacted_until = 2, .positions = &.{ 0, 1, 2 }, .coverage_ends = &.{ 1, 2, 3 }, .keys = &codec_test_values, .values = &codec_test_values, .bias = &.{ 0, 0.69, 0 }, .fp8 = .{ .first_row = 1, .rows = 1 } };
const codec_test_state: State = .{ .identity = test_identity, .logical_length = 3, .tokens = &.{ 0, 0, 0 }, .logits = &.{0.1234567}, .layer_donors = &.{0}, .donors = &.{.{ .heads = &.{codec_test_head} }} };

fn codecAllocationFailures(a: std.mem.Allocator, encoding: Encoding) !void {
    const bytes = try encodeWithEncoding(a, codec_test_state, .{}, encoding, .{});
    defer a.free(bytes);
    var reader: CheckpointChunkReader = .{ .bytes = bytes, .chunk = 3 };
    var restored = try decodeReaderWithControl(a, &reader, bytes.len, test_identity, .{}, .{});
    defer restored.deinit();
    var memory = try decode(a, bytes, test_identity, .{});
    defer memory.deinit();
}

test "checkpoint V3 exact f16 and validated FP8 preserve continuation data and reexport" {
    const a = std.testing.allocator;
    const old = try encodeV2(a, codec_test_state, .{});
    defer a.free(old);
    for ([_]Encoding{ .f16, .fp8 }) |encoding| {
        const bytes = try encodeWithEncoding(a, codec_test_state, .{}, encoding, .{});
        defer a.free(bytes);
        try std.testing.expect(bytes.len < old.len);
        try std.testing.expectEqual(old.len - 24 * 4 + 16 - @as(usize, if (encoding == .fp8) 16 else 0), bytes.len);
        var decoded = try decode(a, bytes, test_identity, .{});
        defer decoded.deinit();
        const head = decoded.state.donors[0].heads[0];
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(&codec_test_values), std.mem.sliceAsBytes(head.keys));
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(&codec_test_values), std.mem.sliceAsBytes(head.values));
        try std.testing.expectEqualSlices(f32, codec_test_state.logits, decoded.state.logits);
        try std.testing.expectEqualSlices(f32, codec_test_head.bias, head.bias);
        const again = try encodeWithEncoding(a, decoded.state, .{}, encoding, .{});
        defer a.free(again);
        try std.testing.expectEqualSlices(u8, bytes, again);
        for ([_]usize{ 1, 3, 8191 }) |chunk| {
            var reader: CheckpointChunkReader = .{ .bytes = bytes, .chunk = chunk };
            var streamed = try decodeReaderWithControl(a, &reader, bytes.len, test_identity, .{}, .{});
            defer streamed.deinit();
            try std.testing.expectEqualSlices(f32, head.keys, streamed.state.donors[0].heads[0].keys);
        }
        // Wire shrinkage must not weaken the independent expanded-state bound.
        var reader: CheckpointChunkReader = .{ .bytes = bytes };
        try std.testing.expectError(error.KvCheckpointLimit, decodeReaderWithControl(a, &reader, bytes.len, test_identity, .{ .max_state_bytes = try decoded.state.residentBytes() - 1 }, .{}));
        try std.testing.checkAllAllocationFailures(a, codecAllocationFailures, .{encoding});
    }
}

test "checkpoint V3 refuses new rounding and hostile quantization metadata" {
    const a = std.testing.allocator;
    var head = codec_test_head;
    var donor = codec_test_state.donors[0];
    donor.heads = @as(*const [1]Head, @ptrCast(&head));
    var state = codec_test_state;
    state.donors = @as(*const [1]Donor, @ptrCast(&donor));
    var keys = codec_test_values;
    head.keys = &keys;
    keys[8] = 1.0625;
    try std.testing.expectError(error.KvStoragePrecisionMismatch, encodeWithEncoding(a, state, .{}, .fp8, .{}));
    keys[8] = 0.1;
    try std.testing.expectError(error.KvStoragePrecisionMismatch, encodeWithEncoding(a, state, .{}, .f16, .{}));
    keys = codec_test_values;
    head.fp8.rows = 3;
    try std.testing.expectError(error.InvalidKvCheckpoint, encodeWithEncoding(a, state, .{}, .fp8, .{}));
    head = codec_test_head;
    donor.window = 3;
    try std.testing.expectError(error.InvalidKvCheckpoint, encodeWithEncoding(a, state, .{}, .fp8, .{}));
    const bytes = try encodeWithEncoding(a, codec_test_state, .{}, .fp8, .{});
    defer a.free(bytes);
    // Header (152), state counts (24), tokens/logits/mapping (20), donor (12),
    // head geometry (16), quantization (16), positions/ends (48), K/V then bias.
    const quantization_offset = 152 + 24 + 20 + 12 + 16;
    for ([_]usize{ quantization_offset, quantization_offset + 4, quantization_offset + 8, quantization_offset + 12, quantization_offset + 16 + 48 + 16 }) |offset| {
        const original = bytes[offset];
        bytes[offset] = if (offset >= quantization_offset + 16) 0x7f else 0xff;
        std.crypto.hash.sha2.Sha256.hash(bytes[0 .. bytes.len - 32], bytes[bytes.len - 32 ..][0..32], .{});
        var reader: CheckpointChunkReader = .{ .bytes = bytes, .chunk = 7 };
        try std.testing.expectError(error.InvalidKvCheckpoint, decodeReaderWithControl(a, &reader, bytes.len, test_identity, .{}, .{}));
        try std.testing.expectError(error.InvalidKvCheckpoint, decode(a, bytes, test_identity, .{}));
        bytes[offset] = original;
    }
}

test "checkpoint V3 streaming cancels and rejects every truncated file" {
    const a = std.testing.allocator;
    for ([_]Encoding{ .f16, .fp8 }) |encoding| {
        var export_counter: CancelAt = .{};
        const bytes = try encodeWithEncoding(a, codec_test_state, .{}, encoding, export_counter.control());
        defer a.free(bytes);
        for (1..export_counter.calls + 1) |at| {
            var cancel: CancelAt = .{ .at = at };
            try std.testing.expectError(error.CompactionCancelled, encodeWithEncoding(a, codec_test_state, .{}, encoding, cancel.control()));
        }
        for (0..bytes.len) |length| {
            var reader: CheckpointChunkReader = .{ .bytes = bytes[0..length], .chunk = 7 };
            try std.testing.expectError(error.InvalidKvCheckpoint, decodeReaderWithControl(a, &reader, bytes.len, test_identity, .{}, .{}));
        }
        var counter: CancelAt = .{};
        var reader: CheckpointChunkReader = .{ .bytes = bytes, .chunk = 7 };
        var restored = try decodeReaderWithControl(a, &reader, bytes.len, test_identity, .{}, counter.control());
        restored.deinit();
        for (1..counter.calls + 1) |at| {
            reader = .{ .bytes = bytes, .chunk = 7 };
            var cancel: CancelAt = .{ .at = at };
            try std.testing.expectError(error.CompactionCancelled, decodeReaderWithControl(a, &reader, bytes.len, test_identity, .{}, cancel.control()));
        }
    }
}

// V4 stores a canonical span table, untouched rotated4/int8 bytes, and exact
// FP16 rows. Expanded host arrays are charged separately and checked against
// the compressed representation; export never silently requantizes values.
fn validatePacked(head: Head, window: u32, control: Control) !void {
    try head.compressed.validate(head.positions.len, head.dim);
    if (head.compressed.spans.len == 0) return;
    if (window != 0 or head.fp8.rows != 0) return error.InvalidKvCheckpoint;
    var keys: [512]f32 = undefined;
    var values: [512]f32 = undefined;
    for (head.compressed.spans) |span| {
        for (0..span.rows) |i| {
            try control.check();
            const row = span.first + i;
            if (head.positions[row] >= head.compacted_until or head.coverage_ends[row] > head.compacted_until) return error.InvalidKvCheckpoint;
            try head.compressed.decodeRow(head.dim, span.offset + i, keys[0..head.dim], values[0..head.dim]);
            if (!std.mem.eql(u8, std.mem.sliceAsBytes(keys[0..head.dim]), std.mem.sliceAsBytes(head.keys[row * head.dim ..][0..head.dim])) or
                !std.mem.eql(u8, std.mem.sliceAsBytes(values[0..head.dim]), std.mem.sliceAsBytes(head.values[row * head.dim ..][0..head.dim]))) return error.InvalidKvCheckpoint;
        }
    }
}

fn readPackedHeader(r: anytype, head: *Head, nr: usize) !usize {
    head.compressed.version = try r.int(u32);
    const count = try r.int(u32);
    const rows = try r.int(u32);
    if (try r.int(u32) != 0 or count > nr or rows > nr or
        (count == 0 and (rows != 0 or head.compressed.version != 0)) or
        (count != 0 and (head.compressed.version != 1 or !@import("turboquant_rotated.zig").supported(head.dim)))) return error.InvalidKvCheckpoint;
    if (try mul(count, 12) > r.wireRemaining()) return error.InvalidKvCheckpoint;
    const spans = try r.alloc(packed_storage.Span, count);
    head.compressed.spans = spans;
    var end: usize = 0;
    var offset: usize = 0;
    for (spans, 0..) |*span, i| {
        span.* = .{ .first = try r.int(u32), .rows = try r.int(u32), .offset = try r.int(u32) };
        if (span.rows == 0 or span.first > nr or span.rows > nr - span.first or (i > 0 and span.first <= end) or span.offset != offset) return error.InvalidKvCheckpoint;
        end = @as(usize, span.first) + span.rows;
        offset += span.rows;
    }
    if (offset != rows) return error.InvalidKvCheckpoint;
    return rows;
}

fn readPackedValues(r: anytype, head: *Head, nr: usize, rows: usize) !void {
    const key_bytes = try mul(rows, @import("turboquant_rotated.zig").rowBytes(head.dim));
    const value_bytes = try mul(rows, packed_storage.valueBytes(head.dim));
    const exact_bytes = try mul(try mul(nr - rows, head.dim), 4);
    if (try add(try add(key_bytes, value_bytes), exact_bytes) > r.wireRemaining()) return error.InvalidKvCheckpoint;
    head.compressed.keys = try r.slice(u8, key_bytes);
    head.compressed.values = try r.slice(u8, value_bytes);
    try head.compressed.validate(nr, head.dim);
    const keys = try r.alloc(f32, try mul(nr, head.dim));
    head.keys = keys;
    const values = try r.alloc(f32, keys.len);
    head.values = values;
    for ([_][]f32{ keys, values }) |output| {
        // Exact rows are contiguous on the wire even when their destinations
        // are separated by compressed spans. Batch reads across these gaps:
        // per-row reads multiply transport and cancellation-probe overhead.
        // A refill checks cancellation and consumes at most 16 KiB, just like
        // the V3 reader. Never read into the following values/bias payload.
        var buffer: [16384]u8 = undefined;
        var buffered: usize = 0;
        var used: usize = 0;
        var remaining = try mul(nr - rows, head.dim);
        var runs = head.compressed.exactRuns(nr);
        while (runs.next()) |run| {
            var offset = run.first * head.dim;
            const end = offset + run.rows * head.dim;
            while (offset < end) {
                if (used == buffered) {
                    try r.control.check();
                    buffered = @min(remaining, buffer.len / 2);
                    used = 0;
                    try r.read(buffer[0 .. buffered * 2]);
                    remaining -= buffered;
                }
                const count = @min(end - offset, buffered - used);
                for (output[offset..][0..count], 0..) |*value, d|
                    value.* = @as(f16, @bitCast(std.mem.readInt(u16, buffer[(used + d) * 2 ..][0..2], .little)));
                offset += count;
                used += count;
            }
        }
    }
    for (head.compressed.spans) |span| for (0..span.rows) |i| {
        try r.control.check();
        const start = (span.first + i) * head.dim;
        try head.compressed.decodeRow(head.dim, span.offset + i, keys[start..][0..head.dim], values[start..][0..head.dim]);
    };
}

fn packedCheckpointFixture(a: std.mem.Allocator) !void {
    var keys: [6 * 64]f32 = @splat(0.25);
    var values: [6 * 64]f32 = @splat(2);
    var first = try (packed_storage.Packed{}).append(a, 1, 64, keys[64..128], values[64..128], .{});
    defer first.deinit(a);
    var compressed = try first.append(a, 3, 64, keys[192..256], values[192..256], .{});
    defer compressed.deinit(a);
    try compressed.decodeRow(64, 0, keys[64..128], values[64..128]);
    try compressed.decodeRow(64, 1, keys[192..256], values[192..256]);
    const head: Head = .{ .dim = 64, .compacted_until = 4, .positions = &.{ 0, 1, 2, 3, 4, 5 }, .coverage_ends = &.{ 1, 2, 3, 4, 5, 6 }, .keys = &keys, .values = &values, .bias = &.{ 0, 0, 0, 0, 0, 0 }, .compressed = compressed };
    const state: State = .{ .identity = test_identity, .logical_length = 6, .tokens = &.{ 0, 0, 0, 0, 0, 0 }, .logits = &.{1}, .layer_donors = &.{ 0, 0 }, .donors = &.{.{ .heads = &.{head} }} };
    const encoded = try encodeWithEncoding(a, state, .{}, .rotated4, .{});
    defer a.free(encoded);
    try std.testing.expectEqual(@as(u32, 4), std.mem.readInt(u32, encoded[8..12], .little));
    var restored = try decode(a, encoded, test_identity, .{});
    defer restored.deinit();
    const again = try encodeWithEncoding(a, restored.state, .{}, .rotated4, .{});
    defer a.free(again);
    try std.testing.expectEqualSlices(u8, encoded, again);
    try std.testing.expectEqualSlices(f32, &keys, restored.state.donors[0].heads[0].keys);
    try std.testing.expectEqualSlices(u8, compressed.keys, restored.state.donors[0].heads[0].compressed.keys);
    var clone = try cloneMetadata(a, state);
    defer clone.deinit();
    @constCast(clone.state.donors[0].heads)[0] = try cloneHead(a, head);
    try std.testing.expectEqualSlices(u8, compressed.values, clone.state.donors[0].heads[0].compressed.values);
    try checkpointStreamingAllocationFailure(a, encoded);
    for ([_]Encoding{ .f32, .f16, .fp8 }) |encoding|
        try std.testing.expectError(error.KvStoragePrecisionMismatch, encodedSize(state, .{}, encoding, .{}));
    try std.testing.expectError(error.KvCheckpointLimit, decode(std.testing.allocator, encoded, test_identity, .{ .max_state_bytes = (try state.residentBytes()) - 1 }));
    // Expanded host data cannot disagree with the authoritative stored bytes.
    keys[64] += 1;
    try std.testing.expectError(error.InvalidKvCheckpoint, state.validate(.{}));
    keys[64] -= 1;
}

test "rotated4 checkpoint preserves mixed spans exact rows and ownership through every allocation failure" {
    try packedCheckpointFixture(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, packedCheckpointFixture, .{});
}

test "rotated4 streaming exact rows cross bounded slabs and compressed gaps without consuming the next frame" {
    const a = std.testing.allocator;
    const rows = 80;
    const dim = 512;
    var keys: [rows * dim]f32 = undefined;
    var values: [rows * dim]f32 = undefined;
    var positions: [rows]u64 = undefined;
    var ends: [rows]u64 = undefined;
    const tokens: [rows]u32 = @splat(0);
    const bias: [rows]f32 = @splat(0);
    for ([_]enum { exact, mixed, compressed }{ .exact, .mixed, .compressed }) |kind| {
        for (&keys, &values, 0..) |*key, *value, i| {
            key.* = @as(f32, @floatFromInt(i % 31)) / 32;
            value.* = -@as(f32, @floatFromInt(i % 17)) / 16;
        }
        for (&positions, &ends, 0..) |*pos, *end, i| {
            pos.* = i;
            end.* = i + 1;
        }
        var all = try (packed_storage.Packed{}).append(a, 0, dim, &keys, &values, .{});
        defer all.deinit(a);
        var selected: [rows]bool = undefined;
        for (&selected, 0..) |*keep, row| keep.* = kind == .compressed or (kind == .mixed and row % 2 == 1);
        var compressed = try all.selectNew(a, 0, &selected, dim, .{});
        defer compressed.deinit(a);
        for (compressed.spans) |span| for (0..span.rows) |i| {
            const first = (span.first + i) * dim;
            try compressed.decodeRow(dim, span.offset + i, keys[first..][0..dim], values[first..][0..dim]);
        };
        const head: Head = .{ .dim = dim, .compacted_until = rows, .positions = &positions, .coverage_ends = &ends, .keys = &keys, .values = &values, .bias = &bias, .compressed = compressed };
        const state: State = .{ .identity = test_identity, .logical_length = rows, .tokens = &tokens, .logits = &.{1}, .layer_donors = &.{0}, .donors = &.{.{ .heads = &.{head} }} };
        const encoded = try encodeWithEncoding(a, state, .{}, .rotated4, .{});
        defer a.free(encoded);
        const framed = try a.alloc(u8, encoded.len + 3);
        defer a.free(framed);
        @memcpy(framed[0..encoded.len], encoded);
        @memcpy(framed[encoded.len..], "END");
        for ([_]usize{ 97, 16384 }) |chunk| {
            var reader: CheckpointChunkReader = .{ .bytes = framed, .chunk = chunk };
            var restored = try decodeReaderWithControl(a, &reader, encoded.len, test_identity, .{}, .{});
            defer restored.deinit();
            try std.testing.expectEqual(encoded.len, reader.offset);
            try std.testing.expect(reader.maximum_requested <= 16384);
            try std.testing.expectEqualSlices(f32, &keys, restored.state.donors[0].heads[0].keys);
            try std.testing.expectEqualSlices(f32, &values, restored.state.donors[0].heads[0].values);
            const again = try encodeWithEncoding(a, restored.state, .{}, .rotated4, .{});
            defer a.free(again);
            try std.testing.expectEqualSlices(u8, encoded, again);
        }
    }
}

test "rotated4 checkpoint rejects malformed span metadata and cancels streaming at every boundary" {
    const a = std.testing.allocator;
    var keys: [64]f32 = @splat(0);
    var values: [64]f32 = @splat(2);
    var compressed = try (packed_storage.Packed{}).append(a, 0, 64, &keys, &values, .{});
    defer compressed.deinit(a);
    try compressed.decodeRow(64, 0, &keys, &values);
    const head: Head = .{ .dim = 64, .compacted_until = 2, .positions = &.{0}, .coverage_ends = &.{2}, .bias = &.{0.5}, .keys = &keys, .values = &values, .compressed = compressed };
    const state: State = .{ .identity = test_identity, .logical_length = 2, .tokens = &.{ 0, 0 }, .logits = &.{0}, .layer_donors = &.{0}, .donors = &.{.{ .heads = &.{head} }} };
    const bytes = try encodeWithEncoding(a, state, .{}, .rotated4, .{});
    defer a.free(bytes);
    var counter: CancelAt = .{};
    var reader: CheckpointChunkReader = .{ .bytes = bytes };
    var restored = try decodeReaderWithControl(a, &reader, bytes.len, test_identity, .{}, counter.control());
    defer restored.deinit();
    for (1..counter.calls + 1) |at| {
        var cancel: CancelAt = .{ .at = at };
        reader = .{ .bytes = bytes };
        try std.testing.expectError(error.CompactionCancelled, decodeReaderWithControl(a, &reader, bytes.len, test_identity, .{}, cancel.control()));
    }
    for (0..bytes.len) |length| {
        reader = .{ .bytes = bytes[0..length] };
        try std.testing.expectError(error.InvalidKvCheckpoint, decodeReaderWithControl(a, &reader, bytes.len, test_identity, .{}, .{}));
    }
    // Recompute integrity to exercise structural validation, not just checksum.
    const packed_header = digest_offset + 24 + 16 + 12 + 16;
    for ([_]usize{ packed_header, packed_header + 4, packed_header + 8, packed_header + 12, packed_header + 16, packed_header + 20, packed_header + 24 }) |offset| {
        const previous = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        std.mem.writeInt(u32, bytes[offset..][0..4], std.math.maxInt(u32), .little);
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes[0 .. bytes.len - 32], &hash, .{});
        @memcpy(bytes[bytes.len - 32 ..], &hash);
        try std.testing.expectError(error.InvalidKvCheckpoint, decode(a, bytes, test_identity, .{}));
        reader = .{ .bytes = bytes };
        try std.testing.expectError(error.InvalidKvCheckpoint, decodeReaderWithControl(a, &reader, bytes.len, test_identity, .{}, .{}));
        std.mem.writeInt(u32, bytes[offset..][0..4], previous, .little);
    }
}
