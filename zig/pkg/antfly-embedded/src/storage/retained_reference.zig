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

//! Local retained afterimages: authenticated metadata and immutable-value leases.
//! This is a storage representation, never a new receiver protocol. A bounded
//! streaming adapter produces the established REF3/4/G + RFV5 transport on read.
const std = @import("std");
const payload = @import("document_payload.zig");
const wire = @import("retained_frame.zig");
const A = std.mem.Allocator;
pub const magic = "RFS1";
pub const Effect = struct {
    key: []const u8,
    value: ?[]const u8,
    timestamp: u64,
    manifest: ?payload.Manifest = null,
    offset: u32 = 0,
    pub fn valueLength(self: Effect) usize {
        return if (self.manifest) |m| m.logical_bytes else if (self.value) |v| v.len else 0;
    }
};
fn hash(raw: []const u8) [32]u8 {
    var d: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &d, .{});
    return d;
}
pub fn isRecord(raw: []const u8) bool {
    return std.mem.startsWith(u8, raw, magic);
}
pub fn encode(a: A, sequence: u64, direct: bool, graph: bool, effects: []const Effect) ![]u8 {
    if (effects.len == 0 or effects.len > wire.max_effects or (graph and !direct)) return error.RetainedEffectsCorrupt;
    var size: usize = 49;
    for (effects) |effect| size = try std.math.add(usize, size, 17 + effect.key.len + if (effect.value) |v| v.len else @as(usize, 0));
    // References are bounded metadata. Large inline afterimages retain the
    // established chunked path instead of making one enormous local record.
    if (size > 16 * 1024 * 1024) return error.RetainedEffectsFull;
    const out = try a.alloc(u8, size);
    errdefer a.free(out);
    @memcpy(out[0..4], magic);
    std.mem.writeInt(u64, out[4..12], sequence, .little);
    std.mem.writeInt(u32, out[12..16], @intCast(effects.len), .little);
    out[16] = @as(u8, @intFromBool(direct)) | (@as(u8, @intFromBool(graph)) << 1);
    var pos: usize = 17;
    for (effects) |effect| {
        std.mem.writeInt(u32, out[pos..][0..4], @intCast(effect.key.len), .little);
        std.mem.writeInt(u32, out[pos + 4 ..][0..4], if (effect.value) |v| @intCast(v.len) else std.math.maxInt(u32), .little);
        std.mem.writeInt(u64, out[pos + 8 ..][0..8], effect.timestamp, .little);
        out[pos + 16] = @intFromBool(effect.manifest != null);
        pos += 17;
        @memcpy(out[pos..][0..effect.key.len], effect.key);
        pos += effect.key.len;
        if (effect.value) |v| {
            @memcpy(out[pos..][0..v.len], v);
            pos += v.len;
        }
    }
    @memcpy(out[pos..][0..32], &hash(out[0..pos]));
    return out;
}
pub const Plan = struct {
    sequence: u64,
    direct: bool,
    graph: bool,
    effects: []Effect,
    total: u32,
    footer: [32]u8 = @splat(0),
    pub fn decode(a: A, raw: []const u8, sequence: u64) !Plan {
        if (raw.len < 49 or !isRecord(raw) or std.mem.readInt(u64, raw[4..12], .little) != sequence or
            raw[16] > 3 or raw[16] == 2 or !std.mem.eql(u8, raw[raw.len - 32 ..], &hash(raw[0 .. raw.len - 32]))) return error.RetainedEffectsCorrupt;
        const count = std.mem.readInt(u32, raw[12..16], .little);
        if (count == 0 or count > wire.max_effects) return error.RetainedEffectsCorrupt;
        const effects = try a.alloc(Effect, count);
        errdefer a.free(effects);
        var initialized: usize = 0;
        errdefer for (effects[0..initialized]) |effect| if (effect.manifest) |m| a.free(m.parts);
        var pos: usize = 17;
        var offset: usize = 16;
        for (effects, 0..) |*effect, index| {
            if (raw.len - 32 -| pos < 17) return error.RetainedEffectsCorrupt;
            const kl = std.mem.readInt(u32, raw[pos..][0..4], .little);
            const vl = std.mem.readInt(u32, raw[pos + 4 ..][0..4], .little);
            const ts = std.mem.readInt(u64, raw[pos + 8 ..][0..8], .little);
            const tag = raw[pos + 16];
            pos += 17;
            if (kl == 0 or kl > raw.len - 32 -| pos or tag > 1) return error.RetainedEffectsCorrupt;
            const key = raw[pos..][0..kl];
            pos += kl;
            if (index != 0 and std.mem.order(u8, effects[index - 1].key, key) != .lt) return error.RetainedEffectsCorrupt;
            var value: ?[]const u8 = null;
            if (vl != std.math.maxInt(u32)) {
                if (vl > raw.len - 32 -| pos) return error.RetainedEffectsCorrupt;
                value = raw[pos..][0..vl];
                pos += vl;
            } else if (tag != 0 or ts != 0) return error.RetainedEffectsCorrupt;
            if (tag == 1 and !@import("internal_keys.zig").isPrimaryDocumentKey(key)) return error.RetainedEffectsCorrupt;
            effect.* = .{ .key = key, .value = value, .timestamp = ts, .offset = @intCast(offset) };
            if (tag == 1) effect.manifest = try payload.Manifest.decode(a, value.?);
            initialized += 1;
            if (effect.valueLength() > wire.max_logical_bytes or kl > wire.max_logical_bytes) return error.RetainedEffectsFull;
            offset = try std.math.add(usize, offset, 16 + @as(usize, kl) + effect.valueLength());
            if (offset > wire.max_logical_bytes - 32) return error.RetainedEffectsFull;
        }
        if (pos != raw.len - 32) return error.RetainedEffectsCorrupt;
        return .{ .sequence = sequence, .direct = raw[16] & 1 != 0, .graph = raw[16] & 2 != 0, .effects = effects, .total = @intCast(offset + 32) };
    }
    pub fn deinit(self: Plan, a: A) void {
        for (self.effects) |effect| if (effect.manifest) |m| a.free(m.parts);
        a.free(self.effects);
    }
    fn copyPiece(offset: usize, output: []u8, position: usize, bytes: []const u8) void {
        const start = @max(offset, position);
        const end = @min(offset + output.len, position + bytes.len);
        if (start < end) @memcpy(output[start - offset ..][0 .. end - start], bytes[start - position ..][0 .. end - start]);
    }
    pub fn readInto(self: Plan, reader: anytype, offset: usize, output: []u8) !void {
        if (offset > self.total or output.len > self.total - offset) return error.RetainedEffectsCursorMismatch;
        var header: [16]u8 = undefined;
        @memcpy(header[0..4], if (self.graph) "REFG" else if (self.direct) "REF4" else "REF3");
        std.mem.writeInt(u64, header[4..12], self.sequence, .little);
        std.mem.writeInt(u32, header[12..16], @intCast(self.effects.len), .little);
        copyPiece(offset, output, 0, &header);
        // Binary search skips effects preceding a resumed chunk.
        var low: usize = 0;
        var high = self.effects.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (self.effects[mid].offset <= offset) low = mid + 1 else high = mid;
        }
        var index = low -| 1;
        while (index < self.effects.len) : (index += 1) {
            const e = self.effects[index];
            const start: usize = e.offset;
            if (start >= offset + output.len) break;
            var eh: [16]u8 = undefined;
            std.mem.writeInt(u32, eh[0..4], @intCast(e.key.len), .little);
            std.mem.writeInt(u32, eh[4..8], if (e.value != null) @intCast(e.valueLength()) else std.math.maxInt(u32), .little);
            std.mem.writeInt(u64, eh[8..16], e.timestamp, .little);
            copyPiece(offset, output, start, &eh);
            copyPiece(offset, output, start + 16, e.key);
            const vs = start + 16 + e.key.len;
            if (e.manifest) |m| {
                const l = @max(offset, vs);
                const r = @min(offset + output.len, vs + m.logical_bytes);
                if (l < r) try m.readInto(reader, e.key, l - vs, output[l - offset ..][0 .. r - l]);
            } else if (e.value) |v| copyPiece(offset, output, vs, v);
        }
        copyPiece(offset, output, self.total - 32, &self.footer);
    }
    const ValueVerifier = struct {
        const Span = struct { begin: usize, end: usize, digest: wire.Digest };
        plan: *const Plan,
        effect_index: usize = 0,
        part_index: usize = 0,
        part_offset: usize = 0,
        active: ?Span = null,
        digest: std.crypto.hash.sha2.Sha256 = .init(.{}),
        fn next(self: *@This()) ?Span {
            while (self.effect_index < self.plan.effects.len) {
                const effect = self.plan.effects[self.effect_index];
                if (effect.manifest) |manifest| while (self.part_index < manifest.parts.len) {
                    const part = manifest.parts[self.part_index];
                    const begin = effect.offset + 16 + effect.key.len + self.part_offset;
                    self.part_index += 1;
                    const length = switch (part) {
                        .blob => |blob| blob.length,
                        .inline_json => |value| value.len,
                    };
                    self.part_offset += length;
                    if (part == .blob) return .{ .begin = begin, .end = begin + length, .digest = part.blob.digest };
                };
                self.effect_index += 1;
                self.part_index = 0;
                self.part_offset = 0;
            }
            return null;
        }
        fn observe(self: *@This(), offset: usize, bytes: []const u8) !void {
            while (self.active) |span| {
                if (span.begin >= offset + bytes.len) break;
                const begin = @max(offset, span.begin);
                const end = @min(offset + bytes.len, span.end);
                if (begin >= end) return error.InvalidDocumentManifest;
                self.digest.update(bytes[begin - offset ..][0 .. end - begin]);
                if (end != span.end) break;
                if (!std.mem.eql(u8, &self.digest.finalResult(), &span.digest)) return error.InvalidDocumentManifest;
                self.digest = .init(.{});
                self.active = self.next();
            }
        }
    };
    /// One streaming pass computes the actual legacy byte hashes. It runs on
    /// consumption, never on the metadata write's latency or WAL path.
    pub fn transport(self: *Plan, a: A, reader: anytype, scratch: []u8) ![]u8 {
        if (scratch.len < wire.chunk_bytes) return error.InvalidBatch;
        const count = try std.math.divCeil(u32, self.total, wire.chunk_bytes);
        const hashes = try a.alloc(wire.Digest, count);
        defer a.free(hashes);
        const offsets = try a.alloc(u32, self.effects.len);
        defer a.free(offsets);
        for (self.effects, offsets) |e, *o| o.* = e.offset;
        var h = std.crypto.hash.sha2.Sha256.init(.{});
        var verifier: ValueVerifier = .{ .plan = self };
        verifier.active = verifier.next();
        var pos: usize = 0;
        var ordinal: usize = 0;
        while (self.total - 32 - pos >= wire.chunk_bytes) {
            try self.readInto(reader, pos, scratch[0..wire.chunk_bytes]);
            try verifier.observe(pos, scratch[0..wire.chunk_bytes]);
            h.update(scratch[0..wire.chunk_bytes]);
            hashes[ordinal] = hash(scratch[0..wire.chunk_bytes]);
            ordinal += 1;
            pos += wire.chunk_bytes;
        }
        const tail = self.total - 32 - pos;
        try self.readInto(reader, pos, scratch[0..tail]);
        try verifier.observe(pos, scratch[0..tail]);
        if (verifier.active != null) return error.InvalidDocumentManifest;
        h.update(scratch[0..tail]);
        self.footer = h.peek();
        h.update(&self.footer);
        // The footer can cross a chunk boundary. Finish the pending chunk and
        // at most one final footer-only chunk without retaining prior chunks.
        const first = @min(@as(usize, 32), wire.chunk_bytes - tail);
        @memcpy(scratch[tail..][0..first], self.footer[0..first]);
        hashes[ordinal] = hash(scratch[0 .. tail + first]);
        ordinal += 1;
        if (first < 32) {
            hashes[ordinal] = hash(self.footer[first..]);
            ordinal += 1;
        }
        if (ordinal != count) return error.RetainedEffectsCorrupt;
        return (wire.Descriptor{ .sequence = self.sequence, .total = self.total, .direct_vectors = self.direct, .graph_artifacts = self.graph, .logical_digest = h.finalResult(), .payload_checksum = self.footer, .chunk_hashes = hashes, .effect_offsets = offsets }).encodeAlloc(a);
    }
    pub fn pin(self: Plan, a: A, txn: anytype) !u64 {
        var charged: u64 = 0;
        for (self.effects) |effect| if (effect.manifest) |m| {
            const values = try payload.immutableValues(a, m);
            defer a.free(values);
            charged = try std.math.add(u64, charged, try payload.ownership.acquirePin(a, txn, effect.key, values));
        };
        return charged;
    }
    pub fn unpin(self: Plan, a: A, txn: anytype) !u64 {
        var released: u64 = 0;
        for (self.effects) |effect| if (effect.manifest) |m| {
            const values = try payload.immutableValues(a, m);
            defer a.free(values);
            released = try std.math.add(u64, released, try payload.ownership.releasePin(a, txn, effect.key, values));
        };
        return released;
    }
};

test "document payload retained transport hashes footer boundary crossings without full-frame scratch" {
    const a = std.testing.allocator;
    const Reader = struct {
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.UnexpectedRead;
        }
    };
    var reader: Reader = .{};
    const key = "\x01row\x10";
    const scratch = try a.alloc(u8, wire.chunk_bytes);
    defer a.free(scratch);
    for ([_]usize{ 0, 1, 16, 31, 32, 33, 64 }) |gap| {
        const value = try a.alloc(u8, wire.chunk_bytes - 16 - 16 - key.len - gap);
        defer a.free(value);
        @memset(value, 'x');
        const encoded = try encode(a, 7, false, false, &.{.{ .key = key, .value = value, .timestamp = 123 }});
        defer a.free(encoded);
        var plan = try Plan.decode(a, encoded, 7);
        defer plan.deinit(a);
        const descriptor = try plan.transport(a, &reader, scratch);
        defer a.free(descriptor);
        const complete = try a.alloc(u8, plan.total);
        defer a.free(complete);
        try plan.readInto(&reader, 0, complete);
        try std.testing.expectEqualSlices(u8, &hash(complete[0 .. complete.len - 32]), &plan.footer);
        const Dummy = struct {
            fn read(_: *anyopaque, _: u64, _: u32, _: []u8) !usize {
                return error.UnexpectedRead;
            }
        };
        const view = try wire.View.fromDescriptor(descriptor, 7, .{ .context = @ptrFromInt(1), .read_chunk = Dummy.read });
        try std.testing.expectEqualSlices(u8, &hash(complete), &view.logical_digest);
        for (0..view.chunk_count) |ordinal| {
            const begin = ordinal * wire.chunk_bytes;
            try std.testing.expectEqualSlices(u8, &hash(complete[begin..@min(complete.len, begin + wire.chunk_bytes)]), &try view.chunkHashAt(@intCast(ordinal)));
        }
        const damaged = try a.dupe(u8, encoded);
        defer a.free(damaged);
        damaged[16] = 4;
        @memcpy(damaged[damaged.len - 32 ..], &hash(damaged[0 .. damaged.len - 32]));
        try std.testing.expectError(error.RetainedEffectsCorrupt, Plan.decode(a, damaged, 7));
    }
}
