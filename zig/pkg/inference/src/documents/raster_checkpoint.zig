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

//! Raster frames keep scalar JSON metadata small and encode measured-cost
//! receipts as fixed-width records. Count and frame bounds precede allocation.
const std = @import("std");
const c = @import("contract.zig");
const readers = @import("antfly_readers");
const output = @import("antfly_documents").output;
const A = std.mem.Allocator;
const magic = "AFRF";
const version: u32 = 1;
const prefix_bytes = 16;
const receipt_bytes = 56; // SHA-256 key followed by three big-endian u64 counters.
const max_scalar_bytes = 4096;
pub const Bounds = struct {
    max_receipts: usize,
    max_frame_bytes: usize = readers.document_rpc.max_payload_bytes,
    fn frameBytes(self: Bounds) usize {
        return @min(self.max_frame_bytes, readers.document_rpc.max_payload_bytes);
    }
};
const Metadata = struct {
    mime_type: []const u8,
    width: u32,
    height: u32,
    effective_dpi: u16,
    raster_to_source: c.Transform,
    usage: c.Usage,
};
pub const Header = struct {
    mime_type: []const u8,
    width: u32,
    height: u32,
    effective_dpi: u16,
    raster_to_source: c.Transform,
    usage: c.Usage,
    costs: []const c.costs.Receipt = &.{},
};
pub const Frame = struct { header: Header, bytes: []const u8 };
pub fn encode(a: A, header: Header, bytes: []const u8, bounds: Bounds) ![]u8 {
    if (header.costs.len > bounds.max_receipts) return error.DocumentOutputTooLarge;
    const count = std.math.cast(u32, header.costs.len) orelse return error.DocumentOutputTooLarge;
    const receipts_len = std.math.mul(usize, header.costs.len, receipt_bytes) catch return error.DocumentOutputTooLarge;
    const body_len = std.math.add(usize, receipts_len, bytes.len) catch return error.DocumentOutputTooLarge;
    if (prefix_bytes > bounds.frameBytes() or body_len > bounds.frameBytes() - prefix_bytes) return error.DocumentOutputTooLarge;
    var scalar: Metadata = undefined;
    inline for (@typeInfo(Metadata).@"struct".field_names) |field| @field(scalar, field) = @field(header, field);
    const metadata = try output.valueAlloc(a, scalar, @min(max_scalar_bytes, bounds.frameBytes() - prefix_bytes - body_len));
    defer a.free(metadata);
    const result = try a.alloc(u8, prefix_bytes + metadata.len + body_len);
    @memcpy(result[0..4], magic);
    std.mem.writeInt(u32, result[4..8], version, .big);
    std.mem.writeInt(u32, result[8..12], @intCast(metadata.len), .big);
    std.mem.writeInt(u32, result[12..16], count, .big);
    @memcpy(result[prefix_bytes..][0..metadata.len], metadata);
    var cursor: usize = prefix_bytes + metadata.len;
    for (header.costs) |receipt| {
        @memcpy(result[cursor..][0..32], &receipt.key);
        std.mem.writeInt(u64, result[cursor + 32 ..][0..8], receipt.measured.prompt_tokens, .big);
        std.mem.writeInt(u64, result[cursor + 40 ..][0..8], receipt.measured.completion_tokens, .big);
        std.mem.writeInt(u64, result[cursor + 48 ..][0..8], receipt.measured.rendered_pixels, .big);
        cursor += receipt_bytes;
    }
    @memcpy(result[cursor..], bytes);
    return result;
}
pub fn decode(a: A, bytes: []const u8, bounds: Bounds) !Frame {
    if (bytes.len > bounds.frameBytes()) return error.DocumentOutputTooLarge;
    if (bytes.len < 4) return error.InvalidDocumentCheckpoint;
    if (!std.mem.eql(u8, bytes[0..4], magic)) {
        // Older frames embedded their (small) receipt list in scalar JSON.
        const length = std.mem.readInt(u32, bytes[0..4], .big);
        if (length > max_scalar_bytes or length > bytes.len - 4) return error.InvalidDocumentCheckpoint;
        const header = try std.json.parseFromSliceLeaky(Header, a, bytes[4..][0..length], .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
        if (header.costs.len > bounds.max_receipts) return error.DocumentOutputTooLarge;
        return .{ .header = header, .bytes = bytes[4 + length ..] };
    }
    if (bytes.len < prefix_bytes or std.mem.readInt(u32, bytes[4..8], .big) != version) return error.InvalidDocumentCheckpoint;
    const length = std.mem.readInt(u32, bytes[8..12], .big);
    const count = std.mem.readInt(u32, bytes[12..16], .big);
    if (length > max_scalar_bytes or length > bytes.len - prefix_bytes) return error.InvalidDocumentCheckpoint;
    if (count > bounds.max_receipts) return error.DocumentOutputTooLarge;
    var cursor: usize = prefix_bytes + length;
    if (count > (bytes.len - cursor) / receipt_bytes) return error.InvalidDocumentCheckpoint;
    const scalar = try std.json.parseFromSliceLeaky(Metadata, a, bytes[prefix_bytes..][0..length], .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
    var header: Header = undefined;
    inline for (@typeInfo(Metadata).@"struct".field_names) |field| @field(header, field) = @field(scalar, field);
    const costs = try a.alloc(c.costs.Receipt, count);
    for (costs) |*receipt| {
        receipt.* = .{ .key = bytes[cursor..][0..32].*, .measured = .{
            .prompt_tokens = std.mem.readInt(u64, bytes[cursor + 32 ..][0..8], .big),
            .completion_tokens = std.mem.readInt(u64, bytes[cursor + 40 ..][0..8], .big),
            .rendered_pixels = std.mem.readInt(u64, bytes[cursor + 48 ..][0..8], .big),
        } };
        cursor += receipt_bytes;
    }
    header.costs = costs;
    return .{ .header = header, .bytes = bytes[cursor..] };
}

test "document raster checkpoint validates receipt and frame bounds before allocation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    var costs: [64]c.costs.Receipt = undefined;
    for (&costs, 0..) |*receipt, index| receipt.* = .{ .key = @splat(@intCast(index)), .measured = .{ .prompt_tokens = index, .completion_tokens = index * 2, .rendered_pixels = index * 3 } };
    const header: Header = .{ .mime_type = "image/png", .width = 10, .height = 20, .effective_dpi = 72, .raster_to_source = .{}, .usage = .{}, .costs = &costs };
    const frame = try encode(work, header, "image bytes", .{ .max_receipts = costs.len });
    const decoded = try decode(work, frame, .{ .max_receipts = costs.len });
    try std.testing.expectEqualDeep(header, decoded.header);
    try std.testing.expectEqualStrings("image bytes", decoded.bytes);
    // Exact boundaries work; denial and malformed lengths need no allocation.
    _ = try decode(work, frame, .{ .max_receipts = costs.len, .max_frame_bytes = frame.len });
    try std.testing.expectError(error.DocumentOutputTooLarge, encode(std.testing.failing_allocator, header, "", .{ .max_receipts = costs.len - 1 }));
    try std.testing.expectError(error.DocumentOutputTooLarge, encode(std.testing.failing_allocator, header, "", .{ .max_receipts = costs.len, .max_frame_bytes = prefix_bytes }));
    try std.testing.expectError(error.DocumentOutputTooLarge, decode(std.testing.failing_allocator, frame, .{ .max_receipts = costs.len - 1 }));
    try std.testing.expectError(error.DocumentOutputTooLarge, decode(std.testing.failing_allocator, frame, .{ .max_receipts = costs.len, .max_frame_bytes = frame.len - 1 }));
    try std.testing.expectError(error.InvalidDocumentCheckpoint, decode(std.testing.failing_allocator, frame[0 .. frame.len - "image bytes".len - 1], .{ .max_receipts = costs.len }));
    std.mem.writeInt(u32, frame[12..16], std.math.maxInt(u32), .big);
    try std.testing.expectError(error.DocumentOutputTooLarge, decode(std.testing.failing_allocator, frame, .{ .max_receipts = costs.len }));
    std.mem.writeInt(u32, frame[12..16], 0, .big);
    std.mem.writeInt(u32, frame[8..12], std.math.maxInt(u32), .big);
    try std.testing.expectError(error.InvalidDocumentCheckpoint, decode(std.testing.failing_allocator, frame, .{ .max_receipts = costs.len }));
    std.mem.writeInt(u32, frame[4..8], version + 1, .big);
    try std.testing.expectError(error.InvalidDocumentCheckpoint, decode(std.testing.failing_allocator, frame, .{ .max_receipts = costs.len }));
    try std.testing.expectError(error.InvalidDocumentCheckpoint, decode(std.testing.failing_allocator, magic, .{ .max_receipts = costs.len }));
}

test "document raster checkpoint reads bounded legacy metadata" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const costs = [_]c.costs.Receipt{.{ .key = @splat(7), .measured = .{ .rendered_pixels = 200 } }};
    const header: Header = .{ .mime_type = "image/png", .width = 10, .height = 20, .effective_dpi = 72, .raster_to_source = .{}, .usage = .{ .rendered_pixels = 200 }, .costs = &costs };
    const metadata = try std.json.Stringify.valueAlloc(work, header, .{});
    const legacy = try work.alloc(u8, 4 + metadata.len + 3);
    std.mem.writeInt(u32, legacy[0..4], @intCast(metadata.len), .big);
    @memcpy(legacy[4..][0..metadata.len], metadata);
    @memcpy(legacy[4 + metadata.len ..], "png");
    const decoded = try decode(work, legacy, .{ .max_receipts = 1 });
    try std.testing.expectEqualDeep(header, decoded.header);
    try std.testing.expectEqualStrings("png", decoded.bytes);
    try std.testing.expectError(error.DocumentOutputTooLarge, decode(work, legacy, .{ .max_receipts = 0 }));
}
