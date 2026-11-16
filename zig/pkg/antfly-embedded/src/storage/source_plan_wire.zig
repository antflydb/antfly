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

//! Bounded private binary-safe transport for native source-plan commands.
//! This codec grants no source or root authority; authenticated owner ingress
//! must independently qualify the command's immutable catalog fence.
const std = @import("std");
pub const control = @import("source_plan_control.zig");
const storage = control.storage;
const A = std.mem.Allocator;
pub const max_bytes = 2 * 1024 * 1024;
pub const Command = struct {
    version: u8,
    root: storage.Root,
    spec: storage.Spec,
    action: @FieldType(control.Request, "action"),
    ordinal: u16 = 0,
    chunk_base64: []const u8 = "",
    max_chunks: u8 = storage.max_reclaim_chunks,
};
pub const Encoded = struct {
    allocator: A,
    command: Command,
    chunk: []u8,
    pub fn init(a: A, request: control.Request) !Encoded {
        try request.validate();
        const chunk = try a.alloc(u8, std.base64.standard.Encoder.calcSize(request.chunk.len));
        errdefer a.free(chunk);
        _ = std.base64.standard.Encoder.encode(chunk, request.chunk);
        return .{ .allocator = a, .chunk = chunk, .command = .{ .version = 1, .root = request.root, .spec = request.spec, .action = request.action, .ordinal = request.ordinal, .chunk_base64 = chunk, .max_chunks = request.max_chunks } };
    }
    pub fn deinit(self: *Encoded) void {
        self.allocator.free(self.chunk);
        self.* = undefined;
    }
};
pub const Owned = struct {
    parsed: std.json.Parsed(Command),
    allocator: A,
    chunk: []u8,
    request: control.Request,
    pub fn deinit(self: *Owned) void {
        self.allocator.free(self.chunk);
        self.parsed.deinit();
        self.* = undefined;
    }
};
pub fn encode(a: A, request: control.Request) ![]u8 {
    var encoded = try Encoded.init(a, request);
    defer encoded.deinit();
    const bytes = try std.json.Stringify.valueAlloc(a, encoded.command, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_bytes) return error.InvalidSourcePlanWire;
    return bytes;
}
fn canonical(bytes: []const u8, encoded: []const u8) bool {
    if (std.base64.standard.Encoder.calcSize(bytes.len) != encoded.len) return false;
    var offset: usize = 0;
    while (offset < bytes.len) : (offset += 3) {
        var quartet: [4]u8 = undefined;
        const part = std.base64.standard.Encoder.encode(&quartet, bytes[offset..@min(bytes.len, offset + 3)]);
        if (!std.mem.eql(u8, part, encoded[offset / 3 * 4 ..][0..part.len])) return false;
    }
    return true;
}
/// Owns decoded binary only; the command's spec slices remain borrowed from
/// the caller's parsed command until the synchronous native call completes.
pub const Decoded = struct {
    allocator: A,
    chunk: []u8,
    request: control.Request,
    pub fn deinit(self: *Decoded) void {
        self.allocator.free(self.chunk);
        self.* = undefined;
    }
};
pub const Recovery = struct { command: Command, entry: control.Entry, minimum_applied_index: u64 };
pub const ChunkReply = struct { observation: control.Observation, chunk_base64: []const u8 };
pub fn fromCommand(a: A, value: Command) !Decoded {
    if (value.version != 1 or value.chunk_base64.len > std.base64.standard.Encoder.calcSize(storage.chunk_bytes)) return error.InvalidSourcePlanWire;
    const length = std.base64.standard.Decoder.calcSizeForSlice(value.chunk_base64) catch return error.InvalidSourcePlanWire;
    if (length > storage.chunk_bytes) return error.InvalidSourcePlanWire;
    const chunk = try a.alloc(u8, length);
    errdefer a.free(chunk);
    std.base64.standard.Decoder.decode(chunk, value.chunk_base64) catch return error.InvalidSourcePlanWire;
    if (!canonical(chunk, value.chunk_base64)) return error.InvalidSourcePlanWire;
    const request: control.Request = .{ .root = value.root, .spec = value.spec, .action = value.action, .ordinal = value.ordinal, .chunk = chunk, .max_chunks = value.max_chunks };
    try request.validate();
    return .{ .allocator = a, .chunk = chunk, .request = request };
}
fn finish(a: A, parsed: std.json.Parsed(Command)) !Owned {
    const decoded = try fromCommand(a, parsed.value);
    return .{ .parsed = parsed, .allocator = a, .chunk = decoded.chunk, .request = decoded.request };
}
pub fn decode(a: A, bytes: []const u8) !Owned {
    if (bytes.len > max_bytes) return error.InvalidSourcePlanWire;
    var parsed = try std.json.parseFromSlice(Command, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" });
    errdefer parsed.deinit();
    return finish(a, parsed);
}
/// The caller must have rejected duplicate fields while parsing the envelope.
/// All returned command strings, digest slices and binary bytes are owned.
pub fn decodeValue(a: A, value: std.json.Value) !Owned {
    var parsed = try std.json.parseFromValue(Command, a, value, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    errdefer parsed.deinit();
    return finish(a, parsed);
}

fn fixture(bytes: []const u8, hashes: []const storage.Digest) control.Request {
    var root: storage.Root = .{ .authority = .raft, .namespace = @splat(1), .generation = 0, .fence = @splat(2) };
    std.mem.writeInt(u64, root.namespace[0..8], 1, .big);
    var attempt: [16]u8 = @splat(1);
    std.mem.writeInt(u64, attempt[0..8], 1, .big);
    return .{ .root = root, .spec = .{ .namespace = storage.namespaceFor(root, 1), .storage_epoch = 1, .binding = @splat(3), .domain = storage.domainFor(@splat(3)), .attempt = attempt, .bytes = bytes.len, .checksum = @splat(4), .plan_digest = @splat(5), .chunk_hashes = hashes }, .action = .chunk, .chunk = bytes };
}

test "source plan wire owns arbitrary binary and freezes semantic command identity" {
    const a = std.testing.allocator;
    var payload: [256]u8 = undefined;
    for (&payload, 0..) |*byte, i| byte.* = @intCast(i);
    const hashes = [_]storage.Digest{storage.chunkDigest(0, &payload)};
    const request = fixture(&payload, &hashes);
    const bytes = try encode(a, request);
    var owned = try decode(a, bytes);
    defer owned.deinit();
    @memset(bytes, 0);
    a.free(bytes);
    try std.testing.expectEqualSlices(u8, &payload, owned.request.chunk);
    try std.testing.expectEqual(request.digest(), owned.request.digest());
    try std.testing.expectEqualDeep(request.spec, owned.request.spec);
    const again = try encode(a, owned.request);
    defer a.free(again);
    var reparsed = try decode(a, again);
    defer reparsed.deinit();
    try std.testing.expectEqual(request.digest(), reparsed.request.digest());
}

test "source plan wire rejects duplicate unknown mismatched and noncanonical commands" {
    const a = std.testing.allocator;
    const payload: [64]u8 = @splat(255);
    const hashes = [_]storage.Digest{storage.chunkDigest(0, &payload)};
    const request = fixture(&payload, &hashes);
    const bytes = try encode(a, request);
    defer a.free(bytes);
    const duplicate = try std.fmt.allocPrint(a, "{s},\"version\":1}}", .{bytes[0 .. bytes.len - 1]});
    defer a.free(duplicate);
    try std.testing.expectError(error.DuplicateField, decode(a, duplicate));
    const unknown = try std.fmt.allocPrint(a, "{s},\"extra\":0}}", .{bytes[0 .. bytes.len - 1]});
    defer a.free(unknown);
    try std.testing.expectError(error.UnknownField, decode(a, unknown));
    var encoded = try Encoded.init(a, request);
    defer encoded.deinit();
    // The final quartet is /w==. /x== decodes to the same byte but is not
    // canonical base64, so it cannot create another wire spelling of a grant.
    encoded.chunk[encoded.chunk.len - 3] = 'x';
    const bad = try std.json.Stringify.valueAlloc(a, encoded.command, .{});
    defer a.free(bad);
    try std.testing.expectError(error.InvalidSourcePlanWire, decode(a, bad));
    encoded.command.version = 2;
    const future = try std.json.Stringify.valueAlloc(a, encoded.command, .{});
    defer a.free(future);
    try std.testing.expectError(error.InvalidSourcePlanWire, decode(a, future));
    encoded.command.version = 1;
    _ = std.base64.standard.Encoder.encode(encoded.chunk, &payload);
    encoded.command.action = .begin;
    const ignored = try std.json.Stringify.valueAlloc(a, encoded.command, .{});
    defer a.free(ignored);
    try std.testing.expectError(error.InvalidSourceAdmission, decode(a, ignored));
    encoded.command.action = .chunk;
    encoded.command.spec.chunk_hashes = &.{@splat(7)};
    const changed = try std.json.Stringify.valueAlloc(a, encoded.command, .{});
    defer a.free(changed);
    try std.testing.expectError(error.SourcePayloadConflict, decode(a, changed));
}

fn allocationRoundTrip(a: A) !void {
    const payload: [64]u8 = @splat(255);
    const hashes = [_]storage.Digest{storage.chunkDigest(0, &payload)};
    const bytes = try encode(a, fixture(&payload, &hashes));
    defer a.free(bytes);
    var owned = try decode(a, bytes);
    defer owned.deinit();
}
test "source plan wire allocation failure releases every owned buffer" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationRoundTrip, .{});
}
