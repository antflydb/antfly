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

//! Authenticated inline references to already durable owner-admitted cuts.
//! The signing key lives only in native metadata; tokens carry no credentials.
const std = @import("std");
const A = std.mem.Allocator;
pub const Key = [32]u8;
pub const max_payload = 1024 * 1024;
pub const max_token_bytes = 2 * max_payload + 4096;
pub const max_lake_token_bytes = 16 * 1024;
fn tokenLimit(kind: Kind) usize {
    return if (kind == .lake) max_lake_token_bytes else max_token_bytes;
}
pub const Kind = enum { lake, composed };
pub const Scope = struct { table_id: u64, object_generation: u64, owner: [16]u8 };
const binary_version: u8 = 2;
const binary_header_bytes = 1 + 8 + 8;
const Envelope = struct { version: u8 = 1, issued_ms: u64, expires_ms: u64, payload: []const u8 };
pub const Opened = struct { scope: Scope, expires_ms: u64, payload: []const u8 };
fn prefix(kind: Kind) []const u8 {
    return if (kind == .lake) "lake3:" else "source3:";
}
fn signature(key: Key, bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&result, bytes, &key);
    return result;
}
pub fn seal(a: A, kind: Kind, scope: Scope, key: Key, payload: []const u8, now: u64, expires: u64) ![]const u8 {
    if (payload.len == 0 or payload.len > max_payload or now == 0 or (expires > now and expires - now > std.time.ms_per_hour) or scope.table_id == 0 or std.mem.allEqual(u8, &scope.owner, 0) or std.mem.allEqual(u8, &key, 0)) return error.InvalidQueryRequest;
    if (expires <= now) return error.DeadlineExceeded;
    // Raw framing preserves the full payload allowance independently of JSON
    // escape density. The outer scope and HMAC contract remains unchanged.
    const bytes = try a.alloc(u8, binary_header_bytes + payload.len);
    defer a.free(bytes);
    bytes[0] = binary_version;
    std.mem.writeInt(u64, bytes[1..9], now, .little);
    std.mem.writeInt(u64, bytes[9..17], expires, .little);
    @memcpy(bytes[binary_header_bytes..], payload);
    return encodeEnvelope(a, kind, scope, key, bytes);
}
fn encodeEnvelope(a: A, kind: Kind, scope: Scope, key: Key, bytes: []const u8) ![]const u8 {
    const codec = std.base64.url_safe_no_pad;
    const encoded_size = codec.Encoder.calcSize(bytes.len);
    if (encoded_size > tokenLimit(kind)) return error.QueryCandidateBudgetExceeded;
    const encoded = try a.alloc(u8, encoded_size);
    defer a.free(encoded);
    _ = codec.Encoder.encode(encoded, bytes);
    const message = try std.fmt.allocPrint(a, "{s}{d}:{d}:{s}:{s}", .{ prefix(kind), scope.table_id, scope.object_generation, std.fmt.bytesToHex(&scope.owner, .lower), encoded });
    defer a.free(message);
    if (message.len > tokenLimit(kind) - 65) return error.QueryCandidateBudgetExceeded;
    return std.fmt.allocPrint(a, "{s}:{s}", .{ message, std.fmt.bytesToHex(&signature(key, message), .lower) });
}
const Parts = struct { scope: Scope, message: []const u8, encoded: []const u8, mac: [32]u8 };
fn parts(kind: Kind, token: []const u8) !Parts {
    if (token.len > tokenLimit(kind) or !std.mem.startsWith(u8, token, prefix(kind))) return error.InvalidQueryRequest;
    var fields = std.mem.splitScalar(u8, token[prefix(kind).len..], ':');
    const id = fields.next() orelse return error.InvalidQueryRequest;
    const generation = fields.next() orelse return error.InvalidQueryRequest;
    const owner = fields.next() orelse return error.InvalidQueryRequest;
    const encoded = fields.next() orelse return error.InvalidQueryRequest;
    const mac = fields.next() orelse return error.InvalidQueryRequest;
    if (fields.next() != null or owner.len != 32 or mac.len != 64 or encoded.len == 0) return error.InvalidQueryRequest;
    var result: Parts = .{ .scope = .{
        .table_id = std.fmt.parseUnsigned(u64, id, 10) catch return error.InvalidQueryRequest,
        .object_generation = std.fmt.parseUnsigned(u64, generation, 10) catch return error.InvalidQueryRequest,
        .owner = undefined,
    }, .message = token[0 .. token.len - 65], .encoded = encoded, .mac = undefined };
    _ = std.fmt.hexToBytes(&result.scope.owner, owner) catch return error.InvalidQueryRequest;
    _ = std.fmt.hexToBytes(&result.mac, mac) catch return error.InvalidQueryRequest;
    if (result.scope.table_id == 0 or std.mem.allEqual(u8, &result.scope.owner, 0)) return error.InvalidQueryRequest;
    return result;
}
/// Routing hint only. The caller must authorize the bound table and verify open()
/// against its durable owner key before consuming any payload or expiry.
pub fn peek(kind: Kind, token: []const u8) !Scope {
    return (try parts(kind, token)).scope;
}
pub fn open(a: A, kind: Kind, scope: Scope, key: Key, token: []const u8, now: u64) !Opened {
    const parsed = try parts(kind, token);
    if (parsed.scope.table_id != scope.table_id or parsed.scope.object_generation != scope.object_generation or !std.crypto.timing_safe.eql([16]u8, parsed.scope.owner, scope.owner)) return error.CatalogGenerationChanged;
    if (!std.crypto.timing_safe.eql([32]u8, parsed.mac, signature(key, parsed.message))) return error.InvalidQueryRequest;
    const codec = std.base64.url_safe_no_pad.Decoder;
    const size = codec.calcSizeForSlice(parsed.encoded) catch return error.InvalidQueryRequest;
    if (size > 2 * max_payload + 1024) return error.QueryCandidateBudgetExceeded;
    const bytes = try a.alloc(u8, size);
    defer a.free(bytes);
    codec.decode(bytes, parsed.encoded) catch return error.InvalidQueryRequest;
    if (bytes.len != 0 and bytes[0] == binary_version) {
        if (bytes.len <= binary_header_bytes or bytes.len - binary_header_bytes > max_payload) return error.CatalogGenerationChanged;
        const issued_ms = std.mem.readInt(u64, bytes[1..9], .little);
        const expires_ms = std.mem.readInt(u64, bytes[9..17], .little);
        try validateLifetime(issued_ms, expires_ms, now);
        return .{ .scope = scope, .expires_ms = expires_ms, .payload = try a.dupe(u8, bytes[binary_header_bytes..]) };
    }
    // Existing cursors retain their complete horizon through upgrades.
    const value = std.json.parseFromSliceLeaky(Envelope, a, bytes, .{ .allocate = .alloc_always }) catch return error.InvalidQueryRequest;
    if (value.version != 1 or value.payload.len == 0 or value.payload.len > max_payload) return error.CatalogGenerationChanged;
    try validateLifetime(value.issued_ms, value.expires_ms, now);
    return .{ .scope = scope, .expires_ms = value.expires_ms, .payload = value.payload };
}
fn validateLifetime(issued_ms: u64, expires_ms: u64, now: u64) !void {
    if (issued_ms == 0 or expires_ms <= issued_ms or expires_ms - issued_ms > std.time.ms_per_hour or issued_ms > now +| 60_000 or expires_ms <= now) return error.CatalogGenerationChanged;
}

test "owner capabilities preserve opaque bytes and fence signatures scope kind and expiry" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const scope: Scope = .{ .table_id = std.math.maxInt(u64), .object_generation = 7, .owner = @splat(1) };
    const key: Key = @splat(2);
    const token = try seal(a, .composed, scope, key, "{\"cursor\":1}", 1000, 301000);
    try std.testing.expectEqual(scope.table_id, (try peek(.composed, token)).table_id);
    const value = try open(a, .composed, scope, key, token, 2000);
    try std.testing.expectEqualStrings("{\"cursor\":1}", value.payload);
    try std.testing.expectError(error.CatalogGenerationChanged, open(a, .composed, scope, key, token, 301000));
    try std.testing.expectError(error.InvalidQueryRequest, open(a, .lake, scope, key, token, 2000));
    try std.testing.expectError(error.InvalidQueryRequest, open(a, .composed, scope, @splat(3), token, 2000));
    var other = scope;
    other.object_generation += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, open(a, .composed, other, key, token, 2000));
    const tampered = try a.dupe(u8, token);
    tampered[tampered.len - 1] = if (tampered[tampered.len - 1] == '0') '1' else '0';
    try std.testing.expectError(error.InvalidQueryRequest, open(a, .composed, scope, key, tampered, 2000));
    try std.testing.expectError(error.InvalidQueryRequest, seal(a, .lake, scope, key, "payload", 1, std.time.ms_per_hour + 2));
}

test "owner capability encoder and decoder share encoded limits" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const scope: Scope = .{ .table_id = 1, .object_generation = 1, .owner = @splat(1) };
    const key: Key = @splat(2);
    const payload = try a.alloc(u8, max_payload);
    @memset(payload, 'a');
    const token = try seal(a, .composed, scope, key, payload, 1, 1000);
    try std.testing.expectEqualStrings(payload, (try open(a, .composed, scope, key, token, 2)).payload);
    @memset(payload, '"');
    const escaped = try seal(a, .composed, scope, key, payload, 1, 1000);
    try std.testing.expect(escaped.len <= max_token_bytes);
    try std.testing.expectEqualStrings(payload, (try open(a, .composed, scope, key, escaped, 2)).payload);
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, seal(a, .lake, scope, key, payload, 1, 1000));
    try std.testing.expectError(error.DeadlineExceeded, seal(a, .composed, scope, key, "payload", 1000, 1000));
}

test "owner capability framing preserves quote-heavy ordered composed frontiers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const quote = try a.alloc(u8, 200_000);
    @memset(quote, '"');
    const Position = struct {
        table: []const u8,
        after: []const []const u8,
        raw_count: usize = 1,
        visible_count: usize = 1,
        total: usize = 2,
        identity_pinned: bool = true,
        remote_snapshot: []const u8 = "lake3:already-durable",
        identity: []const u8 = "null",
    };
    const positions = [_]Position{
        .{ .table = "history", .after = &.{ quote, "row-one" } },
        .{ .table = "current", .after = &.{ quote, "row-two" } },
    };
    const payload = try std.json.Stringify.valueAlloc(a, .{ .version = 3, .fingerprint = @as([]const u8, &(@as([64]u8, @splat('a')))), .sources = positions, .total = @as(?usize, null), .aggregations = @as(?bool, null) }, .{});
    try std.testing.expect(payload.len <= max_payload);
    // These are admitted scalar sort-key sizes. Nesting the JSON frontier
    // inside another JSON string used to exceed the encoded token allowance.
    const nested = try std.json.Stringify.valueAlloc(a, Envelope{ .issued_ms = 1000, .expires_ms = 301000, .payload = payload }, .{});
    try std.testing.expect(std.base64.url_safe_no_pad.Encoder.calcSize(nested.len) > max_token_bytes);
    const scope: Scope = .{ .table_id = 1, .object_generation = 1, .owner = @splat(1) };
    const key: Key = @splat(2);
    const token = try seal(a, .composed, scope, key, payload, 1000, 301000);
    try std.testing.expect(token.len <= max_token_bytes);
    try std.testing.expectEqualStrings(payload, (try open(a, .composed, scope, key, token, 2000)).payload);
}

test "owner capabilities decode legacy envelopes and preserve opaque binary payloads" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const scope: Scope = .{ .table_id = 1, .object_generation = 1, .owner = @splat(1) };
    const key: Key = @splat(2);
    const legacy_payload = "{\"sources\":[],\"escaped\":\"\\\\\\\"\"}";
    const legacy_envelope = try std.json.Stringify.valueAlloc(a, Envelope{ .issued_ms = 1000, .expires_ms = 301000, .payload = legacy_payload }, .{});
    const legacy = try encodeEnvelope(a, .composed, scope, key, legacy_envelope);
    try std.testing.expectEqualStrings(legacy_payload, (try open(a, .composed, scope, key, legacy, 2000)).payload);
    try std.testing.expectError(error.CatalogGenerationChanged, open(a, .composed, scope, key, legacy, 301000));
    const binary_payload = [_]u8{ 0, 1, 2, 255, 0xc3, 0x28, '"', '\\' };
    const token = try seal(a, .composed, scope, key, &binary_payload, 1000, 301000);
    try std.testing.expectEqualSlices(u8, &binary_payload, (try open(a, .composed, scope, key, token, 2000)).payload);
    const truncated = try encodeEnvelope(a, .composed, scope, key, &.{binary_version});
    try std.testing.expectError(error.CatalogGenerationChanged, open(a, .composed, scope, key, truncated, 2000));
    var bad_lifetime: [binary_header_bytes + 1]u8 = @splat(0);
    bad_lifetime[0] = binary_version;
    std.mem.writeInt(u64, bad_lifetime[1..9], 1000, .little);
    std.mem.writeInt(u64, bad_lifetime[9..17], 1000, .little);
    const invalid = try encodeEnvelope(a, .composed, scope, key, &bad_lifetime);
    try std.testing.expectError(error.CatalogGenerationChanged, open(a, .composed, scope, key, invalid, 1000));
}
