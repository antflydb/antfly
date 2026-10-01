// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! AFSE v1 envelope-encrypted records. Persistence, authorization, and trusted
//! current-revision selection belong to the backend, not this codec.
const std = @import("std");
const contract = @import("secret_contract.zig");
const callback = @import("../runtime_callback_abi.zig");
const Aead = std.crypto.aead.chacha_poly.XChaCha20Poly1305;

pub const Identity = contract.Identity;
pub const SecretBytes = contract.SecretBytes;
pub const DataKey = [Aead.key_length]u8;
pub const version: u16 = 1;
pub const algorithm: u16 = 1; // XChaCha20-Poly1305
pub const max_wrapped_key_bytes = 64 * 1024;
pub const header_bytes = 54;
pub const max_record_bytes = header_bytes + 3 * contract.max_identity_bytes + max_wrapped_key_bytes + contract.max_value_bytes + Aead.tag_length;
const magic = "AFSE";

pub const WrappedKey = struct {
    /// Stable, concrete wrapping key identifier, not an alias whose target can change.
    key_id: []u8,
    bytes: []u8,

    pub fn deinit(self: *WrappedKey, alloc: std.mem.Allocator) void {
        alloc.free(self.key_id);
        alloc.free(self.bytes);
        self.* = undefined;
    }
};

/// The host owns this provider and its key material. Never resolve its bootstrap
/// credentials through the native store it unlocks. Callbacks may perform I/O;
/// they must never execute inside deterministic Raft apply.
pub const KeyProvider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    const Boundary = callback.Boundary(VTable);
    pub const VTable = struct {
        wrap: *const fn (*anyopaque, std.mem.Allocator, Identity, *const DataKey) anyerror!WrappedKey,
        unwrap: *const fn (*anyopaque, Identity, []const u8, []const u8, *DataKey) anyerror!void,
    };

    fn wrap(self: KeyProvider, alloc: std.mem.Allocator, identity: Identity, key: *const DataKey) !WrappedKey {
        return Boundary.call("wrap", self.dispatch, self.vtable.wrap, .{ self.ptr, alloc, identity, key });
    }

    fn unwrap(self: KeyProvider, identity: Identity, key_id: []const u8, wrapped: []const u8, key: *DataKey) !void {
        return Boundary.call("unwrap", self.dispatch, self.vtable.unwrap, .{ self.ptr, identity, key_id, wrapped, key });
    }
};

/// Borrowed, UNAUTHENTICATED view. Decode validates framing only. Never use
/// these fields for authorization or to choose the expected current identity.
pub const View = struct {
    identity: Identity,
    key_id: []const u8,
    wrapped_key: []const u8,
    nonce: [Aead.nonce_length]u8,
    ciphertext: []const u8,
    tag: [Aead.tag_length]u8,
    associated_data: []const u8,
};

/// Generates both the data key and nonce with fallible OS-backed entropy.
/// Returns ciphertext only; no production API accepts caller-supplied nonces.
pub fn seal(alloc: std.mem.Allocator, io: std.Io, provider: KeyProvider, identity: Identity, plaintext: []const u8) ![]u8 {
    try identity.validate();
    if (plaintext.len > contract.max_value_bytes) return error.InvalidArgument;
    var key: DataKey = undefined;
    defer std.crypto.secureZero(u8, &key);
    try io.randomSecure(&key);
    var nonce: [Aead.nonce_length]u8 = undefined;
    try io.randomSecure(&nonce);
    var wrapped = try provider.wrap(alloc, identity, &key);
    defer wrapped.deinit(alloc);
    try contract.validateName(wrapped.key_id);
    if (wrapped.bytes.len == 0 or wrapped.bytes.len > max_wrapped_key_bytes) return error.InvalidArgument;
    return encode(alloc, identity, plaintext, wrapped, nonce, &key);
}

fn encode(alloc: std.mem.Allocator, identity: Identity, plaintext: []const u8, wrapped: WrappedKey, nonce: [Aead.nonce_length]u8, key: *const DataKey) ![]u8 {
    const prefix_len = header_bytes + identity.scope.len + identity.key.len + wrapped.key_id.len + wrapped.bytes.len;
    const out = try alloc.alloc(u8, prefix_len + plaintext.len + Aead.tag_length);
    @memcpy(out[0..4], magic);
    std.mem.writeInt(u16, out[4..6], version, .little);
    std.mem.writeInt(u16, out[6..8], algorithm, .little);
    std.mem.writeInt(u64, out[8..16], identity.revision, .little);
    std.mem.writeInt(u16, out[16..18], @intCast(identity.scope.len), .little);
    std.mem.writeInt(u16, out[18..20], @intCast(identity.key.len), .little);
    std.mem.writeInt(u16, out[20..22], @intCast(wrapped.key_id.len), .little);
    std.mem.writeInt(u32, out[22..26], @intCast(wrapped.bytes.len), .little);
    std.mem.writeInt(u32, out[26..30], @intCast(plaintext.len), .little);
    @memcpy(out[30..54], &nonce);
    var offset: usize = header_bytes;
    for ([_][]const u8{ identity.scope, identity.key, wrapped.key_id, wrapped.bytes }) |field| {
        @memcpy(out[offset..][0..field.len], field);
        offset += field.len;
    }
    var tag: [Aead.tag_length]u8 = undefined;
    Aead.encrypt(out[prefix_len..][0..plaintext.len], &tag, plaintext, out[0..prefix_len], nonce, key.*);
    @memcpy(out[out.len - Aead.tag_length ..], &tag);
    return out;
}

pub fn decode(encoded: []const u8) !View {
    if (encoded.len < header_bytes + Aead.tag_length or encoded.len > max_record_bytes) return error.CorruptInput;
    if (!std.mem.eql(u8, encoded[0..4], magic)) return error.CorruptInput;
    if (std.mem.readInt(u16, encoded[4..6], .little) != version or std.mem.readInt(u16, encoded[6..8], .little) != algorithm) return error.UnsupportedVersion;
    const scope_len: usize = std.mem.readInt(u16, encoded[16..18], .little);
    const key_len: usize = std.mem.readInt(u16, encoded[18..20], .little);
    const key_id_len: usize = std.mem.readInt(u16, encoded[20..22], .little);
    const wrapped_len: usize = std.mem.readInt(u32, encoded[22..26], .little);
    const ciphertext_len: usize = std.mem.readInt(u32, encoded[26..30], .little);
    if (scope_len == 0 or scope_len > contract.max_identity_bytes or
        key_len == 0 or key_len > contract.max_identity_bytes or
        key_id_len == 0 or key_id_len > contract.max_identity_bytes or
        wrapped_len == 0 or wrapped_len > max_wrapped_key_bytes or
        ciphertext_len > contract.max_value_bytes) return error.CorruptInput;
    // Bounds above make these sums safe even on 32-bit hosts.
    const prefix_len = header_bytes + scope_len + key_len + key_id_len + wrapped_len;
    if (encoded.len != prefix_len + ciphertext_len + Aead.tag_length) return error.CorruptInput;
    const identity = Identity{
        .scope = encoded[header_bytes..][0..scope_len],
        .key = encoded[header_bytes + scope_len ..][0..key_len],
        .revision = std.mem.readInt(u64, encoded[8..16], .little),
    };
    identity.validate() catch return error.CorruptInput;
    const key_id = encoded[header_bytes + scope_len + key_len ..][0..key_id_len];
    contract.validateName(key_id) catch return error.CorruptInput;
    return .{
        .identity = identity,
        .key_id = key_id,
        .wrapped_key = encoded[prefix_len - wrapped_len .. prefix_len],
        .nonce = encoded[30..54].*,
        .ciphertext = encoded[prefix_len..][0..ciphertext_len],
        .tag = encoded[encoded.len - Aead.tag_length ..][0..Aead.tag_length].*,
        .associated_data = encoded[0..prefix_len],
    };
}

/// `expected` must come from the trusted storage index/committed revision, not
/// decode(encoded). AEAD alone cannot detect replay of an older valid record.
pub fn open(alloc: std.mem.Allocator, provider: KeyProvider, expected: Identity, encoded: []const u8) !SecretBytes {
    try expected.validate();
    const record = try decode(encoded);
    if (!expected.eql(record.identity)) return error.CorruptInput;
    var key: DataKey = undefined;
    defer std.crypto.secureZero(u8, &key);
    try provider.unwrap(expected, record.key_id, record.wrapped_key, &key);
    var result = SecretBytes{ .bytes = try alloc.alloc(u8, record.ciphertext.len) };
    errdefer result.deinit(alloc);
    Aead.decrypt(result.bytes, record.ciphertext, record.tag, record.associated_data, record.nonce, key) catch return error.CorruptInput;
    return result;
}

// Test-only authenticated wrapping provider; never used as a production key source.
const TestProvider = struct {
    key: DataKey = [_]u8{7} ** 32,
    unavailable: bool = false,
    unwrap_calls: usize = 0,
    fn provider(self: *@This()) KeyProvider {
        return .{ .ptr = self, .vtable = &.{ .wrap = wrap, .unwrap = unwrap } };
    }
    fn wrap(ptr: *anyopaque, alloc: std.mem.Allocator, identity: Identity, key: *const DataKey) !WrappedKey {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.unavailable) return error.Unavailable;
        const id = try alloc.dupe(u8, "test-key-1");
        errdefer alloc.free(id);
        const bytes = try alloc.alloc(u8, 24 + 32 + 16);
        errdefer alloc.free(bytes);
        try std.Options.debug_io.randomSecure(bytes[0..24]);
        var tag: [16]u8 = undefined;
        Aead.encrypt(bytes[24..56], &tag, key, identity.scope, bytes[0..24].*, self.key);
        @memcpy(bytes[56..72], &tag);
        return .{ .key_id = id, .bytes = bytes };
    }
    fn unwrap(ptr: *anyopaque, identity: Identity, key_id: []const u8, bytes: []const u8, key: *DataKey) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.unwrap_calls += 1;
        if (self.unavailable) return error.Unavailable;
        if (!std.mem.eql(u8, key_id, "test-key-1") or bytes.len != 72) return error.CorruptInput;
        Aead.decrypt(key, bytes[24..56], bytes[56..72].*, identity.scope, bytes[0..24].*, self.key) catch return error.CorruptInput;
    }
};

const test_identity = Identity{ .scope = "tenant-1", .key = "provider.token", .revision = 42 };

test "secret record round trips binary and empty values with randomized ciphertext" {
    const alloc = std.testing.allocator;
    var provider = TestProvider{};
    for ([_][]const u8{ "", "credential\x00\xff" }) |plaintext| {
        const encoded = try seal(alloc, std.Options.debug_io, provider.provider(), test_identity, plaintext);
        defer alloc.free(encoded);
        var opened = try open(alloc, provider.provider(), test_identity, encoded);
        defer opened.deinit(alloc);
        try std.testing.expectEqualSlices(u8, plaintext, opened.bytes);
        const another = try seal(alloc, std.Options.debug_io, provider.provider(), test_identity, plaintext);
        defer alloc.free(another);
        try std.testing.expect(!std.mem.eql(u8, encoded, another));
        try std.testing.expectEqual(@as(u64, 42), (try decode(encoded)).identity.revision);
    }
}

test "secret record rejects every modified byte truncation trailing data and identity substitution" {
    const alloc = std.testing.allocator;
    var provider = TestProvider{};
    const encoded = try seal(alloc, std.Options.debug_io, provider.provider(), test_identity, "credential");
    defer alloc.free(encoded);
    for (0..encoded.len) |i| {
        encoded[i] ^= 1;
        if (open(alloc, provider.provider(), test_identity, encoded)) |value| {
            var unexpected = value;
            unexpected.deinit(alloc);
            return error.TestUnexpectedResult;
        } else |_| {}
        encoded[i] ^= 1;
        try std.testing.expectError(error.CorruptInput, decode(encoded[0..i]));
    }
    const extended = try std.mem.concat(alloc, u8, &.{ encoded, "\x00" });
    defer alloc.free(extended);
    try std.testing.expectError(error.CorruptInput, decode(extended));
    const before = provider.unwrap_calls;
    for ([_]Identity{
        .{ .scope = "tenant-2", .key = test_identity.key, .revision = 42 },
        .{ .scope = test_identity.scope, .key = "other.token", .revision = 42 },
        .{ .scope = test_identity.scope, .key = test_identity.key, .revision = 43 },
    }) |identity| try std.testing.expectError(error.CorruptInput, open(alloc, provider.provider(), identity, encoded));
    try std.testing.expectEqual(before, provider.unwrap_calls);
    provider.key[0] ^= 1;
    try std.testing.expectError(error.CorruptInput, open(alloc, provider.provider(), test_identity, encoded));
    provider.key[0] ^= 1;
    provider.unavailable = true;
    try std.testing.expectError(error.Unavailable, open(alloc, provider.provider(), test_identity, encoded));
    try std.testing.expectError(error.Unavailable, seal(alloc, std.Options.debug_io, provider.provider(), test_identity, "credential"));
}

test "secret record validates bounds and refuses unsupported formats" {
    const alloc = std.testing.allocator;
    var provider = TestProvider{};
    const encoded = try seal(alloc, std.Options.debug_io, provider.provider(), test_identity, "value");
    defer alloc.free(encoded);
    encoded[4] = 2;
    try std.testing.expectError(error.UnsupportedVersion, decode(encoded));
    encoded[4] = 1;
    encoded[6] = 2;
    try std.testing.expectError(error.UnsupportedVersion, decode(encoded));
    encoded[6] = 1;
    @memset(encoded[22..30], 255);
    try std.testing.expectError(error.CorruptInput, decode(encoded));
    try std.testing.expectError(error.InvalidArgument, seal(alloc, std.Options.debug_io, provider.provider(), .{ .scope = "", .key = "key", .revision = 1 }, "value"));
    try std.testing.expectError(error.InvalidArgument, seal(alloc, std.Options.debug_io, provider.provider(), .{ .scope = "s", .key = "key", .revision = 0 }, "value"));
}

test "secret record fails closed without entropy and cleans up allocation failures" {
    var provider = TestProvider{};
    try std.testing.expectError(error.EntropyUnavailable, seal(std.testing.allocator, std.Io.failing, provider.provider(), test_identity, "value"));
    const Scenario = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var keys = TestProvider{};
            const record = try seal(alloc, std.Options.debug_io, keys.provider(), test_identity, "private-value");
            defer alloc.free(record);
            try std.testing.expect(std.mem.indexOf(u8, record, "private-value") == null);
            var plaintext = try open(alloc, keys.provider(), test_identity, record);
            defer plaintext.deinit(alloc);
            try std.testing.expectEqualStrings("private-value", plaintext.bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
}

test "secret record AFSE v1 matches an independent libsodium wire vector" {
    // Generated with PyNaCl 1.6.2 crypto_aead_xchacha20poly1305_ietf_encrypt.
    // Python prefix: struct.pack('<4sHHQHHHII24s', b'AFSE', 1, 1, 42,
    //   8, 14, 15, 18, 12, bytes(range(24))) followed by the four strings.
    const hex = "41465345010001002a0000000000000008000e000f00120000000c000000000102030405060708090a0b0c0d0e0f101112131415161774656e616e742d3170726f76696465722e746f6b656e746573742d766563746f722d6b656b6f70617175652d777261707065642d6b6579fdb06a1bf5bcf9c75228263104ee82ac52a02fb97c897f869a64588b";
    var expected: [hex.len / 2]u8 = undefined;
    _ = try std.fmt.hexToBytes(&expected, hex);
    var key: DataKey = undefined;
    for (&key, 0..) |*byte, i| byte.* = @intCast(i);
    defer std.crypto.secureZero(u8, &key);
    var nonce: [24]u8 = undefined;
    for (&nonce, 0..) |*byte, i| byte.* = @intCast(i);
    const wrapped = WrappedKey{ .key_id = @constCast("test-vector-kek"), .bytes = @constCast("opaque-wrapped-key") };
    const encoded = try encode(std.testing.allocator, test_identity, "credential\x00\xff", wrapped, nonce, &key);
    defer std.testing.allocator.free(encoded);
    try std.testing.expectEqualSlices(u8, &expected, encoded);
    const FixedVectorProvider = struct {
        fn wrap(_: *anyopaque, _: std.mem.Allocator, _: Identity, _: *const DataKey) !WrappedKey {
            return error.Unavailable;
        }
        fn unwrap(_: *anyopaque, _: Identity, id: []const u8, blob: []const u8, output: *DataKey) !void {
            if (!std.mem.eql(u8, id, "test-vector-kek") or !std.mem.eql(u8, blob, "opaque-wrapped-key")) return error.CorruptInput;
            for (output, 0..) |*byte, i| byte.* = @intCast(i);
        }
    };
    var context: u8 = 0;
    const provider = KeyProvider{ .ptr = &context, .vtable = &.{ .wrap = FixedVectorProvider.wrap, .unwrap = FixedVectorProvider.unwrap } };
    var plaintext = try open(std.testing.allocator, provider, test_identity, &expected);
    defer plaintext.deinit(std.testing.allocator);
    try std.testing.expectEqualSlices(u8, "credential\x00\xff", plaintext.bytes);
}
