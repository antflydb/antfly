// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Public-client OAuth/OIDC contract. No browser or database dependencies.
const std = @import("std");
pub const issuer = "https://auth.openai.com";
pub const resource = "https://api.openai.com/v1";
pub const scopes = "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct";
pub const Identity = struct { iss: []const u8, sub: []const u8, email: []const u8 = "", aud: std.json.Value, exp: i64, nonce: []const u8 };

pub fn encode(alloc: std.mem.Allocator, bytes: []const u8) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    for (bytes) |c| {
        if (std.ascii.isAlphanumeric(c) or std.mem.indexOfScalar(u8, "-._~", c) != null) try out.writer.writeByte(c) else try out.writer.print("%{X:0>2}", .{c});
    }
    return out.toOwnedSlice();
}
pub fn form(alloc: std.mem.Allocator, pairs: []const [2][]const u8) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    for (pairs, 0..) |pair, i| {
        const value = try encode(alloc, pair[1]);
        defer alloc.free(value);
        if (i != 0) try out.writer.writeByte('&');
        try out.writer.print("{s}={s}", .{ pair[0], value });
    }
    return out.toOwnedSlice();
}
pub fn base64(alloc: std.mem.Allocator, bytes: []const u8) ![]u8 {
    const e = std.base64.url_safe_no_pad.Encoder;
    const result = try alloc.alloc(u8, e.calcSize(bytes.len));
    _ = e.encode(result, bytes);
    return result;
}
pub fn decode(alloc: std.mem.Allocator, bytes: []const u8) ![]u8 {
    const d = std.base64.url_safe_no_pad.Decoder;
    const result = try alloc.alloc(u8, try d.calcSizeForSlice(bytes));
    errdefer alloc.free(result);
    try d.decode(result, bytes);
    return result;
}
pub fn hasScope(granted: []const u8, scope: []const u8) bool {
    var it = std.mem.tokenizeScalar(u8, granted, ' ');
    while (it.next()) |s| if (std.mem.eql(u8, s, scope)) return true;
    return false;
}
/// Returns a parsed identity owned by the caller. Only pinned RS256 signing
/// keys are accepted; JWT-provided URLs and algorithms never select a trust root.
pub fn verify(alloc: std.mem.Allocator, token: []const u8, jwks: []const u8, client_id: []const u8, nonce: []const u8, now: i64) !std.json.Parsed(Identity) {
    var it = std.mem.splitScalar(u8, token, '.');
    const header_segment = it.next() orelse return error.InvalidIdentity;
    const payload_segment = it.next() orelse return error.InvalidIdentity;
    const signature_segment = it.next() orelse return error.InvalidIdentity;
    if (it.next() != null) return error.InvalidIdentity;
    const header_bytes = try decode(alloc, header_segment);
    defer alloc.free(header_bytes);
    const Header = struct { alg: []const u8, kid: []const u8, crit: ?std.json.Value = null };
    var header = try std.json.parseFromSlice(Header, alloc, header_bytes, .{ .ignore_unknown_fields = true });
    defer header.deinit();
    if (!std.mem.eql(u8, header.value.alg, "RS256") or header.value.crit != null) return error.InvalidIdentity;
    const Key = struct { kty: []const u8, kid: []const u8 = "", n: []const u8 = "", e: []const u8 = "", alg: ?[]const u8 = null, use: ?[]const u8 = null };
    var keys = try std.json.parseFromSlice(struct { keys: []const Key }, alloc, jwks, .{ .ignore_unknown_fields = true });
    defer keys.deinit();
    var match: ?Key = null;
    for (keys.value.keys) |key| if (std.mem.eql(u8, key.kid, header.value.kid)) {
        if (match != null) return error.InvalidIdentity;
        match = key;
    };
    const key = match orelse return error.InvalidIdentity;
    if (!std.mem.eql(u8, key.kty, "RSA")) return error.InvalidIdentity;
    if (key.alg) |alg| if (!std.mem.eql(u8, alg, "RS256")) return error.InvalidIdentity;
    if (key.use) |use| if (!std.mem.eql(u8, use, "sig")) return error.InvalidIdentity;
    const modulus = try decode(alloc, key.n);
    defer alloc.free(modulus);
    const exponent = try decode(alloc, key.e);
    defer alloc.free(exponent);
    const signature = try decode(alloc, signature_segment);
    defer alloc.free(signature);
    const rsa = std.crypto.Certificate.rsa;
    const public_key = try rsa.PublicKey.fromBytes(exponent, modulus);
    const signed = token[0 .. header_segment.len + 1 + payload_segment.len];
    switch (modulus.len) {
        inline 256, 384, 512 => |len| {
            if (signature.len != len) return error.InvalidIdentity;
            const sig = rsa.PKCS1v1_5Signature.fromBytes(len, signature);
            rsa.PKCS1v1_5Signature.concatVerify(len, &sig, &.{signed}, public_key, std.crypto.hash.sha2.Sha256) catch return error.InvalidIdentity;
        },
        else => return error.InvalidIdentity,
    }
    const payload = try decode(alloc, payload_segment);
    defer alloc.free(payload);
    var identity = try std.json.parseFromSlice(Identity, alloc, payload, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
    errdefer identity.deinit();
    const id = identity.value;
    var audience_matches = false;
    switch (id.aud) {
        .string => |aud| audience_matches = std.mem.eql(u8, aud, client_id),
        .array => |audiences| {
            // Multiple audiences require azp validation; reject this unsupported
            // form rather than treating any audience as the authorized party.
            if (audiences.items.len == 1 and audiences.items[0] == .string) audience_matches = std.mem.eql(u8, audiences.items[0].string, client_id);
        },
        else => {},
    }
    if (!std.mem.eql(u8, id.iss, issuer) or id.sub.len == 0 or !audience_matches or id.exp <= now or !std.mem.eql(u8, id.nonce, nonce)) return error.InvalidIdentity;
    return identity;
}

test "chatgpt protocol scopes and form escaping" {
    const a = std.testing.allocator;
    const encoded = try form(a, &.{.{ "code", "a&b=+ /" }});
    defer a.free(encoded);
    try std.testing.expectEqualStrings("code=a%26b%3D%2B%20%2F", encoded);
    try std.testing.expect(hasScope(scopes, "chatgpt.tokens.use.direct"));
    try std.testing.expect(!hasScope("chatgpt.tokens.use.direct.extra", "chatgpt.tokens.use.direct"));
}
test "chatgpt protocol rejects unsigned identity" {
    try std.testing.expectError(error.InvalidIdentity, verify(std.testing.allocator, "eyJhbGciOiJub25lIiwia2lkIjoiYSJ9.e30.", "{\"keys\":[]}", "client", "nonce", 0));
}

// Fixtures contain only public signing material and signed example claims.
test "chatgpt protocol verifies signature and binds audience nonce and expiry" {
    const a = std.testing.allocator;
    const token = @embedFile("fixtures/identity.jwt");
    const jwks = @embedFile("fixtures/jwks.json");
    var identity = try verify(a, token, jwks, "issued-client", "test-nonce", 1);
    defer identity.deinit();
    try std.testing.expectEqualStrings("subject-a", identity.value.sub);
    try std.testing.expectError(error.InvalidIdentity, verify(a, token, jwks, "wrong-client", "test-nonce", 1));
    try std.testing.expectError(error.InvalidIdentity, verify(a, token, jwks, "issued-client", "wrong-nonce", 1));
    try std.testing.expectError(error.InvalidIdentity, verify(a, token, jwks, "issued-client", "test-nonce", 4102444800));
    const corrupt = try a.dupe(u8, token);
    defer a.free(corrupt);
    const last = corrupt.len - 2;
    corrupt[last] = if (corrupt[last] == 'A') 'B' else 'A';
    try std.testing.expectError(error.InvalidIdentity, verify(a, corrupt, jwks, "issued-client", "test-nonce", 1));
}
