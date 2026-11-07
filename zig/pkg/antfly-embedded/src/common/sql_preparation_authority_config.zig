// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Private SQL authority neither admits public principals nor authenticates
//! node transport. One versioned keystore record atomically publishes the pair.
const std = @import("std");
const secrets = @import("secrets.zig");
const A = std.mem.Allocator;
pub const bundle_key = "antfly.sql_preparation.authority";
pub const secret_key = "antfly.sql_preparation.secret";
pub const issuer_key = "antfly.sql_preparation.issuer";
const Bundle = struct { version: u32, secret: []const u8, issuer: []const u8 };
pub const Owned = struct {
    secret: []u8,
    issuer: []u8,
    pub fn deinit(self: *Owned, a: A) void {
        @memset(self.secret, 0);
        a.free(self.secret);
        a.free(self.issuer);
        self.* = undefined;
    }
};
fn validate(secret: []const u8, issuer: []const u8) !void {
    if (secret.len == 0 or secret.len > 4096 or issuer.len == 0 or issuer.len > 256 or !std.unicode.utf8ValidateSlice(issuer) or std.mem.indexOfScalar(u8, issuer, 0) != null) return error.InvalidSqlPreparationAuthority;
}
fn decode(a: A, json: []const u8) !Owned {
    if (json.len > 8192) return error.InvalidSqlPreparationAuthority;
    var parsed = std.json.parseFromSlice(Bundle, a, json, .{}) catch |failure| return if (failure == error.OutOfMemory) failure else error.InvalidSqlPreparationAuthority;
    defer parsed.deinit();
    if (parsed.value.version != 1) return error.InvalidSqlPreparationAuthority;
    try validate(parsed.value.secret, parsed.value.issuer);
    const secret = try a.dupe(u8, parsed.value.secret);
    errdefer {
        @memset(secret, 0);
        a.free(secret);
    }
    return .{ .secret = secret, .issuer = try a.dupe(u8, parsed.value.issuer) };
}
/// Cluster runtimes pass false; independently generated node keys cannot
/// authenticate each other. Standalone permits generation only after role
/// validation for its normal writable deployment. No ephemeral fallback.
pub fn loadRuntime(a: A, io: std.Io, store: ?*secrets.FileStore, allow_generate: bool) !?Owned {
    if (store) |configured| return loadUsingIo(a, configured, io, allow_generate);
    var environment = try secrets.FileStore.initConfiguredWithIo(a, io, .{});
    defer environment.deinit();
    return loadUsingIo(a, &environment, io, false);
}
pub fn load(a: A, store: ?*secrets.FileStore, allow_generate: bool) !?Owned {
    const file = store orelse return null;
    return loadUsingIo(a, file, file.io, allow_generate);
}
fn loadUsingIo(a: A, file: *secrets.FileStore, random_io: std.Io, allow_generate: bool) !?Owned {
    const secret_read: ?secrets.ResolvedSecret = file.getOwnedWithGeneration(a, secret_key) catch |failure| switch (failure) {
        error.SecretNotFound => null,
        else => return failure,
    };
    const secret = if (secret_read) |value| value.value else null;
    errdefer if (secret) |value| {
        @memset(value, 0);
        a.free(value);
    };
    const issuer_read: ?secrets.ResolvedSecret = file.getOwnedWithGeneration(a, issuer_key) catch |failure| switch (failure) {
        error.SecretNotFound => null,
        else => return failure,
    };
    const issuer = if (issuer_read) |value| value.value else null;
    errdefer if (issuer) |value| a.free(value);
    if (secret != null or issuer != null) {
        if (secret == null or issuer == null) return error.InvalidSqlPreparationAuthority;
        if (secret_read.?.generation != issuer_read.?.generation) return error.InvalidSqlPreparationAuthority;
        try validate(secret.?, issuer.?);
        return .{ .secret = secret.?, .issuer = issuer.? };
    }
    if (try file.getOwned(a, bundle_key)) |encoded| {
        defer {
            @memset(encoded, 0);
            a.free(encoded);
        }
        return try decode(a, encoded);
    }
    if (!allow_generate or !file.writable or !file.has_file or file.native_config != null) return null;
    var entropy: [48]u8 = undefined;
    defer @memset(&entropy, 0);
    try random_io.randomSecure(&entropy);
    var key = std.fmt.bytesToHex(entropy[0..32].*, .lower);
    defer @memset(&key, 0);
    const identity = std.fmt.bytesToHex(entropy[32..48].*, .lower);
    const issuer_value = try std.fmt.allocPrint(a, "standalone-sql:{s}", .{identity});
    defer a.free(issuer_value);
    const encoded = try std.json.Stringify.valueAlloc(a, Bundle{ .version = 1, .secret = &key, .issuer = issuer_value }, .{});
    defer {
        @memset(encoded, 0);
        a.free(encoded);
    }
    // FileStore's atomic rename/fsync publishes both values together. Never
    // return credentials before the durable write succeeds and is observable.
    var recorded = try file.put(a, bundle_key, encoded);
    defer recorded.deinit(a);
    const accepted = (try file.getOwned(a, bundle_key)) orelse return error.InvalidSqlPreparationAuthority;
    defer {
        @memset(accepted, 0);
        a.free(accepted);
    }
    return try decode(a, accepted);
}

pub fn checkStandaloneLifecycle(a: A, io: std.Io) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(directory);
    const path = try std.fs.path.join(a, &.{ directory, "private-authority.json" });
    defer a.free(path);
    var first: Owned = undefined;
    {
        var store = try secrets.FileStore.initWithIo(a, io, path);
        defer store.deinit();
        store.environment_enabled = false;
        // Cluster/readonly boot never creates a node-local key.
        try std.testing.expectEqual(@as(?Owned, null), try load(a, &store, false));
        store.writable = false;
        try std.testing.expectEqual(@as(?Owned, null), try load(a, &store, true));
        try std.testing.expectEqual(@as(?[]u8, null), try store.getOwned(a, bundle_key));
        store.writable = true;
        first = (try load(a, &store, true)).?;
        // Neither public authentication nor node transport is provisioned.
        try std.testing.expectEqual(@as(?[]u8, null), try store.getOwned(a, "antfly.trusted_principal.secret"));
        try std.testing.expectEqual(@as(?[]u8, null), try store.getOwned(a, "antfly.internal_service.secret"));
        const encoded = (try store.getOwned(a, bundle_key)).?;
        defer {
            @memset(encoded, 0);
            a.free(encoded);
        }
        var decoded = try decode(a, encoded);
        defer decoded.deinit(a);
        try std.testing.expectEqualStrings(first.secret, decoded.secret);
        try std.testing.expectEqualStrings(first.issuer, decoded.issuer);
    }
    defer first.deinit(a);
    var reopened = try secrets.FileStore.initWithIo(a, io, path);
    defer reopened.deinit();
    reopened.environment_enabled = false;
    reopened.writable = false;
    var second = (try load(a, &reopened, false)).?;
    defer second.deinit(a);
    try std.testing.expectEqualStrings(first.secret, second.secret);
    try std.testing.expectEqualStrings(first.issuer, second.issuer);
    // Explicit configured pair takes precedence; a partial pair is an error
    // rather than permission to retain, generate or use a different bundle.
    reopened.writable = true;
    var secret = try reopened.put(a, secret_key, "configured-cluster-key");
    defer secret.deinit(a);
    try std.testing.expectError(error.InvalidSqlPreparationAuthority, load(a, &reopened, true));
    var issuer = try reopened.put(a, issuer_key, "configured-cluster-issuer");
    defer issuer.deinit(a);
    var explicit = (try load(a, &reopened, false)).?;
    defer explicit.deinit(a);
    try std.testing.expectEqualStrings("configured-cluster-key", explicit.secret);
    try std.testing.expectEqualStrings("configured-cluster-issuer", explicit.issuer);
    _ = try reopened.delete(secret_key);
    _ = try reopened.delete(issuer_key);
    var broken = try reopened.put(a, bundle_key, "{\"version\":2,\"secret\":\"must-not-rotate\",\"issuer\":\"bad\"}");
    defer broken.deinit(a);
    try std.testing.expectError(error.InvalidSqlPreparationAuthority, load(a, &reopened, true));
    const retained = (try reopened.getOwned(a, bundle_key)).?;
    defer {
        @memset(retained, 0);
        a.free(retained);
    }
    try std.testing.expectEqualStrings("{\"version\":2,\"secret\":\"must-not-rotate\",\"issuer\":\"bad\"}", retained);
}

test "dedicated SQL preparation authority persists atomically across restart without public auth or transport borrowing" {
    try checkStandaloneLifecycle(std.testing.allocator, std.testing.io);
}
