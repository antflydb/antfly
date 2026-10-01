// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Mounted bootstrap keyring. Reloaded per operation so atomic file replacement
//! can rotate the active wrapping key while retaining previous decrypt keys.
const std = @import("std");
const record = @import("secret_record.zig");
const contract = @import("secret_contract.zig");
const Aead = std.crypto.aead.chacha_poly.XChaCha20Poly1305;

pub const Keyring = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    const Config = struct { active: []const u8, keys: []const struct { id: []const u8, key: []const u8 } };

    pub fn validate(self: *Keyring) !void {
        var loaded = try self.load(null);
        loaded.deinit(self.alloc);
    }

    pub fn provider(self: *Keyring) record.KeyProvider {
        return .{ .ptr = self, .vtable = &.{ .wrap = wrap, .unwrap = unwrap } };
    }
    const Loaded = struct {
        id: []u8,
        key: record.DataKey,
        fn deinit(self: *Loaded, alloc: std.mem.Allocator) void {
            std.crypto.secureZero(u8, &self.key);
            alloc.free(self.id);
        }
    };
    fn load(self: *Keyring, requested: ?[]const u8) !Loaded {
        const raw = std.Io.Dir.cwd().readFileAlloc(self.io, self.path, self.alloc, .limited(64 * 1024)) catch return error.Unavailable;
        defer {
            std.crypto.secureZero(u8, raw);
            self.alloc.free(raw);
        }
        // Borrow JSON strings where possible; scrub decoded escapes as well.
        var parsed = std.json.parseFromSlice(Config, self.alloc, raw, .{ .allocate = .alloc_if_needed }) catch return error.CorruptInput;
        defer parsed.deinit();
        defer for (parsed.value.keys) |item| std.crypto.secureZero(u8, @constCast(item.key));
        const id = requested orelse parsed.value.active;
        try contract.validateName(parsed.value.active);
        var found: ?record.DataKey = null;
        defer if (found) |*key| std.crypto.secureZero(u8, key);
        var has_active = false;
        for (parsed.value.keys, 0..) |item, i| {
            try contract.validateName(item.id);
            for (parsed.value.keys[0..i]) |previous| if (std.mem.eql(u8, previous.id, item.id)) return error.CorruptInput;
            if (item.key.len != 64) return error.CorruptInput;
            var key: record.DataKey = undefined;
            defer std.crypto.secureZero(u8, &key);
            _ = std.fmt.hexToBytes(&key, item.key) catch return error.CorruptInput;
            has_active = has_active or std.mem.eql(u8, item.id, parsed.value.active);
            if (std.mem.eql(u8, id, item.id)) found = key;
        }
        if (!has_active) return error.CorruptInput;
        const key = found orelse return error.Unavailable;
        return .{ .id = try self.alloc.dupe(u8, id), .key = key };
    }
    fn aad(alloc: std.mem.Allocator, identity: record.Identity, id: []const u8) ![]u8 {
        // JSON array framing is unambiguous, including names containing delimiters.
        return std.json.Stringify.valueAlloc(alloc, .{ "antfly-wrapping-key-v1", identity.scope, identity.key, identity.revision, id }, .{});
    }
    fn wrap(ptr: *anyopaque, alloc: std.mem.Allocator, identity: record.Identity, key: *const record.DataKey) !record.WrappedKey {
        const self: *Keyring = @ptrCast(@alignCast(ptr));
        var loaded = try self.load(null);
        defer loaded.deinit(self.alloc);
        const associated = try aad(alloc, identity, loaded.id);
        defer alloc.free(associated);
        const out = try alloc.alloc(u8, 72);
        errdefer alloc.free(out);
        try self.io.randomSecure(out[0..24]);
        var tag: [16]u8 = undefined;
        Aead.encrypt(out[24..56], &tag, key, associated, out[0..24].*, loaded.key);
        @memcpy(out[56..72], &tag);
        return .{ .key_id = try alloc.dupe(u8, loaded.id), .bytes = out };
    }
    fn unwrap(ptr: *anyopaque, identity: record.Identity, id: []const u8, bytes: []const u8, key: *record.DataKey) !void {
        const self: *Keyring = @ptrCast(@alignCast(ptr));
        if (bytes.len != 72) return error.CorruptInput;
        var loaded = try self.load(id);
        defer loaded.deinit(self.alloc);
        const associated = try aad(self.alloc, identity, id);
        defer self.alloc.free(associated);
        Aead.decrypt(key, bytes[24..56], bytes[56..72].*, associated, bytes[0..24].*, loaded.key) catch {
            std.crypto.secureZero(u8, key);
            return error.CorruptInput;
        };
    }
};
