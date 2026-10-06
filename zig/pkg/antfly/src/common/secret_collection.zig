// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded per-scope ciphertext snapshots shared by Raft and object storage.
//! Storage is trusted for freshness and absence; AFSE authenticates each value.
const std = @import("std");
const contract = @import("secret_contract.zig");
const record = @import("secret_record.zig");
const Allocator = std.mem.Allocator;

pub const max_bytes = 8 * 1024 * 1024;
pub const max_entries = 1024;
const legacy_header_bytes = 20;
const header_bytes = 36;

fn headerSize(raw: []const u8) !usize {
    if (raw.len < legacy_header_bytes or raw.len > max_bytes or !std.mem.eql(u8, raw[0..4], "AFSC")) return error.CorruptInput;
    const size: usize = switch (std.mem.readInt(u16, raw[4..6], .little)) {
        1 => legacy_header_bytes,
        2 => header_bytes,
        else => return error.UnsupportedVersion,
    };
    if (raw.len < size) return error.CorruptInput;
    return size;
}

pub fn storedScope(raw: []const u8) ![]const u8 {
    const size = try headerSize(raw);
    const len = std.mem.readInt(u16, raw[6..8], .little);
    if (len > raw.len - size) return error.CorruptInput;
    return raw[size..][0..len];
}

pub const Snapshot = struct {
    bytes: ?[]u8 = null,
    /// Backend-owned CAS identity (e.g. ETag), allocated with the read allocator.
    token: ?[]u8 = null,

    pub fn deinit(self: *Snapshot, alloc: Allocator) void {
        if (self.bytes) |bytes| alloc.free(bytes);
        if (self.token) |token| alloc.free(token);
        self.* = undefined;
    }
};

pub const Entry = struct { envelope: []const u8, view: record.View };
pub const View = struct {
    revision: u64,
    entries: []Entry,

    pub fn deinit(self: *View, alloc: Allocator) void {
        alloc.free(self.entries);
        self.* = undefined;
    }

    pub fn find(self: View, key: []const u8) ?Entry {
        for (self.entries) |entry| {
            if (std.mem.eql(u8, entry.view.identity.key, key)) return entry;
        }
        return null;
    }
};

/// Framing validation only. Never derives authorization from stored identities.
pub fn decode(alloc: Allocator, scope: []const u8, bytes: ?[]const u8) !View {
    try contract.validateName(scope);
    const raw = bytes orelse return .{ .revision = 0, .entries = try alloc.alloc(Entry, 0) };
    const size = try headerSize(raw);
    const scope_len = std.mem.readInt(u16, raw[6..8], .little);
    const revision = std.mem.readInt(u64, raw[8..16], .little);
    const count = std.mem.readInt(u32, raw[16..20], .little);
    if (revision == 0 or count > max_entries or scope_len > raw.len - size) return error.CorruptInput;
    if (!std.mem.eql(u8, scope, raw[size..][0..scope_len])) return error.CorruptInput;
    const entries = try alloc.alloc(Entry, count);
    errdefer alloc.free(entries);
    var offset: usize = size + scope_len;
    for (entries, 0..) |*entry, i| {
        if (raw.len - offset < 4) return error.CorruptInput;
        const len = std.mem.readInt(u32, raw[offset..][0..4], .little);
        offset += 4;
        if (len > raw.len - offset) return error.CorruptInput;
        const envelope = raw[offset..][0..len];
        const view = try record.decode(envelope);
        if (!std.mem.eql(u8, view.identity.scope, scope) or view.identity.revision > revision) return error.CorruptInput;
        if (i > 0 and !std.mem.lessThan(u8, entries[i - 1].view.identity.key, view.identity.key)) return error.CorruptInput;
        entry.* = .{ .envelope = envelope, .view = view };
        offset += len;
    }
    if (offset != raw.len) return error.CorruptInput;
    return .{ .revision = revision, .entries = entries };
}

pub fn replace(alloc: Allocator, io: std.Io, scope: []const u8, previous: View, key: []const u8, envelope: ?[]const u8) ![]u8 {
    const revision = std.math.add(u64, previous.revision, 1) catch return error.Unavailable;
    const count = previous.entries.len - @as(usize, if (previous.find(key) != null) 1 else 0) + @as(usize, if (envelope != null) 1 else 0);
    if (count > max_entries) return error.ResourceRequestTooLarge;
    if (envelope) |bytes| {
        const view = try record.decode(bytes);
        if (!(contract.Identity{ .scope = scope, .key = key, .revision = revision }).eql(view.identity)) return error.CorruptInput;
    }
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(alloc);
    var header: [header_bytes]u8 = undefined;
    @memcpy(header[0..4], "AFSC");
    std.mem.writeInt(u16, header[4..6], 2, .little);
    // Distinguish even identical concurrent deletes when observing forwarded
    // Raft publication. Only the exact prepared attempt may report success.
    try io.randomSecure(header[20..36]);
    std.mem.writeInt(u16, header[6..8], @intCast(scope.len), .little);
    std.mem.writeInt(u64, header[8..16], revision, .little);
    std.mem.writeInt(u32, header[16..20], @intCast(count), .little);
    try out.appendSlice(alloc, &header);
    try out.appendSlice(alloc, scope);
    var pending = envelope;
    for (previous.entries) |entry| {
        if (pending) |bytes| {
            if (!std.mem.lessThan(u8, entry.view.identity.key, key)) {
                try appendEnvelope(alloc, &out, bytes);
                pending = null;
            }
        }
        if (!std.mem.eql(u8, entry.view.identity.key, key)) try appendEnvelope(alloc, &out, entry.envelope);
    }
    if (pending) |bytes| try appendEnvelope(alloc, &out, bytes);
    return out.toOwnedSlice(alloc);
}

fn appendEnvelope(alloc: Allocator, out: *std.ArrayList(u8), envelope: []const u8) !void {
    if (envelope.len + 4 > max_bytes - out.items.len) return error.ResourceRequestTooLarge;
    var len: [4]u8 = undefined;
    std.mem.writeInt(u32, &len, @intCast(envelope.len), .little);
    try out.appendSlice(alloc, &len);
    try out.appendSlice(alloc, envelope);
}

pub fn scopeDigest(scope: []const u8) [64]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(scope, &hash, .{});
    return std.fmt.bytesToHex(hash, .lower);
}

/// Backend.read must return a linearizable owned snapshot, including absence.
/// Backend.publish must atomically compare its snapshot and durably publish,
/// returning Conflict only for a known non-commit; ambiguous results propagate
/// as OutcomeUnknown. Providers are borrowed and must support concurrent calls.
pub fn Store(comptime Backend: type) type {
    return struct {
        const Self = @This();
        allocator: Allocator,
        io: std.Io,
        scope: []u8,
        provider: record.KeyProvider,
        backend: Backend,

        pub fn init(alloc: Allocator, io: std.Io, scope: []const u8, provider: record.KeyProvider, backend: Backend) !Self {
            try contract.validateName(scope);
            return .{ .allocator = alloc, .io = io, .scope = try alloc.dupe(u8, scope), .provider = provider, .backend = backend };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.scope);
            self.* = undefined;
        }

        pub fn source(self: *Self) contract.Source {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .list_metadata = list, .refresh = refresh } };
        }

        pub fn nativeStore(self: *Self) contract.NativeStore {
            return .{ .source = self.source(), .writer = .{ .ptr = self, .vtable = &.{ .put = put, .remove_override = remove } } };
        }

        fn authorize(self: *Self, scope: []const u8) !void {
            if (!std.mem.eql(u8, scope, self.scope)) return error.Unauthorized;
        }

        fn resolve(ptr: *anyopaque, alloc: Allocator, scope: []const u8, key: []const u8, options: contract.ReadOptions) !contract.Lookup {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.authorize(scope);
            var snapshot = try self.backend.read(alloc, scope);
            defer snapshot.deinit(alloc);
            var view = try decode(alloc, scope, snapshot.bytes);
            defer view.deinit(alloc);
            if (view.revision < options.min_revision) return error.Unavailable;
            const entry = view.find(key) orelse return .{ .revision = view.revision };
            const secret = try record.open(alloc, self.provider, .{ .scope = scope, .key = key, .revision = entry.view.identity.revision }, entry.envelope);
            return .{ .revision = view.revision, .value = .{ .revision = entry.view.identity.revision, .secret = secret } };
        }

        fn list(ptr: *anyopaque, alloc: Allocator, scope: []const u8, options: contract.ReadOptions) !contract.Listing {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.authorize(scope);
            var snapshot = try self.backend.read(alloc, scope);
            defer snapshot.deinit(alloc);
            var view = try decode(alloc, scope, snapshot.bytes);
            defer view.deinit(alloc);
            if (view.revision < options.min_revision) return error.Unavailable;
            var entries: std.ArrayList(contract.Metadata) = .empty;
            errdefer {
                for (entries.items) |entry| alloc.free(entry.key);
                entries.deinit(alloc);
            }
            for (view.entries) |entry| {
                const key = try alloc.dupe(u8, entry.view.identity.key);
                errdefer alloc.free(key);
                try entries.append(alloc, .{ .key = key, .revision = entry.view.identity.revision });
            }
            return .{ .revision = view.revision, .entries = try entries.toOwnedSlice(alloc) };
        }

        fn refresh(ptr: *anyopaque, scope: []const u8) !contract.Health {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.authorize(scope);
            var snapshot = try self.backend.read(self.allocator, scope);
            defer snapshot.deinit(self.allocator);
            var view = try decode(self.allocator, scope, snapshot.bytes);
            defer view.deinit(self.allocator);
            return .{ .revision = view.revision };
        }

        fn put(ptr: *anyopaque, scope: []const u8, key: []const u8, value: []const u8, expected: contract.ExpectedRevision) !contract.Mutation {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.mutate(scope, key, value, expected);
        }

        fn remove(ptr: *anyopaque, scope: []const u8, key: []const u8, expected: contract.ExpectedRevision) !contract.Mutation {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.mutate(scope, key, null, expected);
        }

        fn mutate(self: *Self, scope: []const u8, key: []const u8, value: ?[]const u8, expected: contract.ExpectedRevision) !contract.Mutation {
            try self.authorize(scope);
            const alloc = self.allocator;
            // Retry preparation for unrelated concurrent changes. The caller's
            // entry precondition is rechecked after every scope CAS conflict.
            for (0..8) |_| {
                var snapshot = try self.backend.read(alloc, scope);
                defer snapshot.deinit(alloc);
                var previous = try decode(alloc, scope, snapshot.bytes);
                defer previous.deinit(alloc);
                const current = previous.find(key);
                try expected.check(if (current) |entry| entry.view.identity.revision else null);
                if (value == null and current == null) return .{ .revision = previous.revision, .changed = false };
                const revision = std.math.add(u64, previous.revision, 1) catch return error.Unavailable;
                const envelope = if (value) |plaintext| try record.seal(alloc, self.io, self.provider, .{ .scope = scope, .key = key, .revision = revision }, plaintext) else null;
                defer if (envelope) |bytes| alloc.free(bytes);
                const bytes = try replace(alloc, self.io, scope, previous, key, envelope);
                defer alloc.free(bytes);
                self.backend.publish(alloc, scope, snapshot, bytes) catch |err| switch (err) {
                    error.Conflict => continue,
                    else => return err,
                };
                return .{ .revision = revision };
            }
            return error.Conflict;
        }
    };
}
