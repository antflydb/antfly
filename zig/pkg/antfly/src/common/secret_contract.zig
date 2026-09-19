// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Storage-independent native secret contract. Handles borrow their provider;
//! owned results use the caller's allocator. Providers enforce authorization,
//! freshness, and durable atomic compare-and-swap. No secret value is public API metadata.
const std = @import("std");
const callback = @import("../runtime_callback_abi.zig");

pub const Revision = u64;
pub const max_identity_bytes = 1024;
pub const max_value_bytes = 1024 * 1024;

pub fn validateName(name: []const u8) !void {
    if (name.len == 0 or name.len > max_identity_bytes or
        std.mem.indexOfScalar(u8, name, 0) != null or !std.unicode.utf8ValidateSlice(name)) return error.InvalidArgument;
}

pub const Identity = struct {
    scope: []const u8,
    key: []const u8,
    revision: Revision,

    pub fn validate(self: Identity) !void {
        try validateName(self.scope);
        try validateName(self.key);
        if (self.revision == 0) return error.InvalidArgument;
    }

    pub fn eql(self: Identity, other: Identity) bool {
        return self.revision == other.revision and std.mem.eql(u8, self.scope, other.scope) and std.mem.eql(u8, self.key, other.key);
    }
};

/// Best-effort erasure of this owned allocation, not a promise about caller
/// copies, provider buffers, swap, or crash dumps.
pub const SecretBytes = struct {
    bytes: []u8,

    pub fn deinit(self: *SecretBytes, alloc: std.mem.Allocator) void {
        std.crypto.secureZero(u8, self.bytes);
        alloc.free(self.bytes);
        self.* = undefined;
    }
};

pub const Value = struct {
    secret: SecretBytes,
    revision: Revision,
};

pub const Lookup = struct {
    /// Monotonic scope snapshot revision, including when the key is absent.
    revision: Revision,
    value: ?Value = null,

    pub fn deinit(self: *Lookup, alloc: std.mem.Allocator) void {
        if (self.value) |*value| value.secret.deinit(alloc);
        self.* = undefined;
    }
};

pub const ReadOptions = struct {
    /// Source-local revision, never a revision from a different source.
    min_revision: Revision = 0,
};

pub const Metadata = struct {
    key: []u8,
    revision: Revision,
};

pub const Listing = struct {
    revision: Revision,
    entries: []Metadata,

    pub fn deinit(self: *Listing, alloc: std.mem.Allocator) void {
        for (self.entries) |entry| alloc.free(entry.key);
        alloc.free(self.entries);
        self.* = undefined;
    }
};

pub const Health = struct {
    revision: Revision,
    stale: bool = false,
    available: bool = true,
};

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    const Boundary = callback.Boundary(VTable);

    pub const VTable = struct {
        resolve: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, ReadOptions) anyerror!Lookup,
        list_metadata: *const fn (*anyopaque, std.mem.Allocator, []const u8, ReadOptions) anyerror!Listing,
        refresh: *const fn (*anyopaque, []const u8) anyerror!Health,
    };

    pub fn resolve(self: Source, alloc: std.mem.Allocator, scope: []const u8, key: []const u8, options: ReadOptions) !Lookup {
        try validateName(scope);
        try validateName(key);
        var result = try Boundary.call("resolve", self.dispatch, self.vtable.resolve, .{ self.ptr, alloc, scope, key, options });
        errdefer result.deinit(alloc);
        if (result.revision < options.min_revision) return error.Unavailable;
        if (result.value) |value| {
            if (value.revision == 0 or value.revision > result.revision or value.secret.bytes.len > max_value_bytes) return error.CorruptInput;
        }
        return result;
    }

    pub fn listMetadata(self: Source, alloc: std.mem.Allocator, scope: []const u8, options: ReadOptions) !Listing {
        try validateName(scope);
        var result = try Boundary.call("list_metadata", self.dispatch, self.vtable.list_metadata, .{ self.ptr, alloc, scope, options });
        errdefer result.deinit(alloc);
        if (result.revision < options.min_revision) return error.Unavailable;
        for (result.entries) |entry| {
            try validateName(entry.key);
            if (entry.revision == 0 or entry.revision > result.revision) return error.CorruptInput;
        }
        return result;
    }

    pub fn refresh(self: Source, scope: []const u8) !Health {
        try validateName(scope);
        return Boundary.call("refresh", self.dispatch, self.vtable.refresh, .{ self.ptr, scope });
    }
};

pub const ExpectedRevision = union(enum) {
    any,
    absent,
    /// Expected current entry revision (not the scope snapshot revision).
    exact: Revision,

    /// Called under the backend's commit lock/transaction, not before it.
    pub fn check(self: ExpectedRevision, current: ?Revision) !void {
        switch (self) {
            .any => {},
            .absent => if (current != null) return error.Conflict,
            .exact => |expected| {
                if (expected == 0) return error.InvalidArgument;
                if (current == null or current.? != expected) return error.Conflict;
            },
        }
    }
};

pub const Mutation = struct {
    /// Committed scope revision; put also assigns it to the changed entry.
    revision: Revision,
    /// False only for removal of an already absent native override.
    changed: bool = true,
};

/// No writer exists on a read-only source. Removing an override reveals the
/// next source; it never creates a resolver-level deletion tombstone.
pub const NativeStore = struct {
    source: Source,
    writer: Writer,

    pub const Writer = struct {
        ptr: *anyopaque,
        vtable: *const VTable,
        dispatch: Boundary.Dispatch = Boundary.local_dispatch,
        const Boundary = callback.Boundary(VTable);
        pub const VTable = struct {
            put: *const fn (*anyopaque, []const u8, []const u8, []const u8, ExpectedRevision) anyerror!Mutation,
            remove_override: *const fn (*anyopaque, []const u8, []const u8, ExpectedRevision) anyerror!Mutation,
        };

        pub fn put(self: Writer, scope: []const u8, key: []const u8, value: []const u8, expected: ExpectedRevision) !Mutation {
            try validateRequest(scope, key, expected);
            if (value.len > max_value_bytes) return error.InvalidArgument;
            const result = try Boundary.call("put", self.dispatch, self.vtable.put, .{ self.ptr, scope, key, value, expected });
            if (!result.changed) return error.CorruptInput;
            try validateMutation(result, expected);
            return result;
        }

        pub fn removeOverride(self: Writer, scope: []const u8, key: []const u8, expected: ExpectedRevision) !Mutation {
            try validateRequest(scope, key, expected);
            const result = try Boundary.call("remove_override", self.dispatch, self.vtable.remove_override, .{ self.ptr, scope, key, expected });
            try validateMutation(result, expected);
            return result;
        }

        fn validateMutation(result: Mutation, expected: ExpectedRevision) !void {
            if (result.changed and result.revision == 0) return error.CorruptInput;
            if (expected == .exact and (!result.changed or result.revision <= expected.exact)) return error.CorruptInput;
        }

        fn validateRequest(scope: []const u8, key: []const u8, expected: ExpectedRevision) !void {
            try validateName(scope);
            try validateName(key);
            if (expected == .exact and expected.exact == 0) return error.InvalidArgument;
        }
    };
};

pub const NamedSource = struct {
    name: []const u8,
    source: Source,
    options: ReadOptions = .{},
};

pub const Resolved = struct {
    /// Borrowed from the configured source list, never from persisted input.
    source_name: []const u8,
    lookup: Lookup,

    pub fn deinit(self: *Resolved, alloc: std.mem.Allocator) void {
        self.lookup.deinit(alloc);
        self.* = undefined;
    }
};

/// The caller assembles native, external sources, and optional environment in
/// priority order. Only a successful absence permits fallback. Even empty
/// values win. Unavailable/corrupt/unauthorized sources stop resolution.
pub fn resolveOrdered(alloc: std.mem.Allocator, sources: []const NamedSource, scope: []const u8, key: []const u8) !?Resolved {
    try validateName(scope);
    try validateName(key);
    for (sources) |source| {
        var result = try source.source.resolve(alloc, scope, key, source.options);
        if (result.value != null) return .{ .source_name = source.name, .lookup = result };
        result.deinit(alloc);
    }
    return null;
}

test "secret contract only authoritative absence permits fallback" {
    const Fake = struct {
        value: ?[]const u8 = null,
        unavailable: bool = false,
        calls: usize = 0,
        fn resolve(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8, _: ReadOptions) !Lookup {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.unavailable) return error.Unavailable;
            return .{ .revision = 3, .value = if (self.value) |v| .{ .secret = .{ .bytes = try alloc.dupe(u8, v) }, .revision = 2 } else null };
        }
        fn list(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: ReadOptions) !Listing {
            return .{ .revision = 3, .entries = try alloc.alloc(Metadata, 0) };
        }
        fn refresh(_: *anyopaque, _: []const u8) !Health {
            return .{ .revision = 3 };
        }
        fn source(self: *@This()) Source {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .list_metadata = list, .refresh = refresh } };
        }
    };
    const alloc = std.testing.allocator;
    var first = Fake{};
    var second = Fake{ .value = "fallback" };
    const sources = [_]NamedSource{ .{ .name = "native", .source = first.source() }, .{ .name = "external", .source = second.source() } };
    var resolved = (try resolveOrdered(alloc, &sources, "tenant", "token")).?;
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("external", resolved.source_name);
    first.value = "";
    var empty = (try resolveOrdered(alloc, &sources, "tenant", "token")).?;
    defer empty.deinit(alloc);
    try std.testing.expectEqualStrings("native", empty.source_name);
    try std.testing.expectEqual(@as(usize, 0), empty.lookup.value.?.secret.bytes.len);
    first.unavailable = true;
    try std.testing.expectError(error.Unavailable, resolveOrdered(alloc, &sources, "tenant", "token"));
    try std.testing.expectEqual(@as(usize, 1), second.calls);
    first.unavailable = false;
    try std.testing.expectError(error.Unavailable, first.source().resolve(alloc, "tenant", "token", .{ .min_revision = 4 }));
    first.value = null;
    try std.testing.expectError(error.Unavailable, first.source().resolve(alloc, "tenant", "token", .{ .min_revision = 4 }));
    try std.testing.expectError(error.Unavailable, first.source().listMetadata(alloc, "tenant", .{ .min_revision = 4 }));
    var listing = try first.source().listMetadata(alloc, "tenant", .{});
    defer listing.deinit(alloc);
    try std.testing.expectEqual(@as(Revision, 3), (try first.source().refresh("tenant")).revision);
    try (ExpectedRevision{ .exact = 3 }).check(3);
    try std.testing.expectError(error.Conflict, (ExpectedRevision{ .exact = 2 }).check(3));
    try std.testing.expectError(error.Conflict, @as(ExpectedRevision, .absent).check(3));
    try @as(ExpectedRevision, .absent).check(null);
    try std.testing.expectError(error.InvalidArgument, (ExpectedRevision{ .exact = 0 }).check(null));
}

test "secret contract native mutations preserve conditional writes and deletion revisions" {
    const Fake = struct {
        revision: Revision = 0,
        current: ?Revision = null,
        value: ?[]u8 = null,
        fn writer(self: *@This()) NativeStore.Writer {
            return .{ .ptr = self, .vtable = &.{ .put = put, .remove_override = remove } };
        }
        fn put(ptr: *anyopaque, _: []const u8, _: []const u8, value: []const u8, expected: ExpectedRevision) !Mutation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try expected.check(self.current);
            const next = try std.testing.allocator.dupe(u8, value);
            if (self.value) |old| {
                var bytes = SecretBytes{ .bytes = old };
                bytes.deinit(std.testing.allocator);
            }
            self.value = next;
            self.revision += 1;
            self.current = self.revision;
            return .{ .revision = self.revision };
        }
        fn remove(ptr: *anyopaque, _: []const u8, _: []const u8, expected: ExpectedRevision) !Mutation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try expected.check(self.current);
            if (self.value) |old| {
                var bytes = SecretBytes{ .bytes = old };
                bytes.deinit(std.testing.allocator);
                self.value = null;
                self.current = null;
                self.revision += 1;
                return .{ .revision = self.revision };
            }
            return .{ .revision = self.revision, .changed = false };
        }
    };
    var backend = Fake{};
    defer if (backend.value) |v| {
        var bytes = SecretBytes{ .bytes = v };
        bytes.deinit(std.testing.allocator);
    };
    const writer = backend.writer();
    const created = try writer.put("scope", "key", "first", .absent);
    try std.testing.expectError(error.Conflict, writer.put("scope", "key", "lost update", .absent));
    const updated = try writer.put("scope", "key", "second", .{ .exact = created.revision });
    try std.testing.expect(updated.revision > created.revision);
    try std.testing.expectError(error.Conflict, writer.removeOverride("scope", "key", .{ .exact = created.revision }));
    try std.testing.expectEqualStrings("second", backend.value.?);
    const removed = try writer.removeOverride("scope", "key", .{ .exact = updated.revision });
    try std.testing.expect(removed.revision > updated.revision);
    try std.testing.expect(!(try writer.removeOverride("scope", "key", .absent)).changed);
    const recreated = try writer.put("scope", "key", "third", .absent);
    try std.testing.expect(recreated.revision > removed.revision);
    try std.testing.expectError(error.Conflict, writer.put("scope", "key", "stale", .{ .exact = updated.revision }));
}
