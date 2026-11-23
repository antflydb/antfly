// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Private execution controls carried by the existing authenticated SQL-lock
//! participant. Scope is identity, never an authorization or a PREPARE vote.
const std = @import("std");
const reservations = @import("document_revision_reservation.zig");
const transactions = @import("transactions.zig");
const internal = @import("internal_keys.zig");

pub const Control = struct {
    scope: reservations.Scope,
    subtransaction: u64 = 0,
    action: enum { reserve, release, renew },
    keys: []const []const u8 = &.{},
    references: []const reservations.Reference = &.{},
    predicates: []const transactions.VersionPredicate = &.{},

    pub fn validate(self: Control, admission: anytype) !void {
        try requireScope(self.scope, admission);
        switch (self.action) {
            .reserve => {
                try validateKeys(self.keys);
                if (self.keys.len == 0 or self.references.len != 0 or self.predicates.len != self.keys.len) return error.InvalidLockEffect;
                for (self.keys, self.predicates) |key, predicate| {
                    if (!std.mem.eql(u8, key, predicate.key) or predicate.comparison != .document_version or predicate.expected_value != null or
                        (predicate.expected_version != 0 and predicate.expected_content_digest == null) or
                        (predicate.unique_absence and (predicate.expected_version != 0 or predicate.expected_content_digest != null))) return error.InvalidLockEffect;
                }
            },
            .release => {
                if (self.subtransaction != 0) return error.InvalidLockEffect;
                if (self.keys.len != 0 or self.predicates.len != 0) return error.InvalidLockEffect;
                try (reservations.Handoff{ .scope = self.scope, .references = self.references }).validate();
                for (self.references) |reference| try validateKeys(&.{reference.key});
            },
            .renew => if (self.subtransaction != 0 or self.keys.len != 0 or self.references.len != 0 or self.predicates.len != 0) return error.InvalidLockEffect,
        }
    }
};

pub const Query = struct {
    scope: reservations.Scope,
    keys: []const []const u8,
    pub fn validate(self: Query, admission: anytype) !void {
        try requireScope(self.scope, admission);
        try validateKeys(self.keys);
    }
};

pub const Observation = struct {
    header: ?reservations.Header,
    status: ?transactions.TxnStatus,
    references: []const reservations.Reference,
};

pub fn requireScope(scope: reservations.Scope, admission: anytype) !void {
    try scope.validate();
    try admission.validate();
    const ns = admission.binding.identity;
    if (admission.purpose != .execution or !std.mem.eql(u8, &scope.transaction_id, &admission.transaction) or
        !std.mem.eql(u8, &scope.principal_digest, &admission.source.lease.principal) or
        !std.mem.eql(u8, &scope.metadata_incarnation, &std.fmt.bytesToHex(admission.binding.namespace.incarnation, .lower)) or
        scope.table_id != ns.table_id or scope.shard_id != ns.shard_id or scope.range_id != ns.range_id) return error.InvalidLockScope;
}

pub fn validateKeys(keys: []const []const u8) !void {
    if (keys.len > reservations.max_rows) return error.LockPlanLimitExceeded;
    var bytes: usize = 0;
    for (keys, 0..) |key, index| {
        if (key.len == 0 or internal.isInternalUserKey(key) or std.mem.startsWith(u8, key, "\x00\x00__metadata__:") or
            std.mem.startsWith(u8, key, "splitstate:") or std.mem.startsWith(u8, key, "splitdelta:") or
            (index != 0 and std.mem.order(u8, keys[index - 1], key) != .lt)) return error.InvalidLockEffect;
        bytes = std.math.add(usize, bytes, key.len) catch return error.LockPlanLimitExceeded;
        if (bytes > reservations.max_key_bytes) return error.LockPlanLimitExceeded;
    }
}

/// Called inside the accepting writer under the primary apply fence. A
/// zero revision with a digest still requires an existing legacy primary.
pub fn requirePredicates(backing: std.mem.Allocator, txn: anytype, predicates: []const transactions.VersionPredicate) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const keys = try a.alloc([]const u8, predicates.len);
    const primary = try a.alloc(?[]const u8, predicates.len);
    const timestamps = try a.alloc(?[]const u8, predicates.len);
    for (predicates, keys) |predicate, *key| key.* = try internal.documentKeyAlloc(a, predicate.key);
    try txn.getManySorted(keys, primary);
    for (predicates, keys) |predicate, *key| key.* = try internal.ttlKeyAlloc(a, predicate.key);
    try txn.getManySorted(keys, timestamps);
    for (predicates, primary, timestamps) |predicate, value, timestamp| {
        if (predicate.expected_content_digest) |expected| {
            const bytes = value orelse return error.VersionConflict;
            const version = if (timestamp) |raw| @import("ttl.zig").writeVersion(raw) orelse return error.InvalidTimestamp else 0;
            if (version != predicate.expected_version) return error.VersionConflict;
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
            if (!std.mem.eql(u8, &digest, &expected)) return error.VersionConflict;
        } else if (value != null or timestamp != null) {
            return if (predicate.unique_absence and value != null) error.UniqueConstraintViolation else error.VersionConflict;
        }
    }
}

/// The vector uses a; its key bytes borrow the validated query.
pub fn observe(a: std.mem.Allocator, read: anytype, query: Query) !Observation {
    const header = try reservations.load(read, query.scope.transaction_id);
    if (header) |held| if (!held.scope.eql(query.scope)) return error.LockOwnerChanged;
    const record = try transactions.externalParticipant(read, query.scope.transaction_id);
    if ((header == null) != (record == null)) {
        // An ordinary SQL-lock participant may predate the first reservation.
        if (header != null) return error.InvalidTxnRecord;
    }
    var result: std.ArrayList(reservations.Reference) = .empty;
    errdefer result.deinit(a);
    if (header != null and query.keys.len != 0) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const addresses = try scratch.alloc([]const u8, query.keys.len);
        const values = try scratch.alloc(?[]const u8, query.keys.len);
        for (query.keys, addresses) |key, *address| address.* = try reservations.rowKey(scratch, query.scope.transaction_id, key);
        try read.getManySorted(addresses, values);
        for (query.keys, values) |key, value| if (value) |raw| {
            try result.append(a, .{ .key = key, .version = try reservations.rowVersion(raw) });
        };
    }
    return .{ .header = header, .status = if (record) |native| native.status else null, .references = try result.toOwnedSlice(a) };
}

test "document revision reservation controls reject actor namespace purpose unsafe predicates and malformed windows" {
    const realm: [16]u8 = @splat(1);
    const admission: @import("pessimistic_lock/participant.zig").Admission = .{
        .purpose = .execution,
        .binding = .{ .namespace = .{ .home = .data, .group = 7, .incarnation = realm }, .identity = .{ .table_id = 3, .shard_id = 7, .range_id = 8 } },
        .source = .{ .group = 1, .lease = .{ .backend = .{ .incarnation = realm, .id = @splat(2), .fence = 1 }, .database = 4, .node = 5, .principal = @splat(6), .expires_at_ms = 1000 }, .connection = @splat('a'), .nonce = @splat(8), .standalone = false },
        .transaction = @splat(9),
        .generation = 1,
        .parent_command = .{ .sequence = 1, .nonce = @splat(10), .executor = @splat(11) },
        .serial = 1,
    };
    const scope: reservations.Scope = .{ .metadata_incarnation = std.fmt.bytesToHex(realm, .lower), .transaction_id = admission.transaction, .principal_digest = admission.source.lease.principal, .begin_timestamp = 10, .table_id = 3, .shard_id = 7, .range_id = 8 };
    const request: Control = .{ .scope = scope, .action = .reserve, .keys = &.{"a"}, .predicates = &.{.{ .key = "a", .expected_version = 9, .expected_content_digest = @splat(7) }} };
    try request.validate(admission);
    var zero = request;
    zero.predicates = &.{.{ .key = "a", .expected_version = 0, .expected_content_digest = @splat(7) }};
    try zero.validate(admission);
    zero.predicates = &.{.{ .key = "a", .expected_version = 0, .expected_content_digest = @splat(7), .unique_absence = true }};
    try std.testing.expectError(error.InvalidLockEffect, zero.validate(admission));
    var wrong = request;
    wrong.scope.principal_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidLockScope, wrong.validate(admission));
    wrong = request;
    wrong.scope.transaction_id[0] ^= 1;
    try std.testing.expectError(error.InvalidLockScope, wrong.validate(admission));
    wrong = request;
    wrong.scope.range_id += 1;
    try std.testing.expectError(error.InvalidLockScope, wrong.validate(admission));
    var purpose = admission;
    purpose.purpose = .resolution;
    try std.testing.expectError(error.InvalidLockScope, request.validate(purpose));
    wrong = request;
    wrong.predicates = &.{};
    try std.testing.expectError(error.InvalidLockEffect, wrong.validate(admission));
    wrong = request;
    wrong.predicates = &.{.{ .key = "a", .comparison = .exact_value }};
    try std.testing.expectError(error.InvalidLockEffect, wrong.validate(admission));
    wrong = request;
    wrong.predicates = &.{.{ .key = "a", .expected_version = 9 }};
    try std.testing.expectError(error.InvalidLockEffect, wrong.validate(admission));
    wrong = request;
    wrong.predicates = &.{.{ .key = "b" }};
    try std.testing.expectError(error.InvalidLockEffect, wrong.validate(admission));
    wrong = request;
    wrong.keys = &.{ "b", "a" };
    try std.testing.expectError(error.InvalidLockEffect, wrong.validate(admission));
    wrong = request;
    wrong.keys = &.{"\x00\x00__metadata__:private"};
    try std.testing.expectError(error.InvalidLockEffect, wrong.validate(admission));
    wrong = .{ .scope = scope, .action = .release, .references = &.{.{ .key = "a", .version = 0 }} };
    try std.testing.expectError(error.InvalidTxnRecord, wrong.validate(admission));
    wrong = .{ .scope = scope, .action = .release, .references = &.{.{ .key = "splitstate:private", .version = 9 }} };
    try std.testing.expectError(error.InvalidLockEffect, wrong.validate(admission));
    try (Control{ .scope = scope, .action = .renew }).validate(admission);
    wrong = request;
    wrong.action = .renew;
    try std.testing.expectError(error.InvalidLockEffect, wrong.validate(admission));
    const query: Query = .{ .scope = scope, .keys = &.{"a"} };
    try query.validate(admission);
    var mixed: @import("pessimistic_lock/control.zig").Query = .{ .admission = admission, .document_revisions = query };
    try mixed.validate();
    mixed.intent_key = "a";
    try std.testing.expectError(error.InvalidLockScope, mixed.validate());
}

test "document revision reservation predicates distinguish legacy presence from insert absence" {
    const View = struct {
        present: bool = true,
        pub fn getManySorted(self: @This(), keys: []const []const u8, values: []?[]const u8) !void {
            const a = std.testing.allocator;
            const physical = try internal.documentKeyAlloc(a, "a");
            defer a.free(physical);
            for (keys, values) |key, *value| value.* = if (self.present and std.mem.eql(u8, key, physical)) "{}" else null;
        }
    };
    const a = std.testing.allocator;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("{}", &digest, .{});
    const legacy: transactions.VersionPredicate = .{ .key = "a", .expected_content_digest = digest };
    const insert: transactions.VersionPredicate = .{ .key = "a", .unique_absence = true };
    try requirePredicates(a, View{}, &.{legacy});
    try std.testing.expectError(error.UniqueConstraintViolation, requirePredicates(a, View{}, &.{insert}));
    try std.testing.expectError(error.VersionConflict, requirePredicates(a, View{ .present = false }, &.{legacy}));
    try requirePredicates(a, View{ .present = false }, &.{insert});
}
