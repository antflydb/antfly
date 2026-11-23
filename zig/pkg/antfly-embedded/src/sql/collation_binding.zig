// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Statement-owned comparison domains. Resolve a named collation once before
//! binding; a row cannot load libraries, query catalogs or re-resolve names.
const std = @import("std");
const records = @import("../system_catalog/collations.zig");
const native = @import("native_collation.zig");
pub const Binary = enum { c, posix };
pub const Witness = union(enum) { binary: Binary, named: records.Bound };
pub const Strength = enum { default, implicit, explicit, indeterminate };

pub const View = struct {
    witness: Witness,
    prepared: ?native.Prepared = null,
    locale: []const u8 = "",
    allocator: ?std.mem.Allocator = null,

    pub fn init(provider: *const native.Provider, a: std.mem.Allocator, record: records.Record) !View {
        try record.validate();
        const version: native.Version = .{ .collation = record.definition.version.collation, .uca = record.definition.version.uca };
        var prepared = try provider.prepare(a, record.definition.locale, record.definition.deterministic, version);
        errdefer prepared.deinit();
        return .{ .witness = .{ .named = record.bound() }, .prepared = prepared, .locale = try a.dupe(u8, record.definition.locale), .allocator = a };
    }
    pub fn deinit(self: *View) void {
        if (self.prepared) |*prepared| prepared.deinit();
        if (self.allocator) |a| a.free(self.locale);
        self.* = undefined;
    }
    pub fn sameDomain(left: *const View, right: *const View) bool {
        return std.meta.eql(left.witness, right.witness);
    }
    pub fn compare(self: *const View, left: []const u8, right: []const u8) !std.math.Order {
        switch (self.witness) {
            .binary => return std.mem.order(u8, left, right),
            .named => |bound| {
                const prepared = self.prepared orelse return error.SqlUnboundCollation;
                if (!std.meta.eql(bound.version.collation, prepared.version.collation) or !std.meta.eql(bound.version.uca, prepared.version.uca) or bound.deterministic != prepared.deterministic)
                    return error.SqlCollationVersionMismatch;
                return prepared.compare(left, right);
            },
        }
    }
};

/// `unresolved` is a durable witness only. It can never execute without native
/// rebinding to a verified provider; JSON cannot construct an ICU handle.
pub const Binding = struct {
    strength: Strength = .default,
    view: ?*const View = null,
    unresolved: ?*const Witness = null,

    pub fn identity(self: Binding) ?Witness {
        return if (self.view) |view| view.witness else if (self.unresolved) |witness| witness.* else null;
    }
    pub fn eql(left: Binding, right: Binding) bool {
        return left.strength == right.strength and std.meta.eql(left.identity(), right.identity());
    }
    pub fn validate(self: Binding) !void {
        if (self.view != null and self.unresolved != null) return error.SqlUnboundCollation;
        const witness = self.identity();
        if ((self.strength == .default or self.strength == .indeterminate) != (witness == null)) return error.SqlUnboundCollation;
        if (witness) |value| if (value == .named) try value.named.validate();
    }
    pub fn requireExecutable(self: Binding) !void {
        try self.validate();
        if (self.strength == .indeterminate) return error.SqlIndeterminateCollation;
        if (self.unresolved != null) return error.SqlUnboundCollation;
    }
    pub fn merge(left: Binding, right: Binding) !Binding {
        try left.validate();
        try right.validate();
        if (left.strength == .explicit and right.strength == .explicit) {
            if (!std.meta.eql(left.identity(), right.identity())) return error.SqlCollationMismatch;
            return left;
        }
        if (left.strength == .explicit) return left;
        if (right.strength == .explicit) return right;
        if (left.strength == .indeterminate or right.strength == .indeterminate) return .{ .strength = .indeterminate };
        if (left.strength == .default) return right;
        if (right.strength == .default) return left;
        if (!std.meta.eql(left.identity(), right.identity())) return .{ .strength = .indeterminate };
        return left;
    }
    pub fn compare(left: Binding, right: Binding, lhs: []const u8, rhs: []const u8) !std.math.Order {
        const domain = try merge(left, right);
        try domain.requireExecutable();
        return if (domain.view) |view| view.compare(lhs, rhs) else std.mem.order(u8, lhs, rhs);
    }
    pub const Wire = struct { strength: Strength, witness: ?Witness };
    pub fn nativeJsonProjection(self: Binding) Wire {
        return .{ .strength = self.strength, .witness = self.identity() };
    }
    pub fn jsonStringify(self: Binding, writer: anytype) !void {
        return writer.write(self.nativeJsonProjection());
    }
    pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!Binding {
        const wire = try std.json.innerParse(Wire, a, source, options);
        const witness = if (wire.witness) |witness_value| blk: {
            const owned = try a.create(Witness);
            owned.* = witness_value;
            break :blk owned;
        } else null;
        const result: Binding = .{ .strength = wire.strength, .unresolved = witness };
        result.validate() catch return error.UnexpectedToken;
        return result;
    }
    pub fn jsonParseFromValue(a: std.mem.Allocator, value: std.json.Value, options: std.json.ParseOptions) std.json.ParseFromValueError!Binding {
        const wire = try std.json.innerParseFromValue(Wire, a, value, options);
        const witness = if (wire.witness) |witness_value| blk: {
            const owned = try a.create(Witness);
            owned.* = witness_value;
            break :blk owned;
        } else null;
        const result: Binding = .{ .strength = wire.strength, .unresolved = witness };
        result.validate() catch return error.UnexpectedToken;
        return result;
    }
};

pub const Name = struct { database: ?[]const u8 = null, namespace: ?[]const u8 = null, name: []const u8 };
pub const Resolver = struct {
    ptr: *anyopaque,
    resolve: *const fn (*anyopaque, std.mem.Allocator, Name) anyerror!*const View,
    resolve_bound: ?*const fn (*anyopaque, std.mem.Allocator, records.Bound) anyerror!*const View = null,
};

test "SQL collation derivation keeps explicit conflicts and implicit indeterminacy distinct" {
    const c: View = .{ .witness = .{ .binary = .c } };
    const posix: View = .{ .witness = .{ .binary = .posix } };
    const implicit: Binding = .{ .strength = .implicit, .view = &c };
    const explicit: Binding = .{ .strength = .explicit, .view = &c };
    const other: Binding = .{ .strength = .implicit, .view = &posix };
    try std.testing.expect(Binding.eql(implicit, try Binding.merge(.{}, implicit)));
    try std.testing.expect(Binding.eql(explicit, try Binding.merge(other, explicit)));
    const ambiguous = try Binding.merge(implicit, other);
    try std.testing.expectEqual(Strength.indeterminate, ambiguous.strength);
    try std.testing.expectError(error.SqlIndeterminateCollation, ambiguous.requireExecutable());
    try std.testing.expectError(error.SqlCollationMismatch, Binding.merge(explicit, .{ .strength = .explicit, .view = &posix }));
    try std.testing.expect(Binding.eql(explicit, try Binding.merge(ambiguous, explicit)));
    const wire = try std.json.Stringify.valueAlloc(std.testing.allocator, explicit, .{});
    defer std.testing.allocator.free(wire);
    var restored = try std.json.parseFromSlice(Binding, std.testing.allocator, wire, .{});
    defer restored.deinit();
    try std.testing.expect(Binding.eql(explicit, restored.value));
    try std.testing.expectError(error.SqlUnboundCollation, restored.value.requireExecutable());
}
