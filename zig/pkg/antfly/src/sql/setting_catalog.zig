// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Catalog-owned SQL settings. The native owner loads one authorized catalog
//! snapshot; a statement copies it with its session overlay before binding.
//! Policy settings never enter a client-writable overlay. Callers must carry
//! the resulting View to every local or remote evaluator of that statement.
const std = @import("std");

const durable = @import("../system_catalog/settings.zig");
pub const Kind = durable.Kind;
pub const Value = durable.Value;
pub const Identity = durable.Identity;
pub const Scope = durable.Scope;
pub const Definition = durable.Definition;
pub const RawSnapshot = durable.Snapshot;

/// Implementations must read definition identities, defaults, and the catalog
/// epoch at one durable visibility cut, after authenticating the exact scope.
/// load returns all borrowed or allocated slices in the supplied allocator's
/// lifetime. The View owns that allocator and releases it on deinit().
/// Publication is a separate administrator-only metadata Raft operation;
/// SQL binding never receives that capability.
pub const Owner = struct {
    ptr: *anyopaque,
    load: *const fn (*anyopaque, std.mem.Allocator, Scope) anyerror!RawSnapshot,
};

pub const OverlayEntry = struct { identity: Identity, value: Value };

/// Deep-owned immutable input to binding and evaluation. Keep one View alive
/// through all bound programs and remote read requests for the statement.
pub const View = struct {
    arena: std.heap.ArenaAllocator,
    scope: Scope,
    epoch: u64,
    definitions: []const Definition,
    values: []const Value,

    pub fn capture(backing: std.mem.Allocator, owner: Owner, scope: Scope, overlay: []const OverlayEntry) !View {
        var arena = std.heap.ArenaAllocator.init(backing);
        errdefer arena.deinit();
        const alloc = arena.allocator();
        const raw = try owner.load(owner.ptr, alloc, scope);
        if (!std.mem.eql(u8, raw.scope.principal, scope.principal) or
            !std.mem.eql(u8, raw.scope.database, scope.database) or raw.epoch == 0)
            return error.InvalidSettingCatalogSnapshot;
        if (raw.definitions.len > 1024 or overlay.len > 1024) return error.SettingLimitExceeded;
        const definitions = try alloc.alloc(Definition, raw.definitions.len);
        const values = try alloc.alloc(Value, raw.definitions.len);
        for (raw.definitions, 0..) |source, i| {
            try validateDefinition(source);
            for (raw.definitions[0..i]) |prior| {
                if (prior.identity.id == source.identity.id or std.ascii.eqlIgnoreCase(prior.name, source.name))
                    return error.InvalidSettingCatalogSnapshot;
            }
            definitions[i] = source;
            definitions[i].name = try alloc.dupe(u8, source.name);
            definitions[i].default = try copyValue(alloc, source.default);
            definitions[i].database_default = if (source.database_default) |v| try copyValue(alloc, v) else null;
            definitions[i].role_default = if (source.role_default) |v| try copyValue(alloc, v) else null;
            const effective = source.role_default orelse source.database_default orelse source.default;
            values[i] = try copyValue(alloc, effective);
        }
        for (overlay, 0..) |entry, i| {
            for (overlay[0..i]) |prior| if (prior.identity.id == entry.identity.id) return error.InvalidSettingOverlay;
            const index = indexById(definitions, entry.identity.id) orelse return error.SettingCatalogChanged;
            const def = definitions[index];
            if (def.identity.generation != entry.identity.generation) return error.SettingCatalogChanged;
            if (!def.session_writable or def.policy_sensitive) return error.SettingWriteForbidden;
            try validateValue(def.kind, entry.value);
            values[index] = try copyValue(alloc, entry.value);
        }
        const owned_scope: Scope = .{
            .principal = try alloc.dupe(u8, scope.principal),
            .database = try alloc.dupe(u8, scope.database),
        };
        return .{ .arena = arena, .scope = owned_scope, .epoch = raw.epoch, .definitions = definitions, .values = values };
    }

    pub fn deinit(self: *View) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn resolve(self: View, name: []const u8) !struct { identity: Identity, value: Value } {
        for (self.definitions, self.values) |definition, value| {
            if (std.ascii.eqlIgnoreCase(definition.name, name)) return .{ .identity = definition.identity, .value = value };
        }
        return error.UnknownSetting;
    }

    /// Prepared plans retain identity and generation; execution supplies the
    /// newly captured value only if the definition is still the same one.
    pub fn resolveDependency(self: View, identity: Identity) !Value {
        const index = indexById(self.definitions, identity.id) orelse return error.SettingCatalogChanged;
        if (self.definitions[index].identity.generation != identity.generation) return error.SettingCatalogChanged;
        return self.values[index];
    }
};

fn indexById(definitions: []const Definition, id: u64) ?usize {
    for (definitions, 0..) |definition, i| if (definition.identity.id == id) return i;
    return null;
}

fn validateDefinition(definition: Definition) !void {
    if (definition.identity.id == 0 or definition.identity.generation == 0 or
        definition.name.len == 0 or definition.name.len > 128 or
        (definition.policy_sensitive and definition.session_writable))
        return error.InvalidSettingCatalogSnapshot;
    for (definition.name) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_' and ch != '.') return error.InvalidSettingCatalogSnapshot;
    try validateValue(definition.kind, definition.default);
    if (definition.database_default) |v| try validateValue(definition.kind, v);
    if (definition.role_default) |v| try validateValue(definition.kind, v);
}

fn validateValue(kind: Kind, value: Value) !void {
    if (std.meta.activeTag(value) != kind) return error.InvalidSettingValue;
    if (value == .string) {
        if (value.string.len > 4096 or !std.unicode.utf8ValidateSlice(value.string)) return error.InvalidSettingValue;
    }
}

fn copyValue(alloc: std.mem.Allocator, value: Value) !Value {
    return switch (value) {
        .boolean => |v| .{ .boolean = v },
        .integer => |v| .{ .integer = v },
        .string => |v| .{ .string = try alloc.dupe(u8, v) },
    };
}

test "setting view owns scope allocations made after snapshot capture" {
    const Fixture = struct {
        fn load(_: *anyopaque, _: std.mem.Allocator, scope: Scope) !RawSnapshot {
            return .{ .scope = scope, .epoch = 1, .definitions = &.{} };
        }
    };
    var marker: u8 = 0;
    const principal = try std.testing.allocator.alloc(u8, 8192);
    defer std.testing.allocator.free(principal);
    @memset(principal, 'a');
    const database = try std.testing.allocator.alloc(u8, 8192);
    defer std.testing.allocator.free(database);
    @memset(database, 'b');
    var view = try View.capture(std.testing.allocator, .{ .ptr = &marker, .load = Fixture.load }, .{ .principal = principal, .database = database }, &.{});
    defer view.deinit();
    try std.testing.expectEqualStrings(principal, view.scope.principal);
    try std.testing.expectEqualStrings(database, view.scope.database);
}

test "setting view pins defaults and values while rejecting client policy escalation" {
    const Fake = struct {
        definitions: []const Definition,
        epoch: u64 = 7,
        fn load(ptr: *anyopaque, _: std.mem.Allocator, scope: Scope) !RawSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .scope = scope, .epoch = self.epoch, .definitions = self.definitions };
        }
        fn owner(self: *@This()) Owner {
            return .{ .ptr = self, .load = load };
        }
    };
    var tenant = [_]u8{ 'a', 'l', 'p', 'h', 'a' };
    var fake: Fake = .{ .definitions = &.{
        .{ .identity = .{ .id = 1, .generation = 3 }, .name = "app.tenant", .kind = .string, .policy_sensitive = true, .default = .{ .string = "none" }, .database_default = .{ .string = "db" }, .role_default = .{ .string = &tenant } },
        .{ .identity = .{ .id = 2, .generation = 1 }, .name = "app.color", .kind = .string, .session_writable = true, .default = .{ .string = "blue" } },
    } };
    const scope: Scope = .{ .principal = "alice", .database = "main" };
    var view = try View.capture(std.testing.allocator, fake.owner(), scope, &.{.{ .identity = .{ .id = 2, .generation = 1 }, .value = .{ .string = "green" } }});
    defer view.deinit();
    tenant[0] = 'x';
    try std.testing.expectEqualStrings("alpha", (try view.resolve("app.tenant")).value.string);
    try std.testing.expectEqualStrings("green", (try view.resolve("APP.COLOR")).value.string);
    try std.testing.expectError(error.SettingWriteForbidden, View.capture(std.testing.allocator, fake.owner(), scope, &.{.{ .identity = .{ .id = 1, .generation = 3 }, .value = .{ .string = "other" } }}));
    try std.testing.expectError(error.SettingCatalogChanged, view.resolveDependency(.{ .id = 2, .generation = 2 }));
    try std.testing.expectError(error.SettingCatalogChanged, View.capture(std.testing.allocator, fake.owner(), scope, &.{.{ .identity = .{ .id = 2, .generation = 2 }, .value = .{ .string = "green" } }}));
    fake.epoch = 8;
    try std.testing.expectEqual(@as(u64, 7), view.epoch);
}

test "setting view rejects malformed owner snapshots and duplicate overlays" {
    const Fake = struct {
        raw: RawSnapshot,
        fn load(ptr: *anyopaque, _: std.mem.Allocator, _: Scope) !RawSnapshot {
            return (@as(*@This(), @ptrCast(@alignCast(ptr)))).raw;
        }
        fn owner(self: *@This()) Owner {
            return .{ .ptr = self, .load = load };
        }
    };
    const scope: Scope = .{ .principal = "alice", .database = "main" };
    const definition: Definition = .{ .identity = .{ .id = 1, .generation = 1 }, .name = "app.note", .kind = .string, .session_writable = true, .default = .{ .string = "" } };
    var fake: Fake = .{ .raw = .{ .scope = scope, .epoch = 1, .definitions = &.{definition} } };
    const entry: OverlayEntry = .{ .identity = definition.identity, .value = .{ .string = "ok" } };
    try std.testing.expectError(error.InvalidSettingOverlay, View.capture(std.testing.allocator, fake.owner(), scope, &.{ entry, entry }));
    fake.raw.scope.principal = "mallory";
    try std.testing.expectError(error.InvalidSettingCatalogSnapshot, View.capture(std.testing.allocator, fake.owner(), scope, &.{}));
    fake.raw.scope = scope;
    fake.raw.definitions = &.{ definition, definition };
    try std.testing.expectError(error.InvalidSettingCatalogSnapshot, View.capture(std.testing.allocator, fake.owner(), scope, &.{}));
}

test "SQL current_setting evaluates from one authorized pinned view" {
    const catalog = @import("catalog.zig");
    const compiler = @import("compiler.zig");
    const describe = @import("describe.zig");
    const runtime = @import("runtime.zig");
    const Fake = struct {
        definition: Definition,
        unavailable: bool = false,
        fn load(ptr: *anyopaque, _: std.mem.Allocator, scope: Scope) !RawSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.unavailable) return error.SettingCatalogUnavailable;
            return .{ .scope = scope, .epoch = 11, .definitions = @as([*]const Definition, @ptrCast(&self.definition))[0..1] };
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn resolve(_: *anyopaque, _: std.mem.Allocator, name: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
            return .{ .id = if (std.mem.eql(u8, name.table, "a")) 1 else 2, .physical_name = name.table, .schema_version = 1, .columns = &.{.{ .name = "id", .path = "id", .type = .integer, .nullable = false }} };
        }
    };
    var native: Fake = .{ .definition = .{ .identity = .{ .id = 9, .generation = 4 }, .name = "app.tenant", .kind = .string, .policy_sensitive = true, .default = .{ .string = "denied" }, .role_default = .{ .string = "alice-tenant" } } };
    const owner: Owner = .{ .ptr = &native, .load = Fake.load };
    var view = try View.capture(std.testing.allocator, owner, .{ .principal = "alice", .database = "main" }, &.{});
    defer view.deinit();
    var compiled = try compiler.compile(std.testing.allocator, "SELECT current_setting('app.tenant')", .{});
    defer compiled.deinit();
    const plain: catalog.Backend = .{ .ptr = &native, .vtable = &.{ .resolve = Fake.resolve, .scan = undefined, .mutate = undefined, .checkpoint = Fake.checkpoint } };
    try std.testing.expectError(error.SettingCatalogUnavailable, runtime.execute(std.testing.allocator, plain, &compiled, &.{}, .{}));
    var with_capture = plain;
    with_capture.setting_capture = .{ .owner = owner, .scope = .{ .principal = "alice", .database = "main" } };
    var description = try describe.describe(std.testing.allocator, with_capture, &compiled, &.{});
    defer description.deinit();
    try std.testing.expectEqualStrings("alice-tenant", (try description.binding.scalars.projections[0].?.evaluate(std.testing.allocator, &.{}, &.{}, .{})).value.string);
    var result = try runtime.execute(std.testing.allocator, with_capture, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("alice-tenant", result.output.rows[0][0].string);
    var joined = try compiler.compile(std.testing.allocator, "SELECT a.id FROM a JOIN b ON a.id = b.id AND current_setting('app.tenant') = 'alice-tenant'", .{});
    defer joined.deinit();
    try std.testing.expectError(error.SettingCatalogUnavailable, describe.describe(std.testing.allocator, plain, &joined, &.{}));
    var joined_description = try describe.describe(std.testing.allocator, with_capture, &joined, &.{});
    defer joined_description.deinit();
    try std.testing.expect(joined_description.binding.relation != null);
    with_capture.setting_capture.?.overlay = &.{.{ .identity = native.definition.identity, .value = .{ .string = "other" } }};
    try std.testing.expectError(error.SettingWriteForbidden, runtime.execute(std.testing.allocator, with_capture, &compiled, &.{}, .{}));
    with_capture.setting_capture.?.overlay = &.{};
    with_capture.settings_view = &view;
    native.unavailable = true;
    try std.testing.expectError(error.SettingCatalogUnavailable, runtime.execute(std.testing.allocator, with_capture, &compiled, &.{}, .{}));
    try std.testing.expectError(error.SettingCatalogUnavailable, describe.describe(std.testing.allocator, with_capture, &compiled, &.{}));
    native.unavailable = false;
    with_capture.settings_view = null;
    native.definition.role_default = .{ .string = "changed" };
    try std.testing.expectEqualStrings("alice-tenant", result.output.rows[0][0].string);

    const scalar = @import("scalar.zig");
    var expression = try compiler.compileScalar(std.testing.allocator, "current_setting('app.tenant')", .{});
    defer expression.deinit();
    var program = try scalar.bindExpectedWithSettings(std.testing.allocator, expression.expression, &.{}, &.{}, null, .{}, &view);
    defer program.deinit();
    native.definition.identity.generation = 5;
    var newer = try View.capture(std.testing.allocator, owner, .{ .principal = "alice", .database = "main" }, &.{});
    defer newer.deinit();
    program.settings = &newer;
    try std.testing.expectError(error.SettingCatalogChanged, program.evaluate(std.testing.allocator, &.{}, &.{}, .{}));

    var dynamic = try compiler.compileScalar(std.testing.allocator, "current_setting($1)", .{});
    defer dynamic.deinit();
    try std.testing.expectError(error.UnsupportedSqlShape, scalar.bindExpectedWithSettings(std.testing.allocator, dynamic.expression, &.{}, &.{.string}, null, .{}, &view));
}
