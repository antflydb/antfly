// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Typed, composable workload commands for deterministic scenarios.
//!
//! Command-class, command, structured-parameter, and actor scheduling choices
//! remain separate. This avoids letting one opaque random value determine an
//! entire operation and gives reducers stable parameter identities.

const std = @import("std");
const ids = @import("id.zig");
const transition = @import("transition.zig");

pub const CommandId = ids.StableId;
pub const ActorId = ids.StableId;

pub const Class = enum { setup, driver, observer, recovery };

pub const ParameterSpec = struct {
    id: ids.StableId,
    name: []const u8,
    minimum: i64,
    maximum: i64,
    default: i64,
    boundaries: []const i64 = &.{},

    pub fn named(name: []const u8, minimum: i64, maximum: i64, default: i64) ParameterSpec {
        return .{
            .id = ids.stable("command-parameter", name),
            .name = name,
            .minimum = minimum,
            .maximum = maximum,
            .default = default,
        };
    }

    pub fn validate(self: ParameterSpec) !void {
        if (self.id == 0) return error.InvalidCommandParameterId;
        if (self.name.len == 0) return error.EmptyCommandParameterName;
        if (self.minimum > self.maximum or self.default < self.minimum or self.default > self.maximum)
            return error.InvalidCommandParameterRange;
        var prior: ?i64 = null;
        for (self.boundaries) |boundary| {
            if (boundary < self.minimum or boundary > self.maximum) return error.CommandParameterBoundaryOutOfRange;
            if (prior != null and boundary <= prior.?) return error.NonCanonicalCommandParameterBoundaries;
            prior = boundary;
        }
    }
};

pub const ParameterBuilder = struct {
    specs: std.ArrayListUnmanaged(ParameterSpec) = .empty,

    pub fn deinit(self: *ParameterBuilder, allocator: std.mem.Allocator) void {
        self.specs.deinit(allocator);
        self.* = .{};
    }

    pub fn add(self: *ParameterBuilder, allocator: std.mem.Allocator, spec: ParameterSpec) !void {
        try spec.validate();
        try self.specs.append(allocator, spec);
    }

    pub fn canonicalize(self: *ParameterBuilder) !void {
        std.mem.sort(ParameterSpec, self.specs.items, {}, struct {
            fn lessThan(_: void, lhs: ParameterSpec, rhs: ParameterSpec) bool {
                return lhs.id < rhs.id;
            }
        }.lessThan);
        for (self.specs.items, 0..) |spec, index| {
            if (index > 0 and self.specs.items[index - 1].id == spec.id) return error.DuplicateCommandParameterId;
        }
    }
};

pub const Value = struct {
    id: ids.StableId,
    value: i64,

    pub fn named(name: []const u8, value: i64) Value {
        return .{ .id = ids.stable("command-parameter", name), .value = value };
    }
};

pub const Parameters = struct {
    values: []const Value,

    pub fn validate(self: Parameters, schema: []const ParameterSpec) !void {
        if (self.values.len != schema.len) return error.CommandParameterCountMismatch;
        var prior: ids.StableId = 0;
        for (self.values, 0..) |value, index| {
            if (index > 0 and value.id <= prior) return error.NonCanonicalCommandParameters;
            prior = value.id;
            const spec = schema[index];
            if (value.id != spec.id) return error.CommandParameterSchemaMismatch;
            if (value.value < spec.minimum or value.value > spec.maximum) return error.CommandParameterOutOfRange;
        }
    }

    pub fn digest(self: Parameters) u64 {
        var result = ids.stable("command-parameters", "empty");
        for (self.values) |value| result = ids.derive("command-parameter-value", result ^ value.id, @bitCast(value.value));
        return result;
    }
};

pub fn Command(comptime Model: type, comptime World: type) type {
    return struct {
        id: CommandId,
        name: []const u8,
        class: Class,
        enabled_fn: *const fn (*const Model) bool,
        start_fn: *const fn (*World, Parameters) anyerror!ActorId,
        parameter_schema_fn: *const fn (*ParameterBuilder, std.mem.Allocator) anyerror!void,

        const Self = @This();

        pub fn named(
            class: Class,
            name: []const u8,
            enabled_fn: *const fn (*const Model) bool,
            start_fn: *const fn (*World, Parameters) anyerror!ActorId,
            parameter_schema_fn: *const fn (*ParameterBuilder, std.mem.Allocator) anyerror!void,
        ) Self {
            return .{
                .id = ids.stable("command", name),
                .name = name,
                .class = class,
                .enabled_fn = enabled_fn,
                .start_fn = start_fn,
                .parameter_schema_fn = parameter_schema_fn,
            };
        }

        pub fn validate(self: Self) !void {
            if (self.id == 0) return error.InvalidCommandId;
            if (self.name.len == 0) return error.EmptyCommandName;
        }

        pub fn enabled(self: Self, model: *const Model) bool {
            return self.enabled_fn(model);
        }

        pub fn schema(self: Self, allocator: std.mem.Allocator) !ParameterBuilder {
            var builder = ParameterBuilder{};
            errdefer builder.deinit(allocator);
            try self.parameter_schema_fn(&builder, allocator);
            try builder.canonicalize();
            return builder;
        }

        pub fn start(self: Self, world: *World, parameters: Parameters, allocator: std.mem.Allocator) !ActorId {
            var builder = try self.schema(allocator);
            defer builder.deinit(allocator);
            try parameters.validate(builder.specs.items);
            const actor_id = try self.start_fn(world, parameters);
            if (actor_id == 0) return error.InvalidCommandActorId;
            return actor_id;
        }

        pub fn asTransition(self: Self) transition.Transition {
            return .{
                .id = ids.derive("command.start", self.id, @intFromEnum(self.class)),
                .name = self.name,
                .kind = .workload,
                .resource_id = self.id,
            };
        }
    };
}

pub fn Registry(comptime Model: type, comptime World: type) type {
    const CommandType = Command(Model, World);
    return struct {
        commands: []const CommandType,

        const Self = @This();

        pub fn validate(self: Self) !void {
            for (self.commands, 0..) |command, index| {
                try command.validate();
                for (self.commands[0..index]) |prior| if (prior.id == command.id) return error.DuplicateCommandId;
            }
        }

        pub fn enabledClasses(self: Self, model: *const Model) std.EnumSet(Class) {
            var result = std.EnumSet(Class).initEmpty();
            for (self.commands) |command| if (command.enabled(model)) result.insert(command.class);
            return result;
        }

        pub fn enumerateEnabled(
            self: Self,
            model: *const Model,
            class: Class,
            list: *transition.List,
            allocator: std.mem.Allocator,
        ) !void {
            for (self.commands) |command| {
                if (command.class == class and command.enabled(model)) try list.append(allocator, command.asTransition());
            }
        }

        pub fn commandForTransition(self: Self, transition_id: ids.StableId) ?CommandType {
            for (self.commands) |command| if (command.asTransition().id == transition_id) return command;
            return null;
        }
    };
}

pub fn invocationId(command_id: CommandId, actor_sequence: u64, parameters: Parameters) ActorId {
    return ids.derive("command.invocation", ids.derive("command.invocation.command", command_id, actor_sequence), parameters.digest());
}

test "registry separates class, command, parameters, and actor identity" {
    const Model = struct { configured: bool = false };
    const World = struct { model: Model = .{}, started: usize = 0 };
    const CommandType = Command(Model, World);
    const Helpers = struct {
        fn setupEnabled(model: *const Model) bool {
            return !model.configured;
        }
        fn driverEnabled(model: *const Model) bool {
            return model.configured;
        }
        fn schema(builder: *ParameterBuilder, allocator: std.mem.Allocator) !void {
            var count = ParameterSpec.named("test.count", 1, 8, 1);
            count.boundaries = &.{ 1, 8 };
            try builder.add(allocator, count);
        }
        fn start(world: *World, parameters: Parameters) !ActorId {
            world.started += 1;
            return invocationId(ids.stable("command", "test.setup"), @intCast(world.started), parameters);
        }
        fn emptySchema(_: *ParameterBuilder, _: std.mem.Allocator) !void {}
    };
    const commands = [_]CommandType{
        CommandType.named(.setup, "test.setup", Helpers.setupEnabled, Helpers.start, Helpers.schema),
        CommandType.named(.driver, "test.driver", Helpers.driverEnabled, Helpers.start, Helpers.emptySchema),
    };
    const registry = Registry(Model, World){ .commands = &commands };
    try registry.validate();
    var world = World{};
    try std.testing.expect(registry.enabledClasses(&world.model).contains(.setup));
    try std.testing.expect(!registry.enabledClasses(&world.model).contains(.driver));

    var enabled = transition.List{};
    defer enabled.deinit(std.testing.allocator);
    try registry.enumerateEnabled(&world.model, .setup, &enabled, std.testing.allocator);
    try enabled.canonicalize();
    const setup = registry.commandForTransition(enabled.items.items[0].id).?;
    const values = [_]Value{Value.named("test.count", 8)};
    const actor = try setup.start(&world, .{ .values = &values }, std.testing.allocator);
    try std.testing.expect(actor != 0);
    try std.testing.expectEqual(@as(usize, 1), world.started);
}

test "command parameter schemas reject noncanonical and out-of-range values" {
    const schema = [_]ParameterSpec{
        ParameterSpec.named("a", 0, 2, 1),
        ParameterSpec.named("b", -1, 1, 0),
    };
    var canonical = schema;
    std.mem.sort(ParameterSpec, &canonical, {}, struct {
        fn lessThan(_: void, lhs: ParameterSpec, rhs: ParameterSpec) bool {
            return lhs.id < rhs.id;
        }
    }.lessThan);
    const good = [_]Value{
        .{ .id = canonical[0].id, .value = canonical[0].default },
        .{ .id = canonical[1].id, .value = canonical[1].default },
    };
    try (Parameters{ .values = &good }).validate(&canonical);
    var bad = good;
    bad[0].value = canonical[0].maximum + 1;
    try std.testing.expectError(error.CommandParameterOutOfRange, (Parameters{ .values = &bad }).validate(&canonical));
}
