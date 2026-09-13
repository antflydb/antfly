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
const event = @import("event.zig");
const observation = @import("observation.zig");
const outcome = @import("outcome.zig");
const property = @import("property.zig");
const replay = @import("replay.zig");
const runner = @import("runner.zig");

pub const CommandId = ids.StableId;
pub const ActorId = ids.StableId;

pub const Class = enum { setup, driver, observer, recovery };

/// Template role. Temporal roles (`first`, `eventually`, and `finally`) are
/// obligations, while the other roles define normal-work concurrency.
pub const Role = enum {
    /// Runs to completion before normal workload commands become eligible.
    first,
    /// May overlap compatible commands, including another invocation of self.
    parallel,
    /// Starts only in a quiescent command set and blocks other starts while active.
    serial,
    /// May overlap compatible commands but has at most one active invocation.
    singleton,
    /// Ordinary workload command eligible throughout the non-quiet phase.
    anytime,
    /// Must complete once after the quiet suffix is requested.
    eventually,
    /// Must complete once after eventual obligations and all actors quiesce.
    finally,
};

pub const FaultPolicy = enum {
    /// The command is admissible with or without active modeled faults.
    allow,
    /// The command is admissible only after all modeled faults stop.
    require_clean,
    /// The command is useful only while at least one modeled fault is active.
    require_active,
};

pub const Quiescence = enum {
    none,
    before_start,
    after_start,
    before_and_after,

    pub fn requiresBefore(self: Quiescence) bool {
        return self == .before_start or self == .before_and_after;
    }

    pub fn requiresAfter(self: Quiescence) bool {
        return self == .after_start or self == .before_and_after;
    }
};

/// Compatibility is evaluated in both directions. A non-empty allow-list is
/// restrictive; either side may deny an overlap. Equal non-null exclusion
/// groups are mutually exclusive even when allow-lists otherwise agree.
pub const ConcurrencyPolicy = struct {
    compatible_with: []const CommandId = &.{},
    incompatible_with: []const CommandId = &.{},
    exclusion_group: ?ids.StableId = null,

    pub fn validate(self: ConcurrencyPolicy, command_id: CommandId) !void {
        try validateCanonicalIds(self.compatible_with);
        try validateCanonicalIds(self.incompatible_with);
        for (self.compatible_with) |candidate| {
            if (candidate == command_id) return error.RedundantSelfCompatibility;
            if (containsId(self.incompatible_with, candidate)) return error.ConflictingCommandCompatibility;
        }
        for (self.incompatible_with) |candidate| if (candidate == command_id)
            return error.RedundantSelfIncompatibility;
        if (self.exclusion_group) |group| if (group == 0) return error.InvalidCommandExclusionGroup;
    }

    fn allows(self: ConcurrencyPolicy, other_id: CommandId) bool {
        if (containsId(self.incompatible_with, other_id)) return false;
        return self.compatible_with.len == 0 or containsId(self.compatible_with, other_id);
    }
};

pub const DefinitionOptions = struct {
    class: Class,
    role: Role,
    fault_policy: FaultPolicy = .allow,
    quiescence: Quiescence = .none,
    concurrency: ConcurrencyPolicy = .{},
};

pub const Environment = struct {
    active_faults: usize = 0,
};

pub const Phase = enum { first, workload, eventually, finally, complete };

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
        role: Role,
        fault_policy: FaultPolicy = .allow,
        quiescence: Quiescence = .none,
        concurrency: ConcurrencyPolicy = .{},
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
            return define(.{
                .class = class,
                .role = switch (class) {
                    .setup => .first,
                    .driver => .anytime,
                    .observer => .parallel,
                    .recovery => .eventually,
                },
            }, name, enabled_fn, start_fn, parameter_schema_fn);
        }

        pub fn define(
            options: DefinitionOptions,
            name: []const u8,
            enabled_fn: *const fn (*const Model) bool,
            start_fn: *const fn (*World, Parameters) anyerror!ActorId,
            parameter_schema_fn: *const fn (*ParameterBuilder, std.mem.Allocator) anyerror!void,
        ) Self {
            return .{
                .id = ids.stable("command", name),
                .name = name,
                .class = options.class,
                .role = options.role,
                .fault_policy = options.fault_policy,
                .quiescence = options.quiescence,
                .concurrency = options.concurrency,
                .enabled_fn = enabled_fn,
                .start_fn = start_fn,
                .parameter_schema_fn = parameter_schema_fn,
            };
        }

        pub fn validate(self: Self) !void {
            if (self.id == 0) return error.InvalidCommandId;
            if (self.name.len == 0) return error.EmptyCommandName;
            try self.concurrency.validate(self.id);
            if ((self.role == .serial or self.role == .finally) and self.quiescence == .none)
                return error.SerialCommandMustDeclareQuiescence;
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
                .id = ids.derive("command.start", self.id, @intFromEnum(self.role)),
                .name = self.name,
                .kind = .workload,
                .resource_id = self.id,
            };
        }
    };
}

const ActiveInvocation = struct {
    actor_id: ActorId,
    command_index: usize,
};

/// Stateful role composer. It owns no production actors; it only records their
/// stable identities and admits starts. The scenario completes an invocation
/// after its production operation reaches a safe boundary.
pub fn Composer(comptime Model: type, comptime World: type) type {
    const CommandType = Command(Model, World);
    const RegistryType = Registry(Model, World);
    return struct {
        allocator: std.mem.Allocator,
        registry: RegistryType,
        phase: Phase = .first,
        active: std.ArrayListUnmanaged(ActiveInvocation) = .empty,
        started: []u64,
        completed: []u64,
        required: []bool,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, registry: RegistryType, model: *const Model) !Self {
            try registry.validate();
            const started = try allocator.alloc(u64, registry.commands.len);
            errdefer allocator.free(started);
            const completed = try allocator.alloc(u64, registry.commands.len);
            errdefer allocator.free(completed);
            const required = try allocator.alloc(bool, registry.commands.len);
            @memset(started, 0);
            @memset(completed, 0);
            @memset(required, false);
            for (registry.commands, 0..) |command, index| {
                required[index] = command.role == .first and command.enabled(model);
            }
            var self = Self{
                .allocator = allocator,
                .registry = registry,
                .started = started,
                .completed = completed,
                .required = required,
            };
            self.advancePhase();
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.active.deinit(self.allocator);
            self.allocator.free(self.started);
            self.allocator.free(self.completed);
            self.allocator.free(self.required);
            self.* = undefined;
        }

        pub fn currentPhase(self: *const Self) Phase {
            return self.phase;
        }

        pub fn activeCount(self: *const Self) usize {
            return self.active.items.len;
        }

        pub fn requestQuietSuffix(self: *Self, model: *const Model) !void {
            if (self.phase == .first) return error.FirstCommandsIncomplete;
            if (self.phase != .workload) return error.QuietSuffixAlreadyRequested;
            self.phase = .eventually;
            @memset(self.required, false);
            for (self.registry.commands, 0..) |command, index| {
                self.required[index] = command.role == .eventually and command.enabled(model);
            }
            self.advancePhase();
            if (self.phase == .finally) self.captureFinalRequirements(model);
            self.advancePhase();
        }

        pub fn enumerate(
            self: *Self,
            model: *const Model,
            environment: Environment,
            list: *transition.List,
            allocator: std.mem.Allocator,
        ) !void {
            self.advancePhase();
            if (self.phase == .complete) return;
            for (self.registry.commands, 0..) |command, index| {
                if (!self.roleEligible(command.role, index)) continue;
                if (!command.enabled(model)) continue;
                if (!self.admitted(index, environment)) continue;
                try list.append(allocator, command.asTransition());
            }
        }

        pub fn start(
            self: *Self,
            world: *World,
            model: *const Model,
            transition_id: ids.StableId,
            parameters: Parameters,
            environment: Environment,
        ) !ActorId {
            const index = self.commandIndexForTransition(transition_id) orelse return error.UnknownCommandTransition;
            if (!self.roleEligible(self.registry.commands[index].role, index)) return error.CommandRoleNotEligible;
            if (!self.registry.commands[index].enabled(model)) return error.CommandDisabled;
            if (!self.admitted(index, environment)) return error.CommandNotAdmitted;
            const actor_id = try self.registry.commands[index].start(world, parameters, self.allocator);
            for (self.active.items) |active| if (active.actor_id == actor_id) return error.DuplicateCommandActorId;
            try self.active.append(self.allocator, .{ .actor_id = actor_id, .command_index = index });
            self.started[index] +%= 1;
            return actor_id;
        }

        pub fn complete(self: *Self, actor_id: ActorId, model: *const Model) !void {
            for (self.active.items, 0..) |active, index| {
                if (active.actor_id != actor_id) continue;
                self.completed[active.command_index] +%= 1;
                _ = self.active.orderedRemove(index);
                const before = self.phase;
                self.advancePhase();
                if (before == .eventually and self.phase == .finally) self.captureFinalRequirements(model);
                self.advancePhase();
                return;
            }
            return error.UnknownCommandActorId;
        }

        fn roleEligible(self: *const Self, role: Role, index: usize) bool {
            return switch (self.phase) {
                .first => role == .first and self.required[index] and self.started[index] == 0,
                .workload => switch (role) {
                    .parallel, .serial, .singleton, .anytime => true,
                    else => false,
                },
                .eventually => role == .eventually and self.required[index] and self.started[index] == 0,
                .finally => role == .finally and self.required[index] and self.started[index] == 0,
                .complete => false,
            };
        }

        fn admitted(self: *const Self, candidate_index: usize, environment: Environment) bool {
            const candidate = self.registry.commands[candidate_index];
            switch (candidate.fault_policy) {
                .allow => {},
                .require_clean => if (environment.active_faults != 0) return false,
                .require_active => if (environment.active_faults == 0) return false,
            }
            if (candidate.quiescence.requiresBefore() and self.active.items.len != 0) return false;
            if ((candidate.role == .serial or candidate.role == .finally) and self.active.items.len != 0) return false;
            if (candidate.role == .singleton) for (self.active.items) |active| {
                if (active.command_index == candidate_index) return false;
            };
            for (self.active.items) |active| {
                const running = self.registry.commands[active.command_index];
                if (running.role == .serial or running.quiescence.requiresAfter()) return false;
                if (!commandsCompatible(candidate, running)) return false;
            }
            return true;
        }

        fn advancePhase(self: *Self) void {
            if (self.active.items.len != 0) return;
            switch (self.phase) {
                .first => {
                    if (self.requirementsComplete()) {
                        self.phase = .workload;
                        @memset(self.required, false);
                    }
                },
                .eventually => {
                    if (self.requirementsComplete()) self.phase = .finally;
                },
                .finally => {
                    if (self.requirementsComplete()) self.phase = .complete;
                },
                .workload, .complete => {},
            }
        }

        fn requirementsComplete(self: *const Self) bool {
            for (self.required, 0..) |required, index| {
                if (required and self.completed[index] == 0) return false;
            }
            return true;
        }

        fn captureFinalRequirements(self: *Self, model: *const Model) void {
            @memset(self.required, false);
            for (self.registry.commands, 0..) |command, index| {
                self.required[index] = command.role == .finally and command.enabled(model);
            }
        }

        fn commandIndexForTransition(self: *const Self, transition_id: ids.StableId) ?usize {
            for (self.registry.commands, 0..) |command, index| if (command.asTransition().id == transition_id) return index;
            return null;
        }

        fn commandsCompatible(left: CommandType, right: CommandType) bool {
            if (left.concurrency.exclusion_group != null and left.concurrency.exclusion_group == right.concurrency.exclusion_group)
                return false;
            return left.concurrency.allows(right.id) and right.concurrency.allows(left.id);
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

fn containsId(values: []const ids.StableId, needle: ids.StableId) bool {
    for (values) |value| if (value == needle) return true;
    return false;
}

fn validateCanonicalIds(values: []const ids.StableId) !void {
    var prior: ids.StableId = 0;
    for (values, 0..) |value, index| {
        if (value == 0) return error.InvalidCommandCompatibilityId;
        if (index > 0 and value <= prior) return error.NonCanonicalCommandCompatibility;
        prior = value;
    }
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

test "command composer enforces temporal roles quiet suffix and fault policy" {
    const Model = struct { enabled: bool = true };
    const World = struct { sequence: u64 = 0 };
    const CommandType = Command(Model, World);
    const Helpers = struct {
        fn enabled(model: *const Model) bool {
            return model.enabled;
        }
        fn emptySchema(_: *ParameterBuilder, _: std.mem.Allocator) !void {}
        fn startNamed(comptime command_name: []const u8, world: *World, parameters: Parameters) !ActorId {
            world.sequence += 1;
            return invocationId(ids.stable("command", command_name), world.sequence, parameters);
        }
        fn first(world: *World, parameters: Parameters) !ActorId {
            return startNamed("compose.first", world, parameters);
        }
        fn parallel(world: *World, parameters: Parameters) !ActorId {
            return startNamed("compose.parallel", world, parameters);
        }
        fn singleton(world: *World, parameters: Parameters) !ActorId {
            return startNamed("compose.singleton", world, parameters);
        }
        fn serial(world: *World, parameters: Parameters) !ActorId {
            return startNamed("compose.serial", world, parameters);
        }
        fn faultOnly(world: *World, parameters: Parameters) !ActorId {
            return startNamed("compose.fault-only", world, parameters);
        }
        fn eventual(world: *World, parameters: Parameters) !ActorId {
            return startNamed("compose.eventual", world, parameters);
        }
        fn final(world: *World, parameters: Parameters) !ActorId {
            return startNamed("compose.final", world, parameters);
        }
    };
    const commands = [_]CommandType{
        CommandType.define(.{ .class = .setup, .role = .first }, "compose.first", Helpers.enabled, Helpers.first, Helpers.emptySchema),
        CommandType.define(.{ .class = .driver, .role = .parallel }, "compose.parallel", Helpers.enabled, Helpers.parallel, Helpers.emptySchema),
        CommandType.define(.{ .class = .driver, .role = .singleton }, "compose.singleton", Helpers.enabled, Helpers.singleton, Helpers.emptySchema),
        CommandType.define(.{ .class = .driver, .role = .serial, .quiescence = .before_and_after }, "compose.serial", Helpers.enabled, Helpers.serial, Helpers.emptySchema),
        CommandType.define(.{ .class = .driver, .role = .anytime, .fault_policy = .require_active }, "compose.fault-only", Helpers.enabled, Helpers.faultOnly, Helpers.emptySchema),
        CommandType.define(.{ .class = .recovery, .role = .eventually, .fault_policy = .require_clean }, "compose.eventual", Helpers.enabled, Helpers.eventual, Helpers.emptySchema),
        CommandType.define(.{ .class = .recovery, .role = .finally, .quiescence = .before_and_after }, "compose.final", Helpers.enabled, Helpers.final, Helpers.emptySchema),
    };
    const registry = Registry(Model, World){ .commands = &commands };
    const model = Model{};
    var world = World{};
    var composer = try Composer(Model, World).init(std.testing.allocator, registry, &model);
    defer composer.deinit();
    const no_parameters = Parameters{ .values = &.{} };

    var enabled = transition.List{};
    defer enabled.deinit(std.testing.allocator);
    try composer.enumerate(&model, .{}, &enabled, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), enabled.items.items.len);
    const first_actor = try composer.start(&world, &model, commands[0].asTransition().id, no_parameters, .{});
    try composer.complete(first_actor, &model);
    try std.testing.expectEqual(Phase.workload, composer.currentPhase());

    const parallel_actor = try composer.start(&world, &model, commands[1].asTransition().id, no_parameters, .{});
    const singleton_actor = try composer.start(&world, &model, commands[2].asTransition().id, no_parameters, .{});
    try std.testing.expectError(error.CommandNotAdmitted, composer.start(&world, &model, commands[2].asTransition().id, no_parameters, .{}));
    try std.testing.expectError(error.CommandNotAdmitted, composer.start(&world, &model, commands[3].asTransition().id, no_parameters, .{}));
    try std.testing.expectError(error.CommandNotAdmitted, composer.start(&world, &model, commands[4].asTransition().id, no_parameters, .{}));
    try composer.complete(parallel_actor, &model);
    try composer.complete(singleton_actor, &model);

    const fault_actor = try composer.start(&world, &model, commands[4].asTransition().id, no_parameters, .{ .active_faults = 1 });
    try composer.complete(fault_actor, &model);
    const serial_actor = try composer.start(&world, &model, commands[3].asTransition().id, no_parameters, .{});
    try std.testing.expectError(error.CommandNotAdmitted, composer.start(&world, &model, commands[1].asTransition().id, no_parameters, .{}));
    try composer.complete(serial_actor, &model);

    try composer.requestQuietSuffix(&model);
    try std.testing.expectEqual(Phase.eventually, composer.currentPhase());
    enabled.items.clearRetainingCapacity();
    try composer.enumerate(&model, .{ .active_faults = 1 }, &enabled, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), enabled.items.items.len);
    const eventual_actor = try composer.start(&world, &model, commands[5].asTransition().id, no_parameters, .{});
    try composer.complete(eventual_actor, &model);
    try std.testing.expectEqual(Phase.finally, composer.currentPhase());
    const final_actor = try composer.start(&world, &model, commands[6].asTransition().id, no_parameters, .{});
    try composer.complete(final_actor, &model);
    try std.testing.expectEqual(Phase.complete, composer.currentPhase());
}

test "command composer applies symmetric compatibility and exclusion" {
    const Model = struct {};
    const World = struct { sequence: u64 = 0 };
    const CommandType = Command(Model, World);
    const exclusion = ids.stable("command-exclusion", "storage-writer");
    const Helpers = struct {
        fn enabled(_: *const Model) bool {
            return true;
        }
        fn emptySchema(_: *ParameterBuilder, _: std.mem.Allocator) !void {}
        fn left(world: *World, parameters: Parameters) !ActorId {
            world.sequence += 1;
            return invocationId(ids.stable("command", "compat.left"), world.sequence, parameters);
        }
        fn right(world: *World, parameters: Parameters) !ActorId {
            world.sequence += 1;
            return invocationId(ids.stable("command", "compat.right"), world.sequence, parameters);
        }
    };
    const commands = [_]CommandType{
        CommandType.define(.{ .class = .driver, .role = .parallel, .concurrency = .{ .exclusion_group = exclusion } }, "compat.left", Helpers.enabled, Helpers.left, Helpers.emptySchema),
        CommandType.define(.{ .class = .driver, .role = .parallel, .concurrency = .{ .exclusion_group = exclusion } }, "compat.right", Helpers.enabled, Helpers.right, Helpers.emptySchema),
    };
    const model = Model{};
    var world = World{};
    var composer = try Composer(Model, World).init(std.testing.allocator, .{ .commands = &commands }, &model);
    defer composer.deinit();
    try std.testing.expectEqual(Phase.workload, composer.currentPhase());
    const actor = try composer.start(&world, &model, commands[0].asTransition().id, .{ .values = &.{} }, .{});
    try std.testing.expectError(error.CommandNotAdmitted, composer.start(&world, &model, commands[1].asTransition().id, .{ .values = &.{} }, .{}));
    try composer.complete(actor, &model);
    const right_actor = try composer.start(&world, &model, commands[1].asTransition().id, .{ .values = &.{} }, .{});
    try composer.complete(right_actor, &model);
}

const ComposerReplayModel = struct { enabled: bool = true };
const ComposerReplayCommandWorld = struct { sequence: u64 = 0 };
const ComposerReplayCommand = Command(ComposerReplayModel, ComposerReplayCommandWorld);

const ComposerReplayHelpers = struct {
    fn enabled(model: *const ComposerReplayModel) bool {
        return model.enabled;
    }
    fn emptySchema(_: *ParameterBuilder, _: std.mem.Allocator) !void {}
    fn startNamed(comptime name: []const u8, world: *ComposerReplayCommandWorld, parameters: Parameters) !ActorId {
        world.sequence += 1;
        return invocationId(ids.stable("command", name), world.sequence, parameters);
    }
    fn first(world: *ComposerReplayCommandWorld, parameters: Parameters) !ActorId {
        return startNamed("replay.first", world, parameters);
    }
    fn anytime(world: *ComposerReplayCommandWorld, parameters: Parameters) !ActorId {
        return startNamed("replay.anytime", world, parameters);
    }
    fn eventual(world: *ComposerReplayCommandWorld, parameters: Parameters) !ActorId {
        return startNamed("replay.eventual", world, parameters);
    }
    fn final(world: *ComposerReplayCommandWorld, parameters: Parameters) !ActorId {
        return startNamed("replay.final", world, parameters);
    }
};

const composer_replay_commands = [_]ComposerReplayCommand{
    ComposerReplayCommand.define(.{ .class = .setup, .role = .first }, "replay.first", ComposerReplayHelpers.enabled, ComposerReplayHelpers.first, ComposerReplayHelpers.emptySchema),
    ComposerReplayCommand.define(.{ .class = .driver, .role = .anytime }, "replay.anytime", ComposerReplayHelpers.enabled, ComposerReplayHelpers.anytime, ComposerReplayHelpers.emptySchema),
    ComposerReplayCommand.define(.{ .class = .recovery, .role = .eventually, .fault_policy = .require_clean }, "replay.eventual", ComposerReplayHelpers.enabled, ComposerReplayHelpers.eventual, ComposerReplayHelpers.emptySchema),
    ComposerReplayCommand.define(.{ .class = .recovery, .role = .finally, .quiescence = .before_and_after }, "replay.final", ComposerReplayHelpers.enabled, ComposerReplayHelpers.final, ComposerReplayHelpers.emptySchema),
};

const ComposerReplayScenario = struct {
    pub const name: []const u8 = "command-composer-replay";
    pub const version: u32 = 1;
    const complete_property = ids.stable("property", "command-composer-replay.complete");
    const quiet_id = ids.stable("transition", "command-composer-replay.quiet");
    pub const properties = &[_]property.Declaration{.{ .id = complete_property, .name = "command-composer-replay.complete", .kind = .reachable }};
    const ComposerType = Composer(ComposerReplayModel, ComposerReplayCommandWorld);

    pub const World = struct {
        model: ComposerReplayModel = .{},
        command_world: ComposerReplayCommandWorld = .{},
        composer: ComposerType,
        normal_runs: u64 = 0,
    };

    pub fn init(allocator: std.mem.Allocator) !World {
        const model = ComposerReplayModel{};
        return .{
            .model = model,
            .composer = try ComposerType.init(allocator, .{ .commands = &composer_replay_commands }, &model),
        };
    }

    pub fn deinit(world: *World, _: std.mem.Allocator) void {
        world.composer.deinit();
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *transition.List, allocator: std.mem.Allocator) !void {
        try world.composer.enumerate(&world.model, .{}, list, allocator);
        if (world.composer.currentPhase() == .workload and world.normal_runs > 0) {
            try list.append(allocator, .{ .id = quiet_id, .name = "command-composer-replay.quiet", .kind = .quiescence });
        }
    }

    pub fn execute(world: *World, selected: transition.Transition, events: *event.Sink, allocator: std.mem.Allocator) !outcome.TransitionOutcome {
        if (selected.id == quiet_id) {
            try world.composer.requestQuietSuffix(&world.model);
        } else {
            const actor = try world.composer.start(&world.command_world, &world.model, selected.id, .{ .values = &.{} }, .{});
            if (selected.id == composer_replay_commands[1].asTransition().id) world.normal_runs += 1;
            try world.composer.complete(actor, &world.model);
        }
        try events.emitNamed(allocator, .domain, selected.name, @intFromEnum(world.composer.currentPhase()));
        return .applied();
    }

    pub fn observe(world: *World, builder: *observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, "command-composer-replay.phase", @intFromEnum(world.composer.currentPhase()));
        try builder.addNamed(allocator, "command-composer-replay.normal-runs", @intCast(world.normal_runs));
    }

    pub fn evaluate(world: *World, sink: *property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, complete_property, world.composer.currentPhase() == .complete);
    }

    pub fn done(world: *World) bool {
        return world.composer.currentPhase() == .complete;
    }
};

test "command composer roles record and exact replay" {
    const selections = [_]ids.StableId{
        composer_replay_commands[0].asTransition().id,
        composer_replay_commands[1].asTransition().id,
        ComposerReplayScenario.quiet_id,
        composer_replay_commands[2].asTransition().id,
        composer_replay_commands[3].asTransition().id,
    };
    var scripted = @import("choice.zig").Scripted{ .selections = &selections };
    var artifact = try runner.run(ComposerReplayScenario, std.testing.allocator, scripted.source(), .{ .transition_budget = selections.len });
    defer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    var replayed = try replay.exact(ComposerReplayScenario, std.testing.allocator, &artifact);
    replayed.deinit();
}
