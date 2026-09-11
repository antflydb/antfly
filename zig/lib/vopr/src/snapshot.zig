// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Scenario-owned logical checkpoints. These serialize modeled identifiers and
//! values, never heap pointers or OS resources. They are an exploration
//! optimization only: every retained artifact must still replay from init.

const std = @import("std");
const health = @import("health.zig");
const ids = @import("id.zig");
const property = @import("property.zig");
const trace = @import("trace.zig");

pub const Logical = struct {
    scenario_id: ids.StableId,
    scenario_version: u32,
    transition_index: u64,
    prefix_digest: u64,
    observation_digest: u64,
    state_digest: u64,
    bytes: []u8,
    property_statuses: []property.Status,
    /// Path-dependent diagnostic state sampled while reconstructing this
    /// prefix. It does not affect checkpoint or canonical trace identity.
    health_recorder: ?health.Recorder = null,

    pub fn deinit(self: *Logical, allocator: std.mem.Allocator) void {
        allocator.free(self.property_statuses);
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

pub fn capture(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    world: *const Scenario.World,
    transition_index: u64,
    observation_digest: u64,
) !Logical {
    return captureExecution(Scenario, allocator, world, &.{}, transition_index, observation_digest, 0);
}

pub fn captureExecution(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    world: *const Scenario.World,
    property_statuses: []const property.Status,
    transition_index: u64,
    observation_digest: u64,
    prefix_digest: u64,
) !Logical {
    comptime assertContract(Scenario);
    const bytes = try Scenario.snapshotAlloc(world, allocator);
    errdefer allocator.free(bytes);
    const statuses = try allocator.dupe(property.Status, property_statuses);
    const world_digest = ids.digest(bytes);
    const properties_digest = digestPropertyStatuses(statuses);
    return .{
        .scenario_id = ids.stable("scenario", Scenario.name),
        .scenario_version = Scenario.version,
        .transition_index = transition_index,
        .prefix_digest = prefix_digest,
        .observation_digest = observation_digest,
        .state_digest = ids.derive("snapshot.state", world_digest, properties_digest),
        .bytes = bytes,
        .property_statuses = statuses,
    };
}

pub fn restore(comptime Scenario: type, world: *Scenario.World, checkpoint: Logical, allocator: std.mem.Allocator) !void {
    comptime assertContract(Scenario);
    if (checkpoint.scenario_id != ids.stable("scenario", Scenario.name) or checkpoint.scenario_version != Scenario.version)
        return error.IncompatibleLogicalSnapshot;
    const state_digest = ids.derive("snapshot.state", ids.digest(checkpoint.bytes), digestPropertyStatuses(checkpoint.property_statuses));
    if (state_digest != checkpoint.state_digest) return error.CorruptLogicalSnapshot;
    try Scenario.restoreSnapshot(world, checkpoint.bytes, allocator);
}

pub fn restorePropertyTracker(checkpoint: Logical, tracker: *property.Tracker) !void {
    if (checkpoint.property_statuses.len != tracker.statuses.len) return error.IncompatibleLogicalSnapshotProperties;
    @memcpy(tracker.statuses, checkpoint.property_statuses);
}

/// Stable identity of the clean-world configuration and exact choice prefix
/// that produced a checkpoint. This prevents an observation digest from being
/// mistaken for a complete state signature.
pub fn prefixDigest(artifact: *const trace.Trace, prefix_len: usize) !u64 {
    if (prefix_len > artifact.choices.items.len) return error.InvalidSnapshotPrefix;
    var digest = ids.derive(
        "snapshot.prefix.header",
        ids.stable("system", artifact.header.system),
        ids.derive("snapshot.prefix.scenario", ids.stable("scenario", artifact.header.scenario), artifact.header.scenario_version),
    );
    digest = ids.derive("snapshot.prefix.budget", digest, artifact.config.transition_budget);
    digest = ids.derive("snapshot.prefix.resource", digest, artifact.config.resource_budget);
    if (artifact.config.seed) |seed| digest = ids.derive("snapshot.prefix.seed", digest, seed);
    for (artifact.config.fixture_hashes) |value| digest = ids.derive("snapshot.prefix.fixture", digest, value);
    for (artifact.config.feature_flags) |value| digest = ids.derive("snapshot.prefix.feature", digest, value);
    for (artifact.config.backend_ids) |value| digest = ids.derive("snapshot.prefix.backend", digest, value);
    for (artifact.config.scenario_parameters) |parameter| {
        digest = ids.derive("snapshot.prefix.parameter", digest, parameter.id);
        digest = ids.derive("snapshot.prefix.parameter.value", digest, @bitCast(parameter.value));
    }
    for (artifact.choices.items[0..prefix_len]) |record| {
        digest = ids.derive("snapshot.prefix.choice.site", digest, record.site_id);
        digest = ids.derive("snapshot.prefix.choice.occurrence", digest, record.occurrence);
        digest = ids.derive("snapshot.prefix.choice.selected", digest, record.selected_id);
        for (record.enabled_ids) |enabled_id| digest = ids.derive("snapshot.prefix.choice.enabled", digest, enabled_id);
    }
    return digest;
}

pub const Store = struct {
    const Key = struct {
        prefix_digest: u64,
        state_digest: u64,
    };

    allocator: std.mem.Allocator,
    checkpoints: std.ArrayListUnmanaged(Logical) = .empty,
    keys: std.AutoHashMapUnmanaged(Key, usize) = .empty,

    pub fn init(allocator: std.mem.Allocator) Store {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Store) void {
        for (self.checkpoints.items) |*checkpoint| checkpoint.deinit(self.allocator);
        self.checkpoints.deinit(self.allocator);
        self.keys.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn add(self: *Store, checkpoint: Logical) !struct { index: usize, inserted: bool } {
        const key = Key{ .prefix_digest = checkpoint.prefix_digest, .state_digest = checkpoint.state_digest };
        const gop = try self.keys.getOrPut(self.allocator, key);
        if (gop.found_existing) return .{ .index = gop.value_ptr.*, .inserted = false };
        errdefer _ = self.keys.remove(key);
        const index = self.checkpoints.items.len;
        try self.checkpoints.append(self.allocator, checkpoint);
        gop.value_ptr.* = index;
        return .{ .index = index, .inserted = true };
    }

    pub fn findPrefix(self: *const Store, prefix_digest: u64) ?Logical {
        for (self.checkpoints.items) |checkpoint| {
            if (checkpoint.prefix_digest == prefix_digest) return checkpoint;
        }
        return null;
    }
};

fn digestPropertyStatuses(statuses: []const property.Status) u64 {
    var digest = ids.stable("snapshot", "property-statuses");
    for (statuses) |status| {
        digest = ids.derive("snapshot.property.evaluations", digest, status.evaluations);
        digest = ids.derive("snapshot.property.flags", digest, @as(u64, @intFromBool(status.ever_true)) |
            (@as(u64, @intFromBool(status.ever_false)) << 1) |
            (@as(u64, @intFromBool(status.failed)) << 2) |
            (@as(u64, @intFromBool(status.satisfied_after_quiescence)) << 3));
        digest = ids.derive("snapshot.property.failure", digest, status.first_failure_transition orelse std.math.maxInt(u64));
        digest = ids.derive("snapshot.property.quiescence", digest, status.quiescence_started_at orelse std.math.maxInt(u64));
    }
    return digest;
}

pub fn assertContract(comptime Scenario: type) void {
    if (!@hasDecl(Scenario, "World")) @compileError("snapshot scenario is missing World");
    if (!@hasDecl(Scenario, "name")) @compileError("snapshot scenario is missing name");
    if (!@hasDecl(Scenario, "version")) @compileError("snapshot scenario is missing version");
    if (!@hasDecl(Scenario, "snapshotAlloc")) @compileError("snapshot scenario is missing snapshotAlloc");
    if (!@hasDecl(Scenario, "restoreSnapshot")) @compileError("snapshot scenario is missing restoreSnapshot");
}

test "logical snapshot restores values and detects corruption" {
    const Scenario = struct {
        pub const name: []const u8 = "snapshot-test";
        pub const version: u32 = 1;
        pub const World = struct { value: u64 };

        pub fn snapshotAlloc(world: *const World, allocator: std.mem.Allocator) ![]u8 {
            const bytes = try allocator.alloc(u8, @sizeOf(u64));
            std.mem.writeInt(u64, bytes[0..@sizeOf(u64)], world.value, .little);
            return bytes;
        }

        pub fn restoreSnapshot(world: *World, bytes: []const u8, _: std.mem.Allocator) !void {
            if (bytes.len != @sizeOf(u64)) return error.InvalidSnapshot;
            world.value = std.mem.readInt(u64, bytes[0..@sizeOf(u64)], .little);
        }
    };

    var world = Scenario.World{ .value = 42 };
    var checkpoint = try capture(Scenario, std.testing.allocator, &world, 7, 91);
    defer checkpoint.deinit(std.testing.allocator);
    world.value = 0;
    try restore(Scenario, &world, checkpoint, std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 42), world.value);
    checkpoint.bytes[0] ^= 1;
    try std.testing.expectError(error.CorruptLogicalSnapshot, restore(Scenario, &world, checkpoint, std.testing.allocator));
}
