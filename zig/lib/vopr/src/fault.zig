// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Reusable fault lifecycle and exploration-budget controller.
//!
//! Admission is side-effect free, starts/stops are explicit, and beginning a
//! quiet suffix never heals anything implicitly. Replay therefore observes
//! exactly the same lifecycle decisions as generation.

const std = @import("std");
const event = @import("event.zig");
const ids = @import("id.zig");
const transition = @import("transition.zig");

pub const FaultId = ids.StableId;

pub const Kind = enum { node, partitioned_link, delayed_message, storage, resource, custom };
pub const Lifecycle = enum { persistent, one_shot };

pub const Spec = struct {
    id: FaultId,
    name: []const u8,
    kind: Kind,
    lifecycle: Lifecycle = .persistent,
    actor_id: ?ids.StableId = null,
    resource_id: ?ids.StableId = null,

    pub fn named(kind: Kind, lifecycle: Lifecycle, name: []const u8) Spec {
        return .{ .id = ids.stable("fault", name), .name = name, .kind = kind, .lifecycle = lifecycle };
    }

    pub fn validate(self: Spec) !void {
        if (self.id == 0) return error.InvalidFaultId;
        if (self.name.len == 0) return error.EmptyFaultName;
        if ((self.kind == .node or self.kind == .partitioned_link or self.kind == .delayed_message or self.kind == .storage) and self.resource_id == null)
            return error.FaultResourceRequired;
    }

    pub fn startTransition(self: Spec) transition.Transition {
        return .{
            .id = ids.derive("fault.start", self.id, 0),
            .name = self.name,
            .kind = .fault,
            .actor_id = self.actor_id,
            .resource_id = self.id,
            .fault_phase = .start,
        };
    }

    pub fn stopTransition(self: Spec) transition.Transition {
        return .{
            .id = ids.derive("fault.stop", self.id, 0),
            .name = self.name,
            .kind = .fault,
            .actor_id = self.actor_id,
            .resource_id = self.id,
            .fault_phase = .end,
        };
    }
};

pub const QuietSuffixPolicy = enum {
    preserve_generation_policy,
    /// Disable new hostile starts; active faults still require selected stop
    /// transitions and are never healed as a side effect of phase entry.
    heal_recoverable,
};

pub const Budgets = struct {
    max_simultaneous_node_failures: u32 = 1,
    max_partitioned_links: u32 = 2,
    max_outstanding_delayed_messages: u32 = 64,
    max_storage_faults_per_durability_epoch: u32 = 1,
    minimum_healthy_nodes: u32 = 1,
    allow_quorum_loss: bool = false,
    quiet_suffix_policy: QuietSuffixPolicy = .heal_recoverable,

    pub fn validate(self: Budgets, total_nodes: u32) !void {
        if (self.minimum_healthy_nodes > total_nodes) return error.MinimumHealthyNodesExceedsCluster;
        if (!self.allow_quorum_loss and total_nodes > 0 and self.minimum_healthy_nodes == 0)
            return error.HealthyNodeFloorRequired;
    }
};

pub const Rejection = enum {
    quiet_suffix,
    duplicate_fault,
    duplicate_resource_fault,
    node_failure_budget,
    healthy_node_floor,
    partition_budget,
    delayed_message_budget,
    storage_epoch_budget,
};

pub const Admission = union(enum) {
    allowed,
    rejected: Rejection,

    pub fn isAllowed(self: Admission) bool {
        return self == .allowed;
    }
};

const Active = struct { spec: Spec };

pub const Controller = struct {
    allocator: std.mem.Allocator,
    budgets: Budgets,
    total_nodes: u32,
    active: std.ArrayListUnmanaged(Active) = .empty,
    outstanding_delayed_messages: u32 = 0,
    storage_faults_this_epoch: u32 = 0,
    durability_epoch: u64 = 0,
    quiet_suffix: bool = false,

    pub fn init(allocator: std.mem.Allocator, total_nodes: u32, budgets: Budgets) !Controller {
        try budgets.validate(total_nodes);
        return .{ .allocator = allocator, .budgets = budgets, .total_nodes = total_nodes };
    }

    pub fn deinit(self: *Controller) void {
        for (self.active.items) |active| self.allocator.free(active.spec.name);
        self.active.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn admission(self: *const Controller, spec: Spec) !Admission {
        try spec.validate();
        if (self.quiet_suffix and self.budgets.quiet_suffix_policy == .heal_recoverable)
            return .{ .rejected = .quiet_suffix };
        for (self.active.items) |active| {
            if (active.spec.id == spec.id) return .{ .rejected = .duplicate_fault };
            if (active.spec.kind == spec.kind and active.spec.resource_id != null and active.spec.resource_id == spec.resource_id)
                return .{ .rejected = .duplicate_resource_fault };
        }
        switch (spec.kind) {
            .node => {
                const failed = self.activeCount(.node);
                if (failed >= self.budgets.max_simultaneous_node_failures)
                    return .{ .rejected = .node_failure_budget };
                if (!self.budgets.allow_quorum_loss and self.total_nodes -| (failed + 1) < self.budgets.minimum_healthy_nodes)
                    return .{ .rejected = .healthy_node_floor };
            },
            .partitioned_link => if (self.activeCount(.partitioned_link) >= self.budgets.max_partitioned_links)
                return .{ .rejected = .partition_budget },
            .delayed_message => if (self.outstanding_delayed_messages >= self.budgets.max_outstanding_delayed_messages)
                return .{ .rejected = .delayed_message_budget },
            .storage => if (self.storage_faults_this_epoch >= self.budgets.max_storage_faults_per_durability_epoch)
                return .{ .rejected = .storage_epoch_budget },
            .resource, .custom => {},
        }
        return .allowed;
    }

    pub fn start(self: *Controller, spec: Spec, sink: *event.Sink, allocator: std.mem.Allocator) !void {
        switch (try self.admission(spec)) {
            .allowed => {},
            .rejected => |reason| return rejectionError(reason),
        }
        const name = try self.allocator.dupe(u8, spec.name);
        {
            errdefer self.allocator.free(name);
            try self.active.append(self.allocator, .{ .spec = .{
                .id = spec.id,
                .name = name,
                .kind = spec.kind,
                .lifecycle = spec.lifecycle,
                .actor_id = spec.actor_id,
                .resource_id = spec.resource_id,
            } });
        }
        errdefer {
            const removed = self.active.pop().?;
            self.allocator.free(removed.spec.name);
        }
        if (spec.kind == .storage) self.storage_faults_this_epoch += 1;
        errdefer {
            if (spec.kind == .storage) self.storage_faults_this_epoch -= 1;
        }
        try emitLifecycle(sink, allocator, .fault_started, spec);
    }

    pub fn stop(self: *Controller, fault_id: FaultId, sink: *event.Sink, allocator: std.mem.Allocator) !void {
        const index = self.indexOf(fault_id) orelse return error.UnknownActiveFault;
        const removed = self.active.orderedRemove(index);
        defer self.allocator.free(removed.spec.name);
        try emitLifecycle(sink, allocator, .fault_stopped, removed.spec);
    }

    /// Expires the lowest-ID matching one-shot fault after the modeled
    /// operation actually consumes it.
    pub fn consumeOneShot(
        self: *Controller,
        kind: Kind,
        resource_id: ?ids.StableId,
        sink: *event.Sink,
        allocator: std.mem.Allocator,
    ) !?FaultId {
        var selected_index: ?usize = null;
        var selected_id: FaultId = std.math.maxInt(FaultId);
        for (self.active.items, 0..) |active, index| {
            if (active.spec.lifecycle != .one_shot or active.spec.kind != kind) continue;
            if (resource_id != null and active.spec.resource_id != resource_id) continue;
            if (active.spec.id < selected_id) {
                selected_id = active.spec.id;
                selected_index = index;
            }
        }
        const index = selected_index orelse return null;
        const removed = self.active.orderedRemove(index);
        defer self.allocator.free(removed.spec.name);
        try emitLifecycle(sink, allocator, .fault_stopped, removed.spec);
        return removed.spec.id;
    }

    pub fn beginQuietSuffix(self: *Controller) void {
        self.quiet_suffix = true;
    }

    pub fn beginDurabilityEpoch(self: *Controller) !void {
        if (self.activeCount(.storage) != 0) return error.StorageFaultActiveAcrossDurabilityEpoch;
        self.durability_epoch +|= 1;
        self.storage_faults_this_epoch = 0;
    }

    pub fn setOutstandingDelayedMessages(self: *Controller, count: u32) void {
        self.outstanding_delayed_messages = count;
    }

    pub fn activeCount(self: *const Controller, kind: Kind) u32 {
        var count: u32 = 0;
        for (self.active.items) |active| count += @intFromBool(active.spec.kind == kind);
        return count;
    }

    pub fn activeTotal(self: *const Controller) usize {
        return self.active.items.len;
    }

    pub fn isActive(self: *const Controller, fault_id: FaultId) bool {
        return self.indexOf(fault_id) != null;
    }

    pub fn shouldEnumerateStopsOnly(self: *const Controller) bool {
        return self.quiet_suffix and self.budgets.quiet_suffix_policy == .heal_recoverable;
    }

    fn indexOf(self: *const Controller, fault_id: FaultId) ?usize {
        for (self.active.items, 0..) |active, index| if (active.spec.id == fault_id) return index;
        return null;
    }
};

fn rejectionError(reason: Rejection) anyerror {
    return switch (reason) {
        .quiet_suffix => error.FaultStartDisabledDuringQuietSuffix,
        .duplicate_fault => error.DuplicateActiveFault,
        .duplicate_resource_fault => error.DuplicateResourceFault,
        .node_failure_budget => error.NodeFailureBudgetExceeded,
        .healthy_node_floor => error.HealthyNodeFloorExceeded,
        .partition_budget => error.PartitionBudgetExceeded,
        .delayed_message_budget => error.DelayedMessageBudgetExceeded,
        .storage_epoch_budget => error.StorageFaultEpochBudgetExceeded,
    };
}

fn emitLifecycle(sink: *event.Sink, allocator: std.mem.Allocator, kind: event.Kind, spec: Spec) !void {
    try sink.emit(allocator, .{
        .id = ids.derive("event.fault-lifecycle", spec.id, @intFromEnum(kind)),
        .name = spec.name,
        .kind = kind,
        .actor_id = spec.actor_id,
        .resource_id = spec.id,
        .payload_digest = spec.resource_id orelse 0,
    });
}

test "independent lifecycle faults overlap within explicit budgets" {
    var controller = try Controller.init(std.testing.allocator, 3, .{
        .max_simultaneous_node_failures = 1,
        .max_partitioned_links = 2,
        .minimum_healthy_nodes = 2,
    });
    defer controller.deinit();
    var sink = event.Sink{};
    defer sink.deinit(std.testing.allocator);

    var node = Spec.named(.node, .persistent, "node.1.crashed");
    node.resource_id = 1;
    var link_a = Spec.named(.partitioned_link, .persistent, "link.1.2.partitioned");
    link_a.resource_id = 12;
    var link_b = Spec.named(.partitioned_link, .persistent, "link.2.1.partitioned");
    link_b.resource_id = 21;
    try controller.start(node, &sink, std.testing.allocator);
    try controller.start(link_a, &sink, std.testing.allocator);
    try controller.start(link_b, &sink, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), controller.activeTotal());

    var second_node = Spec.named(.node, .persistent, "node.2.crashed");
    second_node.resource_id = 2;
    try std.testing.expectEqual(Rejection.node_failure_budget, (try controller.admission(second_node)).rejected);
    try controller.stop(link_a.id, &sink, std.testing.allocator);
    try std.testing.expect(!controller.isActive(link_a.id));
    try std.testing.expect(controller.isActive(link_b.id));
}

test "one-shot consumption and quiet suffix produce explicit stop events" {
    var controller = try Controller.init(std.testing.allocator, 3, .{});
    defer controller.deinit();
    var sink = event.Sink{};
    defer sink.deinit(std.testing.allocator);
    var write = Spec.named(.storage, .one_shot, "storage.next-write-fails");
    write.resource_id = 44;
    try controller.start(write, &sink, std.testing.allocator);
    try std.testing.expectEqual(write.id, (try controller.consumeOneShot(.storage, 44, &sink, std.testing.allocator)).?);
    try std.testing.expectEqual(event.Kind.fault_started, sink.events.items[0].kind);
    try std.testing.expectEqual(event.Kind.fault_stopped, sink.events.items[1].kind);

    try controller.beginDurabilityEpoch();
    controller.beginQuietSuffix();
    try std.testing.expectEqual(Rejection.quiet_suffix, (try controller.admission(write)).rejected);
    try std.testing.expect(controller.shouldEnumerateStopsOnly());
}

test "storage epochs and delayed-message state enforce separate budgets" {
    var controller = try Controller.init(std.testing.allocator, 3, .{
        .max_outstanding_delayed_messages = 1,
        .max_storage_faults_per_durability_epoch = 1,
    });
    defer controller.deinit();
    var sink = event.Sink{};
    defer sink.deinit(std.testing.allocator);
    var storage = Spec.named(.storage, .persistent, "storage.sync-fails");
    storage.resource_id = 1;
    try controller.start(storage, &sink, std.testing.allocator);
    try controller.stop(storage.id, &sink, std.testing.allocator);
    var another = Spec.named(.storage, .persistent, "storage.write-fails");
    another.resource_id = 2;
    try std.testing.expectEqual(Rejection.storage_epoch_budget, (try controller.admission(another)).rejected);
    try controller.beginDurabilityEpoch();
    try std.testing.expect((try controller.admission(another)).isAllowed());

    controller.setOutstandingDelayedMessages(1);
    var delayed = Spec.named(.delayed_message, .one_shot, "network.delay-message");
    delayed.resource_id = 90;
    try std.testing.expectEqual(Rejection.delayed_message_budget, (try controller.admission(delayed)).rejected);
}

test "generic runner records controller lifecycle without synthetic pulses" {
    const choice = @import("choice.zig");
    const observation = @import("observation.zig");
    const outcome = @import("outcome.zig");
    const property = @import("property.zig");
    const runner = @import("runner.zig");

    const Scenario = struct {
        pub const name: []const u8 = "fault-controller-lifecycle";
        pub const version: u32 = 1;
        pub const properties = &[_]property.Declaration{};
        const fault_spec: Spec = .{
            .id = ids.stable("fault", "node.1.pause"),
            .name = "node.1.pause",
            .kind = .node,
            .resource_id = 1,
        };
        pub const World = struct { controller: Controller, stage: u8 = 0 };
        pub fn init(allocator: std.mem.Allocator) !World {
            return .{ .controller = try Controller.init(allocator, 3, .{}) };
        }
        pub fn deinit(world: *World, _: std.mem.Allocator) void {
            world.controller.deinit();
        }
        pub fn enumerate(world: *World, list: *transition.List, allocator: std.mem.Allocator) !void {
            if (world.stage == 0)
                try list.append(allocator, fault_spec.startTransition())
            else if (world.stage == 1)
                try list.append(allocator, fault_spec.stopTransition());
        }
        pub fn execute(world: *World, selected: transition.Transition, sink: *event.Sink, allocator: std.mem.Allocator) !outcome.TransitionOutcome {
            if (selected.id == fault_spec.startTransition().id and world.stage == 0) {
                try world.controller.start(fault_spec, sink, allocator);
                world.stage = 1;
                return outcome.TransitionOutcome.applied();
            }
            if (selected.id == fault_spec.stopTransition().id and world.stage == 1) {
                try world.controller.stop(fault_spec.id, sink, allocator);
                world.stage = 2;
                return outcome.TransitionOutcome.targetReached("fault.lifecycle.complete", 2);
            }
            return error.InvalidFaultLifecycleTransition;
        }
        pub fn observe(world: *World, builder: *observation.Builder, allocator: std.mem.Allocator) !void {
            try builder.addNamed(allocator, "fault.active", @intCast(world.controller.activeTotal()));
        }
        pub fn evaluate(_: *World, _: *property.Sink, _: std.mem.Allocator) !void {}
        pub fn done(world: *World) bool {
            return world.stage == 2;
        }
    };

    var scripted = choice.Scripted{ .selections = &.{ Scenario.fault_spec.startTransition().id, Scenario.fault_spec.stopTransition().id } };
    var artifact = try runner.run(Scenario, std.testing.allocator, scripted.source(), .{ .transition_budget = 2 });
    defer artifact.deinit();
    try std.testing.expectEqual(@as(usize, 2), artifact.faults.items.len);
    try std.testing.expectEqual(transition.FaultPhase.start, artifact.faults.items[0].phase);
    try std.testing.expectEqual(transition.FaultPhase.end, artifact.faults.items[1].phase);
    try std.testing.expectEqual(Scenario.fault_spec.id, artifact.faults.items[0].id);
    try std.testing.expectEqual(Scenario.fault_spec.id, artifact.faults.items[1].id);
}
