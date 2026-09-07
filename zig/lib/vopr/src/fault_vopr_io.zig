// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Applies the reusable fault algebra to the deterministic `VoprIo` backend.
//! Persistent effects are rebuilt from controller state on every reconcile;
//! one-shot effects are armed exactly once per fault activation.

const std = @import("std");
const fault = @import("fault.zig");
const vopr_io = @import("vopr_io.zig");

pub const Effect = enum {
    network_outage,
    network_delivery_paused,
    network_reordering,
    drop_next_network_packet,
    duplicate_next_network_packet,
    fail_next_file_read,
    fail_next_file_write,
    drop_next_file_sync,
    crash_file_system,
};

pub const Mapping = struct {
    fault_id: fault.FaultId,
    effect: Effect,
};

pub const Adapter = struct {
    allocator: std.mem.Allocator,
    io: *vopr_io.VoprIo,
    mappings: []const Mapping,
    armed_one_shots: std.ArrayListUnmanaged(fault.FaultId) = .empty,

    pub fn init(allocator: std.mem.Allocator, io: *vopr_io.VoprIo, mappings: []const Mapping) Adapter {
        return .{ .allocator = allocator, .io = io, .mappings = mappings };
    }

    pub fn deinit(self: *Adapter) void {
        self.armed_one_shots.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn reconcile(self: *Adapter, controller: *const fault.Controller) !void {
        self.io.setNetworkOutage(false);
        self.io.setNetworkDeliveryPaused(false);
        self.io.setNetworkReordering(false);

        var index: usize = 0;
        while (index < self.armed_one_shots.items.len) {
            if (controller.isActive(self.armed_one_shots.items[index])) {
                index += 1;
            } else {
                _ = self.armed_one_shots.swapRemove(index);
            }
        }

        const ordered = try controller.effectiveOrderAlloc(self.allocator);
        defer self.allocator.free(ordered);
        for (ordered) |fault_id| {
            const mapping = self.mappingFor(fault_id) orelse continue;
            const spec = controller.activeSpec(fault_id) orelse return error.UnknownActiveFault;
            if (spec.lifecycle == .one_shot and self.wasArmed(fault_id)) continue;
            try self.apply(mapping.effect);
            if (spec.lifecycle == .one_shot) try self.armed_one_shots.append(self.allocator, fault_id);
        }
    }

    fn mappingFor(self: *const Adapter, fault_id: fault.FaultId) ?Mapping {
        for (self.mappings) |mapping| if (mapping.fault_id == fault_id) return mapping;
        return null;
    }

    fn wasArmed(self: *const Adapter, fault_id: fault.FaultId) bool {
        for (self.armed_one_shots.items) |armed| if (armed == fault_id) return true;
        return false;
    }

    fn apply(self: *Adapter, effect: Effect) !void {
        switch (effect) {
            .network_outage => self.io.setNetworkOutage(true),
            .network_delivery_paused => self.io.setNetworkDeliveryPaused(true),
            .network_reordering => self.io.setNetworkReordering(true),
            .drop_next_network_packet => self.io.dropNextNetworkPacket(),
            .duplicate_next_network_packet => self.io.duplicateNextNetworkPacket(),
            .fail_next_file_read => self.io.failNextFileRead(),
            .fail_next_file_write => self.io.failNextFileWrite(),
            .drop_next_file_sync => self.io.dropNextFileSync(),
            .crash_file_system => try self.io.crashFileSystem(),
        }
    }
};

test "fault algebra drives persistent and one-shot VoprIo effects" {
    const event = @import("event.zig");
    const ids = @import("id.zig");
    var sim = try vopr_io.VoprIo.init(.{ .required = .of(&.{ .files, .sockets }) });
    defer sim.deinit();
    var controller = try fault.Controller.initWithAlgebra(std.testing.allocator, 1, .{
        .minimum_healthy_nodes = 1,
        .max_storage_faults_per_durability_epoch = 2,
    }, .{ .rules = &.{.{ .left = .partition, .right = .storage_corruption, .relationship = .left_precedes }} });
    defer controller.deinit();
    var events = event.Sink{};
    defer events.deinit(std.testing.allocator);
    var outage = fault.Spec.named(.partitioned_link, .persistent, "network.outage");
    outage.resource_id = ids.stable("resource", "network");
    var write_failure = fault.Spec.named(.storage, .one_shot, "storage.fail-next-write");
    write_failure.resource_id = ids.stable("resource", "filesystem");
    try controller.start(write_failure, &events, std.testing.allocator);
    try controller.start(outage, &events, std.testing.allocator);
    var adapter = Adapter.init(std.testing.allocator, &sim, &.{
        .{ .fault_id = outage.id, .effect = .network_outage },
        .{ .fault_id = write_failure.id, .effect = .fail_next_file_write },
    });
    defer adapter.deinit();
    try adapter.reconcile(&controller);
    try std.testing.expect(sim.network.faults.network_down);
    try std.testing.expect(sim.files.faults.fail_next_write);
    sim.files.faults.fail_next_write = false;
    try adapter.reconcile(&controller);
    try std.testing.expect(!sim.files.faults.fail_next_write);
    try controller.stop(outage.id, &events, std.testing.allocator);
    try adapter.reconcile(&controller);
    try std.testing.expect(!sim.network.faults.network_down);
}
