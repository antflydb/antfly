// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

//! CAS-backed exclusion between HA seed publication and namespace deletion.
//!
//! The control object deliberately lives beside, rather than beneath, the
//! generation prefix. Cleanup can therefore prove the data prefix empty while
//! retaining a durable tombstone that prevents a paused writer from recreating
//! objects after a successful deletion receipt has been emitted.

const std = @import("std");
const Allocator = std.mem.Allocator;
const object_storage = @import("../object_storage.zig");

pub const version: u16 = 1;
const max_control_bytes = 16 * 1024 * 1024;

pub const Store = struct {
    client: *object_storage.ObjectStorage,
    bucket: []const u8,
    prefix: []const u8,
};

pub const Binding = struct {
    topology_id: []const u8,
    topology_generation: u64,
};

const Phase = enum { active, publishing, deleting, deleted };

const State = struct {
    version: u16,
    revision: u64,
    phase: Phase,
    topology_id: []const u8,
    topology_generation: u64,
    owner: []const u8,
    final_receipt_json: ?[]const u8 = null,
};

pub const Lease = struct {
    key: []u8,
    etag: []u8,
    revision: u64,

    pub fn deinit(self: *Lease, alloc: Allocator) void {
        alloc.free(self.key);
        alloc.free(self.etag);
        self.* = undefined;
    }
};

pub const DeleteAcquisition = union(enum) {
    lease: Lease,
    complete: []u8,

    pub fn deinit(self: *DeleteAcquisition, alloc: Allocator) void {
        switch (self.*) {
            .lease => |*lease| lease.deinit(alloc),
            .complete => |body| alloc.free(body),
        }
        self.* = undefined;
    }
};

pub fn acquirePublish(alloc: Allocator, store: Store, binding: Binding, owner: []const u8) !Lease {
    const key = try controlKeyAlloc(alloc, store.prefix);
    errdefer alloc.free(key);
    var current = store.client.getObject(store.bucket, key, .{}) catch |err| switch (err) {
        error.FileNotFound => {
            return try createLease(alloc, store, key, .{
                .version = version,
                .revision = 1,
                .phase = .publishing,
                .topology_id = binding.topology_id,
                .topology_generation = binding.topology_generation,
                .owner = owner,
            });
        },
        else => return err,
    };
    defer current.deinit(alloc);
    const etag = current.metadata.etag orelse return error.SeedNamespaceControlMissingEtag;
    var parsed = std.json.parseFromSlice(State, alloc, current.body, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidSeedNamespaceControl;
    defer parsed.deinit();
    const state = parsed.value;
    try validateState(state);
    if (state.phase == .publishing and sameAuthority(state, binding) and std.mem.eql(u8, state.owner, owner)) {
        return .{ .key = key, .etag = try alloc.dupe(u8, etag), .revision = state.revision };
    }
    // One stable namespace serves the whole HA topology. Once a publication is
    // active, permit only a strictly monotonic generation handoff for that
    // same topology; foreign topology IDs and generation rollback remain
    // fail-closed under the same object-store CAS.
    if (state.phase != .active or
        (!sameAuthority(state, binding) and !sameTopologyNewerGeneration(state, binding)))
        return error.SeedNamespaceUnavailable;
    return try replaceWithLease(alloc, store, key, etag, .{
        .version = version,
        .revision = try std.math.add(u64, state.revision, 1),
        .phase = .publishing,
        .topology_id = binding.topology_id,
        .topology_generation = binding.topology_generation,
        .owner = owner,
    });
}

pub fn releasePublish(alloc: Allocator, store: Store, binding: Binding, owner: []const u8, lease: Lease) !void {
    const body = try std.json.Stringify.valueAlloc(alloc, State{
        .version = version,
        .revision = try std.math.add(u64, lease.revision, 1),
        .phase = .active,
        .topology_id = binding.topology_id,
        .topology_generation = binding.topology_generation,
        .owner = owner,
    }, .{});
    defer alloc.free(body);
    var result = store.client.putObject(store.bucket, lease.key, body, .{
        .content_type = "application/json",
        .if_match_etag = lease.etag,
    }) catch |err| switch (err) {
        error.PreconditionFailed => return error.SeedNamespaceControlLost,
        else => return err,
    };
    result.deinit(alloc);
}

pub fn acquireDelete(alloc: Allocator, store: Store, binding: Binding, owner: []const u8) !DeleteAcquisition {
    const key = try controlKeyAlloc(alloc, store.prefix);
    errdefer alloc.free(key);
    var current = store.client.getObject(store.bucket, key, .{}) catch |err| switch (err) {
        error.FileNotFound => return .{ .lease = try createLease(alloc, store, key, .{
            .version = version,
            .revision = 1,
            .phase = .deleting,
            .topology_id = binding.topology_id,
            .topology_generation = binding.topology_generation,
            .owner = owner,
        }) },
        else => return err,
    };
    defer current.deinit(alloc);
    const etag = current.metadata.etag orelse return error.SeedNamespaceControlMissingEtag;
    var parsed = std.json.parseFromSlice(State, alloc, current.body, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidSeedNamespaceControl;
    defer parsed.deinit();
    const state = parsed.value;
    try validateState(state);
    if (state.phase == .publishing) return error.SeedPrefixCleanupWriterActive;
    if (state.phase == .deleted) {
        if (!sameAuthority(state, binding) or !std.mem.eql(u8, state.owner, owner))
            return error.SeedNamespaceUnavailable;
        const receipt = state.final_receipt_json orelse return error.InvalidSeedNamespaceControl;
        alloc.free(key);
        return .{ .complete = try alloc.dupe(u8, receipt) };
    }
    if (state.phase == .deleting) {
        if (!sameAuthority(state, binding) or !std.mem.eql(u8, state.owner, owner))
            return error.SeedNamespaceUnavailable;
        return .{ .lease = .{ .key = key, .etag = try alloc.dupe(u8, etag), .revision = state.revision } };
    }
    if (!sameAuthority(state, binding)) return error.SeedNamespaceUnavailable;
    return .{ .lease = try replaceWithLease(alloc, store, key, etag, .{
        .version = version,
        .revision = try std.math.add(u64, state.revision, 1),
        .phase = .deleting,
        .topology_id = binding.topology_id,
        .topology_generation = binding.topology_generation,
        .owner = owner,
    }) };
}

pub fn finishDelete(alloc: Allocator, store: Store, binding: Binding, owner: []const u8, lease: Lease, receipt_json: []const u8) !void {
    if (receipt_json.len > max_control_bytes) return error.SeedNamespaceControlTooLarge;
    const body = try std.json.Stringify.valueAlloc(alloc, State{
        .version = version,
        .revision = try std.math.add(u64, lease.revision, 1),
        .phase = .deleted,
        .topology_id = binding.topology_id,
        .topology_generation = binding.topology_generation,
        .owner = owner,
        .final_receipt_json = receipt_json,
    }, .{});
    defer alloc.free(body);
    var result = store.client.putObject(store.bucket, lease.key, body, .{
        .content_type = "application/json",
        .if_match_etag = lease.etag,
    }) catch |err| switch (err) {
        error.PreconditionFailed => return error.SeedNamespaceControlLost,
        else => return err,
    };
    result.deinit(alloc);
}

fn createLease(alloc: Allocator, store: Store, key: []u8, state: State) !Lease {
    const body = try std.json.Stringify.valueAlloc(alloc, state, .{});
    defer alloc.free(body);
    var result = store.client.putObject(store.bucket, key, body, .{
        .content_type = "application/json",
        .if_none_match = true,
    }) catch |err| switch (err) {
        error.PreconditionFailed => return error.SeedNamespaceControlConflict,
        else => return err,
    };
    defer result.deinit(alloc);
    const etag = result.etag orelse return error.SeedNamespaceControlMissingEtag;
    return .{ .key = key, .etag = try alloc.dupe(u8, etag), .revision = state.revision };
}

fn replaceWithLease(alloc: Allocator, store: Store, key: []u8, etag: []const u8, state: State) !Lease {
    const body = try std.json.Stringify.valueAlloc(alloc, state, .{});
    defer alloc.free(body);
    var result = store.client.putObject(store.bucket, key, body, .{
        .content_type = "application/json",
        .if_match_etag = etag,
    }) catch |err| switch (err) {
        error.PreconditionFailed => return error.SeedNamespaceControlConflict,
        else => return err,
    };
    defer result.deinit(alloc);
    const next_etag = result.etag orelse return error.SeedNamespaceControlMissingEtag;
    return .{ .key = key, .etag = try alloc.dupe(u8, next_etag), .revision = state.revision };
}

fn controlKeyAlloc(alloc: Allocator, prefix: []const u8) ![]u8 {
    const normalized = std.mem.trim(u8, prefix, "/");
    if (normalized.len == 0) return try alloc.dupe(u8, ".antfly-ha-seeds.control.json");
    return try std.fmt.allocPrint(alloc, "{s}.control.json", .{normalized});
}

test "storage.ha seed namespace control normalizes remote URI prefixes beside the data namespace" {
    const alloc = std.testing.allocator;
    const without_separator = try controlKeyAlloc(alloc, "instances/instance-a/ha-seeds");
    defer alloc.free(without_separator);
    const with_separator = try controlKeyAlloc(alloc, "/instances/instance-a/ha-seeds/");
    defer alloc.free(with_separator);

    try std.testing.expectEqualStrings("instances/instance-a/ha-seeds.control.json", without_separator);
    try std.testing.expectEqualStrings(without_separator, with_separator);
}

fn sameAuthority(state: State, binding: Binding) bool {
    return state.topology_generation == binding.topology_generation and
        std.mem.eql(u8, state.topology_id, binding.topology_id);
}

fn sameTopologyNewerGeneration(state: State, binding: Binding) bool {
    return binding.topology_generation > state.topology_generation and
        std.mem.eql(u8, state.topology_id, binding.topology_id);
}

test "storage.ha seed namespace permits only monotonic same-topology generation handoff" {
    const state = State{
        .version = version,
        .revision = 2,
        .phase = .active,
        .topology_id = "topology-a",
        .topology_generation = 1,
        .owner = "initial-generation",
    };
    try std.testing.expect(sameTopologyNewerGeneration(state, .{ .topology_id = "topology-a", .topology_generation = 2 }));
    try std.testing.expect(!sameTopologyNewerGeneration(state, .{ .topology_id = "topology-a", .topology_generation = 1 }));
    try std.testing.expect(!sameTopologyNewerGeneration(state, .{ .topology_id = "topology-a", .topology_generation = 0 }));
    try std.testing.expect(!sameTopologyNewerGeneration(state, .{ .topology_id = "topology-b", .topology_generation = 2 }));
}

fn validateState(state: State) !void {
    if (state.version != version or state.revision == 0 or state.topology_id.len == 0 or
        state.topology_generation == 0 or state.owner.len == 0)
        return error.InvalidSeedNamespaceControl;
    if (state.phase == .deleted and state.final_receipt_json == null)
        return error.InvalidSeedNamespaceControl;
    if (state.phase != .deleted and state.final_receipt_json != null)
        return error.InvalidSeedNamespaceControl;
}
