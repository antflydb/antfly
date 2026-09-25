// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Private parent-owner FK generation publication boundary. The request is
//! only an identity/probe; the receiving owner must fetch its own exact,
//! linearizable metadata decision before proposing a replicated transition.
const std = @import("std");
const operation = @import("operation.zig");
const callback_abi = @import("../runtime_callback_abi.zig");
const integrity = @import("../storage/db/relational_integrity_contract.zig");
const identity = @import("../storage/db/doc_identity_namespace.zig");

pub const Action = enum { stage, activate, acknowledge, cancel };

/// A child owner first fences old-generation writes, then installs the exact
/// metadata-published successor schema before reopening admission. These are
/// identity-only requests; the owner obtains the current plan at read-index.
pub const SourceAction = enum { fence, install, cancel };

/// Initial FK-bearing CREATE has no prior child catalog. Metadata keeps the
/// candidate and owner descriptors hidden until every parent has accepted the
/// new generation and each child owner has a durable provision/release proof.
pub const InitialChildAction = enum { provision, release, cancel };

pub const InitialChildRequest = struct {
    plan_id: [16]u8,
    child_table_id: u64,
    child_table_name: []const u8,
    child_group_id: u64,
    action: InitialChildAction,

    pub fn validate(self: InitialChildRequest, group_id: u64) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or self.child_table_id == 0 or
            self.child_group_id == 0 or self.child_group_id != group_id or
            self.child_table_name.len == 0 or self.child_table_name.len > 256 or
            !std.unicode.utf8ValidateSlice(self.child_table_name)) return error.InvalidGenerationPublication;
    }
};

pub const InitialChildReceipt = struct {
    plan_id: [16]u8,
    child_table_id: u64,
    child_group_id: u64,
    action: InitialChildAction,
    namespace: identity.Namespace,
    plan_digest: integrity.Digest,
    schema_version: u32,
    schema_digest: integrity.Digest,
    public_schema_json_digest: integrity.Digest,
    catalog_digest: integrity.Digest,
    row_count: u64,
    applied_term: u64,
    applied_index: u64,

    pub fn validate(self: InitialChildReceipt, request: InitialChildRequest) !void {
        if (!std.mem.eql(u8, &self.plan_id, &request.plan_id) or
            self.child_table_id != request.child_table_id or
            self.child_group_id != request.child_group_id or
            self.action != request.action or
            self.namespace.table_id != request.child_table_id or
            self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            self.row_count != 0 or
            self.applied_term == 0 or self.applied_index == 0 or
            std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.schema_digest, 0) or
            std.mem.allEqual(u8, &self.public_schema_json_digest, 0) or
            std.mem.allEqual(u8, &self.catalog_digest, 0)) return error.InvalidGenerationPublication;
    }

    pub fn digest(self: InitialChildReceipt) integrity.Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly initial FK child owner receipt v1");
        hash.update(&self.plan_id);
        hash.update(@tagName(self.action));
        var buf: [8]u8 = undefined;
        inline for (.{ self.child_table_id, self.child_group_id, self.namespace.table_id, self.namespace.shard_id, self.namespace.range_id, self.row_count, self.applied_term, self.applied_index }) |value| {
            std.mem.writeInt(u64, &buf, value, .little);
            hash.update(&buf);
        }
        std.mem.writeInt(u32, buf[0..4], self.schema_version, .little);
        hash.update(buf[0..4]);
        hash.update(&self.plan_digest);
        hash.update(&self.schema_digest);
        hash.update(&self.public_schema_json_digest);
        hash.update(&self.catalog_digest);
        var result: integrity.Digest = undefined;
        hash.final(&result);
        return result;
    }
};

pub const SourceRequest = struct {
    plan_id: [16]u8,
    child_table_id: u64,
    child_table_name: []const u8,
    child_group_id: u64,
    action: SourceAction,

    pub fn validate(self: SourceRequest, group_id: u64) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or self.child_table_id == 0 or
            self.child_group_id == 0 or self.child_group_id != group_id or
            self.child_table_name.len == 0 or self.child_table_name.len > 256 or
            !std.unicode.utf8ValidateSlice(self.child_table_name))
            return error.InvalidGenerationPublication;
    }
};

pub const SourceReceipt = struct {
    plan_id: [16]u8,
    child_table_id: u64,
    child_group_id: u64,
    action: SourceAction,
    plan_digest: integrity.Digest,
    fence_digest: integrity.Digest,
    before_schema_version: u32,
    before_schema_digest: integrity.Digest,
    before_catalog_digest: integrity.Digest,
    after_schema_version: u32,
    after_schema_digest: integrity.Digest,
    after_catalog_digest: integrity.Digest,
    applied_term: u64,
    applied_index: u64,

    pub fn validate(self: SourceReceipt, request: SourceRequest) !void {
        if (!std.mem.eql(u8, &self.plan_id, &request.plan_id) or
            self.child_table_id != request.child_table_id or
            self.child_group_id != request.child_group_id or
            self.action != request.action or
            self.after_schema_version <= self.before_schema_version or
            self.applied_term == 0 or self.applied_index == 0 or
            std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.fence_digest, 0) or
            std.mem.allEqual(u8, &self.before_schema_digest, 0) or
            std.mem.allEqual(u8, &self.after_schema_digest, 0) or
            std.mem.allEqual(u8, &self.before_catalog_digest, 0) or
            std.mem.allEqual(u8, &self.after_catalog_digest, 0))
            return error.InvalidGenerationPublication;
    }

    pub fn digest(self: SourceReceipt) integrity.Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly child FK generation publication receipt v1");
        hash.update(&self.plan_id);
        hash.update(@tagName(self.action));
        var buf: [8]u8 = undefined;
        inline for (.{ self.child_table_id, self.child_group_id, self.applied_term, self.applied_index }) |value| {
            std.mem.writeInt(u64, &buf, value, .little);
            hash.update(&buf);
        }
        inline for (.{ self.before_schema_version, self.after_schema_version }) |value| {
            std.mem.writeInt(u32, buf[0..4], value, .little);
            hash.update(buf[0..4]);
        }
        hash.update(&self.plan_digest);
        hash.update(&self.fence_digest);
        hash.update(&self.before_schema_digest);
        hash.update(&self.before_catalog_digest);
        hash.update(&self.after_schema_digest);
        hash.update(&self.after_catalog_digest);
        var result: integrity.Digest = undefined;
        hash.final(&result);
        return result;
    }
};

pub const Request = struct {
    plan_id: [16]u8,
    parent_table_id: u64,
    parent_group_id: u64,
    child_table_id: u64,
    child_table_name: []const u8,
    action: Action,

    pub fn validate(self: Request, group_id: u64) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or self.parent_table_id == 0 or
            self.parent_group_id == 0 or self.parent_group_id != group_id or
            self.child_table_id == 0 or self.child_table_name.len == 0 or
            self.child_table_name.len > 256 or !std.unicode.utf8ValidateSlice(self.child_table_name)) return error.InvalidGenerationPublication;
    }
};

pub const Receipt = struct {
    plan_id: [16]u8,
    parent_table_id: u64,
    parent_group_id: u64,
    child_table_id: u64,
    action: Action,
    transitions_digest: integrity.Digest,
    decision_digest: integrity.Digest,
    applied_term: u64,
    applied_index: u64,

    pub fn validate(self: Receipt, request: Request) !void {
        if (!std.mem.eql(u8, &self.plan_id, &request.plan_id) or
            self.parent_table_id != request.parent_table_id or
            self.parent_group_id != request.parent_group_id or
            self.child_table_id != request.child_table_id or
            self.action != request.action or
            self.applied_term == 0 or self.applied_index == 0 or
            std.mem.allEqual(u8, &self.transitions_digest, 0) or
            std.mem.allEqual(u8, &self.decision_digest, 0))
            return error.InvalidGenerationPublication;
    }

    pub fn digest(self: Receipt) integrity.Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly parent FK generation publication receipt v1");
        hash.update(&self.plan_id);
        var buf: [8]u8 = undefined;
        inline for (.{ self.parent_table_id, self.parent_group_id, self.child_table_id, self.applied_term, self.applied_index }) |value| {
            std.mem.writeInt(u64, &buf, value, .little);
            hash.update(&buf);
        }
        hash.update(@tagName(self.action));
        hash.update(&self.transitions_digest);
        hash.update(&self.decision_digest);
        var result: integrity.Digest = undefined;
        hash.final(&result);
        return result;
    }
};

pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Receipt,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Receipt };
    const BoundaryAbi = callback_abi.Boundary(VTable);

    pub fn execute(self: Port, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: Request, context: operation.RequestContext) !Receipt {
        try context.ensureActive();
        try request.validate(group_id);
        const receipt = try BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, alloc, table_name, group_id, request, context });
        try receipt.validate(request);
        return receipt;
    }
};

pub const SourcePort = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, SourceRequest, operation.RequestContext) anyerror!SourceReceipt,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, SourceRequest, operation.RequestContext) anyerror!SourceReceipt };
    const BoundaryAbi = callback_abi.Boundary(VTable);

    pub fn execute(self: SourcePort, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: SourceRequest, context: operation.RequestContext) !SourceReceipt {
        try context.ensureActive();
        try request.validate(group_id);
        const receipt = try BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, alloc, table_name, group_id, request, context });
        try receipt.validate(request);
        return receipt;
    }
};

pub const InitialChildPort = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, InitialChildRequest, operation.RequestContext) anyerror!InitialChildReceipt,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, InitialChildRequest, operation.RequestContext) anyerror!InitialChildReceipt };
    const BoundaryAbi = callback_abi.Boundary(VTable);

    pub fn execute(self: InitialChildPort, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: InitialChildRequest, context: operation.RequestContext) !InitialChildReceipt {
        try context.ensureActive();
        try request.validate(group_id);
        const receipt = try BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, alloc, table_name, group_id, request, context });
        try receipt.validate(request);
        return receipt;
    }
};

test "FK generation private owner request binds physical owner and receipt transition" {
    const req: Request = .{ .plan_id = @splat(1), .parent_table_id = 7, .parent_group_id = 9, .child_table_id = 11, .child_table_name = "children", .action = .stage };
    try req.validate(9);
    try std.testing.expectError(error.InvalidGenerationPublication, req.validate(10));
    const receipt: Receipt = .{ .plan_id = req.plan_id, .parent_table_id = req.parent_table_id, .parent_group_id = req.parent_group_id, .child_table_id = req.child_table_id, .action = .stage, .transitions_digest = @splat(2), .decision_digest = @splat(4), .applied_term = 5, .applied_index = 6 };
    try receipt.validate(req);
    var changed = receipt;
    changed.transitions_digest = @splat(9);
    const first = receipt.digest();
    const second = changed.digest();
    try std.testing.expect(!std.mem.eql(u8, &first, &second));
}

test "FK generation child source request and receipt bind schema cut" {
    const req: SourceRequest = .{ .plan_id = @splat(1), .child_table_id = 7, .child_table_name = "children", .child_group_id = 9, .action = .fence };
    try req.validate(9);
    try std.testing.expectError(error.InvalidGenerationPublication, req.validate(10));
    const receipt: SourceReceipt = .{
        .plan_id = req.plan_id,
        .child_table_id = req.child_table_id,
        .child_group_id = req.child_group_id,
        .action = .fence,
        .plan_digest = @splat(2),
        .fence_digest = @splat(3),
        .before_schema_version = 1,
        .before_schema_digest = @splat(4),
        .before_catalog_digest = @splat(6),
        .after_schema_version = 2,
        .after_schema_digest = @splat(5),
        .after_catalog_digest = @splat(7),
        .applied_term = 6,
        .applied_index = 7,
    };
    try receipt.validate(req);
    // A newly created SQL table has native schema version zero. Its first
    // FK generation is the valid 0 -> 1 schema cut.
    var initial = receipt;
    initial.before_schema_version = 0;
    initial.after_schema_version = 1;
    try initial.validate(req);
    const wire = try std.json.Stringify.valueAlloc(std.testing.allocator, initial, .{});
    defer std.testing.allocator.free(wire);
    var decoded = try std.json.parseFromSlice(SourceReceipt, std.testing.allocator, wire, .{});
    defer decoded.deinit();
    try decoded.value.validate(req);
    try std.testing.expectEqual(initial.digest(), decoded.value.digest());
    initial.after_schema_version = 0;
    try std.testing.expectError(error.InvalidGenerationPublication, initial.validate(req));
    var changed = receipt;
    changed.after_schema_digest = @splat(9);
    const first = receipt.digest();
    const second = changed.digest();
    try std.testing.expect(!std.mem.eql(u8, &first, &second));
}

test "initial FK child receipt binds hidden owner and empty state" {
    const request: InitialChildRequest = .{ .plan_id = @splat(1), .child_table_id = 7, .child_table_name = "children", .child_group_id = 9, .action = .provision };
    try request.validate(9);
    try std.testing.expectError(error.InvalidGenerationPublication, request.validate(10));
    const receipt: InitialChildReceipt = .{
        .plan_id = request.plan_id,
        .child_table_id = request.child_table_id,
        .child_group_id = request.child_group_id,
        .action = .provision,
        .namespace = .{ .table_id = 7, .shard_id = 11, .range_id = 13 },
        .plan_digest = @splat(2),
        .schema_version = 1,
        .schema_digest = @splat(3),
        .public_schema_json_digest = @splat(4),
        .catalog_digest = @splat(5),
        .row_count = 0,
        .applied_term = 1,
        .applied_index = 2,
    };
    try receipt.validate(request);
    var first_epoch = receipt;
    first_epoch.schema_version = 0;
    try first_epoch.validate(request);
    var changed = receipt;
    changed.row_count = 1;
    try std.testing.expectError(error.InvalidGenerationPublication, changed.validate(request));
    changed = receipt;
    changed.catalog_digest = @splat(6);
    const first = receipt.digest();
    const second = changed.digest();
    try std.testing.expect(!std.mem.eql(u8, &first, &second));
}

test "FK generation schema Raft command keeps canonical JSON bytes on wire" {
    const topology = @import("../storage/db/relational_integrity_topology_contract.zig");
    const schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}";
    const command: topology.Command = .{
        .action = .install_child_schema,
        .fence = .{
            .role = .child_generation_source,
            .transition_id = 7,
            .attempt = 8,
            .admission_epoch = 9,
            .peer_group_id = 10,
            .owner_group_id = 10,
            .namespace = .{ .table_id = 11, .shard_id = 12, .range_id = 13 },
            .catalog_digest = @splat(14),
        },
        .child_schema_install = .{
            .schema_json = schema_json,
            .before_schema_json_digest = @splat(18),
            .schema_json_digest = @splat(15),
            .before_catalog_digest = @splat(16),
            .after_catalog_digest = @splat(17),
        },
    };
    const alloc = std.testing.allocator;
    const encoded = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\\\"version\\\"") != null);
    var parsed = try std.json.parseFromSlice(topology.Command, alloc, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings(schema_json, parsed.value.child_schema_install.?.schema_json);
}
