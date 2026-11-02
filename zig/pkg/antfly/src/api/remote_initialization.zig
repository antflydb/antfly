// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Root-signed first-use handshake. The journal calls resolve under its local
//! exclusive lock; the owner calls activate after durable initialization and
//! before publishing ingress. Every call retains the original bootstrap budget.
const std = @import("std");
const wire = @import("../metadata/remote_attempt_initialization.zig");
const durable = @import("../common/remote_fence_store.zig");
const operation = @import("antfly_local_sources").api_operation;

pub const Driver = struct {
    allocator: std.mem.Allocator,
    context: *anyopaque,
    call: *const fn (*anyopaque, std.mem.Allocator, operation.RequestContext, wire.Request) anyerror![]u8,
    request: operation.RequestContext,
    identity: wire.Identity,
    reporter: u64,
    seed: [32]u8,
    candidate: durable.InitializationId,

    fn invoke(self: *Driver, action: wire.Action, id: durable.InitializationId) !wire.Entry {
        try self.request.ensureActive();
        const request = try wire.Request.sign(self.identity, self.reporter, action, id, self.seed);
        const bytes = try self.call(self.context, self.allocator, self.request, request);
        defer self.allocator.free(bytes);
        try self.request.ensureActive();
        if (bytes.len > 8192) return error.InvalidStoreRootEnrollment;
        var parsed = std.json.parseFromSlice(wire.Entry, self.allocator, bytes, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidStoreRootEnrollment;
        defer parsed.deinit();
        try parsed.value.validate();
        if (!std.meta.eql(parsed.value.identity, self.identity)) return error.StoreRootEnrollmentChanged;
        return parsed.value;
    }
    fn resolve(raw: *anyopaque) !durable.InitializationGrant {
        const self: *Driver = @ptrCast(@alignCast(raw));
        // Normal restart only reads. Missing authority must be reserved through
        // the coordinated decoder barrier before any local journal is created.
        const entry = self.invoke(.status, @splat(0)) catch |err| switch (err) {
            error.CatalogNotFound => try self.invoke(.prepare, self.candidate),
            else => return err,
        };
        return .{ .id = entry.initialization_id, .phase = switch (entry.phase) {
            .prepared => .prepared,
            .active => .active,
        } };
    }
    pub fn authority(self: *Driver) durable.InitializationAuthority {
        return .{ .context = self, .resolve = resolve };
    }
    pub fn activate(raw: *anyopaque, id: durable.InitializationId) !void {
        const self: *Driver = @ptrCast(@alignCast(raw));
        const entry = self.invoke(.activate, id) catch |err| switch (err) {
            // A lost response can be resolved by a fresh linearizable status,
            // never by assuming our proposal was committed.
            error.MetadataMutationOutcomeUnknown => try self.invoke(.status, @splat(0)),
            else => return err,
        };
        if (entry.phase != .active or !std.mem.eql(u8, &entry.initialization_id, &id)) return error.MetadataMutationOutcomeUnknown;
    }
};

const TestCatalog = struct {
    entry: ?wire.Entry = null,
    calls: usize = 0,
    prepares: usize = 0,
    lost_activation: bool = false,
    block_activation: bool = false,
    wrong_identity: bool = false,
    wrong_id: bool = false,
    fn invoke(raw: *anyopaque, alloc: std.mem.Allocator, context: operation.RequestContext, request: wire.Request) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try context.ensureActive();
        try std.testing.expect(!context.setting_admin);
        try request.validate();
        self.calls += 1;
        if (request.action == .status) {
            var entry = self.entry orelse return error.CatalogNotFound;
            if (self.wrong_identity) entry.identity.node_id += 1;
            return std.json.Stringify.valueAlloc(alloc, entry, .{});
        }
        if (request.action == .prepare) self.prepares += 1;
        if (request.action == .activate and self.block_activation) return error.MetadataMutationOutcomeUnknown;
        self.entry = try wire.transition(self.entry, request);
        if (request.action == .activate and self.lost_activation) return error.MetadataMutationOutcomeUnknown;
        var entry = self.entry.?;
        if (self.wrong_id) entry.initialization_id = @splat(8);
        return std.json.Stringify.valueAlloc(alloc, entry, .{});
    }
    fn driver(self: *@This(), allocator: std.mem.Allocator) !Driver {
        const seed: [32]u8 = @splat(7);
        return .{ .allocator = allocator, .context = self, .call = invoke, .request = .{}, .identity = .{ .metadata_incarnation = "11111111111111111111111111111111".*, .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = (try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed)).public_key.toBytes() }, .reporter = 4, .seed = seed, .candidate = @splat(1) };
    }
};

test "remote initialization bootstrap resolves lost activation without changing the original ID" {
    var catalog: TestCatalog = .{};
    var driver = try catalog.driver(std.testing.allocator);
    const authority = driver.authority();
    const prepared = try authority.resolve(authority.context);
    try std.testing.expectEqual(durable.InitializationGrant{ .id = @splat(1), .phase = .prepared }, prepared);
    driver.candidate = @splat(2);
    try std.testing.expectEqualDeep(prepared, try authority.resolve(authority.context));
    try std.testing.expectEqual(@as(usize, 1), catalog.prepares);
    catalog.block_activation = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, Driver.activate(&driver, prepared.id));
    catalog.block_activation = false;
    catalog.lost_activation = true;
    try Driver.activate(&driver, prepared.id);
    try std.testing.expectEqual(wire.Phase.active, catalog.entry.?.phase);
    try std.testing.expectEqualDeep(prepared.id, (try authority.resolve(authority.context)).id);
    catalog.wrong_identity = true;
    try std.testing.expectError(error.StoreRootEnrollmentChanged, authority.resolve(authority.context));
    catalog.wrong_identity = false;
    catalog.lost_activation = false;
    catalog.wrong_id = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, Driver.activate(&driver, prepared.id));
    const calls = catalog.calls;
    driver.request.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, authority.resolve(authority.context));
    try std.testing.expectEqual(calls, catalog.calls);
}

test "remote initialization bootstrap allocation failures never invent activation success" {
    const Failure = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var catalog: TestCatalog = .{};
            var driver = try catalog.driver(allocator);
            const authority = driver.authority();
            const grant = try authority.resolve(authority.context);
            catalog.lost_activation = true;
            try Driver.activate(&driver, grant.id);
            try std.testing.expectEqual(wire.Phase.active, catalog.entry.?.phase);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Failure.run, .{});
}
