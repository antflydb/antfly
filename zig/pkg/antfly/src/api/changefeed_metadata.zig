// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Internal metadata authority for feed owners. This is not a public state
//! inspection API: raw records contain private root positions and policy data.
//! Only a trusted internal maintenance principal may invoke this capability.
//! Public authorization and ownership checks precede creation of that context.
const std = @import("std");
const control = @import("../metadata/changefeed_control.zig");
const local = @import("antfly_local_sources");
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;
pub const path = "/internal/v1/catalog/changefeed-state-v1";
pub const max_wire_bytes = control.max_state_bytes * 6 + 4096;
pub const Action = enum { read, authority, mutate };
pub const Request = struct {
    version: u16 = 1,
    action: Action,
    metadata_group_id: u64,
    metadata_incarnation: [32]u8,
    id: ?control.Id = null,
    write: ?[]const u8 = null,
    pub fn validate(self: Request) !void {
        if (self.version != 1 or self.metadata_group_id == 0 or std.mem.allEqual(u8, &self.metadata_incarnation, 0)) return error.InvalidChangefeed;
        switch (self.action) {
            .read => if (self.id == null or self.write != null) return error.InvalidChangefeed,
            .authority => if (self.id != null or self.write != null) return error.InvalidChangefeed,
            .mutate => if (self.id != null or self.write == null or self.write.?.len == 0 or self.write.?.len > control.max_state_bytes) return error.InvalidChangefeed,
        }
    }
};
pub const Reply = struct {
    version: u16 = 1,
    action: Action,
    metadata_group_id: u64,
    metadata_incarnation: [32]u8,
    id: ?control.Id = null,
    state: ?[]const u8 = null,
    authority: ?control.Stamp = null,
    pub fn validate(self: Reply, a: A, request: Request) !void {
        try request.validate();
        if (self.version != 1 or self.action != request.action or self.metadata_group_id != request.metadata_group_id or !std.mem.eql(u8, &self.metadata_incarnation, &request.metadata_incarnation)) return error.ChangefeedCorrupt;
        switch (self.action) {
            .read => {
                if (self.id == null or !std.mem.eql(u8, &self.id.?, &request.id.?) or self.state == null or self.authority != null) return error.ChangefeedCorrupt;
                var parsed = try control.parse(a, self.state.?);
                defer parsed.deinit();
                if (parsed.value.phase != .empty and !std.mem.eql(u8, &parsed.value.id, &request.id.?)) return error.ChangefeedCorrupt;
            },
            .authority => {
                if (self.id != null or self.state != null) return error.ChangefeedCorrupt;
                if (self.authority) |stamp| if (stamp.sequence == 0 or std.mem.allEqual(u8, &stamp.incarnation, 0)) return error.ChangefeedCorrupt;
            },
            .mutate => if (self.id != null or self.state != null or self.authority != null) return error.ChangefeedCorrupt,
        }
    }
};
pub fn parseRequest(a: A, bytes: []const u8) !std.json.Parsed(Request) {
    if (bytes.len == 0 or bytes.len > max_wire_bytes) return error.InvalidChangefeed;
    var parsed = try std.json.parseFromSlice(Request, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}
pub const Port = struct {
    ptr: *anyopaque,
    read_fn: *const fn (*anyopaque, A, control.Id, Context) anyerror![]u8,
    mutate_fn: *const fn (*anyopaque, A, control.Write, Context) anyerror!void,
    authority_fn: *const fn (*anyopaque, A, Context) anyerror!?control.Stamp,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct { read: @FieldType(Port, "read_fn"), mutate: @FieldType(Port, "mutate_fn"), authority: @FieldType(Port, "authority_fn") };
    const Boundary = local.runtime_callback_abi.Boundary(VTable);
    fn authorize(context: Context) !void {
        try context.ensureActive();
        const principal = context.principal orelse return error.Forbidden;
        if (principal.kind != .internal or !std.mem.eql(u8, principal.subject, "changefeed-maintenance")) return error.Forbidden;
    }
    pub fn read(self: Port, a: A, id: control.Id, context: Context) ![]u8 {
        try authorize(context);
        const bytes = try Boundary.call("read", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, id, context });
        errdefer a.free(bytes);
        try context.ensureActive();
        var parsed = try control.parse(a, bytes);
        defer parsed.deinit();
        if (parsed.value.phase != .empty and !std.mem.eql(u8, &parsed.value.id, &id)) return error.ChangefeedCorrupt;
        return bytes;
    }
    pub fn mutate(self: Port, a: A, write: control.Write, context: Context) !void {
        try authorize(context);
        try write.validate();
        try Boundary.call("mutate", self.boundary_dispatch, self.mutate_fn, .{ self.ptr, a, write, context });
        // Cancellation after a successful callback cannot certify non-commit.
        context.ensureActive() catch return error.MetadataMutationOutcomeUnknown;
    }
    /// Complete an authorized slot seal, or recover its exact start after an
    /// ambiguous write. The caller supplies the qualified cut and allocation.
    pub fn sealSlot(self: Port, a: A, write: control.Write, now: u64, context: Context) !control.SlotStart {
        try write.validate();
        if (write.mutation != .seal or write.mutation.seal.slot == null or now == 0) return error.InvalidChangefeed;
        const uncertain = blk: {
            self.mutate(a, write, context) catch |err| {
                if (err == error.MetadataMutationOutcomeUnknown) break :blk true;
                return err;
            };
            break :blk false;
        };
        const bytes = self.read(a, write.id, context) catch |err| {
            if (uncertain) return error.MetadataMutationOutcomeUnknown;
            return err;
        };
        defer a.free(bytes);
        var observed = try control.parse(a, bytes);
        defer observed.deinit();
        if (uncertain and observed.value.phase == .enrolling and observed.value.epoch == write.expected_epoch and observed.value.revision == write.expected_revision) return error.MetadataMutationOutcomeUnknown;
        return control.recoverSlotStart(observed.value, write, now);
    }

    /// Apply the authorized drop once. An uncertain response is resolved by
    /// observing the same slot's durable closing or deleted state.
    pub fn dropSlot(self: Port, a: A, write: control.Write, context: Context) !void {
        try write.validate();
        if (write.mutation != .slot or write.mutation.slot != .drop) return error.InvalidChangefeed;
        const drop = write.mutation.slot.drop;
        if (!std.mem.eql(u8, &write.id, &control.pg_slot.feedId(drop.database, drop.name))) return error.InvalidChangefeed;
        self.mutate(a, write, context) catch |err| {
            if (err != error.MetadataMutationOutcomeUnknown) return err;
            const bytes = self.read(a, write.id, context) catch return err;
            defer a.free(bytes);
            var observed = control.parse(a, bytes) catch return err;
            defer observed.deinit();
            const state = observed.value;
            if (state.phase != .closing and state.phase != .deleted) return err;
            const definition = state.definition orelse return err;
            const slot = state.slot orelse return err;
            if (state.revision < write.expected_revision or state.epoch != write.expected_epoch or
                !std.mem.eql(u8, &state.authority, &write.authority) or
                !std.mem.eql(u8, &definition.owner, &write.owner) or
                !std.mem.eql(u8, &definition.authorization, &write.authorization) or
                !std.mem.eql(u8, &slot.database, &drop.database) or
                !std.mem.eql(u8, slot.name, drop.name)) return err;
        };
    }

    pub fn authority(self: Port, a: A, context: Context) !?control.Stamp {
        try authorize(context);
        const result = try Boundary.call("authority", self.boundary_dispatch, self.authority_fn, .{ self.ptr, a, context });
        try context.ensureActive();
        if (result) |stamp| if (stamp.sequence == 0 or std.mem.allEqual(u8, &stamp.incarnation, 0)) return error.ChangefeedCorrupt;
        return result;
    }
    pub fn fromService(svc: anytype) Port {
        const Service = @typeInfo(@TypeOf(svc)).pointer.child;
        const Adapter = struct {
            fn cast(raw: *anyopaque) *Service {
                return @ptrCast(@alignCast(raw));
            }
            fn read(raw: *anyopaque, a: A, id: control.Id, context: Context) ![]u8 {
                try authorize(context);
                return control.readOnService(cast(raw), a, id, context);
            }
            fn mutate(raw: *anyopaque, a: A, write: control.Write, context: Context) !void {
                try authorize(context);
                return control.mutateOnService(cast(raw), a, write, context);
            }
            fn authority(raw: *anyopaque, a: A, context: Context) !?control.Stamp {
                try authorize(context);
                const self = cast(raw);
                try self.ensureLinearizableReadWithContext(context);
                const store = self.projectedStore() orelse return error.MissingMetadataStore;
                return store.getChangefeedAuthority(a, self.metadata_group_id);
            }
        };
        return .{ .ptr = svc, .read_fn = Adapter.read, .mutate_fn = Adapter.mutate, .authority_fn = Adapter.authority };
    }
};
/// Request-scoped facade accepted by the shared feed/root/spool owners. No
/// synthetic proposal receipt or local projection is manufactured for a port.
pub const Client = struct {
    port: Port,
    pub fn readChangefeedState(self: *Client, a: A, id: control.Id, request: Context) ![]u8 {
        return self.port.read(a, id, request);
    }
    pub fn mutateChangefeedState(self: *Client, a: A, write: control.Write, request: Context) !void {
        return self.port.mutate(a, write, request);
    }
};

pub const test_support = if (@import("builtin").is_test) struct {
    const fixture = @import("../pg_spool_cleanup_test.zig");
    const Store = @import("../metadata/storage/raft_apply_store.zig").RaftApplyStore;
    const Table = @import("../metadata/domain.zig").TableRecord;
    const group = local.common_group_ids.main_metadata_group_id;
    pub fn contracts() !void {
        const a = std.testing.allocator;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const store_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/feed-metadata-port", .{tmp.sub_path});
        defer a.free(store_path);
        var store = try Store.init(a, .{ .root_dir = store_path });
        defer store.deinit();
        var svc: fixture.Service = .{ .store = &store, .id = @splat(4) };
        const port = Port.fromService(&svc);
        const authorized: Context = .{ .principal = .{ .kind = .internal, .subject = "changefeed-maintenance" } };
        try std.testing.expectError(error.Forbidden, port.read(a, svc.id, .{}));
        try std.testing.expectError(error.Forbidden, port.authority(a, .{ .principal = .{ .kind = .user, .subject = "changefeed-maintenance" } }));
        try std.testing.expectEqual(@as(u64, 0), svc.proposals);
        try std.testing.expect((try port.authority(a, authorized)) == null);
        const table: Table = .{ .table_id = 7, .name = "docs", .schema_json = "{}" };
        try store.applyStandaloneCommand(group, .{ .upsert_table = table });
        var client: Client = .{ .port = port };
        const write: control.Write = .{ .id = svc.id, .authority = @splat(9), .owner = @splat(1), .authorization = @splat(2), .expected_revision = 0, .expected_epoch = 0, .mutation = .{ .create = .{ .id = svc.id, .definition = .{ .owner = @splat(1), .authorization = @splat(2), .tables = &.{.{ .table_id = 7, .binding = control.tableBinding(table), .policy_revision = 1, .contract = .native_document }} }, .now = 100, .expires_at = 1000 } } };
        svc.lose_ack = true;
        try std.testing.expectError(error.MetadataMutationOutcomeUnknown, control.mutateOnService(&client, a, write, authorized));
        svc.lose_ack = false;
        store.deinit();
        store = try Store.init(a, .{ .root_dir = store_path });
        const bytes = try control.readOnService(&client, a, svc.id, authorized);
        defer a.free(bytes);
        var parsed = try control.parse(a, bytes);
        defer parsed.deinit();
        try std.testing.expectEqual(@as(u64, 1), parsed.value.revision);
        const authority = (try port.authority(a, authorized)).?;
        try std.testing.expectEqual(@as(u64, 1), authority.sequence);
        try std.testing.expectEqual(write.authority, authority.incarnation);
        // A stale retry cannot manufacture a second creation or receipt.
        try std.testing.expectError(error.ChangefeedConflict, control.mutateOnService(&client, a, write, authorized));
        try std.testing.expectEqual(@as(u64, 1), svc.proposals);
        try std.testing.expectError(error.DeadlineExceeded, port.read(a, svc.id, .{ .principal = authorized.principal, .deadline_ns = 1 }));
        try slotSealRecovery(authorized);
        // Physical cache allocations vary between reads. Exercise the port's
        // owned response and parser cleanup against a deterministic callback.
        const Fixed = struct {
            bytes: []const u8,
            fn read(raw: *anyopaque, allocator: A, _: control.Id, _: Context) ![]u8 {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return allocator.dupe(u8, self.bytes);
            }
        };
        var fixed: Fixed = .{ .bytes = bytes };
        const fixed_port: Port = .{ .ptr = &fixed, .read_fn = Fixed.read, .mutate_fn = undefined, .authority_fn = undefined };
        // JSON arena growth may fall back after a failed resize, so allocation
        // counts are not stable. Fail the two mandatory initial allocations:
        // the response copy and the parser's first allocation.
        for (0..2) |fail_index| {
            var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
            try std.testing.expectError(error.OutOfMemory, readAllocations(failing.allocator(), fixed_port, svc.id, authorized));
            try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        }
    }
    fn slotSealRecovery(context: Context) !void {
        const a = std.testing.allocator;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const configuration: control.pg_slot.Configuration = .{ .database = @splat(8), .name = "slot", .consistent_lsn = 8192 };
        const id = control.pg_slot.feedId(configuration.database, configuration.name);
        const definition: control.Definition = .{
            .owner = @splat(1),
            .authorization = @splat(2),
            .projection = .rows,
            .tables = &.{.{ .table_id = 7, .binding = @splat(3), .policy_revision = 1, .contract = .native_relational }},
        };
        const namespace: control.Namespace = [8]u8{ 0, 0, 0, 0, 0, 0, 0, 7 } ++ @as([16]u8, @splat(1));
        const stamp: control.Stamp = .{ .incarnation = @splat(9), .sequence = 1 };
        var state = try (control.Mutation{ .create = .{ .id = id, .definition = definition, .now = 100, .expires_at = 1000 } }).apply(scratch, .{}, stamp);
        state = try (control.Mutation{ .plan_roots = &.{.{ .namespace = namespace, .ownership_epoch = 1 }} }).apply(scratch, state, .{ .incarnation = stamp.incarnation, .sequence = 2 });
        state = try (control.Mutation{ .enroll = .{ .namespace = namespace, .ownership_epoch = 1, .grant_sequence = state.roots[0].grant_sequence, .consumer_epoch = 1, .start = 10 } }).apply(scratch, state, .{ .incarnation = stamp.incarnation, .sequence = 3 });
        const write: control.Write = .{ .id = id, .authority = stamp.incarnation, .owner = definition.owner, .authorization = definition.authorization, .expected_revision = state.revision, .expected_epoch = state.epoch, .mutation = .{ .seal = .{ .cut = @splat(7), .slot = configuration } } };
        const sealed = try write.apply(scratch, state, 4);
        const Fake = struct {
            state: control.State,
            sealed: control.State,
            commit: bool = true,
            mutations: usize = 0,

            fn read(raw: *anyopaque, allocator: A, _: control.Id, _: Context) anyerror![]u8 {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return control.encode(allocator, self.state);
            }
            fn mutate(raw: *anyopaque, _: A, _: control.Write, _: Context) anyerror!void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.mutations += 1;
                if (self.commit) self.state = self.sealed;
                return error.MetadataMutationOutcomeUnknown;
            }
        };
        var fake: Fake = .{ .state = state, .sealed = sealed, .commit = false };
        const port: Port = .{ .ptr = &fake, .read_fn = Fake.read, .mutate_fn = Fake.mutate, .authority_fn = undefined };
        try std.testing.expectError(error.MetadataMutationOutcomeUnknown, port.sealSlot(a, write, 101, context));
        try std.testing.expectEqual(@as(usize, 1), fake.mutations);
        fake.commit = true;
        const commands = @import("sql_pgwire.zig").SlotCommands;
        const created = try commands.create(port, scratch, .{ .slot = configuration.name }, write, 101, "qualified-snapshot", context);
        try std.testing.expectEqualStrings("CREATE_REPLICATION_SLOT", created.command_tag);
        try std.testing.expectEqualStrings("0/2000", created.rows[0][1].string);
        try std.testing.expectEqualStrings("qualified-snapshot", created.rows[0][2].string);
        try std.testing.expect(created.mutation_outcome == .committed);
        try std.testing.expectEqual(@as(usize, 2), fake.mutations);
        var drop = write;
        drop.expected_revision = sealed.revision;
        drop.mutation = .{ .slot = .{ .drop = .{ .database = configuration.database, .name = configuration.name, .now = 102 } } };
        fake.sealed = try drop.apply(scratch, sealed, 5);
        fake.commit = false;
        try std.testing.expectError(error.MetadataMutationOutcomeUnknown, port.dropSlot(a, drop, context));
        try std.testing.expectEqual(@as(usize, 3), fake.mutations);
        fake.commit = true;
        const dropped = try commands.drop(port, a, .{ .drop_slot = .{ .slot = configuration.name, .wait = false } }, drop, context);
        try std.testing.expectEqualStrings("DROP_REPLICATION_SLOT", dropped.command_tag);
        try std.testing.expect(dropped.mutation_outcome == .committed);
        try std.testing.expectEqual(@as(usize, 4), fake.mutations);
        try std.testing.expectEqual(control.Phase.closing, fake.state.phase);
        var stale = drop;
        stale.expected_epoch += 1;
        try std.testing.expectError(error.MetadataMutationOutcomeUnknown, port.dropSlot(a, stale, context));
    }

    fn readAllocations(a: A, port: Port, id: control.Id, context: Context) !void {
        const bytes = try port.read(a, id, context);
        defer a.free(bytes);
    }
} else struct {};
