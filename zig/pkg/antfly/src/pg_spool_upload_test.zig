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

const std = @import("std");
const fixture = @import("pg_spool_cleanup_test.zig");
const control = @import("metadata/changefeed_control.zig");
const upload = @import("metadata/pg_spool_upload.zig");
const cleanup = @import("metadata/pg_spool_cleanup.zig");
const spool = @import("metadata/pg_spool.zig");
const adapter = @import("metadata/pg_spool_artifacts.zig");
const pgoutput = @import("pgwire/pgoutput.zig");
const artifacts = @import("serverless/artifacts/store.zig");
const FsStore = @import("serverless/artifacts/fs_store.zig").FsStore;
const Store = @import("metadata/storage/raft_apply_store.zig").RaftApplyStore;
const Table = @import("metadata/domain.zig").TableRecord;
const group = @import("antfly_local_sources").common_group_ids.main_metadata_group_id;
const A = std.mem.Allocator;
const Request = fixture.Request;
const Registry = struct {
    fs: *FsStore,
    scopes: usize = 0,
    pub fn newUploadScope(self: *@This(), state: control.State, id: u64, _: Request) !control.pg_slot.spool_control.Scope {
        self.scopes += 1;
        const scope = try artifacts.UploadScope.forPublication(control.pg_slot.spool_control.Scope.domainFor(state.id, state.epoch), id, std.testing.io);
        return .{ .namespace = @splat(9), .domain = scope.domain, .attempt = scope.attempt };
    }
    pub fn newBuildToken(_: *@This(), _: anytype, _: Request) !control.Id {
        var token: control.Id = undefined;
        std.testing.io.random(&token);
        return token;
    }
    pub fn artifactStore(self: *@This(), _: anytype, _: anytype) !artifacts.ArtifactStore {
        var store = self.fs.artifactStore();
        store.namespace_identity = @splat(9);
        return store;
    }
    pub fn qualify(_: *@This(), _: control.State, shared: bool, _: anytype) !void {
        if (shared) return error.PgSpoolStorageUnsupported;
    }
    pub fn protect(_: *@This(), _: control.State, _: anytype, seal: spool.Seal, writer: *adapter.Adapter, _: Request) !void {
        _ = try writer.recordClosure(seal);
    }
    pub fn authorizeCleanup(_: *@This(), state: control.State, _: Request) !void {
        if (state.definition == null) return error.TestDenied;
    }
};
fn namespace() control.Namespace {
    var value: control.Namespace = @splat(0);
    std.mem.writeInt(u64, value[0..8], 7, .big);
    std.mem.writeInt(u64, value[8..16], 1, .big);
    std.mem.writeInt(u64, value[16..24], 1, .big);
    return value;
}
const Iterator = struct {
    start: u64,
    bytes: u64 = 104,
    position: usize = 0,
    corrupt: bool = false,
    pub fn deinit(_: *@This()) void {}
    pub fn next(self: *@This(), a: A, limit: u32, _: Request) !?[]u8 {
        const index = self.position;
        self.position += 1;
        if (index == 0) return try pgoutput.encode(a, .{ .begin = .{ .final_lsn = @fromBackingInt(self.start + self.bytes - 26), .commit_time = -1, .xid = 42 } }, limit);
        if (index <= 3) return try pgoutput.encode(a, .{ .insert = .{ .relation = 42, .values = &.{.{ .text = if (self.corrupt and index == 1) "broken" else "frozen" }} } }, limit);
        if (index == 4) return try pgoutput.encode(a, .{ .commit = .{ .commit_lsn = @fromBackingInt(self.start + self.bytes - 26), .end_lsn = @fromBackingInt(self.start + self.bytes), .commit_time = -1 } }, limit);
        return null;
    }
};
const Producer = struct {
    initial: ?control.Digest = null,
    prepares: usize = 0,
    invisible: bool = true,
    opens: usize = 0,
    denied: bool = false,
    corrupt: bool = false,
    clock: u64 = 110,
    renew_on_open: ?*fixture.Service = null,
    pub fn authorize(self: *@This(), _: control.State, _: Request) !void {
        if (self.denied) return error.TestDenied;
    }
    pub fn nowSeconds(self: *@This(), _: Request) !u64 {
        return self.clock;
    }
    pub fn prepareInvisible(self: *@This(), a: A, state: control.State, _: Request) !?control.Delivery {
        if (!self.invisible) return null;
        const advances = try a.dupe(control.Advance, &.{.{ .namespace = namespace(), .position = .{ .sequence = 11 } }});
        var delivery: control.Delivery = .{
            .sequence = state.latest_delivery + 1,
            .before = state.latest_cursor,
            .cursor = @splat(0),
            .authority = @splat(5),
            .issued_at = self.clock,
            .advances = advances,
        };
        delivery.cursor = delivery.identity(state.id, state.epoch);
        return delivery;
    }
    pub fn prepare(self: *@This(), a: A, state: control.State, _: Request) !?upload.Candidate {
        self.prepares += 1;
        const slot = state.slot.?;
        if (self.initial == null) self.initial = slot.mapped_cursor;
        if (!std.mem.eql(u8, &self.initial.?, &slot.mapped_cursor)) return null;
        var iterator: Iterator = .{ .start = slot.next_lsn };
        var sha = upload.WireHasher.init();
        while (try iterator.next(a, spool.max_message_bytes, .{})) |message| {
            defer a.free(message);
            try sha.add(message);
        }
        const advances = try a.alloc(control.Advance, 1);
        advances[0] = .{ .namespace = namespace(), .position = .{ .sequence = 12 } };
        var candidate: upload.Candidate = .{ .delivery = .{ .sequence = state.latest_delivery + 1, .before = slot.mapped_cursor, .cursor = @splat(0), .authority = @splat(0), .issued_at = 110, .advances = advances }, .projection = @splat(8), .completion = @splat(4), .wire = sha.final(), .messages = 5, .visible_events = 3, .bytes = 104, .xid = 42, .commit_time = -1 };
        for (state.cursors) |receipt| if (std.mem.eql(u8, &receipt.before, &slot.mapped_cursor)) {
            candidate.delivery = receipt;
            return candidate;
        };
        candidate.delivery.authority = candidate.proof(state);
        candidate.delivery.cursor = candidate.delivery.identity(state.id, state.epoch);
        return candidate;
    }
    pub fn verify(_: *@This(), state: control.State, spec: anytype, _: Request) !void {
        // This fixture's historical universe is exactly one fixed relational
        // transaction. Production must verify actual retained completion proof.
        if (spec.bytes != 104 or spec.messages != 5 or !std.mem.eql(u8, &spec.definition, &state.definition.?.identity()) or !std.mem.eql(u8, &spec.completion, &@as(control.Digest, @splat(4)))) return error.TestInvalidHistory;
    }
    pub fn open(self: *@This(), _: A, _: control.State, spec: anytype, _: Request) !Iterator {
        self.opens += 1;
        if (self.renew_on_open) |svc| {
            self.renew_on_open = null;
            try svc.mutate(std.testing.allocator, .{ .slot = .{ .acquire = .{ .actor = @splat(2), .expected = 1, .now = 145, .expires = 195 } } });
            self.clock = 160; // original grant expired; same owner was renewed
        }
        return .{ .start = spec.start_lsn, .bytes = spec.bytes, .corrupt = self.corrupt };
    }
};
pub fn run() !void {
    return runImpl(false);
}
pub fn runStream() !void {
    return runImpl(true);
}
fn runImpl(with_stream: bool) !void {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/upload-data", .{tmp.sub_path});
    defer a.free(path);
    const meta_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/upload-meta", .{tmp.sub_path});
    defer a.free(meta_path);
    var fs = try FsStore.init(a, path);
    defer fs.deinit();
    var meta = try Store.init(a, .{ .root_dir = meta_path });
    defer meta.deinit();
    const id = control.pg_slot.feedId(@splat(1), "upload_slot");
    var svc: fixture.Service = .{ .store = &meta, .id = id };
    var registry: Registry = .{ .fs = &fs };
    var producer: Producer = .{};
    const table: Table = .{ .table_id = 7, .name = "rows", .schema_json = "{}" };
    try meta.applyStandaloneCommand(group, .{ .upsert_table = table });
    try svc.mutate(a, .{ .create = .{ .id = id, .definition = .{ .owner = @splat(1), .authorization = @splat(2), .projection = .rows, .tables = &.{.{ .table_id = 7, .binding = control.tableBinding(table), .policy_revision = 1, .contract = .native_relational }} }, .now = 100, .expires_at = 1000 } });
    try svc.mutate(a, .{ .plan_roots = &.{.{ .namespace = namespace(), .ownership_epoch = 1 }} });
    {
        var state = try svc.load(a);
        defer state.deinit();
        try svc.mutate(a, .{ .enroll = .{ .namespace = namespace(), .ownership_epoch = 1, .grant_sequence = state.value.roots[0].grant_sequence, .consumer_epoch = 1, .start = 10 } });
    }
    try svc.mutate(a, .{ .seal = .{ .cut = @splat(7) } });
    try svc.mutate(a, .{ .slot = .{ .configure = .{ .database = @splat(1), .name = "upload_slot" } } });
    try svc.mutate(a, .{ .slot = .{ .acquire = .{ .actor = @splat(2), .expected = 0, .now = 100, .expires = 150 } } });
    const session: control.pg_slot.Session = .{ .actor = @splat(2), .fence = 1 };
    try svc.mutate(a, .{ .slot = .{ .bind = .{ .session = session, .now = 101, .binding = .{ .publications = &.{"rows_pub"}, .projection = @splat(8) } } } });
    const config: upload.Config = .{ .require_shared = false }; // explicitly local fixture
    try std.testing.expectEqual(.skipped_invisible, (try upload.drive(&svc, &registry, &producer, a, id, session, config, Request{})).status);
    producer.invisible = false;
    {
        var skipped = try svc.load(a);
        defer skipped.deinit();
        try std.testing.expectEqual(@as(u64, 11), skipped.value.roots[0].acknowledged.sequence);
        try std.testing.expectEqual(@as(u64, 1), skipped.value.slot.?.next_lsn);
        try std.testing.expectEqual(@as(usize, 0), skipped.value.slot.?.spool.records.len);
    }
    try std.testing.expectEqual(.staged, (try upload.drive(&svc, &registry, &producer, a, id, session, config, Request{})).status);
    try std.testing.expectError(error.PgSpoolStorageUnsupported, upload.drive(&svc, &registry, &producer, a, id, session, .{}, Request{}));
    svc.lose_ack = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, upload.drive(&svc, &registry, &producer, a, id, session, config, Request{}));
    svc.lose_ack = false;
    const scopes = registry.scopes;
    meta.deinit();
    meta = try Store.init(a, .{ .root_dir = meta_path });
    fs.deinit();
    fs = try FsStore.initExisting(a, path);
    producer.corrupt = true;
    try std.testing.expectError(error.PgSpoolWireMismatch, upload.drive(&svc, &registry, &producer, a, id, session, config, Request{}));
    try std.testing.expectEqual(scopes, registry.scopes);
    meta.deinit();
    meta = try Store.init(a, .{ .root_dir = meta_path });
    fs.deinit();
    fs = try FsStore.initExisting(a, path);
    const prepares = producer.prepares;
    try std.testing.expectEqual(.abandoned, (try upload.drive(&svc, &registry, &producer, a, id, session, config, Request{})).status);
    try std.testing.expectEqual(prepares, producer.prepares);
    producer.corrupt = false;
    try std.testing.expectEqual(.reserved, (try upload.drive(&svc, &registry, &producer, a, id, session, config, Request{})).status);
    svc.lose_ack = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, upload.drive(&svc, &registry, &producer, a, id, session, config, Request{}));
    svc.lose_ack = false;
    try std.testing.expectEqual(@as(usize, 1), producer.opens);
    meta.deinit();
    meta = try Store.init(a, .{ .root_dir = meta_path });
    fs.deinit();
    fs = try FsStore.initExisting(a, path);
    try std.testing.expectEqual(.abandoned, (try upload.drive(&svc, &registry, &producer, a, id, session, config, Request{})).status);
    try std.testing.expectEqual(.reserved, (try upload.drive(&svc, &registry, &producer, a, id, session, config, Request{})).status);
    producer.renew_on_open = &svc;
    svc.lose_ack_at = svc.proposals + 3; // build/renew succeed; publication reply is lost
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, upload.drive(&svc, &registry, &producer, a, id, session, config, Request{}));
    svc.lose_ack_at = null;
    meta.deinit();
    meta = try Store.init(a, .{ .root_dir = meta_path });
    fs.deinit();
    fs = try FsStore.initExisting(a, path);
    try std.testing.expectEqual(.idle, (try upload.drive(&svc, &registry, &producer, a, id, session, config, Request{})).status);
    try std.testing.expectEqual(@as(usize, 2), producer.opens);
    var current = try svc.load(a);
    defer current.deinit();
    const slot = current.value.slot.?;
    try std.testing.expectEqual(@as(u64, 105), slot.next_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot.confirmed_lsn);
    try std.testing.expectEqual(@as(usize, 1), slot.entries.len);
    try std.testing.expectEqual(@as(u64, 3), slot.entries[0].spool_id);
    try std.testing.expectEqual(@as(usize, 3), slot.spool.records.len);
    const record = slot.spool.records[2];
    var store = fs.artifactStore();
    store.namespace_identity = @splat(9);
    var reader_store = try adapter.Adapter.init(store, .{ .domain = record.scope.domain, .attempt = record.scope.attempt }, .read, .{});
    const reader = try spool.Reader.open(a, &reader_store, record.manifest, .{ .feed = id, .epoch = 1, .definition = record.spec.definition, .projection = record.spec.projection, .storage = record.spec.storage });
    const row = (try reader.message(a, &reader_store, 21, 1024)).?;
    defer a.free(row);
    try std.testing.expect(std.mem.indexOf(u8, row, "frozen") != null);
    producer.denied = true;
    try std.testing.expectError(error.TestDenied, upload.drive(&svc, &registry, &producer, a, id, session, config, Request{}));
    producer.denied = false;
    try std.testing.expectError(error.ChangefeedConflict, upload.drive(&svc, &registry, &producer, a, id, .{ .actor = @splat(3), .fence = 1 }, config, Request{}));
    for (0..100) |_| {
        const result = try cleanup.cleanup(&svc, &registry, a, id, .{ .work = 32, .deletions = 8 }, Request{});
        if (result.status == .waiting) break;
    } else return error.TestUnexpectedResult;
    var collected = try svc.load(a);
    defer collected.deinit();
    try std.testing.expectEqual(@as(u64, 2), collected.value.slot.?.spool.collected_through);
    try std.testing.expectEqual(try control.pg_slot.spool_control.reservationBytes(104), collected.value.slot.?.spool.charged_bytes);
    if (with_stream) try @import("pg_spool_stream_test.zig").run(&svc, &registry, producer.clock);
}
