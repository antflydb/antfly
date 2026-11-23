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
const local = @import("antfly_local_sources");
const fixture = @import("../owner_test_fixture.zig");
const client = @import("../owner_test_client.zig");
const SourceCampaign = @import("sql_document_mutation_lifecycle_integration_test.zig").SourceCampaign;
const wire = @import("antfly_metadata_openapi").types;
const Json = std.json.Value;

fn verifySelinuxChallengeWire() !void {
    const security = @import("../../../../system_catalog/security_labels.zig");
    const approval_type = security.labels.KernelRelabel;
    const key = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(9));
    const challenge: security.SelinuxChallenge = .{
        .revision = 7,
        .prepared = .{
            .intent = .{
                .target = .{ .database = "default", .namespace = "public", .table = "usage_records" },
                .class = .table,
                .provider = "selinux",
                .label = "system_u:object_r:sepgsql_table_t:s0",
                .actor = .{ .session_user = .bootstrap, .credential = "" },
                .native_administrator = true,
            },
            .object = .{ .incarnation = @splat(1), .database_id = 2, .class = .table, .id = 3 },
            .provider_name = "selinux",
            .provider_digest = @splat(2),
        },
        .admission = .{ .request = .{ .nonce = @splat(3), .head = 0 }, .incarnation = @splat(1) },
        .authority = .{ .verifier = key.public_key.toBytes(), .policy = .{ .digest = @splat(2), .sequence = 5 } },
        .previous = null,
    };
    const a = std.testing.allocator;
    const bytes = try std.json.Stringify.valueAlloc(a, challenge, .{});
    defer a.free(bytes);
    const decoded = try std.json.parseFromSlice(security.SelinuxChallenge, a, bytes, .{});
    defer decoded.deinit();
    const catalog_cut = try decoded.value.cut();
    const approval = try approval_type.Approval.sign(.{
        .object = catalog_cut.object,
        .catalog_revision = catalog_cut.revision,
        .command_nonce = catalog_cut.nonce,
        .actor = catalog_cut.actor,
        .subject = @splat(4),
        .policy = decoded.value.authority.policy.digest,
        .policy_sequence = decoded.value.authority.policy.sequence,
        .authority_epoch = decoded.value.authority.epoch,
        .previous = approval_type.labelDigest(catalog_cut.previous),
        .proposed = approval_type.labelDigest(catalog_cut.proposed),
    }, key);
    const command = try decoded.value.finish(approval);
    const encoded_command = try std.json.Stringify.valueAlloc(a, command, .{});
    defer a.free(encoded_command);
    const parsed = try std.json.parseFromSlice(@TypeOf(command), a, encoded_command, .{});
    defer parsed.deinit();
    try security.validateCommand(parsed.value);
    try std.testing.expectEqual(approval, parsed.value.sql_security_label.?.selinux_approval.?);
    var changed = decoded.value;
    changed.prepared.intent.label = null;
    try std.testing.expectError(error.SelinuxAdmissionChanged, changed.finish(approval));
}

fn verifySelinuxAuthorityReplay() !void {
    const raft = @import("../../../../metadata/storage/raft_apply_store.zig");
    const protocol = @import("../../../../metadata/topology_protocol.zig");
    const storage = @import("../../../../system_catalog/storage.zig");
    const a = std.testing.allocator;
    const group = 734;
    const incarnation = "11111111111111111111111111111111".*;
    const Fixture = struct {
        fn read(store: *raft.RaftApplyStore) !storage.SelinuxAuthorityState {
            var txn = try store.store.beginReadTxn();
            defer txn.abort();
            return storage.readSelinuxAuthorityState(a, &txn, group);
        }
        fn apply(store: *raft.RaftApplyStore, update: storage.SelinuxAuthorityUpdate) !void {
            const payload = try std.json.Stringify.valueAlloc(a, update, .{});
            defer a.free(payload);
            const frame = try raft.encodeTransitionCommand(a, .{ .update_sql_selinux_authority = payload });
            defer a.free(frame);
            var decoded = (try raft.decodeTransitionCommand(a, frame)) orelse return error.InvalidMetadataTransitionEncoding;
            defer decoded.deinit(a);
            try store.applyStandaloneCommand(group, decoded);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/selinux-authority", .{tmp.sub_path});
    defer a.free(path);
    const key = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(41));
    const install: storage.SelinuxAuthorityUpdate = .{
        .incarnation = @splat(0x11),
        .expected_epoch = 0,
        .active = .{ .verifier = key.public_key.toBytes(), .policy = .{ .digest = @splat(8), .sequence = 9 } },
    };
    {
        var store = try raft.RaftApplyStore.init(a, .{ .root_dir = path });
        defer store.deinit();
        try store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = incarnation });
        try Fixture.apply(&store, install);
        try std.testing.expectEqual(@as(u64, 0), (try Fixture.read(&store)).epoch);

        const activation = try std.json.Stringify.valueAlloc(a, protocol.Activation{
            .version = protocol.sql_selinux_approval_version,
            .incarnation = incarnation,
            .member_count = 1,
            .membership_fingerprint = @splat(7),
        }, .{});
        defer a.free(activation);
        try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = activation });
        var foreign = install;
        foreign.incarnation[0] ^= 1;
        try Fixture.apply(&store, foreign);
        try std.testing.expectEqual(@as(u64, 0), (try Fixture.read(&store)).epoch);

        try Fixture.apply(&store, install);
        try Fixture.apply(&store, install);
        const active = try Fixture.read(&store);
        try std.testing.expectEqual(@as(u64, 1), active.epoch);
        try std.testing.expectEqual(active.epoch, active.active.?.epoch);
        try std.testing.expectEqual(key.public_key.toBytes(), active.active.?.verifier);
        try Fixture.apply(&store, .{ .incarnation = install.incarnation, .expected_epoch = active.epoch, .active = null });
    }
    var reopened = try raft.RaftApplyStore.init(a, .{ .root_dir = path });
    defer reopened.deinit();
    try Fixture.apply(&reopened, install);
    const withdrawn = try Fixture.read(&reopened);
    try std.testing.expectEqual(@as(u64, 2), withdrawn.epoch);
    try std.testing.expect(withdrawn.active == null);

    const snapshot = try reopened.snapshotBuilder().buildSnapshot(a, group);
    defer a.free(snapshot);
    const replica_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/selinux-replica", .{tmp.sub_path});
    defer a.free(replica_path);
    var replica = try raft.RaftApplyStore.init(a, .{ .root_dir = replica_path });
    defer replica.deinit();
    try std.testing.expect(try replica.snapshotBuilder().installSnapshot(a, group, 10, snapshot));
    try Fixture.apply(&replica, install);
    try std.testing.expectEqualDeep(withdrawn, try Fixture.read(&replica));
}

fn verifyLiveSelinuxPgwire(ctx: *fixture.Context, original: []const u8) !void {
    const builtin = @import("builtin");
    const required = if (comptime builtin.link_libc) std.c.getenv("ANTFLY_TEST_SELINUX_REQUIRED") != null else false;
    if (comptime builtin.os.tag != .linux or !builtin.link_libc) {
        if (required) return error.SelinuxUnavailable;
        return;
    }
    const allowed = std.mem.span(std.c.getenv("ANTFLY_TEST_SELINUX_ALLOW_CONTEXT") orelse {
        if (required) return error.MissingSelinuxTestContext;
        return;
    });
    const denied = std.mem.span(std.c.getenv("ANTFLY_TEST_SELINUX_DENY_CONTEXT") orelse return error.MissingSelinuxTestContext);
    if (std.mem.indexOfAny(u8, allowed, "'\\") != null or std.mem.indexOfAny(u8, denied, "'\\") != null)
        return error.InvalidSelinuxContext;

    const security = @import("../../../../system_catalog/security_labels.zig");
    const metadata = @import("../../../../metadata/service.zig");
    const pgwire = @import("../../../../pgwire/cursor_reference_test_support.zig");
    const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingPublicApi;
    const io = api.sharedApiNetworkIo() orelse return error.PgwireRequiresBackendRuntime;
    const a = ctx.alloc;
    const svc = ctx.metadata.server.svc;
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const group = svc.metadata_group_id;
    const control: local.api_operation.RequestContext = .{ .setting_admin = true };
    var policy = try local.system_catalog_domain.selinux_linux.LinuxPolicy.init(io, a);
    defer policy.deinit();
    const key = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(37));
    const admission = try store.systemCatalogAdmission(a, group, .{ .action = .alter, .kind = .table, .database = "default", .namespace = "public", .name = "usage_records" });
    const installed = try metadata.submitSqlSelinuxAuthority(svc, control, .{
        .incarnation = admission.incarnation,
        .expected_epoch = (try store.sqlSelinuxAuthority(group)).epoch,
        .active = .{ .verifier = key.public_key.toBytes(), .policy = try policy.currentPolicy() },
    });

    var random: [8]u8 = undefined;
    try io.randomSecure(&random);
    var name: [64]u8 = undefined;
    const address = try std.Io.net.UnixAddress.init(try std.fmt.bufPrint(&name, "\x00antfly-selinux-sql-{x}", .{std.mem.readInt(u64, &random, .little)}));
    const listener = try @import("../../transport/pgwire.zig").Listener.startWithSelinux(api, .{ .enabled = true }, .{
        .address = address,
        .policy = &policy,
        .key = &key,
    });
    defer listener.deinit();
    const stream = try address.connect(io);
    defer stream.close(io);
    var input_buffer: [8192]u8 = undefined;
    var output_buffer: [8192]u8 = undefined;
    var input = stream.reader(io, &input_buffer);
    var output = stream.writer(io, &output_buffer);
    try pgwire.startup(a, &output.interface, "pk-rewriter", "pk-rewrite-durable-password", "default");
    try output.interface.flush();

    const Wire = struct {
        fn ready(alloc: std.mem.Allocator, reader: *std.Io.Reader, expected: ?[]const u8) !void {
            var result: bool = false;
            for (0..64) |_| {
                const tag = try reader.takeByte();
                const size = try reader.takeInt(u32, .big);
                if (size < 4 or size > 64 * 1024) return error.InvalidPgwireFrame;
                const body = try alloc.alloc(u8, size - 4);
                defer alloc.free(body);
                try reader.readSliceAll(body);
                switch (tag) {
                    'E' => {
                        const wanted = expected orelse {
                            std.debug.print("SELinux pgwire error: {s}\n", .{body});
                            return error.UnexpectedPgwireError;
                        };
                        var fields: @import("../../../../pgwire/protocol.zig").Cursor = .{ .bytes = body };
                        while (try fields.int(u8) != 0) {
                            const code = body[fields.offset - 1];
                            const field = try fields.string();
                            if (code == 'C') {
                                try std.testing.expectEqualStrings(wanted, field);
                                result = true;
                            }
                        }
                    },
                    'C' => {
                        if (expected != null) return error.UnexpectedPgwireSuccess;
                        try std.testing.expectEqualStrings("SECURITY LABEL\x00", body);
                        result = true;
                    },
                    'Z' => {
                        try std.testing.expect(result);
                        return;
                    },
                    'R' => if (body.len == 4 and std.mem.readInt(u32, body[0..4], .big) == 0) {
                        result = true;
                    },
                    else => {},
                }
            }
            return error.MissingPgwireReady;
        }

        fn query(alloc: std.mem.Allocator, writer: *std.Io.Writer, reader: *std.Io.Reader, sql: []const u8, state: ?[]const u8) !void {
            const body = try alloc.dupeSentinel(u8, sql, 0);
            defer alloc.free(body);
            try pgwire.frame(writer, 'Q', body[0 .. body.len + 1]);
            try writer.flush();
            try ready(alloc, reader, state);
        }
    };
    try Wire.ready(a, &input.interface, null);
    const before = try store.systemCatalogMeta(a, group);
    try Wire.query(a, &output.interface, &input.interface, original, "42602");
    try std.testing.expectEqual(before.revision, (try store.systemCatalogMeta(a, group)).revision);
    const denied_sql = try std.fmt.allocPrint(a, "SECURITY LABEL FOR selinux ON TABLE usage_records IS '{s}'", .{denied});
    defer a.free(denied_sql);
    try Wire.query(a, &output.interface, &input.interface, denied_sql, "42501");
    try std.testing.expectEqual(before.revision, (try store.systemCatalogMeta(a, group)).revision);
    const allowed_sql = try std.fmt.allocPrint(a, "SECURITY LABEL FOR selinux ON TABLE usage_records IS '{s}'", .{allowed});
    defer a.free(allowed_sql);
    try Wire.query(a, &output.interface, &input.interface, allowed_sql, null);
    try std.testing.expectEqual(before.revision + 1, (try store.systemCatalogMeta(a, group)).revision);
    try pgwire.frame(&output.interface, 'X', "");
    try output.interface.flush();
    var persisted = try store.systemCatalogSnapshot(a, group);
    defer persisted.deinit();
    const table = for (persisted.value.resources) |resource| {
        if (resource.kind == .table and std.mem.eql(u8, resource.name, "usage_records")) break resource;
    } else return error.MissingLabelObject;
    const label = try security.labels.labelForPolicy("selinux", installed.active.?.policy.digest, table.sql_security_labels);
    try std.testing.expectEqualStrings(allowed, label orelse return error.MissingLabelObject);
    std.debug.print("live SELinux pgwire: invalid context rejected, policy denial enforced, allowed label persisted\n", .{});
}

test "mounted SQL SECURITY LABEL originals retain real provider policy identity authorization and cold labels" {
    try verifySelinuxChallengeWire();
    try verifySelinuxAuthorityReplay();
    const Campaign = struct {
        pub const native_sessions = true;
        pub const native_auth = true;
        pub const cold_catalog = true;
        inner: SourceCampaign,
        originals: [3][]const u8,
        table_id: u64 = 0,
        procedure_id: u64 = 0,
        receipt: ?[]const u8 = null,
        initial: ?@import("../../../../metadata/table_manager.zig").TableRecord = null,
        pub fn configureAuth(manager: *@import("../../../../usermgr/mod.zig").UserManager) !void {
            const users = @import("../../../../usermgr/mod.zig");
            inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
                var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
                defer permission.deinit(manager.alloc);
                try manager.addPermissionToUser("pk-rewriter", permission);
            }
        }
        fn command(ctx: *fixture.Context, sql: []const u8, expected: ?[]const u8) !void {
            var response = try client.execute(ctx, sql, .write_admission);
            defer response.deinit(ctx.alloc);
            if (expected) |code| {
                if (response.status != (if (std.mem.eql(u8, code, "42501")) @as(u16, 403) else @as(u16, 400))) std.debug.print("SECURITY LABEL expected {s}: {d} {s}\n", .{ code, response.status, response.body });
                try std.testing.expectEqual(if (std.mem.eql(u8, code, "42501")) @as(u16, 403) else @as(u16, 400), response.status);
                var parsed = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
                defer parsed.deinit();
                try std.testing.expectEqualStrings(code, parsed.value.code);
            } else {
                if (response.status != 200) std.debug.print("SECURITY LABEL SQL={s}: {d} {s}\n", .{ sql, response.status, response.body });
                try std.testing.expectEqual(@as(u16, 200), response.status);
                if (std.mem.startsWith(u8, sql, "SECURITY LABEL")) {
                    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
                    defer parsed.deinit();
                    try std.testing.expectEqualStrings("SECURITY LABEL", parsed.value.command_tag);
                    try std.testing.expectEqual(wire.SQLMutationOutcome.committed, parsed.value.mutation_outcome.?);
                    try std.testing.expectEqual(@as(i64, 0), parsed.value.rows_affected);
                    try std.testing.expectEqual(@as(usize, 0), parsed.value.rows.len);
                    try std.testing.expectEqual(@as(usize, 0), parsed.value.columns.len);
                }
            }
        }
        fn observe(ctx: *fixture.Context, kind: local.system_catalog_domain.Kind, name: []const u8, signature: ?[]const u8, expected: ?[]const u8) !u64 {
            const store = ctx.metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
            var state = try store.systemCatalogSnapshot(ctx.alloc, 2188);
            defer state.deinit();
            for (state.value.resources) |resource| {
                if (resource.kind != kind or !std.mem.eql(u8, resource.name, name)) continue;
                if (signature) |wanted| if (resource.signature == null or !std.mem.eql(u8, wanted, resource.signature.?)) continue;
                if (expected) |text| {
                    try std.testing.expectEqual(@as(usize, 1), resource.sql_security_labels.len);
                    const label = resource.sql_security_labels[0];
                    try std.testing.expectEqualStrings("antfly", label.provider);
                    try std.testing.expectEqualStrings(text, label.label);
                    try std.testing.expectEqualSlices(u8, &local.system_catalog_domain.security_labels.native_policy_digest, &label.policy_digest);
                } else try std.testing.expectEqual(@as(usize, 0), resource.sql_security_labels.len);
                return resource.id;
            }
            return error.MissingLabelObject;
        }
        fn tableDefinition(ctx: *fixture.Context, a: std.mem.Allocator) !@import("../../../../metadata/table_manager.zig").TableRecord {
            const source = @import("../../../http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
            const lookup: local.system_catalog_domain.ResolveMany = .{ .relations = &.{.{ .name = "usage_records" }} };
            const bytes = try source.systemCatalog(ctx.alloc, .{}, .{ .resolve_many = lookup });
            defer ctx.alloc.free(bytes);
            var parsed = try std.json.parseFromSlice(local.system_catalog_domain.ResolvedMany, ctx.alloc, bytes, .{});
            defer parsed.deinit();
            try parsed.value.validateRelations(lookup);
            const found = parsed.value.relations[0] orelse return error.MissingLabelObject;
            const store = ctx.metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
            return (try store.getTable(a, 2188, found.owner.table_id)) orelse error.MissingLabelObject;
        }
        fn verify(self: *@This(), ctx: *fixture.Context, cold: bool) !void {
            try std.testing.expectEqual(@as(u64, 2), try observe(ctx, .namespace, "public", null, "internal"));
            const table_id = try observe(ctx, .table, "usage_records", null, "internal");
            const procedure_id = try observe(ctx, .routine, "rotate_usage", "", "public");
            if (self.table_id == 0) self.table_id = table_id else try std.testing.expectEqual(self.table_id, table_id);
            if (self.procedure_id == 0) self.procedure_id = procedure_id else try std.testing.expectEqual(self.procedure_id, procedure_id);
            const current = try tableDefinition(ctx, ctx.alloc);
            defer @import("../../../../metadata/table_manager.zig").freeTable(ctx.alloc, current);
            try std.testing.expect(@import("../../../../metadata/table_manager.zig").tableDefinitionsEqual(self.initial.?, current));
            if (!cold) try self.inner.fingerprints(ctx, .prepared);
            try self.inner.verify(ctx, cold);
        }
        fn verifySignedSelinuxApply(ctx: *fixture.Context) !void {
            const security = @import("../../../../system_catalog/security_labels.zig");
            const mutations = @import("../../../../system_catalog/catalog_mutations.zig");
            const metadata = @import("../../../../metadata/service.zig");
            const ops = @import("../../../../system_catalog/operations.zig");
            const svc = ctx.metadata.server.svc;
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const group = svc.metadata_group_id;
            const a = ctx.alloc;
            const control: local.api_operation.RequestContext = .{ .setting_admin = true };
            const key = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(16));
            const authority: security.labels.KernelRelabel.Authority = .{
                .verifier = key.public_key.toBytes(),
                .policy = .{ .digest = @splat(17), .sequence = 6 },
            };
            const name = "selinux_signed_scope";
            const label = "system_u:object_r:sepgsql_schema_t:s0";
            try command(ctx, "CREATE SCHEMA selinux_signed_scope", null);
            const intent: security.Intent = .{
                .target = .{ .database = "default", .namespace = "public", .table = name },
                .class = .namespace,
                .provider = "selinux",
                .label = label,
                .actor = .{ .session_user = .bootstrap, .credential = "" },
                .native_administrator = true,
            };
            const admission = try store.systemCatalogAdmission(a, group, intent.mutation());
            const installed = try metadata.submitSqlSelinuxAuthority(svc, control, .{
                .incarnation = admission.incarnation,
                .expected_epoch = (try store.sqlSelinuxAuthority(group)).epoch,
                .active = authority,
            });
            const TestSigner = struct {
                fn prepare(service: @TypeOf(svc), alloc: std.mem.Allocator, context: local.api_operation.RequestContext, request: security.Intent, nonce: [16]u8, signer: std.crypto.sign.Ed25519.KeyPair) !@import("../../../../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand {
                    const bytes = try ops.prepareSqlCatalog(service, alloc, context, .{
                        .request = .{ .mutation = request.mutation() },
                        .nonce = nonce,
                        .security_label = request,
                    });
                    const challenge = try std.json.parseFromSliceLeaky(security.SelinuxChallenge, alloc, bytes, .{});
                    const cut = try challenge.cut();
                    const kernel = security.labels.KernelRelabel;
                    return challenge.finish(try kernel.Approval.sign(.{
                        .object = cut.object,
                        .catalog_revision = cut.revision,
                        .command_nonce = cut.nonce,
                        .actor = cut.actor,
                        .subject = @splat(18),
                        .policy = challenge.authority.policy.digest,
                        .policy_sequence = challenge.authority.policy.sequence,
                        .authority_epoch = challenge.authority.epoch,
                        .previous = kernel.labelDigest(cut.previous),
                        .proposed = kernel.labelDigest(cut.proposed),
                    }, signer));
                }
            };
            var signed_arena: std.heap.ArenaAllocator = .init(a);
            defer signed_arena.deinit();
            const scratch = signed_arena.allocator();
            const prepared = try TestSigner.prepare(svc, scratch, control, intent, @splat(19), key);
            const committed = try ops.mutateSqlCatalog(svc, scratch, control, prepared);
            const result = try std.json.parseFromSliceLeaky(?mutations.Outcome, scratch, committed, .{});
            try std.testing.expect(result != null and result.? == .success);
            var catalog_state = try store.systemCatalogSnapshot(a, group);
            defer catalog_state.deinit();
            const scope = for (catalog_state.value.resources) |resource| {
                if (resource.kind == .namespace and std.mem.eql(u8, resource.name, name)) break resource;
            } else return error.MissingLabelObject;
            const stored = try security.labels.labelForPolicy("selinux", authority.policy.digest, scope.sql_security_labels);
            try std.testing.expectEqualStrings(label, stored.?);

            var clear = intent;
            clear.label = null;
            const pre_rotation = try TestSigner.prepare(svc, scratch, control, clear, @splat(20), key);
            var next_authority = authority;
            next_authority.policy = .{ .digest = @splat(21), .sequence = authority.policy.sequence + 1 };
            const rotated = try metadata.submitSqlSelinuxAuthority(svc, control, .{
                .incarnation = admission.incarnation,
                .expected_epoch = installed.epoch,
                .active = next_authority,
            });
            const stale_rotation_result = try ops.mutateSqlCatalog(svc, scratch, control, pre_rotation);
            const stale_rotation = try std.json.parseFromSliceLeaky(?mutations.Outcome, scratch, stale_rotation_result, .{});
            try std.testing.expect(stale_rotation != null and stale_rotation.? == .rejected);
            try std.testing.expectEqual(.CatalogGenerationChanged, stale_rotation.?.rejected);

            // Revalidate the existing label under the new authority, then
            // prove both relabel and clear can publish under the rotated key.
            var replacement = intent;
            replacement.label = "system_u:object_r:rotated_schema_t:s0";
            const relabeled = try TestSigner.prepare(svc, scratch, control, replacement, @splat(22), key);
            const relabel_result = try ops.mutateSqlCatalog(svc, scratch, control, relabeled);
            const relabel_outcome = try std.json.parseFromSliceLeaky(?mutations.Outcome, scratch, relabel_result, .{});
            try std.testing.expect(relabel_outcome != null and relabel_outcome.? == .success);
            const cleared = try TestSigner.prepare(svc, scratch, control, clear, @splat(23), key);
            const clear_result = try ops.mutateSqlCatalog(svc, scratch, control, cleared);
            const clear_outcome = try std.json.parseFromSliceLeaky(?mutations.Outcome, scratch, clear_result, .{});
            try std.testing.expect(clear_outcome != null and clear_outcome.? == .success);

            const stale = try TestSigner.prepare(svc, scratch, control, intent, @splat(24), key);
            _ = try metadata.submitSqlSelinuxAuthority(svc, control, .{
                .incarnation = admission.incarnation,
                .expected_epoch = rotated.epoch,
                .active = null,
            });
            const denied = try ops.mutateSqlCatalog(svc, scratch, control, stale);
            const outcome = try std.json.parseFromSliceLeaky(?mutations.Outcome, scratch, denied, .{});
            try std.testing.expect(outcome != null and outcome.? == .rejected);
            try std.testing.expectEqual(.Forbidden, outcome.?.rejected);
            try command(ctx, "DROP SCHEMA selinux_signed_scope", null);
        }
        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            // Use actual native credentials for creation and public row seeding.
            // SQL INSERT transport is outside this catalog cohort; the existing
            // public batch endpoint supplies real persisted rows without it.
            try self.inner.credentials.activateWithCatalog(ctx, true);
            ctx.headers = ctx.admin_headers;
            try command(ctx, self.inner.entry.tables[0].create, null);
            const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
            var seeded = try fixture.batch(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "usage_records", "{\"inserts\":{\"u1\":{\"id\":\"u1\",\"status\":\"before\"},\"u9\":{\"id\":\"u9\",\"status\":\"untouched\"}},\"sync_level\":\"write\"}");
            defer seeded.deinit(ctx.alloc);
            if (seeded.status != 201) std.debug.print("SECURITY LABEL public seed status={d} body={s}\n", .{ seeded.status, seeded.body });
            try std.testing.expectEqual(@as(u16, 201), seeded.status);
            try self.inner.fingerprints(ctx, .initial);
            self.initial = try tableDefinition(ctx, self.inner.retained.allocator());
            try command(ctx, "CREATE PROCEDURE rotate_usage() LANGUAGE plpgsql AS $$BEGIN NULL; END$$", null);
            try command(ctx, self.originals[0], "22023"); // genuine selinux unavailable, no catalog effect.
            try command(ctx, self.originals[1], null);
            try command(ctx, self.originals[2], null);
            _ = try observe(ctx, .routine, "rotate_usage", "", null);
            try command(ctx, "SECURITY LABEL FOR antfly ON TABLE usage_records IS 'internal'", null);
            try command(ctx, "SECURITY LABEL ON PROCEDURE rotate_usage() IS 'public'", null);
            const original_table = try observe(ctx, .table, "usage_records", null, "internal");
            try command(ctx, "ALTER TABLE usage_records RENAME TO renamed_label_usage", null);
            try std.testing.expectEqual(original_table, try observe(ctx, .table, "renamed_label_usage", null, "internal"));
            try command(ctx, "ALTER TABLE renamed_label_usage RENAME TO usage_records", null);
            const original_procedure = try observe(ctx, .routine, "rotate_usage", "", "public");
            try command(ctx, "COMMENT ON PROCEDURE rotate_usage() IS 'retained alongside label'", null);
            try command(ctx, "CREATE OR REPLACE PROCEDURE rotate_usage() LANGUAGE plpgsql AS $$BEGIN NULL; END$$", null);
            try std.testing.expectEqual(original_procedure, try observe(ctx, .routine, "rotate_usage", "", "public"));
            try command(ctx, "SECURITY LABEL ON TABLE usage_records IS 'arbitrary'", "22023");
            try command(ctx, "SECURITY LABEL FOR selinux ON PROCEDURE public.missing_label_procedure() IS NULL", "22023");
            try command(ctx, "SECURITY LABEL ON PROCEDURE public.missing_label_procedure() IS NULL", "42883");
            var reader = ctx.*;
            reader.headers = ctx.reader_headers;
            for (self.originals[1..]) |sql| try command(&reader, sql, "42501");
            try command(&reader, "SECURITY LABEL ON TABLE usage_records IS 'public'", "42501");
            // Independent namespace lifecycle retains identity through rename
            // and retires every label through DROP and same-name recreation.
            try command(ctx, "CREATE SCHEMA label_scope", null);
            try command(ctx, "SECURITY LABEL ON SCHEMA label_scope IS 'internal'", null);
            const scope_id = try observe(ctx, .namespace, "label_scope", null, "internal");
            try command(ctx, "ALTER SCHEMA label_scope RENAME TO label_scope_moved", null);
            try std.testing.expectEqual(scope_id, try observe(ctx, .namespace, "label_scope_moved", null, "internal"));
            try command(ctx, "DROP SCHEMA label_scope_moved", null);
            try command(ctx, "CREATE SCHEMA label_scope_moved", null);
            try std.testing.expect(scope_id != try observe(ctx, .namespace, "label_scope_moved", null, null));
            try command(ctx, "DROP SCHEMA label_scope_moved", null);
            try verifySignedSelinuxApply(ctx);
            // NULL affects only the selected overloaded procedure identity.
            try command(ctx, "CREATE PROCEDURE rotate_usage(integer) LANGUAGE plpgsql AS $$BEGIN NULL; END$$", null);
            try command(ctx, "SECURITY LABEL ON PROCEDURE rotate_usage(integer) IS 'internal'", null);
            try command(ctx, "SECURITY LABEL ON PROCEDURE public.rotate_usage IS 'public'", "42725");
            _ = try observe(ctx, .routine, "rotate_usage", "int32", "internal");
            try command(ctx, "SECURITY LABEL ON PROCEDURE rotate_usage(integer) IS NULL", null);
            _ = try observe(ctx, .routine, "rotate_usage", "int32", null);
            try command(ctx, "DROP PROCEDURE rotate_usage(integer)", null);
            // Revocation removes relabel authority without changing labels.
            const users = @import("../../../../usermgr/mod.zig");
            const manager = (ctx.metadata.server.owned_public_http_server orelse return error.MissingPublicApi).cfg.user_manager orelse return error.MissingUserManager;
            var permission = try users.Permission.initOwned(ctx.alloc, .namespace, "*", .admin);
            defer permission.deinit(ctx.alloc);
            var key = try manager.createApiKey("pk-rewriter", "security-label-revocation", &.{permission}, &.{}, null);
            defer key.deinit(manager.alloc);
            const authorization = try std.fmt.allocPrint(ctx.alloc, "ApiKey {s}", .{key.encoded});
            defer ctx.alloc.free(authorization);
            const headers = [_]@import("../../../../raft/transport/http_common.zig").RequestHeader{.{ .name = "authorization", .value = authorization }};
            var attenuated = ctx.*;
            attenuated.headers = &headers;
            try command(&attenuated, "SECURITY LABEL ON SCHEMA public IS 'internal'", null);
            try manager.deleteApiKey("pk-rewriter", key.key.key_id);
            var revoked = try client.execute(&attenuated, "SECURITY LABEL ON SCHEMA public IS NULL", .write_admission);
            defer revoked.deinit(ctx.alloc);
            try std.testing.expect(revoked.status == 401 or revoked.status == 403);
            _ = try observe(ctx, .namespace, "public", null, "internal");
            // Capture a real native public recovery receipt for cold observation.
            var response = try client.execute(ctx, "SECURITY LABEL ON SCHEMA public IS 'internal'", .write_admission);
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("SECURITY LABEL", parsed.value.command_tag);
            try std.testing.expect(parsed.value.catalog_receipt != null);
            self.receipt = try self.inner.retained.allocator().dupe(u8, parsed.value.catalog_receipt.?.recovery_token);
            try self.verify(ctx, false);
        }
        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
            try self.verify(ctx, true);
            const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .recovery_token = self.receipt.? }, .{});
            defer ctx.alloc.free(body);
            var recovered = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/operations/status", .POST, body, 5000);
            defer recovered.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), recovered.status);
            var observed = try std.json.parseFromSlice(wire.SQLCatalogOperationReceipt, ctx.alloc, recovered.body, .{});
            defer observed.deinit();
            try std.testing.expectEqual(.committed, observed.value.state);
            var denied = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.reader_headers, ctx.base, "/db/v1/sql/operations/status", .POST, body, 5000);
            defer denied.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 403), denied.status);
            // A cold public relabel changes only the selected point metadata.
            try command(ctx, self.originals[2], null);
            _ = try observe(ctx, .routine, "rotate_usage", "", null);
            _ = try observe(ctx, .table, "usage_records", null, "internal");
            try command(ctx, "DROP PROCEDURE rotate_usage()", null);
            try command(ctx, "CREATE PROCEDURE rotate_usage() LANGUAGE plpgsql AS $$BEGIN NULL; END$$", null);
            try std.testing.expect(self.procedure_id != try observe(ctx, .routine, "rotate_usage", "", null));
            try verifyLiveSelinuxPgwire(ctx, self.originals[0]);
        }
    };
    const a = std.testing.allocator;
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    var retained = std.heap.ArenaAllocator.init(a);
    defer retained.deinit();
    const rows = [_][]const Json{ &.{ .{ .string = "u1" }, .{ .string = "before" } }, &.{ .{ .string = "u9" }, .{ .string = "untouched" } } };
    try fixture.runWithState(Campaign, .{
        .originals = .{ (try corpus.get("sql-0153")).sql, (try corpus.get("sql-0154")).sql, (try corpus.get("sql-0155")).sql },
        .inner = .{ .retained = &retained, .entry = .{ .id = "sql-0153", .profile = "native executable antfly provider", .sqlstate = null, .command_tag = "SECURITY LABEL", .affected = 0, .output = .{ .columns = &.{}, .column_oids = &.{}, .rows = &.{} }, .tables = &.{.{ .name = "usage_records", .create = "CREATE TABLE usage_records(id TEXT,status TEXT)", .seed = "INSERT INTO usage_records(id,status) VALUES ('u1','before'),('u9','untouched')", .read = "SELECT id,status FROM usage_records ORDER BY id", .final = .{ .columns = &.{ "id", "status" }, .column_oids = &.{ 25, 25 }, .rows = &rows } }} } },
    });
}
