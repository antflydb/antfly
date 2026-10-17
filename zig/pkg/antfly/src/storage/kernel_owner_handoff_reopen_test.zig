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

const std = @import("std");
const abi = @import("kernel_owner_abi");
const client = @import("kernel_owner_client.zig");
const db_mod = @import("antfly_local_sources").storage_db_db;
const staging = @import("antfly_local_sources").storage_db_restore_staging_contract;
const handoff = @import("antfly_local_sources").storage_db_empty_generation_handoff;

test "storage owner Raft schema controls cross the checked archive and recover terminal replay" {
    const a = std.testing.allocator;
    const local = @import("antfly_local_sources");
    const job = local.storage_db_relational_nullability_job;
    const contract = local.storage_db_relational_schema_owner_contract;
    const before =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const after =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","nullable":false}},"required":["n"],"additionalProperties":false}}}}
    ;
    const namespace: local.storage_db_doc_identity_namespace.Namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const Driver = struct {
        fn header(database: *db_mod.DB) !contract.Header {
            var read = try database.core.store.beginReadTxn();
            defer read.abort();
            return (try job.load(&read)).?;
        }
        fn apply(source: *db_mod.DB, target: *client.Owner, command: contract.Command, term: u64, index: u64) !void {
            const bytes = try command.encodeRaft(std.testing.allocator);
            var bytes_live = true;
            defer if (bytes_live) std.testing.allocator.free(bytes);
            const Cancellation = struct {
                fn check(ptr: ?*anyopaque) callconv(.c) u8 {
                    const flag: *bool = @ptrCast(@alignCast(ptr.?));
                    return @intFromBool(flag.*);
                }
            };
            var canceled = false;
            var prepared = try target.prepareRaftSchemaOwnerControl("rows", bytes, term, index, .{ .cancellation_ctx = &canceled, .cancellation_fn = Cancellation.check });
            defer prepared.deinit();
            // Rejected concurrent preparation neither replaces nor leaks the
            // existing ticket. All transport bytes may be released immediately.
            try std.testing.expectError(error.StorageBusy, target.prepareRaftSchemaOwnerControl("rows", bytes, term, index, .{}));
            std.testing.allocator.free(bytes);
            bytes_live = false;
            canceled = true; // the preparation callback must not be retained
            var failure: abi.FailureIdentity = .{};
            try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_apply_prepared_raft_schema_control(null, &.{ .prepared = prepared.handle, .term = term, .index = index }, &failure));
            try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_apply_prepared_raft_schema_control(target.handle, &.{ .prepared = prepared.handle, .term = term, .index = index + 1 }, &failure));
            try std.testing.expectEqual(abi.RaftSchemaOwnerPreparedApplyRequest.operation, failure.operation);
            try std.testing.expectError(error.DeadlineExceeded, prepared.apply(.{ .execution_deadline_ns = 0 }));
            try source.applyRaftSchemaOwnerCommand(command, 72, .{ .term = term, .index = index }, .{});
            try prepared.apply(.{});
            try prepared.apply(.{}); // exact retry: durable marker, not a flag
            const stale = prepared.handle;
            prepared.deinit();
            try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_apply_prepared_raft_schema_control(target.handle, &.{ .prepared = stale, .term = term, .index = index }, &failure));
            abi.antfly_storage_owner_destroy_raft_schema_preparation(stale);
        }
    };
    for ([_]bool{ true, false }) |cancel_only| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
        defer a.free(root);
        const primary_path = try std.fs.path.join(a, &.{ root, "primary" });
        defer a.free(primary_path);
        const peer_path = try std.fs.path.join(a, &.{ root, "peer" });
        defer a.free(peer_path);
        var primary = try db_mod.DB.open(a, primary_path, options);
        defer primary.close();
        {
            var seed = try db_mod.DB.open(a, peer_path, options);
            defer seed.close();
            for ([_]*db_mod.DB{ &primary, &seed }) |database| {
                try database.setSchemaJson(a, before);
                try database.applyOrderedCommittedMutation(.{ .timestamp_ns = 100, .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} }, .{ .term = 1, .index = 1 }, false);
            }
        }
        const prior = try primary.core.store.get(a, local.storage_db_relational_integrity_catalog.key);
        defer a.free(prior);
        var candidate = try job.Candidate.init(a, namespace.table_id, before, after, prior, &.{"n"}, .{});
        defer candidate.deinit();
        const topology = try primary.relationalTopologyIdentity();
        const binding = binding: {
            var read = try primary.core.store.beginReadTxn();
            defer read.abort();
            break :binding try job.prepareBinding(&read, &candidate, @splat(31), @splat(32), .{ .role = .schema_validation, .transition_id = 33, .attempt = 1, .admission_epoch = topology.next_epoch, .owner_group_id = 72, .peer_group_id = 72, .namespace = namespace, .catalog_digest = topology.catalog_digest }, .raft);
        };
        var context: client.Context = .{};
        try context.ensure();
        defer context.deinit();
        var deferred_catalog: u8 = 0;
        const open: abi.OpenRequest = .{ .context = context.handle, .path = .fromSlice(peer_path), .table_name = .fromSlice("rows"), .group_id = 72, .has_identity_namespace = 1, .identity_table_id = namespace.table_id, .identity_shard_id = namespace.shard_id, .identity_range_id = namespace.range_id, .online_source_authority = 1, .schema_json = .fromSlice(before), .indexes_json = .fromSlice("{}"), .owner_catalog_deferred_out = &deferred_catalog };
        var durable_open = open;
        durable_open.catalog_mode = .durable_only;
        durable_open.historical_raft_apply = 1;
        durable_open.schema_json = .{};
        durable_open.indexes_json = .{};
        var peer = try client.Owner.open(open);
        var peer_live = true;
        defer if (peer_live) peer.deinit();
        const begin: contract.Command = .{ .binding = binding, .action = .begin, .payload = candidate.inputs };
        const wire = try begin.encodeRaft(a);
        defer a.free(wire);
        var failure: abi.FailureIdentity = .{};
        try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_apply_raft_schema_control(peer.handle, &.{ .version = abi.abi_version - 1, .command = .{ .ptr = @ptrFromInt(1), .len = 48 } }, &failure));
        try std.testing.expectEqual(abi.FailureBoundary.storage_owner, failure.boundary);
        try std.testing.expectEqual(abi.RaftSchemaOwnerApplyRequest.operation, failure.operation);
        try std.testing.expectEqual(abi.abi_version, failure.boundary_version);
        try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_apply_raft_schema_control(peer.handle, &.{ .table_name = .{ .len = 4 }, .command = .fromSlice(wire), .term = 2, .index = 10 }, &failure));
        try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_apply_raft_schema_control(peer.handle, &.{ .table_name = .fromSlice("rows"), .command = .{ .ptr = @ptrFromInt(1), .len = contract.Command.max_raft_wire_bytes + 1 }, .term = 2, .index = 10 }, &failure));
        try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_apply_raft_schema_control(peer.handle, &.{ .table_name = .fromSlice("rows"), .command = .fromSlice(wire), .term = 2, .index = 10, ._reserved = 1 }, &failure));
        try std.testing.expectError(error.DeadlineExceeded, peer.applyRaftSchemaOwnerControl("rows", wire, 2, 10, .{ .execution_deadline_ns = 0 }));
        wire[wire.len - 1] ^= 1;
        try std.testing.expectError(error.InvalidSchemaOwnerControl, peer.applyRaftSchemaOwnerControl("rows", wire, 2, 10, .{}));
        wire[wire.len - 1] ^= 1;
        var invalid_ticket: ?*anyopaque = @ptrFromInt(1);
        try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_prepare_raft_schema_control(peer.handle, &.{ .version = abi.abi_version - 1, .command = .{ .ptr = @ptrFromInt(1), .len = 48 } }, &invalid_ticket, &failure));
        try std.testing.expect(invalid_ticket == null);
        try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_apply_prepared_raft_schema_control(peer.handle, &.{ .version = abi.abi_version - 1, .prepared = @ptrFromInt(1) }, &failure));
        try std.testing.expectError(error.DeadlineExceeded, peer.prepareRaftSchemaOwnerControl("rows", wire, 2, 10, .{ .execution_deadline_ns = 0 }));
        {
            var discarded = try peer.prepareRaftSchemaOwnerControl("rows", wire, 2, 10, .{});
            defer discarded.deinit();
            const foreign_path = try std.fs.path.join(a, &.{ root, "foreign" });
            defer a.free(foreign_path);
            var foreign_open = open;
            foreign_open.path = .fromSlice(foreign_path);
            foreign_open.owner_catalog_deferred_out = null;
            var foreign_owner = try client.Owner.open(foreign_open);
            defer foreign_owner.deinit();
            try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_apply_prepared_raft_schema_control(foreign_owner.handle, &.{ .prepared = discarded.handle, .term = 2, .index = 10 }, &failure));
            try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_apply_prepared_raft_schema_control(peer.handle, &.{ .prepared = peer.handle, .term = 2, .index = 10 }, &failure));
            // Dropping unpublished work must release both its owner pin and
            // its admission slot without advancing any durable state.
        }
        try Driver.apply(&primary, &peer, begin, 2, 10);
        {
            // A control-lane ticket pins the provider while close drains. Use
            // the registry's actual closing rejection as the synchronization
            // event, not a sleep-based assertion about thread scheduling.
            var pinned = try peer.prepareRaftSchemaOwnerControl("rows", wire, 2, 10, .{});
            errdefer pinned.deinit();
            const Close = struct {
                owner: ?*anyopaque,
                done: std.atomic.Value(bool) = .init(false),
                fn run(self: *@This()) void {
                    abi.antfly_storage_owner_close(self.owner);
                    self.done.store(true, .release);
                }
            };
            var closing: Close = .{ .owner = peer.handle };
            var future = try std.testing.io.concurrent(Close.run, .{&closing});
            peer_live = false;
            defer {
                pinned.deinit();
                future.await(std.testing.io);
            }
            for (0..1000) |_| {
                pinned.apply(.{}) catch |err| {
                    if (err != error.InvalidArgument) return err;
                    break;
                };
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            } else return error.OwnerCloseDidNotStart;
            try std.testing.expect(!closing.done.load(.acquire));
            pinned.deinit();
            future.await(std.testing.io);
            try std.testing.expect(closing.done.load(.acquire));
        }
        peer = try client.Owner.open(open);
        peer_live = true;
        try std.testing.expectError(error.ConflictingOrderedApplyReceipt, peer.applyRaftSchemaOwnerControl("rows", wire, 3, 10, .{}));
        try Driver.apply(&primary, &peer, .{ .binding = binding, .action = .freeze }, 2, 11);
        peer.deinit();
        peer_live = false;
        peer = try client.Owner.open(durable_open);
        peer_live = true;
        try std.testing.expectEqual(@as(u8, 1), deferred_catalog);
        var index: u64 = 12;
        for (0..8) |_| {
            var page = try primary.prepareRaftSchemaOwnerPage(binding, .{ .records = 1, .time_ns = std.time.ns_per_s }, .{});
            defer page.deinit();
            try Driver.apply(&primary, &peer, page.command, 2, index);
            index += 1;
            if ((try Driver.header(&primary)).phase == .ready) break;
        } else return error.SchemaValidationDidNotFinish;
        const ready = try Driver.header(&primary);
        var payload: [64]u8 = @splat(91);
        @memcpy(payload[32..64], &try ready.digest());
        const terminal: contract.Command = .{ .binding = binding, .action = if (cancel_only) .cancel else .install, .payload = if (cancel_only) "" else &payload };
        try Driver.apply(&primary, &peer, terminal, 3, index);
        const completed = try Driver.header(&primary);
        // Old admission is no longer preparable after cancellation. The actual
        // applied watermark, not a synthetic phase receipt, makes replay safe.
        try Driver.apply(&primary, &peer, begin, 2, 10);
        var foreign = begin;
        foreign.binding.fence.namespace.range_id += 1;
        const foreign_wire = try foreign.encodeRaft(a);
        defer a.free(foreign_wire);
        try std.testing.expectError(error.IdentityNamespaceMismatch, peer.applyRaftSchemaOwnerControl("rows", foreign_wire, 2, 10, .{}));
        peer.deinit();
        peer_live = false;
        {
            var restored = try db_mod.DB.open(a, peer_path, options);
            defer restored.close();
            try std.testing.expectEqualDeep(completed, try Driver.header(&restored));
            try std.testing.expectEqual(@as(u64, index), (try restored.orderedApplyReceipt()).?.index);
            const row = (try restored.get(a, "a")).?;
            defer a.free(row);
            try std.testing.expectEqualStrings("{\"n\":1}", row);
        }
        peer = try client.Owner.open(open);
        peer_live = true;
        try std.testing.expectEqual(@as(u8, if (cancel_only) 0 else 1), deferred_catalog);
        const terminal_wire = try terminal.encodeRaft(a);
        defer a.free(terminal_wire);
        try peer.applyRaftSchemaOwnerControl("rows", terminal_wire, 3, index + 1, .{});
        try peer.applyRaftSchemaOwnerControl("rows", wire, 2, 10, .{});
    }
}

test "storage owner durable catalog cold open preserves relational and document state without descriptor hints" {
    const a = std.testing.allocator;
    const local = @import("antfly_local_sources");
    const namespace: local.storage_db_doc_identity_namespace.Namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 };
    const relational =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const document =
        \\{"version":1,"storage_mode":"document","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .online_source_authority = .raft, .start_index_workers = false, .start_optional_runtimes = false };
    for ([_][]const u8{ relational, document }) |schema_json| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
        defer a.free(path);
        var existing_options = options;
        existing_options.require_existing_catalog = true;
        existing_options.open_mode = .writer_no_replay;
        const missing = try std.fs.path.join(a, &.{ path, "missing" });
        defer a.free(missing);
        try std.testing.expectError(error.FileNotFound, db_mod.DB.open(a, missing, existing_options));
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, missing, .{}));
        {
            var seed = try db_mod.DB.open(a, path, options);
            defer seed.close();
            try local.storage_local_write.configureStorageKernelOwnerDb(a, &seed, "rows", schema_json, "{\"old\":{\"type\":\"full_text\"}}", null, null, null, null, null);
            try seed.applyOrderedCommittedMutation(.{ .timestamp_ns = 100, .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} }, .{ .term = 1, .index = 1 }, false);
            try std.testing.expect(seed.hasIndex("old"));
        }
        var context: client.Context = .{};
        try context.ensure();
        defer context.deinit();
        var deferred_catalog: u8 = 0;
        const open: abi.OpenRequest = .{ .context = context.handle, .path = .fromSlice(path), .table_name = .fromSlice("rows"), .group_id = 72, .catalog_mode = .durable_only, .historical_raft_apply = 1, .has_identity_namespace = 1, .identity_table_id = namespace.table_id, .identity_shard_id = namespace.shard_id, .identity_range_id = namespace.range_id, .owner_catalog_deferred_out = &deferred_catalog };
        var bad = open;
        bad.catalog_mode = @fromBackingInt(@intCast(255));
        try std.testing.expectError(error.InvalidArgument, client.Owner.open(bad));
        bad = open;
        bad.schema_json = .{ .ptr = @ptrFromInt(1), .len = 1 };
        try std.testing.expectError(error.InvalidArgument, client.Owner.open(bad));
        bad = open;
        bad.indexes_json = .fromSlice("{}");
        try std.testing.expectError(error.InvalidArgument, client.Owner.open(bad));
        bad = open;
        bad.dense_embedding_storage = .primary_lsm;
        try std.testing.expectError(error.InvalidArgument, client.Owner.open(bad));
        {
            var owner = try client.Owner.open(open);
            defer owner.deinit();
            try std.testing.expectEqual(@as(u8, 1), deferred_catalog);
        }
        var wrong = existing_options;
        wrong.identity_namespace.?.range_id += 1;
        try std.testing.expectError(error.IdentityNamespaceMismatch, db_mod.DB.open(a, path, wrong));
        wrong = existing_options;
        wrong.online_source_authority = .native;
        try std.testing.expectError(error.OnlineSourceScopeChanged, db_mod.DB.open(a, path, wrong));
        var audited = try db_mod.DB.open(a, path, existing_options);
        defer audited.close();
        try std.testing.expect(audited.hasIndex("old"));
        try std.testing.expectEqual(@as(u32, 1), audited.core.schema.?.version);
        try std.testing.expectEqualDeep(@as(?db_mod.OrderedApplyReceipt, .{ .term = 1, .index = 1 }), try audited.orderedApplyReceipt());
        const row = (try audited.get(a, "a")).?;
        defer a.free(row);
        try std.testing.expectEqualStrings("{\"n\":1}", row);
        var read = try audited.core.store.beginReadTxn();
        defer read.abort();
        const source = try local.storage_source_authority.require(&read, .raft, local.storage_db_online_source_contract.namespaceBytes(namespace));
        try std.testing.expectEqual(@as(u64, 0), source.sequence);
        {
            // Cold control must not run ordinary writer import rollback: that
            // could erase rows before the owner has proved this generation.
            const pending_path = try std.fs.path.join(a, &.{ path, "pending" });
            defer a.free(pending_path);
            const marker_key = "\x00\x00__metadata__:portable_import_in_progress_v1";
            const publishing = "antfly-portable-import-publishing-v2\n";
            {
                var pending = try db_mod.DB.open(a, pending_path, options);
                defer pending.close();
                try pending.setSchemaJson(a, schema_json);
                try pending.applyOrderedCommittedMutation(.{ .timestamp_ns = 100, .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} }, .{ .term = 1, .index = 1 }, false);
                try pending.core.store.putBatch(&.{.{ .key = marker_key, .value = publishing }}, &.{});
            }
            try std.testing.expectError(error.IncompletePortableImport, db_mod.DB.open(a, pending_path, existing_options));
            var backend = try local.storage_lsm_backend.BackendHandle.open(a, pending_path, .{ .backend = .{ .read_only = true, .create_if_missing = false } });
            defer backend.close();
            var runtime_store = try backend.backend.runtimeStore(a, .{ .name = "docs" });
            var raw = local.storage_docstore.DocStore.openRuntime(a, runtime_store) catch |err| {
                runtime_store.deinit();
                return err;
            };
            defer raw.close();
            const marker = try raw.get(a, marker_key);
            defer a.free(marker);
            try std.testing.expectEqualStrings(publishing, marker);
            const catalog_bytes = try raw.get(a, local.storage_db_table_catalog.key);
            defer a.free(catalog_bytes);
            try std.testing.expectEqual(@as(u32, 1), (try local.storage_db_table_catalog.Catalog.decode(catalog_bytes)).active_schema_version);
        }
        {
            // A durable hidden marker is a denial even without request-side
            // bootstrap hints. This fixture claims no provisioning authority.
            const hidden_path = try std.fs.path.join(a, &.{ path, "hidden" });
            defer a.free(hidden_path);
            const initial = local.storage_db_relational_initial_child_publication;
            const record: initial.Record = .{ .phase = .hidden, .plan_id = @splat(1), .plan_digest = @splat(2), .namespace = namespace, .schema_version = 1, .row_count = 0, .schema_digest = @splat(3), .public_schema_json_digest = @splat(4), .catalog_digest = @splat(5), .provision_term = 1, .provision_index = 1 };
            const encoded = try record.encode();
            {
                var hidden = try db_mod.DB.open(a, hidden_path, options);
                defer hidden.close();
                try local.storage_db_doc_identity.writeNamespaceToStore(hidden.core.store, namespace);
                try hidden.setSchemaJson(a, schema_json);
                try hidden.core.store.put(initial.key, &encoded);
            }
            var hidden_open = open;
            hidden_open.path = .fromSlice(hidden_path);
            try std.testing.expectError(error.InitialChildNotPublished, client.Owner.open(hidden_open));
        }
    }
}

test "storage owner handoff receipt survives shared-context hidden to public reopen" {
    const alloc = std.testing.allocator;
    const public_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const namespace: @import("antfly_local_sources").storage_db_doc_identity_namespace.Namespace = .{ .table_id = 72, .shard_id = 7201, .range_id = 7201 };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/owner-handoff-reopen", .{tmp.sub_path});
    defer alloc.free(path);
    var parsed_schema = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(alloc, public_schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try @import("antfly_local_sources").schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer @import("antfly_local_sources").storage_schema.freeSchema(alloc, runtime_schema);
    const schema_bytes = try @import("antfly_local_sources").storage_schema.serializeSchema(alloc, runtime_schema);
    defer alloc.free(schema_bytes);
    const scope: staging.Scope = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .source_artifact_digest = @splat(0),
        .source_namespace = .{ .table_id = 71, .shard_id = 7101, .range_id = 7101 },
        .target_namespace = namespace,
        .target_schema_digest = staging.digest(schema_bytes),
        .empty_generation = true,
    };
    const install: handoff.Install = .{
        .scope = scope.digest(),
        .source_summary_digest = @splat(3),
        .retired_digest = @splat(4),
        .retired_count = 0,
        .mappings = &.{},
    };
    const bootstrap: staging.OwnerBootstrap = .{
        .scope = scope,
        .table_name = "docs",
        .schema_json = public_schema_json,
        .indexes_json = "{}",
        .byte_range = .{ .start = "", .end = "" },
        .empty_generation_handoff = .{
            .source_summary_digest = install.source_summary_digest,
            .retired_digest = install.retired_digest,
            .retired_count = install.retired_count,
            .expected_install_receipt_digest = try handoff.installReceiptDigest(install),
        },
    };
    {
        var seeded = try db_mod.DB.open(alloc, path, .{ .identity_namespace = namespace, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
        defer seeded.close();
        try @import("antfly_local_sources").storage_db_doc_identity.writeNamespaceToStore(seeded.core.store, namespace);
        try seeded.setSchemaJson(alloc, public_schema_json);
        try seeded.reserveRestoreStagingScoped(alloc, scope);
        try seeded.installRestoreStagingBootstrap(alloc, bootstrap);
        const imported = try (staging.Progress{ .scope = scope, .phase = .imported, .rows_complete = true, .artifacts_complete = true }).encode(alloc);
        defer alloc.free(imported);
        try seeded.core.store.put(staging.key, imported);
        _ = try seeded.finishRestoreStaging(alloc, scope.digest(), .validated);
        var txn = try seeded.core.store.beginWriteTxn();
        var txn_open = true;
        defer if (txn_open) txn.abort();
        _ = try handoff.stageInstall(alloc, &txn, install, scope.plan_id, 7, 11);
        try txn.commit();
        txn_open = false;
        try seeded.core.store.sync(true);
    }
    const bootstrap_json = try std.json.Stringify.valueAlloc(alloc, bootstrap, .{});
    defer alloc.free(bootstrap_json);
    var context = client.Context{};
    try context.ensure();
    defer context.deinit();
    var options: abi.OpenRequest = .{
        .context = context.handle,
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = namespace.shard_id,
        .has_identity_namespace = 1,
        .identity_table_id = namespace.table_id,
        .identity_shard_id = namespace.shard_id,
        .identity_range_id = namespace.range_id,
        .schema_json = .fromSlice(public_schema_json),
        .indexes_json = .fromSlice("{}"),
        .restore_bootstrap_json = .fromSlice(bootstrap_json),
    };
    const lookup_contract = @import("antfly_local_sources").api_local_query_contract;
    const hidden_request = try lookup_contract.encodeStorageKernelLookupRequest(alloc, "", .{
        .restore_staging_scope = scope.digest(),
        .restore_staging_plan_id = scope.plan_id,
        .relational_topology_json = "{\"mode\":\"generation_handoff_install\"}",
    });
    defer alloc.free(hidden_request);
    const published_request = try lookup_contract.encodeStorageKernelLookupRequest(alloc, "", .{
        .relational_topology_json = "{\"mode\":\"generation_handoff_install\"}",
    });
    defer alloc.free(published_request);
    {
        var hidden = try client.Owner.open(options);
        defer hidden.deinit();
        var result = try hidden.lookupJson("docs", hidden_request);
        defer result.deinit();
        var parsed = try std.json.parseFromSlice(?staging.GenerationAdmissionReceipt, alloc, result.bytes(), .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(u64, 11), (parsed.value orelse return error.GenerationHandoffInstallMissing).applied_index);
    }
    var durable_open = options;
    durable_open.catalog_mode = .durable_only;
    durable_open.historical_raft_apply = 1;
    durable_open.schema_json = .{};
    durable_open.indexes_json = .{};
    durable_open.restore_bootstrap_json = .{};
    try std.testing.expectError(error.RestoreStagingScopeChanged, client.Owner.open(durable_open));
    {
        var promoted = try db_mod.DB.open(alloc, path, .{ .identity_namespace = namespace, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
        defer promoted.close();
        _ = try promoted.finishRestoreStaging(alloc, scope.digest(), .published);
    }
    options.restore_bootstrap_json = .{};
    {
        var durable = try client.Owner.open(durable_open);
        defer durable.deinit();
    }
    // The published catalog descriptor supplies its canonical schema. The
    // CAPI then enables stale-schema rejection and optional runtimes, unlike
    // the private bootstrap open above.
    options.schema_json = .fromSlice(public_schema_json);
    var published = try client.Owner.open(options);
    defer published.deinit();
    var result = try published.lookupJson("docs", published_request);
    defer result.deinit();
    var parsed = try std.json.parseFromSlice(?staging.GenerationAdmissionReceipt, alloc, result.bytes(), .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u64, 11), (parsed.value orelse return error.GenerationHandoffInstallMissing).applied_index);
}
