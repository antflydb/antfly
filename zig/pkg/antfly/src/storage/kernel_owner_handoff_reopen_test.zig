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
            errdefer std.debug.print("checked schema owner control action={s} term={d} index={d}\n", .{ @tagName(command.action), term, index });
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
            try std.testing.expectError(error.StorageBusy, target.prepareSchemaProposal("rows", command.binding, .page, 1, std.time.ns_per_s, .{}));
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
            const expected = try source.observeRaftSchemaOwner(command.binding, 72, .{});
            const observed = try target.raftSchemaOwnerObservation("rows", command.binding, .{});
            try std.testing.expectEqual(prepared.physical_root_incarnation, observed.physical_root_incarnation);
            try std.testing.expectEqual(@as(u64, 0), prepared.root_generation);
            try std.testing.expectEqualDeep(expected.binding, observed.binding);
            try std.testing.expectEqualDeep(expected.header, observed.header);
            try std.testing.expectEqualDeep(expected.applied, observed.applied);
            try std.testing.expect(observed.physical_root_incarnation != 0);
            const stale = prepared.handle;
            prepared.deinit();
            try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_apply_prepared_raft_schema_control(target.handle, &.{ .prepared = stale, .term = term, .index = index }, &failure));
            abi.antfly_storage_owner_destroy_raft_schema_preparation(stale);
        }
    };
    for ([_]bool{ true, false }) |cancel_only| {
        var checkpoint: []const u8 = "bootstrap";
        errdefer std.debug.print("checked schema owner campaign cancel_only={} checkpoint={s}\n", .{ cancel_only, checkpoint });
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
                try database.applyOrderedCommittedMutation(.{ .timestamp_ns = 100, .writes = &.{ .{ .key = "a", .value = "{\"n\":1}" }, .{ .key = "b", .value = "{\"n\":2}" } } }, .{ .term = 1, .index = 1 }, false);
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
        const prepared_cut = try peer.schemaOwnerPreparation("rows", .{ .id = binding.id, .provisional_plan = binding.plan_digest, .fence = binding.fence, .authority = .raft }, candidate.inputs, .{});
        try std.testing.expectEqualDeep(binding, prepared_cut.binding);
        try std.testing.expectEqualDeep(@as(@FieldType(local.storage_db_receipt_position.Position, "raft"), .{ .term = 1, .index = 1 }), prepared_cut.position.?.raft);
        const begin: contract.Command = .{ .binding = binding, .action = .begin, .payload = candidate.inputs };
        const wire = try begin.encodeRaft(a);
        defer a.free(wire);
        var failure: abi.FailureIdentity = .{};
        {
            const encoded_binding = try std.json.Stringify.valueAlloc(a, binding, .{});
            defer a.free(encoded_binding);
            var response: abi.OwnedBytes = .{};
            try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_raft_schema_observation_json(peer.handle, &.{ .version = abi.abi_version - 1 }, &response, &failure));
            try std.testing.expectEqual(abi.RaftSchemaOwnerObservationRequest.operation, failure.operation);
            try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_raft_schema_observation_json(peer.handle, &.{ .table_name = .{ .len = 4 }, .binding_json = .fromSlice(encoded_binding) }, &response, &failure));
            try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_raft_schema_observation_json(peer.handle, &.{ .table_name = .fromSlice("rows"), .binding_json = .fromSlice(encoded_binding), .control = .{ ._reserved0 = 1 } }, &response, &failure));
            try std.testing.expect(response.ptr == null and response.len == 0);
            try std.testing.expectError(error.DeadlineExceeded, peer.raftSchemaOwnerObservation("rows", binding, .{ .execution_deadline_ns = 0 }));
            var foreign_binding = binding;
            foreign_binding.fence.namespace.range_id += 1;
            try std.testing.expectError(error.IdentityNamespaceMismatch, peer.raftSchemaOwnerObservation("rows", foreign_binding, .{}));
        }
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
        var invalid_ticket: abi.RaftSchemaOwnerPreparationResult = .{ .prepared = @ptrFromInt(1) };
        try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_prepare_raft_schema_control(peer.handle, &.{ .version = abi.abi_version - 1, .command = .{ .ptr = @ptrFromInt(1), .len = 48 } }, &invalid_ticket, &failure));
        try std.testing.expect(invalid_ticket.prepared == null and std.mem.allEqual(u8, &invalid_ticket.physical_root_incarnation_le, 0));
        try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_apply_prepared_raft_schema_control(peer.handle, &.{ .version = abi.abi_version - 1, .prepared = @ptrFromInt(1) }, &failure));
        try std.testing.expectError(error.DeadlineExceeded, peer.prepareRaftSchemaOwnerControl("rows", wire, 2, 10, .{ .execution_deadline_ns = 0 }));
        {
            checkpoint = "discard preparation";
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
        checkpoint = "apply begin";
        try Driver.apply(&primary, &peer, begin, 2, 10);
        {
            var invalid_proposal: abi.SchemaOwnerProposalResult = .{};
            try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_prepare_schema_proposal(peer.handle, &.{ .version = abi.abi_version - 1, .descriptor_json = .{ .ptr = @ptrFromInt(1), .len = 1 } }, &invalid_proposal, &failure));
            try std.testing.expectEqual(abi.SchemaOwnerProposalRequest.operation, failure.operation);
            try std.testing.expect(invalid_proposal.command.ptr == null and invalid_proposal.command.len == 0);
            try std.testing.expectError(error.DeadlineExceeded, peer.prepareSchemaProposal("rows", binding, .page, 1, std.time.ns_per_s, .{ .execution_deadline_ns = 0 }));
            try std.testing.expectError(error.InvalidArgument, peer.prepareSchemaProposal("rows", binding, .begin, 1, std.time.ns_per_s, .{}));
            var foreign = binding;
            foreign.fence.namespace.range_id += 1;
            try std.testing.expectError(error.IdentityNamespaceMismatch, peer.prepareSchemaProposal("rows", foreign, .reconcile_transactions, 1, std.time.ns_per_s, .{}));
            const observed = try peer.raftSchemaOwnerObservation("rows", binding, .{});
            var debt = try peer.prepareSchemaProposal("rows", binding, .reconcile_transactions, 1, std.time.ns_per_s, .{});
            defer debt.deinit();
            try std.testing.expectEqual(@as(u64, 0), debt.root_generation);
            try std.testing.expectEqual(observed.physical_root_incarnation, debt.physical_root_incarnation);
            try std.testing.expectEqualDeep(observed, try peer.raftSchemaOwnerObservation("rows", binding, .{}));
        }
        {
            // A control-lane ticket pins the provider while close drains. Use
            // the registry's actual closing rejection as the synchronization
            // event, not a sleep-based assertion about thread scheduling.
            checkpoint = "pin duplicate begin during close";
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
            const observed = try peer.raftSchemaOwnerObservation("rows", binding, .{});
            var proposal = try peer.prepareSchemaProposal("rows", binding, .page, 1, std.time.ns_per_s, .{});
            defer proposal.deinit();
            const expected = try page.command.encodeRaft(a);
            defer a.free(expected);
            try std.testing.expectEqualSlices(u8, expected, proposal.command.bytes());
            try std.testing.expectEqual(observed.physical_root_incarnation, proposal.physical_root_incarnation);
            try std.testing.expectEqualDeep(observed, try peer.raftSchemaOwnerObservation("rows", binding, .{}));
            try Driver.apply(&primary, &peer, try contract.Command.decodeRaft(proposal.command.bytes()), 2, index);
            index += 1;
            if ((try Driver.header(&primary)).phase == .ready) break;
        } else return error.SchemaValidationDidNotFinish;
        const ready = try Driver.header(&primary);
        {
            checkpoint = "bounded full-image audit through archive";
            const wire_contract = local.storage_db_relational_schema_root_contract;
            const signing = try local.storage_db_root_signing_identity.loadOrCreate(a, std.testing.io, root);
            const audit_root: client.Owner.SchemaAuditRoot = .{ .dir = root, .incarnation = signing.root_incarnation, .public_key = signing.public_key };
            var bad_response: abi.OwnedBytes = .{};
            try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_schema_audit_json(peer.handle, &.{ .version = abi.abi_version - 1, .inputs = .{ .ptr = @ptrFromInt(1), .len = 48 } }, &bad_response, &failure));
            try std.testing.expect(bad_response.ptr == null and bad_response.len == 0);
            try std.testing.expectEqual(abi.SchemaOwnerAuditRequest.operation, failure.operation);
            try std.testing.expectEqual(abi.FailureBoundary.storage_owner, failure.boundary);
            try std.testing.expectError(error.InvalidRelationalRowsBudget, peer.stepSchemaOwnerAudit("rows", binding, candidate.inputs, audit_root, .{ .records = 0 }, .{}));
            try std.testing.expectError(error.DeadlineExceeded, peer.stepSchemaOwnerAudit("rows", binding, candidate.inputs, audit_root, .{}, .{ .execution_deadline_ns = 0 }));
            var foreign_binding = binding;
            foreign_binding.fence.namespace.range_id += 1;
            try std.testing.expectError(error.IdentityNamespaceMismatch, peer.stepSchemaOwnerAudit("rows", foreign_binding, candidate.inputs, audit_root, .{}, .{}));
            const initial = try peer.stepSchemaOwnerAudit("rows", binding, candidate.inputs, audit_root, .{ .records = 1, .time_ns = std.time.ns_per_s }, .{});
            // A tiny first turn cannot pass row coverage off as complete
            // transaction/debt/history coverage. Resume on a NEW owner.
            try std.testing.expect(initial == .pending);
            peer.deinit();
            peer_live = false;
            peer = try client.Owner.open(durable_open);
            peer_live = true;
            var complete: ?wire_contract.Evidence = null;
            for (0..128) |_| {
                const progress = try peer.stepSchemaOwnerAudit("rows", binding, candidate.inputs, audit_root, .{ .records = 1, .time_ns = std.time.ns_per_s }, .{});
                if (progress == .complete) {
                    complete = progress.complete;
                    break;
                }
            }
            const sealed = complete orelse return error.SchemaValidationDidNotFinish;
            const observed = try peer.raftSchemaOwnerObservation("rows", binding, .{});
            try std.testing.expectEqualDeep(ready, sealed.value.rows.header);
            try std.testing.expectEqual(@as(u64, 2), sealed.value.rows.rows);
            try std.testing.expectEqual(observed.physical_root_incarnation, sealed.value.physical_root);
            try std.testing.expect(sealed.value.qualification.directory_head != null);
            try std.testing.expect(sealed.value.qualification.history.records != 0);
            try sealed.verify(signing.public_key);
            const repeated = try peer.stepSchemaOwnerAudit("rows", binding, candidate.inputs, audit_root, .{ .records = 1, .time_ns = std.time.ns_per_s }, .{});
            try std.testing.expectEqualDeep(sealed, repeated.complete);
            var wrong_root = audit_root;
            wrong_root.public_key[0] ^= 1;
            try std.testing.expectError(error.InvalidSchemaValidationJob, peer.stepSchemaOwnerAudit("rows", binding, candidate.inputs, wrong_root, .{}, .{}));
            // Audit and proof-sidecar writes cannot advance the data log or
            // change the schema job, even when a frontend rejects the proof.
            try std.testing.expectEqualDeep(observed, try peer.raftSchemaOwnerObservation("rows", binding, .{}));
        }
        var payload: [64]u8 = @splat(91);
        @memcpy(payload[32..64], &try ready.digest());
        // Retain the canonical source header in the exact checkpoint, not a
        // later live observation after installation/cancellation advances it.
        var captured = try @import("server_db_adapter.zig").captureSnapshot(&primary, 72, ready.updated.raft.index);
        defer captured.deinit();
        try std.testing.expectEqualDeep(ready, captured.identity.schema_header.?);
        const terminal: contract.Command = .{ .binding = binding, .action = if (cancel_only) .cancel else .install, .payload = if (cancel_only) "" else &payload };
        try Driver.apply(&primary, &peer, terminal, 3, index);
        const completed = try Driver.header(&primary);
        {
            var serialized = std.Io.Writer.Allocating.init(a);
            defer serialized.deinit();
            const receipt = try captured.writeWithReceipt(&serialized.writer, .none);
            const saved = try @import("../raft/storage/native_snapshot.zig").identity(serialized.written());
            try std.testing.expectEqualDeep(ready, saved.schema_header.?);
            try std.testing.expectEqualDeep(captured.identity, receipt.identity);
            try std.testing.expectEqualSlices(u8, &receipt.footer_digest, serialized.written()[serialized.written().len - 32 ..]);
            var absent = saved;
            absent.schema_header = null;
            try std.testing.expectError(error.InvalidSnapshot, absent.validate());
            var changed = saved;
            changed.schema_header.?.last_effect[0] ^= 1;
            try std.testing.expectError(error.InvalidSnapshot, changed.validate());
            changed = saved;
            changed.group_id += 1;
            try std.testing.expectError(error.InvalidSnapshot, changed.validate());
        }
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
        if (!cancel_only) {
            checkpoint = "release publication";
            const installed = try Driver.header(&primary);
            var release_payload: [96]u8 = undefined;
            @memcpy(release_payload[0..32], &installed.installation.?.decision);
            @memcpy(release_payload[32..64], &try installed.digest());
            @memset(release_payload[64..96], 95);
            try Driver.apply(&primary, &peer, .{ .binding = binding, .action = .release, .payload = &release_payload }, 4, index + 2);
            const released = try peer.raftSchemaOwnerObservation("rows", binding, .{});
            try std.testing.expectEqual(contract.Phase.released, released.header.?.phase);
            try std.testing.expectEqual(@as(u64, index + 2), released.applied.?.index);
        }
    }
}

test "storage owner native schema controls cross the checked archive without synthetic commit positions" {
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
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .online_source_authority = .native, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    for ([_]bool{ true, false }) |cancel_only| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
        defer a.free(path);
        var initial = try db_mod.DB.open(a, path, options);
        var initial_live = true;
        defer if (initial_live) initial.close();
        try initial.setSchemaJson(a, before);
        try initial.batch(.{ .timestamp_ns = 100, .writes = &.{ .{ .key = "a", .value = "{\"n\":1}" }, .{ .key = "b", .value = "{\"n\":2}" } } });
        const predecessor = try initial.core.store.get(a, local.storage_db_relational_integrity_catalog.key);
        defer a.free(predecessor);
        var candidate = try job.Candidate.init(a, namespace.table_id, before, after, predecessor, &.{"n"}, .{});
        defer candidate.deinit();
        const topology = try initial.relationalTopologyIdentity();
        const binding = binding: {
            var read = try initial.core.store.beginReadTxn();
            defer read.abort();
            break :binding try job.prepareBinding(&read, &candidate, @splat(41), @splat(42), .{ .role = .schema_validation, .transition_id = 43, .attempt = 1, .admission_epoch = topology.next_epoch, .owner_group_id = 72, .peer_group_id = 72, .namespace = namespace, .catalog_digest = topology.catalog_digest }, .native);
        };
        const row_key = try local.storage_internal_keys.relationalRowKeyAlloc(a, "a");
        defer a.free(row_key);
        const row_before = try initial.core.store.get(a, row_key);
        defer a.free(row_before);
        initial.close();
        initial_live = false;
        var context: client.Context = .{};
        try context.ensure();
        defer context.deinit();
        var deferred_catalog: u8 = 0;
        const open: abi.OpenRequest = .{ .context = context.handle, .path = .fromSlice(path), .table_name = .fromSlice("rows"), .group_id = 72, .has_identity_namespace = 1, .identity_table_id = namespace.table_id, .identity_shard_id = namespace.shard_id, .identity_range_id = namespace.range_id, .online_source_authority = 2, .schema_json = .fromSlice(before), .indexes_json = .fromSlice("{}"), .owner_catalog_deferred_out = &deferred_catalog };
        var owner = try client.Owner.open(open);
        var owner_live = true;
        defer if (owner_live) owner.deinit();
        const descriptor: contract.PreparationDescriptor = .{ .id = binding.id, .provisional_plan = binding.plan_digest, .fence = binding.fence, .authority = .native };
        const prepared_cut = try owner.schemaOwnerPreparation("rows", descriptor, candidate.inputs, .{});
        try std.testing.expectEqualDeep(binding, prepared_cut.binding);
        const observed_clock = (try owner.nativeSchemaOwnerObservation("rows", binding, .{})).sequence;
        if (prepared_cut.position) |cut| {
            try std.testing.expectEqual(observed_clock, cut.native.sequence);
        } else try std.testing.expectEqual(@as(u64, 0), observed_clock);
        try std.testing.expect(!prepared_cut.replication_pending);
        try std.testing.expectError(error.DeadlineExceeded, owner.schemaOwnerPreparation("rows", descriptor, candidate.inputs, .{ .execution_deadline_ns = 0 }));
        var wrong_scope = descriptor;
        wrong_scope.fence.namespace.range_id += 1;
        try std.testing.expectError(error.IdentityNamespaceMismatch, owner.schemaOwnerPreparation("rows", wrong_scope, candidate.inputs, .{}));
        const begin: contract.Command = .{ .binding = binding, .action = .begin, .payload = candidate.inputs };
        try std.testing.expectError(error.DeadlineExceeded, owner.nativeSchemaOwnerControl("rows", begin, 1, std.time.ns_per_s, .{ .execution_deadline_ns = 0 }));
        try std.testing.expect((try owner.nativeSchemaOwnerObservation("rows", binding, .{})).header == null);
        var failure: abi.FailureIdentity = .{};
        var response: abi.OwnedBytes = .{};
        try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_native_schema_control_json(owner.handle, &.{ .version = abi.abi_version - 1 }, &response, &failure));
        try std.testing.expectEqual(abi.NativeSchemaOwnerControlRequest.operation, failure.operation);
        try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_schema_preparation_json(owner.handle, &.{ .version = abi.abi_version - 1 }, &response, &failure));
        try std.testing.expectEqual(abi.SchemaOwnerPreparationRequest.operation, failure.operation);
        const admitted = try owner.nativeSchemaOwnerControl("rows", begin, 1, std.time.ns_per_s, .{});
        try std.testing.expectEqual(contract.Phase.fencing, admitted.header.?.phase);
        try std.testing.expect(!admitted.replication_pending and admitted.errorName() == null);
        const retry = try owner.nativeSchemaOwnerControl("rows", begin, 1, std.time.ns_per_s, .{});
        try std.testing.expect(retry.duplicate);
        try std.testing.expectEqualDeep(admitted.position, retry.position);
        _ = try owner.nativeSchemaOwnerControl("rows", .{ .binding = binding, .action = .freeze }, 1, std.time.ns_per_s, .{});
        var validated: contract.Header = undefined;
        for (0..16) |_| {
            const page = try owner.nativeSchemaOwnerControl("rows", .{ .binding = binding, .action = .page }, 1, std.time.ns_per_s, .{});
            const observed = try owner.nativeSchemaOwnerObservation("rows", binding, .{});
            try std.testing.expectEqualDeep(page.header, observed.header);
            if (page.header.?.phase == .ready) {
                validated = page.header.?;
                break;
            }
        } else return error.NativeSchemaValidationDidNotFinish;
        const signing = try local.storage_db_root_signing_identity.loadOrCreate(a, std.testing.io, path);
        var image_qualified = false;
        for (0..64) |iteration| {
            const audited = try owner.stepSchemaOwnerAudit("rows", binding, if (iteration == 0) candidate.inputs else "", .{ .dir = path, .incarnation = signing.root_incarnation, .public_key = signing.public_key }, .{ .records = 1, .time_ns = std.time.ns_per_s }, .{});
            try std.testing.expect(audited != .needs_inputs);
            if (audited == .complete) {
                try audited.complete.verify(signing.public_key);
                try std.testing.expect(audited.complete.value.rows.snapshot_cut == .native);
                try std.testing.expectEqualDeep(namespace, audited.complete.value.rows.snapshot_cut.native.namespace);
                try std.testing.expect(audited.complete.value.qualification.origin.applied == null);
                try std.testing.expect(audited.complete.value.qualification.origin.authority.?.kind == .native);
                image_qualified = true;
                break;
            }
        }
        try std.testing.expect(image_qualified);
        if (cancel_only) {
            const canceled = try owner.nativeSchemaOwnerControl("rows", .{ .binding = binding, .action = .cancel }, 1, std.time.ns_per_s, .{});
            try std.testing.expectEqual(contract.Phase.canceled, canceled.header.?.phase);
        } else {
            var install_body: [64]u8 = undefined;
            @memset(install_body[0..32], 91);
            @memcpy(install_body[32..64], &try validated.digest());
            const installed = try owner.nativeSchemaOwnerControl("rows", .{ .binding = binding, .action = .install, .payload = &install_body }, 1, std.time.ns_per_s, .{});
            try std.testing.expectEqual(contract.Phase.installed, installed.header.?.phase);
            owner.deinit();
            owner_live = false;
            owner = try client.Owner.open(open);
            owner_live = true;
            try std.testing.expectEqual(@as(u8, 1), deferred_catalog);
            var release_body: [96]u8 = undefined;
            @memcpy(release_body[0..32], &installed.header.?.installation.?.decision);
            @memcpy(release_body[32..64], &try installed.header.?.digest());
            @memset(release_body[64..96], 95);
            const release: contract.Command = .{ .binding = binding, .action = .release, .payload = &release_body };
            const released = try owner.nativeSchemaOwnerControl("rows", release, 1, std.time.ns_per_s, .{});
            try std.testing.expectEqual(contract.Phase.released, released.header.?.phase);
            const repeated = try owner.nativeSchemaOwnerControl("rows", release, 1, std.time.ns_per_s, .{});
            try std.testing.expect(repeated.duplicate);
            try std.testing.expectEqualDeep(released.position, repeated.position);
        }
        owner.deinit();
        owner_live = false;
        var reopened = try db_mod.DB.open(a, path, options);
        defer reopened.close();
        try std.testing.expect(try reopened.orderedApplyReceipt() == null);
        const preserved = try reopened.core.store.get(a, row_key);
        defer a.free(preserved);
        try std.testing.expectEqualSlices(u8, row_before, preserved);
        try reopened.batch(.{ .timestamp_ns = 200, .writes = &.{.{ .key = "c", .value = "{\"n\":3}" }} });
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
