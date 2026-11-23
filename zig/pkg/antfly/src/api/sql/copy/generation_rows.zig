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

//! Manifested postimages and historical tuple births from an immutable SQL cut.
const std = @import("std");
const local = @import("antfly_local_sources");
const contract = local.storage_db_relational_population_contract;
const keys = contract.frozen_key;
const sessions = @import("../../transactions.zig");
const generations = @import("../../../metadata/sql/relation_lifecycle/generation_intents.zig");
const placement = @import("../../../metadata/table_manager.zig");
const A = std.mem.Allocator;
const Write = local.storage_db_types.BatchWrite;

pub const Stream = struct {
    arena: std.heap.ArenaAllocator,
    manifest: contract.TransactionRows,
    definition: [32]u8,
    rows: []const Write,
    offset: usize = 0,
    page: [contract.max_page_rows]contract.Row = undefined,

    /// Session row bodies remain borrowed for the lifetime of the stream.
    pub fn init(backing: A, session: *const sessions.Session, entry: generations.Entry, image: contract.Image) !Stream {
        if (!session.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        const metadata = session.sql orelse return error.SqlTransactionNotActive;
        const current = metadata.catalog_generations.latest(entry.target) orelse return error.CatalogGenerationChanged;
        if (!current.sameDeclaration(entry) or current.subtransaction != entry.subtransaction or !std.mem.eql(u8, &session.txn_id, &entry.transaction)) return error.CatalogGenerationChanged;
        var scratch = std.heap.ArenaAllocator.init(backing);
        defer scratch.deinit();
        const prepared = try current.prepared(scratch.allocator());
        return initPrepared(backing, session, current, prepared, image);
    }

    fn initPrepared(backing: A, session: *const sessions.Session, current: generations.Entry, prepared: @import("../../../metadata/relation_create.zig").Prepared, image: contract.Image) !Stream {
        var arena = std.heap.ArenaAllocator.init(backing);
        errdefer arena.deinit();
        const a = arena.allocator();
        const writes = switch (image) {
            .current => try currentRows(session.staged, prepared.child.name),
            .frozen => try frozenRows(a, session.staged, current, prepared.child.schema_json),
        };
        const built = try build(a, prepared, session.txn_id, current.nonce, writes, image);
        return .{ .arena = arena, .manifest = built.manifest, .definition = built.definition, .rows = built.rows };
    }

    fn currentRows(staged: ?sessions.OwnedTransactionCommitRequest, physical: []const u8) ![]const Write {
        const request = staged orelse return &.{};
        var found: ?[]const Write = null;
        for (request.tables) |table| {
            if (!std.mem.eql(u8, request.physicalName(table.table_name), physical)) continue;
            if (found != null or table.batch.transforms.len != 0 or table.batch.graph_writes.len != 0 or table.batch.graph_deletes.len != 0) return error.InvalidTransactionSessionRecord;
            found = table.batch.writes;
        }
        return found orelse &.{};
    }

    fn frozenRows(a: A, staged: ?sessions.OwnedTransactionCommitRequest, entry: generations.Entry, schema_json: []const u8) ![]const Write {
        const request = staged orelse return &.{};
        const row_group = for (request.frozen_births.groups.items) |*group| {
            if (std.mem.eql(u8, &group.origin.physical, &entry.physical)) break group;
        } else return &.{};
        const schema = try std.json.parseFromSliceLeaky(struct { version: u32 = 0 }, a, schema_json, .{ .ignore_unknown_fields = true });
        if (row_group.origin.schema_version != schema.version or !std.mem.eql(u8, &row_group.origin.nonce, &entry.nonce) or !std.mem.eql(u8, &row_group.origin.transaction, &entry.transaction)) return error.CatalogGenerationChanged;
        const rows = row_group.rows.items;
        const writes = try a.alloc(Write, rows.len);
        for (rows, writes, 0..) |row, *write, ordinal| write.* = .{
            .key = try keys.encodeBirth(a, row.key, @intCast(ordinal), if (row.superseded) .superseded else .surviving),
            .value = row.document,
            .json_null_fields = row.json_null_fields,
        };
        return writes;
    }

    const Built = struct { manifest: contract.TransactionRows, definition: [32]u8, rows: []const Write };
    fn build(a: A, prepared: @import("../../../metadata/relation_create.zig").Prepared, transaction: [16]u8, nonce: [16]u8, writes: []const Write, image: contract.Image) !Built {
        if (writes.len > 4096) return error.TransactionTooLarge;
        const sorted = try a.dupe(Write, writes);
        std.mem.sort(Write, sorted, {}, struct {
            fn less(_: void, lhs: Write, rhs: Write) bool {
                return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            }
        }.less);
        for (sorted) |*write| if (write.json_null_fields.len > 1) {
            const names = try a.dupe([]const u8, write.json_null_fields);
            std.mem.sort([]const u8, names, {}, struct {
                fn less(_: void, lhs: []const u8, rhs: []const u8) bool {
                    return std.mem.order(u8, lhs, rhs) == .lt;
                }
            }.less);
            write.json_null_fields = names;
        };
        const parts = try a.alloc(contract.RowManifest, prepared.child_ranges.len);
        for (prepared.child_ranges, parts) |range, *part| part.* = .{ .partition = placement.rangeDocIdentityShardId(range), .rows = 0, .content = contract.contentStart() };
        std.mem.sort(contract.RowManifest, parts, {}, struct {
            fn less(_: void, lhs: contract.RowManifest, rhs: contract.RowManifest) bool {
                return lhs.partition < rhs.partition;
            }
        }.less);
        var slots: std.AutoHashMapUnmanaged(u64, usize) = .empty;
        for (parts, 0..) |part, i| {
            if ((try slots.getOrPut(a, part.partition)).found_existing) return error.InvalidTransactionSessionRecord;
            slots.getPtr(part.partition).?.* = i;
        }
        const ranges = try a.dupe(local.common_topology_records.RangeRecord, prepared.child_ranges);
        placement.sortKeyspaceRanges(local.common_topology_records.RangeRecord, ranges);
        // Translate boundaries once. The historical owner routes and stores
        // physical tuple keys, so all births of a logical key stay together.
        if (image == .frozen) for (ranges) |*range| {
            const physical = try keys.physicalRange(a, .{
                .start = range.start_key,
                .end = range.end_key orelse "",
            });
            range.start_key = physical.start;
            range.end_key = if (physical.end.len != 0) physical.end else null;
        };
        var range_index: usize = 0;
        for (sorted, 0..) |write, i| {
            if (i != 0 and std.mem.eql(u8, sorted[i - 1].key, write.key)) return error.InvalidTransactionSessionRecord;
            while (ranges[range_index].end_key) |end| {
                if (std.mem.order(u8, write.key, end) == .lt) break;
                range_index += 1;
                if (range_index == ranges.len) return error.InvalidTransactionSessionRecord;
            }
            const row: contract.Row = .{ .ordinal = @intCast(i), .key = write.key, .document = write.value, .json_null_fields = write.json_null_fields };
            const part = &parts[slots.get(placement.rangeDocIdentityShardId(ranges[range_index])).?];
            part.content = try contract.contentNext(part.content, row);
            part.rows += 1;
        }
        const manifest: contract.TransactionRows = .{ .image = image, .transaction = transaction, .generation_nonce = nonce, .source_rows = @intCast(sorted.len), .partitions = parts };
        return .{ .manifest = manifest, .definition = try manifest.digest(), .rows = sorted };
    }

    pub fn deinit(self: *Stream) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn next(self: *Stream, limit: usize) !?[]const contract.Row {
        if (limit == 0) return error.InvalidRelationPopulation;
        if (self.offset == self.rows.len) return null;
        const capacity = @min(self.rows.len - self.offset, limit, contract.max_page_rows);
        var bytes: usize = 0;
        var count: usize = 0;
        for (self.rows[self.offset..][0..capacity], self.offset..) |write, ordinal| {
            var size = write.key.len +| write.value.len;
            for (write.json_null_fields) |name| size +|= name.len;
            if (size > contract.max_page_bytes - bytes) break;
            bytes += size;
            self.page[count] = .{ .ordinal = @intCast(ordinal), .key = write.key, .document = write.value, .json_null_fields = write.json_null_fields };
            count += 1;
        }
        if (count == 0) return error.RelationPopulationPageTooLarge;
        self.offset += count;
        return self.page[0..count];
    }
};

const staging = @import("../../../metadata/restore_staging.zig");
const registry = @import("../../../system_catalog/prepared_transactions.zig");

/// Resolve both owners from the durable plan. A lost reply can repeat either
/// cancel without reloading the transaction or its staged row bodies.
pub fn cancelOwner(a: A, job: staging.Job, target: staging.Target, range_index: usize, port: @import("../../restore_owner.zig").Port, context: local.api_operation.RequestContext) !staging.OwnerReceipt {
    if (job.state != .canceling or job.plan.catalog_binding == null or range_index >= target.ranges.len)
        return error.InvalidRestoreStaging;
    const range = target.ranges[range_index];
    const scope = try staging.ownerScope(a, job.plan, job.plan_digest, target, range);
    var historical_receipt: ?staging.Digest = null;
    if (target.frozen_population != null) {
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        const scratch = region.allocator();
        const archive = try staging.frozenOwnerBootstrap(scratch, job.plan, job.plan_digest, target, range_index);
        const finished = try port.execute(scratch, target.table.name, range.group_id, .{ .scope = archive.scope, .action = .cancel, .native_execution = target.table.sql_persistence }, context);
        if (finished.phase != .canceled) return error.CatalogPublicationProofPending;
        historical_receipt = finished.receipt;
    }
    const live = try port.execute(a, target.table.name, range.group_id, .{ .scope = scope, .action = .cancel, .native_execution = target.table.sql_persistence }, context);
    if (live.phase != .canceled) return error.CatalogPublicationProofPending;
    const completion = if (historical_receipt) |historical| combined: {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-catalog-canceled-images-v1");
        hash.update(&scope.digest());
        hash.update(&live.receipt);
        hash.update(&historical);
        var digest: staging.Digest = undefined;
        hash.final(&digest);
        break :combined digest;
    } else live.receipt;
    return .{ .group_id = range.group_id, .range_id = scope.target_namespace.range_id, .plan_digest = job.plan_digest, .completion_digest = completion };
}

/// The sealed plan owns its descriptors and manifests. Row bodies borrow the
/// same immutable SQL session cut used to derive the decision identity.
pub const Cohort = struct {
    arena: std.heap.ArenaAllocator,
    plan: staging.Plan,
    participant: registry.CatalogParticipant,
    sources: []Sources,

    const Sources = struct {
        current: Stream,
        frozen: ?Stream = null,
        fn deinit(self: *Sources) void {
            self.current.deinit();
            if (self.frozen) |*stream| stream.deinit();
        }
    };

    pub fn init(backing: A, session: *const sessions.Session, id: staging.Id, binding: registry.CatalogBinding, capture: contract.Capture) !Cohort {
        return initWithPredecessors(backing, session, id, binding, capture, null);
    }

    pub fn initWithPredecessors(backing: A, session: *const sessions.Session, id: staging.Id, binding: registry.CatalogBinding, capture: contract.Capture, predecessors: ?@import("../../generation_handoff_preview.zig").Source) !Cohort {
        try binding.validate(binding.fence.group);
        if (!session.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        const metadata = session.sql orelse return error.SqlTransactionNotActive;
        const pending = metadata.catalog_generations;
        if (pending.entries.items.len == 0 or
            !std.mem.eql(u8, &binding.transaction, &session.txn_id) or !std.mem.eql(u8, &binding.intent_digest, &pending.digest()))
            return error.InvalidCatalogMutation;
        var arena: std.heap.ArenaAllocator = .init(backing);
        errdefer arena.deinit();
        const a = arena.allocator();
        const sources = try a.alloc(Sources, pending.entries.items.len);
        const targets = try a.alloc(staging.Target, pending.entries.items.len);
        var count: usize = 0;
        errdefer for (sources[0..count]) |*source| source.deinit();
        for (pending.entries.items, 0..) |origin, ordinal| {
            const already_seen = for (pending.entries.items[0..ordinal]) |prior| {
                if (prior.sameTarget(origin.target)) break true;
            } else false;
            if (already_seen) continue;
            const entry = pending.latest(origin.target).?;
            if (!entry.birth.backend.eql(binding.fence.backend) or entry.birth.group != binding.fence.group or
                entry.birth.database != binding.fence.database or entry.birth.standalone != binding.fence.standalone or
                !std.mem.eql(u8, &entry.birth.principal, &binding.fence.principal)) return error.CatalogGenerationChanged;
            const prepared = try entry.prepared(a);
            const original = if (origin.sameDeclaration(entry)) prepared else try origin.prepared(a);
            if (prepared.partition_birth != null or prepared.copy_effects != null or prepared.counter_effects != null)
                return error.UnsupportedSqlExecution;
            sources[count] = .{ .current = try Stream.initPrepared(backing, session, entry, prepared, .current) };
            const source = &sources[count];
            const target = &targets[count];
            count += 1;
            target.* = try targetFor(a, entry, prepared, original, capture, &source.current);
            if (target.replace) |*before| {
                const source_reader = predecessors orelse return error.SqlRelationRecoveryIncomplete;
                if (before.ranges.len != target.ranges.len) return error.CatalogGenerationChanged;
                const fences = try a.alloc(local.storage_db_relational_integrity_topology_contract.Fence, before.ranges.len);
                const handoffs = try a.alloc(staging.GenerationHandoffRange, before.ranges.len);
                for (before.ranges, target.ranges, fences, handoffs) |old, next, *fence, *handoff| {
                    if (!std.mem.eql(u8, old.start_key, next.start_key) or !std.mem.eql(u8, old.end_key orelse "", next.end_key orelse ""))
                        return error.CatalogGenerationChanged;
                    const preview = try source_reader.preview(a, id, before.table.name, old, next.group_id);
                    fence.* = preview.fence;
                    handoff.* = preview.handoff;
                }
                before.fences = fences;
                target.generation_handoffs = handoffs;
            }
            if (session.staged) |request| {
                for (request.frozen_births.groups.items) |births| {
                    if (!std.mem.eql(u8, &births.origin.physical, &entry.physical)) continue;
                    source.frozen = try Stream.initPrepared(backing, session, entry, prepared, .frozen);
                    const schema = try @import("../../../metadata/sql/relation_lifecycle/frozen_schema.zig").derive(a, prepared.child.schema_json);
                    target.frozen_population = .{ .schema_json = schema, .capture = try captureFor(a, capture, &source.frozen.?, schema) };
                    break;
                }
            }
        }
        try pending.requireTargets(a, targets[0..count]);
        const cohort_digest = try @import("../../../metadata/relation_create.zig").fingerprint(a, targets[0..count]);
        const plan: staging.Plan = .{
            .id = id,
            .expected_incarnation = std.fmt.bytesToHex(binding.fence.backend.incarnation, .lower),
            .cohort_digest = cohort_digest,
            .targets = targets[0..count],
            .catalog_binding = binding,
        };
        const participant: registry.CatalogParticipant = .{ .staging_id = id, .plan_digest = try plan.digest(a), .intent_digest = binding.intent_digest };
        try participant.validate();
        return .{ .arena = arena, .plan = plan, .participant = participant, .sources = sources[0..count] };
    }

    /// Rebuild only the row streams. Owner handoffs and capture identities
    /// must come from the retained plan, even if the live catalog has moved.
    /// Both the plan and session must outlive the returned cohort.
    pub fn rejoin(backing: A, session: *const sessions.Session, plan: staging.Plan, participant: registry.CatalogParticipant) !Cohort {
        if (!session.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        const metadata = session.sql orelse return error.SqlTransactionNotActive;
        const pending = &metadata.catalog_generations;
        const binding = plan.catalog_binding orelse return error.InvalidCatalogMutation;
        try participant.validate();
        if (!std.mem.eql(u8, &participant.staging_id, &plan.id) or
            !std.mem.eql(u8, &participant.plan_digest, &try plan.digest(backing)) or
            !std.mem.eql(u8, &participant.intent_digest, &binding.intent_digest) or
            !std.mem.eql(u8, &binding.transaction, &session.txn_id))
            return error.ExternalTransactionDecisionChanged;
        var arena: std.heap.ArenaAllocator = .init(backing);
        errdefer arena.deinit();
        const a = arena.allocator();
        try plan.validateCatalog(a, binding.fence.group, pending);
        _ = arena.reset(.retain_capacity);
        const sources = try a.alloc(Sources, plan.targets.len);
        var initialized: usize = 0;
        errdefer for (sources[0..initialized]) |*source| source.deinit();
        for (plan.targets, sources) |target, *source| {
            const entry = for (pending.entries.items) |candidate| {
                if (candidate.generation == target.table.table_id) break candidate;
            } else return error.InvalidCatalogMutation;
            const prepared = try entry.prepared(a);
            source.* = .{ .current = try Stream.initPrepared(backing, session, entry, prepared, .current) };
            initialized += 1;
            try requireCapture(a, target.population.?, &source.current, target.table.schema_json);
            const has_frozen_rows = if (session.staged) |request| archive: {
                for (request.frozen_births.groups.items) |births| {
                    if (std.mem.eql(u8, &births.origin.physical, &entry.physical)) break :archive true;
                }
                break :archive false;
            } else false;
            if (has_frozen_rows != (target.frozen_population != null)) return error.RelationPopulationChanged;
            if (target.frozen_population) |archive| {
                source.frozen = try Stream.initPrepared(backing, session, entry, prepared, .frozen);
                try requireCapture(a, archive.capture, &source.frozen.?, archive.schema_json);
            }
        }
        return .{ .arena = arena, .plan = plan, .participant = participant, .sources = sources };
    }

    fn requireCapture(a: A, retained: contract.Capture, stream: *const Stream, schema: []const u8) !void {
        const rebuilt = try captureFor(a, retained, stream, schema);
        const fingerprints = @import("../../../metadata/relation_create.zig");
        if (!std.mem.eql(u8, &try fingerprints.fingerprint(a, rebuilt), &try fingerprints.fingerprint(a, retained)))
            return error.RelationPopulationChanged;
    }

    fn targetFor(a: A, entry: generations.Entry, prepared: @import("../../../metadata/relation_create.zig").Prepared, original: @import("../../../metadata/relation_create.zig").Prepared, capture: contract.Capture, stream: *const Stream) !staging.Target {
        var target: staging.Target = .{
            .source_table_id = 0,
            .population = try captureFor(a, capture, stream, prepared.child.schema_json),
            .table = prepared.child,
            .ranges = prepared.child_ranges,
            .creation_guard = original.creation_guard,
            .catalog_binding = .{
                .kind = .table,
                .id = entry.generation,
                .sql_relation_id = original.catalog_id,
                .parent_id = original.namespace_id,
                .tablespace_id = original.tablespace_id,
                .name = try a.dupe(u8, entry.target.table),
                .storage_name = prepared.child.name,
                .sql_owner = original.sql_owner,
            },
        };
        if (original.previous) |before| {
            var resource = try before.resource.withPhysicalRelationId(prepared.child.table_id);
            resource.storage_name = prepared.child.name;
            target.catalog_binding = resource;
            target.source_table_id = before.table.table_id;
            target.replace = .{ .table = before.table, .ranges = before.ranges };
        }
        return target;
    }

    fn captureFor(a: A, template: contract.Capture, stream: *const Stream, declaration: []const u8) !contract.Capture {
        var capture = template;
        capture.principal_name = try a.dupe(u8, template.principal_name);
        capture.transaction_rows = stream.manifest;
        capture.capture_id = stream.manifest.generation_nonce;
        capture.definition = stream.definition;
        capture.source_definition = stream.definition;
        capture.target_schema = try staging.runtimeSchemaDigestForTable(a, declaration);
        if (stream.manifest.image == .frozen) {
            const schema = @import("../../../metadata/sql/relation_lifecycle/frozen_schema.zig");
            capture.source_definition = schema.sourceDefinition(stream.definition, declaration);
            capture.capture_id = capture.source_definition[0..16].*;
        }
        try capture.validate();
        return capture;
    }

    /// Deliver one immutable image through the normal resumable owner upload.
    /// Each retry starts at ordinal zero so the worker can verify any prefix
    /// already retained by the destination before appending rows.
    pub fn deliverImage(self: *Cohort, a: A, target_index: usize, image: contract.Image, port: @import("../../restore_owner.zig").Port, context: local.api_operation.RequestContext, options: @import("../execution/population_delivery.zig").Options) ![]contract.Cursor {
        if (target_index >= self.sources.len) return error.InvalidRelationPopulation;
        const target = self.plan.targets[target_index];
        const sources = &self.sources[target_index];
        const stream = switch (image) {
            .current => &sources.current,
            .frozen => if (sources.frozen) |*frozen| frozen else return error.InvalidRelationPopulation,
        };
        const capture = switch (image) {
            .current => target.population.?,
            .frozen => target.frozen_population.?.capture,
        };
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const delivery = @import("../execution/population_delivery.zig");
        const destinations = try scratch.alloc(delivery.Destination, target.ranges.len);
        for (destinations, target.ranges, 0..) |*destination, range, index| {
            const bootstrap = if (image == .frozen)
                try staging.frozenOwnerBootstrap(scratch, self.plan, self.participant.plan_digest, target, index)
            else
                try staging.ownerBootstrapForRangeIndex(scratch, self.plan, self.participant.plan_digest, target, index);
            destination.* = .{
                .port = port,
                .table = target.table.name,
                .group = range.group_id,
                .native_execution = target.table.sql_persistence,
                .scope = bootstrap.scope,
                .range = bootstrap.byte_range,
            };
        }
        stream.offset = 0;
        return delivery.deliverCohort(a, destinations, .{
            .schema_json = if (image == .frozen) target.frozen_population.?.schema_json else target.table.schema_json,
            .definition = capture.definition,
            .source_definition = capture.source_definition,
            .ptr = stream,
            .next = nextPopulation,
            .finish = finishPopulation,
        }, context, options);
    }

    /// Upload both bound images before emitting a metadata import receipt.
    /// A resumed worker asks each archive owner to reopen the same seal.
    pub fn deliverTarget(self: *Cohort, a: A, target_index: usize, port: @import("../../restore_owner.zig").Port, context: local.api_operation.RequestContext, options: @import("../execution/population_delivery.zig").Options) ![]staging.OwnerReceipt {
        const live = try self.deliverImage(a, target_index, .current, port, context, options);
        defer a.free(live);
        const target = self.plan.targets[target_index];
        const historical = if (target.frozen_population != null)
            try self.deliverImage(a, target_index, .frozen, port, context, options)
        else
            null;
        defer if (historical) |cursors| a.free(cursors);

        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const receipts = try a.alloc(staging.OwnerReceipt, target.ranges.len);
        errdefer a.free(receipts);
        for (target.ranges, live, receipts, 0..) |range, cursor, *receipt, index| {
            _ = arena.reset(.retain_capacity);
            const scope = try staging.ownerScope(scratch, self.plan, self.participant.plan_digest, target, range);
            const status = try port.execute(scratch, target.table.name, range.group_id, .{ .scope = scope, .action = .status, .native_execution = target.table.sql_persistence }, context);
            if (!std.meta.eql(status.population, @as(?contract.Cursor, cursor))) return error.RelationPopulationChanged;
            receipt.* = .{
                .group_id = range.group_id,
                .range_id = scope.target_namespace.range_id,
                .plan_digest = self.participant.plan_digest,
                .completion_digest = status.receipt,
                .population = cursor,
            };
            if (historical) |archive| {
                const bootstrap = try staging.frozenOwnerBootstrap(scratch, self.plan, self.participant.plan_digest, target, index);
                const sealed = try port.execute(scratch, target.table.name, range.group_id, .{ .scope = bootstrap.scope, .action = .capture_initial, .native_execution = target.table.sql_persistence }, context);
                const handle = sealed.frozen_image orelse return error.SourceSnapshotIncomplete;
                try handle.requireScope(bootstrap.scope);
                if (!std.meta.eql(sealed.population, @as(?contract.Cursor, archive[index]))) return error.RelationPopulationChanged;
                receipt.frozen_population = handle.binding;
            }
            try receipt.requirePopulation(scope);
        }
        return receipts;
    }

    /// Rejoin the durable live owner after the paired import proof was
    /// recorded. Validation receipts use a different metadata phase.
    pub fn validateOwner(self: *const Cohort, a: A, target_index: usize, range_index: usize, port: @import("../../restore_owner.zig").Port, context: local.api_operation.RequestContext) !staging.OwnerReceipt {
        if (target_index >= self.plan.targets.len) return error.InvalidRestoreStaging;
        const target = self.plan.targets[target_index];
        if (range_index >= target.ranges.len) return error.InvalidRestoreStaging;
        const range = target.ranges[range_index];
        const scope = try staging.ownerScope(a, self.plan, self.participant.plan_digest, target, range);
        const response = try port.execute(a, target.table.name, range.group_id, .{ .scope = scope, .action = .validate, .native_execution = target.table.sql_persistence }, context);
        if (response.phase != .validated) return error.RestoreValidationPending;
        return .{
            .group_id = range.group_id,
            .range_id = scope.target_namespace.range_id,
            .plan_digest = self.participant.plan_digest,
            .completion_digest = response.receipt,
        };
    }

    fn nextPopulation(raw: *anyopaque, limit: u32) !?[]const contract.Row {
        const stream: *Stream = @ptrCast(@alignCast(raw));
        return stream.next(limit);
    }

    fn finishPopulation(raw: *anyopaque) !void {
        const stream: *Stream = @ptrCast(@alignCast(raw));
        if (stream.offset != stream.rows.len) return error.SourceSnapshotIncomplete;
    }

    pub fn deinit(self: *Cohort) void {
        for (self.sources) |*source| source.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Validate the live rows before reserving publication owners. This uses
/// only final declarations and staged postimages, so replaced generations
/// and historical frozen births cannot satisfy current constraints.
pub fn validateCurrentRows(backing: A, session: *const sessions.Session, context: local.api_operation.RequestContext) !void {
    const staged = session.staged orelse return;
    const metadata = session.sql orelse return error.SqlTransactionNotActive;
    var scratch = std.heap.ArenaAllocator.init(backing);
    defer scratch.deinit();
    const a = scratch.allocator();
    const view = try staged.distributedTables(a);
    defer view.deinit();
    var records: std.ArrayList(local.common_topology_records.TableRecord) = .empty;
    var tables: std.ArrayList(local.api_distributed_txn_contract.TableCommitRequest) = .empty;
    const pending = &metadata.catalog_generations;
    for (pending.entries.items) |entry| {
        const latest = pending.latest(entry.target) orelse return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, &entry.nonce, &latest.nonce)) continue;
        const physical = "table:".* ++ std.fmt.bytesToHex(entry.physical, .lower);
        for (view.tables) |table| {
            if (table.writes.len == 0 or !std.mem.eql(u8, table.table_name, &physical)) continue;
            const prepared = try entry.prepared(a);
            try records.append(a, prepared.child);
            // Reconstruct claims from the final fold without replaying
            // historical deletes or update predicates.
            try tables.append(a, .{
                .table_name = prepared.child.name,
                .relational_schema_version = table.relational_schema_version orelse return error.CatalogGenerationChanged,
                .writes = table.writes,
            });
            break;
        }
    }
    if (tables.items.len != 0) try local.api_relational_integrity_commit.validatePrivateCommit(backing, records.items, tables.items, context);
}
