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

//! SQL session delivery and metadata-authoritative transaction coordination.
//! Native postimages are never replayed from a SQL session during recovery.
const std = @import("std");
const local = @import("antfly_local_sources");
const registry = @import("../../../system_catalog/prepared_transactions.zig");
const distributed = @import("../../distributed_txn.zig");
const transactions = @import("../../transactions.zig");
const http = @import("../../http_server.zig");
const A = std.mem.Allocator;

const ScopedFailureStage = enum { payload, cohort, lock_cohort, admit, prepare, recover, session, session_receipt, session_scope, session_outbox_ack, session_native_recover, session_index_scope, predecessor_resolve, predecessor_lock_release };
var scoped_failure_stages: std.atomic.Value(u16) = .init(0);
fn diagnoseScopedFailure(stage: ScopedFailureStage, err: anyerror, _: ?*std.builtin.StackTrace) void {
    if (comptime @import("builtin").mode == .debug) {
        const bit = @as(u16, 1) << @backingInt(stage);
        if (scoped_failure_stages.fetchOr(bit, .monotonic) & bit != 0) return;
        std.debug.print("scoped SQL admission failure stage={s} err={s}\n", .{ @tagName(stage), @errorName(err) });
    }
}

pub const Registry = struct {
    server: *http.ApiHttpServer,
    context: local.api_operation.RequestContext,
    actor: registry.Actor = .{ .session_user = .bootstrap, .credential = "" },
    database_id: u64,
    native_admin: bool = false,
    // A head is an admission window, not an equality precondition. Reuse it
    // across this bounded controller pass instead of adding a quorum read for
    // every phase. Durable CAS still fences the exact job revision.
    anchor: ?struct { incarnation: [16]u8, standalone: bool, head: u64 } = null,

    pub fn authority(self: *@This()) distributed.ExternalAuthority {
        return .{ .ptr = self, .read = read, .admit = admit, .advance = advance, .next_version = nextVersion, .prepare_locks = prepareLocks, .resolve_locks = resolveLocks, .retire_native = retireNative, .reserve_capture = reserveCapture, .observe_capture = observeCapture, .observe_terminal = observeTerminal, .drive_catalog = driveCatalog };
    }

    const Publication = @import("../../logical_replication_capture_worker.zig");

    fn publicationContext(self: *@This()) local.api_operation.RequestContext {
        var private = self.context;
        private.setting_admin = true;
        private.sql_completion = null;
        private.sql_completion_submission = null;
        private.sql_command_fence = null;
        return private;
    }

    fn bindPublication(self: *@This(), a: A, cut: registry.Snapshot, transaction: [16]u8, cohort: *distributed.ExternalPreparation) !void {
        if (!cut.publication_capture_ready or cohort.manifest.len == 0) return;
        const installation = @import("../../../system_catalog/replication/installation.zig");
        var routes: [registry.max_participants]installation.DemandRoute = undefined;
        const batch = routes[0..cohort.manifest.len];
        for (batch, cohort.manifest) |*route, part| route.* = part.route;
        const bytes = try self.server.source.systemCatalog(a, self.publicationContext(), .{ .sql_publication_capture_work = .{ .demand_routes = batch } });
        defer a.free(bytes);
        const bindings = try std.json.parseFromSliceLeaky([]const ?installation.DemandBinding, a, bytes, .{ .allocate = .alloc_always });
        try cohort.bindPublication(a, transaction, bindings);
    }

    fn reserveCapture(raw: *anyopaque, a: A, part: registry.Participant, request: Publication.Capacity.Request) anyerror!Publication.Proofs.CapacityReceipt {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.server.routedPreparedPublicationReceipt(a, part.table_name, part.route, .capacity, request, self.publicationContext());
    }

    fn observeCapture(raw: *anyopaque, a: A, part: registry.Participant, demand: Publication.Capacity.Observation) anyerror!?Publication.Proofs.OutboxReceipt {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.server.routedPreparedPublicationReceipt(a, part.table_name, part.route, .outbox, demand, self.publicationContext());
    }

    fn observeTerminal(raw: *anyopaque, a: A, part: registry.Participant, demand: Publication.Capacity.Observation) anyerror!?Publication.Proofs.TerminalResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.server.routedPreparedPublicationReceipt(a, part.table_name, part.route, .terminal, demand, self.publicationContext());
    }

    pub fn snapshot(self: *@This(), a: A, lookup: registry.Read) !registry.Snapshot {
        var private = self.context;
        private.setting_admin = true;
        // The identity embeds the command scope. Independent decision delivery
        // must keep the sealed admission hash unchanged across client death.
        if (private.sql_command_fence != null) {
            private.sql_completion = null;
            private.sql_completion_submission = null;
            private.sql_command_fence = null;
        }
        try private.ensureActive();
        const bytes = try self.server.source.systemCatalog(a, private, .{ .sql_prepared_read = lookup });
        defer a.free(bytes);
        const cut = try std.json.parseFromSliceLeaky(registry.Snapshot, a, bytes, .{ .allocate = .alloc_always });
        if (std.mem.allEqual(u8, &cut.incarnation, 0) or cut.active > registry.max_active or cut.outstanding_receipts > registry.max_receipts or cut.records.len > 8 or cut.receipts.len > 8) return error.InvalidSqlBackendResponse;
        for (cut.receipts) |receipt| {
            try receipt.validate();
            try receipt.session.requireRealm(cut.standalone);
            if (!std.mem.eql(u8, &receipt.owner.metadata_incarnation, &std.fmt.bytesToHex(cut.incarnation, .lower))) return error.CatalogGenerationChanged;
        }
        for (cut.records) |record| {
            try record.validate(a);
            if (record.identity.session) |session| try session.requireRealm(cut.standalone);
            if (!std.mem.eql(u8, &record.identity.metadata_incarnation, &std.fmt.bytesToHex(cut.incarnation, .lower))) return error.CatalogGenerationChanged;
        }
        self.anchor = .{ .incarnation = cut.incarnation, .standalone = cut.standalone, .head = cut.head };
        return cut;
    }

    fn checkRealm(cut: registry.Snapshot, identity: registry.Identity) !void {
        if (!std.mem.eql(u8, &identity.metadata_incarnation, &std.fmt.bytesToHex(cut.incarnation, .lower))) return error.CatalogGenerationChanged;
        if (identity.session) |session| try session.requireRealm(cut.standalone);
    }

    fn mutate(self: *@This(), a: A, request: registry.Mutation) !void {
        var private = self.context;
        private.setting_admin = true;
        // The identity embeds the command scope. Independent decision delivery
        // must keep the sealed admission hash unchanged across client death.
        if (private.sql_command_fence != null) {
            private.sql_completion = null;
            private.sql_completion_submission = null;
            private.sql_command_fence = null;
        }
        try private.ensureActive();
        const bytes = try self.server.source.systemCatalog(a, private, .{ .sql_prepared_mutate = request });
        defer a.free(bytes);
        const outcome = (try std.json.parseFromSliceLeaky(?registry.outcomes.Outcome, a, bytes, .{})) orelse return error.MetadataMutationOutcomeUnknown;
        _ = try outcome.result();
    }

    fn read(raw: *anyopaque, a: A, operation: [16]u8) anyerror!?registry.Record {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const cut = try self.snapshot(a, .{ .operation = operation });
        if (cut.records.len == 0) return null;
        if (cut.records.len != 1 or !std.mem.eql(u8, &cut.records[0].identity.operation_id, &operation)) return error.InvalidSqlBackendResponse;
        return cut.records[0];
    }

    fn admit(raw: *anyopaque, a: A, identity: registry.Identity, participants: []const registry.Participant) anyerror!registry.Record {
        const self: *@This() = @ptrCast(@alignCast(raw));
        // This is a production check, not a caller's assertion. The purpose,
        // signed plan and participant digest were committed together locally.
        const bytes = (try self.server.txn_sessions.externalPreparationSeal(self.server.alloc, identity.txn_id)) orelse return error.InvalidTransactionSessionRecord;
        defer self.server.alloc.free(bytes);
        const sealed = try std.json.parseFromSliceLeaky(transactions.ExternalPreparationSeal, a, bytes, .{ .allocate = .alloc_always });
        const owner = try identity.owner(a);
        if (!(try sealed.identity.owner(a)).eql(owner) or !std.mem.eql(u8, &identity.manifest_digest, &try registry.manifestDigest(a, participants, identity.metadata_incarnation))) return error.ExternalTransactionDecisionChanged;
        const cut = try self.snapshot(a, .{ .operation = identity.operation_id });
        try checkRealm(cut, identity);
        if (cut.records.len != 0) {
            const existing = cut.records[0];
            if (!(try existing.identity.owner(a)).eql(owner)) return error.ExternalTransactionDecisionChanged;
            return existing;
        }
        // Retirement never authorizes recreating a GID under an old plan.
        if (cut.receipts.len != 0) return error.ExternalTransactionDecisionChanged;
        try self.mutate(a, try sealed.admissionRequest());
        // The exact retained success proves this deterministic admission.
        // A concurrent decision is fenced by the native owner and the next
        // expected-revision CAS; another observation cannot eliminate that race.
        return .{ .identity = identity, .participants = participants };
    }

    fn prepareLocks(raw: *anyopaque, a: A, record: registry.Record) anyerror!bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const complete = try @import("data_locks.zig").transactionQuantum(self.server, a, self.context, record, null);
        self.anchor = null;
        return complete;
    }

    fn resolveLocks(raw: *anyopaque, a: A, record: registry.Record) anyerror!bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return @import("data_locks.zig").transactionQuantum(self.server, a, self.context, record, record.nativeLockMask() & ~record.native_lock_releases);
    }

    fn retireNative(raw: *anyopaque, a: A, record: registry.Record) anyerror!registry.Record {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var claims: std.ArrayListUnmanaged(registry.NativeRetirement) = .empty;
        defer claims.deinit(a);
        const retired = record.nativeRetirementMask();
        for (record.participants, 0..) |part, index| {
            if (part.route.route.execution != .native_local or retired & (@as(u64, 1) << @intCast(index)) != 0) continue;
            if (try @import("../recovery/native_recovery.zig").retiredGeneration(a, self.server.catalogSource(), part.route, self.context)) |loss| {
                try claims.append(a, .{ .member = @intCast(index), .digest = try loss.identityDigest(a) });
            }
        }
        if (claims.items.len == 0) return record;
        return advance(raw, a, record, .{ .native_retired = claims.items });
    }

    fn advance(raw: *anyopaque, a: A, record: registry.Record, input: registry.Action) anyerror!registry.Record {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var action = input;
        if (action == .prepare and record.phase == .preparing) {
            if (record.identity.catalog_participant) |participant| try self.preflightCatalogPredecessors(a, participant);
        }
        if (action == .commit and record.publicationCaptureMask() != 0)
            action.commit.commit_time_unix_us = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_us;
        if (action == .handoff) {
            if (try self.acknowledgeCatalogTarget(a, record)) return error.MetadataMutationOutcomeUnknown;
            if (!try self.releaseScopeQuantum(a, record.identity)) return error.MetadataMutationOutcomeUnknown;
        }
        const owner = try record.identity.owner(a);
        for (0..2) |attempt| {
            if (self.anchor == null) _ = try self.snapshot(a, .{ .operation = record.identity.operation_id });
            const anchor = self.anchor.?;
            if (!std.mem.eql(u8, &record.identity.metadata_incarnation, &std.fmt.bytesToHex(anchor.incarnation, .lower))) return error.CatalogGenerationChanged;
            var nonce: [16]u8 = undefined;
            try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
            const request: registry.Mutation = .{ .notification_control = if (record.identity.notification_plan != null) true else null, .admission = .{ .nonce = nonce, .head = anchor.head }, .incarnation = anchor.incarnation, .actor = if (action == .commit or action == .abort) self.actor else .{ .session_user = .bootstrap, .credential = "" }, .native_admin = self.native_admin, .standalone = anchor.standalone, .database_id = self.database_id, .operation = .{ .advance = .{ .owner = owner, .expected_revision = record.revision, .action = action } } };
            self.mutate(a, request) catch |err| {
                if (err != error.SequenceAdmissionExpired or attempt != 0) return err;
                // Refresh only a proven exhausted admission window. Reusing
                // the same job CAS prevents a lost prior reply from applying
                // this transition twice, even after its receipt was evicted.
                _ = try self.snapshot(a, .{ .operation = record.identity.operation_id });
                continue;
            };
            if (action == .lock_handoff or action == .lock_release or action == .catalog_complete) {
                // The native quantum may atomically publish its completion.
                // Its phase comes from the accepting owner, never an assumed
                // local postimage of a background job.
                const cut = try self.snapshot(a, .{ .operation = record.identity.operation_id });
                if (cut.records.len != 1 or cut.records[0].revision < record.revision or !(try cut.records[0].identity.owner(a)).eql(owner)) return error.MetadataMutationOutcomeUnknown;
                return cut.records[0];
            }
            // Reconstruct the deterministic postimage from the exact command's
            // durable success, including retirement of its operation row.
            return registry.advance(a, record, record.revision, owner, action);
        }
        return error.MetadataMutationOutcomeUnknown;
    }

    const CatalogStages = @import("../../../metadata/restore_staging.zig");
    const GenerationRows = @import("../copy/generation_rows.zig");
    const RestoreOwner = @import("../../restore_owner.zig");

    fn catalogOwnerPort(self: *@This()) RestoreOwner.Port {
        const TargetPort = struct {
            fn execute(raw: *anyopaque, alloc: A, table: []const u8, group: u64, request: RestoreOwner.Request, ctx: local.api_operation.RequestContext) !RestoreOwner.Response {
                const server: *http.ApiHttpServer = @ptrCast(@alignCast(raw));
                return server.executeRestoreOwner(alloc, table, group, request, ctx);
            }
        };
        return .{ .ptr = self.server, .execute_fn = TargetPort.execute };
    }

    fn submitCatalogProgress(self: *@This(), a: A, command: CatalogStages.Command) !void {
        const reply = try self.server.source.systemCatalog(a, self.publicationContext(), .{ .sql_restore_progress = command });
        defer a.free(reply);
        if (!std.mem.eql(u8, reply, "true")) return error.InvalidCatalogRecord;
    }

    fn observeCatalogJob(self: *@This(), a: A, participant: registry.CatalogParticipant) !?std.json.Parsed(CatalogStages.Job) {
        const context = self.publicationContext();
        const bytes = (try self.server.source.getRestoreStaging(a, participant.staging_id, context)) orelse return null;
        defer a.free(bytes);
        var parsed = try std.json.parseFromSlice(CatalogStages.Job, a, bytes, .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        try requireCatalogJob(a, parsed.value, participant);
        return parsed;
    }

    fn requireCatalogJob(a: A, job: CatalogStages.Job, participant: registry.CatalogParticipant) !void {
        const binding = job.plan.catalog_binding orelse return error.InvalidCatalogMutation;
        if (!std.mem.eql(u8, &job.plan.id, &participant.staging_id) or
            !std.mem.eql(u8, &job.plan_digest, &participant.plan_digest) or
            !std.mem.eql(u8, &binding.intent_digest, &participant.intent_digest) or
            !std.mem.eql(u8, &try job.plan.digest(a), &participant.plan_digest))
            return error.ExternalTransactionDecisionChanged;
    }

    /// Import and validate on the same owner endpoints used by restore, with
    /// independent metadata receipts for the two phases. A partially uploaded
    /// target replays its sealed prefix and contributes only outstanding votes.
    fn prepareCatalogOwners(self: *@This(), a: A, cohort: *GenerationRows.Cohort) !void {
        var phase: enum { observe, deliver, import_receipt, integrity, validate_owner, validation_receipt } = .observe;
        errdefer if (comptime @import("builtin").mode == .debug) {
            const Diagnostics = struct {
                var reported: std.atomic.Value(u8) = .init(0);
            };
            const bit = @as(u8, 1) << @as(u3, @intCast(@backingInt(phase)));
            if (Diagnostics.reported.fetchOr(bit, .monotonic) & bit == 0)
                std.debug.print("SQL catalog preparation failed phase={s}\n", .{@tagName(phase)});
        };
        var job = (try self.observeCatalogJob(a, cohort.participant)) orelse return error.CatalogPublicationProofPending;
        defer job.deinit();
        const context = self.publicationContext();
        const port = self.catalogOwnerPort();
        if (job.value.state == .importing) {
            var ordinal: u32 = 0;
            for (cohort.plan.targets, 0..) |target, index| {
                const start = ordinal;
                ordinal += @intCast(target.ranges.len);
                if (ordinal <= job.value.completed_owners) continue;
                phase = .deliver;
                const receipts = try cohort.deliverTarget(a, index, port, context, .{
                    .readiness_io = self.server.embedding_provider_runtime.io,
                });
                defer a.free(receipts);
                const first = job.value.completed_owners -| start;
                phase = .import_receipt;
                for (receipts[first..]) |receipt| try self.submitCatalogProgress(a, .{
                    .action = .imported,
                    .id = cohort.plan.id,
                    .expected_revision = job.value.revision,
                    .record_receipt_identity = true,
                    .receipt = receipt,
                });
            }
            // Only the final import advances the revision and resets the
            // owner count. Re-observe rather than assuming transport success.
            phase = .observe;
            const observed = (try self.observeCatalogJob(a, cohort.participant)) orelse return error.CatalogPublicationProofPending;
            job.deinit();
            job = observed;
        }
        if (job.value.state != .validating) return error.CatalogPublicationProofPending;
        // Imported primary rows do not establish distributed UNIQUE/FK
        // coverage. Drive the same hidden-cohort activation as staged restore
        // before asking any owner to attest validation.
        phase = .integrity;
        if (job.value.completed_owners == 0) {
            const integrity = local.api_relational_integrity_commit;
            for (cohort.plan.targets) |target| {
                if (!try integrity.requiresActivation(a, target.table.schema_json)) continue;
                const validation = try (self.server.cfg.restore_validation orelse return error.RestoreValidationPending).prepare(a, job.value, context);
                defer validation.deinit();
                var cursor: @import("../../restore_catalog.zig").ValidationCursor = .{};
                while (true) {
                    try context.ensureActive();
                    var page = std.heap.ArenaAllocator.init(a);
                    defer page.deinit();
                    if (try validation.step(page.allocator(), &cursor, context)) break;
                }
                break;
            }
        }
        const io = self.server.embedding_provider_runtime.io;
        var ordinal: u32 = 0;
        for (cohort.plan.targets, 0..) |target, index| {
            for (target.ranges, 0..) |_, range_index| {
                defer ordinal += 1;
                if (ordinal < job.value.completed_owners) continue;
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                phase = .validate_owner;
                const receipt = retry: while (true) {
                    try context.ensureActive();
                    _ = scratch.reset(.retain_capacity);
                    break :retry cohort.validateOwner(scratch.allocator(), index, range_index, port, context) catch |err| switch (err) {
                        error.RestoreValidationPending, error.RestoreProjectionCatchUpPending, error.IndexRebuilding => {
                            try io.sleep(.fromMilliseconds(10), .awake);
                            continue;
                        },
                        else => return err,
                    };
                };
                phase = .validation_receipt;
                try self.submitCatalogProgress(scratch.allocator(), .{
                    .action = .validated,
                    .id = cohort.plan.id,
                    .expected_revision = job.value.revision,
                    .record_receipt_identity = true,
                    .receipt = receipt,
                });
            }
        }
    }

    // A predecessor may be a lock-only participant of this same commit.
    // Install its serving fence after data votes and lock custody transfer,
    // then retain the source proof before the metadata PREPARED transition.
    fn preflightCatalogPredecessors(self: *@This(), a: A, participant: registry.CatalogParticipant) !void {
        var job = (try self.observeCatalogJob(a, participant)) orelse return error.CatalogPublicationProofPending;
        defer job.deinit();
        if (job.value.state != .validating) return error.CatalogPublicationProofPending;
        const context = self.publicationContext();
        const io = self.server.embedding_provider_runtime.io;
        const plan = job.value.plan;
        var revision = job.value.revision;
        var index: usize = 0;
        while (try plan.catalogPredecessorAt(index)) |old| : (index += 1) {
            try context.ensureActive();
            if (try self.server.source.getRestoreStagingReceipt(a, plan.id, .validating, old.range.group_id, context)) |receipt| {
                a.free(receipt);
                continue;
            }
            const proof = while (true) {
                try context.ensureActive();
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                break self.preflightCatalogPredecessor(scratch.allocator(), plan.id, participant.plan_digest, old, context) catch |err| switch (err) {
                    error.RestoreStagingWait, error.StorageReadTemporarilyUnavailable, error.RaftApplyWriterUnavailable => {
                        try io.sleep(.fromMilliseconds(10), .awake);
                        continue;
                    },
                    else => return err,
                };
            };
            try self.submitCatalogProgress(a, .{
                .id = plan.id,
                .action = .source_handoff_checked,
                .expected_revision = revision,
                .record_receipt_identity = true,
                .receipt = .{
                    .group_id = old.range.group_id,
                    .range_id = if (old.range.range_id != 0) old.range.range_id else old.range.group_id,
                    .plan_digest = participant.plan_digest,
                    .completion_digest = proof,
                },
            });
            revision += 1;
        }
    }

    fn preflightCatalogPredecessor(self: *@This(), a: A, plan_id: CatalogStages.Id, plan_digest: CatalogStages.Digest, old: CatalogStages.Plan.CatalogPredecessor, context: local.api_operation.RequestContext) ![32]u8 {
        const reads = self.server.table_reads orelse return error.UnsupportedOperation;
        const writes = self.server.table_writes orelse return error.UnsupportedOperation;
        const handoff = (try CatalogStages.generationHandoffForOldRange(old.target, old.range)) orelse return error.RestoreSourceProofMissing;
        try http.requireGenerationHandoffReceiptAuthority(a, reads, old.table.name, old.range.start_key, old.fence.namespace, null, null, null, context, false);
        try http.installSqlPredecessorFence(a, reads, writes, old.table.name, old.range.start_key, old.fence, .{
            .plan_id = plan_id,
            .plan_digest = plan_digest,
        });
        return http.readGenerationHandoffPreflight(a, reads, old.table.name, old.range.start_key, old.fence, plan_id, plan_digest, handoff, context);
    }

    /// A committed namespace can be visible while the owner still holds an
    /// unpublished restore marker. Drive one read-indexed, durable ACK before
    /// releasing the transaction scope.
    fn acknowledgeCatalogTarget(self: *@This(), a: A, record: registry.Record) !bool {
        const participant = record.identity.catalog_participant orelse return false;
        if (record.catalog_outcome != .published) return false;
        var observed = (try self.observeCatalogJob(a, participant)) orelse return error.CatalogPublicationProofPending;
        defer observed.deinit();
        const job = observed.value;
        if (job.state != .published) return error.CatalogPublicationProofPending;
        const selected = job.plan.catalogCancelTarget(job.published_owners) orelse return false;
        const range = selected.target.ranges[selected.range_index];
        const scope = try CatalogStages.ownerScope(a, job.plan, job.plan_digest, selected.target, range);
        const response = try self.server.executeRestoreOwner(a, selected.target.table.name, range.group_id, .{
            .scope = scope,
            .action = .publish,
            .native_execution = selected.target.table.sql_persistence,
        }, self.publicationContext());
        if (response.phase != .published or response.population == null) return error.CatalogPublicationProofPending;
        try self.submitCatalogProgress(a, .{
            .id = job.plan.id,
            .action = .catalog_owner_published,
            .expected_revision = job.revision,
            .record_receipt_identity = true,
            .receipt = .{
                .group_id = range.group_id,
                .range_id = scope.target_namespace.range_id,
                .plan_digest = job.plan_digest,
                .completion_digest = response.receipt,
                .population = response.population,
            },
        });
        return true;
    }

    fn driveCatalog(raw: *anyopaque, a: A, record: registry.Record, outcome: registry.CatalogOutcome) anyerror!bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const stages = @import("../../../metadata/restore_staging.zig");
        const participant = record.identity.catalog_participant orelse return error.InvalidCatalogMutation;
        var parsed = (try self.observeCatalogJob(a, participant)) orelse return error.CatalogPublicationProofPending;
        defer parsed.deinit();
        const job = parsed.value;
        const phase = job.plan.catalogPublicationPhase() orelse return error.InvalidCatalogRecord;
        const action: stages.Command.Action = switch (outcome) {
            .published => switch (job.state) {
                .validating => switch (phase.state) {
                    .validating => return false,
                    .cutover => .begin_cutover,
                    .activating => if (catalogRestoreHasOld(job.plan)) .begin_cutover else .begin_activation,
                    else => return error.InvalidCatalogRecord,
                },
                .cutover => cutover: {
                    if (try job.plan.catalogPredecessorAt(job.completed_owners)) |old|
                        return self.fenceCatalogPredecessor(a, record, job, old);
                    if (phase.state != .activating) return false;
                    break :cutover .begin_activation;
                },
                .activating => return self.activateCatalogOwner(a, job),
                else => return false,
            },
            .canceled => if (job.state == .canceling) return self.cancelCatalogOwner(a, job) else if (job.state == .canceled) return false else .begin_cancel,
            .pending => return error.InvalidPreparedTransactionPhase,
        };
        const reply = try self.server.source.systemCatalog(a, self.publicationContext(), .{ .sql_restore_progress = .{
            .id = job.plan.id,
            .action = action,
            .expected_revision = job.revision,
        } });
        defer a.free(reply);
        if (!std.mem.eql(u8, reply, "true")) return error.InvalidCatalogRecord;
        return true;
    }

    fn fenceCatalogPredecessor(self: *@This(), a: A, record: registry.Record, job: @import("../../../metadata/restore_staging.zig").Job, old: @import("../../../metadata/restore_staging.zig").Plan.CatalogPredecessor) !bool {
        const stages = @import("../../../metadata/restore_staging.zig");
        var phase: enum { authority, participant, drain, seal, receipt } = .authority;
        errdefer if (comptime @import("builtin").mode == .debug) {
            const Diagnostics = struct {
                var reported: std.atomic.Value(u8) = .init(0);
            };
            const bit = @as(u8, 1) << @as(u3, @intCast(@backingInt(phase)));
            if (Diagnostics.reported.fetchOr(bit, .monotonic) & bit == 0)
                std.debug.print("SQL catalog predecessor recovery waiting phase={s} owner={}\n", .{ @tagName(phase), old.range.group_id });
        };
        const context = self.publicationContext();
        const reads = self.server.table_reads orelse return error.UnsupportedOperation;
        const writes = self.server.table_writes orelse return error.UnsupportedOperation;
        const handoff = (try stages.generationHandoffForOldRange(old.target, old.range)) orelse return error.RestoreSourceProofMissing;
        try http.requireGenerationHandoffReceiptAuthority(a, reads, old.table.name, old.range.start_key, old.fence.namespace, null, null, null, context, true);
        const seal = (http.observeGenerationHandoffSeal(a, reads, old.table.name, old.range.start_key, old.fence, job.plan.id, job.plan_digest, handoff) catch |err| switch (err) {
            error.RestoreStagingWait => return error.CatalogPublicationProofPending,
            else => return err,
        }) orelse drain: {
            phase = .participant;
            const member = try registry.predecessorParticipant(record, old);
            if (member) |index| {
                // Only the member on the generation being discarded may resolve
                // here. Other participants wait for atomic catalog publication.
                try http.installSqlPredecessorFence(a, reads, writes, old.table.name, old.range.start_key, old.fence, .{
                    .plan_id = job.plan.id,
                    .plan_digest = job.plan_digest,
                });
                if (!try self.drainCatalogPredecessor(a, record, index, context)) return true;
            }
            phase = .drain;
            http.waitForRestoreCutoverFence(a, reads, writes, old.table.name, old.range.start_key, old.fence, null, .{
                .plan_id = job.plan.id,
                .plan_digest = job.plan_digest,
            }) catch |err| switch (err) {
                error.RestoreStagingWait => return error.CatalogPublicationProofPending,
                else => return err,
            };
            phase = .seal;
            break :drain http.waitForGenerationHandoffSeal(a, reads, writes, old.table.name, old.range.start_key, old.fence, job.plan.id, job.plan_digest, handoff) catch |err| switch (err) {
                error.RestoreStagingWait => return error.CatalogPublicationProofPending,
                else => return err,
            };
        };
        phase = .receipt;
        const reply = try self.server.source.systemCatalog(a, context, .{ .sql_restore_progress = .{
            .id = job.plan.id,
            .action = .old_fenced,
            .expected_revision = job.revision,
            .record_receipt_identity = true,
            .receipt = .{
                .group_id = old.range.group_id,
                .range_id = if (old.range.range_id != 0) old.range.range_id else old.range.group_id,
                .plan_digest = job.plan_digest,
                .completion_digest = try stages.generationHandoffOldFenceDigest(old.fence, seal, null),
            },
        } });
        defer a.free(reply);
        if (!std.mem.eql(u8, reply, "true")) return error.InvalidCatalogRecord;
        return true;
    }

    fn drainCatalogPredecessor(self: *@This(), a: A, record: registry.Record, index: usize, context: local.api_operation.RequestContext) !bool {
        const decision = record.decision orelse return error.CatalogPublicationProofPending;
        if (decision.status != .committed or record.phase != .decided or record.activations != record.mask()) return error.CatalogPublicationProofPending;
        const part = record.participants[index];
        const request: distributed.TxnResolveRequest = .{
            .txn_id = record.identity.txn_id,
            .status = .committed,
            .commit_version = decision.version,
            .sync_level = .write,
            .external_decision_owner = try record.identity.owner(a),
            .route_fence = if (part.route.route.execution == .native_local) part.route else null,
            .topology_epoch = 0,
        };
        var transport: ParticipantTransport = undefined;
        try transport.init(self.server);
        transport.local_worker.recovery_deadline_ns = context.deadline_ns;
        if (transport.hosted) transport.hosted_worker.recovery_deadline_ns = context.deadline_ns;
        const worker = transport.worker();
        (if (context.deadline_ns) |deadline|
            worker.resolveGroupUntil(a, part.route.route.group_id, part.table_name, request, deadline)
        else
            worker.resolveGroupWithCancellation(a, part.route.route.group_id, part.table_name, request, context.cancellation)) catch |err| {
            diagnoseScopedFailure(.predecessor_resolve, err, @errorReturnTrace());
            return err;
        };
        const released = @import("data_locks.zig").transactionQuantum(self.server, a, context, record, @as(u64, 1) << @intCast(index)) catch |err| {
            diagnoseScopedFailure(.predecessor_lock_release, err, @errorReturnTrace());
            return err;
        };
        if (!released) return false;
        if (index == 0) {
            // This physical coordinator is about to disappear. The durable
            // metadata manifest already owns every peer's remaining replay.
            var ids: [registry.max_participants][]const u8 = undefined;
            var used: usize = 0;
            defer for (ids[0..used]) |id| a.free(id);
            for (record.participants) |member| {
                ids[used] = try distributed.participantIdForGroupScoped(a, member.table_name, member.route.route.group_id, null, null);
                used += 1;
            }
            try worker.acknowledgeManyGroup(a, part.route.route.group_id, part.table_name, .{
                .txn_id = record.identity.txn_id,
                // The Raft ACK is an ordered control over the captured
                // participant and durable local transaction roster.
                .participants = ids[0..used],
            });
        }
        return true;
    }

    fn activateCatalogOwner(self: *@This(), a: A, job: @import("../../../metadata/restore_staging.zig").Job) !bool {
        const stages = @import("../../../metadata/restore_staging.zig");
        const selected = job.plan.catalogActivationOwnerAt(job.completed_owners) orelse return false;
        const group = selected.range.group_id;
        const context = self.publicationContext();
        const reads = self.server.table_reads orelse return error.UnsupportedOperation;
        const writes = self.server.table_writes orelse return error.UnsupportedOperation;
        const mapped = (try stages.mappedEmptyGenerationHandoffForGroup(a, job.plan, job.plan_digest, group)) orelse return error.RestoreSourceProofMissing;
        const scope = try stages.ownerScope(a, job.plan, job.plan_digest, selected.target, selected.range);
        const fence = try stages.emptyGenerationDestinationFence(job.plan, selected.target, selected.range, selected.handoff, scope);
        try http.requireGenerationHandoffReceiptAuthority(a, reads, selected.target.table.name, selected.range.start_key, scope.target_namespace, group, mapped.command.scope, job.plan.id, context, true);
        _ = (try writes.batch(a, selected.target.table.name, .{
            .restore_staging_scope = mapped.command.scope,
            .restore_staging_plan_id = job.plan.id,
            .relational_topology = .{ .fence = fence, .action = .install_generation_handoff, .generation_handoff_install = mapped.command },
        })) orelse return error.CatalogPublicationProofPending;
        var status = (try reads.lookupGroupLocal(a, group, selected.target.table.name, "", .{
            .relational_topology_json = "{\"mode\":\"generation_handoff_install\"}",
            .restore_staging_scope = mapped.command.scope,
            .restore_staging_plan_id = job.plan.id,
            .execution_deadline_ns = (try context.platformDeadline()).deadline_ns,
            .cancellation = context.cancellation,
        }, .read_index)) orelse return error.CatalogPublicationProofPending;
        defer status.deinit(a);
        const Receipt = local.storage_db_restore_staging_contract.GenerationAdmissionReceipt;
        var parsed = try std.json.parseFromSlice(?Receipt, a, status.json, .{});
        defer parsed.deinit();
        const installed = parsed.value orelse return error.CatalogPublicationProofPending;
        try (try installed.position()).requireNamespace(fence.namespace);
        if (!std.mem.eql(u8, &installed.scope, &mapped.command.scope) or
            !std.mem.eql(u8, &installed.source_summary_digest, &mapped.command.source_summary_digest) or
            !std.mem.eql(u8, &installed.logical_digest, &mapped.expected_receipt_digest))
            return error.RestoreSourceProofMissing;
        const reply = try self.server.source.systemCatalog(a, context, .{ .sql_restore_progress = .{
            .id = job.plan.id,
            .action = .target_admissions_activated,
            .expected_revision = job.revision,
            .record_receipt_identity = true,
            .receipt = .{
                .group_id = group,
                .range_id = if (selected.range.range_id != 0) selected.range.range_id else group,
                .plan_digest = job.plan_digest,
                .completion_digest = installed.logical_digest,
            },
        } });
        defer a.free(reply);
        if (!std.mem.eql(u8, reply, "true")) return error.InvalidCatalogRecord;
        return true;
    }

    fn cancelCatalogOwner(self: *@This(), a: A, job: @import("../../../metadata/restore_staging.zig").Job) !bool {
        const context = self.publicationContext();
        const port = self.catalogOwnerPort();
        const receipt = if (job.plan.catalogCancelTarget(job.completed_owners)) |next|
            try @import("../copy/generation_rows.zig").cancelOwner(a, job, next.target, next.range_index, port, context)
        else predecessor: {
            const old = (try job.plan.catalogCancelPredecessor(job.completed_owners)) orelse return false;
            const writes = self.server.table_writes orelse return error.UnsupportedOperation;
            _ = (try writes.batch(a, old.table.name, .{ .relational_topology = .{ .fence = old.fence, .action = .cancel } })) orelse return error.CatalogPublicationProofPending;
            break :predecessor @import("../../../metadata/restore_staging.zig").OwnerReceipt{
                .group_id = old.range.group_id,
                .range_id = if (old.range.range_id != 0) old.range.range_id else old.range.group_id,
                .plan_digest = job.plan_digest,
                .completion_digest = try @import("../../../metadata/restore_staging.zig").predecessorCancelDigest(old.fence),
            };
        };
        const reply = try self.server.source.systemCatalog(a, context, .{ .sql_restore_progress = .{
            .id = job.plan.id,
            .action = .canceled,
            .expected_revision = job.revision,
            .record_receipt_identity = true,
            .receipt = receipt,
        } });
        defer a.free(reply);
        if (!std.mem.eql(u8, reply, "true")) return error.InvalidCatalogRecord;
        return true;
    }

    fn catalogRestoreHasOld(plan: @import("../../../metadata/restore_staging.zig").Plan) bool {
        for (plan.targets) |target| if (target.replace != null and target.native_reset == null) return true;
        for (plan.external_fk_parents) |parent| if (parent.ranges.len != 0) return true;
        return false;
    }

    fn nextVersion(_: *anyopaque, floor: u64) anyerror!u64 {
        return distributed.commitVersion(floor);
    }

    /// Before admission, cancellation races the exact sealed request for its
    /// original nonce. After journal eviction, its fixed old head is a durable
    /// fence; atomic job/receipt absence distinguishes an unadmitted plan from
    /// work that has already reached the native journals.
    fn cancelUnadmitted(self: *@This(), a: A, seal: transactions.ExternalPreparationSeal) !bool {
        const original = try seal.admissionRequest();
        var cancel = original;
        cancel.actor = .{ .session_user = .bootstrap, .credential = "" };
        cancel.operation = .{ .cancel_admission = .{ .original_actor = original.actor, .original = original.operation.admit } };
        var private = self.context;
        private.setting_admin = true;
        if (private.sql_command_fence != null) {
            private.sql_completion = null;
            private.sql_completion_submission = null;
            private.sql_command_fence = null;
        }
        const response = self.server.source.systemCatalog(a, private, .{ .sql_prepared_mutate = cancel });
        var fenced = false;
        if (response) |bytes| {
            defer a.free(bytes);
            if (try std.json.parseFromSliceLeaky(?registry.outcomes.Outcome, a, bytes, .{})) |outcome| fenced = outcome == .rejected;
        } else |err| switch (err) {
            error.SequenceAdmissionExpired, error.MetadataMutationOutcomeUnknown => {},
            else => return err,
        }
        const cut = try self.snapshot(a, .{ .operation = seal.identity.operation_id });
        try checkRealm(cut, seal.identity);
        // The authoritative window/absence cut remains a proof even when the
        // cancellation response itself was lost or its old receipt was evicted.
        const head_fence = cut.head >= original.admission.head and cut.head - original.admission.head >= registry.outcomes.retained;
        return (fenced or head_fence) and cut.records.len == 0 and cut.receipts.len == 0;
    }

    /// Unadmitted SQL may already hold transaction locks. Native cleanup must
    /// prove the original purpose was rejected or permanently fenced, in the
    /// same writer transaction that releases a quantum of those locks.
    fn releaseUnadmittedScope(self: *@This(), a: A, seal: transactions.ExternalPreparationSeal) !bool {
        const ref = seal.identity.lock_scope orelse return true;
        const types = @import("../../../system_catalog/lock_authority.zig");
        const anchor = try self.snapshot(a, .{ .operation = seal.identity.operation_id });
        var context = self.context;
        context.setting_admin = true;
        context.sql_command_fence = null;
        context.sql_completion = null;
        context.sql_completion_submission = null;
        const query: types.Query = .{ .incarnation = anchor.incarnation, .standalone = anchor.standalone, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = seal.identity.database_id, .backend = ref.backend };
        const before = try std.json.parseFromSliceLeaky(types.Snapshot, a, try self.server.source.systemCatalog(a, context, .{ .sql_lock_read = query }), .{});
        const scope = before.scope orelse return error.InvalidSqlBackendResponse;
        if (scope.next_command > ref.command.sequence or scope.transaction == null) return true;
        if (!std.meta.eql(scope.transaction, @as(?types.scopes.Transaction, .{ .id = seal.identity.txn_id }))) return error.ExternalTransactionDecisionChanged;
        var nonce: [16]u8 = undefined;
        try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
        const request: types.Request = .{ .admission = .{ .head = before.head, .nonce = nonce }, .incarnation = before.incarnation, .standalone = before.standalone, .actor = query.actor, .database = query.database, .backend = ref.backend, .operation = .{ .release_unadmitted = try seal.admissionRequest() } };
        if (!try @import("data_locks.zig").beforeMetadataMutation(self.server, a, context, request)) return false;
        const bytes = try self.server.source.systemCatalog(a, context, .{ .sql_lock_mutate = request });
        const result = (try std.json.parseFromSliceLeaky(?types.outcomes.Outcome, a, bytes, .{})) orelse return error.MetadataMutationOutcomeUnknown;
        _ = try result.result();
        self.anchor = null;
        // One bounded cleanup quantum, then yield to the existing supervisor.
        const after = try std.json.parseFromSliceLeaky(types.Snapshot, a, try self.server.source.systemCatalog(a, context, .{ .sql_lock_read = query }), .{});
        return after.scope != null and after.scope.?.transaction == null;
    }

    /// One journal quantum per recovery pass. Scope cleanup shares the same
    /// native decision registry and retains its terminal receipt until done.
    fn releaseScopeQuantum(self: *@This(), a: A, identity: registry.Identity) !bool {
        const ref = identity.lock_scope orelse return true;
        const types = @import("../../../system_catalog/lock_authority.zig");
        const anchor = try self.snapshot(a, .{ .operation = identity.operation_id });
        if (identity.kind == .named) for (anchor.receipts) |receipt| {
            if (receipt.status == .prepared and receipt.owner.eql(try identity.owner(a))) return true;
        };
        var context = self.context;
        context.setting_admin = true;
        const query: types.Query = .{ .incarnation = anchor.incarnation, .standalone = anchor.standalone, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = identity.database_id, .backend = ref.backend };
        const before = try std.json.parseFromSliceLeaky(types.Snapshot, a, try self.server.source.systemCatalog(a, context, .{ .sql_lock_read = query }), .{});
        const scope = before.scope orelse {
            const session = identity.session orelse return error.InvalidSqlBackendResponse;
            var proof_query = query;
            proof_query.retired_connection = .{
                .id = session.connection_id orelse return error.InvalidSqlBackendResponse,
                .node = session.owner_node_id,
            };
            const proof = try std.json.parseFromSliceLeaky(types.Snapshot, a, try self.server.source.systemCatalog(a, context, .{ .sql_lock_read = proof_query }), .{});
            if (proof.superseded != true) return error.InvalidSqlBackendResponse;
            return true;
        };
        // A completed sealed command is a permanent cleanup witness, even if
        // later transactions have replaced the single recent release marker.
        if (scope.next_command > ref.command.sequence) return true;
        const expected: types.scopes.Transaction = .{ .id = identity.txn_id, .decision = .{ .operation = identity.operation_id, .admission = (try identity.owner(a)).admission_digest } };
        if (scope.transaction == null) {
            if (!std.meta.eql(scope.released_transaction, @as(?types.scopes.Transaction, expected))) return error.ExternalTransactionDecisionChanged;
            return true;
        }
        if (!std.meta.eql(scope.transaction, @as(?types.scopes.Transaction, expected))) return error.ExternalTransactionDecisionChanged;
        var nonce: [16]u8 = undefined;
        try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
        const request: types.Request = .{ .admission = .{ .nonce = nonce, .head = before.head }, .incarnation = anchor.incarnation, .standalone = anchor.standalone, .actor = query.actor, .database = identity.database_id, .backend = ref.backend, .operation = .{ .release = expected } };
        if (!try @import("data_locks.zig").beforeMetadataMutation(self.server, a, context, request)) return false;
        const result = try std.json.parseFromSliceLeaky(types.outcomes.Outcome, a, try self.server.source.systemCatalog(a, context, .{ .sql_lock_mutate = request }), .{});
        _ = try result.result();
        self.server.sql_lock_work_pending.store(true, .release);
        self.anchor = null; // Scope progress advances the shared admission head.
        const after = try std.json.parseFromSliceLeaky(types.Snapshot, a, try self.server.source.systemCatalog(a, context, .{ .sql_lock_read = query }), .{});
        return after.scope != null and after.scope.?.transaction == null and std.meta.eql(after.scope.?.released_transaction, @as(?types.scopes.Transaction, expected));
    }
    pub fn acknowledgeReceipt(self: *@This(), a: A, identity: registry.Identity) !void {
        const scope_released = self.releaseScopeQuantum(a, identity) catch |err| {
            diagnoseScopedFailure(.session_scope, err, @errorReturnTrace());
            return err;
        };
        if (!scope_released) return error.MetadataMutationOutcomeUnknown;
        const cut = try self.snapshot(a, .{ .receipt = identity.operation_id });
        try checkRealm(cut, identity);
        if (cut.receipts.len == 0) return;
        var nonce: [16]u8 = undefined;
        try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
        self.mutate(a, .{ .notification_control = if (identity.notification_plan != null) true else null, .admission = .{ .nonce = nonce, .head = cut.head }, .incarnation = cut.incarnation, .actor = .{ .session_user = .bootstrap, .credential = "" }, .standalone = cut.standalone, .database_id = identity.database_id, .operation = .{ .acknowledge_receipt = .{ .owner = try identity.owner(a), .txn_id = identity.txn_id } } }) catch |err| {
            diagnoseScopedFailure(.session_outbox_ack, err, @errorReturnTrace());
            return err;
        };
    }
};

/// The session proof is durable before the metadata delivery ACK. A lost ACK
/// reply is reconciled by an authoritative absence in the same incarnation.
/// Pending or absent admission remains debt; it cannot become ordinary commit.
pub fn recoverSession(server: *http.ApiHttpServer, txn_id: [16]u8, bytes: []const u8, prepared: bool, aborted: bool, committed: bool) !void {
    return recoverSessionWithContext(server, txn_id, bytes, prepared, aborted, committed, .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s });
}

/// Drain only an already terminal local transaction's exact completion. This
/// never admits a SQL body, scans the work queue, or changes its decision. An
/// autocommit caller keeps its command loan until native scope release and ACK.
pub fn drainSessionCompletion(server: *http.ApiHttpServer, txn_id: [16]u8, context: local.api_operation.RequestContext) !void {
    const store = server.txn_sessions.durable orelse return error.SqlPreparedDurabilityUnavailable;
    var quanta: u8 = 0;
    while (true) {
        try context.ensureActive();
        var state = (try store.loadInto(server.alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        defer state.deinit(server.alloc);
        if (!state.terminal_abort and state.terminal_commit == null) return error.SqlTransactionOutcomeUnknown;
        if (state.external_receipt_acknowledged) return;
        recoverSessionWithContext(server, txn_id, state.external_preparation orelse return error.InvalidTransactionSessionRecord, state.external_preparation_prepared, state.terminal_abort, state.terminal_commit != null, context) catch |err| switch (err) {
            error.MetadataMutationOutcomeUnknown => {},
            else => return err,
        };
        quanta += 1;
        if (quanta == 8) {
            try server.embedding_provider_runtime.io.sleep(.fromMilliseconds(1), .awake);
            quanta = 0;
        }
    }
}

/// A sealed request can have a definite admission rejection while its old
/// native locks still require several cancellation/release quanta. Finish only
/// that persisted purpose; never rebuild its cohort or evaluate the SQL again.
fn drainSealedSessionCompletion(server: *http.ApiHttpServer, txn_id: [16]u8, expected_seal: []const u8, context: local.api_operation.RequestContext) !void {
    var bounded = try context.platformDeadline();
    const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s;
    bounded.deadline_ns = @min(bounded.deadline_ns orelse deadline, deadline);
    const Driver = struct {
        server: *http.ApiHttpServer,
        txn_id: [16]u8,
        expected_seal: []const u8,
        context: local.api_operation.RequestContext,

        pub fn check(self: *@This()) !void {
            try self.context.ensureActive();
        }
        pub fn step(self: *@This()) !bool {
            const store = self.server.txn_sessions.durable orelse return error.SqlPreparedDurabilityUnavailable;
            var state = (try store.loadInto(self.server.alloc, self.txn_id)) orelse return error.SqlTransactionNotActive;
            defer state.deinit(self.server.alloc);
            const retained = state.external_preparation orelse return error.InvalidTransactionSessionRecord;
            if (!std.mem.eql(u8, retained, self.expected_seal)) return error.ExternalTransactionDecisionChanged;
            if (state.external_preparation_prepared) return error.SqlTransactionOutcomeUnknown;
            if ((state.terminal_abort or state.terminal_commit != null) and state.external_receipt_acknowledged) return true;
            try recoverSessionWithContext(self.server, self.txn_id, retained, false, state.terminal_abort, state.terminal_commit != null, self.context);
            return false;
        }
        pub fn pause(self: *@This()) !void {
            try self.server.embedding_provider_runtime.io.sleep(.fromMilliseconds(1), .awake);
        }
    };
    var driver: Driver = .{ .server = server, .txn_id = txn_id, .expected_seal = expected_seal, .context = bounded };
    try session_completion.drainSealedSession(&driver);
}

const session_completion = @import("../../../sql/session_completion.zig");
pub const sessionRequiresPhysicalRecovery = session_completion.sessionRequiresPhysicalRecovery;
pub const sessionPhysicalRecoveryFailure = session_completion.sessionPhysicalRecoveryFailure;

fn recoverSessionWithContext(server: *http.ApiHttpServer, txn_id: [16]u8, bytes: []const u8, prepared: bool, aborted: bool, committed: bool, context: local.api_operation.RequestContext) !void {
    var region = std.heap.ArenaAllocator.init(server.alloc);
    defer region.deinit();
    const a = region.allocator();
    const seal = try std.json.parseFromSliceLeaky(transactions.ExternalPreparationSeal, a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, &txn_id, &seal.identity.txn_id)) return error.InvalidTransactionSessionRecord;
    var source: Registry = .{ .server = server, .context = context, .database_id = seal.identity.database_id };
    source.actor = seal.actor;
    const cut = try source.snapshot(a, .{ .operation = seal.identity.operation_id });
    try Registry.checkRealm(cut, seal.identity);
    if (cut.receipts.len == 0) {
        if (!prepared and !aborted and !committed) {
            if (seal.identity.catalog_participant) |participant| {
                // Lost reservation/admission replies rejoin the immutable
                // plan. Only an attempt with no reserved owners may take
                // the ordinary unadmitted cancellation path.
                if (try source.observeCatalogJob(a, participant)) |job| {
                    job.deinit();
                    _ = try Registry.admit(&source, a, seal.identity, seal.participants);
                    return error.MetadataMutationOutcomeUnknown;
                }
                if (@import("antfly_platform").time.realtimeNs() < seal.identity.expires_at_unix_ns) {
                    if (Registry.admit(&source, a, seal.identity, seal.participants)) |_| {
                        return error.MetadataMutationOutcomeUnknown;
                    } else |err| switch (err) {
                        error.SequenceAdmissionExpired => {},
                        error.MetadataMutationOutcomeUnknown => return err,
                        else => if (registry.outcomes.Outcome.fromError(err) == null) return err,
                    }
                }
            }
            if (try source.cancelUnadmitted(a, seal)) {
                if (!try source.releaseUnadmittedScope(a, seal)) return error.MetadataMutationOutcomeUnknown;
                _ = (try server.txn_sessions.recordExternalAdmissionRejection(server.alloc, txn_id, try seal.identity.owner(a))) orelse return error.SqlTransactionNotActive;
                return;
            }
            return error.MetadataMutationOutcomeUnknown;
        }
    } else {
        if (cut.receipts.len != 1) return error.InvalidSqlBackendResponse;
        if (cut.receipts[0].status == .pending) {
            if (cut.records.len != 1) return error.InvalidSqlBackendResponse;
            const record = cut.records[0];
            if (record.identity.catalog_participant) |participant| {
                const now = @import("antfly_platform").time.realtimeNs();
                if (record.phase == .preparing and now < record.identity.expires_at_unix_ns) {
                    var state = (try server.txn_sessions.durable.?.loadInto(a, txn_id)) orelse return error.SqlTransactionNotActive;
                    defer state.deinit(a);
                    var plan = try transactions.parseCatalogExecutionPlan(a, state.execution_plan orelse return error.SqlRelationRecoveryIncomplete);
                    defer plan.deinit();
                    var cohort = try Registry.GenerationRows.Cohort.rejoin(a, &state, plan.value.catalog, participant);
                    defer cohort.deinit();
                    try source.prepareCatalogOwners(a, &cohort);
                    if (record.participants.len == 0) _ = try Registry.advance(&source, a, record, .prepare);
                    return error.MetadataMutationOutcomeUnknown;
                }
                if (record.participants.len == 0) {
                    _ = try recoverEmptyCohort(&source, a, record, now);
                    return error.MetadataMutationOutcomeUnknown;
                }
            }
            // A private commit keeps its coordinator-owned preparation alive
            // through the immutable admission deadline. The work controller
            // completes vote collection and decides it; a session recovery
            // pass must not replace that live purpose with an early abort.
            if (record.phase == .preparing and (record.identity.kind == .named or @import("antfly_platform").time.realtimeNs() >= record.identity.expires_at_unix_ns))
                _ = try Registry.advance(&source, a, record, .{ .abort = try distributed.commitVersion(record.identity.begin_timestamp) });
            return error.MetadataMutationOutcomeUnknown;
        }
        if (seal.identity.kind == .session_commit) {
            _ = (server.txn_sessions.recordExternalCommitReceipt(server.alloc, txn_id, cut.receipts[0]) catch |err| {
                diagnoseScopedFailure(.session_receipt, err, @errorReturnTrace());
                return err;
            }) orelse return error.SqlTransactionNotActive;
            if (cut.receipts[0].progress == .resolving or (cut.records.len == 1 and
                sessionRequiresPhysicalRecovery(cut.receipts[0].progress, cut.records[0])))
            {
                // The synchronous caller owns this exact durable purpose. It
                // can advance its admitted control graph directly rather than
                // flooding metadata reads while waiting for a global worker.
                if (cut.records.len != 1) return error.InvalidSqlBackendResponse;
                if (cut.records[0].participants.len == 0) {
                    _ = try recoverEmptyCohort(&source, a, cut.records[0], @import("antfly_platform").time.realtimeNs());
                    return error.MetadataMutationOutcomeUnknown;
                }
                var transport: ParticipantTransport = undefined;
                try transport.init(server);
                const pass_deadline = @import("antfly_platform").time.monotonicNs() +| distributed.external_recovery_pass_timeout_ns;
                const deadline = if (context.deadline_ns) |value| @min(value, pass_deadline) else pass_deadline;
                transport.local_worker.recovery_deadline_ns = deadline;
                if (transport.hosted) transport.hosted_worker.recovery_deadline_ns = deadline;
                _ = distributed.recoverExternalTransaction(a, transport.worker(), source.authority(), seal.identity.operation_id, @import("antfly_platform").time.realtimeNs(), .{ .fanout_io = server.sharedApiIo(), .external_deadline_ns = deadline }) catch |err| {
                    const classified = sessionPhysicalRecoveryFailure(err);
                    if (classified == error.MetadataMutationOutcomeUnknown) return classified;
                    diagnoseScopedFailure(.session_native_recover, err, @errorReturnTrace());
                    return classified;
                };
                return error.MetadataMutationOutcomeUnknown;
            }
            if (cut.receipts[0].progress == .indexing) {
                // Row intents have resolved: release transactional locks while
                // the independent index barrier remains durable recovery debt.
                _ = source.releaseScopeQuantum(a, seal.identity) catch |err| {
                    diagnoseScopedFailure(.session_index_scope, err, @errorReturnTrace());
                    return err;
                };
                return error.MetadataMutationOutcomeUnknown;
            }
        } else {
            _ = (try server.txn_sessions.recordExternalPreparationReceipt(server.alloc, txn_id, cut.receipts[0])) orelse return error.SqlTransactionNotActive;
        }
        try source.acknowledgeReceipt(a, seal.identity);
    }
    _ = (try server.txn_sessions.markExternalPreparationReceiptAcknowledged(server.alloc, txn_id)) orelse return error.SqlTransactionNotActive;
}

/// The same leader-fenced supervisor retires expired/closed command scopes.
/// One indexed owner and one native quantum per call; unresolved decisions
/// remain held while the cursor advances so they cannot starve other owners.
pub fn advanceLockWork(server: *http.ApiHttpServer, after: *?@import("../../../system_catalog/lock_authority.zig").scopes.Work) !bool {
    const group = server.cfg.prepared_transaction_group_id;
    if (server.cfg.prepared_transaction_leadership) |leadership| {
        if (!leadership.isLocalLeader(group)) return false;
    } else if (server.cfg.deployment_mode != .standalone) return false;
    const types = @import("../../../system_catalog/lock_authority.zig");
    var region = std.heap.ArenaAllocator.init(server.alloc);
    defer region.deinit();
    const a = region.allocator();
    const time = @import("antfly_platform").time;
    const context: local.api_operation.RequestContext = .{ .setting_admin = true, .deadline_ns = time.monotonicNs() +| std.time.ns_per_s };
    const now_ms = time.realtimeNs() / std.time.ns_per_ms;
    const actor: registry.Actor = .{ .session_user = .bootstrap, .credential = "" };
    const query: types.Query = .{ .incarnation = @splat(0), .standalone = server.cfg.deployment_mode == .standalone, .actor = actor, .database = 0, .work = .{ .observed_at_ms = now_ms, .after = after.*, .wrap = true } };
    const cut = try std.json.parseFromSliceLeaky(types.Snapshot, a, try server.source.systemCatalog(a, context, .{ .sql_lock_read = query }), .{});
    const work = cut.next_work orelse {
        after.* = null;
        return false;
    };
    after.* = work;
    const scope = cut.scope orelse return error.InvalidSqlBackendResponse;
    var nonce: [16]u8 = undefined;
    try server.embedding_provider_runtime.io.randomSecure(&nonce);
    const operation: @FieldType(types.Request, "operation") = if (scope.releasing)
        .{ .release = scope.transaction orelse return error.InvalidSqlBackendResponse }
    else if (scope.state == .active)
        .{ .fence = .{ .expired = .{ .expected_expiry = scope.registration.lease.expires_at_ms, .observed_at_ms = now_ms } } }
    else if (scope.transaction) |transaction| .{ .release = transaction } else .close_step;
    const request: types.Request = .{ .admission = .{ .nonce = nonce, .head = cut.head }, .incarnation = cut.incarnation, .standalone = cut.standalone, .actor = actor, .database = scope.registration.lease.database, .backend = work.backend, .operation = operation };
    if (!try @import("data_locks.zig").beforeMetadataMutation(server, a, context, request)) return true;
    const outcome = try std.json.parseFromSliceLeaky(types.outcomes.Outcome, a, try server.source.systemCatalog(a, context, .{ .sql_lock_mutate = request }), .{});
    _ = outcome.result() catch |err| switch (err) {
        error.CatalogGenerationChanged, error.CatalogPublicationProofPending => return true,
        else => return err,
    };
    return true;
}

/// One job per supervisor pass, with a keyset cursor so indefinitely prepared
/// transactions cannot starve incomplete or decided jobs later in the index.
/// Scheduling follows the existing metadata-group leadership capability; the
/// durable CAS and native owner checks remain authoritative across elections.
pub fn advanceWork(server: *http.ApiHttpServer, after: *?[16]u8) !void {
    const group = server.cfg.prepared_transaction_group_id;
    if (server.cfg.prepared_transaction_leadership) |leadership| {
        if (!leadership.isLocalLeader(group)) return;
    } else if (server.cfg.deployment_mode != .standalone) return;
    var region = std.heap.ArenaAllocator.init(server.alloc);
    defer region.deinit();
    const a = region.allocator();
    const time = @import("antfly_platform").time;
    var source: Registry = .{ .server = server, .context = .{ .setting_admin = true, .deadline_ns = time.monotonicNs() +| 5 * std.time.ns_per_s }, .database_id = 1 };
    const cut = try source.snapshot(a, .{ .work = after.* });
    if (cut.records.len == 0) {
        after.* = null;
        return;
    }
    const record = cut.records[0];
    after.* = record.identity.operation_id;
    source.database_id = record.identity.database_id;
    if ((record.phase == .preparing or record.phase == .prepared) and registry.recoveryDecision(record, time.realtimeNs()) == null) return;
    if (record.participants.len == 0) {
        _ = try recoverEmptyCohort(&source, a, record, time.realtimeNs());
        return;
    }
    const writes = server.table_writes orelse return;
    const deadline = time.monotonicNs() +| distributed.external_recovery_pass_timeout_ns;
    var local_worker = distributed.LocalTableWriteParticipantWorker.init(writes);
    local_worker.recovery_deadline_ns = deadline;
    var fallback = @import("../../table_router.zig").CatalogBackedGroupRouter.init(server.catalogSource(), server.localSessionNodeId());
    var hosted_worker: distributed.HostedParticipantWorker = undefined;
    const worker = if (server.cfg.deployment_mode == .standalone) local_worker.worker() else hosted: {
        hosted_worker = distributed.HostedParticipantWorker.init(server.catalogSource(), server.cfg.session_router orelse fallback.router(), writes, server.cfg.session_executor orelse return error.UnsupportedSqlExecution);
        hosted_worker.recovery_deadline_ns = deadline;
        _ = hosted_worker.withInternalServiceAuth(server.cfg.internal_service_secret, server.cfg.internal_service_issuer);
        break :hosted hosted_worker.worker();
    };
    _ = try distributed.recoverExternalTransaction(a, worker, source.authority(), record.identity.operation_id, time.realtimeNs(), .{ .fanout_io = server.sharedApiIo(), .external_deadline_ns = deadline });
}
const sql_session = local.sql_session;
const ast = local.sql_ast;
const execution = @import("../execution/execution.zig");

fn databaseIdentity(root: *execution.Adapter, a: A) !u64 {
    const bytes = try root.server.source.systemCatalog(a, root.context, .{ .read = .{ .kind = .database, .name = root.database } });
    defer a.free(bytes);
    const state = try std.json.parseFromSliceLeaky(local.system_catalog_domain.State, a, bytes, .{ .allocate = .alloc_always });
    for (state.resources) |resource| if (resource.kind == .database and std.mem.eql(u8, resource.name, root.database)) return resource.id;
    return error.DatabaseNotFound;
}

const ParticipantTransport = struct {
    local_worker: distributed.LocalTableWriteParticipantWorker,
    hosted_worker: distributed.HostedParticipantWorker,
    fallback: @import("../../table_router.zig").CatalogBackedGroupRouter,
    hosted: bool,

    fn init(self: *@This(), server: *http.ApiHttpServer) !void {
        const writes = server.table_writes orelse return error.UnsupportedSqlExecution;
        self.fallback = @import("../../table_router.zig").CatalogBackedGroupRouter.init(server.catalogSource(), server.localSessionNodeId());
        self.local_worker = distributed.LocalTableWriteParticipantWorker.init(writes);
        self.hosted = server.cfg.deployment_mode != .standalone;
        if (self.hosted) {
            self.hosted_worker = distributed.HostedParticipantWorker.init(server.catalogSource(), server.cfg.session_router orelse self.fallback.router(), writes, server.cfg.session_executor orelse return error.UnsupportedSqlExecution);
            _ = self.hosted_worker.withInternalServiceAuth(server.cfg.internal_service_secret, server.cfg.internal_service_issuer);
        }
    }
    fn worker(self: *@This()) distributed.ParticipantWorker {
        return if (self.hosted) self.hosted_worker.worker() else self.local_worker.worker();
    }
};

/// Empty cohorts still have a durable decision and scope-release barrier.
/// They require no storage transport and must remain recoverable on nodes
/// without a local table writer. At most one decision and one handoff per pass.
fn recoverEmptyCohort(source: *Registry, a: A, before: registry.Record, now_ns: u64) !registry.Record {
    if (before.participants.len != 0) return error.InvalidTxnRequest;
    var record = before;
    if (registry.recoveryDecision(record, now_ns)) |decision| {
        const version = try distributed.commitVersion(record.identity.begin_timestamp);
        record = try Registry.advance(source, a, record, switch (decision) {
            .abort => .{ .abort = version },
            .commit => .{ .commit = .{ .version = version, .sync_level = record.identity.commit_sync_level } },
        });
    }
    if (record.pendingCatalogCompletion()) |outcome| {
        const progressed = try distributed.recoverCatalogCompletion(a, source.authority(), record, outcome);
        if (progressed.revision == record.revision) return record;
        record = progressed;
    }
    if (record.phase == .locking) record = try Registry.advance(source, a, record, .lock_handoff);
    if (record.phase == .resolved and record.hasPreparedLocks() and !record.locks_released) record = try Registry.advance(source, a, record, .lock_release);
    if (record.phase == .resolved and (!record.hasPreparedLocks() or record.locks_released)) record = try Registry.advance(source, a, record, .handoff);
    return record;
}

/// Scoped COMMIT uses the existing external decision graph. The local seal is
/// durable before native admission; every later error is reconciliation debt,
/// never permission to run the ordinary commit coordinator under the same ID.
pub fn commitScoped(server: *http.ApiHttpServer, user: ?http.AuthenticatedIdentity, context: local.api_operation.RequestContext, id: sql_session.Id, tables: []const distributed.TableCommitRequest, abort: bool) !sql_session.CommitResult {
    return completeScoped(server, user, context, id, tables, abort, false);
}

/// Retire an effect-free producer's unused admission, not its parent command.
/// Canonical cancellation fences the original sealed purpose permanently;
/// release_unadmitted drains its native locks with decision=null and therefore
/// retains the enclosing command fence until the DDL owner actually finishes.
pub fn retirePopulationRead(server: *http.ApiHttpServer, user: ?http.AuthenticatedIdentity, context: local.api_operation.RequestContext, id: sql_session.Id) !sql_session.CommitResult {
    const lease = server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
    defer lease.release();
    const durable = server.txn_sessions.durable orelse return error.SqlPreparedDurabilityUnavailable;
    var state = (try durable.loadInto(server.alloc, id)) orelse return error.SqlTransactionNotActive;
    defer state.deinit(server.alloc);
    if (!try execution.Adapter.populationReadOnly(server.alloc, state)) return error.UnsupportedSqlExecution;
    return completeScoped(server, user, context, id, &.{}, false, true);
}

fn completeScoped(server: *http.ApiHttpServer, user: ?http.AuthenticatedIdentity, context: local.api_operation.RequestContext, id: sql_session.Id, tables: []const distributed.TableCommitRequest, abort: bool, retire_read: bool) !sql_session.CommitResult {
    const frame = context.sql_command_fence orelse return error.InvalidLockScope;
    if (abort and tables.len != 0) return error.InvalidTxnRequest;
    if (server.txn_sessions.durable == null) return error.SqlPreparedDurabilityUnavailable;
    const info = server.txn_sessions.getInfo(id) orelse return error.SqlTransactionNotActive;
    var state = (try server.txn_sessions.getSqlState(server.alloc, id)) orelse return error.SqlTransactionNotActive;
    defer state.deinit(server.alloc);
    const connection_id = state.connection_id orelse return error.SqlConnectionNotFound;
    if (!abort and !state.volatile_payload_available) {
        diagnoseScopedFailure(.payload, error.SqlRelationRecoveryIncomplete, null);
        return error.SqlRelationRecoveryIncomplete;
    }
    if (state.owner_node_id != server.localSessionNodeId() or (state.metadata.accepted_lake_reads and tables.len != 0)) return error.UnsupportedSqlExecution;
    var region = std.heap.ArenaAllocator.init(server.alloc);
    defer region.deinit();
    const a = region.allocator();
    var current_user = user;
    var root: execution.Adapter = .{ .server = server, .identity = &current_user, .context = context, .database = state.metadata.database };
    var source: Registry = .{ .server = server, .context = context, .database_id = try databaseIdentity(&root, a) };
    if (source.database_id != frame.database) return error.InvalidLockScope;
    const captured_tables = try @import("../catalog/relation_lifetimes.zig").attachOwnerMinters(server, a, context, tables);
    var cohort = distributed.ExternalPreparation.init(a, server.catalogSource(), captured_tables) catch |err| {
        diagnoseScopedFailure(.cohort, err, @errorReturnTrace());
        return err;
    };
    defer cohort.deinit(a);
    @import("data_locks.zig").attachPreparation(server, a, context, id, &cohort) catch |err| {
        diagnoseScopedFailure(.lock_cohort, err, @errorReturnTrace());
        return err;
    };
    var transport: ParticipantTransport = undefined;
    if (cohort.manifest.len != 0) try transport.init(server);
    const actor = try @import("../recovery/authorization_recovery.zig").actor(server, user);
    source.actor = actor;
    var operation_id: [16]u8 = undefined;
    try server.embedding_provider_runtime.io.randomSecure(&operation_id);
    const cut = try source.snapshot(a, .{ .operation = operation_id });
    if (cut.standalone != frame.standalone or !std.mem.eql(u8, &cut.incarnation, &frame.backend.incarnation)) return error.CatalogGenerationChanged;
    try source.bindPublication(a, cut, id, &cohort);
    const now = @import("antfly_platform").time.realtimeNs();
    const pending = &state.metadata.catalog_generations;
    var generation_session: ?transactions.Session = null;
    defer if (generation_session) |*session| session.deinit(a);
    var generation_cohort: ?Registry.GenerationRows.Cohort = null;
    defer if (generation_cohort) |*owned| owned.deinit();
    if (!abort and pending.entries.items.len != 0) {
        generation_session = (try server.txn_sessions.durable.?.loadInto(a, id)) orelse return error.SqlTransactionNotActive;
        for (pending.entries.items) |entry|
            try @import("../catalog/catalog_auth.zig").authorizeTarget(server, user, a, entry.target);
        const entry = pending.entries.items[0];
        const context_read = source.publicationContext();
        try Registry.GenerationRows.validateCurrentRows(a, &generation_session.?, context_read);
        const bytes = try server.source.systemCatalog(a, context_read, .{ .read = .{ .kind = .namespace, .database = entry.target.database, .name = entry.target.namespace } });
        defer a.free(bytes);
        const catalog = try std.json.parseFromSliceLeaky(struct { revision: u64 }, a, bytes, .{ .ignore_unknown_fields = true });
        const staging_id = try Registry.CatalogStages.idForAttempt(@max(1, std.mem.readInt(u64, operation_id[0..8], .little)), 1);
        const binding: registry.CatalogBinding = .{ .fence = frame.*, .transaction = id, .operation = operation_id, .intent_digest = pending.digest() };
        const routed: ?@import("../../generation_handoff_preview.zig").Routed = if (server.table_reads) |reads| .{ .reads = reads, .context = context_read } else null;
        const predecessors = if (routed) |*owner| owner.source() else null;
        generation_cohort = try Registry.GenerationRows.Cohort.initWithPredecessors(a, &generation_session.?, staging_id, binding, .{
            .capture_id = operation_id,
            .definition = @splat(1),
            .source_definition = @splat(1),
            .target_schema = @splat(1),
            .principal_name = if (user) |identity| identity.username else "",
            .principal_instance = if (user) |identity| identity.user_instance_id orelse return error.Forbidden else operation_id,
            .catalog_revision = catalog.revision,
            .write_version = @max(1, now),
            .expires_at_ms = now / std.time.ns_per_ms +| 5 * std.time.ms_per_min,
        }, predecessors);
        try generation_cohort.?.plan.validateCatalog(a, frame.group, pending);
        for (generation_cohort.?.plan.targets) |target| for (tables) |table| {
            if (std.mem.eql(u8, table.table_name, target.table.name)) return error.InvalidTxnRequest;
        };
    }
    const encoded = if (generation_cohort) |owned|
        try transactions.encodeCatalogExecutionPlan(a, .{ .tables = tables, .catalog = owned.plan })
    else
        try transactions.encodeExecutionPlan(a, tables);
    const notification_plan: ?local.sql_ast.notifications.Plan = if (state.metadata.notifications.commands.items.len == 0) null else notification: {
        var connection = try @import("connections.zig").loadWithCommand(server.txn_sessions.durable.?, a, connection_id, http.transactionPrincipal(user) orelse "", server.localSessionNodeId(), now / std.time.ns_per_ms, context.sql_command_fence);
        defer connection.deinit();
        break :notification .{ .connection = connection_id, .generation = connection.value.generation, .sender_pid = connection.value.notificationPid(), .commands = state.metadata.notifications.commands.items, .discard = abort };
    };
    const identity: registry.Identity = .{
        .catalog_participant = if (generation_cohort) |owned| owned.participant else null,
        .notification_plan = notification_plan,
        .kind = .session_commit,
        .lock_scope = .{ .backend = frame.backend, .command = frame.command },
        .commit_sync_level = if (info.sync_level == .propose) .write else info.sync_level,
        .session = .{ .owner_node_id = state.owner_node_id, .connection_id = connection_id, .standalone = cut.standalone },
        .gid = "",
        .database_id = source.database_id,
        .native_service = context.native_service,
        .principal = actor.principal(),
        .txn_id = id,
        .operation_id = operation_id,
        .metadata_incarnation = std.fmt.bytesToHex(cut.incarnation, .lower),
        .begin_timestamp = info.begin_timestamp,
        .expires_at_unix_ns = try std.math.add(u64, now, 5 * std.time.ns_per_min),
        .plan_digest = try registry.planDigest(encoded),
        .manifest_digest = try registry.manifestDigest(a, cohort.manifest, std.fmt.bytesToHex(cut.incarnation, .lower)),
    };
    const seal: transactions.ExternalPreparationSeal = .{ .admission_head = cut.head, .admitted_at_ns = now, .standalone = cut.standalone, .identity = identity, .actor = actor, .participants = cohort.manifest };
    if (context.sql_completion) |completion| try completion.prepare_transaction(completion.ptr, id);
    var plan = (try server.txn_sessions.sealExternalEncodedExecutionPlan(server.alloc, id, seal, encoded)) orelse return error.SqlTransactionNotActive;
    defer plan.deinit();
    // A zero-row transaction can still own transaction locks. It traverses the
    // same durable decision and release barriers, without requiring a fictitious
    // table participant or constructing a transport that will never be called.
    deliver: {
        if (retire_read) break :deliver;
        if (generation_cohort) |*owned| {
            // The metadata writer reserves the generation together with
            // admission. Only the retained result can start owner writes.
            _ = Registry.admit(&source, a, identity, cohort.manifest) catch |err| {
                diagnoseScopedFailure(.admit, err, @errorReturnTrace());
                break :deliver;
            };
            source.prepareCatalogOwners(a, owned) catch |err| {
                diagnoseScopedFailure(.prepare, err, @errorReturnTrace());
                break :deliver;
            };
        }
        var record = if (abort or cohort.manifest.len == 0)
            Registry.admit(&source, a, identity, cohort.manifest) catch |err| {
                diagnoseScopedFailure(.admit, err, @errorReturnTrace());
                break :deliver;
            }
        else
            cohort.prepare(a, transport.worker(), source.authority(), identity, .{ .fanout_io = server.sharedApiIo(), .external_deadline_ns = context.deadline_ns }) catch |err| {
                diagnoseScopedFailure(.prepare, err, @errorReturnTrace());
                break :deliver;
            };
        if (cohort.manifest.len == 0) {
            if (abort) {
                record = Registry.advance(&source, a, record, .{ .abort = distributed.commitVersion(info.begin_timestamp) catch break :deliver }) catch break :deliver;
            } else {
                record = Registry.advance(&source, a, record, .prepare) catch break :deliver;
            }
            _ = recoverEmptyCohort(&source, a, record, now) catch break :deliver;
        } else {
            if (abort) record = Registry.advance(&source, a, record, .{ .abort = distributed.commitVersion(info.begin_timestamp) catch break :deliver }) catch break :deliver;
            _ = distributed.recoverExternalTransaction(a, transport.worker(), source.authority(), operation_id, now, .{ .fanout_io = server.sharedApiIo(), .external_deadline_ns = context.deadline_ns }) catch break :deliver;
        }
    }
    const bytes = (server.txn_sessions.externalPreparationSeal(server.alloc, id) catch return .{ .outcome = .unknown, .reconciliation_id = id }) orelse return .{ .outcome = .unknown, .reconciliation_id = id };
    defer server.alloc.free(bytes);
    // Exact native receipt adoption persists the terminal result before ACK.
    // Lost admission/reply and incomplete lock cleanup are owned by maintenance.
    drainSealedSessionCompletion(server, id, bytes, context) catch |err| {
        // A deadline or lost authority still leaves the same sealed purpose
        // pending. Only an authenticated terminal journal below is definitive.
        diagnoseScopedFailure(.session, err, @errorReturnTrace());
    };
    // Read the retained seal and terminal decision from ONE durable session
    // observation. externalPreparationSeal is deliberately admission-only and
    // refuses terminal sessions; it cannot be used to observe completion.
    var observed = (server.txn_sessions.durable.?.loadInto(server.alloc, id) catch return .{ .outcome = .unknown, .reconciliation_id = id }) orelse return .{ .outcome = .unknown, .reconciliation_id = id };
    defer observed.deinit(server.alloc);
    const terminal = session_completion.sealedTerminalOutcome(observed, bytes);
    if (terminal == .unknown) return .{ .outcome = .unknown, .reconciliation_id = id };
    // A terminal decision is durable before native lock retirement. Keep the
    // SQL command loan until its exact completion is acknowledged: otherwise a
    // successful ROLLBACK/COMMIT can leave locks visible to the next command.
    drainSessionCompletion(server, id, context) catch return .{
        // A namespace change must be visible before SQL emits COMMIT. The
        // sealed decision remains authoritative and can be reconciled by ID.
        .outcome = if (terminal == .aborted or generation_cohort != null) .unknown else .committed_pending,
        .reconciliation_id = id,
    };
    if (generation_cohort != null and terminal == .committed_pending)
        return .{ .outcome = .unknown, .reconciliation_id = id };
    return .{ .outcome = switch (terminal) {
        .aborted => .aborted,
        .committed => .committed,
        .committed_pending => .committed_pending,
        .committed_repair_required => .committed_repair_required,
        .unknown => unreachable,
    }, .reconciliation_id = id };
}

/// Session ownership, current schema/RLS, predicates and FK/UNIQUE preparation
/// use the ordinary transaction pipeline. Only the durable decision authority
/// and the terminal SQL boundary differ from COMMIT.
pub fn execute(root: *execution.Adapter, session: *sql_session.Session, coordinator: *@import("session.zig").Coordinator, a: A, command: ast.PreparedTransaction) !local.sql_runtime.Result {
    const server = root.server;
    const tag = switch (command.action) {
        .prepare => "PREPARE TRANSACTION",
        .commit => "COMMIT PREPARED",
        .rollback => "ROLLBACK PREPARED",
    };
    var result = try local.sql_runtime.Result.empty(a, tag);
    errdefer result.deinit();
    if (command.action != .prepare) {
        if (session.transaction_id != null) return error.SqlPreparedInsideTransaction;
        try registry.validateGid(command.gid);
        var region = std.heap.ArenaAllocator.init(server.alloc);
        defer region.deinit();
        const scratch = region.allocator();
        const actor = try @import("../recovery/authorization_recovery.zig").actor(server, root.identity.*);
        var source: Registry = .{ .server = server, .context = root.context, .actor = actor, .database_id = try databaseIdentity(root, scratch), .native_admin = try http.resourcePermissionCurrentlyAllowed(root.identity.*, .user, "*", .admin) };
        const cut = try source.snapshot(scratch, .{ .gid = command.gid });
        if (cut.records.len != 1) return error.SqlPreparedTransactionNotFound;
        var record = cut.records[0];
        if (record.identity.database_id != source.database_id) return error.SqlPreparedTransactionWrongDatabase;
        try local.api_operation.requireNativeServiceAccess(record.identity.native_service, root.context.native_service);
        if (!source.native_admin and actor.principal().kind != .bootstrap and !record.identity.principal.eql(actor.principal())) return error.Forbidden;
        if (record.phase == .preparing or record.phase == .locking) return error.SqlPreparedTransactionNotFound;
        const committed = command.action == .commit;
        if (record.phase == .prepared) {
            const version = try distributed.commitVersion(record.identity.begin_timestamp);
            record = Registry.advance(&source, scratch, record, if (committed) .{ .commit = .{ .version = version, .sync_level = try local.sql_setting_catalog.mutationSyncLevel(root.setting_overlay) } } else .{ .abort = version }) catch return error.SqlTransactionOutcomeUnknown;
        }
        if (record.decision == null or (record.decision.?.status == .committed) != committed) return error.ExternalTransactionDecisionChanged;
        root.outcome_transaction_id = std.fmt.bytesToHex(record.identity.txn_id, .lower);
        var transport: ParticipantTransport = undefined;
        transport.init(server) catch return error.SqlTransactionOutcomeUnknown;
        const time = @import("antfly_platform").time;
        const deadline = root.context.deadline_ns orelse time.monotonicNs() +| 30 * std.time.ns_per_s;
        source.context.deadline_ns = deadline;
        transport.local_worker.recovery_deadline_ns = deadline;
        if (transport.hosted) transport.hosted_worker.recovery_deadline_ns = deadline;
        while (true) {
            root.context.ensureActive() catch return error.SqlTransactionOutcomeUnknown;
            if (time.monotonicNs() >= deadline) return error.SqlTransactionOutcomeUnknown;
            const finished = pass: {
                // Reclaim each pass's snapshots and transport allocations. A
                // large lock set must not accumulate one arena per quantum.
                var pass_region = std.heap.ArenaAllocator.init(server.alloc);
                defer pass_region.deinit();
                const temporary = pass_region.allocator();
                const advanced = distributed.recoverExternalTransaction(temporary, transport.worker(), source.authority(), record.identity.operation_id, time.realtimeNs(), .{ .fanout_io = server.sharedApiIo(), .external_deadline_ns = deadline }) catch |err| switch (err) {
                    // The recovery driver yields after eight durable quanta. Its
                    // incomplete-decision result is resumable for this exact
                    // admitted operation, including lock-only participants.
                    error.CommitDecisionUnknown, error.CatalogGenerationChanged, error.MetadataMutationOutcomeUnknown, error.CatalogPublicationProofPending => break :pass false,
                    error.SqlPreparedTransactionNotFound => {
                        // Another controller can finish this exact admitted
                        // decision. Only absence in the same durable realm is
                        // its retirement witness; SQL/GID lookup is not replayed.
                        const completed = try source.snapshot(temporary, .{ .operation = record.identity.operation_id });
                        try Registry.checkRealm(completed, record.identity);
                        if (completed.records.len != 0) break :pass false;
                        break :pass true;
                    },
                    else => {
                        std.log.warn("prepared SQL resolution rejected err={s}", .{@errorName(err)});
                        return error.SqlTransactionOutcomeUnknown;
                    },
                };
                break :pass advanced.phase == .retired;
            };
            if (finished) break;
            try server.embedding_provider_runtime.io.sleep(.fromMilliseconds(1), .awake);
        }
        if (committed) result.output.mutation_outcome = .committed;
        return result;
    }

    const id = session.transaction_id orelse {
        result.output.command_tag = "ROLLBACK";
        if (root.notice_sink) |sink| try sink.emit(sink.ptr, .{ .code = "25P01", .severity = .warning, .message = "there is no transaction in progress" });
        return result;
    };
    const lease = server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
    defer lease.release();
    const prior_execution = coordinator.borrowed_execution;
    defer coordinator.borrowed_execution = prior_execution;
    coordinator.borrowed_execution = &lease;
    var sealed = false;
    errdefer if (!sealed) {
        if (session.owner.vtable.rollback(session.owner.ptr, session.scope, id)) |_| {
            session.transaction_id = null;
        } else |_| {
            root.outcome_transaction_id = std.fmt.bytesToHex(id, .lower);
        }
    };
    const current = try session.owner.vtable.inspect(session.owner.ptr, session.scope, id);
    if (current.state == .failed) {
        try session.owner.vtable.rollback(session.owner.ptr, session.scope, id);
        session.transaction_id = null;
        result.output.command_tag = "ROLLBACK";
        return result;
    }
    if (current.state != .active) {
        sealed = true;
        return error.SqlTransactionOutcomeUnknown;
    }
    try registry.validateGid(command.gid);
    if (server.txn_sessions.durable == null) return error.SqlPreparedDurabilityUnavailable;
    const info = server.txn_sessions.getInfo(id) orelse return error.SqlTransactionNotActive;
    var state = (try server.txn_sessions.getSqlState(server.alloc, id)) orelse return error.SqlTransactionNotActive;
    defer state.deinit(server.alloc);
    if (state.metadata.accepted_lake_reads or state.metadata.temporary_relation_access) return error.UnsupportedSqlExecution;
    if (state.metadata.notifications.commands.items.len != 0) return error.UnsupportedSqlExecution;
    try root.validateAcceptedSerializable(server.alloc, id);
    var request = try server.txn_sessions.cloneCommitRequest(server.alloc, id, null);
    defer if (request) |*value| value.deinit(server.alloc);
    // A named prepared transaction may outlive its SQL connection. PostgreSQL
    // refuses TEMP dependencies, including reads with no mutation participant.
    if (request) |value| for (value.catalog_bindings.items) |binding| {
        if (binding.sql_persistence) |persistence| if (persistence.kind == .temporary)
            return error.UnsupportedSqlExecution;
    };
    var region = std.heap.ArenaAllocator.init(server.alloc);
    defer region.deinit();
    const scratch = region.allocator();
    var sql_scope = if (request) |value| try @import("../execution/authorization_runtime.zig").TransactionScope.init(server, root.identity.*, root.context, scratch, value) else null;
    defer if (sql_scope) |*authority_owner| authority_owner.deinit();
    if (request) |value| if (!try server.transactionRequestAuthorizedWithSql(root.identity.*, value, if (sql_scope) |*scope| scope else null)) return error.Forbidden;
    const commit_context = if (sql_scope) |*scope| scope.context() else root.context;
    const view = if (request) |value| try value.distributedTables(scratch) else null;
    defer if (view) |value| value.deinit();
    const tables: []const distributed.TableCommitRequest = if (view) |value| value.tables else &.{};
    try server.validateCommitTablesAgainstSchema(root.context, tables);
    if (request) |value| if (try server.validateCommitReadSet(value) != null) return error.SqlWriteConflict;
    var prepared = try server.preparePublicCommitWithIntegrity(scratch, tables, commit_context);
    defer prepared.deinit();
    const signed = try server.signCurrentRowPolicyMutationParticipants(scratch, prepared.tables, commit_context);
    var cohort = try distributed.ExternalPreparation.init(scratch, server.catalogSource(), signed);
    defer cohort.deinit(scratch);
    try @import("data_locks.zig").attachPreparation(server, scratch, root.context, id, &cohort);
    var transport: ParticipantTransport = undefined;
    try transport.init(server);
    const encoded = try transactions.encodeExecutionPlan(server.alloc, signed);
    defer server.alloc.free(encoded);
    var source: Registry = .{ .server = server, .context = root.context, .database_id = try databaseIdentity(root, scratch) };
    const cut = try source.snapshot(scratch, .{ .gid = command.gid });
    try source.bindPublication(scratch, cut, id, &cohort);
    const actor = try @import("../recovery/authorization_recovery.zig").actor(server, root.identity.*);
    var operation_id: [16]u8 = undefined;
    try server.embedding_provider_runtime.io.randomSecure(&operation_id);
    const now = @import("antfly_platform").time.realtimeNs();
    const identity: registry.Identity = .{ .lock_scope = if (root.context.sql_command_fence) |fence| .{ .backend = fence.backend, .command = fence.command } else null, .session = .{ .owner_node_id = server.localSessionNodeId(), .connection_id = state.connection_id, .standalone = cut.standalone }, .gid = command.gid, .database_id = source.database_id, .native_service = root.context.native_service, .principal = actor.principal(), .txn_id = id, .operation_id = operation_id, .metadata_incarnation = std.fmt.bytesToHex(cut.incarnation, .lower), .begin_timestamp = info.begin_timestamp, .expires_at_unix_ns = try std.math.add(u64, now, 5 * std.time.ns_per_min), .plan_digest = try registry.planDigest(encoded), .manifest_digest = try registry.manifestDigest(scratch, cohort.manifest, std.fmt.bytesToHex(cut.incarnation, .lower)) };
    const seal: transactions.ExternalPreparationSeal = .{ .admission_head = cut.head, .admitted_at_ns = now, .standalone = cut.standalone, .identity = identity, .actor = actor, .participants = cohort.manifest };
    var plan = (try server.txn_sessions.sealExternalEncodedExecutionPlan(server.alloc, id, seal, encoded)) orelse return error.SqlTransactionNotActive;
    defer plan.deinit();
    sealed = true;
    root.outcome_transaction_id = std.fmt.bytesToHex(id, .lower);
    _ = cohort.prepare(scratch, transport.worker(), source.authority(), identity, .{ .fanout_io = server.sharedApiIo() }) catch |err| {
        // A definite original rejection can close immediately. An admitted
        // cohort remains metadata-owned recovery debt, never ordinary abort.
        const owned_seal = (try server.txn_sessions.externalPreparationSeal(server.alloc, id)) orelse return error.SqlTransactionOutcomeUnknown;
        defer server.alloc.free(owned_seal);
        recoverSessionWithContext(server, id, owned_seal, false, false, false, root.context) catch {};
        var observed = (try server.txn_sessions.getSqlState(server.alloc, id)) orelse return error.SqlTransactionOutcomeUnknown;
        defer observed.deinit(server.alloc);
        if (observed.terminal_abort) {
            // Exact cancellation proved this immutable PREPARE was never
            // admitted. Complete its durable connection detachment before
            // advertising idle/no reconciliation ID. A lost detach reply
            // remains owned by this same aborted session, never a SQL retry.
            if (root.connection_id != null)
                session.owner.vtable.rollback(session.owner.ptr, session.scope, id) catch return error.SqlTransactionOutcomeUnknown;
            session.transaction_id = null;
            root.outcome_transaction_id = null;
            return err;
        }
        if (!observed.terminal_prepare) {
            std.log.warn("SQL transaction preparation requires reconciliation txn={s} err={s}", .{ std.fmt.bytesToHex(id, .lower), @errorName(err) });
            return error.SqlTransactionOutcomeUnknown;
        }
        session.transaction_id = null;
        return result;
    };
    const delivered = try source.snapshot(scratch, .{ .receipt = operation_id });
    if (delivered.receipts.len != 1 or delivered.receipts[0].status != .prepared) return error.SqlTransactionOutcomeUnknown;
    _ = (try server.txn_sessions.recordExternalPreparationReceipt(server.alloc, id, delivered.receipts[0])) orelse return error.SqlTransactionOutcomeUnknown;
    // SQL/HTTP connections reconcile from the durable distinct prepared state.
    // Metadata outbox ACK delivery is independently retryable maintenance.
    session.transaction_id = null;
    return result;
}
