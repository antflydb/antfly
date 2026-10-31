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
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const db_mod = @import("../db.zig");
const DB = db_mod.DB;

/// Query state owned by `DB`: schema identity, the retained query cut, native
/// query warming, and row policy authority and publication.
pub const Query = struct {
    warming: @import("../native_query_warming.zig").Owner = .{},
    schema: Schema = .{},
    cut: Cut = .{},
    row_policy: RowPolicy = .{},

    /// Query schema identity, and the resolver's cached schema digest for that identity.
    pub const Schema = struct {
        identity: std.atomic.Value(u64) = .init(0),
        resolver_mutex: std.atomic.Mutex = .unlocked,
        resolver_epoch: ?DB.ResolverQuerySchemaEpoch = null,
        resolver_digest: [32]u8 = undefined,
    };

    /// The immutable query cut a read-only DB was opened at. Installed only after
    /// query-cut admission; never persisted.
    pub const Cut = struct {
        value_scope: ?@import("../../../search/value_membership.zig").Scope = null,
        lease: ?@import("../native_query_cut.zig").Guard = null,
        remote_lease: ?@import("../native_query_cut_repository.zig").StorageLease = null,
    };

    /// Row policy verifier retained by monolithic managed opens until workers drain.
    pub const RowPolicy = struct {
        authority_secret: ?[]u8 = null,
        authority_issuer: ?[]u8 = null,
        table_name: ?[]u8 = null,
    };

    fn dbPtr(self: *Query) *DB {
        return @alignCast(@fieldParentPtr("query", self));
    }

    /// A process-local incarnation prevents epoch ABA across reopened or
    /// refreshed immutable DB snapshots, including allocator address reuse.
    pub fn schemaIdentity(self: *Query) u64 {
        const existing = self.schema.identity.load(.acquire);
        if (existing != 0) return existing;
        const fresh = db_mod.next_query_schema_identity.fetchAdd(1, .monotonic);
        std.debug.assert(fresh != 0);
        return self.schema.identity.cmpxchgStrong(0, fresh, .acq_rel, .acquire) orelse fresh;
    }

    pub fn openCut(self: *Query, request: @import("../native_query_cut.zig").Request, cancellation: db_mod.types.CancellationToken) !DB {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("openQueryCut", .{ db, request, cancellation });
        const cut = @import("../native_query_cut.zig");
        try request.validate(cut.nowMs());
        const origin = try request.namespace(db.core.identity_namespace);
        const cache_id = try request.cacheId(db.alloc, db.core.identity_namespace);
        defer db.alloc.free(cache_id);
        if (request.create) try db.captureQueryCut(request, cancellation);
        const io = db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
        const path = try cut.pathAlloc(db.alloc, db.core.path, cache_id);
        defer db.alloc.free(path);
        const expiry_deadline = db_mod.monotonicTimeNs() +| (request.expires_ms -| cut.nowMs()) * std.time.ns_per_ms;
        const control: cut.Control = .{ .parent = cancellation, .deadline_ns = if (request.timeout_ms) |value| @min(db_mod.monotonicTimeNs() +| value *| std.time.ns_per_ms, expiry_deadline) else expiry_deadline };
        var guard = try cut.lock(db.alloc, io, db.core.path, control.token());
        var guarded = true;
        defer if (guarded) guard.deinit();
        var source_proof: ?[32]u8 = null;
        const local_exists = try db_mod.snapshotPathExists(io, path);
        const local_usable = if (local_exists) validated: {
            // Local files are a cache of the retained generation. Validate
            // under the parent guard before trusting directory existence.
            source_proof = cut.validateWithSourceProof(db.alloc, io, path, request, origin, control.token()) catch |err| switch (err) {
                error.CatalogGenerationChanged => break :validated false,
                else => return err,
            };
            break :validated true;
        } else false;
        if (!local_usable) if (db.backend_runtime.query_cut_repository) |repository| {
            // The read lease retains the caller's token beyond this stack frame.
            if (try repository.openRead(io, path, request, origin, cancellation)) |opened| {
                var lease = opened;
                errdefer lease.deinit();
                // The remote manifest is immutable. No local capture or
                // collection can change it, so opening pages must not hold
                // the filesystem publication guard across provider I/O.
                guard.deinit();
                guarded = false;
                var readonly = try DB.open(db.alloc, path, .{
                    .open_mode = .query_readonly,
                    .physical_root_mode = .external_backend,
                    .storage = lease.view,
                    .backend_runtime = db.backend_runtime,
                    .identity_namespace = origin,
                });
                errdefer readonly.close();
                try control.token().check();
                readonly.query.cut.value_scope = @import("../../../search/value_membership.zig").Scope.init(db.core.path, request.id, origin, request.expires_ms);
                readonly.query.cut.remote_lease = lease;
                return readonly;
            }
            // Retained files are authority; local files are a disposable cache.
            // Admission alone cannot detect a missing sealed file behind a valid
            // manifest. Protect active readers before replacing any invalid root.
            if (local_exists) try cut.discardInvalidCache(db.alloc, io, std.fs.path.dirname(path).?, cache_id, path);
            try db.admitQueryCut(io, cache_id, control.token());
            try repository.recover(io, path, request, origin, control.token());
            errdefer std.Io.Dir.cwd().deleteTree(io, path) catch {};
            try db.admitQueryCut(io, cache_id, control.token());
            source_proof = try cut.validateWithSourceProof(db.alloc, io, path, request, origin, control.token());
        } else return error.CatalogGenerationChanged;
        var reader = try cut.readLease(db.alloc, io, db.core.path, cache_id, control.token());
        guard.deinit();
        guarded = false;
        var transferred = false;
        defer if (!transferred) reader.deinit();
        var readonly = try DB.open(db.alloc, path, .{
            .open_mode = .query_readonly,
            .backend_runtime = db.backend_runtime,
            .identity_namespace = origin,
            .prefer_existing_identity_namespace = true,
        });
        errdefer readonly.close();
        try control.token().check();
        readonly.query.cut.value_scope = @import("../../../search/value_membership.zig").Scope.init(db.core.path, request.id, origin, request.expires_ms).withSourceProof(source_proof);
        readonly.query.cut.lease = reader;
        transferred = true;
        return readonly;
    }

    /// Apply an exact metadata-read-index policy publication through the data
    /// Raft log. The ingress must obtain `bundle_bytes` from the provisioned
    /// metadata leader callback, never from a user request. The bundle,
    /// fail-closed preparing catalog, pending receipt intent, and applied
    /// marker commit atomically on every replica. Receipt publication is a
    /// separate owner-local completion after old leases drain; no metadata ACK
    /// is available until that completion is durable.
    fn readRowPolicyReceiptLocked(self: *Query, expected: db_mod.row_policy_bundle_mod.Receipt) !db_mod.row_policy_bundle_mod.Receipt {
        const db = self.dbPtr();
        var key_buf: [128]u8 = undefined;
        const key = try expected.key(&key_buf);
        const bytes = db.core.store.get(db.alloc, key) catch return error.InvalidRowPolicyReceipt;
        defer db.alloc.free(bytes);
        const stored = try db_mod.row_policy_bundle_mod.Receipt.decode(bytes);
        if (!std.meta.eql(expected, stored)) return error.InvalidRowPolicyReceipt;
        return stored;
    }

    fn readRowPolicyApplyResultLocked(self: *Query, expected: db_mod.row_policy_bundle_mod.Receipt) !?db_mod.row_policy_bundle_mod.Receipt {
        const db = self.dbPtr();
        const receipt = self.readRowPolicyReceiptLocked(expected) catch |err| switch (err) {
            error.InvalidRowPolicyReceipt => null,
            else => return err,
        };
        if (receipt) |stored| return stored;
        const pending = (try db.pendingRowPolicyReceipt()) orelse return error.InvalidRowPolicyReceipt;
        if (!std.meta.eql(pending, expected)) return error.RowPolicyCatalogChanged;
        return null;
    }

    /// Private owner status query. A coordinator reads this only after the
    /// data-Raft proposal has committed; it is never a substitute for the
    /// metadata read-index bundle or an owner ACK. It exposes no user rows.
    pub fn loadRowPolicyReceipt(self: *Query, generation: u64, phase: @import("../../../system_catalog/policies.zig").Publication.Phase) !db_mod.row_policy_bundle_mod.Receipt {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("loadRowPolicyReceipt", .{ db, generation, phase });
        if (generation == 0) return error.InvalidRowPolicyReceipt;
        var key_buf: [128]u8 = undefined;
        const identity: db_mod.row_policy_bundle_mod.Receipt = .{
            .table_id = db.core.identity_namespace.table_id,
            .generation = generation,
            .catalog_epoch = 0,
            .phase = phase,
            .applied_term = 0,
            .applied_index = 0,
            .bundle_digest = @splat(0),
            .descriptor_digest = @splat(0),
        };
        const key = try identity.key(&key_buf);
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        const bytes = db.core.store.get(db.alloc, key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (bytes) |encoded| {
            defer db.alloc.free(encoded);
            const stored = try db_mod.row_policy_bundle_mod.Receipt.decode(encoded);
            if (stored.table_id != identity.table_id or stored.generation != generation or stored.phase != phase)
                return error.InvalidRowPolicyReceipt;
            return stored;
        }
        const pending = (try db.pendingRowPolicyReceipt()) orelse return error.NotFound;
        if (pending.table_id != identity.table_id or pending.generation != generation or pending.phase != phase)
            return error.NotFound;
        return db.finalizePendingRowPolicyReceiptLocked(pending);
    }

    pub fn applyReplicatedRowPolicyPublication(self: *Query, bundle_bytes: []const u8, request: @import("../../../system_catalog/policies.zig").InstallRequest, identity: db_mod.OrderedApplyReceipt) !?db_mod.row_policy_bundle_mod.Receipt {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("applyReplicatedRowPolicyPublication", .{ db, bundle_bytes, request, identity });
        return self.applyRowPolicyPublicationInternal(bundle_bytes, request, identity, null);
    }

    fn finishRowPolicyReplicationCommitLocked(self: *Query, payload: ?[]const u8, outbox_key: ?[]const u8, apply_held: *bool, replication_mutation: *?db_mod.MutationBarrier.ExclusiveLease) !void {
        const db = self.dbPtr();
        const encoded = payload orelse return;
        var ctx = db.batchContext();
        var deferred = db_mod.ReplicationDeferredCommitGates.begin(db_mod.replicationTransitionMutexFromContext(&ctx));
        defer deferred.releaseTransition();
        deferred.append(db_mod.appendReplicationEncodedSchemaMetadataCommitLockedContext(&ctx, encoded) catch {
            db.core.unlockApply();
            apply_held.* = false;
            if (replication_mutation.*) |*lease| lease.release();
            replication_mutation.* = null;
            return error.DurabilityOutcomeUnknown;
        });
        db.core.unlockApply();
        apply_held.* = false;
        if (replication_mutation.*) |*lease| lease.release();
        replication_mutation.* = null;
        deferred.waitForDurabilityAndAuthority(ctx.replication_write_gate) catch return error.DurabilityOutcomeUnknown;
        db.clearDurableReplicationOutbox(outbox_key.?) catch |err| std.log.warn(
            "row policy HA durability acknowledged but outbox cleanup is pending path={s} err={s}",
            .{ db.core.path, @errorName(err) },
        );
    }

    fn applyRowPolicyPublicationInternal(self: *Query, bundle_bytes: []const u8, request: @import("../../../system_catalog/policies.zig").InstallRequest, identity: db_mod.OrderedApplyReceipt, replication_lsn: ?u64) !?db_mod.row_policy_bundle_mod.Receipt {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        if (identity.term == 0 or identity.index == 0 or (replication_lsn != null and replication_lsn.? == 0)) return error.InvalidRowPolicyPublication;
        if (replication_lsn == null and db.local_execution.replication_async_metadata_mirror == null and
            (db.local_execution.replication_async_batch_mirror != null or db.local_execution.replication_write_gate != null)) return error.ReplicationPublisherUnavailable;
        // The policy cut must be serialized with all in-flight primary writes,
        // including writers that passed their fast outbox preflight already.
        // Production hot-standby mirrors share this capture barrier with every mutation.
        if (replication_lsn == null) if (db.local_execution.replication_async_metadata_mirror) |mirror|
            if (mirror.mutation_barrier == null) return error.ReplicationPublisherUnavailable;
        var replication_mutation: ?db_mod.MutationBarrier.ExclusiveLease = null;
        defer if (replication_mutation) |*lease| lease.release();
        if (replication_lsn == null) {
            try db.enforceReplicationWriteGate();
            try db.ensureDurableReplicationStartupBarrier();
            try db.preflightReplicationMetadataSyncCommit();
            if (db.local_execution.replication_async_metadata_mirror) |mirror|
                replication_mutation = mirror.mutation_barrier.?.acquireExclusive();
        }
        var schedule_replication_recovery_on_exit = false;
        defer if (schedule_replication_recovery_on_exit) db.scheduleDurableReplicationOutboxRecovery();
        const replication_payload = if (replication_lsn == null and db.local_execution.replication_async_metadata_mirror != null)
            try db_mod.replication_effects_mod.encodeRowPolicyMetadataMutationAlloc(db.alloc, bundle_bytes, request, identity)
        else
            null;
        defer if (replication_payload) |payload| db.alloc.free(payload);
        const replication_outbox = if (replication_payload) |payload| blk: {
            // This is a WAL search lower bound, not an LSN reservation.
            // Recovery matches the exact payload and owner identity under the
            // transition/log locks even if another publisher appended first.
            const from_lsn = db.local_execution.replication_async_metadata_mirror.?.publisher.nextLsn();
            const encoded = try db_mod.encodeDurableReplicationOutboxAlloc(db.alloc, from_lsn, payload);
            errdefer db.alloc.free(encoded);
            const key = try db_mod.durableReplicationOutboxKeyAlloc(db.alloc, .row_policy, from_lsn, db.core.root_generation, payload);
            break :blk .{ .key = key, .value = encoded };
        } else null;
        defer if (replication_outbox) |outbox| {
            db.alloc.free(outbox.key);
            db.alloc.free(outbox.value);
        };
        const schema = db.core.schema orelse return error.RowPolicyCatalogChanged;
        var installed = try db_mod.row_policy_bundle_mod.Installed.init(db.alloc, bundle_bytes, db.core.identity_namespace.table_id, schema);
        defer installed.deinit();
        if (installed.parsed.value.table_id != request.table_id or
            installed.parsed.value.policy_generation != request.expected_generation or
            installed.parsed.value.catalog_epoch != request.expected_catalog_epoch or
            installed.parsed.value.phase != request.expected_phase)
            return error.RowPolicyCatalogChanged;
        var bundle_digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bundle_bytes, &bundle_digest, .{});
        const expected_receipt: db_mod.row_policy_bundle_mod.Receipt = .{
            .table_id = request.table_id,
            .generation = request.expected_generation,
            .catalog_epoch = request.expected_catalog_epoch,
            .phase = request.expected_phase,
            .applied_term = identity.term,
            .applied_index = identity.index,
            .bundle_digest = bundle_digest,
            .descriptor_digest = request.expected_descriptor_digest,
        };
        if (try db.orderedMutationAlreadyApplied(identity)) {
            db_mod.lockApply(db);
            const already = self.readRowPolicyApplyResultLocked(expected_receipt) catch |err| {
                db.core.unlockApply();
                return err;
            };
            db.core.unlockApply();
            if (replication_lsn) |lsn| try db.recordReplicationApplied(lsn);
            return already;
        }
        const phase = installed.parsed.value.phase;
        const pending_phase = phase == .pending_install or phase == .pending_disable;
        try db.lockApplyForPortableRuntime();
        var apply_held = true;
        defer if (apply_held) db.core.unlockApply();
        if (switch (try db_mod.orderedApplyDisposition(try db_mod.readOrderedApplyReceipt(db.alloc, db.core.store), identity)) {
            .already_applied => true,
            .apply => false,
        }) {
            const already = try self.readRowPolicyApplyResultLocked(expected_receipt);
            db.core.unlockApply();
            apply_held = false;
            if (replication_lsn) |lsn| try db.recordReplicationApplied(lsn);
            return already;
        }
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try db.requireOnlineArtifactCatalogMutableLocked(&read);
            try @import("../relational_integrity_topology.zig").requireUnfenced(&read);
        }
        const stage_with_serving = pending_phase and db.local_execution.row_policy_gate.currentPhase() == .active;
        if (!stage_with_serving and pending_phase) {
            if (db.local_execution.row_policy_gate.currentPhase() != .preparing)
                try db.local_execution.row_policy_gate.beginPreparing(db.local_execution.row_policy_gate.currentPhase());
        } else if (!pending_phase and db.local_execution.row_policy_gate.currentPhase() == .active and
            (phase == .serving_install or phase == .serving_disable))
        {
            try db.local_execution.row_policy_gate.beginPreparing(.active);
        } else if (!stage_with_serving and db.local_execution.row_policy_gate.currentPhase() != .preparing) {
            return error.RowPolicyCatalogChanged;
        }
        if (try db.pendingRowPolicyReceipt()) |prior| {
            // A lagging follower need not have received a coordinator receipt
            // probe for the earlier phase. A later metadata-authorized Raft
            // phase may supersede that fail-closed intent without admitting
            // readers or claiming the earlier local ACK. The latest phase
            // still waits for all old leases before it can serve.
            const next_phase = installed.parsed.value.phase;
            const follows = if (request.expected_generation > prior.generation)
                next_phase == .pending_install or next_phase == .pending_disable
            else if (request.expected_generation == prior.generation)
                switch (prior.phase) {
                    .pending_install => next_phase == .serving_install,
                    .serving_install => next_phase == .active or next_phase == .pending_disable,
                    .pending_disable => next_phase == .serving_disable,
                    .serving_disable => next_phase == .disabled,
                    .active, .disabled => false,
                }
            else
                false;
            if (!follows or prior.table_id != request.table_id or
                !std.mem.eql(u8, &prior.descriptor_digest, &request.expected_descriptor_digest) or
                identity.index <= prior.applied_index) return error.RowPolicyCatalogChanged;
        }
        const previous = db.core.table_catalog;
        const publication = installed.parsed.value;
        const current_range = db.core.byteRange();
        const actual_descriptor_digest = try (@import("../../../system_catalog/policies.zig").OwnerDescriptor{
            .table_id = db.core.identity_namespace.table_id,
            .group_id = request.owner_group_id,
            .shard_id = db.core.identity_namespace.shard_id,
            .range_id = db.core.identity_namespace.range_id,
            .schema_version = publication.schema_version,
            .schema_digest = publication.schema_digest,
            .range_start = current_range.start,
            .range_end = current_range.end,
        }).digest();
        if (!std.mem.eql(u8, &actual_descriptor_digest, &request.expected_descriptor_digest))
            return error.RowPolicyCatalogChanged;
        if (previous.storage_mode != .relational or previous.active_schema_version != publication.schema_version or
            publication.policy_generation < previous.row_policy_generation or
            (publication.policy_generation == previous.row_policy_generation and !stage_with_serving and
                previous.row_policy_phase != .preparing)) return error.RowPolicyCatalogChanged;
        var receipt_bytes: [db_mod.ordered_apply_receipt_value_len]u8 = undefined;
        const receipt = db_mod.orderedApplyReceiptWrite(identity, &receipt_bytes);
        if (stage_with_serving) {
            if (db.local_execution.row_policy_gate.currentPhase() != .active or previous.row_policy_phase != .active or
                publication.policy_generation <= previous.row_policy_generation or
                (try db.pendingRowPolicyReceipt()) != null) return error.RowPolicyCatalogChanged;
            const existing_candidate = db.core.store.get(db.alloc, db_mod.row_policy_bundle_mod.candidate_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (existing_candidate) |value| db.alloc.free(value);
            if (existing_candidate != null) return error.RowPolicyCatalogChanged;
            const encoded_receipt = expected_receipt.encode();
            var receipt_key_buf: [128]u8 = undefined;
            const receipt_key = try expected_receipt.key(&receipt_key_buf);
            var stage_txn = try db.core.store.beginWriteTxn();
            errdefer stage_txn.abort();
            try stage_txn.put(db_mod.row_policy_bundle_mod.candidate_key, bundle_bytes);
            try stage_txn.put(receipt_key, &encoded_receipt);
            try stage_txn.put(receipt.key, receipt.value);
            var replication_lsn_buf: [db_mod.replication_applied_lsn_value_len]u8 = undefined;
            if (replication_lsn) |lsn| {
                const marker = db_mod.replicationAppliedSequenceWrite(lsn, &replication_lsn_buf);
                try stage_txn.put(marker.key, marker.value);
            }
            if (replication_outbox) |outbox| try stage_txn.put(outbox.key, outbox.value);
            if (replication_outbox != null) {
                // Conservative on abort: the next admission performs an
                // apply-fenced empty scan and clears both fast-negative flags.
                db.local_execution.durable_replication_outbox_maybe.store(true, .release);
                db.local_execution.row_policy_replication_outbox_pending.store(true, .release);
            }
            try stage_txn.commit();
            if (replication_outbox != null) {
                schedule_replication_recovery_on_exit = true;
            }
            try self.finishRowPolicyReplicationCommitLocked(replication_payload, if (replication_outbox) |outbox| outbox.key else null, &apply_held, &replication_mutation);
            return expected_receipt;
        }
        if (phase == .serving_install or phase == .serving_disable) {
            const candidate = try db.core.store.get(db.alloc, db_mod.row_policy_bundle_mod.candidate_key);
            defer db.alloc.free(candidate);
            var parsed_candidate = try db_mod.row_policy_bundle_mod.Installed.init(db.alloc, candidate, db.core.identity_namespace.table_id, schema);
            defer parsed_candidate.deinit();
            if (parsed_candidate.parsed.value.table_id != publication.table_id or
                parsed_candidate.parsed.value.policy_generation != publication.policy_generation or
                parsed_candidate.parsed.value.catalog_epoch != publication.catalog_epoch or
                !std.mem.eql(u8, &parsed_candidate.parsed.value.schema_digest, &publication.schema_digest) or
                parsed_candidate.parsed.value.phase != (if (phase == .serving_install)
                    @as(@import("../../../system_catalog/policies.zig").Publication.Phase, .pending_install)
                else
                    .pending_disable))
                return error.RowPolicyCatalogChanged;
            const candidate_definitions = try std.json.Stringify.valueAlloc(db.alloc, .{ .records = parsed_candidate.parsed.value.records, .settings = parsed_candidate.parsed.value.settings }, .{});
            defer db.alloc.free(candidate_definitions);
            const serving_definitions = try std.json.Stringify.valueAlloc(db.alloc, .{ .records = publication.records, .settings = publication.settings }, .{});
            defer db.alloc.free(serving_definitions);
            if (!std.mem.eql(u8, candidate_definitions, serving_definitions)) return error.RowPolicyCatalogChanged;
        }
        var next = previous;
        next.row_policy_generation = publication.policy_generation;
        next.row_policy_catalog_epoch = publication.catalog_epoch;
        next.row_policy_phase = .preparing;
        next.generation +|= 1;
        const catalog_bytes = next.encode();
        const encoded_pending = expected_receipt.encode();
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(db_mod.row_policy_bundle_mod.key, bundle_bytes);
        if (pending_phase) try txn.put(db_mod.row_policy_bundle_mod.candidate_key, bundle_bytes);
        if (phase == .serving_install or phase == .serving_disable) try txn.delete(db_mod.row_policy_bundle_mod.candidate_key);
        try txn.put(db_mod.row_policy_bundle_mod.pending_key, &encoded_pending);
        try txn.put(db_mod.table_catalog_mod.key, &catalog_bytes);
        try txn.put(receipt.key, receipt.value);
        var replication_lsn_buf: [db_mod.replication_applied_lsn_value_len]u8 = undefined;
        if (replication_lsn) |lsn| {
            const marker = db_mod.replicationAppliedSequenceWrite(lsn, &replication_lsn_buf);
            try txn.put(marker.key, marker.value);
        }
        if (replication_outbox) |outbox| try txn.put(outbox.key, outbox.value);
        if (replication_outbox != null) {
            db.local_execution.durable_replication_outbox_maybe.store(true, .release);
            db.local_execution.row_policy_replication_outbox_pending.store(true, .release);
        }
        try txn.commit();
        if (replication_outbox != null) {
            schedule_replication_recovery_on_exit = true;
        }
        db.core.table_catalog = next;
        // Keep the old immutable bundle alive for previously admitted bound
        // readers. The receipt probe swaps it only after those leases drain.
        try self.finishRowPolicyReplicationCommitLocked(replication_payload, if (replication_outbox) |outbox| outbox.key else null, &apply_held, &replication_mutation);
        return null;
    }

    pub fn applyReplicatedRowPolicy(self: *Query, mutation: db_mod.replication_effects_mod.RowPolicyMutation, sequence: u64) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("applyReplicatedRowPolicy", .{ db, mutation, sequence });
        _ = try self.applyRowPolicyPublicationInternal(
            mutation.bundle,
            mutation.request,
            mutation.entry,
            sequence,
        );
    }

    pub fn verifyRowPolicyPrincipal(self: *Query, alloc: Allocator, token: []const u8, database: []const u8, access: db_mod.row_policy_authority_mod.Access, now_seconds: i64) !std.json.Parsed(db_mod.row_policy_authority_mod.Payload) {
        const db = self.dbPtr();
        if (token.len == 0 or token.len > db_mod.row_policy_authority_mod.maximum_token_bytes or database.len == 0)
            return error.RowPolicyAuthenticationRequired;
        return db_mod.row_policy_authority_mod.verify(alloc, db.local_execution.row_policy_authority_secret orelse return error.RowPolicyAuthorityUnavailable, db.local_execution.row_policy_authority_issuer orelse return error.RowPolicyAuthorityUnavailable, .{
            .table_id = db.core.identity_namespace.table_id,
            .table = db.local_execution.row_policy_table_name orelse return error.RowPolicyAuthorityUnavailable,
            .database = database,
            .policy_generation = db.local_execution.row_policy_gate.generation.load(.acquire),
            .catalog_epoch = db.local_execution.row_policy_gate.catalog_epoch.load(.acquire),
            .access = access,
        }, now_seconds, token);
    }

    pub fn enforceRowPolicyIntentsLocked(self: *Query, schema_view: db_mod.schema_registry_mod.SchemaView, intents: []const db_mod.transactions_mod.WriteIntent, principal: *const db_mod.row_policy_authority_mod.Payload) !void {
        const db = self.dbPtr();
        if (principal.access != .write) return error.RowPolicyAuthenticationRequired;
        const bundle = if (db.local_execution.row_policy_bundle) |*installed| installed else return error.RowPolicyCatalogChanged;
        const schema = schema_view.tableSchema().*;
        var insert_check = try bundle.captureEvaluation(db.alloc, schema, principal, .insert);
        defer insert_check.deinit();
        var update_old = try bundle.captureEvaluation(db.alloc, schema, principal, .update_old);
        defer update_old.deinit();
        var update_new = try bundle.captureEvaluation(db.alloc, schema, principal, .update_new);
        defer update_new.deinit();
        var delete_old = try bundle.captureEvaluation(db.alloc, schema, principal, .delete);
        defer delete_old.deinit();
        var probe = try db.core.store.beginProbeTxn();
        defer probe.abort();
        for (intents) |intent| {
            if (db_mod.isMetadataKey(intent.key)) return error.RowPolicyMutationUnsupported;
            const store_key = try db_mod.encodeStoreLookupKeyAlloc(db, db.alloc, intent.key);
            defer db.alloc.free(store_key);
            const old_bytes: ?[]const u8 = probe.getLeased(store_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (old_bytes) |bytes| {
                if (try db_mod.relational_store.rowSchemaVersion(bytes) != schema.version) return error.RowPolicyCatalogChanged;
                const old = if (db.core.store.valuesAreAuthenticated())
                    try db_mod.relational_row_codec.ordinalRowViewTrusted(bytes, schema, schema_view.physicalLayout())
                else
                    try db_mod.relational_row_codec.ordinalRowViewSelective(bytes, schema, schema_view.physicalLayout());
                if (!try (if (intent.value == null) delete_old else update_old).permits(old)) return error.RowPolicyDenied;
            }
            if (intent.value != null) {
                const encoded = intent.prepared_row orelse return error.RowPolicyMutationUnsupported;
                const new = try db_mod.relational_row_codec.ordinalRowViewTrusted(encoded, schema, schema_view.physicalLayout());
                if (!try (if (old_bytes == null) insert_check else update_new).permits(new)) return error.RowPolicyDenied;
            }
        }
    }
};

test "db query capture seals outside mutation fences and retains the selected generation" {
    for ([_]bool{ false, true }) |missing_summary| {
        const alloc = std.testing.allocator;
        var directory = try db_mod.TestDirectory.init("query-seal-admission");
        defer directory.cleanup();
        const path = std.mem.span(directory.path().ptr);
        defer db_mod.cleanupTempDir(directory.path().ptr);
        const pins = try std.fmt.allocPrint(alloc, "{s}.query-pins", .{path});
        defer alloc.free(pins);
        defer std.Io.Dir.cwd().deleteTree(std.testing.io, pins) catch {};
        var db = try DB.open(alloc, path, .{
            .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 },
            .start_index_workers = false,
            .start_optional_runtime_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
        const cut = @import("../native_query_cut.zig");
        const empty_id: [64]u8 = @splat('c');
        var empty_request: cut.Request = .{ .id = &empty_id, .table_id = 7, .expires_ms = cut.nowMs() + 300_000, .create = true };
        // A foreground reader/maintenance owner can retain catalog admission
        // throughout capture. No exclusive reader drain is needed for pins.
        try std.testing.expect(db.beginPublishedDenseSearch());
        try db.captureQueryCut(empty_request, .none);
        db.endPublishedDenseSearch();
        empty_request.create = false;
        {
            var empty = try db.query.openCut(empty_request, .none);
            defer empty.close();
            var empty_hits = try empty.search(alloc, .{ .full_text = .{ .match = .{ .field = "body", .text = "original" } }, .include_stored = false });
            defer empty_hits.deinit();
            try std.testing.expectEqual(@as(usize, 0), empty_hits.hits.len);
        }
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"original\"}" }}, .sync_level = .full_index });
        if (missing_summary) {
            try db.core.store.putBatch(&.{}, &.{db_mod.internal_keys.identity_visibility_summary_key[0..]});
            db.core.identity_visibility.summary = null;
        }
        const Probe = struct {
            db: *DB,
            replace_index: bool,
            released: bool = false,
            failure: ?anyerror = null,
            fn admitted(_: *anyopaque) void {}
            fn materialize(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                // Fail without blocking if a regression retains any fence. Only
                // run synchronous writes after proving admission is available.
                if (!self.db.core.tryLockApplyExclusive()) return;
                self.db.core.unlockApply();
                var capture = self.db.core.snapshot_admission.tryAcquireCapture() orelse return;
                capture.release();
                var replay = self.db.core.snapshot_replay_admission.tryAcquireCapture() orelse return;
                replay.release();
                if (!self.db.local_execution.index_structural_mutation_mutex.tryLock()) return;
                self.db.local_execution.index_structural_mutation_mutex.unlock();
                if (self.db.indexCatalogBarrierActive()) return;
                self.released = true;
                self.db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"replacement\"}" }}, .sync_level = .full_index }) catch |err| {
                    self.failure = err;
                    return;
                };
                _ = self.db.runLsmMaintenanceStepBestEffort() catch |err| {
                    self.failure = err;
                    return;
                };
                // Catalog admission requires its maintained summary. The
                // missing-summary variant exercises conservative reads only.
                if (!self.replace_index) return;
                const removed = self.db.deleteIndex("text") catch |err| {
                    self.failure = err;
                    return;
                };
                if (!removed) {
                    self.failure = error.TestExpectedEqual;
                    return;
                }
                var drained = false;
                for (0..1000) |_| {
                    const progress = self.db.advanceGeneratedArtifactCleanupPage("text") catch |err| {
                        self.failure = err;
                        return;
                    };
                    if (progress == .idle) {
                        drained = true;
                        break;
                    }
                    if (progress == .busy) std.testing.io.sleep(.fromMilliseconds(1), .awake) catch {};
                }
                if (!drained) {
                    self.failure = error.TestCleanupDidNotComplete;
                    return;
                }
                self.db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" }) catch |err| {
                    self.failure = err;
                    return;
                };
            }
        };
        var probe = Probe{ .db = &db, .replace_index = !missing_summary };
        DB.test_snapshot_fence_hook = .{ .ptr = &probe, .after_capture_admission = Probe.admitted, .before_artifact_materialize = Probe.materialize };
        defer DB.test_snapshot_fence_hook = null;
        const id: [64]u8 = @splat('b');
        var request: cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = cut.nowMs() + 300_000, .create = true };
        try db.captureQueryCut(request, .none);
        if (probe.failure) |err| return err;
        try std.testing.expect(probe.released);
        request.create = false;
        var retained = try db.query.openCut(request, .none);
        defer retained.close();
        if (missing_summary) try std.testing.expect(try db_mod.doc_identity.visibilitySummaryFromStore(retained.core.store) == null);
        const old = (try retained.get(alloc, "doc")).?;
        defer alloc.free(old);
        try std.testing.expect(std.mem.indexOf(u8, old, "original") != null);
        const current = (try db.get(alloc, "doc")).?;
        defer alloc.free(current);
        try std.testing.expect(std.mem.indexOf(u8, current, "replacement") != null);
        var old_hits = try retained.search(alloc, .{ .full_text = .{ .match = .{ .field = "body", .text = "original" } }, .include_stored = false });
        defer old_hits.deinit();
        try std.testing.expectEqual(@as(usize, 1), old_hits.hits.len);
    }
}
