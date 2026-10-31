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
const Indexes = @import("mod.zig").Indexes;

/// Index repair owned by `DB.indexes`: repair intents and their scheduler, leases, shadow rebuilds and activation, for every index kind.
pub const RepairIndexes = struct {
    clock: ?db_mod.platform_clock.Clock = null,
    cleanup_owner_id: u64 = 0,
    mutex: std.atomic.Mutex = .unlocked,
    active: std.StringHashMapUnmanaged(bool) = .{},
    shadow_hook: ?DB.ShadowIndexRepairHook = null,

    fn dbPtr(self: *RepairIndexes) *DB {
        const parent: *Indexes = @alignCast(@fieldParentPtr("repair", self));
        return parent.dbPtr();
    }

    fn dbConstPtr(self: *const RepairIndexes) *const DB {
        const parent: *const Indexes = @alignCast(@fieldParentPtr("repair", self));
        return parent.dbConstPtr();
    }

    /// Rehydrate visibility edges raised before the managed owner attached its
    /// hook from durable repair authority. AsyncContext deliberately does not
    /// retain borrowed intent strings; reducing those edges to unknown scope,
    /// however, turns ordinary initial materialization into a table-wide
    /// repair fence. Hook attachment is a control-plane boundary, so paying one
    /// bounded checkpoint read here keeps steady-state notification allocation
    /// free while preserving exact index/incarnation identity.
    pub fn replayPendingVisibility(self: *RepairIndexes, hook: db_mod.QueryVisibilityHook) void {
        const db = self.dbPtr();
        db_mod.lockAtomic(&db.async_context.index_repair_control_mutex);
        var state = self.loadState(db.runtime_alloc) catch |err| {
            db.async_context.index_repair_control_mutex.unlock();
            switch (err) {
                // A pending edge may have been cleared before attachment. No
                // durable intent means there is no visibility debt to replay.
                error.FileNotFound => return,
                else => {
                    // If durable scope itself cannot be trusted, retain the
                    // conservative group-wide invalidation contract.
                    std.log.warn("index repair visibility audit failed err={s}", .{@errorName(err)});
                    hook.notify(.{ .change = .index_repair_pending });
                    return;
                },
            }
        };
        db.async_context.index_repair_control_mutex.unlock();
        defer state.deinit(db.runtime_alloc);

        for (state.entries.items) |entry| {
            const intent = entry.intent;
            if (intent.phase == .terminal or intent.automation != .enabled) continue;
            hook.notify(.{
                .change = .index_repair_pending,
                .repair = .{
                    .index_name = intent.index_name,
                    .work_class = switch (intent.work_class) {
                        .repair => .repair,
                        .initial_build => .initial_build,
                    },
                    .repair_id = intent.repair_id,
                    .revision = intent.revision,
                    .config_hash = intent.config_hash,
                    .root_generation = intent.root_generation,
                    .admission = repairAdmission(intent),
                    .action_required = actionRequired(intent),
                },
            });
        }
    }

    pub fn notifyVisibilityHook(
        ctx: *db_mod.AsyncContext,
        change: db_mod.QueryVisibilityChange,
        intent: db_mod.index_repair_state.IndexRepairIntent,
        persisted_revision: u64,
        previous_admission: db_mod.IndexRepairAdmission,
        previous_action_required: bool,
        group_repair_debt_remains: bool,
    ) void {
        std.debug.assert(change == .index_repair_pending or change == .index_repair_cleared);
        const admission: db_mod.IndexRepairAdmission = if (change == .index_repair_cleared)
            .serviceable
        else
            repairAdmission(intent);
        DB.notifyQueryVisibilityEvent(ctx, .{
            .change = change,
            .repair = .{
                .index_name = intent.index_name,
                .work_class = switch (intent.work_class) {
                    .repair => .repair,
                    .initial_build => .initial_build,
                },
                .repair_id = intent.repair_id,
                .revision = persisted_revision,
                .config_hash = intent.config_hash,
                .root_generation = intent.root_generation,
                .previous_admission = previous_admission,
                .admission = admission,
                .previous_action_required = previous_action_required,
                .action_required = change != .index_repair_cleared and actionRequired(intent),
            },
            .group_repair_debt_remains = group_repair_debt_remains,
        });
    }

    pub fn scheduleInactiveShadowCleanup(self: *RepairIndexes) void {
        const db = self.dbPtr();
        scheduleInactiveShadowCleanupContext(
            db.core.index_manager,
            db.backend_runtime.durable_jobs,
            self.cleanup_owner_id,
        );
    }

    fn scheduleInactiveShadowCleanupContext(
        manager: *db_mod.index_manager_mod.IndexManager,
        lane: db_mod.background_runtime_mod.DurableJobLane,
        owner_id: u64,
    ) void {
        @import("../cleanup_job_owner.zig").schedule(.{
            .ptr = manager,
            .state = &manager.repair_cleanup_state,
            .lane = lane,
            .owner_id = owner_id,
            .io = manager.checkpointIo(),
            .advance = advanceShadowCleanup,
            .name = "repair-generation",
        });
    }

    fn advanceShadowCleanup(ptr: *anyopaque) !@import("../cleanup_job_owner.zig").Advance {
        const manager: *db_mod.index_manager_mod.IndexManager = @ptrCast(@alignCast(ptr));
        return if (try manager.cleanupInactiveRepairShadowRootsPage()) .progressed else .idle;
    }

    pub fn beginLease(self: *RepairIndexes, index_name: []const u8) !bool {
        const db = self.dbPtr();
        db_mod.lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.active.contains(index_name)) return false;
        const owned = try db.alloc.dupe(u8, index_name);
        errdefer db.alloc.free(owned);
        try self.active.put(db.alloc, owned, false);
        return true;
    }

    pub fn activeCancellationRequested(self: *RepairIndexes, index_name: []const u8) bool {
        db_mod.lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.active.get(index_name) orelse false;
    }

    fn requestActiveCancellation(self: *RepairIndexes, index_name: []const u8) bool {
        db_mod.lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const requested = self.active.getPtr(index_name) orelse return false;
        requested.* = true;
        return true;
    }

    pub fn endLease(self: *RepairIndexes, index_name: []const u8) void {
        const db = self.dbPtr();
        db_mod.lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.active.fetchRemove(index_name)) |removed| {
            db.alloc.free(@constCast(removed.key));
        }
    }

    pub fn listIssuesPage(
        self: *RepairIndexes,
        alloc: Allocator,
        req: db_mod.types.ArtifactRepairListRequest,
    ) !db_mod.types.ArtifactRepairListResult {
        const db = self.dbPtr();
        const configs = try db.core.listIndexes(alloc);
        defer db_mod.types.freeIndexConfigs(alloc, configs);
        std.mem.sort(db_mod.types.IndexConfig, configs, {}, struct {
            fn lessThan(_: void, lhs: db_mod.types.IndexConfig, rhs: db_mod.types.IndexConfig) bool {
                return std.mem.order(u8, lhs.name, rhs.name) == .lt;
            }
        }.lessThan);

        var issues = std.ArrayListUnmanaged(db_mod.types.ArtifactRepairIssue).empty;
        errdefer {
            for (issues.items) |*issue| issue.deinit(alloc);
            issues.deinit(alloc);
        }

        const cursor = req.cursor orelse "";
        var last_returned: ?[]const u8 = null;
        var has_more = false;
        var scanned: u64 = 0;
        for (configs) |cfg| {
            if (cursor.len != 0 and std.mem.order(u8, cfg.name, cursor) != .gt) continue;
            if (req.index_name) |requested| {
                if (!std.mem.eql(u8, requested, cfg.name)) continue;
            }
            const artifact_kind = DB.artifactRepairKindForIndexKind(cfg.kind) orelse continue;
            if (req.artifact_kind) |requested_kind| {
                if (requested_kind != artifact_kind) continue;
            }
            if (!(try db.indexGenerationRepairRequired(alloc, cfg.name))) continue;
            scanned += 1;
            if (req.limit != 0 and issues.items.len >= req.limit) {
                has_more = true;
                break;
            }

            const load_error = db.core.index_manager.loadFailure(cfg.name);
            try issues.append(alloc, .{
                .artifact_kind = artifact_kind,
                .index_name = try alloc.dupe(u8, cfg.name),
                .artifact_name = try alloc.dupe(u8, cfg.name),
                .repairable = true,
                .unsupported_reason = "",
                .reason = if (load_error != null) .unreadable_artifact else .missing_artifact,
                .last_error = if (load_error) |err_name| try alloc.dupe(u8, err_name) else try alloc.dupe(u8, "index_repair_required"),
            });
            last_returned = cfg.name;
        }

        return .{
            .issues = try issues.toOwnedSlice(alloc),
            .limit = req.limit,
            .scanned = scanned,
            .next_cursor = if (has_more and last_returned != null) try alloc.dupe(u8, last_returned.?) else null,
            .has_more = has_more,
        };
    }

    pub fn stateLocation(self: *const RepairIndexes) !db_mod.index_repair_state.Location {
        const db = self.dbConstPtr();
        return try stateLocationContext(db.async_context);
    }

    fn stateLocationContext(ctx: *const db_mod.AsyncContext) !db_mod.index_repair_state.Location {
        return ctx.index_repair_checkpoint orelse error.DurableIndexRepairStateUnavailable;
    }

    pub fn hasPendingIntents(self: *const RepairIndexes, alloc: Allocator) !bool {
        const db = self.dbConstPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("hasPendingIndexRepairIntents", .{ db, alloc });
        if (builtin.os.tag == .freestanding) return db.hasManagedIndexAdmissionMarker(alloc);
        const location = try self.stateLocation();
        var state = db_mod.index_repair_state.loadAt(alloc, location) catch |err| switch (err) {
            error.FileNotFound => return db.hasManagedIndexAdmissionMarker(alloc),
            else => return err,
        };
        defer state.deinit(alloc);
        if (state.entries.items.len != 0) return true;
        return db.hasManagedIndexAdmissionMarker(alloc);
    }

    /// Requires index_repair_scheduler_mutex.
    fn publishProgressWaitHint(ctx: *db_mod.AsyncContext) void {
        ctx.index_repair_progress_wait_pending.store(
            ctx.index_repair_scheduler.initialized and
                ctx.index_repair_scheduler.progress_waiters != 0,
            .release,
        );
    }

    fn invalidateSchedulerDirectory(ctx: *db_mod.AsyncContext) void {
        _ = ctx.index_repair_scheduler_revision.fetchAdd(1, .acq_rel);
        db_mod.lockAtomic(&ctx.index_repair_scheduler_mutex);
        defer ctx.index_repair_scheduler_mutex.unlock();
        ctx.index_repair_scheduler.deinit(ctx.alloc);
        publishProgressWaitHint(ctx);
    }

    pub fn noteSchedulerUpsert(
        ctx: *db_mod.AsyncContext,
        intent: db_mod.index_repair_state.IndexRepairIntent,
        persisted_revision: u64,
        control_revision: u64,
    ) void {
        _ = ctx.index_repair_scheduler_revision.fetchAdd(1, .acq_rel);
        db_mod.lockAtomic(&ctx.index_repair_scheduler_mutex);
        defer ctx.index_repair_scheduler_mutex.unlock();
        if (!ctx.index_repair_scheduler.initialized) {
            publishProgressWaitHint(ctx);
            return;
        }
        switch (ctx.index_repair_scheduler.classifyControlMutation(intent.identity(), control_revision)) {
            .stale => return,
            .gap => {
                ctx.index_repair_scheduler.deinit(ctx.alloc);
                publishProgressWaitHint(ctx);
                return;
            },
            .next => {},
        }
        ctx.index_repair_scheduler.upsert(ctx.alloc, intent, persisted_revision) catch {
            // The durable checkpoint remains authoritative. Allocation or a
            // consistency mismatch invalidates only the acceleration layer;
            // the next owner rebuilds it from the committed checkpoint.
            ctx.index_repair_scheduler.deinit(ctx.alloc);
            publishProgressWaitHint(ctx);
            return;
        };
        ctx.index_repair_scheduler.control_revision = control_revision;
        publishProgressWaitHint(ctx);
    }

    fn noteSchedulerRemove(
        ctx: *db_mod.AsyncContext,
        identity: db_mod.index_repair_state.ReplicaIdentity,
        repair_id: u128,
        control_revision: u64,
    ) void {
        _ = ctx.index_repair_scheduler_revision.fetchAdd(1, .acq_rel);
        db_mod.lockAtomic(&ctx.index_repair_scheduler_mutex);
        defer ctx.index_repair_scheduler_mutex.unlock();
        if (!ctx.index_repair_scheduler.initialized) {
            publishProgressWaitHint(ctx);
            return;
        }
        switch (ctx.index_repair_scheduler.classifyControlMutation(identity, control_revision)) {
            .stale => return,
            .gap => {
                ctx.index_repair_scheduler.deinit(ctx.alloc);
                publishProgressWaitHint(ctx);
                return;
            },
            .next => {},
        }
        ctx.index_repair_scheduler.remove(ctx.alloc, repair_id);
        ctx.index_repair_scheduler.control_revision = control_revision;
        publishProgressWaitHint(ctx);
    }

    pub fn nowMs(self: *const RepairIndexes) u64 {
        const db = self.dbConstPtr();
        return (self.clock orelse db.backend_runtime.clock()).nowRealtimeMs();
    }

    pub fn deferForProgress(
        self: *RepairIndexes,
        repair_id: u128,
        expected_revision: u64,
        wake_at_sequence: u64,
    ) bool {
        const db = self.dbPtr();
        const fallback_at_ms = self.nowMs() +|
            DB.progressive_index_repair_audit_interval_ms;
        db_mod.lockAtomic(&db.async_context.index_repair_scheduler_mutex);
        defer db.async_context.index_repair_scheduler_mutex.unlock();
        if (!db.async_context.index_repair_scheduler.initialized) return false;
        const armed = db.async_context.index_repair_scheduler.deferForProgress(
            repair_id,
            expected_revision,
            wake_at_sequence,
            fallback_at_ms,
        );
        if (armed) db.async_context.index_repair_progress_wait_pending.store(true, .release);
        return armed;
    }

    pub fn deferForAudit(
        self: *RepairIndexes,
        repair_id: u128,
        expected_revision: u64,
    ) bool {
        const db = self.dbPtr();
        const fallback_at_ms = self.nowMs() +|
            DB.progressive_index_repair_audit_interval_ms;
        db_mod.lockAtomic(&db.async_context.index_repair_scheduler_mutex);
        defer db.async_context.index_repair_scheduler_mutex.unlock();
        if (!db.async_context.index_repair_scheduler.initialized) return false;
        const deferred = db.async_context.index_repair_scheduler.deferForAudit(
            repair_id,
            expected_revision,
            fallback_at_ms,
        );
        if (deferred) publishProgressWaitHint(db.async_context);
        return deferred;
    }

    /// Park an initial-build intent on the one watermark that can change the
    /// current replay proof. Once that target is already visible, later index
    /// publications cannot by themselves prove source-coverage completion;
    /// use the bounded audit deadline instead of waking repair for every newly
    /// generated artifact.
    fn deferUntilTargetOrAudit(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
        expected_revision: u64,
        index_name: []const u8,
        observed_sequence: u64,
        target_sequence: u64,
    ) !bool {
        const db = self.dbPtr();
        if (observed_sequence >= target_sequence) {
            return self.deferForAudit(repair_id, expected_revision);
        }
        if (!self.deferForProgress(
            repair_id,
            expected_revision,
            target_sequence,
        )) return false;

        // Arm-then-recheck closes the lost-wakeup window between observing the
        // watermark and installing the revision-scoped resident wait.
        const current_sequence = try db.managedIndexAppliedSequence(alloc, index_name);
        if (current_sequence >= target_sequence) {
            _ = wakeForProgressContext(
                db.async_context,
                index_name,
                current_sequence,
            );
        }
        return true;
    }

    pub fn wakeForProgressContext(
        ctx: *db_mod.AsyncContext,
        index_name: []const u8,
        applied_sequence: u64,
    ) ?db_mod.IndexRepairSchedulerDirectory.ProgressWake {
        if (!ctx.index_repair_progress_wait_pending.load(.acquire)) return null;
        db_mod.lockAtomic(&ctx.index_repair_scheduler_mutex);
        defer ctx.index_repair_scheduler_mutex.unlock();
        if (!ctx.index_repair_scheduler.initialized) {
            ctx.index_repair_progress_wait_pending.store(false, .release);
            return null;
        }
        const woke = ctx.index_repair_scheduler.wakeForIndexProgress(index_name, applied_sequence);
        if (ctx.index_repair_scheduler.progress_waiters == 0) {
            ctx.index_repair_progress_wait_pending.store(false, .release);
        }
        return woke;
    }

    pub fn ensureSchedulerDirectory(self: *RepairIndexes, alloc: Allocator) !void {
        const db = self.dbPtr();
        while (true) {
            db_mod.lockAtomic(&db.async_context.index_repair_scheduler_mutex);
            const initialized = db.async_context.index_repair_scheduler.initialized;
            const current_root = if (db.async_context.index_repair_scheduler.identity) |identity|
                identity.root_generation == db.core.root_generation
            else
                false;
            if (initialized and !current_root) {
                db.async_context.index_repair_scheduler.deinit(db.async_context.alloc);
                publishProgressWaitHint(db.async_context);
                _ = db.async_context.index_repair_scheduler_revision.fetchAdd(1, .acq_rel);
            }
            db.async_context.index_repair_scheduler_mutex.unlock();
            if (initialized and current_root) return;
            if (initialized) continue;

            const observed_revision = db.async_context.index_repair_scheduler_revision.load(.acquire);
            var state = try self.loadOrCreateCurrentState(alloc);
            defer state.deinit(alloc);
            var replacement = try db_mod.IndexRepairSchedulerDirectory.buildFromState(db.async_context.alloc, state);
            var replacement_owned = true;
            defer if (replacement_owned) replacement.deinit(db.async_context.alloc);

            db_mod.lockAtomic(&db.async_context.index_repair_scheduler_mutex);
            defer db.async_context.index_repair_scheduler_mutex.unlock();
            if (db.async_context.index_repair_scheduler.initialized) return;
            if (db.async_context.index_repair_scheduler_revision.load(.acquire) != observed_revision) continue;
            db.async_context.index_repair_scheduler = replacement;
            replacement_owned = false;
            publishProgressWaitHint(db.async_context);
            return;
        }
    }

    pub fn intentSummary(self: *RepairIndexes, alloc: Allocator) !DB.IndexRepairIntentSummary {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("indexRepairIntentSummary", .{ db, alloc });
        return self.intentSummaryForIndex(alloc, null);
    }

    pub fn intentSummaryForIndex(self: *RepairIndexes, alloc: Allocator, target_index_name: ?[]const u8) !DB.IndexRepairIntentSummary {
        const db = self.dbPtr();
        if (db.managedAdmissionMaterializationPending()) try db.drainManagedIndexAdmissions(alloc);
        try self.ensureSchedulerDirectory(alloc);
        db_mod.lockAtomic(&db.async_context.index_repair_scheduler_mutex);
        defer db.async_context.index_repair_scheduler_mutex.unlock();
        return db.async_context.index_repair_scheduler.summaryForIndex(target_index_name);
    }

    pub fn loadState(self: *const RepairIndexes, alloc: Allocator) !db_mod.index_repair_state.State {
        const db = self.dbConstPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("loadIndexRepairState", .{ db, alloc });
        return try db_mod.index_repair_state.loadAt(alloc, try self.stateLocation());
    }

    pub fn refreshAvailabilityGate(self: *RepairIndexes, alloc: Allocator) !void {
        const db = self.dbPtr();
        var state = self.loadState(alloc) catch |err| switch (err) {
            error.FileNotFound, error.DurableIndexRepairStateUnavailable => return,
            error.InvalidIndexRepairState => {
                db.async_context.index_repair_state_corrupt.store(true, .release);
                db.async_context.index_repair_replay_pinned.store(true, .release);
                const configs = try db.core.index_manager.listIndexesPublic(alloc);
                defer {
                    for (configs) |*cfg| cfg.deinit(alloc);
                    alloc.free(configs);
                }
                for (configs) |cfg| try db.core.index_manager.markUnscopedRepairUnavailable(cfg.name);
                return;
            },
            else => return err,
        };
        defer state.deinit(alloc);
        // Opening already has to inspect every durable intent to install the
        // fail-closed serving gates. Build the exact scheduler projection from
        // that same snapshot so the first maintenance quantum never pays a
        // second O(N) cold-directory reconstruction.
        var scheduler = try db_mod.IndexRepairSchedulerDirectory.buildFromState(db.async_context.alloc, state);
        var scheduler_owned = true;
        defer if (scheduler_owned) scheduler.deinit(db.async_context.alloc);
        db_mod.lockAtomic(&db.async_context.index_repair_scheduler_mutex);
        if (!db.async_context.index_repair_scheduler.initialized) {
            db.async_context.index_repair_scheduler = scheduler;
            scheduler_owned = false;
        }
        db.async_context.index_repair_scheduler_mutex.unlock();
        for (state.entries.items) |entry| {
            var unavailable = intentBlocksService(entry.intent);
            if (!unavailable) {
                if ((entry.intent.trigger == .incomplete_bulk_publish or entry.intent.trigger == .text_projection_upgrade)) {
                    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, entry.intent.index_name);
                    unavailable = checkpoint.status != .clean or checkpoint.config_hash != entry.intent.config_hash;
                }
            }
            if (unavailable) {
                try db.core.index_manager.markUnscopedRepairUnavailable(entry.intent.index_name);
                db.core.index_manager.publishRepairAdmission(
                    entry.intent.index_name,
                    DB.repairAdmissionVersion(entry.intent, entry.intent.revision, entry.intent.control_revision),
                    true,
                    false,
                );
            } else db.core.index_manager.publishRepairAdmission(
                entry.intent.index_name,
                DB.repairAdmissionVersion(entry.intent, entry.intent.revision, entry.intent.control_revision),
                false,
                false,
            );
        }
    }

    /// A writable generation may clear its gate after activating the repaired
    /// root. A query-only DB is pinned to the root it opened, so it must retain
    /// every startup gate until retirement; durable checkpoint completion can
    /// describe a newer root and is not authority to expose the pinned one.
    /// Writable DBs recheck only rejected indexes, keeping healthy queries free
    /// of checkpoint I/O.
    pub fn refreshAvailabilityForIndex(self: *RepairIndexes, alloc: Allocator, index_name: []const u8) !void {
        const db = self.dbPtr();
        const pinned_read_generation = db_mod.openModeRequiresReadOnlyBackends(db.open_mode);
        var state = self.loadState(alloc) catch |err| switch (err) {
            error.FileNotFound => {
                if (pinned_read_generation) return;
                try db.clearRepairGateIfAdmissionCompleted(alloc, index_name);
                return;
            },
            error.DurableIndexRepairStateUnavailable => return,
            error.InvalidIndexRepairState => {
                db.async_context.index_repair_state_corrupt.store(true, .release);
                db.async_context.index_repair_replay_pinned.store(true, .release);
                return;
            },
            else => return err,
        };
        defer state.deinit(alloc);

        try self.refreshAvailabilityForIndexFromState(alloc, index_name, &state, pinned_read_generation);
    }

    pub fn refreshAvailabilityForIndexFromState(
        self: *RepairIndexes,
        alloc: Allocator,
        index_name: []const u8,
        state: *const db_mod.index_repair_state.State,
        pinned_read_generation: bool,
    ) !void {
        const db = self.dbPtr();
        // A checkpoint from another root generation is not authority to clear
        // this generation's gate. Startup/root-transition repair owns it.
        if (state.identity.root_generation != db.core.root_generation) return;

        const entry_index = state.findIndex(index_name) orelse {
            if (pinned_read_generation) return;
            try db.clearRepairGateIfAdmissionCompleted(alloc, index_name);
            return;
        };
        const intent = state.entries.items[entry_index].intent;
        var unavailable = intentBlocksService(intent);
        const pinned_generation_proved_serviceable = unavailable and
            try db.managedAdmissionGenerationIsQueryable(alloc, intent);
        if (pinned_generation_proved_serviceable) {
            unavailable = false;
        }
        // Durable phase/checkpoint transitions can describe a newer root than
        // the query-only DB has open. Only the proof above inspected this
        // pinned in-memory generation, so it is the sole read-only exception.
        if (pinned_read_generation) {
            if (pinned_generation_proved_serviceable) {
                db.core.index_manager.publishRepairServiceabilityProof(
                    index_name,
                    DB.repairAdmissionVersion(intent, intent.revision, intent.control_revision),
                );
            }
            return;
        }
        if (!unavailable and (intent.trigger == .incomplete_bulk_publish or intent.trigger == .text_projection_upgrade)) {
            const checkpoint = try db.core.loadProjectionCheckpoint(alloc, index_name);
            unavailable = checkpoint.status != .clean or checkpoint.config_hash != intent.config_hash;
        }
        if (!unavailable) db.core.index_manager.publishRepairServiceabilityProof(
            index_name,
            DB.repairAdmissionVersion(intent, intent.revision, intent.control_revision),
        );
    }

    pub fn intentBlocksService(intent: db_mod.index_repair_state.IndexRepairIntent) bool {
        // Initial materialization has no validated predecessor to retain.
        // Replay can reclassify its trigger while source artifacts arrive,
        // but only the scoped canonical proof or validated cleanup may open
        // admission; the ordinary replay-repair exception is not authority.
        if (intent.work_class == .initial_build) return intent.phase != .cleanup;
        return switch (intent.trigger) {
            .incomplete_bulk_publish, .text_projection_upgrade => intent.phase != .cleanup,
            // Only an operator-requested rebuild starts from a generation that
            // is still proven healthy. Coverage mismatch or missing proof is
            // fail-closed until replacement validation publishes cleanup.
            .operator_generation_rebuild, .storage_format_migration, .artifact_baseline_adoption => intent.phase == .activating or
                intent.phase == .validating or
                intent.phase == .rolling_back,
            .replay_artifact_unavailable => intent.phase == .activating or
                intent.phase == .validating or
                intent.phase == .rolling_back,
            .artifact_coverage_mismatch,
            .artifact_counter_missing,
            .root_generation_rebuild,
            .projection_generation_invalid,
            .operator_generation_validation,
            .catalog_admission,
            => intent.phase != .cleanup,
        };
    }

    pub fn repairAdmission(intent: db_mod.index_repair_state.IndexRepairIntent) db_mod.IndexRepairAdmission {
        return if (intentBlocksService(intent)) .blocked else .serviceable;
    }

    pub fn actionRequired(intent: db_mod.index_repair_state.IndexRepairIntent) bool {
        if (intent.automation == .paused) return true;
        return intent.phase == .terminal and
            db_mod.retryableIndexRepairTerminalPhase(intent.last_error, intent.trigger) == null;
    }

    pub fn loadOrCreateCurrentState(self: *RepairIndexes, alloc: Allocator) !db_mod.index_repair_state.State {
        const db = self.dbPtr();
        const location = try self.stateLocation();
        var state = try db_mod.index_repair_state.loadOrCreateAt(alloc, location, db.core.root_generation);
        var state_owned = true;
        defer if (state_owned) state.deinit(alloc);
        if (state.identity.root_generation == db.core.root_generation) {
            state_owned = false;
            return state;
        }

        // A root-generation transition fences every old repair identity. An
        // inactive candidate can be deleted. A pointer-selected candidate must
        // never become silently serviceable merely because its old intent is
        // discarded: persist fresh, new-root reconstruction debt first and
        // keep that index fail-closed until a new generation is validated.
        var replacement_intents = std.ArrayListUnmanaged(db_mod.index_repair_state.IndexRepairIntent).empty;
        defer {
            for (replacement_intents.items) |*intent| intent.deinit(alloc);
            replacement_intents.deinit(alloc);
        }
        for (state.entries.items) |entry| {
            const candidate = entry.intent.candidate_relative_path orelse continue;
            try db_mod.index_repair_state.validateCandidateRelativePath(entry.intent.index_name, candidate);
            if (try db.core.index_manager.isRepairCandidateActive(entry.intent.index_name, candidate)) {
                const cfg = db.core.index_manager.get(entry.intent.index_name) orelse
                    return error.InvalidIndexRepairState;
                const now_ms = self.nowMs();
                const target_sequence = db.core.nextDerivedSequence();
                var replacement_intent = db_mod.index_repair_state.IndexRepairIntent{
                    .repair_id = try db_mod.index_repair_state.newRepairId(alloc),
                    .db_identity = state.identity.db_identity,
                    .group_id = db.localRepairGroupId(),
                    .replica_id = state.identity.replica_id,
                    .root_generation = db.core.root_generation,
                    .index_name = try alloc.dupe(u8, entry.intent.index_name),
                    .kind = cfg.kind,
                    .config_hash = db_mod.types.indexConfigHash(cfg.*),
                    .trigger = .root_generation_rebuild,
                    .detected_sequence = target_sequence,
                    .target_sequence = target_sequence,
                    .started_at_ms = now_ms,
                    .updated_at_ms = now_ms,
                    .owner_epoch = 0,
                    .last_error = try alloc.dupe(u8, "root_generation_changed_during_activation"),
                };
                var replacement_intent_owned = true;
                errdefer if (replacement_intent_owned) replacement_intent.deinit(alloc);
                try replacement_intents.append(alloc, replacement_intent);
                replacement_intent_owned = false;
                try db.core.index_manager.prepareRepairAdmissionRebind(
                    entry.intent.index_name,
                    DB.repairAdmissionVersion(replacement_intent, 1, 1),
                );
                continue;
            }
            const separator = std.mem.indexOfScalar(u8, candidate, '/') orelse return error.InvalidRepairCandidatePath;
            const shadow_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ db.core.index_manager.base_path, candidate[0..separator] });
            defer alloc.free(shadow_root);
            if (db.core.index_manager.generation_storage orelse db.primary_lsm_storage) |storage| {
                try storage.deleteTree(shadow_root);
            } else if (comptime builtin.os.tag == .freestanding) {
                return error.UnsupportedPlatform;
            } else {
                var io_impl = db_mod.threadedIo();
                defer io_impl.deinit();
                try std.Io.Dir.cwd().deleteTree(io_impl.io(), shadow_root);
            }
        }
        const old_identity = state.identity;
        const replacement = try db_mod.index_repair_state.resetForRootGenerationWithIntentsAt(
            alloc,
            location,
            old_identity,
            db.core.root_generation,
            replacement_intents.items,
        );
        for (replacement.entries.items) |entry| {
            db.core.index_manager.publishRepairAdmission(
                entry.intent.index_name,
                DB.repairAdmissionVersion(entry.intent, entry.intent.revision, entry.intent.control_revision),
                intentBlocksService(entry.intent),
                false,
            );
        }
        // Only the checkpoint replacement authorizes retirement of old-root
        // admissions. Active candidates were pre-bound to their new identity,
        // so the exact old publication is ignored for them; inactive entries
        // are released without allowing a same-name incarnation to be opened.
        for (state.entries.items) |entry| {
            db.core.index_manager.publishRepairAdmission(
                entry.intent.index_name,
                DB.repairAdmissionVersion(entry.intent, entry.intent.revision +| 1, state.control_revision +| 1),
                false,
                false,
            );
            db.core.index_manager.clearUnscopedRepairUnavailable(entry.intent.index_name);
        }
        state.deinit(alloc);
        state_owned = false;
        invalidateSchedulerDirectory(db.async_context);
        const observed_revision = db.async_context.index_repair_scheduler_revision.load(.acquire);
        var scheduler = try db_mod.IndexRepairSchedulerDirectory.buildFromState(db.async_context.alloc, replacement);
        var scheduler_owned = true;
        defer if (scheduler_owned) scheduler.deinit(db.async_context.alloc);
        db_mod.lockAtomic(&db.async_context.index_repair_scheduler_mutex);
        if (!db.async_context.index_repair_scheduler.initialized and
            db.async_context.index_repair_scheduler_revision.load(.acquire) == observed_revision)
        {
            db.async_context.index_repair_scheduler = scheduler;
            scheduler_owned = false;
        }
        publishProgressWaitHint(db.async_context);
        db.async_context.index_repair_scheduler_mutex.unlock();
        return replacement;
    }

    pub fn loadEntryById(
        self: *const RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
    ) !db_mod.index_repair_state.Entry {
        var state = try self.loadState(alloc);
        defer state.deinit(alloc);
        for (state.entries.items) |entry| {
            if (entry.intent.repair_id == repair_id) return try entry.clone(alloc);
        }
        return error.IndexRepairIntentNotFound;
    }

    /// Atomically protects replay before exposing a primary snapshot. The
    /// provisional zero pin and truncation share repair_replay_mutex, closing
    /// the floor-capture race even if the process stops between either durable
    /// checkpoint update.
    pub fn beginPinnedSnapshot(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
    ) !DB.PinnedIndexRepairSnapshot {
        const db = self.dbPtr();
        db.core.repair_replay_mutex.lockUncancelable(db.core.index_manager.checkpointIo());
        defer db.core.repair_replay_mutex.unlock(db.core.index_manager.checkpointIo());
        const location = try self.stateLocation();
        var state = try db_mod.index_repair_state.loadAt(alloc, location);
        defer state.deinit(alloc);
        const entry_index = blk: {
            for (state.entries.items, 0..) |entry, i| {
                if (entry.intent.repair_id == repair_id) break :blk i;
            }
            return error.IndexRepairIntentNotFound;
        };
        var entry = try state.entries.items[entry_index].clone(alloc);
        defer entry.deinit(alloc);
        if (entry.intent.phase == .terminal or entry.intent.automation == .paused) return error.IndexRepairNotRunnable;
        var expected = db_mod.index_repair_state.ExpectedTransition{
            .repair_id = entry.intent.repair_id,
            .revision = entry.intent.revision,
            .phase = entry.intent.phase,
            .config_hash = entry.intent.config_hash,
            .root_generation = entry.intent.root_generation,
            .owner_epoch = entry.intent.owner_epoch,
        };
        if (entry.pin == null) {
            entry.pin = .{
                .repair_id = entry.intent.repair_id,
                .db_identity = entry.intent.db_identity,
                .replica_id = entry.intent.replica_id,
                .root_generation = entry.intent.root_generation,
                .index_name = try alloc.dupe(u8, entry.intent.index_name),
                .retain_after_sequence = 0,
            };
            const control_revision = try db_mod.index_repair_state.putEntryAt(alloc, location, state.identity, expected, entry);
            db.core.index_manager.publishRepairAdmission(
                entry.intent.index_name,
                DB.repairAdmissionVersion(entry.intent, expected.revision +| 1, control_revision),
                intentBlocksService(entry.intent),
                true,
            );
            noteSchedulerUpsert(db.async_context, entry.intent, expected.revision +| 1, control_revision);
        } else {
            // Re-entering snapshot acquisition must become conservative before
            // replacing a previously finalized build floor.
            entry.pin.?.retain_after_sequence = 0;
            const control_revision = try db_mod.index_repair_state.putEntryAt(alloc, location, state.identity, expected, entry);
            db.core.index_manager.publishRepairAdmission(
                entry.intent.index_name,
                DB.repairAdmissionVersion(entry.intent, expected.revision +| 1, control_revision),
                intentBlocksService(entry.intent),
                true,
            );
            noteSchedulerUpsert(db.async_context, entry.intent, expected.revision +| 1, control_revision);
        }
        expected.revision +|= 1;
        db.async_context.index_repair_replay_pinned.store(true, .release);

        var txn = try db.core.store.beginReadTxn();
        errdefer txn.abort();
        const build_floor = try db.core.store.lastReplaySequenceFromTxn(&txn, 0);
        entry.intent.build_floor_sequence = build_floor;
        entry.intent.build_source_guard = try @import("../artifact_source_gap.zig").Guard.capture(&txn);
        entry.intent.updated_at_ms = self.nowMs();
        entry.pin.?.retain_after_sequence = build_floor;
        const control_revision = try db_mod.index_repair_state.putEntryAt(alloc, location, state.identity, expected, entry);
        db.core.index_manager.publishRepairAdmission(
            entry.intent.index_name,
            DB.repairAdmissionVersion(entry.intent, expected.revision +| 1, control_revision),
            intentBlocksService(entry.intent),
            true,
        );
        noteSchedulerUpsert(db.async_context, entry.intent, expected.revision +| 1, control_revision);
        return .{
            .txn = txn,
            .repair_id = repair_id,
            .build_floor_sequence = build_floor,
        };
    }

    fn phaseHasActivationCertification(phase: db_mod.index_repair_state.Phase) bool {
        return switch (phase) {
            .activating, .validating, .cleanup, .rolling_back => true,
            else => false,
        };
    }

    /// Resolve the phase produced by the complete update transaction. Failure
    /// accounting can move an intent to `terminal` without populating the
    /// explicit phase field, while an explicit phase retains the historical
    /// last-writer precedence of the mutation block below. Safety invariants
    /// must reason about this effective phase, not the update's encoding.
    fn intentEffectivePhase(
        current: db_mod.index_repair_state.Phase,
        update: DB.IndexRepairIntentUpdate,
    ) db_mod.index_repair_state.Phase {
        if (update.phase) |phase| return phase;
        if (update.attempt_failure) |failure| {
            if (failure.terminal) return .terminal;
        }
        return current;
    }

    pub fn retryDelayMs(repair_id: u128, attempt_count: u32) u64 {
        const exponent: u6 = @intCast(@min(attempt_count -| 1, 5));
        const base_ms: u64 = 30 * std.time.ms_per_s;
        const maximum_ms: u64 = 10 * std.time.ms_per_min;
        const nominal = @min(base_ms << exponent, maximum_ms);
        // Stable per-attempt +/-10% jitter prevents a node restart from
        // synchronizing every pending repair while keeping the retry time
        // reconstructible and bounded.
        var seed: [20]u8 = undefined;
        std.mem.writeInt(u128, seed[0..16], repair_id, .little);
        std.mem.writeInt(u32, seed[16..20], attempt_count, .little);
        const spread = @max(@as(u64, 1), nominal / 5);
        const jitter = std.hash.Wyhash.hash(0x4944585250525954, &seed) % spread;
        return nominal - nominal / 10 + jitter;
    }

    pub fn idForIndex(self: *const RepairIndexes, alloc: Allocator, index_name: []const u8) !?u128 {
        const db = self.dbConstPtr();
        return try idForIndexContext(db.async_context, alloc, index_name);
    }

    pub fn idForIndexContext(ctx: *const db_mod.AsyncContext, alloc: Allocator, index_name: []const u8) !?u128 {
        const location = stateLocationContext(ctx) catch {
            // Backends without a replica-local durable checkpoint cannot own
            // repair intents, so read-side discovery is equivalent to empty.
            // Intent creation remains fail-closed through the same capability.
            return null;
        };
        var state = db_mod.index_repair_state.loadAt(alloc, location) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer state.deinit(alloc);
        const i = state.findIndex(index_name) orelse return null;
        return state.entries.items[i].intent.repair_id;
    }

    fn validateActivationState(self: *RepairIndexes, alloc: Allocator, repair_id: u128, expected_owner_epoch: u64) !void {
        const db = self.dbPtr();
        var entry = try self.loadEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        if (entry.intent.root_generation != db.core.root_generation or
            entry.intent.group_id != db.localRepairGroupId())
        {
            return error.RepairOwnershipLost;
        }
        if (entry.intent.phase == .terminal or entry.intent.automation == .paused) return error.IndexRepairNotRunnable;
        if (expected_owner_epoch != 0 and entry.intent.owner_epoch != expected_owner_epoch) {
            return error.RepairOwnershipLost;
        }
        const cfg = db.core.index_manager.get(entry.intent.index_name) orelse return error.IndexRepairConfigurationChanged;
        if (cfg.kind != entry.intent.kind or db_mod.types.indexConfigHash(cfg.*) != entry.intent.config_hash) {
            return error.IndexRepairConfigurationChanged;
        }
    }

    pub fn updateIntent(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
        update: DB.IndexRepairIntentUpdate,
    ) !void {
        return try self.updateIntentWithBindingEffect(
            alloc,
            repair_id,
            update,
            .retain,
            null,
        );
    }

    /// Consume a scheduler audit grant by advancing exactly the durable
    /// revision selected by that scheduler turn. This is a real control
    /// transition, not a read-then-act check: the control mutex plus the
    /// checkpoint CAS linearize the grant against pause/resume, owner handoff,
    /// replay, and retry mutations. The returned revision is the owned turn's
    /// new authority for any wait it publishes after the audit.
    fn consumeAuditGrant(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
        expected_revision: u64,
    ) !?u64 {
        self.updateIntentWithBindingEffect(
            alloc,
            repair_id,
            .{},
            .retain,
            expected_revision,
        ) catch |err| switch (err) {
            error.RepairTransitionConflict => return null,
            else => return err,
        };
        return expected_revision +| 1;
    }

    fn updateIntentWithBindingEffect(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
        update: DB.IndexRepairIntentUpdate,
        binding_effect: DB.IndexRepairBindingEffect,
        required_revision: ?u64,
    ) !void {
        const db = self.dbPtr();
        db_mod.lockAtomic(&db.async_context.index_repair_control_mutex);
        var control_locked = true;
        defer if (control_locked) db.async_context.index_repair_control_mutex.unlock();
        const location = try self.stateLocation();
        var state = try db_mod.index_repair_state.loadAt(alloc, location);
        defer state.deinit(alloc);
        const i = blk: {
            for (state.entries.items, 0..) |entry, entry_i| {
                if (entry.intent.repair_id == repair_id) break :blk entry_i;
            }
            return error.IndexRepairIntentNotFound;
        };
        if (required_revision) |revision| {
            if (state.entries.items[i].intent.revision != revision) {
                return error.RepairTransitionConflict;
            }
        }
        var entry = try state.entries.items[i].clone(alloc);
        defer entry.deinit(alloc);
        const effective_phase = intentEffectivePhase(entry.intent.phase, update);
        if (update.candidate_applied_sequence) |candidate_sequence| {
            const remains_activation_certified = phaseHasActivationCertification(entry.intent.phase) and
                phaseHasActivationCertification(effective_phase);
            if (remains_activation_certified and candidate_sequence != entry.intent.candidate_applied_sequence) {
                return error.InvalidIndexRepairState;
            }
        }
        const previous_admission = repairAdmission(entry.intent);
        const previous_action_required = actionRequired(entry.intent);
        const previous_trigger = entry.intent.trigger;
        const previous_phase = entry.intent.phase;
        const expected = db_mod.index_repair_state.ExpectedTransition{
            .repair_id = entry.intent.repair_id,
            .revision = entry.intent.revision,
            .phase = entry.intent.phase,
            .config_hash = entry.intent.config_hash,
            .root_generation = entry.intent.root_generation,
            .owner_epoch = entry.intent.owner_epoch,
        };
        if (update.attempt_failure != null and update.replace_last_error) {
            return error.InvalidArgument;
        }

        // Stage every fallible owned-string replacement before mutating the
        // cloned intent. Once staging succeeds, the swaps below are infallible;
        // on a later checkpoint failure, entry.deinit owns the replacements.
        const replacement_candidate_path = if (update.replace_candidate_path)
            if (update.candidate_relative_path) |value| try alloc.dupe(u8, value) else null
        else
            null;
        var replacement_candidate_path_owned = replacement_candidate_path != null;
        errdefer if (replacement_candidate_path_owned) alloc.free(replacement_candidate_path.?);

        const replacement_previous_active_path = if (update.replace_previous_active_path)
            if (update.previous_active_relative_path) |value| try alloc.dupe(u8, value) else null
        else
            null;
        var replacement_previous_active_path_owned = replacement_previous_active_path != null;
        errdefer if (replacement_previous_active_path_owned) alloc.free(replacement_previous_active_path.?);

        const replacement_build_resume_key = if (update.replace_build_resume_key)
            if (update.build_resume_key) |value| try alloc.dupe(u8, value) else null
        else
            null;
        var replacement_build_resume_key_owned = replacement_build_resume_key != null;
        errdefer if (replacement_build_resume_key_owned) alloc.free(replacement_build_resume_key.?);

        const replacement_source_replay_resume_key = if (update.replace_source_replay_resume_key)
            if (update.source_replay_resume_key) |value| try alloc.dupe(u8, value) else null
        else
            null;
        var replacement_source_replay_resume_key_owned = replacement_source_replay_resume_key != null;
        errdefer if (replacement_source_replay_resume_key_owned) alloc.free(replacement_source_replay_resume_key.?);

        const replaces_last_error = update.attempt_failure != null or update.replace_last_error;
        const replacement_last_error_source = if (update.attempt_failure) |failure|
            @as(?[]const u8, failure.err_name)
        else
            update.last_error;
        const replacement_last_error = if (replaces_last_error)
            if (replacement_last_error_source) |value| try alloc.dupe(u8, value) else null
        else
            null;
        var replacement_last_error_owned = replacement_last_error != null;
        errdefer if (replacement_last_error_owned) alloc.free(replacement_last_error.?);

        const now_ms = self.nowMs();
        if (update.attempt_failure) |failure| {
            entry.intent.attempt_count = @max(entry.intent.attempt_count, 1);
            entry.intent.failure_streak +|= 1;
            entry.intent.next_retry_at_ms = if (failure.terminal)
                0
            else
                now_ms +| retryDelayMs(repair_id, entry.intent.failure_streak);
        }
        if (update.trigger) |value| entry.intent.trigger = value;
        if (update.operator_job_id) |value| entry.intent.operator_job_id = value;
        if (update.operator_job_created_at_ms) |value| entry.intent.operator_job_created_at_ms = value;
        if (update.phase != null or update.attempt_failure != null) entry.intent.phase = effective_phase;
        if (update.replace_candidate_path) {
            if (entry.intent.candidate_relative_path) |value| alloc.free(value);
            entry.intent.candidate_relative_path = replacement_candidate_path;
            replacement_candidate_path_owned = false;
        }
        if (update.previous_pointer_captured) |value| entry.intent.previous_pointer_captured = value;
        if (update.replace_previous_active_path) {
            if (entry.intent.previous_active_relative_path) |value| alloc.free(value);
            entry.intent.previous_active_relative_path = replacement_previous_active_path;
            replacement_previous_active_path_owned = false;
        }
        if (update.candidate_applied_sequence) |value| entry.intent.candidate_applied_sequence = value;
        if (update.replace_build_resume_key) {
            if (entry.intent.build_resume_key) |value| alloc.free(value);
            entry.intent.build_resume_key = replacement_build_resume_key;
            replacement_build_resume_key_owned = false;
        }
        if (update.build_reprocessed) |value| entry.intent.build_reprocessed = value;
        if (update.replace_source_replay_resume_key) {
            if (entry.intent.source_replay_resume_key) |value| alloc.free(value);
            entry.intent.source_replay_resume_key = replacement_source_replay_resume_key;
            replacement_source_replay_resume_key_owned = false;
        }
        if (update.source_replay_reprocessed) |value| entry.intent.source_replay_reprocessed = value;
        if (update.source_replay_state) |value| entry.intent.source_replay_state = value;
        if (update.estimated_candidate_bytes) |value| entry.intent.estimated_candidate_bytes = value;
        if (update.planned_disk_bytes) |value| entry.intent.planned_disk_bytes = value;
        if (update.target_sequence) |value| entry.intent.target_sequence = value;
        if (update.attempt_count) |value| entry.intent.attempt_count = value;
        if (update.failure_streak) |value| entry.intent.failure_streak = value;
        if (update.next_retry_at_ms) |value| entry.intent.next_retry_at_ms = value;
        if (update.automation) |value| entry.intent.automation = value;
        if (update.owner_epoch) |value| entry.intent.owner_epoch = value;
        if (replaces_last_error) {
            if (entry.intent.last_error) |value| alloc.free(value);
            entry.intent.last_error = replacement_last_error;
            replacement_last_error_owned = false;
        }
        entry.intent.updated_at_ms = now_ms;
        const blocks_service = intentBlocksService(entry.intent);
        const preserve_serviceability = blocks_service and
            previous_admission == .blocked and
            previous_trigger == entry.intent.trigger and
            previous_phase == entry.intent.phase;
        var admission_fence: ?db_mod.index_manager_mod.IndexManager.RepairAdmissionFence = if (blocks_service and
            !preserve_serviceability)
            try db.core.index_manager.beginRepairAdmissionFence(entry.intent.index_name)
        else
            null;
        defer if (admission_fence) |*fence| {
            db.core.index_manager.cancelRepairAdmissionFence(fence, entry.intent.index_name);
        };
        const control_revision = try db_mod.index_repair_state.putEntryAt(alloc, location, state.identity, expected, entry);
        const persisted_revision = entry.intent.revision +| 1;
        if (admission_fence) |*fence| {
            db.core.index_manager.finishRepairAdmissionFence(
                fence,
                entry.intent.index_name,
                DB.repairAdmissionVersion(entry.intent, persisted_revision, control_revision),
                true,
                false,
            );
        } else {
            db.core.index_manager.publishRepairAdmission(
                entry.intent.index_name,
                DB.repairAdmissionVersion(entry.intent, persisted_revision, control_revision),
                blocks_service,
                preserve_serviceability,
            );
        }
        noteSchedulerUpsert(db.async_context, entry.intent, persisted_revision, control_revision);
        if (binding_effect == .release) {
            db.core.index_manager.releaseFailedIndexLoadRepairClaim(
                entry.intent.index_name,
                entry.intent.repair_id,
            );
        }
        // Hooks may enqueue work or inspect the DB. Publish them only after the
        // durable transition, scheduler projection, and binding effect are one
        // completed control transaction.
        db.async_context.index_repair_control_mutex.unlock();
        control_locked = false;
        const admission = repairAdmission(entry.intent);
        const action_required = actionRequired(entry.intent);
        if (previous_admission != admission or
            previous_action_required != action_required)
        {
            notifyVisibilityHook(
                db.async_context,
                .index_repair_pending,
                entry.intent,
                persisted_revision,
                previous_admission,
                previous_action_required,
                false,
            );
        }
        const persisted_phase = update.phase orelse
            if (update.attempt_failure != null) entry.intent.phase else null;
        if (persisted_phase) |phase| {
            if (self.shadow_hook) |hook| {
                if (hook.after_phase_persisted) |after_phase| {
                    try after_phase(hook.ptr, db, repair_id, phase);
                }
            }
        }
    }

    pub fn createGenerationIntent(
        self: *RepairIndexes,
        alloc: Allocator,
        cfg: db_mod.types.IndexConfig,
        trigger: db_mod.index_repair_state.Trigger,
        operator_job_id: u64,
        operator_job_created_at_ms: u64,
        last_error: ?[]const u8,
    ) !u128 {
        return try self.createGenerationIntentAtTarget(
            alloc,
            cfg,
            trigger,
            operator_job_id,
            operator_job_created_at_ms,
            last_error,
            null,
            .not_required,
            .repair,
        );
    }

    pub fn createGenerationIntentAtTarget(
        self: *RepairIndexes,
        alloc: Allocator,
        cfg: db_mod.types.IndexConfig,
        trigger: db_mod.index_repair_state.Trigger,
        operator_job_id: u64,
        operator_job_created_at_ms: u64,
        last_error: ?[]const u8,
        minimum_target_sequence: ?u64,
        source_replay_state: db_mod.index_repair_state.SourceReplayState,
        work_class: db_mod.index_repair_state.WorkClass,
    ) !u128 {
        const db = self.dbPtr();
        db_mod.lockAtomic(&db.async_context.index_repair_control_mutex);
        var control_locked = true;
        defer if (control_locked) db.async_context.index_repair_control_mutex.unlock();
        var state = try self.loadOrCreateCurrentState(alloc);
        defer state.deinit(alloc);
        if (state.findIndex(cfg.name)) |i| return state.entries.items[i].intent.repair_id;
        const location = try self.stateLocation();
        const now_ms = self.nowMs();
        const target_sequence = @max(
            db.core.nextDerivedSequence(),
            minimum_target_sequence orelse 0,
        );
        const repair_id = try db_mod.index_repair_state.newRepairId(alloc);
        const index_name = try alloc.dupe(u8, cfg.name);
        var intent_allocations_transferred = false;
        errdefer if (!intent_allocations_transferred) alloc.free(index_name);
        const owned_last_error = if (last_error) |value| try alloc.dupe(u8, value) else null;
        errdefer if (!intent_allocations_transferred) if (owned_last_error) |value| alloc.free(value);
        var intent = db_mod.index_repair_state.IndexRepairIntent{
            .repair_id = repair_id,
            .db_identity = state.identity.db_identity,
            .group_id = db.localRepairGroupId(),
            .replica_id = state.identity.replica_id,
            .root_generation = state.identity.root_generation,
            .index_name = index_name,
            .kind = cfg.kind,
            .config_hash = db_mod.types.indexConfigHash(cfg),
            .trigger = trigger,
            .work_class = work_class,
            .operator_job_id = operator_job_id,
            .operator_job_created_at_ms = operator_job_created_at_ms,
            .detected_sequence = target_sequence,
            .target_sequence = target_sequence,
            .started_at_ms = now_ms,
            .updated_at_ms = now_ms,
            .owner_epoch = 0,
            .last_error = owned_last_error,
            .source_replay_state = source_replay_state,
        };
        intent_allocations_transferred = true;
        defer intent.deinit(alloc);
        const blocks_service = intentBlocksService(intent);
        var admission_fence: ?db_mod.index_manager_mod.IndexManager.RepairAdmissionFence = if (blocks_service)
            try db.core.index_manager.beginRepairAdmissionFence(intent.index_name)
        else
            null;
        defer if (admission_fence) |*fence| {
            db.core.index_manager.cancelRepairAdmissionFence(fence, intent.index_name);
        };
        const control_revision = db_mod.index_repair_state.putEntryAt(alloc, location, state.identity, null, .{ .intent = intent }) catch |err| switch (err) {
            // Another operator/maintenance caller may win creation between the
            // optimistic state read and the checkpoint CAS. Adopt that durable
            // intent; callers subsequently attach their job identity or
            // promote its safety classification with revision-fenced retries.
            error.RepairTransitionConflict => return (try self.idForIndex(alloc, cfg.name)) orelse return err,
            else => return err,
        };
        const persisted_revision: u64 = 1;
        if (admission_fence) |*fence| {
            db.core.index_manager.finishRepairAdmissionFence(
                fence,
                intent.index_name,
                DB.repairAdmissionVersion(intent, persisted_revision, control_revision),
                true,
                false,
            );
        }
        noteSchedulerUpsert(db.async_context, intent, persisted_revision, control_revision);
        db.async_context.index_repair_control_mutex.unlock();
        control_locked = false;
        notifyVisibilityHook(
            db.async_context,
            .index_repair_pending,
            intent,
            persisted_revision,
            .serviceable,
            false,
            false,
        );
        return repair_id;
    }

    pub fn createOperatorGenerationIntent(
        self: *RepairIndexes,
        alloc: Allocator,
        cfg: db_mod.types.IndexConfig,
        operator_job_id: u64,
        operator_job_created_at_ms: u64,
    ) !u128 {
        const db = self.dbPtr();
        const repair_id = try self.createGenerationIntent(
            alloc,
            cfg,
            if (cfg.kind == .dense_vector) .operator_generation_validation else .operator_generation_rebuild,
            operator_job_id,
            operator_job_created_at_ms,
            null,
        );
        try db.attachOperatorRepairRequest(
            alloc,
            repair_id,
            operator_job_id,
            operator_job_created_at_ms,
        );
        return repair_id;
    }

    pub fn ensureAutomaticDenseGenerationIntent(
        self: *RepairIndexes,
        alloc: Allocator,
        cfg: db_mod.types.IndexConfig,
        trigger: db_mod.index_repair_state.Trigger,
        last_error: []const u8,
    ) !u128 {
        return (try self.ensureAutomaticDenseGenerationIntentOutcome(
            alloc,
            cfg,
            trigger,
            last_error,
        )).repair_id;
    }

    pub fn ensureAutomaticDenseGenerationIntentOutcome(
        self: *RepairIndexes,
        alloc: Allocator,
        cfg: db_mod.types.IndexConfig,
        trigger: db_mod.index_repair_state.Trigger,
        last_error: []const u8,
    ) !DB.AutomaticDenseGenerationRepairIntentOutcome {
        const db = self.dbPtr();
        std.debug.assert(trigger == .artifact_coverage_mismatch or
            trigger == .artifact_counter_missing or
            trigger == .projection_generation_invalid or
            trigger == .storage_format_migration);

        // Correctness repairs close the query gate before persistence. A
        // physical-format migration is different: v1 remains the proven
        // serving generation while its v2 shadow is built and caught up.
        const close_query_gate = trigger != .storage_format_migration;
        const was_unavailable = db.core.index_manager.repairUnavailable(cfg.name);
        if (close_query_gate) try db.core.index_manager.markUnscopedRepairUnavailable(cfg.name);

        const existing_repair_id = try self.idForIndex(alloc, cfg.name);
        const repair_id = existing_repair_id orelse try self.createGenerationIntent(
            alloc,
            cfg,
            trigger,
            0,
            0,
            last_error,
        );
        if (existing_repair_id == null) {
            std.log.warn("dense generation repair intent created index={s} trigger={s} reason={s}", .{
                cfg.name,
                @tagName(trigger),
                last_error,
            });
        }

        // Creation races with an operator request are harmless: re-read the
        // winning intent and promote its safety classification when needed.
        var attempts: usize = 0;
        while (attempts < 4) : (attempts += 1) {
            var entry = try self.loadEntryById(alloc, repair_id);
            defer entry.deinit(alloc);
            if (DB.automaticDenseGenerationRepairTriggerPriority(trigger) <=
                DB.automaticDenseGenerationRepairTriggerPriority(entry.intent.trigger)) return .{
                .repair_id = repair_id,
                .made_progress = existing_repair_id == null or (close_query_gate and !was_unavailable),
            };
            self.updateIntent(alloc, repair_id, .{
                .trigger = trigger,
                .replace_last_error = true,
                .last_error = last_error,
            }) catch |err| switch (err) {
                error.RepairTransitionConflict => continue,
                else => return err,
            };
            return .{ .repair_id = repair_id, .made_progress = true };
        }
        return error.RepairTransitionConflict;
    }

    pub fn validateSourcePreflight(self: *RepairIndexes, alloc: Allocator, cfg: db_mod.types.IndexConfig) !void {
        const db = self.dbPtr();
        if (!try db_mod.index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg)) return;
        const target_count = try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, cfg.name);
        if (target_count == null) return error.RepairSourceCoverageIncomplete;
    }

    fn currentIdMatches(self: *const RepairIndexes, alloc: Allocator, index_name: []const u8, expected_repair_id: ?u128) !?u128 {
        const repair_id = (try self.idForIndex(alloc, index_name)) orelse {
            if (expected_repair_id != null) return error.StaleIndexRepairControl;
            return null;
        };
        if (expected_repair_id) |expected| {
            if (repair_id != expected) return error.StaleIndexRepairControl;
        }
        return repair_id;
    }

    pub fn pauseAutomatic(self: *RepairIndexes, alloc: Allocator, index_name: []const u8, expected_repair_id: ?u128) !bool {
        const repair_id = (try self.currentIdMatches(alloc, index_name, expected_repair_id)) orelse return false;
        try self.updateIntent(alloc, repair_id, .{ .automation = .paused });
        _ = self.requestActiveCancellation(index_name);
        return true;
    }

    pub fn cancelCurrentAttempt(self: *RepairIndexes, alloc: Allocator, index_name: []const u8, expected_repair_id: ?u128) !bool {
        _ = (try self.currentIdMatches(alloc, index_name, expected_repair_id)) orelse return false;
        return self.requestActiveCancellation(index_name);
    }

    pub fn resumeAutomatic(self: *RepairIndexes, alloc: Allocator, index_name: []const u8, expected_repair_id: ?u128) !bool {
        const repair_id = (try self.currentIdMatches(alloc, index_name, expected_repair_id)) orelse return false;
        try self.updateIntent(alloc, repair_id, .{
            .automation = .enabled,
            .next_retry_at_ms = 0,
            .replace_last_error = true,
        });
        return true;
    }

    /// Quiesce repair for deletion while retaining its durable gate. The caller
    /// owns the returned lease and removes the checkpoint only after catalog
    /// absence and marker deletion commit atomically.
    pub fn prepareForDeletion(self: *RepairIndexes, alloc: Allocator, index_name: []const u8) !?u128 {
        const db = self.dbPtr();
        const repair_id = (try self.idForIndex(alloc, index_name)) orelse return null;
        if (!(try self.beginLease(index_name))) return error.IndexRepairInProgress;
        errdefer self.endLease(index_name);
        var entry = try self.loadEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        if (entry.intent.candidate_relative_path) |candidate| {
            if (!try db.core.index_manager.isRepairCandidateActive(index_name, candidate)) {
                try self.discardInactiveCandidate(alloc, repair_id);
            }
        }
        return repair_id;
    }

    pub fn removeOrphanedIntents(self: *RepairIndexes, alloc: Allocator) !void {
        const db = self.dbPtr();
        var state = self.loadState(alloc) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer state.deinit(alloc);
        var orphan_ids = std.ArrayListUnmanaged(u128).empty;
        defer orphan_ids.deinit(alloc);
        for (state.entries.items) |entry| {
            if (db.core.index_manager.get(entry.intent.index_name) == null) {
                try orphan_ids.append(alloc, entry.intent.repair_id);
            }
        }
        for (orphan_ids.items) |repair_id| try self.removeIntentAndPin(alloc, repair_id);
    }

    /// Requires the DB apply lock. Fresh admission must not adopt repair state
    /// from a catalog generation whose deletion already committed.
    pub fn removeOrphanedIntentForFreshAdmission(self: *RepairIndexes, alloc: Allocator, index_name: []const u8) !void {
        const db = self.dbPtr();
        if (db.core.index_manager.get(index_name) != null) return;
        const repair_id = (try self.idForIndex(alloc, index_name)) orelse return;
        try self.removeIntentAndPin(alloc, repair_id);
    }

    pub fn recordAttemptFailure(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
        err_name: []const u8,
        terminal: bool,
    ) !void {
        if (terminal) std.log.warn("index repair failed terminally repair_id={x} err={s}", .{ repair_id, err_name });
        try self.updateIntentWithBindingEffect(
            alloc,
            repair_id,
            .{
                .attempt_failure = .{
                    .err_name = err_name,
                    .terminal = terminal,
                },
            },
            if (terminal) .release else .retain,
            null,
        );
    }

    pub fn failureIsTerminal(err: anyerror) bool {
        return switch (err) {
            error.InvalidArgument,
            error.NotFound,
            error.UnsupportedOperation,
            error.InvalidIndexRepairState,
            error.InvalidRepairCandidatePath,
            error.ReplicaIdentityMismatch,
            error.DurableIndexRepairStateUnavailable,
            error.RepairSourceCoverageIncomplete,
            error.IndexRepairConfigurationChanged,
            => true,
            else => false,
        };
    }

    pub fn removeIntentAndPin(self: *RepairIndexes, alloc: Allocator, repair_id: u128) !void {
        const db = self.dbPtr();
        return try removeIntentAndPinContext(db.async_context, alloc, repair_id);
    }

    pub fn removeIntentAndPinContext(ctx: *db_mod.AsyncContext, alloc: Allocator, repair_id: u128) !void {
        db_mod.lockAtomic(&ctx.index_repair_control_mutex);
        var control_locked = true;
        defer if (control_locked) ctx.index_repair_control_mutex.unlock();
        // Pin creation, pin removal, the in-memory pressure gate, and replay
        // truncation must observe one serialization order. Per-index repair
        // leases are insufficient because two different indexes may complete
        // concurrently.
        const repair_replay_mutex = ctx.repair_replay_mutex orelse return error.DurableIndexRepairStateUnavailable;
        repair_replay_mutex.lockUncancelable(ctx.index_manager.checkpointIo());
        defer repair_replay_mutex.unlock(ctx.index_manager.checkpointIo());
        const location = try stateLocationContext(ctx);
        var state = try db_mod.index_repair_state.loadAt(alloc, location);
        defer state.deinit(alloc);
        const entry = blk: {
            for (state.entries.items) |candidate| {
                if (candidate.intent.repair_id == repair_id) break :blk candidate;
            }
            return error.IndexRepairIntentNotFound;
        };
        if (entry.intent.phase == .cleanup) try DB.persistOperatorRepairCompletionContext(ctx, alloc, entry.intent);
        var remaining_replay_pin = false;
        for (state.entries.items) |candidate| {
            if (candidate.intent.repair_id != repair_id and candidate.pin != null) {
                remaining_replay_pin = true;
                break;
            }
        }
        // For managed admission, clear the primary-store lifecycle marker only
        // after activation published a clean generation. Do this before
        // removing the checkpoint intent: a crash between the two leaves
        // harmless resumable checkpoint debt, never an admitted generation
        // with no proof.
        const admission_key = try db_mod.internal_keys.managedIndexAdmissionKeyAlloc(alloc, entry.intent.index_name);
        defer alloc.free(admission_key);
        ctx.store.delete(admission_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        const control_revision = try db_mod.index_repair_state.removeEntryAndPinAt(alloc, location, state.identity, .{
            .repair_id = repair_id,
            .revision = entry.intent.revision,
            .phase = entry.intent.phase,
            .config_hash = entry.intent.config_hash,
            .root_generation = entry.intent.root_generation,
            .owner_epoch = entry.intent.owner_epoch,
        });
        noteSchedulerRemove(ctx, state.identity, repair_id, control_revision);
        ctx.index_repair_replay_pinned.store(remaining_replay_pin, .release);
        ctx.index_manager.publishRepairAdmission(
            entry.intent.index_name,
            DB.repairAdmissionVersion(entry.intent, entry.intent.revision +| 1, control_revision),
            false,
            false,
        );
        ctx.index_manager.releaseFailedIndexLoadRepairClaim(
            entry.intent.index_name,
            entry.intent.repair_id,
        );
        ctx.index_repair_control_mutex.unlock();
        control_locked = false;
        notifyVisibilityHook(
            ctx,
            .index_repair_cleared,
            entry.intent,
            entry.intent.revision,
            repairAdmission(entry.intent),
            actionRequired(entry.intent),
            remaining_replay_pin,
        );
    }

    pub fn discardInactiveCandidate(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
    ) !void {
        return try self.discardInactiveCandidateWithTerminal(
            alloc,
            repair_id,
            null,
        );
    }

    /// Deletes only a proven-inactive candidate, then publishes its durable
    /// disposition in one repair-control checkpoint. A manual predecessor
    /// failure must not expose an intermediate runnable preflight intent if the
    /// process stops after filesystem cleanup.
    pub fn discardInactiveCandidateWithTerminal(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
        terminal_reason: ?[]const u8,
    ) !void {
        const db = self.dbPtr();
        var entry = try self.loadEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        const candidate = entry.intent.candidate_relative_path orelse return;
        try db_mod.index_repair_state.validateCandidateRelativePath(entry.intent.index_name, candidate);
        if (try db.core.index_manager.isRepairCandidateActive(entry.intent.index_name, candidate)) {
            return error.RepairCandidateAlreadyActive;
        }
        const separator = std.mem.indexOfScalar(u8, candidate, '/') orelse return error.InvalidRepairCandidatePath;
        const shadow_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ db.core.index_manager.base_path, candidate[0..separator] });
        defer alloc.free(shadow_root);
        if (entry.intent.kind == .algebraic) {
            // Algebraic generations live in the primary store and may be much
            // larger than a scheduler turn. The directory is their durable
            // namespace marker: clear only the writer marker here, detach the
            // intent below, then let paged orphan collection drain the keys.
            try db.core.index_manager.clearRepairShadow(alloc, shadow_root);
        } else if (db.core.index_manager.generation_storage orelse db.primary_lsm_storage) |storage| {
            try storage.deleteTree(shadow_root);
        } else if (comptime builtin.os.tag == .freestanding) {
            return error.UnsupportedPlatform;
        } else {
            var io_impl = db_mod.threadedIo();
            defer io_impl.deinit();
            // `deleteTree` is idempotent for an absent root. A cancelled
            // pre-checkpoint build may already have removed its non-reopenable
            // shadow in the unwind path.
            try std.Io.Dir.cwd().deleteTree(io_impl.io(), shadow_root);
        }
        const reset = DB.IndexRepairIntentUpdate{
            .phase = .preflight,
            .replace_candidate_path = true,
            .replace_build_resume_key = true,
            .build_reprocessed = 0,
            .candidate_applied_sequence = 0,
            .target_sequence = db.core.nextDerivedSequence(),
            .failure_streak = 0,
            .next_retry_at_ms = 0,
            .replace_last_error = true,
        };
        if (terminal_reason) |reason| {
            var terminal = reset;
            terminal.phase = null;
            terminal.failure_streak = null;
            terminal.next_retry_at_ms = null;
            terminal.replace_last_error = false;
            terminal.attempt_failure = .{
                .err_name = reason,
                .terminal = true,
            };
            try self.updateIntentWithBindingEffect(
                alloc,
                repair_id,
                terminal,
                .release,
                null,
            );
        } else {
            try self.updateIntent(alloc, repair_id, reset);
        }
        if (entry.intent.kind == .algebraic) self.scheduleInactiveShadowCleanup();
    }

    /// Completes the idempotent half of activation recovery. Certification
    /// failures are data outcomes rather than raw scheduler errors: the caller
    /// must durably roll the untrusted incarnation back or expose an exact
    /// action-required terminal state.
    fn reconcileActivated(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
    ) !DB.ActivatedIndexRepairReconciliation {
        const db = self.dbPtr();
        // Restart reconciliation is the second half of the same catalog
        // transaction as ordinary activation. Serialize it with every live
        // index publication and foreground apply, then reload the durable
        // intent inside that fence so pointer, runtime, config, manifest,
        // checkpoint, and admission retirement describe one incarnation.
        if (comptime builtin.is_test) {
            if (DB.test_index_repair_reconcile_fence_hook) |hook| {
                if (hook.before_fence) |before_fence| try before_fence(hook.ptr, db, repair_id);
            }
        }
        var structural_guard = db.beginIndexStructuralMutation("index repair activation reconciliation", "*");
        defer structural_guard.deinit();
        db_mod.lockApply(db);
        defer db.core.unlockApply();

        var entry = try self.loadEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        switch (entry.intent.phase) {
            .activating, .validating, .cleanup, .rolling_back => {},
            else => return .not_active,
        }
        if (entry.intent.root_generation != db.core.root_generation or
            entry.intent.group_id != db.localRepairGroupId())
        {
            return error.RepairOwnershipLost;
        }
        const candidate = entry.intent.candidate_relative_path orelse return .not_active;
        try db_mod.index_repair_state.validateCandidateRelativePath(entry.intent.index_name, candidate);
        const active_pointer = try db.core.index_manager.capturePublishedIndexRootPointerForRecovery(
            entry.intent.index_name,
        );
        defer if (active_pointer) |value| alloc.free(value);
        if (active_pointer == null or !std.mem.eql(u8, active_pointer.?, candidate)) return .not_active;
        const cfg = db.core.index_manager.get(entry.intent.index_name) orelse return .not_active;
        if (cfg.kind != entry.intent.kind or db_mod.types.indexConfigHash(cfg.*) != entry.intent.config_hash) {
            return error.IndexRepairConfigurationChanged;
        }
        if (!db.core.index_manager.has(entry.intent.index_name) or
            db.core.index_manager.loadFailure(entry.intent.index_name) != null)
        {
            return .not_active;
        }

        const candidate_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ db.core.index_manager.base_path, candidate });
        defer alloc.free(candidate_path);
        const certification = try db_mod.index_generation_manifest.certifyReadyWithStorage(
            db.core.index_manager.generation_storage,
            alloc,
            candidate_path,
            repair_id,
            entry.intent.index_name,
            entry.intent.config_hash,
        );
        const manifest_sequence = switch (certification) {
            .ready => |sequence| sequence,
            .missing => return .{ .certification_failed = .manifest_missing },
            .invalid => return .{ .certification_failed = .manifest_invalid },
            .identity_mismatch => return .{ .certification_failed = .manifest_identity_mismatch },
        };
        if (manifest_sequence != entry.intent.candidate_applied_sequence) {
            return .{ .certification_failed = .manifest_sequence_mismatch };
        }
        if (comptime builtin.is_test) {
            if (DB.test_index_repair_reconcile_fence_hook) |hook| {
                if (hook.after_validation) |after_validation| try after_validation(hook.ptr, db, repair_id);
            }
        }
        try db.core.index_manager.syncIndexByName(entry.intent.index_name, true);
        const checkpoint = try db.core.loadProjectionCheckpoint(alloc, entry.intent.index_name);
        const applied_sequence = @max(
            checkpoint.applied_sequence,
            @max(entry.intent.candidate_applied_sequence, manifest_sequence),
        );
        if (entry.intent.phase != .cleanup or checkpoint.status != .clean or checkpoint.config_hash != entry.intent.config_hash) {
            const update = db_mod.apply_state.AppliedSequenceUpdate{
                .index_name = entry.intent.index_name,
                .sequence = applied_sequence,
                .status = .clean,
                .generation = checkpoint.generation +| 1,
                .config_hash = entry.intent.config_hash,
            };
            try DB.saveIndexStatusSnapshots(alloc, db.core.store, db.core.index_manager, &[_]db_mod.apply_state.AppliedSequenceUpdate{update});
            try db.core.saveAppliedSequence(entry.intent.index_name, applied_sequence);
            try db.core.saveProjectionCheckpoint(entry.intent.index_name, .{
                .applied_sequence = update.sequence,
                .status = update.status,
                .generation = update.generation,
                .config_hash = update.config_hash,
            });
            try self.updateIntent(alloc, repair_id, .{
                .phase = .cleanup,
                .target_sequence = applied_sequence,
                .next_retry_at_ms = 0,
                .replace_last_error = true,
            });
        }
        if (comptime builtin.is_test) {
            if (DB.test_index_repair_reconcile_fence_hook) |hook| {
                if (hook.before_retirement) |before_retirement| try before_retirement(hook.ptr, db, repair_id);
            }
        }
        try self.removeIntentAndPin(alloc, repair_id);
        self.scheduleInactiveShadowCleanup();
        return .committed;
    }

    pub fn activatedRollbackFailureDisposition(
        action: db_mod.index_manager_mod.IndexManager.IndexLoadRecoveryAction,
    ) DB.ActivatedIndexRepairRollback {
        return switch (action) {
            .retry_open => .retry_deferred,
            .rebuild_from_artifacts => .rebuild_required,
            .manual_intervention => .action_required,
        };
    }

    /// Rolls a failed-to-load activated candidate back to the exact pointer
    /// captured before activation. `rolling_back` is checkpointed while the
    /// structural/apply fence and exact quarantine claim are both held, before
    /// the pointer changes. Restart can therefore distinguish a crash before
    /// activation from an interrupted rollback and idempotently finish from
    /// either pointer. Slow reopen I/O remains outside the service fence while
    /// the exact repair claim prevents discovery or an ordinary retry from
    /// publishing underneath it.
    pub fn rollbackUnavailableActivated(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
        certification_failure: ?DB.ActivatedIndexRepairCertificationFailure,
    ) !DB.ActivatedIndexRepairRollback {
        const db = self.dbPtr();
        var entry = try self.loadEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        if (!entry.intent.previous_pointer_captured) return .not_active;
        const candidate = entry.intent.candidate_relative_path orelse return .not_active;
        const rollback_certification_failure = certification_failure orelse blk: {
            if (entry.intent.phase != .rolling_back) break :blk null;
            break :blk DB.ActivatedIndexRepairCertificationFailure.fromReason(
                entry.intent.last_error orelse break :blk null,
            );
        };
        var claim: ?db_mod.index_manager_mod.IndexManager.FailedIndexLoadRepairClaim = null;
        var predecessor_loaded = false;
        {
            var structural_guard = db.beginIndexStructuralMutation("index repair rollback", entry.intent.index_name);
            defer structural_guard.deinit();
            try db.lockApplyForPortableRuntime();
            defer db.core.unlockApply();

            const active_pointer = try db.core.index_manager.capturePublishedIndexRootPointerForRecovery(
                entry.intent.index_name,
            );
            defer if (active_pointer) |value| alloc.free(value);
            const candidate_active = active_pointer != null and
                std.mem.eql(u8, active_pointer.?, candidate);
            const predecessor_active = if (entry.intent.previous_active_relative_path) |previous|
                active_pointer != null and std.mem.eql(u8, active_pointer.?, previous)
            else
                active_pointer == null;

            if (entry.intent.phase != .rolling_back and !candidate_active) return .not_active;
            if (entry.intent.phase == .rolling_back and !candidate_active and !predecessor_active) {
                return error.RepairOwnershipLost;
            }

            if (db.core.index_manager.failedIndexLoadSnapshot(entry.intent.index_name)) |failure| {
                claim = switch (db.core.index_manager.claimFailedIndexLoadRepair(
                    entry.intent.index_name,
                    .{ .exact = failure.token },
                    entry.intent.repair_id,
                )) {
                    .claimed, .already_claimed => |value| value,
                    .absent, .mismatch, .claimed_by_other => return error.RepairOwnershipLost,
                };
            } else if (predecessor_active and db.core.index_manager.has(entry.intent.index_name)) {
                // A restart may have already opened the restored predecessor
                // before the durable rollback owner resumes.
                predecessor_loaded = true;
            } else if (!candidate_active or rollback_certification_failure == null) {
                // A loaded active candidate is reconciled by the caller before
                // entering rollback. Absence here has no safe retry identity;
                // an inactive pointer with no loaded predecessor is likewise
                // not evidence that rollback completed.
                return error.RepairOwnershipLost;
            }

            if (entry.intent.phase != .rolling_back) {
                var checkpointed = false;
                defer if (!checkpointed) db.core.index_manager.releaseFailedIndexLoadRepairClaim(
                    entry.intent.index_name,
                    entry.intent.repair_id,
                );
                try self.updateIntent(alloc, repair_id, .{
                    .phase = .rolling_back,
                    .next_retry_at_ms = 0,
                    .last_error = if (rollback_certification_failure) |failure| failure.reason() else null,
                    .replace_last_error = true,
                });
                checkpointed = true;
            }
            if (candidate_active) {
                if (rollback_certification_failure) |failure| {
                    claim = try db.core.index_manager.rollbackUncertifiedActiveIndexRoot(
                        entry.intent.index_name,
                        candidate,
                        entry.intent.previous_active_relative_path,
                        entry.intent.config_hash,
                        failure.reason(),
                        repair_id,
                    );
                } else {
                    try db.core.index_manager.restoreActiveIndexRootPointer(
                        entry.intent.index_name,
                        candidate,
                        entry.intent.previous_active_relative_path,
                    );
                }
            }
        }

        if (predecessor_loaded) {
            try self.discardInactiveCandidate(alloc, repair_id);
            return .prior_generation_restored;
        }
        if (comptime builtin.is_test) {
            if (DB.test_index_repair_rollback_retry_hook) |hook| {
                try hook.before_retry(hook.ptr, db, repair_id);
            }
        }
        const retry = try db.maintenance.retryQuarantinedIndexLoadTarget(.{
            .name = entry.intent.index_name,
            .token = claim.?.token,
            .authority = .{ .repair_claim = entry.intent.repair_id },
        }, true);
        switch (retry.target_outcome) {
            .recovered => {
                try self.discardInactiveCandidate(alloc, repair_id);
                return .prior_generation_restored;
            },
            .attempted_failed => switch (activatedRollbackFailureDisposition(
                retry.target_action orelse return error.RepairOwnershipLost,
            )) {
                .retry_deferred => {
                    // Retain both the rollback phase and exact claim. The
                    // durable repair backoff owns subsequent retries, avoiding
                    // a hot loop and preventing generic retry from racing it.
                    try self.recordAttemptFailure(
                        alloc,
                        repair_id,
                        if (rollback_certification_failure) |failure|
                            failure.reason()
                        else
                            "rollback_predecessor_retry_open",
                        false,
                    );
                    return .retry_deferred;
                },
                .action_required => {
                    try self.discardInactiveCandidateWithTerminal(
                        alloc,
                        repair_id,
                        if (rollback_certification_failure) |failure|
                            failure.reason()
                        else
                            "rollback_predecessor_manual_intervention",
                    );
                    return .action_required;
                },
                .rebuild_required => {
                    try self.discardInactiveCandidate(alloc, repair_id);
                    return .rebuild_required;
                },
                .not_active, .prior_generation_restored => unreachable,
            },
            // An exact repair-authorized forced retry is always targeted and
            // ignores backoff. Any other outcome means its incarnation or
            // ownership changed while detached I/O was in flight.
            .not_targeted, .absent, .stale, .repair_claimed, .backoff => return error.RepairOwnershipLost,
        }
    }

    pub fn advanceIntent(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
        options: db_mod.types.ArtifactRepairRunOptions,
    ) anyerror!DB.IndexRepairAdvanceResult {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("advanceIndexRepairIntent", .{ db, alloc, repair_id, options });
        return try self.advanceScheduledIntent(alloc, repair_id, options, null, false);
    }

    pub fn advanceScheduledIntent(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
        options: db_mod.types.ArtifactRepairRunOptions,
        scheduled_revision: ?u64,
        audit_due: bool,
    ) anyerror!DB.IndexRepairAdvanceResult {
        const db = self.dbPtr();
        // Resolve the durable name before taking the process-local lease, then
        // reload after acquisition. A pause or terminal transition which wins
        // between those operations must be observed by this owner; using the
        // locator snapshot would let an acknowledged pause start one more
        // repair quantum. The checkpoint is already read repeatedly by a
        // material state-machine turn, while deferred intents remain on the
        // O(1) scheduler path and never pay this ownership handoff cost.
        var locator = try self.loadEntryById(alloc, repair_id);
        defer locator.deinit(alloc);
        if (!(try self.beginLease(locator.intent.index_name))) {
            return .{
                .repair_id = repair_id,
                .busy = true,
            };
        }
        defer self.endLease(locator.intent.index_name);

        if (comptime builtin.is_test) {
            if (DB.test_index_repair_advance_lease_hook) |hook| {
                if (hook.after_acquire) |after_acquire| try after_acquire(hook.ptr, db, repair_id);
            }
        }
        var entry = try self.loadEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        if (!std.mem.eql(u8, locator.intent.index_name, entry.intent.index_name)) {
            return error.InvalidIndexRepairState;
        }
        if (comptime builtin.is_test) {
            if (DB.test_index_repair_advance_lease_hook) |hook| {
                if (hook.after_reload) |after_reload| try after_reload(hook.ptr, db, repair_id);
            }
        }
        // An audit grant is a process-local acceleration token for one exact
        // durable revision. Selection releases the scheduler mutex before the
        // repair lease is acquired, so a source-replay, retry, pause/resume, or
        // concurrent owner transition may supersede it in that interval. The
        // reloaded durable entry is authoritative; never let a stale token skip
        // the newly revised generation's bounded convergence wait.
        const audit_revision = if (audit_due and
            scheduled_revision != null and
            entry.intent.revision == scheduled_revision.?)
            scheduled_revision
        else
            null;
        var run_control = DB.DurableIndexRepairRunControl{
            .db = db,
            .index_name = entry.intent.index_name,
            .upstream = options.cancel_check,
            .ownership = options.activation_check,
        };
        var owned_options = options;
        owned_options.cancel_check = .{
            .ptr = &run_control,
            .is_requested = DB.DurableIndexRepairRunControl.cancelled,
        };
        return try self.advanceIntentOwned(
            alloc,
            repair_id,
            owned_options,
            &entry,
            audit_revision,
        );
    }

    /// Executes one repair quantum while `active_index_repairs[index_name]` is
    /// held by the caller. Keeping the lease outside every phase makes pause,
    /// cancel, deletion, rollback, and activation share one ownership domain.
    fn advanceIntentOwned(
        self: *RepairIndexes,
        alloc: Allocator,
        repair_id: u128,
        options: db_mod.types.ArtifactRepairRunOptions,
        entry: *db_mod.index_repair_state.Entry,
        audit_revision: ?u64,
    ) anyerror!DB.IndexRepairAdvanceResult {
        const db = self.dbPtr();
        var result = DB.IndexRepairAdvanceResult{ .repair_id = repair_id };
        var effective_options = options;
        if (effective_options.capacity_source == null) effective_options.capacity_source = db.capacity_source;

        if (effective_options.source_replay_only and !db_mod.IndexRepairSchedulerDirectory.sourceRecoveryIntent(entry.intent)) {
            result.deferred = true;
            return result;
        }

        if (entry.intent.root_generation != db.core.root_generation or
            entry.intent.group_id != db.localRepairGroupId())
        {
            try self.recordAttemptFailure(alloc, repair_id, "repair_identity_mismatch", true);
            result.terminal = true;
            return result;
        }
        if (entry.intent.phase == .terminal) {
            if (db_mod.retryableIndexRepairTerminalPhase(
                entry.intent.last_error,
                entry.intent.trigger,
            )) |retry_phase| {
                try self.updateIntent(alloc, repair_id, .{
                    .phase = retry_phase,
                    .next_retry_at_ms = 0,
                    .replace_last_error = true,
                });
                entry.intent.phase = retry_phase;
                entry.intent.revision +|= 1;
            } else {
                result.terminal = true;
                return result;
            }
        }
        // Only startup discovery creates publication/projection-upgrade intents, so
        // this durable provenance is the authority to reclaim quarantine
        // ownership. Operator and generation-health intents may coexist with
        // retry/manual load failures but must not be reclassified as automatic
        // destructive recovery merely because they share an index name.
        if ((entry.intent.trigger == .incomplete_bulk_publish or entry.intent.trigger == .text_projection_upgrade) and
            entry.intent.phase != .rolling_back)
        {
            // Retryable terminal phases release their quarantine claim so an
            // operator may intervene. Reclaiming classifies action/config and
            // claims the current incarnation in one catalog-locked operation.
            // Policy or identity drift is terminal for this durable intent: a
            // fresh audit must authorize any different action.
            switch (db.core.index_manager.claimFailedIndexLoadRepair(
                entry.intent.index_name,
                .{ .current_policy = .{
                    .action = .rebuild_from_artifacts,
                    .config_hash = entry.intent.config_hash,
                } },
                repair_id,
            )) {
                .claimed, .already_claimed, .absent => {},
                .claimed_by_other => {
                    result.deferred = true;
                    return result;
                },
                .mismatch => |mismatch| {
                    try self.recordAttemptFailure(
                        alloc,
                        repair_id,
                        db_mod.quarantineClaimMismatchReason(mismatch.differences),
                        true,
                    );
                    result.terminal = true;
                    return result;
                },
            }
        }
        // A candidate which already failed coverage cannot manufacture an
        // artifact missing below its snapshot floor. Discard only that
        // inactive candidate and rebuild from a current pinned snapshot.
        if (entry.intent.candidate_relative_path != null and
            db_mod.retryableIndexRepairTerminalPhase(
                entry.intent.last_error,
                entry.intent.trigger,
            ) != null)
        {
            try self.discardInactiveCandidate(alloc, repair_id);
            result.attempted = true;
            result.deferred = true;
            return result;
        }
        if (entry.intent.automation == .paused) {
            result.deferred = true;
            return result;
        }
        const now_ms = self.nowMs();
        if (entry.intent.next_retry_at_ms > now_ms) {
            result.deferred = true;
            result.next_retry_at_ms = entry.intent.next_retry_at_ms;
            return result;
        }
        db_mod.checkArtifactRepairActivationOwner(options) catch |err| switch (err) {
            error.RepairOwnershipLost => {
                result.deferred = true;
                return result;
            },
            else => return err,
        };

        // Claim the durable state machine with the same epoch captured by the
        // owner-side activation fence. A later term/root owner may safely
        // reclaim the intent, but every subsequent transition and the final
        // activation in this attempt are CAS-fenced by the new epoch.
        if (options.owner_epoch != 0 and entry.intent.owner_epoch != options.owner_epoch) {
            try self.updateIntent(alloc, repair_id, .{ .owner_epoch = options.owner_epoch });
            entry.intent.owner_epoch = options.owner_epoch;
            entry.intent.revision +|= 1;
        }

        const cfg_ptr = db.core.index_manager.get(entry.intent.index_name) orelse {
            try self.recordAttemptFailure(alloc, repair_id, "index_configuration_missing", true);
            result.terminal = true;
            return result;
        };
        if (cfg_ptr.kind != entry.intent.kind or db_mod.types.indexConfigHash(cfg_ptr.*) != entry.intent.config_hash) {
            try self.recordAttemptFailure(alloc, repair_id, "index_configuration_changed", true);
            result.terminal = true;
            return result;
        }

        if (effective_options.source_replay_only and entry.intent.source_replay_state == .pending and db.enrichment.runtime == null) {
            // An unconstructed optional driver is not evidence that source
            // discovery is complete. Retain its durable cursor for a live owner.
            result.deferred = true;
            return result;
        }

        if (entry.intent.phase == .detected and entry.intent.source_replay_state == .pending) {
            // Managed DDL installs the generator for future writes under the
            // synchronous structural fence, but existing-row discovery belongs
            // to this durable bounded owner. Append one primary-store page,
            // then publish its cursor. A crash before the cursor repeats an
            // idempotent page; a crash after it can never skip source rows.
            const artifact_names = try db.core.index_manager.artifactProducerNamesForIndexAlloc(
                alloc,
                entry.intent.index_name,
            );
            defer {
                for (artifact_names) |name| alloc.free(name);
                alloc.free(artifact_names);
            }
            var page = try db.enrichment.appendGeneratedEnrichmentsFromStoredDocsPage(
                alloc,
                artifact_names,
                true,
                entry.intent.source_replay_resume_key,
                128,
                .{
                    .deadline_ns = if (effective_options.source_replay_only) db.indexes.graph.replacementWorkNow() +| 25 * std.time.ns_per_ms else null,
                    .min_records_before_deadline = if (effective_options.source_replay_only) 1 else 0,
                },
            );
            defer page.deinit(alloc);
            try self.updateIntent(alloc, repair_id, .{
                .source_replay_resume_key = page.next_resume_key,
                .replace_source_replay_resume_key = true,
                .source_replay_reprocessed = entry.intent.source_replay_reprocessed +| @as(u64, @intCast(page.documents_scanned)),
                .source_replay_state = if (page.complete) .complete else .pending,
                .target_sequence = @max(entry.intent.target_sequence, page.target_sequence),
                .failure_streak = 0,
                .next_retry_at_ms = 0,
            });
            result.attempted = true;
            result.documents_reprocessed = @intCast(page.documents_scanned);
            result.deferred = true;
            return result;
        }

        // Activation recovery is structural O(1) work and precedes corpus
        // validation or capacity admission. `rolling_back` is an explicit
        // durable decision: if restart finds the candidate healthy before the
        // pointer moved, reconciliation may still commit it; otherwise the
        // rollback owner must finish retiring it and must never resume it as a
        // convergence candidate.
        if (entry.intent.phase == .activating or
            entry.intent.phase == .validating or
            entry.intent.phase == .cleanup or
            entry.intent.phase == .rolling_back)
        {
            if (entry.intent.candidate_relative_path == null) {
                try self.recordAttemptFailure(alloc, repair_id, "activated_candidate_missing", true);
                result.terminal = true;
                return result;
            }
            const reconciliation = try self.reconcileActivated(alloc, repair_id);
            var certification_failure: ?DB.ActivatedIndexRepairCertificationFailure = null;
            switch (reconciliation) {
                .committed => {
                    result.attempted = true;
                    var repair = db_mod.types.ArtifactRepairResult{ .indexes_rebuilt = 1 };
                    try self.finalizeCommittedOutcome(alloc, entry.intent.index_name, 0, &repair);
                    DB.applyCommittedRepairOutcomeToAdvance(&result, repair);
                    result.repaired = repair.indexes_degraded_after == 0 and !repair.debt_remaining;
                    return result;
                },
                .not_active => {},
                .certification_failed => |failure| certification_failure = failure,
            }
            const rolled_back = try self.rollbackUnavailableActivated(
                alloc,
                repair_id,
                certification_failure,
            );
            switch (rolled_back) {
                .not_active => {
                    // `activating` is durable before pointer publication. If
                    // that publication never happened, retain the ready shadow
                    // and resume its bounded convergence path.
                    try self.updateIntent(alloc, repair_id, .{
                        .phase = .waiting_for_convergence,
                    });
                    entry.intent.phase = .waiting_for_convergence;
                    entry.intent.revision +|= 1;
                },
                .prior_generation_restored => {
                    // Yield after the rollback quantum. The restored generation
                    // is immediately available while the original durable
                    // repair resumes from preflight on the next owner turn.
                    result.attempted = true;
                    result.deferred = true;
                    return result;
                },
                .retry_deferred => {
                    var deferred = try self.loadEntryById(alloc, repair_id);
                    defer deferred.deinit(alloc);
                    result.attempted = true;
                    result.deferred = true;
                    result.next_retry_at_ms = deferred.intent.next_retry_at_ms;
                    return result;
                },
                .action_required => {
                    result.attempted = true;
                    result.terminal = true;
                    return result;
                },
                .rebuild_required => {},
            }
        }

        // Activation/rollback recovery is a committed-generation safety
        // boundary and therefore wins a concurrent cancellation. Everything
        // below may start new scan/build work and must honor the cancellation
        // bit installed by pause/cancel while this lease is visible.
        if (effective_options.cancelled()) {
            result.deferred = true;
            return result;
        }

        if (effective_options.source_replay_only) {
            if (try db.retireServiceableInitialBuild(alloc, repair_id, entry.intent)) {
                result.attempted = true;
                result.repaired = true;
            } else result.deferred = true;
            return result;
        }

        // A managed admission can become queryable before its exact physical
        // cardinality is known. Publish that identity proof before either
        // queryability exit below: the snapshot is ordered with foreground
        // artifact mutations by the apply lock, and later mutations contribute
        // to its signed delta. Deferring first used to park a healthy recreated
        // index forever because no subsequent path owned counter bootstrap.
        db.ensureDenseArtifactTargetCounterForRepair(
            alloc,
            cfg_ptr.*,
            repair_id,
            effective_options.cancel_check,
        ) catch |err| switch (err) {
            error.Canceled, error.StaleDenseArtifactCounterBootstrap => {
                result.attempted = true;
                result.deferred = true;
                return result;
            },
            error.RepairResourceUnavailable => {
                const attempt_count = entry.intent.attempt_count +| 1;
                const failure_streak = entry.intent.failure_streak +| 1;
                const next_retry_at_ms = now_ms +| retryDelayMs(repair_id, failure_streak);
                try self.updateIntent(alloc, repair_id, .{
                    .attempt_count = attempt_count,
                    .failure_streak = failure_streak,
                    .next_retry_at_ms = next_retry_at_ms,
                    .last_error = "RepairResourceUnavailable",
                    .replace_last_error = true,
                });
                result.attempted = true;
                result.deferred = true;
                result.next_retry_at_ms = next_retry_at_ms;
                return result;
            },
            else => return err,
        };

        // Check the exact index generation before the shared enrichment
        // producer fence. Other indexes can legitimately advance that global
        // runtime while this managed admission is already complete. Letting
        // unrelated producer work win here keeps a healthy durable intent at
        // the head of a one-slot repair queue forever.
        if (try db.retireServiceableInitialBuild(alloc, repair_id, entry.intent)) {
            result.attempted = true;
            result.repaired = true;
            return result;
        }

        // Progressive admission has already published a trustworthy canonical
        // generation. Keep serving and advancing that generation instead of
        // allocating a shadow candidate, which would otherwise make query
        // admission regress from queryable_partial back to fail-closed. The
        // durable intent remains pending and the stronger proof above retires
        // it once coverage and replay converge.
        if (try db.managedAdmissionGenerationIsQueryable(alloc, entry.intent)) {
            const observed_sequence = try db.managedIndexAppliedSequence(alloc, entry.intent.index_name);
            // Selection's slow-audit grant is authority for one exact durable
            // revision, not for the whole owned turn. Claim it with a durable
            // CAS immediately before using the queryable shortcut. Besides
            // closing the selection/reload and reload/use races, the revision
            // advance visibly consumes the process-local audit token so a
            // retry must receive a fresh scheduler grant.
            const consumed_audit_revision = if (audit_revision) |expected_revision|
                try self.consumeAuditGrant(alloc, repair_id, expected_revision)
            else
                null;
            if (consumed_audit_revision) |revision| entry.intent.revision = revision;
            // The audit rechecks the publication proof; its deadline is not
            // evidence of damaged artifacts. A certified partial generation
            // must continue through its canonical worker even when provider
            // failures or new writes keep source coverage incomplete.
            try db.enrichment.ensureManagedAdmissionCanonicalWorker(alloc, entry.intent);
            if (!try self.deferUntilTargetOrAudit(
                alloc,
                repair_id,
                entry.intent.revision,
                entry.intent.index_name,
                observed_sequence,
                entry.intent.target_sequence,
            )) {
                result.busy = true;
                return result;
            }
            result.deferred = true;
            return result;
        }

        if (entry.intent.phase == .detected and entry.intent.source_replay_state == .complete) {
            // The bounded source-replay phase advances the intent target after
            // every durable page, so its final value is the producer-completion
            // fence. Starting a shadow from artifacts
            // below that fence can snapshot a later artifact value and then
            // replay the earlier delete which produced it, permanently
            // removing deterministic chunk identities from the candidate.
            if (db.enrichment.runtime) |runtime| {
                const enrichment = runtime.stats();
                if (enrichment.enabled and enrichment.applied_sequence < entry.intent.target_sequence) {
                    // The source-discovery owner has durably queued all work;
                    // provider execution and derived publication now own the
                    // next progress edge. Park this exact intent instead of
                    // polling it on every repair scheduler turn. A derived
                    // publication wakes it only when the durable target has
                    // not yet been reached; after that, the bounded audit owns
                    // coverage completion, including terminal/skipped output
                    // which produces no index watermark edge.
                    const observed_sequence = try db.managedIndexAppliedSequence(alloc, entry.intent.index_name);
                    if (!try self.deferUntilTargetOrAudit(
                        alloc,
                        repair_id,
                        entry.intent.revision,
                        entry.intent.index_name,
                        observed_sequence,
                        entry.intent.target_sequence,
                    )) {
                        result.busy = true;
                        return result;
                    }
                    result.deferred = true;
                    return result;
                }
            }

            // The producer fence is sufficient for shadow construction: all
            // artifacts and their derived records are now durable, and the
            // candidate captures the current replay floor after that output.
            // Do not wait for the ordinary canonical worker to apply each
            // generated record individually. Large chunked admissions are
            // intentionally completed by the bounded bulk snapshot path.
        }

        // Automatic reconstruction is destructive only at pointer activation,
        // but creating an apparently valid empty candidate from missing source
        // metadata is still unsafe. Full per-artifact validation remains part
        // of the shadow build.
        self.validateSourcePreflight(alloc, cfg_ptr.*) catch |err| {
            const err_name = if (err == error.RepairSourceCoverageIncomplete)
                "dense_artifact_coverage_missing"
            else
                @errorName(err);
            try self.recordAttemptFailure(alloc, repair_id, err_name, true);
            result.attempted = true;
            result.terminal = true;
            return result;
        };

        const active_index_bytes = db.core.index_manager.activeIndexStorageBytes(entry.intent.index_name) catch 0;
        const source_estimate = db.artifactBackedDenseRepairEstimate(alloc, cfg_ptr.*) catch 0;
        var estimated_candidate_bytes = @max(
            entry.intent.estimated_candidate_bytes,
            @max(options.estimated_candidate_bytes, @max(active_index_bytes, source_estimate)),
        );
        if (estimated_candidate_bytes == 0) {
            estimated_candidate_bytes = 64 * 1024 * 1024;
        }
        const cleanup_and_replay_headroom = @max(estimated_candidate_bytes / 4, 256 * 1024 * 1024);
        const required_disk_bytes = estimated_candidate_bytes +| cleanup_and_replay_headroom;
        const planned_disk_bytes = @max(
            entry.intent.planned_disk_bytes,
            @max(options.planned_disk_bytes, required_disk_bytes),
        );
        if (entry.intent.estimated_candidate_bytes != estimated_candidate_bytes or
            entry.intent.planned_disk_bytes != planned_disk_bytes)
        {
            try self.updateIntent(alloc, repair_id, .{
                .estimated_candidate_bytes = estimated_candidate_bytes,
                .planned_disk_bytes = planned_disk_bytes,
            });
            entry.intent.estimated_candidate_bytes = estimated_candidate_bytes;
            entry.intent.planned_disk_bytes = planned_disk_bytes;
        }

        // Capacity is checked and claimed through the shared ResourceManager
        // so every DB on the node participates in one admission domain. Existing
        // candidate bytes are already reflected in filesystem availability;
        // reserve only the remaining growth and cleanup headroom on resume.
        // Reconciliation above is intentionally admission-free because it
        // releases space and must remain possible on an already-full volume.
        var candidate_bytes: u64 = 0;
        if (entry.intent.candidate_relative_path) |candidate| {
            try db_mod.index_repair_state.validateCandidateRelativePath(entry.intent.index_name, candidate);
            const separator = std.mem.indexOfScalar(u8, candidate, '/') orelse return error.InvalidRepairCandidatePath;
            if (entry.intent.kind == .algebraic) {
                const storage_namespace = try std.fmt.allocPrint(
                    alloc,
                    "{s}/{s}",
                    .{ candidate[0..separator], entry.intent.index_name },
                );
                defer alloc.free(storage_namespace);
                candidate_bytes = db.core.index_manager.algebraicStorageNamespaceBytes(storage_namespace) catch 0;
            } else {
                const candidate_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ db.core.index_manager.base_path, candidate[0..separator] });
                defer alloc.free(candidate_root);
                candidate_bytes = db.core.index_manager.generationStorageBytes(candidate_root) catch 0;
            }
        }
        const remaining_disk_claim = planned_disk_bytes -| @min(planned_disk_bytes, candidate_bytes);
        const manager = db.core.index_manager.resource_manager orelse return error.ResourceManagerUnavailable;
        const first_capacity = DB.repairCapacityObservation(effective_options) catch |err| {
            try self.recordAttemptFailure(alloc, repair_id, @errorName(err), false);
            result.deferred = true;
            result.disk_wait = true;
            return result;
        };
        var disk_reservation = manager.reserveCapacity(
            alloc,
            DB.repairCapacityDomain(effective_options),
            remaining_disk_claim,
            first_capacity,
            db_mod.monotonicTimeNs(),
        ) catch |err| switch (err) {
            error.CapacityUnavailable, error.CapacityObservationStale => {
                try self.recordAttemptFailure(alloc, repair_id, "disk_admission_unavailable", false);
                result.deferred = true;
                result.disk_wait = true;
                var updated = try self.loadEntryById(alloc, repair_id);
                defer updated.deinit(alloc);
                result.next_retry_at_ms = updated.intent.next_retry_at_ms;
                if (entry.intent.last_error == null or
                    !std.mem.eql(u8, entry.intent.last_error.?, "disk_admission_unavailable"))
                {
                    std.log.info("dense generation repair waiting for disk capacity index={s} available_bytes={?} requested_growth_bytes={} retry_at_ms={} reason={s}", .{
                        entry.intent.index_name,
                        first_capacity.available_bytes,
                        remaining_disk_claim,
                        result.next_retry_at_ms,
                        @errorName(err),
                    });
                }
                return result;
            },
            else => return err,
        };
        defer disk_reservation.release();
        var capacity_guard = DB.RepairCapacityGuard{
            .reservation = &disk_reservation,
            .db = db,
            .alloc = alloc,
            .repair_id = repair_id,
            .options = effective_options,
            .admitted_total_bytes = planned_disk_bytes,
            .headroom_bytes = cleanup_and_replay_headroom,
            .algebraic_index_name = if (entry.intent.kind == .algebraic) entry.intent.index_name else null,
        };

        const attempt_count = entry.intent.attempt_count +| 1;
        try self.updateIntent(alloc, repair_id, .{
            .attempt_count = attempt_count,
            .next_retry_at_ms = 0,
        });
        entry.intent.attempt_count = attempt_count;
        result.attempted = true;
        var run_options = effective_options;
        run_options.capacity_check = .{
            .ptr = &capacity_guard,
            .reconcile = DB.RepairCapacityGuard.reconcile,
            .revalidate = DB.RepairCapacityGuard.revalidate,
            .bind_candidate_root = DB.RepairCapacityGuard.bindCandidateRoot,
        };
        const artifact_kind = DB.artifactRepairKindForIndexKind(entry.intent.kind) orelse {
            try self.recordAttemptFailure(alloc, repair_id, "unsupported_index_kind", true);
            result.terminal = true;
            return result;
        };
        const repair = db.repairArtifactIssuesWithRequestOptionsInternal(alloc, .{
            .target = .index,
            .artifact_kind = artifact_kind,
            .index_name = entry.intent.index_name,
            .limit = 1,
            .force = true,
        }, run_options, .already_held) catch |err| {
            // Fault-injection hooks model process loss, not an ordinary
            // returned repair failure. Let the test harness observe the
            // injected stop without persisting retry/backoff state that a
            // crashed process could never have written.
            if (comptime builtin.is_test) {
                if (self.shadow_hook != null and std.mem.startsWith(u8, @errorName(err), "Test")) {
                    return err;
                }
            }
            if (err == error.Canceled and run_options.cancelled()) {
                result.deferred = true;
                return result;
            }
            var current_trigger = entry.intent.trigger;
            if (err == error.RepairSourceCoverageIncomplete) {
                var current = try self.loadEntryById(alloc, repair_id);
                defer current.deinit(alloc);
                current_trigger = current.intent.trigger;
            }
            if (err == error.RepairSourceCoverageIncomplete and
                entry.intent.kind == .dense_vector and
                db.enrichment.runtime != null and
                (current_trigger == .operator_generation_rebuild or
                    current_trigger == .storage_format_migration or
                    current_trigger == .artifact_coverage_mismatch or
                    current_trigger == .replay_artifact_unavailable))
            {
                // A shadow rebuild consumes stored embedding artifacts and
                // cannot repair one missing below its snapshot floor. Re-arm
                // the managed producer from primary documents first.
                const artifact_names = try db_mod.index_manager_mod.denseConfigArtifactNamesAlloc(alloc, cfg_ptr.*);
                defer {
                    for (artifact_names) |name| alloc.free(name);
                    alloc.free(artifact_names);
                }
                var queued_refs: usize = 0;
                for (artifact_names) |artifact_name| {
                    queued_refs +|= db.enrichment.reprocessGeneratedEnrichmentFromStoredDocs(alloc, artifact_name) catch |reprocess_err| blk: {
                        std.log.warn(
                            "dense repair source recovery deferred index={s} artifact={s} err={s}",
                            .{ entry.intent.index_name, artifact_name, @errorName(reprocess_err) },
                        );
                        break :blk 0;
                    };
                }
                if (queued_refs != 0) {
                    const recovery_target = db.core.nextDerivedSequence();
                    const runtime = db.enrichment.runtime.?;
                    if (runtime.isStarted()) {
                        runtime.notifySequence(recovery_target);
                        runtime.waitForApplied(recovery_target) catch |drain_err| {
                            std.log.warn(
                                "dense repair source worker recovery deferred index={s} err={s}",
                                .{ entry.intent.index_name, @errorName(drain_err) },
                            );
                        };
                    } else {
                        // Bounded startup/repair owners must not start a
                        // long-lived worker which can outlive and pin them.
                        runtime.catchUpUntil(recovery_target) catch |drain_err| {
                            std.log.warn(
                                "dense repair source foreground recovery deferred index={s} err={s}",
                                .{ entry.intent.index_name, @errorName(drain_err) },
                            );
                        };
                    }
                }
            }
            const capacity_wait = err == error.CapacityUnavailable or err == error.CapacityObservationStale;
            // Use the same classification for live failures and persisted
            // terminal states from older binaries. Catalog admission retains
            // its existing producer outcomes; it does not re-arm failed work.
            const retryable_replacement_coverage =
                db_mod.retryableIndexRepairTerminalPhase(@errorName(err), current_trigger) != null;
            const terminal_failure =
                !retryable_replacement_coverage and failureIsTerminal(err);
            try self.recordAttemptFailure(
                alloc,
                repair_id,
                if (capacity_wait) "disk_admission_unavailable" else @errorName(err),
                terminal_failure,
            );
            result.disk_wait = capacity_wait;
            if (terminal_failure) {
                result.terminal = true;
            } else {
                result.deferred = true;
                var updated = try self.loadEntryById(alloc, repair_id);
                defer updated.deinit(alloc);
                result.next_retry_at_ms = updated.intent.next_retry_at_ms;
            }
            return result;
        };
        result.documents_reprocessed = repair.reprocessed;
        DB.applyCommittedRepairOutcomeToAdvance(&result, repair);

        // A committed replacement wins a cancellation observed after the
        // final pointer/checkpoint publication. Artifact debt discovered
        // during the rebuild remains in its own durable queue and must not
        // resurrect the already-completed generation intent.
        if (repair.indexes_rebuilt != 0 or repair.repaired != 0) {
            result.repaired = repair.indexes_degraded_after == 0 and !repair.debt_remaining;
            return result;
        }

        // Cooperative owner shutdown is not a failed repair attempt. Durable
        // snapshot/catch-up boundaries have already left the candidate in a
        // resumable phase, so preserve its retry schedule and let the next
        // owner continue immediately.
        if (run_options.cancelled()) {
            result.deferred = true;
            return result;
        }
        if (repair.in_progress != 0) {
            try self.updateIntent(alloc, repair_id, .{
                .failure_streak = 0,
                .next_retry_at_ms = 0,
                .replace_last_error = true,
            });
            result.busy = true;
            return result;
        }
        try self.recordAttemptFailure(alloc, repair_id, "repair_attempt_incomplete", false);
        result.deferred = true;
        var updated = try self.loadEntryById(alloc, repair_id);
        defer updated.deinit(alloc);
        result.next_retry_at_ms = updated.intent.next_retry_at_ms;
        return result;
    }

    pub fn inspectionWindow(
        candidate_count: usize,
        execution_limit: usize,
        cursor: u64,
    ) DB.IndexRepairInspectionWindow {
        if (candidate_count == 0 or execution_limit == 0) return .{ .start = 0, .budget = 0 };
        return .{
            .start = @intCast(cursor % candidate_count),
            .budget = @min(
                candidate_count,
                std.math.mul(
                    usize,
                    execution_limit,
                    DB.index_repair_inspections_per_execution_slot,
                ) catch std.math.maxInt(usize),
            ),
        };
    }

    pub fn selectSchedulerQuantum(
        self: *RepairIndexes,
        alloc: Allocator,
        execution_limit: usize,
    ) !DB.IndexRepairSchedulerSelection {
        return self.selectSchedulerQuantumForIndex(alloc, execution_limit, null);
    }

    pub fn selectSchedulerQuantumForIndex(self: *RepairIndexes, alloc: Allocator, execution_limit: usize, target_index_name: ?[]const u8) !DB.IndexRepairSchedulerSelection {
        return self.selectSchedulerQuantumScoped(alloc, execution_limit, target_index_name, false);
    }

    pub fn selectSchedulerQuantumScoped(self: *RepairIndexes, alloc: Allocator, execution_limit: usize, target_index_name: ?[]const u8, source_replay_only: bool) !DB.IndexRepairSchedulerSelection {
        const db = self.dbPtr();
        try self.ensureSchedulerDirectory(alloc);
        var selection: DB.IndexRepairSchedulerSelection = .{};
        errdefer selection.deinit(alloc);
        db_mod.lockAtomic(&db.async_context.index_repair_scheduler_mutex);
        defer db.async_context.index_repair_scheduler_mutex.unlock();
        const directory = &db.async_context.index_repair_scheduler;
        if (source_replay_only and directory.sourceRecoveryCountForIndex(target_index_name) == 0) return selection;
        const summary = directory.summaryForIndex(target_index_name);
        selection.terminal = if (source_replay_only) 0 else summary.terminal;
        selection.deferred = if (source_replay_only) 0 else summary.paused;
        selection.remaining = if (source_replay_only) directory.sourceRecoveryCountForIndex(target_index_name) else summary.runnable + summary.paused + summary.terminal;
        selection.next_retry_at_ms = if (source_replay_only) 0 else summary.earliest_retry_at_ms;
        const target_index = if (target_index_name) |name| directory.by_name.get(name) else null;
        if (target_index_name != null and target_index == null) return selection;
        const inspection = if (target_index) |index| DB.IndexRepairInspectionWindow{ .start = index, .budget = @min(execution_limit, 1) } else inspectionWindow(
            directory.records.items.len,
            execution_limit,
            @intCast(directory.cursor),
        );
        try selection.repairs.ensureTotalCapacity(alloc, inspection.budget);
        const now_ms = self.nowMs();
        var next_source_cursor: ?usize = null;
        while (selection.inspected < inspection.budget) : (selection.inspected += 1) {
            const record_index = (inspection.start + selection.inspected) % directory.records.items.len;
            const record = directory.records.items[record_index];
            if (target_index_name == null) directory.cursor = (record_index + 1) % directory.records.items.len;
            if (record.class != .runnable or (source_replay_only and !record.source_recovery)) continue;
            if (record.next_retry_at_ms > now_ms) {
                selection.deferred += 1;
                if (selection.next_retry_at_ms == 0 or record.next_retry_at_ms < selection.next_retry_at_ms) {
                    selection.next_retry_at_ms = record.next_retry_at_ms;
                }
                continue;
            }
            if (record.progress_wait_until_sequence != null) {
                // The slow audit deadline fired without a matching progress
                // callback. Clear the process-local wait before execution so
                // the new observation can arm a fresh token and deadline.
                const mutable = &directory.records.items[record_index];
                mutable.progress_wait_until_sequence = null;
                directory.progress_waiters -= 1;
                mutable.next_retry_at_ms = mutable.durable_next_retry_at_ms;
                directory.rescheduleRunnable(mutable.repair_id);
                if (directory.progress_waiters == 0) {
                    db.async_context.index_repair_progress_wait_pending.store(false, .release);
                }
            }
            selection.repairs.appendAssumeCapacity(.{
                .repair_id = record.repair_id,
                .revision = record.revision,
                .audit_due = record.audit_wait_armed,
            });
            if (source_replay_only and target_index_name == null and next_source_cursor == null)
                next_source_cursor = (record_index + 1) % directory.records.items.len;
        }
        // Inspection can wrap over a small directory while execution consumes
        // only its first material slot. Advance after that selected source,
        // not after the whole inspected window, so a long source migration
        // cannot repeatedly win ahead of another index in the same table.
        if (next_source_cursor) |next| directory.cursor = next;
        return selection;
    }

    pub fn rebuildWithShadowReplacement(
        self: *RepairIndexes,
        alloc: Allocator,
        cfg: db_mod.types.IndexConfig,
        options: db_mod.types.ArtifactRepairRunOptions,
        durable_repair_id: ?u128,
    ) !DB.ShadowIndexReplacementResult {
        var yield_progress = DB.ShadowIndexReplacementResult{};
        return self.rebuildWithShadowReplacementOwned(alloc, cfg, options, durable_repair_id, &yield_progress) catch |err| switch (err) {
            error.RepairSourceChanged => blk: {
                // A source race requires a fresh snapshot, not failure backoff
                // or repeated catch-up of an irrecoverably stale candidate.
                // The owned builder has released all physical handles here.
                const repair_id = durable_repair_id orelse return err;
                try self.discardInactiveCandidate(alloc, repair_id);
                yield_progress.yielded = true;
                break :blk yield_progress;
            },
            // A bounded activation pause yielding to readers/writers is not
            // a storage failure. Keep the durable candidate runnable instead
            // of imposing failure backoff on ordinary scheduler contention.
            // Snapshot work already completed in this turn still belongs in
            // its result: a resumed candidate will not scan or count it again.
            error.RepairActivationBudgetExhausted => blk: {
                yield_progress.yielded = true;
                break :blk yield_progress;
            },
            else => return err,
        };
    }

    pub fn rebuildWithShadowReplacementOwned(
        self: *RepairIndexes,
        alloc: Allocator,
        cfg: db_mod.types.IndexConfig,
        options: db_mod.types.ArtifactRepairRunOptions,
        durable_repair_id: ?u128,
        yield_progress: *DB.ShadowIndexReplacementResult,
    ) !DB.ShadowIndexReplacementResult {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("rebuildIndexWithShadowReplacementOwned", .{ db, alloc, cfg, options, durable_repair_id, yield_progress });
        try db_mod.checkArtifactRepairCancelled(options);
        const working_set_plan = if (db.core.index_manager.resource_manager) |manager|
            try DB.repairWorkingSetPlan(alloc, manager, cfg)
        else
            DB.RepairWorkingSetPlan{ .reservation_bytes = 0 };
        var repair_admission: ?db_mod.resource_manager_mod.Reservation = if (db.core.index_manager.resource_manager) |manager|
            manager.reserve(.dense_repair_working_set, working_set_plan.reservation_bytes) catch
                return error.RepairResourceUnavailable
        else
            null;
        defer if (repair_admission) |*reservation| reservation.release();

        // A forced dense rebuild is initially fail-closed. Only the admitted
        // background worker may prove the current generation safe enough to
        // remain online during shadow construction. Automatic anomaly
        // classifications stay fail-closed and reuse the same reconstruction.
        if (durable_repair_id) |repair_id| {
            var durable_entry = try self.loadEntryById(alloc, repair_id);
            defer durable_entry.deinit(alloc);
            if (durable_entry.intent.trigger == .operator_generation_validation) {
                switch (try db.validateOperatorDenseGenerationForOnlineRebuild(alloc, cfg, options)) {
                    .online_safe => try self.updateIntent(alloc, repair_id, .{
                        .trigger = .operator_generation_rebuild,
                        .replace_last_error = true,
                    }),
                    .replay_pending => return error.OperatorGenerationValidationPending,
                    .fail_closed => |classification| try self.updateIntent(alloc, repair_id, .{
                        .trigger = classification.trigger,
                        .last_error = classification.last_error,
                        .replace_last_error = true,
                    }),
                }
            }
        }
        var resume_candidate = false;
        var resume_building = false;
        var resume_requires_ready_manifest = false;
        var persisted_build_floor_sequence: u64 = 0;
        var build_source_guard: ?@import("../artifact_source_gap.zig").Guard = null;
        var persisted_build_reprocessed: u64 = 0;
        var build_resume_key: ?[]u8 = null;
        defer if (build_resume_key) |key| alloc.free(key);
        const generation_id = durable_repair_id orelse try db_mod.index_repair_state.newRepairId(alloc);
        var shadow_base: []u8 = undefined;
        if (durable_repair_id) |repair_id| {
            var entry = try self.loadEntryById(alloc, repair_id);
            defer entry.deinit(alloc);
            const resumable_phase = switch (entry.intent.phase) {
                .building => entry.intent.build_resume_key != null and
                    db_mod.indexRepairSupportsSnapshotCursor(cfg.kind),
                .catching_up, .ready, .waiting_for_convergence => true,
                else => false,
            };
            if (resumable_phase and entry.intent.candidate_relative_path != null) {
                const valid_source = blk: {
                    const guard = entry.intent.build_source_guard orelse break :blk false;
                    var read = try db.core.store.beginReadTxn();
                    defer read.abort();
                    guard.requireCurrent(&read) catch |err| switch (err) {
                        error.EnrichmentSourceChanged => break :blk false,
                        else => return err,
                    };
                    break :blk true;
                };
                if (!valid_source) {
                    return error.RepairSourceChanged;
                }
                build_source_guard = entry.intent.build_source_guard;
                const candidate = entry.intent.candidate_relative_path.?;
                try db_mod.index_repair_state.validateCandidateRelativePath(cfg.name, candidate);
                const separator = std.mem.indexOfScalar(u8, candidate, '/') orelse return error.InvalidRepairCandidatePath;
                shadow_base = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ db.core.index_manager.base_path, candidate[0..separator] });
                resume_candidate = true;
                resume_building = entry.intent.phase == .building;
                resume_requires_ready_manifest = entry.intent.phase == .ready or entry.intent.phase == .waiting_for_convergence;
                persisted_build_floor_sequence = entry.intent.build_floor_sequence;
                persisted_build_reprocessed = entry.intent.build_reprocessed;
                build_resume_key = if (entry.intent.build_resume_key) |key| try alloc.dupe(u8, key) else null;
            } else {
                try self.discardInactiveCandidate(alloc, repair_id);
                shadow_base = try db_mod.createUniqueRepairShadowBaseWithStorage(alloc, db.core.index_manager.base_path, db.core.index_manager.generation_storage);
            }
        } else {
            shadow_base = try db_mod.createUniqueRepairShadowBaseWithStorage(alloc, db.core.index_manager.base_path, db.core.index_manager.generation_storage);
        }
        if (!resume_candidate) try db.core.index_manager.markRepairShadow(alloc, shadow_base);
        var shadow_installed = false;
        var candidate_reopenable = resume_candidate;
        defer {
            var io_impl = db_mod.threadedIo();
            defer io_impl.deinit();
            if (!shadow_installed and !candidate_reopenable) {
                if (cfg.kind == .algebraic) {
                    // Leave the index directory as a durable namespace marker.
                    // Once the in-progress marker is cleared, paged orphan
                    // cleanup can resume after failure or process restart.
                    db.core.index_manager.clearRepairShadow(alloc, shadow_base) catch {};
                    self.scheduleInactiveShadowCleanup();
                } else {
                    db.core.index_manager.clearRepairShadow(alloc, shadow_base) catch {};
                    if (db.core.index_manager.generation_storage) |storage|
                        storage.deleteTree(shadow_base) catch {}
                    else
                        std.Io.Dir.cwd().deleteTree(io_impl.io(), shadow_base) catch {};
                }
            }
            alloc.free(shadow_base);
        }
        const shadow_indexes_path = try std.fmt.allocPrint(alloc, "{s}/indexes", .{shadow_base});
        defer alloc.free(shadow_indexes_path);
        if (!resume_candidate) try db.core.index_manager.createGenerationDirectory(shadow_indexes_path);

        const shadow_index_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ shadow_indexes_path, cfg.name });
        defer alloc.free(shadow_index_path);
        // Generation manifests belong to the repair owner. Storage backends
        // may keep index data in a separate namespace and need not create
        // this filesystem directory as a side effect of opening an index.
        try db.core.index_manager.createGenerationDirectory(shadow_index_path);
        const shadow_checkpoint_path = try std.fmt.allocPrint(alloc, "{s}/applied-sequences", .{shadow_base});
        defer alloc.free(shadow_checkpoint_path);
        if (options.capacity_check) |check| try check.bindCandidateRoot(shadow_base);
        if (resume_requires_ready_manifest) {
            _ = db_mod.index_generation_manifest.validateReadyWithStorage(
                db.core.index_manager.generation_storage,
                alloc,
                shadow_index_path,
                generation_id,
                cfg.name,
                db_mod.types.indexConfigHash(cfg),
            ) catch {
                candidate_reopenable = false;
                if (durable_repair_id) |repair_id| self.discardInactiveCandidate(alloc, repair_id) catch {};
                return error.RepairCandidateResumeInvalid;
            };
        }
        if (!resume_candidate) {
            if (durable_repair_id) |repair_id| {
                const candidate_relative_path = try std.fmt.allocPrint(
                    alloc,
                    "{s}/indexes/{s}",
                    .{ std.fs.path.basename(shadow_base), cfg.name },
                );
                defer alloc.free(candidate_relative_path);
                try db_mod.index_repair_state.validateCandidateRelativePath(cfg.name, candidate_relative_path);
                try self.updateIntent(alloc, repair_id, .{
                    .phase = .building,
                    .candidate_relative_path = candidate_relative_path,
                    .replace_candidate_path = true,
                });
            }
        }

        var shadow_backends = db.index_backends;
        // The parent catalog capability floor authorizes creation of this
        // private candidate. It must become independently authoritative before
        // the checksummed manifest and active pointer can publish it.
        shadow_backends.dense_native_candidate_build_authorized = if (db.index_backends.dense_native_migration_policy_source) |source|
            source.authorityPermitted()
        else
            true;
        // Detached index entries are adopted by the serving manager. Their
        // mutexes, configs and runtime objects must share its allocator, even
        // when this repair was requested through a short-lived HTTP arena.
        var shadow_manager = try db_mod.index_manager_mod.IndexManager.initWithOptions(
            db.core.index_manager.alloc,
            shadow_base,
            shadow_backends,
        );
        // Repair candidates share the table's immutable source owner too.
        // Otherwise their native base build silently recreates payload copies.
        shadow_manager.source_payload_store = db.core.index_manager.source_payload_store;
        shadow_manager.setIo(db.backend_runtime.io());
        shadow_manager.setAppliedSequenceCheckpointPath(shadow_checkpoint_path);
        shadow_manager.registerReplacementIndex(db.core.store, cfg) catch |err| {
            shadow_manager.transferLsmOwnerCloneStatsTo(db.core.index_manager) catch |transfer_err| {
                std.log.warn("failed to preserve unopened shadow LSM clone attribution index={s} err={s}", .{
                    cfg.name,
                    @errorName(transfer_err),
                });
            };
            shadow_manager.deinit();
            if (resume_candidate) {
                candidate_reopenable = false;
                if (durable_repair_id) |repair_id| self.discardInactiveCandidate(alloc, repair_id) catch {};
                return error.RepairCandidateResumeInvalid;
            }
            return err;
        };
        var shadow_manager_open = true;
        defer if (shadow_manager_open) {
            shadow_manager.transferLsmOwnerCloneStatsTo(db.core.index_manager) catch |err| {
                std.log.warn("failed to preserve shadow LSM clone attribution index={s} err={s}", .{
                    cfg.name,
                    @errorName(err),
                });
            };
            shadow_manager.deinit();
        };

        // A scan cursor is useful only when there is a durable intent to own
        // it. Non-durable/internal one-shot rebuild callers retain the existing
        // complete bulk behavior even if they forward a scheduler policy by
        // mistake; otherwise they could repeatedly discard partial shadows.
        var effective_options = options;
        if (durable_repair_id == null) effective_options.yield_check = null;
        const cooperative_snapshot_build =
            db_mod.indexRepairSupportsSnapshotCursor(cfg.kind) and
            effective_options.yield_check != null;
        var repair_issue_counter: db_mod.AtomicU64 = .init(0);
        defer yield_progress.unresolved_artifacts = repair_issue_counter.load(.monotonic);
        var shadow_ctx = db_mod.AsyncContext{
            .alloc = alloc,
            .io = db.backend_runtime.io(),
            .store = db.core.store,
            .relational_base_rows = db_mod.relationalColumns(db) != null,
            .applied_sequence_checkpoint_path = shadow_checkpoint_path,
            .index_manager = &shadow_manager,
            .apply_mutex = db.async_context.apply_mutex,
            .dense_bulk_session_scope = .external,
            .resolution_runtime = db.enrichment.resolution.runtime,
            .promotion_runtime = db.enrichment.promotion.runtime,
            .repair_options = effective_options,
            .repair_issue_counter = &repair_issue_counter,
        };
        defer shadow_ctx.deinit(alloc);

        var build_floor_sequence: u64 = if (resume_building)
            persisted_build_floor_sequence
        else if (resume_candidate)
            try db_mod.apply_state.loadAppliedSequenceWithCheckpointWithStorage(
                shadow_manager.generation_storage,
                alloc,
                shadow_manager.checkpointIo(),
                db.core.store,
                shadow_checkpoint_path,
                cfg.name,
            )
        else
            0;
        var expected_snapshot_coverage: ?u64 = null;
        const completed_snapshot_build = !resume_candidate or resume_building;
        const rebuilt: u64 = if (resume_candidate and !resume_building) 0 else switch (cfg.kind) {
            .dense_vector, .sparse_vector, .graph, .full_text, .algebraic => rebuilt_blk: {
                try db_mod.checkArtifactRepairCancelled(options);
                var pinned_snapshot: ?DB.PinnedIndexRepairSnapshot = if (!resume_building) if (durable_repair_id) |repair_id|
                    try self.beginPinnedSnapshot(alloc, repair_id)
                else
                    null else null;
                defer if (pinned_snapshot) |*pinned| pinned.deinit();
                var ordinary_snapshot: ?db_mod.docstore_mod.DocStore.Txn = if (pinned_snapshot == null)
                    try db.core.store.beginReadTxn()
                else
                    null;
                defer if (ordinary_snapshot) |*ordinary| ordinary.abort();
                const snapshot_txn = if (pinned_snapshot) |*pinned|
                    &pinned.txn
                else
                    &ordinary_snapshot.?;
                if (!resume_building) build_source_guard = try @import("../artifact_source_gap.zig").Guard.capture(snapshot_txn);
                build_floor_sequence = if (resume_building)
                    persisted_build_floor_sequence
                else if (pinned_snapshot) |pinned|
                    pinned.build_floor_sequence
                else
                    try db.core.store.lastReplaySequenceFromTxn(snapshot_txn, 0);
                shadow_ctx.repair_sequence = build_floor_sequence;
                shadow_ctx.snapshot_read_txn = snapshot_txn;
                defer shadow_ctx.snapshot_read_txn = null;
                const count: u64 = switch (cfg.kind) {
                    .dense_vector => count_blk: {
                        var slice = try db_mod.rebuildDenseIndexForTargetCoverageSliceContext(
                            &shadow_ctx,
                            cfg.name,
                            working_set_plan.dense_rebuild_batch_items,
                            build_resume_key,
                        );
                        defer slice.deinit(alloc);
                        const count: u64 = persisted_build_reprocessed +| @as(u64, @intCast(slice.rebuilt));
                        if (!slice.complete()) {
                            if (options.capacity_check) |check| {
                                try check.current(try shadow_manager.generationStorageBytes(shadow_base));
                            }
                            if (durable_repair_id) |repair_id| {
                                try self.updateIntent(alloc, repair_id, .{
                                    .phase = .building,
                                    .build_resume_key = slice.resume_key.?,
                                    .replace_build_resume_key = true,
                                    .build_reprocessed = count,
                                    .failure_streak = 0,
                                    .next_retry_at_ms = 0,
                                    .replace_last_error = true,
                                });
                            }
                            candidate_reopenable = durable_repair_id != null;
                            return .{ .reprocessed = @intCast(slice.rebuilt), .yielded = true };
                        }
                        const completed_dense = shadow_manager.denseIndex(cfg.name) orelse return error.IndexNotFound;
                        if (completed_dense.managed_direct_field) {
                            // The complete pinned primary scan counts raw and
                            // stripped/artifact-backed values exactly once.
                            // Verify before catch-up, which can change count.
                            expected_snapshot_coverage = count;
                            if (!db_mod.denseCoverageMatchesTarget(completed_dense.index.stats().active_count, count)) return error.RepairSourceCoverageIncomplete;
                        } else if (cooperative_snapshot_build) {
                            // A count observed between slices is not a stable
                            // coverage assertion. Require the durable,
                            // generation-scoped outcome tuple when available;
                            // validate its exact fenced value after final
                            // replay below.
                            const dense_entry = shadow_manager.denseIndex(cfg.name) orelse return error.IndexNotFound;
                            expected_snapshot_coverage = if (db_mod.denseIndexIsArtifactBacked(dense_entry))
                                try db_mod.denseTargetCountForIndexContext(&shadow_ctx, cfg.name)
                            else
                                null;
                        } else {
                            expected_snapshot_coverage = try db_mod.denseTargetCountForIndexContext(&shadow_ctx, cfg.name);
                        }
                        break :count_blk count;
                    },
                    .sparse_vector => count_blk: {
                        var slice = try db_mod.rebuildSparseIndexFromStoredEmbeddingArtifactsSliceContext(&shadow_ctx, cfg.name, if (builtin.is_test) db_mod.test_dense_repair_rebuild_batch_size orelse 256 else 256, build_resume_key);
                        defer slice.deinit(alloc);
                        const count = persisted_build_reprocessed +| @as(u64, @intCast(slice.rebuilt));
                        if (!slice.complete()) {
                            if (durable_repair_id) |repair_id| try self.updateIntent(alloc, repair_id, .{
                                .phase = .building,
                                .build_resume_key = slice.resume_key.?,
                                .replace_build_resume_key = true,
                                .build_reprocessed = count,
                                .failure_streak = 0,
                                .next_retry_at_ms = 0,
                                .replace_last_error = true,
                            });
                            candidate_reopenable = durable_repair_id != null;
                            return .{ .reprocessed = @intCast(slice.rebuilt), .yielded = true };
                        }
                        const built = shadow_manager.sparseIndex(cfg.name) orelse return error.IndexNotFound;
                        if (built.managed_direct_field and built.index.stats().doc_count != count) return error.RepairSourceCoverageIncomplete;
                        break :count_blk count;
                    },
                    .graph => @intCast(try db_mod.applySplitGraphArtifactsForIndexStreamingContext(
                        &shadow_ctx,
                        cfg.name,
                        db_mod.graph_repair_rebuild_batch_size,
                    )),
                    .full_text => count_blk: {
                        var slice = try shadow_manager.rebuildFullTextIndexFromReadTxnSlice(db.core.store, snapshot_txn, cfg.name, build_resume_key, effective_options);
                        defer slice.deinit(shadow_manager.alloc);
                        if (slice.resume_key) |cursor| {
                            if (durable_repair_id) |repair_id| try self.updateIntent(alloc, repair_id, .{
                                .phase = .building,
                                .build_resume_key = cursor,
                                .replace_build_resume_key = true,
                                .build_reprocessed = slice.rebuilt,
                                .failure_streak = 0,
                                .next_retry_at_ms = 0,
                                .replace_last_error = true,
                            });
                            candidate_reopenable = durable_repair_id != null;
                            return .{ .reprocessed = @intCast(slice.rebuilt -| persisted_build_reprocessed), .yielded = true };
                        }
                        break :count_blk slice.rebuilt;
                    },
                    .algebraic => count_blk: {
                        var slice = try db_mod.rebuildAlgebraicIndexFromSnapshotSliceContext(
                            &shadow_ctx,
                            cfg.name,
                            if (comptime builtin.is_test)
                                db_mod.test_algebraic_repair_rebuild_batch_size orelse 256
                            else
                                256,
                            build_resume_key,
                        );
                        defer slice.deinit(alloc);
                        const count = persisted_build_reprocessed +| @as(u64, @intCast(slice.rebuilt));
                        if (!slice.complete()) {
                            if (durable_repair_id) |repair_id| {
                                try self.updateIntent(alloc, repair_id, .{
                                    .phase = .building,
                                    .build_resume_key = slice.resume_key.?,
                                    .replace_build_resume_key = true,
                                    .build_reprocessed = count,
                                    .failure_streak = 0,
                                    .next_retry_at_ms = 0,
                                    .replace_last_error = true,
                                });
                            }
                            candidate_reopenable = durable_repair_id != null;
                            return .{ .reprocessed = @intCast(slice.rebuilt), .yielded = true };
                        }
                        break :count_blk count;
                    },
                };
                break :rebuilt_blk count;
            },
        };
        // Durable intent progress is cumulative, while the public/run result
        // reports work performed by this scheduler turn. Keeping those two
        // meanings separate prevents the final resumed slice from recounting
        // every vector processed by earlier turns.
        const reprocessed_this_pass = if (resume_building and
            db_mod.indexRepairSupportsSnapshotCursor(cfg.kind))
            rebuilt -| persisted_build_reprocessed
        else
            rebuilt;
        yield_progress.reprocessed = reprocessed_this_pass;

        const index_ref = db_mod.index_manager_mod.ManagedIndexRef{
            .name = cfg.name,
            .kind = cfg.kind,
        };
        if (cfg.kind == .dense_vector) {
            const built_entry = shadow_manager.denseIndex(cfg.name) orelse return error.IndexNotFound;
            if (completed_snapshot_build and db_mod.denseIndexIsArtifactBacked(built_entry) and expected_snapshot_coverage == null) {
                return error.RepairSourceCoverageIncomplete;
            }
            if (!cooperative_snapshot_build) {
                if (expected_snapshot_coverage) |expected| {
                    if (!db_mod.denseCoverageMatchesTarget(built_entry.index.stats().active_count, expected)) {
                        return error.RepairSourceCoverageIncomplete;
                    }
                }
            }
        }
        if (completed_snapshot_build) {
            try db_mod.checkArtifactRepairCancelled(options);
            try self.saveShadowReplacementAppliedSequence(alloc, &shadow_manager, shadow_checkpoint_path, index_ref, build_floor_sequence);
            if (options.capacity_check) |check| {
                try check.current(try db_mod.repairCandidateStorageBytes(alloc, cfg, &shadow_manager, shadow_base));
            }
            if (durable_repair_id) |repair_id| try self.updateIntent(alloc, repair_id, .{
                .phase = .catching_up,
                .replace_build_resume_key = true,
                .build_reprocessed = rebuilt,
                .candidate_applied_sequence = build_floor_sequence,
                .failure_streak = 0,
                .next_retry_at_ms = 0,
                .replace_last_error = true,
            });
            // From this transition onward the candidate and its applied
            // checkpoint are independently reopenable. Cancellation, owner
            // loss, or restart preserves it for bounded catch-up resumption.
            candidate_reopenable = durable_repair_id != null;
            if (self.shadow_hook) |hook| {
                try hook.after_snapshot_build(hook.ptr, db, cfg.name, build_floor_sequence);
            }
        }
        var observed_ns_per_sequence: u64 = 0;
        var catch_up_start_sequence = build_floor_sequence;
        var catch_up_start_ns = db_mod.monotonicTimeNs();
        const initial_catch_up_target = db.core.nextDerivedSequence();
        const initial_catch_up = try self.catchUpShadowReplacementUntil(
            alloc,
            &shadow_manager,
            shadow_checkpoint_path,
            index_ref,
            initial_catch_up_target,
            options,
            null,
            0,
        );
        var converged_sequence = initial_catch_up.applied_sequence;
        if (options.capacity_check) |check| {
            try check.current(try db_mod.repairCandidateStorageBytes(alloc, cfg, &shadow_manager, shadow_base));
        }
        observed_ns_per_sequence = DB.observeRepairCatchUpCost(
            observed_ns_per_sequence,
            catch_up_start_sequence,
            converged_sequence,
            db_mod.elapsedSince(catch_up_start_ns),
        );
        if (initial_catch_up.yielded) {
            if (durable_repair_id) |repair_id| try self.updateIntent(alloc, repair_id, .{
                .phase = .catching_up,
                .candidate_applied_sequence = converged_sequence,
                .target_sequence = initial_catch_up_target,
                .failure_streak = 0,
                .next_retry_at_ms = 0,
                .replace_last_error = true,
            });
            candidate_reopenable = durable_repair_id != null;
            return .{ .reprocessed = reprocessed_this_pass, .yielded = true };
        }
        // A generation manifest is a promise that the candidate is
        // independently recoverable. Snapshot construction can legitimately
        // bypass source captures, so explicitly bootstrap and validate the
        // native HBC generation before making that promise. The first native
        // checkpoint is already a full segment; later convergence is flattened
        // once, after the candidate is close enough to activate.
        try DB.finalizeShadowNativePostingGeneration(
            &shadow_manager,
            index_ref,
            converged_sequence,
            true,
            false,
        );
        try db_mod.index_generation_manifest.writeReadyForPhysicalFormatWithStorage(
            db.core.index_manager.generation_storage,
            alloc,
            shadow_index_path,
            generation_id,
            cfg.name,
            db_mod.types.indexConfigHash(cfg),
            converged_sequence,
            try shadowPhysicalFormat(&shadow_manager, index_ref),
        );
        if (durable_repair_id) |repair_id| try self.updateIntent(alloc, repair_id, .{
            .phase = .ready,
            .candidate_applied_sequence = converged_sequence,
            .target_sequence = db.core.nextDerivedSequence(),
        });

        if (durable_repair_id) |repair_id| try self.updateIntent(alloc, repair_id, .{
            .phase = .waiting_for_convergence,
            .candidate_applied_sequence = converged_sequence,
            .target_sequence = db.core.nextDerivedSequence(),
        });
        const max_activation_gap_sequences = @max(@as(u64, 1), options.max_activation_gap_sequences);
        const max_convergence_rounds: usize = @max(@as(usize, 1), options.max_convergence_rounds);
        const max_activation_pause_ns = std.math.mul(
            u64,
            @max(@as(u64, 1), options.max_activation_pause_ms),
            std.time.ns_per_ms,
        ) catch std.math.maxInt(u64);
        var convergence_round: usize = 0;
        while (!DB.repairActivationAdmissible(
            db.core.nextDerivedSequence() -| converged_sequence,
            observed_ns_per_sequence,
            max_activation_gap_sequences,
            max_activation_pause_ns,
        ) and
            convergence_round < max_convergence_rounds) : (convergence_round += 1)
        {
            const convergence_target = db.core.nextDerivedSequence();
            if (durable_repair_id) |repair_id| try self.updateIntent(alloc, repair_id, .{
                .phase = .catching_up,
                .candidate_applied_sequence = converged_sequence,
                .target_sequence = convergence_target,
            });
            catch_up_start_sequence = converged_sequence;
            catch_up_start_ns = db_mod.monotonicTimeNs();
            const convergence_catch_up = try self.catchUpShadowReplacementUntil(
                alloc,
                &shadow_manager,
                shadow_checkpoint_path,
                index_ref,
                convergence_target,
                options,
                null,
                0,
            );
            converged_sequence = convergence_catch_up.applied_sequence;
            if (options.capacity_check) |check| {
                try check.current(try db_mod.repairCandidateStorageBytes(alloc, cfg, &shadow_manager, shadow_base));
            }
            observed_ns_per_sequence = DB.observeRepairCatchUpCost(
                observed_ns_per_sequence,
                catch_up_start_sequence,
                converged_sequence,
                db_mod.elapsedSince(catch_up_start_ns),
            );
            if (convergence_catch_up.yielded) {
                if (durable_repair_id) |repair_id| try self.updateIntent(alloc, repair_id, .{
                    .phase = .catching_up,
                    .candidate_applied_sequence = converged_sequence,
                    .target_sequence = convergence_target,
                    .failure_streak = 0,
                    .next_retry_at_ms = 0,
                    .replace_last_error = true,
                });
                candidate_reopenable = durable_repair_id != null;
                return .{ .reprocessed = reprocessed_this_pass, .yielded = true };
            }
            try DB.finalizeShadowNativePostingGeneration(
                &shadow_manager,
                index_ref,
                converged_sequence,
                false,
                false,
            );
            try db_mod.index_generation_manifest.writeReadyForPhysicalFormatWithStorage(
                db.core.index_manager.generation_storage,
                alloc,
                shadow_index_path,
                generation_id,
                cfg.name,
                db_mod.types.indexConfigHash(cfg),
                converged_sequence,
                try shadowPhysicalFormat(&shadow_manager, index_ref),
            );
            if (durable_repair_id) |repair_id| try self.updateIntent(alloc, repair_id, .{
                .phase = .ready,
                .candidate_applied_sequence = converged_sequence,
                .target_sequence = convergence_target,
            });
            if (durable_repair_id) |repair_id| try self.updateIntent(alloc, repair_id, .{
                .phase = .waiting_for_convergence,
                .candidate_applied_sequence = converged_sequence,
                .target_sequence = db.core.nextDerivedSequence(),
            });
        }
        if (!DB.repairActivationAdmissible(
            db.core.nextDerivedSequence() -| converged_sequence,
            observed_ns_per_sequence,
            max_activation_gap_sequences,
            max_activation_pause_ns,
        )) {
            return error.RepairActivationBudgetExhausted;
        }
        // Flatten outside the reader/apply pause. Final fenced replay below is
        // deliberately allowed to remain as a small committed WAL tail: a
        // whole-corpus rewrite inside the activation budget would make repair
        // latency scale with index size rather than with the bounded catch-up
        // gap.
        try DB.finalizeShadowNativePostingGeneration(
            &shadow_manager,
            index_ref,
            converged_sequence,
            true,
            true,
        );
        try db_mod.index_generation_manifest.writeReadyForPhysicalFormatWithStorage(
            db.core.index_manager.generation_storage,
            alloc,
            shadow_index_path,
            generation_id,
            cfg.name,
            db_mod.types.indexConfigHash(cfg),
            converged_sequence,
            try shadowPhysicalFormat(&shadow_manager, index_ref),
        );
        const ready_sequence = try db_mod.index_generation_manifest.validateReadyWithStorage(
            db.core.index_manager.generation_storage,
            alloc,
            shadow_index_path,
            generation_id,
            cfg.name,
            db_mod.types.indexConfigHash(cfg),
        );
        if (ready_sequence != converged_sequence) return error.IndexGenerationManifestMismatch;
        try db_mod.checkArtifactRepairActivationOwner(options);
        // The ordinary derived worker owns an in-memory replay cursor and
        // applied watermark that are independent of the durable projection
        // checkpoint. Stop it before publication. Otherwise it can resume
        // after the pointer swap from its pre-repair cursor and apply journal
        // history that the shadow's snapshot already includes, corrupting the
        // newly activated generation with stale deletes. The replacement is
        // caught up under the activation fence below; reinstall the worker at
        // that exact baseline after publication.
        var worker_reinstall_needed = false;
        var worker_reinstall_sequence: u64 = 0;
        defer if (worker_reinstall_needed) {
            db.executor.addWorker(cfg.name, index_ref, worker_reinstall_sequence) catch |err| {
                std.log.err(
                    "failed to restore derived worker after index repair activation index={s} sequence={} err={s}",
                    .{ cfg.name, worker_reinstall_sequence, @errorName(err) },
                );
            };
            const restore_target = db.core.nextDerivedSequence();
            if (restore_target > worker_reinstall_sequence) db.executor.notifyIndexes(restore_target, &.{cfg.name});
        };
        if (db.start_index_workers) {
            worker_reinstall_sequence = try db.core.loadAppliedSequence(alloc, cfg.name);
            worker_reinstall_needed = true;
            if (db.executor.appliedSequence(cfg.name)) |worker_applied| {
                db.executor.removeWorker(cfg.name);
                worker_reinstall_sequence = @max(worker_reinstall_sequence, worker_applied);
            }
        }
        // Final publication detaches and later destroys the active inline
        // generation. Drain maintenance before starting the bounded reader
        // pause: this work has no catalog effect and may wait for an active
        // merge or compaction whose duration is not part of query downtime.
        var structural_guard = db.beginDrainedIndexStructuralMutation(
            "index repair activation",
            cfg.name,
        );
        defer structural_guard.deinit();
        const activation_started_ns = db_mod.monotonicTimeNs();
        const activation_deadline_ns = activation_started_ns +| max_activation_pause_ns;
        if (!structural_guard.acquireCatalogBarrierUntil(activation_deadline_ns)) {
            return error.RepairActivationBudgetExhausted;
        }
        var unpublished_replacement: ?db_mod.index_manager_mod.IndexManager.DetachedIndex = null;
        defer if (unpublished_replacement) |*replacement| {
            shadow_manager.destroyDetachedReplacementIndex(replacement);
        };
        var retired_generation: ?db_mod.index_manager_mod.IndexManager.DetachedIndex = null;
        defer if (retired_generation) |*retired| {
            db.core.index_manager.destroyDetachedReplacementIndex(retired);
        };
        var activation_metric_recorded = false;
        defer if (!activation_metric_recorded) {
            if (db.core.index_manager.resource_manager) |manager| {
                manager.recordIndexRepairActivation(db_mod.monotonicTimeNs() -| activation_started_ns, max_activation_pause_ns);
            }
        };
        try db_mod.checkArtifactRepairCancelled(options);
        if (!db.lockApplyUntil(activation_deadline_ns)) return error.RepairActivationBudgetExhausted;
        var apply_lock_held = true;
        defer if (apply_lock_held) db.core.unlockApply();

        const final_target = db.core.nextDerivedSequence();
        if (!DB.repairActivationAdmissible(
            final_target -| converged_sequence,
            observed_ns_per_sequence,
            max_activation_gap_sequences,
            max_activation_pause_ns,
        )) {
            return error.RepairActivationBudgetExhausted;
        }
        const activation_replay_deadline_ns = DB.repairActivationReplayDeadline(
            db_mod.monotonicTimeNs(),
            activation_deadline_ns,
            max_activation_pause_ns,
        ) orelse return error.RepairActivationBudgetExhausted;
        const activation_catch_up = self.catchUpShadowReplacementUntil(
            alloc,
            &shadow_manager,
            shadow_checkpoint_path,
            index_ref,
            final_target,
            options,
            activation_replay_deadline_ns,
            observed_ns_per_sequence,
        ) catch |err| switch (err) {
            error.CatchUpDeadlineExceeded => return error.RepairActivationBudgetExhausted,
            else => return err,
        };
        std.debug.assert(!activation_catch_up.yielded);
        const reached_target = activation_catch_up.applied_sequence;
        if (reached_target < final_target) return error.ShadowIndexCatchUpIncomplete;
        // Catch-up cannot repair a write which never entered the replay log.
        // Recheck the original snapshot's persisted source guard under the
        // activation writer fence, before publishing any candidate pointer.
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const guard = build_source_guard orelse return error.RepairSourceChanged;
            guard.requireCurrent(&read) catch |err| switch (err) {
                error.EnrichmentSourceChanged => {
                    candidate_reopenable = false;
                    return error.RepairSourceChanged;
                },
                else => return err,
            };
        }
        // The generation-scoped coverage tuple is maintained in the same
        // fenced primary write stream and is O(1) to read.
        if (cfg.kind == .dense_vector) {
            const dense_entry = shadow_manager.denseIndex(cfg.name) orelse return error.IndexNotFound;
            if (db_mod.denseIndexIsArtifactBacked(dense_entry)) {
                const expected = (try db_mod.denseTargetCountForIndexContext(db.async_context, cfg.name)) orelse
                    return error.RepairSourceCoverageIncomplete;
                if (!db_mod.denseCoverageMatchesTarget(dense_entry.index.stats().active_count, expected)) {
                    return error.RepairSourceCoverageIncomplete;
                }
            } else if (dense_entry.managed_direct_field) {
                // An index created over old raw rows may have only a partial
                // outcome tuple. Complete snapshot + fenced journal replay is
                // the proof in that case; a complete tuple strengthens it.
                if (try db_mod.denseTargetCountForIndexContext(db.async_context, cfg.name)) |expected| {
                    if (!db_mod.denseCoverageMatchesTarget(dense_entry.index.stats().active_count, expected)) return error.RepairSourceCoverageIncomplete;
                }
            }
        }
        // Catch-up persisted every source window through the native WAL before
        // advancing its shadow watermark. Verify that exact boundary while the
        // source writer is fenced, but never perform full compaction here.
        try DB.finalizeShadowNativePostingGeneration(
            &shadow_manager,
            index_ref,
            reached_target,
            false,
            false,
        );
        try db_mod.checkArtifactRepairCancelled(options);
        try db_mod.checkArtifactRepairActivationOwner(options);
        try DB.ensureRepairActivationDeadline(activation_deadline_ns);
        if (durable_repair_id) |repair_id| try self.validateActivationState(alloc, repair_id, options.owner_epoch);
        try DB.ensureRepairActivationDeadline(activation_deadline_ns);
        const previous_active_pointer = try db.core.index_manager.captureActiveIndexRootPointer(cfg.name);
        defer if (previous_active_pointer) |value| db.core.index_manager.alloc.free(value);
        try DB.ensureRepairActivationDeadline(activation_deadline_ns);
        if (durable_repair_id) |repair_id| try self.updateIntent(alloc, repair_id, .{
            .phase = .activating,
            .candidate_applied_sequence = reached_target,
            .target_sequence = final_target,
            .previous_pointer_captured = true,
            .previous_active_relative_path = previous_active_pointer,
            .replace_previous_active_path = true,
        });
        try DB.ensureRepairActivationDeadline(activation_deadline_ns);

        // Final replay may advance beyond the preliminary ready marker. Make
        // readiness describe the exact durable generation installed by the
        // pointer swap, not merely the pre-barrier candidate.
        if (cfg.kind == .full_text) if (build_source_guard) |guard| if (guard.boundary) |boundary| {
            const text = shadow_manager.textIndexEntry(cfg.name) orelse return error.IndexNotFound;
            try text.persistent.publishProjectionSeal(.{
                .root = db.root_incarnation,
                .namespace = boundary.authority.namespace,
                .generation = cfg.coverage_generation,
                .config_hash = db_mod.types.indexConfigHash(cfg),
                .applied_sequence = reached_target,
                .baseline = guard,
            });
            try DB.ensureRepairActivationDeadline(activation_deadline_ns);
        };
        try db_mod.index_generation_manifest.writeReadyForPhysicalFormatWithStorage(
            db.core.index_manager.generation_storage,
            alloc,
            shadow_index_path,
            generation_id,
            cfg.name,
            db_mod.types.indexConfigHash(cfg),
            reached_target,
            try shadowPhysicalFormat(&shadow_manager, index_ref),
        );
        try DB.ensureRepairActivationDeadline(activation_deadline_ns);
        const manifest_sequence = try db_mod.index_generation_manifest.validateReadyWithStorage(
            db.core.index_manager.generation_storage,
            alloc,
            shadow_index_path,
            generation_id,
            cfg.name,
            db_mod.types.indexConfigHash(cfg),
        );
        if (manifest_sequence != reached_target) return error.IndexGenerationManifestMismatch;
        try DB.ensureRepairActivationDeadline(activation_deadline_ns);

        if (cfg.kind == .algebraic) {
            // Certify this physical projection only after the final replay has
            // converged under the apply fence. The marker lives in the shadow
            // namespace, so a crash before pointer publication cannot release
            // the active generation's lifecycle gate.
            try shadow_manager.persistAlgebraicProjectionConfigReady(db.core.store, cfg.name);
            try DB.ensureRepairActivationDeadline(activation_deadline_ns);
        }

        // Transfer the already-open runtime for every index family. Pointer
        // publication and in-memory handoff are allocation-free, and the
        // predecessor is retired only after service fences are released.
        unpublished_replacement = try shadow_manager.detachReplacementIndex(cfg.name);
        retired_generation = try db.core.index_manager.installAdoptedReplacementIndex(
            db.core.store,
            cfg,
            shadow_index_path,
            previous_active_pointer,
            &unpublished_replacement.?,
        );
        unpublished_replacement = null;
        shadow_installed = true;
        if (self.shadow_hook) |hook| {
            if (hook.after_pointer_activation) |after_activation| try after_activation(hook.ptr, db, cfg.name);
        }
        if (durable_repair_id) |repair_id| try self.updateIntent(alloc, repair_id, .{
            .phase = .validating,
            .candidate_applied_sequence = final_target,
            .target_sequence = final_target,
        });
        db.core.index_manager.clearRepairShadow(alloc, shadow_base) catch |err| {
            std.log.warn("failed to clear repair shadow in-progress marker index={s} err={s}", .{ cfg.name, @errorName(err) });
        };
        const prior_checkpoint = try db.core.loadProjectionCheckpoint(alloc, cfg.name);
        const final_update = db_mod.apply_state.AppliedSequenceUpdate{
            .index_name = cfg.name,
            .sequence = final_target,
            .status = .clean,
            .generation = prior_checkpoint.generation +| 1,
            .config_hash = db_mod.types.indexConfigHash(cfg),
        };
        try DB.saveIndexStatusSnapshots(alloc, db.core.store, db.core.index_manager, &[_]db_mod.apply_state.AppliedSequenceUpdate{final_update});
        try db.core.saveAppliedSequence(cfg.name, final_target);
        try db.core.saveProjectionCheckpoint(cfg.name, .{
            .applied_sequence = final_update.sequence,
            .status = final_update.status,
            .generation = final_update.generation,
            .config_hash = final_update.config_hash,
        });
        // The pointer and clean checkpoint are now ordered before any later
        // foreground apply. Release the write fence before operator hooks,
        // intent cleanup, and generation garbage collection. Capture the
        // actual service pause before releasing it.
        // Generation teardown and other post-commit work are measured
        // separately and cannot produce false pause-budget alarms.
        const activation_pause_ns = db_mod.monotonicTimeNs() -| activation_started_ns;
        db.core.unlockApply();
        apply_lock_held = false;
        structural_guard.releaseCatalogBarrier();
        if (retired_generation) |*retired| {
            db.core.index_manager.destroyDetachedReplacementIndex(retired);
            retired_generation = null;
        }
        if (shadow_manager_open) {
            shadow_manager.transferLsmOwnerCloneStatsTo(db.core.index_manager) catch |err| {
                std.log.warn("failed to preserve activated shadow LSM clone attribution index={s} err={s}", .{
                    cfg.name,
                    @errorName(err),
                });
            };
            shadow_manager.deinit();
            shadow_manager_open = false;
        }
        structural_guard.release();
        if (worker_reinstall_needed) {
            worker_reinstall_sequence = final_target;
            try db.executor.addWorker(cfg.name, index_ref, worker_reinstall_sequence);
            worker_reinstall_needed = false;
            const current_target = db.core.nextDerivedSequence();
            if (current_target > worker_reinstall_sequence) db.executor.notifyIndexes(current_target, &.{cfg.name});
        }
        if (db.core.index_manager.resource_manager) |manager| {
            manager.recordIndexRepairActivation(activation_pause_ns, max_activation_pause_ns);
        }
        activation_metric_recorded = true;
        if (activation_pause_ns > max_activation_pause_ns) {
            std.log.warn(
                "index repair activation exceeded pause budget index={s} elapsed_ms={} budget_ms={}",
                .{
                    cfg.name,
                    @divTrunc(activation_pause_ns, std.time.ns_per_ms),
                    @divTrunc(max_activation_pause_ns, std.time.ns_per_ms),
                },
            );
        }
        if (self.shadow_hook) |hook| {
            if (hook.after_clean_checkpoint) |after_checkpoint| try after_checkpoint(hook.ptr, db, cfg.name);
        }
        if (durable_repair_id) |repair_id| {
            try self.updateIntent(alloc, repair_id, .{
                .phase = .cleanup,
                .candidate_applied_sequence = final_target,
                .target_sequence = final_target,
            });
            // The clean checkpoint and replacement are durable before this
            // single local transition releases replay retention and removes
            // the intent.
            try self.removeIntentAndPin(alloc, repair_id);
            self.scheduleInactiveShadowCleanup();
        }

        return .{
            .reprocessed = reprocessed_this_pass,
            .applied_sequence = final_target,
            .unresolved_artifacts = repair_issue_counter.load(.monotonic),
        };
    }

    fn catchUpShadowReplacementUntil(
        self: *RepairIndexes,
        alloc: Allocator,
        shadow_manager: *db_mod.index_manager_mod.IndexManager,
        shadow_checkpoint_path: ?[]const u8,
        index_ref: db_mod.index_manager_mod.ManagedIndexRef,
        target_sequence: u64,
        options: db_mod.types.ArtifactRepairRunOptions,
        deadline_ns: ?u64,
        observed_ns_per_sequence: u64,
    ) !DB.ShadowCatchUpResult {
        const db = self.dbPtr();
        if (target_sequence == 0) return .{ .applied_sequence = 0 };
        // Cooperative yielding is only valid before activation fencing. Final
        // replay has its own hard deadline and must either reach that target or
        // abort activation while the write/search barriers remain held.
        const cooperative = deadline_ns == null and options.yield_check != null;
        var batch_ctx = db.batchContext();
        batch_ctx.index_manager = shadow_manager;
        batch_ctx.applied_sequence_checkpoint_path = shadow_checkpoint_path;
        batch_ctx.async_context = null;
        batch_ctx.dense_bulk_session_scope = .external;

        var applied = try db_mod.apply_state.loadAppliedSequenceWithCheckpointWithStorage(
            shadow_manager.generation_storage,
            alloc,
            shadow_manager.checkpointIo(),
            db.core.store,
            shadow_checkpoint_path,
            index_ref.name,
        );
        while (applied < target_sequence) {
            try db_mod.checkArtifactRepairCancelled(options);
            if (index_ref.kind == .algebraic) {
                if (options.capacity_check) |check| {
                    try check.current(try shadow_manager.activeIndexStorageBytes(index_ref.name));
                }
            }
            if (deadline_ns) |deadline| {
                if (db_mod.monotonicTimeNs() >= deadline) return error.RepairActivationBudgetExhausted;
            }
            var replay_ctx = db_mod.ReplayApplyContextBatch{
                .batch = &batch_ctx,
                .dense_bulk_session_scope = .external,
            };
            const remaining_sequences = target_sequence -| applied;
            var max_records_per_window = if (comptime builtin.is_test)
                db_mod.test_index_repair_catch_up_max_records_per_window orelse db_mod.derived_worker.catch_up_max_records_per_window_default
            else
                db_mod.derived_worker.catch_up_max_records_per_window_default;
            var max_items_per_window: usize = 0;
            if (deadline_ns) |deadline| {
                const now = db_mod.monotonicTimeNs();
                if (now >= deadline) return error.CatchUpDeadlineExceeded;
                const per_sequence_ns = @max(@as(u64, 1), observed_ns_per_sequence);
                const affordable_sequences = (deadline - now) / per_sequence_ns;
                if (affordable_sequences == 0) return error.CatchUpDeadlineExceeded;
                const bounded_sequences = @min(remaining_sequences, affordable_sequences);
                max_records_per_window = @max(@as(usize, 1), @min(
                    db_mod.derived_worker.catch_up_max_records_per_window_default,
                    std.math.cast(usize, bounded_sequences) orelse std.math.maxInt(usize),
                ));
                max_items_per_window = max_records_per_window;
            }
            const catch_up_stats = try db_mod.derived_worker.catchUpIndexWithOptions(
                alloc,
                db.core.replaySource(),
                index_ref,
                applied,
                &replay_ctx,
                db_mod.applyDerivedBatchToIndexReplayContext,
                .{
                    .resource_manager = shadow_manager.resource_manager,
                    .window_ctx = &replay_ctx,
                    .begin_window_fn = db_mod.beginDerivedCatchUpWindowContext,
                    .finish_window_fn = db_mod.finishDerivedCatchUpWindowContext,
                    .target_sequence = target_sequence,
                    .max_records_per_window = max_records_per_window,
                    .max_items_per_window = max_items_per_window,
                    // One independently durable window lets the owner inspect
                    // its cooperative deadline without adding a clock read to
                    // every replay record or changing the ordinary catch-up
                    // path. The outer loop immediately reopens from the saved
                    // applied sequence while budget remains.
                    .max_windows_per_call = if (cooperative) 1 else 0,
                    .deadline_ns = deadline_ns,
                },
            );
            if (index_ref.kind == .algebraic) {
                if (options.capacity_check) |check| {
                    try check.current(try shadow_manager.activeIndexStorageBytes(index_ref.name));
                }
            }
            const advanced = catch_up_stats.appliedSequenceAdvance(applied) orelse blk: {
                if (catch_up_stats.shouldTryTargetAdvance(applied, target_sequence) and
                    try db_mod.canAdvanceDerivedReplayTargetContext(&batch_ctx, index_ref, applied, target_sequence))
                {
                    break :blk target_sequence;
                }
                break :blk applied;
            };
            if (advanced <= applied) return .{ .applied_sequence = applied };
            try self.saveShadowReplacementAppliedSequence(alloc, shadow_manager, shadow_checkpoint_path, index_ref, advanced);
            applied = advanced;
            if (cooperative and applied < target_sequence and options.yield_check.?.requested()) {
                return .{ .applied_sequence = applied, .yielded = true };
            }
            if (deadline_ns) |deadline| {
                if (applied < target_sequence and db_mod.monotonicTimeNs() >= deadline) {
                    return error.RepairActivationBudgetExhausted;
                }
            }
        }
        try db_mod.checkArtifactRepairCancelled(options);
        return .{ .applied_sequence = applied };
    }

    fn saveShadowReplacementAppliedSequence(
        self: *RepairIndexes,
        alloc: Allocator,
        shadow_manager: *db_mod.index_manager_mod.IndexManager,
        shadow_checkpoint_path: ?[]const u8,
        index_ref: db_mod.index_manager_mod.ManagedIndexRef,
        sequence: u64,
    ) !void {
        const db = self.dbPtr();
        try shadow_manager.checkpointLsmWalForManagedIndex(index_ref);
        const update = db_mod.apply_state.AppliedSequenceUpdate{
            .index_name = index_ref.name,
            .sequence = sequence,
            .config_hash = if (shadow_manager.get(index_ref.name)) |value| db_mod.types.indexConfigHash(value.*) else 0,
        };
        const updates = [_]db_mod.apply_state.AppliedSequenceUpdate{update};
        try db_mod.savePhysicalProjectionMetadataForAppliedSequenceUpdates(shadow_manager, &updates);
        try db_mod.checkpointManagedProjectionEffectsForAppliedSequenceUpdates(shadow_manager, &updates);
        try db_mod.apply_state.saveAppliedSequenceUpdateWithCheckpointWithStorage(
            shadow_manager.generation_storage,
            alloc,
            shadow_manager.checkpointIo(),
            db.core.store,
            shadow_checkpoint_path,
            update,
        );
    }

    fn shadowPhysicalFormat(
        shadow_manager: *db_mod.index_manager_mod.IndexManager,
        index_ref: db_mod.index_manager_mod.ManagedIndexRef,
    ) !db_mod.index_generation_manifest.PhysicalFormat {
        if (index_ref.kind != .dense_vector) return .legacy_lsm;
        const entry = shadow_manager.denseIndex(index_ref.name) orelse return error.IndexNotFound;
        return if (entry.index.experimentalPostingWalAuthoritative())
            .dense_native_v2
        else
            .legacy_lsm;
    }

    /// Classify a generation after its replacement is durably committed.
    /// Both the initial response and crash-idempotent job replay enter here so
    /// a completion marker can never be mistaken for proof of healthy coverage.
    pub fn finalizeCommittedOutcome(
        self: *RepairIndexes,
        alloc: Allocator,
        index_name: []const u8,
        observed_unresolved_artifacts: u64,
        result: *db_mod.types.ArtifactRepairResult,
    ) !void {
        const db = self.dbPtr();
        const repair_summary_ready = try db.artifactRepairSummaryReady(alloc);
        const unresolved_artifacts = if (repair_summary_ready)
            @max(
                observed_unresolved_artifacts,
                (try db.artifactRepairSummaryIndexSnapshot(alloc, index_name, true)).count,
            )
        else
            observed_unresolved_artifacts;
        result.unresolved += unresolved_artifacts;

        // A rebuilding summary cannot prove absence. Preserve debt without a
        // corpus scan on this latency-sensitive completion path; background
        // summary maintenance will publish the exact count.
        result.debt_remaining = result.debt_remaining or !repair_summary_ready or unresolved_artifacts != 0;

        const generation_repair_required = try db.indexGenerationRepairRequired(alloc, index_name);
        const coverage = try db.committedIndexCoverageDisposition(alloc, index_name);
        if (generation_repair_required or coverage != .complete) {
            result.unresolved += 1;
            result.debt_remaining = true;
            result.indexes_degraded_after = 1;
        }

        // "Rebuilt" describes completed work. "Repaired" is the stronger
        // operator promise that no known or indeterminate debt remains.
        result.indexes_degraded_after = @intFromBool(result.debt_remaining);
        if (!result.debt_remaining) {
            result.repaired += 1;
        }
    }

    pub fn applyDurableStats(
        self: *RepairIndexes,
        alloc: Allocator,
        state: ?*const db_mod.index_repair_state.State,
        repair_state_corrupt: bool,
        item: *db_mod.types.DBIndexStats,
    ) !void {
        const db = self.dbPtr();
        // Compatibility overlays can carry a retired intent. Replace the
        // complete durable lifecycle bundle, including the no-intent case.
        if (item.index_repair_last_error) |value| alloc.free(value);
        const defaults = db_mod.types.DBIndexStats{ .name = item.name, .kind = item.kind };
        inline for (comptime std.meta.fieldNames(db_mod.types.DBIndexStats)) |reflected_name| {
            if (comptime std.mem.startsWith(u8, reflected_name, "index_repair_") or
                std.mem.eql(u8, reflected_name, "index_lifecycle_work_class"))
            {
                @field(item, reflected_name) = @field(defaults, reflected_name);
            }
        }
        if (repair_state_corrupt) {
            item.index_lifecycle_work_class = .repair;
            item.index_repair_trigger = "corrupt_local_repair_state";
            item.index_repair_phase = "terminal";
            item.index_repair_wait_reason = "terminal";
            item.index_repair_status = .failed;
            item.index_repair_action_required = true;
            item.repair_degraded = true;
            return;
        }
        const durable = state orelse return;
        const i = durable.findIndex(item.name) orelse return;
        const intent = durable.entries.items[i].intent;
        item.index_repair_id = intent.repair_id;
        item.index_lifecycle_work_class = switch (intent.work_class) {
            .initial_build => .initial_build,
            .repair => .repair,
        };
        item.index_repair_trigger = @tagName(intent.trigger);
        item.index_repair_phase = @tagName(intent.phase);
        item.index_repair_automation = @tagName(intent.automation);
        item.index_repair_attempts = intent.attempt_count;
        item.index_repair_started_at_ms = intent.started_at_ms;
        item.index_repair_updated_at_ms = intent.updated_at_ms;
        item.index_repair_build_floor_sequence = intent.build_floor_sequence;
        item.index_repair_applied_sequence = intent.candidate_applied_sequence;
        item.index_repair_target_sequence = intent.target_sequence;
        item.index_repair_next_retry_at_ms = intent.next_retry_at_ms;
        item.index_repair_last_error = if (intent.last_error) |value| try alloc.dupe(u8, value) else null;
        item.index_repair_wait_reason = if (intent.automation == .paused)
            "paused"
        else if (intent.phase == .terminal)
            "terminal"
        else if (intent.next_retry_at_ms > self.nowMs())
            "backoff"
        else if (intent.phase == .rolling_back)
            "rollback"
        else if (intent.phase == .waiting_for_convergence)
            "convergence"
        else
            "none";
        item.index_repair_action_required = actionRequired(intent);
        item.index_repair_status = db_mod.index_repair_status.summarize(
            true,
            item.index_repair_automation,
            item.index_repair_phase,
            item.index_repair_wait_reason,
            item.index_repair_action_required,
        );
        // Keep status available when either optional bounded proof cannot be
        // read; query admission independently retries it and remains closed.
        // A complete active generation makes managed-admission repair debt
        // redundant even before its scheduler quantum removes the intent.
        // Preserve degradation for merely queryable progressive generations:
        // their physical artifact count can still be advancing.
        const active_generation_complete =
            db.managedAdmissionGenerationIsServiceable(alloc, intent) catch false;
        // Use the same intent-scoped exception as query admission. An older
        // publication certificate can still match after damage discovery or
        // after admission hands off to a shadow build. It cannot override that
        // repair's closed gate: only an initial canonical build may establish
        // progressive serviceability without replacement validation.
        const active_generation_queryable =
            db.managedAdmissionGenerationIsQueryable(alloc, intent) catch false;
        const generation_installed = db.repairGenerationInstalled(intent);
        // The resident gate is version/incarnation fenced. Pair its decision
        // with physical presence so neither a missing slot nor a stale config
        // can manufacture serviceability.
        const resident_admission_open = !db.core.index_manager.repairUnavailable(intent.index_name) and
            db.core.index_manager.loadFailure(intent.index_name) == null and
            generation_installed;
        const admitted_serving_generation = repairAdmission(intent) == .serviceable and
            resident_admission_open;
        // Query admission is monotonic for one managed incarnation. Once an
        // authoritative status sample proves the progressive generation safe,
        // clear the same resident gate used by search. A following producer
        // batch can temporarily move the artifact counter ahead of the HBC
        // publication boundary; that must not make status revoke a generation
        // which queries continue serving from its last published snapshot.
        if (active_generation_complete or active_generation_queryable) {
            db.core.index_manager.publishRepairServiceabilityProof(
                intent.index_name,
                DB.repairAdmissionVersion(intent, intent.revision, intent.control_revision),
            );
        }
        item.index_repair_active_generation_serviceable = active_generation_complete or
            active_generation_queryable or
            admitted_serving_generation;
        if (intent.work_class == .initial_build) {
            // A new catalog definition over an existing corpus is normal
            // restartable build work. It remains gated until publication,
            // but is neither corruption nor degraded service.
            item.backfill_active = intent.phase != .terminal;
            item.repair_degraded = false;
        } else switch (intent.trigger) {
            .incomplete_bulk_publish,
            .text_projection_upgrade,
            .root_generation_rebuild,
            .projection_generation_invalid,
            .operator_generation_validation,
            .catalog_admission,
            => item.repair_degraded = true,
            .artifact_coverage_mismatch, .artifact_counter_missing => {
                // Source-coverage proof is incomplete or false. Keep the
                // compact degraded state visible while queries fail closed.
                item.backfill_active = intent.phase != .terminal;
                item.repair_degraded = true;
            },
            .replay_artifact_unavailable => {
                item.backfill_active = intent.phase != .terminal;
                item.repair_degraded = true;
            },
            .operator_generation_rebuild, .storage_format_migration, .artifact_baseline_adoption => {
                // The prior generation remains the serving generation during
                // construction. Report background rebuild activity without
                // falsely degrading a healthy index; activation/validation or
                // a terminal replacement failure remains visible as degraded.
                item.backfill_active = intent.phase != .terminal;
                item.repair_degraded = intent.phase == .activating or
                    intent.phase == .validating or
                    intent.phase == .rolling_back or
                    intent.phase == .terminal;
            },
        }
        if (active_generation_complete) item.repair_degraded = false;
    }

    pub fn loadStateForStats(self: *const RepairIndexes, alloc: Allocator) !?db_mod.index_repair_state.State {
        if (comptime builtin.os.tag == .freestanding) {
            return null;
        } else {
            return self.loadState(alloc) catch |err| switch (err) {
                error.FileNotFound, error.DurableIndexRepairStateUnavailable, error.InvalidIndexRepairState => null,
                else => return err,
            };
        }
    }

    pub fn currentNonInitialIntentMatches(
        self: *const RepairIndexes,
        state: *const db_mod.index_repair_state.State,
        cfg: db_mod.types.IndexConfig,
    ) bool {
        const db = self.dbConstPtr();
        const index = state.findIndex(cfg.name) orelse return false;
        const intent = state.entries.items[index].intent;
        return intent.work_class != .initial_build and
            intent.phase != .terminal and
            intent.root_generation == db.core.root_generation and
            intent.group_id == db.localRepairGroupId() and
            intent.config_hash == db_mod.types.indexConfigHash(cfg);
    }
};

test "db close retires runtime owners for memory primary backend" {
    const alloc = std.testing.allocator;
    var runtime = try db_mod.background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .filesystem_io = std.testing.io,
    });
    defer runtime.deinit();

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
        .primary_backend = .{ .mem = .{} },
    });
    const backend_owner_id = db.backend_owner_id;
    const repair_cleanup_owner_id = db.indexes.repair.cleanup_owner_id;
    db.close();

    const Fns = struct {
        fn run(_: *anyopaque) !void {}
        pub fn deinit(_: *anyopaque) void {}
    };
    var ctx: u8 = 0;
    try std.testing.expectError(error.BackgroundOwnerClosed, runtime.ptr().durable_jobs.submit(.{
        .owner_id = backend_owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    }));
    try std.testing.expectError(error.BackgroundOwnerClosed, runtime.ptr().durable_jobs.submit(.{
        .owner_id = repair_cleanup_owner_id,
        .class = .cleanup,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    }));
}

test "db vector status revalidates stale repair admission without a query" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_status",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sparse_status",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.addIndex(.{
        .name = "repair_sibling",
        .kind = .full_text,
        .config_json = "{}",
    });

    // Keep unrelated nonblocking durable debt so stats exercises one preloaded
    // repair snapshot rather than rereading it for each stale vector gate.
    const sibling_cfg = db.core.index_manager.get("repair_sibling") orelse return error.TestUnexpectedResult;
    _ = try db.indexes.repair.createGenerationIntent(
        alloc,
        sibling_cfg.*,
        .operator_generation_rebuild,
        0,
        0,
        null,
    );

    // Model a process that observed durable repair debt immediately before
    // another owner either made the intent serviceable or removed it. Status
    // is itself an admission observer: it must heal the stale resident gates
    // rather than wait for a user query to make index wait truthful.
    try db.core.index_manager.markUnscopedRepairUnavailable("dense_status");
    try db.core.index_manager.markUnscopedRepairUnavailable("sparse_status");
    try std.testing.expect(db.core.index_manager.repairUnavailable("dense_status"));
    try std.testing.expect(db.core.index_manager.repairUnavailable("sparse_status"));

    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    var observed_dense = false;
    var observed_sparse = false;
    for (stats.indexes) |index_stats| {
        if (std.mem.eql(u8, index_stats.name, "dense_status")) {
            observed_dense = true;
            try std.testing.expect(index_stats.serving_snapshot_ready);
            try std.testing.expectEqual(db.backend_owner_id, index_stats.serving_snapshot_owner_id);
            try std.testing.expectEqual(db.status_owner_epoch, index_stats.serving_publication.?.owner_epoch);
        } else if (std.mem.eql(u8, index_stats.name, "sparse_status")) {
            observed_sparse = true;
            try std.testing.expect(index_stats.serving_snapshot_ready);
            try std.testing.expectEqual(db.status_owner_epoch, index_stats.serving_publication.?.owner_epoch);
        }
    }
    try std.testing.expect(observed_dense);
    try std.testing.expect(observed_sparse);
    try std.testing.expect(!db.core.index_manager.repairUnavailable("dense_status"));
    try std.testing.expect(!db.core.index_manager.repairUnavailable("sparse_status"));
    var refreshed = try db.runtimeStatusStatsConsistent(alloc);
    defer db_mod.types.freeDBStats(alloc, refreshed);
    try std.testing.expect(refreshed.indexes[0].serving_publication.?.revision > stats.indexes[0].serving_publication.?.revision);
    try std.testing.expect(db.overlayRuntimeStatusBestEffort(alloc, &refreshed));
    for (refreshed.indexes) |item| {
        try std.testing.expectEqual(@as(?@import("../publication.zig").Stamp, null), item.serving_publication);
        try std.testing.expectEqual(@as(?@import("../publication.zig").Stamp, null), item.coverage_publication);
    }
}

test "db managed operator repair persists intent without running reconstruction inline" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = "graph_v1",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), queued.scanned);
    try std.testing.expectEqual(@as(u64, 1), queued.in_progress);
    try std.testing.expectEqual(@as(u64, 0), queued.indexes_rebuilt);
    try std.testing.expect(queued.debt_remaining);

    const repair_id = (try db.indexes.repair.idForIndex(alloc, "graph_v1")) orelse
        return error.TestUnexpectedResult;
    var pending = try db.indexes.repair.loadEntryById(alloc, repair_id);
    defer pending.deinit(alloc);
    try std.testing.expectEqual(db_mod.index_repair_state.Phase.detected, pending.intent.phase);
    try std.testing.expect(pending.intent.candidate_relative_path == null);

    const completed = try db.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(completed.attempted);
    try std.testing.expect(completed.repaired);
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));
}

test "index repair advance lease covers cancellation and deletion" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = "graph_v1",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    const repair_id = (try db.indexes.repair.idForIndex(alloc, "graph_v1")) orelse
        return error.TestUnexpectedResult;

    const Race = struct {
        db: *DB,
        repair_id: u128,
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        result: ?DB.IndexRepairAdvanceResult = null,
        err: ?anyerror = null,

        fn afterAcquire(raw: *anyopaque, _: *DB, observed_repair_id: u128) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.repair_id, observed_repair_id);
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
        }

        fn run(self: *@This()) void {
            self.result = self.db.indexes.repair.advanceIntent(self.db.alloc, self.repair_id, .{}) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    var race = Race{ .db = &db, .repair_id = repair_id };
    DB.test_index_repair_advance_lease_hook = .{
        .ptr = &race,
        .after_acquire = Race.afterAcquire,
    };
    defer DB.test_index_repair_advance_lease_hook = null;

    var advance_thread = try std.testing.io.concurrent(Race.run, .{&race});
    var joined = false;
    defer if (!joined) {
        race.release.store(true, .release);
        advance_thread.await(std.testing.io);
    };
    var entered = false;
    for (0..100_000) |_| {
        if (race.entered.load(.acquire)) {
            entered = true;
            break;
        }
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    if (!entered) return error.TestTimeout;

    // Both controls observe the same full-quantum owner. Cancellation is
    // accepted, while deletion must retry instead of crossing an unowned
    // preflight/reconciliation interval.
    try std.testing.expect(try db.indexes.repair.cancelCurrentAttempt(alloc, "graph_v1", repair_id));
    try std.testing.expectError(
        error.IndexRepairInProgress,
        db.indexes.repair.prepareForDeletion(alloc, "graph_v1"),
    );

    race.release.store(true, .release);
    advance_thread.await(std.testing.io);
    joined = true;
    DB.test_index_repair_advance_lease_hook = null;
    try std.testing.expect(race.err == null);
    try std.testing.expect(race.result != null);
    try std.testing.expect(race.result.?.deferred);
    try std.testing.expect(!race.result.?.attempted);
    try std.testing.expect(!db.indexes.repair.activeCancellationRequested("graph_v1"));

    // Cancellation is attempt-scoped. Once the owner releases its lease, the
    // next quantum can finish the still-enabled durable repair normally. A
    // non-activation phase must not even enter restart reconciliation's global
    // structural/apply fence; the ordinary activation path takes its own
    // bounded publication fence only after the candidate is ready.
    const ReconcileProbe = struct {
        calls: usize = 0,

        fn beforeFence(raw: *anyopaque, _: *DB, _: u128) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
        }
    };
    var reconcile_probe = ReconcileProbe{};
    DB.test_index_repair_reconcile_fence_hook = .{
        .ptr = &reconcile_probe,
        .before_fence = ReconcileProbe.beforeFence,
    };
    defer DB.test_index_repair_reconcile_fence_hook = null;
    const completed = try db.indexes.repair.advanceIntent(alloc, repair_id, .{});
    DB.test_index_repair_reconcile_fence_hook = null;
    try std.testing.expectEqual(@as(usize, 0), reconcile_probe.calls);
    try std.testing.expect(completed.attempted);
    try std.testing.expect(completed.repaired);
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));
}

test "db index repair shadow swap survives reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const text_cfg: db_mod.types.IndexConfig = .{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(text_cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"alpha durable\"}" },
                .{ .key = "doc:b", .value = "{\"body\":\"beta durable\"}" },
            },
            .sync_level = .full_index,
        });
        try db.core.saveProjectionCheckpoint("ft_v1", .{
            .applied_sequence = 0,
            .status = .repair_required,
            .config_hash = db_mod.types.indexConfigHash(text_cfg),
        });

        const stale_canonical_file = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1/stale-before-repair", .{std.mem.span(path)});
        defer alloc.free(stale_canonical_file);
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{
            .sub_path = stale_canonical_file,
            .data = "stale",
        });

        var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .full_text,
            .index_name = "ft_v1",
            .limit = 1,
        });
        defer repair.deinit(alloc);
        try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    }

    const abandoned_shadow = try std.fmt.allocPrint(alloc, "{s}/.repair-shadow-abandoned/indexes/ft_v1", .{std.mem.span(path)});
    defer alloc.free(abandoned_shadow);
    try db_mod.ensureDirPath(abandoned_shadow);
    const in_progress_shadow_root = try std.fmt.allocPrint(alloc, "{s}/.repair-shadow-live-build", .{std.mem.span(path)});
    defer alloc.free(in_progress_shadow_root);
    const in_progress_shadow = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1", .{in_progress_shadow_root});
    defer alloc.free(in_progress_shadow);
    try db_mod.ensureDirPath(in_progress_shadow);
    try db_mod.index_manager_mod.IndexManager.writeRepairShadowInProgressMarker(alloc, in_progress_shadow_root);

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();

        const checkpoint = try reopened.core.loadProjectionCheckpoint(alloc, "ft_v1");
        try std.testing.expectEqual(db_mod.apply_state.ProjectionStatus.clean, checkpoint.status);

        var after = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        });
        defer after.deinit();
        try std.testing.expectEqual(@as(u32, 1), after.total_hits);
        try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
        reopened.backend_runtime.durable_jobs.drainOwner(reopened.indexes.repair.cleanup_owner_id);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, abandoned_shadow, .{}));
        try std.Io.Dir.cwd().access(std.testing.io, in_progress_shadow, .{});
        const stale_canonical_file = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1/stale-before-repair", .{std.mem.span(path)});
        defer alloc.free(stale_canonical_file);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, stale_canonical_file, .{}));
    }
}

test "db index repair shadow swap preserves post snapshot mutations" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const text_cfg: db_mod.types.IndexConfig = .{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    };

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(text_cfg);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha before\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta before\"}" },
        },
        .sync_level = .full_index,
    });
    try db.core.saveProjectionCheckpoint("ft_v1", .{
        .applied_sequence = 0,
        .status = .repair_required,
        .config_hash = db_mod.types.indexConfigHash(text_cfg),
    });

    const HookContext = struct {
        fired: bool = false,
        clean_checkpoint_fired: bool = false,
        observed_floor: u64 = 0,
    };
    const Hook = struct {
        fn afterSnapshotBuild(ptr: *anyopaque, hook_db: *DB, index_name: []const u8, build_floor_sequence: u64) anyerror!void {
            const ctx: *HookContext = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("ft_v1", index_name);
            try std.testing.expect(build_floor_sequence > 0);
            try std.testing.expect(!ctx.fired);
            ctx.fired = true;
            ctx.observed_floor = build_floor_sequence;

            try hook_db.batch(.{
                .writes = &.{
                    .{ .key = "doc:b", .value = "{\"body\":\"gamma after\"}" },
                    .{ .key = "doc:c", .value = "{\"body\":\"alpha after\"}" },
                },
                .deletes = &.{"doc:a"},
                .sync_level = .write,
            });
        }

        fn afterCleanCheckpoint(ptr: *anyopaque, hook_db: *DB, index_name: []const u8) anyerror!void {
            const ctx: *HookContext = @ptrCast(@alignCast(ptr));
            try std.testing.expect(!ctx.clean_checkpoint_fired);
            ctx.clean_checkpoint_fired = true;
            const durable_applied = try hook_db.core.loadAppliedSequence(hook_db.alloc, index_name);
            const worker_applied = hook_db.executor.appliedSequence(index_name) orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(durable_applied, worker_applied);
        }
    };
    var hook_ctx = HookContext{};
    db.indexes.repair.shadow_hook = .{
        .ptr = &hook_ctx,
        .after_snapshot_build = Hook.afterSnapshotBuild,
        .after_clean_checkpoint = Hook.afterCleanCheckpoint,
    };
    defer db.indexes.repair.shadow_hook = null;

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .full_text,
        .index_name = "ft_v1",
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expect(hook_ctx.fired);
    try std.testing.expect(hook_ctx.clean_checkpoint_fired);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expect(!repair.debt_remaining);
    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "ft_v1");
    try std.testing.expectEqual(db_mod.apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expect(checkpoint.applied_sequence > hook_ctx.observed_floor);

    var alpha = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer alpha.deinit();
    try std.testing.expectEqual(@as(u32, 1), alpha.total_hits);
    try std.testing.expectEqualStrings("doc:c", alpha.hits[0].id);

    var beta = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "beta" } },
    });
    defer beta.deinit();
    try std.testing.expectEqual(@as(u32, 0), beta.total_hits);

    var gamma = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "gamma" } },
    });
    defer gamma.deinit();
    try std.testing.expectEqual(@as(u32, 1), gamma.total_hits);
    try std.testing.expectEqualStrings("doc:b", gamma.hits[0].id);
}

test "db index repair serializes duplicate repairs for one index" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });
    try std.testing.expect(try db.indexes.repair.beginLease("graph_v1"));
    defer db.indexes.repair.endLease("graph_v1");

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .graph,
        .index_name = "graph_v1",
        .limit = 1,
        .force = true,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.in_progress);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);
    try std.testing.expectEqual(@as(u64, 1), repair.unresolved);
    try std.testing.expect(repair.debt_remaining);
    try std.testing.expectEqual(@as(u64, 0), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_degraded_after);
}

test "db index repair streams graph artifact rebuild in batches" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{ .name = "graph_stream", .kind = .graph, .config_json = "{}" });
    try db.addIndex(.{ .name = "graph_other", .kind = .graph, .config_json = "{}" });

    const total_edges = db_mod.graph_repair_rebuild_batch_size + 3;
    var writes = std.ArrayListUnmanaged(db_mod.docstore_mod.KVPair).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        writes.deinit(alloc);
    }
    for (0..total_edges) |i| {
        const source = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{i});
        defer alloc.free(source);
        const target = try std.fmt.allocPrint(alloc, "target:{d:0>5}", .{i});
        defer alloc.free(target);
        const key = try db_mod.internal_keys.graphEdgeArtifactKeyAlloc(alloc, source, "graph_stream", "links", target);
        errdefer alloc.free(key);
        const value = try db_mod.enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, db.core.index_manager.graphIndex("graph_stream").?.config.coverage_generation, 1.0, 0, 0, "");
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = key, .value = value });
    }
    {
        const other_key = try db_mod.internal_keys.graphEdgeArtifactKeyAlloc(alloc, "other:a", "graph_other", "links", "other:b");
        errdefer alloc.free(other_key);
        const other_value = try db_mod.enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, db.core.index_manager.graphIndex("graph_other").?.config.coverage_generation, 1.0, 0, 0, "");
        errdefer alloc.free(other_value);
        // After append the list owns both allocations, including on failure.
        try writes.append(alloc, .{ .key = other_key, .value = other_value });
    }
    try db.core.store.putBatch(writes.items, &.{});

    // The restore/split entry point uses the same bounded streaming path as
    // online repair; it must neither materialize the complete artifact keyspace
    // nor rescan it for every configured graph index.
    db_mod.test_graph_repair_stream_flushes.store(0, .monotonic);
    db_mod.test_graph_repair_stream_scans.store(0, .monotonic);
    try db.rebuildGraphIndexesForTargetCoverage(alloc);
    try std.testing.expectEqual(@as(u64, 2), db_mod.test_graph_repair_stream_flushes.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), db_mod.test_graph_repair_stream_scans.load(.monotonic));

    const other_edges = try db.getEdges(alloc, "graph_other", "other:a", "links", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, other_edges);
    try std.testing.expectEqual(@as(usize, 1), other_edges.len);
    try std.testing.expectEqualStrings("other:b", other_edges[0].target);

    db_mod.test_graph_repair_stream_flushes.store(0, .monotonic);
    // Force an activation yield independently of host speed: one millisecond
    // cannot cover the five-millisecond publication reserve. The completed
    // snapshot work must still be reported and remain reusable after reopen.
    var repair = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .artifact_kind = .graph,
        .index_name = "graph_stream",
        .limit = 1,
        .force = true,
    }, .{ .max_activation_pause_ms = 1 });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, @intCast(total_edges)), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 1), repair.in_progress);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);
    try std.testing.expect(repair.debt_remaining);
    try std.testing.expectEqual(@as(u64, 2), db_mod.test_graph_repair_stream_flushes.load(.monotonic));

    const repair_id = (try db.indexes.repair.idForIndex(alloc, "graph_stream")) orelse return error.TestUnexpectedResult;
    var pending = try db.indexes.repair.loadEntryById(alloc, repair_id);
    defer pending.deinit(alloc);
    try std.testing.expectEqual(@as(u64, total_edges), pending.intent.build_reprocessed);
    try std.testing.expectEqual(@as(u32, 0), pending.intent.failure_streak);
    try std.testing.expectEqual(@as(u64, 0), pending.intent.next_retry_at_ms);
    try std.testing.expect(pending.intent.last_error == null);
    try std.testing.expect(pending.intent.candidate_relative_path != null);

    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{});
    db_mod.test_graph_repair_stream_flushes.store(0, .monotonic);
    const resumed = try db.indexes.repair.advanceIntent(alloc, repair_id, db_mod.repair_completion_test_options);
    try std.testing.expect(resumed.repaired);
    try std.testing.expectEqual(@as(u64, 1), resumed.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 0), resumed.documents_reprocessed);
    try std.testing.expectEqual(@as(u64, 0), db_mod.test_graph_repair_stream_flushes.load(.monotonic));
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));
    try std.testing.expect(try db.core.index_manager.isRepairCandidateActive("graph_stream", pending.intent.candidate_relative_path.?));

    const last_source = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{total_edges - 1});
    defer alloc.free(last_source);
    const last_target = try std.fmt.allocPrint(alloc, "target:{d:0>5}", .{total_edges - 1});
    defer alloc.free(last_target);
    const edges = try db.getEdges(alloc, "graph_stream", last_source, "links", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings(last_target, edges[0].target);

    // Reuse the large artifact fixture to verify that split retirement stays
    // page bounded even when every scanned row is a graph artifact.
    db_mod.test_graph_split_retire_flushes.store(0, .monotonic);
    const synthetic_split: db_mod.shard_mod.SplitState = .{
        .phase = .prepare,
        .split_key = "doc:",
        .new_shard_id = 2,
        .started_at = 1,
        .original_range_end = "doc;",
    };
    db_mod.test_graph_split_retire_abort_after_flush.store(true, .monotonic);
    defer db_mod.test_graph_split_retire_abort_after_flush.store(false, .monotonic);
    try std.testing.expectError(error.TestInjectedBackfillFailure, db_mod.retireSplitEntitySourcedEdgesLocked(&db, synthetic_split));
    try std.testing.expectEqual(@as(u64, 1), db_mod.test_graph_split_retire_flushes.load(.monotonic));
    try db_mod.retireSplitEntitySourcedEdgesLocked(&db, synthetic_split);
    try std.testing.expectEqual(@as(u64, 2), db_mod.test_graph_split_retire_flushes.load(.monotonic));
    const final_cursor = try db.core.store.get(alloc, db_mod.graph_split_retire_cursor_key);
    defer alloc.free(final_cursor);
    try std.testing.expect(final_cursor.len > 0);
}

test "db managed dense delete quiesces rate-limited enrichment and recreates cleanly" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var gated = db_mod.GateDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
        },
    });
    defer db.close();

    const cfg: db_mod.types.IndexConfig = .{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    };
    try db.addIndex(cfg);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta architecture notes\"}" },
            .{ .key = "doc:c", .value = "{\"body\":\"gamma implementation details\"}" },
        },
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 200 and gated.snapshot().blocked_requests == 0) : (attempts += 1) {
        db_mod.sleepNs(10 * std.time.ns_per_ms);
    }
    try std.testing.expect(gated.snapshot().blocked_requests > 0);

    // One coalesced issue may accumulate more terminal-failure history than a
    // single retirement page during a prolonged provider outage. Admission
    // must remain fenced until all of it is gone, rather than shifting cleanup
    // and fail-closed retries onto the replacement generation's first writes.
    const retired_issue: db_mod.types.ArtifactRepairIssue = .{
        .artifact_kind = .embedding,
        .index_name = cfg.name,
        .doc_key = "doc:retired",
        .artifact_name = cfg.name,
        .artifact_key = "retired-generation-artifact",
        .reason = .enrichment_failed,
    };
    for (0..db_mod.enrichment_terminal_failure_retirement_page_size + 7) |i| {
        var sequenced = retired_issue;
        sequenced.sequence = 10_000 + @as(u64, @intCast(i));
        try db.recordArtifactRepairIssue(alloc, sequenced);
    }
    const retired_issue_key = try db_mod.artifactRepairIssueKeyForIssueAlloc(alloc, retired_issue);
    defer alloc.free(retired_issue_key);
    const retired_marker_prefix = try db_mod.internal_keys.enrichmentTerminalFailureIssuePrefixAlloc(alloc, retired_issue_key);
    defer alloc.free(retired_marker_prefix);
    {
        const marker_rows = try db.core.store.scanPrefixPage(alloc, retired_marker_prefix, null, 1);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, marker_rows);
        try std.testing.expectEqual(@as(usize, 1), marker_rows.len);
    }
    const retired_generation_key = try db_mod.internal_keys.enrichmentTerminalFailureGenerationKeyAlloc(alloc, retired_issue_key);
    defer alloc.free(retired_generation_key);
    const retired_completion_key = try db.artifactRepairCompletionKeyForIssueAlloc(alloc, retired_issue);
    defer alloc.free(retired_completion_key);

    const delete_started_ns = db_mod.monotonicTimeNs();
    try std.testing.expect(try db.deleteIndex("semantic_idx"));
    try std.testing.expect(db_mod.monotonicTimeNs() - delete_started_ns < 2 * std.time.ns_per_s);

    gated.allowAll();
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);
    {
        const issues = try db.listArtifactRepairIssues(alloc, .embedding, cfg.name, 0);
        defer db_mod.types.freeArtifactRepairIssues(alloc, issues);
        try std.testing.expectEqual(@as(usize, 0), issues.len);
        const marker_rows = try db.core.store.scanPrefixPage(alloc, retired_marker_prefix, null, 1);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, marker_rows);
        try std.testing.expectEqual(@as(usize, 0), marker_rows.len);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, retired_generation_key));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, retired_completion_key));
    }
    try db.addIndex(cfg);
    try db.runUntilIdle();

    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
    try std.testing.expect(!stats.enrichment.worker_failed);
    try std.testing.expectEqual(@as(u64, 3), stats.indexes[0].doc_count);
    try std.testing.expect(stats.indexes[0].replay_applied_sequence >= stats.indexes[0].replay_target_sequence);
    try std.testing.expect(stats.indexes[0].repair_summary_ready);
    try std.testing.expect(!stats.indexes[0].repair_degraded);
    try std.testing.expectEqual(@as(u64, 0), stats.indexes[0].repair_issue_count);
}

test "db open quarantines dense index with unsupported artifact version" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const dense_cfg: db_mod.types.IndexConfig = .{
        .name = "dv_quarantine",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dv_quarantine\"}}",
    };

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    const open_options: db_mod.OpenOptions = .{
        .enrichment = .{
            .owner_id = "quarantine-worker",
            .dense_embedder = deterministic.interface(),
        },
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), open_options);
        defer db.close();

        try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
        try db.addIndex(dense_cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        // Persist an HBC metadata record with a version this build does not
        // support, simulating an artifact written by an incompatible build.
        const entry = db.core.denseIndex("dv_quarantine") orelse return error.TestUnexpectedResult;
        try entry.index.setMetadataVersionForTest(99);
    }

    var db = try DB.open(alloc, std.mem.span(path), open_options);
    defer db.close();

    // The broken dense index is quarantined: absent from the runtime, its
    // load error recorded, and the rest of the table fully usable.
    try std.testing.expect(db.core.denseIndex("dv_quarantine") == null);
    const recorded = db.core.index_manager.loadFailure("dv_quarantine") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("UnsupportedVersion", recorded);

    var ft_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "alpha" } },
    });
    defer ft_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), ft_result.total_hits);

    {
        const stats = try db.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, stats);
        var found = false;
        for (stats.indexes) |item| {
            if (!std.mem.eql(u8, item.name, "dv_quarantine")) continue;
            found = true;
            const load_error = item.load_error orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualStrings("UnsupportedVersion", load_error);
            try std.testing.expect(!item.backfill_active);
        }
        try std.testing.expect(found);
    }

    // Queries that explicitly reference the quarantined index get a distinct
    // error instead of IndexNotFound or a stall.
    const query_vec = [_]f32{ 0.1, 0.2, 0.3 };
    try std.testing.expectError(error.IndexUnavailable, db.search(alloc, .{
        .index_name = "dv_quarantine",
        .dense = .{
            .vector = &query_vec,
            .k = 1,
        },
        .limit = 1,
    }));

    // Drop + recreate recovers and clears the recorded failure.
    try std.testing.expect(try db.deleteIndex("dv_quarantine"));
    try std.testing.expect(db.core.index_manager.loadFailure("dv_quarantine") == null);
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);
    try db.addIndex(dense_cfg);
    try db.runUntilIdle();
    try std.testing.expect(db.core.denseIndex("dv_quarantine") != null);

    const recovered_vec = try deterministic.interface().embedDense(alloc, "dv_quarantine", "alpha concept overview", 3);
    defer alloc.free(recovered_vec);
    var recovered = try db_mod.waitForSearchResult(alloc, &db, .{
        .index_name = "dv_quarantine",
        .dense = .{
            .vector = recovered_vec,
            .k = 1,
        },
        .limit = 1,
    }, 1);
    defer recovered.deinit();
    try std.testing.expect(recovered.total_hits >= 1);
}

test "db drops quarantined dense index after persisted index directory corruption" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const path_slice = std.mem.span(path);

    const dense_cfg: db_mod.types.IndexConfig = .{
        .name = "dv_corrupt",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dv_corrupt\"}}",
    };

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    const open_options: db_mod.OpenOptions = .{
        .enrichment = .{
            .owner_id = "corrupt-worker",
            .dense_embedder = deterministic.interface(),
        },
    };

    {
        var db = try DB.open(alloc, path_slice, open_options);
        defer db.close();

        try db.addIndex(dense_cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"alpha concept overview\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();
        try std.testing.expect(db.core.denseIndex("dv_corrupt") != null);
    }

    const index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dv_corrupt", .{path_slice});
    defer alloc.free(index_path);
    try std.testing.expect((try db_mod.corruptNonEmptyFilesUnderDir(alloc, index_path)) > 0);

    var db = try DB.open(alloc, path_slice, open_options);
    defer db.close();

    try std.testing.expect(db.core.denseIndex("dv_corrupt") == null);
    _ = db.core.index_manager.loadFailure("dv_corrupt") orelse return error.TestUnexpectedResult;

    try std.testing.expect(try db.deleteIndex("dv_corrupt"));
    try std.testing.expect(db.core.index_manager.loadFailure("dv_corrupt") == null);
    try std.testing.expect(db.core.index_manager.get("dv_corrupt") == null);
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);

    var io_impl = db_mod.threadedIo();
    defer io_impl.deinit();
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().statFile(io_impl.io(), index_path, .{}));
}

test "db managed dense enrichment delete recreate recovers after corrupt artifact" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var counting = db_mod.CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    const cfg: db_mod.types.IndexConfig = .{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    };

    try db.addIndex(cfg);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta architecture notes\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    {
        const stats = try db.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(?u64, 2), stats.indexes[0].doc_count);
    }

    const artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "semantic_idx");
    defer alloc.free(artifact_key);
    try db.core.store.put(artifact_key, "bad-artifact");

    try std.testing.expect(try db.deleteIndex("semantic_idx"));
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);
    try db.addIndex(cfg);
    try db.runUntilIdle();

    {
        const stats = try db.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(?u64, 2), stats.indexes[0].doc_count);
        try std.testing.expect(stats.indexes[0].replay_applied_sequence >= 2);
        try std.testing.expectEqual(stats.indexes[0].replay_target_sequence, stats.indexes[0].replay_applied_sequence);
    }

    const query_vec = try counting.interface().embedDense(alloc, "semantic_idx", "alpha concept overview", 3);
    defer alloc.free(query_vec);
    var result = try db.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db managed dense enrichment delete recreate recovers after corrupt artifact across reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var counting = db_mod.CountingDenseEmbedder{};
    const cfg: db_mod.types.IndexConfig = .{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = counting.interface(),
            },
        });
        defer db.close();

        try db.addIndex(cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta architecture notes\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "semantic_idx");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer reopened.close();

    try std.testing.expect(try reopened.deleteIndex("semantic_idx"));
    reopened.backend_runtime.durable_jobs.drainOwner(reopened.indexes.repair.cleanup_owner_id);
    try reopened.addIndex(cfg);
    try reopened.runUntilIdle();

    {
        const stats = try reopened.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(?u64, 2), stats.indexes[0].doc_count);
        try std.testing.expect(stats.indexes[0].replay_applied_sequence >= 2);
        try std.testing.expectEqual(stats.indexes[0].replay_target_sequence, stats.indexes[0].replay_applied_sequence);
    }

    const query_vec = try counting.interface().embedDense(alloc, "semantic_idx", "alpha concept overview", 3);
    defer alloc.free(query_vec);
    var result = try reopened.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db recreated managed dense index converges replay and irrelevant resolver stages" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var counting = db_mod.CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    const config: db_mod.types.IndexConfig = .{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    };
    try db.addIndex(config);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const calls_before_recreate = counting.calls;
    try std.testing.expect(calls_before_recreate > 0);
    try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    try db_mod.noteTargetAdvanceDebtLogged(db.async_context, "dv_v1", db_mod.monotonicTimeNs());
    try std.testing.expect(try db.deleteIndex("dv_v1"));
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);
    try db.addIndex(config);
    try db.runUntilIdle();

    try std.testing.expect(counting.calls >= calls_before_recreate);
    try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    const replay = for (stats.indexes) |item| {
        if (std.mem.eql(u8, item.name, "dv_v1")) break item;
    } else return error.TestUnexpectedResult;
    try std.testing.expect(replay.replay_applied_sequence >= replay.replay_target_sequence);
    try std.testing.expect(!stats.resolution.enabled);
    try std.testing.expect(!stats.resolution.catch_up_required);
    try std.testing.expect(!stats.promotion.enabled);
    try std.testing.expect(!stats.promotion.catch_up_required);
}

test "idle generated coverage gap becomes durable paged recovery debt" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "document_text",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });
    try db.addIndex(.{
        .name = "semantic",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    // A pre-cardinality corpus must not force startup to scan every source
    // document. Unknown cardinality conservatively seeds the same bounded,
    // resumable recovery intent.
    try db.core.store.delete(&db_mod.internal_keys.range_document_count_key);

    // Model the post-watermark crash window: no runnable enrichment owner is
    // attached, while the current generation still has zero terminal source
    // outcomes for one durable primary document.
    try std.testing.expect(try db.generatedCoverageReplayNeeded(alloc));
    // An incomplete generated projection is ordinary initial-build work, not
    // generic dense artifact repair. The recovery owner below persists the
    // exact job class when no live producer owns the gap.
    try std.testing.expect(!(try db.denseArtifactRebuildMaintenanceNeeded(alloc)));
    // Text and vectors maintain independent source-completion receipts. Both
    // missing receipts need durable recovery when the source count is unknown.
    try std.testing.expect(try db.core.indexRequiresEnrichmentReplay("document_text"));
    try std.testing.expectEqual(@as(usize, 2), try db.ensureGeneratedCoverageRecoveryIntents(alloc));
    const text_repair_id = (try db.indexes.repair.idForIndex(alloc, "document_text")) orelse return error.TestUnexpectedResult;
    var text_entry = try db.indexes.repair.loadEntryById(alloc, text_repair_id);
    defer text_entry.deinit(alloc);
    try std.testing.expectEqual(db_mod.index_repair_state.WorkClass.initial_build, text_entry.intent.work_class);
    try std.testing.expectEqual(db_mod.index_repair_state.SourceReplayState.pending, text_entry.intent.source_replay_state);
    try std.testing.expect(try db.generatedCoverageRecoveryOwnedByInitialBuild(alloc));
    // The exact initial-build job owns this temporary zero-vector state. The
    // generic artifact planner must not enqueue a duplicate repair job.
    try std.testing.expect(!(try db.denseArtifactRebuildMaintenanceNeeded(alloc)));
    const repair_id = (try db.indexes.repair.idForIndex(alloc, "semantic")) orelse return error.TestUnexpectedResult;
    var entry = try db.indexes.repair.loadEntryById(alloc, repair_id);
    defer entry.deinit(alloc);
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.replay_artifact_unavailable, entry.intent.trigger);
    try std.testing.expectEqual(db_mod.index_repair_state.WorkClass.initial_build, entry.intent.work_class);
    try std.testing.expectEqual(db_mod.index_repair_state.SourceReplayState.pending, entry.intent.source_replay_state);
    try std.testing.expectEqual(db_mod.index_repair_state.Phase.detected, entry.intent.phase);
    // This initial generation has no published outcomes to serve yet.
    try std.testing.expect(db.core.index_manager.repairUnavailable("semantic"));
}

test "managed dense physical migration is online and uses durable repair intent" {
    const Gate = struct {
        permitted: bool = false,

        pub fn read(ptr: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            return self.permitted;
        }
    };
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var gate = Gate{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .index_backends = .{ .dense_native_migration_policy_source = .{
            .ptr = &gate,
            .authority_permitted = Gate.read,
        } },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });

    try std.testing.expect(!(try db.indexGenerationRepairRequired(alloc, "dense_idx")));
    gate.permitted = true;
    try std.testing.expect(try db.indexGenerationRepairRequired(alloc, "dense_idx"));
    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const healthy_checkpoint = try db.core.loadProjectionCheckpoint(alloc, "dense_idx");
    try db.core.saveProjectionCheckpoint("dense_idx", .{
        .applied_sequence = healthy_checkpoint.applied_sequence,
        .status = .repair_required,
        .generation = healthy_checkpoint.generation,
        .config_hash = healthy_checkpoint.config_hash,
    });
    const correctness_classification =
        (try db.classifyCurrentDenseGenerationRepair(alloc, cfg.*)) orelse
        return error.TestUnexpectedResult;
    try std.testing.expectEqual(
        db_mod.index_repair_state.Trigger.projection_generation_invalid,
        correctness_classification.trigger,
    );
    try db.core.saveProjectionCheckpoint("dense_idx", healthy_checkpoint);
    const classification = (try db.classifyCurrentDenseGenerationRepair(alloc, cfg.*)) orelse
        return error.TestUnexpectedResult;
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.storage_format_migration, classification.trigger);
    const repair_id = try db.indexes.repair.ensureAutomaticDenseGenerationIntent(
        alloc,
        cfg.*,
        classification.trigger,
        classification.last_error,
    );
    try std.testing.expect(!db.core.index_manager.repairUnavailable("dense_idx"));
    var repair = try db.indexes.repair.loadEntryById(alloc, repair_id);
    defer repair.deinit(alloc);
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.storage_format_migration, repair.intent.trigger);
    try std.testing.expectEqual(db_mod.IndexRepairAdmission.serviceable, db_mod.RepairIndexes.repairAdmission(repair.intent));

    // Candidate vector-block publication is table-wide runtime work. It must
    // not project onto the readiness of the still-authoritative v1 index.
    db.async_context.dense_admission.finalizing.store(true, .release);
    defer db.async_context.dense_admission.finalizing.store(false, .release);
    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    const dense_stats = for (stats.indexes) |item| {
        if (std.mem.eql(u8, item.name, "dense_idx")) break item;
    } else return error.TestUnexpectedResult;
    try std.testing.expect(stats.async_indexing.dense_projection_finalizing);
    try std.testing.expect(!dense_stats.dense_vector_projection_pending);
}

test "db replay blocks and preserves corrupt dense embedding artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const stored_key = try db_mod.internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");

        const derived_batch = db_mod.derived_types.DerivedBatch{
            .dense_embeddings = &.{
                .{
                    .index_name = "dv_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .vector = &[_]f32{ 1, 0 },
                },
            },
        };

        appended_sequence = try db_mod.appendDerivedBatchRecord(&db, derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer db_mod.types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.corrupt_embedding_artifact, issues[0].reason);
    try std.testing.expectEqual(.embedding, issues[0].artifact_kind);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("dv_v1", issues[0].artifact_name);
    const repair_id = (try reopened.indexes.repair.idForIndex(alloc, "dv_v1")) orelse
        return error.TestUnexpectedResult;
    var replay_repair = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
    defer replay_repair.deinit(alloc);
    try std.testing.expectEqual(
        db_mod.index_repair_state.Trigger.replay_artifact_unavailable,
        replay_repair.intent.trigger,
    );
    try std.testing.expect(!reopened.core.index_manager.repairUnavailable("dv_v1"));

    // A terminal replacement failure is actionable completeness debt, but it
    // cannot revoke the explicitly retained active generation. Status and the
    // API query coordinator consume this same scoped serviceability proof.
    try reopened.indexes.repair.updateIntent(alloc, repair_id, .{
        .phase = .terminal,
        .last_error = "IndexRepairConfigurationChanged",
        .replace_last_error = true,
    });
    const terminal_stats = try reopened.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, terminal_stats);
    var observed_terminal_repair = false;
    for (terminal_stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "dv_v1")) continue;
        observed_terminal_repair = true;
        try std.testing.expectEqual(db_mod.types.IndexRepairStatus.failed, index_stats.index_repair_status.?);
        try std.testing.expect(index_stats.index_repair_action_required);
        try std.testing.expect(index_stats.index_repair_active_generation_serviceable);
        try std.testing.expect(index_stats.serving_snapshot_ready);
    }
    try std.testing.expect(observed_terminal_repair);
    try std.testing.expect(!reopened.core.index_manager.repairUnavailable("dv_v1"));

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);

    const artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_key);
    const artifact_value = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    try std.testing.expectEqualStrings("bad-artifact", artifact_value);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(@as(u64, 0), dense_applied);
    try std.testing.expect(appended_sequence > dense_applied);

    reopened.runDerivedUntil(appended_sequence) catch |err| switch (err) {
        error.ArtifactRepairRequired => {},
        else => return err,
    };
    const issues_after_replay = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer db_mod.types.freeEmbeddingArtifactRepairIssues(alloc, issues_after_replay);
    try std.testing.expectEqual(@as(usize, 1), issues_after_replay.len);
    try std.testing.expectEqual(issues[0].first_seen_ns, issues_after_replay[0].first_seen_ns);
    try std.testing.expectEqual(issues[0].attempts, issues_after_replay[0].attempts);
    try std.testing.expectEqualStrings(issues[0].last_error, issues_after_replay[0].last_error);

    const repair = try reopened.repairEmbeddingArtifactIssues(alloc, 10);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.failed);
    const issues_after_repair = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer db_mod.types.freeEmbeddingArtifactRepairIssues(alloc, issues_after_repair);
    try std.testing.expectEqual(@as(usize, 1), issues_after_repair.len);
    try std.testing.expectEqual(@as(u64, 1), issues_after_repair[0].attempts);
    try std.testing.expectEqualStrings("embedding_enrichment_unavailable", issues_after_repair[0].last_error);
}

test "db repeated replay preserves nonblocking dense artifact repair intent" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const stored_key = try db_mod.internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");
        _ = try db_mod.appendDerivedBatchRecord(&db, .{
            .dense_embeddings = &.{.{
                .index_name = "dv_v1",
                .doc_key = "doc:a",
                .artifact_key = artifact_key,
                .vector = &[_]f32{ 1, 0 },
            }},
        });
    }

    for (0..2) |_| {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();

        const repair_id = (try reopened.indexes.repair.idForIndex(alloc, "dv_v1")) orelse
            return error.TestUnexpectedResult;
        var replay_repair = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
        defer replay_repair.deinit(alloc);
        try std.testing.expectEqual(
            db_mod.index_repair_state.Trigger.replay_artifact_unavailable,
            replay_repair.intent.trigger,
        );
        try std.testing.expect(!reopened.core.index_manager.repairUnavailable("dv_v1"));

        var result = try reopened.search(alloc, .{
            .index_name = "dv_v1",
            .dense = .{ .vector = &[_]f32{ 1, 0 }, .k = 1 },
            .limit = 1,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 0), result.total_hits);
    }
}

test "db dense artifact surplus uses quarantined generation replacement" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );

    // Inject an index-only vector to model a stale key left by an interrupted
    // or defective replay. There is intentionally no matching source artifact
    // and therefore no target-counter increment.
    var ghost_vector = [_]f32{ 0, 1, 0 };
    try db.core.index_manager.denseIndex("dense_idx").?.index.insertVectorForTest(0xdead_beef, &ghost_vector);
    try std.testing.expectEqual(
        @as(u64, 2),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    try std.testing.expect(try db.hasPendingDenseArtifactRebuild(alloc));

    try std.testing.expect((try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc)) > 0);
    const first_repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse
        return error.TestUnexpectedResult;
    var first_state = try db.indexes.repair.loadState(alloc);
    try std.testing.expectEqual(
        db_mod.index_repair_state.Trigger.artifact_coverage_mismatch,
        first_state.entries.items[0].intent.trigger,
    );
    first_state.deinit(alloc);
    // A source-counter surplus proves that this published generation is not a
    // healthy rollback anchor. Keep its files intact for replacement, but fail
    // queries closed throughout reconstruction.
    try std.testing.expectEqual(
        @as(u64, 2),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    const blocked_stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, blocked_stats);
    var observed_blocked_snapshot = false;
    for (blocked_stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "dense_idx")) continue;
        observed_blocked_snapshot = true;
        try std.testing.expect(!index_stats.serving_snapshot_ready);
    }
    try std.testing.expect(observed_blocked_snapshot);
    try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    }));
    const QuarantinedGenerationHook = struct {
        fn afterSnapshot(_: *anyopaque, hooked_db: *DB, _: []const u8, _: u64) !void {
            try std.testing.expectEqual(
                @as(u64, 2),
                hooked_db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
            );
            try std.testing.expectError(error.IndexRebuilding, hooked_db.search(hooked_db.alloc, .{
                .index_name = "dense_idx",
                .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
                .limit = 1,
            }));
        }
    };
    var hook_context: u8 = 0;
    db.indexes.repair.shadow_hook = .{
        .ptr = &hook_context,
        .after_snapshot_build = QuarantinedGenerationHook.afterSnapshot,
    };
    const first_repair = try db.indexes.repair.advanceIntent(alloc, first_repair_id, .{});
    db.indexes.repair.shadow_hook = null;
    try std.testing.expect(first_repair.repaired);
    try std.testing.expectEqual(
        @as(u64, 1),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));

    var result = try db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0, 1, 0 }, .k = 2 } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    // The zero-target case must also reset a surplus, including after a crash
    // left a scan cursor belonging to the prior generation.
    try db.batch(.{
        .deletes = &.{"doc:a"},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(
        @as(?u64, 0),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );
    try db.core.index_manager.denseIndex("dense_idx").?.index.insertVectorForTest(0xbeef_dead, &ghost_vector);
    const rebuild_state_path = try db.denseIndexRebuildStatePathAlloc(alloc, "dense_idx");
    defer alloc.free(rebuild_state_path);
    const rebuild_state = db_mod.backfill_state_mod.RebuildState.init(rebuild_state_path);
    try rebuild_state.update("stale-generation-cursor");
    try std.testing.expect(try db.hasPendingDenseArtifactRebuild(alloc));
    _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    const zero_repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse
        return error.TestUnexpectedResult;
    try std.testing.expectEqual(
        @as(u64, 1),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    const zero_repair = try db.indexes.repair.advanceIntent(alloc, zero_repair_id, .{});
    try std.testing.expect(zero_repair.repaired);
    try std.testing.expectEqual(
        @as(u64, 0),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));
}

test "db dense shadow activation rejects surplus candidate coverage" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
        },
        .sync_level = .full_index,
    });
    const active_pointer_before = try db.core.index_manager.captureActiveIndexRootPointer("dense_idx");
    defer if (active_pointer_before) |pointer| alloc.free(pointer);

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .artifact_kind = .embedding,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    const repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse
        return error.TestUnexpectedResult;

    const CoverageHook = struct {
        fn afterSnapshot(_: *anyopaque, hooked_db: *DB, index_name: []const u8, _: u64) !void {
            const key = try DB.denseArtifactTargetCounterKeyAlloc(hooked_db.alloc, index_name);
            defer hooked_db.alloc.free(key);
            var reduced: [8]u8 = undefined;
            std.mem.writeInt(u64, &reduced, 1, .little);
            try hooked_db.core.store.put(key, &reduced);
        }
    };
    var hook_context: u8 = 0;
    db.indexes.repair.shadow_hook = .{
        .ptr = &hook_context,
        .after_snapshot_build = CoverageHook.afterSnapshot,
    };
    const outcome = try db.indexes.repair.advanceIntent(alloc, repair_id, .{});
    db.indexes.repair.shadow_hook = null;
    try std.testing.expect(outcome.attempted);
    try std.testing.expect(outcome.deferred);
    try std.testing.expect(!outcome.terminal);
    try std.testing.expect(!outcome.repaired);
    try std.testing.expect(try db.indexes.repair.hasPendingIntents(alloc));

    // Restore the deliberately altered source metadata. The failed candidate
    // must not have replaced the previous healthy two-vector generation.
    const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_idx");
    defer alloc.free(counter_key);
    var restored: [8]u8 = undefined;
    std.mem.writeInt(u64, &restored, 2, .little);
    try db.core.store.put(counter_key, &restored);
    try std.testing.expectEqual(
        @as(u64, 2),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    const active_pointer_after = try db.core.index_manager.captureActiveIndexRootPointer("dense_idx");
    defer if (active_pointer_after) |pointer| alloc.free(pointer);
    try std.testing.expectEqual(active_pointer_before == null, active_pointer_after == null);
    if (active_pointer_before) |before| {
        try std.testing.expectEqualStrings(before, active_pointer_after.?);
    }

    // Coverage failure makes this snapshot candidate insufficient. The next
    // turn discards it; the following turn rebuilds from authoritative source.
    const reset = try db.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(reset.attempted);
    try std.testing.expect(reset.deferred);
    var reset_entry = try db.indexes.repair.loadEntryById(alloc, repair_id);
    try std.testing.expectEqual(db_mod.index_repair_state.Phase.preflight, reset_entry.intent.phase);
    try std.testing.expect(reset_entry.intent.candidate_relative_path == null);
    try std.testing.expect(reset_entry.intent.last_error == null);
    try std.testing.expectEqual(@as(u32, 0), reset_entry.intent.failure_streak);
    reset_entry.deinit(alloc);

    const recovered = try db.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(recovered.attempted);
    try std.testing.expect(recovered.repaired);
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));
}

test "db repair activation budget yields without failure backoff and resumes its candidate" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("repair-activation-yield");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });
    const repair_id = (try db.admitManagedIndex(.{
        .name = "full_text_index_v0",
        .kind = .full_text,
        .config_json = "{}",
    })) orelse return error.TestUnexpectedResult;
    var yielded = false;
    for (0..8) |_| {
        // One millisecond cannot satisfy the five-millisecond publication
        // reserve, independently of host speed. No wall-clock sleep is needed
        // to force the production activation-budget path.
        const step = try db.indexes.repair.advanceIntent(alloc, repair_id, .{ .max_activation_pause_ms = 1 });
        try std.testing.expect(!step.repaired and !step.terminal);
        var entry = try db.indexes.repair.loadEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        try std.testing.expectEqual(@as(u32, 0), entry.intent.failure_streak);
        try std.testing.expectEqual(@as(u64, 0), entry.intent.next_retry_at_ms);
        try std.testing.expect(entry.intent.last_error == null);
        if (step.busy) {
            yielded = true;
            break;
        }
    }
    try std.testing.expect(yielded);
    var pending = try db.indexes.repair.loadEntryById(alloc, repair_id);
    defer pending.deinit(alloc);
    try std.testing.expect(pending.intent.candidate_relative_path != null);
    const resumed = try db.indexes.repair.advanceIntent(alloc, repair_id, .{ .max_activation_pause_ms = 5_000 });
    try std.testing.expect(resumed.attempted and resumed.repaired);
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));
    try std.testing.expect(try db.core.index_manager.isRepairCandidateActive("full_text_index_v0", pending.intent.candidate_relative_path.?));
    var result = try db.search(alloc, .{ .index_name = "full_text_index_v0", .query = .{ .match = .{ .field = "_all", .text = "alpha" } }, .limit = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db automatic dense repair bootstraps missing coverage metadata" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
        const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_idx");
        defer alloc.free(counter_key);
        try db.core.store.delete(counter_key);
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeSentinel(u8, dense_path, 0);
    defer alloc.free(dense_path_z);
    {
        var hbc = try db_mod.hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 2,
            .storage_backend = .lsm,
        }, .{});
        try hbc.markPublishedGenerationIncompleteForTest();
        hbc.close();
    }

    var reopened = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    _ = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    const repair_id = (try reopened.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    const result = try reopened.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(result.attempted);
    try std.testing.expect(!result.terminal);
    try std.testing.expect(result.repaired);
    try std.testing.expect(!try reopened.indexes.repair.hasPendingIntents(alloc));
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, reopened.core.store, "dense_idx"),
    );
    try std.testing.expect(try DB.loadDenseArtifactCounterBootstrap(alloc, reopened.core.store, "dense_idx") == null);

    try std.testing.expect(reopened.core.index_manager.loadFailure("dense_idx") == null);

    var search_result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer search_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), search_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", search_result.hits[0].id);
}

test "db quarantined dense bootstrap tracks concurrent insert update and delete" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const cfg: db_mod.types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };

    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(cfg);
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" }},
            .sync_level = .full_index,
        });
        const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, cfg.name);
        defer alloc.free(counter_key);
        try db.core.store.delete(counter_key);
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}", .{ std.mem.span(path), cfg.name });
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeSentinel(u8, dense_path, 0);
    defer alloc.free(dense_path_z);
    {
        var hbc = try db_mod.hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{});
        try hbc.markPublishedGenerationIncompleteForTest();
        hbc.close();
    }

    var reopened = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    // This is an explicitly legacy-v1 root. Its injected incomplete
    // publication is represented by the durable bulk marker; native-v2 would
    // fail with MissingPostingCheckpoint after CURRENT is removed.
    try std.testing.expectEqualStrings("IncompleteBulkPublish", reopened.core.index_manager.loadFailure(cfg.name).?);
    try std.testing.expect(reopened.core.index_manager.denseIndex(cfg.name) == null);
    _ = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    const repair_id = (try reopened.indexes.repair.idForIndex(alloc, cfg.name)) orelse return error.TestUnexpectedResult;

    var bootstrap_snapshot = try reopened.beginDenseArtifactCounterBootstrapSnapshot(alloc, cfg.name, repair_id);
    defer bootstrap_snapshot.deinit();

    try reopened.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" }},
        .sync_level = .write,
    });
    try std.testing.expectEqual(
        @as(i64, 1),
        (try DB.loadDenseArtifactCounterBootstrap(alloc, reopened.core.store, cfg.name)).?.delta,
    );
    try reopened.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha-2\",\"_embeddings\":{\"dense_idx\":[0,0,1]}}" }},
        .sync_level = .write,
    });
    try std.testing.expectEqual(
        @as(i64, 1),
        (try DB.loadDenseArtifactCounterBootstrap(alloc, reopened.core.store, cfg.name)).?.delta,
    );
    try reopened.batch(.{
        .deletes = &.{"doc:a"},
        .sync_level = .write,
    });
    try std.testing.expectEqual(
        @as(i64, 0),
        (try DB.loadDenseArtifactCounterBootstrap(alloc, reopened.core.store, cfg.name)).?.delta,
    );

    const snapshot_count = try reopened.countDenseArtifactsForConfigFromReadTxn(
        alloc,
        cfg,
        &bootstrap_snapshot.txn,
        null,
    );
    try std.testing.expectEqual(@as(u64, 1), snapshot_count);
    try reopened.finishDenseArtifactCounterBootstrap(
        alloc,
        cfg.name,
        repair_id,
        bootstrap_snapshot.attempt_id,
        snapshot_count,
    );
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, reopened.core.store, cfg.name),
    );

    const repaired = try reopened.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(repaired.repaired);
    var result = try reopened.search(alloc, .{
        .index_name = cfg.name,
        .query = .{ .dense_knn = .{ .vector = &.{ 0, 1, 0 }, .k = 2 } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
}

test "db corrupt repair checkpoint preserves primary availability and fails indexes closed" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var repair_checkpoint_path: []u8 = undefined;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
        repair_checkpoint_path = try alloc.dupe(u8, db.core.index_repair_checkpoint.?.path);
    }
    defer alloc.free(repair_checkpoint_path);
    try db_mod.writeRawProjectionCheckpointSidecarForTest(repair_checkpoint_path, "corrupt-index-repair-state");

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expect(reopened.async_context.index_repair_state_corrupt.load(.acquire));
    try std.testing.expect(reopened.async_context.index_repair_replay_pinned.load(.acquire));
    try std.testing.expectError(error.InvalidIndexRepairState, reopened.indexes.repair.hasPendingIntents(alloc));
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    }));
    const stats = try reopened.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.repair_degraded);
    try std.testing.expect(stats.indexes[0].repair_degraded);
    try std.testing.expectEqualStrings("corrupt_local_repair_state", stats.indexes[0].index_repair_trigger);

    const primary = (try reopened.get(alloc, "doc:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(primary);
    try reopened.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
        .sync_level = .write,
    });
    const appended = (try reopened.get(alloc, "doc:b")) orelse return error.TestUnexpectedResult;
    defer alloc.free(appended);
}

test "db index repair rebuilds dense index with missing native publication" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            },
            .sync_level = .full_index,
        });
    }

    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    const dense_index_path_z = try alloc.dupeSentinel(u8, dense_index_path, 0);
    defer alloc.free(dense_index_path_z);
    {
        var hbc = try db_mod.hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_index_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{});
        try hbc.markPublishedGenerationIncompleteForTest();
        hbc.close();
    }

    var reopened = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const recorded = reopened.core.index_manager.loadFailure("dense_idx") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("IncompleteBulkPublish", recorded);
    const discovery = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    try std.testing.expectEqual(@as(usize, 1), discovery.discovered);
    try std.testing.expect(try reopened.indexes.repair.hasPendingIntents(alloc));
    const duplicate_discovery = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    try std.testing.expectEqual(@as(usize, 0), duplicate_discovery.discovered);
    try std.testing.expectEqual(@as(usize, 0), duplicate_discovery.already_pending);
    try std.testing.expect(duplicate_discovery.automatic_sweep_complete);
    var discovered_state = try reopened.indexes.repair.loadState(alloc);
    const repair_id = discovered_state.entries.items[0].intent.repair_id;
    try std.testing.expectEqual(db_mod.index_repair_state.Phase.detected, discovered_state.entries.items[0].intent.phase);
    try std.testing.expect(discovered_state.entries.items[0].pin == null);
    discovered_state.deinit(alloc);

    var pinned_snapshot = try reopened.indexes.repair.beginPinnedSnapshot(alloc, repair_id);
    const pinned_floor = pinned_snapshot.build_floor_sequence;
    pinned_snapshot.deinit();
    var pinned_state = try reopened.indexes.repair.loadState(alloc);
    try std.testing.expectEqual(@as(?u64, pinned_floor), pinned_state.minimumRetainAfterSequence());
    pinned_state.deinit(alloc);

    // Discovery and pin acquisition must not delete or reopen the poisoned
    // root merely to make rebuilding convenient.
    try std.testing.expectError(
        error.IncompleteBulkPublish,
        db_mod.hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_index_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{}),
    );
    {
        const status_key = try DB.indexStatusKeyAlloc(alloc, "dense_idx");
        defer alloc.free(status_key);
        var stale_status: [DB.index_status_encoded_len]u8 = undefined;
        DB.encodeIndexStatusSnapshot(.{
            .kind = .dense_vector,
            .doc_count = 99,
            .node_count = 99,
            .root_node = 99,
        }, &stale_status);
        try reopened.core.store.put(status_key, &stale_status);
    }
    {
        const stats = try reopened.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
    }

    const OwnershipFence = struct {
        lost: bool = false,

        fn current(ptr: *anyopaque) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return !self.lost;
        }

        fn afterSnapshot(ptr: *anyopaque, _: *DB, _: []const u8, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lost = true;
        }
    };
    var ownership_fence = OwnershipFence{};
    reopened.indexes.repair.shadow_hook = .{
        .ptr = &ownership_fence,
        .after_snapshot_build = OwnershipFence.afterSnapshot,
    };
    const ownership_lost = try reopened.indexes.repair.advanceIntent(alloc, repair_id, .{
        .owner_epoch = 17,
        .activation_check = .{
            .ptr = &ownership_fence,
            .is_current_owner = OwnershipFence.current,
        },
    });
    reopened.indexes.repair.shadow_hook = null;
    try std.testing.expect(ownership_lost.attempted);
    try std.testing.expect(ownership_lost.deferred);
    try std.testing.expect(!ownership_lost.repaired);
    try std.testing.expect(try reopened.indexes.repair.hasPendingIntents(alloc));
    var reserved_state = try reopened.indexes.repair.loadState(alloc);
    try std.testing.expect(reserved_state.entries.items[0].intent.estimated_candidate_bytes > 0);
    try std.testing.expectEqual(@as(u64, 17), reserved_state.entries.items[0].intent.owner_epoch);
    try std.testing.expect(
        reserved_state.entries.items[0].intent.planned_disk_bytes >=
            reserved_state.entries.items[0].intent.estimated_candidate_bytes,
    );
    reserved_state.deinit(alloc);
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    }));
    try std.testing.expectError(
        error.StaleIndexRepairControl,
        reopened.indexes.repair.pauseAutomatic(alloc, "dense_idx", repair_id + 1),
    );
    try std.testing.expect(try reopened.indexes.repair.resumeAutomatic(alloc, "dense_idx", null));

    const repair = try reopened.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(repair.attempted);
    try std.testing.expect(repair.repaired);
    // The first attempt reached a durable candidate before ownership loss;
    // retry reopens and catches up that generation instead of rescanning the
    // primary corpus.
    try std.testing.expectEqual(@as(u64, 0), repair.documents_reprocessed);
    try std.testing.expect(reopened.core.index_manager.loadFailure("dense_idx") == null);
    try std.testing.expect(!try reopened.indexes.repair.hasPendingIntents(alloc));
    var completed_state = try reopened.indexes.repair.loadState(alloc);
    defer completed_state.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), completed_state.entries.items.len);
    {
        const persisted_status = (try reopened.loadIndexStatusSnapshot(alloc, "dense_idx")).?;
        try std.testing.expectEqual(@as(u64, 2), persisted_status.doc_count);
    }

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);

    // The adopted shadow handle must be a normal active runtime, including
    // loader callbacks and subsequent foreground apply; it must not retain a
    // dependency on the destroyed shadow manager.
    try reopened.batch(.{
        .writes = &.{.{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0,0,1]}}" }},
        .sync_level = .full_index,
    });
    var after_adoption = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 0.0, 0.0, 1.0 },
            .k = 1,
        } },
        .limit = 1,
    });
    defer after_adoption.deinit();
    try std.testing.expectEqual(@as(u32, 1), after_adoption.total_hits);
    try std.testing.expectEqualStrings("doc:c", after_adoption.hits[0].id);
}

test "quarantine binding reconciliation serializes with terminal transition" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var dense_path: ?[]u8 = null;
    defer if (dense_path) |value| alloc.free(value);
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
        dense_path = try db.core.index_manager.activeIndexPath("dense_idx");
    }

    const dense_path_z = try alloc.dupeSentinel(u8, dense_path.?, 0);
    defer alloc.free(dense_path_z);
    {
        var hbc = try db_mod.hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 2,
            .storage_backend = .lsm,
        }, .{});
        try hbc.markPublishedGenerationIncompleteForTest();
        hbc.close();
    }

    var repair_id: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        _ = try db.discoverRecoverableStartupIndexFailures(alloc, 1);
        repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse
            return error.TestUnexpectedResult;
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const Context = struct {
        db: *DB,
        repair_id: u128,
        observed: std.atomic.Value(bool) = .init(false),
        terminal_attempted: std.atomic.Value(bool) = .init(false),
        terminal_error: ?anyerror = null,

        fn terminal(ptr: *@This()) void {
            while (!ptr.observed.load(.acquire)) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
            ptr.terminal_attempted.store(true, .release);
            ptr.db.indexes.repair.recordAttemptFailure(
                ptr.db.alloc,
                ptr.repair_id,
                "test_terminal",
                true,
            ) catch |err| {
                ptr.terminal_error = err;
            };
        }

        fn afterObservation(raw: *anyopaque, _: *DB, observed_repair_id: u128) !void {
            const ptr: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(ptr.repair_id, observed_repair_id);
            ptr.observed.store(true, .release);
            while (!ptr.terminal_attempted.load(.acquire)) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
            // The terminal thread is now waiting on repair-control ownership.
            // Returning lets discovery finish its pending binding transaction;
            // terminalization must then release that exact binding.
        }
    };
    var context = Context{ .db = &reopened, .repair_id = repair_id };
    var terminal_thread = try std.testing.io.concurrent(Context.terminal, .{&context});
    var terminal_thread_joined = false;
    defer if (!terminal_thread_joined) {
        context.observed.store(true, .release);
        terminal_thread.await(std.testing.io);
    };
    DB.test_index_repair_discovery_observation_hook = .{
        .ptr = &context,
        .after_observation = Context.afterObservation,
    };
    defer DB.test_index_repair_discovery_observation_hook = null;
    const discovery = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    terminal_thread.await(std.testing.io);
    terminal_thread_joined = true;
    try std.testing.expect(context.terminal_error == null);
    try std.testing.expectEqual(@as(usize, 1), discovery.already_pending);

    var terminal = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
    defer terminal.deinit(alloc);
    try std.testing.expectEqual(db_mod.index_repair_state.Phase.terminal, terminal.intent.phase);
    try std.testing.expect(
        reopened.core.index_manager.failedIndexLoadSnapshot("dense_idx").?.claim_owner_repair_id == null,
    );
}

test "db restart reconciles activated dense repair without rebuilding" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeSentinel(u8, dense_path, 0);
    defer alloc.free(dense_path_z);
    {
        var hbc = try db_mod.hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 2,
            .storage_backend = .lsm,
        }, .{});
        try hbc.markPublishedGenerationIncompleteForTest();
        hbc.close();
    }

    var repair_id: u128 = 0;
    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        _ = try db.discoverRecoverableStartupIndexFailures(alloc, 1);
        repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;

        const CrashHook = struct {
            fn afterSnapshot(_: *anyopaque, _: *DB, _: []const u8, _: u64) !void {}
            fn afterActivation(_: *anyopaque, _: *DB, _: []const u8) !void {
                return error.TestCrashAfterPointerActivation;
            }
        };
        var hook_ctx: u8 = 0;
        db.indexes.repair.shadow_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = CrashHook.afterSnapshot,
            .after_pointer_activation = CrashHook.afterActivation,
        };
        try std.testing.expectError(error.TestCrashAfterPointerActivation, db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .embedding,
            .index_name = "dense_idx",
            .limit = 1,
        }));
        db.indexes.repair.shadow_hook = null;
        var interrupted = try db.indexes.repair.loadEntryById(alloc, repair_id);
        defer interrupted.deinit(alloc);
        try std.testing.expectEqual(db_mod.index_repair_state.Phase.activating, interrupted.intent.phase);
        try std.testing.expect(interrupted.pin != null);
        try std.testing.expect(try db.core.index_manager.isRepairCandidateActive("dense_idx", interrupted.intent.candidate_relative_path.?));
        try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
            .limit = 1,
        }));
    }

    var reopened = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    }));

    const ReconcileRace = struct {
        db: *DB,
        repair_id: u128,
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        structural_started: std.atomic.Value(bool) = .init(false),
        structural_acquired: std.atomic.Value(bool) = .init(false),
        result: ?DB.IndexRepairAdvanceResult = null,
        err: ?anyerror = null,

        fn afterValidation(raw: *anyopaque, _: *DB, observed_repair_id: u128) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.repair_id, observed_repair_id);
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
        }

        fn reconcile(self: *@This()) void {
            self.result = self.db.indexes.repair.advanceIntent(self.db.alloc, self.repair_id, .{}) catch |err| {
                self.err = err;
                return;
            };
        }

        fn structural(self: *@This()) void {
            self.structural_started.store(true, .release);
            var guard = self.db.beginIndexStructuralMutation("test concurrent index publication", "dense_idx");
            defer guard.deinit();
            self.structural_acquired.store(true, .release);
        }
    };
    var race = ReconcileRace{ .db = &reopened, .repair_id = repair_id };
    DB.test_index_repair_reconcile_fence_hook = .{
        .ptr = &race,
        .after_validation = ReconcileRace.afterValidation,
    };
    defer DB.test_index_repair_reconcile_fence_hook = null;

    var reconcile_thread = try std.testing.io.concurrent(ReconcileRace.reconcile, .{&race});
    var reconcile_joined = false;
    defer if (!reconcile_joined) {
        race.release.store(true, .release);
        reconcile_thread.await(std.testing.io);
    };
    var reconcile_entered = false;
    for (0..100_000) |_| {
        if (race.entered.load(.acquire)) {
            reconcile_entered = true;
            break;
        }
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    if (!reconcile_entered) return error.TestTimeout;

    var structural_thread = try std.testing.io.concurrent(ReconcileRace.structural, .{&race});
    var structural_joined = false;
    defer if (!structural_joined) {
        race.release.store(true, .release);
        structural_thread.await(std.testing.io);
    };
    var structural_started = false;
    for (0..100_000) |_| {
        if (race.structural_started.load(.acquire)) {
            structural_started = true;
            break;
        }
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    if (!structural_started) return error.TestTimeout;
    for (0..256) |_| std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expect(!race.structural_acquired.load(.acquire));

    race.release.store(true, .release);
    reconcile_thread.await(std.testing.io);
    reconcile_joined = true;
    structural_thread.await(std.testing.io);
    structural_joined = true;
    DB.test_index_repair_reconcile_fence_hook = null;
    try std.testing.expect(race.err == null);
    const reconciled = race.result orelse return error.TestUnexpectedResult;
    try std.testing.expect(race.structural_acquired.load(.acquire));
    try std.testing.expect(reconciled.attempted);
    try std.testing.expect(reconciled.repaired);
    try std.testing.expect(!try reopened.indexes.repair.hasPendingIntents(alloc));
    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db root generation rollover preserves activated repair debt fail closed" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var old_repair_id: u128 = 0;
    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .lsm_root_generation = 1,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeSentinel(u8, dense_path, 0);
    defer alloc.free(dense_path_z);
    {
        var hbc = try db_mod.hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 2,
            .storage_backend = .lsm,
        }, .{ .root_generation = 1 });
        try hbc.markPublishedGenerationIncompleteForTest();
        hbc.close();
    }

    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .lsm_root_generation = 1,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        _ = try db.discoverRecoverableStartupIndexFailures(alloc, 1);
        old_repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse
            return error.TestUnexpectedResult;

        const CrashHook = struct {
            fn afterSnapshot(_: *anyopaque, _: *DB, _: []const u8, _: u64) !void {}

            fn afterActivation(_: *anyopaque, _: *DB, _: []const u8) !void {
                return error.TestCrashAfterPointerActivation;
            }
        };
        var hook_ctx: u8 = 0;
        db.indexes.repair.shadow_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = CrashHook.afterSnapshot,
            .after_pointer_activation = CrashHook.afterActivation,
        };
        try std.testing.expectError(
            error.TestCrashAfterPointerActivation,
            db.repairArtifactIssuesWithRequestOptions(alloc, .{
                .target = .index,
                .artifact_kind = .embedding,
                .index_name = "dense_idx",
                .limit = 1,
            }, db_mod.repair_completion_test_options),
        );
        db.indexes.repair.shadow_hook = null;
    }

    var reopened = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .lsm_root_generation = 2,
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    }));
    const new_repair_id = (try reopened.indexes.repair.idForIndex(alloc, "dense_idx")) orelse
        return error.TestUnexpectedResult;
    try std.testing.expect(new_repair_id != old_repair_id);
    var replacement = try reopened.indexes.repair.loadEntryById(alloc, new_repair_id);
    defer replacement.deinit(alloc);
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.root_generation_rebuild, replacement.intent.trigger);
    try std.testing.expectEqual(@as(u64, 2), replacement.intent.root_generation);
    try std.testing.expect(replacement.intent.candidate_relative_path == null);

    // This checks generation rollover recovery, not the production pause SLA.
    const repaired = try reopened.indexes.repair.advanceIntent(alloc, new_repair_id, db_mod.repair_completion_test_options);
    try std.testing.expect(repaired.attempted);
    try std.testing.expect(repaired.repaired);
    try std.testing.expect(!try reopened.indexes.repair.hasPendingIntents(alloc));
    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db restart reconciles an activated graph repair without rebuilding" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var repair_id: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });

        const edge_key = try db_mod.internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph_v1", "mentions", "doc:b");
        defer alloc.free(edge_key);
        const edge_value = try db_mod.enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, db.core.index_manager.graphIndex("graph_v1").?.config.coverage_generation, 0.75, 0, 0, "");
        defer alloc.free(edge_value);
        try db.core.store.put(edge_key, edge_value);

        const CrashHook = struct {
            fn afterSnapshot(_: *anyopaque, _: *DB, _: []const u8, _: u64) !void {}
            fn afterActivation(_: *anyopaque, _: *DB, _: []const u8) !void {
                return error.TestCrashAfterPointerActivation;
            }
        };
        var hook_ctx: u8 = 0;
        db.indexes.repair.shadow_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = CrashHook.afterSnapshot,
            .after_pointer_activation = CrashHook.afterActivation,
        };
        try std.testing.expectError(error.TestCrashAfterPointerActivation, db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .graph,
            .index_name = "graph_v1",
            .limit = 1,
            .force = true,
        }));
        db.indexes.repair.shadow_hook = null;
        repair_id = (try db.indexes.repair.idForIndex(alloc, "graph_v1")) orelse return error.TestUnexpectedResult;
        var interrupted = try db.indexes.repair.loadEntryById(alloc, repair_id);
        defer interrupted.deinit(alloc);
        try std.testing.expectEqual(db_mod.index_repair_state.Phase.activating, interrupted.intent.phase);
        try std.testing.expect(try db.core.index_manager.isRepairCandidateActive("graph_v1", interrupted.intent.candidate_relative_path.?));
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    const reconciled = try reopened.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(reconciled.attempted);
    try std.testing.expect(reconciled.repaired);
    try std.testing.expect(!try reopened.indexes.repair.hasPendingIntents(alloc));
    const edges = try reopened.getEdges(alloc, "graph_v1", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
}

test "db paused dense repair resumes its durable candidate after restart" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeSentinel(u8, dense_path, 0);
    defer alloc.free(dense_path_z);
    {
        var hbc = try db_mod.hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 2,
            .storage_backend = .lsm,
        }, .{});
        try hbc.markPublishedGenerationIncompleteForTest();
        hbc.close();
    }

    var repair_id: u128 = 0;
    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        _ = try db.discoverRecoverableStartupIndexFailures(alloc, 1);
        repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;

        const PauseHook = struct {
            fn afterSnapshot(_: *anyopaque, hook_db: *DB, index_name: []const u8, _: u64) !void {
                var control = try hook_db.repairArtifactIssuesWithRequest(hook_db.alloc, .{
                    .target = .index,
                    .index_name = index_name,
                    .control = .pause_automatic,
                });
                defer control.deinit(hook_db.alloc);
                try std.testing.expectEqual(@as(u64, 1), control.controls_applied);
            }
        };
        var hook_ctx: u8 = 0;
        db.indexes.repair.shadow_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = PauseHook.afterSnapshot,
        };
        const paused = try db.indexes.repair.advanceIntent(alloc, repair_id, .{});
        db.indexes.repair.shadow_hook = null;
        try std.testing.expect(paused.attempted);
        try std.testing.expect(paused.deferred);
        try std.testing.expect(!paused.repaired);

        var state = try db.indexes.repair.loadEntryById(alloc, repair_id);
        defer state.deinit(alloc);
        try std.testing.expectEqual(db_mod.index_repair_state.Automation.paused, state.intent.automation);
        try std.testing.expectEqual(db_mod.index_repair_state.Phase.catching_up, state.intent.phase);
        try std.testing.expect(state.intent.candidate_relative_path != null);
        try std.testing.expect(state.intent.estimated_candidate_bytes > 0);
        try std.testing.expect(state.intent.planned_disk_bytes >= state.intent.estimated_candidate_bytes);
        try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
            .limit = 1,
        }));
    }

    var reopened = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    var restarted_state = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
    try std.testing.expect(restarted_state.intent.planned_disk_bytes >= restarted_state.intent.estimated_candidate_bytes);
    try std.testing.expect(restarted_state.intent.planned_disk_bytes > 0);
    restarted_state.deinit(alloc);
    var resume_control = try reopened.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .control = .resume_automatic,
    });
    defer resume_control.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), resume_control.controls_applied);
    const resumed = try reopened.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(resumed.attempted);
    try std.testing.expect(resumed.repaired);
    try std.testing.expectEqual(@as(u64, 0), resumed.documents_reprocessed);
    try std.testing.expect(!try reopened.indexes.repair.hasPendingIntents(alloc));

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db dense repair durably yields and resumes a reopenable building candidate" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    db_mod.test_dense_repair_rebuild_batch_size = 1;
    defer db_mod.test_dense_repair_rebuild_batch_size = null;
    db_mod.test_index_repair_catch_up_max_records_per_window = 1;
    defer db_mod.test_index_repair_catch_up_max_records_per_window = null;

    const cfg: db_mod.types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"bravo\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"charlie\",\"_embeddings\":{\"dense_idx\":[0,0,1]}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"_embeddings\":{\"dense_idx\":[1,1,0]}}" },
            },
            .sync_level = .full_index,
        });
    }

    const active_path = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}", .{ std.mem.span(path), cfg.name });
    defer alloc.free(active_path);
    const active_path_z = try alloc.dupeSentinel(u8, active_path, 0);
    defer alloc.free(active_path_z);
    {
        var hbc = try db_mod.hbc_mod.HBCIndex.openWithLsmOptions(alloc, active_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{});
        try hbc.markPublishedGenerationIncompleteForTest();
        hbc.close();
    }

    const Yield = struct {
        fn requested(_: *anyopaque) bool {
            return true;
        }
    };
    var yield_token: u8 = 0;
    const options = db_mod.types.ArtifactRepairRunOptions{
        .yield_check = .{ .ptr = &yield_token, .is_requested = Yield.requested },
        // This test covers durable yield/resume checkpoints, not the default
        // activation pause SLA. Leave enough headroom for contended CI hosts.
        .max_activation_pause_ms = 5_000,
    };
    var repair_id: u128 = 0;
    var candidate_path: []u8 = undefined;
    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        _ = try db.discoverRecoverableStartupIndexFailures(alloc, 1);
        repair_id = (try db.indexes.repair.idForIndex(alloc, cfg.name)) orelse return error.TestUnexpectedResult;
        const first = try db.indexes.repair.advanceIntent(alloc, repair_id, options);
        try std.testing.expect(first.attempted);
        try std.testing.expect(first.busy);
        try std.testing.expect(!first.repaired);
        try std.testing.expectEqual(@as(u64, 1), first.documents_reprocessed);

        var state = try db.indexes.repair.loadState(alloc);
        defer state.deinit(alloc);
        const intent = state.entries.items[0].intent;
        try std.testing.expectEqual(db_mod.index_repair_state.Phase.building, intent.phase);
        try std.testing.expect(intent.build_resume_key != null);
        try std.testing.expectEqual(@as(u64, 1), intent.build_reprocessed);
        try std.testing.expectEqual(@as(u32, 0), intent.failure_streak);
        candidate_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ std.mem.span(path), intent.candidate_relative_path.? });

        // Mutate both sides of the saved source cursor. The new key sorts
        // before the first artifact slice and must arrive through replay; the
        // deleted key sorts after it and may disappear from a later snapshot.
        // Final fenced coverage must still converge without restarting the
        // candidate from zero.
        try db.batch(.{
            .writes = &.{.{ .key = "doc:0", .value = "{\"title\":\"zero\",\"_embeddings\":{\"dense_idx\":[1,0,1]}}" }},
            .sync_level = .write,
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"bravo-updated\",\"_embeddings\":{\"dense_idx\":[0,1,1]}}" }},
            .sync_level = .write,
        });
        try db.batch(.{ .deletes = &.{"doc:d"}, .sync_level = .write });
    }
    defer alloc.free(candidate_path);

    // A yielded building candidate has no incomplete-publication marker and is
    // independently reopenable before the next process resumes its cursor.
    const candidate_path_z = try alloc.dupeSentinel(u8, candidate_path, 0);
    defer alloc.free(candidate_path_z);
    {
        var candidate = try db_mod.hbc_mod.HBCIndex.openWithLsmOptions(alloc, candidate_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{});
        candidate.close();
    }

    var documents_reprocessed: u64 = 1;
    var observed_catch_up_yield = false;
    {
        var reopened = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();
        for (0..16) |_| {
            const step = try reopened.indexes.repair.advanceIntent(alloc, repair_id, options);
            documents_reprocessed += step.documents_reprocessed;
            try std.testing.expect(!step.repaired);
            try std.testing.expect(step.busy);
            try std.testing.expect(!step.terminal);
            var state = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
            defer state.deinit(alloc);
            if (state.intent.phase == .catching_up) {
                try std.testing.expect(state.intent.candidate_applied_sequence > state.intent.build_floor_sequence);
                observed_catch_up_yield = true;
                break;
            }
        }
    }
    try std.testing.expect(observed_catch_up_yield);

    // Reopen from the durable replay checkpoint, proving a yielded catch-up
    // turn is independently restartable just like a yielded source scan.
    var final_reopened = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer final_reopened.close();
    var repaired = false;
    for (0..16) |_| {
        const step = try final_reopened.indexes.repair.advanceIntent(alloc, repair_id, options);
        documents_reprocessed += step.documents_reprocessed;
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(step.busy);
        try std.testing.expect(!step.terminal);
    }
    try std.testing.expect(repaired);
    // Three source artifacts were scanned (the fourth was concurrently
    // deleted); doc:0 arrived through replay and is intentionally not counted
    // as snapshot-reprocessed work.
    try std.testing.expectEqual(@as(u64, 3), documents_reprocessed);
    try std.testing.expect(!try final_reopened.indexes.repair.hasPendingIntents(alloc));

    var result = try final_reopened.search(alloc, .{
        .index_name = cfg.name,
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 4 } },
        .limit = 4,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 4), result.total_hits);
    var found_replayed_insert = false;
    for (result.hits) |hit| {
        found_replayed_insert = found_replayed_insert or std.mem.eql(u8, hit.id, "doc:0");
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:d"));
    }
    try std.testing.expect(found_replayed_insert);
}

test "db managed vector admission captures writes while durable repair is pending" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const cfg = db_mod.types.IndexConfig{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:old", .value = "{\"body\":\"before admission\"}" }},
        .sync_level = .write,
    });

    _ = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(try db.indexes.repair.hasPendingIntents(alloc));
    try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
        .index_name = cfg.name,
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
    }));

    // A post-admission write is allowed to commit while the corpus rebuild is
    // pending. With workers disabled, its target watermark is durable proof
    // that catch-up owns the write instead of silently dropping the vector.
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:new",
            .value = "{\"body\":\"after admission\",\"embedding\":[1,0,0]}",
        }},
        .sync_level = .write,
    });
    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    const target = for (stats.indexes) |item| {
        if (std.mem.eql(u8, item.name, cfg.name)) break item;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 0), target.doc_count);
    try std.testing.expect(target.replay_target_sequence > target.replay_applied_sequence);
}

test "db managed external dense admission ignores unrelated source rows" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const cfg = db_mod.types.IndexConfig{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "readiness", .value = "{\"ready\":true}" }},
        .sync_level = .write,
    });

    try std.testing.expect((try db.admitManagedIndex(cfg)) == null);
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));
    try std.testing.expect(!db.core.index_manager.repairUnavailable(cfg.name));
    try std.testing.expectEqual(
        @as(?u64, 0),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, cfg.name),
    );
    const admission_key = try db_mod.internal_keys.managedIndexAdmissionKeyAlloc(alloc, cfg.name);
    defer alloc.free(admission_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, admission_key));

    // Subsequent caller-owned artifacts still enter through ordinary replay.
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:new",
            .value = "{\"body\":\"after admission\",\"_embeddings\":{\"semantic_idx\":[1,0,0]}}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, cfg.name),
    );
    var result = try db.search(alloc, .{
        .index_name = cfg.name,
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:new", result.hits[0].id);
}

test "db managed vector admission discovers artifacts committed before resident activation" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const cfg = db_mod.types.IndexConfig{
        .name = "late_external",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    // Model a catalog create whose resident activation is queued while the
    // write path accepts the newly committed external artifact.
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"arrived during activation\",\"_embeddings\":{\"late_external\":[1,0,0]}}",
        }},
        .sync_level = .write,
    });
    const artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", cfg.name);
    defer alloc.free(artifact_key);
    const artifact = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact);

    const repair_id = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;
    // A non-empty managed admission must bootstrap from the authoritative
    // artifact namespace. Zero here would incorrectly certify an empty target.
    try std.testing.expectEqual(
        @as(?u64, null),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, cfg.name),
    );

    var repaired = false;
    for (0..16) |_| {
        const step = try db.indexes.repair.advanceIntent(alloc, repair_id, db_mod.repair_completion_test_options);
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(!step.terminal);
    }
    try std.testing.expect(repaired);
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, cfg.name),
    );
    try std.testing.expectEqual(
        @as(u64, 1),
        db.core.index_manager.denseIndex(cfg.name).?.index.stats().active_count,
    );
    var result = try db.search(alloc, .{
        .index_name = cfg.name,
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db managed repair scheduler defers canonical worker until shadow activation" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    const cfg = db_mod.types.IndexConfig{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json =
        \\{"field":"body","dims":3,"metric":"cosine","embedding_name":"semantic_idx","publication_policy":"atomic","generator":{"kind":"dense_embedding","source_field":"body","embedding_name":"semantic_idx"}}
        ,
        .coverage_generation = 44,
    };
    // Model the provisioned runtime's durable repair owner without starting
    // unrelated optional test threads.
    db.local_execution.optional_runtime_workers_enabled = true;
    const repair_id = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;
    db.local_execution.optional_runtime_workers_enabled = false;

    try std.testing.expect(db.executor.appliedSequence(cfg.name) == null);
    try db_mod.drainManagedAdmissionSourceReplayForTest(&db, alloc, repair_id);
    try std.testing.expect(db.executor.appliedSequence(cfg.name) == null);
    try std.testing.expectEqual(
        @as(u64, 1),
        (try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, cfg.name)).?,
    );
    // Force the generation path even though the manual enrichment drain can
    // make the tiny canonical generation independently serviceable. This
    // isolates the activation contract under test: an absent admission worker
    // must be installed at the shadow's final replay fence.
    try db.indexes.repair.updateIntent(alloc, repair_id, .{
        .trigger = .operator_generation_rebuild,
        .replace_last_error = true,
    });

    var repaired = false;
    for (0..16) |_| {
        const step = try db.indexes.repair.advanceIntent(alloc, repair_id, db_mod.repair_completion_test_options);
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(!step.terminal);
    }
    try std.testing.expect(repaired);
    const durable_applied = try db.core.loadAppliedSequence(alloc, cfg.name);
    try std.testing.expectEqual(durable_applied, db.executor.appliedSequence(cfg.name).?);
    try std.testing.expect(!db.core.index_manager.repairUnavailable(cfg.name));

    // Intent retirement must leave a live canonical consumer, not merely a
    // serviceable point-in-time generation. Publish one later derived record
    // and prove the newly attached worker advances from the durable cursor.
    const stored_key = try db_mod.internal_keys.documentKeyAlloc(alloc, "doc:b");
    defer alloc.free(stored_key);
    try db.core.store.put(stored_key, "{\"body\":\"beta\"}");
    const artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:b", cfg.name);
    defer alloc.free(artifact_key);
    try db_mod.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 0, 1, 0 });
    const later_sequence = try db_mod.appendDerivedBatchRecord(&db, .{
        .dense_embeddings = &.{.{
            .index_name = cfg.name,
            .doc_key = "doc:b",
            .artifact_key = artifact_key,
            .vector = &[_]f32{ 1, 0, 0 },
        }},
    });
    try db.runDerivedUntil(later_sequence);
    try std.testing.expect(db.executor.appliedSequence(cfg.name).? >= later_sequence);
}

test "resident index repair scheduler skips deferred prefixes with bounded fair quanta" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    _ = try db.indexes.repair.intentSummary(alloc);

    {
        db_mod.lockAtomic(&db.async_context.index_repair_scheduler_mutex);
        defer db.async_context.index_repair_scheduler_mutex.unlock();
        for (0..18) |i| {
            const index_name = try std.fmt.allocPrint(alloc, "repair-{d}", .{i});
            var intent = db_mod.index_repair_state.IndexRepairIntent{
                .repair_id = i + 1,
                .db_identity = 1,
                .group_id = 1,
                .replica_id = 1,
                .root_generation = 1,
                .index_name = index_name,
                .kind = .full_text,
                .config_hash = 1,
                .detected_sequence = 1,
                .target_sequence = 1,
                .started_at_ms = 1,
                .updated_at_ms = 1,
                .owner_epoch = 0,
                .automation = if (i < 17) .paused else .enabled,
            };
            try db.async_context.index_repair_scheduler.upsert(db.async_context.alloc, intent, 1);
            intent.deinit(alloc);
        }
    }

    // Exact migration repair bypasses unrelated paused prefixes without
    // consuming the ordinary scheduler's fairness cursor.
    const cursor_before = db.async_context.index_repair_scheduler.cursor;
    var targeted = try db.indexes.repair.selectSchedulerQuantumForIndex(alloc, 1, "repair-17");
    defer targeted.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), targeted.inspected);
    try std.testing.expectEqual(@as(usize, 1), targeted.repairs.items.len);
    try std.testing.expectEqual(@as(usize, 1), targeted.remaining);
    try std.testing.expectEqual(@as(usize, 0), targeted.deferred);
    try std.testing.expectEqual(@as(u128, 18), targeted.repairs.items[0].repair_id);
    try std.testing.expectEqual(cursor_before, db.async_context.index_repair_scheduler.cursor);
    var paused_target = try db.indexes.repair.selectSchedulerQuantumForIndex(alloc, 1, "repair-0");
    defer paused_target.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), paused_target.repairs.items.len);
    try std.testing.expectEqual(@as(usize, 1), paused_target.deferred);
    var absent_target = try db.indexes.repair.selectSchedulerQuantumForIndex(alloc, 1, "absent");
    defer absent_target.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), absent_target.inspected);
    try std.testing.expectEqual(@as(usize, 0), absent_target.remaining);
    const target_summary = try db.indexes.repair.intentSummaryForIndex(alloc, "repair-17");
    try std.testing.expectEqual(@as(usize, 1), target_summary.runnable);
    try std.testing.expectEqual(@as(usize, 0), target_summary.paused);
    try std.testing.expectEqual(@as(usize, 1), (try db.indexes.repair.intentSummaryForIndex(alloc, "repair-0")).paused);
    try std.testing.expectEqual(@as(usize, 0), (try db.indexes.repair.intentSummaryForIndex(alloc, "absent")).runnable);

    var first = try db.indexes.repair.selectSchedulerQuantum(alloc, 1);
    defer first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 8), first.inspected);
    try std.testing.expectEqual(@as(usize, 0), first.repairs.items.len);

    var second = try db.indexes.repair.selectSchedulerQuantum(alloc, 1);
    defer second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 8), second.inspected);
    try std.testing.expectEqual(@as(usize, 0), second.repairs.items.len);

    var third = try db.indexes.repair.selectSchedulerQuantum(alloc, 1);
    defer third.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 8), third.inspected);
    try std.testing.expectEqual(@as(usize, 1), third.repairs.items.len);
    try std.testing.expectEqual(@as(u128, 18), third.repairs.items[0].repair_id);
    try std.testing.expect(!third.repairs.items[0].audit_due);
}

test "index repair intent string replacement is allocation failure safe" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try db.indexes.repair.createGenerationIntent(
        alloc,
        cfg.*,
        .operator_generation_validation,
        0,
        0,
        "old-error",
    );
    try db.indexes.repair.updateIntent(alloc, repair_id, .{
        .candidate_relative_path = ".repair-shadow-old/indexes/dense_idx",
        .replace_candidate_path = true,
        .previous_pointer_captured = true,
        .previous_active_relative_path = ".repair-shadow-old-active/indexes/dense_idx",
        .replace_previous_active_path = true,
        .build_resume_key = "old-resume",
        .replace_build_resume_key = true,
        .last_error = "old-error-2",
        .replace_last_error = true,
    });

    try @import("antfly_platform").allocator.checkAllAllocationFailures(
        alloc,
        struct {
            fn run(failing_alloc: Allocator, target_db: *DB, target_repair_id: u128) !void {
                try target_db.indexes.repair.updateIntent(failing_alloc, target_repair_id, .{
                    .candidate_relative_path = ".repair-shadow-new/indexes/dense_idx",
                    .replace_candidate_path = true,
                    .previous_active_relative_path = ".repair-shadow-new-active/indexes/dense_idx",
                    .replace_previous_active_path = true,
                    .build_resume_key = "new-resume",
                    .replace_build_resume_key = true,
                    .last_error = "new-error",
                    .replace_last_error = true,
                });
            }
        }.run,
        .{ &db, repair_id },
    );
    try @import("antfly_platform").allocator.checkAllAllocationFailures(
        alloc,
        struct {
            fn run(failing_alloc: Allocator, target_db: *DB, target_repair_id: u128) !void {
                try target_db.indexes.repair.recordAttemptFailure(
                    failing_alloc,
                    target_repair_id,
                    "next-failure",
                    false,
                );
            }
        }.run,
        .{ &db, repair_id },
    );

    var updated = try db.indexes.repair.loadEntryById(alloc, repair_id);
    defer updated.deinit(alloc);
    try std.testing.expectEqualStrings(".repair-shadow-new/indexes/dense_idx", updated.intent.candidate_relative_path.?);
    try std.testing.expectEqualStrings(".repair-shadow-new-active/indexes/dense_idx", updated.intent.previous_active_relative_path.?);
    try std.testing.expectEqualStrings("new-resume", updated.intent.build_resume_key.?);
    try std.testing.expectEqualStrings("next-failure", updated.intent.last_error.?);
    try std.testing.expectEqual(@as(u32, 1), updated.intent.failure_streak);
}

test "source recovery scheduler rotates source jobs within one table" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("db");
    defer directory.cleanup();
    var db = try DB.open(a, std.mem.span(directory.path().ptr), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    const left = db_mod.types.IndexConfig{ .name = "left", .kind = .full_text, .config_json = "{\"field\":\"body\"}" };
    const right = db_mod.types.IndexConfig{ .name = "right", .kind = .full_text, .config_json = "{\"field\":\"body\"}" };
    try db.addIndex(left);
    try db.addIndex(right);
    const first = try db.indexes.repair.createGenerationIntentAtTarget(a, left, .replay_artifact_unavailable, 0, 0, null, null, .pending, .initial_build);
    const second = try db.indexes.repair.createGenerationIntentAtTarget(a, right, .replay_artifact_unavailable, 0, 0, null, null, .pending, .initial_build);
    var one = try db.indexes.repair.selectSchedulerQuantumScoped(a, 1, null, true);
    defer one.deinit(a);
    var two = try db.indexes.repair.selectSchedulerQuantumScoped(a, 1, null, true);
    defer two.deinit(a);
    try std.testing.expect(one.repairs.items[0].repair_id != two.repairs.items[0].repair_id);
    try std.testing.expect(one.repairs.items[0].repair_id == first or one.repairs.items[0].repair_id == second);
    try std.testing.expect(two.repairs.items[0].repair_id == first or two.repairs.items[0].repair_id == second);
    try std.testing.expect(one.inspected <= DB.index_repair_inspections_per_execution_slot);
    try std.testing.expect(two.inspected <= DB.index_repair_inspections_per_execution_slot);
}

test "db empty managed index does not invent generated coverage recovery debt" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer db.close();
    try std.testing.expectEqual(@as(?u64, null), try db_mod.range_cardinality.load(alloc, db.core.store));
    _ = try db.admitManagedIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json =
        \\{"field":"body","dims":3,"metric":"cosine","embedding_name":"semantic_idx","publication_policy":"progressive","generator":{"kind":"dense_embedding","source_field":"body","embedding_name":"semantic_idx"}}
        ,
        .coverage_generation = 42,
    });
    // No primary mutation has created the range counter. The empty user-key
    // namespace nevertheless proves there are no missing source outcomes;
    // repeated idle audits must not create replacement work.
    try std.testing.expect(!try db.generatedCoverageReplayNeeded(alloc));
    try std.testing.expectEqual(@as(usize, 0), try db.ensureGeneratedCoverageRecoveryIntents(alloc));
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));

    // A legacy primary record can lack both local cardinality and identity
    // metadata. It must remain unknown even when identity counters look empty.
    const primary_key = try db_mod.internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(primary_key);
    try db.core.store.put(primary_key, "{\"body\":\"alpha\"}");
    try std.testing.expect(try db.generatedCoverageReplayNeeded(alloc));
}

test "db completed chunked managed admission uses physical artifact count" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var counting = db_mod.CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{ .dense_embedder = counting.interface() },
    });
    defer db.close();
    const doc_count = 50;
    const writes = try alloc.alloc(db_mod.types.BatchWrite, doc_count);
    defer {
        for (writes) |write| alloc.free(@constCast(write.key));
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i}),
            .value = "{\"title\":\"same\",\"body\":\"alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu\"}",
        };
    }
    for (0..5) |batch_index| {
        const offset = batch_index * 10;
        try db.batch(.{ .writes = writes[offset .. offset + 10], .sync_level = .write });
    }

    const cfg = db_mod.types.IndexConfig{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json =
        \\{"field":"embedding","dims":3,"generator":{"kind":"dense_embedding","source_field":"body","chunk_name":"semantic_idx_chunks","chunk_size":4,"chunk_overlap":1}}
        ,
        .coverage_generation = 43,
    };
    const repair_id = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;
    try db_mod.drainManagedAdmissionSourceReplayForTest(&db, alloc, repair_id);
    try db_mod.awaitManagedAdmissionPublicationForTest(&db, alloc, repair_id);

    const active = db.core.index_manager.denseIndex(cfg.name) orelse return error.TestUnexpectedResult;
    const physical_count = active.index.stats().active_count;
    try std.testing.expect(physical_count > doc_count);
    const artifact_count = (try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, cfg.name)) orelse
        return error.TestUnexpectedResult;
    try std.testing.expectEqual(artifact_count, physical_count);
    const publication_stats = try db.runtimeStatusStatsConsistent(alloc);
    defer db_mod.types.freeDBStats(alloc, publication_stats);
    const publication_index = blk: {
        for (publication_stats.indexes) |index_stats| {
            if (std.mem.eql(u8, index_stats.name, cfg.name)) break :blk index_stats;
        }
        return error.TestUnexpectedResult;
    };
    try std.testing.expect(publication_index.publication_target_ready);
    try std.testing.expectEqual(physical_count, publication_index.publication_target_count);
    const calls_before_replay = counting.calls;
    try std.testing.expect(calls_before_replay > 0);
    const dense_replay_before = try db.core.store.latestReplaySequenceForHint(.dense_vector, 0);
    const enrichment_replay_before = try db.core.store.latestReplaySequenceForHint(.enrichment, 0);

    var repair = try db.indexes.repair.loadEntryById(alloc, repair_id);
    defer repair.deinit(alloc);
    try std.testing.expect(try db.managedAdmissionGenerationIsServiceable(alloc, repair.intent));

    const completed = try db.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(completed.attempted);
    try std.testing.expect(completed.repaired);
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));

    // Replaying structurally identical source documents must not enqueue new
    // embedding work, even when an API serializer changes object key order.
    const reordered_writes = try alloc.alloc(db_mod.types.BatchWrite, doc_count);
    defer alloc.free(reordered_writes);
    for (reordered_writes, 0..) |*write, i| {
        write.* = .{
            .key = writes[i].key,
            .value = "{\"body\":\"alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu\",\"title\":\"same\"}",
        };
    }
    for (0..5) |batch_index| {
        const offset = batch_index * 10;
        try db.batch(.{ .writes = reordered_writes[offset .. offset + 10], .sync_level = .write });
    }
    try db.runUntilIdle();
    try std.testing.expectEqual(physical_count, active.index.stats().active_count);
    try std.testing.expectEqual(calls_before_replay, counting.calls);
    try std.testing.expectEqual(dense_replay_before, try db.core.store.latestReplaySequenceForHint(.dense_vector, 0));
    try std.testing.expectEqual(enrichment_replay_before, try db.core.store.latestReplaySequenceForHint(.enrichment, 0));
}

test "db managed algebraic admission builds and reopens an isolated generation" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const cfg = db_mod.types.IndexConfig{
        .name = "analytics_idx",
        .kind = .algebraic,
        .config_json =
        \\{"version":1,"table":"docs","capability_lifecycle_status":"rebuild_required","dynamic_rules_backfill_pending":true,"group_fields":[{"name":"category","path":"category","type":"string"}],"dynamic_field_rules":[{"name":"ids","match":"*_id","type":"string"}],"materializations":[]}
        ,
    };
    var active_namespace: []u8 = undefined;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:old", .value = "{\"category\":\"tools\"}" }},
            .sync_level = .write,
        });

        const repair_id = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;
        try std.testing.expect(db.core.index_manager.repairUnavailable(cfg.name));
        try std.testing.expect(try db.indexes.repair.hasPendingIntents(alloc));
        const canonical = db.core.index_manager.algebraicIndex(cfg.name) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("canonical/analytics_idx", canonical.index.storageNamespace());
        try std.testing.expect(!canonical.index.plannerLifecycleReady());
        try std.testing.expect(canonical.index.resolveField("user_id", .group) == null);

        var repaired = false;
        for (0..16) |_| {
            const step = try db.indexes.repair.advanceIntent(alloc, repair_id, db_mod.repair_completion_test_options);
            if (step.repaired) {
                repaired = true;
                break;
            }
            try std.testing.expect(!step.terminal);
            try std.testing.expectEqual(@as(u64, 0), step.next_retry_at_ms);
        }
        try std.testing.expect(repaired);
        try std.testing.expect(!db.core.index_manager.repairUnavailable(cfg.name));
        try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));

        const active = db.core.index_manager.algebraicIndex(cfg.name) orelse return error.TestUnexpectedResult;
        try std.testing.expect(std.mem.startsWith(u8, active.index.storageNamespace(), ".repair-shadow-"));
        try std.testing.expect(active.index.plannerLifecycleReady());
        try std.testing.expect(active.index.resolveField("user_id", .group) != null);
        active_namespace = try alloc.dupe(u8, active.index.storageNamespace());
        var resolved = (try active.index.resolvedDocFilterForFilterJsonAlloc(
            db.core.store,
            "{\"term\":{\"category\":\"tools\"}}",
        )) orelse return error.TestUnexpectedResult;
        defer resolved.deinit(alloc);
        switch (resolved.include) {
            .ordinals => |ordinals| try std.testing.expectEqual(@as(usize, 1), ordinals.len),
            else => return error.ExpectedOrdinalDocSet,
        }
    }
    defer alloc.free(active_namespace);

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    const active = reopened.core.index_manager.algebraicIndex(cfg.name) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(active_namespace, active.index.storageNamespace());
    try std.testing.expect(active.index.plannerLifecycleReady());
    try std.testing.expect(active.index.resolveField("user_id", .group) != null);
    var resolved = (try active.index.resolvedDocFilterForFilterJsonAlloc(
        reopened.core.store,
        "{\"term\":{\"category\":\"tools\"}}",
    )) orelse return error.TestUnexpectedResult;
    defer resolved.deinit(alloc);
    switch (resolved.include) {
        .ordinals => |ordinals| try std.testing.expectEqual(@as(usize, 1), ordinals.len),
        else => return error.ExpectedOrdinalDocSet,
    }
}

test "db algebraic generation build yields and resumes from its durable source cursor" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const cfg = db_mod.types.IndexConfig{
        .name = "analytics_idx",
        .kind = .algebraic,
        .config_json =
        \\{"version":1,"table":"docs","group_fields":[{"name":"category","path":"category","type":"string"}],"materializations":[]}
        ,
    };
    db_mod.test_algebraic_repair_rebuild_batch_size = 1;
    defer db_mod.test_algebraic_repair_rebuild_batch_size = null;
    const Yield = struct {
        fn requested(_: *anyopaque) bool {
            return true;
        }
    };
    var yield_token: u8 = 0;
    var repair_id: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"category\":\"tools\"}" },
                .{ .key = "doc:b", .value = "{\"category\":\"books\"}" },
            },
            .sync_level = .write,
        });
        repair_id = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;
        var yield_options = db_mod.repair_completion_test_options;
        yield_options.yield_check = .{ .ptr = &yield_token, .is_requested = Yield.requested };
        const first = try db.indexes.repair.advanceIntent(alloc, repair_id, yield_options);
        try std.testing.expect(first.attempted);
        try std.testing.expect(first.busy);
        try std.testing.expect(!first.repaired);
        try std.testing.expectEqual(@as(u64, 1), first.documents_reprocessed);

        var state = try db.indexes.repair.loadState(alloc);
        defer state.deinit(alloc);
        const intent = state.entries.items[0].intent;
        try std.testing.expectEqual(db_mod.index_repair_state.Phase.building, intent.phase);
        try std.testing.expect(intent.build_resume_key != null);
        try std.testing.expectEqual(@as(u64, 1), intent.build_reprocessed);
        const candidate = intent.candidate_relative_path orelse return error.TestUnexpectedResult;
        const separator = std.mem.indexOfScalar(u8, candidate, '/') orelse return error.TestUnexpectedResult;
        const storage_namespace = try std.fmt.allocPrint(
            alloc,
            "{s}/{s}",
            .{ candidate[0..separator], cfg.name },
        );
        defer alloc.free(storage_namespace);
        try std.testing.expect((try db.core.index_manager.algebraicStorageNamespaceBytes(storage_namespace)) > 0);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expectEqual(repair_id, (try reopened.indexes.repair.idForIndex(alloc, cfg.name)).?);
    var repaired = false;
    for (0..16) |_| {
        const step = try reopened.indexes.repair.advanceIntent(alloc, repair_id, db_mod.repair_completion_test_options);
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(!step.terminal);
        try std.testing.expectEqual(@as(u64, 0), step.next_retry_at_ms);
    }
    try std.testing.expect(repaired);
    const active = reopened.core.index_manager.algebraicIndex(cfg.name) orelse return error.TestUnexpectedResult;
    var tools = (try active.index.resolvedDocFilterForFilterJsonAlloc(
        reopened.core.store,
        "{\"term\":{\"category\":\"tools\"}}",
    )) orelse return error.TestUnexpectedResult;
    defer tools.deinit(alloc);
    var books = (try active.index.resolvedDocFilterForFilterJsonAlloc(
        reopened.core.store,
        "{\"term\":{\"category\":\"books\"}}",
    )) orelse return error.TestUnexpectedResult;
    defer books.deinit(alloc);
    switch (tools.include) {
        .ordinals => |ordinals| try std.testing.expectEqual(@as(usize, 1), ordinals.len),
        else => return error.ExpectedOrdinalDocSet,
    }
    switch (books.include) {
        .ordinals => |ordinals| try std.testing.expectEqual(@as(usize, 1), ordinals.len),
        else => return error.ExpectedOrdinalDocSet,
    }
}

test "db forced algebraic repair persists an operator generation intent before execution" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const cfg = db_mod.types.IndexConfig{
        .name = "analytics_idx",
        .kind = .algebraic,
        .config_json =
        \\{"version":1,"table":"docs","group_fields":[{"name":"category","path":"category","type":"string"}],"materializations":[]}
        ,
    };
    var repair_id: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(cfg);
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"category\":\"tools\"}" }},
            .sync_level = .full_index,
        });
        var deferred = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
            .target = .index,
            .artifact_kind = .algebraic,
            .index_name = cfg.name,
            .limit = 1,
            .force = true,
            .repair_job_id = 77,
            .repair_job_created_at_ms = 88,
        }, .{ .defer_durable_index_repair_execution = true });
        defer deferred.deinit(alloc);
        try std.testing.expectEqual(@as(u64, 1), deferred.in_progress);
        try std.testing.expectEqual(@as(u64, 0), deferred.indexes_rebuilt);
        repair_id = (try db.indexes.repair.idForIndex(alloc, cfg.name)) orelse return error.TestUnexpectedResult;
        var entry = try db.indexes.repair.loadEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        try std.testing.expectEqual(db_mod.index_repair_state.Trigger.operator_generation_rebuild, entry.intent.trigger);
        try std.testing.expectEqual(@as(u64, 77), entry.intent.operator_job_id);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expectEqual(repair_id, (try reopened.indexes.repair.idForIndex(alloc, cfg.name)).?);
    var repaired = false;
    for (0..16) |_| {
        const step = try reopened.indexes.repair.advanceIntent(alloc, repair_id, db_mod.repair_completion_test_options);
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(!step.terminal);
        try std.testing.expectEqual(@as(u64, 0), step.next_retry_at_ms);
    }
    try std.testing.expect(repaired);
    try std.testing.expect((try reopened.indexes.repair.idForIndex(alloc, cfg.name)) == null);
}

test "db algebraic post-commit activation crash recovers through generation repair" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const cfg = db_mod.types.IndexConfig{
        .name = "analytics_idx",
        .kind = .algebraic,
        .config_json =
        \\{"version":1,"table":"docs","group_fields":[{"name":"category","path":"category","type":"string"}],"materializations":[]}
        ,
    };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:old", .value = "{\"category\":\"tools\"}" }},
            .sync_level = .write,
        });

        DB.test_fail_index_activation_after_catalog_commit = true;
        defer DB.test_fail_index_activation_after_catalog_commit = false;
        const installed = try db.installIndexWhileEnrichmentQuiesced(cfg, .ordinary);
        DB.test_fail_index_activation_after_catalog_commit = false;
        const post_commit_error = installed.post_commit_error orelse return error.TestUnexpectedResult;
        try std.testing.expect(post_commit_error == error.TestPostCommitIndexActivation);

        const admission_key = try db_mod.internal_keys.managedIndexAdmissionKeyAlloc(alloc, cfg.name);
        defer alloc.free(admission_key);
        const raw = try db.core.store.get(alloc, admission_key);
        defer alloc.free(raw);
        const marker = try DB.decodeManagedIndexAdmissionMarker(raw);
        try std.testing.expectEqual(DB.IndexAdmissionDisposition.activation_fence, marker.disposition);
        try std.testing.expect(db.core.index_manager.repairUnavailable(cfg.name));
    }

    // Writable startup reconstructs durable repair debt from the admission
    // marker. The canonical generation stays gated until snapshot build,
    // replay, and root-pointer activation all finish.
    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expect(reopened.core.index_manager.algebraicIndex(cfg.name) != null);
    try std.testing.expect(reopened.core.index_manager.repairUnavailable(cfg.name));
    try std.testing.expect(try reopened.indexes.repair.hasPendingIntents(alloc));
    const repair_id = (try reopened.materializeManagedIndexAdmission(alloc, cfg.name)) orelse
        return error.TestUnexpectedResult;
    var repaired = false;
    for (0..16) |_| {
        const step = try reopened.indexes.repair.advanceIntent(alloc, repair_id, db_mod.repair_completion_test_options);
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(!step.terminal);
        try std.testing.expectEqual(@as(u64, 0), step.next_retry_at_ms);
    }
    try std.testing.expect(repaired);
    try std.testing.expect(!reopened.core.index_manager.repairUnavailable(cfg.name));
    try std.testing.expect(!try reopened.indexes.repair.hasPendingIntents(alloc));
    const admission_key = try db_mod.internal_keys.managedIndexAdmissionKeyAlloc(alloc, cfg.name);
    defer alloc.free(admission_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, admission_key));
    const active = reopened.core.index_manager.algebraicIndex(cfg.name) orelse return error.TestUnexpectedResult;
    try std.testing.expect(std.mem.startsWith(u8, active.index.storageNamespace(), ".repair-shadow-"));
}

test "db managed full text admission survives restart without in-place backfill" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const cfg = db_mod.types.IndexConfig{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    };
    var repair_checkpoint_path: ?[]u8 = null;
    defer if (repair_checkpoint_path) |value| alloc.free(value);
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
            .sync_level = .write,
        });

        const repair_id = (try db.admitManagedFullTextIndex(cfg)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(repair_id, (try db.materializeManagedIndexAdmission(alloc, cfg.name)).?);
        try std.testing.expect(try db.indexes.repair.hasPendingIntents(alloc));
        repair_checkpoint_path = try alloc.dupe(u8, db.core.index_repair_checkpoint.?.path);

        const stats = try db.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, stats);
        const target = for (stats.indexes) |item| {
            if (std.mem.eql(u8, item.name, cfg.name)) break item;
        } else return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u64, 0), target.doc_count);
    }

    // Model a crash after the atomic catalog/outbox commit but before the
    // repair checkpoint becomes durable. Reopen must reconstruct the intent
    // from the primary-store marker without running an in-place backfill.
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteFile(io_impl.io(), repair_checkpoint_path.?);

    // A read-only/status process cannot materialize the checkpoint, but the
    // primary marker must still close service during this crash window.
    var readonly = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer readonly.close();
    try std.testing.expectError(error.IndexRebuilding, readonly.search(alloc, .{
        .index_name = cfg.name,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    }));

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const reopened_stats = try reopened.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, reopened_stats);
    const reopened_target = for (reopened_stats.indexes) |item| {
        if (std.mem.eql(u8, item.name, cfg.name)) break item;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 0), reopened_target.doc_count);
    const repair_id = (try reopened.materializeManagedIndexAdmission(alloc, cfg.name)) orelse
        return error.TestUnexpectedResult;
    const summary = try reopened.indexes.repair.intentSummary(alloc);
    try std.testing.expectEqual(@as(usize, 1), summary.runnable);

    var repaired = false;
    for (0..16) |_| {
        const step = try reopened.indexes.repair.advanceIntent(alloc, repair_id, db_mod.repair_completion_test_options);
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(!step.terminal);
        try std.testing.expectEqual(@as(u64, 0), step.next_retry_at_ms);
    }
    try std.testing.expect(repaired);
    try std.testing.expect(!try reopened.indexes.repair.hasPendingIntents(alloc));
    const admission_key = try db_mod.internal_keys.managedIndexAdmissionKeyAlloc(alloc, cfg.name);
    defer alloc.free(admission_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, admission_key));

    // This reader still owns the pre-activation root. Sidecar completion is
    // not permission to expose it; cache retirement and a fresh open publish
    // the repaired generation to subsequent requests.
    try std.testing.expectError(error.IndexRebuilding, readonly.search(alloc, .{
        .index_name = cfg.name,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    }));

    var result = try reopened.search(alloc, .{
        .index_name = cfg.name,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
}

test "db full text repair page replay is idempotent without compaction" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("full-text-page-replay");
    defer tmp.cleanup();
    const path = tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const cfg: db_mod.types.IndexConfig = .{ .name = "full_text_index_v1", .kind = .full_text, .config_json = "{}" };
    db_mod.index_manager_mod.test_text_backfill_batch_size = 1;
    defer db_mod.index_manager_mod.test_text_backfill_batch_size = null;
    const Yield = struct {
        fn requested(_: *anyopaque) bool {
            return true;
        }
    };
    var token: u8 = 0;
    var options = db_mod.repair_completion_test_options;
    options.yield_check = .{ .ptr = &token, .is_requested = Yield.requested };
    var repair_id: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .open_mode = .writer_no_replay, .start_index_workers = false, .ttl_cleanup = .{ .enabled = false } });
        defer db.close();
        try db.batch(.{ .writes = &.{
            .{ .key = "a", .value = "{\"body\":\"original\"}" },
            .{ .key = "b", .value = "{\"body\":\"original\"}" },
            .{ .key = "c", .value = "{\"body\":\"original\"}" },
        }, .sync_level = .write });
        repair_id = (try db.admitManagedFullTextIndex(cfg)).?;
        const first = try db.indexes.repair.advanceIntent(alloc, repair_id, options);
        try std.testing.expect(first.busy and !first.repaired);
        var checkpoint = try db.indexes.repair.loadEntryById(alloc, repair_id);
        defer checkpoint.deinit(alloc);
        const second = try db.indexes.repair.advanceIntent(alloc, repair_id, options);
        try std.testing.expect(second.busy and !second.repaired);
        // Persist the state left by a crash between page durability and its
        // intent cursor. No later source mutations or merges may hide repeats.
        try db.indexes.repair.updateIntent(alloc, repair_id, .{
            .phase = .building,
            .build_resume_key = checkpoint.intent.build_resume_key.?,
            .replace_build_resume_key = true,
            .build_reprocessed = checkpoint.intent.build_reprocessed,
        });
    }
    var reopened = try DB.open(alloc, std.mem.span(path), .{ .open_mode = .writer_no_replay, .start_index_workers = false, .ttl_cleanup = .{ .enabled = false } });
    defer reopened.close();
    // Restart policy may change; the durable page format still owns resume.
    options.yield_check = null;
    var complete = false;
    for (0..32) |_| {
        const step = try reopened.indexes.repair.advanceIntent(alloc, repair_id, options);
        try std.testing.expect(!step.terminal);
        if (step.repaired) {
            complete = true;
            break;
        }
    }
    try std.testing.expect(complete);
    try std.testing.expectEqual(@as(u32, 3), reopened.core.index_manager.textIndex(cfg.name).?.snapshot().liveDocCount());
    var all = try reopened.search(alloc, .{ .index_name = cfg.name, .full_text = .{ .match_all = {} }, .count_only = true });
    defer all.deinit();
    try std.testing.expectEqual(@as(u32, 3), all.total_hits);
}

test "db full text repair yields resumes after reopen and catches writes behind its cursor" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("full-text-repair-slices");
    defer tmp.cleanup();
    const path = tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const cfg: db_mod.types.IndexConfig = .{ .name = "full_text_index_v1", .kind = .full_text, .config_json = "{}" };
    db_mod.index_manager_mod.test_text_backfill_batch_size = 1;
    defer db_mod.index_manager_mod.test_text_backfill_batch_size = null;
    const Yield = struct {
        fn requested(_: *anyopaque) bool {
            return true;
        }
    };
    var token: u8 = 0;
    var options = db_mod.repair_completion_test_options;
    options.yield_check = .{ .ptr = &token, .is_requested = Yield.requested };
    var repair_id: u128 = 0;
    var candidate: []u8 = undefined;
    var first_cursor: []u8 = undefined;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .open_mode = .writer_no_replay, .start_index_workers = false, .ttl_cleanup = .{ .enabled = false } });
        defer db.close();
        try db.batch(.{ .writes = &.{
            .{ .key = "a", .value = "{\"body\":\"original\"}" },
            .{ .key = "b", .value = "{\"body\":\"original\"}" },
            .{ .key = "c", .value = "{\"body\":\"original\"}" },
        }, .sync_level = .write });
        repair_id = (try db.admitManagedFullTextIndex(cfg)).?;
        const first = try db.indexes.repair.advanceIntent(alloc, repair_id, options);
        try std.testing.expect(first.attempted and first.busy and !first.repaired);
        try std.testing.expectEqual(@as(u64, 1), first.documents_reprocessed);
        var entry = try db.indexes.repair.loadEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        try std.testing.expectEqual(db_mod.index_repair_state.Phase.building, entry.intent.phase);
        try std.testing.expect(entry.intent.build_resume_key != null);
        candidate = try alloc.dupe(u8, entry.intent.candidate_relative_path.?);
        first_cursor = try alloc.dupe(u8, entry.intent.build_resume_key.?);
        // These mutations cross the saved cursor in both directions. Replay
        // from the pinned build floor must repair the mixed snapshot slices.
        try db.batch(.{ .writes = &.{
            .{ .key = "a", .value = "{\"body\":\"changed\"}" },
            .{ .key = "0", .value = "{\"body\":\"inserted\"}" },
        }, .deletes = &.{"c"}, .sync_level = .write });
    }
    defer alloc.free(candidate);
    defer alloc.free(first_cursor);
    var reopened = try DB.open(alloc, std.mem.span(path), .{ .open_mode = .writer_no_replay, .start_index_workers = false, .ttl_cleanup = .{ .enabled = false } });
    defer reopened.close();
    {
        var entry = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        try std.testing.expectEqualStrings(candidate, entry.intent.candidate_relative_path.?);
    }
    // Model a crash/cancellation after the second page is durable but before
    // its separate repair-intent cursor commits. Reopening the private index
    // must replay that page as an upsert, not append duplicate live documents.
    const second = try reopened.indexes.repair.advanceIntent(alloc, repair_id, options);
    try std.testing.expect(second.attempted and second.busy and !second.repaired);
    try reopened.indexes.repair.updateIntent(alloc, repair_id, .{
        .phase = .building,
        .build_resume_key = first_cursor,
        .replace_build_resume_key = true,
        .build_reprocessed = 1,
    });
    var complete = false;
    for (0..32) |_| {
        const step = try reopened.indexes.repair.advanceIntent(alloc, repair_id, options);
        try std.testing.expect(!step.terminal);
        if (step.repaired) {
            complete = true;
            break;
        }
    }
    if (!complete) {
        var remaining = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
        defer remaining.deinit(alloc);
        std.debug.print("full text resume incomplete phase={s} error={?s} retry={} cursor_present={} count={}\n", .{ @tagName(remaining.intent.phase), remaining.intent.last_error, remaining.intent.next_retry_at_ms, remaining.intent.build_resume_key != null, remaining.intent.build_reprocessed });
    }
    try std.testing.expect(complete);
    var all = try reopened.search(alloc, .{ .index_name = cfg.name, .full_text = .{ .match_all = {} } });
    defer all.deinit();
    try std.testing.expectEqual(@as(u32, 3), all.total_hits);
    for ([_][]const u8{ "changed", "inserted", "original" }) |word| {
        var result = try reopened.search(alloc, .{ .index_name = cfg.name, .full_text = .{ .match = .{ .field = "body", .text = word } } });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    }
}

test "db named repair advances managed full text admission without force" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    const cfg = db_mod.types.IndexConfig{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    };
    try std.testing.expect((try db.admitManagedFullTextIndex(cfg)) != null);
    try std.testing.expect(try db.indexes.repair.hasPendingIntents(alloc));

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = cfg.name,
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expect(!repair.debt_remaining);
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));

    var result = try db.search(alloc, .{
        .index_name = cfg.name,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
}

test "db managed full text admission avoids debt for an empty source" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    db_mod.index_manager_mod.test_text_backfill_invocations = 0;
    defer db_mod.index_manager_mod.test_text_backfill_invocations = 0;
    try std.testing.expect((try db.admitManagedFullTextIndex(.{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    })) == null);
    try std.testing.expectEqual(@as(usize, 0), db_mod.index_manager_mod.test_text_backfill_invocations);
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));
}

test "db ordinary index admission remains fail closed after post-commit activation failure" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    DB.test_fail_index_activation_after_catalog_commit = true;
    defer DB.test_fail_index_activation_after_catalog_commit = false;
    try db.addIndex(.{
        .name = "ft",
        .kind = .full_text,
        .config_json = "{}",
    });
    DB.test_fail_index_activation_after_catalog_commit = false;

    try std.testing.expect(db.core.index_manager.get("ft") != null);
    try std.testing.expect(try db.indexes.repair.hasPendingIntents(alloc));
    const admission_key = try db_mod.internal_keys.managedIndexAdmissionKeyAlloc(alloc, "ft");
    defer alloc.free(admission_key);
    const marker_raw = try db.core.store.get(alloc, admission_key);
    defer alloc.free(marker_raw);
    const marker = try DB.decodeManagedIndexAdmissionMarker(marker_raw);
    try std.testing.expectEqual(DB.IndexAdmissionDisposition.activation_fence, marker.disposition);
    try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
        .index_name = "ft",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    }));
}

test "db managed admission ignores stale zero identity cache" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    // Model a primary commit followed by a hot-standby mirror failure before runtime
    // cache publication. Admission authority must remain the primary store.
    db.core.identity_visibility.summary = .{};
    try std.testing.expect((try db.admitManagedFullTextIndex(.{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    })) != null);
    try std.testing.expect(try db.indexes.repair.hasPendingIntents(alloc));
}

test "db managed index deletion commits catalog absence with marker removal" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const index_name = "full_text_index_v1";

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
            .sync_level = .write,
        });
        try std.testing.expect((try db.admitManagedFullTextIndex(.{
            .name = index_name,
            .kind = .full_text,
            .config_json = "{}",
        })) != null);

        DB.test_fail_managed_index_delete_after_catalog_commit = true;
        defer DB.test_fail_managed_index_delete_after_catalog_commit = false;
        try std.testing.expectError(error.TestManagedIndexDeleteCrash, db.deleteIndex(index_name));
        DB.test_fail_managed_index_delete_after_catalog_commit = false;
    }

    // The simulated crash precedes checkpoint cleanup. Catalog and marker must
    // nevertheless agree, and writable startup reclaims the orphaned intent.
    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expect(reopened.core.index_manager.get(index_name) == null);
    const admission_key = try db_mod.internal_keys.managedIndexAdmissionKeyAlloc(alloc, index_name);
    defer alloc.free(admission_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, admission_key));
    try std.testing.expect(!try reopened.indexes.repair.hasPendingIntents(alloc));
}

test "db managed index deletion remains successful after post-commit cleanup failure" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const index_name = "full_text_index_v1";

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    const old_repair_id = (try db.admitManagedFullTextIndex(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = "{}",
    })) orelse return error.TestUnexpectedResult;

    db_mod.index_manager_mod.test_inject_index_removal_cleanup_error = error.TestPostCommitCleanup;
    defer db_mod.index_manager_mod.test_inject_index_removal_cleanup_error = null;
    db_mod.index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(std.math.maxInt(u32), .release);
    defer db_mod.index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(0, .release);
    DB.test_fail_managed_index_repair_cleanup_after_catalog_commit = true;
    defer DB.test_fail_managed_index_repair_cleanup_after_catalog_commit = false;
    try std.testing.expect(try db.deleteIndex(index_name));
    db_mod.index_manager_mod.test_inject_index_removal_cleanup_error = null;
    DB.test_fail_managed_index_repair_cleanup_after_catalog_commit = false;

    try std.testing.expect(db.core.index_manager.get(index_name) == null);
    const admission_key = try db_mod.internal_keys.managedIndexAdmissionKeyAlloc(alloc, index_name);
    defer alloc.free(admission_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, admission_key));
    try std.testing.expect(try db.indexes.repair.hasPendingIntents(alloc));

    // Same-name recreation is bounded: it reports the durable retirement debt
    // instead of scanning the corpus on the request thread. The generation
    // owner drains that debt and reconciliation can then retry admission.
    try std.testing.expectError(error.IndexArtifactCleanupPending, db.admitManagedFullTextIndex(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = "{}",
    }));
    db_mod.index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(0, .release);
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);

    const new_repair_id = (try db.admitManagedFullTextIndex(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = "{}",
    })) orelse return error.TestUnexpectedResult;
    try std.testing.expect(new_repair_id != old_repair_id);
    try std.testing.expect(db.core.index_manager.get(index_name) != null);
}

test "db generated artifact cleanup retries beyond the transient burst" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const cfg: db_mod.types.IndexConfig = .{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    };

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(cfg);
    const old_provenance = (try db.core.textIndexEntry(cfg.name).?.persistent.readGenerationMetadataAlloc(
        alloc,
        db_mod.persistent_mod.text_projection_provenance_meta_key,
    )) orelse return error.TestExpectedEqual;
    defer alloc.free(old_provenance);

    db_mod.index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(12, .release);
    defer db_mod.index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(0, .release);
    try std.testing.expect(try db.deleteIndex(cfg.name));
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);

    try std.testing.expectEqual(
        @as(u32, 0),
        db_mod.index_manager_mod.test_generated_artifact_cleanup_failures_remaining.load(.acquire),
    );
    try db.addIndex(cfg);
    const new_provenance = (try db.core.textIndexEntry(cfg.name).?.persistent.readGenerationMetadataAlloc(
        alloc,
        db_mod.persistent_mod.text_projection_provenance_meta_key,
    )) orelse return error.TestExpectedEqual;
    defer alloc.free(new_provenance);
    try std.testing.expect(!std.mem.eql(u8, old_provenance, new_provenance));
}

test "db generated artifact finalization releases page arbitration and preserves admission fence" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const first: db_mod.types.IndexConfig = .{
        .name = "a_full_text",
        .kind = .full_text,
        .config_json = "{}",
    };
    const second: db_mod.types.IndexConfig = .{
        .name = "b_full_text",
        .kind = .full_text,
        .config_json = "{}",
    };

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(first);
    try db.addIndex(second);

    DB.test_generated_artifact_finalization_entered.store(false, .release);
    DB.test_release_generated_artifact_finalization.store(false, .release);
    DB.test_block_generated_artifact_finalization.store(true, .release);
    defer {
        DB.test_release_generated_artifact_finalization.store(true, .release);
        DB.test_block_generated_artifact_finalization.store(false, .release);
    }
    try std.testing.expect(try db.deleteIndex(first.name));
    var wait_attempts: usize = 0;
    while (!DB.test_generated_artifact_finalization_entered.load(.acquire)) : (wait_attempts += 1) {
        try std.testing.expect(wait_attempts < 100_000);
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }

    try std.testing.expect(db.async_context.index_artifact_cleanup_mutex.tryLock());
    db.async_context.index_artifact_cleanup_mutex.unlock();
    try std.testing.expectError(error.IndexArtifactCleanupPending, db.addIndex(first));

    try std.testing.expect(try db.deleteIndex(second.name));
    try std.testing.expectEqual(
        DB.GeneratedArtifactCleanupAdvanceResult.progressed,
        try db.advanceGeneratedArtifactCleanupPage(second.name),
    );

    DB.test_release_generated_artifact_finalization.store(true, .release);
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);

    var second_drained = false;
    for (0..1_000) |_| {
        switch (try db.advanceGeneratedArtifactCleanupPage(second.name)) {
            .idle => {
                second_drained = true;
                break;
            },
            .progressed => {},
            .busy => std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {},
        }
    }
    try std.testing.expect(second_drained);
    try db.addIndex(first);
    try db.addIndex(second);
}

test "db managed admission materialization serializes with index deletion" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const index_name = "full_text_index_v1";

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    _ = try db.installIndexWhileEnrichmentQuiesced(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = "{}",
    }, .managed);

    const Race = struct {
        db: *DB,
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        delete_started: std.atomic.Value(bool) = .init(false),
        delete_completed: std.atomic.Value(bool) = .init(false),
        materialize_result: ?u128 = null,
        materialize_err: ?anyerror = null,
        delete_result: bool = false,
        delete_err: ?anyerror = null,

        fn afterConfigLookup(ptr: *anyopaque, _: *DB, _: []const u8) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
        }

        fn materialize(self: *@This()) void {
            self.materialize_result = self.db.materializeManagedIndexAdmission(self.db.alloc, index_name) catch |err| {
                self.materialize_err = err;
                return;
            };
        }

        fn delete(self: *@This()) void {
            self.delete_started.store(true, .release);
            self.delete_result = self.db.deleteIndex(index_name) catch |err| {
                self.delete_err = err;
                self.delete_completed.store(true, .release);
                return;
            };
            self.delete_completed.store(true, .release);
        }
    };

    var race = Race{ .db = &db };
    DB.test_managed_admission_materialization_hook = .{
        .ptr = &race,
        .after_config_lookup = Race.afterConfigLookup,
    };
    defer DB.test_managed_admission_materialization_hook = null;

    var materialize_thread = try std.testing.io.concurrent(Race.materialize, .{&race});
    var entered = false;
    for (0..100_000) |_| {
        if (race.entered.load(.acquire)) {
            entered = true;
            break;
        }
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    if (!entered) {
        race.release.store(true, .release);
        materialize_thread.await(std.testing.io);
        return error.TestTimeout;
    }

    var delete_thread = std.testing.io.concurrent(Race.delete, .{&race}) catch |err| {
        race.release.store(true, .release);
        materialize_thread.await(std.testing.io);
        return err;
    };
    var delete_started = false;
    for (0..100_000) |_| {
        if (race.delete_started.load(.acquire)) {
            delete_started = true;
            break;
        }
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    if (!delete_started) {
        race.release.store(true, .release);
        materialize_thread.await(std.testing.io);
        delete_thread.await(std.testing.io);
        return error.TestTimeout;
    }
    for (0..256) |_| std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    const deletion_crossed_materialization = race.delete_completed.load(.acquire);
    race.release.store(true, .release);
    materialize_thread.await(std.testing.io);
    delete_thread.await(std.testing.io);

    try std.testing.expect(!deletion_crossed_materialization);
    try std.testing.expect(race.materialize_err == null);
    try std.testing.expect(race.materialize_result != null);
    try std.testing.expect(race.delete_err == null);
    try std.testing.expect(race.delete_result);
    try std.testing.expect(db.core.index_manager.get(index_name) == null);
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));
}

test "db managed admission materialization never infers debt from replay lag" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{ .name = "ft", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    try std.testing.expect((try db.materializeManagedIndexAdmission(alloc, "ft")) == null);
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));
}

test "db managed admission rejects regressed identity evidence" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    const cfg = db_mod.types.IndexConfig{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    };
    _ = (try db.admitManagedFullTextIndex(cfg)) orelse return error.TestUnexpectedResult;

    const admission_key = try db_mod.internal_keys.managedIndexAdmissionKeyAlloc(alloc, cfg.name);
    defer alloc.free(admission_key);
    const raw = try db.core.store.get(alloc, admission_key);
    defer alloc.free(raw);
    var marker = try DB.decodeManagedIndexAdmissionMarker(raw);
    marker.identity_generation += 1;
    const regressed_marker = DB.encodeManagedIndexAdmissionMarker(marker);
    try db.core.store.put(admission_key, &regressed_marker);

    try std.testing.expectError(
        error.InvalidDocIdentity,
        db.materializeManagedIndexAdmission(alloc, cfg.name),
    );
    try std.testing.expect(try db.indexes.repair.hasPendingIntents(alloc));
}

test "db dense repair defers before candidate creation when node admission is exhausted" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeSentinel(u8, dense_path, 0);
    defer alloc.free(dense_path_z);
    {
        var hbc = try db_mod.hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 2,
            .storage_backend = .lsm,
        }, .{});
        try hbc.markPublishedGenerationIncompleteForTest();
        hbc.close();
    }

    var budgets = db_mod.resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(db_mod.resource_manager_mod.Slice.dense_repair_working_set)] = .{
        .soft_limit_bytes = 2 * 1024 * 1024,
        .hard_limit_bytes = 4 * 1024 * 1024,
    };
    var resources = db_mod.resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer resources.deinit(alloc);
    var reopened = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .resource_manager = &resources,
    });
    defer reopened.close();
    _ = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    const repair_id = (try reopened.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    const deferred = try reopened.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(deferred.attempted);
    try std.testing.expect(deferred.deferred);
    try std.testing.expect(!deferred.repaired);

    var entry = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
    defer entry.deinit(alloc);
    try std.testing.expect(entry.intent.candidate_relative_path == null);
    try std.testing.expectEqualStrings("RepairResourceUnavailable", entry.intent.last_error.?);
    try std.testing.expectEqual(@as(u32, 1), entry.intent.attempt_count);
    try std.testing.expectEqual(@as(u32, 1), entry.intent.failure_streak);
    const first_retry_at_ms = entry.intent.next_retry_at_ms;
    const admission = resources.sliceStats(.dense_repair_working_set);
    try std.testing.expectEqual(@as(u64, 0), admission.used_bytes);
    try std.testing.expectEqual(@as(u64, 1), admission.hard_limit_rejections);
    // This fixture is explicitly legacy-v1, so its injected interrupted
    // publication is represented by the durable LSM bulk marker. Native-v2
    // uses a missing CURRENT/checkpoint for the equivalent fail-closed state.
    try std.testing.expectError(error.IncompleteBulkPublish, db_mod.hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
        .dims = 2,
        .storage_backend = .lsm,
    }, .{}));

    // A persisted retry does not reset its consecutive-failure backoff across
    // executor passes or process restart.
    try reopened.indexes.repair.updateIntent(alloc, repair_id, .{ .next_retry_at_ms = 0 });
    const second = try reopened.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(second.attempted);
    try std.testing.expect(second.deferred);
    var retried = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
    defer retried.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), retried.intent.attempt_count);
    try std.testing.expectEqual(@as(u32, 2), retried.intent.failure_streak);
    try std.testing.expect(retried.intent.next_retry_at_ms > first_retry_at_ms);
}

test "db dense repair uses resource manager capacity admission before building" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
        .sync_level = .full_index,
    });

    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try db.indexes.repair.createOperatorGenerationIntent(alloc, cfg.*, 0, 0);
    const deferred = try db.indexes.repair.advanceIntent(alloc, repair_id, .{
        .capacity_domain_id = 77,
        .capacity_observation = .{ .available_bytes = 1, .capacity_bytes = 1 },
    });
    try std.testing.expect(deferred.deferred);
    try std.testing.expect(deferred.disk_wait);
    try std.testing.expect(!deferred.attempted);

    var entry = try db.indexes.repair.loadEntryById(alloc, repair_id);
    try std.testing.expect(entry.intent.candidate_relative_path == null);
    try std.testing.expectEqualStrings("disk_admission_unavailable", entry.intent.last_error.?);
    const first_estimate = entry.intent.estimated_candidate_bytes;
    const first_reservation = entry.intent.planned_disk_bytes;
    entry.deinit(alloc);
    const admission = db.core.index_manager.resource_manager.?.capacityStats();
    try std.testing.expectEqual(@as(u64, 0), admission.reserved_bytes);
    try std.testing.expectEqual(@as(u64, 1), admission.denials);

    // A durable intent must refresh upward when the live generation or the
    // scheduler's estimate grows between retries.
    try db.indexes.repair.updateIntent(alloc, repair_id, .{ .next_retry_at_ms = 0 });
    const larger_estimate = first_estimate +| 1024 * 1024 * 1024;
    const retried = try db.indexes.repair.advanceIntent(alloc, repair_id, .{
        .estimated_candidate_bytes = larger_estimate,
        .capacity_domain_id = 77,
        .capacity_observation = .{ .available_bytes = 1, .capacity_bytes = 1 },
    });
    try std.testing.expect(retried.disk_wait);
    var refreshed = try db.indexes.repair.loadEntryById(alloc, repair_id);
    defer refreshed.deinit(alloc);
    try std.testing.expectEqual(larger_estimate, refreshed.intent.estimated_candidate_bytes);
    try std.testing.expect(refreshed.intent.planned_disk_bytes > first_reservation);
}

test "db dense replay failure upgrades a preflight validation intent" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"cosine\",\"external\":true}",
    });
    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try db.indexes.repair.createOperatorGenerationIntent(alloc, cfg.*, 0, 0);
    try db.indexes.repair.updateIntent(alloc, repair_id, .{
        .phase = .preflight,
        .last_error = "OperatorGenerationValidationPending",
        .replace_last_error = true,
    });

    try std.testing.expect(try db.markDenseReplayArtifactRepairRequired(alloc, cfg.name));
    var upgraded = try db.indexes.repair.loadEntryById(alloc, repair_id);
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.projection_generation_invalid, upgraded.intent.trigger);
    try std.testing.expectEqual(db_mod.index_repair_state.Phase.preflight, upgraded.intent.phase);
    try std.testing.expectEqualStrings("dense_replay_artifact_missing", upgraded.intent.last_error.?);
    try std.testing.expectEqual(@as(u64, 0), upgraded.intent.next_retry_at_ms);
    upgraded.deinit(alloc);

    try db.indexes.repair.updateIntent(alloc, repair_id, .{
        .trigger = .operator_generation_validation,
        .last_error = "OperatorGenerationValidationPending",
        .replace_last_error = true,
    });
    try db.markDenseReplayArtifactCoverageRepairRequired(alloc, cfg.name);
    upgraded = try db.indexes.repair.loadEntryById(alloc, repair_id);
    defer upgraded.deinit(alloc);
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.artifact_coverage_mismatch, upgraded.intent.trigger);
    try std.testing.expectEqualStrings("dense_replay_artifact_unreadable", upgraded.intent.last_error.?);
}

test "db dense replay missing artifact remains a normal managed admission dependency" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    const cfg = db_mod.types.IndexConfig{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\"}}",
    };
    const repair_id = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;

    try std.testing.expect(!try db.markDenseReplayArtifactRepairRequired(alloc, cfg.name));
    var retained = try db.indexes.repair.loadEntryById(alloc, repair_id);
    defer retained.deinit(alloc);
    try std.testing.expectEqual(db_mod.index_repair_state.WorkClass.initial_build, retained.intent.work_class);
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.catalog_admission, retained.intent.trigger);
    try std.testing.expectEqual(db_mod.index_repair_state.Phase.detected, retained.intent.phase);
    try std.testing.expectEqualStrings("managed_catalog_admission_rebuild", retained.intent.last_error.?);
}

test "db durable repair classification emits exact admission and action edges" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "search_idx",
        .kind = .full_text,
        .config_json = "{}",
    });
    const cfg = db.core.index_manager.get("search_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try db.indexes.repair.createGenerationIntent(
        alloc,
        cfg.*,
        .operator_generation_rebuild,
        0,
        0,
        null,
    );
    const serviceable_stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, serviceable_stats);
    var observed_serviceable_rebuild = false;
    for (serviceable_stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "search_idx")) continue;
        observed_serviceable_rebuild = true;
        try std.testing.expectEqual(db_mod.types.IndexRepairStatus.rebuilding, index_stats.index_repair_status.?);
        try std.testing.expect(index_stats.index_repair_active_generation_serviceable);
    }
    try std.testing.expect(observed_serviceable_rebuild);

    const Capture = struct {
        count: usize = 0,
        repair_id: u128 = 0,
        previous_admission: db_mod.IndexRepairAdmission = .unknown,
        admission: db_mod.IndexRepairAdmission = .unknown,
        previous_action_required: bool = false,
        action_required: bool = false,

        fn onChange(ptr: *anyopaque, event: db_mod.QueryVisibilityEvent) void {
            if (event.change != .index_repair_pending) return;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const repair = event.repair orelse return;
            self.count += 1;
            self.repair_id = repair.repair_id;
            self.previous_admission = repair.previous_admission;
            self.admission = repair.admission;
            self.previous_action_required = repair.previous_action_required;
            self.action_required = repair.action_required;
        }
    };
    var capture = Capture{};
    db.setQueryVisibilityHook(.{
        .ptr = &capture,
        .on_change = Capture.onChange,
    });
    // Hook attachment replays the current durable level so a newly resident
    // owner cannot miss repair debt created before it installed the callback.
    try std.testing.expectEqual(@as(usize, 1), capture.count);
    try std.testing.expectEqual(repair_id, capture.repair_id);
    capture = .{};

    try db.indexes.repair.updateIntent(alloc, repair_id, .{
        .trigger = .projection_generation_invalid,
        .last_error = "projection_identity_invalid",
        .replace_last_error = true,
    });
    try std.testing.expectEqual(@as(usize, 1), capture.count);
    try std.testing.expectEqual(repair_id, capture.repair_id);
    try std.testing.expectEqual(db_mod.IndexRepairAdmission.serviceable, capture.previous_admission);
    try std.testing.expectEqual(db_mod.IndexRepairAdmission.blocked, capture.admission);

    // Diagnostic progress alone does not create visibility churn.
    try db.indexes.repair.updateIntent(alloc, repair_id, .{
        .last_error = "more_specific_projection_identity_invalid",
        .replace_last_error = true,
    });
    try std.testing.expectEqual(@as(usize, 1), capture.count);

    try db.indexes.repair.updateIntent(alloc, repair_id, .{ .automation = .paused });
    try std.testing.expectEqual(@as(usize, 2), capture.count);
    try std.testing.expectEqual(db_mod.IndexRepairAdmission.blocked, capture.previous_admission);
    try std.testing.expectEqual(db_mod.IndexRepairAdmission.blocked, capture.admission);
    try std.testing.expect(!capture.previous_action_required);
    try std.testing.expect(capture.action_required);
}

test "db forced generation repair completion is crash idempotent before api acknowledgement" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try db.indexes.repair.createOperatorGenerationIntent(alloc, cfg.*, 42, 1_234_567);
    inline for ([_]db_mod.index_repair_state.Phase{
        .preflight,
        .building,
        .catching_up,
        .ready,
        .activating,
        .validating,
        .cleanup,
    }) |phase| try db.indexes.repair.updateIntent(alloc, repair_id, .{ .phase = phase });
    try db.indexes.repair.removeIntentAndPin(alloc, repair_id);

    // Simulate an API process crash after the group completed but before its
    // repair job recorded the pass. Redispatch of the stable job identity must
    // observe completion instead of creating a second full generation.
    const replayed = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
        .repair_job_id = 42,
        .repair_job_created_at_ms = 1_234_567,
    });
    try std.testing.expectEqual(@as(u64, 1), replayed.repaired);
    try std.testing.expectEqual(@as(u64, 1), replayed.indexes_rebuilt);
    try std.testing.expect((try db.indexes.repair.idForIndex(alloc, "dense_idx")) == null);
}

test "db incomplete HBC snapshot persists generation repair before maintenance" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var repair_id: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        const dense = db.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult;

        const stale_generation = dense.index.publishedGeneration();
        // Native refresh of an identical SearchView is intentionally a no-op.
        // Commit an actual generation before reporting a delayed old failure.
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
        try std.testing.expect(dense.index.publishedGeneration() != stale_generation);
        dense.index.noteIncompletePublishedSnapshotForGeneration(stale_generation);
        try std.testing.expect(!try db.persistIncompleteHbcSnapshotRepair(dense));
        try std.testing.expect((try db.indexes.repair.idForIndex(alloc, "dense_idx")) == null);
        try std.testing.expect(!db.core.index_manager.repairUnavailable("dense_idx"));

        dense.index.noteIncompletePublishedSnapshot();

        try std.testing.expect(try db.persistIncompleteHbcSnapshotRepair(dense));
        try std.testing.expect(db.core.index_manager.repairUnavailable("dense_idx"));
        repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
        var repair = try db.indexes.repair.loadEntryById(alloc, repair_id);
        defer repair.deinit(alloc);
        try std.testing.expectEqual(db_mod.index_repair_state.Trigger.projection_generation_invalid, repair.intent.trigger);
        try std.testing.expectEqualStrings("dense_hbc_published_snapshot_incomplete", repair.intent.last_error.?);
    }

    // The query-path decision must survive a process boundary even if the
    // periodic repair-maintenance pass never ran in the detecting process.
    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expectEqual(
        repair_id,
        (try reopened.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult,
    );
    try std.testing.expect(reopened.core.index_manager.repairUnavailable("dense_idx"));
}

test "db forced repair attaches to automatic generation intent idempotently" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try db.indexes.repair.ensureAutomaticDenseGenerationIntent(
        alloc,
        cfg.*,
        .artifact_coverage_mismatch,
        "dense_artifact_coverage_surplus",
    );

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
        .repair_job_id = 77,
        .repair_job_created_at_ms = 8_765_432,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), queued.in_progress);

    var attached = try db.indexes.repair.loadEntryById(alloc, repair_id);
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.artifact_coverage_mismatch, attached.intent.trigger);
    try std.testing.expectEqual(@as(u64, 77), attached.intent.operator_job_id);
    try std.testing.expectEqual(@as(u64, 8_765_432), attached.intent.operator_job_created_at_ms);
    attached.deinit(alloc);

    try std.testing.expectEqual(repair_id, try db.indexes.repair.ensureAutomaticDenseGenerationIntent(
        alloc,
        cfg.*,
        .projection_generation_invalid,
        "dense_projection_structure_invalid",
    ));
    var promoted = try db.indexes.repair.loadEntryById(alloc, repair_id);
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.projection_generation_invalid, promoted.intent.trigger);
    try std.testing.expectEqual(@as(u64, 77), promoted.intent.operator_job_id);
    promoted.deinit(alloc);
    try std.testing.expect(db.core.index_manager.repairUnavailable("dense_idx"));

    inline for ([_]db_mod.index_repair_state.Phase{
        .preflight,
        .building,
        .catching_up,
        .ready,
        .activating,
        .validating,
        .cleanup,
    }) |phase| try db.indexes.repair.updateIntent(alloc, repair_id, .{ .phase = phase });
    try db.indexes.repair.removeIntentAndPin(alloc, repair_id);

    const replayed = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
        .repair_job_id = 77,
        .repair_job_created_at_ms = 8_765_432,
    });
    try std.testing.expectEqual(@as(u64, 1), replayed.repaired);
    try std.testing.expectEqual(@as(u64, 1), replayed.indexes_rebuilt);
    try std.testing.expect((try db.indexes.repair.idForIndex(alloc, "dense_idx")) == null);
}

test "db forced repair preserves missing-counter fail-closed classification" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}",
        }},
        .sync_level = .full_index,
    });
    const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_idx");
    defer alloc.free(counter_key);
    try db.core.store.delete(counter_key);

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
        .repair_job_id = 88,
        .repair_job_created_at_ms = 9_876_543,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), queued.in_progress);

    const repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    var intent = try db.indexes.repair.loadEntryById(alloc, repair_id);
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.artifact_counter_missing, intent.intent.trigger);
    try std.testing.expectEqual(@as(u64, 88), intent.intent.operator_job_id);
    intent.deinit(alloc);
    try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    }));
}

test "db removing one repair pin preserves pressure gate for another index" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    for ([_][]const u8{ "dense_a", "dense_b" }) |name| {
        try db.addIndex(.{
            .name = name,
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
    }
    const Hook = struct {
        cleared_a: usize = 0,
        clear_reported_remaining_debt: bool = false,
        pending_unknown: usize = 0,
        pending_exact: usize = 0,

        fn onChange(ptr: *anyopaque, event: db_mod.QueryVisibilityEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            switch (event.change) {
                .index_repair_cleared => if (event.repair) |repair| {
                    self.cleared_a += @intFromBool(std.mem.eql(u8, repair.index_name, "dense_a"));
                    self.clear_reported_remaining_debt = event.group_repair_debt_remains;
                },
                .index_repair_pending => if (event.repair == null) {
                    self.pending_unknown += 1;
                } else {
                    self.pending_exact += 1;
                },
                else => {},
            }
        }
    };
    var hook = Hook{};
    db.setQueryVisibilityHook(.{
        .ptr = &hook,
        .on_change = Hook.onChange,
    });
    defer db.setQueryVisibilityHook(null);
    const cfg_a = db.core.index_manager.get("dense_a") orelse return error.TestUnexpectedResult;
    const cfg_b = db.core.index_manager.get("dense_b") orelse return error.TestUnexpectedResult;
    const repair_a = try db.indexes.repair.createOperatorGenerationIntent(alloc, cfg_a.*, 0, 0);
    const repair_b = try db.indexes.repair.createOperatorGenerationIntent(alloc, cfg_b.*, 0, 0);
    var snapshot_a = try db.indexes.repair.beginPinnedSnapshot(alloc, repair_a);
    snapshot_a.deinit();
    var snapshot_b = try db.indexes.repair.beginPinnedSnapshot(alloc, repair_b);
    snapshot_b.deinit();

    hook = .{};
    try db.indexes.repair.removeIntentAndPin(alloc, repair_a);
    try std.testing.expect(db.async_context.index_repair_replay_pinned.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), hook.cleared_a);
    try std.testing.expect(hook.clear_reported_remaining_debt);
    try std.testing.expectEqual(@as(usize, 0), hook.pending_unknown);
    try std.testing.expectEqual(@as(usize, 0), hook.pending_exact);
    var remaining = try db.indexes.repair.loadState(alloc);
    try std.testing.expect(remaining.minimumRetainAfterSequence() != null);
    remaining.deinit(alloc);

    // Hook churn must rehydrate the exact remaining intent. The clear's
    // aggregate scheduling bit cannot degrade back into anonymous repair
    // scope, which would conservatively revoke every cached incarnation.
    db.setQueryVisibilityHook(null);
    hook = .{};
    db.setQueryVisibilityHook(.{
        .ptr = &hook,
        .on_change = Hook.onChange,
    });
    try std.testing.expectEqual(@as(usize, 0), hook.pending_unknown);
    try std.testing.expectEqual(@as(usize, 1), hook.pending_exact);

    try db.indexes.repair.removeIntentAndPin(alloc, repair_b);
    try std.testing.expect(!db.async_context.index_repair_replay_pinned.load(.acquire));
}

test "db healthy dense generation remains searchable until replacement activation" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1]}}" },
        },
        .sync_level = .full_index,
    });

    const StopBeforeActivation = struct {
        fn afterSnapshot(_: *anyopaque, hook_db: *DB, _: []const u8, _: u64) !void {
            var result = try hook_db.search(hook_db.alloc, .{
                .index_name = "dense_idx",
                .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
                .limit = 2,
            });
            defer result.deinit();
            try std.testing.expectEqual(@as(u32, 2), result.total_hits);
            return error.TestStopGenerationBeforeActivation;
        }
    };
    var hook_ctx: u8 = 0;
    db.indexes.repair.shadow_hook = .{
        .ptr = &hook_ctx,
        .after_snapshot_build = StopBeforeActivation.afterSnapshot,
    };
    try std.testing.expectError(error.TestStopGenerationBeforeActivation, db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .embedding,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
    }));
    db.indexes.repair.shadow_hook = null;

    const generation_repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    var generation_state = try db.indexes.repair.loadEntryById(alloc, generation_repair_id);
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.operator_generation_rebuild, generation_state.intent.trigger);
    try std.testing.expectEqual(db_mod.index_repair_state.Phase.catching_up, generation_state.intent.phase);
    try std.testing.expect(generation_state.pin != null);
    try std.testing.expect(generation_state.intent.candidate_relative_path != null);
    generation_state.deinit(alloc);
    {
        const stats = try db.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, stats);
        const dense_stats = for (stats.indexes) |item| {
            if (std.mem.eql(u8, item.name, "dense_idx")) break item;
        } else return error.TestUnexpectedResult;
        try std.testing.expect(dense_stats.backfill_active);
        try std.testing.expect(!dense_stats.repair_degraded);
    }

    {
        var result = try db.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
            .limit = 2,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 2), result.total_hits);
        try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    }

    var rebuilt = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .embedding,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
    });
    defer rebuilt.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), rebuilt.indexes_rebuilt);
    // The interrupted explicit generation is durable and resumable; the
    // second request catches up and activates it without rescanning the corpus.
    try std.testing.expectEqual(@as(u64, 0), rebuilt.reprocessed);
    try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));

    var result = try db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db forced dense repair stays fail closed until background health proof" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
        .sync_level = .full_index,
    });

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    const repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    var pending = try db.indexes.repair.loadEntryById(alloc, repair_id);
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.operator_generation_validation, pending.intent.trigger);
    pending.deinit(alloc);
    try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
    }));

    const AfterValidatedSnapshot = struct {
        fn run(_: *anyopaque, hook_db: *DB, _: []const u8, _: u64) !void {
            const current_id = (try hook_db.indexes.repair.idForIndex(hook_db.alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
            var current = try hook_db.indexes.repair.loadEntryById(hook_db.alloc, current_id);
            defer current.deinit(hook_db.alloc);
            try std.testing.expectEqual(db_mod.index_repair_state.Trigger.operator_generation_rebuild, current.intent.trigger);
            var result = try hook_db.search(hook_db.alloc, .{
                .index_name = "dense_idx",
                .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
            });
            defer result.deinit();
            try std.testing.expectEqual(@as(u32, 1), result.total_hits);
            return error.TestStopAfterOperatorGenerationValidation;
        }
    };
    var hook_ctx: u8 = 0;
    db.indexes.repair.shadow_hook = .{ .ptr = &hook_ctx, .after_snapshot_build = AfterValidatedSnapshot.run };
    try std.testing.expectError(
        error.TestStopAfterOperatorGenerationValidation,
        db.indexes.repair.advanceIntent(alloc, repair_id, .{}),
    );
    db.indexes.repair.shadow_hook = null;
}

test "db forced dense repair keeps structurally invalid generation fail closed" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true,\"leaf_size\":2}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}" },
            .{ .key = "doc:b", .value = "{\"_embeddings\":{\"dense_idx\":[0,1]}}" },
            .{ .key = "doc:c", .value = "{\"_embeddings\":{\"dense_idx\":[0.9,0.1]}}" },
            .{ .key = "doc:d", .value = "{\"_embeddings\":{\"dense_idx\":[0.1,0.9]}}" },
        },
        .sync_level = .full_index,
    });
    const dense_entry = db.core.index_manager.denseIndex("dense_idx").?;
    var read_txn = try dense_entry.index.beginReadTxn();
    var root = try dense_entry.index.loadNode(&read_txn, dense_entry.index.metadata.root_node);
    read_txn.abort();
    defer root.deinit(alloc);
    try std.testing.expect(!root.is_leaf);
    try dense_entry.index.deleteNodeHeaderForTest(root.children[0]);

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    const repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;

    const AfterInvalidSnapshot = struct {
        fn run(_: *anyopaque, hook_db: *DB, _: []const u8, _: u64) !void {
            const current_id = (try hook_db.indexes.repair.idForIndex(hook_db.alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
            var current = try hook_db.indexes.repair.loadEntryById(hook_db.alloc, current_id);
            defer current.deinit(hook_db.alloc);
            try std.testing.expectEqual(db_mod.index_repair_state.Trigger.projection_generation_invalid, current.intent.trigger);
            try std.testing.expectError(error.IndexRebuilding, hook_db.search(hook_db.alloc, .{
                .index_name = "dense_idx",
                .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
            }));
            return error.TestStopAfterInvalidOperatorGenerationValidation;
        }
    };
    var hook_ctx: u8 = 0;
    db.indexes.repair.shadow_hook = .{ .ptr = &hook_ctx, .after_snapshot_build = AfterInvalidSnapshot.run };
    try std.testing.expectError(
        error.TestStopAfterInvalidOperatorGenerationValidation,
        db.indexes.repair.advanceIntent(alloc, repair_id, .{}),
    );
    db.indexes.repair.shadow_hook = null;
}

test "db restart automatically resumes pinned explicit dense generation rebuild" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1]}}" },
            },
            .sync_level = .full_index,
        });

        const StopAtReady = struct {
            fn afterSnapshot(_: *anyopaque, _: *DB, _: []const u8, _: u64) !void {}

            fn afterPhase(_: *anyopaque, _: *DB, _: u128, phase: db_mod.index_repair_state.Phase) !void {
                if (phase == .ready) return error.TestStopExplicitGenerationAtReady;
            }
        };
        var hook_ctx: u8 = 0;
        db.indexes.repair.shadow_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = StopAtReady.afterSnapshot,
            .after_phase_persisted = StopAtReady.afterPhase,
        };
        try std.testing.expectError(error.TestStopExplicitGenerationAtReady, db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .embedding,
            .index_name = "dense_idx",
            .limit = 1,
            .force = true,
        }));
        db.indexes.repair.shadow_hook = null;
        try std.testing.expect(try db.indexes.repair.hasPendingIntents(alloc));
        const repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
        var ready = try db.indexes.repair.loadEntryById(alloc, repair_id);
        defer ready.deinit(alloc);
        try std.testing.expectEqual(db_mod.index_repair_state.Phase.ready, ready.intent.phase);

        // The active generation remains serviceable before restart.
        var active = try db.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
            .limit = 2,
        });
        active.deinit();
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    // Reloading the durable intent must not quarantine the prior healthy
    // generation while the candidate is still in catching_up.
    var before_resume = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
        .limit = 2,
    });
    before_resume.deinit();

    const resumed = try reopened.repairRecoverableStartupIndexFailures(alloc, 1, .{});
    try std.testing.expectEqual(@as(usize, 1), resumed.attempted);
    try std.testing.expectEqual(@as(usize, 1), resumed.repaired);
    try std.testing.expectEqual(@as(u64, 0), resumed.documents_reprocessed);
    try std.testing.expect(!try reopened.indexes.repair.hasPendingIntents(alloc));

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db restart clears stale dense generation intent after clean checkpoint" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var repair_id: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });

        const CrashHook = struct {
            fn afterSnapshot(_: *anyopaque, _: *DB, _: []const u8, _: u64) !void {}
            fn afterCleanCheckpoint(_: *anyopaque, _: *DB, _: []const u8) !void {
                return error.TestCrashAfterCleanCheckpoint;
            }
        };
        var hook_ctx: u8 = 0;
        db.indexes.repair.shadow_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = CrashHook.afterSnapshot,
            .after_clean_checkpoint = CrashHook.afterCleanCheckpoint,
        };
        try std.testing.expectError(error.TestCrashAfterCleanCheckpoint, db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .embedding,
            .index_name = "dense_idx",
            .limit = 1,
            .force = true,
        }));
        db.indexes.repair.shadow_hook = null;

        repair_id = (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
        var interrupted = try db.indexes.repair.loadEntryById(alloc, repair_id);
        defer interrupted.deinit(alloc);
        try std.testing.expectEqual(db_mod.index_repair_state.Phase.validating, interrupted.intent.phase);
        try std.testing.expect(interrupted.pin != null);
        try std.testing.expect(interrupted.intent.previous_pointer_captured);
        // Native-v2 indexes always publish through a managed root pointer.
        // Repair activation must retain the distinct predecessor so a crash
        // before certification remains rollback-safe.
        const predecessor = interrupted.intent.previous_active_relative_path orelse
            return error.TestUnexpectedResult;
        try std.testing.expect(!std.mem.eql(
            u8,
            predecessor,
            interrupted.intent.candidate_relative_path.?,
        ));
        try std.testing.expect(try db.core.index_manager.isRepairCandidateActive(
            "dense_idx",
            interrupted.intent.candidate_relative_path.?,
        ));
        const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "dense_idx");
        try std.testing.expectEqual(db_mod.apply_state.ProjectionStatus.clean, checkpoint.status);
        try std.testing.expectEqual(interrupted.intent.config_hash, checkpoint.config_hash);
        try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
            .limit = 1,
        }));
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    var reopened_closed = false;
    defer if (!reopened_closed) reopened.close();
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    }));

    var before_reconcile = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
    const certified_sequence = before_reconcile.intent.candidate_applied_sequence;
    before_reconcile.deinit(alloc);
    try reopened.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1]}}" }},
        .sync_level = .full_index,
    });
    const progressed_checkpoint = try reopened.core.loadProjectionCheckpoint(alloc, "dense_idx");
    try std.testing.expect(progressed_checkpoint.applied_sequence > certified_sequence);

    const RetirementCrash = struct {
        fn beforeRetirement(_: *anyopaque, _: *DB, _: u128) !void {
            return error.TestCrashBeforeRepairRetirement;
        }
    };
    var retirement_hook_ctx: u8 = 0;
    DB.test_index_repair_reconcile_fence_hook = .{
        .ptr = &retirement_hook_ctx,
        .before_retirement = RetirementCrash.beforeRetirement,
    };
    defer DB.test_index_repair_reconcile_fence_hook = null;
    try std.testing.expectError(
        error.TestCrashBeforeRepairRetirement,
        reopened.indexes.repair.advanceIntent(alloc, repair_id, .{}),
    );
    DB.test_index_repair_reconcile_fence_hook = null;

    var cleanup = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
    defer cleanup.deinit(alloc);
    try std.testing.expectEqual(db_mod.index_repair_state.Phase.cleanup, cleanup.intent.phase);
    // The manifest certification is immutable even though the serving
    // checkpoint advanced before retirement. A retry can therefore validate
    // the same installed incarnation and finish idempotently.
    try std.testing.expectEqual(certified_sequence, cleanup.intent.candidate_applied_sequence);
    try std.testing.expectEqual(progressed_checkpoint.applied_sequence, cleanup.intent.target_sequence);

    reopened.close();
    reopened_closed = true;
    var final_reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer final_reopened.close();

    const reconciled = try final_reopened.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(reconciled.attempted);
    try std.testing.expect(reconciled.repaired);
    try std.testing.expectEqual(@as(u64, 0), reconciled.documents_reprocessed);
    try std.testing.expect(!try final_reopened.indexes.repair.hasPendingIntents(alloc));

    var result = try final_reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db missing activation certification rolls back to serviceable dense predecessor" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
        .sync_level = .full_index,
    });

    var interrupted = try db_mod.interruptDenseRepairAfterActivation(alloc, &db);
    defer interrupted.deinit(alloc);
    const manifest_path = try std.fmt.allocPrint(
        alloc,
        "{s}/{s}/.antfly-index-generation",
        .{ std.mem.span(path), interrupted.candidate_relative_path },
    );
    defer alloc.free(manifest_path);
    try std.Io.Dir.cwd().deleteFile(std.testing.io, manifest_path);

    const resumed = try db.indexes.repair.advanceIntent(alloc, interrupted.repair_id, .{});
    try std.testing.expect(resumed.attempted);
    try std.testing.expect(resumed.deferred);
    try std.testing.expect(!resumed.terminal);
    try std.testing.expect(db.core.index_manager.failedIndexLoadSnapshot("dense_idx") == null);

    var reset = try db.indexes.repair.loadEntryById(alloc, interrupted.repair_id);
    defer reset.deinit(alloc);
    try std.testing.expectEqual(db_mod.index_repair_state.Phase.preflight, reset.intent.phase);
    try std.testing.expect(reset.intent.candidate_relative_path == null);

    var result = try db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db missing activation certification exposes predecessor action required" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
        .sync_level = .full_index,
    });

    var interrupted = try db_mod.interruptDenseRepairAfterActivation(alloc, &db);
    defer interrupted.deinit(alloc);
    const manifest_path = try std.fmt.allocPrint(
        alloc,
        "{s}/{s}/.antfly-index-generation",
        .{ std.mem.span(path), interrupted.candidate_relative_path },
    );
    defer alloc.free(manifest_path);
    try std.Io.Dir.cwd().deleteFile(std.testing.io, manifest_path);

    const CrashAfterRollback = struct {
        fn beforeRetry(_: *anyopaque, _: *DB, _: u128) !void {
            return error.TestCrashAfterUncertifiedRollback;
        }
    };
    var rollback_hook_ctx: u8 = 0;
    DB.test_index_repair_rollback_retry_hook = .{
        .ptr = &rollback_hook_ctx,
        .before_retry = CrashAfterRollback.beforeRetry,
    };
    defer DB.test_index_repair_rollback_retry_hook = null;
    try std.testing.expectError(
        error.TestCrashAfterUncertifiedRollback,
        db.indexes.repair.advanceIntent(alloc, interrupted.repair_id, .{}),
    );
    DB.test_index_repair_rollback_retry_hook = null;
    var rolling_back = try db.indexes.repair.loadEntryById(alloc, interrupted.repair_id);
    defer rolling_back.deinit(alloc);
    try std.testing.expectEqual(db_mod.index_repair_state.Phase.rolling_back, rolling_back.intent.phase);
    try std.testing.expectEqualStrings("activation_manifest_missing", rolling_back.intent.last_error.?);
    try std.testing.expect(!try db.core.index_manager.isRepairCandidateActive(
        "dense_idx",
        interrupted.candidate_relative_path,
    ));
    try std.testing.expectEqual(
        interrupted.repair_id,
        db.core.index_manager.failedIndexLoadSnapshot("dense_idx").?.claim_owner_repair_id.?,
    );

    db_mod.index_manager_mod.test_inject_index_open_error = error.InvalidIndexConfig;
    defer db_mod.index_manager_mod.test_inject_index_open_error = null;
    const resumed = try db.indexes.repair.advanceIntent(alloc, interrupted.repair_id, .{});
    db_mod.index_manager_mod.test_inject_index_open_error = null;
    try std.testing.expect(resumed.attempted);
    try std.testing.expect(resumed.terminal);

    var terminal = try db.indexes.repair.loadEntryById(alloc, interrupted.repair_id);
    defer terminal.deinit(alloc);
    try std.testing.expectEqual(db_mod.index_repair_state.Phase.terminal, terminal.intent.phase);
    try std.testing.expectEqualStrings("activation_manifest_missing", terminal.intent.last_error.?);
    try std.testing.expect(terminal.intent.candidate_relative_path == null);
    try std.testing.expect(db_mod.RepairIndexes.actionRequired(terminal.intent));

    const failure = db.core.index_manager.failedIndexLoadSnapshot("dense_idx") orelse
        return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("InvalidIndexConfig", failure.err_name);
    try std.testing.expectEqual(
        db_mod.index_manager_mod.IndexManager.IndexLoadRecoveryAction.manual_intervention,
        failure.token.action,
    );
    try std.testing.expect(failure.claim_owner_repair_id == null);
    const terminal_stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, terminal_stats);
    var observed_terminal_status = false;
    for (terminal_stats.indexes) |item| {
        if (!std.mem.eql(u8, item.name, "dense_idx")) continue;
        observed_terminal_status = true;
        try std.testing.expectEqual(db_mod.types.IndexRepairStatus.failed, item.index_repair_status.?);
        try std.testing.expect(item.index_repair_action_required);
        try std.testing.expectEqualStrings("activation_manifest_missing", item.index_repair_last_error.?);
        try std.testing.expectEqualStrings("InvalidIndexConfig", item.load_error.?);
    }
    try std.testing.expect(observed_terminal_status);
    try std.testing.expectError(error.IndexUnavailable, db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    }));
}

test "db dense artifact rebuild preserves stable vector ids distinct from ordinals" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            },
            .sync_level = .full_index,
        });

        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try db_mod.doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b")) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(db_mod.doc_identity.DocOrdinal, 2), ordinal);
        const stable_vector_id = db_mod.denseTestVectorId("doc:b");
        try std.testing.expect(stable_vector_id != @as(u64, ordinal));
        try std.testing.expectEqual(@as(?u64, stable_vector_id), try db.core.index_manager.lookupDenseVectorId(db.core.store, "dense_idx", "doc:b"));

        // Exercise the production reconstruction path. New dense indexes are
        // pointer-selected native generations, so deleting the legacy
        // canonical directory is no longer a meaningful rebuild trigger.
        const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
        const repair_id = try db.indexes.repair.createGenerationIntent(
            alloc,
            cfg.*,
            .operator_generation_rebuild,
            0,
            0,
            null,
        );
        var repaired: DB.IndexRepairAdvanceResult = undefined;
        var documents_reprocessed: u64 = 0;
        for (0..4) |_| {
            repaired = try db.indexes.repair.advanceIntent(alloc, repair_id, db_mod.repair_completion_test_options);
            documents_reprocessed +|= repaired.documents_reprocessed;
            if (repaired.repaired) break;
            try std.testing.expect(repaired.deferred or repaired.busy);
            try std.testing.expect(!repaired.terminal);
            try std.testing.expectEqual(@as(u64, 0), repaired.next_retry_at_ms);
        }
        try std.testing.expect(repaired.repaired);
        try std.testing.expectEqual(@as(u64, 2), documents_reprocessed);
        try std.testing.expect(!try db.indexes.repair.hasPendingIntents(alloc));
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
    });
    defer reopened.close();

    var txn = try reopened.core.store.beginProbeTxn();
    defer txn.abort();
    const ordinal = (try db_mod.doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b")) orelse return error.TestUnexpectedResult;
    const stable_vector_id = db_mod.denseTestVectorId("doc:b");
    try std.testing.expect(stable_vector_id != @as(u64, ordinal));
    try std.testing.expectEqual(@as(?u64, stable_vector_id), try reopened.core.index_manager.lookupDenseVectorId(reopened.core.store, "dense_idx", "doc:b"));

    const vector_ids = try reopened.core.index_manager.lookupDenseVectorIdsForOrdinalsAlloc(alloc, reopened.core.store, "dense_idx", &.{ordinal});
    defer alloc.free(vector_ids);
    try std.testing.expectEqual(@as(usize, 1), vector_ids.len);
    try std.testing.expectEqual(stable_vector_id, vector_ids[0]);

    const dense_entry = reopened.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult;
    const stable_metadata = (try dense_entry.index.getMetadata(stable_vector_id)) orelse return error.TestUnexpectedResult;
    defer alloc.free(stable_metadata);
    try std.testing.expectEqualStrings("doc:b", stable_metadata);
    try std.testing.expectEqual(@as(?[]u8, null), try dense_entry.index.getMetadata(@as(u64, ordinal)));
}

test "db dense artifact rebuild rejects clean checkpoint for stale config identity" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            },
            .sync_level = .full_index,
        });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 4,
            .config_hash = db_mod.types.indexConfigHash(.{
                .name = "dense_idx",
                .kind = .dense_vector,
                .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"external\":true}",
            }),
        });

        const stale_artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:stale", "dense_idx");
        defer alloc.free(stale_artifact_key);
        try db_mod.putDenseEmbeddingArtifactForTest(&db, alloc, stale_artifact_key, null, &[_]f32{ 0, 1, 0 });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    try std.testing.expect(try reopened.hasPendingDenseArtifactRebuild(alloc));
    try std.testing.expectEqual(@as(usize, 1), try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
    const repair_id = (try reopened.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    var intent = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
    try std.testing.expectEqual(db_mod.index_repair_state.Trigger.projection_generation_invalid, intent.intent.trigger);
    intent.deinit(alloc);
    try std.testing.expect(reopened.core.index_manager.repairUnavailable("dense_idx"));
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    }));
}

test "db dense artifact rebuild bootstraps missing counter metadata" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const dense_cfg: db_mod.types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };

    {
        var db = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(dense_cfg);
        const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_idx");
        defer alloc.free(counter_key);
        try db.core.store.putBatch(&.{}, &.{counter_key});
        const stored_key = try db_mod.internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});
        const artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dense_idx");
        defer alloc.free(artifact_key);
        try db_mod.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 9,
            .config_hash = db_mod.types.indexConfigHash(dense_cfg),
        });
    }

    var io_impl = db_mod.threadedIo();
    defer io_impl.deinit();
    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var repair_id: u128 = 0;
    {
        var reopened = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();

        try std.testing.expect(try reopened.hasPendingDenseArtifactRebuild(alloc));
        try std.testing.expectEqual(@as(usize, 1), try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
        repair_id = (try reopened.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
        var intent = try reopened.indexes.repair.loadEntryById(alloc, repair_id);
        try std.testing.expectEqual(db_mod.index_repair_state.Trigger.artifact_counter_missing, intent.intent.trigger);
        intent.deinit(alloc);
        try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
            .limit = 1,
        }));
    }

    // The missing-proof gate is reconstructed from the durable intent before
    // any repair owner resumes work after process restart.
    {
        var resumed = try db_mod.openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer resumed.close();
        try std.testing.expectEqual(
            repair_id,
            (try resumed.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult,
        );
        try std.testing.expectError(error.IndexRebuilding, resumed.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
            .limit = 1,
        }));
        const repaired = try resumed.indexes.repair.advanceIntent(alloc, repair_id, .{});
        try std.testing.expect(repaired.repaired);
        try std.testing.expectEqual(
            @as(?u64, 1),
            try DB.loadDenseArtifactTargetCounter(alloc, resumed.core.store, "dense_idx"),
        );
        try std.testing.expect(!(try resumed.hasPendingDenseArtifactRebuild(alloc)));
    }
}

test "db dense artifact rebuild force-resets corrupt external dense structure" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"leaf_size\":2}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"_embeddings\":{\"dense_idx\":[0,0,1]}}" },
                .{ .key = "doc:e", .value = "{\"title\":\"epsilon\",\"_embeddings\":{\"dense_idx\":[0.8,0.2,0]}}" },
                .{ .key = "doc:f", .value = "{\"title\":\"zeta\",\"_embeddings\":{\"dense_idx\":[0.7,0.3,0]}}" },
            },
            .sync_level = .full_index,
        });

        const dense_entry = db.core.index_manager.denseIndex("dense_idx").?;
        try std.testing.expect(dense_entry.index.stats().node_count > 1);

        var read_txn = try dense_entry.index.beginReadTxn();
        defer read_txn.abort();
        var root = try dense_entry.index.loadNode(&read_txn, dense_entry.index.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expect(!root.is_leaf);
        try dense_entry.index.deleteNodeHeaderForTest(root.children[0]);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    // Native startup may validate lazily. Exercise the real query detection
    // path before asking the durable repair owner to replace this generation.
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .dense = .{ .vector = &.{ 1, 0, 0 }, .k = 6 },
        .search_effort = 1,
    }));

    // The query or startup persisted the corrupt generation repair intent.
    // Broad discovery must not compete with that durable owner.
    try std.testing.expect(try reopened.indexes.repair.hasPendingIntents(alloc));

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    try std.testing.expectEqual(@as(usize, 0), rebuilt);
    try std.testing.expect(!try reopened.hasPendingDenseArtifactRebuild(alloc));
    try std.testing.expect(try reopened.indexes.repair.hasPendingIntents(alloc));
    const repair_id = (try reopened.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    const advanced = try reopened.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try std.testing.expect(advanced.repaired);
    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));

    const stats = try reopened.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    var dense_doc_count: ?u64 = null;
    for (stats.indexes) |index| {
        if (std.mem.eql(u8, index.name, "dense_idx")) dense_doc_count = index.doc_count;
    }
    try std.testing.expectEqual(@as(?u64, 6), dense_doc_count);

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 3,
        } },
        .limit = 3,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 3), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db reverse graph probe rejects a deleted or replaced index incarnation" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("graph-incarnation");
    defer directory.cleanup();
    const path = directory.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    const cfg = db_mod.types.IndexConfig{ .name = "graph_idx", .kind = .graph, .config_json = "{}" };
    try db.addIndex(cfg);
    const previous = db.core.index_manager.coverageIdentityForIndex("graph_idx").?;
    try std.testing.expect(try db.deleteIndex("graph_idx"));
    // Even an empty probe must not certify a missing or replacement index.
    try std.testing.expectError(error.IndexGenerationMismatch, db.graphHasIncomingEdgesForInternalRead(alloc, "graph_idx", &.{}, previous, null));
    // Same-name admission waits for the retired incarnation's asynchronous
    // artifact cleanup. Join its owner instead of racing it or sleeping.
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);
    try db.addIndex(cfg);
    const current = db.core.index_manager.coverageIdentityForIndex("graph_idx").?;
    try std.testing.expect(previous.generation != current.generation);
    try std.testing.expectEqual(previous.config_fingerprint, current.config_fingerprint);
    try std.testing.expectError(error.IndexGenerationMismatch, db.graphHasIncomingEdgesForInternalRead(alloc, "graph_idx", &.{"absent"}, previous, null));
    const incoming = try db.graphHasIncomingEdgesForInternalRead(alloc, "graph_idx", &.{"absent"}, current, null);
    defer alloc.free(incoming);
    try std.testing.expectEqualSlices(bool, &.{false}, incoming);
}

test "online direct vector shadow repair resumes raw and stripped rows without orphan vectors" {
    const alloc = std.testing.allocator;
    db_mod.test_dense_repair_rebuild_batch_size = 1;
    defer db_mod.test_dense_repair_rebuild_batch_size = null;
    for ([_]bool{ false, true }) |sparse| {
        var directory = try db_mod.TestDirectory.init("online-vector-repair");
        defer directory.cleanup();
        const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } };
        var db = try DB.open(alloc, directory.path(), options);
        defer db.close();
        const raw = if (sparse) "{\"v\":{\"indices\":[1,3],\"values\":[2,4]}}" else "{\"v\":[1,2]}";
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = raw }}, .sync_level = .full_index });
        try db.addIndex(.{ .name = "vector", .kind = if (sparse) .sparse_vector else .dense_vector, .config_json = if (sparse) "{\"field\":\"v\"}" else "{\"field\":\"v\",\"dims\":2}" });
        try db.batch(.{ .writes = &.{.{ .key = "b", .value = raw }}, .sync_level = .full_index });
        const authored_key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "b", "vector");
        defer alloc.free(authored_key);
        {
            const authored = try db.core.store.get(alloc, authored_key);
            defer alloc.free(authored);
            try std.testing.expect((try db_mod.enrichment_artifact_codec.decodeHeader(authored)).flags.authored);
        }
        const orphan_key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "orphan", "vector");
        defer alloc.free(orphan_key);
        const orphan = if (sparse) try db_mod.enrichment_artifact_codec.encodeSparseEmbeddingAlloc(alloc, null, &.{1}, &.{1}) else try db_mod.enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &.{ 1, 2 });
        defer alloc.free(orphan);
        try db.core.store.put(orphan_key, orphan);
        var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{ .target = .index, .artifact_kind = .embedding, .index_name = "vector", .limit = 1, .force = true }, .{ .defer_durable_index_repair_execution = true });
        defer queued.deinit(alloc);
        const repair_id = (try db.indexes.repair.idForIndex(alloc, "vector")) orelse return error.TestUnexpectedResult;
        const Yield = struct {
            fn requested(_: *anyopaque) bool {
                return true;
            }
        };
        var context: u8 = 0;
        const first = try db.indexes.repair.advanceIntent(alloc, repair_id, .{ .yield_check = .{ .ptr = &context, .is_requested = Yield.requested }, .max_activation_pause_ms = 5000 });
        try std.testing.expect(first.busy and !first.repaired);
        {
            var entry = try db.indexes.repair.loadEntryById(alloc, repair_id);
            defer entry.deinit(alloc);
            try std.testing.expect(entry.intent.build_resume_key != null);
        }
        db.close();
        db = try DB.open(alloc, directory.path(), options);
        var repaired = false;
        for (0..16) |_| {
            const step = try db.indexes.repair.advanceIntent(alloc, repair_id, .{ .max_activation_pause_ms = 5000 });
            if (step.repaired) {
                repaired = true;
                break;
            }
        }
        try std.testing.expect(repaired);
        {
            const authored = try db.core.store.get(alloc, authored_key);
            defer alloc.free(authored);
            try std.testing.expect((try db_mod.enrichment_artifact_codec.decodeHeader(authored)).flags.authored);
        }
        var result = try db.search(alloc, .{ .index_name = "vector", .query = if (sparse) .{ .sparse_knn = .{ .indices = &.{1}, .values = &.{1}, .k = 10 } } else .{ .dense_knn = .{ .vector = &.{ 1, 2 }, .k = 10 } }, .limit = 10 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
        for (result.hits) |hit| try std.testing.expect(!std.mem.eql(u8, hit.id, "orphan"));
    }
}

test "db native deferred restore preserves generated dense generation without embedder" {
    const alloc = std.testing.allocator;
    var source_tmp = try db_mod.TestDirectory.init("db");
    defer source_tmp.cleanup();
    const source_path = source_tmp.path().ptr;
    defer db_mod.cleanupTempDir(source_path);
    var restore_tmp = try db_mod.TestDirectory.init("db");
    defer restore_tmp.cleanup();
    const restore_path = restore_tmp.path().ptr;
    defer db_mod.cleanupTempDir(restore_path);
    const primary_backend: db_mod.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    var source_applied: u64 = 0;
    var source_generation: u64 = 0;
    var source_count: u64 = 0;
    {
        var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var source = try DB.open(alloc, std.mem.span(source_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "native-backup-source",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer source.close();
        try source.addIndex(.{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
        });
        try source.addIndex(.{
            .name = "text_idx",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        });
        try source.batch(.{
            .writes = &.{.{
                .key = "doc:native",
                .value = "{\"body\":\"abcdefghijklmno\",\"title\":\"portable native generation\"}",
            }},
            .sync_level = .full_index,
        });
        try source.runUntilIdle();
        // A fresh native-v2 index can now publish its clean projection during
        // the normal full-index barrier. Older roots still converge through
        // the durable repair lane. The backup invariant is the same in both
        // cases, so exercise repair only when authoritative debt remains
        // instead of requiring a particular publication schedule.
        if (try source.denseArtifactRebuildMaintenanceNeeded(alloc)) {
            try std.testing.expect(try source.runDenseArtifactRebuildMaintenanceWithProgress(alloc, null, null));
            _ = try source.repairRecoverableStartupIndexFailures(alloc, 1, .{});
        }
        try source.runUntilIdle();
        const cfg = source.core.index_manager.get("semantic_idx") orelse return error.TestUnexpectedResult;
        const checkpoint = try source.core.loadProjectionCheckpoint(alloc, cfg.name);
        source_applied = checkpoint.applied_sequence;
        source_generation = checkpoint.generation;
        source_count = source.core.index_manager.denseIndex("semantic_idx").?.index.stats().active_count;
        try std.testing.expect(source_count > 0);
        _ = try source.core.index_manager.cleanupInactiveRepairShadowRootsPage();
        const active_relative = (try source.core.index_manager.captureActiveIndexRootPointer("semantic_idx")) orelse
            return error.TestUnexpectedResult;
        defer alloc.free(active_relative);
        const active_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ std.mem.span(source_path), active_relative });
        defer alloc.free(active_root);
        for ([_][]const u8{ "runs", "wal", "manifest.bin" }) |legacy_name| {
            const legacy_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ active_root, legacy_name });
            defer alloc.free(legacy_path);
            try std.testing.expectError(
                error.FileNotFound,
                std.Io.Dir.cwd().access(std.testing.io, legacy_path, .{}),
            );
        }
        _ = try source.snapshotNative("native-fast");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/native-fast", .{std.mem.span(source_path)});
    defer alloc.free(snapshot_root);
    defer {
        var snapshots_buf: [512]u8 = undefined;
        if (std.fmt.bufPrint(&snapshots_buf, "{s}.snapshots", .{std.mem.span(source_path)})) |snapshots| {
            var cleanup_io = db_mod.threadedIo();
            defer cleanup_io.deinit();
            std.Io.Dir.cwd().deleteTree(cleanup_io.io(), snapshots) catch {};
        } else |_| {}
    }
    const native_manifest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, db_mod.native_backup.manifest_file_name });
    defer alloc.free(native_manifest_path);
    const native_manifest_raw = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        native_manifest_path,
        alloc,
        .limited(db_mod.native_backup.max_manifest_bytes),
    );
    defer alloc.free(native_manifest_raw);
    var native_manifest = try std.json.parseFromSlice(db_mod.native_backup.Manifest, alloc, native_manifest_raw, .{});
    defer native_manifest.deinit();
    var saw_native_dense = false;
    for (native_manifest.value.projections) |projection| {
        if (!std.mem.eql(u8, projection.name, "semantic_idx")) continue;
        try std.testing.expectEqualStrings(
            db_mod.index_manager_mod.IndexManager.dense_native_backup_backend_id,
            projection.backend_id,
        );
        saw_native_dense = true;
    }
    try std.testing.expect(saw_native_dense);
    const native_posting_current = try std.fmt.allocPrint(
        alloc,
        "{s}/indexes/semantic_idx/posting-segments/CURRENT",
        .{snapshot_root},
    );
    defer alloc.free(native_posting_current);
    try std.Io.Dir.accessAbsolute(std.testing.io, native_posting_current, .{});

    // Exact-vector blocks are shared acceleration rather than posting
    // authority. Simulate losing one after capture: restore must retain the
    // native posting generation and serve exact results from primary vectors,
    // even though this fixture intentionally has no embedder for rebuilding.
    var removed_shared_acceleration = false;
    for (native_manifest.value.artifacts) |artifact| {
        if (artifact.role != .shared_acceleration or !std.mem.endsWith(u8, artifact.path, ".afvb")) continue;
        const artifact_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, artifact.path });
        defer alloc.free(artifact_path);
        try std.Io.Dir.cwd().deleteFile(std.testing.io, artifact_path);
        removed_shared_acceleration = true;
        break;
    }
    try std.testing.expect(removed_shared_acceleration);

    var transition = try db_mod.generation_lifecycle.beginProcessExclusive(std.mem.span(restore_path));
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();
    var restore_runtime = try db_mod.background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .filesystem_io = std.testing.io,
    });
    defer restore_runtime.deinit();
    try DB.restoreSnapshotToDeferredRuntimeRepair(
        &staged,
        alloc,
        snapshot_root,
        staged.path(),
        .{
            .primary_backend = primary_backend,
            .backend_runtime = restore_runtime.ptr(),
        },
        .{
            .backup_id = "native-fast",
            .location = "local",
            .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            .snapshot_path = "native-fast",
            .group_id = 7001,
        },
    );
    try std.testing.expect(!(try DB.restoreRuntimeRepairNeededForPathWithIo(alloc, std.testing.io, staged.path())));
    _ = try staged.publish();
    staged.deinit();
    transition.deinit();

    var restored = try DB.open(alloc, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .start_optional_runtimes = false,
    });
    defer restored.close();
    const cfg = restored.core.index_manager.get("semantic_idx") orelse return error.TestUnexpectedResult;
    const checkpoint = try restored.core.loadProjectionCheckpoint(alloc, cfg.name);
    try std.testing.expectEqual(source_applied, checkpoint.applied_sequence);
    try std.testing.expectEqual(source_generation, checkpoint.generation);
    try std.testing.expectEqual(source_count, restored.core.index_manager.denseIndex("semantic_idx").?.index.stats().active_count);
    try std.testing.expect(!try restored.core.index_manager.denseNativePhysicalMigrationRequired("semantic_idx"));
    try std.testing.expect((try restored.indexes.repair.idForIndex(alloc, "semantic_idx")) == null);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    const vector = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(vector);
    var result = try restored.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{ .vector = vector, .k = 3 },
        .return_mode = .parent,
    });
    defer result.deinit();
    try std.testing.expect(result.hits.len > 0);
    try std.testing.expectEqualStrings("doc:native", result.hits[0].id);

    var text_result = try restored.search(alloc, .{
        .index_name = "text_idx",
        .full_text = .{ .match = .{ .field = "title", .text = "generation" } },
    });
    defer text_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), text_result.total_hits);
    try std.testing.expectEqualStrings("doc:native", text_result.hits[0].id);
}

test "db restore dense artifact completion publishes counter bootstrap as a bounded quantum" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}",
        }},
        .sync_level = .full_index,
    });

    const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_idx");
    defer alloc.free(counter_key);
    try db.core.store.delete(counter_key);
    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try db.indexes.repair.ensureAutomaticDenseGenerationIntent(
        alloc,
        cfg.*,
        .artifact_counter_missing,
        "dense_artifact_counter_missing",
    );

    try std.testing.expectEqual(
        DB.RestoreDenseArtifactCompletion.progressed,
        try db.completeRestoreDenseArtifactRepairs(alloc),
    );
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );
    try std.testing.expectEqual(
        repair_id,
        (try db.indexes.repair.idForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult,
    );

    try std.testing.expectEqual(
        DB.RestoreDenseArtifactCompletion.completed_with_progress,
        try db.completeRestoreDenseArtifactRepairs(alloc),
    );
    try std.testing.expect((try db.indexes.repair.idForIndex(alloc, "dense_idx")) == null);
}

test "db restore dense artifact completion keeps every intent when one proof is pending" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    inline for (.{ "dense_a", "dense_b" }) |name| {
        try db.addIndex(.{
            .name = name,
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
        });
    }
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_a\":[1,0],\"dense_b\":[0,1]}}",
        }},
        .sync_level = .full_index,
    });

    const cfg_a = db.core.index_manager.get("dense_a") orelse return error.TestUnexpectedResult;
    const repair_a = try db.indexes.repair.ensureAutomaticDenseGenerationIntent(
        alloc,
        cfg_a.*,
        .artifact_coverage_mismatch,
        "dense_artifact_coverage_mismatch",
    );
    const cfg_b = db.core.index_manager.get("dense_b") orelse return error.TestUnexpectedResult;
    const repair_b = try db.indexes.repair.ensureAutomaticDenseGenerationIntent(
        alloc,
        cfg_b.*,
        .artifact_coverage_mismatch,
        "dense_artifact_coverage_mismatch",
    );
    const counter_b = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_b");
    defer alloc.free(counter_b);
    try db.core.store.delete(counter_b);

    try std.testing.expectError(
        error.RestoreDenseCounterProofIncomplete,
        db.completeRestoreDenseArtifactRepairs(alloc),
    );
    try std.testing.expectEqual(
        repair_a,
        (try db.indexes.repair.idForIndex(alloc, "dense_a")) orelse return error.TestUnexpectedResult,
    );
    try std.testing.expectEqual(
        repair_b,
        (try db.indexes.repair.idForIndex(alloc, "dense_b")) orelse return error.TestUnexpectedResult,
    );
}

test "db restore dense artifact completion retires an obsolete invalid-generation shadow" {
    const alloc = std.testing.allocator;
    var test_directory = try db_mod.TestDirectory.init("db");
    defer test_directory.cleanup();
    const path = test_directory.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}",
        }},
        .sync_level = .full_index,
    });

    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try db.indexes.repair.ensureAutomaticDenseGenerationIntent(
        alloc,
        cfg.*,
        .projection_generation_invalid,
        "dense_projection_checkpoint_repair_required",
    );
    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, cfg.name);
    try db.core.saveProjectionCheckpoint(cfg.name, .{
        .applied_sequence = checkpoint.applied_sequence,
        .status = .repair_required,
        .generation = checkpoint.generation,
        .config_hash = db_mod.types.indexConfigHash(cfg.*),
    });

    const shadow_base = try db_mod.createUniqueRepairShadowBase(alloc, std.mem.span(path));
    defer alloc.free(shadow_base);
    const candidate_relative_path = try std.fmt.allocPrint(
        alloc,
        "{s}/indexes/{s}",
        .{ std.fs.path.basename(shadow_base), cfg.name },
    );
    defer alloc.free(candidate_relative_path);
    try db.indexes.repair.updateIntent(alloc, repair_id, .{ .phase = .preflight });
    try db.indexes.repair.updateIntent(alloc, repair_id, .{
        .phase = .building,
        .candidate_relative_path = candidate_relative_path,
        .replace_candidate_path = true,
    });
    try db.indexes.repair.recordAttemptFailure(
        alloc,
        repair_id,
        @errorName(error.RepairSourceCoverageIncomplete),
        true,
    );

    try std.testing.expectEqual(
        DB.RestoreDenseArtifactCompletion.completed_with_progress,
        try db.completeRestoreDenseArtifactRepairs(alloc),
    );
    try std.testing.expect((try db.indexes.repair.idForIndex(alloc, cfg.name)) == null);
    const completed_checkpoint = try db.core.loadProjectionCheckpoint(alloc, cfg.name);
    try std.testing.expectEqual(db_mod.apply_state.ProjectionStatus.clean, completed_checkpoint.status);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, shadow_base, .{}));
}

test "db restore final artifact recount preserves an exact native generation" {
    const alloc = std.testing.allocator;
    var test_directory = try db_mod.TestDirectory.init("db");
    defer test_directory.cleanup();
    const path = test_directory.path().ptr;
    const primary_backend: db_mod.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    var lsm_cache = db_mod.lsm_backend_mod.Cache.init(alloc, 8 * 1024 * 1024);
    defer lsm_cache.deinit();
    var hbc_cache = db_mod.hbc_mod.Cache.init(alloc);
    defer hbc_cache.deinit();
    var resident_runtime = try db_mod.background_runtime_mod.BackendRuntimeHandle.init(alloc, .{});
    defer resident_runtime.deinit();
    {
        var old_live = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
            .lsm_cache = &lsm_cache,
            .hbc_cache = &hbc_cache,
            .backend_runtime = resident_runtime.ptr(),
            .lsm_root_generation = 41,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer old_live.close();
        try old_live.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
        });
    }
    var transition = try db_mod.generation_lifecycle.beginProcessExclusive(std.mem.span(path));
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();

    {
        var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var db = try DB.open(alloc, staged.path(), .{
            .primary_backend = primary_backend,
            .lsm_root_generation = 41,
            .staged_generation = &staged,
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
        });
        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value = "{\"body\":\"abcdefghijklmno\"}",
            }},
            .sync_level = .full_index,
        });
    }

    var expected_active: u64 = 0;
    {
        var db = try DB.open(alloc, staged.path(), .{
            .primary_backend = primary_backend,
            .lsm_root_generation = 41,
            .staged_generation = &staged,
            .start_index_workers = false,
            .start_optional_runtimes = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
        const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, cfg.name);
        defer alloc.free(counter_key);
        try db.core.store.delete(counter_key);
        _ = try db.indexes.repair.ensureAutomaticDenseGenerationIntent(
            alloc,
            cfg.*,
            .artifact_counter_missing,
            "dense_artifact_counter_missing",
        );
        const dense = db.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult;
        expected_active = dense.index.stats().active_count;
        try std.testing.expect(expected_active > 0);
        // Native-v2 already has exact published coverage. Repair the missing
        // durable counter without inventing an unowned bulk session or
        // rebuilding the active root; the fresh owner below proves durability.
        const repaired = try db.repairRestoreDenseArtifactCoverageFromFinalArtifacts(alloc);
        try std.testing.expectEqual(@as(usize, 0), repaired.rebuilt);
        try std.testing.expect(repaired.made_progress);
        const completed_checkpoint = try db.core.loadProjectionCheckpoint(alloc, "dense_idx");
        try std.testing.expectEqual(db_mod.apply_state.ProjectionStatus.clean, completed_checkpoint.status);
        try std.testing.expectEqual(
            DB.RestoreDenseArtifactCompletion.completed_with_progress,
            try db.completeRestoreDenseArtifactRepairs(alloc),
        );
        _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsOutcomeWithProgress(alloc, null, null);
        try db.sync(true);
        try db.syncIndexes(true);
    }
    try staged.seal();
    _ = try staged.publish();
    staged.deinit();
    transition.deinit();

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = primary_backend,
        .lsm_root_generation = 41,
        .lsm_cache = &lsm_cache,
        .hbc_cache = &hbc_cache,
        .backend_runtime = resident_runtime.ptr(),
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    const dense = reopened.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(expected_active, dense.index.stats().active_count);
}

test "db restore owner converges rediscovered dense projection intent" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}",
        }},
        .sync_level = .full_index,
    });

    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, cfg.name);
    try db.core.saveProjectionCheckpoint(cfg.name, .{
        .applied_sequence = checkpoint.applied_sequence,
        .status = .repair_required,
        .generation = checkpoint.generation,
        .config_hash = db_mod.types.indexConfigHash(cfg.*),
    });

    const discovered = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsOutcomeWithProgress(alloc, null, null);
    try std.testing.expectEqual(@as(usize, 1), discovered.reported_work);
    try std.testing.expect(discovered.made_progress);
    try std.testing.expect(!discovered.dense_visibility_changed);

    const rediscovered = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsOutcomeWithProgress(alloc, null, null);
    // Durable repair ownership excludes duplicate in-place discovery work.
    try std.testing.expectEqual(@as(usize, 0), rediscovered.reported_work);
    try std.testing.expect((try db.indexes.repair.idForIndex(alloc, "dense_idx")) != null);
    try std.testing.expect(!rediscovered.made_progress);
    try std.testing.expect(!rediscovered.dense_visibility_changed);

    try DB.markRestorePrimaryRestoredForPathWithArtifact(
        alloc,
        std.mem.span(path),
        "snap1",
        "file:///tmp/backups",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "snapshots/snap1",
        7001,
    );
    try db.updateRestoreRuntimeRepairPhaseWithIo(alloc, std.testing.io, "sync_indexes", false);
    try std.testing.expect(try db.repairRestoreRuntimeStateStepIfNeeded(alloc));
    try std.testing.expect((try db.indexes.repair.idForIndex(alloc, "dense_idx")) == null);
    var restore_state = (try DB.readRestoreStateForPathWithIo(alloc, std.testing.io, std.mem.span(path))).?;
    defer restore_state.deinit(alloc);
    try std.testing.expect(restore_state.runtime_repair_complete);
    try std.testing.expectEqualStrings("complete", restore_state.phase);
}

test "db restore dense rebuild publishes mixed progress before worker wait" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    inline for (.{ "dense_a", "dense_b" }) |name| {
        try db.addIndex(.{
            .name = name,
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
        });
    }
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_a\":[1,0],\"dense_b\":[0,1]}}",
        }},
        .sync_level = .full_index,
    });

    try db_mod.resetDenseIndexForArtifactRebuildForTest(&db, "dense_a");
    var ghost_vector = [_]f32{ 1, 1 };
    try db.core.index_manager.denseIndex("dense_b").?.index.insertVectorForTest(0xdead_beef, &ghost_vector);
    try DB.markRestorePrimaryRestoredForPathWithArtifact(
        alloc,
        std.mem.span(path),
        "snap1",
        "file:///tmp/backups",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "snapshots/snap1",
        7001,
    );
    try db.updateRestoreRuntimeRepairPhaseWithIo(alloc, std.testing.io, "rebuild_replayed_artifacts", false);

    // Simulate a resident managed writer without racing a real worker against
    // this deterministic step test.
    db.start_index_workers = true;
    try std.testing.expect(try db.repairRestoreRuntimeStateStepIfNeeded(alloc));
    try std.testing.expectEqual(
        @as(u64, 1),
        db.core.index_manager.denseIndex("dense_a").?.index.stats().active_count,
    );
    try std.testing.expect((try db.indexes.repair.idForIndex(alloc, "dense_b")) != null);

    var restore_state = (try DB.readRestoreStateForPathWithIo(alloc, std.testing.io, std.mem.span(path))).?;
    defer restore_state.deinit(alloc);
    try std.testing.expectEqualStrings("rebuild_replayed_artifacts", restore_state.phase);
    // Watermark repair can publish one more bounded progress quantum after
    // rebuilding. Once drained, the pending owner must yield, not spin.
    try std.testing.expect(try db.repairRestoreRuntimeStateStepIfNeeded(alloc));
    var waiting = (try DB.readRestoreStateForPathWithIo(alloc, std.testing.io, std.mem.span(path))).?;
    defer waiting.deinit(alloc);
    try std.testing.expectEqualStrings("rebuild_replayed_artifacts", waiting.phase);
    try std.testing.expectError(error.RestoreRuntimeRepairIncomplete, db.repairRestoreRuntimeStateStepIfNeeded(alloc));
    try std.testing.expect(!waiting.runtime_repair_complete);
    try std.testing.expect((try db.indexes.repair.idForIndex(alloc, "dense_b")) != null);
}

test "source vector table persists references without an ANN index and reopens" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-a");
    defer alloc.free(key);
    const other_key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-b");
    defer alloc.free(other_key);
    const first = try db_mod.enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, 11, &.{ 1, 2, 3 });
    defer alloc.free(first);
    const second = try db_mod.enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, 12, &.{ 4, 5 });
    defer alloc.free(second);
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .table_storage = .{ .dense_embeddings = .vector_store },
            .start_index_workers = false,
            .start_optional_runtimes = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.core.store.putBatch(&.{ .{ .key = key, .value = first }, .{ .key = other_key, .value = second } }, &.{});
        var raw = try db.core.store.runtime_store.beginRead();
        defer raw.abort();
        try std.testing.expect(@import("../../artifact_payload.zig").isReference(try raw.get(key)));
        try std.testing.expectError(error.ImmutableTableStorageSettings, db.configureTableStorage(.{}));
        try db.sync(true);
    }
    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expectEqual(db_mod.table_storage_mod.DenseEmbeddings.vector_store, reopened.local_execution.table_storage.dense_embeddings);
    const value = try reopened.core.store.get(alloc, key);
    defer alloc.free(value);
    try std.testing.expectEqualSlices(u8, first, value);
    const other = try reopened.core.store.get(alloc, other_key);
    defer alloc.free(other);
    try std.testing.expectEqualSlices(u8, second, other);
    // Open schedules artifact cleanup even with index workers disabled. Its
    // metadata scans retain payload sessions, so join this owner's startup
    // jobs before driving the bounded collector to completion.
    reopened.backend_runtime.durable_jobs.drainOwner(reopened.indexes.repair.cleanup_owner_id);
    var collection_turns: usize = 0;
    while (!try reopened.collectSourceVectorGarbage()) : (collection_turns += 1) {
        if (collection_turns == 1024) return error.CollectionDidNotFinish;
        db_mod.platform_time.yieldBriefly();
    }
    try std.testing.expectEqual(@as(u64, 2), (try reopened.sourceVectorStats()).?.live_payloads_at_collection);
}

test "source vector table collection defers until an old read transaction releases its payload session" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{
        .table_storage = .{ .dense_embeddings = .vector_store },
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);

    const key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    const first = try db_mod.enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, 11, &.{ 1, 2, 3 });
    defer alloc.free(first);
    const replacement = try db_mod.enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, 12, &.{ 4, 5, 6 });
    defer alloc.free(replacement);
    try db.core.store.put(key, first);
    try db.sync(true);
    // Full sync may start a bounded mark before the reader is admitted. Drain
    // that cut first: the assertion below tests admission of a NEW collection
    // while an old snapshot is active, not completion of its already-safe cut.
    var initial_gc_turns: usize = 0;
    while (!try db.collectSourceVectorGarbage()) : (initial_gc_turns += 1) {
        if (initial_gc_turns == 1024) return error.CollectionDidNotFinish;
        db_mod.platform_time.yieldBriefly();
    }

    {
        var reader = try db.core.store.beginReadTxn();
        defer reader.abort();
        try db.core.store.put(key, replacement);
        try std.testing.expectEqual(@as(u64, 2), (try db.sourceVectorStats()).?.retained_payloads);
        const before = (try db.sourceVectorStats()).?;
        try std.testing.expect(!try db.collectSourceVectorGarbage());
        const deferred = (try db.sourceVectorStats()).?;
        try std.testing.expectEqual(before.collections, deferred.collections);
        try std.testing.expectEqual(before.collection_deferrals + 1, deferred.collection_deferrals);
        try std.testing.expectEqual(@as(u64, 2), deferred.retained_payloads);
        // Resolve only after the attempted collection: the old transaction's
        // primary reference must still have a live source payload to read.
        try std.testing.expectEqualSlices(u8, first, try reader.get(key));
    }

    var collection_turns: usize = 0;
    while (!try db.collectSourceVectorGarbage()) : (collection_turns += 1) {
        if (collection_turns == 1024) return error.CollectionDidNotFinish;
        db_mod.platform_time.yieldBriefly();
    }
    try std.testing.expectEqual(@as(u64, 1), (try db.sourceVectorStats()).?.live_payloads_at_collection);
    try std.testing.expectEqual(@as(u64, 1), (try db.sourceVectorStats()).?.retained_payloads);
    const current = try db.core.store.get(alloc, key);
    defer alloc.free(current);
    try std.testing.expectEqualSlices(u8, replacement, current);
}

test "source vector table retains artifacts after dropping last consumer and rebuilds" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{
        .table_storage = .{ .dense_embeddings = .vector_store },
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    const config = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"embedding_name\":\"shared_source\"}";
    try db.addIndex(.{ .name = "first", .kind = .dense_vector, .config_json = config });
    const key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc", "shared_source");
    defer alloc.free(key);
    const doc_key = try db_mod.internal_keys.documentKeyAlloc(alloc, "doc");
    defer alloc.free(doc_key);
    try db.core.store.put(doc_key, "{\"title\":\"test\"}");
    try db_mod.putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, key, null, &.{ 1, 0, 0 });
    _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    try std.testing.expect(try db.deleteIndex("first"));
    var pages: usize = 0;
    while (try db.advanceGeneratedArtifactCleanupPage(null) != .idle) : (pages += 1) {
        if (pages > 10000) return error.CleanupDidNotDrain;
        try db.core.index_manager.checkpointIo().sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    const retained = try db.core.store.get(alloc, key);
    defer alloc.free(retained);
    try std.testing.expectEqual(@as(usize, 3), try db_mod.enrichment_artifact_codec.decodeDenseEmbeddingDims(retained));
    try db.addIndex(.{ .name = "replacement", .kind = .dense_vector, .config_json = config });
    _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    const repair_id = (try db.indexes.repair.idForIndex(alloc, "replacement")) orelse return error.TestUnexpectedResult;
    _ = try db.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(u64, 1), db.core.index_manager.denseIndex("replacement").?.index.metadata.active_count);
    // Native repair candidates must retain the table's source ownership. A
    // missing binding used to silently build a second full-payload ANN base.
    const relative = (try db.core.index_manager.captureActiveIndexRootPointer("replacement")) orelse return error.TestUnexpectedResult;
    defer alloc.free(relative);
    const candidate_root = std.fs.path.dirname(std.fs.path.dirname(relative).?).?;
    const map_root = try std.fmt.allocPrint(alloc, "{s}/{s}/vector-blocks", .{ std.mem.span(path), candidate_root });
    defer alloc.free(map_root);
    var map = try @import("../../vector_block_store.zig").Store.openReadOnlyWithBlocks(alloc, db.core.index_manager.vector_block_storage.?, map_root);
    defer map.deinit();
    try std.testing.expectEqual(.artifact_reference, map.baseEncoding());
}

test "db capacity deferred dense repairs exclusively own both model generations across restart" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const configs = [_]db_mod.types.IndexConfig{
        .{ .name = "model_a", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}" },
        .{ .name = "model_b", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}" },
    };
    const options: db_mod.OpenOptions = .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    };
    var repair_ids: [2]u128 = undefined;
    {
        var db = try DB.open(alloc, std.mem.span(path), options);
        defer db.close();
        for (configs) |cfg| try db.addIndex(cfg);
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"model_a\":[1,0,0],\"model_b\":[0,1]}}" }},
            .sync_level = .write,
        });
        for (configs, 0..) |cfg, i| {
            // Reproduce a publication cursor ahead of an empty canonical ANN
            // generation. Durable shadow repair owns the missing artifacts.
            try db.core.saveAppliedSequence(cfg.name, 1);
            try db.core.saveProjectionCheckpoint(cfg.name, .{
                .applied_sequence = 1,
                .status = .rebuilding,
                .generation = 1,
                .config_hash = db_mod.types.indexConfigHash(cfg),
            });
            repair_ids[i] = try db.indexes.repair.createOperatorGenerationIntent(alloc, cfg, 0, 0);
            const step = try db.indexes.repair.advanceIntent(alloc, repair_ids[i], .{
                .capacity_domain_id = 77,
                .capacity_observation = .{ .available_bytes = 1, .capacity_bytes = 1 },
            });
            try std.testing.expect(step.disk_wait);
            try std.testing.expect(step.deferred);
            try std.testing.expectEqual(@as(u64, 0), db.core.index_manager.denseIndex(cfg.name).?.index.stats().active_count);
        }
        var plan = try db.collectDenseArtifactRebuildPlan(alloc);
        defer plan.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), plan.targets.len);
        try std.testing.expectEqual(@as(usize, 0), plan.generation_repairs.len);
    }
    var db = try DB.open(alloc, std.mem.span(path), options);
    defer db.close();
    {
        var plan = try db.collectDenseArtifactRebuildPlan(alloc);
        defer plan.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), plan.targets.len);
        try std.testing.expectEqual(@as(usize, 0), plan.generation_repairs.len);
    }
    for (configs, repair_ids) |cfg, repair_id| {
        try db.indexes.repair.updateIntent(alloc, repair_id, .{ .next_retry_at_ms = 0 });
        var repaired = false;
        for (0..16) |_| {
            const step = try db.indexes.repair.advanceIntent(alloc, repair_id, db_mod.repair_completion_test_options);
            try std.testing.expect(!step.terminal);
            if (step.repaired) {
                repaired = true;
                break;
            }
        }
        try std.testing.expect(repaired);
        try std.testing.expectEqual(@as(u64, 1), db.core.index_manager.denseIndex(cfg.name).?.index.stats().active_count);
    }
}

test "db graph projected endpoint retirement survives restore index recreation" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("review-retirement-index");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    const cfg = db_mod.types.IndexConfig{ .name = "facts", .kind = .graph, .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
    };
    try db.addIndex(cfg);
    try db.batch(.{ .writes = &.{
        .{ .key = "a", .value = "{\"relations\":{\"relations\":[{\"type\":\"R\",\"target\":{\"document_id\":\"b\"}}]}}" },
        .{ .key = "b", .value = "{}" },
    }, .sync_level = .full_index });
    try db.runUntilIdle();
    try db.batch(.{ .deletes = &.{"b"}, .sync_level = .full_index });
    try db.runUntilIdle();
    const input_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "a", "asset", "relations_v1");
    defer alloc.free(input_key);
    const input_value = try db.core.store.get(alloc, input_key);
    defer alloc.free(input_value);
    try std.testing.expect(try db.deleteIndex("facts"));
    try db.runUntilIdle();
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);
    try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{}" }}, .sync_level = .full_index });
    try db.addIndex(cfg);
    try db.core.store.put(input_key, input_value);
    const changed = try db_mod.materializeGraphSourceArtifactsForIndex(alloc, db.core.store, db.core.index_manager, &.{input_key}, "facts", .{});
    defer db_mod.freeOwnedKeySlice(alloc, changed);
    try db_mod.applyGraphArtifactMutationPages(db.async_context, "facts", &.{}, changed, 0);
    try db.runUntilIdle();
    const edges = try db.getEdges(alloc, "facts", "a", "", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
}

test "db prepared posting refresh rejects index replacement and releases capture ownership" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    const config: db_mod.types.IndexConfig = .{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"use_quantization\":false}",
    };
    try db.addIndex(config);
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1.0,0.0]}" }}, .sync_level = .full_index });
    const entry = db.core.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try entry.index.markNodePostingDirtyForTest(entry.index.metadata.root_node);
    const manager = entry.index.resource_manager;
    entry.index.resource_manager = null;
    const prepared = (try db.core.index_manager.captureDensePostingRefresh()).?;
    defer prepared.deinit();
    entry.index.resource_manager = manager;
    // No capture lock survives into computation: both reader and writer
    // admission are available, and replacement can retire the original index.
    try std.testing.expect(db.core.tryLockApplyShared());
    db.core.unlockApplyShared();
    try std.testing.expect(entry.apply_mutex.tryLock());
    entry.apply_mutex.unlock();
    // Retire the document first so recreating the index does not invoke the
    // independent backfill/projection-publication lifecycle in this test.
    try db.batch(.{ .deletes = &.{"doc:a"}, .sync_level = .full_index });
    try std.testing.expect(try db.deleteIndex("dv_v1"));
    for (0..1000) |_| {
        if (try db.advanceGeneratedArtifactCleanupPage("dv_v1") == .idle) break;
        db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);
    }
    // Computation remains valid after the original index has been destroyed;
    // only its attempt to publish must be rejected.
    try prepared.build();
    try db.addIndex(config);
    const replacement = db.core.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try std.testing.expect(replacement.capture_incarnation != prepared.identity);
    // Force an equal epoch to prove incarnation, rather than accidental epoch
    // inequality, is what rejects reuse of the same catalog name.
    prepared.epoch = replacement.index.published_mutation_epoch.load(.acquire);
    const result = try db.core.index_manager.publishPreparedDensePostingRefresh(prepared);
    try std.testing.expect(result.pending);
    try std.testing.expectEqual(@as(usize, 0), result.repaired);
}

test "source vector migration consolidates ANN serving while preserving queries and last-index ownership" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("migration-ann");
    defer tmp.cleanup();
    const path = std.mem.span(tmp.path().ptr);
    var db = try DB.open(alloc, path, .{
        .table_storage = .{ .dense_embeddings = .primary_lsm },
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    const cfg: db_mod.types.IndexConfig = .{ .name = "semantic", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"embedding_name\":\"shared\"}" };
    try db.addIndex(cfg);
    try db.batch(.{ .writes = &.{
        .{ .key = "a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"shared\":[1,0,0]}}" },
        .{ .key = "b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"shared\":[0,1,0]}}" },
    }, .sync_level = .write });
    try db.runUntilIdle();
    const request: db_mod.vector_migration.contract.Request = .{ .job_id = "ann", .mode = .online, .budget = .{ .batch_rows = 3 } };
    try db.startVectorMigration(request);
    try std.testing.expectError(error.VectorMigrationActive, db.deleteIndex(cfg.name));
    var checked_reclamation_admission = false;
    for (0..256) |_| {
        var status = (try db_mod.vector_migration.load(alloc, db.core.store)).?;
        defer status.deinit();
        var result = try db.search(alloc, .{ .index_name = "semantic", .dense = .{ .vector = &.{ 1, 0, 0 }, .k = 2 }, .limit = 2 });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 2), result.total_hits);
        try std.testing.expectEqualStrings("a", result.hits[0].id);
        if (status.value.phase == .complete) break;
        if (status.value.phase == .reclaiming and !status.value.primary_reclamation_requested and !checked_reclamation_admission) {
            // Model the recovery interval before the source binding is ready.
            // A persisted serving phase must not authorize destructive work
            // against an unavailable projection in the new owner lifetime.
            const manager = db.core.index_manager;
            const source = manager.source_payload_store;
            manager.source_payload_store = null;
            defer manager.source_payload_store = source;
            try db.advanceVectorMigration(request.job_id);
            var pending = (try db_mod.vector_migration.load(alloc, db.core.store)).?;
            defer pending.deinit();
            try std.testing.expectEqual(db_mod.vector_migration.contract.Phase.reclaiming, pending.value.phase);
            try std.testing.expect(!pending.value.primary_reclamation_requested);
            checked_reclamation_admission = true;
            continue;
        }
        if (status.value.phase == .ready) try db.publishVectorMigration(request.job_id) else try db.advanceVectorMigration(request.job_id);
    } else return error.VectorMigrationDidNotFinish;
    try std.testing.expect(checked_reclamation_admission);
    try std.testing.expect(db.core.index_manager.sourceMigrationServingComplete());
    try std.testing.expect(try db.deleteIndex(cfg.name));
    const key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "a", "shared");
    defer alloc.free(key);
    const retained = try db.core.store.get(alloc, key);
    defer alloc.free(retained);
    try std.testing.expectEqual(@as(usize, 3), try db_mod.enrichment_artifact_codec.decodeDenseEmbeddingDims(retained));
    _ = try db.admitManagedIndex(.{ .name = "replacement", .kind = cfg.kind, .config_json = cfg.config_json });
    _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    const repair_id = (try db.indexes.repair.idForIndex(alloc, "replacement")) orelse return error.TestUnexpectedResult;
    _ = try db.indexes.repair.advanceIntent(alloc, repair_id, .{});
    try db.runUntilIdle();
    var result = try db.search(alloc, .{ .index_name = "replacement", .dense = .{ .vector = &.{ 1, 0, 0 }, .k = 2 }, .limit = 2 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
}

test "lite external dense dimension replacement keeps writes available (#1216)" {
    const a = std.testing.allocator;
    const lite = @import("../../lite/backend.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/dimension.aflite", .{tmp.sub_path});
    defer a.free(path);
    var handle = try lite.Handle.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .enabled = false } });
    defer handle.deinit();
    var opts = db_mod.OpenOptions{ .start_index_workers = true, .start_optional_runtimes = false, .ttl_cleanup = .{ .enabled = false } };
    try handle.configureDbOpenOptionsForNamespace(&opts, &tmp.sub_path);
    var db = try DB.open(a, path, opts);
    defer db.close();
    _ = try db.admitManagedIndex(.{ .name = "img", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}" });
    var keys: [20][32]u8 = undefined;
    var old_writes: [20]db_mod.types.BatchWrite = undefined;
    for (&old_writes, &keys, 0..) |*write, *key, i| write.* = .{
        .key = try std.fmt.bufPrint(key, "old-{d}", .{i}),
        .value = "{\"body\":\"old document\",\"_embeddings\":{\"img\":[1,0,0]}}",
    };
    try db.batch(.{ .writes = &old_writes });
    try db.runUntilIdle();
    try std.testing.expect(try db.deleteIndex("img"));
    // Deletion schedules cleanup on the durable owner. Join that owner before
    // reusing the name: a nonblocking .busy response is lease contention, not
    // another page of work, and tight polling can exhaust an arbitrary budget
    // while the finalizer is still checkpointing on a loaded test host.
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);
    try std.testing.expectEqual(DB.GeneratedArtifactCleanupAdvanceResult.idle, try db.advanceGeneratedArtifactCleanupPage("img"));
    const repair = try db.admitManagedIndex(.{ .name = "img", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":4,\"external\":true}" });
    // Admission must not block either vector or ordinary writes while its
    // new generation is still rebuilding from retained authored artifacts.
    try db.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"body\":\"new document\",\"_embeddings\":{\"img\":[0,1,0,0]}}" }} });
    try db.batch(.{ .writes = &.{.{ .key = "plain", .value = "{\"body\":\"no vector\"}" }} });
    if (repair) |id| {
        var complete = false;
        for (0..20) |_| {
            if ((try db.indexes.repair.advanceIntent(a, id, db_mod.repair_completion_test_options)).repaired) {
                complete = true;
                break;
            }
        }
        try std.testing.expect(complete);
    }
    for (0..2) |round| {
        if (round == 1) {
            db.close();
            handle.deinit();
            handle = try lite.Handle.open(a, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .enabled = false } });
            opts = .{ .start_index_workers = true, .start_optional_runtimes = false, .ttl_cleanup = .{ .enabled = false } };
            try handle.configureDbOpenOptionsForNamespace(&opts, &tmp.sub_path);
            db = try DB.open(a, path, opts);
        }
        try db.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"body\":\"new document\",\"_embeddings\":{\"img\":[0,1,0,0]}}" }} });
        try db.batch(.{ .writes = &.{.{ .key = "plain", .value = "{\"body\":\"no vector\"}" }} });
        try db.runUntilIdle();
        var result = try db.search(a, .{ .index_name = "img", .query = .{ .dense_knn = .{ .vector = &.{ 0, 1, 0, 0 }, .k = 8 } }, .limit = 8 });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("new", result.hits[0].id);
        try std.testing.expectEqual(@as(?u64, 1), try DB.loadDenseArtifactTargetCounter(a, db.core.store, "img"));
        const checkpoint = try db.core.loadProjectionCheckpoint(a, "img");
        try std.testing.expectEqual(db_mod.apply_state.ProjectionStatus.clean, checkpoint.status);
    }
}

test "lite generation cleanup and capacity accounting survive reopen (#1210)" {
    const a = std.testing.allocator;
    const lite = @import("../../lite/backend.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/maintenance.aflite", .{tmp.sub_path});
    defer a.free(path);
    var handle = try lite.Handle.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .enabled = false } });
    defer handle.deinit();
    var opts = db_mod.OpenOptions{ .start_index_workers = false, .start_optional_runtimes = false, .ttl_cleanup = .{ .enabled = false } };
    try handle.configureDbOpenOptionsForNamespace(&opts, &tmp.sub_path);
    var db = try DB.open(a, path, opts);
    defer db.close();
    // Isolate explicit collector turns from the one-shot cleanup scheduled by open.
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);
    const base = try a.dupe(u8, db.core.index_manager.base_path);
    defer a.free(base);
    const orphan = try std.fs.path.join(a, &.{ base, ".repair-shadow-orphan" });
    defer a.free(orphan);
    const orphan_file = try std.fs.path.join(a, &.{ orphan, "indexes/img/payload" });
    defer a.free(orphan_file);
    const active_file = try std.fs.path.join(a, &.{ base, ".repair-shadow-active/indexes/img/payload" });
    defer a.free(active_file);
    const active_pointer = try std.fs.path.join(a, &.{ base, "indexes/img/.antfly-active-index-root" });
    defer a.free(active_pointer);
    const stale = try std.fs.path.join(a, &.{ base, ".repair-shadow-stale/.antfly-repair-shadow-in-progress" });
    defer a.free(stale);
    const pending = try std.fs.path.join(a, &.{ base, ".repair-shadow-pending" });
    defer a.free(pending);
    const pending_file = try std.fs.path.join(a, &.{ pending, "indexes/img/payload" });
    defer a.free(pending_file);
    const corrupt_file = try std.fs.path.join(a, &.{ base, ".repair-shadow-corrupt/indexes/broken/payload" });
    defer a.free(corrupt_file);
    const corrupt_pointer = try std.fs.path.join(a, &.{ base, "indexes/broken/.antfly-active-index-root" });
    defer a.free(corrupt_pointer);
    const algebraic_path = try std.fs.path.join(a, &.{ base, ".repair-shadow-algebraic/indexes/alg" });
    defer a.free(algebraic_path);
    const algebraic_marker = try std.fs.path.join(a, &.{ algebraic_path, ".antfly-generation-directory" });
    defer a.free(algebraic_marker);
    const fact_prefix = try db_mod.algebraic_mod.index.storagePrefixAlloc(a, ".repair-shadow-algebraic/alg");
    defer a.free(fact_prefix);
    const fact_key = try std.fmt.allocPrint(a, "{s}|8:orphan", .{fact_prefix});
    defer a.free(fact_key);
    const payload = "0123456789012345678901234567890123456789";
    {
        const manager = db.core.index_manager;
        const storage = manager.generation_storage.?;
        try storage.writeFileAbsolute(orphan_file, payload);
        try storage.writeFileAbsolute(stale, "antfly-repair-shadow-in-progress-v1\npid=2147483647\n");
        try storage.writeFileAbsolute(active_file, payload);
        // The collector must honor pointers even without a loaded catalog row.
        try storage.writeFileAbsolute(active_pointer, "antfly-active-index-root-v1\n.repair-shadow-active/indexes/img\n");
        try manager.markRepairShadow(a, pending);
        try storage.writeFileAbsolute(pending_file, payload);
        try storage.writeFileAbsolute(corrupt_file, payload);
        try storage.writeFileAbsolute(corrupt_pointer, "corrupt");
        try manager.createGenerationDirectory(algebraic_path);
        try db.core.store.put(fact_key, "fact");

        try std.testing.expectEqual(@as(u64, 40), try manager.activeIndexStorageBytes("img"));
        const cfg: db_mod.types.IndexConfig = .{ .name = "img", .kind = .dense_vector, .config_json = "{}" };
        try std.testing.expectEqual(@as(u64, 40), try db_mod.repairCandidateStorageBytes(a, cfg, manager, orphan));
        const Capacity = struct {
            available: u64 = 100,
            fn observe(ptr: *anyopaque) anyerror!db_mod.resource_manager_mod.CapacityObservation {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                return .{ .available_bytes = self.available, .capacity_bytes = 100, .observed_at_ns = db_mod.monotonicTimeNs(), .valid_for_ns = 5 * std.time.ns_per_s };
            }
        };
        var capacity: Capacity = .{};
        var resources = db_mod.resource_manager_mod.ResourceManager.init(.{ .disk_safety_floor_bytes = 0, .disk_safety_floor_divisor = 0 });
        defer resources.deinit(a);
        var reservation = try resources.reserveCapacity(a, 9, 80, try Capacity.observe(&capacity), db_mod.monotonicTimeNs());
        defer reservation.release();
        var guard: DB.RepairCapacityGuard = .{
            .reservation = &reservation,
            .db = &db,
            .alloc = a,
            .repair_id = 0,
            .options = .{ .capacity_source = .{ .ptr = &capacity, .domain_id = 9, .observe = Capacity.observe } },
            .admitted_total_bytes = 80,
            .headroom_bytes = 0,
        };
        try DB.RepairCapacityGuard.bindCandidateRoot(&guard, orphan);
        capacity.available = 60;
        try DB.RepairCapacityGuard.revalidate(&guard);
        try std.testing.expectEqual(@as(u64, 40), resources.capacityStats().reserved_bytes);
        try std.testing.expectEqual(@as(u64, 0), resources.capacityStats().denials);
        // One bounded deletion page removes the fact, leaving its durable
        // generation marker for the next collector instance to finish.
        try std.testing.expect(try manager.cleanupInactiveRepairShadowRootsPage());
        try std.testing.expectEqual(@as(u64, 0), try storage.fileSize(algebraic_marker));
    }
    db.close();
    handle.deinit();
    handle = try lite.Handle.open(a, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .enabled = false } });
    opts = .{ .start_index_workers = false, .start_optional_runtimes = false, .ttl_cleanup = .{ .enabled = false } };
    try handle.configureDbOpenOptionsForNamespace(&opts, &tmp.sub_path);
    db = try DB.open(a, path, opts);
    db.backend_runtime.durable_jobs.drainOwner(db.indexes.repair.cleanup_owner_id);
    const manager = db.core.index_manager;
    const storage = manager.generation_storage.?;
    var pages: usize = 0;
    while (try manager.cleanupInactiveRepairShadowRootsPage()) : (pages += 1) try std.testing.expect(pages < 20);
    try std.testing.expectError(error.FileNotFound, storage.fileSize(orphan_file));
    try std.testing.expectError(error.FileNotFound, storage.fileSize(stale));
    try std.testing.expectError(error.FileNotFound, storage.fileSize(algebraic_marker));
    try std.testing.expectError(error.NotFound, db.core.store.get(a, fact_key));
    try std.testing.expectEqual(@as(u64, 40), try storage.fileSize(active_file));
    try std.testing.expectEqual(@as(u64, 40), try storage.fileSize(pending_file));
    try std.testing.expectEqual(@as(u64, 40), try storage.fileSize(corrupt_file));
    try manager.clearRepairShadow(a, pending);
    try storage.deleteFileAbsolute(corrupt_pointer);
    while (try manager.cleanupInactiveRepairShadowRootsPage()) {}
    try std.testing.expectError(error.FileNotFound, storage.fileSize(pending_file));
    try std.testing.expectError(error.FileNotFound, storage.fileSize(corrupt_file));
    try std.testing.expectEqual(@as(u64, 40), try storage.fileSize(active_file));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, base, .{}));
}

test "capi native memory standalone external maintenance completes managed text admission" {
    const a = std.testing.allocator;
    const lite = @import("../../lite/backend.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/admission.aflite", .{tmp.sub_path});
    defer a.free(path);
    var handle = try lite.Handle.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .enabled = false } });
    defer handle.deinit();
    var options = db_mod.OpenOptions{ .start_index_workers = false, .start_optional_runtimes = false, .start_independent_maintenance = false, .ttl_cleanup = .{ .enabled = false } };
    try handle.configureDbOpenOptionsForNamespace(&options, &tmp.sub_path);
    var db = try DB.open(a, path, options);
    defer db.close();
    try db.batch(.{ .writes = &.{.{ .key = "source", .value = "{\"body\":\"silver submarine\"}" }}, .sync_level = .write });
    const cfg: db_mod.types.IndexConfig = .{ .name = "text", .kind = .full_text, .config_json = "{\"field\":\"body\"}" };
    _ = (try db.admitManagedIndex(cfg)) orelse return error.ExpectedBackfill;
    try std.testing.expect(db.core.index_manager.repairUnavailable(cfg.name));
    var complete = false;
    for (0..32) |_| {
        _ = try DB.independentMaintenanceRun(&db, .repair);
        if (!try db.indexes.repair.hasPendingIntents(a)) {
            complete = true;
            break;
        }
    }
    try std.testing.expect(complete);
    try std.testing.expect(!db.core.index_manager.repairUnavailable(cfg.name));
    var result = try db.search(a, .{ .index_name = "text", .full_text = .{ .match = .{ .field = "body", .text = "submarine" } }, .limit = 10 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("source", result.hits[0].id);
}
