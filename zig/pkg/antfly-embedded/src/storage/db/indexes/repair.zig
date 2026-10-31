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
