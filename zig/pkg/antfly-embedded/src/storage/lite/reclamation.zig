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

//! Owner shrinking policy and storage admission. Revision 4 retires/reuses pages
//! independently of optional generation shrinking. No integrity walk belongs
//! on the routine commit path.
const std = @import("std");

pub const Options = struct {
    /// Optional physical shrinking. Logical reuse remains independently enabled.
    enabled: bool = true,
    page_reuse: bool = true,
    retirement_work_pages: usize = 128,
    /// Minimum growth interval; exact scans also require proportional change
    /// as the store grows. Retirement uses the smaller of this minimum and
    /// minimum_reclaim_bytes, subject to the same proportional floor.
    assessment_bytes: u64 = 64 * 1024 * 1024,
    minimum_reclaim_bytes: u64 = 256 * 1024 * 1024,
    amplification: u32 = 2,
    retry_ms: u32 = 1000,
    /// Limits combined current, retired, artifact and rewrite workspace bytes. Zero is
    /// unlimited. This is an admission limit, not a filesystem free-space probe.
    max_storage_bytes: u64 = 0,
    /// Optional additional temporary space for a redo format migration or
    /// reservation shrink. The installed generation must fit the base cap.
    redo_rewrite_workspace_overage_bytes: u64 = 0,
    disk_headroom_bytes: u64 = 64 * 1024 * 1024,
    /// Virtual/borrowed runtimes can supply capacity without escaping their
    /// filesystem. Native owners use the platform probe when supported.
    capacity_probe: ?CapacityProbe = null,

    pub fn validate(self: Options) !void {
        if (self.assessment_bytes == 0 or self.minimum_reclaim_bytes == 0 or
            self.amplification < 2 or self.retry_ms == 0 or self.retirement_work_pages == 0 or
            (self.max_storage_bytes != 0 and self.max_storage_bytes < 4096)) return error.InvalidLiteMaintenanceOptions;
    }
};

pub const CapacityProbe = struct {
    context: *anyopaque,
    available: *const fn (*anyopaque, []const u8) anyerror!u64,
};

pub const State = enum { disabled, idle, assessing, rewriting, deferred, failed };
pub const Reason = enum { none, disabled, concurrency_unavailable, writer_busy, retained_readers, storage_budget, low_disk, canceled, maintenance_error };

/// Counts follow the allocator cache across serving replacements. Appended
/// pages include attempted allocation in subsequently rolled-back work.
pub const AllocatorWork = struct {
    full_loads: u64 = 0,
    full_load_ns: u64 = 0,
    journal_rebases: u64 = 0,
    journal_rebase_ns: u64 = 0,
    cache_transfers: u64 = 0,
    /// Warm ledgers copied (not moved) from an idle owner of the same file.
    cache_copies: u64 = 0,
    artifact_source_opens: u64 = 0,
    artifact_source_reuses: u64 = 0,
    digest_rebuilds: u64 = 0,
    digest_rebuild_ns: u64 = 0,
    reuse_lock_refusals: u64 = 0,
    appended_pages: u64 = 0,
    reuse_blocked_append_pages: u64 = 0,
    /// Full `reader_epochs` namespace opens (parent/directory/identity
    /// resolved from scratch): once per `NativeFile`, plus once per rebind
    /// or externally-moved parent directory. Test-observable proof that the
    /// namespace cache amortizes this across commits instead of paying it
    /// on every one.
    reader_namespace_opens: u64 = 0,
    /// Full `reader_epochs.scanCached` directory scans (iterate + trylock
    /// every marker). O(1) per commit regardless of registered-reader count
    /// as long as the readers directory's membership is unchanged between
    /// commits; see `NativeFile.scanReaderEpochsCached`.
    reader_namespace_full_scans: u64 = 0,
    /// `NativeFile.maybeAutoCheckpointRedoLog`'s own opportunistic
    /// redo-log checkpoint attempts that failed (budget, disk, an
    /// allocator invariant, OOM -- any error). That commit still durably
    /// published; only the *opportunistic* follow-up checkpoint did not.
    /// Monotonic, never reset: a maintenance or budget path watching this
    /// climb, with no corresponding drop in `redo_checkpoint_pending`
    /// (`NativeFile.redoCheckpointPendingError`), has a persistently
    /// failing checkpoint on its hands, not a one-off.
    redo_checkpoint_failures: u64 = 0,
    /// Redo-log folds this process published (the checkpoint's
    /// `redo_folded_through` advanced). Monotonic, like the failures.
    redo_checkpoint_folds: u64 = 0,

    pub fn absorb(self: *AllocatorWork, source: *AllocatorWork) void {
        if (self == source) return;
        inline for (comptime std.meta.fieldNames(AllocatorWork)) |name| @field(self, name) +|= @field(source, name);
        source.* = .{};
    }
};

pub const Status = struct {
    state: State = .idle,
    reason: Reason = .none,
    current_file_bytes: u64 = 0,
    allocator_enabled: bool = false,
    shrinking_enabled: bool = false,
    reusable_pages: u64 = 0,
    pending_retirement_objects: u64 = 0,
    pending_data_retirement_objects: u64 = 0,
    reused_pages: u64 = 0,
    allocator_work: AllocatorWork = .{},
    allocator_journal_bytes: u64 = 0,
    data_reuse_frontier: u64 = 0,
    metadata_reuse_frontier: u64 = 0,
    oldest_data_retirement_epoch: ?u64 = null,
    oldest_metadata_retirement_epoch: ?u64 = null,
    coordinated_reader_count: u64 = 0,
    poisoned_coordinated_readers: u64 = 0,
    oldest_coordinated_reader_age_ms: u64 = 0,
    retired_objects_serviced: u64 = 0,
    retirement_activity_bytes: u64 = 0,
    retired_file_bytes: u64 = 0,
    artifact_file_bytes: u64 = 0,
    retired_generations: u64 = 0,
    retained_readers: u64 = 0,
    oldest_reader_age_ms: u64 = 0,
    temporary_bytes: u64 = 0,
    available_disk_bytes: ?u64 = null,
    compact_size_estimate: ?u64 = null,
    live_bytes_estimate: ?u64 = null,
    estimated_at_sequence: ?u64 = null,
    assessment_count: u64 = 0,
    rewrite_count: u64 = 0,
    last_reclaimed_bytes: u64 = 0,
    last_error: ?[]const u8 = null,

    pub fn totalBytes(self: Status) u64 {
        return self.current_file_bytes +| self.retired_file_bytes +| self.temporary_bytes +| self.artifact_file_bytes;
    }
};

pub const Policy = struct {
    options: Options = .{},
    status: Status = .{},
    next_assessment_size: u64 = 0,
    assessment_activity_bytes: u64 = 0,
    retirement_bytes: u64 = 0,
    assessed_retirement_bytes: u64 = 0,
    /// Monotonic: bumped by `requestReassessment` every time something asks
    /// for an out-of-band reassessment (a config change, a restored or
    /// imported generation, or any other caller that wants the owner's own
    /// next `due()` check to notice regardless of size/activity). `completed`
    /// compares this against the value a `Snapshot` captured when its own
    /// vacuum run started, so a request made *while* that run was already in
    /// flight is never silently consumed by its completion -- see
    /// `completed`'s own doc comment.
    reassessment_requested: u64 = 0,
    /// Retained by an external owner's checkpoint refresh even when its exact
    /// assessment cache is discarded. A canceled copy must not require more
    /// file growth before shrinking gets another bounded attempt.
    rewrite_retry_at_ns: ?i96 = null,
    rewrite_retry_delay_ns: i96 = 30 * std.time.ns_per_s,

    pub fn rewriteRetryDue(self: Policy, now_ns: i96) bool {
        return if (self.rewrite_retry_at_ns) |deadline| now_ns >= deadline else false;
    }

    pub fn scheduleRewriteRetry(self: *Policy, now_ns: i96, scan_ns: ?i96) void {
        if (scan_ns) |elapsed| self.rewrite_retry_delay_ns = @max(@as(i96, 30 * std.time.ns_per_s), elapsed *| 64);
        self.rewrite_retry_at_ns = now_ns +| self.rewrite_retry_delay_ns;
    }

    pub fn init(options: Options) !Policy {
        try options.validate();
        return .{ .options = options, .status = .{
            .state = if (options.enabled) .idle else .disabled,
            .reason = if (options.enabled) .none else .disabled,
        } };
    }

    pub fn due(self: Policy, size: u64) bool {
        const activity_due = self.retirement_bytes -| self.assessed_retirement_bytes >=
            self.assessment_activity_bytes;
        return self.options.enabled and size >= self.options.minimum_reclaim_bytes and
            (size >= self.next_assessment_size or activity_due);
    }

    // An exact assessment traverses all live records. Charge at least a
    // quarter of the assessed physical size in growth or committed retirement
    // before another scan, so routine scan work is amortized to changed data.
    // Retirement includes inline deletions, which need not release whole pages.
    // Deliberately does *not* touch `assessed_retirement_bytes`: `assessed`
    // and `completed` each set that from a different, caller-appropriate
    // source (see their own doc comments), not uniformly from "whatever
    // `retirement_bytes` is right now" the way this helper used to for both.
    //
    // `fixed_overhead` (Bug 3, perf/lwa-budget-tail, §9.7a): a redo-log
    // file's own fixed, non-reclaimable redo-log reservation
    // (`NativeFile.header.redo_log_capacity` -- 0 for a legacy file),
    // never part of a rewrite's own potential savings. Only the
    // *proportional* quarter below is computed net of it: `physical`
    // itself (and the absolute `next_assessment_size` threshold derived
    // from it) stays in the same raw-file-size units `due()` compares
    // against, so a caller that always measures the same way (raw
    // `file.stat().size`, this struct's own contract) never sees a unit
    // mismatch between the two.
    fn resetAssessmentBudget(self: *Policy, physical: u64, fixed_overhead: u64) void {
        const proportional = (physical -| fixed_overhead) / 4;
        self.next_assessment_size = physical +| @max(self.options.assessment_bytes, proportional);
        self.assessment_activity_bytes = @max(@min(self.options.assessment_bytes, self.options.minimum_reclaim_bytes), proportional);
    }

    /// Forces the next `due()` check to return true regardless of size or
    /// accrued activity: a config change, a restored/imported generation
    /// adopting an entirely different file, or any other caller that needs
    /// the owner's own next check to notice without waiting for size or
    /// activity to clear the ordinary thresholds on their own. The one
    /// correct way to ask for this -- see `completed`'s own doc comment for
    /// why directly poking `next_assessment_size` from outside, the way this
    /// replaces, could have its effect silently discarded by a vacuum that
    /// was already in flight when the request was made.
    pub fn requestReassessment(self: *Policy) void {
        self.next_assessment_size = 0;
        self.reassessment_requested +%= 1;
    }

    pub fn assessed(self: *Policy, physical: u64, compact: u64, live: u64, sequence: u64, fixed_overhead: u64) bool {
        self.resetAssessmentBudget(physical, fixed_overhead);
        // Assessment time, not completion time: this *is* "what this
        // decision is based on", the correct baseline `due()`'s activity
        // half measures fresh accrual against until the next assessment or
        // completion updates it.
        self.assessed_retirement_bytes = self.retirement_bytes;
        self.status.assessment_count +|= 1;
        self.status.compact_size_estimate = compact;
        self.status.live_bytes_estimate = live;
        self.status.estimated_at_sequence = sequence;
        return self.worthwhile(physical, compact, fixed_overhead);
    }

    // `fixed_overhead`: see `resetAssessmentBudget`'s own doc comment.
    // Unlike that function, every use of `physical` here is proportional
    // (how much of it is reclaimable relative to `compact`), so the
    // reservation is subtracted from `physical` itself, not just an
    // increment derived from it: the reservation is physically always
    // present and never shrinks, so it would otherwise count as
    // permanently "dead" space no rewrite can ever actually recover,
    // keeping a redo-log file perpetually "worthwhile" to rewrite on
    // every assessment even with no real savings available.
    pub fn worthwhile(self: Policy, physical: u64, compact: u64, fixed_overhead: u64) bool {
        const reclaimable = physical -| fixed_overhead;
        return reclaimable -| compact >= self.options.minimum_reclaim_bytes and
            reclaimable > compact *| self.options.amplification;
    }

    /// A point-in-time capture of exactly what a vacuum run saw when it
    /// started -- the correct baseline for that same run's own `completed`
    /// call to consume. `performVacuum`'s round loop (`docstore.zig`) can run
    /// for a while, contended, escalating through a fair writer ticket; by
    /// the time it returns, fresh retirement debt may have accrued, and
    /// `requestReassessment` may have been called, *during* that run, not
    /// before it. Capture this right before starting the run, not read
    /// `retirement_bytes`/`reassessment_requested` fresh inside `completed`
    /// itself once it is already too late to tell "existed before this run"
    /// apart from "arrived during it".
    pub const Snapshot = struct {
        retirement_bytes: u64,
        reassessment_requested: u64,
    };

    pub fn snapshot(self: Policy) Snapshot {
        return .{ .retirement_bytes = self.retirement_bytes, .reassessment_requested = self.reassessment_requested };
    }

    /// `started`: this same vacuum run's own `snapshot()`, taken before it
    /// began. Two things this deliberately does *not* do, both because
    /// `resetAssessmentBudget`'s old shared baseline -- "whatever
    /// `retirement_bytes`/`next_assessment_size` happen to be right now" --
    /// silently discarded whatever happened *during* a long-running,
    /// possibly-contended vacuum (confirmed live: the owner going idle with
    /// no error while the file stayed well over its envelope, the exact
    /// same byte count every time -- a vacuum that started earlier finishing
    /// after newer retirement debt or an explicit `requestReassessment` call
    /// that arrived during its own run):
    ///
    /// 1. `assessed_retirement_bytes` is set from `started.retirement_bytes`
    ///    (what this run *actually* started with), never from `self.
    ///    retirement_bytes`'s current value -- debt that accrued while this
    ///    run was in flight stays outstanding for `due()`'s activity half to
    ///    see, instead of being marked "already assessed" by a run that
    ///    never looked at it.
    /// 2. `next_assessment_size` only resets to the ordinary, comfortable
    ///    post-rewrite threshold when no `requestReassessment` happened
    ///    since `started` was captured. One that did happen keeps the
    ///    size-based trigger forced low: this run's own result cannot have
    ///    satisfied a request it did not yet exist to observe.
    pub fn completed(self: *Policy, physical: u64, reclaimed: u64, started: Snapshot, fixed_overhead: u64) void {
        const requested_during_run = self.reassessment_requested != started.reassessment_requested;
        self.resetAssessmentBudget(physical, fixed_overhead);
        if (requested_during_run) self.next_assessment_size = 0;
        self.assessed_retirement_bytes = started.retirement_bytes;
        self.status.rewrite_count +|= 1;
        self.status.last_reclaimed_bytes = reclaimed;
        self.rewrite_retry_at_ns = null;
        self.status.state = if (self.options.enabled) .idle else .disabled;
        self.status.reason = if (self.options.enabled) .none else .disabled;
        self.status.last_error = null;
        // Catch-up can change the live roots. The pre-copy estimate is stale.
        self.status.estimated_at_sequence = null;
    }
};

test "lite reclamation policy uses hysteresis and saturating size arithmetic" {
    var policy = try Policy.init(.{ .assessment_bytes = 64, .minimum_reclaim_bytes = 256 });
    try std.testing.expect(!policy.due(255));
    try std.testing.expect(policy.due(1024));
    try std.testing.expect(policy.assessed(1024, 300, 200, 7, 0));
    try std.testing.expect(!policy.due(1025));
    policy.completed(300, 724, policy.snapshot(), 0);
    try std.testing.expect(!policy.due(374));
    try std.testing.expect(policy.due(375));
    try std.testing.expect(!policy.assessed(600, 300, 200, 8, 0));
    try std.testing.expect(!policy.assessed(std.math.maxInt(u64), std.math.maxInt(u64) / 2 + 1, 0, 9, 0));
    try std.testing.expectEqual(std.math.maxInt(u64), policy.next_assessment_size);
    try std.testing.expectError(error.InvalidLiteMaintenanceOptions, Policy.init(.{ .assessment_bytes = 0 }));
    try std.testing.expectError(error.InvalidLiteMaintenanceOptions, Policy.init(.{ .max_storage_bytes = 4095 }));
}

// Regression: `completed` used to call the same `resetAssessmentBudget`
// `assessed` does, baselining `assessed_retirement_bytes` from whatever
// `retirement_bytes` happened to be *at completion time* -- "now", not what
// the run that is completing actually started with. A `requestReassessment`
// call made while that run was still in flight (its own `Snapshot` already
// captured, before the request) got silently overwritten back to the
// ordinary, comfortable threshold the instant that run's own (unrelated,
// already-stale) completion ran -- the request never took effect.
test "lite reclamation requestReassessment made during a vacuum survives that vacuum's own completion" {
    var policy = try Policy.init(.{ .assessment_bytes = 64, .minimum_reclaim_bytes = 256 });
    try std.testing.expect(policy.assessed(1024, 300, 200, 7, 0));
    try std.testing.expect(!policy.due(1024));
    // The vacuum this snapshot belongs to captures it here, before it runs.
    const started = policy.snapshot();
    // While that run is still in flight, something else -- a restored or
    // imported generation, a config change, or simply another caller --
    // asks for an out-of-band reassessment.
    policy.requestReassessment();
    // The run finishes and reports completion using the snapshot it
    // captured before that request ever existed. A large `physical` here
    // is deliberate: the *ordinary* post-completion threshold this would
    // otherwise set (`physical + max(assessment_bytes, physical/4)`,
    // comfortably above 1024) must not be what makes `due()` true below --
    // only the request-preservation path can, by forcing it back to zero.
    policy.completed(5000, 724, started, 0);
    try std.testing.expectEqual(@as(u64, 0), policy.next_assessment_size);
    // The request must not have been discarded by that stale completion:
    // `due()` must say yes regardless of size now.
    try std.testing.expect(policy.due(1024));
}

// Regression, the other half of the same bug: fresh retirement debt that
// accrues *during* a vacuum run -- not just an explicit request -- must
// also stay outstanding for the next `due()` check, not get marked
// "already assessed" by a completion that started before that debt existed
// and never looked at it. Numbers chosen so the size-based half of `due()`
// alone would say no (`next_assessment_size` ends up well above 1024) --
// only the activity-based half, correctly baselined, can carry this.
test "lite reclamation completed from a stale snapshot does not discard debt accrued during the run" {
    var policy = try Policy.init(.{ .assessment_bytes = 64, .minimum_reclaim_bytes = 256 });
    try std.testing.expect(policy.assessed(1024, 300, 200, 7, 0));
    const started = policy.snapshot();
    // Retirement debt from concurrent writes accrues while this run --
    // already holding `started` -- is still in flight.
    policy.retirement_bytes += 1000;
    policy.completed(2000, 500, started, 0);
    try std.testing.expect(policy.next_assessment_size > 1024);
    try std.testing.expectEqual(started.retirement_bytes, policy.assessed_retirement_bytes);
    try std.testing.expect(policy.due(1024));
}

test "lite reclamation assessments amortize steady reuse and growth to store size" {
    const mib = 1024 * 1024;
    for ([_]u64{ 1024 * mib, 16 * 1024 * mib, 64 * 1024 * mib }) |physical| {
        var policy = try Policy.init(.{});
        try std.testing.expect(!policy.assessed(physical, physical - 4096, physical - 8192, 1, 0));
        // Identical 2 GiB of updates causes fewer scans as the store grows.
        // Each scan requires proportional retirement, even with no file growth.
        var scanned_bytes: u64 = 0;
        for (0..32) |i| {
            policy.retirement_bytes += 64 * mib;
            if (policy.due(physical)) {
                scanned_bytes += physical;
                try std.testing.expect(!policy.assessed(physical, physical - 4096, physical - 8192, i + 2, 0));
            }
        }
        try std.testing.expect(scanned_bytes <= policy.retirement_bytes * 4);
        const activity = policy.retirement_bytes;
        // File growth obeys the same proportional budget independently.
        try std.testing.expect(!policy.due(policy.next_assessment_size - 1));
        try std.testing.expect(policy.due(policy.next_assessment_size));
        try std.testing.expectEqual(activity, policy.retirement_bytes);
    }
}

test "lite reclamation proportional assessments still observe deletion without growth" {
    const physical = 16 * 1024 * 1024 * 1024;
    var policy = try Policy.init(.{});
    try std.testing.expect(!policy.assessed(physical, physical, physical, 1, 0));
    var compact: u64 = physical;
    // Includes inline retirement: a shrinking trigger must not require free
    // whole pages or increasing file size. At most a quarter-file of activity
    // separates the exact assessments as amplification crosses the threshold.
    for (0..3) |i| {
        policy.retirement_bytes += physical / 4;
        compact -= physical / 4;
        try std.testing.expect(policy.due(physical));
        try std.testing.expectEqual(i == 2, policy.assessed(physical, compact, compact, i + 2, 0));
    }
}
