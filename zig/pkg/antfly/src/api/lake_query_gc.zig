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

//! Query-artifact maintenance owns its own durable store witness. Index GC
//! may finish independently; all categories share one admitted work budget.
const std = @import("std");
const local = @import("antfly_local_sources");
const Store = @import("lake_index_store.zig").Store;
const artifacts = @import("../serverless/artifacts/store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const clock = @import("antfly_platform").time;
const A = std.mem.Allocator;
// Current row payloads and recent jobs retain at most 24h. Admit a second day
// for in-flight publication; a renewed/active record also keeps the witness.
pub const horizon_ms = 2 * std.time.ms_per_day;
pub const renew_ms = std.time.ms_per_hour;
pub const Result = struct { more: bool = false, retained: bool = false, failed: bool = false, progressed: bool = false };
const Revision = @import("objectstore").ObjectRevision;
pub const CompletionProofs = struct {
    const Proof = union(enum) { unknown, absent, revision: Revision };
    allocator: A,
    store: [32]u8 = @splat(0),
    table: u64 = 0,
    horizon: u64 = 0,
    bound: bool = false,
    slots: [5]Proof = @splat(.unknown),
    pub fn init(a: A) CompletionProofs {
        return .{ .allocator = a };
    }
    pub fn deinit(self: *CompletionProofs) void {
        for (&self.slots) |*slot| {
            if (slot.* == .revision) slot.revision.deinit(self.allocator);
            slot.* = .unknown;
        }
        self.bound = false;
    }
    fn begin(self: *CompletionProofs, store: [32]u8, table: u64, horizon: u64) void {
        self.deinit();
        self.store = store;
        self.table = table;
        self.horizon = horizon;
        self.bound = true;
    }
    fn matches(self: *const CompletionProofs, store: [32]u8, table: u64, horizon: u64) bool {
        return self.bound and self.table == table and self.horizon == horizon and std.mem.eql(u8, &self.store, &store);
    }
};
pub const Budget = struct {
    deletes: usize,
    reads: u64,
    work: usize,
    deadline_ns: u64,
    cancellation: Cancellation = .none,
    sweep_interval_ms: u64 = 30_000,
    // Once the durable writer horizon has passed, absent empty categories
    // must stay absent so bounded checkpoint retirement can finish.
    retire_after_ms: u64 = 0,
    // Turn-owned CAS proofs avoid fetching every completion marker twice.
    proofs: ?*CompletionProofs = null,
    proof_category: ?usize = null,
    fn completion(self: *Budget, revision: ?Revision, retained: bool, swept_after: u64) !void {
        if (!self.retiring() or retained or swept_after <= self.retire_after_ms) return;
        const proofs = self.proofs orelse return;
        const category = self.proof_category orelse return;
        const next: CompletionProofs.Proof = if (revision) |value| .{ .revision = try value.clone(proofs.allocator) } else .absent;
        if (proofs.slots[category] == .revision) proofs.slots[category].revision.deinit(proofs.allocator);
        proofs.slots[category] = next;
    }
    fn retiring(self: *const Budget) bool {
        return self.retire_after_ms != 0;
    }
    pub fn check(self: *const Budget) !void {
        try self.cancellation.check();
        if (clock.monotonicNs() >= self.deadline_ns) return error.DeadlineExceeded;
    }
    pub fn visit(self: *Budget) !void {
        try self.check();
        if (self.work == 0) return error.QueryGcBudgetExhausted;
        self.work -= 1;
    }
    pub fn remove(self: *Budget) !void {
        try self.check();
        if (self.deletes == 0) return error.QueryGcBudgetExhausted;
        self.deletes -= 1;
    }
    pub fn token(self: *const Budget) Cancellation {
        return .{ .ptr = self, .check_fn = checkRaw, .is_cancelled_fn = canceled };
    }
    fn checkRaw(raw: *const anyopaque) !void {
        return (@as(*const Budget, @ptrCast(@alignCast(raw)))).check();
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Budget = @ptrCast(@alignCast(raw));
        // Transport probes may run from a canceled task; only the explicit
        // check path may execute fallible owner-renewal callbacks.
        return self.cancellation.isCancelled() or clock.monotonicNs() >= self.deadline_ns;
    }
};
// A fixed, allocation-free admission memo only suppresses redundant metadata
// reads. A cache miss always rechecks durable authority before publication.
pub const AdmissionCache = struct {
    const Entry = struct { table: u64, store: [32]u8, until_ms: u64 };
    entries: [256]?Entry = @splat(null),
    next: usize = 0,
    mutex: std.atomic.Mutex = .unlocked,
    pub fn contains(self: *AdmissionCache, table: u64, store: [32]u8, now: u64) bool {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        for (self.entries) |item| if (item) |v| if (v.table == table and std.mem.eql(u8, &v.store, &store) and v.until_ms > now) return true;
        return false;
    }
    pub fn remember(self: *AdmissionCache, table: u64, store: [32]u8, until: u64) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.entries[self.next] = .{ .table = table, .store = store, .until_ms = until };
        self.next = (self.next + 1) % self.entries.len;
    }
};
pub fn collect(a: A, store: *Store, table: u64, phase: u8, now: u64, budget: *Budget) Result {
    var result: Result = .{};
    if (budget.proofs) |proofs| proofs.begin(store.identity, table, budget.retire_after_ms);
    const previous_category = budget.proof_category;
    defer budget.proof_category = previous_category;
    // Rotate even after a poisoned category or an exhausted budget. The phase
    // is persisted with the metadata witness, so restart cannot starve a lane.
    for (0..5) |offset| {
        const category = (@as(usize, phase) + offset) % 5;
        budget.proof_category = category;
        const page = (switch (category) {
            0...2 => collectScopes(a, store, table, scopeDomain(a, store, table, category) catch {
                result.failed = true;
                continue;
            }, now, budget),
            3 => collectRecords(a, store, table, "recent-search", now, budget),
            4 => collectRecords(a, store, table, "row-enrichment", now, budget),
            else => unreachable,
        }) catch |err| {
            result.more = true;
            if (err != error.QueryGcBudgetExhausted and err != error.DeadlineExceeded and err != error.Canceled and err != error.Cancelled) {
                result.failed = true;
                std.log.warn("query artifact collection deferred table_id={d} category={d} err={s}", .{ table, category, @errorName(err) });
            }
            continue;
        };
        result.more = result.more or page.more;
        result.retained = result.retained or page.retained;
        result.failed = result.failed or page.failed;
        result.progressed = result.progressed or page.progressed;
    }
    return result;
}
fn scopeDomain(a: A, store: *Store, table: u64, category: usize) ![32]u8 {
    return switch (category) {
        0 => @import("lake_sql_retained_cut.zig").domain(table, try @import("native_retained_cut.zig").storeIdentity(a, store.locator)),
        1 => @import("lake_retained_cut.zig").domain(table, if (table == 0) try @import("native_retained_cut.zig").storeIdentity(a, store.locator) else store.identity),
        2 => @import("lake_recent_vectors.zig").scopeDomain(table, store.identity),
        else => unreachable,
    };
}

/// Remove only completed, unretained checkpoints before consuming the store's
/// durable ownership witness. A short budget resumes this idempotent cleanup
/// next turn; a concurrently advanced checkpoint retains ownership instead.
pub fn retireCompleted(a: A, store: *Store, table: u64, proofs: *const CompletionProofs, progressed: *bool, budget: *Budget) !void {
    if (!proofs.matches(store.identity, table, budget.retire_after_ms)) return error.QueryGcRetainedProgress;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var client = store.opened.client;
    for (proofs.slots, 0..) |proof, category| {
        const revision = switch (proof) {
            .unknown => return error.QueryGcRetainedProgress,
            .absent => continue,
            .revision => |value| value,
        };
        const name = if (category < 3)
            try std.fmt.allocPrint(scratch, "scope-{s}", .{std.fmt.bytesToHex(&(try scopeDomain(scratch, store, table, category)), .lower)})
        else if (category == 3) "recent-search" else "row-enrichment";
        const key = try std.fmt.allocPrint(scratch, "{s}{s}expiring-gc/{d}/{s}.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", table, name });
        try budget.visit();
        try budget.remove();
        client.compareAndDelete(store.opened.bucket, key, revision, .{ .cancellation = .{ .ptr = budget, .is_cancelled_fn = Budget.canceled } }) catch |err| switch (err) {
            error.FileNotFound, error.NotFound, error.ObjectNotFound => {},
            error.PreconditionFailed => return error.QueryGcRetainedProgress,
            else => return err,
        };
        progressed.* = true;
    }
}

const ScopeProgress = struct {
    version: u16 = 1,
    after_suffix: ?[]const u8 = null,
    continuation: ?[]const u8 = null,
    retained: bool = false,
    resume_after_ms: ?u64 = null,
    sweep_started_ms: ?u64 = null,
};
const ScopePage = struct { result: Result, progress: ScopeProgress };

// The allocator is turn-owned scratch; provider continuations and suffixes
// remain valid until the caller has conditionally persisted this page.
fn scanScopes(a: A, target: *artifacts.ArtifactStore, domain: [32]u8, previous: ScopeProgress, now: u64, budget: *Budget) !ScopePage {
    if (budget.work == 0 or budget.deletes == 0) return .{ .result = .{ .more = true }, .progress = previous };
    const Visitor = struct {
        a: A,
        target: *artifacts.ArtifactStore,
        budget: *Budget,
        result: Result,
        suffix: ?[97]u8 = null,
        continuation: ?[]const u8,
        fn visit(raw: *anyopaque, scope: artifacts.UploadScope, id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.budget.visit();
            try self.budget.remove();
            self.target.deleteWithCancellation(id, self.budget.token()) catch |err| {
                try self.budget.check();
                if (err != error.FileNotFound) self.result.retained = true;
                if (err != error.ArtifactRetentionActive and err != error.ArtifactVersionDeletionPaused and err != error.FileNotFound) {
                    self.result.failed = true;
                    std.log.warn("query scoped artifact retained id={s} err={s}", .{ id, @errorName(err) });
                }
            };
            var suffix: [97]u8 = undefined;
            suffix[0..32].* = std.fmt.bytesToHex(&scope.attempt, .lower);
            suffix[32] = '/';
            @memcpy(suffix[33..], try artifacts.sha256ChecksumFromArtifactId(id));
            self.suffix = suffix;
            self.result.progressed = true;
        }
        fn retain(raw: *anyopaque, _: artifacts.UploadScope) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.result.retained = true;
        }
        fn maintenance(raw: *anyopaque, operation: artifacts.ScopedUploadVisitor.Maintenance) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.budget.check();
            if (self.budget.work == 0 or (operation == .remove and self.budget.deletes == 0)) return error.QueryGcBudgetExhausted;
            try self.budget.visit();
            if (operation == .remove) try self.budget.remove();
            self.result.progressed = true;
        }
        fn checkpoint(raw: *anyopaque, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.continuation = try self.a.dupe(u8, bytes);
            self.result.progressed = true;
        }
    };
    var visitor: Visitor = .{ .a = a, .target = target, .budget = budget, .result = .{ .retained = previous.retained }, .continuation = previous.continuation };
    try budget.check();
    target.visitScopedUploads(domain, .{
        .ptr = &visitor,
        .visit = Visitor.visit,
        .maintenance = Visitor.maintenance,
        .checkpoint = Visitor.checkpoint,
        .cleanup_staging = true,
        .reclaim_retired_inventory = true,
        .retain_scope = Visitor.retain,
        .continuation = previous.continuation,
        .after_suffix = previous.after_suffix,
        .fencing_cutoff = now -| 30_000,
        .max_entries = @min(128, @min(budget.work, budget.deletes)),
    }, budget.token()) catch |err| switch (err) {
        error.ArtifactEnumerationPaused, error.QueryGcBudgetExhausted, error.DeadlineExceeded, error.Canceled, error.Cancelled => visitor.result.more = true,
        else => return err,
    };
    return .{ .result = visitor.result, .progress = .{
        .after_suffix = if (visitor.suffix) |suffix| try a.dupe(u8, &suffix) else previous.after_suffix,
        .continuation = visitor.continuation,
        .retained = visitor.result.retained,
        .sweep_started_ms = previous.sweep_started_ms,
    } };
}

fn collectScopes(a: A, store: *Store, table: u64, domain: [32]u8, now: u64, budget: *Budget) !Result {
    const deadline = budget.deadline_ns;
    defer budget.deadline_ns = deadline;
    budget.deadline_ns -= @min(std.time.ns_per_s, (deadline -| clock.monotonicNs()) / 5);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const key = try std.fmt.allocPrint(scratch, "{s}{s}expiring-gc/{d}/scope-{s}.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", table, std.fmt.bytesToHex(&domain, .lower) });
    var client = store.opened.client;
    var previous = readObject(client, store.opened.bucket, key, 16384, budget) catch |err| switch (err) {
        error.FileNotFound, error.NotFound, error.ObjectNotFound => null,
        else => return err,
    };
    defer if (previous) |*value| value.deinit(client.allocator);
    var progress: ScopeProgress = if (previous) |value| std.json.parseFromSliceLeaky(ScopeProgress, scratch, value.body, .{ .ignore_unknown_fields = true }) catch .{} else .{};
    if (progress.version != 1 or (if (progress.after_suffix) |suffix| suffix.len != 97 else false)) progress = .{};
    // A retirement proof must cover a sweep admitted after the writer horizon.
    // Legacy progress has no admission timestamp and replays conservatively.
    if (progress.sweep_started_ms == null or (budget.retiring() and progress.sweep_started_ms.? <= budget.retire_after_ms)) progress = .{ .sweep_started_ms = now };
    if (progress.resume_after_ms) |retry_after| {
        if (retry_after > now and retry_after <= now +| budget.sweep_interval_ms and (!budget.retiring() or (progress.sweep_started_ms orelse 0) > budget.retire_after_ms)) {
            try budget.completion(previous.?.metadata.writeRevision() orelse return error.MissingObjectRevision, progress.retained, progress.sweep_started_ms orelse 0);
            return .{ .retained = progress.retained };
        }
        progress = .{ .sweep_started_ms = now };
    }
    var target = store.artifactStore();
    var page = try scanScopes(scratch, &target, domain, progress, now, budget);
    budget.deadline_ns = deadline;
    if (!page.result.more) {
        // A completed category rests while other categories drain. Persist
        // retained evidence too, so another process cannot consume ownership
        // while this category is waiting for its next ordinary sweep.
        page.progress = .{ .retained = page.result.retained, .resume_after_ms = now +| budget.sweep_interval_ms, .sweep_started_ms = progress.sweep_started_ms };
    }
    if (budget.retiring() and previous == null and !page.result.more and !page.result.progressed and !page.result.retained) {
        try budget.completion(null, false, now);
        return page.result;
    }
    if (!page.result.more or page.result.progressed) {
        const bytes = try std.json.Stringify.valueAlloc(scratch, page.progress, .{ .emit_null_optional_fields = false });
        if (bytes.len > 16384) return error.InvalidArtifactEnumerationContinuation;
        var written = client.compareAndPut(store.opened.bucket, key, bytes, if (previous) |value| value.metadata.writeRevision() orelse return error.MissingObjectRevision else null, .{ .cancellation = .{ .ptr = budget, .is_cancelled_fn = Budget.canceled } }) catch |err| switch (err) {
            error.PreconditionFailed, error.ObjectAlreadyExists => return .{ .more = true, .retained = true },
            else => return err,
        };
        defer written.deinit(client.allocator);
        if (!page.result.more) try budget.completion(written.writeRevision() orelse return error.MissingObjectRevision, page.result.retained, progress.sweep_started_ms.?);
    }
    return page.result;
}
pub const RecordProgress = struct { version: u16 = 2, cursor: ?[]const u8 = null, after_key: ?[]const u8 = null, retained: bool = false, resume_after_ms: ?u64 = null, sweep_started_ms: ?u64 = null };
const Progress = RecordProgress;
fn readObject(source: anytype, bucket: []const u8, key: []const u8, limit: usize, budget: *Budget) !@import("objectstore").GetResult {
    var client = source;
    try budget.check();
    if (budget.reads == 0) return error.QueryGcBudgetExhausted;
    // A failed/oversized response may already have consumed its allowance.
    // Reserve before I/O, then refund only bytes proven not to have been read.
    const allowance = @min(limit, budget.reads);
    budget.reads -= allowance;
    var value = client.getObject(bucket, key, .{ .skip_metadata_probe = true, .max_response_bytes = allowance, .cancellation = .{ .ptr = budget, .is_cancelled_fn = Budget.canceled } }) catch |err| {
        if (err == error.FileNotFound or err == error.NotFound or err == error.ObjectNotFound) budget.reads += allowance;
        return err;
    };
    if (value.body.len > allowance) {
        value.deinit(client.allocator);
        return error.QueryGcBudgetExhausted;
    }
    budget.reads += allowance - value.body.len;
    return value;
}
pub fn collectRecords(a: A, store: *Store, table: u64, kind: []const u8, now: u64, budget: *Budget) !Result {
    // Leave time to persist completed work if a provider call reaches the
    // processing deadline. Otherwise repeated short turns replay the prefix.
    const deadline = budget.deadline_ns;
    defer budget.deadline_ns = deadline;
    budget.deadline_ns -= @min(std.time.ns_per_s, (deadline -| clock.monotonicNs()) / 5);
    var client = store.opened.client;
    const prefix = try std.fmt.allocPrint(a, "{s}{s}{s}/{d}/", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", kind, table });
    defer a.free(prefix);
    const checkpoint = try std.fmt.allocPrint(a, "{s}{s}expiring-gc/{d}/{s}.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", table, kind });
    defer a.free(checkpoint);
    var previous = readObject(client, store.opened.bucket, checkpoint, 16384, budget) catch |err| switch (err) {
        error.FileNotFound, error.NotFound, error.ObjectNotFound => null,
        else => return err,
    };
    defer if (previous) |*v| v.deinit(client.allocator);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var progress: Progress = if (previous) |v| std.json.parseFromSliceLeaky(Progress, scratch, v.body, .{ .ignore_unknown_fields = true }) catch .{} else .{};
    if (progress.version != 2 or (progress.cursor != null and progress.after_key != null) or (if (progress.after_key) |key| !std.mem.startsWith(u8, key, prefix) or key.len > 4096 else false)) progress = .{};
    // A retirement proof must cover a sweep admitted after the writer horizon.
    // Legacy progress has no admission timestamp and replays conservatively.
    if (progress.sweep_started_ms == null or (budget.retiring() and progress.sweep_started_ms.? <= budget.retire_after_ms)) progress = .{ .sweep_started_ms = now };
    if (progress.resume_after_ms) |retry_after| {
        if (retry_after > now and retry_after <= now +| budget.sweep_interval_ms and (!budget.retiring() or (progress.sweep_started_ms orelse 0) > budget.retire_after_ms)) {
            try budget.completion(previous.?.metadata.writeRevision() orelse return error.MissingObjectRevision, progress.retained, progress.sweep_started_ms orelse 0);
            return .{ .retained = progress.retained };
        }
        progress = .{ .sweep_started_ms = now };
    }
    var page = try client.listObjects(store.opened.bucket, .{ .prefix = prefix, .max_keys = 128, .continuation_token = progress.cursor, .start_after = progress.after_key, .cancellation = .{ .ptr = budget, .is_cancelled_fn = Budget.canceled } });
    defer page.deinit(client.allocator);
    var result: Result = .{ .retained = progress.retained };
    var processed: usize = 0;
    var last = progress.after_key;
    for (page.entries) |object| {
        if (processed == 16) break;
        if (!std.mem.startsWith(u8, object.key, prefix) or object.key.len > 4096) return error.InvalidExpiringLakeObject;
        record(&client, store.opened.bucket, object.key, now, budget) catch |err| switch (err) {
            error.QueryGcBudgetExhausted, error.DeadlineExceeded, error.Canceled, error.Cancelled => break,
            error.FileNotFound, error.NotFound, error.ObjectNotFound => {},
            error.QueryGcRetainedRecord => {
                result.retained = true;
            },
            // Malformed records remain untouched, but never poison later
            // records or another category/index sweep. Persist the safe cursor.
            else => {
                result.failed = true;
                result.retained = true;
                std.log.warn("query artifact record retained key={s} err={s}", .{ object.key, @errorName(err) });
            },
        };
        processed += 1;
        last = object.key;
    }
    budget.deadline_ns = deadline;
    result.progressed = processed != 0;
    result.more = processed != page.entries.len or page.next_continuation_token != null;
    const saved: Progress = .{ .cursor = if (processed == page.entries.len) page.next_continuation_token else null, .after_key = if (processed == page.entries.len) null else last, .retained = result.retained, .resume_after_ms = if (result.more) null else now +| budget.sweep_interval_ms, .sweep_started_ms = progress.sweep_started_ms };
    if (budget.retiring() and previous == null and page.entries.len == 0) {
        try budget.completion(null, false, now);
        return result;
    }
    const bytes = try std.json.Stringify.valueAlloc(a, saved, .{ .emit_null_optional_fields = false });
    defer a.free(bytes);
    if (previous) |v| if (std.mem.eql(u8, bytes, v.body)) {
        if (!result.more) try budget.completion(v.metadata.writeRevision() orelse return error.MissingObjectRevision, result.retained, progress.sweep_started_ms.?);
        return result;
    };
    // This bounded progress write is part of the same deadline. A failed CAS
    // simply replays idempotent discovery on the next pass.
    var written = client.compareAndPut(store.opened.bucket, checkpoint, bytes, if (previous) |v| v.metadata.writeRevision() orelse return error.MissingObjectRevision else null, .{ .cancellation = .{ .ptr = budget, .is_cancelled_fn = Budget.canceled } }) catch |err| switch (err) {
        error.PreconditionFailed, error.ObjectAlreadyExists => return .{ .more = true, .retained = true },
        else => return err,
    };
    defer written.deinit(client.allocator);
    if (!result.more) try budget.completion(written.writeRevision() orelse return error.MissingObjectRevision, result.retained, progress.sweep_started_ms.?);
    return result;
}
fn record(client: anytype, bucket: []const u8, key: []const u8, now: u64, budget: *Budget) !void {
    try budget.visit();
    var value = try readObject(client.*, bucket, key, 2 * 1024 * 1024, budget);
    defer value.deinit(client.allocator);
    var parsed = try std.json.parseFromSlice(std.json.Value, client.allocator, value.body, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidExpiringLakeObject;
    const expires = parsed.value.object.get("expires_ms") orelse return error.QueryGcRetainedRecord;
    if (expires != .integer or expires.integer <= 0) return error.InvalidExpiringLakeObject;
    if (@as(u64, @intCast(expires.integer)) +| 30_000 >= now) return error.QueryGcRetainedRecord;
    if (parsed.value.object.get("lease_until_ms")) |lease| if (lease == .integer and lease.integer > 0 and @as(u64, @intCast(lease.integer)) > now) return error.QueryGcRetainedRecord;
    try budget.remove();
    client.compareAndDelete(bucket, key, value.metadata.writeRevision() orelse return error.MissingObjectRevision, .{ .cancellation = .{ .ptr = budget, .is_cancelled_fn = Budget.canceled } }) catch |err| switch (err) {
        error.PreconditionFailed => return error.QueryGcRetainedRecord,
        error.NotFound, error.ObjectNotFound, error.FileNotFound => {},
        else => return err,
    };
}

test "external lake query cleanup bounds work and skips poison across restart" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("query-gc-budgets");
    defer directory.cleanup();
    var config = try local.common_config.Config.parseFromSlice(a, "{}");
    defer config.deinit();
    var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
    defer store.deinit();
    var client = store.opened.client;
    const prefix = try std.fmt.allocPrint(a, "{s}{s}recent-search/7/", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/" });
    defer a.free(prefix);
    for (0..41) |i| {
        const key = try std.fmt.allocPrint(a, "{s}{d:0>4}.json", .{ prefix, i });
        defer a.free(key);
        var saved = try client.putObject(store.opened.bucket, key, if (i == 0) "{invalid" else "{\"expires_ms\":1}", .{});
        saved.deinit(client.allocator);
    }
    for (0..100) |_| {
        // Recreate all transient state each turn, as after a restart. The
        // durable cursor must advance beyond both poison and deleted keys.
        var budget: Budget = .{ .deletes = 1, .reads = 1024, .work = 4, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s };
        _ = try collectRecords(a, &store, 7, "recent-search", 100000, &budget);
        try std.testing.expect(budget.reads <= 1024);
        try std.testing.expect(budget.work <= 4);
    }
    var page = try client.listObjects(store.opened.bucket, .{ .prefix = prefix, .max_keys = 128 });
    defer page.deinit(client.allocator);
    try std.testing.expectEqual(@as(usize, 1), page.entries.len);
    try std.testing.expect(std.mem.endsWith(u8, page.entries[0].key, "0000.json"));
    var zero: Budget = .{ .deletes = 0, .reads = 0, .work = 0, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s };
    try std.testing.expectError(error.QueryGcBudgetExhausted, collectRecords(a, &store, 7, "recent-search", 100000, &zero));
}

test "external lake query cleanup charges failed bounded reads" {
    const Fake = struct {
        allocator: A,
        fn getObject(_: *@This(), _: []const u8, _: []const u8, options: @import("objectstore").GetOptions) anyerror!@import("objectstore").GetResult {
            try std.testing.expectEqual(@as(?usize, 17), options.max_response_bytes);
            return error.StreamTooLong;
        }
    };
    var budget: Budget = .{ .deletes = 1, .reads = 17, .work = 1, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s };
    const client: Fake = .{ .allocator = std.testing.allocator };
    try std.testing.expectError(error.StreamTooLong, readObject(client, "bucket", "large", 100, &budget));
    try std.testing.expectEqual(@as(u64, 0), budget.reads);
    try std.testing.expectError(error.QueryGcBudgetExhausted, readObject(client, "bucket", "next", 100, &budget));
}

test "external lake query cleanup resumes past protected objects after restart without live scans" {
    const Fake = struct {
        removed: [129]bool = @splat(false),
        hold: bool = true,
        calls: usize = 0,
        fn artifactStore(self: *@This()) artifacts.ArtifactStore {
            return .{ .allocator = std.testing.allocator, .ptr = self, .vtable = &.{ .deinit = undefined, .put = undefined, .get_alloc = undefined, .get_range_alloc = undefined, .stat = undefined, .delete = undefined, .delete_with_cancellation = remove, .visit_scoped_uploads = visit } };
        }
        fn remove(raw: *anyopaque, id: []const u8, _: Cancellation) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const i = try std.fmt.parseInt(usize, try artifacts.sha256ChecksumFromArtifactId(id), 16);
            if (self.hold and i < 128) return error.ArtifactRetentionActive;
            self.removed[i] = true;
        }
        fn visit(raw: *anyopaque, domain: [32]u8, visitor: artifacts.ScopedUploadVisitor, _: Cancellation) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expectEqual(@as(u64, 0), visitor.fencing_floor);
            try std.testing.expect(visitor.checkpoint != null);
            var scope: artifacts.UploadScope = .{ .domain = domain, .attempt = @splat(0) };
            std.mem.writeInt(u64, scope.attempt[0..8], 1, .big);
            scope.attempt[15] = 1;
            var count: usize = 0;
            for (0..129) |i| {
                if (self.removed[i]) continue;
                var checksum: [64]u8 = undefined;
                _ = try std.fmt.bufPrint(&checksum, "{x:0>64}", .{i});
                var suffix: [97]u8 = undefined;
                suffix[0..32].* = std.fmt.bytesToHex(&scope.attempt, .lower);
                suffix[32] = '/';
                @memcpy(suffix[33..], &checksum);
                if (visitor.after_suffix) |after| if (std.mem.order(u8, &suffix, after) != .gt) continue;
                if (count == visitor.max_entries.?) return error.ArtifactEnumerationPaused;
                const id = try scope.artifactId(&checksum);
                try visitor.visit(visitor.ptr, scope, &id);
                count += 1;
            }
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fake: Fake = .{};
    var target = fake.artifactStore();
    var progress: ScopeProgress = .{};
    for (0..129) |_| {
        var budget: Budget = .{ .deletes = 1, .reads = 1024, .work = 1, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s };
        const page = try scanScopes(a, &target, @splat(1), progress, 100000, &budget);
        // Serialize/reload every turn, including retained evidence from earlier
        // pages. Releasing a later eligible object cannot forget legal holds.
        progress = try std.json.parseFromSliceLeaky(ScopeProgress, a, try std.json.Stringify.valueAlloc(a, page.progress, .{}), .{});
        try std.testing.expect(page.result.retained);
    }
    try std.testing.expect(fake.removed[128]);
    try std.testing.expectEqual(@as(usize, 129), fake.calls);
    fake.hold = false;
    var budget: Budget = .{ .deletes = 1024, .reads = 1024, .work = 1024, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s };
    const finished = try scanScopes(a, &target, @splat(1), .{}, 100000, &budget);
    try std.testing.expect(!finished.result.more and !finished.result.retained);
    for (fake.removed) |removed| try std.testing.expect(removed);
}

test "external lake query cleanup persists journals and reclaims interrupted fenced attempts" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("query-gc-journal-restart");
    defer directory.cleanup();
    var config = try local.common_config.Config.parseFromSlice(a, "{}");
    defer config.deinit();
    const domain: [32]u8 = @splat(7);
    var expired_paths: [2][]const u8 = undefined;
    var inventory_paths: [2][]const u8 = undefined;
    var active_paths: [2][]const u8 = undefined;
    {
        var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
        defer store.deinit();
        var target = store.artifactStore();
        for ([_]u64{ 1, 2, 200000 }, 0..) |expiry, group| {
            const scope = try artifacts.UploadScope.forPublication(domain, expiry, std.testing.io);
            const inventory = &store.implementation.local_inventory.?;
            const payload_dir = try std.fs.path.join(scratch, &.{ inventory.root_dir, "graph", &std.fmt.bytesToHex(&domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
            if (group < 2) {
                expired_paths[group] = payload_dir;
                inventory_paths[group] = try std.fs.path.join(scratch, &.{ inventory.inventory_root, &std.fmt.bytesToHex(&domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
            }
            for (0..if (group == 0) @as(usize, 20) else 1) |i| {
                var text: [16]u8 = undefined;
                var value = try target.putScoped(scope, try std.fmt.bufPrint(&text, "{d}", .{i}), .none);
                defer value.deinit(a);
                if (i == 0) {
                    const suffix = try artifacts.storageSuffixAlloc(scratch, value.artifact_id);
                    const payload = try std.fs.path.join(scratch, &.{ inventory.root_dir, suffix });
                    const pending = try std.fmt.allocPrint(scratch, "{s}.pending-v2", .{payload});
                    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = pending, .data = "interrupted upload" });
                    if (group == 1) try target.delete(value.artifact_id);
                    if (group == 2) active_paths = .{ payload, pending };
                }
            }
        }
    }
    var journal_reads: u64 = 0;
    var complete = false;
    for (0..100) |_| {
        var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
        defer store.deinit();
        var budget: Budget = .{ .deletes = 1, .reads = 4096, .work = 4, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s };
        const result = try collectScopes(a, &store, 7, domain, 100000, &budget);
        journal_reads += store.implementation.local_inventory.?.journal_records_read.load(.monotonic);
        if (!result.more) {
            try std.testing.expect(!result.retained and !result.failed);
            complete = true;
            break;
        }
    }
    try std.testing.expect(complete);
    try std.testing.expect(journal_reads >= 21 and journal_reads <= 63);
    for (expired_paths) |path| try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, path, .{}));
    for (inventory_paths) |path| try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, path, .{}));
    for (active_paths) |path| try std.Io.Dir.cwd().access(std.testing.io, path, .{});
    // Both a fast continuation and a fresh ordinary sweep after restart must
    // avoid reading historical journals, while future uploads remain intact.
    for ([_]u64{ 100001, 130001 }) |now| {
        var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
        defer store.deinit();
        var budget: Budget = .{ .deletes = 128, .reads = 4096, .work = 128, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s };
        const result = try collectScopes(a, &store, 7, domain, now, &budget);
        try std.testing.expect(!result.more and !result.retained and !result.failed);
        try std.testing.expectEqual(@as(usize, 0), store.implementation.local_inventory.?.journal_records_read.load(.monotonic));
        for (active_paths) |path| try std.Io.Dir.cwd().access(std.testing.io, path, .{});
    }
}

test "external lake query cleanup completed categories rest durably while neighbors drain" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("query-gc-category-rounds");
    defer directory.cleanup();
    var config = try local.common_config.Config.parseFromSlice(a, "{}");
    defer config.deinit();
    {
        var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
        defer store.deinit();
        var client = store.opened.client;
        for ([_][]const u8{ "recent-search", "row-enrichment" }, [_]usize{ 17, 33 }) |kind, count| {
            for (0..count) |i| {
                const key = try std.fmt.allocPrint(a, "{s}{s}{s}/7/{d:0>4}.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", kind, i });
                defer a.free(key);
                var saved = try client.putObject(store.opened.bucket, key, "{\"expires_ms\":200000}", .{});
                saved.deinit(client.allocator);
            }
        }
    }
    var inspected: usize = 0;
    for (0..3) |turn| {
        var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
        defer store.deinit();
        var budget: Budget = .{ .deletes = 4096, .reads = 1024 * 1024, .work = 4096, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s, .sweep_interval_ms = 1234 };
        const result = collect(a, &store, 7, 0, 100000, &budget);
        inspected += 4096 - budget.work;
        try std.testing.expectEqual(turn != 2, result.more);
        try std.testing.expect(result.progressed and result.retained and !result.failed);
    }
    try std.testing.expectEqual(@as(usize, 50), inspected);
    // Rest survives process reconstruction and honors the configured interval.
    for ([_]u64{ 100001, 101233 }) |now| {
        var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
        defer store.deinit();
        var budget: Budget = .{ .deletes = 4096, .reads = 1024 * 1024, .work = 4096, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s, .sweep_interval_ms = 1234 };
        const result = collect(a, &store, 7, 0, now, &budget);
        try std.testing.expect(!result.more and !result.progressed and result.retained and !result.failed);
        try std.testing.expectEqual(@as(usize, 4096), budget.work);
    }
    // A later sweep must retry held records and reclaim them once expired.
    var deleted: usize = 0;
    for (0..3) |turn| {
        var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
        defer store.deinit();
        var budget: Budget = .{ .deletes = 4096, .reads = 1024 * 1024, .work = 4096, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s, .sweep_interval_ms = 1234 };
        const result = collect(a, &store, 7, 0, 231000, &budget);
        deleted += 4096 - budget.deletes;
        try std.testing.expectEqual(turn != 2, result.more);
        try std.testing.expect(!result.retained and !result.failed);
    }
    try std.testing.expectEqual(@as(usize, 50), deleted);
    var retired = false;
    for (0..6) |_| {
        var proofs = CompletionProofs.init(a);
        defer proofs.deinit();
        var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
        defer store.deinit();
        var budget: Budget = .{ .deletes = 1, .reads = 512, .work = 1, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s, .sweep_interval_ms = 1234, .retire_after_ms = 200000, .proofs = &proofs };
        const result = collect(a, &store, 7, 0, 231001, &budget);
        try std.testing.expect(!result.more and !result.retained and !result.failed);
        var retirement_progress = false;
        retireCompleted(a, &store, 7, &proofs, &retirement_progress, &budget) catch |err| {
            if (err != error.QueryGcBudgetExhausted) return err;
            continue;
        };
        retired = true;
        break;
    }
    try std.testing.expect(retired);
    var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
    defer store.deinit();
    const prefix = try std.fmt.allocPrint(a, "{s}{s}expiring-gc/7/", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/" });
    defer a.free(prefix);
    var remaining = try store.opened.client.listObjects(store.opened.bucket, .{ .prefix = prefix, .max_keys = 16 });
    defer remaining.deinit(store.opened.client.allocator);
    try std.testing.expectEqual(@as(usize, 0), remaining.entries.len);
}

test "external lake query cleanup release requires a sweep after the writer horizon" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("query-gc-horizon-sweep");
    defer directory.cleanup();
    var config = try local.common_config.Config.parseFromSlice(a, "{}");
    defer config.deinit();
    var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
    defer store.deinit();
    const domain: [32]u8 = @splat(31);
    const scope = try artifacts.UploadScope.forPublication(domain, 110000, std.testing.io);
    var target = store.artifactStore();
    var value = try target.putScoped(scope, "future at first sweep", .none);
    defer value.deinit(a);
    var first_budget: Budget = .{ .deletes = 128, .reads = 16384, .work = 128, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s, .sweep_interval_ms = 90000 };
    const first = try collectScopes(a, &store, 7, domain, 100000, &first_budget);
    try std.testing.expect(!first.more and !first.retained);
    var budget: Budget = .{ .deletes = 128, .reads = 16384, .work = 128, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s, .sweep_interval_ms = 90000, .retire_after_ms = 120000 };
    // The old completion is still inside its interval, but predates the
    // writer horizon. It cannot prove that these now-expired uploads are gone.
    const result = try collectScopes(a, &store, 7, domain, 150001, &budget);
    try std.testing.expect(result.progressed and !result.more and !result.retained and !result.failed);
    try std.testing.expectError(error.FileNotFound, target.getAlloc(value.artifact_id));
}

test "external lake query cleanup replacement journals restart both short and long continuations" {
    const a = std.testing.allocator;
    for ([_]usize{ 0, 1, 20 }) |count| {
        var directory = try local.common_test_directory.TestDirectory.init("query-recreated-journal");
        defer directory.cleanup();
        var config = try local.common_config.Config.parseFromSlice(a, "{}");
        defer config.deinit();
        var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
        defer store.deinit();
        const domain = try scopeDomain(a, &store, 7, 0);
        const scope = try artifacts.UploadScope.forPublication(domain, 1, std.testing.io);
        var target = store.artifactStore();
        var last: ?[]u8 = null;
        defer if (last) |id| a.free(id);
        for (0..20) |i| {
            var text: [32]u8 = undefined;
            var value = try target.putScoped(scope, try std.fmt.bufPrint(&text, "old-{d}", .{i}), .none);
            defer value.deinit(a);
            if (i == 19) last = try a.dupe(u8, value.artifact_id);
        }
        for (0..19) |_| {
            var budget: Budget = .{ .deletes = 1, .reads = 16384, .work = 1, .deadline_ns = clock.monotonicNs() + 5 * std.time.ns_per_s };
            _ = try collectScopes(a, &store, 7, domain, 100000, &budget);
        }
        try target.delete(last.?);
        const inventory = &store.implementation.local_inventory.?;
        const metadata_path = try std.fs.path.join(a, &.{ inventory.inventory_root, &std.fmt.bytesToHex(&domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
        defer a.free(metadata_path);
        // Interrupted retirement, followed by orphan re-registration. Old
        // offsets may exceed the replacement or fit inside unrelated records.
        try std.Io.Dir.cwd().deleteTree(std.testing.io, metadata_path);
        try inventory.durable_dirs.invalidate(a, std.testing.io, metadata_path);
        var ids: std.ArrayList([]u8) = .empty;
        defer {
            for (ids.items) |id| a.free(id);
            ids.deinit(a);
        }
        var pending: ?[]u8 = null;
        defer if (pending) |path| a.free(path);
        for (0..@max(1, count)) |i| {
            var text: [32]u8 = undefined;
            var value = try target.putScoped(scope, try std.fmt.bufPrint(&text, "new-{d}", .{i}), .none);
            defer value.deinit(a);
            try ids.append(a, try a.dupe(u8, value.artifact_id));
            if (count == 0) {
                const suffix = try artifacts.storageSuffixAlloc(a, value.artifact_id);
                defer a.free(suffix);
                pending = try std.fmt.allocPrint(a, "{s}/{s}.pending-v2", .{ inventory.root_dir, suffix });
                try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = pending.?, .data = "interrupted new upload" });
                try target.delete(value.artifact_id);
                try std.Io.Dir.cwd().deleteTree(std.testing.io, metadata_path);
                try inventory.durable_dirs.invalidate(a, std.testing.io, metadata_path);
            }
        }
        var budget: Budget = .{ .deletes = 128, .reads = 16384, .work = 128, .deadline_ns = clock.monotonicNs() + 5 * std.time.ns_per_s };
        const result = try collectScopes(a, &store, 7, domain, 100000, &budget);
        try std.testing.expect(!result.more and !result.retained and !result.failed);
        for (ids.items) |id| try std.testing.expectError(error.FileNotFound, target.getAlloc(id));
        if (pending) |path| try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, path, .{}));
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, metadata_path, .{}));
    }
}

test "external lake query cleanup legacy temporary inventories resume linearly after restart" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("query-legacy-temporaries");
    defer directory.cleanup();
    var config = try local.common_config.Config.parseFromSlice(a, "{}");
    defer config.deinit();
    const domain: [32]u8 = @splat(43);
    {
        var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
        defer store.deinit();
        var target = store.artifactStore();
        const scope = try artifacts.UploadScope.forPublication(domain, 1, std.testing.io);
        var value = try target.putScoped(scope, "legacy staging", .none);
        defer value.deinit(a);
        const suffix = try artifacts.storageSuffixAlloc(a, value.artifact_id);
        defer a.free(suffix);
        for (0..129) |i| {
            const pending = try std.fmt.allocPrint(a, "{s}/{s}.tmp-{x:0>32}", .{ store.implementation.local_inventory.?.root_dir, suffix, i });
            defer a.free(pending);
            try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = pending, .data = "interrupted upload" });
        }
        try target.delete(value.artifact_id);
    }
    var reads: usize = 0;
    var migration_reads: usize = 0;
    var complete = false;
    for (0..400) |_| {
        var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
        defer store.deinit();
        var budget: Budget = .{ .deletes = 1, .reads = 16384, .work = 1, .deadline_ns = clock.monotonicNs() + 5 * std.time.ns_per_s, .retire_after_ms = 90000 };
        const result = try collectScopes(a, &store, 7, domain, 100000, &budget);
        const temporary_reads = store.implementation.local_inventory.?.temporary_records_read.load(.monotonic);
        reads += temporary_reads;
        migration_reads += store.implementation.local_inventory.?.migration_entries_read.load(.monotonic);
        if (store.implementation.local_inventory.?.migration_entries_read.load(.monotonic) != 0)
            try std.testing.expect(result.progressed);
        if (temporary_reads != 0) {
            try std.testing.expectEqual(@as(usize, 1), temporary_reads);
            try std.testing.expectEqual(@as(usize, 0), budget.deletes);
            try std.testing.expectEqual(@as(usize, 0), budget.work);
        }
        if (!result.more) {
            try std.testing.expect(!result.retained and !result.failed);
            complete = true;
            break;
        }
    }
    try std.testing.expect(complete);
    try std.testing.expectEqual(@as(usize, 129), reads);
    try std.testing.expectEqual(@as(usize, 129), migration_reads);
    var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
    defer store.deinit();
    var budget: Budget = .{ .deletes = 128, .reads = 16384, .work = 128, .deadline_ns = clock.monotonicNs() + 5 * std.time.ns_per_s };
    _ = try collectScopes(a, &store, 7, domain, 131000, &budget);
    try std.testing.expectEqual(@as(usize, 0), store.implementation.local_inventory.?.temporary_records_read.load(.monotonic));
}

test "external lake query cleanup filesystem staging shares the turn deletion budget across categories" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("query-shared-staging-budget");
    defer directory.cleanup();
    var config = try local.common_config.Config.parseFromSlice(a, "{}");
    defer config.deinit();
    var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
    defer store.deinit();
    var target = store.artifactStore();
    var paths: [3][]const u8 = undefined;
    for (0..3) |category| {
        const scope = try artifacts.UploadScope.forPublication(try scopeDomain(a, &store, 7, category), 1, std.testing.io);
        var value = try target.putScoped(scope, "interrupted staging", .none);
        defer value.deinit(a);
        const suffix = try artifacts.storageSuffixAlloc(scratch, value.artifact_id);
        paths[category] = try std.fmt.allocPrint(scratch, "{s}/{s}.pending-v2", .{ store.implementation.local_inventory.?.root_dir, suffix });
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = paths[category], .data = "unfinished" });
        try target.delete(value.artifact_id);
    }
    var budget: Budget = .{ .deletes = 1, .reads = 16384, .work = 128, .deadline_ns = clock.monotonicNs() + 5 * std.time.ns_per_s };
    const result = collect(a, &store, 7, 0, 100000, &budget);
    try std.testing.expect(result.more and !result.failed);
    try std.testing.expectEqual(@as(usize, 0), budget.deletes);
    var removed: usize = 0;
    for (paths) |path| std.Io.Dir.cwd().access(std.testing.io, path, .{}) catch |err| {
        if (err != error.FileNotFound) return err;
        removed += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), removed);
    // Index GC receives this same remainder and cannot admit a second delete.
    try std.testing.expectError(error.QueryGcBudgetExhausted, budget.remove());
}

test "external lake query cleanup retirement reuses read proofs and fences concurrent changes" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("query-retirement-proofs");
    defer directory.cleanup();
    var config = try local.common_config.Config.parseFromSlice(a, "{}");
    defer config.deinit();
    var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
    defer store.deinit();
    const now = clock.realtimeNs() / std.time.ns_per_ms;
    var seed: Budget = .{ .deletes = 128, .reads = 16384, .work = 128, .deadline_ns = clock.monotonicNs() + 5 * std.time.ns_per_s };
    _ = collect(a, &store, 7, 0, now, &seed);
    var proofs = CompletionProofs.init(a);
    defer proofs.deinit();
    var budget: Budget = .{ .deletes = 128, .reads = 512, .work = 128, .deadline_ns = clock.monotonicNs() + 5 * std.time.ns_per_s, .retire_after_ms = now - 1, .proofs = &proofs };
    const result = collect(a, &store, 7, 0, now + 1, &budget);
    try std.testing.expect(!result.more and !result.retained and !result.failed);
    const key = try std.fmt.allocPrint(a, "{s}{s}expiring-gc/7/scope-{s}.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", std.fmt.bytesToHex(&(try scopeDomain(a, &store, 7, 0)), .lower) });
    defer a.free(key);
    var client = store.opened.client;
    const held_bytes = try std.json.Stringify.valueAlloc(a, ScopeProgress{ .retained = true, .resume_after_ms = now + 30001, .sweep_started_ms = now + 1 }, .{ .emit_null_optional_fields = false });
    defer a.free(held_bytes);
    var changed = try client.putObject(store.opened.bucket, key, held_bytes, .{});
    changed.deinit(client.allocator);
    var retirement_progress = false;
    try std.testing.expectError(error.QueryGcRetainedProgress, retireCompleted(a, &store, 7, &proofs, &retirement_progress, &budget));
    // A complete sweep after the retained completion's rest interval clears
    // its evidence. Retirement then uses the newly collected CAS revisions.
    var refresh: Budget = .{ .deletes = 128, .reads = 512, .work = 128, .deadline_ns = clock.monotonicNs() + 5 * std.time.ns_per_s, .retire_after_ms = now - 1, .proofs = &proofs };
    const held = collect(a, &store, 7, 0, now + 1, &refresh);
    try std.testing.expect(held.retained and !held.more and !held.failed);
    var next: Budget = .{ .deletes = 128, .reads = 512, .work = 128, .deadline_ns = clock.monotonicNs() + 5 * std.time.ns_per_s, .retire_after_ms = now - 1, .proofs = &proofs };
    const final = collect(a, &store, 7, 0, now + 30002, &next);
    try std.testing.expect(!final.more and !final.retained and !final.failed);
    const before = next.reads;
    try retireCompleted(a, &store, 7, &proofs, &retirement_progress, &next);
    try std.testing.expectEqual(before, next.reads);
    try std.testing.expectEqual(@as(usize, 123), next.deletes);
}

test "external lake query cleanup retirement restarts sweeps admitted before the writer horizon" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("query-retirement-admission");
    defer directory.cleanup();
    var config = try local.common_config.Config.parseFromSlice(a, "{}");
    defer config.deinit();
    var store = try Store.openNative(a, &config, null, false, .standalone, directory.path());
    defer store.deinit();
    const domain = try scopeDomain(a, &store, 7, 0);
    const earlier = try artifacts.UploadScope.forPublication(domain, 1, std.testing.io);
    const later = try artifacts.UploadScope.forPublication(domain, 2, std.testing.io);
    var target = store.artifactStore();
    var first = try target.putScoped(earlier, "early scope", .none);
    defer first.deinit(a);
    for (0..20) |i| {
        var text: [32]u8 = undefined;
        var value = try target.putScoped(later, try std.fmt.bufPrint(&text, "later-{d}", .{i}), .none);
        value.deinit(a);
    }
    // Advance beyond the first scope before the writer horizon passes,
    // including its separately budgeted metadata retirement.
    const earlier_path = try std.fs.path.join(a, &.{ store.implementation.local_inventory.?.root_dir, "graph", &std.fmt.bytesToHex(&domain, .lower), &std.fmt.bytesToHex(&earlier.attempt, .lower) });
    defer a.free(earlier_path);
    var earlier_retired = false;
    for (0..16) |_| {
        var budget: Budget = .{ .deletes = 1, .reads = 16384, .work = 1, .deadline_ns = clock.monotonicNs() + 5 * std.time.ns_per_s };
        const result = try collectScopes(a, &store, 7, domain, 100000, &budget);
        try std.testing.expect(result.more);
        std.Io.Dir.cwd().access(std.testing.io, earlier_path, .{}) catch |err| {
            if (err != error.FileNotFound) return err;
            earlier_retired = true;
            break;
        };
    }
    try std.testing.expect(earlier_retired);
    var late = try target.putScoped(earlier, "late orphan behind the cursor", .none);
    defer late.deinit(a);
    var budget: Budget = .{ .deletes = 128, .reads = 16384, .work = 128, .deadline_ns = clock.monotonicNs() + 5 * std.time.ns_per_s, .retire_after_ms = 120000 };
    const result = try collectScopes(a, &store, 7, domain, 150000, &budget);
    try std.testing.expect(!result.more and !result.retained and !result.failed);
    const remaining: ?[]u8 = target.getAlloc(late.artifact_id) catch null;
    defer if (remaining) |bytes| a.free(bytes);
    try std.testing.expect(remaining == null);
}
