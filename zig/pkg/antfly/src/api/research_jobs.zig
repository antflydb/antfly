// Copyright 2026 Antfly, Inc.
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

//! Durable research jobs. A job stores the research request and advances one
//! bounded phase per pass; every pass checkpoints `research_state` into the
//! stored request, so a restart resumes from the last completed phase rather
//! than replaying model calls.
//!
//! Jobs are owned by the authenticated principal. Lookups by any other
//! principal report not-found so job IDs do not disclose existence. Stored
//! requests must not carry inline generator API keys (see validateRequest).
//!
//! Concurrency: one advance at a time per job, fenced by an attempt counter
//! and a lease. A pass whose lease expired (crashed or hung advance) is
//! superseded; its late checkpoint is discarded.

const std = @import("std");
const docstore_mod = @import("../storage/docstore.zig");
const backend_erased = @import("../storage/backend_erased.zig");
const platform_time = @import("antfly_platform").time;
const research_agent = @import("research_agent.zig");
const agent_tools = @import("agent_tools.zig");
const platform_sync = @import("antfly_platform").sync;

pub const StoreConfig = struct {
    path: ?[]const u8 = null,
    retention_ms: ?u64 = null,
    /// Jobs per owner that may be queued or running at once.
    max_active_per_owner: usize = 16,
};

pub const OpenedStore = struct {
    alloc: std.mem.Allocator,
    path_z: [:0]u8,
    docstore: *docstore_mod.DocStore,

    pub fn open(alloc: std.mem.Allocator, path: []const u8) !OpenedStore {
        const path_z = try alloc.dupeZ(u8, path);
        errdefer alloc.free(path_z);
        const docstore = try alloc.create(docstore_mod.DocStore);
        errdefer alloc.destroy(docstore);
        docstore.* = try docstore_mod.DocStore.open(alloc, path_z, .{});
        return .{ .alloc = alloc, .path_z = path_z, .docstore = docstore };
    }

    pub fn deinit(self: *OpenedStore) void {
        self.docstore.close();
        self.alloc.destroy(self.docstore);
        self.alloc.free(self.path_z);
        self.* = undefined;
    }
};

pub const JobState = enum { queued, running, succeeded, failed, cancelled };

/// Persisted record. `request` is the research request JSON whose
/// research_state is replaced after every pass; `result` is the latest
/// ResearchAgentResult JSON.
pub const Record = struct {
    job_id: []const u8,
    owner: []const u8,
    state: JobState,
    phase: []const u8 = "plan",
    query: []const u8 = "",
    request: []const u8,
    result: ?[]const u8 = null,
    attempt: u64 = 0,
    advances: u64 = 0,
    cancel_requested: bool = false,
    last_error: ?[]const u8 = null,
    lease_until_ms: u64 = 0,
    created_at_ms: u64,
    updated_at_ms: u64,
    expires_at_ms: u64,

    pub fn terminal(self: Record) bool {
        return switch (self.state) {
            .succeeded, .failed, .cancelled => true,
            .queued, .running => false,
        };
    }
};

pub const Begin = union(enum) {
    /// The caller owns this attempt and must call finish with the record.
    started: Record,
    /// Terminal job; nothing to do.
    terminal: Record,
    /// Another advance holds an unexpired lease.
    busy: Record,
};

pub const Outcome = struct {
    state: JobState,
    phase: []const u8,
    request: []const u8,
    result: []const u8,
    last_error: ?[]const u8 = null,
};

const key_prefix = "__api_research_jobs__:";

pub const Store = struct {
    alloc: std.mem.Allocator,
    cfg: StoreConfig,
    opened_store: ?*OpenedStore = null,
    /// Engine-owned store (standalone LSM root). Borrowed; outlives `Store`.
    runtime: ?*backend_erased.Store = null,
    mutex: std.atomic.Mutex = .unlocked,
    /// job_id -> encoded Record, owned by `alloc`.
    jobs: std.StringHashMapUnmanaged([]u8) = .empty,
    /// job_id -> owner and activity, kept beside `jobs` so quota checks do
    /// not re-parse every record. Keys are borrowed from `jobs`.
    meta: std.StringHashMapUnmanaged(Meta) = .empty,

    const Meta = struct { owner: []u8, active: bool };

    pub fn init(alloc: std.mem.Allocator, cfg: StoreConfig) Store {
        return .{ .alloc = alloc, .cfg = cfg };
    }

    pub fn deinit(self: *Store) void {
        var meta_it = self.meta.valueIterator();
        while (meta_it.next()) |meta| self.alloc.free(meta.owner);
        self.meta.deinit(self.alloc);
        var it = self.jobs.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.*);
        }
        self.jobs.deinit(self.alloc);
        if (self.opened_store) |store| {
            store.deinit();
            self.alloc.destroy(store);
        }
        self.* = undefined;
    }

    pub fn retentionMillis(self: *const Store) u64 {
        return self.cfg.retention_ms orelse 7 * 86_400_000;
    }

    /// Attach durable storage and recover persisted jobs. Interrupted
    /// attempts return to queued (or cancelled when cancellation was asked).
    pub fn attachOpenedStore(self: *Store, opened: *OpenedStore) !void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        self.opened_store = opened;
        errdefer self.opened_store = null;
        const results = try opened.docstore.scanPrefix(self.alloc, key_prefix);
        defer docstore_mod.DocStore.freeResults(self.alloc, results);
        for (results) |kv| {
            var parsed = std.json.parseFromSlice(Record, self.alloc, kv.value, .{ .ignore_unknown_fields = true }) catch continue;
            defer parsed.deinit();
            var record = parsed.value;
            if (record.state == .running) {
                record.state = if (record.cancel_requested) .cancelled else .queued;
                record.last_error = if (record.cancel_requested) "cancel_requested" else "recovered_interrupted_attempt";
                record.lease_until_ms = 0;
                record.updated_at_ms = nowMillis();
            }
            try self.putLocked(record);
        }
    }

    /// Attach an engine-owned store and recover persisted jobs, as
    /// attachOpenedStore does for a docstore.
    pub fn attachRuntime(self: *Store, runtime: *backend_erased.Store) !void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        self.runtime = runtime;
        errdefer self.runtime = null;
        var rows = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (rows.items) |row| self.alloc.free(row);
            rows.deinit(self.alloc);
        }
        {
            var txn = try runtime.beginCurrentScan();
            defer txn.abort();
            var cursor = try txn.openCursor();
            defer cursor.close();
            var entry = try cursor.seekAtOrAfter(key_prefix);
            while (entry) |row| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, row.key, key_prefix)) break;
                try rows.append(self.alloc, try self.alloc.dupe(u8, row.value));
            }
        }
        for (rows.items) |value| {
            var parsed = std.json.parseFromSlice(Record, self.alloc, value, .{ .ignore_unknown_fields = true }) catch continue;
            defer parsed.deinit();
            var record = parsed.value;
            if (record.state == .running) {
                record.state = if (record.cancel_requested) .cancelled else .queued;
                record.last_error = if (record.cancel_requested) "cancel_requested" else "recovered_interrupted_attempt";
                record.lease_until_ms = 0;
                record.updated_at_ms = nowMillis();
            }
            try self.putLocked(record);
        }
    }

    /// Create a queued job. Returns error.TooManyActiveJobs when the owner
    /// already has the configured number of non-terminal jobs.
    pub fn create(self: *Store, alloc: std.mem.Allocator, job_id: []const u8, owner: []const u8, query: []const u8, request: []const u8) ![]u8 {
        const now = nowMillis();
        lock(&self.mutex);
        defer self.mutex.unlock();
        var active: usize = 0;
        var it = self.meta.valueIterator();
        while (it.next()) |meta| {
            if (meta.active and std.mem.eql(u8, meta.owner, owner)) active += 1;
        }
        if (active >= self.cfg.max_active_per_owner) return error.TooManyActiveJobs;
        if (self.jobs.contains(job_id)) return error.JobExists;
        const record = Record{
            .job_id = job_id,
            .owner = owner,
            .state = .queued,
            .query = agent_tools.truncateUtf8(query, 4096),
            .request = request,
            .created_at_ms = now,
            .updated_at_ms = now,
            .expires_at_ms = now + self.retentionMillis(),
        };
        try self.putLocked(record);
        return std.json.Stringify.valueAlloc(alloc, record, .{});
    }

    /// Load a record visible to `owner`, parsed into `arena`.
    pub fn load(self: *Store, arena: std.mem.Allocator, job_id: []const u8, owner: []const u8) !?Record {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const encoded = self.jobs.get(job_id) orelse return null;
        const record = try std.json.parseFromSliceLeaky(Record, arena, try arena.dupe(u8, encoded), .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        if (!std.mem.eql(u8, record.owner, owner)) return null;
        return record;
    }

    /// Claim the next attempt. `lease_ms` bounds how long the attempt may run
    /// before another advance may supersede it.
    pub fn begin(self: *Store, arena: std.mem.Allocator, job_id: []const u8, owner: []const u8, lease_ms: u64) !?Begin {
        const now = nowMillis();
        lock(&self.mutex);
        defer self.mutex.unlock();
        const encoded = self.jobs.get(job_id) orelse return null;
        var record = try std.json.parseFromSliceLeaky(Record, arena, try arena.dupe(u8, encoded), .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        if (!std.mem.eql(u8, record.owner, owner)) return null;
        if (record.terminal()) return .{ .terminal = record };
        if (record.state == .running and now < record.lease_until_ms) return .{ .busy = record };
        if (record.cancel_requested) {
            record.state = .cancelled;
            record.last_error = "cancel_requested";
            record.updated_at_ms = now;
            try self.putLocked(record);
            return .{ .terminal = record };
        }
        record.state = .running;
        record.attempt += 1;
        record.lease_until_ms = now +| lease_ms;
        record.updated_at_ms = now;
        record.expires_at_ms = now + self.retentionMillis();
        try self.putLocked(record);
        return .{ .started = record };
    }

    /// Record the outcome of an attempt. A superseded attempt (another advance
    /// claimed the job after this lease expired) is discarded. Cancellation
    /// requested during the pass wins over a non-terminal outcome.
    pub fn finish(self: *Store, arena: std.mem.Allocator, claimed: Record, outcome: Outcome) !Record {
        const now = nowMillis();
        lock(&self.mutex);
        defer self.mutex.unlock();
        const encoded = self.jobs.get(claimed.job_id) orelse return error.NotFound;
        var record = try std.json.parseFromSliceLeaky(Record, arena, try arena.dupe(u8, encoded), .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        if (record.attempt != claimed.attempt or record.state != .running) return record;
        record.state = if (record.cancel_requested and outcome.state != .succeeded and outcome.state != .failed) .cancelled else outcome.state;
        record.phase = outcome.phase;
        record.request = outcome.request;
        record.result = outcome.result;
        record.last_error = if (record.state == .cancelled) "cancel_requested" else outcome.last_error;
        record.advances += 1;
        record.lease_until_ms = 0;
        record.updated_at_ms = now;
        record.expires_at_ms = now + self.retentionMillis();
        try self.putLocked(record);
        return record;
    }

    /// Persist a completed phase while the attempt keeps its lease, so a crash
    /// in a later phase of the same advance resumes after this one. Returns
    /// null when the attempt was superseded or cancellation was requested:
    /// the caller must stop and call `finish`.
    pub fn checkpoint(self: *Store, arena: std.mem.Allocator, claimed: Record, outcome: Outcome, lease_ms: u64) !?Record {
        const now = nowMillis();
        lock(&self.mutex);
        defer self.mutex.unlock();
        const encoded = self.jobs.get(claimed.job_id) orelse return error.NotFound;
        var record = try std.json.parseFromSliceLeaky(Record, arena, try arena.dupe(u8, encoded), .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        if (record.attempt != claimed.attempt or record.state != .running or record.cancel_requested) return null;
        record.phase = outcome.phase;
        record.request = outcome.request;
        record.result = outcome.result;
        record.advances += 1;
        record.lease_until_ms = now +| lease_ms;
        record.updated_at_ms = now;
        record.expires_at_ms = now + self.retentionMillis();
        try self.putLocked(record);
        return record;
    }

    pub fn requestCancel(self: *Store, arena: std.mem.Allocator, job_id: []const u8, owner: []const u8) !?Record {
        const now = nowMillis();
        lock(&self.mutex);
        defer self.mutex.unlock();
        const encoded = self.jobs.get(job_id) orelse return null;
        var record = try std.json.parseFromSliceLeaky(Record, arena, try arena.dupe(u8, encoded), .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        if (!std.mem.eql(u8, record.owner, owner)) return null;
        if (record.terminal()) return record;
        record.cancel_requested = true;
        // A running pass observes the flag when it checkpoints.
        if (record.state != .running or now >= record.lease_until_ms) {
            record.state = .cancelled;
            record.last_error = "cancel_requested";
        }
        record.updated_at_ms = now;
        try self.putLocked(record);
        return record;
    }

    pub fn cleanupExpiredJobs(self: *Store) void {
        const now = nowMillis();
        lock(&self.mutex);
        defer self.mutex.unlock();
        var expired = std.ArrayListUnmanaged([]const u8).empty;
        defer expired.deinit(self.alloc);
        var it = self.jobs.iterator();
        while (it.next()) |entry| {
            // A record that no longer parses can never be served, counted or
            // expired normally; remove it rather than keep it forever.
            var parsed = std.json.parseFromSlice(Record, self.alloc, entry.value_ptr.*, .{ .ignore_unknown_fields = true }) catch {
                expired.append(self.alloc, entry.key_ptr.*) catch {};
                continue;
            };
            defer parsed.deinit();
            if (parsed.value.expires_at_ms > now) continue;
            if (parsed.value.state == .running and now < parsed.value.lease_until_ms) continue;
            expired.append(self.alloc, entry.key_ptr.*) catch continue;
        }
        for (expired.items) |job_id| {
            const key = std.fmt.allocPrint(self.alloc, "{s}{s}", .{ key_prefix, job_id }) catch continue;
            defer self.alloc.free(key);
            if (self.opened_store) |opened| opened.docstore.putBatch(&.{}, &.{key}) catch {};
            if (self.runtime) |runtime| deleteRuntime(runtime, key) catch {};
            if (self.meta.fetchRemove(job_id)) |removed| self.alloc.free(removed.value.owner);
            if (self.jobs.fetchRemove(job_id)) |removed| {
                self.alloc.free(removed.value);
                self.alloc.free(removed.key);
            }
        }
    }

    fn putLocked(self: *Store, record: Record) !void {
        const encoded = try std.json.Stringify.valueAlloc(self.alloc, record, .{});
        errdefer self.alloc.free(encoded);
        if (self.opened_store != null or self.runtime != null) {
            const key = try std.fmt.allocPrint(self.alloc, "{s}{s}", .{ key_prefix, record.job_id });
            defer self.alloc.free(key);
            if (self.opened_store) |opened| try opened.docstore.put(key, encoded);
            if (self.runtime) |runtime| {
                var txn = try runtime.beginWrite();
                errdefer txn.abort();
                try txn.put(key, encoded);
                try txn.commit();
            }
        }
        const owner = try self.alloc.dupe(u8, record.owner);
        errdefer self.alloc.free(owner);
        try self.meta.ensureUnusedCapacity(self.alloc, 1);
        if (self.jobs.getEntry(record.job_id)) |entry| {
            self.alloc.free(entry.value_ptr.*);
            entry.value_ptr.* = encoded;
        } else {
            const key = try self.alloc.dupe(u8, record.job_id);
            errdefer self.alloc.free(key);
            try self.jobs.put(self.alloc, key, encoded);
        }
        const key = self.jobs.getKey(record.job_id).?;
        const slot = self.meta.getOrPutAssumeCapacity(key);
        if (slot.found_existing) self.alloc.free(slot.value_ptr.owner);
        slot.value_ptr.* = .{ .owner = owner, .active = !record.terminal() };
    }
};

fn deleteRuntime(runtime: *backend_erased.Store, key: []const u8) !void {
    var txn = try runtime.beginWrite();
    errdefer txn.abort();
    txn.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    try txn.commit();
}

/// Unguessable job identifier from caller-supplied entropy.
pub fn formatJobId(buf: *[36]u8, entropy: [16]u8) []const u8 {
    const hex = std.fmt.bytesToHex(entropy, .lower);
    @memcpy(buf[0..4], "rsj_");
    @memcpy(buf[4..36], &hex);
    return buf[0..36];
}

pub fn validJobId(job_id: []const u8) bool {
    if (job_id.len != 36 or !std.mem.startsWith(u8, job_id, "rsj_")) return false;
    for (job_id[4..]) |c| if (!std.ascii.isHex(c) or std.ascii.isUpper(c)) return false;
    return true;
}

/// Durable jobs persist the request. Reject inline credentials; generators
/// must use `${secret:...}` references, environment fallbacks or server-side
/// connections instead.
pub fn validateRequest(arena: std.mem.Allocator, request: std.json.Value) !void {
    if (request != .object) return error.InvalidResearchAgentRequest;
    if (containsInlineSecret(request)) return error.InlineCredentialNotAllowed;
    _ = arena;
}

fn containsInlineSecret(value: std.json.Value) bool {
    switch (value) {
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                const key = entry.key_ptr.*;
                const sensitive = std.mem.eql(u8, key, "api_key") or std.mem.eql(u8, key, "secret_access_key") or std.mem.eql(u8, key, "session_token") or std.mem.eql(u8, key, "credentials_json");
                if (sensitive and entry.value_ptr.* == .string) {
                    const text = entry.value_ptr.string;
                    if (text.len > 0 and !(std.mem.startsWith(u8, text, "${secret:") or std.mem.startsWith(u8, text, "${env:"))) return true;
                }
                if (containsInlineSecret(entry.value_ptr.*)) return true;
            }
        },
        .array => |array| for (array.items) |item| {
            if (containsInlineSecret(item)) return true;
        },
        else => {},
    }
    return false;
}

/// Normalize a start request: jobs never stream and never ask the user.
pub fn normalizeRequest(arena: std.mem.Allocator, request: std.json.Value) ![]const u8 {
    if (request != .object) return error.InvalidResearchAgentRequest;
    var object = try request.object.clone(arena);
    try object.put(arena, "stream", .{ .bool = false });
    try object.put(arena, "interactive", .{ .bool = false });
    return std.json.Stringify.valueAlloc(arena, std.json.Value{ .object = object }, .{});
}

/// Errors after which the same pass may succeed if advanced again.
fn transient(err: anyerror) bool {
    return switch (err) {
        error.RateLimit,
        error.GenerationCapacityUnavailable,
        error.GenerateRequestFailed,
        error.EmptyResponse,
        error.Timeout,
        error.DeadlineExceeded,
        error.Cancelled,
        error.Canceled,
        => true,
        else => false,
    };
}

/// Run up to `max_phases` phases of the stored request and produce the
/// checkpoint to persist. Never returns request errors to the caller: they
/// become a failed job with last_error.
pub fn advance(
    arena: std.mem.Allocator,
    query_runner: research_agent.QueryRunner,
    generator: research_agent.GenerationRunner,
    request_json: []const u8,
    max_phases: usize,
    deadline_ns: ?u64,
) !Outcome {
    const parsed = research_agent.parseRequest(arena, request_json) catch |err| switch (err) {
        else => return .{ .state = .failed, .phase = "plan", .request = request_json, .result = "{}", .last_error = @errorName(err) },
    };
    const current_phase: []const u8 = if (parsed.request.research_state) |state| @tagName(state.phase) else "plan";
    const result = research_agent.run(arena, query_runner, generator, parsed, null, .{ .max_phases = max_phases, .deadline_ns = deadline_ns }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return .{
            .state = if (transient(err)) .queued else .failed,
            .phase = current_phase,
            .request = request_json,
            .result = "{}",
            .last_error = @errorName(err),
        },
    };
    var object = parsed.raw;
    const state_json = try std.json.Stringify.valueAlloc(arena, result.research_state, .{ .emit_null_optional_fields = false });
    try object.put(arena, "research_state", try std.json.parseFromSliceLeaky(std.json.Value, arena, state_json, .{ .allocate = .alloc_always }));
    const phase = result.research_state.phase;
    const reason: ?[]const u8 = if (result.incomplete_details) |details| details.reason else null;
    const state: JobState = if (phase == .done)
        .succeeded
    else switch (result.status) {
        .in_progress => .queued,
        .incomplete => if (reason != null and std.mem.eql(u8, reason.?, "deadline")) .queued else .failed,
        .completed => .succeeded,
        .clarification_required, .failed => .failed,
    };
    return .{
        .state = state,
        .phase = @tagName(phase),
        .request = try std.json.Stringify.valueAlloc(arena, std.json.Value{ .object = object }, .{}),
        .result = try std.json.Stringify.valueAlloc(arena, result, .{ .emit_null_optional_fields = false }),
        .last_error = if (state == .failed) reason orelse @tagName(result.status) else null,
    };
}

/// Run up to `max_phases` phases of a claimed job, one phase per pass, and
/// checkpoint after each. Stops early on a terminal or failed phase, on
/// cancellation, or when the attempt is superseded. Always ends with
/// `finish` unless superseded, and returns the latest record.
pub fn advanceClaimed(
    store: *Store,
    arena: std.mem.Allocator,
    query_runner: research_agent.QueryRunner,
    generator: research_agent.GenerationRunner,
    claimed: Record,
    max_phases: usize,
    deadline_ns: ?u64,
    lease_ms: u64,
) !Record {
    var current = claimed;
    var phase: usize = 0;
    while (true) : (phase += 1) {
        const outcome = try advance(arena, query_runner, generator, current.request, 1, deadline_ns);
        const expired = if (deadline_ns) |deadline| platform_time.monotonicNs() >= deadline else false;
        const last = phase + 1 >= max_phases or outcome.state != .queued or expired;
        if (last) return store.finish(arena, claimed, outcome);
        current = (try store.checkpoint(arena, claimed, outcome, lease_ms)) orelse
            return store.finish(arena, claimed, outcome);
    }
}

/// Public ResearchJob JSON for a record. The stored request is not exposed.
pub fn jobJson(arena: std.mem.Allocator, record: Record) ![]const u8 {
    var object = std.json.ObjectMap.empty;
    try object.put(arena, "job_id", .{ .string = record.job_id });
    try object.put(arena, "state", .{ .string = @tagName(record.state) });
    try object.put(arena, "phase", .{ .string = record.phase });
    try object.put(arena, "query", .{ .string = record.query });
    try object.put(arena, "advances", .{ .integer = @intCast(record.advances) });
    try object.put(arena, "cancel_requested", .{ .bool = record.cancel_requested });
    if (record.last_error) |message| try object.put(arena, "last_error", .{ .string = message });
    try object.put(arena, "created_at_ms", .{ .integer = @intCast(record.created_at_ms) });
    try object.put(arena, "updated_at_ms", .{ .integer = @intCast(record.updated_at_ms) });
    try object.put(arena, "expires_at_ms", .{ .integer = @intCast(record.expires_at_ms) });
    if (record.result) |result| if (result.len > 2) {
        const value = std.json.parseFromSliceLeaky(std.json.Value, arena, result, .{ .allocate = .alloc_always }) catch null;
        if (value) |v| try object.put(arena, "result", v);
    };
    return std.json.Stringify.valueAlloc(arena, std.json.Value{ .object = object }, .{});
}

pub fn nowMillis() u64 {
    return @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms);
}

/// Waiters yield the CPU after a brief spin instead of busy-spinning while
/// another operation writes a record to disk.
fn lock(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

test "research job store scopes jobs to their owner and fences attempts" {
    const alloc = std.testing.allocator;
    var store = Store.init(alloc, .{ .max_active_per_owner = 2 });
    defer store.deinit();
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const created = try store.create(alloc, "rsj_a", "alice", "q", "{\"query\":\"q\"}");
    alloc.free(created);
    try std.testing.expect((try store.load(arena, "rsj_a", "mallory")) == null);
    try std.testing.expectEqual(JobState.queued, (try store.load(arena, "rsj_a", "alice")).?.state);

    const first = (try store.begin(arena, "rsj_a", "alice", 60_000)).?.started;
    try std.testing.expectEqual(@as(u64, 1), first.attempt);
    // A concurrent advance sees the lease.
    try std.testing.expect((try store.begin(arena, "rsj_a", "alice", 60_000)).? == .busy);
    const done = try store.finish(arena, first, .{ .state = .queued, .phase = "research", .request = "{\"query\":\"q\",\"research_state\":{\"phase\":\"research\"}}", .result = "{}" });
    try std.testing.expectEqual(JobState.queued, done.state);
    try std.testing.expectEqualStrings("research", done.phase);
    try std.testing.expectEqual(@as(u64, 1), done.advances);

    // An expired lease is superseded and the late checkpoint is discarded.
    const stale = (try store.begin(arena, "rsj_a", "alice", 0)).?.started;
    const fresh = (try store.begin(arena, "rsj_a", "alice", 60_000)).?.started;
    try std.testing.expectEqual(stale.attempt + 1, fresh.attempt);
    const discarded = try store.finish(arena, stale, .{ .state = .succeeded, .phase = "done", .request = "{}", .result = "{}" });
    try std.testing.expectEqual(JobState.running, discarded.state);

    // Cancellation during a pass wins over a non-terminal checkpoint.
    _ = (try store.requestCancel(arena, "rsj_a", "alice")).?;
    const cancelled = try store.finish(arena, fresh, .{ .state = .queued, .phase = "reflect", .request = "{}", .result = "{}" });
    try std.testing.expectEqual(JobState.cancelled, cancelled.state);
    try std.testing.expect((try store.begin(arena, "rsj_a", "alice", 60_000)).? == .terminal);

    // Active-job quota per owner.
    alloc.free(try store.create(alloc, "rsj_b", "alice", "q", "{}"));
    alloc.free(try store.create(alloc, "rsj_c", "alice", "q", "{}"));
    try std.testing.expectError(error.TooManyActiveJobs, store.create(alloc, "rsj_d", "alice", "q", "{}"));
    alloc.free(try store.create(alloc, "rsj_e", "bob", "q", "{}"));
}

test "research job requests reject inline credentials and ids are well formed" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const inline_key = try std.json.parseFromSliceLeaky(std.json.Value, arena, "{\"generator\":{\"provider\":\"openai\",\"api_key\":\"sk-live\"}}", .{});
    try std.testing.expectError(error.InlineCredentialNotAllowed, validateRequest(arena, inline_key));
    const nested = try std.json.parseFromSliceLeaky(std.json.Value, arena, "{\"tools\":{\"web_search_config\":{\"provider\":\"exa\",\"api_key\":\"k\"}}}", .{});
    try std.testing.expectError(error.InlineCredentialNotAllowed, validateRequest(arena, nested));
    const referenced = try std.json.parseFromSliceLeaky(std.json.Value, arena, "{\"generator\":{\"provider\":\"openai\",\"api_key\":\"${secret:openai.key}\"}}", .{});
    try validateRequest(arena, referenced);
    var buf: [36]u8 = undefined;
    const id = formatJobId(&buf, [_]u8{0xab} ** 16);
    try std.testing.expect(validJobId(id));
    try std.testing.expect(!validJobId("rsj_../../etc"));
}

test "research job store survives reopen and recovers interrupted attempts" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const pid: u64 = @intCast(std.posix.system.getpid());
    const db_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/research-jobs-{d}-{x}", .{ tmp.sub_path, pid, std.testing.random_seed });
    defer alloc.free(db_path);
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    {
        var store = Store.init(alloc, .{});
        defer store.deinit();
        const opened = try alloc.create(OpenedStore);
        opened.* = try OpenedStore.open(alloc, db_path);
        try store.attachOpenedStore(opened);
        alloc.free(try store.create(alloc, "rsj_x", "alice", "q", "{\"query\":\"q\"}"));
        _ = (try store.begin(arena, "rsj_x", "alice", 60_000)).?.started;
    }
    var store = Store.init(alloc, .{});
    defer store.deinit();
    const opened = try alloc.create(OpenedStore);
    opened.* = try OpenedStore.open(alloc, db_path);
    try store.attachOpenedStore(opened);
    const recovered = (try store.load(arena, "rsj_x", "alice")).?;
    try std.testing.expectEqual(JobState.queued, recovered.state);
    try std.testing.expectEqualStrings("recovered_interrupted_attempt", recovered.last_error.?);
    try std.testing.expectEqual(@as(u64, 1), recovered.attempt);
}

test "research job queries are stored on a UTF-8 boundary and quotas use cached metadata" {
    const alloc = std.testing.allocator;
    var store = Store.init(alloc, .{ .max_active_per_owner = 1 });
    defer store.deinit();
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    // 4095 ASCII bytes then a multibyte code point across the 4096 limit.
    const query = try std.mem.concat(arena, u8, &.{ "q" ** 4095, "\u{1f600}" });
    alloc.free(try store.create(alloc, "rsj_u", "alice", query, "{}"));
    const record = (try store.load(arena, "rsj_u", "alice")).?;
    try std.testing.expect(std.unicode.utf8ValidateSlice(record.query));
    try std.testing.expectError(error.TooManyActiveJobs, store.create(alloc, "rsj_v", "alice", "q", "{}"));
    // A terminal job no longer counts against the quota.
    _ = (try store.requestCancel(arena, "rsj_u", "alice")).?;
    alloc.free(try store.create(alloc, "rsj_v", "alice", "q", "{}"));
}

test "research job cleanup removes records that no longer parse" {
    const alloc = std.testing.allocator;
    var store = Store.init(alloc, .{});
    defer store.deinit();
    alloc.free(try store.create(alloc, "rsj_bad", "alice", "q", "{}"));
    const slot = store.jobs.getPtr("rsj_bad").?;
    alloc.free(slot.*);
    slot.* = try alloc.dupe(u8, "{\"job_id\":\"rsj_bad\",\"owner\":\"\xe4\"");
    store.cleanupExpiredJobs();
    try std.testing.expect(!store.jobs.contains("rsj_bad"));
    try std.testing.expect(!store.meta.contains("rsj_bad"));
}
