// Copyright 2026 Antfly, Inc.
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

//! Registry of live streaming transcription sessions.
//!
//! Sessions are heap entries keyed by a caller-supplied random id. The
//! registry lock only guards the map; an append marks its entry `in_use`
//! and drops the lock before running the decoder, so one slow session never
//! blocks the others. A concurrent append, delete, or sweep of an in-use
//! entry is refused instead of waiting. Idle entries expire after their TTL
//! and are reclaimed on the next create or explicit sweep.

const std = @import("std");
const streaming = @import("../pipelines/streaming_transcription.zig");

pub const id_len: usize = 32;
pub const default_max_sessions: usize = 64;
/// Audio buffered across every open session, in milliseconds. 64 sessions
/// each holding a full 60 s buffer would pin 245 MiB of f32 samples; this
/// keeps the node-wide worst case near 40 MiB.
pub const default_max_total_buffered_ms: u64 = 600_000;
pub const default_ttl_seconds: u32 = 300;
pub const max_ttl_seconds: u32 = 3600;

const ns_per_s: u64 = std.time.ns_per_s;

pub const CreateParams = struct {
    id: [id_len]u8,
    model: []const u8,
    language: ?[]const u8 = null,
    streaming: streaming.Config = .{},
    ttl_seconds: u32 = default_ttl_seconds,
    now_wall_s: i64,
    now_mono_ns: u64,
    /// Io used to wake event streams of entries retired during the sweep.
    io: std.Io,
};

/// Owned copy of an entry's public state, safe to use after the registry
/// lock is released.
pub const Snapshot = struct {
    id: [id_len]u8,
    model: []u8,
    language: ?[]u8,
    created: i64,
    expires_at: i64,
    stats: streaming.Stats,

    pub fn deinit(self: *Snapshot, allocator: std.mem.Allocator) void {
        allocator.free(self.model);
        if (self.language) |language| allocator.free(language);
    }
};

pub const Entry = struct {
    id: [id_len]u8,
    model: []u8,
    language: ?[]u8,
    session: streaming.Session,
    ttl_ns: u64,
    created_wall_s: i64,
    created_mono_ns: u64,
    last_used_mono_ns: u64,
    in_use: bool = false,
    /// Stats published at the last `release`, so snapshots never read the
    /// session while an append is mutating it.
    published_stats: streaming.Stats = .{ .buffered_ms = 0, .total_ms = 0, .decodes = 0, .finals = 0, .partials = 0 },
    /// Owned copy of the conditioning prompt the session config points at.
    prompt: ?[]u8 = null,
    /// Events not yet delivered to the events stream, oldest first.
    pending: std.ArrayListUnmanaged(streaming.Event) = .empty,
    /// Set when `pending` grows or the entry closes; event streams wait on it.
    wake: std.Io.Event = .unset,
    /// Number of event streams holding a reference. A closed entry is
    /// destroyed by whoever drops the last reference.
    watchers: u32 = 0,
    closed: bool = false,

    pub fn expiresAt(self: *const Entry) i64 {
        const idle_base_s: i64 = @intCast((self.last_used_mono_ns -| self.created_mono_ns) / ns_per_s);
        const ttl_s: i64 = @intCast(self.ttl_ns / ns_per_s);
        return self.created_wall_s + idle_base_s + ttl_s;
    }

    fn expired(self: *const Entry, now_mono_ns: u64) bool {
        return now_mono_ns -| self.last_used_mono_ns >= self.ttl_ns;
    }

    fn snapshot(self: *const Entry, allocator: std.mem.Allocator) !Snapshot {
        const model = try allocator.dupe(u8, self.model);
        errdefer allocator.free(model);
        const language = if (self.language) |l| try allocator.dupe(u8, l) else null;
        return .{
            .id = self.id,
            .model = model,
            .language = language,
            .created = self.created_wall_s,
            .expires_at = self.expiresAt(),
            .stats = self.published_stats,
        };
    }

    fn destroy(self: *Entry, allocator: std.mem.Allocator) void {
        self.session.deinit();
        for (self.pending.items) |*event| event.deinit(allocator);
        self.pending.deinit(allocator);
        allocator.free(self.model);
        if (self.language) |language| allocator.free(language);
        if (self.prompt) |prompt| allocator.free(prompt);
        allocator.destroy(self);
    }
};

/// Bounded copy of undelivered events for one event-stream drain.
pub const max_pending_events: usize = 256;

pub const Registry = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.StringHashMapUnmanaged(*Entry) = .empty,
    /// Removed entries still referenced by an event stream. Destroyed when
    /// their last watcher leaves.
    closed: std.ArrayListUnmanaged(*Entry) = .empty,
    max_sessions: usize = default_max_sessions,
    max_total_buffered_ms: u64 = default_max_total_buffered_ms,

    pub fn init(allocator: std.mem.Allocator) Registry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Registry) void {
        var it = self.entries.valueIterator();
        while (it.next()) |entry| entry.*.destroy(self.allocator);
        self.entries.deinit(self.allocator);
        for (self.closed.items) |entry| entry.destroy(self.allocator);
        self.closed.deinit(self.allocator);
    }

    pub fn count(self: *Registry) usize {
        self.lock();
        defer self.mutex.unlock();
        return self.entries.count();
    }

    /// Register a session and return a snapshot allocated from `allocator`.
    /// Expired idle sessions are reclaimed first so a full registry recovers
    /// without operator action.
    pub fn create(self: *Registry, allocator: std.mem.Allocator, params: CreateParams) !Snapshot {
        if (params.model.len == 0) return error.ModelRequired;
        if (params.ttl_seconds == 0 or params.ttl_seconds > max_ttl_seconds) return error.InvalidSessionTtl;
        var session = try streaming.Session.init(self.allocator, params.streaming);
        errdefer session.deinit();

        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        const model = try self.allocator.dupe(u8, params.model);
        errdefer self.allocator.free(model);
        const language = if (params.language) |l| try self.allocator.dupe(u8, l) else null;
        errdefer if (language) |l| self.allocator.free(l);
        const prompt = if (params.streaming.initial_prompt) |p| try self.allocator.dupe(u8, p) else null;
        errdefer if (prompt) |p| self.allocator.free(p);
        session.config.initial_prompt = prompt;
        entry.* = .{
            .id = params.id,
            .model = model,
            .language = language,
            .prompt = prompt,
            .session = session,
            .ttl_ns = @as(u64, params.ttl_seconds) * ns_per_s,
            .created_wall_s = params.now_wall_s,
            .created_mono_ns = params.now_mono_ns,
            .last_used_mono_ns = params.now_mono_ns,
        };
        // Everything that can fail happens before the entry is visible, so
        // a failure never leaves a freed entry in the map.
        var created = try entry.snapshot(allocator);
        errdefer created.deinit(allocator);

        self.lock();
        defer self.mutex.unlock();
        _ = self.sweepLocked(params.now_mono_ns, params.io);
        if (self.entries.count() >= self.max_sessions) return error.TooManySessions;
        const slot = try self.entries.getOrPut(self.allocator, &entry.id);
        if (slot.found_existing) return error.DuplicateSessionId;
        slot.value_ptr.* = entry;
        return created;
    }

    pub fn contains(self: *Registry, id: []const u8) bool {
        self.lock();
        defer self.mutex.unlock();
        return self.entries.contains(id);
    }

    pub fn snapshot(self: *Registry, allocator: std.mem.Allocator, id: []const u8) !?Snapshot {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.entries.get(id) orelse return null;
        return try entry.snapshot(allocator);
    }

    /// Mark the entry busy and hand it to the caller. The pointer stays
    /// valid until `release`; nobody else can remove a busy entry.
    pub fn acquire(self: *Registry, id: []const u8, now_mono_ns: u64, io: std.Io) error{ SessionNotFound, SessionBusy, SessionExpired }!*Entry {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.entries.get(id) orelse return error.SessionNotFound;
        if (entry.in_use) return error.SessionBusy;
        if (entry.expired(now_mono_ns)) {
            _ = self.entries.remove(id);
            self.retireLocked(entry, io);
            return error.SessionExpired;
        }
        entry.in_use = true;
        entry.last_used_mono_ns = now_mono_ns;
        return entry;
    }

    /// Whether `entry` (held by the caller) may buffer `add_ms` more audio
    /// without pushing the node past `max_total_buffered_ms`. Other sessions
    /// are counted at their published size. Passing reserves the room: the
    /// entry's published size becomes its size after the append, so a
    /// session appending concurrently sees it at once rather than at the
    /// next `release`. A failed append leaves the reservation in place
    /// until `release` publishes the real size, which only errs safe.
    pub fn canBuffer(self: *Registry, entry: *Entry, add_ms: u64) bool {
        const own_ms = entry.session.stats().buffered_ms;
        self.lock();
        defer self.mutex.unlock();
        var total: u64 = own_ms + add_ms;
        var it = self.entries.valueIterator();
        while (it.next()) |candidate| {
            if (candidate.* == entry) continue;
            total += candidate.*.published_stats.buffered_ms;
        }
        if (total > self.max_total_buffered_ms) return false;
        entry.published_stats.buffered_ms = own_ms + add_ms;
        return true;
    }

    pub fn release(self: *Registry, entry: *Entry, now_mono_ns: u64) void {
        const stats = entry.session.stats();
        self.lock();
        defer self.mutex.unlock();
        entry.published_stats = stats;
        entry.in_use = false;
        entry.last_used_mono_ns = now_mono_ns;
    }

    pub fn remove(self: *Registry, id: []const u8, io: std.Io) error{ SessionNotFound, SessionBusy }!void {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.entries.get(id) orelse return error.SessionNotFound;
        if (entry.in_use) return error.SessionBusy;
        _ = self.entries.remove(id);
        self.retireLocked(entry, io);
    }

    /// Detach an entry from the map. With watchers it stays alive, marked
    /// closed, until the last watcher calls `unwatch`.
    fn retireLocked(self: *Registry, entry: *Entry, io: std.Io) void {
        if (entry.watchers == 0) {
            entry.destroy(self.allocator);
            return;
        }
        entry.closed = true;
        entry.wake.set(io);
        self.closed.append(self.allocator, entry) catch {
            // Without a slot to park it, the entry leaks rather than dangles;
            // watchers still observe `closed` and stop.
        };
    }

    /// Register an event-stream reader. The returned entry stays valid until
    /// `unwatch`, even if the session is deleted or expires meanwhile.
    pub fn watch(self: *Registry, id: []const u8) error{SessionNotFound}!*Entry {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.entries.get(id) orelse return error.SessionNotFound;
        entry.watchers += 1;
        return entry;
    }

    pub fn unwatch(self: *Registry, entry: *Entry) void {
        self.lock();
        defer self.mutex.unlock();
        entry.watchers -= 1;
        if (entry.watchers != 0 or !entry.closed) return;
        for (self.closed.items, 0..) |candidate, index| {
            if (candidate == entry) {
                _ = self.closed.swapRemove(index);
                break;
            }
        }
        entry.destroy(self.allocator);
    }

    /// Queue events for the entry's event stream and wake it. Events are
    /// moved; the caller must not deinit them afterwards. On error nothing
    /// was moved and the caller still owns every event. Beyond the pending
    /// cap the oldest partials are dropped first, finals are kept.
    pub fn publish(self: *Registry, entry: *Entry, events: []streaming.Event, io: std.Io) !void {
        self.lock();
        defer self.mutex.unlock();
        // All or nothing: room for the whole batch is made before any event
        // changes hands, so a failure cannot leave part of it queued.
        try entry.pending.ensureUnusedCapacity(self.allocator, events.len);
        entry.pending.appendSliceAssumeCapacity(events);
        var index: usize = 0;
        while (entry.pending.items.len > max_pending_events and index < entry.pending.items.len) {
            if (entry.pending.items[index].kind == .partial) {
                var dropped = entry.pending.orderedRemove(index);
                dropped.deinit(self.allocator);
            } else index += 1;
        }
        entry.wake.set(io);
    }

    /// Move undelivered events into `out` and reset the wake signal under
    /// the lock, so a publish racing with the drain re-triggers the waiter.
    /// Returns false once the entry is closed and drained.
    pub fn drain(self: *Registry, entry: *Entry, out: *std.ArrayListUnmanaged(streaming.Event)) !bool {
        self.lock();
        defer self.mutex.unlock();
        const before = out.items.len;
        try out.appendSlice(self.allocator, entry.pending.items);
        entry.pending.clearRetainingCapacity();
        entry.wake.reset();
        return !entry.closed or out.items.len > before;
    }

    pub fn sweepExpired(self: *Registry, now_mono_ns: u64, io: std.Io) usize {
        self.lock();
        defer self.mutex.unlock();
        return self.sweepLocked(now_mono_ns, io);
    }

    fn sweepLocked(self: *Registry, now_mono_ns: u64, io: std.Io) usize {
        var removed: usize = 0;
        var it = self.entries.iterator();
        var doomed: [default_max_sessions]*Entry = undefined;
        var doomed_len: usize = 0;
        while (it.next()) |kv| {
            const entry = kv.value_ptr.*;
            if (entry.in_use or !entry.expired(now_mono_ns)) continue;
            if (doomed_len == doomed.len) break;
            doomed[doomed_len] = entry;
            doomed_len += 1;
        }
        for (doomed[0..doomed_len]) |entry| {
            _ = self.entries.remove(&entry.id);
            self.retireLocked(entry, io);
            removed += 1;
        }
        return removed;
    }

    fn lock(self: *Registry) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
};

/// Lower-case hex rendering of 16 random bytes.
pub fn formatId(random: [id_len / 2]u8) [id_len]u8 {
    var out: [id_len]u8 = undefined;
    const digits = "0123456789abcdef";
    for (random, 0..) |byte, i| {
        out[i * 2] = digits[byte >> 4];
        out[i * 2 + 1] = digits[byte & 0x0f];
    }
    return out;
}

pub fn isValidId(id: []const u8) bool {
    if (id.len != id_len) return false;
    for (id) |c| {
        if (!std.ascii.isHex(c) or std.ascii.isUpper(c)) return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn testId(seed: u8) [id_len]u8 {
    var raw: [id_len / 2]u8 = undefined;
    @memset(&raw, seed);
    return formatId(raw);
}

test "registry creates snapshots and refuses concurrent use" {
    const allocator = std.testing.allocator;
    var registry = Registry.init(allocator);
    defer registry.deinit();

    var created = try registry.create(allocator, .{
        .id = testId(1),
        .model = "openai/whisper-tiny",
        .language = "en",
        .ttl_seconds = 10,
        .now_wall_s = 1_000,
        .now_mono_ns = 0,
        .io = std.testing.io,
    });
    defer created.deinit(allocator);
    try std.testing.expectEqualStrings("openai/whisper-tiny", created.model);
    try std.testing.expectEqualStrings("en", created.language.?);
    try std.testing.expectEqual(@as(i64, 1_010), created.expires_at);
    try std.testing.expectEqual(@as(usize, 1), registry.count());

    const entry = try registry.acquire(&created.id, 2 * ns_per_s, std.testing.io);
    try std.testing.expectError(error.SessionBusy, registry.acquire(&created.id, 2 * ns_per_s, std.testing.io));
    try std.testing.expectError(error.SessionBusy, registry.remove(&created.id, std.testing.io));
    try std.testing.expectEqual(@as(usize, 0), registry.sweepExpired(100 * ns_per_s, std.testing.io));
    registry.release(entry, 3 * ns_per_s);

    var current = (try registry.snapshot(allocator, &created.id)).?;
    defer current.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1_013), current.expires_at);

    try std.testing.expect(registry.contains(&created.id));
    try registry.remove(&created.id, std.testing.io);
    try std.testing.expect(!registry.contains(&created.id));
    try std.testing.expectError(error.SessionNotFound, registry.remove(&created.id, std.testing.io));
    try std.testing.expect((try registry.snapshot(allocator, &created.id)) == null);
}

test "registry caps audio buffered across sessions" {
    const allocator = std.testing.allocator;
    var registry = Registry.init(allocator);
    registry.max_total_buffered_ms = 1000;
    defer registry.deinit();

    var a = try registry.create(allocator, .{ .id = testId(1), .model = "m", .now_wall_s = 0, .now_mono_ns = 0, .io = std.testing.io });
    defer a.deinit(allocator);
    var b = try registry.create(allocator, .{ .id = testId(2), .model = "m", .now_wall_s = 0, .now_mono_ns = 0, .io = std.testing.io });
    defer b.deinit(allocator);

    const silence = [_]f32{0} ** 16_000;
    const entry_a = try registry.acquire(&a.id, 1, std.testing.io);
    try std.testing.expect(registry.canBuffer(entry_a, 1000));
    try std.testing.expect(!registry.canBuffer(entry_a, 1001));
    try entry_a.session.append(silence[0..8000], 16_000);
    try std.testing.expect(registry.canBuffer(entry_a, 500));
    try std.testing.expect(!registry.canBuffer(entry_a, 501));
    registry.release(entry_a, 2);

    // Session a's 500 ms is published now and counts against session b.
    const entry_b = try registry.acquire(&b.id, 3, std.testing.io);
    try std.testing.expect(registry.canBuffer(entry_b, 500));
    try std.testing.expect(!registry.canBuffer(entry_b, 501));
    registry.release(entry_b, 4);
}

test "registry counts a concurrent session's reservation before it releases" {
    const allocator = std.testing.allocator;
    var registry = Registry.init(allocator);
    registry.max_total_buffered_ms = 1000;
    defer registry.deinit();

    var a = try registry.create(allocator, .{ .id = testId(1), .model = "m", .now_wall_s = 0, .now_mono_ns = 0, .io = std.testing.io });
    defer a.deinit(allocator);
    var b = try registry.create(allocator, .{ .id = testId(2), .model = "m", .now_wall_s = 0, .now_mono_ns = 0, .io = std.testing.io });
    defer b.deinit(allocator);

    // Both sessions are mid-append: a has been cleared for 600 ms but has
    // not released yet, so b may only take the remaining 400 ms.
    const entry_a = try registry.acquire(&a.id, 1, std.testing.io);
    const entry_b = try registry.acquire(&b.id, 1, std.testing.io);
    try std.testing.expect(registry.canBuffer(entry_a, 600));
    try std.testing.expect(!registry.canBuffer(entry_b, 401));
    try std.testing.expect(registry.canBuffer(entry_b, 400));
    try std.testing.expect(!registry.canBuffer(entry_a, 601));

    // A smaller append after a larger reservation shrinks it.
    try std.testing.expect(registry.canBuffer(entry_a, 100));
    try std.testing.expect(registry.canBuffer(entry_b, 900));
    registry.release(entry_a, 2);
    registry.release(entry_b, 2);
}

test "registry does not keep an entry whose creation failed" {
    const allocator = std.testing.allocator;
    var registry = Registry.init(allocator);
    defer registry.deinit();

    // The response snapshot is the last allocation of `create`; failing it
    // must not leave the (freed) entry registered.
    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, registry.create(failing.allocator(), .{ .id = testId(1), .model = "m", .now_wall_s = 0, .now_mono_ns = 0, .io = std.testing.io }));
    try std.testing.expect(!registry.contains(&testId(1)));
    try std.testing.expectEqual(@as(usize, 0), registry.count());

    var a = try registry.create(allocator, .{ .id = testId(1), .model = "m", .now_wall_s = 0, .now_mono_ns = 0, .io = std.testing.io });
    defer a.deinit(allocator);
    try std.testing.expect(registry.contains(&a.id));
}

test "registry publishes a batch of events all or nothing" {
    const allocator = std.testing.allocator;
    var registry = Registry.init(allocator);
    defer registry.deinit();
    var a = try registry.create(allocator, .{ .id = testId(1), .model = "m", .now_wall_s = 0, .now_mono_ns = 0, .io = std.testing.io });
    defer a.deinit(allocator);
    const entry = try registry.watch(&a.id);
    defer registry.unwatch(entry);

    var events: [2]streaming.Event = undefined;
    for (&events, 0..) |*event, i| {
        event.* = .{
            .kind = .final,
            .sequence = i,
            .text = try allocator.dupe(u8, "x"),
            .stable_text = try allocator.dupe(u8, "x"),
            .start_ms = 0,
            .end_ms = 1,
            .language = null,
        };
    }
    defer for (&events) |*event| event.deinit(allocator);

    // The queue cannot grow: nothing may have moved, so the caller's
    // cleanup above frees each event exactly once.
    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    registry.allocator = failing.allocator();
    try std.testing.expectError(error.OutOfMemory, registry.publish(entry, &events, std.testing.io));
    registry.allocator = allocator;
    try std.testing.expectEqual(@as(usize, 0), entry.pending.items.len);

    // A successful publish moves the batch; hand the caller fresh copies
    // so its deferred cleanup stays balanced.
    var moved: [2]streaming.Event = undefined;
    for (&moved, 0..) |*event, i| {
        event.* = .{
            .kind = .final,
            .sequence = 10 + i,
            .text = try allocator.dupe(u8, "y"),
            .stable_text = try allocator.dupe(u8, "y"),
            .start_ms = 0,
            .end_ms = 1,
            .language = null,
        };
    }
    try registry.publish(entry, &moved, std.testing.io);
    var out = std.ArrayListUnmanaged(streaming.Event).empty;
    defer {
        for (out.items) |*event| event.deinit(allocator);
        out.deinit(allocator);
    }
    try std.testing.expect(try registry.drain(entry, &out));
    try std.testing.expectEqual(@as(usize, 2), out.items.len);
    try std.testing.expectEqual(@as(u64, 10), out.items[0].sequence);
}

test "registry expires idle sessions and enforces the cap" {
    const allocator = std.testing.allocator;
    var registry = Registry.init(allocator);
    registry.max_sessions = 2;
    defer registry.deinit();

    var a = try registry.create(allocator, .{ .id = testId(1), .model = "m", .ttl_seconds = 1, .now_wall_s = 0, .now_mono_ns = 0, .io = std.testing.io });
    defer a.deinit(allocator);
    var b = try registry.create(allocator, .{ .id = testId(2), .model = "m", .ttl_seconds = 60, .now_wall_s = 0, .now_mono_ns = 0, .io = std.testing.io });
    defer b.deinit(allocator);
    try std.testing.expectError(error.TooManySessions, registry.create(allocator, .{ .id = testId(3), .model = "m", .now_wall_s = 0, .now_mono_ns = 0, .io = std.testing.io }));

    // Two seconds later the first session has expired, so a third fits.
    var c = try registry.create(allocator, .{ .id = testId(3), .model = "m", .now_wall_s = 2, .now_mono_ns = 2 * ns_per_s, .io = std.testing.io });
    defer c.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), registry.count());
    try std.testing.expectError(error.SessionNotFound, registry.acquire(&a.id, 2 * ns_per_s, std.testing.io));
    try std.testing.expectError(error.SessionExpired, registry.acquire(&b.id, 61 * ns_per_s, std.testing.io));
    try std.testing.expectEqual(@as(usize, 1), registry.count());

    try std.testing.expectError(error.DuplicateSessionId, registry.create(allocator, .{ .id = testId(3), .model = "m", .now_wall_s = 2, .now_mono_ns = 2 * ns_per_s, .io = std.testing.io }));
    try std.testing.expectError(error.InvalidSessionTtl, registry.create(allocator, .{ .id = testId(4), .model = "m", .ttl_seconds = 0, .now_wall_s = 0, .now_mono_ns = 0, .io = std.testing.io }));
    try std.testing.expectError(error.ModelRequired, registry.create(allocator, .{ .id = testId(4), .model = "", .now_wall_s = 0, .now_mono_ns = 0, .io = std.testing.io }));
}

test "session ids are lower-case hex" {
    const id = testId(0xab);
    try std.testing.expect(isValidId(&id));
    try std.testing.expectEqualStrings("abababababababababababababababab", &id);
    try std.testing.expect(!isValidId("ABABABABABABABABABABABABABABABAB"));
    try std.testing.expect(!isValidId("short"));
}

test "registry keeps a watched entry alive until the last watcher leaves" {
    const allocator = std.testing.allocator;
    var registry = Registry.init(allocator);
    defer registry.deinit();
    var created = try registry.create(allocator, .{ .id = testId(9), .model = "m", .now_wall_s = 0, .now_mono_ns = 0, .streaming = .{ .initial_prompt = "Antfly" }, .io = std.testing.io });
    defer created.deinit(allocator);

    const watched = try registry.watch(&created.id);
    try std.testing.expectEqualStrings("Antfly", watched.session.config.initial_prompt.?);
    var events = [_]streaming.Event{.{
        .kind = .final,
        .sequence = 0,
        .text = try allocator.dupe(u8, "hi"),
        .stable_text = try allocator.dupe(u8, "hi"),
        .start_ms = 0,
        .end_ms = 10,
        .language = null,
    }};
    try registry.publish(watched, &events, std.testing.io);
    var drained = std.ArrayListUnmanaged(streaming.Event).empty;
    defer {
        for (drained.items) |*event| event.deinit(allocator);
        drained.deinit(allocator);
    }
    try std.testing.expect(try registry.drain(watched, &drained));
    try std.testing.expectEqual(@as(usize, 1), drained.items.len);

    // Delete while watched: the entry is retired, not destroyed.
    try registry.remove(&created.id, std.testing.io);
    try std.testing.expectEqual(@as(usize, 0), registry.count());
    try std.testing.expect(watched.closed);
    try std.testing.expect(!(try registry.drain(watched, &drained)));
    registry.unwatch(watched);
    try std.testing.expectEqual(@as(usize, 0), registry.closed.items.len);
}
