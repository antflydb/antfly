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

//! Session-owned cache of tree-packed Laya trunk keys and values
//! (models/laya/LAYA.md, "State cache"). A trunk never attends to a branch,
//! so its per-layer keys and values depend only on its tokens and the model.
//! A later request about the same state encodes only its question branches.
//!
//! Entries are host memory on CPU and retained device tensors on Metal, f16 by
//! default (ANTFLY_LAYA_TRUNK_CACHE_DTYPE=f32 keeps them exact). They are
//! bounded by `limit_bytes` and evicted least recently used; entries in use
//! are pinned and never evicted. The bound defaults to
//! ANTFLY_LAYA_TRUNK_CACHE_MB (256 MiB); 0 disables caching. Trunks shorter
//! than `min_tokens` are not cached. With admission configured, every
//! entry holds a KV lease on the model's admission controller.
//!
//! `lookup` reserves an entry's bytes and admission lease before the caller
//! encodes anything, so concurrent misses never exceed the budget, and it
//! lets only one caller fill a given key: later misses for that key wait for
//! the fill instead of duplicating it. An entry that cannot fit is bypassed
//! without encoding it for the cache.
const std = @import("std");
const platform = @import("antfly_platform");
const memory = @import("../runtime/tier/memory.zig");

pub const Precision = enum { f16, f32 };

pub const default_limit_mb = 256;
/// Shorter trunks cost less to re-encode than the cached path's fixed
/// overhead (measured on Metal and CPU with the released checkpoint).
pub const default_min_tokens = 96;

/// Per-layer trunk keys (after RoPE for the encoder) and values, `[T, H]`
/// each, for every encoder layer followed by every decision-head layer, in
/// slot order `[layer][keys, values]`. Host entries store `host16` or
/// `host32`; device entries own backend tensors through `device` (released
/// by `device_deinit`).
const Cleanup = @import("../execution/cleanup_context.zig").Context;
const BackingOwner = @import("../execution/backing_retirement.zig").BackingOwner;
const retirement = @import("../execution/resource_retirement.zig");
const storage = @import("../execution/storage_admission.zig");
const prepared = @import("../execution/prepared_storage_admission.zig");
const DomainPin = @import("../runtime/tier/admission_domain.zig").Pin;

pub const Entry = struct {
    key: [32]u8,
    tokens: usize,
    hidden: usize,
    layers: usize,
    precision: Precision,
    host16: []f16 = &.{},
    host32: []f32 = &.{},
    device: ?*anyopaque = null,
    device_deinit: ?*const fn (?*anyopaque, std.mem.Allocator) void = null,
    lease: ?memory.AdmissionLease = null,
    // Typed cold ownership is attached only after actual admission succeeds.
    // No cached creation Epoch, Model, Source or capability is retained.
    allocation: ?std.mem.Allocator = null,
    metadata_backing: ?*BackingOwner = null,
    quota: ?*retirement.Charge = null,
    unsubmitted: bool = false,
    retired_next: ?*Entry = null,
    retirement_node: retirement.Node = undefined,
    reservation_claimed: bool = true,
    pins: usize = 0,
    last_used: u64 = 0,
    /// A `fill` entry whose publication failed: still in use by its filler,
    /// so it stays reserved and leased until its last `release`.
    orphaned: bool = false,

    fn span(self: *const Entry, slot: usize) [2]usize {
        const n = self.tokens * self.hidden;
        return .{ slot * n, n };
    }
    /// Copy one host slot (`2 * layer` for keys, `+ 1` for values) in.
    pub fn store(self: *Entry, slot: usize, values: []const f32) void {
        const at = self.span(slot);
        switch (self.precision) {
            .f16 => for (self.host16[at[0]..][0..at[1]], values) |*dst, v| {
                dst.* = @floatCast(v);
            },
            .f32 => @memcpy(self.host32[at[0]..][0..at[1]], values),
        }
    }
    /// Copy one host slot out as f32.
    pub fn load(self: *const Entry, slot: usize, out: []f32) void {
        const at = self.span(slot);
        switch (self.precision) {
            .f16 => for (out, self.host16[at[0]..][0..at[1]]) |*dst, v| {
                dst.* = v;
            },
            .f32 => @memcpy(out, self.host32[at[0]..][0..at[1]]),
        }
    }
    pub fn bytes(self: *const Entry) usize {
        const width: usize = if (self.precision == .f16) 2 else 4;
        return 2 * self.layers * self.tokens * self.hidden * width;
    }
};

/// The admission controller, domain, and limits of the owning session.
pub const Admission = struct {
    controller: *memory.AdmissionController,
    backend_class: memory.BackendClass,
    limits: memory.Limits,
    /// Entries live in backend (device) memory rather than host memory.
    device: bool,
};

pub const Stats = struct { hits: u64 = 0, misses: u64 = 0, entries: usize = 0, bytes: usize = 0, evictions: u64 = 0, refusals: u64 = 0, waits: u64 = 0 };

/// What `lookup` hands back. `hit` and `fill` entries are pinned: `release`
/// them when done. A `fill` entry must be filled and then `publish`ed, or
/// `abandon`ed on failure. `bypass` means: do not use the cache.
pub const Lookup = union(enum) { hit: *Entry, fill: *Entry, bypass };

/// One actual paid pointer-array allocation. Its by-value list contains
/// borrowed Entry pointers, but destruction frees metadata only. Creator and
/// cold snapshot refs prevent actual allocation/header ABA across a pause.
const ListOwner = struct {
    allocator: std.mem.Allocator,
    list: std.ArrayListUnmanaged(*Entry),
    backing: *BackingOwner,
    references: std.atomic.Value(usize) = .init(1),
    retired_next: ?*ListOwner = null,

    fn retain(self: *@This()) *@This() {
        const before = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(before != 0 and before != std.math.maxInt(usize));
        return self;
    }
    fn releaseOutside(self: *@This(), current: Cleanup) !void {
        try validateBoundary(current);
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        const allocator = self.allocator;
        const backing = self.backing;
        self.list.deinit(allocator);
        allocator.destroy(self);
        releaseBackingAfterPhysicalDeath(backing, current);
    }
};

fn validHostKvLease(lease: memory.AdmissionLease, admission: Admission, bytes: usize) bool {
    if (admission.device or lease.controller != admission.controller or lease.retain_backend_class != admission.backend_class) return false;
    return std.meta.eql(lease.amounts, memory.AdmissionAmounts{ .host_kv_bytes = bytes });
}
pub fn validateMetadataBinding(binding: storage.BackingReservation, capability: storage.Capability, bytes: usize) !void {
    const reservation = binding.owner.charge.storageReservation() orelse return error.InvalidPhysicalStorageReservation;
    if (!reservation.identity.eql(capability.identity) or binding.owner.admittedBytes() != bytes) return error.InvalidPhysicalStorageReservation;
    if (try binding.construction_epoch.isSealed()) return error.RetirementEpochSealed;
    const actual = capability.currentCleanupEpoch() orelse return error.MissingCurrentCleanupEpoch;
    if (try actual.isSealed()) return error.RetirementEpochSealed;
    if (actual.record != binding.construction_epoch.record or actual.nonce != binding.construction_epoch.nonce) return error.ForeignNativeWeightModel;
}

fn validHostPayload(entry: *const Entry) bool {
    if (entry.device != null or entry.device_deinit != null) return false;
    const geometry = coldFillGeometry(entry.tokens, entry.layers, entry.hidden, entry.precision) catch return false;
    return switch (entry.precision) {
        .f16 => entry.host16.len == geometry.elements and entry.host32.len == 0,
        .f32 => entry.host32.len == geometry.elements and entry.host16.len == 0,
    };
}

fn entryOwnedBy(entry: *const Entry, admission: Admission) bool {
    if (!validHostPayload(entry)) return false;
    const quota = entry.quota orelse return false;
    const backing = entry.metadata_backing orelse return false;
    const reservation = backing.charge.storageReservation() orelse return false;
    const domain = quota.domain orelse return false;
    if (domain.controller() != admission.controller or reservation.identity.controller != admission.controller or
        reservation.identity.backend_class != admission.backend_class) return false;
    return switch (quota.quota) {
        .run => |lease| validHostKvLease(lease, admission, entry.bytes()),
        else => false,
    };
}

fn validateBoundary(current: Cleanup) !void {
    if (current.boundary) |epoch| {
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        return;
    }
    if (!current.outside_execution) return error.CacheRetirementCleanupAuthorityRequired;
}
fn releaseBackingAfterPhysicalDeath(backing: *BackingOwner, current: Cleanup) void {
    if (current.boundary) |epoch| backing.release(epoch) else backing.releaseOutsideExecution();
}

/// A real cold metadata window. Paid array owners and the selected Entry are
/// pinned until explicit outside finish, so pointer/capacity equality cannot
/// accidentally accept freed/reused storage. Initial raw arrays additionally
/// require the SAME local nonwrapping revision on receive.
pub const ColdSnapshot = struct {
    revision: u64,
    limit_bytes: usize,
    min_tokens: usize,
    precision: Precision,
    admission: ?Admission,
    entries: std.ArrayListUnmanaged(*Entry),
    pending: std.ArrayListUnmanaged(*Entry),
    entries_owner: ?*ListOwner,
    pending_owner: ?*ListOwner,
    hit: ?*Entry,
    pending_same_key: bool,
    finished: bool = false,

    /// No Model/lane or FIFO turn may be held by this caller. The Entry pin
    /// can become final after peer eviction; typed physical death is queued,
    /// raw physical death follows its original explicit-outside contract.
    pub fn finishOutsideExecution(self: *@This(), cache: *Cache, current: Cleanup) !void {
        try validateBoundary(current);
        if (self.finished) return cache.drainRetiredOutsideExecution(current);
        if (self.hit) |entry| {
            cache.release(entry);
            self.hit = null;
        }
        if (self.entries_owner) |owner| {
            try owner.releaseOutside(current);
            self.entries_owner = null;
        }
        if (self.pending_owner) |owner| {
            try owner.releaseOutside(current);
            self.pending_owner = null;
        }
        self.finished = true;
        try cache.drainRetiredOutsideExecution(current);
    }
};

pub const ListGeometry = struct {
    capacity: usize,
    bytes: usize,
    replace: bool,
};

/// SAME stdlib allocation path as the real pointer-list producer. A refusing
/// allocator observes requested bytes only; it allocates no dummy buffer and
/// never reads an arbitrary allocator context or copies stdlib private layout.
fn pointerListCapacity(needed: usize) !usize {
    if (needed == 0) return 0;
    const Recorder = struct {
        bytes: usize = 0,
        fn alloc(raw: *anyopaque, len: usize, _: std.mem.Alignment, _: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.bytes == 0);
            self.bytes = len;
            return null;
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize) void {
            unreachable;
        }
    };
    var recorder: Recorder = .{};
    const allocator: std.mem.Allocator = .{ .ptr = &recorder, .vtable = &.{ .alloc = Recorder.alloc, .resize = Recorder.resize, .remap = Recorder.remap, .free = Recorder.free } };
    var list: std.ArrayListUnmanaged(*Entry) = .empty;
    list.ensureTotalCapacity(allocator, needed) catch {
        if (recorder.bytes == 0 or recorder.bytes % @sizeOf(*Entry) != 0) return error.InvalidTrunkListLayout;
        return recorder.bytes / @sizeOf(*Entry);
    };
    return error.InvalidTrunkListLayout;
}

pub const ListKind = enum { entries, pending };

pub fn coldListGeometry(snapshot_value: *const ColdSnapshot, comptime which: ListKind, extra: usize) !ListGeometry {
    const list = @field(snapshot_value, @tagName(which));
    const owner = @field(snapshot_value, @tagName(which) ++ "_owner");
    if (owner) |value| {
        const admission = snapshot_value.admission orelse return error.UnmanagedTrunkCacheMetadata;
        const reservation = value.backing.charge.storageReservation() orelse return error.UnmanagedTrunkCacheMetadata;
        if (reservation.identity.controller != admission.controller or reservation.identity.backend_class != admission.backend_class)
            return error.ForeignAdmissionDomain;
    }
    const needed = try std.math.add(usize, list.items.len, extra);
    if (owner != null and needed <= list.capacity) return .{ .capacity = list.capacity, .bytes = 0, .replace = false };
    const replace = needed > list.capacity;
    const capacity = if (replace) try pointerListCapacity(needed) else list.capacity;
    return .{ .capacity = capacity, .bytes = if (capacity == 0) 0 else try std.math.add(usize, @sizeOf(ListOwner), try std.math.mul(usize, capacity, @sizeOf(*Entry))), .replace = replace };
}

/// Only unpublished cold control owns the borrowed construction epoch. Once
/// published the ListOwner holds no capability/Source/Epoch/Model pointer.
pub const PreparedList = struct {
    cache: ?*Cache = null,
    revision: u64 = 0,
    which: enum { entries, pending } = .entries,
    owner: ?*ListOwner = null,
    binding: ?storage.BackingReservation = null,
    replace: bool = false,
    committed: bool = false,
    pub fn abortOutsideExecution(self: *@This()) void {
        const owner = self.owner orelse return;
        std.debug.assert(!self.committed);
        if (self.replace) owner.list.deinit(owner.allocator);
        owner.allocator.destroy(owner);
        self.owner = null;
        if (self.binding) |binding| binding.owner.release(binding.construction_epoch);
        self.binding = null;
    }
};

pub const FillGeometry = struct { elements: usize, payload_bytes: usize, metadata_bytes: usize };
pub fn coldFillGeometry(tokens: usize, layers: usize, hidden: usize, precision: Precision) !FillGeometry {
    if (tokens == 0 or layers == 0 or hidden == 0) return error.InvalidTrunkCacheGeometry;
    const elements = try std.math.mul(usize, try std.math.mul(usize, try std.math.mul(usize, 2, layers), tokens), hidden);
    return .{
        .elements = elements,
        .payload_bytes = try std.math.mul(usize, elements, if (precision == .f16) @as(usize, 2) else 4),
        .metadata_bytes = try std.math.add(usize, @sizeOf(Entry), try retirement.PreparedCharge.requestedMetadataBytes(0)),
    };
}

pub const Cache = struct {
    allocator: std.mem.Allocator,
    // Test-only explicit owner: the fixture keeps its failing allocator alive
    // through every real B callback. Production has no provenance override.
    test_cold_allocator: if (@import("builtin").is_test) bool else void = if (@import("builtin").is_test) false else {},
    limit_bytes: usize,
    min_tokens: usize = default_min_tokens,
    precision: Precision = .f16,
    admission: ?Admission = null,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    /// Reserved entries being filled; their bytes count in `reserved_bytes`.
    pending: std.ArrayListUnmanaged(*Entry) = .empty,
    entries_owner: ?*ListOwner = null,
    pending_owner: ?*ListOwner = null,
    retired_entries: ?*Entry = null,
    retired_lists: ?*ListOwner = null,
    // One preowned move slot for EACH actual raw container. No allocation,
    // borrowed entry destruction or supplier is needed under pressure gates.
    retired_raw_entries: std.ArrayListUnmanaged(*Entry) = .empty,
    retired_raw_pending: std.ArrayListUnmanaged(*Entry) = .empty,
    // A single real receiving Model owns this short numerical scope. Cold
    // peers may observe busy, but never publish/grow metadata into its call.
    active_receiver: bool = false,
    // Local actual cache mutation identity, never a global Model generation.
    // Saturation stops new cold certificates; raw compatibility can continue.
    revision: u64 = 0,
    reserved_bytes: usize = 0,
    clock: u64 = 0,
    stats: Stats = .{},

    pub fn init(allocator: std.mem.Allocator, limit_bytes: usize) Cache {
        return .{ .allocator = allocator, .limit_bytes = limit_bytes };
    }

    pub fn fromEnvironment(allocator: std.mem.Allocator) Cache {
        const mb = platform.env.getenvUsize("ANTFLY_LAYA_TRUNK_CACHE_MB") orelse default_limit_mb;
        var cache = init(allocator, std.math.mul(usize, mb, 1024 * 1024) catch std.math.maxInt(usize));
        if (platform.env.getenv("ANTFLY_LAYA_TRUNK_CACHE_DTYPE")) |dtype| {
            if (std.mem.eql(u8, dtype, "f32")) cache.precision = .f32;
        }
        return cache;
    }

    pub fn deinit(self: *Cache) void {
        self.deinitWithCleanup(.{ .outside_execution = true }) catch
            @import("antfly_platform").inference_process_supervisor.restartWorker();
    }

    /// Actual caller has ended Model/lane. A typed close validates every
    /// pending/borrowed entry before detaching any physical owner. Registered
    /// callbacks never retain this Cache or a device/provider closure.
    pub fn deinitWithCleanup(self: *Cache, current: Cleanup) !void {
        try validateBoundary(current);
        platform.sync.lockYielding(&self.mutex);
        if (self.active_receiver or self.pending.items.len != 0) {
            self.mutex.unlock();
            return error.TrunkCacheFillStillPending;
        }
        for (self.entries.items) |entry| {
            if (entry.pins != 0) {
                self.mutex.unlock();
                return error.TrunkCacheEntryStillBorrowed;
            }
            if ((entry.metadata_backing != null or entry.quota != null) and
                (entry.device != null or entry.device_deinit != null or entry.allocation == null))
            {
                self.mutex.unlock();
                return error.UnsupportedTypedTrunkBacking;
            }
        }
        var entries = self.entries;
        var pending = self.pending;
        self.entries = .empty;
        self.pending = .empty;
        const entries_paid = self.entries_owner != null;
        const pending_paid = self.pending_owner != null;
        if (self.entries_owner) |owner| {
            owner.retired_next = self.retired_lists;
            self.retired_lists = owner;
            self.entries_owner = null;
        }
        if (self.pending_owner) |owner| {
            owner.retired_next = self.retired_lists;
            self.retired_lists = owner;
            self.pending_owner = null;
        }
        // Owners retain their exact array metadata until the outside drain;
        // raw arrays are owned directly by these local by-value lists.
        self.stats.bytes = 0;
        self.stats.entries = 0;
        self.revision +|= 1;
        self.mutex.unlock();
        for (entries.items) |entry| self.destroy(entry);
        if (!entries_paid) entries.deinit(self.allocator);
        if (!pending_paid) pending.deinit(self.allocator);
        try self.drainRetiredOutsideExecution(current);
    }
    /// Metadata-only, without hit/miss counters, LRU mutation, allocation or
    /// supplier callbacks. Caller may hold the selected Model only for this
    /// short snapshot, then MUST release its actual FIFO turn before waiting
    /// on pending_same_key or funding a replacement.
    pub fn snapshotCold(self: *Cache, cache_key: [32]u8) !ColdSnapshot {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.active_receiver) return error.TrunkCacheBusyBeforeExecution;
        if (self.revision >= std.math.maxInt(u64) - 4) return error.TrunkCacheMetadataIdentityExhausted;
        var hit: ?*Entry = null;
        for (self.entries.items) |entry| if (std.mem.eql(u8, &entry.key, &cache_key)) {
            if (entry.pins == std.math.maxInt(usize)) return error.TrunkCachePinOverflow;
            hit = entry;
            break;
        };
        var pending_same_key = false;
        for (self.pending.items) |entry| if (!entry.orphaned and std.mem.eql(u8, &entry.key, &cache_key)) {
            pending_same_key = true;
            break;
        };
        // These are creator-backed refs; no final release or callback occurs
        // inside this mutex on any successful snapshot.
        if (hit) |entry| entry.pins += 1;
        return .{
            .revision = self.revision,
            .limit_bytes = self.limit_bytes,
            .min_tokens = self.min_tokens,
            .precision = self.precision,
            .admission = self.admission,
            .entries = self.entries,
            .pending = self.pending,
            .entries_owner = if (self.entries_owner) |owner| owner.retain() else null,
            .pending_owner = if (self.pending_owner) |owner| owner.retain() else null,
            .hit = hit,
            .pending_same_key = pending_same_key,
        };
    }
    pub const ColdKey = struct { entry: ?*Entry, pending: bool, metadata_owned: bool };
    /// Additional selected keys in the SAME pinned metadata window. Ownership
    /// of each successful hit pin moves to the cold caller; never store only a
    /// header address as a lifetime witness.
    pub fn inspectColdKey(self: *Cache, snapshot_value: *const ColdSnapshot, cache_key: [32]u8) !ColdKey {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.active_receiver or !self.matchesColdLocked(snapshot_value)) return error.TrunkCacheChangedBeforeExecution;
        for (self.entries.items) |entry| if (std.mem.eql(u8, &entry.key, &cache_key)) {
            if (entry.pins == std.math.maxInt(usize)) return error.TrunkCachePinOverflow;
            const owned = entry.metadata_backing != null and entry.quota != null;
            if (owned) {
                const admission = self.admission orelse return error.UnmanagedTrunkCacheMetadata;
                if (!entryOwnedBy(entry, admission)) return error.ForeignAdmissionDomain;
            }
            entry.pins += 1;
            return .{ .entry = entry, .pending = false, .metadata_owned = owned };
        };
        for (self.pending.items) |entry| if (!entry.orphaned and std.mem.eql(u8, &entry.key, &cache_key))
            return .{ .entry = null, .pending = true, .metadata_owned = false };
        return .{ .entry = null, .pending = false, .metadata_owned = false };
    }

    fn matchesColdLocked(self: *const Cache, snapshot_value: *const ColdSnapshot) bool {
        if (snapshot_value.finished or snapshot_value.revision != self.revision or
            self.revision >= std.math.maxInt(u64) - 1 or
            snapshot_value.limit_bytes != self.limit_bytes or snapshot_value.min_tokens != self.min_tokens or
            snapshot_value.precision != self.precision or !std.meta.eql(snapshot_value.admission, self.admission)) return false;
        inline for (.{ "entries", "pending" }) |name| {
            const before = @field(snapshot_value, name);
            const actual = @field(self, name);
            if (before.capacity != actual.capacity or before.items.ptr != actual.items.ptr or before.items.len != actual.items.len or
                @field(snapshot_value, name ++ "_owner") != @field(self, name ++ "_owner")) return false;
        }
        if (snapshot_value.hit) |hit| {
            if (hit.pins == 0 or std.mem.indexOfScalar(*Entry, self.entries.items, hit) == null) return false;
        }
        return true;
    }
    pub fn validateCold(self: *Cache, snapshot_value: *const ColdSnapshot) !void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.active_receiver or !self.matchesColdLocked(snapshot_value)) return error.TrunkCacheChangedBeforeExecution;
    }

    /// Model-owned numerical scope, entered only after the complete cold
    /// census and before any math. Cold peers wait outside Model/FIFO; this
    /// marker neither waits nor supplies numerical authority by itself.
    pub fn beginPreparedCall(self: *Cache, snapshot_value: *const ColdSnapshot) !void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.active_receiver or !self.matchesColdLocked(snapshot_value)) return error.TrunkCacheChangedBeforeExecution;
        self.active_receiver = true;
    }
    pub fn endPreparedCall(self: *Cache) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(self.active_receiver);
        self.active_receiver = false;
    }

    /// Copied SAME-cache owned KV for intrinsic whole-request validation only.
    /// It is NOT fresh storage credit and MUST NOT enter supplementPairDeficit
    /// or another acquisition. Peer-only quota remains ordinary contention.
    pub fn ownedAmountsCold(self: *Cache, snapshot_value: *const ColdSnapshot, controller: *memory.AdmissionController) !memory.AdmissionAmounts {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.active_receiver or !self.matchesColdLocked(snapshot_value)) return error.TrunkCacheChangedBeforeExecution;
        var amounts: memory.AdmissionAmounts = .{};
        inline for (.{ "entries", "pending" }) |name| for (@field(self, name).items) |entry| {
            const lease = entry.lease orelse if (entry.quota) |quota| switch (quota.quota) {
                .run => |held| held,
                else => return error.UnmanagedTrunkCacheMetadata,
            } else return error.UnmanagedTrunkCacheMetadata;
            if (lease.controller != controller) return error.ForeignAdmissionDomain;
            amounts = try amounts.merge(lease.amounts);
            if (entry.metadata_backing) |backing| {
                const physical = backing.charge.storageReservation() orelse return error.InvalidPhysicalStorageReservation;
                if (physical.identity.controller != controller or physical.identity.backend_class != lease.retain_backend_class) return error.ForeignAdmissionDomain;
                amounts.host_scratch_bytes = try std.math.add(usize, amounts.host_scratch_bytes, backing.admittedBytes());
            }
        };
        inline for (.{ self.entries_owner, self.pending_owner }) |maybe_owner| if (maybe_owner) |owner| {
            const physical = owner.backing.charge.storageReservation() orelse return error.InvalidPhysicalStorageReservation;
            const admission = self.admission orelse return error.UnmanagedTrunkCacheMetadata;
            if (physical.identity.controller != controller or physical.identity.backend_class != admission.backend_class) return error.ForeignAdmissionDomain;
            amounts.host_scratch_bytes = try std.math.add(usize, amounts.host_scratch_bytes, owner.backing.admittedBytes());
        };
        return amounts;
    }

    pub fn validateColdRevisionHeadroom(self: *Cache, snapshot_value: *const ColdSnapshot, steps: usize) !void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.active_receiver or !self.matchesColdLocked(snapshot_value)) return error.TrunkCacheChangedBeforeExecution;
        const count = std.math.cast(u64, steps) orelse return error.TrunkCacheMetadataIdentityExhausted;
        if (count >= std.math.maxInt(u64) - self.revision) return error.TrunkCacheMetadataIdentityExhausted;
    }

    /// Existing host-cache pressure selection holds the real Model exclusion.
    /// Detach only an unpinned original LRU victim; no callback/allocator runs.
    /// Outside settlement keeps the actual retained Model alive until raw
    /// bytes die; typed late owners independently retain their old Domain.
    pub fn reclaimOneDeferringPhysical(self: *Cache) usize {
        if (!self.supportsColdOwnership()) return 0; // no invented allocator ancestry
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.active_receiver) return 0;
        var victim: ?usize = null;
        for (self.entries.items, 0..) |entry, index| {
            if (entry.pins != 0) continue;
            if (victim == null or entry.last_used < self.entries.items[victim.?].last_used) victim = index;
        }
        const index = victim orelse return 0;
        const entry = self.entries.swapRemove(index);
        const bytes = entry.bytes();
        self.stats.bytes -= bytes;
        self.stats.evictions += 1;
        self.stats.entries = self.entries.items.len;
        self.revision +|= 1;
        entry.retired_next = self.retired_entries;
        self.retired_entries = entry;
        self.trimEmptyContainersLocked();
        return bytes;
    }
    fn trimEmptyContainersLocked(self: *Cache) void {
        inline for (.{ "entries", "pending" }) |name| {
            trim: {
                const list = &@field(self, name);
                if (list.items.len != 0 or list.capacity == 0) break :trim;
                const owner_slot = &@field(self, name ++ "_owner");
                if (owner_slot.*) |owner| {
                    owner.retired_next = self.retired_lists;
                    self.retired_lists = owner;
                    owner_slot.* = null;
                } else {
                    const raw_slot = &@field(self, "retired_raw_" ++ name);
                    if (raw_slot.capacity != 0) break :trim;
                    raw_slot.* = list.*;
                }
                list.* = .empty;
                self.revision +|= 1;
            }
        }
    }
    pub fn retirementPending(self: *Cache) bool {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return self.retired_entries != null or self.retired_lists != null or self.retired_raw_entries.capacity != 0 or self.retired_raw_pending.capacity != 0;
    }

    pub fn supportsColdOwnership(self: *const Cache) bool {
        if (comptime @import("builtin").is_test) if (self.test_cold_allocator) return true;
        const program = @import("../execution/program.zig");
        if (program.processMetadataAllocator(self.allocator)) return true;
        return @import("builtin").is_test and program.knownSameMetadataAllocator(self.allocator, std.testing.allocator);
    }
    /// Must follow exact supplemental admission and run outside Model/lane.
    /// The actual existing array remains borrowed until commit; no unpublished
    /// owner may free it on OOM/cancel. Replacement arrays are independently
    /// allocated, and old paid arrays retain their own independent quota.
    pub fn prepareListOutsideExecution(self: *Cache, snapshot_value: *const ColdSnapshot, comptime which: ListKind, extra: usize, capability: storage.Capability) !PreparedList {
        if (!self.supportsColdOwnership()) return error.UnsupportedTrunkCacheMetadataAllocator;
        const admission = snapshot_value.admission orelse return error.UnmanagedTrunkCacheMetadata;
        if (admission.device) return error.UnsupportedTypedTrunkBacking;
        if (capability.identity.controller != admission.controller or capability.identity.backend_class != admission.backend_class)
            return error.ForeignAdmissionDomain;
        const geometry = try coldListGeometry(snapshot_value, which, extra);
        if (geometry.bytes == 0) return .{};
        const binding = try capability.reserveBacking(self.allocator, .native, .host_workspace, geometry.bytes) orelse return error.UnpreparedPhysicalStorageReservation;
        errdefer binding.owner.release(binding.construction_epoch);
        try validateMetadataBinding(binding, capability, geometry.bytes);
        const owner = try self.allocator.create(ListOwner);
        errdefer self.allocator.destroy(owner);
        var list: std.ArrayListUnmanaged(*Entry) = if (geometry.replace) .empty else @field(snapshot_value, @tagName(which));
        if (geometry.replace) {
            try list.ensureTotalCapacity(self.allocator, try std.math.add(usize, @field(snapshot_value, @tagName(which)).items.len, extra));
            if (list.capacity != geometry.capacity) {
                list.deinit(self.allocator);
                return error.TrunkCacheChangedBeforeExecution;
            }
        }
        owner.* = .{ .allocator = self.allocator, .list = list, .backing = binding.owner };
        return .{ .cache = self, .revision = snapshot_value.revision, .which = @fromBackingInt(@intCast(@backingInt(which))), .owner = owner, .binding = binding, .replace = geometry.replace };
    }
    /// Outside-only atomic metadata publication. Both allocations/funding and
    /// every fallible validation precede the first swap. No Entry/key/lease is
    /// changed, no cache hit is fabricated, and old storage dies after unlock.
    pub fn commitColdListsOutsideExecution(self: *Cache, snapshot_value: *ColdSnapshot, entries_plan: *PreparedList, pending_plan: *PreparedList, current: Cleanup) !void {
        try validateBoundary(current);
        var raw_old: [2]std.ArrayListUnmanaged(*Entry) = @splat(.empty);
        var old_pins: [2]?*ListOwner = @splat(null);
        platform.sync.lockYielding(&self.mutex);
        if (self.active_receiver or !self.matchesColdLocked(snapshot_value)) {
            self.mutex.unlock();
            return error.TrunkCacheChangedBeforeExecution;
        }
        inline for (.{ "entries", "pending" }, 0..) |_, index| {
            const plan = if (index == 0) entries_plan else pending_plan;
            if (plan.owner) |owner| {
                if (plan.committed or plan.cache != self or plan.revision != snapshot_value.revision or
                    @backingInt(plan.which) != index or plan.binding == null or plan.binding.?.owner != owner.backing)
                {
                    self.mutex.unlock();
                    return error.InvalidTrunkCacheMetadataPlan;
                }
                const list = if (index == 0) self.entries else self.pending;
                if (plan.replace and owner.list.capacity < list.items.len) {
                    self.mutex.unlock();
                    return error.TrunkCacheChangedBeforeExecution;
                }
            }
        }
        inline for (.{ "entries", "pending" }, 0..) |name, index| {
            const plan = if (index == 0) entries_plan else pending_plan;
            if (plan.owner) |owner| {
                const list = &@field(self, name);
                const owner_slot = &@field(self, name ++ "_owner");
                if (plan.replace) {
                    owner.list.appendSliceAssumeCapacity(list.items);
                    if (owner_slot.* == null) raw_old[index] = list.*;
                }
                if (owner_slot.*) |old_owner| {
                    old_owner.retired_next = self.retired_lists;
                    self.retired_lists = old_owner;
                }
                list.* = owner.list;
                owner_slot.* = owner;
                // Replace the snapshot's actual creator-backed pin before
                // dropping its old pin outside this metadata mutex.
                old_pins[index] = @field(snapshot_value, name ++ "_owner");
                @field(snapshot_value, name ++ "_owner") = owner.retain();
                @field(snapshot_value, name) = list.*;
                plan.owner = null;
                plan.binding = null;
                plan.committed = true;
                self.revision +|= 1;
            }
        }
        snapshot_value.revision = self.revision;
        self.mutex.unlock();
        for (&raw_old) |*list| list.deinit(self.allocator);
        for (old_pins) |maybe_owner| if (maybe_owner) |owner| try owner.releaseOutside(current);
        try self.drainRetiredOutsideExecution(current);
    }

    fn prepareEntryChargeOutsideExecution(self: *Cache) !retirement.PreparedCharge {
        if (comptime @import("builtin").is_test) if (self.test_cold_allocator)
            return retirement.PreparedCharge.prepareWithAllocatorForTest(self.allocator, 0);
        return retirement.PreparedCharge.prepare(0);
    }

    /// Metadata-only upgrade of the SAME pinned raw hit. The original KV
    /// lease is transferred exactly once; there is no zero-byte acquisition,
    /// payload allocation, retag, eviction or precision/layout change.
    pub fn prepareHitOwnershipOutsideExecution(self: *Cache, snapshot_value: *const ColdSnapshot, capability: storage.Capability, old_domain: DomainPin) !void {
        if (!self.supportsColdOwnership()) return error.UnsupportedTrunkCacheMetadataAllocator;
        const entry = snapshot_value.hit orelse return error.MissingTrunkCacheHit;
        const admission = snapshot_value.admission orelse return error.UnmanagedTrunkCacheMetadata;
        if (admission.device) return error.UnsupportedTypedTrunkBacking;
        if (old_domain.controller() != admission.controller or capability.identity.controller != admission.controller or
            capability.identity.backend_class != admission.backend_class) return error.ForeignAdmissionDomain;
        platform.sync.lockYielding(&self.mutex);
        if (self.active_receiver or !self.matchesColdLocked(snapshot_value)) {
            self.mutex.unlock();
            return error.TrunkCacheChangedBeforeExecution;
        }
        if (!validHostPayload(entry)) {
            self.mutex.unlock();
            return error.InvalidTrunkCacheBacking;
        }
        const already_owned = entry.metadata_backing != null and entry.quota != null;
        const foreign_owner = already_owned and !entryOwnedBy(entry, admission);
        if (!already_owned) {
            const raw_lease = entry.lease orelse {
                self.mutex.unlock();
                return error.UnmanagedTrunkCacheMetadata;
            };
            if (raw_lease.controller != admission.controller or raw_lease.retain_backend_class != admission.backend_class) {
                self.mutex.unlock();
                return error.ForeignAdmissionDomain;
            }
            if (!validHostKvLease(raw_lease, admission, entry.bytes())) {
                self.mutex.unlock();
                return error.InvalidTrunkCacheBacking;
            }
        }
        self.mutex.unlock();
        if (foreign_owner) return error.ForeignAdmissionDomain;
        if (already_owned) return;
        const geometry = try coldFillGeometry(entry.tokens, entry.layers, entry.hidden, entry.precision);
        const binding = try capability.reserveBacking(self.allocator, .native, .host_workspace, geometry.metadata_bytes) orelse return error.UnpreparedPhysicalStorageReservation;
        var metadata_live = true;
        defer if (metadata_live) binding.owner.release(binding.construction_epoch);
        try validateMetadataBinding(binding, capability, geometry.metadata_bytes);
        var shell = try self.prepareEntryChargeOutsideExecution();
        defer shell.destroyUnused();
        var domain: ?DomainPin = old_domain.retain();
        defer if (domain) |pin| pin.release();
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.active_receiver or !self.matchesColdLocked(snapshot_value)) return error.TrunkCacheChangedBeforeExecution;
        if (entry.metadata_backing != null and entry.quota != null) {
            if (!entryOwnedBy(entry, admission)) return error.ForeignAdmissionDomain;
            return;
        }
        if (entry.metadata_backing != null or entry.quota != null or entry.device != null or entry.device_deinit != null or entry.lease == null or
            !validHostKvLease(entry.lease.?, admission, entry.bytes())) return error.UnmanagedTrunkCacheMetadata;
        // No fallible work or callback follows this ownership move.
        entry.quota = shell.arm(&entry.lease, &domain) catch unreachable;
        entry.metadata_backing = binding.owner;
        entry.allocation = self.allocator;
        metadata_live = false;
    }

    /// This caller has no Model/lane/FIFO turn. Exact Entry+pure Charge
    /// metadata was separately declared; the original KV reservation keeps its
    /// original class/cache cap/controller, without borrowing any run floor.
    /// Null is the original optional cache refusal. The returned SAME Entry is
    /// still an unsubmitted pending fill, never a fabricated cache hit.
    pub fn prepareFillOutsideExecution(self: *Cache, snapshot_value: *ColdSnapshot, cache_key: [32]u8, tokens: usize, layers: usize, hidden: usize, capability: storage.Capability, old_domain: DomainPin) !?*Entry {
        if (!self.supportsColdOwnership()) return error.UnsupportedTrunkCacheMetadataAllocator;
        const admission = snapshot_value.admission orelse return error.UnmanagedTrunkCacheMetadata;
        if (admission.device) return error.UnsupportedTypedTrunkBacking;
        if (old_domain.controller() != admission.controller or capability.identity.controller != admission.controller or
            capability.identity.backend_class != admission.backend_class) return error.ForeignAdmissionDomain;
        const geometry = try coldFillGeometry(tokens, layers, hidden, snapshot_value.precision);
        const binding = try capability.reserveBacking(self.allocator, .native, .host_workspace, geometry.metadata_bytes) orelse return error.UnpreparedPhysicalStorageReservation;
        var metadata_live = true;
        defer if (metadata_live) binding.owner.release(binding.construction_epoch);
        try validateMetadataBinding(binding, capability, geometry.metadata_bytes);
        var shell = try self.prepareEntryChargeOutsideExecution();
        defer shell.destroyUnused();
        var domain: ?DomainPin = old_domain.retain();
        defer if (domain) |pin| pin.release();
        platform.sync.lockYielding(&self.mutex);
        if (self.active_receiver or !self.matchesColdLocked(snapshot_value) or snapshot_value.hit != null or snapshot_value.pending_same_key or
            self.pending.capacity == self.pending.items.len or self.pending_owner == null or
            self.entries_owner == null or self.entries.capacity == self.entries.items.len)
        {
            self.mutex.unlock();
            return error.TrunkCacheChangedBeforeExecution;
        }
        const reserved = self.reserveWithSnapshotLocked(geometry.payload_bytes, snapshot_value) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.mutex.unlock();
        const claim = reserved orelse return null;
        var lease = claim.lease;
        var claim_live = true;
        defer if (claim_live) {
            platform.sync.lockYielding(&self.mutex);
            self.reserved_bytes -= claim.bytes;
            self.mutex.unlock();
            if (lease) |*value| value.release();
        };
        if (lease == null or !validHostKvLease(lease.?, admission, geometry.payload_bytes)) return error.UnmanagedTrunkCacheMetadata;
        const entry = try self.allocator.create(Entry);
        var entry_live = true;
        defer if (entry_live) self.allocator.destroy(entry);
        entry.* = .{ .key = cache_key, .tokens = tokens, .hidden = hidden, .layers = layers, .precision = snapshot_value.precision, .pins = 1, .allocation = self.allocator, .unsubmitted = true };
        switch (entry.precision) {
            .f16 => entry.host16 = try self.allocator.alloc(f16, geometry.elements),
            .f32 => entry.host32 = try self.allocator.alloc(f32, geometry.elements),
        }
        var payload_live = true;
        defer if (payload_live) {
            self.allocator.free(entry.host16);
            self.allocator.free(entry.host32);
        };
        platform.sync.lockYielding(&self.mutex);
        if (self.active_receiver or !self.matchesColdLocked(snapshot_value) or self.pending.capacity == self.pending.items.len) {
            self.mutex.unlock();
            return error.TrunkCacheChangedBeforeExecution;
        }
        // Every remaining operation is infallible. Arm checks were performed
        // against the actual lease/domain above, before any publication.
        entry.quota = shell.arm(&lease, &domain) catch unreachable;
        entry.metadata_backing = binding.owner;
        self.pending.appendAssumeCapacity(entry);
        self.revision +|= 1;
        snapshot_value.revision = self.revision;
        snapshot_value.pending = self.pending;
        self.mutex.unlock();
        claim_live = false;
        metadata_live = false;
        payload_live = false;
        entry_live = false;
        return entry;
    }

    pub fn validateColdEntry(self: *Cache, snapshot_value: *const ColdSnapshot, entry: *Entry, cache_key: [32]u8, tokens: usize, layers: usize, hidden: usize) !void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.active_receiver or !self.matchesColdLocked(snapshot_value) or entry.pins == 0 or
            entry.tokens != tokens or entry.layers != layers or entry.hidden != hidden or !std.mem.eql(u8, &entry.key, &cache_key))
            return error.TrunkCacheChangedBeforeExecution;
        const admission = self.admission orelse return error.UnmanagedTrunkCacheMetadata;
        if (!entryOwnedBy(entry, admission)) return error.ForeignAdmissionDomain;
        if (std.mem.indexOfScalar(*Entry, self.entries.items, entry) != null) return;
        if (!entry.unsubmitted or entry.orphaned or std.mem.indexOfScalar(*Entry, self.pending.items, entry) == null)
            return error.TrunkCacheChangedBeforeExecution;
    }

    /// Actual receiving Model is held. No allocation/callback and no yield.
    /// Caller validated SAME seven inputs/checkpoint/B before this cache seam.
    /// The cold Source pin survives all numerical/error paths; this extra pin
    /// is the original lookup loan consumed by fillTrunk/release/abandon.
    pub fn consumePrepared(self: *Cache, snapshot_value: *const ColdSnapshot, entry: *Entry, current: Cleanup) !Lookup {
        try validateBoundary(current);
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (!self.active_receiver or !self.matchesColdLocked(snapshot_value) or entry.pins == 0 or entry.pins == std.math.maxInt(usize))
            return error.TrunkCacheChangedBeforeExecution;
        if (std.mem.indexOfScalar(*Entry, self.entries.items, entry) != null) {
            const admission = self.admission orelse return error.UnmanagedTrunkCacheMetadata;
            if (!entryOwnedBy(entry, admission)) return error.ForeignAdmissionDomain;
            entry.pins += 1;
            self.clock += 1;
            entry.last_used = self.clock;
            self.stats.hits += 1;
            return .{ .hit = entry };
        }
        if (!entry.unsubmitted or entry.orphaned or entry.metadata_backing == null or entry.quota == null or
            std.mem.indexOfScalar(*Entry, self.pending.items, entry) == null or
            self.entries_owner == null or self.entries.items.len == self.entries.capacity)
            return error.TrunkCacheChangedBeforeExecution;
        entry.unsubmitted = false;
        entry.pins += 1;
        self.stats.misses += 1;
        return .{ .fill = entry };
    }
    pub fn consumePreparedBypass(self: *Cache, snapshot_value: *const ColdSnapshot, current: Cleanup) !Lookup {
        try validateBoundary(current);
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (!self.active_receiver or !self.matchesColdLocked(snapshot_value)) return error.TrunkCacheChangedDuringExecution;
        self.stats.misses += 1;
        self.stats.refusals += 1;
        return .bypass;
    }

    /// SAME publication as publish, using the already paid capacity. This
    /// updates only the calling plan's snapshot for its own exact mutation;
    /// a peer mutation cannot be mistaken for local progress after math.
    pub fn publishPrepared(self: *Cache, entry: *Entry, snapshot_value: *ColdSnapshot) !void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (!self.active_receiver or !self.matchesColdLocked(snapshot_value) or entry.unsubmitted or entry.orphaned or
            std.mem.indexOfScalar(*Entry, self.pending.items, entry) == null)
            return error.TrunkCacheChangedDuringExecution;
        if (self.entries_owner == null or self.entries.items.len == self.entries.capacity) {
            entry.orphaned = true; // original one-call/no-publication policy
            return;
        }
        self.removePendingLocked(entry);
        self.entries.appendAssumeCapacity(entry);
        self.clock += 1;
        entry.last_used = self.clock;
        self.stats.bytes += entry.bytes();
        self.stats.entries = self.entries.items.len;
        self.revision +|= 1;
        snapshot_value.revision = self.revision;
        snapshot_value.entries = self.entries;
        snapshot_value.pending = self.pending;
    }

    pub fn finishPreparedEntryOutsideExecution(self: *Cache, entry: *Entry, current: Cleanup) !void {
        try validateBoundary(current);
        platform.sync.lockYielding(&self.mutex);
        if (entry.unsubmitted) entry.orphaned = true;
        self.mutex.unlock();
        self.release(entry);
        try self.drainRetiredOutsideExecution(current);
    }

    /// Precision of entries created from now on; entries already cached or
    /// being filled keep theirs.
    pub fn setPrecision(self: *Cache, precision: Precision) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.precision = precision;
        self.revision +|= 1;
    }

    pub fn setLimit(self: *Cache, limit_bytes: usize, min_tokens: usize) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.limit_bytes = limit_bytes;
        self.min_tokens = min_tokens;
        self.revision +|= 1;
    }

    /// Charge future entries to the session's admission controller.
    pub fn configureAdmission(self: *Cache, admission: Admission) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.admission = admission;
        self.revision +|= 1;
    }

    fn destroy(self: *Cache, entry: *Entry) void {
        if (entry.metadata_backing != null or entry.quota != null) {
            platform.sync.lockYielding(&self.mutex);
            entry.retired_next = self.retired_entries;
            self.retired_entries = entry;
            self.mutex.unlock();
            return;
        }
        self.destroyEntryOutside(entry, .{ .outside_execution = true }) catch unreachable;
    }
    fn destroyEntryOutside(self: *Cache, entry: *Entry, current: Cleanup) !void {
        try validateBoundary(current); // before ANY payload or ref mutation
        if (entry.metadata_backing != null or entry.quota != null) {
            // Typed metadata is prepared only for host entries. Late callbacks
            // own pure CPU storage/Charge/Backing, never a provider closure.
            if (entry.device != null or entry.device_deinit != null or entry.allocation == null)
                return error.UnsupportedTypedTrunkBacking;
            if (current.boundary) |epoch| {
                entry.retirement_node = .{ .context = entry, .release_fn = destroyRetiredEntry };
                // The SAME actual B now owns physical storage as well as quota.
                // No saved epoch, auxiliary allocation or late Model is needed.
                const boundary = try epoch.boundary();
                try boundary.register(boundary.context, boundary.nonce, &entry.retirement_node);
                return;
            }
        }
        destroyPhysicalEntry(entry, entry.allocation orelse self.allocator);
    }
    fn destroyRetiredEntry(raw: *anyopaque) callconv(.c) void {
        const entry: *Entry = @ptrCast(@alignCast(raw));
        destroyPhysicalEntry(entry, entry.allocation.?);
    }
    fn destroyPhysicalEntry(entry: *Entry, allocator: std.mem.Allocator) void {
        var lease = entry.lease;
        const quota = entry.quota;
        const metadata = entry.metadata_backing;
        if (entry.device_deinit) |free_device| free_device(entry.device, allocator);
        allocator.free(entry.host16);
        allocator.free(entry.host32);
        allocator.destroy(entry);
        // Actual payload AND Entry header die before either class is refunded.
        // Typed B callback is outside all gates and no longer needs an Epoch.
        if (lease) |*owned| owned.release();
        if (quota) |held| held.release();
        if (metadata) |backing| backing.releaseOutsideExecution();
    }
    /// Actual caller proves outside Model/lane; raw callers do not gain that
    /// authority merely by omitting B. No Cache/Source/allocator context enters
    /// a delayed quota callback: only existing Charge/Backing owners remain.
    pub fn drainRetiredOutsideExecution(self: *Cache, current: Cleanup) !void {
        try validateBoundary(current);
        while (true) {
            platform.sync.lockYielding(&self.mutex);
            const entry = self.retired_entries;
            if (entry) |value| {
                self.retired_entries = value.retired_next;
                value.retired_next = null;
            }
            self.mutex.unlock();
            if (entry) |value| {
                self.destroyEntryOutside(value, current) catch |err| {
                    platform.sync.lockYielding(&self.mutex);
                    value.retired_next = self.retired_entries;
                    self.retired_entries = value;
                    self.mutex.unlock();
                    return err;
                };
            } else break;
        }
        while (true) {
            platform.sync.lockYielding(&self.mutex);
            const owner = self.retired_lists;
            if (owner) |value| {
                self.retired_lists = value.retired_next;
                value.retired_next = null;
            }
            self.mutex.unlock();
            if (owner) |value| {
                value.releaseOutside(current) catch |err| {
                    platform.sync.lockYielding(&self.mutex);
                    value.retired_next = self.retired_lists;
                    self.retired_lists = value;
                    self.mutex.unlock();
                    return err;
                };
            } else break;
        }
        platform.sync.lockYielding(&self.mutex);
        var raw_entries = self.retired_raw_entries;
        var raw_pending = self.retired_raw_pending;
        self.retired_raw_entries = .empty;
        self.retired_raw_pending = .empty;
        self.mutex.unlock();
        raw_entries.deinit(self.allocator);
        raw_pending.deinit(self.allocator);
    }

    pub fn key(ids: []const i64, layers: usize, hidden: usize) [32]u8 {
        return keyAt(ids, layers, hidden, 0);
    }

    /// The key of a trunk encoded at logical positions `first_position..`
    /// (`packing.question_first` moves it); its keys carry those RoPE angles.
    pub fn keyAt(ids: []const i64, layers: usize, hidden: usize, first_position: usize) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-laya-trunk/v2");
        hash.update(std.mem.asBytes(&layers));
        hash.update(std.mem.asBytes(&hidden));
        hash.update(std.mem.asBytes(&first_position));
        hash.update(std.mem.sliceAsBytes(ids));
        return hash.finalResult();
    }

    /// Pin and return a cached trunk, or null on a miss.
    pub fn acquire(self: *Cache, k: [32]u8) ?*Entry {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.hitLocked(k)) |entry| return entry;
        self.stats.misses += 1;
        return null;
    }

    /// Whether `k` is cached, without pinning it or counting a hit. The
    /// answer can go stale at once; it only guides scheduling.
    pub fn contains(self: *Cache, k: [32]u8) bool {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| if (std.mem.eql(u8, &entry.key, &k)) return true;
        return false;
    }

    fn hitLocked(self: *Cache, k: [32]u8) ?*Entry {
        for (self.entries.items) |entry| if (std.mem.eql(u8, &entry.key, &k)) {
            entry.pins += 1;
            self.clock += 1;
            entry.last_used = self.clock;
            self.stats.hits += 1;
            return entry;
        };
        return null;
    }

    fn pendingLocked(self: *Cache, k: [32]u8) bool {
        // An orphaned entry will never be published, so nobody waits on it.
        for (self.pending.items) |entry| if (!entry.orphaned and !entry.unsubmitted and std.mem.eql(u8, &entry.key, &k)) return true;
        return false;
    }

    /// A cached trunk (`hit`), or a reserved entry for the caller to fill
    /// (`fill`), or `bypass` when the entry cannot fit the budget or the
    /// admission controller. Reservation happens before any encoding. A miss
    /// on a key another caller is filling waits for that fill.
    pub fn lookup(self: *Cache, k: [32]u8, tokens: usize, layers: usize, hidden: usize, host: bool) !Lookup {
        const count = try std.math.mul(usize, try std.math.mul(usize, 2 * layers, tokens), hidden);
        while (true) {
            platform.sync.lockYielding(&self.mutex);
            const precision = self.precision;
            const size = std.math.mul(usize, count, if (precision == .f16) @as(usize, 2) else 4) catch |err| {
                self.mutex.unlock();
                return err;
            };
            if (self.hitLocked(k)) |entry| {
                self.mutex.unlock();
                return .{ .hit = entry };
            }
            if (self.pendingLocked(k)) {
                self.stats.waits += 1;
                self.mutex.unlock();
                platform.time.yieldBriefly();
                continue;
            }
            if (size > self.limit_bytes) {
                self.stats.misses += 1;
                self.stats.refusals += 1;
                self.mutex.unlock();
                return .bypass;
            }
            // Preallocate the SAME pending Entry outside the mutex. Publishing
            // that header BEFORE the unlocked quota callback preserves original
            // same-key waiting: peers never spuriously bypass to different math
            // merely because this producer is between claim and allocation.
            self.mutex.unlock();
            const entry = try self.allocator.create(Entry);
            entry.* = .{ .key = k, .tokens = tokens, .hidden = hidden, .layers = layers, .precision = precision, .pins = 1, .reservation_claimed = false };
            platform.sync.lockYielding(&self.mutex);
            self.ensureListUnusedCapacityUnlocked(.pending, 1) catch |err| {
                self.mutex.unlock();
                self.allocator.destroy(entry);
                return err;
            };
            const peer = self.hitLocked(k);
            if (peer != null or self.pendingLocked(k)) {
                self.mutex.unlock();
                self.allocator.destroy(entry);
                if (peer) |hit| return .{ .hit = hit };
                continue;
            }
            self.stats.misses += 1;
            self.pending.appendAssumeCapacity(entry);
            self.revision +|= 1;
            const reserved = self.reserveLocked(size) catch |err| {
                self.removePendingLocked(entry);
                self.mutex.unlock();
                self.allocator.destroy(entry);
                return err;
            };
            const claim = reserved orelse {
                self.stats.refusals += 1;
                self.removePendingLocked(entry);
                self.mutex.unlock();
                self.allocator.destroy(entry);
                return .bypass;
            };
            entry.lease = claim.lease;
            entry.reservation_claimed = true;
            self.mutex.unlock();
            const host_count = if (host) count else 0;
            const allocated = switch (precision) {
                .f16 => if (self.allocator.alloc(f16, host_count)) |v| blk: {
                    entry.host16 = v;
                    break :blk true;
                } else |_| false,
                .f32 => if (self.allocator.alloc(f32, host_count)) |v| blk: {
                    entry.host32 = v;
                    break :blk true;
                } else |_| false,
            };
            if (!allocated) {
                self.abandon(entry);
                return error.OutOfMemory;
            }
            return .{ .fill = entry };
        }
    }

    const Reserved = struct { lease: ?memory.AdmissionLease, bytes: usize };

    /// Make room for `size` bytes under the byte budget and the admission
    /// controller, evicting least recently used unpinned entries. Null when
    /// it cannot fit.
    fn reserveLocked(self: *Cache, size: usize) !?Reserved {
        return self.reserveWithSnapshotLocked(size, null);
    }
    fn reserveWithSnapshotLocked(self: *Cache, size: usize, snapshot_value: ?*ColdSnapshot) !?Reserved {
        if (size > self.limit_bytes) return null;
        while (self.stats.bytes + self.reserved_bytes + size > self.limit_bytes) {
            if (!try self.evictForReservationLocked(snapshot_value)) return null;
            if (size > self.limit_bytes) return null;
        }
        self.reserved_bytes = try std.math.add(usize, self.reserved_bytes, size);
        while (true) {
            const admission = self.admission orelse return .{ .lease = null, .bytes = size };
            const amounts: memory.AdmissionAmounts = if (admission.device) .{ .backend_kv_bytes = size } else .{ .host_kv_bytes = size };
            const before_revision = self.revision;
            self.mutex.unlock();
            const acquired = admission.controller.tryAcquire(admission.backend_class, admission.limits, amounts, true);
            platform.sync.lockYielding(&self.mutex);
            if (snapshot_value) |expected_view| {
                if (before_revision != self.revision or !self.matchesColdLocked(expected_view)) {
                    self.reserved_bytes -= size;
                    if (acquired) |lease| {
                        var held = lease;
                        self.mutex.unlock();
                        held.release(); // no physical KV was born
                        platform.sync.lockYielding(&self.mutex);
                    } else |_| {}
                    return error.TrunkCacheChangedBeforeExecution;
                }
            }
            if (acquired) |lease| return .{ .lease = lease, .bytes = size } else |_| {
                // Original useful-victim retry; physical backing and its quota
                // die in evictOne without this cache mutex held.
                const evicted = self.evictForReservationLocked(snapshot_value) catch |err| {
                    self.reserved_bytes -= size;
                    return err;
                };
                if (!evicted) {
                    self.reserved_bytes -= size;
                    return null;
                }
            }
        }
    }

    /// SAME LRU victim policy. A cold caller advances its expected_view only for
    /// the exact single mutation it performed; callbacks/peer raw changes are
    /// detected by the nonrecycling local revision AND pinned allocation owner.
    fn evictForReservationLocked(self: *Cache, snapshot_value: ?*ColdSnapshot) !bool {
        const expected_view = snapshot_value orelse return self.evictOne();
        if (self.active_receiver or !self.matchesColdLocked(expected_view) or self.revision >= std.math.maxInt(u64) - 2)
            return error.TrunkCacheChangedBeforeExecution;
        const before_revision = self.revision;
        const before_count = self.entries.items.len;
        if (!self.evictOne()) return false;
        if (self.revision != before_revision + 1 or self.entries.items.len + 1 != before_count or
            self.entries.capacity != expected_view.entries.capacity or self.entries.items.ptr != expected_view.entries.items.ptr or self.entries_owner != expected_view.entries_owner or
            self.pending.items.len != expected_view.pending.items.len or self.pending.capacity != expected_view.pending.capacity or self.pending.items.ptr != expected_view.pending.items.ptr or self.pending_owner != expected_view.pending_owner or
            self.limit_bytes != expected_view.limit_bytes or self.min_tokens != expected_view.min_tokens or self.precision != expected_view.precision or !std.meta.eql(self.admission, expected_view.admission))
            return error.TrunkCacheChangedBeforeExecution;
        expected_view.revision = self.revision;
        expected_view.entries = self.entries;
        // This is an explicitly outside-Model/FIFO cold caller, not a numerical
        // release. Victim pins were zero and no receiver is active. Settle the
        // detached physical owner before its original quota can fund a retry.
        self.mutex.unlock();
        const drained = self.drainRetiredOutsideExecution(.{ .outside_execution = true });
        platform.sync.lockYielding(&self.mutex);
        try drained;
        if (!self.matchesColdLocked(expected_view)) return error.TrunkCacheChangedBeforeExecution;
        return true;
    }

    fn ensureListUnusedCapacityUnlocked(self: *Cache, comptime which: ListKind, extra: usize) !void {
        const list = if (which == .entries) &self.entries else &self.pending;
        while (list.capacity - list.items.len < extra) {
            const needed = try std.math.add(usize, list.items.len, extra);
            self.mutex.unlock();
            var replacement: std.ArrayListUnmanaged(*Entry) = .empty;
            replacement.ensureTotalCapacity(self.allocator, needed) catch |err| {
                platform.sync.lockYielding(&self.mutex);
                return err;
            };
            platform.sync.lockYielding(&self.mutex);
            if (list.capacity - list.items.len >= extra or replacement.capacity -| list.items.len < extra) {
                self.mutex.unlock();
                replacement.deinit(self.allocator);
                platform.sync.lockYielding(&self.mutex);
                continue;
            }
            replacement.appendSliceAssumeCapacity(list.items);
            var previous = list.*;
            const owner_slot = if (which == .entries) &self.entries_owner else &self.pending_owner;
            const previous_owner = owner_slot.*;
            owner_slot.* = null;
            list.* = replacement;
            if (previous_owner) |owner| {
                // The old paid allocation stays alive until all real cold
                // snapshot pins end and an explicit outside drain retires it.
                owner.retired_next = self.retired_lists;
                self.retired_lists = owner;
            }
            self.revision +|= 1;
            self.mutex.unlock();
            if (previous_owner == null) previous.deinit(self.allocator);
            platform.sync.lockYielding(&self.mutex);
        }
    }

    fn dropClaimedReservationLocked(self: *Cache, reserved: Reserved) void {
        self.reserved_bytes -= reserved.bytes;
        self.mutex.unlock();
        dropReservation(reserved);
        platform.sync.lockYielding(&self.mutex);
    }

    fn dropReservation(reserved: Reserved) void {
        if (reserved.lease) |lease| {
            var owned = lease;
            owned.release();
        }
    }

    fn removePendingLocked(self: *Cache, entry: *Entry) void {
        const index = std.mem.indexOfScalar(*Entry, self.pending.items, entry).?;
        _ = self.pending.swapRemove(index);
        if (entry.reservation_claimed) self.reserved_bytes -= entry.bytes();
        self.revision +|= 1;
    }

    /// Unpin an entry from `lookup` (either kind) or `acquire`. An entry that
    /// is no longer cached is freed on its last release.
    pub fn release(self: *Cache, entry: *Entry) void {
        platform.sync.lockYielding(&self.mutex);
        entry.pins -= 1;
        var destroy_now = false;
        if (entry.pins == 0) {
            if (entry.orphaned) {
                self.removePendingLocked(entry);
                destroy_now = true;
            } else if (std.mem.indexOfScalar(*Entry, self.entries.items, entry) == null and
                std.mem.indexOfScalar(*Entry, self.pending.items, entry) == null) destroy_now = true;
        }
        self.mutex.unlock();
        if (destroy_now) self.destroy(entry);
    }

    /// Give up a `fill` entry (the fill failed): drop its reservation and
    /// lease and free it. Waiters retry and one of them fills instead. The
    /// caller must not `release` it afterwards.
    pub fn abandon(self: *Cache, entry: *Entry) void {
        platform.sync.lockYielding(&self.mutex);
        entry.pins -= 1;
        const destroy_now = entry.pins == 0;
        if (destroy_now) self.removePendingLocked(entry) else entry.orphaned = true;
        self.mutex.unlock();
        if (destroy_now) self.destroy(entry);
    }

    /// Evict the least recently used unpinned entry; false when none is.
    fn evictOne(self: *Cache) bool {
        var victim: ?usize = null;
        for (self.entries.items, 0..) |candidate, i| {
            if (candidate.pins != 0) continue;
            if (victim == null or candidate.last_used < self.entries.items[victim.?].last_used) victim = i;
        }
        const index = victim orelse return false;
        const evicted = self.entries.swapRemove(index);
        self.stats.bytes -= evicted.bytes();
        self.stats.evictions += 1;
        self.revision +|= 1;
        self.mutex.unlock();
        self.destroy(evicted);
        platform.sync.lockYielding(&self.mutex);
        return true;
    }

    /// Publish a filled `fill` entry. Its bytes and lease were reserved by
    /// `lookup`, so it always fits; if the list cannot grow, the entry is
    /// used once and freed on release instead. The caller keeps its pin and
    /// must `release` it.
    pub fn publish(self: *Cache, entry: *Entry) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (entry.metadata_backing != null) {
            if (self.entries_owner == null or self.entries.items.len == self.entries.capacity) {
                entry.orphaned = true;
                return;
            }
        } else self.ensureListUnusedCapacityUnlocked(.entries, 1) catch {
            entry.orphaned = true;
            return;
        };
        self.removePendingLocked(entry);
        self.entries.appendAssumeCapacity(entry);
        self.clock += 1;
        entry.last_used = self.clock;
        self.stats.bytes += entry.bytes();
        self.stats.entries = self.entries.items.len;
        self.revision +|= 1;
    }

    pub fn snapshot(self: *Cache) Stats {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var out = self.stats;
        out.entries = self.entries.items.len;
        return out;
    }
};

/// Test helper: `lookup` that must reserve, then fill with `value` and publish.
fn fillFor(cache: *Cache, k: [32]u8, tokens: usize, layers: usize, hidden: usize, value: f16) !*Entry {
    const entry = switch (try cache.lookup(k, tokens, layers, hidden, true)) {
        .fill => |e| e,
        else => return error.TestUnexpectedResult,
    };
    @memset(entry.host16, value);
    @memset(entry.host32, @as(f32, value));
    cache.publish(entry);
    return entry;
}

test "laya trunk cache pins, evicts least recently used, and bypasses what cannot fit" {
    const a = std.testing.allocator;
    var cache = Cache.init(a, 2 * 2 * 4 * 8 * @sizeOf(f16));
    defer cache.deinit();
    const k1 = Cache.key(&.{ 1, 2, 3, 4 }, 1, 8);
    const k2 = Cache.key(&.{ 1, 2, 3, 5 }, 1, 8);
    const k3 = Cache.key(&.{ 9, 9, 9, 9 }, 1, 8);
    try std.testing.expect(!std.mem.eql(u8, &k1, &k2));
    try std.testing.expect(cache.acquire(k1) == null);
    for ([_][32]u8{ k1, k2 }) |k| cache.release(try fillFor(&cache, k, 4, 1, 8, 1));
    try std.testing.expectEqual(@as(usize, 2), cache.snapshot().entries);
    // A second lookup of a cached key is a hit, not a fill.
    switch (try cache.lookup(k1, 4, 1, 8, true)) {
        .hit => |e| cache.release(e),
        else => return error.TestUnexpectedResult,
    }
    // k1 is pinned, so reserving k3 must evict k2 even though k1 is older.
    const pinned = cache.acquire(k1).?;
    cache.release(try fillFor(&cache, k3, 4, 1, 8, 1));
    try std.testing.expect(cache.acquire(k2) == null);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().evictions);
    cache.release(pinned);
    // An entry larger than the whole budget is bypassed before any encoding.
    try std.testing.expect((try cache.lookup(Cache.key(&.{7}, 3, 64), 4, 3, 64, false)) == .bypass);
    try std.testing.expectEqual(@as(usize, 2), cache.snapshot().entries);
    // An abandoned fill frees its reservation, so the key can be filled again.
    const k4 = Cache.key(&.{4}, 1, 8);
    switch (try cache.lookup(k4, 4, 1, 8, true)) {
        .fill => |e| cache.abandon(e),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 0), cache.reserved_bytes);
    cache.release(try fillFor(&cache, k4, 4, 1, 8, 1));
}

test "laya trunk cache stores f16 and f32 slots and charges admission before filling" {
    const a = std.testing.allocator;
    for ([_]Precision{ .f16, .f32 }) |precision| {
        var cache = Cache.init(a, 1 << 20);
        defer cache.deinit();
        cache.setPrecision(precision);
        const entry = switch (try cache.lookup(Cache.key(&.{1}, 1, 4), 2, 1, 4, true)) {
            .fill => |e| e,
            else => return error.TestUnexpectedResult,
        };
        const values = [_]f32{ 0.5, -1.25, 3.0e-3, 1000.0, 1, 2, 3, 4 };
        entry.store(1, &values);
        var out: [8]f32 = undefined;
        entry.load(1, &out);
        for (values, out) |want, got| try std.testing.expectApproxEqRel(want, got, @as(f32, if (precision == .f16) 1e-3 else 1e-7));
        try std.testing.expectEqual(@as(usize, 2 * 1 * 2 * 4 * (if (precision == .f16) @as(usize, 2) else 4)), entry.bytes());
        cache.publish(entry);
        cache.release(entry);
    }
    // A controller with no room refuses the reservation: nothing is filled.
    var controller: memory.AdmissionController = .{};
    var cache = Cache.init(a, 1 << 20);
    defer cache.deinit();
    cache.configureAdmission(.{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 16 }, .device = false });
    try std.testing.expect((try cache.lookup(Cache.key(&.{2}, 1, 4), 2, 1, 4, true)) == .bypass);
    try std.testing.expectEqual(@as(usize, 0), cache.snapshot().entries);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().refusals);
    // With room, the lease is taken at reservation, held while cached, and
    // released on eviction.
    cache.configureAdmission(.{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1 << 20 }, .device = false });
    const kept = switch (try cache.lookup(Cache.key(&.{3}, 1, 4), 2, 1, 4, true)) {
        .fill => |e| e,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(kept.bytes(), controller.snapshot().host_kv_bytes);
    cache.publish(kept);
    cache.release(kept);
    try std.testing.expectEqual(kept.bytes(), controller.snapshot().host_kv_bytes);
    cache.deinit();
    cache = Cache.init(a, 1 << 20);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_kv_bytes);
}

test "laya trunk cache keeps an entry it could not publish charged until its last release" {
    var controller: memory.AdmissionController = .{};
    // lookup allocates the entry, grows `pending`, then the host slice;
    // publish's growth of `entries` is the fourth allocation.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 3 });
    var cache = Cache.init(failing.allocator(), 1 << 20);
    defer cache.deinit();
    cache.configureAdmission(.{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1 << 20 }, .device = false });
    const k = Cache.key(&.{8}, 1, 4);
    const entry = switch (try cache.lookup(k, 2, 1, 4, true)) {
        .fill => |e| e,
        else => return error.TestUnexpectedResult,
    };
    cache.publish(entry);
    try std.testing.expect(entry.orphaned);
    try std.testing.expectEqual(@as(usize, 0), cache.snapshot().entries);
    // Still in use by its filler: its bytes and lease stay charged.
    try std.testing.expectEqual(entry.bytes(), cache.reserved_bytes);
    try std.testing.expectEqual(entry.bytes(), controller.snapshot().host_kv_bytes);
    // Nobody waits on an entry that will never be published.
    platform.sync.lockYielding(&cache.mutex);
    try std.testing.expect(!cache.pendingLocked(k));
    cache.mutex.unlock();
    cache.release(entry);
    try std.testing.expectEqual(@as(usize, 0), cache.reserved_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_kv_bytes);
}

test "laya trunk cache keeps concurrent misses inside the budget and fills each key once" {
    const a = std.testing.allocator;
    const tokens = 4;
    const layers = 1;
    const hidden = 8;
    const entry_bytes = 2 * layers * tokens * hidden * @sizeOf(f16);
    // Room for exactly one entry.
    var cache = Cache.init(a, entry_bytes);
    defer cache.deinit();
    const Worker = struct {
        cache: *Cache,
        key: [32]u8,
        fills: *std.atomic.Value(u32),
        hits: *std.atomic.Value(u32),
        bypasses: *std.atomic.Value(u32),
        over_budget: *std.atomic.Value(bool),
        failed: *std.atomic.Value(bool),
        fn run(self: @This()) void {
            const result = self.cache.lookup(self.key, tokens, layers, hidden, true) catch {
                self.failed.store(true, .seq_cst);
                return;
            };
            switch (result) {
                .fill => |entry| {
                    _ = self.fills.fetchAdd(1, .seq_cst);
                    // Encoding takes a while; others must wait, not refill.
                    platform.sync.lockYielding(&self.cache.mutex);
                    if (self.cache.stats.bytes + self.cache.reserved_bytes > self.cache.limit_bytes) self.over_budget.store(true, .seq_cst);
                    self.cache.mutex.unlock();
                    for (0..200) |_| platform.time.yieldBriefly();
                    @memset(entry.host16, 3);
                    self.cache.publish(entry);
                    self.cache.release(entry);
                },
                .hit => |entry| {
                    _ = self.hits.fetchAdd(1, .seq_cst);
                    if (entry.host16[0] != 3) self.failed.store(true, .seq_cst);
                    self.cache.release(entry);
                },
                .bypass => _ = self.bypasses.fetchAdd(1, .seq_cst),
            }
        }
    };
    var fills = std.atomic.Value(u32).init(0);
    var hits = std.atomic.Value(u32).init(0);
    var bypasses = std.atomic.Value(u32).init(0);
    var over_budget = std.atomic.Value(bool).init(false);
    var failed = std.atomic.Value(bool).init(false);
    const shared = Cache.key(&.{ 5, 5, 5 }, layers, hidden);
    var threads: [8]std.Thread = undefined;
    for (&threads, 0..) |*thread, i| {
        // Six threads miss on one key; two on keys of their own.
        const k = if (i < 6) shared else Cache.key(&.{@intCast(i)}, layers, hidden);
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{Worker{ .cache = &cache, .key = k, .fills = &fills, .hits = &hits, .bypasses = &bypasses, .over_budget = &over_budget, .failed = &failed }});
    }
    for (threads) |thread| thread.join();
    try std.testing.expect(!failed.load(.seq_cst));
    try std.testing.expect(!over_budget.load(.seq_cst));
    try std.testing.expectEqual(@as(u32, 8), fills.load(.seq_cst) + hits.load(.seq_cst) + bypasses.load(.seq_cst));
    const stats = cache.snapshot();
    try std.testing.expect(stats.bytes <= entry_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.reserved_bytes);
    // The shared key is filled once unless it was evicted and refilled
    // between waves; never more fills than distinct keys plus evictions.
    try std.testing.expect(fills.load(.seq_cst) <= 3 + stats.evictions);
}

// New actual-allocator oracle: quota sees both payload and Entry already dead,
// and every resource callback can reenter the real cache metadata mutex.
test "laya trunk cache allocator physical death precedes unlocked admission refund" {
    const Spy = struct {
        cache: ?*Cache = null,
        expected_header: usize = 0,
        expected_payload: usize = 0,
        header_dead: bool = false,
        payload_dead: bool = false,
        violation: bool = false,
        releases: usize = 0,
        fn inspect(self: *@This()) void {
            if (self.cache) |cache| {
                if (cache.mutex.tryLock()) cache.mutex.unlock() else self.violation = true;
            }
        }
        fn alloc(raw: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inspect();
            return std.testing.allocator.rawAlloc(n, alignment, ra);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inspect();
            const address = @intFromPtr(bytes.ptr);
            std.testing.allocator.rawFree(bytes, alignment, ra);
            if (address == self.expected_header) self.header_dead = true;
            if (address == self.expected_payload) self.payload_dead = true;
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inspect();
            return 1;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inspect();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inspect();
            self.releases += 1;
            if (!self.header_dead or !self.payload_dead) self.violation = true;
        }
    };
    var spy = Spy{};
    const a = std.mem.Allocator{ .ptr = &spy, .vtable = &.{ .alloc = Spy.alloc, .resize = Spy.resize, .remap = Spy.remap, .free = Spy.free } };
    var cache = Cache.init(a, 4096);
    defer cache.deinit();
    spy.cache = &cache;
    var controller: memory.AdmissionController = .{};
    try controller.configureResourceBudget(.{ .context = &spy, .try_reserve = Spy.reserve, .retain = Spy.retain, .release = Spy.release });
    cache.configureAdmission(.{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 4096 }, .device = false });
    const entry = switch (try cache.lookup(Cache.key(&.{1}, 1, 4), 2, 1, 4, true)) {
        .fill => |value| value,
        else => return error.TestUnexpectedResult,
    };
    spy.expected_header = @intFromPtr(entry);
    spy.expected_payload = @intFromPtr(entry.host16.ptr);
    try std.testing.expect(!spy.header_dead and !spy.payload_dead and !spy.violation);
    cache.abandon(entry);
    try std.testing.expect(spy.header_dead and spy.payload_dead and !spy.violation);
    try std.testing.expectEqual(@as(usize, 1), spy.releases);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_kv_bytes);
}

test "laya trunk cache cold pointer-list geometry uses actual stdlib allocations" {
    for ([_]usize{ 1, 2, 7, 17, 129 }) |count| {
        var list: std.ArrayListUnmanaged(*Entry) = .empty;
        defer list.deinit(std.testing.allocator);
        try list.ensureTotalCapacity(std.testing.allocator, count);
        try std.testing.expectEqual(list.capacity, try pointerListCapacity(count));
    }
    try std.testing.expectEqual(@as(usize, 0), try pointerListCapacity(0));
}

test "laya trunk cache cold SAME Entry and paid containers survive sealed A until actual close B" {
    const a = std.testing.allocator;
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const sink = @import("../execution/retirement_sink.zig");
    const domain = try Domain.create();
    defer domain.release();
    const old_domain = domain.pin();
    defer old_domain.release();
    var cache = Cache.init(a, 4096);
    defer cache.deinit();
    cache.configureAdmission(.{ .controller = &domain.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1 << 20 }, .device = false });
    const cache_key = Cache.key(&.{ 7, 8, 9, 10 }, 1, 8);
    var snapshot_value = try cache.snapshotCold(cache_key);
    var snapshot_live = true;
    defer if (snapshot_live) snapshot_value.finishOutsideExecution(&cache, .{ .outside_execution = true }) catch @panic("cold snapshot unwind failed");
    const entries_geometry = try coldListGeometry(&snapshot_value, .entries, 1);
    const pending_geometry = try coldListGeometry(&snapshot_value, .pending, 1);
    const fill_geometry = try coldFillGeometry(4, 1, 8, .f16);
    const total_metadata = entries_geometry.bytes + pending_geometry.bytes + fill_geometry.metadata_bytes;
    var source = try domain.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 1 << 20 }, .{ .host_scratch_bytes = total_metadata }, false);
    defer source.release();
    var credit = memory.AdmissionAmounts{ .host_scratch_bytes = total_metadata };
    const Fixture = struct {
        source: *memory.AdmissionLease,
        credit: *memory.AdmissionAmounts,
        domain: DomainPin,
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, backend: @import("../graph/backend_contracts.zig").BackendKind, class: @import("../execution/storage_accounting.zig").Class, bytes: usize) !storage.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return storage.fromRunLease(allocator, self.source, .{ .controller = self.domain.controller(), .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1 << 20 }, .check_live_memory = false, .credit = self.credit, .lifetime = storage.Lifetime.forAdmissionDomain(self.domain) }, backend, class, bytes);
        }
        fn capability(self: *@This()) storage.Capability {
            return .{ .identity = .{ .controller = self.domain.controller(), .backend_class = .cpu }, .context = self, .services = &.{ .reserve = reserve } };
        }
    };
    var fixture = Fixture{ .source = &source, .credit = &credit, .domain = old_domain };
    var banks: ?*prepared.Prepared = try prepared.Prepared.prepare(fixture.capability(), &.{
        .{ .backend = .native, .class = .host_workspace, .bytes = entries_geometry.bytes },
        .{ .backend = .native, .class = .host_workspace, .bytes = pending_geometry.bytes },
        .{ .backend = .native, .class = .host_workspace, .bytes = fill_geometry.metadata_bytes },
    });
    var owner = try sink.Ref.create();
    defer owner.release();
    var guard_a = try owner.enter();
    defer guard_a.finish();
    var epoch_a = try guard_a.retainEpoch();
    defer epoch_a.release();
    defer if (banks) |value| value.finishInvocationWithBoundary(&epoch_a);
    try banks.?.bindConstructionEpoch(&epoch_a);
    var cap = prepared.Prepared.forDispatch(banks, fixture.capability().identity);
    // This fixture explicitly owns the same live invocation and construction B.
    // Production RunPermit supplies its independent currentDispatchEpoch query.
    var cap_services = cap.services.*;
    cap_services.currentCleanupEpoch = cap_services.constructionEpoch;
    cap.services = &cap_services;
    var entries_plan = try cache.prepareListOutsideExecution(&snapshot_value, .entries, 1, cap);
    defer entries_plan.abortOutsideExecution();
    var pending_plan = try cache.prepareListOutsideExecution(&snapshot_value, .pending, 1, cap);
    defer pending_plan.abortOutsideExecution();
    try cache.commitColdListsOutsideExecution(&snapshot_value, &entries_plan, &pending_plan, .{ .boundary = &epoch_a });
    const actual_binding = storage.BackingReservation{ .owner = snapshot_value.entries_owner.?.backing, .construction_epoch = &epoch_a };
    const before_refusal = domain.controller.snapshot();
    try validateMetadataBinding(actual_binding, cap, entries_geometry.bytes);
    try std.testing.expectError(error.InvalidPhysicalStorageReservation, validateMetadataBinding(actual_binding, cap, entries_geometry.bytes + 1));
    var foreign_class = cap;
    foreign_class.identity.backend_class = .gpu;
    try std.testing.expectError(error.InvalidPhysicalStorageReservation, validateMetadataBinding(actual_binding, foreign_class, entries_geometry.bytes));
    var no_current = cap;
    var no_current_services = cap.services.*;
    no_current_services.currentCleanupEpoch = null;
    no_current.services = &no_current_services;
    try std.testing.expectError(error.MissingCurrentCleanupEpoch, validateMetadataBinding(actual_binding, no_current, entries_geometry.bytes));
    var other_guard = try owner.enter();
    defer other_guard.finish();
    var other_epoch = try other_guard.retainEpoch();
    defer other_epoch.release();
    var foreign_binding = actual_binding;
    foreign_binding.construction_epoch = &other_epoch;
    try std.testing.expectError(error.ForeignNativeWeightModel, validateMetadataBinding(foreign_binding, cap, entries_geometry.bytes));
    other_guard.finish();
    try std.testing.expectError(error.RetirementEpochSealed, validateMetadataBinding(foreign_binding, cap, entries_geometry.bytes));
    try std.testing.expectEqualDeep(before_refusal, domain.controller.snapshot());
    try cache.validateCold(&snapshot_value);
    const entry = (try cache.prepareFillOutsideExecution(&snapshot_value, cache_key, 4, 1, 8, cap, old_domain)).?;
    var entry_pin_live = true;
    defer if (entry_pin_live) cache.finishPreparedEntryOutsideExecution(entry, .{ .boundary = &epoch_a }) catch @panic("cold Entry unwind failed");
    // A raw Model owner must never wait on a producer not yet entered.
    platform.sync.lockYielding(&cache.mutex);
    const raw_wait = cache.pendingLocked(cache_key);
    cache.mutex.unlock();
    try std.testing.expect(!raw_wait and entry.unsubmitted);
    try cache.beginPreparedCall(&snapshot_value);
    var receiver_live = true;
    defer if (receiver_live) cache.endPreparedCall();
    const same_entry = switch (try cache.consumePrepared(&snapshot_value, entry, .{ .boundary = &epoch_a })) {
        .fill => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expect(same_entry == entry);
    @memset(entry.host16, 0.75);
    try cache.publishPrepared(entry, &snapshot_value);
    cache.release(entry); // original numerical lookup loan
    cache.endPreparedCall();
    receiver_live = false;
    try cache.finishPreparedEntryOutsideExecution(entry, .{ .boundary = &epoch_a });
    entry_pin_live = false;
    try snapshot_value.finishOutsideExecution(&cache, .{ .boundary = &epoch_a });
    snapshot_live = false;
    banks.?.releaseAfterExecution(&epoch_a);
    banks = null;
    guard_a.finish();
    try std.testing.expect(try epoch_a.isSealed());
    try std.testing.expectEqual(total_metadata, domain.controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(fill_geometry.payload_bytes, domain.controller.snapshot().host_kv_bytes);
    try std.testing.expectError(error.RetirementEpochSealed, cache.deinitWithCleanup(.{ .boundary = &epoch_a }));
    try std.testing.expectEqual(@as(usize, 1), cache.snapshot().entries);
    var guard_b = try owner.enter();
    defer guard_b.finish();
    var epoch_b = try guard_b.retainEpoch();
    defer epoch_b.release();
    const other_key = Cache.key(&.{ 101, 102, 103, 104 }, 1, 8);
    var paused = try cache.snapshotCold(other_key);
    var paused_live = true;
    defer if (paused_live) paused.finishOutsideExecution(&cache, .{ .boundary = &epoch_b }) catch @panic("paid metadata snapshot unwind");
    const pinned_list = paused.entries_owner.?;
    const pinned_capacity = paused.entries.capacity;
    try std.testing.expectEqual(fill_geometry.payload_bytes, cache.reclaimOneDeferringPhysical());
    try std.testing.expect(cache.entries_owner == null and cache.entries.capacity == 0);
    try cache.drainRetiredOutsideExecution(.{ .boundary = &epoch_b });
    // The old list allocation is still physically pinned across the pause;
    // old Entry payload and quota also wait actual B, independently of A.
    try std.testing.expect(paused.entries_owner.? == pinned_list);
    try std.testing.expectEqual(pinned_capacity, pinned_list.list.capacity);
    try std.testing.expectEqual(@as(f16, 0.75), entry.host16[0]);
    try std.testing.expectEqual(total_metadata, domain.controller.snapshot().host_scratch_bytes);
    const raw_replacement = try fillFor(&cache, other_key, 4, 1, 8, 0.5);
    cache.release(raw_replacement);
    try std.testing.expectEqual(pinned_capacity, cache.entries.capacity);
    try std.testing.expectError(error.TrunkCacheChangedBeforeExecution, cache.validateCold(&paused));
    try paused.finishOutsideExecution(&cache, .{ .boundary = &epoch_b });
    paused_live = false;
    try std.testing.expectEqual(total_metadata, domain.controller.snapshot().host_scratch_bytes);
    try cache.deinitWithCleanup(.{ .boundary = &epoch_b });
    // Entry physical storage and SAME KV lease remain held by actual B. List
    // physical metadata may already be dead, but its exact charge still waits B.
    try std.testing.expectEqual(fill_geometry.payload_bytes, domain.controller.snapshot().host_kv_bytes);
    try std.testing.expectEqual(total_metadata, domain.controller.snapshot().host_scratch_bytes);
    guard_b.finish();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "laya raw hit rejects same-controller foreign class before lease move or metadata supplier" {
    const a = std.testing.allocator;
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    const pin = domain.pin();
    defer pin.release();
    var cache = Cache.init(a, 4096);
    defer cache.deinit();
    cache.configureAdmission(.{ .controller = pin.controller(), .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1 << 20 }, .device = false });
    const cache_key = Cache.key(&.{ 4, 5, 6, 7 }, 1, 8);
    const entry = try fillFor(&cache, cache_key, 4, 1, 8, 0.5);
    cache.release(entry);
    const physical = entry.host16.ptr;
    const amounts = pin.controller().snapshot();
    cache.configureAdmission(.{ .controller = pin.controller(), .backend_class = .gpu, .limits = .{ .host_limit_bytes = 1 << 20 }, .device = false });
    var snapshot_value = try cache.snapshotCold(cache_key);
    defer snapshot_value.finishOutsideExecution(&cache, .{ .outside_execution = true }) catch @panic("foreign class snapshot did not finish");
    const pins_before = entry.pins;
    const Poison = struct {
        calls: usize = 0,
        fn reserve(raw: *anyopaque, _: std.mem.Allocator, _: @import("../graph/backend_contracts.zig").BackendKind, _: @import("../execution/storage_accounting.zig").Class, _: usize) !storage.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.UnexpectedMetadataSupplier;
        }
        fn take(raw: *anyopaque, _: std.mem.Allocator, _: @import("../graph/backend_contracts.zig").BackendKind, _: @import("../execution/storage_accounting.zig").Class, _: usize) !storage.BackingReservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.UnexpectedMetadataSupplier;
        }
    };
    var poison = Poison{};
    const capability = storage.Capability{ .identity = .{ .controller = pin.controller(), .backend_class = .gpu }, .context = &poison, .services = &.{ .reserve = Poison.reserve, .takeBacking = Poison.take } };
    try std.testing.expectError(error.ForeignAdmissionDomain, cache.prepareHitOwnershipOutsideExecution(&snapshot_value, capability, pin));
    try std.testing.expectEqual(@as(usize, 0), poison.calls);
    try std.testing.expect(entry.lease != null and entry.quota == null and entry.metadata_backing == null);
    try std.testing.expect(entry.host16.ptr == physical);
    try std.testing.expectEqual(pins_before, entry.pins);
    try std.testing.expectEqualDeep(amounts, pin.controller().snapshot());
}

const ColdQuotaFixture = if (@import("builtin").is_test) struct {
    source: *memory.AdmissionLease,
    credit: *memory.AdmissionAmounts,
    domain: DomainPin,
    fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, backend: @import("../graph/backend_contracts.zig").BackendKind, class: @import("../execution/storage_accounting.zig").Class, bytes: usize) !storage.Reservation {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return storage.fromRunLease(allocator, self.source, .{ .controller = self.domain.controller(), .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1 << 20 }, .check_live_memory = false, .credit = self.credit, .lifetime = storage.Lifetime.forAdmissionDomain(self.domain) }, backend, class, bytes);
    }
    fn capability(self: *@This()) storage.Capability {
        return .{ .identity = .{ .controller = self.domain.controller(), .backend_class = .cpu }, .context = self, .services = &.{ .reserve = reserve } };
    }
} else void;

fn coldCacheConstructionOomCase(allocator: std.mem.Allocator) !void {
    const sink = @import("../execution/retirement_sink.zig");
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    const pin = domain.pin();
    defer pin.release();
    var owner = try sink.Ref.create();
    defer owner.release();
    var guard = try owner.enter();
    defer guard.finish(); // EVERY failing-allocator object dies before return
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    const current = Cleanup{ .boundary = &epoch };
    var cache = Cache.init(allocator, 4096);
    cache.test_cold_allocator = true; // explicit fixture-owned context lives through B
    var cache_live = true;
    defer if (cache_live) cache.deinit();
    cache.configureAdmission(.{ .controller = pin.controller(), .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1 << 20 }, .device = false });
    const cache_key = Cache.key(&.{ 11, 12, 13, 14 }, 1, 8);
    var snapshot_value = try cache.snapshotCold(cache_key);
    var snapshot_live = true;
    defer if (snapshot_live) snapshot_value.finishOutsideExecution(&cache, current) catch @panic("all-OOM snapshot unwind");
    const entry_layout = try coldListGeometry(&snapshot_value, .entries, 1);
    const pending_layout = try coldListGeometry(&snapshot_value, .pending, 1);
    const fill_layout = try coldFillGeometry(4, 1, 8, .f16);
    const total = entry_layout.bytes + pending_layout.bytes + fill_layout.metadata_bytes;
    var source = try pin.controller().tryAcquire(.cpu, .{ .host_limit_bytes = 1 << 20 }, .{ .host_scratch_bytes = total }, false);
    defer source.release();
    var credit = memory.AdmissionAmounts{ .host_scratch_bytes = total };
    var fixture = ColdQuotaFixture{ .source = &source, .credit = &credit, .domain = pin };
    var banks: ?*prepared.Prepared = try prepared.Prepared.prepare(fixture.capability(), &.{
        .{ .backend = .native, .class = .host_workspace, .bytes = entry_layout.bytes },
        .{ .backend = .native, .class = .host_workspace, .bytes = pending_layout.bytes },
        .{ .backend = .native, .class = .host_workspace, .bytes = fill_layout.metadata_bytes },
    });
    defer if (banks) |value| value.finishInvocationWithBoundary(&epoch);
    try banks.?.bindConstructionEpoch(&epoch);
    var cap = prepared.Prepared.forDispatch(banks, fixture.capability().identity);
    // This fixture explicitly owns the same live invocation and construction B.
    // Production RunPermit supplies its independent currentDispatchEpoch query.
    var cap_services = cap.services.*;
    cap_services.currentCleanupEpoch = cap_services.constructionEpoch;
    cap.services = &cap_services;
    var entries_plan = try cache.prepareListOutsideExecution(&snapshot_value, .entries, 1, cap);
    defer entries_plan.abortOutsideExecution();
    var pending_plan = try cache.prepareListOutsideExecution(&snapshot_value, .pending, 1, cap);
    defer pending_plan.abortOutsideExecution();
    try cache.commitColdListsOutsideExecution(&snapshot_value, &entries_plan, &pending_plan, current);
    const entry = (try cache.prepareFillOutsideExecution(&snapshot_value, cache_key, 4, 1, 8, cap, pin)).?;
    var entry_live = true;
    defer if (entry_live) cache.finishPreparedEntryOutsideExecution(entry, current) catch @panic("all-OOM Entry unwind");
    try cache.beginPreparedCall(&snapshot_value);
    var receiver_live = true;
    defer if (receiver_live) cache.endPreparedCall();
    try std.testing.expect(try cache.consumePrepared(&snapshot_value, entry, current) == .fill);
    @memset(entry.host16, 0.25);
    try cache.publishPrepared(entry, &snapshot_value);
    cache.release(entry);
    cache.endPreparedCall();
    receiver_live = false;
    try cache.finishPreparedEntryOutsideExecution(entry, current);
    entry_live = false;
    try snapshot_value.finishOutsideExecution(&cache, current);
    snapshot_live = false;
    try cache.deinitWithCleanup(current);
    cache_live = false;
    banks.?.releaseAfterExecution(&epoch);
    banks = null;
    guard.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
}

test "laya cold list Entry Charge and payload construction unwind every allocation failure before B and quota" {
    try coldCacheConstructionOomCase(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, coldCacheConstructionOomCase, .{});
}

test "laya raw same-key quota claim remains pending rather than changing peer to uncached math" {
    const a = std.testing.allocator;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var cache = Cache.init(a, 128);
    defer cache.deinit();
    cache.configureAdmission(.{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 4096 }, .device = false });
    const Probe = struct {
        cache: *Cache,
        entered: std.atomic.Value(bool) = .init(false),
        release_claim: std.atomic.Value(bool) = .init(false),
        reserve_calls: std.atomic.Value(usize) = .init(0),
        releases: std.atomic.Value(usize) = .init(0),
        violation: std.atomic.Value(bool) = .init(false),
        fn unlocked(self: *@This()) void {
            if (self.cache.mutex.tryLock()) self.cache.mutex.unlock() else self.violation.store(true, .release);
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.unlocked();
            const token = self.reserve_calls.fetchAdd(1, .acq_rel) + 1;
            self.entered.store(true, .release);
            while (!self.release_claim.load(.acquire)) platform.time.yieldBriefly();
            return token;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.unlocked();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.unlocked();
            _ = self.releases.fetchAdd(1, .acq_rel);
        }
    };
    var probe = Probe{ .cache = &cache };
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    const Work = struct {
        cache: *Cache,
        cache_key: [32]u8,
        fills: *std.atomic.Value(usize),
        hits: *std.atomic.Value(usize),
        bypasses: *std.atomic.Value(usize),
        failed: *std.atomic.Value(bool),
        fn run(self: @This()) void {
            const result = self.cache.lookup(self.cache_key, 4, 1, 8, true) catch {
                self.failed.store(true, .release);
                return;
            };
            switch (result) {
                .fill => |entry| {
                    _ = self.fills.fetchAdd(1, .acq_rel);
                    @memset(entry.host16, 0.25);
                    self.cache.publish(entry);
                    self.cache.release(entry);
                },
                .hit => |entry| {
                    _ = self.hits.fetchAdd(1, .acq_rel);
                    self.cache.release(entry);
                },
                .bypass => {
                    _ = self.bypasses.fetchAdd(1, .acq_rel);
                },
            }
        }
    };
    var fills = std.atomic.Value(usize).init(0);
    var hits = std.atomic.Value(usize).init(0);
    var bypasses = std.atomic.Value(usize).init(0);
    var failed = std.atomic.Value(bool).init(false);
    const work = Work{ .cache = &cache, .cache_key = Cache.key(&.{ 21, 22, 23, 24 }, 1, 8), .fills = &fills, .hits = &hits, .bypasses = &bypasses, .failed = &failed };
    const first = try std.Thread.spawn(.{}, Work.run, .{work});
    var peer: ?std.Thread = null;
    var joined = false;
    defer if (!joined) {
        probe.release_claim.store(true, .release);
        first.join();
        if (peer) |thread| thread.join();
    };
    for (0..100000) |_| {
        if (probe.entered.load(.acquire)) break;
        platform.time.yieldBriefly();
    }
    try std.testing.expect(probe.entered.load(.acquire));
    peer = try std.Thread.spawn(.{}, Work.run, .{work});
    for (0..100000) |_| {
        if (cache.snapshot().waits != 0) break;
        platform.time.yieldBriefly();
    }
    try std.testing.expect(cache.snapshot().waits != 0);
    probe.release_claim.store(true, .release);
    first.join();
    peer.?.join();
    joined = true;
    try std.testing.expect(!failed.load(.acquire) and !probe.violation.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), fills.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), hits.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), bypasses.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), probe.reserve_calls.load(.acquire));
    cache.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe.releases.load(.acquire));
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "laya raw pressure clear and same-size replacement invalidate cold metadata before publication" {
    const allocator = std.testing.allocator;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var cache = Cache.init(allocator, 4096);
    defer cache.deinit();
    cache.setPrecision(.f16);
    cache.configureAdmission(.{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 4096 }, .device = false });
    const first_key = Cache.key(&.{ 31, 32, 33, 34 }, 1, 8);
    const second_key = Cache.key(&.{ 41, 42, 43, 44 }, 1, 8);
    const miss_key = Cache.key(&.{ 51, 52, 53, 54 }, 1, 8);
    const first = try fillFor(&cache, first_key, 4, 1, 8, 0.25);
    cache.release(first);
    var stale = try cache.snapshotCold(miss_key);
    defer stale.finishOutsideExecution(&cache, .{ .outside_execution = true }) catch @panic("raw pressure snapshot unwind");
    const old_capacity = stale.entries.capacity;
    const old_revision = stale.revision;
    const charged = controller.snapshot();
    try std.testing.expectEqual(@as(usize, 128), cache.reclaimOneDeferringPhysical());
    try std.testing.expect(cache.retirementPending());
    try std.testing.expectEqual(@as(usize, 0), cache.snapshot().entries);
    // Detachment does not refund either the still-live raw payload or array.
    try std.testing.expectEqualDeep(charged, controller.snapshot());
    try std.testing.expectError(error.TrunkCacheChangedBeforeExecution, cache.validateCold(&stale));
    try cache.drainRetiredOutsideExecution(.{ .outside_execution = true });
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
    const second = try fillFor(&cache, second_key, 4, 1, 8, 0.5);
    cache.release(second);
    // The actual replacement has exactly the same dimensions and capacity.
    // Its allocator may reuse every old address: revision still forbids ABA.
    try std.testing.expectEqual(old_capacity, cache.entries.capacity);
    try std.testing.expect(cache.revision > old_revision);
    try std.testing.expectError(error.TrunkCacheChangedBeforeExecution, cache.validateCold(&stale));
    var fresh = try cache.snapshotCold(second_key);
    defer fresh.finishOutsideExecution(&cache, .{ .outside_execution = true }) catch @panic("fresh pressure snapshot unwind");
    try cache.validateCold(&fresh);
    try std.testing.expectEqual(@as(usize, 0), cache.reclaimOneDeferringPhysical()); // actual Entry pin
    try stale.finishOutsideExecution(&cache, .{ .outside_execution = true });
}

test "laya metadata revision exhaustion stays sticky through real raw mutation" {
    var cache = Cache.init(std.testing.allocator, 4096);
    defer cache.deinit();
    const cache_key = Cache.key(&.{ 61, 62, 63, 64 }, 1, 8);
    var prior = try cache.snapshotCold(cache_key);
    defer prior.finishOutsideExecution(&cache, .{ .outside_execution = true }) catch @panic("revision snapshot unwind");
    cache.revision = std.math.maxInt(u64) - 1; // fixture-only exhaustion injection
    cache.setPrecision(.f32);
    cache.setLimit(4096, 1);
    try std.testing.expectEqual(std.math.maxInt(u64), cache.revision);
    try std.testing.expectError(error.TrunkCacheMetadataIdentityExhausted, cache.snapshotCold(cache_key));
    try std.testing.expectError(error.TrunkCacheChangedBeforeExecution, cache.validateCold(&prior));
    const entry = try fillFor(&cache, cache_key, 4, 1, 8, 0.5);
    cache.release(entry); // legacy raw cache remains operational, no wrap/reset
    try std.testing.expectEqual(std.math.maxInt(u64), cache.revision);
    try std.testing.expectError(error.TrunkCacheMetadataIdentityExhausted, cache.snapshotCold(cache_key));
}
