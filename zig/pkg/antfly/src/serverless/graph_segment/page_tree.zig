// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Immutable, content-addressed ordered pages. Storage identities never depend
//! on graph-wide ordinals. A sorted batch rewrites each affected subtree once;
//! unchanged subtrees remain reachable by the same digest. This module does not
//! publish roots: the enclosing manifest CAS is the only visibility boundary.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;

// Ordinary pages target 32 KiB. A larger hard cap admits indivisible long
// identities (stored edge types alone may be 64 KiB) without changing normal
// update amplification. Branch packing always makes progress with >=2 entries.
pub const max_page_bytes = 1024 * 1024;
pub const target_page_bytes = 32 * 1024;
pub const bulk_target_page_bytes = 256 * 1024;
// Leave room for an ordered u64 prefix without reducing the accepted document
// identifier size in the document facts index.
pub const max_key_bytes = 256 * 1024 + 8;
pub const max_record_bytes = max_page_bytes / 2 - header_bytes;
pub const max_height = 16;
pub const max_aggregate_bytes = 256;
const header_bytes = 56;
const ref_bytes = 61;
const magic = "AFGPT003";

pub const Ref = struct {
    digest: [32]u8,
    attempt: [16]u8 = @splat(0),
    bytes: u32,
    height: u8,
    records: u64,

    pub fn eql(a: Ref, b: Ref) bool {
        return a.bytes == b.bytes and a.height == b.height and a.records == b.records and
            std.mem.eql(u8, &a.digest, &b.digest) and std.mem.eql(u8, &a.attempt, &b.attempt);
    }

    pub fn validate(self: Ref) !void {
        if (self.bytes < header_bytes or self.bytes > max_page_bytes or
            self.height > max_height or self.records == 0) return error.InvalidGraphPage;
    }
};

/// Borrowed, operation-scoped capabilities. get must bound its allocation by
/// ref.bytes. The tree authenticates untrusted bytes; decoded leases may reuse
/// an earlier complete validation under the exact reference and domain.
/// put must durably persist exactly these bytes before returning. check is also
/// called between CPU batches, so cancellation is not dependent on storage I/O.
pub const Store = struct {
    /// Namespace reclamation domain is authenticated in every page, preventing
    /// content deduplication from coupling independent namespace collectors.
    domain: [32]u8 = @splat(0),
    attempt: [16]u8 = @splat(0),
    ptr: *anyopaque,
    get: *const fn (*anyopaque, Allocator, Ref) anyerror![]u8,
    /// Optional immutable decoded-page lease. The provider must authenticate
    /// domain, full reference and structure using readPage on a plain store.
    /// Authority and per-operation read admission must be checked on every hit.
    decoded_page: ?struct {
        ptr: *anyopaque,
        read: *const fn (*anyopaque, Allocator, Ref) anyerror!Page,
    } = null,
    put: *const fn (*anyopaque, Ref, []const u8) anyerror!void,
    /// Persist the entire bounded batch before returning. On failure no root
    /// escapes the private sorted builder; scoped orphans remain collectible.
    put_many: ?*const fn (*anyopaque, []const Write) anyerror!void = null,
    check: *const fn (*anyopaque) anyerror!void,
    /// Pure bounded reduction, authenticated in the page and parent routing entry.
    /// Existing trees retain their original wire representation when absent.
    reduce: ?*const fn (Allocator, []const Entry, u8) anyerror![]u8 = null,
};
pub const Write = struct { ref: Ref, bytes: []const u8 };
pub const max_write_batch = 8;

/// Small operation-local immutable page cache. It is a capability wrapper, not
/// mutable state installed on the shared artifact-store owner. Planning many
/// touched sources can reuse routing pages without a GET per source per level.
pub const Cache = struct {
    alloc: Allocator,
    underlying: Store,
    slots: [16]?Slot = @splat(null),
    clock: u64 = 0,
    bytes: usize = 0,
    max_bytes: usize = 512 * 1024,

    const Slot = struct { ref: Ref, bytes: []u8, used: u64 };

    pub fn deinit(self: *Cache) void {
        for (&self.slots) |*slot| if (slot.*) |value| {
            self.alloc.free(value.bytes);
            slot.* = null;
        };
        self.bytes = 0;
    }

    pub fn store(self: *Cache) Store {
        return .{ .domain = self.underlying.domain, .attempt = self.underlying.attempt, .ptr = self, .get = get, .put = put, .check = check, .reduce = self.underlying.reduce };
    }

    fn check(ptr: *anyopaque) !void {
        const self: *Cache = @ptrCast(@alignCast(ptr));
        try self.underlying.check(self.underlying.ptr);
    }

    fn get(ptr: *anyopaque, alloc: Allocator, ref: Ref) ![]u8 {
        const self: *Cache = @ptrCast(@alignCast(ptr));
        try check(ptr);
        self.clock +|= 1;
        for (&self.slots) |*slot| if (slot.*) |*value| {
            if (value.ref.eql(ref)) {
                value.used = self.clock;
                return alloc.dupe(u8, value.bytes);
            }
        };
        const bytes = try self.underlying.get(self.underlying.ptr, alloc, ref);
        errdefer alloc.free(bytes);
        if (bytes.len > self.max_bytes) return bytes;
        var digest: [32]u8 = undefined;
        Sha256.hash(bytes, &digest, .{});
        if (bytes.len != ref.bytes or !std.mem.eql(u8, &digest, &ref.digest)) return error.ArtifactIntegrityMismatch;
        const owned = try self.alloc.dupe(u8, bytes);
        errdefer self.alloc.free(owned);
        while (true) {
            var free: ?usize = null;
            var oldest: ?usize = null;
            for (self.slots, 0..) |slot, i| {
                if (slot) |value| {
                    if (oldest == null or value.used < self.slots[oldest.?].?.used) oldest = i;
                } else free = i;
            }
            if (free != null and bytes.len <= self.max_bytes - self.bytes) {
                self.slots[free.?] = .{ .ref = ref, .bytes = owned, .used = self.clock };
                self.bytes += bytes.len;
                return bytes;
            }
            const index = oldest orelse return error.InvalidGraphPage;
            const evicted = self.slots[index].?;
            self.bytes -= evicted.bytes.len;
            self.alloc.free(evicted.bytes);
            self.slots[index] = null;
        }
    }

    fn put(ptr: *anyopaque, ref: Ref, bytes: []const u8) !void {
        const self: *Cache = @ptrCast(@alignCast(ptr));
        try self.underlying.put(self.underlying.ptr, ref, bytes);
    }
};

pub const Mutation = struct {
    key: []const u8,
    /// null deletes; an empty slice is a present, empty value.
    value: ?[]const u8,
};

pub const Entry = struct {
    key: []const u8,
    value: []const u8 = "",
    child: ?Ref = null,
    aggregate: []const u8 = "",

    fn size(self: Entry) usize {
        return 8 + self.key.len + if (self.child != null) ref_bytes + self.aggregate.len else self.value.len;
    }
};

pub const Page = struct {
    bytes: []const u8,
    entries: []const Entry,
    ref: Ref,
    aggregate: []const u8 = "",
    lease: ?struct {
        owner: *anyopaque,
        item: *anyopaque,
        release: *const fn (*anyopaque, *anyopaque) void,
    } = null,

    pub fn deinit(self: *Page, alloc: Allocator) void {
        if (self.lease) |lease| {
            lease.release(lease.owner, lease.item);
        } else {
            alloc.free(self.entries);
            alloc.free(self.bytes);
        }
        self.* = undefined;
    }
};

const Child = struct {
    first: []u8,
    ref: Ref,
    aggregate: []u8,

    pub fn deinit(self: Child, alloc: Allocator) void {
        alloc.free(self.first);
        alloc.free(self.aggregate);
    }
};

const Children = std.ArrayListUnmanaged(Child);

fn freeChildren(alloc: Allocator, children: *Children) void {
    for (children.items) |child| child.deinit(alloc);
    children.deinit(alloc);
}

fn appendChild(alloc: Allocator, children: *Children, first: []const u8, ref: Ref, aggregate: []const u8) !void {
    const owned = try alloc.dupe(u8, first);
    errdefer alloc.free(owned);
    const reduced = try alloc.dupe(u8, aggregate);
    errdefer alloc.free(reduced);
    try children.append(alloc, .{ .first = owned, .ref = ref, .aggregate = reduced });
}

fn readU32(bytes: []const u8, offset: usize) u32 {
    return std.mem.readInt(u32, bytes[offset..][0..4], .little);
}

fn readU64(bytes: []const u8, offset: usize) u64 {
    return std.mem.readInt(u64, bytes[offset..][0..8], .little);
}

fn writeU32(bytes: []u8, offset: usize, value: usize) void {
    std.mem.writeInt(u32, bytes[offset..][0..4], @intCast(value), .little);
}

fn writeU64(bytes: []u8, offset: usize, value: u64) void {
    std.mem.writeInt(u64, bytes[offset..][0..8], value, .little);
}

fn less(a: []const u8, b: []const u8) bool {
    return std.mem.order(u8, a, b) == .lt;
}

/// An independently owned authenticated page. Callers may retain its immutable
/// bytes/entry views in a bounded decode cache, under the full ref and domain.
pub fn readPage(alloc: Allocator, store: Store, ref: Ref) !Page {
    return load(alloc, store, ref);
}

fn load(alloc: Allocator, store: Store, ref: Ref) !Page {
    try store.check(store.ptr);
    try ref.validate();
    if (store.decoded_page) |cache| return cache.read(cache.ptr, alloc, ref);
    const bytes = try store.get(store.ptr, alloc, ref);
    errdefer alloc.free(bytes);
    if (bytes.len != ref.bytes) return error.InvalidGraphPage;
    var digest: [32]u8 = undefined;
    Sha256.hash(bytes, &digest, .{});
    if (!std.mem.eql(u8, &digest, &ref.digest)) return error.ArtifactIntegrityMismatch;
    if (!std.mem.eql(u8, bytes[24..56], &store.domain)) return error.GraphPageDomainMismatch;
    if (!std.mem.eql(u8, bytes[0..8], magic) or bytes[8] != ref.height or
        bytes[9] > 1 or readU64(bytes, 16) != ref.records)
        return error.InvalidGraphPage;
    const aggregate_len = std.mem.readInt(u16, bytes[10..12], .little);
    if (aggregate_len > max_aggregate_bytes or aggregate_len > bytes.len - header_bytes or
        (bytes[9] == 0 and aggregate_len != 0) or (bytes[9] == 1 and aggregate_len == 0)) return error.InvalidGraphPage;
    const aggregate = bytes[header_bytes..][0..aggregate_len];
    const count = readU32(bytes, 12);
    if (count == 0 or count > (bytes.len - header_bytes) / 9) return error.InvalidGraphPage;
    const entries = try alloc.alloc(Entry, count);
    errdefer alloc.free(entries);
    var offset: usize = header_bytes + aggregate_len;
    var records: u64 = 0;
    for (entries, 0..) |*entry, i| {
        if (offset > bytes.len or bytes.len - offset < 8) return error.InvalidGraphPage;
        const key_len = readU32(bytes, offset);
        const value_len = readU32(bytes, offset + 4);
        offset += 8;
        if (key_len == 0 or key_len > max_key_bytes or key_len > bytes.len - offset)
            return error.InvalidGraphPage;
        const key = bytes[offset..][0..key_len];
        offset += key_len;
        if (i != 0 and !less(entries[i - 1].key, key)) return error.InvalidGraphPage;
        if (value_len > bytes.len - offset) return error.InvalidGraphPage;
        const value = bytes[offset..][0..value_len];
        offset += value_len;
        entry.* = .{ .key = key };
        if (ref.height == 0) {
            entry.value = value;
            records += 1;
        } else {
            if (value_len < ref_bytes or value_len > ref_bytes + max_aggregate_bytes or (bytes[9] == 0 and value_len != ref_bytes)) return error.InvalidGraphPage;
            const child: Ref = .{
                .digest = value[0..32].*,
                .bytes = readU32(value, 32),
                .height = value[36],
                .records = readU64(value, 37),
                .attempt = value[45..61].*,
            };
            try child.validate();
            if (child.height + 1 != ref.height) return error.InvalidGraphPage;
            records = std.math.add(u64, records, child.records) catch return error.InvalidGraphPage;
            entry.child = child;
            entry.aggregate = value[ref_bytes..];
        }
        if (entry.size() > max_record_bytes) return error.InvalidGraphPage;
    }
    if (offset != bytes.len or records != ref.records) return error.InvalidGraphPage;
    return .{ .bytes = bytes, .entries = entries, .ref = ref, .aggregate = aggregate };
}

fn save(alloc: Allocator, store: Store, entries: []const Entry, height: u8, aggregate_out: *[]u8) !Ref {
    const page = try encode(alloc, store, entries, height, aggregate_out);
    defer alloc.free(page.bytes);
    errdefer {
        alloc.free(aggregate_out.*);
        aggregate_out.* = &.{};
    }
    try store.put(store.ptr, page.ref, page.bytes);
    return page.ref;
}

fn encode(alloc: Allocator, store: Store, entries: []const Entry, height: u8, aggregate_out: *[]u8) !Write {
    try store.check(store.ptr);
    if (entries.len == 0 or height > max_height) return error.InvalidGraphPage;
    const aggregate = if (store.reduce) |reduce| try reduce(alloc, entries, height) else try alloc.alloc(u8, 0);
    errdefer alloc.free(aggregate);
    if (aggregate.len > max_aggregate_bytes or (store.reduce != null and aggregate.len == 0)) return error.InvalidGraphPage;
    var size: usize = header_bytes + aggregate.len;
    var records: u64 = 0;
    for (entries) |entry| {
        size += entry.size() - (if (entry.child != null and store.reduce == null) entry.aggregate.len else 0);
        records = try std.math.add(u64, records, if (entry.child) |child| child.records else 1);
    }
    if (size > max_page_bytes) return error.GraphPageTooLarge;
    const bytes = try alloc.alloc(u8, size);
    errdefer alloc.free(bytes);
    @memcpy(bytes[0..8], magic);
    @memcpy(bytes[24..56], &store.domain);
    bytes[8] = height;
    bytes[9] = if (aggregate.len == 0) 0 else 1;
    std.mem.writeInt(u16, bytes[10..12], @intCast(aggregate.len), .little);
    writeU32(bytes, 12, entries.len);
    writeU64(bytes, 16, records);
    @memcpy(bytes[header_bytes..][0..aggregate.len], aggregate);
    var offset: usize = header_bytes + aggregate.len;
    for (entries) |entry| {
        writeU32(bytes, offset, entry.key.len);
        const child_aggregate = if (store.reduce != null) entry.aggregate else "";
        writeU32(bytes, offset + 4, if (entry.child != null) ref_bytes + child_aggregate.len else entry.value.len);
        offset += 8;
        @memcpy(bytes[offset..][0..entry.key.len], entry.key);
        offset += entry.key.len;
        if (entry.child) |child| {
            @memcpy(bytes[offset..][0..32], &child.digest);
            writeU32(bytes, offset + 32, child.bytes);
            bytes[offset + 36] = child.height;
            writeU64(bytes, offset + 37, child.records);
            @memcpy(bytes[offset + 45 ..][0..16], &child.attempt);
            @memcpy(bytes[offset + ref_bytes ..][0..child_aggregate.len], child_aggregate);
            offset += ref_bytes + child_aggregate.len;
        } else {
            @memcpy(bytes[offset..][0..entry.value.len], entry.value);
            offset += entry.value.len;
        }
    }
    var digest: [32]u8 = undefined;
    Sha256.hash(bytes, &digest, .{});
    const ref: Ref = .{ .digest = digest, .attempt = store.attempt, .bytes = @intCast(size), .height = height, .records = records };
    aggregate_out.* = aggregate;
    return .{ .ref = ref, .bytes = bytes };
}

/// Pack toward half-full pages, allowing one indivisible record above the
/// target. Split boundaries depend only on ordered records, not process state.
fn pack(alloc: Allocator, store: Store, entries: []const Entry, height: u8, out: *Children) !void {
    return packAt(alloc, store, entries, height, out, target_page_bytes);
}
fn packingTarget(bytes: usize) usize {
    return if (bytes > target_page_bytes) bulk_target_page_bytes else target_page_bytes;
}
/// Preserve the size class of bulk-built pages on local edits. Balance a split
/// across its outputs so a nearly full page does not create a tiny tail that
/// immediately makes occupancy repair rewrite another bulk sibling.
fn packAt(alloc: Allocator, store: Store, entries: []const Entry, height: u8, out: *Children, target: usize) !void {
    var payload: usize = 0;
    for (entries) |entry| payload += entry.size();
    const pages = @max(1, std.math.divCeil(usize, payload, target - header_bytes) catch unreachable);
    const balanced = if (target == target_page_bytes) target else header_bytes + (std.math.divCeil(usize, payload, pages) catch unreachable);
    var begin: usize = 0;
    while (begin < entries.len) {
        var end = begin;
        var bytes: usize = header_bytes;
        while (end < entries.len) : (end += 1) {
            const size = entries[end].size();
            if (end - begin >= (if (height == 0) @as(usize, 1) else 2) and bytes + size > balanced) break;
            bytes += size;
        }
        var aggregate: []u8 = undefined;
        const ref = try save(alloc, store, entries[begin..end], height, &aggregate);
        defer alloc.free(aggregate);
        try appendChild(alloc, out, entries[begin].key, ref, aggregate);
        begin = end;
    }
}

fn childEntries(alloc: Allocator, children: []const Child) ![]Entry {
    const entries = try alloc.alloc(Entry, children.len);
    for (entries, children) |*entry, child| entry.* = .{ .key = child.first, .child = child.ref, .aggregate = child.aggregate };
    return entries;
}

/// Repair adjacent underfull pages with bounded reads. This is essential for
/// deletion-heavy workloads: an immutable tree must not become a tombstone or
/// one-record-per-object overlay. Unchanged, sufficiently full pages are not read.
fn rebalance(alloc: Allocator, store: Store, children: *Children) !void {
    var i: usize = 0;
    while (i + 1 < children.items.len) {
        const a = children.items[i];
        const b = children.items[i + 1];
        // A quarter-page threshold leaves headroom for subsequent small edits.
        if (a.ref.bytes >= target_page_bytes / 2 and b.ref.bytes >= target_page_bytes / 2) {
            i += 1;
            continue;
        }
        var left = try load(alloc, store, a.ref);
        defer left.deinit(alloc);
        var right = try load(alloc, store, b.ref);
        defer right.deinit(alloc);
        if (a.ref.height != b.ref.height or !std.mem.eql(u8, a.first, left.entries[0].key) or
            !std.mem.eql(u8, b.first, right.entries[0].key) or !less(left.entries[left.entries.len - 1].key, right.entries[0].key))
            return error.InvalidGraphPage;
        const entries = try alloc.alloc(Entry, left.entries.len + right.entries.len);
        defer alloc.free(entries);
        @memcpy(entries[0..left.entries.len], left.entries);
        @memcpy(entries[left.entries.len..], right.entries);
        var replacement: Children = .empty;
        defer freeChildren(alloc, &replacement);
        // Their combined payload fits: merge without unnecessarily splitting
        // it back at the soft target. Otherwise redistribute via pack.
        const target = @max(packingTarget(left.bytes.len), packingTarget(right.bytes.len));
        if (left.bytes.len + right.bytes.len - header_bytes <= target) {
            var aggregate: []u8 = undefined;
            const ref = try save(alloc, store, entries, a.ref.height, &aggregate);
            defer alloc.free(aggregate);
            try appendChild(alloc, &replacement, entries[0].key, ref, aggregate);
        } else {
            try packAt(alloc, store, entries, a.ref.height, &replacement, target);
        }
        try children.ensureUnusedCapacity(alloc, replacement.items.len);
        a.deinit(alloc);
        b.deinit(alloc);
        _ = children.orderedRemove(i);
        _ = children.orderedRemove(i);
        try children.insertSlice(alloc, i, replacement.items);
        const count = replacement.items.len;
        replacement.clearRetainingCapacity(); // ownership transferred
        if (count > 1) i += count - 1;
    }
}

fn edit(alloc: Allocator, store: Store, prior: Ref, changes: []const Mutation, out: *Children, first: ?[]const u8, end_key: ?[]const u8) anyerror!void {
    var page = try load(alloc, store, prior);
    defer page.deinit(alloc);
    if (first) |key| if (!std.mem.eql(u8, key, page.entries[0].key)) return error.InvalidGraphPage;
    if (end_key) |key| if (!less(page.entries[page.entries.len - 1].key, key)) return error.InvalidGraphPage;
    if (prior.height == 0) {
        var entries: std.ArrayListUnmanaged(Entry) = .empty;
        defer entries.deinit(alloc);
        try entries.ensureTotalCapacity(alloc, page.entries.len + changes.len);
        var a: usize = 0;
        var b: usize = 0;
        while (a < page.entries.len or b < changes.len) {
            if ((a + b) % 1024 == 0) try store.check(store.ptr);
            if (b == changes.len or (a < page.entries.len and less(page.entries[a].key, changes[b].key))) {
                entries.appendAssumeCapacity(page.entries[a]);
                a += 1;
            } else {
                if (a < page.entries.len and std.mem.eql(u8, page.entries[a].key, changes[b].key)) a += 1;
                if (changes[b].value) |value| entries.appendAssumeCapacity(.{ .key = changes[b].key, .value = value });
                b += 1;
            }
        }
        if (entries.items.len == page.entries.len) {
            for (entries.items, page.entries) |next, old| {
                if (!std.mem.eql(u8, next.key, old.key) or !std.mem.eql(u8, next.value, old.value)) break;
            } else {
                try appendChild(alloc, out, page.entries[0].key, prior, page.aggregate);
                return;
            }
        }
        return packAt(alloc, store, entries.items, 0, out, packingTarget(prior.bytes));
    }
    var children: Children = .empty;
    defer freeChildren(alloc, &children);
    var begin: usize = 0;
    var changed = false;
    var repair_occupancy = false;
    for (page.entries, 0..) |entry, i| {
        var end = begin;
        while (end < changes.len and (i + 1 == page.entries.len or less(changes[end].key, page.entries[i + 1].key))) : (end += 1) {}
        if (end == begin) {
            try appendChild(alloc, &children, entry.key, entry.child.?, entry.aggregate);
        } else {
            const count = children.items.len;
            try edit(alloc, store, entry.child.?, changes[begin..end], &children, entry.key, if (i + 1 < page.entries.len) page.entries[i + 1].key else end_key);
            if (children.items.len != count + 1 or !children.items[count].ref.eql(entry.child.?)) {
                changed = true;
                // Value-only edits do not owe maintenance for an unrelated
                // underfull tail. Repair splits/removals and newly shrunken
                // pages; preserve every other immutable child by reference.
                if (children.items.len != count + 1) repair_occupancy = true else {
                    const next = children.items[count].ref;
                    const old = entry.child.?;
                    if (next.records != old.records or (next.bytes < target_page_bytes / 2 and next.bytes < old.bytes)) repair_occupancy = true;
                }
            }
        }
        begin = end;
    }
    if (!changed) return appendChild(alloc, out, page.entries[0].key, prior, page.aggregate);
    if (repair_occupancy) try rebalance(alloc, store, &children);
    const entries = try childEntries(alloc, children.items);
    defer alloc.free(entries);
    try packAt(alloc, store, entries, prior.height, out, packingTarget(prior.bytes));
}

/// Join strictly disjoint key ranges by rewriting only their boundary spines.
/// Overlapping ranges decline without writes so a reducer can merge equal keys.
/// Descendants retain their original attempt identities and GC reachability.
pub fn joinDisjoint(alloc: Allocator, store: Store, left: Ref, right: Ref) !?Ref {
    // Bounds cursors release their decoded leaves before allocating join
    // spines, so disjoint detection does not inflate the reduction workspace.
    const reversed = (try disjointOrder(alloc, store, left, right)) orelse return null;
    var children: Children = .empty;
    defer freeChildren(alloc, &children);
    try joinBoundary(alloc, store, if (reversed) right else left, if (reversed) left else right, &children);
    while (children.items.len > 1) {
        const height = children.items[0].ref.height + 1;
        if (height > max_height) return error.GraphPageTreeTooDeep;
        const entries = try childEntries(alloc, children.items);
        defer alloc.free(entries);
        var parents: Children = .empty;
        errdefer freeChildren(alloc, &parents);
        try pack(alloc, store, entries, height, &parents);
        freeChildren(alloc, &children);
        children = parents;
    }
    return children.items[0].ref;
}
fn disjointOrder(alloc: Allocator, store: Store, left: Ref, right: Ref) !?bool {
    var lf = try Cursor.init(alloc, store, left, "", null);
    defer lf.deinit();
    var rl = try Cursor.initReverse(alloc, store, right, "", null);
    defer rl.deinit();
    const first_left = (try lf.next()) orelse return error.InvalidGraphPage;
    const last_right = (try rl.next()) orelse return error.InvalidGraphPage;
    const reversed = less(last_right.key, first_left.key);
    if (!reversed) {
        var ll = try Cursor.initReverse(alloc, store, left, "", null);
        defer ll.deinit();
        var rf = try Cursor.init(alloc, store, right, "", null);
        defer rf.deinit();
        const last_left = (try ll.next()) orelse return error.InvalidGraphPage;
        const first_right = (try rf.next()) orelse return error.InvalidGraphPage;
        if (!less(last_left.key, first_right.key)) return null;
    }
    return reversed;
}
fn joinBoundary(alloc: Allocator, store: Store, left: Ref, right: Ref, out: *Children) anyerror!void {
    var lp = try load(alloc, store, left);
    defer lp.deinit(alloc);
    var rp = try load(alloc, store, right);
    defer rp.deinit(alloc);
    var boundary: Children = .empty;
    defer freeChildren(alloc, &boundary);
    var entries: []Entry = undefined;
    const height = @max(left.height, right.height);
    if (left.height == right.height) {
        entries = try alloc.alloc(Entry, lp.entries.len + rp.entries.len);
        @memcpy(entries[0..lp.entries.len], lp.entries);
        @memcpy(entries[lp.entries.len..], rp.entries);
    } else if (left.height > right.height) {
        try joinBoundary(alloc, store, lp.entries[lp.entries.len - 1].child.?, right, &boundary);
        entries = try alloc.alloc(Entry, lp.entries.len - 1 + boundary.items.len);
        @memcpy(entries[0 .. lp.entries.len - 1], lp.entries[0 .. lp.entries.len - 1]);
        for (boundary.items, entries[lp.entries.len - 1 ..]) |child, *entry| entry.* = .{ .key = child.first, .child = child.ref, .aggregate = child.aggregate };
    } else {
        try joinBoundary(alloc, store, left, rp.entries[0].child.?, &boundary);
        entries = try alloc.alloc(Entry, boundary.items.len + rp.entries.len - 1);
        for (boundary.items, entries[0..boundary.items.len]) |child, *entry| entry.* = .{ .key = child.first, .child = child.ref, .aggregate = child.aggregate };
        @memcpy(entries[boundary.items.len..], rp.entries[1..]);
    }
    defer alloc.free(entries);
    // Equal-height roots adopt their descendants without decoding row values;
    // unequal heights splice only the shared-height boundary and its ancestors.
    try packAt(alloc, store, entries, height, out, @max(packingTarget(left.bytes), packingTarget(right.bytes)));
}

/// Mutations must have strictly increasing, unique keys. Validate the entire
/// batch before uploading anything. The returned root owns no allocation and
/// can be atomically bound into a manifest; every prior root remains readable.
pub fn apply(alloc: Allocator, store: Store, prior: ?Ref, changes: []const Mutation) !?Ref {
    try store.check(store.ptr);
    if (prior) |ref| try ref.validate();
    for (changes, 0..) |change, i| {
        if (i % 1024 == 0) try store.check(store.ptr);
        if (change.key.len == 0 or change.key.len > max_key_bytes or
            (if (change.value) |value| value.len else 0) > max_record_bytes - 8 - change.key.len)
            return error.GraphPageRecordTooLarge;
        if (i != 0 and !less(changes[i - 1].key, change.key)) return error.UnsortedGraphPageMutations;
    }
    if (changes.len == 0) return prior;
    if (prior == null) {
        const Source = struct {
            remaining: []const Mutation,
            pub fn next(self: *@This()) !?Cursor.Record {
                while (self.remaining.len > 0) {
                    const change = self.remaining[0];
                    self.remaining = self.remaining[1..];
                    if (change.value) |value| return .{ .key = change.key, .value = value };
                }
                return null;
            }
        };
        var source: Source = .{ .remaining = changes };
        return buildSorted(alloc, store, &source);
    }
    var children: Children = .empty;
    defer freeChildren(alloc, &children);
    try edit(alloc, store, prior.?, changes, &children, null, null);
    while (children.items.len > 1) {
        const height = children.items[0].ref.height + 1;
        if (height > max_height) return error.GraphPageTreeTooDeep;
        const entries = try childEntries(alloc, children.items);
        defer alloc.free(entries);
        var parents: Children = .empty;
        errdefer freeChildren(alloc, &parents);
        try pack(alloc, store, entries, height, &parents);
        freeChildren(alloc, &children);
        children = parents;
    }
    if (children.items.len == 0) return null;
    var root = children.items[0].ref;
    // Collapse single-child roots after large deletions. Descendant heights are
    // authenticated and strictly decrease, so malformed objects cannot loop.
    while (root.height != 0) {
        var page = try load(alloc, store, root);
        defer page.deinit(alloc);
        if (page.entries.len != 1) break;
        root = page.entries[0].child.?;
    }
    return root;
}

/// Retain a strictly ordered subset whose membership the caller has already
/// authenticated against prior. Counts let a complete branch survive without
/// fetching any descendant pages. Empty branches are dropped without reads.
/// This is intentionally separate from apply: unknown keys are mutations,
/// never evidence that an old branch is wholly retained.
pub fn retainKnown(alloc: Allocator, store: Store, prior: ?Ref, keys: []const []const u8) !?Ref {
    try store.check(store.ptr);
    for (keys, 0..) |key, i| if (key.len == 0 or (i != 0 and !less(keys[i - 1], key))) return error.UnsortedGraphPageMutations;
    const root = prior orelse return if (keys.len == 0) null else error.InvalidGraphPage;
    try root.validate();
    if (keys.len > root.records) return error.InvalidGraphPage;
    if (keys.len == root.records) return root;
    if (keys.len == 0) return null;
    var children: Children = .empty;
    defer freeChildren(alloc, &children);
    try retainPage(alloc, store, root, keys, &children);
    if (children.items.len != 1) return error.InvalidGraphPage;
    var result = children.items[0].ref;
    while (result.height != 0) {
        var page = try load(alloc, store, result);
        defer page.deinit(alloc);
        if (page.entries.len != 1) break;
        result = page.entries[0].child.?;
    }
    return result;
}
fn retainPage(alloc: Allocator, store: Store, ref: Ref, keys: []const []const u8, out: *Children) anyerror!void {
    try store.check(store.ptr);
    if (keys.len == 0) return;
    if (keys.len > ref.records) return error.InvalidGraphPage;
    if (keys.len == ref.records) {
        if (store.reduce == null) return appendChild(alloc, out, keys[0], ref, "");
        var retained = try load(alloc, store, ref);
        defer retained.deinit(alloc);
        return appendChild(alloc, out, keys[0], ref, retained.aggregate);
    }
    var page = try load(alloc, store, ref);
    defer page.deinit(alloc);
    if (ref.height == 0) {
        var entries: std.ArrayList(Entry) = .empty;
        defer entries.deinit(alloc);
        var next: usize = 0;
        for (page.entries) |entry| if (next < keys.len and std.mem.eql(u8, entry.key, keys[next])) {
            try entries.append(alloc, entry);
            next += 1;
        };
        if (next != keys.len) return error.InvalidGraphPage;
        return packAt(alloc, store, entries.items, 0, out, packingTarget(ref.bytes));
    }
    var children: Children = .empty;
    defer freeChildren(alloc, &children);
    var begin: usize = 0;
    for (page.entries, 0..) |entry, i| {
        var end = begin;
        while (end < keys.len and (i + 1 == page.entries.len or less(keys[end], page.entries[i + 1].key))) : (end += 1) {}
        try retainPage(alloc, store, entry.child.?, keys[begin..end], &children);
        begin = end;
    }
    try rebalance(alloc, store, &children);
    const entries = try childEntries(alloc, children.items);
    defer alloc.free(entries);
    try pack(alloc, store, entries, ref.height, out);
}

/// Construct an initial tree from a strictly ordered stream. Only one pending
/// page per height and at most max_write_batch encoded pages are retained.
/// Leaf records and routing entries are released when their page is encoded;
/// encoded pages are released after their whole durable batch completes. The source's borrowed
/// record may be invalidated by its next call. Like incremental apply, callers
/// must account for orphan uploads if source, allocation or storage fails.
pub fn buildSorted(alloc: Allocator, store: Store, source: anytype) !?Ref {
    return buildSortedWithOptions(alloc, store, source, .{});
}

pub const BuildOptions = struct {
    target_bytes: usize = target_page_bytes,
};

/// Bulk publication may pack larger pages to reduce object-store requests.
/// Decoders, record limits and the maximum durable batch remain unchanged.
pub fn buildSortedWithOptions(alloc: Allocator, store: Store, source: anytype, options: BuildOptions) !?Ref {
    if (options.target_bytes < header_bytes + 8 or options.target_bytes > max_page_bytes) return error.InvalidGraphPageSize;
    var builder: SortedBuilder = .{ .alloc = alloc, .store = store, .target_bytes = options.target_bytes };
    defer builder.deinit();
    while (try source.next()) |record| try builder.add(record.key, record.value);
    return builder.finish();
}

pub const SortedBuilder = struct {
    alloc: Allocator,
    store: Store,
    target_bytes: usize = target_page_bytes,
    previous: ?[]u8 = null,
    previous_is_upper: bool = false,
    closed: bool = false,
    pending: [max_write_batch]Write = undefined,
    pending_count: usize = 0,
    // The extra slot holds the final root ref, never another persisted page.
    levels: [max_height + 2]Level = @splat(.{}),

    const Level = struct {
        entries: std.ArrayListUnmanaged(Entry) = .empty,
        bytes: usize = header_bytes,

        fn clear(self: *Level, alloc: Allocator) void {
            for (self.entries.items) |entry| {
                alloc.free(entry.key);
                if (entry.child == null) alloc.free(entry.value);
                alloc.free(entry.aggregate);
            }
            self.entries.clearRetainingCapacity();
            self.bytes = header_bytes;
        }
    };

    pub fn add(self: *SortedBuilder, key: []const u8, value: []const u8) !void {
        try self.store.check(self.store.ptr);
        if (key.len == 0 or key.len > max_key_bytes or value.len > max_record_bytes - 8 - key.len) return error.GraphPageRecordTooLarge;
        try self.checkOrder(key);
        const previous = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(previous);
        try self.append(0, .{ .key = key, .value = value });
        if (self.previous) |old| self.alloc.free(old);
        self.previous = previous;
        self.previous_is_upper = false;
    }
    fn checkOrder(self: *SortedBuilder, key: []const u8) !void {
        if (self.closed) return error.UnsortedGraphPageMutations;
        if (self.previous) |previous| if (if (self.previous_is_upper) less(key, previous) else !less(previous, key)) return error.UnsortedGraphPageMutations;
    }
    /// Adopt a verified, complete half-open routing interval. Flush preceding
    /// lower levels before splicing the ref at its native height. Descendants
    /// keep their original attempts and authenticated reduction summaries.
    fn addSubtree(self: *SortedBuilder, first: []const u8, upper: ?[]const u8, ref: Ref, aggregate: []const u8) !void {
        try self.store.check(self.store.ptr);
        try ref.validate();
        try self.checkOrder(first);
        if (upper) |bound| if (!less(first, bound)) return error.InvalidGraphPage;
        const previous = if (upper) |bound| try self.alloc.dupe(u8, bound) else null;
        errdefer if (previous) |bytes| self.alloc.free(bytes);
        for (0..@as(usize, ref.height) + 1) |height| if (self.levels[height].entries.items.len != 0) try self.flush(height);
        try self.append(@as(usize, ref.height) + 1, .{ .key = first, .child = ref, .aggregate = aggregate });
        if (self.previous) |old| self.alloc.free(old);
        self.previous = previous;
        self.previous_is_upper = true;
        self.closed = upper == null;
    }
    pub fn finish(self: *SortedBuilder) !?Ref {
        try self.store.check(self.store.ptr);
        for (0..self.levels.len) |height| {
            const level = &self.levels[height];
            if (level.entries.items.len == 0) continue;
            if (height > 0 and level.entries.items.len == 1) {
                var higher_pending = false;
                for (self.levels[height + 1 ..]) |higher| higher_pending = higher_pending or higher.entries.items.len > 0;
                if (!higher_pending) {
                    try self.persist();
                    return level.entries.items[0].child.?;
                }
            }
            try self.flush(height);
        }
        return null;
    }

    pub fn deinit(self: *SortedBuilder) void {
        for (self.pending[0..self.pending_count]) |page| self.alloc.free(page.bytes);
        if (self.previous) |previous| self.alloc.free(previous);
        for (&self.levels) |*level| {
            level.clear(self.alloc);
            level.entries.deinit(self.alloc);
        }
    }

    fn append(self: *SortedBuilder, height: usize, entry: Entry) anyerror!void {
        if (height >= self.levels.len) return error.GraphPageTreeTooDeep;
        const level = &self.levels[height];
        const minimum: usize = if (height == 0) 1 else 2;
        if (level.entries.items.len >= minimum and level.bytes + entry.size() > self.target_bytes)
            try self.flush(height);
        const key = try self.alloc.dupe(u8, entry.key);
        errdefer self.alloc.free(key);
        const value = if (entry.child == null) try self.alloc.dupe(u8, entry.value) else "";
        errdefer if (entry.child == null) self.alloc.free(value);
        const aggregate = try self.alloc.dupe(u8, entry.aggregate);
        errdefer self.alloc.free(aggregate);
        try level.entries.append(self.alloc, .{ .key = key, .value = value, .child = entry.child, .aggregate = aggregate });
        level.bytes += entry.size();
    }

    fn flush(self: *SortedBuilder, height: usize) anyerror!void {
        if (height > max_height) return error.GraphPageTreeTooDeep;
        const level = &self.levels[height];
        var aggregate: []u8 = &.{};
        defer self.alloc.free(aggregate);
        const ref = if (self.store.put_many != null) queued: {
            const page = try encode(self.alloc, self.store, level.entries.items, @intCast(height), &aggregate);
            self.pending[self.pending_count] = page;
            self.pending_count += 1;
            if (self.pending_count == self.pending.len) try self.persist();
            break :queued page.ref;
        } else try save(self.alloc, self.store, level.entries.items, @intCast(height), &aggregate);
        try self.append(height + 1, .{ .key = level.entries.items[0].key, .child = ref, .aggregate = aggregate });
        level.clear(self.alloc);
    }

    fn persist(self: *SortedBuilder) !void {
        if (self.pending_count == 0) return;
        try self.store.check(self.store.ptr);
        try self.store.put_many.?(self.store.ptr, self.pending[0..self.pending_count]);
        for (self.pending[0..self.pending_count]) |page| self.alloc.free(page.bytes);
        self.pending_count = 0;
    }
};

/// Merge sorted immutable trees while adopting nonoverlapping routing ranges.
/// The reducer is called only for equal keys; the supplied scratch arena is
/// reset after each record. Memory is bounded by two ancestor paths and one
/// sorted-builder page per height, independent of the number of groups.
pub fn mergeSorted(alloc: Allocator, store: Store, left: Ref, right: Ref, reducer: anytype) !Ref {
    var l = try MergeFrontier.init(alloc, store, left);
    defer l.deinit();
    var r = try MergeFrontier.init(alloc, store, right);
    defer r.deinit();
    var builder: SortedBuilder = .{ .alloc = alloc, .store = store };
    defer builder.deinit();
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    while (true) {
        try store.check(store.ptr);
        const lp = l.peek();
        const rp = r.peek();
        if (lp == null and rp == null) break;
        const order = if (lp == null) std.math.Order.gt else if (rp == null) std.math.Order.lt else std.mem.order(u8, lp.?.first, rp.?.first);
        if (order == .eq) {
            if (lp.?.ref != null) {
                try l.expand();
                continue;
            }
            if (rp.?.ref != null) {
                try r.expand();
                continue;
            }
            _ = scratch.reset(.free_all);
            try builder.add(lp.?.first, try reducer.merge(scratch.allocator(), lp.?.value, rp.?.value));
            l.consume();
            r.consume();
        } else {
            const stream = if (order == .lt) &l else &r;
            const item = if (order == .lt) lp.? else rp.?;
            const other = if (order == .lt) rp else lp;
            if (item.ref) |ref| {
                const disjoint = other == null or (if (item.end) |end| !less(other.?.first, end) else false);
                if (!disjoint) {
                    try stream.expand();
                    continue;
                }
                try builder.addSubtree(item.first, item.end, ref, item.aggregate);
            } else try builder.add(item.first, item.value);
            stream.consume();
        }
    }
    return (try builder.finish()) orelse error.InvalidGraphPage;
}
const MergeFrontier = struct {
    alloc: Allocator,
    store: Store,
    frames: [max_height + 1]Frame = undefined,
    depth: usize = 0,
    const Frame = struct { page: Page, next: usize = 0, end: ?[]const u8, whole: bool = true };
    const Item = struct { first: []const u8, end: ?[]const u8, ref: ?Ref, value: []const u8 = "", aggregate: []const u8 = "" };
    fn init(alloc: Allocator, store: Store, root: Ref) !MergeFrontier {
        var self: MergeFrontier = .{ .alloc = alloc, .store = store };
        self.frames[0] = .{ .page = try load(alloc, store, root), .end = null };
        self.depth = 1;
        return self;
    }
    fn deinit(self: *MergeFrontier) void {
        for (self.frames[0..self.depth]) |*frame| frame.page.deinit(self.alloc);
        self.depth = 0;
    }
    fn peek(self: *MergeFrontier) ?Item {
        while (self.depth != 0) {
            const frame = &self.frames[self.depth - 1];
            if (frame.whole) return .{ .first = frame.page.entries[0].key, .end = frame.end, .ref = frame.page.ref, .aggregate = frame.page.aggregate };
            if (frame.next == frame.page.entries.len) {
                frame.page.deinit(self.alloc);
                self.depth -= 1;
                continue;
            }
            const entry = frame.page.entries[frame.next];
            return .{ .first = entry.key, .end = if (frame.next + 1 < frame.page.entries.len) frame.page.entries[frame.next + 1].key else frame.end, .ref = entry.child, .value = entry.value, .aggregate = entry.aggregate };
        }
        return null;
    }
    fn consume(self: *MergeFrontier) void {
        const frame = &self.frames[self.depth - 1];
        if (frame.whole) {
            frame.page.deinit(self.alloc);
            self.depth -= 1;
        } else frame.next += 1;
    }
    fn expand(self: *MergeFrontier) !void {
        const frame = &self.frames[self.depth - 1];
        if (frame.whole) {
            frame.whole = false;
            return;
        }
        const item = self.peek().?;
        if (self.depth == self.frames.len) return error.InvalidGraphPage;
        var page = try load(self.alloc, self.store, item.ref.?);
        errdefer page.deinit(self.alloc);
        if (!std.mem.eql(u8, item.first, page.entries[0].key)) return error.InvalidGraphPage;
        if (item.end) |end| if (!less(page.entries[page.entries.len - 1].key, end)) return error.InvalidGraphPage;
        frame.next += 1;
        self.frames[self.depth] = .{ .page = page, .end = item.end };
        self.depth += 1;
    }
};

/// Resolve a sorted membership batch by visiting each affected subtree once.
/// Unrelated leaves are never scanned; all pages retain normal integrity and
/// parent separator validation. Duplicate query keys are supported.
pub fn containsMany(alloc: Allocator, store: Store, root: ?Ref, keys: []const []const u8, found: []bool) !void {
    if (keys.len != found.len) return error.InvalidGraphPage;
    for (keys, 0..) |key, index| if (index != 0 and less(key, keys[index - 1])) return error.InvalidGraphPage;
    @memset(found, false);
    if (root) |ref| if (keys.len != 0) try containsPage(alloc, store, ref, keys, found, null, null);
}
fn containsPage(alloc: Allocator, store: Store, ref: Ref, keys: []const []const u8, found: []bool, first: ?[]const u8, end: ?[]const u8) !void {
    var page = try load(alloc, store, ref);
    defer page.deinit(alloc);
    if (first) |key| if (!std.mem.eql(u8, key, page.entries[0].key)) return error.InvalidGraphPage;
    if (end) |key| if (!less(page.entries[page.entries.len - 1].key, key)) return error.InvalidGraphPage;
    var position: usize = 0;
    if (ref.height == 0) {
        for (keys, found) |key, *present| {
            while (position < page.entries.len and less(page.entries[position].key, key)) position += 1;
            present.* = position < page.entries.len and std.mem.eql(u8, page.entries[position].key, key);
        }
        return;
    }
    while (position < keys.len and less(keys[position], page.entries[0].key)) position += 1;
    for (page.entries, 0..) |entry, index| {
        const upper = if (index + 1 < page.entries.len) page.entries[index + 1].key else end;
        const begin = position;
        while (position < keys.len and (upper == null or less(keys[position], upper.?))) position += 1;
        if (position != begin) try containsPage(alloc, store, entry.child.?, keys[begin..position], found[begin..position], entry.key, upper);
        if (position == keys.len) break;
    }
}

/// Best-first traversal of authenticated reductions. Missing reductions have
/// an infinite bound and are visited exactly. Only strictly losing subtrees
/// are pruned, so equal-bound records remain available for stable tie breaks.
pub const BestFirst = struct {
    pub const Bound = struct {
        ptr: *anyopaque,
        value: *const fn (*anyopaque, []const u8, ?u64) anyerror!f32,
    };
    const Node = struct {
        score: f32,
        first: ?[]const u8 = null,
        end: ?[]const u8 = null,
        ref: ?Ref = null,
        value: ?[]const u8 = null,
        fn compare(_: void, left: Node, right: Node) std.math.Order {
            return std.math.order(right.score, left.score);
        }
        fn bytes(self: Node) usize {
            return (if (self.first) |v| v.len else 0) + (if (self.end) |v| v.len else 0) + (if (self.value) |v| v.len else 0);
        }
        fn deinit(self: Node, a: Allocator) void {
            if (self.first) |v| a.free(v);
            if (self.end) |v| a.free(v);
            if (self.value) |v| a.free(v);
        }
    };
    alloc: Allocator,
    store: Store,
    bound: Bound,
    lower: []const u8,
    upper: ?[]const u8,
    heap: std.PriorityQueue(Node, void, Node.compare) = .initContext({}),
    retained: usize = 0,
    returned: ?Node = null,
    pruned: bool = false,
    pruned_nodes: u64 = 0,
    max_retained: usize = 4 * 1024 * 1024,
    max_nodes: usize = 8192,

    pub fn init(a: Allocator, store: Store, root: ?Ref, lower: []const u8, upper: ?[]const u8, bound: Bound) !BestFirst {
        var self: BestFirst = .{ .alloc = a, .store = store, .lower = lower, .upper = upper, .bound = bound };
        if (root) |ref| try self.heap.push(a, .{ .ref = ref, .score = std.math.inf(f32) });
        return self;
    }
    pub fn deinit(self: *BestFirst) void {
        if (self.returned) |node| node.deinit(self.alloc);
        while (self.heap.pop()) |node| node.deinit(self.alloc);
        self.heap.deinit(self.alloc);
    }
    fn add(self: *BestFirst, first: []const u8, end: ?[]const u8, ref: ?Ref, value: []const u8, score: f32) !void {
        if (std.math.isNan(score) or score < 0) return error.InvalidGraphPage;
        const bytes = first.len + (if (end) |v| v.len else 0) + (if (ref == null) value.len else 0);
        if (self.heap.items.len >= self.max_nodes or bytes > self.max_retained -| self.retained) return error.BestFirstBudgetExceeded;
        var node: Node = .{ .score = score, .ref = ref };
        errdefer node.deinit(self.alloc);
        node.first = try self.alloc.dupe(u8, first);
        if (end) |v| node.end = try self.alloc.dupe(u8, v);
        if (ref == null) node.value = try self.alloc.dupe(u8, value);
        try self.heap.push(self.alloc, node);
        self.retained += bytes;
    }
    /// The returned record borrows the cursor until next or deinit.
    pub fn next(self: *BestFirst, minimum_score: ?f32) !?Cursor.Record {
        try self.store.check(self.store.ptr);
        if (minimum_score) |v| if (!std.math.isFinite(v) or v < 0) return error.InvalidGraphPage;
        if (self.returned) |node| {
            node.deinit(self.alloc);
            self.returned = null;
        }
        while (self.heap.pop()) |node| {
            self.retained -= node.bytes();
            var transferred = false;
            defer if (!transferred) node.deinit(self.alloc);
            try self.store.check(self.store.ptr);
            if (minimum_score) |cutoff| if (node.score < cutoff) {
                self.pruned = true;
                self.pruned_nodes +|= 1;
                continue;
            };
            const ref = node.ref orelse {
                self.returned = node;
                transferred = true;
                return .{ .key = node.first.?, .value = node.value.? };
            };
            var page = try load(self.alloc, self.store, ref);
            defer page.deinit(self.alloc);
            if (node.first) |first| if (!std.mem.eql(u8, page.entries[0].key, first)) return error.InvalidGraphPage;
            if (node.end) |end| if (!less(page.entries[page.entries.len - 1].key, end)) return error.InvalidGraphPage;
            for (page.entries, 0..) |entry, index| {
                const end = if (index + 1 < page.entries.len) page.entries[index + 1].key else node.end;
                if (self.upper) |upper| if (!less(entry.key, upper)) break;
                if (entry.child) |child| {
                    if (end) |last| if (!less(self.lower, last)) continue;
                    const score = if (entry.aggregate.len == 0) std.math.inf(f32) else try self.bound.value(self.bound.ptr, entry.aggregate, child.records);
                    try self.add(entry.key, end, child, "", score);
                } else {
                    if (less(entry.key, self.lower)) continue;
                    try self.add(entry.key, null, null, entry.value, try self.bound.value(self.bound.ptr, entry.value, null));
                }
            }
        }
        return null;
    }
};

/// Ordered half-open range scan. Memory is bounded by (height + 1) pages, not
/// degree or graph size. Returned records borrow the cursor until next/deinit.
pub const Cursor = struct {
    alloc: Allocator,
    store: Store,
    frames: [max_height + 1]Frame = undefined,
    depth: usize = 0,
    upper: ?[]const u8,
    lower: []const u8 = "",
    reverse: bool = false,

    const Frame = struct { page: Page, next: usize, end: ?[]const u8 };
    pub const Record = struct { key: []const u8, value: []const u8 };

    pub fn init(alloc: Allocator, store: Store, root: ?Ref, lower: []const u8, upper: ?[]const u8) !Cursor {
        var self: Cursor = .{ .alloc = alloc, .store = store, .upper = upper };
        errdefer self.deinit();
        if (upper) |bound| if (!less(lower, bound)) return self;
        if (root) |ref| try self.descend(ref, lower, null, null);
        return self;
    }

    /// Reverse traversal of the same half-open range. Seek one root-to-leaf
    /// path at the upper bound, then visit preceding siblings lazily.
    pub fn initReverse(alloc: Allocator, store: Store, root: ?Ref, lower: []const u8, upper: ?[]const u8) !Cursor {
        var self: Cursor = .{ .alloc = alloc, .store = store, .upper = upper, .lower = lower, .reverse = true };
        errdefer self.deinit();
        if (upper) |bound| if (!less(lower, bound)) return self;
        if (root) |ref| try self.descendReverse(ref, upper, null, null);
        return self;
    }

    fn descendReverse(self: *Cursor, first: Ref, upper: ?[]const u8, first_key: ?[]const u8, end_key: ?[]const u8) !void {
        var ref = first;
        var expected_first = first_key;
        var expected_end = end_key;
        while (true) {
            if (self.depth == self.frames.len) return error.InvalidGraphPage;
            var page = try load(self.alloc, self.store, ref);
            errdefer page.deinit(self.alloc);
            if (expected_first) |key| if (!std.mem.eql(u8, key, page.entries[0].key)) return error.InvalidGraphPage;
            if (expected_end) |key| if (!less(page.entries[page.entries.len - 1].key, key)) return error.InvalidGraphPage;
            var lo: usize = 0;
            var hi = page.entries.len;
            if (upper) |bound| {
                while (lo < hi) {
                    const mid = lo + (hi - lo) / 2;
                    if (less(page.entries[mid].key, bound)) lo = mid + 1 else hi = mid;
                }
            } else lo = hi;
            if (ref.height == 0 or lo == 0) {
                self.frames[self.depth] = .{ .page = page, .next = lo, .end = expected_end };
                self.depth += 1;
                return;
            }
            const index = lo - 1;
            self.frames[self.depth] = .{ .page = page, .next = index, .end = expected_end };
            self.depth += 1;
            ref = page.entries[index].child.?;
            expected_first = page.entries[index].key;
            if (index + 1 < page.entries.len) expected_end = page.entries[index + 1].key;
        }
    }

    fn previous(self: *Cursor) !?Record {
        while (self.depth != 0) {
            const frame = &self.frames[self.depth - 1];
            if (frame.next == 0) {
                frame.page.deinit(self.alloc);
                self.depth -= 1;
                continue;
            }
            frame.next -= 1;
            const entry = frame.page.entries[frame.next];
            if (entry.child) |child| {
                const end = if (frame.next + 1 < frame.page.entries.len) frame.page.entries[frame.next + 1].key else frame.end;
                try self.descendReverse(child, null, entry.key, end);
            } else {
                if (less(entry.key, self.lower)) {
                    self.deinit();
                    return null;
                }
                return .{ .key = entry.key, .value = entry.value };
            }
        }
        return null;
    }

    /// Resume an ordered stream by its stable record offset without reading
    /// preceding leaves. Authenticated subtree counts bound this to one path.
    pub fn initAtRank(alloc: Allocator, store: Store, root: ?Ref, rank: u64) !Cursor {
        var self: Cursor = .{ .alloc = alloc, .store = store, .upper = null };
        errdefer self.deinit();
        var ref = root orelse return self;
        if (rank >= ref.records) return self;
        var remaining = rank;
        var first: ?[]const u8 = null;
        var end: ?[]const u8 = null;
        while (true) {
            if (self.depth == self.frames.len) return error.InvalidGraphPage;
            var page = try load(alloc, store, ref);
            errdefer page.deinit(alloc);
            if (first) |key| if (!std.mem.eql(u8, key, page.entries[0].key)) return error.InvalidGraphPage;
            if (end) |key| if (!less(page.entries[page.entries.len - 1].key, key)) return error.InvalidGraphPage;
            if (ref.height == 0) {
                if (remaining >= page.entries.len) return error.InvalidGraphPage;
                self.frames[self.depth] = .{ .page = page, .next = @intCast(remaining), .end = end };
                self.depth += 1;
                return self;
            }
            var index: usize = 0;
            while (index < page.entries.len and remaining >= page.entries[index].child.?.records) : (index += 1)
                remaining -= page.entries[index].child.?.records;
            if (index == page.entries.len) return error.InvalidGraphPage;
            self.frames[self.depth] = .{ .page = page, .next = index + 1, .end = end };
            self.depth += 1;
            ref = page.entries[index].child.?;
            first = page.entries[index].key;
            if (index + 1 < page.entries.len) end = page.entries[index + 1].key;
        }
    }

    pub fn deinit(self: *Cursor) void {
        for (self.frames[0..self.depth]) |*frame| frame.page.deinit(self.alloc);
        self.depth = 0;
    }

    fn descend(self: *Cursor, first: Ref, lower: []const u8, first_key: ?[]const u8, end_key: ?[]const u8) !void {
        var ref = first;
        var expected_first = first_key;
        var expected_end = end_key;
        while (true) {
            if (self.depth == self.frames.len) return error.InvalidGraphPage;
            var page = try load(self.alloc, self.store, ref);
            errdefer page.deinit(self.alloc);
            if (expected_first) |key| if (!std.mem.eql(u8, key, page.entries[0].key)) return error.InvalidGraphPage;
            if (expected_end) |key| if (!less(page.entries[page.entries.len - 1].key, key)) return error.InvalidGraphPage;
            var lo: usize = 0;
            var hi = page.entries.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (less(page.entries[mid].key, lower)) lo = mid + 1 else hi = mid;
            }
            if (ref.height == 0) {
                self.frames[self.depth] = .{ .page = page, .next = lo, .end = expected_end };
                self.depth += 1;
                return;
            }
            const index = if (lo < page.entries.len and std.mem.eql(u8, page.entries[lo].key, lower)) lo else lo -| 1;
            self.frames[self.depth] = .{ .page = page, .next = index + 1, .end = expected_end };
            self.depth += 1;
            ref = page.entries[index].child.?;
            expected_first = page.entries[index].key;
            if (index + 1 < page.entries.len) expected_end = page.entries[index + 1].key;
        }
    }

    /// Advance a forward stream to a key without reading skipped sibling
    /// leaves. Retain the authenticated ancestor path and never rewind.
    pub fn seekForward(self: *Cursor, lower: []const u8) !void {
        try self.store.check(self.store.ptr);
        if (self.reverse) return error.InvalidGraphPage;
        if (self.upper) |upper| if (!less(lower, upper)) {
            self.deinit();
            return;
        };
        while (self.depth != 0) {
            const frame = &self.frames[self.depth - 1];
            if (frame.end) |end| if (!less(lower, end)) {
                frame.page.deinit(self.alloc);
                self.depth -= 1;
                continue;
            };
            var lo: usize = 0;
            var hi = frame.page.entries.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (less(frame.page.entries[mid].key, lower)) lo = mid + 1 else hi = mid;
            }
            if (frame.page.ref.height == 0) {
                frame.next = @max(frame.next, lo);
                return;
            }
            const index = @max(frame.next, if (lo < frame.page.entries.len and std.mem.eql(u8, frame.page.entries[lo].key, lower)) lo else lo -| 1);
            if (index == frame.page.entries.len) {
                frame.page.deinit(self.alloc);
                self.depth -= 1;
                continue;
            }
            const entry = frame.page.entries[index];
            const end = if (index + 1 < frame.page.entries.len) frame.page.entries[index + 1].key else frame.end;
            frame.next = index + 1;
            try self.descend(entry.child.?, lower, entry.key, end);
            return;
        }
    }

    pub fn next(self: *Cursor) !?Record {
        if (self.reverse) {
            try self.store.check(self.store.ptr);
            return self.previous();
        }
        return self.nextBounded(self.upper, false);
    }
    /// A sequence of disjoint ascending ranges can retain its verified path.
    /// Reaching a window bound does not consume its first excluded record.
    pub fn nextWindow(self: *Cursor, upper: ?[]const u8) !?Record {
        if (self.reverse) return error.InvalidGraphPage;
        return self.nextBounded(upper, true);
    }
    fn nextBounded(self: *Cursor, upper: ?[]const u8, retain_path: bool) !?Record {
        try self.store.check(self.store.ptr);
        while (self.depth != 0) {
            const frame = &self.frames[self.depth - 1];
            if (frame.next == frame.page.entries.len) {
                frame.page.deinit(self.alloc);
                self.depth -= 1;
                continue;
            }
            const entry = frame.page.entries[frame.next];
            if (upper) |bound| if (!less(entry.key, bound)) {
                if (!retain_path) self.deinit();
                return null;
            };
            frame.next += 1;
            if (entry.child) |child| {
                const end = if (frame.next < frame.page.entries.len) frame.page.entries[frame.next].key else frame.end;
                try self.descend(child, "", entry.key, end);
            } else return .{ .key = entry.key, .value = entry.value };
        }
        return null;
    }
};

/// Cardinality of a half-open key range. Fully covered immutable subtrees use
/// their authenticated record count; only the two boundary paths need reads.
/// This supports selected-type admission without scanning edges beforehand.
pub fn countRange(alloc: Allocator, store: Store, root: ?Ref, lower: []const u8, upper: ?[]const u8) !u64 {
    try store.check(store.ptr);
    if (upper) |bound| if (!less(lower, bound)) return 0;
    return countInner(alloc, store, root orelse return 0, lower, upper, null, null);
}

fn countInner(alloc: Allocator, store: Store, ref: Ref, lower: []const u8, upper: ?[]const u8, first: ?[]const u8, end: ?[]const u8) anyerror!u64 {
    if (end) |key| if (!less(lower, key)) return 0;
    if (first) |key| {
        if (upper) |bound| if (!less(key, bound)) return 0;
        if (!less(key, lower) and (upper == null or (end != null and !less(upper.?, end.?)))) return ref.records;
    }
    var page = try load(alloc, store, ref);
    defer page.deinit(alloc);
    if (first) |key| if (!std.mem.eql(u8, key, page.entries[0].key)) return error.InvalidGraphPage;
    if (end) |key| if (!less(page.entries[page.entries.len - 1].key, key)) return error.InvalidGraphPage;
    var count: u64 = 0;
    for (page.entries, 0..) |entry, i| {
        if (upper) |bound| if (!less(entry.key, bound)) break;
        if (entry.child) |child| {
            count = try std.math.add(u64, count, try countInner(alloc, store, child, lower, upper, entry.key, if (i + 1 < page.entries.len) page.entries[i + 1].key else end));
        } else if (!less(entry.key, lower)) count += 1;
    }
    return count;
}

/// Merge a sorted stream of disjoint half-open ranges with the authenticated
/// tree. Each visited page is decoded once, including for absent probe keys.
/// Source.next lends bounds until its next call; Source.admit can stop optional
/// costing without making a partial cardinality authoritative.
pub const Range = struct { lower: []const u8, upper: ?[]const u8 };
pub const RangeCounts = struct { records: u64 = 0, ranges: u64 = 0, pages: u64 = 0 };
pub fn countSortedRanges(alloc: Allocator, store: Store, root: ?Ref, source: anytype) !?RangeCounts {
    const Walker = struct {
        a: Allocator,
        store: Store,
        source: @TypeOf(source),
        range: ?Range = null,
        counts: RangeCounts = .{},
        fn advance(self: *@This()) !bool {
            try self.store.check(self.store.ptr);
            self.range = try self.source.next();
            if (self.range) |range| {
                if (range.upper) |upper| if (!less(range.lower, upper)) return error.InvalidGraphPage;
                self.counts.ranges +|= 1;
            }
            return self.source.admit(self.counts);
        }
        fn visit(self: *@This(), ref: Ref, first: ?[]const u8, end: ?[]const u8) anyerror!bool {
            if (self.range == null) return true;
            var page = try load(self.a, self.store, ref);
            defer page.deinit(self.a);
            self.counts.pages +|= 1;
            if (!try self.source.admit(self.counts)) return false;
            if (first) |key| if (!std.mem.eql(u8, key, page.entries[0].key)) return error.InvalidGraphPage;
            if (end) |key| if (!less(page.entries[page.entries.len - 1].key, key)) return error.InvalidGraphPage;
            for (page.entries, 0..) |entry, i| {
                while (self.range) |range| {
                    if (range.upper == null or less(entry.key, range.upper.?)) break;
                    if (!try self.advance()) return false;
                }
                const range = self.range orelse return true;
                const child_end = if (i + 1 < page.entries.len) page.entries[i + 1].key else end;
                if (entry.child) |child| {
                    if (child_end) |bound| if (!less(range.lower, bound)) continue;
                    const covered = !less(entry.key, range.lower) and (range.upper == null or (child_end != null and !less(range.upper.?, child_end.?)));
                    if (covered) self.counts.records +|= child.records else if (!try self.visit(child, entry.key, child_end)) return false;
                } else if (!less(entry.key, range.lower)) self.counts.records +|= 1;
                if (!try self.source.admit(self.counts)) return false;
            }
            return true;
        }
    };
    var walker: Walker = .{ .a = alloc, .store = store, .source = source };
    if (!try walker.advance()) return null;
    if (root) |ref| if (!try walker.visit(ref, null, null)) return null;
    // Keys beyond the last tree entry cannot match; no need to read them.
    return walker.counts;
}

/// Fold a half-open range using authenticated reductions for wholly covered
/// children. Only boundary paths need descendant reads. Legacy pages fall back
/// to exact leaf records; the visitor owns the reduction's type and validation.
pub fn reduceRange(alloc: Allocator, store: Store, root: ?Ref, lower: []const u8, upper: ?[]const u8, visitor: anytype) !void {
    if (root) |ref| try reduceInner(alloc, store, ref, lower, upper, null, null, visitor);
}
/// Visit leaves in a key range, rejecting children from authenticated summaries.
/// Legacy children without summaries remain eligible for an exact leaf walk.
pub fn visitRange(alloc: Allocator, store: Store, root: ?Ref, lower: []const u8, upper: ?[]const u8, visitor: anytype) !void {
    if (root) |ref| try visitInner(alloc, store, ref, lower, upper, null, null, visitor);
}
fn visitInner(alloc: Allocator, store: Store, ref: Ref, lower: []const u8, upper: ?[]const u8, first: ?[]const u8, end: ?[]const u8, visitor: anytype) anyerror!void {
    try store.check(store.ptr);
    var page = try load(alloc, store, ref);
    defer page.deinit(alloc);
    if (first) |key| if (!std.mem.eql(u8, key, page.entries[0].key)) return error.InvalidGraphPage;
    if (end) |key| if (!less(page.entries[page.entries.len - 1].key, key)) return error.InvalidGraphPage;
    for (page.entries, 0..) |entry, i| {
        if (upper) |bound| if (!less(entry.key, bound)) break;
        const child_end = if (i + 1 < page.entries.len) page.entries[i + 1].key else end;
        if (child_end) |bound| if (!less(lower, bound)) continue;
        if (entry.child) |child| {
            if (!try visitor.descend(entry.aggregate, child.records)) continue;
            try visitInner(alloc, store, child, lower, upper, entry.key, child_end, visitor);
        } else if (!less(entry.key, lower)) try visitor.record(entry.key, entry.value);
    }
}
fn reduceInner(alloc: Allocator, store: Store, ref: Ref, lower: []const u8, upper: ?[]const u8, first: ?[]const u8, end: ?[]const u8, visitor: anytype) anyerror!void {
    var page = try load(alloc, store, ref);
    defer page.deinit(alloc);
    if (first) |key| if (!std.mem.eql(u8, key, page.entries[0].key)) return error.InvalidGraphPage;
    if (end) |key| if (!less(page.entries[page.entries.len - 1].key, key)) return error.InvalidGraphPage;
    for (page.entries, 0..) |entry, i| {
        if (upper) |bound| if (!less(entry.key, bound)) break;
        const child_end = if (i + 1 < page.entries.len) page.entries[i + 1].key else end;
        if (child_end) |bound| if (!less(lower, bound)) continue;
        if (entry.child) |child| {
            const covered = !less(entry.key, lower) and (upper == null or (child_end != null and !less(upper.?, child_end.?)));
            if (covered and entry.aggregate.len != 0) try visitor.aggregate(entry.aggregate, child.records) else try reduceInner(alloc, store, child, lower, upper, entry.key, child_end, visitor);
        } else if (!less(entry.key, lower)) try visitor.record(entry.key, entry.value);
    }
}

/// Reachability traversal in child-before-parent order. A reclamation caller
/// must skip protected digests and delete a page only in visit. If interrupted,
/// the surviving parent still identifies every remaining descendant. Missing
/// pages are tolerated only for reclamation replay, never for retained roots.
/// Optional visitRecord callbacks borrow leaf bytes only for the duration of
/// the call and run before visit, allowing body-before-page reclamation without
/// reloading the leaf.
/// Visit one authenticated page without recursively loading its descendants.
/// Durable collectors can checkpoint the child frontier between pages.
pub fn walkPage(alloc: Allocator, store: Store, root: Ref, visitor: anytype) anyerror!void {
    var page = try load(alloc, store, root);
    defer page.deinit(alloc);
    for (page.entries) |entry| {
        if (entry.child) |child| {
            const Visitor = switch (@typeInfo(@TypeOf(visitor))) {
                .pointer => |pointer| pointer.child,
                else => @TypeOf(visitor),
            };
            if (comptime @hasDecl(Visitor, "childSummary")) try visitor.childSummary(child, entry.aggregate) else try visitor.child(child);
        } else try visitor.record(entry.key, entry.value);
    }
}

pub fn walkPostOrder(alloc: Allocator, store: Store, root: Ref, visitor: anytype, allow_missing: bool) anyerror!void {
    try store.check(store.ptr);
    if (try visitor.skip(root)) return;
    var page = load(alloc, store, root) catch |err| switch (err) {
        error.FileNotFound => if (allow_missing) return else return err,
        else => return err,
    };
    defer page.deinit(alloc);
    for (page.entries) |entry| if (entry.child) |child| {
        try walkPostOrder(alloc, store, child, visitor, allow_missing);
    };
    const Visitor = switch (@typeInfo(@TypeOf(visitor))) {
        .pointer => |pointer| pointer.child,
        else => @TypeOf(visitor),
    };
    if (comptime @hasDecl(Visitor, "visitRecord")) {
        if (root.height == 0) for (page.entries) |entry| {
            try visitor.visitRecord(root, entry.key, entry.value);
        };
    }
    try visitor.visit(root);
}

pub const testing = struct {
    pub const MemoryStore = TestStore;
};

const TestStore = struct {
    alloc: Allocator,
    pages: std.AutoHashMapUnmanaged([32]u8, []u8) = .empty,
    reads: usize = 0,
    writes: usize = 0,
    written_bytes: usize = 0,
    canceled: bool = false,
    fail_put: ?usize = null,

    pub fn deinit(self: *TestStore) void {
        var values = self.pages.valueIterator();
        while (values.next()) |bytes| self.alloc.free(bytes.*);
        self.pages.deinit(self.alloc);
    }

    pub fn store(self: *TestStore) Store {
        return .{ .ptr = self, .get = get, .put = put, .check = check };
    }

    fn check(ptr: *anyopaque) !void {
        const self: *TestStore = @ptrCast(@alignCast(ptr));
        if (self.canceled) return error.Canceled;
    }

    fn get(ptr: *anyopaque, alloc: Allocator, ref: Ref) ![]u8 {
        const self: *TestStore = @ptrCast(@alignCast(ptr));
        self.reads += 1;
        return alloc.dupe(u8, self.pages.get(ref.digest) orelse return error.FileNotFound);
    }

    fn put(ptr: *anyopaque, ref: Ref, bytes: []const u8) !void {
        const self: *TestStore = @ptrCast(@alignCast(ptr));
        if (self.fail_put) |at| if (self.writes == at) return error.InjectedUploadFailure;
        self.writes += 1;
        self.written_bytes += bytes.len;
        if (self.pages.contains(ref.digest)) return;
        const owned = try self.alloc.dupe(u8, bytes);
        errdefer self.alloc.free(owned);
        try self.pages.put(self.alloc, ref.digest, owned);
    }
};

test "serverless graph bulk packing preserves bounded forward and reverse reads" {
    const a = std.testing.allocator;
    const Source = struct {
        position: u64 = 0,
        key: [8]u8 = undefined,
        value: [1024]u8 = @splat(42),
        pub fn next(self: *@This()) !?Cursor.Record {
            if (self.position == 1000) return null;
            std.mem.writeInt(u64, &self.key, self.position, .big);
            self.position += 1;
            return .{ .key = &self.key, .value = &self.value };
        }
    };
    var small: TestStore = .{ .alloc = a };
    defer small.deinit();
    var large: TestStore = .{ .alloc = a };
    defer large.deinit();
    var first: Source = .{};
    var second: Source = .{};
    const small_root = try buildSorted(a, small.store(), &first);
    const large_root = try buildSortedWithOptions(a, large.store(), &second, .{ .target_bytes = bulk_target_page_bytes });
    try std.testing.expect(large.writes * 4 < small.writes);
    for ([_][2]u64{ .{ 0, 1000 }, .{ 228, 700 }, .{ 999, 1000 }, .{ 500, 500 } }) |bounds| {
        var lower: [8]u8 = undefined;
        var upper: [8]u8 = undefined;
        std.mem.writeInt(u64, &lower, bounds[0], .big);
        std.mem.writeInt(u64, &upper, bounds[1], .big);
        for ([_]bool{ false, true }) |reverse| {
            var original = if (reverse) try Cursor.initReverse(a, small.store(), small_root, &lower, &upper) else try Cursor.init(a, small.store(), small_root, &lower, &upper);
            defer original.deinit();
            var bulk_cursor = if (reverse) try Cursor.initReverse(a, large.store(), large_root, &lower, &upper) else try Cursor.init(a, large.store(), large_root, &lower, &upper);
            defer bulk_cursor.deinit();
            var count: u64 = 0;
            while (try original.next()) |row| {
                const actual = (try bulk_cursor.next()).?;
                try std.testing.expectEqualSlices(u8, row.key, actual.key);
                try std.testing.expectEqualSlices(u8, row.value, actual.value);
                count += 1;
            }
            try std.testing.expectEqual(bounds[1] - bounds[0], count);
            try std.testing.expect((try bulk_cursor.next()) == null);
        }
    }
    var invalid: Source = .{};
    const before = large.writes;
    for ([_]usize{ 0, max_page_bytes + 1 }) |target| {
        try std.testing.expectError(error.InvalidGraphPageSize, buildSortedWithOptions(a, large.store(), &invalid, .{ .target_bytes = target }));
    }
    try std.testing.expectEqual(@as(u64, 0), invalid.position);
    try std.testing.expectEqual(before, large.writes);
    var failed: TestStore = .{ .alloc = a, .fail_put = 2 };
    defer failed.deinit();
    var fresh: Source = .{};
    try std.testing.expectError(error.InjectedUploadFailure, buildSortedWithOptions(a, failed.store(), &fresh, .{ .target_bytes = bulk_target_page_bytes }));
}

test "serverless graph sorted page batches preserve identities and fail before root publication" {
    const a = std.testing.allocator;
    const Source = struct {
        position: u64 = 0,
        key: [8]u8 = undefined,
        value: [1024]u8 = @splat(7),
        pub fn next(self: *@This()) !?Cursor.Record {
            if (self.position == 1000) return null;
            std.mem.writeInt(u64, &self.key, self.position, .big);
            self.position += 1;
            return .{ .key = &self.key, .value = &self.value };
        }
    };
    const Batch = struct {
        backing: *TestStore,
        calls: usize = 0,
        largest: usize = 0,
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try TestStore.check(self.backing);
        }
        fn get(ptr: *anyopaque, alloc: Allocator, ref: Ref) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return TestStore.get(self.backing, alloc, ref);
        }
        fn put(_: *anyopaque, _: Ref, _: []const u8) !void {
            return error.UnexpectedSerialWrite;
        }
        fn many(ptr: *anyopaque, writes: []const Write) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.largest = @max(self.largest, writes.len);
            if (writes.len > max_write_batch) return error.UnboundedPageWrites;
            for (writes) |write| try TestStore.put(self.backing, write.ref, write.bytes);
        }
        fn store(self: *@This()) Store {
            return .{ .ptr = self, .check = check, .get = get, .put = put, .put_many = many };
        }
    };
    var serial: TestStore = .{ .alloc = a };
    defer serial.deinit();
    var first: Source = .{};
    const original = (try buildSorted(a, serial.store(), &first)).?;
    var backing: TestStore = .{ .alloc = a };
    defer backing.deinit();
    var batch: Batch = .{ .backing = &backing };
    var source: Source = .{};
    const root = (try buildSorted(a, batch.store(), &source)).?;
    try std.testing.expect(Ref.eql(original, root));
    try std.testing.expectEqual(serial.writes, backing.writes);
    try std.testing.expect(batch.calls > 1);
    try std.testing.expectEqual(max_write_batch, batch.largest);
    var cursor = try Cursor.init(a, batch.store(), root, "", null);
    defer cursor.deinit();
    var rows: usize = 0;
    while (try cursor.next()) |row| {
        try std.testing.expectEqual(rows, std.mem.readInt(u64, row.key[0..8], .big));
        rows += 1;
    }
    try std.testing.expectEqual(@as(usize, 1000), rows);
    for ([_]usize{ 2, serial.writes - 1 }) |failure| {
        var failed: TestStore = .{ .alloc = a, .fail_put = failure };
        defer failed.deinit();
        var failing: Batch = .{ .backing = &failed };
        var fresh: Source = .{};
        try std.testing.expectError(error.InjectedUploadFailure, buildSorted(a, failing.store(), &fresh));
    }
}

test "serverless graph page tree rank cursors resume without reading earlier leaves" {
    const a = std.testing.allocator;
    var backing: TestStore = .{ .alloc = a };
    defer backing.deinit();
    const count = 2000;
    const names = try a.alloc([8]u8, count);
    defer a.free(names);
    const changes = try a.alloc(Mutation, count);
    defer a.free(changes);
    const value = @as([128]u8, @splat(42));
    for (names, changes, 0..) |*name, *change, i| {
        std.mem.writeInt(u64, name, i, .big);
        change.* = .{ .key = name, .value = &value };
    }
    const root = (try apply(a, backing.store(), null, changes)).?;
    var seeking = try Cursor.init(a, backing.store(), root, "", names[count - 1][0..]);
    defer seeking.deinit();
    _ = try seeking.next();
    const before_seek = backing.reads;
    try seeking.seekForward(&names[1500]);
    try std.testing.expectEqualSlices(u8, &names[1500], (try seeking.next()).?.key);
    try std.testing.expect(backing.reads - before_seek <= root.height + 1);
    // Lower targets never rewind a forward stream.
    try seeking.seekForward(&names[10]);
    try std.testing.expectEqualSlices(u8, &names[1501], (try seeking.next()).?.key);
    try seeking.seekForward(&names[count - 1]);
    try std.testing.expect((try seeking.next()) == null);
    for ([_]u64{ 0, 1, 233, 1024, count - 2, count - 1, count, count + 1 }) |rank| {
        const reads = backing.reads;
        var cursor = try Cursor.initAtRank(a, backing.store(), root, rank);
        defer cursor.deinit();
        try std.testing.expect(backing.reads - reads <= @as(usize, root.height) + 1);
        var expected = rank;
        while (try cursor.next()) |record| : (expected += 1)
            try std.testing.expectEqual(expected, std.mem.readInt(u64, record.key[0..8], .big));
        try std.testing.expectEqual(@max(rank, count), expected);
    }
}

test "serverless graph page tree ordered batches preserve snapshots and bound one-record rewrite" {
    const alloc = std.testing.allocator;
    var backing: TestStore = .{ .alloc = alloc };
    defer backing.deinit();
    const count = 10000;
    const keys = try alloc.alloc([8]u8, count);
    defer alloc.free(keys);
    const changes = try alloc.alloc(Mutation, count);
    defer alloc.free(changes);
    const value = @as([64]u8, @splat(42));
    for (keys, changes, 0..) |*key, *change, i| {
        std.mem.writeInt(u64, key, i, .big);
        change.* = .{ .key = key, .value = &value };
    }
    const original = (try apply(alloc, backing.store(), null, changes)).?;
    try std.testing.expect(original.height > 0);
    try std.testing.expectEqual(count, original.records);
    const before_bytes = backing.written_bytes;
    const before_puts = backing.writes;
    const updated = (try apply(alloc, backing.store(), original, &.{.{ .key = &keys[5000], .value = "replacement" }})).?;
    try std.testing.expect(!updated.eql(original));
    try std.testing.expect(backing.written_bytes - before_bytes < 3 * max_page_bytes);
    try std.testing.expect(backing.writes - before_puts <= 3);
    var old = try Cursor.init(alloc, backing.store(), original, &keys[4999], &keys[5002]);
    defer old.deinit();
    var next = try Cursor.init(alloc, backing.store(), updated, &keys[4999], &keys[5002]);
    defer next.deinit();
    for (4999..5002) |i| {
        const old_record = (try old.next()).?;
        const next_record = (try next.next()).?;
        try std.testing.expectEqualSlices(u8, &keys[i], old_record.key);
        try std.testing.expectEqualSlices(u8, &keys[i], next_record.key);
        try std.testing.expectEqualSlices(u8, &value, old_record.value);
        try std.testing.expectEqualSlices(u8, if (i == 5000) "replacement" else &value, next_record.value);
    }
    try std.testing.expectEqual(null, try old.next());
    try std.testing.expectEqual(null, try next.next());
    const writes = backing.writes;
    const unchanged = (try apply(alloc, backing.store(), updated, &.{.{ .key = &keys[5000], .value = "replacement" }})).?;
    try std.testing.expect(updated.eql(unchanged));
    try std.testing.expectEqual(writes, backing.writes);
    // A large deletion must collapse the root, not retain a long sparse chain.
    for (changes[1..]) |*change| change.value = null;
    const tiny = (try apply(alloc, backing.store(), updated, changes)).?;
    try std.testing.expectEqual(0, tiny.height);
    try std.testing.expectEqual(1, tiny.records);
    changes[0].value = null;
    try std.testing.expectEqual(null, try apply(alloc, backing.store(), tiny, changes[0..1]));
}

test "serverless graph sorted bootstrap owns borrowed records and writes only reachable pages" {
    const a = std.testing.allocator;
    const Source = struct {
        key: [8]u8 = undefined,
        value: [128]u8 = @splat(42),
        index: u64 = 0,
        limit: u64,
        pub fn next(self: *@This()) !?Cursor.Record {
            if (self.index == self.limit) return null;
            std.mem.writeInt(u64, &self.key, self.index, .big);
            self.index += 1;
            return .{ .key = &self.key, .value = &self.value };
        }
    };
    for ([_]u64{ 0, 1, 2, 228, 229, 10000 }) |count| {
        var backing: TestStore = .{ .alloc = a };
        defer backing.deinit();
        var source: Source = .{ .limit = count };
        const root = try buildSorted(a, backing.store(), &source);
        try std.testing.expectEqual(@as(usize, 0), backing.reads);
        if (count == 0) {
            try std.testing.expectEqual(null, root);
            continue;
        }
        var cursor = try Cursor.init(a, backing.store(), root, "", null);
        defer cursor.deinit();
        var index: u64 = 0;
        while (try cursor.next()) |record| : (index += 1) {
            try std.testing.expectEqual(index, std.mem.readInt(u64, record.key[0..8], .big));
            try std.testing.expectEqualSlices(u8, &source.value, record.value);
        }
        try std.testing.expectEqual(count, index);
        const Visitor = struct {
            count: usize = 0,
            pub fn skip(_: *@This(), _: Ref) !bool {
                return false;
            }
            pub fn visit(self: *@This(), _: Ref) !void {
                self.count += 1;
            }
        };
        var visitor: Visitor = .{};
        try walkPostOrder(a, backing.store(), root.?, &visitor, false);
        try std.testing.expectEqual(visitor.count, backing.writes);
    }
}

test "serverless graph page tree visits borrowed body records before leaves with one GET per page" {
    const a = std.testing.allocator;
    var backing: TestStore = .{ .alloc = a };
    defer backing.deinit();
    const Source = struct {
        next_id: u64 = 0,
        key: [8]u8 = undefined,
        body_ref: [80]u8 = @splat(42),
        pub fn next(self: *@This()) !?Cursor.Record {
            if (self.next_id == 1000) return null;
            std.mem.writeInt(u64, &self.key, self.next_id, .big);
            self.next_id += 1;
            return .{ .key = &self.key, .value = &self.body_ref };
        }
    };
    var source: Source = .{};
    const root = (try buildSorted(a, backing.store(), &source)).?;
    try std.testing.expect(root.height > 0);
    const Visitor = struct {
        pages: usize = 0,
        bodies: usize = 0,
        pending_leaf_bodies: u64 = 0,
        pub fn skip(_: *@This(), _: Ref) !bool {
            return false;
        }
        pub fn visitRecord(self: *@This(), ref: Ref, key: []const u8, body: []const u8) !void {
            try std.testing.expectEqual(@as(u8, 0), ref.height);
            try std.testing.expectEqual(@as(usize, 8), key.len);
            try std.testing.expectEqual(@as(usize, 80), body.len);
            self.bodies += 1;
            self.pending_leaf_bodies += 1;
        }
        pub fn visit(self: *@This(), ref: Ref) !void {
            if (ref.height == 0) {
                try std.testing.expectEqual(ref.records, self.pending_leaf_bodies);
                self.pending_leaf_bodies = 0;
            } else try std.testing.expectEqual(@as(u64, 0), self.pending_leaf_bodies);
            self.pages += 1;
        }
    };
    var visitor: Visitor = .{};
    const prior_reads = backing.reads;
    try walkPostOrder(a, backing.store(), root, &visitor, false);
    try std.testing.expectEqual(@as(usize, 1000), visitor.bodies);
    try std.testing.expectEqual(visitor.pages, backing.reads - prior_reads);
    try std.testing.expectEqual(backing.pages.count(), visitor.pages);
}

test "serverless graph sorted bootstrap rejects invalid streamed order" {
    const a = std.testing.allocator;
    const Source = struct {
        index: usize = 0,
        pub fn next(self: *@This()) !?Cursor.Record {
            self.index += 1;
            return if (self.index <= 2) .{ .key = "duplicate", .value = "" } else null;
        }
    };
    var backing: TestStore = .{ .alloc = a };
    defer backing.deinit();
    var source: Source = .{};
    try std.testing.expectError(error.UnsortedGraphPageMutations, buildSorted(a, backing.store(), &source));
    try std.testing.expectEqual(@as(usize, 0), backing.writes);
}

test "serverless graph page tree rejects invalid batches before writes and authenticates reads" {
    const alloc = std.testing.allocator;
    var backing: TestStore = .{ .alloc = alloc };
    defer backing.deinit();
    try std.testing.expectError(error.UnsortedGraphPageMutations, apply(alloc, backing.store(), null, &.{
        .{ .key = "b", .value = "" }, .{ .key = "a", .value = "" },
    }));
    try std.testing.expectEqual(0, backing.writes);
    const root = (try apply(alloc, backing.store(), null, &.{.{ .key = "a", .value = "b" }})).?;
    backing.canceled = true;
    try std.testing.expectError(error.Canceled, Cursor.init(alloc, backing.store(), root, "", null));
    backing.canceled = false;
    backing.pages.get(root.digest).?[header_bytes] ^= 1;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, Cursor.init(alloc, backing.store(), root, "", null));
}

fn allocationExercise(alloc: Allocator) !void {
    // Persisted objects deliberately have an independent lifetime from the
    // failed operation, exactly as an object store does after a lost HEAD CAS.
    var backing: TestStore = .{ .alloc = std.testing.allocator };
    defer backing.deinit();
    const value = @as([8000]u8, @splat(7));
    const root = (try apply(alloc, backing.store(), null, &.{
        .{ .key = "a", .value = &value }, .{ .key = "b", .value = &value },
        .{ .key = "c", .value = &value }, .{ .key = "d", .value = &value },
        .{ .key = "e", .value = &value }, .{ .key = "f", .value = &value },
        .{ .key = "g", .value = &value }, .{ .key = "h", .value = &value },
        .{ .key = "i", .value = &value },
    })).?;
    const next = (try apply(alloc, backing.store(), root, &.{
        .{ .key = "b", .value = null }, .{ .key = "c", .value = null },
        .{ .key = "d", .value = null }, .{ .key = "g", .value = "changed" },
    })).?;
    var cursor = try Cursor.init(alloc, backing.store(), next, "c", "i");
    defer cursor.deinit();
    while (try cursor.next()) |_| {}
    var ranked = try Cursor.initAtRank(alloc, backing.store(), next, 3);
    defer ranked.deinit();
    while (try ranked.next()) |_| {}
}

test "serverless graph page tree releases every allocation on failed build update and scan" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, allocationExercise, .{});
}

test "serverless graph page tree interrupted uploads never change the published root" {
    const alloc = std.testing.allocator;
    var backing: TestStore = .{ .alloc = alloc };
    defer backing.deinit();
    const value = @as([16000]u8, @splat(17));
    const changes = [_]Mutation{
        .{ .key = "a", .value = &value }, .{ .key = "b", .value = &value },
        .{ .key = "c", .value = &value }, .{ .key = "d", .value = &value },
        .{ .key = "e", .value = &value }, .{ .key = "f", .value = &value },
    };
    const root = (try apply(alloc, backing.store(), null, &changes)).?;
    var fail_at: usize = 0;
    while (true) : (fail_at += 1) {
        backing.fail_put = backing.writes + fail_at;
        const result = apply(alloc, backing.store(), root, &.{.{ .key = "c", .value = "changed" }});
        if (result) |_| break else |err| try std.testing.expectEqual(error.InjectedUploadFailure, err);
        var pinned = try Cursor.init(alloc, backing.store(), root, "", null);
        defer pinned.deinit();
        for (changes) |change| {
            const record = (try pinned.next()).?;
            try std.testing.expectEqualSlices(u8, change.key, record.key);
            try std.testing.expectEqualSlices(u8, change.value.?, record.value);
        }
        try std.testing.expectEqual(null, try pinned.next());
    }
    try std.testing.expect(fail_at > 1);
}

test "serverless graph page tree interrupted postorder reclamation preserves shared pages" {
    const alloc = std.testing.allocator;
    var backing: TestStore = .{ .alloc = alloc };
    defer backing.deinit();
    const value = @as([16000]u8, @splat(17));
    const changes = [_]Mutation{
        .{ .key = "a", .value = &value }, .{ .key = "b", .value = &value },
        .{ .key = "c", .value = &value }, .{ .key = "d", .value = &value },
        .{ .key = "e", .value = &value }, .{ .key = "f", .value = &value },
    };
    const old = (try apply(alloc, backing.store(), null, &changes)).?;
    const current = (try apply(alloc, backing.store(), old, &.{.{ .key = "c", .value = "changed" }})).?;
    const Visitor = struct {
        backing: *TestStore,
        retained: std.AutoHashMapUnmanaged([32]u8, void) = .empty,
        collecting: bool = true,
        deleted: usize = 0,
        stop_after: ?usize = null,

        fn skip(self: *@This(), ref: Ref) !bool {
            return self.retained.contains(ref.digest);
        }

        fn visit(self: *@This(), ref: Ref) !void {
            if (self.collecting) return self.retained.put(self.backing.alloc, ref.digest, {});
            if (self.stop_after) |limit| if (self.deleted == limit) return error.InjectedGcInterruption;
            if (self.backing.pages.fetchRemove(ref.digest)) |entry| {
                self.backing.alloc.free(entry.value);
                self.deleted += 1;
            }
        }
    };
    var visitor: Visitor = .{ .backing = &backing };
    defer visitor.retained.deinit(alloc);
    try walkPostOrder(alloc, backing.store(), current, &visitor, false);
    visitor.collecting = false;
    visitor.stop_after = 1;
    try std.testing.expectError(error.InjectedGcInterruption, walkPostOrder(alloc, backing.store(), old, &visitor, true));
    try std.testing.expect(backing.pages.contains(old.digest));
    visitor.stop_after = null;
    try walkPostOrder(alloc, backing.store(), old, &visitor, true);
    try std.testing.expect(!backing.pages.contains(old.digest));
    var cursor = try Cursor.init(alloc, backing.store(), current, "", null);
    defer cursor.deinit();
    for (changes) |change| {
        const record = (try cursor.next()).?;
        try std.testing.expectEqualSlices(u8, change.key, record.key);
        try std.testing.expectEqualSlices(u8, if (std.mem.eql(u8, change.key, "c")) "changed" else &value, record.value);
    }
    try std.testing.expectEqual(null, try cursor.next());
}

test "serverless graph page tree batch churn matches eager ordered oracle" {
    const alloc = std.testing.allocator;
    var backing: TestStore = .{ .alloc = alloc };
    defer backing.deinit();
    var random = std.Random.DefaultPrng.init(0x172329);
    const count = 320;
    var keys: [count][8]u8 = undefined;
    var present = @as([count]bool, @splat(false));
    const value = @as([1024]u8, @splat(9));
    var root: ?Ref = null;
    for (&keys, 0..) |*key, i| std.mem.writeInt(u64, key, i, .big);
    for (0..80) |_| {
        var changes: std.ArrayListUnmanaged(Mutation) = .empty;
        defer changes.deinit(alloc);
        for (&keys, &present) |*key, *exists| {
            if (random.random().uintLessThan(u32, 8) != 0) continue;
            exists.* = random.random().boolean();
            try changes.append(alloc, .{ .key = key, .value = if (exists.*) &value else null });
        }
        root = try apply(alloc, backing.store(), root, changes.items);
        var cursor = try Cursor.init(alloc, backing.store(), root, "", null);
        defer cursor.deinit();
        var expected: u64 = 0;
        for (&keys, present) |*key, exists| {
            if (!exists) continue;
            expected += 1;
            const record = (try cursor.next()).?;
            try std.testing.expectEqualSlices(u8, key, record.key);
            try std.testing.expectEqualSlices(u8, &value, record.value);
        }
        try std.testing.expectEqual(null, try cursor.next());
        try std.testing.expectEqual(expected, if (root) |ref| ref.records else 0);
        try std.testing.expectEqual(expected, try countRange(alloc, backing.store(), root, "", null));
        var middle: u64 = 0;
        for (present[80..240]) |exists| middle += @intFromBool(exists);
        try std.testing.expectEqual(middle, try countRange(alloc, backing.store(), root, &keys[80], &keys[240]));
    }
}

test "serverless graph page cache reuses authenticated pages within its byte cap" {
    const alloc = std.testing.allocator;
    var backing: TestStore = .{ .alloc = alloc };
    defer backing.deinit();
    const value = @as([16000]u8, @splat(3));
    const root = (try apply(alloc, backing.store(), null, &.{
        .{ .key = "a", .value = &value }, .{ .key = "b", .value = &value },
        .{ .key = "c", .value = &value }, .{ .key = "d", .value = &value },
    })).?;
    var cache: Cache = .{ .alloc = alloc, .underlying = backing.store(), .max_bytes = max_page_bytes };
    defer cache.deinit();
    for (0..3) |i| {
        const reads = backing.reads;
        var cursor = try Cursor.init(alloc, cache.store(), root, "a", "b");
        defer cursor.deinit();
        try std.testing.expectEqualStrings("a", (try cursor.next()).?.key);
        try std.testing.expectEqual(null, try cursor.next());
        if (i != 0) try std.testing.expectEqual(reads, backing.reads);
        try std.testing.expect(cache.bytes <= cache.max_bytes);
    }
}

test "serverless graph pages bound long indivisible keys and branch packing makes progress" {
    const alloc = std.testing.allocator;
    var backing: TestStore = .{ .alloc = alloc };
    defer backing.deinit();
    var changes: [5]Mutation = undefined;
    var initialized: usize = 0;
    defer for (changes[0..initialized]) |change| alloc.free(change.key);
    for (&changes, 0..) |*change, i| {
        const key = try alloc.alloc(u8, 70 * 1024);
        @memset(key, 'a');
        key[key.len - 1] += @intCast(i);
        change.* = .{ .key = key, .value = "" };
        initialized += 1;
    }
    const root = (try apply(alloc, backing.store(), null, &changes)).?;
    try std.testing.expect(root.height < max_height);
    var cursor = try Cursor.init(alloc, backing.store(), root, "", null);
    defer cursor.deinit();
    for (changes) |change| try std.testing.expectEqualStrings(change.key, (try cursor.next()).?.key);
    try std.testing.expectEqual(null, try cursor.next());
}

test "external lake batched membership visits shared routing paths once" {
    const a = std.testing.allocator;
    var backing: TestStore = .{ .alloc = a };
    defer backing.deinit();
    var names: [2000][8]u8 = undefined;
    var changes: [2000]Mutation = undefined;
    const value = @as([128]u8, @splat(42));
    for (&names, &changes, 0..) |*name, *change, index| {
        std.mem.writeInt(u64, name, index * 2, .big);
        change.* = .{ .key = name, .value = &value };
    }
    const root = (try apply(a, backing.store(), null, &changes)).?;
    var query_names: [256][8]u8 = undefined;
    var keys: [256][]const u8 = undefined;
    var found: [256]bool = undefined;
    for (&query_names, &keys, 0..) |*name, *key, index| {
        std.mem.writeInt(u64, name, 1800 + index, .big);
        key.* = name;
    }
    const reads = backing.reads;
    try containsMany(a, backing.store(), root, &keys, &found);
    for (found, 0..) |present, index| try std.testing.expectEqual(index % 2 == 0, present);
    try std.testing.expect(backing.reads - reads < 8);
    try containsMany(a, backing.store(), null, &keys, &found);
    for (found) |present| try std.testing.expect(!present);
    std.mem.swap([]const u8, &keys[0], &keys[1]);
    try std.testing.expectError(error.InvalidGraphPage, containsMany(a, backing.store(), root, &keys, &found));
}

test "external lake contribution retention adopts full branches and removes obsolete keys" {
    const a = std.testing.allocator;
    var backing: TestStore = .{ .alloc = a };
    defer backing.deinit();
    const count = 2000;
    const names = try a.alloc([8]u8, count);
    defer a.free(names);
    const changes = try a.alloc(Mutation, count);
    defer a.free(changes);
    const keep = try a.alloc([]const u8, count);
    defer a.free(keep);
    const value = @as([128]u8, @splat(42));
    for (names, changes, keep, 0..) |*name, *change, *key, i| {
        std.mem.writeInt(u64, name, i, .big);
        change.* = .{ .key = name, .value = &value };
        key.* = name;
    }
    const prior = (try apply(a, backing.store(), null, changes)).?;
    const reads = backing.reads;
    const writes = backing.writes;
    try std.testing.expect(prior.eql((try retainKnown(a, backing.store(), prior, keep)).?));
    try std.testing.expectEqual(reads, backing.reads);
    try std.testing.expectEqual(writes, backing.writes);
    const retained = (try retainKnown(a, backing.store(), prior, keep[0..1500])).?;
    try std.testing.expectEqual(@as(u64, 1500), retained.records);
    // Only the frontier and a partially retained leaf need reads.
    try std.testing.expect(backing.reads - reads < 8);
    var cursor = try Cursor.init(a, backing.store(), retained, "", null);
    defer cursor.deinit();
    var index: usize = 0;
    while (try cursor.next()) |row| : (index += 1) {
        try std.testing.expectEqualSlices(u8, &names[index], row.key);
        try std.testing.expectEqualSlices(u8, &value, row.value);
    }
    try std.testing.expectEqual(@as(usize, 1500), index);
    try std.testing.expect(try retainKnown(a, backing.store(), prior, &.{}) == null);
    // Prior generations remain immutable and readable after the cut.
    try std.testing.expectEqual(@as(u64, 2000), try countRange(a, backing.store(), prior, "", null));
}

test "external lake reverse tree seeks bounded ranges across leaves" {
    const a = std.testing.allocator;
    var backing: TestStore = .{ .alloc = a };
    defer backing.deinit();
    const Source = struct {
        index: u64 = 0,
        key: [8]u8 = undefined,
        pub fn next(self: *@This()) !?Cursor.Record {
            if (self.index == 10000) return null;
            std.mem.writeInt(u64, &self.key, self.index, .big);
            self.index += 1;
            return .{ .key = &self.key, .value = "value" };
        }
    };
    var source: Source = .{};
    const root = try buildSorted(a, backing.store(), &source);
    for ([_][2]u64{ .{ 0, 10000 }, .{ 0, 1 }, .{ 228, 500 }, .{ 9990, 10000 }, .{ 500, 500 }, .{ 10001, 11000 } }) |bounds| {
        var lower: [8]u8 = undefined;
        var upper: [8]u8 = undefined;
        std.mem.writeInt(u64, &lower, bounds[0], .big);
        std.mem.writeInt(u64, &upper, bounds[1], .big);
        var reverse = try Cursor.initReverse(a, backing.store(), root, &lower, &upper);
        defer reverse.deinit();
        var expected = @min(bounds[1], 10000);
        while (try reverse.next()) |record| {
            expected -= 1;
            try std.testing.expectEqual(expected, std.mem.readInt(u64, record.key[0..8], .big));
        }
        try std.testing.expectEqual(@min(bounds[0], 10000), expected);
    }
    const before = backing.reads;
    var last = try Cursor.initReverse(a, backing.store(), root, "", null);
    defer last.deinit();
    try std.testing.expectEqual(@as(u64, 9999), std.mem.readInt(u64, (try last.next()).?.key[0..8], .big));
    try std.testing.expect(backing.reads - before <= root.?.height + 1);
    // Even a warm leaf must check cancellation before returning another row.
    backing.canceled = true;
    try std.testing.expectError(error.Canceled, last.next());
    backing.canceled = false;
}

test "external lake text statistics page reductions reuse interior subtrees and survive COW allocation failures" {
    const a = std.testing.allocator;
    var memory: testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    const Source = struct {
        position: u64 = 0,
        key: [8]u8 = undefined,
        value: [8]u8 = undefined,
        pub fn next(self: *@This()) !?Cursor.Record {
            if (self.position == 6000) return null;
            std.mem.writeInt(u64, &self.key, self.position, .big);
            std.mem.writeInt(u64, &self.value, 1, .little);
            self.position += 1;
            return .{ .key = &self.key, .value = &self.value };
        }
        fn many(raw: *anyopaque, writes: []const Write) !void {
            for (writes) |write| try TestStore.put(raw, write.ref, write.bytes);
        }
        fn reduce(alloc: Allocator, entries: []const Entry, height: u8) ![]u8 {
            var sum: u64 = 0;
            for (entries) |entry| {
                const bytes = if (height == 0) entry.value else entry.aggregate;
                if (bytes.len != 8) return error.InvalidGraphPage;
                sum = try std.math.add(u64, sum, std.mem.readInt(u64, bytes[0..8], .little));
            }
            const out = try alloc.alloc(u8, 8);
            std.mem.writeInt(u64, out[0..8], sum, .little);
            return out;
        }
    };
    var store = memory.store();
    store.reduce = Source.reduce;
    store.put_many = Source.many;
    var source: Source = .{};
    const root = try buildSorted(a, store, &source);
    var failed: TestStore = .{ .alloc = a, .fail_put = 0 };
    defer failed.deinit();
    var failing_store = failed.store();
    failing_store.reduce = Source.reduce;
    failing_store.put_many = Source.many;
    var failing_source: Source = .{};
    try std.testing.expectError(error.InjectedUploadFailure, buildSorted(a, failing_store, &failing_source));
    var key: [8]u8 = undefined;
    var value: [8]u8 = undefined;
    std.mem.writeInt(u64, &key, 2000, .big);
    std.mem.writeInt(u64, &value, 100, .little);
    const Check = struct {
        fn run(alloc: Allocator, backing: Store, prior: ?Ref, key_: []const u8, value_: []const u8) !void {
            const updated = try apply(alloc, backing, prior, &.{.{ .key = key_, .value = value_ }});
            const Fold = struct {
                sum: u64 = 0,
                nodes: usize = 0,
                records: usize = 0,
                pub fn aggregate(self: *@This(), bytes: []const u8, _: u64) !void {
                    if (bytes.len != 8) return error.InvalidGraphPage;
                    self.sum += std.mem.readInt(u64, bytes[0..8], .little);
                    self.nodes += 1;
                }
                pub fn record(self: *@This(), _: []const u8, bytes: []const u8) !void {
                    self.sum += std.mem.readInt(u64, bytes[0..8], .little);
                    self.records += 1;
                }
            };
            var first: [8]u8 = undefined;
            var end: [8]u8 = undefined;
            std.mem.writeInt(u64, &first, 1000, .big);
            std.mem.writeInt(u64, &end, 5000, .big);
            var fold: Fold = .{};
            try reduceRange(alloc, backing, updated, &first, &end, &fold);
            try std.testing.expectEqual(@as(u64, 4099), fold.sum);
            if (backing.reduce != null) {
                try std.testing.expect(fold.nodes > 0);
                try std.testing.expect(fold.records < 4000);
            }
        }
    };
    try Check.run(a, store, root, &key, &value);
    // A generic writer without the reducer drops reductions on modified paths;
    // range folding still combines untouched aggregates with exact leaf records.
    var plain = store;
    plain.reduce = null;
    try Check.run(a, plain, root, &key, &value);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{ store, root, &key, &value });
}

test "external lake dynamic exact keys sorted range zipper skips absent prefixes beyond old probe cap" {
    const a = std.testing.allocator;
    var memory: testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    const Records = struct {
        ordinal: u32 = 0,
        key: [8]u8 = undefined,
        pub fn next(self: *@This()) !?Cursor.Record {
            if (self.ordinal == 3000) return null;
            std.mem.writeInt(u32, self.key[0..4], (self.ordinal / 3) * 100, .big);
            std.mem.writeInt(u32, self.key[4..8], self.ordinal % 3, .big);
            self.ordinal += 1;
            return .{ .key = &self.key, .value = "value" };
        }
    };
    var records: Records = .{};
    const root = try buildSorted(a, memory.store(), &records);
    const Ranges = struct {
        ordinal: u32 = 0,
        lower: [4]u8 = undefined,
        upper: [4]u8 = undefined,
        limit: u64 = std.math.maxInt(u64),
        pub fn next(self: *@This()) !?Range {
            if (self.ordinal == 100000) return null;
            std.mem.writeInt(u32, &self.lower, self.ordinal, .big);
            std.mem.writeInt(u32, &self.upper, self.ordinal + 1, .big);
            self.ordinal += 1;
            return .{ .lower = &self.lower, .upper = &self.upper };
        }
        pub fn admit(self: *@This(), counts: RangeCounts) !bool {
            return counts.records <= self.limit;
        }
    };
    const before = memory.reads;
    var ranges: Ranges = .{};
    const counted = (try countSortedRanges(a, memory.store(), root, &ranges)).?;
    try std.testing.expectEqual(@as(u64, 3000), counted.records);
    try std.testing.expect(counted.ranges > 65536);
    try std.testing.expectEqual(counted.pages, memory.reads - before);
    try std.testing.expect(counted.pages < 100);
    var bounded: Ranges = .{ .limit = 1 };
    try std.testing.expect((try countSortedRanges(a, memory.store(), root, &bounded)) == null);
}

test "external lake best first traversal preserves ties prunes losing pages and checks cancellation" {
    const a = std.testing.allocator;
    var memory: TestStore = .{ .alloc = a };
    defer memory.deinit();
    const Source = struct {
        at: u64 = 0,
        key: [8]u8 = undefined,
        value: [128]u8 = @splat(0),
        pub fn next(self: *@This()) !?Cursor.Record {
            if (self.at == 5000) return null;
            std.mem.writeInt(u64, &self.key, self.at, .big);
            const score: f32 = if (self.at >= 4998) 1000 else 1;
            std.mem.writeInt(u32, self.value[0..4], @bitCast(score), .little);
            self.at += 1;
            return .{ .key = &self.key, .value = &self.value };
        }
        fn reduce(a_: Allocator, entries: []const Entry, height: u8) ![]u8 {
            var score: f32 = 0;
            for (entries) |entry| score = @max(score, @as(f32, @bitCast(std.mem.readInt(u32, (if (height == 0) entry.value else entry.aggregate)[0..4], .little))));
            const out = try a_.alloc(u8, 4);
            std.mem.writeInt(u32, out[0..4], @bitCast(score), .little);
            return out;
        }
        fn bound(_: *anyopaque, bytes: []const u8, _: ?u64) !f32 {
            if (bytes.len < 4) return error.InvalidGraphPage;
            return @bitCast(std.mem.readInt(u32, bytes[0..4], .little));
        }
    };
    var source: Source = .{};
    var store = memory.store();
    store.reduce = Source.reduce;
    const root = try buildSorted(a, store, &source);
    const all_pages = memory.pages.count();
    memory.reads = 0;
    var cursor = try BestFirst.init(a, store, root, "", null, .{ .ptr = &source, .value = Source.bound });
    defer cursor.deinit();
    const first = (try cursor.next(null)).?;
    const first_id = std.mem.readInt(u64, first.key[0..8], .big);
    try std.testing.expect(first_id >= 4998);
    const second = (try cursor.next(1000)).?;
    try std.testing.expect(std.mem.readInt(u64, second.key[0..8], .big) >= 4998);
    try std.testing.expect(std.mem.readInt(u64, second.key[0..8], .big) != first_id);
    try std.testing.expect((try cursor.next(1000)) == null);
    try std.testing.expect(cursor.pruned);
    try std.testing.expect(memory.reads < all_pages);
    memory.canceled = true;
    try std.testing.expectError(error.Canceled, cursor.next(1000));
    memory.canceled = false;
    // A bounded frontier declines optional planning instead of truncating it.
    var limited = try BestFirst.init(a, store, root, "", null, .{ .ptr = &source, .value = Source.bound });
    defer limited.deinit();
    limited.max_nodes = 0;
    try std.testing.expectError(error.BestFirstBudgetExceeded, limited.next(null));
    // Missing reductions never authorize subtree pruning.
    var old_source: Source = .{};
    const old_root = try buildSorted(a, memory.store(), &old_source);
    var old = try BestFirst.init(a, memory.store(), old_root, "", null, .{ .ptr = &source, .value = Source.bound });
    defer old.deinit();
    var found: usize = 0;
    while (try old.next(1000)) |_| found += 1;
    try std.testing.expectEqual(@as(usize, 2), found);
}

test "serverless graph value updates retain unrelated short tails and deletion repairs target pages" {
    const a = std.testing.allocator;
    var memory: TestStore = .{ .alloc = a };
    defer memory.deinit();
    var builder: SortedBuilder = .{ .alloc = a, .store = memory.store() };
    defer builder.deinit();
    var key: [8]u8 = undefined;
    var value: [220]u8 = @splat('a');
    for (0..300) |i| {
        std.mem.writeInt(u64, &key, i, .big);
        try builder.add(&key, &value);
    }
    const before = (try builder.finish()).?;
    var prior = try readPage(a, memory.store(), before);
    defer prior.deinit(a);
    try std.testing.expect(prior.entries.len >= 3 and prior.ref.height == 1);
    const last = prior.entries[prior.entries.len - 1].child.?;
    try std.testing.expect(last.bytes < target_page_bytes / 2);
    std.mem.writeInt(u64, &key, 0, .big);
    value[0] = 'b';
    const writes = memory.writes;
    const after = (try apply(a, memory.store(), before, &.{.{ .key = &key, .value = &value }})).?;
    try std.testing.expectEqual(@as(usize, 2), memory.writes - writes);
    var fresh = try readPage(a, memory.store(), after);
    defer fresh.deinit(a);
    try std.testing.expectEqual(prior.entries.len, fresh.entries.len);
    for (prior.entries[1..], fresh.entries[1..]) |l, r| try std.testing.expect(l.child.?.eql(r.child.?));
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const edits = try arena.allocator().alloc(Mutation, 200);
    for (edits, 0..) |*mutation, i| {
        const bytes = try arena.allocator().alloc(u8, 8);
        std.mem.writeInt(u64, bytes[0..8], i, .big);
        mutation.* = .{ .key = bytes, .value = null };
    }
    const removed = (try apply(a, memory.store(), after, edits)).?;
    try std.testing.expectEqual(@as(u64, 100), removed.records);
    var cursor = try Cursor.init(a, memory.store(), removed, "", null);
    defer cursor.deinit();
    for (200..300) |expected| {
        const record = (try cursor.next()).?;
        try std.testing.expectEqual(@as(u64, @intCast(expected)), std.mem.readInt(u64, record.key[0..8], .big));
    }
    try std.testing.expect((try cursor.next()) == null);
}

test "serverless graph window navigation batches bound publication write amplification" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const changes = try out.alloc(Mutation, 1024);
    const value: [64]u8 = @splat('f');
    for (changes, 0..) |*change, i| {
        var input: [8]u8 = undefined;
        std.mem.writeInt(u64, &input, i, .little);
        const key = try out.create([32]u8);
        std.crypto.hash.sha2.Sha256.hash(&input, key, .{});
        change.* = .{ .key = key, .value = &value };
    }
    var writes: [2]usize = undefined;
    var bytes: [2]u64 = undefined;
    for ([_]usize{ 1, 64 }, 0..) |batch_size, run| {
        var memory: TestStore = .{ .alloc = a };
        defer memory.deinit();
        var root: ?Ref = null;
        var begin: usize = 0;
        while (begin < changes.len) : (begin += 64) {
            const window = changes[begin..][0..64];
            std.mem.sort(Mutation, window, {}, struct {
                fn less(_: void, left: Mutation, right: Mutation) bool {
                    return std.mem.lessThan(u8, left.key, right.key);
                }
            }.less);
            var pos: usize = 0;
            while (pos < window.len) : (pos += batch_size) root = try apply(a, memory.store(), root, window[pos..][0..batch_size]);
        }
        try std.testing.expectEqual(@as(u64, 1024), root.?.records);
        var cursor = try Cursor.init(a, memory.store(), root, "", null);
        defer cursor.deinit();
        var count: usize = 0;
        while (try cursor.next()) |record| {
            try std.testing.expectEqualSlices(u8, &value, record.value);
            count += 1;
        }
        try std.testing.expectEqual(changes.len, count);
        writes[run] = memory.writes;
        bytes[run] = memory.written_bytes;
        std.debug.print("navigation batch={} writes={} bytes={}\n", .{ batch_size, writes[run], bytes[run] });
    }
    try std.testing.expect(writes[1] * 16 < writes[0]);
    try std.testing.expect(bytes[1] * 16 < bytes[0]);
}

test "serverless graph disjoint joins preserve descendants overlap fallback and unequal heights" {
    const a = std.testing.allocator;
    var memory: TestStore = .{ .alloc = a };
    defer memory.deinit();
    const Source = struct {
        position: u64,
        end: u64,
        key: [8]u8 = undefined,
        value: [128]u8 = @splat(7),
        pub fn next(self: *@This()) !?Cursor.Record {
            if (self.position == self.end) return null;
            std.mem.writeInt(u64, &self.key, self.position, .big);
            self.position += 1;
            return .{ .key = &self.key, .value = &self.value };
        }
    };
    var left_source: Source = .{ .position = 0, .end = 12000 };
    var right_source: Source = .{ .position = 12001, .end = 24001 };
    var middle_source: Source = .{ .position = 12000, .end = 12001 };
    var backing = memory.store();
    backing.attempt = @splat(1);
    const left = (try buildSorted(a, backing, &left_source)).?;
    backing.attempt = @splat(2);
    const right = (try buildSorted(a, backing, &right_source)).?;
    const middle = (try buildSorted(a, backing, &middle_source)).?;
    backing.attempt = @splat(3);
    try std.testing.expect(left.height > middle.height);
    const written = memory.written_bytes;
    const reads = memory.reads;
    const joined = (try joinDisjoint(a, backing, right, left)).?;
    const bytes = memory.written_bytes - written;
    const loaded = memory.reads - reads;
    try std.testing.expectEqual(@as(u64, 24000), joined.records);
    try std.testing.expect(bytes < 64 * 1024);
    try std.testing.expect(loaded < 20);
    // The new routing root still references both original attempts.
    var page = try readPage(a, backing, joined);
    defer page.deinit(a);
    try std.testing.expectEqual(@as(u8, 1), page.entries[0].child.?.attempt[0]);
    try std.testing.expectEqual(@as(u8, 2), page.entries[page.entries.len - 1].child.?.attempt[0]);
    const before_overlap = memory.writes;
    try std.testing.expect((try joinDisjoint(a, backing, joined, middle)) == null);
    try std.testing.expectEqual(before_overlap, memory.writes);
    const prefix = (try joinDisjoint(a, backing, left, middle)).?;
    const suffix = (try joinDisjoint(a, backing, middle, right)).?;
    try std.testing.expectEqual(@as(u64, 12001), prefix.records);
    try std.testing.expectEqual(@as(u64, 12001), suffix.records);
    const full = (try joinDisjoint(a, backing, prefix, right)).?;
    var cursor = try Cursor.init(a, backing, full, "", null);
    defer cursor.deinit();
    var count: u64 = 0;
    while (try cursor.next()) |record| {
        try std.testing.expectEqual(count, std.mem.readInt(u64, record.key[0..8], .big));
        try std.testing.expectEqualSlices(u8, &left_source.value, record.value);
        count += 1;
    }
    try std.testing.expectEqual(@as(u64, 24001), count);
    try std.testing.checkAllAllocationFailures(a, disjointJoinFaultScenario, .{ backing, left, middle });
    std.debug.print("disjoint tree join: records=24000 written_bytes={} page_reads={}\n", .{ bytes, loaded });
}
fn disjointJoinFaultScenario(a: Allocator, store: Store, left: Ref, right: Ref) !void {
    const root = (try joinDisjoint(a, store, left, right)).?;
    try std.testing.expectEqual(left.records + right.records, root.records);
}

const MergeTestReducer = struct {
    pub fn merge(_: @This(), a: Allocator, left: []const u8, right: []const u8) ![]const u8 {
        const value = try a.dupe(u8, left);
        std.mem.writeInt(u64, value[0..8], std.mem.readInt(u64, left[0..8], .little) + std.mem.readInt(u64, right[0..8], .little), .little);
        return value;
    }
};
fn mergeSortedFaultScenario(a: Allocator, store: Store, left: Ref, right: Ref, count: u64) !void {
    const root = try mergeSorted(a, store, left, right, MergeTestReducer{});
    try std.testing.expectEqual(count, root.records);
}
test "serverless graph interval merge reuses partial overlaps and retains old attempts" {
    const a = std.testing.allocator;
    var memory: TestStore = .{ .alloc = a };
    defer memory.deinit();
    const Source = struct {
        position: u64,
        end: u64,
        stride: u64 = 1,
        key: [8]u8 = undefined,
        value: [128]u8 = @splat(0),
        pub fn next(self: *@This()) !?Cursor.Record {
            if (self.position >= self.end) return null;
            std.mem.writeInt(u64, &self.key, self.position, .big);
            std.mem.writeInt(u64, self.value[0..8], 1, .little);
            self.position += self.stride;
            return .{ .key = &self.key, .value = &self.value };
        }
    };
    var ls: Source = .{ .position = 0, .end = 12000 };
    var rs: Source = .{ .position = 11999, .end = 24000 };
    var backing = memory.store();
    backing.attempt = @splat(1);
    const left = (try buildSorted(a, backing, &ls)).?;
    backing.attempt = @splat(2);
    const right = (try buildSorted(a, backing, &rs)).?;
    backing.attempt = @splat(3);
    const before = memory.written_bytes;
    const reads = memory.reads;
    const root = try mergeSorted(a, backing, left, right, MergeTestReducer{});
    const bytes = memory.written_bytes - before;
    const loaded = memory.reads - reads;
    try std.testing.expectEqual(@as(u64, 24000), root.records);
    try std.testing.expect(bytes < 128 * 1024);
    try std.testing.expect(loaded < 30);
    var cursor = try Cursor.init(a, backing, root, "", null);
    defer cursor.deinit();
    var count: u64 = 0;
    while (try cursor.next()) |item| {
        try std.testing.expectEqual(count, std.mem.readInt(u64, item.key[0..8], .big));
        try std.testing.expectEqual(@as(u64, if (count == 11999) 2 else 1), std.mem.readInt(u64, item.value[0..8], .little));
        count += 1;
    }
    try std.testing.expectEqual(root.records, count);
    const Retained = struct {
        attempts: [3]bool = @splat(false),
        pub fn skip(_: *@This(), _: Ref) !bool {
            return false;
        }
        pub fn visit(self: *@This(), ref: Ref) !void {
            if (ref.attempt[0] >= 1 and ref.attempt[0] <= 3) self.attempts[ref.attempt[0] - 1] = true;
        }
    };
    var retained: Retained = .{};
    try walkPostOrder(a, backing, root, &retained, false);
    try std.testing.expectEqualSlices(bool, &.{ true, true, true }, &retained.attempts);
    // Reversed inputs and repeated equal roots preserve reducer multiplicity.
    const reverse = try mergeSorted(a, backing, right, left, MergeTestReducer{});
    try std.testing.expectEqual(root.records, reverse.records);
    const doubled = try mergeSorted(a, backing, left, left, MergeTestReducer{});
    try std.testing.expectEqual(left.records, doubled.records);
    var dc = try Cursor.init(a, backing, doubled, "", null);
    defer dc.deinit();
    while (try dc.next()) |item| try std.testing.expectEqual(@as(u64, 2), std.mem.readInt(u64, item.value[0..8], .little));
    var tiny: Source = .{ .position = 11999, .end = 12003, .stride = 2 };
    const small = (try buildSorted(a, backing, &tiny)).?;
    try std.testing.expect(left.height > small.height);
    try std.testing.checkAllAllocationFailures(a, mergeSortedFaultScenario, .{ backing, small, small, small.records });
    try mergeSortedFaultScenario(a, backing, left, small, 12001);
    memory.fail_put = memory.writes;
    try std.testing.expectError(error.InjectedUploadFailure, mergeSorted(a, backing, left, small, MergeTestReducer{}));
    memory.fail_put = null;
    memory.canceled = true;
    try std.testing.expectError(error.Canceled, mergeSorted(a, backing, left, right, MergeTestReducer{}));
    std.debug.print("overlap tree merge: records=24000 written_bytes={} page_reads={}\n", .{ bytes, loaded });
}

test "serverless graph interval merge preserves interleaved keys and authenticated summaries" {
    const a = std.testing.allocator;
    var memory: TestStore = .{ .alloc = a };
    defer memory.deinit();
    const Summary = struct {
        fn reduce(alloc: Allocator, entries: []const Entry, height: u8) ![]u8 {
            var sum: u64 = 0;
            for (entries) |entry| sum += std.mem.readInt(u64, (if (height == 0) entry.value else entry.aggregate)[0..8], .little);
            const bytes = try alloc.alloc(u8, 8);
            std.mem.writeInt(u64, bytes[0..8], sum, .little);
            return bytes;
        }
    };
    const Source = struct {
        position: u64 = 0,
        divisor: u64,
        key: [8]u8 = undefined,
        value: [8]u8 = undefined,
        pub fn next(self: *@This()) !?Cursor.Record {
            while (self.position < 1000 and self.position % self.divisor == 0) self.position += 1;
            if (self.position == 1000) return null;
            std.mem.writeInt(u64, &self.key, self.position, .big);
            std.mem.writeInt(u64, &self.value, 1, .little);
            self.position += 1;
            return .{ .key = &self.key, .value = &self.value };
        }
    };
    var backing = memory.store();
    backing.reduce = Summary.reduce;
    var ls: Source = .{ .divisor = 4 };
    var rs: Source = .{ .divisor = 3 };
    const left = (try buildSortedWithOptions(a, backing, &ls, .{ .target_bytes = 512 })).?;
    const right = (try buildSortedWithOptions(a, backing, &rs, .{ .target_bytes = 2048 })).?;
    const root = try mergeSorted(a, backing, left, right, MergeTestReducer{});
    var cursor = try Cursor.init(a, backing, root, "", null);
    defer cursor.deinit();
    var groups: u64 = 0;
    var sum: u64 = 0;
    for (0..1000) |i| {
        const expected: u64 = @as(u64, @intFromBool(i % 4 != 0)) + @intFromBool(i % 3 != 0);
        if (expected == 0) continue;
        const item = (try cursor.next()).?;
        try std.testing.expectEqual(@as(u64, i), std.mem.readInt(u64, item.key[0..8], .big));
        try std.testing.expectEqual(expected, std.mem.readInt(u64, item.value[0..8], .little));
        groups += 1;
        sum += expected;
    }
    try std.testing.expect((try cursor.next()) == null);
    try std.testing.expectEqual(groups, root.records);
    var page = try readPage(a, backing, root);
    defer page.deinit(a);
    try std.testing.expectEqual(sum, std.mem.readInt(u64, page.aggregate[0..8], .little));
}
