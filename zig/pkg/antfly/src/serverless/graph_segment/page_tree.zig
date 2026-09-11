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
pub const max_key_bytes = 256 * 1024;
pub const max_record_bytes = max_page_bytes / 2 - header_bytes;
pub const max_height = 16;
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
/// ref.bytes. The tree independently verifies every returned page digest.
/// put must durably persist exactly these bytes before returning. check is also
/// called between CPU batches, so cancellation is not dependent on storage I/O.
pub const Store = struct {
    /// Namespace reclamation domain is authenticated in every page, preventing
    /// content deduplication from coupling independent namespace collectors.
    domain: [32]u8 = @splat(0),
    attempt: [16]u8 = @splat(0),
    ptr: *anyopaque,
    get: *const fn (*anyopaque, Allocator, Ref) anyerror![]u8,
    put: *const fn (*anyopaque, Ref, []const u8) anyerror!void,
    check: *const fn (*anyopaque) anyerror!void,
};

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
        return .{ .domain = self.underlying.domain, .attempt = self.underlying.attempt, .ptr = self, .get = get, .put = put, .check = check };
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

const Entry = struct {
    key: []const u8,
    value: []const u8 = "",
    child: ?Ref = null,

    fn size(self: Entry) usize {
        return 8 + self.key.len + if (self.child != null) ref_bytes else self.value.len;
    }
};

const Page = struct {
    bytes: []u8,
    entries: []Entry,
    ref: Ref,

    fn deinit(self: *Page, alloc: Allocator) void {
        alloc.free(self.entries);
        alloc.free(self.bytes);
        self.* = undefined;
    }
};

const Child = struct {
    first: []u8,
    ref: Ref,

    fn deinit(self: Child, alloc: Allocator) void {
        alloc.free(self.first);
    }
};

const Children = std.ArrayListUnmanaged(Child);

fn freeChildren(alloc: Allocator, children: *Children) void {
    for (children.items) |child| child.deinit(alloc);
    children.deinit(alloc);
}

fn appendChild(alloc: Allocator, children: *Children, first: []const u8, ref: Ref) !void {
    const owned = try alloc.dupe(u8, first);
    errdefer alloc.free(owned);
    try children.append(alloc, .{ .first = owned, .ref = ref });
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

fn load(alloc: Allocator, store: Store, ref: Ref) !Page {
    try store.check(store.ptr);
    try ref.validate();
    const bytes = try store.get(store.ptr, alloc, ref);
    errdefer alloc.free(bytes);
    if (bytes.len != ref.bytes) return error.InvalidGraphPage;
    var digest: [32]u8 = undefined;
    Sha256.hash(bytes, &digest, .{});
    if (!std.mem.eql(u8, &digest, &ref.digest)) return error.ArtifactIntegrityMismatch;
    if (!std.mem.eql(u8, bytes[24..56], &store.domain)) return error.GraphPageDomainMismatch;
    if (!std.mem.eql(u8, bytes[0..8], magic) or bytes[8] != ref.height or
        !std.mem.allEqual(u8, bytes[9..12], 0) or readU64(bytes, 16) != ref.records)
        return error.InvalidGraphPage;
    const count = readU32(bytes, 12);
    if (count == 0 or count > (bytes.len - header_bytes) / 9) return error.InvalidGraphPage;
    const entries = try alloc.alloc(Entry, count);
    errdefer alloc.free(entries);
    var offset: usize = header_bytes;
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
            if (value_len != ref_bytes) return error.InvalidGraphPage;
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
        }
        if (entry.size() > max_record_bytes) return error.InvalidGraphPage;
    }
    if (offset != bytes.len or records != ref.records) return error.InvalidGraphPage;
    return .{ .bytes = bytes, .entries = entries, .ref = ref };
}

fn save(alloc: Allocator, store: Store, entries: []const Entry, height: u8) !Ref {
    try store.check(store.ptr);
    if (entries.len == 0 or height > max_height) return error.InvalidGraphPage;
    var size: usize = header_bytes;
    var records: u64 = 0;
    for (entries) |entry| {
        size += entry.size();
        records = try std.math.add(u64, records, if (entry.child) |child| child.records else 1);
    }
    if (size > max_page_bytes) return error.GraphPageTooLarge;
    const bytes = try alloc.alloc(u8, size);
    defer alloc.free(bytes);
    @memcpy(bytes[0..8], magic);
    @memcpy(bytes[24..56], &store.domain);
    bytes[8] = height;
    @memset(bytes[9..12], 0);
    writeU32(bytes, 12, entries.len);
    writeU64(bytes, 16, records);
    var offset: usize = header_bytes;
    for (entries) |entry| {
        writeU32(bytes, offset, entry.key.len);
        writeU32(bytes, offset + 4, if (entry.child != null) ref_bytes else entry.value.len);
        offset += 8;
        @memcpy(bytes[offset..][0..entry.key.len], entry.key);
        offset += entry.key.len;
        if (entry.child) |child| {
            @memcpy(bytes[offset..][0..32], &child.digest);
            writeU32(bytes, offset + 32, child.bytes);
            bytes[offset + 36] = child.height;
            writeU64(bytes, offset + 37, child.records);
            @memcpy(bytes[offset + 45 ..][0..16], &child.attempt);
            offset += ref_bytes;
        } else {
            @memcpy(bytes[offset..][0..entry.value.len], entry.value);
            offset += entry.value.len;
        }
    }
    var digest: [32]u8 = undefined;
    Sha256.hash(bytes, &digest, .{});
    const ref: Ref = .{ .digest = digest, .attempt = store.attempt, .bytes = @intCast(size), .height = height, .records = records };
    try store.put(store.ptr, ref, bytes);
    return ref;
}

/// Pack toward half-full pages, allowing one indivisible record above the
/// target. Split boundaries depend only on ordered records, not process state.
fn pack(alloc: Allocator, store: Store, entries: []const Entry, height: u8, out: *Children) !void {
    var begin: usize = 0;
    while (begin < entries.len) {
        var end = begin;
        var bytes: usize = header_bytes;
        while (end < entries.len) : (end += 1) {
            const size = entries[end].size();
            if (end - begin >= (if (height == 0) @as(usize, 1) else 2) and bytes + size > target_page_bytes) break;
            bytes += size;
        }
        const ref = try save(alloc, store, entries[begin..end], height);
        try appendChild(alloc, out, entries[begin].key, ref);
        begin = end;
    }
}

fn childEntries(alloc: Allocator, children: []const Child) ![]Entry {
    const entries = try alloc.alloc(Entry, children.len);
    for (entries, children) |*entry, child| entry.* = .{ .key = child.first, .child = child.ref };
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
        if (left.bytes.len + right.bytes.len - header_bytes <= max_page_bytes) {
            const ref = try save(alloc, store, entries, a.ref.height);
            try appendChild(alloc, &replacement, entries[0].key, ref);
        } else {
            try pack(alloc, store, entries, a.ref.height, &replacement);
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
                try appendChild(alloc, out, page.entries[0].key, prior);
                return;
            }
        }
        return pack(alloc, store, entries.items, 0, out);
    }
    var children: Children = .empty;
    defer freeChildren(alloc, &children);
    var begin: usize = 0;
    var changed = false;
    for (page.entries, 0..) |entry, i| {
        var end = begin;
        while (end < changes.len and (i + 1 == page.entries.len or less(changes[end].key, page.entries[i + 1].key))) : (end += 1) {}
        if (end == begin) {
            try appendChild(alloc, &children, entry.key, entry.child.?);
        } else {
            const count = children.items.len;
            try edit(alloc, store, entry.child.?, changes[begin..end], &children, entry.key, if (i + 1 < page.entries.len) page.entries[i + 1].key else end_key);
            if (children.items.len != count + 1 or !children.items[count].ref.eql(entry.child.?)) changed = true;
        }
        begin = end;
    }
    if (!changed) return appendChild(alloc, out, page.entries[0].key, prior);
    try rebalance(alloc, store, &children);
    const entries = try childEntries(alloc, children.items);
    defer alloc.free(entries);
    try pack(alloc, store, entries, prior.height, out);
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

/// Construct an initial tree from a strictly ordered stream. Only one pending
/// page per height is retained; leaf records and every level's routing entries
/// are released as soon as their page is persisted. The source's borrowed
/// record may be invalidated by its next call. Like incremental apply, callers
/// must account for orphan uploads if source, allocation or storage fails.
pub fn buildSorted(alloc: Allocator, store: Store, source: anytype) !?Ref {
    var builder: SortedBuilder = .{ .alloc = alloc, .store = store };
    defer builder.deinit();
    while (try source.next()) |record| {
        try store.check(store.ptr);
        if (record.key.len == 0 or record.key.len > max_key_bytes or
            record.value.len > max_record_bytes - 8 - record.key.len)
            return error.GraphPageRecordTooLarge;
        if (builder.previous) |previous| if (!less(previous, record.key)) return error.UnsortedGraphPageMutations;
        const previous = try alloc.dupe(u8, record.key);
        errdefer alloc.free(previous);
        try builder.append(0, .{ .key = record.key, .value = record.value });
        if (builder.previous) |old| alloc.free(old);
        builder.previous = previous;
    }
    try store.check(store.ptr);
    for (0..builder.levels.len) |height| {
        const level = &builder.levels[height];
        if (level.entries.items.len == 0) continue;
        if (height > 0 and level.entries.items.len == 1) {
            var higher_pending = false;
            for (builder.levels[height + 1 ..]) |higher| higher_pending = higher_pending or higher.entries.items.len > 0;
            if (!higher_pending) return level.entries.items[0].child.?;
        }
        try builder.flush(height);
    }
    return null;
}

const SortedBuilder = struct {
    alloc: Allocator,
    store: Store,
    previous: ?[]u8 = null,
    // The extra slot holds the final root ref, never another persisted page.
    levels: [max_height + 2]Level = @splat(.{}),

    const Level = struct {
        entries: std.ArrayListUnmanaged(Entry) = .empty,
        bytes: usize = header_bytes,

        fn clear(self: *Level, alloc: Allocator) void {
            for (self.entries.items) |entry| {
                alloc.free(entry.key);
                if (entry.child == null) alloc.free(entry.value);
            }
            self.entries.clearRetainingCapacity();
            self.bytes = header_bytes;
        }
    };

    fn deinit(self: *SortedBuilder) void {
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
        if (level.entries.items.len >= minimum and level.bytes + entry.size() > target_page_bytes)
            try self.flush(height);
        const key = try self.alloc.dupe(u8, entry.key);
        errdefer self.alloc.free(key);
        const value = if (entry.child == null) try self.alloc.dupe(u8, entry.value) else "";
        errdefer if (entry.child == null) self.alloc.free(value);
        try level.entries.append(self.alloc, .{ .key = key, .value = value, .child = entry.child });
        level.bytes += entry.size();
    }

    fn flush(self: *SortedBuilder, height: usize) anyerror!void {
        if (height > max_height) return error.GraphPageTreeTooDeep;
        const level = &self.levels[height];
        const ref = try save(self.alloc, self.store, level.entries.items, @intCast(height));
        try self.append(height + 1, .{ .key = level.entries.items[0].key, .child = ref });
        level.clear(self.alloc);
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

    const Frame = struct { page: Page, next: usize, end: ?[]const u8 };
    pub const Record = struct { key: []const u8, value: []const u8 };

    pub fn init(alloc: Allocator, store: Store, root: ?Ref, lower: []const u8, upper: ?[]const u8) !Cursor {
        var self: Cursor = .{ .alloc = alloc, .store = store, .upper = upper };
        errdefer self.deinit();
        if (upper) |bound| if (!less(lower, bound)) return self;
        if (root) |ref| try self.descend(ref, lower, null, null);
        return self;
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

    pub fn next(self: *Cursor) !?Record {
        try self.store.check(self.store.ptr);
        while (self.depth != 0) {
            const frame = &self.frames[self.depth - 1];
            if (frame.next == frame.page.entries.len) {
                frame.page.deinit(self.alloc);
                self.depth -= 1;
                continue;
            }
            const entry = frame.page.entries[frame.next];
            if (self.upper) |bound| if (!less(entry.key, bound)) {
                self.deinit();
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

/// Reachability traversal in child-before-parent order. A reclamation caller
/// must skip protected digests and delete a page only in visit. If interrupted,
/// the surviving parent still identifies every remaining descendant. Missing
/// pages are tolerated only for reclamation replay, never for retained roots.
/// Optional visitRecord callbacks borrow leaf bytes only for the duration of
/// the call and run before visit, allowing body-before-page reclamation without
/// reloading the leaf.
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

test "serverless graph page tree rank cursors resume without reading earlier leaves" {
    const a = std.testing.allocator;
    var backing: TestStore = .{ .alloc = a };
    defer backing.deinit();
    const count = 2000;
    const names = try a.alloc([8]u8, count);
    defer a.free(names);
    const changes = try a.alloc(Mutation, count);
    defer a.free(changes);
    const value = [_]u8{42} ** 128;
    for (names, changes, 0..) |*name, *change, i| {
        std.mem.writeInt(u64, name, i, .big);
        change.* = .{ .key = name, .value = &value };
    }
    const root = (try apply(a, backing.store(), null, changes)).?;
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
    const value = [_]u8{42} ** 64;
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
    const value = [_]u8{7} ** 8000;
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
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationExercise, .{});
}

test "serverless graph page tree interrupted uploads never change the published root" {
    const alloc = std.testing.allocator;
    var backing: TestStore = .{ .alloc = alloc };
    defer backing.deinit();
    const value = [_]u8{17} ** 16000;
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
    const value = [_]u8{17} ** 16000;
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
    var present = [_]bool{false} ** count;
    const value = [_]u8{9} ** 1024;
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
    const value = [_]u8{3} ** 16000;
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
