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

//! Native single-file Antfly Lite format primitives.
//!
//! This module owns the v3-native `.aflite` on-disk header and checkpoint-slot
//! layout plus the first native page stores used by the Lite backend.

const std = @import("std");
const builtin = @import("builtin");
const Crc32 = @import("antfly_hash").Crc32;
const antfly_platform = @import("antfly_platform");
const platform_sync = antfly_platform.sync;
const fs_paths = @import("../../common/fs_paths.zig");
const threaded_io_limits = @import("../../common/threaded_io_limits.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const maintenance = @import("../maintenance.zig");

const Allocator = std.mem.Allocator;

/// Per-operation admission for newly written external payload pages. Catalog
/// records and tree navigation pages remain eligible for caching. Bypassing
/// admission still invalidates both cached bytes and links for reused page IDs.
pub const WriteOptions = struct {
    payload_cache: enum { normal, cold_sequential } = .normal,
};

pub const magic = "AFLITE\x03P";
const unpacked_v3_magic = "AFLITE\x03N";
pub const format_version: u32 = 3;
pub const default_page_size: u32 = 4096;
pub const header_size: usize = 4096;
pub const checkpoint_slot_count = 2;
pub const checkpoint_slot_size: usize = 72;
pub const page_magic = "AFLP";
pub const page_header_size: usize = 16;

const magic_offset: usize = 0;
const version_offset: usize = 8;
const page_size_offset: usize = 12;
const header_size_offset: usize = 16;
const active_checkpoint_offset: usize = 20;
const checkpoint_slots_offset: usize = 64;
const checkpoint_slots_end: usize = checkpoint_slots_offset + checkpoint_slot_count * checkpoint_slot_size;
const checkpoint_slot_payload_size: usize = 64;
const checkpoint_slot_checksum_offset: usize = checkpoint_slot_payload_size;
const header_checksum_offset: usize = header_size - 4;
const page_crc_offset: usize = 12;

pub const PageKind = enum(u8) {
    data = 1,
    catalog = 2,
    document = 3,
    value = 4,
    free_map = 5,
    document_index = 6,
    value_extent = 7,
    catalog_index = 8,
    record_bundle = 9,
};

// Record references reserve the high bit and a 16-bit byte offset. Ordinary
// page IDs retain their existing encoding; packed references never name value
// or index pages. Bounds and record boundaries are checked before decoding.
const packed_record_flag: u64 = @as(u64, 1) << 63;
const packed_page_mask: u64 = (@as(u64, 1) << 47) - 1;
fn physicalPage(reference: u64) u64 {
    return if (reference & packed_record_flag != 0) reference & packed_page_mask else reference;
}

fn recordWalkLimit(checkpoint: CheckpointSlot, page_size: u32) u64 {
    return checkpoint.page_count *| @as(u64, page_size / 4);
}

const catalog_key_len_mask: u32 = 0x00ff_ffff;
const catalog_delete_flag: u32 = 1 << 31;
const catalog_external_value_flag: u32 = 1 << 30;
const document_delete_flag: u8 = 1 << 0;
const document_external_value_flag: u8 = 1 << 1;
const document_namespace_link_flag: u8 = 1 << 2;
const namespace_directory_key = "\x00antfly.document_namespaces.v1";
pub const secret_catalog_prefix = "\x00antfly.secrets.v1/";
const namespace_directory_magic = "AFNSIDX2";
const namespace_directory_snapshot_interval: u16 = 256;
const value_page_header_size: usize = 8;
const free_map_format_version: u32 = 1;
const free_map_header_size: usize = 16;
const document_index_magic = "AFDIDX02";
const document_index_header_size: usize = 12;

const DocumentIndexNodeKind = enum(u8) {
    leaf = 1,
    internal = 2,
};

// Bound every encoded key slot so an insertion can always split into two
// pages. Long keys borrow their bytes from immutable catalog/document records;
// separators retain the same record reference through copy-on-write splits.
const index_inline_key_limit = 512;
const index_external_key_marker = std.math.maxInt(u16);

const DocumentIndexNode = struct {
    kind: DocumentIndexNodeKind,
    keys: [][]u8,
    pointers: []u64,
    key_pages: ?[]u64 = null,

    fn deinit(self: *DocumentIndexNode, allocator: Allocator) void {
        for (self.keys) |key| allocator.free(key);
        allocator.free(self.keys);
        allocator.free(self.pointers);
        if (self.key_pages) |pages| allocator.free(pages);
        self.* = undefined;
    }
};

pub const DocumentIndexEntry = struct {
    key: []u8,
    document_page_id: u64,

    pub fn deinit(self: *DocumentIndexEntry, allocator: Allocator) void {
        allocator.free(self.key);
        self.* = undefined;
    }
};

/// Cursor over one checkpoint's copy-on-write document index. The cursor owns
/// one decoded root-to-leaf path, so sequential scans read each index page once
/// instead of repeating a root seek for every key. Memory is bounded by the
/// decoded path, including any referenced overflow keys, rather than key count.
pub const DocumentIndexCursor = struct {
    const Frame = struct {
        node: DocumentIndexNode,
        position: usize,
    };

    file: *NativeFile,
    checkpoint: CheckpointSlot,
    frames: std.ArrayListUnmanaged(Frame) = .empty,

    pub fn init(file: *NativeFile, checkpoint: CheckpointSlot) DocumentIndexCursor {
        return .{ .file = file, .checkpoint = checkpoint };
    }

    pub fn deinit(self: *DocumentIndexCursor) void {
        self.clear();
        self.frames.deinit(self.file.allocator);
        self.* = undefined;
    }

    pub fn first(self: *DocumentIndexCursor) !?DocumentIndexEntry {
        self.clear();
        if (self.checkpoint.document_index_root_page == 0) return null;
        try self.descendExtreme(self.checkpoint.document_index_root_page, true);
        return try self.currentEntry();
    }

    pub fn last(self: *DocumentIndexCursor) !?DocumentIndexEntry {
        self.clear();
        if (self.checkpoint.document_index_root_page == 0) return null;
        try self.descendExtreme(self.checkpoint.document_index_root_page, false);
        return try self.currentEntry();
    }

    pub fn seekAtOrAfter(self: *DocumentIndexCursor, key: []const u8, strict: bool) !?DocumentIndexEntry {
        self.clear();
        var page_id = self.checkpoint.document_index_root_page;
        while (page_id != 0) {
            if (self.frames.items.len > 64) return error.InvalidDocumentIndex;
            var node = try self.file.readDocumentIndexNode(page_id, self.checkpoint);
            if (node.kind == .leaf) {
                const position = if (strict) upperBoundIndexKeys(node.keys, key) else lowerBoundIndexKeys(node.keys, key);
                self.frames.append(self.file.allocator, .{ .node = node, .position = position }) catch |err| {
                    node.deinit(self.file.allocator);
                    return err;
                };
                if (position < node.keys.len) return try self.currentEntry();
                return try self.next();
            }
            const position = upperBoundIndexKeys(node.keys, key);
            page_id = node.pointers[position];
            self.frames.append(self.file.allocator, .{ .node = node, .position = position }) catch |err| {
                node.deinit(self.file.allocator);
                return err;
            };
        }
        return null;
    }

    pub fn seekAtOrBefore(self: *DocumentIndexCursor, key: []const u8, strict: bool) !?DocumentIndexEntry {
        self.clear();
        var page_id = self.checkpoint.document_index_root_page;
        while (page_id != 0) {
            if (self.frames.items.len > 64) return error.InvalidDocumentIndex;
            var node = try self.file.readDocumentIndexNode(page_id, self.checkpoint);
            if (node.kind == .leaf) {
                const bound = if (strict) lowerBoundIndexKeys(node.keys, key) else upperBoundIndexKeys(node.keys, key);
                self.frames.append(self.file.allocator, .{ .node = node, .position = if (bound == 0) 0 else bound - 1 }) catch |err| {
                    node.deinit(self.file.allocator);
                    return err;
                };
                if (bound > 0) return try self.currentEntry();
                return try self.prev();
            }
            const position = upperBoundIndexKeys(node.keys, key);
            page_id = node.pointers[position];
            self.frames.append(self.file.allocator, .{ .node = node, .position = position }) catch |err| {
                node.deinit(self.file.allocator);
                return err;
            };
        }
        return null;
    }

    pub fn next(self: *DocumentIndexCursor) !?DocumentIndexEntry {
        if (self.frames.items.len == 0) return null;
        const leaf = &self.frames.items[self.frames.items.len - 1];
        if (leaf.node.kind != .leaf) return error.InvalidDocumentIndex;
        if (leaf.position + 1 < leaf.node.keys.len) {
            leaf.position += 1;
            return try self.currentEntry();
        }
        self.popFrame();
        while (self.frames.items.len > 0) {
            const parent = &self.frames.items[self.frames.items.len - 1];
            if (parent.node.kind != .internal) return error.InvalidDocumentIndex;
            if (parent.position + 1 < parent.node.pointers.len) {
                parent.position += 1;
                const child = parent.node.pointers[parent.position];
                try self.descendExtreme(child, true);
                return try self.currentEntry();
            }
            self.popFrame();
        }
        return null;
    }

    pub fn prev(self: *DocumentIndexCursor) !?DocumentIndexEntry {
        if (self.frames.items.len == 0) return null;
        const leaf = &self.frames.items[self.frames.items.len - 1];
        if (leaf.node.kind != .leaf) return error.InvalidDocumentIndex;
        if (leaf.position > 0 and leaf.position <= leaf.node.keys.len) {
            leaf.position -= 1;
            return try self.currentEntry();
        }
        self.popFrame();
        while (self.frames.items.len > 0) {
            const parent = &self.frames.items[self.frames.items.len - 1];
            if (parent.node.kind != .internal) return error.InvalidDocumentIndex;
            if (parent.position > 0) {
                parent.position -= 1;
                const child = parent.node.pointers[parent.position];
                try self.descendExtreme(child, false);
                return try self.currentEntry();
            }
            self.popFrame();
        }
        return null;
    }

    fn descendExtreme(self: *DocumentIndexCursor, root_page_id: u64, toward_first: bool) !void {
        var page_id = root_page_id;
        while (page_id != 0) {
            if (self.frames.items.len > 64) return error.InvalidDocumentIndex;
            var node = try self.file.readDocumentIndexNode(page_id, self.checkpoint);
            const position = switch (node.kind) {
                .leaf => if (toward_first) 0 else node.keys.len - 1,
                .internal => if (toward_first) 0 else node.pointers.len - 1,
            };
            const next_page = if (node.kind == .internal) node.pointers[position] else 0;
            self.frames.append(self.file.allocator, .{ .node = node, .position = position }) catch |err| {
                node.deinit(self.file.allocator);
                return err;
            };
            page_id = next_page;
        }
    }

    fn currentEntry(self: *DocumentIndexCursor) !?DocumentIndexEntry {
        if (self.frames.items.len == 0) return null;
        const frame = &self.frames.items[self.frames.items.len - 1];
        if (frame.node.kind != .leaf or frame.position >= frame.node.keys.len) return null;
        return .{
            .key = try self.file.allocator.dupe(u8, frame.node.keys[frame.position]),
            .document_page_id = frame.node.pointers[frame.position],
        };
    }

    fn popFrame(self: *DocumentIndexCursor) void {
        var frame = self.frames.pop().?;
        frame.node.deinit(self.file.allocator);
    }

    fn clear(self: *DocumentIndexCursor) void {
        while (self.frames.items.len > 0) self.popFrame();
    }
};

/// A cursor-local view of one immutable physical record page. Decode and check
/// its checksum once, index bundle boundaries once, and borrow payload slices
/// until the next physical page is loaded. Memory is bounded by page size.
/// The caller must keep the file generation and checkpoint pinned for its life.
pub const RecordPageReader = struct {
    raw: ?[]u8 = null,
    payload: []const u8 = &.{},
    page_id: u64 = 0,
    kind: PageKind = .data,
    offsets: std.ArrayListUnmanaged(u16) = .empty,

    pub fn deinit(self: *RecordPageReader, allocator: Allocator) void {
        if (self.raw) |raw| allocator.free(raw);
        self.offsets.deinit(allocator);
        self.* = .{};
    }

    fn read(self: *RecordPageReader, file: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot, reference: u64, expected: PageKind) ![]const u8 {
        const page = physicalPage(reference);
        if (page == 0 or page >= checkpoint.page_count) return error.InvalidPageId;
        const bundled = reference & packed_record_flag != 0;
        const kind: PageKind = if (bundled) .record_bundle else expected;
        if (self.page_id != page) {
            // Invalidate before any fallible load, so a failed validation can
            // never leave a reusable, partially validated view.
            self.page_id = 0;
            self.payload = &.{};
            self.offsets.clearRetainingCapacity();
            if (self.raw) |raw| allocator.free(raw);
            self.raw = null;
            self.raw = try file.readPhysicalPageAlloc(allocator, page, checkpoint);
            self.payload = try decodePagePayload(self.raw.?, kind);
            if (bundled) {
                var offset: usize = 0;
                while (offset < self.payload.len) {
                    const record = try packedRecordAtOffset(self.payload, offset);
                    try self.offsets.append(allocator, @intCast(offset));
                    offset += 4 + record.bytes.len;
                }
            }
            self.kind = kind;
            self.page_id = page;
        }
        if (self.kind != kind) return error.InvalidNativePageKind;
        if (!bundled) return self.payload;
        const wanted: u16 = @intCast((reference >> 47) & 0xffff);
        var low: usize = 0;
        var high = self.offsets.items.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (self.offsets.items[mid] < wanted) low = mid + 1 else high = mid;
        }
        if (low == self.offsets.items.len or self.offsets.items[low] != wanted) return error.InvalidPageId;
        const record = try packedRecordAtOffset(self.payload, wanted);
        if (record.kind != expected) return error.InvalidNativePageKind;
        return record.bytes;
    }

    pub fn documentValueAlloc(self: *RecordPageReader, file: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot, indexed: DocumentIndexEntry) !?[]u8 {
        const entry = try decodeDocumentEntry(try self.read(file, allocator, checkpoint, indexed.document_page_id, .document));
        if (!std.mem.eql(u8, entry.key, indexed.key)) return error.InvalidDocumentIndex;
        if (entry.is_delete) return null;
        return if (entry.external_value_root_page != 0)
            try file.readValuePagesAtCheckpointAlloc(allocator, entry.external_value_root_page, entry.external_value_len, checkpoint)
        else
            try allocator.dupe(u8, entry.value);
    }
};

/// Ordered live catalog keys at a pinned checkpoint. The prefix is borrowed
/// for the cursor lifetime. Callers fence generation replacement (vacuum) while
/// using this cursor; ordinary copy-on-write commits may continue concurrently.
pub const CatalogCursor = struct {
    records: RecordPageReader = .{},
    index: DocumentIndexCursor,
    prefix: []const u8,
    started: bool = false,
    done: bool = false,
    immediate_children_only: bool = false,

    pub fn deinit(self: *CatalogCursor) void {
        self.records.deinit(self.index.file.allocator);
        self.index.deinit();
        self.* = undefined;
    }

    pub fn nextRecordAlloc(self: *CatalogCursor, allocator: Allocator) !?OwnedCatalogRecord {
        const file = self.index.file;
        const key = (try self.next()) orelse return null;
        defer file.allocator.free(key.key);
        const entry = try decodeCatalogEntry(try self.records.read(file, file.allocator, self.index.checkpoint, key.page_id, .catalog));
        const value = try file.catalogEntryValueAtCheckpointAlloc(allocator, entry, self.index.checkpoint);
        errdefer allocator.free(value);
        return .{ .key = try allocator.dupe(u8, key.key), .value = value };
    }

    /// The caller owns the returned key, allocated with the native file allocator.
    pub fn next(self: *CatalogCursor) !?OwnedCatalogKey {
        if (self.done) return null;
        const file = self.index.file;
        var candidate = if (self.started) try self.index.next() else try self.index.seekAtOrAfter(self.prefix, false);
        self.started = true;
        while (candidate) |owned| {
            var entry = owned;
            defer entry.deinit(file.allocator);
            if (!std.mem.startsWith(u8, entry.key, self.prefix)) break;
            if (self.immediate_children_only) {
                if (std.mem.indexOfScalar(u8, entry.key[self.prefix.len..], '/')) |slash| {
                    // '/' is not the maximum byte, so incrementing the final
                    // slash is the exact exclusive upper bound of this subtree.
                    // Seek before decoding any descendant catalog records.
                    const bound = try file.allocator.dupe(u8, entry.key[0 .. self.prefix.len + slash + 1]);
                    defer file.allocator.free(bound);
                    bound[bound.len - 1] += 1;
                    candidate = try self.index.seekAtOrAfter(bound, false);
                    continue;
                }
            }
            const record = try decodeCatalogEntry(try self.records.read(file, file.allocator, self.index.checkpoint, entry.document_page_id, .catalog));
            if (!std.mem.eql(u8, entry.key, record.key)) return error.InvalidDocumentIndex;
            if (record.is_delete) {
                candidate = try self.index.next();
                continue;
            }
            const key = entry.key;
            entry.key = &.{};
            return .{ .key = key, .page_id = entry.document_page_id };
        }
        self.done = true;
        return null;
    }
};

/// Transaction-local copy-on-write B+ tree editor. Each visited page is decoded
/// once, external keys stay references until compared, and only the final dirty
/// nodes reachable from the new root are written. Deletion merges or redistributes
/// neighboring nodes; old checkpoints retain their original pages and separators.
const IndexEditor = struct {
    const Key = struct {
        bytes: []const u8,
        page: u64 = 0,

        fn slotSize(self: Key) usize {
            return 10 + (if (self.page != 0) @as(usize, 8) else self.bytes.len);
        }
    };
    const Link = struct { page: u64 = 0, node: ?*Node = null };
    const Node = struct {
        kind: DocumentIndexNodeKind,
        page: u64 = 0,
        dirty: bool = true,
        keys: std.ArrayListUnmanaged(Key) = .empty,
        links: std.ArrayListUnmanaged(Link) = .empty,

        fn size(self: *const Node) usize {
            var result: usize = document_index_header_size + @as(usize, if (self.kind == .internal) 8 else 0);
            for (self.keys.items) |key| result += key.slotSize();
            return result;
        }
    };
    const Split = struct { separator: Key, right: Link };
    const Erased = struct { removed: bool, split: ?Split = null };

    file: *NativeFile,
    checkpoint: CheckpointSlot,
    arena: std.heap.ArenaAllocator,
    root: Link,

    fn init(file: *NativeFile, checkpoint: CheckpointSlot, root: u64) IndexEditor {
        return .{ .file = file, .checkpoint = checkpoint, .arena = .init(file.allocator), .root = .{ .page = root } };
    }

    fn deinit(self: *IndexEditor) void {
        self.arena.deinit();
    }

    fn newNode(self: *IndexEditor, kind: DocumentIndexNodeKind) !*Node {
        const node = try self.arena.allocator().create(Node);
        node.* = .{ .kind = kind };
        return node;
    }

    fn load(self: *IndexEditor, link: *Link) !*Node {
        if (link.node) |node| return node;
        const payload = try self.file.readPagePayloadByKindAllocForCheckpoint(self.file.allocator, link.page, .document_index, self.checkpoint);
        defer self.file.allocator.free(payload);
        const alloc = self.arena.allocator();
        // Decoded buffers belong to the editor arena; transfer key slices
        // directly instead of allocating another copy of every inline key.
        const decoded = try decodeDocumentIndexNode(alloc, payload);
        const node = try self.newNode(decoded.kind);
        node.page = link.page;
        node.dirty = false;
        try node.keys.ensureTotalCapacity(alloc, decoded.keys.len);
        try node.links.ensureTotalCapacity(alloc, decoded.pointers.len);
        for (decoded.keys, decoded.key_pages.?) |key, page| {
            node.keys.appendAssumeCapacity(.{ .bytes = key, .page = page });
        }
        for (decoded.pointers) |page| node.links.appendAssumeCapacity(.{ .page = page });
        link.node = node;
        return node;
    }

    fn resolve(self: *IndexEditor, key: *Key) ![]const u8 {
        if (key.page == 0 or key.bytes.len != 0) return key.bytes;
        const raw = try self.file.readPageAllocForCheckpoint(self.file.allocator, key.page, self.checkpoint);
        defer self.file.allocator.free(raw);
        const bytes = switch (raw[4]) {
            @intFromEnum(PageKind.catalog) => (try decodeCatalogEntry(try decodePagePayload(raw, .catalog))).key,
            @intFromEnum(PageKind.document) => (try decodeDocumentEntry(try decodePagePayload(raw, .document))).key,
            else => return error.InvalidDocumentIndex,
        };
        if (bytes.len <= index_inline_key_limit) return error.InvalidDocumentIndex;
        key.bytes = try self.arena.allocator().dupe(u8, bytes);
        return key.bytes;
    }

    fn bound(self: *IndexEditor, node: *Node, key: []const u8, upper: bool) !usize {
        var low: usize = 0;
        var high = node.keys.items.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const order = std.mem.order(u8, try self.resolve(&node.keys.items[mid]), key);
            if (order == .lt or (upper and order == .eq)) low = mid + 1 else high = mid;
        }
        return low;
    }

    fn put(self: *IndexEditor, key: []const u8, page: u64) !void {
        if (self.root.page == 0 and self.root.node == null) self.root.node = try self.newNode(.leaf);
        if (try self.insert(try self.load(&self.root), key, page, 0)) |split| {
            const root = try self.newNode(.internal);
            try root.keys.append(self.arena.allocator(), split.separator);
            try root.links.appendSlice(self.arena.allocator(), &.{ self.root, split.right });
            self.root = .{ .node = root };
        }
    }

    fn insert(self: *IndexEditor, node: *Node, key: []const u8, page: u64, depth: usize) anyerror!?Split {
        if (depth > 64) return error.InvalidDocumentIndex;
        const alloc = self.arena.allocator();
        const index = try self.bound(node, key, node.kind == .internal);
        if (node.kind == .leaf) {
            const replacement = index < node.keys.items.len and std.mem.eql(u8, try self.resolve(&node.keys.items[index]), key);
            const owned = Key{ .bytes = try alloc.dupe(u8, key), .page = if (key.len > index_inline_key_limit) page else 0 };
            if (replacement) {
                node.keys.items[index] = owned;
                node.links.items[index] = .{ .page = page };
            } else {
                try node.keys.insert(alloc, index, owned);
                try node.links.insert(alloc, index, .{ .page = page });
            }
        } else {
            const child = try self.load(&node.links.items[index]);
            if (try self.insert(child, key, page, depth + 1)) |split| {
                try node.keys.insert(alloc, index, split.separator);
                try node.links.insert(alloc, index + 1, split.right);
            }
        }
        node.dirty = true;
        if (node.size() <= self.file.maxPagePayloadBytes()) return null;
        return try self.splitNode(node);
    }

    /// Choose a byte-balanced split in linear time without fetching key bytes.
    fn splitNode(self: *IndexEditor, node: *Node) !Split {
        const capacity = self.file.maxPagePayloadBytes();
        const header: usize = document_index_header_size + @as(usize, if (node.kind == .internal) 8 else 0);
        const total = node.size() - header;
        var prefix: usize = 0;
        var best: ?usize = null;
        var imbalance: usize = std.math.maxInt(usize);
        for (node.keys.items, 0..) |key, i| {
            const left = header + prefix;
            const right = header + total - prefix - (if (node.kind == .internal) key.slotSize() else 0);
            if ((node.kind == .internal or i > 0) and left <= capacity and right <= capacity) {
                const delta = if (left > right) left - right else right - left;
                if (delta < imbalance) {
                    best = i;
                    imbalance = delta;
                }
            }
            prefix += key.slotSize();
        }
        const at = best orelse return error.DocumentIndexNodeTooLarge;
        const right = try self.newNode(node.kind);
        const separator = node.keys.items[at];
        const key_start = at + @as(usize, if (node.kind == .internal) 1 else 0);
        const link_start = key_start;
        try right.keys.appendSlice(self.arena.allocator(), node.keys.items[key_start..]);
        try right.links.appendSlice(self.arena.allocator(), node.links.items[link_start..]);
        node.keys.items.len = at;
        node.links.items.len = at + @as(usize, if (node.kind == .internal) 1 else 0);
        node.dirty = true;
        return .{ .separator = separator, .right = .{ .node = right } };
    }

    fn remove(self: *IndexEditor, key: []const u8) !void {
        if (self.root.page == 0 and self.root.node == null) return;
        const root = try self.load(&self.root);
        const result = try self.erase(root, key, 0);
        if (!result.removed) return;
        if (result.split) |split| {
            const parent = try self.newNode(.internal);
            try parent.keys.append(self.arena.allocator(), split.separator);
            try parent.links.appendSlice(self.arena.allocator(), &.{ self.root, split.right });
            self.root = .{ .node = parent };
        }
        if (root.links.items.len == 0) {
            self.root = .{};
            return;
        }
        var depth: usize = 0;
        while (true) : (depth += 1) {
            if (depth > 64) return error.InvalidDocumentIndex;
            const current = try self.load(&self.root);
            if (current.kind != .internal or current.links.items.len != 1) break;
            self.root = current.links.items[0];
        }
    }

    fn erase(self: *IndexEditor, node: *Node, key: []const u8, depth: usize) anyerror!Erased {
        if (depth > 64) return error.InvalidDocumentIndex;
        const index = try self.bound(node, key, node.kind == .internal);
        if (node.kind == .leaf) {
            if (index == node.keys.items.len or !std.mem.eql(u8, try self.resolve(&node.keys.items[index]), key)) return .{ .removed = false };
            _ = node.keys.orderedRemove(index);
            _ = node.links.orderedRemove(index);
        } else {
            const child = try self.load(&node.links.items[index]);
            const result = try self.erase(child, key, depth + 1);
            if (!result.removed) return result;
            if (result.split) |split| {
                try node.keys.insert(self.arena.allocator(), index, split.separator);
                try node.links.insert(self.arena.allocator(), index + 1, split.right);
            } else if (child.links.items.len == 0) {
                _ = node.links.orderedRemove(index);
                if (node.keys.items.len != 0) _ = node.keys.orderedRemove(if (index == 0) 0 else index - 1);
            } else if (node.links.items.len > 1 and child.size() < self.file.maxPagePayloadBytes() / 2) {
                try self.rebalance(node, if (index == 0) 0 else index - 1);
            }
        }
        node.dirty = true;
        // Replacing a separator during redistribution can grow a parent even
        // during deletion: variable-width inline keys need the same split
        // propagation as insertion.
        return .{ .removed = true, .split = if (node.size() > self.file.maxPagePayloadBytes()) try self.splitNode(node) else null };
    }

    fn rebalance(self: *IndexEditor, parent: *Node, left_index: usize) !void {
        const left = try self.load(&parent.links.items[left_index]);
        const right = try self.load(&parent.links.items[left_index + 1]);
        if (left.kind != right.kind) return error.InvalidDocumentIndex;
        const alloc = self.arena.allocator();
        if (left.kind == .internal) try left.keys.append(alloc, parent.keys.items[left_index]);
        try left.keys.appendSlice(alloc, right.keys.items);
        try left.links.appendSlice(alloc, right.links.items);
        left.dirty = true;
        if (left.size() <= self.file.maxPagePayloadBytes()) {
            _ = parent.keys.orderedRemove(left_index);
            _ = parent.links.orderedRemove(left_index + 1);
        } else {
            const halves = try self.splitNode(left);
            parent.keys.items[left_index] = halves.separator;
            parent.links.items[left_index + 1] = halves.right;
        }
    }

    fn finish(self: *IndexEditor, pages: *PageAllocator) !u64 {
        if (self.root.page == 0 and self.root.node == null) return 0;
        return try self.flush(&self.root, pages, 0);
    }

    fn flush(self: *IndexEditor, link: *Link, pages: *PageAllocator, depth: usize) anyerror!u64 {
        if (depth > 64) return error.InvalidDocumentIndex;
        const node = link.node orelse return link.page;
        if (!node.dirty) return node.page;
        const alloc = self.arena.allocator();
        const pointers = try alloc.alloc(u64, node.links.items.len);
        for (node.links.items, 0..) |*child, i| pointers[i] = if (node.kind == .leaf) child.page else try self.flush(child, pages, depth + 1);
        const keys = try alloc.alloc([]u8, node.keys.items.len);
        const key_pages = try alloc.alloc(u64, node.keys.items.len);
        for (node.keys.items, 0..) |key, i| {
            keys[i] = @constCast(key.bytes);
            key_pages[i] = key.page;
        }
        const page = try self.file.writeDocumentIndexNode(pages, .{ .kind = node.kind, .keys = keys, .pointers = pointers, .key_pages = key_pages });
        node.page = page;
        node.dirty = false;
        link.page = page;
        return page;
    }
};

const CatalogRoot = enum {
    metadata,
    index,
};

pub const CatalogEntry = struct {
    previous_page: u64,
    key: []const u8,
    value: []const u8,
    is_delete: bool = false,
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
};

pub const DocumentEntry = struct {
    previous_page: u64,
    previous_namespace_page: u64 = 0,
    key: []const u8,
    value: []const u8 = "",
    is_delete: bool = false,
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
};

const ValuePage = struct {
    next_page: u64,
    chunk: []const u8,
};

const EncodedCatalogEntry = struct {
    previous_page: u64,
    key: []const u8,
    value: []const u8 = "",
    is_delete: bool = false,
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
};

const FreeMap = struct {
    covered_page_count: u64,
    free_pages: []u64,
};

const PageAllocator = struct {
    file: *NativeFile,
    free_pages: []u64,
    next_free_index: usize = 0,
    next_page_id: u64,
    batch: ?PageWriteBatch = null,
    pack_records: bool = false,
    record_buffer: [65536]u8 = undefined,
    record_used: usize = 0,
    record_page: u64 = 0,
    data_lock_file: ?std.Io.File = null,

    fn deinit(self: *PageAllocator) void {
        if (self.data_lock_file) |lock_file| {
            lock_file.close(self.file.runtimeIo());
        }
        self.file.allocator.free(self.free_pages);
    }

    fn writePage(self: *PageAllocator, page: u64, kind: PageKind, payload: []const u8) !void {
        if (self.batch == null) self.batch = .{ .file = self.file };
        try self.batch.?.appendPage(page, kind, payload);
    }

    fn flush(self: *PageAllocator) !void {
        try self.flushRecords();
        if (self.batch) |*batch| try batch.flush();
    }

    fn writeRecord(self: *PageAllocator, kind: PageKind, payload: []const u8) !u64 {
        std.debug.assert(kind == .catalog or kind == .document);
        const capacity = self.file.maxPagePayloadBytes();
        if (!self.pack_records or payload.len + 4 > capacity / 2) {
            const page = try self.allocate();
            try self.writePage(page, kind, payload);
            return page;
        }
        if (self.record_used + 4 + payload.len > capacity) try self.flushRecords();
        if (self.record_used == 0) {
            self.record_page = try self.allocate();
            if (self.record_page > packed_page_mask) return error.RecordTooLarge;
        }
        const offset = self.record_used;
        const out = self.record_buffer[offset..][0 .. 4 + payload.len];
        std.mem.writeInt(u16, out[0..2], @intCast(payload.len), .little);
        out[2] = @intFromEnum(kind);
        out[3] = 0;
        @memcpy(out[4..], payload);
        self.record_used += out.len;
        return packed_record_flag | (@as(u64, @intCast(offset)) << 47) | self.record_page;
    }

    fn flushRecords(self: *PageAllocator) !void {
        if (self.record_used == 0) return;
        try self.writePage(self.record_page, .record_bundle, self.record_buffer[0..self.record_used]);
        self.record_used = 0;
    }

    fn allocate(self: *PageAllocator) !u64 {
        if (self.next_free_index < self.free_pages.len) {
            const page_id = self.free_pages[self.next_free_index];
            self.next_free_index += 1;
            return page_id;
        }

        const page_id = self.next_page_id;
        self.next_page_id = try std.math.add(u64, self.next_page_id, 1);
        return page_id;
    }

    /// Free pages were validated against both durable checkpoint slots when
    /// this allocator was created. Pages not consumed by the current commit
    /// remain safe to advertise without re-walking every historical chain.
    fn remainingFreePages(self: *const PageAllocator) []const u64 {
        return self.free_pages[self.next_free_index..];
    }
};

pub const DocumentMutation = struct {
    key: []const u8,
    value: []const u8 = "",
    is_delete: bool = false,
    /// Internal transaction-local reference to a streamed, already written value.
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
};

const PendingDocumentIndexEntry = struct {
    key: []const u8,
    document_page_id: u64,
    ordinal: usize,

    fn lessThan(_: void, lhs: PendingDocumentIndexEntry, rhs: PendingDocumentIndexEntry) bool {
        return switch (std.mem.order(u8, lhs.key, rhs.key)) {
            .lt => true,
            .gt => false,
            .eq => lhs.ordinal < rhs.ordinal,
        };
    }
};

pub const CatalogMutation = struct {
    key: []const u8,
    value: []const u8 = "",
    is_delete: bool = false,
    /// Internal transaction-local reference to a streamed, already written value.
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
};

pub const OwnedDocument = struct {
    key: []u8,
    value: []u8,
};

pub const CheckpointSlot = struct {
    commit_sequence: u64 = 0,
    catalog_root_page: u64 = 0,
    document_root_page: u64 = 0,
    index_catalog_root_page: u64 = 0,
    free_map_root_page: u64 = 0,
    page_count: u64 = 1,
    namespace_directory_root_page: u64 = 0,
    document_index_root_page: u64 = 0,
};

pub const LockMode = enum {
    writer,
    reader,
};

pub const OpenOptions = struct {
    read_only: bool = false,
    no_sync: bool = false,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
};

pub const PathWriterLock = struct {
    io_impl: std.Io.Threaded,
    borrowed_io: ?std.Io = null,
    file: std.Io.File,

    pub fn close(self: *PathWriterLock) void {
        const io = self.borrowed_io orelse self.io_impl.io();
        self.file.close(io);
        if (self.borrowed_io == null) self.io_impl.deinit();
        self.* = undefined;
    }
};

const LockFile = struct {
    file: std.Io.File,
};

pub const CreateOptions = struct {
    exclusive: bool = false,
    no_sync: bool = false,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    writer_lock_marker: []const u8 = "",
};

pub const Header = struct {
    packed_records: bool = true,
    page_size: u32 = default_page_size,
    active_checkpoint: u8 = 0,
    checkpoints: [checkpoint_slot_count]CheckpointSlot = .{ .{}, .{} },
};

pub const InspectReport = struct {
    valid: bool,
    format_version: u32,
    page_size: u32,
    active_checkpoint: u8,
    commit_sequence: u64,
    page_count: u64,
    issue: ?[]const u8 = null,
};

pub const CheckReport = struct {
    valid: bool,
    file_size: u64,
    valid_prefix_size: u64,
    tail_bytes: u64,
    record_count: u64,
    live_file_count: u64,
    live_bytes: u64,
    compact_size: u64,
    reclaimable_bytes: u64,
    issue: ?[]const u8 = null,
};

pub const VacuumReport = struct {
    before_size: u64,
    after_size: u64,
    reclaimed_bytes: u64,
    live_file_count: u64,
    live_bytes: u64,
};

/// Result of publishing a replacement generation after the atomic rename has
/// crossed its commit point. `durability_unknown` means the live process has
/// adopted the replacement, but the parent-directory sync failed, so crash
/// durability cannot be promised and callers must not retry automatically.
pub const GenerationPublicationOutcome = enum {
    complete,
    durability_unknown,
};

pub const StableSnapshotReport = struct {
    source_size: u64,
    snapshot_size: u64,
    checkpoint_sequence: u64,
    page_count: u64,
    tail_bytes: u64,
};

pub const OwnedCatalogRecord = struct {
    key: []u8,
    value: []u8,
};

pub const OwnedCatalogKey = struct {
    key: []u8,
    page_id: u64 = 0,
};

const OwnedLiveRecordRef = struct {
    key: []u8,
    page_id: u64,
};

fn liveRecordRefLessThan(_: void, lhs: OwnedLiveRecordRef, rhs: OwnedLiveRecordRef) bool {
    return std.mem.order(u8, lhs.key, rhs.key) == .lt;
}

/// Decoded chain-navigation metadata for a page, cached so reachability
/// walks can traverse chains without re-reading and re-decoding page
/// payloads. Mirrors exactly the fields the walks consume.
const PageLinkInfo = struct {
    kind: PageKind,
    /// catalog/document: previous page in the chain; value: next page.
    link_page: u64 = 0,
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
    /// value pages only.
    chunk_len: usize = 0,
    is_delete: bool = false,
    value_len: usize = 0,
    /// catalog pages only; owned by the cache.
    key: []u8 = &.{},
};

/// A copy of one page's link info handed out by the cache. `key` (catalog
/// pages) is owned by the caller.
const PageLinkCopy = struct {
    kind: PageKind,
    link_page: u64,
    external_value_root_page: u64,
    external_value_len: usize,
    chunk_len: usize,
    key: ?[]u8,
};

/// In-memory cache of encoded pages, keyed by page id.
///
/// Safe when OS file locks are available because page contents are stable for
/// the lifetime of an open handle: all in-process page writes flow through
/// `writePage` (which updates the cache) or vacuum replacement (which
/// clears it), sidecar writer locks serialize writers, and read-only
/// data-file shared locks block the exclusive data-rewrite lock needed for
/// free-page reuse and vacuum. Filesystems that cannot provide those locks are
/// rejected before a `NativeFile` is returned, so cached pages are never used
/// by an unfenced handle.
const PageCache = struct {
    const default_limit_bytes: usize = 64 * 1024 * 1024;
    const default_link_limit_bytes: usize = 16 * 1024 * 1024;
    const link_entry_overhead: usize = @sizeOf(PageLinkInfo) + @sizeOf(u64);

    const CachedPage = struct { bytes: []u8, credit: u8, metadata: bool };
    mutex: std.atomic.Mutex = .unlocked,
    pages: std.AutoArrayHashMapUnmanaged(u64, CachedPage) = .empty,
    clock_hand: usize = 0,
    links: std.AutoHashMapUnmanaged(u64, PageLinkInfo) = .empty,
    total_bytes: usize = 0,
    link_bytes: usize = 0,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    resource_page_accounted_bytes: u64 = 0,
    resource_link_accounted_bytes: u64 = 0,
    /// CLOCK admission starts payloads cold; accesses promote them, while
    /// navigation metadata starts with a second chance. Overflow evicts only
    /// enough bytes for the incoming page, preserving the rest of the cache.
    limit_bytes: usize = default_limit_bytes,
    link_limit_bytes: usize = default_link_limit_bytes,

    fn getCopy(self: *PageCache, allocator: Allocator, page_id: u64) !?[]u8 {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const cached = self.pages.getPtr(page_id) orelse return null;
        cached.credit = if (cached.metadata) 3 else 2;
        return try allocator.dupe(u8, cached.bytes);
    }

    fn copyInto(self: *PageCache, page_id: u64, out: []u8) bool {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const cached = self.pages.getPtr(page_id) orelse return false;
        if (cached.bytes.len != out.len) return false;
        cached.credit = if (cached.metadata) 3 else 2;
        @memcpy(out, cached.bytes);
        return true;
    }

    fn attachResourceManager(self: *PageCache, manager: *resource_manager_mod.ResourceManager) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.resource_manager = manager;
        self.refreshPageResourceUsageLocked();
        self.refreshLinkResourceUsageLocked();
    }

    fn put(self: *PageCache, allocator: Allocator, page_id: u64, page: []const u8) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        // Replacement must invalidate the old bytes even if admission fails.
        if (self.pages.fetchSwapRemove(page_id)) |old| {
            self.total_bytes -= old.value.bytes.len;
            allocator.free(old.value.bytes);
        }
        defer self.refreshPageResourceUsageLocked();
        if (self.clearPagesForHardPressureLocked(allocator)) return;
        if (page.len > self.limit_bytes) return;
        self.evictPagesToLocked(allocator, self.limit_bytes - page.len);
        const owned = allocator.dupe(u8, page) catch return;
        const metadata = page.len >= page_header_size and switch (page[4]) {
            @intFromEnum(PageKind.document_index), @intFromEnum(PageKind.catalog_index), @intFromEnum(PageKind.value_extent) => true,
            else => false,
        };
        self.pages.put(allocator, page_id, .{ .bytes = owned, .credit = if (metadata) 2 else 0, .metadata = metadata }) catch {
            allocator.free(owned);
            return;
        };
        self.total_bytes += page.len;
        self.refreshPageResourceUsageLocked();
        _ = self.clearPagesForHardPressureLocked(allocator);
    }

    fn evictPagesToLocked(self: *PageCache, allocator: Allocator, target: usize) void {
        while (self.total_bytes > target and self.pages.count() != 0) {
            if (self.clock_hand >= self.pages.count()) self.clock_hand = 0;
            const entry = &self.pages.values()[self.clock_hand];
            if (entry.credit != 0) {
                entry.credit -= 1;
                self.clock_hand += 1;
            } else {
                self.total_bytes -= entry.bytes.len;
                allocator.free(entry.bytes);
                self.pages.swapRemoveAt(self.clock_hand);
            }
        }
    }

    fn getLinksCopy(self: *PageCache, allocator: Allocator, page_id: u64) !?PageLinkCopy {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const cached = self.links.get(page_id) orelse return null;
        return .{
            .kind = cached.kind,
            .link_page = cached.link_page,
            .external_value_root_page = cached.external_value_root_page,
            .external_value_len = cached.external_value_len,
            .chunk_len = cached.chunk_len,
            .key = if (cached.key.len > 0) try allocator.dupe(u8, cached.key) else null,
        };
    }

    fn putLinks(self: *PageCache, allocator: Allocator, page_id: u64, info: PageLinkInfo) void {
        const owned_key = if (info.key.len > 0) allocator.dupe(u8, info.key) catch return else @as([]u8, &.{});
        var owned = info;
        owned.key = owned_key;

        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.clearLinksForHardPressureLocked(allocator)) {
            if (owned.key.len > 0) allocator.free(owned.key);
            return;
        }
        if (self.links.getEntry(page_id)) |entry| {
            self.link_bytes -= entry.value_ptr.key.len + link_entry_overhead;
            if (entry.value_ptr.key.len > 0) allocator.free(entry.value_ptr.key);
            entry.value_ptr.* = owned;
            self.link_bytes += owned.key.len + link_entry_overhead;
            self.refreshLinkResourceUsageLocked();
            _ = self.clearLinksForHardPressureLocked(allocator);
            return;
        }
        if (self.link_bytes + owned.key.len + link_entry_overhead > self.link_limit_bytes) {
            // At capacity, prefer keeping the resident entries: chain walks
            // revisit the same old pages every commit, so evicting them to
            // admit one new entry would thrash the whole walk.
            if (owned.key.len > 0) allocator.free(owned.key);
            return;
        }
        self.links.put(allocator, page_id, owned) catch {
            if (owned.key.len > 0) allocator.free(owned.key);
            return;
        };
        self.link_bytes += owned.key.len + link_entry_overhead;
        self.refreshLinkResourceUsageLocked();
        _ = self.clearLinksForHardPressureLocked(allocator);
    }

    fn remove(self: *PageCache, allocator: Allocator, page_id: u64) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.removeLocked(allocator, page_id);
    }

    fn removeLinks(self: *PageCache, allocator: Allocator, page_id: u64) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.links.fetchRemove(page_id)) |entry| {
            self.link_bytes -= entry.value.key.len + link_entry_overhead;
            if (entry.value.key.len > 0) allocator.free(entry.value.key);
            self.refreshLinkResourceUsageLocked();
        }
    }

    fn removeLocked(self: *PageCache, allocator: Allocator, page_id: u64) void {
        var removed_page = false;
        var removed_links = false;
        if (self.pages.fetchSwapRemove(page_id)) |entry| {
            self.total_bytes -= entry.value.bytes.len;
            allocator.free(entry.value.bytes);
            removed_page = true;
        }
        if (self.links.fetchRemove(page_id)) |entry| {
            self.link_bytes -= entry.value.key.len + link_entry_overhead;
            if (entry.value.key.len > 0) allocator.free(entry.value.key);
            removed_links = true;
        }
        if (removed_page) self.refreshPageResourceUsageLocked();
        if (removed_links) self.refreshLinkResourceUsageLocked();
    }

    /// Rollback makes the append tail reusable. Streamed private-image writes
    /// can bypass this cache, so discard both bytes and links for those IDs.
    /// Preserve cached pages from the restored checkpoint.
    fn discardFrom(self: *PageCache, allocator: Allocator, first_page: u64) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var i: usize = 0;
        while (i < self.pages.count()) {
            const page = self.pages.keys()[i];
            if (page >= first_page) self.removeLocked(allocator, page) else i += 1;
        }
        // Hash-map removal leaves other entries and the iterator stable.
        var links = self.links.keyIterator();
        while (links.next()) |page| {
            if (page.* >= first_page) self.removeLocked(allocator, page.*);
        }
    }

    fn clear(self: *PageCache, allocator: Allocator) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.clearLocked(allocator);
    }

    fn clearLocked(self: *PageCache, allocator: Allocator) void {
        self.clearPagesLocked(allocator);
        self.clearLinksLocked(allocator);
        self.refreshPageResourceUsageLocked();
        self.refreshLinkResourceUsageLocked();
    }

    fn clearPagesLocked(self: *PageCache, allocator: Allocator) void {
        for (self.pages.values()) |page| allocator.free(page.bytes);
        self.clock_hand = 0;
        self.pages.clearRetainingCapacity();
        self.total_bytes = 0;
    }

    fn clearLinksLocked(self: *PageCache, allocator: Allocator) void {
        var link_it = self.links.valueIterator();
        while (link_it.next()) |info| {
            if (info.key.len > 0) allocator.free(info.key);
        }
        self.links.clearRetainingCapacity();
        self.link_bytes = 0;
    }

    fn deinit(self: *PageCache, allocator: Allocator) void {
        self.clearLocked(allocator);
        self.releaseResourceUsageLocked();
        self.pages.deinit(allocator);
        self.links.deinit(allocator);
    }

    fn refreshPageResourceUsageLocked(self: *PageCache) void {
        const manager = self.resource_manager orelse return;
        manager.observeUsage(.lite_native_page_cache, &self.resource_page_accounted_bytes, @intCast(self.total_bytes));
    }

    fn refreshLinkResourceUsageLocked(self: *PageCache) void {
        const manager = self.resource_manager orelse return;
        manager.observeUsage(.lite_native_link_cache, &self.resource_link_accounted_bytes, @intCast(self.link_bytes));
    }

    fn releaseResourceUsageLocked(self: *PageCache) void {
        const manager = self.resource_manager orelse return;
        manager.observeUsage(.lite_native_page_cache, &self.resource_page_accounted_bytes, 0);
        manager.observeUsage(.lite_native_link_cache, &self.resource_link_accounted_bytes, 0);
        self.resource_manager = null;
    }

    fn clearPagesForHardPressureLocked(self: *PageCache, allocator: Allocator) bool {
        const manager = self.resource_manager orelse return false;
        const stats = manager.sliceStats(.lite_native_page_cache);
        if (stats.pressure != .hard or stats.hard_action != .shrink_cache) return false;
        // Budgets are shared across generations and handles. Reclaim this
        // cache's share of the aggregate excess, even when it is individually
        // smaller than the slice's soft limit.
        const reclaim: usize = @intCast(@min(stats.used_bytes -| stats.soft_limit_bytes, self.total_bytes));
        self.evictPagesToLocked(allocator, @min(self.total_bytes - reclaim, self.limit_bytes));
        self.refreshPageResourceUsageLocked();
        return true;
    }

    fn clearLinksForHardPressureLocked(self: *PageCache, allocator: Allocator) bool {
        const manager = self.resource_manager orelse return false;
        const stats = manager.sliceStats(.lite_native_link_cache);
        if (stats.pressure != .hard or stats.hard_action != .shrink_cache) return false;
        self.clearLinksLocked(allocator);
        self.refreshLinkResourceUsageLocked();
        return true;
    }
};

/// An operation-owned encoded-page buffer. Abort drops pending bytes; callers
/// must flush before finalization. Consecutive page IDs coalesce, while gaps
/// and full buffers flush independently. No metadata needed to read a pending page is
/// published before its owner's final flush.
const PageWriteBatch = struct {
    const capacity = 64 * 1024;
    file: *NativeFile,
    options: WriteOptions = .{},
    buffer: [capacity]u8 = undefined,
    used: usize = 0,
    first_page: u64 = 0,
    failure: ?anyerror = null,

    fn reserve(self: *PageWriteBatch, page_id: u64) ![]u8 {
        if (self.failure) |err| return err;
        const size: usize = self.file.header.page_size;
        if (self.used != 0 and (self.used + size > self.buffer.len or page_id != self.first_page + self.used / size)) try self.flush();
        if (self.used == 0) self.first_page = page_id;
        return self.buffer[self.used..][0..size];
    }

    fn appendPage(self: *PageWriteBatch, page_id: u64, kind: PageKind, payload: []const u8) !void {
        if (payload.len > self.file.maxPagePayloadBytes()) return error.PageTooLarge;
        const page = try self.reserve(page_id);
        encodePage(page, kind, payload);
        self.used += page.len;
    }

    fn appendValue(self: *PageWriteBatch, page_id: u64, next: u64, chunk: []const u8) !void {
        if (chunk.len == 0 or chunk.len > self.file.maxValuePagePayloadBytes()) return error.InvalidNativeValueChain;
        const page = try self.reserve(page_id);
        var prefix: [value_page_header_size]u8 = undefined;
        std.mem.writeInt(u64, &prefix, next, .little);
        encodePageParts(page, .value, &prefix, chunk);
        self.used += page.len;
    }

    fn flush(self: *PageWriteBatch) !void {
        if (self.failure) |err| return err;
        if (self.used == 0) return;
        errdefer |err| self.failure = err;
        const file = self.file;
        const size: usize = file.header.page_size;
        if (builtin.is_test) {
            if (file.test_page_write_fail_after) |remaining| {
                if (remaining == 0) return error.TestPageWriteFailure;
                file.test_page_write_fail_after = remaining - 1;
            }
            _ = file.test_page_write_calls.fetchAdd(1, .monotonic);
            _ = file.test_page_writes.fetchAdd(@intCast(self.used / size), .monotonic);
        }
        try file.file.writePositionalAll(file.runtimeIo(), self.buffer[0..self.used], self.first_page * @as(u64, size));
        var offset: usize = 0;
        while (offset < self.used) : (offset += size) {
            file.cacheWrittenPage(self.first_page + offset / size, self.buffer[offset..][0..size], self.options);
        }
        self.used = 0;
    }
};

pub const ChangeCapture = struct {
    pub const Root = enum { metadata, index, documents };
    const max_key_bytes = 4 * 1024 * 1024;
    const max_keys = 65536;
    keys: [3]std.StringHashMapUnmanaged(void) = .{ .empty, .empty, .empty },
    key_bytes: usize = 0,
    count: usize = 0,
    overflow: bool = false,

    pub fn deinit(self: *ChangeCapture, allocator: Allocator) void {
        for (&self.keys) |*map| {
            var it = map.keyIterator();
            while (it.next()) |key| allocator.free(key.*);
            map.deinit(allocator);
        }
        self.* = .{};
    }

    fn record(self: *ChangeCapture, allocator: Allocator, root: Root, key: []const u8) void {
        if (self.overflow) return;
        const map = &self.keys[@intFromEnum(root)];
        if (map.contains(key)) return;
        if (key.len > max_key_bytes - self.key_bytes or self.count == max_keys) {
            self.overflow = true;
            return;
        }
        const owned = allocator.dupe(u8, key) catch {
            self.overflow = true;
            return;
        };
        map.put(allocator, owned, {}) catch {
            allocator.free(owned);
            self.overflow = true;
            return;
        };
        self.count += 1;
        self.key_bytes += key.len;
    }
};

pub const VacuumImage = struct {
    prepared: NativeFile,
    report: VacuumReport,

    pub fn deinit(self: *VacuumImage) void {
        deleteFilePath(self.prepared.runtimeIo(), self.prepared.path) catch {};
        self.prepared.close();
        self.* = undefined;
    }
};

pub const NativeFile = struct {
    allocator: Allocator,
    /// The default constructors own this Threaded runtime. Borrowed-I/O
    /// constructors leave it undefined and retain `borrowed_io` instead. This
    /// keeps the production convenience API while making the complete Lite
    /// file lifecycle executable by deterministic std.Io implementations.
    io_impl: std.Io.Threaded,
    borrowed_io: ?std.Io = null,
    path: []u8,
    file: std.Io.File,
    writer_lock_file: ?std.Io.File = null,
    header: Header,
    transaction_header: ?Header = null,
    durable_header: ?Header = null,
    change_capture: ?*ChangeCapture = null,
    read_only: bool = false,
    no_sync: bool = false,
    // An error after slot publication may leave disk ahead of header. Do not
    // elide a subsequent write based on the old in-memory checkpoint then.
    checkpoint_publication_uncertain: bool = false,
    page_cache_enabled: std.atomic.Value(bool) = .init(true),
    /// Maintenance readers and private images retain navigation pages only.
    page_cache_policy: enum { normal, metadata_only } = .normal,
    page_cache: PageCache = .{},
    namespace_directory_cache: NamespaceDirectory = .empty,
    namespace_directory_cache_root: u64 = std.math.maxInt(u64),
    namespace_directory_delta_depth: u16 = 0,
    /// While non-zero, page reads bypass the page cache and hit disk.
    /// Integrity checks hold this so they verify on-disk state rather than
    /// cached copies.
    page_cache_bypass: std.atomic.Value(u32) = .init(0),
    test_fail_vacuum_after_adoption: bool = false,
    test_fail_generation_directory_sync: bool = false,
    /// Set once the on-disk free map has been cross-checked against every
    /// valid checkpoint slot (including the crash-safety fallback slot) for
    /// this open file handle. That full-database reachability scan is only
    /// needed to catch corruption present when the file was opened (or
    /// written to out of band); this process's own commits can only ever
    /// consume pages that a prior, already-verified free map declared free,
    /// so re-running the scan on every single mutation is unnecessary and,
    /// for large stores, quadratic in the number of commits. `check()` (via
    /// `validateReachableFreeMap`) always re-verifies regardless of this
    /// flag, for explicit integrity audits.
    free_pages_verified: bool = false,
    // Structural scaling assertions count page operations, independent of
    // filesystem speed and cache warmth. No counters exist in production.
    test_page_reads: if (builtin.is_test) std.atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    test_page_writes: if (builtin.is_test) std.atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},

    test_page_write_calls: if (builtin.is_test) std.atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    test_page_write_fail_after: if (builtin.is_test) ?usize else void = if (builtin.is_test) null else {},

    pub fn open(allocator: Allocator, path: []const u8, read_only: bool) !NativeFile {
        return try openWithOptions(allocator, path, .{ .read_only = read_only });
    }

    pub fn openWithOptions(allocator: Allocator, path: []const u8, opts: OpenOptions) !NativeFile {
        var io_impl = threaded_io_limits.initService(allocator);
        errdefer io_impl.deinit();
        return try openWithRuntime(allocator, path, opts, io_impl, null);
    }

    /// Opens a Lite file using a caller-owned std.Io runtime. The runtime must
    /// outlive the returned file. No native thread or host-I/O escape is
    /// created by this path.
    pub fn openWithIo(allocator: Allocator, io: std.Io, path: []const u8, opts: OpenOptions) !NativeFile {
        return try openWithRuntime(allocator, path, opts, undefined, io);
    }

    fn openWithRuntime(
        allocator: Allocator,
        path: []const u8,
        opts: OpenOptions,
        io_impl: std.Io.Threaded,
        borrowed_io: ?std.Io,
    ) !NativeFile {
        var owned_io_impl = io_impl;
        const io = borrowed_io orelse owned_io_impl.io();

        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);

        var writer_lock_file: ?std.Io.File = null;
        if (!opts.read_only) {
            const writer_lock = try acquireWriterLock(allocator, io, path);
            writer_lock_file = writer_lock.file;
        }
        errdefer if (writer_lock_file) |lock_file| lock_file.close(io);

        const opened_file = try openDataFile(io, path, if (opts.read_only) .reader else .writer);
        const file = opened_file.file;
        errdefer file.close(io);

        var header_bytes: [header_size]u8 = undefined;
        try readHeaderExactAt(file, io, &header_bytes);
        var header = try decodeHeader(&header_bytes);
        const file_size = (try file.stat(io)).size;
        header.active_checkpoint = try selectCompleteCheckpointForFile(header, file_size);

        var result = NativeFile{
            .allocator = allocator,
            .io_impl = owned_io_impl,
            .borrowed_io = borrowed_io,
            .path = owned_path,
            .file = file,
            .writer_lock_file = writer_lock_file,
            .header = header,
            .read_only = opts.read_only,
            .no_sync = opts.no_sync,
            .page_cache_enabled = .init(true),
        };
        if (opts.resource_manager) |manager| result.page_cache.attachResourceManager(manager);
        return result;
    }

    pub fn create(allocator: Allocator, path: []const u8) !NativeFile {
        return try createWithMode(allocator, path, false, false, null, "");
    }

    pub fn createNew(allocator: Allocator, path: []const u8) !NativeFile {
        return try createWithMode(allocator, path, true, false, null, "");
    }

    pub fn createWithOptions(allocator: Allocator, path: []const u8, opts: CreateOptions) !NativeFile {
        return try createWithMode(allocator, path, opts.exclusive, opts.no_sync, opts.resource_manager, opts.writer_lock_marker);
    }

    /// Creates a Lite file using a caller-owned std.Io runtime. The runtime
    /// must outlive the returned file.
    pub fn createWithIo(allocator: Allocator, io: std.Io, path: []const u8, opts: CreateOptions) !NativeFile {
        return try createWithRuntime(allocator, path, opts.exclusive, opts.no_sync, opts.resource_manager, opts.writer_lock_marker, undefined, io);
    }

    fn createWithMode(
        allocator: Allocator,
        path: []const u8,
        exclusive: bool,
        no_sync: bool,
        resource_manager: ?*resource_manager_mod.ResourceManager,
        writer_lock_marker: []const u8,
    ) !NativeFile {
        var io_impl = threaded_io_limits.initService(allocator);
        errdefer io_impl.deinit();
        return try createWithRuntime(allocator, path, exclusive, no_sync, resource_manager, writer_lock_marker, io_impl, null);
    }

    fn createWithRuntime(
        allocator: Allocator,
        path: []const u8,
        exclusive: bool,
        no_sync: bool,
        resource_manager: ?*resource_manager_mod.ResourceManager,
        writer_lock_marker: []const u8,
        io_impl: std.Io.Threaded,
        borrowed_io: ?std.Io,
    ) !NativeFile {
        var owned_io_impl = io_impl;
        const io = borrowed_io orelse owned_io_impl.io();

        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);

        const writer_lock = try acquireWriterLock(allocator, io, path);
        var writer_lock_file = writer_lock.file;
        errdefer writer_lock_file.close(io);
        if (writer_lock_marker.len > 0) {
            try writer_lock_file.writePositionalAll(io, writer_lock_marker, 0);
            try writer_lock_file.setLength(io, writer_lock_marker.len);
            if (!no_sync) try writer_lock_file.sync(io);
        }

        var encoded: [header_size]u8 = undefined;
        encodeHeader(&encoded, .{});

        const replace_existing = !exclusive and pathExists(io, path);
        const replacement_path = if (replace_existing)
            try realPathAlloc(allocator, io, path)
        else
            null;
        defer if (replacement_path) |canonical| allocator.free(canonical);
        const create_target = if (replacement_path) |canonical| canonical else path;
        const staging_path = if (replace_existing)
            try std.fmt.allocPrint(allocator, "{s}.tmp-aflite-create", .{create_target})
        else
            null;
        defer if (staging_path) |tmp_path| allocator.free(tmp_path);
        errdefer if (staging_path) |tmp_path| deleteFilePath(io, tmp_path) catch {};

        // Reinitializing an existing artifact is an atomic generation swap.
        // Truncating the existing inode would corrupt snapshots held by
        // concurrent read-only processes, whose shared lock intentionally
        // permits an append-only writer.
        const create_path = staging_path orelse create_target;
        var file = try createDataFile(io, create_path, .{
            .truncate = true,
            .exclusive = exclusive,
        });
        var file_open = true;
        errdefer if (file_open) file.close(io);

        try file.writePositionalAll(io, &encoded, 0);
        if (!no_sync) {
            try file.sync(io);
        }
        if (staging_path) |tmp_path| {
            file.close(io);
            file_open = false;
            renameFilePath(io, tmp_path, create_target) catch |err| {
                deleteFilePath(io, tmp_path) catch {};
                return err;
            };
            if (!no_sync) try fs_paths.syncDirPortable(io, std.fs.path.dirname(create_target) orelse ".");
            file = (try openDataFile(io, path, .writer)).file;
            file_open = true;
        } else if (!no_sync) {
            try fs_paths.syncDirPortable(io, std.fs.path.dirname(create_target) orelse ".");
        }

        var result = NativeFile{
            .allocator = allocator,
            .io_impl = owned_io_impl,
            .borrowed_io = borrowed_io,
            .path = owned_path,
            .file = file,
            .writer_lock_file = writer_lock_file,
            .header = .{},
            .read_only = false,
            .no_sync = no_sync,
            .page_cache_enabled = .init(true),
        };
        if (resource_manager) |manager| result.page_cache.attachResourceManager(manager);
        return result;
    }

    pub fn close(self: *NativeFile) void {
        const io = self.runtimeIo();
        deinitNamespaceDirectory(self.allocator, &self.namespace_directory_cache);
        self.page_cache.deinit(self.allocator);
        if (self.writer_lock_file) |lock_file| {
            lock_file.close(io);
        }
        self.file.close(io);
        self.allocator.free(self.path);
        if (self.borrowed_io == null) self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn usesBorrowedIo(self: *const NativeFile) bool {
        return self.borrowed_io != null;
    }

    /// Returns the runtime used for file operations and synchronization. The
    /// returned interface is borrowed from this file and is invalid after
    /// `close`.
    pub fn runtime(self: *NativeFile) std.Io {
        return self.borrowed_io orelse self.io_impl.io();
    }

    fn runtimeIo(self: *NativeFile) std.Io {
        return self.runtime();
    }

    pub fn activeCheckpoint(self: *const NativeFile) CheckpointSlot {
        return self.header.checkpoints[self.header.active_checkpoint];
    }

    pub fn check(self: *NativeFile) !CheckReport {
        return try self.checkWithCancel(null);
    }

    pub fn checkWithCancel(self: *NativeFile, cancel: ?*const maintenance.CancelToken) !CheckReport {
        if (cancel) |token| try token.check();
        return self.checkAtFileSizeWithCancel((try self.file.stat(self.runtimeIo())).size, cancel);
    }

    /// Check a pinned header against the file length captured with that header.
    /// Appends after pinning do not turn a valid snapshot into a tail error.
    pub fn checkAtFileSizeWithCancel(self: *NativeFile, file_size: u64, cancel: ?*const maintenance.CancelToken) !CheckReport {
        if (cancel) |token| try token.check();
        // Integrity checking must observe on-disk state, not cached pages.
        _ = self.page_cache_bypass.fetchAdd(1, .monotonic);
        defer _ = self.page_cache_bypass.fetchSub(1, .monotonic);

        const checkpoint = self.activeCheckpoint();
        const expected_size = try checkpointPrefixSize(checkpoint, self.header.page_size);

        const report = CheckReport{
            .valid = true,
            .file_size = file_size,
            .valid_prefix_size = @min(file_size, expected_size),
            .tail_bytes = if (file_size > expected_size) file_size - expected_size else 0,
            .record_count = if (checkpoint.page_count > 0) checkpoint.page_count - 1 else 0,
            .live_file_count = 0,
            .live_bytes = 0,
            .compact_size = expected_size,
            .reclaimable_bytes = 0,
        };

        if (checkpoint.page_count == 0) return invalidCheck(report, "invalid_page_count");
        if (file_size < expected_size) return invalidCheck(report, "truncated_file");

        var reachable_pages = std.AutoHashMapUnmanaged(u64, void){};
        defer reachable_pages.deinit(self.allocator);

        const catalog_records = self.countReachableChainPagesWithCancel(.catalog, checkpoint.catalog_root_page, &reachable_pages, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        if (cancel) |token| try token.check();
        const namespace_directory_records = self.countReachableChainPagesWithCancel(.catalog, checkpoint.namespace_directory_root_page, &reachable_pages, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        if ((checkpoint.document_root_page == 0 and namespace_directory_records != 0) or
            (checkpoint.document_root_page != 0 and namespace_directory_records == 0))
            return invalidCheck(report, "invalid_namespace_directory");
        self.validateNamespaceDirectory(checkpoint) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        if (cancel) |token| try token.check();
        const index_catalog_records = self.countReachableChainPagesWithCancel(.catalog, checkpoint.index_catalog_root_page, &reachable_pages, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        const document_records = self.countReachableChainPagesWithCancel(.document, checkpoint.document_root_page, &reachable_pages, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        if ((checkpoint.document_root_page == 0) != (checkpoint.document_index_root_page == 0))
            return invalidCheck(report, "invalid_document_index");
        const document_index_pages = self.collectDocumentIndexPages(checkpoint, &reachable_pages, true, true, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        self.validateDocumentIndexCoverage(checkpoint, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        if (cancel) |token| try token.check();
        self.validateReachableFreeMap(checkpoint, &reachable_pages) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        const live = self.liveStats(cancel) catch |err| {
            if (err == error.MaintenanceCanceled) return err;
            return invalidCheck(report, issueForPageCheckError(err));
        };

        var valid = report;
        _ = document_index_pages;
        valid.record_count = catalog_records + index_catalog_records + document_records;
        valid.live_file_count = live.record_count;
        valid.live_bytes = live.bytes;
        valid.compact_size = live.compact_size;
        valid.reclaimable_bytes = if (file_size > live.compact_size) file_size - live.compact_size else 0;
        if (valid.tail_bytes != 0) return invalidCheck(valid, "tail_bytes");
        return valid;
    }

    pub fn allocatePage(self: *NativeFile, contents: []const u8) !u64 {
        if (self.read_only) return error.ReadOnly;
        const previous = self.activeCheckpoint();
        var page_allocator = try self.pageAllocatorFromFreeMap(previous);
        defer page_allocator.deinit();

        const page_id = try page_allocator.allocate();
        try page_allocator.writePage(page_id, .data, contents);

        var next = previous;
        next.commit_sequence += 1;
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        try page_allocator.flush();
        try self.writeFreeMapPage(next.free_map_root_page, next.page_count, page_allocator.remainingFreePages());
        try self.syncIfRequired();

        try self.publishCheckpoint(next);
        return page_id;
    }

    pub fn readPageAlloc(self: *NativeFile, allocator: Allocator, page_id: u64) ![]u8 {
        return try self.readPageAllocForCheckpoint(allocator, page_id, self.activeCheckpoint());
    }

    fn readPageAllocForCheckpoint(self: *NativeFile, allocator: Allocator, page_id: u64, checkpoint: CheckpointSlot) ![]u8 {
        const page = try self.readPhysicalPageAlloc(allocator, physicalPage(page_id), checkpoint);
        errdefer allocator.free(page);
        if (page_id & packed_record_flag != 0) try unpackRecordPage(page, page_id);
        return page;
    }

    fn admitPageToCache(self: *const NativeFile, page: []const u8) bool {
        if (page.len <= 4 or page[4] == @intFromEnum(PageKind.free_map)) return false;
        if (self.page_cache_policy == .normal) return true;
        return switch (page[4]) {
            @intFromEnum(PageKind.document_index), @intFromEnum(PageKind.catalog_index), @intFromEnum(PageKind.value_extent) => true,
            else => false,
        };
    }

    fn readPhysicalPageAlloc(self: *NativeFile, allocator: Allocator, page_id: u64, checkpoint: CheckpointSlot) ![]u8 {
        if (builtin.is_test) _ = self.test_page_reads.fetchAdd(1, .monotonic);
        if (page_id == 0 or page_id >= checkpoint.page_count) return error.InvalidPageId;

        const use_cache = self.page_cache_enabled.load(.monotonic) and self.page_cache_bypass.load(.monotonic) == 0;
        if (use_cache) {
            if (try self.page_cache.getCopy(allocator, page_id)) |cached| return cached;
        }

        const page_size: usize = @intCast(self.header.page_size);
        const page = try allocator.alloc(u8, page_size);
        errdefer allocator.free(page);

        try readExactAt(self.file, self.runtimeIo(), page, page_id * @as(u64, self.header.page_size));
        // Free-map pages are rewritten every commit and read once, so caching
        // them buys nothing; skipping them also keeps free-map validation
        // reading disk truth before any pages are handed out for reuse.
        if (use_cache and self.admitPageToCache(page)) {
            self.page_cache.put(self.allocator, page_id, page);
        }
        return page;
    }

    /// Bounded scratch reads for point lookups avoid allocating decoded keys
    /// and page copies at every level of the catalog/document B-tree.
    fn readPageInto(self: *NativeFile, page_id: u64, checkpoint: CheckpointSlot, scratch: *[65536]u8) ![]const u8 {
        if (page_id & packed_record_flag != 0) {
            _ = try self.readPageInto(physicalPage(page_id), checkpoint, scratch);
            try unpackRecordPage(scratch[0..self.header.page_size], page_id);
            return scratch[0..self.header.page_size];
        }
        if (builtin.is_test) _ = self.test_page_reads.fetchAdd(1, .monotonic);
        if (page_id == 0 or page_id >= checkpoint.page_count) return error.InvalidPageId;
        const page = scratch[0..self.header.page_size];
        const use_cache = self.page_cache_enabled.load(.monotonic) and self.page_cache_bypass.load(.monotonic) == 0;
        if (use_cache and self.page_cache.copyInto(page_id, page)) return page;
        try readExactAt(self.file, self.runtimeIo(), page, page_id * @as(u64, self.header.page_size));
        if (use_cache and self.admitPageToCache(page)) self.page_cache.put(self.allocator, page_id, page);
        return page;
    }

    pub fn readPagePayloadAlloc(self: *NativeFile, allocator: Allocator, page_id: u64) ![]u8 {
        const page = try self.readPageAlloc(allocator, page_id);
        defer allocator.free(page);
        return try decodePagePayloadAlloc(allocator, page, .data);
    }

    pub fn putCatalogRecord(self: *NativeFile, key: []const u8, value: []const u8) !void {
        try self.putCatalogBatch(&.{.{ .key = key, .value = value }});
    }

    pub fn deleteCatalogRecord(self: *NativeFile, key: []const u8) !void {
        try self.putCatalogBatch(&.{.{ .key = key, .is_delete = true }});
    }

    pub fn putCatalogBatch(self: *NativeFile, mutations: []const CatalogMutation) !void {
        return try self.putCatalogBatchForRoot(.metadata, mutations, .{});
    }

    pub fn putIndexCatalogRecord(self: *NativeFile, key: []const u8, value: []const u8) !void {
        try self.putIndexCatalogRecordWithOptions(key, value, .{});
    }

    pub fn putIndexCatalogRecordWithOptions(self: *NativeFile, key: []const u8, value: []const u8, options: WriteOptions) !void {
        try self.putCatalogBatchForRoot(.index, &.{.{ .key = key, .value = value }}, options);
    }

    /// Import a private, seekable staging file using bounded buffers. The
    /// caller serializes publication with other native mutations and keeps the
    /// source alive and unchanged throughout this call. No staged data enters
    /// the committed catalog until the final checkpoint publication.
    pub fn putIndexCatalogRecordFromFile(self: *NativeFile, key: []const u8, source: std.Io.File, len: usize, options: WriteOptions) !void {
        if (self.change_capture) |capture| capture.record(self.allocator, .index, key);
        if (self.read_only) return error.ReadOnly;
        if (key.len > catalog_key_len_mask or len > std.math.maxInt(u32)) return error.RecordTooLarge;
        const fixed_len = 16 + key.len;
        if (fixed_len > self.maxPagePayloadBytes()) return error.PageTooLarge;
        var buffer: [65536]u8 = undefined;
        if (len <= self.maxPagePayloadBytes() - fixed_len) {
            try readExactAt(source, self.runtimeIo(), buffer[0..len], 0);
            return try self.putIndexCatalogRecordWithOptions(key, buffer[0..len], options);
        }
        if (fixed_len + 8 > self.maxPagePayloadBytes()) return error.PageTooLarge;
        const previous = self.activeCheckpoint();
        const roots = try self.readCatalogRoots(previous.index_catalog_root_page, previous);
        var pages = try self.pageAllocatorFromFreeMap(previous);
        defer pages.deinit();
        var builder = ExtentAppender{ .file = self, .pages = &pages, .tail = undefined, .batch = .{ .file = self, .options = options } };
        defer builder.deinit();
        const chunk_size = self.maxValuePagePayloadBytes();
        const read_size = buffer.len / chunk_size * chunk_size;
        var offset: usize = 0;
        while (offset < len) {
            const n = @min(read_size, len - offset);
            try readExactAt(source, self.runtimeIo(), buffer[0..n], offset);
            var pos: usize = 0;
            while (pos < n) {
                const end = @min(n, pos + chunk_size);
                try builder.pushValue(buffer[pos..end]);
                pos = end;
            }
            offset += n;
        }
        const value = try builder.finish();
        var payload = std.ArrayListUnmanaged(u8).empty;
        defer payload.deinit(self.allocator);
        try encodeCatalogEntryRaw(self.allocator, &payload, .{ .previous_page = roots.history, .key = key, .external_value_root_page = value.page, .external_value_len = len });
        const record = try pages.allocate();
        try pages.writePage(record, .catalog, payload.items);
        var editor = IndexEditor.init(self, previous, roots.index);
        defer editor.deinit();
        try editor.put(key, record);
        const index = try editor.finish(&pages);
        var next = previous;
        next.commit_sequence += 1;
        next.index_catalog_root_page = try self.writeCatalogRoot(&pages, record, index);
        next.free_map_root_page = try pages.allocate();
        next.page_count = pages.next_page_id;
        try pages.flush();
        try self.writeFreeMapPage(next.free_map_root_page, next.page_count, pages.remainingFreePages());
        try self.syncIfRequired();
        try self.publishCheckpoint(next);
    }

    pub fn appendIndexCatalogRecord(self: *NativeFile, key: []const u8, suffix: []const u8) !void {
        try self.appendCatalogRecordForRoot(.index, key, suffix);
    }

    pub fn deleteIndexCatalogRecord(self: *NativeFile, key: []const u8) !void {
        try self.putIndexCatalogBatch(&.{.{ .key = key, .is_delete = true }});
    }

    pub fn renameIndexCatalogRecord(self: *NativeFile, old_key: []const u8, new_key: []const u8) !void {
        try self.renameCatalogRecordForRoot(.index, old_key, new_key);
    }

    pub fn putIndexCatalogBatch(self: *NativeFile, mutations: []const CatalogMutation) !void {
        return try self.putCatalogBatchForRoot(.index, mutations, .{});
    }

    const CatalogRoots = struct { history: u64, index: u64 = 0, indexed: bool = false };

    fn readCatalogRoots(self: *NativeFile, page: u64, checkpoint: CheckpointSlot) !CatalogRoots {
        if (page == 0) return .{ .history = 0 };
        var scratch: [65536]u8 = undefined;
        const raw = try self.readPageInto(page, checkpoint, &scratch);
        if (raw[4] != @intFromEnum(PageKind.catalog_index)) {
            // Namespace-directory records have their own delta-chain format.
            // Metadata and index catalogs must carry the revision-3 descriptor.
            if (page != checkpoint.namespace_directory_root_page) return error.UnexpectedNativePageKind;
            _ = try decodePagePayload(raw, .catalog);
            return .{ .history = page };
        }
        const payload = try decodePagePayload(raw, .catalog_index);
        if (payload.len != 16) return error.InvalidNativePageChain;
        const history = std.mem.readInt(u64, payload[0..8], .little);
        const index = std.mem.readInt(u64, payload[8..16], .little);
        if (history == 0 or physicalPage(history) >= checkpoint.page_count or index >= checkpoint.page_count) return error.InvalidNativePageChain;
        return .{ .history = history, .index = index, .indexed = true };
    }

    fn catalogHistoryRoot(self: *NativeFile, checkpoint: CheckpointSlot, root: CatalogRoot) !u64 {
        return (try self.readCatalogRoots(catalogRootPage(checkpoint, root), checkpoint)).history;
    }

    fn lookupCatalogPage(self: *NativeFile, checkpoint: CheckpointSlot, root: CatalogRoot, key: []const u8) !?u64 {
        const roots = try self.readCatalogRoots(catalogRootPage(checkpoint, root), checkpoint);
        var indexed = checkpoint;
        indexed.document_index_root_page = roots.index;
        return try self.lookupDocumentIndexPage(indexed, key);
    }

    fn upsertCatalogIndex(self: *NativeFile, pages: *PageAllocator, index: u64, key: []const u8, page: u64) !u64 {
        var checkpoint = self.activeCheckpoint();
        checkpoint.page_count = pages.next_page_id;
        return try self.upsertDocumentIndex(pages, index, key, page, checkpoint);
    }

    fn writeCatalogRoot(_: *NativeFile, pages: *PageAllocator, history: u64, index: u64) !u64 {
        var payload: [16]u8 = undefined;
        std.mem.writeInt(u64, payload[0..8], history, .little);
        std.mem.writeInt(u64, payload[8..16], index, .little);
        const page = try pages.allocate();
        try pages.writePage(page, .catalog_index, &payload);
        return page;
    }

    fn validateCatalogIndexEntries(self: *NativeFile, checkpoint: CheckpointSlot, index: u64, reachable: *ReachablePageSet) !void {
        var indexed = checkpoint;
        indexed.document_index_root_page = index;
        var cursor = DocumentIndexCursor.init(self, indexed);
        defer cursor.deinit();
        var current = try cursor.first();
        while (current) |entry| {
            var owned = entry;
            defer owned.deinit(self.allocator);
            if (!reachable.contains(owned.document_page_id)) return error.InvalidNativePageChain;
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, owned.document_page_id, .catalog, checkpoint);
            defer self.allocator.free(payload);
            const record = try decodeCatalogEntry(payload);
            if (!std.mem.eql(u8, record.key, owned.key)) return error.InvalidNativePageChain;
            current = try cursor.next();
        }
    }

    fn putCatalogBatchForRoot(self: *NativeFile, root: CatalogRoot, mutations: []const CatalogMutation, options: WriteOptions) !void {
        if (self.read_only) return error.ReadOnly;
        if (mutations.len == 0) return;
        for (mutations) |mutation| try self.validateCatalogMutation(mutation);
        if (self.change_capture) |capture| for (mutations) |mutation| capture.record(self.allocator, if (root == .metadata) .metadata else .index, mutation.key);

        // Small index files include WAL control records, which are frequently
        // reset to the same contents. Avoid growing catalog history for those
        // writes. Bound comparison work to one inline record; bulk mutations
        // and spilled values retain the normal publication path.
        if (root == .index and mutations.len == 1 and !self.checkpoint_publication_uncertain) {
            const mutation = mutations[0];
            const size = try self.getCatalogRecordSizeFromRoot(root, mutation.key);
            if (mutation.is_delete and size == null) return self.syncIfRequired();
            if (mutation.external_value_root_page == 0 and !mutation.is_delete and size != null and size.? == mutation.value.len and self.catalogEntryFitsInline(mutation.key, mutation.value)) {
                const existing = try self.getCatalogRecordFromRootAlloc(self.allocator, root, mutation.key);
                defer if (existing) |bytes| self.allocator.free(bytes);
                if (existing) |bytes| {
                    if (std.mem.eql(u8, bytes, mutation.value)) return self.syncIfRequired();
                }
            }
        }

        const previous = self.activeCheckpoint();
        const roots = try self.readCatalogRoots(catalogRootPage(previous, root), previous);
        var next_root_page = roots.history;
        var page_allocator = try self.pageAllocatorFromFreeMap(previous);
        defer page_allocator.deinit();
        page_allocator.pack_records = self.header.packed_records and mutations.len > 1;
        var editor = IndexEditor.init(self, previous, roots.index);
        defer editor.deinit();

        for (mutations) |mutation| {
            var external_value_root_page: u64 = mutation.external_value_root_page;
            if (external_value_root_page == 0 and !mutation.is_delete and !self.catalogEntryFitsInline(mutation.key, mutation.value)) {
                external_value_root_page = try self.writeCatalogValue(&page_allocator, mutation.value, options);
            }

            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(self.allocator);
            try encodeCatalogEntry(self.allocator, &payload, .{
                .previous_page = next_root_page,
                .key = mutation.key,
                .value = mutation.value,
                .is_delete = mutation.is_delete,
                .external_value_root_page = external_value_root_page,
                .external_value_len = mutation.external_value_len,
            });
            const page_id = try page_allocator.writeRecord(.catalog, payload.items);
            next_root_page = page_id;
            if (mutation.is_delete) try editor.remove(mutation.key) else try editor.put(mutation.key, page_id);
        }

        const key_index = try editor.finish(&page_allocator);
        var next = previous;
        next.commit_sequence += 1;
        setCatalogRootPage(&next, root, try self.writeCatalogRoot(&page_allocator, next_root_page, key_index));
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        try page_allocator.flush();
        try self.writeFreeMapPage(next.free_map_root_page, next.page_count, page_allocator.remainingFreePages());
        try self.syncIfRequired();

        try self.publishCheckpoint(next);
    }

    fn appendCatalogRecordForRoot(self: *NativeFile, root: CatalogRoot, key: []const u8, suffix: []const u8) !void {
        if (self.change_capture) |capture| capture.record(self.allocator, if (root == .metadata) .metadata else .index, key);
        if (self.read_only) return error.ReadOnly;

        const previous = self.activeCheckpoint();
        const found_page = (try self.lookupCatalogPage(previous, root, key)) orelse {
            return try self.putCatalogBatchForRoot(root, &.{.{ .key = key, .value = suffix }}, .{});
        };
        const found_payload = try self.readPagePayloadByKindAlloc(self.allocator, found_page, .catalog);
        defer self.allocator.free(found_payload);
        const entry = try decodeCatalogEntry(found_payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNativePageChain;
        if (entry.is_delete) return try self.putCatalogBatchForRoot(root, &.{.{ .key = key, .value = suffix }}, .{});

        const old_len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
        const total_len = try std.math.add(usize, old_len, suffix.len);

        const roots = try self.readCatalogRoots(catalogRootPage(previous, root), previous);
        var next_root_page = roots.history;
        var page_allocator = try self.pageAllocatorFromFreeMap(previous);
        defer page_allocator.deinit();
        var key_index = roots.index;

        var external_value_root_page: u64 = 0;
        var inline_value: []u8 = &.{};
        defer if (inline_value.len > 0) self.allocator.free(inline_value);

        const fixed_len = 16 + key.len;
        const fits_inline = fixed_len <= self.maxPagePayloadBytes() and total_len <= self.maxPagePayloadBytes() - fixed_len;
        if (fits_inline) {
            inline_value = try self.allocator.alloc(u8, total_len);
            if (entry.external_value_root_page != 0) {
                const old_value = try self.readValuePagesAlloc(self.allocator, entry.external_value_root_page, entry.external_value_len);
                defer self.allocator.free(old_value);
                @memcpy(inline_value[0..old_value.len], old_value);
            } else {
                @memcpy(inline_value[0..entry.value.len], entry.value);
            }
            @memcpy(inline_value[old_len..], suffix);
        } else {
            external_value_root_page = try self.appendCatalogValueTree(&page_allocator, entry, suffix);
        }

        const catalog_page_id = try page_allocator.allocate();
        var payload = std.ArrayListUnmanaged(u8).empty;
        defer payload.deinit(self.allocator);
        try encodeCatalogEntryRaw(self.allocator, &payload, .{
            .previous_page = next_root_page,
            .key = key,
            .value = inline_value,
            .external_value_root_page = external_value_root_page,
            .external_value_len = if (external_value_root_page != 0) total_len else 0,
        });
        try page_allocator.writePage(catalog_page_id, .catalog, payload.items);
        next_root_page = catalog_page_id;
        key_index = try self.upsertCatalogIndex(&page_allocator, key_index, key, catalog_page_id);

        var next = previous;
        next.commit_sequence += 1;
        setCatalogRootPage(&next, root, try self.writeCatalogRoot(&page_allocator, next_root_page, key_index));
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        try page_allocator.flush();
        try self.writeFreeMapPage(next.free_map_root_page, next.page_count, page_allocator.remainingFreePages());
        try self.syncIfRequired();

        try self.publishCheckpoint(next);
    }

    fn renameCatalogRecordForRoot(self: *NativeFile, root: CatalogRoot, old_key: []const u8, new_key: []const u8) !void {
        if (self.change_capture) |capture| {
            capture.record(self.allocator, if (root == .metadata) .metadata else .index, old_key);
            capture.record(self.allocator, if (root == .metadata) .metadata else .index, new_key);
        }
        if (self.read_only) return error.ReadOnly;
        if (std.mem.eql(u8, old_key, new_key)) return;

        const previous = self.activeCheckpoint();
        const found_page = (try self.lookupCatalogPage(previous, root, old_key)) orelse {
            return;
        };
        const found_payload = try self.readPagePayloadByKindAlloc(self.allocator, found_page, .catalog);
        defer self.allocator.free(found_payload);
        const entry = try decodeCatalogEntry(found_payload);
        if (!std.mem.eql(u8, entry.key, old_key)) return error.InvalidNativePageChain;
        if (entry.is_delete) return;

        const roots = try self.readCatalogRoots(catalogRootPage(previous, root), previous);
        var next_root_page = roots.history;
        var page_allocator = try self.pageAllocatorFromFreeMap(previous);
        defer page_allocator.deinit();
        var editor = IndexEditor.init(self, previous, roots.index);
        defer editor.deinit();

        var external_value_root_page: u64 = 0;
        if (entry.external_value_root_page != 0) {
            external_value_root_page = entry.external_value_root_page;
        }

        {
            const new_page_id = try page_allocator.allocate();
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(self.allocator);
            try encodeCatalogEntryRaw(self.allocator, &payload, .{
                .previous_page = next_root_page,
                .key = new_key,
                .value = entry.value,
                .external_value_root_page = external_value_root_page,
                .external_value_len = entry.external_value_len,
            });
            try page_allocator.writePage(new_page_id, .catalog, payload.items);
            next_root_page = new_page_id;
            try editor.put(new_key, new_page_id);
        }

        {
            const tombstone_page_id = try page_allocator.allocate();
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(self.allocator);
            try encodeCatalogEntryRaw(self.allocator, &payload, .{
                .previous_page = next_root_page,
                .key = old_key,
                .is_delete = true,
            });
            try page_allocator.writePage(tombstone_page_id, .catalog, payload.items);
            next_root_page = tombstone_page_id;
            try editor.remove(old_key);
        }

        const key_index = try editor.finish(&page_allocator);
        var next = previous;
        next.commit_sequence += 1;
        setCatalogRootPage(&next, root, try self.writeCatalogRoot(&page_allocator, next_root_page, key_index));
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        try page_allocator.flush();
        try self.writeFreeMapPage(next.free_map_root_page, next.page_count, page_allocator.remainingFreePages());
        try self.syncIfRequired();

        try self.publishCheckpoint(next);
    }

    pub fn getCatalogRecordAlloc(self: *NativeFile, allocator: Allocator, key: []const u8) !?[]u8 {
        return try self.getCatalogRecordFromRootAlloc(allocator, .metadata, key);
    }

    /// Includes scope heads retained after every secret has been deleted.
    /// Caller holds the catalog lock; values never need to be loaded/decrypted.
    pub fn hasSecretState(self: *NativeFile) !bool {
        var cursor = try self.metadataCatalogCursor(self.activeCheckpoint(), secret_catalog_prefix);
        defer cursor.deinit();
        const entry = (try cursor.next()) orelse return false;
        self.allocator.free(entry.key);
        return true;
    }

    pub fn metadataCatalogCursor(self: *NativeFile, checkpoint: CheckpointSlot, prefix: []const u8) !CatalogCursor {
        return try self.catalogCursor(checkpoint, .metadata, prefix);
    }

    pub fn getIndexCatalogRecordAlloc(self: *NativeFile, allocator: Allocator, key: []const u8) !?[]u8 {
        return try self.getCatalogRecordFromRootAlloc(allocator, .index, key);
    }

    pub fn getIndexCatalogRecordAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, key: []const u8, checkpoint: CheckpointSlot) !?[]u8 {
        return try self.getCatalogRecordFromRootAtCheckpointAlloc(allocator, .index, key, checkpoint);
    }

    pub fn getIndexCatalogRecordSize(self: *NativeFile, key: []const u8) !?usize {
        return try self.getCatalogRecordSizeFromRoot(.index, key);
    }

    pub fn getIndexCatalogRecordSizeAtCheckpoint(self: *NativeFile, key: []const u8, checkpoint: CheckpointSlot) !?usize {
        return try self.getCatalogRecordSizeFromRootAtCheckpoint(.index, key, checkpoint);
    }

    pub fn getIndexCatalogRecordRangeAlloc(
        self: *NativeFile,
        allocator: Allocator,
        key: []const u8,
        offset: u64,
        len: usize,
    ) !?[]u8 {
        return try self.getCatalogRecordRangeFromRootAlloc(allocator, .index, key, offset, len);
    }

    pub fn getIndexCatalogRecordRangeAtCheckpointAlloc(
        self: *NativeFile,
        allocator: Allocator,
        key: []const u8,
        offset: u64,
        len: usize,
        checkpoint: CheckpointSlot,
    ) !?[]u8 {
        return try self.getCatalogRecordRangeFromRootAtCheckpointAlloc(allocator, .index, key, offset, len, checkpoint);
    }

    fn getCatalogRecordFromRootAlloc(
        self: *NativeFile,
        allocator: Allocator,
        root: CatalogRoot,
        key: []const u8,
    ) !?[]u8 {
        return try self.getCatalogRecordFromRootAtCheckpointAlloc(allocator, root, key, self.activeCheckpoint());
    }

    fn getCatalogRecordFromRootAtCheckpointAlloc(
        self: *NativeFile,
        allocator: Allocator,
        root: CatalogRoot,
        key: []const u8,
        checkpoint: CheckpointSlot,
    ) !?[]u8 {
        const page_id = (try self.lookupCatalogPage(checkpoint, root, key)) orelse return null;
        var scratch: [65536]u8 = undefined;
        const payload = try decodePagePayload(try self.readPageInto(page_id, checkpoint, &scratch), .catalog);
        const entry = try decodeCatalogEntry(payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNativePageChain;
        if (entry.is_delete) return null;
        return try self.catalogEntryValueAtCheckpointAlloc(allocator, entry, checkpoint);
    }

    fn getCatalogRecordSizeFromRoot(
        self: *NativeFile,
        root: CatalogRoot,
        key: []const u8,
    ) !?usize {
        return try self.getCatalogRecordSizeFromRootAtCheckpoint(root, key, self.activeCheckpoint());
    }

    fn getCatalogRecordSizeFromRootAtCheckpoint(
        self: *NativeFile,
        root: CatalogRoot,
        key: []const u8,
        checkpoint: CheckpointSlot,
    ) !?usize {
        const page_id = (try self.lookupCatalogPage(checkpoint, root, key)) orelse return null;
        var scratch: [65536]u8 = undefined;
        const payload = try decodePagePayload(try self.readPageInto(page_id, checkpoint, &scratch), .catalog);
        const entry = try decodeCatalogEntry(payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNativePageChain;
        if (entry.is_delete) return null;
        return if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
    }

    fn getCatalogRecordRangeFromRootAlloc(
        self: *NativeFile,
        allocator: Allocator,
        root: CatalogRoot,
        key: []const u8,
        offset: u64,
        len: usize,
    ) !?[]u8 {
        return try self.getCatalogRecordRangeFromRootAtCheckpointAlloc(allocator, root, key, offset, len, self.activeCheckpoint());
    }

    fn getCatalogRecordRangeFromRootAtCheckpointAlloc(
        self: *NativeFile,
        allocator: Allocator,
        root: CatalogRoot,
        key: []const u8,
        offset: u64,
        len: usize,
        checkpoint: CheckpointSlot,
    ) !?[]u8 {
        const page_id = (try self.lookupCatalogPage(checkpoint, root, key)) orelse return null;
        var scratch: [65536]u8 = undefined;
        const payload = try decodePagePayload(try self.readPageInto(page_id, checkpoint, &scratch), .catalog);
        const entry = try decodeCatalogEntry(payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNativePageChain;
        if (entry.is_delete) return null;
        return try self.catalogEntryRangeAlloc(allocator, entry, offset, len, checkpoint);
    }

    pub fn snapshotCatalogRecordsAlloc(self: *NativeFile, allocator: Allocator) ![]OwnedCatalogRecord {
        return try self.snapshotCatalogRecordsFromRootAlloc(allocator, .metadata);
    }

    pub fn snapshotIndexCatalogRecordsAlloc(self: *NativeFile, allocator: Allocator) ![]OwnedCatalogRecord {
        return try self.snapshotCatalogRecordsFromRootAlloc(allocator, .index);
    }

    pub fn snapshotIndexCatalogKeysAlloc(self: *NativeFile, allocator: Allocator) ![]OwnedCatalogKey {
        return try self.snapshotCatalogKeysFromRootAlloc(allocator, .index);
    }

    pub fn indexCatalogCursor(self: *NativeFile, checkpoint: CheckpointSlot, prefix: []const u8) !CatalogCursor {
        return try self.catalogCursor(checkpoint, .index, prefix);
    }

    /// Requires canonical file keys with no empty path components. The
    /// borrowed prefix must end at a directory separator. Callers accepting
    /// repeated/trailing separators must use indexCatalogCursor and dirname
    /// filtering instead: such byte ranges can contain immediate files.
    /// Nested subtrees are skipped within the same checkpoint.
    pub fn indexCatalogDirectoryCursor(self: *NativeFile, checkpoint: CheckpointSlot, prefix: []const u8) !CatalogCursor {
        if (prefix.len == 0 or prefix[prefix.len - 1] != '/') return error.InvalidNativeIndexPath;
        var cursor = try self.catalogCursor(checkpoint, .index, prefix);
        cursor.immediate_children_only = true;
        return cursor;
    }

    fn catalogCursor(self: *NativeFile, checkpoint: CheckpointSlot, root: CatalogRoot, prefix: []const u8) !CatalogCursor {
        const roots = try self.readCatalogRoots(catalogRootPage(checkpoint, root), checkpoint);
        var indexed = checkpoint;
        indexed.document_index_root_page = roots.index;
        return .{ .index = DocumentIndexCursor.init(self, indexed), .prefix = prefix };
    }

    fn snapshotCatalogRecordsFromRootAlloc(self: *NativeFile, allocator: Allocator, root: CatalogRoot) ![]OwnedCatalogRecord {
        var map = std.StringHashMapUnmanaged(?[]u8).empty;
        defer {
            var it = map.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                if (entry.value_ptr.*) |value| allocator.free(value);
            }
            map.deinit(allocator);
        }

        var page_id = try self.catalogHistoryRoot(self.activeCheckpoint(), root);
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAlloc(allocator, page_id, .catalog);
            defer allocator.free(payload);
            const entry = try decodeCatalogEntry(payload);

            if (!map.contains(entry.key)) {
                const owned_key = try allocator.dupe(u8, entry.key);
                errdefer allocator.free(owned_key);
                const owned_value = if (entry.is_delete) null else try self.catalogEntryValueAlloc(allocator, entry);
                errdefer if (owned_value) |value| allocator.free(value);
                try map.put(allocator, owned_key, owned_value);
            }
            page_id = entry.previous_page;
        }

        var records = std.ArrayListUnmanaged(OwnedCatalogRecord).empty;
        errdefer {
            for (records.items) |record| {
                allocator.free(record.key);
                allocator.free(record.value);
            }
            records.deinit(allocator);
        }
        var it = map.iterator();
        while (it.next()) |entry| {
            const stored_value = entry.value_ptr.* orelse continue;
            const key = try allocator.dupe(u8, entry.key_ptr.*);
            errdefer allocator.free(key);
            const value = try allocator.dupe(u8, stored_value);
            errdefer allocator.free(value);
            try records.append(allocator, .{ .key = key, .value = value });
        }

        std.mem.sort(OwnedCatalogRecord, records.items, {}, struct {
            fn lessThan(_: void, lhs: OwnedCatalogRecord, rhs: OwnedCatalogRecord) bool {
                return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            }
        }.lessThan);

        return try records.toOwnedSlice(allocator);
    }

    fn snapshotCatalogKeysFromRootAlloc(self: *NativeFile, allocator: Allocator, root: CatalogRoot) ![]OwnedCatalogKey {
        var cursor = try self.catalogCursor(self.activeCheckpoint(), root, "");
        defer cursor.deinit();
        var keys = std.ArrayListUnmanaged(OwnedCatalogKey).empty;
        errdefer {
            for (keys.items) |record| allocator.free(record.key);
            keys.deinit(allocator);
        }
        while (try cursor.next()) |record| {
            defer self.allocator.free(record.key);
            const key = try allocator.dupe(u8, record.key);
            errdefer allocator.free(key);
            try keys.append(allocator, .{ .key = key });
        }
        return try keys.toOwnedSlice(allocator);
    }

    pub fn freeSnapshotCatalogRecords(allocator: Allocator, records: []OwnedCatalogRecord) void {
        for (records) |record| {
            allocator.free(record.key);
            allocator.free(record.value);
        }
        allocator.free(records);
    }

    pub fn freeSnapshotCatalogKeys(allocator: Allocator, records: []OwnedCatalogKey) void {
        for (records) |record| allocator.free(record.key);
        allocator.free(records);
    }

    fn snapshotCatalogRefsFromRootAlloc(self: *NativeFile, allocator: Allocator, root: CatalogRoot) ![]OwnedLiveRecordRef {
        var seen = std.StringHashMapUnmanaged(void).empty;
        defer seen.deinit(allocator);
        var tombstones = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (tombstones.items) |key| allocator.free(key);
            tombstones.deinit(allocator);
        }
        var refs = std.ArrayListUnmanaged(OwnedLiveRecordRef).empty;
        errdefer {
            for (refs.items) |record| allocator.free(record.key);
            refs.deinit(allocator);
        }

        var page_id = try self.catalogHistoryRoot(self.activeCheckpoint(), root);
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAlloc(allocator, page_id, .catalog);
            defer allocator.free(payload);
            const entry = try decodeCatalogEntry(payload);
            if (!seen.contains(entry.key)) {
                try seen.ensureUnusedCapacity(allocator, 1);
                const key = try allocator.dupe(u8, entry.key);
                errdefer allocator.free(key);
                if (entry.is_delete) {
                    try tombstones.append(allocator, key);
                } else {
                    try refs.append(allocator, .{ .key = key, .page_id = page_id });
                }
                seen.putAssumeCapacity(key, {});
            }
            page_id = entry.previous_page;
        }
        std.mem.sort(OwnedLiveRecordRef, refs.items, {}, liveRecordRefLessThan);
        return try refs.toOwnedSlice(allocator);
    }

    fn snapshotDocumentRefsAlloc(self: *NativeFile, allocator: Allocator) ![]OwnedLiveRecordRef {
        var seen = std.StringHashMapUnmanaged(void).empty;
        defer seen.deinit(allocator);
        var tombstones = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (tombstones.items) |key| allocator.free(key);
            tombstones.deinit(allocator);
        }
        var refs = std.ArrayListUnmanaged(OwnedLiveRecordRef).empty;
        errdefer {
            for (refs.items) |record| allocator.free(record.key);
            refs.deinit(allocator);
        }

        var page_id = self.activeCheckpoint().document_root_page;
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAlloc(allocator, page_id, .document);
            defer allocator.free(payload);
            const entry = try decodeDocumentEntry(payload);
            if (!seen.contains(entry.key)) {
                try seen.ensureUnusedCapacity(allocator, 1);
                const key = try allocator.dupe(u8, entry.key);
                errdefer allocator.free(key);
                if (entry.is_delete) {
                    try tombstones.append(allocator, key);
                } else {
                    try refs.append(allocator, .{ .key = key, .page_id = page_id });
                }
                seen.putAssumeCapacity(key, {});
            }
            page_id = entry.previous_page;
        }
        std.mem.sort(OwnedLiveRecordRef, refs.items, {}, liveRecordRefLessThan);
        return try refs.toOwnedSlice(allocator);
    }

    fn freeLiveRecordRefs(allocator: Allocator, refs: []OwnedLiveRecordRef) void {
        for (refs) |record| allocator.free(record.key);
        allocator.free(refs);
    }

    const LiveRecordSource = union(enum) {
        catalog: CatalogRoot,
        documents,
    };

    /// Borrow record metadata from the checkpoint's ordered index. Values stay
    /// on disk; returned slices remain valid until the next call to next().
    const LiveRecordCursor = struct {
        index: DocumentIndexCursor,
        kind: PageKind,
        started: bool = false,
        records: RecordPageReader = .{},

        fn init(file: *NativeFile, source: LiveRecordSource) !LiveRecordCursor {
            var checkpoint = file.activeCheckpoint();
            const kind: PageKind = switch (source) {
                .catalog => |root| blk: {
                    const roots = try file.readCatalogRoots(catalogRootPage(checkpoint, root), checkpoint);
                    checkpoint.document_index_root_page = roots.index;
                    break :blk .catalog;
                },
                .documents => .document,
            };
            return .{ .index = DocumentIndexCursor.init(file, checkpoint), .kind = kind };
        }

        fn deinit(self: *LiveRecordCursor) void {
            self.records.deinit(self.index.file.allocator);
            self.index.deinit();
        }

        fn next(self: *LiveRecordCursor, cancel: ?*const maintenance.CancelToken) !?CatalogEntry {
            const file = self.index.file;
            while (true) {
                if (cancel) |token| try token.check();
                var indexed = (if (self.started) try self.index.next() else try self.index.first()) orelse return null;
                self.started = true;
                defer indexed.deinit(file.allocator);
                const payload = try self.records.read(file, file.allocator, self.index.checkpoint, indexed.document_page_id, self.kind);
                const entry = if (self.kind == .catalog) try decodeCatalogEntry(payload) else blk: {
                    const document = try decodeDocumentEntry(payload);
                    break :blk CatalogEntry{
                        .previous_page = document.previous_page,
                        .key = document.key,
                        .value = document.value,
                        .is_delete = document.is_delete,
                        .external_value_root_page = document.external_value_root_page,
                        .external_value_len = document.external_value_len,
                    };
                };
                if (!std.mem.eql(u8, entry.key, indexed.key)) return error.InvalidDocumentIndex;
                if (!entry.is_delete) return entry;
            }
        }
    };

    const NamespaceDirectory = std.StringHashMapUnmanaged(u64);
    const NamespaceDirectoryRecordKind = enum(u8) {
        snapshot = 0,
        delta = 1,
    };

    const LoadedNamespaceDirectory = struct {
        entries: NamespaceDirectory,
        delta_depth: u16,
    };

    fn documentNamespace(key: []const u8) []const u8 {
        const end = (std.mem.indexOfScalar(u8, key, 0) orelse return "") + 1;
        return key[0..end];
    }

    fn deinitNamespaceDirectory(allocator: Allocator, directory: *NamespaceDirectory) void {
        var it = directory.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        directory.deinit(allocator);
    }

    fn applyNamespaceDirectoryRecord(
        allocator: Allocator,
        directory: *NamespaceDirectory,
        raw: []const u8,
    ) !NamespaceDirectoryRecordKind {
        if (raw.len < namespace_directory_magic.len + 1 + 4 or
            !std.mem.eql(u8, raw[0..namespace_directory_magic.len], namespace_directory_magic))
            return error.InvalidNamespaceDirectory;
        var offset: usize = namespace_directory_magic.len;
        const kind: NamespaceDirectoryRecordKind = switch (raw[offset]) {
            0 => .snapshot,
            1 => .delta,
            else => return error.InvalidNamespaceDirectory,
        };
        offset += 1;
        const count = std.mem.readInt(u32, raw[offset..][0..4], .little);
        offset += 4;
        if (@as(usize, count) > (raw.len - offset) / 12) return error.InvalidNamespaceDirectory;
        try directory.ensureUnusedCapacity(allocator, count);
        var record_keys = std.StringHashMapUnmanaged(void).empty;
        defer record_keys.deinit(allocator);
        try record_keys.ensureTotalCapacity(allocator, count);
        var i: u32 = 0;
        while (i < count) : (i += 1) {
            if (raw.len - offset < 4) return error.InvalidNamespaceDirectory;
            const len = std.mem.readInt(u32, raw[offset..][0..4], .little);
            offset += 4;
            if (len > raw.len - offset or raw.len - offset - len < 8) return error.InvalidNamespaceDirectory;
            const raw_key = raw[offset..][0..len];
            offset += len;
            const head = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            if ((raw_key.len > 0 and raw_key[raw_key.len - 1] != 0) or head == 0 or record_keys.contains(raw_key))
                return error.InvalidNamespaceDirectory;
            record_keys.putAssumeCapacity(raw_key, {});

            // Records are replayed newest to oldest. The first head for a
            // namespace is authoritative; older snapshots/deltas fill only
            // namespaces not mentioned by a newer record.
            if (!directory.contains(raw_key)) {
                const key = try allocator.dupe(u8, raw_key);
                directory.putAssumeCapacity(key, head);
            }
        }
        if (offset != raw.len) return error.InvalidNamespaceDirectory;
        return kind;
    }

    fn encodeNamespaceDirectoryAlloc(
        allocator: Allocator,
        kind: NamespaceDirectoryRecordKind,
        directory: *const NamespaceDirectory,
    ) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(allocator);
        errdefer out.deinit();
        try out.writer.writeAll(namespace_directory_magic);
        try out.writer.writeByte(@intFromEnum(kind));
        try out.writer.writeInt(u32, std.math.cast(u32, directory.count()) orelse return error.RecordTooLarge, .little);
        var it = directory.iterator();
        while (it.next()) |entry| {
            try out.writer.writeInt(u32, @intCast(entry.key_ptr.*.len), .little);
            try out.writer.writeAll(entry.key_ptr.*);
            try out.writer.writeInt(u64, entry.value_ptr.*, .little);
        }
        return try out.toOwnedSlice();
    }

    fn loadNamespaceDirectoryWithDepthAlloc(self: *NativeFile, allocator: Allocator) !?LoadedNamespaceDirectory {
        return try self.loadNamespaceDirectoryWithDepthAtCheckpointAlloc(allocator, self.activeCheckpoint());
    }

    fn loadNamespaceDirectoryWithDepthAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot) !?LoadedNamespaceDirectory {
        var root = checkpoint.namespace_directory_root_page;
        if (root == 0) return null;
        var directory = NamespaceDirectory.empty;
        errdefer deinitNamespaceDirectory(allocator, &directory);
        var depth: u16 = 0;
        var walked: u64 = 0;
        while (root != 0) {
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(allocator, root, .catalog, checkpoint);
            defer allocator.free(payload);
            const entry = try decodeCatalogEntry(payload);
            if (!std.mem.eql(u8, entry.key, namespace_directory_key) or entry.is_delete)
                return error.InvalidNamespaceDirectory;
            const raw = try self.catalogEntryValueAlloc(allocator, entry);
            defer allocator.free(raw);
            const kind = try applyNamespaceDirectoryRecord(allocator, &directory, raw);
            walked += 1;
            if (walked > recordWalkLimit(checkpoint, self.header.page_size)) return error.InvalidNativePageChain;
            switch (kind) {
                .snapshot => {
                    if (entry.previous_page != 0) return error.InvalidNamespaceDirectory;
                    return .{ .entries = directory, .delta_depth = depth };
                },
                .delta => {
                    depth = std.math.add(u16, depth, 1) catch return error.InvalidNamespaceDirectory;
                    root = entry.previous_page;
                    if (root == 0) return error.InvalidNamespaceDirectory;
                },
            }
        }
        return error.InvalidNamespaceDirectory;
    }

    fn loadNamespaceDirectoryAlloc(self: *NativeFile, allocator: Allocator) !?NamespaceDirectory {
        const loaded = (try self.loadNamespaceDirectoryWithDepthAlloc(allocator)) orelse return null;
        return loaded.entries;
    }

    fn loadNamespaceDirectoryAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot) !?NamespaceDirectory {
        const loaded = (try self.loadNamespaceDirectoryWithDepthAtCheckpointAlloc(allocator, checkpoint)) orelse return null;
        return loaded.entries;
    }

    fn ensureNamespaceDirectoryCache(self: *NativeFile) !void {
        const root = self.activeCheckpoint().namespace_directory_root_page;
        if (self.namespace_directory_cache_root == root) return;
        const loaded = (try self.loadNamespaceDirectoryWithDepthAlloc(self.allocator)) orelse LoadedNamespaceDirectory{
            .entries = NamespaceDirectory.empty,
            .delta_depth = 0,
        };
        deinitNamespaceDirectory(self.allocator, &self.namespace_directory_cache);
        self.namespace_directory_cache = loaded.entries;
        self.namespace_directory_delta_depth = loaded.delta_depth;
        self.namespace_directory_cache_root = root;
    }

    fn validateNamespaceDirectory(self: *NativeFile, checkpoint: CheckpointSlot) !void {
        var directory = (try self.loadNamespaceDirectoryAlloc(self.allocator)) orelse return;
        defer deinitNamespaceDirectory(self.allocator, &directory);

        // Verify the complete index in one global-history pass. Each map value
        // is the document page that the next occurrence of that namespace must
        // have. This proves directory heads are current, every document is
        // indexed exactly once, and every namespace link targets the next older
        // document without adding a second O(history) namespace traversal.
        var expected_pages = std.StringHashMapUnmanaged(u64).empty;
        defer expected_pages.deinit(self.allocator);
        try expected_pages.ensureTotalCapacity(self.allocator, directory.count());
        var directory_it = directory.iterator();
        while (directory_it.next()) |entry| {
            expected_pages.putAssumeCapacity(entry.key_ptr.*, entry.value_ptr.*);
        }

        var page_id = checkpoint.document_root_page;
        var walked: u64 = 0;
        while (page_id != 0) {
            if (physicalPage(page_id) >= checkpoint.page_count) return error.InvalidPageId;
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, page_id, .document, checkpoint);
            defer self.allocator.free(payload);
            const entry = try decodeDocumentEntry(payload);
            const expected = expected_pages.getPtr(documentNamespace(entry.key)) orelse return error.InvalidNamespaceDirectory;
            if (expected.* != page_id) return error.InvalidNamespaceDirectory;
            expected.* = entry.previous_namespace_page;
            page_id = entry.previous_page;
            walked += 1;
            if (walked > recordWalkLimit(checkpoint, self.header.page_size)) return error.InvalidNativePageChain;
        }
        var expected_it = expected_pages.valueIterator();
        while (expected_it.next()) |expected| {
            if (expected.* != 0) return error.InvalidNamespaceDirectory;
        }
    }

    pub fn putDocument(self: *NativeFile, key: []const u8, value: []const u8) !void {
        try self.putDocumentBatch(&.{.{ .key = key, .value = value }});
    }

    pub fn deleteDocument(self: *NativeFile, key: []const u8) !void {
        try self.putDocumentBatch(&.{.{ .key = key, .is_delete = true }});
    }

    pub fn putDocumentBatch(self: *NativeFile, mutations: []const DocumentMutation) !void {
        if (self.read_only) return error.ReadOnly;
        if (mutations.len == 0) return;
        for (mutations) |mutation| try self.validateDocumentMutation(mutation);
        if (self.change_capture) |capture| for (mutations) |mutation| capture.record(self.allocator, .documents, mutation.key);

        const previous = self.activeCheckpoint();
        try self.ensureNamespaceDirectoryCache();
        if (previous.document_root_page != 0 and self.namespace_directory_cache.count() == 0)
            return error.InvalidNamespaceDirectory;
        var next_root_page = previous.document_root_page;
        var next_index_root_page = previous.document_index_root_page;
        const bulk_build_initial_index = next_index_root_page == 0;
        var initial_index_entries = std.ArrayListUnmanaged(PendingDocumentIndexEntry).empty;
        defer initial_index_entries.deinit(self.allocator);
        if (bulk_build_initial_index) try initial_index_entries.ensureTotalCapacity(self.allocator, mutations.len);
        var page_allocator = try self.pageAllocatorFromFreeMap(previous);
        defer page_allocator.deinit();
        page_allocator.pack_records = self.header.packed_records and mutations.len > 1;
        var editor = IndexEditor.init(self, previous, next_index_root_page);
        defer editor.deinit();
        var changed_heads = NamespaceDirectory.empty;
        defer changed_heads.deinit(self.allocator);
        try changed_heads.ensureTotalCapacity(
            self.allocator,
            std.math.cast(u32, mutations.len) orelse return error.RecordTooLarge,
        );

        for (mutations, 0..) |mutation, ordinal| {
            var external_value_root_page: u64 = mutation.external_value_root_page;
            if (external_value_root_page == 0 and !mutation.is_delete and !self.documentEntryFitsInline(mutation.key, mutation.value)) {
                external_value_root_page = try self.writeValuePagesAllocated(&page_allocator, mutation.value);
            }

            const namespace = documentNamespace(mutation.key);
            const previous_namespace_page = changed_heads.get(namespace) orelse
                self.namespace_directory_cache.get(namespace) orelse 0;
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(self.allocator);
            try encodeDocumentEntry(self.allocator, &payload, .{
                .previous_page = next_root_page,
                .previous_namespace_page = previous_namespace_page,
                .key = mutation.key,
                .value = mutation.value,
                .is_delete = mutation.is_delete,
                .external_value_root_page = external_value_root_page,
                .external_value_len = mutation.external_value_len,
            });
            const page_id = try page_allocator.writeRecord(.document, payload.items);
            next_root_page = page_id;
            if (bulk_build_initial_index) {
                initial_index_entries.appendAssumeCapacity(.{
                    .key = mutation.key,
                    .document_page_id = page_id,
                    .ordinal = ordinal,
                });
            } else {
                try editor.put(mutation.key, page_id);
            }
            if (changed_heads.getPtr(namespace)) |head| {
                head.* = page_id;
            } else {
                // Mutation keys remain live for the duration of this commit;
                // ownership is acquired only for namespaces newly entering
                // the durable materialized cache below.
                changed_heads.putAssumeCapacity(namespace, page_id);
            }
        }

        if (bulk_build_initial_index) {
            std.mem.sort(PendingDocumentIndexEntry, initial_index_entries.items, {}, PendingDocumentIndexEntry.lessThan);
            var builder = DocumentIndexBulkBuilder{
                .owner = self,
                .file = self.file,
                .next_page_id = &page_allocator.next_page_id,
            };
            defer builder.deinit();
            var index: usize = 0;
            while (index < initial_index_entries.items.len) {
                var end = index + 1;
                while (end < initial_index_entries.items.len and
                    std.mem.eql(u8, initial_index_entries.items[index].key, initial_index_entries.items[end].key)) : (end += 1)
                {}
                const latest = initial_index_entries.items[end - 1];
                try builder.add(latest.key, latest.document_page_id);
                index = end;
            }
            next_index_root_page = try builder.finish();
        } else {
            next_index_root_page = try editor.finish(&page_allocator);
        }

        const write_snapshot = previous.namespace_directory_root_page == 0 or
            self.namespace_directory_delta_depth + 1 >= namespace_directory_snapshot_interval;
        var snapshot = NamespaceDirectory.empty;
        defer snapshot.deinit(self.allocator);
        const directory_to_encode = if (write_snapshot) blk: {
            try snapshot.ensureTotalCapacity(
                self.allocator,
                self.namespace_directory_cache.count() + changed_heads.count(),
            );
            var cached_it = self.namespace_directory_cache.iterator();
            while (cached_it.next()) |entry|
                snapshot.putAssumeCapacity(entry.key_ptr.*, entry.value_ptr.*);
            var changed_it = changed_heads.iterator();
            while (changed_it.next()) |entry| {
                if (snapshot.getPtr(entry.key_ptr.*)) |head|
                    head.* = entry.value_ptr.*
                else
                    snapshot.putAssumeCapacity(entry.key_ptr.*, entry.value_ptr.*);
            }
            break :blk &snapshot;
        } else &changed_heads;
        const record_kind: NamespaceDirectoryRecordKind = if (write_snapshot) .snapshot else .delta;
        const encoded_directory = try encodeNamespaceDirectoryAlloc(self.allocator, record_kind, directory_to_encode);
        defer self.allocator.free(encoded_directory);
        try self.validateCatalogMutation(.{ .key = namespace_directory_key, .value = encoded_directory });
        var directory_external_root: u64 = 0;
        if (!self.catalogEntryFitsInline(namespace_directory_key, encoded_directory)) {
            directory_external_root = try self.writeValuePagesAllocated(&page_allocator, encoded_directory);
        }
        const directory_page = try page_allocator.allocate();
        var directory_payload = std.ArrayListUnmanaged(u8).empty;
        defer directory_payload.deinit(self.allocator);
        try encodeCatalogEntry(self.allocator, &directory_payload, .{
            .previous_page = if (write_snapshot) 0 else previous.namespace_directory_root_page,
            .key = namespace_directory_key,
            .value = encoded_directory,
            .external_value_root_page = directory_external_root,
        });
        try page_allocator.writePage(directory_page, .catalog, directory_payload.items);

        var next = previous;
        next.commit_sequence += 1;
        next.document_root_page = next_root_page;
        next.namespace_directory_root_page = directory_page;
        next.document_index_root_page = next_index_root_page;
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        try page_allocator.flush();
        try self.writeFreeMapPage(next.free_map_root_page, next.page_count, page_allocator.remainingFreePages());
        try self.syncIfRequired();

        // Reserve and allocate all cache state before publishing. After the
        // checkpoint is durable, cache publication is allocation-free and
        // therefore cannot fail or diverge from disk state.
        try self.namespace_directory_cache.ensureUnusedCapacity(self.allocator, changed_heads.count());
        var new_entries = std.ArrayListUnmanaged(struct { key: []u8, head: u64 }).empty;
        defer {
            for (new_entries.items) |entry| self.allocator.free(entry.key);
            new_entries.deinit(self.allocator);
        }
        try new_entries.ensureTotalCapacity(self.allocator, changed_heads.count());
        var changed_it = changed_heads.iterator();
        while (changed_it.next()) |entry| {
            if (!self.namespace_directory_cache.contains(entry.key_ptr.*)) {
                const owned = try self.allocator.dupe(u8, entry.key_ptr.*);
                new_entries.appendAssumeCapacity(.{ .key = owned, .head = entry.value_ptr.* });
            }
        }

        try self.publishCheckpoint(next);

        changed_it = changed_heads.iterator();
        while (changed_it.next()) |entry| {
            if (self.namespace_directory_cache.getPtr(entry.key_ptr.*)) |head|
                head.* = entry.value_ptr.*;
        }
        for (new_entries.items) |entry| {
            self.namespace_directory_cache.putAssumeCapacity(entry.key, entry.head);
        }
        new_entries.items.len = 0;
        self.namespace_directory_cache_root = directory_page;
        self.namespace_directory_delta_depth = if (write_snapshot) 0 else self.namespace_directory_delta_depth + 1;
    }

    fn readDocumentIndexNode(self: *NativeFile, page_id: u64, checkpoint: CheckpointSlot) !DocumentIndexNode {
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, page_id, .document_index, checkpoint);
        defer self.allocator.free(payload);
        return try self.decodeResolvedDocumentIndexNode(payload, checkpoint);
    }

    fn decodeResolvedDocumentIndexNode(self: *NativeFile, payload: []const u8, checkpoint: CheckpointSlot) !DocumentIndexNode {
        var node = try decodeDocumentIndexNode(self.allocator, payload);
        errdefer node.deinit(self.allocator);
        for (0..node.keys.len) |i| {
            const key = try self.resolveIndexKey(&node, i, checkpoint);
            if (i > 0 and std.mem.order(u8, node.keys[i - 1], key) != .lt) return error.InvalidDocumentIndex;
        }
        return node;
    }

    fn resolveIndexKey(self: *NativeFile, node: *DocumentIndexNode, index: usize, checkpoint: CheckpointSlot) ![]const u8 {
        const page = node.key_pages.?[index];
        if (page == 0 or node.keys[index].len != 0) return node.keys[index];
        const raw = try self.readPageAllocForCheckpoint(self.allocator, page, checkpoint);
        defer self.allocator.free(raw);
        const bytes = switch (raw[4]) {
            @intFromEnum(PageKind.catalog) => (try decodeCatalogEntry(try decodePagePayload(raw, .catalog))).key,
            @intFromEnum(PageKind.document) => (try decodeDocumentEntry(try decodePagePayload(raw, .document))).key,
            else => return error.InvalidDocumentIndex,
        };
        if (bytes.len <= index_inline_key_limit) return error.InvalidDocumentIndex;
        const owned = try self.allocator.dupe(u8, bytes);
        self.allocator.free(node.keys[index]);
        node.keys[index] = owned;
        return owned;
    }

    fn writeDocumentIndexNode(self: *NativeFile, page_allocator: *PageAllocator, node: DocumentIndexNode) !u64 {
        const encoded = try encodeDocumentIndexNode(self.allocator, node);
        defer self.allocator.free(encoded);
        if (encoded.len > self.maxPagePayloadBytes()) return error.DocumentIndexNodeTooLarge;
        const page_id = try page_allocator.allocate();
        try page_allocator.writePage(page_id, .document_index, encoded);
        return page_id;
    }

    fn upsertDocumentIndex(
        self: *NativeFile,
        page_allocator: *PageAllocator,
        root_page_id: u64,
        key: []const u8,
        document_page_id: u64,
        checkpoint: CheckpointSlot,
    ) !u64 {
        var editor = IndexEditor.init(self, checkpoint, root_page_id);
        defer editor.deinit();
        try editor.put(key, document_page_id);
        return try editor.finish(page_allocator);
    }

    fn lookupDocumentIndexPage(self: *NativeFile, checkpoint: CheckpointSlot, key: []const u8) !?u64 {
        var page_id = checkpoint.document_index_root_page;
        var scratch: [65536]u8 = undefined;
        var depth: usize = 0;
        while (page_id != 0) : (depth += 1) {
            if (depth > 64) return error.InvalidDocumentIndex;
            const raw = try decodePagePayload(try self.readPageInto(page_id, checkpoint, &scratch), .document_index);
            const probe = probeDocumentIndexNode(raw, key) catch |err| switch (err) {
                error.ExternalIndexKey => blk: {
                    // Decode slot offsets once, but fetch only keys visited by
                    // binary search. A point lookup must not read every large
                    // key referenced by a high-fanout page. Full ordering and
                    // reference coverage are audited by check().
                    var node = try decodeDocumentIndexNode(self.allocator, raw);
                    defer node.deinit(self.allocator);
                    var low: usize = 0;
                    var high = node.keys.len;
                    while (low < high) {
                        const mid = low + (high - low) / 2;
                        const candidate = try self.resolveIndexKey(&node, mid, checkpoint);
                        const order = std.mem.order(u8, candidate, key);
                        if (order == .lt or (node.kind == .internal and order == .eq)) low = mid + 1 else high = mid;
                    }
                    if (node.kind == .leaf) {
                        const matches = low < node.keys.len and std.mem.eql(u8, try self.resolveIndexKey(&node, low, checkpoint), key);
                        break :blk IndexProbe{ .leaf = true, .page = if (matches) node.pointers[low] else null };
                    }
                    break :blk IndexProbe{ .leaf = false, .page = node.pointers[low] };
                },
                else => return err,
            };
            if (probe.leaf) return probe.page;
            page_id = probe.page orelse return error.InvalidDocumentIndex;
        }
        return null;
    }

    pub fn documentValueAtIndexEntryAlloc(self: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot, indexed: DocumentIndexEntry) !?[]u8 {
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(allocator, indexed.document_page_id, .document, checkpoint);
        defer allocator.free(payload);
        const entry = try decodeDocumentEntry(payload);
        if (!std.mem.eql(u8, entry.key, indexed.key)) return error.InvalidDocumentIndex;
        if (entry.is_delete) return null;
        return if (entry.external_value_root_page != 0) try self.readValuePagesAtCheckpointAlloc(allocator, entry.external_value_root_page, entry.external_value_len, checkpoint) else try allocator.dupe(u8, entry.value);
    }

    pub fn getDocumentAlloc(self: *NativeFile, allocator: Allocator, key: []const u8) !?[]u8 {
        const checkpoint = self.activeCheckpoint();
        return try self.getDocumentAtCheckpointAlloc(allocator, checkpoint, key);
    }

    /// Resolves a key against the ordered index root pinned by `checkpoint`.
    /// Referenced document pages are reclaimed only by vacuum.
    pub fn getDocumentAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot, key: []const u8) !?[]u8 {
        const page_id = (try self.lookupDocumentIndexPage(checkpoint, key)) orelse return null;
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(allocator, page_id, .document, checkpoint);
        defer allocator.free(payload);
        const entry = try decodeDocumentEntry(payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidDocumentIndex;
        if (entry.is_delete) return null;
        return if (entry.external_value_root_page != 0) try self.readValuePagesAtCheckpointAlloc(allocator, entry.external_value_root_page, entry.external_value_len, checkpoint) else try allocator.dupe(u8, entry.value);
    }

    /// Sorted multi-read: decode each visited index node once, and descend
    /// only into children containing requested keys. Each returned value is
    /// independently owned; errors release and clear every output.
    pub fn getDocumentsAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        for (keys, 0..) |key, i| {
            if (i > 0 and std.mem.order(u8, keys[i - 1], key) == .gt) return error.InvalidBatch;
        }
        errdefer {
            for (values) |value| if (value) |bytes| allocator.free(bytes);
            @memset(values, null);
        }
        if (keys.len == 0 or checkpoint.document_index_root_page == 0) return;
        const references = try allocator.alloc(u64, keys.len);
        defer allocator.free(references);
        @memset(references, 0);
        try self.readDocumentBatchNode(checkpoint, checkpoint.document_index_root_page, keys, references, 0);
        const order = try allocator.alloc(usize, keys.len);
        defer allocator.free(order);
        for (order, 0..) |*position, i| position.* = i;
        std.mem.sort(usize, order, references, struct {
            fn less(refs: []const u64, lhs: usize, rhs: usize) bool {
                const a = physicalPage(refs[lhs]);
                const b = physicalPage(refs[rhs]);
                return a < b or (a == b and refs[lhs] < refs[rhs]);
            }
        }.less);
        var reader = RecordPageReader{};
        defer reader.deinit(allocator);
        for (order) |i| {
            const reference = references[i];
            if (reference == 0) continue;
            const bytes = try reader.read(self, allocator, checkpoint, reference, .document);
            const entry = try decodeDocumentEntry(bytes);
            if (!std.mem.eql(u8, entry.key, keys[i])) return error.InvalidDocumentIndex;
            if (!entry.is_delete) values[i] = if (entry.external_value_root_page != 0)
                try self.readValuePagesAtCheckpointAlloc(allocator, entry.external_value_root_page, entry.external_value_len, checkpoint)
            else
                try allocator.dupe(u8, entry.value);
        }
    }

    fn readDocumentBatchNode(self: *NativeFile, checkpoint: CheckpointSlot, page: u64, keys: []const []const u8, references: []u64, depth: usize) !void {
        if (depth > 64) return error.InvalidDocumentIndex;
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, page, .document_index, checkpoint);
        defer self.allocator.free(payload);
        var node = try decodeDocumentIndexNode(self.allocator, payload);
        defer node.deinit(self.allocator);
        var group_start: usize = 0;
        var group_child: usize = 0;
        for (keys, 0..) |key, i| {
            var low: usize = 0;
            var high = node.keys.len;
            while (low < high) {
                const mid = low + (high - low) / 2;
                const order = std.mem.order(u8, try self.resolveIndexKey(&node, mid, checkpoint), key);
                if (order == .lt or (node.kind == .internal and order == .eq)) low = mid + 1 else high = mid;
            }
            if (node.kind == .leaf) {
                if (low < node.keys.len and std.mem.eql(u8, try self.resolveIndexKey(&node, low, checkpoint), key)) references[i] = node.pointers[low];
            } else {
                if (i > 0 and low != group_child) {
                    try self.readDocumentBatchNode(checkpoint, node.pointers[group_child], keys[group_start..i], references[group_start..i], depth + 1);
                    group_start = i;
                }
                group_child = low;
            }
        }
        if (node.kind == .internal) try self.readDocumentBatchNode(checkpoint, node.pointers[group_child], keys[group_start..], references[group_start..], depth + 1);
    }

    pub fn snapshotDocumentsAlloc(self: *NativeFile, allocator: Allocator) ![]OwnedDocument {
        return try self.snapshotDocumentsWithPrefixAlloc(allocator, "");
    }

    /// Materializes only live documents in `prefix`. Current files use the
    /// persisted namespace directory and per-namespace page links, making the
    /// walk proportional to that namespace's history.
    pub fn snapshotDocumentsWithPrefixAlloc(self: *NativeFile, allocator: Allocator, prefix: []const u8) ![]OwnedDocument {
        return try self.snapshotDocumentsWithPrefixAtCheckpointAlloc(allocator, prefix, self.activeCheckpoint());
    }

    fn snapshotDocumentsWithPrefixAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, prefix: []const u8, checkpoint: CheckpointSlot) ![]OwnedDocument {
        if (prefix.len == 0) return try self.snapshotDocumentsFromChainAlloc(allocator, prefix, checkpoint.document_root_page, false, checkpoint);
        var directory = (try self.loadNamespaceDirectoryAtCheckpointAlloc(allocator, checkpoint)) orelse {
            if (checkpoint.document_root_page == 0) return try allocator.alloc(OwnedDocument, 0);
            return error.InvalidNamespaceDirectory;
        };
        defer NativeFile.deinitNamespaceDirectory(allocator, &directory);
        const head = directory.get(prefix) orelse return try allocator.alloc(OwnedDocument, 0);
        return try self.snapshotDocumentsFromChainAlloc(allocator, prefix, head, true, checkpoint);
    }

    fn snapshotDocumentsFromChainAlloc(self: *NativeFile, allocator: Allocator, prefix: []const u8, root_page: u64, namespace_chain: bool, checkpoint: CheckpointSlot) ![]OwnedDocument {
        var docs = std.ArrayListUnmanaged(OwnedDocument).empty;
        errdefer {
            for (docs.items) |doc| {
                allocator.free(doc.key);
                allocator.free(doc.value);
            }
            docs.deinit(allocator);
        }

        // Keys already resolved while walking newest-to-oldest. Live keys are
        // owned by `docs`, tombstone keys by `tombstone_keys`; the set itself
        // borrows both, so entries are reserved before ownership transfers.
        var seen = std.StringHashMapUnmanaged(void).empty;
        defer seen.deinit(allocator);
        var tombstone_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (tombstone_keys.items) |key| allocator.free(key);
            tombstone_keys.deinit(allocator);
        }

        var page_id = root_page;
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(allocator, page_id, .document, checkpoint);
            defer allocator.free(payload);
            const entry = try decodeDocumentEntry(payload);

            if (!std.mem.startsWith(u8, entry.key, prefix)) {
                if (namespace_chain) return error.InvalidNamespaceDirectory;
                page_id = entry.previous_page;
                continue;
            }

            if (!seen.contains(entry.key)) {
                try seen.ensureUnusedCapacity(allocator, 1);
                if (entry.is_delete) {
                    try tombstone_keys.ensureUnusedCapacity(allocator, 1);
                    const owned_key = try allocator.dupe(u8, entry.key);
                    tombstone_keys.appendAssumeCapacity(owned_key);
                    seen.putAssumeCapacity(owned_key, {});
                } else {
                    try docs.ensureUnusedCapacity(allocator, 1);
                    const owned_key = try allocator.dupe(u8, entry.key);
                    errdefer allocator.free(owned_key);
                    const owned_value = try self.documentEntryValueAlloc(allocator, entry);
                    docs.appendAssumeCapacity(.{ .key = owned_key, .value = owned_value });
                    seen.putAssumeCapacity(owned_key, {});
                }
            }
            page_id = if (namespace_chain) entry.previous_namespace_page else entry.previous_page;
        }

        std.mem.sort(OwnedDocument, docs.items, {}, struct {
            fn lessThan(_: void, lhs: OwnedDocument, rhs: OwnedDocument) bool {
                return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            }
        }.lessThan);

        return try docs.toOwnedSlice(allocator);
    }

    pub fn freeSnapshotDocuments(allocator: Allocator, docs: []OwnedDocument) void {
        for (docs) |doc| {
            allocator.free(doc.key);
            allocator.free(doc.value);
        }
        allocator.free(docs);
    }

    /// Sequential, checksum-checked reader for both extent trees and document
    /// value chains. Holds one page and one extent node per tree level.
    const ValueChunkCursor = struct {
        const Frame = struct { node: ExtentNode, position: usize = 0 };
        file: *NativeFile,
        checkpoint: CheckpointSlot,
        frames: std.ArrayListUnmanaged(Frame) = .empty,
        pending: ?ExtentRef,
        chain_page: u64,
        remaining: usize,
        scratch: [65536]u8 = undefined,

        fn init(file: *NativeFile, root: u64, len: usize) !ValueChunkCursor {
            if (len == 0) return error.InvalidNativeValueChain;
            const checkpoint = file.activeCheckpoint();
            const tree = try file.valueTreeRoot(root, len, checkpoint);
            return .{ .file = file, .checkpoint = checkpoint, .pending = tree, .chain_page = if (tree == null) root else 0, .remaining = len };
        }

        fn deinit(self: *ValueChunkCursor) void {
            self.frames.deinit(self.file.allocator);
        }

        /// The returned slice is borrowed until next(). Cancellation is checked
        /// on every page, including internal nodes of large extent trees.
        fn next(self: *ValueChunkCursor, cancel: ?*const maintenance.CancelToken) !?[]const u8 {
            while (true) {
                if (cancel) |token| try token.check();
                if (self.chain_page != 0) {
                    const raw = try self.file.readPageInto(self.chain_page, self.checkpoint, &self.scratch);
                    const value = try decodeValuePage(try decodePagePayload(raw, .value));
                    if (value.chunk.len == 0 or value.chunk.len > self.remaining) return error.InvalidNativeValueChain;
                    self.remaining -= value.chunk.len;
                    if ((self.remaining == 0) != (value.next_page == 0)) return error.InvalidNativeValueChain;
                    self.chain_page = value.next_page;
                    return value.chunk;
                }
                const ref = self.pending orelse blk: {
                    while (self.frames.items.len > 0) {
                        const frame = &self.frames.items[self.frames.items.len - 1];
                        if (frame.position == frame.node.count) {
                            _ = self.frames.pop();
                            continue;
                        }
                        const child = frame.node.children[frame.position];
                        frame.position += 1;
                        break :blk child;
                    }
                    if (self.remaining != 0) return error.InvalidNativeValueChain;
                    return null;
                };
                self.pending = null;
                const raw = try self.file.readPageInto(ref.page, self.checkpoint, &self.scratch);
                if (ref.height != 0) {
                    const node = try decodeExtentNode(try decodePagePayload(raw, .value_extent), ref.len);
                    if (node.height != ref.height) return error.InvalidNativeValueChain;
                    try self.frames.append(self.file.allocator, .{ .node = node });
                    continue;
                }
                const value = try decodeValuePage(try decodePagePayload(raw, .value));
                if (value.next_page != 0 or value.chunk.len == 0 or value.chunk.len != ref.len or value.chunk.len > self.remaining)
                    return error.InvalidNativeValueChain;
                self.remaining -= value.chunk.len;
                return value.chunk;
            }
        }
    };

    /// Repack arbitrary source chunk boundaries into full output pages. Catalog
    /// values use the bounded extent frontier; documents use contiguous chains.
    fn copyExternalValue(self: *NativeFile, file: std.Io.File, next_page: *u64, root: u64, len: usize, tree: bool, cancel: ?*const maintenance.CancelToken) !u64 {
        var source = try ValueChunkCursor.init(self, root, len);
        defer source.deinit();
        var writer = NativeFile{
            .allocator = self.allocator,
            .io_impl = undefined,
            .borrowed_io = self.runtimeIo(),
            .path = @constCast(""),
            .file = file,
            .header = .{ .page_size = self.header.page_size },
            .no_sync = true,
            .page_cache_enabled = .init(false),
        };
        var pages = PageAllocator{ .file = &writer, .free_pages = &.{}, .next_page_id = next_page.* };
        var builder = ExtentAppender{ .file = &writer, .pages = &pages, .tail = undefined, .batch = .{ .file = &writer } };
        defer builder.deinit();
        const chain_root = next_page.*;
        var buffer: [65536]u8 = undefined;
        const chunk_size = self.maxValuePagePayloadBytes();
        var buffered: usize = 0;
        var written: usize = 0;
        while (try source.next(cancel)) |chunk| {
            var offset: usize = 0;
            while (offset < chunk.len) {
                const n = @min(chunk_size - buffered, chunk.len - offset);
                @memcpy(buffer[value_page_header_size + buffered ..][0..n], chunk[offset..][0..n]);
                buffered += n;
                offset += n;
                if (buffered == chunk_size or written + buffered == len) {
                    if (cancel) |token| try token.check();
                    if (tree) {
                        try builder.pushValue(buffer[value_page_header_size..][0..buffered]);
                    } else {
                        const page = try pages.allocate();
                        const next = if (written + buffered < len) page + 1 else 0;
                        try builder.batch.appendValue(page, next, buffer[value_page_header_size..][0..buffered]);
                    }
                    written += buffered;
                    buffered = 0;
                }
            }
        }
        if (buffered != 0 or written != len) return error.InvalidNativeValueChain;
        if (!tree) {
            try builder.batch.flush();
            next_page.* = pages.next_page_id;
            return chain_root;
        }
        const result = try builder.finish();
        next_page.* = pages.next_page_id;
        return result.page;
    }

    const VacuumValue = struct {
        inline_value: []const u8,
        owned: ?[]u8 = null,
        root: u64 = 0,
        len: usize,

        fn deinit(self: VacuumValue, allocator: Allocator) void {
            if (self.owned) |value| allocator.free(value);
        }
    };

    fn copyVacuumValue(self: *NativeFile, file: std.Io.File, next_page: *u64, entry: CatalogEntry, tree: bool, cancel: ?*const maintenance.CancelToken) !VacuumValue {
        const len = if (entry.external_value_root_page == 0) entry.value.len else entry.external_value_len;
        if (entry.external_value_root_page == 0) return .{ .inline_value = entry.value, .len = len };
        const header_len: usize = if (tree) 16 else 28;
        if (header_len + entry.key.len + len <= self.maxPagePayloadBytes()) {
            // Re-inlining an external value allocates at most one page.
            const value = try self.catalogEntryValueAlloc(self.allocator, entry);
            return .{ .inline_value = value, .owned = value, .len = len };
        }
        const root = try self.copyExternalValue(file, next_page, entry.external_value_root_page, len, tree, cancel);
        return .{ .inline_value = "", .root = root, .len = len };
    }

    fn copyVacuumCatalogRecords(
        self: *NativeFile,
        compact_file: std.Io.File,
        root: CatalogRoot,
        next_page_id: *u64,
        destination_root_page: *u64,
        live_bytes: *u64,
        cancel: ?*const maintenance.CancelToken,
    ) !usize {
        var cursor = try LiveRecordCursor.init(self, .{ .catalog = root });
        defer cursor.deinit();

        const io = self.runtimeIo();
        var writer = NativeFile{ .allocator = self.allocator, .io_impl = undefined, .borrowed_io = io, .path = @constCast(""), .file = compact_file, .header = self.header, .page_cache_enabled = .init(false) };
        var pages = PageAllocator{ .file = &writer, .free_pages = &.{}, .next_page_id = next_page_id.*, .pack_records = true };
        defer pages.deinit();
        var key_index = DocumentIndexBulkBuilder{ .owner = self, .file = compact_file, .next_page_id = &pages.next_page_id };
        defer key_index.deinit();
        var count: usize = 0;
        while (try cursor.next(cancel)) |record| {
            const value = try self.copyVacuumValue(compact_file, &pages.next_page_id, record, true, cancel);
            defer value.deinit(self.allocator);
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(self.allocator);
            try encodeCatalogEntry(self.allocator, &payload, .{
                .previous_page = destination_root_page.*,
                .key = record.key,
                .value = value.inline_value,
                .external_value_root_page = value.root,
                .external_value_len = value.len,
            });
            destination_root_page.* = try pages.writeRecord(.catalog, payload.items);
            try key_index.add(record.key, destination_root_page.*);
            live_bytes.* +|= record.key.len + value.len;
            count += 1;
        }
        const index_root = try key_index.finish();
        if (count > 0) {
            destination_root_page.* = try self.writeCatalogRoot(&pages, destination_root_page.*, index_root);
        }
        try pages.flush();
        next_page_id.* = pages.next_page_id;
        return count;
    }

    fn copyVacuumDocumentRecords(
        self: *NativeFile,
        compact_file: std.Io.File,
        next_page_id: *u64,
        document_root_page: *u64,
        document_index_root_page: *u64,
        namespace_directory: *NamespaceDirectory,
        live_bytes: *u64,
        cancel: ?*const maintenance.CancelToken,
    ) !usize {
        var cursor = try LiveRecordCursor.init(self, .documents);
        defer cursor.deinit();

        const io = self.runtimeIo();
        var writer = NativeFile{ .allocator = self.allocator, .io_impl = undefined, .borrowed_io = io, .path = @constCast(""), .file = compact_file, .header = self.header, .page_cache_enabled = .init(false) };
        var pages = PageAllocator{ .file = &writer, .free_pages = &.{}, .next_page_id = next_page_id.*, .pack_records = true };
        defer pages.deinit();
        var document_index = DocumentIndexBulkBuilder{
            .owner = self,
            .file = compact_file,
            .next_page_id = &pages.next_page_id,
        };
        defer document_index.deinit();
        var count: usize = 0;
        while (try cursor.next(cancel)) |record| {
            const value = try self.copyVacuumValue(compact_file, &pages.next_page_id, record, false, cancel);
            defer value.deinit(self.allocator);
            const namespace = documentNamespace(record.key);
            const previous_namespace_page = namespace_directory.get(namespace) orelse 0;
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(self.allocator);
            try encodeDocumentEntry(self.allocator, &payload, .{
                .previous_page = document_root_page.*,
                .previous_namespace_page = previous_namespace_page,
                .key = record.key,
                .value = value.inline_value,
                .external_value_root_page = value.root,
                .external_value_len = value.len,
            });
            document_root_page.* = try pages.writeRecord(.document, payload.items);
            try document_index.add(record.key, document_root_page.*);
            if (namespace_directory.getPtr(namespace)) |head| {
                head.* = document_root_page.*;
            } else {
                const owned_namespace = try self.allocator.dupe(u8, namespace);
                namespace_directory.put(self.allocator, owned_namespace, document_root_page.*) catch |err| {
                    self.allocator.free(owned_namespace);
                    return err;
                };
            }
            live_bytes.* +|= record.key.len + value.len;
            count += 1;
        }
        document_index_root_page.* = try document_index.finish();
        try pages.flush();
        next_page_id.* = pages.next_page_id;
        return count;
    }

    pub fn vacuum(self: *NativeFile) !VacuumReport {
        return try self.vacuumWithCancel(null);
    }

    pub fn vacuumWithCancel(self: *NativeFile, cancel: ?*const maintenance.CancelToken) !VacuumReport {
        if (self.read_only) return error.ReadOnly;
        var data_lock = try acquireDataRewriteLock(self.runtimeIo(), self.path);
        defer data_lock.file.close(self.runtimeIo());
        var image = try self.prepareVacuum(cancel);
        defer image.deinit();
        try self.publishVacuum(&image);
        return image.report;
    }

    pub fn publishVacuum(self: *NativeFile, image: *VacuumImage) !void {
        const outcome = try self.replaceWithPreparedGeneration(&image.prepared);
        if (self.test_fail_vacuum_after_adoption) {
            self.test_fail_vacuum_after_adoption = false;
            return error.InjectedVacuumPostRenameFailure;
        }
        if (outcome == .durability_unknown) return error.OutcomeUnknown;
    }

    pub fn preparePublicationSequence(self: *NativeFile, sequence: u64) !void {
        var checkpoint = self.activeCheckpoint();
        checkpoint.commit_sequence = sequence;
        // Catch-up can replay keys from failed/no-op foreground mutations, so
        // its private sequence may exceed the live store's publication sequence.
        // Both recovery slots must identify the final image; a newer private
        // fallback slot must never win checkpoint selection after reopen.
        self.header.checkpoints = .{ checkpoint, checkpoint };
        self.header.active_checkpoint = 0;
        var encoded: [header_size]u8 = undefined;
        encodeHeader(&encoded, self.header);
        try self.file.writePositionalAll(self.runtimeIo(), &encoded, 0);
        try self.syncIfRequired();
    }

    // Bound editor state and retained inline values independently. External
    // values are streamed directly into the unpublished image, never retained.
    const catchup_batch_keys = 1024;
    const catchup_batch_bytes = 1024 * 1024;

    pub fn applyCapturedChanges(self: *NativeFile, destination: *NativeFile, capture: *const ChangeCapture, report: *VacuumReport, cancel: ?*const maintenance.CancelToken) !void {
        if (capture.overflow) return error.FileBusy;
        var updated_report = report.*;
        try destination.beginTransaction();
        errdefer destination.abortTransaction();
        const checkpoint = self.activeCheckpoint();
        inline for (0..3) |root| {
            const Mutation = if (root == 2) DocumentMutation else CatalogMutation;
            const map = capture.keys[root];
            const keys = try self.allocator.alloc([]const u8, map.count());
            defer self.allocator.free(keys);
            var iter = map.keyIterator();
            for (keys) |*key| key.* = iter.next().?.*;
            std.mem.sort([]const u8, keys, {}, struct {
                fn less(_: void, lhs: []const u8, rhs: []const u8) bool {
                    return std.mem.order(u8, lhs, rhs) == .lt;
                }
            }.less);
            var reader = RecordPageReader{};
            defer reader.deinit(self.allocator);
            var position: usize = 0;
            while (position < keys.len) {
                var arena = std.heap.ArenaAllocator.init(self.allocator);
                defer arena.deinit();
                const alloc = arena.allocator();
                var mutations = std.ArrayListUnmanaged(Mutation).empty;
                var retained_bytes: usize = 0;
                while (position < keys.len and mutations.items.len < catchup_batch_keys and retained_bytes < catchup_batch_bytes) : (position += 1) {
                    if (cancel) |token| try token.check();
                    const key = keys[position];
                    const page = if (root == 2) try self.lookupDocumentIndexPage(checkpoint, key) else try self.lookupCatalogPage(checkpoint, if (root == 0) .metadata else .index, key);
                    const old_size = try destination.liveRecordSize(root, key);
                    var record: ?CatalogEntry = null;
                    if (page) |id| {
                        const payload = try reader.read(self, self.allocator, checkpoint, id, if (root == 2) .document else .catalog);
                        if (root == 2) {
                            const doc = try decodeDocumentEntry(payload);
                            if (!doc.is_delete) record = .{ .previous_page = 0, .key = doc.key, .value = doc.value, .external_value_root_page = doc.external_value_root_page, .external_value_len = doc.external_value_len };
                        } else {
                            const entry = try decodeCatalogEntry(payload);
                            if (!entry.is_delete) record = entry;
                        }
                    }
                    const copied: VacuumValue = if (record) |entry| blk: {
                        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNativePageChain;
                        const value = try self.copyVacuumValue(destination.file, &destination.header.checkpoints[destination.header.active_checkpoint].page_count, entry, root != 2, cancel);
                        // The old free map cannot cover newly streamed pages.
                        if (value.root != 0) destination.header.checkpoints[destination.header.active_checkpoint].free_map_root_page = 0;
                        break :blk value;
                    } else .{ .inline_value = "", .len = 0 };
                    defer copied.deinit(self.allocator);
                    if (old_size) |len| {
                        updated_report.live_file_count -= 1;
                        updated_report.live_bytes -= len;
                    }
                    if (record != null) {
                        updated_report.live_file_count += 1;
                        updated_report.live_bytes += key.len + copied.len;
                    }
                    const value = try alloc.dupe(u8, copied.inline_value);
                    retained_bytes += key.len + value.len;
                    try mutations.append(alloc, .{ .key = key, .value = value, .is_delete = record == null, .external_value_root_page = copied.root, .external_value_len = if (copied.root != 0) copied.len else 0 });
                }
                if (root == 2) {
                    try destination.putDocumentBatch(mutations.items);
                } else {
                    try destination.putCatalogBatchForRoot(if (root == 0) .metadata else .index, mutations.items, .{ .payload_cache = .cold_sequential });
                }
            }
        }
        if (cancel) |token| try token.check();
        try destination.commitTransaction();
        updated_report.after_size = destination.activeCheckpoint().page_count * @as(u64, destination.header.page_size);
        updated_report.reclaimed_bytes = updated_report.before_size -| updated_report.after_size;
        report.* = updated_report;
    }

    fn liveRecordSize(self: *NativeFile, root: usize, key: []const u8) !?usize {
        const checkpoint = self.activeCheckpoint();
        const page = (if (root == 2) try self.lookupDocumentIndexPage(checkpoint, key) else try self.lookupCatalogPage(checkpoint, if (root == 0) .metadata else .index, key)) orelse return null;
        const raw = try self.readPageAllocForCheckpoint(self.allocator, page, checkpoint);
        defer self.allocator.free(raw);
        if (root == 2) {
            const entry = try decodeDocumentEntry(try decodePagePayload(raw, .document));
            return if (entry.is_delete) null else entry.key.len + (if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len);
        }
        const entry = try decodeCatalogEntry(try decodePagePayload(raw, .catalog));
        return if (entry.is_delete) null else entry.key.len + (if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len);
    }

    pub fn prepareVacuum(self: *NativeFile, cancel: ?*const maintenance.CancelToken) !VacuumImage {
        if (cancel) |token| try token.check();

        const io = self.runtimeIo();

        const before_size = (try self.file.stat(io)).size;
        const previous = self.activeCheckpoint();

        const page_size: usize = @intCast(self.header.page_size);
        var random: [16]u8 = undefined;
        try io.randomSecure(&random);
        const basename = try std.fmt.allocPrint(self.allocator, ".aflite-compact-{x}", .{random});
        defer self.allocator.free(basename);
        const tmp_path = try std.fs.path.join(self.allocator, &.{ std.fs.path.dirname(self.path) orelse ".", basename });
        errdefer self.allocator.free(tmp_path);
        errdefer deleteFilePath(io, tmp_path) catch {};
        var compact_file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .read = true, .exclusive = true, .permissions = .fromMode(0o600) });
        var compact_file_open = true;
        defer if (compact_file_open) compact_file.close(io);
        try compact_file.setLength(io, page_size);

        var next_page_id: u64 = 1;
        var catalog_root_page: u64 = 0;
        var index_catalog_root_page: u64 = 0;
        var document_root_page: u64 = 0;
        var document_index_root_page: u64 = 0;
        var namespace_directory_root_page: u64 = 0;
        var free_map_root_page: u64 = 0;
        var live_bytes: u64 = 0;
        var live_record_count: usize = 0;
        var namespace_directory = NamespaceDirectory.empty;
        defer deinitNamespaceDirectory(self.allocator, &namespace_directory);

        live_record_count += try self.copyVacuumCatalogRecords(compact_file, .metadata, &next_page_id, &catalog_root_page, &live_bytes, cancel);
        live_record_count += try self.copyVacuumCatalogRecords(compact_file, .index, &next_page_id, &index_catalog_root_page, &live_bytes, cancel);
        const document_count = try self.copyVacuumDocumentRecords(compact_file, &next_page_id, &document_root_page, &document_index_root_page, &namespace_directory, &live_bytes, cancel);
        live_record_count += document_count;

        if (document_count > 0) {
            const encoded_directory = try encodeNamespaceDirectoryAlloc(self.allocator, .snapshot, &namespace_directory);
            defer self.allocator.free(encoded_directory);
            const directory_external_root = if (self.catalogEntryFitsInline(namespace_directory_key, encoded_directory))
                0
            else
                try appendValuePagesToFile(self.allocator, compact_file, io, page_size, self.maxValuePagePayloadBytes(), &next_page_id, encoded_directory);
            var directory_payload = std.ArrayListUnmanaged(u8).empty;
            defer directory_payload.deinit(self.allocator);
            try encodeCatalogEntry(self.allocator, &directory_payload, .{
                .previous_page = 0,
                .key = namespace_directory_key,
                .value = encoded_directory,
                .external_value_root_page = directory_external_root,
            });
            namespace_directory_root_page = try appendPageToFile(self.allocator, compact_file, io, page_size, &next_page_id, .catalog, directory_payload.items);
        }

        free_map_root_page = try appendFreeMapPageToFile(self.allocator, compact_file, io, page_size, &next_page_id, next_page_id + 1, &.{});

        const checkpoint = CheckpointSlot{
            .commit_sequence = previous.commit_sequence + 1,
            .catalog_root_page = catalog_root_page,
            .document_root_page = document_root_page,
            .index_catalog_root_page = index_catalog_root_page,
            .free_map_root_page = free_map_root_page,
            .page_count = next_page_id,
            .namespace_directory_root_page = namespace_directory_root_page,
            .document_index_root_page = document_index_root_page,
        };
        const compact_header = Header{
            .page_size = self.header.page_size,
            .active_checkpoint = 0,
            .checkpoints = .{ checkpoint, .{} },
        };

        var encoded_header: [header_size]u8 = undefined;
        encodeHeader(&encoded_header, compact_header);
        try compact_file.writePositionalAll(io, &encoded_header, 0);
        const after_size = next_page_id * @as(u64, self.header.page_size);
        try compact_file.setLength(io, after_size);
        if (!self.no_sync) try compact_file.sync(io);
        if (cancel) |token| try token.check();
        compact_file_open = false;
        return .{ .prepared = .{ .allocator = self.allocator, .io_impl = undefined, .borrowed_io = io, .path = tmp_path, .file = compact_file, .header = compact_header, .no_sync = self.no_sync, .page_cache_policy = .metadata_only, .page_cache = .{ .resource_manager = self.page_cache.resource_manager } }, .report = .{
            .before_size = before_size,
            .after_size = after_size,
            .reclaimed_bytes = if (before_size > after_size) before_size - after_size else 0,
            .live_file_count = @intCast(live_record_count),
            .live_bytes = live_bytes,
        } };
    }

    pub fn copyStableSnapshotToPath(self: *NativeFile, dest_path: []const u8, replace: bool) !StableSnapshotReport {
        const io = self.runtimeIo();
        if (std.mem.eql(u8, self.path, dest_path) or try pathsReferToSameExistingFile(self.allocator, io, self.path, dest_path)) {
            return error.InvalidNativeSnapshotPath;
        }
        const dest_exists = pathExists(io, dest_path);
        if (!replace and dest_exists) return error.PathAlreadyExists;

        var dest_lock = try lockWriterPathWithIo(self.allocator, io, dest_path);
        defer dest_lock.close();

        if (!replace and !dest_exists and pathExists(io, dest_path)) return error.PathAlreadyExists;

        const checkpoint = self.activeCheckpoint();
        const snapshot_size = try checkpointPrefixSize(checkpoint, self.header.page_size);
        const source_size = (try self.file.stat(io)).size;
        if (source_size < snapshot_size) return error.TruncatedNativeSnapshotSource;

        const tmp_path = try std.fmt.allocPrint(self.allocator, "{s}.tmp-aflite-snapshot", .{dest_path});
        defer self.allocator.free(tmp_path);
        errdefer deleteFilePath(io, tmp_path) catch {};

        {
            var out_file = try createSnapshotFile(io, tmp_path);
            defer out_file.close(io);

            const chunk_size: usize = 1024 * 1024;
            const buffer_len: usize = @intCast(@min(@as(u64, chunk_size), snapshot_size));
            const buffer = try self.allocator.alloc(u8, @max(buffer_len, 1));
            defer self.allocator.free(buffer);

            var offset: u64 = 0;
            while (offset < snapshot_size) {
                const len: usize = @intCast(@min(@as(u64, buffer.len), snapshot_size - offset));
                try readExactAt(self.file, io, buffer[0..len], offset);
                try out_file.writePositionalAll(io, buffer[0..len], offset);
                offset += len;
            }
            var snapshot_header: [header_size]u8 = undefined;
            encodeHeader(&snapshot_header, self.header);
            try out_file.writePositionalAll(io, &snapshot_header, 0);
            try out_file.setLength(io, snapshot_size);
            try out_file.sync(io);
        }

        renameFilePath(io, tmp_path, dest_path) catch |err| {
            deleteFilePath(io, tmp_path) catch {};
            return err;
        };
        try fs_paths.syncDirPortable(
            io,
            std.fs.path.dirname(dest_path) orelse ".",
        );

        return .{
            .source_size = source_size,
            .snapshot_size = snapshot_size,
            .checkpoint_sequence = checkpoint.commit_sequence,
            .page_count = checkpoint.page_count,
            .tail_bytes = source_size - snapshot_size,
        };
    }

    pub fn maxPagePayloadBytes(self: *const NativeFile) usize {
        return @as(usize, @intCast(self.header.page_size)) - page_header_size;
    }

    pub fn maxValuePagePayloadBytes(self: *const NativeFile) usize {
        return self.maxPagePayloadBytes() - value_page_header_size;
    }

    fn validateCatalogMutation(self: *const NativeFile, mutation: CatalogMutation) !void {
        if (mutation.key.len > catalog_key_len_mask or mutation.value.len > std.math.maxInt(u32)) return error.RecordTooLarge;
        const fixed_len = 16 + mutation.key.len;
        if (fixed_len > self.maxPagePayloadBytes()) return error.PageTooLarge;
        if (mutation.external_value_root_page != 0) {
            if (self.transaction_header == null or mutation.is_delete or mutation.value.len != 0 or mutation.external_value_len == 0 or mutation.external_value_len > std.math.maxInt(u32) or mutation.external_value_root_page >= self.activeCheckpoint().page_count) return error.InvalidNativeValueChain;
            if (fixed_len + 8 > self.maxPagePayloadBytes()) return error.PageTooLarge;
            return;
        }
        if (mutation.is_delete) return;
        if (mutation.value.len <= self.maxPagePayloadBytes() - fixed_len) return;
        if (value_page_header_size > self.maxPagePayloadBytes()) return error.InvalidNativePageLength;
        if (fixed_len + 8 > self.maxPagePayloadBytes()) return error.PageTooLarge;
    }

    fn catalogEntryFitsInline(self: *const NativeFile, key: []const u8, value: []const u8) bool {
        const fixed_len = 16 + key.len;
        return fixed_len <= self.maxPagePayloadBytes() and value.len <= self.maxPagePayloadBytes() - fixed_len;
    }

    fn catalogEntryValueAlloc(self: *NativeFile, allocator: Allocator, entry: CatalogEntry) ![]u8 {
        return try self.catalogEntryValueAtCheckpointAlloc(allocator, entry, self.activeCheckpoint());
    }

    fn catalogEntryValueAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, entry: CatalogEntry, checkpoint: CheckpointSlot) ![]u8 {
        if (entry.external_value_root_page != 0) {
            return try self.readValuePagesAtCheckpointAlloc(allocator, entry.external_value_root_page, entry.external_value_len, checkpoint);
        }
        return try allocator.dupe(u8, entry.value);
    }

    fn catalogEntryRangeAlloc(self: *NativeFile, allocator: Allocator, entry: CatalogEntry, offset: u64, len: usize, checkpoint: CheckpointSlot) ![]u8 {
        const value_len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
        if (offset > std.math.maxInt(usize)) return error.EndOfStream;
        const start: usize = @intCast(offset);
        if (start > value_len or value_len - start < len) return error.EndOfStream;
        if (entry.external_value_root_page != 0) {
            return try self.readValuePagesRangeAlloc(allocator, entry.external_value_root_page, value_len, start, len, checkpoint);
        }
        return try allocator.dupe(u8, entry.value[start..][0..len]);
    }

    fn validateDocumentMutation(self: *const NativeFile, mutation: DocumentMutation) !void {
        if (mutation.key.len > std.math.maxInt(u32) or mutation.value.len > std.math.maxInt(u32)) return error.RecordTooLarge;
        const fixed_len = 28 + mutation.key.len;
        if (fixed_len > self.maxPagePayloadBytes()) return error.PageTooLarge;
        if (mutation.external_value_root_page != 0) {
            if (self.transaction_header == null or mutation.is_delete or mutation.value.len != 0 or mutation.external_value_len == 0 or mutation.external_value_len > std.math.maxInt(u32) or mutation.external_value_root_page >= self.activeCheckpoint().page_count) return error.InvalidNativeValueChain;
            if (fixed_len + 8 > self.maxPagePayloadBytes()) return error.PageTooLarge;
            return;
        }
        if (mutation.is_delete) return;
        if (mutation.value.len <= self.maxPagePayloadBytes() - fixed_len) return;
        if (value_page_header_size > self.maxPagePayloadBytes()) return error.InvalidNativePageLength;
        if (fixed_len + 8 > self.maxPagePayloadBytes()) return error.PageTooLarge;
    }

    fn documentEntryFitsInline(self: *const NativeFile, key: []const u8, value: []const u8) bool {
        const fixed_len = 28 + key.len;
        return fixed_len <= self.maxPagePayloadBytes() and value.len <= self.maxPagePayloadBytes() - fixed_len;
    }

    fn documentEntryValueAlloc(self: *NativeFile, allocator: Allocator, entry: DocumentEntry) ![]u8 {
        if (entry.external_value_root_page != 0) {
            return try self.readValuePagesAlloc(allocator, entry.external_value_root_page, entry.external_value_len);
        }
        return try allocator.dupe(u8, entry.value);
    }

    fn readPagePayloadByKindAlloc(self: *NativeFile, allocator: Allocator, page_id: u64, kind: PageKind) ![]u8 {
        return try self.readPagePayloadByKindAllocForCheckpoint(allocator, page_id, kind, self.activeCheckpoint());
    }

    fn readPagePayloadByKindAllocForCheckpoint(
        self: *NativeFile,
        allocator: Allocator,
        page_id: u64,
        kind: PageKind,
        checkpoint: CheckpointSlot,
    ) ![]u8 {
        const page = try self.readPageAllocForCheckpoint(allocator, page_id, checkpoint);
        defer allocator.free(page);
        return try decodePagePayloadAlloc(allocator, page, kind);
    }

    const ReachablePageSet = std.AutoHashMapUnmanaged(u64, void);

    fn markReachablePage(self: *NativeFile, reachable_pages: *ReachablePageSet, page_id: u64, page_count: u64) !void {
        const physical = physicalPage(page_id);
        if (physical == 0 or physical >= page_count) return error.InvalidPageId;
        const entry = try reachable_pages.getOrPut(self.allocator, page_id);
        if (entry.found_existing) return error.InvalidNativePageChain;
        // Multiple records may share a physical page. Keep both identities:
        // record addresses detect cycles; physical addresses protect reuse.
        if (physical != page_id) try reachable_pages.put(self.allocator, physical, {});
    }

    fn countReachableChainPages(self: *NativeFile, kind: PageKind, root_page_id: u64, reachable_pages: *ReachablePageSet) !u64 {
        return try self.countReachableChainPagesWithCancel(kind, root_page_id, reachable_pages, null);
    }

    fn countReachableChainPagesWithCancel(self: *NativeFile, kind: PageKind, root_page_id: u64, reachable_pages: *ReachablePageSet, cancel: ?*const maintenance.CancelToken) !u64 {
        return try self.countReachableChainPagesForCheckpoint(kind, root_page_id, self.activeCheckpoint(), reachable_pages, cancel);
    }

    fn countReachableChainPagesForCheckpoint(
        self: *NativeFile,
        kind: PageKind,
        root_page_id: u64,
        checkpoint: CheckpointSlot,
        reachable_pages: *ReachablePageSet,
        cancel: ?*const maintenance.CancelToken,
    ) !u64 {
        var seen_catalog_keys = std.StringHashMapUnmanaged(void).empty;
        defer {
            var it = seen_catalog_keys.iterator();
            while (it.next()) |entry| self.allocator.free(entry.key_ptr.*);
            seen_catalog_keys.deinit(self.allocator);
        }

        var count: u64 = 0;
        var page_id = root_page_id;
        var catalog_index: u64 = 0;
        var has_catalog_index = false;
        if (kind == .catalog and root_page_id != 0) {
            const roots = try self.readCatalogRoots(root_page_id, checkpoint);
            if (roots.indexed) {
                try self.markReachablePage(reachable_pages, root_page_id, checkpoint.page_count);
                catalog_index = roots.index;
                has_catalog_index = true;
                page_id = roots.history;
            }
        }
        const use_link_cache = !has_catalog_index and self.page_cache_enabled.load(.monotonic) and self.page_cache_bypass.load(.monotonic) == 0;
        while (page_id != 0) {
            if (cancel) |token| try token.check();
            try self.markReachablePage(reachable_pages, page_id, checkpoint.page_count);

            if (use_link_cache) blk: {
                const links = (try self.page_cache.getLinksCopy(self.allocator, page_id)) orelse break :blk;
                defer if (links.key) |key| self.allocator.free(key);
                if (links.kind != kind) return error.UnexpectedNativePageKind;
                switch (kind) {
                    .catalog => {
                        const entry_key = links.key orelse &[_]u8{};
                        const seen = seen_catalog_keys.contains(entry_key);
                        if (!seen) {
                            const owned_key = try self.allocator.dupe(u8, entry_key);
                            errdefer self.allocator.free(owned_key);
                            try seen_catalog_keys.put(self.allocator, owned_key, {});
                        }
                        if (!seen and links.external_value_root_page != 0) {
                            try self.validateReachableValuePages(links.external_value_root_page, links.external_value_len, checkpoint, reachable_pages);
                        }
                    },
                    .document => {
                        if (links.external_value_root_page != 0) {
                            try self.validateReachableValuePages(links.external_value_root_page, links.external_value_len, checkpoint, reachable_pages);
                        }
                    },
                    .data, .value, .free_map, .document_index, .value_extent, .catalog_index, .record_bundle => return error.UnexpectedNativePageKind,
                }
                page_id = links.link_page;
                count += 1;
                if (count > recordWalkLimit(checkpoint, self.header.page_size)) return error.InvalidNativePageChain;
                continue;
            }

            const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, page_id, kind, checkpoint);
            defer self.allocator.free(payload);
            page_id = switch (kind) {
                .catalog => blk: {
                    const entry = try decodeCatalogEntry(payload);
                    const seen = seen_catalog_keys.contains(entry.key);
                    if (!seen) {
                        const owned_key = try self.allocator.dupe(u8, entry.key);
                        errdefer self.allocator.free(owned_key);
                        try seen_catalog_keys.put(self.allocator, owned_key, {});
                    }
                    if (!seen and has_catalog_index) {
                        var indexed = checkpoint;
                        indexed.document_index_root_page = catalog_index;
                        const found = try self.lookupDocumentIndexPage(indexed, entry.key);
                        if (found != page_id and !(entry.is_delete and found == null)) return error.InvalidNativePageChain;
                    }
                    if (!seen and entry.external_value_root_page != 0) {
                        try self.validateReachableValuePages(entry.external_value_root_page, entry.external_value_len, checkpoint, reachable_pages);
                    }
                    if (use_link_cache) self.cachePageLinks(page_id, .catalog, payload);
                    break :blk entry.previous_page;
                },
                .document => blk: {
                    const entry = try decodeDocumentEntry(payload);
                    if (entry.external_value_root_page != 0) {
                        try self.validateReachableValuePages(entry.external_value_root_page, entry.external_value_len, checkpoint, reachable_pages);
                    }
                    if (use_link_cache) self.cachePageLinks(page_id, .document, payload);
                    break :blk entry.previous_page;
                },
                .data, .value, .free_map, .document_index, .value_extent, .catalog_index, .record_bundle => return error.UnexpectedNativePageKind,
            };
            count += 1;
            if (count > recordWalkLimit(checkpoint, self.header.page_size)) return error.InvalidNativePageChain;
        }
        if (catalog_index != 0) {
            var indexed = checkpoint;
            indexed.document_index_root_page = catalog_index;
            _ = try self.collectDocumentIndexPages(indexed, reachable_pages, false, true, cancel);
            try self.validateCatalogIndexEntries(checkpoint, catalog_index, reachable_pages);
        }
        return count;
    }

    const extent_fanout = 64;
    const extent_header_size = 16;
    const extent_magic = "AFEXT003";
    const ExtentRef = struct { page: u64, len: u64, height: u8 = 0 };
    const ExtentNode = struct {
        height: u8,
        count: usize,
        children: [extent_fanout]ExtentRef = undefined,
    };

    fn decodeExtentNode(payload: []const u8, expected_len: u64) !ExtentNode {
        if (payload.len < extent_header_size or !std.mem.eql(u8, payload[0..8], extent_magic)) return error.InvalidNativeValueChain;
        const height = payload[8];
        const count = std.mem.readInt(u16, payload[10..12], .little);
        if (height == 0 or height > 63 or count == 0 or count > extent_fanout or payload.len != extent_header_size + @as(usize, count) * 16)
            return error.InvalidNativeValueChain;
        var node = ExtentNode{ .height = height, .count = count };
        var total: u64 = 0;
        for (node.children[0..count], 0..) |*child, i| {
            const bytes = payload[extent_header_size + i * 16 ..][0..16];
            child.* = .{
                .page = std.mem.readInt(u64, bytes[0..8], .little),
                .len = std.mem.readInt(u64, bytes[8..16], .little),
                .height = height - 1,
            };
            if (child.page == 0 or child.len == 0) return error.InvalidNativeValueChain;
            total = std.math.add(u64, total, child.len) catch return error.InvalidNativeValueChain;
        }
        if (total != expected_len) return error.InvalidNativeValueChain;
        return node;
    }

    fn writeExtentNode(self: *NativeFile, pages: *PageAllocator, batch: *PageWriteBatch, height: u8, children: []const ExtentRef) !ExtentRef {
        _ = self;
        if (height == 0 or height > 63 or children.len == 0 or children.len > extent_fanout) return error.InvalidNativeValueChain;
        var payload: [extent_header_size + extent_fanout * 16]u8 = @splat(0);
        @memcpy(payload[0..8], extent_magic);
        payload[8] = height;
        std.mem.writeInt(u16, payload[10..12], @intCast(children.len), .little);
        var len: u64 = 0;
        for (children, 0..) |child, i| {
            if (child.height + 1 != height or child.len == 0) return error.InvalidNativeValueChain;
            len = try std.math.add(u64, len, child.len);
            const bytes = payload[extent_header_size + i * 16 ..][0..16];
            std.mem.writeInt(u64, bytes[0..8], child.page, .little);
            std.mem.writeInt(u64, bytes[8..16], child.len, .little);
        }
        const page = try pages.allocate();
        try batch.appendPage(page, .value_extent, payload[0 .. extent_header_size + children.len * 16]);
        return .{ .page = page, .len = len, .height = height };
    }

    /// Stream a packed tree through the same bounded frontier used by appends.
    /// Neither initial writes nor vacuum need an array of every leaf reference.
    fn writeValueTree(self: *NativeFile, pages: *PageAllocator, value: []const u8, options: WriteOptions) !ExtentRef {
        if (value.len == 0) return error.InvalidNativeValueChain;
        var builder = ExtentAppender{ .file = self, .pages = pages, .tail = undefined, .batch = .{ .file = self, .options = options } };
        defer builder.deinit();
        var offset: usize = 0;
        while (offset < value.len) {
            const end = @min(value.len, offset + self.maxValuePagePayloadBytes());
            try builder.pushValue(value[offset..end]);
            offset = end;
        }
        return try builder.finish();
    }

    fn writeCatalogValue(self: *NativeFile, pages: *PageAllocator, value: []const u8, options: WriteOptions) !u64 {
        return (try self.writeValueTree(pages, value, options)).page;
    }

    fn valueTreeRoot(self: *NativeFile, root: u64, len: usize, checkpoint: CheckpointSlot) !?ExtentRef {
        const raw = try self.readPageAllocForCheckpoint(self.allocator, root, checkpoint);
        defer self.allocator.free(raw);
        if (raw[4] == @intFromEnum(PageKind.value_extent)) {
            const payload = try decodePagePayloadAlloc(self.allocator, raw, .value_extent);
            defer self.allocator.free(payload);
            const node = try decodeExtentNode(payload, len);
            return .{ .page = root, .len = len, .height = node.height };
        }
        const payload = try decodePagePayloadAlloc(self.allocator, raw, .value);
        defer self.allocator.free(payload);
        const leaf = try decodeValuePage(payload);
        if (leaf.next_page != 0) return null; // document/namespace linked value
        if (leaf.chunk.len != len or len == 0) return error.InvalidNativeValueChain;
        return .{ .page = root, .len = len };
    }

    /// A bounded right frontier: one unfinished node at each height. Sealed
    /// suffix nodes are written once and fed into the next level; existing
    /// full subtrees remain shared. Memory is O(fanout * height), independent
    /// of suffix length, and work is O(new leaves + height).
    const ExtentAppender = struct {
        const Level = struct {
            children: [extent_fanout]ExtentRef = undefined,
            count: usize = 0,
            original: ?ExtentRef = null,
            original_count: usize = 0,
            original_last: ExtentRef = undefined,
            unchanged: bool = true,
        };
        file: *NativeFile,
        pages: *PageAllocator,
        levels: std.ArrayListUnmanaged(Level) = .empty,
        tail: ExtentRef,
        batch: PageWriteBatch,

        fn pushValue(self: *ExtentAppender, value: []const u8) !void {
            const page = try self.pages.allocate();
            try self.batch.appendValue(page, 0, value);
            try self.push(.{ .page = page, .len = value.len });
        }

        fn init(file: *NativeFile, pages: *PageAllocator, root: ExtentRef) !ExtentAppender {
            var result = ExtentAppender{ .file = file, .pages = pages, .tail = root, .batch = .{ .file = file } };
            errdefer result.deinit();
            var checkpoint = file.activeCheckpoint();
            checkpoint.page_count = pages.next_page_id;
            while (result.tail.height > 0) {
                const ref = result.tail;
                const payload = try file.readPagePayloadByKindAllocForCheckpoint(file.allocator, ref.page, .value_extent, checkpoint);
                defer file.allocator.free(payload);
                const node = try decodeExtentNode(payload, ref.len);
                if (node.height != ref.height) return error.InvalidNativeValueChain;
                try result.ensureLevel(ref.height);
                result.levels.items[ref.height] = .{
                    .children = node.children,
                    .count = node.count - 1,
                    .original = ref,
                    .original_count = node.count,
                    .original_last = node.children[node.count - 1],
                };
                result.tail = node.children[node.count - 1];
            }
            return result;
        }

        fn deinit(self: *ExtentAppender) void {
            self.levels.deinit(self.file.allocator);
        }

        fn ensureLevel(self: *ExtentAppender, height: usize) !void {
            if (height >= 64) return error.InvalidNativeValueChain;
            if (height < self.levels.items.len) return;
            const before = self.levels.items.len;
            try self.levels.resize(self.file.allocator, height + 1);
            @memset(self.levels.items[before..], .{});
        }

        fn push(self: *ExtentAppender, child: ExtentRef) anyerror!void {
            if (child.height >= 63) return error.InvalidNativeValueChain;
            const height = child.height + 1;
            try self.ensureLevel(height);
            const level = &self.levels.items[height];
            if (level.original == null or level.count + 1 != level.original_count or
                !std.meta.eql(child, level.original_last)) level.unchanged = false;
            level.children[level.count] = child;
            level.count += 1;
            if (level.count == extent_fanout) {
                const parent = try self.seal(height);
                try self.push(parent);
            }
        }

        fn seal(self: *ExtentAppender, height: u8) !ExtentRef {
            const level = &self.levels.items[height];
            const ref = if (level.unchanged and level.original != null and level.count == level.original_count)
                level.original.?
            else
                try self.file.writeExtentNode(self.pages, &self.batch, height, level.children[0..level.count]);
            level.* = .{};
            return ref;
        }

        fn finish(self: *ExtentAppender) !ExtentRef {
            const root = try self.finishTree();
            // Callers may read newly built trees, then publish their roots.
            // Never return a root while any of its pages remain buffered.
            try self.batch.flush();
            return root;
        }

        fn finishTree(self: *ExtentAppender) !ExtentRef {
            var height: usize = 1;
            while (height < self.levels.items.len) : (height += 1) {
                const level = &self.levels.items[height];
                if (level.count == 0) continue;
                var higher_pending = false;
                for (self.levels.items[height + 1 ..]) |higher| {
                    if (higher.count != 0) higher_pending = true;
                }
                // Do not introduce a unary root above the final subtree.
                if (!higher_pending and level.count == 1) return level.children[0];
                const ref = try self.seal(@intCast(height));
                if (!higher_pending) return ref;
                try self.push(ref);
            }
            return error.InvalidNativeValueChain;
        }
    };

    fn appendCatalogValueTree(self: *NativeFile, pages: *PageAllocator, entry: CatalogEntry, suffix: []const u8) !u64 {
        const root = if (entry.external_value_root_page != 0)
            (try self.valueTreeRoot(entry.external_value_root_page, entry.external_value_len, self.activeCheckpoint())) orelse return error.InvalidNativeValueChain
        else if (entry.value.len > 0)
            try self.writeValueTree(pages, entry.value, .{})
        else
            return (try self.writeValueTree(pages, suffix, .{})).page;
        if (suffix.len == 0) return root.page;
        var appender = try ExtentAppender.init(self, pages, root);
        defer appender.deinit();
        var checkpoint = self.activeCheckpoint();
        checkpoint.page_count = pages.next_page_id;
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, appender.tail.page, .value, checkpoint);
        defer self.allocator.free(payload);
        const tail = try decodeValuePage(payload);
        if (tail.next_page != 0 or tail.chunk.len != appender.tail.len) return error.InvalidNativeValueChain;
        const take = @min(self.maxValuePagePayloadBytes() - tail.chunk.len, suffix.len);
        if (take == 0) {
            try appender.push(appender.tail);
        } else {
            const combined = try self.allocator.alloc(u8, tail.chunk.len + take);
            defer self.allocator.free(combined);
            @memcpy(combined[0..tail.chunk.len], tail.chunk);
            @memcpy(combined[tail.chunk.len..], suffix[0..take]);
            try appender.pushValue(combined);
        }
        var offset = take;
        while (offset < suffix.len) {
            const end = @min(suffix.len, offset + self.maxValuePagePayloadBytes());
            try appender.pushValue(suffix[offset..end]);
            offset = end;
        }
        return (try appender.finish()).page;
    }

    fn readExtentRange(self: *NativeFile, ref: ExtentRef, checkpoint: CheckpointSlot, start: usize, out: []u8) !void {
        if (start > ref.len or out.len > ref.len - start) return error.InvalidNativeValueChain;
        if (ref.height == 0) {
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, ref.page, .value, checkpoint);
            defer self.allocator.free(payload);
            const leaf = try decodeValuePage(payload);
            if (leaf.next_page != 0 or leaf.chunk.len != ref.len) return error.InvalidNativeValueChain;
            @memcpy(out, leaf.chunk[start..][0..out.len]);
            return;
        }
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, ref.page, .value_extent, checkpoint);
        defer self.allocator.free(payload);
        const node = try decodeExtentNode(payload, ref.len);
        if (node.height != ref.height) return error.InvalidNativeValueChain;
        var offset: u64 = 0;
        var written: usize = 0;
        for (node.children[0..node.count]) |child| {
            const end = offset + child.len;
            if (end > start and offset < start + out.len) {
                const from: usize = @intCast(@max(offset, start) - offset);
                const count: usize = @intCast(@min(end, start + out.len) - @max(offset, start));
                try self.readExtentRange(child, checkpoint, from, out[written..][0..count]);
                written += count;
            }
            offset = end;
            if (written == out.len) break;
        }
        if (written != out.len) return error.InvalidNativeValueChain;
    }

    fn validateExtent(self: *NativeFile, ref: ExtentRef, checkpoint: CheckpointSlot, reachable: *ReachablePageSet) !void {
        try self.markReachablePage(reachable, ref.page, checkpoint.page_count);
        if (ref.height == 0) {
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, ref.page, .value, checkpoint);
            defer self.allocator.free(payload);
            const leaf = try decodeValuePage(payload);
            if (leaf.next_page != 0 or leaf.chunk.len != ref.len) return error.InvalidNativeValueChain;
            return;
        }
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, ref.page, .value_extent, checkpoint);
        defer self.allocator.free(payload);
        const node = try decodeExtentNode(payload, ref.len);
        if (node.height != ref.height) return error.InvalidNativeValueChain;
        for (node.children[0..node.count]) |child| try self.validateExtent(child, checkpoint, reachable);
    }

    fn writeValuePagesAllocated(self: *NativeFile, page_allocator: *PageAllocator, value: []const u8) !u64 {
        if (value.len == 0) return error.InvalidNativeValueChain;
        const chunk_size = self.maxValuePagePayloadBytes();
        if (chunk_size == 0) return error.InvalidNativePageLength;
        var batch = PageWriteBatch{ .file = self };
        var page = try page_allocator.allocate();
        const root = page;
        var offset: usize = 0;
        while (offset < value.len) {
            const len = @min(chunk_size, value.len - offset);
            // One-page lookahead preserves arbitrary free-page allocation
            // order without retaining an array of every page in the chain.
            const next = if (offset + len < value.len) try page_allocator.allocate() else 0;
            try batch.appendValue(page, next, value[offset..][0..len]);
            page = next;
            offset += len;
        }
        try batch.flush();
        return root;
    }

    fn writeValuePageChunk(self: *NativeFile, page_ids: []const u64, page_index: usize, chunk: []const u8, options: WriteOptions) !void {
        if (chunk.len == 0 or chunk.len > self.maxValuePagePayloadBytes()) return error.InvalidNativeValueChain;
        const next_page_id = if (page_index + 1 < page_ids.len) page_ids[page_index + 1] else 0;
        var batch = PageWriteBatch{ .file = self, .options = options };
        try batch.appendValue(page_ids[page_index], next_page_id, chunk);
        try batch.flush();
    }

    fn pageAllocatorFromFreeMap(self: *NativeFile, checkpoint: CheckpointSlot) !PageAllocator {
        var free_pages = try self.readFreePagesAlloc(checkpoint);
        errdefer self.allocator.free(free_pages);
        if (!self.free_pages_verified) {
            try self.validateFreePagesSafeForCheckpointSlots(free_pages);
            self.free_pages_verified = true;
        }
        var data_lock_file: ?std.Io.File = null;
        if (free_pages.len > 0) {
            const data_lock = acquireDataRewriteLock(self.runtimeIo(), self.path) catch |err| switch (err) {
                error.WouldBlock => blk: {
                    self.allocator.free(free_pages);
                    free_pages = try self.allocator.alloc(u64, 0);
                    break :blk null;
                },
                else => return err,
            };
            if (data_lock) |lock| {
                data_lock_file = lock.file;
            }
        }
        return .{
            .file = self,
            .free_pages = free_pages,
            .next_page_id = checkpoint.page_count,
            .data_lock_file = data_lock_file,
        };
    }

    fn readFreePagesAlloc(self: *NativeFile, checkpoint: CheckpointSlot) ![]u64 {
        if (checkpoint.free_map_root_page == 0) return try self.allocator.alloc(u64, 0);

        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, checkpoint.free_map_root_page, .free_map, checkpoint);
        defer self.allocator.free(payload);
        const free_map = try decodeFreeMapAlloc(self.allocator, payload, checkpoint.page_count);
        return free_map.free_pages;
    }

    fn writeFreeMapPage(self: *NativeFile, page_id: u64, covered_page_count: u64, free_pages: []const u64) !void {
        const payload = try encodeFreeMapAlloc(self.allocator, self.header.page_size, covered_page_count, free_pages);
        defer self.allocator.free(payload);
        try self.writePage(page_id, .free_map, payload);
    }

    fn computeFreePagesForPublishedCheckpoint(self: *NativeFile, next: CheckpointSlot, previous: CheckpointSlot) ![]u64 {
        var reachable_pages = std.AutoHashMapUnmanaged(u64, void){};
        defer reachable_pages.deinit(self.allocator);

        try self.collectCheckpointReachablePages(next, &reachable_pages);
        if (validCheckpointSlot(previous)) {
            try self.collectCheckpointReachablePages(previous, &reachable_pages);
        }

        var free_pages = std.ArrayListUnmanaged(u64).empty;
        errdefer free_pages.deinit(self.allocator);

        const max_entries = maxFreeMapEntries(self.header.page_size);
        var page_id: u64 = 1;
        while (page_id < next.page_count and free_pages.items.len < max_entries) : (page_id += 1) {
            if (!reachable_pages.contains(page_id)) {
                try free_pages.append(self.allocator, page_id);
            }
        }

        return try free_pages.toOwnedSlice(self.allocator);
    }

    fn collectCheckpointReachablePages(self: *NativeFile, checkpoint: CheckpointSlot, out: *ReachablePageSet) !void {
        var checkpoint_pages = std.AutoHashMapUnmanaged(u64, void){};
        defer checkpoint_pages.deinit(self.allocator);

        _ = try self.countReachableChainPagesForCheckpoint(.catalog, checkpoint.catalog_root_page, checkpoint, &checkpoint_pages, null);
        _ = try self.countReachableChainPagesForCheckpoint(.catalog, checkpoint.index_catalog_root_page, checkpoint, &checkpoint_pages, null);
        _ = try self.countReachableChainPagesForCheckpoint(.catalog, checkpoint.namespace_directory_root_page, checkpoint, &checkpoint_pages, null);
        _ = try self.countReachableChainPagesForCheckpoint(.document, checkpoint.document_root_page, checkpoint, &checkpoint_pages, null);
        _ = try self.collectDocumentIndexPages(checkpoint, &checkpoint_pages, false, true, null);
        if (checkpoint.free_map_root_page != 0) {
            try self.markReachablePage(&checkpoint_pages, checkpoint.free_map_root_page, checkpoint.page_count);
        }

        var it = checkpoint_pages.iterator();
        while (it.next()) |entry| {
            try out.put(self.allocator, entry.key_ptr.*, {});
        }
    }

    fn collectDocumentIndexPages(
        self: *NativeFile,
        checkpoint: CheckpointSlot,
        reachable_pages: *ReachablePageSet,
        validate_documents: bool,
        validate_key_references: bool,
        cancel: ?*const maintenance.CancelToken,
    ) !u64 {
        if (checkpoint.document_index_root_page == 0) return 0;
        return try self.collectDocumentIndexSubtree(
            checkpoint,
            checkpoint.document_index_root_page,
            null,
            null,
            reachable_pages,
            validate_documents,
            validate_key_references,
            cancel,
            0,
        );
    }

    fn collectDocumentIndexSubtree(
        self: *NativeFile,
        checkpoint: CheckpointSlot,
        page_id: u64,
        lower: ?[]const u8,
        upper: ?[]const u8,
        reachable_pages: *ReachablePageSet,
        validate_documents: bool,
        validate_key_references: bool,
        cancel: ?*const maintenance.CancelToken,
        depth: usize,
    ) !u64 {
        if (depth > 64) return error.InvalidDocumentIndex;
        if (cancel) |token| try token.check();
        try self.markReachablePage(reachable_pages, page_id, checkpoint.page_count);
        var node = try self.readDocumentIndexNode(page_id, checkpoint);
        defer node.deinit(self.allocator);
        if (validate_key_references) for (node.key_pages.?) |page| {
            if (page != 0 and !reachable_pages.contains(page)) return error.InvalidDocumentIndex;
        };
        for (node.keys, 0..) |key, i| {
            if (i > 0 and std.mem.order(u8, node.keys[i - 1], key) != .lt) return error.InvalidDocumentIndex;
            if (lower) |bound| if (std.mem.order(u8, key, bound) == .lt) return error.InvalidDocumentIndex;
            if (upper) |bound| if (std.mem.order(u8, key, bound) != .lt) return error.InvalidDocumentIndex;
        }

        switch (node.kind) {
            .leaf => {
                if (validate_documents) {
                    for (node.keys, node.pointers) |key, document_page_id| {
                        if (!reachable_pages.contains(document_page_id)) return error.InvalidDocumentIndex;
                        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, document_page_id, .document, checkpoint);
                        defer self.allocator.free(payload);
                        const entry = try decodeDocumentEntry(payload);
                        if (!std.mem.eql(u8, key, entry.key)) return error.InvalidDocumentIndex;
                    }
                }
                return 1;
            },
            .internal => {
                var count: u64 = 1;
                for (node.pointers, 0..) |child, i| {
                    if (physicalPage(child) == 0 or physicalPage(child) >= checkpoint.page_count) return error.InvalidPageId;
                    count += try self.collectDocumentIndexSubtree(
                        checkpoint,
                        child,
                        if (i == 0) lower else node.keys[i - 1],
                        if (i == node.keys.len) upper else node.keys[i],
                        reachable_pages,
                        validate_documents,
                        validate_key_references,
                        cancel,
                        depth + 1,
                    );
                }
                return count;
            },
        }
    }

    /// Proves that the ordered index contains exactly the newest page for every
    /// key in document history. The unresolved set stores only page IDs, not
    /// keys or values, keeping integrity-check memory proportional to eight
    /// bytes plus hash overhead per live key.
    fn validateDocumentIndexCoverage(self: *NativeFile, checkpoint: CheckpointSlot, cancel: ?*const maintenance.CancelToken) !void {
        var unresolved = std.AutoHashMapUnmanaged(u64, void){};
        defer unresolved.deinit(self.allocator);

        var cursor = DocumentIndexCursor.init(self, checkpoint);
        defer cursor.deinit();
        var indexed = try cursor.first();
        while (indexed) |entry| {
            var owned = entry;
            defer owned.deinit(self.allocator);
            if (cancel) |token| try token.check();
            const inserted = try unresolved.getOrPut(self.allocator, owned.document_page_id);
            if (inserted.found_existing) return error.InvalidDocumentIndex;
            indexed = try cursor.next();
        }

        var page_id = checkpoint.document_root_page;
        var walked: u64 = 0;
        while (page_id != 0) {
            if (cancel) |token| try token.check();
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, page_id, .document, checkpoint);
            defer self.allocator.free(payload);
            const entry = try decodeDocumentEntry(payload);
            const indexed_page = (try self.lookupDocumentIndexPage(checkpoint, entry.key)) orelse return error.InvalidDocumentIndex;
            if (unresolved.contains(indexed_page)) {
                if (indexed_page != page_id) return error.InvalidDocumentIndex;
                _ = unresolved.remove(indexed_page);
            }
            page_id = entry.previous_page;
            walked += 1;
            if (walked > recordWalkLimit(checkpoint, self.header.page_size)) return error.InvalidNativePageChain;
        }
        if (unresolved.count() != 0) return error.InvalidDocumentIndex;
    }

    fn collectAllValidCheckpointReachablePages(self: *NativeFile, out: *ReachablePageSet) !void {
        const file_size = (try self.file.stat(self.runtimeIo())).size;
        for (self.header.checkpoints) |slot| {
            if (!validCheckpointSlot(slot)) continue;
            const expected_size = checkpointPrefixSize(slot, self.header.page_size) catch continue;
            if (expected_size > file_size) continue;
            try self.collectCheckpointReachablePages(slot, out);
        }
    }

    fn validateFreePagesSafeForCheckpointSlots(self: *NativeFile, free_pages: []const u64) !void {
        // Append-only writes have nothing to reclaim. Walking both checkpoint
        // graphs here makes each small catalog write proportional to history.
        // Nonempty free maps still require the full protected-page proof.
        if (free_pages.len == 0) return;
        var protected_pages = std.AutoHashMapUnmanaged(u64, void){};
        defer protected_pages.deinit(self.allocator);

        try self.collectAllValidCheckpointReachablePages(&protected_pages);
        for (free_pages) |page_id| {
            if (protected_pages.contains(page_id)) return error.InvalidNativeFreeMap;
        }
    }

    fn readValuePagesAlloc(self: *NativeFile, allocator: Allocator, root_page_id: u64, value_len: usize) ![]u8 {
        return try self.readValuePagesAtCheckpointAlloc(allocator, root_page_id, value_len, self.activeCheckpoint());
    }

    fn readValuePagesAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, root_page_id: u64, value_len: usize, checkpoint: CheckpointSlot) ![]u8 {
        if (value_len == 0 or root_page_id == 0) return error.InvalidNativeValueChain;

        if (try self.valueTreeRoot(root_page_id, value_len, checkpoint)) |ref| {
            const out = try allocator.alloc(u8, value_len);
            errdefer allocator.free(out);
            try self.readExtentRange(ref, checkpoint, 0, out);
            return out;
        }

        const value = try allocator.alloc(u8, value_len);
        errdefer allocator.free(value);

        var written: usize = 0;
        var page_id = root_page_id;
        var pages_seen: u64 = 0;
        while (page_id != 0) {
            pages_seen += 1;
            if (pages_seen > checkpoint.page_count) return error.InvalidNativeValueChain;

            const payload = try self.readPagePayloadByKindAllocForCheckpoint(allocator, page_id, .value, checkpoint);
            defer allocator.free(payload);
            const page = try decodeValuePage(payload);
            if (page.chunk.len == 0) return error.InvalidNativeValueChain;
            if (page.chunk.len > value_len - written) return error.InvalidNativeValueChain;
            @memcpy(value[written..][0..page.chunk.len], page.chunk);
            written += page.chunk.len;
            page_id = page.next_page;
            if (written == value_len and page_id != 0) return error.InvalidNativeValueChain;
        }

        if (written != value_len) return error.InvalidNativeValueChain;
        return value;
    }

    fn readValuePagesRangeAlloc(
        self: *NativeFile,
        allocator: Allocator,
        root_page_id: u64,
        value_len: usize,
        range_start: usize,
        range_len: usize,
        checkpoint: CheckpointSlot,
    ) ![]u8 {
        if (value_len == 0 or root_page_id == 0) return error.InvalidNativeValueChain;

        if (try self.valueTreeRoot(root_page_id, value_len, checkpoint)) |ref| {
            const bytes = try allocator.alloc(u8, range_len);
            errdefer allocator.free(bytes);
            try self.readExtentRange(ref, checkpoint, range_start, bytes);
            return bytes;
        }

        const out = try allocator.alloc(u8, range_len);
        errdefer allocator.free(out);

        const range_end = range_start + range_len;
        var value_offset: usize = 0;
        var written: usize = 0;
        var page_id = root_page_id;
        var pages_seen: u64 = 0;
        while (page_id != 0) {
            pages_seen += 1;
            if (pages_seen > checkpoint.page_count) return error.InvalidNativeValueChain;

            const payload = try self.readPagePayloadByKindAllocForCheckpoint(allocator, page_id, .value, checkpoint);
            defer allocator.free(payload);
            const page = try decodeValuePage(payload);
            if (page.chunk.len == 0) return error.InvalidNativeValueChain;
            if (page.chunk.len > value_len - value_offset) return error.InvalidNativeValueChain;

            const page_start = value_offset;
            const page_end = page_start + page.chunk.len;
            if (page_end > range_start and page_start < range_end) {
                const copy_start = if (range_start > page_start) range_start - page_start else 0;
                const copy_end = @min(page.chunk.len, range_end - page_start);
                const copy_len = copy_end - copy_start;
                @memcpy(out[written..][0..copy_len], page.chunk[copy_start..][0..copy_len]);
                written += copy_len;
            }

            value_offset = page_end;
            page_id = page.next_page;
            if (value_offset == value_len and page_id != 0) return error.InvalidNativeValueChain;
            if (value_offset >= range_end and written == range_len) {
                break;
            }
        }

        if (written != range_len) return error.InvalidNativeValueChain;
        return out;
    }

    fn validateReachableValuePages(
        self: *NativeFile,
        root_page_id: u64,
        value_len: usize,
        checkpoint: CheckpointSlot,
        reachable_pages: *ReachablePageSet,
    ) !void {
        if (value_len == 0 or root_page_id == 0) return error.InvalidNativeValueChain;

        if (try self.valueTreeRoot(root_page_id, value_len, checkpoint)) |ref| {
            return try self.validateExtent(ref, checkpoint, reachable_pages);
        }

        var remaining = value_len;
        var page_id = root_page_id;
        var pages_seen: u64 = 0;
        const use_link_cache = self.page_cache_enabled.load(.monotonic) and self.page_cache_bypass.load(.monotonic) == 0;
        while (page_id != 0) {
            pages_seen += 1;
            if (pages_seen > checkpoint.page_count) return error.InvalidNativeValueChain;

            try self.markReachablePage(reachable_pages, page_id, checkpoint.page_count);

            var chunk_len: usize = 0;
            var next_page: u64 = 0;
            var resolved = false;
            if (use_link_cache) {
                if (try self.page_cache.getLinksCopy(self.allocator, page_id)) |links| {
                    defer if (links.key) |key| self.allocator.free(key);
                    if (links.kind != .value) return error.UnexpectedNativePageKind;
                    chunk_len = links.chunk_len;
                    next_page = links.link_page;
                    resolved = true;
                }
            }
            if (!resolved) {
                const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, page_id, .value, checkpoint);
                defer self.allocator.free(payload);
                const page = try decodeValuePage(payload);
                chunk_len = page.chunk.len;
                next_page = page.next_page;
                if (use_link_cache) self.cachePageLinks(page_id, .value, payload);
            }

            if (chunk_len == 0) return error.InvalidNativeValueChain;
            if (chunk_len > remaining) return error.InvalidNativeValueChain;
            remaining -= chunk_len;
            page_id = next_page;
            if (remaining == 0 and page_id != 0) return error.InvalidNativeValueChain;
        }

        if (remaining != 0) return error.InvalidNativeValueChain;
    }

    fn validateReachableFreeMap(self: *NativeFile, checkpoint: CheckpointSlot, reachable_pages: *ReachablePageSet) !void {
        if (checkpoint.free_map_root_page == 0) return;
        try self.markReachablePage(reachable_pages, checkpoint.free_map_root_page, checkpoint.page_count);

        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, checkpoint.free_map_root_page, .free_map, checkpoint);
        defer self.allocator.free(payload);
        const free_map = try decodeFreeMapAlloc(self.allocator, payload, checkpoint.page_count);
        defer self.allocator.free(free_map.free_pages);

        for (free_map.free_pages) |page_id| {
            if (reachable_pages.contains(page_id)) return error.InvalidNativeFreeMap;
        }
        try self.validateFreePagesSafeForCheckpointSlots(free_map.free_pages);
    }

    const LiveStats = struct {
        record_count: u64,
        bytes: u64,
        compact_size: u64,
    };

    /// Compact-layout accounting uses only record metadata and ordered keys.
    /// Integrity checking separately visits and validates all reachable payloads.
    fn liveStats(self: *NativeFile, cancel: ?*const maintenance.CancelToken) !LiveStats {
        var live_bytes: u64 = 0;
        var compact_pages: u64 = 2; // header and the vacuum's empty free map
        var record_count: u64 = 0;
        var namespace_directory = NamespaceDirectory.empty;
        defer deinitNamespaceDirectory(self.allocator, &namespace_directory);

        for ([_]LiveRecordSource{ .{ .catalog = .metadata }, .{ .catalog = .index }, .documents }) |source| {
            var cursor = try LiveRecordCursor.init(self, source);
            defer cursor.deinit();
            var counter = DocumentIndexBulkBuilder{ .owner = self, .file = undefined, .next_page_id = &compact_pages, .count_only = true };
            defer counter.deinit();
            var count: u64 = 0;
            var packed_used: usize = 0;
            while (try cursor.next(cancel)) |record| {
                const len = if (record.external_value_root_page == 0) record.value.len else record.external_value_len;
                const is_catalog = cursor.kind == .catalog;
                const header_len: usize = if (is_catalog) 16 else 28;
                live_bytes +|= record.key.len + len;
                const external = header_len + record.key.len + len > self.maxPagePayloadBytes();
                const record_size = header_len + record.key.len + (if (external) @as(usize, 8) else len);
                if (external) compact_pages += if (is_catalog) self.valueTreePageCount(len) else self.valuePageCount(len);
                if (record_size + 4 > self.maxPagePayloadBytes() / 2) {
                    compact_pages += 1;
                } else {
                    if (packed_used == 0 or packed_used + 4 + record_size > self.maxPagePayloadBytes()) {
                        compact_pages += 1;
                        packed_used = 0;
                    }
                    packed_used += 4 + record_size;
                }
                try counter.add(record.key, 1);
                count += 1;
                if (!is_catalog) {
                    const namespace = documentNamespace(record.key);
                    if (!namespace_directory.contains(namespace)) {
                        const owned = try self.allocator.dupe(u8, namespace);
                        errdefer self.allocator.free(owned);
                        try namespace_directory.put(self.allocator, owned, 1);
                    }
                }
            }
            _ = try counter.finish();
            if (cursor.kind == .catalog and count > 0) compact_pages += 1; // descriptor
            record_count += count;
        }
        if (namespace_directory.count() > 0) {
            const encoded = try encodeNamespaceDirectoryAlloc(self.allocator, .snapshot, &namespace_directory);
            defer self.allocator.free(encoded);
            compact_pages += 1;
            if (!self.catalogEntryFitsInline(namespace_directory_key, encoded)) compact_pages += self.valuePageCount(encoded.len);
        }
        return .{ .record_count = record_count, .bytes = live_bytes, .compact_size = compact_pages * @as(u64, self.header.page_size) };
    }

    fn valueTreePageCount(self: *const NativeFile, value_len: usize) u64 {
        var level = self.valuePageCount(value_len);
        var total = level;
        while (level > 1) {
            level = std.math.divCeil(u64, level, extent_fanout) catch unreachable;
            total += level;
        }
        return total;
    }

    fn valuePageCount(self: *const NativeFile, value_len: usize) u64 {
        std.debug.assert(value_len > 0);
        return @intCast(std.math.divCeil(usize, value_len, self.maxValuePagePayloadBytes()) catch unreachable);
    }

    fn writePage(self: *NativeFile, page_id: u64, kind: PageKind, contents: []const u8) !void {
        return self.writePageWithOptions(page_id, kind, contents, .{});
    }

    fn writePageWithOptions(self: *NativeFile, page_id: u64, kind: PageKind, contents: []const u8, options: WriteOptions) !void {
        var batch = PageWriteBatch{ .file = self, .options = options };
        try batch.appendPage(page_id, kind, contents);
        try batch.flush();
    }

    /// Cache admission happens only after the complete encoded write succeeds.
    fn cacheWrittenPage(self: *NativeFile, page_id: u64, page: []const u8, options: WriteOptions) void {
        const kind: PageKind = @enumFromInt(page[4]);
        const len = std.mem.readInt(u32, page[8..12], .little);
        const contents = page[page_header_size..][0..len];
        if (!self.admitPageToCache(page) or (kind == .value and options.payload_cache == .cold_sequential)) {
            // Skipping admission must still remove stale bytes AND navigation
            // links from a previous use of this page ID. Cold payload pages
            // also skip link admission; extent/catalog metadata stays warm.
            self.page_cache.remove(self.allocator, page_id);
        } else if (self.page_cache_enabled.load(.monotonic)) {
            self.page_cache.put(self.allocator, page_id, page);
            self.cachePageLinks(page_id, kind, contents);
        }
    }

    /// Best-effort: decode and cache the chain-navigation metadata for a page
    /// just written, so reachability walks can traverse it without re-reading
    /// the payload. On any decode surprise the stale entry is dropped and the
    /// walks fall back to the payload path.
    fn cachePageLinks(self: *NativeFile, page_id: u64, kind: PageKind, payload: []const u8) void {
        if (page_id & packed_record_flag != 0) return;
        if (!self.page_cache_enabled.load(.monotonic)) return;
        switch (kind) {
            .document => {
                const entry = decodeDocumentEntry(payload) catch {
                    self.page_cache.remove(self.allocator, page_id);
                    return;
                };
                self.page_cache.putLinks(self.allocator, page_id, .{
                    .kind = .document,
                    .link_page = entry.previous_page,
                    .external_value_root_page = entry.external_value_root_page,
                    .external_value_len = entry.external_value_len,
                });
            },
            .catalog => {
                const entry = decodeCatalogEntry(payload) catch {
                    self.page_cache.remove(self.allocator, page_id);
                    return;
                };
                self.page_cache.putLinks(self.allocator, page_id, .{
                    .kind = .catalog,
                    .link_page = entry.previous_page,
                    .external_value_root_page = entry.external_value_root_page,
                    .external_value_len = entry.external_value_len,
                    .key = @constCast(entry.key),
                    .is_delete = entry.is_delete,
                    .value_len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len,
                });
            },
            .value => {
                const page = decodeValuePage(payload) catch {
                    self.page_cache.remove(self.allocator, page_id);
                    return;
                };
                self.page_cache.putLinks(self.allocator, page_id, .{
                    .kind = .value,
                    .link_page = page.next_page,
                    .chunk_len = page.chunk.len,
                });
            },
            // A reused page id may carry stale link info from a previous life.
            .data, .value_extent, .catalog_index, .record_bundle => self.page_cache.removeLinks(self.allocator, page_id),
            .document_index => self.page_cache.removeLinks(self.allocator, page_id),
            .free_map => unreachable,
        }
    }

    /// The caller serializes writers and checkpoint acquisition throughout a
    /// transaction. Intermediate roots are private; existing pinned readers
    /// continue reading immutable pages from their own checkpoint.
    pub fn beginTransaction(self: *NativeFile) !void {
        if (self.read_only) return error.ReadOnly;
        if (self.transaction_header != null) return error.TransactionAlreadyActive;
        if (self.checkpoint_publication_uncertain) return error.OutcomeUnknown;
        self.transaction_header = self.header;
    }

    pub fn abortTransaction(self: *NativeFile) void {
        const previous = self.transaction_header orelse return;
        self.header = previous;
        self.transaction_header = null;
        self.discardTransactionTail();
        self.namespace_directory_cache_root = std.math.maxInt(u64);
        deinitNamespaceDirectory(self.allocator, &self.namespace_directory_cache);
        self.namespace_directory_cache = .empty;
        self.namespace_directory_delta_depth = 0;
    }

    fn discardTransactionTail(self: *NativeFile) void {
        const first_page = self.activeCheckpoint().page_count;
        self.page_cache.discardFrom(self.allocator, first_page);
        const size = first_page * @as(u64, self.header.page_size);
        self.file.setLength(self.runtimeIo(), size) catch {
            self.checkpoint_publication_uncertain = true;
        };
    }

    pub fn commitTransaction(self: *NativeFile) !void {
        return self.commitTransactionWithDurability(true);
    }

    pub fn commitTransactionWithDurability(self: *NativeFile, durable: bool) !void {
        const previous = self.transaction_header orelse return error.InvalidTransactionState;
        var next = self.activeCheckpoint();
        const changed = next.commit_sequence != previous.checkpoints[previous.active_checkpoint].commit_sequence;
        self.header = previous;
        self.transaction_header = null;
        errdefer {
            if (!self.checkpoint_publication_uncertain) self.discardTransactionTail();
            self.namespace_directory_cache_root = std.math.maxInt(u64);
            deinitNamespaceDirectory(self.allocator, &self.namespace_directory_cache);
            self.namespace_directory_cache = .empty;
            self.namespace_directory_delta_depth = 0;
        }
        if (changed) next.commit_sequence = previous.checkpoints[previous.active_checkpoint].commit_sequence + 1;
        if (!durable and !self.no_sync) {
            // Keep the last durable checkpoint slots intact until an explicit
            // durability barrier. Readers on this handle see the new roots;
            // reopening after a crash sees the previous complete checkpoint.
            if (changed) {
                if (self.durable_header == null) self.durable_header = previous;
                self.header.checkpoints[self.header.active_checkpoint] = next;
            }
            return;
        }
        if (changed) {
            try self.syncIfRequired();
            try self.publishCheckpoint(next);
        } else try self.sync();
    }

    fn publishCheckpoint(self: *NativeFile, checkpoint: CheckpointSlot) !void {
        if (self.transaction_header != null) {
            self.header.checkpoints[self.header.active_checkpoint] = checkpoint;
            return;
        }
        if (self.durable_header) |durable| {
            self.header = durable;
            self.durable_header = null;
        }
        const next_slot: u8 = if (self.header.active_checkpoint == 0) 1 else 0;

        var encoded_slot: [checkpoint_slot_size]u8 = undefined;
        encodeCheckpointSlot(&encoded_slot, checkpoint);

        const io = self.runtimeIo();
        self.checkpoint_publication_uncertain = true;
        try self.file.writePositionalAll(io, &encoded_slot, checkpointOffset(next_slot));
        try self.syncIfRequired();
        const active_checkpoint: [1]u8 = .{next_slot};
        try self.file.writePositionalAll(io, &active_checkpoint, active_checkpoint_offset);
        try self.syncIfRequired();

        self.header.checkpoints[next_slot] = checkpoint;
        self.header.active_checkpoint = next_slot;
        self.checkpoint_publication_uncertain = false;
    }

    fn syncIfRequired(self: *NativeFile) !void {
        if (!self.no_sync and self.transaction_header == null) try self.file.sync(self.runtimeIo());
    }

    pub fn sync(self: *NativeFile) !void {
        if (self.transaction_header != null) return;
        try self.syncIfRequired();
        if (self.durable_header != null) try self.publishCheckpoint(self.activeCheckpoint());
    }

    pub fn failNextGenerationDirectorySyncForTest(self: *NativeFile) void {
        std.debug.assert(builtin.is_test);
        self.test_fail_generation_directory_sync = true;
    }

    /// Atomically publishes a fully built Lite generation into this file and
    /// adopts its already-open descriptor. The destination writer lock stays
    /// with `self`; `prepared` receives the retired descriptor and can be
    /// closed normally after this returns.
    pub fn replaceWithPreparedGeneration(self: *NativeFile, prepared: *NativeFile) !GenerationPublicationOutcome {
        if (self.read_only or prepared.read_only) return error.ReadOnly;
        if (std.mem.eql(u8, self.path, prepared.path)) return error.InvalidNativeSnapshotPath;

        try prepared.syncIfRequired();
        const io = self.runtimeIo();
        try renameFilePath(io, prepared.path, self.path);

        std.mem.swap(std.Io.File, &self.file, &prepared.file);
        const retired_header = self.header;
        self.header = prepared.header;
        prepared.header = retired_header;
        self.durable_header = null;
        prepared.durable_header = null;
        self.free_pages_verified = false;
        prepared.free_pages_verified = false;
        self.namespace_directory_cache_root = std.math.maxInt(u64);
        self.namespace_directory_delta_depth = 0;
        deinitNamespaceDirectory(self.allocator, &self.namespace_directory_cache);
        self.namespace_directory_cache = .empty;
        self.page_cache.clear(self.allocator);

        // The generation is already visible and the live handle has adopted
        // it. Preserve that committed state while returning an explicit
        // durability outcome to the layer that can finish rebinding runtime
        // metadata before surfacing it to the caller.
        if (!self.no_sync) {
            if (builtin.is_test and self.test_fail_generation_directory_sync) {
                self.test_fail_generation_directory_sync = false;
                std.log.err("Lite restore published but parent directory sync failed path={s} class={s}", .{ self.path, @errorName(error.InjectedGenerationDirectorySyncFailure) });
                return .durability_unknown;
            }
            fs_paths.syncDirPortable(io, std.fs.path.dirname(self.path) orelse ".") catch |err| {
                std.log.err("Lite restore published but parent directory sync failed path={s} class={s}", .{ self.path, @errorName(err) });
                return .durability_unknown;
            };
        }
        return .complete;
    }
};

fn appendPageToFile(
    allocator: Allocator,
    file: std.Io.File,
    io: std.Io,
    page_size: usize,
    next_page_id: *u64,
    kind: PageKind,
    contents: []const u8,
) !u64 {
    if (contents.len > page_size - page_header_size) return error.PageTooLarge;
    const page_id = next_page_id.*;
    const page = try allocator.alloc(u8, page_size);
    defer allocator.free(page);
    encodePage(page, kind, contents);
    try file.writePositionalAll(io, page, page_id * @as(u64, @intCast(page_size)));
    next_page_id.* += 1;
    return page_id;
}

const DocumentIndexChild = struct {
    first_key: []u8,
    key_page: u64,
    page_id: u64,
};

/// Packed B+ tree builder with one unfinished node per level. Long keys
/// remain record references, including separators; only the last input key is
/// materialized for order validation. Counting uses exactly the same frontier.
const DocumentIndexBulkBuilder = struct {
    const Level = struct {
        children: std.ArrayListUnmanaged(DocumentIndexChild) = .empty,
        bytes: usize = document_index_header_size + @sizeOf(u64),
    };

    owner: *NativeFile,
    file: std.Io.File,
    next_page_id: *u64,
    count_only: bool = false,
    leaf_keys: std.ArrayListUnmanaged([]u8) = .empty,
    leaf_pointers: std.ArrayListUnmanaged(u64) = .empty,
    leaf_key_pages: std.ArrayListUnmanaged(u64) = .empty,
    leaf_bytes: usize = document_index_header_size,
    last_key: std.ArrayListUnmanaged(u8) = .empty,
    has_last_key: bool = false,
    levels: std.ArrayListUnmanaged(Level) = .empty,

    fn deinit(self: *DocumentIndexBulkBuilder) void {
        for (self.leaf_keys.items) |key| self.owner.allocator.free(key);
        self.leaf_keys.deinit(self.owner.allocator);
        self.leaf_pointers.deinit(self.owner.allocator);
        self.leaf_key_pages.deinit(self.owner.allocator);
        self.last_key.deinit(self.owner.allocator);
        for (self.levels.items) |*level| {
            for (level.children.items) |child| self.owner.allocator.free(child.first_key);
            level.children.deinit(self.owner.allocator);
        }
        self.levels.deinit(self.owner.allocator);
    }

    fn appendNode(self: *DocumentIndexBulkBuilder, node: DocumentIndexNode) !u64 {
        if (self.count_only) {
            const page = self.next_page_id.*;
            self.next_page_id.* += 1;
            return page;
        }
        const encoded = try encodeDocumentIndexNode(self.owner.allocator, node);
        defer self.owner.allocator.free(encoded);
        return try appendPageToFile(self.owner.allocator, self.file, self.owner.runtimeIo(), self.owner.header.page_size, self.next_page_id, .document_index, encoded);
    }

    fn add(self: *DocumentIndexBulkBuilder, key: []const u8, document_page_id: u64) !void {
        if (self.has_last_key and std.mem.order(u8, self.last_key.items, key) != .lt)
            return error.InvalidDocumentIndexOrder;
        if (key.len > std.math.maxInt(u16)) return error.RecordTooLarge;
        const external = key.len > index_inline_key_limit;
        const slot_size = 10 + (if (external) @as(usize, 8) else key.len);
        if (document_index_header_size + slot_size > self.owner.maxPagePayloadBytes()) return error.DocumentIndexNodeTooLarge;
        if (self.leaf_bytes + slot_size > self.owner.maxPagePayloadBytes()) try self.flushLeaf();
        const alloc = self.owner.allocator;
        try self.leaf_keys.ensureUnusedCapacity(alloc, 1);
        try self.leaf_pointers.ensureUnusedCapacity(alloc, 1);
        try self.leaf_key_pages.ensureUnusedCapacity(alloc, 1);
        try self.last_key.ensureTotalCapacity(alloc, key.len);
        const owned = try alloc.dupe(u8, if (external) "" else key);
        self.leaf_keys.appendAssumeCapacity(owned);
        self.leaf_pointers.appendAssumeCapacity(document_page_id);
        self.leaf_key_pages.appendAssumeCapacity(if (external) document_page_id else 0);
        self.leaf_bytes += slot_size;
        self.last_key.clearRetainingCapacity();
        self.last_key.appendSliceAssumeCapacity(key);
        self.has_last_key = true;
    }

    fn flushLeaf(self: *DocumentIndexBulkBuilder) !void {
        if (self.leaf_keys.items.len == 0) return;
        const page = try self.appendNode(.{ .kind = .leaf, .keys = self.leaf_keys.items, .pointers = self.leaf_pointers.items, .key_pages = self.leaf_key_pages.items });
        const child = DocumentIndexChild{
            .first_key = try self.owner.allocator.dupe(u8, self.leaf_keys.items[0]),
            .key_page = self.leaf_key_pages.items[0],
            .page_id = page,
        };
        // pushChild consumes the separator even on failure.
        try self.pushChild(0, child);
        for (self.leaf_keys.items) |key| self.owner.allocator.free(key);
        self.leaf_keys.clearRetainingCapacity();
        self.leaf_pointers.clearRetainingCapacity();
        self.leaf_key_pages.clearRetainingCapacity();
        self.leaf_bytes = document_index_header_size;
    }

    fn pushChild(self: *DocumentIndexBulkBuilder, height: usize, child: DocumentIndexChild) anyerror!void {
        errdefer self.owner.allocator.free(child.first_key);
        if (height >= 64) return error.InvalidDocumentIndex;
        if (height >= self.levels.items.len) {
            const old = self.levels.items.len;
            try self.levels.resize(self.owner.allocator, height + 1);
            @memset(self.levels.items[old..], .{});
        }
        const slot_size = 10 + (if (child.key_page != 0) @as(usize, 8) else child.first_key.len);
        if (self.levels.items[height].children.items.len > 0 and
            self.levels.items[height].bytes + slot_size > self.owner.maxPagePayloadBytes())
        {
            if (self.levels.items[height].children.items.len < 2) return error.DocumentIndexNodeTooLarge;
            const parent = try self.sealLevel(height);
            try self.pushChild(height + 1, parent);
        }
        // Recursion can relocate levels, so reacquire this pointer afterward.
        const level = &self.levels.items[height];
        const extra = if (level.children.items.len == 0) 0 else slot_size;
        try level.children.append(self.owner.allocator, child);
        level.bytes += extra;
    }

    fn sealLevel(self: *DocumentIndexBulkBuilder, height: usize) !DocumentIndexChild {
        const level = &self.levels.items[height];
        const group = level.children.items;
        const page = try self.appendInternalGroup(group);
        const parent = DocumentIndexChild{ .first_key = group[0].first_key, .key_page = group[0].key_page, .page_id = page };
        for (group[1..]) |child| self.owner.allocator.free(child.first_key);
        level.children.clearRetainingCapacity();
        level.bytes = document_index_header_size + @sizeOf(u64);
        return parent;
    }

    fn finish(self: *DocumentIndexBulkBuilder) !u64 {
        try self.flushLeaf();
        var height: usize = 0;
        while (height < self.levels.items.len) : (height += 1) {
            const count = self.levels.items[height].children.items.len;
            if (count == 0) continue;
            var higher_pending = false;
            for (self.levels.items[height + 1 ..]) |level| {
                if (level.children.items.len != 0) higher_pending = true;
            }
            if (count == 1 and !higher_pending) return self.levels.items[height].children.items[0].page_id;
            const parent = try self.sealLevel(height);
            try self.pushChild(height + 1, parent);
        }
        return 0;
    }

    fn appendInternalGroup(self: *DocumentIndexBulkBuilder, children: []const DocumentIndexChild) !u64 {
        const alloc = self.owner.allocator;
        const keys = try alloc.alloc([]u8, children.len - 1);
        defer alloc.free(keys);
        const pointers = try alloc.alloc(u64, children.len);
        defer alloc.free(pointers);
        const key_pages = try alloc.alloc(u64, children.len - 1);
        defer alloc.free(key_pages);
        for (children, 0..) |child, i| {
            pointers[i] = child.page_id;
            if (i > 0) {
                keys[i - 1] = child.first_key;
                key_pages[i - 1] = child.key_page;
            }
        }
        return try self.appendNode(.{ .kind = .internal, .keys = keys, .pointers = pointers, .key_pages = key_pages });
    }
};

fn appendValuePagesToFile(
    allocator: Allocator,
    file: std.Io.File,
    io: std.Io,
    page_size: usize,
    chunk_size: usize,
    next_page_id: *u64,
    value: []const u8,
) !u64 {
    if (value.len == 0) return error.InvalidNativeValueChain;
    if (chunk_size == 0) return error.InvalidNativePageLength;
    const page_count = std.math.divCeil(usize, value.len, chunk_size) catch unreachable;
    const root_page_id = next_page_id.*;

    var offset: usize = 0;
    var page_index: usize = 0;
    while (offset < value.len) : (page_index += 1) {
        const len = @min(chunk_size, value.len - offset);
        const current_page_id = next_page_id.*;
        const next_value_page = if (page_index + 1 < page_count) current_page_id + 1 else 0;
        const payload = try allocator.alloc(u8, value_page_header_size + len);
        defer allocator.free(payload);
        std.mem.writeInt(u64, payload[0..8], next_value_page, .little);
        @memcpy(payload[value_page_header_size..][0..len], value[offset..][0..len]);
        _ = try appendPageToFile(allocator, file, io, page_size, next_page_id, .value, payload);
        offset += len;
    }
    return root_page_id;
}

fn appendFreeMapPageToFile(
    allocator: Allocator,
    file: std.Io.File,
    io: std.Io,
    page_size: usize,
    next_page_id: *u64,
    covered_page_count: u64,
    free_pages: []const u64,
) !u64 {
    const payload = try encodeFreeMapAlloc(allocator, @intCast(page_size), covered_page_count, free_pages);
    defer allocator.free(payload);
    return try appendPageToFile(allocator, file, io, page_size, next_page_id, .free_map, payload);
}

fn catalogRootPage(slot: CheckpointSlot, root: CatalogRoot) u64 {
    return switch (root) {
        .metadata => slot.catalog_root_page,
        .index => slot.index_catalog_root_page,
    };
}

fn setCatalogRootPage(slot: *CheckpointSlot, root: CatalogRoot, page_id: u64) void {
    switch (root) {
        .metadata => slot.catalog_root_page = page_id,
        .index => slot.index_catalog_root_page = page_id,
    }
}

pub fn create(io: std.Io, path: []const u8) !void {
    var writer_lock_file = (try acquireWriterLock(std.heap.page_allocator, io, path)).file;
    defer writer_lock_file.close(io);

    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});

    const replace_existing = pathExists(io, path);
    const replacement_path = if (replace_existing)
        try realPathAlloc(std.heap.page_allocator, io, path)
    else
        null;
    defer if (replacement_path) |canonical| std.heap.page_allocator.free(canonical);
    const create_target = if (replacement_path) |canonical| canonical else path;
    const staging_path = if (replace_existing)
        try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-aflite-create", .{create_target})
    else
        null;
    defer if (staging_path) |tmp_path| std.heap.page_allocator.free(tmp_path);
    errdefer if (staging_path) |tmp_path| deleteFilePath(io, tmp_path) catch {};

    var file = try createDataFile(io, staging_path orelse create_target, .{ .truncate = true });
    var file_open = true;
    defer if (file_open) file.close(io);

    try file.writePositionalAll(io, &encoded, 0);
    try file.sync(io);
    if (staging_path) |tmp_path| {
        file.close(io);
        file_open = false;
        renameFilePath(io, tmp_path, create_target) catch |err| {
            deleteFilePath(io, tmp_path) catch {};
            return err;
        };
    }
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(create_target) orelse ".");
}

pub fn lockWriterPath(allocator: Allocator, path: []const u8) !PathWriterLock {
    var io_impl = threaded_io_limits.initService(allocator);
    errdefer io_impl.deinit();

    const file = (try acquireWriterLock(allocator, io_impl.io(), path)).file;
    errdefer file.close(io_impl.io());

    return .{
        .io_impl = io_impl,
        .file = file,
    };
}

/// Acquires the same cross-process writer lock through a caller-owned std.Io.
pub fn lockWriterPathWithIo(allocator: Allocator, io: std.Io, path: []const u8) !PathWriterLock {
    const file = (try acquireWriterLock(allocator, io, path)).file;
    errdefer file.close(io);
    return .{
        .io_impl = undefined,
        .borrowed_io = io,
        .file = file,
    };
}

fn openDataFile(io: std.Io, path: []const u8, lock_mode: LockMode) !LockFile {
    const file = std.Io.Dir.cwd().openFile(io, path, .{
        .mode = if (lock_mode == .reader) .read_only else .read_write,
        .lock = if (lock_mode == .reader) .shared else .none,
        .lock_nonblocking = true,
    }) catch |err| switch (err) {
        // Snapshot safety and maintenance fencing depend on the kernel lock.
        // Silently reopening without it turns an unsupported filesystem into
        // a data-corruption hazard, so every normal Lite open fails closed.
        error.FileLocksUnsupported => return error.FileLocksUnsupported,
        else => return err,
    };
    return .{ .file = file };
}

const CreateDataFileOptions = struct {
    truncate: bool = true,
    exclusive: bool = false,
};

fn createDataFile(io: std.Io, path: []const u8, opts: CreateDataFileOptions) !std.Io.File {
    return std.Io.Dir.cwd().createFile(io, path, .{
        .read = true,
        .truncate = opts.truncate,
        .exclusive = opts.exclusive,
    });
}

fn acquireWriterLock(allocator: Allocator, io: std.Io, path: []const u8) !LockFile {
    const lock_path = try writerLockPathAlloc(allocator, io, path);
    defer allocator.free(lock_path);
    const file = std.Io.Dir.cwd().createFile(io, lock_path, .{
        .read = true,
        .truncate = false,
        .lock = .exclusive,
        .lock_nonblocking = true,
    }) catch |err| switch (err) {
        error.FileLocksUnsupported => return error.FileLocksUnsupported,
        else => return err,
    };
    return .{ .file = file };
}

fn writerLockPathAlloc(allocator: Allocator, io: std.Io, path: []const u8) ![]u8 {
    const canonical_data_path = realPathAlloc(allocator, io, path) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    if (canonical_data_path) |canonical| {
        defer allocator.free(canonical);
        return appendLockSuffix(allocator, canonical);
    }

    const dirname = std.fs.path.dirname(path) orelse ".";
    const basename = std.fs.path.basename(path);
    const canonical_parent = try realPathAlloc(allocator, io, dirname);
    defer allocator.free(canonical_parent);
    const canonical_missing_path = try std.fs.path.join(allocator, &.{ canonical_parent, basename });
    defer allocator.free(canonical_missing_path);
    return appendLockSuffix(allocator, canonical_missing_path);
}

fn pathsReferToSameExistingFile(allocator: Allocator, io: std.Io, a: []const u8, b: []const u8) !bool {
    const a_real = realPathAlloc(allocator, io, a) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    defer allocator.free(a_real);

    const b_real = realPathAlloc(allocator, io, b) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    defer allocator.free(b_real);

    return std.mem.eql(u8, a_real, b_real);
}

fn realPathAlloc(allocator: Allocator, io: std.Io, path: []const u8) ![:0]u8 {
    if (std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.realPathFileAbsoluteAlloc(io, path, allocator);
    }
    return try std.Io.Dir.cwd().realPathFileAlloc(io, path, allocator);
}

fn appendLockSuffix(allocator: Allocator, path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, "{s}.lock", .{path});
}

fn acquireDataRewriteLock(io: std.Io, path: []const u8) !LockFile {
    const file = std.Io.Dir.cwd().openFile(io, path, .{
        .mode = .read_write,
        .lock = .exclusive,
        .lock_nonblocking = true,
    }) catch |err| switch (err) {
        error.FileLocksUnsupported => return error.FileLocksUnsupported,
        else => return err,
    };
    return .{ .file = file };
}

fn pathExists(io: std.Io, path: []const u8) bool {
    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.accessAbsolute(io, path, .{}) catch return false;
    } else {
        std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    }
    return true;
}

fn createSnapshotFile(io: std.Io, path: []const u8) !std.Io.File {
    if (std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.createFileAbsolute(io, path, .{ .read = true, .truncate = true });
    }
    return try std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .truncate = true });
}

fn renameFilePath(io: std.Io, old_path: []const u8, new_path: []const u8) !void {
    if (std.fs.path.isAbsolute(old_path) and std.fs.path.isAbsolute(new_path)) {
        try std.Io.Dir.renameAbsolute(old_path, new_path, io);
    } else {
        try std.Io.Dir.rename(std.Io.Dir.cwd(), old_path, std.Io.Dir.cwd(), new_path, io);
    }
}

fn deleteFilePath(io: std.Io, path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.deleteFileAbsolute(io, path);
    } else {
        try std.Io.Dir.cwd().deleteFile(io, path);
    }
}

pub fn inspect(_: Allocator, io: std.Io, path: []const u8) !InspectReport {
    var file = (try openDataFile(io, path, .reader)).file;
    defer file.close(io);

    var header_bytes: [header_size]u8 = undefined;
    try readHeaderExactAt(file, io, &header_bytes);
    return inspectBytes(&header_bytes);
}

pub fn checkFile(allocator: Allocator, path: []const u8) !CheckReport {
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var file = (try openDataFile(io, path, .reader)).file;
    defer file.close(io);

    const file_size = (try file.stat(io)).size;
    var header_bytes: [header_size]u8 = undefined;
    const read = try file.readPositionalAll(io, &header_bytes, 0);
    if (read != header_size) {
        return invalidCheck(.{
            .valid = true,
            .file_size = file_size,
            .valid_prefix_size = 0,
            .tail_bytes = file_size,
            .record_count = 0,
            .live_file_count = 0,
            .live_bytes = 0,
            .compact_size = 0,
            .reclaimable_bytes = 0,
        }, "truncated_header");
    }
    const header = decodeHeader(&header_bytes) catch |err| {
        return invalidCheck(.{
            .valid = true,
            .file_size = file_size,
            .valid_prefix_size = 0,
            .tail_bytes = file_size,
            .record_count = 0,
            .live_file_count = 0,
            .live_bytes = 0,
            .compact_size = 0,
            .reclaimable_bytes = 0,
        }, issueForDecodeError(err));
    };
    _ = selectCompleteCheckpointForFile(header, file_size) catch |err| {
        const checkpoint = header.checkpoints[header.active_checkpoint];
        const expected_size = checkpointPrefixSize(checkpoint, header.page_size) catch 0;
        return invalidCheck(.{
            .valid = true,
            .file_size = file_size,
            .valid_prefix_size = file_size,
            .tail_bytes = 0,
            .record_count = if (expected_size > 0 and checkpoint.page_count > 0) checkpoint.page_count - 1 else 0,
            .live_file_count = 0,
            .live_bytes = 0,
            .compact_size = expected_size,
            .reclaimable_bytes = 0,
        }, switch (err) {
            error.InvalidNativeCheckpoint => "invalid_checkpoint",
            error.TruncatedNativeFile => "truncated_file",
        });
    };
    var native_file = try NativeFile.open(allocator, path, true);
    defer native_file.close();
    return try native_file.check();
}

pub fn copyStableSnapshot(allocator: Allocator, source_path: []const u8, dest_path: []const u8, replace: bool) !StableSnapshotReport {
    if (std.mem.eql(u8, source_path, dest_path)) return error.InvalidNativeSnapshotPath;

    var source = try NativeFile.open(allocator, source_path, true);
    defer source.close();
    return try source.copyStableSnapshotToPath(dest_path, replace);
}

pub fn inspectBytes(raw: []const u8) InspectReport {
    const header = decodeHeader(raw) catch |err| {
        return .{
            .valid = false,
            .format_version = 0,
            .page_size = 0,
            .active_checkpoint = 0,
            .commit_sequence = 0,
            .page_count = 0,
            .issue = issueForDecodeError(err),
        };
    };
    const active = header.checkpoints[header.active_checkpoint];
    return .{
        .valid = true,
        .format_version = format_version,
        .page_size = header.page_size,
        .active_checkpoint = header.active_checkpoint,
        .commit_sequence = active.commit_sequence,
        .page_count = active.page_count,
    };
}

pub fn encodeHeader(out: *[header_size]u8, header: Header) void {
    @memset(out, 0);
    @memcpy(out[magic_offset..][0..magic.len], if (header.packed_records) magic else unpacked_v3_magic);
    std.mem.writeInt(u32, out[version_offset..][0..4], format_version, .little);
    std.mem.writeInt(u32, out[page_size_offset..][0..4], header.page_size, .little);
    std.mem.writeInt(u32, out[header_size_offset..][0..4], header_size, .little);
    out[active_checkpoint_offset] = header.active_checkpoint;

    for (header.checkpoints, 0..) |slot, index| {
        encodeCheckpointSlot(out[checkpointOffset(index)..][0..checkpoint_slot_size], slot);
    }

    std.mem.writeInt(u32, out[header_checksum_offset..][0..4], headerChecksum(out), .little);
}

pub fn decodeHeader(raw: []const u8) !Header {
    if (raw.len < header_size) return error.TruncatedNativeHeader;
    const header_raw = raw[0..header_size];
    const has_packed_records = std.mem.eql(u8, header_raw[magic_offset..][0..magic.len], magic);
    if (!has_packed_records and !std.mem.eql(u8, header_raw[magic_offset..][0..magic.len], unpacked_v3_magic)) return error.InvalidNativeMagic;

    const version = std.mem.readInt(u32, header_raw[version_offset..][0..4], .little);
    if (version != format_version) return error.UnsupportedNativeFormatVersion;

    const encoded_header_size = std.mem.readInt(u32, header_raw[header_size_offset..][0..4], .little);
    if (encoded_header_size != header_size) return error.InvalidNativeHeaderSize;

    const expected_checksum = std.mem.readInt(u32, header_raw[header_checksum_offset..][0..4], .little);
    if (expected_checksum != headerChecksum(header_raw)) return error.NativeHeaderChecksumMismatch;

    const page_size = std.mem.readInt(u32, header_raw[page_size_offset..][0..4], .little);
    if (!validPageSize(page_size)) return error.InvalidNativePageSize;

    const active_hint = header_raw[active_checkpoint_offset];

    var checkpoints: [checkpoint_slot_count]CheckpointSlot = undefined;
    var valid_slots: [checkpoint_slot_count]bool = .{false} ** checkpoint_slot_count;
    for (&checkpoints, 0..) |*slot, index| {
        slot.* = decodeCheckpointSlot(header_raw[checkpointOffset(index)..][0..checkpoint_slot_size]) catch {
            slot.* = .{};
            continue;
        };
        valid_slots[index] = validCheckpointSlot(slot.*);
    }
    const active_checkpoint = try selectActiveCheckpoint(checkpoints, valid_slots, active_hint);

    return .{
        .packed_records = has_packed_records,
        .page_size = page_size,
        .active_checkpoint = active_checkpoint,
        .checkpoints = checkpoints,
    };
}

fn checkpointOffset(index: usize) usize {
    return checkpoint_slots_offset + index * checkpoint_slot_size;
}

fn encodeCheckpointSlot(out: []u8, slot: CheckpointSlot) void {
    std.debug.assert(out.len == checkpoint_slot_size);
    @memset(out, 0);
    std.mem.writeInt(u64, out[0..8], slot.commit_sequence, .little);
    std.mem.writeInt(u64, out[8..16], slot.catalog_root_page, .little);
    std.mem.writeInt(u64, out[16..24], slot.document_root_page, .little);
    std.mem.writeInt(u64, out[24..32], slot.index_catalog_root_page, .little);
    std.mem.writeInt(u64, out[32..40], slot.free_map_root_page, .little);
    std.mem.writeInt(u64, out[40..48], slot.page_count, .little);
    std.mem.writeInt(u64, out[48..56], slot.namespace_directory_root_page, .little);
    std.mem.writeInt(u64, out[56..64], slot.document_index_root_page, .little);
    std.mem.writeInt(u32, out[checkpoint_slot_checksum_offset..][0..4], checkpointSlotChecksum(out), .little);
}

fn decodeCheckpointSlot(raw: []const u8) !CheckpointSlot {
    std.debug.assert(raw.len == checkpoint_slot_size);
    const expected_checksum = std.mem.readInt(u32, raw[checkpoint_slot_checksum_offset..][0..4], .little);
    if (expected_checksum != 0 and expected_checksum != checkpointSlotChecksum(raw)) return error.NativeCheckpointChecksumMismatch;
    return .{
        .commit_sequence = std.mem.readInt(u64, raw[0..8], .little),
        .catalog_root_page = std.mem.readInt(u64, raw[8..16], .little),
        .document_root_page = std.mem.readInt(u64, raw[16..24], .little),
        .index_catalog_root_page = std.mem.readInt(u64, raw[24..32], .little),
        .free_map_root_page = std.mem.readInt(u64, raw[32..40], .little),
        .page_count = std.mem.readInt(u64, raw[40..48], .little),
        .namespace_directory_root_page = std.mem.readInt(u64, raw[48..56], .little),
        .document_index_root_page = std.mem.readInt(u64, raw[56..64], .little),
    };
}

fn validCheckpointSlot(slot: CheckpointSlot) bool {
    if (slot.page_count == 0) return false;
    if (!validCheckpointRoot(slot.catalog_root_page, slot.page_count)) return false;
    if (!validCheckpointRoot(slot.document_root_page, slot.page_count)) return false;
    if (!validCheckpointRoot(slot.index_catalog_root_page, slot.page_count)) return false;
    if (!validCheckpointRoot(slot.free_map_root_page, slot.page_count)) return false;
    if (!validCheckpointRoot(slot.namespace_directory_root_page, slot.page_count)) return false;
    if (!validCheckpointRoot(slot.document_index_root_page, slot.page_count)) return false;
    return true;
}

fn checkpointPrefixSize(slot: CheckpointSlot, page_size: u32) !u64 {
    return std.math.mul(u64, slot.page_count, @as(u64, page_size)) catch error.InvalidNativeCheckpoint;
}

fn validCheckpointRoot(root_page: u64, page_count: u64) bool {
    return root_page == 0 or (physicalPage(root_page) != 0 and physicalPage(root_page) < page_count);
}

fn selectActiveCheckpoint(
    checkpoints: [checkpoint_slot_count]CheckpointSlot,
    valid_slots: [checkpoint_slot_count]bool,
    active_hint: u8,
) !u8 {
    var best: ?u8 = null;
    for (checkpoints, 0..) |slot, index| {
        if (!valid_slots[index]) continue;
        const slot_index: u8 = @intCast(index);
        if (best) |best_index| {
            const best_slot = checkpoints[best_index];
            if (slot.commit_sequence > best_slot.commit_sequence or
                (slot.commit_sequence == best_slot.commit_sequence and slot_index == active_hint))
            {
                best = slot_index;
            }
        } else {
            best = slot_index;
        }
    }
    return best orelse error.InvalidNativeCheckpoint;
}

fn selectCompleteCheckpointForFile(header: Header, file_size: u64) !u8 {
    var best: ?u8 = null;
    var saw_valid_slot = false;
    var saw_invalid_size = false;
    for (header.checkpoints, 0..) |slot, index| {
        if (!validCheckpointSlot(slot)) continue;
        saw_valid_slot = true;
        const expected_size = checkpointPrefixSize(slot, header.page_size) catch {
            saw_invalid_size = true;
            continue;
        };
        if (expected_size > file_size) continue;
        const slot_index: u8 = @intCast(index);
        if (best) |best_index| {
            const best_slot = header.checkpoints[best_index];
            if (slot.commit_sequence > best_slot.commit_sequence or
                (slot.commit_sequence == best_slot.commit_sequence and slot_index == header.active_checkpoint))
            {
                best = slot_index;
            }
        } else {
            best = slot_index;
        }
    }
    if (best) |index| return index;
    if (saw_invalid_size) return error.InvalidNativeCheckpoint;
    return if (saw_valid_slot) error.TruncatedNativeFile else error.InvalidNativeCheckpoint;
}

fn encodePage(out: []u8, kind: PageKind, payload: []const u8) void {
    encodePageParts(out, kind, &.{}, payload);
}

fn encodePageParts(out: []u8, kind: PageKind, prefix: []const u8, payload: []const u8) void {
    const len = prefix.len + payload.len;
    std.debug.assert(out.len >= page_header_size);
    std.debug.assert(len <= out.len - page_header_size);
    @memset(out, 0);
    @memcpy(out[0..page_magic.len], page_magic);
    out[4] = @intFromEnum(kind);
    std.mem.writeInt(u32, out[8..12], @intCast(len), .little);
    @memcpy(out[page_header_size..][0..prefix.len], prefix);
    @memcpy(out[page_header_size + prefix.len ..][0..payload.len], payload);

    var crc = Crc32.init();
    crc.update(out[0..page_crc_offset]);
    crc.update(out[page_header_size..][0..len]);
    std.mem.writeInt(u32, out[page_crc_offset..][0..4], crc.final(), .little);
}

const PackedRecord = struct { kind: PageKind, bytes: []const u8 };

fn packedRecordAtOffset(payload: []const u8, offset: usize) !PackedRecord {
    if (offset > payload.len or payload.len - offset < 4) return error.InvalidNativePageLength;
    const len = std.mem.readInt(u16, payload[offset..][0..2], .little);
    if (len > payload.len - offset - 4 or payload[offset + 3] != 0) return error.InvalidNativePageLength;
    const kind: PageKind = switch (payload[offset + 2]) {
        @intFromEnum(PageKind.catalog) => .catalog,
        @intFromEnum(PageKind.document) => .document,
        else => return error.InvalidNativePageKind,
    };
    return .{ .kind = kind, .bytes = payload[offset + 4 ..][0..len] };
}

fn packedRecordPayload(payload: []const u8, reference: u64) !PackedRecord {
    const wanted: usize = @intCast((reference >> 47) & 0xffff);
    var offset: usize = 0;
    while (offset < payload.len) {
        const record = try packedRecordAtOffset(payload, offset);
        if (offset == wanted) return record;
        offset += 4 + record.bytes.len;
    }
    return error.InvalidPageId;
}

fn unpackRecordPage(raw: []u8, reference: u64) !void {
    const record = try packedRecordPayload(try decodePagePayload(raw, .record_bundle), reference);
    var scratch: [65536]u8 = undefined;
    @memcpy(scratch[0..record.bytes.len], record.bytes);
    encodePage(raw, record.kind, scratch[0..record.bytes.len]);
}

fn decodePagePayloadAlloc(allocator: Allocator, raw: []const u8, expected_kind: PageKind) ![]u8 {
    return try allocator.dupe(u8, try decodePagePayload(raw, expected_kind));
}

fn decodePagePayload(raw: []const u8, expected_kind: PageKind) ![]const u8 {
    if (raw.len < page_header_size) return error.TruncatedNativePage;
    if (!std.mem.eql(u8, raw[0..page_magic.len], page_magic)) return error.InvalidNativePageMagic;
    const kind_raw = raw[4];
    const kind: PageKind = switch (kind_raw) {
        @intFromEnum(PageKind.data) => .data,
        @intFromEnum(PageKind.catalog) => .catalog,
        @intFromEnum(PageKind.document) => .document,
        @intFromEnum(PageKind.value) => .value,
        @intFromEnum(PageKind.free_map) => .free_map,
        @intFromEnum(PageKind.document_index) => .document_index,
        @intFromEnum(PageKind.value_extent) => .value_extent,
        @intFromEnum(PageKind.catalog_index) => .catalog_index,
        @intFromEnum(PageKind.record_bundle) => .record_bundle,
        else => return error.InvalidNativePageKind,
    };
    if (kind != expected_kind) return error.UnexpectedNativePageKind;

    const payload_len = std.mem.readInt(u32, raw[8..12], .little);
    if (payload_len > raw.len - page_header_size) return error.InvalidNativePageLength;

    var crc = Crc32.init();
    crc.update(raw[0..page_crc_offset]);
    crc.update(raw[page_header_size..][0..payload_len]);
    const expected_crc = std.mem.readInt(u32, raw[page_crc_offset..][0..4], .little);
    if (crc.final() != expected_crc) return error.NativePageChecksumMismatch;

    return raw[page_header_size..][0..payload_len];
}

fn encodeCatalogEntry(allocator: Allocator, out: *std.ArrayListUnmanaged(u8), entry: CatalogEntry) !void {
    try encodeCatalogEntryRaw(allocator, out, .{
        .previous_page = entry.previous_page,
        .key = entry.key,
        .value = entry.value,
        .is_delete = entry.is_delete,
        .external_value_root_page = entry.external_value_root_page,
        .external_value_len = if (entry.external_value_root_page != 0 and entry.external_value_len == 0) entry.value.len else entry.external_value_len,
    });
}

fn encodeCatalogEntryRaw(allocator: Allocator, out: *std.ArrayListUnmanaged(u8), entry: EncodedCatalogEntry) !void {
    const value_len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
    if (entry.key.len > catalog_key_len_mask or value_len > std.math.maxInt(u32)) return error.RecordTooLarge;
    if (entry.is_delete and entry.external_value_root_page != 0) return error.InvalidNativeCatalogEntryFlags;
    const external_value = entry.external_value_root_page != 0;
    if (external_value and entry.external_value_len == 0) return error.InvalidNativeValueChain;

    const start = out.items.len;
    const stored_value_len: usize = if (external_value) 8 else value_len;
    try out.resize(allocator, start + 16 + entry.key.len + stored_value_len);
    const encoded = out.items[start..];
    std.mem.writeInt(u64, encoded[0..8], entry.previous_page, .little);
    const key_len_flags: u32 =
        @as(u32, @intCast(entry.key.len)) |
        (if (entry.is_delete) catalog_delete_flag else 0) |
        (if (external_value) catalog_external_value_flag else 0);
    std.mem.writeInt(u32, encoded[8..12], key_len_flags, .little);
    std.mem.writeInt(u32, encoded[12..16], @intCast(value_len), .little);
    @memcpy(encoded[16..][0..entry.key.len], entry.key);
    if (external_value) {
        std.mem.writeInt(u64, encoded[16 + entry.key.len ..][0..8], entry.external_value_root_page, .little);
    } else {
        @memcpy(encoded[16 + entry.key.len ..][0..entry.value.len], entry.value);
    }
}

fn decodeCatalogEntry(raw: []const u8) !CatalogEntry {
    if (raw.len < 16) return error.TruncatedNativeCatalogEntry;
    const previous_page = std.mem.readInt(u64, raw[0..8], .little);
    const key_len_flags = std.mem.readInt(u32, raw[8..12], .little);
    const flags = key_len_flags & ~catalog_key_len_mask;
    if (flags & ~(catalog_delete_flag | catalog_external_value_flag) != 0) return error.InvalidNativeCatalogEntryFlags;
    const is_delete = flags & catalog_delete_flag != 0;
    const external_value = flags & catalog_external_value_flag != 0;
    if (is_delete and external_value) return error.InvalidNativeCatalogEntryFlags;

    const key_len = key_len_flags & catalog_key_len_mask;
    const value_len = std.mem.readInt(u32, raw[12..16], .little);
    const stored_value_len: u64 = if (external_value) 8 else value_len;
    const payload_len = @as(u64, key_len) + stored_value_len;
    if (payload_len > raw.len - 16) return error.TruncatedNativeCatalogEntry;
    const key_start: usize = 16;
    const key_end = key_start + @as(usize, @intCast(key_len));
    const stored_value_end = key_end + @as(usize, @intCast(stored_value_len));
    const external_value_root_page = if (external_value) blk: {
        if (value_len == 0) return error.InvalidNativeValueChain;
        const root = std.mem.readInt(u64, raw[key_end..][0..8], .little);
        if (root == 0) return error.InvalidNativeValueChain;
        break :blk root;
    } else 0;
    return .{
        .previous_page = previous_page,
        .key = raw[key_start..key_end],
        .value = if (external_value) raw[key_end..key_end] else raw[key_end..stored_value_end],
        .is_delete = is_delete,
        .external_value_root_page = external_value_root_page,
        .external_value_len = if (external_value) @intCast(value_len) else 0,
    };
}

fn encodeDocumentEntry(allocator: Allocator, out: *std.ArrayListUnmanaged(u8), entry: DocumentEntry) !void {
    const value_len = if (entry.external_value_root_page != 0 and entry.external_value_len != 0) entry.external_value_len else entry.value.len;
    if (entry.key.len > std.math.maxInt(u32) or value_len > std.math.maxInt(u32)) return error.RecordTooLarge;
    if (entry.is_delete and entry.external_value_root_page != 0) return error.InvalidNativeDocumentEntryFlags;
    const external_value = entry.external_value_root_page != 0;
    if (external_value and value_len == 0) return error.InvalidNativeValueChain;

    const start = out.items.len;
    const stored_value_len: usize = if (external_value) 8 else value_len;
    const header_len: usize = 28;
    try out.resize(allocator, start + header_len + entry.key.len + stored_value_len);
    const encoded = out.items[start..];
    std.mem.writeInt(u64, encoded[0..8], entry.previous_page, .little);
    encoded[8] =
        (if (entry.is_delete) document_delete_flag else 0) |
        (if (external_value) document_external_value_flag else 0) |
        document_namespace_link_flag;
    @memset(encoded[9..12], 0);
    std.mem.writeInt(u32, encoded[12..16], @intCast(entry.key.len), .little);
    std.mem.writeInt(u32, encoded[16..20], @intCast(value_len), .little);
    std.mem.writeInt(u64, encoded[20..28], entry.previous_namespace_page, .little);
    @memcpy(encoded[header_len..][0..entry.key.len], entry.key);
    if (external_value) {
        std.mem.writeInt(u64, encoded[header_len + entry.key.len ..][0..8], entry.external_value_root_page, .little);
    } else {
        @memcpy(encoded[header_len + entry.key.len ..][0..entry.value.len], entry.value);
    }
}

fn decodeDocumentEntry(raw: []const u8) !DocumentEntry {
    if (raw.len < 20) return error.TruncatedNativeDocumentEntry;
    const previous_page = std.mem.readInt(u64, raw[0..8], .little);
    const flags = raw[8];
    if (flags & ~(document_delete_flag | document_external_value_flag | document_namespace_link_flag) != 0) return error.InvalidNativeDocumentEntryFlags;
    const is_delete = flags & document_delete_flag != 0;
    const external_value = flags & document_external_value_flag != 0;
    const has_namespace_link = flags & document_namespace_link_flag != 0;
    if (!has_namespace_link) return error.InvalidNativeDocumentEntryFlags;
    if (is_delete and external_value) return error.InvalidNativeDocumentEntryFlags;

    const key_len = std.mem.readInt(u32, raw[12..16], .little);
    const value_len = std.mem.readInt(u32, raw[16..20], .little);
    const stored_value_len: u64 = if (external_value) 8 else value_len;
    const header_len: usize = 28;
    if (raw.len < header_len) return error.TruncatedNativeDocumentEntry;
    const payload_len = @as(u64, key_len) + stored_value_len;
    if (payload_len > raw.len - header_len) return error.TruncatedNativeDocumentEntry;
    const key_start: usize = header_len;
    const key_end = key_start + @as(usize, @intCast(key_len));
    const stored_value_end = key_end + @as(usize, @intCast(stored_value_len));
    const external_value_root_page = if (external_value) blk: {
        if (value_len == 0) return error.InvalidNativeValueChain;
        const root = std.mem.readInt(u64, raw[key_end..][0..8], .little);
        if (root == 0) return error.InvalidNativeValueChain;
        break :blk root;
    } else 0;
    return .{
        .previous_page = previous_page,
        .previous_namespace_page = std.mem.readInt(u64, raw[20..28], .little),
        .key = raw[key_start..key_end],
        .value = if (external_value) raw[key_end..key_end] else raw[key_end..stored_value_end],
        .is_delete = is_delete,
        .external_value_root_page = external_value_root_page,
        .external_value_len = if (external_value) @intCast(value_len) else 0,
    };
}

fn encodedDocumentIndexNodeSize(node: DocumentIndexNode) !usize {
    if (node.keys.len > std.math.maxInt(u16)) return error.RecordTooLarge;
    if ((node.kind == .leaf and node.pointers.len != node.keys.len) or
        (node.kind == .internal and node.pointers.len != node.keys.len + 1))
        return error.InvalidDocumentIndex;
    const internal_header_size: usize = if (node.kind == .internal) @sizeOf(u64) else 0;
    var size: usize = document_index_header_size + internal_header_size;
    for (node.keys, 0..) |key, i| {
        if (key.len > std.math.maxInt(u16)) return error.RecordTooLarge;
        size = try std.math.add(usize, size, @sizeOf(u16) + @sizeOf(u64) + (if (key.len > index_inline_key_limit or (node.key_pages != null and node.key_pages.?[i] != 0)) @as(usize, 8) else key.len));
    }
    return size;
}

fn lowerBoundIndexKeys(keys: []const []u8, key: []const u8) usize {
    var low: usize = 0;
    var high = keys.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (std.mem.order(u8, keys[mid], key) == .lt)
            low = mid + 1
        else
            high = mid;
    }
    return low;
}

fn upperBoundIndexKeys(keys: []const []u8, key: []const u8) usize {
    var low: usize = 0;
    var high = keys.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (std.mem.order(u8, keys[mid], key) != .gt)
            low = mid + 1
        else
            high = mid;
    }
    return low;
}

fn encodeDocumentIndexNode(allocator: Allocator, node: DocumentIndexNode) ![]u8 {
    const size = try encodedDocumentIndexNodeSize(node);
    const out = try allocator.alloc(u8, size);
    errdefer allocator.free(out);
    @memcpy(out[0..document_index_magic.len], document_index_magic);
    out[8] = @intFromEnum(node.kind);
    out[9] = 0;
    std.mem.writeInt(u16, out[10..12], @intCast(node.keys.len), .little);
    var pos: usize = document_index_header_size;
    if (node.kind == .internal) {
        std.mem.writeInt(u64, out[pos..][0..8], node.pointers[0], .little);
        pos += 8;
    }
    for (node.keys, 0..) |key, i| {
        const external = key.len > index_inline_key_limit or (node.key_pages != null and node.key_pages.?[i] != 0);
        std.mem.writeInt(u16, out[pos..][0..2], if (external) index_external_key_marker else @intCast(key.len), .little);
        pos += 2;
        const pointer_index = if (node.kind == .leaf) i else i + 1;
        std.mem.writeInt(u64, out[pos..][0..8], node.pointers[pointer_index], .little);
        pos += 8;
        if (external) {
            const pages = node.key_pages orelse return error.InvalidDocumentIndex;
            if (pages[i] == 0) return error.InvalidDocumentIndex;
            std.mem.writeInt(u64, out[pos..][0..8], pages[i], .little);
            pos += 8;
        } else {
            @memcpy(out[pos..][0..key.len], key);
            pos += key.len;
        }
    }
    std.debug.assert(pos == out.len);
    return out;
}

const IndexProbe = struct { leaf: bool, page: ?u64 };

fn probeDocumentIndexNode(raw: []const u8, key: []const u8) !IndexProbe {
    if (raw.len < document_index_header_size or !std.mem.eql(u8, raw[0..8], document_index_magic) or raw[9] != 0)
        return error.InvalidDocumentIndex;
    const leaf = switch (raw[8]) {
        1 => true,
        2 => false,
        else => return error.InvalidDocumentIndex,
    };
    const count = std.mem.readInt(u16, raw[10..12], .little);
    if (leaf and count == 0) return error.InvalidDocumentIndex;
    var pos: usize = document_index_header_size;
    var selected: ?u64 = null;
    if (!leaf) {
        if (pos + 8 > raw.len) return error.InvalidDocumentIndex;
        selected = std.mem.readInt(u64, raw[pos..][0..8], .little);
        pos += 8;
    }
    var previous: ?[]const u8 = null;
    for (0..count) |_| {
        if (pos + 10 > raw.len) return error.InvalidDocumentIndex;
        const len = std.mem.readInt(u16, raw[pos..][0..2], .little);
        const pointer = std.mem.readInt(u64, raw[pos + 2 ..][0..8], .little);
        pos += 10;
        if (len == index_external_key_marker) return error.ExternalIndexKey;
        if (len > index_inline_key_limit or len > raw.len - pos) return error.InvalidDocumentIndex;
        const candidate = raw[pos..][0..len];
        if (previous) |prev| if (std.mem.order(u8, prev, candidate) != .lt) return error.InvalidDocumentIndex;
        previous = candidate;
        const order = std.mem.order(u8, key, candidate);
        if ((leaf and order == .eq) or (!leaf and order != .lt)) selected = pointer;
        pos += len;
    }
    if (pos != raw.len) return error.InvalidDocumentIndex;
    return .{ .leaf = leaf, .page = selected };
}

fn decodeDocumentIndexNode(allocator: Allocator, raw: []const u8) !DocumentIndexNode {
    if (raw.len < document_index_header_size or !std.mem.eql(u8, raw[0..8], document_index_magic))
        return error.InvalidDocumentIndex;
    const kind: DocumentIndexNodeKind = switch (raw[8]) {
        @intFromEnum(DocumentIndexNodeKind.leaf) => .leaf,
        @intFromEnum(DocumentIndexNodeKind.internal) => .internal,
        else => return error.InvalidDocumentIndex,
    };
    if (raw[9] != 0) return error.InvalidDocumentIndex;
    const count: usize = std.mem.readInt(u16, raw[10..12], .little);
    if (kind == .leaf and count == 0) return error.InvalidDocumentIndex;
    const keys = try allocator.alloc([]u8, count);
    var keys_initialized: usize = 0;
    errdefer {
        for (keys[0..keys_initialized]) |key| allocator.free(key);
        allocator.free(keys);
    }
    const key_pages = try allocator.alloc(u64, count);
    errdefer allocator.free(key_pages);
    @memset(key_pages, 0);
    const pointer_extra: usize = if (kind == .internal) 1 else 0;
    const pointers = try allocator.alloc(u64, count + pointer_extra);
    errdefer allocator.free(pointers);
    var pos: usize = document_index_header_size;
    if (kind == .internal) {
        if (pos + 8 > raw.len) return error.InvalidDocumentIndex;
        pointers[0] = std.mem.readInt(u64, raw[pos..][0..8], .little);
        pos += 8;
    }
    for (keys, 0..) |*key, i| {
        if (pos + 10 > raw.len) return error.InvalidDocumentIndex;
        const key_len: usize = std.mem.readInt(u16, raw[pos..][0..2], .little);
        pos += 2;
        const pointer_index = if (kind == .leaf) i else i + 1;
        pointers[pointer_index] = std.mem.readInt(u64, raw[pos..][0..8], .little);
        pos += 8;
        if (key_len == index_external_key_marker) {
            if (pos + 8 > raw.len) return error.InvalidDocumentIndex;
            key_pages[i] = std.mem.readInt(u64, raw[pos..][0..8], .little);
            if (key_pages[i] == 0) return error.InvalidDocumentIndex;
            key.* = try allocator.alloc(u8, 0);
            pos += 8;
        } else {
            if (key_len > index_inline_key_limit or pos + key_len > raw.len) return error.InvalidDocumentIndex;
            key.* = try allocator.dupe(u8, raw[pos .. pos + key_len]);
            pos += key_len;
        }
        keys_initialized += 1;
        if (i > 0 and key_pages[i - 1] == 0 and key_pages[i] == 0 and std.mem.order(u8, keys[i - 1], key.*) != .lt) return error.InvalidDocumentIndex;
    }
    if (pos != raw.len) return error.InvalidDocumentIndex;
    return .{ .kind = kind, .keys = keys, .pointers = pointers, .key_pages = key_pages };
}

fn decodeValuePage(raw: []const u8) !ValuePage {
    if (raw.len < value_page_header_size) return error.TruncatedNativeValuePage;
    return .{
        .next_page = std.mem.readInt(u64, raw[0..8], .little),
        .chunk = raw[value_page_header_size..],
    };
}

fn encodeFreeMapAlloc(allocator: Allocator, page_size: u32, covered_page_count: u64, free_pages: []const u64) ![]u8 {
    if (free_pages.len > maxFreeMapEntries(page_size)) return error.NativeFreeMapTooLarge;
    if (free_pages.len > std.math.maxInt(u32)) return error.NativeFreeMapTooLarge;

    var previous_page_id: u64 = 0;
    for (free_pages) |page_id| {
        if (page_id == 0 or page_id >= covered_page_count) return error.InvalidNativeFreeMap;
        if (page_id <= previous_page_id) return error.InvalidNativeFreeMap;
        previous_page_id = page_id;
    }

    const payload = try allocator.alloc(u8, free_map_header_size + free_pages.len * 8);
    errdefer allocator.free(payload);
    std.mem.writeInt(u32, payload[0..4], free_map_format_version, .little);
    std.mem.writeInt(u32, payload[4..8], @intCast(free_pages.len), .little);
    std.mem.writeInt(u64, payload[8..16], covered_page_count, .little);
    for (free_pages, 0..) |page_id, index| {
        const offset = free_map_header_size + index * 8;
        std.mem.writeInt(u64, payload[offset..][0..8], page_id, .little);
    }
    return payload;
}

fn decodeFreeMapAlloc(allocator: Allocator, raw: []const u8, checkpoint_page_count: u64) !FreeMap {
    if (raw.len < free_map_header_size) return error.TruncatedNativeFreeMap;
    const version = std.mem.readInt(u32, raw[0..4], .little);
    if (version != free_map_format_version) return error.InvalidNativeFreeMap;
    const free_page_count = std.mem.readInt(u32, raw[4..8], .little);
    const covered_page_count = std.mem.readInt(u64, raw[8..16], .little);
    if (covered_page_count != checkpoint_page_count) return error.InvalidNativeFreeMap;
    const expected_len = free_map_header_size + @as(usize, free_page_count) * 8;
    if (raw.len != expected_len) return error.InvalidNativeFreeMap;

    const free_pages = try allocator.alloc(u64, free_page_count);
    errdefer allocator.free(free_pages);
    var previous_page_id: u64 = 0;
    for (free_pages, 0..) |*page_id, index| {
        const offset = free_map_header_size + index * 8;
        page_id.* = std.mem.readInt(u64, raw[offset..][0..8], .little);
        if (page_id.* == 0 or page_id.* >= checkpoint_page_count) return error.InvalidNativeFreeMap;
        if (page_id.* <= previous_page_id) return error.InvalidNativeFreeMap;
        previous_page_id = page_id.*;
    }
    return .{
        .covered_page_count = covered_page_count,
        .free_pages = free_pages,
    };
}

fn maxFreeMapEntries(page_size: u32) usize {
    std.debug.assert(page_size >= page_header_size + free_map_header_size);
    return (@as(usize, @intCast(page_size)) - page_header_size - free_map_header_size) / 8;
}

fn readExactAt(file: std.Io.File, io: std.Io, out: []u8, offset: u64) !void {
    const read = try file.readPositionalAll(io, out, offset);
    if (read != out.len) return error.EndOfStream;
}

fn readHeaderExactAt(file: std.Io.File, io: std.Io, out: *[header_size]u8) !void {
    const read = try file.readPositionalAll(io, out, 0);
    if (read != header_size) return error.TruncatedNativeHeader;
}

fn headerChecksum(raw: []const u8) u32 {
    var crc = Crc32.init();
    crc.update(raw[0..active_checkpoint_offset]);
    crc.update(raw[active_checkpoint_offset + 1 .. checkpoint_slots_offset]);
    crc.update(raw[checkpoint_slots_end..header_checksum_offset]);
    return crc.final();
}

fn checkpointSlotChecksum(raw: []const u8) u32 {
    var crc = Crc32.init();
    crc.update(raw[0..checkpoint_slot_payload_size]);
    return crc.final();
}

fn validPageSize(page_size: u32) bool {
    return page_size >= 4096 and page_size <= 65536 and std.math.isPowerOfTwo(page_size);
}

fn issueForDecodeError(err: anyerror) []const u8 {
    return switch (err) {
        error.TruncatedNativeHeader => "truncated_header",
        error.InvalidNativeMagic => "invalid_magic",
        error.UnsupportedNativeFormatVersion => "unsupported_format_version",
        error.InvalidNativeHeaderSize => "invalid_header_size",
        error.NativeHeaderChecksumMismatch => "header_checksum_mismatch",
        error.InvalidNativePageSize => "invalid_page_size",
        error.InvalidNativeCheckpointSlot => "invalid_checkpoint_slot",
        error.NativeCheckpointChecksumMismatch => "checkpoint_checksum_mismatch",
        error.InvalidNativeCheckpoint => "invalid_checkpoint",
        else => "invalid_header",
    };
}

fn issueForPageCheckError(err: anyerror) []const u8 {
    return switch (err) {
        error.InvalidPageId => "invalid_page_id",
        error.TruncatedNativePage => "truncated_page",
        error.InvalidNativePageMagic => "invalid_page_magic",
        error.InvalidNativePageKind => "invalid_page_kind",
        error.UnexpectedNativePageKind => "unexpected_page_kind",
        error.InvalidNativePageLength => "invalid_page_length",
        error.NativePageChecksumMismatch => "page_checksum_mismatch",
        error.TruncatedNativeCatalogEntry => "truncated_catalog_entry",
        error.InvalidNativeCatalogEntryFlags => "invalid_catalog_entry_flags",
        error.TruncatedNativeDocumentEntry => "truncated_document_entry",
        error.InvalidNativeDocumentEntryFlags => "invalid_document_entry_flags",
        error.InvalidNamespaceDirectory => "invalid_namespace_directory",
        error.InvalidDocumentIndex,
        error.InvalidDocumentIndexOrder,
        => "invalid_document_index",
        error.InvalidNativePageChain => "invalid_page_chain",
        error.TruncatedNativeValuePage => "truncated_value_page",
        error.InvalidNativeValueChain => "invalid_value_chain",
        error.TruncatedNativeFreeMap,
        error.InvalidNativeFreeMap,
        error.UnsupportedNativeFreeMap,
        => "invalid_free_map",
        else => "invalid_page",
    };
}

fn invalidCheck(report: CheckReport, issue: []const u8) CheckReport {
    var invalid = report;
    invalid.valid = false;
    invalid.issue = issue;
    return invalid;
}

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

fn readHeaderForTest(path: []const u8) ![header_size]u8 {
    var header_bytes: [header_size]u8 = undefined;
    var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_only });
    defer file.close(std.testing.io);
    try readExactAt(file, std.testing.io, &header_bytes, 0);
    return header_bytes;
}

test "lite native header round trips initial checkpoint" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});

    const header = try decodeHeader(&encoded);
    try std.testing.expectEqual(default_page_size, header.page_size);
    try std.testing.expectEqual(@as(u8, 0), header.active_checkpoint);
    try std.testing.expectEqual(@as(u64, 0), header.checkpoints[0].commit_sequence);
    try std.testing.expectEqual(@as(u64, 1), header.checkpoints[0].page_count);

    const report = inspectBytes(&encoded);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(format_version, report.format_version);
    try std.testing.expectEqual(@as(u64, 1), report.page_count);
}

test "lite native header rejects corrupted checksum" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});
    encoded[page_size_offset] ^= 0xff;

    const report = inspectBytes(&encoded);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("header_checksum_mismatch", report.issue.?);
}

test "lite native header rejects unsupported format version" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});
    std.mem.writeInt(u32, encoded[version_offset..][0..4], format_version + 1, .little);

    const report = inspectBytes(&encoded);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("unsupported_format_version", report.issue.?);
    try std.testing.expectError(error.UnsupportedNativeFormatVersion, decodeHeader(&encoded));
}

test "lite native header selects newest valid checkpoint slot" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{
        .active_checkpoint = 0,
        .checkpoints = .{
            .{ .commit_sequence = 1, .page_count = 2 },
            .{ .commit_sequence = 2, .page_count = 3 },
        },
    });

    const header = try decodeHeader(&encoded);
    try std.testing.expectEqual(@as(u8, 1), header.active_checkpoint);

    const report = inspectBytes(&encoded);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u8, 1), report.active_checkpoint);
    try std.testing.expectEqual(@as(u64, 2), report.commit_sequence);
    try std.testing.expectEqual(@as(u64, 3), report.page_count);
}

test "lite native header recovers from corrupted active checkpoint hint" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{
        .active_checkpoint = 1,
        .checkpoints = .{
            .{ .commit_sequence = 1, .page_count = 2 },
            .{ .commit_sequence = 2, .page_count = 3 },
        },
    });
    encoded[active_checkpoint_offset] = 0xff;

    const header = try decodeHeader(&encoded);
    try std.testing.expectEqual(@as(u8, 1), header.active_checkpoint);
    try std.testing.expectEqual(@as(u64, 2), header.checkpoints[header.active_checkpoint].commit_sequence);
}

test "lite native header recovers previous checkpoint from a checksum-bad slot" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{
        .active_checkpoint = 1,
        .checkpoints = .{
            .{ .commit_sequence = 1, .page_count = 2 },
            .{ .commit_sequence = 2, .page_count = 3 },
        },
    });
    encoded[checkpointOffset(1)] ^= 0xff;

    const header = try decodeHeader(&encoded);
    try std.testing.expectEqual(@as(u8, 0), header.active_checkpoint);
    try std.testing.expectEqual(@as(u64, 1), header.checkpoints[header.active_checkpoint].commit_sequence);
}

test "lite native create writes inspectable aflite file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native.aflite");
    defer allocator.free(path);

    try create(std.testing.io, path);
    const report = try inspect(allocator, std.testing.io, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(format_version, report.format_version);
    try std.testing.expectEqual(default_page_size, report.page_size);
    try std.testing.expectEqual(@as(u8, 0), report.active_checkpoint);
    try std.testing.expectEqual(@as(u64, 0), report.commit_sequence);
    try std.testing.expectEqual(@as(u64, 1), report.page_count);
}

test "lite native open options propagate no_sync to file writes" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-no-sync.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.createWithOptions(allocator, path, .{ .no_sync = true });
        defer file.close();
        try std.testing.expect(file.no_sync);
        try file.putDocument("doc:no-sync", "value");
    }

    {
        var reopened = try NativeFile.openWithOptions(allocator, path, .{
            .read_only = true,
            .no_sync = true,
        });
        defer reopened.close();
        try std.testing.expect(reopened.no_sync);

        const value = (try reopened.getDocumentAlloc(allocator, "doc:no-sync")) orelse return error.TestExpectedEqual;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("value", value);
    }
}

test "lite native createNew rejects existing aflite without truncating" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-create-new-existing.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:keep", "survives");
    }

    try std.testing.expectError(error.PathAlreadyExists, NativeFile.createNew(allocator, path));

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const value = (try reopened.getDocumentAlloc(allocator, "doc:keep")) orelse return error.TestExpectedEqual;
    defer allocator.free(value);
    try std.testing.expectEqualStrings("survives", value);
}

test "lite native recreate atomically replaces the generation pinned by readers" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-recreate-pinned-reader.aflite");
    defer allocator.free(path);

    {
        var original = try NativeFile.create(allocator, path);
        defer original.close();
        try original.putDocument("doc:old", "pinned");
    }

    var pinned = try NativeFile.open(allocator, path, true);
    defer pinned.close();

    {
        var replacement = try NativeFile.create(allocator, path);
        defer replacement.close();
        try std.testing.expectEqual(@as(u64, 0), replacement.activeCheckpoint().commit_sequence);
    }

    const old_value = (try pinned.getDocumentAlloc(allocator, "doc:old")) orelse return error.TestExpectedEqual;
    defer allocator.free(old_value);
    try std.testing.expectEqualStrings("pinned", old_value);

    var current = try NativeFile.open(allocator, path, true);
    defer current.close();
    try std.testing.expect((try current.getDocumentAlloc(allocator, "doc:old")) == null);
}

test "lite native recreate through symlink preserves canonical lock identity" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const target_path = try testPath(allocator, tmp, "native-recreate-symlink-target.aflite");
    defer allocator.free(target_path);
    const alias_path = try testPath(allocator, tmp, "native-recreate-symlink-alias.aflite");
    defer allocator.free(alias_path);

    {
        var original = try NativeFile.create(allocator, target_path);
        defer original.close();
        try original.putDocument("doc:old", "replaced");
    }
    const canonical_target = try realPathAlloc(allocator, std.testing.io, target_path);
    defer allocator.free(canonical_target);
    try std.Io.Dir.cwd().symLink(std.testing.io, canonical_target, alias_path, .{});

    {
        var replacement = try NativeFile.create(allocator, alias_path);
        defer replacement.close();
        try std.testing.expectEqual(@as(u64, 0), replacement.activeCheckpoint().commit_sequence);
    }

    const canonical_alias = try realPathAlloc(allocator, std.testing.io, alias_path);
    defer allocator.free(canonical_alias);
    try std.testing.expectEqualStrings(canonical_target, canonical_alias);

    var current = try NativeFile.open(allocator, target_path, true);
    defer current.close();
    try std.testing.expect((try current.getDocumentAlloc(allocator, "doc:old")) == null);
}

test "lite native open rejects unsupported format version" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-unsupported-version.aflite");
    defer allocator.free(path);

    try create(std.testing.io, path);
    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        var raw_version: [4]u8 = undefined;
        std.mem.writeInt(u32, &raw_version, format_version + 1, .little);
        try file.writePositionalAll(std.testing.io, &raw_version, version_offset);
    }

    try std.testing.expectError(error.UnsupportedNativeFormatVersion, NativeFile.open(allocator, path, true));
}

test "lite native open rejects short files as truncated native headers" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-short-header.aflite");
    defer allocator.free(path);

    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{});
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "not enough header bytes", 0);
    }

    try std.testing.expectError(error.TruncatedNativeHeader, NativeFile.open(allocator, path, true));
    try std.testing.expectError(error.TruncatedNativeHeader, inspect(allocator, std.testing.io, path));
}

test "lite native inspect reads only the header page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-with-pages.aflite");
    defer allocator.free(path);

    try create(std.testing.io, path);
    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{ .truncate = false });
        defer file.close(std.testing.io);
        const size = (try file.stat(std.testing.io)).size;
        var writer = file.writer(std.testing.io, &.{});
        try writer.seekTo(size);
        try writer.interface.writeAll("future-page-data");
        try writer.end();
    }

    const report = try inspect(allocator, std.testing.io, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(format_version, report.format_version);
}

test "lite native file appends page and publishes checkpoint" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-pages.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        const page_id = try file.allocatePage("hello native page");
        try std.testing.expectEqual(@as(u64, 1), page_id);
        try std.testing.expectEqual(@as(u64, 1), file.activeCheckpoint().commit_sequence);
        try std.testing.expectEqual(@as(u64, 3), file.activeCheckpoint().page_count);
        try std.testing.expectEqual(@as(u64, 2), file.activeCheckpoint().free_map_root_page);

        const page = try file.readPagePayloadAlloc(allocator, page_id);
        defer allocator.free(page);
        try std.testing.expectEqualStrings("hello native page", page);
    }

    const report = try inspect(allocator, std.testing.io, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.commit_sequence);
    try std.testing.expectEqual(@as(u64, 3), report.page_count);
}

test "lite native file reopens allocated pages" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-reopen.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        _ = try file.allocatePage("persisted");
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 1), reopened.activeCheckpoint().commit_sequence);
    try std.testing.expectEqual(@as(u64, 3), reopened.activeCheckpoint().page_count);
    const page = try reopened.readPagePayloadAlloc(allocator, 1);
    defer allocator.free(page);
    try std.testing.expectEqualStrings("persisted", page);
    try std.testing.expectError(error.ReadOnly, reopened.allocatePage("nope"));
}

test "lite native file publishes checkpoint without rewriting static header" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-slot-publish.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        var before: [header_size]u8 = undefined;
        try readExactAt(file.file, file.io_impl.io(), &before, 0);
        _ = try file.allocatePage("slot-only publish");
        var after: [header_size]u8 = undefined;
        try readExactAt(file.file, file.io_impl.io(), &after, 0);

        try std.testing.expectEqualSlices(u8, before[0..active_checkpoint_offset], after[0..active_checkpoint_offset]);
        try std.testing.expectEqualSlices(u8, before[active_checkpoint_offset + 1 .. checkpoint_slots_offset], after[active_checkpoint_offset + 1 .. checkpoint_slots_offset]);
        try std.testing.expectEqualSlices(u8, before[checkpointOffset(0)..][0..checkpoint_slot_size], after[checkpointOffset(0)..][0..checkpoint_slot_size]);
        try std.testing.expectEqualSlices(u8, before[checkpoint_slots_end..header_size], after[checkpoint_slots_end..header_size]);
        try std.testing.expectEqual(@as(u8, 1), after[active_checkpoint_offset]);
        try std.testing.expect(!std.mem.eql(u8, before[checkpointOffset(1)..][0..checkpoint_slot_size], after[checkpointOffset(1)..][0..checkpoint_slot_size]));
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 1), reopened.activeCheckpoint().commit_sequence);
    try std.testing.expectEqual(@as(u64, 3), reopened.activeCheckpoint().page_count);
}

test "lite native file recovers older complete checkpoint when newest prefix is truncated" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-truncated-newest-checkpoint.aflite");
    defer allocator.free(path);

    const stable_size = blk: {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        try file.putDocument("doc:recover", "stable");
        const stable_checkpoint = file.activeCheckpoint();
        const stable_size = try checkpointPrefixSize(stable_checkpoint, file.header.page_size);

        try file.putDocument("doc:recover", "newer");
        try std.testing.expect(file.activeCheckpoint().commit_sequence > stable_checkpoint.commit_sequence);
        try std.testing.expect(try checkpointPrefixSize(file.activeCheckpoint(), file.header.page_size) > stable_size);
        break :blk stable_size;
    };

    {
        var raw = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer raw.close(std.testing.io);
        try raw.setLength(std.testing.io, stable_size);
        try raw.sync(std.testing.io);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 1), reopened.activeCheckpoint().commit_sequence);
    const value = (try reopened.getDocumentAlloc(allocator, "doc:recover")).?;
    defer allocator.free(value);
    try std.testing.expectEqualStrings("stable", value);

    const report = try checkFile(allocator, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.record_count);
    try std.testing.expectEqual(@as(u64, 0), report.tail_bytes);
}

test "lite native file permits concurrent readers" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-reader-locks.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        _ = try file.allocatePage("persisted");
    }

    var reader_a = try NativeFile.open(allocator, path, true);
    defer reader_a.close();

    var reader_b = try NativeFile.open(allocator, path, true);
    defer reader_b.close();

    try std.testing.expectEqual(@as(u64, 3), reader_a.activeCheckpoint().page_count);
    try std.testing.expectEqual(@as(u64, 3), reader_b.activeCheckpoint().page_count);
}

test "lite native file active writer permits readers but blocks second writer" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-writer-lock.aflite");
    defer allocator.free(path);

    var writer = try NativeFile.create(allocator, path);
    defer writer.close();
    _ = try writer.allocatePage("committed before reader");

    try std.testing.expectError(error.WouldBlock, NativeFile.open(allocator, path, false));

    var reader = try NativeFile.open(allocator, path, true);
    defer reader.close();
    try std.testing.expectEqual(@as(u64, 3), reader.activeCheckpoint().page_count);

    const report = try inspect(allocator, std.testing.io, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.commit_sequence);

    try std.testing.expectError(error.WouldBlock, writer.vacuum());
}

test "lite native file canonicalizes writer lock path spellings" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-writer-lock-canonical.aflite");
    defer allocator.free(path);

    var writer = try NativeFile.create(allocator, path);
    defer writer.close();

    const alternate_path = try std.fmt.allocPrint(allocator, "./{s}", .{path});
    defer allocator.free(alternate_path);
    try std.testing.expectError(error.WouldBlock, NativeFile.open(allocator, alternate_path, false));
}

test "lite native file detects corrupted page payload" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-corrupt-page.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        _ = try file.allocatePage("checksum");
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectError(error.NativePageChecksumMismatch, reopened.readPagePayloadAlloc(allocator, 1));
}

test "lite native catalog stores and reopens records" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-catalog.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putCatalogRecord("schema", "{\"version\":1}");
        try file.putCatalogRecord("index:text", "ready");
        try file.putCatalogRecord("schema", "{\"version\":2}");
        try std.testing.expectEqual(@as(u64, 3), file.activeCheckpoint().commit_sequence);
        try std.testing.expect(file.activeCheckpoint().catalog_root_page != 0);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const schema = (try reopened.getCatalogRecordAlloc(allocator, "schema")).?;
    defer allocator.free(schema);
    try std.testing.expectEqualStrings("{\"version\":2}", schema);

    const index = (try reopened.getCatalogRecordAlloc(allocator, "index:text")).?;
    defer allocator.free(index);
    try std.testing.expectEqualStrings("ready", index);

    try std.testing.expectEqual(@as(?[]u8, null), try reopened.getCatalogRecordAlloc(allocator, "missing"));
}

test "lite native catalog supports tombstones and spilled values" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-catalog-large.aflite");
    defer allocator.free(path);

    const large = try allocator.alloc(u8, default_page_size * 3);
    defer allocator.free(large);
    for (large, 0..) |*byte, i| byte.* = @intCast(i % 251);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putCatalogRecord("index:large", large);
        try file.putCatalogRecord("index:gone", "delete me");
        try file.deleteCatalogRecord("index:gone");
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const got = (try reopened.getCatalogRecordAlloc(allocator, "index:large")).?;
    defer allocator.free(got);
    try std.testing.expectEqualSlices(u8, large, got);
    try std.testing.expectEqual(@as(?[]u8, null), try reopened.getCatalogRecordAlloc(allocator, "index:gone"));

    const records = try reopened.snapshotCatalogRecordsAlloc(allocator);
    defer NativeFile.freeSnapshotCatalogRecords(allocator, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqualStrings("index:large", records[0].key);
    try std.testing.expectEqualSlices(u8, large, records[0].value);

    const report = try reopened.check();
    try std.testing.expect(report.valid);
}

test "lite native index catalog snapshots live keys without values" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-index-catalog-keys.aflite");
    defer allocator.free(path);

    const large = try allocator.alloc(u8, default_page_size * 2);
    defer allocator.free(large);
    @memset(large, 'x');

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putIndexCatalogRecord("/index/b.tbl", large);
        try file.putIndexCatalogRecord("/index/a.tbl", "small");
        try file.putIndexCatalogRecord("/index/deleted.tbl", "gone");
        try file.deleteIndexCatalogRecord("/index/deleted.tbl");
        try file.putIndexCatalogRecord("/index/a.tbl", "newer");
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const keys = try reopened.snapshotIndexCatalogKeysAlloc(allocator);
    defer NativeFile.freeSnapshotCatalogKeys(allocator, keys);
    try std.testing.expectEqual(@as(usize, 2), keys.len);
    try std.testing.expectEqualStrings("/index/a.tbl", keys[0].key);
    try std.testing.expectEqualStrings("/index/b.tbl", keys[1].key);
}

test "lite native catalog detects corrupted root page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-catalog-corrupt.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putCatalogRecord("schema", "value");
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectError(error.NativePageChecksumMismatch, reopened.getCatalogRecordAlloc(allocator, "schema"));
}

test "lite native document store persists records" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-documents.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "{\"title\":\"one\"}");
        try file.putDocument("doc:2", "{\"title\":\"two\"}");
        try std.testing.expectEqual(@as(u64, 2), file.activeCheckpoint().commit_sequence);
        try std.testing.expect(file.activeCheckpoint().document_root_page != 0);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const doc1 = (try reopened.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(doc1);
    try std.testing.expectEqualStrings("{\"title\":\"one\"}", doc1);

    const doc2 = (try reopened.getDocumentAlloc(allocator, "doc:2")).?;
    defer allocator.free(doc2);
    try std.testing.expectEqualStrings("{\"title\":\"two\"}", doc2);

    try std.testing.expectEqual(@as(?[]u8, null), try reopened.getDocumentAlloc(allocator, "missing"));
}

test "lite native document store returns newest overwrite" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-overwrite.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putDocument("doc:1", "old");
    try file.putDocument("doc:1", "new");

    const value = (try file.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(value);
    try std.testing.expectEqualStrings("new", value);
}

test "lite native hot commits remain append only until explicit vacuum" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-free-map-reuse.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putDocument("doc:1", "v1");
    try file.putDocument("doc:1", "v2");
    try file.putDocument("doc:1", "v3");

    const reusable = try file.readFreePagesAlloc(file.activeCheckpoint());
    defer allocator.free(reusable);
    try std.testing.expectEqual(@as(usize, 0), reusable.len);

    const before_size = (try file.file.stat(file.io_impl.io())).size;
    try file.putDocument("doc:1", "v4");
    const after_size = (try file.file.stat(file.io_impl.io())).size;

    // Commits carry forward already-known free pages but never perform a
    // whole-file reachability walk. Explicit vacuum is the bounded place where
    // obsolete history is reclaimed.
    try std.testing.expectEqual(before_size + 4 * default_page_size, after_size);
    const report = try file.check();
    try std.testing.expect(report.valid);

    const value = (try file.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(value);
    try std.testing.expectEqualStrings("v4", value);
}

test "lite native free map does not reuse pages while reader pins older checkpoint" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-free-map-reader-protected.aflite");
    defer allocator.free(path);

    var writer = try NativeFile.create(allocator, path);
    defer writer.close();

    try writer.putDocument("doc:1", "v1");
    try writer.putDocument("doc:1", "v2");
    const before_size = (try writer.file.stat(writer.io_impl.io())).size;

    var reader = try NativeFile.open(allocator, path, true);
    defer reader.close();
    const reader_checkpoint = reader.activeCheckpoint();
    const reader_value_before = (try reader.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(reader_value_before);
    try std.testing.expectEqualStrings("v2", reader_value_before);

    try writer.putDocument("doc:1", "v3");
    try writer.putDocument("doc:1", "v4");
    const after_size = (try writer.file.stat(writer.io_impl.io())).size;
    try std.testing.expect(after_size > before_size + default_page_size);

    try std.testing.expectEqual(reader_checkpoint.commit_sequence, reader.activeCheckpoint().commit_sequence);
    const reader_value_after = (try reader.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(reader_value_after);
    try std.testing.expectEqualStrings("v2", reader_value_after);
    const reader_free_pages = try reader.readFreePagesAlloc(reader.activeCheckpoint());
    defer allocator.free(reader_free_pages);

    const writer_value = (try writer.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(writer_value);
    try std.testing.expectEqualStrings("v4", writer_value);

    const report = try writer.check();
    try std.testing.expect(report.valid);
}

test "lite native stable snapshot preserves pinned reader checkpoint while writer advances" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-pinned-reader-snapshot.aflite");
    defer allocator.free(path);
    const snapshot_path = try testPath(allocator, tmp, "native-pinned-reader-snapshot-copy.aflite");
    defer allocator.free(snapshot_path);

    var writer = try NativeFile.create(allocator, path);
    defer writer.close();

    try writer.putDocument("doc:1", "v1");
    try writer.putDocument("doc:1", "v2");

    var reader = try NativeFile.open(allocator, path, true);
    defer reader.close();
    const reader_checkpoint = reader.activeCheckpoint();
    const reader_value_before = (try reader.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(reader_value_before);
    try std.testing.expectEqualStrings("v2", reader_value_before);

    try writer.putDocument("doc:1", "v3");
    try writer.putDocument("doc:1", "v4");

    const snapshot_report = try reader.copyStableSnapshotToPath(snapshot_path, false);
    try std.testing.expectEqual(reader_checkpoint.commit_sequence, snapshot_report.checkpoint_sequence);
    try std.testing.expect(snapshot_report.tail_bytes > 0);

    const snapshot_check = try checkFile(allocator, snapshot_path);
    try std.testing.expect(snapshot_check.valid);
    try std.testing.expectEqual(@as(u64, 0), snapshot_check.tail_bytes);

    var snapshot = try NativeFile.open(allocator, snapshot_path, true);
    defer snapshot.close();
    try std.testing.expectEqual(reader_checkpoint.commit_sequence, snapshot.activeCheckpoint().commit_sequence);
    const snapshot_value = (try snapshot.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(snapshot_value);
    try std.testing.expectEqualStrings("v2", snapshot_value);

    const writer_value = (try writer.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(writer_value);
    try std.testing.expectEqualStrings("v4", writer_value);
}

test "lite native document store spills large values into value pages" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-large.aflite");
    defer allocator.free(path);

    const large = try allocator.alloc(u8, 9000);
    defer allocator.free(large);
    for (large, 0..) |*byte, i| {
        byte.* = @intCast('a' + (i % 26));
    }

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        const value_pages = std.math.divCeil(usize, large.len, file.maxValuePagePayloadBytes()) catch unreachable;
        try file.putDocument("doc:large", large);
        try std.testing.expectEqual(@as(u64, @intCast(5 + value_pages)), file.activeCheckpoint().page_count);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const value = (try reopened.getDocumentAlloc(allocator, "doc:large")).?;
    defer allocator.free(value);
    try std.testing.expectEqualSlices(u8, large, value);

    const docs = try reopened.snapshotDocumentsAlloc(allocator);
    defer NativeFile.freeSnapshotDocuments(allocator, docs);
    try std.testing.expectEqual(@as(usize, 1), docs.len);
    try std.testing.expectEqualStrings("doc:large", docs[0].key);
    try std.testing.expectEqualSlices(u8, large, docs[0].value);

    const report = try reopened.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.record_count);
}

test "lite native document tombstone hides older value after reopen" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-delete.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "old");
        try file.deleteDocument("doc:1");
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectEqual(@as(?[]u8, null), try reopened.getDocumentAlloc(allocator, "doc:1"));
}

test "lite native document store detects corrupted root page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-corrupt.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectError(error.NativePageChecksumMismatch, reopened.getDocumentAlloc(allocator, "doc:1"));
}

test "lite native document store detects corrupted external value page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-large-corrupt.aflite");
    defer allocator.free(path);

    const large = try allocator.alloc(u8, 9000);
    defer allocator.free(large);
    @memset(large, 'x');

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:large", large);
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectError(error.NativePageChecksumMismatch, reopened.getDocumentAlloc(allocator, "doc:large"));

    const report = try reopened.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("page_checksum_mismatch", report.issue.?);
}

test "lite native document batch publishes one checkpoint" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-batch.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocumentBatch(&.{
            .{ .key = "doc:b", .value = "second" },
            .{ .key = "doc:a", .value = "first" },
            .{ .key = "doc:b", .value = "newer second" },
            .{ .key = "doc:c", .value = "deleted" },
            .{ .key = "doc:c", .is_delete = true },
        });
        try std.testing.expectEqual(@as(u64, 1), file.activeCheckpoint().commit_sequence);
        try std.testing.expectEqual(@as(u64, 5), file.activeCheckpoint().page_count);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const doc_a = (try reopened.getDocumentAlloc(allocator, "doc:a")).?;
    defer allocator.free(doc_a);
    try std.testing.expectEqualStrings("first", doc_a);

    const doc_b = (try reopened.getDocumentAlloc(allocator, "doc:b")).?;
    defer allocator.free(doc_b);
    try std.testing.expectEqualStrings("newer second", doc_b);

    try std.testing.expectEqual(@as(?[]u8, null), try reopened.getDocumentAlloc(allocator, "doc:c"));
}

test "lite native document snapshot returns sorted live records" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-snapshot.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putDocumentBatch(&.{
        .{ .key = "doc:b", .value = "second" },
        .{ .key = "doc:a", .value = "first" },
        .{ .key = "doc:b", .value = "newer second" },
        .{ .key = "doc:c", .value = "third" },
        .{ .key = "doc:c", .is_delete = true },
    });

    const docs = try file.snapshotDocumentsAlloc(allocator);
    defer NativeFile.freeSnapshotDocuments(allocator, docs);

    try std.testing.expectEqual(@as(usize, 2), docs.len);
    try std.testing.expectEqualStrings("doc:a", docs[0].key);
    try std.testing.expectEqualStrings("first", docs[0].value);
    try std.testing.expectEqualStrings("doc:b", docs[1].key);
    try std.testing.expectEqualStrings("newer second", docs[1].value);
}

test "lite native namespace snapshot does not read unrelated document chains" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-namespace-index.aflite");
    defer allocator.free(path);
    const prefix_a = [_]u8{ 't', 'a', 0 };
    const prefix_b = [_]u8{ 't', 'b', 0 };
    const key_a = prefix_a ++ "doc:a".*;
    const key_b = prefix_b ++ "doc:b".*;

    var a_head: u64 = 0;
    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument(&key_a, "a");
        try file.putDocument(&key_b, "b");
        var directory = (try file.loadNamespaceDirectoryAlloc(allocator)).?;
        defer NativeFile.deinitNamespaceDirectory(allocator, &directory);
        a_head = directory.get(&prefix_a).?;
        const user_catalog = try file.snapshotCatalogRecordsAlloc(allocator);
        defer NativeFile.freeSnapshotCatalogRecords(allocator, user_catalog);
        try std.testing.expectEqual(@as(usize, 0), user_catalog.len);
    }
    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", a_head * default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const docs_b = try reopened.snapshotDocumentsWithPrefixAlloc(allocator, &prefix_b);
    defer NativeFile.freeSnapshotDocuments(allocator, docs_b);
    try std.testing.expectEqual(@as(usize, 1), docs_b.len);
    try std.testing.expectEqualStrings("b", docs_b[0].value);
    try std.testing.expectError(error.NativePageChecksumMismatch, reopened.snapshotDocumentsAlloc(allocator));
}

test "lite native namespace directory uses bounded deltas and survives cold reopen" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-namespace-deltas.aflite");
    defer allocator.free(path);
    const prefix_a = [_]u8{ 't', 'a', 0 };
    const prefix_b = [_]u8{ 't', 'b', 0 };

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        var value_buf: [32]u8 = undefined;
        var key_buf: [64]u8 = undefined;
        for (0..300) |i| {
            const prefix = if (i % 2 == 0) &prefix_a else &prefix_b;
            const key_tail = try std.fmt.bufPrint(&key_buf, "doc-{d}", .{i});
            var key = std.ArrayListUnmanaged(u8).empty;
            defer key.deinit(allocator);
            try key.appendSlice(allocator, prefix);
            try key.appendSlice(allocator, key_tail);
            const value = try std.fmt.bufPrint(&value_buf, "value-{d}", .{i});
            try file.putDocument(key.items, value);
        }
        try std.testing.expect(file.namespace_directory_delta_depth < namespace_directory_snapshot_interval);
        var reachable = std.AutoHashMapUnmanaged(u64, void){};
        defer reachable.deinit(allocator);
        const directory_pages = try file.countReachableChainPages(
            .catalog,
            file.activeCheckpoint().namespace_directory_root_page,
            &reachable,
        );
        try std.testing.expectEqual(@as(u64, file.namespace_directory_delta_depth + 1), directory_pages);
        try std.testing.expect((try file.check()).valid);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const docs_a = try reopened.snapshotDocumentsWithPrefixAlloc(allocator, &prefix_a);
    defer NativeFile.freeSnapshotDocuments(allocator, docs_a);
    const docs_b = try reopened.snapshotDocumentsWithPrefixAlloc(allocator, &prefix_b);
    defer NativeFile.freeSnapshotDocuments(allocator, docs_b);
    try std.testing.expectEqual(@as(usize, 150), docs_a.len);
    try std.testing.expectEqual(@as(usize, 150), docs_b.len);
}

test "lite native check rejects incomplete namespace links with valid page checksums" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-namespace-link-check.aflite");
    defer allocator.free(path);
    const prefix = [_]u8{ 't', 0 };
    const first_key = prefix ++ "first".*;
    const second_key = prefix ++ "second".*;

    var file = try NativeFile.create(allocator, path);
    defer file.close();
    try file.putDocument(&first_key, "one");
    try file.putDocument(&second_key, "two");

    const head = file.activeCheckpoint().document_root_page;
    const payload = try file.readPagePayloadByKindAlloc(allocator, head, .document);
    defer allocator.free(payload);
    const entry = try decodeDocumentEntry(payload);
    var rewritten = std.ArrayListUnmanaged(u8).empty;
    defer rewritten.deinit(allocator);
    try encodeDocumentEntry(allocator, &rewritten, .{
        .previous_page = entry.previous_page,
        .previous_namespace_page = 0,
        .key = entry.key,
        .value = entry.value,
        .is_delete = entry.is_delete,
        .external_value_root_page = entry.external_value_root_page,
    });
    try file.writePage(head, .document, rewritten.items);

    const report = try file.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_namespace_directory", report.issue.?);
}

test "lite native check validates committed root chains" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putCatalogRecord("schema", "{\"version\":1}");
    try file.putDocument("doc:1", "{\"title\":\"one\"}");

    const report = try file.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(?[]const u8, null), report.issue);
    try std.testing.expectEqual(@as(u64, 2), report.record_count);
    try std.testing.expectEqual(@as(u64, 2), report.live_file_count);
    try std.testing.expect(report.live_bytes > 0);
    try std.testing.expectEqual(@as(u64, default_page_size * 9), report.file_size);
    try std.testing.expectEqual(@as(u64, default_page_size * 8), report.compact_size);
    try std.testing.expectEqual(@as(u64, 0), report.tail_bytes);
    try std.testing.expectEqual(@as(u64, default_page_size), report.reclaimable_bytes);
}

test "lite native check validates committed index catalog root chain" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-index-catalog.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putCatalogRecord("schema", "{\"version\":1}");
    try file.putIndexCatalogRecord("index/files/hbc/postings.bin", "index bytes");
    try file.putDocument("doc:1", "{\"title\":\"one\"}");

    const checkpoint = file.activeCheckpoint();
    try std.testing.expect(checkpoint.catalog_root_page != 0);
    try std.testing.expect(checkpoint.index_catalog_root_page != 0);
    try std.testing.expect(checkpoint.document_root_page != 0);

    const report = try file.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(?[]const u8, null), report.issue);
    try std.testing.expectEqual(@as(u64, 3), report.record_count);
    try std.testing.expectEqual(@as(u64, 3), report.live_file_count);
    try std.testing.expect(report.live_bytes > 0);
    try std.testing.expectEqual(@as(u64, default_page_size * 13), report.file_size);
    try std.testing.expectEqual(@as(u64, default_page_size * 11), report.compact_size);
    try std.testing.expectEqual(@as(u64, default_page_size * 2), report.reclaimable_bytes);

    const index_file = (try file.getIndexCatalogRecordAlloc(allocator, "index/files/hbc/postings.bin")).?;
    defer allocator.free(index_file);
    try std.testing.expectEqualStrings("index bytes", index_file);
}

test "lite native check reports overlapping committed root pages" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-overlap.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        try file.putCatalogRecord("schema", "{\"version\":1}");
        try file.putIndexCatalogRecord("index/files/hbc/postings.bin", "index bytes");

        const checkpoint = file.activeCheckpoint();
        try std.testing.expect(checkpoint.catalog_root_page != 0);
        try std.testing.expect(checkpoint.index_catalog_root_page != 0);
        try std.testing.expect(checkpoint.catalog_root_page != checkpoint.index_catalog_root_page);
    }

    var header_bytes = try readHeaderForTest(path);
    var header = try decodeHeader(&header_bytes);
    header.checkpoints[header.active_checkpoint].index_catalog_root_page =
        header.checkpoints[header.active_checkpoint].catalog_root_page;
    encodeHeader(&header_bytes, header);

    {
        var raw = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer raw.close(std.testing.io);
        try raw.writePositionalAll(std.testing.io, &header_bytes, 0);
        try raw.sync(std.testing.io);
    }

    const report = try checkFile(allocator, path);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_page_chain", report.issue.?);
}

test "lite native stable snapshot copies committed prefix without tail bytes" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const source_path = try testPath(allocator, tmp, "native-snapshot-source.aflite");
    defer allocator.free(source_path);
    const snapshot_path = try testPath(allocator, tmp, "native-snapshot-copy.aflite");
    defer allocator.free(snapshot_path);

    const snapshot_size = blk: {
        var file = try NativeFile.create(allocator, source_path);
        defer file.close();
        try file.putCatalogRecord("schema", "{\"version\":1}");
        try file.putIndexCatalogRecord("index/files/hbc/postings.bin", "index bytes");
        try file.putDocument("doc:1", "{\"title\":\"one\"}");
        const checkpoint = file.activeCheckpoint();
        try std.testing.expect(checkpoint.free_map_root_page != 0);
        break :blk try checkpointPrefixSize(checkpoint, file.header.page_size);
    };

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, source_path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "uncommitted tail", snapshot_size);
    }

    const source_report = try checkFile(allocator, source_path);
    try std.testing.expect(!source_report.valid);
    try std.testing.expectEqualStrings("tail_bytes", source_report.issue.?);
    try std.testing.expect(source_report.tail_bytes > 0);

    const snapshot_report = try copyStableSnapshot(allocator, source_path, snapshot_path, false);
    try std.testing.expectEqual(snapshot_size, snapshot_report.snapshot_size);
    try std.testing.expectEqual(@as(u64, "uncommitted tail".len), snapshot_report.tail_bytes);

    const clean_report = try checkFile(allocator, snapshot_path);
    try std.testing.expect(clean_report.valid);
    try std.testing.expectEqual(@as(?[]const u8, null), clean_report.issue);
    try std.testing.expectEqual(@as(u64, 0), clean_report.tail_bytes);
    try std.testing.expectEqual(snapshot_report.snapshot_size, clean_report.file_size);

    var reopened = try NativeFile.open(allocator, snapshot_path, true);
    defer reopened.close();
    try std.testing.expect(reopened.activeCheckpoint().free_map_root_page != 0);

    const schema = (try reopened.getCatalogRecordAlloc(allocator, "schema")).?;
    defer allocator.free(schema);
    try std.testing.expectEqualStrings("{\"version\":1}", schema);

    const index_file = (try reopened.getIndexCatalogRecordAlloc(allocator, "index/files/hbc/postings.bin")).?;
    defer allocator.free(index_file);
    try std.testing.expectEqualStrings("index bytes", index_file);

    const doc = (try reopened.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(doc);
    try std.testing.expectEqualStrings("{\"title\":\"one\"}", doc);
}

test "lite native stable snapshot rejects same target by canonical path" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const path = try testPath(allocator, tmp, "native-snapshot-self.aflite");
    defer allocator.free(path);
    const nested_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/nested", .{tmp.sub_path});
    defer allocator.free(nested_dir);
    const alias_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/nested/../native-snapshot-self.aflite", .{tmp.sub_path});
    defer allocator.free(alias_path);

    try fs_paths.createDirPathPortable(io, nested_dir);
    {
        var writer = try NativeFile.create(allocator, path);
        defer writer.close();
        try writer.putDocument("doc:self", "{\"title\":\"same target\"}");
    }

    var reader = try NativeFile.open(allocator, path, true);
    defer reader.close();
    try std.testing.expectError(error.InvalidNativeSnapshotPath, reader.copyStableSnapshotToPath(alias_path, true));

    const report = try checkFile(allocator, path);
    try std.testing.expect(report.valid);
}

test "lite native stable snapshot holds output writer lock before staging" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const source_path = try testPath(allocator, tmp, "native-snapshot-lock-source.aflite");
    defer allocator.free(source_path);
    const snapshot_path = try testPath(allocator, tmp, "native-snapshot-lock-copy.aflite");
    defer allocator.free(snapshot_path);
    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp-aflite-snapshot", .{snapshot_path});
    defer allocator.free(tmp_path);

    {
        var file = try NativeFile.create(allocator, source_path);
        defer file.close();
        try file.putDocument("doc:visible", "visible");
    }

    var dest_lock = try lockWriterPath(allocator, snapshot_path);
    defer dest_lock.close();

    try std.testing.expectError(error.WouldBlock, copyStableSnapshot(allocator, source_path, snapshot_path, false));
    try std.testing.expect(!pathExists(std.testing.io, snapshot_path));
    try std.testing.expect(!pathExists(std.testing.io, tmp_path));
}

test "lite native vacuum rewrites live catalog and document records" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-vacuum.aflite");
    defer allocator.free(path);

    const large_value = try allocator.alloc(u8, default_page_size * 2);
    defer allocator.free(large_value);
    @memset(large_value, 'x');

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        try file.putCatalogRecord("schema", "{\"version\":1}");
        try file.putCatalogRecord("schema", "{\"version\":2}");
        try file.putDocument("doc:live", large_value);
        try file.putDocument("doc:live", "small");
        try file.putDocument("doc:gone", "deleted");
        try file.deleteDocument("doc:gone");

        const before = try file.check();
        try std.testing.expect(before.record_count > 2);
        try std.testing.expectEqual(@as(u64, 2), before.live_file_count);
        try std.testing.expect(before.live_bytes > 0);
        try std.testing.expect(before.compact_size < before.file_size);
        try std.testing.expect(before.reclaimable_bytes > 0);

        const vacuumed = try file.vacuum();
        try std.testing.expect(vacuumed.before_size > vacuumed.after_size);
        try std.testing.expect(vacuumed.reclaimed_bytes > 0);
        try std.testing.expect(file.activeCheckpoint().free_map_root_page != 0);

        const after = try file.check();
        try std.testing.expect(after.valid);
        try std.testing.expectEqual(vacuumed.after_size, after.file_size);
        try std.testing.expectEqual(@as(u64, 2), after.record_count);
        try std.testing.expectEqual(@as(u64, 2), after.live_file_count);
        try std.testing.expectEqual(after.file_size, after.compact_size);
        try std.testing.expectEqual(@as(u64, 0), after.reclaimable_bytes);

        const schema = (try file.getCatalogRecordAlloc(allocator, "schema")).?;
        defer allocator.free(schema);
        try std.testing.expectEqualStrings("{\"version\":2}", schema);

        const live = (try file.getDocumentAlloc(allocator, "doc:live")).?;
        defer allocator.free(live);
        try std.testing.expectEqualStrings("small", live);
        try std.testing.expectEqual(@as(?[]u8, null), try file.getDocumentAlloc(allocator, "doc:gone"));
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const report = try reopened.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 2), report.record_count);

    const schema = (try reopened.getCatalogRecordAlloc(allocator, "schema")).?;
    defer allocator.free(schema);
    try std.testing.expectEqualStrings("{\"version\":2}", schema);

    const live = (try reopened.getDocumentAlloc(allocator, "doc:live")).?;
    defer allocator.free(live);
    try std.testing.expectEqualStrings("small", live);
}

test "lite native vacuum atomically replaces file and keeps writer handle usable" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-vacuum-replace.aflite");
    defer allocator.free(path);
    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp-aflite-vacuum", .{path});
    defer allocator.free(tmp_path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        try file.putDocument("doc:live", "old");
        try file.putDocument("doc:live", "new");

        const vacuumed = try file.vacuum();
        try std.testing.expect(vacuumed.reclaimed_bytes > 0);
        try std.testing.expect(!pathExists(file.io_impl.io(), tmp_path));

        try file.putDocument("doc:after-vacuum", "writer still attached");
        const after = (try file.getDocumentAlloc(allocator, "doc:after-vacuum")).?;
        defer allocator.free(after);
        try std.testing.expectEqualStrings("writer still attached", after);

        const report = try file.check();
        try std.testing.expect(report.valid);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const live = (try reopened.getDocumentAlloc(allocator, "doc:live")).?;
    defer allocator.free(live);
    try std.testing.expectEqualStrings("new", live);
    const after = (try reopened.getDocumentAlloc(allocator, "doc:after-vacuum")).?;
    defer allocator.free(after);
    try std.testing.expectEqualStrings("writer still attached", after);
}

test "lite native vacuum keeps adopted replacement usable after post rename failure" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-vacuum-post-rename-failure.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:live", "old");
        try file.putDocument("doc:live", "new");
        file.test_fail_vacuum_after_adoption = true;
        try std.testing.expectError(error.InjectedVacuumPostRenameFailure, file.vacuum());

        // The failure is reported, but the process must never continue on the
        // unlinked pre-vacuum inode.
        try file.putDocument("doc:after", "durable on adopted file");
        const current = (try file.getDocumentAlloc(allocator, "doc:after")).?;
        defer allocator.free(current);
        try std.testing.expectEqualStrings("durable on adopted file", current);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const persisted = (try reopened.getDocumentAlloc(allocator, "doc:after")).?;
    defer allocator.free(persisted);
    try std.testing.expectEqualStrings("durable on adopted file", persisted);
}

test "lite native check validates committed free map root" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-free-map-corrupt.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putDocument("doc:1", "value");
    _ = try file.vacuum();

    const checkpoint = file.activeCheckpoint();
    try std.testing.expect(checkpoint.free_map_root_page != 0);

    const payload = try encodeFreeMapAlloc(allocator, default_page_size, checkpoint.page_count + 1, &.{});
    defer allocator.free(payload);
    var page: [default_page_size]u8 = undefined;
    encodePage(&page, .free_map, payload);
    try file.file.writePositionalAll(file.io_impl.io(), &page, checkpoint.free_map_root_page * default_page_size);
    try file.file.sync(file.io_impl.io());

    const report = try file.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_free_map", report.issue.?);
}

test "lite native free map reads are bounded by supplied checkpoint" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-free-map-checkpoint-bound.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putDocument("doc:1", "value");

    var checkpoint = file.activeCheckpoint();
    try std.testing.expect(checkpoint.free_map_root_page != 0);
    checkpoint.page_count = checkpoint.free_map_root_page;

    try std.testing.expectError(error.InvalidPageId, file.readFreePagesAlloc(checkpoint));

    var reachable_pages = std.AutoHashMapUnmanaged(u64, void){};
    defer reachable_pages.deinit(allocator);
    try std.testing.expectError(error.InvalidPageId, file.validateReachableFreeMap(checkpoint, &reachable_pages));
}

test "lite native unchanged small index records do not publish checkpoints" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "catalog-noop.aflite");
    defer allocator.free(path);
    var file = try NativeFile.create(allocator, path);
    defer file.close();
    try file.putIndexCatalogRecord("control", "state");
    const before = file.activeCheckpoint();
    for (0..8) |_| {
        try file.putIndexCatalogRecord("control", "state");
        try file.deleteIndexCatalogRecord("absent");
    }
    try std.testing.expectEqualDeep(before, file.activeCheckpoint());
    // Equal length is insufficient: changed bytes must still be published.
    try file.putIndexCatalogRecord("control", "other");
    try std.testing.expectEqual(before.commit_sequence + 1, file.activeCheckpoint().commit_sequence);
    try file.deleteIndexCatalogRecord("control");
    const deleted = file.activeCheckpoint();
    try file.deleteIndexCatalogRecord("control");
    try std.testing.expectEqualDeep(deleted, file.activeCheckpoint());
    try file.putIndexCatalogRecord("control", "other");
    const restored = file.activeCheckpoint();
    // An ambiguous publication cannot use the old header as a no-op proof.
    file.checkpoint_publication_uncertain = true;
    try file.putIndexCatalogRecord("control", "other");
    try std.testing.expectEqual(restored.commit_sequence + 1, file.activeCheckpoint().commit_sequence);
    try std.testing.expect(!file.checkpoint_publication_uncertain);
    file.page_cache.clear(allocator);
    const cold = file.activeCheckpoint();
    try file.putIndexCatalogRecord("control", "other");
    try std.testing.expectEqualDeep(cold, file.activeCheckpoint());
    var reader = try NativeFile.open(allocator, path, true);
    defer reader.close();
    const value = (try reader.getIndexCatalogRecordAlloc(allocator, "control")).?;
    defer allocator.free(value);
    try std.testing.expectEqualStrings("other", value);
    try std.testing.expectError(error.ReadOnly, reader.putIndexCatalogRecord("control", "other"));
}

test "lite native catalog point lookups skip history without allocating" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "catalog-probes.aflite");
    defer allocator.free(path);
    var file = try NativeFile.create(allocator, path);
    defer file.close();
    try file.putIndexCatalogRecord("target", "original");
    const pinned = file.activeCheckpoint();
    for (0..32) |i| try file.putIndexCatalogRecord("noise", std.mem.asBytes(&i));
    try file.putIndexCatalogRecord("deleted", "value");
    try file.deleteIndexCatalogRecord("deleted");

    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    file.allocator = failing.allocator();
    {
        defer file.allocator = allocator;
        try std.testing.expectEqual(@as(?usize, 8), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "target", file.activeCheckpoint()));
        try std.testing.expectEqual(@as(?usize, null), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "missing", file.activeCheckpoint()));
        try std.testing.expectEqual(@as(?usize, null), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "deleted", file.activeCheckpoint()));
        const value = (try file.getCatalogRecordFromRootAtCheckpointAlloc(allocator, .index, "target", pinned)).?;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("original", value);
        try std.testing.expect(!failing.has_induced_failure);
        file.allocator = allocator;
        const range = (try file.getCatalogRecordRangeFromRootAtCheckpointAlloc(allocator, .index, "target", 1, 3, file.activeCheckpoint())).?;
        defer allocator.free(range);
        try std.testing.expectEqualStrings("rig", range);
        try std.testing.expect(!failing.has_induced_failure);
    }
    try file.putIndexCatalogRecord("target", "new");
    try std.testing.expectEqual(@as(?usize, 3), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "target", file.activeCheckpoint()));
    try std.testing.expectEqual(@as(?usize, 8), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "target", pinned));
    file.page_cache.clear(allocator);
    try std.testing.expectEqual(@as(?usize, 3), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "target", file.activeCheckpoint()));
    file.page_cache_enabled.store(false, .monotonic);
    try std.testing.expectEqual(@as(?usize, 8), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "target", pinned));
}

test "lite native empty free map validation does not allocate or walk checkpoints" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-empty-free-map.aflite");
    defer allocator.free(path);
    var file = try NativeFile.create(allocator, path);
    defer file.close();
    try file.putDocument("doc:1", "v1");
    try file.putDocument("doc:1", "v2");

    // Any reachability walk requires scratch allocation, even with warm pages.
    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    file.allocator = failing.allocator();
    defer file.allocator = allocator;
    try file.validateFreePagesSafeForCheckpointSlots(&.{});
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    try std.testing.expect(!failing.has_induced_failure);
}

test "lite native free map cannot reclaim previous checkpoint pages" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-free-map-previous-protected.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        try file.putDocument("doc:1", "v1");
        try file.putDocument("doc:1", "v2");

        const active = file.activeCheckpoint();
        const previous = file.header.checkpoints[if (file.header.active_checkpoint == 0) 1 else 0];
        try std.testing.expect(active.free_map_root_page != 0);
        try std.testing.expect(previous.free_map_root_page != 0);
        try std.testing.expect(active.free_map_root_page != previous.free_map_root_page);

        const payload = try encodeFreeMapAlloc(allocator, default_page_size, active.page_count, &.{previous.free_map_root_page});
        defer allocator.free(payload);
        var page: [default_page_size]u8 = undefined;
        encodePage(&page, .free_map, payload);
        try file.file.writePositionalAll(file.io_impl.io(), &page, active.free_map_root_page * default_page_size);
        try file.file.sync(file.io_impl.io());

        const report = try file.check();
        try std.testing.expect(!report.valid);
        try std.testing.expectEqualStrings("invalid_free_map", report.issue.?);
    }

    // The free-map-vs-fallback-checkpoint cross-check only needs to run once
    // per open handle: this process's own commits can only ever reuse pages
    // that a previously verified free map already declared free, so
    // re-scanning every checkpoint slot's full reachable set on every single
    // mutation would make ingest quadratic in the number of commits.
    // Corruption written out of band (as above) is instead caught the next
    // time the file is opened and its free map is trusted again.
    var reopened = try NativeFile.open(allocator, path, false);
    defer reopened.close();
    try std.testing.expectError(error.InvalidNativeFreeMap, reopened.putDocument("doc:1", "v3"));
}

test "lite native check reports corrupted committed document page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-corrupt.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const report = try reopened.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("page_checksum_mismatch", report.issue.?);
}

test "lite native check reports corrupted committed document index page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-document-index-corrupt.aflite");
    defer allocator.free(path);

    const root_page = blk: {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
        break :blk file.activeCheckpoint().document_index_root_page;
    };

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", root_page * default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const report = try reopened.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("page_checksum_mismatch", report.issue.?);
}

test "lite native check rejects a structurally valid stale document index pointer" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-document-index-stale.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();
    try file.putDocument("doc:1", "old");
    try file.putDocument("doc:1", "new");

    const checkpoint = file.activeCheckpoint();
    const newest_payload = try file.readPagePayloadByKindAllocForCheckpoint(allocator, checkpoint.document_root_page, .document, checkpoint);
    defer allocator.free(newest_payload);
    const newest = try decodeDocumentEntry(newest_payload);
    try std.testing.expect(newest.previous_page != 0);

    var index_node = try file.readDocumentIndexNode(checkpoint.document_index_root_page, checkpoint);
    defer index_node.deinit(allocator);
    try std.testing.expectEqual(DocumentIndexNodeKind.leaf, index_node.kind);
    try std.testing.expectEqual(@as(usize, 1), index_node.pointers.len);
    index_node.pointers[0] = newest.previous_page;
    const encoded = try encodeDocumentIndexNode(allocator, index_node);
    defer allocator.free(encoded);
    var page: [default_page_size]u8 = undefined;
    encodePage(&page, .document_index, encoded);
    try file.file.writePositionalAll(file.io_impl.io(), &page, checkpoint.document_index_root_page * default_page_size);
    file.page_cache.clear(allocator);

    const report = try file.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_document_index", report.issue.?);
}

test "lite native check reports corrupted committed index catalog page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-index-corrupt.aflite");
    defer allocator.free(path);

    const root_page = blk: {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putIndexCatalogRecord("index/files/hbc/postings.bin", "index bytes");
        break :blk file.activeCheckpoint().index_catalog_root_page;
    };

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", root_page * default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const report = try reopened.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("page_checksum_mismatch", report.issue.?);
}

test "lite native check reports corrupted index catalog external value page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-index-value-corrupt.aflite");
    defer allocator.free(path);

    const large_value = try allocator.alloc(u8, default_page_size * 2);
    defer allocator.free(large_value);
    @memset(large_value, 'i');

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putIndexCatalogRecord("index/files/hbc/postings.bin", large_value);
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const report = try reopened.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("page_checksum_mismatch", report.issue.?);
}

test "lite native check reports truncated committed file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-truncated.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.setLength(std.testing.io, default_page_size + 16);
    }

    {
        var reopened = try NativeFile.open(allocator, path, true);
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 0), reopened.activeCheckpoint().commit_sequence);
        try std.testing.expectEqual(@as(?[]u8, null), try reopened.getDocumentAlloc(allocator, "doc:1"));
    }

    const report = try checkFile(allocator, path);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("tail_bytes", report.issue.?);
}

test "lite native checkFile reports corrupted header" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-header-corrupt.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", page_size_offset);
    }

    const report = try checkFile(allocator, path);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("header_checksum_mismatch", report.issue.?);
}

test "lite native checkFile reports invalid checkpoint metadata separately from truncation" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-invalid-checkpoint.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
    }

    var header_bytes = try readHeaderForTest(path);
    var header = try decodeHeader(&header_bytes);
    for (&header.checkpoints) |*slot| {
        slot.catalog_root_page = slot.page_count;
        slot.document_root_page = slot.page_count;
        slot.index_catalog_root_page = slot.page_count;
        slot.free_map_root_page = slot.page_count;
    }
    encodeHeader(&header_bytes, header);

    {
        var raw = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer raw.close(std.testing.io);
        try raw.writePositionalAll(std.testing.io, &header_bytes, 0);
        try raw.sync(std.testing.io);
    }

    const report = try checkFile(allocator, path);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_checkpoint", report.issue.?);
}

test "lite native checkFile reports checkpoint prefix overflow as invalid metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-checkpoint-overflow.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
    }

    var header_bytes = try readHeaderForTest(path);
    var header = try decodeHeader(&header_bytes);
    for (&header.checkpoints, 0..) |*slot, index| {
        slot.* = .{
            .commit_sequence = @as(u64, @intCast(index + 1)),
            .catalog_root_page = 0,
            .document_root_page = 0,
            .index_catalog_root_page = 0,
            .free_map_root_page = 0,
            .page_count = std.math.maxInt(u64),
        };
    }
    encodeHeader(&header_bytes, header);

    {
        var raw = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer raw.close(std.testing.io);
        try raw.writePositionalAll(std.testing.io, &header_bytes, 0);
        try raw.sync(std.testing.io);
    }

    const report = try checkFile(allocator, path);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_checkpoint", report.issue.?);
    try std.testing.expectEqual(@as(u64, 0), report.record_count);
    try std.testing.expectEqual(@as(u64, 0), report.compact_size);
}

test "lite native page cache tracks put remove and incremental eviction" {
    const allocator = std.testing.allocator;

    var cache = PageCache{ .limit_bytes = 32 };
    defer cache.deinit(allocator);

    cache.put(allocator, 1, "0123456789ab");
    cache.put(allocator, 2, "0123456789ab");
    try std.testing.expectEqual(@as(usize, 24), cache.total_bytes);

    const hit = (try cache.getCopy(allocator, 1)) orelse return error.TestUnexpectedResult;
    defer allocator.free(hit);
    try std.testing.expectEqualStrings("0123456789ab", hit);
    try std.testing.expectEqual(@as(?[]u8, null), try cache.getCopy(allocator, 3));

    cache.put(allocator, 1, "ba9876543210");
    try std.testing.expectEqual(@as(usize, 24), cache.total_bytes);
    const replaced = (try cache.getCopy(allocator, 1)) orelse return error.TestUnexpectedResult;
    defer allocator.free(replaced);
    try std.testing.expectEqualStrings("ba9876543210", replaced);

    cache.remove(allocator, 2);
    try std.testing.expectEqual(@as(usize, 12), cache.total_bytes);
    try std.testing.expectEqual(@as(?[]u8, null), try cache.getCopy(allocator, 2));

    // A full-capacity incoming page necessarily replaces every resident page.
    cache.put(allocator, 4, "0123456789ab");
    cache.put(allocator, 5, "0123456789abcdefghijklmnopqrstuv");
    try std.testing.expectEqual(@as(usize, 32), cache.total_bytes);
    try std.testing.expectEqual(@as(?[]u8, null), try cache.getCopy(allocator, 1));
    try std.testing.expectEqual(@as(?[]u8, null), try cache.getCopy(allocator, 4));
    const survivor = (try cache.getCopy(allocator, 5)) orelse return error.TestUnexpectedResult;
    defer allocator.free(survivor);
    try std.testing.expectEqual(@as(usize, 32), survivor.len);
}

test "lite native page and link caches report usage to resource manager" {
    const allocator = std.testing.allocator;

    var manager = resource_manager_mod.ResourceManager.init(.{});
    var cache = PageCache{ .limit_bytes = 128, .link_limit_bytes = 256 };
    defer cache.deinit(allocator);
    cache.attachResourceManager(&manager);

    cache.put(allocator, 1, "0123456789ab");
    var page_stats = manager.sliceStats(.lite_native_page_cache);
    try std.testing.expectEqual(@as(u64, 12), page_stats.used_bytes);

    cache.putLinks(allocator, 1, .{
        .kind = .document,
        .link_page = 0,
        .external_value_root_page = 7,
        .external_value_len = 128,
    });
    var link_stats = manager.sliceStats(.lite_native_link_cache);
    try std.testing.expect(link_stats.used_bytes > 0);

    cache.remove(allocator, 1);
    page_stats = manager.sliceStats(.lite_native_page_cache);
    link_stats = manager.sliceStats(.lite_native_link_cache);
    try std.testing.expectEqual(@as(u64, 0), page_stats.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), link_stats.used_bytes);
}

test "lite native page and link caches shrink under hard resource pressure" {
    const allocator = std.testing.allocator;

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lite_native_page_cache)] = .{
        .soft_limit_bytes = 4,
        .hard_limit_bytes = 8,
    };
    budgets[@intFromEnum(resource_manager_mod.Slice.lite_native_link_cache)] = .{
        .soft_limit_bytes = 4,
        .hard_limit_bytes = 8,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var cache = PageCache{ .limit_bytes = 128, .link_limit_bytes = 256 };
    defer cache.deinit(allocator);
    cache.attachResourceManager(&manager);

    cache.put(allocator, 1, "0123456789ab");
    const page_stats = manager.sliceStats(.lite_native_page_cache);
    try std.testing.expectEqual(@as(usize, 0), cache.total_bytes);
    try std.testing.expectEqual(@as(u64, 0), page_stats.used_bytes);
    try std.testing.expect(page_stats.hard_limit_rejections > 0);

    cache.putLinks(allocator, 2, .{ .kind = .value, .link_page = 0, .chunk_len = 1 });
    const link_stats = manager.sliceStats(.lite_native_link_cache);
    try std.testing.expectEqual(@as(usize, 0), cache.link_bytes);
    try std.testing.expectEqual(@as(u64, 0), link_stats.used_bytes);
    try std.testing.expect(link_stats.hard_limit_rejections > 0);
}

test "lite native page cache serves updated documents after page reuse and vacuum" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-page-cache-reuse.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    // Enough update churn to force free-page reuse across many commits while
    // reads run through the cache.
    var round: usize = 0;
    while (round < 20) : (round += 1) {
        var value_buf: [32]u8 = undefined;
        const value = try std.fmt.bufPrint(&value_buf, "round-{d}", .{round});
        try file.putDocument("doc:cache", value);

        const read = (try file.getDocumentAlloc(allocator, "doc:cache")) orelse return error.TestUnexpectedResult;
        defer allocator.free(read);
        try std.testing.expectEqualStrings(value, read);

        const docs = try file.snapshotDocumentsAlloc(allocator);
        defer NativeFile.freeSnapshotDocuments(allocator, docs);
        try std.testing.expectEqual(@as(usize, 1), docs.len);
        try std.testing.expectEqualStrings(value, docs[0].value);
    }

    const report_before = try file.check();
    try std.testing.expect(report_before.valid);

    _ = try file.vacuum();

    const read = (try file.getDocumentAlloc(allocator, "doc:cache")) orelse return error.TestUnexpectedResult;
    defer allocator.free(read);
    try std.testing.expectEqualStrings("round-19", read);

    const report = try file.check();
    try std.testing.expect(report.valid);
}

test "lite native catalog index bounds cold hits and misses across checkpoints" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catalog-index-scaling.aflite");
    defer alloc.free(path);
    {
        var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
        defer file.close();
        for (0..400) |i| {
            var key: [32]u8 = undefined;
            try file.putIndexCatalogRecord(try std.fmt.bufPrint(&key, "key-{d:0>4}", .{i}), "original");
        }
    }
    var file = try NativeFile.openWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    const pinned = file.activeCheckpoint();
    for ([_][]const u8{ "key-0000", "key-0200", "missing" }) |key| {
        const before = file.test_page_reads.load(.monotonic);
        const found = try file.getIndexCatalogRecordAlloc(alloc, key);
        defer if (found) |value| alloc.free(value);
        if (std.mem.eql(u8, key, "missing")) try std.testing.expect(found == null) else try std.testing.expectEqualStrings("original", found.?);
        try std.testing.expect(file.test_page_reads.load(.monotonic) - before <= 6);
    }
    try file.putIndexCatalogRecord("key-0000", "changed");
    try file.renameIndexCatalogRecord("key-0200", "renamed");
    try file.deleteIndexCatalogRecord("key-0399");
    const old = (try file.getIndexCatalogRecordAtCheckpointAlloc(alloc, "key-0000", pinned)).?;
    defer alloc.free(old);
    try std.testing.expectEqualStrings("original", old);
    try std.testing.expectEqual(@as(?usize, null), try file.getIndexCatalogRecordSize("key-0200"));
    try std.testing.expectEqual(@as(?usize, 8), try file.getIndexCatalogRecordSize("renamed"));
    try std.testing.expectEqual(@as(?usize, 8), try file.getIndexCatalogRecordSizeAtCheckpoint("key-0399", pinned));
    try std.testing.expectEqual(@as(?usize, null), try file.getIndexCatalogRecordSize("key-0399"));
    try std.testing.expect((try file.check()).valid);
}

test "lite native extent appends and tail reads are bounded and preserve snapshots" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "extent-scaling.aflite");
    defer alloc.free(path);
    const snapshot_path = try testPath(alloc, tmp, "extent-snapshot.aflite");
    defer alloc.free(snapshot_path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const chunk = file.maxValuePagePayloadBytes();
    // Cross the 64-by-64 leaf boundary to exercise cascading splits and
    // growth to a third branch level, not just a single root split.
    const initial = try alloc.alloc(u8, chunk * 4095);
    defer alloc.free(initial);
    @memset(initial, 'a');
    try file.putIndexCatalogRecord("wal", initial);
    const pinned = file.activeCheckpoint();
    var expected = std.ArrayListUnmanaged(u8).empty;
    defer expected.deinit(alloc);
    try expected.appendSlice(alloc, initial);
    const suffix = try alloc.alloc(u8, chunk);
    defer alloc.free(suffix);
    file.page_cache_enabled.store(false, .monotonic);
    for (0..256) |i| {
        @memset(suffix, @intCast(i));
        const reads = file.test_page_reads.load(.monotonic);
        const writes = file.test_page_writes.load(.monotonic);
        try file.appendIndexCatalogRecord("wal", suffix);
        // Copying the existing value would write at least 4096 pages on the
        // very first append and grow linearly thereafter.
        try std.testing.expect(file.test_page_writes.load(.monotonic) - writes <= 10);
        try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 20);
        try expected.appendSlice(alloc, suffix);
        const before_tail = file.test_page_reads.load(.monotonic);
        const tail = (try file.getIndexCatalogRecordRangeAlloc(alloc, "wal", expected.items.len - 17, 17)).?;
        defer alloc.free(tail);
        try std.testing.expectEqualSlices(u8, suffix[suffix.len - 17 ..], tail);
        try std.testing.expect(file.test_page_reads.load(.monotonic) - before_tail <= 8);
    }
    // Fill a partial leaf and read a range crossing a leaf boundary.
    try file.appendIndexCatalogRecord("wal", "end");
    try file.appendIndexCatalogRecord("wal", "ing");
    try expected.appendSlice(alloc, "ending");
    const cross = (try file.getIndexCatalogRecordRangeAlloc(alloc, "wal", expected.items.len - 12, 12)).?;
    defer alloc.free(cross);
    try std.testing.expectEqualSlices(u8, expected.items[expected.items.len - 12 ..], cross);
    const original = (try file.getIndexCatalogRecordAtCheckpointAlloc(alloc, "wal", pinned)).?;
    defer alloc.free(original);
    try std.testing.expectEqualSlices(u8, initial, original);
    const all = (try file.getIndexCatalogRecordAlloc(alloc, "wal")).?;
    defer alloc.free(all);
    try std.testing.expectEqualSlices(u8, expected.items, all);
    try std.testing.expect((try file.check()).valid);
    _ = try file.copyStableSnapshotToPath(snapshot_path, false);
    var snapshot = try NativeFile.open(alloc, snapshot_path, true);
    defer snapshot.close();
    const copied = (try snapshot.getIndexCatalogRecordAlloc(alloc, "wal")).?;
    defer alloc.free(copied);
    try std.testing.expectEqualSlices(u8, expected.items, copied);
    _ = try file.vacuum();
    const compact = (try file.getIndexCatalogRecordAlloc(alloc, "wal")).?;
    defer alloc.free(compact);
    try std.testing.expectEqualSlices(u8, expected.items, compact);
    const report = try file.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(report.file_size, report.compact_size);
}

test "lite native revision 3 rejects revision 2 without modifying the file" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "rejected-v2.aflite");
    defer alloc.free(path);
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});
    std.mem.writeInt(u32, encoded[version_offset..][0..4], 2, .little);
    std.mem.writeInt(u32, encoded[header_checksum_offset..][0..4], headerChecksum(&encoded), .little);
    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{});
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, &encoded, 0);
    }
    try std.testing.expectError(error.UnsupportedNativeFormatVersion, NativeFile.open(alloc, path, true));
    try std.testing.expectError(error.UnsupportedNativeFormatVersion, NativeFile.open(alloc, path, false));
    var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{});
    defer file.close(std.testing.io);
    var actual: [header_size]u8 = undefined;
    try readHeaderExactAt(file, std.testing.io, &actual);
    try std.testing.expectEqualSlices(u8, &encoded, &actual);
}

test "lite native extent checks reject corrupted child lengths" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "extent-corrupt.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const value = try alloc.alloc(u8, default_page_size * 3);
    defer alloc.free(value);
    @memset(value, 'v');
    try file.putIndexCatalogRecord("wal", value);
    const record = (try file.lookupCatalogPage(file.activeCheckpoint(), .index, "wal")).?;
    const record_payload = try file.readPagePayloadByKindAlloc(alloc, record, .catalog);
    defer alloc.free(record_payload);
    const entry = try decodeCatalogEntry(record_payload);
    const payload = try file.readPagePayloadByKindAlloc(alloc, entry.external_value_root_page, .value_extent);
    defer alloc.free(payload);
    payload[NativeFile.extent_header_size + 8] ^= 1;
    try file.writePage(entry.external_value_root_page, .value_extent, payload);
    try std.testing.expectError(error.InvalidNativeValueChain, file.getIndexCatalogRecordAlloc(alloc, "wal"));
    try std.testing.expect(!(try file.check()).valid);
}

test "lite native catalog index check rejects stale or cross key pointers" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catalog-index-corrupt.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    try file.putIndexCatalogBatch(&.{ .{ .key = "a", .value = "one" }, .{ .key = "b", .value = "two" } });
    const roots = try file.readCatalogRoots(file.activeCheckpoint().index_catalog_root_page, file.activeCheckpoint());
    var node = try file.readDocumentIndexNode(roots.index, file.activeCheckpoint());
    defer node.deinit(alloc);
    node.pointers[0] = node.pointers[1];
    const payload = try encodeDocumentIndexNode(alloc, node);
    defer alloc.free(payload);
    try file.writePage(roots.index, .document_index, payload);
    try std.testing.expectError(error.InvalidNativePageChain, file.getIndexCatalogRecordAlloc(alloc, "a"));
    try std.testing.expect(!(try file.check()).valid);
}

test "lite native incomplete extent commit falls back to prior indexed checkpoint" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "extent-fallback.aflite");
    defer alloc.free(path);
    const value = try alloc.alloc(u8, default_page_size * 3);
    defer alloc.free(value);
    @memset(value, 'v');
    {
        var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
        defer file.close();
        try file.putIndexCatalogRecord("wal", value);
        const previous = file.activeCheckpoint();
        try file.appendIndexCatalogRecord("wal", "incomplete");
        try file.file.setLength(file.runtimeIo(), previous.page_count * default_page_size);
    }
    var file = try NativeFile.open(alloc, path, false);
    defer file.close();
    const recovered = (try file.getIndexCatalogRecordAlloc(alloc, "wal")).?;
    defer alloc.free(recovered);
    try std.testing.expectEqualSlices(u8, value, recovered);
    try file.appendIndexCatalogRecord("wal", "recovered");
    const tail = (try file.getIndexCatalogRecordRangeAlloc(alloc, "wal", value.len, 9)).?;
    defer alloc.free(tail);
    try std.testing.expectEqualStrings("recovered", tail);
    try std.testing.expect((try file.check()).valid);
}

test "lite native commits extend positional writes without stat or resize calls" {
    const Counter = struct {
        var base: std.Io = undefined;
        var stats: usize = 0;
        var resizes: usize = 0;
        fn stat(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.StatError!std.Io.File.Stat {
            stats += 1;
            return base.vtable.fileStat(userdata, file);
        }
        fn resize(userdata: ?*anyopaque, file: std.Io.File, len: u64) std.Io.File.SetLengthError!void {
            resizes += 1;
            return base.vtable.fileSetLength(userdata, file, len);
        }
    };
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "positional-growth.aflite");
    defer alloc.free(path);
    Counter.base = std.testing.io;
    var vtable = std.testing.io.vtable.*;
    vtable.fileStat = Counter.stat;
    vtable.fileSetLength = Counter.resize;
    const io = std.Io{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    var file = try NativeFile.createWithIo(alloc, io, path, .{ .no_sync = true });
    defer file.close();
    Counter.stats = 0;
    Counter.resizes = 0;
    const value = try alloc.alloc(u8, default_page_size * 16);
    defer alloc.free(value);
    @memset(value, 'x');
    try file.putIndexCatalogRecord("large", value);
    try file.appendIndexCatalogRecord("large", "tail");
    try file.putDocument("doc", value);
    try std.testing.expectEqual(@as(usize, 0), Counter.stats);
    try std.testing.expectEqual(@as(usize, 0), Counter.resizes);
    try std.testing.expectEqual(file.activeCheckpoint().page_count * file.header.page_size, (try file.file.stat(std.testing.io)).size);
    try std.testing.expect((try file.check()).valid);
}

test "lite native catalog preserves maximum length keys through vacuum" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catalog-max-key.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const key = try alloc.alloc(u8, file.maxPagePayloadBytes() - 16);
    defer alloc.free(key);
    @memset(key, 'k');
    try file.putIndexCatalogRecord(key, "");
    try file.putIndexCatalogRecord("ordinary", "indexed");
    try std.testing.expectEqual(@as(?usize, 0), try file.getIndexCatalogRecordSize(key));
    try std.testing.expect((try file.check()).valid);
    _ = try file.vacuum();
    try std.testing.expectEqual(@as(?usize, 0), try file.getIndexCatalogRecordSize(key));
    try file.deleteIndexCatalogRecord(key);
    try std.testing.expectEqual(@as(?usize, null), try file.getIndexCatalogRecordSize(key));
    try std.testing.expect((try file.check()).valid);
}

test "lite native catalog indexes mixed large empty and maximum keys across splits and vacuum" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catalog-overflow-keys.aflite");
    defer alloc.free(path);
    {
        var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
        defer file.close();
        const a = [_]u8{'a'} ** 1000;
        const z = [_]u8{'z'} ** 1000;
        const m = [_]u8{'m'} ** 4000;
        try file.putIndexCatalogRecord(&a, "a");
        try file.putIndexCatalogRecord(&z, "z");
        // Previously passed the catalog's key-size check but could not be
        // partitioned into two inline B-tree leaves.
        try file.putIndexCatalogRecord(&m, "m");
        try file.putIndexCatalogRecord("", "empty");
        const maximum = try alloc.alloc(u8, file.maxPagePayloadBytes() - 16);
        defer alloc.free(maximum);
        @memset(maximum, 'x');
        try file.putIndexCatalogRecord(maximum, "");
        for (0..400) |i| {
            var key: [1000]u8 = @splat('k');
            _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
            try file.putIndexCatalogRecord(&key, "long");
        }
        const before_lookup = file.test_page_reads.load(.monotonic);
        try std.testing.expectEqual(@as(?usize, 1), try file.getIndexCatalogRecordSize(&m));
        try std.testing.expect(file.test_page_reads.load(.monotonic) - before_lookup <= 20);
        const pinned = file.activeCheckpoint();
        var cursor = try file.indexCatalogCursor(pinned, "");
        defer cursor.deinit();
        try file.deleteIndexCatalogRecord(&a);
        try file.putIndexCatalogRecord(&m, "updated");
        try file.renameIndexCatalogRecord(&z, "renamed");
        var count: usize = 0;
        while (try cursor.next()) |record| {
            defer alloc.free(record.key);
            if (count == 0) try std.testing.expectEqualStrings("", record.key);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 405), count);
        const old = (try file.getIndexCatalogRecordAtCheckpointAlloc(alloc, &m, pinned)).?;
        defer alloc.free(old);
        try std.testing.expectEqualStrings("m", old);
        try std.testing.expect((try file.check()).valid);
        _ = try file.vacuum();
        try std.testing.expect((try file.check()).valid);
    }
    var reopened = try NativeFile.open(alloc, path, true);
    defer reopened.close();
    const keys = try reopened.snapshotIndexCatalogKeysAlloc(alloc);
    defer NativeFile.freeSnapshotCatalogKeys(alloc, keys);
    try std.testing.expectEqual(@as(usize, 404), keys.len);
    try std.testing.expectEqualStrings("", keys[0].key);
    const m = [_]u8{'m'} ** 4000;
    const value = (try reopened.getIndexCatalogRecordAlloc(alloc, &m)).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("updated", value);
    try std.testing.expect((try reopened.check()).valid);
}

test "lite native overflow key references reject non-record pages and out of checkpoint references" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "corrupt-key-reference.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const key = [_]u8{'k'} ** 1000;
    try file.putIndexCatalogRecord(&key, "value");
    const checkpoint = file.activeCheckpoint();
    const roots = try file.readCatalogRoots(checkpoint.index_catalog_root_page, checkpoint);
    var node = try file.readDocumentIndexNode(roots.index, checkpoint);
    defer node.deinit(alloc);
    node.key_pages.?[0] = roots.index;
    const encoded = try encodeDocumentIndexNode(alloc, node);
    defer alloc.free(encoded);
    try file.writePage(roots.index, .document_index, encoded);
    try std.testing.expectError(error.InvalidDocumentIndex, file.getIndexCatalogRecordAlloc(alloc, &key));
    try std.testing.expect(!(try file.check()).valid);
    node.key_pages.?[0] = checkpoint.page_count;
    const outside = try encodeDocumentIndexNode(alloc, node);
    defer alloc.free(outside);
    try file.writePage(roots.index, .document_index, outside);
    try std.testing.expectError(error.InvalidPageId, file.getIndexCatalogRecordAlloc(alloc, &key));
    try std.testing.expect(!(try file.check()).valid);
    // A well-formed record within the file is still unsafe if no checkpoint
    // history owns it: free-page reclamation must never lose a separator key.
    const orphan = try file.allocatePage("orphan");
    const record = try file.readPagePayloadByKindAlloc(alloc, node.pointers[0], .catalog);
    defer alloc.free(record);
    try file.writePage(orphan, .catalog, record);
    node.key_pages.?[0] = orphan;
    const unowned = try encodeDocumentIndexNode(alloc, node);
    defer alloc.free(unowned);
    try file.writePage(roots.index, .document_index, unowned);
    const readable = (try file.getIndexCatalogRecordAlloc(alloc, &key)).?;
    defer alloc.free(readable);
    try std.testing.expectEqualStrings("value", readable);
    try std.testing.expect(!(try file.check()).valid);
}

test "lite native large appends seal each suffix subtree once" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "bulk-extent-append.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const chunk = file.maxValuePagePayloadBytes();
    const bytes = try alloc.alloc(u8, chunk * 4096);
    defer alloc.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i);
    try file.putIndexCatalogRecord("/wal", bytes);
    const pinned = file.activeCheckpoint();
    const before = file.test_page_writes.load(.monotonic);
    const before_reads = file.test_page_reads.load(.monotonic);
    try file.appendIndexCatalogRecord("/wal", bytes);
    const writes = file.test_page_writes.load(.monotonic) - before;
    // 4096 leaves, 64 branch pages, their parent, a new root, and catalog
    // publication. The old per-leaf path copying wrote 16,453 pages.
    try std.testing.expect(writes <= 4170);
    try std.testing.expect(file.test_page_reads.load(.monotonic) - before_reads <= 20);
    const old = (try file.getIndexCatalogRecordAtCheckpointAlloc(alloc, "/wal", pinned)).?;
    defer alloc.free(old);
    try std.testing.expectEqualSlices(u8, bytes, old);
    const all = (try file.getIndexCatalogRecordAlloc(alloc, "/wal")).?;
    defer alloc.free(all);
    try std.testing.expectEqual(@as(usize, bytes.len * 2), all.len);
    try std.testing.expectEqualSlices(u8, bytes, all[0..bytes.len]);
    try std.testing.expectEqualSlices(u8, bytes, all[bytes.len..]);
    // Cross the same boundaries starting from a partially filled tail.
    try file.putIndexCatalogRecord("/partial", bytes[0 .. bytes.len - 17]);
    const partial_before = file.test_page_writes.load(.monotonic);
    try file.appendIndexCatalogRecord("/partial", bytes);
    try std.testing.expect(file.test_page_writes.load(.monotonic) - partial_before <= 4175);
    const partial = (try file.getIndexCatalogRecordAlloc(alloc, "/partial")).?;
    defer alloc.free(partial);
    try std.testing.expectEqualSlices(u8, bytes[0 .. bytes.len - 17], partial[0 .. bytes.len - 17]);
    try std.testing.expectEqualSlices(u8, bytes, partial[bytes.len - 17 ..]);
    try std.testing.expect((try file.check()).valid);
}

test "lite native batched extent append handles all small tree boundary shapes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "extent-frontier-boundaries.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const chunk = file.maxValuePagePayloadBytes();
    var expected = std.ArrayListUnmanaged(u8).empty;
    defer expected.deinit(alloc);
    for ([_]usize{ 1, chunk - 1, 1, chunk * 62 - 1, chunk, 1, chunk * 65 + 17, 0, chunk * 129 }) |len| {
        const suffix = try alloc.alloc(u8, len);
        defer alloc.free(suffix);
        @memset(suffix, @truncate(expected.items.len));
        try file.appendIndexCatalogRecord("wal", suffix);
        try expected.appendSlice(alloc, suffix);
        const actual = (try file.getIndexCatalogRecordAlloc(alloc, "wal")).?;
        defer alloc.free(actual);
        try std.testing.expectEqualSlices(u8, expected.items, actual);
        try std.testing.expect((try file.check()).valid);
    }
}

test "lite native document overflow keys survive bulk build overwrite and vacuum" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "document-overflow-keys.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const key_bytes = try alloc.alloc(u8, 1000 * 300);
    defer alloc.free(key_bytes);
    @memset(key_bytes, 'k');
    const mutations = try alloc.alloc(DocumentMutation, 300);
    defer alloc.free(mutations);
    for (mutations, 0..) |*mutation, i| {
        const key = key_bytes[i * 1000 ..][0..1000];
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        mutation.* = .{ .key = key, .value = "original" };
    }
    try file.putDocumentBatch(mutations);
    const pinned = file.activeCheckpoint();
    try file.putDocument(mutations[225].key, "new");
    try file.deleteDocument(mutations[226].key);
    const old = (try file.getDocumentAtCheckpointAlloc(alloc, pinned, mutations[225].key)).?;
    defer alloc.free(old);
    try std.testing.expectEqualStrings("original", old);
    try std.testing.expect((try file.check()).valid);
    _ = try file.vacuum();
    const current = (try file.getDocumentAlloc(alloc, mutations[225].key)).?;
    defer alloc.free(current);
    try std.testing.expectEqualStrings("new", current);
    try std.testing.expect((try file.getDocumentAlloc(alloc, mutations[226].key)) == null);
    try std.testing.expect((try file.check()).valid);
}

test "lite native catalog deletion keeps directory scans independent of retired generations" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "live-catalog-deletion.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    for (0..1000) |i| {
        var key: [64]u8 = undefined;
        try file.putIndexCatalogRecord(try std.fmt.bufPrint(&key, "/generation/block-{d:0>8}", .{i}), "x");
    }
    const pinned = file.activeCheckpoint();
    try file.putIndexCatalogRecord("/generation/CURRENT", "live");
    for (0..1000) |i| {
        var key: [64]u8 = undefined;
        try file.deleteIndexCatalogRecord(try std.fmt.bufPrint(&key, "/generation/block-{d:0>8}", .{i}));
    }
    const before = file.test_page_reads.load(.monotonic);
    var cursor = try file.indexCatalogCursor(file.activeCheckpoint(), "/generation/");
    defer cursor.deinit();
    const only = (try cursor.next()).?;
    defer alloc.free(only.key);
    try std.testing.expectEqualStrings("/generation/CURRENT", only.key);
    try std.testing.expect((try cursor.next()) == null);
    try std.testing.expect(file.test_page_reads.load(.monotonic) - before <= 4);
    var old = try file.indexCatalogCursor(pinned, "/generation/");
    defer old.deinit();
    var count: usize = 0;
    while (try old.next()) |record| {
        alloc.free(record.key);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 1000), count);
    try std.testing.expect((try file.check()).valid);
    try file.deleteIndexCatalogRecord("/generation/CURRENT");
    const roots = try file.readCatalogRoots(file.activeCheckpoint().index_catalog_root_page, file.activeCheckpoint());
    try std.testing.expectEqual(@as(u64, 0), roots.index);
    try file.putIndexCatalogRecord("/generation/new", "new");
    try std.testing.expect((try file.check()).valid);
}

test "lite native catalog batches write only final reachable index nodes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catalog-batch-editor.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const keys = try alloc.alloc([16]u8, 1024);
    defer alloc.free(keys);
    const mutations = try alloc.alloc(CatalogMutation, keys.len);
    defer alloc.free(mutations);
    for (mutations, 0..) |*mutation, i| mutation.* = .{ .key = try std.fmt.bufPrint(&keys[i], "key-{d:0>8}", .{i}), .value = "old" };
    for (0..2) |pass| {
        const before = file.activeCheckpoint().page_count;
        try file.putIndexCatalogBatch(mutations);
        const checkpoint = file.activeCheckpoint();
        const roots = try file.readCatalogRoots(checkpoint.index_catalog_root_page, checkpoint);
        var indexed = checkpoint;
        indexed.document_index_root_page = roots.index;
        var reachable = NativeFile.ReachablePageSet{};
        defer reachable.deinit(alloc);
        const nodes = try file.collectDocumentIndexPages(indexed, &reachable, false, false, null);
        // Packed records plus the final index, descriptor and free map.
        // Intermediate copy-on-write paths would exceed this bound.
        try std.testing.expect(checkpoint.page_count - before <= nodes + 16);
        try std.testing.expect(checkpoint.page_count - before <= 1050);
        try std.testing.expect((try file.check()).valid);
        if (pass == 0) for (mutations) |*mutation| {
            mutation.value = "new";
        };
    }
    // Repeated keys preserve input-order semantics without emitting transient
    // tree versions, including removal followed by recreation in one commit.
    const pinned = file.activeCheckpoint();
    try file.putIndexCatalogBatch(&.{
        .{ .key = mutations[0].key, .is_delete = true },
        .{ .key = mutations[0].key, .value = "recreated" },
        .{ .key = mutations[1].key, .value = "temporary" },
        .{ .key = mutations[1].key, .is_delete = true },
    });
    const old = (try file.getIndexCatalogRecordAtCheckpointAlloc(alloc, mutations[0].key, pinned)).?;
    defer alloc.free(old);
    try std.testing.expectEqualStrings("new", old);
    const recreated = (try file.getIndexCatalogRecordAlloc(alloc, mutations[0].key)).?;
    defer alloc.free(recreated);
    try std.testing.expectEqualStrings("recreated", recreated);
    try std.testing.expect((try file.getIndexCatalogRecordAlloc(alloc, mutations[1].key)) == null);
    try std.testing.expect((try file.check()).valid);
}

test "lite native long key updates resolve only comparison keys" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "lazy-key-editor.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    for (0..400) |i| {
        var key: [1000]u8 = @splat('k');
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        try file.putIndexCatalogRecord(&key, "old");
    }
    var key: [1000]u8 = @splat('k');
    _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{@as(usize, 200)});
    const before = file.test_page_reads.load(.monotonic);
    try file.putIndexCatalogRecord(&key, "replacement");
    try std.testing.expect(file.test_page_reads.load(.monotonic) - before <= 32);
    const before_delete = file.test_page_reads.load(.monotonic);
    try file.deleteIndexCatalogRecord(&key);
    try std.testing.expect(file.test_page_reads.load(.monotonic) - before_delete <= 32);
    try std.testing.expect((try file.check()).valid);
}

test "lite native catalog editor rebalances mixed key widths across deletion and reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "mixed-key-editor.aflite");
    defer alloc.free(path);
    const count = 1536;
    const keys = try alloc.alloc([]u8, count);
    defer alloc.free(keys);
    var initialized: usize = 0;
    defer for (keys[0..initialized]) |key| alloc.free(key);
    const widths = [_]usize{ 8, 16, 511, 512, 513, 1000, 4000 };
    for (keys, 0..) |*key, i| {
        key.* = try alloc.alloc(u8, if (i == 0) 0 else widths[i % widths.len]);
        initialized += 1;
        @memset(key.*, 'k');
        if (i != 0) _ = try std.fmt.bufPrint(key.*[0..8], "{d:0>8}", .{i});
    }
    {
        var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
        defer file.close();
        const mutations = try alloc.alloc(CatalogMutation, count);
        defer alloc.free(mutations);
        for (mutations, 0..) |*mutation, i| mutation.* = .{ .key = keys[(i * 1009) % count], .value = "old" };
        try file.putIndexCatalogBatch(mutations);
        const pinned = file.activeCheckpoint();
        try std.testing.expect((try file.check()).valid);
        // Coprime permutations exercise both sides of internal nodes, merging,
        // redistribution, and separator replacement with different slot sizes.
        for (0..4) |phase| {
            for (mutations[0 .. count / 4], 0..) |*mutation, j| mutation.* = .{ .key = keys[((phase * (count / 4) + j) * 1013) % count], .is_delete = true };
            try file.putIndexCatalogBatch(mutations[0 .. count / 4]);
            var cursor = try file.indexCatalogCursor(file.activeCheckpoint(), "");
            defer cursor.deinit();
            var actual: usize = 0;
            while (try cursor.next()) |record| {
                alloc.free(record.key);
                actual += 1;
            }
            try std.testing.expectEqual(count - (phase + 1) * (count / 4), actual);
            try std.testing.expect((try file.check()).valid);
        }
        var old = try file.indexCatalogCursor(pinned, "");
        defer old.deinit();
        var old_count: usize = 0;
        while (try old.next()) |record| {
            alloc.free(record.key);
            old_count += 1;
        }
        try std.testing.expectEqual(@as(usize, count), old_count);
        // Rebuild from an empty live tree while preserving deletion history.
        for (mutations, keys) |*mutation, key| mutation.* = .{ .key = key, .value = "new" };
        try file.putIndexCatalogBatch(mutations);
        try file.renameIndexCatalogRecord(keys[17], "/renamed");
        try std.testing.expect((try file.check()).valid);
        _ = try file.vacuum();
    }
    var reopened = try NativeFile.open(alloc, path, false);
    defer reopened.close();
    try std.testing.expect((try reopened.getIndexCatalogRecordAlloc(alloc, keys[17])) == null);
    const value = (try reopened.getIndexCatalogRecordAlloc(alloc, "/renamed")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("new", value);
    try std.testing.expect((try reopened.check()).valid);
}

test "lite native existing document batches write each changed tree node once" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "document-batch-editor.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const keys = try alloc.alloc([16]u8, 1024);
    defer alloc.free(keys);
    const mutations = try alloc.alloc(DocumentMutation, keys.len);
    defer alloc.free(mutations);
    for (mutations, 0..) |*mutation, i| mutation.* = .{ .key = try std.fmt.bufPrint(&keys[i], "doc-{d:0>8}", .{i}), .value = "old" };
    try file.putDocumentBatch(mutations);
    const pinned = file.activeCheckpoint();
    for (mutations) |*mutation| mutation.value = "new";
    const before = file.activeCheckpoint().page_count;
    try file.putDocumentBatch(mutations);
    const checkpoint = file.activeCheckpoint();
    var reachable = NativeFile.ReachablePageSet{};
    defer reachable.deinit(alloc);
    const nodes = try file.collectDocumentIndexPages(checkpoint, &reachable, false, false, null);
    try std.testing.expect(checkpoint.page_count - before <= nodes + 16);
    try std.testing.expect(checkpoint.page_count - before <= 1050);
    const old = (try file.getDocumentAtCheckpointAlloc(alloc, pinned, mutations[0].key)).?;
    defer alloc.free(old);
    try std.testing.expectEqualStrings("old", old);
    const current = (try file.getDocumentAlloc(alloc, mutations[0].key)).?;
    defer alloc.free(current);
    try std.testing.expectEqualStrings("new", current);
    try std.testing.expect((try file.check()).valid);
}

test "lite native catalog editor mixed batches match a reference map" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "model-catalog-editor.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const count = 192;
    var expected: [count]?u64 = @splat(null);
    const keys = try alloc.alloc([]u8, count);
    defer alloc.free(keys);
    var initialized: usize = 0;
    defer for (keys[0..initialized]) |key| alloc.free(key);
    const widths = [_]usize{ 8, 511, 512, 513, 1000 };
    for (keys, 0..) |*key, i| {
        key.* = try alloc.alloc(u8, if (i == 0) 0 else widths[i % widths.len]);
        initialized += 1;
        @memset(key.*, 'm');
        if (i != 0) _ = try std.fmt.bufPrint(key.*[0..8], "{d:0>8}", .{i});
    }
    var rng = std.Random.DefaultPrng.init(803);
    const random = rng.random();
    for (0..120) |round| {
        var values: [24][8]u8 = undefined;
        var mutations: [24]CatalogMutation = undefined;
        for (&mutations, 0..) |*mutation, i| {
            const selected = random.uintLessThan(usize, count);
            const deleted = random.uintLessThan(u8, 3) == 0;
            const version: u64 = round * mutations.len + i;
            std.mem.writeInt(u64, &values[i], version, .little);
            mutation.* = .{ .key = keys[selected], .value = &values[i], .is_delete = deleted };
            expected[selected] = if (deleted) null else version;
        }
        try file.putIndexCatalogBatch(&mutations);
        if (round % 12 == 0 or round == 119) {
            for (keys, expected) |key, want| {
                const actual = try file.getIndexCatalogRecordAlloc(alloc, key);
                defer if (actual) |value| alloc.free(value);
                if (want) |version| {
                    try std.testing.expect(actual != null);
                    try std.testing.expectEqual(version, std.mem.readInt(u64, actual.?[0..8], .little));
                } else try std.testing.expect(actual == null);
            }
            try std.testing.expect((try file.check()).valid);
        }
    }
}

const MaintenanceTestAllocator = @import("test_allocator.zig").BudgetAllocator;

test "lite native maintenance streams large values under a bounded heap budget" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "bounded-maintenance.aflite");
    defer alloc.free(path);
    const value = try alloc.alloc(u8, 2 * 1024 * 1024 + 137);
    defer alloc.free(value);
    for (value, 0..) |*byte, i| byte.* = @intCast(i % 251);
    var budget = MaintenanceTestAllocator{ .backing = alloc };
    {
        var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        try file.putCatalogRecord("schema", value);
        try file.putIndexCatalogRecord("vectors", value[0..4301]);
        try file.appendIndexCatalogRecord("vectors", value[4301..5298]);
        try file.appendIndexCatalogRecord("vectors", value[5298..]);
        try file.putDocument("docs\x00large", value);
        budget.peak = budget.live;
        budget.limit = budget.live + 512 * 1024;
        const reads = file.test_page_reads.load(.monotonic);
        const stats = try file.liveStats(null);
        try std.testing.expectEqual(@as(u64, 3), stats.record_count);
        try std.testing.expectEqual(@as(u64, value.len * 3 + "schema".len + "vectors".len + "docs\x00large".len), stats.bytes);
        // Statistics must not read any of the thousands of value pages.
        try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 12);
        const checked = try file.check();
        try std.testing.expect(checked.valid);
        try std.testing.expectEqual(stats.compact_size, checked.compact_size);

        // Cancel during value copying, after the temporary output exists.
        const checkpoint = file.activeCheckpoint();
        var cancel = maintenance.CancelToken{};
        budget.cancel = &cancel;
        budget.cancel_after = 40;
        try std.testing.expectError(error.MaintenanceCanceled, file.vacuumWithCancel(&cancel));
        try std.testing.expectError(error.MaintenanceCanceled, file.liveStats(&cancel));
        budget.cancel = null;
        budget.cancel_after = std.math.maxInt(usize);
        try std.testing.expect(std.meta.eql(checkpoint, file.activeCheckpoint()));
        const temp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-aflite-vacuum", .{path});
        defer alloc.free(temp_path);
        try std.testing.expect(!pathExists(std.testing.io, temp_path));

        const vacuumed = try file.vacuum();
        try std.testing.expectEqual(stats.compact_size, vacuumed.after_size);
        try std.testing.expectEqual(stats.bytes, vacuumed.live_bytes);
        try std.testing.expect((try file.check()).valid);
        try std.testing.expect(budget.peak <= budget.limit);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    var reopened = try NativeFile.open(alloc, path, true);
    defer reopened.close();
    const catalog = (try reopened.getCatalogRecordAlloc(alloc, "schema")).?;
    defer alloc.free(catalog);
    try std.testing.expectEqualSlices(u8, value, catalog);
    const index = (try reopened.getIndexCatalogRecordAlloc(alloc, "vectors")).?;
    defer alloc.free(index);
    try std.testing.expectEqualSlices(u8, value, index);
    const document = (try reopened.getDocumentAlloc(alloc, "docs\x00large")).?;
    defer alloc.free(document);
    try std.testing.expectEqualSlices(u8, value, document);
}

test "lite native vacuum reads live indexes instead of superseded history" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "indexed-vacuum.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    for (0..1000) |i| {
        var value: [8]u8 = undefined;
        std.mem.writeInt(u64, &value, i, .little);
        try file.putCatalogRecord("schema", &value);
        try file.putIndexCatalogRecord("index", &value);
        try file.putDocument("doc", &value);
    }
    const stats = try file.liveStats(null);
    const reads = file.test_page_reads.load(.monotonic);
    const vacuumed = try file.vacuum();
    try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 16);
    try std.testing.expectEqual(@as(u64, 3), vacuumed.live_file_count);
    try std.testing.expectEqual(stats.compact_size, vacuumed.after_size);
    const document = (try file.getDocumentAlloc(alloc, "doc")).?;
    defer alloc.free(document);
    try std.testing.expectEqual(@as(u64, 999), std.mem.readInt(u64, document[0..8], .little));
    try std.testing.expect((try file.check()).valid);
}

test "lite native compact statistics match packed live document and namespace indexes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "packed-stats.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    for (0..512) |i| {
        var key: [32]u8 = undefined;
        const formatted = try std.fmt.bufPrint(&key, "namespace-{d:0>4}\x00doc", .{i});
        try file.putDocument(formatted, "value");
        if (i != 511) try file.deleteDocument(formatted);
    }
    const before = try file.check();
    try std.testing.expect(before.valid);
    const vacuumed = try file.vacuum();
    try std.testing.expectEqual(before.compact_size, vacuumed.after_size);
    try std.testing.expectEqual(before.live_bytes, vacuumed.live_bytes);
    try std.testing.expectEqual(@as(u64, 1), vacuumed.live_file_count);
    const after = try file.check();
    try std.testing.expect(after.valid);
    try std.testing.expectEqual(after.file_size, after.compact_size);
    try std.testing.expectEqual(@as(u64, 0), after.reclaimable_bytes);
}

test "lite native streaming vacuum rejects corrupt values before publication" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "stream-corruption.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    const value = try alloc.alloc(u8, default_page_size * 3);
    defer alloc.free(value);
    @memset(value, 'v');
    try file.putIndexCatalogRecord("index", value);
    try file.putDocument("doc", value);
    const checkpoint = file.activeCheckpoint();
    const temp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-aflite-vacuum", .{path});
    defer alloc.free(temp_path);

    const catalog_page = (try file.lookupCatalogPage(checkpoint, .index, "index")).?;
    const catalog_payload = try file.readPagePayloadByKindAlloc(alloc, catalog_page, .catalog);
    defer alloc.free(catalog_payload);
    const catalog = try decodeCatalogEntry(catalog_payload);
    const extent = try file.readPagePayloadByKindAlloc(alloc, catalog.external_value_root_page, .value_extent);
    defer alloc.free(extent);
    extent[NativeFile.extent_header_size + 8] ^= 1;
    try file.writePage(catalog.external_value_root_page, .value_extent, extent);
    try std.testing.expectError(error.InvalidNativeValueChain, file.vacuum());
    try std.testing.expect(std.meta.eql(checkpoint, file.activeCheckpoint()));
    try std.testing.expect(!pathExists(std.testing.io, temp_path));
    try std.testing.expect(!(try file.check()).valid);
    extent[NativeFile.extent_header_size + 8] ^= 1;
    try file.writePage(catalog.external_value_root_page, .value_extent, extent);

    const document_payload = try file.readPagePayloadByKindAlloc(alloc, checkpoint.document_root_page, .document);
    defer alloc.free(document_payload);
    const document = try decodeDocumentEntry(document_payload);
    const first = try file.readPagePayloadByKindAlloc(alloc, document.external_value_root_page, .value);
    defer alloc.free(first);
    const next = std.mem.readInt(u64, first[0..8], .little);
    for ([_]u64{ 0, document.external_value_root_page }) |bad_next| {
        std.mem.writeInt(u64, first[0..8], bad_next, .little);
        try file.writePage(document.external_value_root_page, .value, first);
        try std.testing.expectError(error.InvalidNativeValueChain, file.vacuum());
        try std.testing.expect(std.meta.eql(checkpoint, file.activeCheckpoint()));
        try std.testing.expect(!pathExists(std.testing.io, temp_path));
    }
    std.mem.writeInt(u64, first[0..8], next, .little);
    try file.writePage(document.external_value_root_page, .value, first);
    // A late checksum failure must also discard pages already copied.
    const raw = try file.readPageAlloc(alloc, next);
    defer alloc.free(raw);
    raw[page_header_size + value_page_header_size] ^= 1;
    try file.file.writePositionalAll(file.runtimeIo(), raw, next * default_page_size);
    try std.testing.expectError(error.NativePageChecksumMismatch, file.vacuum());
    try std.testing.expect(std.meta.eql(checkpoint, file.activeCheckpoint()));
    try std.testing.expect(!pathExists(std.testing.io, temp_path));
    raw[page_header_size + value_page_header_size] ^= 1;
    try file.file.writePositionalAll(file.runtimeIo(), raw, next * default_page_size);
    _ = try file.vacuum();
    try std.testing.expect((try file.check()).valid);
}

test "lite native bulk index frontier counts a million long keys with bounded heap" {
    var budget = MaintenanceTestAllocator{ .backing = std.testing.allocator, .limit = 256 * 1024 };
    {
        var file = NativeFile{ .allocator = budget.allocator(), .io_impl = undefined, .borrowed_io = std.testing.io, .path = @constCast(""), .file = undefined, .header = .{} };
        var pages: u64 = 1;
        var builder = DocumentIndexBulkBuilder{ .owner = &file, .file = undefined, .next_page_id = &pages, .count_only = true };
        defer builder.deinit();
        for (0..1_000_000) |i| {
            var key: [4000]u8 = @splat('k');
            _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
            try builder.add(&key, 1);
        }
        try std.testing.expect(try builder.finish() != 0);
        try std.testing.expect(pages > 4000);
        try std.testing.expect(builder.levels.items.len >= 3);
        try std.testing.expect(budget.peak <= budget.limit);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "lite native bulk index frontier preserves mixed keys and exact page counts" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "bulk-frontier.aflite");
    defer alloc.free(path);
    var budget = MaintenanceTestAllocator{ .backing = alloc, .limit = 256 * 1024 };
    {
        var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        const count = 16384;
        var next: u64 = 1;
        var counted: u64 = 1;
        var builder = DocumentIndexBulkBuilder{ .owner = &file, .file = file.file, .next_page_id = &next };
        defer builder.deinit();
        var counter = DocumentIndexBulkBuilder{ .owner = &file, .file = undefined, .next_page_id = &counted, .count_only = true };
        defer counter.deinit();
        var history: u64 = 0;
        for (0..count) |i| {
            var key: [4000]u8 = @splat('k');
            _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
            const widths = [_]usize{ 16, 512, 4000 };
            const bytes = key[0..if (i == 0) 0 else widths[i % widths.len]];
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(budget.allocator());
            try encodeCatalogEntry(budget.allocator(), &payload, .{ .previous_page = history, .key = bytes, .value = "v" });
            history = try appendPageToFile(budget.allocator(), file.file, std.testing.io, default_page_size, &next, .catalog, payload.items);
            try builder.add(bytes, history);
            try counter.add(bytes, history);
        }
        const root = try builder.finish();
        _ = try counter.finish();
        try std.testing.expectEqual(next - count, counted);
        try std.testing.expect(builder.levels.items.len >= 2);
        try std.testing.expect(budget.peak <= budget.limit);
        // Decoding a cursor's long keys has its own path-sized memory bound;
        // the cap above isolates the builder and its counting mode.
        budget.limit = std.math.maxInt(usize);
        var cursor = DocumentIndexCursor.init(&file, .{ .page_count = next, .document_index_root_page = root });
        defer cursor.deinit();
        var entry = try cursor.first();
        var seen: usize = 0;
        while (entry) |value| : (entry = try cursor.next()) {
            var owned = value;
            defer owned.deinit(budget.allocator());
            var key: [4000]u8 = @splat('k');
            _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{seen});
            const widths = [_]usize{ 16, 512, 4000 };
            try std.testing.expectEqualSlices(u8, key[0..if (seen == 0) 0 else widths[seen % widths.len]], value.key);
            seen += 1;
        }
        try std.testing.expectEqual(@as(usize, count), seen);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "lite native bulk index frontier releases ownership on allocation failures" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var file = NativeFile{ .allocator = alloc, .io_impl = undefined, .borrowed_io = std.testing.io, .path = @constCast(""), .file = undefined, .header = .{} };
            var pages: u64 = 1;
            var builder = DocumentIndexBulkBuilder{ .owner = &file, .file = undefined, .next_page_id = &pages, .count_only = true };
            defer builder.deinit();
            for (0..80) |i| {
                var key: [512]u8 = @splat('k');
                _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
                try builder.add(&key, 1);
            }
            try std.testing.expect(try builder.finish() != 0);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "lite native cold payload writes invalidate reused page bytes and links" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "cold-reuse.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const id = try file.allocatePage("old");
    const ids = [_]u64{id};
    // Exercise the overwrite boundary directly, including a cached link from
    // the page's previous lifetime. A cold replacement must invalidate both.
    try file.writeValuePageChunk(&ids, 0, "old payload", .{});
    try std.testing.expect(file.page_cache.pages.contains(id));
    try std.testing.expect(file.page_cache.links.contains(id));
    try file.writeValuePageChunk(&ids, 0, "cold replacement", .{ .payload_cache = .cold_sequential });
    try std.testing.expect(!file.page_cache.pages.contains(id));
    try std.testing.expect(!file.page_cache.links.contains(id));
    const page = try file.readPageAlloc(alloc, id);
    defer alloc.free(page);
    const payload = try decodePagePayloadAlloc(alloc, page, .value);
    defer alloc.free(payload);
    try std.testing.expectEqualStrings("cold replacement", payload[value_page_header_size..]);
    // A subsequent read can admit cold-written data normally.
    try std.testing.expect(file.page_cache.pages.contains(id));
}

test "lite native page batches coalesce runs without heap allocation and preserve gaps" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "page-batch.aflite");
    defer alloc.free(path);
    var budget = MaintenanceTestAllocator{ .backing = alloc };
    var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    budget.limit = budget.live; // Neither encoding nor batching may allocate.
    for ([_]u32{ 4096, 65536 }) |size| {
        file.header.page_size = size;
        const run: u64 = PageWriteBatch.capacity / size;
        var batch = PageWriteBatch{ .file = &file };
        const before = file.test_page_write_calls.load(.monotonic);
        // Keep a sentinel between disjoint runs and visit the second run in
        // descending page order, as can happen with a fragmented free map.
        try file.writePage(100, .data, "untouched gap");
        for (1..@intCast(run + 2)) |id| try batch.appendValue(id, 0, "batched value");
        try batch.appendPage(102, .data, "high page");
        try batch.appendPage(101, .data, "low page");
        try batch.flush();
        try std.testing.expectEqual(@as(u64, 5), file.test_page_write_calls.load(.monotonic) - before);
        var raw: [65536]u8 = undefined;
        try readExactAt(file.file, std.testing.io, raw[0..size], @as(u64, size) * 100);
        try std.testing.expectEqualStrings("untouched gap", try decodePagePayload(raw[0..size], .data));
        for (1..@intCast(run + 2)) |id| {
            try readExactAt(file.file, std.testing.io, raw[0..size], @as(u64, size) * id);
            const value = try decodeValuePage(try decodePagePayload(raw[0..size], .value));
            try std.testing.expectEqualStrings("batched value", value.chunk);
            try std.testing.expectEqual(@as(u64, 0), value.next_page);
        }
        try readExactAt(file.file, std.testing.io, raw[0..size], @as(u64, size) * 101);
        try std.testing.expectEqualStrings("low page", try decodePagePayload(raw[0..size], .data));
        try readExactAt(file.file, std.testing.io, raw[0..size], @as(u64, size) * 102);
        try std.testing.expectEqualStrings("high page", try decodePagePayload(raw[0..size], .data));
    }
}

test "lite native failed page batches do not admit cache entries or retry" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "failed-page-batch.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    const first = file.activeCheckpoint().page_count;
    var batch = PageWriteBatch{ .file = &file };
    try batch.appendValue(first, 0, "private");
    try std.testing.expect(!file.page_cache.pages.contains(first));
    file.test_page_write_fail_after = 0;
    try std.testing.expectError(error.TestPageWriteFailure, batch.flush());
    try std.testing.expect(!file.page_cache.pages.contains(first));
    try std.testing.expect(!file.page_cache.links.contains(first));
    file.test_page_write_fail_after = null;
    try std.testing.expectError(error.TestPageWriteFailure, batch.appendValue(first + 1, 0, "retry"));
    try std.testing.expectError(error.TestPageWriteFailure, batch.flush());
    var fresh = PageWriteBatch{ .file = &file };
    try fresh.appendValue(first, 0, "fresh");
    try fresh.flush();
    try std.testing.expect(file.page_cache.pages.contains(first));
    try std.testing.expect(file.page_cache.links.contains(first));
}

test "lite native CLOCK retains hot pages across cold scans and skips oversized admission" {
    const alloc = std.testing.allocator;
    var cache = PageCache{ .limit_bytes = 128 };
    defer cache.deinit(alloc);
    cache.put(alloc, 1, "hot-page");
    for (2..4096) |id| {
        var hot: [8]u8 = undefined;
        try std.testing.expect(cache.copyInto(1, &hot));
        cache.put(alloc, id, "coldpage");
        try std.testing.expect(cache.total_bytes <= 128);
    }
    try std.testing.expect(cache.pages.count() > 1);
    cache.put(alloc, 9999, &([_]u8{0} ** 129));
    try std.testing.expect(!cache.pages.contains(9999));
    var hot: [8]u8 = undefined;
    try std.testing.expect(cache.copyInto(1, &hot));
}

test "lite native transaction publishes catalog and document roots atomically" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "native-root-transaction.aflite");
    defer alloc.free(path);
    {
        var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{});
        defer file.close();
        const before = file.activeCheckpoint().commit_sequence;
        try file.beginTransaction();
        try file.putDocument("document", "value");
        try file.putIndexCatalogRecord("index", "segment");
        try file.putCatalogRecord("metadata", "settings");
        // Independent opens observe only the last durable checkpoint.
        {
            var old = try NativeFile.openWithIo(alloc, std.testing.io, path, .{ .read_only = true });
            defer old.close();
            try std.testing.expect((try old.getDocumentAlloc(alloc, "document")) == null);
        }
        try file.commitTransaction();
        try std.testing.expectEqual(before + 1, file.activeCheckpoint().commit_sequence);
        try file.beginTransaction();
        try file.putDocument("document", "aborted");
        try file.deleteIndexCatalogRecord("index");
        file.abortTransaction();
        const document = (try file.getDocumentAlloc(alloc, "document")).?;
        defer alloc.free(document);
        try std.testing.expectEqualStrings("value", document);
        const index = (try file.getIndexCatalogRecordAlloc(alloc, "index")).?;
        defer alloc.free(index);
        try std.testing.expectEqualStrings("segment", index);
        try std.testing.expect((try file.check()).valid);
    }
    var reopened = try NativeFile.openWithIo(alloc, std.testing.io, path, .{ .read_only = true });
    defer reopened.close();
    const metadata = (try reopened.getCatalogRecordAlloc(alloc, "metadata")).?;
    defer alloc.free(metadata);
    try std.testing.expectEqualStrings("settings", metadata);
    try std.testing.expect((try reopened.check()).valid);
}

test "lite native small document batches coalesce record and index writes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "small-record-batch.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    var keys: [1024][16]u8 = undefined;
    var mutations: [1024]DocumentMutation = undefined;
    for (&keys, &mutations, 0..) |*key, *mutation, i| mutation.* = .{ .key = try std.fmt.bufPrint(key, "key-{d:0>8}", .{i}), .value = "small" };
    const before = file.test_page_write_calls.load(.monotonic);
    try file.putDocumentBatch(&mutations);
    try std.testing.expect(file.test_page_write_calls.load(.monotonic) - before <= 80);
    const reads = file.test_page_reads.load(.monotonic);
    var requested: [1024][]const u8 = undefined;
    for (mutations, &requested) |mutation, *key| key.* = mutation.key;
    var values: [1024]?[]const u8 = undefined;
    try file.getDocumentsAtCheckpointAlloc(alloc, file.activeCheckpoint(), &requested, &values);
    defer for (values) |value| if (value) |bytes| alloc.free(bytes);
    for (values) |value| try std.testing.expectEqualStrings("small", value.?);
    // Each physical bundle and shared index node is read once.
    try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 40);
}

test "lite deferred durability preserves the durable roots until a shared barrier" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "deferred-durability.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{});
    defer file.close();
    try file.putIndexCatalogRecord("wal", "first");
    for (0..4) |_| {
        try file.beginTransaction();
        try file.appendIndexCatalogRecord("wal", "+");
        try file.commitTransactionWithDurability(false);
    }
    const live = (try file.getIndexCatalogRecordAlloc(alloc, "wal")).?;
    defer alloc.free(live);
    try std.testing.expectEqualStrings("first++++", live);
    {
        var disk = try NativeFile.openWithIo(alloc, std.testing.io, path, .{ .read_only = true });
        defer disk.close();
        const value = (try disk.getIndexCatalogRecordAlloc(alloc, "wal")).?;
        defer alloc.free(value);
        try std.testing.expectEqualStrings("first", value);
    }
    try file.sync();
    var reopened = try NativeFile.openWithIo(alloc, std.testing.io, path, .{ .read_only = true });
    defer reopened.close();
    const durable = (try reopened.getIndexCatalogRecordAlloc(alloc, "wal")).?;
    defer alloc.free(durable);
    try std.testing.expectEqualStrings("first++++", durable);
    try std.testing.expect((try reopened.check()).valid);
}

test "lite compaction catches final mutations across all roots and streams large values" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "compaction-catch-up.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try file.putDocument("deleted", "old");
    try file.putIndexCatalogRecord("old-name", "index");
    var source = try NativeFile.openWithIo(alloc, std.testing.io, path, .{ .read_only = true, .no_sync = true });
    defer source.close();
    var capture = ChangeCapture{};
    defer capture.deinit(alloc);
    file.change_capture = &capture;
    defer file.change_capture = null;
    var image = try source.prepareVacuum(null);
    defer image.deinit();
    const large = try alloc.alloc(u8, 2 * 1024 * 1024);
    defer alloc.free(large);
    @memset(large, 123);
    try file.deleteDocument("deleted");
    try file.putDocument("large", large);
    try file.renameIndexCatalogRecord("old-name", "new-name");
    try file.putCatalogRecord("secret-head", "revision-1");
    try file.putCatalogRecord("secret-head", "revision-2");
    try file.putIndexCatalogRecord("large-index", large);
    try file.applyCapturedChanges(&image.prepared, &capture, &image.report, null);
    try file.publishVacuum(&image);
    try std.testing.expect((try file.getDocumentAlloc(alloc, "deleted")) == null);
    try std.testing.expect((try file.getIndexCatalogRecordAlloc(alloc, "old-name")) == null);
    const doc = (try file.getDocumentAlloc(alloc, "large")).?;
    defer alloc.free(doc);
    try std.testing.expectEqualSlices(u8, large, doc);
    const index = (try file.getIndexCatalogRecordAlloc(alloc, "large-index")).?;
    defer alloc.free(index);
    try std.testing.expectEqualSlices(u8, large, index);
    const secret = (try file.getCatalogRecordAlloc(alloc, "secret-head")).?;
    defer alloc.free(secret);
    try std.testing.expectEqualStrings("revision-2", secret);
    const report = try file.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(report.live_file_count, image.report.live_file_count);
    try std.testing.expectEqual(report.live_bytes, image.report.live_bytes);
}

test "lite unpacked v3 remains readable and vacuum explicitly adopts packed signature" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "unpacked-v3.aflite");
    defer alloc.free(path);
    {
        var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.header.packed_records = false;
        var header: [header_size]u8 = undefined;
        encodeHeader(&header, file.header);
        try file.file.writePositionalAll(std.testing.io, &header, 0);
        try file.putDocumentBatch(&.{ .{ .key = "a", .value = "one" }, .{ .key = "b", .value = "two" } });
        try std.testing.expect(file.activeCheckpoint().document_root_page & packed_record_flag == 0);
    }
    var file = try NativeFile.openWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try std.testing.expect(!file.header.packed_records);
    _ = try file.vacuum();
    try std.testing.expect(file.header.packed_records);
    var header: [header_size]u8 = undefined;
    try readHeaderExactAt(file.file, std.testing.io, &header);
    try std.testing.expect(!std.mem.eql(u8, header[0..magic.len], unpacked_v3_magic));
    try std.testing.expect((try decodeHeader(&header)).packed_records);
    const value = (try file.getDocumentAlloc(alloc, "b")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("two", value);
    try std.testing.expect((try file.check()).valid);
}

test "lite packed record references validate boundaries and physical checksums" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "packed-record-validation.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try file.putDocumentBatch(&.{ .{ .key = "a", .value = "one" }, .{ .key = "b", .value = "two" } });
    const checkpoint = file.activeCheckpoint();
    const reference = (try file.lookupDocumentIndexPage(checkpoint, "a")).?;
    try std.testing.expect(reference & packed_record_flag != 0);
    try std.testing.expectError(error.InvalidPageId, file.readPageAlloc(alloc, reference + (@as(u64, 1) << 47)));
    file.page_cache_enabled.store(false, .monotonic);
    try file.file.writePositionalAll(std.testing.io, "X", physicalPage(reference) * file.header.page_size + page_header_size);
    try std.testing.expectError(error.NativePageChecksumMismatch, file.getDocumentAlloc(alloc, "a"));
    try std.testing.expect(!(try file.check()).valid);
}

test "lite page replacement invalidates old bytes even when shared pressure bypasses admission" {
    const alloc = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lite_native_page_cache)] = .{ .soft_limit_bytes = 16, .hard_limit_bytes = 24 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var cache = PageCache{ .limit_bytes = 128 };
    defer cache.deinit(alloc);
    cache.attachResourceManager(&manager);
    cache.put(alloc, 1, "old-page");
    cache.put(alloc, 2, "other");
    var external: u64 = 0;
    manager.observeUsage(.lite_native_page_cache, &external, 32);
    defer manager.observeUsage(.lite_native_page_cache, &external, 0);
    try std.testing.expectEqual(.hard, manager.sliceStats(.lite_native_page_cache).pressure);
    cache.put(alloc, 1, "new-page");
    try std.testing.expect((try cache.getCopy(alloc, 1)) == null);
    try std.testing.expectEqual(@as(usize, 0), cache.total_bytes);
}

test "lite vacuum catchup batches every root and accounts private caches through failure and publication" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "bounded-catchup.aflite");
    defer alloc.free(path);
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lite_native_page_cache)] = .{ .soft_limit_bytes = 32768, .hard_limit_bytes = 65536 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    {
        var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true, .resource_manager = &manager });
        defer file.close();
        var image = try file.prepareVacuum(null);
        defer image.deinit();
        try std.testing.expect(image.prepared.page_cache.resource_manager == &manager);
        var capture = ChangeCapture{};
        defer capture.deinit(alloc);
        file.change_capture = &capture;
        defer file.change_capture = null;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const keys = try arena.allocator().alloc([]const u8, 4096);
        for (keys, 0..) |*key, i| key.* = try std.fmt.allocPrint(arena.allocator(), "key-{d:0>8}", .{i});
        const docs = try arena.allocator().alloc(DocumentMutation, keys.len);
        const catalog = try arena.allocator().alloc(CatalogMutation, keys.len);
        for (keys, docs, catalog) |key, *doc, *entry| {
            doc.* = .{ .key = key, .value = "small payload" };
            entry.* = .{ .key = key, .value = "small payload" };
        }
        try file.putDocumentBatch(docs);
        try file.putCatalogBatch(catalog);
        try file.putIndexCatalogBatch(catalog);
        const before = image.prepared.activeCheckpoint();
        const report_before = image.report;
        // Fail after earlier chunks have written and changed private roots.
        image.prepared.test_page_write_fail_after = 5;
        try std.testing.expectError(error.TestPageWriteFailure, file.applyCapturedChanges(&image.prepared, &capture, &image.report, null));
        image.prepared.test_page_write_fail_after = null;
        try std.testing.expectEqualDeep(before, image.prepared.activeCheckpoint());
        try std.testing.expectEqualDeep(report_before, image.report);
        try std.testing.expectEqual(before.page_count * file.header.page_size, (try image.prepared.file.stat(std.testing.io)).size);
        try std.testing.expect((try image.prepared.check()).valid);
        var cancel = maintenance.CancelToken{};
        cancel.request();
        try std.testing.expectError(error.MaintenanceCanceled, file.applyCapturedChanges(&image.prepared, &capture, &image.report, &cancel));
        try std.testing.expectEqualDeep(report_before, image.report);
        // Retry with a streamed value that reuses IDs previously occupied by
        // cached index pages in the aborted tail.
        const large = try alloc.alloc(u8, 1024 * 1024);
        defer alloc.free(large);
        @memset(large, 42);
        try file.putCatalogRecord(keys[0], large);
        try file.applyCapturedChanges(&image.prepared, &capture, &image.report, null);
        const streamed = (try image.prepared.getCatalogRecordAlloc(alloc, keys[0])).?;
        defer alloc.free(streamed);
        try std.testing.expectEqualSlices(u8, large, streamed);
        const stats = manager.sliceStats(.lite_native_page_cache);
        try std.testing.expect(stats.used_bytes <= stats.hard_limit_bytes);
        try std.testing.expectEqual(file.page_cache.total_bytes + image.prepared.page_cache.total_bytes, stats.used_bytes);
        try std.testing.expect(image.report.after_size < file.activeCheckpoint().page_count * file.header.page_size * 2);
        try std.testing.expect(image.prepared.test_page_writes.load(.monotonic) < keys.len);
        const checked = try image.prepared.check();
        try std.testing.expect(checked.valid);
        try std.testing.expectEqual(@as(usize, 3 * keys.len), image.report.live_file_count);
        try std.testing.expectEqual(checked.live_file_count, image.report.live_file_count);
        try std.testing.expectEqual(checked.live_bytes, image.report.live_bytes);
        try file.publishVacuum(&image);
        const value = (try file.getDocumentAlloc(alloc, keys[keys.len - 1])).?;
        defer alloc.free(value);
        try std.testing.expectEqualStrings("small payload", value);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lite_native_page_cache).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lite_native_link_cache).used_bytes);
}

test "lite packed record reader reuses validated pages in both directions and rejects invalid views" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "record-page-reader.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try file.putDocumentBatch(&.{ .{ .key = "a", .value = "one" }, .{ .key = "b", .value = "two" }, .{ .key = "c", .value = "three" } });
    const checkpoint = file.activeCheckpoint();
    const a = (try file.lookupDocumentIndexPage(checkpoint, "a")).?;
    const b = (try file.lookupDocumentIndexPage(checkpoint, "b")).?;
    const c = (try file.lookupDocumentIndexPage(checkpoint, "c")).?;
    try std.testing.expectEqual(physicalPage(a), physicalPage(c));
    var reader = RecordPageReader{};
    defer reader.deinit(alloc);
    const before = file.test_page_reads.load(.monotonic);
    for ([_]u64{ a, b, c, b, a }) |ref| _ = try reader.read(&file, alloc, checkpoint, ref, .document);
    try std.testing.expectEqual(@as(u64, 1), file.test_page_reads.load(.monotonic) - before);
    try std.testing.expectError(error.InvalidPageId, reader.read(&file, alloc, checkpoint, a + (@as(u64, 1) << 47), .document));
    try std.testing.expectError(error.InvalidNativePageKind, reader.read(&file, alloc, checkpoint, a, .catalog));
    try std.testing.expectError(error.InvalidNativePageKind, reader.read(&file, alloc, checkpoint, physicalPage(a), .document));
    try std.testing.expectEqualStrings("one", (try decodeDocumentEntry(try reader.read(&file, alloc, checkpoint, a, .document))).value);
    reader.deinit(alloc);
    file.page_cache_enabled.store(false, .monotonic);
    const raw = try file.readPhysicalPageAlloc(alloc, physicalPage(a), checkpoint);
    defer alloc.free(raw);
    try file.file.writePositionalAll(std.testing.io, "X", physicalPage(a) * file.header.page_size + page_header_size);
    try std.testing.expectError(error.NativePageChecksumMismatch, reader.read(&file, alloc, checkpoint, a, .document));
    // An unsuccessful load must not cache a validated view; retry disk truth.
    try file.file.writePositionalAll(std.testing.io, raw, physicalPage(a) * file.header.page_size);
    try std.testing.expectEqualStrings("two", (try decodeDocumentEntry(try reader.read(&file, alloc, checkpoint, b, .document))).value);
}

test "lite vacuum catchup bounds retained inline bytes and rolls back mid-copy cancellation" {
    const alloc = std.testing.allocator;
    const BudgetAllocator = @import("test_allocator.zig").BudgetAllocator;
    var budget = BudgetAllocator{ .backing = alloc };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catchup-inline-budget.aflite");
    defer alloc.free(path);
    {
        var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        var image = try file.prepareVacuum(null);
        defer image.deinit();
        image.prepared.page_cache_enabled.store(false, .monotonic);
        var capture = ChangeCapture{};
        defer capture.deinit(budget.allocator());
        file.change_capture = &capture;
        defer file.change_capture = null;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const docs = try arena.allocator().alloc(DocumentMutation, 4096);
        const value = [_]u8{42} ** 3000;
        for (docs, 0..) |*doc, i| doc.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "key-{d:0>8}", .{i}), .value = &value };
        try file.putDocumentBatch(docs);
        const before = image.prepared.activeCheckpoint();
        const report_before = image.report;
        budget.peak = budget.live;
        budget.limit = budget.live + 5 * 1024 * 1024;
        var cancel = maintenance.CancelToken{};
        budget.cancel = &cancel;
        budget.cancel_after = 4000;
        try std.testing.expectError(error.MaintenanceCanceled, file.applyCapturedChanges(&image.prepared, &capture, &image.report, &cancel));
        try std.testing.expect(image.prepared.test_page_writes.load(.monotonic) > 0);
        try std.testing.expectEqualDeep(before, image.prepared.activeCheckpoint());
        try std.testing.expectEqualDeep(report_before, image.report);
        budget.cancel = null;
        try file.applyCapturedChanges(&image.prepared, &capture, &image.report, null);
        try std.testing.expect(budget.peak <= budget.limit);
        try std.testing.expectEqual(@as(usize, docs.len), image.report.live_file_count);
        const copied = (try image.prepared.getDocumentAlloc(alloc, docs[docs.len - 1].key)).?;
        defer alloc.free(copied);
        try std.testing.expectEqualSlices(u8, &value, copied);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "lite rollback drops only tail cache pages and independently cached links" {
    const alloc = std.testing.allocator;
    var manager = resource_manager_mod.ResourceManager.init(.{});
    var cache = PageCache{};
    defer cache.deinit(alloc);
    cache.attachResourceManager(&manager);
    cache.put(alloc, 1, "kept");
    cache.put(alloc, 2, "dropped");
    cache.put(alloc, 3, "also dropped");
    cache.putLinks(alloc, 1, .{ .kind = .value, .link_page = 0 });
    cache.putLinks(alloc, 2, .{ .kind = .value, .link_page = 0 });
    cache.putLinks(alloc, 4, .{ .kind = .value, .link_page = 0 });
    cache.discardFrom(alloc, 2);
    try std.testing.expectEqual(@as(usize, 1), cache.pages.count());
    try std.testing.expect(cache.pages.contains(1));
    try std.testing.expectEqual(@as(usize, 1), cache.links.count());
    try std.testing.expect(cache.links.contains(1));
    try std.testing.expectEqual(@as(u64, 4), manager.sliceStats(.lite_native_page_cache).used_bytes);
    try std.testing.expectEqual(cache.link_bytes, manager.sliceStats(.lite_native_link_cache).used_bytes);
}
