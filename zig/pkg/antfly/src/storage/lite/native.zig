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
//! This module owns the v1-native `.aflite` on-disk header and checkpoint-slot
//! layout plus the first native page stores used by the Lite backend.

const std = @import("std");
const antfly_platform = @import("antfly_platform");
const platform_sync = antfly_platform.sync;
const fs_paths = @import("../../common/fs_paths.zig");
const threaded_io_limits = @import("../../common/threaded_io_limits.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const lsm_backend = @import("../lsm_backend.zig");
const backend_types = @import("../backend_types.zig");
const maintenance = @import("../maintenance.zig");

const Allocator = std.mem.Allocator;

const vacuum_index_batch_keys: usize = 4096;
const vacuum_index_flush_bytes: u64 = 8 * 1024 * 1024;

pub const magic = "AFLITE\x03N";
pub const format_version: u32 = 2;
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
};

const catalog_key_len_mask: u32 = 0x00ff_ffff;
const catalog_delete_flag: u32 = 1 << 31;
const catalog_external_value_flag: u32 = 1 << 30;
const document_delete_flag: u8 = 1 << 0;
const document_external_value_flag: u8 = 1 << 1;
const document_namespace_link_flag: u8 = 1 << 2;
const namespace_directory_key = "\x00antfly.document_namespaces.v1";
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

const DocumentIndexNode = struct {
    kind: DocumentIndexNodeKind,
    keys: [][]u8,
    pointers: []u64,

    fn deinit(self: *DocumentIndexNode, allocator: Allocator) void {
        for (self.keys) |key| allocator.free(key);
        allocator.free(self.keys);
        allocator.free(self.pointers);
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
/// instead of repeating a root seek for every key. Memory is bounded by tree
/// height times page size.
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

const DocumentIndexSplit = struct {
    separator: []u8,
    right_page_id: u64,
};

const DocumentIndexInsertResult = struct {
    page_id: u64,
    split: ?DocumentIndexSplit = null,

    fn deinit(self: *DocumentIndexInsertResult, allocator: Allocator) void {
        if (self.split) |split| allocator.free(split.separator);
        self.* = undefined;
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
    data_lock_file: ?std.Io.File = null,

    fn deinit(self: *PageAllocator) void {
        if (self.data_lock_file) |lock_file| {
            lock_file.close(self.file.runtimeIo());
        }
        self.file.allocator.free(self.free_pages);
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
};

pub const Header = struct {
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
    const link_entry_overhead: usize = 64;

    mutex: std.atomic.Mutex = .unlocked,
    pages: std.AutoHashMapUnmanaged(u64, []u8) = .empty,
    links: std.AutoHashMapUnmanaged(u64, PageLinkInfo) = .empty,
    total_bytes: usize = 0,
    link_bytes: usize = 0,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    resource_page_accounted_bytes: u64 = 0,
    resource_link_accounted_bytes: u64 = 0,
    /// Page-bytes cap. Eviction is wholesale: overflow clears the page bytes
    /// and starts over, which keeps the bookkeeping trivially correct and is
    /// cheap to rebuild at Lite's target file sizes. Link entries are
    /// budgeted separately — they are ~50x smaller per page, and reachability
    /// walks depend on them staying resident even when the page-bytes working
    /// set exceeds its cap.
    limit_bytes: usize = default_limit_bytes,
    link_limit_bytes: usize = default_link_limit_bytes,

    fn getCopy(self: *PageCache, allocator: Allocator, page_id: u64) !?[]u8 {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const cached = self.pages.get(page_id) orelse return null;
        return try allocator.dupe(u8, cached);
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
        if (self.clearPagesForHardPressureLocked(allocator)) return;
        if (self.pages.getEntry(page_id)) |entry| {
            self.total_bytes -= entry.value_ptr.len;
            allocator.free(entry.value_ptr.*);
            entry.value_ptr.* = allocator.dupe(u8, page) catch {
                std.debug.assert(self.pages.remove(page_id));
                self.refreshPageResourceUsageLocked();
                return;
            };
            self.total_bytes += page.len;
            self.refreshPageResourceUsageLocked();
            _ = self.clearPagesForHardPressureLocked(allocator);
            return;
        }
        if (self.total_bytes + page.len > self.limit_bytes) {
            self.clearPagesLocked(allocator);
            self.refreshPageResourceUsageLocked();
        }
        const owned = allocator.dupe(u8, page) catch return;
        self.pages.put(allocator, page_id, owned) catch {
            allocator.free(owned);
            return;
        };
        self.total_bytes += page.len;
        self.refreshPageResourceUsageLocked();
        _ = self.clearPagesForHardPressureLocked(allocator);
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
        if (self.pages.fetchRemove(page_id)) |entry| {
            self.total_bytes -= entry.value.len;
            allocator.free(entry.value);
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
        var it = self.pages.valueIterator();
        while (it.next()) |page| allocator.free(page.*);
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
        self.clearPagesLocked(allocator);
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
    read_only: bool = false,
    no_sync: bool = false,
    page_cache_enabled: std.atomic.Value(bool) = .init(true),
    page_cache: PageCache = .{},
    namespace_directory_cache: NamespaceDirectory = .empty,
    namespace_directory_cache_root: u64 = std.math.maxInt(u64),
    namespace_directory_delta_depth: u16 = 0,
    /// While non-zero, page reads bypass the page cache and hit disk.
    /// Integrity checks hold this so they verify on-disk state rather than
    /// cached copies.
    page_cache_bypass: std.atomic.Value(u32) = .init(0),
    test_fail_vacuum_after_adoption: bool = false,

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
        return try createWithMode(allocator, path, false, false, null);
    }

    pub fn createNew(allocator: Allocator, path: []const u8) !NativeFile {
        return try createWithMode(allocator, path, true, false, null);
    }

    pub fn createWithOptions(allocator: Allocator, path: []const u8, opts: CreateOptions) !NativeFile {
        return try createWithMode(allocator, path, opts.exclusive, opts.no_sync, opts.resource_manager);
    }

    /// Creates a Lite file using a caller-owned std.Io runtime. The runtime
    /// must outlive the returned file.
    pub fn createWithIo(allocator: Allocator, io: std.Io, path: []const u8, opts: CreateOptions) !NativeFile {
        return try createWithRuntime(allocator, path, opts.exclusive, opts.no_sync, opts.resource_manager, undefined, io);
    }

    fn createWithMode(
        allocator: Allocator,
        path: []const u8,
        exclusive: bool,
        no_sync: bool,
        resource_manager: ?*resource_manager_mod.ResourceManager,
    ) !NativeFile {
        var io_impl = threaded_io_limits.initService(allocator);
        errdefer io_impl.deinit();
        return try createWithRuntime(allocator, path, exclusive, no_sync, resource_manager, io_impl, null);
    }

    fn createWithRuntime(
        allocator: Allocator,
        path: []const u8,
        exclusive: bool,
        no_sync: bool,
        resource_manager: ?*resource_manager_mod.ResourceManager,
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
        // Integrity checking must observe on-disk state, not cached pages.
        _ = self.page_cache_bypass.fetchAdd(1, .monotonic);
        defer _ = self.page_cache_bypass.fetchSub(1, .monotonic);

        const checkpoint = self.activeCheckpoint();
        const expected_size = try checkpointPrefixSize(checkpoint, self.header.page_size);
        const file_size = (try self.file.stat(self.runtimeIo())).size;

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
        const document_index_pages = self.collectDocumentIndexPages(checkpoint, &reachable_pages, true, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        self.validateDocumentIndexCoverage(checkpoint, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        if (cancel) |token| try token.check();
        self.validateReachableFreeMap(checkpoint, &reachable_pages) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        const live = self.liveStats() catch |err| {
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
        try self.writePage(page_id, .data, contents);

        var next = previous;
        next.commit_sequence += 1;
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        try self.writeFreeMapPage(next.free_map_root_page, next.page_count, page_allocator.remainingFreePages());
        try self.syncIfRequired();

        try self.publishCheckpoint(next);
        return page_id;
    }

    pub fn readPageAlloc(self: *NativeFile, allocator: Allocator, page_id: u64) ![]u8 {
        return try self.readPageAllocForCheckpoint(allocator, page_id, self.activeCheckpoint());
    }

    fn readPageAllocForCheckpoint(self: *NativeFile, allocator: Allocator, page_id: u64, checkpoint: CheckpointSlot) ![]u8 {
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
        if (use_cache and page.len > 4 and page[4] != @intFromEnum(PageKind.free_map)) {
            self.page_cache.put(self.allocator, page_id, page);
        }
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
        return try self.putCatalogBatchForRoot(.metadata, mutations);
    }

    pub fn putIndexCatalogRecord(self: *NativeFile, key: []const u8, value: []const u8) !void {
        try self.putIndexCatalogBatch(&.{.{ .key = key, .value = value }});
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
        return try self.putCatalogBatchForRoot(.index, mutations);
    }

    fn putCatalogBatchForRoot(self: *NativeFile, root: CatalogRoot, mutations: []const CatalogMutation) !void {
        if (self.read_only) return error.ReadOnly;
        if (mutations.len == 0) return;
        for (mutations) |mutation| try self.validateCatalogMutation(mutation);

        const previous = self.activeCheckpoint();
        var next_root_page = catalogRootPage(previous, root);
        var page_allocator = try self.pageAllocatorFromFreeMap(previous);
        defer page_allocator.deinit();

        for (mutations) |mutation| {
            var external_value_root_page: u64 = 0;
            if (!mutation.is_delete and !self.catalogEntryFitsInline(mutation.key, mutation.value)) {
                external_value_root_page = try self.writeValuePagesAllocated(&page_allocator, mutation.value);
            }

            const page_id = try page_allocator.allocate();
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(self.allocator);
            try encodeCatalogEntry(self.allocator, &payload, .{
                .previous_page = next_root_page,
                .key = mutation.key,
                .value = mutation.value,
                .is_delete = mutation.is_delete,
                .external_value_root_page = external_value_root_page,
            });
            try self.writePage(page_id, .catalog, payload.items);
            next_root_page = page_id;
        }

        var next = previous;
        next.commit_sequence += 1;
        setCatalogRootPage(&next, root, next_root_page);
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        try self.writeFreeMapPage(next.free_map_root_page, next.page_count, page_allocator.remainingFreePages());
        try self.syncIfRequired();

        try self.publishCheckpoint(next);
    }

    fn appendCatalogRecordForRoot(self: *NativeFile, root: CatalogRoot, key: []const u8, suffix: []const u8) !void {
        if (self.read_only) return error.ReadOnly;

        const previous = self.activeCheckpoint();
        var found_entry: ?CatalogEntry = null;
        var found_payload: ?[]u8 = null;

        var page_id = catalogRootPage(previous, root);
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAlloc(self.allocator, page_id, .catalog);
            const entry = decodeCatalogEntry(payload) catch |err| {
                self.allocator.free(payload);
                return err;
            };
            if (std.mem.eql(u8, entry.key, key)) {
                if (entry.is_delete) {
                    self.allocator.free(payload);
                    return try self.putCatalogBatchForRoot(root, &.{.{ .key = key, .value = suffix }});
                }
                found_entry = entry;
                found_payload = payload;
                break;
            }
            page_id = entry.previous_page;
            self.allocator.free(payload);
        }

        const entry = found_entry orelse return try self.putCatalogBatchForRoot(root, &.{.{ .key = key, .value = suffix }});
        defer if (found_payload) |payload| self.allocator.free(payload);

        const old_len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
        const total_len = try std.math.add(usize, old_len, suffix.len);

        var next_root_page = catalogRootPage(previous, root);
        var page_allocator = try self.pageAllocatorFromFreeMap(previous);
        defer page_allocator.deinit();

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
            external_value_root_page = try self.writeAppendedCatalogValuePagesAllocated(&page_allocator, entry, suffix, total_len);
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
        try self.writePage(catalog_page_id, .catalog, payload.items);
        next_root_page = catalog_page_id;

        var next = previous;
        next.commit_sequence += 1;
        setCatalogRootPage(&next, root, next_root_page);
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        try self.writeFreeMapPage(next.free_map_root_page, next.page_count, page_allocator.remainingFreePages());
        try self.syncIfRequired();

        try self.publishCheckpoint(next);
    }

    fn renameCatalogRecordForRoot(self: *NativeFile, root: CatalogRoot, old_key: []const u8, new_key: []const u8) !void {
        if (self.read_only) return error.ReadOnly;
        if (std.mem.eql(u8, old_key, new_key)) return;

        const previous = self.activeCheckpoint();
        var found_entry: ?CatalogEntry = null;
        var found_payload: ?[]u8 = null;

        var page_id = catalogRootPage(previous, root);
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAlloc(self.allocator, page_id, .catalog);
            const entry = decodeCatalogEntry(payload) catch |err| {
                self.allocator.free(payload);
                return err;
            };
            if (std.mem.eql(u8, entry.key, old_key)) {
                if (entry.is_delete) {
                    self.allocator.free(payload);
                    return;
                }
                found_entry = entry;
                found_payload = payload;
                break;
            }
            page_id = entry.previous_page;
            self.allocator.free(payload);
        }

        const entry = found_entry orelse return;
        defer if (found_payload) |payload| self.allocator.free(payload);

        var next_root_page = catalogRootPage(previous, root);
        var page_allocator = try self.pageAllocatorFromFreeMap(previous);
        defer page_allocator.deinit();

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
            try self.writePage(new_page_id, .catalog, payload.items);
            next_root_page = new_page_id;
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
            try self.writePage(tombstone_page_id, .catalog, payload.items);
            next_root_page = tombstone_page_id;
        }

        var next = previous;
        next.commit_sequence += 1;
        setCatalogRootPage(&next, root, next_root_page);
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        try self.writeFreeMapPage(next.free_map_root_page, next.page_count, page_allocator.remainingFreePages());
        try self.syncIfRequired();

        try self.publishCheckpoint(next);
    }

    pub fn getCatalogRecordAlloc(self: *NativeFile, allocator: Allocator, key: []const u8) !?[]u8 {
        return try self.getCatalogRecordFromRootAlloc(allocator, .metadata, key);
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
        var page_id = catalogRootPage(checkpoint, root);
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(allocator, page_id, .catalog, checkpoint);
            defer allocator.free(payload);
            const entry = try decodeCatalogEntry(payload);
            if (std.mem.eql(u8, entry.key, key)) {
                if (entry.is_delete) return null;
                return try self.catalogEntryValueAlloc(allocator, entry);
            }
            page_id = entry.previous_page;
        }
        return null;
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
        var page_id = catalogRootPage(checkpoint, root);
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, page_id, .catalog, checkpoint);
            defer self.allocator.free(payload);
            const entry = try decodeCatalogEntry(payload);
            if (std.mem.eql(u8, entry.key, key)) {
                if (entry.is_delete) return null;
                return if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
            }
            page_id = entry.previous_page;
        }
        return null;
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
        var page_id = catalogRootPage(checkpoint, root);
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, page_id, .catalog, checkpoint);
            defer self.allocator.free(payload);
            const entry = try decodeCatalogEntry(payload);
            if (std.mem.eql(u8, entry.key, key)) {
                if (entry.is_delete) return null;
                return try self.catalogEntryRangeAlloc(allocator, entry, offset, len);
            }
            page_id = entry.previous_page;
        }
        return null;
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

        var page_id = catalogRootPage(self.activeCheckpoint(), root);
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
        var map = std.StringHashMapUnmanaged(bool).empty;
        defer {
            var it = map.iterator();
            while (it.next()) |entry| allocator.free(entry.key_ptr.*);
            map.deinit(allocator);
        }

        var page_id = catalogRootPage(self.activeCheckpoint(), root);
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAlloc(allocator, page_id, .catalog);
            defer allocator.free(payload);
            const entry = try decodeCatalogEntry(payload);

            if (!map.contains(entry.key)) {
                const owned_key = try allocator.dupe(u8, entry.key);
                errdefer allocator.free(owned_key);
                try map.put(allocator, owned_key, !entry.is_delete);
            }
            page_id = entry.previous_page;
        }

        var keys = std.ArrayListUnmanaged(OwnedCatalogKey).empty;
        errdefer {
            for (keys.items) |record| allocator.free(record.key);
            keys.deinit(allocator);
        }
        var it = map.iterator();
        while (it.next()) |entry| {
            if (!entry.value_ptr.*) continue;
            const key = try allocator.dupe(u8, entry.key_ptr.*);
            errdefer allocator.free(key);
            try keys.append(allocator, .{ .key = key });
        }

        std.mem.sort(OwnedCatalogKey, keys.items, {}, struct {
            fn lessThan(_: void, lhs: OwnedCatalogKey, rhs: OwnedCatalogKey) bool {
                return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            }
        }.lessThan);

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

        var page_id = catalogRootPage(self.activeCheckpoint(), root);
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

    const VacuumRecordSource = union(enum) {
        catalog: CatalogRoot,
        documents,
    };

    const VacuumLiveIndex = struct {
        alloc: Allocator,
        io: std.Io,
        path: []u8,
        backend: *lsm_backend.Backend,
        store: @import("../backend_erased.zig").Store,

        fn deinit(self: *VacuumLiveIndex) void {
            self.store.deinit();
            self.backend.close();
            self.alloc.destroy(self.backend);
            std.Io.Dir.cwd().deleteTree(self.io, self.path) catch {};
            self.alloc.free(self.path);
            self.* = undefined;
        }
    };

    fn buildVacuumLiveIndex(self: *NativeFile, source: VacuumRecordSource, suffix: []const u8, cancel: ?*const maintenance.CancelToken) !VacuumLiveIndex {
        const path = try std.fmt.allocPrint(self.allocator, "{s}.tmp-aflite-vacuum-{s}-index", .{ self.path, suffix });
        errdefer self.allocator.free(path);
        const io = self.runtimeIo();
        std.Io.Dir.cwd().deleteTree(io, path) catch {};
        errdefer std.Io.Dir.cwd().deleteTree(io, path) catch {};

        const backend = try self.allocator.create(lsm_backend.Backend);
        errdefer self.allocator.destroy(backend);
        backend.* = try lsm_backend.Backend.open(self.allocator, path, .{
            .flush_threshold = vacuum_index_batch_keys,
            .flush_threshold_bytes = vacuum_index_flush_bytes,
            .compact_threshold_runs = 4,
            .wal_enabled = false,
            .foreground_soft_compaction = true,
        });
        errdefer backend.close();
        var store = try backend.runtimeStore(self.allocator, backend_types.Namespace{});
        errdefer store.deinit();

        var txn = try store.beginWrite();
        var txn_open = true;
        errdefer if (txn_open) txn.abort();
        var pending: usize = 0;
        var page_id = switch (source) {
            .catalog => |root| catalogRootPage(self.activeCheckpoint(), root),
            .documents => self.activeCheckpoint().document_root_page,
        };
        while (page_id != 0) {
            if (cancel) |token| try token.check();
            const payload = switch (source) {
                .catalog => try self.readPagePayloadByKindAlloc(self.allocator, page_id, .catalog),
                .documents => try self.readPagePayloadByKindAlloc(self.allocator, page_id, .document),
            };
            defer self.allocator.free(payload);
            const record = switch (source) {
                .catalog => blk: {
                    const entry = try decodeCatalogEntry(payload);
                    break :blk .{ entry.key, entry.is_delete, entry.previous_page };
                },
                .documents => blk: {
                    const entry = try decodeDocumentEntry(payload);
                    break :blk .{ entry.key, entry.is_delete, entry.previous_page };
                },
            };
            const key = record[0];
            const is_delete = record[1];
            const previous_page = record[2];
            _ = txn.get(key) catch |err| switch (err) {
                error.NotFound => blk: {
                    var encoded: [9]u8 = undefined;
                    encoded[0] = @intFromBool(is_delete);
                    std.mem.writeInt(u64, encoded[1..9], page_id, .little);
                    try txn.put(key, &encoded);
                    pending += 1;
                    break :blk &encoded;
                },
                else => return err,
            };
            if (pending >= vacuum_index_batch_keys) {
                try txn.commit();
                txn_open = false;
                txn = try store.beginWrite();
                txn_open = true;
                pending = 0;
            }
            page_id = previous_page;
        }
        try txn.commit();
        txn_open = false;
        return .{ .alloc = self.allocator, .io = io, .path = path, .backend = backend, .store = store };
    }

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
            if (walked > checkpoint.page_count) return error.InvalidNativePageChain;
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
            if (page_id >= checkpoint.page_count) return error.InvalidPageId;
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, page_id, .document, checkpoint);
            defer self.allocator.free(payload);
            const entry = try decodeDocumentEntry(payload);
            const expected = expected_pages.getPtr(documentNamespace(entry.key)) orelse return error.InvalidNamespaceDirectory;
            if (expected.* != page_id) return error.InvalidNamespaceDirectory;
            expected.* = entry.previous_namespace_page;
            page_id = entry.previous_page;
            walked += 1;
            if (walked > checkpoint.page_count) return error.InvalidNativePageChain;
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
        var changed_heads = NamespaceDirectory.empty;
        defer changed_heads.deinit(self.allocator);
        try changed_heads.ensureTotalCapacity(
            self.allocator,
            std.math.cast(u32, mutations.len) orelse return error.RecordTooLarge,
        );

        for (mutations, 0..) |mutation, ordinal| {
            var external_value_root_page: u64 = 0;
            if (!mutation.is_delete and !self.documentEntryFitsInline(mutation.key, mutation.value)) {
                external_value_root_page = try self.writeValuePagesAllocated(&page_allocator, mutation.value);
            }

            const page_id = try page_allocator.allocate();
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
            });
            try self.writePage(page_id, .document, payload.items);
            next_root_page = page_id;
            if (bulk_build_initial_index) {
                initial_index_entries.appendAssumeCapacity(.{
                    .key = mutation.key,
                    .document_page_id = page_id,
                    .ordinal = ordinal,
                });
            } else {
                var working_checkpoint = previous;
                working_checkpoint.page_count = page_allocator.next_page_id;
                next_index_root_page = try self.upsertDocumentIndex(
                    &page_allocator,
                    next_index_root_page,
                    mutation.key,
                    page_id,
                    working_checkpoint,
                );
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
        try self.writePage(directory_page, .catalog, directory_payload.items);

        var next = previous;
        next.commit_sequence += 1;
        next.document_root_page = next_root_page;
        next.namespace_directory_root_page = directory_page;
        next.document_index_root_page = next_index_root_page;
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

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
        return try decodeDocumentIndexNode(self.allocator, payload);
    }

    fn writeDocumentIndexNode(self: *NativeFile, page_allocator: *PageAllocator, node: DocumentIndexNode) !u64 {
        const encoded = try encodeDocumentIndexNode(self.allocator, node);
        defer self.allocator.free(encoded);
        if (encoded.len > self.maxPagePayloadBytes()) return error.DocumentIndexNodeTooLarge;
        const page_id = try page_allocator.allocate();
        try self.writePage(page_id, .document_index, encoded);
        return page_id;
    }

    fn cloneLeafWithUpsert(self: *NativeFile, node: DocumentIndexNode, key: []const u8, document_page_id: u64) !DocumentIndexNode {
        const index = lowerBoundIndexKeys(node.keys, key);
        const replace = index < node.keys.len and std.mem.eql(u8, node.keys[index], key);
        const count = node.keys.len + @intFromBool(!replace);
        const keys = try self.allocator.alloc([]u8, count);
        var initialized: usize = 0;
        errdefer {
            for (keys[0..initialized]) |owned| self.allocator.free(owned);
            self.allocator.free(keys);
        }
        const pointers = try self.allocator.alloc(u64, count);
        errdefer self.allocator.free(pointers);
        var source: usize = 0;
        for (0..count) |out_index| {
            if (out_index == index) {
                keys[out_index] = try self.allocator.dupe(u8, key);
                pointers[out_index] = document_page_id;
                initialized += 1;
                if (replace) source += 1;
            } else {
                keys[out_index] = try self.allocator.dupe(u8, node.keys[source]);
                pointers[out_index] = node.pointers[source];
                initialized += 1;
                source += 1;
            }
        }
        return .{ .kind = .leaf, .keys = keys, .pointers = pointers };
    }

    fn cloneInternalWithChildResult(
        self: *NativeFile,
        node: DocumentIndexNode,
        child_index: usize,
        child_result: DocumentIndexInsertResult,
    ) !DocumentIndexNode {
        const extra: usize = if (child_result.split != null) 1 else 0;
        const keys = try self.allocator.alloc([]u8, node.keys.len + extra);
        var initialized: usize = 0;
        errdefer {
            for (keys[0..initialized]) |owned| self.allocator.free(owned);
            self.allocator.free(keys);
        }
        const pointers = try self.allocator.alloc(u64, node.pointers.len + extra);
        errdefer self.allocator.free(pointers);

        for (node.pointers, 0..) |pointer, i| pointers[i + @intFromBool(extra == 1 and i > child_index)] = pointer;
        pointers[child_index] = child_result.page_id;
        if (child_result.split) |split| pointers[child_index + 1] = split.right_page_id;

        var source: usize = 0;
        for (keys, 0..) |*out_key, i| {
            if (extra == 1 and i == child_index) {
                out_key.* = try self.allocator.dupe(u8, child_result.split.?.separator);
            } else {
                out_key.* = try self.allocator.dupe(u8, node.keys[source]);
                source += 1;
            }
            initialized += 1;
        }
        return .{ .kind = .internal, .keys = keys, .pointers = pointers };
    }

    fn documentIndexNodeFits(self: *NativeFile, node: DocumentIndexNode) bool {
        const size = encodedDocumentIndexNodeSize(node) catch return false;
        return size <= self.maxPagePayloadBytes();
    }

    fn insertDocumentIndexNode(
        self: *NativeFile,
        page_allocator: *PageAllocator,
        page_id: u64,
        key: []const u8,
        document_page_id: u64,
        checkpoint: CheckpointSlot,
    ) !DocumentIndexInsertResult {
        var node = try self.readDocumentIndexNode(page_id, checkpoint);
        defer node.deinit(self.allocator);
        if (node.kind == .leaf) {
            var updated = try self.cloneLeafWithUpsert(node, key, document_page_id);
            defer updated.deinit(self.allocator);
            if (self.documentIndexNodeFits(updated)) return .{ .page_id = try self.writeDocumentIndexNode(page_allocator, updated) };

            var best_split: ?usize = null;
            var best_imbalance: usize = std.math.maxInt(usize);
            var split_index: usize = 1;
            while (split_index < updated.keys.len) : (split_index += 1) {
                const left = DocumentIndexNode{ .kind = .leaf, .keys = updated.keys[0..split_index], .pointers = updated.pointers[0..split_index] };
                const right = DocumentIndexNode{ .kind = .leaf, .keys = updated.keys[split_index..], .pointers = updated.pointers[split_index..] };
                if (!self.documentIndexNodeFits(left) or !self.documentIndexNodeFits(right)) continue;
                const left_size = try encodedDocumentIndexNodeSize(left);
                const right_size = try encodedDocumentIndexNodeSize(right);
                const imbalance = if (left_size > right_size) left_size - right_size else right_size - left_size;
                if (imbalance < best_imbalance) {
                    best_imbalance = imbalance;
                    best_split = split_index;
                }
            }
            const balanced_split = best_split orelse return error.DocumentIndexNodeTooLarge;
            const left = DocumentIndexNode{ .kind = .leaf, .keys = updated.keys[0..balanced_split], .pointers = updated.pointers[0..balanced_split] };
            const right = DocumentIndexNode{ .kind = .leaf, .keys = updated.keys[balanced_split..], .pointers = updated.pointers[balanced_split..] };
            const left_page = try self.writeDocumentIndexNode(page_allocator, left);
            const right_page = try self.writeDocumentIndexNode(page_allocator, right);
            return .{
                .page_id = left_page,
                .split = .{
                    .separator = try self.allocator.dupe(u8, right.keys[0]),
                    .right_page_id = right_page,
                },
            };
        }

        const child_index = upperBoundIndexKeys(node.keys, key);
        var child_result = try self.insertDocumentIndexNode(page_allocator, node.pointers[child_index], key, document_page_id, checkpoint);
        defer child_result.deinit(self.allocator);
        var updated = try self.cloneInternalWithChildResult(node, child_index, child_result);
        defer updated.deinit(self.allocator);
        if (self.documentIndexNodeFits(updated)) return .{ .page_id = try self.writeDocumentIndexNode(page_allocator, updated) };

        var best_promote: ?usize = null;
        var best_imbalance: usize = std.math.maxInt(usize);
        for (0..updated.keys.len) |promote_index| {
            const left = DocumentIndexNode{
                .kind = .internal,
                .keys = updated.keys[0..promote_index],
                .pointers = updated.pointers[0 .. promote_index + 1],
            };
            const right = DocumentIndexNode{
                .kind = .internal,
                .keys = updated.keys[promote_index + 1 ..],
                .pointers = updated.pointers[promote_index + 1 ..],
            };
            if (!self.documentIndexNodeFits(left) or !self.documentIndexNodeFits(right)) continue;
            const left_size = try encodedDocumentIndexNodeSize(left);
            const right_size = try encodedDocumentIndexNodeSize(right);
            const imbalance = if (left_size > right_size) left_size - right_size else right_size - left_size;
            if (imbalance < best_imbalance) {
                best_imbalance = imbalance;
                best_promote = promote_index;
            }
        }
        const promote_index = best_promote orelse return error.DocumentIndexNodeTooLarge;
        const left = DocumentIndexNode{ .kind = .internal, .keys = updated.keys[0..promote_index], .pointers = updated.pointers[0 .. promote_index + 1] };
        const right = DocumentIndexNode{ .kind = .internal, .keys = updated.keys[promote_index + 1 ..], .pointers = updated.pointers[promote_index + 1 ..] };
        const left_page = try self.writeDocumentIndexNode(page_allocator, left);
        const right_page = try self.writeDocumentIndexNode(page_allocator, right);
        return .{
            .page_id = left_page,
            .split = .{
                .separator = try self.allocator.dupe(u8, updated.keys[promote_index]),
                .right_page_id = right_page,
            },
        };
    }

    fn upsertDocumentIndex(
        self: *NativeFile,
        page_allocator: *PageAllocator,
        root_page_id: u64,
        key: []const u8,
        document_page_id: u64,
        checkpoint: CheckpointSlot,
    ) !u64 {
        if (root_page_id == 0) {
            const keys = [_][]u8{@constCast(key)};
            const pointers = [_]u64{document_page_id};
            return try self.writeDocumentIndexNode(page_allocator, .{ .kind = .leaf, .keys = @constCast(&keys), .pointers = @constCast(&pointers) });
        }
        var result = try self.insertDocumentIndexNode(page_allocator, root_page_id, key, document_page_id, checkpoint);
        defer result.deinit(self.allocator);
        const split = result.split orelse return result.page_id;
        const keys = [_][]u8{split.separator};
        const pointers = [_]u64{ result.page_id, split.right_page_id };
        return try self.writeDocumentIndexNode(page_allocator, .{ .kind = .internal, .keys = @constCast(&keys), .pointers = @constCast(&pointers) });
    }

    fn lookupDocumentIndexPage(self: *NativeFile, checkpoint: CheckpointSlot, key: []const u8) !?u64 {
        var page_id = checkpoint.document_index_root_page;
        while (page_id != 0) {
            var node = try self.readDocumentIndexNode(page_id, checkpoint);
            defer node.deinit(self.allocator);
            if (node.kind == .leaf) {
                const index = lowerBoundIndexKeys(node.keys, key);
                if (index == node.keys.len or !std.mem.eql(u8, node.keys[index], key)) return null;
                return node.pointers[index];
            }
            page_id = node.pointers[upperBoundIndexKeys(node.keys, key)];
        }
        return null;
    }

    pub fn documentValueAtIndexEntryAlloc(self: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot, indexed: DocumentIndexEntry) !?[]u8 {
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(allocator, indexed.document_page_id, .document, checkpoint);
        defer allocator.free(payload);
        const entry = try decodeDocumentEntry(payload);
        if (!std.mem.eql(u8, entry.key, indexed.key)) return error.InvalidDocumentIndex;
        if (entry.is_delete) return null;
        return try self.documentEntryValueAlloc(allocator, entry);
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
        return try self.documentEntryValueAlloc(allocator, entry);
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

    fn copyVacuumCatalogRecords(
        self: *NativeFile,
        compact_file: std.Io.File,
        root: CatalogRoot,
        suffix: []const u8,
        next_page_id: *u64,
        destination_root_page: *u64,
        live_bytes: *u64,
        cancel: ?*const maintenance.CancelToken,
    ) !usize {
        var index = try self.buildVacuumLiveIndex(.{ .catalog = root }, suffix, cancel);
        defer index.deinit();
        var read = try index.store.beginRead();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();

        const io = self.runtimeIo();
        const page_size: usize = @intCast(self.header.page_size);
        var count: usize = 0;
        var maybe_entry = try cursor.first();
        while (maybe_entry) |record| : (maybe_entry = try cursor.next()) {
            if (cancel) |token| try token.check();
            if (record.value.len != 9) return error.InvalidVacuumIndex;
            if (record.value[0] != 0) continue;
            const source_page_id = std.mem.readInt(u64, record.value[1..9], .little);
            const source_payload = try self.readPagePayloadByKindAlloc(self.allocator, source_page_id, .catalog);
            defer self.allocator.free(source_payload);
            const source_entry = try decodeCatalogEntry(source_payload);
            if (!std.mem.eql(u8, source_entry.key, record.key) or source_entry.is_delete) return error.InvalidNativePageChain;
            const value = try self.catalogEntryValueAlloc(self.allocator, source_entry);
            defer self.allocator.free(value);
            const external_value_root_page = if (value.len == 0 or self.catalogEntryFitsInline(record.key, value))
                0
            else
                try appendValuePagesToFile(self.allocator, compact_file, io, page_size, self.maxValuePagePayloadBytes(), next_page_id, value);
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(self.allocator);
            try encodeCatalogEntry(self.allocator, &payload, .{
                .previous_page = destination_root_page.*,
                .key = record.key,
                .value = value,
                .external_value_root_page = external_value_root_page,
            });
            destination_root_page.* = try appendPageToFile(self.allocator, compact_file, io, page_size, next_page_id, .catalog, payload.items);
            live_bytes.* +|= record.key.len + value.len;
            count += 1;
        }
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
        var index = try self.buildVacuumLiveIndex(.documents, "documents", cancel);
        defer index.deinit();
        var read = try index.store.beginRead();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();

        const io = self.runtimeIo();
        const page_size: usize = @intCast(self.header.page_size);
        var document_index = DocumentIndexBulkBuilder{
            .owner = self,
            .file = compact_file,
            .next_page_id = next_page_id,
        };
        defer document_index.deinit();
        var count: usize = 0;
        var maybe_entry = try cursor.first();
        while (maybe_entry) |record| : (maybe_entry = try cursor.next()) {
            if (cancel) |token| try token.check();
            if (record.value.len != 9) return error.InvalidVacuumIndex;
            if (record.value[0] != 0) continue;
            const source_page_id = std.mem.readInt(u64, record.value[1..9], .little);
            const source_payload = try self.readPagePayloadByKindAlloc(self.allocator, source_page_id, .document);
            defer self.allocator.free(source_payload);
            const source_entry = try decodeDocumentEntry(source_payload);
            if (!std.mem.eql(u8, source_entry.key, record.key) or source_entry.is_delete) return error.InvalidNativePageChain;
            const value = try self.documentEntryValueAlloc(self.allocator, source_entry);
            defer self.allocator.free(value);
            const external_value_root_page = if (self.documentEntryFitsInline(record.key, value))
                0
            else
                try appendValuePagesToFile(self.allocator, compact_file, io, page_size, self.maxValuePagePayloadBytes(), next_page_id, value);
            const namespace = documentNamespace(record.key);
            const previous_namespace_page = namespace_directory.get(namespace) orelse 0;
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(self.allocator);
            try encodeDocumentEntry(self.allocator, &payload, .{
                .previous_page = document_root_page.*,
                .previous_namespace_page = previous_namespace_page,
                .key = record.key,
                .value = value,
                .external_value_root_page = external_value_root_page,
            });
            document_root_page.* = try appendPageToFile(self.allocator, compact_file, io, page_size, next_page_id, .document, payload.items);
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
        return count;
    }

    pub fn vacuum(self: *NativeFile) !VacuumReport {
        return try self.vacuumWithCancel(null);
    }

    pub fn vacuumWithCancel(self: *NativeFile, cancel: ?*const maintenance.CancelToken) !VacuumReport {
        if (self.read_only) return error.ReadOnly;
        if (cancel) |token| try token.check();

        const io = self.runtimeIo();
        var data_lock = try acquireDataRewriteLock(io, self.path);
        defer data_lock.file.close(io);

        const before_size = (try self.file.stat(io)).size;
        const previous = self.activeCheckpoint();

        const page_size: usize = @intCast(self.header.page_size);
        const tmp_path = try std.fmt.allocPrint(self.allocator, "{s}.tmp-aflite-vacuum", .{self.path});
        defer self.allocator.free(tmp_path);
        errdefer deleteFilePath(io, tmp_path) catch {};
        var compact_file = try createSnapshotFile(io, tmp_path);
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

        live_record_count += try self.copyVacuumCatalogRecords(compact_file, .metadata, "metadata", &next_page_id, &catalog_root_page, &live_bytes, cancel);
        live_record_count += try self.copyVacuumCatalogRecords(compact_file, .index, "indexes", &next_page_id, &index_catalog_root_page, &live_bytes, cancel);
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
        try self.replaceOpenFileWithVacuumFile(tmp_path, compact_file, compact_header, &compact_file_open);

        return .{
            .before_size = before_size,
            .after_size = after_size,
            .reclaimed_bytes = if (before_size > after_size) before_size - after_size else 0,
            .live_file_count = @intCast(live_record_count),
            .live_bytes = live_bytes,
        };
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
        if (entry.external_value_root_page != 0) {
            return try self.readValuePagesAlloc(allocator, entry.external_value_root_page, entry.external_value_len);
        }
        return try allocator.dupe(u8, entry.value);
    }

    fn catalogEntryRangeAlloc(self: *NativeFile, allocator: Allocator, entry: CatalogEntry, offset: u64, len: usize) ![]u8 {
        const value_len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
        if (offset > std.math.maxInt(usize)) return error.EndOfStream;
        const start: usize = @intCast(offset);
        if (start > value_len or value_len - start < len) return error.EndOfStream;
        if (entry.external_value_root_page != 0) {
            return try self.readValuePagesRangeAlloc(allocator, entry.external_value_root_page, value_len, start, len);
        }
        return try allocator.dupe(u8, entry.value[start..][0..len]);
    }

    fn validateDocumentMutation(self: *const NativeFile, mutation: DocumentMutation) !void {
        if (mutation.key.len > std.math.maxInt(u32) or mutation.value.len > std.math.maxInt(u32)) return error.RecordTooLarge;
        const fixed_len = 28 + mutation.key.len;
        if (fixed_len > self.maxPagePayloadBytes()) return error.PageTooLarge;
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
        if (page_id == 0 or page_id >= page_count) return error.InvalidPageId;
        const entry = try reachable_pages.getOrPut(self.allocator, page_id);
        if (entry.found_existing) return error.InvalidNativePageChain;
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
        const use_link_cache = self.page_cache_enabled.load(.monotonic) and self.page_cache_bypass.load(.monotonic) == 0;
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
                    .data, .value, .free_map, .document_index => return error.UnexpectedNativePageKind,
                }
                page_id = links.link_page;
                count += 1;
                if (count > checkpoint.page_count) return error.InvalidNativePageChain;
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
                .data, .value, .free_map, .document_index => return error.UnexpectedNativePageKind,
            };
            count += 1;
            if (count > checkpoint.page_count) return error.InvalidNativePageChain;
        }
        return count;
    }

    fn writeValuePagesAllocated(self: *NativeFile, page_allocator: *PageAllocator, value: []const u8) !u64 {
        if (value.len == 0) return error.InvalidNativeValueChain;
        const chunk_size = self.maxValuePagePayloadBytes();
        if (chunk_size == 0) return error.InvalidNativePageLength;
        const page_count = std.math.divCeil(usize, value.len, chunk_size) catch unreachable;

        const page_ids = try self.allocator.alloc(u64, page_count);
        defer self.allocator.free(page_ids);
        for (page_ids) |*page_id| page_id.* = try page_allocator.allocate();

        var offset: usize = 0;
        var page_index: usize = 0;
        while (offset < value.len) : (page_index += 1) {
            const len = @min(chunk_size, value.len - offset);
            const page_id = page_ids[page_index];
            const next_page = if (page_index + 1 < page_count) page_ids[page_index + 1] else 0;

            const payload = try self.allocator.alloc(u8, value_page_header_size + len);
            defer self.allocator.free(payload);
            std.mem.writeInt(u64, payload[0..8], next_page, .little);
            @memcpy(payload[value_page_header_size..][0..len], value[offset..][0..len]);

            try self.writePage(page_id, .value, payload);
            offset += len;
        }

        return page_ids[0];
    }

    fn copyValuePagesAllocated(self: *NativeFile, page_allocator: *PageAllocator, root_page_id: u64, value_len: usize) !u64 {
        if (value_len == 0 or root_page_id == 0) return error.InvalidNativeValueChain;
        const page_count = try self.countValuePages(root_page_id, value_len);

        const page_ids = try self.allocator.alloc(u64, page_count);
        defer self.allocator.free(page_ids);
        for (page_ids) |*page_id| page_id.* = try page_allocator.allocate();

        var remaining = value_len;
        var source_page_id = root_page_id;
        var page_index: usize = 0;
        while (source_page_id != 0) : (page_index += 1) {
            if (page_index >= page_ids.len) return error.InvalidNativeValueChain;
            const payload = try self.readPagePayloadByKindAlloc(self.allocator, source_page_id, .value);
            defer self.allocator.free(payload);
            const page = try decodeValuePage(payload);
            if (page.chunk.len == 0) return error.InvalidNativeValueChain;
            if (page.chunk.len > remaining) return error.InvalidNativeValueChain;

            const dest_page_id = page_ids[page_index];
            const next_dest_page_id = if (page_index + 1 < page_ids.len) page_ids[page_index + 1] else 0;
            const copied_payload = try self.allocator.alloc(u8, value_page_header_size + page.chunk.len);
            defer self.allocator.free(copied_payload);
            std.mem.writeInt(u64, copied_payload[0..8], next_dest_page_id, .little);
            @memcpy(copied_payload[value_page_header_size..], page.chunk);
            try self.writePage(dest_page_id, .value, copied_payload);

            remaining -= page.chunk.len;
            source_page_id = page.next_page;
            if (remaining == 0 and source_page_id != 0) return error.InvalidNativeValueChain;
        }

        if (remaining != 0 or page_index != page_ids.len) return error.InvalidNativeValueChain;
        return page_ids[0];
    }

    fn writeAppendedCatalogValuePagesAllocated(
        self: *NativeFile,
        page_allocator: *PageAllocator,
        entry: CatalogEntry,
        suffix: []const u8,
        total_len: usize,
    ) !u64 {
        if (total_len == 0) return error.InvalidNativeValueChain;
        const old_page_count: usize = if (entry.external_value_root_page != 0)
            try self.countValuePages(entry.external_value_root_page, entry.external_value_len)
        else if (entry.value.len > 0)
            1
        else
            0;
        const suffix_page_count: usize = if (suffix.len == 0)
            0
        else
            std.math.divCeil(usize, suffix.len, self.maxValuePagePayloadBytes()) catch unreachable;
        const page_count = try std.math.add(usize, old_page_count, suffix_page_count);
        if (page_count == 0) return error.InvalidNativeValueChain;

        const page_ids = try self.allocator.alloc(u64, page_count);
        defer self.allocator.free(page_ids);
        for (page_ids) |*page_id| page_id.* = try page_allocator.allocate();

        var page_index: usize = 0;
        if (entry.external_value_root_page != 0) {
            var remaining = entry.external_value_len;
            var source_page_id = entry.external_value_root_page;
            while (source_page_id != 0) : (page_index += 1) {
                if (page_index >= old_page_count) return error.InvalidNativeValueChain;
                const payload = try self.readPagePayloadByKindAlloc(self.allocator, source_page_id, .value);
                defer self.allocator.free(payload);
                const page = try decodeValuePage(payload);
                if (page.chunk.len == 0) return error.InvalidNativeValueChain;
                if (page.chunk.len > remaining) return error.InvalidNativeValueChain;
                try self.writeValuePageChunk(page_ids, page_index, page.chunk);
                remaining -= page.chunk.len;
                source_page_id = page.next_page;
                if (remaining == 0 and source_page_id != 0) return error.InvalidNativeValueChain;
            }
            if (remaining != 0 or page_index != old_page_count) return error.InvalidNativeValueChain;
        } else if (entry.value.len > 0) {
            try self.writeValuePageChunk(page_ids, page_index, entry.value);
            page_index += 1;
        }

        var suffix_offset: usize = 0;
        while (suffix_offset < suffix.len) : (page_index += 1) {
            if (page_index >= page_ids.len) return error.InvalidNativeValueChain;
            const len = @min(self.maxValuePagePayloadBytes(), suffix.len - suffix_offset);
            try self.writeValuePageChunk(page_ids, page_index, suffix[suffix_offset..][0..len]);
            suffix_offset += len;
        }

        if (page_index != page_ids.len) return error.InvalidNativeValueChain;
        return page_ids[0];
    }

    fn countValuePages(self: *NativeFile, root_page_id: u64, value_len: usize) !usize {
        if (value_len == 0 or root_page_id == 0) return error.InvalidNativeValueChain;
        var remaining = value_len;
        var page_id = root_page_id;
        var count: usize = 0;
        while (page_id != 0) {
            count += 1;
            if (count > self.activeCheckpoint().page_count) return error.InvalidNativeValueChain;
            const payload = try self.readPagePayloadByKindAlloc(self.allocator, page_id, .value);
            defer self.allocator.free(payload);
            const page = try decodeValuePage(payload);
            if (page.chunk.len == 0) return error.InvalidNativeValueChain;
            if (page.chunk.len > remaining) return error.InvalidNativeValueChain;
            remaining -= page.chunk.len;
            page_id = page.next_page;
            if (remaining == 0 and page_id != 0) return error.InvalidNativeValueChain;
        }
        if (remaining != 0) return error.InvalidNativeValueChain;
        return count;
    }

    fn writeValuePageChunk(self: *NativeFile, page_ids: []const u64, page_index: usize, chunk: []const u8) !void {
        if (chunk.len == 0 or chunk.len > self.maxValuePagePayloadBytes()) return error.InvalidNativeValueChain;
        const next_page_id = if (page_index + 1 < page_ids.len) page_ids[page_index + 1] else 0;
        const payload = try self.allocator.alloc(u8, value_page_header_size + chunk.len);
        defer self.allocator.free(payload);
        std.mem.writeInt(u64, payload[0..8], next_page_id, .little);
        @memcpy(payload[value_page_header_size..], chunk);
        try self.writePage(page_ids[page_index], .value, payload);
    }

    fn pageAllocatorFromFreeMap(self: *NativeFile, checkpoint: CheckpointSlot) !PageAllocator {
        var free_pages = try self.readFreePagesAlloc(checkpoint);
        errdefer self.allocator.free(free_pages);
        try self.validateFreePagesSafeForCheckpointSlots(free_pages);
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
        _ = try self.collectDocumentIndexPages(checkpoint, &checkpoint_pages, false, null);
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
        cancel: ?*const maintenance.CancelToken,
        depth: usize,
    ) !u64 {
        if (depth > 64) return error.InvalidDocumentIndex;
        if (cancel) |token| try token.check();
        try self.markReachablePage(reachable_pages, page_id, checkpoint.page_count);
        var node = try self.readDocumentIndexNode(page_id, checkpoint);
        defer node.deinit(self.allocator);
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
                    if (child == 0 or child >= checkpoint.page_count) return error.InvalidPageId;
                    count += try self.collectDocumentIndexSubtree(
                        checkpoint,
                        child,
                        if (i == 0) lower else node.keys[i - 1],
                        if (i == node.keys.len) upper else node.keys[i],
                        reachable_pages,
                        validate_documents,
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
            if (walked > checkpoint.page_count) return error.InvalidNativePageChain;
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
        var protected_pages = std.AutoHashMapUnmanaged(u64, void){};
        defer protected_pages.deinit(self.allocator);

        try self.collectAllValidCheckpointReachablePages(&protected_pages);
        for (free_pages) |page_id| {
            if (protected_pages.contains(page_id)) return error.InvalidNativeFreeMap;
        }
    }

    fn readValuePagesAlloc(self: *NativeFile, allocator: Allocator, root_page_id: u64, value_len: usize) ![]u8 {
        if (value_len == 0 or root_page_id == 0) return error.InvalidNativeValueChain;

        const value = try allocator.alloc(u8, value_len);
        errdefer allocator.free(value);

        var written: usize = 0;
        var page_id = root_page_id;
        var pages_seen: u64 = 0;
        while (page_id != 0) {
            pages_seen += 1;
            if (pages_seen > self.activeCheckpoint().page_count) return error.InvalidNativeValueChain;

            const payload = try self.readPagePayloadByKindAlloc(allocator, page_id, .value);
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
    ) ![]u8 {
        if (value_len == 0 or root_page_id == 0) return error.InvalidNativeValueChain;

        const out = try allocator.alloc(u8, range_len);
        errdefer allocator.free(out);

        const range_end = range_start + range_len;
        var value_offset: usize = 0;
        var written: usize = 0;
        var page_id = root_page_id;
        var pages_seen: u64 = 0;
        while (page_id != 0) {
            pages_seen += 1;
            if (pages_seen > self.activeCheckpoint().page_count) return error.InvalidNativeValueChain;

            const payload = try self.readPagePayloadByKindAlloc(allocator, page_id, .value);
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

    fn liveStats(self: *NativeFile) !LiveStats {
        const catalog_records = try self.snapshotCatalogRecordsAlloc(self.allocator);
        defer freeSnapshotCatalogRecords(self.allocator, catalog_records);

        const index_catalog_records = try self.snapshotIndexCatalogRecordsAlloc(self.allocator);
        defer freeSnapshotCatalogRecords(self.allocator, index_catalog_records);

        var live_bytes: u64 = 0;
        var compact_pages: u64 = 1;
        var document_count: u64 = 0;

        for (catalog_records) |record| {
            live_bytes +|= record.key.len + record.value.len;
            compact_pages += 1;
            if (record.value.len != 0 and !self.catalogEntryFitsInline(record.key, record.value)) {
                compact_pages += self.valuePageCount(record.value.len);
            }
        }

        for (index_catalog_records) |record| {
            live_bytes +|= record.key.len + record.value.len;
            compact_pages += 1;
            if (record.value.len != 0 and !self.catalogEntryFitsInline(record.key, record.value)) {
                compact_pages += self.valuePageCount(record.value.len);
            }
        }

        const checkpoint = self.activeCheckpoint();
        var index_cursor = DocumentIndexCursor.init(self, checkpoint);
        defer index_cursor.deinit();
        var maybe_document = try index_cursor.first();
        while (maybe_document) |indexed| {
            var owned_indexed = indexed;
            defer owned_indexed.deinit(self.allocator);
            const value = try self.documentValueAtIndexEntryAlloc(self.allocator, checkpoint, owned_indexed);
            if (value) |owned_value| {
                defer self.allocator.free(owned_value);
                live_bytes +|= owned_indexed.key.len + owned_value.len;
                compact_pages += 1;
                document_count += 1;
                if (!self.documentEntryFitsInline(owned_indexed.key, owned_value)) {
                    compact_pages += self.valuePageCount(owned_value.len);
                }
            }
            maybe_document = try index_cursor.next();
        }
        if (document_count > 0) {
            var directory = (try self.loadNamespaceDirectoryAlloc(self.allocator)) orelse return error.InvalidNamespaceDirectory;
            defer deinitNamespaceDirectory(self.allocator, &directory);
            const encoded = try encodeNamespaceDirectoryAlloc(self.allocator, .snapshot, &directory);
            defer self.allocator.free(encoded);
            compact_pages += 1;
            if (!self.catalogEntryFitsInline(namespace_directory_key, encoded)) compact_pages += self.valuePageCount(encoded.len);
        }
        if (self.activeCheckpoint().free_map_root_page != 0) compact_pages += 1;
        var document_index_pages = ReachablePageSet{};
        defer document_index_pages.deinit(self.allocator);
        compact_pages += try self.collectDocumentIndexPages(self.activeCheckpoint(), &document_index_pages, false, null);

        return .{
            .record_count = @as(u64, @intCast(catalog_records.len + index_catalog_records.len)) + document_count,
            .bytes = live_bytes,
            .compact_size = compact_pages * @as(u64, self.header.page_size),
        };
    }

    fn valuePageCount(self: *const NativeFile, value_len: usize) u64 {
        std.debug.assert(value_len > 0);
        return @intCast(std.math.divCeil(usize, value_len, self.maxValuePagePayloadBytes()) catch unreachable);
    }

    fn writePage(self: *NativeFile, page_id: u64, kind: PageKind, contents: []const u8) !void {
        if (contents.len > self.maxPagePayloadBytes()) return error.PageTooLarge;

        const page_size: usize = @intCast(self.header.page_size);
        const page_offset = page_id * @as(u64, self.header.page_size);

        const page = try self.allocator.alloc(u8, page_size);
        defer self.allocator.free(page);
        encodePage(page, kind, contents);

        const page_end = page_offset + self.header.page_size;
        const io = self.runtimeIo();
        const file_size = (try self.file.stat(io)).size;
        if (file_size < page_end) {
            try self.file.setLength(io, page_end);
        }
        try self.file.writePositionalAll(io, page, page_offset);
        if (kind == .free_map) {
            // A reused page id may still be cached under its previous life;
            // free-map pages themselves are not cached (see read path).
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
            .data => self.page_cache.removeLinks(self.allocator, page_id),
            .document_index => self.page_cache.removeLinks(self.allocator, page_id),
            .free_map => unreachable,
        }
    }

    fn publishCheckpoint(self: *NativeFile, checkpoint: CheckpointSlot) !void {
        const next_slot: u8 = if (self.header.active_checkpoint == 0) 1 else 0;

        var encoded_slot: [checkpoint_slot_size]u8 = undefined;
        encodeCheckpointSlot(&encoded_slot, checkpoint);

        const io = self.runtimeIo();
        try self.file.writePositionalAll(io, &encoded_slot, checkpointOffset(next_slot));
        try self.syncIfRequired();
        const active_checkpoint: [1]u8 = .{next_slot};
        try self.file.writePositionalAll(io, &active_checkpoint, active_checkpoint_offset);
        try self.syncIfRequired();

        self.header.checkpoints[next_slot] = checkpoint;
        self.header.active_checkpoint = next_slot;
    }

    fn syncIfRequired(self: *NativeFile) !void {
        if (!self.no_sync) try self.file.sync(self.runtimeIo());
    }

    pub fn sync(self: *NativeFile) !void {
        try self.syncIfRequired();
    }
    /// Publishes a fully synced vacuum file and adopts its already-open handle.
    /// Once rename succeeds there are deliberately no fallible reopen steps:
    /// even if the parent-directory sync reports an error, subsequent requests
    /// continue on the same inode now reachable through `self.path` rather than
    /// writing the unlinked pre-vacuum inode.
    fn replaceOpenFileWithVacuumFile(
        self: *NativeFile,
        tmp_path: []const u8,
        replacement_file: std.Io.File,
        replacement_header: Header,
        caller_owns_replacement: *bool,
    ) !void {
        const io = self.runtimeIo();
        try renameFilePath(io, tmp_path, self.path);

        const previous_file = self.file;
        self.file = replacement_file;
        caller_owns_replacement.* = false;
        self.header = replacement_header;
        self.namespace_directory_cache_root = std.math.maxInt(u64);
        self.page_cache.clear(self.allocator);
        previous_file.close(io);

        if (self.test_fail_vacuum_after_adoption) {
            self.test_fail_vacuum_after_adoption = false;
            return error.InjectedVacuumPostRenameFailure;
        }

        if (!self.no_sync) {
            try fs_paths.syncDirPortable(io, std.fs.path.dirname(self.path) orelse ".");
        }
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
    page_id: u64,
};

/// Streaming bulk loader used by vacuum. It retains at most one leaf's keys
/// plus one separator per output page, rather than materializing all keys.
const DocumentIndexBulkBuilder = struct {
    owner: *NativeFile,
    file: std.Io.File,
    next_page_id: *u64,
    leaf_keys: std.ArrayListUnmanaged([]u8) = .empty,
    leaf_pointers: std.ArrayListUnmanaged(u64) = .empty,
    children: std.ArrayListUnmanaged(DocumentIndexChild) = .empty,

    fn deinit(self: *DocumentIndexBulkBuilder) void {
        for (self.leaf_keys.items) |key| self.owner.allocator.free(key);
        self.leaf_keys.deinit(self.owner.allocator);
        self.leaf_pointers.deinit(self.owner.allocator);
        freeDocumentIndexChildren(self.owner.allocator, &self.children);
    }

    fn nodeFits(self: *DocumentIndexBulkBuilder, node: DocumentIndexNode) bool {
        const size = encodedDocumentIndexNodeSize(node) catch return false;
        return size <= self.owner.maxPagePayloadBytes();
    }

    fn appendNode(self: *DocumentIndexBulkBuilder, node: DocumentIndexNode) !u64 {
        const encoded = try encodeDocumentIndexNode(self.owner.allocator, node);
        defer self.owner.allocator.free(encoded);
        return try appendPageToFile(
            self.owner.allocator,
            self.file,
            self.owner.runtimeIo(),
            @intCast(self.owner.header.page_size),
            self.next_page_id,
            .document_index,
            encoded,
        );
    }

    fn add(self: *DocumentIndexBulkBuilder, key: []const u8, document_page_id: u64) !void {
        if (self.leaf_keys.items.len > 0 and std.mem.order(u8, self.leaf_keys.items[self.leaf_keys.items.len - 1], key) != .lt)
            return error.InvalidDocumentIndexOrder;
        const owned = try self.owner.allocator.dupe(u8, key);
        self.leaf_keys.append(self.owner.allocator, owned) catch |err| {
            self.owner.allocator.free(owned);
            return err;
        };
        self.leaf_pointers.append(self.owner.allocator, document_page_id) catch |err| {
            _ = self.leaf_keys.pop();
            self.owner.allocator.free(owned);
            return err;
        };
        const node = DocumentIndexNode{ .kind = .leaf, .keys = self.leaf_keys.items, .pointers = self.leaf_pointers.items };
        if (self.nodeFits(node)) return;
        if (self.leaf_keys.items.len == 1) return error.DocumentIndexNodeTooLarge;

        const last_key = self.leaf_keys.pop().?;
        const last_pointer = self.leaf_pointers.pop().?;
        self.flushLeaf() catch |err| {
            self.owner.allocator.free(last_key);
            return err;
        };
        self.leaf_keys.append(self.owner.allocator, last_key) catch |err| {
            self.owner.allocator.free(last_key);
            return err;
        };
        self.leaf_pointers.append(self.owner.allocator, last_pointer) catch |err| {
            _ = self.leaf_keys.pop();
            self.owner.allocator.free(last_key);
            return err;
        };
    }

    fn flushLeaf(self: *DocumentIndexBulkBuilder) !void {
        if (self.leaf_keys.items.len == 0) return;
        const first_key = try self.owner.allocator.dupe(u8, self.leaf_keys.items[0]);
        errdefer self.owner.allocator.free(first_key);
        const page_id = try self.appendNode(.{ .kind = .leaf, .keys = self.leaf_keys.items, .pointers = self.leaf_pointers.items });
        try self.children.append(self.owner.allocator, .{ .first_key = first_key, .page_id = page_id });
        for (self.leaf_keys.items) |key| self.owner.allocator.free(key);
        self.leaf_keys.clearRetainingCapacity();
        self.leaf_pointers.clearRetainingCapacity();
    }

    fn finish(self: *DocumentIndexBulkBuilder) !u64 {
        try self.flushLeaf();
        if (self.children.items.len == 0) return 0;

        while (self.children.items.len > 1) {
            var next = std.ArrayListUnmanaged(DocumentIndexChild).empty;
            errdefer freeDocumentIndexChildren(self.owner.allocator, &next);
            var start: usize = 0;
            while (start < self.children.items.len) {
                var end = start + 1;
                if (end < self.children.items.len) end += 1;
                while (end <= self.children.items.len) : (end += 1) {
                    if (!try self.internalGroupFits(self.children.items[start..end])) break;
                }
                end -= 1;
                if (end <= start + 1 and start + 1 < self.children.items.len)
                    return error.DocumentIndexNodeTooLarge;
                const group = self.children.items[start..end];
                const first_key = try self.owner.allocator.dupe(u8, group[0].first_key);
                errdefer self.owner.allocator.free(first_key);
                const page_id = try self.appendInternalGroup(group);
                try next.append(self.owner.allocator, .{ .first_key = first_key, .page_id = page_id });
                start = end;
            }
            freeDocumentIndexChildren(self.owner.allocator, &self.children);
            self.children = next;
        }
        return self.children.items[0].page_id;
    }

    fn internalGroupFits(self: *DocumentIndexBulkBuilder, children: []const DocumentIndexChild) !bool {
        if (children.len == 0) return false;
        var size: usize = document_index_header_size + @sizeOf(u64);
        for (children[1..]) |child| {
            size = try std.math.add(usize, size, @sizeOf(u16) + @sizeOf(u64) + child.first_key.len);
        }
        return size <= self.owner.maxPagePayloadBytes() and children.len - 1 <= std.math.maxInt(u16);
    }

    fn appendInternalGroup(self: *DocumentIndexBulkBuilder, children: []const DocumentIndexChild) !u64 {
        const keys = try self.owner.allocator.alloc([]u8, children.len - 1);
        defer self.owner.allocator.free(keys);
        const pointers = try self.owner.allocator.alloc(u64, children.len);
        defer self.owner.allocator.free(pointers);
        for (children, 0..) |child, i| {
            pointers[i] = child.page_id;
            if (i > 0) keys[i - 1] = child.first_key;
        }
        return try self.appendNode(.{ .kind = .internal, .keys = keys, .pointers = pointers });
    }
};

fn freeDocumentIndexChildren(allocator: Allocator, children: *std.ArrayListUnmanaged(DocumentIndexChild)) void {
    for (children.items) |child| allocator.free(child.first_key);
    children.deinit(allocator);
    children.* = .empty;
}

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
    @memcpy(out[magic_offset..][0..magic.len], magic);
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
    if (!std.mem.eql(u8, header_raw[magic_offset..][0..magic.len], magic)) return error.InvalidNativeMagic;

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
    return root_page == 0 or root_page < page_count;
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
    std.debug.assert(out.len >= page_header_size);
    std.debug.assert(payload.len <= out.len - page_header_size);
    @memset(out, 0);
    @memcpy(out[0..page_magic.len], page_magic);
    out[4] = @intFromEnum(kind);
    std.mem.writeInt(u32, out[8..12], @intCast(payload.len), .little);
    @memcpy(out[page_header_size..][0..payload.len], payload);

    var crc = std.hash.Crc32.init();
    crc.update(out[0..page_crc_offset]);
    crc.update(out[page_header_size..][0..payload.len]);
    std.mem.writeInt(u32, out[page_crc_offset..][0..4], crc.final(), .little);
}

fn decodePagePayloadAlloc(allocator: Allocator, raw: []const u8, expected_kind: PageKind) ![]u8 {
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
        else => return error.InvalidNativePageKind,
    };
    if (kind != expected_kind) return error.UnexpectedNativePageKind;

    const payload_len = std.mem.readInt(u32, raw[8..12], .little);
    if (payload_len > raw.len - page_header_size) return error.InvalidNativePageLength;

    var crc = std.hash.Crc32.init();
    crc.update(raw[0..page_crc_offset]);
    crc.update(raw[page_header_size..][0..payload_len]);
    const expected_crc = std.mem.readInt(u32, raw[page_crc_offset..][0..4], .little);
    if (crc.final() != expected_crc) return error.NativePageChecksumMismatch;

    return try allocator.dupe(u8, raw[page_header_size..][0..payload_len]);
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
    if (entry.key.len > std.math.maxInt(u32) or entry.value.len > std.math.maxInt(u32)) return error.RecordTooLarge;
    if (entry.is_delete and entry.external_value_root_page != 0) return error.InvalidNativeDocumentEntryFlags;
    const external_value = entry.external_value_root_page != 0;
    if (external_value and entry.value.len == 0) return error.InvalidNativeValueChain;

    const start = out.items.len;
    const stored_value_len: usize = if (external_value) 8 else entry.value.len;
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
    std.mem.writeInt(u32, encoded[16..20], @intCast(entry.value.len), .little);
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
    for (node.keys) |key| {
        if (key.len == 0 or key.len > std.math.maxInt(u16)) return error.RecordTooLarge;
        size = try std.math.add(usize, size, @sizeOf(u16) + @sizeOf(u64) + key.len);
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
        std.mem.writeInt(u16, out[pos..][0..2], @intCast(key.len), .little);
        pos += 2;
        const pointer_index = if (node.kind == .leaf) i else i + 1;
        std.mem.writeInt(u64, out[pos..][0..8], node.pointers[pointer_index], .little);
        pos += 8;
        @memcpy(out[pos..][0..key.len], key);
        pos += key.len;
    }
    std.debug.assert(pos == out.len);
    return out;
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
        if (key_len == 0 or pos + key_len > raw.len) return error.InvalidDocumentIndex;
        key.* = try allocator.dupe(u8, raw[pos .. pos + key_len]);
        keys_initialized += 1;
        pos += key_len;
        if (i > 0 and std.mem.order(u8, keys[i - 1], key.*) != .lt) return error.InvalidDocumentIndex;
    }
    if (pos != raw.len) return error.InvalidDocumentIndex;
    return .{ .kind = kind, .keys = keys, .pointers = pointers };
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
    var crc = std.hash.Crc32.init();
    crc.update(raw[0..active_checkpoint_offset]);
    crc.update(raw[active_checkpoint_offset + 1 .. checkpoint_slots_offset]);
    crc.update(raw[checkpoint_slots_end..header_checksum_offset]);
    return crc.final();
}

fn checkpointSlotChecksum(raw: []const u8) u32 {
    var crc = std.hash.Crc32.init();
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
        try std.testing.expectEqual(@as(u64, 9), file.activeCheckpoint().page_count);
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
    try std.testing.expectEqual(@as(u64, default_page_size * 7), report.file_size);
    try std.testing.expectEqual(@as(u64, default_page_size * 6), report.compact_size);
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
    try std.testing.expectEqual(@as(u64, default_page_size * 9), report.file_size);
    try std.testing.expectEqual(@as(u64, default_page_size * 7), report.compact_size);
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

test "lite native free map cannot reclaim previous checkpoint pages" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-free-map-previous-protected.aflite");
    defer allocator.free(path);

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
    try std.testing.expectError(error.InvalidNativeFreeMap, file.putDocument("doc:1", "v3"));
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

test "lite native page cache tracks put remove and wholesale eviction" {
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

    // Overflow clears everything and keeps only the incoming page.
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
