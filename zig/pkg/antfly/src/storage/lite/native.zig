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

const Allocator = std.mem.Allocator;

pub const magic = "AFLITE\x02N";
pub const format_version: u32 = 1;
pub const default_page_size: u32 = 4096;
pub const header_size: usize = 4096;
pub const checkpoint_slot_count = 2;
pub const checkpoint_slot_size: usize = 64;
pub const page_magic = "AFLP";
pub const page_header_size: usize = 16;

const magic_offset: usize = 0;
const version_offset: usize = 8;
const page_size_offset: usize = 12;
const header_size_offset: usize = 16;
const active_checkpoint_offset: usize = 20;
const checkpoint_slots_offset: usize = 64;
const header_checksum_offset: usize = header_size - 4;
const page_crc_offset: usize = 12;

pub const PageKind = enum(u8) {
    data = 1,
    catalog = 2,
    document = 3,
    value = 4,
};

const document_delete_flag: u8 = 1 << 0;
const document_external_value_flag: u8 = 1 << 1;
const value_page_header_size: usize = 8;

pub const CatalogEntry = struct {
    previous_page: u64,
    key: []const u8,
    value: []const u8,
};

pub const DocumentEntry = struct {
    previous_page: u64,
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

pub const DocumentMutation = struct {
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
};

pub const LockMode = enum {
    writer,
    reader,
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

pub const NativeFile = struct {
    allocator: Allocator,
    io_impl: std.Io.Threaded,
    path: []u8,
    file: std.Io.File,
    header: Header,
    read_only: bool = false,

    pub fn open(allocator: Allocator, path: []const u8, read_only: bool) !NativeFile {
        var io_impl = std.Io.Threaded.init(allocator, .{});
        errdefer io_impl.deinit();
        const io = io_impl.io();

        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);

        const file = try openLockedFile(io, path, if (read_only) .reader else .writer);
        errdefer file.close(io);

        var header_bytes: [header_size]u8 = undefined;
        try readExactAt(file, io, &header_bytes, 0);

        return .{
            .allocator = allocator,
            .io_impl = io_impl,
            .path = owned_path,
            .file = file,
            .header = try decodeHeader(&header_bytes),
            .read_only = read_only,
        };
    }

    pub fn create(allocator: Allocator, path: []const u8) !NativeFile {
        var io_impl = std.Io.Threaded.init(allocator, .{});
        errdefer io_impl.deinit();
        const io = io_impl.io();

        try createFile(io, path, .writer);
        io_impl.deinit();
        return try open(allocator, path, false);
    }

    pub fn close(self: *NativeFile) void {
        self.file.close(self.io_impl.io());
        self.allocator.free(self.path);
        self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn activeCheckpoint(self: *const NativeFile) CheckpointSlot {
        return self.header.checkpoints[self.header.active_checkpoint];
    }

    pub fn check(self: *NativeFile) !CheckReport {
        const checkpoint = self.activeCheckpoint();
        const page_size: u64 = self.header.page_size;
        const expected_size = checkpoint.page_count * page_size;
        const file_size = (try self.file.stat(self.io_impl.io())).size;

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

        const catalog_records = self.countChainPages(.catalog, checkpoint.catalog_root_page) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        const document_records = self.countChainPages(.document, checkpoint.document_root_page) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        if (report.tail_bytes != 0) return invalidCheck(report, "tail_bytes");

        var valid = report;
        valid.record_count = catalog_records + document_records;
        return valid;
    }

    pub fn allocatePage(self: *NativeFile, contents: []const u8) !u64 {
        const previous = self.activeCheckpoint();
        const page_id = previous.page_count;
        var next = previous;
        next.commit_sequence += 1;
        next.page_count = page_id + 1;
        return try self.appendPage(.data, contents, next);
    }

    pub fn readPageAlloc(self: *NativeFile, allocator: Allocator, page_id: u64) ![]u8 {
        const checkpoint = self.activeCheckpoint();
        if (page_id == 0 or page_id >= checkpoint.page_count) return error.InvalidPageId;

        const page_size: usize = @intCast(self.header.page_size);
        const page = try allocator.alloc(u8, page_size);
        errdefer allocator.free(page);

        try readExactAt(self.file, self.io_impl.io(), page, page_id * @as(u64, self.header.page_size));
        return page;
    }

    pub fn readPagePayloadAlloc(self: *NativeFile, allocator: Allocator, page_id: u64) ![]u8 {
        const page = try self.readPageAlloc(allocator, page_id);
        defer allocator.free(page);
        return try decodePagePayloadAlloc(allocator, page, .data);
    }

    pub fn putCatalogRecord(self: *NativeFile, key: []const u8, value: []const u8) !void {
        const previous = self.activeCheckpoint();
        const page_id = previous.page_count;
        var payload = std.ArrayListUnmanaged(u8).empty;
        defer payload.deinit(self.allocator);
        try encodeCatalogEntry(self.allocator, &payload, .{
            .previous_page = previous.catalog_root_page,
            .key = key,
            .value = value,
        });

        var next = previous;
        next.commit_sequence += 1;
        next.catalog_root_page = page_id;
        next.page_count = page_id + 1;
        _ = try self.appendPage(.catalog, payload.items, next);
    }

    pub fn getCatalogRecordAlloc(self: *NativeFile, allocator: Allocator, key: []const u8) !?[]u8 {
        var page_id = self.activeCheckpoint().catalog_root_page;
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAlloc(allocator, page_id, .catalog);
            defer allocator.free(payload);
            const entry = try decodeCatalogEntry(payload);
            if (std.mem.eql(u8, entry.key, key)) return try allocator.dupe(u8, entry.value);
            page_id = entry.previous_page;
        }
        return null;
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
        var next_root_page = previous.document_root_page;
        var next_page_id = previous.page_count;

        for (mutations) |mutation| {
            var external_value_root_page: u64 = 0;
            if (!mutation.is_delete and !self.documentEntryFitsInline(mutation.key, mutation.value)) {
                external_value_root_page = next_page_id;
                next_page_id = try self.writeValuePages(next_page_id, mutation.value);
            }

            const page_id = next_page_id;
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(self.allocator);
            try encodeDocumentEntry(self.allocator, &payload, .{
                .previous_page = next_root_page,
                .key = mutation.key,
                .value = mutation.value,
                .is_delete = mutation.is_delete,
                .external_value_root_page = external_value_root_page,
            });
            try self.writePage(page_id, .document, payload.items);
            next_root_page = page_id;
            next_page_id += 1;
        }

        try self.file.sync(self.io_impl.io());

        var next = previous;
        next.commit_sequence += 1;
        next.document_root_page = next_root_page;
        next.page_count = next_page_id;
        try self.publishCheckpoint(next);
    }

    pub fn getDocumentAlloc(self: *NativeFile, allocator: Allocator, key: []const u8) !?[]u8 {
        var page_id = self.activeCheckpoint().document_root_page;
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAlloc(allocator, page_id, .document);
            defer allocator.free(payload);
            const entry = try decodeDocumentEntry(payload);
            if (std.mem.eql(u8, entry.key, key)) {
                if (entry.is_delete) return null;
                return try self.documentEntryValueAlloc(allocator, entry);
            }
            page_id = entry.previous_page;
        }
        return null;
    }

    pub fn snapshotDocumentsAlloc(self: *NativeFile, allocator: Allocator) ![]OwnedDocument {
        var map = std.StringHashMapUnmanaged(?[]u8).empty;
        defer {
            var it = map.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                if (entry.value_ptr.*) |value| allocator.free(value);
            }
            map.deinit(allocator);
        }

        var page_id = self.activeCheckpoint().document_root_page;
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAlloc(allocator, page_id, .document);
            defer allocator.free(payload);
            const entry = try decodeDocumentEntry(payload);

            if (!map.contains(entry.key)) {
                const owned_key = try allocator.dupe(u8, entry.key);
                errdefer allocator.free(owned_key);
                const owned_value = if (entry.is_delete) null else try self.documentEntryValueAlloc(allocator, entry);
                errdefer if (owned_value) |value| allocator.free(value);
                try map.put(allocator, owned_key, owned_value);
            }
            page_id = entry.previous_page;
        }

        var docs = std.ArrayListUnmanaged(OwnedDocument).empty;
        errdefer {
            for (docs.items) |doc| {
                allocator.free(doc.key);
                allocator.free(doc.value);
            }
            docs.deinit(allocator);
        }
        var it = map.iterator();
        while (it.next()) |entry| {
            const value = entry.value_ptr.* orelse continue;
            const key = try allocator.dupe(u8, entry.key_ptr.*);
            errdefer allocator.free(key);
            const value_copy = try allocator.dupe(u8, value);
            errdefer allocator.free(value_copy);
            try docs.append(allocator, .{ .key = key, .value = value_copy });
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

    pub fn maxPagePayloadBytes(self: *const NativeFile) usize {
        return @as(usize, @intCast(self.header.page_size)) - page_header_size;
    }

    pub fn maxValuePagePayloadBytes(self: *const NativeFile) usize {
        return self.maxPagePayloadBytes() - value_page_header_size;
    }

    fn validateDocumentMutation(self: *const NativeFile, mutation: DocumentMutation) !void {
        if (mutation.key.len > std.math.maxInt(u32) or mutation.value.len > std.math.maxInt(u32)) return error.RecordTooLarge;
        const fixed_len = 20 + mutation.key.len;
        if (fixed_len > self.maxPagePayloadBytes()) return error.PageTooLarge;
        if (mutation.is_delete) return;
        if (mutation.value.len <= self.maxPagePayloadBytes() - fixed_len) return;
        if (value_page_header_size > self.maxPagePayloadBytes()) return error.InvalidNativePageLength;
        if (fixed_len + 8 > self.maxPagePayloadBytes()) return error.PageTooLarge;
    }

    fn documentEntryFitsInline(self: *const NativeFile, key: []const u8, value: []const u8) bool {
        const fixed_len = 20 + key.len;
        return fixed_len <= self.maxPagePayloadBytes() and value.len <= self.maxPagePayloadBytes() - fixed_len;
    }

    fn documentEntryValueAlloc(self: *NativeFile, allocator: Allocator, entry: DocumentEntry) ![]u8 {
        if (entry.external_value_root_page != 0) {
            return try self.readValuePagesAlloc(allocator, entry.external_value_root_page, entry.external_value_len);
        }
        return try allocator.dupe(u8, entry.value);
    }

    fn readPagePayloadByKindAlloc(self: *NativeFile, allocator: Allocator, page_id: u64, kind: PageKind) ![]u8 {
        const page = try self.readPageAlloc(allocator, page_id);
        defer allocator.free(page);
        return try decodePagePayloadAlloc(allocator, page, kind);
    }

    fn countChainPages(self: *NativeFile, kind: PageKind, root_page_id: u64) !u64 {
        var count: u64 = 0;
        var page_id = root_page_id;
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAlloc(self.allocator, page_id, kind);
            defer self.allocator.free(payload);
            page_id = switch (kind) {
                .catalog => (try decodeCatalogEntry(payload)).previous_page,
                .document => blk: {
                    const entry = try decodeDocumentEntry(payload);
                    if (entry.external_value_root_page != 0) {
                        try self.validateValuePages(entry.external_value_root_page, entry.external_value_len);
                    }
                    break :blk entry.previous_page;
                },
                .data, .value => return error.UnexpectedNativePageKind,
            };
            count += 1;
            if (count > self.activeCheckpoint().page_count) return error.InvalidNativePageChain;
        }
        return count;
    }

    fn appendPage(self: *NativeFile, kind: PageKind, contents: []const u8, checkpoint: CheckpointSlot) !u64 {
        if (self.read_only) return error.ReadOnly;

        const previous = self.activeCheckpoint();
        const page_id = previous.page_count;
        if (checkpoint.page_count != page_id + 1) return error.InvalidNativeCheckpoint;

        try self.writePage(page_id, kind, contents);
        try self.file.sync(self.io_impl.io());
        try self.publishCheckpoint(checkpoint);
        return page_id;
    }

    fn writeValuePages(self: *NativeFile, first_page_id: u64, value: []const u8) !u64 {
        if (value.len == 0) return error.InvalidNativeValueChain;
        const chunk_size = self.maxValuePagePayloadBytes();
        if (chunk_size == 0) return error.InvalidNativePageLength;
        const page_count = std.math.divCeil(usize, value.len, chunk_size) catch unreachable;

        var page_id = first_page_id;
        var offset: usize = 0;
        var page_index: usize = 0;
        while (offset < value.len) : (page_index += 1) {
            const len = @min(chunk_size, value.len - offset);
            const next_page = if (page_index + 1 < page_count) page_id + 1 else 0;

            const payload = try self.allocator.alloc(u8, value_page_header_size + len);
            defer self.allocator.free(payload);
            std.mem.writeInt(u64, payload[0..8], next_page, .little);
            @memcpy(payload[value_page_header_size..][0..len], value[offset..][0..len]);

            try self.writePage(page_id, .value, payload);
            page_id += 1;
            offset += len;
        }

        return page_id;
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

    fn validateValuePages(self: *NativeFile, root_page_id: u64, value_len: usize) !void {
        const value = try self.readValuePagesAlloc(self.allocator, root_page_id, value_len);
        self.allocator.free(value);
    }

    fn writePage(self: *NativeFile, page_id: u64, kind: PageKind, contents: []const u8) !void {
        if (contents.len > self.maxPagePayloadBytes()) return error.PageTooLarge;

        const page_size: usize = @intCast(self.header.page_size);
        const page_offset = page_id * @as(u64, self.header.page_size);

        const page = try self.allocator.alloc(u8, page_size);
        defer self.allocator.free(page);
        encodePage(page, kind, contents);

        try self.file.setLength(self.io_impl.io(), page_offset + self.header.page_size);
        try self.file.writePositionalAll(self.io_impl.io(), page, page_offset);
    }

    fn publishCheckpoint(self: *NativeFile, checkpoint: CheckpointSlot) !void {
        const next_slot: u8 = if (self.header.active_checkpoint == 0) 1 else 0;
        self.header.checkpoints[next_slot] = checkpoint;
        self.header.active_checkpoint = next_slot;

        var encoded: [header_size]u8 = undefined;
        encodeHeader(&encoded, self.header);

        try self.file.writePositionalAll(self.io_impl.io(), &encoded, 0);
        try self.file.sync(self.io_impl.io());
    }
};

pub fn create(io: std.Io, path: []const u8) !void {
    try createFile(io, path, .writer);
}

fn createFile(io: std.Io, path: []const u8, lock_mode: LockMode) !void {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});

    var file = try createLockedFile(io, path, lock_mode, true);
    defer file.close(io);

    try file.writePositionalAll(io, &encoded, 0);
    try file.sync(io);
}

fn openLockedFile(io: std.Io, path: []const u8, lock_mode: LockMode) !std.Io.File {
    return std.Io.Dir.cwd().openFile(io, path, .{
        .mode = if (lock_mode == .reader) .read_only else .read_write,
        .lock = fileLockForMode(lock_mode),
        .lock_nonblocking = true,
    }) catch |err| switch (err) {
        error.FileLocksUnsupported => try std.Io.Dir.cwd().openFile(io, path, .{
            .mode = if (lock_mode == .reader) .read_only else .read_write,
        }),
        else => return err,
    };
}

fn createLockedFile(io: std.Io, path: []const u8, lock_mode: LockMode, truncate: bool) !std.Io.File {
    return std.Io.Dir.cwd().createFile(io, path, .{
        .read = true,
        .truncate = truncate,
        .lock = fileLockForMode(lock_mode),
        .lock_nonblocking = true,
    }) catch |err| switch (err) {
        error.FileLocksUnsupported => try std.Io.Dir.cwd().createFile(io, path, .{
            .read = true,
            .truncate = truncate,
        }),
        else => return err,
    };
}

fn fileLockForMode(mode: LockMode) std.Io.File.Lock {
    return switch (mode) {
        .writer => .exclusive,
        .reader => .shared,
    };
}

pub fn inspect(_: Allocator, io: std.Io, path: []const u8) !InspectReport {
    var file = try openLockedFile(io, path, .reader);
    defer file.close(io);

    var header_bytes: [header_size]u8 = undefined;
    try readExactAt(file, io, &header_bytes, 0);
    return inspectBytes(&header_bytes);
}

pub fn checkFile(allocator: Allocator, path: []const u8) !CheckReport {
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var file = try openLockedFile(io, path, .reader);
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
    _ = decodeHeader(&header_bytes) catch |err| {
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

    var native_file = try NativeFile.open(allocator, path, true);
    defer native_file.close();
    return try native_file.check();
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

    const active_checkpoint = header_raw[active_checkpoint_offset];
    if (active_checkpoint >= checkpoint_slot_count) return error.InvalidNativeCheckpointSlot;

    var checkpoints: [checkpoint_slot_count]CheckpointSlot = undefined;
    for (&checkpoints, 0..) |*slot, index| {
        slot.* = decodeCheckpointSlot(header_raw[checkpointOffset(index)..][0..checkpoint_slot_size]);
    }

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
    std.mem.writeInt(u64, out[0..8], slot.commit_sequence, .little);
    std.mem.writeInt(u64, out[8..16], slot.catalog_root_page, .little);
    std.mem.writeInt(u64, out[16..24], slot.document_root_page, .little);
    std.mem.writeInt(u64, out[24..32], slot.index_catalog_root_page, .little);
    std.mem.writeInt(u64, out[32..40], slot.free_map_root_page, .little);
    std.mem.writeInt(u64, out[40..48], slot.page_count, .little);
}

fn decodeCheckpointSlot(raw: []const u8) CheckpointSlot {
    std.debug.assert(raw.len == checkpoint_slot_size);
    return .{
        .commit_sequence = std.mem.readInt(u64, raw[0..8], .little),
        .catalog_root_page = std.mem.readInt(u64, raw[8..16], .little),
        .document_root_page = std.mem.readInt(u64, raw[16..24], .little),
        .index_catalog_root_page = std.mem.readInt(u64, raw[24..32], .little),
        .free_map_root_page = std.mem.readInt(u64, raw[32..40], .little),
        .page_count = std.mem.readInt(u64, raw[40..48], .little),
    };
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
    if (entry.key.len > std.math.maxInt(u32) or entry.value.len > std.math.maxInt(u32)) return error.RecordTooLarge;
    const start = out.items.len;
    try out.resize(allocator, start + 16 + entry.key.len + entry.value.len);
    const encoded = out.items[start..];
    std.mem.writeInt(u64, encoded[0..8], entry.previous_page, .little);
    std.mem.writeInt(u32, encoded[8..12], @intCast(entry.key.len), .little);
    std.mem.writeInt(u32, encoded[12..16], @intCast(entry.value.len), .little);
    @memcpy(encoded[16..][0..entry.key.len], entry.key);
    @memcpy(encoded[16 + entry.key.len ..][0..entry.value.len], entry.value);
}

fn decodeCatalogEntry(raw: []const u8) !CatalogEntry {
    if (raw.len < 16) return error.TruncatedNativeCatalogEntry;
    const previous_page = std.mem.readInt(u64, raw[0..8], .little);
    const key_len = std.mem.readInt(u32, raw[8..12], .little);
    const value_len = std.mem.readInt(u32, raw[12..16], .little);
    const payload_len = @as(u64, key_len) + @as(u64, value_len);
    if (payload_len > raw.len - 16) return error.TruncatedNativeCatalogEntry;
    const key_start: usize = 16;
    const key_end = key_start + @as(usize, @intCast(key_len));
    const value_end = key_end + @as(usize, @intCast(value_len));
    return .{
        .previous_page = previous_page,
        .key = raw[key_start..key_end],
        .value = raw[key_end..value_end],
    };
}

fn encodeDocumentEntry(allocator: Allocator, out: *std.ArrayListUnmanaged(u8), entry: DocumentEntry) !void {
    if (entry.key.len > std.math.maxInt(u32) or entry.value.len > std.math.maxInt(u32)) return error.RecordTooLarge;
    if (entry.is_delete and entry.external_value_root_page != 0) return error.InvalidNativeDocumentEntryFlags;
    const external_value = entry.external_value_root_page != 0;
    if (external_value and entry.value.len == 0) return error.InvalidNativeValueChain;

    const start = out.items.len;
    const stored_value_len: usize = if (external_value) 8 else entry.value.len;
    try out.resize(allocator, start + 20 + entry.key.len + stored_value_len);
    const encoded = out.items[start..];
    std.mem.writeInt(u64, encoded[0..8], entry.previous_page, .little);
    encoded[8] =
        (if (entry.is_delete) document_delete_flag else 0) |
        (if (external_value) document_external_value_flag else 0);
    @memset(encoded[9..12], 0);
    std.mem.writeInt(u32, encoded[12..16], @intCast(entry.key.len), .little);
    std.mem.writeInt(u32, encoded[16..20], @intCast(entry.value.len), .little);
    @memcpy(encoded[20..][0..entry.key.len], entry.key);
    if (external_value) {
        std.mem.writeInt(u64, encoded[20 + entry.key.len ..][0..8], entry.external_value_root_page, .little);
    } else {
        @memcpy(encoded[20 + entry.key.len ..][0..entry.value.len], entry.value);
    }
}

fn decodeDocumentEntry(raw: []const u8) !DocumentEntry {
    if (raw.len < 20) return error.TruncatedNativeDocumentEntry;
    const previous_page = std.mem.readInt(u64, raw[0..8], .little);
    const flags = raw[8];
    if (flags & ~(document_delete_flag | document_external_value_flag) != 0) return error.InvalidNativeDocumentEntryFlags;
    const is_delete = flags & document_delete_flag != 0;
    const external_value = flags & document_external_value_flag != 0;
    if (is_delete and external_value) return error.InvalidNativeDocumentEntryFlags;

    const key_len = std.mem.readInt(u32, raw[12..16], .little);
    const value_len = std.mem.readInt(u32, raw[16..20], .little);
    const stored_value_len: u64 = if (external_value) 8 else value_len;
    const payload_len = @as(u64, key_len) + stored_value_len;
    if (payload_len > raw.len - 20) return error.TruncatedNativeDocumentEntry;
    const key_start: usize = 20;
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

fn decodeValuePage(raw: []const u8) !ValuePage {
    if (raw.len < value_page_header_size) return error.TruncatedNativeValuePage;
    return .{
        .next_page = std.mem.readInt(u64, raw[0..8], .little),
        .chunk = raw[value_page_header_size..],
    };
}

fn readExactAt(file: std.Io.File, io: std.Io, out: []u8, offset: u64) !void {
    const read = try file.readPositionalAll(io, out, offset);
    if (read != out.len) return error.EndOfStream;
}

fn headerChecksum(raw: []const u8) u32 {
    var crc = std.hash.Crc32.init();
    crc.update(raw[0..header_checksum_offset]);
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
        error.TruncatedNativeDocumentEntry => "truncated_document_entry",
        error.InvalidNativeDocumentEntryFlags => "invalid_document_entry_flags",
        error.InvalidNativePageChain => "invalid_page_chain",
        error.TruncatedNativeValuePage => "truncated_value_page",
        error.InvalidNativeValueChain => "invalid_value_chain",
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
        try std.testing.expectEqual(@as(u64, 2), file.activeCheckpoint().page_count);

        const page = try file.readPagePayloadAlloc(allocator, page_id);
        defer allocator.free(page);
        try std.testing.expectEqualStrings("hello native page", page);
    }

    const report = try inspect(allocator, std.testing.io, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.commit_sequence);
    try std.testing.expectEqual(@as(u64, 2), report.page_count);
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
    try std.testing.expectEqual(@as(u64, 2), reopened.activeCheckpoint().page_count);
    const page = try reopened.readPagePayloadAlloc(allocator, 1);
    defer allocator.free(page);
    try std.testing.expectEqualStrings("persisted", page);
    try std.testing.expectError(error.ReadOnly, reopened.allocatePage("nope"));
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

    try std.testing.expectEqual(@as(u64, 2), reader_a.activeCheckpoint().page_count);
    try std.testing.expectEqual(@as(u64, 2), reader_b.activeCheckpoint().page_count);
}

test "lite native file active writer blocks other opens" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-writer-lock.aflite");
    defer allocator.free(path);

    var writer = try NativeFile.create(allocator, path);
    defer writer.close();

    try std.testing.expectError(error.WouldBlock, NativeFile.open(allocator, path, false));
    try std.testing.expectError(error.WouldBlock, NativeFile.open(allocator, path, true));
    try std.testing.expectError(error.WouldBlock, inspect(allocator, std.testing.io, path));
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
        try std.testing.expectEqual(@as(u64, @intCast(2 + value_pages)), file.activeCheckpoint().page_count);
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
        try std.testing.expectEqual(@as(u64, 6), file.activeCheckpoint().page_count);
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
    try std.testing.expectEqual(@as(u64, default_page_size * 3), report.file_size);
    try std.testing.expectEqual(@as(u64, 0), report.tail_bytes);
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

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const report = try reopened.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("truncated_file", report.issue.?);
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
