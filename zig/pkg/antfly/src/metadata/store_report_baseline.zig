// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Resumable full-inventory publication. Chunks build an invisible generation;
//! only activation changes the report root and acknowledged sparse cursor.
const std = @import("std");
const metadata = @import("table_manager.zig");
const updates = @import("store_report_update.zig");

pub const path_suffix = "/status/baseline";
pub const max_chunk_bytes = 2 * 1024 * 1024;
pub const max_report_bytes = 1024 * 1024;
pub const max_inventory_bytes = 512 * 1024 * 1024;
pub const max_chunks = 4096;
pub const max_groups_per_chunk = 64;

pub const Generation = struct {
    incarnation: u64 = 0,
    sequence: u64 = 0,
};
pub const Action = enum { prepare, chunk, activate };
pub const Request = struct {
    version: u16 = 1,
    action: Action,
    cursor: updates.Cursor,
    chunk_count: u32,
    chunk_index: u32 = 0,
    total_bytes: u64,
    report: metadata.StoreStatusReport,

    pub fn validate(self: Request, alloc: std.mem.Allocator) !void {
        if (self.version != 1 or self.chunk_count == 0 or self.chunk_count > max_chunks or self.total_bytes > max_inventory_bytes or self.total_bytes == 0 or self.cursor.reporter_incarnation != self.report.reporter_incarnation) return error.InvalidStoreReporterFence;
        try (updates.Update{ .sequence = self.cursor.sequence, .report = self.report }).validate(alloc);
        if (self.action == .chunk) {
            if (self.chunk_index >= self.chunk_count) return error.InvalidStoreReporterFence;
            var ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
            defer ids.deinit(alloc);
            for (self.report.group_statuses) |item| try ids.put(alloc, item.group_id, {});
            for (self.report.runtime_statuses) |item| try ids.put(alloc, item.group_id, {});
            for (self.report.runtime_statuses) |runtime| for (runtime.indexes) |index| {
                if (index.embedding_activity_observed or !std.meta.eql(index.embedding_activity, @as(@TypeOf(index.embedding_activity), .{}))) return error.InvalidStoreReporterFence;
            };
            if (ids.count() == 0 or ids.count() > max_groups_per_chunk) return error.InvalidStoreReporterFence;
        } else if (self.report.group_statuses.len != 0 or self.report.runtime_statuses.len != 0) return error.InvalidStoreReporterFence;
    }
    pub fn progressQuery(self: Request) !ProgressQuery {
        return .{ .store_id = self.report.store_id, .cursor = self.cursor, .action = self.action, .chunk_index = self.chunk_index, .chunk_count = self.chunk_count, .chunk_digest = if (self.action == .chunk) try reportDigest(self.report) else @splat(0) };
    }
    pub fn generation(self: Request) Generation {
        return .{ .incarnation = self.cursor.reporter_incarnation, .sequence = self.cursor.sequence };
    }
};
pub const ProgressQuery = struct {
    store_id: u64,
    cursor: updates.Cursor,
    action: Action,
    chunk_index: u32,
    chunk_count: u32,
    chunk_digest: [32]u8,
};
pub const Progress = struct {
    cursor: updates.Cursor,
    next_chunk: u32 = 0,
    collecting: bool = false,
    activated: bool = false,
};
pub const Command = struct {
    request: Request,
    expected_header: [32]u8,
    admission_cursor: ?updates.Cursor,
    // Calculated from canonical HTTP input at admission; the replicated binary
    // command carries these facts so apply never expands runtime reports to JSON.
    report_digest: [32]u8 = @splat(0),
    report_bytes: u64 = 0,
};

pub fn reportDigest(value: anytype) ![32]u8 {
    var buffer: [4096]u8 = undefined;
    var hash: std.Io.Writer.Hashing(std.crypto.hash.sha2.Sha256) = .init(&buffer);
    try std.json.Stringify.value(value, .{}, &hash.writer);
    try hash.writer.flush();
    return hash.hasher.finalResult();
}

pub fn chainDigest(previous: [32]u8, chunk: [32]u8) [32]u8 {
    var hash: std.crypto.hash.sha2.Sha256 = .init(.{});
    hash.update(&previous);
    hash.update(&chunk);
    return hash.finalResult();
}

pub fn reportSize(value: anytype) !usize {
    var buffer: [4096]u8 = undefined;
    var count = std.Io.Writer.Discarding.init(&buffer);
    try std.json.Stringify.value(value, .{}, &count.writer);
    return @intCast(count.fullCount());
}

/// The plan owns descriptors only. Report payloads stay pinned by Prepared;
/// serialization uses at most one bounded chunk, including during retries.
pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    chunks: []metadata.StoreStatusReport,
    request: Request,

    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
    }

    pub fn init(alloc: std.mem.Allocator, prepared: *const updates.Publisher.Prepared) !Plan {
        if (!prepared.full) return error.InvalidStoreReporterFence;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        const replacements = try a.dupe(updates.Publisher.Pending, prepared.replacements);
        std.mem.sort(updates.Publisher.Pending, replacements, {}, struct {
            fn less(_: void, left: updates.Publisher.Pending, right: updates.Publisher.Pending) bool {
                return left.id < right.id;
            }
        }.less);
        const health_class = try a.dupe(u8, prepared.update.report.health_class);
        var chunks: std.ArrayListUnmanaged(metadata.StoreStatusReport) = .empty;
        var chain: [32]u8 = @splat(0);
        var total: usize = 0;
        var start: usize = 0;
        while (start < replacements.len) {
            var count = @min(max_groups_per_chunk, replacements.len - start);
            while (true) {
                var groups: std.ArrayListUnmanaged(metadata.GroupStatusReport) = .empty;
                var runtime: std.ArrayListUnmanaged(metadata.RuntimeGroupStatusReport) = .empty;
                for (replacements[start..][0..count]) |item| {
                    try groups.appendSlice(a, item.group.groups);
                    for (item.group.runtimes) |*report| for (report.indexes) |*idx| {
                        idx.embedding_activity_observed = false;
                        idx.embedding_activity = .{};
                    };
                    try runtime.appendSlice(a, item.group.runtimes);
                }
                var report = prepared.update.report;
                report.health_class = health_class;
                report.group_statuses = groups.items;
                report.runtime_statuses = runtime.items;
                const size = try reportSize(report);
                if (size > max_report_bytes) {
                    if (count == 1) return error.ResourceRequestTooLarge;
                    count = @max(1, count / 2);
                    continue;
                }
                total = try std.math.add(usize, total, size);
                if (total > max_inventory_bytes or chunks.items.len == max_chunks) return error.ResourceRequestTooLarge;
                chain = chainDigest(chain, try reportDigest(report));
                try chunks.append(a, report);
                start += count;
                break;
            }
        }
        if (chunks.items.len == 0) return error.InvalidStoreReporterFence;
        var header = prepared.update.report;
        header.health_class = health_class;
        header.group_statuses = &.{};
        header.runtime_statuses = &.{};
        return .{
            .arena = arena,
            .chunks = chunks.items,
            .request = .{
                .action = .prepare,
                .cursor = .{ .reporter_incarnation = header.reporter_incarnation, .sequence = prepared.update.sequence, .digest = chain },
                .chunk_count = @intCast(chunks.items.len),
                .total_bytes = total,
                .report = header,
            },
        };
    }
};
