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
pub const max_fragment_bytes = 512 * 1024;
pub const max_batch_chunks = 8;
pub const max_batch_report_bytes = 1536 * 1024;

pub const Generation = struct {
    incarnation: u64 = 0,
    sequence: u64 = 0,
};
pub const Action = enum { prepare, chunk, activate, batch, fragment };
/// Byte framing is independent of group/index cardinality. Only a completed,
/// verified group enters the staged generation; no fragment becomes visible.
pub const Fragment = struct {
    group_id: u64,
    offset: u64,
    total_bytes: u64,
    digest: [32]u8,
    data: []const u8, // base64 canonical group report bytes

    pub fn size(self: Fragment) !usize {
        return std.base64.standard.Decoder.calcSizeForSlice(self.data);
    }
    pub fn final(self: Fragment) !bool {
        return self.offset +| try self.size() == self.total_bytes;
    }
};

pub const Request = struct {
    version: u16 = 1,
    action: Action,
    cursor: updates.Cursor,
    chunk_count: u32,
    chunk_index: u32 = 0,
    total_bytes: u64,
    report: metadata.StoreStatusReport,
    batch: []const Request = &.{},
    fragment: ?Fragment = null,

    pub fn validate(self: Request, alloc: std.mem.Allocator) !void {
        if (self.action == .fragment) {
            const fragment = self.fragment orelse return error.InvalidStoreReporterFence;
            const size = try fragment.size();
            if (size == 0 or size > max_fragment_bytes or fragment.group_id == 0 or fragment.total_bytes > max_inventory_bytes or fragment.total_bytes > self.total_bytes or
                fragment.offset >= fragment.total_bytes or size > fragment.total_bytes - fragment.offset or self.chunk_index >= self.chunk_count)
                return error.InvalidStoreReporterFence;
            const decoded = try alloc.alloc(u8, size);
            defer alloc.free(decoded);
            try std.base64.standard.Decoder.decode(decoded, fragment.data);
        } else if (self.fragment != null) return error.InvalidStoreReporterFence;
        if (self.action == .batch) {
            if (self.batch.len < 2 or self.batch.len > max_batch_chunks or self.batch.len > self.chunk_count -| self.chunk_index) return error.InvalidStoreReporterFence;
            for (self.batch, 0..) |item, i| {
                if (item.action != .chunk or item.batch.len != 0 or item.chunk_index != self.chunk_index + i or
                    !std.meta.eql(item.cursor, self.cursor) or item.chunk_count != self.chunk_count or item.total_bytes != self.total_bytes or
                    item.report.store_id != self.report.store_id) return error.InvalidStoreReporterFence;
                try item.validate(alloc);
                var header = item.report;
                header.group_statuses = &.{};
                header.runtime_statuses = &.{};
                if (!std.mem.eql(u8, &try reportDigest(header), &try reportDigest(self.report))) return error.InvalidStoreReporterFence;
            }
        } else if (self.batch.len != 0) return error.InvalidStoreReporterFence;
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
    pub fn admissionFacts(self: Request) !Facts {
        if (self.action == .batch) return self.batch[self.batch.len - 1].admissionFacts();
        if (self.fragment) |fragment| return .{ .digest = try reportDigest(fragment), .bytes = try fragment.size() };
        if (self.action == .chunk) return reportFacts(self.report);
        return .{ .digest = @splat(0), .bytes = 0 };
    }
    pub fn progressQueryWithDigest(self: Request, digest: [32]u8) ProgressQuery {
        if (self.action == .batch) return self.batch[self.batch.len - 1].progressQueryWithDigest(digest);
        return .{ .store_id = self.report.store_id, .cursor = self.cursor, .action = self.action, .chunk_index = self.chunk_index, .chunk_count = self.chunk_count, .chunk_digest = digest };
    }
    pub fn progressQuery(self: Request) !ProgressQuery {
        return self.progressQueryWithDigest((try self.admissionFacts()).digest);
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

pub const Facts = struct { digest: [32]u8, bytes: usize };

/// Hash and count the same canonical byte stream in one allocation-free pass.
pub fn reportFacts(value: anytype) !Facts {
    var buffer: [4096]u8 = undefined;
    var count = std.Io.Writer.Discarding.init(&.{});
    var hash = std.Io.Writer.Hashed(std.crypto.hash.sha2.Sha256).initHasher(&count.writer, .init(.{}), &buffer);
    try std.json.Stringify.value(value, .{}, &hash.writer);
    try hash.writer.flush();
    return .{ .digest = hash.hasher.finalResult(), .bytes = @intCast(count.fullCount()) };
}

pub fn reportDigest(value: anytype) ![32]u8 {
    return (try reportFacts(value)).digest;
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

/// Ordinary chunks borrow Prepared. Oversized groups own independently
/// retryable byte frames; neither retries nor batch planning serialize them again.
pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    chunks: []metadata.StoreStatusReport,
    fragments: []?Fragment,
    chunk_sizes: []usize,
    request: Request,

    /// A bounded batch shares one admission barrier and one replicated commit.
    /// Descriptors borrow the pinned plan; callers own only this small buffer.
    pub fn nextRequest(self: *const Plan, next_chunk: u32, buffer: *[max_batch_chunks]Request) !Request {
        var request = self.request;
        if (next_chunk == self.chunks.len) {
            request.action = .activate;
            return request;
        }
        if (next_chunk > self.chunks.len) return error.InvalidStoreReporterFence;
        if (self.fragments[next_chunk]) |fragment| {
            request.action = .fragment;
            request.chunk_index = next_chunk;
            request.fragment = fragment;
            return request;
        }
        var count: usize = 0;
        var bytes: usize = 0;
        for (self.chunks[next_chunk..], next_chunk..) |report, index| {
            if (self.fragments[index] != null) break;
            const size = self.chunk_sizes[index];
            if (count != 0 and (count == buffer.len or bytes + size > max_batch_report_bytes)) break;
            buffer[count] = self.request;
            buffer[count].action = .chunk;
            buffer[count].chunk_index = next_chunk + @as(u32, @intCast(count));
            buffer[count].report = report;
            bytes += size;
            count += 1;
        }
        if (count == 1) return buffer[0];
        request.action = .batch;
        request.chunk_index = next_chunk;
        request.batch = buffer[0..count];
        return request;
    }

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
        var fragments: std.ArrayListUnmanaged(?Fragment) = .empty;
        var chunk_sizes: std.ArrayListUnmanaged(usize) = .empty;
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
                    if (count == 1) {
                        total = try std.math.add(usize, total, size);
                        const fragment_count = std.math.divCeil(usize, size, max_fragment_bytes) catch unreachable;
                        if (total > max_inventory_bytes or fragment_count > max_chunks - chunks.items.len) return error.ResourceRequestTooLarge;
                        // Only unusually large groups need owned frame bytes.
                        // Serialize once, then retain bounded independent frames.
                        const encoded = try std.json.Stringify.valueAlloc(alloc, report, .{});
                        defer alloc.free(encoded);
                        var digest: [32]u8 = undefined;
                        std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
                        var offset: usize = 0;
                        while (offset < encoded.len) {
                            const end = @min(offset + max_fragment_bytes, encoded.len);
                            const data = try a.alloc(u8, std.base64.standard.Encoder.calcSize(end - offset));
                            _ = std.base64.standard.Encoder.encode(data, encoded[offset..end]);
                            const fragment: Fragment = .{ .group_id = replacements[start].id, .offset = offset, .total_bytes = encoded.len, .digest = digest, .data = data };
                            chain = chainDigest(chain, try reportDigest(fragment));
                            var header = report;
                            header.group_statuses = &.{};
                            header.runtime_statuses = &.{};
                            try chunks.append(a, header);
                            try fragments.append(a, fragment);
                            try chunk_sizes.append(a, end - offset);
                            offset = end;
                        }
                        start += 1;
                        break;
                    }
                    count = @max(1, count / 2);
                    continue;
                }
                total = try std.math.add(usize, total, size);
                if (total > max_inventory_bytes or chunks.items.len == max_chunks) return error.ResourceRequestTooLarge;
                chain = chainDigest(chain, try reportDigest(report));
                try chunks.append(a, report);
                try fragments.append(a, null);
                try chunk_sizes.append(a, size);
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
            .fragments = fragments.items,
            .chunk_sizes = chunk_sizes.items,
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
