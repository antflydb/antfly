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

//! Seekable, authenticated native text statistics. Raw segment envelopes are
//! independent of request scoring parameters; global DF updates use bounded
//! sorted deltas and copy-on-write pages, reusing unchanged file contributions.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const page_store = @import("../serverless/graph_segment/page_store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
pub const Ref = tree.Ref;
const inverted = local.section_inverted;

pub fn keyAlloc(a: A, field: []const u8, term: []const u8) ![]u8 {
    if (field.len > std.math.maxInt(u32) or field.len +| term.len > tree.max_key_bytes - 4) return error.InvalidNativeLakeTextStatistics;
    const key = try a.alloc(u8, 4 + field.len + term.len);
    std.mem.writeInt(u32, key[0..4], @intCast(field.len), .big);
    @memcpy(key[4..][0..field.len], field);
    @memcpy(key[4 + field.len ..], term);
    return key;
}
pub const Summary = struct {
    frequency: u32,
    max_frequency: u32,
    min_norm: u32,
    fn decode(bytes: []const u8) !Summary {
        if (bytes.len != 12) return error.InvalidNativeLakeTextStatistics;
        const result: Summary = .{ .frequency = std.mem.readInt(u32, bytes[0..4], .little), .max_frequency = std.mem.readInt(u32, bytes[4..8], .little), .min_norm = std.mem.readInt(u32, bytes[8..12], .little) };
        if (result.frequency == 0 or result.max_frequency == 0) return error.InvalidNativeLakeTextStatistics;
        return result;
    }
    pub fn bound(self: Summary, average: f32, config: inverted.BM25Config) local.index.IndexSnapshot.TextTermSummary {
        const valid = std.math.isFinite(config.k1) and config.k1 >= 0 and std.math.isFinite(config.b) and config.b >= 0 and config.b <= 1 and std.math.isFinite(average) and average > 0;
        return .{ .frequency = self.frequency, .tf_upper = if (!valid) std.math.inf(f32) else if (self.max_frequency == std.math.maxInt(u32)) inverted.BM25TermScorer.init(average, 1, config).maxScore() else inverted.bm25ScoreWithIdf(self.max_frequency, self.min_norm, average, 1, config) };
    }
};

pub fn publishSegment(a: A, store: *stores.ArtifactStore, bytes: []const u8, cancellation: Cancellation) !?Ref {
    var segment = try local.segment.SegmentReader.init(a, bytes);
    defer segment.deinit();
    const names = try a.alloc([]const u8, segment.fields.len);
    defer a.free(names);
    for (segment.fields, names) |field, *name| name.* = field.name;
    std.mem.sort([]const u8, names, {}, struct {
        fn less(_: void, left: []const u8, right: []const u8) bool {
            return left.len < right.len or (left.len == right.len and std.mem.order(u8, left, right) == .lt);
        }
    }.less);
    const Source = struct {
        a: A,
        segment: *local.segment.SegmentReader,
        names: []const []const u8,
        position: usize = 0,
        field: []const u8 = "",
        reader: ?inverted.ScopedInvertedIndexReader = null,
        iterator: ?inverted.ScopedInvertedIndexReader.Iterator = null,
        key: ?[]u8 = null,
        value: [12]u8 = undefined,
        fn close(self: *@This()) void {
            if (self.iterator) |*iterator| iterator.deinit();
            if (self.reader) |*reader| reader.deinit();
            self.iterator = null;
            self.reader = null;
        }
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            while (true) {
                if (self.iterator) |*iterator| if (try iterator.next()) |entry| {
                    if (self.key) |key| self.a.free(key);
                    self.key = null;
                    self.key = try keyAlloc(self.a, self.field, entry.term);
                    var summary: Summary = .{ .frequency = entry.result.docFreq(), .max_frequency = std.math.maxInt(u32), .min_norm = 0 };
                    switch (entry.result) {
                        .one_hit => |hit| {
                            summary.max_frequency = 1;
                            summary.min_norm = hit.norm_bits;
                        },
                        .postings => |postings| if (postings.block_max) |blocks| {
                            if (blocks.frequencyNormAtOrdinal(0) != null) {
                                summary.max_frequency = 0;
                                summary.min_norm = std.math.maxInt(u32);
                                var i: usize = 0;
                                while (blocks.frequencyNormAtOrdinal(i)) |bound| : (i += 1) {
                                    summary.max_frequency = @max(summary.max_frequency, bound.frequency);
                                    summary.min_norm = @min(summary.min_norm, bound.norm);
                                }
                            }
                        },
                    }
                    std.mem.writeInt(u32, self.value[0..4], summary.frequency, .little);
                    std.mem.writeInt(u32, self.value[4..8], summary.max_frequency, .little);
                    std.mem.writeInt(u32, self.value[8..12], summary.min_norm, .little);
                    _ = try Summary.decode(&self.value);
                    return .{ .key = self.key.?, .value = &self.value };
                };
                self.close();
                if (self.position == self.names.len) return null;
                self.field = self.names[self.position];
                self.position += 1;
                self.reader = try self.segment.invertedIndexScoped(self.a, self.field);
                if (self.reader) |*reader| self.iterator = try reader.termIterator();
            }
        }
    };
    var source: Source = .{ .a = a, .segment = &segment, .names = names };
    defer source.close();
    defer if (source.key) |key| a.free(key);
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var reads: u64 = 0;
    var writes: u64 = 128 * 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    return tree.buildSorted(a, pages.store(), &source);
}

/// Private builder: sort memory is bounded and its manager caps temporary disk.
/// Only changed/replaced file summaries enter the delta, never unchanged terms.
pub const Builder = struct {
    a: A,
    sort: local.sql_spill.Sort,
    ordinal: u64 = 0,
    pub fn init(a: A, manager: *local.sql_spill.Manager) Builder {
        return .{ .a = a, .sort = local.sql_spill.Sort.init(a, manager, &.{.{}}, 4 * 1024 * 1024) };
    }
    pub fn deinit(self: *Builder) void {
        self.sort.deinit();
    }
    pub fn add(self: *Builder, store: tree.Store, root: ?Ref, sign: i64) !void {
        if (sign != 1 and sign != -1) return error.InvalidNativeLakeTextStatistics;
        var cursor = try tree.Cursor.init(self.a, store, root, "", null);
        defer cursor.deinit();
        while (try cursor.next()) |record| {
            const summary = try Summary.decode(record.value);
            try self.sort.add(.{ .keys = &.{.{ .value = .{ .string = record.key }, .sql_null = false }}, .values = &.{.{ .value = .{ .integer = sign * @as(i64, summary.frequency) }, .sql_null = false }}, .ordinal = self.ordinal });
            self.ordinal += 1;
        }
    }
    pub fn finish(self: *Builder, store: tree.Store, prior: ?Ref) !?Ref {
        try self.sort.finish();
        const Deltas = struct {
            a: A,
            sort: *local.sql_spill.Sort,
            current: ?local.sql_operators.Row = null,
            scratch: std.heap.ArenaAllocator,
            key: ?[]u8 = null,
            delta: i64 = 0,
            pub fn next(source: *@This()) !?tree.Cursor.Record {
                if (source.key) |key| source.a.free(key);
                source.key = null;
                if (source.current == null) source.current = try source.sort.next(source.scratch.allocator());
                const first = source.current orelse return null;
                source.key = try source.a.dupe(u8, first.keys[0].value.string);
                source.delta = 0;
                while (source.current) |row| {
                    if (!std.mem.eql(u8, source.key.?, row.keys[0].value.string)) break;
                    source.delta = std.math.add(i64, source.delta, row.values[0].value.integer) catch return error.InvalidNativeLakeTextStatistics;
                    _ = source.scratch.reset(.retain_capacity);
                    source.current = try source.sort.next(source.scratch.allocator());
                }
                return .{ .key = source.key.?, .value = "" };
            }
        };
        var deltas: Deltas = .{ .a = self.a, .sort = &self.sort, .scratch = std.heap.ArenaAllocator.init(self.a) };
        defer deltas.scratch.deinit();
        defer if (deltas.key) |key| self.a.free(key);
        if (prior == null) {
            const Initial = struct {
                deltas: *Deltas,
                value: [8]u8 = undefined,
                pub fn next(source: *@This()) !?tree.Cursor.Record {
                    while (try source.deltas.next()) |record| {
                        if (source.deltas.delta < 0) return error.InvalidNativeLakeTextStatistics;
                        if (source.deltas.delta == 0) continue;
                        std.mem.writeInt(u64, &source.value, @intCast(source.deltas.delta), .big);
                        return .{ .key = record.key, .value = &source.value };
                    }
                    return null;
                }
            };
            var initial: Initial = .{ .deltas = &deltas };
            return tree.buildSorted(self.a, store, &initial);
        }
        var cache: tree.Cache = .{ .alloc = self.a, .underlying = store };
        defer cache.deinit();
        var root = prior;
        var batch = std.heap.ArenaAllocator.init(self.a);
        defer batch.deinit();
        var mutations: std.ArrayListUnmanaged(tree.Mutation) = .empty;
        var retained: usize = 0;
        while (try deltas.next()) |record| {
            if (deltas.delta == 0) continue;
            const count = blk: {
                var cursor = try tree.Cursor.init(self.a, cache.store(), prior, record.key, null);
                defer cursor.deinit();
                const found = try cursor.next();
                break :blk if (found) |old| if (std.mem.eql(u8, old.key, record.key)) try decodeFrequency(old.value) else 0 else 0;
            };
            const next = @as(i128, count) + deltas.delta;
            if (next < 0 or next > std.math.maxInt(u64)) return error.InvalidNativeLakeTextStatistics;
            const a = batch.allocator();
            const key = try a.dupe(u8, record.key);
            const value = if (next == 0) null else blk: {
                const bytes = try a.alloc(u8, 8);
                std.mem.writeInt(u64, bytes[0..8], @intCast(next), .big);
                break :blk bytes;
            };
            try mutations.append(a, .{ .key = key, .value = value });
            retained += key.len + 8 + @sizeOf(tree.Mutation);
            if (retained >= 2 * 1024 * 1024 or mutations.items.len >= 4096) {
                root = try tree.apply(self.a, cache.store(), root, mutations.items);
                _ = batch.reset(.free_all);
                mutations = .empty;
                retained = 0;
            }
        }
        return if (mutations.items.len != 0) tree.apply(self.a, cache.store(), root, mutations.items) else root;
    }
};
fn decodeFrequency(bytes: []const u8) !u64 {
    if (bytes.len != 8) return error.InvalidNativeLakeTextStatistics;
    const count = std.mem.readInt(u64, bytes[0..8], .big);
    if (count == 0) return error.InvalidNativeLakeTextStatistics;
    return count;
}

/// Query-owned capability. The corpus owner retains refs only, never this reader.
pub const Reader = struct {
    read: @import("lake_index_seekable_text.zig").Read,
    domain: [32]u8,
    global: ?Ref,
    segments: []const ?Ref,
    pub fn interface(self: *Reader) local.index.IndexSnapshot.TextStatistics {
        return .{ .ptr = self, .check = checkInterface, .frequencies = frequencies, .summary = summary };
    }
    fn checkInterface(raw: *anyopaque) !void {
        return @as(*Reader, @ptrCast(@alignCast(raw))).check();
    }
    fn check(self: *Reader) !void {
        try self.read.context.ensureActive();
        try self.read.cancellation.check();
    }
    fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, _: [32]u8, cancellation: Cancellation, budget: *u64) ![]u8 {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.check();
        if (offset != 0 or length != ref.byte_len) return error.InvalidNativeLakeTextStatistics;
        try stores.chargeReadBudget(budget, length);
        return artifacts.readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, self.read.cache);
    }
    fn lookup(self: *Reader, a: A, cache: tree.Store, root: ?Ref, key: []const u8) !?[]u8 {
        try self.check();
        var cursor = try tree.Cursor.init(a, cache, root, key, null);
        defer cursor.deinit();
        const record = (try cursor.next()) orelse return null;
        if (!std.mem.eql(u8, record.key, key)) return null;
        return try a.dupe(u8, record.value);
    }
    fn frequencies(raw: *anyopaque, a: A, field: []const u8, terms: []const []const u8, output: []u32) !void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.check();
        if (terms.len != output.len) return error.InvalidNativeLakeTextStatistics;
        var store = self.read.store;
        var reads: u64 = 64 * 1024 * 1024;
        var writes: u64 = 0;
        var pages: page_store.PageStore = .{ .domain = self.domain, .artifacts = &store, .cancellation = self.read.cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .read_cache = .{ .ptr = self, .read = readPage } };
        var cache: tree.Cache = .{ .alloc = a, .underlying = pages.store() };
        defer cache.deinit();
        for (terms, output) |term, *count| {
            const key = try keyAlloc(a, field, term);
            defer a.free(key);
            const value = try self.lookup(a, cache.store(), self.global, key);
            defer if (value) |bytes| a.free(bytes);
            count.* = if (value) |bytes| @intCast(@min(std.math.maxInt(u32), try decodeFrequency(bytes))) else 0;
        }
        try self.check();
    }
    fn summary(raw: *anyopaque, a: A, segment: usize, field: []const u8, term: []const u8, average: f32, config: inverted.BM25Config) !local.index.IndexSnapshot.TextTermSummary {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.check();
        if (segment >= self.segments.len) return error.InvalidNativeLakeTextStatistics;
        var store = self.read.store;
        var reads: u64 = 16 * 1024 * 1024;
        var writes: u64 = 0;
        var pages: page_store.PageStore = .{ .domain = self.domain, .artifacts = &store, .cancellation = self.read.cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .read_cache = .{ .ptr = self, .read = readPage } };
        const key = try keyAlloc(a, field, term);
        defer a.free(key);
        const value = try self.lookup(a, pages.store(), self.segments[segment], key);
        defer if (value) |bytes| a.free(bytes);
        const result: local.index.IndexSnapshot.TextTermSummary = if (value) |bytes| (try Summary.decode(bytes)).bound(average, config) else .{ .frequency = 0, .tf_upper = 0 };
        try self.check();
        return result;
    }
};

test "external lake text statistics survive cold reads incremental replacement and authority changes" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-text-statistics");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(3) };
    const Fixture = struct {
        fn segment(alloc: A, term: []const u8, frequency: u32) ![]u8 {
            var writer = local.segment.SegmentWriter.init(alloc);
            defer writer.deinit();
            var text = inverted.InvertedIndexBuilder.init(alloc, .{});
            defer text.deinit();
            for (0..2) |doc| {
                try writer.addStoredDoc(if (doc == 0) "one" else "two", "{}");
                try text.addDocument(@intCast(doc), &.{ .{ .term = term, .freq = frequency, .norm = 100 }, .{ .term = "common", .freq = 1, .norm = 100 } });
            }
            const bytes = try text.build();
            defer alloc.free(bytes);
            try writer.addSection(try writer.addField("body"), .inverted_text, bytes);
            return writer.build();
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const first = try Fixture.segment(a, "alpha", 100000);
    defer a.free(first);
    const second = try Fixture.segment(a, "beta", 2);
    defer a.free(second);
    const replacement = try Fixture.segment(a, "gamma", 4);
    defer a.free(replacement);
    const roots = [_]?Ref{ try publishSegment(a, &store, first, .none), try publishSegment(a, &store, second, .none), try publishSegment(a, &store, replacement, .none) };
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = 64 * 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = @splat(7), .attempt = @splat(3), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Fixture.checkpoint };
    defer manager.deinit();
    var initial = Builder.init(a, &manager);
    defer initial.deinit();
    try initial.add(pages.store(), roots[0], 1);
    try initial.add(pages.store(), roots[1], 1);
    const global = try initial.finish(pages.store(), null);
    var reader: Reader = .{ .read = .{ .store = store, .cache = null, .context = .{}, .cancellation = .none }, .domain = @splat(7), .global = global, .segments = roots[0..2] };
    const Source = struct {
        bytes: []const u8,
        fail: bool = false,
        calls: usize = 0,
        fn source(self: *@This()) local.index.SegmentSource {
            return .{ .ranges = .{ .ptr = self, .length = self.bytes.len, .read_into = read, .close = close, .bind_read_context = bind } };
        }
        fn bind(raw: *anyopaque, _: A, _: *anyopaque) !local.index.SegmentSource {
            return @as(*@This(), @ptrCast(@alignCast(raw))).source();
        }
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.fail) return error.TestDictionaryReadForbidden;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var source: Source = .{ .bytes = first };
    var second_source: Source = .{ .bytes = second };
    var writer = try local.index.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegmentWithIdData(1, .fromNative(source.source()));
    try writer.addSegmentWithIdData(2, .fromNative(second_source.source()));
    const facade = try writer.acquireSnapshotWithReadContext(&reader);
    defer facade.release();
    facade.text_statistics = reader.interface();
    source.fail = true;
    second_source.fail = true;
    const calls = source.calls + second_source.calls;
    var counts: [4]u32 = undefined;
    try facade.termDocFreqs(a, "body", &.{ "alpha", "beta", "absent", "alpha" }, &counts);
    try std.testing.expectEqualSlices(u32, &.{ 2, 2, 0, 2 }, &counts);
    const summary = (try facade.cachedTextTermSummary(a, 0, "body", "alpha", 100, .{})).?;
    try std.testing.expectEqual(@as(u32, 2), summary.frequency);
    try std.testing.expectEqual(inverted.BM25TermScorer.init(100, 1, .{}).maxScore(), summary.tf_upper);
    try std.testing.expectEqual(calls, source.calls + second_source.calls);
    var delta = Builder.init(a, &manager);
    defer delta.deinit();
    try delta.add(pages.store(), roots[0], -1);
    try delta.add(pages.store(), roots[2], 1);
    const updated = try delta.finish(pages.store(), global);
    var reopened: Reader = .{ .read = reader.read, .domain = reader.domain, .global = updated, .segments = &.{ roots[2], roots[1] } };
    try Reader.frequencies(&reopened, a, "body", &.{ "alpha", "beta", "gamma", "common" }, &counts);
    try std.testing.expectEqualSlices(u32, &.{ 0, 2, 2, 4 }, &counts);
    var unchanged = Builder.init(a, &manager);
    defer unchanged.deinit();
    try std.testing.expect((try unchanged.finish(pages.store(), updated)).?.eql(updated.?));
    const Check = struct {
        fn run(alloc: A, value: *Reader) !void {
            var result: [2]u32 = undefined;
            try Reader.frequencies(value, alloc, "body", &.{ "gamma", "common" }, &result);
            try std.testing.expectEqualSlices(u32, &.{ 2, 4 }, &result);
            _ = try Reader.summary(value, alloc, 0, "body", "gamma", 100, .{ .k1 = 2, .b = 0.5 });
        }
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{&reopened});
    var invalid = reopened;
    invalid.global = roots[2]; // An authenticated summary is not a DF value.
    try std.testing.expectError(error.InvalidNativeLakeTextStatistics, Reader.frequencies(&invalid, a, "body", &.{"gamma"}, counts[0..1]));
    invalid = reopened;
    invalid.segments = &.{updated};
    try std.testing.expectError(error.InvalidNativeLakeTextStatistics, Reader.summary(&invalid, a, 0, "body", "gamma", 100, .{}));
    invalid = reopened;
    invalid.global.?.digest[0] ^= 1;
    try std.testing.expectError(error.FileNotFound, Reader.frequencies(&invalid, a, "body", &.{"gamma"}, counts[0..1]));
    const payload = try store.getAlloc(&try page_store.PageStore.identity(reader.domain, updated.?));
    defer a.free(payload);
    var foreign_store = store;
    foreign_store.upload_scope = .{ .domain = @splat(8), .attempt = updated.?.attempt };
    var foreign = try foreign_store.putScoped(foreign_store.upload_scope.?, payload, .none);
    defer foreign.deinit(a);
    invalid = reopened;
    invalid.domain = @splat(8);
    try std.testing.expectError(error.GraphPageDomainMismatch, Reader.frequencies(&invalid, a, "body", &.{"gamma"}, counts[0..1]));
    var canceled = true;
    reader.read.cancellation = .{ .ptr = &canceled, .is_cancelled_fn = Check.canceled };
    try std.testing.expectError(error.Canceled, facade.termDocFreqs(a, "body", &.{"uncached"}, counts[0..1]));
    // Warm scalar caches must still check the current request authority.
    try std.testing.expectError(error.Canceled, facade.termDocFreqs(a, "body", &.{"alpha"}, counts[0..1]));
    try std.testing.expectError(error.Canceled, facade.cachedTextTermSummary(a, 0, "body", "alpha", 100, .{}));
    try std.testing.expectError(error.Canceled, Reader.summary(&reader, a, 0, "body", "alpha", 100, .{}));
}

test "external lake text statistics spill sorted deltas and reuse untouched pages" {
    const a = std.testing.allocator;
    var backing: tree.testing.MemoryStore = .{ .alloc = a };
    defer backing.deinit();
    const Source = struct {
        position: u64 = 0,
        count: u64,
        key: [16]u8 = undefined,
        value: [12]u8 = undefined,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.position == self.count) return null;
            std.mem.writeInt(u32, self.key[0..4], 4, .big);
            @memcpy(self.key[4..8], "body");
            std.mem.writeInt(u64, self.key[8..16], self.position, .big);
            std.mem.writeInt(u32, self.value[0..4], 1, .little);
            std.mem.writeInt(u32, self.value[4..8], 2, .little);
            std.mem.writeInt(u32, self.value[8..12], 10, .little);
            self.position += 1;
            return .{ .key = &self.key, .value = &self.value };
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var source: Source = .{ .count = 8000 };
    const summaries = try tree.buildSorted(a, backing.store(), &source);
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Source.checkpoint };
    defer manager.deinit();
    var initial = Builder.init(a, &manager);
    defer initial.deinit();
    initial.sort.memory_bytes = 64 * 1024;
    initial.sort.parallel_runs = false;
    try initial.add(backing.store(), summaries, 1);
    try initial.add(backing.store(), summaries, 1);
    const global = (try initial.finish(backing.store(), null)).?;
    try std.testing.expect(global.height > 0);
    try std.testing.expect(initial.sort.output_count > 0);
    var one: Source = .{ .count = 1 };
    const removed = try tree.buildSorted(a, backing.store(), &one);
    const written = backing.written_bytes;
    var update = Builder.init(a, &manager);
    defer update.deinit();
    try update.add(backing.store(), removed, -1);
    const changed = (try update.finish(backing.store(), global)).?;
    // A one-term change rewrites its path, not the entire vocabulary.
    try std.testing.expect(backing.written_bytes - written < 64 * 1024);
    try std.testing.expectEqual(global.records, changed.records);
    for ([_]u64{ 0, 1, 7999 }) |ordinal| {
        var key: [16]u8 = undefined;
        std.mem.writeInt(u32, key[0..4], 4, .big);
        @memcpy(key[4..8], "body");
        std.mem.writeInt(u64, key[8..16], ordinal, .big);
        var cursor = try tree.Cursor.init(a, backing.store(), changed, &key, null);
        defer cursor.deinit();
        const record = (try cursor.next()).?;
        try std.testing.expectEqualSlices(u8, &key, record.key);
        try std.testing.expectEqual(@as(u64, if (ordinal == 0) 1 else 2), try decodeFrequency(record.value));
    }
    var remove_again = Builder.init(a, &manager);
    defer remove_again.deinit();
    try remove_again.add(backing.store(), removed, -1);
    const without = (try remove_again.finish(backing.store(), changed)).?;
    try std.testing.expectEqual(@as(u64, 7999), without.records);
    var invalid = Builder.init(a, &manager);
    defer invalid.deinit();
    try invalid.add(backing.store(), removed, -1);
    try std.testing.expectError(error.InvalidNativeLakeTextStatistics, invalid.finish(backing.store(), without));
}
