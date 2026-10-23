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
const directory_mod = @import("lake_index_term_directory.zig");

pub fn keyAlloc(a: A, field: []const u8, term: []const u8) ![]u8 {
    if (field.len > std.math.maxInt(u32) or field.len +| term.len > tree.max_key_bytes - 4) return error.InvalidNativeLakeTextStatistics;
    const key = try a.alloc(u8, 4 + field.len + term.len);
    std.mem.writeInt(u32, key[0..4], @intCast(field.len), .big);
    @memcpy(key[4..][0..field.len], field);
    @memcpy(key[4 + field.len ..], term);
    return key;
}
pub const Summary = struct {
    pub const Pair = struct { frequency: u32, norm: u32 };
    pub const max_pairs = 8;
    pub const max_bytes = 9 + 8 * max_pairs;
    frequency: u32,
    max_frequency: u32,
    min_norm: u32,
    pairs: [max_pairs]Pair = undefined,
    pair_count: u8 = 0,
    /// Dominance is independent of average length and valid BM25 parameters.
    /// At capacity, merging two envelopes widens their coverage conservatively.
    pub fn addPair(self: *Summary, pair: Pair) void {
        if (pair.frequency == 0) return;
        self.max_frequency = @max(self.max_frequency, pair.frequency);
        self.min_norm = @min(self.min_norm, pair.norm);
        var i: usize = 0;
        while (i < self.pair_count) {
            const old = self.pairs[i];
            if (old.frequency >= pair.frequency and old.norm <= pair.norm) return;
            if (pair.frequency >= old.frequency and pair.norm <= old.norm) {
                self.pair_count -= 1;
                self.pairs[i] = self.pairs[self.pair_count];
            } else i += 1;
        }
        if (self.pair_count == max_pairs) {
            const merged: Pair = .{ .frequency = @max(self.pairs[0].frequency, pair.frequency), .norm = @min(self.pairs[0].norm, pair.norm) };
            self.pair_count -= 1;
            self.pairs[0] = self.pairs[self.pair_count];
            self.addPair(merged);
            return;
        }
        self.pairs[self.pair_count] = pair;
        self.pair_count += 1;
    }
    pub fn merge(self: *Summary, other: Summary) void {
        self.frequency = @max(self.frequency, other.frequency);
        if (other.pair_count == 0) self.addPair(.{ .frequency = other.max_frequency, .norm = other.min_norm }) else for (other.pairs[0..other.pair_count]) |pair| self.addPair(pair);
    }
    pub fn encode(self: Summary, buffer: *[max_bytes]u8) []const u8 {
        @memcpy(buffer[0..4], "ATS2");
        std.mem.writeInt(u32, buffer[4..8], self.frequency, .little);
        const count: u8 = @max(1, self.pair_count);
        buffer[8] = count;
        for (0..count) |i| {
            const pair = if (self.pair_count == 0) Pair{ .frequency = self.max_frequency, .norm = self.min_norm } else self.pairs[i];
            std.mem.writeInt(u32, buffer[9 + i * 8 ..][0..4], pair.frequency, .little);
            std.mem.writeInt(u32, buffer[13 + i * 8 ..][0..4], pair.norm, .little);
        }
        return buffer[0 .. 9 + @as(usize, count) * 8];
    }
    pub fn decode(bytes: []const u8) !Summary {
        if (bytes.len == 12) {
            const result: Summary = .{ .frequency = std.mem.readInt(u32, bytes[0..4], .little), .max_frequency = std.mem.readInt(u32, bytes[4..8], .little), .min_norm = std.mem.readInt(u32, bytes[8..12], .little) };
            if (result.frequency == 0 or result.max_frequency == 0) return error.InvalidNativeLakeTextStatistics;
            return result;
        }
        if (bytes.len < 9 or !std.mem.eql(u8, bytes[0..4], "ATS2")) return error.InvalidNativeLakeTextStatistics;
        const count = bytes[8];
        if (count == 0 or count > max_pairs or bytes.len != 9 + @as(usize, count) * 8) return error.InvalidNativeLakeTextStatistics;
        var result: Summary = .{ .frequency = std.mem.readInt(u32, bytes[4..8], .little), .max_frequency = 0, .min_norm = std.math.maxInt(u32) };
        if (result.frequency == 0) return error.InvalidNativeLakeTextStatistics;
        for (0..count) |i| {
            const pair: Pair = .{ .frequency = std.mem.readInt(u32, bytes[9 + i * 8 ..][0..4], .little), .norm = std.mem.readInt(u32, bytes[13 + i * 8 ..][0..4], .little) };
            if (pair.frequency == 0) return error.InvalidNativeLakeTextStatistics;
            result.addPair(pair);
        }
        return result;
    }
    /// An exact f32 ceiling for the common single-occurrence population.
    /// Quantized maxima of one prove every posting has frequency one. Norm
    /// minima remain conservative for both canonical scoring arithmetic paths.
    pub fn fixedFrequencyScore(self: Summary, average: f32, config: inverted.BM25Config, idf: f32, legacy: bool) ?f32 {
        if (!(average > 0) or !std.math.isFinite(average) or !std.math.isFinite(idf) or idf < 0 or
            !std.math.isFinite(config.k1) or config.k1 < 0 or !std.math.isFinite(config.b) or config.b < 0 or config.b > 1 or self.max_frequency != 1) return null;
        const value = if (legacy) inverted.bm25ScoreWithIdf(1, self.min_norm, average, idf, config) else inverted.BM25TermScorer.init(average, idf, config).score(1, self.min_norm);
        return if (std.math.isFinite(value)) value else null;
    }
    /// Evaluate the bounded integer frequency domain, rather than assuming
    /// f32 arithmetic is monotone in frequency. For each fixed numerator the
    /// score is monotone in norm, so the authenticated norm minimum suffices.
    /// Large/escaped domains retain the ordinary conservative bound.
    pub fn exactScoreCeiling(self: Summary, average: f32, config: inverted.BM25Config, idf: f32, legacy: bool) ?f32 {
        if (!(average > 0) or !std.math.isFinite(average) or !std.math.isFinite(idf) or idf < 0 or
            !std.math.isFinite(config.k1) or config.k1 < 0 or !std.math.isFinite(config.b) or config.b < 0 or config.b > 1 or self.max_frequency > 256) return null;
        const scorer = inverted.BM25TermScorer.init(average, idf, config);
        var upper: f32 = 0;
        const count = @max(1, self.pair_count);
        for (0..count) |i| {
            const pair = if (self.pair_count == 0) Pair{ .frequency = self.max_frequency, .norm = self.min_norm } else self.pairs[i];
            for (1..@as(usize, pair.frequency) + 1) |frequency| {
                const value = if (legacy) inverted.bm25ScoreWithIdf(@intCast(frequency), pair.norm, average, idf, config) else scorer.score(@intCast(frequency), pair.norm);
                if (!std.math.isFinite(value)) return null;
                upper = @max(upper, value);
            }
        }
        return upper;
    }
    pub fn bound(self: Summary, average: f32, config: inverted.BM25Config) local.index.IndexSnapshot.TextTermSummary {
        const valid = std.math.isFinite(config.k1) and config.k1 >= 0 and std.math.isFinite(config.b) and config.b >= 0 and config.b <= 1 and std.math.isFinite(average) and average > 0;
        var upper: f32 = 0;
        const count = @max(1, self.pair_count);
        for (0..count) |i| {
            const pair = if (self.pair_count == 0) Pair{ .frequency = self.max_frequency, .norm = self.min_norm } else self.pairs[i];
            const bound_value = if (!valid) std.math.inf(f32) else if (pair.frequency == std.math.maxInt(u32)) inverted.BM25TermScorer.init(average, 1, config).maxScore() else inverted.bm25ScoreWithIdf(pair.frequency, pair.norm, average, 1, config);
            upper = @max(upper, bound_value);
        }
        return .{ .frequency = self.frequency, .tf_upper = upper };
    }
};

/// Native writers already emit terms in order within each field. Retain one
/// compact field directory and append private binary frames, then replay fields
/// in publication order. No per-term JSON, sort, or second dictionary pass.
pub const Captured = struct {
    const Field = struct { name: []u8, begin: u64, end: u64 };
    const header_bytes = 10;
    a: A,
    manager: *local.sql_spill.Manager,
    phase: ?*const stores.IoPhase = null,
    file: ?local.sql_spill.File = null,
    buffer: [16 * 1024]u8 = undefined,
    used: usize = 0,
    previous: std.ArrayList(u8) = .empty,
    restart: u8 = 0,
    fields: std.ArrayList(Field) = .empty,
    term: ?[]const u8 = null,
    count: u64 = 0,
    sealed: bool = false,
    disabled: bool = false,
    pub fn create(a: A, manager: *local.sql_spill.Manager) !*Captured {
        const self = try a.create(Captured);
        self.* = .{ .a = a, .manager = manager };
        return self;
    }
    pub fn deinit(self: *Captured) void {
        if (self.file) |*file| file.close();
        self.previous.deinit(self.a);
        for (self.fields.items) |field| self.a.free(field.name);
        self.fields.deinit(self.a);
        self.a.destroy(self);
    }
    fn io(self: *Captured) !stores.IoGuard {
        if (self.phase) |phase| try phase.begin(phase.ptr, 1);
        return .{ .phase = self.phase };
    }
    pub fn observer(self: *Captured) inverted.TermObserver {
        return .{ .ptr = self, .begin = begin, .finish = finish };
    }
    fn discard(self: *Captured, err: anyerror) !void {
        switch (err) {
            error.OutOfMemory, error.SqlProgramLimitExceeded => {
                if (self.file) |*file| file.close();
                self.file = null;
                self.used = 0;
                self.previous.deinit(self.a);
                self.previous = .empty;
                self.term = null;
                for (self.fields.items) |field| self.a.free(field.name);
                self.fields.deinit(self.a);
                self.fields = .empty;
                self.disabled = true;
            },
            else => return err,
        }
    }
    fn begin(raw: *anyopaque, field: []const u8, term: []const u8) !void {
        const self: *Captured = @ptrCast(@alignCast(raw));
        if (self.disabled) return;
        self.beginTerm(field, term) catch |err| try self.discard(err);
    }
    fn beginTerm(self: *Captured, field: []const u8, term: []const u8) !void {
        if (self.sealed or self.term != null or field.len +| term.len > tree.max_key_bytes - 4) return error.InvalidNativeLakeTextStatistics;
        const offset = self.length();
        if (self.fields.items.len == 0 or !std.mem.eql(u8, self.fields.items[self.fields.items.len - 1].name, field)) {
            const name = try self.a.dupe(u8, field);
            errdefer self.a.free(name);
            try self.fields.append(self.a, .{ .name = name, .begin = offset, .end = offset });
            self.previous.clearRetainingCapacity();
            self.restart = 0;
        }
        // The observer is synchronous: the writer retains this term until
        // finish returns. Only the per-field directory needs owned names.
        self.term = term;
    }
    fn finish(raw: *anyopaque, frequency: u32, bounds: ?inverted.BlockMaxInfo, norm: ?u32) !void {
        const self: *Captured = @ptrCast(@alignCast(raw));
        if (self.disabled) return;
        self.finishTerm(frequency, bounds, norm) catch |err| try self.discard(err);
    }
    fn finishTerm(self: *Captured, frequency: u32, bounds: ?inverted.BlockMaxInfo, norm: ?u32) !void {
        const term = self.term orelse return error.InvalidNativeLakeTextStatistics;
        if (frequency == 0) {
            self.term = null;
            return;
        }
        var summary: Summary = .{ .frequency = frequency, .max_frequency = std.math.maxInt(u32), .min_norm = 0 };
        if (norm) |value| {
            summary.max_frequency = 0;
            summary.min_norm = std.math.maxInt(u32);
            summary.addPair(.{ .frequency = 1, .norm = value });
        } else if (bounds) |blocks| {
            if (blocks.frequencyNormAtOrdinal(0) != null) {
                summary.max_frequency = 0;
                summary.min_norm = std.math.maxInt(u32);
                var i: usize = 0;
                while (blocks.frequencyNormAtOrdinal(i)) |pair| : (i += 1) summary.addPair(.{ .frequency = pair.frequency, .norm = pair.norm });
            }
        }
        var buffer: [Summary.max_bytes]u8 = undefined;
        const encoded = summary.encode(&buffer);
        var header: [header_bytes]u8 = undefined;
        // Restart every 64 terms and at each field boundary. The frame remains
        // sequential and private; durable summary keys/values do not change.
        var prefix: usize = 0;
        if (self.restart != 0) while (prefix < @min(self.previous.items.len, term.len) and self.previous.items[prefix] == term[prefix]) : (prefix += 1) {};
        std.mem.writeInt(u32, header[0..4], @intCast(prefix), .little);
        std.mem.writeInt(u32, header[4..8], @intCast(term.len - prefix), .little);
        std.mem.writeInt(u16, header[8..10], @intCast(encoded.len), .little);
        try self.appendPart(&header);
        try self.appendPart(term[prefix..]);
        try self.appendPart(encoded);
        try self.previous.resize(self.a, term.len);
        @memcpy(self.previous.items, term);
        self.restart = (self.restart + 1) % 64;
        self.fields.items[self.fields.items.len - 1].end = self.length();
        self.count += 1;
        self.term = null;
    }
    fn length(self: *Captured) u64 {
        return (if (self.file) |file| file.size else @as(u64, 0)) + self.used;
    }
    fn write(self: *Captured, bytes: []const u8) !void {
        var guard = try self.io();
        defer guard.abort();
        self.writeRaw(bytes) catch |err| return guard.failed(err);
        try guard.finish();
    }
    fn writeRaw(self: *Captured, bytes: []const u8) !void {
        if (self.file == null) self.file = try self.manager.create();
        const file = &self.file.?;
        try file.writeRaw(file.size, bytes);
    }
    fn flush(self: *Captured) !void {
        if (self.used == 0) return;
        try self.write(self.buffer[0..self.used]);
        self.used = 0;
    }
    fn appendPart(self: *Captured, bytes: []const u8) !void {
        if (bytes.len > self.buffer.len - self.used) try self.flush();
        if (bytes.len >= self.buffer.len) return self.write(bytes);
        @memcpy(self.buffer[self.used..][0..bytes.len], bytes);
        self.used += bytes.len;
    }
    pub fn seal(self: *Captured) !void {
        if (self.sealed or self.disabled) return;
        if (self.term != null) return error.InvalidNativeLakeTextStatistics;
        self.flush() catch |err| return self.discard(err);
        if (self.file) |*file| {
            var guard = try self.io();
            defer guard.abort();
            file.seal() catch |err| return guard.failed(err);
            try guard.finish();
        }
        std.mem.sort(Field, self.fields.items, {}, struct {
            fn less(_: void, left: Field, right: Field) bool {
                return left.name.len < right.name.len or (left.name.len == right.name.len and std.mem.order(u8, left.name, right.name) == .lt);
            }
        }.less);
        self.sealed = true;
    }
    fn read(self: *Captured, offset: u64, bytes: []u8) !void {
        var guard = try self.io();
        defer guard.abort();
        self.file.?.readRaw(offset, bytes) catch |err| return guard.failed(err);
        try guard.finish();
    }
    pub fn publish(self: *Captured, store: *stores.ArtifactStore, cancellation: Cancellation, executor: ?std.Io) !?Ref {
        return self.publishConsuming(store, cancellation, executor, NoContributions{});
    }
    /// Synchronous borrowed records are copied by bounded reducers before the
    /// summary is released. Checkpoints still refer only to durable trees.
    pub fn publishConsuming(self: *Captured, store: *stores.ArtifactStore, cancellation: Cancellation, executor: ?std.Io, consumer: anytype) !?Ref {
        try self.seal();
        if (self.disabled) return error.InvalidNativeLakeTextStatistics;
        const Source = struct {
            capture: *Captured,
            consumer: @TypeOf(consumer),
            field: usize = 0,
            offset: ?u64 = null,
            key: ?[]u8 = null,
            value: [Summary.max_bytes]u8 = undefined,
            buffer: [64 * 1024]u8 = undefined,
            buffer_start: u64 = 0,
            buffer_len: usize = 0,
            fn read(self_: *@This(), offset: u64, bytes: []u8) !void {
                var done: usize = 0;
                while (done < bytes.len) {
                    const position = offset + done;
                    if (position < self_.buffer_start or position - self_.buffer_start >= self_.buffer_len) {
                        self_.buffer_start = position;
                        self_.buffer_len = @intCast(@min(self_.buffer.len, self_.capture.file.?.size - position));
                        if (self_.buffer_len == 0) return error.InvalidNativeLakeTextStatistics;
                        try self_.capture.read(position, self_.buffer[0..self_.buffer_len]);
                    }
                    const within: usize = @intCast(position - self_.buffer_start);
                    const n = @min(bytes.len - done, self_.buffer_len - within);
                    @memcpy(bytes[done..][0..n], self_.buffer[within..][0..n]);
                    done += n;
                }
            }
            pub fn next(self_: *@This()) !?tree.Cursor.Record {
                const captured = self_.capture;
                const previous = self_.key;
                defer if (previous) |key| captured.a.free(key);
                self_.key = null;
                while (self_.field < captured.fields.items.len) {
                    const field = captured.fields.items[self_.field];
                    const offset = self_.offset orelse field.begin;
                    if (offset == field.end) {
                        self_.field += 1;
                        self_.offset = null;
                        continue;
                    }
                    if (offset > field.end or field.end - offset < header_bytes) return error.InvalidNativeLakeTextStatistics;
                    var header: [header_bytes]u8 = undefined;
                    try self_.read(offset, &header);
                    const prefix = std.mem.readInt(u32, header[0..4], .little);
                    const suffix = std.mem.readInt(u32, header[4..8], .little);
                    const term_len = try std.math.add(usize, prefix, suffix);
                    const value_len = std.mem.readInt(u16, header[8..10], .little);
                    if (value_len > self_.value.len or @as(u64, suffix) + value_len > field.end - offset - header_bytes or field.name.len +| term_len > tree.max_key_bytes - 4) return error.InvalidNativeLakeTextStatistics;
                    if (prefix != 0 and (offset == field.begin or previous == null or previous.?.len < 4 + field.name.len + prefix)) return error.InvalidNativeLakeTextStatistics;
                    const key = try captured.a.alloc(u8, 4 + field.name.len + term_len);
                    self_.key = key;
                    std.mem.writeInt(u32, key[0..4], @intCast(field.name.len), .big);
                    @memcpy(key[4..][0..field.name.len], field.name);
                    if (prefix != 0) @memcpy(key[4 + field.name.len ..][0..prefix], previous.?[4 + field.name.len ..][0..prefix]);
                    try self_.read(offset + header_bytes, key[4 + field.name.len + prefix ..]);
                    try self_.read(offset + header_bytes + suffix, self_.value[0..value_len]);
                    _ = try Summary.decode(self_.value[0..value_len]);
                    self_.offset = offset + header_bytes + suffix + value_len;
                    const record: tree.Cursor.Record = .{ .key = key, .value = self_.value[0..value_len] };
                    try self_.consumer.record(record);
                    return record;
                }
                return null;
            }
        };
        var source: Source = .{ .capture = self, .consumer = consumer };
        defer if (source.key) |key| self.a.free(key);
        const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
        var reads: u64 = 0;
        var writes: u64 = 128 * 1024 * 1024;
        var pages: page_store.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .io = executor };
        pages.admitPublication();
        return tree.buildSortedWithOptions(self.a, pages.store(), &source, .{ .target_bytes = tree.bulk_target_page_bytes });
    }
};

pub fn publishSegment(a: A, store: *stores.ArtifactStore, bytes: []const u8, cancellation: Cancellation) !?Ref {
    return publishSegmentWithIo(a, store, bytes, cancellation, null);
}

pub fn publishSegmentWithIo(a: A, store: *stores.ArtifactStore, bytes: []const u8, cancellation: Cancellation, io: ?std.Io) !?Ref {
    var segment = try local.segment.SegmentReader.init(a, bytes);
    defer segment.deinit();
    return publishReaderWithIo(a, store, &segment, cancellation, io);
}
pub const NoContributions = struct {
    pub fn record(_: @This(), _: tree.Cursor.Record) !void {}
};
pub const Contributions = struct {
    global: ?*Builder = null,
    directory: ?*@import("lake_index_term_directory.zig").Builder = null,
    segment: [32]u8,
    pub fn record(self: @This(), value: tree.Cursor.Record) !void {
        if (self.global) |builder| try builder.addRecord(value, 1);
        if (self.directory) |builder| try builder.addRecord(value, self.segment, 1);
    }
};
pub fn publishReaderWithIo(a: A, store: *stores.ArtifactStore, segment: *local.segment.SegmentReader, cancellation: Cancellation, io: ?std.Io) !?Ref {
    return publishReaderConsuming(a, store, segment, cancellation, io, NoContributions{});
}
/// The immutable source remains caller-owned, including file-backed outputs.
pub fn publishReaderConsuming(a: A, store: *stores.ArtifactStore, segment: *local.segment.SegmentReader, cancellation: Cancellation, io: ?std.Io, consumer: anytype) !?Ref {
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
        consumer: @TypeOf(consumer),
        names: []const []const u8,
        position: usize = 0,
        field: []const u8 = "",
        reader: ?inverted.ScopedInvertedIndexReader = null,
        iterator: ?inverted.ScopedInvertedIndexReader.Iterator = null,
        key: ?[]u8 = null,
        value: [Summary.max_bytes]u8 = undefined,
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
                            summary.max_frequency = 0;
                            summary.min_norm = std.math.maxInt(u32);
                            summary.addPair(.{ .frequency = 1, .norm = hit.norm_bits });
                        },
                        .postings => |postings| if (postings.block_max) |blocks| {
                            if (blocks.frequencyNormAtOrdinal(0) != null) {
                                summary.max_frequency = 0;
                                summary.min_norm = std.math.maxInt(u32);
                                var i: usize = 0;
                                while (blocks.frequencyNormAtOrdinal(i)) |bound| : (i += 1) {
                                    summary.addPair(.{ .frequency = bound.frequency, .norm = bound.norm });
                                }
                            }
                        },
                    }
                    const value = summary.encode(&self.value);
                    _ = try Summary.decode(value);
                    const record: tree.Cursor.Record = .{ .key = self.key.?, .value = value };
                    try self.consumer.record(record);
                    return record;
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
    var source: Source = .{ .a = a, .segment = segment, .names = names, .consumer = consumer };
    defer source.close();
    defer if (source.key) |key| a.free(key);
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var reads: u64 = 0;
    var writes: u64 = 128 * 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .io = io };
    pages.admitPublication();
    return tree.buildSortedWithOptions(a, pages.store(), &source, .{ .target_bytes = tree.bulk_target_page_bytes });
}

/// Private builder: sort memory is bounded and its manager caps temporary disk.
/// Only changed/replaced file summaries enter the delta, never unchanged terms.
pub const Builder = struct {
    pub const Strategy = enum { auto, cow, merge };
    a: A,
    sort: local.sql_spill.Sort,
    ordinal: u64 = 0,
    strategy: Strategy = .auto,
    selected: Strategy = .auto,
    point_seeks: u64 = 0,
    distinct_changes: u64 = 0,
    durable: ?@import("lake_index_delta_runs.zig").Forest = null,
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
        while (try cursor.next()) |record| try self.addRecord(record, sign);
    }
    pub fn addRecord(self: *Builder, record: tree.Cursor.Record, sign: i64) !void {
        if (sign != 1 and sign != -1) return error.InvalidNativeLakeTextStatistics;
        const summary = try Summary.decode(record.value);
        try self.sort.add(.{ .keys = &.{.{ .value = .{ .string = record.key }, .sql_null = false }}, .values = &.{.{ .value = .{ .integer = sign * @as(i64, summary.frequency) }, .sql_null = false }}, .ordinal = self.ordinal });
        self.ordinal += 1;
        if (self.durable != null and (self.sort.total >= 16384 or self.sort.manager.live_bytes >= 32 * 1024 * 1024)) try self.flush();
    }
    const Deltas = struct {
        a: A,
        sort: *local.sql_spill.Sort,
        cursor: ?*tree.Cursor = null,
        current: ?local.sql_operators.Row = null,
        scratch: std.heap.ArenaAllocator,
        key: ?[]u8 = null,
        delta: i64 = 0,
        pub fn next(source: *@This()) !?tree.Cursor.Record {
            if (source.cursor) |cursor| {
                const record = try cursor.next() orelse return null;
                if (record.value.len != 8) return error.InvalidNativeLakeTextStatistics;
                source.delta = std.mem.readInt(i64, record.value[0..8], .big);
                return .{ .key = record.key, .value = "" };
            }
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
    /// Spill only one bounded batch locally, then retain immutable remote runs.
    pub fn enableDurable(self: *Builder, store: tree.Store) void {
        self.durable = .{ .a = self.a, .store = store, .policy = .sum };
        self.sort.shared_runs = true;
    }
    pub fn flush(self: *Builder) !void {
        if (self.sort.total == 0) return;
        const forest = if (self.durable) |*value| value else return error.InvalidNativeLakeDeltaRun;
        try self.sort.finish();
        var deltas: Deltas = .{ .a = self.a, .sort = &self.sort, .scratch = .init(self.a) };
        defer deltas.scratch.deinit();
        defer if (deltas.key) |key| self.a.free(key);
        const Source = struct {
            deltas: *Deltas,
            value: [8]u8 = undefined,
            pub fn next(source: *@This()) !?tree.Cursor.Record {
                while (try source.deltas.next()) |record| {
                    if (source.deltas.delta == 0) continue;
                    std.mem.writeInt(i64, &source.value, source.deltas.delta, .big);
                    return .{ .key = record.key, .value = &source.value };
                }
                return null;
            }
        };
        var source: Source = .{ .deltas = &deltas };
        try forest.add(&source);
        const manager = self.sort.manager;
        const memory = self.sort.memory_bytes;
        self.sort.deinit();
        self.sort = local.sql_spill.Sort.init(self.a, manager, &.{.{}}, memory);
        self.sort.shared_runs = true;
    }
    pub fn finish(self: *Builder, store: tree.Store, prior: ?Ref) !?Ref {
        var delta_cursor: ?tree.Cursor = null;
        defer if (delta_cursor) |*value| value.deinit();
        var run: ?tree.Ref = null;
        if (self.durable) |*forest| {
            try self.flush();
            run = try forest.finish();
            delta_cursor = try tree.Cursor.init(self.a, store, run, "", null);
        } else try self.sort.finish();
        var deltas: Deltas = .{ .a = self.a, .sort = &self.sort, .cursor = if (delta_cursor) |*value| value else null, .scratch = std.heap.ArenaAllocator.init(self.a) };
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
            return tree.buildSortedWithOptions(self.a, store, &initial, .{ .target_bytes = tree.bulk_target_page_bytes });
        }
        // Count actual nonzero keys in a bounded replay of the sorted deltas.
        // Repeated contributions and cancellations never force a vocabulary rewrite.
        while (try deltas.next()) |_| if (deltas.delta != 0) {
            self.distinct_changes += 1;
        };
        if (self.distinct_changes == 0) return prior;
        deltas.current = null;
        _ = deltas.scratch.reset(.free_all);
        if (delta_cursor) |*value| {
            value.deinit();
            value.* = try tree.Cursor.init(self.a, store, run, "", null);
        } else try self.sort.rewind();
        var pending = try deltas.next();
        self.selected = if (self.strategy == .auto) (if (self.distinct_changes >= @max(4096, prior.?.records / 4)) .merge else .cow) else self.strategy;
        if (self.selected == .merge) {
            const Merge = struct {
                old: *tree.Cursor,
                deltas: *Deltas,
                left: ?tree.Cursor.Record = null,
                right: ?tree.Cursor.Record,
                advance_left: bool = true,
                advance_right: bool = false,
                value: [8]u8 = undefined,
                pub fn next(source: *@This()) !?tree.Cursor.Record {
                    while (true) {
                        if (source.advance_left) source.left = try source.old.next();
                        if (source.advance_right) source.right = try source.deltas.next();
                        source.advance_left = false;
                        source.advance_right = false;
                        if (source.left == null and source.right == null) return null;
                        const order: std.math.Order = if (source.left == null) .gt else if (source.right == null) .lt else std.mem.order(u8, source.left.?.key, source.right.?.key);
                        const key = if (order == .lt) source.left.?.key else source.right.?.key;
                        const count: i128 = (if (order != .gt) @as(i128, try decodeFrequency(source.left.?.value)) else 0) + (if (order != .lt) @as(i128, source.deltas.delta) else 0);
                        source.advance_left = order != .gt;
                        source.advance_right = order != .lt;
                        if (count < 0 or count > std.math.maxInt(u64)) return error.InvalidNativeLakeTextStatistics;
                        if (count == 0) continue;
                        std.mem.writeInt(u64, &source.value, @intCast(count), .big);
                        return .{ .key = key, .value = &source.value };
                    }
                }
            };
            var cursor = try tree.Cursor.init(self.a, store, prior, "", null);
            defer cursor.deinit();
            var merged: Merge = .{ .old = &cursor, .deltas = &deltas, .right = pending };
            return tree.buildSorted(self.a, store, &merged);
        }
        var cache: tree.Cache = .{ .alloc = self.a, .underlying = store };
        defer cache.deinit();
        var root = prior;
        var batch = std.heap.ArenaAllocator.init(self.a);
        defer batch.deinit();
        var mutations: std.ArrayListUnmanaged(tree.Mutation) = .empty;
        var retained: usize = 0;
        while (pending) |record| : (pending = try deltas.next()) {
            if (deltas.delta == 0) continue;
            self.point_seeks += 1;
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

pub const IdentityOrdinal = struct { identity: [32]u8, segment_idx: usize, doc_offset: u32 };
pub fn identityLess(_: void, left: IdentityOrdinal, right: IdentityOrdinal) bool {
    return std.mem.order(u8, &left.identity, &right.identity) == .lt;
}

/// Query-owned capability. The corpus owner retains refs only, never this reader.
pub const Reader = struct {
    partial_inventory: bool = false,
    read: @import("lake_index_seekable_text.zig").Read,
    domain: [32]u8,
    global: ?Ref,
    segments: []const ?Ref,
    allocator: A = std.heap.page_allocator,
    directory: directory_mod.Roots = .{},
    identities: []const [32]u8 = &.{},
    ordinals: []const IdentityOrdinal = &.{},
    routing: []const local.index.IndexSnapshot.TextCandidate = &.{},
    range_reads: ?*RangeReads = null,
    range_reductions: u64 = 0,
    candidate_bytes: usize = 0,
    plans: std.StringHashMapUnmanaged([]const directory_mod.Branch) = .empty,
    plan_bytes: usize = 0,
    plan_limit: usize = 4 * 1024 * 1024,
    planning_branch_reads: u64 = 0,
    const RangeReads = struct {
        store: stores.ArtifactStore,
        reads: u64 = 64 * 1024 * 1024,
        writes: u64 = 0,
        pages: page_store.PageStore = undefined,
        cache: tree.Cache = undefined,
    };
    pub fn deinit(self: *Reader) void {
        if (self.range_reads) |reads| {
            reads.cache.deinit();
            self.allocator.destroy(reads);
        }
        var it = self.plans.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.plans.deinit(self.allocator);
    }
    pub fn interface(self: *Reader) local.index.IndexSnapshot.TextStatistics {
        return .{ .ptr = self, .check = checkInterface, .frequencies = frequencies, .summary = summary, .prepare = prepare, .planning = planning, .summaries = summaries, .hierarchical = self.directory.leaves != null, .candidates = candidates, .routing = self.routing, .range_summary = if (self.directory.version == 2 and self.directory.leaves != null and self.routing.len == self.segments.len and self.ordinals.len == self.segments.len) rangeSummary else null };
    }
    fn checkInterface(raw: *anyopaque) !void {
        return @as(*Reader, @ptrCast(@alignCast(raw))).check();
    }
    fn check(self: *Reader) !void {
        try self.read.context.ensureActive();
        try self.read.cancellation.check();
    }
    fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, digest: [32]u8, cancellation: Cancellation, budget: *u64) ![]u8 {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.check();
        try stores.chargeReadBudget(budget, length);
        return artifacts.readPageRange(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, offset, length, digest, cancellation, self.read.cache);
    }
    fn lookup(self: *Reader, a: A, cache: tree.Store, root: ?Ref, key: []const u8) !?[]u8 {
        try self.check();
        var cursor = try tree.Cursor.init(a, cache, root, key, null);
        defer cursor.deinit();
        const record = (try cursor.next()) orelse return null;
        if (!std.mem.eql(u8, record.key, key)) return null;
        return try a.dupe(u8, record.value);
    }
    /// Serial request preparation seals immutable raw branch envelopes before
    /// metadata/scoring lanes start. Cache size is independent of corpus size.
    fn prepare(raw: *anyopaque, _: A, field: []const u8, terms: []const []const u8) !void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.check();
        if (self.directory.branches == null) return;
        const a = self.allocator;
        if (self.identities.len != self.segments.len) return error.InvalidNativeLakeTermDirectory;
        var store = self.read.store;
        var reads: u64 = 64 * 1024 * 1024;
        var writes: u64 = 0;
        var pages: page_store.PageStore = .{ .domain = self.domain, .artifacts = &store, .cancellation = self.read.cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .read_cache = .{ .ptr = self, .read = readPage } };
        var cache: tree.Cache = .{ .alloc = a, .underlying = pages.store() };
        defer cache.deinit();
        for (terms) |term| {
            const key = try keyAlloc(a, field, term);
            defer a.free(key);
            if (self.plans.contains(key)) continue;
            if (key.len + @sizeOf(@TypeOf(self.plans).KV) > self.plan_limit -| self.plan_bytes) continue;
            const prefix = directory_mod.prefixAlloc(a, key) catch |err| {
                if (err == error.NativeLakeTermDirectoryKeyTooLarge) continue;
                return err;
            };
            defer a.free(prefix);
            const end = try a.dupe(u8, prefix);
            defer a.free(end);
            end[end.len - 1] = 1;
            var cursor = tree.Cursor.init(a, cache.store(), self.directory.branches, prefix, end) catch |err| {
                if (err == error.ArtifactReadBudgetExceeded) return;
                return err;
            };
            defer cursor.deinit();
            var branches: [256]directory_mod.Branch = undefined;
            var count: usize = 0;
            while (cursor.next() catch |err| {
                // Optional preparation never installs a partial term inventory.
                if (err == error.ArtifactReadBudgetExceeded) return;
                return err;
            }) |record| {
                if (record.key.len != prefix.len + 1 or !std.mem.startsWith(u8, record.key, prefix) or count == branches.len) return error.InvalidNativeLakeTermDirectory;
                const branch = if (self.directory.version == 2) try directory_mod.Branch.decode(record.value) else try directory_mod.Branch.decodeLegacy(record.value);
                if (branch.first[0] != branch.last[0] or branch.first[0] != record.key[prefix.len]) return error.InvalidNativeLakeTermDirectory;
                branches[count] = branch;
                count += 1;
                self.planning_branch_reads += 1;
            }
            const retained = key.len + count * @sizeOf(directory_mod.Branch) + @sizeOf(@TypeOf(self.plans).KV);
            if (retained > self.plan_limit -| self.plan_bytes) continue;
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            const owned = try self.allocator.dupe(directory_mod.Branch, branches[0..count]);
            errdefer self.allocator.free(owned);
            try self.plans.put(self.allocator, owned_key, owned);
            self.plan_bytes += retained;
        }
        try self.check();
    }
    /// Small term inventories enumerate exact immutable identities. Broad terms
    /// remain unknown, never truncated memberships. Ordinals/offsets are built
    /// once by the immutable corpus owner and carry no request capabilities.
    fn candidates(raw: *anyopaque, a: A, field: []const u8, term: []const u8) !?[]const local.index.IndexSnapshot.TextCandidate {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.check();
        if (self.directory.version != 2 or self.directory.leaves == null or self.ordinals.len != self.segments.len) return null;
        const key = try keyAlloc(a, field, term);
        defer a.free(key);
        const branches = self.plans.get(key) orelse return null;
        var count: u64 = 0;
        for (branches) |branch| count +|= branch.count;
        const Candidate = local.index.IndexSnapshot.TextCandidate;
        if (count > 4096 or count * @sizeOf(Candidate) > self.plan_limit -| self.candidate_bytes) return null;
        if (count == 0) return &.{};
        const prefix = directory_mod.prefixAlloc(a, key) catch |err| switch (err) {
            error.NativeLakeTermDirectoryKeyTooLarge => return null,
            else => return err,
        };
        defer a.free(prefix);
        const end = try a.dupe(u8, prefix);
        defer a.free(end);
        end[end.len - 1] = 1;
        var store = self.read.store;
        var reads: u64 = 64 * 1024 * 1024;
        var writes: u64 = 0;
        var pages: page_store.PageStore = .{ .domain = self.domain, .artifacts = &store, .cancellation = self.read.cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .read_cache = .{ .ptr = self, .read = readPage } };
        var cursor = tree.Cursor.init(a, pages.store(), self.directory.leaves, prefix, end) catch |err| switch (err) {
            error.ArtifactReadBudgetExceeded => return null,
            else => return err,
        };
        defer cursor.deinit();
        const result = try a.alloc(Candidate, @intCast(count));
        errdefer a.free(result);
        var position: usize = 0;
        var visited: usize = 0;
        while (cursor.next() catch |err| switch (err) {
            error.ArtifactReadBudgetExceeded => {
                a.free(result);
                return null;
            },
            else => return err,
        }) |record| {
            if (visited == result.len or record.key.len != prefix.len + 32 or !std.mem.startsWith(u8, record.key, prefix)) return error.InvalidNativeLakeTermDirectory;
            visited += 1;
            _ = try Summary.decode(record.value);
            const id = record.key[prefix.len..][0..32];
            var lo: usize = 0;
            var hi = self.ordinals.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (std.mem.order(u8, &self.ordinals[mid].identity, id) == .lt) lo = mid + 1 else hi = mid;
            }
            if (lo == self.ordinals.len or !std.mem.eql(u8, &self.ordinals[lo].identity, id)) {
                if (self.partial_inventory) continue;
                return error.InvalidNativeLakeTermDirectory;
            }
            result[position] = .{ .segment_idx = self.ordinals[lo].segment_idx, .doc_offset = self.ordinals[lo].doc_offset };
            position += 1;
        }
        if (visited != result.len) return error.InvalidNativeLakeTermDirectory;
        std.mem.sort(Candidate, result[0..position], {}, struct {
            fn less(_: void, left: Candidate, right: Candidate) bool {
                return left.segment_idx < right.segment_idx;
            }
        }.less);
        self.candidate_bytes += result.len * @sizeOf(Candidate);
        try self.check();
        return try a.realloc(result, position);
    }
    fn planning(raw: *anyopaque, a: A, segment: usize, field: []const u8, term: []const u8, average: f32, config: inverted.BM25Config) !?local.index.IndexSnapshot.TextTermSummary {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.check();
        if (segment >= self.segments.len) return error.InvalidNativeLakeTextStatistics;
        const key = try keyAlloc(a, field, term);
        defer a.free(key);
        const branches = self.plans.get(key) orelse return null;
        const id = self.identities[segment];
        // At most one branch per hash bucket, ordered by the page-tree key.
        var lo: usize = 0;
        var hi = branches.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (branches[mid].first[0] < id[0]) lo = mid + 1 else hi = mid;
        }
        if (lo == branches.len or branches[lo].first[0] != id[0] or std.mem.order(u8, &id, &branches[lo].first) == .lt or std.mem.order(u8, &id, &branches[lo].last) == .gt) return .{ .frequency = 0, .tf_upper = 0 };
        var result = branches[lo].envelope.bound(average, config);
        // This is a superset, not an exact segment DF. A zero distributed hint
        // uses DF=1 for a conservative IDF; scoring binds the true native DF.
        result.frequency = 1;
        return result;
    }
    /// Fold authenticated subtree envelopes over a contiguous immutable-ID
    /// interval. Interior reductions avoid enumerating broad term memberships.
    /// The coordinator alone uses this bounded, query-owned routing cache.
    fn rangeSummary(raw: *anyopaque, a: A, first: usize, end: usize, field: []const u8, term: []const u8, average: f32, config: inverted.BM25Config) !local.index.IndexSnapshot.TextTermSummary {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.check();
        if (first >= end or end > self.ordinals.len) return error.InvalidNativeLakeTextStatistics;
        const key = try keyAlloc(a, field, term);
        defer a.free(key);
        const prefix = directory_mod.prefixAlloc(a, key) catch |err| switch (err) {
            error.NativeLakeTermDirectoryKeyTooLarge => return .{ .frequency = 1, .tf_upper = std.math.inf(f32) },
            else => return err,
        };
        defer a.free(prefix);
        const lower = try std.mem.concat(a, u8, &.{ prefix, &self.ordinals[first].identity });
        defer a.free(lower);
        const upper = if (end == self.ordinals.len) try a.dupe(u8, prefix) else try std.mem.concat(a, u8, &.{ prefix, &self.ordinals[end].identity });
        defer a.free(upper);
        if (end == self.ordinals.len) upper[upper.len - 1] = 1;
        if (self.range_reads == null) {
            const reads = try self.allocator.create(RangeReads);
            reads.* = .{ .store = self.read.store };
            reads.pages = .{ .domain = self.domain, .artifacts = &reads.store, .cancellation = self.read.cancellation, .remaining_read_bytes = &reads.reads, .remaining_write_bytes = &reads.writes, .read_cache = .{ .ptr = self, .read = readPage } };
            reads.cache = .{ .alloc = self.allocator, .underlying = reads.pages.store() };
            self.range_reads = reads;
        }
        const Fold = struct {
            value: ?Summary = null,
            reductions: u64 = 0,
            pub fn aggregate(self_: *@This(), bytes: []const u8, count: u64) !void {
                const branch = try directory_mod.Branch.decode(bytes);
                if (branch.count != count) return error.InvalidNativeLakeTermDirectory;
                self_.merge(branch.envelope);
                self_.reductions += 1;
            }
            pub fn record(self_: *@This(), _: []const u8, bytes: []const u8) !void {
                self_.merge(try Summary.decode(bytes));
            }
            fn merge(self_: *@This(), value: Summary) void {
                if (self_.value) |*current| current.merge(value) else self_.value = value;
            }
        };
        var fold: Fold = .{};
        tree.reduceRange(a, self.range_reads.?.cache.store(), self.directory.leaves, lower, upper, &fold) catch |err| switch (err) {
            // A budget-limited optional bound becomes unknown, never a partial
            // envelope. Exact leaf scoring still uses its own bounded seeks.
            error.ArtifactReadBudgetExceeded => return .{ .frequency = 1, .tf_upper = std.math.inf(f32) },
            else => return err,
        };
        self.range_reductions += fold.reductions;
        try self.check();
        if (fold.value) |value| {
            var bound = value.bound(average, config);
            bound.frequency = 1;
            return bound;
        }
        return .{ .frequency = 0, .tf_upper = 0 };
    }
    const seek_lane_bytes = 16 * 1024 * 1024;
    const SeekBudget = struct {
        allocator: local.sql_parallel_scheduler.LockedAllocator,
        mutex: std.atomic.Mutex = .unlocked,
        remaining: u64 = 64 * 1024 * 1024,
    };
    const SeekRead = struct {
        reader: *Reader,
        budget: *SeekBudget,
        fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, digest: [32]u8, token: Cancellation, _: *u64) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.reader.check();
            {
                @import("antfly_platform").sync.lockYielding(&self.budget.mutex);
                defer self.budget.mutex.unlock();
                try stores.chargeReadBudget(&self.budget.remaining, length);
            }
            return artifacts.readPageRange(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, offset, length, digest, token, self.reader.read.cache);
        }
    };
    /// Each lane owns its routing cache and bounded decode arena. Shared
    /// verified artifact singleflight deduplicates overlapping ancestor reads.
    /// Results occupy disjoint original-order slots; every task is joined
    /// before the reader capability or output storage can leave scope.
    fn frequencies(raw: *anyopaque, a: A, field: []const u8, terms: []const []const u8, output: []u32) !void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.check();
        if (terms.len != output.len) return error.InvalidNativeLakeTextStatistics;
        var budget: SeekBudget = .{ .allocator = .{ .backing = a } };
        const io = self.read.context.io;
        const lanes: usize = if (io != null) @min(4, @max(1, terms.len)) else 1;
        var tasks: [3]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
        defer if (io) |runtime| for (&tasks) |*slot| if (slot.*) |*task| {
            task.cancel(runtime) catch {};
        };
        for (1..lanes) |lane| {
            tasks[lane - 1] = local.sql_parallel_scheduler.global().submitIo(io.?, seek_lane_bytes, frequencyLane, .{ self, field, terms, output, &budget, lane, lanes });
            if (tasks[lane - 1] == null) try self.frequencyLane(field, terms, output, &budget, lane, lanes);
        }
        try self.frequencyLane(field, terms, output, &budget, 0, lanes);
        if (io) |runtime| for (&tasks) |*slot| if (slot.*) |*task| {
            var owned = task.*;
            slot.* = null;
            try owned.await(runtime);
        };
        try self.check();
    }
    fn summaries(raw: *anyopaque, a: A, segment: usize, field: []const u8, terms: []const []const u8, average: f32, config: inverted.BM25Config, output: []local.index.IndexSnapshot.TextTermSummary) !void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.check();
        if (segment >= self.segments.len or terms.len != output.len) return error.InvalidNativeLakeTextStatistics;
        var budget: SeekBudget = .{ .allocator = .{ .backing = a } };
        const io = self.read.context.io;
        const lanes: usize = if (io != null) @min(4, @max(1, terms.len)) else 1;
        var tasks: [3]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
        defer if (io) |runtime| for (&tasks) |*slot| if (slot.*) |*task| {
            task.cancel(runtime) catch {};
        };
        for (1..lanes) |lane| {
            tasks[lane - 1] = local.sql_parallel_scheduler.global().submitIo(io.?, seek_lane_bytes, summaryLane, .{ self, segment, field, terms, average, config, output, &budget, lane, lanes });
            if (tasks[lane - 1] == null) try self.summaryLane(segment, field, terms, average, config, output, &budget, lane, lanes);
        }
        try self.summaryLane(segment, field, terms, average, config, output, &budget, 0, lanes);
        if (io) |runtime| for (&tasks) |*slot| if (slot.*) |*task| {
            var owned = task.*;
            slot.* = null;
            try owned.await(runtime);
        };
        try self.check();
    }
    fn frequencyLane(self: *Reader, field: []const u8, terms: []const []const u8, output: []u32, shared: *SeekBudget, lane: usize, lanes: usize) anyerror!void {
        try self.check();
        if (terms.len != output.len) return error.InvalidNativeLakeTextStatistics;
        var memory: local.sql_memory_budget = .{ .backing = shared.allocator.allocator(), .limit = seek_lane_bytes };
        var arena = std.heap.ArenaAllocator.init(memory.allocator());
        defer arena.deinit();
        const a = arena.allocator();
        var store = self.read.store;
        var read_context: SeekRead = .{ .reader = self, .budget = shared };
        var writes: u64 = 0;
        var pages: page_store.PageStore = .{ .domain = self.domain, .artifacts = &store, .cancellation = self.read.cancellation, .remaining_read_bytes = &shared.remaining, .remaining_write_bytes = &writes, .read_cache = .{ .ptr = &read_context, .read = SeekRead.readPage } };
        var cache: tree.Cache = .{ .alloc = memory.allocator(), .underlying = pages.store() };
        defer cache.deinit();
        var i = lane;
        while (i < terms.len) : (i += lanes) {
            // Routing pages survive; cursor/key/value scratch does not grow
            // with the number of terms assigned to this lane.
            _ = arena.reset(.free_all);
            const term = terms[i];
            const count = &output[i];
            const key = try keyAlloc(a, field, term);
            defer a.free(key);
            const value = try self.lookup(a, cache.store(), self.global, key);
            defer if (value) |bytes| a.free(bytes);
            count.* = if (value) |bytes| @intCast(@min(std.math.maxInt(u32), try decodeFrequency(bytes))) else 0;
        }
        try self.check();
    }
    fn summaryLane(self: *Reader, segment: usize, field: []const u8, terms: []const []const u8, average: f32, config: inverted.BM25Config, output: []local.index.IndexSnapshot.TextTermSummary, shared: *SeekBudget, lane: usize, lanes: usize) anyerror!void {
        try self.check();
        if (segment >= self.segments.len or terms.len != output.len) return error.InvalidNativeLakeTextStatistics;
        var memory: local.sql_memory_budget = .{ .backing = shared.allocator.allocator(), .limit = seek_lane_bytes };
        var arena = std.heap.ArenaAllocator.init(memory.allocator());
        defer arena.deinit();
        const a = arena.allocator();
        var store = self.read.store;
        var read_context: SeekRead = .{ .reader = self, .budget = shared };
        var writes: u64 = 0;
        var pages: page_store.PageStore = .{ .domain = self.domain, .artifacts = &store, .cancellation = self.read.cancellation, .remaining_read_bytes = &shared.remaining, .remaining_write_bytes = &writes, .read_cache = .{ .ptr = &read_context, .read = SeekRead.readPage } };
        var cache: tree.Cache = .{ .alloc = memory.allocator(), .underlying = pages.store() };
        defer cache.deinit();
        var i = lane;
        while (i < terms.len) : (i += lanes) {
            // Routing pages survive; cursor/key/value scratch does not grow
            // with the number of terms assigned to this lane.
            _ = arena.reset(.free_all);
            const term = terms[i];
            const result = &output[i];
            const key = try keyAlloc(a, field, term);
            defer a.free(key);
            var lookup_key = key;
            var root = self.segments[segment];
            if (self.directory.leaves != null) {
                if (self.identities.len != self.segments.len) return error.InvalidNativeLakeTermDirectory;
                const prefix = directory_mod.prefixAlloc(a, key) catch |err| switch (err) {
                    error.NativeLakeTermDirectoryKeyTooLarge => null,
                    else => return err,
                };
                if (prefix) |encoded| {
                    defer a.free(encoded);
                    lookup_key = try a.alloc(u8, encoded.len + 32);
                    @memcpy(lookup_key[0..encoded.len], encoded);
                    @memcpy(lookup_key[encoded.len..], &self.identities[segment]);
                    root = self.directory.leaves;
                }
            }
            defer if (lookup_key.ptr != key.ptr) a.free(lookup_key);
            const value = try self.lookup(a, cache.store(), root, lookup_key);
            defer if (value) |bytes| a.free(bytes);
            result.* = if (value) |bytes| (try Summary.decode(bytes)).bound(average, config) else .{ .frequency = 0, .tf_upper = 0 };
        }
        try self.check();
    }
    fn summary(raw: *anyopaque, a: A, segment: usize, field: []const u8, term: []const u8, average: f32, config: inverted.BM25Config) !local.index.IndexSnapshot.TextTermSummary {
        var output: [1]local.index.IndexSnapshot.TextTermSummary = undefined;
        try summaries(raw, a, segment, field, &.{term}, average, config, &output);
        return output[0];
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
    // Same original-order results with independent cold seeks in four lanes.
    var parallel = reopened;
    parallel.read.context.io = std.testing.io;
    var parallel_counts: [4]u32 = undefined;
    try Reader.frequencies(&parallel, a, "body", &.{ "alpha", "beta", "gamma", "common" }, &parallel_counts);
    try std.testing.expectEqualSlices(u32, &counts, &parallel_counts);
    var serial_summaries: [4]local.index.IndexSnapshot.TextTermSummary = undefined;
    var parallel_summaries: [4]local.index.IndexSnapshot.TextTermSummary = undefined;
    try Reader.summaries(&reopened, a, 0, "body", &.{ "gamma", "common", "absent", "gamma" }, 100, .{}, &serial_summaries);
    try Reader.summaries(&parallel, a, 0, "body", &.{ "gamma", "common", "absent", "gamma" }, 100, .{}, &parallel_summaries);
    try std.testing.expectEqualDeep(serial_summaries, parallel_summaries);
    // Lane zero succeeds while an admitted background lane rejects its key.
    // Joining that error must consume the task before deferred cleanup runs.
    const oversized = try a.alloc(u8, tree.max_key_bytes);
    defer a.free(oversized);
    @memset(oversized, 'x');
    try std.testing.expectError(error.InvalidNativeLakeTextStatistics, Reader.frequencies(&parallel, a, "body", &.{ "gamma", oversized }, parallel_counts[0..2]));
    try std.testing.expectError(error.InvalidNativeLakeTextStatistics, Reader.summaries(&parallel, a, 0, "body", &.{ "gamma", oversized }, 100, .{}, parallel_summaries[0..2]));
    parallel.read.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, Reader.frequencies(&parallel, a, "body", &.{ "alpha", "beta", "gamma", "common" }, &parallel_counts));
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
    var source: Source = .{ .count = 16000 };
    const summaries = try tree.buildSorted(a, backing.store(), &source);
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Source.checkpoint };
    defer manager.deinit();
    var initial = Builder.init(a, &manager);
    defer initial.deinit();
    initial.sort.memory_bytes = tree.bulk_target_page_bytes;
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
    std.debug.print("text statistics single-term update: written_bytes={}\n", .{backing.written_bytes - written});
    // A one-term change rewrites its path, not the entire vocabulary.
    try std.testing.expect(backing.written_bytes - written < tree.bulk_target_page_bytes * 2);
    try std.testing.expectEqual(global.records, changed.records);
    for ([_]u64{ 0, 1, 15999 }) |ordinal| {
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
    try std.testing.expectEqual(@as(u64, 15999), without.records);
    var invalid = Builder.init(a, &manager);
    defer invalid.deinit();
    try invalid.add(backing.store(), removed, -1);
    try std.testing.expectError(error.InvalidNativeLakeTextStatistics, invalid.finish(backing.store(), without));
}

test "external lake text statistics compact envelopes dominate all blocks and tighten scalar bounds" {
    const a = std.testing.allocator;
    var envelope: Summary = .{ .frequency = 100, .max_frequency = 0, .min_norm = std.math.maxInt(u32) };
    var pairs: [100]Summary.Pair = undefined;
    for (&pairs, 0..) |*pair, i| {
        pair.* = .{ .frequency = @intCast(i + 1), .norm = @intCast(i * i + 1) };
        envelope.addPair(pair.*);
    }
    try std.testing.expect(envelope.pair_count <= Summary.max_pairs);
    var bytes: [Summary.max_bytes]u8 = undefined;
    const decoded = try Summary.decode(envelope.encode(&bytes));
    for ([_]f32{ 0, 0.1, 1.2, 10 }) |k1| for ([_]f32{ 0, 0.25, 0.75, 1 }) |b| for ([_]f32{ 1, 50, 1000 }) |average| {
        const ceiling = decoded.bound(average, .{ .k1 = k1, .b = b }).tf_upper;
        for (pairs) |pair| try std.testing.expect(ceiling >= inverted.bm25ScoreWithIdf(pair.frequency, pair.norm, average, 1, .{ .k1 = k1, .b = b }));
    };
    var tight: Summary = .{ .frequency = 2, .max_frequency = 0, .min_norm = std.math.maxInt(u32) };
    tight.addPair(.{ .frequency = 1, .norm = 1 });
    tight.addPair(.{ .frequency = 10, .norm = 1000 });
    try std.testing.expect(tight.bound(100, .{}).tf_upper < inverted.bm25ScoreWithIdf(10, 1, 100, 1, .{}));
    tight.addPair(.{ .frequency = std.math.maxInt(u32), .norm = 5 });
    try std.testing.expectEqual(inverted.BM25TermScorer.init(100, 1, .{}).maxScore(), tight.bound(100, .{}).tf_upper);
    try std.testing.expect(std.math.isInf(tight.bound(100, .{ .b = -1 }).tf_upper));
    _ = a;
}

test "external lake text statistics dense streaming merge equals COW with no point seeks" {
    const a = std.testing.allocator;
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    const Source = struct {
        next_id: u32 = 0,
        end: u32,
        key: [4]u8 = undefined,
        value: [12]u8 = undefined,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.next_id == self.end) return null;
            std.mem.writeInt(u32, &self.key, self.next_id, .big);
            self.next_id += 1;
            std.mem.writeInt(u32, self.value[0..4], 1, .little);
            std.mem.writeInt(u32, self.value[4..8], 1, .little);
            std.mem.writeInt(u32, self.value[8..12], 10, .little);
            return .{ .key = &self.key, .value = &self.value };
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var first: Source = .{ .end = 6000 };
    const contribution = try tree.buildSorted(a, memory.store(), &first);
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Source.checkpoint };
    defer manager.deinit();
    var initial = Builder.init(a, &manager);
    defer initial.deinit();
    try initial.add(memory.store(), contribution, 1);
    const prior = try initial.finish(memory.store(), null);
    var second: Source = .{ .next_id = 3000, .end = 9000 };
    const added = try tree.buildSorted(a, memory.store(), &second);
    var cow = Builder.init(a, &manager);
    defer cow.deinit();
    cow.strategy = .cow;
    var merged = Builder.init(a, &manager);
    defer merged.deinit();
    for ([_]*Builder{ &cow, &merged }) |builder| {
        try builder.add(memory.store(), contribution, -1);
        try builder.add(memory.store(), added, 1);
    }
    const cow_root = try cow.finish(memory.store(), prior);
    const merge_root = try merged.finish(memory.store(), prior);
    try std.testing.expectEqual(Builder.Strategy.merge, merged.selected);
    try std.testing.expectEqual(@as(u64, 0), merged.point_seeks);
    try std.testing.expect(cow.point_seeks > 0);
    var left = try tree.Cursor.init(a, memory.store(), cow_root, "", null);
    defer left.deinit();
    var right = try tree.Cursor.init(a, memory.store(), merge_root, "", null);
    defer right.deinit();
    while (try left.next()) |record| {
        const actual = (try right.next()).?;
        try std.testing.expectEqualSlices(u8, record.key, actual.key);
        try std.testing.expectEqualSlices(u8, record.value, actual.value);
    }
    try std.testing.expect((try right.next()) == null);
    // Thousands of repeated, mostly cancelling contributions change one key.
    // Strategy selection must reflect one nonzero delta, including spilled replay.
    var repeated = Builder.init(a, &manager);
    defer repeated.deinit();
    repeated.sort.memory_bytes = 64 * 1024;
    repeated.sort.parallel_runs = false;
    var repeat_value: [Summary.max_bytes]u8 = undefined;
    const repeat_summary: Summary = .{ .frequency = 1, .max_frequency = 1, .min_norm = 10 };
    for (0..8193) |i| try repeated.addRecord(.{ .key = "\x00\x00\x00\x00", .value = repeat_summary.encode(&repeat_value) }, if (i < 4097) 1 else -1);
    const repeated_root = try repeated.finish(memory.store(), prior);
    try std.testing.expectEqual(Builder.Strategy.cow, repeated.selected);
    try std.testing.expectEqual(@as(u64, 1), repeated.distinct_changes);
    try std.testing.expectEqual(@as(u64, 1), repeated.point_seeks);
    var repeated_cursor = try tree.Cursor.init(a, memory.store(), repeated_root, "", null);
    defer repeated_cursor.deinit();
    try std.testing.expectEqual(@as(u64, 2), try decodeFrequency((try repeated_cursor.next()).?.value));
    var canceled_delta = Builder.init(a, &manager);
    defer canceled_delta.deinit();
    try canceled_delta.add(memory.store(), contribution, -1);
    try canceled_delta.add(memory.store(), contribution, 1);
    try std.testing.expect((try canceled_delta.finish(memory.store(), prior)).?.eql(prior.?));
    try std.testing.expectEqual(@as(u64, 0), canceled_delta.point_seeks);
}

test "external lake text statistics hierarchy plans rare terms across ten thousand identities without leaf reads" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("term-directed-planning");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(6), .attempt = @splat(4) };
    const Source = struct {
        done: bool = false,
        buffer: [Summary.max_bytes]u8 = undefined,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.done) return null;
            self.done = true;
            var summary: Summary = .{ .frequency = 2, .max_frequency = 0, .min_norm = std.math.maxInt(u32) };
            summary.addPair(.{ .frequency = 1, .norm = 1 });
            summary.addPair(.{ .frequency = 20, .norm = 2000 });
            return .{ .key = "\x00\x00\x00\x04bodyrare", .value = summary.encode(&self.buffer) };
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = 64 * 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = @splat(6), .attempt = @splat(4), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var source: Source = .{};
    const summary = try tree.buildSorted(a, pages.store(), &source);
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Source.checkpoint };
    defer manager.deinit();
    const ids = try a.alloc([32]u8, 10000);
    defer a.free(ids);
    const segments = try a.alloc(?Ref, ids.len);
    defer a.free(segments);
    for (ids, segments, 0..) |*id, *ref, ordinal| {
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(u64, &encoded, ordinal, .big);
        id.* = directory_mod.identity(&encoded);
        ref.* = null; // Missing native summary trees must never be opened.
    }
    const ordinals = try a.alloc(IdentityOrdinal, ids.len);
    defer a.free(ordinals);
    for (ordinals, ids, 0..) |*ordinal, id, i| ordinal.* = .{ .identity = id, .segment_idx = i, .doc_offset = @intCast(i * 2) };
    std.mem.sort(IdentityOrdinal, ordinals, {}, identityLess);
    var builder = directory_mod.Builder.init(a, &manager);
    defer builder.deinit();
    try builder.add(pages.store(), summary, ids[4321], 1);
    const roots = try builder.finish(pages.store(), .{});
    var reader: Reader = .{ .allocator = a, .read = .{ .store = store, .cache = null, .context = .{}, .cancellation = .none }, .domain = @splat(6), .global = null, .segments = segments, .identities = ids, .ordinals = ordinals, .directory = roots };
    defer reader.deinit();
    try Reader.prepare(&reader, a, "body", &.{ "rare", "absent" });
    try std.testing.expectEqual(@as(u64, 1), reader.planning_branch_reads);
    const selected = (try Reader.candidates(&reader, a, "body", "rare")).?;
    defer a.free(selected);
    try std.testing.expectEqual(@as(usize, 1), selected.len);
    try std.testing.expectEqual(@as(usize, 4321), selected[0].segment_idx);
    try std.testing.expectEqual(@as(u32, 8642), selected[0].doc_offset);
    const absent = (try Reader.candidates(&reader, a, "body", "absent")).?;
    defer a.free(absent);
    try std.testing.expectEqual(@as(usize, 0), absent.len);
    const long_term = try a.alloc(u8, tree.max_key_bytes - 40);
    defer a.free(long_term);
    @memset(long_term, 'x');
    try std.testing.expect((try Reader.candidates(&reader, a, "body", long_term)) == null);
    var fallback: [1]local.index.IndexSnapshot.TextTermSummary = undefined;
    try Reader.summaries(&reader, a, 4321, "body", &.{long_term}, 100, .{}, &fallback);
    try std.testing.expectEqual(@as(u32, 0), fallback[0].frequency);
    var admitted: usize = 0;
    for (0..ids.len) |ordinal| {
        const planned = (try Reader.planning(&reader, a, ordinal, "body", "rare", 100, .{})).?;
        if (planned.frequency != 0) {
            admitted += 1;
            try std.testing.expectEqual(@as(usize, 4321), ordinal);
        }
        try std.testing.expectEqual(@as(u32, 0), (try Reader.planning(&reader, a, ordinal, "body", "absent", 100, .{})).?.frequency);
    }
    try std.testing.expectEqual(@as(usize, 1), admitted);
    var exact: [2]local.index.IndexSnapshot.TextTermSummary = undefined;
    try Reader.summaries(&reader, a, 4321, "body", &.{ "rare", "absent" }, 100, .{}, &exact);
    try std.testing.expectEqual(@as(u32, 2), exact[0].frequency);
    try std.testing.expectEqual(@as(u32, 0), exact[1].frequency);
    // Branches carry stable identities, not offsets from the publishing writer.
    std.mem.swap([32]u8, &ids[4321], &ids[123]);
    try std.testing.expectEqual(@as(u32, 0), (try Reader.planning(&reader, a, 4321, "body", "rare", 100, .{})).?.frequency);
    try std.testing.expectEqual(@as(u32, 1), (try Reader.planning(&reader, a, 123, "body", "rare", 100, .{})).?.frequency);
    // Raw envelopes remain reusable for different BM25 settings.
    const custom = (try Reader.planning(&reader, a, 123, "body", "rare", 100, .{ .k1 = 2, .b = 1 })).?;
    try std.testing.expect(custom.tf_upper >= inverted.bm25ScoreWithIdf(20, 2000, 100, 1, .{ .k1 = 2, .b = 1 }));
    // A bounded preparation cache falls back without installing partial sets.
    var capped: Reader = .{ .allocator = a, .read = reader.read, .domain = reader.domain, .global = null, .segments = segments, .identities = ids, .directory = roots, .plan_limit = 0 };
    defer capped.deinit();
    try Reader.prepare(&capped, a, "body", &.{"rare"});
    try std.testing.expect((try Reader.planning(&capped, a, 123, "body", "rare", 100, .{})) == null);
    var invalid: Reader = .{ .allocator = a, .read = reader.read, .domain = reader.domain, .global = null, .segments = segments, .identities = ids, .directory = roots };
    defer invalid.deinit();
    invalid.directory.branches.?.digest[0] ^= 1;
    try std.testing.expectError(error.FileNotFound, Reader.prepare(&invalid, a, "body", &.{"rare"}));
    const Check = struct {
        fn run(alloc: A, template: *const Reader) !void {
            var fresh: Reader = .{ .allocator = alloc, .read = template.read, .domain = template.domain, .global = null, .segments = template.segments, .identities = template.identities, .ordinals = template.ordinals, .directory = template.directory };
            defer fresh.deinit();
            try Reader.prepare(&fresh, alloc, "body", &.{ "rare", "absent" });
            const candidates_ = (try Reader.candidates(&fresh, alloc, "body", "rare")).?;
            defer alloc.free(candidates_);
            try std.testing.expectEqual(@as(usize, 1), candidates_.len);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{&reader});
}

test "external lake text statistics broad ranges fold authenticated interior envelopes" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("term-range-planning");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(6), .attempt = @splat(4) };
    const key = try keyAlloc(a, "body", "common");
    defer a.free(key);
    const prefix = try directory_mod.prefixAlloc(a, key);
    defer a.free(prefix);
    const Source = struct {
        key: []u8,
        prefix: []const u8,
        ordinal: u32 = 0,
        buffer: [Summary.max_bytes]u8 = undefined,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.ordinal == 5000) return null;
            @memcpy(self.key[0..self.prefix.len], self.prefix);
            @memset(self.key[self.prefix.len..], 0);
            std.mem.writeInt(u32, self.key[self.key.len - 4 ..][0..4], self.ordinal, .big);
            self.ordinal += 1;
            const value: Summary = .{ .frequency = 1, .max_frequency = if (self.ordinal <= 2500) 1 else 10, .min_norm = 10 };
            return .{ .key = self.key, .value = value.encode(&self.buffer) };
        }
    };
    const record_key = try a.alloc(u8, prefix.len + 32);
    defer a.free(record_key);
    var source: Source = .{ .key = record_key, .prefix = prefix };
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = 64 * 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = @splat(6), .attempt = @splat(4), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var page_source = pages.store();
    page_source.reduce = directory_mod.reduce;
    const root = try tree.buildSorted(a, page_source, &source);
    const ordinals = try a.alloc(IdentityOrdinal, 5000);
    defer a.free(ordinals);
    const segments = try a.alloc(?Ref, 5000);
    defer a.free(segments);
    @memset(segments, null);
    for (ordinals, 0..) |*ordinal, i| {
        ordinal.* = .{ .identity = @splat(0), .segment_idx = i, .doc_offset = @intCast(i) };
        std.mem.writeInt(u32, ordinal.identity[28..32], @intCast(i), .big);
    }
    var reader: Reader = .{ .allocator = a, .read = .{ .store = store, .cache = null, .context = .{}, .cancellation = .none }, .domain = @splat(6), .global = null, .segments = segments, .ordinals = ordinals, .directory = .{ .version = 2, .leaves = root } };
    defer reader.deinit();
    const low = try Reader.rangeSummary(&reader, a, 0, 2500, "body", "common", 10, .{});
    const high = try Reader.rangeSummary(&reader, a, 2500, 5000, "body", "common", 10, .{});
    try std.testing.expect(high.tf_upper > low.tf_upper);
    try std.testing.expect(reader.range_reductions > 0);
    try std.testing.expect(low.tf_upper >= inverted.bm25ScoreWithIdf(1, 10, 10, 1, .{}));
    try std.testing.expect(high.tf_upper >= inverted.bm25ScoreWithIdf(10, 10, 10, 1, .{}));
    try std.testing.expectEqual(@as(u32, 0), (try Reader.rangeSummary(&reader, a, 0, 5000, "body", "absent", 10, .{})).frequency);
    // An exhausted optional routing budget returns a full unknown ceiling.
    reader.range_reads.?.cache.deinit();
    reader.range_reads.?.reads = 0;
    const unknown = try Reader.rangeSummary(&reader, a, 0, 5000, "body", "common", 10, .{});
    try std.testing.expect(std.math.isInf(unknown.tf_upper));
    reader.read.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, Reader.rangeSummary(&reader, a, 0, 5000, "body", "common", 10, .{}));
}

test "external lake text statistics durable deltas coalesce files and resume signed contributions" {
    const a = std.testing.allocator;
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check, .max_bytes = 64 * 1024 };
    defer manager.deinit();
    var base = Builder.init(a, &manager);
    defer base.deinit();
    var buffer: [Summary.max_bytes]u8 = undefined;
    const summary = (Summary{ .frequency = 3, .max_frequency = 3, .min_norm = 2 }).encode(&buffer);
    try base.addRecord(.{ .key = "common", .value = summary }, 1);
    const prior = try base.finish(memory.store(), null);
    var publication_writes: [2]usize = undefined;
    for ([_]usize{ 1, 64 }, 0..) |width, mode| {
        var deltas = Builder.init(a, &manager);
        defer deltas.deinit();
        deltas.enableDurable(memory.store());
        // Compare per-file durability with completed-file checkpoint batches.
        try deltas.addRecord(.{ .key = "common", .value = summary }, -1);
        try deltas.flush();
        const writes_before = memory.writes;
        for (0..70) |i| {
            try deltas.addRecord(.{ .key = "common", .value = summary }, 1);
            if ((i + 1) % width == 0) try deltas.flush();
        }
        try deltas.flush();
        publication_writes[mode] = memory.writes - writes_before;
        const checkpoint = try deltas.durable.?.snapshot(a);
        defer a.free(checkpoint);
        try std.testing.expect(checkpoint.len <= 7);
        var resumed = Builder.init(a, &manager);
        defer resumed.deinit();
        resumed.enableDurable(memory.store());
        try resumed.durable.?.restore(checkpoint);
        const result = try resumed.finish(memory.store(), prior);
        var cursor = try tree.Cursor.init(a, memory.store(), result, "", null);
        defer cursor.deinit();
        try std.testing.expectEqual(@as(u64, 210), try decodeFrequency((try cursor.next()).?.value));
        try std.testing.expect((try cursor.next()) == null);
    }
    // The same resumable signed state needs far fewer immutable page writes.
    try std.testing.expect(publication_writes[1] * 8 < publication_writes[0]);
    var old = try tree.Cursor.init(a, memory.store(), prior, "", null);
    defer old.deinit();
    try std.testing.expectEqual(@as(u64, 3), try decodeFrequency((try old.next()).?.value));
}

test "external lake global frontier exact frequency one ceilings cover canonical scorer arithmetic" {
    const summary: Summary = .{ .frequency = 100, .max_frequency = 1, .min_norm = 3 };
    for ([_]f32{ 0.125, 1, 100, 1000000 }) |average| for ([_]f32{ 0, 0.5, 1.2, 8 }) |k1| for ([_]f32{ 0, 0.75, 1 }) |b| for ([_]f32{ 0, 0.0001, 1, 100 }) |idf| for ([_]bool{ false, true }) |legacy| {
        const config: inverted.BM25Config = .{ .k1 = k1, .b = b };
        const bound_ = summary.fixedFrequencyScore(average, config, idf, legacy).?;
        for ([_]u32{ 3, 4, 5, 255, 65535, std.math.maxInt(u32) }) |norm| {
            const actual = if (legacy) inverted.bm25ScoreWithIdf(1, norm, average, idf, config) else inverted.BM25TermScorer.init(average, idf, config).score(1, norm);
            try std.testing.expect(bound_ >= actual);
        }
    };
    var uncertain = summary;
    uncertain.max_frequency = 2;
    try std.testing.expect(uncertain.fixedFrequencyScore(100, .{}, 1, false) == null);
}

test "external lake global frontier repeated frequency and phrase ceilings cover exact f32 scores" {
    for ([_]u32{ 2, 3, 7, 31, 127, 256 }) |maximum| {
        const summary: Summary = .{ .frequency = 100, .max_frequency = maximum, .min_norm = 3 };
        for ([_]f32{ 0.125, 100, 1000000 }) |average| for ([_]f32{ 0, 1.2, 8 }) |k1| for ([_]f32{ 0, 0.75, 1 }) |b| for ([_]f32{ 0, 0.0001, 7, 100 }) |idf| for ([_]bool{ false, true }) |legacy| {
            const config: inverted.BM25Config = .{ .k1 = k1, .b = b };
            const ceiling = summary.exactScoreCeiling(average, config, idf, legacy).?;
            for (1..@as(usize, maximum) + 1) |frequency| for ([_]u32{ 3, 4, 255, 65535, std.math.maxInt(u32) }) |norm| {
                const actual = if (legacy) inverted.bm25ScoreWithIdf(@intCast(frequency), norm, average, idf, config) else inverted.BM25TermScorer.init(average, idf, config).score(@intCast(frequency), norm);
                try std.testing.expect(ceiling >= actual);
            };
        };
    }
    const escaped: Summary = .{ .frequency = 100, .max_frequency = std.math.maxInt(u32), .min_norm = 3 };
    try std.testing.expect(escaped.exactScoreCeiling(100, .{}, 1, false) == null);
}

test "external lake captured writer summaries equal reader summaries for initial and deleted merges" {
    const a = std.testing.allocator;
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var context: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = Check.call, .async_writes = false };
    defer manager.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("captured-text-statistics");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    const docs = try out.alloc(local.introducer.TextDocument, 4200);
    for (docs, 0..) |*doc, i| {
        const text = try std.fmt.allocPrint(out, "quasar common common unique{d} {s}", .{ i, if (i % 3 == 0) "more words many many many" else "short" });
        doc.* = .{ .id = try std.fmt.allocPrint(out, "{d:0>8}", .{i}), .stored_data = "{}", .text_fields = try out.dupe(local.introducer.TextField, &.{ .{ .field_name = "body", .text = text }, .{ .field_name = "tag", .text = "same" } }) };
    }
    const captured = try Captured.create(a, &manager);
    defer captured.deinit();
    var memory = local.segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    sink.term_observer = captured.observer();
    try local.storage_db_document_mapper.writeTextSegmentFromProjectionBatchToSink(a, .{ .docs = docs }, .{}, .{ .index_sort = &.{.{ .field = "_id", .desc = false }} }, &sink);
    const bytes = try memory.finishOwned();
    defer a.free(bytes);
    var reader = try local.segment.SegmentReader.init(a, bytes);
    defer reader.deinit();
    const expected = (try publishReaderWithIo(a, &store, &reader, .none, std.testing.io)).?;
    const actual = (try captured.publish(&store, .none, std.testing.io)).?;
    try std.testing.expect(std.meta.eql(expected, actual));
    try std.testing.expect(captured.count > 4200);
    var deleted = local.encoding_roaring.RoaringBitmap.init(a);
    defer deleted.deinit();
    try deleted.addRange(128, 255);
    // Include unchanged section copying, terms deleted entirely, large streamed
    // merges and sorted document maps.
    for (0..4) |case| {
        const merged_capture = try Captured.create(a, &manager);
        defer merged_capture.deinit();
        var merged = local.segment.MemorySegmentSink.init(a);
        defer merged.deinit();
        var merged_sink = merged.sink();
        merged_sink.term_observer = merged_capture.observer();
        const inputs: [2]local.segment.MergeInput = .{ .{ .reader = &reader, .deleted = if (case == 0) null else deleted }, .{ .reader = &reader } };
        const selected = inputs[0..if (case >= 2) @as(usize, 2) else 1];
        const options: local.segment.MergeOptions = .{ .index_sort = if (case == 3) &.{.{ .field = "_id", .desc = false }} else &.{} };
        try local.segment.writeMergedSegmentToSinkWithOptions(a, &merged_sink, selected, options);
        const merged_bytes = try merged.finishOwned();
        defer a.free(merged_bytes);
        const reference_bytes = try local.segment.mergeSegmentInputsWithOptions(a, selected, options);
        defer a.free(reference_bytes);
        try std.testing.expectEqualSlices(u8, reference_bytes, merged_bytes);
        var merged_reader = try local.segment.SegmentReader.init(a, merged_bytes);
        defer merged_reader.deinit();
        const expected_merged = (try publishReaderWithIo(a, &store, &merged_reader, .none, std.testing.io)).?;
        const actual_merged = (try merged_capture.publish(&store, .none, std.testing.io)).?;
        try std.testing.expect(std.meta.eql(expected_merged, actual_merged));
    }
}

test "external lake captured summaries release private spills on every allocation failure" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: A) !void {
            var context: u8 = 0;
            var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = check, .async_writes = false };
            defer manager.deinit();
            {
                const captured = try Captured.create(a, &manager);
                defer captured.deinit();
                const observer = captured.observer();
                for ([_][]const u8{ "alpha", "beta" }) |term| {
                    try observer.begin(observer.ptr, "body", term);
                    try observer.finish(observer.ptr, 1, null, 128);
                }
                try captured.seal();
                // The production observer deliberately declines optional
                // capture on OOM. Tell the allocation-failure harness that
                // its injected failure was observed after cleanup.
                if (captured.disabled) return error.OutOfMemory;
            }
            try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
            try std.testing.expectEqual(@as(usize, 0), manager.files);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "external lake captured prefix frames bound repeated dictionary bytes and restart per field" {
    const a = std.testing.allocator;
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var context: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = Check.call, .async_writes = false };
    defer manager.deinit();
    const captured = try Captured.create(a, &manager);
    defer captured.deinit();
    const observer = captured.observer();
    const prefix: [200]u8 = @splat('a');
    for ([_][]const u8{ "z", "body" }) |field| for (0..4096) |i| {
        var term: [256]u8 = undefined;
        const text = try std.fmt.bufPrint(&term, "{s}{d:0>8}", .{ prefix, i });
        try observer.begin(observer.ptr, field, text);
        try observer.finish(observer.ptr, 1, null, 3);
    };
    try captured.seal();
    try std.testing.expect(!captured.disabled);
    try std.testing.expect(captured.length() < 8192 * 64);
    try std.testing.expectEqual(@as(u64, 8192), captured.count);
    try std.testing.expectEqualStrings("z", captured.fields.items[0].name);
}
