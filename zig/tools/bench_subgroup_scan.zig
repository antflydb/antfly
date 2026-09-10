//! Offline combined-cost screen on an authenticated immutable AFPS directory.
//! Borrowed leaf bytes remain owned for the whole run. This is not a live
//! generation-lease/recovery test and does not measure exact completion/recall.
const std = @import("std");
const vi = @import("antfly_vector_index");
const vector = @import("antfly_vector");
const weighted = vi.weighted_subgroup_selection;
const reps = @import("bench_subgroup_routing.zig");
const Range = vector.quantizer.ScoreRange;
const Mode = enum { control, sort_predicate, partition_predicate, partition_ranges };
const Leaf = struct { view: vi.quantized_directory.View, first_group: usize };
const Group = struct { codes: []i8, scale: f32, weight: u32 };

const Sink = struct {
    results: *vi.search_results.ApproxSearchResults,
    ids: []const u64,
    first: usize = 0,
    count: usize = 0,
    distances: [8]f32 = undefined,
    bounds: [8]f32 = undefined,
    pub fn write(self: *@This(), index: usize, distance: f32, bound: f32) void {
        if (index != self.first + self.count) {
            self.flush();
            self.first = index;
        }
        self.distances[self.count] = distance;
        self.bounds[self.count] = bound;
        self.count += 1;
        if (self.count == 8) self.flush();
    }
    fn flush(self: *@This()) void {
        if (self.count == 0) return;
        self.results.addApproxResults(self.ids[self.first..][0..self.count], self.distances[0..self.count], self.bounds[0..self.count]);
        self.first += self.count;
        self.count = 0;
    }
};
const Predicate = struct {
    sink: *Sink,
    ends: []const u32,
    selected: []const bool,
    group: usize = 0,
    pub fn accepts(self: *@This(), index: usize) bool {
        while (index >= self.ends[self.group]) self.group += 1;
        return self.selected[self.group];
    }
    pub fn write(self: *@This(), index: usize, distance: f32, bound: f32) void {
        self.sink.write(index, distance, bound);
    }
};

fn word(bytes: []const u8, offset: usize) u32 {
    return std.mem.readInt(u32, bytes[offset..][0..4], .little);
}
fn now(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).nanoseconds;
}

pub fn main(init: std.process.Init) !void {
    const alloc = init.arena.allocator();
    const args = try init.minimal.args.toSlice(alloc);
    // Rotation is explicit: the runtime configuration isn't encoded in AFPS.
    if (args.len != 4 or (!std.mem.eql(u8, args[3], "none") and !std.mem.eql(u8, args[3], "givens"))) return error.ExpectedSegmentQueryFixtureRotation;
    const segment_bytes = try std.Io.Dir.cwd().readFileAllocOptions(init.io, args[1], alloc, .limited(512 * 1024 * 1024), .@"64", null);
    var segment = try vi.posting_segment.VerifiedReader.init(alloc, segment_bytes);
    defer segment.deinit();
    const metadata_bytes = (try segment.getValue(0, .index_metadata)) orelse return error.MissingMetadata;
    if (metadata_bytes.len != vi.hbc.IndexMetadata.encoded_size) return error.InvalidMetadata;
    const metadata = vi.hbc.IndexMetadata.decode(metadata_bytes);
    const directory_bytes = (try segment.getNestedContainer(0, .quantized_directory)) orelse return error.MissingDirectory;
    var directory = try vi.quantized_directory.VerifiedReader.init(alloc, directory_bytes);
    defer directory.deinit();
    const dims = directory.reader.dims;
    if (dims != metadata.dims or directory.reader.metric != metadata.metric or directory.reader.metric != @intFromEnum(vector.vector.DistanceMetric.cosine) or dims > 4096) return error.UnsupportedDirectory;
    var leaves: std.ArrayList(Leaf) = .empty;
    var groups: std.ArrayList(Group) = .empty;
    var total_rows: u64 = 0;
    for (0..directory.reader.posting_count) |i| {
        const id = directory.missingProjectionLeafAt(i) orelse continue;
        const view = try directory.getAt(i, id);
        const plan = view.subgroup_plan orelse continue;
        if (groups.items.len + plan.ends.len > 65536) return error.TooManyGroups;
        try leaves.append(alloc, .{ .view = view, .first_group = groups.items.len });
        for (0..plan.ends.len) |g| {
            const range = plan.range(g);
            const codes = try alloc.alloc(i8, dims);
            const scale = reps.quantize(plan.centers[g * dims ..][0..dims], codes);
            try groups.append(alloc, .{ .codes = codes, .scale = scale, .weight = @intCast(range.end - range.start) });
        }
        total_rows += view.count;
    }
    if (leaves.items.len == 0) return error.NoSubgroupLeaves;
    if (total_rows != metadata.active_count) return error.IncompleteSubgroupBase;
    const fixture = try std.Io.Dir.cwd().readFileAlloc(init.io, args[2], alloc, .limited(160 * 1024 * 1024));
    if (fixture.len < 16 or !std.mem.eql(u8, fixture[0..4], "SGRP") or word(fixture, 8) != dims) return error.InvalidFixture;
    const fixture_groups: usize = word(fixture, 4);
    const encoded_query_count: usize = word(fixture, 12);
    if (fixture_groups > 8192 or encoded_query_count == 0 or encoded_query_count > 256 or fixture.len != 16 + (fixture_groups + encoded_query_count) * dims * 4) return error.InvalidFixture;
    const debug = @import("builtin").mode == .Debug;
    const query_count = if (debug) @min(encoded_query_count, 4) else encoded_query_count;
    const queries = try alloc.alloc(f32, query_count * dims);
    const approx_queries = try alloc.alloc(f32, queries.len);
    var rotation = try vector.vector.RandomOrthogonalTransformer.init(alloc, if (std.mem.eql(u8, args[3], "givens")) .givens else .none, dims, metadata.quantizer_seed);
    defer rotation.deinit();
    for (queries, 0..) |*value, i| {
        value.* = @bitCast(word(fixture, 16 + (fixture_groups * dims + i) * 4));
        if (!std.math.isFinite(value.*)) return error.InvalidFixture;
    }
    for (0..query_count) |qi| {
        _ = rotation.transform(queries[qi * dims ..][0..dims], approx_queries[qi * dims ..][0..dims]);
        _ = vector.vector.normalize(approx_queries[qi * dims ..][0..dims]);
    }
    var quantizer = try vector.quantizer.RaBitQuantizer.init(alloc, dims, metadata.quantizer_seed, .cosine);
    defer quantizer.deinit();
    var scratch = try vector.quantizer.RaBitQuantizer.EstimateScratch.init(alloc, dims);
    defer scratch.deinit(alloc);
    var results = try vi.search_results.ApproxSearchResults.initCapacity(alloc, 100, 900, 1800);
    defer results.deinit();
    const parent_order = try alloc.alloc(weighted.Entry, leaves.items.len);
    const frontier = try alloc.alloc(bool, leaves.items.len);
    const entries = try alloc.alloc(weighted.Entry, groups.items.len);
    const selected = try alloc.alloc(bool, groups.items.len);
    const qcodes = try alloc.alloc(i8, dims);
    const reference = try alloc.alloc(vi.search_results.ApproxSearchResult, query_count * results.max_items);
    const reference_counts = try alloc.alloc(usize, query_count);
    const modes = [_]Mode{ .control, .sort_predicate, .partition_predicate, .partition_ranges };
    for (0..@as(usize, if (debug) 1 else 6)) |round| {
        for (0..modes.len) |position| {
            // The first sorting pass establishes untimed output identity for
            // all selected-work modes. Later rounds reverse ordering.
            const mode = modes[if (round % 2 == 0) position else modes.len - 1 - position];
            var routing_ns: i96 = 0;
            var selection_ns: i96 = 0;
            var scan_ns: i96 = 0;
            var scanned: u64 = 0;
            var frontier_rows: u64 = 0;
            var fallbacks: usize = 0;
            var checksum: u64 = 0;
            for (0..query_count) |qi| {
                const query = queries[qi * dims ..][0..dims];
                // Same fixed frontier for every mode. Source-space mean of
                // stored subgroup representatives is an offline parent proxy,
                // NOT the live HBC routing traversal. This setup is untimed.
                for (leaves.items, 0..) |leaf, li| {
                    const plan = leaf.view.subgroup_plan.?;
                    var score: f64 = 0;
                    for (0..plan.ends.len) |g| score += reps.scalarDot(query, plan.centers[g * dims ..][0..dims]);
                    parent_order[li] = .{ .score = score / @as(f64, @floatFromInt(plan.ends.len)), .id = @intCast(li), .weight = @intCast(leaf.view.count) };
                }
                std.mem.sort(weighted.Entry, parent_order, {}, weighted.Entry.less);
                @memset(frontier, false);
                var work: u64 = 0;
                for (parent_order) |parent| {
                    frontier[parent.id] = true;
                    work += parent.weight;
                    if (work >= (total_rows + 1) / 2) break;
                }
                frontier_rows += work;
                results.items.clearRetainingCapacity();
                const start = now(init.io);
                var eligible: usize = 0;
                if (mode != .control) {
                    const query_scale = reps.quantize(query, qcodes);
                    for (leaves.items, 0..) |leaf, li| {
                        if (!frontier[li]) continue;
                        for (0..leaf.view.subgroup_plan.?.ends.len) |g| {
                            const id = leaf.first_group + g;
                            const group = groups.items[id];
                            entries[eligible] = .{ .id = @intCast(id), .weight = group.weight, .score = @as(f64, @floatFromInt(reps.intDot(qcodes, group.codes))) * query_scale * group.scale };
                            eligible += 1;
                        }
                    }
                }
                const routed = now(init.io);
                if (mode != .control) {
                    const budget = (work * 3 + 3) / 4;
                    if (mode == .sort_predicate) {
                        std.mem.sort(weighted.Entry, entries[0..eligible], {}, weighted.Entry.less);
                        @memset(selected, false);
                        var accepted: u64 = 0;
                        for (entries[0..eligible]) |entry| {
                            selected[entry.id] = true;
                            accepted += entry.weight;
                            if (accepted >= budget) break;
                        }
                    } else {
                        const selection = try weighted.select(entries[0..eligible], selected, budget, null);
                        fallbacks += @intFromBool(selection.fallback);
                    }
                }
                const planned = now(init.io);
                for (leaves.items, 0..) |leaf, li| {
                    if (!frontier[li]) continue;
                    var set = leaf.view.asProto();
                    var sink = Sink{ .results = &results, .ids = leaf.view.member_ids };
                    if (mode == .control) {
                        try quantizer.estimateDistancesTo(&set, approx_queries[qi * dims ..][0..dims], &scratch, null, &sink);
                        scanned += leaf.view.count;
                    } else {
                        const plan = leaf.view.subgroup_plan.?;
                        const chosen = selected[leaf.first_group..][0..plan.ends.len];
                        var ranges: [16]Range = undefined;
                        var range_count: usize = 0;
                        for (chosen, 0..) |keep, g| if (keep) {
                            const range = plan.range(g);
                            scanned += range.end - range.start;
                            if (range_count != 0 and ranges[range_count - 1].end == range.start) {
                                ranges[range_count - 1].end = range.end;
                            } else {
                                ranges[range_count] = .{ .start = range.start, .end = range.end };
                                range_count += 1;
                            }
                        };
                        if (mode == .partition_ranges) {
                            try quantizer.estimateDistancesInRangesTo(&set, approx_queries[qi * dims ..][0..dims], &scratch, null, ranges[0..range_count], &sink);
                        } else {
                            var predicate = Predicate{ .sink = &sink, .ends = plan.ends, .selected = chosen };
                            try quantizer.estimateDistancesTo(&set, approx_queries[qi * dims ..][0..dims], &scratch, null, &predicate);
                        }
                    }
                    sink.flush();
                }
                const finished = now(init.io);
                routing_ns += routed - start;
                selection_ns += planned - routed;
                scan_ns += finished - planned;
                results.sort();
                var hash: u64 = 593;
                for (results.items.items) |item| {
                    hash = (hash ^ item.vector_id) *% 0x100000001b3;
                    hash = (hash ^ @as(u32, @bitCast(item.distance))) *% 0x100000001b3;
                    hash = (hash ^ @as(u32, @bitCast(item.error_bound))) *% 0x100000001b3;
                }
                const expected = reference[qi * results.max_items ..][0..results.max_items];
                if (mode == .sort_predicate and round == 0) {
                    reference_counts[qi] = results.items.items.len;
                    @memcpy(expected[0..results.items.items.len], results.items.items);
                }
                if (mode != .control) {
                    if (reference_counts[qi] != results.items.items.len) return error.CandidateParityFailure;
                    for (results.items.items, expected[0..reference_counts[qi]]) |actual, wanted|
                        if (!std.meta.eql(actual, wanted)) return error.CandidateParityFailure;
                }
                checksum +%= hash;
            }
            const divisor: f64 = @floatFromInt(query_count);
            std.debug.print("{{\"round\":{},\"warmup\":{},\"mode\":\"{s}\",\"dims\":{},\"leaves\":{},\"groups\":{},\"queries\":{},\"vectors_per_query\":{d},\"frontier_vectors_per_query\":{d},\"routing_ns\":{d},\"selection_ns\":{d},\"scan_ns\":{d},\"fallbacks\":{},\"checksum\":{}}}\n", .{ round, round == 0, @tagName(mode), dims, leaves.items.len, groups.items.len, query_count, @as(f64, @floatFromInt(scanned)) / divisor, @as(f64, @floatFromInt(frontier_rows)) / divisor, @as(f64, @floatFromInt(routing_ns)) / divisor, @as(f64, @floatFromInt(selection_ns)) / divisor, @as(f64, @floatFromInt(scan_ns)) / divisor, fallbacks, checksum });
        }
    }
}
