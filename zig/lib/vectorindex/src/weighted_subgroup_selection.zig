//! Allocation-free weighted ANN work selection. Scores are hints, NOT bounds.
//! Select the same whole-group prefix as score-descending/id-ascending sorting,
//! but return a membership mask so serving keeps its original insertion order.
const std = @import("std");

pub const Entry = struct {
    score: f64,
    id: u32,
    weight: u32,

    pub fn less(_: void, a: Entry, b: Entry) bool {
        return a.score > b.score or (a.score == b.score and a.id < b.id);
    }
};

pub const Cancellation = struct {
    ptr: *const anyopaque,
    cancelled: *const fn (*const anyopaque) bool,
    fn check(self: ?Cancellation) !void {
        if (self) |token| if (token.cancelled(token.ptr)) return error.Canceled;
    }
};

pub const Result = struct { groups: usize, weight: u64, fallback: bool };

/// Entries are query-owned scratch and are permuted. IDs must be distinct and
/// fit the caller's mask; weights must be positive and scores finite. The mask
/// is undefined on error. Work is bounded by introspective sort fallback;
/// cancellation is checked during validation, partitioning, and mask emission.
pub fn select(entries: []Entry, mask: []bool, budget: u64, cancellation: ?Cancellation) !Result {
    return selectWithLimit(entries, mask, budget, cancellation, 2 * std.math.log2_int(usize, @max(entries.len, 1)) + 1);
}

fn selectWithLimit(entries: []Entry, mask: []bool, budget: u64, cancellation: ?Cancellation, max_partitions: usize) !Result {
    try Cancellation.check(cancellation);
    @memset(mask, false);
    var total: u64 = 0;
    for (entries, 0..) |entry, i| {
        if (i % 256 == 0) try Cancellation.check(cancellation);
        if (entry.id >= mask.len or mask[entry.id] or entry.weight == 0 or !std.math.isFinite(entry.score)) return error.InvalidWeightedSelection;
        mask[entry.id] = true;
        total = try std.math.add(u64, total, entry.weight);
    }
    if (budget > total) return error.InvalidWeightedSelection;
    if (budget == total) return .{ .groups = entries.len, .weight = total, .fallback = false };
    @memset(mask, false);
    if (budget == 0) return .{ .groups = 0, .weight = 0, .fallback = false };
    var lo: usize = 0;
    var hi: usize = entries.len;
    var remaining = budget;
    var partitions: usize = 0;
    var fallback = false;
    var end: usize = undefined;
    while (true) {
        try Cancellation.check(cancellation);
        if (partitions == max_partitions or hi - lo <= 16) {
            // Only the unresolved interval is sorted. The ordinary randomized
            // geometry path does not sort the full directory.
            fallback = hi - lo > 16;
            std.mem.sort(Entry, entries[lo..hi], {}, Entry.less);
            end = lo;
            var weight: u64 = 0;
            while (weight < remaining) : (end += 1) weight += entries[end].weight;
            break;
        }
        partitions += 1;
        var pivots = [_]Entry{ entries[lo], entries[lo + (hi - lo) / 2], entries[hi - 1] };
        std.mem.sort(Entry, &pivots, {}, Entry.less);
        const pivot = pivots[1];
        var better = lo;
        var cursor = lo;
        var worse = hi;
        var better_weight: u64 = 0;
        var steps: usize = 0;
        while (cursor < worse) : (steps += 1) {
            if (steps % 256 == 0) try Cancellation.check(cancellation);
            if (Entry.less({}, entries[cursor], pivot)) {
                better_weight += entries[cursor].weight;
                std.mem.swap(Entry, &entries[cursor], &entries[better]);
                better += 1;
                cursor += 1;
            } else if (Entry.less({}, pivot, entries[cursor])) {
                worse -= 1;
                std.mem.swap(Entry, &entries[cursor], &entries[worse]);
            } else cursor += 1;
        }
        // Distinct IDs make the equal partition exactly one element.
        std.debug.assert(worse == better + 1);
        if (remaining <= better_weight) {
            hi = better;
        } else if (remaining <= better_weight + pivot.weight) {
            end = worse;
            break;
        } else {
            remaining -= better_weight + pivot.weight;
            lo = worse;
        }
    }
    var selected_weight: u64 = 0;
    for (entries[0..end], 0..) |entry, i| {
        if (i % 256 == 0) try Cancellation.check(cancellation);
        mask[entry.id] = true;
        selected_weight += entry.weight;
    }
    return .{ .groups = end, .weight = selected_weight, .fallback = fallback };
}

test "weighted partition matches stable sorted prefix including ties and fallback" {
    var prng = std.Random.DefaultPrng.init(593);
    var source: [257]Entry = undefined;
    for (&source, 0..) |*entry, i| entry.* = .{ .score = @floatFromInt(prng.random().uintLessThan(u8, 7)), .id = @intCast(i), .weight = prng.random().uintLessThan(u32, 250) + 1 };
    for ([_]usize{ 1, 2, 15, 16, 17, 64, 257 }) |count| {
        var sorted = source;
        std.mem.sort(Entry, sorted[0..count], {}, Entry.less);
        var total: u64 = 0;
        for (sorted[0..count]) |entry| total += entry.weight;
        for ([_]u64{ 0, 1, total / 4, total / 2, total - 1, total }) |budget| {
            var expected = [_]bool{false} ** source.len;
            var weight: u64 = 0;
            var end: usize = 0;
            while (weight < budget) : (end += 1) {
                expected[sorted[end].id] = true;
                weight += sorted[end].weight;
            }
            for ([_]usize{ 0, 32 }) |limit| {
                var entries = source;
                var mask: [source.len]bool = undefined;
                const result = try selectWithLimit(entries[0..count], &mask, budget, null, limit);
                try std.testing.expectEqual(weight, result.weight);
                try std.testing.expectEqual(end, result.groups);
                try std.testing.expectEqualSlices(bool, &expected, &mask);
            }
        }
    }
}

test "weighted selection rejects invalid plans and observes cancellation" {
    var mask: [2]bool = undefined;
    var entries = [_]Entry{ .{ .score = 1, .id = 0, .weight = 2 }, .{ .score = 1, .id = 1, .weight = 3 } };
    try std.testing.expectError(error.InvalidWeightedSelection, select(&entries, &mask, 6, null));
    entries[1].id = 0;
    try std.testing.expectError(error.InvalidWeightedSelection, select(&entries, &mask, 1, null));
    entries[1].id = 1;
    entries[1].score = std.math.nan(f64);
    try std.testing.expectError(error.InvalidWeightedSelection, select(&entries, &mask, 1, null));
    entries[1].score = 1;
    entries[1].weight = 0;
    try std.testing.expectError(error.InvalidWeightedSelection, select(&entries, &mask, 1, null));
    entries[1].weight = 3;
    entries[1].id = 2;
    try std.testing.expectError(error.InvalidWeightedSelection, select(&entries, &mask, 1, null));
    entries[1].id = 1;
    const State = struct {
        fn cancelled(_: *const anyopaque) bool {
            return true;
        }
    };
    try std.testing.expectError(error.Canceled, select(&entries, &mask, 1, .{ .ptr = &mask, .cancelled = State.cancelled }));
}

test "weighted selection observes cancellation inside partition work" {
    const State = struct {
        checks: usize = 0,
        fn cancelled(ptr: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
            self.checks += 1;
            return self.checks >= 8;
        }
    };
    var entries: [1024]Entry = undefined;
    var mask: [1024]bool = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .score = @floatFromInt(i % 67), .id = @intCast(i), .weight = 1 };
    var state = State{};
    // Entry + four validation polls + partition-entry + the first two inner
    // partition polls. Cancellation must not wait for complete selection.
    try std.testing.expectError(error.Canceled, select(&entries, &mask, 512, .{ .ptr = &state, .cancelled = State.cancelled }));
    try std.testing.expectEqual(@as(usize, 8), state.checks);
}
