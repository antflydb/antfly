//! Generation-owned ANN hints, never distance certificates or public scores.
const std = @import("std");

pub const View = struct {
    codes: []const i8,
    scales: []const f32,
    dims: usize,

    pub fn score(self: View, group: usize, query: []const i8, query_scale: f32) f32 {
        std.debug.assert(query.len == self.dims and group < self.scales.len);
        const center = self.codes[group * self.dims ..][0..self.dims];
        var sums: @Vector(16, i32) = @splat(0);
        var i: usize = 0;
        while (i + 16 <= query.len) : (i += 16) {
            const q: @Vector(16, i8) = query[i..][0..16].*;
            const c: @Vector(16, i8) = center[i..][0..16].*;
            sums += @as(@Vector(16, i32), q) * @as(@Vector(16, i32), c);
        }
        var total: i32 = @reduce(.Add, sums);
        while (i < query.len) : (i += 1) total += @as(i32, query[i]) * center[i];
        return @as(f32, @floatFromInt(total)) * query_scale * self.scales[group];
    }
};

pub const Owned = struct {
    view: View,

    pub fn init(alloc: std.mem.Allocator, centers: []const f32, dims: usize) !Owned {
        if (dims == 0 or dims > 4096 or centers.len % dims != 0) return error.InvalidCompactSubgroups;
        const codes = try alloc.alloc(i8, centers.len);
        errdefer alloc.free(codes);
        const scales = try alloc.alloc(f32, centers.len / dims);
        errdefer alloc.free(scales);
        for (scales, 0..) |*scale, group| scale.* = try quantize(centers[group * dims ..][0..dims], codes[group * dims ..][0..dims]);
        return .{ .view = .{ .codes = codes, .scales = scales, .dims = dims } };
    }

    pub fn deinit(self: *Owned, alloc: std.mem.Allocator) void {
        alloc.free(self.view.codes);
        alloc.free(self.view.scales);
        self.* = undefined;
    }
};

/// Append-only hint cache, owned by one leased immutable generation. Slots
/// never move, replace entries, or reclaim them while readers can hold views.
/// Warm lookups are acquire loads, not a shared query mutex. Collision/budget
/// exhaustion is an optional-hint miss; callers retain float32 routing.
pub const Cache = struct {
    const Entry = struct { key: usize, owned: Owned };
    const Slot = std.atomic.Value(?*Entry);
    alloc: std.mem.Allocator,
    slots: []Slot,

    pub fn init(alloc: std.mem.Allocator, expected_entries: usize) !Cache {
        const size = try std.math.ceilPowerOfTwo(usize, @max(16, try std.math.mul(usize, expected_entries, 2)));
        const slots = try alloc.alloc(Slot, size);
        for (slots) |*slot| slot.* = .init(null);
        return .{ .alloc = alloc, .slots = slots };
    }

    pub fn deinit(self: *Cache) void {
        for (self.slots) |slot| if (slot.load(.monotonic)) |entry| {
            entry.owned.deinit(self.alloc);
            self.alloc.destroy(entry);
        };
        self.alloc.free(self.slots);
        self.* = undefined;
    }

    fn matches(entry: *Entry, centers: []const f32, dims: usize) bool {
        return entry.key == @intFromPtr(centers.ptr) and entry.owned.view.dims == dims and entry.owned.view.codes.len == centers.len;
    }

    pub fn getOrCreate(self: *Cache, centers: []const f32, dims: usize) !View {
        const key = @intFromPtr(centers.ptr);
        const hash = std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
        var staged: ?*Entry = null;
        defer if (staged) |entry| {
            entry.owned.deinit(self.alloc);
            self.alloc.destroy(entry);
        };
        for (0..@min(16, self.slots.len)) |probe| {
            const slot = &self.slots[(@as(usize, @truncate(hash)) +% probe) & (self.slots.len - 1)];
            if (slot.load(.acquire)) |entry| {
                if (matches(entry, centers, dims)) return entry.owned.view;
                continue;
            }
            if (staged == null) {
                const entry = try self.alloc.create(Entry);
                errdefer self.alloc.destroy(entry);
                entry.* = .{ .key = key, .owned = try Owned.init(self.alloc, centers, dims) };
                staged = entry;
            }
            if (slot.cmpxchgStrong(null, staged.?, .acq_rel, .acquire)) |winner| {
                if (matches(winner.?, centers, dims)) return winner.?.owned.view;
            } else {
                const view = staged.?.owned.view;
                staged = null;
                return view;
            }
        }
        return error.CompactSubgroupCacheFull;
    }
};

pub fn quantize(values: []const f32, codes: []i8) !f32 {
    if (values.len != codes.len or values.len > 4096) return error.InvalidCompactSubgroups;
    var maximum: f32 = 0;
    for (values) |value| {
        if (!std.math.isFinite(value)) return error.InvalidCompactSubgroups;
        maximum = @max(maximum, @abs(value));
    }
    // Divide by maximum before multiplying: reciprocal scale can overflow
    // for valid subnormal representatives. Zero centers keep exact zero hints.
    if (maximum == 0) {
        @memset(codes, 0);
        return 0;
    }
    for (values, codes) |value, *code| code.* = @intFromFloat(@round(@max(-127, @min(127, (value / maximum) * 127))));
    return maximum / 127;
}

fn allocationExercise(alloc: std.mem.Allocator) !void {
    var owned = try Owned.init(alloc, &.{ 1, -1, 0, 0.5, 0, 0 }, 3);
    defer owned.deinit(alloc);
    var query: [3]i8 = undefined;
    const scale = try quantize(&.{ 1, -1, 0 }, &query);
    try std.testing.expectApproxEqAbs(@as(f32, 2), owned.view.score(0, &query, scale), 0.00001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), owned.view.score(1, &query, scale), 0.00001);
}

test "compact subgroup hints handle zero tails overflow and allocation failure" {
    try allocationExercise(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationExercise, .{});
    var codes: [17]i8 = undefined;
    const zero = [_]f32{0} ** 17;
    try std.testing.expectEqual(@as(f32, 0), try quantize(&zero, &codes));
    const values = [_]f32{1} ** 17;
    var owned = try Owned.init(std.testing.allocator, &values, 17);
    defer owned.deinit(std.testing.allocator);
    const scale = try quantize(&values, &codes);
    try std.testing.expectApproxEqAbs(@as(f32, 17), owned.view.score(0, &codes, scale), 0.0001);
    var one: [1]i8 = undefined;
    _ = try quantize(&.{std.math.floatMin(f32)}, &one);
    try std.testing.expectEqual(@as(i8, 127), one[0]);
    try std.testing.expectError(error.InvalidCompactSubgroups, quantize(&.{std.math.inf(f32)}, &one));
}

fn cacheAllocationExercise(alloc: std.mem.Allocator) !void {
    var cache = try Cache.init(alloc, 2);
    defer cache.deinit();
    const centers = [_]f32{ 1, -1, 0, 0.5, 0, 0 };
    const first = try cache.getOrCreate(&centers, 3);
    const second = try cache.getOrCreate(&centers, 3);
    try std.testing.expect(first.codes.ptr == second.codes.ptr);
}

test "compact subgroup cache releases partial allocations and retains immutable views" {
    try cacheAllocationExercise(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, cacheAllocationExercise, .{});
}

test "compact subgroup cache concurrent first readers publish one immutable entry" {
    const alloc = std.testing.allocator;
    var runtime = std.Io.Threaded.init(alloc, .{});
    defer runtime.deinit();
    var cache = try Cache.init(alloc, 1);
    defer cache.deinit();
    var group: std.Io.Group = .init;
    defer group.cancel(runtime.io());
    const centers = [_]f32{ 1, -1, 0, 0.5, 0, 0 };
    var pointers: [16]?[*]const i8 = @splat(null);
    const Work = struct {
        fn run(target: *Cache, values: []const f32, output: *?[*]const i8) void {
            const view = target.getOrCreate(values, 3) catch return;
            output.* = view.codes.ptr;
        }
    };
    for (&pointers) |*output| try group.concurrent(runtime.io(), Work.run, .{ &cache, &centers, output });
    try group.await(runtime.io());
    for (pointers) |pointer| try std.testing.expect(pointer != null and pointer == pointers[0]);
    var published: usize = 0;
    for (cache.slots) |slot| if (slot.load(.acquire) != null) {
        published += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), published);
}
