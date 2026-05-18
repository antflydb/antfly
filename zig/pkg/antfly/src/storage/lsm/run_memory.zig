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

const std = @import("std");
const binary_search = @import("binary_search.zig");
const k_way_merge = @import("k_way_merge.zig");
const table_layout = @import("table_layout.zig");

pub fn RunMemoryType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const key_from_value = Table.key_from_value;

    return struct {
        const Self = @This();

        pub const Mutability = enum {
            mutable,
            immutable,
        };

        const RunOrigin = enum {
            mutable,
            immutable,
        };

        const SortedRun = struct {
            min: u32,
            max: u32,
            origin: RunOrigin,
        };

        const sorted_runs_max = 64;

        const SortedRunTracker = struct {
            runs: [sorted_runs_max]SortedRun = undefined,
            runs_count: u8 = 0,

            fn init() SortedRunTracker {
                return .{};
            }

            fn reset(self: *SortedRunTracker) void {
                self.* = .{};
            }

            fn count(self: *const SortedRunTracker) u32 {
                return self.runs_count;
            }

            fn add(self: *SortedRunTracker, run: SortedRun) void {
                if (run.min == run.max) return;
                self.runs[self.runs_count] = run;
                self.runs_count += 1;
            }

            fn addFrontAndPropagateOffset(self: *SortedRunTracker, run: SortedRun) void {
                if (run.min == run.max) return;
                std.debug.assert(self.runs_count + 1 <= self.runs.len);
                std.mem.copyBackwards(SortedRun, self.runs[1 .. self.runs_count + 1], self.runs[0..self.runs_count]);
                self.runs[0] = run;
                self.runs_count += 1;
                for (self.runs[1..self.count()]) |*older| {
                    older.min += @intCast(run.max);
                    older.max += @intCast(run.max);
                }
            }

            fn last(self: *const SortedRunTracker) ?*const SortedRun {
                if (self.count() == 0) return null;
                return &self.runs[self.count() - 1];
            }

            fn mergeContext(self: *const SortedRunTracker, values: []const Value) MergeContext {
                var context = MergeContext{
                    .streams = undefined,
                    .streams_count = 0,
                };

                var stream_idx: u32 = 0;
                for (self.runs[0..self.count()]) |run| {
                    if (run.origin != .immutable) continue;
                    context.streams[stream_idx] = values[run.min..run.max];
                    stream_idx += 1;
                    break;
                }
                for (self.runs[0..self.count()]) |run| {
                    if (run.origin == .immutable) continue;
                    context.streams[stream_idx] = values[run.min..run.max];
                    stream_idx += 1;
                }
                context.streams_count = stream_idx;
                return context;
            }
        };

        const MergeContext = struct {
            streams: [sorted_runs_max][]const Value,
            streams_count: u32,

            fn streamPeek(context: *MergeContext, stream_index: u32) k_way_merge.Pending!?Key {
                const stream = context.streams[stream_index];
                if (stream.len == 0) return null;
                return key_from_value(&stream[0]);
            }

            fn streamPop(context: *MergeContext, stream_index: u32) Value {
                const stream = context.streams[stream_index];
                context.streams[stream_index] = stream[1..];
                return stream[0];
            }
        };

        const MergeIterator = k_way_merge.IteratorType(
            MergeContext,
            Key,
            Value,
            .{
                .streams_max = sorted_runs_max,
                .deduplicate = false,
            },
            key_from_value,
            MergeContext.streamPeek,
            MergeContext.streamPop,
        );

        const DedupSink = struct {
            out: []Value,
            target_index: u32 = 0,
            pending: ?Value = null,

            fn init(out: []Value) DedupSink {
                return .{ .out = out };
            }

            fn push(self: *DedupSink, value: Value) void {
                const pending = self.pending orelse {
                    self.pending = value;
                    return;
                };

                if (key_from_value(&pending) == key_from_value(&value)) {
                    if (Table.usage == .secondary_index) {
                        std.debug.assert(Table.tombstone(&pending) != Table.tombstone(&value));
                        self.pending = null;
                    } else {
                        self.pending = value;
                    }
                } else {
                    self.out[self.target_index] = pending;
                    self.target_index += 1;
                    self.pending = value;
                }
            }

            fn finish(self: *DedupSink) u32 {
                if (self.pending) |pending| {
                    self.out[self.target_index] = pending;
                    self.target_index += 1;
                    self.pending = null;
                }
                return self.target_index;
            }
        };

        alloc: std.mem.Allocator,
        values: []Value,
        count_: u32 = 0,
        mutability: Mutability,
        run_tracker: SortedRunTracker = SortedRunTracker.init(),

        pub fn init(alloc: std.mem.Allocator, mutability: Mutability, capacity: usize) !Self {
            return .{
                .alloc = alloc,
                .values = try alloc.alloc(Value, capacity),
                .mutability = mutability,
            };
        }

        pub fn deinit(self: *Self) void {
            self.alloc.free(self.values);
            self.* = undefined;
        }

        pub fn reset(self: *Self) void {
            self.count_ = 0;
            self.run_tracker.reset();
        }

        pub fn count(self: *const Self) u32 {
            return self.count_;
        }

        pub fn valuesUsed(self: *const Self) []Value {
            return self.values[0..self.count_];
        }

        pub fn put(self: *Self, value: *const Value) void {
            std.debug.assert(self.mutability == .mutable);
            std.debug.assert(self.count() < self.values.len);

            const run_count = self.run_tracker.count();
            if (run_count > 0 and self.run_tracker.runs[run_count - 1].max == self.count()) {
                const expand = self.count() == 0 or
                    key_from_value(&self.values[self.count() - 1]) < key_from_value(value);
                self.run_tracker.runs[run_count - 1].max += @intFromBool(expand);
            }

            self.values[self.count()] = value.*;
            self.count_ += 1;
        }

        pub fn sorted(self: *const Self) bool {
            if (self.count() == 0) return true;
            if (self.run_tracker.count() != 1) return false;
            const last = self.run_tracker.last().?;
            return last.min == 0 and last.max == self.count();
        }

        pub fn keyRangeContains(self: *const Self, key: Key) bool {
            std.debug.assert(self.sorted());
            if (self.count() == 0) return false;
            return self.keyMin() <= key and key <= self.keyMax();
        }

        pub fn keyMin(self: *const Self) Key {
            std.debug.assert(self.sorted());
            return key_from_value(&self.valuesUsed()[0]);
        }

        pub fn keyMax(self: *const Self) Key {
            std.debug.assert(self.sorted());
            const values = self.valuesUsed();
            return key_from_value(&values[values.len - 1]);
        }

        pub fn get(self: *const Self, key: Key) ?*const Value {
            std.debug.assert(self.sorted());
            if (!self.keyRangeContains(key)) return null;
            return binary_search.searchValues(
                Key,
                Value,
                key_from_value,
                self.valuesUsed(),
                key,
                .{ .mode = .upper_bound },
            );
        }

        pub fn sort(self: *Self) void {
            std.debug.assert(self.mutability == .mutable);
            if (!self.sorted()) {
                _ = self.sortSuffixFromOffset(0);
                self.run_tracker.reset();
                self.run_tracker.add(.{
                    .min = 0,
                    .max = self.count(),
                    .origin = .mutable,
                });
            }
        }

        pub fn sortSuffix(self: *Self) void {
            std.debug.assert(self.mutability == .mutable);
            if (self.sorted()) return;

            const offset = if (self.run_tracker.last()) |last| last.max else 0;
            if (offset == self.count()) return;
            const run = self.sortSuffixFromOffset(offset);
            self.run_tracker.add(run);
        }

        fn sortSuffixFromOffset(self: *Self, offset: u32) SortedRun {
            const suffix = self.values[offset..self.count()];
            std.mem.sort(Value, suffix, {}, struct {
                fn lessThan(_: void, a: Value, b: Value) bool {
                    return key_from_value(&a) < key_from_value(&b);
                }
            }.lessThan);

            var sink = DedupSink.init(self.values[offset..self.count()]);
            for (suffix) |value| sink.push(value);
            self.count_ = offset + sink.finish();
            return .{
                .min = offset,
                .max = self.count(),
                .origin = .mutable,
            };
        }

        fn finalizeImmutable(self: *Self) void {
            std.debug.assert(self.mutability == .immutable);
            self.run_tracker.reset();
            self.run_tracker.add(.{
                .min = 0,
                .max = self.count(),
                .origin = .immutable,
            });
        }

        pub fn compact(into_immutable: *Self, from_mutable: *Self) void {
            std.debug.assert(into_immutable.mutability == .immutable);
            std.debug.assert(from_mutable.mutability == .mutable);

            if (from_mutable.sorted()) {
                std.mem.swap([]Value, &from_mutable.values, &into_immutable.values);
                into_immutable.count_ = from_mutable.count();
            } else {
                var merge_context = from_mutable.run_tracker.mergeContext(from_mutable.valuesUsed());
                var merge_iterator = MergeIterator.init(
                    &merge_context,
                    @intCast(merge_context.streams_count),
                    .ascending,
                );
                var dedup = DedupSink.init(into_immutable.values);
                while (merge_iterator.pop() catch unreachable) |value| {
                    dedup.push(value);
                }
                into_immutable.count_ = dedup.finish();
            }

            from_mutable.reset();
            into_immutable.finalizeImmutable();
        }

        pub fn absorb(into_immutable: *Self, from_mutable: *Self) void {
            std.debug.assert(into_immutable.mutability == .immutable);
            std.debug.assert(from_mutable.mutability == .mutable);
            if (from_mutable.count() == 0) return;

            const immutable_count = into_immutable.count();
            const mutable_count = from_mutable.count();
            std.debug.assert(immutable_count + mutable_count <= from_mutable.values.len);

            std.mem.copyForwards(
                Value,
                into_immutable.values[immutable_count .. immutable_count + mutable_count],
                from_mutable.values[0..mutable_count],
            );
            std.mem.swap([]Value, &from_mutable.values, &into_immutable.values);

            from_mutable.run_tracker.addFrontAndPropagateOffset(.{
                .min = 0,
                .max = immutable_count,
                .origin = .immutable,
            });
            from_mutable.count_ = immutable_count + mutable_count;
            into_immutable.compact(from_mutable);
        }
    };
}

const TestHelper = struct {
    pub const TestUsage = enum {
        general,
        secondary_index,
    };

    fn TestTable(comptime test_usage: TestUsage) type {
        return struct {
            pub const Key = u32;
            pub const Value = struct {
                key: Key,
                version: u32,
                tombstone: bool,
            };
            pub const usage = switch (test_usage) {
                .general => table_layout.Usage.general,
                .secondary_index => table_layout.Usage.secondary_index,
            };

            pub fn key_from_value(v: *const Value) Key {
                return v.key;
            }

            pub fn tombstone(v: *const Value) bool {
                return v.tombstone;
            }
        };
    }
};

test "run memory compact deduplicates with last value winning" {
    const Table = TestHelper.TestTable(.general);
    const RunMemory = RunMemoryType(Table);

    var immutable = try RunMemory.init(std.testing.allocator, .immutable, 16);
    defer immutable.deinit();
    var mutable = try RunMemory.init(std.testing.allocator, .mutable, 16);
    defer mutable.deinit();

    mutable.put(&.{ .key = 2, .version = 0, .tombstone = false });
    mutable.put(&.{ .key = 2, .version = 1, .tombstone = false });
    mutable.sortSuffix();

    mutable.put(&.{ .key = 2, .version = 2, .tombstone = false });
    mutable.put(&.{ .key = 2, .version = 3, .tombstone = false });
    mutable.sortSuffix();

    immutable.compact(&mutable);
    try std.testing.expect(immutable.sorted());
    try std.testing.expectEqual(@as(u32, 1), immutable.count());
    try std.testing.expectEqual(@as(u32, 3), immutable.valuesUsed()[0].version);
}

test "run memory absorb preserves immutable precedence on merge" {
    const Table = TestHelper.TestTable(.general);
    const RunMemory = RunMemoryType(Table);

    var immutable = try RunMemory.init(std.testing.allocator, .immutable, 16);
    defer immutable.deinit();
    var mutable = try RunMemory.init(std.testing.allocator, .mutable, 16);
    defer mutable.deinit();

    mutable.put(&.{ .key = 2, .version = 0, .tombstone = false });
    mutable.put(&.{ .key = 4, .version = 0, .tombstone = false });
    mutable.sort();
    immutable.compact(&mutable);

    mutable.put(&.{ .key = 2, .version = 1, .tombstone = false });
    mutable.put(&.{ .key = 5, .version = 0, .tombstone = false });
    mutable.sort();
    immutable.absorb(&mutable);

    try std.testing.expect(immutable.sorted());
    try std.testing.expectEqual(@as(u32, 3), immutable.count());
    const values = immutable.valuesUsed();
    try std.testing.expectEqual(@as(u32, 1), values[0].version);
    try std.testing.expectEqual(@as(u32, 4), values[1].key);
    try std.testing.expectEqual(@as(u32, 5), values[2].key);
}

test "run memory annihilates secondary-index tombstones" {
    const Table = TestHelper.TestTable(.secondary_index);
    const RunMemory = RunMemoryType(Table);

    var immutable = try RunMemory.init(std.testing.allocator, .immutable, 16);
    defer immutable.deinit();
    var mutable = try RunMemory.init(std.testing.allocator, .mutable, 16);
    defer mutable.deinit();

    mutable.put(&.{ .key = 2, .version = 0, .tombstone = false });
    mutable.put(&.{ .key = 2, .version = 0, .tombstone = true });
    mutable.sortSuffix();

    immutable.compact(&mutable);
    try std.testing.expect(immutable.sorted());
    try std.testing.expectEqual(@as(u32, 0), immutable.count());
}
