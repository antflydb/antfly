// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Format-cost certificate for the fixed completion maintenance encoder.
//! Every quantity is additive, including tombstones and mutually exclusive
//! outcomes. Overwrites never refund capacity before a new durable baseline.
const std = @import("std");

pub const block_bytes = 32 * 1024;
pub const fixed_file_bytes = 256;
pub const record_metadata_bytes = 256;

fn add(a: u64, b: u64) !u64 {
    return std.math.add(u64, a, b) catch error.UnsupportedCompletionProfile;
}
fn mul(a: u64, b: u64) !u64 {
    return std.math.mul(u64, a, b) catch error.UnsupportedCompletionProfile;
}

pub const Cost = struct {
    records: u64 = 0,
    encoded_bytes: u64 = 0,
    metadata_bytes: u64 = 0,
    block_weight: u64 = 0,
    max_key_bytes: u64 = 0,
    max_record_bytes: u64 = 0,

    pub fn record(namespace_bytes: usize, key_bytes: usize, value_bytes: usize) !Cost {
        const key = try add(namespace_bytes, key_bytes);
        const encoded = try add(try add(13, key), value_bytes);
        return .{
            .records = 1,
            .encoded_bytes = encoded,
            // Per entry: a packed u16 offset; at worst a separate block
            // containing 60 fixed metadata bytes, two key bounds and two
            // Bloom headers. The 14-bit filter rounds to a power of two:
            // fewer than four bytes/key, with an eight-byte minimum. The
            // maintenance encoder has no prefix hashes or hash-slot array.
            .metadata_bytes = try add(record_metadata_bytes, try mul(2, key)),
            .block_weight = @min(encoded, block_bytes),
            .max_key_bytes = key,
            .max_record_bytes = encoded,
        };
    }

    pub fn plus(a: Cost, b: Cost) !Cost {
        return .{
            .records = try add(a.records, b.records),
            .encoded_bytes = try add(a.encoded_bytes, b.encoded_bytes),
            .metadata_bytes = try add(a.metadata_bytes, b.metadata_bytes),
            .block_weight = try add(a.block_weight, b.block_weight),
            .max_key_bytes = @max(a.max_key_bytes, b.max_key_bytes),
            .max_record_bytes = @max(a.max_record_bytes, b.max_record_bytes),
        };
    }

    pub fn repeated(self: Cost, count: u64) !Cost {
        return .{
            .records = try mul(self.records, count),
            .encoded_bytes = try mul(self.encoded_bytes, count),
            .metadata_bytes = try mul(self.metadata_bytes, count),
            .block_weight = try mul(self.block_weight, count),
            .max_key_bytes = if (count == 0) 0 else self.max_key_bytes,
            .max_record_bytes = if (count == 0) 0 else self.max_record_bytes,
        };
    }
};

pub const Limits = struct {
    metadata_bytes: u64 = 1024 * 1024,
    file_bytes: u64 = std.math.maxInt(u32),
    outputs: u64 = 64,
    additional_runs: u64 = 4,
    frontier_bytes: u64 = 16 * 1024 * 1024,
    sequential_block_bytes: u64 = 32,
};

pub const Certificate = struct {
    outputs: u64,
    blocks: u64,
    frontier_bytes: u64,
    // Two bounds per output remain live until the entire merge finishes.
    retained_boundary_bytes: u64,
};

/// Reserve monotonic native identifiers for every cohort drain and its next
/// maintenance, before accepting any obligation. All checks precede I/O.
pub fn nativeCounterHeadroom(manifest_sequence: u64, next_run_id: u64, wal_segment: u64, cells: u64) !void {
    _ = try add(manifest_sequence, try add(cells, 2));
    _ = try add(next_run_id, try add(65, cells));
    // At most 128 foreground records, prepare + outcome per cell, and the
    // protected checkpoint exclusion marker (current_segment + 1).
    _ = try add(wal_segment, try add(129, try mul(2, cells)));
}

pub fn sharedCounterHeadroom(value: u64, increment: u64, cells: u64) !void {
    _ = try add(value, try mul(increment, cells));
}

pub fn certify(cost: Cost, limits: Limits) !Certificate {
    if (cost.records == 0) return .{ .outputs = 0, .blocks = 0, .frontier_bytes = 0, .retained_boundary_bytes = 0 };
    const largest_metadata = try add(record_metadata_bytes, try mul(2, cost.max_key_bytes));
    const metadata_overhead = try add(fixed_file_bytes, largest_metadata);
    const data_overhead = try add(try add(limits.metadata_bytes, fixed_file_bytes), cost.max_record_bytes);
    if (limits.metadata_bytes <= metadata_overhead or limits.file_bytes <= data_overhead)
        return error.UnsupportedCompletionProfile;
    // If greedy output closes before the next record, either its additive
    // metadata cost exceeds M-fixed-largest_record, or its logical data cost
    // exceeds F-M-fixed-largest_record. Charge each closed file to one reason.
    // Compression cannot increase these costs: adaptive encoding falls back
    // to raw blocks. Logical u32 limits are covered by the stricter file bound.
    const outputs = try add(1, try add(
        cost.metadata_bytes / (limits.metadata_bytes - metadata_overhead),
        cost.encoded_bytes / (limits.file_bytes - data_overhead),
    ));
    if (outputs > limits.outputs) return error.UnsupportedCompletionProfile;
    const runs = try add(outputs, limits.additional_runs);
    // Two adjacent greedily packed blocks contain >32KiB of weighted bytes;
    // oversized records count as 32KiB. File boundaries add at most one block
    // each. Include the protected cohort's additional SSTs before maintenance.
    const blocks = try add(runs, try mul(2, cost.block_weight) / block_bytes);
    const block_size = @max(block_bytes, cost.max_record_bytes);
    const frontier = try add(try mul(blocks, limits.sequential_block_bytes), try mul(try mul(runs, 2), block_size));
    if (frontier > limits.frontier_bytes) return error.UnsupportedCompletionProfile;
    return .{
        .outputs = outputs,
        .blocks = blocks,
        .frontier_bytes = frontier,
        .retained_boundary_bytes = try mul(try mul(outputs, 2), cost.max_key_bytes),
    };
}

test "completion capacity certificate charges all accepted growth and checked arithmetic" {
    const one = try Cost.record(4, 12, 100);
    const baseline = try one.repeated(1000);
    const future = try baseline.plus(try one.repeated(1024));
    try std.testing.expect((try certify(future, .{})).frontier_bytes >= (try certify(baseline, .{})).frontier_bytes);
    try std.testing.expectEqual(@as(u64, 2024), future.records);
    try std.testing.expectError(error.UnsupportedCompletionProfile, (Cost{ .records = std.math.maxInt(u64) }).plus(one));
    try std.testing.expectError(error.UnsupportedCompletionProfile, one.repeated(std.math.maxInt(u64)));
    try std.testing.expectError(error.UnsupportedCompletionProfile, Cost.record(std.math.maxInt(usize), 1, 0));
}

test "completion capacity certificate rejects output and future frontier exhaustion independently" {
    const small = try (try Cost.record(0, 8, 1)).repeated(10000);
    try std.testing.expectError(error.UnsupportedCompletionProfile, certify(small, .{ .outputs = 1 }));
    const large = try (try Cost.record(0, 8, 200 * 1024)).repeated(1000);
    try std.testing.expectError(error.UnsupportedCompletionProfile, certify(large, .{ .frontier_bytes = 1024 * 1024 }));
    try std.testing.expect((try certify(large, .{})).outputs <= 64);
    try std.testing.expectError(error.UnsupportedCompletionProfile, certify(try Cost.record(0, 600 * 1024, 0), .{}));
}

test "completion capacity certificate accounts physical splitting without a total database byte cap" {
    const one = try Cost.record(0, 1, 16 * 1024);
    const cost = try one.repeated(20000);
    const proof = try certify(cost, .{});
    try std.testing.expect(cost.encoded_bytes > 256 * 1024 * 1024);
    try std.testing.expect(proof.outputs <= 64);
    const small_files = try certify(try one.repeated(100), .{ .file_bytes = 2 * 1024 * 1024 });
    try std.testing.expect(small_files.outputs > 1);
}

test "completion capacity certificate reserves cohort counter successors before acceptance" {
    const max = std.math.maxInt(u64);
    try nativeCounterHeadroom(max - 6, max - 69, max - 137, 4);
    try std.testing.expectError(error.UnsupportedCompletionProfile, nativeCounterHeadroom(max - 5, 1, 1, 4));
    try std.testing.expectError(error.UnsupportedCompletionProfile, nativeCounterHeadroom(1, max - 68, 1, 4));
    try std.testing.expectError(error.UnsupportedCompletionProfile, nativeCounterHeadroom(1, 1, max - 136, 4));
    try sharedCounterHeadroom(max - 40, 10, 4);
    try std.testing.expectError(error.UnsupportedCompletionProfile, sharedCounterHeadroom(max - 39, 10, 4));
}
