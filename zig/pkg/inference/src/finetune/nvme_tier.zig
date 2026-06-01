// Copyright 2026 Antfly, Inc.
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

// NVMe spill tier: a file-backed scratch space for evicted LoRA gradients
// and optimizer states that would otherwise overflow host RAM. Standalone
// module with only `std` imports; the caller owns eviction policy and how
// slots map to gradient / optimizer blocks.
//
// Non-goals for this first version: compression, free-slot coalescing,
// asynchronous I/O. All reads and writes are synchronous; we rely on the
// kernel page cache for hit-path performance.

const std = @import("std");

/// A handle to a slab of bytes living on disk.
pub const NvmeSlot = struct {
    /// Byte offset into the backing scratch file.
    offset: u64,
    /// Exact byte length of the payload.
    length: u64,
    /// Unique monotonic id assigned at allocation time. Used for assertions
    /// and checksums.
    id: u64,
};

pub const NvmeTierConfig = struct {
    /// Path of the scratch file. Will be created if missing, truncated on init.
    path: []const u8,
    /// Maximum total scratch bytes. Allocation beyond this returns
    /// `error.NvmeBudgetExceeded`.
    max_bytes: u64 = 8 * 1024 * 1024 * 1024, // 8 GiB default
    /// Align each slot to this many bytes (helps page cache line up).
    slot_alignment: u64 = 4096,
};

pub const NvmeTier = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    file: std.Io.File,
    config: NvmeTierConfig,

    /// Current watermark (next free byte).
    watermark: u64 = 0,

    /// Free list of reclaimed slots, sorted by length desc for first-fit.
    free_slots: std.ArrayList(NvmeSlot),

    /// Next slot id.
    next_id: u64 = 0,

    /// Cumulative byte counters for observability.
    write_bytes_total: u64 = 0,
    read_bytes_total: u64 = 0,

    /// Create the scratch file (truncating any prior contents) and return a
    /// live tier. `config.path` is resolved relative to `dir`. Production
    /// callers pass `std.Io.Dir.cwd()`; tests typically pass `tmp.dir`.
    pub fn init(
        allocator: std.mem.Allocator,
        dir: std.Io.Dir,
        io: std.Io,
        config: NvmeTierConfig,
    ) !NvmeTier {
        if (config.slot_alignment == 0) return error.InvalidAlignment;

        const file = try dir.createFile(io, config.path, .{
            .read = true,
            .truncate = true,
        });
        errdefer file.close(io);

        return NvmeTier{
            .allocator = allocator,
            .io = io,
            .file = file,
            .config = config,
            .watermark = 0,
            .free_slots = .empty,
            .next_id = 0,
            .write_bytes_total = 0,
            .read_bytes_total = 0,
        };
    }

    pub fn deinit(self: *NvmeTier) void {
        self.free_slots.deinit(self.allocator);
        self.file.close(self.io);
    }

    /// Round `value` up to the next multiple of `alignment`. Both are u64;
    /// `alignment` must be non-zero.
    fn alignUp(value: u64, alignment: u64) u64 {
        const rem = value % alignment;
        if (rem == 0) return value;
        return value + (alignment - rem);
    }

    /// Reserve a slot of the given byte length. First tries the free list;
    /// falls back to extending the watermark. Returns `error.NvmeBudgetExceeded`
    /// if the request would exceed `max_bytes`.
    pub fn allocate(self: *NvmeTier, bytes: u64) !NvmeSlot {
        if (bytes == 0) return error.ZeroLengthSlot;

        // First-fit over the free list. The list is not strictly sorted (free
        // just appends), so we scan linearly and pick the first slot large
        // enough. The whole free slot is reused — we do not carve.
        var i: usize = 0;
        while (i < self.free_slots.items.len) : (i += 1) {
            if (self.free_slots.items[i].length >= bytes) {
                const reused = self.free_slots.swapRemove(i);
                const id = self.next_id;
                self.next_id += 1;
                // The caller asked for `bytes`; we expose exactly that length
                // in the returned slot even though the underlying region is
                // `reused.length`. Internal waste is intentional in v1.
                return NvmeSlot{
                    .offset = reused.offset,
                    .length = bytes,
                    .id = id,
                };
            }
        }

        // No reusable slot — extend the watermark.
        const aligned_start = alignUp(self.watermark, self.config.slot_alignment);
        const new_watermark = aligned_start + bytes;
        if (new_watermark > self.config.max_bytes) {
            return error.NvmeBudgetExceeded;
        }

        const id = self.next_id;
        self.next_id += 1;
        self.watermark = new_watermark;
        return NvmeSlot{
            .offset = aligned_start,
            .length = bytes,
            .id = id,
        };
    }

    /// Release a slot back to the free list. The backing file region is NOT
    /// truncated — the space will be reused by a future `allocate`.
    pub fn free(self: *NvmeTier, slot: NvmeSlot) void {
        // Append-only free list; no coalescing in this version.
        self.free_slots.append(self.allocator, slot) catch {
            // If the append OOMs we leak the slot region — acceptable for
            // a scratch tier whose lifetime is bounded by the training run.
            return;
        };
    }

    /// Write `data` into `slot`. `data.len` must equal `slot.length`.
    pub fn write(self: *NvmeTier, slot: NvmeSlot, data: []const u8) !void {
        if (data.len != slot.length) return error.LengthMismatch;
        if (data.len == 0) return;
        try self.file.writePositionalAll(self.io, data, slot.offset);
        self.write_bytes_total += data.len;
    }

    /// Read `slot.length` bytes from `slot` into `out`. `out.len` must equal
    /// `slot.length`.
    pub fn read(self: *NvmeTier, slot: NvmeSlot, out: []u8) !void {
        if (out.len != slot.length) return error.LengthMismatch;
        if (out.len == 0) return;
        const n = try self.file.readPositionalAll(self.io, out, slot.offset);
        if (n != out.len) return error.ShortRead;
        self.read_bytes_total += out.len;
    }

    /// Discard all slots and reset the watermark. Preserves the backing file.
    pub fn reset(self: *NvmeTier) void {
        self.free_slots.clearRetainingCapacity();
        self.watermark = 0;
        self.next_id = 0;
        // Leave cumulative write_bytes_total / read_bytes_total untouched so
        // stats reflect lifetime I/O, not per-epoch activity.
    }

    pub const Stats = struct {
        allocated_bytes: u64,
        free_bytes: u64, // sum of free-list slots
        watermark: u64,
        max_bytes: u64,
        write_bytes_total: u64,
        read_bytes_total: u64,
    };

    pub fn stats(self: *const NvmeTier) Stats {
        var free_bytes: u64 = 0;
        for (self.free_slots.items) |s| free_bytes += s.length;
        const allocated = if (self.watermark >= free_bytes)
            self.watermark - free_bytes
        else
            0;
        return .{
            .allocated_bytes = allocated,
            .free_bytes = free_bytes,
            .watermark = self.watermark,
            .max_bytes = self.config.max_bytes,
            .write_bytes_total = self.write_bytes_total,
            .read_bytes_total = self.read_bytes_total,
        };
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// Helper: spin up a tier rooted in a temporary directory with a small budget.
fn makeTier(
    tmp: *std.testing.TmpDir,
    max_bytes: u64,
    alignment: u64,
) !NvmeTier {
    return NvmeTier.init(testing.allocator, tmp.dir, testing.io, .{
        .path = "scratch.bin",
        .max_bytes = max_bytes,
        .slot_alignment = alignment,
    });
}

test "init creates scratch file and deinit closes it cleanly" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var tier = try makeTier(&tmp, 1 * 1024 * 1024, 64);
    defer tier.deinit();

    // The file must exist under the tmp dir.
    const stat = try tmp.dir.statFile(testing.io, "scratch.bin", .{});
    try testing.expect(stat.kind == .file);

    // No allocations yet.
    const s = tier.stats();
    try testing.expectEqual(@as(u64, 0), s.allocated_bytes);
    try testing.expectEqual(@as(u64, 0), s.watermark);
    try testing.expectEqual(@as(u64, 0), s.write_bytes_total);
    try testing.expectEqual(@as(u64, 0), s.read_bytes_total);
}

test "allocate/write/read round-trip preserves bytes" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var tier = try makeTier(&tmp, 1 * 1024 * 1024, 64);
    defer tier.deinit();

    const slot = try tier.allocate(4);
    try testing.expectEqual(@as(u64, 4), slot.length);

    const payload = [_]u8{ 1, 2, 3, 4 };
    try tier.write(slot, &payload);

    var out: [4]u8 = undefined;
    try tier.read(slot, &out);
    try testing.expectEqualSlices(u8, &payload, &out);

    const s = tier.stats();
    try testing.expectEqual(@as(u64, 4), s.write_bytes_total);
    try testing.expectEqual(@as(u64, 4), s.read_bytes_total);
}

test "two allocations are non-overlapping and stats reflect both" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var tier = try makeTier(&tmp, 1 * 1024 * 1024, 64);
    defer tier.deinit();

    const a = try tier.allocate(100);
    const b = try tier.allocate(200);

    try testing.expect(a.offset != b.offset);
    // b must start past the end of a (respecting alignment).
    try testing.expect(b.offset >= a.offset + a.length);

    // Watermark reflects the aligned end of b.
    const s = tier.stats();
    try testing.expect(s.watermark >= a.length + b.length);
    try testing.expectEqual(s.watermark, s.allocated_bytes); // nothing freed
    try testing.expectEqual(@as(u64, 0), s.free_bytes);
}

test "free then allocate reuses a slot via first-fit" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var tier = try makeTier(&tmp, 1 * 1024 * 1024, 64);
    defer tier.deinit();

    const a = try tier.allocate(128);
    const watermark_after_a = tier.watermark;
    tier.free(a);

    // Requesting <= 128 bytes should pull `a` off the free list, without
    // advancing the watermark.
    const b = try tier.allocate(64);
    try testing.expectEqual(a.offset, b.offset);
    try testing.expectEqual(watermark_after_a, tier.watermark);
    try testing.expect(b.id != a.id); // fresh id
}

test "allocate beyond max_bytes returns NvmeBudgetExceeded" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var tier = try makeTier(&tmp, 4096, 64);
    defer tier.deinit();

    // Fits exactly.
    _ = try tier.allocate(2048);
    // This one pushes past 4096 after alignment and must fail.
    const result = tier.allocate(4096);
    try testing.expectError(error.NvmeBudgetExceeded, result);
}

test "multiple writes over the same slot are idempotent (last writer wins)" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var tier = try makeTier(&tmp, 1 * 1024 * 1024, 64);
    defer tier.deinit();

    const slot = try tier.allocate(8);

    const first = [_]u8{ 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA };
    try tier.write(slot, &first);

    const second = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    try tier.write(slot, &second);

    var out: [8]u8 = undefined;
    try tier.read(slot, &out);
    try testing.expectEqualSlices(u8, &second, &out);
}

test "reset clears free list and watermark" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var tier = try makeTier(&tmp, 1 * 1024 * 1024, 64);
    defer tier.deinit();

    const a = try tier.allocate(256);
    _ = try tier.allocate(256);
    tier.free(a);

    try testing.expect(tier.watermark > 0);
    try testing.expect(tier.free_slots.items.len > 0);

    tier.reset();

    try testing.expectEqual(@as(u64, 0), tier.watermark);
    try testing.expectEqual(@as(usize, 0), tier.free_slots.items.len);

    // After reset we can allocate again starting at offset 0 (possibly aligned).
    const c = try tier.allocate(16);
    try testing.expectEqual(@as(u64, 0), c.offset);
}

test "slot_alignment pads offsets correctly" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var tier = try makeTier(&tmp, 1 * 1024 * 1024, 4096);
    defer tier.deinit();

    const a = try tier.allocate(10);
    const b = try tier.allocate(10);
    try testing.expectEqual(@as(u64, 0), a.offset);
    try testing.expectEqual(@as(u64, 4096), b.offset);
}
