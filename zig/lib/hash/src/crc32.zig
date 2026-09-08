// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! IEEE CRC32 with exactly the std.hash.Crc32 wire semantics. The accelerated
//! path is selected only when the compilation target guarantees ARM CRC;
//! portable slicing-by-eight handles every other target, including x86 (whose
//! SSE4.2 CRC instruction uses a different polynomial). Neither path allocates.
//! The algorithm originated in lib/image's PNG encoder. Keep this module
//! dependency-free so storage formats and other libraries can share it.
const std = @import("std");
const builtin = @import("builtin");

pub const Crc32 = struct {
    crc: u32 = 0xffffffff,

    pub fn init() Crc32 {
        return .{};
    }

    pub fn update(self: *Crc32, bytes: []const u8) void {
        if (comptime builtin.cpu.arch == .aarch64 and std.Target.aarch64.featureSetHas(builtin.cpu.features, .crc)) {
            self.crc = crc32Arm64Update(self.crc, bytes);
        } else {
            self.crc = portableUpdate(self.crc, bytes);
        }
    }

    pub fn final(self: Crc32) u32 {
        return self.crc ^ 0xffffffff;
    }

    pub fn hash(bytes: []const u8) u32 {
        var crc = init();
        crc.update(bytes);
        return crc.final();
    }
};

fn portableUpdate(initial_crc: u32, bytes: []const u8) u32 {
    const tables = comptime crc32SlicingTables();
    var crc = initial_crc;
    var index: usize = 0;

    while (index + 8 <= bytes.len) : (index += 8) {
        crc ^= readU32le(bytes[index .. index + 4]);
        const next = readU32le(bytes[index + 4 .. index + 8]);
        crc =
            tables[7][@as(u8, @truncate(crc))] ^
            tables[6][@as(u8, @truncate(crc >> 8))] ^
            tables[5][@as(u8, @truncate(crc >> 16))] ^
            tables[4][@as(u8, @truncate(crc >> 24))] ^
            tables[3][@as(u8, @truncate(next))] ^
            tables[2][@as(u8, @truncate(next >> 8))] ^
            tables[1][@as(u8, @truncate(next >> 16))] ^
            tables[0][@as(u8, @truncate(next >> 24))];
    }

    while (index < bytes.len) : (index += 1) {
        crc = tables[0][@as(u8, @truncate(crc ^ bytes[index]))] ^ (crc >> 8);
    }
    return crc;
}

fn crc32SlicingTables() [8][256]u32 {
    @setEvalBranchQuota(30000);
    const polynomial: u32 = 0xedb88320;
    var tables: [8][256]u32 = undefined;
    for (0..256) |i| {
        var crc: u32 = @intCast(i);
        for (0..8) |_| {
            crc = if ((crc & 1) != 0) (crc >> 1) ^ polynomial else crc >> 1;
        }
        tables[0][i] = crc;
    }
    for (1..8) |table_index| {
        for (0..256) |i| {
            const previous = tables[table_index - 1][i];
            tables[table_index][i] = (previous >> 8) ^ tables[0][@as(u8, @truncate(previous))];
        }
    }
    return tables;
}

fn readU32le(bytes: []const u8) u32 {
    return @as(u32, bytes[0]) |
        (@as(u32, bytes[1]) << 8) |
        (@as(u32, bytes[2]) << 16) |
        (@as(u32, bytes[3]) << 24);
}

fn readU16le(bytes: []const u8) u16 {
    return @as(u16, bytes[0]) |
        (@as(u16, bytes[1]) << 8);
}

fn readU64le(bytes: []const u8) u64 {
    return @as(u64, bytes[0]) |
        (@as(u64, bytes[1]) << 8) |
        (@as(u64, bytes[2]) << 16) |
        (@as(u64, bytes[3]) << 24) |
        (@as(u64, bytes[4]) << 32) |
        (@as(u64, bytes[5]) << 40) |
        (@as(u64, bytes[6]) << 48) |
        (@as(u64, bytes[7]) << 56);
}

fn crc32Arm64Update(initial_crc: u32, bytes: []const u8) u32 {
    var crc = initial_crc;
    var index: usize = 0;
    while (index + 8 <= bytes.len) : (index += 8) {
        crc = crc32Arm64U64(crc, readU64le(bytes[index .. index + 8]));
    }
    if (index + 4 <= bytes.len) {
        crc = crc32Arm64U32(crc, readU32le(bytes[index .. index + 4]));
        index += 4;
    }
    if (index + 2 <= bytes.len) {
        crc = crc32Arm64U16(crc, readU16le(bytes[index .. index + 2]));
        index += 2;
    }
    if (index < bytes.len) {
        crc = crc32Arm64U8(crc, bytes[index]);
    }
    return crc;
}

fn crc32Arm64U64(crc: u32, value: u64) u32 {
    return asm ("crc32x %[out:w], %[crc:w], %[value]"
        : [out] "=r" (-> u32),
        : [crc] "r" (crc),
          [value] "r" (value),
    );
}

fn crc32Arm64U32(crc: u32, value: u32) u32 {
    return asm ("crc32w %[out:w], %[crc:w], %[value:w]"
        : [out] "=r" (-> u32),
        : [crc] "r" (crc),
          [value] "r" (value),
    );
}

fn crc32Arm64U16(crc: u32, value: u16) u32 {
    return asm ("crc32h %[out:w], %[crc:w], %[value:w]"
        : [out] "=r" (-> u32),
        : [crc] "r" (crc),
          [value] "r" (value),
    );
}

fn crc32Arm64U8(crc: u32, value: u8) u32 {
    return asm ("crc32b %[out:w], %[crc:w], %[value:w]"
        : [out] "=r" (-> u32),
        : [crc] "r" (crc),
          [value] "r" (value),
    );
}

test "native CRC32 agrees with standard across alignment tails and incremental updates" {
    var bytes: [4097]u8 = undefined;
    var random = std.Random.DefaultPrng.init(0x593c32);
    random.random().bytes(&bytes);
    try std.testing.expectEqual(@as(u32, 0xcbf43926), Crc32.hash("123456789"));
    for (0..8) |offset| {
        for (0..257) |len| {
            const data = bytes[offset..][0..len];
            const expected = std.hash.Crc32.hash(data);
            try std.testing.expectEqual(expected, Crc32.hash(data));
            try std.testing.expectEqual(expected, portableUpdate(0xffffffff, data) ^ 0xffffffff);
            var crc = Crc32.init();
            crc.update(data[0 .. len / 3]);
            crc.update(&.{});
            crc.update(data[len / 3 .. len / 2]);
            crc.update(data[len / 2 ..]);
            try std.testing.expectEqual(expected, crc.final());
        }
    }
    try std.testing.expectEqual(std.hash.Crc32.hash(&bytes), Crc32.hash(&bytes));
    var portable = portableUpdate(0xffffffff, bytes[0..1999]);
    portable = portableUpdate(portable, bytes[1999..]);
    try std.testing.expectEqual(std.hash.Crc32.hash(&bytes), portable ^ 0xffffffff);
}

test "native CRC32 throughput microbenchmark" {
    if (builtin.mode != .ReleaseFast) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    const bytes = try alloc.alloc(u8, 1024 * 1024);
    defer alloc.free(bytes);
    var random = std.Random.DefaultPrng.init(0x593);
    random.random().bytes(bytes);
    for (0..3) |round| {
        inline for (.{ std.hash.Crc32, Crc32 }) |Impl| {
            var sum: u32 = 0;
            const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
            for (0..64) |iteration| {
                bytes[0] = @truncate(iteration + round);
                sum +%= Impl.hash(bytes);
            }
            const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - started;
            std.debug.print("crc32 implementation={s} bytes={} elapsed_ns={} checksum={}\n", .{
                @typeName(Impl), bytes.len * 64, elapsed, sum,
            });
        }
    }
}
