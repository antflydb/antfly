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

//! IEEE CRC32 and CRC32C with std-compatible streaming and wire semantics.
//! Compilation-target guarantees bypass discovery. Baseline builds discover
//! optional instructions once, and unsupported platforms remain portable.
const std = @import("std");
const builtin = @import("builtin");
const cpu = @import("cpu.zig");
const portable = @import("portable_crc.zig");
const arm = @import("arm_crc.zig");
const x86_ieee = @import("x86_crc32.zig");
const x86_castagnoli = @import("x86_crc32c.zig");

// Zig 0.16's non-LLVM x86 backend cannot encode either PCLMULQDQ or
// CRC32 r64,r64. Keep those kernels out of semantic analysis in Debug builds
// using that backend; LLVM release builds retain hardware acceleration.
const asm_kernels_supported = builtin.zig_backend != .stage2_c and builtin.zig_backend != .stage2_x86_64;

pub const Crc32 = Crc(false);
pub const Crc32c = Crc(true);
pub const Implementation = enum { slicing_by_eight, arm_crc, x86_pclmul, x86_crc };

fn Crc(comptime castagnoli: bool) type {
    return struct {
        const Self = @This();
        const polynomial: u32 = if (castagnoli) 0x82f63b78 else 0xedb88320;
        crc: u32 = 0xffffffff,

        pub fn init() Self {
            return .{};
        }

        /// Available bulk kernel. Short IEEE buffers use the portable path
        /// even when PCLMUL is available, avoiding folding setup overhead.
        pub fn implementation() Implementation {
            const baseline = comptime select(cpu.guaranteed());
            if (comptime baseline != .slicing_by_eight) return baseline;
            return select(cpu.features());
        }

        fn select(features: cpu.Features) Implementation {
            if (comptime !asm_kernels_supported) return .slicing_by_eight;
            return switch (builtin.cpu.arch) {
                .aarch64 => if (features.arm_crc) .arm_crc else .slicing_by_eight,
                .x86_64 => if (castagnoli)
                    (if (features.x86_crc) .x86_crc else .slicing_by_eight)
                else
                    (if (features.pclmul) .x86_pclmul else .slicing_by_eight),
                else => .slicing_by_eight,
            };
        }

        pub fn update(self: *Self, bytes: []const u8) void {
            if (bytes.len == 0) return;
            if (@inComptime()) {
                self.updatePortable(bytes);
                return;
            }
            if (comptime asm_kernels_supported) {
                if (comptime builtin.cpu.arch == .aarch64) {
                    if (implementation() == .arm_crc) {
                        self.crc = arm.update(castagnoli, self.crc, bytes);
                        return;
                    }
                } else if (comptime builtin.cpu.arch == .x86_64) {
                    if (comptime castagnoli) {
                        if (implementation() == .x86_crc) {
                            self.crc = x86_castagnoli.update(self.crc, bytes);
                            return;
                        }
                    } else if (bytes.len >= 64 and implementation() == .x86_pclmul) {
                        const folded_len = bytes.len & ~@as(usize, 15);
                        self.crc = x86_ieee.update(self.crc, bytes[0..folded_len]);
                        self.updatePortable(bytes[folded_len..]);
                        return;
                    }
                }
            }
            self.updatePortable(bytes);
        }

        fn updatePortable(self: *Self, bytes: []const u8) void {
            self.crc = portable.update(u32, polynomial, self.crc, bytes);
        }

        pub fn final(self: Self) u32 {
            return self.crc ^ 0xffffffff;
        }

        pub fn hash(bytes: []const u8) u32 {
            var crc = init();
            crc.update(bytes);
            return crc.final();
        }
    };
}

test "CRC32 and CRC32C preserve known vectors and comptime hashing" {
    try std.testing.expectEqual(@as(u32, 0xcbf43926), Crc32.hash("123456789"));
    try std.testing.expectEqual(@as(u32, 0xe3069283), Crc32c.hash("123456789"));
    try std.testing.expectEqual(@as(u32, 0xcbf43926), comptime Crc32.hash("123456789"));
    try std.testing.expectEqual(@as(u32, 0xe3069283), comptime Crc32c.hash("123456789"));
    try std.testing.expectEqual(@as(u32, 0), Crc32.hash(""));
    try std.testing.expectEqual(@as(u32, 0), Crc32c.hash(""));
    // SSE4.2 alone must never select IEEE acceleration.
    try std.testing.expectEqual(Implementation.slicing_by_eight, Crc32.select(.{ .x86_crc = true }));
    try std.testing.expectEqual(Implementation.slicing_by_eight, Crc32.select(.{}));
    try std.testing.expectEqual(Implementation.slicing_by_eight, Crc32c.select(.{}));
}

test "CRC dispatch selects the available bulk kernels" {
    const available = cpu.features();
    try std.testing.expectEqual(Crc32.select(available), Crc32.implementation());
    try std.testing.expectEqual(Crc32c.select(available), Crc32c.implementation());
    std.debug.print("CRC dispatch target={s} crc32={s} crc32c={s}\n", .{
        builtin.cpu.model.name, @tagName(Crc32.implementation()), @tagName(Crc32c.implementation()),
    });
}

test "CRC32 kernels agree across unaligned buffers tails and incremental updates" {
    var bytes: [65536 + 32]u8 = undefined;
    var random = std.Random.DefaultPrng.init(0x593c32);
    random.random().bytes(&bytes);
    inline for (.{ false, true }) |castagnoli| {
        const Impl = Crc(castagnoli);
        const Oracle = if (castagnoli) std.hash.crc.Crc32Iscsi else std.hash.Crc32;
        for (0..32) |offset| {
            for (0..513) |len| try check(Impl, Oracle, bytes[offset..][0..len]);
            for ([_]usize{ 1023, 1024, 1031, 4095, 4096, 16383, 16384, 65535, 65536 }) |len| {
                try check(Impl, Oracle, bytes[offset..][0..len]);
            }
        }
        for (0..256) |split| {
            const data = bytes[1..1028];
            var crc = Impl.init();
            crc.update(data[0..split]);
            crc.update(data[split..]);
            try std.testing.expectEqual(Oracle.hash(data), crc.final());
        }
    }
}

fn check(comptime Impl: type, comptime Oracle: type, data: []const u8) !void {
    const expected = Oracle.hash(data);
    try std.testing.expectEqual(expected, Impl.hash(data));
    var fast = Impl.init();
    var slow = Impl.init();
    const split = data.len / 3;
    fast.update(data[0..split]);
    fast.update(&.{});
    fast.update(data[split..]);
    slow.updatePortable(data[0..split]);
    slow.updatePortable(&.{});
    slow.updatePortable(data[split..]);
    try std.testing.expectEqual(expected, fast.final());
    try std.testing.expectEqual(expected, slow.final());
    // final is non-mutating and both states remain usable after it.
    fast.update("suffix");
    slow.updatePortable("suffix");
    var oracle = Oracle.init();
    oracle.update(data);
    oracle.update("suffix");
    try std.testing.expectEqual(oracle.final(), fast.final());
    try std.testing.expectEqual(oracle.final(), slow.final());
}
