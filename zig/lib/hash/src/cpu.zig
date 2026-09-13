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

//! Only these feature bits are cached, so concurrent first calls may safely
//! repeat detection. No allocation, lock, initialization order, or libc is
//! required. Hosted x86-64 platforms preserve the baseline SSE2 register state;
//! these kernels do not use AVX and therefore need no XGETBV check.
const std = @import("std");
const builtin = @import("builtin");

pub const Features = packed struct(u8) {
    arm_crc: bool = false,
    x86_crc: bool = false,
    pclmul: bool = false,
    _reserved: u4 = 0,
    initialized: bool = true,
};

var cached = std.atomic.Value(u8).init(0);

pub fn features() Features {
    if (comptime builtin.cpu.arch != .aarch64 and builtin.cpu.arch != .x86_64) return .{};
    if (comptime builtin.zig_backend == .stage2_c) return .{};
    if (comptime builtin.os.tag == .freestanding) return guaranteed();
    return featuresCached(&cached);
}

fn featuresCached(cache: *std.atomic.Value(u8)) Features {
    const value = cache.load(.monotonic);
    if (value != 0) return @bitCast(value);
    const detected = detect();
    cache.store(@bitCast(detected), .monotonic);
    return detected;
}

pub fn guaranteed() Features {
    if (comptime builtin.zig_backend == .stage2_c) return .{};
    return switch (builtin.cpu.arch) {
        .aarch64 => .{ .arm_crc = builtin.cpu.has(.aarch64, .crc) },
        .x86_64 => .{
            .x86_crc = builtin.cpu.has(.x86, .crc32),
            .pclmul = builtin.cpu.has(.x86, .pclmul),
        },
        else => .{},
    };
}

fn detect() Features {
    if (comptime builtin.zig_backend == .stage2_c or builtin.os.tag == .freestanding) return guaranteed();
    var result = guaranteed();
    switch (builtin.cpu.arch) {
        .aarch64 => switch (builtin.os.tag) {
            .linux => result.arm_crc = result.arm_crc or fromArmHwcap(linuxHwcap()).arm_crc,
            // Every supported Apple Silicon macOS machine has ARM CRC, even
            // when the caller deliberately compiles with -mcpu=generic.
            .macos => result.arm_crc = true,
            else => {}, // Unsupported OS discovery retains the safe baseline.
        },
        .x86_64 => {
            var eax: u32 = undefined;
            var ebx: u32 = undefined;
            var ecx: u32 = undefined;
            var edx: u32 = undefined;
            asm volatile ("cpuid"
                : [_] "={eax}" (eax),
                  [_] "={ebx}" (ebx),
                  [_] "={ecx}" (ecx),
                  [_] "={edx}" (edx),
                : [_] "{eax}" (@as(u32, 1)),
                  [_] "{ecx}" (@as(u32, 0)),
            );
            const native = fromX86Leaf1(ecx, edx);
            result.x86_crc = result.x86_crc or native.x86_crc;
            result.pclmul = result.pclmul or native.pclmul;
        },
        else => {},
    }
    return result;
}

fn linuxHwcap() usize {
    // libc owns auxv initialization for C-hosted executables and shared libs.
    // Zig owns it for libc-free executables. A libc-free library without a
    // startup-provided auxv simply stays portable.
    if (builtin.link_libc) return std.c.getauxval(std.elf.AT_HWCAP);
    const auxv = std.os.linux.elf_aux_maybe orelse return 0;
    var index: usize = 0;
    while (auxv[index].a_type != std.elf.AT_NULL) : (index += 1) {
        if (auxv[index].a_type == std.elf.AT_HWCAP) return auxv[index].a_un.a_val;
    }
    return 0;
}

fn fromArmHwcap(hwcap: usize) Features {
    return .{ .arm_crc = hwcap & (1 << 7) != 0 }; // Linux AArch64 HWCAP_CRC32.
}

fn fromX86Leaf1(ecx: u32, edx: u32) Features {
    return .{
        .x86_crc = ecx & (1 << 20) != 0, // SSE4.2 CRC32C (not IEEE CRC32).
        .pclmul = ecx & (1 << 1) != 0 and edx & (1 << 26) != 0,
    };
}

test "CPU feature decoding requires the exact optional instruction bits" {
    try std.testing.expect(!fromArmHwcap(0).arm_crc);
    try std.testing.expect(fromArmHwcap(1 << 7).arm_crc);
    try std.testing.expect(!fromX86Leaf1(0, 0).x86_crc);
    try std.testing.expect(fromX86Leaf1(1 << 20, 0).x86_crc);
    try std.testing.expect(!fromX86Leaf1(1 << 20, 1 << 26).pclmul);
    try std.testing.expect(!fromX86Leaf1(1 << 1, 0).pclmul);
    try std.testing.expect(fromX86Leaf1(1 << 1, 1 << 26).pclmul);
    try std.testing.expectEqual(features(), features());
}

test "CPU feature cache is safe during concurrent first use" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const Worker = struct {
        cache: *std.atomic.Value(u8),
        start: *std.Io.Event,
        observed: Features = .{},

        fn run(self: *@This()) void {
            self.start.waitUncancelable(std.testing.io);
            self.observed = featuresCached(self.cache);
        }
    };
    var cache = std.atomic.Value(u8).init(0);
    var start: std.Io.Event = .unset;
    var workers: [8]Worker = undefined;
    var tasks: [8]std.Io.Future(void) = undefined;
    var started: usize = 0;
    errdefer {
        start.set(std.testing.io);
        for (tasks[0..started]) |*task| task.await(std.testing.io);
    }
    for (&workers, &tasks) |*worker, *task| {
        worker.* = .{ .cache = &cache, .start = &start };
        task.* = try std.testing.io.concurrent(Worker.run, .{worker});
        started += 1;
    }
    start.set(std.testing.io);
    for (&tasks) |*task| task.await(std.testing.io);
    started = 0;
    const expected = detect();
    for (workers) |worker| try std.testing.expectEqual(expected, worker.observed);
    try std.testing.expectEqual(expected, featuresCached(&cache));
}
