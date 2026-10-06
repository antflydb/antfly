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

//! Linux x86 kernel selection. No optional instruction executes until both
//! CPU support and OS preservation of XMM/YMM state have been established.
const std = @import("std");
const builtin = @import("builtin");
pub const enabled = builtin.os.tag == .linux and builtin.cpu.arch == .x86_64;
pub const Kernel = enum(u8) { portable = 1, avx2 = 2 };
var cached: std.atomic.Value(u8) = .init(0);

const Cpuid = struct { eax: u32, ebx: u32, ecx: u32, edx: u32 };
fn cpuid(leaf: u32) Cpuid {
    var eax: u32 = undefined;
    var ebx: u32 = undefined;
    var ecx: u32 = undefined;
    var edx: u32 = undefined;
    asm volatile ("cpuid"
        : [_] "={eax}" (eax),
          [_] "={ebx}" (ebx),
          [_] "={ecx}" (ecx),
          [_] "={edx}" (edx),
        : [_] "{eax}" (leaf),
          [_] "{ecx}" (@as(u32, 0)),
    );
    return .{ .eax = eax, .ebx = ebx, .ecx = ecx, .edx = edx };
}

const required_leaf1: u32 = (1 << 12) | (1 << 26) | (1 << 27) | (1 << 28) | (1 << 29);
fn supported(leaf1: u32, leaf7: u32, xcr0: u64) bool {
    return leaf1 & required_leaf1 == required_leaf1 and leaf7 & (1 << 5) != 0 and xcr0 & 6 == 6;
}

pub fn available() bool {
    if (comptime !enabled) return false;
    if (cpuid(0).eax < 7) return false;
    const leaf1 = cpuid(1).ecx;
    // XGETBV itself is illegal unless XSAVE and OSXSAVE are present.
    if (leaf1 & required_leaf1 != required_leaf1) return false;
    var lo: u32 = undefined;
    var hi: u32 = undefined;
    asm volatile ("xgetbv"
        : [_] "={eax}" (lo),
          [_] "={edx}" (hi),
        : [_] "{ecx}" (@as(u32, 0)),
    );
    return supported(leaf1, cpuid(7).ebx, (@as(u64, hi) << 32) | lo);
}

pub fn resolve(name: []const u8, has_avx2: bool) !Kernel {
    if (std.mem.eql(u8, name, "portable")) return .portable;
    if (std.mem.eql(u8, name, "auto")) return if (has_avx2) .avx2 else .portable;
    if (std.mem.eql(u8, name, "avx2")) return if (has_avx2) .avx2 else error.UnsupportedX86Kernel;
    return error.InvalidX86Kernel;
}

pub fn selected() Kernel {
    if (comptime !enabled) return .portable;
    const value = cached.load(.acquire);
    if (value != 0) return @fromBackingInt(@intCast(value));
    const name = if (builtin.link_libc) blk: {
        const raw = std.c.getenv("ANTFLY_INFERENCE_X86_KERNEL") orelse break :blk "auto";
        break :blk std.mem.span(raw);
    } else "auto";
    const result = resolve(name, available()) catch |err| std.debug.panic("ANTFLY_INFERENCE_X86_KERNEL={s}: {s}", .{ name, @errorName(err) });
    // Racing initializers derive the same immutable process policy.
    _ = cached.cmpxchgStrong(0, @backingInt(result), .release, .monotonic);
    return @fromBackingInt(@intCast(cached.load(.acquire)));
}

test "AVX2 requires all CPU features and OS vector state" {
    try std.testing.expect(supported(required_leaf1, 1 << 5, 6));
    inline for (.{ 12, 26, 27, 28, 29 }) |bit|
        try std.testing.expect(!supported(required_leaf1 & ~(@as(u32, 1) << bit), 1 << 5, 6));
    try std.testing.expect(!supported(required_leaf1, 0, 6));
    for ([_]u64{ 0, 2, 4 }) |state| try std.testing.expect(!supported(required_leaf1, 1 << 5, state));
    try std.testing.expectEqual(Kernel.portable, try resolve("auto", false));
    try std.testing.expectEqual(Kernel.avx2, try resolve("auto", true));
    try std.testing.expectEqual(Kernel.portable, try resolve("portable", true));
    try std.testing.expectError(error.UnsupportedX86Kernel, resolve("avx2", false));
    try std.testing.expectError(error.InvalidX86Kernel, resolve("unknown", true));
}

test "x86 kernel selection is stable during concurrent first use" {
    if (comptime !enabled or builtin.single_threaded) return error.SkipZigTest;
    cached.store(0, .release);
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
    defer runtime.deinit();
    var group: std.Io.Group = .init;
    defer group.cancel(runtime.io());
    var results: [16]Kernel = undefined;
    for (&results) |*result| group.async(runtime.io(), struct {
        fn run(out: *Kernel) void {
            out.* = selected();
        }
    }.run, .{result});
    try group.await(runtime.io());
    for (results) |result| try std.testing.expectEqual(results[0], result);
}
