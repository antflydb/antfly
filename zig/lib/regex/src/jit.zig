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

//! Native W^X consume-chain JIT with shared Unicode predicate helpers. Unsupported operations return to the
//! bounded backtracking VM, retaining all capture and control-flow semantics.
const std = @import("std");
const builtin = @import("builtin");
pub const Context = extern struct {
    text: [*]const u8,
    length: usize,
    position: usize,
    steps: usize,
    pc: u32,
    slow: *const fn (*Context, u32) callconv(.c) u32,
    user: *const anyopaque,
};
pub const Result = enum(u32) { fallback, fail, limit };
const stride = if (builtin.cpu.arch == .aarch64) 192 else 256;
pub const Program = struct {
    memory: []u8,
    states: usize,
    pub fn supported() bool {
        return (builtin.cpu.arch == .aarch64 or builtin.cpu.arch == .x86_64) and
            (builtin.os.tag == .macos or builtin.os.tag == .linux or builtin.os.tag == .windows);
    }
    pub fn compile(states: anytype) ?Program {
        if (comptime !supported()) return null;
        if (states.len == 0 or states.len * stride > 1024 * 1024) return null;
        var consumes: usize = 0;
        for (states) |state| if (state.tag == .consume) {
            consumes += 1;
        };
        if (consumes == 0) return null;
        const bytes = Memory.allocate(states.len * stride) orelse return null;
        var sealed = false;
        defer if (!sealed) Memory.release(bytes);
        @memset(bytes, 0);
        for (states, 0..) |state, i| {
            const block = bytes[i * stride ..][0..stride];
            if (comptime builtin.cpu.arch == .aarch64) arm64(block, i, state) else x86(block, i, state);
        }
        if (!Memory.seal(bytes)) return null;
        sealed = true;
        return .{ .memory = bytes, .states = states.len };
    }
    pub fn deinit(self: *Program) void {
        Memory.release(self.memory);
    }
    pub fn run(self: *const Program, context: *Context) Result {
        std.debug.assert(context.pc < self.states);
        const entry: *const fn (*Context) callconv(.c) u32 = @ptrCast(@alignCast(self.memory.ptr + context.pc * stride));
        return @fromBackingInt(entry(context));
    }
};
const Memory = struct {
    extern "c" fn sys_icache_invalidate(*const anyopaque, usize) void;
    extern "c" fn __clear_cache(*anyopaque, *anyopaque) void;
    extern "kernel32" fn VirtualAlloc(?*anyopaque, usize, u32, u32) callconv(.winapi) ?*anyopaque;
    extern "kernel32" fn VirtualProtect(*anyopaque, usize, u32, *u32) callconv(.winapi) i32;
    extern "kernel32" fn VirtualFree(*anyopaque, usize, u32) callconv(.winapi) i32;
    extern "kernel32" fn FlushInstructionCache(std.os.windows.HANDLE, *const anyopaque, usize) callconv(.winapi) i32;
    extern "kernel32" fn GetCurrentProcess() callconv(.winapi) std.os.windows.HANDLE;
    fn allocate(len: usize) ?[]u8 {
        if (comptime builtin.os.tag == .windows) {
            const address = VirtualAlloc(null, len, 0x3000, 0x04) orelse return null;
            return @as([*]u8, @ptrCast(address))[0..len];
        } else {
            const address = std.c.mmap(null, len, .{ .READ = true, .WRITE = true }, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0);
            if (address == std.c.MAP_FAILED) return null;
            return @as([*]u8, @ptrCast(address))[0..len];
        }
    }
    fn seal(memory: []u8) bool {
        if (comptime builtin.os.tag == .windows) {
            var previous: u32 = undefined;
            return VirtualProtect(memory.ptr, memory.len, 0x20, &previous) != 0 and FlushInstructionCache(GetCurrentProcess(), memory.ptr, memory.len) != 0;
        } else {
            if (std.c.mprotect(@ptrCast(@alignCast(memory.ptr)), memory.len, .{ .READ = true, .EXEC = true }) != 0) return false;
            if (comptime builtin.cpu.arch == .aarch64) {
                if (comptime builtin.os.tag == .macos) sys_icache_invalidate(memory.ptr, memory.len) else __clear_cache(memory.ptr, memory.ptr + memory.len);
            }
            return true;
        }
    }
    fn release(memory: []u8) void {
        if (comptime builtin.os.tag == .windows) {
            _ = VirtualFree(memory.ptr, 0, 0x8000);
        } else _ = std.c.munmap(@ptrCast(@alignCast(memory.ptr)), memory.len);
    }
};
fn word(block: []u8, index: usize, value: u32) void {
    std.mem.writeInt(u32, block[index * 4 ..][0..4], value, .little);
}
fn relative(from: usize, to: usize, bits: comptime_int) u32 {
    const delta = @as(i64, @intCast(to)) - @as(i64, @intCast(from));
    std.debug.assert(@mod(delta, 4) == 0);
    const signed: @Int(.signed, bits) = @intCast(@divExact(delta, 4));
    return @as(@Int(.unsigned, bits), @bitCast(signed));
}
fn arm64(block: []u8, pc: usize, state: anytype) void {
    if (state.tag != .consume) {
        word(block, 0, 0x52800004 | (@as(u32, @intCast(pc)) << 5)); // mov w4, pc
        word(block, 1, 0xb9002004); // str w4, [x0,#32]
        word(block, 2, 0x52800000); // mov w0, fallback
        word(block, 3, 0xd65f03c0); // ret
        return;
    }
    const instructions = [_]u32{
        0xf9400c01, // ldr x1,[x0,#24] (remaining VM steps)
        0xb4000001 | (relative(4, 128, 19) << 5), // cbz x1,limit
        0xd1000421, 0xf9000c01, // sub x1,#1; str steps
        0xf9400802, 0xf9400403, 0xeb03005f, // position, length, compare
        0x54000002 | (relative(28, 120, 19) << 5), // b.hs fail
        0xf9400003, 0x38626864, // text pointer; ldrb w4,[x3,x2]
        0x7102009f, // cmp w4,#128
        0x54000002 | (relative(44, 80, 19) << 5), // b.hs Unicode helper
        0x10000005 | (((160 - 48) >> 2) << 5), // adr x5,ASCII masks
        0x53067c86, 0xf86658a5, 0x9ac424a5, // mask index; load; variable shift
        0x36000005 | (relative(64, 120, 14) << 5), // tbz mask,#0,fail
        0x91000442,                                                        0xf9000802, // increment/store position
        0x14000000 | relative(pc * stride + 76, state.first * stride, 26),
        0xa9bf7bf3, // stp x19,x30,[sp,#-16]!
        0xaa0003f3, // mov x19,x0 (preserve context across helper)
        0x52800001 | (@as(u32, @intCast(pc)) << 5), // mov w1,pc
        0xf9401410, // ldr x16,[x0,#40] (slow predicate)
        0xd63f0200, // blr x16
        0x2a0003e4, // mov w4,w0
        0xaa1303e0, // mov x0,x19
        0xa8c17bf3, // ldp x19,x30,[sp],#16
        0x34000004 | (relative(112, 120, 19) << 5), // cbz w4,fail
        0x14000000 | relative(pc * stride + 116, state.first * stride, 26),
        0x52800020, 0xd65f03c0, // fail; ret
        0x52800040, 0xd65f03c0, // limit; ret
    };
    for (instructions, 0..) |instruction, index| word(block, index, instruction);
    std.mem.writeInt(u64, block[160..168], state.ascii_mask[0], .little);
    std.mem.writeInt(u64, block[168..176], state.ascii_mask[1], .little);
}
const X86 = struct {
    block: []u8,
    at: usize = 0,
    fn emit(self: *X86, bytes: []const u8) void {
        @memcpy(self.block[self.at..][0..bytes.len], bytes);
        self.at += bytes.len;
    }
    fn u32le(self: *X86, value: u32) void {
        std.mem.writeInt(u32, self.block[self.at..][0..4], value, .little);
        self.at += 4;
    }
    fn u64le(self: *X86, value: u64) void {
        std.mem.writeInt(u64, self.block[self.at..][0..8], value, .little);
        self.at += 8;
    }
    fn jump(self: *X86, opcode: []const u8) usize {
        self.emit(opcode);
        const fix = self.at;
        self.u32le(0);
        return fix;
    }
    fn patch(self: *X86, fix: usize, target: i64) void {
        const delta: i32 = @intCast(target - @as(i64, @intCast(fix + 4)));
        std.mem.writeInt(i32, self.block[fix..][0..4], delta, .little);
    }
    fn fallback(self: *X86, pc: usize) void {
        self.emit(&.{ 0x41, 0xc7, 0x42, 0x20 });
        self.u32le(@intCast(pc)); // store pc
        self.emit(&.{ 0x31, 0xc0, 0xc3 }); // xor eax,eax; ret
    }
};
fn x86(block: []u8, pc: usize, state: anytype) void {
    var out = X86{ .block = block };
    out.emit(&.{ 0x49, 0x89, if (builtin.os.tag == .windows) 0xca else 0xfa }); // r10 = context
    if (state.tag != .consume) {
        out.fallback(pc);
        return;
    }
    out.emit(&.{ 0x49, 0x8b, 0x42, 0x18, 0x48, 0x85, 0xc0 });
    const limit = out.jump(&.{ 0x0f, 0x84 });
    out.emit(&.{ 0x48, 0xff, 0xc8, 0x49, 0x89, 0x42, 0x18 });
    out.emit(&.{ 0x4d, 0x8b, 0x42, 0x10, 0x4d, 0x3b, 0x42, 0x08 });
    const end = out.jump(&.{ 0x0f, 0x83 });
    out.emit(&.{ 0x4d, 0x8b, 0x0a, 0x47, 0x0f, 0xb6, 0x1c, 0x01 });
    out.emit(&.{ 0x41, 0x81, 0xfb, 0x80, 0, 0, 0 });
    const unicode_path = out.jump(&.{ 0x0f, 0x83 });
    out.emit(&.{ 0x41, 0x83, 0xfb, 0x40 });
    const low_mask = out.jump(&.{ 0x0f, 0x82 });
    out.emit(&.{ 0x48, 0xba });
    out.u64le(state.ascii_mask[1]);
    const mask_ready = out.jump(&.{0xe9});
    out.patch(low_mask, @intCast(out.at));
    out.emit(&.{ 0x48, 0xba });
    out.u64le(state.ascii_mask[0]);
    out.patch(mask_ready, @intCast(out.at));
    out.emit(&.{ 0x4c, 0x0f, 0xa3, 0xda }); // bt rdx,r11 (modulo 64)
    const absent = out.jump(&.{ 0x0f, 0x83 });
    out.emit(&.{ 0x49, 0xff, 0xc0, 0x4d, 0x89, 0x42, 0x10 });
    const next = out.jump(&.{0xe9});
    out.patch(next, @as(i64, @intCast(state.first)) * stride - @as(i64, @intCast(pc)) * stride);
    out.patch(unicode_path, @intCast(out.at));
    if (comptime builtin.os.tag == .windows) {
        // Incoming rsp is 8 mod 16. Reserve shadow space and preserve context.
        out.emit(&.{ 0x48, 0x83, 0xec, 0x28, 0x4c, 0x89, 0x54, 0x24, 0x20, 0x4c, 0x89, 0xd1, 0xba });
        out.u32le(@intCast(pc));
        out.emit(&.{ 0x41, 0xff, 0x52, 0x28, 0x48, 0x8b, 0x4c, 0x24, 0x20, 0x48, 0x83, 0xc4, 0x28 });
    } else {
        out.emit(&.{ 0x48, 0x83, 0xec, 0x18, 0x4c, 0x89, 0x14, 0x24, 0x4c, 0x89, 0xd7, 0xbe });
        out.u32le(@intCast(pc));
        out.emit(&.{ 0x41, 0xff, 0x52, 0x28, 0x48, 0x8b, 0x3c, 0x24, 0x48, 0x83, 0xc4, 0x18 });
    }
    out.emit(&.{ 0x85, 0xc0 }); // test eax,eax
    const unicode_failed = out.jump(&.{ 0x0f, 0x84 });
    const unicode_next = out.jump(&.{0xe9});
    out.patch(unicode_next, @as(i64, @intCast(state.first)) * stride - @as(i64, @intCast(pc)) * stride);
    out.patch(end, @intCast(out.at));
    out.patch(absent, @intCast(out.at));
    out.patch(unicode_failed, @intCast(out.at));
    out.emit(&.{0xb8});
    out.u32le(1);
    out.emit(&.{0xc3});
    out.patch(limit, @intCast(out.at));
    out.emit(&.{0xb8});
    out.u32le(2);
    out.emit(&.{0xc3});
    std.debug.assert(out.at <= stride);
}
