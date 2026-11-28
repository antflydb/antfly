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

//! Exact scratch/output lifetimes for the existing BF16-input/F32-output
//! dense SiLU FFN. Geometry is a declaration, never provider/source authority.
const std = @import("std");
const storage = @import("../../execution/prepared_storage_admission.zig");

pub const Layout = struct {
    rows: usize,
    hidden: usize,
    intermediate: usize,
    gate_offset: usize,
    up_offset: usize,
    scratch_bytes: usize,
    output_bytes: usize,
    vendor_bytes: usize,
    vendor_workspace_bytes: usize,
    const alignment = @import("buffer.zig").DeviceBuffer.allocation_alignment;

    fn alignBytes(bytes: usize) !usize {
        return (try std.math.add(usize, bytes, alignment - 1)) & ~@as(usize, alignment - 1);
    }
    pub fn init(rows: usize, hidden: usize, intermediate: usize, vendor_bytes: usize) !Layout {
        return initStorage(rows, hidden, intermediate, vendor_bytes, .f32);
    }

    pub fn initCompact(rows: usize, hidden: usize, intermediate: usize) !Layout {
        return initStorage(rows, hidden, intermediate, 0, .bf16);
    }

    fn initStorage(rows: usize, hidden: usize, intermediate: usize, vendor_bytes: usize, comptime intermediate_storage: enum { f32, bf16 }) !Layout {
        if (rows == 0 or rows > 8192 or hidden == 0 or intermediate == 0 or hidden > std.math.maxInt(u32) or intermediate > std.math.maxInt(u32)) return error.InvalidShape;
        const narrow = try std.math.mul(usize, rows, hidden);
        const wide = try std.math.mul(usize, rows, intermediate);
        if (narrow > std.math.maxInt(u32) or wide > std.math.maxInt(u32)) return error.InvalidShape;
        // Normalized BF16 dies after gate AND up; activated BF16 is then
        // written on that same stream. Gate/up remain distinct through SiLU.
        const activation = try alignBytes(try std.math.mul(usize, @max(narrow, wide), 2));
        const gate_bytes = switch (intermediate_storage) {
            .f32 => try alignBytes(try std.math.mul(usize, wide, @sizeOf(f32))),
            .bf16 => try alignBytes(try std.math.mul(usize, @max(narrow, wide), @sizeOf(u16))),
        };
        const up_bytes = if (intermediate_storage == .f32) gate_bytes else try alignBytes(try std.math.mul(usize, wide, @sizeOf(u16)));
        const up = try std.math.add(usize, activation, gate_bytes);
        return .{ .rows = rows, .hidden = hidden, .intermediate = intermediate, .gate_offset = activation, .up_offset = up, .scratch_bytes = try std.math.add(usize, up, up_bytes), .output_bytes = try alignBytes(try std.math.mul(usize, narrow, 4)), .vendor_bytes = try alignBytes(vendor_bytes), .vendor_workspace_bytes = vendor_bytes };
    }
    pub fn requirements(self: Layout, builder: *storage.Builder) !void {
        try builder.append(.{ .backend = .cuda, .class = .workspace, .bytes = self.scratch_bytes });
        for (0..2) |_| try builder.append(.{ .backend = .cuda, .class = .activation, .bytes = self.output_bytes });
        if (self.vendor_bytes != 0) try builder.append(.{ .backend = .cuda, .class = .workspace, .bytes = self.vendor_bytes });
    }
    pub fn matches(self: Layout, rows: usize, hidden: usize, intermediate: usize, vendor_bytes: usize) bool {
        const current = init(rows, hidden, intermediate, vendor_bytes) catch return false;
        return std.meta.eql(self, current);
    }
};

test "dense FFN layout derives original lifetimes and checked physical banks" {
    const small = try Layout.init(1, 1024, 3584, 0);
    try std.testing.expectEqual(@as(usize, 35840), small.scratch_bytes);
    try std.testing.expectEqual(@as(usize, 4096), small.output_bytes);
    const large = try Layout.init(1, 2560, 9216, 0);
    try std.testing.expectEqual(@as(usize, 92160), large.scratch_bytes);
    try std.testing.expectEqual(@as(usize, 10240), large.output_bytes);
    try std.testing.expect(small.gate_offset >= 2 * 3584);
    try std.testing.expect(small.up_offset >= small.gate_offset + 4 * 3584);
    try std.testing.expect(small.scratch_bytes >= small.up_offset + 4 * 3584);
    const compact = try Layout.initCompact(1, 1024, 3584);
    try std.testing.expectEqual(@as(usize, 3 * 3584 * @sizeOf(u16)), compact.scratch_bytes);
    try std.testing.expectEqual(compact.output_bytes, small.output_bytes);
    const compact_narrow = try Layout.initCompact(1, 1024, 512);
    try std.testing.expect(compact_narrow.up_offset - compact_narrow.gate_offset >= 1024 * @sizeOf(u16));
    const vendor = try Layout.init(1, 1024, 3584, 9);
    try std.testing.expectEqual(@as(usize, 256), vendor.vendor_bytes);
    try std.testing.expectEqual(@as(usize, 9), vendor.vendor_workspace_bytes);
    try std.testing.expect(!small.matches(2, 1024, 3584, 0));
    try std.testing.expectError(error.InvalidShape, Layout.init(0, 1024, 3584, 0));
    try std.testing.expectError(error.InvalidShape, Layout.init(1, std.math.maxInt(usize), 2, 0));
    try std.testing.expectError(error.Overflow, Layout.init(1, 1024, 3584, std.math.maxInt(usize)));
}
