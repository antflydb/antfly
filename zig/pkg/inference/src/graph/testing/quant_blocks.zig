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

const std = @import("std");
const Format = @import("../quant_matmul.zig").Format;
pub fn fill(comptime format: Format, trial: usize, raw: []u8) void {
    for (raw, 0..) |*v, i| v.* = @truncate((i * 73 + trial * 131 + 19) ^ (i * trial * 17));
    // Exercise positive/negative, subnormal and non-power-of-two scales.
    const half_scale = [_]u16{ 0x3000, 0xb555, 0x3c01, 0x0001, 0x3555, 0xaaff, 0x0000, 0x8000 };
    const d = half_scale[trial % half_scale.len];
    const m = half_scale[(trial + 3) % half_scale.len];
    switch (format) {
        .q8_k => std.mem.writeInt(u32, raw[0..4], @bitCast(@as(f32, if (trial % 2 == 0) 0.1234567 else -0.25781)), .little),
        .q2_k => {
            std.mem.writeInt(u16, raw[16..18], d, .little);
            std.mem.writeInt(u16, raw[18..20], m, .little);
        },
        .q3_k => std.mem.writeInt(u16, raw[108..110], d, .little),
        .q6_k => std.mem.writeInt(u16, raw[208..210], d, .little),
        else => {
            std.mem.writeInt(u16, raw[0..2], d, .little);
            if (format == .q4_1 or format == .q5_1 or format == .q4_k or format == .q5_k) std.mem.writeInt(u16, raw[2..4], m, .little);
        },
    }
}
