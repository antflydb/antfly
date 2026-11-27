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
const platform = @import("antfly_platform");
const inkling = @import("architectures/inkling2_checkpoint.zig");
const files = @import("util/c_file.zig");
const safetensors = @import("models/safetensors.zig");

test "Inkling 2 Mini validates the released decoder header and all 48 visits" {
    const allocator = std.testing.allocator;
    const dir = platform.env.getenv("ANTFLY_INKLING2_MODEL_DIR") orelse return error.Inkling2FixtureRequired;
    const config_path = try std.fs.path.join(allocator, &.{ dir, "config.json" });
    defer allocator.free(config_path);
    const config_bytes = try files.readFileMax(allocator, config_path, 64 * 1024);
    defer allocator.free(config_bytes);
    const config = try inkling.Config.parse(allocator, config_bytes);
    defer config.deinit();

    const shard_path = try std.fs.path.join(allocator, &.{ dir, inkling.shard_name });
    defer allocator.free(shard_path);
    var prefix: [8]u8 = undefined;
    try files.readRegionInto(allocator, shard_path, 0, &prefix);
    const header_size = std.mem.readInt(u64, &prefix, .little);
    if (header_size > safetensors.max_header_size) return error.HeaderTooLarge;
    const header_bytes = try files.readRegion(allocator, shard_path, 0, 8 + @as(usize, @intCast(header_size)));
    defer allocator.free(header_bytes);
    try std.testing.expectError(error.UnsupportedDType, safetensors.parseHeader(allocator, header_bytes));
    var decoded = try safetensors.parseHeaderWithOptions(allocator, header_bytes, inkling.header_options);
    defer decoded.header.deinit();
    try inkling.validateDecoder(&decoded.header, config.value.text_config);

    const visits = try config.value.visits();
    var full: u16 = 0;
    var sliding: u16 = 0;
    for (visits, 0..) |visit, n| {
        const weight_layer = if (n < 6) n else if (n < 42) 6 + (n - 6) % 12 else 18 + n - 42;
        const pass = if (n >= 6 and n < 42) (n - 6) / 12 else 0;
        try std.testing.expectEqual(weight_layer, visit.weight_layer);
        try std.testing.expectEqual(pass, visit.pass);
        const group: @TypeOf(visit.group) = if (weight_layer % 6 == 5) .full else .sliding;
        try std.testing.expectEqual(group, visit.group);
        const expected_slot = if (weight_layer % 6 == 5) &full else &sliding;
        try std.testing.expectEqual(expected_slot.*, visit.slot);
        expected_slot.* += 1;
    }
    try std.testing.expectEqual(@as(u16, 8), full);
    try std.testing.expectEqual(@as(u16, 40), sliding);
}
