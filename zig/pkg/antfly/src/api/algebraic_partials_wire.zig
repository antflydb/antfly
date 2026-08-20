// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub const response_encoding_header = "X-Antfly-Algebraic-Partials-Encoding";
pub const base64_v1 = "base64-v1";

pub fn acceptsBase64V1(value: ?[]const u8) bool {
    return if (value) |encoding| std.ascii.eqlIgnoreCase(encoding, base64_v1) else false;
}

const Base64Partial = struct {
    canonical_axis: []const u8,
    metric: []const u8 = "",
    law: []const u8,
    value_base64: []const u8,
};

const Base64Response = struct {
    partials: []const Base64Partial,
};

const LegacyPartial = struct {
    canonical_axis: []const u8,
    metric: []const u8,
    law: []const u8,
    value: []const u8,
};

/// Converts the binary-safe response produced inside a current node into the
/// legacy response understood by pre-base64 coordinators. The HTTP handler only
/// calls this when the requester did not advertise base64-v1 support; current
/// coordinators always request the binary-safe representation.
pub fn base64V1ToLegacyAlloc(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(Base64Response, alloc, body, .{});
    defer parsed.deinit();

    const legacy = try alloc.alloc(LegacyPartial, parsed.value.partials.len);
    defer alloc.free(legacy);
    var initialized: usize = 0;
    defer for (legacy[0..initialized]) |partial| alloc.free(@constCast(partial.value));

    for (parsed.value.partials, 0..) |partial, i| {
        const decoded_len = try std.base64.standard.Decoder.calcSizeForSlice(partial.value_base64);
        const decoded = try alloc.alloc(u8, decoded_len);
        errdefer alloc.free(decoded);
        try std.base64.standard.Decoder.decode(decoded, partial.value_base64);
        legacy[i] = .{
            .canonical_axis = partial.canonical_axis,
            .metric = partial.metric,
            .law = partial.law,
            .value = decoded,
        };
        initialized += 1;
    }

    return try std.json.Stringify.valueAlloc(alloc, .{ .partials = legacy }, .{});
}

test "algebraic partial wire transcodes base64-v1 for legacy coordinators" {
    const alloc = std.testing.allocator;
    const legacy = try base64V1ToLegacyAlloc(
        alloc,
        "{\"partials\":[{\"canonical_axis\":\"axis\",\"metric\":\"m\",\"law\":\"hll\",\"value_base64\":\"AP+A\"}]}",
    );
    defer alloc.free(legacy);

    const Expected = struct {
        partials: []const struct {
            canonical_axis: []const u8,
            metric: []const u8,
            law: []const u8,
            value: []const u8,
        },
    };
    var parsed = try std.json.parseFromSlice(Expected, alloc, legacy, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.partials.len);
    try std.testing.expectEqualSlices(u8, &.{ 0, 255, 128 }, parsed.value.partials[0].value);
}
