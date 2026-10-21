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

//! Logical durable-expression operands, independent of ordered index keys.
//! Payloads borrow a pinned row/schema owner or the invocation's owned region.
//! JSON is an ingress/output representation, never a source of SQL identity.
const std = @import("std");
const layout = @import("../common/sql_array_layout.zig");

pub const Array = struct {
    element_type: layout.Kind,
    bytes: []const u8,

    /// Only canonical bytes admitted by the owning codec and protected by a
    /// pinned owner may enter. This extent check does not authenticate input.
    pub fn view(self: Array) !layout.View {
        return layout.View.openAuthenticated(self.element_type, self.bytes, .{});
    }
};

pub const Value = union(enum) {
    null,
    string: []const u8,
    blob: []const u8,
    boolean: bool,
    datetime: i128,
    integer: i64,
    number: f64,
    numeric: []const u8,
    sql_array: Array,

    /// Explicit transport-neutral scalar adaptation; never parse a value or
    /// infer its SQL type. The source supplies the exact scalar tag/payload.
    pub fn fromScalar(value: anytype) Value {
        return switch (value) {
            inline else => |payload, tag| @unionInit(Value, @tagName(tag), payload),
        };
    }

    /// Ordered-key domains remain independently versioned and scalar-only.
    /// Adding an expression value must not silently widen persistent indexes.
    pub fn scalar(self: Value, comptime Scalar: type) !Scalar {
        return switch (self) {
            .sql_array => error.UnsupportedRelationalValueType,
            inline else => |payload, tag| @unionInit(Scalar, @tagName(tag), payload),
        };
    }
};

test "SQL row values preserve exact scalar operands across explicit key projection" {
    const Scalar = union(enum) {
        null,
        string: []const u8,
        blob: []const u8,
        boolean: bool,
        datetime: i128,
        integer: i64,
        number: f64,
        numeric: []const u8,
    };
    for ([_]Scalar{
        .null,
        .{ .string = "text" },
        .{ .blob = &.{ 0, 255 } },
        .{ .boolean = true },
        .{ .datetime = std.math.minInt(i128) },
        .{ .integer = std.math.maxInt(i64) },
        .{ .number = -0.0 },
        .{ .numeric = &.{ 1, 2, 3, 4 } },
    }) |value| try std.testing.expectEqualDeep(value, try Value.fromScalar(value).scalar(Scalar));
    var bytes: [layout.header_size]u8 = @splat(0);
    bytes[0] = layout.version;
    const array: Value = .{ .sql_array = .{ .element_type = .int64, .bytes = &bytes } };
    try std.testing.expectEqual(@as(u32, 0), (try array.sql_array.view()).count);
    try std.testing.expectError(error.UnsupportedRelationalValueType, array.scalar(Scalar));
}
