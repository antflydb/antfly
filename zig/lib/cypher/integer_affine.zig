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

//! Checked integer affine lineage. Keeping the dependency even when its
//! coefficient cancels preserves the domain and errors of intermediate nodes.
const std = @import("std");
pub const Form = struct {
    field: ?usize = null,
    scale: i64 = 0,
    offset: i64 = 0,
    direct: bool = false,
    pub fn column(field: usize) Form {
        return .{ .field = field, .scale = 1, .direct = true };
    }
    pub fn literal(value: i64) Form {
        return .{ .offset = value };
    }
    pub fn negative(self: Form) ?Form {
        return .{ .field = self.field, .scale = std.math.negate(self.scale) catch return null, .offset = std.math.negate(self.offset) catch return null };
    }
    pub fn combine(left: Form, right: Form, op: enum { add, subtract, multiply }) ?Form {
        if (left.field != null and right.field != null and left.field.? != right.field.?) return null;
        const field = left.field orelse right.field;
        return switch (op) {
            .add => .{ .field = field, .scale = std.math.add(i64, left.scale, right.scale) catch return null, .offset = std.math.add(i64, left.offset, right.offset) catch return null },
            .subtract => .{ .field = field, .scale = std.math.sub(i64, left.scale, right.scale) catch return null, .offset = std.math.sub(i64, left.offset, right.offset) catch return null },
            .multiply => blk: {
                if (left.field != null and right.field != null) return null;
                const variable = if (left.field != null) left else right;
                const constant = if (left.field != null) right.offset else left.offset;
                break :blk .{ .field = field, .scale = std.math.mul(i64, variable.scale, constant) catch return null, .offset = std.math.mul(i64, variable.offset, constant) catch return null };
            },
        };
    }
    /// A one-sign argument preserves Cypher SUM's intermediate overflow.
    pub fn oneSign(self: Form) bool {
        return (self.scale >= 0 and self.offset >= 0) or (self.scale <= 0 and self.offset <= 0);
    }
};

test "affine lineage retains cancelled dependencies and checked intermediates" {
    const x = Form.column(3);
    const cancelled = Form.combine(x, x, .subtract).?;
    try std.testing.expectEqual(@as(?usize, 3), cancelled.field);
    try std.testing.expectEqual(@as(i64, 0), cancelled.scale);
    try std.testing.expect(!cancelled.direct);
    try std.testing.expect(Form.combine(x, Form.column(4), .add) == null);
    try std.testing.expect(Form.combine(Form.literal(std.math.maxInt(i64)), Form.literal(1), .add) == null);
    try std.testing.expect(Form.literal(std.math.minInt(i64)).negative() == null);
}
