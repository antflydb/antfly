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

//! Exact builtin identity shared by SQL binding and immutable storage schemas.
//! Backing values are durable; append identities, never reorder or reuse them.
//! This module has no query-runtime or native-storage dependencies.
/// A coercion/column modifier, not another builtin type or value identity.
/// For arrays this constrains each NUMERIC element, never dimensions/bounds.
pub const NumericModifier = struct {
    precision: u16,
    scale: i16 = 0,

    pub fn validate(self: NumericModifier) !void {
        if (self.precision < 1 or self.precision > 1000 or self.scale < -1000 or self.scale > 1000)
            return error.SqlInvalidParameterValue;
    }

    pub fn eql(left: ?NumericModifier, right: ?NumericModifier) bool {
        if (left) |a| {
            const b = right orelse return false;
            return a.precision == b.precision and a.scale == b.scale;
        }
        return right == null;
    }

    /// PostgreSQL encodes a signed eleven-bit scale plus its varlena header.
    pub fn postgres(self: NumericModifier) !i32 {
        try self.validate();
        const scale: u16 = @bitCast(self.scale);
        return @intCast((@as(u32, self.precision) << 16 | (scale & 0x7ff)) + 4);
    }
};

pub const Type = enum(u8) {
    text = 0,
    int16 = 1,
    int32 = 2,
    int64 = 3,
    float32 = 4,
    float64 = 5,
    boolean = 6,
    uuid = 7,
    jsonb = 8,
    numeric = 9,

    /// Public enums are generated from OpenAPI; keep durable tags independent
    /// while checking that every generated identity has a storage counterpart.
    pub fn fromWire(value: anytype) Type {
        return switch (value) {
            inline else => |kind| @field(Type, @tagName(kind)),
        };
    }

    pub fn oid(self: Type) u32 {
        return switch (self) {
            .text => 25,
            .int16 => 21,
            .int32 => 23,
            .int64 => 20,
            .float32 => 700,
            .float64 => 701,
            .boolean => 16,
            .uuid => 2950,
            .jsonb => 3802,
            .numeric => 1700,
        };
    }

    pub fn arrayOid(self: Type) u32 {
        return switch (self) {
            .text => 1009,
            .int16 => 1005,
            .int32 => 1007,
            .int64 => 1016,
            .float32 => 1021,
            .float64 => 1022,
            .boolean => 1000,
            .uuid => 2951,
            .jsonb => 3807,
            .numeric => 1231,
        };
    }
};
