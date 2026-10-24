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

/// Value equality for immutable memory contracts, including nested owned slices.
/// No serialization, allocation, or pointer-identity assumptions.
pub fn equal(comptime T: type, left: T, right: T) bool {
    return switch (@typeInfo(T)) {
        .pointer => |pointer| if (pointer.size == .slice) blk: {
            if (left.len != right.len) break :blk false;
            for (left, right) |a, b| if (!equal(pointer.child, a, b)) break :blk false;
            break :blk true;
        } else left == right,
        .@"struct" => |structure| blk: {
            inline for (structure.field_names) |name| if (!equal(@FieldType(T, name), @field(left, name), @field(right, name))) break :blk false;
            break :blk true;
        },
        .optional => |optional| if (left) |a| if (right) |b| equal(optional.child, a, b) else false else right == null,
        .array => |array| blk: {
            for (left, right) |a, b| if (!equal(array.child, a, b)) break :blk false;
            break :blk true;
        },
        else => left == right,
    };
}
