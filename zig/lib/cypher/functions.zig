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

//! Shared scalar signatures for compile-time validation and execution.
const std = @import("std");
pub const Signature = struct { name: []const u8, minimum: usize = 1, maximum: usize = 1, repeatable: bool = false };
pub const signatures = [_]Signature{
    .{ .repeatable = true, .name = "coalesce", .maximum = std.math.maxInt(usize) },
    .{ .repeatable = true, .name = "antfly.documentKey" },
    .{ .repeatable = true, .name = "elementId" },
    .{ .repeatable = true, .name = "labels" },
    .{ .repeatable = true, .name = "type" },
    .{ .repeatable = true, .name = "properties" },
    .{ .repeatable = true, .name = "nodes" },
    .{ .repeatable = true, .name = "relationships" },
    .{ .repeatable = true, .name = "length" },
    .{ .repeatable = true, .name = "size" },
    .{ .repeatable = true, .name = "head" },
    .{ .repeatable = true, .name = "last" },
    .{ .repeatable = true, .name = "toLower" },
    .{ .repeatable = true, .name = "toUpper" },
    .{ .repeatable = true, .name = "abs" },
    .{ .repeatable = true, .name = "keys" },
    .{ .repeatable = true, .name = "reverse" },
    .{ .repeatable = true, .name = "trim" },
    .{ .repeatable = true, .name = "ltrim" },
    .{ .repeatable = true, .name = "rtrim" },
    .{ .repeatable = true, .name = "toString" },
    .{ .repeatable = true, .name = "toInteger" },
    .{ .repeatable = true, .name = "toFloat" },
    .{ .repeatable = true, .name = "toBoolean" },
    .{ .repeatable = true, .name = "range", .minimum = 2, .maximum = 3 },
    .{ .repeatable = true, .name = "substring", .minimum = 2, .maximum = 3 },
};
pub fn lookup(name: []const u8) ?Signature {
    for (signatures) |signature| if (std.ascii.eqlIgnoreCase(name, signature.name)) return signature;
    return null;
}
