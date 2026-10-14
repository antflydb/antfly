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
pub const Signature = struct { name: []const u8, minimum: usize = 1, maximum: usize = 1 };
pub const signatures = [_]Signature{
    .{ .name = "coalesce", .maximum = std.math.maxInt(usize) },
    .{ .name = "antfly.documentKey" },
    .{ .name = "elementId" },
    .{ .name = "labels" },
    .{ .name = "type" },
    .{ .name = "properties" },
    .{ .name = "nodes" },
    .{ .name = "relationships" },
    .{ .name = "length" },
    .{ .name = "size" },
    .{ .name = "head" },
    .{ .name = "last" },
    .{ .name = "toLower" },
    .{ .name = "toUpper" },
    .{ .name = "abs" },
    .{ .name = "keys" },
    .{ .name = "reverse" },
    .{ .name = "trim" },
    .{ .name = "ltrim" },
    .{ .name = "rtrim" },
    .{ .name = "toString" },
    .{ .name = "toInteger" },
    .{ .name = "toFloat" },
    .{ .name = "toBoolean" },
    .{ .name = "range", .minimum = 2, .maximum = 3 },
    .{ .name = "substring", .minimum = 2, .maximum = 3 },
};
pub fn lookup(name: []const u8) ?Signature {
    for (signatures) |signature| if (std.ascii.eqlIgnoreCase(name, signature.name)) return signature;
    return null;
}
