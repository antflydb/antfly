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
pub const Result = enum { unknown, primitive, primitive_items, arguments, element, preserve };
pub const Signature = struct { result: Result = .unknown, name: []const u8, minimum: usize = 1, maximum: usize = 1, repeatable: bool = false };
pub const signatures = [_]Signature{
    .{ .repeatable = true, .result = .arguments, .name = "coalesce", .maximum = std.math.maxInt(usize) },
    .{ .repeatable = true, .result = .primitive, .name = "antfly.documentKey" },
    .{ .repeatable = true, .result = .primitive, .name = "elementId" },
    .{ .repeatable = true, .result = .primitive_items, .name = "labels" },
    .{ .repeatable = true, .result = .primitive, .name = "type" },
    .{ .repeatable = true, .name = "properties" },
    .{ .repeatable = true, .name = "nodes" },
    .{ .repeatable = true, .name = "relationships" },
    .{ .repeatable = true, .result = .primitive, .name = "length" },
    .{ .repeatable = true, .result = .primitive, .name = "size" },
    .{ .repeatable = true, .result = .element, .name = "head" },
    .{ .repeatable = true, .result = .element, .name = "last" },
    .{ .repeatable = true, .result = .primitive, .name = "toLower" },
    .{ .repeatable = true, .result = .primitive, .name = "toUpper" },
    .{ .repeatable = true, .result = .primitive, .name = "abs" },
    .{ .repeatable = true, .result = .primitive_items, .name = "keys" },
    .{ .repeatable = true, .result = .preserve, .name = "reverse" },
    .{ .repeatable = true, .result = .primitive, .name = "trim" },
    .{ .repeatable = true, .result = .primitive, .name = "ltrim" },
    .{ .repeatable = true, .result = .primitive, .name = "rtrim" },
    .{ .repeatable = true, .result = .primitive, .name = "toString" },
    .{ .repeatable = true, .result = .primitive, .name = "toInteger" },
    .{ .repeatable = true, .result = .primitive, .name = "toFloat" },
    .{ .repeatable = true, .result = .primitive, .name = "toBoolean" },
    .{ .repeatable = true, .result = .primitive_items, .name = "range", .minimum = 2, .maximum = 3 },
    .{ .repeatable = true, .result = .primitive, .name = "substring", .minimum = 2, .maximum = 3 },
};
pub fn lookup(name: []const u8) ?Signature {
    for (signatures) |signature| if (std.ascii.eqlIgnoreCase(name, signature.name)) return signature;
    return null;
}
