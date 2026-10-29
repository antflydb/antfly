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

//! Public C symbols are emitted only by the compiled local storage owner.
const std = @import("std");

/// True in consumer builds, where public C symbols live in the storage owner.
pub const forwarding = @import("../storage/engine_selection.zig").enabled;

/// Publish every public `antfly_*` function declared in `namespace`.
pub fn publishAll(comptime namespace: type) void {
    if (forwarding) return;
    const names = @typeInfo(namespace).@"struct".decl_names;
    @setEvalBranchQuota(names.len * 200);
    for (names) |name| {
        if (!std.mem.startsWith(u8, name, "antfly_")) continue;
        if (@typeInfo(@TypeOf(@field(namespace, name))) != .@"fn") continue;
        @export(&@field(namespace, name), .{ .name = name });
    }
}

/// Call the storage owner's copy of public C function `f`, named `name`.
pub inline fn forward(comptime f: anytype, comptime name: []const u8, args: anytype) @typeInfo(@TypeOf(f)).@"fn".return_type.? {
    return @call(.auto, @extern(*const @TypeOf(f), .{ .name = name }), args);
}
