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

//! Own decoded records while their exact supporting-function contracts are used.
//! Both point publication and whole-state restore consume this representation.
const std = @import("std");
const routines = @import("routines.zig");
const casts = @import("casts.zig");
pub const Set = struct {
    routine: ?std.json.Parsed(routines.Record) = null,
    cast: ?std.json.Parsed(casts.Record) = null,
    uses: []const routines.FunctionUse,

    pub fn init(a: std.mem.Allocator, resource: anytype) !Set {
        if (resource.kind == .routine) {
            var parsed = try routines.Record.decode(a, resource.source_json);
            errdefer parsed.deinit();
            return .{ .routine = parsed, .uses = try parsed.value.functionUsesAlloc(a) };
        }
        if (resource.kind != .cast) return error.InvalidCatalogRecord;
        var parsed = try casts.Record.decode(a, resource.source_json);
        errdefer parsed.deinit();
        var scratch: [3]routines.Type = undefined;
        const use = parsed.value.definition.functionUse(&scratch);
        const arguments = try a.dupe(routines.Type, use.arguments);
        errdefer a.free(arguments);
        const uses = try a.alloc(routines.FunctionUse, 1);
        uses[0] = .{ .reference = use.reference, .arguments = arguments, .result = use.result };
        return .{ .cast = parsed, .uses = uses };
    }
    pub fn deinit(self: *Set, a: std.mem.Allocator) void {
        routines.Record.freeFunctionUses(a, self.uses);
        if (self.routine) |*record| record.deinit();
        if (self.cast) |*record| record.deinit();
    }
    pub fn id(self: Set) u64 {
        return if (self.routine) |record| record.value.id else self.cast.?.value.id;
    }
};
