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

//! Statement-owned callable bindings. Metadata lookup and body preparation occur
//! before row execution; a resolved call contains no catalog or parser callback.
const std = @import("std");
const scalar = @import("scalar.zig");
pub const max_arguments = 100;
pub const Argument = struct { type: scalar.Type, unknown_literal: bool = false };
pub const Resolved = struct {
    id: u64,
    generation: u32,
    arguments: []const scalar.Type,
    output: scalar.Type,
    program: *const scalar.Program,
    strict: bool = false,
};
pub const Aggregate = struct {
    id: u64,
    generation: u32,
    arguments: []const scalar.Type,
    state: scalar.Type,
    output: scalar.Type,
    transition: *const Resolved,
    final: ?*const Resolved = null,
    initial: scalar.Datum = .{},
    const Symbol = struct { id: u64, generation: u32 };
    const Wire = struct { id: u64, generation: u32, transition: Symbol, final: ?Symbol };
    pub fn nativeJsonProjection(self: @This()) Wire {
        return .{ .id = self.id, .generation = self.generation, .transition = .{ .id = self.transition.id, .generation = self.transition.generation }, .final = if (self.final) |function| .{ .id = function.id, .generation = function.generation } else null };
    }
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        return writer.write(self.nativeJsonProjection());
    }
    /// Catalog-owned binding is mandatory; JSON cannot construct executable
    /// function pointers or bypass live prepared-generation authorization.
    pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!@This() {
        _ = a;
        _ = options;
        return error.UnexpectedToken;
    }
    pub fn jsonParseFromValue(a: std.mem.Allocator, value: std.json.Value, options: std.json.ParseOptions) std.json.ParseFromValueError!@This() {
        _ = a;
        _ = value;
        _ = options;
        return error.UnexpectedToken;
    }
};
pub const Resolver = struct {
    ptr: *anyopaque,
    resolve: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const Argument) anyerror!*const Resolved,
    resolve_reference: ?*const fn (*anyopaque, std.mem.Allocator, @import("../system_catalog/routines.zig").FunctionReference, []const Argument) anyerror!*const Resolved = null,
    resolve_operator: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const Argument) anyerror!*const Resolved = null,
    resolve_aggregate: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const Argument) anyerror!?*const Aggregate = null,
};
