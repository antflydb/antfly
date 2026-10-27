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
pub const Resolver = struct {
    ptr: *anyopaque,
    resolve: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const Argument) anyerror!*const Resolved,
};
