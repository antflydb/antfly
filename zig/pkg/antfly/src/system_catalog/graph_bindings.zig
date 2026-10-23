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

//! Resolve graph dependencies inside the metadata mutation's pinned read.
const std = @import("std");
const indexes = @import("../api/indexes.zig");
pub fn indexIdentity(a: std.mem.Allocator, indexes_json: []const u8, name: []const u8) !?@import("cypher").catalog.IndexIdentity {
    var lookup = (try indexes.lookupSingleIndexConfig(a, indexes_json, name)) orelse return null;
    defer lookup.deinit();
    if (indexes.inferIndexType(name, lookup.config) != .graph) return null;
    const identity = (try indexes.indexRuntimeIdentity(a, name, lookup.config)) orelse return null;
    return .{ .incarnation = identity.incarnation, .config_hash = identity.config_hash };
}
