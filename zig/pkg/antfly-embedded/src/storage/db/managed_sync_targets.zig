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

//! Owned names and incarnation-scoped convergence targets. Keeping this data
//! contract separate lets retained assemblies avoid physical DB imports.
const Allocator = @import("std").mem.Allocator;
const IndexTargetVisibility = @import("types.zig").IndexTargetVisibility;

pub const ManagedSyncTargets = struct {
    full_text_indexes: []const []const u8 = &.{},
    all_indexes: []const []const u8 = &.{},
    // Visibility targets are broader than synchronous wait targets: a record
    // with a missing dependency, or source work that schedules a generated
    // downstream artifact, still advances that index's convergence target.
    // Each identity owns its name so target collection does not need a second
    // parallel allocation solely to manage name lifetime.
    target_identities: []const IndexTargetVisibility = &.{},
    target_scope_known: bool = false,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.full_text_indexes) |name| alloc.free(@constCast(name));
        if (self.full_text_indexes.len > 0) alloc.free(self.full_text_indexes);
        for (self.all_indexes) |name| alloc.free(@constCast(name));
        if (self.all_indexes.len > 0) alloc.free(self.all_indexes);
        for (self.target_identities) |identity| alloc.free(@constCast(identity.index_name));
        if (self.target_identities.len > 0) alloc.free(self.target_identities);
        self.* = undefined;
    }
};
