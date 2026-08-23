// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Minimal resolver interface for turning catalog external bindings into
//! manifest publication plans.

const std = @import("std");
const Allocator = std.mem.Allocator;
const catalog_binding = @import("../external_source/catalog_binding.zig");
const external_source_manifest = @import("external_source_manifest.zig");

pub const ResolveRequest = struct {
    namespace: []const u8,
    table_name: []const u8,
    binding: catalog_binding.Binding,
};

pub const Resolver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        resolve: *const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            request: ResolveRequest,
        ) anyerror!?external_source_manifest.Plan,
    };

    pub fn resolveAlloc(
        self: Resolver,
        alloc: Allocator,
        request: ResolveRequest,
    ) !?external_source_manifest.Plan {
        return try self.vtable.resolve(self.ptr, alloc, request);
    }
};

test "external source plan resolver api compiles" {
    _ = ResolveRequest;
    _ = Resolver;
}
