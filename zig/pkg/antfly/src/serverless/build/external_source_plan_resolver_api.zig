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

//! Minimal resolver interface for turning catalog external bindings into
//! manifest publication plans.

const std = @import("std");
const Allocator = std.mem.Allocator;
const catalog_binding = @import("antfly_local_sources").serverless_external_source_catalog_binding;
const external_source_manifest = @import("external_source_manifest.zig");

pub const ResolveRequest = struct {
    namespace: []const u8,
    table_name: []const u8,
    binding: catalog_binding.Binding,
    /// Borrowed, request-local publication authority. Discovery must not keep
    /// a shared mutable artifact-store pointer or upload before fencing.
    artifacts: *@import("../artifacts/store.zig").ArtifactStore,
    previous_artifacts: []const @import("antfly_local_sources").serverless_manifest_artifact_ref.ArtifactRef = &.{},
    cancellation: @import("antfly_cancellation").CancellationToken = .none,
};

pub const Resolver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        resolve: *const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            request: ResolveRequest,
        ) anyerror!external_source_manifest.Plan,
    };

    pub fn resolveAlloc(
        self: Resolver,
        alloc: Allocator,
        request: ResolveRequest,
    ) !external_source_manifest.Plan {
        return try self.vtable.resolve(self.ptr, alloc, request);
    }
};

test "external source plan resolver api compiles" {
    _ = ResolveRequest;
    _ = Resolver;
}
