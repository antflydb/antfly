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

//! Hosts resolve managed credential references; the reader owns returned stores.
const std = @import("std");
const binding = @import("external_source/catalog_binding.zig");
const stores = @import("object_store_support.zig");
pub const OpenOptions = struct {
    file_bucket: []const u8 = "antfly",
    resolver: ?Resolver = null,
    pub const Resolver = struct {
        ptr: *const anyopaque,
        open_fn: *const fn (*const anyopaque, std.mem.Allocator, binding.Binding) anyerror!stores.OpenedObjectStore,
    };
    pub fn open(self: OpenOptions, alloc: std.mem.Allocator, source: binding.Binding) !stores.OpenedObjectStore {
        try source.validateReadOnlyMvp();
        if (self.resolver) |resolver| return resolver.open_fn(resolver.ptr, alloc, source);
        if (source.credential_ref != null) return error.ExternalLakeCredentialRefNotFound;
        return stores.OpenedObjectStore.initRemoteUriWithOptions(alloc, source.source_uri, self.file_bucket, .{ .ensure_bucket = false });
    }
};

test "lake host resolver rejects managed credentials without host policy" {
    const source: binding.Binding = .{ .table_id = "events", .format = .parquet, .source_uri = "s3://bucket/events", .schema_fingerprint = "test", .credential_ref = .{ .ref_id = "managed" } };
    try std.testing.expectError(error.ExternalLakeCredentialRefNotFound, (OpenOptions{}).open(std.testing.allocator, source));
}
