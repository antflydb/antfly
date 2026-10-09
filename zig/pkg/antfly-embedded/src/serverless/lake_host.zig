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

//! Hosts resolve managed credential references; the reader owns returned stores.
const std = @import("std");
const binding = @import("external_source/catalog_binding.zig");
const stores = @import("object_store_support.zig");
const catalog = @import("external_source/lake_catalog/mod.zig");
pub const Binding = binding.Binding;
pub const OpenedObjectStore = stores.OpenedObjectStore;

pub const OpenOptions = struct {
    file_bucket: []const u8 = "antfly",
    resolver: ?Resolver = null,
    catalog_resolver: ?CatalogResolver = null,
    pub const CatalogResolver = struct {
        ptr: *const anyopaque,
        load_fn: *const fn (*const anyopaque, std.mem.Allocator, binding.Binding, catalog.types.Context) anyerror!catalog.types.Table,
    };
    pub const Resolver = struct {
        ptr: *const anyopaque,
        open_fn: *const fn (*const anyopaque, std.mem.Allocator, binding.Binding) anyerror!stores.OpenedObjectStore,
    };
    pub fn open(self: OpenOptions, alloc: std.mem.Allocator, source: binding.Binding) !stores.OpenedObjectStore {
        try source.validateSupported();
        if (self.resolver) |resolver| return resolver.open_fn(resolver.ptr, alloc, source);
        if (source.credential_ref != null) return error.ExternalLakeCredentialRefNotFound;
        return stores.OpenedObjectStore.initRemoteUriWithOptions(alloc, source.source_uri, self.file_bucket, .{ .ensure_bucket = false });
    }

    pub fn resolveCatalog(self: OpenOptions, alloc: std.mem.Allocator, source: binding.Binding, opened: stores.OpenedObjectStore, context: catalog.types.Context) !catalog.types.Table {
        const config = source.catalog orelse return error.InvalidLakeCatalog;
        try config.validate();
        if (config.type == .managed) {
            const managed: catalog.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = source.source_uri, .context = context };
            return managed.load(alloc);
        }
        const resolver = self.catalog_resolver orelse return error.LakeCatalogConnectionRequired;
        return resolver.load_fn(resolver.ptr, alloc, source, context);
    }
};

test "lake host resolver rejects managed credentials without host policy" {
    const source: binding.Binding = .{ .table_id = "events", .format = .parquet, .source_uri = "s3://bucket/events", .schema_fingerprint = "test", .credential_ref = .{ .ref_id = "managed" } };
    try std.testing.expectError(error.ExternalLakeCredentialRefNotFound, (OpenOptions{}).open(std.testing.allocator, source));
}
