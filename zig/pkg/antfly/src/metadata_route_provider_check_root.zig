// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Analyze real metadata route/read-index providers without storage linking.
const std = @import("std");
pub const antfly_sources = @import("source_owner_physical.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
pub const capi_dependencies = @import("capi_dependencies.zig");
const catalog = @import("api/table_catalog.zig");
const services = @import("metadata/service.zig");
const server_mod = @import("metadata/server.zig");
fn check(comptime Service: type, raw: *anyopaque, alloc: *const std.mem.Allocator, budget: *const catalog.RoutingBudget) u8 {
    var result = catalog.metadataRouteWithAdmission(Service, @ptrCast(@alignCast(raw)), alloc.*, "rows", .all_ranges, budget.*) catch return 0;
    defer result.deinit(alloc.*);
    return 1;
}
export fn antfly_check_local_metadata_route(raw: *anyopaque, alloc: *const std.mem.Allocator, budget: *const catalog.RoutingBudget) callconv(.c) u8 {
    return check(services.MetadataService, raw, alloc, budget);
}
export fn antfly_check_http_metadata_route(raw: *anyopaque, alloc: *const std.mem.Allocator, budget: *const catalog.RoutingBudget) callconv(.c) u8 {
    return check(services.MetadataHttpService, raw, alloc, budget);
}
export fn antfly_check_metadata_server_route(raw: *anyopaque, alloc: *const std.mem.Allocator, budget: *const catalog.RoutingBudget) callconv(.c) u8 {
    const server: *server_mod.MetadataServer = @ptrCast(@alignCast(raw));
    const source = catalog.CatalogSource.fromMetadataServer(server);
    var result = source.vtable.validate_route_with_admission.?(source.ptr, alloc.*, "rows", .all_ranges, budget.*) catch return 0;
    defer result.deinit(alloc.*);
    return 1;
}
