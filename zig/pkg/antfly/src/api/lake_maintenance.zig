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

//! Admin maintenance contract. Stable operation IDs resolve saved catalog
//! intents; defaults plan only. File vacuum requires an explicit ownership and
//! external-reader retention contract, independent of ordinary table writes.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.serverless_external_source_mod.lake_catalog;
const configured = @import("../serverless/configured_object_store_support.zig");
const ingestion = @import("../serverless/lake_ingestion.zig");
const overlay = @import("lake_search_overlay.zig");
const server_api = @import("http_server.zig");
const A = std.mem.Allocator;
const Request = struct { action: enum { compact, vacuum, wal_gc }, operation_id: []const u8, dry_run: bool = true, exclusive_ownership: bool = false, max_rows: u64 = 16384, max_bytes: u64 = 32 * 1024 * 1024, retain_ms: u64 = 7 * 24 * 60 * 60 * 1000, keep_latest: usize = 2, max_deleted: usize = 4096 };
pub const Result = struct { body: []u8, mutated: bool };
pub fn execute(a: A, server: *server_api.ApiHttpServer, table: local.common_topology_records.TableRecord, binding: local.serverless_external_source_catalog_binding.Binding, options: configured.BindingObjectStoreOpenOptions, context: catalog.types.Context, request_context: local.api_operation.RequestContext, body: []const u8) !Result {
    if (body.len > 16384) return error.InvalidLakeMaintenanceLimits;
    var parsed = try std.json.parseFromSlice(Request, a, body, .{});
    defer parsed.deinit();
    const request = parsed.value;
    if (request.operation_id.len == 0 or request.operation_id.len > 256) return error.InvalidLakeMaintenanceLimits;
    if (request.action == .compact) {
        const result = try @import("../serverless/lake_compaction.zig").run(a, binding, options, context, .{ .operation_id = request.operation_id, .dry_run = request.dry_run, .max_rows = request.max_rows, .max_bytes = request.max_bytes });
        return .{ .body = try std.json.Stringify.valueAlloc(a, result, .{}), .mutated = result.committed };
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var current = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer current.deinit(a);
    const root = try catalog.metadata.parse(scratch, current.table.metadata_json);
    var protected: std.ArrayList([]const u8) = .empty;
    var wal_cut = try ingestion.coverage(a, current.table);
    // The lifecycle is the admission authority for index-generation readers.
    // Only current, live readers and in-flight publication inputs are roots;
    // an unleased historical publication cannot admit a new reader.
    const authority = server.source.lakeIndexLifecycleAuthority(request_context) orelse return error.ExternalLakeIndexUnavailable;
    const state = try authority.readState(scratch, table.table_id);
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    for (state.publications) |publication| {
        const live_reader = for (state.readers) |reader| {
            if (reader.generation == publication.generation and reader.expires_ms +| @import("../metadata/lake_index_lifecycle.zig").grace_ms >= now) break true;
        } else false;
        if (state.current != publication.generation and !live_reader) continue;
        if (publication.base_source != .external_iceberg) return error.InvalidLakeCatalog;
        const id = publication.base_source.external_iceberg.snapshot_id;
        try protected.append(scratch, id);
        const coverage = overlay.snapshotCoverage(root, id) catch {
            wal_cut = 0;
            continue;
        };
        wal_cut = @min(wal_cut, coverage);
    }
    if (request.action == .vacuum) {
        const result = try @import("../serverless/lake_vacuum.zig").run(a, binding, options, context, .{ .operation_id = request.operation_id, .dry_run = request.dry_run, .exclusive_ownership = request.exclusive_ownership, .retain_ms = request.retain_ms, .keep_latest = request.keep_latest, .max_deleted = request.max_deleted }, protected.items);
        return .{ .body = try std.json.Stringify.valueAlloc(a, result, .{}), .mutated = !request.dry_run and result.expired_snapshots != 0 };
    }
    var opened = try ingestion.openQueue(a, binding, options);
    defer opened.deinit();
    const wal: @import("../serverless/lake_wal.zig").Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = try ingestion.prefix(scratch, opened.prefix, binding, options), .context = context };
    const result = try wal.prune(scratch, wal_cut, request.max_deleted, request.dry_run);
    return .{ .body = try std.json.Stringify.valueAlloc(a, result, .{}), .mutated = false };
}
