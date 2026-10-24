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

//! Table-owned resolver resources. Runtime policy changes use the metadata
//! compare-and-swap contract; data owners consume them through reconciliation.
const std = @import("std");
const local = @import("antfly_local_sources");
const resolver = @import("antfly_resolver");
const curation = local.storage_db_resolver_curation;
const Config = local.storage_db_catalog_resolver_catalog.ResolverConfig;
const operation = local.api_operation;
const public_contract = @import("public_index_contract.zig");
const runtime_status = local.api_runtime_status;
const types = local.storage_db_selected_root.db.types;

pub const Operation = enum {
    list,
    get,
    render,
    resolve,
    redirects,
    merge,
    unmerge,
    suppressions,
    suppress,
    unsuppress,
    reviews,
    review,
    namespace,
    drop,
    entity_redirects,
    entity_merge,
    entity_unmerge,
    entity_suppress,
    entity_unsuppress,
    entity_distinct,
    proposals,
    proposal,
    proposal_create,
    proposal_accept,
    proposal_reject,
};
pub const Route = struct { table_name: []const u8 = "", resolver_name: []const u8 = "", key: []const u8 = "", op: Operation };

pub fn matchTablePath(path: []const u8) ?Route {
    const prefix = "/tables/";
    if (!std.mem.startsWith(u8, path, prefix)) return null;
    const rest = path[prefix.len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return null;
    if (slash == 0) return null;
    var route = matchSuffix(rest[slash + 1 ..]) orelse return null;
    route.table_name = rest[0..slash];
    return route;
}
pub fn matchSuffix(suffix: []const u8) ?Route {
    if (std.mem.eql(u8, suffix, "curation/proposals")) return .{ .op = .proposals };
    if (std.mem.startsWith(u8, suffix, "curation/proposals/")) {
        const tail = suffix["curation/proposals/".len..];
        if (std.mem.indexOfScalar(u8, tail, '/')) |_| return null;
        if (std.mem.endsWith(u8, tail, ":accept")) return .{ .op = .proposal_accept, .key = tail[0 .. tail.len - 7] };
        if (std.mem.endsWith(u8, tail, ":reject")) return .{ .op = .proposal_reject, .key = tail[0 .. tail.len - 7] };
        if (tail.len == 0) return null;
        return .{ .op = .proposal, .key = tail };
    }
    if (std.mem.eql(u8, suffix, "resolvers")) return .{ .op = .list };
    if (std.mem.startsWith(u8, suffix, "resolvers/")) {
        const rest = suffix["resolvers/".len..];
        if (rest.len == 0) return null;
        if (std.mem.indexOfScalar(u8, rest, '/')) |slash| {
            if (slash == 0) return null;
            const name = rest[0..slash];
            const tail = rest[slash + 1 ..];
            if (std.mem.eql(u8, tail, "redirects")) return .{ .resolver_name = name, .op = .redirects };
            if (std.mem.eql(u8, tail, "suppressions")) return .{ .resolver_name = name, .op = .suppressions };
            if (std.mem.eql(u8, tail, "reviews")) return .{ .resolver_name = name, .op = .reviews };
            inline for (.{ .{ "redirects/", Operation.unmerge }, .{ "suppressions/", Operation.unsuppress }, .{ "reviews/", Operation.review } }) |entry| {
                if (std.mem.startsWith(u8, tail, entry[0])) {
                    const key = tail[entry[0].len..];
                    if (key.len == 0 or std.mem.indexOfScalar(u8, key, '/') != null) return null;
                    return .{ .resolver_name = name, .key = key, .op = entry[1] };
                }
            }
            return null;
        }
        if (std.mem.lastIndexOfScalar(u8, rest, ':')) |colon| {
            if (colon == 0) return null;
            const action = rest[colon + 1 ..];
            const op: Operation = if (std.mem.eql(u8, action, "render")) .render else if (std.mem.eql(u8, action, "resolve")) .resolve else if (std.mem.eql(u8, action, "namespace")) .namespace else return null;
            return .{ .resolver_name = rest[0..colon], .op = op };
        }
        return .{ .resolver_name = rest, .op = .get };
    }
    if (std.mem.startsWith(u8, suffix, "entities/")) {
        const rest = suffix["entities/".len..];
        if (std.mem.endsWith(u8, rest, "/redirects")) {
            const key = rest[0 .. rest.len - "/redirects".len];
            if (key.len == 0 or std.mem.indexOfScalar(u8, key, '/') != null) return null;
            return .{ .key = key, .op = .entity_redirects };
        }
        const colon = std.mem.lastIndexOfScalar(u8, rest, ':') orelse return null;
        if (colon == 0 or std.mem.indexOfScalar(u8, rest[0..colon], '/') != null) return null;
        const action = rest[colon + 1 ..];
        const op: Operation = if (std.mem.eql(u8, action, "merge")) .entity_merge else if (std.mem.eql(u8, action, "unmerge")) .entity_unmerge else if (std.mem.eql(u8, action, "suppress")) .entity_suppress else if (std.mem.eql(u8, action, "unsuppress")) .entity_unsuppress else if (std.mem.eql(u8, action, "distinct")) .entity_distinct else return null;
        return .{ .key = rest[0..colon], .op = op };
    }
    return null;
}
pub fn readOnly(op: Operation) bool {
    if (op == .proposals or op == .proposal) return true;
    return op == .render or op == .resolve;
}
pub const GroupProgress = struct {
    group_id: u64,
    available: bool = false,
    fresh: bool = false,
    curation_revision: u64 = 0,
    namespace_curation_generation: u64 = 0,
    namespace_curation_table_id: u64 = 0,
    namespace_curation_missing: bool = false,
    config_generation: u64 = 0,
    effective_generation: u64 = 0,
    backfill_pending: bool = true,
    // A private owner proof, never inferred from an absent runtime snapshot.
    empty: bool = false,
    resolution: types.ReplayStageStats = .{},
    promotion: types.ReplayStageStats = .{},

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |name| {
            if (comptime std.mem.eql(u8, name, "empty") or std.mem.eql(u8, name, "namespace_curation_generation") or std.mem.eql(u8, name, "namespace_curation_table_id") or std.mem.eql(u8, name, "namespace_curation_missing")) continue;
            try jw.objectField(name);
            try jw.write(@field(self, name));
        }
        try jw.endObject();
    }
};
const ObservationTarget = struct {
    name: []const u8,
    config_generation: u64,
    promote: bool,
    authority_table_id: u64 = 0,
    authority_missing: bool = false,
    fn fromConfig(cfg: Config) ObservationTarget {
        return .{ .name = cfg.name, .config_generation = cfg.config_generation, .promote = cfg.promote, .authority_table_id = cfg.namespace_curation_table_id, .authority_missing = cfg.namespace_curation_missing };
    }
};
pub const Progress = struct { applied: bool, shards: []const GroupProgress, observation_error: []const u8 = "" };
const ReplayRequirement = struct {
    target: ObservationTarget,
    revision: u64,
    floor: ?ReplayFloor,
    fn applied(self: ReplayRequirement, row: GroupProgress) bool {
        const revision = if (self.target.authority_table_id != 0) row.namespace_curation_generation else row.curation_revision;
        if (self.target.authority_table_id != 0 and (row.namespace_curation_missing != self.target.authority_missing or row.namespace_curation_table_id != self.target.authority_table_id)) return false;
        if (!row.available or !row.fresh or revision < self.revision or
            row.config_generation < self.target.config_generation or row.backfill_pending or
            !curation.stageApplied(row.resolution, row.empty) or
            (self.target.promote and !curation.stageApplied(row.promotion, row.empty))) return false;
        if (self.floor) |floor| if (row.group_id == floor.group) {
            return row.resolution.applied_sequence >= floor.sequence and
                (!self.target.promote or row.promotion.applied_sequence >= floor.sequence);
        };
        return true;
    }
};
const Sample = struct {
    groups: std.AutoArrayHashMapUnmanaged(u64, void) = .empty,
    observations: std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(u64, GroupProgress)) = .empty,

    fn progress(self: Sample, a: std.mem.Allocator, cfg: ObservationTarget, revision: u64) !Progress {
        return self.progressForReplay(a, .{ .target = cfg, .revision = revision, .floor = null });
    }
    fn progressForReplay(self: Sample, a: std.mem.Allocator, required: ReplayRequirement) !Progress {
        const rows = try a.alloc(GroupProgress, self.groups.count());
        var applied = rows.len > 0;
        var floor_observed = required.floor == null;
        for (self.groups.keys(), rows) |id, *row| {
            row.* = if (self.observations.get(required.target.name)) |by_group| by_group.get(id) orelse .{ .group_id = id } else .{ .group_id = id };
            applied = applied and required.applied(row.*);
            if (required.floor) |floor| if (id == floor.group) {
                floor_observed = true;
            };
        }
        return .{ .applied = applied and floor_observed, .shards = rows };
    }
    fn needsReplayProbe(self: Sample, group: u64, required: ReplayRequirement) bool {
        const by_group = self.observations.get(required.target.name) orelse return true;
        return !required.applied(by_group.get(group) orelse return true);
    }
};

fn sample(server: anytype, a: std.mem.Allocator, snapshot: anytype, table: anytype, request: operation.RequestContext, probe_owners: bool) !Sample {
    return sampleForReplay(server, a, snapshot, table, request, probe_owners, null);
}
fn sampleForReplay(server: anytype, a: std.mem.Allocator, snapshot: anytype, table: anytype, request: operation.RequestContext, probe_owners: bool, required: ?ReplayRequirement) !Sample {
    return sampleForRequirements(server, a, snapshot, table, request, probe_owners, if (required) |value| &.{value} else &.{});
}
fn sampleForRequirements(server: anytype, a: std.mem.Allocator, snapshot: anytype, table: anytype, request: operation.RequestContext, probe_owners: bool, required: []const ReplayRequirement) !Sample {
    var result: Sample = .{};
    var wanted: std.StringHashMapUnmanaged(void) = .empty;
    for (required) |target| try wanted.put(a, target.target.name, {});
    for (snapshot.ranges) |range| if (range.table_id == table.table_id) try result.groups.put(a, range.group_id, {});
    var statuses = try server.localTableRuntimeStatusesWithSnapshot(table.name, snapshot);
    defer if (statuses) |*value| value.deinit(server.alloc);
    if (statuses) |values| for (values.items) |status| {
        if (!result.groups.contains(status.group_id)) continue;
        for (status.stats.resolver_replay.resolvers) |cfg| {
            if (required.len > 0 and !wanted.contains(cfg.name)) continue;
            const slot = try result.observations.getOrPut(a, cfg.name);
            if (!slot.found_existing) {
                slot.key_ptr.* = try a.dupe(u8, cfg.name);
                slot.value_ptr.* = .empty;
            }
            var resolution = status.stats.resolution;
            resolution.blocked_reason = try a.dupe(u8, resolution.blocked_reason);
            var promotion = status.stats.promotion;
            promotion.blocked_reason = try a.dupe(u8, promotion.blocked_reason);
            try slot.value_ptr.put(a, status.group_id, .{
                .group_id = status.group_id,
                .available = true,
                .fresh = runtime_status.statusRuntimeFresh(status),
                .curation_revision = cfg.curation_revision,
                .namespace_curation_generation = cfg.namespace_curation_generation,
                .namespace_curation_table_id = cfg.namespace_curation_table_id,
                .namespace_curation_missing = cfg.namespace_curation_missing,
                .config_generation = cfg.config_generation,
                .effective_generation = cfg.effective_generation,
                .backfill_pending = status.stats.resolver_replay.reresolve_pending,
                .resolution = resolution,
                .promotion = promotion,
            });
        }
    };
    if (!probe_owners) return result;
    // Heartbeats and read snapshots can omit resolver diagnostics. Ask the
    // current mutation owner for one compact report per incomplete group,
    // rather than mistaking an idle/read-only snapshot for applied curation.
    const desired = if (required.len == 0) try configs(a, table.indexes_json) else &.{};
    var selected_names: std.StringArrayHashMapUnmanaged(void) = .empty;
    if (required.len > 0) {
        for (required) |target| try selected_names.put(a, target.target.name, {});
    } else for (desired) |cfg| if (!cfg.retired) {
        try selected_names.put(a, cfg.name, {});
    };
    const Probe = struct {
        group: u64,
        names: []const []const u8,
        bytes: ?[]u8 = null,
        failure: ?anyerror = null,
        fn run(self: *@This(), owner: @TypeOf(server), name: []const u8, context: operation.RequestContext) void {
            const body = std.json.Stringify.valueAlloc(std.heap.smp_allocator, .{ .operation = "progress", .resolver_names = self.names }, .{}) catch |err| {
                self.failure = err;
                return;
            };
            defer std.heap.smp_allocator.free(body);
            self.bytes = owner.executeResolverOperation(std.heap.smp_allocator, name, self.group, body, context) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    var probes: std.ArrayListUnmanaged(Probe) = .empty;
    defer for (probes.items) |probe| if (probe.bytes) |bytes| std.heap.smp_allocator.free(bytes);
    for (result.groups.keys()) |group| {
        const needs_probe = for (desired) |cfg| {
            if (cfg.retired) continue;
            const by_group = result.observations.get(cfg.name) orelse break true;
            const row = by_group.get(group) orelse break true;
            if (!row.available or !row.fresh or row.curation_revision < cfg.curation_revision or
                row.config_generation < cfg.config_generation or row.backfill_pending or
                !row.resolution.enabled or row.resolution.blocked or row.resolution.catch_up_required or
                row.resolution.applied_sequence < row.resolution.target_sequence or
                (cfg.promote and (!row.promotion.enabled or row.promotion.blocked or row.promotion.catch_up_required or row.promotion.applied_sequence < row.promotion.target_sequence))) break true;
        } else false;
        const behind_requested = for (required) |target| {
            if (result.needsReplayProbe(group, target)) break true;
        } else false;
        if (needs_probe or behind_requested) try probes.append(a, .{ .group = group, .names = selected_names.keys() });
    }
    var offset: usize = 0;
    while (offset < probes.items.len) {
        const batch = probes.items[offset..@min(probes.items.len, offset + 16)];
        if (request.fanout_io orelse request.deadline_io) |borrow| {
            var receiver = try borrow.receive();
            const io = receiver.io();
            var tasks: std.Io.Group = .init;
            defer tasks.cancel(io);
            for (batch) |*probe| tasks.async(io, Probe.run, .{ probe, server, table.name, request });
            tasks.await(io) catch return error.Canceled;
        } else for (batch) |*probe| Probe.run(probe, server, table.name, request);
        for (batch) |probe| {
            if (probe.failure) |err| switch (err) {
                error.Canceled, error.Cancelled => return error.Canceled,
                error.DeadlineExceeded => return err,
                else => continue,
            };
            const bytes = probe.bytes orelse continue;
            const report = try std.json.parseFromSliceLeaky(curation.ProgressReport, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
            try mergeOwnerProgress(a, &result, probe.group, report);
        }
        offset += batch.len;
    }
    return result;
}

fn mergeOwnerProgress(a: std.mem.Allocator, result: *Sample, group: u64, report: curation.ProgressReport) !void {
    for (report.resolvers) |row| {
        const slot = try result.observations.getOrPut(a, row.name);
        if (!slot.found_existing) slot.value_ptr.* = .empty;
        try slot.value_ptr.put(a, group, .{
            .group_id = group,
            .available = true,
            .fresh = true,
            .curation_revision = row.curation_revision,
            .namespace_curation_generation = row.namespace_curation_generation,
            .namespace_curation_table_id = row.namespace_curation_table_id,
            .namespace_curation_missing = row.namespace_curation_missing,
            .config_generation = row.config_generation,
            .effective_generation = row.effective_generation,
            .backfill_pending = row.backfill_pending,
            .empty = row.empty,
            .resolution = row.resolution,
            .promotion = row.promotion,
        });
    }
}

fn tableByName(snapshot: anytype, name: []const u8) !@TypeOf(&snapshot.tables[0]) {
    for (snapshot.tables) |*table| if (std.mem.eql(u8, table.name, name)) return table;
    return error.TableNotFound;
}

fn configs(a: std.mem.Allocator, indexes_json: []const u8) ![]const Config {
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, indexes_json, .{});
    const metadata = try curation.metadataFromValue(a, root);
    var definitions: std.ArrayListUnmanaged(Config) = .empty;
    try local.metadata_local_index_reconcile.collectDesiredResolvers(a, root, &definitions);
    return curation.joinMetadata(a, definitions.items, metadata);
}

fn findConfig(all: []const Config, name: []const u8) !Config {
    for (all) |cfg| if (std.mem.eql(u8, cfg.name, name)) return cfg;
    return error.ResolverNotFound;
}

fn publicConfig(a: std.mem.Allocator, cfg: Config) !std.json.Value {
    const value = try cfg.publicValue(a);
    var out: std.json.ObjectMap = .empty;
    var it = value.object.iterator();
    while (it.next()) |entry| if (public_contract.isAllowedGraphResolverField(entry.key_ptr.*)) try out.put(a, entry.key_ptr.*, entry.value_ptr.*);
    _ = out.swapRemove("scorer_json");
    if (cfg.scorer_json.len > 0) try out.put(a, "scorer", try std.json.parseFromSliceLeaky(std.json.Value, a, cfg.scorer_json, .{}));
    return .{ .object = out };
}

fn owners(a: std.mem.Allocator, indexes_json: []const u8, name: []const u8) ![]const []const u8 {
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, indexes_json, .{});
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    try collectOwners(a, root, "", name, &names);
    return names.items;
}
fn collectOwners(a: std.mem.Allocator, value: std.json.Value, parent: []const u8, name: []const u8, out: *std.ArrayListUnmanaged([]const u8)) !void {
    switch (value) {
        .object => |object| {
            const index = if (object.get("name")) |v| if (v == .string) v.string else parent else parent;
            if (object.get("resolvers")) |declarations| if (declarations == .array) {
                for (declarations.array.items) |item| if (item == .object) if (item.object.get("name")) |v| if (v == .string and std.mem.eql(u8, v.string, name) and index.len > 0) try out.append(a, index);
            };
            var it = object.iterator();
            while (it.next()) |entry| {
                if (local.api_local_indexes.index_metadata.isOpaqueEntry(entry.key_ptr.*) or std.mem.eql(u8, entry.key_ptr.*, "resolvers")) continue;
                try collectOwners(a, entry.value_ptr.*, entry.key_ptr.*, name, out);
            }
        },
        .array => |array| for (array.items) |item| try collectOwners(a, item, parent, name, out),
        else => {},
    }
}

fn summary(a: std.mem.Allocator, table: anytype, cfg: Config, observation: Sample) !std.json.Value {
    const state = try resolver.curation.parseState(a, cfg.curation_json);
    return json(a, .{
        .name = cfg.name,
        .source_table = table.name,
        .entity_table = cfg.table,
        .declaring_indexes = try owners(a, table.indexes_json, cfg.name),
        .retired = cfg.retired,
        .config = if (cfg.retired) std.json.Value.null else try publicConfig(a, cfg),
        .revision = cfg.curation_revision,
        .namespace_generation = cfg.namespace_generation,
        .redirects = try resolver.curation.redirectsWithOrigins(a, try cfg.effectiveAliasesAlloc(a), state, cfg.merges),
        .curation = state,
        .progress = try observation.progress(a, ObservationTarget.fromConfig(cfg), cfg.curation_revision),
    });
}

pub fn handle(server: anytype, alloc: std.mem.Allocator, op: Operation, table_name: []const u8, resolver_name: []const u8, key: []const u8, body: []const u8, query: []const u8, request: operation.RequestContext) !@import("public_table_http.zig").OwnedResponse {
    return executeWithRetry(server, alloc, op, table_name, resolver_name, key, body, query, request) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        else => {
            if (err == error.OutOfMemory) return err;
            const status = failureStatus(err);
            return .{ .status = status, .json = true, .retry_after_seconds = if (status == 429 or status == 503) 1 else null, .body = try std.json.Stringify.valueAlloc(alloc, .{ .@"error" = @errorName(err), .code = @errorName(err), .retryable = status == 429 or status == 503 or (status == 409 and err != error.ResolverCurationConflict and err != error.ResolverNamespaceCurationRequired and err != error.ResolverNamespaceMigrationConflict) }, .{}) };
        },
    };
}

fn executeWithRetry(server: anytype, alloc: std.mem.Allocator, op: Operation, table_name: []const u8, resolver_name: []const u8, key: []const u8, body: []const u8, query: []const u8, request: operation.RequestContext) !@import("public_table_http.zig").OwnedResponse {
    const retryable_request = op == .merge or op == .unmerge or op == .suppress or op == .unsuppress or op == .proposal_create or op == .proposal_accept or op == .proposal_reject or op == .entity_redirects or op == .entity_merge or op == .entity_unmerge or op == .entity_suppress or op == .entity_unsuppress or op == .entity_distinct;
    var attempts: u8 = 0;
    while (true) {
        return execute(server, alloc, op, table_name, resolver_name, key, body, query, request) catch |err| {
            // These errors certify that the catalog CAS did not commit. Take
            // a fresh adoption cut, preserving any client expected_revision.
            if (retryable_request and attempts < 2 and (err == error.TableLifecycleConflict or err == error.TableGenerationChanged or err == error.CatalogGenerationChanged)) {
                attempts += 1;
                continue;
            }
            return err;
        };
    }
}

pub fn failureStatus(err: anyerror) u16 {
    return switch (err) {
        error.TableNotFound, error.ResolverNotFound, error.NotFound => 404,
        error.Forbidden, error.RowPolicyAuthenticationRequired, error.RowPolicyUnsupported, error.RowPolicyCatalogChanged => 403,
        error.Busy, error.WriterLocked, error.CapacityExhausted, error.ResourceBudgetExceeded => 429,
        error.Conflict, error.ResolverCurationConflict, error.ResolverNamespaceMigrationConflict, error.ResolverNamespaceCurationRequired, error.TableGenerationChanged, error.CatalogGenerationChanged, error.TableLifecycleConflict, error.TableTransitionActive, error.ObjectTableDefinitionConflict => 409,
        error.GroupLeaderUnavailable, error.HttpExecutorUnavailable, error.MetadataMutationOutcomeUnknown, error.StorageUnavailable, error.UnknownGroup, error.NotLeader, error.LeaderUnavailable, error.ProposalDropped, error.Unavailable => 503,
        error.ResolverIdentityActive => 409,
        error.UnsupportedOperation, error.MethodNotAllowed => 405,
        error.InvalidCharacter, error.InvalidPadding, error.InvalidLength, error.InvalidResolverDistinctPair, error.InvalidResolverCurationRequest, error.InvalidResolverSuppression, error.InvalidResolverCurationKey, error.ResolverCurationLimitExceeded, error.ResolverRedirectExists, error.SuppressedResolverRedirectTarget, error.ResolverDistinctPair, error.InvalidResolverAlias, error.InvalidResolverAliasCycle, error.ResolverMergeNamespaceChange, error.InvalidArgument, error.InvalidResolverCurationVersion, error.SyntaxError, error.UnexpectedToken, error.UnknownField, error.MissingField, error.InvalidNumber, error.Overflow, error.InvalidEnumTag => 400,
        else => 500,
    };
}

const catalog_read = @import("../metadata/resolver_catalog_read.zig");
fn readResolverCatalog(server: anytype, a: std.mem.Allocator, input: catalog_read.Request, request: operation.RequestContext) !catalog_read.Page {
    try request.ensureActive();
    const bytes = try server.source.systemCatalog(a, request, .{ .resolver_catalog_read = input });
    const page = try std.json.parseFromSliceLeaky(catalog_read.Page, a, bytes, .{});
    if (page.version != 1) return error.InvalidCatalogRecord;
    return page;
}

fn execute(server: anytype, alloc: std.mem.Allocator, op: Operation, table_name: []const u8, resolver_name: []const u8, key: []const u8, body: []const u8, query: []const u8, request: operation.RequestContext) !@import("public_table_http.zig").OwnedResponse {
    try request.ensureActive();
    if (body.len > 4 * 1024 * 1024) return error.ResolverCurationLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const entity_mutation = op == .proposal_create or op == .proposal_accept or op == .proposal_reject or op == .entity_merge or op == .entity_unmerge or op == .entity_suppress or op == .entity_unsuppress or op == .entity_distinct;
    // All resolver resources use the same scoped source cut. Full snapshots
    // remain only for sources without the private catalog-read capability.
    const page: ?catalog_read.Page = if (server.source.vtable.supports_resolver_catalog_read)
        try readResolverCatalog(server, a, .{ .table_name = table_name, .projection_names = if (op == .list) null else if (resolver_name.len > 0) &.{resolver_name} else &.{}, .routing_siblings = op == .resolve }, request)
    else
        null;
    const adoption_revision = if (page) |value| @as(?u64, value.revision) else if (entity_mutation and server.source.vtable.linearizable_routing_snapshot != null) blk: {
        var cut = try server.source.linearizableRoutingSnapshot(request.deadline_ns);
        defer server.source.freeRoutingSnapshot(&cut);
        break :blk @as(?u64, cut.catalog_revision);
    } else null;
    var snapshot = if (page) |*value| value.snapshot() else (try server.source.linearizableSnapshot(request)) orelse return error.TableNotFound;
    defer if (page == null) server.source.freeAdminSnapshot(&snapshot);
    const table = try tableByName(&snapshot, table_name);
    if (op == .proposals or op == .proposal or op == .proposal_create or op == .proposal_accept or op == .proposal_reject)
        return proposalOperation(server, alloc, a, &snapshot, table, op, key, body, query, request, adoption_revision);
    if (op == .entity_redirects or op == .entity_merge or op == .entity_unmerge or op == .entity_suppress or op == .entity_unsuppress or op == .entity_distinct)
        return entityOperation(server, alloc, a, &snapshot, table, op, key, body, request, adoption_revision, null);
    const all = if (page) |value| value.configs orelse try configs(a, value.executionIndexes()) else try configs(a, table.indexes_json);
    if (op == .list) {
        const observation = try sample(server, a, &snapshot, table, request, true);
        var out = std.json.Array.init(a);
        for (all) |cfg| try out.append(try summary(a, table, cfg, observation));
        return response(alloc, 200, .{ .resolvers = std.json.Value{ .array = out } });
    }
    if (op == .drop) {
        const plan = try curation.planDropMetadata(a, table.indexes_json, resolver_name);
        var replacement = table.*;
        replacement.indexes_json = plan.indexes_json;
        const stamp = if (plan.changed) try server.source.replaceTableDefinitionStamped(table.*, replacement) else null;
        return response(alloc, 202, .{ .resolver = resolver_name, .changed = plan.changed, .revision = plan.revision, .replay_sequence = plan.revision, .replay_sequence_kind = "resolver_revision", .metadata_commit = stamp, .applied = false });
    }
    const cfg = try findConfig(all, resolver_name);
    if (cfg.retired and (op == .render or op == .resolve or op == .reviews or op == .review)) return error.ResolverNotFound;
    switch (op) {
        .reviews, .review => return reviewsOperation(server, alloc, a, &snapshot, table, cfg, op, key, body, query, request),
        .resolve => return resolveQuery(server, alloc, a, table, all, cfg, body, request),
        .get => return response(alloc, 200, try summary(a, table, cfg, try sampleForReplay(server, a, &snapshot, table, request, true, .{ .target = ObservationTarget.fromConfig(cfg), .revision = cfg.curation_revision, .floor = null }))),
        .redirects => {
            const state = try resolver.curation.parseState(a, cfg.curation_json);
            return response(alloc, 200, .{ .redirects = try resolver.curation.redirectsWithOrigins(a, try cfg.effectiveAliasesAlloc(a), state, cfg.merges), .distinct = state.distinct, .revision = cfg.curation_revision });
        },
        .suppressions => return response(alloc, 200, .{
            .configured = if (cfg.suppress_json.len > 0) try std.json.parseFromSliceLeaky(resolver.curation.Suppression, a, cfg.suppress_json, .{}) else resolver.curation.Suppression{},
            .runtime = (try resolver.curation.parseState(a, cfg.curation_json)).suppress,
            .revision = cfg.curation_revision,
        }),
        .render => {
            const Mention = struct { text: []const u8, label: []const u8, local_id: []const u8 = "query" };
            const mention = try std.json.parseFromSliceLeaky(Mention, a, body, .{});
            const routing = local.storage_db_resolution_runtime;
            const excluded = try routing.siblingClaimedLabelsAlloc(a, all, &cfg);
            var parsed = try resolver.Resolver.initFromParts(a, cfg.table, cfg.key_template, .{ .labels = cfg.labels, .exclude_labels = excluded, .min_confidence = cfg.min_confidence orelse 0 }, cfg.type_must_match, "");
            defer parsed.deinit();
            if (!parsed.consumesLabel(mention.label)) return error.InvalidArgument;
            const minted = try parsed.renderKeyAlloc(a, .{ .text = mention.text, .label = mention.label, .local_id = mention.local_id });
            const aliases = try cfg.effectiveAliasesAlloc(a);
            const index = try resolver.curation.RedirectIndex.init(a, aliases);
            const policy = try resolver.curation.Index.init(a, cfg.suppress_json, cfg.curation_json);
            const suppressed = (try policy.textReason(a, mention.text)) != null or policy.keySuppressed(minted) or policy.keySuppressed(index.canonical(minted));
            return response(alloc, 200, .{ .minted_key = minted, .key = index.canonical(minted), .table = cfg.table, .config_generation = cfg.effectiveGeneration(), .suppressed = suppressed });
        },
        .namespace => {
            const intent = try std.json.parseFromSliceLeaky(curation.NamespaceRequest, a, body, .{});
            try server.authorizeIntegrityMutations(request, &.{.{ .table_name = intent.table, .deletes = &.{"resolver_namespace_authorization"} }});
            const plan = try curation.planNamespaceMetadata(a, table.indexes_json, resolver_name, intent);
            const grants = local.api_stored_destination_authorization;
            const sealed = try grants.sealIndexesJsonForPrincipalAlloc(a, plan.indexes_json, table.name, request.destination_authorization_principal, .{ .manager = server.cfg.user_manager, .auth_enabled = server.cfg.auth_enabled });
            var replacement = table.*;
            replacement.indexes_json = sealed;
            const next = try findConfig(try configs(a, sealed), resolver_name);
            const stamp = try server.source.replaceTableDefinitionStamped(table.*, replacement);
            const progress = try observeAccepted(server, a, table.name, ObservationTarget.fromConfig(next), plan.revision, intent.wait, request);
            return response(alloc, if (progress.applied) 200 else 202, .{ .resolver = resolver_name, .changed = true, .revision = plan.revision, .replay_sequence = plan.revision, .replay_sequence_kind = "resolver_revision", .metadata_commit = stamp, .applied = progress.applied, .progress = progress });
        },
        .merge, .unmerge, .suppress, .unsuppress => {
            var destination_name = cfg.table;
            if (server.source.vtable.system_catalog != null) {
                const targets = [_]local.system_catalog_domain.Target{try local.system_catalog_domain.Target.parse(cfg.table)};
                const bytes = try server.source.systemCatalog(a, request, .{ .resolve_many = .{ .targets = &targets } });
                const bound = try std.json.parseFromSliceLeaky(local.system_catalog_domain.ResolvedMany, a, bytes, .{});
                if (bound.tables.len != 1) return error.InvalidCatalogRecord;
                destination_name = if (bound.tables[0]) |target| target.name else "";
            }
            if (page) |source_cut| {
                if (destination_name.len > 0) {
                    // Bind the destination and inspect its authority at the
                    // same cut as the source, without cloning unrelated tables.
                    const destination = try readResolverCatalog(server, a, .{ .table_name = destination_name, .expected_revision = source_cut.revision }, request);
                    try requireSourceCuration(a, destination.table.indexes_json, cfg.key_template);
                }
            } else for (snapshot.tables) |destination| {
                if (std.mem.eql(u8, destination.name, destination_name)) try requireSourceCuration(a, destination.indexes_json, cfg.key_template);
            }
            const intent = try mutationRequest(a, op, key, body, query);
            const plan = try curation.planMetadata(a, table.indexes_json, resolver_name, intent);
            const stamp = if (plan.changed) committed: {
                var replacement = table.*;
                replacement.indexes_json = plan.indexes_json;
                try request.ensureActive();
                break :committed try server.source.replaceTableDefinitionStamped(table.*, replacement);
            } else null;
            const progress = try observeAccepted(server, a, table.name, ObservationTarget.fromConfig(cfg), plan.revision, intent.wait, request);
            return response(alloc, if (progress.applied) 200 else 202, .{ .resolver = resolver_name, .changed = plan.changed, .revision = plan.revision, .replay_sequence = plan.revision, .replay_sequence_kind = "resolver_revision", .metadata_commit = stamp, .applied = progress.applied, .progress = progress });
        },
        else => return error.UnsupportedOperation,
    }
}

fn requireSourceCuration(a: std.mem.Allocator, indexes_json: []const u8, key_template: []const u8) !void {
    if (indexes_json.len == 0) return;
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, indexes_json, .{});
    if ((try curation.Ownership.init(a, root)).owns(key_template)) return error.ResolverNamespaceCurationRequired;
}

fn namespaceAdopted(stored: curation.Namespaces, key_template: []const u8) bool {
    for (stored.namespaces) |namespace| if (std.mem.eql(u8, namespace.key_template, key_template)) return true;
    return false;
}

// Only the compact observation identity survives a catalog page. Full source
// definitions and parsed policies are released before the next page is read.
const EntityConsumer = struct { table_name: []const u8, key_template: []const u8, target: ObservationTarget };
const LocalTarget = struct { target: []const u8, conflict: bool = false };
const EntityDiscovery = struct {
    a: std.mem.Allocator,
    owner: std.mem.Allocator,
    namespaces: *std.StringArrayHashMapUnmanaged(curation.Namespace),
    stored: curation.Namespaces,
    adoption_revision: ?u64,
    standalone: bool,
    key: []const u8,
    observations: bool,
    aliases: bool,
    authority_table_id: u64 = 0,
    consumers: std.ArrayListUnmanaged(EntityConsumer) = .empty,
    builders: std.StringHashMapUnmanaged(curation.NamespaceBuilder) = .empty,
    local_targets: std.StringHashMapUnmanaged(LocalTarget) = .empty,
    profiles: std.ArrayListUnmanaged(@import("../metadata/resolver_authority.zig").AliasProfile) = .empty,
    profile_keys: std.StringHashMapUnmanaged(void) = .empty,

    fn deinit(self: *EntityDiscovery) void {
        self.builders.deinit(self.owner);
    }
    fn finish(self: *EntityDiscovery) !void {
        var it = self.builders.iterator();
        while (it.next()) |entry| {
            const namespace = self.namespaces.getPtr(entry.key_ptr.*).?;
            var next = try entry.value_ptr.finish(self.a);
            next.adoption_revision = namespace.adoption_revision;
            next.adoption_standalone = namespace.adoption_standalone;
            namespace.* = next;
        }
    }
    fn consume(self: *EntityDiscovery, scratch: std.mem.Allocator, table_name: []const u8, cfg: Config) !void {
        const entry = try self.namespaces.getOrPut(self.a, cfg.key_template);
        if (!entry.found_existing) {
            entry.key_ptr.* = try self.a.dupe(u8, cfg.key_template);
            entry.value_ptr.* = .{ .key_template = entry.key_ptr.*, .adoption_revision = self.adoption_revision, .adoption_standalone = self.standalone };
        }
        const template = entry.key_ptr.*;
        const authoritative = namespaceAdopted(self.stored, template);
        if (!authoritative) {
            const builder = try self.builders.getOrPut(self.owner, template);
            if (!builder.found_existing) builder.value_ptr.* = .{ .template = template };
            try builder.value_ptr.add(self.a, scratch, cfg);
        }
        const static_aliases = if (self.observations) try cfg.effectiveAliasesAlloc(scratch) else &.{};
        const profile_key = if (self.observations) try std.json.Stringify.valueAlloc(scratch, .{ template, static_aliases, cfg.suppress_json }, .{}) else "";
        if (self.observations and !self.profile_keys.contains(profile_key)) {
            try self.profile_keys.put(self.a, try self.a.dupe(u8, profile_key), {});
            const owned = try self.a.alloc(resolver.KeyAlias, static_aliases.len);
            for (static_aliases, owned) |alias, *copy| copy.* = .{ .key = try self.a.dupe(u8, alias.key), .target = try self.a.dupe(u8, alias.target) };
            try self.profiles.append(self.a, .{ .key_template = template, .aliases = owned, .suppress_json = try self.a.dupe(u8, cfg.suppress_json) });
        }
        if (self.aliases) for (try cfg.effectiveAliasesAlloc(scratch)) |alias| {
            if (!std.mem.eql(u8, alias.key, self.key)) continue;
            const prior = try self.local_targets.getOrPut(self.a, template);
            if (!prior.found_existing) prior.value_ptr.* = .{ .target = try self.a.dupe(u8, alias.target) } else if (!std.mem.eql(u8, prior.value_ptr.target, alias.target)) prior.value_ptr.conflict = true;
        };
        if (!self.observations) return;
        try self.consumers.append(self.a, .{ .table_name = try self.a.dupe(u8, table_name), .key_template = template, .target = .{
            .name = try self.a.dupe(u8, cfg.name),
            .config_generation = cfg.config_generation,
            .promote = cfg.promote,
            .authority_table_id = if (authoritative) self.authority_table_id else 0,
        } });
    }
};
/// Static aliases stay local declarations, but planning absorbed releases and
/// canonical paths must see the same affected graph as validation. Native
/// corrections override declarations; tombstones mask them. Ambiguity outside
/// the affected component cannot block an unrelated correction.
fn entityEffectiveAliases(a: std.mem.Allocator, namespace: curation.Namespace, profiles: []const native_authority.AliasProfile, key: []const u8, into: ?[]const u8) ![]const resolver.KeyAlias {
    return native_authority.records.effectiveAliases(a, namespace, profiles, key, into);
}

/// Both passes read the same revision-fenced catalog cut. A page arena owns
/// transport bytes, table schemas and parsed configs; callbacks retain only
/// output identities or the folded namespace policy.
fn scanEntityConsumers(server: anytype, owner: std.mem.Allocator, snapshot: anytype, entity_table: anytype, key: []const u8, request: operation.RequestContext, adoption_revision: ?u64, stored: curation.Namespaces, context: anytype) !void {
    const ConsumerConfig = struct { source_name: []const u8, config: Config };
    if (server.source.vtable.supports_resolver_catalog_read) {
        var cursor: ?[]const u8 = null;
        defer if (cursor) |value| owner.free(value);
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        defer {
            var keys = seen.keyIterator();
            while (keys.next()) |value| owner.free(value.*);
            seen.deinit(owner);
        }
        while (true) {
            var arena = std.heap.ArenaAllocator.init(owner);
            defer arena.deinit();
            const scratch = arena.allocator();
            const page = try readResolverCatalog(server, scratch, .{ .table_name = entity_table.name, .mode = .consumers, .key = key, .expected_revision = adoption_revision, .cursor = cursor, .compact_consumers = server.source.vtable.supports_resolver_authority }, request);
            var pending: std.ArrayList(ConsumerConfig) = .empty;
            var source_reads: std.ArrayList([]const u8) = .empty;
            for (page.consumers) |consumer| {
                var needs_source_read = false;
                for (consumer.configs orelse try configs(scratch, consumer.table.indexes_json)) |cfg| {
                    for (consumer.names) |name| if (std.mem.eql(u8, cfg.name, name)) {
                        const identity = try std.json.Stringify.valueAlloc(scratch, .{ consumer.table.table_id, cfg.name }, .{});
                        if (seen.contains(identity)) break;
                        try seen.put(owner, try owner.dupe(u8, identity), {});
                        needs_source_read = needs_source_read or !namespaceAdopted(stored, cfg.key_template);
                        try pending.append(scratch, .{ .source_name = consumer.table.name, .config = cfg });
                        break;
                    };
                }
                if (needs_source_read) try source_reads.append(scratch, consumer.table.name);
            }
            // Legacy policy remains source-owned. Authorize each page in one
            // bounded reverse-binding batch before folding any private policy;
            // adopted namespaces need no source reads or per-consumer RPCs.
            if (source_reads.items.len != 0) try server.authorizeTableReads(scratch, request, source_reads.items);
            for (pending.items) |consumer| try context.consume(scratch, consumer.source_name, consumer.config);
            const next = if (page.next_cursor) |value| try owner.dupe(u8, value) else null;
            if (cursor) |value| owner.free(value);
            cursor = next;
            if (cursor == null) break;
        }
    } else {
        // Compatibility path for sources without the scoped catalog capability.
        var arena = std.heap.ArenaAllocator.init(owner);
        defer arena.deinit();
        const a = arena.allocator();
        var destinations: std.StringArrayHashMapUnmanaged([]const u8) = .empty;
        const source_configs = try a.alloc([]const Config, snapshot.tables.len);
        for (snapshot.tables, source_configs) |source, *all| {
            all.* = try configs(a, if (source.indexes_json.len == 0) "{}" else source.indexes_json);
            for (all.*) |cfg| try destinations.put(a, cfg.table, cfg.table);
        }
        if (server.source.vtable.system_catalog != null) {
            const domain = local.system_catalog_domain;
            var offset: usize = 0;
            var revision: ?u64 = null;
            while (offset < destinations.count()) {
                const end = @min(destinations.count(), offset + 256);
                const targets = try a.alloc(domain.Target, end - offset);
                for (destinations.keys()[offset..end], targets) |name, *target| target.* = try domain.Target.parse(name);
                const reply = try server.source.systemCatalog(a, request, .{ .resolve_many = .{ .targets = targets, .expected_revision = revision } });
                const bound = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, reply, .{});
                if (bound.tables.len != targets.len) return error.InvalidCatalogRecord;
                revision = bound.revision;
                for (bound.tables, destinations.values()[offset..end]) |target, *name| name.* = if (target) |table| table.name else "";
                offset = end;
            }
        }
        var pending: std.ArrayList(ConsumerConfig) = .empty;
        var source_reads: std.ArrayList([]const u8) = .empty;
        for (snapshot.tables, source_configs) |source, all| {
            var needs_source_read = false;
            for (all) |cfg| {
                if (!std.mem.eql(u8, destinations.get(cfg.table).?, entity_table.name) or !curation.keyInNamespace(cfg.key_template, key)) continue;
                needs_source_read = needs_source_read or !namespaceAdopted(stored, cfg.key_template);
                try pending.append(a, .{ .source_name = source.name, .config = cfg });
            }
            if (needs_source_read) try source_reads.append(a, source.name);
        }
        if (source_reads.items.len != 0) try server.authorizeTableReads(a, request, source_reads.items);
        for (pending.items) |consumer| try context.consume(a, consumer.source_name, consumer.config);
    }
}

/// HTTP decisions must fit the metadata CAS as well as the standalone queue.
/// Both expected and replacement carry the full queue. Reserve terminal growth
/// on both sides, plus room for the bounded logical relation-owner guard.
fn validateProposalPublicationCapacity(alloc: std.mem.Allocator, table: @import("../metadata/table_manager.zig").TableRecord, state: resolver.curation.State) !void {
    const codec = @import("../metadata/storage/raft_apply_store.zig");
    const protocol = @import("../metadata/topology_protocol.zig");
    // Index JSON is a length-framed byte string in the codec. Size its two
    // copies arithmetically instead of copying queue evidence into a probe.
    var header = table;
    header.indexes_json = "";
    const encoded = try codec.encodeTransitionCommand(alloc, .{ .compare_and_replace_table = .{ .expected = header, .replacement = header } });
    defer alloc.free(encoded);
    const growth = resolver.curation.proposalCapacityBytes(state) - resolver.curation.jsonSize(state);
    if (encoded.len + 2 * (table.indexes_json.len + growth) + 64 * 1024 > protocol.max_transition_command_bytes) return error.ResolverCurationLimitExceeded;
}

const proposals_key = curation.proposals_key;
const ProposalEdit = struct { id: []const u8, decision: resolver.curation.ProposalDecision };
const native_authority = @import("../metadata/resolver_authority.zig");
fn readNativeAuthority(server: anytype, a: std.mem.Allocator, table: anytype, input: native_authority.Read, request: operation.RequestContext, cut: ?u64) !?native_authority.View {
    if (!server.source.vtable.supports_resolver_authority) return null;
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, if (table.indexes_json.len == 0) "{}" else table.indexes_json, .{});
    if (!native_authority.isNative(root)) return null;
    const bytes = try server.source.systemCatalog(a, request, .{ .resolver_catalog_read = .{ .table_name = table.name, .table_id = table.table_id, .mode = .authority, .authority = input, .expected_revision = cut } });
    const view = try std.json.parseFromSliceLeaky(native_authority.View, a, bytes, .{});
    if (!view.native or view.table_id != table.table_id) return error.CatalogGenerationChanged;
    return view;
}
fn withNativeAuthority(a: std.mem.Allocator, root: *std.json.Value, view: native_authority.View) !void {
    var namespaces: std.ArrayList(curation.Namespace) = .empty;
    for (view.namespaces) |value| try namespaces.append(a, value.namespace);
    try root.object.put(a, curation.namespaces_key, try json(a, curation.Namespaces{ .namespaces = namespaces.items }));
    try root.object.put(a, proposals_key, try json(a, view.proposals));
}
fn publishAuthority(server: anytype, a: std.mem.Allocator, table: anytype, root: std.json.Value, view: ?native_authority.View, owned: []const []const u8, cut: ?u64, request: operation.RequestContext) !?@import("../metadata/api.zig").CatalogMutationStamp {
    var replacement = table.*;
    if (!server.source.vtable.supports_resolver_authority) {
        replacement.indexes_json = try std.json.Stringify.valueAlloc(a, root, .{});
        return server.source.replaceTableDefinitionStamped(table.*, replacement);
    }
    var write: native_authority.Write = .{ .table_id = table.table_id, .schema = try native_authority.schemaFingerprint(a, table.*), .catalog_revision = cut, .standalone = server.cfg.deployment_mode == .standalone, .owned_templates = owned };
    const before = view orelse legacy: {
        const old_root = try std.json.parseFromSliceLeaky(std.json.Value, a, if (table.indexes_json.len == 0) "{}" else table.indexes_json, .{});
        const old_namespaces = try curation.namespacesFromValue(a, old_root);
        const old_queue = try proposalQueue(a, old_root);
        const values = try a.alloc(native_authority.records.Snapshot, old_namespaces.namespaces.len);
        for (old_namespaces.namespaces, values) |namespace, *value| value.* = .{ .head = .{ .key_template = namespace.key_template, .revision = namespace.revision }, .namespace = namespace };
        write.bootstrap = .{ .expected_indexes = try resolver.fingerprint(a, table.indexes_json) };
        break :legacy native_authority.View{ .native = false, .table_id = table.table_id, .namespaces = values, .proposals = old_queue, .queue_revision = if (old_queue.proposals.len > 0) 1 else 0 };
    };
    {
        var patches: std.ArrayList(native_authority.records.Patch) = .empty;
        for ((try curation.namespacesFromValue(a, root)).namespaces) |next| {
            const old = for (before.namespaces) |value| {
                if (std.mem.eql(u8, value.head.key_template, next.key_template)) break value.namespace;
            } else curation.Namespace{ .key_template = next.key_template };
            if (next.revision != old.revision) try patches.append(a, try native_authority.records.difference(a, old, next));
        }
        write.patches = patches.items;
        const next = try proposalQueue(a, root);
        var previous: std.StringHashMapUnmanaged([]const u8) = .empty;
        for (before.proposals.proposals) |value| try previous.put(a, value.id, try std.json.Stringify.valueAlloc(a, value, .{}));
        var changed: std.ArrayList(resolver.curation.Proposal) = .empty;
        for (next.proposals) |value| {
            const raw = try std.json.Stringify.valueAlloc(a, value, .{});
            if (previous.get(value.id)) |old| if (std.mem.eql(u8, old, raw)) continue;
            try changed.append(a, value);
        }
        if (changed.items.len > 0) write.queue = .{ .expected_revision = before.queue_revision, .revision = try std.math.add(u64, before.queue_revision, 1), .proposals = changed.items };
    }
    try request.ensureActive();
    const bytes = try server.source.systemCatalog(a, request, .{ .resolver_authority_mutate = write });
    const result = try std.json.parseFromSliceLeaky(native_authority.View, a, bytes, .{});
    if (!result.receipt) return error.ResolverCurationConflict;
    return result.stamp;
}

fn proposalQueue(a: std.mem.Allocator, root: std.json.Value) !resolver.curation.State {
    const value = root.object.get(proposals_key) orelse return .{};
    const state = try resolver.curation.parseState(a, try std.json.Stringify.valueAlloc(a, value, .{}));
    try resolver.curation.validateProposalQueue(state);
    return state;
}
fn findProposal(state: resolver.curation.State, id: []const u8) !resolver.curation.Proposal {
    for (state.proposals) |proposal| if (std.mem.eql(u8, proposal.id, id)) return proposal;
    return error.NotFound;
}
/// A terminal decision observes current authoritative namespaces without
/// planning, folding legacy policy or publishing another correction. Only
/// compact consumer identities survive each fenced catalog page.
const ProposalReplay = struct {
    a: std.mem.Allocator,
    namespaces: std.StringArrayHashMapUnmanaged(curation.Namespace) = .empty,
    authority_table_id: u64 = 0,
    consumers: std.ArrayListUnmanaged(EntityConsumer) = .empty,

    fn consume(self: *ProposalReplay, _: std.mem.Allocator, table_name: []const u8, cfg: Config) !void {
        const namespace = self.namespaces.get(cfg.key_template) orelse return;
        try self.consumers.append(self.a, .{ .table_name = try self.a.dupe(u8, table_name), .key_template = namespace.key_template, .target = .{
            .name = try self.a.dupe(u8, cfg.name),
            .config_generation = cfg.config_generation,
            .promote = cfg.promote,
            .authority_table_id = self.authority_table_id,
        } });
    }
};

fn observeProposalDecision(server: anytype, alloc: std.mem.Allocator, a: std.mem.Allocator, snapshot: anytype, table: anytype, root: std.json.Value, proposal: resolver.curation.Proposal, wait: ?resolver.curation.Wait, request: operation.RequestContext, adoption_revision: ?u64) !@import("public_table_http.zig").OwnedResponse {
    // Plain rejections have no policy publication to wait for. Rejections
    // that pinned a pair carry a nonzero namespace revision like acceptances.
    const has_policy = proposal.state == .accepted or (proposal.decided != null and proposal.decided.?.revision > 0);
    var results = std.json.Array.init(a);
    var applied = true;
    if (has_policy) {
        const stored = try curation.namespacesFromValue(a, root);
        var replay: ProposalReplay = .{ .a = a, .authority_table_id = if (native_authority.isNative(root)) table.table_id else 0 };
        for (stored.namespaces) |namespace| if (curation.keyInNamespace(namespace.key_template, proposal.key)) try replay.namespaces.put(a, namespace.key_template, namespace);
        scanEntityConsumers(server, alloc, snapshot, table, proposal.key, request, adoption_revision, stored, &replay) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => {
                // A committed decision remains accepted when its consumers
                // cannot be observed. Surface that uncertainty as pending.
                try results.append(try json(a, .{ .accepted = true, .applied = false, .@"error" = @errorName(err) }));
                return response(alloc, 202, .{ .proposal = proposal, .changed = false, .complete = true, .atomicity = "namespace", .applied = false, .results = std.json.Value{ .array = results } });
            },
        };
        const observations = try observeConsumers(server, a, replay.consumers.items, replay.namespaces, wait, request);
        for (replay.consumers.items, observations) |consumer, progress| {
            const namespace = replay.namespaces.get(consumer.key_template).?;
            applied = applied and progress.applied;
            try results.append(try json(a, .{ .source_table = consumer.table_name, .resolver = consumer.target.name, .changed = false, .revision = namespace.revision, .replay_sequence = namespace.revision, .replay_sequence_kind = "resolver_revision", .accepted = true, .@"error" = progress.observation_error, .applied = progress.applied, .progress = progress }));
        }
    }
    return response(alloc, if (applied) 200 else 202, .{ .proposal = proposal, .changed = false, .complete = true, .atomicity = "namespace", .applied = applied, .results = std.json.Value{ .array = results } });
}

fn proposalOperation(server: anytype, alloc: std.mem.Allocator, a: std.mem.Allocator, snapshot: anytype, table: anytype, op: Operation, id: []const u8, body: []const u8, query: []const u8, request: operation.RequestContext, adoption_revision: ?u64) !@import("public_table_http.zig").OwnedResponse {
    if (readOnly(op)) try server.authorizeTableReads(a, request, &.{table.name}) else try server.authorizeIntegrityMutations(request, &.{.{ .table_name = table.name, .deletes = &.{id} }});
    var root = try std.json.parseFromSliceLeaky(std.json.Value, a, if (table.indexes_json.len == 0) "{}" else table.indexes_json, .{});
    var native: ?native_authority.View = null;
    var owned_templates: std.ArrayList([]const u8) = .empty;
    if (server.source.vtable.supports_resolver_authority) {
        var input: native_authority.Read = .{ .mode = .proposal, .id = id };
        if (op == .proposal_create) {
            const proposed = try std.json.parseFromSliceLeaky(resolver.curation.ProposalRequest, a, body, .{});
            input = .{ .key = proposed.key, .into = proposed.into, .id = try resolver.curation.proposalId(a, proposed) };
        } else if (op == .proposals) {
            const filter = try resolver.curation.proposalQuery(a, query);
            input = .{ .mode = .proposals, .state = filter.state, .action = filter.action, .cursor = if (filter.cursor.len == 0) null else filter.cursor, .limit = filter.limit };
        }
        native = try readNativeAuthority(server, a, table, input, request, adoption_revision);
        if (native) |view| {
            if (op == .proposals) return response(alloc, 200, resolver.curation.ProposalPage{ .proposals = view.proposals.proposals, .next_cursor = view.next_cursor });
            if (op == .proposal_accept or op == .proposal_reject) {
                const selected = try findProposal(view.proposals, id);
                native = try readNativeAuthority(server, a, table, .{ .key = selected.key, .into = selected.into, .id = id }, request, adoption_revision);
            }
            try withNativeAuthority(a, &root, native.?);
        }
    }
    var state = try proposalQueue(a, root);
    if (op == .proposal) return response(alloc, 200, try findProposal(state, id));
    if (op == .proposals) {
        const page = try resolver.curation.proposalPage(a, state, query);
        return response(alloc, 200, page);
    }
    if (op == .proposal_create) {
        const input = try std.json.parseFromSliceLeaky(resolver.curation.ProposalRequest, a, body, .{});
        // Creation must prove the same namespace and authorization as an entity
        // action, without changing policy or scheduling model work.
        const stored = try curation.namespacesFromValue(a, root);
        var namespaces: std.StringArrayHashMapUnmanaged(curation.Namespace) = .empty;
        var discovery: EntityDiscovery = .{ .a = a, .owner = alloc, .namespaces = &namespaces, .stored = stored, .adoption_revision = adoption_revision, .standalone = server.cfg.deployment_mode == .standalone, .key = input.key, .observations = false, .aliases = false };
        defer discovery.deinit();
        for (stored.namespaces) |namespace| if (curation.keyInNamespace(namespace.key_template, input.key)) try namespaces.put(a, namespace.key_template, namespace);
        try scanEntityConsumers(server, alloc, snapshot, table, input.key, request, adoption_revision, stored, &discovery);
        try discovery.finish();
        if (namespaces.count() == 0) return error.ResolverNotFound;
        try owned_templates.appendSlice(a, namespaces.keys());
        if (input.into) |into| for (namespaces.values()) |namespace| if (!curation.keyInNamespace(namespace.key_template, into)) return error.InvalidResolverCurationRequest;
        const stamp = curation.manualOrigin(.{ .action = .suppress, .actor = input.actor, .reason = input.reason }, 0);
        const plan = try resolver.curation.propose(a, state, &.{}, input, stamp);
        if (!plan.changed) return response(alloc, 200, try findProposal(state, try resolver.curation.proposalId(a, input)));
        state = plan.state;
    } else {
        const Input = struct { options: ?resolver.curation.Options = null, pin_distinct: bool = false, actor: []const u8 = "", reason: []const u8 = "", expected_revision: ?u64 = null, wait: ?resolver.curation.Wait = null };
        const input = try std.json.parseFromSliceLeaky(Input, a, if (body.len == 0) "{}" else body, .{});
        const decision: resolver.curation.ProposalDecision = .{ .accept = op == .proposal_accept, .options = input.options, .pin_distinct = input.pin_distinct, .actor = input.actor, .reason = input.reason, .expected_revision = input.expected_revision, .wait = input.wait };
        const proposal = try findProposal(state, id);
        const wanted: resolver.curation.ProposalState = if (decision.accept) .accepted else .rejected;
        if (decision.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
        if (proposal.state == wanted) return observeProposalDecision(server, alloc, a, snapshot, table, root, proposal, decision.wait, request, adoption_revision);
        if (proposal.state != .open) return error.ResolverCurationConflict;
        if (decision.accept or decision.pin_distinct) {
            const options = input.options orelse proposal.options;
            const action: Operation = if (!decision.accept) .entity_distinct else switch (proposal.action) {
                .merge => .entity_merge,
                .unmerge => .entity_unmerge,
                .suppress => .entity_suppress,
                .unsuppress => .entity_unsuppress,
                .distinct => .entity_distinct,
            };
            const bytes = try std.json.Stringify.valueAlloc(a, .{ .into = proposal.into, .replace = options.replace, .pin_distinct = options.pin_distinct, .absorbed = options.absorbed, .actor = input.actor, .reason = input.reason, .expected_revision = input.expected_revision, .wait = input.wait }, .{});
            return entityOperation(server, alloc, a, snapshot, table, action, proposal.key, bytes, request, adoption_revision, .{ .id = id, .decision = decision });
        }
        state = (try resolver.curation.decide(a, state, &.{}, &.{}, id, decision, curation.manualOrigin(.{ .action = .suppress, .actor = input.actor, .reason = input.reason }, 0))).state;
    }
    try root.object.put(a, proposals_key, try json(a, state));
    var replacement = table.*;
    replacement.indexes_json = try std.json.Stringify.valueAlloc(a, root, .{});
    if (op == .proposal_create and !server.source.vtable.supports_resolver_authority) try validateProposalPublicationCapacity(alloc, replacement, state);
    try request.ensureActive();
    _ = try publishAuthority(server, a, table, root, native, owned_templates.items, adoption_revision, request);
    if (op == .proposal_create) return response(alloc, 200, try findProposal(state, try resolver.curation.proposalId(a, try std.json.parseFromSliceLeaky(resolver.curation.ProposalRequest, a, body, .{}))));
    return response(alloc, 200, .{ .proposal = try findProposal(state, id), .changed = true, .complete = true, .atomicity = "namespace", .applied = true, .results = @as([]const std.json.Value, &.{}) });
}

/// One destination-table CAS owns every matching exact-template namespace.
/// Sources are observations of asynchronous replay, never curation authorities.
fn entityOperation(server: anytype, alloc: std.mem.Allocator, a: std.mem.Allocator, snapshot: anytype, entity_table: anytype, op: Operation, key: []const u8, body: []const u8, request: operation.RequestContext, adoption_revision: ?u64, proposal_edit: ?ProposalEdit) !@import("public_table_http.zig").OwnedResponse {
    if (op == .entity_redirects) {
        try server.authorizeTableReads(a, request, &.{entity_table.name});
    } else try server.authorizeIntegrityMutations(request, &.{.{ .table_name = entity_table.name, .deletes = &.{key} }});
    const Input = struct { into: ?[]const u8 = null, replace: bool = false, pin_distinct: bool = false, absorbed: resolver.curation.Absorbed = .reject, actor: []const u8 = "", reason: []const u8 = "", expected_revision: ?u64 = null, wait: ?resolver.curation.Wait = null };
    const input = try std.json.parseFromSliceLeaky(Input, a, if (body.len == 0) "{}" else body, .{});
    if (key.len == 0 or (op == .entity_merge and input.into == null)) return error.InvalidResolverCurationRequest;
    if (input.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
    var root = try std.json.parseFromSliceLeaky(std.json.Value, a, if (entity_table.indexes_json.len == 0) "{}" else entity_table.indexes_json, .{});
    var native = try readNativeAuthority(server, a, entity_table, .{ .key = key, .into = input.into, .include_proposals = op != .entity_redirects, .id = if (proposal_edit) |edit| edit.id else "" }, request, adoption_revision);
    if (native) |view| try withNativeAuthority(a, &root, view);
    const stored = try curation.namespacesFromValue(a, root);
    var namespaces: std.StringArrayHashMapUnmanaged(curation.Namespace) = .empty;
    for (stored.namespaces) |namespace| if (curation.keyInNamespace(namespace.key_template, key)) try namespaces.put(a, namespace.key_template, namespace);
    var discovery: EntityDiscovery = .{ .a = a, .owner = alloc, .namespaces = &namespaces, .stored = stored, .adoption_revision = adoption_revision, .standalone = server.cfg.deployment_mode == .standalone, .key = key, .observations = op != .entity_redirects, .aliases = op == .entity_merge or op == .entity_unmerge, .authority_table_id = if (native != null) entity_table.table_id else 0 };
    defer discovery.deinit();
    // An adopted namespace is self-contained. Inspection does not depend on
    // source consumers or copy their static/projected policy into the request.
    if (op != .entity_redirects or native == null or namespaces.count() == 0)
        try scanEntityConsumers(server, alloc, snapshot, entity_table, key, request, adoption_revision, stored, &discovery);
    try discovery.finish();
    if (native != null and discovery.profiles.items.len > 0) {
        var seeds: std.ArrayList([]const u8) = .empty;
        var targets = discovery.local_targets.valueIterator();
        while (targets.next()) |target| try seeds.append(a, target.target);
        native = try readNativeAuthority(server, a, entity_table, .{ .key = key, .into = input.into, .seeds = seeds.items, .profiles = discovery.profiles.items, .id = if (proposal_edit) |edit| edit.id else "" }, request, adoption_revision);
        try withNativeAuthority(a, &root, native.?);
        for (native.?.namespaces) |value| try namespaces.put(a, value.head.key_template, value.namespace);
    }
    if (namespaces.count() == 0) return error.ResolverNotFound;
    if (op == .entity_redirects) {
        var states = std.json.Array.init(a);
        for (namespaces.values()) |namespace| {
            const state = try resolver.curation.entityState(a, namespace.state, namespace.merges, key);
            try states.append(try json(a, .{ .source_table = entity_table.name, .resolver = "", .key_template = namespace.key_template, .revision = namespace.revision, .redirects = try resolver.curation.redirectsWithOrigins(a, state.redirects, state.curation, namespace.merges), .canonical_key = state.canonical_key, .curation = state.curation }));
        }
        return response(alloc, 200, .{ .entity_table = entity_table.name, .key = key, .complete = true, .atomicity = "namespace", .results = std.json.Value{ .array = states } });
    }
    const action: resolver.curation.Action = switch (op) {
        .entity_merge => .merge,
        .entity_unmerge => .unmerge,
        .entity_suppress => .suppress,
        .entity_unsuppress => .unsuppress,
        .entity_distinct => .distinct,
        else => unreachable,
    };
    const intent: resolver.curation.Request = .{ .action = action, .key = key, .to = input.into, .absorbed = input.absorbed, .actor = input.actor, .reason = input.reason, .replace = input.replace, .pin_distinct = input.pin_distinct, .expected_revision = input.expected_revision, .wait = input.wait };
    var changed = false;
    var proposal_state = try proposalQueue(a, root);
    const original_proposals = proposal_state.proposals;
    var absorbed_results: std.StringHashMapUnmanaged([]const resolver.curation.AbsorbedResult) = .empty;
    for (namespaces.values()) |*namespace| {
        if (input.into) |target| if (!curation.keyInNamespace(namespace.key_template, target)) return error.InvalidResolverCurationRequest;
        if (input.expected_revision) |expected| if (expected != namespace.revision) return error.ResolverCurationConflict;
        const policy_merges = namespace.merges;
        const effective = try native_authority.records.planningAliases(a, namespace.*, try entityEffectiveAliases(a, namespace.*, discovery.profiles.items, key, input.into), action, key, input.into);
        var planning_state = namespace.state;
        const stamp = curation.manualOrigin(intent, try std.math.add(u64, namespace.revision, 1));
        if (proposal_edit) |edit| {
            var proposal = try findProposal(.{ .proposals = original_proposals }, edit.id);
            proposal.evidence = null;
            planning_state.proposals = try a.dupe(resolver.curation.Proposal, &.{proposal});
        }
        var plan = if (proposal_edit) |edit|
            try resolver.curation.decide(a, planning_state, policy_merges, effective, edit.id, edit.decision, stamp)
        else
            try resolver.curation.planWithOrigin(a, try std.json.Stringify.valueAlloc(a, planning_state, .{}), policy_merges, effective, intent, stamp);
        const local_decision = if (proposal_edit != null) plan.state.proposals[0] else @as(?resolver.curation.Proposal, null);
        plan.state.proposals = namespace.state.proposals;
        const policy_changed = !Config.aliasesEqual(namespace.merges, plan.merges) or !std.mem.eql(u8, try std.json.Stringify.valueAlloc(a, namespace.state, .{}), try std.json.Stringify.valueAlloc(a, plan.state, .{}));
        if (policy_changed or proposal_edit != null) {
            const record_origin = if (local_decision) |proposal| proposal.decided.? else stamp;
            const actual_intent = if (proposal_edit) |edit| resolver.curation.decisionRequest(try findProposal(.{ .proposals = original_proposals }, edit.id), edit.decision) else intent;
            proposal_state.proposals = try resolver.curation.supersede(a, proposal_state.proposals, actual_intent, plan.absorbed, record_origin);
            if (local_decision) |proposal| {
                const records = try a.dupe(resolver.curation.Proposal, proposal_state.proposals);
                for (records) |*record| if (std.mem.eql(u8, record.id, proposal.id)) {
                    record.state = proposal.state;
                    record.decided = proposal.decided;
                    if (!policy_changed) record.decided.?.revision = namespace.revision;
                };
                proposal_state.proposals = records;
                changed = true;
            }
        }
        for (plan.absorbed) |absorbed| try server.authorizeIntegrityMutations(request, &.{.{ .table_name = entity_table.name, .deletes = &.{absorbed.key} }});
        try absorbed_results.put(a, namespace.key_template, plan.absorbed);
        const check: Config = .{ .name = "namespace", .table = entity_table.name, .source_artifact = "metadata", .resolution_artifact = "resolved", .key_template = namespace.key_template, .merges = plan.merges, .curation_json = try std.json.Stringify.valueAlloc(a, plan.state, .{}) };
        try check.validate();
        if (policy_changed or !namespaceAdopted(stored, namespace.key_template)) {
            namespace.adoption_revision = adoption_revision;
            namespace.adoption_standalone = server.cfg.deployment_mode == .standalone;
            namespace.state = plan.state;
            namespace.merges = plan.merges;
            namespace.revision = try std.math.add(u64, namespace.revision, 1);
            try curation.replaceNamespace(a, &root, namespace.*);
            changed = true;
        }
    }
    for (namespaces.values()) |namespace| try native_authority.records.validateProfiles(a, namespace, discovery.profiles.items);
    if (changed) {
        _ = try resolver.curation.parseState(a, try std.json.Stringify.valueAlloc(a, proposal_state, .{}));
        try root.object.put(a, proposals_key, try json(a, proposal_state));
    }
    try request.ensureActive();
    const stamp = if (changed) try publishAuthority(server, a, entity_table, root, native, namespaces.keys(), adoption_revision, request) else null;
    var results = std.json.Array.init(a);
    var applied = true;
    const observations = try observeConsumers(server, a, discovery.consumers.items, namespaces, input.wait, request);
    for (discovery.consumers.items, observations) |consumer, progress| {
        const namespace = namespaces.get(consumer.key_template).?;
        applied = applied and progress.applied;
        try results.append(try json(a, .{ .source_table = consumer.table_name, .resolver = consumer.target.name, .absorbed = absorbed_results.get(consumer.key_template) orelse &.{}, .changed = changed, .revision = namespace.revision, .replay_sequence = namespace.revision, .replay_sequence_kind = "resolver_revision", .accepted = true, .@"error" = progress.observation_error, .applied = progress.applied, .progress = progress }));
    }
    return response(alloc, if (applied) 200 else 202, .{ .entity_table = entity_table.name, .key = key, .atomicity = "namespace", .complete = true, .changed = changed, .proposal = if (proposal_edit) |edit| try findProposal(proposal_state, edit.id) else @as(?resolver.curation.Proposal, null), .metadata_commit = stamp, .applied = applied, .results = std.json.Value{ .array = results } });
}

/// Waiting observes detached workers and yields between samples; it never
/// executes extraction, model inference or promotion on the request worker.
/// Admission has finished. Observation failures cannot turn a committed write
/// into an admission error or cause executeWithRetry to repeat the mutation.
fn observeAccepted(server: anytype, a: std.mem.Allocator, table_name: []const u8, cfg: ObservationTarget, revision: u64, wait: ?resolver.curation.Wait, request: operation.RequestContext) !Progress {
    return observeReplayAccepted(server, a, table_name, cfg, revision, wait, request, null);
}
fn observeReplayAccepted(server: anytype, a: std.mem.Allocator, table_name: []const u8, cfg: ObservationTarget, revision: u64, wait: ?resolver.curation.Wait, request: operation.RequestContext, floor: ?ReplayFloor) !Progress {
    return observeReplayUntil(server, a, table_name, cfg, revision, wait, request, floor) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => .{ .applied = false, .shards = &.{}, .observation_error = @errorName(err) },
    };
}
const ReplayFloor = struct { group: u64, sequence: u64 };
fn observeConsumers(server: anytype, a: std.mem.Allocator, consumers: []const EntityConsumer, namespaces: std.StringArrayHashMapUnmanaged(curation.Namespace), wait: ?resolver.curation.Wait, request: operation.RequestContext) ![]const Progress {
    var tables: std.StringArrayHashMapUnmanaged(std.ArrayList(usize)) = .empty;
    for (consumers, 0..) |consumer, i| {
        const slot = try tables.getOrPut(a, consumer.table_name);
        if (!slot.found_existing) slot.value_ptr.* = .empty;
        try slot.value_ptr.append(a, i);
    }
    const clock = @import("antfly_platform").time;
    const deadline = clock.monotonicNs() +| @as(u64, if (wait) |value| value.timeout_ms else 0) * std.time.ns_per_ms;
    const Probe = struct {
        arena: std.heap.ArenaAllocator = .init(std.heap.smp_allocator),
        table: []const u8,
        indices: []const usize,
        progress: ?[]const Progress = null,
        failure: ?anyerror = null,
        done: bool = false,
        fn run(self: *@This(), srv: @TypeOf(server), inputs: []const EntityConsumer, policy: @TypeOf(namespaces), requested_wait: @TypeOf(wait), context: operation.RequestContext, end: u64) void {
            self.arena.deinit();
            self.arena = .init(std.heap.smp_allocator);
            self.progress = null;
            self.failure = null;
            const scratch = self.arena.allocator();
            const targets = scratch.alloc(ReplayRequirement, self.indices.len) catch |err| {
                self.failure = err;
                return;
            };
            for (self.indices, targets) |index, *target| {
                const consumer = inputs[index];
                target.* = .{ .target = consumer.target, .revision = policy.get(consumer.key_template).?.revision, .floor = null };
            }
            const remaining = (end -| @import("antfly_platform").time.monotonicNs() +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms;
            if (requested_wait != null and remaining == 0) {
                self.failure = error.DeadlineExceeded;
                return;
            }
            var bounded = context;
            if (requested_wait != null) bounded.deadline_ns = @min(bounded.deadline_ns orelse std.math.maxInt(u64), end);
            self.progress = observeReplayTargetsMode(srv, scratch, self.table, targets, if (requested_wait != null) .{ .timeout_ms = @intCast(remaining) } else null, bounded, true) catch |err| {
                self.failure = err;
                return;
            };
            self.done = for (self.progress.?) |value| {
                if (!value.applied) break false;
            } else true;
        }
    };
    const probes = try a.alloc(Probe, tables.count());
    for (tables.keys(), tables.values(), probes) |table, selected, *probe| probe.* = .{ .table = table, .indices = selected.items };
    defer for (probes) |*probe| probe.arena.deinit();
    while (true) {
        var offset: usize = 0;
        while (offset < probes.len) {
            var batch: [16]*Probe = undefined;
            var count: usize = 0;
            while (offset < probes.len and count < batch.len) : (offset += 1) {
                if (probes[offset].done) continue;
                batch[count] = &probes[offset];
                count += 1;
            }
            if (request.fanout_io orelse request.deadline_io) |borrow| {
                var receiver = try borrow.receive();
                const io = receiver.io();
                var tasks: std.Io.Group = .init;
                defer tasks.cancel(io);
                for (batch[0..count]) |probe| tasks.async(io, Probe.run, .{ probe, server, consumers, namespaces, wait, request, deadline });
                tasks.await(io) catch return error.Canceled;
            } else for (batch[0..count]) |probe| Probe.run(probe, server, consumers, namespaces, wait, request, deadline);
            try request.ensureActive();
            for (batch[0..count]) |probe| if (probe.failure) |failure| {
                if (failure == error.OutOfMemory) return error.OutOfMemory;
            };
        }
        const complete = for (probes) |probe| {
            if (!probe.done) break false;
        } else true;
        const borrow = request.fanout_io orelse request.deadline_io;
        if (complete or wait == null or clock.monotonicNs() >= deadline or borrow == null) break;
        var receiver = try borrow.?.receive();
        try receiver.io().sleep(.fromNanoseconds(@intCast(@min(100 * std.time.ns_per_ms, deadline -| clock.monotonicNs()))), .awake);
    }
    const result = try a.alloc(Progress, consumers.len);
    for (probes) |probe| {
        if (probe.progress) |progress| {
            for (probe.indices, progress) |index, value| {
                const rows = try a.dupe(GroupProgress, value.shards);
                for (rows) |*row| {
                    row.resolution.blocked_reason = try a.dupe(u8, row.resolution.blocked_reason);
                    row.promotion.blocked_reason = try a.dupe(u8, row.promotion.blocked_reason);
                }
                result[index] = .{ .applied = value.applied, .shards = rows, .observation_error = try a.dupe(u8, value.observation_error) };
            }
        } else for (probe.indices) |index| result[index] = .{ .applied = false, .shards = &.{}, .observation_error = if (probe.failure) |err| @errorName(err) else "Unavailable" };
    }
    return result;
}

fn pendingProgress(a: std.mem.Allocator, count: usize, failure: []const u8) ![]const Progress {
    const result = try a.alloc(Progress, count);
    @memset(result, .{ .applied = false, .shards = &.{}, .observation_error = failure });
    return result;
}
fn observeReplayUntil(server: anytype, a: std.mem.Allocator, table_name: []const u8, cfg: ObservationTarget, revision: u64, wait: ?resolver.curation.Wait, request: operation.RequestContext, floor: ?ReplayFloor) !Progress {
    return (try observeReplayTargets(server, a, table_name, &.{.{ .target = cfg, .revision = revision, .floor = floor }}, wait, request))[0];
}
fn observeReplayTargets(server: anytype, a: std.mem.Allocator, table_name: []const u8, targets: []const ReplayRequirement, wait: ?resolver.curation.Wait, request: operation.RequestContext) ![]const Progress {
    return observeReplayTargetsMode(server, a, table_name, targets, wait, request, false);
}
fn observeReplayTargetsMode(server: anytype, a: std.mem.Allocator, table_name: []const u8, targets: []const ReplayRequirement, wait: ?resolver.curation.Wait, request: operation.RequestContext, once: bool) ![]const Progress {
    const clock = @import("antfly_platform").time;
    const deadline = clock.monotonicNs() +| @as(u64, if (wait) |w| w.timeout_ms else 0) * std.time.ns_per_ms;
    while (true) {
        try request.ensureActive();
        var iteration = std.heap.ArenaAllocator.init(std.heap.smp_allocator);
        defer iteration.deinit();
        const scratch = iteration.allocator();
        var observation_request = try request.platformDeadline();
        if (wait != null) observation_request.deadline_ns = @min(observation_request.deadline_ns orelse std.math.maxInt(u64), deadline);
        const scoped = server.source.vtable.supports_resolver_catalog_read;
        const page: ?catalog_read.Page = if (scoped) readResolverCatalog(server, scratch, .{ .table_name = table_name, .projection_names = &.{} }, observation_request) catch |err| {
            try request.ensureActive();
            if (err == error.DeadlineExceeded and clock.monotonicNs() >= deadline) return pendingProgress(a, targets.len, @errorName(err));
            return err;
        } else null;
        var snapshot = if (page) |*value| value.snapshot() else (server.source.linearizableSnapshot(observation_request) catch |err| {
            try request.ensureActive();
            if (err == error.DeadlineExceeded and clock.monotonicNs() >= deadline) return pendingProgress(a, targets.len, @errorName(err));
            return err;
        }) orelse return error.TableNotFound;
        const table = tableByName(&snapshot, table_name) catch |err| {
            if (!scoped) server.source.freeAdminSnapshot(&snapshot);
            return err;
        };
        const observation = sampleForRequirements(server, scratch, &snapshot, table, observation_request, wait != null, targets) catch |err| {
            if (!scoped) server.source.freeAdminSnapshot(&snapshot);
            try request.ensureActive();
            if (err == error.DeadlineExceeded and clock.monotonicNs() >= deadline) return pendingProgress(a, targets.len, @errorName(err));
            return err;
        };
        if (!scoped) server.source.freeAdminSnapshot(&snapshot);
        const progress = try scratch.alloc(Progress, targets.len);
        var applied = true;
        for (targets, progress) |target, *out| {
            out.* = try observation.progressForReplay(scratch, target);
            applied = applied and out.applied;
        }
        const borrow = request.fanout_io orelse request.deadline_io;
        if (once or applied or clock.monotonicNs() >= deadline or borrow == null) {
            const owned = try a.alloc(Progress, progress.len);
            for (progress, owned) |value, *out| {
                const rows = try a.dupe(GroupProgress, value.shards);
                for (rows) |*row| {
                    row.resolution.blocked_reason = try a.dupe(u8, row.resolution.blocked_reason);
                    row.promotion.blocked_reason = try a.dupe(u8, row.promotion.blocked_reason);
                }
                out.* = .{ .applied = value.applied, .shards = rows };
            }
            return owned;
        }
        var receiver = try borrow.?.receive();
        try receiver.io().sleep(.fromNanoseconds(@intCast(@min(100 * std.time.ns_per_ms, deadline -| clock.monotonicNs()))), .awake);
    }
}

const GroupReviewToken = struct { group: u64, value: ?[]const u8 = null };
fn reviewsOperation(server: anytype, alloc: std.mem.Allocator, a: std.mem.Allocator, snapshot: anytype, table: anytype, cfg: Config, op: Operation, id: []const u8, body: []const u8, query: []const u8, request: operation.RequestContext) !@import("public_table_http.zig").OwnedResponse {
    var groups: std.AutoArrayHashMapUnmanaged(u64, void) = .empty;
    for (snapshot.ranges) |range| if (range.table_id == table.table_id) try groups.put(a, range.group_id, {});
    const ordered = try a.dupe(u64, groups.keys());
    std.mem.sort(u64, ordered, {}, std.sort.asc(u64));
    if (groups.count() == 0) return error.StorageUnavailable;
    if (op == .review) {
        if (query.len > 0) return error.InvalidResolverCurationRequest;
        const token = try curation.decodeToken(GroupReviewToken, a, id);
        if (!groups.contains(token.group)) return error.ResolverCurationConflict;
        const Input = struct { actor: []const u8 = "", reason: []const u8 = "", decision: resolver.Decision, key: ?[]const u8 = null, table: ?[]const u8 = null, wait: ?resolver.curation.Wait = null };
        const input = try std.json.parseFromSliceLeaky(Input, a, body, .{});
        const bytes = try std.json.Stringify.valueAlloc(a, curation.ReviewRequest{ .actor = input.actor, .reason = input.reason, .operation = .review, .resolver = cfg.name, .expected_revision = cfg.curation_revision, .id = token.value, .decision = input.decision, .key = input.key, .table = input.table }, .{});
        if (input.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
        const reply = try server.executeResolverOperation(a, table.name, token.group, bytes, request);
        var value = try std.json.parseFromSliceLeaky(std.json.Value, a, reply, .{});
        const sequence = value.object.get("replay_sequence").?.integer;
        const progress = try observeReplayAccepted(server, a, table.name, ObservationTarget.fromConfig(cfg), cfg.curation_revision, input.wait, request, .{ .group = token.group, .sequence = @intCast(sequence) });
        try value.object.put(a, "applied", .{ .bool = progress.applied });
        try value.object.put(a, "progress", try json(a, progress));
        return response(alloc, if (progress.applied) 200 else 202, value);
    }
    var cursor: ?GroupReviewToken = null;
    var limit: u16 = 64;
    var parts = std.mem.splitScalar(u8, query, '&');
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    while (parts.next()) |part| {
        if (part.len == 0) continue;
        const split = std.mem.indexOfScalar(u8, part, '=') orelse return error.InvalidResolverCurationRequest;
        const name = part[0..split];
        const slot = try seen.getOrPut(a, name);
        if (slot.found_existing) return error.InvalidResolverCurationRequest;
        const value = part[split + 1 ..];
        if (std.mem.eql(u8, name, "cursor")) cursor = try curation.decodeToken(GroupReviewToken, a, value) else if (std.mem.eql(u8, name, "limit")) limit = try std.fmt.parseInt(u16, value, 10) else return error.InvalidResolverCurationRequest;
    }
    if (limit == 0 or limit > 256) return error.InvalidResolverCurationRequest;
    const group = if (cursor) |c| c.group else ordered[0];
    const index = std.mem.indexOfScalar(u64, ordered, group) orelse return error.ResolverCurationConflict;
    const bytes = try std.json.Stringify.valueAlloc(a, curation.ReviewRequest{ .operation = .reviews, .resolver = cfg.name, .cursor = if (cursor) |c| c.value else null, .limit = limit }, .{});
    const reply = try server.executeResolverOperation(a, table.name, group, bytes, request);
    var page = try std.json.parseFromSliceLeaky(std.json.Value, a, reply, .{});
    if (page != .object) return error.InvalidResolverCurationRequest;
    const items = page.object.getPtr("reviews") orelse return error.InvalidResolverCurationRequest;
    for (items.array.items) |*item| {
        const raw = item.object.get("id").?.string;
        try item.object.put(a, "id", .{ .string = try curation.encodeToken(a, GroupReviewToken{ .group = group, .value = raw }) });
    }
    const next = page.object.get("next_cursor") orelse .null;
    const next_token: ?GroupReviewToken = if (next == .string) .{ .group = group, .value = next.string } else if (index + 1 < groups.count()) .{ .group = ordered[index + 1] } else null;
    try page.object.put(a, "next_cursor", if (next_token) |token| .{ .string = try curation.encodeToken(a, token) } else .null);
    return response(alloc, 200, page);
}

fn mutationRequest(a: std.mem.Allocator, op: Operation, key: []const u8, body: []const u8, query: []const u8) !resolver.curation.Request {
    var root = try std.json.parseFromSliceLeaky(std.json.Value, a, if (body.len == 0) "{}" else body, .{});
    if (root != .object or root.object.contains("action")) return error.InvalidResolverCurationRequest;
    const action: []const u8 = switch (op) {
        .merge => "merge",
        .unmerge => "unmerge",
        .suppress => if (root.object.contains("add") or root.object.contains("remove")) "suppressions" else "suppress",
        .unsuppress => "unsuppress",
        else => return error.InvalidResolverCurationRequest,
    };
    if (key.len > 0) {
        if (root.object.contains("key") or root.object.contains("from")) return error.InvalidResolverCurationRequest;
        try root.object.put(a, "key", .{ .string = key });
    }
    if (query.len > 0) {
        if (op != .unmerge or (!std.mem.eql(u8, query, "pin_distinct=true") and !std.mem.eql(u8, query, "pin_distinct=false"))) return error.InvalidResolverCurationRequest;
        if (root.object.contains("pin_distinct")) return error.InvalidResolverCurationRequest;
        try root.object.put(a, "pin_distinct", .{ .bool = std.mem.eql(u8, query, "pin_distinct=true") });
    }
    try root.object.put(a, "action", .{ .string = action });
    return std.json.parseFromSliceLeaky(resolver.curation.Request, a, try std.json.Stringify.valueAlloc(a, root, .{}), .{});
}

fn resolveQuery(server: anytype, alloc: std.mem.Allocator, a: std.mem.Allocator, table: anytype, all: []const Config, cfg: Config, body: []const u8, request: operation.RequestContext) !@import("public_table_http.zig").OwnedResponse {
    const Input = struct { text: ?[]const u8 = null, extraction: ?std.json.Value = null };
    const input = try std.json.parseFromSliceLeaky(Input, a, body, .{});
    if ((input.text == null) == (input.extraction == null) or cfg.source_artifact_kind != .asset) return error.InvalidArgument;
    const Server = @typeInfo(@TypeOf(server)).pointer.child;
    const Bridge = struct {
        server: *Server,
        request: operation.RequestContext,
        source_table: []const u8,
        arena: std.mem.Allocator,
        revision: ?u64 = null,
        filters: std.StringHashMapUnmanaged([]const u8) = .empty,
        fn filter(ptr: *anyopaque, _: std.mem.Allocator, physical: []const u8) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.request.ensureActive();
            return self.filters.get(physical) orelse error.Forbidden;
        }
        fn options(ptr: *anyopaque) local.asset_producer_runtime.Runtime.Options {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .antfly_provider = self.server.antfly_provider, .inference_api_url = self.server.configuredInferenceAPIURL(), .secret_store = self.server.cfg.secret_store, .source_table = self.source_table };
        }
        fn bind(ptr: *anyopaque, allocator: std.mem.Allocator, names: []const []const u8) ![][]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const domain = local.system_catalog_domain;
            var scratch = std.heap.ArenaAllocator.init(allocator);
            defer scratch.deinit();
            const arena = scratch.allocator();
            const out = try allocator.alloc([]u8, names.len);
            var kept: usize = 0;
            errdefer {
                for (out[0..kept]) |name| allocator.free(name);
                allocator.free(out);
            }
            var offset: usize = 0;
            while (offset < names.len) {
                const end = @min(names.len, offset + 256);
                const targets = try arena.alloc(domain.Target, end - offset);
                const filters = try arena.alloc([]const u8, targets.len);
                for (names[offset..end], targets, filters) |name, *target, *predicate| {
                    target.* = try domain.Target.parse(name);
                    const logical = try target.resourceNameAlloc(arena);
                    predicate.* = try self.server.authorizedReadFilter(self.arena, self.request, logical);
                }
                const bound: ?[]const ?domain.ResolvedTable = if (self.server.source.vtable.system_catalog != null) resolved: {
                    const bytes = try self.server.source.systemCatalog(arena, self.request, .{ .resolve_many = .{ .targets = targets, .expected_revision = self.revision } });
                    const result = try std.json.parseFromSliceLeaky(domain.ResolvedMany, arena, bytes, .{});
                    if (result.tables.len != targets.len) return error.InvalidCatalogRecord;
                    self.revision = result.revision;
                    break :resolved result.tables;
                } else null;
                for (names[offset..end], filters, out[offset..end], 0..) |name, predicate, *physical, i| {
                    const storage = if (bound) |tables| (tables[i] orelse return error.TableNotFound).name else name;
                    try self.filters.put(self.arena, try self.arena.dupe(u8, storage), predicate);
                    physical.* = try allocator.dupe(u8, storage);
                    kept += 1;
                }
                offset = end;
            }
            return out;
        }
    };
    var bridge: Bridge = .{ .server = server, .request = request, .source_table = table.name, .arena = a };
    // Preflight every destination actually executed, including compositional
    // siblings, before extraction/model work. Redirect survivors bind through
    // the same credential-aware callback and catalog revision on demand.
    var names: std.StringArrayHashMapUnmanaged(void) = .empty;
    try names.put(a, cfg.table, {});
    for (all) |item| if (!item.retired and cfg.sharesRoutingDomain(item)) try names.put(a, item.table, {});
    _ = try Bridge.bind(&bridge, a, names.keys());
    const io = server.inferenceIo();
    const invocation: local.storage_db_enrichment_asset_producer.InvocationContext = .{ .io = io, .deadline_ns = request.deadline_ns, .cancellation = request.cancellation };
    var candidates: @import("distributed_candidate_source.zig").DistributedCandidateSource = .{
        .reads = server.table_reads orelse return error.UnsupportedOperation,
        .io = io,
        .request_context = invocation,
        .producer_options = .{ .ptr = &bridge, .get = Bridge.options },
        .catalog_binding = .{ .ptr = &bridge, .bind_fn = Bridge.bind },
        .read_filter = .{ .ptr = &bridge, .get = Bridge.filter },
    };
    const managed = local.inference_managed_embedder;
    const options: managed.InitOptions = .{
        .antfly_provider = server.antfly_provider,
        .io = io,
        .bounded_http_request = true,
        .deadline_ns = request.deadline_ns,
        .cancellation = request.cancellation,
        .inference_api_url = server.configuredInferenceAPIURL(),
        .secret_store = server.cfg.secret_store,
        .source_table = table.name,
    };
    const extraction = if (input.text) |text| extracted: {
        var enrichments: std.ArrayListUnmanaged(types.EnrichmentConfig) = .empty;
        try local.metadata_local_index_reconcile.collectDesiredEnrichmentsFromJson(a, table.indexes_json, options, &enrichments);
        for (enrichments.items) |enrichment| {
            if (enrichment.kind != .asset or !std.mem.eql(u8, enrichment.name, cfg.source_artifact)) continue;
            var producer_config = try local.storage_db_enrichment_asset_producer.parseProducerConfig(a, enrichment.producer_json);
            defer producer_config.deinit(a);
            if (producer_config.type != .extractor) return error.InvalidArgument;
            const runtime = try local.asset_producer_runtime.Runtime.createOwned(a, io, Bridge.options(&bridge));
            defer {
                runtime.deinit();
                a.destroy(runtime);
            }
            break :extracted try runtime.producer().withInvocationContext(invocation).produce(a, .{ .producer_type = .extractor, .config_json = producer_config.config_json, .content_type = enrichment.content_type, .source_text = text, .item_id = "query" });
        }
        return error.InvalidArgument;
    } else try std.json.Stringify.valueAlloc(a, input.extraction.?, .{});
    var embedding: ?managed.ManagedEmbedder = null;
    defer if (embedding) |*value| value.deinit();
    if (cfg.name_embedding.len > 0) {
        const physical = if (candidates.catalog_binding) |binding| try binding.bindOne(a, cfg.table) else cfg.table;
        embedding = try managed.ManagedEmbedder.initFromIndexesJsonWithOptions(a, try readResolverIndexes(server, a, physical, request), options);
    }
    var active: std.ArrayListUnmanaged(Config) = .empty;
    for (all) |item| if (!item.retired) try active.append(a, item);
    const result = try local.storage_db_resolution_runtime.resolveQueryExtractionAlloc(a, active.items, &cfg, extraction, candidates.candidateSource(), if (embedding) |*value| value.denseInterface() else null);
    try request.ensureActive();
    if (!std.mem.eql(u8, try readResolverIndexes(server, a, table.name, request), table.indexes_json)) return error.Busy;
    return response(alloc, 200, .{ .extraction = try std.json.parseFromSliceLeaky(std.json.Value, a, extraction, .{}), .resolution = try std.json.parseFromSliceLeaky(std.json.Value, a, result, .{}) });
}

/// Query-time embedding configuration and final source validation share the
/// scoped definition path, including nested reads after candidate execution.
fn readResolverIndexes(server: anytype, a: std.mem.Allocator, name: []const u8, request: operation.RequestContext) ![]const u8 {
    if (server.source.vtable.supports_resolver_catalog_read) return (try readResolverCatalog(server, a, .{ .table_name = name, .projection_names = &.{} }, request)).table.indexes_json;
    var snapshot = (try server.source.linearizableSnapshot(request)) orelse return error.TableNotFound;
    defer server.source.freeAdminSnapshot(&snapshot);
    return a.dupe(u8, (try tableByName(&snapshot, name)).indexes_json);
}

fn response(a: std.mem.Allocator, status: u16, value: anytype) !@import("public_table_http.zig").OwnedResponse {
    return .{ .status = status, .json = true, .body = try std.json.Stringify.valueAlloc(a, value, .{}) };
}
fn json(a: std.mem.Allocator, value: anytype) !std.json.Value {
    return std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, value, .{}), .{});
}

test "resolver curation HTTP routes keep encoded keys inside one resource and classify reads" {
    const route = matchTablePath("/tables/notes/resolvers/concepts/redirects/concept%2Fkillall").?;
    try std.testing.expectEqualStrings("notes", route.table_name);
    try std.testing.expectEqualStrings("concept%2Fkillall", route.key);
    try std.testing.expectEqual(Operation.unmerge, route.op);
    try std.testing.expect(matchSuffix("resolvers/concepts/redirects/concept/killall") == null);
    try std.testing.expectEqual(Operation.entity_merge, matchTablePath("/tables/concepts/entities/concept%2Fnote:merge").?.op);
    try std.testing.expectEqual(Operation.resolve, matchSuffix("resolvers/concepts:resolve").?.op);
    try std.testing.expect(readOnly(.resolve));
    try std.testing.expect(!readOnly(.unmerge));
}

test "resolver curation HTTP ingress handlers compile against the serving owner" {
    const Handler = @import("httpx_handler.zig").AntflyApiHandler;
    _ = &Handler.listResolvers;
    _ = &Handler.mergeResolverKeys;
    _ = &Handler.resolveResolverQuery;
    _ = &Handler.mergeEntity;
}

test "resolver curation progress requires the revision and complete bounded backfill" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var observation: Sample = .{};
    try observation.groups.put(a, 1, {});
    var by_group: std.AutoHashMapUnmanaged(u64, GroupProgress) = .empty;
    try by_group.put(a, 1, .{ .group_id = 1, .available = true, .fresh = true, .curation_revision = 2, .config_generation = 3, .backfill_pending = true, .resolution = .{ .enabled = true, .target_sequence = 5, .applied_sequence = 5 } });
    try observation.observations.put(a, "concepts", by_group);
    const cfg: Config = .{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "{{_entity.text}}", .promote = false, .config_generation = 3 };
    try std.testing.expect(!(try observation.progress(a, ObservationTarget.fromConfig(cfg), 2)).applied);
    observation.observations.getPtr("concepts").?.getPtr(1).?.backfill_pending = false;
    try std.testing.expect((try observation.progress(a, ObservationTarget.fromConfig(cfg), 2)).applied);
    try std.testing.expect(!(try observation.progress(a, ObservationTarget.fromConfig(cfg), 3)).applied);
    observation.observations.getPtr("concepts").?.getPtr(1).?.fresh = false;
    try std.testing.expect(!(try observation.progress(a, ObservationTarget.fromConfig(cfg), 2)).applied);
}

test "resolver curation progress obtains missing diagnostics from the current owner" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var observation: Sample = .{};
    try observation.groups.put(a, 7, {});
    const cfg: Config = .{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "{{_entity.text}}", .promote = false };
    try std.testing.expect(!(try observation.progress(a, ObservationTarget.fromConfig(cfg), 4)).applied);
    const row: curation.ProgressRow = .{ .name = "concepts", .config_generation = 0, .effective_generation = 4, .curation_revision = 4, .backfill_pending = false, .resolution = .{ .enabled = true, .target_sequence = 9, .applied_sequence = 9 }, .promotion = .{} };
    try mergeOwnerProgress(a, &observation, 7, .{ .resolvers = &.{row} });
    try std.testing.expect((try observation.progress(a, ObservationTarget.fromConfig(cfg), 4)).applied);
    try std.testing.expect(!(try observation.progress(a, ObservationTarget.fromConfig(cfg), 5)).applied);
}

test "resolver curation progress completes a proven empty owner without invented workers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var observation: Sample = .{};
    try observation.groups.put(a, 1, {});
    const cfg: Config = .{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "{{_entity.text}}", .promote = false };
    var row: curation.ProgressRow = .{ .name = "concepts", .config_generation = 0, .effective_generation = 4, .curation_revision = 4, .backfill_pending = false, .empty = true, .resolution = .{}, .promotion = .{} };
    try mergeOwnerProgress(a, &observation, 1, .{ .resolvers = &.{row} });
    try std.testing.expect((try observation.progress(a, ObservationTarget.fromConfig(cfg), 4)).applied);
    row.resolution.target_sequence = 1;
    try mergeOwnerProgress(a, &observation, 1, .{ .resolvers = &.{row} });
    try std.testing.expect(!(try observation.progress(a, ObservationTarget.fromConfig(cfg), 4)).applied);
}

test "resolver curation progress probes an idle heartbeat below an accepted journal target" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Server = struct {
        alloc: std.mem.Allocator,
        probes: usize = 0,
        local_resolution: u64 = 4,
        pub fn localTableRuntimeStatusesWithSnapshot(self: *@This(), _: []const u8, _: anytype) !?runtime_status.LocalTableRuntimeStatuses {
            var row: runtime_status.LocalTableRuntimeStatus = .{ .group_id = 7, .metadata = .{ .source = .remote_store, .freshness = .fresh }, .stats = .{} };
            row.stats.resolution = .{ .enabled = true, .target_sequence = self.local_resolution, .applied_sequence = self.local_resolution };
            row.stats.promotion = .{ .enabled = true, .target_sequence = 4, .applied_sequence = 4 };
            row.stats.resolver_replay.resolvers = &.{.{ .name = "concepts", .config_generation = 1 }};
            var rows = [_]runtime_status.LocalTableRuntimeStatus{row};
            const observed: runtime_status.LocalTableRuntimeStatuses = .{ .items = &rows };
            return try observed.clone(self.alloc);
        }
        pub fn executeResolverOperation(self: *@This(), allocator: std.mem.Allocator, _: []const u8, group: u64, body: []const u8, _: operation.RequestContext) ![]u8 {
            try std.testing.expectEqual(@as(u64, 7), group);
            const scoped = try std.json.parseFromSlice(curation.ReviewRequest, allocator, body, .{});
            defer scoped.deinit();
            try std.testing.expectEqual(@as(usize, 1), scoped.value.resolver_names.?.len);
            try std.testing.expectEqualStrings("concepts", scoped.value.resolver_names.?[0]);
            self.probes += 1;
            return std.json.Stringify.valueAlloc(allocator, curation.ProgressReport{ .resolvers = &.{.{
                .name = "concepts",
                .config_generation = 1,
                .effective_generation = 1,
                .curation_revision = 0,
                .backfill_pending = false,
                .resolution = .{ .enabled = true, .target_sequence = 5, .applied_sequence = 5 },
                .promotion = .{ .enabled = true, .target_sequence = 5, .applied_sequence = 5 },
            }} }, .{});
        }
    };
    var server: Server = .{ .alloc = std.testing.allocator };
    const Range = struct { table_id: u64, group_id: u64 };
    const snapshot = .{ .ranges = &[_]Range{.{ .table_id = 1, .group_id = 7 }} };
    const table: @import("../metadata/table_manager.zig").TableRecord = .{ .table_id = 1, .name = "source", .indexes_json =
        \\{"knowledge":{"type":"graph","resolvers":[{"name":"concepts","table":"entities","source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{uslug _entity.text}}","config_generation":1,"promote":true}]}}
    };
    const target: ObservationTarget = .{ .name = "concepts", .config_generation = 1, .promote = false };
    const idle = try sample(&server, a, &snapshot, &table, .{}, true);
    try std.testing.expectEqual(@as(usize, 0), server.probes);
    try std.testing.expect((try idle.progress(a, target, 0)).applied);
    const current = try sampleForReplay(&server, a, &snapshot, &table, .{}, true, .{ .target = target, .revision = 0, .floor = .{ .group = 7, .sequence = 5 } });
    try std.testing.expectEqual(@as(usize, 1), server.probes);
    try std.testing.expect((try current.progressForReplay(a, .{ .target = target, .revision = 0, .floor = .{ .group = 7, .sequence = 5 } })).applied);
    try std.testing.expectEqual(@as(u64, 5), current.observations.get("concepts").?.get(7).?.resolution.applied_sequence);
    // Promotion must independently satisfy the journal floor, even when
    // resolution has caught up and both cached stages look idle.
    server.local_resolution = 5;
    var promoted = target;
    promoted.promote = true;
    _ = try sampleForReplay(&server, a, &snapshot, &table, .{}, true, .{ .target = promoted, .revision = 0, .floor = .{ .group = 7, .sequence = 5 } });
    try std.testing.expectEqual(@as(usize, 2), server.probes);
    // A point wait must not probe an unrelated resolver absent from heartbeats.
    var unrelated = table;
    unrelated.indexes_json = "{\"resolvers\":[{\"name\":\"unrelated\",\"table\":\"entities\",\"source_artifact\":\"other\",\"resolution_artifact\":\"other-out\",\"key_template\":\"other/{{_key}}\"}]}";
    server.local_resolution = 5;
    const probes_before = server.probes;
    const selected = try sampleForReplay(&server, a, &snapshot, &unrelated, .{}, true, .{ .target = target, .revision = 0, .floor = .{ .group = 7, .sequence = 5 } });
    try std.testing.expectEqual(probes_before, server.probes);
    try std.testing.expect((try selected.progress(a, target, 0)).applied);
}

test "resolver curation HTTP queue capacity reserves both metadata CAS definitions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const pure = resolver.curation;
    var state: pure.State = .{};
    var count: usize = 0;
    while (true) : (count += 1) {
        const next = try pure.propose(a, state, &.{}, .{ .action = .suppress, .key = try std.fmt.allocPrint(a, "concept/{d}", .{count}) }, .{});
        const bytes = try std.json.Stringify.valueAlloc(a, .{ ._curation_proposals = next.state }, .{});
        validateProposalPublicationCapacity(std.testing.allocator, .{ .table_id = 1, .name = "entities", .indexes_json = bytes }, next.state) catch |err| {
            try std.testing.expectEqual(error.ResolverCurationLimitExceeded, err);
            break;
        };
        state = next.state;
    }
    try std.testing.expect(count > 1);
    // The shared 4 MiB queue still has capacity, but HTTP's two-definition
    // transition is the tighter authority and must reject creation earlier.
    const actor: [256]u8 = @splat(0);
    const reason: [4096]u8 = @splat(0);
    for (0..count) |i| {
        state = (try pure.decide(a, state, &.{}, &.{}, state.proposals[i].id, .{ .accept = false }, .{ .actor = &actor, .reason = &reason, .revision = std.math.maxInt(u64), .created_at_ms = std.math.maxInt(u64) })).state;
        const bytes = try std.json.Stringify.valueAlloc(a, .{ ._curation_proposals = state }, .{});
        try validateProposalPublicationCapacity(std.testing.allocator, .{ .table_id = 1, .name = "entities", .indexes_json = bytes }, state);
    }
}

test "resolver curation HTTP proposal evidence cannot install enrichment producers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const evidence = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \\{"enrichments":[{"name":"from_evidence","kind":"chunk","field":"body","chunk_size":256}],"nested":{"type":"embeddings","embedder":{"provider":"invalid"},"enrichments":"not configuration"}}
    , .{});
    const proposed = try resolver.curation.propose(a, .{}, &.{}, .{ .action = .suppress, .key = "concept/x", .evidence = evidence }, .{});
    const bytes = try std.json.Stringify.valueAlloc(a, .{ ._curation_proposals = proposed.state }, .{});
    const discovered = try local.api_local_indexes.collectArtifactEnrichmentsFromTableIndexesJsonWithOptions(std.testing.allocator, bytes, .{});
    defer types.freeEnrichmentConfigs(std.testing.allocator, discovered);
    try std.testing.expectEqual(@as(usize, 0), discovered.len);
}

test "resolver curation HTTP control metadata is opaque to discovery and authorization" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const payload = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \\{"enrichments":"not configuration","resolvers":[{"table":"protected"}],"nested":{"resolvers":"not configuration"},"_antfly_destination_authorization_v1":{"evidence":"preserve me"},"type":"embeddings","embedder":{"provider":"invalid"}}
    , .{});
    var root: std.json.Value = .{ .object = .{} };
    for ([_][]const u8{ "_curation_proposals", "_resolver_curation", "_resolver_namespaces" }) |key| {
        try root.object.put(a, key, payload);
    }
    // After parsing the document, discovery needs no allocations for opaque
    // payloads, regardless of their configuration-shaped contents.
    var empty: [0]u8 = .{};
    var fixed = std.heap.FixedBufferAllocator.init(&empty);
    var enrichments: std.ArrayListUnmanaged(types.EnrichmentConfig) = .empty;
    try local.api_local_indexes.collectArtifactEnrichmentsFromValueWithOptions(fixed.allocator(), root, .{}, &enrichments);
    try std.testing.expectEqual(@as(usize, 0), enrichments.items.len);
    var declarations: std.ArrayListUnmanaged(Config) = .empty;
    try local.metadata_local_index_reconcile.collectDesiredResolvers(fixed.allocator(), root, &declarations);
    try std.testing.expectEqual(@as(usize, 0), declarations.items.len);
    const managed = local.inference_managed_embedder;
    try managed.validateEmbeddingProducerOwnershipValue(a, root);
    var embedder = try managed.ManagedEmbedder.initFromIndexValueObject(std.testing.allocator, root);
    defer embedder.deinit();
    try std.testing.expectEqual(@as(usize, 0), embedder.entries.len);
    const bytes = try std.json.Stringify.valueAlloc(a, root, .{});
    const grants = local.api_stored_destination_authorization;
    try std.testing.expect(!try grants.configurationHasDestinations(a, "[]", bytes));
    try grants.authorizeIndexesJson(a, bytes, "source", .{ .auth_enabled = true });
    const sealed = try grants.sealIndexesJsonForPrincipalAlloc(a, bytes, "source", "actor", .{ .auth_enabled = true });
    // Sealing real declarations must leave opaque evidence (including fields
    // named like internal credentials) semantically unchanged.
    const restored = try std.json.parseFromSliceLeaky(std.json.Value, a, sealed, .{});
    try std.testing.expectEqualStrings(bytes, try std.json.Stringify.valueAlloc(a, restored, .{}));

    const live = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \\{"enrichments":[{"name":"live","kind":"chunk","field":"body","chunk_size":256}],"resolvers":[{"name":"r","table":"entities","source_artifact":"mentions","resolution_artifact":"resolved","key_template":"concept/{{text}}"}]}
    , .{});
    // Actual declarations may still appear in nested index objects.
    try root.object.put(a, "graph", live);
    try local.api_local_indexes.collectArtifactEnrichmentsFromValueWithOptions(a, root, .{}, &enrichments);
    try std.testing.expectEqual(@as(usize, 1), enrichments.items.len);
    try std.testing.expectEqualStrings("live", enrichments.items[0].name);
    try local.metadata_local_index_reconcile.collectDesiredResolvers(a, root, &declarations);
    try std.testing.expectEqual(@as(usize, 1), declarations.items.len);
    const with_live = try std.json.Stringify.valueAlloc(a, root, .{});
    try std.testing.expect(try grants.configurationHasDestinations(a, "[]", with_live));
    try std.testing.expectError(error.StoredDestinationAuthorizationMissing, grants.authorizeIndexesJson(a, with_live, "source", .{ .auth_enabled = true }));
    const live_sealed = try grants.sealIndexesJsonForPrincipalAlloc(a, with_live, "source", grants.auth_disabled_principal, .{ .auth_enabled = false });
    try grants.authorizeIndexesJson(a, live_sealed, "source", .{ .auth_enabled = false });
    const sealed_root = try std.json.parseFromSliceLeaky(std.json.Value, a, live_sealed, .{});
    for ([_][]const u8{ "_curation_proposals", "_resolver_curation", "_resolver_namespaces" }) |key| {
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, payload, .{}), try std.json.Stringify.valueAlloc(a, sealed_root.object.get(key).?, .{}));
    }
}

test "resolver curation HTTP namespace edits preserve resolver-shaped proposal evidence" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const declaration = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \\{"resolvers":[{"name":"r","table":"entities","source_artifact":"mentions","resolution_artifact":"resolved","key_template":"concept/{{text}}"}]}
    , .{});
    const proposed = try resolver.curation.propose(a, .{}, &.{}, .{ .action = .suppress, .key = "concept/x", .evidence = declaration }, .{});
    const before = try std.json.Stringify.valueAlloc(a, proposed.state, .{});
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .graph = declaration, ._curation_proposals = proposed.state }, .{});
    const plan = try curation.planNamespaceMetadata(a, bytes, "r", .{ .table = "new_entities", .key_template = "new/{{text}}", .reset_merges = true, .reset_curation = true });
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, plan.indexes_json, .{});
    try std.testing.expectEqualStrings(before, try std.json.Stringify.valueAlloc(a, root.object.get("_curation_proposals").?, .{}));
    const declared = root.object.get("graph").?.object.get("resolvers").?.array.items[0];
    try std.testing.expectEqualStrings("new_entities", declared.object.get("table").?.string);
    try std.testing.expectEqualStrings("new/{{text}}", declared.object.get("key_template").?.string);
    const declaring = try owners(a, plan.indexes_json, "r");
    try std.testing.expectEqual(@as(usize, 1), declaring.len);
    try std.testing.expectEqualStrings("graph", declaring[0]);
}

test "resolver curation progress shares catalog and status reads across namespace consumers" {
    const Snapshot = @import("../metadata/api.zig").AdminSnapshot;
    const Source = struct {
        vtable: struct { supports_resolver_catalog_read: bool = false } = .{},
        reads: usize = 0,
        snapshot: Snapshot,
        pub fn systemCatalog(_: *@This(), _: std.mem.Allocator, _: operation.RequestContext, _: anytype) ![]u8 {
            return error.UnsupportedOperation;
        }
        pub fn linearizableSnapshot(self: *@This(), _: operation.RequestContext) !?Snapshot {
            self.reads += 1;
            return self.snapshot;
        }
        pub fn freeAdminSnapshot(_: *@This(), _: *Snapshot) void {}
    };
    const Server = struct {
        alloc: std.mem.Allocator,
        source: Source,
        samples: usize = 0,
        pub fn localTableRuntimeStatusesWithSnapshot(self: *@This(), _: []const u8, _: anytype) !?runtime_status.LocalTableRuntimeStatuses {
            self.samples += 1;
            var row: runtime_status.LocalTableRuntimeStatus = .{ .group_id = 7, .metadata = .{ .source = .remote_store, .freshness = .fresh }, .stats = .{} };
            row.stats.resolution = .{ .enabled = true };
            row.stats.resolver_replay.resolvers = &.{ .{ .name = "concepts", .curation_revision = 4 }, .{ .name = "events", .curation_revision = 4 } };
            var rows = [_]runtime_status.LocalTableRuntimeStatus{row};
            const statuses: runtime_status.LocalTableRuntimeStatuses = .{ .items = &rows };
            return try statuses.clone(self.alloc);
        }
        pub fn executeResolverOperation(_: *@This(), _: std.mem.Allocator, _: []const u8, _: u64, _: []const u8, _: operation.RequestContext) ![]u8 {
            return error.UnsupportedOperation;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tables = [_]@import("../metadata/table_manager.zig").TableRecord{.{ .name = "source", .table_id = 1, .indexes_json = "{}" }};
    var ranges = [_]@import("../metadata/table_manager.zig").RangeRecord{.{ .table_id = 1, .group_id = 7, .start_key = "" }};
    var server: Server = .{ .alloc = std.testing.allocator, .source = .{ .snapshot = .{ .status = .{ .metadata_group_id = 0, .metrics = .{} }, .tables = &tables, .ranges = &ranges, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} } } };
    var namespaces: std.StringArrayHashMapUnmanaged(curation.Namespace) = .empty;
    try namespaces.put(a, "concept/{{ _entity.text }}", .{ .key_template = "concept/{{ _entity.text }}", .revision = 4 });
    try namespaces.put(a, "event/{{ _entity.text }}", .{ .key_template = "event/{{ _entity.text }}", .revision = 5 });
    const progress = try observeConsumers(&server, a, &.{ .{ .table_name = "source", .key_template = namespaces.keys()[0], .target = .{ .name = "concepts", .config_generation = 0, .promote = false } }, .{ .table_name = "source", .key_template = namespaces.keys()[1], .target = .{ .name = "events", .config_generation = 0, .promote = false } } }, namespaces, null, .{});
    try std.testing.expectEqual(@as(usize, 1), server.source.reads);
    try std.testing.expectEqual(@as(usize, 1), server.samples);
    try std.testing.expect(progress[0].applied);
    try std.testing.expect(!progress[1].applied);
}

test "resolver curation HTTP discovery retains static edges beyond the requested key" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var namespaces: std.StringArrayHashMapUnmanaged(curation.Namespace) = .empty;
    const template = "entity/{{_key}}";
    var discovery: EntityDiscovery = .{ .a = a, .owner = std.testing.allocator, .namespaces = &namespaces, .stored = .{ .namespaces = &.{.{ .key_template = template, .revision = 1 }} }, .adoption_revision = 1, .standalone = true, .key = "entity/a", .observations = true, .aliases = true, .authority_table_id = 7 };
    defer discovery.deinit();
    try discovery.consume(a, "source", .{ .name = "concepts", .table = "entities", .key_template = template, .source_artifact = "kg", .resolution_artifact = "resolved", .aliases = &.{ .{ .key = "entity/b", .target = "entity/c" }, .{ .key = "entity/d", .target = "entity/e" } } });
    try std.testing.expectEqual(@as(usize, 1), discovery.profiles.items.len);
    try std.testing.expectEqual(@as(usize, 2), discovery.profiles.items[0].aliases.len);
    try std.testing.expectEqual(@as(usize, 0), discovery.local_targets.count());
    try std.testing.expectEqual(@as(u64, 7), discovery.consumers.items[0].target.authority_table_id);
}

test "resolver curation HTTP plans static absorbed releases without unrelated alias conflicts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const template = "entity/{{_key}}";
    const namespace: curation.Namespace = .{ .key_template = template, .revision = 1, .merges = &.{.{ .key = "entity/q", .target = "entity/r" }} };
    const profiles = [_]native_authority.AliasProfile{
        .{ .key_template = template, .aliases = &.{ .{ .key = "entity/b", .target = "entity/c" }, .{ .key = "entity/r", .target = "entity/x" }, .{ .key = "entity/x", .target = "entity/y" } } },
        .{ .key_template = template, .aliases = &.{.{ .key = "entity/x", .target = "entity/z" }} },
    };
    const effective = try entityEffectiveAliases(a, namespace, &profiles, "entity/c", null);
    try std.testing.expectEqual(@as(usize, 1), effective.len);
    const plan = try resolver.curation.plan(a, "", namespace.merges, effective, .{ .action = .suppress, .key = "entity/c", .absorbed = .release });
    try std.testing.expectEqual(@as(usize, 1), plan.absorbed.len);
    try std.testing.expectEqualStrings("entity/b", plan.absorbed[0].key);
    try std.testing.expectEqualStrings("entity/b", plan.state.removed_aliases[0]);
    try std.testing.expectError(error.ResolverNamespaceMigrationConflict, entityEffectiveAliases(a, namespace, &profiles, "entity/x", null));
}

test "resolver curation progress observes source tables with bounded parallel ownership" {
    const Snapshot = @import("../metadata/api.zig").AdminSnapshot;
    const Source = struct {
        vtable: struct { supports_resolver_catalog_read: bool = false } = .{},
        reads: std.atomic.Value(usize) = .init(0),
        snapshot: Snapshot,
        pub fn systemCatalog(_: *@This(), _: std.mem.Allocator, _: operation.RequestContext, _: anytype) ![]u8 {
            return error.UnsupportedOperation;
        }
        pub fn linearizableSnapshot(self: *@This(), _: operation.RequestContext) !?Snapshot {
            _ = self.reads.fetchAdd(1, .monotonic);
            return self.snapshot;
        }
        pub fn freeAdminSnapshot(_: *@This(), _: *Snapshot) void {}
    };
    const Server = struct {
        alloc: std.mem.Allocator = std.heap.smp_allocator,
        source: Source,
        active: std.atomic.Value(usize) = .init(0),
        maximum: std.atomic.Value(usize) = .init(0),
        pub fn localTableRuntimeStatusesWithSnapshot(self: *@This(), name: []const u8, _: anytype) !?runtime_status.LocalTableRuntimeStatuses {
            const active = self.active.fetchAdd(1, .monotonic) + 1;
            defer _ = self.active.fetchSub(1, .monotonic);
            var prior = self.maximum.load(.monotonic);
            while (prior < active) prior = self.maximum.cmpxchgWeak(prior, active, .monotonic, .monotonic) orelse break;
            try std.testing.io.sleep(.fromMilliseconds(5), .awake);
            const index = try std.fmt.parseInt(u64, name[7..], 10);
            var row: runtime_status.LocalTableRuntimeStatus = .{ .group_id = index + 100, .metadata = .{ .source = .remote_store, .freshness = .fresh }, .stats = .{} };
            row.stats.resolution = .{ .enabled = true };
            row.stats.resolver_replay.resolvers = &.{.{ .name = "concepts", .curation_revision = 4 }};
            var rows = [_]runtime_status.LocalTableRuntimeStatus{row};
            return try (runtime_status.LocalTableRuntimeStatuses{ .items = &rows }).clone(self.alloc);
        }
        pub fn executeResolverOperation(_: *@This(), _: std.mem.Allocator, _: []const u8, _: u64, _: []const u8, _: operation.RequestContext) ![]u8 {
            return error.UnsupportedOperation;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const tables = try a.alloc(@import("../metadata/table_manager.zig").TableRecord, 32);
    const ranges = try a.alloc(@import("../metadata/table_manager.zig").RangeRecord, 32);
    const consumers = try a.alloc(EntityConsumer, 32);
    for (tables, ranges, consumers, 0..) |*table, *range, *consumer, i| {
        const name = try std.fmt.allocPrint(a, "source-{d}", .{i});
        table.* = .{ .name = name, .table_id = i + 1, .indexes_json = "{}" };
        range.* = .{ .table_id = i + 1, .group_id = i + 100, .start_key = "" };
        consumer.* = .{ .table_name = name, .key_template = "concept/{{_key}}", .target = .{ .name = "concepts", .config_generation = 0, .promote = false } };
    }
    var server: Server = .{ .source = .{ .snapshot = .{ .status = .{ .metadata_group_id = 0, .metrics = .{} }, .tables = tables, .ranges = ranges, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} } } };
    var namespaces: std.StringArrayHashMapUnmanaged(curation.Namespace) = .empty;
    try namespaces.put(a, "concept/{{_key}}", .{ .key_template = "concept/{{_key}}", .revision = 4 });
    const io = std.testing.io;
    const progress = try observeConsumers(&server, a, consumers, namespaces, null, .{ .fanout_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&io) });
    for (progress) |value| try std.testing.expect(value.applied);
    try std.testing.expectEqual(@as(usize, 32), server.source.reads.load(.monotonic));
    try std.testing.expect(server.maximum.load(.monotonic) > 1 and server.maximum.load(.monotonic) <= 16);
    try std.testing.expectEqual(@as(usize, 0), server.active.load(.monotonic));
}

test "resolver curation HTTP streaming adoption owns distinct policy pages" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const template = "concept/{{_key}}";
    var namespaces: std.StringArrayHashMapUnmanaged(curation.Namespace) = .empty;
    var discovery: EntityDiscovery = .{ .a = a, .owner = std.testing.allocator, .namespaces = &namespaces, .stored = .{}, .adoption_revision = 12, .standalone = true, .key = "concept/a-0", .observations = false, .aliases = false };
    defer discovery.deinit();
    for (0..5000) |i| {
        var page = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer page.deinit();
        const scratch = page.allocator();
        const noise = try std.fmt.allocPrint(scratch, "concept/noise-{d}", .{i});
        const state: resolver.curation.State = .{ .suppress = .{ .keys = &.{noise}, .entries = &.{.{ .key = noise, .origin = .{ .actor = try std.fmt.allocPrint(scratch, "owner-{d}", .{i}) } }} } };
        try discovery.consume(scratch, "source", .{ .name = "r", .table = "entities", .source_artifact = "kg", .resolution_artifact = "out", .key_template = template, .curation_revision = i + 1, .merges = &.{.{ .key = try std.fmt.allocPrint(scratch, "concept/a-{d}", .{i}), .target = try std.fmt.allocPrint(scratch, "concept/b-{d}", .{i}) }}, .curation_json = try std.json.Stringify.valueAlloc(scratch, state, .{}) });
    }
    try discovery.finish();
    const namespace = namespaces.get(template).?;
    try std.testing.expectEqual(@as(usize, 5000), namespace.merges.len);
    try std.testing.expectEqual(@as(usize, 5000), namespace.state.suppress.entries.len);
    try std.testing.expectEqualStrings("owner-0", namespace.state.suppress.entries[0].origin.actor);
    try std.testing.expectEqual(@as(u64, 5000), namespace.revision);
    try std.testing.expectEqual(@as(?u64, 12), namespace.adoption_revision);
}
