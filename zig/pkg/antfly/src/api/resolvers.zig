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
        const op: Operation = if (std.mem.eql(u8, action, "merge")) .entity_merge else if (std.mem.eql(u8, action, "unmerge")) .entity_unmerge else if (std.mem.eql(u8, action, "suppress")) .entity_suppress else if (std.mem.eql(u8, action, "unsuppress")) .entity_unsuppress else return null;
        return .{ .key = rest[0..colon], .op = op };
    }
    return null;
}
pub fn readOnly(op: Operation) bool {
    return op == .render or op == .resolve;
}
pub const GroupProgress = struct {
    group_id: u64,
    available: bool = false,
    fresh: bool = false,
    curation_revision: u64 = 0,
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
            if (comptime std.mem.eql(u8, name, "empty")) continue;
            try jw.objectField(name);
            try jw.write(@field(self, name));
        }
        try jw.endObject();
    }
};
pub const Progress = struct { applied: bool, shards: []const GroupProgress };
const Sample = struct {
    groups: std.AutoArrayHashMapUnmanaged(u64, void) = .empty,
    observations: std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(u64, GroupProgress)) = .empty,

    fn progress(self: Sample, a: std.mem.Allocator, cfg: Config, revision: u64) !Progress {
        const rows = try a.alloc(GroupProgress, self.groups.count());
        var applied = rows.len > 0;
        for (self.groups.keys(), rows) |id, *row| {
            row.* = if (self.observations.get(cfg.name)) |by_group| by_group.get(id) orelse .{ .group_id = id } else .{ .group_id = id };
            applied = applied and row.available and row.fresh and row.curation_revision >= revision and
                row.config_generation >= cfg.config_generation and !row.backfill_pending and
                curation.stageApplied(row.resolution, row.empty) and
                (!cfg.promote or curation.stageApplied(row.promotion, row.empty));
        }
        return .{ .applied = applied, .shards = rows };
    }
};

fn sample(server: anytype, a: std.mem.Allocator, snapshot: anytype, table: anytype, request: operation.RequestContext, probe_owners: bool) !Sample {
    var result: Sample = .{};
    for (snapshot.ranges) |range| if (range.table_id == table.table_id) try result.groups.put(a, range.group_id, {});
    var statuses = try server.localTableRuntimeStatusesWithSnapshot(table.name, snapshot);
    defer if (statuses) |*value| value.deinit(server.alloc);
    if (statuses) |values| for (values.items) |status| {
        if (!result.groups.contains(status.group_id)) continue;
        for (status.stats.resolver_replay.resolvers) |cfg| {
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
    const desired = try configs(a, table.indexes_json);
    const Probe = struct {
        group: u64,
        bytes: ?[]u8 = null,
        failure: ?anyerror = null,
        fn run(self: *@This(), owner: @TypeOf(server), name: []const u8, context: operation.RequestContext) void {
            self.bytes = owner.executeResolverOperation(std.heap.smp_allocator, name, self.group, "{\"operation\":\"progress\"}", context) catch |err| {
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
        if (needs_probe) try probes.append(a, .{ .group = group });
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
    var out: std.ArrayListUnmanaged(Config) = .empty;
    try local.metadata_local_index_reconcile.collectDesiredResolvers(a, root, &out);
    for (out.items) |*cfg| for (metadata.identities) |identity| if (!identity.dropped and std.mem.eql(u8, cfg.name, identity.name)) {
        cfg.* = try curation.overlayMetadata(a, cfg.*, identity);
        break;
    };
    for (metadata.identities) |identity| {
        if (identity.dropped) continue;
        var found = false;
        for (out.items) |cfg| if (std.mem.eql(u8, cfg.name, identity.name)) {
            found = true;
            break;
        };
        if (!found) try out.append(a, try curation.overlayMetadata(a, .{ .name = identity.name, .table = identity.table, .key_template = identity.key_template, .source_artifact = "retired_source", .resolution_artifact = "retired_resolution", .promote = false, .retired = true }, identity));
    }
    return out.items;
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
                if (std.mem.eql(u8, entry.key_ptr.*, curation.metadata_key) or std.mem.eql(u8, entry.key_ptr.*, "resolvers")) continue;
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
        .redirects = try cfg.effectiveAliasesAlloc(a),
        .curation = state,
        .progress = try observation.progress(a, cfg, cfg.curation_revision),
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
    const entity_mutation = op == .entity_merge or op == .entity_unmerge or op == .entity_suppress or op == .entity_unsuppress;
    var attempts: u8 = 0;
    while (true) {
        return execute(server, alloc, op, table_name, resolver_name, key, body, query, request) catch |err| {
            // These errors certify that the catalog CAS did not commit. Take
            // a fresh adoption cut, preserving any client expected_revision.
            if (entity_mutation and attempts < 2 and (err == error.TableLifecycleConflict or err == error.TableGenerationChanged)) {
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
        error.Conflict, error.ResolverCurationConflict, error.ResolverNamespaceMigrationConflict, error.ResolverNamespaceCurationRequired, error.TableGenerationChanged, error.TableLifecycleConflict, error.TableTransitionActive, error.ObjectTableDefinitionConflict => 409,
        error.GroupLeaderUnavailable, error.HttpExecutorUnavailable, error.MetadataMutationOutcomeUnknown, error.StorageUnavailable, error.UnknownGroup, error.NotLeader, error.LeaderUnavailable, error.ProposalDropped, error.Unavailable => 503,
        error.ResolverIdentityActive => 409,
        error.UnsupportedOperation, error.MethodNotAllowed => 405,
        error.InvalidCharacter, error.InvalidPadding, error.InvalidLength, error.InvalidResolverDistinctPair, error.InvalidResolverCurationRequest, error.InvalidResolverSuppression, error.InvalidResolverCurationKey, error.ResolverCurationLimitExceeded, error.ResolverRedirectExists, error.SuppressedResolverRedirectTarget, error.ResolverDistinctPair, error.InvalidResolverAlias, error.InvalidResolverAliasCycle, error.ResolverMergeNamespaceChange, error.InvalidArgument, error.InvalidResolverCurationVersion, error.SyntaxError, error.UnexpectedToken, error.UnknownField, error.MissingField, error.InvalidNumber, error.Overflow, error.InvalidEnumTag => 400,
        else => 500,
    };
}

fn execute(server: anytype, alloc: std.mem.Allocator, op: Operation, table_name: []const u8, resolver_name: []const u8, key: []const u8, body: []const u8, query: []const u8, request: operation.RequestContext) !@import("public_table_http.zig").OwnedResponse {
    try request.ensureActive();
    if (body.len > 4 * 1024 * 1024) return error.ResolverCurationLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const entity_mutation = op == .entity_merge or op == .entity_unmerge or op == .entity_suppress or op == .entity_unsuppress;
    const adoption_revision = if (entity_mutation and server.source.vtable.linearizable_routing_snapshot != null) blk: {
        var cut = try server.source.linearizableRoutingSnapshot(request.deadline_ns);
        defer server.source.freeRoutingSnapshot(&cut);
        break :blk @as(?u64, cut.catalog_revision);
    } else null;
    var snapshot = (try server.source.linearizableSnapshot(request)) orelse return error.TableNotFound;
    defer server.source.freeAdminSnapshot(&snapshot);
    const table = try tableByName(&snapshot, table_name);
    if (op == .entity_redirects or op == .entity_merge or op == .entity_unmerge or op == .entity_suppress or op == .entity_unsuppress)
        return entityOperation(server, alloc, a, &snapshot, table, op, key, body, request, adoption_revision);
    const all = try configs(a, table.indexes_json);
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
        .get => return response(alloc, 200, try summary(a, table, cfg, try sample(server, a, &snapshot, table, request, true))),
        .redirects => {
            const state = try resolver.curation.parseState(a, cfg.curation_json);
            return response(alloc, 200, .{ .redirects = try cfg.effectiveAliasesAlloc(a), .distinct = state.distinct, .revision = cfg.curation_revision });
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
            const stamp = try server.source.replaceTableDefinitionStamped(table.*, replacement);
            const next = try findConfig(try configs(a, sealed), resolver_name);
            const progress = try observeUntil(server, a, table.name, next, plan.revision, intent.wait, request);
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
            for (snapshot.tables) |destination| {
                if (!std.mem.eql(u8, destination.name, destination_name) or destination.indexes_json.len == 0) continue;
                const namespaces = try curation.namespacesFromValue(a, try std.json.parseFromSliceLeaky(std.json.Value, a, destination.indexes_json, .{}));
                for (namespaces.namespaces) |namespace| if (std.mem.eql(u8, namespace.key_template, cfg.key_template)) return error.ResolverNamespaceCurationRequired;
            }
            const intent = try mutationRequest(a, op, key, body, query);
            const plan = try curation.planMetadata(a, table.indexes_json, resolver_name, intent);
            const stamp = if (plan.changed) committed: {
                var replacement = table.*;
                replacement.indexes_json = plan.indexes_json;
                try request.ensureActive();
                break :committed try server.source.replaceTableDefinitionStamped(table.*, replacement);
            } else null;
            const progress = try observeUntil(server, a, table.name, cfg, plan.revision, intent.wait, request);
            return response(alloc, if (progress.applied) 200 else 202, .{ .resolver = resolver_name, .changed = plan.changed, .revision = plan.revision, .replay_sequence = plan.revision, .replay_sequence_kind = "resolver_revision", .metadata_commit = stamp, .applied = progress.applied, .progress = progress });
        },
        else => return error.UnsupportedOperation,
    }
}

/// One destination-table CAS owns every matching exact-template namespace.
/// Sources are observations of asynchronous replay, never curation authorities.
fn entityOperation(server: anytype, alloc: std.mem.Allocator, a: std.mem.Allocator, snapshot: anytype, entity_table: anytype, op: Operation, key: []const u8, body: []const u8, request: operation.RequestContext, adoption_revision: ?u64) !@import("public_table_http.zig").OwnedResponse {
    if (op == .entity_redirects) {
        try server.authorizeTableReads(a, request, &.{entity_table.name});
    } else try server.authorizeIntegrityMutations(request, &.{.{ .table_name = entity_table.name, .deletes = &.{key} }});
    const Input = struct { into: ?[]const u8 = null, replace: bool = false, pin_distinct: bool = false, expected_revision: ?u64 = null, wait: ?resolver.curation.Wait = null };
    const input = try std.json.parseFromSliceLeaky(Input, a, if (body.len == 0) "{}" else body, .{});
    if (key.len == 0 or (op == .entity_merge and input.into == null)) return error.InvalidResolverCurationRequest;
    if (input.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
    var root = try std.json.parseFromSliceLeaky(std.json.Value, a, if (entity_table.indexes_json.len == 0) "{}" else entity_table.indexes_json, .{});
    const stored = try curation.namespacesFromValue(a, root);
    var namespaces: std.StringArrayHashMapUnmanaged(curation.Namespace) = .empty;
    for (stored.namespaces) |namespace| if (curation.keyInNamespace(namespace.key_template, key)) try namespaces.put(a, namespace.key_template, namespace);
    const Table = @TypeOf(snapshot.tables[0]);
    const Consumer = struct { table: Table, cfg: Config };
    var consumers: std.ArrayListUnmanaged(Consumer) = .empty;
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
    for (snapshot.tables, source_configs) |source, all| for (all) |cfg| {
        if (!std.mem.eql(u8, destinations.get(cfg.table).?, entity_table.name) or !curation.keyInNamespace(cfg.key_template, key)) continue;
        try consumers.append(a, .{ .table = source, .cfg = cfg });
        if (!namespaces.contains(cfg.key_template)) try namespaces.put(a, cfg.key_template, .{ .key_template = cfg.key_template, .adoption_revision = adoption_revision, .adoption_standalone = server.cfg.deployment_mode == .standalone });
        // Fold legacy state only once. After adoption, source records are
        // derived projections and must never resurrect an undone correction.
        const authoritative = for (stored.namespaces) |item| {
            if (std.mem.eql(u8, item.key_template, cfg.key_template)) break true;
        } else false;
        if (!authoritative) {
            const current = namespaces.getPtr(cfg.key_template).?;
            current.* = try curation.foldNamespace(a, current.*, cfg);
        }
    };
    if (namespaces.count() == 0) return error.ResolverNotFound;
    if (op == .entity_redirects) {
        var states = std.json.Array.init(a);
        for (namespaces.values()) |namespace| {
            const state = try resolver.curation.entityState(a, namespace.state, namespace.merges, key);
            try states.append(try json(a, .{ .source_table = entity_table.name, .resolver = "", .key_template = namespace.key_template, .revision = namespace.revision, .redirects = state.redirects, .canonical_key = state.canonical_key, .curation = state.curation }));
        }
        return response(alloc, 200, .{ .entity_table = entity_table.name, .key = key, .complete = true, .atomicity = "namespace", .results = std.json.Value{ .array = states } });
    }
    const action: resolver.curation.Action = switch (op) {
        .entity_merge => .merge,
        .entity_unmerge => .unmerge,
        .entity_suppress => .suppress,
        .entity_unsuppress => .unsuppress,
        else => unreachable,
    };
    const intent: resolver.curation.Request = .{ .action = action, .key = key, .to = input.into, .replace = input.replace, .pin_distinct = input.pin_distinct, .expected_revision = input.expected_revision, .wait = input.wait };
    var changed = false;
    for (namespaces.values()) |*namespace| {
        if (input.into) |target| if (!curation.keyInNamespace(namespace.key_template, target)) return error.InvalidResolverCurationRequest;
        if (input.expected_revision) |expected| if (expected != namespace.revision) return error.ResolverCurationConflict;
        var effective: std.ArrayListUnmanaged(resolver.KeyAlias) = .empty;
        try effective.appendSlice(a, namespace.merges);
        const redirected = for (namespace.merges) |alias| {
            if (std.mem.eql(u8, alias.key, key)) break true;
        } else false;
        if (!redirected and (action == .merge or action == .unmerge)) {
            var local_target: ?[]const u8 = null;
            for (consumers.items) |consumer| {
                if (!std.mem.eql(u8, consumer.cfg.key_template, namespace.key_template)) continue;
                for (try consumer.cfg.effectiveAliasesAlloc(a)) |alias| {
                    if (!std.mem.eql(u8, alias.key, key)) continue;
                    if (local_target) |target| {
                        if (!std.mem.eql(u8, target, alias.target)) return error.ResolverNamespaceMigrationConflict;
                    } else local_target = alias.target;
                }
            }
            if (local_target) |target| try effective.append(a, .{ .key = key, .target = target });
        }
        const plan = try resolver.curation.plan(a, try std.json.Stringify.valueAlloc(a, namespace.state, .{}), namespace.merges, effective.items, intent);
        const check: Config = .{ .name = "namespace", .table = entity_table.name, .source_artifact = "metadata", .resolution_artifact = "resolved", .key_template = namespace.key_template, .merges = plan.merges, .curation_json = try std.json.Stringify.valueAlloc(a, plan.state, .{}) };
        try check.validate();
        for (consumers.items) |consumer| {
            if (!std.mem.eql(u8, consumer.cfg.key_template, namespace.key_template)) continue;
            var projected = consumer.cfg;
            projected.merges = plan.merges;
            projected.curation_json = check.curation_json;
            try projected.validate();
        }
        if (plan.changed or namespace.revision == 0) {
            namespace.state = plan.state;
            namespace.merges = plan.merges;
            namespace.revision = try std.math.add(u64, namespace.revision, 1);
            try curation.replaceNamespace(a, &root, namespace.*);
            changed = true;
        }
    }
    var replacement = entity_table.*;
    replacement.indexes_json = try std.json.Stringify.valueAlloc(a, root, .{});
    try request.ensureActive();
    const stamp = if (changed) try server.source.replaceTableDefinitionStamped(entity_table.*, replacement) else null;
    var results = std.json.Array.init(a);
    var applied = true;
    const deadline = @import("antfly_platform").time.monotonicNs() +| @as(u64, if (input.wait) |wait| wait.timeout_ms else 0) * std.time.ns_per_ms;
    for (consumers.items) |consumer| {
        const namespace = namespaces.get(consumer.cfg.key_template).?;
        const remaining = (deadline -| @import("antfly_platform").time.monotonicNs()) / std.time.ns_per_ms;
        const progress = try observeUntil(server, a, consumer.table.name, consumer.cfg, namespace.revision, if (remaining > 0) .{ .timeout_ms = @intCast(remaining) } else null, request);
        applied = applied and progress.applied;
        try results.append(try json(a, .{ .source_table = consumer.table.name, .resolver = consumer.cfg.name, .changed = changed, .revision = namespace.revision, .replay_sequence = namespace.revision, .replay_sequence_kind = "resolver_revision", .accepted = true, .@"error" = "", .applied = progress.applied, .progress = progress }));
    }
    return response(alloc, if (applied) 200 else 202, .{ .entity_table = entity_table.name, .key = key, .atomicity = "namespace", .complete = true, .changed = changed, .metadata_commit = stamp, .applied = applied, .results = std.json.Value{ .array = results } });
}

/// Waiting observes detached workers and yields between samples; it never
/// executes extraction, model inference or promotion on the request worker.
fn observeUntil(server: anytype, a: std.mem.Allocator, table_name: []const u8, cfg: Config, revision: u64, wait: ?resolver.curation.Wait, request: operation.RequestContext) !Progress {
    return observeReplayUntil(server, a, table_name, cfg, revision, wait, request, null);
}
const ReplayFloor = struct { group: u64, sequence: u64 };
fn observeReplayUntil(server: anytype, a: std.mem.Allocator, table_name: []const u8, cfg: Config, revision: u64, wait: ?resolver.curation.Wait, request: operation.RequestContext, floor: ?ReplayFloor) !Progress {
    const clock = @import("antfly_platform").time;
    const deadline = clock.monotonicNs() +| @as(u64, if (wait) |w| w.timeout_ms else 0) * std.time.ns_per_ms;
    while (true) {
        try request.ensureActive();
        var iteration = std.heap.ArenaAllocator.init(std.heap.smp_allocator);
        defer iteration.deinit();
        const scratch = iteration.allocator();
        var observation_request = try request.platformDeadline();
        if (wait != null) observation_request.deadline_ns = @min(observation_request.deadline_ns orelse std.math.maxInt(u64), deadline);
        var snapshot = (server.source.linearizableSnapshot(observation_request) catch |err| {
            try request.ensureActive();
            if (err == error.DeadlineExceeded and clock.monotonicNs() >= deadline) return .{ .applied = false, .shards = &.{} };
            return err;
        }) orelse return error.TableNotFound;
        const table = tableByName(&snapshot, table_name) catch |err| {
            server.source.freeAdminSnapshot(&snapshot);
            return err;
        };
        const observation = sample(server, scratch, &snapshot, table, observation_request, wait != null) catch |err| {
            server.source.freeAdminSnapshot(&snapshot);
            try request.ensureActive();
            if (err == error.DeadlineExceeded and clock.monotonicNs() >= deadline) return .{ .applied = false, .shards = &.{} };
            return err;
        };
        server.source.freeAdminSnapshot(&snapshot);
        var progress = try observation.progress(scratch, cfg, revision);
        if (floor) |target| {
            var observed = false;
            for (progress.shards) |row| if (row.group_id == target.group) {
                observed = row.resolution.applied_sequence >= target.sequence and (!cfg.promote or row.promotion.applied_sequence >= target.sequence);
                break;
            };
            progress.applied = progress.applied and observed;
        }
        const borrow = request.fanout_io orelse request.deadline_io;
        if (progress.applied or clock.monotonicNs() >= deadline or borrow == null) {
            const rows = try a.dupe(GroupProgress, progress.shards);
            for (rows) |*row| {
                row.resolution.blocked_reason = try a.dupe(u8, row.resolution.blocked_reason);
                row.promotion.blocked_reason = try a.dupe(u8, row.promotion.blocked_reason);
            }
            return .{ .applied = progress.applied, .shards = rows };
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
        const Input = struct { decision: resolver.Decision, key: ?[]const u8 = null, table: ?[]const u8 = null, wait: ?resolver.curation.Wait = null };
        const input = try std.json.parseFromSliceLeaky(Input, a, body, .{});
        const bytes = try std.json.Stringify.valueAlloc(a, curation.ReviewRequest{ .operation = .review, .resolver = cfg.name, .expected_revision = cfg.curation_revision, .id = token.value, .decision = input.decision, .key = input.key, .table = input.table }, .{});
        if (input.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
        const reply = try server.executeResolverOperation(a, table.name, token.group, bytes, request);
        var value = try std.json.parseFromSliceLeaky(std.json.Value, a, reply, .{});
        const sequence = value.object.get("replay_sequence").?.integer;
        const progress = try observeReplayUntil(server, a, table.name, cfg, cfg.curation_revision, input.wait, request, .{ .group = token.group, .sequence = @intCast(sequence) });
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
        var destinations = (try server.source.linearizableSnapshot(request)) orelse return error.TableNotFound;
        defer server.source.freeAdminSnapshot(&destinations);
        const destination = try tableByName(&destinations, physical);
        embedding = try managed.ManagedEmbedder.initFromIndexesJsonWithOptions(a, destination.indexes_json, options);
    }
    var active: std.ArrayListUnmanaged(Config) = .empty;
    for (all) |item| if (!item.retired) try active.append(a, item);
    const result = try local.storage_db_resolution_runtime.resolveQueryExtractionAlloc(a, active.items, &cfg, extraction, candidates.candidateSource(), if (embedding) |*value| value.denseInterface() else null);
    try request.ensureActive();
    var latest = (try server.source.linearizableSnapshot(request)) orelse return error.TableNotFound;
    defer server.source.freeAdminSnapshot(&latest);
    const current = try tableByName(&latest, table.name);
    if (!std.mem.eql(u8, current.indexes_json, table.indexes_json)) return error.Busy;
    return response(alloc, 200, .{ .extraction = try std.json.parseFromSliceLeaky(std.json.Value, a, extraction, .{}), .resolution = try std.json.parseFromSliceLeaky(std.json.Value, a, result, .{}) });
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
    try std.testing.expect(!(try observation.progress(a, cfg, 2)).applied);
    observation.observations.getPtr("concepts").?.getPtr(1).?.backfill_pending = false;
    try std.testing.expect((try observation.progress(a, cfg, 2)).applied);
    try std.testing.expect(!(try observation.progress(a, cfg, 3)).applied);
    observation.observations.getPtr("concepts").?.getPtr(1).?.fresh = false;
    try std.testing.expect(!(try observation.progress(a, cfg, 2)).applied);
}

test "resolver curation progress obtains missing diagnostics from the current owner" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var observation: Sample = .{};
    try observation.groups.put(a, 7, {});
    const cfg: Config = .{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "{{_entity.text}}", .promote = false };
    try std.testing.expect(!(try observation.progress(a, cfg, 4)).applied);
    const row: curation.ProgressRow = .{ .name = "concepts", .config_generation = 0, .effective_generation = 4, .curation_revision = 4, .backfill_pending = false, .resolution = .{ .enabled = true, .target_sequence = 9, .applied_sequence = 9 }, .promotion = .{} };
    try mergeOwnerProgress(a, &observation, 7, .{ .resolvers = &.{row} });
    try std.testing.expect((try observation.progress(a, cfg, 4)).applied);
    try std.testing.expect(!(try observation.progress(a, cfg, 5)).applied);
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
    try std.testing.expect((try observation.progress(a, cfg, 4)).applied);
    row.resolution.target_sequence = 1;
    try mergeOwnerProgress(a, &observation, 1, .{ .resolvers = &.{row} });
    try std.testing.expect(!(try observation.progress(a, cfg, 4)).applied);
}
