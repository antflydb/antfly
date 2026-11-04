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

//! Expire guarded snapshots, then collect only proved native-owned unreachable
//! objects. Saved jobs survive lost commits and interrupted bounded sweeps.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.serverless_external_source_mod.lake_catalog;
const configured = @import("configured_object_store_support.zig");
const ingestion = @import("lake_ingestion.zig");
const pins = @import("lake_snapshot_pins.zig");
const inputs = @import("lake_vacuum_inputs.zig");
const iceberg = local.serverless_external_source_mod.iceberg_avro;
const A = std.mem.Allocator;
const V = std.json.Value;
/// `max_expired` bounds one job. Older eligible snapshots expire first and the
/// rest are reported as `remaining_snapshots`, so a large backlog drains over
/// several bounded jobs instead of exceeding one job's read budget forever.
/// Planning and execution checkpoint independently under the same operation ID;
/// callers repeat incomplete results. max_plan_steps bounds immutable input reads.
pub const Options = struct { operation_id: []const u8, dry_run: bool = true, exclusive_ownership: bool = false, retain_ms: u64 = 7 * 24 * 60 * 60 * 1000, keep_latest: usize = 2, max_deleted: usize = 4096, max_expired: usize = default_max_expired, max_plan_steps: usize = 16 };
pub const default_max_expired: usize = 256;
pub const Result = struct { planning: bool = false, expired_snapshots: usize = 0, remaining_snapshots: usize = 0, eligible_objects: usize = 0, deleted_objects: usize = 0, retained_objects: usize = 0, complete: bool = false, conflicted: bool = false, delegated: bool = false, provider: ?catalog.maintenance.Provider = null, provider_state: ?catalog.maintenance.State = null };
const plan = @import("lake_vacuum_plan.zig");
const Retired = plan.Retired;
// Versions one and two keep their existing serialization and replay hashes.
// Version three records a no-op that must finish cleanup without a catalog commit.
const Job = struct { version: u8 = 1, id: []const u8, expected: []const u8, body: []const u8, timestamp_ms: i64, retired: []const Retired, nodes: []const plan.Inline = &.{} };
// Mutable planning progress is separate from the immutable catalog intent.
// Each turn reads a bounded number of immutable inputs. Spill nodes are durable
// before the conditional checkpoint publication; inline nodes need no PUT.
const max_planning_bytes = 64 * 1024 * 1024;
const Candidate = struct { id: i64, uri: []const u8 };
const ManifestRoot = struct { uri: []const u8, root: []const u8 };
const Planning = struct {
    version: u8 = 1,
    spilling: bool = false,
    uuid: []const u8,
    before: i64,
    keep_latest: usize,
    retain_ms: u64,
    max_expired: usize,
    candidates: []const Candidate,
    candidate: usize = 0,
    manifest: usize = 0,
    partial: ?[]const u8 = null,
    retired: []const Retired = &.{},
    nodes: []const plan.Inline = &.{},
    manifests: []const ManifestRoot = &.{},
};
fn planningTurn(a: A, client_value: @import("objectstore").Client, bucket: []const u8, key: []const u8, root: V, uuid: []const u8, protected: []const []const u8, request: Options, pin_store: pins.Store, destination: catalog.row_commit.Files, plans: *plan.Store, budget: *u64, cache: *ManifestFiles) !?Planning {
    const original_context = plans.context;
    const input_store = try inputs.Store.init(a, client_value, bucket, plans.prefix, request.operation_id, original_context);
    var lease = input_store.acquire(a) catch |err| switch (err) {
        error.LakeVacuumPlanClosed => return null,
        else => return err,
    };
    // On failure an object PUT may still have an uncertain remote outcome.
    // Retain admission until its deadline/grace rather than releasing early.
    var checkpointed = false;
    defer if (checkpointed) lease.release(a) catch {};
    plans.context = lease.context();
    defer plans.context = original_context;
    var leased_destination = destination;
    leased_destination.context = plans.context;
    var client = client_value;
    var saved = client.getObject(bucket, key, .{ .max_response_bytes = max_planning_bytes, .cancellation = catalog.types.contextCancellation(&plans.context) }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (saved) |*value| value.deinit(client.allocator);
    const now_ns = @import("antfly_platform").time.realtimeNs();
    var progress: Planning = undefined;
    if (saved) |value| {
        progress = try std.json.parseFromSliceLeaky(Planning, a, value.body, .{ .allocate = .alloc_always });
        if (progress.version != 1 or !std.mem.eql(u8, progress.uuid, uuid) or progress.retain_ms != request.retain_ms or progress.keep_latest != request.keep_latest or progress.max_expired != request.max_expired or progress.candidate > progress.candidates.len) return error.InvalidLakeVacuumPlan;
        try plans.seedPlanning(progress.nodes);
    } else {
        const before = @as(i64, @intCast(now_ns / std.time.ns_per_ms)) -| @as(i64, @intCast(request.retain_ms));
        const keep = try retainedIds(a, root, protected, request.keep_latest, before);
        var candidates: std.ArrayList(Candidate) = .empty;
        for ((try catalog.metadata.get(root, "snapshots")).array.items) |snapshot| {
            const id = try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id"));
            if (keep.contains(try std.fmt.allocPrint(a, "{d}", .{id}))) continue;
            try candidates.append(a, .{ .id = id, .uri = try catalog.metadata.str(try catalog.metadata.get(snapshot, "manifest-list")) });
        }
        progress = .{ .uuid = uuid, .before = before, .keep_latest = request.keep_latest, .retain_ms = request.retain_ms, .max_expired = request.max_expired, .candidates = candidates.items };
    }
    var retired: std.ArrayList(Retired) = .empty;
    try retired.appendSlice(a, progress.retired);
    var roots: std.StringHashMapUnmanaged([]const u8) = .empty;
    var manifest_roots: std.ArrayList(ManifestRoot) = .empty;
    try manifest_roots.appendSlice(a, progress.manifests);
    var cache_bytes: usize = 0;
    for (manifest_roots.items) |entry| {
        try roots.put(a, entry.uri, entry.root);
        cache_bytes += entry.uri.len + 160;
    }
    var inventories: catalog.manifest_inventory.Store = .{ .files = leased_destination, .client = client, .bucket = bucket, .prefix = std.mem.trimEnd(u8, input_store.prefix, "/") };
    var steps: usize = 0;
    const initial_budget = budget.*;
    while (!progress.spilling and progress.candidate < progress.candidates.len and retired.items.len < progress.max_expired and steps < request.max_plan_steps and initial_budget - budget.* + inventories.bytes_read < 8 * 1024 * 1024) {
        try plans.context.ensureActive();
        if (plans.context.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s >= deadline) break;
        const candidate = progress.candidates[progress.candidate];
        const text = try std.fmt.allocPrint(a, "{d}", .{candidate.id});
        if (progress.partial == null) {
            steps += 1;
            if (has(protected, text) or (try pin_store.deadline(a, text)) +| pins.grace_ns >= now_ns) {
                progress.candidate += 1;
                continue;
            }
        }
        // Immutable inputs can disappear after another vacuum wins. Never use
        // an orphaned partial plan to remove a newly referenced snapshot.
        const present = for ((try catalog.metadata.get(root, "snapshots")).array.items) |snapshot| {
            if (try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id")) == candidate.id) break std.mem.eql(u8, candidate.uri, try catalog.metadata.str(try catalog.metadata.get(snapshot, "manifest-list")));
        } else false;
        if (!present) {
            progress.candidate += 1;
            progress.manifest = 0;
            progress.partial = null;
            continue;
        }
        var sources: catalog.manifest_inventory.BootstrapBudget = .{ .remaining_sources = 1 };
        const list = (try inventories.load(a, candidate.uri, true, &sources)) orelse return error.InvalidLakeVacuumPlan;
        const source_bytes = inventories.source_bytes_read;
        inventories.source_bytes_read = 0;
        steps += 1;
        if (progress.manifest > list.count) return error.InvalidLakeVacuumPlan;
        var entries: catalog.manifest_inventory.Store.Reader = .{ .store = &inventories, .a = a, .inventory_ = list };
        var children: std.ArrayList([]const u8) = .empty;
        try children.append(a, progress.partial orelse try plans.objects(try a.dupe([]const u8, &.{candidate.uri})));
        const first_manifest = progress.manifest;
        while (progress.manifest < list.count) {
            try plans.context.ensureActive();
            if (plans.context.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s >= deadline) break;
            // A large source list is cached durably in its own turn. On
            // resume, admit at least one manifest even if the cache header
            // itself exceeds the soft byte quantum; cursor progress is saved.
            if (initial_budget - budget.* + inventories.bytes_read >= 8 * 1024 * 1024 and
                (source_bytes != 0 or progress.manifest != first_manifest)) break;
            const manifest_uri = (try entries.next(progress.manifest)) orelse return error.InvalidLakeVacuumPlan;
            const ref = roots.get(manifest_uri) orelse built: {
                if (steps >= request.max_plan_steps) break;
                // Inventories need only URIs; the bounded GET enforces the
                // remaining budget without trusting a declared source length.
                const paths = try manifestFiles(a, leased_destination, cache, manifest_uri, 0, budget);
                steps += 1;
                const objects = try a.alloc([]const u8, paths.len + 1);
                objects[0] = manifest_uri;
                @memcpy(objects[1..], paths);
                const hash = try plans.objects(objects);
                try roots.put(a, manifest_uri, hash);
                try manifest_roots.append(a, .{ .uri = manifest_uri, .root = hash });
                cache_bytes += manifest_uri.len + 160;
                break :built hash;
            };
            try children.append(a, ref);
            progress.manifest += 1;
        }
        progress.partial = try plans.children(children.items);
        if (progress.manifest == list.count) {
            try retired.append(a, .{ .snapshot = text, .object_plan = progress.partial });
            progress.candidate += 1;
            progress.manifest = 0;
            progress.partial = null;
        }
    }
    budget.* -= inventories.bytes_read;
    progress.retired = retired.items;
    // Bound the cross-turn manifest cache independently of snapshot history.
    var start: usize = 0;
    while (cache_bytes > 2 * 1024 * 1024 and start < manifest_roots.items.len) : (start += 1) cache_bytes -= manifest_roots.items[start].uri.len + 160;
    progress.manifests = manifest_roots.items[start..];
    var dependencies: std.ArrayList([]const u8) = .empty;
    for (progress.retired) |entry| try dependencies.append(a, entry.object_plan.?);
    if (progress.partial) |hash| try dependencies.append(a, hash);
    for (progress.manifests) |entry| try dependencies.append(a, entry.root);
    progress.nodes = try plans.inlineFor(dependencies.items);
    plans.uploads_remaining = 16;
    progress.spilling = false;
    plans.persist(dependencies.items, progress.nodes) catch |err| switch (err) {
        error.LakeVacuumPlanYield => progress.spilling = true,
        else => return err,
    };
    if (progress.spilling) progress.nodes = try plans.pendingNodes() else progress.nodes = try plans.inlineFor(dependencies.items);
    const body = try std.json.Stringify.valueAlloc(a, progress, .{});
    if (body.len > max_planning_bytes) return error.LakeVacuumBudgetExceeded;
    var stored = if (saved) |value|
        try client.compareAndPut(bucket, key, body, value.metadata.writeRevision() orelse return error.MissingObjectRevision, .{ .cancellation = catalog.types.contextCancellation(&plans.context) })
    else
        try client.putObject(bucket, key, body, .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&plans.context) });
    stored.deinit(client.allocator);
    checkpointed = true;
    return if (!progress.spilling and (progress.candidate == progress.candidates.len or progress.retired.len == progress.max_expired)) progress else null;
}
const Execution = struct {
    version: u8 = 3,
    job: []const u8,
    pinned: usize = 0,
    // Legacy numeric positions are retained only to decode version-one jobs.
    scan: usize = 0,
    cursor: plan.Cursor = .{},
    // A traversal window can outlive its smaller catalog retirement batch.
    // These unfenced candidates must be checked against current authority on
    // every resume; the cursor's durable closure owns their inventory.
    pending: []const []const u8 = &.{},
    approved: []const []const u8 = &.{},
    approved_index: usize = 0,
    next_scan: usize = 0,
    exhausted: bool = false,
    complete: bool = false,
};
fn executeJob(a: A, binding: local.serverless_external_source_catalog_binding.Binding, options: configured.BindingObjectStoreOpenOptions, request: Options, protected: []const []const u8, pin_store: pins.Store, destination: catalog.row_commit.Files, plans: *plan.Store, job: Job) !Result {
    var client = plans.client;
    const context = plans.context;
    var visits: ?catalog.retirement_index.Index = null;
    defer if (visits) |*index| index.deinit();
    const key = try std.fmt.allocPrint(a, "{s}/maintenance/vacuum-execution/{s}.json", .{ plans.prefix, catalog.types.digestHex(request.operation_id) });
    const job_hash = try a.dupe(u8, &catalog.types.digestHex(try std.json.Stringify.valueAlloc(a, job, .{})));
    var saved = client.getObject(plans.bucket, key, .{ .max_response_bytes = catalog.types.max_commit_bytes, .cancellation = catalog.types.contextCancellation(&context) }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (saved) |*value| value.deinit(client.allocator);
    var progress: Execution = if (saved) |value| try std.json.parseFromSliceLeaky(Execution, a, value.body, .{ .allocate = .alloc_always }) else .{ .job = job_hash };
    if ((progress.version != 1 and progress.version != 2 and progress.version != 3) or !std.mem.eql(u8, progress.job, job_hash) or progress.pinned > job.retired.len or progress.approved_index > progress.approved.len or progress.pending.len > catalog.types.max_reference_steps) return error.InvalidLakeVacuumPlan;
    if (progress.version < 3 and progress.pending.len != 0) return error.InvalidLakeVacuumPlan;
    if (progress.complete and progress.pending.len != 0) return error.InvalidLakeVacuumPlan;
    if (progress.version == 1) {
        // Restart traversal once for legacy numeric cursors, then checkpoint
        // bounded frontier progress. Already deleted objects stay harmless;
        // preserve any pending batch's existing catalog retirement proof.
        progress.scan = 0;
        progress.next_scan = 0;
        progress.cursor = .{};
        if (!progress.complete) progress.exhausted = false;
    }
    progress.version = 3;
    const input_store = try inputs.Store.init(a, client, plans.bucket, plans.prefix, request.operation_id, context);
    var result: Result = .{ .expired_snapshots = job.retired.len };
    var latest = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer latest.deinit(a);
    const live = try catalog.metadata.parse(a, latest.table.metadata_json);
    const now_ms: i64 = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
    const keep = try retainedIds(a, live, protected, request.keep_latest, now_ms -| @as(i64, @intCast(request.retain_ms)));
    for ((try catalog.metadata.get(live, "snapshots")).array.items) |snapshot| {
        const id = try std.fmt.allocPrint(a, "{d}", .{try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id"))});
        if (!keep.contains(id)) result.remaining_snapshots += 1;
    }
    if (progress.complete) {
        result.complete = try input_store.finish(a);
        return result;
    }
    // A retired pin marker permanently closes reader admission. Checkpoints
    // therefore need only a contiguous prefix, not a per-pass reader inventory.
    for (job.retired) |entry| if (has(protected, entry.snapshot)) return result;
    const pin_end = @min(job.retired.len, progress.pinned + 16);
    while (progress.pinned < pin_end) {
        if (!try pin_store.retire(a, job.retired[progress.pinned].snapshot, @import("antfly_platform").time.realtimeNs())) break;
        progress.pinned += 1;
    }
    if (progress.pinned == job.retired.len) {
        if (progress.approved.len == 0) {
            selection: {
                var ready = try configured.executeLakeCatalogAlloc(a, binding, options, context, .{ .references = .{ .objects = &.{} } });
                defer ready.deinit(a);
                if (!ready.references.ready) break :selection;
                var selected: std.ArrayList([]const u8) = .empty;
                var candidates = progress.pending;
                var walker: ?plan.Walker = null;
                var exhausted = progress.exhausted;
                if (candidates.len == 0) {
                    var collected: std.ArrayList([]const u8) = .empty;
                    visits = catalog.retirement_index.Index.init(a, client, plans.bucket, try std.fmt.allocPrint(a, "{s}/maintenance/vacuum-traversal", .{plans.prefix}), context);
                    const seen = &visits.?;
                    if (progress.cursor.visited != null) seen.loadHints() catch {};
                    try context.ensureActive();
                    walker = try plan.Walker.init(plans, job.retired, seen, progress.cursor);
                    var steps = catalog.types.max_reference_steps - ready.references.steps;
                    if (steps == 0) break :selection;
                    exhausted = false;
                    // Retained objects consume traversal work, not deletion
                    // slots. Keep the existing 64-step bound and buffer any
                    // candidates beyond the smaller fenced retirement batch.
                    while (collected.items.len < catalog.types.max_reference_steps) {
                        const uri = (walker.?.next(&steps) catch |err| switch (err) {
                            error.LakeVacuumPlanYield => break,
                            else => return err,
                        }) orelse {
                            exhausted = true;
                            break;
                        };
                        if (!has(collected.items, uri)) try collected.append(a, uri);
                    }
                    candidates = collected.items;
                }
                var references = try configured.executeLakeCatalogAlloc(a, binding, options, context, .{ .references = .{ .objects = candidates, .max_steps = 0 } });
                defer references.deinit(a);
                if (!references.references.ready) break :selection;
                var consumed: usize = 0;
                for (candidates, references.references.referenced) |uri, referenced| {
                    if (selected.items.len == 16) break;
                    try context.ensureActive();
                    if (context.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s >= deadline) break;
                    consumed += 1;
                    if (referenced) {
                        result.retained_objects += 1;
                        continue;
                    }
                    if (try collectOwned(a, destination, uri, true) != .eligible) {
                        result.retained_objects += 1;
                        continue;
                    }
                    if (!has(selected.items, uri)) try selected.append(a, uri);
                }
                if (selected.items.len != 0) {
                    const version = references.references.table.version orelse return error.LakeCatalogConditionalWritesRequired;
                    const intent = try std.json.Stringify.valueAlloc(a, .{ .version = version, .objects = selected.items }, .{});
                    var proof = configured.executeLakeCatalogAlloc(a, binding, options, context, .{ .retire = .{ .id = try std.fmt.allocPrint(a, "vacuum-retire-{s}", .{catalog.types.digestHex(intent)}), .expected_metadata_location = references.references.table.metadata_location, .expected_version = version, .objects = selected.items } }) catch |err| switch (err) {
                        error.LakeReachabilityPending => break :selection,
                        else => return err,
                    };
                    defer proof.deinit(a);
                    if (proof.table.retirement_root == null) return error.InvalidLakeRetirementIndex;
                }
                progress.approved = selected.items;
                progress.approved_index = 0;
                if (walker) |*value| progress.cursor = try value.checkpoint();
                progress.pending = candidates[consumed..];
                progress.exhausted = exhausted;
            }
            // Save the fenced deletion batch before any physical deletion.
        } else {
            const end = @min(progress.approved.len, progress.approved_index + @min(request.max_deleted, 16));
            while (progress.approved_index < end) : (progress.approved_index += 1) {
                switch (try collectOwned(a, destination, progress.approved[progress.approved_index], false)) {
                    .deleted => {
                        result.deleted_objects += 1;
                        result.eligible_objects += 1;
                    },
                    .eligible => return error.InvalidLakeVacuumPlan,
                    .retained => result.retained_objects += 1,
                }
            }
        }
        if (progress.approved_index == progress.approved.len) {
            progress.approved = &.{};
            progress.approved_index = 0;
            progress.complete = progress.exhausted and progress.pending.len == 0;
        }
    }
    const bytes = try std.json.Stringify.valueAlloc(a, progress, .{});
    var stored = if (saved) |value|
        try client.compareAndPut(plans.bucket, key, bytes, value.metadata.writeRevision() orelse return error.MissingObjectRevision, .{ .cancellation = catalog.types.contextCancellation(&context) })
    else
        try client.putObject(plans.bucket, key, bytes, .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&context) });
    stored.deinit(client.allocator);
    // Only the saved cursor admits traversal. Reuse authenticated visited nodes
    // across cold turns, publishing this optional cache after the progress CAS.
    if (visits) |*index| {
        index.saveHints(&.{progress.cursor.visited}) catch {};
        try context.ensureActive();
    }
    result.complete = progress.complete and try input_store.finish(a);
    return result;
}
const Marker = struct { uri: []const u8, sha256: []const u8, revision: @import("objectstore").ObjectRevision, owner: []const u8 };
fn has(values: []const []const u8, value: []const u8) bool {
    for (values) |candidate| if (std.mem.eql(u8, candidate, value)) return true;
    return false;
}
/// Live data paths of each immutable manifest, read once per vacuum pass.
/// Snapshots carry earlier manifests forward, so without this every snapshot
/// re-reads its ancestors' manifests and one pass costs O(snapshots^2) reads.
const ManifestFiles = std.StringHashMapUnmanaged([]const []const u8);
fn manifestFiles(a: A, files: catalog.row_commit.Files, cache: *ManifestFiles, path: []const u8, length: u64, budget: *u64) ![]const []const u8 {
    if (cache.get(path)) |paths| return paths;
    if (length > budget.*) return error.LakeVacuumBudgetExceeded;
    const manifest_bytes = try catalog.row_commit.readLimited(a, files, path, @intCast(@min(budget.*, 16 * 1024 * 1024)));
    if (manifest_bytes.len > budget.*) return error.LakeVacuumBudgetExceeded;
    budget.* -= manifest_bytes.len;
    const manifest = try iceberg.parseDataManifestAlloc(a, manifest_bytes);
    var live: std.ArrayList([]const u8) = .empty;
    for (manifest.entries) |file| if (file.status != .deleted) try live.append(a, file.file_path);
    try cache.put(a, path, live.items);
    return live.items;
}
fn collectSnapshot(a: A, files: catalog.row_commit.Files, snapshot: V, budget: *u64, cache: *ManifestFiles) ![]const []const u8 {
    var paths: std.StringHashMapUnmanaged(void) = .empty;
    const list_uri = try catalog.metadata.str(try catalog.metadata.get(snapshot, "manifest-list"));
    const bytes = try catalog.row_commit.readLimited(a, files, list_uri, @intCast(@min(budget.*, 16 * 1024 * 1024)));
    if (bytes.len > budget.*) return error.LakeVacuumBudgetExceeded;
    budget.* -= bytes.len;
    try paths.put(a, list_uri, {});
    const list = try iceberg.parseManifestListAlloc(a, bytes);
    for (list.entries) |entry| {
        try files.context.ensureActive();
        try paths.put(a, entry.manifest_path, {});
        for (try manifestFiles(a, files, cache, entry.manifest_path, entry.manifest_length, budget)) |file_path| {
            if (paths.count() == 262144) return error.LakeVacuumBudgetExceeded;
            try paths.put(a, file_path, {});
        }
    }
    const result = try a.alloc([]const u8, paths.count());
    var iterator = paths.keyIterator();
    for (result) |*path| path.* = iterator.next().?.*;
    return result;
}
/// Copy immutable manifest inventories into the private journal before snapshot
/// removal. Shared ancestors share the same graph nodes; other jobs may safely
/// collect the original manifests while this job is interrupted.
fn planSnapshot(a: A, files: catalog.row_commit.Files, snapshot: V, budget: *u64, cache: *ManifestFiles, plans: *plan.Store, roots: *std.StringHashMapUnmanaged([]const u8)) ![]const u8 {
    const uri = try catalog.metadata.str(try catalog.metadata.get(snapshot, "manifest-list"));
    const bytes = try catalog.row_commit.readLimited(a, files, uri, @intCast(@min(budget.*, 16 * 1024 * 1024)));
    if (bytes.len > budget.*) return error.LakeVacuumBudgetExceeded;
    budget.* -= bytes.len;
    const list = try iceberg.parseManifestListAlloc(a, bytes);
    var children: std.ArrayList([]const u8) = .empty;
    try children.append(a, try plans.objects(try a.dupe([]const u8, &.{uri})));
    for (list.entries) |entry| {
        const root = roots.get(entry.manifest_path) orelse built: {
            const paths = try manifestFiles(a, files, cache, entry.manifest_path, entry.manifest_length, budget);
            const objects = try a.alloc([]const u8, paths.len + 1);
            objects[0] = entry.manifest_path;
            @memcpy(objects[1..], paths);
            const ref = try plans.objects(objects);
            try roots.put(a, entry.manifest_path, ref);
            break :built ref;
        };
        try children.append(a, root);
    }
    return plans.children(children.items);
}
const IdSet = std.StringHashMapUnmanaged(void);
/// Accepted overlays certify every commit between their publication base and
/// the current head. Keeping the base alone can leave that proof with a hole.
/// Unknown/broken ancestry retains conservatively; it never manufactures a
/// native-commit certificate or changes query validation.
/// All allocations, including the result, belong to the operation arena.
pub fn protectOverlayAncestry(a: A, root: V, bases: []const []const u8) ![]const []const u8 {
    if (bases.len == 0) return bases;
    const m = catalog.metadata;
    const snapshots_value = try m.get(root, "snapshots");
    if (snapshots_value != .array or snapshots_value.array.items.len > 100_000) return error.InvalidLakeMetadata;
    const snapshots = snapshots_value.array.items;
    const Entry = struct { text: []const u8, parent: ?i64 };
    var nodes: std.AutoHashMapUnmanaged(i64, Entry) = .empty;
    for (snapshots) |snapshot| {
        const id = try m.int(try m.get(snapshot, "snapshot-id"));
        const parent = snapshot.object.get("parent-snapshot-id") orelse .null;
        const entry = try nodes.getOrPut(a, id);
        if (entry.found_existing) return error.InvalidLakeMetadata;
        entry.value_ptr.* = .{ .text = try std.fmt.allocPrint(a, "{d}", .{id}), .parent = if (parent == .null) null else try m.int(parent) };
    }
    var protected: IdSet = .empty;
    var wanted: std.AutoHashMapUnmanaged(i64, void) = .empty;
    var unknown = false;
    for (bases) |base| {
        try protected.put(a, base, {});
        const id = std.fmt.parseInt(i64, base, 10) catch {
            unknown = true;
            continue;
        };
        try wanted.put(a, id, {});
    }
    var complete = false;
    const head = root.object.get("current-snapshot-id") orelse .null;
    if (!unknown and head != .null) {
        var current = try m.int(head);
        // A linear bound also terminates corrupt cycles without allocating an
        // additional per-publication graph or rescanning the snapshot array.
        for (0..snapshots.len + 1) |_| {
            const node = nodes.get(current) orelse {
                // A previously planned job must not retire a missing middle
                // snapshot while a live base still depends on that edge.
                try protected.put(a, try std.fmt.allocPrint(a, "{d}", .{current}), {});
                break;
            };
            try protected.put(a, node.text, {});
            _ = wanted.remove(current);
            if (wanted.count() == 0) {
                complete = true;
                break;
            }
            current = node.parent orelse break;
        }
    }
    if (!complete) {
        var all = nodes.valueIterator();
        while (all.next()) |node| try protected.put(a, node.text, {});
    }
    const result = try a.alloc([]const u8, protected.count());
    var keys = protected.keyIterator();
    for (result) |*id| id.* = (keys.next() orelse unreachable).*;
    std.mem.sort([]const u8, result, {}, struct {
        fn less(_: void, left: []const u8, right: []const u8) bool {
            return std.mem.lessThan(u8, left, right);
        }
    }.less);
    return result;
}
fn retainedIds(a: A, root: V, extra: []const []const u8, keep_latest: usize, before: i64) !IdSet {
    var retained: IdSet = .empty;
    for (extra) |id| try retained.put(a, id, {});
    const snapshots = (try catalog.metadata.get(root, "snapshots")).array.items;
    const refs = try catalog.metadata.get(root, "refs");
    var refs_iterator = refs.object.iterator();
    while (refs_iterator.next()) |ref| try retained.put(a, try std.fmt.allocPrint(a, "{d}", .{try catalog.metadata.int(try catalog.metadata.get(ref.value_ptr.*, "snapshot-id"))}), {});
    // Iceberg snapshots append in commit order. Every named ref is protected
    // independently; the newest retained window additionally protects history.
    for (snapshots, 0..) |snapshot, index| {
        if (snapshots.len - index <= keep_latest or try catalog.metadata.int(try catalog.metadata.get(snapshot, "timestamp-ms")) >= before) {
            try retained.put(a, try std.fmt.allocPrint(a, "{d}", .{try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id"))}), {});
        }
    }
    return retained;
}
pub fn run(backing: A, binding: local.serverless_external_source_catalog_binding.Binding, options: configured.BindingObjectStoreOpenOptions, parent_context: catalog.types.Context, request: Options, protected: []const []const u8) !Result {
    // Provider transports allocate internally as well as through per-call
    // result allocators. One shared wrapper protects all providers opened in
    // this request, including when the caller supplied an unsynchronized arena.
    // All clients/workers drain before this scope and its allocator can end.
    var allocations: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = backing };
    const a = allocations.allocator();
    var context = parent_context;
    context.parallel_object_io = true;
    if (request.max_plan_steps < 3 or request.max_plan_steps > 128 or request.operation_id.len == 0 or request.operation_id.len > 256 or request.retain_ms > std.math.maxInt(i64) or request.retain_ms < 10 * 60 * 1000 or request.keep_latest == 0 or request.keep_latest > 1024 or request.max_deleted == 0 or request.max_deleted > 4096 or request.max_expired == 0 or request.max_expired > 4096) return error.InvalidLakeMaintenanceLimits;
    // A local object-store retirement marker cannot fence commits or readers
    // admitted by an external catalog. An ownership assertion is insufficient:
    // late REST commits can resurrect a file after our final mark phase.
    if (binding.catalog != null and binding.catalog.?.type == .rest and binding.catalog.?.maintenance != null) {
        var current = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
        defer current.deinit(a);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const metadata = try catalog.metadata.parse(arena.allocator(), current.table.metadata_json);
        const uuid = try catalog.metadata.str(try catalog.metadata.get(metadata, "table-uuid"));
        const body = try configured.executeExternalMaintenanceAlloc(a, binding, options, context, uuid, current.table.metadata_location, protected, .{ .operation_id = request.operation_id, .dry_run = request.dry_run, .retain_ms = request.retain_ms, .keep_latest = request.keep_latest, .max_deleted = request.max_deleted });
        defer a.free(body);
        const receipt = try std.json.parseFromSliceLeaky(catalog.maintenance.Result, arena.allocator(), body, .{ .ignore_unknown_fields = true });
        return .{ .expired_snapshots = receipt.expired_snapshots, .eligible_objects = receipt.eligible_objects, .deleted_objects = receipt.deleted_objects, .retained_objects = receipt.retained_objects, .complete = receipt.state == .complete, .delegated = true, .provider = receipt.provider, .provider_state = receipt.state };
    }
    if (!request.dry_run and binding.catalog != null and binding.catalog.?.type == .rest) return error.LakeVacuumCatalogCoordinationRequired;
    if (!request.dry_run and !request.exclusive_ownership) return error.LakeVacuumOwnershipRequired;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var queue = try ingestion.openQueue(a, binding, options);
    defer queue.deinit();
    const base = try ingestion.prefix(scratch, queue.prefix, binding, options);
    const job_key = try std.fmt.allocPrint(scratch, "{s}/maintenance/vacuum/{s}.json", .{ base, catalog.types.digestHex(request.operation_id) });
    var queue_client = queue.client;
    var saved = queue_client.getObject(queue.bucket, job_key, .{ .cancellation = catalog.types.contextCancellation(&context), .max_response_bytes = catalog.types.max_commit_bytes }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (saved) |*value| value.deinit(queue_client.allocator);
    var current = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer current.deinit(a);
    const root = try catalog.metadata.parse(scratch, current.table.metadata_json);
    const uuid = try catalog.metadata.str(try catalog.metadata.get(root, "table-uuid"));
    const pin_store: pins.Store = .{ .client = queue.client, .bucket = queue.bucket, .prefix = try pins.namespace(scratch, queue.prefix, binding, uuid), .context = context };
    var file_options = options;
    file_options.read_only = request.dry_run;
    var files = try configured.openBindingObjectStoreAlloc(a, binding, file_options);
    defer files.deinit();
    const destination = try ingestion.destinationFiles(scratch, files, binding.source_uri, context);
    var budget: u64 = 256 * 1024 * 1024;
    var manifests: ManifestFiles = .empty;
    var plans: plan.Store = .{ .a = scratch, .client = queue.client, .bucket = queue.bucket, .prefix = base, .context = context, .dry_run = request.dry_run, .defer_writes = !request.dry_run };
    var planned_manifests: std.StringHashMapUnmanaged([]const u8) = .empty;
    var job: Job = undefined;
    var result: Result = .{};
    if (saved) |value| {
        if (request.dry_run) return error.LakeMaintenanceAlreadyStarted;
        job = try std.json.parseFromSliceLeaky(Job, scratch, value.body, .{ .allocate = .alloc_always });
        if (job.version != 1 and job.version != 2 and job.version != 3) return error.InvalidLakeVacuumPlan;
        if (job.version == 3 and (job.retired.len != 0 or job.nodes.len != 0)) return error.InvalidLakeVacuumPlan;
        for (job.retired) |retired| if ((job.version == 2) != (retired.object_plan != null)) return error.InvalidLakeVacuumPlan;
        try plans.seed(job.nodes);
    } else {
        var retired: std.ArrayList(Retired) = .empty;
        var ids: std.ArrayList(i64) = .empty;
        const now_ns = @import("antfly_platform").time.realtimeNs();
        const now_ms: i64 = @intCast(now_ns / std.time.ns_per_ms);
        var authority_root = root;
        if (!request.dry_run) {
            const planning_key = try std.fmt.allocPrint(scratch, "{s}/maintenance/vacuum-planning/{s}.json", .{ base, catalog.types.digestHex(request.operation_id) });
            const progress = (try planningTurn(scratch, queue.client, queue.bucket, planning_key, root, uuid, protected, request, pin_store, destination, &plans, &budget, &manifests)) orelse return .{ .planning = true };
            // Rebase only the unfinished planner, never an immutable intent.
            // Ingestion may have published many snapshots between turns.
            const refreshed = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
            current.deinit(a);
            current = refreshed;
            authority_root = try catalog.metadata.parse(scratch, current.table.metadata_json);
            if (!std.mem.eql(u8, uuid, try catalog.metadata.str(try catalog.metadata.get(authority_root, "table-uuid")))) return error.InvalidLakeVacuumPlan;
            const keep = try retainedIds(scratch, authority_root, protected, progress.keep_latest, progress.before);
            var candidate_uris: std.AutoHashMapUnmanaged(i64, []const u8) = .empty;
            for (progress.candidates) |candidate| try candidate_uris.put(scratch, candidate.id, candidate.uri);
            for (progress.retired) |entry| {
                if (keep.contains(entry.snapshot)) continue;
                const id = try std.fmt.parseInt(i64, entry.snapshot, 10);
                for ((try catalog.metadata.get(authority_root, "snapshots")).array.items) |snapshot| {
                    if (try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id")) != id) continue;
                    if (!std.mem.eql(u8, candidate_uris.get(id) orelse return error.InvalidLakeVacuumPlan, try catalog.metadata.str(try catalog.metadata.get(snapshot, "manifest-list")))) continue;
                    try retired.append(scratch, entry);
                    try ids.append(scratch, id);
                    break;
                }
            }
        } else {
            const before = now_ms -| (std.math.cast(i64, request.retain_ms) orelse return error.InvalidLakeMaintenanceLimits);
            const keep = try retainedIds(scratch, root, protected, request.keep_latest, before);
            for ((try catalog.metadata.get(root, "snapshots")).array.items) |snapshot| {
                const id = try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id"));
                const text = try std.fmt.allocPrint(scratch, "{d}", .{id});
                if (keep.contains(text)) continue;
                // Do not read pins or manifests beyond this job's bound. The
                // backlog is counted below from the post-commit authority.
                if (ids.items.len == request.max_expired) continue;
                if ((try pin_store.deadline(a, text)) +| pins.grace_ns >= now_ns) continue;
                try retired.append(scratch, .{ .snapshot = text, .object_plan = try planSnapshot(scratch, destination, snapshot, &budget, &manifests, &plans, &planned_manifests) });
                try ids.append(scratch, id);
            }
        }
        result.expired_snapshots = ids.items.len;
        if (ids.items.len == 0 and request.dry_run) {
            result.complete = true;
            return result;
        }
        const parent = try catalog.metadata.get(authority_root, "current-snapshot-id");
        const body = try std.json.Stringify.valueAlloc(scratch, .{ .requirements = .{.{ .type = "assert-ref-snapshot-id", .ref = "main", .@"snapshot-id" = parent }}, .updates = .{.{ .action = "remove-snapshots", .@"snapshot-ids" = ids.items }} }, .{});
        job = .{ .version = if (ids.items.len == 0) 3 else 2, .id = try std.fmt.allocPrint(scratch, "vacuum-{s}", .{catalog.types.digestHex(request.operation_id)}), .expected = current.table.metadata_location, .body = body, .timestamp_ms = now_ms, .retired = retired.items, .nodes = try plans.inlineNodes() };
        if (!request.dry_run) {
            const roots = try scratch.alloc([]const u8, job.retired.len);
            for (job.retired, roots) |entry, *hash| hash.* = entry.object_plan.?;
            job.nodes = try plans.inlineFor(roots);
            // Filtering to final roots can only expand their inline allowance;
            // external closures were already flushed by the bounded planner.
            try plans.persist(roots, job.nodes);
            const bytes = try std.json.Stringify.valueAlloc(scratch, job, .{});
            if (bytes.len > catalog.types.max_commit_bytes) return error.LakeVacuumBudgetExceeded;
            var stored = try queue_client.putObject(queue.bucket, job_key, bytes, .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&context) });
            stored.deinit(queue_client.allocator);
        }
    }
    if (!request.dry_run) {
        const input_store = try inputs.Store.init(scratch, queue.client, queue.bucket, base, request.operation_id, context);
        // The immutable job owns all replay dependencies before closing input
        // admission. Existing registered planners are drained before cleanup.
        try input_store.close(scratch);
        if (job.version == 3) return .{ .complete = try input_store.finish(scratch) };
    }
    // Snapshot removal precedes retirement. Concurrent refs/commits must win a
    // catalog CAS; a failed job never deletes files or blindly rebases its intent.
    if (!request.dry_run) {
        var committed = configured.executeLakeCatalogAlloc(a, binding, options, context, .{ .commit = .{ .id = job.id, .expected_metadata_location = job.expected, .body = job.body, .timestamp_ms = job.timestamp_ms } }) catch |err| switch (err) {
            // Definitive rejection terminates this immutable plan. The next
            // scheduled operation can plan from current authority; uncertain
            // outcomes still retry the exact saved intent.
            error.LakeCommitConflict => {
                const input_store = try inputs.Store.init(scratch, queue.client, queue.bucket, base, request.operation_id, context);
                return .{ .complete = try input_store.finish(scratch), .conflicted = true };
            },
            else => return err,
        };
        defer committed.deinit(a);
    }
    if (!request.dry_run) return executeJob(scratch, binding, options, request, protected, pin_store, destination, &plans, job);
    // Re-read authority after resolving the intent. A newer safe publication
    // can retain old native objects, and the mark set must include its roots.
    var latest = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer latest.deinit(a);
    const live_root = try catalog.metadata.parse(scratch, latest.table.metadata_json);
    // Backlog is derived from current authority on every turn, including saved
    // job replay. It must not disappear after a timeout, deletion limit or pin.
    const now_ms: i64 = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
    const before = now_ms -| (std.math.cast(i64, request.retain_ms) orelse return error.InvalidLakeMaintenanceLimits);
    const keep = try retainedIds(scratch, live_root, protected, request.keep_latest, before);
    var marked: std.StringHashMapUnmanaged(void) = .empty;
    inline for (.{ "statistics", "partition-statistics" }) |kind| {
        if (live_root.object.get(kind)) |entries| {
            if (entries != .array) return error.InvalidLakeMetadata;
            for (entries.array.items) |entry| try marked.put(scratch, try catalog.metadata.str(try catalog.metadata.get(entry, "statistics-path")), {});
        }
    }
    for ((try catalog.metadata.get(live_root, "snapshots")).array.items) |snapshot| {
        const id = try std.fmt.allocPrint(scratch, "{d}", .{try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id"))});
        const expired = for (job.retired) |old| {
            if (std.mem.eql(u8, old.snapshot, id)) break true;
        } else false;
        if (request.dry_run and expired) continue;
        if (!keep.contains(id)) result.remaining_snapshots += 1;
        for (try collectSnapshot(scratch, destination, snapshot, &budget, &manifests)) |uri| try marked.put(scratch, uri, {});
    }
    // Dry runs inspect ownership without pin retirement or journal writes.
    result.expired_snapshots = job.retired.len;
    result.complete = true;
    var objects: plan.Iterator = .{ .store = &plans, .retired = job.retired };
    while (try objects.next()) |uri| {
        if (marked.contains(uri)) {
            result.retained_objects += 1;
            continue;
        }
        switch (try collectOwned(scratch, destination, uri, true)) {
            .retained => result.retained_objects += 1,
            .eligible => result.eligible_objects += 1,
            .deleted => unreachable,
        }
    }
    return result;
}

test "external lake vacuum retains refs newest snapshots recent commits and reader roots" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(V, a,
        \\{"refs":{"main":{"snapshot-id":5},"history":{"snapshot-id":1}},"snapshots":[{"snapshot-id":1,"timestamp-ms":1},{"snapshot-id":2,"timestamp-ms":2},{"snapshot-id":3,"timestamp-ms":3},{"snapshot-id":4,"timestamp-ms":100},{"snapshot-id":5,"timestamp-ms":5}]}
    , .{});
    const retained = try retainedIds(a, root, &.{"3"}, 1, 90);
    try std.testing.expect(retained.contains("1"));
    try std.testing.expect(!retained.contains("2"));
    try std.testing.expect(retained.contains("3"));
    try std.testing.expect(retained.contains("4"));
    try std.testing.expect(retained.contains("5"));
}

test "external lake vacuum ancestry protection unions live bases without retaining older history" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(V, a,
        \\{"current-snapshot-id":4,"snapshots":[{"snapshot-id":1},{"snapshot-id":9},{"snapshot-id":2,"parent-snapshot-id":1},{"snapshot-id":3,"parent-snapshot-id":2},{"snapshot-id":4,"parent-snapshot-id":3}]}
    , .{});
    const protected = try protectOverlayAncestry(a, root, &.{ "3", "2", "3" });
    try std.testing.expectEqual(@as(usize, 3), protected.len);
    for (protected, [_][]const u8{ "2", "3", "4" }) |id, expected| try std.testing.expectEqualStrings(expected, id);
    const current = try protectOverlayAncestry(a, root, &.{"4"});
    try std.testing.expectEqual(@as(usize, 1), current.len);
    try std.testing.expectEqualStrings("4", current[0]);
    try std.testing.expectEqual(@as(usize, 0), (try protectOverlayAncestry(a, root, &.{})).len);
}

test "external lake vacuum ancestry protection fails closed for gaps cycles and unknown bases" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        \\{"current-snapshot-id":3,"snapshots":[{"snapshot-id":1},{"snapshot-id":9},{"snapshot-id":3,"parent-snapshot-id":2}]}
        ,
        \\{"current-snapshot-id":3,"snapshots":[{"snapshot-id":1},{"snapshot-id":9},{"snapshot-id":2,"parent-snapshot-id":3},{"snapshot-id":3,"parent-snapshot-id":2}]}
        ,
    }) |bytes| {
        const root = try std.json.parseFromSliceLeaky(V, a, bytes, .{});
        const protected = try protectOverlayAncestry(a, root, &.{"1"});
        // Include a missing middle ID as a guard against an already-planned
        // retirement, and terminate cycles without granting new read authority.
        for ([_][]const u8{ "1", "2", "3", "9" }) |id| try std.testing.expect(has(protected, id));
        const unknown = try protectOverlayAncestry(a, root, &.{"empty:initial"});
        try std.testing.expect(has(unknown, "1") and has(unknown, "3") and has(unknown, "9"));
    }
}

const Collection = enum { retained, eligible, deleted };
fn collectOwned(a: A, files: catalog.row_commit.Files, uri: []const u8, dry_run: bool) !Collection {
    var client = files.client;
    const table_prefix = try std.fmt.allocPrint(a, "{s}/", .{std.mem.trimEnd(u8, files.uri, "/")});
    if (!std.mem.startsWith(u8, uri, table_prefix) or std.mem.indexOf(u8, uri[table_prefix.len..], "..") != null) {
        return .retained;
    }
    const marker_key = try std.fmt.allocPrint(a, "{s}{s}.antfly-owned/{s}.json", .{ files.prefix, if (files.prefix.len == 0) "" else "/", catalog.types.digestHex(uri) });
    var marker = client.getObject(files.bucket, marker_key, .{ .skip_metadata_probe = true, .cancellation = catalog.types.contextCancellation(&files.context), .max_response_bytes = 4096 }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => {
            return .retained;
        },
        else => return err,
    };
    defer marker.deinit(client.allocator);
    const proof = try std.json.parseFromSliceLeaky(Marker, a, marker.body, .{});
    if (!std.mem.eql(u8, proof.owner, "antfly-native-lake-v2") or !std.mem.eql(u8, proof.uri, uri) or proof.sha256.len != 64) return error.LakeArtifactIdentityConflict;
    if (dry_run) return .eligible;
    const key = try std.fmt.allocPrint(a, "{s}{s}{s}", .{ files.prefix, if (files.prefix.len == 0) "" else "/", uri[table_prefix.len..] });
    client.compareAndDelete(files.bucket, key, proof.revision, .{ .cancellation = catalog.types.contextCancellation(&files.context) }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => {},
        else => return err,
    };
    // Removal of the ownership proof is conditional as well. A retry
    // after a lost delete response observes absence and stays safe.
    try client.compareAndDelete(files.bucket, marker_key, marker.metadata.writeRevision() orelse return error.MissingObjectRevision, .{ .cancellation = catalog.types.contextCancellation(&files.context) });
    return .deleted;
}

test "external lake vacuum deletes only owned unchanged objects and replays absence" {
    const objectstore = @import("objectstore");
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const files: catalog.row_commit.Files = .{ .client = memory.client(), .bucket = "archive", .prefix = "hn", .uri = "s3://archive/hn" };
    const uri = try catalog.row_commit.upload(a, files, "data/owned.parquet", "native-content");
    try std.testing.expectEqual(Collection.eligible, try collectOwned(a, files, uri, true));
    try std.testing.expectEqual(Collection.deleted, try collectOwned(a, files, uri, false));
    try std.testing.expectEqual(Collection.retained, try collectOwned(a, files, uri, false));
    var client = files.client;
    var external = try client.putObject(files.bucket, "hn/data/external.parquet", "external", .{});
    external.deinit(alloc);
    try std.testing.expectEqual(Collection.retained, try collectOwned(a, files, "s3://archive/hn/data/external.parquet", false));
    const replaced_uri = try catalog.row_commit.upload(a, files, "data/replaced.parquet", "old-content");
    var changed = try client.putObject(files.bucket, "hn/data/replaced.parquet", "new-content", .{});
    changed.deinit(alloc);
    try std.testing.expectError(error.PreconditionFailed, collectOwned(a, files, replaced_uri, false));
    var remaining = try client.getObject(files.bucket, "hn/data/replaced.parquet", .{});
    defer remaining.deinit(alloc);
    try std.testing.expectEqualStrings("new-content", remaining.body);
}

test "external lake REST vacuum cannot delete using an exclusive ownership assertion" {
    const binding: local.serverless_external_source_catalog_binding.Binding = .{ .table_id = "events", .format = .iceberg, .source_uri = "s3://bucket/events", .schema_fingerprint = "schema", .write_policy = .iceberg_writer, .catalog = .{ .type = .rest, .connection = "catalog", .uri = "https://catalog.example.com", .namespace = &.{"analytics"}, .name = "events" } };
    // No credentials or object I/O are needed: reject before any persisted
    // intent, catalog mutation, retirement marker, or file deletion.
    try std.testing.expectError(error.LakeVacuumCatalogCoordinationRequired, run(std.testing.allocator, binding, .{}, .{}, .{ .operation_id = "vacuum", .dry_run = false, .exclusive_ownership = true }, &.{}));
}

test "external lake vacuum deletes bucket root objects and ownership markers" {
    const objectstore = @import("objectstore");
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const files: catalog.row_commit.Files = .{ .client = memory.client(), .bucket = "archive", .prefix = "", .uri = "s3://archive" };
    const uri = try catalog.row_commit.upload(a, files, "data/owned.parquet", "native-content");
    try std.testing.expectEqual(Collection.eligible, try collectOwned(a, files, uri, true));
    var client = files.client;
    var before = try client.getObject(files.bucket, "data/owned.parquet", .{});
    before.deinit(alloc);
    try std.testing.expectEqual(Collection.deleted, try collectOwned(a, files, uri, false));
    try std.testing.expectError(error.FileNotFound, client.getObject(files.bucket, "data/owned.parquet", .{}));
    const marker_key = try std.fmt.allocPrint(a, ".antfly-owned/{s}.json", .{catalog.types.digestHex(uri)});
    try std.testing.expectError(error.FileNotFound, client.getObject(files.bucket, marker_key, .{}));
    try std.testing.expectEqual(Collection.retained, try collectOwned(a, files, uri, false));
}

test "external lake vacuum definitive commit conflict completes its saved operation" {
    const alloc = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-vacuum-conflict");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    var config = try local.common_config.Config.parseFromSlice(alloc, json);
    defer config.deinit();
    const binding: local.serverless_external_source_catalog_binding.Binding = .{ .table_id = "events", .format = .iceberg, .source_uri = try std.fmt.allocPrint(a, "file://{s}/warehouse", .{directory.path()}), .schema_fingerprint = "schema", .write_policy = .iceberg_writer, .catalog = .{ .type = .managed } };
    const options: configured.BindingObjectStoreOpenOptions = .{ .node_config = &config, .catalog_table_id = 7 };
    var initial = try configured.executeLakeCatalogAlloc(alloc, binding, options, .{}, .{ .create = .{ .id = "create", .expected_metadata_location = "", .body = "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", .timestamp_ms = 1 } });
    defer initial.deinit(alloc);
    var queue = try ingestion.openQueue(alloc, binding, options);
    defer queue.deinit();
    const base = try ingestion.prefix(a, queue.prefix, binding, options);
    const key = try std.fmt.allocPrint(a, "{s}/maintenance/vacuum/{s}.json", .{ base, catalog.types.digestHex("scheduled-0-vacuum") });
    const saved: Job = .{ .id = "saved-vacuum", .expected = initial.table.metadata_location, .body = "{\"requirements\":[],\"updates\":[]}", .timestamp_ms = 2, .retired = &.{} };
    var client = queue.client;
    var stored = try client.putObject(queue.bucket, key, try std.json.Stringify.valueAlloc(a, saved, .{}), .{});
    stored.deinit(client.allocator);
    var concurrent = try configured.executeLakeCatalogAlloc(alloc, binding, options, .{}, .{ .commit = .{ .id = "concurrent", .expected_metadata_location = initial.table.metadata_location, .body = "{\"requirements\":[],\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"owner\":\"other\"}}]}", .timestamp_ms = 3 } });
    defer concurrent.deinit(alloc);
    for (0..2) |_| {
        const result = try run(alloc, binding, options, .{}, .{ .operation_id = "scheduled-0-vacuum", .dry_run = false, .exclusive_ownership = true }, &.{});
        try std.testing.expect(result.complete and result.conflicted);
        try std.testing.expectEqual(@as(usize, 0), result.deleted_objects);
        try std.testing.expectEqual(@as(usize, 0), result.expired_snapshots);
    }
}

test "external lake vacuum expires a large backlog in bounded oldest-first jobs" {
    const alloc = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-vacuum-backlog");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    var config = try local.common_config.Config.parseFromSlice(alloc, json);
    defer config.deinit();
    const binding: local.serverless_external_source_catalog_binding.Binding = .{ .table_id = "events", .format = .iceberg, .source_uri = try std.fmt.allocPrint(a, "file://{s}/warehouse", .{directory.path()}), .schema_fingerprint = "schema", .write_policy = .iceberg_writer, .catalog = .{ .type = .managed } };
    const options: configured.BindingObjectStoreOpenOptions = .{ .node_config = &config, .catalog_table_id = 11 };
    var opened = try configured.openBindingObjectStoreAlloc(alloc, binding, options);
    defer opened.deinit();
    var managed: catalog.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = binding.source_uri };
    var table = try managed.create(a, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    const files = try ingestion.destinationFiles(a, opened, binding.source_uri, .{});
    // Six ancient commits: five are eligible once the newest is kept.
    for (1..7) |n| {
        const row = try std.json.parseFromSliceLeaky(V, a, try std.fmt.allocPrint(a, "{{\"id\":{d}}}", .{n}), .{});
        const batch: catalog.row_commit.Batch = .{ .batch_id = try std.fmt.allocPrint(a, "b{d}", .{n}), .source = "s", .epoch = "1", .checkpoint = try std.fmt.allocPrint(a, "{d}", .{n}), .expected_checkpoint = if (n == 1) null else try std.fmt.allocPrint(a, "{d}", .{n - 1}), .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = row }} };
        const prepared = try catalog.row_commit.prepare(a, table, files, batch, n, @intCast(n + 1));
        table = try managed.commit(a, .{ .id = batch.batch_id, .expected_metadata_location = table.metadata_location, .body = prepared.body, .timestamp_ms = @intCast(n + 1) });
    }
    const expected = [_][2]usize{ .{ 2, 3 }, .{ 2, 1 }, .{ 1, 0 } };
    for (expected, 0..) |counts, pass| {
        const request: Options = .{ .operation_id = try std.fmt.allocPrint(a, "vacuum-{d}", .{pass}), .dry_run = false, .exclusive_ownership = true, .retain_ms = 10 * 60 * 1000, .keep_latest = 1, .max_expired = 2, .max_deleted = 1 };
        var result = try run(alloc, binding, options, .{}, request, &.{});
        if (pass == 0) {
            try std.testing.expect(!result.complete);
            // Resume a pre-frontier execution journal with a fenced batch.
            // Migration must preserve that batch and restart only traversal.
            var queue = try ingestion.openQueue(alloc, binding, options);
            defer queue.deinit();
            const execution_key = try std.fmt.allocPrint(a, "{s}/maintenance/vacuum-execution/{s}.json", .{ try ingestion.prefix(a, queue.prefix, binding, options), catalog.types.digestHex(request.operation_id) });
            var previous = try queue.client.getObject(queue.bucket, execution_key, .{});
            defer previous.deinit(queue.client.allocator);
            var legacy = try std.json.parseFromSliceLeaky(Execution, a, previous.body, .{ .allocate = .alloc_always });
            // Reachability bootstrap may yield before selecting the first
            // fenced batch. Migrate only once that batch is durable.
            var bootstrap_turns: usize = 0;
            while (legacy.approved.len == 0 and bootstrap_turns < 64) : (bootstrap_turns += 1) {
                result = try run(alloc, binding, options, .{}, request, &.{});
                previous.deinit(queue.client.allocator);
                previous = try queue.client.getObject(queue.bucket, execution_key, .{});
                legacy = try std.json.parseFromSliceLeaky(Execution, a, previous.body, .{ .allocate = .alloc_always });
            }
            try std.testing.expect(legacy.approved.len != 0);
            legacy.version = 1;
            legacy.scan = 0;
            legacy.next_scan = 1;
            legacy.cursor = .{};
            var wire = try std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, legacy, .{}), .{});
            _ = wire.object.swapRemove("cursor");
            var replaced = try queue.client.compareAndPut(queue.bucket, execution_key, try std.json.Stringify.valueAlloc(a, wire, .{}), previous.metadata.writeRevision() orelse return error.MissingObjectRevision, .{});
            replaced.deinit(queue.client.allocator);
        }
        var deleted = result.deleted_objects;
        // A deletion-limited job spans turns; every turn must retain backlog.
        var turns: usize = 0;
        while (!result.complete and turns < 64) : (turns += 1) {
            try std.testing.expectEqual(counts[1], result.remaining_snapshots);
            result = try run(alloc, binding, options, .{}, request, &.{});
            deleted += result.deleted_objects;
        }
        try std.testing.expect(result.complete and !result.conflicted);
        try std.testing.expectEqual(counts[0], result.expired_snapshots);
        try std.testing.expectEqual(counts[1], result.remaining_snapshots);
        // Reopen the saved immutable job, as after a lost response/restart.
        // Older jobs have no persisted backlog field; derive it from authority.
        const replay = try run(alloc, binding, options, .{}, request, &.{});
        try std.testing.expect(replay.complete and !replay.conflicted);
        try std.testing.expectEqual(counts[1], replay.remaining_snapshots);
        // Expired manifest lists are owned and unreferenced, so they are collected.
        try std.testing.expect(deleted >= counts[0]);
    }
    const final = try managed.load(a);
    const root = try catalog.metadata.parse(a, final.metadata_json);
    try std.testing.expectEqual(@as(usize, 1), (try catalog.metadata.get(root, "snapshots")).array.items.len);
    var cache_queue = try ingestion.openQueue(alloc, binding, options);
    defer cache_queue.deinit();
    var cache_client = cache_queue.client;
    const cache_prefix = try std.fmt.allocPrint(a, "{s}/maintenance/vacuum-inputs/", .{try ingestion.prefix(a, cache_queue.prefix, binding, options)});
    var listed = try cache_client.listObjects(cache_queue.bucket, .{ .prefix = cache_prefix });
    defer listed.deinit(cache_client.allocator);
    try std.testing.expectEqual(@as(usize, 0), listed.entries.len);
    // Completed-job replay no longer depends on temporary input inventories.
    for (expected, 0..) |_, pass| {
        const replay = try run(alloc, binding, options, .{}, .{ .operation_id = try std.fmt.allocPrint(a, "vacuum-{d}", .{pass}), .dry_run = false, .exclusive_ownership = true, .retain_ms = 600000, .keep_latest = 1, .max_expired = 2, .max_deleted = 1 }, &.{});
        try std.testing.expect(replay.complete and !replay.conflicted);
    }
    try std.testing.expectError(error.InvalidLakeMaintenanceLimits, run(alloc, binding, options, .{}, .{ .operation_id = "bad", .max_expired = 0 }, &.{}));
}

test "external lake vacuum resumes 200 snapshots within remote budgets and revalidates advancing authority" {
    const alloc = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-vacuum-backlog");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    var config = try local.common_config.Config.parseFromSlice(alloc, json);
    defer config.deinit();
    const binding: local.serverless_external_source_catalog_binding.Binding = .{ .table_id = "events", .format = .iceberg, .source_uri = try std.fmt.allocPrint(a, "file://{s}/warehouse", .{directory.path()}), .schema_fingerprint = "schema", .write_policy = .iceberg_writer, .catalog = .{ .type = .managed } };
    const options: configured.BindingObjectStoreOpenOptions = .{ .node_config = &config, .catalog_table_id = 11 };
    var opened = try configured.openBindingObjectStoreAlloc(alloc, binding, options);
    defer opened.deinit();
    var managed: catalog.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = binding.source_uri };
    var table = try managed.create(a, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    const files = try ingestion.destinationFiles(a, opened, binding.source_uri, .{});
    // The old inline job exceeded 4 MiB for this ordinary history.
    for (1..201) |n| {
        const row = try std.json.parseFromSliceLeaky(V, a, try std.fmt.allocPrint(a, "{{\"id\":{d}}}", .{n}), .{});
        const batch: catalog.row_commit.Batch = .{ .batch_id = try std.fmt.allocPrint(a, "b{d}", .{n}), .source = "s", .epoch = "1", .checkpoint = try std.fmt.allocPrint(a, "{d}", .{n}), .expected_checkpoint = if (n == 1) null else try std.fmt.allocPrint(a, "{d}", .{n - 1}), .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = row }} };
        const prepared = try catalog.row_commit.prepare(a, table, files, batch, n, @intCast(n + 1));
        table = try managed.commit(a, .{ .id = batch.batch_id, .expected_metadata_location = table.metadata_location, .body = prepared.body, .timestamp_ms = @intCast(n + 1) });
    }
    const request: Options = .{ .operation_id = "large-history", .dry_run = false, .exclusive_ownership = true, .retain_ms = 10 * 60 * 1000, .keep_latest = 1, .max_plan_steps = 5 };
    const metrics = @import("objectstore").operation_metrics;
    const put_index = @backingInt(metrics.Operation.put);
    const delete_index = @backingInt(metrics.Operation.conditional_delete);
    const Budget = struct {
        puts: u64,
        deletes: ?u64 = null,
        fn writes() u64 {
            const calls = metrics.runtime.snapshot().calls;
            return calls[put_index] + calls[@backingInt(metrics.Operation.conditional_put)];
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.deletes) |start| if (metrics.runtime.snapshot().calls[delete_index] >= start + 2) return error.InjectedSweepInterruption;
            if (writes() - self.puts >= 600) return error.DeadlineExceeded;
        }
    };
    var result: Result = .{};
    var deleted: usize = 0;
    var turns: usize = 0;
    var interrupted = false;
    while (!result.complete and turns < 2048) : (turns += 1) {
        var operation_budget: Budget = .{ .puts = Budget.writes(), .deletes = if (interrupted) null else metrics.runtime.snapshot().calls[delete_index] };
        result = run(alloc, binding, options, .{ .checkpoint = .{ .ptr = &operation_budget, .check = Budget.check } }, request, &.{}) catch |err| {
            if (!interrupted and metrics.runtime.snapshot().calls[delete_index] >= operation_budget.deletes.? + 2) {
                // Physical deletion happened, but its progress checkpoint did
                // not. Reopen the same fenced batch on the following turn.
                interrupted = true;
                continue;
            }
            return err;
        };
        deleted += result.deleted_objects;
        if (turns == 0) {
            try std.testing.expect(result.planning and !result.complete);
            // Ingestion advances authority while the durable planner is open.
            const row = try std.json.parseFromSliceLeaky(V, a, "{\"id\":201}", .{});
            const batch: catalog.row_commit.Batch = .{ .batch_id = "b201", .source = "s", .epoch = "1", .checkpoint = "201", .expected_checkpoint = "200", .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = row }} };
            const prepared = try catalog.row_commit.prepare(a, table, files, batch, 201, 202);
            table = try managed.commit(a, .{ .id = batch.batch_id, .expected_metadata_location = table.metadata_location, .body = prepared.body, .timestamp_ms = 202 });
            const metadata = try catalog.metadata.parse(a, table.metadata_json);
            const first_id = try catalog.metadata.int(try catalog.metadata.get((try catalog.metadata.get(metadata, "snapshots")).array.items[0], "snapshot-id"));
            const tag = try std.json.Stringify.valueAlloc(a, .{ .requirements = .{}, .updates = .{.{ .action = "set-snapshot-ref", .@"ref-name" = "review-pin", .type = "tag", .@"snapshot-id" = first_id }} }, .{});
            table = try managed.commit(a, .{ .id = "tag-during-planning", .expected_metadata_location = table.metadata_location, .body = tag, .timestamp_ms = 203 });
        }
    }
    try std.testing.expect(interrupted);
    try std.testing.expect(turns > 2 and result.complete and !result.conflicted);
    try std.testing.expectEqual(@as(usize, 198), result.expired_snapshots);
    try std.testing.expectEqual(@as(usize, 1), result.remaining_snapshots);
    try std.testing.expect(deleted >= 197);
    var queue = try ingestion.openQueue(alloc, binding, options);
    defer queue.deinit();
    const key = try std.fmt.allocPrint(a, "{s}/maintenance/vacuum/{s}.json", .{ try ingestion.prefix(a, queue.prefix, binding, options), catalog.types.digestHex(request.operation_id) });
    var saved = try queue.client.getObject(queue.bucket, key, .{});
    defer saved.deinit(queue.client.allocator);
    try std.testing.expect(saved.body.len < plan.inline_budget + 64 * 1024);
    const job = try std.json.parseFromSliceLeaky(Job, a, saved.body, .{});
    try std.testing.expectEqual(@as(u8, 2), job.version);
    try std.testing.expect(job.nodes.len > 0);
    for (job.retired) |retired| {
        try std.testing.expect(retired.objects.len == 0 and retired.object_plan != null);
    }
    // Cold turns must reuse bounded visited-node metadata, not fetch the same
    // immutable trie on every resume. The saved cursor remains the authority.
    const hint_key = try std.fmt.allocPrint(a, "{s}/maintenance/vacuum-traversal/warm-v1.json", .{try ingestion.prefix(a, queue.prefix, binding, options)});
    var hint = try queue.client.getObject(queue.bucket, hint_key, .{});
    defer hint.deinit(queue.client.allocator);
    try std.testing.expect(hint.body.len <= catalog.retirement_index.hint_bytes);
    // Expired source lists are gone. A fresh process must finish from journal
    // inventories, without relying on their cached source bytes.
    const replay = try run(alloc, binding, options, .{}, request, &.{});
    try std.testing.expect(replay.complete and !replay.conflicted);
    try std.testing.expectEqual(@as(usize, 198), replay.expired_snapshots);
    try std.testing.expectEqual(@as(usize, 0), replay.deleted_objects);
}

test "external lake vacuum planner resumes large manifest lists from private cached chunks" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const storage = @import("objectstore");
    var memory = storage.MemoryClient.init(alloc);
    defer memory.deinit();
    var authority: catalog.managed.Managed = .{ .client = memory.client(), .bucket = "archive", .prefix = "events", .source_uri = "gs://archive/events" };
    var table = try authority.create(a, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    const files: catalog.row_commit.Files = .{ .client = memory.client(), .bucket = "archive", .prefix = "events", .uri = "gs://archive/events" };
    const Count = struct {
        reads: usize = 0,
        fn matches(raw: *anyopaque, _: []const u8, key: []const u8) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (std.mem.endsWith(u8, key, "/wide-list.avro")) self.reads += 1;
            return false;
        }
    };
    var cache = storage.MemoryClient.init(alloc);
    defer cache.deinit();
    var faults = storage.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    var count: Count = .{};
    var wide_size: usize = 0;
    for (1..3) |n| {
        const row = try std.json.parseFromSliceLeaky(V, a, try std.fmt.allocPrint(a, "{{\"id\":{d}}}", .{n}), .{});
        const batch: catalog.row_commit.Batch = .{ .batch_id = try std.fmt.allocPrint(a, "b{d}", .{n}), .source = "s", .epoch = "1", .checkpoint = try std.fmt.allocPrint(a, "{d}", .{n}), .expected_checkpoint = if (n == 1) null else "1", .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = row }} };
        const prepared = try catalog.row_commit.prepare(a, table, files, batch, n, @intCast(n + 1));
        const body = try std.json.parseFromSliceLeaky(V, a, prepared.body, .{});
        if (n == 1) {
            for ((try catalog.metadata.get(body, "updates")).array.items) |*update| {
                if (!std.mem.eql(u8, try catalog.metadata.str(try catalog.metadata.get(update.*, "action")), "add-snapshot")) continue;
                const snapshot = update.object.getPtr("snapshot").?;
                const original = try iceberg.parseManifestListAlloc(a, try catalog.row_commit.read(a, files, try catalog.metadata.str(try catalog.metadata.get(snapshot.*, "manifest-list"))));
                const entries = try a.alloc(V, original.entries.len);
                for (original.entries, entries) |entry, *out| out.* = try catalog.row_commit.listEntry(a, entry);
                const padding = try a.alloc(u8, 8 * 1024 * 1024);
                @memset(padding, 'x');
                const bytes = try catalog.avro_writer.ocf(a, catalog.avro_writer.list_schema, entries, &.{ .{ .key = "format-version", .value = "2" }, .{ .key = "unused-padding", .value = padding } });
                wide_size = bytes.len;
                const uri = try catalog.row_commit.upload(a, files, "metadata/wide-list.avro", bytes);
                try snapshot.object.put(a, "manifest-list", .{ .string = uri });
            }
        }
        table = try authority.commit(a, .{ .id = batch.batch_id, .expected_metadata_location = table.metadata_location, .body = try std.json.Stringify.valueAlloc(a, body, .{}), .timestamp_ms = @intCast(n + 1) });
    }
    const root = try catalog.metadata.parse(a, table.metadata_json);
    const uuid = try catalog.metadata.str(try catalog.metadata.get(root, "table-uuid"));
    faults.get_filter = .{ .ptr = &count, .matches = Count.matches };
    var source_files = files;
    source_files.client = faults.client();
    const pin_store: pins.Store = .{ .client = cache.client(), .bucket = "journal", .prefix = "pins", .context = .{} };
    const request: Options = .{ .operation_id = "wide", .dry_run = false, .exclusive_ownership = true, .keep_latest = 1, .retain_ms = 600000 };
    for (0..3) |turn| {
        var plans: plan.Store = .{ .a = a, .client = cache.client(), .bucket = "journal", .prefix = "private", .context = .{}, .dry_run = false, .defer_writes = true };
        var budget: u64 = 256 * 1024 * 1024;
        var manifests: ManifestFiles = .empty;
        const result = try planningTurn(a, cache.client(), "journal", "private/planning.json", root, uuid, &.{}, request, pin_store, source_files, &plans, &budget, &manifests);
        if (turn == 0) try std.testing.expect(result == null) else try std.testing.expect(result != null);
    }
    var client = cache.client();
    var object = try client.getObject("journal", "private/planning.json", .{});
    defer object.deinit(alloc);
    const progress = try std.json.parseFromSliceLeaky(Planning, a, object.body, .{});
    try std.testing.expect(wide_size > 8 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 1), count.reads);
    try std.testing.expectEqual(@as(usize, 0), progress.manifest);
    try std.testing.expectEqual(@as(usize, 1), progress.candidate);
    try std.testing.expectEqual(@as(usize, 1), progress.retired.len);
}

test "external lake vacuum progresses while ingestion advances metadata every turn" {
    const alloc = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-vacuum-continuous-ingestion");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    var config = try local.common_config.Config.parseFromSlice(alloc, json);
    defer config.deinit();
    const binding: local.serverless_external_source_catalog_binding.Binding = .{ .table_id = "events", .format = .iceberg, .source_uri = try std.fmt.allocPrint(a, "file://{s}/warehouse", .{directory.path()}), .schema_fingerprint = "schema", .write_policy = .iceberg_writer, .catalog = .{ .type = .managed } };
    const options: configured.BindingObjectStoreOpenOptions = .{ .node_config = &config, .catalog_table_id = 11 };
    var opened = try configured.openBindingObjectStoreAlloc(alloc, binding, options);
    defer opened.deinit();
    const authority: catalog.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = binding.source_uri };
    var created = try authority.create(alloc, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    created.deinit(alloc);
    const files = try ingestion.destinationFiles(a, opened, binding.source_uri, .{});
    const Writer = struct {
        fn append(allocator: A, managed: catalog.managed.Managed, destination: catalog.row_commit.Files, n: usize) !void {
            var work = std.heap.ArenaAllocator.init(allocator);
            defer work.deinit();
            const scratch = work.allocator();
            var table = try managed.load(scratch);
            defer table.deinit(scratch);
            const row = try std.json.parseFromSliceLeaky(V, scratch, try std.fmt.allocPrint(scratch, "{{\"id\":{d}}}", .{n}), .{});
            const batch: catalog.row_commit.Batch = .{ .batch_id = try std.fmt.allocPrint(scratch, "b{d}", .{n}), .source = "s", .epoch = "1", .checkpoint = try std.fmt.allocPrint(scratch, "{d}", .{n}), .expected_checkpoint = if (n == 1) null else try std.fmt.allocPrint(scratch, "{d}", .{n - 1}), .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = row }} };
            const prepared = try catalog.row_commit.prepare(scratch, table, destination, batch, n, @intCast(n + 1));
            var published = try managed.commit(scratch, .{ .id = batch.batch_id, .expected_metadata_location = table.metadata_location, .body = prepared.body, .timestamp_ms = @intCast(n + 1) });
            published.deinit(scratch);
        }
    };
    for (1..21) |n| try Writer.append(alloc, authority, files, n);
    const metrics = @import("objectstore").operation_metrics;
    const Budget = struct {
        start: u64,
        fn writes() u64 {
            const calls = metrics.runtime.snapshot().calls;
            return calls[@backingInt(metrics.Operation.put)] + calls[@backingInt(metrics.Operation.conditional_put)];
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (writes() - self.start >= 600) return error.TestUnexpectedResult;
        }
    };
    var result: Result = .{};
    var deleted: usize = 0;
    var turns: usize = 0;
    while (!result.complete and turns < 256) : (turns += 1) {
        // Production reconciliation drains WAL before running maintenance.
        try Writer.append(alloc, authority, files, 21 + turns);
        var budget: Budget = .{ .start = Budget.writes() };
        result = try run(alloc, binding, options, .{ .checkpoint = .{ .ptr = &budget, .check = Budget.check } }, .{ .operation_id = "continuous-ingestion", .dry_run = false, .exclusive_ownership = true, .retain_ms = 600000, .keep_latest = 1 }, &.{});
        deleted += result.deleted_objects;
    }
    try std.testing.expect(result.complete and !result.conflicted);
    try std.testing.expect(deleted != 0);
    try std.testing.expectEqual(@as(usize, 20), result.expired_snapshots);
    const current = try authority.load(a);
    try std.testing.expectEqual(@as(u64, 20 + turns), try ingestion.coverage(a, current));
}

test "external lake vacuum no-op jobs reclaim inputs and replay without catalog mutation" {
    const alloc = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-vacuum-noop-cleanup");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    var config = try local.common_config.Config.parseFromSlice(alloc, json);
    defer config.deinit();
    const binding: local.serverless_external_source_catalog_binding.Binding = .{ .table_id = "events", .format = .iceberg, .source_uri = try std.fmt.allocPrint(a, "file://{s}/warehouse", .{directory.path()}), .schema_fingerprint = "schema", .write_policy = .iceberg_writer, .catalog = .{ .type = .managed } };
    const options: configured.BindingObjectStoreOpenOptions = .{ .node_config = &config, .catalog_table_id = 11 };
    var opened = try configured.openBindingObjectStoreAlloc(alloc, binding, options);
    defer opened.deinit();
    const authority: catalog.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = binding.source_uri };
    var table = try authority.create(a, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    const files = try ingestion.destinationFiles(a, opened, binding.source_uri, .{});
    for (1..3) |n| {
        const row = try std.json.parseFromSliceLeaky(V, a, try std.fmt.allocPrint(a, "{{\"id\":{d}}}", .{n}), .{});
        const batch: catalog.row_commit.Batch = .{ .batch_id = try std.fmt.allocPrint(a, "b{d}", .{n}), .source = "s", .epoch = "1", .checkpoint = try std.fmt.allocPrint(a, "{d}", .{n}), .expected_checkpoint = if (n == 1) null else "1", .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = row }} };
        const prepared = try catalog.row_commit.prepare(a, table, files, batch, n, @intCast(n + 1));
        table = try authority.commit(a, .{ .id = batch.batch_id, .expected_metadata_location = table.metadata_location, .body = prepared.body, .timestamp_ms = @intCast(n + 1) });
    }
    const root = try catalog.metadata.parse(a, table.metadata_json);
    const snapshots = (try catalog.metadata.get(root, "snapshots")).array.items;
    const protected = try a.alloc([]const u8, snapshots.len);
    for (snapshots, protected) |snapshot, *id| id.* = try std.fmt.allocPrint(a, "{d}", .{try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id"))});
    const request: Options = .{ .operation_id = "noop-cleanup", .dry_run = false, .exclusive_ownership = true, .retain_ms = 600000, .keep_latest = 1, .max_plan_steps = 3 };
    var result = try run(alloc, binding, options, .{}, request, &.{});
    try std.testing.expect(result.planning and !result.complete);
    // New protections invalidate the candidates after their inputs were cached.
    for (0..16) |_| {
        result = try run(alloc, binding, options, .{}, request, protected);
        if (result.complete) break;
    }
    try std.testing.expect(result.complete and result.expired_snapshots == 0);
    var queue = try ingestion.openQueue(alloc, binding, options);
    defer queue.deinit();
    var client = queue.client;
    const base = try ingestion.prefix(a, queue.prefix, binding, options);
    const input_store = try inputs.Store.init(a, queue.client, queue.bucket, base, request.operation_id, .{});
    var listed = try client.listObjects(queue.bucket, .{ .prefix = input_store.prefix });
    defer listed.deinit(client.allocator);
    try std.testing.expectEqual(@as(usize, 0), listed.entries.len);
    try std.testing.expectError(error.FileNotFound, client.getObject(queue.bucket, input_store.planning_key, .{}));
    // Dropping those protections cannot replan the same already completed ID.
    const replay = try run(alloc, binding, options, .{}, request, &.{});
    try std.testing.expect(replay.complete and replay.expired_snapshots == 0);
    var current = try authority.load(a);
    defer current.deinit(a);
    try std.testing.expectEqualStrings(table.metadata_location, current.metadata_location);

    // An uncertain planning-checkpoint response must not release its writer
    // lease: the remote write may still be completing after the caller fails.
    var faults = @import("objectstore").ScriptedFaultClient.init(alloc, queue.client);
    defer faults.deinit();
    var failed_request = request;
    failed_request.operation_id = "failed-planner";
    const failed_inputs = try inputs.Store.init(a, faults.client(), queue.bucket, base, failed_request.operation_id, .{});
    var target_key = .{ .key = failed_inputs.planning_key };
    faults.put_filter = .{ .ptr = &target_key, .matches = struct {
        fn matches(raw: *anyopaque, _: []const u8, key: []const u8, _: []const u8) bool {
            const expected_key: *const struct { key: []const u8 } = @ptrCast(@alignCast(raw));
            return std.mem.eql(u8, expected_key.key, key);
        }
    }.matches };
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    var failed_plans: plan.Store = .{ .a = a, .client = faults.client(), .bucket = queue.bucket, .prefix = base, .context = .{}, .dry_run = false, .defer_writes = true };
    const failed_pins: pins.Store = .{ .client = queue.client, .bucket = queue.bucket, .prefix = "failed-pins", .context = .{} };
    var failed_budget: u64 = 256 * 1024 * 1024;
    var failed_cache: ManifestFiles = .empty;
    try std.testing.expectError(error.ConnectionResetByPeer, planningTurn(a, faults.client(), queue.bucket, failed_inputs.planning_key, root, try catalog.metadata.str(try catalog.metadata.get(root, "table-uuid")), &.{}, failed_request, failed_pins, files, &failed_plans, &failed_budget, &failed_cache));
    var registration = try client.getObject(queue.bucket, failed_inputs.state_key, .{});
    defer registration.deinit(client.allocator);
    const admission = try std.json.parseFromSliceLeaky(V, a, registration.body, .{});
    try std.testing.expectEqual(@as(usize, 1), (try catalog.metadata.get(admission, "leases")).array.items.len);
}

test "external lake vacuum selection crosses retained objects without spending the delete allowance" {
    try verifyBufferedSelection(4);
}

test "external lake vacuum selection retains excess candidates across cold deletion limited turns" {
    try verifyBufferedSelection(22);
}

fn verifyBufferedSelection(owned_count: usize) !void {
    const alloc = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-vacuum-selection");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    var config = try local.common_config.Config.parseFromSlice(alloc, json);
    defer config.deinit();
    const binding: local.serverless_external_source_catalog_binding.Binding = .{ .table_id = "events", .format = .iceberg, .source_uri = try std.fmt.allocPrint(a, "file://{s}/warehouse", .{directory.path()}), .schema_fingerprint = "schema", .write_policy = .iceberg_writer, .catalog = .{ .type = .managed } };
    const options: configured.BindingObjectStoreOpenOptions = .{ .node_config = &config, .catalog_table_id = 11 };
    var opened = try configured.openBindingObjectStoreAlloc(alloc, binding, options);
    defer opened.deinit();
    const authority: catalog.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = binding.source_uri };
    const table = try authority.create(a, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    const files = try ingestion.destinationFiles(a, opened, binding.source_uri, .{});
    const base = try ingestion.prefix(a, opened.prefix, binding, options);
    const request: Options = .{ .operation_id = "selection", .dry_run = false, .exclusive_ownership = true, .retain_ms = 600000, .keep_latest = 1, .max_deleted = 1 };
    const input_store = try inputs.Store.init(a, opened.client, opened.bucket, base, request.operation_id, .{});
    try input_store.close(a);
    var objects: std.ArrayList([]const u8) = .empty;
    // Retained, externally owned files precede the expired native files in an
    // already committed job. Finding them must consume traversal work, rather
    // than the physical-deletion allowance or an entire maintenance turn.
    for (0..24) |i| {
        const name = try std.fmt.allocPrint(a, "data/external-{d}.parquet", .{i});
        var stored = try opened.client.putObject(opened.bucket, try std.fmt.allocPrint(a, "{s}{s}{s}", .{ files.prefix, if (files.prefix.len == 0) "" else "/", name }), "external", .{});
        stored.deinit(opened.client.allocator);
        try objects.append(a, try std.fmt.allocPrint(a, "{s}/{s}", .{ files.uri, name }));
    }
    for (0..owned_count) |i| try objects.append(a, try catalog.row_commit.upload(a, files, try std.fmt.allocPrint(a, "data/expired-{d}.parquet", .{i}), "expired"));
    const job: Job = .{ .id = "retired-job", .expected = table.metadata_location, .body = "{}", .timestamp_ms = 2, .retired = &.{.{ .snapshot = "2", .objects = objects.items }} };
    const pin_store: pins.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = try std.fmt.allocPrint(a, "{s}/selection-pins", .{base}) };
    const execution_key = try std.fmt.allocPrint(a, "{s}/maintenance/vacuum-execution/{s}.json", .{ base, catalog.types.digestHex(request.operation_id) });
    var deleted: usize = 0;
    const turns = owned_count + try std.math.divCeil(usize, owned_count, 16);
    for (0..turns) |turn| {
        // Reopen every turn, including the deletion-limited approved batch.
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        var plans: plan.Store = .{ .a = scratch.allocator(), .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = .{}, .dry_run = false };
        const result = try executeJob(scratch.allocator(), binding, options, request, &.{}, pin_store, files, &plans, job);
        try std.testing.expect(result.deleted_objects <= request.max_deleted);
        deleted += result.deleted_objects;
        if (turn == 0) {
            try std.testing.expectEqual(@as(usize, 0), result.deleted_objects);
            try std.testing.expectEqual(@as(usize, 24), result.retained_objects);
            var saved = try opened.client.getObject(opened.bucket, execution_key, .{});
            defer saved.deinit(opened.client.allocator);
            const progress = try std.json.parseFromSliceLeaky(Execution, scratch.allocator(), saved.body, .{});
            try std.testing.expectEqual(@min(16, owned_count), progress.approved.len);
            try std.testing.expectEqual(owned_count - @min(16, owned_count), progress.pending.len);
            try std.testing.expectEqual(@as(u8, 3), progress.version);
            try std.testing.expectEqual(@as(usize, 0), progress.approved_index);
        }
        try std.testing.expectEqual(turn + 1 == turns, result.complete);
    }
    try std.testing.expectEqual(owned_count, deleted);
    for (0..24) |i| {
        var preserved = try opened.client.getObject(opened.bucket, try std.fmt.allocPrint(a, "{s}{s}data/external-{d}.parquet", .{ files.prefix, if (files.prefix.len == 0) "" else "/", i }), .{});
        defer preserved.deinit(opened.client.allocator);
        try std.testing.expectEqualStrings("external", preserved.body);
    }
}
