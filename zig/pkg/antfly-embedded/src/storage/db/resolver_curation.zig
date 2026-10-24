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

//! Shared resolver curation operations. Transports never reconstruct redirects
//! themselves: one versioned catalog update owns state, fences and replay.
const std = @import("std");
const index_metadata = @import("../../api/index_metadata.zig");
const resolver = @import("antfly_resolver");
const catalog = @import("catalog/resolver_catalog.zig");
pub const metadata_key = "_resolver_curation";
pub const MetadataIdentity = struct {
    name: []const u8,
    table: []const u8,
    key_template: []const u8,
    merges: []const resolver.KeyAlias = &.{},
    state: resolver.curation.State = .{},
    revision: u64,
    namespace_generation: u64 = 0,
    dropped: bool = false,
    namespace_owned: bool = false,
    authority_table_id: u64 = 0,
    projection_only: bool = false,
    authority_missing: bool = false,
    projection_ref: ?[]const u8 = null,
    // Per-read borrowed policy cache; never accepted as serialized authority.
    state_json: []const u8 = "",
    policy_validated: bool = false,
    shared_projection: ?*catalog.SharedProjection = null,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            if (comptime !std.mem.eql(u8, field, "state_json") and !std.mem.eql(u8, field, "policy_validated") and !std.mem.eql(u8, field, "shared_projection")) {
                const include = if (comptime std.mem.eql(u8, field, "projection_ref")) self.projection_ref != null else if (comptime std.mem.eql(u8, field, "state") or std.mem.eql(u8, field, "merges")) self.projection_ref == null else true;
                if (include) {
                    try writer.objectField(field);
                    try writer.write(@field(self, field));
                }
            }
        }
        try writer.endObject();
    }
};
pub const MetadataProjection = struct { id: []const u8, namespace: Namespace };
pub const Metadata = struct {
    version: u32 = 1,
    identities: []const MetadataIdentity = &.{},
    projections: []const MetadataProjection = &.{},
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        try writer.objectField("version");
        try writer.write(self.version);
        try writer.objectField("identities");
        try writer.write(self.identities);
        if (self.projections.len > 0) {
            try writer.objectField("projections");
            try writer.write(self.projections);
        }
        try writer.endObject();
    }
};
/// Authoritative corrections belong to a destination table and exact key template.
pub const namespaces_key = "_resolver_namespaces";
pub const Namespace = @import("resolver_namespace.zig").Namespace;
pub const Namespaces = @import("resolver_namespace.zig").Namespaces;
pub const proposals_key = "_curation_proposals";

/// Borrow only authority identities from the parsed catalog. Evidence and
/// attribution are irrelevant to ownership and never copied into this index.
pub const Ownership = struct {
    templates: std.StringHashMapUnmanaged(void) = .empty,
    proposal_keys: std.ArrayListUnmanaged([]const u8) = .empty,

    pub fn init(a: std.mem.Allocator, root: std.json.Value) !Ownership {
        if (root != .object) return error.InvalidResolverCurationRequest;
        var out: Ownership = .{};
        if (root.object.get("_resolver_record_owners")) |raw| {
            if (raw != .array or raw.array.items.len > 4096) return error.InvalidResolverCurationRequest;
            for (raw.array.items) |item| {
                if (item != .string) return error.InvalidResolverCurationRequest;
                try out.templates.put(a, item.string, {});
            }
        }
        if (root.object.get(namespaces_key)) |raw| {
            if (raw != .object) return error.InvalidResolverCurationRequest;
            if (raw.object.get("namespaces")) |items| {
                if (items != .array or items.array.items.len > 4096) return error.InvalidResolverCurationRequest;
                for (items.array.items) |item| {
                    if (item != .object) return error.InvalidResolverCurationRequest;
                    const template = item.object.get("key_template") orelse return error.InvalidResolverCurationRequest;
                    if (template != .string) return error.InvalidResolverCurationRequest;
                    try out.templates.put(a, template.string, {});
                }
            }
        }
        if (root.object.get(proposals_key)) |raw| {
            if (raw != .object) return error.InvalidResolverCurationRequest;
            if (raw.object.get("proposals")) |items| {
                if (items != .array or items.array.items.len > 4096) return error.InvalidResolverCurationRequest;
                for (items.array.items) |item| {
                    if (item != .object) return error.InvalidResolverCurationRequest;
                    const key = item.object.get("key") orelse return error.InvalidResolverCurationRequest;
                    if (key != .string) return error.InvalidResolverCurationRequest;
                    try out.proposal_keys.append(a, key.string);
                }
            }
        }
        return out;
    }
    pub fn owns(self: Ownership, template: []const u8) bool {
        if (self.templates.contains(template)) return true;
        for (self.proposal_keys.items) |key| if (keyInNamespace(template, key)) return true;
        return false;
    }
};

pub fn namespacesFromValue(a: std.mem.Allocator, root: std.json.Value) !Namespaces {
    const value = if (root == .object) root.object.get(namespaces_key) orelse return .{} else return .{};
    const result = try std.json.parseFromSliceLeaky(Namespaces, a, try std.json.Stringify.valueAlloc(a, value, .{}), .{});
    if (result.version != 1 or result.namespaces.len > 4096) return error.InvalidResolverCurationVersion;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    for (result.namespaces) |item| {
        if (item.key_template.len == 0 or (item.revision == 0 and !item.record_store)) return error.InvalidResolverCurationRequest;
        if ((try seen.getOrPut(a, item.key_template)).found_existing) return error.InvalidResolverCurationRequest;
        _ = try resolver.curation.RedirectIndex.init(a, item.merges);
        _ = try resolver.curation.Index.init(a, "", try std.json.Stringify.valueAlloc(a, item.state, .{}));
    }
    return result;
}
pub fn replaceNamespace(a: std.mem.Allocator, root: *std.json.Value, next: Namespace) !void {
    const current = try namespacesFromValue(a, root.*);
    var items: std.ArrayListUnmanaged(Namespace) = .empty;
    var found = false;
    for (current.namespaces) |item| {
        const match = std.mem.eql(u8, item.key_template, next.key_template);
        try items.append(a, if (match) next else item);
        found = found or match;
    }
    if (!found) try items.append(a, next);
    try root.object.put(a, namespaces_key, try json(a, Namespaces{ .namespaces = items.items }));
}
/// Upgrade folds identical legacy corrections and rejects disagreements before
/// publishing the shared authority. Declarative aliases remain source-local.
pub fn foldNamespace(a: std.mem.Allocator, current: Namespace, cfg: catalog.ResolverConfig) !Namespace {
    // Worker projections are never migration authority. In particular, a
    // dropped/recreated destination must not revive its predecessor's policy.
    if (cfg.namespace_curation_table_id != 0 or cfg.namespace_curation_generation != 0) return current;
    var next = current;
    var aliases: std.StringArrayHashMapUnmanaged([]const u8) = .empty;
    for (current.merges) |item| try aliases.put(a, item.key, item.target);
    for (cfg.merges) |item| {
        if (aliases.get(item.key)) |target| {
            if (!std.mem.eql(u8, target, item.target)) return error.ResolverNamespaceMigrationConflict;
        } else try aliases.put(a, item.key, item.target);
    }
    const merged = try a.alloc(resolver.KeyAlias, aliases.count());
    for (aliases.keys(), aliases.values(), merged) |key, target, *item| item.* = .{ .key = key, .target = target };
    _ = try resolver.curation.RedirectIndex.init(a, merged);
    const prior = try resolver.curation.parseState(a, cfg.curation_json);
    var keys: std.StringArrayHashMapUnmanaged(void) = .empty;
    for (current.state.suppress.keys) |key| try keys.put(a, key, {});
    for (prior.suppress.keys) |key| try keys.put(a, key, {});
    var texts: std.StringArrayHashMapUnmanaged(void) = .empty;
    for (current.state.suppress.texts) |text| try texts.put(a, text, {});
    for (prior.suppress.texts) |text| try texts.put(a, text, {});
    var removed: std.StringArrayHashMapUnmanaged(void) = .empty;
    for (current.state.removed_aliases) |key| try removed.put(a, key, {});
    for (prior.removed_aliases) |key| try removed.put(a, key, {});
    var distinct: std.ArrayListUnmanaged(resolver.curation.Pair) = .empty;
    try distinct.appendSlice(a, current.state.distinct);
    for (prior.distinct) |pair| {
        const exists = for (distinct.items) |old| {
            if ((std.mem.eql(u8, old.from, pair.from) and std.mem.eql(u8, old.to, pair.to)) or (std.mem.eql(u8, old.from, pair.to) and std.mem.eql(u8, old.to, pair.from))) break true;
        } else false;
        if (!exists) try distinct.append(a, pair);
    }
    next.merges = merged;
    var suppression_entries: std.StringArrayHashMapUnmanaged(resolver.curation.Entry) = .empty;
    for (prior.suppress.entries) |entry| try suppression_entries.put(a, entry.key, entry);
    for (current.state.suppress.entries) |entry| try suppression_entries.put(a, entry.key, entry);
    var text_entries: std.StringArrayHashMapUnmanaged(resolver.curation.Entry) = .empty;
    for (prior.suppress.text_entries) |entry| try text_entries.put(a, entry.key, entry);
    for (current.state.suppress.text_entries) |entry| try text_entries.put(a, entry.key, entry);
    var origins: std.StringArrayHashMapUnmanaged(resolver.curation.RedirectOrigin) = .empty;
    for (prior.redirect_origins) |entry| try origins.put(a, entry.key, entry);
    for (current.state.redirect_origins) |entry| try origins.put(a, entry.key, entry);
    next.state = .{ .redirect_origins = origins.values(), .suppress = .{ .entries = suppression_entries.values(), .text_entries = text_entries.values(), .keys = keys.keys(), .texts = texts.keys(), .min_text_chars = @max(current.state.suppress.min_text_chars, prior.suppress.min_text_chars) }, .removed_aliases = removed.keys(), .distinct = distinct.items };
    var index = try resolver.curation.Index.init(a, "", try std.json.Stringify.valueAlloc(a, next.state, .{}));
    try index.bindRedirects(a, try resolver.curation.RedirectIndex.init(a, merged));
    for (merged) |item| if (index.keySuppressed(item.target) or try index.pinnedDistinct(a, item.key, item.target)) return error.ResolverNamespaceMigrationConflict;
    next.revision = @max(next.revision, cfg.curation_revision);
    return next;
}

pub const MetadataPlan = struct { indexes_json: []const u8, changed: bool, revision: u64, config_generation: u64 };

/// Scratch-arena result used by the metadata CAS path. Index declarations are
/// only read here; the edit changes the reserved table-owned identity record.
pub const NamedRequest = struct { name: []const u8, request: resolver.curation.Request };
pub const NamedResult = struct { name: []const u8, changed: bool, revision: u64, config_generation: u64 };
pub const MetadataBatchPlan = struct { indexes_json: []const u8, changed: bool, results: []const NamedResult };

pub fn planMetadata(a: std.mem.Allocator, indexes_json: []const u8, name: []const u8, request: resolver.curation.Request) !MetadataPlan {
    const batch = try planMetadataBatch(a, indexes_json, &.{.{ .name = name, .request = request }});
    const result = batch.results[0];
    return .{ .indexes_json = batch.indexes_json, .changed = result.changed, .revision = result.revision, .config_generation = result.config_generation };
}

/// Multiple identities in one source table publish in one metadata CAS. Parse
/// the declarations once and serialize the resulting table definition once.
pub fn planMetadataBatch(a: std.mem.Allocator, indexes_json: []const u8, requests: []const NamedRequest) !MetadataBatchPlan {
    if (requests.len > 4096) return error.ResolverCurationLimitExceeded;
    var root = try std.json.parseFromSliceLeaky(std.json.Value, a, indexes_json, .{});
    const metadata = try metadataFromValue(a, root);
    var desired: std.ArrayListUnmanaged(catalog.ResolverConfig) = .empty;
    try @import("../../metadata/index_reconcile.zig").collectDesiredResolvers(a, root, &desired);
    var definitions: std.StringHashMapUnmanaged(catalog.ResolverConfig) = .empty;
    for (desired.items) |cfg| try definitions.put(a, cfg.name, cfg);
    var records: std.ArrayListUnmanaged(MetadataIdentity) = .empty;
    try records.appendSlice(a, metadata.identities);
    var by_name: std.StringHashMapUnmanaged(usize) = .empty;
    for (metadata.identities, 0..) |identity, i| {
        try by_name.put(a, identity.name, i);
        if (identity.dropped) continue;
        const original = definitions.get(identity.name) orelse catalog.ResolverConfig{ .name = identity.name, .table = identity.table, .key_template = identity.key_template, .source_artifact = "retired_source", .resolution_artifact = "retired_resolution", .promote = false, .retired = true };
        try definitions.put(a, identity.name, try overlayMetadata(a, original, identity));
    }
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    const results = try a.alloc(NamedResult, requests.len);
    var changed = false;
    for (requests, results) |named, *result| {
        const unique = try seen.getOrPut(a, named.name);
        if (unique.found_existing) return error.InvalidResolverCurationRequest;
        const cfg = definitions.get(named.name) orelse return error.ResolverNotFound;
        if (by_name.get(named.name)) |i| if (records.items[i].namespace_owned) return error.ResolverNamespaceCurationRequired;
        const request = named.request;
        if (request.expected_revision) |expected| if (expected != cfg.curation_revision) return error.ResolverCurationConflict;
        const edit = try resolver.curation.planWithOrigin(a, cfg.curation_json, cfg.merges, try cfg.effectiveAliasesAlloc(a), request, manualOrigin(request, try std.math.add(u64, cfg.curation_revision, 1)));
        result.* = .{ .name = named.name, .changed = edit.changed, .revision = cfg.curation_revision, .config_generation = cfg.config_generation };
        if (!edit.changed) continue;
        var next = cfg;
        next.curation_json = try std.json.Stringify.valueAlloc(a, edit.state, .{});
        const base_revision = if (by_name.get(named.name)) |i| @max(cfg.curation_revision, records.items[i].revision) else cfg.curation_revision;
        next.curation_revision = try std.math.add(u64, base_revision, 1);
        next.merges = edit.merges;
        if (next.merges.len > 0) {
            next.merge_generation = try std.math.add(u64, cfg.effectiveGeneration(), 1);
            next.merge_state_version = 1;
        }
        try next.validate();
        const identity: MetadataIdentity = .{ .name = named.name, .table = cfg.table, .key_template = cfg.key_template, .merges = next.merges, .state = edit.state, .revision = next.curation_revision, .namespace_generation = cfg.namespace_generation };
        if (by_name.get(named.name)) |i| records.items[i] = identity else {
            try by_name.put(a, named.name, records.items.len);
            try records.append(a, identity);
        }
        result.revision = next.curation_revision;
        changed = true;
    }
    if (!changed) return .{ .indexes_json = indexes_json, .changed = false, .results = results };
    try root.object.put(a, metadata_key, try json(a, Metadata{ .identities = records.items }));
    const output = try std.json.Stringify.valueAlloc(a, root, .{});
    if (output.len > 16 * 1024 * 1024) return error.ResolverCurationLimitExceeded;
    return .{ .indexes_json = output, .changed = true, .results = results };
}

pub fn metadataFromValue(a: std.mem.Allocator, value: std.json.Value) !Metadata {
    if (value != .object) return .{};
    const raw = value.object.get(metadata_key) orelse return .{};
    var metadata = try std.json.parseFromSliceLeaky(Metadata, a, try std.json.Stringify.valueAlloc(a, raw, .{}), .{ .allocate = .alloc_always });
    if (metadata.version != 1 or metadata.identities.len > 4096 or metadata.projections.len > 4096) return error.InvalidResolverCurationVersion;
    const Payload = struct { namespace: Namespace, projection: *catalog.SharedProjection };
    var shared: std.StringHashMapUnmanaged(Payload) = .empty;
    for (metadata.projections) |projection| {
        const slot = try shared.getOrPut(a, projection.id);
        if (slot.found_existing or projection.id.len != 64) return error.InvalidResolverCurationRequest;
        const encoded = try std.json.Stringify.valueAlloc(a, projection.namespace, .{});
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
        if (!std.mem.eql(u8, projection.id, &std.fmt.bytesToHex(digest, .lower))) return error.InvalidResolverCurationRequest;
        const probe: catalog.ResolverConfig = .{ .name = "projection", .table = "entities", .source_artifact = "source", .resolution_artifact = "resolved", .key_template = projection.namespace.key_template, .merges = projection.namespace.merges, .curation_json = try std.json.Stringify.valueAlloc(a, projection.namespace.state, .{}) };
        try probe.validatePolicy();
        slot.value_ptr.* = .{ .namespace = projection.namespace, .projection = try catalog.SharedProjection.create(a, projection.namespace.merges, probe.curation_json) };
    }
    var names: std.StringHashMapUnmanaged(void) = .empty;
    const identities = try a.dupe(MetadataIdentity, metadata.identities);
    metadata.identities = identities;
    for (identities) |*identity| {
        identity.state_json = "";
        identity.policy_validated = false;
        identity.shared_projection = null;
        if ((identity.revision == 0 and !(identity.namespace_owned and identity.authority_table_id != 0)) or identity.name.len == 0) return error.InvalidResolverCurationRequest;
        if ((try names.getOrPut(a, identity.name)).found_existing) return error.InvalidResolverCurationRequest;
        if (identity.projection_ref) |id| {
            const payload = shared.get(id) orelse return error.InvalidResolverCurationRequest;
            if (!identity.namespace_owned or identity.projection_only or !std.mem.eql(u8, identity.key_template, payload.namespace.key_template) or identity.revision != payload.namespace.revision or identity.authority_missing == payload.namespace.record_store) return error.InvalidResolverCurationRequest;
            identity.merges = payload.projection.merges;
            identity.state = payload.namespace.state;
            identity.state_json = payload.projection.curation_json;
            identity.shared_projection = payload.projection;
            identity.policy_validated = true;
        } else _ = try overlayMetadata(a, .{ .name = identity.name, .table = identity.table, .key_template = identity.key_template, .source_artifact = "metadata", .resolution_artifact = "metadata_resolved" }, identity.*);
    }
    return metadata;
}
pub fn overlayMetadata(a: std.mem.Allocator, cfg: catalog.ResolverConfig, identity: MetadataIdentity) !catalog.ResolverConfig {
    return overlayMetadataCached(a, cfg, identity, null);
}
pub fn overlayMetadataCached(a: std.mem.Allocator, cfg: catalog.ResolverConfig, identity: MetadataIdentity, profiles: ?*std.StringHashMapUnmanaged(void)) !catalog.ResolverConfig {
    if (!std.mem.eql(u8, identity.name, cfg.name) or !std.mem.eql(u8, identity.table, cfg.table) or !std.mem.eql(u8, identity.key_template, cfg.key_template)) return error.ResolverMergeNamespaceChange;
    var next = cfg;
    next.merges = identity.merges;
    next.shared_projection = identity.shared_projection;
    next.curation_json = if (identity.state_json.len > 0) identity.state_json else try std.json.Stringify.valueAlloc(a, identity.state, .{});
    next.curation_revision = identity.revision;
    next.namespace_curation_generation = if (identity.namespace_owned) identity.revision else 0;
    next.namespace_curation_table_id = identity.authority_table_id;
    next.namespace_curation_missing = identity.authority_missing;
    next.namespace_generation = identity.namespace_generation;
    if (next.merges.len > 0) {
        next.merge_generation = @max(identity.revision, cfg.effectiveGeneration());
        next.merge_state_version = 1;
    }
    try next.validateDefinition();
    if (!identity.policy_validated or next.aliases.len > 0 or next.suppress_json.len > 0) {
        var validate = true;
        if (identity.projection_ref) |reference| if (profiles) |cache| {
            const key = try std.json.Stringify.valueAlloc(a, .{ reference, next.aliases, next.suppress_json }, .{});
            validate = !(try cache.getOrPut(a, key)).found_existing;
        };
        if (validate) try next.validatePolicy();
    }
    if (next.shared_projection) |projection| next.validation_proof = .{ .policy_digest = projection.digest, .static_digest = try catalog.policyProfileDigest(a, next.aliases, next.suppress_json) };
    return next;
}

pub fn replaceMetadata(a: std.mem.Allocator, root: *std.json.Value, identity: MetadataIdentity) !void {
    const old = try metadataFromValue(a, root.*);
    var records: std.ArrayListUnmanaged(MetadataIdentity) = .empty;
    var replaced = false;
    for (old.identities) |previous| {
        try records.append(a, if (std.mem.eql(u8, previous.name, identity.name)) identity else previous);
        replaced = replaced or std.mem.eql(u8, previous.name, identity.name);
    }
    if (!replaced) try records.append(a, identity);
    try root.object.put(a, metadata_key, try json(a, Metadata{ .identities = records.items, .projections = old.projections }));
}

pub fn manualOrigin(request: resolver.curation.Request, revision: u64) resolver.curation.Origin {
    return .{ .actor = request.actor, .reason = request.reason, .revision = revision, .created_at_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms };
}

pub fn mutate(db: anytype, a: std.mem.Allocator, name: []const u8, request_json: []const u8) ![]u8 {
    if (request_json.len > 4 * 1024 * 1024) return error.ResolverCurationLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const request = try std.json.parseFromSliceLeaky(resolver.curation.Request, scratch, request_json, .{});
    const configs = try db.core.index_manager.listResolverIdentities(scratch);
    for (configs) |cfg| {
        if (!std.mem.eql(u8, cfg.name, name)) continue;
        if (request.expected_revision) |expected| if (expected != cfg.curation_revision) return error.ResolverCurationConflict;
        const aliases = try cfg.effectiveAliasesAlloc(scratch);
        const plan = try resolver.curation.planWithOrigin(scratch, cfg.curation_json, cfg.merges, aliases, request, manualOrigin(request, try std.math.add(u64, cfg.curation_revision, 1)));
        var next = cfg;
        if (plan.changed) {
            next.curation_json = try std.json.Stringify.valueAlloc(scratch, plan.state, .{});
            next.curation_revision = try std.math.add(u64, cfg.curation_revision, 1);
            next.merges = plan.merges;
            if (cfg.merges.len > 0 or next.merges.len > 0) {
                next.merge_generation = try std.math.add(u64, cfg.effectiveGeneration(), 1);
                next.merge_state_version = if (next.merges.len == 0) 2 else 1;
            }
            try next.validate();
            _ = try db.upsertResolverWithResultOptions(next, .{ .drain_backfill = false, .wait_for_activity = true });
        }
        return std.json.Stringify.valueAlloc(a, .{
            .resolver = name,
            .changed = plan.changed,
            .absorbed = plan.absorbed,
            .revision = next.curation_revision,
            .replay_sequence = db.core.store.lastReplaySequence(0),
            .applied = try applied(db, next),
            .progress = db.pendingWorkStats(),
        }, .{});
    }
    return error.ResolverNotFound;
}

/// Publish a prevalidated edit through the same catalog owner as direct
/// curation. Proposal receipts live in this edit and commit with its policy.
pub fn publishPlan(db: anytype, a: std.mem.Allocator, cfg: catalog.ResolverConfig, edit: resolver.curation.Plan) ![]u8 {
    var next = cfg;
    if (edit.changed) {
        next.curation_json = try std.json.Stringify.valueAlloc(a, edit.state, .{});
        next.curation_revision = try std.math.add(u64, cfg.curation_revision, 1);
        next.merges = edit.merges;
        if (cfg.merges.len > 0 or next.merges.len > 0) {
            next.merge_generation = try std.math.add(u64, cfg.effectiveGeneration(), 1);
            next.merge_state_version = if (next.merges.len == 0) 2 else 1;
        }
        try next.validate();
        _ = try db.upsertResolverWithResultOptions(next, .{ .drain_backfill = false, .wait_for_activity = true });
    }
    return std.json.Stringify.valueAlloc(a, .{ .resolver = cfg.name, .changed = edit.changed, .absorbed = edit.absorbed, .revision = next.curation_revision, .accepted = true, .replay_sequence = db.core.store.lastReplaySequence(0), .replay_sequence_kind = "source_journal", .applied = try applied(db, next), .progress = db.pendingWorkStats() }, .{});
}

pub fn list(db: anytype, a: std.mem.Allocator, name: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const identities = try db.core.index_manager.listResolverIdentities(scratch);
    var out = std.json.Array.init(scratch);
    for (identities) |cfg| {
        if (name.len > 0 and !std.mem.eql(u8, cfg.name, name)) continue;
        const state = try resolver.curation.parseState(scratch, cfg.curation_json);
        var item = try cfg.publicValue(scratch);
        try item.object.put(scratch, "redirects", try json(scratch, try resolver.curation.redirectsWithOrigins(scratch, try cfg.effectiveAliasesAlloc(scratch), state, cfg.merges)));
        try item.object.put(scratch, "curation", try json(scratch, state));
        try item.object.put(scratch, "progress", try json(scratch, db.pendingWorkStats()));
        if (name.len > 0) return std.json.Stringify.valueAlloc(a, item, .{});
        try out.append(item);
    }
    if (name.len > 0) return error.ResolverNotFound;
    return std.json.Stringify.valueAlloc(a, std.json.Value{ .array = out }, .{});
}

fn json(a: std.mem.Allocator, value: anytype) !std.json.Value {
    return std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, value, .{}), .{});
}

test "resolver curation persists revisions redirects suppression and idempotent unmerge" {
    const DB = @import("selected_root.zig").db.DB;
    const a = std.testing.allocator;
    var tmp = try @import("../../common/test_directory.zig").TestDirectory.init("resolver-curation");
    defer tmp.cleanup();
    var db = try DB.open(a, tmp.path(), .{});
    defer db.close();
    try db.addResolver(.{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{ uslug _entity.text }}", .promote = false });
    const merge = try mutate(&db, a, "concepts", "{\"action\":\"merge\",\"from\":\"a\",\"to\":\"b\"}");
    defer a.free(merge);
    try std.testing.expect(std.mem.indexOf(u8, merge, "\"changed\":true") != null);
    const undo = try mutate(&db, a, "concepts", "{\"action\":\"unmerge\",\"from\":\"a\",\"pin_distinct\":true}");
    defer a.free(undo);
    const noop = try mutate(&db, a, "concepts", "{\"action\":\"unmerge\",\"from\":\"a\"}");
    defer a.free(noop);
    try std.testing.expect(std.mem.indexOf(u8, noop, "\"changed\":false") != null);
    try std.testing.expectError(error.ResolverDistinctPair, mutate(&db, a, "concepts", "{\"action\":\"merge\",\"from\":\"a\",\"to\":\"b\"}"));
    try std.testing.expectError(error.ResolverCurationConflict, mutate(&db, a, "concepts", "{\"action\":\"suppress\",\"key\":\"noise\",\"expected_revision\":0}"));
    const suppressed = try mutate(&db, a, "concepts", "{\"action\":\"suppress\",\"keys\":[\"noise\"],\"texts\":[\"Note\"]}");
    defer a.free(suppressed);
    const listing = try list(&db, a, "concepts");
    defer a.free(listing);
    try std.testing.expect(std.mem.indexOf(u8, listing, "\"noise\"") != null);
    const stored = try db.listResolvers(a);
    defer {
        for (stored) |*cfg| cfg.deinit(a);
        a.free(stored);
    }
    try std.testing.expectEqual(@as(u64, 3), stored[0].curation_revision);
    try std.testing.expectEqual(@as(usize, 0), stored[0].merges.len);
}

test "resolver curation metadata owns runtime state independently of graph declarations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const indexes =
        \\{"knowledge":{"name":"knowledge","type":"graph","resolvers":[{"name":"concepts","table":"entities","source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{uslug _entity.text}}","promote":false}]}}
    ;
    const merged = try planMetadata(a, indexes, "concepts", .{ .action = .merge, .from = "a", .to = "b" });
    try std.testing.expectEqual(@as(u64, 1), merged.revision);
    const unchanged = try planMetadata(a, merged.indexes_json, "concepts", .{ .action = .merge, .from = "a", .to = "b" });
    try std.testing.expect(!unchanged.changed);
    const undone = try planMetadata(a, merged.indexes_json, "concepts", .{ .action = .unmerge, .from = "a", .pin_distinct = true });
    try std.testing.expectEqual(@as(u64, 2), undone.revision);
    try std.testing.expectError(error.ResolverDistinctPair, planMetadata(a, undone.indexes_json, "concepts", .{ .action = .merge, .from = "a", .to = "b" }));
    const original = try std.json.parseFromSliceLeaky(std.json.Value, a, indexes, .{});
    const current = try std.json.parseFromSliceLeaky(std.json.Value, a, undone.indexes_json, .{});
    try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, original.object.get("knowledge"), .{}), try std.json.Stringify.valueAlloc(a, current.object.get("knowledge"), .{}));
    try std.testing.expect(@import("../../api/indexes.zig").isReservedIndexMetadataEntry(metadata_key));
}

pub const NamespaceRequest = struct {
    table: []const u8,
    key_template: []const u8,
    reset_merges: bool = false,
    key_map: ?[]const resolver.KeyAlias = null,
    aliases: ?[]const resolver.KeyAlias = null,
    reset_curation: bool = false,
    expected_revision: ?u64 = null,
    wait: ?resolver.curation.Wait = null,
};

/// Namespace migration maps every durable key, including negative policy.
/// Missing mappings fail before catalog publication; reset keeps text policy.
pub fn namespaceConfig(a: std.mem.Allocator, cfg: catalog.ResolverConfig, request: NamespaceRequest) !catalog.ResolverConfig {
    if (request.expected_revision) |revision| if (revision != cfg.curation_revision) return error.ResolverCurationConflict;
    if (request.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
    if (request.reset_merges == (request.key_map != null) or (request.key_map != null and request.aliases != null)) return error.InvalidResolverCurationRequest;
    var mapping: std.StringHashMapUnmanaged([]const u8) = .empty;
    if (request.key_map) |items| for (items) |item| {
        if (item.key.len == 0 or item.target.len == 0) return error.InvalidResolverCurationRequest;
        const slot = try mapping.getOrPut(a, item.key);
        if (slot.found_existing) return error.InvalidResolverCurationRequest;
        slot.value_ptr.* = item.target;
    };
    const Mapper = struct {
        fn key(map: std.StringHashMapUnmanaged([]const u8), original: []const u8) ![]const u8 {
            return map.get(original) orelse error.InvalidResolverCurationRequest;
        }
        fn aliases(allocator: std.mem.Allocator, map: std.StringHashMapUnmanaged([]const u8), input: []const resolver.KeyAlias) ![]const resolver.KeyAlias {
            const out = try allocator.alloc(resolver.KeyAlias, input.len);
            for (input, out) |old, *next| next.* = .{ .key = try key(map, old.key), .target = try key(map, old.target) };
            return out;
        }
        fn keys(allocator: std.mem.Allocator, map: std.StringHashMapUnmanaged([]const u8), input: []const []const u8) ![]const []const u8 {
            const out = try allocator.alloc([]const u8, input.len);
            for (input, out) |old, *next| next.* = try key(map, old);
            return out;
        }
    };
    var next = cfg;
    next.table = request.table;
    next.key_template = request.key_template;
    var state = try resolver.curation.parseState(a, cfg.curation_json);
    // Receipts are scoped to the exact destination namespace. The table queue
    // retains history; carrying receipts to another table could mistake that
    // table's equal action/key proposal ID for an already applied decision.
    if (!std.mem.eql(u8, cfg.table, request.table) or !std.mem.eql(u8, cfg.key_template, request.key_template)) {
        state.proposals = &.{};
        state.receipt_scope = null;
        next.namespace_curation_generation = 0;
        next.namespace_curation_table_id = 0;
        next.namespace_curation_missing = false;
    }
    var suppression = if (cfg.suppress_json.len > 0) try std.json.parseFromSliceLeaky(resolver.curation.Suppression, a, cfg.suppress_json, .{}) else resolver.curation.Suppression{};
    if (request.reset_merges and !request.reset_curation and (state.distinct.len > 0 or state.removed_aliases.len > 0 or state.suppress.keys.len > 0 or suppression.keys.len > 0)) return error.InvalidResolverCurationRequest;
    if (request.reset_merges) {
        next.aliases = request.aliases orelse &.{};
        next.merges = &.{};
        state.distinct = &.{};
        state.removed_aliases = &.{};
        state.suppress.keys = &.{};
        state.suppress.entries = &.{};
        state.redirect_origins = &.{};
        suppression.keys = &.{};
    } else {
        next.aliases = try Mapper.aliases(a, mapping, cfg.aliases);
        next.merges = try Mapper.aliases(a, mapping, cfg.merges);
        const pairs = try a.alloc(resolver.curation.Pair, state.distinct.len);
        for (state.distinct, pairs) |old, *pair| pair.* = .{ .from = try Mapper.key(mapping, old.from), .to = try Mapper.key(mapping, old.to), .origin = old.origin };
        state.distinct = pairs;
        state.removed_aliases = try Mapper.keys(a, mapping, state.removed_aliases);
        state.suppress.keys = try Mapper.keys(a, mapping, state.suppress.keys);
        const entries = try a.dupe(resolver.curation.Entry, state.suppress.entries);
        for (entries) |*entry| {
            entry.key = try Mapper.key(mapping, entry.key);
            if (entry.origin.cascade_from) |from| entry.origin.cascade_from = try Mapper.key(mapping, from);
        }
        state.suppress.entries = entries;
        const origins = try a.dupe(resolver.curation.RedirectOrigin, state.redirect_origins);
        for (origins) |*entry| {
            entry.key = try Mapper.key(mapping, entry.key);
            entry.target = try Mapper.key(mapping, entry.target);
        }
        state.redirect_origins = origins;
        suppression.keys = try Mapper.keys(a, mapping, suppression.keys);
    }
    if (cfg.suppress_json.len > 0) next.suppress_json = try std.json.Stringify.valueAlloc(a, suppression, .{});
    if (cfg.curation_json.len > 0 or cfg.curation_revision > 0) {
        next.curation_json = try std.json.Stringify.valueAlloc(a, state, .{});
        next.curation_revision = try std.math.add(u64, cfg.curation_revision, 1);
    }
    next.merge_generation = if (next.merges.len > 0) try std.math.add(u64, cfg.effectiveGeneration(), 1) else 0;
    next.merge_state_version = if (next.merges.len > 0) 1 else 0;
    next.namespace_generation = try std.math.add(u64, cfg.namespace_generation, 1);
    try next.validate();
    return next;
}

pub fn planNamespaceMetadata(a: std.mem.Allocator, indexes_json: []const u8, name: []const u8, request: NamespaceRequest) !MetadataPlan {
    var root = try std.json.parseFromSliceLeaky(std.json.Value, a, indexes_json, .{});
    const metadata = try metadataFromValue(a, root);
    var desired: std.ArrayListUnmanaged(catalog.ResolverConfig) = .empty;
    try @import("../../metadata/index_reconcile.zig").collectDesiredResolvers(a, root, &desired);
    var selected: ?catalog.ResolverConfig = null;
    for (desired.items) |cfg| if (std.mem.eql(u8, cfg.name, name)) {
        selected = cfg;
        break;
    };
    for (metadata.identities) |identity| if (std.mem.eql(u8, identity.name, name)) {
        selected = try overlayMetadata(a, selected orelse catalog.ResolverConfig{ .name = identity.name, .table = identity.table, .key_template = identity.key_template, .source_artifact = "retired_source", .resolution_artifact = "retired_resolution", .retired = true, .promote = false }, identity);
        break;
    };
    const cfg = selected orelse return error.ResolverNotFound;
    var next = try namespaceConfig(a, cfg, request);
    // Every namespace operation creates a durable identity fence, including
    // namespaces that have never had redirects or suppression.
    if (next.curation_revision == 0) next.curation_revision = 1;
    const Update = struct {
        fn walk(allocator: std.mem.Allocator, value: *std.json.Value, definition: catalog.ResolverConfig) !void {
            switch (value.*) {
                .object => |*object| {
                    if (object.getPtr("resolvers")) |items| if (items.* == .array) {
                        for (items.array.items) |*item| {
                            if (item.* != .object) continue;
                            const id = item.object.get("name") orelse continue;
                            if (id != .string or !std.mem.eql(u8, id.string, definition.name)) continue;
                            try item.object.put(allocator, "table", .{ .string = definition.table });
                            try item.object.put(allocator, "key_template", .{ .string = definition.key_template });
                            try item.object.put(allocator, "aliases", try json(allocator, definition.aliases));
                            if (definition.suppress_json.len > 0) try item.object.put(allocator, "suppress", try std.json.parseFromSliceLeaky(std.json.Value, allocator, definition.suppress_json, .{}));
                        }
                    };
                    var it = object.iterator();
                    while (it.next()) |entry| if (!index_metadata.isOpaqueEntry(entry.key_ptr.*) and !std.mem.eql(u8, entry.key_ptr.*, "resolvers")) try walk(allocator, entry.value_ptr, definition);
                },
                .array => |*items| for (items.items) |*item| try walk(allocator, item, definition),
                else => {},
            }
        }
    };
    try Update.walk(a, &root, next);
    try replaceMetadata(a, &root, .{ .name = name, .table = next.table, .key_template = next.key_template, .merges = next.merges, .state = try resolver.curation.parseState(a, next.curation_json), .revision = next.curation_revision, .namespace_generation = next.namespace_generation });
    return .{ .indexes_json = try std.json.Stringify.valueAlloc(a, root, .{}), .changed = true, .revision = next.curation_revision, .config_generation = next.config_generation };
}

pub const ProgressRow = struct {
    name: []const u8,
    config_generation: u64,
    effective_generation: u64,
    curation_revision: u64,
    namespace_curation_generation: u64 = 0,
    namespace_curation_table_id: u64 = 0,
    namespace_curation_missing: bool = false,
    backfill_pending: bool,
    empty: bool = false,
    resolution: @import("types.zig").ReplayStageStats,
    promotion: @import("types.zig").ReplayStageStats,
};
pub const ProgressReport = struct { resolvers: []const ProgressRow };

/// One compact owner observation for every resolver in a shard. This does not
/// drain work: the owner’s ordinary reconciliation and detached workers retain
/// sole responsibility for execution and publication.
pub fn progressReportAlloc(db: anytype, alloc: std.mem.Allocator) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const configs = try db.listResolvers(a);
    const stats = db.pendingWorkStats();
    const backlog = try @import("resolution_runtime.zig").hasDurableReresolveBacklog(db.core.store);
    const empty = try emptyReplayOwner(db, a, stats);
    const rows = try a.alloc(ProgressRow, configs.len);
    for (configs, rows) |cfg, *row| row.* = .{
        .name = cfg.name,
        .config_generation = cfg.config_generation,
        .effective_generation = cfg.effectiveGeneration(),
        .curation_revision = cfg.curation_revision,
        .namespace_curation_generation = cfg.namespace_curation_generation,
        .namespace_curation_table_id = cfg.namespace_curation_table_id,
        .namespace_curation_missing = cfg.namespace_curation_missing,
        .backfill_pending = backlog,
        .empty = empty,
        .resolution = stats.resolution,
        .promotion = stats.promotion,
    };
    return std.json.Stringify.valueAlloc(alloc, ProgressReport{ .resolvers = rows }, .{});
}

pub const ReviewRequest = struct { actor: []const u8 = "", reason: []const u8 = "", command: ?@import("resolver_review.zig").Command = null, expected_revision: ?u64 = null, wait: ?resolver.curation.Wait = null, operation: enum { reviews, review, prepare_review, review_status, progress }, resolver: []const u8 = "", cursor: ?[]const u8 = null, limit: u16 = 64, id: ?[]const u8 = null, decision: ?resolver.Decision = null, table: ?[]const u8 = null, key: ?[]const u8 = null };
const ReviewToken = struct { doc_key: []const u8, local_id: []const u8, hash: []const u8 };
const ReviewCursor = struct { after: ?[]const u8 = null, row: ?[]const u8 = null, offset: usize = 0 };
pub fn encodeToken(a: std.mem.Allocator, value: anytype) ![]const u8 {
    const raw = try std.json.Stringify.valueAlloc(a, value, .{});
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const out = try a.alloc(u8, encoder.calcSize(raw.len));
    return encoder.encode(out, raw);
}
pub fn decodeToken(comptime T: type, a: std.mem.Allocator, encoded: []const u8) !T {
    if (encoded.len > 65536) return error.InvalidResolverCurationRequest;
    const decoder = std.base64.url_safe_no_pad.Decoder;
    const raw = try a.alloc(u8, try decoder.calcSizeForSlice(encoded));
    try decoder.decode(raw, encoded);
    return std.json.parseFromSliceLeaky(T, a, raw, .{});
}

/// Bounded keyset pagination. Each call visits at most 256 storage keys and
/// returns at most 256 mentions; opaque IDs bind decisions to artifact bytes.
pub fn reviewOperation(db: anytype, alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const input = try std.json.parseFromSliceLeaky(ReviewRequest, a, body, .{});
    if (input.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
    if (input.limit == 0 or input.limit > 256) return error.InvalidResolverCurationRequest;
    // Progress is table metadata, with no row/artifact contents. It must remain
    // observable even when raw review access is denied by row security.
    if (input.operation == .progress) return progressReportAlloc(db, alloc);
    var policy_lease = if (input.operation == .reviews) try db.local_execution.row_policy_gate.enterRawRead() else try db.local_execution.row_policy_gate.enterRaw();
    defer policy_lease.release();
    if (input.operation == .review_status) {
        const command = input.command orelse return error.InvalidResolverCurationRequest;
        try command.validate();
        const result = (try @import("resolver_review.zig").receipt(a, db.core.store, command)) orelse return error.StorageUnavailable;
        if (!result.accepted) return error.ResolverCurationConflict;
        return std.json.Stringify.valueAlloc(alloc, .{ .resolver = command.resolver, .changed = true, .revision = command.curation_revision, .replay_sequence = result.sequence, .replay_sequence_kind = "source_journal", .applied = false }, .{});
    }
    const all = try db.listResolvers(a);
    var selected: ?catalog.ResolverConfig = null;
    for (all) |cfg| if (std.mem.eql(u8, cfg.name, input.resolver)) {
        selected = cfg;
        break;
    };
    const cfg = selected orelse return error.ResolverNotFound;
    if (input.operation == .review or input.operation == .prepare_review) {
        const token = try decodeToken(ReviewToken, a, input.id orelse return error.InvalidResolverCurationRequest);
        const decision = input.decision orelse return error.InvalidResolverCurationRequest;
        if (decision == .review) return error.InvalidResolverCurationRequest;
        const original = if (decision == .suppressed) "" else try reviewOriginalKey(db, a, cfg, token);
        const key = input.key orelse if (decision == .suppressed) "" else if (decision == .new) original else return error.InvalidResolverCurationRequest;
        const table = input.table orelse cfg.table;
        if (!std.mem.eql(u8, table, cfg.table)) return error.InvalidResolverCurationRequest;
        var policy = try resolver.curation.Index.init(a, cfg.suppress_json, cfg.curation_json);
        const aliases = try resolver.curation.RedirectIndex.init(a, try cfg.effectiveAliasesAlloc(a));
        try policy.bindRedirects(a, aliases);
        if (decision != .suppressed) {
            try resolver.curation.validKey(key);
            if (policy.keySuppressed(key) or policy.keySuppressed(aliases.canonical(key))) return error.SuppressedResolverRedirectTarget;
            if (try policy.pinnedDistinct(a, original, key)) return error.ResolverDistinctPair;
        }
        var namespace: [24]u8 = undefined;
        @import("doc_identity.zig").encodeNamespace(&namespace, db.core.identity_namespace);
        const review = @import("resolver_review.zig");
        var origin = manualOrigin(.{ .action = .suppress, .actor = input.actor, .reason = input.reason }, cfg.curation_revision);
        origin.origin = .review;
        const command = review.Command{ .origin = origin, .namespace = namespace, .policy_digest = try review.policyDigest(a, db.core.store), .override_digest = try review.overrideDigest(a, db.core.store, token.doc_key, cfg.source_artifact, cfg.resolution_artifact), .resolver = cfg.name, .document = token.doc_key, .source_artifact = cfg.source_artifact, .resolution_artifact = cfg.resolution_artifact, .local_id = token.local_id, .expected_hash = token.hash, .config_generation = cfg.effectiveGeneration(), .curation_revision = input.expected_revision orelse cfg.curation_revision, .decision = decision, .table = table, .key = key };
        if (input.operation == .prepare_review) {
            const prepared = try review.plan(a, db.core, command, true);
            if (!prepared.accepted) return error.ResolverCurationConflict;
            return std.json.Stringify.valueAlloc(alloc, command, .{});
        }
        if (try db.maintenanceRequiresOrderedApply()) return error.OrderedResolverReviewRequired;
        // Native/Lite and HA use the same primary batch and full mutation
        // replication contract; no replay-only side channel owns an override.
        // Batch admission acquires its own policy lease. Do not retain this
        // read lease while it finalizes a concurrent policy publication.
        policy_lease.release();
        try db.batch(.{ .resolver_review = command, .sync_level = .write });
        const outcome = (try review.receipt(a, db.core.store, command)) orelse return error.StorageUnavailable;
        if (!outcome.accepted) return error.ResolverCurationConflict;
        return std.json.Stringify.valueAlloc(alloc, .{ .resolver = cfg.name, .changed = true, .revision = cfg.curation_revision, .replay_sequence = outcome.sequence, .replay_sequence_kind = "source_journal", .applied = false }, .{});
    }
    const cursor = if (input.cursor) |value| try decodeToken(ReviewCursor, a, value) else ReviewCursor{};
    const keys = @import("../internal_keys.zig");
    const prefix = [_]u8{keys.user_namespace};
    const rows = try db.core.store.scanPrefixPage(a, &prefix, cursor.after, 256);
    var output = std.json.Array.init(a);
    var next: ?[]const u8 = null;
    var previous = cursor.after;
    for (rows) |row| {
        if (!keys.isResolutionArtifactKey(row.key)) {
            previous = row.key;
            continue;
        }
        const parsed_key = (try keys.parseResolutionArtifactKeyAlloc(a, row.key)) orelse {
            previous = row.key;
            continue;
        };
        if (!std.mem.eql(u8, parsed_key.artifact_name, cfg.resolution_artifact)) {
            previous = row.key;
            continue;
        }
        var parsed = try resolver.parseResolution(a, row.value);
        defer parsed.deinit();
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(row.value, &digest, .{});
        const hash = std.fmt.bytesToHex(digest, .lower);
        const start = if (cursor.row) |resume_key| if (std.mem.eql(u8, resume_key, row.key)) cursor.offset else 0 else 0;
        for (parsed.entities, 0..) |entity, i| {
            if (i < start or entity.decision != .review) continue;
            if (output.items.len == input.limit) {
                next = try encodeToken(a, ReviewCursor{ .after = previous, .row = row.key, .offset = i });
                break;
            }
            const id = try encodeToken(a, ReviewToken{ .doc_key = parsed_key.doc_key, .local_id = entity.local_id, .hash = &hash });
            try output.append(try json(a, .{ .id = id, .source_key = parsed_key.doc_key, .local_id = entity.local_id, .label = entity.label, .surface_form = entity.surface_form, .key = entity.doc_ref.key, .table = entity.doc_ref.table, .confidence = entity.confidence, .adjudication = entity.adjudication, .resolver = cfg.name, .config_generation = parsed.config_generation }));
        }
        if (next != null) break;
        previous = row.key;
    }
    if (next == null and rows.len == 256) next = try encodeToken(a, ReviewCursor{ .after = previous });
    try policy_lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
    return std.json.Stringify.valueAlloc(alloc, .{ .reviews = std.json.Value{ .array = output }, .next_cursor = next }, .{});
}

fn reviewOriginalKey(db: anytype, a: std.mem.Allocator, cfg: catalog.ResolverConfig, token: ReviewToken) ![]const u8 {
    const artifact = try @import("../internal_keys.zig").resolutionArtifactKeyAlloc(a, token.doc_key, cfg.resolution_artifact);
    const raw = db.core.store.get(a, artifact) catch |err| switch (err) {
        error.NotFound => return error.ResolverCurationConflict,
        else => return err,
    };
    var current = try resolver.parseResolution(a, raw);
    defer current.deinit();
    var policy = try resolver.Resolver.initFromParts(a, cfg.table, cfg.key_template, .{}, cfg.type_must_match, "");
    defer policy.deinit();
    for (current.entities) |entity| if (std.mem.eql(u8, entity.local_id, token.local_id)) return policy.renderKeyAlloc(a, .{ .text = entity.surface_form, .label = entity.label, .local_id = entity.local_id });
    return error.ResolverCurationConflict;
}

pub const keyInNamespace = @import("resolver_namespace.zig").keyInNamespace;

pub const drop_marker_prefix = catalog.drop_marker_prefix;

pub fn planDropMetadata(a: std.mem.Allocator, indexes_json: []const u8, name: []const u8) !MetadataPlan {
    var root = try std.json.parseFromSliceLeaky(std.json.Value, a, indexes_json, .{});
    var desired: std.ArrayListUnmanaged(catalog.ResolverConfig) = .empty;
    try @import("../../metadata/index_reconcile.zig").collectDesiredResolvers(a, root, &desired);
    for (desired.items) |cfg| if (std.mem.eql(u8, cfg.name, name)) return error.ResolverIdentityActive;
    const metadata = try metadataFromValue(a, root);
    for (metadata.identities) |identity| {
        if (!std.mem.eql(u8, identity.name, name)) continue;
        if (identity.dropped) return .{ .indexes_json = indexes_json, .changed = false, .revision = identity.revision, .config_generation = 0 };
        var tombstone = identity;
        tombstone.dropped = true;
        tombstone.revision = try std.math.add(u64, identity.revision, 1);
        tombstone.state = .{};
        tombstone.merges = &.{};
        try replaceMetadata(a, &root, tombstone);
        return .{ .indexes_json = try std.json.Stringify.valueAlloc(a, root, .{}), .changed = true, .revision = tombstone.revision, .config_generation = 0 };
    }
    return error.ResolverNotFound;
}

/// The durable drop marker is written before reactivation is allowed. A crash
/// between catalog deletion and marker publication simply repeats an empty drop.
pub fn consumeMetadataDrop(db: anytype, alloc: std.mem.Allocator, identity: MetadataIdentity) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const marker = try std.fmt.allocPrint(a, "{s}{s}", .{ drop_marker_prefix, identity.name });
    const old: ?[]u8 = db.core.store.get(a, marker) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (old) |bytes| if ((std.fmt.parseInt(u64, bytes, 10) catch 0) >= identity.revision) return;
    _ = try db.removeResolverWithoutDrain(identity.name);
    _ = try db.dropRetiredResolverIdentity(identity.name);
    const version = @max(db.core.index_manager.minimum_reader_version.load(.acquire), catalog.curation_reader_version);
    try db.core.store.putBatch(&.{ .{ .key = marker, .value = try std.fmt.allocPrint(a, "{d}", .{identity.revision}) }, .{ .key = catalog.minimum_reader_key, .value = try catalog.readerVersionBytes(version) } }, &.{});
    db.core.index_manager.minimum_reader_version.store(version, .release);
}

test "resolver curation reviews bind decisions to artifact revisions and paginate mentions" {
    const DB = @import("selected_root.zig").db.DB;
    const keys = @import("../internal_keys.zig");
    const a = std.testing.allocator;
    var tmp = try @import("../../common/test_directory.zig").TestDirectory.init("resolver-review-page");
    defer tmp.cleanup();
    var db = try DB.open(a, tmp.path(), .{});
    defer db.close();
    try db.addResolver(.{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{uslug _entity.text}}", .promote = false, .config_generation = 1 });
    const artifact = try keys.resolutionArtifactKeyAlloc(a, "source", "resolved");
    defer a.free(artifact);
    const resolution =
        \\{"config_generation":1,"entities":[{"local_id":"a","doc_ref":{"table":"entities","key":"concept/alpha"},"confidence":0.7,"decision":"review","label":"concept","canonical_name":"Alpha","surface_form":"Alpha"},{"local_id":"b","doc_ref":{"table":"entities","key":"concept/beta"},"confidence":0.7,"decision":"review","label":"concept","canonical_name":"Beta","surface_form":"Beta"}]}
    ;
    try db.core.store.put(artifact, resolution);
    const page = try reviewOperation(&db, a, "{\"operation\":\"reviews\",\"resolver\":\"concepts\",\"limit\":1}");
    defer a.free(page);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, page, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.object.get("reviews").?.array.items.len);
    const id = parsed.value.object.get("reviews").?.array.items[0].object.get("id").?.string;
    const second_request = try std.json.Stringify.valueAlloc(a, ReviewRequest{ .operation = .reviews, .resolver = "concepts", .limit = 1, .cursor = parsed.value.object.get("next_cursor").?.string }, .{});
    defer a.free(second_request);
    const second = try reviewOperation(&db, a, second_request);
    defer a.free(second);
    try std.testing.expect(std.mem.indexOf(u8, second, "\"local_id\":\"b\"") != null);
    const decision = try std.json.Stringify.valueAlloc(a, ReviewRequest{ .operation = .review, .resolver = "concepts", .id = id, .decision = .suppressed }, .{});
    defer a.free(decision);
    const accepted = try reviewOperation(&db, a, decision);
    defer a.free(accepted);
    try db.core.store.put(artifact, "{\"config_generation\":2,\"entities\":[]}");
    try std.testing.expectError(error.ResolverCurationConflict, reviewOperation(&db, a, decision));
    try db.local_execution.row_policy_gate.beginPreparing(.disabled);
    try std.testing.expectError(error.RowPolicyAuthenticationRequired, reviewOperation(&db, a, "{\"operation\":\"reviews\",\"resolver\":\"concepts\"}"));
    const progress = try reviewOperation(&db, a, "{\"operation\":\"progress\"}");
    defer a.free(progress);
    const report = try std.json.parseFromSlice(ProgressReport, a, progress, .{});
    defer report.deinit();
    try std.testing.expectEqual(@as(usize, 1), report.value.resolvers.len);
    try std.testing.expectEqualStrings("concepts", report.value.resolvers[0].name);
}

test "resolver curation namespace maps negative policy and preserves text suppression" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cfg: catalog.ResolverConfig = .{ .name = "concepts", .table = "old", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "old/{{uslug _entity.text}}", .curation_json = "{\"version\":1,\"suppress\":{\"keys\":[\"old/noise\"],\"texts\":[\"Note\"]},\"distinct\":[{\"from\":\"old/a\",\"to\":\"old/b\"}],\"removed_aliases\":[\"old/a\"]}", .curation_revision = 2 };
    try std.testing.expectError(error.InvalidResolverCurationRequest, namespaceConfig(a, cfg, .{ .table = "new", .key_template = "new/{{uslug _entity.text}}", .reset_merges = true }));
    const reset = try namespaceConfig(a, cfg, .{ .table = "new", .key_template = "new/{{uslug _entity.text}}", .reset_merges = true, .reset_curation = true });
    const state = try resolver.curation.parseState(a, reset.curation_json);
    try std.testing.expectEqual(@as(usize, 0), state.suppress.keys.len);
    try std.testing.expectEqualStrings("Note", state.suppress.texts[0]);
    const mapping = [_]resolver.KeyAlias{ .{ .key = "old/a", .target = "new/a" }, .{ .key = "old/b", .target = "new/b" }, .{ .key = "old/noise", .target = "new/noise" } };
    const mapped = try namespaceConfig(a, cfg, .{ .table = "new", .key_template = "new/{{uslug _entity.text}}", .key_map = &mapping });
    const next = try resolver.curation.parseState(a, mapped.curation_json);
    try std.testing.expectEqualStrings("new/noise", next.suppress.keys[0]);
    try std.testing.expectEqualStrings("new/b", next.distinct[0].to);
    try std.testing.expectEqual(@as(u64, 3), mapped.curation_revision);
}

/// Before removing/replacing an index, retain its last resolver namespace in
/// table-owned metadata. An uncurated identity must remain discoverable after
/// its last declaring index disappears too.
pub fn retainRetiredMetadata(alloc: std.mem.Allocator, before: []const u8, after: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const prior = try std.json.parseFromSliceLeaky(std.json.Value, a, before, .{});
    var next = try std.json.parseFromSliceLeaky(std.json.Value, a, after, .{});
    var old: std.ArrayListUnmanaged(catalog.ResolverConfig) = .empty;
    var desired: std.ArrayListUnmanaged(catalog.ResolverConfig) = .empty;
    const reconcile = @import("../../metadata/index_reconcile.zig");
    try reconcile.collectDesiredResolvers(a, prior, &old);
    try reconcile.collectDesiredResolvers(a, next, &desired);
    inline for (.{ "_resolver_record_store", "_resolver_record_owners" }) |key| if (prior.object.get(key)) |value| try next.object.put(a, key, value);
    if (prior.object.get(namespaces_key)) |value| try next.object.put(a, namespaces_key, value);
    if (prior.object.get("_curation_proposals")) |value| try next.object.put(a, "_curation_proposals", value);
    const metadata = try metadataFromValue(a, next);
    var retained: std.StringHashMapUnmanaged(void) = .empty;
    for (metadata.identities) |identity| try retained.put(a, identity.name, {});
    for (desired.items) |cfg| try retained.put(a, cfg.name, {});
    for (old.items) |cfg| {
        if (retained.contains(cfg.name)) continue;
        try replaceMetadata(a, &next, .{ .name = cfg.name, .table = cfg.table, .key_template = cfg.key_template, .merges = cfg.merges, .state = try resolver.curation.parseState(a, cfg.curation_json), .revision = @max(1, cfg.curation_revision), .namespace_generation = cfg.namespace_generation });
        try retained.put(a, cfg.name, {});
    }
    return std.json.Stringify.valueAlloc(alloc, next, .{});
}

pub fn stageApplied(value: @import("types.zig").ReplayStageStats, empty: bool) bool {
    if (value.blocked or value.catch_up_required) return false;
    if (!value.enabled) return empty and value.target_sequence == 0 and value.applied_sequence == 0;
    return value.applied_sequence >= value.target_sequence;
}

fn emptyReplayOwner(db: anytype, a: std.mem.Allocator, stats: anytype) !bool {
    if (stats.derived_target_sequence != 0) return false;
    const count = try @import("range_cardinality.zig").loadOrProveEmpty(a, db.core.store);
    return count != null and count.? == 0;
}

pub fn applied(db: anytype, cfg: anytype) !bool {
    if (cfg.retired) return true;
    if (try @import("resolution_runtime.zig").hasDurableReresolveBacklog(db.core.store)) return false;
    const stats = db.pendingWorkStats();
    const empty = try emptyReplayOwner(db, db.alloc, stats);
    return stageApplied(stats.resolution, empty) and (!cfg.promote or stageApplied(stats.promotion, empty));
}

test "resolver curation durable drop fence cannot delete a reactivated fresh identity" {
    const DB = @import("selected_root.zig").db.DB;
    const a = std.testing.allocator;
    var tmp = try @import("../../common/test_directory.zig").TestDirectory.init("resolver-drop-fence");
    defer tmp.cleanup();
    var db = try DB.open(a, tmp.path(), .{});
    defer db.close();
    const cfg: catalog.ResolverConfig = .{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{uslug _entity.text}}", .promote = false };
    try db.addResolver(cfg);
    const identity: MetadataIdentity = .{ .name = cfg.name, .table = cfg.table, .key_template = cfg.key_template, .revision = 3, .dropped = true };
    try consumeMetadataDrop(&db, a, identity);
    try db.addResolver(cfg);
    try consumeMetadataDrop(&db, a, identity);
    const retained = try db.listResolverIdentities(a);
    defer {
        for (retained) |*item| item.deinit(a);
        a.free(retained);
    }
    try std.testing.expectEqual(@as(usize, 1), retained.len);
    try std.testing.expectEqual(catalog.curation_reader_version, db.core.index_manager.minimum_reader_version.load(.acquire));
}

test "resolver curation ordered review commits outcomes replay and receipts together" {
    const DB = @import("selected_root.zig").db.DB;
    const review = @import("resolver_review.zig");
    const keys = @import("../internal_keys.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = try @import("../../common/test_directory.zig").TestDirectory.init("ordered-resolver-review");
    defer tmp.cleanup();
    // Keep this shared contract free of literal physical-DB imports, even
    // inside tests: Zig records those files in control consumers' cache keys.
    const options: @import("selected_root.zig").db.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .start_index_workers = false, .start_optional_runtimes = false };
    var db = try DB.open(std.testing.allocator, tmp.path(), options);
    var closed = false;
    defer if (!closed) db.close();
    try db.addResolver(.{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{uslug _entity.text}}", .promote = false, .config_generation = 1 });
    const artifact = try keys.resolutionArtifactKeyAlloc(a, "source", "resolved");
    const raw = "{\"config_generation\":1,\"entities\":[{\"local_id\":\"a\",\"doc_ref\":{\"table\":\"entities\",\"key\":\"concept/alpha\"},\"confidence\":0.7,\"decision\":\"review\",\"label\":\"concept\",\"surface_form\":\"Alpha\"}]}";
    try db.core.store.put(artifact, raw);
    var namespace: [24]u8 = undefined;
    @import("doc_identity.zig").encodeNamespace(&namespace, db.core.identity_namespace);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &hash, .{});
    const command: review.Command = .{ .namespace = namespace, .resolver = "concepts", .document = "source", .source_artifact = "kg", .resolution_artifact = "resolved", .local_id = "a", .expected_hash = &std.fmt.bytesToHex(hash, .lower), .config_generation = 1, .curation_revision = 0, .decision = .suppressed, .table = "entities", .key = "" };
    // Unit-scoped decisions re-drive the original extraction unit, rather
    // than inventing another asset nested beneath its internal key.
    const unit = try keys.documentUnitArtifactKeyAlloc(a, "source", "kg", "unit:1");
    try db.core.store.put(try keys.resolutionArtifactKeyAlloc(a, unit, "resolved"), raw);
    var unit_command = command;
    unit_command.document = unit;
    const unit_plan = try review.plan(a, db.core, unit_command, true);
    try std.testing.expect(unit_plan.accepted);
    try std.testing.expectEqualStrings(unit, unit_plan.source_key.?);
    try std.testing.expectEqualStrings("source", try unit_command.ownerDocumentAlloc(a));
    try std.testing.expectEqualStrings("source", try @import("artifact/publication_owner.zig").documentAlloc(a, try unit_command.receiptKey(a)));
    const replication = @import("replication_effects.zig");
    for ([_]bool{ false, true }) |ordered| {
        const payload = if (ordered) try replication.encodeRaftBatchMutationRequestAlloc(a, .{ .resolver_review = command }, .{ .term = 1, .index = 1 }) else try replication.encodeBatchMutationRequestAlloc(a, .{ .resolver_review = command });
        const record: @import("replication_record.zig").RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = payload };
        var parsed = try replication.decodeBatchMutationRequest(std.testing.allocator, record);
        defer parsed.deinit();
        try std.testing.expectEqual(@as(u32, if (ordered) 23 else 22), parsed.value.schema_version);
        try std.testing.expectEqual(command.config_generation, parsed.value.request.resolver_review.?.config_generation);
        try std.testing.expect(try replication.decodeRestoreFinishForReplay(std.testing.allocator, record) == null);
    }
    const codec = @import("../../api/batch.zig");
    const encoded = try codec.encodeBatchRequest(a, .{ .resolver_review = command });
    try std.testing.expectError(error.InvalidBatchRequest, codec.parseBatchRequest(std.testing.allocator, encoded));
    var decoded = try codec.parseInternalBatchRequest(std.testing.allocator, encoded);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqual(command.config_generation, decoded.req.resolver_review.?.config_generation);
    try db.applyOrderedCommittedMutation(.{ .resolver_review = command }, .{ .term = 1, .index = 1 }, false);
    const accepted = (try review.receipt(a, db.core.store, command)).?;
    try std.testing.expect(accepted.accepted);
    try std.testing.expect(accepted.sequence > 0);
    // A curator decision requires the curation format, not every feature the
    // current binary understands (grouped sources and namespaces are separate).
    try std.testing.expectEqual(catalog.curation_reader_version, db.core.index_manager.minimum_reader_version.load(.acquire));
    const override_key = try @import("resolution_runtime.zig").reviewOverrideArtifactKeyAlloc(a, "source", "kg", "resolved");
    const override = try db.core.store.get(a, override_key);
    try std.testing.expect(std.mem.indexOf(u8, override, "suppressed") != null);
    try std.testing.expectEqual(@as(u64, 1), (try db.orderedApplyReceipt()).?.index);
    const sequence = db.core.store.lastReplaySequence(0);
    try db.applyOrderedCommittedMutation(.{ .resolver_review = command }, .{ .term = 1, .index = 1 }, false);
    try std.testing.expectEqual(sequence, db.core.store.lastReplaySequence(0));
    // Re-admitting an accepted decision at a later Raft position must preserve
    // a higher floor installed by another feature after the original receipt.
    try db.core.store.put(catalog.minimum_reader_key, try catalog.readerVersionBytes(catalog.namespace_curation_reader_version));
    db.core.index_manager.minimum_reader_version.store(catalog.namespace_curation_reader_version, .release);
    try db.applyOrderedCommittedMutation(.{ .resolver_review = command }, .{ .term = 1, .index = 2 }, false);
    try std.testing.expectEqual(catalog.namespace_curation_reader_version, db.core.index_manager.minimum_reader_version.load(.acquire));
    // A stale command records rejection and advances Raft even though its
    // override cannot be applied. No failed CAS can wedge follower replay.
    var stale = command;
    stale.curation_revision = 1;
    try db.applyOrderedCommittedMutation(.{ .resolver_review = stale }, .{ .term = 1, .index = 3 }, false);
    try std.testing.expect(!(try review.receipt(a, db.core.store, stale)).?.accepted);
    try std.testing.expectEqualStrings(override, try db.core.store.get(a, override_key));
    try std.testing.expectEqual(@as(u64, 3), (try db.orderedApplyReceipt()).?.index);
    db.close();
    closed = true;
    var reopened = try DB.open(std.testing.allocator, tmp.path(), options);
    defer reopened.close();
    try std.testing.expect(!(try review.receipt(a, reopened.core.store, stale)).?.accepted);
    try std.testing.expectEqualStrings(override, try reopened.core.store.get(a, override_key));
    try std.testing.expectEqual(@as(u64, 3), (try reopened.orderedApplyReceipt()).?.index);
    try std.testing.expectEqual(catalog.namespace_curation_reader_version, reopened.core.index_manager.minimum_reader_version.load(.acquire));
}

test "resolver curation namespace migration folds identical state and rejects conflicts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cfg: catalog.ResolverConfig = .{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{uslug _entity.text}}", .merges = &.{.{ .key = "concept/a", .target = "concept/b" }}, .curation_json = "{\"suppress\":{\"keys\":[\"concept/noise\"]}}", .curation_revision = 3 };
    const first = try foldNamespace(a, .{ .key_template = cfg.key_template }, cfg);
    const identical = try foldNamespace(a, first, cfg);
    try std.testing.expectEqual(@as(usize, 1), identical.merges.len);
    try std.testing.expectEqual(@as(usize, 1), identical.state.suppress.keys.len);
    try std.testing.expectEqual(@as(u64, 3), identical.revision);
    var conflicting = cfg;
    conflicting.merges = &.{.{ .key = "concept/a", .target = "concept/c" }};
    try std.testing.expectError(error.ResolverNamespaceMigrationConflict, foldNamespace(a, first, conflicting));
}

test "resolver curation proposal evidence cannot declare indexes or resolver identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expect(@import("../../api/indexes.zig").isReservedIndexMetadataEntry("_curation_proposals"));
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \\{"_curation_proposals":{"proposals":[{"evidence":{"resolvers":[{"malformed":true}]}}]}}
    , .{});
    var out: std.ArrayListUnmanaged(catalog.ResolverConfig) = .empty;
    try @import("../../metadata/index_reconcile.zig").collectDesiredResolvers(a, root, &out);
    try std.testing.expectEqual(@as(usize, 0), out.items.len);
}

test "resolver curation namespace changes cannot carry equal proposal receipts into another table" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const proposed = try resolver.curation.propose(a, .{}, &.{}, .{ .action = .suppress, .key = "concept/noise" }, .{});
    const accepted = try resolver.curation.decide(a, proposed.state, &.{}, &.{}, proposed.state.proposals[0].id, .{ .accept = true }, .{});
    const cfg: catalog.ResolverConfig = .{ .name = "concepts", .table = "old", .key_template = "concept/{{uslug _entity.text}}", .source_artifact = "kg", .resolution_artifact = "resolved", .curation_json = try std.json.Stringify.valueAlloc(a, accepted.state, .{}), .curation_revision = 1 };
    const moved = try namespaceConfig(a, cfg, .{ .table = "new", .key_template = cfg.key_template, .reset_merges = true, .reset_curation = true });
    const next = try resolver.curation.parseState(a, moved.curation_json);
    try std.testing.expectEqual(@as(usize, 0), next.proposals.len);
    const same = try namespaceConfig(a, cfg, .{ .table = "old", .key_template = cfg.key_template, .reset_merges = true, .reset_curation = true });
    try std.testing.expectEqual(@as(usize, 1), (try resolver.curation.parseState(a, same.curation_json)).proposals.len);
}

test "resolver curation ownership index borrows opaque proposal evidence" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const evidence = try a.alloc(u8, 8192);
    @memset(evidence, 'x');
    const bytes = try std.json.Stringify.valueAlloc(a, .{
        ._resolver_namespaces = .{ .namespaces = .{.{ .key_template = "other/{{uslug _entity.text}}" }} },
        ._curation_proposals = .{ .proposals = .{.{ .key = "concept/x", .evidence = .{ .data = evidence } }} },
    }, .{});
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{});
    var memory: [1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&memory);
    const index = try Ownership.init(fixed.allocator(), root);
    try std.testing.expect(index.owns("concept/{{uslug _entity.text}}"));
    try std.testing.expect(index.owns("other/{{uslug _entity.text}}"));
    try std.testing.expect(!index.owns("person/{{uslug _entity.text}}"));
}
