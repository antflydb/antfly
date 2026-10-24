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

//! Ordered immutable chunk staging. Appends and retirement change private
//! rows only; selection publishes a head and inventory with the normal
//! producer receipt, source guards, provenance and replay transaction.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const generations = @import("artifact_chunk_generation.zig");
const chunks = @import("artifact_chunk_manifest.zig");
const catalog = @import("catalog/enrichment_catalog.zig");
const inventory = @import("artifact_inventory.zig");
const keys = @import("../internal_keys.zig");
const ids = @import("artifact_ids.zig");
const text = @import("artifact_asset_publication.zig");
const Allocator = std.mem.Allocator;

fn configFor(a: Allocator, command: publication.Command, catalogs: inventory.Catalogs) !struct { config: catalog.EnrichmentConfig, configs: []const catalog.EnrichmentConfig } {
    if (command.sources.len != 1 or command.producer_kind != .enrichment or command.producer_scope_key.len != 0 or
        command.producer_generation != command.authority_epoch or !std.mem.eql(u8, command.producer_name, command.producer_artifact_name)) return error.InvalidBatchRequest;
    const configs = try catalog.deserializeCatalog(a, catalogs.enrichments);
    const config = for (configs) |value| {
        if (value.kind == .chunk and std.mem.eql(u8, value.name, command.producer_name)) break value;
    } else return error.EnrichmentSourceChanged;
    if (config.neighbor_context_json.len != 0) return error.OnlineMergeArtifactTailsUnsupported;
    if (config.source_artifact_name.len != 0) {
        const upstream = for (configs) |value| {
            if (value.kind == .asset and std.mem.eql(u8, value.name, config.source_artifact_name)) break value;
        } else return error.InvalidBatchRequest;
        const producer = try @import("enrichment/asset_producer.zig").parseProducerConfig(a, upstream.producer_json);
        if (producer.type != .media_output) return error.OnlineMergeArtifactTailsUnsupported;
    }
    return .{ .config = config, .configs = configs };
}
fn planFor(a: Allocator, command: publication.Command) !generations.Plan {
    const page = command.chunk_generation orelse return error.InvalidBatchRequest;
    const spec = generations.Spec.decode(&page.spec) catch return error.InvalidBatchRequest;
    if (command.mode != .prune_chunks and (!std.meta.eql(spec.authority, publication.Authority{ .namespace = command.namespace, .epoch = command.authority_epoch, .catalog_digest = command.catalog_digest }) or
        !std.mem.eql(u8, &spec.input_digest, &command.inputDigest()))) return error.InvalidBatchRequest;
    const scope = try chunks.keyAlloc(a, command.sources[0].document_key, command.producer_artifact_name);
    defer a.free(scope);
    return generations.Plan.init(a, scope, spec);
}
fn requireRow(a: Allocator, command: publication.Command, config: catalog.EnrichmentConfig, effect: publication.Mutation, ordinal: u32) !bool {
    if (effect.family != .document_artifact or effect.source_index != 0 or effect.value == null or !command.sources[0].exists) return error.InvalidBatchRequest;
    const expected = try keys.chunkArtifactKeyAlloc(a, command.sources[0].document_key, config.name, ordinal);
    if (!std.mem.eql(u8, effect.key, expected)) return error.InvalidBatchRequest;
    const parsed = std.json.parseFromSlice(std.json.Value, a, effect.value.?, .{}) catch |err| {
        if (err == error.OutOfMemory) return err;
        return error.InvalidBatchRequest;
    };
    if (parsed.value != .object) return error.InvalidBatchRequest;
    const object = parsed.value.object;
    for ([_][]const u8{ "_parent_doc_key", "_artifact_name", "_source_field" }, [_][]const u8{ command.sources[0].document_key, config.name, config.source_field }) |field, value| {
        const found = object.get(field) orelse return error.InvalidBatchRequest;
        if (found != .string or !std.mem.eql(u8, found.string, value)) return error.InvalidBatchRequest;
    }
    const id = object.get("_chunk_id") orelse return error.InvalidBatchRequest;
    if (id != .integer or id.integer != ordinal) return error.InvalidBatchRequest;
    const mime = object.get("_mime_type") orelse return error.InvalidBatchRequest;
    if (mime != .string or mime.string.len == 0) return error.InvalidBatchRequest;
    const is_text = std.mem.eql(u8, mime.string, "text/plain");
    const payload = object.get(config.source_field) orelse (if (is_text) null else object.get("_data")) orelse return error.InvalidBatchRequest;
    if (payload != .string) return error.InvalidBatchRequest;
    return is_text;
}
fn requireUpstream(a: Allocator, command: publication.Command, config: catalog.EnrichmentConfig, count: u32) !void {
    const scope = try chunks.keyAlloc(a, command.sources[0].document_key, config.name);
    try text.requireUpstream(a, command, config, .{ .family = .document_artifact, .key = scope, .value = if (count != 0) "present" else null, .source_index = 0 }, false);
}

pub const Control = struct {
    arena: std.heap.ArenaAllocator,
    plan: generations.Plan,
    append: ?generations.PreparedAppend = null,
    retirement: ?generations.PreparedRetirement = null,
    command: publication.Command,
    pub fn deinit(self: *Control) void {
        if (self.append) |*value| value.deinit();
        if (self.retirement) |*value| value.deinit();
        self.plan.deinit();
        self.arena.deinit();
    }
    pub fn stage(self: *Control, a: Allocator, txn: anytype) !void {
        try publication.validateSources(a, txn, self.command.namespace, self.command.sources);
        try publication.validateArtifactSources(a, txn, self.command.namespace, self.command.sources, self.command.artifact_sources);
        try publication.validateArtifactSources(a, txn, self.command.namespace, self.command.sources, self.command.mutation_preconditions);
        const authority = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
        if (authority.epoch != self.command.authority_epoch or !std.mem.eql(u8, &authority.catalog_digest, &self.command.catalog_digest)) return error.ArtifactCatalogDrift;
        // Collecting unselected private rows cannot change a sealed logical
        // source cut. Appends still obey the producer admission fence.
        if (self.command.mode != .prune_chunks) {
            if (try @import("artifact_producer_obligations.zig").load(txn)) |obligations| if (obligations.sealed_attempt != null) return error.RetainedEffectsFenceMismatch;
        }
        if (self.command.mode == .append_chunks and self.append == null) {
            _ = try self.plan.begin(txn);
        } else if (self.append) |*append| {
            if (append.previous.progress.count == 0) _ = try self.plan.begin(txn);
            _ = try append.stage(&self.plan, txn);
        } else {
            // Only a currently accepted replacement authorizes collecting its
            // abandoned/previous private generations. The selected head is
            // checked again by both retirement operations.
            var proof = try @import("artifact_chunk_publication.zig").readAcceptedRoot(a, txn, self.command.sources[0].document_key, self.command.producer_name);
            defer proof.deinit();
            const Guard = struct {
                pub fn validate(_: @This(), _: @TypeOf(txn)) !void {}
            };
            _ = try self.plan.retire(txn, authority, Guard{});
            if (self.retirement) |*retirement| _ = try retirement.stage(&self.plan, txn);
        }
    }
};
pub fn prepareControl(a: Allocator, txn: anytype, command: publication.Command, catalogs: inventory.Catalogs) !Control {
    try command.validate(a);
    if (command.mode != .append_chunks and command.mode != .prune_chunks) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const configured = try configFor(scratch, command, catalogs);
    var plan = try planFor(a, command);
    errdefer plan.deinit();
    try requireUpstream(scratch, command, configured.config, if (command.mode == .prune_chunks) 0 else plan.spec.output.count);
    var result: Control = .{ .arena = arena, .plan = plan, .command = command };
    if (command.mode == .append_chunks) {
        const previous = command.chunk_generation.?.previous.?;
        const payloads = try scratch.alloc([]const u8, command.mutations.len);
        var row_arena = std.heap.ArenaAllocator.init(a);
        defer row_arena.deinit();
        for (command.mutations, payloads, 0..) |effect, *payload, i| {
            if (!row_arena.reset(.retain_capacity)) return error.OutOfMemory;
            _ = try requireRow(row_arena.allocator(), command, configured.config, effect, std.math.add(u32, previous.count, @intCast(i)) catch return error.InvalidBatchRequest);
            payload.* = effect.value.?;
        }
        if (payloads.len == 0) {
            if (plan.spec.output.count != 0 or !std.meta.eql(previous, chunks.Builder.init().finish())) return error.InvalidBatchRequest;
        } else result.append = try generations.PreparedAppend.init(a, &plan, .{ .spec = plan.spec, .progress = previous }, payloads);
    } else {
        var state = plan.load(txn) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        const claimed = command.chunk_generation.?.retired_through.?;
        if (claimed > state.progress.count) return error.InvalidBatchRequest;
        if (claimed > state.retired_through) return error.ArtifactPublicationPending;
        state.retiring = true;
        if (claimed == state.retired_through) result.retirement = try generations.PreparedRetirement.init(a, &plan, .{ .namespace = command.namespace, .epoch = command.authority_epoch, .catalog_digest = command.catalog_digest }, state, 128);
    }
    result.arena = arena;
    return result;
}

pub fn prepareSelection(a: Allocator, command: publication.Command, catalogs: inventory.Catalogs) !publication.PreparedEffects {
    try command.validate(a);
    if (command.mode != .publish or command.mutations.len != 2) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const configured = try configFor(owned, command, catalogs);
    var plan = try planFor(a, command);
    defer plan.deinit();
    const scope = try chunks.keyAlloc(owned, command.sources[0].document_key, command.producer_name);
    var head = false;
    var manifest = false;
    for (command.mutations) |effect| {
        if (effect.family != .document_artifact or effect.source_index != 0 or effect.value == null) return error.InvalidBatchRequest;
        if (std.mem.eql(u8, effect.key, scope) and std.mem.eql(u8, effect.value.?, &plan.spec.output.encode())) {
            manifest = true;
        } else if (std.mem.eql(u8, effect.key, plan.head_key) and std.mem.eql(u8, effect.value.?, &plan.spec.encode())) {
            head = true;
        } else return error.InvalidBatchRequest;
    }
    if (!head or !manifest) return error.InvalidBatchRequest;
    for ([_][]const u8{ scope, plan.head_key }) |key| {
        const guarded = for (command.mutation_preconditions) |condition| {
            if (condition.source_index == 0 and std.mem.eql(u8, condition.key, key)) break true;
        } else false;
        if (!guarded) return error.InvalidBatchRequest;
    }
    try requireUpstream(owned, command, configured.config, plan.spec.output.count);
    var prepared = try text.prepareTextEffects(&arena, command, catalogs, configured.config, configured.configs, &.{}, &.{}, true);
    prepared.chunk_fence = .{ .key = scope, .member_count = 0, .next_count = plan.spec.output.count, .generation_state_key = try owned.dupe(u8, plan.state_key), .generation_spec = plan.spec };
    prepared.batch.changed_artifact_keys = try owned.dupe([]const u8, &.{ scope, try owned.dupe(u8, plan.head_key) });
    prepared.arena = arena;
    return prepared;
}

/// Full-text consumers still need their ordinary replay projections. This
/// bounded preparation happens off-lock against immutable staged rows; the
/// writer checks the generation's complete digest again before selection.
pub fn projectionWorkingSetBytes(a: Allocator, txn: anytype, command: publication.Command, catalogs: inventory.Catalogs) !u64 {
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const configured = try configFor(scratch.allocator(), command, catalogs);
    if (!try hasTextConsumer(scratch.allocator(), configured.config, catalogs)) return 0;
    var plan = try planFor(a, command);
    defer plan.deinit();
    const state = plan.load(txn) catch |err| if (err == error.NotFound) return error.ArtifactPublicationPending else return err;
    if (state.retiring or !std.meta.eql(state.progress, plan.spec.output)) return error.ArtifactPublicationPending;
    const scope = try chunks.keyAlloc(scratch.allocator(), command.sources[0].document_key, configured.config.name);
    const previous = try chunks.Manifest.decode(try txn.get(scope));
    const payload = std.math.mul(u64, plan.spec.output.payload_bytes, 8) catch return error.ResourceBudgetExceeded;
    const members = std.math.mul(u64, @max(previous.count, plan.spec.output.count), 4096) catch return error.ResourceBudgetExceeded;
    return std.math.add(u64, payload, members) catch return error.ResourceBudgetExceeded;
}
fn hasTextConsumer(a: Allocator, config: catalog.EnrichmentConfig, catalogs: inventory.Catalogs) !bool {
    const manager = @import("catalog/index_manager.zig");
    const indexes = if (catalogs.indexes.len == 0) &.{} else try manager.deserializeCatalog(a, catalogs.indexes);
    const include_default = config.full_text_index or try @import("../../chunking/types.zig").parseHasFullTextIndexFromSlice(a, config.chunker_json);
    for (indexes) |index| {
        if (index.kind != .full_text) continue;
        const consumer = try manager.TextArtifactConsumer.init(a, index.config_json);
        if (consumer.consumes(config.name, include_default)) return true;
    }
    return false;
}
pub fn bindSelection(a: Allocator, txn: anytype, command: publication.Command, catalogs: inventory.Catalogs, prepared: *publication.PreparedEffects) !void {
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const configured = try configFor(scratch.allocator(), command, catalogs);
    if (!try hasTextConsumer(scratch.allocator(), configured.config, catalogs)) return;
    var plan = try planFor(a, command);
    defer plan.deinit();
    const state = plan.load(txn) catch |err| if (err == error.NotFound) return error.ArtifactPublicationPending else return err;
    if (state.retiring or !std.meta.eql(state.progress, plan.spec.output)) return error.ArtifactPublicationPending;
    const scope = prepared.chunk_fence.?.key;
    const previous = try chunks.Manifest.decode(try txn.get(scope));
    const count = @max(previous.count, plan.spec.output.count);
    const owned = prepared.arena.allocator();
    if (plan.spec.output.payload_bytes > publication.max_payload_bytes or count > publication.max_payload_bytes / @sizeOf(publication.Mutation)) return error.ResourceBudgetExceeded;
    const effects = try owned.alloc(publication.Mutation, count);
    const is_text = try owned.alloc(bool, count);
    for (effects, is_text, 0..) |*effect, *text_member, ordinal| {
        const key = try keys.chunkArtifactKeyAlloc(owned, command.sources[0].document_key, configured.config.name, @intCast(ordinal));
        var payload: ?[]const u8 = null;
        if (ordinal < plan.spec.output.count) {
            const physical = try scratch.allocator().alloc(u8, plan.row_prefix.len + 4);
            @memcpy(physical[0..plan.row_prefix.len], plan.row_prefix);
            std.mem.writeInt(u32, physical[plan.row_prefix.len..][0..4], @intCast(ordinal), .big);
            payload = try owned.dupe(u8, try txn.get(physical));
        }
        effect.* = .{ .family = .document_artifact, .key = key, .value = payload, .source_index = 0 };
        text_member.* = if (payload != null) try requireRow(scratch.allocator(), command, configured.config, effect.*, @intCast(ordinal)) else false;
    }
    var projection = try text.prepareTextEffects(&prepared.arena, command, catalogs, configured.config, configured.configs, effects, is_text, true);
    projection.chunk_fence = prepared.chunk_fence;
    prepared.* = projection;
}

test "model aware media staged chunk commands own bounded preparation through allocation failures and binary transport" {
    const a = std.testing.allocator;
    const encoded_catalog = try catalog.serializeCatalog(a, &.{.{ .name = "chunks", .kind = .chunk, .source_field = "body", .chunk_size = 8 }});
    defer a.free(encoded_catalog);
    const catalogs: inventory.Catalogs = .{ .indexes = "", .enrichments = encoded_catalog, .resolvers = "" };
    const scope = try chunks.keyAlloc(a, "doc", "chunks");
    defer a.free(scope);
    var rows = try chunks.PreparedRows.init(a, "doc", "chunks", "body", &.{.{ .chunk_id = 0, .text = @constCast("hello") }});
    defer rows.deinit();
    const sources = [_]publication.Source{.{ .document_key = "doc", .timestamp = 1, .content_digest = @splat(3), .input_position = null }};
    var command: publication.Command = .{ .mode = .append_chunks, .producer_kind = .enrichment, .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = catalogs.digest(), .producer_name = "chunks", .producer_artifact_name = "chunks", .producer_generation = 1, .sources = &sources, .mutations = rows.mutations, .publication_digest = @splat(0) };
    const spec = try generations.Spec.init(.{ .namespace = command.namespace, .epoch = 1, .catalog_digest = command.catalog_digest }, scope, command.inputDigest(), rows.manifest.?, 1);
    command.chunk_generation = .{ .spec = spec.encode(), .previous = chunks.Builder.init().finish() };
    command.publication_digest = command.digest();
    const Check = struct {
        fn run(alloc: Allocator, value: publication.Command, configs: inventory.Catalogs) !void {
            const NoRead = struct {
                pub fn get(_: *@This(), _: []const u8) anyerror![]const u8 {
                    return error.UnexpectedRead;
                }
            };
            var read: NoRead = .{};
            var prepared = try prepareControl(alloc, &read, value, configs);
            defer prepared.deinit();
        }
    };
    // Arena consolidation may make a particular allocation index disappear
    // with a different backing alignment. Exercise every bounded failure slot
    // while checking both error propagation and complete rollback.
    for (0..128) |fail_index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
        Check.run(failing.allocator(), command, catalogs) catch |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
            continue;
        };
        try std.testing.expect(!failing.has_induced_failure);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
    const codec = @import("artifact_publication_transport_codec.zig");
    const bytes = try codec.encodeAlloc(a, command);
    defer a.free(bytes);
    var decoded = try codec.decodeBorrowed(a, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(command, decoded.command);
    try std.testing.expect(!codec.controlAdmissionHint(bytes));
    const raw = try std.json.Stringify.valueAlloc(a, command, .{});
    defer a.free(raw);
    var parsed = try std.json.parseFromSlice(publication.Command, a, raw, .{});
    defer parsed.deinit();
    try std.testing.expectEqualDeep(command, parsed.value);
    var forged = command;
    forged.sources = &.{.{ .document_key = "doc", .timestamp = 2, .content_digest = @splat(4), .input_position = null }};
    forged.publication_digest = forged.digest();
    try std.testing.expectError(error.InvalidBatchRequest, Check.run(a, forged, catalogs));
}
