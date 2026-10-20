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

//! Public embedded DB C ABI.
comptime {
    _ = @import("sql_describe.zig");
}
const handles = @import("handles.zig");
pub const testMaintenanceCaptureAdmission = @import("lite_connection.zig").testMaintenanceCaptureAdmission;
pub const std = handles.std;
pub const builtin = handles.builtin;
pub const local_write = handles.local_write;
pub const capi = handles.capi;
pub const kernel_owner_abi = handles.kernel_owner_abi;
pub const local_query_client = handles.local_query_client;
pub const capi_build_options = handles.capi_build_options;
pub const db_mod = handles.db_mod;
pub const read_consistency = handles.read_consistency;
pub const transactions_mod = handles.transactions_mod;
pub const aggregations_mod = handles.aggregations_mod;
pub const search_agg_mod = handles.search_agg_mod;
pub const geo_mod = handles.geo_mod;
pub const lite_backend = handles.lite_backend;
pub const batch_api = handles.batch_api;
pub const query_api = handles.query_api;
pub const tables_api = handles.tables_api;
pub const table_reads_api = handles.table_reads_api;
pub const inference_provider = handles.inference_provider;
pub const managed_embedder = handles.managed_embedder;
pub const Allocator = handles.Allocator;
pub const abi_version = handles.abi_version;
pub const Handle = handles.Handle;
pub const stopLiteEmbeddedInference = handles.stopLiteEmbeddedInference;
pub const closeHandle = handles.closeHandle;
pub const liteOpenModeCanWrite = handles.liteOpenModeCanWrite;
pub const currentIdentityReadGenerationForHandle = handles.currentIdentityReadGenerationForHandle;
pub const stampSearchRequestIdentityGeneration = handles.stampSearchRequestIdentityGeneration;
pub const ReadableLeaseHookFn = handles.ReadableLeaseHookFn;
pub const ReadableLeaseHook = handles.ReadableLeaseHook;
pub const asHandle = handles.asHandle;
pub const HandleRegistryOf = handles.HandleRegistryOf;
pub const HandleRegistry = handles.HandleRegistry;
pub const handle_registry = &handles.handle_registry;
pub const handleRegistry = handles.handleRegistry;
pub const closeHandleId = handles.closeHandleId;
pub const handleLockIo = handles.handleLockIo;
pub const dupBytes = handles.dupBytes;
pub const JsonSearchAggregationRequest = handles.JsonSearchAggregationRequest;
pub const JsonNumericRangeRequest = handles.JsonNumericRangeRequest;
pub const JsonDateRangeRequest = handles.JsonDateRangeRequest;
pub const JsonDistanceRangeRequest = handles.JsonDistanceRangeRequest;
pub const JsonSearchAggregationBucket = handles.JsonSearchAggregationBucket;
pub const JsonSearchAggregationResult = handles.JsonSearchAggregationResult;
pub const freeAggregationRequests = handles.freeAggregationRequests;
pub const freeRawBuffer = handles.freeRawBuffer;
pub const computeSearchAggregations = handles.computeSearchAggregations;
pub const computeSingleAggregation = handles.computeSingleAggregation;
pub const NumericMetricKind = handles.NumericMetricKind;
pub const computeNumericMetricAggregation = handles.computeNumericMetricAggregation;
pub const computeCardinalityAggregation = handles.computeCardinalityAggregation;
pub const computeTermsAggregation = handles.computeTermsAggregation;
pub const computeHistogramAggregation = handles.computeHistogramAggregation;
pub const computeDateHistogramAggregation = handles.computeDateHistogramAggregation;
pub const computeRangeAggregation = handles.computeRangeAggregation;
pub const matchesNumericRangeValue = handles.matchesNumericRangeValue;
pub const matchesDateRangeValue = handles.matchesDateRangeValue;
pub const matchesGeoDistanceValue = handles.matchesGeoDistanceValue;
pub const accumulateNumericJsonValue = handles.accumulateNumericJsonValue;
pub const collectCardinalityValues = handles.collectCardinalityValues;
pub const appendTermAggregationValuesZig = handles.appendTermAggregationValuesZig;
pub const jsonValueToTermKey = handles.jsonValueToTermKey;
pub const stringifyJsonValueCompact = handles.stringifyJsonValueCompact;
pub const distanceToMeters = handles.distanceToMeters;
pub const extractGeoPointFieldFromStoredJson = handles.extractGeoPointFieldFromStoredJson;
pub const jsonValueToF64 = handles.jsonValueToF64;
pub const fillHistogramBucketKeys = handles.fillHistogramBucketKeys;
pub const fillDateHistogramBucketKeys = handles.fillDateHistogramBucketKeys;
pub const nextDateHistogramBucketKey = handles.nextDateHistogramBucketKey;
pub const addCalendarMonths = handles.addCalendarMonths;
pub const addCalendarYears = handles.addCalendarYears;
pub const civilDateToBucketNs = handles.civilDateToBucketNs;
pub const extractNumericFieldFromStoredJson = handles.extractNumericFieldFromStoredJson;
pub const extractTimestampFieldFromStoredJson = handles.extractTimestampFieldFromStoredJson;
pub const parseDateInterval = handles.parseDateInterval;
pub const parseRfc3339ToNs = handles.parseRfc3339ToNs;
pub const daysFromCivil = handles.daysFromCivil;
pub const formatRfc3339Bucket = handles.formatRfc3339Bucket;
pub const civilFromDays = handles.civilFromDays;
pub const extractValueAtPath = handles.extractValueAtPath;
pub const antfly = @import("../capi_embedded_root.zig");
pub const TestDirectory = antfly.testing.TestDirectory;
pub const TestDirectoryType = TestDirectory;
pub const vector_mod = @import("antfly_vector").vector;
pub const ApiTypes = capi;
pub const search_wire = @import("search_wire.zig");
pub const hbc = antfly.hbc;
pub const graph_mod = antfly.graph;
pub const traversal_mod = antfly.traversal;
pub const paths_mod = antfly.paths;
pub const graph_query_mod = antfly.graph_query;
const relationship_filter = graph_query_mod.relationship_filter;
pub const graph_pattern_mod = antfly.graph_pattern;
pub const lite_restore_staging = antfly.lite.restore_staging;
pub const portable_backup = antfly.portable_backup;
pub const indexes_api = antfly.public_api.indexes;
pub fn monotonicNowNs() u64 {
    return antfly.platform_time.monotonicNs();
}

pub fn startLiteEmbeddedInference(
    handle: *Handle,
    alloc: Allocator,
    path: []const u8,
    budget_options: inference_provider.EmbeddedInferenceNodeOptions,
) !void {
    const io_impl = try alloc.create(std.Io.Threaded);
    errdefer alloc.destroy(io_impl);
    io_impl.* = std.Io.Threaded.init(std.heap.page_allocator, .{});
    errdefer io_impl.deinit();
    const data_dir = std.fs.path.dirname(path) orelse ".";
    var owned_options = budget_options;
    const models_dir = if (budget_options.models_dir) |directory| try alloc.dupe(u8, directory) else null;
    errdefer if (models_dir) |directory| alloc.free(directory);
    owned_options.models_dir = models_dir;
    const service = try alloc.create(handles.SharedLiteInference);
    errdefer alloc.destroy(service);
    const created = try inference_provider.createEmbeddedInferenceNode(data_dir, io_impl.io(), owned_options);
    service.* = .{
        .alloc = alloc,
        .io = io_impl,
        .models_dir = models_dir,
        .lifetime = .{ .handle = created.handle, .resource_owner = created.resource_owner },
    };
    handle.lite_inference = service;
    // Report the policy the node actually resolved (host-detected by
    // default, or the caller's explicit override) rather than leaving
    // `lite_inference_status` at the pre-open placeholder values.
    if (handle.lite_inference_status) |*status| {
        status.process_memory_limit_bytes = @intCast(created.process_memory_limit_bytes);
        status.process_memory_limit_source = @tagName(created.process_memory_limit_source);
        status.host_budget_mb = created.host_budget_mb;
        status.backend_budget_mb = created.backend_budget_mb;
        status.combined_budget_mb = created.combined_budget_mb;
        status.kv_budget_mb = created.kv_budget_mb;
        status.scratch_budget_mb = created.scratch_budget_mb;
    }
}

pub fn liteManagedEmbeddingIndexConfigJson(
    alloc: Allocator,
    kind: db_mod.types.IndexKind,
    config_json: []const u8,
) ![]u8 {
    if (kind != .dense_vector and kind != .sparse_vector) return try alloc.dupe(u8, config_json);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = std.json.parseFromSlice(std.json.Value, arena, config_json, .{}) catch
        return try alloc.dupe(u8, config_json);
    if (parsed.value != .object) return try alloc.dupe(u8, config_json);

    if (parsed.value.object.get("type") == null) {
        try parsed.value.object.put(arena, "type", .{ .string = "embeddings" });
    }
    if (kind == .sparse_vector and parsed.value.object.get("sparse") == null) {
        try parsed.value.object.put(arena, "sparse", .{ .bool = true });
    }
    if (parsed.value.object.get("dimension") == null) {
        if (parsed.value.object.get("dims")) |dims_value| {
            try parsed.value.object.put(arena, "dimension", dims_value);
        }
    }
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
}

/// Server table provisioning accepts the artifact-stream chunk pattern
/// (`go/pkg/docsaf`'s `chunk` enrichment producing a `doc_chunks_v1` artifact,
/// consumed by an embeddings index via `"sources":[{"artifact":...}]`) by
/// nesting an `"enrichments"` array inside whichever index config
/// authoritatively owns each producer, then harvesting every nested
/// declaration across the whole table (`api/indexes.zig`'s
/// `collectArtifactEnrichmentsFromTableIndexesJsonWithOptions`, dependency
/// sorted via `sortArtifactEnrichmentsByDependency`) and registering each one
/// with `db.upsertEnrichment` *before* admitting the physical indexes
/// (`metadata_table_provisioner.reconcileDbIndexesWithOptions` calls
/// `ensureEnrichments` ahead of `ensureIndexes`). A native Lite handle only
/// ever admits one index at a time through `antfly_db_add_index_json`, so
/// there is no single merged table definition to harvest from; this instead
/// harvests the `"enrichments"` nested in *this* index's own raw config
/// before it is translated/admitted, mirroring the same per-index shape
/// docsaf and the dogfood example already send. Two caveats callers must
/// respect that the server's atomic table-create request does not have: (1)
/// a producer index (e.g. the `chunk` enrichment's owning `full_text` index)
/// must be added before any index whose `sources`/`embedding_name` names an
/// artifact that producer's enrichment declares, since enrichment admission
/// validates upstream references immediately; (2) re-adding the same index
/// name replays its enrichment declarations too, which is harmless because
/// `db.upsertEnrichment` is idempotent for an unchanged config. Scoped to the
/// native profile: a hosted Lite handle's owning process reconciles its own
/// enrichments the same way the server does.
pub fn registerLiteIndexEnrichments(handle: *Handle, config_json: []const u8, rollback: *LiteCatalogRollback) !void {
    var arena_impl = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = std.json.parseFromSlice(std.json.Value, arena, config_json, .{}) catch return;
    if (parsed.value != .object or parsed.value.object.get("enrichments") == null) return;

    const alloc = handle.alloc;
    var collected: std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig) = .empty;
    defer {
        for (collected.items) |*cfg| cfg.deinit(alloc);
        collected.deinit(alloc);
    }
    try indexes_api.collectArtifactEnrichmentsFromValueWithOptions(
        alloc,
        parsed.value,
        .{ .antfly_provider = handle.liteAntflyProvider() },
        &collected,
    );
    if (collected.items.len == 0) return;
    indexes_api.sortArtifactEnrichmentsByDependency(collected.items);
    for (collected.items) |cfg| {
        // Record the touch before mutating, so a mid-loop failure still rolls
        // back every enrichment this call may have changed.
        try rollback.willTouchEnrichment(cfg.kind, cfg.name);
        _ = try handle.database().*.upsertEnrichment(cfg);
    }
}

/// Undo buffer for the catalog mutations an embedded AddIndex performs
/// before index admission (nested enrichments and resolvers).
/// The C ABI admits one index per call with no transaction around the
/// enrichment/resolver catalogs, so a rejected or partially failed AddIndex
/// must restore the pre-call configuration itself: durably upserting an
/// existing enrichment's changed geometry and then failing index admission
/// (for example with IndexAlreadyExists) must not leave the changed
/// enrichment active.
pub const LiteCatalogRollback = struct {
    const TouchedEnrichment = struct {
        kind: db_mod.types.EnrichmentKind,
        name: []u8,
    };

    handle: *Handle,
    /// Full pre-call enrichment catalog (owned).
    prior: []db_mod.types.EnrichmentConfig,
    /// Full pre-call resolver catalog (owned).
    prior_resolvers: []db_mod.ResolverConfig,
    touched: std.ArrayListUnmanaged(TouchedEnrichment) = .empty,
    touched_resolvers: std.ArrayListUnmanaged([]u8) = .empty,

    pub fn init(handle: *Handle) !LiteCatalogRollback {
        const prior = try handle.database().*.listEnrichments(handle.alloc);
        errdefer db_mod.types.freeEnrichmentConfigs(handle.alloc, prior);
        return .{
            .handle = handle,
            .prior = prior,
            .prior_resolvers = try handle.database().core.index_manager.listResolverIdentities(handle.alloc),
        };
    }

    pub fn deinit(self: *LiteCatalogRollback) void {
        const alloc = self.handle.alloc;
        db_mod.types.freeEnrichmentConfigs(alloc, self.prior);
        for (self.prior_resolvers) |*cfg| cfg.deinit(alloc);
        alloc.free(self.prior_resolvers);
        for (self.touched.items) |touch| alloc.free(touch.name);
        self.touched.deinit(alloc);
        for (self.touched_resolvers.items) |name| alloc.free(name);
        self.touched_resolvers.deinit(alloc);
    }

    pub fn willTouchEnrichment(self: *LiteCatalogRollback, kind: db_mod.types.EnrichmentKind, name: []const u8) !void {
        const alloc = self.handle.alloc;
        for (self.touched.items) |touch| {
            if (touch.kind == kind and std.mem.eql(u8, touch.name, name)) return;
        }
        try self.touched.append(alloc, .{ .kind = kind, .name = try alloc.dupe(u8, name) });
    }

    pub fn willTouchResolver(self: *LiteCatalogRollback, name: []const u8) !void {
        const alloc = self.handle.alloc;
        for (self.touched_resolvers.items) |touched| {
            if (std.mem.eql(u8, touched, name)) return;
        }
        try self.touched_resolvers.append(alloc, try alloc.dupe(u8, name));
    }

    /// Best-effort restore of every touched enrichment to its pre-call
    /// configuration: re-upsert the prior config, or delete an enrichment
    /// this call introduced. Restore failures are logged, never masked over
    /// the admission error the caller is already returning.
    pub fn restore(self: *LiteCatalogRollback) void {
        for (self.touched_resolvers.items) |name| {
            const prior = blk: {
                for (self.prior_resolvers) |cfg| {
                    if (std.mem.eql(u8, cfg.name, name)) break :blk cfg;
                }
                break :blk null;
            };
            self.handle.database().*.restoreResolverIdentityWithoutDrain(name, prior) catch |err| {
                std.log.warn("AddIndex rollback failed to restore resolver identity {s}: {s}", .{ name, @errorName(err) });
            };
        }
        for (self.touched.items) |touch| {
            const prior = blk: {
                for (self.prior) |cfg| {
                    if (cfg.kind == touch.kind and std.mem.eql(u8, cfg.name, touch.name)) break :blk cfg;
                }
                break :blk null;
            };
            if (prior) |cfg| {
                _ = self.handle.database().*.upsertEnrichment(cfg) catch |err| {
                    std.log.warn("lite AddIndex rollback failed to restore enrichment {s}: {s}", .{ touch.name, @errorName(err) });
                };
            } else {
                _ = self.handle.database().*.deleteEnrichment(touch.kind, touch.name) catch |err| {
                    std.log.warn("lite AddIndex rollback failed to remove enrichment {s}: {s}", .{ touch.name, @errorName(err) });
                };
            }
        }
    }
};

/// Register the entity resolvers nested in this index's own raw config, the
/// way `registerLiteIndexEnrichments` harvests nested `"enrichments"`. The
/// server registers resolvers from the whole table's indexes JSON
/// (`metadata_table_provisioner.ensureResolversWithOptions`) after index
/// provisioning; a native Lite handle admits one index at a time, so this
/// harvests the graph config's `"resolvers"` array before graph construction so
/// retained canonical endpoints are available during index rebuild. Deletion reconciles ownership across all admitted graph indexes;
/// the last owner's removal retires execution but retains durable identity. `upsertResolver`
/// is idempotent for an unchanged config; backfill is deferred to the
/// resolver workers (or the next `antfly_db_run_until_idle`).
pub fn registerLiteIndexResolvers(handle: *Handle, config_json: []const u8, rollback: *LiteCatalogRollback) !void {
    var arena_impl = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = std.json.parseFromSlice(std.json.Value, arena, config_json, .{}) catch return;
    if (parsed.value != .object) return;
    const resolvers = parsed.value.object.get("resolvers") orelse return;
    if (resolvers != .array) return;
    for (resolvers.array.items) |item| {
        if (item != .object) continue;
        var cfg = try db_mod.ResolverConfig.parseValue(arena, item);
        // Config replay may include catalog metadata read back by a caller.
        // Curation is owned by the merge API and cannot be replaced inline.
        cfg.value.merges = &.{};
        cfg.value.merge_generation = 0;
        cfg.value.merge_state_version = 0;
        cfg.value.resolution_generation = 0;
        cfg.value.namespace_generation = 0;
        cfg.value.inline_managed = true;
        cfg.value.retired = false;
        cfg.value.replacement_index = "";
        cfg.value.replacement_resolver = "";
        // Record the touch before mutating, so a mid-loop failure (an
        // invalid later resolver, a label conflict) still restores every
        // earlier insertion or replacement this call made.
        try rollback.willTouchResolver(cfg.value.name);
        _ = try handle.database().*.upsertResolverWithResultOptions(cfg.value, .{ .drain_backfill = false, .wait_for_activity = true });
    }
}

pub fn needsDefaultEmbeddingField(object: std.json.ObjectMap) bool {
    if (object.get("field") != null) return false;
    if (object.get("template") != null) return false;
    if (object.get("sources") != null) return false;
    if (object.get("embedding_name") != null) return false;
    if (object.get("source_artifact_name") != null) return false;
    if (object.get("chunker") != null) return false;
    if (object.get("external")) |external| {
        if (external == .bool and external.bool) return false;
    }
    return true;
}

pub fn litePhysicalIndexConfigJson(
    alloc: Allocator,
    kind: db_mod.types.IndexKind,
    name: []const u8,
    config_json: []const u8,
    provider: ?managed_embedder.AntflyProvider,
) ![]u8 {
    if (kind != .dense_vector and kind != .sparse_vector) return try alloc.dupe(u8, config_json);
    const bridged = try liteManagedEmbeddingIndexConfigJson(alloc, kind, config_json);
    defer alloc.free(bridged);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = std.json.parseFromSlice(std.json.Value, arena, bridged, .{}) catch
        return try alloc.dupe(u8, config_json);
    // The translator requires `field` (or `template`/an artifact source) on
    // a non-external embeddings config and otherwise bails out before ever
    // resolving dimensions -- before this, a caller relying on the same
    // "embedding" default `field` this function's own post-failure fallback
    // below injects would always take that fallback, which has no way to
    // learn the real vector width and stores an index `db.addIndex` then
    // rejects for a missing `dims`. Inject the default proactively so
    // translation actually runs and probes the configured embedder (local or
    // remote) for its output width instead of falling back before trying.
    if (parsed.value == .object and needsDefaultEmbeddingField(parsed.value.object)) {
        parsed.value.object.put(arena, "field", .{ .string = "embedding" }) catch
            return try alloc.dupe(u8, config_json);
    }
    // `provider` must be threaded through here too: an embedder with no
    // `api_url` translates to a durable `"antfly:embedded"` semantic
    // producer identity, and the translator rejects that identity outright
    // when no embedded provider is attached to validate it against.
    return managed_embedder.translateEmbeddingsIndexConfigJsonWithOptions(alloc, name, parsed.value, .{ .antfly_provider = provider }) catch {
        // A translation failure (for example dimension auto-detection
        // requiring a live round trip this call cannot make) must not turn
        // into a hard AddIndex error. Fall back to the bridged config, but
        // `index_manager.parseDenseConfig` hard-requires `field` on every
        // dense/sparse entry -- callers who omit it entirely (relying on
        // translation to supply the artifact-storage default) would
        // otherwise fail `db.addIndex` outright instead of landing in the
        // same "no managed producer configured" no-op state as any other
        // untranslatable config.
        if (parsed.value == .object and parsed.value.object.get("field") == null) {
            parsed.value.object.put(arena, "field", .{ .string = "embedding" }) catch
                return try alloc.dupe(u8, config_json);
            return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
        }
        return try alloc.dupe(u8, config_json);
    };
}

/// Rebuilds the DB's managed embedding/chunking/extraction runtime from the
/// currently declared indexes and enrichments. Provider "antfly" producers
/// with no `api_url` route through the Lite handle's embedded inference
/// provider when one exists (see `startLiteEmbeddedInference`); producers
/// that carry their own `api_url` call that remote Antfly inference service
/// directly and work fine with a null local provider. Scoped to the native
/// profile: hosted Lite handles are reconciled by their owning process and
/// non-Lite (storage-owner) handles are reconciled through
/// `configureStorageKernelOwnerDb` instead. Includes every declared index
/// kind (not just dense/sparse vector) plus standalone enrichments so a
/// graph index's asset-producer extractor and any chunk enrichments are
/// discovered the same way `indexesJsonNeedsAssetProducer` /
/// `indexesJsonHasGeneratedEnrichment` discover them for the server. A
/// database with neither a local provider nor any producer `api_url`
/// resolves an empty producer set, which is a safe no-op: pending work stays
/// visible and neither open nor AddIndex/AddEnrichment errors.
/// Builds the merged `{"<index_name>":<bridged_config_json>, ...}` blob both
/// `refreshLiteManagedEmbeddingRuntime` (write-time enrichment wiring) and
/// `LiteSemanticResolver` (query-time `semantic_search` embedding) feed to
/// `managed_embedder.zig`. Every index kind is included, not just
/// dense_vector/sparse_vector: `indexesJsonNeedsAssetProducer`/
/// `indexesJsonHasGeneratedEnrichment` recursively scan the whole merged
/// object for a nested `"kind":"asset","producer_json":...}` object (see
/// examples/dogfood's knowledgeGraphIndexJSON, which declares the graph
/// index's extractor exactly that way, under an "artifact" key inside the
/// graph index's own config), so a graph/full_text/algebraic index must not
/// be filtered out here even though `managed_embedder.zig`'s embedder
/// scanner only ever recognizes a dense_vector/sparse_vector entry.
/// `liteManagedEmbeddingIndexConfigJson` passes every other kind through
/// unchanged.
///
/// Also appends every *standalone* catalog enrichment from `db.listEnrichments`
/// -- one registered directly through `antfly_db_add_enrichment_json` with no
/// index nesting the same declaration in its own config (see
/// `registerLiteIndexEnrichments`). Without this, a `kind:"asset"` extractor
/// or a `kind:"chunk"` enrichment added standalone is accepted into the
/// catalog (`db.addEnrichment` validates and stores it) but never gets an
/// asset producer or chunk provider wired up: `indexesJsonNeedsAssetProducer`/
/// `indexesJsonHasGeneratedEnrichment` only ever saw the index catalog, so a
/// document's pending generated-enrichment work for that name stays "accepted"
/// forever with nothing servicing it (a stall, surfaced as
/// `error.RunUntilIdleNoProgress` from `antfly_db_run_until_idle`). Each
/// standalone entry is appended under a `"$enrichment:<kind>:<name>"` key --
/// reserved so it cannot collide with a real index name -- as an object shaped
/// `{"kind":<kind>,"producer_json":<...>}` (only when non-empty), which is
/// exactly the shape the two scanners above already recognize wherever it
/// appears in the merged tree. `managed_embedder.zig`'s own scanners
/// (`parseManagedEmbeddingEntry`, `addArtifactBackedManagedEmbeddingEntries`)
/// only ever look at top-level entries carrying `"type":"embeddings"`, and
/// this shape carries no `"type"` or `"enrichments"` key, so it is inert to
/// them and to `LiteSemanticResolver`'s query-time resolution. Caller owns the
/// returned slice.
pub fn liteMergedIndexesJsonAlloc(handle: *Handle) ![]u8 {
    return liteMergedIndexesForDatabaseJsonAlloc(handle.alloc, handle.database());
}

pub fn liteMergedIndexesForDatabaseJsonAlloc(alloc: Allocator, database: *db_mod.DB) ![]u8 {
    const configs = try database.listIndexes(alloc);
    defer db_mod.types.freeIndexConfigs(alloc, configs);
    const enrichments = try database.listEnrichments(alloc);
    defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(alloc);
    try buf.append(alloc, '{');
    var wrote_any = false;
    for (configs) |cfg| {
        if (wrote_any) try buf.append(alloc, ',');
        wrote_any = true;
        const escaped_name = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(cfg.name, .{})});
        defer alloc.free(escaped_name);
        try buf.appendSlice(alloc, escaped_name);
        try buf.append(alloc, ':');
        const entry_config_json = try liteManagedEmbeddingIndexConfigJson(alloc, cfg.kind, cfg.config_json);
        defer alloc.free(entry_config_json);
        try buf.appendSlice(alloc, entry_config_json);
    }
    for (enrichments) |cfg| {
        if (wrote_any) try buf.append(alloc, ',');
        wrote_any = true;
        const merged_key = try std.fmt.allocPrint(alloc, "$enrichment:{s}:{s}", .{ @tagName(cfg.kind), cfg.name });
        defer alloc.free(merged_key);
        const escaped_key = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(merged_key, .{})});
        defer alloc.free(escaped_key);
        try buf.appendSlice(alloc, escaped_key);
        try buf.append(alloc, ':');
        const entry_json = try liteEnrichmentCatalogEntryJsonAlloc(alloc, cfg);
        defer alloc.free(entry_json);
        try buf.appendSlice(alloc, entry_json);
    }
    try buf.append(alloc, '}');
    return try buf.toOwnedSlice(alloc);
}

/// Builds the `{"kind":<kind>,"producer_json":<...>}`-shaped object
/// `liteMergedIndexesJsonAlloc` nests under each standalone catalog
/// enrichment's reserved `"$enrichment:<kind>:<name>"` key. `producer_json`
/// is included only for an `asset` enrichment that carries one (the field
/// `objectIsModelBackedAssetEnrichment` inspects); `kind:"chunk"` needs no
/// further fields since `jsonValueHasGeneratedEnrichment` treats any
/// `"kind":"chunk"` object as a generated-enrichment marker regardless of
/// its other fields. A standalone `embedding` enrichment (always paired with
/// an owning dense/sparse index's own `"type":"embeddings"` config, already
/// merged in above) carries neither marker and is included only for listing
/// symmetry; it is inert to every scanner.
pub fn liteEnrichmentCatalogEntryJsonAlloc(alloc: Allocator, cfg: db_mod.types.EnrichmentConfig) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(alloc);
    const kind_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(@tagName(cfg.kind), .{})});
    defer alloc.free(kind_json);
    try buf.appendSlice(alloc, "{\"kind\":");
    try buf.appendSlice(alloc, kind_json);
    if (cfg.producer_json.len > 0) {
        const producer_json_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(cfg.producer_json, .{})});
        defer alloc.free(producer_json_json);
        try buf.appendSlice(alloc, ",\"producer_json\":");
        try buf.appendSlice(alloc, producer_json_json);
    }
    if (cfg.source) |source| {
        const json = try std.json.Stringify.valueAlloc(alloc, source, .{});
        defer alloc.free(json);
        try buf.appendSlice(alloc, ",\"source\":");
        try buf.appendSlice(alloc, json);
    }
    if (cfg.prior_context) |context| {
        const json = try std.json.Stringify.valueAlloc(alloc, context, .{});
        defer alloc.free(json);
        try buf.appendSlice(alloc, ",\"prior_context\":");
        try buf.appendSlice(alloc, json);
    }
    try buf.append(alloc, '}');
    return try buf.toOwnedSlice(alloc);
}

pub fn refreshLiteManagedEmbeddingRuntime(handle: *Handle) !void {
    return refreshLiteManagedEmbeddingRuntimeForDatabase(handle, handle.database());
}

/// Reopened namespaces share the root's inference provider and replay options,
/// but restore their own catalog's enrichment runtime before accepting writes.
pub fn refreshLiteManagedEmbeddingRuntimeForDatabase(handle: *Handle, database: *db_mod.DB) !void {
    if (handle.lite_profile != .native) return;
    // A read-only/status-only handle has nothing to reconcile toward, and
    // `db.reconfigureEnrichmentRuntime` unconditionally fails with
    // `error.ReadOnly` on one -- before it even looks at whether there is
    // anything to configure. Every open of an already-written native Lite
    // database (query_readonly, status_only) would otherwise fail outright.
    if (!liteOpenModeCanWrite(handle.open_mode)) return;
    const provider = handle.liteAntflyProvider();
    const alloc = handle.alloc;
    const merged_json = try liteMergedIndexesForDatabaseJsonAlloc(alloc, database);
    defer alloc.free(merged_json);

    // Not `local_write.reconfigureManagedDbEnrichmentRuntime` directly: that
    // helper derives `enable_without_producers` purely from
    // `indexesJsonHasGeneratedEnrichment`'s scan of the *index* catalog's own
    // config shape (an inline `"kind":"chunk"`/`"asset"` object, or an
    // `"embeddings"` config). A full-text index that references a chunk
    // enrichment by name -- `{"chunk_name":"..."}`, the shape
    // `antfly_db_add_index_json` stores -- carries no such literal marker, so
    // the scan misses it even though a caller that opened this handle with
    // `generated_enrichment_replay` explicitly asked to resume exactly that
    // pending work. This function runs unconditionally after every open,
    // addIndex, and addEnrichment, so without preserving that intent here it
    // silently tears down and never rebuilds the runtime
    // `replayGeneratedEnrichmentsFromStoredDocs` depends on the very first
    // time this handle reconciles anything.
    var enrichments = try local_write.createManagedDbEnrichments(
        alloc,
        merged_json,
        database.backend_runtime,
        provider,
        null,
        null,
        "",
        null,
        null,
    );
    defer enrichments.deinit(alloc);
    var cfg = enrichments.takeConfig();
    cfg.enable_without_producers = cfg.enable_without_producers or handle.lite_generated_enrichment_replay;
    try database.reconfigureEnrichmentRuntime(cfg);
    database.setResolutionEmbedder(if (database.enrichment_runtime) |runtime| runtime.config.dense_embedder else null);
}

pub const stamped_generation_optimistic_attempts = 4;

/// Stamps `req` with the current identity generation and runs `query.run`
/// against it. Reads run concurrently with writes, so a batch can commit
/// between the stamp and the executor's re-check, which then rejects the
/// stale stamp with IdentityReadGenerationChanged. When the caller did not pin
/// a generation, restamp and retry, as the server's public query path does.
/// The last attempt holds the handle's write mutex so no write can intervene,
/// which bounds retries under sustained writes. A caller-pinned generation
/// that has gone stale is returned as an error without retrying.
pub fn runAtStampedGeneration(
    handle: *Handle,
    req: *db_mod.types.SearchRequest,
    query: anytype,
) !@TypeOf(query).Result {
    return runAtStampedGenerationWithOptions(handle, req, query, .{});
}

pub const StampedGenerationOptions = struct {
    /// Run the readable-lease hook for the stamped request. Paths whose export
    /// already ran a request-specific hook (dense search) turn this off.
    prepare: bool = true,
};

pub fn runAtStampedGenerationWithOptions(
    handle: *Handle,
    req: *db_mod.types.SearchRequest,
    query: anytype,
    comptime options: StampedGenerationOptions,
) !@TypeOf(query).Result {
    const pinned = req.identity_read_generation;
    var attempt: u32 = 0;
    while (true) : (attempt += 1) {
        const block_writers = pinned == null and attempt == stamped_generation_optimistic_attempts;
        const last = pinned != null or block_writers;
        // Same order as write exports (api_lock shared, then write_mutex), so
        // this cannot deadlock against them.
        if (block_writers) handle.write_mutex.lockUncancelable(handleLockIo());
        defer if (block_writers) handle.write_mutex.unlock(handleLockIo());
        req.identity_read_generation = pinned;
        try stampSearchRequestIdentityGeneration(handle, req);
        if (options.prepare) try handle.prepareSearchRequest(req.*);
        return query.run(handle, req.*) catch |err| {
            if (err == error.IdentityReadGenerationChanged and !last) continue;
            return err;
        };
    }
}

pub const LocalSearchQuery = struct {
    const Result = db_mod.types.SearchResult;
    pub fn run(_: LocalSearchQuery, handle: *Handle, req: db_mod.types.SearchRequest) !Result {
        return executeLocalSearch(handle, req);
    }
};

pub fn executeLocalSearch(handle: *Handle, req: db_mod.types.SearchRequest) !db_mod.types.SearchResult {
    if (comptime capi_build_options.linked_storage) {
        const request_json = try table_reads_api.encodeStorageKernelQueryRequest(handle.alloc, req);
        defer handle.alloc.free(request_json);
        var failure: kernel_owner_abi.FailureIdentity = .{};
        var cancellation = req.cancellation;
        const response = try local_query_client.executeJsonAlloc(
            handle.alloc,
            @ptrCast(handle.database()),
            "docs",
            request_json,
            .internal,
            antfly.local_query_controls.executionOptions(req),
            req.execution_deadline_ns,
            if (cancellation != null) @ptrCast(&cancellation.?) else null,
            if (cancellation != null) cancellationTokenRequested else null,
            &failure,
        );
        defer handle.alloc.free(response.json);
        var result = table_reads_api.parseStorageKernelSearchResult(handle.alloc, response.json) catch |err| {
            std.log.err("local query returned an invalid response wire error={s}", .{@errorName(err)});
            return error.InvalidBoundaryQueryResponse;
        };
        result.identity_read_generation = response.identity_read_generation;
        return result;
    }
    return try handle.database().*.search(handle.alloc, req);
}

pub fn cancellationTokenRequested(ctx: ?*anyopaque) callconv(.c) u8 {
    const token: *const db_mod.types.CancellationToken = @ptrCast(@alignCast(ctx orelse return 0));
    return @intFromBool(token.isCancelled());
}

pub fn localInferenceRuntimeAvailable() bool {
    return capi_build_options.inference_enabled and
        lite_backend.capabilitiesForProfile(.native).local_inference_runtime;
}

pub fn publishHandle(handle: *Handle) !*anyopaque {
    return handleRegistry().register(handle) catch |err| {
        closeHandle(handle);
        return err;
    };
}

/// How an export may overlap with other calls on the same handle.
pub const HandleAccess = enum {
    /// Pure queries. Any number run in parallel with each other and with a
    /// write; the storage layer serves them from pinned snapshots.
    read,
    /// Document writes, transactions, and storage rewrites (compact, vacuum).
    /// One at a time per handle, concurrent with reads.
    write,
    /// Enrichment drains, replay, and snapshot copies. One at a time per
    /// handle, concurrent with reads and writes, mirroring the background
    /// maintenance workers that already run alongside them.
    maintain,
    /// Schema, index, enrichment, range, and restore changes. Waits for
    /// in-flight calls and blocks new ones until done.
    exclusive,
};

/// Held for the duration of one export call; see `enterHandle`.
pub const HandleGuard = struct {
    handle: *Handle,
    slot: *HandleRegistry.Slot,
    parent_slot: ?*HandleRegistry.Slot = null,
    lock_handle: *Handle,
    access: HandleAccess,
    connection: ?*@import("lite_connection.zig").Connection = null,
    path_lease: ?lite_backend.native.PathWriterLock = null,
    connection_gate_held: bool = false,
    entry_error: ?capi.ErrorCode = null,
    entry_sequence: u64 = 0,
    pinned_store_readonly: ?bool = null,
    suspended_query_root: ?*Handle = null,

    /// Keep registry references and the provider snapshot alive, releasing both
    /// process and file locks while inference runs on an owned request plan.
    pub fn suspendQuery(self: *HandleGuard) void {
        const connection = self.connection orelse return;
        std.debug.assert(self.connection_gate_held and self.entry_error == null);
        const root = connection.current.?;
        root.query_borrows += 1;
        self.suspended_query_root = root;
        // Owned memory plans can release after catalog provisioning. Publish
        // those already-committed primary changes before dropping the lease.
        if (self.path_lease != null and root.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence != self.entry_sequence) connection.notifyMutation();
        connection.detach();
        if (self.path_lease) |lease| {
            var owned = lease;
            owned.close();
        }
        self.path_lease = null;
        self.connection_gate_held = false;
        connection.gate.mutex.unlock(handleLockIo());
        connection.mutex.unlock(handleLockIo());
    }

    pub fn resumeQuery(self: *HandleGuard, ptr: ?*anyopaque) bool {
        return self.resumeAccess(ptr, .read);
    }
    pub fn resumeAccess(self: *HandleGuard, ptr: ?*anyopaque, access: HandleAccess) bool {
        if (self.suspended_query_root == null) return true;
        // Original registry references keep the connection/owner alive.
        const next = enterHandle(ptr, access) orelse return false;
        const connection = self.connection.?;
        if (!next.connection_gate_held) connection.gate.mutex.lockUncancelable(handleLockIo());
        self.suspended_query_root.?.query_borrows -= 1;
        connection.collectRetired();
        if (!next.connection_gate_held) connection.gate.mutex.unlock(handleLockIo());
        if (self.parent_slot) |slot| HandleRegistry.leave(slot);
        HandleRegistry.leave(self.slot);
        self.* = next;
        return true;
    }

    pub fn leave(self: HandleGuard) void {
        const io = handleLockIo();
        if (self.suspended_query_root) |root| {
            const connection = self.connection.?;
            connection.gate.mutex.lockUncancelable(io);
            root.query_borrows -= 1;
            connection.collectRetired();
            connection.gate.mutex.unlock(io);
            if (self.parent_slot) |slot| HandleRegistry.leave(slot);
            HandleRegistry.leave(self.slot);
            return;
        }
        if (self.connection) |connection| {
            if (self.pinned_store_readonly) |was_readonly| {
                const store = connection.current.?.owned_lite_backend.?.native_docstore.?;
                store.read_only = was_readonly;
                store.file.read_only = was_readonly;
            }
            if (self.entry_error == null and self.path_lease != null) {
                if (connection.current) |root| {
                    if (root.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence != self.entry_sequence) connection.notifyMutation();
                }
            }
            if (self.connection_gate_held) {
                connection.detach();
                connection.collectRetired();
            }
            if (self.path_lease) |lease| {
                var owned = lease;
                owned.close();
            }
            if (self.connection_gate_held) connection.gate.mutex.unlock(io);
            connection.mutex.unlock(io);
            if (self.parent_slot) |slot| HandleRegistry.leave(slot);
            HandleRegistry.leave(self.slot);
            return;
        }
        switch (self.access) {
            .read => self.lock_handle.api_lock.unlockShared(io),
            .write => {
                self.lock_handle.write_mutex.unlock(io);
                self.lock_handle.api_lock.unlockShared(io);
            },
            .maintain => {
                self.lock_handle.maintenance_mutex.unlock(io);
                self.lock_handle.api_lock.unlockShared(io);
            },
            .exclusive => self.lock_handle.api_lock.unlock(io),
        }
        if (self.parent_slot) |slot| HandleRegistry.leave(slot);
        HandleRegistry.leave(self.slot);
    }
};

/// Entry point for every export that takes a DB handle. Returns null for a
/// null handle or one that is being closed. Must be taken exactly once per
/// export, at entry: the lock is not reentrant, so internal helpers must not
/// call it again.
pub fn enterHandle(ptr: ?*anyopaque, access: HandleAccess) ?HandleGuard {
    return enterHandleInternal(ptr, access, false);
}

/// Cursor continuation uses a previously pinned immutable generation.
pub fn enterHandlePinned(ptr: ?*anyopaque, access: HandleAccess) ?HandleGuard {
    return enterHandleInternal(ptr, access, true);
}

fn enterHandleInternal(ptr: ?*anyopaque, access: HandleAccess, pinned: bool) ?HandleGuard {
    const handle, const slot = handleRegistry().enter(ptr) orelse return null;
    var parent_slot: ?*HandleRegistry.Slot = null;
    const lock_handle = if (handle.parent_id) |id| blk: {
        const parent, const parent_entry = handleRegistry().enter(id) orelse {
            HandleRegistry.leave(slot);
            return null;
        };
        parent_slot = parent_entry;
        break :blk parent;
    } else handle;
    if (lock_handle.lite_connection) |connection| {
        connection.mutex.lockUncancelable(handleLockIo());
        var guard: HandleGuard = .{ .handle = handle, .lock_handle = lock_handle, .parent_slot = parent_slot, .slot = slot, .access = access, .connection = connection };
        if (!pinned) {
            guard.path_lease = connection.acquire() catch |err| {
                guard.entry_error = capi.mapError(err);
                return guard;
            };
        } else connection.gate.mutex.lockUncancelable(handleLockIo());
        guard.connection_gate_held = true;
        const root = (if (pinned) connection.pin() else connection.refresh()) catch |err| {
            guard.entry_error = capi.mapError(err);
            return guard;
        };
        guard.handle = root;
        if (pinned) {
            // Fetch/close may release the final segment reader and invoke its
            // cleanup callback. No pinned call owns a writer lease, so cleanup
            // must queue its deletion for a later maintenance boundary.
            const store = root.owned_lite_backend.?.native_docstore.?;
            guard.pinned_store_readonly = store.read_only;
            store.read_only = true;
            store.file.read_only = true;
        }
        guard.entry_sequence = root.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence;
        if (handle.parent_id != null) {
            const database = @import("tables.zig").get(root, handle.selected_table_name orelse "default") catch |err| {
                guard.entry_error = capi.mapError(err);
                return guard;
            };
            if (database.core.identity_namespace.table_id != handle.selected_table_id) {
                guard.entry_error = .invalid_argument;
                return guard;
            }
            handle.selected_db = database;
            handle.parent_handle = root;
            guard.handle = handle;
        }
        if (!pinned) @import("sql_commit.zig").recover(root) catch |err| {
            guard.entry_error = capi.mapError(err);
        };
        if (!pinned and guard.entry_error == null) @import("sql_ddl.zig").recover(root) catch |err| {
            guard.entry_error = capi.mapError(err);
        };
        return guard;
    }
    const effective_access = if (lock_handle.embedded_path != null) HandleAccess.exclusive else access;
    const io = handleLockIo();
    switch (effective_access) {
        .read => lock_handle.api_lock.lockSharedUncancelable(io),
        .write => {
            lock_handle.api_lock.lockSharedUncancelable(io);
            lock_handle.write_mutex.lockUncancelable(io);
        },
        .maintain => {
            lock_handle.api_lock.lockSharedUncancelable(io);
            lock_handle.maintenance_mutex.lockUncancelable(io);
        },
        .exclusive => lock_handle.api_lock.lockUncancelable(io),
    }
    const guard: HandleGuard = .{ .handle = handle, .lock_handle = lock_handle, .parent_slot = parent_slot, .slot = slot, .access = effective_access };
    if (lock_handle.embedded_path != null) {
        @import("sql_commit.zig").recover(lock_handle) catch {
            guard.leave();
            return null;
        };
        @import("sql_ddl.zig").recover(lock_handle) catch {
            guard.leave();
            return null;
        };
    }
    return guard;
}

pub fn beginWithIdAndParticipants(
    handle: *Handle,
    txn_id: transactions_mod.TxnId,
    timestamp_ns: u64,
    participants_ptr: ?[*]const capi.Slice,
    participant_count: usize,
) !void {
    const participants = try handle.alloc.alloc([]const u8, participant_count);
    defer handle.alloc.free(participants);
    for (participants, 0..) |*entry, i| {
        entry.* = participants_ptr.?[i].bytes();
    }
    _ = try handle.database().*.beginTransactionWithIdAndParticipants(txn_id, timestamp_ns, participants);
}

pub fn writeIntentsInternal(
    handle: *Handle,
    txn_id: transactions_mod.TxnId,
    writes_ptr: ?[*]const capi.WriteIntent,
    write_count: usize,
    predicates_ptr: ?[*]const capi.VersionPredicate,
    predicate_count: usize,
) !void {
    var writes = try handle.alloc.alloc(db_mod.types.TransactionWrite, write_count);
    defer handle.alloc.free(writes);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(handle.alloc);
    var predicates = try handle.alloc.alloc(db_mod.types.TransactionVersionPredicate, predicate_count);
    defer handle.alloc.free(predicates);

    var write_len: usize = 0;
    for (0..write_count) |i| {
        const src = writes_ptr.?[i];
        if (src.is_delete) {
            try deletes.append(handle.alloc, src.key.bytes());
        } else {
            writes[write_len] = .{
                .key = src.key.bytes(),
                .value = src.value.bytes(),
            };
            write_len += 1;
        }
    }
    for (0..predicate_count) |i| {
        predicates[i] = .{
            .key = predicates_ptr.?[i].key.bytes(),
            .expected_version = predicates_ptr.?[i].expected_version,
        };
    }

    try handle.database().*.writeTransaction(txn_id, .{
        .writes = writes[0..write_len],
        .deletes = deletes.items,
        .predicates = predicates,
    });
}

pub fn batchInternal(
    handle: *Handle,
    writes_ptr: ?[*]const capi.WriteIntent,
    write_count: usize,
    predicates_ptr: ?[*]const capi.VersionPredicate,
    predicate_count: usize,
    timestamp_ns: u64,
    sync_level: u8,
) !void {
    var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    defer writes.deinit(handle.alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(handle.alloc);
    var predicates = try handle.alloc.alloc(db_mod.types.TransactionVersionPredicate, predicate_count);
    defer handle.alloc.free(predicates);

    for (0..write_count) |i| {
        const src = writes_ptr.?[i];
        if (src.is_delete) {
            try deletes.append(handle.alloc, src.key.bytes());
        } else {
            try writes.append(handle.alloc, .{
                .key = src.key.bytes(),
                .value = src.value.bytes(),
            });
        }
    }
    for (0..predicate_count) |i| {
        predicates[i] = .{
            .key = predicates_ptr.?[i].key.bytes(),
            .expected_version = predicates_ptr.?[i].expected_version,
        };
    }

    const level: db_mod.types.SyncLevel = switch (sync_level) {
        0 => .write,
        1 => .full_index,
        else => return error.InvalidArgument,
    };

    try handle.database().*.batch(.{
        .writes = writes.items,
        .deletes = deletes.items,
        .predicates = predicates,
        .timestamp_ns = timestamp_ns,
        .sync_level = level,
    });
}

pub fn stringifyJson(value: anytype) !capi.Buffer {
    const bytes = try std.fmt.allocPrint(std.heap.c_allocator, "{f}", .{std.json.fmt(value, .{})});
    return .{
        .ptr = bytes.ptr,
        .len = bytes.len,
    };
}

pub fn dupBase64(alloc: Allocator, bytes: []const u8) ![]u8 {
    const size = std.base64.standard.Encoder.calcSize(bytes.len);
    const out = try alloc.alloc(u8, size);
    _ = std.base64.standard.Encoder.encode(out, bytes);
    return out;
}

pub fn decodeBase64Alloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    const size = try std.base64.standard.Decoder.calcSizeForSlice(encoded);
    const out = try alloc.alloc(u8, size);
    errdefer alloc.free(out);
    try std.base64.standard.Decoder.decode(out, encoded);
    return out;
}

pub fn parseEnrichmentKind(kind: []const u8) ?db_mod.types.EnrichmentKind {
    if (std.mem.eql(u8, kind, "group")) return .group;
    if (std.mem.eql(u8, kind, "chunk")) return .chunk;
    if (std.mem.eql(u8, kind, "asset")) return .asset;
    if (std.mem.eql(u8, kind, "embedding")) return .embedding;
    return null;
}

pub fn graphFreeEdges(alloc: Allocator, edges: []graph_mod.Edge) void {
    // GraphIndex.freeEdges already frees both each edge's owned fields and
    // the slice itself. Freeing `edges` again here double-frees it: harmless
    // for the len==0 case (many allocators no-op an empty-slice free), but a
    // real heap corruption once a query returns actual edges -- see
    // antfly_db_get_edges_json below, the only caller.
    graph_mod.GraphIndex.freeEdges(alloc, edges);
}

pub fn traversalFreeResults(alloc: Allocator, results: []traversal_mod.TraversalResult) void {
    traversal_mod.freeOwnedResults(alloc, results);
}

pub const JsonRange = struct {
    start_b64: []u8,
    end_b64: []u8,

    pub fn init(alloc: Allocator, byte_range: db_mod.types.ByteRange) !JsonRange {
        return .{
            .start_b64 = try dupBase64(alloc, byte_range.start),
            .end_b64 = try dupBase64(alloc, byte_range.end),
        };
    }

    pub fn deinit(self: *JsonRange, alloc: Allocator) void {
        alloc.free(self.start_b64);
        alloc.free(self.end_b64);
        self.* = undefined;
    }
};

pub const JsonSplitState = struct {
    phase: u8,
    split_key_b64: []u8,
    new_shard_id: u64,
    started_at: u64,
    original_range_end_b64: []u8,

    pub fn init(alloc: Allocator, state: db_mod.types.SplitState) !JsonSplitState {
        return .{
            .phase = @backingInt(state.phase),
            .split_key_b64 = try dupBase64(alloc, state.split_key),
            .new_shard_id = state.new_shard_id,
            .started_at = state.started_at,
            .original_range_end_b64 = try dupBase64(alloc, state.original_range_end),
        };
    }

    pub fn deinit(self: *JsonSplitState, alloc: Allocator) void {
        alloc.free(self.split_key_b64);
        alloc.free(self.original_range_end_b64);
        self.* = undefined;
    }
};

pub const JsonSplitDeltaWrite = struct {
    key_b64: []u8,
    value_b64: []u8,

    pub fn init(alloc: Allocator, write: db_mod.types.BatchWrite) !JsonSplitDeltaWrite {
        return .{
            .key_b64 = try dupBase64(alloc, write.key),
            .value_b64 = try dupBase64(alloc, write.value),
        };
    }

    pub fn deinit(self: *JsonSplitDeltaWrite, alloc: Allocator) void {
        alloc.free(self.key_b64);
        alloc.free(self.value_b64);
        self.* = undefined;
    }
};

pub const JsonSplitDeltaEntry = struct {
    sequence: u64,
    timestamp: u64,
    writes: []JsonSplitDeltaWrite,
    deletes_b64: [][]u8,

    pub fn init(alloc: Allocator, entry: db_mod.types.SplitDeltaEntry) !JsonSplitDeltaEntry {
        var writes = try alloc.alloc(JsonSplitDeltaWrite, entry.writes.len);
        errdefer alloc.free(writes);
        var write_count: usize = 0;
        errdefer {
            for (writes[0..write_count]) |*write| write.deinit(alloc);
        }
        for (entry.writes, 0..) |write, i| {
            writes[i] = try JsonSplitDeltaWrite.init(alloc, write);
            write_count += 1;
        }

        var deletes = try alloc.alloc([]u8, entry.deletes.len);
        errdefer alloc.free(deletes);
        var delete_count: usize = 0;
        errdefer {
            for (deletes[0..delete_count]) |item| alloc.free(item);
        }
        for (entry.deletes, 0..) |key, i| {
            deletes[i] = try dupBase64(alloc, key);
            delete_count += 1;
        }

        return .{
            .sequence = entry.sequence,
            .timestamp = entry.timestamp,
            .writes = writes,
            .deletes_b64 = deletes,
        };
    }

    pub fn deinit(self: *JsonSplitDeltaEntry, alloc: Allocator) void {
        for (self.writes) |*write| write.deinit(alloc);
        if (self.writes.len > 0) alloc.free(self.writes);
        for (self.deletes_b64) |item| alloc.free(item);
        if (self.deletes_b64.len > 0) alloc.free(self.deletes_b64);
        self.* = undefined;
    }
};

pub const JsonIndexConfig = struct {
    name: []const u8,
    kind: []const u8,
    config_json: []const u8,
};

pub const JsonScanHash = struct {
    id_b64: []u8,
    hash: u64,

    pub fn init(alloc: Allocator, item: db_mod.types.ScanHash) !JsonScanHash {
        return .{
            .id_b64 = try dupBase64(alloc, item.id),
            .hash = item.hash,
        };
    }

    pub fn deinit(self: *JsonScanHash, alloc: Allocator) void {
        alloc.free(self.id_b64);
        self.* = undefined;
    }
};

pub const JsonScanDocument = struct {
    id_b64: []u8,
    json: []const u8,

    pub fn init(alloc: Allocator, item: db_mod.types.ScanDocument) !JsonScanDocument {
        return .{
            .id_b64 = try dupBase64(alloc, item.id),
            .json = item.json,
        };
    }

    pub fn deinit(self: *JsonScanDocument, alloc: Allocator) void {
        alloc.free(self.id_b64);
        self.* = undefined;
    }
};

pub const JsonScanResult = struct {
    hashes: []JsonScanHash,
    documents: []JsonScanDocument,
};

pub const JsonDBStats = struct {
    doc_count: u64,
    index_count: u32,
    indexes_available: bool,
    indexes: []JsonDBIndexStats,
    repair_degraded: bool,
    repair_issue_count: u64,
    repair_summary_ready: bool,
    repair_issue_count_estimated: bool,
    enrichment: JsonEnrichmentStats,
    ttl_cleanup: JsonTTLCleanupStats,
    transaction_recovery: JsonTransactionRecoveryStats,
    text_merge: JsonTextMergeStats,
    term_doc_freq_cache_hits: u64,
    term_doc_freq_cache_misses: u64,
};

pub const JsonDBIndexStats = struct {
    name: []const u8,
    kind: []const u8,
    replay_applied_sequence: u64,
    replay_target_sequence: u64,
    replay_catch_up_required: bool,
    catch_up_active: bool,
    catch_up_phase: []const u8,
    doc_count: u64,
    term_count: u64,
    edge_count: u64,
    graph_counts_pending: bool,
    node_count: u64,
    repair_degraded: bool,
    repair_issue_count: u64,
    repair_summary_ready: bool,
    repair_issue_count_estimated: bool,
    // Durable per-document generation-outcome coverage (produced / skipped /
    // terminal_failed markers), so an embedded consumer can report honest
    // coverage: documents that failed non-retryably are settled failures,
    // not pending work. Mirrors the server's index-status coverage block.
    coverage_produced_count: u64,
    coverage_skipped_count: u64,
    coverage_terminal_failed_count: u64,
    coverage_summary_ready: bool,
};

pub const JsonEnrichmentStats = struct {
    last_error_code: db_mod.types.RuntimeErrorName = .{},
    enabled: bool,
    lease_owned: bool,
    has_lease: bool,
    acquisition_count: u64,
    lease_acquire_failures: u64,
    lost_leases: u64,
    last_acquired_ms: u64,
    target_sequence: u64,
    applied_sequence: u64,
    processed_requests: u64,
    error_count: u64,
    retryable_error_count: u64,
    // Durable count of requests parked non-retryably (terminal disposition);
    // per-document terminal state lives in the index coverage counters.
    fatal_error_count: u64,
    retrying: bool,
    worker_failed: bool,
    stalled: bool,
    stall_reason: []const u8,
    skip_by_hash_count: u64,
    skipped_source_count: u64,
    codec_decode_failures: u64,
    dense_artifact_bytes_written: u64,
    sparse_artifact_bytes_written: u64,
    chunk_artifact_bytes_written: u64,
    artifact_bytes_written: u64,
};

pub const JsonTTLCleanupStats = struct {
    enabled: bool,
    lease_owned: bool,
    has_lease: bool,
    acquisition_count: u64,
    runs: u64,
    scanned_timestamps: u64,
    deleted_docs: u64,
    last_run_ns: u64,
    error_count: u64,
    lease_acquire_failures: u64,
    lost_leases: u64,
    last_acquired_ms: u64,
};

pub const JsonTransactionRecoveryStats = struct {
    enabled: bool,
    lease_owned: bool,
    has_lease: bool,
    acquisition_count: u64,
    lease_acquire_failures: u64,
    lost_leases: u64,
    last_acquired_ms: u64,
    runs: u64,
    scanned_records: u64,
    auto_aborted: u64,
    resolved_finalized: u64,
    cleaned_records: u64,
    kept_recent_pending: u64,
    deferred_unresolved: u64,
    notification_attempts: u64,
    notification_successes: u64,
    notification_failures: u64,
    last_run_ns: u64,
    error_count: u64,
};

pub const JsonTextMergeStats = struct {
    enabled: bool,
    active_indexes: u64,
    active_segments: u64,
    max_active_segments_per_index: u64,
    pending_indexes: u64,
    pending_segments: u64,
    pending_bytes: u64,
    in_flight_merges: u64,
    in_flight_segments: u64,
    completed_merges: u64,
    skipped_stale_merges: u64,
    failed_merges: u64,
    quarantined_merges: u64,
    quarantined_segments: u64,
    last_merge_error: db_mod.types.RuntimeErrorName,
    backpressure_events: u64,
    backpressure_ns: u64,
    max_pending_segments: u64,
    max_pending_bytes: u64,
};

pub const JsonChunkHit = struct {
    id_b64: []u8,
    score: ?f32 = null,
    stored_json: ?[]const u8 = null,
    artifact_ref: ?JsonArtifactRef = null,

    pub fn init(alloc: Allocator, hit: db_mod.types.ChunkHit) !JsonChunkHit {
        return .{
            .id_b64 = try dupBase64(alloc, hit.id),
            .score = hit.score,
            .stored_json = hit.stored_data,
            .artifact_ref = if (hit.artifact_ref) |artifact_ref| try JsonArtifactRef.init(alloc, artifact_ref) else null,
        };
    }

    pub fn deinit(self: *JsonChunkHit, alloc: Allocator) void {
        alloc.free(self.id_b64);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        self.* = undefined;
    }
};

pub const JsonSearchHit = struct {
    id_b64: []u8,
    score: ?f32 = null,
    stored_json: ?[]const u8 = null,
    artifact_ref: ?JsonArtifactRef = null,
    chunk_hits: []JsonChunkHit = &.{},

    pub fn init(alloc: Allocator, hit: db_mod.types.SearchHit) !JsonSearchHit {
        var chunk_hits = try alloc.alloc(JsonChunkHit, hit.chunk_hits.len);
        errdefer alloc.free(chunk_hits);
        var count: usize = 0;
        errdefer {
            for (chunk_hits[0..count]) |*item| item.deinit(alloc);
        }
        for (hit.chunk_hits, 0..) |chunk, i| {
            chunk_hits[i] = try JsonChunkHit.init(alloc, chunk);
            count += 1;
        }
        return .{
            .id_b64 = try dupBase64(alloc, hit.id),
            .score = hit.score,
            .stored_json = hit.stored_data,
            .artifact_ref = if (hit.artifact_ref) |artifact_ref| try JsonArtifactRef.init(alloc, artifact_ref) else null,
            .chunk_hits = chunk_hits,
        };
    }

    pub fn deinit(self: *JsonSearchHit, alloc: Allocator) void {
        alloc.free(self.id_b64);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        for (self.chunk_hits) |*item| item.deinit(alloc);
        if (self.chunk_hits.len > 0) alloc.free(self.chunk_hits);
        self.* = undefined;
    }
};

pub const JsonSearchResult = struct {
    total_hits: u32,
    identity_read_generation: ?u64 = null,
    hits: []JsonSearchHit,
    graph_results: []JsonGraphSearchResult = &.{},
    aggregations: []JsonSearchAggregationResult = &.{},
};

pub const JsonAggregateHitsRequest = struct {
    index_name: []const u8 = "",
    hit_ids_b64: []const []const u8 = &.{},
    identity_read_generation: ?u64 = null,
    aggregations: []const JsonSearchAggregationRequest = &.{},
};

pub const JsonGraphNodeSelectorRequest = struct {
    keys: []const []const u8 = &.{},
    result_ref: []const u8 = "",
    limit: u32 = 0,
};

pub const JsonGraphQueryRequest = struct {
    edge_filter: ?std.json.Value = null,
    name: []const u8,
    type: []const u8,
    index_name: []const u8,
    start_nodes: JsonGraphNodeSelectorRequest,
    target_nodes: ?JsonGraphNodeSelectorRequest = null,
    edge_types: []const []const u8 = &.{},
    direction: []const u8 = "out",
    max_depth: u32 = 3,
    max_results: u32 = 100,
    min_weight: f64 = 0.0,
    max_weight: f64 = 0.0,
    deduplicate: bool = true,
    include_paths: bool = false,
    weight_mode: []const u8 = "min_hops",
    k: u32 = 1,
};

pub const JsonNamedGraphInputSetRequest = struct {
    name: []const u8,
    hit_ids_b64: []const []const u8 = &.{},
    total_hits: u32 = 0,
};

pub const JsonGraphSearchResult = struct {
    name: []u8,
    total_hits: u32,
    identity_read_generation: ?u64 = null,
    nodes: []JsonGraphNode,
    paths: []JsonPath = &.{},
    hits: []JsonSearchHit,

    pub fn init(alloc: Allocator, result: db_mod.types.GraphSearchResult, identity_read_generation: ?u64) !JsonGraphSearchResult {
        var nodes = try alloc.alloc(JsonGraphNode, result.nodes.len);
        errdefer alloc.free(nodes);
        var node_count: usize = 0;
        errdefer {
            for (nodes[0..node_count]) |*item| item.deinit(alloc);
        }
        for (result.nodes, 0..) |node, i| {
            nodes[i] = try JsonGraphNode.init(alloc, node);
            node_count += 1;
        }

        var paths = try alloc.alloc(JsonPath, result.paths.len);
        errdefer alloc.free(paths);
        var path_count: usize = 0;
        errdefer {
            for (paths[0..path_count]) |*item| item.deinit(alloc);
        }
        for (result.paths, 0..) |path, i| {
            paths[i] = try JsonPath.init(alloc, path);
            path_count += 1;
        }

        var hits = try alloc.alloc(JsonSearchHit, result.hits.len);
        errdefer alloc.free(hits);
        var count: usize = 0;
        errdefer {
            for (hits[0..count]) |*item| item.deinit(alloc);
        }
        for (result.hits, 0..) |hit, i| {
            hits[i] = try JsonSearchHit.init(alloc, hit);
            count += 1;
        }
        return .{
            .name = try alloc.dupe(u8, result.name),
            .total_hits = result.total_hits,
            .identity_read_generation = identity_read_generation,
            .nodes = nodes,
            .paths = paths,
            .hits = hits,
        };
    }

    pub fn deinit(self: *JsonGraphSearchResult, alloc: Allocator) void {
        alloc.free(self.name);
        for (self.nodes) |*item| item.deinit(alloc);
        if (self.nodes.len > 0) alloc.free(self.nodes);
        for (self.paths) |*item| item.deinit(alloc);
        if (self.paths.len > 0) alloc.free(self.paths);
        for (self.hits) |*item| item.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
        self.* = undefined;
    }
};

pub fn toAggregationRequest(
    alloc: Allocator,
    requests: []const JsonSearchAggregationRequest,
) ![]aggregations_mod.SearchAggregationRequest {
    const out = try alloc.alloc(aggregations_mod.SearchAggregationRequest, requests.len);
    errdefer alloc.free(out);
    for (requests, 0..) |request, i| {
        const ranges = try alloc.alloc(aggregations_mod.NumericRangeRequest, request.ranges.len);
        errdefer alloc.free(ranges);
        for (request.ranges, 0..) |item, j| {
            ranges[j] = .{ .name = item.name, .start = item.start, .end = item.end };
        }
        const date_ranges = try alloc.alloc(aggregations_mod.DateRangeRequest, request.date_ranges.len);
        errdefer alloc.free(date_ranges);
        for (request.date_ranges, 0..) |item, j| {
            date_ranges[j] = .{ .name = item.name, .start = item.start, .end = item.end };
        }
        const distance_ranges = try alloc.alloc(aggregations_mod.DistanceRangeRequest, request.distance_ranges.len);
        errdefer alloc.free(distance_ranges);
        for (request.distance_ranges, 0..) |item, j| {
            distance_ranges[j] = .{ .name = item.name, .from = item.from, .to = item.to };
        }
        const nested = try toAggregationRequest(alloc, request.aggregations);
        out[i] = .{
            .name = request.name,
            .type = request.type,
            .field = request.field,
            .size = request.size,
            .interval = request.interval,
            .calendar_interval = request.calendar_interval,
            .fixed_interval = request.fixed_interval,
            .min_doc_count = request.min_doc_count,
            .significance_algorithm = request.significance_algorithm,
            .background_query = if (request.background_query_type.len == 0)
                null
            else if (std.mem.eql(u8, request.background_query_type, "match_all"))
                .{ .match_all = {} }
            else if (std.mem.eql(u8, request.background_query_type, "match"))
                .{ .match = .{
                    .field = request.background_field,
                    .text = request.background_text,
                } }
            else if (std.mem.eql(u8, request.background_query_type, "term"))
                .{ .term = .{
                    .field = request.background_field,
                    .term = request.background_text,
                } }
            else
                return error.InvalidArgument,
            .bucket_path = request.bucket_path,
            .sort_order = request.sort_order,
            .from = request.from,
            .window = request.window,
            .gap_policy = request.gap_policy,
            .term_prefix = request.term_prefix,
            .term_pattern = request.term_pattern,
            .ranges = ranges,
            .date_ranges = date_ranges,
            .distance_ranges = distance_ranges,
            .center_lat = request.center_lat,
            .center_lon = request.center_lon,
            .distance_unit = request.distance_unit,
            .geohash_precision = request.geohash_precision,
            .aggregations = nested,
        };
    }
    return out;
}

pub fn toJsonAggregationResults(
    alloc: Allocator,
    results: []aggregations_mod.SearchAggregationResult,
) ![]JsonSearchAggregationResult {
    const out = try alloc.alloc(JsonSearchAggregationResult, results.len);
    errdefer alloc.free(out);
    for (results, 0..) |result, i| {
        const buckets = try alloc.alloc(JsonSearchAggregationBucket, result.buckets.len);
        errdefer alloc.free(buckets);
        for (result.buckets, 0..) |bucket, j| {
            buckets[j] = .{
                .key_json = try alloc.dupe(u8, bucket.key_json),
                .count = bucket.count,
                .score = bucket.score,
                .bg_count = bucket.bg_count,
                .aggregations = try toJsonAggregationResults(alloc, bucket.aggregations),
            };
        }
        out[i] = .{
            .name = result.name,
            .field = result.field,
            .type = result.type,
            .value_json = if (result.value_json) |value| try alloc.dupe(u8, value) else null,
            .metadata_json = if (result.metadata_json) |value| try alloc.dupe(u8, value) else null,
            .buckets = buckets,
        };
    }
    return out;
}

pub fn artifactKindLabel(kind: db_mod.types.ArtifactKind) []const u8 {
    return switch (kind) {
        .chunk => "chunk",
        .asset => "asset",
        .embedding => "embedding",
    };
}

pub const JsonArtifactSourceRef = struct {
    kind: []const u8,
    name: []const u8,
    chunk_id: ?u32 = null,

    pub fn init(source: db_mod.types.ArtifactSourceRef) JsonArtifactSourceRef {
        return .{
            .kind = artifactKindLabel(source.kind),
            .name = source.name,
            .chunk_id = source.chunk_id,
        };
    }
};

pub const JsonArtifactRef = struct {
    document_id_b64: []u8,
    name: []const u8,
    kind: []const u8,
    chunk_id: ?u32 = null,
    source: ?JsonArtifactSourceRef = null,

    pub fn init(alloc: Allocator, artifact_ref: db_mod.types.ArtifactRef) !JsonArtifactRef {
        return .{
            .document_id_b64 = try dupBase64(alloc, artifact_ref.document_id),
            .name = artifact_ref.name,
            .kind = artifactKindLabel(artifact_ref.kind),
            .chunk_id = artifact_ref.chunk_id,
            .source = if (artifact_ref.source) |source| JsonArtifactSourceRef.init(source) else null,
        };
    }

    pub fn deinit(self: *JsonArtifactRef, alloc: Allocator) void {
        alloc.free(self.document_id_b64);
        self.* = undefined;
    }
};

pub const JsonArtifactWrite = struct {
    id_b64: []u8,
    value_b64: []u8,
    artifact_ref: JsonArtifactRef,

    pub fn init(alloc: Allocator, write: db_mod.types.ArtifactWrite) !JsonArtifactWrite {
        return .{
            .id_b64 = try dupBase64(alloc, write.id),
            .value_b64 = try dupBase64(alloc, write.value),
            .artifact_ref = try JsonArtifactRef.init(alloc, write.artifact_ref),
        };
    }

    pub fn deinit(self: *JsonArtifactWrite, alloc: Allocator) void {
        alloc.free(self.id_b64);
        alloc.free(self.value_b64);
        self.artifact_ref.deinit(alloc);
        self.* = undefined;
    }
};

pub const JsonDenseEnrichmentWrite = struct {
    index_name: []const u8,
    doc_key_b64: []u8,
    artifact_id_b64: ?[]u8 = null,
    artifact_ref: ?JsonArtifactRef = null,
    vector: []const f32,

    pub fn init(alloc: Allocator, write: db_mod.types.EnrichmentDenseEmbeddingWrite) !JsonDenseEnrichmentWrite {
        return .{
            .index_name = write.index_name,
            .doc_key_b64 = try dupBase64(alloc, write.doc_key),
            .artifact_id_b64 = if (write.artifact_id) |artifact_id| try dupBase64(alloc, artifact_id) else null,
            .artifact_ref = if (write.artifact_ref) |artifact_ref| try JsonArtifactRef.init(alloc, artifact_ref) else null,
            .vector = write.vector,
        };
    }

    pub fn deinit(self: *JsonDenseEnrichmentWrite, alloc: Allocator) void {
        alloc.free(self.doc_key_b64);
        if (self.artifact_id_b64) |artifact_id_b64| alloc.free(artifact_id_b64);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        self.* = undefined;
    }
};

pub const JsonSparseEnrichmentWrite = struct {
    index_name: []const u8,
    doc_key_b64: []u8,
    indices: []const u32,
    values: []const f32,

    pub fn init(alloc: Allocator, write: db_mod.types.EnrichmentSparseEmbeddingWrite) !JsonSparseEnrichmentWrite {
        return .{
            .index_name = write.index_name,
            .doc_key_b64 = try dupBase64(alloc, write.doc_key),
            .indices = write.indices,
            .values = write.values,
        };
    }

    pub fn deinit(self: *JsonSparseEnrichmentWrite, alloc: Allocator) void {
        alloc.free(self.doc_key_b64);
        self.* = undefined;
    }
};

pub const JsonGraphWrite = struct {
    edge_id: []const u8,
    owner_document: []const u8,
    index_name: []const u8,
    source_b64: []u8,
    target_b64: []u8,
    edge_type: []const u8,
    weight: f64,
    created_at: u64,
    updated_at: u64,
    metadata_json: []const u8,

    pub fn init(alloc: Allocator, write: db_mod.types.GraphEdgeWrite) !JsonGraphWrite {
        return .{
            .edge_id = write.edge_id,
            .owner_document = write.owner_document,
            .index_name = write.index_name,
            .source_b64 = try dupBase64(alloc, write.source),
            .target_b64 = try dupBase64(alloc, write.target),
            .edge_type = write.edge_type,
            .weight = write.weight,
            .created_at = write.created_at,
            .updated_at = write.updated_at,
            .metadata_json = write.metadata_json,
        };
    }

    pub fn deinit(self: *JsonGraphWrite, alloc: Allocator) void {
        alloc.free(self.source_b64);
        alloc.free(self.target_b64);
        self.* = undefined;
    }
};

pub const JsonDocumentEnrichmentWrite = struct {
    key_b64: []u8,
    value_b64: []u8,
    target_index_names: [][]const u8,

    pub fn init(alloc: Allocator, write: db_mod.types.EnrichmentDocumentWrite) !JsonDocumentEnrichmentWrite {
        const target_index_names = try alloc.alloc([]const u8, write.target_index_names.len);
        errdefer alloc.free(target_index_names);
        for (write.target_index_names, 0..) |name, i| target_index_names[i] = name;
        return .{
            .key_b64 = try dupBase64(alloc, write.key),
            .value_b64 = try dupBase64(alloc, write.value),
            .target_index_names = target_index_names,
        };
    }

    pub fn deinit(self: *JsonDocumentEnrichmentWrite, alloc: Allocator) void {
        alloc.free(self.key_b64);
        alloc.free(self.value_b64);
        if (self.target_index_names.len > 0) alloc.free(self.target_index_names);
        self.* = undefined;
    }
};

pub const JsonExtractEnrichmentsResult = struct {
    dense_embeddings: []JsonDenseEnrichmentWrite,
    sparse_embeddings: []JsonSparseEnrichmentWrite,
    graph_writes: []JsonGraphWrite,

    pub fn deinit(self: *JsonExtractEnrichmentsResult, alloc: Allocator) void {
        for (self.dense_embeddings) |*item| item.deinit(alloc);
        if (self.dense_embeddings.len > 0) alloc.free(self.dense_embeddings);
        for (self.sparse_embeddings) |*item| item.deinit(alloc);
        if (self.sparse_embeddings.len > 0) alloc.free(self.sparse_embeddings);
        for (self.graph_writes) |*item| item.deinit(alloc);
        if (self.graph_writes.len > 0) alloc.free(self.graph_writes);
        self.* = undefined;
    }
};

pub const JsonComputeEnrichmentsResult = struct {
    artifact_writes: []JsonArtifactWrite,
    documents: []JsonDocumentEnrichmentWrite,
    dense_embeddings: []JsonDenseEnrichmentWrite,
    failed_keys_b64: [][]u8,

    pub fn deinit(self: *JsonComputeEnrichmentsResult, alloc: Allocator) void {
        for (self.artifact_writes) |*item| item.deinit(alloc);
        if (self.artifact_writes.len > 0) alloc.free(self.artifact_writes);
        for (self.documents) |*item| item.deinit(alloc);
        if (self.documents.len > 0) alloc.free(self.documents);
        for (self.dense_embeddings) |*item| item.deinit(alloc);
        if (self.dense_embeddings.len > 0) alloc.free(self.dense_embeddings);
        for (self.failed_keys_b64) |item| alloc.free(item);
        if (self.failed_keys_b64.len > 0) alloc.free(self.failed_keys_b64);
        self.* = undefined;
    }
};

pub fn buildJsonExtractEnrichmentsResult(
    alloc: Allocator,
    result: db_mod.types.ExtractEnrichmentsResult,
) !JsonExtractEnrichmentsResult {
    var dense_embeddings = try alloc.alloc(JsonDenseEnrichmentWrite, result.dense_embeddings.len);
    var dense_initialized: usize = 0;
    errdefer {
        for (dense_embeddings[0..dense_initialized]) |*item| item.deinit(alloc);
        alloc.free(dense_embeddings);
    }
    for (result.dense_embeddings, 0..) |item, i| {
        dense_embeddings[i] = try JsonDenseEnrichmentWrite.init(alloc, item);
        dense_initialized += 1;
    }

    var sparse_embeddings = try alloc.alloc(JsonSparseEnrichmentWrite, result.sparse_embeddings.len);
    var sparse_initialized: usize = 0;
    errdefer {
        for (sparse_embeddings[0..sparse_initialized]) |*item| item.deinit(alloc);
        alloc.free(sparse_embeddings);
    }
    for (result.sparse_embeddings, 0..) |item, i| {
        sparse_embeddings[i] = try JsonSparseEnrichmentWrite.init(alloc, item);
        sparse_initialized += 1;
    }

    var graph_writes = try alloc.alloc(JsonGraphWrite, result.graph_writes.len);
    var graph_initialized: usize = 0;
    errdefer {
        for (graph_writes[0..graph_initialized]) |*item| item.deinit(alloc);
        alloc.free(graph_writes);
    }
    for (result.graph_writes, 0..) |item, i| {
        graph_writes[i] = try JsonGraphWrite.init(alloc, item);
        graph_initialized += 1;
    }

    return .{
        .dense_embeddings = dense_embeddings,
        .sparse_embeddings = sparse_embeddings,
        .graph_writes = graph_writes,
    };
}

pub fn buildJsonComputeEnrichmentsResult(
    alloc: Allocator,
    result: db_mod.types.ComputeEnrichmentsResult,
) !JsonComputeEnrichmentsResult {
    var artifact_writes = try alloc.alloc(JsonArtifactWrite, result.artifact_writes.len);
    var artifact_initialized: usize = 0;
    errdefer {
        for (artifact_writes[0..artifact_initialized]) |*item| item.deinit(alloc);
        alloc.free(artifact_writes);
    }
    for (result.artifact_writes, 0..) |item, i| {
        artifact_writes[i] = try JsonArtifactWrite.init(alloc, item);
        artifact_initialized += 1;
    }

    var documents = try alloc.alloc(JsonDocumentEnrichmentWrite, result.documents.len);
    var documents_initialized: usize = 0;
    errdefer {
        for (documents[0..documents_initialized]) |*item| item.deinit(alloc);
        alloc.free(documents);
    }
    for (result.documents, 0..) |item, i| {
        documents[i] = try JsonDocumentEnrichmentWrite.init(alloc, item);
        documents_initialized += 1;
    }

    var dense_embeddings = try alloc.alloc(JsonDenseEnrichmentWrite, result.dense_embeddings.len);
    var dense_initialized: usize = 0;
    errdefer {
        for (dense_embeddings[0..dense_initialized]) |*item| item.deinit(alloc);
        alloc.free(dense_embeddings);
    }
    for (result.dense_embeddings, 0..) |item, i| {
        dense_embeddings[i] = try JsonDenseEnrichmentWrite.init(alloc, item);
        dense_initialized += 1;
    }

    var failed_keys_b64 = try alloc.alloc([]u8, result.failed_keys.len);
    var failed_initialized: usize = 0;
    errdefer {
        for (failed_keys_b64[0..failed_initialized]) |item| alloc.free(item);
        alloc.free(failed_keys_b64);
    }
    for (result.failed_keys, 0..) |item, i| {
        failed_keys_b64[i] = try dupBase64(alloc, item);
        failed_initialized += 1;
    }

    return .{
        .artifact_writes = artifact_writes,
        .documents = documents,
        .dense_embeddings = dense_embeddings,
        .failed_keys_b64 = failed_keys_b64,
    };
}

pub fn freeOwnedBatchWrites(alloc: Allocator, writes: []db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    if (writes.len > 0) alloc.free(writes);
}

pub fn decodeBatchWritesRequest(alloc: Allocator, request_json: []const u8) ![]db_mod.types.BatchWrite {
    const Request = struct {
        writes: []const struct {
            key_b64: []const u8,
            value_b64: []const u8,
        },
    };

    var parsed = try std.json.parseFromSlice(Request, alloc, request_json, .{});
    defer parsed.deinit();

    const writes = try alloc.alloc(db_mod.types.BatchWrite, parsed.value.writes.len);
    var initialized: usize = 0;
    errdefer {
        for (writes[0..initialized]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }

    for (parsed.value.writes, 0..) |write, i| {
        writes[i] = .{
            .key = try decodeBase64Alloc(alloc, write.key_b64),
            .value = try decodeBase64Alloc(alloc, write.value_b64),
        };
        initialized += 1;
    }

    return writes;
}

pub const JsonEdge = struct {
    edge_id_b64: ?[]u8 = null,
    owner_document_b64: ?[]u8 = null,
    source_b64: []u8,
    target_b64: []u8,
    edge_type: []const u8,
    weight: f64,
    created_at: u64,
    updated_at: u64,
    metadata_json: []const u8,

    pub fn init(alloc: Allocator, edge: db_mod.types.GraphEdge) !JsonEdge {
        const source = try dupBase64(alloc, edge.source);
        errdefer alloc.free(source);
        const target = try dupBase64(alloc, edge.target);
        errdefer alloc.free(target);
        const id = if (edge.edge_id.len > 0) try dupBase64(alloc, edge.edge_id) else null;
        errdefer if (id) |value| alloc.free(value);
        const owner = if (edge.owner_document.len > 0) try dupBase64(alloc, edge.owner_document) else null;
        return .{
            .source_b64 = source,
            .target_b64 = target,
            .edge_id_b64 = id,
            .owner_document_b64 = owner,
            .edge_type = edge.edge_type,
            .weight = edge.weight,
            .created_at = edge.created_at,
            .updated_at = edge.updated_at,
            .metadata_json = edge.metadata,
        };
    }

    pub fn deinit(self: *JsonEdge, alloc: Allocator) void {
        alloc.free(self.source_b64);
        alloc.free(self.target_b64);
        if (self.edge_id_b64) |value| alloc.free(value);
        if (self.owner_document_b64) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const JsonTraversalResult = struct {
    key_b64: []u8,
    depth: u32,
    total_weight: f64,
    path_b64: ?[][]u8 = null,
    path_edges: []JsonPathEdge = &.{},

    pub fn init(alloc: Allocator, item: db_mod.types.GraphTraversalResult) !JsonTraversalResult {
        var path_b64: ?[][]u8 = null;
        if (item.path) |path| {
            var encoded = try alloc.alloc([]u8, path.len);
            errdefer alloc.free(encoded);
            var count: usize = 0;
            errdefer {
                for (encoded[0..count]) |entry| alloc.free(entry);
            }
            for (path, 0..) |entry, i| {
                encoded[i] = try dupBase64(alloc, entry);
                count += 1;
            }
            path_b64 = encoded;
        }
        errdefer if (path_b64) |items| {
            for (items) |key| alloc.free(key);
            alloc.free(items);
        };
        const path_edges = try jsonPathEdgesAlloc(alloc, item.path_edges orelse &.{});
        errdefer freeJsonPathEdges(alloc, path_edges);
        return .{
            .key_b64 = try dupBase64(alloc, item.key),
            .depth = item.depth,
            .total_weight = item.total_weight,
            .path_b64 = path_b64,
            .path_edges = path_edges,
        };
    }

    pub fn deinit(self: *JsonTraversalResult, alloc: Allocator) void {
        alloc.free(self.key_b64);
        if (self.path_b64) |items| {
            for (items) |entry| alloc.free(entry);
            alloc.free(items);
        }
        freeJsonPathEdges(alloc, self.path_edges);
        self.* = undefined;
    }
};

pub const JsonPathEdge = struct {
    source_b64: []u8,
    target_b64: []u8,
    edge_id_b64: ?[]u8 = null,
    owner_document_b64: ?[]u8 = null,
    edge_type: []const u8,
    weight: f64,
    metadata_json: []const u8,
    traversal_direction: ?u8 = null,

    pub fn init(alloc: Allocator, edge: anytype) !JsonPathEdge {
        const source = try dupBase64(alloc, edge.source);
        errdefer alloc.free(source);
        const target = try dupBase64(alloc, edge.target);
        errdefer alloc.free(target);
        const id = if (edge.edge_id.len > 0) try dupBase64(alloc, edge.edge_id) else null;
        errdefer if (id) |value| alloc.free(value);
        const owner = if (edge.owner_document.len > 0) try dupBase64(alloc, edge.owner_document) else null;
        return .{
            .source_b64 = source,
            .target_b64 = target,
            .edge_id_b64 = id,
            .owner_document_b64 = owner,
            .edge_type = edge.edge_type,
            .weight = edge.weight,
            .metadata_json = edge.metadata,
            .traversal_direction = if (edge.traversal_direction) |direction| switch (direction) {
                .out => 0,
                .in => 1,
                .both => 2,
            } else null,
        };
    }

    pub fn deinit(self: *JsonPathEdge, alloc: Allocator) void {
        alloc.free(self.source_b64);
        alloc.free(self.target_b64);
        if (self.edge_id_b64) |value| alloc.free(value);
        if (self.owner_document_b64) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub fn jsonPathEdgesAlloc(alloc: Allocator, edges: anytype) ![]JsonPathEdge {
    const result = try alloc.alloc(JsonPathEdge, edges.len);
    var count: usize = 0;
    errdefer {
        for (result[0..count]) |*edge| edge.deinit(alloc);
        alloc.free(result);
    }
    for (edges, 0..) |edge, i| {
        result[i] = try JsonPathEdge.init(alloc, edge);
        count += 1;
    }
    return result;
}

pub fn freeJsonPathEdges(alloc: Allocator, edges: []JsonPathEdge) void {
    for (edges) |*edge| edge.deinit(alloc);
    alloc.free(edges);
}

pub const JsonPath = struct {
    nodes_b64: [][]u8,
    edges: []JsonPathEdge,
    total_weight: f64,
    length: u32,

    pub fn init(alloc: Allocator, path: db_mod.types.GraphPath) !JsonPath {
        var nodes = try alloc.alloc([]u8, path.nodes.len);
        errdefer alloc.free(nodes);
        var node_count: usize = 0;
        errdefer {
            for (nodes[0..node_count]) |entry| alloc.free(entry);
        }
        for (path.nodes, 0..) |node, i| {
            nodes[i] = try dupBase64(alloc, node);
            node_count += 1;
        }

        var edges = try alloc.alloc(JsonPathEdge, path.edges.len);
        errdefer alloc.free(edges);
        var edge_count: usize = 0;
        errdefer {
            for (edges[0..edge_count]) |*entry| entry.deinit(alloc);
        }
        for (path.edges, 0..) |edge, i| {
            edges[i] = try JsonPathEdge.init(alloc, edge);
            edge_count += 1;
        }

        return .{
            .nodes_b64 = nodes,
            .edges = edges,
            .total_weight = path.total_weight,
            .length = path.length,
        };
    }

    pub fn deinit(self: *JsonPath, alloc: Allocator) void {
        for (self.nodes_b64) |entry| alloc.free(entry);
        if (self.nodes_b64.len > 0) alloc.free(self.nodes_b64);
        for (self.edges) |*entry| entry.deinit(alloc);
        if (self.edges.len > 0) alloc.free(self.edges);
        self.* = undefined;
    }
};

pub const JsonPatternBinding = struct {
    alias: []u8,
    key_b64: []u8,
    depth: u32,

    pub fn init(alloc: Allocator, binding: graph_pattern_mod.PatternBinding) !JsonPatternBinding {
        return .{
            .alias = try alloc.dupe(u8, binding.alias),
            .key_b64 = try dupBase64(alloc, binding.key),
            .depth = binding.depth,
        };
    }

    pub fn deinit(self: *JsonPatternBinding, alloc: Allocator) void {
        alloc.free(self.alias);
        alloc.free(self.key_b64);
        self.* = undefined;
    }
};

pub const JsonPatternMatch = struct {
    bindings: []JsonPatternBinding,
    path: []JsonPathEdge,

    pub fn init(alloc: Allocator, match: graph_pattern_mod.PatternMatch) !JsonPatternMatch {
        var bindings = try alloc.alloc(JsonPatternBinding, match.bindings.len);
        errdefer alloc.free(bindings);
        var binding_count: usize = 0;
        errdefer {
            for (bindings[0..binding_count]) |*binding| binding.deinit(alloc);
        }
        for (match.bindings, 0..) |binding, i| {
            bindings[i] = try JsonPatternBinding.init(alloc, binding);
            binding_count += 1;
        }

        var path = try alloc.alloc(JsonPathEdge, match.path.len);
        errdefer alloc.free(path);
        var path_count: usize = 0;
        errdefer {
            for (path[0..path_count]) |*entry| entry.deinit(alloc);
        }
        for (match.path, 0..) |edge, i| {
            path[i] = try JsonPathEdge.init(alloc, edge);
            path_count += 1;
        }

        return .{
            .bindings = bindings,
            .path = path,
        };
    }

    pub fn deinit(self: *JsonPatternMatch, alloc: Allocator) void {
        for (self.bindings) |*binding| binding.deinit(alloc);
        if (self.bindings.len > 0) alloc.free(self.bindings);
        for (self.path) |*entry| entry.deinit(alloc);
        if (self.path.len > 0) alloc.free(self.path);
        self.* = undefined;
    }
};

pub const JsonGraphNode = struct {
    key_b64: []u8,
    depth: u32,
    distance: f64,
    path_b64: ?[][]u8 = null,
    path_edges: []JsonPathEdge = &.{},

    pub fn init(alloc: Allocator, node: graph_query_mod.GraphResultNode) !JsonGraphNode {
        var path_b64: ?[][]u8 = null;
        if (node.path) |path| {
            var encoded = try alloc.alloc([]u8, path.len);
            errdefer alloc.free(encoded);
            var count: usize = 0;
            errdefer {
                for (encoded[0..count]) |entry| alloc.free(entry);
            }
            for (path, 0..) |entry, i| {
                encoded[i] = try dupBase64(alloc, entry);
                count += 1;
            }
            path_b64 = encoded;
        }

        errdefer if (path_b64) |items| {
            for (items) |item| alloc.free(item);
            alloc.free(items);
        };
        const path_edges = try jsonPathEdgesAlloc(alloc, node.path_edges orelse &.{});
        errdefer freeJsonPathEdges(alloc, path_edges);

        return .{
            .key_b64 = try dupBase64(alloc, node.key),
            .depth = node.depth,
            .distance = node.distance,
            .path_b64 = path_b64,
            .path_edges = path_edges,
        };
    }

    pub fn deinit(self: *JsonGraphNode, alloc: Allocator) void {
        alloc.free(self.key_b64);
        if (self.path_b64) |items| {
            for (items) |entry| alloc.free(entry);
            alloc.free(items);
        }
        for (self.path_edges) |*edge| edge.deinit(alloc);
        if (self.path_edges.len > 0) alloc.free(self.path_edges);
        self.* = undefined;
    }
};

pub fn antfly_db_open(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_open), .{ .name = "antfly_db_open" }), .{ path, out_handle });
    capi.clearLastError();
    const out = out_handle orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output pointer must not be null");
    out.* = null;
    const path_slice = cStringSpan(path) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    const handle = openDefaultDirectoryHandle(path_slice) catch |err| return capi.mapError(err);
    out.* = publishHandle(handle) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn openDefaultDirectoryHandle(path: []const u8) !*Handle {
    @import("io_lifetime.zig").acquire();
    errdefer @import("io_lifetime.zig").release();
    const alloc = std.heap.c_allocator;
    var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 1, .range_id = 1 }, .prefer_existing_identity_namespace = true, .online_source_authority = .native });
    errdefer db.close();
    const handle = alloc.create(Handle) catch return error.OutOfMemory;
    errdefer alloc.destroy(handle);
    handle.* = .{
        .alloc = alloc,
        .io_lifetime_lease = true,
        .db = db,
        .embedded_path = try alloc.dupe(u8, path),
    };
    @import("tables.zig").loadCatalog(handle) catch |err| {
        alloc.free(handle.embedded_path.?);
        return err;
    };
    handle.database().*.startResidentBackgroundWorkersIfNeeded();
    return handle;
}

pub fn antfly_db_close(handle_ptr: ?*anyopaque) callconv(.c) void {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_close), .{ .name = "antfly_db_close" }), .{handle_ptr});
    // Stale ids and concurrent or repeated closes are no-ops.
    closeHandleId(handle_ptr);
}

/// Threading contract of this library, like sqlite3_threadsafe(). Always
/// ANTFLY_THREADING_SERIALIZED: every handle may be used from any thread,
/// concurrently. See zig/CAPI.md "Thread Safety".
pub fn antfly_threading_mode() callconv(.c) u32 {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_threading_mode), .{ .name = "antfly_threading_mode" }), .{});
    return capi.threading_serialized;
}

pub fn antfly_abi_version() callconv(.c) u32 {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_abi_version), .{ .name = "antfly_abi_version" }), .{});
    return abi_version;
}

pub fn antfly_open_options_size() callconv(.c) u32 {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_open_options_size), .{ .name = "antfly_open_options_size" }), .{});
    return @intCast(@sizeOf(capi.OpenOptions));
}

/// Detail for the latest ErrorCode call on this thread; copy immediately.
pub fn antfly_last_error_detail() callconv(.c) [*:0]const u8 {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_last_error_detail), .{ .name = "antfly_last_error_detail" }), .{});
    return capi.lastErrorDetail();
}

pub fn antfly_last_error_code() callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_last_error_code), .{ .name = "antfly_last_error_code" }), .{});
    return capi.lastErrorCode();
}

pub fn antfly_error_code_name(code: c_int) callconv(.c) [*:0]const u8 {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_error_code_name), .{ .name = "antfly_error_code_name" }), .{code});
    return capi.errorCodeName(code);
}

pub fn antfly_error_code_description(code: c_int) callconv(.c) [*:0]const u8 {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_error_code_description), .{ .name = "antfly_error_code_description" }), .{code});
    return capi.errorCodeDescription(code);
}

pub fn antfly_open_options_init(options: ?*capi.OpenOptions) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_open_options_init), .{ .name = "antfly_open_options_init" }), .{options});
    capi.clearLastError();
    const opts = options orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": options pointer must not be null");
    opts.* = .{};
    return .ok;
}

pub const open_known_flags = capi.open_flag_no_sync |
    capi.open_flag_ttl_cleanup |
    capi.open_flag_remote_provider_configured |
    capi.open_flag_local_runtime_configured |
    capi.open_flag_generated_enrichment_replay;

pub const StorageKind = enum {
    directory,
    lite,
};

pub const LiteResolvedOpenOptions = struct {
    storage_kind: StorageKind = .lite,
    open_mode: db_mod.OpenOptions.OpenMode = .writer,
    profile: lite_backend.Profile = .native,
    map_size: ?usize = null,
    no_sync: bool = false,
    ttl_cleanup: ?db_mod.ttl_runtime.Config = null,
    inference: lite_backend.InferenceOpenOptions = .{},
    generated_enrichment_replay: bool = false,
    busy_timeout_ms: u64 = 0,
    /// Internal: the connection boundary owns the cross-process path lease.
    externally_locked: bool = false,
    artifact_reader_fence: bool = false,
    /// Borrowed only during open, under the connection's publication fence.
    allocator_verification_owner: ?*lite_backend.native.NativeFile = null,
};

pub fn optionFieldType(comptime Options: type, comptime field_name: []const u8) type {
    return @TypeOf(@field(@as(Options, .{}), field_name));
}

pub fn optionHasField(comptime Options: type, comptime field_name: []const u8) bool {
    inline for (comptime std.meta.fieldNames(Options)) |reflected_name| {
        if (std.mem.eql(u8, reflected_name, field_name)) return true;
    }
    return false;
}

pub fn optionFieldPresent(comptime Options: type, abi_size: u32, comptime field_name: []const u8) bool {
    const Field = optionFieldType(Options, field_name);
    const offset = @offsetOf(Options, field_name);
    return abi_size >= offset + @sizeOf(Field);
}

pub fn readOptionField(
    comptime Options: type,
    options: *const Options,
    abi_size: u32,
    comptime field_name: []const u8,
) ?optionFieldType(Options, field_name) {
    const Field = optionFieldType(Options, field_name);
    const offset = @offsetOf(Options, field_name);
    if (abi_size < offset + @sizeOf(Field)) return null;
    const raw: [*]const u8 = @ptrCast(options);
    return std.mem.bytesAsValue(Field, raw[offset..][0..@sizeOf(Field)]).*;
}

pub fn validateOpenOptionsReserved(comptime Options: type, options: *const Options, abi_size: u32) !void {
    if (comptime optionHasField(Options, "reserved0")) {
        if (optionFieldPresent(Options, abi_size, "reserved0")) {
            if (readOptionField(Options, options, abi_size, "reserved0").? != 0) return error.InvalidArgument;
        }
    }
    if (comptime !optionHasField(Options, "reserved")) {
        return;
    }
    const reserved_offset = @offsetOf(Options, "reserved");
    if (abi_size <= reserved_offset) return;
    const available = @min(@as(usize, abi_size) - reserved_offset, @sizeOf(optionFieldType(Options, "reserved")));
    if (available % @sizeOf(u64) != 0) return error.InvalidArgument;
    const raw: [*]const u8 = @ptrCast(options);
    var offset: usize = reserved_offset;
    var remaining = available;
    while (remaining >= @sizeOf(u64)) : ({
        offset += @sizeOf(u64);
        remaining -= @sizeOf(u64);
    }) {
        const word = std.mem.bytesAsValue(u64, raw[offset..][0..@sizeOf(u64)]).*;
        if (word != 0) return error.InvalidArgument;
    }
}

pub fn openModeFromU32(value: u32) !db_mod.OpenOptions.OpenMode {
    return switch (value) {
        0 => .writer,
        1 => .query_readonly,
        2 => .status_only,
        else => return error.InvalidArgument,
    };
}

pub fn profileFromU32(value: u32) !lite_backend.Profile {
    return switch (value) {
        0 => .native,
        1 => .hosted,
        else => return error.InvalidArgument,
    };
}

pub fn validateResolvedOpenOptions(resolved: LiteResolvedOpenOptions) !void {
    if (resolved.profile == .hosted and resolved.ttl_cleanup != null) {
        return error.InvalidArgument;
    }
    if (resolved.profile == .hosted and resolved.generated_enrichment_replay) {
        return error.InvalidArgument;
    }
}

pub fn resolveOpenOptions(options_ptr: ?*const capi.OpenOptions) !LiteResolvedOpenOptions {
    const options = options_ptr orelse return .{ .storage_kind = .directory };
    const abi_size = options.abi_size;
    if (abi_size < @offsetOf(capi.OpenOptions, "storage_kind")) return error.InvalidArgument;
    const flags = readOptionField(capi.OpenOptions, options, abi_size, "flags") orelse 0;
    if ((flags & ~open_known_flags) != 0) return error.InvalidArgument;
    try validateOpenOptionsReserved(capi.OpenOptions, options, abi_size);

    const storage_kind: StorageKind = switch (readOptionField(capi.OpenOptions, options, abi_size, "storage_kind") orelse capi.storage_kind_directory) {
        capi.storage_kind_directory => .directory,
        capi.storage_kind_lite => .lite,
        else => return error.InvalidArgument,
    };
    const open_mode = try openModeFromU32(readOptionField(capi.OpenOptions, options, abi_size, "open_mode") orelse capi.open_mode_writer);
    const profile = try profileFromU32(readOptionField(capi.OpenOptions, options, abi_size, "profile") orelse capi.profile_native);
    const map_size = readOptionField(capi.OpenOptions, options, abi_size, "map_size") orelse 0;
    if (map_size > std.math.maxInt(usize)) return error.InvalidArgument;

    var resolved = LiteResolvedOpenOptions{
        .storage_kind = storage_kind,
        .open_mode = open_mode,
        .profile = profile,
        .map_size = if (map_size == 0) null else @as(usize, @intCast(map_size)),
        .no_sync = (flags & capi.open_flag_no_sync) != 0,
        .inference = .{
            .remote_provider_configured = (flags & capi.open_flag_remote_provider_configured) != 0,
            .local_runtime_configured = (flags & capi.open_flag_local_runtime_configured) != 0,
            .models_dir = if (readOptionField(capi.OpenOptions, options, abi_size, "models_dir")) |directory| if (directory.len > 0) directory.bytes() else null else null,
            .host_budget_mb = readOptionField(capi.OpenOptions, options, abi_size, "inference_host_budget_mb") orelse 0,
            .backend_budget_mb = readOptionField(capi.OpenOptions, options, abi_size, "inference_backend_budget_mb") orelse 0,
            .combined_budget_mb = readOptionField(capi.OpenOptions, options, abi_size, "inference_combined_budget_mb") orelse 0,
            .kv_budget_mb = readOptionField(capi.OpenOptions, options, abi_size, "inference_kv_budget_mb") orelse 0,
            .scratch_budget_mb = readOptionField(capi.OpenOptions, options, abi_size, "inference_scratch_budget_mb") orelse 0,
            .process_memory_budget_mb = readOptionField(capi.OpenOptions, options, abi_size, "inference_process_memory_budget_mb") orelse 0,
        },
        .generated_enrichment_replay = (flags & capi.open_flag_generated_enrichment_replay) != 0,
        .busy_timeout_ms = readOptionField(capi.OpenOptions, options, abi_size, "busy_timeout_ms") orelse 0,
    };
    if ((flags & capi.open_flag_ttl_cleanup) != 0) {
        const owner_id = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_owner_id") orelse capi.Slice{};
        if (owner_id.ptr == null and owner_id.len != 0) {
            return error.InvalidArgument;
        }
        var ttl_cfg = db_mod.ttl_runtime.Config{
            .enabled = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_enabled") orelse false,
            .lease_owned = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_lease_owned") orelse false,
        };
        if (owner_id.len != 0) {
            ttl_cfg.owner_id = owner_id.ptr.?[0..owner_id.len];
        }
        const lease_ttl_ms = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_lease_ttl_ms") orelse 0;
        const interval_ms = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_interval_ms") orelse 0;
        const batch_size = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_batch_size") orelse 0;
        const grace_period_ns = readOptionField(capi.OpenOptions, options, abi_size, "ttl_cleanup_grace_period_ns") orelse 0;
        if (lease_ttl_ms != 0) ttl_cfg.lease_ttl_ms = lease_ttl_ms;
        if (interval_ms != 0) ttl_cfg.interval_ms = interval_ms;
        if (batch_size != 0) ttl_cfg.batch_size = batch_size;
        if (grace_period_ns != 0) ttl_cfg.grace_period_ns = grace_period_ns;
        resolved.ttl_cleanup = ttl_cfg;
    }
    try validateResolvedOpenOptions(resolved);
    return resolved;
}

pub fn openLiteHandle(
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
    out_handle: ?*?*anyopaque,
) capi.ErrorCode {
    capi.clearLastError();
    const out = out_handle orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output pointer must not be null");
    out.* = null;
    const handle = openLiteHandleAlloc(path, resolved, create) catch |err| return capi.mapError(err);
    out.* = publishHandle(handle) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn openLiteHandleAlloc(
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
) !*Handle {
    return try @import("lite_connection.zig").open(path, resolved, create);
}

/// Zig embedding seam behind the C ABI. It constructs the same opaque handle
/// and therefore exercises the same exported request/close/callback paths, but
/// lets an in-process host supply deterministic std.Io and runtime ownership.
/// The runtime and I/O interface must outlive the returned handle.
pub const HostLiteOpenOptions = struct {
    create: bool = false,
    read_only: bool = false,
    hosted: bool = true,
    no_sync: bool = false,
};

pub fn openLiteHandleWithRuntime(
    alloc: Allocator,
    path: []const u8,
    io: std.Io,
    backend_runtime: *db_mod.background_runtime.BackendRuntime,
    options: HostLiteOpenOptions,
) !*anyopaque {
    return publishHandle(try openLiteHandleAllocWithRuntime(alloc, path, .{
        .open_mode = if (options.read_only) .query_readonly else .writer,
        .profile = if (options.hosted) .hosted else .native,
        .no_sync = options.no_sync,
    }, options.create, io, backend_runtime));
}

pub fn closeLiteRuntimeHandle(handle_ptr: ?*anyopaque) void {
    closeHandleId(handle_ptr);
}

pub fn openLiteHandleAllocWithRuntime(
    alloc: Allocator,
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
    borrowed_io: ?std.Io,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !*Handle {
    return openLiteHandleAllocWithSharedInference(alloc, path, resolved, create, borrowed_io, backend_runtime, null);
}

pub fn openLiteHandleAllocWithSharedInference(
    alloc: Allocator,
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
    borrowed_io: ?std.Io,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    inference_source: ?*Handle,
) !*Handle {
    if (create and !liteOpenModeCanWrite(resolved.open_mode)) return error.InvalidArgument;
    var handle_owns_resources = false;
    @import("io_lifetime.zig").acquire();
    errdefer if (!handle_owns_resources) @import("io_lifetime.zig").release();
    // Each cached generation owns its storage pool. Retired cursors may keep
    // an older generation alive while the same index roots reopen in this one.
    var owned_runtime: ?db_mod.background_runtime.BackendRuntimeHandle = if (resolved.externally_locked and backend_runtime == null)
        try db_mod.background_runtime.BackendRuntimeHandle.initManualWithOwnedIo(alloc)
    else
        null;
    errdefer if (!handle_owns_resources) if (owned_runtime) |*runtime| runtime.deinit();
    var backend = if (create)
        try lite_backend.Handle.createWithOptions(alloc, path, .{
            .externally_locked = resolved.externally_locked,
            .exclusive = true,
            .no_sync = resolved.no_sync,
            .io = borrowed_io,
        })
    else
        try lite_backend.Handle.open(alloc, path, .{
            .externally_locked = resolved.externally_locked,
            .read_only = resolved.open_mode == .query_readonly or resolved.open_mode == .status_only,
            .no_sync = resolved.no_sync,
            .io = borrowed_io,
        });
    errdefer if (!handle_owns_resources) backend.deinit();
    if (resolved.allocator_verification_owner) |verified| {
        if (!resolved.externally_locked) return error.InvalidArgument;
        try backend.native_docstore.?.file.inheritAllocatorVerification(verified);
    }
    if (resolved.externally_locked) {
        backend.native_docstore.?.file.externally_locked = true;
        backend.native_docstore.?.maintenance_start_suppressed = true;
    }

    backend.native_docstore.?.artifact_reader_fence = resolved.artifact_reader_fence;

    var opts = db_mod.OpenOptions{
        .open_mode = resolved.open_mode,
        .external_derived_checkpoints = false,
        .backend_runtime = if (owned_runtime) |runtime| runtime.runtime else backend_runtime,
        .online_source_authority = .native,
    };
    if (resolved.map_size) |map_size| opts.map_size = map_size;
    opts.no_sync = resolved.no_sync;
    if (resolved.ttl_cleanup) |ttl_cleanup| opts.ttl_cleanup = ttl_cleanup;
    if (resolved.generated_enrichment_replay) {
        opts.enrichment = .{ .enable_without_producers = true };
    }
    if (resolved.profile == .hosted) {
        opts.executor = .{ .backend = .manual };
        opts.ttl_cleanup = .{ .enabled = false };
        opts.transaction_recovery = .{ .enabled = false };
        opts.text_merge = .{ .enabled = false };
        opts.sparse_compaction = .{ .enabled = false };
        opts.graph_metric_maintenance = .{ .start_background_loop = false };
    }
    opts.start_resolver_workers = false;
    if (resolved.externally_locked) {
        // The connection worker captures inputs and publishes under short path leases.
        opts.start_independent_maintenance = false;
        opts.executor = .{ .backend = .manual };
        opts.start_optional_runtime_workers = false;
        opts.start_resolver_workers = false;
        opts.defer_index_backfill_on_open = true;
    }
    try backend.configureDbOpenOptions(&opts);

    // One identity policy for every Lite surface (C ABI, embedded package,
    // CLI): pin a new file to the embedded root identity and adopt whatever
    // identity an existing file already carries, so a database created
    // through one surface opens through any other.
    const identity = antfly.lite.connection.identityOpenOptions(create);
    opts.identity_namespace = identity.identity_namespace;
    opts.prefer_existing_identity_namespace = identity.prefer_existing_identity_namespace;
    var db = try db_mod.DB.open(alloc, path, opts);
    errdefer if (!handle_owns_resources) db.close();

    if (create) {
        // Antfly Lite databases provision the same default full-text index
        // the server provisions on every table create, through the routine
        // shared with the CLI and the embedded package.
        try antfly.lite.connection.provisionDefaultFullTextIndex(&db);
    }

    const handle = alloc.create(Handle) catch return error.OutOfMemory;
    errdefer if (!handle_owns_resources) alloc.destroy(handle);
    handle.* = .{
        .alloc = alloc,
        .io_lifetime_lease = true,
        .db = db,
        .open_mode = resolved.open_mode,
        .owned_lite_backend = backend,
        .owned_lite_runtime = owned_runtime,
        .embedded_path = try alloc.dupe(u8, path),
        .embedded_open_options = opts,
        .lite_profile = resolved.profile,
        .lite_inference_status = lite_backend.inferenceStatusForProfileWithOptions(resolved.profile, resolved.inference),
        .lite_generated_enrichment_replay = resolved.generated_enrichment_replay,
    };
    // Transfer the complete generation before configuring producers. From this
    // point even a failed open must drain the actual DB stored in the handle,
    // then release inference, storage and IO in the ordinary close order.
    handle_owns_resources = true;
    errdefer {
        handle.lease_snapshot = true;
        handles.closeHandle(handle);
    }
    // Native builds link local inference. The native profile starts a service
    // only when the caller requests it and the target advertises support;
    // hosted/freestanding surfaces retain caller-supplied inference.
    if (resolved.profile == .native and
        resolved.inference.local_runtime_configured and
        capi_build_options.inference_enabled and
        lite_backend.capabilitiesForProfile(.native).local_inference_runtime)
    {
        if (if (inference_source) |source| source.lite_inference else null) |service| {
            service.retain();
            handle.lite_inference = service;
            handle.lite_inference_status = inference_source.?.lite_inference_status;
        } else try startLiteEmbeddedInference(handle, alloc, path, .{
            .models_dir = resolved.inference.models_dir,
            .host_budget_mb = resolved.inference.host_budget_mb,
            .backend_budget_mb = resolved.inference.backend_budget_mb,
            .combined_budget_mb = resolved.inference.combined_budget_mb,
            .kv_budget_mb = resolved.inference.kv_budget_mb,
            .scratch_budget_mb = resolved.inference.scratch_budget_mb,
            .process_memory_budget_mb = resolved.inference.process_memory_budget_mb,
        });
    }
    // Restores the managed enrichment runtime for indexes/enrichments that
    // were already declared in a prior session (`create` only ever adds the
    // default full-text index, so this is a no-op there). Must run after
    // `startLiteEmbeddedInference` so an embedded local provider is already
    // attached to the handle when this reads it.
    try refreshLiteManagedEmbeddingRuntime(handle);
    @import("entity_sink.zig").attach(handle, &handle.db);
    try @import("tables.zig").loadCatalog(handle);
    if (!resolved.externally_locked) handle.database().*.startQuarantineRetryWorkerIfNeeded();
    return handle;
}

pub fn dbOpenOptionsFromResolved(resolved: LiteResolvedOpenOptions, lite: bool) db_mod.OpenOptions {
    var opts = db_mod.OpenOptions{
        .open_mode = resolved.open_mode,
        .external_derived_checkpoints = !lite,
        .online_source_authority = .native,
    };
    if (resolved.map_size) |map_size| opts.map_size = map_size;
    opts.no_sync = resolved.no_sync;
    if (resolved.ttl_cleanup) |ttl_cleanup| opts.ttl_cleanup = ttl_cleanup;
    if (resolved.generated_enrichment_replay) {
        opts.enrichment = .{ .enable_without_producers = true };
    }
    if (resolved.profile == .hosted) {
        opts.executor = .{ .backend = .manual };
        opts.ttl_cleanup = .{ .enabled = false };
        opts.transaction_recovery = .{ .enabled = false };
        opts.text_merge = .{ .enabled = false };
        opts.sparse_compaction = .{ .enabled = false };
        opts.graph_metric_maintenance = .{ .start_background_loop = false };
    }
    return opts;
}

pub fn openDirectoryHandle(
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
    out_handle: ?*?*anyopaque,
) capi.ErrorCode {
    const out = out_handle orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output pointer must not be null");
    out.* = null;
    if (create) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": directory storage must be opened; this entry point cannot create it");
    const handle = openDirectoryHandleAlloc(path, resolved) catch |err| return capi.mapError(err);
    out.* = publishHandle(handle) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn openDirectoryHandleAlloc(path: []const u8, resolved: LiteResolvedOpenOptions) !*Handle {
    @import("io_lifetime.zig").acquire();
    errdefer @import("io_lifetime.zig").release();
    const alloc = std.heap.c_allocator;
    var options = dbOpenOptionsFromResolved(resolved, false);
    options.identity_namespace = .{ .table_id = 1, .shard_id = 1, .range_id = 1 };
    options.prefer_existing_identity_namespace = true;
    var db = try db_mod.DB.open(alloc, path, options);
    errdefer db.close();
    const handle = alloc.create(Handle) catch return error.OutOfMemory;
    errdefer alloc.destroy(handle);
    handle.* = .{
        .alloc = alloc,
        .io_lifetime_lease = true,
        .db = db,
        .open_mode = resolved.open_mode,
        .embedded_path = try alloc.dupe(u8, path),
        .embedded_open_options = options,
    };
    @import("tables.zig").loadCatalog(handle) catch |err| {
        alloc.free(handle.embedded_path.?);
        return err;
    };
    handle.database().*.startResidentBackgroundWorkersIfNeeded();
    return handle;
}

pub fn openGenericHandle(
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
    out_handle: ?*?*anyopaque,
) capi.ErrorCode {
    const first = openGenericHandleOnce(path, resolved, create, out_handle);
    if (first != .busy or resolved.busy_timeout_ms == 0) return first;
    // Another writer holds the writer lock. A failed open releases everything
    // it acquired, so retry the whole open with capped exponential backoff
    // until `busy_timeout_ms` elapses, like sqlite3_busy_timeout.
    const io = handleLockIo();
    const deadline_ns = std.Io.Timestamp.now(io, .awake).toNanoseconds() +
        @as(i96, resolved.busy_timeout_ms) * std.time.ns_per_ms;
    var backoff_ms: i64 = 1;
    while (true) {
        const remaining_ns = deadline_ns - std.Io.Timestamp.now(io, .awake).toNanoseconds();
        if (remaining_ns <= 0) return capi.recordError(.busy, @src().fn_name ++ ": timed out waiting for the Lite writer lock");
        const sleep_ns = @min(@as(i96, backoff_ms) * std.time.ns_per_ms, remaining_ns);
        io.sleep(.fromNanoseconds(sleep_ns), .awake) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        const code = openGenericHandleOnce(path, resolved, create, out_handle);
        if (code != .busy) return capi.ensureError(code, @src().fn_name);
        backoff_ms = @min(backoff_ms * 2, 50);
    }
}

pub fn openGenericHandleOnce(
    path: []const u8,
    resolved: LiteResolvedOpenOptions,
    create: bool,
    out_handle: ?*?*anyopaque,
) capi.ErrorCode {
    return switch (resolved.storage_kind) {
        .directory => openDirectoryHandle(path, resolved, create, out_handle),
        .lite => openLiteHandle(path, resolved, create, out_handle),
    };
}

pub fn cStringSpan(path: ?[*:0]const u8) ?[]const u8 {
    const ptr = path orelse return null;
    return std.mem.span(ptr);
}

pub fn antfly_db_open_with_options(path: ?[*:0]const u8, options: ?*const capi.OpenOptions, out_handle: ?*?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_open_with_options), .{ .name = "antfly_db_open_with_options" }), .{ path, options, out_handle });
    capi.clearLastError();
    const out = out_handle orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output pointer must not be null");
    out.* = null;
    const path_slice = cStringSpan(path) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    const resolved = resolveOpenOptions(options) catch |err| return capi.mapError(err);
    return openGenericHandle(path_slice, resolved, false, out);
}

pub fn antfly_db_create_with_options(path: ?[*:0]const u8, options: ?*const capi.OpenOptions, out_handle: ?*?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_create_with_options), .{ .name = "antfly_db_create_with_options" }), .{ path, options, out_handle });
    capi.clearLastError();
    const out = out_handle orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output pointer must not be null");
    out.* = null;
    const path_slice = cStringSpan(path) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    const resolved = resolveOpenOptions(options) catch |err| return capi.mapError(err);
    return openGenericHandle(path_slice, resolved, true, out);
}

pub fn antfly_lite_open(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_lite_open), .{ .name = "antfly_lite_open" }), .{ path, out_handle });
    capi.clearLastError();
    const path_slice = cStringSpan(path) orelse {
        if (out_handle) |out| out.* = null;
        return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    };
    return openLiteHandle(path_slice, .{}, false, out_handle);
}

pub fn antfly_lite_create(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_lite_create), .{ .name = "antfly_lite_create" }), .{ path, out_handle });
    capi.clearLastError();
    const path_slice = cStringSpan(path) orelse {
        if (out_handle) |out| out.* = null;
        return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    };
    return openLiteHandle(path_slice, .{}, true, out_handle);
}

pub fn antfly_lite_open_hosted(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_lite_open_hosted), .{ .name = "antfly_lite_open_hosted" }), .{ path, out_handle });
    capi.clearLastError();
    const path_slice = cStringSpan(path) orelse {
        if (out_handle) |out| out.* = null;
        return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    };
    return openLiteHandle(path_slice, .{ .profile = .hosted }, false, out_handle);
}

pub fn antfly_lite_create_hosted(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_lite_create_hosted), .{ .name = "antfly_lite_create_hosted" }), .{ path, out_handle });
    capi.clearLastError();
    const path_slice = cStringSpan(path) orelse {
        if (out_handle) |out| out.* = null;
        return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    };
    return openLiteHandle(path_slice, .{ .profile = .hosted }, true, out_handle);
}

pub fn antfly_lite_open_readonly(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_lite_open_readonly), .{ .name = "antfly_lite_open_readonly" }), .{ path, out_handle });
    capi.clearLastError();
    const path_slice = cStringSpan(path) orelse {
        if (out_handle) |out| out.* = null;
        return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    };
    return openLiteHandle(path_slice, .{ .open_mode = .query_readonly }, false, out_handle);
}

pub fn antfly_lite_open_status_only(path: ?[*:0]const u8, out_handle: ?*?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_lite_open_status_only), .{ .name = "antfly_lite_open_status_only" }), .{ path, out_handle });
    capi.clearLastError();
    const path_slice = cStringSpan(path) orelse {
        if (out_handle) |out| out.* = null;
        return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    };
    return openLiteHandle(path_slice, .{ .open_mode = .status_only }, false, out_handle);
}

pub fn resetOutBuffer(out_buf: ?*capi.Buffer) ?*capi.Buffer {
    const out = out_buf orelse return null;
    out.* = .{};
    return out;
}

fn databaseMinimumReaderVersion(handle: *Handle) u32 {
    const root = handle.parent_handle orelse handle;
    var version = root.db.core.index_manager.minimum_reader_version.load(.acquire);
    var it = root.embedded_tables.valueIterator();
    while (it.next()) |table| version = @max(version, table.*.db.core.index_manager.minimum_reader_version.load(.acquire));
    return version;
}

pub fn antfly_db_capabilities_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_capabilities_json), .{ .name = "antfly_db_capabilities_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const profile = handle.lite_profile orelse .native;
    const inference = handle.lite_inference_status orelse lite_backend.inferenceStatusForProfile(profile);
    var capabilities = lite_backend.capabilitiesForProfileWithInferenceStatus(profile, inference);
    capabilities.minimum_reader_version = databaseMinimumReaderVersion(handle);
    out.* = stringifyJson(capabilities) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

/// Storage block for a normal Antfly directory handle's status report.
pub const directory_storage_status: lite_backend.StorageStatus = .{
    .format = "directory",
    .engine = "directory",
    .primary_layout = "directory",
    .replay_layout = "directory",
    .index_layout = "directory",
};

pub fn antfly_db_status_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_status_json), .{ .name = "antfly_db_status_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const storage: lite_backend.StorageStatus = if (handle.owned_lite_backend) |*backend|
        backend.storageStatus()
    else
        directory_storage_status;

    const stats = handle.database().*.stats(handle.alloc) catch |err| return capi.mapError(err);
    defer db_mod.types.freeDBStats(handle.alloc, stats);

    const indexes = dbIndexStatsProjectionAlloc(handle.alloc, stats) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer if (indexes.len > 0) handle.alloc.free(indexes);

    const profile = handle.lite_profile orelse .native;
    const inference = handle.lite_inference_status orelse lite_backend.inferenceStatusForProfile(profile);
    var capabilities = lite_backend.capabilitiesForProfileWithInferenceStatus(profile, inference);
    capabilities.minimum_reader_version = databaseMinimumReaderVersion(handle);
    const status = lite_backend.Status(JsonDBStats){
        .storage = storage,
        .stats = jsonDBStatsProjection(stats, indexes),
        .pending_work = handle.database().*.pendingWorkStats(),
        .inference = inference,
        .capabilities = capabilities,
    };

    var arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const status_json = std.json.Stringify.valueAlloc(a, status, .{}) catch |err| return capi.mapError(err);
    var value = std.json.parseFromSliceLeaky(std.json.Value, a, status_json, .{}) catch |err| return capi.mapError(err);
    if (comptime capi_build_options.inference_enabled) if (handle.lite_inference) |service| {
        const admission = inference_provider.embeddedMemoryAdmissionStats(service.lifetime.handle) catch |err| return capi.mapError(err);
        const bytes = std.json.Stringify.valueAlloc(a, admission, .{}) catch |err| return capi.mapError(err);
        const admission_value = std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{}) catch |err| return capi.mapError(err);
        value.object.getPtr("inference").?.object.put(a, "memory_admission", admission_value) catch |err| return capi.mapError(err);
    };
    out.* = stringifyJson(value) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_backup(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_backup), .{ .name = "antfly_db_backup" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .maintain) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const out_buf_ptr = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const handle = guard.handle;

    @import("tables.zig").requireDatabase(handle) catch |err| return capi.mapError(err);
    var out = std.ArrayList(u8).empty;
    defer out.deinit(handle.alloc);
    @import("database_backup.zig").exportDatabase(handle, &out) catch |err| return capi.mapError(err);
    const bytes = out.toOwnedSlice(handle.alloc) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    out = .empty;
    out_buf_ptr.* = .{ .ptr = bytes.ptr, .len = bytes.len };
    return .ok;
}

pub fn antfly_db_backup_json(handle_ptr: ?*anyopaque, options: capi.Slice, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_backup_json), .{ .name = "antfly_db_backup_json" }), .{ handle_ptr, options, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .maintain) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const out_buf_ptr = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const handle = guard.handle;

    @import("tables.zig").requireDatabase(handle) catch |err| return capi.mapError(err);
    var out = std.ArrayList(u8).empty;
    defer out.deinit(handle.alloc);
    const Request = struct { tables: ?[]const []const u8 = null };
    const request = std.json.parseFromSlice(Request, handle.alloc, options.bytes(), .{}) catch |err| return capi.mapError(err);
    defer request.deinit();
    @import("database_backup.zig").exportTables(handle, &out, request.value.tables) catch |err| return capi.mapError(err);
    const bytes = out.toOwnedSlice(handle.alloc) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    out = .empty;
    out_buf_ptr.* = .{ .ptr = bytes.ptr, .len = bytes.len };
    return .ok;
}

pub fn antfly_db_import_backup(handle_ptr: ?*anyopaque, backup: capi.Slice) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_import_backup), .{ .name = "antfly_db_import_backup" }), .{ handle_ptr, backup });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    @import("tables.zig").requireDatabase(handle) catch |err| return capi.mapError(err);
    if (!liteOpenModeCanWrite(handle.open_mode)) return capi.mapError(error.ReadOnly);
    @import("tables.zig").load(handle) catch |err| return capi.mapError(err);
    if (backup.len == 0 or backup.ptr == null) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": backup bytes must be nonempty and have a non-null pointer");
    if (handle.embedded_tables.count() != 0) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": backup import requires a database without named tables");
    if (handle.sql_cursors.count() != 0 or handle.sql_sessions.count() != 0) return capi.recordError(.busy, @src().fn_name ++ ": close SQL cursors and sessions before importing a backup");
    for (handle.table_handles.items) |id| {
        const child, const slot = handleRegistry().enter(id) orelse continue;
        _ = child;
        HandleRegistry.leave(slot);
        return capi.recordError(.busy, @src().fn_name ++ ": close table handles before importing a backup");
    }
    @import("database_backup.zig").validate(handle.alloc, backup.bytes()) catch |err| return capi.mapError(err);
    @import("sql_ddl.zig").invalidate(handle);
    handle.sql_commit_recovery_needed = true;
    if (handle.owned_lite_backend != null) {
        @import("database_backup.zig").importLite(handle, backup.bytes()) catch |err| return capi.mapError(err);
    } else {
        if (!(handle.db.isPortableImportTargetEmpty(handle.alloc) catch |err| return capi.mapError(err))) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": backup import requires an empty destination database");
        const path = handle.embedded_path orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": operation requires a file-backed Lite database");
        var io_impl = std.Io.Threaded.init(handle.alloc, .{});
        defer io_impl.deinit();
        // No child, session, or cursor borrows the runtime. Close its read lease
        // before publishing the complete directory generation, then reopen
        // whichever generation the durable publication selected.
        handle.db.close();
        handle.db_live = false;
        const restored = restorePortableBackupToDirectory(handle.alloc, io_impl.io(), path, backup.bytes(), true);
        var opts = handle.embedded_open_options;
        opts.backend_runtime = null;
        opts.identity_namespace = .{ .table_id = 1, .shard_id = 1, .range_id = 1 };
        opts.prefer_existing_identity_namespace = true;
        handle.db = db_mod.DB.open(handle.alloc, path, opts) catch |err| {
            handle.sql_decision_uncertain = true;
            return capi.mapError(err);
        };
        handle.db_live = true;
        handle.embedded_catalog_loaded = false;
        restored catch |err| return capi.mapError(err);
    }
    return .ok;
}

pub const RestoreReport = struct {
    format: []const u8,
    path: []const u8,
};

pub fn restoreFormatName(kind: StorageKind) []const u8 {
    return switch (kind) {
        .lite => "aflite",
        .directory => "directory",
    };
}

pub fn antfly_restore_backup_json(
    dest_path: ?[*:0]const u8,
    options: ?*const capi.OpenOptions,
    backup: capi.Slice,
    replace: bool,
    out_buf: ?*capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_restore_backup_json), .{ .name = "antfly_restore_backup_json" }), .{ dest_path, options, backup, replace, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const path = cStringSpan(dest_path) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    if (backup.len == 0) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": backup bytes must not be empty");
    if (backup.ptr == null and backup.len != 0) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": backup pointer must not be null when length is nonzero");
    const resolved = resolveOpenOptions(options) catch |err| return capi.mapError(err);

    const alloc = std.heap.c_allocator;
    var encoded_report = stringifyJson(RestoreReport{ .format = restoreFormatName(resolved.storage_kind), .path = path }) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const restored = switch (resolved.storage_kind) {
        .lite => restorePortableBackupToLiteFile(alloc, io_impl.io(), null, path, backup.bytes(), replace, null),
        .directory => restorePortableBackupToDirectory(alloc, io_impl.io(), path, backup.bytes(), replace),
    };
    restored catch |err| {
        antfly_buffer_free(&encoded_report);
        return capi.mapError(err);
    };
    out.* = encoded_report;
    return .ok;
}

pub fn antfly_restore_backup_file_json(
    dest_path: ?[*:0]const u8,
    options: ?*const capi.OpenOptions,
    backup_path: ?[*:0]const u8,
    replace: bool,
    out_buf: ?*capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_restore_backup_file_json), .{ .name = "antfly_restore_backup_file_json" }), .{ dest_path, options, backup_path, replace, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const destination = cStringSpan(dest_path) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    const source = cStringSpan(backup_path) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    const resolved = resolveOpenOptions(options) catch |err| return capi.mapError(err);

    const alloc = std.heap.c_allocator;
    var encoded_report = stringifyJson(RestoreReport{ .format = restoreFormatName(resolved.storage_kind), .path = destination }) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const restored = switch (resolved.storage_kind) {
        .lite => restorePortableBackupPathToLiteFile(alloc, io_impl.io(), destination, source, replace),
        .directory => restorePortableBackupPathToDirectory(alloc, io_impl.io(), destination, source, replace),
    };
    restored catch |err| {
        antfly_buffer_free(&encoded_report);
        return capi.mapError(err);
    };
    out.* = encoded_report;
    return .ok;
}

pub fn antfly_lite_check_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_lite_check_json), .{ .name = "antfly_lite_check_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    if (handle.owned_lite_backend) |*backend| {
        out.* = stringifyJson(backend.check() catch |err| return capi.mapError(err)) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        return .ok;
    }
    return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": operation requires a file-backed Lite database");
}

pub fn antfly_lite_check_file_json(path: ?[*:0]const u8, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_lite_check_file_json), .{ .name = "antfly_lite_check_file_json" }), .{ path, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const path_slice = cStringSpan(path) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
    const alloc = std.heap.c_allocator;
    const report = lite_backend.checkFile(alloc, path_slice) catch |err| return capi.mapError(err);
    out.* = stringifyJson(report) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_lite_copy_stable_snapshot_json(
    handle_ptr: ?*anyopaque,
    dest_path: ?[*:0]const u8,
    replace: bool,
    out_buf: ?*capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_lite_copy_stable_snapshot_json), .{ .name = "antfly_lite_copy_stable_snapshot_json" }), .{ handle_ptr, dest_path, replace, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const guard = enterHandle(handle_ptr, .maintain) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    if (handle.owned_lite_backend) |*backend| {
        const dest = cStringSpan(dest_path) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": path pointer must not be null");
        out.* = stringifyJson(backend.copyStableSnapshot(dest, replace) catch |err| return capi.mapError(err)) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        return .ok;
    }
    return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": operation requires a file-backed Lite database");
}

pub fn antfly_lite_copy_stable_snapshot_file_json(
    src_path: ?[*:0]const u8,
    dest_path: ?[*:0]const u8,
    replace: bool,
    out_buf: ?*capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_lite_copy_stable_snapshot_file_json), .{ .name = "antfly_lite_copy_stable_snapshot_file_json" }), .{ src_path, dest_path, replace, out_buf });
    capi.clearLastError();
    _ = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    var handle_ptr: ?*anyopaque = null;
    const open_status = antfly_lite_open_readonly(src_path, &handle_ptr);
    if (open_status != .ok) return open_status;
    defer antfly_db_close(handle_ptr);
    return antfly_lite_copy_stable_snapshot_json(handle_ptr, dest_path, replace, out_buf);
}

pub const LiteCompactReport = struct {
    compacted: bool,
    vacuum: lite_backend.VacuumReport,
};

pub fn prepareLiteCompact(handle: *Handle) !void {
    try handle.database().*.runUntilIdle();
    try handle.database().*.forceCompactTextIndexes();
    try handle.database().*.drainScheduledTextMerges();
    try handle.database().*.sync(true);
    try handle.database().*.syncIndexes(true);
}

pub fn antfly_lite_compact_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_lite_compact_json), .{ .name = "antfly_lite_compact_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const guard = enterHandle(handle_ptr, .write) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    if (handle.owned_lite_backend) |*backend| {
        prepareLiteCompact(handle) catch |err| return capi.mapError(err);
        const report = LiteCompactReport{
            .compacted = true,
            .vacuum = backend.vacuum() catch |err| return capi.mapError(err),
        };
        out.* = stringifyJson(report) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        return .ok;
    }
    return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": operation requires a file-backed Lite database");
}

pub fn antfly_lite_vacuum_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_lite_vacuum_json), .{ .name = "antfly_lite_vacuum_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const guard = enterHandle(handle_ptr, .write) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    @import("entity_sink.zig").collectEmbeddingCache(handle.parent_handle orelse handle, true) catch |err| return capi.mapError(err);
    if (handle.owned_lite_backend) |*backend| {
        out.* = stringifyJson(backend.vacuum() catch |err| return capi.mapError(err)) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        return .ok;
    }
    return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": operation requires a file-backed Lite database");
}

pub const LiteReplayGeneratedEnrichmentsReport = struct {
    replayed: usize,
};

pub fn antfly_db_replay_generated_enrichments_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_replay_generated_enrichments_json), .{ .name = "antfly_db_replay_generated_enrichments_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const guard = enterHandle(handle_ptr, .maintain) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const replayed = handle.database().*.replayGeneratedEnrichmentsFromStoredDocs(handle.alloc) catch |err| return capi.mapError(err);
    out.* = stringifyJson(LiteReplayGeneratedEnrichmentsReport{ .replayed = replayed }) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn restorePortableBackupToLiteFileWithRuntime(
    alloc: Allocator,
    io: std.Io,
    backend_runtime: *db_mod.background_runtime.BackendRuntime,
    dest_path: []const u8,
    backup: []const u8,
    replace: bool,
    cancel: ?*const antfly.storage_maintenance.CancelToken,
) !void {
    try restorePortableBackupToLiteFile(alloc, io, backend_runtime, dest_path, backup, replace, cancel);
}

pub fn restorePortableBackupToLiteFile(
    alloc: Allocator,
    io: std.Io,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    dest_path: []const u8,
    backup: []const u8,
    replace: bool,
    cancel: ?*const antfly.storage_maintenance.CancelToken,
) !void {
    if (!lite_backend.isAflitePath(dest_path)) return error.InvalidArgument;
    if (backup.len == 0) return error.InvalidArgument;

    const Populate = struct {
        pub fn run(context: []const u8, alloc_inner: Allocator, db: *db_mod.DB, backend: *lite_backend.Handle, _: std.Io) !void {
            try @import("database_backup.zig").populate(alloc_inner, db, backend, db.core.path, context);
        }
    };
    try restorePortableSourceToLiteFile(alloc, io, backend_runtime, dest_path, replace, backup, Populate.run, cancel);
}

/// Restores a portable backup into a directory database at `dest_path`.
///
/// Uses the engine's generation lifecycle, the same publication path as
/// server-side table restores:
/// - The exclusive generation transition fails with BUSY while any database
///   (libantfly handle, server, or CLI, in this process or another) holds a
///   read lease on the destination, and new opens wait or fail until
///   publication finishes.
/// - The database is built and indexed in an unpublished sibling generation,
///   sealed, then published with a durable marker and an atomic exchange (or
///   rename when nothing exists yet), and the parent directory is synced.
/// - A crash at any point leaves `dest_path` holding the complete old or the
///   complete new database; the next open reconciles the marker and
///   reclaims the other generation.
pub fn restorePortableBackupToDirectory(
    alloc: Allocator,
    io: std.Io,
    dest_path: []const u8,
    backup: []const u8,
    replace: bool,
) !void {
    if (backup.len == 0) return error.InvalidArgument;
    const live_path = std.mem.trimEnd(u8, dest_path, "/");
    if (live_path.len == 0) return error.InvalidArgument;
    var transition = try db_mod.generation_lifecycle.beginProcessExclusiveWithIo(live_path, io);
    defer transition.deinit();
    // Reconcile an interrupted earlier publication before deciding whether
    // the destination exists.
    try transition.reconcilePublished();
    if (capiPathExists(io, live_path) and !replace) return error.PathAlreadyExists;

    var staged = try transition.beginStaging();
    defer staged.deinit();
    {
        var db = try db_mod.DB.open(alloc, staged.path(), .{ .staged_generation = &staged, .identity_namespace = try @import("database_backup.zig").rootIdentity(alloc, backup), .external_derived_checkpoints = false });
        defer db.close();
        try @import("database_backup.zig").populate(alloc, &db, null, staged.path(), backup);
    }
    switch (try staged.publish()) {
        .durable => {},
        .durability_uncertain => {
            std.log.err("directory restore published but crash durability could not be confirmed path={s}", .{live_path});
            return error.DurabilityOutcomeUnknown;
        },
    }
}

pub fn restorePortableBackupPathToDirectory(
    alloc: Allocator,
    io: std.Io,
    dest_path: []const u8,
    backup_path: []const u8,
    replace: bool,
) !void {
    if (!std.mem.endsWith(u8, backup_path, ".afb")) return error.InvalidArgument;
    var backup = try @import("database_backup.zig").MappedBackup.open(io, backup_path);
    defer backup.deinit(io);
    try restorePortableBackupToDirectory(alloc, io, dest_path, backup.bytes, replace);
}

pub fn restorePortableBackupPathToLiteFile(
    alloc: Allocator,
    io: std.Io,
    dest_path: []const u8,
    backup_path: []const u8,
    replace: bool,
) !void {
    if (!std.mem.endsWith(u8, backup_path, ".afb")) return error.InvalidArgument;
    var backup = try @import("database_backup.zig").MappedBackup.open(io, backup_path);
    defer backup.deinit(io);
    try restorePortableBackupToLiteFile(alloc, io, null, dest_path, backup.bytes, replace, null);
}

pub fn restorePortableSourceToLiteFile(
    alloc: Allocator,
    io: std.Io,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    dest_path: []const u8,
    replace: bool,
    context: anytype,
    comptime populate: anytype,
    cancel: ?*const antfly.storage_maintenance.CancelToken,
) !void {
    if (!lite_backend.isAflitePath(dest_path)) return error.InvalidArgument;
    if (cancel) |token| try token.check();

    const dest_exists = capiPathExists(io, dest_path);
    if (dest_exists and !replace) return error.PathAlreadyExists;

    var dest_lock = try antfly.lite.native.lockWriterPathWithIo(alloc, io, dest_path);
    defer dest_lock.close();

    if (!dest_exists and !replace and capiPathExists(io, dest_path)) return error.PathAlreadyExists;

    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.restore-tmp.aflite", .{dest_path});
    defer alloc.free(tmp_path);
    try capiDeleteFileIfExists(io, tmp_path);
    errdefer capiDeleteFilePath(io, tmp_path) catch {};

    {
        var backend = try lite_backend.Handle.createWithOptions(alloc, tmp_path, .{
            .exclusive = true,
            .io = io,
        });
        defer backend.deinit();

        var opts = db_mod.OpenOptions{
            .open_mode = .writer,
            .external_derived_checkpoints = false,
            .backend_runtime = backend_runtime,
            .identity_namespace = try @import("database_backup.zig").rootIdentity(alloc, context),
            .prefer_existing_identity_namespace = false,
        };
        // A caller-supplied std.Io runtime may be cooperative (VoprIo) rather
        // than backed by std.Io.Threaded. Restore is synchronous, so it must
        // not select the executor variant that requires an owned Threaded
        // implementation merely because the runtime exposes an Io interface.
        if (backend_runtime != null) opts.executor = .{ .backend = .manual };
        try backend.configureDbOpenOptions(&opts);

        var db = try db_mod.DB.open(alloc, tmp_path, opts);
        defer db.close();
        try populate(context, alloc, &db, &backend, io);
    }

    if (cancel) |token| try token.check();

    capiRenameFilePath(io, tmp_path, dest_path) catch |err| {
        capiDeleteFilePath(io, tmp_path) catch {};
        return err;
    };
    lite_restore_staging.confirmPublishedFileDurability(io, dest_path) catch |err| {
        std.log.err(
            "Lite restore published but crash durability could not be confirmed path={s} class={s}",
            .{ dest_path, @errorName(err) },
        );
        return error.DurabilityOutcomeUnknown;
    };
}

pub fn capiPathExists(io: std.Io, path: []const u8) bool {
    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.accessAbsolute(io, path, .{}) catch return false;
    } else {
        std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    }
    return true;
}

pub fn capiDeleteFileIfExists(io: std.Io, path: []const u8) !void {
    capiDeleteFilePath(io, path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
}

pub fn capiRenameFilePath(io: std.Io, old_path: []const u8, new_path: []const u8) !void {
    if (std.fs.path.isAbsolute(old_path) or std.fs.path.isAbsolute(new_path)) {
        try std.Io.Dir.renameAbsolute(old_path, new_path, io);
    } else {
        try std.Io.Dir.rename(std.Io.Dir.cwd(), old_path, std.Io.Dir.cwd(), new_path, io);
    }
}

pub fn capiDeleteFilePath(io: std.Io, path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.deleteFileAbsolute(io, path);
    } else {
        try std.Io.Dir.cwd().deleteFile(io, path);
    }
}

pub fn antfly_db_set_readable_lease_hook(
    handle_ptr: ?*anyopaque,
    group_id: u64,
    callback_ctx: ?*anyopaque,
    callback: ?ReadableLeaseHookFn,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_set_readable_lease_hook), .{ .name = "antfly_db_set_readable_lease_hook" }), .{ handle_ptr, group_id, callback_ctx, callback });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    if (callback) |hook| {
        handle.readable_lease_hook = .{
            .group_id = group_id,
            .callback_ctx = callback_ctx,
            .callback = hook,
        };
    } else {
        handle.readable_lease_hook = null;
    }
    return .ok;
}

pub fn antfly_buffer_free(buffer: ?*capi.Buffer) callconv(.c) void {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_buffer_free), .{ .name = "antfly_buffer_free" }), .{buffer});
    const out = buffer orelse return;
    freeRawBuffer(out.ptr, out.len);
    out.* = .{};
}

pub fn wipeBufferBytes(buffer: capi.Buffer) void {
    if (buffer.ptr == null or buffer.len == 0) return;
    std.crypto.secureZero(u8, buffer.ptr.?[0..buffer.len]);
}

pub fn antfly_buffer_free_zero(buffer: ?*capi.Buffer) callconv(.c) void {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_buffer_free_zero), .{ .name = "antfly_buffer_free_zero" }), .{buffer});
    const out = buffer orelse return;
    wipeBufferBytes(out.*);
    antfly_buffer_free(out);
}

pub fn antfly_dense_search_result_free(result: *capi.DenseSearchResult) callconv(.c) void {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_dense_search_result_free), .{ .name = "antfly_dense_search_result_free" }), .{result});
    if (result.hits_ptr) |hits_ptr| {
        const hits = hits_ptr[0..result.hit_count];
        for (hits) |hit| {
            if (hit.id_ptr != null and hit.id_len > 0) {
                std.heap.c_allocator.free(hit.id_ptr.?[0..hit.id_len]);
            }
        }
        std.heap.c_allocator.free(hits);
    }
    result.* = .{};
}

pub fn antfly_packed_dense_search_result_free(result: *capi.PackedDenseSearchResult) callconv(.c) void {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_packed_dense_search_result_free), .{ .name = "antfly_packed_dense_search_result_free" }), .{result});
    if (result.hits_ptr) |hits_ptr| {
        const hits = hits_ptr[0..result.hit_count];
        std.heap.c_allocator.free(hits);
    }
    if (result.ids_ptr != null and result.ids_len > 0) {
        std.heap.c_allocator.free(result.ids_ptr.?[0..result.ids_len]);
    }
    result.* = .{};
}

pub fn packDenseHits(
    total_hits: u32,
    ids: []const []const u8,
    scores: []const f32,
    identity_read_generation: u64,
    out_result: *capi.PackedDenseSearchResult,
) !void {
    std.debug.assert(ids.len == scores.len);

    const alloc = std.heap.c_allocator;
    const hits = try alloc.alloc(capi.PackedDenseSearchHit, ids.len);
    errdefer alloc.free(hits);

    var ids_len: usize = 0;
    for (ids) |id| ids_len += id.len;
    const ids_blob = try alloc.alloc(u8, ids_len);
    errdefer alloc.free(ids_blob);

    var cursor: usize = 0;
    for (ids, scores, 0..) |id, score, i| {
        @memcpy(ids_blob[cursor..][0..id.len], id);
        hits[i] = .{
            .id_offset = cursor,
            .id_len = id.len,
            .score = score,
        };
        cursor += id.len;
    }

    out_result.* = .{
        .hits_ptr = if (hits.len > 0) hits.ptr else null,
        .hit_count = hits.len,
        .total_hits = total_hits,
        .ids_ptr = if (ids_blob.len > 0) ids_blob.ptr else null,
        .ids_len = ids_blob.len,
        .identity_read_generation = identity_read_generation,
    };
}

pub const DenseOwnedResult = struct {
    alloc: Allocator,
    total_hits: u32,
    ids: [][]const u8,
    scores: []f32,
    identity_read_generation: u64,

    pub fn deinit(self: *DenseOwnedResult) void {
        for (self.ids) |id| self.alloc.free(id);
        if (self.ids.len > 0) self.alloc.free(self.ids);
        if (self.scores.len > 0) self.alloc.free(self.scores);
        self.* = undefined;
    }
};

pub const DenseOwnedProfile = struct {
    result: DenseOwnedResult,
    total_ns: u64 = 0,
    index_lookup_ns: u64 = 0,
    search_ns: u64 = 0,
    hits_ns: u64 = 0,
    fallback_ns: u64 = 0,
    hbc_total_ns: u64 = 0,
    hbc_setup_ns: u64 = 0,
    hbc_root_load_ns: u64 = 0,
    hbc_node_cache_miss_ns: u64 = 0,
    hbc_node_cache_misses: u64 = 0,
    hbc_quantized_cache_miss_ns: u64 = 0,
    hbc_quantized_cache_misses: u64 = 0,
    hbc_child_expand_ns: u64 = 0,
    hbc_leaf_score_ns: u64 = 0,
    hbc_rerank_ns: u64 = 0,
    hbc_rerank_vector_load_ns: u64 = 0,
    hbc_rerank_distance_ns: u64 = 0,
    hbc_nodes_visited: u64 = 0,
    hbc_leaves_explored: u64 = 0,
    hbc_reranked_vectors: u64 = 0,
    hit_count: u32 = 0,
    total_hits: u32 = 0,
    used_fast_path: bool = false,

    pub fn deinit(self: *DenseOwnedProfile) void {
        self.result.deinit();
        self.* = undefined;
    }

    pub fn takeResult(self: *DenseOwnedProfile) DenseOwnedResult {
        const result = self.result;
        self.result = .{
            .alloc = result.alloc,
            .total_hits = 0,
            .ids = &.{},
            .scores = &.{},
            .identity_read_generation = result.identity_read_generation,
        };
        return result;
    }
};

pub const DenseResolvedHit = struct {
    id: []u8,
    score: f32,
};

pub const DenseResolvedHits = struct {
    alloc: Allocator,
    total_hits: u32,
    hits: []DenseResolvedHit,

    pub fn deinit(self: *DenseResolvedHits) void {
        for (self.hits) |hit| self.alloc.free(hit.id);
        if (self.hits.len > 0) self.alloc.free(self.hits);
        self.* = undefined;
    }
};

pub const DenseWireOwnedProfile = struct {
    out: capi.Buffer = .{},
    total_ns: u64 = 0,
    decode_ns: u64 = 0,
    search_ns: u64 = 0,
    resolve_ns: u64 = 0,
    encode_ns: u64 = 0,
    fallback_ns: u64 = 0,
    hbc_total_ns: u64 = 0,
    hbc_setup_ns: u64 = 0,
    hbc_root_load_ns: u64 = 0,
    hbc_node_cache_miss_ns: u64 = 0,
    hbc_node_cache_misses: u64 = 0,
    hbc_quantized_cache_miss_ns: u64 = 0,
    hbc_quantized_cache_misses: u64 = 0,
    hbc_child_expand_ns: u64 = 0,
    hbc_leaf_score_ns: u64 = 0,
    hbc_rerank_ns: u64 = 0,
    hbc_rerank_vector_load_ns: u64 = 0,
    hbc_rerank_distance_ns: u64 = 0,
    hbc_nodes_visited: u64 = 0,
    hbc_leaves_explored: u64 = 0,
    hbc_reranked_vectors: u64 = 0,
    hit_count: u32 = 0,
    total_hits: u32 = 0,
    used_fast_path: bool = false,
};

pub fn resolveDenseHitsFromProfiled(
    alloc: Allocator,
    entry: anytype,
    results: *hbc.ProfiledSearchResults,
    limit: u32,
    offset: u32,
) !DenseResolvedHits {
    const raw_hits = results.results.getHits();
    const start: u32 = @min(offset, @as(u32, @intCast(raw_hits.len)));
    const end: u32 = @min(start + limit, @as(u32, @intCast(raw_hits.len)));
    const sliced_hits = raw_hits[@intCast(start)..@intCast(end)];

    const resolved = try alloc.alloc(DenseResolvedHit, sliced_hits.len);
    errdefer alloc.free(resolved);
    var resolved_count: usize = 0;
    errdefer {
        for (resolved[0..resolved_count]) |hit| alloc.free(hit.id);
    }

    for (sliced_hits, 0..) |hit, i| {
        const result_index: usize = @as(usize, @intCast(start)) + i;
        const id = if (results.results.takeMetadata(result_index)) |metadata|
            metadata
        else
            (try entry.index.getMetadata(hit.vector_id)) orelse return error.Internal;
        resolved[i] = .{
            .id = id,
            .score = vector_mod.similarityFromDistance(hit.distance, entry.metric),
        };
        resolved_count += 1;
    }

    return .{
        .alloc = alloc,
        .total_hits = @intCast(raw_hits.len),
        .hits = resolved,
    };
}

pub fn packResolvedDenseHits(
    resolved: *DenseResolvedHits,
    identity_read_generation: u64,
    out_result: *capi.PackedDenseSearchResult,
) !void {
    const alloc = std.heap.c_allocator;
    const hits = try alloc.alloc(capi.PackedDenseSearchHit, resolved.hits.len);
    errdefer alloc.free(hits);

    var ids_len: usize = 0;
    for (resolved.hits) |hit| ids_len += hit.id.len;
    const ids_blob = try alloc.alloc(u8, ids_len);
    errdefer alloc.free(ids_blob);

    var cursor: usize = 0;
    for (resolved.hits, 0..) |hit, i| {
        @memcpy(ids_blob[cursor..][0..hit.id.len], hit.id);
        hits[i] = .{
            .id_offset = cursor,
            .id_len = hit.id.len,
            .score = hit.score,
        };
        cursor += hit.id.len;
    }

    out_result.* = .{
        .hits_ptr = if (hits.len > 0) hits.ptr else null,
        .hit_count = hits.len,
        .total_hits = resolved.total_hits,
        .ids_ptr = if (ids_blob.len > 0) ids_blob.ptr else null,
        .ids_len = ids_blob.len,
        .identity_read_generation = identity_read_generation,
    };
}

pub fn encodeResolvedDenseWireResponse(
    resolved: *DenseResolvedHits,
    identity_read_generation: u64,
) !capi.Buffer {
    const header_len: usize = 4 + 2 + 2 + 4 + 4 + 4;
    const hits_len: usize = resolved.hits.len * @sizeOf(search_wire.PackedHit);
    var ids_len: usize = 0;
    for (resolved.hits) |hit| ids_len += hit.id.len;
    const total_len = header_len + hits_len + ids_len + @sizeOf(u64);
    const out = try std.heap.c_allocator.alloc(u8, total_len);
    errdefer std.heap.c_allocator.free(out);

    var cursor: usize = 0;
    std.mem.writeInt(u32, out[cursor..][0..4], search_wire.magic, .little);
    cursor += 4;
    std.mem.writeInt(u16, out[cursor..][0..2], search_wire.version, .little);
    cursor += 2;
    std.mem.writeInt(u16, out[cursor..][0..2], @backingInt(search_wire.Op.dense_search), .little);
    cursor += 2;
    std.mem.writeInt(u32, out[cursor..][0..4], resolved.total_hits, .little);
    cursor += 4;
    std.mem.writeInt(u32, out[cursor..][0..4], @intCast(resolved.hits.len), .little);
    cursor += 4;
    std.mem.writeInt(u32, out[cursor..][0..4], @intCast(ids_len), .little);
    cursor += 4;

    var id_cursor: u32 = 0;
    for (resolved.hits) |hit| {
        std.mem.writeInt(u32, out[cursor..][0..4], id_cursor, .little);
        cursor += 4;
        std.mem.writeInt(u16, out[cursor..][0..2], @intCast(hit.id.len), .little);
        cursor += 2;
        std.mem.writeInt(u16, out[cursor..][0..2], 0, .little);
        cursor += 2;
        std.mem.writeInt(u32, out[cursor..][0..4], @bitCast(hit.score), .little);
        cursor += 4;
        id_cursor += @intCast(hit.id.len);
    }

    for (resolved.hits) |hit| {
        @memcpy(out[cursor..][0..hit.id.len], hit.id);
        cursor += hit.id.len;
    }
    std.mem.writeInt(u64, out[cursor..][0..8], identity_read_generation, .little);

    return .{ .ptr = out.ptr, .len = out.len };
}

pub fn searchDensePackedFast(
    handle: *Handle,
    index_name: []const u8,
    vector: []const f32,
    k: u32,
    limit: u32,
    offset: u32,
    identity_read_generation: u64,
    out_result: *capi.PackedDenseSearchResult,
) !bool {
    if (handle.database().*.core.schema != null and handle.database().*.core.schema.?.ttl_duration_ns != 0) return false;
    const entry = handle.database().*.core.index_manager.denseIndex(index_name) orelse return false;
    if (entry.chunk_name != null) return false;

    var profiled = try entry.index.searchProfiledRequest(.{
        .query = vector,
        .k = k,
    });
    defer profiled.results.deinit();

    var resolved = try resolveDenseHitsFromProfiled(handle.alloc, entry, &profiled, limit, offset);
    defer resolved.deinit();
    try packResolvedDenseHits(&resolved, identity_read_generation, out_result);
    return true;
}

pub fn searchDenseWireFast(
    handle: *Handle,
    index_name: []const u8,
    vector: []const f32,
    k: u32,
    limit: u32,
    offset: u32,
    identity_read_generation: u64,
) !?capi.Buffer {
    if (handle.database().*.core.schema != null and handle.database().*.core.schema.?.ttl_duration_ns != 0) return null;
    const entry = handle.database().*.core.index_manager.denseIndex(index_name) orelse return null;
    if (entry.chunk_name != null) return null;

    var profiled = try entry.index.searchProfiledRequest(.{
        .query = vector,
        .k = k,
    });
    defer profiled.results.deinit();

    var resolved = try resolveDenseHitsFromProfiled(handle.alloc, entry, &profiled, limit, offset);
    defer resolved.deinit();
    return try encodeResolvedDenseWireResponse(&resolved, identity_read_generation);
}

pub fn searchDenseWireOwnedProfiled(
    handle: *Handle,
    request_buf: []const u8,
) !DenseWireOwnedProfile {
    const total_start = monotonicNowNs();

    const decode_start = monotonicNowNs();
    var req = try search_wire.decodeDenseRequest(handle.alloc, request_buf);
    defer search_wire.freeDenseRequest(handle.alloc, &req);
    const decode_end = monotonicNowNs();
    const identity_read_generation = try currentIdentityReadGenerationForHandle(handle, null);

    if (handle.database().*.core.schema == null or handle.database().*.core.schema.?.ttl_duration_ns == 0) {
        if (handle.database().*.core.index_manager.denseIndex(req.index_name)) |entry| {
            if (entry.chunk_name == null) {
                const search_start = monotonicNowNs();
                var profiled = try entry.index.searchProfiledRequest(.{
                    .query = req.vector,
                    .k = req.k,
                });
                defer profiled.results.deinit();
                const search_end = monotonicNowNs();

                const resolve_start = monotonicNowNs();
                var resolved = try resolveDenseHitsFromProfiled(handle.alloc, entry, &profiled, req.limit, req.offset);
                defer resolved.deinit();
                const resolve_end = monotonicNowNs();

                const encode_start = monotonicNowNs();
                const out = try encodeResolvedDenseWireResponse(&resolved, identity_read_generation);
                const encode_end = monotonicNowNs();

                return .{
                    .out = out,
                    .total_ns = @intCast(encode_end - total_start),
                    .decode_ns = @intCast(decode_end - decode_start),
                    .search_ns = @intCast(search_end - search_start),
                    .resolve_ns = @intCast(resolve_end - resolve_start),
                    .encode_ns = @intCast(encode_end - encode_start),
                    .fallback_ns = 0,
                    .hbc_total_ns = profiled.profile.total_ns,
                    .hbc_setup_ns = profiled.profile.setup_ns,
                    .hbc_root_load_ns = profiled.profile.root_load_ns,
                    .hbc_node_cache_miss_ns = profiled.profile.node_cache_miss_ns,
                    .hbc_node_cache_misses = profiled.profile.node_cache_misses,
                    .hbc_quantized_cache_miss_ns = profiled.profile.quantized_cache_miss_ns,
                    .hbc_quantized_cache_misses = profiled.profile.quantized_cache_misses,
                    .hbc_child_expand_ns = profiled.profile.child_expand_ns,
                    .hbc_leaf_score_ns = profiled.profile.leaf_score_ns,
                    .hbc_rerank_ns = profiled.profile.rerank_ns,
                    .hbc_rerank_vector_load_ns = profiled.profile.rerank_vector_load_ns,
                    .hbc_rerank_distance_ns = profiled.profile.rerank_distance_ns,
                    .hbc_nodes_visited = profiled.profile.nodes_visited,
                    .hbc_leaves_explored = profiled.profile.leaves_explored,
                    .hbc_reranked_vectors = profiled.profile.reranked_vectors,
                    .hit_count = @intCast(resolved.hits.len),
                    .total_hits = resolved.total_hits,
                    .used_fast_path = true,
                };
            }
        }
    }

    const fallback_start = monotonicNowNs();
    var owned = try searchDenseOwned(handle, req.index_name, req.vector, req.k, req.limit, req.offset);
    defer owned.deinit();
    const fallback_end = monotonicNowNs();

    const encode_start = monotonicNowNs();
    const out = try search_wire.encodeDenseResponseAtGeneration(owned.total_hits, owned.ids, owned.scores, owned.identity_read_generation);
    const encode_end = monotonicNowNs();

    return .{
        .out = out,
        .total_ns = @intCast(encode_end - total_start),
        .decode_ns = @intCast(decode_end - decode_start),
        .search_ns = 0,
        .resolve_ns = 0,
        .encode_ns = @intCast(encode_end - encode_start),
        .fallback_ns = @intCast(fallback_end - fallback_start),
        .hbc_total_ns = 0,
        .hbc_setup_ns = 0,
        .hbc_root_load_ns = 0,
        .hbc_node_cache_miss_ns = 0,
        .hbc_node_cache_misses = 0,
        .hbc_quantized_cache_miss_ns = 0,
        .hbc_quantized_cache_misses = 0,
        .hbc_child_expand_ns = 0,
        .hbc_leaf_score_ns = 0,
        .hbc_rerank_ns = 0,
        .hbc_rerank_vector_load_ns = 0,
        .hbc_rerank_distance_ns = 0,
        .hbc_nodes_visited = 0,
        .hbc_leaves_explored = 0,
        .hbc_reranked_vectors = 0,
        .hit_count = @intCast(owned.ids.len),
        .total_hits = owned.total_hits,
        .used_fast_path = false,
    };
}

pub fn searchDenseOwned(
    handle: *Handle,
    index_name: []const u8,
    vector: []const f32,
    k: u32,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    var profiled = try searchDenseOwnedProfiled(handle, index_name, vector, k, limit, offset);
    defer profiled.deinit();
    return profiled.takeResult();
}

pub fn searchDenseOwnedProfiled(
    handle: *Handle,
    index_name: []const u8,
    vector: []const f32,
    k: u32,
    limit: u32,
    offset: u32,
) !DenseOwnedProfile {
    if (vector.len == 0) return error.InvalidArgument;
    const identity_read_generation = try currentIdentityReadGenerationForHandle(handle, null);

    const total_start = monotonicNowNs();
    const lookup_start = monotonicNowNs();
    if (handle.database().*.core.schema == null or handle.database().*.core.schema.?.ttl_duration_ns == 0) {
        if (handle.database().*.core.index_manager.denseIndex(index_name)) |entry| {
            const lookup_end = monotonicNowNs();
            if (entry.chunk_name == null) {
                const search_start = monotonicNowNs();
                var profiled = try entry.index.searchProfiledRequest(.{
                    .query = vector,
                    .k = k,
                });
                defer profiled.results.deinit();
                const search_end = monotonicNowNs();

                const raw_hits = profiled.results.getHits();
                const start: u32 = @min(offset, @as(u32, @intCast(raw_hits.len)));
                const end: u32 = @min(start + limit, @as(u32, @intCast(raw_hits.len)));
                const sliced_hits = raw_hits[@intCast(start)..@intCast(end)];

                const hits_start = monotonicNowNs();
                const ids = try handle.alloc.alloc([]const u8, sliced_hits.len);
                errdefer handle.alloc.free(ids);
                var id_count: usize = 0;
                errdefer {
                    for (ids[0..id_count]) |id| handle.alloc.free(id);
                }

                const scores = try handle.alloc.alloc(f32, sliced_hits.len);
                errdefer handle.alloc.free(scores);

                for (sliced_hits, 0..) |hit, i| {
                    const result_index: usize = @as(usize, @intCast(start)) + i;
                    const id = if (profiled.results.takeMetadata(result_index)) |metadata|
                        metadata
                    else
                        (try entry.index.getMetadata(hit.vector_id)) orelse return error.Internal;
                    ids[i] = id;
                    id_count += 1;
                    scores[i] = hit.distance;
                }
                const hits_end = monotonicNowNs();

                return .{
                    .result = .{
                        .alloc = handle.alloc,
                        .total_hits = @intCast(raw_hits.len),
                        .ids = ids,
                        .scores = scores,
                        .identity_read_generation = identity_read_generation,
                    },
                    .total_ns = @intCast(hits_end - total_start),
                    .index_lookup_ns = @intCast(lookup_end - lookup_start),
                    .search_ns = @intCast(search_end - search_start),
                    .hits_ns = @intCast(hits_end - hits_start),
                    .fallback_ns = 0,
                    .hbc_total_ns = profiled.profile.total_ns,
                    .hbc_setup_ns = profiled.profile.setup_ns,
                    .hbc_root_load_ns = profiled.profile.root_load_ns,
                    .hbc_node_cache_miss_ns = profiled.profile.node_cache_miss_ns,
                    .hbc_node_cache_misses = profiled.profile.node_cache_misses,
                    .hbc_quantized_cache_miss_ns = profiled.profile.quantized_cache_miss_ns,
                    .hbc_quantized_cache_misses = profiled.profile.quantized_cache_misses,
                    .hbc_child_expand_ns = profiled.profile.child_expand_ns,
                    .hbc_leaf_score_ns = profiled.profile.leaf_score_ns,
                    .hbc_rerank_ns = profiled.profile.rerank_ns,
                    .hbc_rerank_vector_load_ns = profiled.profile.rerank_vector_load_ns,
                    .hbc_rerank_distance_ns = profiled.profile.rerank_distance_ns,
                    .hbc_nodes_visited = profiled.profile.nodes_visited,
                    .hbc_leaves_explored = profiled.profile.leaves_explored,
                    .hbc_reranked_vectors = profiled.profile.reranked_vectors,
                    .hit_count = @intCast(sliced_hits.len),
                    .total_hits = @intCast(raw_hits.len),
                    .used_fast_path = true,
                };
            }
        }
    }
    const lookup_end = monotonicNowNs();

    var req: db_mod.types.SearchRequest = .{
        .index_name = index_name,
        .query = .{ .dense_knn = .{
            .vector = vector,
            .k = k,
        } },
        .limit = limit,
        .offset = offset,
        .include_stored = false,
    };

    // The export already ran prepareDenseSearchRequest, so skip the generic
    // lease hook; restamp and retry like the other search paths.
    const fallback_start = monotonicNowNs();
    var result = try runAtStampedGenerationWithOptions(handle, &req, LocalSearchQuery{}, .{ .prepare = false });
    defer result.deinit();
    const fallback_end = monotonicNowNs();
    const fallback_generation = req.identity_read_generation.?;

    const ids = try handle.alloc.alloc([]const u8, result.hits.len);
    errdefer handle.alloc.free(ids);
    var id_count: usize = 0;
    errdefer {
        for (ids[0..id_count]) |id| handle.alloc.free(id);
    }

    const scores = try handle.alloc.alloc(f32, result.hits.len);
    errdefer handle.alloc.free(scores);
    for (result.hits, 0..) |hit, i| {
        ids[i] = try handle.alloc.dupe(u8, hit.id);
        id_count += 1;
        scores[i] = hit.score orelse 0;
    }
    const total_end = monotonicNowNs();
    return .{
        .result = .{
            .alloc = handle.alloc,
            .total_hits = result.total_hits,
            .ids = ids,
            .scores = scores,
            .identity_read_generation = fallback_generation,
        },
        .total_ns = @intCast(total_end - total_start),
        .index_lookup_ns = @intCast(lookup_end - lookup_start),
        .search_ns = 0,
        .hits_ns = 0,
        .fallback_ns = @intCast(fallback_end - fallback_start),
        .hbc_total_ns = 0,
        .hbc_setup_ns = 0,
        .hbc_root_load_ns = 0,
        .hbc_node_cache_miss_ns = 0,
        .hbc_node_cache_misses = 0,
        .hbc_quantized_cache_miss_ns = 0,
        .hbc_quantized_cache_misses = 0,
        .hbc_child_expand_ns = 0,
        .hbc_leaf_score_ns = 0,
        .hbc_rerank_ns = 0,
        .hbc_rerank_vector_load_ns = 0,
        .hbc_rerank_distance_ns = 0,
        .hbc_nodes_visited = 0,
        .hbc_leaves_explored = 0,
        .hbc_reranked_vectors = 0,
        .hit_count = @intCast(result.hits.len),
        .total_hits = result.total_hits,
        .used_fast_path = false,
    };
}

pub fn searchTextMatchOwned(
    handle: *Handle,
    index_name: []const u8,
    field: []const u8,
    text: []const u8,
    analyzer: []const u8,
    boost: f32,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    if (field.len == 0 or text.len == 0) return error.InvalidArgument;

    return searchTextOwned(handle, index_name, .{
        .match = .{
            .field = field,
            .text = text,
            .analyzer = if (analyzer.len > 0) analyzer else null,
            .boost = boost,
        },
    }, limit, offset);
}

pub fn searchTextTermOwned(
    handle: *Handle,
    index_name: []const u8,
    field: []const u8,
    term: []const u8,
    boost: f32,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    if (field.len == 0 or term.len == 0) return error.InvalidArgument;

    return searchTextOwned(handle, index_name, .{
        .term = .{
            .field = field,
            .term = term,
            .boost = boost,
        },
    }, limit, offset);
}

pub fn searchTextMatchPhraseOwned(
    handle: *Handle,
    index_name: []const u8,
    field: []const u8,
    text: []const u8,
    analyzer: []const u8,
    fuzziness: u16,
    auto: bool,
    boost: f32,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    if (field.len == 0 or text.len == 0) return error.InvalidArgument;

    return searchTextOwned(handle, index_name, .{
        .match_phrase = .{
            .field = field,
            .text = text,
            .analyzer = if (analyzer.len > 0) analyzer else null,
            .max_edits = @intCast(fuzziness),
            .auto_fuzzy = auto,
            .boost = boost,
        },
    }, limit, offset);
}

pub fn searchTextOwned(
    handle: *Handle,
    index_name: []const u8,
    query: db_mod.types.Query,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    // An empty index name aliases the default full-text index, matching the
    // server's public-query resolution (see api/tables.zig). A name that
    // does not resolve to an existing index still fails inside
    // executeLocalSearch below.
    const resolved_index_name = if (index_name.len == 0) tables_api.default_full_text_index_name else index_name;

    var req: db_mod.types.SearchRequest = .{
        .index_name = resolved_index_name,
        .query = query,
        .limit = limit,
        .offset = offset,
        .include_stored = false,
    };
    var result = try runAtStampedGeneration(handle, &req, LocalSearchQuery{});
    defer result.deinit();
    const identity_read_generation = req.identity_read_generation.?;

    const ids = try handle.alloc.alloc([]const u8, result.hits.len);
    errdefer handle.alloc.free(ids);
    var id_count: usize = 0;
    errdefer {
        for (ids[0..id_count]) |id| handle.alloc.free(id);
    }

    const scores = try handle.alloc.alloc(f32, result.hits.len);
    errdefer handle.alloc.free(scores);
    for (result.hits, 0..) |hit, i| {
        ids[i] = try handle.alloc.dupe(u8, hit.id);
        id_count += 1;
        scores[i] = hit.score orelse 0;
    }

    return .{
        .alloc = handle.alloc,
        .total_hits = result.total_hits,
        .ids = ids,
        .scores = scores,
        .identity_read_generation = identity_read_generation,
    };
}

pub fn antfly_scan_hash_result_free(result: *capi.ScanHashResult) callconv(.c) void {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_scan_hash_result_free), .{ .name = "antfly_scan_hash_result_free" }), .{result});
    if (result.entries_ptr) |entries_ptr| {
        const entries = entries_ptr[0..result.entry_count];
        for (entries) |entry| {
            if (entry.id_ptr != null and entry.id_len > 0) {
                std.heap.c_allocator.free(entry.id_ptr.?[0..entry.id_len]);
            }
        }
        std.heap.c_allocator.free(entries);
    }
    result.* = .{};
}

pub fn antfly_db_begin_transaction_with_id(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: ?*const [16]u8,
    timestamp_ns: u64,
    participants_ptr: ?[*]const capi.Slice,
    participant_count: usize,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_begin_transaction_with_id), .{ .name = "antfly_db_begin_transaction_with_id" }), .{ handle_ptr, txn_id_ptr, timestamp_ns, participants_ptr, participant_count });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .write) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const txn_id = txn_id_ptr orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": transaction ID pointer must not be null");
    beginWithIdAndParticipants(handle, txn_id.*, timestamp_ns, participants_ptr, participant_count) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_write_transaction(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: ?*const [16]u8,
    writes_ptr: ?[*]const capi.WriteIntent,
    write_count: usize,
    predicates_ptr: ?[*]const capi.VersionPredicate,
    predicate_count: usize,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_write_transaction), .{ .name = "antfly_db_write_transaction" }), .{ handle_ptr, txn_id_ptr, writes_ptr, write_count, predicates_ptr, predicate_count });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .write) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const txn_id = txn_id_ptr orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": transaction ID pointer must not be null");
    if ((write_count > 0 and writes_ptr == null) or (predicate_count > 0 and predicates_ptr == null)) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": write and predicate pointers must not be null when their counts are nonzero");
    writeIntentsInternal(handle, txn_id.*, writes_ptr, write_count, predicates_ptr, predicate_count) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_batch(
    handle_ptr: ?*anyopaque,
    writes_ptr: ?[*]const capi.WriteIntent,
    write_count: usize,
    predicates_ptr: ?[*]const capi.VersionPredicate,
    predicate_count: usize,
    timestamp_ns: u64,
    sync_level: u8,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_batch), .{ .name = "antfly_db_batch" }), .{ handle_ptr, writes_ptr, write_count, predicates_ptr, predicate_count, timestamp_ns, sync_level });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .write) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    if ((write_count > 0 and writes_ptr == null) or (predicate_count > 0 and predicates_ptr == null)) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": write and predicate pointers must not be null when their counts are nonzero");
    batchInternal(handle, writes_ptr, write_count, predicates_ptr, predicate_count, timestamp_ns, sync_level) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_batch_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_batch_json), .{ .name = "antfly_db_batch_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .write) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    var owned = batch_api.parseBatchRequest(handle.alloc, request_json.bytes()) catch |err| return capi.mapError(err);
    defer owned.deinit(handle.alloc);

    handle.database().*.batch(owned.req) catch |err| return capi.mapError(err);
    const response = batch_api.encodeBatchResponse(std.heap.c_allocator, owned.result()) catch |err| return capi.mapError(err);
    out_buf.* = .{
        .ptr = response.ptr,
        .len = response.len,
    };
    return .ok;
}

pub fn antfly_db_resolve_intents(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: ?*const [16]u8,
    status: u8,
    commit_version: u64,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_resolve_intents), .{ .name = "antfly_db_resolve_intents" }), .{ handle_ptr, txn_id_ptr, status, commit_version });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .write) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const txn_id = txn_id_ptr orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": transaction ID pointer must not be null");
    const txn_status: transactions_mod.TxnStatus = switch (status) {
        0 => .pending,
        1 => .committed,
        2 => .aborted,
        else => return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": transaction status must be 0 (pending), 1 (committed), or 2 (aborted)"),
    };
    handle.database().*.resolveTransactionIntents(txn_id.*, txn_status, commit_version) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_get_transaction_status(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: ?*const [16]u8,
    out_status: ?*u8,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_get_transaction_status), .{ .name = "antfly_db_get_transaction_status" }), .{ handle_ptr, txn_id_ptr, out_status });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const out = out_status orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output pointer must not be null");
    out.* = 0;
    const handle = guard.handle;
    const txn_id = txn_id_ptr orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": transaction ID pointer must not be null");
    const status = handle.database().*.getTransactionStatus(txn_id.*) catch |err| return capi.mapError(err);
    out.* = @backingInt(status);
    return .ok;
}

pub fn antfly_db_get_commit_version(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: ?*const [16]u8,
    out_commit_version: ?*u64,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_get_commit_version), .{ .name = "antfly_db_get_commit_version" }), .{ handle_ptr, txn_id_ptr, out_commit_version });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const out = out_commit_version orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output pointer must not be null");
    out.* = 0;
    const handle = guard.handle;
    const txn_id = txn_id_ptr orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": transaction ID pointer must not be null");
    out.* = handle.database().*.getCommitVersion(txn_id.*) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_get_timestamp(
    handle_ptr: ?*anyopaque,
    key: capi.Slice,
    out_timestamp: *u64,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_get_timestamp), .{ .name = "antfly_db_get_timestamp" }), .{ handle_ptr, key, out_timestamp });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    out_timestamp.* = handle.database().*.getTimestamp(handle.alloc, key.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_memory_json(
    handle_ptr: ?*anyopaque,
    space: capi.Slice,
    request_json: capi.Slice,
    out_buf: ?*capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_memory_json), .{ .name = "antfly_db_memory_json" }), .{ handle_ptr, space, request_json, out_buf });
    capi.clearLastError();
    const out = out_buf orelse return capi.mapErrorContext(error.InvalidArgument, "memory: output pointer must not be null");
    out.* = .{};
    var parsed = std.json.parseFromSlice(@import("memory.zig").Request, std.heap.c_allocator, request_json.bytes(), .{ .ignore_unknown_fields = false }) catch |err| return capi.mapError(err);
    defer parsed.deinit();
    const access: HandleAccess = if (@import("../memory/service.zig").mutates(parsed.value.action)) .exclusive else .read;
    var guard = enterHandle(handle_ptr, access) orelse return capi.mapErrorContext(error.InvalidArgument, "memory: invalid database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const result = @import("memory.zig").execute(&guard, handle_ptr, std.heap.c_allocator, space.bytes(), request_json.bytes()) catch |err| return capi.mapError(err);
    out.* = .{ .ptr = result.ptr, .len = result.len };
    return .ok;
}

pub fn antfly_db_lookup_json(
    handle_ptr: ?*anyopaque,
    key: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_lookup_json), .{ .name = "antfly_db_lookup_json" }), .{ handle_ptr, key, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    handle.prepareLookupRequest(key.bytes(), .{}) catch |err| return capi.mapError(err);
    const result = handle.database().*.getDocument(std.heap.c_allocator, key.bytes(), .{}) catch |err| return capi.mapError(err);
    if (result == null) return capi.mapError(error.NotFound);
    out_buf.* = .{
        .ptr = result.?.json.ptr,
        .len = result.?.json.len,
    };
    return .ok;
}

pub fn antfly_db_get_raw(
    handle_ptr: ?*anyopaque,
    key: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_get_raw), .{ .name = "antfly_db_get_raw" }), .{ handle_ptr, key, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const result = handle.database().*.get(std.heap.c_allocator, key.bytes()) catch |err| return capi.mapError(err);
    if (result == null) return capi.mapError(error.NotFound);
    if (@import("../storage/internal_keys.zig").isResolutionArtifactKey(key.bytes())) {
        defer std.heap.c_allocator.free(result.?);
        const visible = @import("../storage/hierarchy_navigation.zig").publicArtifactPayloadAlloc(std.heap.c_allocator, result.?) catch |err| return capi.mapError(err);
        out_buf.* = .{ .ptr = visible.ptr, .len = visible.len };
        return .ok;
    }
    out_buf.* = .{
        .ptr = result.?.ptr,
        .len = result.?.len,
    };
    return .ok;
}

pub fn antfly_db_lookup_artifact_json(
    handle_ptr: ?*anyopaque,
    artifact_id_b64: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_lookup_artifact_json), .{ .name = "antfly_db_lookup_artifact_json" }), .{ handle_ptr, artifact_id_b64, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    handle.prepareLookupRequest(artifact_id_b64.bytes(), .{}) catch |err| return capi.mapError(err);
    const artifact_id = decodeBase64Alloc(handle.alloc, artifact_id_b64.bytes()) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(artifact_id);

    var record = handle.database().*.getPublicArtifact(handle.alloc, artifact_id) catch |err| return capi.mapError(err);
    if (record == null) return capi.mapError(error.NotFound);
    defer record.?.deinit(handle.alloc);

    var payload = JsonArtifactWrite.init(handle.alloc, record.?) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer payload.deinit(handle.alloc);

    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_decode_artifact_id_json(
    artifact_id_b64: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_decode_artifact_id_json), .{ .name = "antfly_decode_artifact_id_json" }), .{ artifact_id_b64, out_buf });
    capi.clearLastError();
    const alloc = std.heap.c_allocator;
    const artifact_id = decodeBase64Alloc(alloc, artifact_id_b64.bytes()) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer alloc.free(artifact_id);

    var artifact_ref = (db_mod.artifact_ids.decodeArtifactPublicIdAlloc(alloc, artifact_id) catch |err| return capi.mapErrorContext(err, @src().fn_name)) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": artifact ID has an invalid public encoding");
    defer artifact_ref.deinit(alloc);

    var payload = JsonArtifactRef.init(alloc, artifact_ref) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer payload.deinit(alloc);

    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_get_schema_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_get_schema_json), .{ .name = "antfly_db_get_schema_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    if (handle.database().*.getSchemaJson(std.heap.c_allocator) catch |err| return capi.mapError(err)) |schema_json| {
        out_buf.* = .{ .ptr = schema_json.ptr, .len = schema_json.len };
    } else {
        out_buf.* = dupBytes("null") catch |err| return capi.mapErrorContext(err, @src().fn_name);
    }
    return .ok;
}

pub fn antfly_db_set_schema_json(
    handle_ptr: ?*anyopaque,
    schema_json: capi.Slice,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_set_schema_json), .{ .name = "antfly_db_set_schema_json" }), .{ handle_ptr, schema_json });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    @import("sql_ddl.zig").invalidate(handle);
    handle.database().*.setSchemaJson(handle.alloc, schema_json.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_run_until_idle(handle_ptr: ?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_run_until_idle), .{ .name = "antfly_db_run_until_idle" }), .{handle_ptr});
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .maintain) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    @import("entity_sink.zig").runUntilIdle(handle) catch |err| return capi.mapError(err);
    return .ok;
}

/// A root Lite connection drains every namespace. Report that same scope,
/// retaining per-table snapshots so callers can identify failed producers.
fn pendingNamespaceStatsValue(a: Allocator, database: *db_mod.DB) !std.json.Value {
    const bytes = try std.json.Stringify.valueAlloc(a, database.pendingWorkStats(), .{});
    var value = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{});
    const resolver_progress = try database.resolverReviewsJsonAlloc(a, "{\"operation\":\"progress\"}");
    try value.object.put(a, "resolver_curation", try std.json.parseFromSliceLeaky(std.json.Value, a, resolver_progress, .{}));
    const debt = try database.listDerivedReplayDebt(a);
    defer {
        for (debt) |*item| item.deinit(a);
        a.free(debt);
    }
    const debt_json = try std.json.Stringify.valueAlloc(a, debt, .{});
    try value.object.put(a, "index_replay_debt", try std.json.parseFromSliceLeaky(std.json.Value, a, debt_json, .{}));
    const jobs = try database.graphReplacementStatusAlloc(a);
    defer jobs.deinit();
    var rows: std.ArrayList(std.json.Value) = .empty;
    for (jobs.value) |job| {
        const upstream = if (database.enrichment_runtime) |runtime| runtime.stats() else db_mod.types.EnrichmentStats{};
        const replaying = job.phase != .failed and !job.producer_replay_complete;
        const waiting = job.phase != .failed and job.producer_replay_target > upstream.applied_sequence;
        const producer_pending = replaying or waiting;
        const blocked_reason: ?[]const u8 = if (replaying) "replaying_producer_sources" else if (waiting) "waiting_for_producer_enrichment" else null;
        // A malformed failed declaration must not hide its retry/error status.
        const producers = database.graphReplacementProducerNamesAlloc(a, job) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => try a.alloc([]u8, 0),
        };
        const status = try std.json.Stringify.valueAlloc(a, .{
            .name = job.name,
            .replaces = job.old,
            .phase = @tagName(job.phase),
            .scanned_keys = job.scanned,
            .applied_sequence = job.applied_sequence,
            .target_sequence = database.core.nextDerivedSequence(),
            .last_error = job.last_error,
            .blocked_reason = blocked_reason,
            .producer_artifacts = producers,
            .producer_replay_target = job.producer_replay_target,
            .producer_applied_sequence = upstream.applied_sequence,
            .upstream_last_error = if (producer_pending and upstream.last_error_code.len > 0) upstream.last_error_code.slice() else @as(?[]const u8, null),
            .upstream_retrying = producer_pending and upstream.retrying,
            .upstream_next_retry_at_ms = if (producer_pending) upstream.next_retry_at_ms else 0,
            .retry_count = job.retry_count,
            .total_retry_count = job.total_retry_count,
            .max_total_retries = db_mod.DB.replacement_total_retry_limit,
            .retry_after_ns = job.retry_after_ns,
            .activation_attempts = job.activation_attempts,
        }, .{});
        try rows.append(a, try std.json.parseFromSliceLeaky(std.json.Value, a, status, .{}));
    }
    try value.object.put(a, "replacements", .{ .array = .fromOwnedSlice(a, rows.items) });
    return value;
}

pub fn pendingWorkStatsJson(handle: *Handle) !capi.Buffer {
    var arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var aggregate = handle.database().pendingWorkStats();
    var output = try pendingNamespaceStatsValue(a, handle.database());
    const context = handle.lite_connection_context orelse if (handle.parent_handle) |parent| parent.lite_connection_context else null;
    if (context) |connection| {
        const stats = try std.json.Stringify.valueAlloc(a, connection.owner.publication_stats, .{});
        try output.object.put(a, "background_publications", try std.json.parseFromSliceLeaky(std.json.Value, a, stats, .{}));
    }
    if (handle.selected_table_name != null) return stringifyJson(output);
    var snapshots: std.json.ObjectMap = .empty;
    var iterator = handle.embedded_tables.iterator();
    while (iterator.next()) |entry| {
        const child = try pendingNamespaceStatsValue(a, &entry.value_ptr.*.db);
        try snapshots.put(a, entry.key_ptr.*, child);
        const child_stats = entry.value_ptr.*.db.pendingWorkStats();
        aggregate.enrichment.enabled = aggregate.enrichment.enabled or child_stats.enrichment.enabled;
        aggregate.enrichment.error_count +|= child_stats.enrichment.error_count;
        aggregate.enrichment.retryable_error_count +|= child_stats.enrichment.retryable_error_count;
        aggregate.enrichment.fatal_error_count +|= child_stats.enrichment.fatal_error_count;
        if (child_stats.enrichment.last_error_code.len > 0) aggregate.enrichment.last_error_code = child_stats.enrichment.last_error_code;
    }
    const enrichment = try std.json.Stringify.valueAlloc(a, aggregate.enrichment, .{});
    try output.object.put(a, "enrichment", try std.json.parseFromSliceLeaky(std.json.Value, a, enrichment, .{}));
    var unavailable: std.json.ObjectMap = .empty;
    var errors = handle.embedded_table_errors.iterator();
    while (errors.next()) |entry| try unavailable.put(a, entry.key_ptr.*, .{ .string = entry.value_ptr.* });
    try output.object.put(a, "unavailable_tables", .{ .object = unavailable });
    try output.object.put(a, "tables", .{ .object = snapshots });
    return stringifyJson(output);
}

pub fn antfly_db_run_until_idle_json(
    handle_ptr: ?*anyopaque,
    out_buf_ptr: ?*capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_run_until_idle_json), .{ .name = "antfly_db_run_until_idle_json" }), .{ handle_ptr, out_buf_ptr });
    capi.clearLastError();
    const out_buf = resetOutBuffer(out_buf_ptr) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const guard = enterHandle(handle_ptr, .maintain) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    @import("entity_sink.zig").runUntilIdle(handle) catch |err| {
        writeRunUntilIdleNoProgressDiagnosticIfAny(handle.database(), out_buf, err);
        if (out_buf.len == 0) out_buf.* = pendingWorkStatsJson(handle) catch .{};
        return capi.mapError(err);
    };
    out_buf.* = pendingWorkStatsJson(handle) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

/// On `error.RunUntilIdleNoProgress` (mapped to `capi.ErrorCode.stalled`),
/// best-effort populate `out_buf` with the exact stuck index name and its
/// indexed/expected counters (see `DB.NoProgressDiagnostic`) instead of
/// leaving callers with only the non-descriptive status code. Any other
/// error leaves `out_buf` untouched, matching every other failure path here.
pub fn writeRunUntilIdleNoProgressDiagnosticIfAny(db: anytype, out_buf: *capi.Buffer, err: anyerror) void {
    if (err != error.RunUntilIdleNoProgress) return;
    const diagnostic = db.lastRunUntilIdleNoProgressDiagnostic() orelse return;
    out_buf.* = stringifyJson(.{
        .index_name = diagnostic.index_name,
        .indexed = diagnostic.indexed,
        .expected = diagnostic.expected,
        .stuck_ms = diagnostic.stuck_ns / std.time.ns_per_ms,
    }) catch return;
}

pub fn antfly_db_pending_work_stats_json(
    handle_ptr: ?*anyopaque,
    out_buf_ptr: ?*capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_pending_work_stats_json), .{ .name = "antfly_db_pending_work_stats_json" }), .{ handle_ptr, out_buf_ptr });
    capi.clearLastError();
    const out_buf = resetOutBuffer(out_buf_ptr) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output buffer pointer must not be null");
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    out_buf.* = pendingWorkStatsJson(handle) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antflyDbExtractEnrichmentsJson(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antflyDbExtractEnrichmentsJson), .{ .name = "antfly_db_extract_enrichments_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const writes = decodeBatchWritesRequest(handle.alloc, request_json.bytes()) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer freeOwnedBatchWrites(handle.alloc, writes);

    var result = handle.database().*.extractEnrichments(handle.alloc, writes) catch |err| return capi.mapError(err);
    defer result.deinit(handle.alloc);

    var payload = buildJsonExtractEnrichmentsResult(handle.alloc, result) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer payload.deinit(handle.alloc);

    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antflyDbComputeEnrichmentsJson(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antflyDbComputeEnrichmentsJson), .{ .name = "antfly_db_compute_enrichments_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    out_buf.* = .{};
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const writes = decodeBatchWritesRequest(handle.alloc, request_json.bytes()) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer freeOwnedBatchWrites(handle.alloc, writes);

    var result = handle.database().*.computeEnrichments(handle.alloc, writes) catch |err| return capi.mapError(err);
    defer result.deinit(handle.alloc);

    var payload = buildJsonComputeEnrichmentsResult(handle.alloc, result) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer payload.deinit(handle.alloc);

    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_update_range(
    handle_ptr: ?*anyopaque,
    start: capi.Slice,
    end: capi.Slice,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_update_range), .{ .name = "antfly_db_update_range" }), .{ handle_ptr, start, end });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    handle.database().*.updateRange(.{
        .start = start.bytes(),
        .end = end.bytes(),
    }) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_get_range_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_get_range_json), .{ .name = "antfly_db_get_range_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    var payload = JsonRange.init(handle.alloc, handle.database().*.getRange()) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer payload.deinit(handle.alloc);
    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_get_split_state_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_get_split_state_json), .{ .name = "antfly_db_get_split_state_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const state = handle.database().*.getSplitState(handle.alloc) catch |err| return capi.mapError(err);
    if (state == null) return capi.mapError(error.NotFound);
    var payload = JsonSplitState.init(handle.alloc, state.?) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer payload.deinit(handle.alloc);
    defer db_mod.types.freeSplitState(handle.alloc, state);
    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_set_split_state_json(
    handle_ptr: ?*anyopaque,
    state_json: capi.Slice,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_set_split_state_json), .{ .name = "antfly_db_set_split_state_json" }), .{ handle_ptr, state_json });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const ParsedState = struct {
        phase: u8,
        split_key_b64: []const u8,
        new_shard_id: u64,
        started_at: u64,
        original_range_end_b64: []const u8,
    };
    var parsed = std.json.parseFromSlice(ParsedState, handle.alloc, state_json.bytes(), .{}) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer parsed.deinit();
    const split_key = decodeBase64Alloc(handle.alloc, parsed.value.split_key_b64) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(split_key);
    const original_range_end = decodeBase64Alloc(handle.alloc, parsed.value.original_range_end_b64) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(original_range_end);
    const phase: db_mod.types.SplitPhase = switch (parsed.value.phase) {
        0 => .none,
        1 => .prepare,
        2 => .splitting,
        3 => .finalizing,
        4 => .rolling_back,
        else => return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": split phase must be in the range 0 through 4"),
    };
    handle.database().*.setSplitState(.{
        .phase = phase,
        .split_key = split_key,
        .new_shard_id = parsed.value.new_shard_id,
        .started_at = parsed.value.started_at,
        .original_range_end = original_range_end,
    }) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_clear_split_state(handle_ptr: ?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_clear_split_state), .{ .name = "antfly_db_clear_split_state" }), .{handle_ptr});
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    handle.database().*.clearSplitState() catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_get_split_delta_seq(
    handle_ptr: ?*anyopaque,
    out_seq: *u64,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_get_split_delta_seq), .{ .name = "antfly_db_get_split_delta_seq" }), .{ handle_ptr, out_seq });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    out_seq.* = handle.database().*.getSplitDeltaSeq();
    return .ok;
}

pub fn antfly_db_get_split_delta_final_seq(
    handle_ptr: ?*anyopaque,
    out_seq: *u64,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_get_split_delta_final_seq), .{ .name = "antfly_db_get_split_delta_final_seq" }), .{ handle_ptr, out_seq });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    out_seq.* = handle.database().*.getSplitDeltaFinalSeq(handle.alloc) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_set_split_delta_final_seq(
    handle_ptr: ?*anyopaque,
    seq: u64,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_set_split_delta_final_seq), .{ .name = "antfly_db_set_split_delta_final_seq" }), .{ handle_ptr, seq });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    handle.database().*.setSplitDeltaFinalSeq(seq) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_clear_split_delta_final_seq(handle_ptr: ?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_clear_split_delta_final_seq), .{ .name = "antfly_db_clear_split_delta_final_seq" }), .{handle_ptr});
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    handle.database().*.clearSplitDeltaFinalSeq() catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_list_split_delta_entries_after_json(
    handle_ptr: ?*anyopaque,
    after_seq: u64,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_list_split_delta_entries_after_json), .{ .name = "antfly_db_list_split_delta_entries_after_json" }), .{ handle_ptr, after_seq, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const entries = handle.database().*.listSplitDeltaEntriesAfter(handle.alloc, after_seq) catch |err| return capi.mapError(err);
    defer db_mod.types.freeSplitDeltaEntries(handle.alloc, entries);

    var payload = handle.alloc.alloc(JsonSplitDeltaEntry, entries.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var payload_count: usize = 0;
    defer {
        for (payload[0..payload_count]) |*entry| entry.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (entries, 0..) |entry, i| {
        payload[i] = JsonSplitDeltaEntry.init(handle.alloc, entry) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        payload_count += 1;
    }

    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_clear_split_delta_entries(handle_ptr: ?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_clear_split_delta_entries), .{ .name = "antfly_db_clear_split_delta_entries" }), .{handle_ptr});
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    handle.database().*.clearSplitDeltaEntries() catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_list_indexes_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_list_indexes_json), .{ .name = "antfly_db_list_indexes_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const configs = handle.database().*.listIndexes(handle.alloc) catch |err| return capi.mapError(err);
    defer db_mod.types.freeIndexConfigs(handle.alloc, configs);

    var payload = handle.alloc.alloc(JsonIndexConfig, configs.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(payload);
    for (configs, 0..) |cfg, i| {
        payload[i] = .{
            .name = cfg.name,
            .kind = @tagName(cfg.kind),
            .config_json = cfg.config_json,
        };
    }

    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_list_enrichments_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_list_enrichments_json), .{ .name = "antfly_db_list_enrichments_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const configs = handle.database().*.listEnrichments(handle.alloc) catch |err| return capi.mapError(err);
    defer db_mod.types.freeEnrichmentConfigs(handle.alloc, configs);
    out_buf.* = stringifyJson(configs) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_scan_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_scan_json), .{ .name = "antfly_db_scan_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        from_key_b64: []const u8 = "",
        to_key_b64: []const u8 = "",
        inclusive_from: bool = false,
        exclusive_to: bool = false,
        include_documents: bool = false,
        limit: u32 = 0,
        fields: []const []const u8 = &.{},
        include_all_fields: bool = true,
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{ .ignore_unknown_fields = true }) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer parsed.deinit();

    const from_key = decodeBase64Alloc(handle.alloc, parsed.value.from_key_b64) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(from_key);
    const to_key = decodeBase64Alloc(handle.alloc, parsed.value.to_key_b64) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(to_key);
    const opts: db_mod.types.ScanOptions = .{
        .inclusive_from = parsed.value.inclusive_from,
        .exclusive_to = parsed.value.exclusive_to,
        .include_documents = parsed.value.include_documents,
        .limit = parsed.value.limit,
        .fields = parsed.value.fields,
        .include_all_fields = parsed.value.include_all_fields,
    };
    handle.prepareScanRequest(from_key, to_key, opts) catch |err| return capi.mapError(err);
    var result = handle.database().*.scan(handle.alloc, from_key, to_key, opts) catch |err| return capi.mapError(err);
    defer result.deinit(handle.alloc);

    var hashes = handle.alloc.alloc(JsonScanHash, result.hashes.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var hash_count: usize = 0;
    defer {
        for (hashes[0..hash_count]) |*item| item.deinit(handle.alloc);
        if (hashes.len > 0) handle.alloc.free(hashes);
    }
    for (result.hashes, 0..) |item, i| {
        hashes[i] = JsonScanHash.init(handle.alloc, item) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        hash_count += 1;
    }

    var documents = handle.alloc.alloc(JsonScanDocument, result.documents.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var document_count: usize = 0;
    defer {
        for (documents[0..document_count]) |*item| item.deinit(handle.alloc);
        if (documents.len > 0) handle.alloc.free(documents);
    }
    for (result.documents, 0..) |item, i| {
        documents[i] = JsonScanDocument.init(handle.alloc, item) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        document_count += 1;
    }

    out_buf.* = stringifyJson(JsonScanResult{
        .hashes = hashes,
        .documents = documents,
    }) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_scan_hashes(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_result: *capi.ScanHashResult,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_scan_hashes), .{ .name = "antfly_db_scan_hashes" }), .{ handle_ptr, request_json, out_result });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        from_key_b64: []const u8 = "",
        to_key_b64: []const u8 = "",
        inclusive_from: bool = false,
        exclusive_to: bool = false,
        limit: u32 = 0,
        fields: []const []const u8 = &.{},
        include_all_fields: bool = true,
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{ .ignore_unknown_fields = true }) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer parsed.deinit();

    const from_key = decodeBase64Alloc(handle.alloc, parsed.value.from_key_b64) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(from_key);
    const to_key = decodeBase64Alloc(handle.alloc, parsed.value.to_key_b64) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(to_key);
    const opts: db_mod.types.ScanOptions = .{
        .inclusive_from = parsed.value.inclusive_from,
        .exclusive_to = parsed.value.exclusive_to,
        .include_documents = false,
        .limit = parsed.value.limit,
        .fields = parsed.value.fields,
        .include_all_fields = parsed.value.include_all_fields,
    };
    handle.prepareScanRequest(from_key, to_key, opts) catch |err| return capi.mapError(err);
    var result = handle.database().*.scan(handle.alloc, from_key, to_key, opts) catch |err| return capi.mapError(err);
    defer result.deinit(handle.alloc);

    const entries = std.heap.c_allocator.alloc(capi.ScanHashEntry, result.hashes.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    errdefer std.heap.c_allocator.free(entries);
    for (result.hashes, 0..) |item, i| {
        const id = std.heap.c_allocator.alloc(u8, item.id.len) catch {
            for (entries[0..i]) |entry| {
                if (entry.id_ptr != null and entry.id_len > 0) {
                    std.heap.c_allocator.free(entry.id_ptr.?[0..entry.id_len]);
                }
            }
            return capi.recordError(.internal, @src().fn_name ++ ": allocating scan hash entry IDs failed: OutOfMemory");
        };
        @memcpy(id, item.id);
        entries[i] = .{
            .id_ptr = id.ptr,
            .id_len = id.len,
            .hash = item.hash,
        };
    }

    out_result.* = .{
        .entries_ptr = entries.ptr,
        .entry_count = entries.len,
    };
    return .ok;
}

pub fn antfly_db_stats_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_stats_json), .{ .name = "antfly_db_stats_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const bytes = dbStatsJsonAlloc(handle) catch |err| return capi.mapError(err);
    out_buf.* = .{ .ptr = bytes.ptr, .len = bytes.len };
    return .ok;
}

pub fn dbStatsJsonAlloc(handle: *Handle) ![]u8 {
    var stats = try handle.database().*.stats(handle.alloc);
    defer db_mod.types.freeDBStats(handle.alloc, stats);

    const context = handle.lite_connection_context orelse if (handle.parent_handle) |parent| parent.lite_connection_context else null;
    if (context) |connection| connection.owner.overlayTtlStats(handle.database(), &stats.ttl_cleanup);

    const indexes = try dbIndexStatsProjectionAlloc(handle.alloc, stats);
    defer if (indexes.len > 0) handle.alloc.free(indexes);

    return try std.fmt.allocPrint(handle.alloc, "{f}", .{std.json.fmt(jsonDBStatsProjection(stats, indexes), .{})});
}

pub fn dbIndexStatsProjectionAlloc(alloc: Allocator, stats: db_mod.types.DBStats) ![]JsonDBIndexStats {
    var indexes = try alloc.alloc(JsonDBIndexStats, stats.indexes.len);
    for (stats.indexes, 0..) |item, i| {
        indexes[i] = .{
            .name = item.name,
            .kind = @tagName(item.kind),
            .replay_applied_sequence = item.replay_applied_sequence,
            .replay_target_sequence = item.replay_target_sequence,
            .replay_catch_up_required = item.replay_catch_up_required,
            .catch_up_active = item.catch_up_active,
            .catch_up_phase = @tagName(item.catch_up_phase),
            .doc_count = item.doc_count,
            .term_count = item.term_count,
            .edge_count = item.edge_count,
            .graph_counts_pending = item.graph_counts_pending,
            .node_count = item.node_count,
            .repair_degraded = item.repair_degraded,
            .repair_issue_count = item.repair_issue_count,
            .repair_summary_ready = item.repair_summary_ready,
            .repair_issue_count_estimated = item.repair_issue_count_estimated,
            .coverage_produced_count = item.coverage_produced_count,
            .coverage_skipped_count = item.coverage_skipped_count,
            .coverage_terminal_failed_count = item.coverage_terminal_failed_count,
            .coverage_summary_ready = item.coverage_summary_ready,
        };
    }
    return indexes;
}

pub fn jsonDBStatsProjection(stats: db_mod.types.DBStats, indexes: []JsonDBIndexStats) JsonDBStats {
    return JsonDBStats{
        .doc_count = stats.doc_count,
        .index_count = stats.index_count,
        .indexes_available = stats.indexes_available,
        .indexes = indexes,
        .repair_degraded = stats.repair_degraded,
        .repair_issue_count = stats.repair_issue_count,
        .repair_summary_ready = stats.repair_summary_ready,
        .repair_issue_count_estimated = stats.repair_issue_count_estimated,
        .enrichment = .{
            .last_error_code = stats.enrichment.last_error_code,
            .enabled = stats.enrichment.enabled,
            .lease_owned = stats.enrichment.lease_owned,
            .has_lease = stats.enrichment.has_lease,
            .acquisition_count = stats.enrichment.acquisition_count,
            .lease_acquire_failures = stats.enrichment.lease_acquire_failures,
            .lost_leases = stats.enrichment.lost_leases,
            .last_acquired_ms = stats.enrichment.last_acquired_ms,
            .target_sequence = stats.enrichment.target_sequence,
            .applied_sequence = stats.enrichment.applied_sequence,
            .processed_requests = stats.enrichment.processed_requests,
            .error_count = stats.enrichment.error_count,
            .retryable_error_count = stats.enrichment.retryable_error_count,
            .fatal_error_count = stats.enrichment.fatal_error_count,
            .retrying = stats.enrichment.retrying,
            .worker_failed = stats.enrichment.worker_failed,
            .stalled = stats.enrichment.stalled,
            .stall_reason = stats.enrichment.stall_reason,
            .skip_by_hash_count = stats.enrichment.skip_by_hash_count,
            .skipped_source_count = stats.enrichment.skipped_source_count,
            .codec_decode_failures = stats.enrichment.codec_decode_failures,
            .dense_artifact_bytes_written = stats.enrichment.dense_artifact_bytes_written,
            .sparse_artifact_bytes_written = stats.enrichment.sparse_artifact_bytes_written,
            .chunk_artifact_bytes_written = stats.enrichment.chunk_artifact_bytes_written,
            .artifact_bytes_written = stats.enrichment.artifact_bytes_written,
        },
        .ttl_cleanup = .{
            .enabled = stats.ttl_cleanup.enabled,
            .lease_owned = stats.ttl_cleanup.lease_owned,
            .has_lease = stats.ttl_cleanup.has_lease,
            .acquisition_count = stats.ttl_cleanup.acquisition_count,
            .runs = stats.ttl_cleanup.runs,
            .scanned_timestamps = stats.ttl_cleanup.scanned_timestamps,
            .deleted_docs = stats.ttl_cleanup.deleted_docs,
            .last_run_ns = stats.ttl_cleanup.last_run_ns,
            .error_count = stats.ttl_cleanup.error_count,
            .lease_acquire_failures = stats.ttl_cleanup.lease_acquire_failures,
            .lost_leases = stats.ttl_cleanup.lost_leases,
            .last_acquired_ms = stats.ttl_cleanup.last_acquired_ms,
        },
        .transaction_recovery = .{
            .enabled = stats.transaction_recovery.enabled,
            .lease_owned = stats.transaction_recovery.lease_owned,
            .has_lease = stats.transaction_recovery.has_lease,
            .acquisition_count = stats.transaction_recovery.acquisition_count,
            .lease_acquire_failures = stats.transaction_recovery.lease_acquire_failures,
            .lost_leases = stats.transaction_recovery.lost_leases,
            .last_acquired_ms = stats.transaction_recovery.last_acquired_ms,
            .runs = stats.transaction_recovery.runs,
            .scanned_records = stats.transaction_recovery.scanned_records,
            .auto_aborted = stats.transaction_recovery.auto_aborted,
            .resolved_finalized = stats.transaction_recovery.resolved_finalized,
            .cleaned_records = stats.transaction_recovery.cleaned_records,
            .kept_recent_pending = stats.transaction_recovery.kept_recent_pending,
            .deferred_unresolved = stats.transaction_recovery.deferred_unresolved,
            .notification_attempts = stats.transaction_recovery.notification_attempts,
            .notification_successes = stats.transaction_recovery.notification_successes,
            .notification_failures = stats.transaction_recovery.notification_failures,
            .last_run_ns = stats.transaction_recovery.last_run_ns,
            .error_count = stats.transaction_recovery.error_count,
        },
        .text_merge = .{
            .enabled = stats.text_merge.enabled,
            .active_indexes = stats.text_merge.active_indexes,
            .active_segments = stats.text_merge.active_segments,
            .max_active_segments_per_index = stats.text_merge.max_active_segments_per_index,
            .pending_indexes = stats.text_merge.pending_indexes,
            .pending_segments = stats.text_merge.pending_segments,
            .pending_bytes = stats.text_merge.pending_bytes,
            .in_flight_merges = stats.text_merge.in_flight_merges,
            .in_flight_segments = stats.text_merge.in_flight_segments,
            .completed_merges = stats.text_merge.completed_merges,
            .skipped_stale_merges = stats.text_merge.skipped_stale_merges,
            .failed_merges = stats.text_merge.failed_merges,
            .quarantined_merges = stats.text_merge.quarantined_merges,
            .quarantined_segments = stats.text_merge.quarantined_segments,
            .last_merge_error = stats.text_merge.last_merge_error,
            .backpressure_events = stats.text_merge.backpressure_events,
            .backpressure_ns = stats.text_merge.backpressure_ns,
            .max_pending_segments = stats.text_merge.max_pending_segments,
            .max_pending_bytes = stats.text_merge.max_pending_bytes,
        },
        .term_doc_freq_cache_hits = stats.term_doc_freq_cache_hits,
        .term_doc_freq_cache_misses = stats.term_doc_freq_cache_misses,
    };
}

pub fn requestLooksLikePublicQueryJson(bytes: []const u8) bool {
    return std.mem.indexOf(u8, bytes, "\"full_text_search\"") != null or
        std.mem.indexOf(u8, bytes, "\"embeddings\"") != null or
        std.mem.indexOf(u8, bytes, "\"graph_queries\"") != null or
        std.mem.indexOf(u8, bytes, "\"graph_metric\"") != null or
        std.mem.indexOf(u8, bytes, "\"graph_metric_rerank\"") != null or
        std.mem.indexOf(u8, bytes, "\"graph_metric_seed\"") != null or
        std.mem.indexOf(u8, bytes, "\"merge_config\"") != null or
        std.mem.indexOf(u8, bytes, "\"indexes\"") != null or
        std.mem.indexOf(u8, bytes, "\"query\"") != null;
}

pub const LiteSemanticResolver = struct {
    handle: *Handle,

    pub fn resolveDenseQuery(
        ptr: *anyopaque,
        alloc: Allocator,
        table_name: []const u8,
        index_name: []const u8,
        semantic_search: []const u8,
        embedding_template: ?[]const u8,
        limit: u32,
    ) anyerror!db_mod.types.DenseKnnQuery {
        _ = table_name;
        if (embedding_template != null) return error.UnsupportedQueryRequest;
        const self: *LiteSemanticResolver = @ptrCast(@alignCast(ptr));
        const handle = self.handle;
        const merged_json = try liteMergedIndexesJsonAlloc(handle);
        defer handle.alloc.free(merged_json);
        var managed = try managed_embedder.ManagedEmbedder.initFromIndexesJsonWithOptions(
            handle.alloc,
            merged_json,
            .{ .antfly_provider = handle.liteAntflyProvider() },
        );
        defer managed.deinit();
        const vector = try managed.embedQuery(alloc, index_name, semantic_search);
        return .{ .vector = vector, .k = limit };
    }

    pub fn resolver(self: *LiteSemanticResolver) query_api.SemanticResolver {
        return .{
            .ptr = self,
            .vtable = &.{ .resolve_dense_query = resolveDenseQuery },
        };
    }
};

pub fn searchPublicQueryJson(
    handle: *Handle,
    table_name: []const u8,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    capi.clearLastError();
    query_api.validateStoragePublicQueryRequest(handle.alloc, request_json.bytes()) catch |err| return capi.mapError(err);
    // `linked_storage` says this compiled library links the full storage
    // internals -- true unconditionally for the default `libantfly` since
    // it is shared with the `antfly` executable -- not that `handle` is a
    // genuine storage-owner/distributed-table handle. A Lite handle
    // (`owned_lite_backend != null`) has no metadata/table catalog for
    // `local_query_client`'s internal semantic-search resolution to look an
    // index's embedder up in, so it always takes the simpler path below,
    // which resolves `semantic_search` itself via `LiteSemanticResolver`.
    if (comptime capi_build_options.linked_storage) {
        if (handle.owned_lite_backend == null) {
            handle.prepareSearchRequest(.{}) catch |err| return capi.mapError(err);
            var failure: kernel_owner_abi.FailureIdentity = .{};
            const response = local_query_client.executeJsonAlloc(
                std.heap.c_allocator,
                @ptrCast(handle.database()),
                table_name,
                request_json.bytes(),
                .public,
                .{},
                null,
                null,
                null,
                &failure,
            ) catch |err| return capi.mapError(err);
            out_buf.* = .{ .ptr = response.json.ptr, .len = response.json.len };
            return .ok;
        }
    }

    var lite_semantic_resolver = LiteSemanticResolver{ .handle = handle };
    const semantic_resolver: ?query_api.SemanticResolver = if (handle.lite_profile == .native)
        lite_semantic_resolver.resolver()
    else
        null;
    var owned = query_api.parsePublicQueryRequest(
        handle.alloc,
        semantic_resolver,
        table_name,
        request_json.bytes(),
    ) catch |err| return capi.mapError(err);
    defer owned.deinit(handle.alloc);

    const DbSearchQuery = struct {
        const Result = db_mod.types.SearchResult;
        pub fn run(_: @This(), h: *Handle, req: db_mod.types.SearchRequest) !Result {
            return h.database().*.search(h.alloc, req);
        }
    };
    var result = runAtStampedGeneration(handle, &owned.req, DbSearchQuery{}) catch |err| return capi.mapError(err);
    defer result.deinit();

    var response = query_api.encodeQueryResponses(
        handle.alloc,
        table_name,
        owned.req,
        .{},
        result,
    ) catch |err| return capi.mapError(err);
    defer response.deinit(handle.alloc);

    out_buf.* = dupBytes(response.json) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

/// SQL over the embedded database catalog. Output is always freed by
/// antfly_buffer_free, including structured SQL diagnostics on failure.
pub fn antfly_db_sql_json(handle_ptr: ?*anyopaque, request_json: capi.Slice, out_buf: *capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_sql_json), .{ .name = "antfly_db_sql_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    out_buf.* = .{};
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    if (handle.parent_id != null) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": SQL sessions require a root database handle");
    // Managed owners require Raft routing and credentials supplied by API SQL.
    if (handle.storage_owner_context != null or handle.storage_owner_path != null or handle.storage_owner_group_id != 0 or handle.readable_lease_hook != null) return capi.recordError(.unsupported, @src().fn_name ++ ": storage owner binding is already configured");
    executeEmbeddedSql(handle, "default", request_json.bytes(), out_buf) catch |err| {
        if (err == error.RowPolicyAuthenticationRequired) return capi.recordError(.unsupported, @src().fn_name ++ ": row policy requires an authenticated identity");
        const diagnostic = antfly.capi_dependencies.sql_errors.describe(err);
        if (std.mem.eql(u8, diagnostic.code, "XX000")) std.log.warn("Embedded SQL execution internal failure err={s}", .{@errorName(err)});
        if (out_buf.ptr == null) out_buf.* = stringifyJson(.{ .@"error" = diagnostic }) catch |encoding_error| return capi.mapErrorContext(encoding_error, @src().fn_name);
        // Preserve request-owned parameter/column context before its arena is freed.
        return capi.recordError(@import("sql_cursor.zig").diagnosticCode(diagnostic), if (err == error.SqlUntranslatableCharacter and out_buf.ptr != null) out_buf.ptr.?[0..out_buf.len] else diagnostic.message);
    };
    return .ok;
}

pub fn executeEmbeddedSql(handle: *Handle, table_name: []const u8, request_json: []const u8, out_buf: *capi.Buffer) !void {
    const session = try @import("sql_session.zig").requestSession(handle, request_json);
    errdefer if (session) |value| {
        if (value.active) value.failed = true;
    };
    if (request_json.len > @import("sql.zig").runtime.resource_limits.request_bytes) return error.SqlRequestTooLarge;
    // Lite has no authenticated principal capability. Hold a raw lease for
    // the entire statement, including DDL paths that do not call row APIs,
    // so policy activation cannot race an already-admitted SQL statement.
    var row_policy_lease = try handle.database().*.local_execution.row_policy_gate.enterRaw();
    defer row_policy_lease.release();
    const sql = @import("sql.zig");
    const Budget = antfly.capi_dependencies.sql_memory_budget;
    var preparation_budget = Budget{ .backing = handle.alloc, .limit = sql.runtime.resource_limits.preparation_bytes };
    const temporary = preparation_budget.allocator();
    const Request = struct {
        statement: []const u8,
        parameters: []const std.json.Value = &.{},
        limit: usize = 128,
        session_id: ?u64 = null,
        database: ?[]const u8 = null,
        namespace: ?[]const u8 = null,
    };
    var parsed = std.json.parseFromSlice(Request, temporary, request_json, .{ .allocate = .alloc_always }) catch |err| {
        if (err == error.OutOfMemory and preparation_budget.exhausted) return error.SqlWorkingMemoryLimitExceeded;
        return error.InvalidSqlParameters;
    };
    defer parsed.deinit();
    if (parsed.value.database != null or parsed.value.namespace != null) return error.UnsupportedSqlExecution;
    var compiled = sql.compiler.compile(temporary, parsed.value.statement, .{}) catch |err| {
        if (err == error.OutOfMemory and preparation_budget.exhausted) return error.SqlWorkingMemoryLimitExceeded;
        return err;
    };
    defer compiled.deinit();
    // Allocate the fallback receipt before entering any commit path.
    var commit_receipt: ?[]u8 = switch (compiled.statement) {
        .insert, .update, .delete, .commit, .create_table, .drop_table, .catalog_ddl => try std.heap.c_allocator.alloc(u8, 512),
        else => null,
    };
    defer if (commit_receipt) |buffer| std.heap.c_allocator.free(buffer);
    if (compiled.statement == .begin) {
        const upper = try std.ascii.allocUpperString(temporary, parsed.value.statement);
        defer temporary.free(upper);
        if (std.mem.indexOf(u8, upper, "ISOLATION") == null) compiled.statement.begin.isolation = .read_committed;
    }
    if (session) |value| {
        var transaction_id: ?db_mod.types.TxnId = null;
        const controlled = value.control(compiled.statement, &transaction_id) catch |err| {
            if (transaction_id) |id| out_buf.* = embeddedSqlUnknownReceipt(&commit_receipt, id);
            return err;
        };
        if (controlled) |output| {
            var result = output;
            defer result.deinit();
            out_buf.* = stringifyJson(result.output) catch |err| {
                if (result.output.mutation_outcome != null) {
                    out_buf.* = embeddedSqlCommitReceipt(&commit_receipt, result.output);
                    return;
                }
                return err;
            };
            return;
        }
        if (value.failed) return error.SqlTransactionAborted;
        if (value.active and (compiled.statement == .create_table or compiled.statement == .drop_table or compiled.statement == .catalog_ddl or compiled.statement == .policy_ddl)) return error.UnsupportedSqlExecution;
    }
    var adapter = sql.Adapter(antfly){ .transaction = session, .handle = handle, .db = handle.database(), .table_name = table_name, .read_only = !liteOpenModeCanWrite(handle.open_mode) or (if (session) |value| value.read_only else false) };
    var error_context: antfly.capi_dependencies.sql_errors.Context = .{};
    var result = sql.runtime.execute(handle.alloc, adapter.backend(), &compiled, parsed.value.parameters, .{ .result_rows = parsed.value.limit, .error_context = &error_context }) catch |err| {
        if (err == error.SqlNotNullViolation or err == error.SqlUntranslatableCharacter) out_buf.* = try stringifyJson(.{ .@"error" = error_context.diagnostic(err) });
        if (err == error.SqlMutationOutcomeUnknown) if (adapter.outcome_transaction_id) |txn_id| {
            out_buf.* = embeddedSqlUnknownReceipt(&commit_receipt, txn_id);
        };
        return err;
    };
    defer result.deinit();
    if (session) |value| if (value.active) {
        result.output.mutation_outcome = null;
    };
    var encoding_budget = Budget{ .backing = handle.alloc, .limit = 16 * 1024 * 1024 };
    const bytes = std.json.Stringify.valueAlloc(encoding_budget.allocator(), result.output, .{}) catch |err| {
        if (result.output.mutation_outcome != null) {
            // Encoding failure after commit must never invite a mutation retry.
            // Preserve the authoritative commit outcome in a bounded envelope.
            out_buf.* = embeddedSqlCommitReceipt(&commit_receipt, result.output);
            return;
        }
        if (err == error.OutOfMemory and encoding_budget.exhausted) return error.SqlWorkingMemoryLimitExceeded;
        return err;
    };
    defer encoding_budget.allocator().free(bytes);
    out_buf.* = dupBytes(bytes) catch |err| {
        if (result.output.mutation_outcome != null) {
            out_buf.* = embeddedSqlCommitReceipt(&commit_receipt, result.output);
            return;
        }
        return err;
    };
}

pub fn embeddedSqlUnknownReceipt(reserved: *?[]u8, txn_id: db_mod.types.TxnId) capi.Buffer {
    const buffer = reserved.*.?;
    @memset(buffer, ' ');
    const txn_hex = std.fmt.bytesToHex(txn_id, .lower);
    _ = std.fmt.bufPrint(buffer, "{f}", .{std.json.fmt(.{
        .transaction_id = @as([]const u8, &txn_hex),
        .@"error" = .{ .code = "40003", .message = "The mutation outcome is unknown. Reconcile the native transaction receipt; do not replay the statement.", .retryable = false },
    }, .{})}) catch unreachable;
    reserved.* = null;
    return .{ .ptr = buffer.ptr, .len = buffer.len };
}

pub fn embeddedSqlCommitReceipt(reserved: *?[]u8, output: antfly.capi_dependencies.sql_runtime.Output) capi.Buffer {
    const buffer = reserved.*.?;
    @memset(buffer, ' ');
    // Only runtime-owned INSERT/UPDATE/DELETE tags and bounded enum/integer
    // fields enter this envelope; 512 bytes exceeds their maximum encoding.
    _ = std.fmt.bufPrint(buffer, "{f}", .{std.json.fmt(.{
        .mutation_outcome = output.mutation_outcome.?,
        .rows_affected = output.rows_affected,
        .command_tag = output.command_tag,
        .@"error" = .{ .code = "53200", .message = "The mutation committed but its full response could not be allocated.", .retryable = false },
    }, .{})}) catch unreachable;
    reserved.* = null;
    return .{ .ptr = buffer.ptr, .len = buffer.len };
}

pub fn antfly_db_search_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_search_json), .{ .name = "antfly_db_search_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    if (requestLooksLikePublicQueryJson(request_json.bytes())) {
        return searchPublicQueryJson(handle, handle.selected_table_name orelse "docs", request_json, out_buf);
    }
    const Request = struct {
        mode: []const u8,
        index_name: []const u8 = "",
        text_query_type: []const u8 = "",
        text_query_json: []const u8 = "",
        field: []const u8 = "",
        text: []const u8 = "",
        vector: []const f32 = &.{},
        indices: []const u32 = &.{},
        values: []const f32 = &.{},
        k: u32 = 10,
        return_mode: []const u8 = "parent",
        max_chunks_per_parent: u32 = 0,
        limit: u32 = 10,
        offset: u32 = 0,
        include_stored: bool = true,
        filter_prefix: []const u8 = "",
        distance_over: ?f32 = null,
        distance_under: ?f32 = null,
        filter_ids: []const u64 = &.{},
        exclude_ids: []const u64 = &.{},
        identity_read_generation: ?u64 = null,
        aggregations: []const JsonSearchAggregationRequest = &.{},
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch |err| {
        return capi.mapError(err);
    };
    defer parsed.deinit();

    var query_arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer query_arena.deinit();
    const query_alloc = query_arena.allocator();

    const return_mode: db_mod.types.ReturnMode = if (std.mem.eql(u8, parsed.value.return_mode, "member"))
        .member
    else if (std.mem.eql(u8, parsed.value.return_mode, "chunk"))
        .chunk
    else if (std.mem.eql(u8, parsed.value.return_mode, "parent_with_chunks"))
        .parent_with_chunks
    else
        .parent;

    var req: db_mod.types.SearchRequest = .{
        .index_name = if (parsed.value.index_name.len > 0) parsed.value.index_name else null,
        .return_mode = return_mode,
        .max_chunks_per_parent = parsed.value.max_chunks_per_parent,
        .limit = parsed.value.limit,
        .offset = parsed.value.offset,
        .include_stored = parsed.value.include_stored,
        .filter_prefix = parsed.value.filter_prefix,
        .distance_over = parsed.value.distance_over,
        .distance_under = parsed.value.distance_under,
        .filter_ids = parsed.value.filter_ids,
        .exclude_ids = parsed.value.exclude_ids,
        .identity_read_generation = parsed.value.identity_read_generation,
    };

    if (std.mem.eql(u8, parsed.value.mode, "full_text")) {
        if (parsed.value.text_query_json.len > 0) {
            var parsed_query = std.json.parseFromSlice(std.json.Value, query_alloc, parsed.value.text_query_json, .{}) catch |err| return capi.mapErrorContext(err, @src().fn_name);
            defer parsed_query.deinit();
            req.full_text = parseTextQueryJson(query_alloc, parsed_query.value) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        } else {
            req.full_text = if (std.mem.eql(u8, parsed.value.text_query_type, "term"))
                .{ .term = .{ .field = parsed.value.field, .term = parsed.value.text } }
            else if (std.mem.eql(u8, parsed.value.text_query_type, "match"))
                .{ .match = .{ .field = parsed.value.field, .text = parsed.value.text } }
            else
                .{ .match_all = {} };
        }
    } else if (std.mem.eql(u8, parsed.value.mode, "dense")) {
        req.dense = .{
            .vector = parsed.value.vector,
            .k = parsed.value.k,
        };
    } else if (std.mem.eql(u8, parsed.value.mode, "sparse")) {
        req.sparse = .{
            .indices = parsed.value.indices,
            .values = parsed.value.values,
            .k = parsed.value.k,
        };
    } else {
        return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": search mode must be full_text, dense, or sparse");
    }

    var result = runAtStampedGeneration(handle, &req, LocalSearchQuery{}) catch |err| return capi.mapError(err);
    defer result.deinit();

    var aggregation_results: []JsonSearchAggregationResult = &.{};
    if (parsed.value.aggregations.len > 0) {
        var agg_source_is_full = result.hits.len == result.total_hits;
        var full_result: ?db_mod.types.SearchResult = null;
        defer {
            if (full_result) |*value| value.deinit();
        }
        if (!agg_source_is_full) {
            if (result.total_hits > aggregations_mod.max_aggregation_source_hits)
                return capi.mapError(error.QueryCandidateBudgetExceeded);
            var agg_req = req;
            agg_req.offset = 0;
            agg_req.limit = if (result.total_hits == 0) 1 else result.total_hits;
            agg_req.include_stored = true;
            full_result = executeLocalSearch(handle, agg_req) catch |err| return capi.mapError(err);
            agg_source_is_full = true;
        }
        const source = if (full_result) |*value| value else &result;
        const requests = toAggregationRequest(handle.alloc, parsed.value.aggregations) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        defer freeAggregationRequests(handle.alloc, requests);
        const backend_results = aggregations_mod.computeSearchAggregations(handle.alloc, requests, source.*, .{
            .index_manager = handle.database().*.core.index_manager,
            .full_text_index_name = req.index_name,
            .identity_read_generation = req.identity_read_generation.?,
        }) catch |err| return capi.mapError(err);
        defer aggregations_mod.deinitResults(handle.alloc, backend_results);
        aggregation_results = toJsonAggregationResults(handle.alloc, backend_results) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    }
    defer {
        for (aggregation_results) |*item| item.deinit(handle.alloc);
        if (aggregation_results.len > 0) handle.alloc.free(aggregation_results);
    }

    var hits = handle.alloc.alloc(JsonSearchHit, result.hits.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var count: usize = 0;
    defer {
        for (hits[0..count]) |*item| item.deinit(handle.alloc);
        if (hits.len > 0) handle.alloc.free(hits);
    }
    for (result.hits, 0..) |hit, i| {
        hits[i] = JsonSearchHit.init(handle.alloc, hit) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        count += 1;
    }
    var graph_results = handle.alloc.alloc(JsonGraphSearchResult, result.graph_results.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var graph_count: usize = 0;
    defer {
        for (graph_results[0..graph_count]) |*item| item.deinit(handle.alloc);
        if (graph_results.len > 0) handle.alloc.free(graph_results);
    }
    for (result.graph_results, 0..) |graph_result, i| {
        graph_results[i] = JsonGraphSearchResult.init(handle.alloc, graph_result, req.identity_read_generation) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        graph_count += 1;
    }
    out_buf.* = stringifyJson(JsonSearchResult{
        .total_hits = result.total_hits,
        .identity_read_generation = req.identity_read_generation,
        .hits = hits,
        .graph_results = graph_results,
        .aggregations = aggregation_results,
    }) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_search_dense(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    vector_ptr: ?[*]const f32,
    vector_len: usize,
    k: u32,
    limit: u32,
    offset: u32,
    out_result: *capi.PackedDenseSearchResult,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_search_dense), .{ .name = "antfly_db_search_dense" }), .{ handle_ptr, index_name, vector_ptr, vector_len, k, limit, offset, out_result });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    if (vector_ptr == null or vector_len == 0) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": query vector must be nonempty and have a non-null pointer");
    handle.prepareDenseSearchRequest(index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset) catch |err| return capi.mapError(err);
    const identity_read_generation = currentIdentityReadGenerationForHandle(handle, null) catch |err| return capi.mapError(err);

    const fast = searchDensePackedFast(handle, index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset, identity_read_generation, out_result) catch |err| return capi.mapError(err);
    if (fast) return .ok;

    var owned = searchDenseOwned(handle, index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    packDenseHits(owned.total_hits, owned.ids, owned.scores, owned.identity_read_generation, out_result) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_search_dense_profile(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    vector_ptr: ?[*]const f32,
    vector_len: usize,
    k: u32,
    limit: u32,
    offset: u32,
    out_profile: *capi.DenseSearchProfile,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_search_dense_profile), .{ .name = "antfly_db_search_dense_profile" }), .{ handle_ptr, index_name, vector_ptr, vector_len, k, limit, offset, out_profile });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    if (vector_ptr == null or vector_len == 0) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": query vector must be nonempty and have a non-null pointer");
    handle.prepareDenseSearchRequest(index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset) catch |err| return capi.mapError(err);

    var profiled = searchDenseOwnedProfiled(handle, index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset) catch |err| return capi.mapError(err);
    defer profiled.deinit();

    out_profile.* = .{
        .total_ns = profiled.total_ns,
        .index_lookup_ns = profiled.index_lookup_ns,
        .search_ns = profiled.search_ns,
        .hits_ns = profiled.hits_ns,
        .fallback_ns = profiled.fallback_ns,
        .hbc_total_ns = profiled.hbc_total_ns,
        .hbc_setup_ns = profiled.hbc_setup_ns,
        .hbc_root_load_ns = profiled.hbc_root_load_ns,
        .hbc_node_cache_miss_ns = profiled.hbc_node_cache_miss_ns,
        .hbc_node_cache_misses = profiled.hbc_node_cache_misses,
        .hbc_quantized_cache_miss_ns = profiled.hbc_quantized_cache_miss_ns,
        .hbc_quantized_cache_misses = profiled.hbc_quantized_cache_misses,
        .hbc_child_expand_ns = profiled.hbc_child_expand_ns,
        .hbc_leaf_score_ns = profiled.hbc_leaf_score_ns,
        .hbc_rerank_ns = profiled.hbc_rerank_ns,
        .hbc_rerank_vector_load_ns = profiled.hbc_rerank_vector_load_ns,
        .hbc_rerank_distance_ns = profiled.hbc_rerank_distance_ns,
        .hbc_nodes_visited = profiled.hbc_nodes_visited,
        .hbc_leaves_explored = profiled.hbc_leaves_explored,
        .hbc_reranked_vectors = profiled.hbc_reranked_vectors,
        .hit_count = profiled.hit_count,
        .total_hits = profiled.total_hits,
        .used_fast_path = profiled.used_fast_path,
    };
    return .ok;
}

pub fn antfly_db_dense_noop(handle_ptr: ?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_dense_noop), .{ .name = "antfly_db_dense_noop" }), .{handle_ptr});
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    _ = guard.handle;
    return .ok;
}

pub fn antfly_db_dense_fixed_packed_result(
    handle_ptr: ?*anyopaque,
    out_result: *capi.PackedDenseSearchResult,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_dense_fixed_packed_result), .{ .name = "antfly_db_dense_fixed_packed_result" }), .{ handle_ptr, out_result });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;

    const ids = [_][]const u8{ "doc-fixed-1", "doc-fixed-2", "doc-fixed-3" };
    const scores = [_]f32{ 0.125, 0.25, 0.5 };
    packDenseHits(@intCast(ids.len), &ids, &scores, currentIdentityReadGenerationForHandle(handle, null) catch |err| return capi.mapError(err), out_result) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_search_dense_wire(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_search_dense_wire), .{ .name = "antfly_db_search_dense_wire" }), .{ handle_ptr, request_buf, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;

    var req = search_wire.decodeDenseRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeDenseRequest(handle.alloc, &req);
    handle.prepareDenseSearchRequest(req.index_name, req.vector, req.k, req.limit, req.offset) catch |err| return capi.mapError(err);

    const identity_read_generation = currentIdentityReadGenerationForHandle(handle, null) catch |err| return capi.mapError(err);
    const maybe_fast = searchDenseWireFast(handle, req.index_name, req.vector, req.k, req.limit, req.offset, identity_read_generation) catch |err| return capi.mapError(err);
    if (maybe_fast) |out| {
        out_buf.* = out;
        return .ok;
    }

    var owned = searchDenseOwned(handle, req.index_name, req.vector, req.k, req.limit, req.offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    out_buf.* = search_wire.encodeDenseResponseAtGeneration(owned.total_hits, owned.ids, owned.scores, owned.identity_read_generation) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_search_dense_wire_profile(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
    out_profile: *capi.DenseWireSearchProfile,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_search_dense_wire_profile), .{ .name = "antfly_db_search_dense_wire_profile" }), .{ handle_ptr, request_buf, out_buf, out_profile });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;

    var req = search_wire.decodeDenseRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeDenseRequest(handle.alloc, &req);
    handle.prepareDenseSearchRequest(req.index_name, req.vector, req.k, req.limit, req.offset) catch |err| return capi.mapError(err);

    const profiled = searchDenseWireOwnedProfiled(handle, request_buf.bytes()) catch |err| return capi.mapError(err);
    out_buf.* = profiled.out;
    out_profile.* = .{
        .total_ns = profiled.total_ns,
        .decode_ns = profiled.decode_ns,
        .search_ns = profiled.search_ns,
        .resolve_ns = profiled.resolve_ns,
        .encode_ns = profiled.encode_ns,
        .fallback_ns = profiled.fallback_ns,
        .hbc_total_ns = profiled.hbc_total_ns,
        .hbc_setup_ns = profiled.hbc_setup_ns,
        .hbc_root_load_ns = profiled.hbc_root_load_ns,
        .hbc_node_cache_miss_ns = profiled.hbc_node_cache_miss_ns,
        .hbc_node_cache_misses = profiled.hbc_node_cache_misses,
        .hbc_quantized_cache_miss_ns = profiled.hbc_quantized_cache_miss_ns,
        .hbc_quantized_cache_misses = profiled.hbc_quantized_cache_misses,
        .hbc_child_expand_ns = profiled.hbc_child_expand_ns,
        .hbc_leaf_score_ns = profiled.hbc_leaf_score_ns,
        .hbc_rerank_ns = profiled.hbc_rerank_ns,
        .hbc_rerank_vector_load_ns = profiled.hbc_rerank_vector_load_ns,
        .hbc_rerank_distance_ns = profiled.hbc_rerank_distance_ns,
        .hbc_nodes_visited = profiled.hbc_nodes_visited,
        .hbc_leaves_explored = profiled.hbc_leaves_explored,
        .hbc_reranked_vectors = profiled.hbc_reranked_vectors,
        .hit_count = profiled.hit_count,
        .total_hits = profiled.total_hits,
        .used_fast_path = profiled.used_fast_path,
    };
    return .ok;
}

pub fn antfly_db_search_text_match(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    field: capi.Slice,
    text: capi.Slice,
    limit: u32,
    offset: u32,
    out_result: *capi.DenseSearchResult,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_search_text_match), .{ .name = "antfly_db_search_text_match" }), .{ handle_ptr, index_name, field, text, limit, offset, out_result });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    var owned = searchTextMatchOwned(handle, index_name.bytes(), field.bytes(), text.bytes(), "", 1.0, limit, offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    const result_alloc = std.heap.c_allocator;
    const hits = result_alloc.alloc(capi.DenseSearchHit, owned.ids.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |hit| {
            if (hit.id_ptr != null and hit.id_len > 0) result_alloc.free(hit.id_ptr.?[0..hit.id_len]);
        }
        result_alloc.free(hits);
    }
    for (owned.ids, owned.scores, 0..) |id, score, i| {
        const duped = result_alloc.dupe(u8, id) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        hits[i] = .{
            .id_ptr = duped.ptr,
            .id_len = duped.len,
            .score = score,
        };
        initialized += 1;
    }
    out_result.* = .{
        .hits_ptr = hits.ptr,
        .hit_count = hits.len,
        .total_hits = owned.total_hits,
        .identity_read_generation = owned.identity_read_generation,
    };
    return .ok;
}

pub fn antfly_db_search_text_match_wire(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_search_text_match_wire), .{ .name = "antfly_db_search_text_match_wire" }), .{ handle_ptr, request_buf, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;

    var req = search_wire.decodeTextMatchRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeTextMatchRequest(handle.alloc, &req);

    var owned = searchTextMatchOwned(handle, req.index_name, req.field, req.text, req.analyzer, req.boost, req.limit, req.offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    out_buf.* = search_wire.encodeDenseResponseAtGeneration(owned.total_hits, owned.ids, owned.scores, owned.identity_read_generation) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_search_text_term_wire(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_search_text_term_wire), .{ .name = "antfly_db_search_text_term_wire" }), .{ handle_ptr, request_buf, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;

    var req = search_wire.decodeTextTermRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeTextTermRequest(handle.alloc, &req);

    var owned = searchTextTermOwned(handle, req.index_name, req.field, req.text, req.boost, req.limit, req.offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    out_buf.* = search_wire.encodeDenseResponseAtGeneration(owned.total_hits, owned.ids, owned.scores, owned.identity_read_generation) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_search_text_match_phrase_wire(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_search_text_match_phrase_wire), .{ .name = "antfly_db_search_text_match_phrase_wire" }), .{ handle_ptr, request_buf, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;

    var req = search_wire.decodeTextMatchPhraseRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeTextMatchPhraseRequest(handle.alloc, &req);

    var owned = searchTextMatchPhraseOwned(handle, req.index_name, req.field, req.text, req.analyzer, req.fuzziness, req.auto, req.boost, req.limit, req.offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    out_buf.* = search_wire.encodeDenseResponseAtGeneration(owned.total_hits, owned.ids, owned.scores, owned.identity_read_generation) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_search_hits_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_result: *capi.DenseSearchResult,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_search_hits_json), .{ .name = "antfly_db_search_hits_json" }), .{ handle_ptr, request_json, out_result });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        mode: []const u8,
        index_name: []const u8 = "",
        text_query_type: []const u8 = "",
        text_query_json: []const u8 = "",
        field: []const u8 = "",
        text: []const u8 = "",
        vector: []const f32 = &.{},
        indices: []const u32 = &.{},
        values: []const f32 = &.{},
        k: u32 = 10,
        return_mode: []const u8 = "parent",
        max_chunks_per_parent: u32 = 0,
        limit: u32 = 10,
        offset: u32 = 0,
        include_stored: bool = false,
        identity_read_generation: ?u64 = null,
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer parsed.deinit();

    if (parsed.value.include_stored) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": ID-only search does not support include_stored");
    if (!std.mem.eql(u8, parsed.value.return_mode, "parent")) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": ID-only search requires return_mode parent");

    var query_arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer query_arena.deinit();
    const query_alloc = query_arena.allocator();

    var req: db_mod.types.SearchRequest = .{
        .index_name = if (parsed.value.index_name.len > 0) parsed.value.index_name else null,
        .return_mode = .parent,
        .max_chunks_per_parent = parsed.value.max_chunks_per_parent,
        .limit = parsed.value.limit,
        .offset = parsed.value.offset,
        .include_stored = false,
        .identity_read_generation = parsed.value.identity_read_generation,
    };

    if (std.mem.eql(u8, parsed.value.mode, "full_text")) {
        if (parsed.value.text_query_json.len > 0) {
            var parsed_query = std.json.parseFromSlice(std.json.Value, query_alloc, parsed.value.text_query_json, .{}) catch |err| return capi.mapErrorContext(err, @src().fn_name);
            defer parsed_query.deinit();
            req.full_text = parseTextQueryJson(query_alloc, parsed_query.value) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        } else {
            req.full_text = if (std.mem.eql(u8, parsed.value.text_query_type, "term"))
                .{ .term = .{ .field = parsed.value.field, .term = parsed.value.text } }
            else if (std.mem.eql(u8, parsed.value.text_query_type, "match"))
                .{ .match = .{ .field = parsed.value.field, .text = parsed.value.text } }
            else
                .{ .match_all = {} };
        }
    } else if (std.mem.eql(u8, parsed.value.mode, "sparse")) {
        req.sparse = .{
            .indices = parsed.value.indices,
            .values = parsed.value.values,
            .k = parsed.value.k,
        };
    } else {
        return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": hit-only search mode must be full_text or sparse");
    }

    var result = runAtStampedGeneration(handle, &req, LocalSearchQuery{}) catch |err| return capi.mapError(err);
    defer result.deinit();
    if (result.graph_results.len > 0) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": ID-only search cannot return graph result documents");

    const result_alloc = std.heap.c_allocator;
    const hits = result_alloc.alloc(capi.DenseSearchHit, result.hits.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |hit| {
            if (hit.id_ptr != null and hit.id_len > 0) result_alloc.free(hit.id_ptr.?[0..hit.id_len]);
        }
        result_alloc.free(hits);
    }
    for (result.hits, 0..) |hit, i| {
        if (hit.stored_data != null or hit.chunk_hits.len > 0) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": ID-only search cannot return stored documents or chunk hits");
        const id = result_alloc.dupe(u8, hit.id) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        hits[i] = .{
            .id_ptr = id.ptr,
            .id_len = id.len,
            .score = hit.score orelse 0,
        };
        initialized += 1;
    }
    out_result.* = .{
        .hits_ptr = hits.ptr,
        .hit_count = hits.len,
        .total_hits = result.total_hits,
        .identity_read_generation = req.identity_read_generation.?,
    };
    return .ok;
}

pub fn parseTextQueryJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror!db_mod.types.TextQuery {
    if (value != .object) return error.InvalidArgument;
    if (value.object.get("match_all") != null) {
        return .{ .match_all = {} };
    }
    if (value.object.get("match_none") != null) {
        return .{ .match_none = {} };
    }
    if (value.object.get("phrase")) |phrase| {
        if (phrase != .object) return error.InvalidArgument;
        const edits_value = phrase.object.get("max_edits") orelse std.json.Value{ .integer = 0 };
        return .{ .phrase = .{
            .field = (phrase.object.get("field") orelse return error.InvalidArgument).string,
            .terms = try parseStringArrayJson(alloc, phrase.object.get("terms") orelse return error.InvalidArgument),
            .max_edits = @intCast(switch (edits_value) {
                .integer => |v| v,
                else => return error.InvalidArgument,
            }),
            .auto_fuzzy = if (phrase.object.get("auto_fuzzy")) |auto| switch (auto) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(phrase.object),
        } };
    }
    if (value.object.get("multi_phrase")) |phrase| {
        if (phrase != .object) return error.InvalidArgument;
        const edits_value = phrase.object.get("max_edits") orelse std.json.Value{ .integer = 0 };
        return .{ .multi_phrase = .{
            .field = (phrase.object.get("field") orelse return error.InvalidArgument).string,
            .terms = try parseStringMatrixJson(alloc, phrase.object.get("terms") orelse return error.InvalidArgument),
            .max_edits = @intCast(switch (edits_value) {
                .integer => |v| v,
                else => return error.InvalidArgument,
            }),
            .auto_fuzzy = if (phrase.object.get("auto_fuzzy")) |auto| switch (auto) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(phrase.object),
        } };
    }
    if (value.object.get("term")) |term| {
        if (term != .object) return error.InvalidArgument;
        return .{ .term = .{
            .field = (term.object.get("field") orelse return error.InvalidArgument).string,
            .term = (term.object.get("term") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(term.object),
        } };
    }
    if (value.object.get("match")) |match| {
        if (match != .object) return error.InvalidArgument;
        return .{ .match = .{
            .field = (match.object.get("field") orelse return error.InvalidArgument).string,
            .text = (match.object.get("text") orelse return error.InvalidArgument).string,
            .analyzer = if (match.object.get("analyzer")) |analyzer| switch (analyzer) {
                .string => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .boost = try parseOptionalBoostJson(match.object),
        } };
    }
    if (value.object.get("match_phrase")) |phrase| {
        if (phrase != .object) return error.InvalidArgument;
        const edits_value = phrase.object.get("max_edits") orelse std.json.Value{ .integer = 0 };
        return .{ .match_phrase = .{
            .field = (phrase.object.get("field") orelse return error.InvalidArgument).string,
            .text = (phrase.object.get("text") orelse return error.InvalidArgument).string,
            .analyzer = if (phrase.object.get("analyzer")) |analyzer| switch (analyzer) {
                .string => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .max_edits = @intCast(switch (edits_value) {
                .integer => |v| v,
                else => return error.InvalidArgument,
            }),
            .auto_fuzzy = if (phrase.object.get("auto_fuzzy")) |auto| switch (auto) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(phrase.object),
        } };
    }
    if (value.object.get("fuzzy")) |fuzzy| {
        if (fuzzy != .object) return error.InvalidArgument;
        const edits_value = fuzzy.object.get("max_edits") orelse std.json.Value{ .integer = 1 };
        return .{ .fuzzy = .{
            .field = (fuzzy.object.get("field") orelse return error.InvalidArgument).string,
            .term = (fuzzy.object.get("term") orelse return error.InvalidArgument).string,
            .max_edits = @intCast(switch (edits_value) {
                .integer => |v| v,
                else => return error.InvalidArgument,
            }),
            .prefix_len = if (fuzzy.object.get("prefix_length")) |prefix| switch (prefix) {
                .integer => |v| @intCast(v),
                else => return error.InvalidArgument,
            } else 0,
            .auto_fuzzy = if (fuzzy.object.get("auto_fuzzy")) |auto| switch (auto) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(fuzzy.object),
        } };
    }
    if (value.object.get("numeric_range")) |range_query| {
        if (range_query != .object) return error.InvalidArgument;
        return .{ .numeric_range = .{
            .field = (range_query.object.get("field") orelse return error.InvalidArgument).string,
            .min = if (range_query.object.get("min")) |min| switch (min) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .max = if (range_query.object.get("max")) |max| switch (max) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .inclusive_min = if (range_query.object.get("inclusive_min")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else true,
            .inclusive_max = if (range_query.object.get("inclusive_max")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(range_query.object),
        } };
    }
    if (value.object.get("date_range")) |range_query| {
        if (range_query != .object) return error.InvalidArgument;
        return .{ .date_range = .{
            .field = (range_query.object.get("field") orelse return error.InvalidArgument).string,
            .start_ns = if (range_query.object.get("start_ns")) |start| switch (start) {
                .integer => |v| @intCast(v),
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .end_ns = if (range_query.object.get("end_ns")) |end| switch (end) {
                .integer => |v| @intCast(v),
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .inclusive_start = if (range_query.object.get("inclusive_start")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else true,
            .inclusive_end = if (range_query.object.get("inclusive_end")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(range_query.object),
        } };
    }
    if (value.object.get("doc_id")) |doc_id| {
        if (doc_id != .object) return error.InvalidArgument;
        return .{ .doc_id = .{
            .ids = try parseStringArrayJson(alloc, doc_id.object.get("ids") orelse return error.InvalidArgument),
            .boost = try parseOptionalBoostJson(doc_id.object),
        } };
    }
    if (value.object.get("bool_field")) |bool_field| {
        if (bool_field != .object) return error.InvalidArgument;
        return .{ .bool_field = .{
            .field = (bool_field.object.get("field") orelse return error.InvalidArgument).string,
            .value = switch (bool_field.object.get("value") orelse return error.InvalidArgument) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            },
            .boost = try parseOptionalBoostJson(bool_field.object),
        } };
    }
    if (value.object.get("geo_distance")) |geo_distance| {
        if (geo_distance != .object) return error.InvalidArgument;
        return .{ .geo_distance = .{
            .field = (geo_distance.object.get("field") orelse return error.InvalidArgument).string,
            .lon = switch (geo_distance.object.get("lon") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .lat = switch (geo_distance.object.get("lat") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .radius_meters = switch (geo_distance.object.get("radius_meters") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .boost = try parseOptionalBoostJson(geo_distance.object),
        } };
    }
    if (value.object.get("geo_bbox")) |geo_bbox| {
        if (geo_bbox != .object) return error.InvalidArgument;
        return .{ .geo_bbox = .{
            .field = (geo_bbox.object.get("field") orelse return error.InvalidArgument).string,
            .min_lat = switch (geo_bbox.object.get("min_lat") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .min_lon = switch (geo_bbox.object.get("min_lon") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .max_lat = switch (geo_bbox.object.get("max_lat") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .max_lon = switch (geo_bbox.object.get("max_lon") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .boost = try parseOptionalBoostJson(geo_bbox.object),
        } };
    }
    if (value.object.get("prefix")) |prefix| {
        if (prefix != .object) return error.InvalidArgument;
        return .{ .prefix = .{
            .field = (prefix.object.get("field") orelse return error.InvalidArgument).string,
            .prefix = (prefix.object.get("prefix") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(prefix.object),
        } };
    }
    if (value.object.get("wildcard")) |wildcard| {
        if (wildcard != .object) return error.InvalidArgument;
        return .{ .wildcard = .{
            .field = (wildcard.object.get("field") orelse return error.InvalidArgument).string,
            .pattern = (wildcard.object.get("pattern") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(wildcard.object),
        } };
    }
    if (value.object.get("regexp")) |regexp| {
        if (regexp != .object) return error.InvalidArgument;
        return .{ .regexp = .{
            .field = (regexp.object.get("field") orelse return error.InvalidArgument).string,
            .pattern = (regexp.object.get("pattern") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(regexp.object),
        } };
    }
    if (value.object.get("term_range")) |term_range| {
        if (term_range != .object) return error.InvalidArgument;
        return .{ .term_range = .{
            .field = (term_range.object.get("field") orelse return error.InvalidArgument).string,
            .min = if (term_range.object.get("min")) |min| switch (min) {
                .string => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .max = if (term_range.object.get("max")) |max| switch (max) {
                .string => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .inclusive_min = if (term_range.object.get("inclusive_min")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else true,
            .inclusive_max = if (term_range.object.get("inclusive_max")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(term_range.object),
        } };
    }
    if (value.object.get("ip_range")) |ip_range| {
        if (ip_range != .object) return error.InvalidArgument;
        return .{ .ip_range = .{
            .field = (ip_range.object.get("field") orelse return error.InvalidArgument).string,
            .cidr = (ip_range.object.get("cidr") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(ip_range.object),
        } };
    }
    if (value.object.get("geo_shape")) |geo_shape| {
        if (geo_shape != .object) return error.InvalidArgument;
        return .{ .geo_shape = .{
            .field = (geo_shape.object.get("field") orelse return error.InvalidArgument).string,
            .relation = if (geo_shape.object.get("relation")) |relation|
                try parseGeoShapeRelation(relation)
            else
                .intersects,
            .polygons = try parseGeoShapePolygonsJson(alloc, geo_shape),
            .boost = try parseOptionalBoostJson(geo_shape.object),
        } };
    }
    if (value.object.get("bool")) |bool_query| {
        if (bool_query != .object) return error.InvalidArgument;

        var must_list = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
        errdefer must_list.deinit(alloc);
        if (bool_query.object.get("filter")) |filter_value| {
            try appendTextQueryArrayJson(alloc, &must_list, filter_value);
        }
        if (bool_query.object.get("must")) |must_value| {
            try appendTextQueryArrayJson(alloc, &must_list, must_value);
        }
        const must = if (must_list.items.len > 0)
            try must_list.toOwnedSlice(alloc)
        else
            &.{};
        const should = if (bool_query.object.get("should")) |should_value|
            try parseTextQueryArrayJson(alloc, should_value)
        else
            &.{};
        const must_not = if (bool_query.object.get("must_not")) |must_not_value|
            try parseTextQueryArrayJson(alloc, must_not_value)
        else
            &.{};
        const min_should = if (bool_query.object.get("min_should")) |min_should_value|
            try parseMinShouldJson(min_should_value)
        else
            0;

        if (must.len == 0 and should.len == 0 and must_not.len == 0) return error.InvalidArgument;
        return .{ .bool_query = .{
            .must = must,
            .should = should,
            .must_not = must_not,
            .min_should = min_should,
            .boost = try parseOptionalBoostJson(bool_query.object),
        } };
    }
    if (value.object.get("conjuncts")) |conjuncts| {
        return .{ .bool_query = .{ .must = try parseTextQueryArrayJson(alloc, conjuncts) } };
    }
    if (value.object.get("disjuncts")) |disjuncts| {
        const min_should = if (value.object.get("min_should")) |min_should_value|
            try parseMinShouldJson(min_should_value)
        else
            0;
        return .{ .bool_query = .{
            .should = try parseTextQueryArrayJson(alloc, disjuncts),
            .min_should = min_should,
        } };
    }
    return error.InvalidArgument;
}

pub fn parseMinShouldJson(value: std.json.Value) anyerror!u32 {
    return switch (value) {
        .integer => |v| if (v < 0) error.InvalidArgument else @intCast(v),
        .float => |v| blk: {
            if (v < 0 or @floor(v) != v or v > std.math.maxInt(u32)) return error.InvalidArgument;
            break :blk @intFromFloat(v);
        },
        else => error.InvalidArgument,
    };
}

pub fn parseOptionalBoostJson(object: std.json.ObjectMap) anyerror!f32 {
    if (object.get("boost")) |value| {
        return switch (value) {
            .float => |v| @floatCast(v),
            .integer => |v| @floatFromInt(v),
            else => return error.InvalidArgument,
        };
    }
    return 1.0;
}

pub fn parseStringArrayJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]const []const u8 {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    var items = try alloc.alloc([]const u8, value.array.items.len);
    errdefer alloc.free(items);
    for (value.array.items, 0..) |item, i| {
        if (item != .string) return error.InvalidArgument;
        items[i] = item.string;
    }
    return items;
}

pub fn parseStringMatrixJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]const []const []const u8 {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    var rows = try alloc.alloc([]const []const u8, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |row| alloc.free(row);
        alloc.free(rows);
    }
    for (value.array.items, 0..) |item, i| {
        rows[i] = try parseStringArrayJson(alloc, item);
        initialized += 1;
    }
    return rows;
}

pub fn parseGeoPointJson(value: std.json.Value) anyerror!db_mod.types.GeoPoint {
    if (value != .object) return error.InvalidArgument;
    return .{
        .lon = switch (value.object.get("lon") orelse return error.InvalidArgument) {
            .integer => |v| @floatFromInt(v),
            .float => |v| v,
            else => return error.InvalidArgument,
        },
        .lat = switch (value.object.get("lat") orelse return error.InvalidArgument) {
            .integer => |v| @floatFromInt(v),
            .float => |v| v,
            else => return error.InvalidArgument,
        },
    };
}

pub fn parseGeoPointArrayJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]const db_mod.types.GeoPoint {
    if (value != .array or value.array.items.len < 3) return error.InvalidArgument;
    var points = try alloc.alloc(db_mod.types.GeoPoint, value.array.items.len);
    errdefer alloc.free(points);
    for (value.array.items, 0..) |item, i| {
        points[i] = try parseGeoPointJson(item);
    }
    if (!std.meta.eql(points[0], points[points.len - 1])) {
        var closed = try alloc.alloc(db_mod.types.GeoPoint, points.len + 1);
        @memcpy(closed[0..points.len], points);
        closed[points.len] = points[0];
        alloc.free(points);
        return closed;
    }
    return points;
}

pub fn parseGeoShapePolygonsJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]const []const db_mod.types.GeoPoint {
    if (value.object.get("polygons")) |polygons_value| {
        if (polygons_value != .array or polygons_value.array.items.len == 0) return error.InvalidArgument;
        var polygons = try alloc.alloc([]const db_mod.types.GeoPoint, polygons_value.array.items.len);
        var initialized: usize = 0;
        errdefer {
            for (polygons[0..initialized]) |polygon| alloc.free(polygon);
            alloc.free(polygons);
        }
        for (polygons_value.array.items, 0..) |item, i| {
            polygons[i] = try parseGeoPointArrayJson(alloc, item);
            initialized += 1;
        }
        return polygons;
    }
    if (value.object.get("polygon")) |polygon_value| {
        var polygons = try alloc.alloc([]const db_mod.types.GeoPoint, 1);
        errdefer alloc.free(polygons);
        polygons[0] = try parseGeoPointArrayJson(alloc, polygon_value);
        return polygons;
    }
    return error.InvalidArgument;
}

pub fn parseGeoShapeRelation(value: std.json.Value) anyerror!db_mod.types.GeoShapeRelation {
    if (value != .string) return error.InvalidArgument;
    if (std.mem.eql(u8, value.string, "intersects")) return .intersects;
    if (std.mem.eql(u8, value.string, "within")) return .within;
    if (std.mem.eql(u8, value.string, "contains")) return .contains;
    return error.InvalidArgument;
}

pub fn parseTextQueryArrayJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]db_mod.types.TextQuery {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    var clauses = try alloc.alloc(db_mod.types.TextQuery, value.array.items.len);
    for (value.array.items, 0..) |item, i| {
        clauses[i] = try parseTextQueryJson(alloc, item);
    }
    return clauses;
}

pub fn appendTextQueryArrayJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    value: std.json.Value,
) anyerror!void {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    try out.ensureUnusedCapacity(alloc, value.array.items.len);
    for (value.array.items) |item| {
        out.appendAssumeCapacity(try parseTextQueryJson(alloc, item));
    }
}

pub fn antfly_db_execute_graph_queries_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_execute_graph_queries_json), .{ .name = "antfly_db_execute_graph_queries_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        graph_queries: []const JsonGraphQueryRequest,
        named_sets: []const JsonNamedGraphInputSetRequest,
        limit: u32 = 10,
        offset: u32 = 0,
        include_stored: bool = true,
        identity_read_generation: ?u64 = null,
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{ .parse_numbers = false }) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer parsed.deinit();

    if (parsed.value.identity_read_generation == null) {
        for (parsed.value.named_sets) |named_set| {
            if (named_set.hit_ids_b64.len > 0) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": named ID sets require a pinned read generation");
        }
    }

    const graph_queries = parseNamedGraphQueries(handle.alloc, parsed.value.graph_queries) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer freeOwnedNamedGraphQueries(handle.alloc, graph_queries);

    const named_sets = parseNamedGraphInputSets(handle.alloc, parsed.value.named_sets) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer freeOwnedNamedGraphInputSets(handle.alloc, named_sets);

    var req: db_mod.types.SearchRequest = .{
        .limit = parsed.value.limit,
        .offset = parsed.value.offset,
        .include_stored = parsed.value.include_stored,
        .graph_queries = graph_queries,
        .identity_read_generation = parsed.value.identity_read_generation,
    };

    const GraphQuery = struct {
        graph_queries: []const db_mod.types.NamedGraphQuery,
        named_sets: []const db_mod.types.NamedGraphInputSet,
        const Result = []db_mod.types.GraphSearchResult;
        pub fn run(self: @This(), h: *Handle, r: db_mod.types.SearchRequest) !Result {
            return h.database().*.executeNamedGraphQueries(h.alloc, r, self.graph_queries, self.named_sets);
        }
    };
    const results = runAtStampedGeneration(handle, &req, GraphQuery{
        .graph_queries = graph_queries,
        .named_sets = named_sets,
    }) catch |err| return capi.mapError(err);
    defer {
        for (results) |*result| result.deinit(handle.alloc);
        if (results.len > 0) handle.alloc.free(results);
    }

    var payload = handle.alloc.alloc(JsonGraphSearchResult, results.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (results, 0..) |result, i| {
        payload[i] = JsonGraphSearchResult.init(handle.alloc, result, req.identity_read_generation) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_aggregate_hits_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_aggregate_hits_json), .{ .name = "antfly_db_aggregate_hits_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    var parsed = std.json.parseFromSlice(JsonAggregateHitsRequest, handle.alloc, request_json.bytes(), .{}) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer parsed.deinit();

    if (parsed.value.hit_ids_b64.len > 0 and parsed.value.identity_read_generation == null) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": hit IDs require identity_read_generation");
    const identity_read_generation = currentIdentityReadGenerationForHandle(handle, parsed.value.identity_read_generation) catch |err| return capi.mapError(err);

    const requests = toAggregationRequest(handle.alloc, parsed.value.aggregations) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer freeAggregationRequests(handle.alloc, requests);

    var hits = handle.alloc.alloc(db_mod.types.SearchHit, parsed.value.hit_ids_b64.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var hit_count: usize = 0;
    defer {
        for (hits[0..hit_count]) |*hit| hit.deinit(handle.alloc);
        if (hits.len > 0) handle.alloc.free(hits);
    }
    for (parsed.value.hit_ids_b64) |item| {
        const hit_id = decodeBase64Alloc(handle.alloc, item) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        errdefer handle.alloc.free(hit_id);
        const stored = handle.database().*.get(handle.alloc, hit_id) catch |err| {
            handle.alloc.free(hit_id);
            return capi.mapError(err);
        } orelse {
            handle.alloc.free(hit_id);
            continue;
        };
        hits[hit_count] = .{
            .id = hit_id,
            .stored_data = stored,
        };
        hit_count += 1;
    }

    const result = db_mod.types.SearchResult{
        .alloc = handle.alloc,
        .hits = hits[0..hit_count],
        .total_hits = @intCast(hit_count),
    };
    const backend_results = aggregations_mod.computeSearchAggregations(handle.alloc, requests, result, .{
        .index_manager = handle.database().*.core.index_manager,
        .full_text_index_name = if (parsed.value.index_name.len > 0) parsed.value.index_name else null,
        .identity_read_generation = identity_read_generation,
    }) catch |err| return capi.mapError(err);
    defer aggregations_mod.deinitResults(handle.alloc, backend_results);

    const aggregation_results = toJsonAggregationResults(handle.alloc, backend_results) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer {
        for (aggregation_results) |*item| item.deinit(handle.alloc);
        if (aggregation_results.len > 0) handle.alloc.free(aggregation_results);
    }

    out_buf.* = stringifyJson(aggregation_results) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn parseNamedGraphQueries(alloc: Allocator, requests: []const JsonGraphQueryRequest) ![]db_mod.types.NamedGraphQuery {
    var queries = try alloc.alloc(db_mod.types.NamedGraphQuery, requests.len);
    errdefer alloc.free(queries);
    var count: usize = 0;
    errdefer {
        for (queries[0..count]) |*query| deinitOwnedNamedGraphQuery(alloc, query);
    }
    for (requests, 0..) |request, i| {
        const name = try alloc.dupe(u8, request.name);
        errdefer alloc.free(name);
        queries[i] = .{
            .name = name,
            .query = try parseGraphQueryRequestOwned(alloc, request),
        };
        count += 1;
    }
    return queries;
}

pub fn parseNamedGraphInputSets(alloc: Allocator, requests: []const JsonNamedGraphInputSetRequest) ![]db_mod.types.NamedGraphInputSet {
    var sets = try alloc.alloc(db_mod.types.NamedGraphInputSet, requests.len);
    errdefer alloc.free(sets);
    var count: usize = 0;
    errdefer {
        for (sets[0..count]) |*set| deinitOwnedNamedGraphInputSet(alloc, set);
    }
    for (requests, 0..) |request, i| {
        sets[i] = .{
            .name = try alloc.dupe(u8, request.name),
            .hit_ids = try decodeGraphHitIds(alloc, request.hit_ids_b64),
            .total_hits = request.total_hits,
        };
        count += 1;
    }
    return sets;
}

fn parseEmbeddedRelationshipFilter(alloc: Allocator, value: ?std.json.Value) !relationship_filter.Filter {
    return if (value) |filter| relationship_filter.parsePublicAlloc(alloc, filter) else .{};
}

pub fn parseGraphQueryRequestOwned(alloc: Allocator, request: JsonGraphQueryRequest) !graph_query_mod.GraphQuery {
    const query_type: graph_query_mod.QueryType = if (std.mem.eql(u8, request.type, "neighbors")) .neighbors else if (std.mem.eql(u8, request.type, "traverse")) .traverse else if (std.mem.eql(u8, request.type, "shortest_path")) .shortest_path else if (std.mem.eql(u8, request.type, "k_shortest_paths")) .k_shortest_paths else return error.InvalidArgument;
    const edge_filter = try parseEmbeddedRelationshipFilter(alloc, request.edge_filter);
    errdefer edge_filter.deinit(alloc);
    const index_name = try alloc.dupe(u8, request.index_name);
    errdefer alloc.free(index_name);
    var start_nodes = try parseGraphNodeSelectorRequestOwned(alloc, request.start_nodes);
    errdefer deinitOwnedNodeSelector(alloc, &start_nodes);
    var target_nodes = if (request.target_nodes) |target| try parseGraphNodeSelectorRequestOwned(alloc, target) else null;
    errdefer if (target_nodes) |*target| deinitOwnedNodeSelector(alloc, target);
    const edge_types = try cloneGraphEdgeTypes(alloc, request.edge_types);
    errdefer {
        for (edge_types) |edge_type| alloc.free(edge_type);
        alloc.free(edge_types);
    }
    return .{
        .query_type = query_type,
        .index_name = index_name,
        .start_nodes = start_nodes,
        .target_nodes = target_nodes,
        .params = .{
            .edge_types = edge_types,
            .edge_filter = edge_filter,
            .direction = parseGraphDirection(request.direction),
            .max_depth = request.max_depth,
            .max_results = request.max_results,
            .min_weight = legacyGraphWeightBound(request.min_weight),
            .max_weight = legacyGraphWeightBound(request.max_weight),
            .deduplicate = request.deduplicate,
            .include_paths = request.include_paths,
            .weight_mode = parseGraphWeightMode(request.weight_mode),
        },
        .k = request.k,
    };
}

pub fn parseGraphNodeSelectorRequestOwned(alloc: Allocator, selector: JsonGraphNodeSelectorRequest) !graph_query_mod.NodeSelector {
    if (selector.keys.len > 0) return .{ .keys = try decodeGraphKeys(alloc, selector.keys) };
    if (selector.result_ref.len > 0) {
        return .{ .result_ref = .{
            .ref = try alloc.dupe(u8, selector.result_ref),
            .limit = selector.limit,
        } };
    }
    return error.InvalidArgument;
}

pub fn decodeGraphKeys(alloc: Allocator, keys: []const []const u8) ![]const []const u8 {
    var owned = try alloc.alloc([]const u8, keys.len);
    errdefer alloc.free(owned);
    var count: usize = 0;
    errdefer {
        for (owned[0..count]) |key| alloc.free(@constCast(key));
    }
    for (keys, 0..) |key, i| {
        owned[i] = try decodeBase64Alloc(alloc, key);
        count += 1;
    }
    return owned;
}

pub fn cloneGraphEdgeTypes(alloc: Allocator, edge_types: []const []const u8) ![]const []const u8 {
    var owned = try alloc.alloc([]const u8, edge_types.len);
    errdefer alloc.free(owned);
    var count: usize = 0;
    errdefer {
        for (owned[0..count]) |item| alloc.free(@constCast(item));
    }
    for (edge_types, 0..) |edge_type, i| {
        owned[i] = try alloc.dupe(u8, edge_type);
        count += 1;
    }
    return owned;
}

pub fn decodeGraphHitIds(alloc: Allocator, hit_ids_b64: []const []const u8) ![]const []const u8 {
    var hit_ids = try alloc.alloc([]const u8, hit_ids_b64.len);
    errdefer alloc.free(hit_ids);
    var count: usize = 0;
    errdefer {
        for (hit_ids[0..count]) |hit_id| alloc.free(@constCast(hit_id));
    }
    for (hit_ids_b64, 0..) |item, i| {
        hit_ids[i] = try decodeBase64Alloc(alloc, item);
        count += 1;
    }
    return hit_ids;
}

pub fn deinitOwnedNodeSelector(alloc: Allocator, selector: *graph_query_mod.NodeSelector) void {
    switch (selector.*) {
        .keys => |keys| {
            for (keys) |key| alloc.free(@constCast(key));
            if (keys.len > 0) alloc.free(keys);
        },
        .identities => |identities| {
            for (identities) |identity| {
                alloc.free(@constCast(identity.key));
                if (identity.table) |table| alloc.free(@constCast(table));
            }
            if (identities.len > 0) alloc.free(identities);
        },
        .result_ref => |result_ref| {
            alloc.free(@constCast(result_ref.ref));
        },
    }
    selector.* = undefined;
}

pub fn deinitOwnedGraphQuery(alloc: Allocator, query: *graph_query_mod.GraphQuery) void {
    alloc.free(@constCast(query.index_name));
    deinitOwnedNodeSelector(alloc, &query.start_nodes);
    if (query.target_nodes) |*target_nodes| deinitOwnedNodeSelector(alloc, target_nodes);
    query.params.edge_filter.deinit(alloc);
    for (query.params.edge_types) |edge_type| alloc.free(@constCast(edge_type));
    if (query.params.edge_types.len > 0) alloc.free(query.params.edge_types);
    query.* = undefined;
}

pub fn deinitOwnedNamedGraphQuery(alloc: Allocator, query: *db_mod.types.NamedGraphQuery) void {
    alloc.free(query.name);
    deinitOwnedGraphQuery(alloc, &query.query);
    query.* = undefined;
}

pub fn freeOwnedNamedGraphQueries(alloc: Allocator, queries: []db_mod.types.NamedGraphQuery) void {
    for (queries) |*query| deinitOwnedNamedGraphQuery(alloc, query);
    if (queries.len > 0) alloc.free(queries);
}

pub fn deinitOwnedNamedGraphInputSet(alloc: Allocator, set: *db_mod.types.NamedGraphInputSet) void {
    alloc.free(@constCast(set.name));
    for (set.hit_ids) |hit_id| alloc.free(@constCast(hit_id));
    if (set.hit_ids.len > 0) alloc.free(@constCast(set.hit_ids));
    set.* = undefined;
}

pub fn freeOwnedNamedGraphInputSets(alloc: Allocator, sets: []db_mod.types.NamedGraphInputSet) void {
    for (sets) |*set| deinitOwnedNamedGraphInputSet(alloc, set);
    if (sets.len > 0) alloc.free(sets);
}

pub fn parseGraphDirection(direction: []const u8) db_mod.types.GraphEdgeDirection {
    if (std.mem.eql(u8, direction, "in")) return .in;
    if (std.mem.eql(u8, direction, "both")) return .both;
    return .out;
}

pub fn parseGraphWeightMode(mode: []const u8) db_mod.types.GraphPathWeightMode {
    if (std.mem.eql(u8, mode, "min_weight")) return .min_weight;
    if (std.mem.eql(u8, mode, "max_weight")) return .max_weight;
    return .min_hops;
}

pub fn legacyGraphWeightBound(value: f64) ?f64 {
    return if (value > 0 and std.math.isFinite(value)) value else null;
}

/// Render using the durable resolver's exact template and Unicode tables. No writes.
pub fn antfly_db_render_resolver_key_json(handle_ptr: ?*anyopaque, resolver_name: capi.Slice, mention_json: capi.Slice, out_buffer: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_render_resolver_key_json), .{ .name = "antfly_db_render_resolver_key_json" }), .{ handle_ptr, resolver_name, mention_json, out_buffer });
    capi.clearLastError();
    const out = out_buffer orelse return capi.mapErrorContext(error.InvalidArgument, "resolver key output pointer must not be null");
    out.* = .{};
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, "invalid or closed database handle");
    defer guard.leave();
    if (guard.entry_error) |code| return capi.ensureError(code, @src().fn_name);
    const handle = guard.handle;
    const configs = handle.database().listResolvers(handle.alloc) catch |err| return capi.mapError(err);
    defer {
        for (configs) |*cfg| cfg.deinit(handle.alloc);
        handle.alloc.free(configs);
    }
    for (configs) |cfg| {
        if (!std.mem.eql(u8, cfg.name, resolver_name.bytes())) continue;
        const Mention = struct { text: []const u8, label: []const u8, local_id: []const u8 = "query" };
        var parsed = std.json.parseFromSlice(Mention, handle.alloc, mention_json.bytes(), .{}) catch |err| return capi.mapErrorContext(err, "invalid resolver mention JSON");
        defer parsed.deinit();
        const resolver_lib = @import("antfly_resolver");
        const routing = @import("../storage/db/resolution_runtime.zig");
        const excluded = routing.siblingClaimedLabelsAlloc(handle.alloc, configs, &cfg) catch |err| return capi.mapError(err);
        defer routing.freeSiblingClaimedLabels(handle.alloc, excluded);
        var resolver = resolver_lib.Resolver.initFromParts(handle.alloc, cfg.table, cfg.key_template, .{ .labels = cfg.labels, .exclude_labels = excluded }, cfg.type_must_match, "") catch |err| return capi.mapError(err);
        defer resolver.deinit();
        if (!resolver.consumesLabel(parsed.value.label)) return capi.mapErrorContext(error.InvalidArgument, "mention label is not consumed by this resolver");
        const minted = resolver.renderKeyAlloc(handle.alloc, .{ .text = parsed.value.text, .label = parsed.value.label, .local_id = parsed.value.local_id }) catch |err| return capi.mapErrorContext(err, "rendering resolver key");
        defer handle.alloc.free(minted);
        const effective_aliases = cfg.effectiveAliasesAlloc(handle.alloc) catch |err| return capi.mapError(err);
        defer handle.alloc.free(effective_aliases);
        var policy_arena = std.heap.ArenaAllocator.init(handle.alloc);
        defer policy_arena.deinit();
        const redirects = resolver_lib.curation.RedirectIndex.init(policy_arena.allocator(), effective_aliases) catch |err| return capi.mapError(err);
        const key = redirects.canonical(minted);
        const policy = resolver_lib.curation.Index.init(policy_arena.allocator(), cfg.suppress_json, cfg.curation_json) catch |err| return capi.mapError(err);
        const suppressed = ((policy.textReason(policy_arena.allocator(), parsed.value.text) catch |err| return capi.mapError(err)) != null) or policy.keySuppressed(minted) or policy.keySuppressed(key);
        out.* = stringifyJson(.{ .minted_key = minted, .key = key, .table = cfg.table, .config_generation = cfg.effectiveGeneration(), .suppressed = suppressed }) catch |err| return capi.mapError(err);
        return .ok;
    }
    return capi.mapErrorContext(error.NotFound, "resolver does not exist");
}

/// Extract ad-hoc text (or accept extraction JSON) and resolve against the
/// durable resolver, including aliases and ANN. Never publishes query artifacts.
pub fn antfly_db_resolve_query_json(handle_ptr: ?*anyopaque, resolver_name: capi.Slice, request_json: capi.Slice, out_buffer: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_resolve_query_json), .{ .name = "antfly_db_resolve_query_json" }), .{ handle_ptr, resolver_name, request_json, out_buffer });
    capi.clearLastError();
    const out = out_buffer orelse return capi.mapErrorContext(error.InvalidArgument, "query resolution output pointer must not be null");
    out.* = .{};
    var guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, "invalid or closed database handle");
    defer guard.leave();
    if (guard.entry_error) |code| return capi.ensureError(code, @src().fn_name);
    var handle = guard.handle;
    const alloc = handle.alloc;
    var request = std.json.parseFromSlice(std.json.Value, alloc, request_json.bytes(), .{}) catch |err| return capi.mapErrorContext(err, "invalid query resolution JSON");
    defer request.deinit();
    if (request.value != .object) return capi.mapErrorContext(error.InvalidArgument, "query request must be an object containing text or extraction");
    const text = request.value.object.get("text");
    const extraction = request.value.object.get("extraction");
    if ((text == null) == (extraction == null)) return capi.mapErrorContext(error.InvalidArgument, "supply exactly one of text or extraction");
    if (text) |value| {
        if (value != .string) return capi.mapErrorContext(error.InvalidArgument, "query text must be a string");
    }
    if (extraction) |value| {
        if (value != .object or value.object.get("entities") == null or value.object.get("entities").? != .array) return capi.mapErrorContext(error.InvalidArgument, "extraction must contain an entities array");
    }
    const configs = handle.database().listResolvers(alloc) catch |err| return capi.mapError(err);
    defer {
        for (configs) |*cfg| cfg.deinit(alloc);
        alloc.free(configs);
    }
    for (configs) |*cfg| {
        if (!std.mem.eql(u8, cfg.name, resolver_name.bytes())) continue;
        if (cfg.source_artifact_kind == .chunk) return capi.mapErrorContext(error.InvalidArgument, "query resolution requires an asset-scoped resolver");
        // Native connections cache producers on their provider-owning snapshot;
        // read-only databases otherwise have no enrichment producer. Repeated
        // queries reuse transport/capability/model state. Only construction is locked;
        // invocation contexts share the runtime's synchronized capability cache.
        const producer_handle = if (guard.connection != null) handle.parent_handle orelse handle else handle;
        const cached_producer = text != null and (guard.connection != null or handle.database().enrichment_runtime == null);
        if (cached_producer) {
            producer_handle.query_producer_mutex.lockUncancelable(handleLockIo());
            defer producer_handle.query_producer_mutex.unlock(handleLockIo());
            if (producer_handle.query_producer == null) {
                const io = handle.database().backend_runtime.io() orelse return capi.mapErrorContext(error.SqlFeatureNotSupported, "query extraction requires a backend IO runtime");
                producer_handle.query_producer = @import("../asset_producer_runtime.zig").Runtime.createOwned(alloc, io, .{ .antfly_provider = producer_handle.liteAntflyProvider() }) catch |err| return capi.mapErrorContext(err, "creating query extractor runtime");
            }
        }
        const extracted = if (text) |value| extracted: {
            var plan = handle.database().resolverQueryPlanAlloc(alloc, cfg.*) catch |err| return capi.mapErrorContext(err, "query text requires the resolver's configured asset extractor");
            defer plan.deinit(alloc);
            if (guard.connection != null) guard.suspendQuery();
            const bytes = if (cached_producer)
                plan.produce(alloc, value.string, producer_handle.query_producer.?.producer().withInvocationContext(.{})) catch |err| return capi.mapErrorContext(err, "extracting query text")
            else
                handle.database().extractResolverQueryAlloc(alloc, cfg.*, value.string, null) catch |err| return capi.mapErrorContext(err, "extracting query text");
            if (!guard.resumeQuery(handle_ptr)) {
                alloc.free(bytes);
                return capi.mapErrorContext(error.Busy, "query handle closed during extraction; retry");
            }
            if (guard.entry_error) |code| {
                alloc.free(bytes);
                return capi.ensureError(code, @src().fn_name);
            }
            handle = guard.handle;
            // Canonical lookup uses the latest snapshot. Configuration changes
            // require a retry rather than mixing two resolver/schema versions.
            const current = handle.database().listResolvers(alloc) catch |err| {
                alloc.free(bytes);
                return capi.mapError(err);
            };
            defer {
                for (current) |*entry| entry.deinit(alloc);
                alloc.free(current);
            }
            var unchanged = current.len == configs.len;
            if (unchanged) for (current, configs) |a, b| {
                if (!a.eql(b)) {
                    unchanged = false;
                    break;
                }
            };
            var latest_plan = handle.database().resolverQueryPlanAlloc(alloc, cfg.*) catch |err| {
                alloc.free(bytes);
                return capi.mapError(err);
            };
            defer latest_plan.deinit(alloc);
            if (!unchanged or !plan.eql(latest_plan)) {
                alloc.free(bytes);
                return capi.mapErrorContext(error.Busy, "resolver or extractor configuration changed during extraction; retry");
            }
            break :extracted bytes;
        } else std.json.Stringify.valueAlloc(alloc, extraction.?, .{}) catch |err| return capi.mapError(err);
        defer alloc.free(extracted);
        const candidates = @import("entity_sink.zig").candidateSource(handle);
        const mention_embedder = handle.database().resolution_embedder;
        const suspend_resolution = guard.connection != null and (cfg.adjudicate_json.len > 0 or cfg.name_embedding.len > 0);
        if (suspend_resolution) guard.suspendQuery();
        const computed = @import("../storage/db/resolution_runtime.zig").resolveQueryExtractionAlloc(alloc, configs, cfg, extracted, candidates, mention_embedder);
        if (!guard.resumeQuery(handle_ptr)) {
            if (computed) |bytes| alloc.free(bytes) else |_| {}
            return capi.mapErrorContext(error.Busy, "query handle closed during resolution; retry");
        }
        if (guard.entry_error) |code| {
            if (computed) |bytes| alloc.free(bytes) else |_| {}
            return capi.ensureError(code, @src().fn_name);
        }
        handle = guard.handle;
        const resolution = computed catch |err| return capi.mapErrorContext(err, "resolving query mentions against canonical entities");
        defer alloc.free(resolution);
        if (suspend_resolution) {
            const current = handle.database().listResolvers(alloc) catch |err| return capi.mapError(err);
            defer {
                for (current) |*entry| entry.deinit(alloc);
                alloc.free(current);
            }
            var unchanged = current.len == configs.len;
            if (unchanged) for (current, configs) |actual, captured| {
                if (!actual.eql(captured)) {
                    unchanged = false;
                    break;
                }
            };
            if (!unchanged) return capi.mapErrorContext(error.Busy, "resolver configuration changed during resolution; retry");
        }
        var parsed_extraction = std.json.parseFromSlice(std.json.Value, alloc, extracted, .{}) catch |err| return capi.mapError(err);
        defer parsed_extraction.deinit();
        var parsed_resolution = std.json.parseFromSlice(std.json.Value, alloc, resolution, .{}) catch |err| return capi.mapError(err);
        defer parsed_resolution.deinit();
        out.* = stringifyJson(.{ .extraction = parsed_extraction.value, .resolution = parsed_resolution.value }) catch |err| return capi.mapError(err);
        return .ok;
    }
    return capi.mapErrorContext(error.NotFound, "resolver does not exist");
}

/// Persist a resolver-level redirect and enqueue corpus re-resolution atomically.
/// Call run_until_idle before expecting all existing mention edges to be re-keyed.
pub fn antfly_db_merge_resolver_keys_json(handle_ptr: ?*anyopaque, resolver_name: capi.Slice, merge_json: capi.Slice) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_merge_resolver_keys_json), .{ .name = "antfly_db_merge_resolver_keys_json" }), .{ handle_ptr, resolver_name, merge_json });
    capi.clearLastError();
    var out: capi.Buffer = .{};
    defer antfly_buffer_free(&out);
    return executeResolverCurationWithWait(handle_ptr, resolver_name.bytes(), merge_json.bytes(), "merge", &out);
}

fn copyJsonBuffer(bytes: []const u8) !capi.Buffer {
    const owned = try std.heap.c_allocator.dupe(u8, bytes);
    return .{ .ptr = owned.ptr, .len = owned.len };
}

fn executeResolverCuration(handle: *Handle, name: []const u8, body: []const u8, action: []const u8, out: ?*capi.Buffer) capi.ErrorCode {
    var parsed = std.json.parseFromSlice(std.json.Value, handle.alloc, body, .{}) catch |err| return capi.mapError(err);
    defer parsed.deinit();
    if (parsed.value != .object or parsed.value.object.contains("action")) return capi.mapError(error.InvalidArgument);
    const effective_action = if (std.mem.eql(u8, action, "suppressions") and !parsed.value.object.contains("add") and !parsed.value.object.contains("remove")) "suppress" else action;
    parsed.value.object.put(parsed.arena.allocator(), "action", .{ .string = effective_action }) catch |err| return capi.mapError(err);
    const request = std.json.Stringify.valueAlloc(handle.alloc, parsed.value, .{}) catch |err| return capi.mapError(err);
    defer handle.alloc.free(request);
    const result = handle.database().curateResolverJsonAlloc(handle.alloc, name, request) catch |err| return capi.mapErrorContext(err, "curating resolver identity");
    defer handle.alloc.free(result);
    if (out) |buffer| buffer.* = copyJsonBuffer(result) catch |err| return capi.mapError(err);
    return .ok;
}

/// Commit while holding writer admission, then observe detached replay while
/// releasing admission between polls. A timeout returns the accepted revision.
fn executeResolverCurationWithWait(handle_ptr: ?*anyopaque, name: []const u8, body: []const u8, action: []const u8, out: *capi.Buffer) capi.ErrorCode {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const input = std.json.parseFromSliceLeaky(std.json.Value, a, body, .{}) catch |err| return capi.mapError(err);
    const wait = if (input == .object) input.object.get("wait") else null;
    const timeout: u64 = if (wait) |w| if (w == .object) if (w.object.get("timeout_ms")) |v| if (v == .integer and v.integer > 0 and v.integer <= 300000) @intCast(v.integer) else return capi.mapError(error.InvalidArgument) else 1000 else return capi.mapError(error.InvalidArgument) else 0;
    {
        const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapError(error.InvalidArgument);
        defer guard.leave();
        if (guard.entry_error) |code| return capi.ensureError(code, @src().fn_name);
        const code = executeResolverCuration(guard.handle, name, body, action, out);
        if (code != .ok or timeout == 0) return code;
    }
    return waitResolverCurationResponse(handle_ptr, timeout, out);
}

/// Observe accepted policy/review revisions without retaining writer admission.
/// Each poll owns bounded scratch memory, even for a five-minute wait.
fn waitResolverCurationResponse(handle_ptr: ?*anyopaque, timeout_ms: u64, out: *capi.Buffer) capi.ErrorCode {
    if (timeout_ms == 0) return .ok;
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var result = std.json.parseFromSliceLeaky(std.json.Value, a, out.ptr.?[0..out.len], .{}) catch return .ok;
    const deadline = monotonicNowNs() +| timeout_ms * std.time.ns_per_ms;
    while (monotonicNowNs() < deadline) {
        var caught_up = true;
        {
            const guard = enterHandle(handle_ptr, .read) orelse return .ok;
            defer guard.leave();
            if (guard.entry_error != null) return .ok;
            var iteration = std.heap.ArenaAllocator.init(std.heap.c_allocator);
            defer iteration.deinit();
            const scratch = iteration.allocator();
            const root = guard.handle.parent_handle orelse guard.handle;
            var singleton = [_]std.json.Value{result};
            const rows = if (result.object.getPtr("results")) |items| if (items.* == .array) items.array.items else singleton[0..] else singleton[0..];
            for (rows) |*row| {
                if (row.* != .object) continue;
                if (row.object.get("accepted")) |accepted| if (accepted == .bool and !accepted.bool) {
                    caught_up = false;
                    continue;
                };
                if (row.object.get("applied")) |observed| if (observed == .bool and observed.bool) continue;
                const resolver_name = row.object.get("resolver") orelse continue;
                if (resolver_name != .string) continue;
                var database = guard.handle.database();
                if (row.object.get("source_table")) |source| if (source == .string) {
                    database = if (std.mem.eql(u8, source.string, "default")) &root.db else if (root.embedded_tables.get(source.string)) |table| &table.db else {
                        caught_up = false;
                        continue;
                    };
                };
                const identities = database.listResolverIdentities(scratch) catch {
                    caught_up = false;
                    continue;
                };
                var done = false;
                for (identities) |cfg| if (std.mem.eql(u8, cfg.name, resolver_name.string)) {
                    done = @import("../storage/db/resolver_curation.zig").applied(database, cfg) catch false;
                    if (row.object.get("revision")) |revision| if (revision == .integer and revision.integer >= 0) {
                        done = done and cfg.curation_revision >= @as(u64, @intCast(revision.integer));
                    };
                    if (row.object.get("replay_sequence_kind")) |kind| if (kind == .string and std.mem.eql(u8, kind.string, "source_journal")) {
                        if (row.object.get("replay_sequence")) |sequence| if (sequence == .integer and sequence.integer >= 0) {
                            const stats = database.pendingWorkStats();
                            const floor: u64 = @intCast(sequence.integer);
                            done = done and stats.resolution.applied_sequence >= floor and (!cfg.promote or stats.promotion.applied_sequence >= floor);
                        };
                    };
                    break;
                };
                caught_up = caught_up and done;
                if (!done) continue;
                row.object.put(a, "applied", .{ .bool = true }) catch return .ok;
                const progress = std.json.Stringify.valueAlloc(a, database.pendingWorkStats(), .{}) catch return .ok;
                row.object.put(a, "progress", std.json.parseFromSliceLeaky(std.json.Value, a, progress, .{}) catch return .ok) catch return .ok;
            }
            if (!result.object.contains("results")) result = singleton[0];
        }
        if (caught_up) {
            result.object.put(a, "applied", .{ .bool = true }) catch return .ok;
            const bytes = std.json.Stringify.valueAlloc(a, result, .{}) catch return .ok;
            antfly_buffer_free(out);
            out.* = copyJsonBuffer(bytes) catch |err| return capi.mapError(err);
            return .ok;
        }
        handleLockIo().sleep(.fromMilliseconds(25), .awake) catch return .ok;
    }
    return .ok;
}

pub fn antfly_db_unmerge_resolver_key_json(handle_ptr: ?*anyopaque, resolver_name: capi.Slice, request_json: capi.Slice, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_unmerge_resolver_key_json), .{ .name = "antfly_db_unmerge_resolver_key_json" }), .{ handle_ptr, resolver_name, request_json, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapError(error.InvalidArgument);
    return executeResolverCurationWithWait(handle_ptr, resolver_name.bytes(), request_json.bytes(), "unmerge", out);
}

pub fn antfly_db_suppress_resolver_keys_json(handle_ptr: ?*anyopaque, resolver_name: capi.Slice, request_json: capi.Slice, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_suppress_resolver_keys_json), .{ .name = "antfly_db_suppress_resolver_keys_json" }), .{ handle_ptr, resolver_name, request_json, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapError(error.InvalidArgument);
    return executeResolverCurationWithWait(handle_ptr, resolver_name.bytes(), request_json.bytes(), "suppressions", out);
}

pub fn antfly_db_unsuppress_resolver_key_json(handle_ptr: ?*anyopaque, resolver_name: capi.Slice, request_json: capi.Slice, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_unsuppress_resolver_key_json), .{ .name = "antfly_db_unsuppress_resolver_key_json" }), .{ handle_ptr, resolver_name, request_json, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapError(error.InvalidArgument);
    return executeResolverCurationWithWait(handle_ptr, resolver_name.bytes(), request_json.bytes(), "unsuppress", out);
}

pub fn antfly_db_resolver_curation_json(handle_ptr: ?*anyopaque, resolver_name: capi.Slice, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_resolver_curation_json), .{ .name = "antfly_db_resolver_curation_json" }), .{ handle_ptr, resolver_name, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapError(error.InvalidArgument);
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapError(error.InvalidArgument);
    defer guard.leave();
    if (guard.entry_error) |value| return capi.ensureError(value, @src().fn_name);
    const bytes = guard.handle.database().resolverCurationJsonAlloc(guard.handle.alloc, resolver_name.bytes()) catch |err| return capi.mapError(err);
    defer guard.handle.alloc.free(bytes);
    out.* = copyJsonBuffer(bytes) catch |err| return capi.mapError(err);
    return .ok;
}

/// Named entity operations share the same durable fanout and replay contract.
fn entityAction(handle: ?*anyopaque, table: capi.Slice, key: capi.Slice, request: capi.Slice, out: ?*capi.Buffer, action: []const u8) capi.ErrorCode {
    capi.clearLastError();
    _ = resetOutBuffer(out) orelse return capi.mapError(error.InvalidArgument);
    if (request.len > 4 * 1024 * 1024) return capi.mapError(error.ResolverCurationLimitExceeded);
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var value = std.json.parseFromSliceLeaky(std.json.Value, a, if (request.len == 0) "{}" else request.bytes(), .{}) catch |err| return capi.mapError(err);
    if (value != .object) return capi.mapError(error.InvalidResolverCurationRequest);
    if (value.object.get("action")) |existing| if (existing != .string or !std.mem.eql(u8, existing.string, action)) return capi.mapError(error.InvalidResolverCurationRequest);
    value.object.put(a, "action", .{ .string = action }) catch |err| return capi.mapError(err);
    const bytes = std.json.Stringify.valueAlloc(a, value, .{}) catch |err| return capi.mapError(err);
    return antfly_db_curate_entity_json(handle, table, key, .fromSlice(bytes), out);
}

pub fn antfly_db_entity_merge_json(handle: ?*anyopaque, table: capi.Slice, key: capi.Slice, request: capi.Slice, out: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    return entityAction(handle, table, key, request, out, "merge");
}

pub fn antfly_db_entity_unmerge_json(handle: ?*anyopaque, table: capi.Slice, key: capi.Slice, request: capi.Slice, out: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    return entityAction(handle, table, key, request, out, "unmerge");
}

pub fn antfly_db_entity_suppress_json(handle: ?*anyopaque, table: capi.Slice, key: capi.Slice, request: capi.Slice, out: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    return entityAction(handle, table, key, request, out, "suppress");
}

pub fn antfly_db_entity_unsuppress_json(handle: ?*anyopaque, table: capi.Slice, key: capi.Slice, request: capi.Slice, out: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    return entityAction(handle, table, key, request, out, "unsuppress");
}

pub fn antfly_db_entity_redirects_json(handle: ?*anyopaque, table: capi.Slice, key: capi.Slice, request: capi.Slice, out: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    return entityAction(handle, table, key, request, out, "redirects");
}

/// Fan out an entity correction across every resolver namespace targeting the
/// entity table. All plans are validated before the first resolver publication.
pub fn antfly_db_curate_entity_json(handle_ptr: ?*anyopaque, entity_table: capi.Slice, entity_key: capi.Slice, request_json: capi.Slice, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_curate_entity_json), .{ .name = "antfly_db_curate_entity_json" }), .{ handle_ptr, entity_table, entity_key, request_json, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapError(error.InvalidArgument);
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapError(error.InvalidArgument);
    var admitted = true;
    defer if (admitted) guard.leave();
    if (guard.entry_error) |value| return capi.ensureError(value, @src().fn_name);
    const handle = guard.handle.parent_handle orelse guard.handle;
    var arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const curation = @import("../storage/db/resolver_curation.zig");
    const pure = @import("antfly_resolver");
    const Input = struct { action: enum { redirects, merge, unmerge, suppress, unsuppress }, into: ?[]const u8 = null, replace: bool = false, pin_distinct: bool = false, wait: ?pure.curation.Wait = null };
    const input = std.json.parseFromSliceLeaky(Input, a, request_json.bytes(), .{}) catch |err| return capi.mapError(err);
    const tables = @import("tables.zig");
    tables.load(handle) catch |err| return capi.mapError(err);
    _ = tables.get(handle, entity_table.bytes()) catch |err| return capi.mapError(err);
    const Pending = struct { db: *db_mod.DB, source: []const u8, cfg: db_mod.ResolverConfig, request: pure.curation.Request };
    var pending: std.ArrayListUnmanaged(Pending) = .empty;
    var sources: std.ArrayListUnmanaged(struct { db: *db_mod.DB, name: []const u8 }) = .empty;
    sources.append(a, .{ .db = &handle.db, .name = "default" }) catch |err| return capi.mapError(err);
    var it = handle.embedded_tables.iterator();
    while (it.next()) |entry| sources.append(a, .{ .db = &entry.value_ptr.*.db, .name = entry.key_ptr.* }) catch |err| return capi.mapError(err);
    var results = std.json.Array.init(a);
    for (sources.items) |source| {
        const configs = source.db.core.index_manager.listResolverIdentities(a) catch |err| return capi.mapError(err);
        for (configs) |cfg| {
            if (!std.mem.eql(u8, cfg.table, entity_table.bytes()) or !curation.keyInNamespace(cfg.key_template, entity_key.bytes())) continue;
            if (input.into) |target| if (!curation.keyInNamespace(cfg.key_template, target)) return capi.mapError(error.InvalidResolverCurationRequest);
            if (input.action == .redirects) {
                const state = pure.curation.parseState(a, cfg.curation_json) catch |err| return capi.mapError(err);
                const aliases = cfg.effectiveAliasesAlloc(a) catch |err| return capi.mapError(err);
                const scoped = pure.curation.entityState(a, state, aliases, entity_key.bytes()) catch |err| return capi.mapError(err);
                const raw = std.json.Stringify.valueAlloc(a, .{ .source_table = source.name, .resolver = cfg.name, .revision = cfg.curation_revision, .redirects = scoped.redirects, .canonical_key = scoped.canonical_key, .curation = scoped.curation }, .{}) catch |err| return capi.mapError(err);
                results.append(std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{}) catch |err| return capi.mapError(err)) catch |err| return capi.mapError(err);
                continue;
            }
            const action: pure.curation.Action = switch (input.action) {
                .merge => .merge,
                .unmerge => .unmerge,
                .suppress => .suppress,
                .unsuppress => .unsuppress,
                .redirects => unreachable,
            };
            const intent: pure.curation.Request = .{ .action = action, .key = entity_key.bytes(), .to = input.into, .replace = input.replace, .pin_distinct = input.pin_distinct, .wait = input.wait, .expected_revision = cfg.curation_revision };
            const edit = pure.curation.plan(a, cfg.curation_json, cfg.merges, cfg.effectiveAliasesAlloc(a) catch |err| return capi.mapError(err), intent) catch |err| return capi.mapError(err);
            var next = cfg;
            next.merges = edit.merges;
            next.curation_json = std.json.Stringify.valueAlloc(a, edit.state, .{}) catch |err| return capi.mapError(err);
            next.validate() catch |err| return capi.mapError(err);
            pending.append(a, .{ .db = source.db, .source = source.name, .cfg = cfg, .request = intent }) catch |err| return capi.mapError(err);
        }
    }
    if (input.action != .redirects and pending.items.len == 0) return capi.mapError(error.ResolverNotFound);
    var complete = true;
    for (pending.items) |item| {
        const bytes = std.json.Stringify.valueAlloc(a, item.request, .{}) catch |err| return capi.mapError(err);
        const raw = curation.mutate(item.db, a, item.cfg.name, bytes) catch |err| {
            complete = false;
            const failed = std.json.Stringify.valueAlloc(a, .{ .source_table = item.source, .resolver = item.cfg.name, .accepted = false, .error_message = @errorName(err) }, .{}) catch |failure| return capi.mapError(failure);
            results.append(std.json.parseFromSliceLeaky(std.json.Value, a, failed, .{}) catch |failure| return capi.mapError(failure)) catch |failure| return capi.mapError(failure);
            continue;
        };
        var value = std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{}) catch |err| return capi.mapError(err);
        value.object.put(a, "source_table", .{ .string = item.source }) catch |err| return capi.mapError(err);
        results.append(value) catch |err| return capi.mapError(err);
    }
    const response = std.json.Stringify.valueAlloc(a, .{ .entity_table = entity_table.bytes(), .key = entity_key.bytes(), .atomicity = "resolver", .complete = complete, .results = std.json.Value{ .array = results } }, .{}) catch |err| return capi.mapError(err);
    out.* = copyJsonBuffer(response) catch |err| return capi.mapError(err);
    admitted = false;
    guard.leave();
    return waitResolverCurationResponse(handle_ptr, if (input.wait) |wait| wait.timeout_ms else 0, out);
}

/// Bounded review page or revision-bound review decision; operation is part
/// of the typed request and shares the serving owner's curation implementation.
pub fn antfly_db_resolver_reviews_json(handle_ptr: ?*anyopaque, request_json: capi.Slice, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_resolver_reviews_json), .{ .name = "antfly_db_resolver_reviews_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapError(error.InvalidArgument);
    if (request_json.len > 4 * 1024 * 1024) return capi.mapError(error.ResolverCurationLimitExceeded);
    const input = std.json.parseFromSlice(@import("../storage/db/resolver_curation.zig").ReviewRequest, std.heap.c_allocator, request_json.bytes(), .{}) catch |err| return capi.mapError(err);
    defer input.deinit();
    const guard = enterHandle(handle_ptr, if (input.value.operation == .review) .exclusive else .read) orelse return capi.mapError(error.InvalidArgument);
    var admitted = true;
    defer if (admitted) guard.leave();
    if (guard.entry_error) |value| return capi.ensureError(value, @src().fn_name);
    const bytes = guard.handle.database().resolverReviewsJsonAlloc(guard.handle.alloc, request_json.bytes()) catch |err| return capi.mapError(err);
    defer guard.handle.alloc.free(bytes);
    out.* = copyJsonBuffer(bytes) catch |err| return capi.mapError(err);
    if (input.value.operation != .review) return .ok;
    admitted = false;
    guard.leave();
    return waitResolverCurationResponse(handle_ptr, if (input.value.wait) |wait| wait.timeout_ms else 0, out);
}

pub fn antfly_db_list_resolver_identities_json(handle_ptr: ?*anyopaque, out_buf: ?*capi.Buffer) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_list_resolver_identities_json), .{ .name = "antfly_db_list_resolver_identities_json" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const out = resetOutBuffer(out_buf) orelse return capi.mapError(error.InvalidArgument);
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapError(error.InvalidArgument);
    defer guard.leave();
    if (guard.entry_error) |code| return capi.ensureError(code, @src().fn_name);
    const handle = guard.handle;
    const identities = handle.database().core.index_manager.listResolverIdentities(handle.alloc) catch |err| return capi.mapError(err);
    defer {
        for (identities) |*cfg| cfg.deinit(handle.alloc);
        handle.alloc.free(identities);
    }
    // Identity management includes the durable redirects omitted from config JSON.
    const Identity = struct { config: std.json.Value, merges: []const @import("antfly_resolver").KeyAlias };
    const result = handle.alloc.alloc(Identity, identities.len) catch |err| return capi.mapError(err);
    defer handle.alloc.free(result);
    var public_arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer public_arena.deinit();
    for (identities, result) |cfg, *identity| identity.* = .{ .config = cfg.publicValue(public_arena.allocator()) catch |err| return capi.mapError(err), .merges = cfg.merges };
    out.* = stringifyJson(result) catch |err| return capi.mapError(err);
    return .ok;
}

/// Explicit irreversible identity deletion is allowed only after retirement.
pub fn antfly_db_drop_resolver_identity(handle_ptr: ?*anyopaque, name: capi.Slice, out_deleted: ?*bool) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_drop_resolver_identity), .{ .name = "antfly_db_drop_resolver_identity" }), .{ handle_ptr, name, out_deleted });
    capi.clearLastError();
    const out = out_deleted orelse return capi.mapError(error.InvalidArgument);
    out.* = false;
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapError(error.InvalidArgument);
    defer guard.leave();
    if (guard.entry_error) |code| return capi.ensureError(code, @src().fn_name);
    out.* = guard.handle.database().dropRetiredResolverIdentity(name.bytes()) catch |err| return capi.mapErrorContext(err, "retire all declaring indexes before dropping resolver identity");
    return .ok;
}

/// Explicitly reset redirects or map every stored alias endpoint into a new namespace.
/// The resolver, identity state and replay cursors commit together.
pub fn antfly_db_set_resolver_namespace_json(handle_ptr: ?*anyopaque, resolver_name: capi.Slice, request_json: capi.Slice) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_set_resolver_namespace_json), .{ .name = "antfly_db_set_resolver_namespace_json" }), .{ handle_ptr, resolver_name, request_json });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, "invalid or closed database handle");
    defer guard.leave();
    if (guard.entry_error) |code| return capi.ensureError(code, @src().fn_name);
    const handle = guard.handle;
    var arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena.deinit();
    const curation = @import("../storage/db/resolver_curation.zig");
    const request = std.json.parseFromSliceLeaky(curation.NamespaceRequest, arena.allocator(), request_json.bytes(), .{}) catch |err| return capi.mapErrorContext(err, "invalid resolver namespace request");
    if (request.key_map != null and request.aliases != null) return capi.mapErrorContext(error.InvalidArgument, "explicit aliases require reset_merges:true");
    if (request.reset_merges == (request.key_map != null)) return capi.mapErrorContext(error.InvalidArgument, "choose reset_merges:true or an explicit key_map, exclusively");
    const configs = handle.database().core.index_manager.listResolverIdentities(handle.alloc) catch |err| return capi.mapError(err);
    defer {
        for (configs) |*cfg| cfg.deinit(handle.alloc);
        handle.alloc.free(configs);
    }
    for (configs) |cfg| {
        if (!std.mem.eql(u8, cfg.name, resolver_name.bytes())) continue;
        // Validate the destination before publishing identity state.
        _ = @import("tables.zig").get(handle.parent_handle orelse handle, request.table) catch |err| return capi.mapErrorContext(if (err == error.UndefinedTable) error.NotFound else err, "resolver namespace destination table does not exist");
        const next = curation.namespaceConfig(arena.allocator(), cfg, request) catch |err| return capi.mapErrorContext(err, "mapping every inline alias endpoint and resolver policy key into the destination namespace");
        _ = handle.database().upsertResolverWithResultOptions(next, .{ .drain_backfill = false, .wait_for_activity = true, .migrate_namespace = true }) catch |err| return capi.mapErrorContext(err, "updating resolver namespace atomically");
        return .ok;
    }
    return capi.mapErrorContext(error.NotFound, "resolver does not exist");
}

var test_index_before_wait_quantum: ?*const fn (?*anyopaque) anyerror!void = null;

/// Registration captures the admitted generation atomically. Optional waiting
/// retains it while releasing API/path admission between maintenance quanta.
pub fn antfly_db_add_index_json(handle_ptr: ?*anyopaque, config_json: capi.Slice) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_add_index_json), .{ .name = "antfly_db_add_index_json" }), .{ handle_ptr, config_json });
    capi.clearLastError();
    var request = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, config_json.bytes(), .{}) catch |err| return capi.mapError(err);
    defer request.deinit();
    var generation: u64 = 0;
    const result = addIndexJsonRegistered(handle_ptr, config_json, &generation);
    if (result != .ok) return result;
    if (request.value != .object) return .ok;
    const wait = request.value.object.get("wait") orelse return .ok;
    const timeout = wait.object.get("timeout_ms").?.integer;
    const name = request.value.object.get("name").?.string;
    const deadline = monotonicNowNs() +| @as(u64, @intCast(timeout)) *| std.time.ns_per_ms;
    while (true) {
        if (builtin.is_test) if (test_index_before_wait_quantum) |hook|
            hook(handle_ptr) catch |err| return capi.mapError(err);
        var background_owner = false;
        const pending = blk: {
            const guard = enterHandle(handle_ptr, .maintain) orelse return capi.mapError(error.InvalidArgument);
            defer guard.leave();
            if (guard.entry_error) |code| return capi.ensureError(code, @src().fn_name);
            const database = guard.handle.database();
            const context = guard.handle.lite_connection_context orelse if (guard.handle.parent_handle) |parent| parent.lite_connection_context else null;
            background_owner = if (context) |connection| connection.owner.worker != null else false;
            const now = monotonicNowNs();
            // Native Lite already owns a detached builder. Foreground build
            // quanta compete with its larger pages and repeatedly invalidate
            // its captured inputs. Observe that owner's generation instead;
            // manual/hosted and directory runtimes retain their inline driver.
            const quantum_ms = if (background_owner) 0 else @min(@as(u64, 100), (deadline -| now +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms);
            database.waitGraphReplacement(name, generation, quantum_ms) catch |err| switch (err) {
                error.Busy => break :blk true,
                else => return capi.mapError(err),
            };
            break :blk false;
        };
        if (!pending) return .ok;
        const now = monotonicNowNs();
        if (now >= deadline) return capi.mapError(error.Busy);
        const delay_ns = @min(deadline - now, (if (background_owner) @as(u64, 25) else 1) * std.time.ns_per_ms);
        handleLockIo().sleep(.fromNanoseconds(@intCast(delay_ns)), .awake) catch |err| return capi.mapError(err);
    }
}

fn addIndexJsonRegistered(
    handle_ptr: ?*anyopaque,
    config_json: capi.Slice,
    generation: *u64,
) capi.ErrorCode {
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    var arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const value = std.json.parseFromSliceLeaky(std.json.Value, a, config_json.bytes(), .{}) catch |err| return capi.mapError(err);
    if (value != .object) return capi.mapError(error.InvalidIndexRequest);
    const name = value.object.get("name") orelse return capi.mapError(error.MissingIndexName);
    if (name != .string or name.string.len == 0) return capi.mapError(error.InvalidIndexName);
    const discriminator = value.object.get("kind") orelse value.object.get("type") orelse return capi.mapError(error.MissingIndexType);
    if (discriminator != .string) return capi.mapError(error.InvalidIndexType);
    const replacement = value.object.get("replaces");
    const wait = value.object.get("wait");
    if (wait) |request| {
        if (request != .object) return capi.mapError(error.InvalidArgument);
        const timeout = request.object.get("timeout_ms") orelse return capi.mapError(error.InvalidArgument);
        if (timeout != .integer or timeout.integer < 0) return capi.mapError(error.InvalidArgument);
    }
    var index_value = value;
    _ = index_value.object.swapRemove("wait");
    _ = index_value.object.swapRemove("replaces");
    const config_value = index_value.object.get("config_json");
    const normalized_config = if (config_value) |config| switch (config) {
        .string => config.string,
        .object => std.json.Stringify.valueAlloc(a, config, .{}) catch |err| return capi.mapError(err),
        else => return capi.mapError(error.InvalidIndexConfig),
    } else std.json.Stringify.valueAlloc(a, index_value, .{}) catch |err| return capi.mapError(err);
    const kind: db_mod.types.IndexKind = if (std.mem.eql(u8, discriminator.string, "embeddings")) blk: {
        const config = std.json.parseFromSliceLeaky(std.json.Value, a, normalized_config, .{}) catch |err| return capi.mapError(err);
        if (config != .object) return capi.mapError(error.InvalidIndexConfig);
        if (config.object.get("sparse")) |sparse| {
            if (sparse != .bool) return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": sparse must be a JSON boolean");
            if (sparse.bool) break :blk .sparse_vector;
        }
        break :blk .dense_vector;
    } else std.meta.stringToEnum(db_mod.types.IndexKind, discriminator.string) orelse return capi.mapError(error.InvalidIndexType);
    // Validate command targets before registering any inline producers/resolvers.
    if (replacement) |old| {
        if (old != .string or old.string.len == 0 or std.mem.eql(u8, old.string, name.string))
            return capi.mapErrorContext(error.InvalidArgument, "replaces must name a different graph index");
        if (kind != .graph) return capi.mapErrorContext(error.InvalidArgument, "atomic replacement requires graph indexes");
        if (handle.database().core.index_manager.get(old.string)) |previous| {
            if (previous.kind != .graph) return capi.mapErrorContext(error.InvalidArgument, "atomic replacement requires graph indexes");
        }
    }
    const parsed = .{ .value = .{ .name = name.string, .config_json = normalized_config } };
    // A native Lite handle also accepts the server's nested "enrichments"
    // shape on this index's own config (see `registerLiteIndexEnrichments`),
    // registering every declared producer before the index below is admitted
    // so an artifact-sourced `sources`/`embedding_name` reference already
    // resolves. The server's atomic table-create request has no such
    // pre-admission catalog mutation, so this call carries its own undo: a
    // rejected admission (IndexAlreadyExists, invalid config) or a partial
    // enrichment/resolver registration restores the pre-call enrichment
    // catalog instead of leaving durably changed producers behind a caller
    // who was told the AddIndex failed.
    {
        const jobs = handle.database().graphReplacementStatusAlloc(handle.alloc) catch |err| return capi.mapError(err);
        defer jobs.deinit();
        for (jobs.value) |job| {
            if (!std.mem.eql(u8, job.name, parsed.value.name)) continue;
            const cfg = job.config orelse return capi.mapError(error.InvalidIndexConfig);
            const same = cfg.kind == kind and (@import("../storage/db/enrichment/config_validation.zig").producerJsonValuesEqual(handle.alloc, cfg.config_json, parsed.value.config_json) catch |err| return capi.mapError(err));
            if (!same or (replacement != null and !std.mem.eql(u8, replacement.?.string, job.old)))
                return capi.mapErrorContext(error.IndexReplacementConfigurationMismatch, "index name is reserved by a different pending replacement; cancel_replacement before changing it");
            if (job.phase == .failed)
                return capi.mapErrorContext(error.IndexReplacementFailed, "replacement failed; inspect pending_work_stats and cancel_replacement before retrying");
            generation.* = job.generation;
            return .ok;
        }
    }
    var rollback: ?LiteCatalogRollback = null;
    defer if (rollback) |*undo| undo.deinit();
    {
        rollback = LiteCatalogRollback.init(handle) catch |err| return capi.mapError(err);
        if (handle.lite_profile == .native) registerLiteIndexEnrichments(handle, parsed.value.config_json, &rollback.?) catch |err| {
            rollback.?.restore();
            return capi.mapError(err);
        };
    }
    // Only a native Lite handle calls `db.addIndex` directly with a raw
    // public-shaped dense/sparse config; the server always translates first
    // (see `litePhysicalIndexConfigJson`). Every other handle keeps calling
    // `db.addIndex` with exactly the config it was given, unchanged.
    const stored_config_json = if (handle.lite_profile == .native)
        litePhysicalIndexConfigJson(handle.alloc, kind, parsed.value.name, parsed.value.config_json, handle.liteAntflyProvider()) catch |err| {
            if (rollback) |*undo| undo.restore();
            return capi.mapError(err);
        }
    else
        parsed.value.config_json;
    defer if (handle.lite_profile == .native) handle.alloc.free(@constCast(stored_config_json));
    // Embedded clients replay inline configs on every open. Admit an identical
    // existing index idempotently, while retaining duplicate-config conflicts.
    var existing_index = false;
    {
        const installed = handle.database().listIndexes(handle.alloc) catch |err| {
            if (rollback) |*undo| undo.restore();
            return capi.mapError(err);
        };
        defer db_mod.types.freeIndexConfigs(handle.alloc, installed);
        for (installed) |cfg| {
            if (!std.mem.eql(u8, cfg.name, parsed.value.name)) continue;
            const same = cfg.kind == kind and (@import("../storage/db/enrichment/config_validation.zig").producerJsonValuesEqual(handle.alloc, cfg.config_json, stored_config_json) catch |err| {
                if (rollback) |*undo| undo.restore();
                return capi.mapError(err);
            });
            if (!same) {
                if (rollback) |*undo| undo.restore();
                return capi.mapErrorContext(error.InvalidArgument, "index already exists with a different configuration");
            }
            existing_index = true;
            generation.* = cfg.coverage_generation;
        }
    }
    // Reactivate inline identities before graph construction reads retained
    // resolution artifacts. Otherwise a new generation can be built while its
    // resolver is retired, then skip already-current resolutions on replay.
    // The undo buffer restores both active and retired state on failed admission.
    const shadow_replacement = !existing_index and replacement != null and handle.database().core.index_manager.get(replacement.?.string) != null;
    if (kind == .graph and !shadow_replacement) {
        registerLiteIndexResolvers(handle, parsed.value.config_json, &rollback.?) catch |err| {
            rollback.?.restore();
            if (err == error.ResolverMergeNamespaceChange)
                return capi.mapErrorContext(err, "changing a resolver table, key_template, or fenced inline aliases requires explicitly resetting or migrating its durable namespace");
            return capi.mapError(err);
        };
    }
    const index_cfg: db_mod.types.IndexConfig = .{ .name = parsed.value.name, .kind = kind, .config_json = stored_config_json };
    if (!existing_index) {
        if (replacement != null and handle.database().core.index_manager.get(replacement.?.string) != null) {
            generation.* = handle.database().replaceGraphIndex(replacement.?.string, index_cfg) catch |err| {
                if (rollback) |*undo| undo.restore();
                return capi.mapError(err);
            };
        } else {
            handle.database().addIndex(index_cfg) catch |err| {
                if (rollback) |*undo| undo.restore();
                return capi.mapError(err);
            };
            generation.* = handle.database().indexGeneration(index_cfg.name) orelse return capi.mapError(error.Cancelled);
        }
    }
    if (existing_index and replacement != null) {
        if (handle.database().core.index_manager.get(replacement.?.string) != null) {
            _ = handle.database().deleteIndex(replacement.?.string) catch |err| {
                // A pre-publication failure must undo inline config changes.
                // After publication the index is gone and its new identity state
                // must remain consistent with the committed catalog.
                if (handle.database().core.index_manager.get(replacement.?.string) != null) rollback.?.restore();
                return capi.mapError(err);
            };
        }
    }
    refreshLiteManagedEmbeddingRuntime(handle) catch |err| return capi.mapError(err);
    @import("tables.zig").noteReaderVersion(handle) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_cancel_replacement(
    handle_ptr: ?*anyopaque,
    name: capi.Slice,
    out_deleted: ?*bool,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_cancel_replacement), .{ .name = "antfly_db_cancel_replacement" }), .{ handle_ptr, name, out_deleted });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const out = out_deleted orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output pointer must not be null");
    out.* = false;
    const handle = guard.handle;
    out.* = handle.database().*.cancelReplacement(name.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_delete_index(
    handle_ptr: ?*anyopaque,
    name: capi.Slice,
    out_deleted: ?*bool,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_delete_index), .{ .name = "antfly_db_delete_index" }), .{ handle_ptr, name, out_deleted });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const out = out_deleted orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output pointer must not be null");
    out.* = false;
    const handle = guard.handle;
    out.* = handle.database().*.deleteIndex(name.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_add_enrichment_json(
    handle_ptr: ?*anyopaque,
    config_json: capi.Slice,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_add_enrichment_json), .{ .name = "antfly_db_add_enrichment_json" }), .{ handle_ptr, config_json });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    var parsed = std.json.parseFromSlice(db_mod.types.EnrichmentConfig, handle.alloc, config_json.bytes(), .{
        .ignore_unknown_fields = true,
    }) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer parsed.deinit();
    handle.database().*.addEnrichment(parsed.value) catch |err| return capi.mapError(err);
    refreshLiteManagedEmbeddingRuntime(handle) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_delete_enrichment(
    handle_ptr: ?*anyopaque,
    kind_slice: capi.Slice,
    name: capi.Slice,
    out_deleted: ?*bool,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_delete_enrichment), .{ .name = "antfly_db_delete_enrichment" }), .{ handle_ptr, kind_slice, name, out_deleted });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const out = out_deleted orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": output pointer must not be null");
    out.* = false;
    const handle = guard.handle;
    const kind = parseEnrichmentKind(kind_slice.bytes()) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": unknown enrichment kind");
    out.* = handle.database().*.deleteEnrichment(kind, name.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_get_edges_json(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    key: capi.Slice,
    edge_type: capi.Slice,
    direction: u8,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_get_edges_json), .{ .name = "antfly_db_get_edges_json" }), .{ handle_ptr, index_name, key, edge_type, direction, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const dir: db_mod.types.GraphEdgeDirection = switch (direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": graph direction must be 0 (out), 1 (in), or 2 (both)"),
    };
    const edges = handle.database().*.getEdges(handle.alloc, index_name.bytes(), key.bytes(), edge_type.bytes(), dir) catch |err| return capi.mapError(err);
    defer graphFreeEdges(handle.alloc, edges);
    var payload = handle.alloc.alloc(JsonEdge, edges.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (edges, 0..) |edge, i| {
        payload[i] = JsonEdge.init(handle.alloc, edge) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_traverse_edges_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_traverse_edges_json), .{ .name = "antfly_db_traverse_edges_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        index_name: []const u8,
        start_key_b64: []const u8,
        edge_types: []const []const u8 = &.{},
        edge_filter: ?std.json.Value = null,
        direction: u8 = 0,
        max_depth: u32 = 3,
        min_weight: f64 = 0.0,
        max_weight: f64 = 0.0,
        max_results: u32 = 100,
        deduplicate_nodes: bool = true,
        include_paths: bool = false,
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{ .parse_numbers = false }) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer parsed.deinit();
    const edge_filter = parseEmbeddedRelationshipFilter(handle.alloc, parsed.value.edge_filter) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer edge_filter.deinit(handle.alloc);
    const start_key = decodeBase64Alloc(handle.alloc, parsed.value.start_key_b64) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(start_key);
    const direction: db_mod.types.GraphEdgeDirection = switch (parsed.value.direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": graph direction must be 0 (out), 1 (in), or 2 (both)"),
    };
    const results = handle.database().*.traverseEdges(handle.alloc, parsed.value.index_name, start_key, .{
        .edge_types = parsed.value.edge_types,
        .edge_filter = edge_filter,
        .direction = direction,
        .max_depth = parsed.value.max_depth,
        .min_weight = legacyGraphWeightBound(parsed.value.min_weight),
        .max_weight = legacyGraphWeightBound(parsed.value.max_weight),
        .max_results = parsed.value.max_results,
        .deduplicate = parsed.value.deduplicate_nodes,
        .include_paths = parsed.value.include_paths,
    }) catch |err| return capi.mapError(err);
    defer traversalFreeResults(handle.alloc, results);
    var payload = handle.alloc.alloc(JsonTraversalResult, results.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (results, 0..) |item, i| {
        payload[i] = JsonTraversalResult.init(handle.alloc, item) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_get_neighbors_json(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    key: capi.Slice,
    edge_type: capi.Slice,
    direction: u8,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_get_neighbors_json), .{ .name = "antfly_db_get_neighbors_json" }), .{ handle_ptr, index_name, key, edge_type, direction, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const dir: db_mod.types.GraphEdgeDirection = switch (direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": graph direction must be 0 (out), 1 (in), or 2 (both)"),
    };
    const results = handle.database().*.getNeighbors(handle.alloc, index_name.bytes(), key.bytes(), edge_type.bytes(), dir) catch |err| return capi.mapError(err);
    defer traversalFreeResults(handle.alloc, results);
    var payload = handle.alloc.alloc(JsonTraversalResult, results.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (results, 0..) |item, i| {
        payload[i] = JsonTraversalResult.init(handle.alloc, item) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_find_shortest_path_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_find_shortest_path_json), .{ .name = "antfly_db_find_shortest_path_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        index_name: []const u8,
        source_b64: []const u8,
        target_b64: []const u8,
        edge_types: []const []const u8 = &.{},
        edge_filter: ?std.json.Value = null,
        direction: u8 = 0,
        weight_mode: []const u8 = "min_hops",
        max_depth: u32 = 50,
        min_weight: f64 = 0.0,
        max_weight: f64 = 0.0,
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{ .parse_numbers = false }) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer parsed.deinit();
    const edge_filter = parseEmbeddedRelationshipFilter(handle.alloc, parsed.value.edge_filter) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer edge_filter.deinit(handle.alloc);
    const source = decodeBase64Alloc(handle.alloc, parsed.value.source_b64) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(source);
    const target = decodeBase64Alloc(handle.alloc, parsed.value.target_b64) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(target);
    const direction: db_mod.types.GraphEdgeDirection = switch (parsed.value.direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": graph direction must be 0 (out), 1 (in), or 2 (both)"),
    };
    const weight_mode: db_mod.types.GraphPathWeightMode = if (std.mem.eql(u8, parsed.value.weight_mode, "min_weight"))
        .min_weight
    else if (std.mem.eql(u8, parsed.value.weight_mode, "max_weight"))
        .max_weight
    else
        .min_hops;
    const maybe_path = handle.database().*.findShortestPathWithOptions(handle.alloc, parsed.value.index_name, source, target, .{
        .edge_types = parsed.value.edge_types,
        .edge_filter = edge_filter,
        .direction = direction,
        .weight_mode = weight_mode,
        .max_depth = parsed.value.max_depth,
        .min_weight = legacyGraphWeightBound(parsed.value.min_weight),
        .max_weight = legacyGraphWeightBound(parsed.value.max_weight),
    }) catch |err| return capi.mapError(err);
    if (maybe_path == null) return capi.mapError(error.NotFound);
    defer paths_mod.freePath(handle.alloc, maybe_path.?);
    var payload = JsonPath.init(handle.alloc, maybe_path.?) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer payload.deinit(handle.alloc);
    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_find_k_shortest_paths_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_find_k_shortest_paths_json), .{ .name = "antfly_db_find_k_shortest_paths_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const Request = struct {
        index_name: []const u8,
        source_b64: []const u8,
        target_b64: []const u8,
        edge_types: []const []const u8 = &.{},
        edge_filter: ?std.json.Value = null,
        direction: u8 = 0,
        weight_mode: []const u8 = "min_hops",
        max_depth: u32 = 50,
        min_weight: f64 = 0.0,
        max_weight: f64 = 0.0,
        k: u32 = 1,
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{ .parse_numbers = false }) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer parsed.deinit();
    const edge_filter = parseEmbeddedRelationshipFilter(handle.alloc, parsed.value.edge_filter) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer edge_filter.deinit(handle.alloc);
    const source = decodeBase64Alloc(handle.alloc, parsed.value.source_b64) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(source);
    const target = decodeBase64Alloc(handle.alloc, parsed.value.target_b64) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(target);
    const direction: db_mod.types.GraphEdgeDirection = switch (parsed.value.direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": graph direction must be 0 (out), 1 (in), or 2 (both)"),
    };
    const weight_mode: db_mod.types.GraphPathWeightMode = if (std.mem.eql(u8, parsed.value.weight_mode, "min_weight"))
        .min_weight
    else if (std.mem.eql(u8, parsed.value.weight_mode, "max_weight"))
        .max_weight
    else
        .min_hops;
    const paths = handle.database().*.findKShortestPathsWithOptions(handle.alloc, parsed.value.index_name, source, target, parsed.value.k, .{
        .edge_types = parsed.value.edge_types,
        .edge_filter = edge_filter,
        .direction = direction,
        .weight_mode = weight_mode,
        .max_depth = parsed.value.max_depth,
        .min_weight = legacyGraphWeightBound(parsed.value.min_weight),
        .max_weight = legacyGraphWeightBound(parsed.value.max_weight),
    }) catch |err| return capi.mapError(err);
    defer paths_mod.freePaths(handle.alloc, paths);
    var payload = handle.alloc.alloc(JsonPath, paths.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (paths, 0..) |path, i| {
        payload[i] = JsonPath.init(handle.alloc, path) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_match_pattern_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_match_pattern_json), .{ .name = "antfly_db_match_pattern_json" }), .{ handle_ptr, request_json, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const JsonPatternNodeFilter = struct {
        filter_prefix: []const u8 = "",
        query_json: []const u8 = "",
    };
    const JsonPatternEdgeStep = struct {
        direction: u8 = 0,
        min_hops: u32 = 1,
        max_hops: u32 = 1,
        min_weight: f64 = 0.0,
        max_weight: f64 = 0.0,
        types: []const []const u8 = &.{},
    };
    const JsonPatternStep = struct {
        alias: []const u8 = "",
        edge: JsonPatternEdgeStep = .{},
        node_filter: JsonPatternNodeFilter = .{},
    };
    const Request = struct {
        index_name: []const u8,
        start_nodes_b64: []const []const u8,
        pattern: []const JsonPatternStep,
        max_results: u32 = 100,
        return_aliases: []const []const u8 = &.{},
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer parsed.deinit();

    var start_nodes = handle.alloc.alloc([]const u8, parsed.value.start_nodes_b64.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer {
        for (start_nodes) |entry| handle.alloc.free(entry);
        if (start_nodes.len > 0) handle.alloc.free(start_nodes);
    }
    var start_count: usize = 0;
    errdefer {
        for (start_nodes[0..start_count]) |entry| handle.alloc.free(entry);
    }
    for (parsed.value.start_nodes_b64, 0..) |item, i| {
        start_nodes[i] = decodeBase64Alloc(handle.alloc, item) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        start_count += 1;
    }

    var pattern = handle.alloc.alloc(graph_pattern_mod.PatternStep, parsed.value.pattern.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(pattern);
    for (parsed.value.pattern, 0..) |step, i| {
        const direction: db_mod.types.GraphEdgeDirection = switch (step.edge.direction) {
            0 => .out,
            1 => .in,
            2 => .both,
            else => return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": graph direction must be 0 (out), 1 (in), or 2 (both)"),
        };
        pattern[i] = .{
            .alias = step.alias,
            .edge = .{
                .direction = direction,
                .min_hops = step.edge.min_hops,
                .max_hops = step.edge.max_hops,
                .min_weight = legacyGraphWeightBound(step.edge.min_weight),
                .max_weight = legacyGraphWeightBound(step.edge.max_weight),
                .types = step.edge.types,
            },
            .node_filter = .{
                .filter_prefix = step.node_filter.filter_prefix,
                .filter_query_json = if (step.node_filter.query_json.len == 0) null else step.node_filter.query_json,
            },
        };
    }

    const matches = handle.database().*.matchPattern(handle.alloc, parsed.value.index_name, start_nodes, pattern, parsed.value.max_results, parsed.value.return_aliases) catch |err| return capi.mapError(err);
    defer graph_pattern_mod.freeMatches(handle.alloc, matches);

    var payload = handle.alloc.alloc(JsonPatternMatch, matches.len) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (matches, 0..) |match, i| {
        payload[i] = JsonPatternMatch.init(handle.alloc, match) catch |err| return capi.mapErrorContext(err, @src().fn_name);
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_create_shadow_index_manager(
    handle_ptr: ?*anyopaque,
    split_key: capi.Slice,
    original_range_end: capi.Slice,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_create_shadow_index_manager), .{ .name = "antfly_db_create_shadow_index_manager" }), .{ handle_ptr, split_key, original_range_end });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    handle.database().*.createShadowIndexManager(split_key.bytes(), original_range_end.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_close_shadow_index_manager(handle_ptr: ?*anyopaque) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_close_shadow_index_manager), .{ .name = "antfly_db_close_shadow_index_manager" }), .{handle_ptr});
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    handle.database().*.closeShadowIndexManager() catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_get_shadow_index_dir(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_get_shadow_index_dir), .{ .name = "antfly_db_get_shadow_index_dir" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const dir = handle.database().*.getShadowIndexDir();
    if (dir.len == 0) return capi.mapError(error.NotFound);
    out_buf.* = dupBytes(dir) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_find_median_key(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_find_median_key), .{ .name = "antfly_db_find_median_key" }), .{ handle_ptr, out_buf });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .read) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const key = handle.database().*.findMedianKey(handle.alloc) catch |err| return capi.mapError(err);
    defer handle.alloc.free(key);
    out_buf.* = dupBytes(key) catch |err| return capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_split(
    handle_ptr: ?*anyopaque,
    curr_start: capi.Slice,
    curr_end: capi.Slice,
    split_key: capi.Slice,
    dest_dir1: capi.Slice,
    dest_dir2: capi.Slice,
    prepare_only: bool,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_split), .{ .name = "antfly_db_split" }), .{ handle_ptr, curr_start, curr_end, split_key, dest_dir1, dest_dir2, prepare_only });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    handle.database().*.split(
        .{
            .start = curr_start.bytes(),
            .end = curr_end.bytes(),
        },
        split_key.bytes(),
        dest_dir1.bytes(),
        dest_dir2.bytes(),
        prepare_only,
    ) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_finalize_split(
    handle_ptr: ?*anyopaque,
    new_start: capi.Slice,
    new_end: capi.Slice,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_finalize_split), .{ .name = "antfly_db_finalize_split" }), .{ handle_ptr, new_start, new_end });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .exclusive) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    handle.database().*.finalizeSplit(.{
        .start = new_start.bytes(),
        .end = new_end.bytes(),
    }) catch |err| return capi.mapError(err);
    return .ok;
}

pub fn antfly_db_snapshot(
    handle_ptr: ?*anyopaque,
    id: capi.Slice,
    out_size: *u64,
) callconv(.c) capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_snapshot), .{ .name = "antfly_db_snapshot" }), .{ handle_ptr, id, out_size });
    capi.clearLastError();
    const guard = enterHandle(handle_ptr, .write) orelse return capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    out_size.* = handle.database().*.snapshot(id.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

comptime {
    _ = @import("inference.zig");
}

// Resolves a caller's handle id without entering it. Exports go through
// `enterHandle` instead; this is for close and for internal callers (the
// storage-owner ABI, tests) that do not race close.
comptime {
    if (!@import("../storage/engine_selection.zig").enabled) {
        @export(&antflyDbExtractEnrichmentsJson, .{
            .name = "antfly_db_extract_enrichments_json",
            .linkage = .strong,
        });
        @export(&antflyDbComputeEnrichmentsJson, .{
            .name = "antfly_db_compute_enrichments_json",
            .linkage = .strong,
        });
    }
}

pub const antfly_sources = @import("../source_owner_storage.zig");

test "issue1015 concurrent Lite handles sustain primary writes and full-text catch-up" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("issue1015-many-lite-handles");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const Worker = struct {
        path: [:0]u8,
        ordinal: usize,
        start: *std.atomic.Value(bool),
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            while (!self.start.load(.acquire)) std.atomic.spinLoopHint();
            self.exercise() catch |err| {
                self.failure = err;
            };
        }
        fn exercise(self: *@This()) !void {
            var handle: ?*anyopaque = null;
            try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(self.path, &handle));
            defer antfly_db_close(handle);
            for (0..200) |i| {
                var request_buffer: [256]u8 = undefined;
                const request = try std.fmt.bufPrint(&request_buffer, "{{\"inserts\":{{\"k:{d}:{d}\":{{\"search_text\":\"raft snapshot number {d}\"}}}},\"sync_level\":\"write\"}}", .{ self.ordinal, i, i });
                var output: capi.Buffer = .{};
                defer antfly_buffer_free(&output);
                try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch_json(handle, .{ .ptr = request.ptr, .len = request.len }, &output));
                if (i % 50 == 0) {
                    const query = "{\"full_text_search\":{\"match\":{\"field\":\"search_text\",\"text\":\"raft\"}},\"limit\":5}";
                    var hits: capi.Buffer = .{};
                    defer antfly_buffer_free(&hits);
                    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(handle, .{ .ptr = query.ptr, .len = query.len }, &hits));
                }
            }
            // Primary availability alone is insufficient: every secondary must
            // catch up too, so degradation cannot hide a dead derived worker.
            try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(handle));
        }
    };
    var start = std.atomic.Value(bool).init(false);
    var workers: [32]Worker = undefined;
    var threads: [32]std.Thread = undefined;
    var spawned: usize = 0;
    defer {
        start.store(true, .release);
        for (threads[0..spawned]) |thread| thread.join();
        for (workers[0..spawned]) |worker| alloc.free(worker.path);
    }
    for (&workers, &threads, 0..) |*worker, *thread, ordinal| {
        const path = try std.fmt.allocPrintSentinel(alloc, "{s}/db-{d}.aflite", .{ directory.path(), ordinal }, 0);
        errdefer alloc.free(path);
        worker.* = .{ .path = path, .ordinal = ordinal, .start = &start };
        thread.* = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, Worker.run, .{worker});
        spawned += 1;
    }
    start.store(true, .release);
    for (threads[0..spawned]) |thread| thread.join();
    const joined = spawned;
    spawned = 0;
    defer for (workers[0..joined]) |worker| alloc.free(worker.path);
    for (workers[0..joined]) |worker| if (worker.failure) |err| return err;
}

test "capi SQL connections share storage while session IDs and close remain independent" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("shared-lite-owner");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(alloc, "{s}/db.aflite", .{directory.path()}, 0);
    defer alloc.free(path);
    const options: LiteResolvedOpenOptions = .{ .no_sync = true, .profile = .hosted, .busy_timeout_ms = 5000 };
    const first = try openLiteHandleAlloc(path, options, true);
    const first_id = try publishHandle(first);
    defer antfly_db_close(first_id);
    var second_options = options;
    second_options.busy_timeout_ms = 30000;
    const second = try openLiteHandleAlloc(path, second_options, false);
    const second_id = try publishHandle(second);
    defer antfly_db_close(second_id);
    try std.testing.expect(first.lite_connection.?.owner == second.lite_connection.?.owner);
    const generation = first.lite_connection.?.current.?;
    var first_session: u64 = 0;
    var second_session: u64 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, @import("sql_session.zig").antfly_db_sql_session_open(first_id, &first_session));
    try std.testing.expectEqual(capi.ErrorCode.ok, @import("sql_session.zig").antfly_db_sql_session_open(second_id, &second_session));
    try std.testing.expectEqual(@as(u64, 1), first_session);
    try std.testing.expectEqual(first_session, second_session);
    for ([_][]const u8{
        \\{"statement":"CREATE TABLE numbers (n BIGINT)","session_id":1}
        ,
        \\{"statement":"BEGIN","session_id":1}
        ,
        \\{"statement":"INSERT INTO numbers (_id,n) VALUES ('row',7)","session_id":1}
        ,
    }) |request| {
        var output: capi.Buffer = .{};
        defer antfly_buffer_free(&output);
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(first_id, .fromSlice(request), &output));
    }
    const select =
        \\{"statement":"SELECT n FROM numbers","session_id":1}
    ;
    var output: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(second_id, .fromSlice(select), &output));
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, output.ptr.?[0..output.len], .{});
    try std.testing.expectEqual(@as(usize, 0), parsed.value.object.get("rows").?.array.items.len);
    parsed.deinit();
    antfly_buffer_free(&output);
    const commit =
        \\{"statement":"COMMIT","session_id":1}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(first_id, .fromSlice(commit), &output));
    antfly_buffer_free(&output);
    try std.testing.expect(second.lite_connection.?.current.? == generation);
    var cursor: u64 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, @import("sql_cursor.zig").antfly_db_sql_open_cursor_json(second_id, .fromSlice(select), &cursor, &output));
    antfly_buffer_free(&output);
    {
        defer _ = @import("sql_cursor.zig").antfly_db_sql_close_cursor(second_id, cursor);
        const guard = enterHandle(first_id, .exclusive).?;
        defer guard.leave();
        try std.testing.expect(guard.entry_error == null);
        // A durable DROP recovery must not retire another stream's runtime.
        try std.testing.expectError(error.SqlStatementReadUnavailable, @import("tables.zig").finishDrop(guard.handle, "numbers"));
    }
    antfly_db_close(first_id);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(second_id, .fromSlice(select), &output));
    defer antfly_buffer_free(&output);
    parsed = try std.json.parseFromSlice(std.json.Value, alloc, output.ptr.?[0..output.len], .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.object.get("rows").?.array.items.len);
}

test "capi SQL contention diagnostics preserve busy and unknown outcomes" {
    for ([_]struct { err: anyerror, code: capi.ErrorCode, state: []const u8 }{
        .{ .err = error.FileBusy, .code = .busy, .state = "55P03" },
        .{ .err = error.WouldBlock, .code = .busy, .state = "55P03" },
        .{ .err = error.OutcomeUnknown, .code = .outcome_unknown, .state = "40003" },
    }) |case| {
        var output: capi.Buffer = .{};
        defer antfly_buffer_free(&output);
        try std.testing.expectEqual(case.code, @import("sql_cursor.zig").diagnostic(case.err, &output));
        const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, output.ptr.?[0..output.len], .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(case.state, parsed.value.object.get("error").?.object.get("code").?.string);
    }
}

test "capi resolver replacement public waits retain admission generation across quanta" {
    const a = std.testing.allocator;
    for (0..4) |mode| {
        var directory = try TestDirectory.init("replacement-public-wait-generation");
        defer directory.cleanup();
        try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
        const path = try std.fmt.allocPrintSentinel(a, "{s}/db.aflite", .{directory.path()}, 0);
        defer a.free(path);
        const handle = try openLiteHandleAlloc(path, .{ .no_sync = true, .profile = .hosted }, true);
        const id = try publishHandle(handle);
        defer antfly_db_close(id);
        const old = "{\"name\":\"old\",\"type\":\"graph\",\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
        const replacement = "{\"name\":\"new\",\"type\":\"graph\",\"replaces\":\"old\",\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
        const wait_replace = "{\"name\":\"new\",\"type\":\"graph\",\"replaces\":\"old\",\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}],\"wait\":{\"timeout_ms\":1000}}";
        const wait_join = "{\"name\":\"new\",\"type\":\"graph\",\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}],\"wait\":{\"timeout_ms\":1000}}";
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(id, .fromSlice(old)));
        if (mode != 0) try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(id, .fromSlice(replacement)));
        const Reuse = struct {
            var later_quantum = false;
            var calls: usize = 0;
            var original_generation: u64 = 0;
            fn hook(handle_ptr: ?*anyopaque) anyerror!void {
                calls += 1;
                {
                    const guard = enterHandle(handle_ptr, .exclusive) orelse return error.TestUnexpectedResult;
                    defer guard.leave();
                    if (guard.entry_error != null) return error.TestUnexpectedResult;
                    const database = guard.handle.database();
                    const jobs = try database.graphReplacementStatusAlloc(guard.handle.alloc);
                    defer jobs.deinit();
                    try std.testing.expectEqual(@as(usize, 1), jobs.value.len);
                    original_generation = jobs.value[0].generation;
                    if (later_quantum and calls == 1) {
                        // Hold the first quantum in backoff, without relying
                        // on corpus size, scheduler speed or thread timing.
                        var job = jobs.value[0];
                        job.retry_after_ns = @as(u64, @intCast(@max(0, std.Io.Timestamp.now(database.core.index_manager.checkpointIo(), .real).toNanoseconds()))) + 2 * std.time.ns_per_s;
                        try database.core.index_manager.saveGraphReplacementBuild(database.core.store, job);
                        return;
                    }
                }
                test_index_before_wait_quantum = null;
                var deleted = false;
                try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_cancel_replacement(handle_ptr, .fromSlice("new"), &deleted));
                try std.testing.expect(deleted);
                try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(handle_ptr));
                const different = "{\"name\":\"new\",\"type\":\"graph\",\"edge_types\":[{\"name\":\"R\",\"field\":\"other_links\"}]}";
                try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle_ptr, .fromSlice(different)));
            }
        };
        Reuse.calls = 0;
        Reuse.original_generation = 0;
        Reuse.later_quantum = mode == 3;
        test_index_before_wait_quantum = Reuse.hook;
        defer test_index_before_wait_quantum = null;
        try std.testing.expectEqual(capi.ErrorCode.cancelled, antfly_db_add_index_json(id, .fromSlice(if (mode == 2) wait_join else wait_replace)));
        try std.testing.expectEqual(@as(usize, if (mode == 3) 2 else 1), Reuse.calls);
        const guard = enterHandle(id, .read) orelse return error.TestUnexpectedResult;
        defer guard.leave();
        const database = guard.handle.database();
        try std.testing.expect(database.indexGeneration("old") != null);
        try std.testing.expect(database.indexGeneration("new").? != Reuse.original_generation);
    }
}

comptime {
    @import("exports.zig").publish(@This(), &.{
        "antfly_db_memory_json",
        "antfly_db_open",
        "antfly_db_close",
        "antfly_threading_mode",
        "antfly_abi_version",
        "antfly_open_options_size",
        "antfly_last_error_detail",
        "antfly_last_error_code",
        "antfly_error_code_name",
        "antfly_error_code_description",
        "antfly_open_options_init",
        "antfly_db_open_with_options",
        "antfly_db_create_with_options",
        "antfly_lite_open",
        "antfly_lite_create",
        "antfly_lite_open_hosted",
        "antfly_lite_create_hosted",
        "antfly_lite_open_readonly",
        "antfly_lite_open_status_only",
        "antfly_db_capabilities_json",
        "antfly_db_status_json",
        "antfly_db_backup",
        "antfly_db_backup_json",
        "antfly_db_import_backup",
        "antfly_restore_backup_json",
        "antfly_restore_backup_file_json",
        "antfly_lite_check_json",
        "antfly_lite_check_file_json",
        "antfly_lite_copy_stable_snapshot_json",
        "antfly_lite_copy_stable_snapshot_file_json",
        "antfly_lite_compact_json",
        "antfly_lite_vacuum_json",
        "antfly_db_replay_generated_enrichments_json",
        "antfly_db_set_readable_lease_hook",
        "antfly_buffer_free",
        "antfly_buffer_free_zero",
        "antfly_dense_search_result_free",
        "antfly_packed_dense_search_result_free",
        "antfly_scan_hash_result_free",
        "antfly_db_begin_transaction_with_id",
        "antfly_db_write_transaction",
        "antfly_db_batch",
        "antfly_db_batch_json",
        "antfly_db_resolve_intents",
        "antfly_db_get_transaction_status",
        "antfly_db_get_commit_version",
        "antfly_db_get_timestamp",
        "antfly_db_lookup_json",
        "antfly_db_get_raw",
        "antfly_db_lookup_artifact_json",
        "antfly_decode_artifact_id_json",
        "antfly_db_get_schema_json",
        "antfly_db_set_schema_json",
        "antfly_db_run_until_idle",
        "antfly_db_run_until_idle_json",
        "antfly_db_pending_work_stats_json",
        "antfly_db_update_range",
        "antfly_db_get_range_json",
        "antfly_db_get_split_state_json",
        "antfly_db_set_split_state_json",
        "antfly_db_clear_split_state",
        "antfly_db_get_split_delta_seq",
        "antfly_db_get_split_delta_final_seq",
        "antfly_db_set_split_delta_final_seq",
        "antfly_db_clear_split_delta_final_seq",
        "antfly_db_list_split_delta_entries_after_json",
        "antfly_db_clear_split_delta_entries",
        "antfly_db_list_indexes_json",
        "antfly_db_list_enrichments_json",
        "antfly_db_scan_json",
        "antfly_db_scan_hashes",
        "antfly_db_stats_json",
        "antfly_db_sql_json",
        "antfly_db_search_json",
        "antfly_db_search_dense",
        "antfly_db_search_dense_profile",
        "antfly_db_dense_noop",
        "antfly_db_dense_fixed_packed_result",
        "antfly_db_search_dense_wire",
        "antfly_db_search_dense_wire_profile",
        "antfly_db_search_text_match",
        "antfly_db_search_text_match_wire",
        "antfly_db_search_text_term_wire",
        "antfly_db_search_text_match_phrase_wire",
        "antfly_db_search_hits_json",
        "antfly_db_execute_graph_queries_json",
        "antfly_db_aggregate_hits_json",
        "antfly_db_render_resolver_key_json",
        "antfly_db_resolve_query_json",
        "antfly_db_merge_resolver_keys_json",
        "antfly_db_unmerge_resolver_key_json",
        "antfly_db_suppress_resolver_keys_json",
        "antfly_db_unsuppress_resolver_key_json",
        "antfly_db_resolver_curation_json",
        "antfly_db_resolver_reviews_json",
        "antfly_db_curate_entity_json",
        "antfly_db_entity_merge_json",
        "antfly_db_entity_unmerge_json",
        "antfly_db_entity_suppress_json",
        "antfly_db_entity_unsuppress_json",
        "antfly_db_entity_redirects_json",
        "antfly_db_list_resolver_identities_json",
        "antfly_db_drop_resolver_identity",
        "antfly_db_set_resolver_namespace_json",
        "antfly_db_add_index_json",
        "antfly_db_cancel_replacement",
        "antfly_db_delete_index",
        "antfly_db_add_enrichment_json",
        "antfly_db_delete_enrichment",
        "antfly_db_get_edges_json",
        "antfly_db_traverse_edges_json",
        "antfly_db_get_neighbors_json",
        "antfly_db_find_shortest_path_json",
        "antfly_db_find_k_shortest_paths_json",
        "antfly_db_match_pattern_json",
        "antfly_db_create_shadow_index_manager",
        "antfly_db_close_shadow_index_manager",
        "antfly_db_get_shadow_index_dir",
        "antfly_db_find_median_key",
        "antfly_db_split",
        "antfly_db_finalize_split",
        "antfly_db_snapshot",
    });
}

test "inline resolver replacement C API admits the same public name with fresh artifacts" {
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("inline-resolver-public-replacement");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(a, "{s}/db.aflite", .{directory.path()}, 0);
    defer a.free(path);
    const handle = try openLiteHandleAlloc(path, .{ .no_sync = true, .profile = .native }, true);
    const id = try publishHandle(handle);
    defer antfly_db_close(id);
    const old =
        \\{"name":"old","kind":"graph","config_json":{"artifact":{"name":"kg_v1","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"source":{"artifact":"kg_v1","format":"extraction_graph","mention_edge_type":"mentions"},"edge_types":[{"name":"mentions"}],"resolvers":[{"name":"concepts","table":"default","source_artifact":"kg_v1","resolution_artifact":"resolved_v1","key_template":"entity/{{ slug _entity.text }}","promote":false}]}}
    ;
    const next =
        \\{"name":"new","kind":"graph","replaces":"old","config_json":{"artifact":{"name":"kg_v2","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"source":{"artifact":"kg_v2","format":"extraction_graph","mention_edge_type":"mentions"},"edge_types":[{"name":"mentions"}],"resolvers":[{"name":"concepts","table":"default","source_artifact":"kg_v2","resolution_artifact":"resolved_v2","key_template":"entity/{{ uslug _entity.text }}","promote":false}]}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(id, .fromSlice(old)));
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(id, .fromSlice(next)));
    // An identical pending admission joins its durable generation.
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(id, .fromSlice(next)));
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(id));
    {
        const guard = enterHandle(id, .read) orelse return error.TestUnexpectedResult;
        defer guard.leave();
        try std.testing.expect(guard.entry_error == null);
        const database = guard.handle.database();
        const resolvers = try database.listResolvers(a);
        defer {
            for (resolvers) |*cfg| cfg.deinit(a);
            a.free(resolvers);
        }
        try std.testing.expectEqual(@as(usize, 1), resolvers.len);
        try std.testing.expectEqualStrings("concepts", resolvers[0].name);
        try std.testing.expectEqualStrings("kg_v2", resolvers[0].source_artifact);
        try std.testing.expectEqualStrings("resolved_v2", resolvers[0].resolution_artifact);
        try std.testing.expectEqualStrings("entity/{{ uslug _entity.text }}", resolvers[0].key_template);
        try std.testing.expect(database.indexGeneration("old") == null);
    }
    // Replaying the declarative config after activation remains idempotent.
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(id, .fromSlice(next)));
}

test "capi native memory persists receipts and forgetting across reopen" {
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("native-memory-reopen");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(a, "{s}/db.aflite", .{directory.path()}, 0);
    defer a.free(path);
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &id));
    defer if (id != null) antfly_db_close(id);
    const Call = struct {
        fn call(handle: ?*anyopaque, body: []const u8) ![]u8 {
            var output: capi.Buffer = .{};
            const code = antfly_db_memory_json(handle, .fromSlice("personal"), .fromSlice(body), &output);
            if (code != .ok) std.debug.print("memory C ABI: {s}\n", .{std.mem.span(capi.lastErrorDetail())});
            try std.testing.expectEqual(capi.ErrorCode.ok, code);
            defer std.heap.c_allocator.free(output.ptr.?[0..output.len]);
            return std.testing.allocator.dupe(u8, output.ptr.?[0..output.len]);
        }
    };
    const create = try Call.call(id, "{\"action\":\"create\",\"idempotency_key\":\"create\"}");
    defer a.free(create);
    const observe = try Call.call(id,
        \\{"action":"observe","idempotency_key":"evidence","body":{"evidence":[{"id":"e1","source_id":"note","source_version":"1","text":"Use Terraform"}],"assertions":[{"id":"a1","subject":"deploy","predicate":"tool","value":"Terraform","statement":"Use Terraform","evidence":[{"id":"e1"}]}]}}
    );
    defer a.free(observe);
    antfly_db_close(id);
    id = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open(path, &id));
    const recalled = try Call.call(id, "{\"action\":\"recall\",\"body\":{\"query\":\"Terraform\",\"token_budget\":8192}}");
    defer a.free(recalled);
    try std.testing.expect(std.mem.indexOf(u8, recalled, "Use Terraform") != null);
    const forgotten = try Call.call(id, "{\"action\":\"forget\",\"idempotency_key\":\"forget\",\"body\":{\"evidence_ids\":[\"e1\"]}}");
    defer a.free(forgotten);
    antfly_db_close(id);
    id = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open(path, &id));
    const hidden = try Call.call(id, "{\"action\":\"recall\",\"body\":{\"query\":\"Terraform\",\"token_budget\":8192}}");
    defer a.free(hidden);
    try std.testing.expect(std.mem.indexOf(u8, hidden, "Use Terraform") == null);
    // Physical retirement, not only the evidence fence, survives native reopen.
    for (0..10) |i| {
        const body = try std.fmt.allocPrint(a, "{{\"action\":\"advance\",\"idempotency_key\":\"cleanup-{d}\"}}", .{i});
        defer a.free(body);
        const cleaned = try Call.call(id, body);
        a.free(cleaned);
    }
    antfly_db_close(id);
    id = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open(path, &id));
    const fresh = try Call.call(id,
        \\{"action":"observe","idempotency_key":"fresh","body":{"evidence":[{"id":"e2","source_id":"fresh","source_version":"1","text":"New fact"}]}}
    );
    defer a.free(fresh);
    var output: capi.Buffer = .{};
    const reused = antfly_db_memory_json(id, .fromSlice("personal"), .fromSlice(
        \\{"action":"assertions","idempotency_key":"reuse","body":{"assertions":[{"id":"a1","subject":"other","predicate":"tool","value":"Other","statement":"Different assertion under a retired ID","evidence":[{"id":"e2"}]}]}}
    ), &output);
    defer if (output.ptr) |ptr| std.heap.c_allocator.free(ptr[0..output.len]);
    try std.testing.expect(reused != .ok);
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(capi.lastErrorDetail()), "MemoryForgotten") != null);
}
