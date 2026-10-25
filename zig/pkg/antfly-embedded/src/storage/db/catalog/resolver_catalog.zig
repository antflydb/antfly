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

//! Persisted catalog of entity-resolution configs (see zig/RESOLUTION.md).
//!
//! A resolver consumes a source extraction artifact for a document, resolves
//! its mentions to canonical entity `DocRef`s, and writes a resolution
//! artifact. The durable config mirrors the enrichment catalog: it is stored
//! per shard and replayed at the recorded `config_generation` so resolution
//! decisions stay stable (the replay-stability invariant).
//!
//! `scorer_json` is the optional matcher scorer config (parsed by
//! `pkg/antfly-embedded/matcher`); an empty `scorer_json` means a purely deterministic resolver
//! that always mints canonical keys from `key_template`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const resolver_lib = @import("antfly_resolver");
const matcher = @import("antfly_matcher");

/// Durable resolver-backfill cursors. They live with the resolver catalog
/// metadata so catalog mutations can mark backfill dirty in the same store
/// transaction that publishes the catalog change.
// Durable key spelling belongs to the catalog contract, not the physical DB.
pub const drop_marker_prefix = "\x00\x00__metadata__:resolver_curation_drop:";

pub const reresolve_resume_key = "\x00\x00__metadata__:resolution_reresolve_resume";
pub const reresolve_repair_resume_key = "\x00\x00__metadata__:resolution_reresolve_repair_resume";

/// Map a `fusion_combine` config string to a matcher fusion strategy. `null`
/// means either "" (fusion disabled) or an unrecognized value; callers that
/// must distinguish (config validation) check `len == 0` first.
pub fn fusionStrategy(name: []const u8) ?matcher.FusionStrategy {
    if (std.mem.eql(u8, name, "noisy_or")) return .noisy_or;
    if (std.mem.eql(u8, name, "max")) return .max;
    if (std.mem.eql(u8, name, "mean")) return .mean;
    return null;
}

fn cloneLabels(alloc: Allocator, labels: []const []const u8) ![]const []const u8 {
    if (labels.len == 0) return &.{};
    const out = try alloc.alloc([]const u8, labels.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |l| alloc.free(@constCast(l));
        alloc.free(out);
    }
    for (labels, 0..) |l, i| {
        out[i] = try alloc.dupe(u8, l);
        initialized += 1;
    }
    return out;
}

/// Monotonic feature epoch, published with catalog mutations and checked before parsing.
pub const minimum_reader_key = "\x00\x00__metadata__:minimum_reader_version";
pub const replacement_reader_base_key = "\x00\x00__metadata__:replacement_reader_base_v1";
pub const graph_expiration_namespace_reader_version: u32 = 4;
pub const adjudication_reader_version: u32 = 5;
pub const curation_reader_version: u32 = 6;
pub const generated_producers_reader_version: u32 = 7;
pub const grouped_sources_reader_version: u32 = 8;
pub const namespace_curation_reader_version: u32 = 9;
pub const curation_provenance_reader_version: u32 = 10;
pub const scoped_curation_reader_version: u32 = 11;
pub const native_resolver_records_reader_version: u32 = 12;
pub const resolver_projection_bindings_reader_version: u32 = 13;
pub const resolver_projection_work_reader_version: u32 = 14;
pub const resolver_projection_delta_reader_version: u32 = 15;
pub const resolver_static_profiles_reader_version: u32 = 19;
pub const resolver_pin_index_reader_version: u32 = 20;
pub const supported_reader_version: u32 = resolver_pin_index_reader_version;

const reader_version_encodings = blk: {
    var encodings: [supported_reader_version + 1][]const u8 = undefined;
    for (&encodings, 0..) |*encoding, version| encoding.* = std.fmt.comptimePrint("{d}", .{version});
    break :blk encodings;
};
pub fn readerVersionBytes(version: u32) ![]const u8 {
    if (version == 0 or version > supported_reader_version) return error.UnsupportedDatabaseReaderVersion;
    return reader_version_encodings[version];
}

pub fn versionedCatalog(a: Allocator, data: []const u8, version: u32) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, data, .{});
    defer parsed.deinit();
    if (parsed.value == .array) {
        return try std.fmt.allocPrint(a, "{{\"version\":{d},\"resolvers\":{s}}}", .{ version, data });
    }
    if (parsed.value != .object) return error.InvalidResolverConfig;
    try parsed.value.object.put(parsed.arena.allocator(), "version", .{ .integer = version });
    return try std.json.Stringify.valueAlloc(a, parsed.value, .{});
}

pub fn catalogReaderVersion(a: Allocator, data: []const u8) !u32 {
    const parsed = try std.json.parseFromSlice(std.json.Value, a, data, .{});
    defer parsed.deinit();
    const rows = try catalogRows(parsed.value);
    var required: u32 = if (parsed.value == .object) @intCast(parsed.value.object.get("version").?.integer) else 1;
    for (rows.array.items) |entry| {
        if (entry != .object) return error.InvalidResolverConfig;
        if (entry.object.get("adjudicate_json")) |adjudicate| {
            required = @max(required, adjudication_reader_version);
            if (adjudicate == .string and adjudicate.string.len > 0) {
                const config = try std.json.parseFromSlice(std.json.Value, a, adjudicate.string, .{});
                defer config.deinit();
                if (config.value == .object) if (config.value.object.get("decider")) |decider| if (decider == .object) if (decider.object.get("decision_method")) |method| {
                    if (method == .string and std.mem.eql(u8, method.string, "generated")) required = @max(required, generated_producers_reader_version);
                };
            }
        }
        if (entry.object.get("curation_json")) |curation| {
            if (curation == .string and curation.string.len > 0) {
                const state = try std.json.parseFromSlice(std.json.Value, a, curation.string, .{});
                defer state.deinit();
                if (state.value == .object and (state.value.object.contains("redirect_origins") or state.value.object.contains("proposals"))) required = @max(required, curation_provenance_reader_version);
                if (state.value == .object and state.value.object.contains("receipt_scope")) required = @max(required, scoped_curation_reader_version);
            }
        }
        if (entry.object.contains("namespace_curation_table_id")) required = @max(required, resolver_projection_bindings_reader_version);
        if (entry.object.contains("namespace_curation_generation")) required = @max(required, namespace_curation_reader_version);
        if (entry.object.contains("suppress_json") or entry.object.contains("curation_json") or entry.object.contains("curation_revision")) required = @max(required, curation_reader_version);
        if (entry.object.contains("replacement_index") or entry.object.contains("replacement_resolver")) required = @max(required, 3);
        for ([_][]const u8{ "inline_managed", "retired", "aliases", "promote", "merge_generation", "merge_state_version", "resolution_generation", "namespace_generation", "retain_work", "max_work_bytes" }) |field| {
            if (entry.object.contains(field)) required = @max(required, 2);
        }
    }
    return required;
}

pub fn readMinimumReaderVersion(txn: anytype, supported: u32) !u32 {
    const raw = txn.get(minimum_reader_key) catch |err| switch (err) {
        error.NotFound => return 1,
        else => return err,
    };
    const version = std.fmt.parseInt(u32, raw, 10) catch return error.InvalidResolverConfig;
    if (version > supported) return error.UnsupportedDatabaseReaderVersion;
    return version;
}

/// Exclude temporary replacement reservations when proving a permanent format
/// upgrade. A migration completion marker must outlive cancellation of a job.
pub fn readAuthoritativeReaderVersion(txn: anytype, supported: u32) !u32 {
    const raw = txn.get(replacement_reader_base_key) catch |err| switch (err) {
        error.NotFound => return readMinimumReaderVersion(txn, supported),
        else => return err,
    };
    const version = try std.fmt.parseInt(u32, raw, 10);
    if (version == 0 or version > supported) return error.UnsupportedDatabaseReaderVersion;
    return version;
}

pub fn stampMinimumReaderVersion(a: Allocator, txn: anytype, catalog: []const u8) !u32 {
    const version = @max(try readMinimumReaderVersion(txn, supported_reader_version), try catalogReaderVersion(a, catalog));
    try txn.put(minimum_reader_key, try readerVersionBytes(version));
    return version;
}

pub const ResolverSourceArtifactKind = enum {
    /// Root and unit-scoped asset artifacts. This is the legacy default.
    asset,
    /// Chunk-scoped artifacts produced under an asset or unit.
    chunk,
    /// Consume every artifact kind with the configured artifact name.
    any,

    pub fn matches(self: ResolverSourceArtifactKind, actual: ResolverSourceArtifactKind) bool {
        return self == .any or self == actual;
    }

    /// Whether two resolver scopes can consume the same artifact kind.
    pub fn overlaps(self: ResolverSourceArtifactKind, other: ResolverSourceArtifactKind) bool {
        return self.matches(other) or other.matches(self);
    }
};

/// In-process validation evidence. It is never deserialized or persisted.
pub const PolicyValidation = struct {
    policy_digest: [64]u8,
    static_digest: [32]u8,
    pub fn jsonParse(_: Allocator, _: anytype, _: std.json.ParseOptions) !PolicyValidation {
        return error.UnexpectedToken;
    }
    pub fn jsonParseFromValue(_: Allocator, _: std.json.Value, _: std.json.ParseOptions) !PolicyValidation {
        return error.UnexpectedToken;
    }
};
pub fn policyProfileDigest(a: Allocator, aliases: []const resolver_lib.KeyAlias, suppress_json: []const u8) ![32]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(try std.json.Stringify.valueAlloc(a, .{ aliases, suppress_json }, .{}), &digest, .{});
    return digest;
}

/// Immutable policy owned once by a catalog generation. Ordinary `clone`
/// remains detached (including arena callers); only persistent catalog slots
/// opt into reference ownership through `cloneWithProjection`.
pub const SharedProjection = struct {
    alloc: Allocator,
    references: std.atomic.Value(usize) = .init(1),
    merges: []const resolver_lib.KeyAlias,
    curation_json: []const u8,
    encoded: []const u8,
    digest: [64]u8,

    pub fn create(alloc: Allocator, merges: []const resolver_lib.KeyAlias, state: []const u8) !*SharedProjection {
        const owned = try ResolverConfig.cloneAliases(alloc, merges);
        errdefer {
            for (owned) |alias| {
                alloc.free(alias.key);
                alloc.free(alias.target);
            }
            alloc.free(owned);
        }
        const text = try alloc.dupe(u8, state);
        errdefer alloc.free(text);
        const encoded = try std.json.Stringify.valueAlloc(alloc, StoredProjection{ .merges = owned, .curation_json = text }, .{});
        errdefer alloc.free(encoded);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
        const result = try alloc.create(SharedProjection);
        result.* = .{ .alloc = alloc, .merges = owned, .curation_json = text, .encoded = encoded, .digest = std.fmt.bytesToHex(digest, .lower) };
        return result;
    }
    pub fn retain(self: *SharedProjection) void {
        _ = self.references.fetchAdd(1, .monotonic);
    }
    pub fn release(self: *SharedProjection) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        const a = self.alloc;
        for (self.merges) |alias| {
            a.free(alias.key);
            a.free(alias.target);
        }
        a.free(self.merges);
        a.free(self.curation_json);
        a.free(self.encoded);
        a.destroy(self);
    }
    // Ownership is never deserialized from a public or durable config.
    pub fn jsonParse(_: Allocator, _: anytype, _: std.json.ParseOptions) !SharedProjection {
        return error.UnexpectedToken;
    }
    pub fn jsonParseFromValue(_: Allocator, _: std.json.Value, _: std.json.ParseOptions) !SharedProjection {
        return error.UnexpectedToken;
    }
};

/// A replay observation is deliberately not an executable resolver config.
/// It carries only owned identity strings and scalar publication fences.
pub const ResolverObservation = struct {
    name: []const u8,
    table: []const u8,
    key_template: []const u8,
    retired: bool,
    promote: bool,
    config_generation: u64,
    effective_generation: u64 = 0,
    namespace_generation: u64,
    curation_revision: u64,
    namespace_curation_table_id: u64,
    namespace_curation_generation: u64,
    namespace_curation_missing: bool,

    pub fn fromConfig(a: Allocator, cfg: ResolverConfig) !ResolverObservation {
        const name = try a.dupe(u8, cfg.name);
        errdefer a.free(name);
        const table = try a.dupe(u8, cfg.table);
        errdefer a.free(table);
        const template = try a.dupe(u8, cfg.key_template);
        return .{ .name = name, .table = table, .key_template = template, .retired = cfg.retired, .promote = cfg.promote, .config_generation = cfg.config_generation, .effective_generation = cfg.effectiveGeneration(), .namespace_generation = cfg.namespace_generation, .curation_revision = cfg.curation_revision, .namespace_curation_table_id = cfg.namespace_curation_table_id, .namespace_curation_generation = cfg.namespace_curation_generation, .namespace_curation_missing = cfg.namespace_curation_missing };
    }
    pub fn deinit(self: *ResolverObservation, a: Allocator) void {
        a.free(self.name);
        a.free(self.table);
        a.free(self.key_template);
        self.* = undefined;
    }
};

/// Shared curation admission contract. These fields suffice to fold legacy
/// namespace policy, retain static declarations and fence owner publication.
/// Execution prompts, providers, artifacts and embedding selectors never travel
/// with an entity consumer. Empty artifact fields keep the private wire shape
/// readable by existing ResolverConfig decoders; this is not executable config.
pub const ResolverAdmission = struct {
    name: []const u8,
    table: []const u8,
    key_template: []const u8,
    source_artifact: []const u8 = "",
    resolution_artifact: []const u8 = "",
    admission_profile: []const u8 = "",
    aliases: []const resolver_lib.KeyAlias = &.{},
    suppress_json: []const u8 = "",
    merges: []const resolver_lib.KeyAlias = &.{},
    curation_json: []const u8 = "{}",
    merge_generation: u64 = 0,
    merge_state_version: u32 = 0,
    resolution_generation: u64 = 0,
    namespace_generation: u64 = 0,
    config_generation: u64 = 0,
    curation_revision: u64 = 0,
    namespace_curation_generation: u64 = 0,
    namespace_curation_table_id: u64 = 0,
    namespace_curation_missing: bool = false,
    retired: bool = false,
    promote: bool = true,

    pub fn fromConfig(cfg: ResolverConfig, namespace_owned: bool) ResolverAdmission {
        var result: ResolverAdmission = .{ .name = cfg.name, .table = cfg.table, .key_template = cfg.key_template };
        inline for (@typeInfo(ResolverAdmission).@"struct".field_names) |field| {
            if (comptime !std.mem.eql(u8, field, "source_artifact") and !std.mem.eql(u8, field, "resolution_artifact")) @field(result, field) = @field(cfg, field);
        }
        if (namespace_owned) {
            result.merges = &.{};
            result.curation_json = "{}";
        }
        return result;
    }
    /// Borrowed adapter for policy helpers shared with resolver execution.
    pub fn config(self: ResolverAdmission) ResolverConfig {
        var result: ResolverConfig = .{ .name = self.name, .table = self.table, .key_template = self.key_template, .source_artifact = "", .resolution_artifact = "" };
        inline for (@typeInfo(ResolverAdmission).@"struct".field_names) |field| @field(result, field) = @field(self, field);
        return result;
    }
    pub fn cloneConfig(a: Allocator, cfg: ResolverConfig, namespace_owned: bool) !ResolverConfig {
        return ResolverConfig.clone(a, fromConfig(cfg, namespace_owned).config());
    }
};

const ResolverConfigField = std.meta.FieldEnum(ResolverConfig);

pub const ResolverConfig = struct {
    /// Catalog name of the resolver.
    name: []const u8,
    /// Entity table that canonical entities live in.
    table: []const u8,
    /// Extraction artifact this resolver consumes.
    source_artifact: []const u8,
    /// Kind of extraction artifact this resolver consumes. Existing configs
    /// default to asset artifacts; chunk-level mention streams must opt in.
    source_artifact_kind: ResolverSourceArtifactKind = .asset,
    /// Resolution artifact this resolver writes.
    resolution_artifact: []const u8,
    /// Template that renders a canonical entity key from a mention.
    key_template: []const u8,
    /// Explicit key redirects, usable without entity promotion.
    admission_profile: []const u8 = "",
    aliases: []const resolver_lib.KeyAlias = &.{},
    /// API-created redirects are durable identity state, never inline config.
    merges: []const resolver_lib.KeyAlias = &.{},
    shared_projection: ?*SharedProjection = null,
    validation_proof: ?PolicyValidation = null,
    /// Combined resolution revision after the first merge. Advanced on either
    /// config changes or merges; the field name retains catalog compatibility.
    merge_generation: u64 = 0,
    merge_state_version: u32 = 0,
    /// Internal clock, including config changes before the first merge.
    resolution_generation: u64 = 0,
    /// Explicit namespace operations fence subsequent declarative replacements,
    /// including resets with no remaining merge records.
    namespace_generation: u64 = 0,
    /// Inline declarations own execution, while retired definitions retain identity
    /// state for re-add/rename. These fields are catalog-owned, never public config.
    inline_managed: ?bool = null,
    retired: bool = false,
    /// Unpublished execution contract owned by a durable graph replacement.
    /// Its internal name keeps the serving resolver addressable during replay.
    replacement_index: []const u8 = "",
    replacement_resolver: []const u8 = "",

    /// Materialize canonical entity documents; false resolves graph keys only.
    promote: bool = true,
    /// Optional work is bounded independently of authoritative resolutions.
    retain_work: resolver_lib.WorkRetention = .full,
    max_work_bytes: usize = 1024 * 1024,
    /// Mention labels this resolver consumes. Empty means all labels
    /// (catch-all). Labeled resolvers sharing a source artifact must claim
    /// disjoint label sets (admission-enforced), and catch-alls skip labels
    /// claimed by labeled siblings at runtime, so each mention routes to at
    /// most one labeled resolver while extraction labels stay
    /// open-vocabulary (label-routed tables, e.g. events vs entities).
    labels: []const []const u8 = &.{},
    /// Require the candidate entity label to match the mention label.
    type_must_match: bool = true,
    /// Optional matcher scorer config; empty means deterministic minting only.
    scorer_json: []const u8 = "",
    /// Frozen, validated REVIEW adjudication policy; public spelling is adjudicate.
    adjudicate_json: []const u8 = "",
    suppress_json: []const u8 = "",
    /// Runtime identity state, preserved across graph replacement/retirement.
    curation_json: []const u8 = "",
    curation_revision: u64 = 0,
    namespace_curation_generation: u64 = 0,
    /// Physical authority incarnation; namespace revisions are scoped to it.
    namespace_curation_table_id: u64 = 0,
    namespace_curation_missing: bool = false,
    /// Candidate blocking strategy: "" (none, deterministic mint) or
    /// "exact_key" (look up the rendered canonical key as a candidate so the
    /// scorer can link to an existing entity). ANN/prefix blocking over the
    /// entity table is a phase-2 extension.
    candidate_search: []const u8 = "",
    /// Dense index to use for `candidate_search = "ann"`. Empty defaults to
    /// `name_embedding` for compatibility with early resolver configs.
    candidate_ann_index: []const u8 = "",
    /// Maximum ANN candidates to request. Zero uses the runtime default.
    candidate_limit: u32 = 0,
    /// Embedding model name used to backfill a mention's name embedding (for
    /// `cosine` scoring / `ann` blocking) when the extraction artifact carries
    /// none. Empty disables backfill. Must match the embedding the entity table
    /// indexes over its canonical name so the vectors are comparable.
    name_embedding: []const u8 = "",
    /// Dimensionality for `name_embedding` (0 lets the embedder decide).
    name_embedding_dims: u32 = 0,
    /// Confidence-fusion strategy for the provenance edge weight: "" (legacy
    /// fixed weight 1.0), "noisy_or", "max", or "mean". When set, the mention
    /// edge's weight is `matcher.fuse` of this extractor's `fusion_trust *`
    /// the mention's asserted confidence, folded with the config-pinned graph
    /// prior. This is the naive first fusion implementation (one source per
    /// resolver); cross-extractor combine over a live snapshot is a later step.
    fusion_combine: []const u8 = "",
    /// This extractor's trust in [0, 1]; scales its asserted confidence.
    fusion_trust: f64 = 1.0,
    /// Config-generation-pinned graph prior belief in [0, 1] folded into the
    /// fused weight (a fixed snapshot value avoids the streaming self-reinforce
    /// caveat: the prior never reads the edges currently being written).
    fusion_prior: f64 = 0.0,
    /// Weight of `fusion_prior` in the fusion; 0 ignores the prior.
    fusion_prior_weight: f64 = 0.0,
    /// Mention admission floor in [0, 1]: mentions whose extractor-asserted
    /// confidence is below this are never resolved (no canonical key, no
    /// mention edge; relation endpoints referencing them stay unresolvable
    /// and are dropped by the canonical-only rule). The cheap
    /// post-extraction junk filter for score-carrying extractors (GLiNER);
    /// 0 admits everything.
    min_confidence: ?f64 = null,
    /// Bumped to force a versioned re-resolution pass.
    config_generation: u64 = 0,

    pub fn parseValue(a: Allocator, value: std.json.Value) !std.json.Parsed(ResolverConfig) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var normalized = try std.json.parseFromSliceLeaky(std.json.Value, scratch, try std.json.Stringify.valueAlloc(scratch, value, .{}), .{});
        if (normalized != .object) return error.InvalidResolverConfig;
        if (normalized.object.fetchSwapRemove("suppress")) |entry| if (entry.value != .null) {
            if (normalized.object.contains("suppress_json")) return error.InvalidResolverConfig;
            try normalized.object.put(scratch, "suppress_json", .{ .string = try std.json.Stringify.valueAlloc(scratch, entry.value, .{}) });
        };
        if (normalized.object.fetchSwapRemove("adjudicate")) |entry| {
            if (entry.value != .null) {
                if (normalized.object.contains("adjudicate_json")) return error.InvalidResolverConfig;
                const raw = try std.json.Stringify.valueAlloc(scratch, entry.value, .{});
                try @import("../../../functions/resolver_adjudication.zig").validate(scratch, raw);
                try normalized.object.put(scratch, "adjudicate_json", .{ .string = raw });
            }
        }
        // Embedded callers may use the same typed scorer spelling as HTTP.
        if (normalized.object.fetchSwapRemove("scorer")) |entry| {
            if (entry.value != .null) {
                if (normalized.object.contains("scorer_json")) return error.InvalidResolverConfig;
                try normalized.object.put(scratch, "scorer_json", .{ .string = try std.json.Stringify.valueAlloc(scratch, entry.value, .{}) });
            }
        }
        return std.json.parseFromValue(ResolverConfig, a, normalized, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    }

    /// Public identity inspection never exposes a frozen provider credential.
    pub fn publicValue(self: ResolverConfig, a: Allocator) !std.json.Value {
        var value = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, self, .{}), .{});
        _ = value.object.swapRemove("curation_json");
        _ = value.object.swapRemove("namespace_curation_table_id");
        _ = value.object.swapRemove("namespace_curation_missing");
        if (value.object.fetchSwapRemove("suppress_json")) |entry|
            try value.object.put(a, "suppress", try std.json.parseFromSliceLeaky(std.json.Value, a, entry.value.string, .{}));
        if (value.object.fetchSwapRemove("adjudicate_json")) |entry| {
            var policy = try std.json.parseFromSliceLeaky(std.json.Value, a, entry.value.string, .{});
            if (policy.object.getPtr("decider")) |decider| {
                try @import("../../../common/credential_safety.zig").redactPublicProviderConfig(a, decider);
            }
            try value.object.put(a, "adjudicate", policy);
        }
        return value;
    }

    fn sharedPolicyDigest(self: ResolverConfig) ?[64]u8 {
        const projection = self.shared_projection orelse return null;
        if (self.merges.ptr != projection.merges.ptr or self.merges.len != projection.merges.len or self.curation_json.ptr != projection.curation_json.ptr or self.curation_json.len != projection.curation_json.len) return null;
        return projection.digest;
    }
    pub fn eql(self: ResolverConfig, other: ResolverConfig) bool {
        const shared_equal = if (self.sharedPolicyDigest()) |left| if (other.sharedPolicyDigest()) |right| std.mem.eql(u8, &left, &right) else false else false;
        inline for (@typeInfo(ResolverConfig).@"struct".field_names, @typeInfo(ResolverConfig).@"struct".field_types) |reflected_name, field_type| {
            if (comptime std.mem.eql(u8, reflected_name, "shared_projection") or std.mem.eql(u8, reflected_name, "validation_proof")) continue;
            if (!shared_equal or (comptime !std.mem.eql(u8, reflected_name, "merges") and !std.mem.eql(u8, reflected_name, "curation_json"))) {
                if (field_type == []const u8) {
                    if (!std.mem.eql(u8, @field(self, reflected_name), @field(other, reflected_name))) return false;
                } else if (field_type == []const []const u8) {
                    const a = @field(self, reflected_name);
                    const b = @field(other, reflected_name);
                    if (a.len != b.len) return false;
                    for (a, b) |as, bs| if (!std.mem.eql(u8, as, bs)) return false;
                } else if (field_type == []const resolver_lib.KeyAlias) {
                    if (!aliasesEqual(@field(self, reflected_name), @field(other, reflected_name))) return false;
                } else if (!std.meta.eql(@field(self, reflected_name), @field(other, reflected_name))) return false;
            }
        }
        return true;
    }

    pub fn rebaseNativeProjection(self: *ResolverConfig, previous: ResolverConfig) !void {
        if (self.namespace_curation_table_id == 0) return;
        const same = self.namespace_curation_table_id == previous.namespace_curation_table_id and std.mem.eql(u8, self.table, previous.table) and std.mem.eql(u8, self.key_template, previous.key_template);
        if (same and self.namespace_curation_generation < previous.namespace_curation_generation) return error.ResolverCurationConflict;
        // The source execution clock is resolution_generation. Curation is
        // scoped to the physical destination and may restart after migration.
        self.curation_revision = self.namespace_curation_generation;
    }

    pub fn effectiveGeneration(self: ResolverConfig) u64 {
        return @max(self.resolution_generation, @max(self.config_generation, self.merge_generation));
    }

    /// Borrowed strings, owned array. Durable curation wins over inline aliases.
    pub fn effectiveAliasesAlloc(self: ResolverConfig, alloc: Allocator) ![]const resolver_lib.KeyAlias {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const curation = try resolver_lib.curation.Index.init(arena.allocator(), "", self.curation_json);
        var curated: std.StringHashMapUnmanaged(void) = .empty;
        for (self.merges) |alias| try curated.put(arena.allocator(), alias.key, {});
        var result: std.ArrayList(resolver_lib.KeyAlias) = .empty;
        errdefer result.deinit(alloc);
        for (self.aliases) |alias| {
            if (curation.removed_aliases.contains(alias.key)) continue;
            if (!curated.contains(alias.key)) try result.append(alloc, alias);
        }
        try result.appendSlice(alloc, self.merges);
        return try result.toOwnedSlice(alloc);
    }

    /// Default catalogs retain their pre-extension wire shape. Required merge
    /// metadata stays versioned; old strict readers reject it rather than
    /// silently forgetting curated identity state on a downgrade.
    pub fn jsonStringify(self: ResolverConfig, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(ResolverConfig).@"struct".field_names) |field| {
            if (comptime !std.mem.eql(u8, field, "merges") and !std.mem.eql(u8, field, "shared_projection") and !std.mem.eql(u8, field, "validation_proof")) {
                const omit = switch (comptime @field(ResolverConfigField, field)) {
                    .adjudicate_json, .suppress_json, .curation_json => @field(self, field).len == 0,
                    .namespace_curation_missing, .retired => !@field(self, field),
                    .namespace_curation_table_id, .namespace_curation_generation, .curation_revision, .merge_generation, .merge_state_version, .resolution_generation, .namespace_generation => @field(self, field) == 0,
                    .aliases, .admission_profile, .replacement_index, .replacement_resolver => @field(self, field).len == 0,
                    .promote => self.promote,
                    .inline_managed => self.inline_managed == null,
                    .retain_work => self.retain_work == .full,
                    .max_work_bytes => self.max_work_bytes == 1024 * 1024,
                    else => false,
                };
                if (!omit) {
                    try writer.objectField(field);
                    try writer.write(@field(self, field));
                }
            }
        }
        try writer.endObject();
    }

    /// Private execution/consumer wire includes legacy corrections that the
    /// declarative serializer intentionally excludes. Runtime ownership stays local.
    pub fn jsonExecution(self: ResolverConfig, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(ResolverConfig).@"struct".field_names) |field| {
            if (comptime !std.mem.eql(u8, field, "shared_projection") and !std.mem.eql(u8, field, "validation_proof")) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    /// Whether this resolver consumes mentions with the given label. An empty
    /// `labels` set is a catch-all.
    pub fn resolvesLabel(self: ResolverConfig, label: []const u8) bool {
        if (self.labels.len == 0) return true;
        for (self.labels) |l| if (std.mem.eql(u8, l, label)) return true;
        return false;
    }

    pub fn aliasesEqual(a: []const resolver_lib.KeyAlias, b: []const resolver_lib.KeyAlias) bool {
        if (a.len != b.len) return false;
        for (a, b) |x, y| if (!std.mem.eql(u8, x.key, y.key) or !std.mem.eql(u8, x.target, y.target)) return false;
        return true;
    }

    pub fn cloneAliases(alloc: Allocator, aliases: []const resolver_lib.KeyAlias) ![]const resolver_lib.KeyAlias {
        const out = try alloc.alloc(resolver_lib.KeyAlias, aliases.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |alias| {
                alloc.free(alias.key);
                alloc.free(alias.target);
            }
            alloc.free(out);
        }
        for (aliases, out) |alias, *dest| {
            const key = try alloc.dupe(u8, alias.key);
            errdefer alloc.free(key);
            dest.* = .{ .key = key, .target = try alloc.dupe(u8, alias.target) };
            initialized += 1;
        }
        return out;
    }

    /// Serving resolvers and each private replacement have separate sibling
    /// routing/identity views. A reservation name is exclusive while active;
    /// its generation is already fenced by catalog admission.
    pub fn sharesRoutingDomain(self: ResolverConfig, other: ResolverConfig) bool {
        return !self.retired and !other.retired and std.mem.eql(u8, self.replacement_index, other.replacement_index);
    }

    /// Graph names are reserved while a replacement is private. Its shadow
    /// uses that same name, so only that graph may consume staged decisions.
    /// Public contracts remain available to graphs with unchanged resolvers.
    pub fn visibleToGraph(self: ResolverConfig, index_name: []const u8) bool {
        return self.replacement_index.len == 0 or std.mem.eql(u8, self.replacement_index, index_name);
    }

    /// Pure source declaration: asynchronous authority projection clocks are
    /// checked separately through native head stamps.
    pub fn queryDefinitionFingerprint(cfg: ResolverConfig, a: Allocator) ![32]u8 {
        var copy = cfg;
        copy.merges = &.{};
        copy.curation_json = "";
        copy.curation_revision = 0;
        copy.merge_generation = 0;
        copy.merge_state_version = 0;
        copy.namespace_generation = 0;
        copy.namespace_curation_generation = 0;
        copy.namespace_curation_table_id = 0;
        copy.namespace_curation_missing = false;
        copy.shared_projection = null;
        copy.validation_proof = null;
        return resolver_lib.fingerprint(a, copy);
    }
    pub fn clone(alloc: Allocator, cfg: ResolverConfig) !ResolverConfig {
        var owned = cfg;
        owned.shared_projection = null;
        // Establish an empty cleanup owner before the first allocation. Keep
        // borrowed fields out of it until each clone has succeeded.
        inline for (@typeInfo(ResolverConfig).@"struct".field_names, @typeInfo(ResolverConfig).@"struct".field_types) |name, T| {
            if (T == []const u8 or T == []const []const u8 or T == []const resolver_lib.KeyAlias) @field(owned, name) = &.{};
        }
        errdefer owned.deinit(alloc);
        inline for (@typeInfo(ResolverConfig).@"struct".field_names, @typeInfo(ResolverConfig).@"struct".field_types) |name, T| {
            if (T == []const u8) {
                @field(owned, name) = if (@field(cfg, name).len > 0) try alloc.dupe(u8, @field(cfg, name)) else "";
            } else if (T == []const []const u8) {
                @field(owned, name) = try cloneLabels(alloc, @field(cfg, name));
            } else if (T == []const resolver_lib.KeyAlias) {
                @field(owned, name) = try cloneAliases(alloc, @field(cfg, name));
            }
        }
        return owned;
    }

    pub fn cloneWithProjection(alloc: Allocator, cfg: ResolverConfig, projection: *SharedProjection) !ResolverConfig {
        var detached = cfg;
        detached.merges = &.{};
        detached.curation_json = "";
        var owned = try clone(alloc, detached);
        projection.retain();
        owned.shared_projection = projection;
        owned.merges = projection.merges;
        owned.curation_json = projection.curation_json;
        return owned;
    }

    pub fn deinit(self: *ResolverConfig, alloc: Allocator) void {
        alloc.free(@constCast(self.name));
        alloc.free(@constCast(self.table));
        alloc.free(@constCast(self.source_artifact));
        alloc.free(@constCast(self.resolution_artifact));
        alloc.free(@constCast(self.key_template));
        for (self.aliases) |alias| {
            alloc.free(alias.key);
            alloc.free(alias.target);
        }
        alloc.free(self.aliases);
        if (self.shared_projection) |projection| {
            projection.release();
        } else {
            for (self.merges) |alias| {
                alloc.free(alias.key);
                alloc.free(alias.target);
            }
            alloc.free(self.merges);
            if (self.curation_json.len > 0) alloc.free(self.curation_json);
        }
        for (self.labels) |l| alloc.free(@constCast(l));
        if (self.labels.len > 0) alloc.free(self.labels);
        if (self.scorer_json.len > 0) alloc.free(@constCast(self.scorer_json));
        if (self.adjudicate_json.len > 0) alloc.free(self.adjudicate_json);
        if (self.suppress_json.len > 0) alloc.free(self.suppress_json);
        if (self.candidate_search.len > 0) alloc.free(@constCast(self.candidate_search));
        if (self.candidate_ann_index.len > 0) alloc.free(@constCast(self.candidate_ann_index));
        if (self.name_embedding.len > 0) alloc.free(@constCast(self.name_embedding));
        if (self.fusion_combine.len > 0) alloc.free(@constCast(self.fusion_combine));
        if (self.admission_profile.len > 0) alloc.free(self.admission_profile);
        if (self.replacement_index.len > 0) alloc.free(self.replacement_index);
        if (self.replacement_resolver.len > 0) alloc.free(self.replacement_resolver);
        self.* = undefined;
    }

    /// Reject a config that can never behave as intended. With fusion enabled:
    /// an unknown `fusion_combine` would silently fall back to the legacy fixed
    /// weight (fusion configured but doing nothing); `fusion_trust` of 0 (or a
    /// prior outside [0, 1]) collapses every edge weight toward 0, which a
    /// weighted traversal with a positive `min_weight` would silently drop.
    pub fn validate(self: ResolverConfig) !void {
        try self.validateDefinition();
        try self.validatePolicy();
    }
    pub fn validatePolicy(self: ResolverConfig) !void {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const a = arena.allocator();
        if (self.shared_projection) |projection| if (self.validation_proof) |proof| {
            const static_digest = try policyProfileDigest(a, self.aliases, self.suppress_json);
            if (self.merges.ptr == projection.merges.ptr and self.merges.len == projection.merges.len and self.curation_json.ptr == projection.curation_json.ptr and self.curation_json.len == projection.curation_json.len and std.mem.eql(u8, &proof.policy_digest, &projection.digest) and std.mem.eql(u8, &proof.static_digest, &static_digest)) return;
        };
        try @import("../resolver_authority.zig").validateProfile(a, .{ .key_template = self.key_template, .merges = self.merges, .state = try resolver_lib.curation.parseState(a, self.curation_json) }, self.aliases, self.suppress_json);
    }
    pub fn validateDefinition(self: ResolverConfig) !void {
        if (self.admission_profile.len != 0) return error.InvalidResolverConfig;
        if (self.adjudicate_json.len > 0) {
            if (self.scorer_json.len == 0) return error.InvalidResolverConfig;
            try @import("../../../functions/resolver_adjudication.zig").validate(std.heap.page_allocator, self.adjudicate_json);
            const policy = try resolver_lib.adjudication.Config.parse(std.heap.page_allocator, self.adjudicate_json);
            defer policy.deinit();
            if (policy.value.description_inputs.mention_artifact) |artifact| {
                if (self.source_artifact_kind != .asset or std.mem.eql(u8, artifact, self.source_artifact) or std.mem.eql(u8, artifact, self.resolution_artifact)) return error.InvalidResolverConfig;
            }
        }
        if (self.max_work_bytes > 16 * 1024 * 1024) return error.InvalidResolverConfig;
        if (self.merge_state_version > 2) return error.UnsupportedResolverCatalogVersion;
        if (self.name.len == 0 or
            self.table.len == 0 or
            self.source_artifact.len == 0 or
            self.resolution_artifact.len == 0 or
            self.key_template.len == 0)
        {
            return error.InvalidResolverConfig;
        }
        if (std.mem.eql(u8, self.source_artifact, self.resolution_artifact)) return error.InvalidResolverConfig;
        for (self.labels, 0..) |label, i| {
            if (label.len == 0) return error.InvalidResolverConfig;
            for (self.labels[0..i]) |prior| {
                if (std.mem.eql(u8, prior, label)) return error.InvalidResolverConfig;
            }
        }
        if (self.candidate_search.len > 0 and
            !std.mem.eql(u8, self.candidate_search, "exact_key") and
            !std.mem.eql(u8, self.candidate_search, "prefix") and
            !std.mem.eql(u8, self.candidate_search, "ann"))
        {
            return error.InvalidResolverConfig;
        }
        if (std.mem.eql(u8, self.candidate_search, "ann")) {
            if (self.candidate_ann_index.len == 0 and self.name_embedding.len == 0) return error.InvalidResolverConfig;
        }
        if (self.min_confidence) |floor| if (!(floor >= 0.0 and floor <= 1.0)) return error.InvalidResolverConfig;
        if (self.fusion_combine.len == 0) return;
        if (fusionStrategy(self.fusion_combine) == null) return error.InvalidResolverConfig;
        if (!(self.fusion_trust > 0.0 and self.fusion_trust <= 1.0)) return error.InvalidResolverConfig;
        if (self.fusion_prior < 0.0 or self.fusion_prior > 1.0) return error.InvalidResolverConfig;
        if (self.fusion_prior_weight < 0.0 or self.fusion_prior_weight > 1.0) return error.InvalidResolverConfig;
    }

    /// Provenance edge weight for one mention from this extractor. When the
    /// resolver declares a fusion strategy, the weight is `matcher.fuse` of this
    /// extractor's `fusion_trust * confidence` folded with the config-pinned
    /// prior; otherwise the legacy fixed 1.0. Single source of truth for both
    /// the sync and async mention-edge materializers.
    pub fn fusedMentionWeight(self: ResolverConfig, confidence: f64) f64 {
        const strategy = fusionStrategy(self.fusion_combine) orelse return 1.0;
        return matcher.fuse(
            strategy,
            &.{.{ .confidence = confidence, .trust = self.fusion_trust }},
            self.fusion_prior,
            self.fusion_prior_weight,
        );
    }
};

/// Extended catalogs carry an explicit envelope. Pre-epoch readers parse only
/// arrays, so they reject this shape even when they ignore unknown row fields.
/// Borrow the definition rows from a validated legacy array or reader envelope.
/// The returned view has the lifetime of the caller's parsed JSON document.
pub fn catalogRows(value: std.json.Value) !std.json.Value {
    if (value == .array) return value;
    if (value != .object) return error.InvalidResolverConfig;
    const version = value.object.get("version") orelse return error.InvalidResolverConfig;
    if (version != .integer or (version.integer < 2 or version.integer > supported_reader_version)) return error.UnsupportedResolverCatalogVersion;
    const rows = value.object.get("resolvers") orelse return error.InvalidResolverConfig;
    if (rows != .array) return error.InvalidResolverConfig;
    return rows;
}

pub fn serializeCatalog(alloc: Allocator, resolvers: []const ResolverConfig) ![]u8 {
    return serializeCatalogWithMinimumVersion(alloc, resolvers, 1);
}

/// Preserve a loaded catalog's envelope independently of the feature versions
/// inferred from its current definitions (which may now be empty).
const StoredProjection = struct { merges: []const resolver_lib.KeyAlias, curation_json: []const u8 };

pub fn serializeCatalogWithMinimumVersion(alloc: Allocator, resolvers: []const ResolverConfig, minimum: u32) ![]u8 {
    if (minimum == 0 or minimum > supported_reader_version) return error.UnsupportedResolverCatalogVersion;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var rows = std.json.Array.init(a);
    var projections: std.StringArrayHashMapUnmanaged([]const u8) = .empty;
    for (resolvers) |cfg| {
        var stored = cfg;
        if (cfg.namespace_curation_table_id != 0) {
            stored.merges = &.{};
            stored.curation_json = "";
        }
        var row = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, stored, .{}), .{});
        if (cfg.namespace_curation_table_id != 0) {
            const shared = if (cfg.shared_projection) |projection| if (cfg.merges.ptr == projection.merges.ptr and cfg.merges.len == projection.merges.len and cfg.curation_json.ptr == projection.curation_json.ptr and cfg.curation_json.len == projection.curation_json.len) projection else null else null;
            const encoded = if (shared) |projection| projection.encoded else try std.json.Stringify.valueAlloc(a, StoredProjection{ .merges = cfg.merges, .curation_json = cfg.curation_json }, .{});
            const id = if (shared) |projection| &projection.digest else blk: {
                var digest: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
                break :blk try a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
            };
            try projections.put(a, id, encoded);
            try row.object.put(a, "projection_ref", .{ .string = id });
        }
        try rows.append(row);
    }
    const encoded_rows = try std.json.Stringify.valueAlloc(a, std.json.Value{ .array = rows }, .{});
    const version = @max(minimum, try catalogReaderVersion(a, encoded_rows));
    if (version == 1) return alloc.dupe(u8, encoded_rows);
    var dictionary: std.json.ObjectMap = .empty;
    for (projections.keys(), projections.values()) |id, encoded| try dictionary.put(a, id, try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{}));
    return std.json.Stringify.valueAlloc(alloc, .{ .version = version, .resolvers = std.json.Value{ .array = rows }, .projections = std.json.Value{ .object = dictionary } }, .{});
}

pub fn deserializeCatalog(alloc: Allocator, data: []const u8) ![]ResolverConfig {
    const root = try std.json.parseFromSlice(std.json.Value, alloc, data, .{});
    defer root.deinit();
    const rows = try catalogRows(root.value);
    const parsed = try std.json.parseFromValue([]ResolverConfig, alloc, rows, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const out = try alloc.alloc(ResolverConfig, parsed.value.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*cfg| cfg.deinit(alloc);
        alloc.free(out);
    }
    var shared: std.StringHashMapUnmanaged(*SharedProjection) = .empty;
    defer {
        var owners = shared.valueIterator();
        while (owners.next()) |owner| owner.*.release();
        shared.deinit(alloc);
    }
    for (parsed.value, 0..) |input, i| {
        const cfg = input;
        if (rows.array.items[i].object.get("projection_ref")) |reference| {
            if (reference != .string or root.value != .object) return error.InvalidResolverConfig;
            const dictionary = root.value.object.get("projections") orelse return error.InvalidResolverConfig;
            if (dictionary != .object) return error.InvalidResolverConfig;
            const projection = dictionary.object.get(reference.string) orelse return error.InvalidResolverConfig;
            const slot = try shared.getOrPut(alloc, reference.string);
            if (!slot.found_existing) {
                errdefer _ = shared.remove(reference.string);
                const decoded = try std.json.parseFromValue(StoredProjection, alloc, projection, .{});
                defer decoded.deinit();
                const owned = try SharedProjection.create(alloc, decoded.value.merges, decoded.value.curation_json);
                errdefer owned.release();
                if (!std.mem.eql(u8, reference.string, &owned.digest)) return error.InvalidResolverConfig;
                slot.value_ptr.* = owned;
            }
            if (cfg.merge_state_version > 2) return error.UnsupportedResolverCatalogVersion;
            if (cfg.retired and cfg.inline_managed != true) return error.InvalidResolverConfig;
            out[i] = try ResolverConfig.cloneWithProjection(alloc, cfg, slot.value_ptr.*);
            initialized += 1;
            continue;
        }
        if (cfg.merge_state_version > 2) return error.UnsupportedResolverCatalogVersion;
        if (cfg.retired and cfg.inline_managed != true) return error.InvalidResolverConfig;
        out[i] = try ResolverConfig.clone(alloc, cfg);
        initialized += 1;
    }
    return out;
}

/// Execution plans exclude retired identities; backups and catalog comparisons
/// use deserializeCatalog so durable merges and namespace fences remain intact.
pub fn deserializeActiveCatalog(alloc: Allocator, data: []const u8) ![]ResolverConfig {
    const all = try deserializeCatalog(alloc, data);
    defer alloc.free(all);
    var active: std.ArrayList(ResolverConfig) = .empty;
    errdefer {
        for (active.items) |*cfg| cfg.deinit(alloc);
        active.deinit(alloc);
    }
    // Allocate before transferring ownership so failure frees every definition.
    active.ensureTotalCapacity(alloc, all.len) catch |err| {
        for (all) |*cfg| cfg.deinit(alloc);
        return err;
    };
    for (all) |*cfg| {
        if (cfg.retired) cfg.deinit(alloc) else active.appendAssumeCapacity(cfg.*);
    }
    return try active.toOwnedSlice(alloc);
}

pub const merge_catalog_key = "\x00\x00__metadata__:resolver_merges_v1";
const MergeState = struct { name: []const u8, aliases: []const resolver_lib.KeyAlias };

/// Validate a portable metadata record before publishing imported state.
pub fn validateSerializedMergeState(alloc: Allocator, data: []const u8) !void {
    var parsed = try std.json.parseFromSlice([]const MergeState, alloc, data, .{});
    defer parsed.deinit();
    for (parsed.value, 0..) |state, index| {
        if (state.name.len == 0 or state.aliases.len == 0) return error.InvalidResolverMergeState;
        for (parsed.value[0..index]) |previous| if (std.mem.eql(u8, previous.name, state.name)) return error.InvalidResolverMergeState;
        try resolver_lib.validateAliases(state.aliases);
    }
}

pub fn serializeMergeState(alloc: Allocator, configs: []const ResolverConfig) ![]u8 {
    var states: std.ArrayList(MergeState) = .empty;
    defer states.deinit(alloc);
    for (configs) |cfg| if (cfg.merges.len > 0 and cfg.namespace_curation_table_id == 0) {
        try states.append(alloc, .{ .name = cfg.name, .aliases = cfg.merges });
    };
    return try std.json.Stringify.valueAlloc(alloc, states.items, .{});
}

pub fn applyMergeState(alloc: Allocator, configs: []ResolverConfig, data: []const u8) !void {
    var parsed = try std.json.parseFromSlice([]MergeState, alloc, data, .{});
    defer parsed.deinit();
    for (parsed.value) |state| {
        var found = false;
        for (configs) |*cfg| {
            if (!std.mem.eql(u8, cfg.name, state.name)) continue;
            if (cfg.merges.len > 0 or (cfg.merge_state_version == 0 or cfg.merge_state_version > 2) or cfg.merge_generation == 0 or state.aliases.len == 0) return error.InvalidResolverMergeState;
            cfg.merges = try ResolverConfig.cloneAliases(alloc, state.aliases);
            try cfg.validate();
            found = true;
            break;
        }
        if (!found) return error.InvalidResolverMergeState;
    }
    for (configs) |cfg| if ((cfg.merge_generation > 0 or cfg.merge_state_version == 1) and cfg.merges.len == 0) return error.MissingResolverMergeState;
}

test "resolver catalog round trip" {
    const alloc = std.testing.allocator;

    const encoded = try serializeCatalog(alloc, &.{
        .{
            .name = "knowledge_graph",
            .table = "entities",
            .source_artifact = "relations_v1",
            .source_artifact_kind = .chunk,
            .resolution_artifact = "resolution_v1",
            .key_template = "{{ lower _entity.label }}/{{ slug _entity.canonical_text }}",
            .scorer_json = "{\"comparisons\":[],\"combine\":{\"bias\":-3.0},\"decision\":{\"match\":0.9}}",
            .config_generation = 7,
        },
        .{
            .name = "people_only",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "people_resolution_v1",
            .key_template = "{{ slug _entity.text }}",
            .type_must_match = false,
        },
    });
    defer alloc.free(encoded);

    const decoded = try deserializeCatalog(alloc, encoded);
    defer {
        for (decoded) |*cfg| cfg.deinit(alloc);
        alloc.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 2), decoded.len);
    try std.testing.expectEqualStrings("knowledge_graph", decoded[0].name);
    try std.testing.expectEqualStrings("entities", decoded[0].table);
    try std.testing.expectEqualStrings("relations_v1", decoded[0].source_artifact);
    try std.testing.expectEqual(ResolverSourceArtifactKind.chunk, decoded[0].source_artifact_kind);
    try std.testing.expectEqualStrings("resolution_v1", decoded[0].resolution_artifact);
    try std.testing.expectEqualStrings("{{ lower _entity.label }}/{{ slug _entity.canonical_text }}", decoded[0].key_template);
    try std.testing.expect(decoded[0].type_must_match);
    try std.testing.expectEqual(@as(u64, 7), decoded[0].config_generation);
    try std.testing.expect(decoded[0].scorer_json.len > 0);

    try std.testing.expectEqualStrings("people_only", decoded[1].name);
    try std.testing.expectEqual(ResolverSourceArtifactKind.asset, decoded[1].source_artifact_kind);
    try std.testing.expect(!decoded[1].type_must_match);
    try std.testing.expectEqual(@as(usize, 0), decoded[1].scorer_json.len);
    try std.testing.expectEqual(@as(u64, 0), decoded[1].config_generation);
}

test "resolver catalog round trip preserves order and empty list" {
    const alloc = std.testing.allocator;

    const empty = try serializeCatalog(alloc, &.{});
    defer alloc.free(empty);
    const decoded_empty = try deserializeCatalog(alloc, empty);
    defer alloc.free(decoded_empty);
    try std.testing.expectEqual(@as(usize, 0), decoded_empty.len);
}

test "resolver catalog defaults legacy source artifact kind to asset" {
    const alloc = std.testing.allocator;

    const decoded = try deserializeCatalog(alloc,
        \\[
        \\  {
        \\    "name": "legacy",
        \\    "table": "entities",
        \\    "source_artifact": "relations_v1",
        \\    "resolution_artifact": "resolution_v1",
        \\    "key_template": "{{ slug _entity.text }}"
        \\  }
        \\]
    );
    defer {
        for (decoded) |*cfg| cfg.deinit(alloc);
        alloc.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqual(ResolverSourceArtifactKind.asset, decoded[0].source_artifact_kind);
}

test "resolver config validates fusion strategy and folds confidence into the weight" {
    const base = ResolverConfig{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ _entity.text }}",
    };

    // No fusion: legacy fixed weight, always valid.
    try base.validate();
    try std.testing.expectEqual(@as(f64, 1.0), base.fusedMentionWeight(0.5));

    // Valid fusion: weight is the fused confidence (noisy_or, trust 0.9,
    // confidence 0.8, no prior -> 1 - (1 - 0.72) = 0.72).
    var fused = base;
    fused.fusion_combine = "noisy_or";
    fused.fusion_trust = 0.9;
    try fused.validate();
    try std.testing.expectApproxEqAbs(@as(f64, 0.72), fused.fusedMentionWeight(0.8), 1e-9);

    // Unknown strategy is rejected, not silently treated as "fusion off".
    var bad_strategy = base;
    bad_strategy.fusion_combine = "noisyor";
    try std.testing.expectError(error.InvalidResolverConfig, bad_strategy.validate());

    // Zero trust would collapse every edge weight toward 0; rejected.
    var zero_trust = base;
    zero_trust.fusion_combine = "mean";
    zero_trust.fusion_trust = 0.0;
    try std.testing.expectError(error.InvalidResolverConfig, zero_trust.validate());

    // Prior outside [0, 1] is rejected.
    var bad_prior = base;
    bad_prior.fusion_combine = "max";
    bad_prior.fusion_prior = 1.5;
    try std.testing.expectError(error.InvalidResolverConfig, bad_prior.validate());
}

test "resolver catalog round trips label routing and filters by label" {
    const alloc = std.testing.allocator;

    const encoded = try serializeCatalog(alloc, &.{
        .{
            .name = "events",
            .table = "events",
            .source_artifact = "kg_v1",
            .resolution_artifact = "events_resolution_v1",
            .key_template = "event/{{ hash _entity.text }}",
            .labels = &.{"event"},
        },
        .{
            .name = "entities",
            .table = "entities",
            .source_artifact = "kg_v1",
            .resolution_artifact = "entities_resolution_v1",
            .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        },
    });
    defer alloc.free(encoded);

    const decoded = try deserializeCatalog(alloc, encoded);
    defer {
        for (decoded) |*cfg| cfg.deinit(alloc);
        alloc.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 2), decoded.len);
    try std.testing.expectEqual(@as(usize, 1), decoded[0].labels.len);
    try std.testing.expectEqualStrings("event", decoded[0].labels[0]);
    try std.testing.expect(decoded[0].resolvesLabel("event"));
    try std.testing.expect(!decoded[0].resolvesLabel("person"));
    // Catch-all: empty labels consume every mention label.
    try std.testing.expectEqual(@as(usize, 0), decoded[1].labels.len);
    try std.testing.expect(decoded[1].resolvesLabel("event"));
    try std.testing.expect(decoded[1].resolvesLabel("person"));
    try std.testing.expect(decoded[0].eql(decoded[0]));
    try std.testing.expect(!decoded[0].eql(decoded[1]));
}

test "resolver config rejects empty and duplicate labels" {
    const base = ResolverConfig{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ _entity.text }}",
    };

    var empty_label = base;
    empty_label.labels = &.{""};
    try std.testing.expectError(error.InvalidResolverConfig, empty_label.validate());

    var duplicate_label = base;
    duplicate_label.labels = &.{ "event", "event" };
    try std.testing.expectError(error.InvalidResolverConfig, duplicate_label.validate());

    var routed = base;
    routed.labels = &.{ "event", "person" };
    try routed.validate();
}

test "resolver config validates required references and candidate mode" {
    const base = ResolverConfig{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ _entity.text }}",
    };

    try base.validate();

    var missing_name = base;
    missing_name.name = "";
    try std.testing.expectError(error.InvalidResolverConfig, missing_name.validate());

    var missing_table = base;
    missing_table.table = "";
    try std.testing.expectError(error.InvalidResolverConfig, missing_table.validate());

    var missing_source = base;
    missing_source.source_artifact = "";
    try std.testing.expectError(error.InvalidResolverConfig, missing_source.validate());

    var missing_resolution = base;
    missing_resolution.resolution_artifact = "";
    try std.testing.expectError(error.InvalidResolverConfig, missing_resolution.validate());

    var missing_key_template = base;
    missing_key_template.key_template = "";
    try std.testing.expectError(error.InvalidResolverConfig, missing_key_template.validate());

    var recursive_artifact = base;
    recursive_artifact.resolution_artifact = recursive_artifact.source_artifact;
    try std.testing.expectError(error.InvalidResolverConfig, recursive_artifact.validate());

    var bad_candidate_mode = base;
    bad_candidate_mode.candidate_search = "nearest";
    try std.testing.expectError(error.InvalidResolverConfig, bad_candidate_mode.validate());

    var exact_key = base;
    exact_key.candidate_search = "exact_key";
    try exact_key.validate();

    var prefix = base;
    prefix.candidate_search = "prefix";
    try prefix.validate();

    var ann_missing_reference = base;
    ann_missing_reference.candidate_search = "ann";
    try std.testing.expectError(error.InvalidResolverConfig, ann_missing_reference.validate());

    var ann_with_index = base;
    ann_with_index.candidate_search = "ann";
    ann_with_index.candidate_ann_index = "entity_name_embedding";
    try ann_with_index.validate();
}

test "capi resolver catalog preserves legacy defaults and versions durable merge state" {
    const alloc = std.testing.allocator;
    var cfg = ResolverConfig{ .name = "r", .table = "default", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "{{ uslug _entity.text }}", .config_generation = 1 };
    const legacy = try serializeCatalog(alloc, &.{cfg});
    defer alloc.free(legacy);
    for ([_][]const u8{ "aliases", "promote", "merges", "merge_generation", "merge_state_version", "resolution_generation", "namespace_generation" }) |field| try std.testing.expect(std.mem.indexOf(u8, legacy, field) == null);
    cfg.merges = &.{.{ .key = "a", .target = "b" }};
    cfg.merge_generation = 2;
    cfg.merge_state_version = 1;
    const catalog = try serializeCatalog(alloc, &.{cfg});
    defer alloc.free(catalog);
    const old_reader = std.json.parseFromSlice([]ResolverConfig, alloc, catalog, .{ .ignore_unknown_fields = true });
    try std.testing.expectError(error.UnexpectedToken, old_reader);
    const sidecar = try serializeMergeState(alloc, &.{cfg});
    defer alloc.free(sidecar);
    const restored = try deserializeCatalog(alloc, catalog);
    defer {
        for (restored) |*entry| entry.deinit(alloc);
        alloc.free(restored);
    }
    try std.testing.expectError(error.MissingResolverMergeState, applyMergeState(alloc, restored, "[]"));
    try applyMergeState(alloc, restored, sidecar);
    try std.testing.expectEqualStrings("b", restored[0].merges[0].target);
    try std.testing.expectEqual(@as(u64, 2), restored[0].effectiveGeneration());
    const future = "[{\"name\":\"r\",\"table\":\"default\",\"source_artifact\":\"kg\",\"resolution_artifact\":\"resolved\",\"key_template\":\"x\",\"future_optional_note\":true}]";
    const forward = try deserializeCatalog(alloc, future);
    defer {
        for (forward) |*entry| entry.deinit(alloc);
        alloc.free(forward);
    }
    const unsupported = "[{\"name\":\"r\",\"table\":\"default\",\"source_artifact\":\"kg\",\"resolution_artifact\":\"resolved\",\"key_template\":\"x\",\"merge_state_version\":3}]";
    try std.testing.expectError(error.UnsupportedResolverCatalogVersion, deserializeCatalog(alloc, unsupported));
}

test "capi retired resolver identities retain merges without entering execution catalogs" {
    const a = std.testing.allocator;
    var cfg: ResolverConfig = .{ .name = "r", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "{{ uslug _entity.text }}", .inline_managed = true, .retired = true };
    const encoded = try serializeCatalog(a, &.{cfg});
    defer a.free(encoded);
    const active = try deserializeActiveCatalog(a, encoded);
    defer a.free(active);
    try std.testing.expectEqual(@as(usize, 0), active.len);
    const identities = try deserializeCatalog(a, encoded);
    defer {
        for (identities) |*entry| entry.deinit(a);
        a.free(identities);
    }
    try applyMergeState(a, identities, "[]");
    try std.testing.expect(identities[0].retired);
    cfg.merges = &.{.{ .key = "a", .target = "b" }};
    cfg.merge_generation = 1;
    cfg.merge_state_version = 1;
    const merged = try serializeCatalog(a, &.{cfg});
    defer a.free(merged);
    const sidecar = try serializeMergeState(a, &.{cfg});
    defer a.free(sidecar);
    const restored = try deserializeCatalog(a, merged);
    defer {
        for (restored) |*entry| entry.deinit(a);
        a.free(restored);
    }
    try applyMergeState(a, restored, sidecar);
    try std.testing.expectEqualStrings("b", restored[0].merges[0].target);
}

test "capi resolver source artifact scopes overlap symmetrically" {
    const kinds = [_]ResolverSourceArtifactKind{ .asset, .chunk, .any };
    for (kinds) |left| {
        for (kinds) |right| {
            const expected = left == right or left == .any or right == .any;
            try std.testing.expectEqual(expected, left.overlaps(right));
            try std.testing.expectEqual(left.overlaps(right), right.overlaps(left));
        }
    }
}

test "capi resolver reader epochs fence older readers and remain monotonic" {
    const Txn = struct {
        value: ?[]const u8 = null,
        pub fn get(self: *@This(), _: []const u8) anyerror![]const u8 {
            return self.value orelse error.NotFound;
        }
        pub fn put(self: *@This(), _: []const u8, value: []const u8) !void {
            self.value = value;
        }
    };
    const a = std.testing.allocator;
    var txn: Txn = .{};
    try std.testing.expectEqual(@as(u32, 1), try readMinimumReaderVersion(&txn, 1));
    try std.testing.expectEqual(@as(u32, 1), try stampMinimumReaderVersion(a, &txn, "[]"));
    try std.testing.expectEqual(@as(u32, 2), try stampMinimumReaderVersion(a, &txn, "[{\"inline_managed\":true}]"));
    try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, readMinimumReaderVersion(&txn, 1));
    try std.testing.expectEqual(@as(u32, 2), try readMinimumReaderVersion(&txn, 2));
    try std.testing.expectEqual(@as(u32, 2), try stampMinimumReaderVersion(a, &txn, "[]"));
    txn.value = "3";
    try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, readMinimumReaderVersion(&txn, 2));
}

test "graph ttl migration catalog epoch remains authoritative with replacement fields" {
    const a = std.testing.allocator;
    const old = "{\"version\":3,\"resolvers\":[{\"replacement_index\":\"shadow\"}]}";
    const fenced = try versionedCatalog(a, old, 4);
    defer a.free(fenced);
    try std.testing.expectEqual(@as(u32, 4), try catalogReaderVersion(a, fenced));
    try std.testing.expectEqualStrings("4", try readerVersionBytes(4));
    try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, readerVersionBytes(supported_reader_version + 1));
}

test "resolver adjudication catalogs fence old readers and redact public credentials" {
    const a = std.testing.allocator;
    const raw =
        \\{"name":"concepts","table":"entities","source_artifact":"kg","resolution_artifact":"resolved","key_template":"{{_entity.text}}","scorer":{"comparisons":[]},"adjudicate":{"decider":{"provider":"antfly","model":"fixture","api_key":"private-key"},"template":"{{m.text}} / {{c.canonical_name}}","question":{"type":"choice","name":"same","instructions":"Are these the same entity?","choices":[{"value":"same"},{"value":"different"}]},"accept":{"choice":"same","min_probability":0.9}}}
    ;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const value = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), raw, .{});
    const parsed = try ResolverConfig.parseValue(a, value);
    defer parsed.deinit();
    try parsed.value.validate();
    const catalog = try serializeCatalog(a, &.{parsed.value});
    defer a.free(catalog);
    try std.testing.expectEqual(adjudication_reader_version, try catalogReaderVersion(a, catalog));
    const reopened = try deserializeCatalog(a, catalog);
    defer {
        for (reopened) |*cfg| cfg.deinit(a);
        a.free(reopened);
    }
    try std.testing.expectEqualStrings(parsed.value.adjudicate_json, reopened[0].adjudicate_json);
    const public = try parsed.value.publicValue(arena.allocator());
    const public_json = try std.json.Stringify.valueAlloc(a, public, .{});
    defer a.free(public_json);
    try std.testing.expect(std.mem.indexOf(u8, public_json, "private-key") == null);
    try std.testing.expect(public.object.contains("adjudicate"));
}

test "ordered artifact inventory resolver catalog serialization preserves explicit envelope" {
    const a = std.testing.allocator;
    for (1..supported_reader_version + 1) |reader_version| {
        const version: u32 = @intCast(reader_version);
        const encoded = try serializeCatalogWithMinimumVersion(a, &.{}, version);
        defer a.free(encoded);
        try std.testing.expectEqual(version, try catalogReaderVersion(a, encoded));
        const definitions = try deserializeCatalog(a, encoded);
        defer a.free(definitions);
        try std.testing.expectEqual(@as(usize, 0), definitions.len);
        const roundtrip = try serializeCatalogWithMinimumVersion(a, definitions, version);
        defer a.free(roundtrip);
        try std.testing.expectEqualStrings(encoded, roundtrip);
    }
    try std.testing.expectError(error.UnsupportedResolverCatalogVersion, serializeCatalogWithMinimumVersion(a, &.{}, 0));
    try std.testing.expectError(error.UnsupportedResolverCatalogVersion, serializeCatalogWithMinimumVersion(a, &.{}, supported_reader_version + 1));
}

test "resolver public inspection redacts nested generated provider credentials without mutating execution" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const policy =
        \\{"decider":{"decision_method":"generated","generator":{"provider":"openai","model":"gpt-4.1","api_key":"nested-private-key","credentials_path":"private-credentials-path","capability_token":"private-capability","url":"https://user:password@provider.example/v1","fallbacks":[{"api_key":"fallback-private-key","endpoint":"https://provider.example/v1?access%5Ftoken=private-query"},{"url":"https://safe.example/v1","connection_id":"hosted"}]},"instructions":"Compare these concepts"}}
    ;
    const cfg = ResolverConfig{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "{{_entity.text}}", .adjudicate_json = policy };
    const public = try cfg.publicValue(a);
    const encoded = try std.json.Stringify.valueAlloc(a, public, .{});
    for ([_][]const u8{ "nested-private-key", "private-credentials-path", "private-capability", "password", "fallback-private-key", "private-query", "api_key", "credentials_path", "capability_token" }) |secret| {
        try std.testing.expect(std.mem.indexOf(u8, encoded, secret) == null);
    }
    try std.testing.expect(std.mem.indexOf(u8, encoded, "https://safe.example/v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "gpt-4.1") != null);
    try std.testing.expect(std.mem.indexOf(u8, cfg.adjudicate_json, "nested-private-key") != null);
}

test "resolver curation state requires the current receipt decoder" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const curation = @import("antfly_resolver").curation;
    const empty = try std.json.Stringify.valueAlloc(a, .{.{ .curation_json = try std.json.Stringify.valueAlloc(a, curation.State{}, .{}) }}, .{});
    try std.testing.expectEqual(scoped_curation_reader_version, try catalogReaderVersion(a, empty));
    const scoped = try std.json.Stringify.valueAlloc(a, .{.{ .curation_json = try std.json.Stringify.valueAlloc(a, curation.State{ .receipt_scope = @splat('a') }, .{}) }}, .{});
    try std.testing.expectEqual(scoped_curation_reader_version, try catalogReaderVersion(a, scoped));
    try std.testing.expectEqualStrings("11", try readerVersionBytes(scoped_curation_reader_version));
}

test "resolver catalog native projection clock follows its physical namespace" {
    const base: ResolverConfig = .{ .name = "concepts", .table = "old", .key_template = "entity/{{_key}}", .source_artifact = "kg", .resolution_artifact = "resolved", .curation_revision = 100, .namespace_curation_generation = 100, .namespace_curation_table_id = 7, .resolution_generation = 200 };
    var next = base;
    next.table = "new";
    next.namespace_curation_table_id = 8;
    next.namespace_curation_generation = 1;
    try next.rebaseNativeProjection(base);
    try std.testing.expectEqual(@as(u64, 1), next.curation_revision);
    try std.testing.expectEqual(@as(u64, 200), next.resolution_generation);
    next = base;
    next.namespace_curation_generation = 99;
    try std.testing.expectError(error.ResolverCurationConflict, next.rebaseNativeProjection(base));
}

test "resolver catalog stores shared native policy once per owner snapshot" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const first: ResolverConfig = .{ .name = "first", .table = "entities", .key_template = "{{_key}}", .source_artifact = "kg", .resolution_artifact = "resolved", .namespace_curation_table_id = 7, .namespace_curation_generation = 1, .curation_revision = 1, .merges = &.{.{ .key = "a", .target = "b" }}, .curation_json = "{\"suppress\":{\"keys\":[\"noise\"]}}" };
    var second = first;
    second.name = "second";
    second.resolution_artifact = "resolved2";
    const raw = try serializeCatalog(a, &.{ first, second });
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
    try std.testing.expectEqual(@as(usize, 1), root.object.get("projections").?.object.count());
    const decoded = try deserializeCatalog(a, raw);
    try std.testing.expectEqual(@as(usize, 2), decoded.len);
    for (decoded) |cfg| {
        try std.testing.expectEqualStrings(first.curation_json, cfg.curation_json);
        try std.testing.expect(ResolverConfig.aliasesEqual(first.merges, cfg.merges));
    }
}

test "resolver catalog shares immutable native policy in memory and after reopen" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const aliases = try scratch.alloc(resolver_lib.KeyAlias, 1000);
    for (aliases, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(scratch, "key-{d}", .{i}), .target = "canonical" };
    const projection = try SharedProjection.create(a, aliases, "{}");
    defer projection.release();
    var configs: [20]ResolverConfig = undefined;
    var count: usize = 0;
    defer for (configs[0..count]) |*cfg| cfg.deinit(a);
    for (&configs, 0..) |*cfg, i| {
        cfg.* = try ResolverConfig.cloneWithProjection(a, .{ .name = try std.fmt.allocPrint(scratch, "resolver-{d}", .{i}), .table = "entities", .source_artifact = "source", .resolution_artifact = "resolved", .key_template = "{{_key}}", .namespace_curation_table_id = 7, .namespace_curation_generation = 1 }, projection);
        count += 1;
        try std.testing.expect(cfg.merges.ptr == projection.merges.ptr);
    }
    var detached = try ResolverConfig.clone(a, configs[0]);
    defer detached.deinit(a);
    try std.testing.expect(detached.shared_projection == null);
    try std.testing.expect(detached.merges.ptr != projection.merges.ptr);
    try std.testing.expect(configs[0].eql(detached));
    const raw = try serializeCatalog(a, &configs);
    defer a.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "shared_projection") == null);
    const restored = try deserializeCatalog(a, raw);
    defer {
        for (restored) |*cfg| cfg.deinit(a);
        a.free(restored);
    }
    for (restored) |cfg| {
        try std.testing.expect(cfg.shared_projection == restored[0].shared_projection);
        try std.testing.expect(cfg.merges.ptr == restored[0].merges.ptr);
        try std.testing.expect(ResolverConfig.aliasesEqual(aliases, cfg.merges));
    }
    // No JSON input can manufacture a runtime ownership reference.
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(ResolverConfig, a, "{\"name\":\"r\",\"table\":\"t\",\"source_artifact\":\"s\",\"resolution_artifact\":\"o\",\"key_template\":\"{{_key}}\",\"shared_projection\":{}}", .{}));
}

test "resolver curation reader floors encode every supported version" {
    for (1..supported_reader_version + 1) |version| {
        const encoded = try readerVersionBytes(@intCast(version));
        try std.testing.expectEqual(version, try std.fmt.parseInt(usize, encoded, 10));
    }
    try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, readerVersionBytes(0));
    try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, readerVersionBytes(supported_reader_version + 1));
}
