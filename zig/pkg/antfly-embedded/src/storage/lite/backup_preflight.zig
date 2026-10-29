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

//! Dependency inventory is independent of destination evaluation. No producer,
//! template helper, network client, or target database is executed here.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Value = std.json.Value;
const codec = @import("../backup_codec.zig");
const portable = @import("../portable_backup.zig");
const artifact_ids = @import("../db/artifact/ids.zig");
const canonical = @import("../db/document/content_hash.zig");
const staging = @import("restore_staging.zig");
const resolver_catalog = @import("../db/catalog/resolver_catalog.zig");
const ProducerType = @import("../db/enrichment/asset_producer.zig").ProducerType;
const enrichment_types = @import("../db/enrichment/enrichment_types.zig");
const hbs = @import("handlebars");
const template_runtime = @import("../../template.zig");
const chunking_types = @import("../../chunking/types.zig");

pub const report_version = 1;
pub const inspector_version = "antfly-backup-preflight/1";
pub const Digest = [64]u8;
pub const Verdict = enum { compatible, blocked, unknown };
pub const Kind = enum { embedded, source_local, source_external, source_expiring, archive_retrieval, page_retrieval, connection, replication_source, model, producer_model, dynamic_loader, source_unresolved };
pub const Options = struct {
    max_input_bytes: u64 = 64 * 1024 * 1024 * 1024,
    max_expanded_bytes: u64 = 128 * 1024 * 1024 * 1024,
    max_definitions: usize = 4096,
    max_requirements: usize = 4096,
    max_examples: usize = 32,
    max_metadata_bytes: usize = 16 * 1024 * 1024,
    max_document_bytes: usize = 64 * 1024 * 1024,
    cancellation: @import("antfly_cancellation").CancellationToken = .none,
};

/// Caller-supplied, versioned evidence, never inferred from model names or
/// stored-vector availability. Digests identify the exact source requirements.
pub const Capability = struct {
    requirement_sha256: []const u8,
    supported: bool,
    query_embedding: bool = false,
    future_ingestion: bool = false,
    model_revision_verified: bool = false,
    preprocessing_verified: bool = false,
    vector_space_verified: bool = false,
    dimensions: u32 = 0,
};
pub const Destination = struct {
    version: u32,
    configuration_id: []const u8,
    requirements: []const Capability = &.{},
};
pub const Example = struct { document_id: []const u8, field: []const u8 };
pub const Requirement = struct {
    kind: Kind,
    sha256: Digest,
    count: u64 = 0,
    dimensions: u32 = 0,
    examples: std.ArrayListUnmanaged(Example) = .empty,

    pub fn jsonStringify(self: @This(), j: anytype) @TypeOf(j.*).Error!void {
        try j.beginObject();
        try j.objectField("kind");
        try j.write(self.kind);
        try j.objectField("sha256");
        try j.write(self.sha256[0..]);
        try j.objectField("count");
        try j.write(self.count);
        try j.objectField("dimensions");
        try j.write(self.dimensions);
        try j.objectField("examples");
        try j.write(self.examples.items);
        try j.endObject();
    }
};
pub const Evaluation = struct { requirement_sha256: []const u8, verdict: Verdict, reason_code: []const u8, remediation: []const u8 };
pub const Report = struct {
    allocator: Allocator,
    format: []const u8 = "unknown",
    format_version: u32 = 0,
    source_sha256: ?Digest = null,
    source_bytes: u64 = 0,
    scan_complete: bool = false,
    analysis_complete: bool = true,
    counts_complete: bool = false,
    failure_code: ?[]const u8 = null,
    documents: u64 = 0,
    metadata_records: u64 = 0,
    artifact_records: u64 = 0,
    shards: u64 = 0,
    requirements: std.ArrayListUnmanaged(Requirement) = .empty,
    examples: usize = 0,

    pub fn deinit(self: *@This()) void {
        for (self.requirements.items) |*r| {
            for (r.examples.items) |e| {
                self.allocator.free(e.document_id);
                self.allocator.free(e.field);
            }
            r.examples.deinit(self.allocator);
        }
        self.requirements.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn add(self: *@This(), options: Options, kind: Kind, identity: []const u8, doc: []const u8, field: []const u8, dimensions: u32) !void {
        try options.cancellation.check();
        const digest = requirementHash(kind, identity);
        var found: ?*Requirement = null;
        for (self.requirements.items) |*r| if (r.kind == kind and std.mem.eql(u8, &r.sha256, &digest)) {
            found = r;
            break;
        };
        const r = found orelse blk: {
            if (self.requirements.items.len >= options.max_requirements) return error.PreflightLimitExceeded;
            try self.requirements.append(self.allocator, .{ .kind = kind, .sha256 = digest, .dimensions = dimensions });
            break :blk &self.requirements.items[self.requirements.items.len - 1];
        };
        r.count = try std.math.add(u64, r.count, 1);
        if (kind == .dynamic_loader or kind == .source_unresolved) self.analysis_complete = false;
        if (self.examples < options.max_examples and r.examples.items.len < 4) {
            const id = try safeLabel(self.allocator, doc);
            errdefer self.allocator.free(id);
            const name = try safeLabel(self.allocator, field);
            errdefer self.allocator.free(name);
            try r.examples.append(self.allocator, .{ .document_id = id, .field = name });
            self.examples += 1;
        }
    }

    pub fn jsonAlloc(self: *const @This(), destination_json: ?[]const u8) ![]u8 {
        var parsed: ?std.json.Parsed(Destination) = null;
        defer if (parsed) |*p| p.deinit();
        if (destination_json) |bytes| {
            if (bytes.len > 1024 * 1024) return error.PreflightLimitExceeded;
            parsed = try std.json.parseFromSlice(Destination, self.allocator, bytes, .{});
            if (parsed.?.value.version != 1 or parsed.?.value.configuration_id.len == 0) return error.InvalidDestinationCapabilities;
            for (parsed.?.value.requirements, 0..) |cap, i| {
                if (cap.requirement_sha256.len != 64) return error.InvalidDestinationCapabilities;
                for (cap.requirement_sha256) |c| if (!std.ascii.isHex(c) or std.ascii.isUpper(c)) return error.InvalidDestinationCapabilities;
                for (parsed.?.value.requirements[0..i]) |prior| if (std.mem.eql(u8, prior.requirement_sha256, cap.requirement_sha256)) return error.InvalidDestinationCapabilities;
            }
        }
        const destination: ?Destination = if (parsed) |p| p.value else null;
        var evaluations: std.ArrayListUnmanaged(Evaluation) = .empty;
        defer evaluations.deinit(self.allocator);
        var verdict: Verdict = if (!self.scan_complete or !self.analysis_complete or destination == null) .unknown else .compatible;
        for (self.requirements.items) |*r| {
            const e = evaluate(r, destination);
            try evaluations.append(self.allocator, e);
            if (e.verdict == .blocked) verdict = .blocked else if (e.verdict == .unknown and verdict == .compatible) verdict = .unknown;
        }
        const dest_digest = if (destination_json) |bytes| hash(bytes) else null;
        return std.json.Stringify.valueAlloc(self.allocator, .{
            .report_version = report_version,
            .inspector_version = inspector_version,
            .source = .{ .format = self.format, .format_version = self.format_version, .size_bytes = self.source_bytes, .sha256 = if (self.source_sha256) |*d| d[0..] else null },
            .completeness = .{ .scan_complete = self.scan_complete, .dependency_analysis_complete = self.analysis_complete, .counts_complete = self.counts_complete, .failure_code = self.failure_code },
            .inventory = .{ .documents = self.documents, .metadata_records = self.metadata_records, .artifact_records = self.artifact_records, .shards = self.shards, .requirements = self.requirements.items },
            .destination = .{ .capabilities_sha256 = if (dest_digest) |*d| d[0..] else null, .supplied = destination != null },
            .compatibility = .{ .verdict = verdict, .evaluations = evaluations.items },
        }, .{});
    }
};

pub fn evaluate(r: *const Requirement, destination: ?Destination) Evaluation {
    var e: Evaluation = .{ .requirement_sha256 = &r.sha256, .verdict = .unknown, .reason_code = "destination_unknown", .remediation = "Supply current destination capabilities for this exact requirement digest." };
    switch (r.kind) {
        .embedded => {
            e.verdict = .compatible;
            e.reason_code = "embedded";
            e.remediation = "No external source access is required.";
            return e;
        },
        .source_local => {
            e.verdict = .blocked;
            e.reason_code = "source_local";
            e.remediation = "Replace the active source with durable destination-accessible content before creating a new backup.";
            return e;
        },
        .source_expiring => {
            e.verdict = .blocked;
            e.reason_code = "source_expiring";
            e.remediation = "Use a durable source and destination-managed credentials; temporary access URLs cannot support future ingestion.";
            return e;
        },
        .dynamic_loader, .source_unresolved => {
            e.verdict = .blocked;
            e.reason_code = if (r.kind == .dynamic_loader) "dependency_uninspectable" else "source_unresolved";
            e.remediation = "Use a supported static source expression or loader and inspect a new backup.";
            return e;
        },
        else => {},
    }
    if (r.kind == .model and r.dimensions == 0) {
        e.reason_code = "model_contract_unknown";
        e.remediation = "Supply a backup with a complete embedding producer and dimension contract; stored vectors alone do not establish query or ingestion compatibility.";
        return e;
    }
    const d = destination orelse return e;
    if (d.version != 1 or d.configuration_id.len == 0) return e;
    for (d.requirements) |cap| if (std.mem.eql(u8, cap.requirement_sha256, &r.sha256)) {
        const supported = cap.supported and (r.kind != .replication_source or cap.future_ingestion) and (r.kind != .producer_model or (cap.future_ingestion and cap.model_revision_verified)) and (r.kind != .model or (cap.query_embedding and cap.future_ingestion and cap.model_revision_verified and cap.preprocessing_verified and cap.vector_space_verified and r.dimensions > 0 and cap.dimensions == r.dimensions));
        e.verdict = if (supported) .compatible else .blocked;
        e.reason_code = if (supported) "capability_verified" else switch (r.kind) {
            .model, .producer_model => "model_incompatible",
            .connection => "connection_missing",
            .replication_source => "replication_incompatible",
            else => "source_unresolved",
        };
        e.remediation = if (supported) "Revalidate this capability at restore admission." else if (r.kind == .model) "Verify the same model revision, preprocessing, dimensions and vector space for both query embedding and future ingestion." else if (r.kind == .replication_source) "Verify destination-managed PostgreSQL credentials, source table, slot/publication access and future logical replication ingestion for this exact source contract." else "Configure durable access and the required retrieval semantics at the destination.";
        return e;
    };
    return e;
}

pub fn hash(bytes: []const u8) Digest {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}
fn requirementHash(kind: Kind, identity: []const u8) Digest {
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    h.update(inspector_version ++ "\x00");
    h.update(@tagName(kind));
    h.update("\x00");
    h.update(identity);
    var digest: [32]u8 = undefined;
    h.final(&digest);
    return std.fmt.bytesToHex(digest, .lower);
}
fn safeLabel(a: Allocator, value: []const u8) ![]u8 {
    if (value.len <= 128 and std.mem.indexOf(u8, value, "://") == null) {
        var safe = true;
        for (value) |c| if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-' and c != '.' and c != ':') {
            safe = false;
            break;
        };
        if (safe) return a.dupe(u8, value);
    }
    return std.fmt.allocPrint(a, "sha256:{s}", .{hash(value)});
}
fn get(v: Value, name: []const u8) Value {
    return if (v == .object) v.object.get(name) orelse .null else .null;
}
fn str(v: Value) []const u8 {
    return if (v == .string) v.string else "";
}
fn simplePath(path: []const u8) bool {
    if (path.len == 0 or path[0] == '.' or path[path.len - 1] == '.' or std.mem.indexOf(u8, path, "..") != null) return false;
    for (path) |c| if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-' and c != '.') return false;
    return true;
}
fn eq(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}
fn number(v: Value) u32 {
    return if (v == .integer and v.integer > 0 and v.integer <= std.math.maxInt(u32)) @intCast(v.integer) else 0;
}

fn producerEnabled(value: Value, fallback: bool) bool {
    if (value == .bool) return value.bool;
    if (value == .object) {
        const enabled = get(value, "enabled");
        return enabled != .bool or enabled.bool;
    }
    return fallback;
}

fn modelBackedProducer(producer: Value) bool {
    const producer_type = ProducerType.parse(str(get(producer, "type"))) orelse return false;
    const config = get(producer, "config");
    const provider = str(get(config, "provider"));
    // Exhaust the runtime enum so a new built-in requires an explicit policy.
    return switch (producer_type) {
        // Labels may embed vocabulary prompts; media outputs may repair a
        // consumer. Both require destination inference evidence on restore.
        .generator, .extractor, .transcriber, .decision, .vector_labels, .media_output => true,
        // Extraction uses the default OCR model for PDF fallback unless
        // explicitly disabled; transcription is separately opt-in.
        .document_extraction => producerEnabled(get(config, "ocr"), producerEnabled(get(config, "ocr_fallback"), true)) or
            producerEnabled(get(config, "transcription"), producerEnabled(get(config, "transcribe_audio"), false)),
        .reader => str(get(config, "model")).len > 0 or eq(provider, "antfly") or eq(provider, "openai") or eq(provider, "vertex") or eq(provider, "apple"),
        .copy => false,
    };
}

const SourceInput = enum {
    text,
    literal,
    locator,
    reader,
    model_chunk,
    prior_context,

    fn interpretsTemplateOutput(self: @This()) bool {
        return switch (self) {
            .literal, .model_chunk, .prior_context => false,
            .text, .locator, .reader => true,
        };
    }
};
const Selector = struct { field: []const u8, template: []const u8, program: ?hbs.Template = null, artifact: []const u8, input: SourceInput, dynamic: bool, grouped: bool };
pub const Inspector = struct {
    allocator: Allocator,
    report: *Report,
    options: Options,
    definitions: std.ArrayListUnmanaged(Selector) = .empty,
    definition_bytes: usize = 0,
    model_names: std.ArrayListUnmanaged(Digest) = .empty,
    decoder: portable.InspectionDocumentDecoder = .{},
    sources: std.ArrayListUnmanaged(SourceBinding) = .empty,
    source_bytes: usize = 0,

    const SourceBinding = struct { name: []const u8, source_artifact_name: []const u8, group_source_json: []const u8 };

    pub fn deinit(self: *@This()) void {
        for (self.definitions.items) |*s| {
            if (s.program) |*program| program.deinit();
            self.allocator.free(s.field);
            self.allocator.free(s.template);
            self.allocator.free(s.artifact);
        }
        self.definitions.deinit(self.allocator);
        for (self.sources.items) |source| {
            self.allocator.free(source.name);
            self.allocator.free(source.source_artifact_name);
            self.allocator.free(source.group_source_json);
        }
        self.sources.deinit(self.allocator);
        self.decoder.deinit(self.allocator);
        self.model_names.deinit(self.allocator);
    }

    pub fn visit(self: *@This(), kind: codec.BlockType, payload: []const u8) !void {
        try self.options.cancellation.check();
        switch (kind) {
            .shard_header => self.report.shards += 1,
            .metadata_batch => {
                self.decoder.metadataLimited(self.allocator, payload, self.options.max_metadata_bytes) catch |err| switch (err) {
                    error.StreamTooLong => return error.PreflightLimitExceeded,
                    else => return err,
                };
                var reader = try codec.KeyValueBatchReader.init(payload);
                while (try reader.next()) |e| {
                    self.report.metadata_records += 1;
                    if (std.mem.endsWith(u8, e.key, ":indexes")) {
                        const json = try staging.tableIndexesJsonFromPortableMetadata(self.allocator, e.value, null);
                        defer self.allocator.free(json);
                        try self.indexCatalogJson(json);
                    } else if (std.mem.endsWith(u8, e.key, ":replication_sources")) {
                        try self.replicationSourcesJson(e.value);
                    } else if (std.mem.endsWith(u8, e.key, ":enrichments") or std.mem.endsWith(u8, e.key, ":resolvers")) try self.metadataJson(e.value);
                }
            },
            .document_batch => {
                const entries = try codec.decodeDocumentBatchBorrowed(self.allocator, payload);
                defer self.allocator.free(entries);
                for (entries) |e| {
                    try self.options.cancellation.check();
                    const json = self.decoder.documentJsonLimitedAlloc(self.allocator, e, self.options.max_document_bytes) catch |err| switch (err) {
                        error.StreamTooLong => return error.PreflightLimitExceeded,
                        else => return err,
                    };
                    defer self.allocator.free(json);
                    try self.document(e.key, json, "");
                    self.report.documents += 1;
                }
            },
            .chunk_batch, .artifact_batch, .resolution_batch => {
                if (kind != .resolution_batch) portable.checkArtifactDocumentLimit(self.allocator, payload, self.options.max_document_bytes) catch |err| switch (err) {
                    error.StreamTooLong => return error.PreflightLimitExceeded,
                    else => return err,
                };
                var reader = try codec.KeyValueBatchReader.init(payload);
                while (try reader.next()) |e| {
                    self.report.artifact_records += 1;
                    if (try artifact_ids.decodeArtifactPublicIdAlloc(self.allocator, e.key)) |ref_value| {
                        var ref = ref_value;
                        defer ref.deinit(self.allocator);
                        if (ref.kind == .asset or ref.kind == .chunk) try self.document(ref.document_id, e.value, ref.name);
                        if (ref.kind == .embedding) try self.storedModel(ref.name, ref.document_id);
                    }
                }
            },
            .embedding_batch => {
                const batch = try codec.decodeEmbeddingBatch(self.allocator, payload);
                defer {
                    self.allocator.free(batch.index_name);
                    for (batch.entries) |e| {
                        self.allocator.free(e.doc_key);
                        self.allocator.free(e.vector);
                    }
                    self.allocator.free(batch.entries);
                }
                for (batch.entries) |e| try self.storedModel(batch.index_name, e.doc_key);
                self.report.artifact_records += batch.entries.len;
            },
            .sparse_batch => {
                const batch = try codec.decodeSparseBatch(self.allocator, payload);
                defer {
                    self.allocator.free(batch.index_name);
                    for (batch.entries) |e| {
                        self.allocator.free(e.doc_key);
                        self.allocator.free(e.indices);
                        self.allocator.free(e.values);
                    }
                    self.allocator.free(batch.entries);
                }
                for (batch.entries) |e| try self.storedModel(batch.index_name, e.doc_key);
                self.report.artifact_records += batch.entries.len;
            },
            else => {}, // Integrity and the remaining inert records are validated by the core reader.
        }
    }

    pub fn metadataJson(self: *@This(), bytes: []const u8) !void {
        if (bytes.len > self.options.max_metadata_bytes) return error.PreflightLimitExceeded;
        var parsed = try std.json.parseFromSlice(Value, self.allocator, bytes, .{});
        defer parsed.deinit();
        try self.collectSources(parsed.value, 0);
        try self.config(parsed.value, 0, "");
    }

    /// Index names are opaque catalog keys, never configuration properties.
    /// Collection entries (such as enrichments) are traversed as definitions.
    pub fn indexCatalogJson(self: *@This(), bytes: []const u8) !void {
        if (bytes.len > self.options.max_metadata_bytes) return error.PreflightLimitExceeded;
        var parsed = try std.json.parseFromSlice(Value, self.allocator, bytes, .{});
        defer parsed.deinit();
        switch (parsed.value) {
            .object => |object| {
                // Catalog incarnations fence runtime ownership; they do not
                // change the executable model contract. Remove only these
                // top-level index fields from our private parse, preserving
                // nested producer settings and every source file byte.
                var definitions = object.iterator();
                while (definitions.next()) |entry| {
                    if (entry.value_ptr.* != .object) continue;
                    _ = entry.value_ptr.object.orderedRemove("_index_incarnation");
                    _ = entry.value_ptr.object.orderedRemove("_coverage_incarnation");
                    // AIDX catalogs expose that same lifecycle identity on
                    // the native wrapper, outside executable config_json.
                    const definition = entry.value_ptr.*;
                    const native_kind = str(get(definition, "kind"));
                    if (get(definition, "config_json") == .string and
                        (eq(native_kind, "dense_vector") or eq(native_kind, "sparse_vector")))
                        _ = entry.value_ptr.object.orderedRemove("coverage_generation");
                }
                var sources = object.iterator();
                while (sources.next()) |entry| try self.collectSources(entry.value_ptr.*, 0);
                var it = object.iterator();
                while (it.next()) |entry| try self.config(entry.value_ptr.*, 0, entry.key_ptr.*);
            },
            .array => {
                try self.collectSources(parsed.value, 0);
                try self.config(parsed.value, 0, "");
            },
            else => return error.InvalidBackupRequest,
        }
    }

    // Prime the whole catalog before selectors are compiled: consumers may
    // precede their upstream group, or reach it through multiple artifacts.
    fn collectSources(self: *@This(), value: Value, depth: usize) anyerror!void {
        if (depth > 32) return error.PreflightLimitExceeded;
        try self.options.cancellation.check();
        if (value == .array) {
            for (value.array.items) |item| try self.collectSources(item, depth + 1);
            return;
        }
        if (value != .object) return;
        const kind = str(get(value, "kind"));
        const name = str(get(value, "name"));
        if (name.len > 0 and (eq(kind, "asset") or eq(kind, "chunk") or eq(kind, "embedding") or eq(kind, "group"))) {
            var public_group: ?[]u8 = null;
            defer if (public_group) |json| self.allocator.free(json);
            if (eq(kind, "group")) public_group = try std.json.Stringify.valueAlloc(self.allocator, get(value, "source"), .{});
            const raw_group = public_group orelse str(get(value, "group_source_json"));
            var normalized_group: ?[]u8 = null;
            defer if (normalized_group) |json| self.allocator.free(json);
            if (raw_group.len > 0) {
                const parsed = try std.json.parseFromSlice(@import("../db/group_sources.zig").Config, self.allocator, raw_group, .{});
                defer parsed.deinit();
                try parsed.value.validate();
                const json = try std.json.Stringify.valueAlloc(self.allocator, parsed.value, .{});
                defer self.allocator.free(json);
                const value_json = try std.json.parseFromSlice(Value, self.allocator, json, .{});
                defer value_json.deinit();
                normalized_group = try canonical.canonicalJsonValueAlloc(self.allocator, value_json.value);
            }
            const group = normalized_group orelse "";
            const upstream = str(get(value, "source_artifact_name"));
            const exists = for (self.sources.items) |prior| {
                if (eq(prior.name, name)) {
                    if (!eq(prior.source_artifact_name, upstream) or !eq(prior.group_source_json, group)) return error.InvalidBackupRequest;
                    break true;
                }
            } else false;
            if (!exists) {
                const bytes = name.len + upstream.len + group.len;
                if (self.sources.items.len >= self.options.max_definitions or bytes > self.options.max_metadata_bytes -| self.source_bytes -| self.definition_bytes) return error.PreflightLimitExceeded;
                const owned_name = try self.allocator.dupe(u8, name);
                errdefer self.allocator.free(owned_name);
                const owned_upstream = try self.allocator.dupe(u8, upstream);
                errdefer self.allocator.free(owned_upstream);
                const owned_group = try self.allocator.dupe(u8, group);
                errdefer self.allocator.free(owned_group);
                try self.sources.append(self.allocator, .{ .name = owned_name, .source_artifact_name = owned_upstream, .group_source_json = owned_group });
                self.source_bytes += bytes;
            }
        }
        if (get(value, "enrichments") != .null) try self.collectSources(get(value, "enrichments"), depth + 1);
    }

    pub fn replicationSourcesJson(self: *@This(), bytes: []const u8) !void {
        if (bytes.len > self.options.max_metadata_bytes) return error.PreflightLimitExceeded;
        var raw = try std.json.parseFromSlice(Value, self.allocator, bytes, .{});
        defer raw.deinit();
        try self.replicationSources(raw.value);
    }

    fn replicationSources(self: *@This(), raw: Value) !void {
        if (raw != .array) return error.InvalidBackupRequest;
        const Source = @import("antfly_metadata_openapi").ReplicationSource;
        var parsed = try std.json.parseFromValue([]Source, self.allocator, raw, .{});
        defer parsed.deinit();
        if (parsed.value.len > self.options.max_definitions) return error.PreflightLimitExceeded;
        for (parsed.value) |source| {
            try self.options.cancellation.check();
            if (!eq(source.type, "postgres") or std.mem.trim(u8, source.dsn, " \t\r\n").len == 0 or std.mem.trim(u8, source.postgres_table, " \t\r\n").len == 0)
                return error.UnsupportedReplicationInspection;
            // Bind the whole declarative source: a capability for another
            // source table, route, slot or publication cannot satisfy this one.
            // Runtime status and action hints are observations, not contracts.
            var contract = source;
            contract.status = null;
            contract.action_hint = null;
            const json = try std.json.Stringify.valueAlloc(self.allocator, contract, .{});
            defer self.allocator.free(json);
            var value = try std.json.parseFromSlice(Value, self.allocator, json, .{});
            defer value.deinit();
            const identity = try canonical.canonicalJsonValueAlloc(self.allocator, value.value);
            defer self.allocator.free(identity);
            try self.report.add(self.options, .replication_source, identity, "", "replication_sources", 0);
        }
    }

    fn priorContext(self: *@This(), value: Value) !void {
        const parsed = try std.json.parseFromValue(@import("../db/group_sources.zig").PriorContext, self.allocator, value, .{});
        defer parsed.deinit();
        try parsed.value.validate();
        try self.selector("prior_context.render", parsed.value.render, parsed.value.artifact, .prior_context, false, false);
    }

    fn definitionIdentity(self: *@This(), value: Value, group: ?[]const u8) ![]u8 {
        if (group) |name| {
            for (self.sources.items) |source| if (eq(source.name, name)) {
                const parsed = try std.json.parseFromSlice(Value, self.allocator, source.group_source_json, .{});
                defer parsed.deinit();
                var identity: std.json.ObjectMap = .empty;
                defer identity.deinit(self.allocator);
                try identity.put(self.allocator, "definition", value);
                try identity.put(self.allocator, "group_source", parsed.value);
                try identity.put(self.allocator, "group_artifact", .{ .string = name });
                return canonical.canonicalJsonValueAlloc(self.allocator, .{ .object = identity });
            };
            return error.InvalidBackupRequest;
        }
        return canonical.canonicalJsonValueAlloc(self.allocator, value);
    }

    fn config(self: *@This(), value: Value, depth: usize, catalog_name: []const u8) anyerror!void {
        if (depth > 32) return error.PreflightLimitExceeded;
        if (value == .array) {
            for (value.array.items) |v| try self.config(v, depth + 1, catalog_name);
            return;
        }
        if (value != .object) return;
        // Resolver execution has its own typed catalog and interpolation
        // language. Frozen adjudication JSON is executable configuration;
        // retired resolver identities and their retained policies are inert.
        if (get(value, "source_artifact") != .null and get(value, "resolution_artifact") != .null and get(value, "key_template") != .null) {
            const parsed = try resolver_catalog.ResolverConfig.parseValue(self.allocator, value);
            defer parsed.deinit();
            const cfg = parsed.value;
            if (cfg.retired) return;
            const identity = try canonical.canonicalJsonValueAlloc(self.allocator, value);
            defer self.allocator.free(identity);
            if (cfg.name_embedding.len > 0) {
                // This is a reference into another table's embedding catalog,
                // not a complete model/preprocessing contract. Dimensions and
                // a model name alone must never certify compatibility.
                try self.report.add(self.options, .model, identity, "", "name_embedding", 0);
            }
            if (cfg.adjudicate_json.len > 0) {
                const policy = try @import("antfly_resolver").adjudication.Config.parse(self.allocator, cfg.adjudicate_json);
                defer policy.deinit();
                try self.report.add(self.options, .producer_model, identity, "", "adjudicate", 0);
                try self.config(policy.value.decider, depth + 1, "decider");
            }
            return;
        }
        // Only executable catalog positions enter this walker. Document values
        // are examined later solely through these source selectors.
        const kind = str(get(value, "kind"));
        const prior_json = str(get(value, "prior_context_json"));
        if (eq(kind, "asset")) {
            if (prior_json.len > 0) {
                var parsed = try std.json.parseFromSlice(Value, self.allocator, prior_json, .{});
                defer parsed.deinit();
                try self.priorContext(parsed.value);
            } else if (get(value, "prior_context") != .null) try self.priorContext(get(value, "prior_context"));
        }
        const typ = str(get(value, "type"));
        const producer_json = str(get(value, "producer_json"));
        var producer_parsed: ?std.json.Parsed(Value) = null;
        defer if (producer_parsed) |*p| p.deinit();
        if (producer_json.len > 0) producer_parsed = try std.json.parseFromSlice(Value, self.allocator, producer_json, .{});
        const producer = if (producer_parsed) |p| p.value else get(value, "producer");
        const producer_type = str(get(producer, "type"));
        const group = if (eq(kind, "chunk") or eq(kind, "embedding")) enrichment_types.groupRoot(self.sources.items, str(get(value, "name"))) else null;
        const field = if (group != null) enrichment_types.grouped_source_field else blk: {
            const f = str(get(value, "source_field"));
            break :blk if (f.len > 0) f else str(get(value, "field"));
        };
        const template = if (group != null) "" else blk: {
            const t = str(get(value, "source_template"));
            break :blk if (t.len > 0) t else str(get(value, "template"));
        };
        const artifact = group orelse str(get(value, "source_artifact_name"));
        var model_chunk = false;
        const chunker_json = str(get(value, "chunker_json"));
        if (eq(kind, "chunk") and chunker_json.len > 0) {
            var chunk_config = try chunking_types.parseConfigFromSlice(self.allocator, chunker_json);
            defer chunk_config.deinit(self.allocator);
            if (chunk_config.embedding) |target| {
                model_chunk = true;
                const identity = try self.definitionIdentity(value, group);
                defer self.allocator.free(identity);
                const dims: u32 = if (target.dimensions) |d| if (d > 0 and d <= std.math.maxInt(u32)) @intCast(d) else 0 else 0;
                try self.report.add(self.options, .model, identity, "", field, dims);
            }
        }
        const is_embedding = eq(kind, "embedding") or eq(kind, "dense_vector") or eq(kind, "sparse_vector") or eq(typ, "embeddings") or get(value, "embedder") != .null or get(value, "semantic_producer") != .null;
        const is_definition = is_embedding or producer != .null or eq(kind, "asset") or eq(kind, "chunk") or get(value, "transcriber") != .null;
        const known = producer_type.len == 0 or ProducerType.parse(producer_type) != null;
        if (!known) try self.report.add(self.options, .dynamic_loader, "producer", "", field, 0);
        if (is_definition and (field.len > 0 or template.len > 0 or artifact.len > 0)) {
            const locator_mode = eq(producer_type, "reader") or eq(producer_type, "transcriber") or eq(producer_type, "document_extraction") or eq(str(get(value, "embedding_input")), "pdf_page_images") or eq(str(get(value, "input")), "pdf_page_images") or eq(str(get(value, "embedding_input")), "image") or eq(str(get(value, "embedding_input")), "audio") or eq(str(get(value, "embedding_input")), "video") or get(value, "transcriber") != .null;
            const parsed_type = ProducerType.parse(producer_type) orelse .copy;
            const literal_output = (eq(kind, "asset") and known and !parsed_type.usesTemplateParts(template)) or
                (eq(kind, "chunk") and !model_chunk) or (is_embedding and template.len == 0);
            try self.selector(field, template, artifact, if (model_chunk) .model_chunk else if (eq(producer_type, "reader")) .reader else if (locator_mode) .locator else if (literal_output) .literal else .text, !known, group != null);
        }
        if (is_embedding) {
            const name = if (str(get(value, "name")).len > 0) str(get(value, "name")) else catalog_name;
            if (name.len > 0) {
                if (self.model_names.items.len >= self.options.max_definitions) return error.PreflightLimitExceeded;
                try self.model_names.append(self.allocator, hash(name));
            }
            var dims = @max(number(get(value, "expected_dims")), @max(number(get(value, "dimension")), @max(number(get(value, "dimensions")), number(get(value, "dims")))));
            if (get(value, "config_json") == .string) {
                var nested = try std.json.parseFromSlice(Value, self.allocator, get(value, "config_json").string, .{});
                defer nested.deinit();
                dims = @max(dims, @max(number(get(nested.value, "dims")), number(get(nested.value, "dimension"))));
            }
            if (!try self.hasModelIdentity(value, 0)) dims = 0;
            // The whole semantic definition binds model/version, preprocessing,
            // template, input modality, dimensions and vector-space assertion.
            const identity = try self.definitionIdentity(value, group);
            defer self.allocator.free(identity);
            try self.report.add(self.options, .model, identity, "", str(get(value, "name")), dims);
        }
        // Providers can select an implicit model (Apple generation forbids an
        // explicit model). Bind the enclosing producer definition, including
        // its source and template, rather than certifying it from model names.
        if (modelBackedProducer(producer) or get(value, "transcriber") != .null or str(get(value, "model")).len > 0) {
            const identity = try self.definitionIdentity(value, group);
            defer self.allocator.free(identity);
            try self.report.add(self.options, .producer_model, identity, "", "model", 0);
        }
        var it = value.object.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            const v = entry.value_ptr.*;
            if (eq(key, "api_url") or eq(key, "base_url") or eq(key, "endpoint") or eq(key, "url") or eq(key, "uri") or eq(key, "source_url") or eq(key, "model_path") or eq(key, "credentials_path")) {
                if (v == .string and v.string.len > 0) {
                    // This v2 semantic-model sentinel selects the built-in
                    // runtime, not a source URI. Its model still needs exact
                    // destination evidence; never exempt media locators.
                    const embedded_model_endpoint = eq(key, "endpoint") and eq(v.string, "antfly:embedded") and
                        number(get(value, "version")) == 2 and eq(str(get(value, "provider")), "antfly") and
                        str(get(value, "model")).len > 0 and get(value, "sparse") == .bool;
                    if (!embedded_model_endpoint) try self.locator(v.string, "", key);
                }
            } else if (eq(key, "connection") or eq(key, "credential") or eq(key, "credential_name") or eq(key, "credentials") or eq(key, "connection_id") or eq(key, "connection_name")) {
                if (v == .string and v.string.len > 0) try self.report.add(self.options, .connection, v.string, "", key, 0);
            } else if (@import("../../common/credential_safety.zig").fieldNameIsSensitive(key) or eq(key, "headers")) {
                if (v != .null and (v != .string or v.string.len > 0)) {
                    const binding = try canonical.canonicalJsonValueAlloc(self.allocator, v);
                    defer self.allocator.free(binding);
                    try self.report.add(self.options, .connection, binding, "", key, 0);
                }
            } else if (eq(key, "replication_sources") and v == .array) {
                try self.replicationSources(v);
            } else if (eq(key, "loader") or eq(key, "custom_loader")) {
                try self.report.add(self.options, .dynamic_loader, key, "", key, 0);
            } else if (eq(key, "config_json") or eq(key, "producer_json") or eq(key, "chunker_json")) {
                if (v == .string and v.string.len > 0) {
                    var p = try std.json.parseFromSlice(Value, self.allocator, v.string, .{});
                    defer p.deinit();
                    try self.config(p.value, depth + 1, catalog_name);
                }
            } else if (eq(key, "schema") or eq(key, "json_schema") or eq(key, "output_schema") or eq(key, "examples") or eq(key, "default")) {
                // Output shape/examples are inert, even inside producer config.
            } else if (v == .object or v == .array) try self.config(v, depth + 1, key);
        }
    }

    fn storedModel(self: *@This(), name: []const u8, doc: []const u8) !void {
        const digest = hash(name);
        for (self.model_names.items) |known| if (eq(&known, &digest)) return;
        // Vectors without a catalog cannot prove future query/ingestion behavior.
        try self.report.add(self.options, .model, name, doc, "stored_embedding", 0);
    }

    fn hasModelIdentity(self: *@This(), v: Value, depth: usize) anyerror!bool {
        if (depth > 32) return error.PreflightLimitExceeded;
        if (v == .array) {
            for (v.array.items) |item| if (try self.hasModelIdentity(item, depth + 1)) return true;
        }
        if (v != .object) return false;
        if (str(get(v, "model")).len > 0) return true;
        var it = v.object.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            const item = entry.value_ptr.*;
            if (item == .object or item == .array) {
                if (try self.hasModelIdentity(item, depth + 1)) return true;
            } else if ((eq(key, "config_json") or eq(key, "producer_json")) and item == .string and item.string.len > 0) {
                var nested = try std.json.parseFromSlice(Value, self.allocator, item.string, .{});
                defer nested.deinit();
                if (try self.hasModelIdentity(nested.value, depth + 1)) return true;
            }
        }
        return false;
    }

    fn selector(self: *@This(), field: []const u8, template: []const u8, artifact: []const u8, input: SourceInput, dynamic: bool, grouped: bool) !void {
        if (self.definitions.items.len >= self.options.max_definitions or field.len + template.len + artifact.len > self.options.max_metadata_bytes -| self.definition_bytes -| self.source_bytes) return error.PreflightLimitExceeded;
        for (self.definitions.items) |prior| if (eq(prior.field, field) and eq(prior.template, template) and eq(prior.artifact, artifact) and prior.input == input and prior.dynamic == dynamic and prior.grouped == grouped) return;
        const a = self.allocator;
        var appended = false;
        const f = try a.dupe(u8, field);
        errdefer if (!appended) a.free(f);
        const t = try a.dupe(u8, template);
        errdefer if (!appended) a.free(t);
        const ar = try a.dupe(u8, artifact);
        errdefer if (!appended) a.free(ar);
        var program = if (template.len > 0) try self.compileTemplate(t, field) else null;
        errdefer if (!appended) {
            if (program) |*parsed| parsed.deinit();
        };
        try self.definitions.append(a, .{ .field = f, .template = t, .program = program, .artifact = ar, .input = input, .dynamic = dynamic, .grouped = grouped });
        appended = true;
        self.definition_bytes += field.len + template.len + artifact.len;
        // Empty tables still carry a future-ingestion dependency.
        if (dynamic) try self.report.add(self.options, .dynamic_loader, "loader", "", field, 0);
        if (program) |parsed| try self.templateSources(parsed.program, .null, "", field, input, true);
    }

    pub fn document(self: *@This(), id: []const u8, bytes: []const u8, artifact: []const u8) !void {
        if (bytes.len > self.options.max_document_bytes) return error.PreflightLimitExceeded;
        if (self.definitions.items.len == 0) return;
        var parsed = @import("antfly-json").parseValueLossless(self.allocator, bytes) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => {
                if (artifact.len > 0) {
                    for (self.definitions.items) |s| if (eq(s.artifact, artifact)) {
                        if (s.input == .literal) continue;
                        if (s.grouped) {
                            try self.report.add(self.options, .source_unresolved, "group_unit_text", id, s.field, 0);
                        } else if (s.input == .prior_context) {
                            try self.report.add(self.options, .source_unresolved, "prior_context_artifact", id, s.field, 0);
                        } else try self.sourceValue(.{ .string = bytes }, id, s.field, s.input);
                    };
                    return;
                }
                return err;
            },
        };
        defer parsed.deinit();
        for (self.definitions.items) |s| {
            // Literal consumers do not interpret media markers. Their helpers
            // are inspected at definition time and with each document's context.
            if (s.input == .literal and s.template.len == 0) continue;
            if (s.grouped) {
                // Retained group assets and private unit documents are the two
                // runtime representations of the same synthesized source text.
                const selected: Value = if (artifact.len == 0 and eq(str(get(parsed.value, "_group_artifact")), s.artifact))
                    enrichment_types.sourceFieldValue(parsed.value, enrichment_types.grouped_source_field, true)
                else if (eq(artifact, s.artifact)) get(parsed.value, "text") else continue;
                if (selected != .string) {
                    try self.report.add(self.options, .source_unresolved, "group_unit_text", id, s.field, 0);
                } else try self.sourceValue(selected, id, s.field, s.input);
                continue;
            }
            if (!eq(s.artifact, artifact)) continue;
            if (s.dynamic) {
                try self.report.add(self.options, .dynamic_loader, "loader", id, s.field, 0);
                continue;
            }
            if (s.template.len > 0) {
                const program = s.program orelse continue; // Unsupported syntax was recorded at definition time.
                const root = if (s.input == .prior_context) @import("../db/group_prior_context.zig").artifactCollection(parsed.value) else parsed.value;
                if (s.input == .prior_context and root == .array) {
                    for (root.array.items) |item| try self.templateSources(program.program, item, id, s.field, s.input, false);
                } else try self.templateSources(program.program, root, id, s.field, s.input, false);
            } else if (artifact.len > 0 and eq(s.field, "text")) {
                // Reuse runtime chunk payload selection, including historical
                // _source_field provenance, instead of guessing stored layouts.
                if (try @import("../../chunking/chunk.zig").artifactTextAlloc(self.allocator, bytes, s.field)) |text| {
                    defer self.allocator.free(text);
                    try self.sourceValue(.{ .string = text }, id, s.field, s.input);
                }
            } else {
                const selected = if (s.field.len == 0) parsed.value else enrichment_types.sourceFieldValue(parsed.value, s.field, false);
                try self.sourceValue(selected, id, s.field, s.input);
            }
        }
    }

    fn sourceValue(self: *@This(), value: Value, doc: []const u8, field: []const u8, input: SourceInput) !void {
        switch (input) {
            .literal, .prior_context => {},
            .model_chunk => try self.modelChunkSource(value, doc, field),
            .reader => {
                if (value == .null) return;
                if (value == .string) return self.readerText(value.string, doc, field);
                if (value == .array) {
                    var source = try @import("../../reader_source.zig").parseArray(self.allocator, value.array.items);
                    defer source.deinit(self.allocator);
                    for (source.images) |image| try self.locator(image, doc, field);
                } else try self.report.add(self.options, .source_unresolved, "structured_locator", doc, field, 0);
            },
            .locator => try self.consumeValue(value, doc, field, true, 0),
            .text => try self.consumeValue(value, doc, field, false, 0),
        }
    }

    fn modelChunkSource(self: *@This(), value: Value, doc: []const u8, field: []const u8) !void {
        const source = @import("../../chunking/chunk.zig").modelSourceFromValue(value) catch {
            try self.report.add(self.options, .source_unresolved, "model_chunk_source", doc, field, 0);
            return;
        };
        switch (source) {
            .none => {},
            .text => {},
            .source => |reference| try self.locator(reference.uri, doc, field),
            .media => try self.report.add(self.options, .embedded, "inline", doc, field, 0),
        }
    }

    fn consumeValue(self: *@This(), v: Value, doc: []const u8, field: []const u8, locator_mode: bool, depth: usize) anyerror!void {
        if (depth > 32) return error.PreflightLimitExceeded;
        switch (v) {
            .string => |s| if (locator_mode) {
                try self.locator(s, doc, field);
            } else {
                try self.directives(s, doc, field);
            },
            .array => |items| for (items.items) |item| try self.consumeValue(item, doc, field, locator_mode, depth + 1),
            .object => if (locator_mode) {
                try self.report.add(self.options, .source_unresolved, "structured_locator", doc, field, 0);
            },
            .null => {},
            else => if (locator_mode) {
                try self.report.add(self.options, .source_unresolved, "nonstring_locator", doc, field, 0);
            },
        }
    }

    fn directives(self: *@This(), text: []const u8, doc: []const u8, field: []const u8) !void {
        return self.directivesKnown(text, doc, field, false);
    }

    fn directivesKnown(self: *@This(), text: []const u8, doc: []const u8, field: []const u8, definition: bool) !void {
        const cleaned = try template_runtime.stripErrorDirectives(self.allocator, text);
        defer self.allocator.free(cleaned);
        var parts: template_runtime.TextPartIterator = .{ .text = cleaned };
        while (parts.next()) |part| switch (part) {
            .media_url => |reference| {
                if (!definition and std.mem.indexOfScalar(u8, reference, 0) != null) {
                    try self.report.add(self.options, .dynamic_loader, "composed_media", doc, field, 0);
                } else try self.templateLocator(reference, doc, field, definition);
            },
            .text => |value| {
                const prefix = "<<<dotprompt:media:url ";
                if (definition) if (std.mem.indexOf(u8, value, prefix)) |start| {
                    const reference = value[start + prefix.len ..];
                    if (std.mem.indexOfScalar(u8, reference, 0) != null)
                        try self.templateLocator(reference, doc, field, true);
                };
            },
            .binary => unreachable,
        };
    }

    fn templatePathSupported(path: hbs.Node) bool {
        return path == .path and path.path.depth == 0 and !path.path.data and simplePath(path.path.original);
    }

    fn templateArgument(self: *@This(), eval: *hbs.Eval, node: *const hbs.Node, doc: []const u8, field: []const u8, definition: bool) !?[]const u8 {
        if (node.* == .string_literal) return node.string_literal.value;
        if (!templatePathSupported(node.*)) {
            try self.report.add(self.options, .dynamic_loader, "template_argument", doc, field, 0);
            return null;
        }
        if (definition) return null;
        const value = eval.resolvePath(&node.path);
        return switch (value) {
            .string => |text| text,
            .undefined, .null => null,
            else => blk: {
                try self.report.add(self.options, .source_unresolved, "template_argument", doc, field, 0);
                break :blk null;
            },
        };
    }

    const HelperOutput = enum { empty, formatter, remote };

    fn templateHelper(self: *@This(), eval: *hbs.Eval, expression: *const hbs.Node, doc: []const u8, field: []const u8, definition: bool, locator_mode: bool) !?HelperOutput {
        const expr = expression.expression;
        const name = expr.path.path.original;
        if (!(eq(name, "media") or eq(name, "remoteMedia") or eq(name, "remoteText") or eq(name, "remotePDF"))) return null;
        if (expr.params.len != 0) return null;
        var url: ?*const hbs.Node = null;
        var credential: ?*const hbs.Node = null;
        if (expr.hash) |hash_node| for (hash_node.hash.pairs) |pair_node| {
            const pair = pair_node.hash_pair;
            if (eq(pair.key, "url")) {
                url = pair.value;
            } else if (!eq(name, "media") and eq(pair.key, template_runtime.remote_credentials_argument)) {
                credential = pair.value;
            } else return null;
        };
        // Match runtime: no URL means the helper returns an empty value.
        const source = url orelse return .empty;
        var reference: ?[]const u8 = null;
        if (source.* == .string_literal) {
            reference = source.string_literal.value;
        } else if (templatePathSupported(source.*)) {
            if (!definition) {
                const value = eval.resolvePath(&source.path);
                reference = if (value == .string) value.string else return .empty;
            }
        } else return null;
        if (reference) |text| if (text.len == 0) return .empty;
        // Runtime checks the URL before resolving credentials. Empty calls
        // cannot fetch, fail credential lookup, or produce opaque output.
        if (credential) |binding| {
            if (try self.templateArgument(eval, binding, doc, field, definition)) |binding_name| {
                if (binding_name.len == 0) {
                    try self.report.add(self.options, .source_unresolved, "template_credential", doc, field, 0);
                } else try self.report.add(self.options, .connection, binding_name, doc, field, 0);
            } else if (!definition) try self.report.add(self.options, .source_unresolved, "template_credential", doc, field, 0);
        }
        // The pure media helper is rendered with the runtime implementation.
        if (locator_mode or !eq(name, "media")) {
            if (reference) |text| try self.locator(text, doc, field);
        }
        return if (eq(name, "media")) .formatter else .remote;
    }

    fn compileTemplate(self: *@This(), text: []const u8, field: []const u8) !?hbs.Template {
        // Use the execution parser, but admit only flat references and known
        // remote-helper argument shapes. Reject nesting before recursive parsing.
        var lexer = hbs.Lexer.init(text);
        var tokens: usize = 0;
        while (true) {
            const token = lexer.next();
            if (token.kind == .eof) break;
            tokens += 1;
            if (tokens / 16 > self.options.max_definitions) return error.PreflightLimitExceeded;
            switch (token.kind) {
                .open_block, .open_inverse, .open_partial, .open_partial_block, .open_inline, .open_sexpr => {
                    try self.report.add(self.options, .dynamic_loader, "template", "", field, 0);
                    return null;
                },
                else => {},
            }
        }
        return hbs.Template.init(self.allocator, text) catch |err| {
            if (err == error.OutOfMemory) return err;
            try self.report.add(self.options, .dynamic_loader, "template", "", field, 0);
            return null;
        };
    }

    fn templateSources(self: *@This(), program: *const hbs.Node, root: Value, doc: []const u8, field: []const u8, input: SourceInput, definition: bool) !void {
        const locator_mode = input == .locator;
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var helpers: hbs.HelperMap = .{};
        try helpers.put(a, "media", hbs.Helper.from(&template_runtime.mediaHelper));
        const partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
        var eval = hbs.Eval.init(a, try template_runtime.jsonToValue(a, root), &helpers, &partials);
        var output: std.ArrayListUnmanaged(u8) = .empty;
        defer output.deinit(self.allocator);
        var strip_next = false;
        var has_helper = false;
        for (program.program.body) |node| {
            try self.options.cancellation.check();
            var safe_node = node.*;
            switch (node.*) {
                .content, .comment => {},
                .mustache => |mustache| {
                    const expression = mustache.expression;
                    if (expression.* != .expression or !templatePathSupported(expression.expression.path.*)) {
                        try self.report.add(self.options, .dynamic_loader, "template", doc, field, 0);
                        return;
                    }
                    const expr = expression.expression;
                    const name = expr.path.path.original;
                    const known_helper = eq(name, "media") or eq(name, "remoteMedia") or eq(name, "remoteText") or eq(name, "remotePDF");
                    if (known_helper or expr.params.len > 0 or expr.hash != null) {
                        const helper_output = (try self.templateHelper(&eval, expression, doc, field, definition, locator_mode)) orelse {
                            try self.report.add(self.options, .dynamic_loader, "template", doc, field, 0);
                            return;
                        };
                        has_helper = has_helper or helper_output != .empty;
                        if (helper_output != .formatter) {
                            // Fetch results can contain or complete media markers. Keep
                            // known surrounding output for inventory, but never certify
                            // an opaque result, even with destination URL evidence.
                            if (helper_output == .remote and input.interpretsTemplateOutput()) try self.report.add(self.options, .dynamic_loader, "helper_output", doc, field, 0);
                            const literal = try a.create(hbs.Node);
                            literal.* = .{ .string_literal = .{ .value = if (helper_output == .remote) "\x00" else "" } };
                            const inert = try a.create(hbs.Node);
                            inert.* = .{ .expression = .{ .path = literal } };
                            safe_node.mustache.expression = inert;
                            safe_node.mustache.unescaped = true;
                        }
                    } else if (definition) {
                        // A document substitution is unknown at definition time.
                        // Preserve its position instead of inventing an empty URL
                        // component; concrete documents are evaluated separately.
                        const literal = try a.create(hbs.Node);
                        literal.* = .{ .string_literal = .{ .value = "\x00" } };
                        const inert = try a.create(hbs.Node);
                        inert.* = .{ .expression = .{ .path = literal } };
                        safe_node.mustache.expression = inert;
                        safe_node.mustache.unescaped = true;
                    }
                },
                else => {
                    try self.report.add(self.options, .dynamic_loader, "template", doc, field, 0);
                    return;
                },
            }
            const strip: @TypeOf(node.mustache.strip) = switch (safe_node) {
                .mustache => |m| m.strip,
                .comment => |c| c.strip,
                else => .{},
            };
            if (strip.open) hbs.trimTrailingWhitespace(&output);
            const body = [_]*hbs.Node{&safe_node};
            var single: hbs.Node = .{ .program = .{ .body = &body } };
            const rendered = try eval.exec(&single);
            const fragment = if (strip_next) hbs.trimLeadingWhitespace(rendered) else rendered;
            if (fragment.len > self.options.max_document_bytes -| output.items.len) return error.PreflightLimitExceeded;
            try output.appendSlice(self.allocator, fragment);
            strip_next = strip.close;
        }
        @import("../../template_remote.zig").validateRenderedTemplate(self.allocator, output.items) catch |err| {
            if (err == error.OutOfMemory) return err;
            try self.report.add(self.options, .source_unresolved, "template_error", doc, field, 0);
            return;
        };
        if (input == .reader) {
            try self.readerTemplate(output.items, doc, field, definition);
        } else if (locator_mode and has_helper) {
            if (program.program.body.len != 1) try self.report.add(self.options, .dynamic_loader, "composed_locator", doc, field, 0);
        } else if (locator_mode) {
            try self.templateLocator(output.items, doc, field, definition);
        } else if (input.interpretsTemplateOutput()) {
            // Scan the complete known output, including markers assembled from
            // several literals/references, with no producer or helper execution.
            try self.directivesKnown(output.items, doc, field, definition);
        }
    }

    fn readerText(self: *@This(), text: []const u8, doc: []const u8, field: []const u8) !void {
        var source = try @import("../../reader_source.zig").parseText(self.allocator, text);
        defer source.deinit(self.allocator);
        for (source.images) |image| try self.locator(image, doc, field);
    }

    fn readerTemplate(self: *@This(), text: []const u8, doc: []const u8, field: []const u8, definition: bool) !void {
        const cleaned = try template_runtime.stripErrorDirectives(self.allocator, text);
        defer self.allocator.free(cleaned);
        var parts: template_runtime.TextPartIterator = .{ .text = cleaned };
        var has_media = false;
        while (parts.next()) |part| switch (part) {
            .media_url => |url| {
                has_media = true;
                if (!definition and std.mem.indexOfScalar(u8, url, 0) != null) {
                    try self.report.add(self.options, .dynamic_loader, "composed_media", doc, field, 0);
                } else try self.templateLocator(url, doc, field, definition);
            },
            else => {},
        };
        // Readers select media parts before falling back to source_text. The
        // surrounding prompt must not be treated as another source locator.
        if (has_media) return;
        if (std.mem.indexOfScalar(u8, text, 0) != null) {
            if (!definition) {
                try self.report.add(self.options, .dynamic_loader, "reader_output", doc, field, 0);
                return;
            }
            if (std.mem.indexOf(u8, cleaned, "<<<dotprompt:media:url ")) |start| {
                // An unknown substitution can supply the closing delimiter.
                try self.directivesKnown(cleaned[start..], doc, field, true);
                return;
            }
            const trimmed = std.mem.trimStart(u8, text, &std.ascii.whitespace);
            if (trimmed.len > 0 and (trimmed[0] == '[' or trimmed[0] == '"')) {
                var source = (@import("../../reader_source.zig").parseTemplate(self.allocator, text, self.options.max_document_bytes) catch |err| switch (err) {
                    error.StreamTooLong => return error.PreflightLimitExceeded,
                    else => return err,
                }) orelse {
                    try self.report.add(self.options, .dynamic_loader, "reader_json_template", doc, field, 0);
                    return;
                };
                defer source.deinit(self.allocator);
                for (source.images) |image| try self.templateLocator(image, doc, field, true);
                return;
            }
            return self.templateLocator(text, doc, field, true);
        }
        try self.readerText(text, doc, field);
    }

    /// Unknown substitutions can extend the current URI component or introduce
    /// delimiters. Analyze only the guaranteed prefix; never fill in a value.
    fn templateLocator(self: *@This(), raw: []const u8, doc: []const u8, field: []const u8, definition: bool) !void {
        const unknown = if (definition) std.mem.indexOfScalar(u8, raw, 0) else null;
        const unknown_at = unknown orelse return self.locator(raw, doc, field);
        const prefix = std.mem.trimStart(u8, raw[0..unknown_at], " \t\r\n");
        if (prefix.len == 0) return;
        const scheme_end = std.mem.indexOfScalar(u8, prefix, ':');
        if (std.mem.indexOfAny(u8, prefix, "/\\")) |slash| {
            // A slash before a possible scheme delimiter establishes a local
            // path even when the remaining filename contains colons.
            if (scheme_end == null or slash < scheme_end.?) {
                try self.report.add(self.options, .source_local, "filesystem", doc, field, 0);
                return;
            }
        }
        const colon = scheme_end orelse return;
        const scheme = std.Uri.parse(prefix[0 .. colon + 1]) catch return;
        if (std.ascii.eqlIgnoreCase(scheme.scheme, "file") or scheme.scheme.len == 1 or std.ascii.eqlIgnoreCase(scheme.scheme, "data"))
            return self.locator(prefix[0 .. colon + 1], doc, field);
        const source_scheme = @import("antfly_scraping").contentScheme(scheme.scheme) catch
            return self.locator(prefix[0 .. colon + 1], doc, field);
        switch (source_scheme) {
            .http, .https, .s3 => {},
            .file, .data => unreachable, // Classified above.
        }
        if (!std.mem.startsWith(u8, prefix[colon + 1 ..], "//")) return;
        const authority = prefix[colon + 3 ..];
        if (std.mem.indexOfAny(u8, authority, "/?#") == null) {
            // The host or port is still open, but literal userinfo already
            // establishes a credential-bearing source even without an origin.
            if (@import("../../common/credential_safety.zig").urlContainsCredentials(prefix))
                try self.report.add(self.options, .source_expiring, "temporary_access", doc, field, 0);
            return;
        }
        var known = prefix;
        const parameter_marker: ?usize = if (std.mem.indexOfScalar(u8, known, '#')) |at| at else std.mem.indexOfScalar(u8, known, '?');
        if (parameter_marker) |marker| {
            // A literal '=' closes a key even when its value is unknown. An
            // unfinished key (e.g. token{{suffix}}) must not become 'token'.
            const parameters = known[marker + 1 ..];
            const start = if (std.mem.lastIndexOfAny(u8, parameters, "&;")) |at| at + 1 else 0;
            if (std.mem.indexOfScalar(u8, parameters[start..], '=') == null)
                known = known[0 .. marker + 1 + start];
        } else if (std.mem.lastIndexOfScalar(u8, known, '%')) |percent| {
            // A substitution may complete a percent escape in the open path.
            if (known.len - percent < 3) known = known[0..percent];
        }
        try self.locatorKnown(known, doc, field, false);
    }

    pub fn locator(self: *@This(), raw: []const u8, doc: []const u8, field: []const u8) !void {
        return self.locatorKnown(raw, doc, field, true);
    }

    fn locatorKnown(self: *@This(), raw: []const u8, doc: []const u8, field: []const u8, complete: bool) !void {
        const text = std.mem.trim(u8, raw, " \t\r\n");
        if (text.len == 0) return;
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const lower = try std.ascii.allocLowerString(a, text);
        if (std.mem.startsWith(u8, lower, "data:")) {
            try self.report.add(self.options, .embedded, "inline", doc, field, 0);
            return;
        }
        const uri = std.Uri.parse(lower) catch {
            try self.report.add(self.options, .source_local, "filesystem", doc, field, 0);
            return;
        };
        if (eq(uri.scheme, "file") or (uri.scheme.len == 1)) {
            try self.report.add(self.options, .source_local, "filesystem", doc, field, 0);
            return;
        }
        const host = if (uri.host) |h| std.mem.trimEnd(u8, try std.ascii.allocLowerString(a, try h.toRawMaybeAlloc(a)), ".") else "";
        var temporary_access = @import("../../common/credential_safety.zig").urlContainsCredentials(text);
        if (uri.query) |q| {
            // Split before decoding: escaped '&' and '=' belong to a query
            // component and must never manufacture another parameter.
            const query = switch (q) {
                .raw, .percent_encoded => |encoded| encoded,
            };
            var parts = std.mem.splitScalar(u8, query, '&');
            while (parts.next()) |part| {
                const encoded_key = part[0 .. std.mem.indexOfScalar(u8, part, '=') orelse part.len];
                const component: std.Uri.Component = .{ .percent_encoded = encoded_key };
                const key = try std.ascii.allocLowerString(a, try component.toRawMaybeAlloc(a));
                if (eq(key, "expires") or eq(key, "se") or eq(key, "expiry") or eq(key, "expires_at") or eq(key, "x-amz-expires") or eq(key, "x-goog-expires")) temporary_access = true;
                if (eq(key, "page") or eq(key, "pages") or eq(key, "range") or eq(key, "page_number") or eq(key, "page_index") or eq(key, "page_range")) try self.report.add(self.options, .page_retrieval, "page_selection", doc, field, 0);
                if (eq(key, "member") or eq(key, "entry") or eq(key, "archive")) try self.report.add(self.options, .archive_retrieval, "archive_member", doc, field, 0);
            }
        }
        if (temporary_access) try self.report.add(self.options, .source_expiring, "temporary_access", doc, field, 0);
        if (uri.fragment != null) try self.report.add(self.options, .page_retrieval, "fragment_selection", doc, field, 0);
        const path = try uri.path.toRawMaybeAlloc(a);
        const path_complete = complete or uri.query != null or uri.fragment != null;
        if (eq(uri.scheme, "zip") or (path_complete and (std.mem.endsWith(u8, path, ".zip") or std.mem.endsWith(u8, path, ".tar") or std.mem.endsWith(u8, path, ".tgz"))) or std.mem.indexOf(u8, path, ".zip/") != null or std.mem.indexOf(u8, path, ".zip!") != null or std.mem.indexOf(u8, path, "!/") != null) try self.report.add(self.options, .archive_retrieval, "archive_member", doc, field, 0);
        const source_scheme = @import("antfly_scraping").contentScheme(uri.scheme) catch {
            try self.report.add(self.options, .source_unresolved, "unsupported_scheme", doc, field, 0);
            return;
        };
        switch (source_scheme) {
            .http, .https => {
                if (host.len == 0) {
                    try self.report.add(self.options, .source_unresolved, "missing_host", doc, field, 0);
                    return;
                }
                // Object-storage authorities name buckets, not network gateways.
                // A valid single-label S3 bucket must not be classified as a
                // local HTTP host merely because it has no dot.
                if (isLocalHost(host)) try self.report.add(self.options, .source_local, "local_gateway", doc, field, 0);
                // Origin scope is deliberately hashed; no path, query, userinfo or
                // credential value is ever included in an example or log.
                const origin = try std.fmt.allocPrint(a, "{s}://{s}:{d}", .{ uri.scheme, host, uri.port orelse if (eq(uri.scheme, "https")) @as(u16, 443) else @as(u16, 80) });
                try self.report.add(self.options, .source_external, origin, doc, field, 0);
            },
            .s3 => {
                @import("antfly_scraping").validateS3Uri(uri, .{ .complete = path_complete }) catch {
                    try self.report.add(self.options, .source_unresolved, "invalid_s3_uri", doc, field, 0);
                    return;
                };
                const origin = try std.fmt.allocPrint(a, "{s}://{s}", .{ uri.scheme, host });
                try self.report.add(self.options, .connection, origin, doc, field, 0);
            },
            .file, .data => unreachable, // Classified before retrieval inspection.
        }
    }
};

fn isLocalHost(host: []const u8) bool {
    if (host.len == 0) return false;
    if (host[0] == '[' and host[host.len - 1] == ']') {
        const literal = host[1 .. host.len - 1];
        // Normalize mixed dotted/hex notation without resolving the address.
        // The pinned std parser only accepts dotted tails after ::ffff:.
        var hexadecimal: [64]u8 = undefined;
        const hex = if (std.mem.indexOfScalar(u8, literal, '.') != null) blk: {
            const colon = std.mem.lastIndexOfScalar(u8, literal, ':') orelse return true;
            const ip4 = std.Io.net.Ip4Address.parse(literal[colon + 1 ..], 0) catch return true;
            break :blk std.fmt.bufPrint(&hexadecimal, "{s}{x}:{x}", .{
                literal[0 .. colon + 1],
                std.mem.readInt(u16, ip4.bytes[0..2], .big),
                std.mem.readInt(u16, ip4.bytes[2..4], .big),
            }) catch return true;
        } else literal;
        // The same parser special-cases ::ffff: as dotted IPv4 only. Expand
        // that prefix so hexadecimal mapped addresses use its general parser.
        var expanded: [64]u8 = undefined;
        const input = if (std.ascii.startsWithIgnoreCase(hex, "::ffff:"))
            std.fmt.bufPrint(&expanded, "0:0:0:0:0:ffff:{s}", .{hex[7..]}) catch return true
        else
            hex;
        const address = std.Io.net.Ip6Address.parse(input, 0) catch return true;
        const b = address.bytes;
        // Classify mapped IPv4 by its actual address, not by the IPv6 prefix.
        if (std.mem.allEqual(u8, b[0..10], 0) and b[10] == 255 and b[11] == 255)
            return isLocalIpv4(b[12], b[13]);
        // Return for public literals too: they must not fall through to the
        // single-label hostname rule below.
        return (std.mem.allEqual(u8, b[0..15], 0) and b[15] <= 1) or b[0] & 0xfe == 0xfc or (b[0] == 0xfe and b[1] & 0xc0 == 0x80);
    }
    if (eq(host, "localhost") or std.mem.endsWith(u8, host, ".localhost") or std.mem.endsWith(u8, host, ".local")) return true;
    var octets = std.mem.splitScalar(u8, host, '.');
    const first = std.fmt.parseUnsigned(u16, octets.next() orelse return false, 10) catch return std.mem.indexOfScalar(u8, host, '.') == null;
    const second = std.fmt.parseUnsigned(u16, octets.next() orelse return true, 10) catch return false;
    return isLocalIpv4(first, second);
}

fn isLocalIpv4(first: u16, second: u16) bool {
    return first == 127 or first == 0 or first == 10 or (first == 169 and second == 254) or (first == 192 and second == 168) or (first == 172 and second >= 16 and second <= 31);
}

fn count(report: Report, kind: Kind) u64 {
    var n: u64 = 0;
    for (report.requirements.items) |r| if (r.kind == kind) {
        n += r.count;
    };
    return n;
}

test "backup preflight catalog names do not hide dependencies" {
    const a = std.testing.allocator;
    const config = "{\"type\":\"embeddings\",\"field\":\"body\",\"template\":\"{{{body}}}\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"mock\"}}";
    for ([_][]const u8{ "normal", "default", "schema", "url", "api_key", "enrichments", "loader", "config_json" }) |name| {
        const metadata = try std.fmt.allocPrint(a, "{{\"{s}\":{s}}}", .{ name, config });
        defer a.free(metadata);
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.indexCatalogJson(metadata);
        try inspector.document("doc", "{\"body\":\"<<<dotprompt:media:url file:///private/active.pdf>>>\"}", "");
        const json = try report.jsonAlloc("{\"version\":1,\"configuration_id\":\"empty-destination\"}");
        defer a.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"verdict\":\"blocked\"") != null);
        try std.testing.expectEqual(@as(u64, 1), count(report, .model));
        try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    }
}

test "backup preflight source fields are literal and template paths are nested" {
    const a = std.testing.allocator;
    const raw = "{\"media.uri\":\"file:///private/literal.pdf\",\"this\":\"file:///private/this.pdf\",\"media[*].uri\":\"file:///private/brackets.pdf\",\"media\":{\"uri\":\"https://example.org/nested.pdf\"}}";
    for ([_][]const u8{ "media.uri", "this", "media[*].uri" }) |field| {
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        const metadata = try std.json.Stringify.valueAlloc(a, .{ .kind = "asset", .source_field = field, .producer = .{ .type = "reader" } }, .{});
        defer a.free(metadata);
        try inspector.metadataJson(metadata);
        try inspector.document("doc", raw, "");
        try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
        try std.testing.expectEqual(@as(u64, 0), count(report, .source_external));
        try std.testing.expect(report.analysis_complete);
    }
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson("{\"kind\":\"asset\",\"source_template\":\"{{remoteMedia url=media.uri}}\",\"producer\":{\"type\":\"reader\"}}");
    try inspector.document("doc", raw, "");
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_local));
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_external));
}

test "backup preflight built in decision is inspectable" {
    const a = std.testing.allocator;
    const producer = "{\"type\":\"decision\",\"config\":{\"version\":\"v1\",\"decider\":{\"provider\":\"antfly\",\"model\":\"mock\"},\"questions\":[{\"name\":\"refund\",\"type\":\"predicate\",\"instructions\":\"Refund?\"}]}}";
    try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "decision", .kind = .asset, .field = "body", .producer_json = producer });
    const catalog = try @import("../db/catalog/enrichment_catalog.zig").serializeCatalog(a, &.{.{ .name = "decision", .kind = .asset, .source_field = "body", .producer_json = producer }});
    defer a.free(catalog);
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(catalog);
    try inspector.document("doc", "{\"body\":\"refund request\"}", "");
    const caps = try a.alloc(Capability, report.requirements.items.len);
    defer a.free(caps);
    for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .future_ingestion = true, .model_revision_verified = true };
    const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "decision-model", .requirements = caps }, .{});
    defer a.free(evidence);
    const json = try report.jsonAlloc(evidence);
    defer a.free(json);
    try std.testing.expectEqual(@as(u64, 0), count(report, .dynamic_loader));
    try std.testing.expectEqual(@as(u64, 2), count(report, .producer_model));
    try std.testing.expect(std.mem.indexOf(u8, json, "\"verdict\":\"compatible\"") != null);
    try std.testing.expect(report.analysis_complete);
    const changed = try @import("../db/catalog/enrichment_catalog.zig").serializeCatalog(a, &.{.{ .name = "decision", .kind = .asset, .source_field = "other", .producer_json = producer }});
    defer a.free(changed);
    try inspector.metadataJson(changed);
    const destination = try std.json.parseFromSlice(Destination, a, evidence, .{});
    defer destination.deinit();
    var unknown: usize = 0;
    for (report.requirements.items) |*r| {
        if (r.kind == .producer_model and evaluate(r, destination.value).verdict == .unknown) unknown += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), unknown);
}

test "backup preflight replication sources require exact destination connection evidence" {
    const a = std.testing.allocator;
    const bytes = "[{\"type\":\"postgres\",\"dsn\":\"postgresql://user:TOPSECRET@db.example/database\",\"postgres_table\":\"public.documents\",\"slot_name\":\"source_slot\"}]";
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.replicationSourcesJson(bytes);
    try std.testing.expectEqual(@as(u64, 1), count(report, .replication_source));
    const json = try report.jsonAlloc("{\"version\":1,\"configuration_id\":\"empty-destination\"}");
    defer a.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"compatibility\":{\"verdict\":\"unknown\"") != null);
    for ([_][]const u8{ "TOPSECRET", "db.example", "public.documents", "source_slot" }) |private| try std.testing.expect(std.mem.indexOf(u8, json, private) == null);
    const original = report.requirements.items[0].sha256;
    var caps = [_]Capability{.{ .requirement_sha256 = &original, .supported = false }};
    const destination: Destination = .{ .version = 1, .configuration_id = "postgres-access", .requirements = &caps };
    try std.testing.expectEqual(Verdict.blocked, evaluate(&report.requirements.items[0], destination).verdict);
    caps[0].supported = true;
    caps[0].future_ingestion = true;
    try std.testing.expectEqual(Verdict.compatible, evaluate(&report.requirements.items[0], destination).verdict);
    const evidence = try std.json.Stringify.valueAlloc(a, destination, .{});
    defer a.free(evidence);
    const verified = try report.jsonAlloc(evidence);
    defer a.free(verified);
    try std.testing.expect(std.mem.indexOf(u8, verified, "\"compatibility\":{\"verdict\":\"compatible\"") != null);
    // Preserve libpq keyword DSNs and unresolved secret references without
    // opening a connection or resolving any credential. Scope changes require
    // new evidence, even when the credential reference is identical.
    try inspector.metadataJson(
        \\{"replication_sources":[{"type":"postgres","dsn":"${secret:pg.dsn}","postgres_table":"public.other"},{"type":"postgres","dsn":"host=db.example dbname=app user=replicator","postgres_table":"public.documents"}]}
    );
    try std.testing.expectEqual(@as(u64, 3), count(report, .replication_source));
    for (report.requirements.items[1..]) |*r| try std.testing.expectEqual(Verdict.unknown, evaluate(r, destination).verdict);
    try std.testing.expectError(error.UnsupportedReplicationInspection, inspector.replicationSourcesJson("[{\"type\":\"future-source\",\"dsn\":\"opaque\",\"postgres_table\":\"rows\"}]"));
}

test "backup preflight prior context static fetches cannot be certified by model evidence" {
    const a = std.testing.allocator;
    const producer = "{\"type\":\"generator\",\"config\":{\"generator\":{\"provider\":\"openai\",\"model\":\"gpt-4.1\",\"api_key\":\"fixture\"}}}";
    const prior: @import("../db/group_sources.zig").PriorContext = .{ .artifact = "claims", .render = "{{remoteText url=\"http://localhost/private\"}}" };
    try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "claims", .kind = .asset, .source_artifact_name = "turns", .producer_json = producer, .prior_context = prior });
    const prior_json = try std.json.Stringify.valueAlloc(a, prior, .{});
    defer a.free(prior_json);
    const metadata = try @import("../db/catalog/enrichment_catalog.zig").serializeCatalog(a, &.{.{ .name = "claims", .kind = .asset, .source_field = "", .source_artifact_name = "turns", .producer_json = producer, .prior_context_json = prior_json }});
    defer a.free(metadata);
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(metadata);
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    const caps = try a.alloc(Capability, report.requirements.items.len);
    defer a.free(caps);
    for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .future_ingestion = true, .model_revision_verified = true };
    const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "model-and-api-key", .requirements = caps }, .{});
    defer a.free(evidence);
    const json = try report.jsonAlloc(evidence);
    defer a.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"compatibility\":{\"verdict\":\"blocked\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "localhost") == null);
}

test "backup preflight prior context selects runtime artifact items and rejects unknown helpers" {
    const a = std.testing.allocator;
    const public =
        \\[{"kind":"asset","source_artifact_name":"turns","producer":{"type":"generator","config":{"provider":"apple"}},"prior_context":{"artifact":"claims","render":"{{remoteText url=source}}"}}]
    ;
    const frozen =
        \\[{"name":"claims","kind":"asset","source_field":"","source_artifact_name":"turns","producer_json":"{\"type\":\"generator\",\"config\":{\"provider\":\"apple\"}}","prior_context_json":"{\"artifact\":\"claims\",\"render\":\"{{remoteText url=source}}\"}"}]
    ;
    for ([_][]const u8{ public, frozen }) |metadata| {
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.metadataJson(metadata);
        // Only the named prior artifact is a source, never unrelated document
        // values or artifact fields outside the runtime's selected items.
        try inspector.document("unrelated", "{\"source\":\"file:///private/unrelated\"}", "other");
        try std.testing.expectEqual(@as(u64, 0), count(report, .source_local));
        for ([_][]const u8{
            "{\"source\":\"file:///private/a\"}",
            "[{\"source\":\"file:///private/b\"}]",
            "{\"items\":[{\"source\":\"file:///private/c\"}],\"source\":\"file:///private/not-selected\"}",
        }) |artifact| try inspector.document("previous-unit", artifact, "claims");
        try std.testing.expectEqual(@as(u64, 3), count(report, .source_local));
        try std.testing.expect(report.analysis_complete);
        try inspector.document("invalid", "invalid-json", "claims");
        try std.testing.expect(!report.analysis_complete);
    }
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{ .max_definitions = 1 } };
    defer inspector.deinit();
    try inspector.metadataJson("{\"kind\":\"asset\",\"prior_context\":{\"artifact\":\"claims\",\"render\":\"{{customLoader source}}\"}}");
    try std.testing.expect(!report.analysis_complete);
    try std.testing.expectEqual(@as(u64, 1), count(report, .dynamic_loader));
    try std.testing.expectError(error.PreflightLimitExceeded, inspector.metadataJson(public));
}

test "backup preflight index incarnation does not change model compatibility" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.indexCatalogJson(
        \\{"semantic":{"name":"semantic","type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"exact-model"},"_index_incarnation":17}}
    );
    const first = report.requirements.items[0].sha256;
    const requirements = report.requirements.items.len;
    try inspector.indexCatalogJson(
        \\{"semantic":{"name":"semantic","type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"exact-model"},"_index_incarnation":29,"_coverage_incarnation":11}}
    );
    try std.testing.expectEqual(requirements, report.requirements.items.len);
    try std.testing.expectEqualStrings(&first, &report.requirements.items[0].sha256);
    // Executable configuration must remain bound even when lifecycle identity
    // is ignored. A different field or model still requires separate evidence.
    try inspector.indexCatalogJson(
        \\{"semantic":{"name":"semantic","type":"embeddings","field":"title","dimension":3,"embedder":{"provider":"antfly","model":"exact-model"},"_index_incarnation":17}}
    );
    try std.testing.expectEqual(requirements + 1, report.requirements.items.len);
    try inspector.indexCatalogJson(
        \\{"semantic":{"name":"semantic","type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"different-model"},"_index_incarnation":17}}
    );
    try std.testing.expectEqual(requirements + 3, report.requirements.items.len);
    // A similarly named nested producer setting is still executable input.
    try inspector.indexCatalogJson(
        \\{"semantic":{"name":"semantic","type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"exact-model","_index_incarnation":17}}}
    );
    try std.testing.expectEqual(requirements + 5, report.requirements.items.len);
    try inspector.indexCatalogJson(
        \\{"semantic":{"name":"semantic","type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"exact-model","_index_incarnation":29}}}
    );
    try std.testing.expectEqual(requirements + 7, report.requirements.items.len);
}

test "backup preflight native index coverage generation is not model identity" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.indexCatalogJson(
        \\{"semantic":{"name":"semantic","kind":"dense_vector","config_json":"{\"field\":\"body\",\"dims\":3,\"embedder\":{\"model\":\"exact-model\"}}","coverage_generation":17}}
    );
    const requirements = report.requirements.items.len;
    try inspector.indexCatalogJson(
        \\{"semantic":{"name":"semantic","kind":"dense_vector","config_json":"{\"field\":\"body\",\"dims\":3,\"embedder\":{\"model\":\"exact-model\"}}","coverage_generation":29}}
    );
    try std.testing.expectEqual(requirements, report.requirements.items.len);
    // Keep every byte of the native executable config bound to evidence.
    try inspector.indexCatalogJson(
        \\{"semantic":{"name":"semantic","kind":"dense_vector","config_json":"{\"field\":\"title\",\"dims\":3,\"embedder\":{\"model\":\"exact-model\"}}","coverage_generation":17}}
    );
    try std.testing.expectEqual(requirements + 2, report.requirements.items.len);
}

test "backup preflight context property names remain valid index catalog names" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.indexCatalogJson(
        \\{"prior_context":{"type":"embeddings","dimension":3,"embedder":{"model":"exact-model"}},"replication_sources":{"type":"embeddings","dimension":3,"embedder":{"model":"exact-model"}}}
    );
    try std.testing.expectEqual(@as(u64, 2), count(report, .model));
    try std.testing.expect(report.analysis_complete);
}

test "backup preflight resolver frozen policies and referenced embeddings require evidence" {
    const a = std.testing.allocator;
    const policy =
        \\{"decider":{"provider":"antfly","model":"fixture","api_key":"private-key"},"template":"{{m.text}} / {{c.canonical_name}}","question":{"type":"choice","name":"same","instructions":"Are these the same entity?","choices":[{"value":"same"},{"value":"different"}]},"accept":{"choice":"same","min_probability":0.9}}
    ;
    var cfg: resolver_catalog.ResolverConfig = .{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "{{_entity.text}}", .scorer_json = "{\"comparisons\":[]}", .adjudicate_json = policy };
    try cfg.validate();
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    const catalog = try resolver_catalog.serializeCatalog(a, &.{cfg});
    defer a.free(catalog);
    try inspector.metadataJson(catalog);
    try std.testing.expect(count(report, .producer_model) > 0);
    try std.testing.expectEqual(@as(u64, 1), count(report, .connection));
    const json = try report.jsonAlloc("{\"version\":1,\"configuration_id\":\"empty-destination\"}");
    defer a.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"verdict\":\"unknown\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "private-key") == null);
    const caps = try a.alloc(Capability, report.requirements.items.len);
    defer a.free(caps);
    for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .future_ingestion = true, .model_revision_verified = true };
    const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "verified-policy", .requirements = caps }, .{});
    defer a.free(evidence);
    const verified = try report.jsonAlloc(evidence);
    defer a.free(verified);
    try std.testing.expect(std.mem.indexOf(u8, verified, "\"compatibility\":{\"verdict\":\"compatible\"") != null);

    // Bind the complete resolver policy, including its input template, rather
    // than accepting evidence for an isolated model name or provider block.
    const original_count = count(report, .producer_model);
    const original_groups = report.requirements.items.len;
    cfg.key_template = "entity/{{_entity.text}}";
    const changed = try resolver_catalog.serializeCatalog(a, &.{cfg});
    defer a.free(changed);
    try inspector.metadataJson(changed);
    try std.testing.expectEqual(original_count + 2, count(report, .producer_model));
    try std.testing.expectEqual(original_groups + 1, report.requirements.items.len);

    cfg.adjudicate_json = "";
    cfg.name_embedding = "other-table-embedder";
    cfg.name_embedding_dims = 3;
    const embedding = try resolver_catalog.serializeCatalog(a, &.{cfg});
    defer a.free(embedding);
    try inspector.metadataJson(embedding);
    try std.testing.expectEqual(@as(u64, 1), count(report, .model));
    for (report.requirements.items) |*r| if (r.kind == .model) {
        const embedding_caps = [_]Capability{.{ .requirement_sha256 = &r.sha256, .supported = true, .query_embedding = true, .future_ingestion = true, .model_revision_verified = true, .preprocessing_verified = true, .vector_space_verified = true, .dimensions = 3 }};
        const e = evaluate(r, .{ .version = 1, .configuration_id = "destination", .requirements = &embedding_caps });
        try std.testing.expectEqualStrings("model_contract_unknown", e.reason_code);
    };
}

test "backup preflight retired resolver policies and deterministic resolvers are inert" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    var cfg: resolver_catalog.ResolverConfig = .{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "{{_entity.text}}" };
    const deterministic = try resolver_catalog.serializeCatalog(a, &.{cfg});
    defer a.free(deterministic);
    try inspector.metadataJson(deterministic);
    cfg.retired = true;
    cfg.inline_managed = true;
    cfg.name_embedding = "retired-model";
    const retired = try resolver_catalog.serializeCatalog(a, &.{cfg});
    defer a.free(retired);
    try inspector.metadataJson(retired);
    try std.testing.expectEqual(@as(usize, 0), report.requirements.items.len);
}

test "backup preflight model chunker source references and inline media share runtime parsing" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(
        \\[{"name":"chunks","kind":"chunk","source_field":"body","chunker_json":"{\"provider\":\"antfly\",\"embedding\":{\"model\":\"media-model\",\"dimensions\":3}}"}]
    );
    try std.testing.expectEqual(@as(u64, 1), count(report, .model));
    try std.testing.expectEqual(@as(u32, 3), report.requirements.items[0].dimensions);
    try inspector.document("doc", "{\"body\":{\"type\":\"source\",\"uri\":\"s3://bucket/clip.mp4\",\"mime_type\":\"video/mp4\",\"version\":\"v1\",\"version_kind\":\"s3_version_id\",\"length\":419430400}}", "");
    try std.testing.expectEqual(@as(u64, 1), count(report, .connection));
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_local));
    const local = try std.json.Stringify.valueAlloc(a, .{ .body = .{ .type = "source", .uri = "https://localhost/clip.mp4", .mime_type = "video/mp4", .version = "\"v1\"", .length = 419430400 } }, .{});
    defer a.free(local);
    try inspector.document("local", local, "");
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    try inspector.document("inline", "{\"body\":{\"type\":\"media\",\"mime_type\":\"image/png\",\"data\":\"AQID\"}}", "");
    try std.testing.expectEqual(@as(u64, 1), count(report, .embedded));
    try std.testing.expect(report.analysis_complete);
    // Incomplete references cannot silently disappear from the inventory.
    try inspector.document("invalid", "{\"body\":{\"type\":\"source\",\"uri\":\"s3://bucket/clip.mp4\"}}", "");
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_unresolved));
    try std.testing.expect(!report.analysis_complete);
}

test "backup preflight bucket authorities are not local HTTP gateways" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    for ([_][]const u8{ "s3://bucket/object", "s3://bucket.local/object" }) |uri| try inspector.locator(uri, "d", "media");
    try std.testing.expectEqual(@as(u64, 2), count(report, .connection));
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_local));
    for (report.requirements.items) |*r| {
        const caps = [_]Capability{.{ .requirement_sha256 = &r.sha256, .supported = true }};
        try std.testing.expectEqual(Verdict.compatible, evaluate(r, .{ .version = 1, .configuration_id = "configured-bucket", .requirements = &caps }).verdict);
    }
    try inspector.locator("http://gateway/object", "d", "media");
    try inspector.locator("https://127.0.0.1/object", "d", "media");
    try std.testing.expectEqual(@as(u64, 2), count(report, .source_local));
}

test "backup preflight active sources incidental text and bounded examples" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{ .max_examples = 2, .max_definitions = 1 } };
    defer inspector.deinit();
    try inspector.metadataJson("[{\"name\":\"pdf\",\"kind\":\"asset\",\"source_field\":\"media\",\"producer_json\":\"{\\\"type\\\":\\\"document_extraction\\\"}\"}]");
    try inspector.document("doc:1", "{\"body\":\"/private/notes https://localhost:9999\",\"media\":\"https://example.org/a.pdf\"}", "");
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_local));
    for (0..1000) |_| try inspector.document("doc:2", "{\"media\":\"file:///private/paper.pdf\"}", "");
    try std.testing.expectEqual(@as(u64, 1000), count(report, .source_local));
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_external));
    try std.testing.expectEqual(@as(usize, 2), report.examples);
    try std.testing.expectError(error.PreflightLimitExceeded, inspector.metadataJson("[{\"kind\":\"asset\",\"field\":\"other\"}]"));
}

test "backup preflight unknown destination and secret redaction" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.locator("https://person:SECRET@example.org/a.zip!/b.pdf?X-Amz-Signature=TOKEN&expires=123&page=2", "https://example.org/?token=DOCSECRET", "media");
    try std.testing.expect(count(report, .source_expiring) > 0);
    try std.testing.expect(count(report, .archive_retrieval) > 0);
    try std.testing.expect(count(report, .page_retrieval) > 0);
    const json = try report.jsonAlloc(null);
    defer a.free(json);
    for ([_][]const u8{ "TOKEN", "DOCSECRET", "person", "example.org", "SECRET" }) |secret| try std.testing.expect(std.mem.indexOf(u8, json, secret) == null);
    var empty: Report = .{ .allocator = a, .scan_complete = true };
    defer empty.deinit();
    const unknown = try empty.jsonAlloc(null);
    defer a.free(unknown);
    try std.testing.expect(std.mem.indexOf(u8, unknown, "\"verdict\":\"unknown\"") != null);
}

test "backup preflight model evidence includes query ingestion revision preprocessing and vector space" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    try report.add(.{}, .model, "model/version/preprocessing/space", "", "embedding", 3);
    const r = &report.requirements.items[0];
    try std.testing.expectEqual(Verdict.unknown, evaluate(r, null).verdict);
    var caps = [_]Capability{.{ .requirement_sha256 = &r.sha256, .supported = true, .dimensions = 3 }};
    const destination: Destination = .{ .version = 1, .configuration_id = "revision-1", .requirements = &caps };
    try std.testing.expectEqual(Verdict.blocked, evaluate(r, destination).verdict);
    caps[0].query_embedding = true;
    caps[0].future_ingestion = true;
    caps[0].model_revision_verified = true;
    caps[0].preprocessing_verified = true;
    caps[0].vector_space_verified = true;
    try std.testing.expectEqual(Verdict.compatible, evaluate(r, destination).verdict);
    caps[0].dimensions = 4;
    try std.testing.expectEqual(Verdict.blocked, evaluate(r, destination).verdict);
}

test "backup preflight implicit Apple generator requires exact producer evidence" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(
        \\[{"name":"summary","kind":"asset","source_field":"body","producer_json":"{\"type\":\"generator\",\"config\":{\"provider\":\"apple\"}}"}]
    );
    try std.testing.expectEqual(@as(u64, 1), count(report, .producer_model));
    const r = &report.requirements.items[0];
    try std.testing.expectEqual(Verdict.unknown, evaluate(r, null).verdict);
    const unattested = try report.jsonAlloc("{\"version\":1,\"configuration_id\":\"destination\"}");
    defer a.free(unattested);
    try std.testing.expect(std.mem.indexOf(u8, unattested, "\"compatibility\":{\"verdict\":\"unknown\"") != null);
    var caps = [_]Capability{.{ .requirement_sha256 = &r.sha256, .supported = true }};
    const destination: Destination = .{ .version = 1, .configuration_id = "destination", .requirements = &caps };
    try std.testing.expectEqual(Verdict.blocked, evaluate(r, destination).verdict);
    caps[0].future_ingestion = true;
    try std.testing.expectEqual(Verdict.blocked, evaluate(r, destination).verdict);
    caps[0].model_revision_verified = true;
    try std.testing.expectEqual(Verdict.compatible, evaluate(r, destination).verdict);

    // Evidence for the same provider must not certify a different source or
    // producer configuration. Keep a copy across requirement-list growth.
    const original = r.sha256;
    caps[0].requirement_sha256 = &original;
    try inspector.metadataJson(
        \\[{"name":"summary","kind":"asset","source_field":"title","producer_json":"{\"type\":\"generator\",\"config\":{\"provider\":\"apple\"}}"}]
    );
    try inspector.metadataJson(
        \\[{"name":"summary","kind":"asset","source_field":"body","producer_json":"{\"type\":\"generator\",\"config\":{\"provider\":\"apple\",\"temperature\":0.5}}"}]
    );
    try std.testing.expectEqual(@as(usize, 3), report.requirements.items.len);
    for (report.requirements.items[1..]) |*changed| {
        try std.testing.expect(!eq(&original, &changed.sha256));
        try std.testing.expectEqual(Verdict.unknown, evaluate(changed, destination).verdict);
    }
}

test "backup preflight implicit producer detection excludes deterministic producers" {
    const a = std.testing.allocator;
    const Case = struct { json: []const u8, model_count: u64 };
    for ([_]Case{
        .{ .json = "{\"producer\":{\"type\":\"transcriber\",\"config\":{\"provider\":\"apple\"}}}", .model_count = 1 },
        .{ .json = "{\"producer\":{\"type\":\"extractor\",\"config\":{\"provider\":\"antfly\"}}}", .model_count = 1 },
        .{ .json = "{\"producer\":{\"type\":\"reader\",\"config\":{\"provider\":\"apple\"}}}", .model_count = 1 },
        .{ .json = "{\"producer\":{\"type\":\"reader\"}}", .model_count = 0 },
        .{ .json = "{\"producer\":{\"type\":\"copy\"}}", .model_count = 0 },
        .{ .json = "{\"kind\":\"asset\",\"source_artifact_name\":\"vectors\",\"producer\":{\"type\":\"vector_labels\",\"config\":{\"labels\":[{\"label\":\"speech\"}]}}}", .model_count = 1 },
        .{ .json = "{\"kind\":\"asset\",\"source_artifact_name\":\"clips\",\"producer\":{\"type\":\"media_output\",\"config\":{\"output_name\":\"speech\"}}}", .model_count = 1 },
        .{ .json = "{\"producer\":{\"type\":\"document_extraction\"}}", .model_count = 1 },
        .{ .json = "{\"producer\":{\"type\":\"document_extraction\",\"config\":{\"ocr_fallback\":false}}}", .model_count = 0 },
        .{ .json = "{\"producer\":{\"type\":\"document_extraction\",\"config\":{\"ocr\":false}}}", .model_count = 0 },
        .{ .json = "{\"producer\":{\"type\":\"document_extraction\",\"config\":{\"ocr\":{\"enabled\":false}}}}", .model_count = 0 },
        .{ .json = "{\"producer\":{\"type\":\"document_extraction\",\"config\":{\"ocr\":false,\"transcription\":true}}}", .model_count = 1 },
    }) |case| {
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.metadataJson(case.json);
        try std.testing.expectEqual(case.model_count, count(report, .producer_model));
    }
}

test "backup preflight embedded semantic model remains subject to complete model evidence" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(
        \\[{"name":"dense","kind":"embedding","source_field":"body","source_template":"{{{body}}}","expected_dims":3,"producer_json":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"exact-model\",\"endpoint\":\"antfly:embedded\",\"sparse\":false}"}]
    );
    try std.testing.expectEqual(@as(u64, 1), count(report, .model));
    try std.testing.expectEqual(@as(u64, 1), count(report, .producer_model));
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_unresolved));
    try std.testing.expect(report.analysis_complete);
    const caps = try a.alloc(Capability, report.requirements.items.len);
    defer a.free(caps);
    const destination: Destination = .{ .version = 1, .configuration_id = "embedded-runtime", .requirements = caps };
    for (report.requirements.items, caps) |*r, *cap| {
        cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .dimensions = r.dimensions };
    }
    for (report.requirements.items, caps) |*r, *cap| {
        try std.testing.expectEqual(Verdict.unknown, evaluate(r, null).verdict);
        try std.testing.expectEqual(Verdict.blocked, evaluate(r, destination).verdict);
        cap.query_embedding = true;
        cap.future_ingestion = true;
        cap.model_revision_verified = true;
        cap.preprocessing_verified = true;
        cap.vector_space_verified = true;
    }
    const evidence = try std.json.Stringify.valueAlloc(a, destination, .{});
    defer a.free(evidence);
    const compatible = try report.jsonAlloc(evidence);
    defer a.free(compatible);
    try std.testing.expect(std.mem.indexOf(u8, compatible, "\"compatibility\":{\"verdict\":\"compatible\"") != null);

    // The exemption belongs only to the model endpoint, never media inputs.
    try inspector.document("d", "{\"body\":\"<<<dotprompt:media:url antfly:embedded>>>\"}", "");
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_unresolved));
    const blocked = try report.jsonAlloc(evidence);
    defer a.free(blocked);
    try std.testing.expect(std.mem.indexOf(u8, blocked, "\"compatibility\":{\"verdict\":\"blocked\"") != null);
}

test "backup preflight embedded endpoint exemption rejects unknown schemes and other schemas" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "{\"version\":2,\"provider\":\"antfly\",\"model\":\"exact-model\",\"endpoint\":\"antfly:unknown\",\"sparse\":false}",
        "{\"version\":2,\"provider\":\"openai\",\"model\":\"exact-model\",\"endpoint\":\"antfly:embedded\",\"sparse\":false}",
        "{\"provider\":\"antfly\",\"model\":\"exact-model\",\"endpoint\":\"antfly:embedded\",\"sparse\":false}",
        "{\"version\":2,\"provider\":\"antfly\",\"model\":\"exact-model\",\"endpoint\":\"antfly:embedded\"}",
        "{\"version\":2,\"provider\":\"antfly\",\"model\":\"exact-model\",\"endpoint\":\"antfly:embedded\",\"sparse\":false,\"url\":\"antfly:embedded\"}",
    }) |json| {
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.metadataJson(json);
        try std.testing.expectEqual(@as(u64, 1), count(report, .source_unresolved));
        try std.testing.expect(!report.analysis_complete);
        for (report.requirements.items) |*r| {
            if (r.kind == .source_unresolved) try std.testing.expectEqual(Verdict.blocked, evaluate(r, null).verdict);
        }
    }
}

test "backup preflight static templates and dynamic loaders never execute" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson("[{\"kind\":\"embedding\",\"source_template\":\"{{remoteMedia url=image}}\",\"expected_dims\":3}]");
    try inspector.document("d", "{\"image\":\"http://localhost:9876/never-fetch\"}", "");
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    try inspector.metadataJson("[{\"kind\":\"asset\",\"source_template\":\"{{customLoader secret}}\"}]");
    try std.testing.expect(!report.analysis_complete);
    try std.testing.expect(count(report, .dynamic_loader) > 0);
}

test "backup preflight rejects unsupported selectors and reports template connections" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson("[{\"kind\":\"embedding\",\"source_template\":\"{{remoteMedia url=image credentials=\\\"private-media\\\"}}\"}]");
    try std.testing.expectEqual(@as(u64, 1), count(report, .connection));
    try inspector.metadataJson("[{\"kind\":\"asset\",\"source_template\":\"{{media[*].url}}\",\"producer\":{\"type\":\"reader\"}}]");
    try std.testing.expect(!report.analysis_complete);
    try inspector.locator("https://example.org/a?%58-Amz-%53ignature=secret", "d", "media");
    try std.testing.expect(count(report, .source_expiring) > 0);
    const bytes = try report.jsonAlloc(null);
    defer a.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "private-media") == null);
}

test "backup preflight stored vectors alone cannot establish a model contract" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.indexCatalogJson("{\"known\":{\"type\":\"embeddings\",\"dimension\":3,\"embedder\":{\"model\":\"exact-model\"}}}");
    const before = count(report, .model);
    try inspector.storedModel("known", "d");
    try std.testing.expectEqual(before, count(report, .model));
    try inspector.storedModel("missing", "d");
    const requirement = &report.requirements.items[report.requirements.items.len - 1];
    const caps = [_]Capability{.{ .requirement_sha256 = &requirement.sha256, .supported = true, .query_embedding = true, .future_ingestion = true, .model_revision_verified = true, .preprocessing_verified = true, .vector_space_verified = true, .dimensions = 3 }};
    const result = evaluate(requirement, .{ .version = 1, .configuration_id = "current", .requirements = &caps });
    try std.testing.expectEqual(Verdict.unknown, result.verdict);
    try std.testing.expectEqualStrings("model_contract_unknown", result.reason_code);
    try std.testing.expect(!eq(&requirementHash(.connection, "same"), &requirementHash(.page_retrieval, "same")));
}

test "backup preflight artifact consumers follow retained payload provenance only" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson("[{\"kind\":\"embedding\",\"field\":\"text\",\"embedding_input\":\"pdf_page_images\",\"source_artifact_name\":\"chunks\",\"expected_dims\":3,\"producer\":{\"model\":\"exact-model\"}}]");
    try inspector.document("d", "{\"body\":\"file:///private/active.pdf\",\"_source_field\":\"body\",\"_provenance\":{\"url\":\"file:///private/historical\"}}", "chunks");
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    try inspector.document("d", "{\"body\":\"https://media.example.org/pages.pdf\",\"_source_field\":\"body\",\"_provenance\":{\"url\":\"file:///private/historical\"}}", "chunks");
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    try inspector.metadataJson("[{\"kind\":\"asset\",\"producer\":{\"type\":\"custom-future-loader\"}}]");
    try std.testing.expect(!report.analysis_complete);
}

test "backup preflight configured credentials are active but schema examples are inert" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson("{\"producer\":{\"type\":\"extractor\",\"config\":{\"connection_id\":\"private-connection\",\"credentials_path\":\"/private/credentials.json\",\"schema\":{\"examples\":[{\"url\":\"file:///irrelevant-example\"}]}}}}");
    try std.testing.expectEqual(@as(u64, 1), count(report, .connection));
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    const output = try report.jsonAlloc(null);
    defer a.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "private-connection") == null);
    try std.testing.expect(std.mem.indexOf(u8, output, "/private") == null);
}

test "backup preflight grouped PDF embedding uses runtime unit text" {
    const a = std.testing.allocator;
    const group = @import("../db/group_sources.zig").Config{ .group_by = "thread", .order_by = "ordinal", .field = "body" };
    try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "turns", .kind = .group, .source = group });
    const group_json = try std.json.Stringify.valueAlloc(a, group, .{});
    defer a.free(group_json);
    const producer = "{\"version\":2,\"provider\":\"antfly\",\"model\":\"mock\",\"endpoint\":\"antfly:embedded\",\"sparse\":false}";
    try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "pages", .kind = .embedding, .field = "body", .source_artifact_name = "chunks", .embedding_input = .pdf_page_images, .expected_dims = 3, .producer_json = producer });
    const projected = try std.json.parseFromSlice(Value, a, "{\"_group_unit\":{\"text\":\"file:///private/group.pdf\"}}", .{});
    defer projected.deinit();
    try std.testing.expectEqualStrings("file:///private/group.pdf", enrichment_types.sourceFieldValue(projected.value, "_group_unit.text", true).string);
    const metadata = try @import("../db/catalog/enrichment_catalog.zig").serializeCatalog(a, &.{
        .{ .name = "pages", .kind = .embedding, .source_field = "body", .source_artifact_name = "chunks", .embedding_input = .pdf_page_images, .expected_dims = 3, .producer_json = producer },
        .{ .name = "chunks", .kind = .chunk, .source_field = "ignored", .source_artifact_name = "turns", .chunk_size = 32 },
        .{ .name = "turns", .kind = .asset, .source_field = "_group_unit", .group_source_json = group_json, .producer_json = "{\"type\":\"copy\"}" },
    });
    defer a.free(metadata);
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(metadata);
    try inspector.document("anchor", "{\"text\":\"file:///private/group.pdf\"}", "turns");
    const caps = try a.alloc(Capability, report.requirements.items.len);
    defer a.free(caps);
    for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .future_ingestion = true, .model_revision_verified = true, .query_embedding = true, .preprocessing_verified = true, .vector_space_verified = true, .dimensions = r.dimensions };
    const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "model-only", .requirements = caps }, .{});
    defer a.free(evidence);
    const json = try report.jsonAlloc(evidence);
    defer a.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"verdict\":\"blocked\"") != null);
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    try std.testing.expect(report.analysis_complete);
    try inspector.document("anchor", "{\"_group_artifact\":\"turns\",\"_group_unit\":{\"text\":\"file:///private/group.pdf\"}}", "");
    try std.testing.expectEqual(@as(u64, 2), count(report, .source_local));
    try inspector.document("anchor", "invalid group asset", "turns");
    try std.testing.expect(!report.analysis_complete);
    try std.testing.expect(count(report, .source_unresolved) > 0);
}

test "backup preflight composed media marker is not certifiable" {
    const a = std.testing.allocator;
    const template = "<<<dotprompt:media:{{kind}} {{uri}}>>>";
    const raw = "{\"kind\":\"url\",\"uri\":\"file:///private/composed.pdf\"}";
    const rendered = try @import("../../template.zig").renderDocument(a, template, raw);
    defer a.free(rendered);
    try std.testing.expectEqualStrings("<<<dotprompt:media:url file:///private/composed.pdf>>>", rendered);
    const parts = try @import("../../template.zig").textToParts(a, rendered);
    defer @import("../../template.zig").freeContentParts(a, parts);
    try std.testing.expectEqual(@as(usize, 1), parts.len);
    try std.testing.expectEqualStrings("file:///private/composed.pdf", parts[0].media_url);
    const producer = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"mock\"}}";
    try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "summary", .kind = .asset, .template = template, .producer_json = producer });
    const metadata = try @import("../db/catalog/enrichment_catalog.zig").serializeCatalog(a, &.{.{ .name = "summary", .kind = .asset, .source_field = "", .source_template = template, .producer_json = producer }});
    defer a.free(metadata);
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(metadata);
    try inspector.document("doc", raw, "");
    const caps = try a.alloc(Capability, report.requirements.items.len);
    defer a.free(caps);
    for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .future_ingestion = true, .model_revision_verified = true };
    const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "model-only", .requirements = caps }, .{});
    defer a.free(evidence);
    const json = try report.jsonAlloc(evidence);
    defer a.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"verdict\":\"blocked\"") != null);
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    try std.testing.expect(report.analysis_complete);
}

test "backup preflight runtime credentials keyword is inspectable" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson("{\"kind\":\"asset\",\"source_template\":\"{{remoteText url=\\\"https://example.org/private\\\" credentials=\\\"my-credential\\\"}}\",\"producer\":{\"type\":\"copy\"}}");
    const json = try report.jsonAlloc(null);
    defer a.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "my-credential") == null);
    try std.testing.expectEqual(@as(u64, 1), count(report, .connection));
    try std.testing.expect(report.analysis_complete);
}

test "backup preflight template AST respects argument order quoting and runtime path lookup" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "{{remoteText credentials='team media' url=media.uri}}",
        "{{remoteMedia url=media.uri credentials=credential}}",
        "{{remotePDF credentials=credential url=media.uri}}",
    }) |source| {
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        const metadata = try std.json.Stringify.valueAlloc(a, .{ .kind = "asset", .source_template = source, .producer = .{ .type = "copy" } }, .{});
        defer a.free(metadata);
        try inspector.metadataJson(metadata);
        try inspector.document("doc", "{\"media\":{\"uri\":\"https://example.org/private\"},\"media.uri\":\"file:///decoy\",\"credential\":\"team media\"}", "");
        try std.testing.expect(report.analysis_complete);
        try std.testing.expectEqual(@as(u64, 1), count(report, .source_external));
        try std.testing.expectEqual(@as(u64, 0), count(report, .source_local));
        try std.testing.expect(count(report, .connection) > 0);
    }
}

test "backup preflight unknown template syntax never invokes helpers" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "{{remoteText url=(lookup this 'url')}}",
        "{{#each urls}}{{remoteText url=this}}{{/each}}",
        "{{> remotePartial}}",
        "{{remoteText url=url credential='wrong-keyword'}}",
    }) |source| {
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        const metadata = try std.json.Stringify.valueAlloc(a, .{ .kind = "asset", .source_template = source, .producer = .{ .type = "copy" } }, .{});
        defer a.free(metadata);
        try inspector.metadataJson(metadata);
        try std.testing.expect(!report.analysis_complete);
        try std.testing.expect(count(report, .dynamic_loader) > 0);
    }
}

test "backup preflight combined substitutions respect escaping and rendered size limits" {
    const a = std.testing.allocator;
    const raw = "{\"prefix\":\"<<<dotprompt:media:ur\",\"suffix\":\"l file:///private/parts.pdf>>>\"}";
    for ([_][]const u8{ "{{{prefix}}}{{{suffix}}}", "{{prefix}}{{suffix}}" }, 0..) |source, i| {
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        const metadata = try std.json.Stringify.valueAlloc(a, .{ .kind = "asset", .source_template = source, .producer = .{ .type = "generator", .config = .{ .provider = "antfly", .model = "mock" } } }, .{});
        defer a.free(metadata);
        try inspector.metadataJson(metadata);
        try inspector.document("doc", raw, "");
        try std.testing.expectEqual(@as(u64, if (i == 0) 1 else 0), count(report, .source_local));
        try std.testing.expect(report.analysis_complete);
    }
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{ .max_document_bytes = raw.len } };
    defer inspector.deinit();
    try inspector.metadataJson("{\"kind\":\"asset\",\"source_template\":\"{{{prefix}}}{{{prefix}}}{{{prefix}}}{{{prefix}}}{{{prefix}}}{{{prefix}}}\",\"producer\":{\"type\":\"copy\"}}");
    try std.testing.expectError(error.PreflightLimitExceeded, inspector.document("doc", raw, ""));
}

test "backup preflight grouped model evidence binds normalized source preprocessing" {
    const a = std.testing.allocator;
    var original: Digest = undefined;
    for ([_][]const u8{ "body", "other" }, 0..) |field, i| {
        const group_json = try std.json.Stringify.valueAlloc(a, .{ .group_by = "thread", .order_by = "ordinal", .field = field }, .{});
        defer a.free(group_json);
        const metadata = try @import("../db/catalog/enrichment_catalog.zig").serializeCatalog(a, &.{
            .{ .name = "pages", .kind = .embedding, .source_field = "ignored", .source_artifact_name = "turns", .embedding_input = .pdf_page_images, .expected_dims = 3, .producer_json = "{\"version\":2,\"provider\":\"antfly\",\"model\":\"mock\",\"endpoint\":\"antfly:embedded\",\"sparse\":false}" },
            .{ .name = "turns", .kind = .asset, .source_field = "_group_unit", .group_source_json = group_json, .producer_json = "{\"type\":\"copy\"}" },
        });
        defer a.free(metadata);
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.metadataJson(metadata);
        // The public manifest and durable catalog spell defaults differently.
        // They must resolve to the same group rather than conflict by JSON order.
        const public = try std.json.Stringify.valueAlloc(a, .{ .kind = "group", .name = "turns", .source = .{ .field = field, .order_by = "ordinal", .group_by = "thread" } }, .{});
        defer a.free(public);
        try inspector.metadataJson(public);
        try std.testing.expectEqual(@as(u64, 1), count(report, .model));
        for (report.requirements.items) |*requirement| {
            if (requirement.kind != .model) continue;
            if (i == 0) {
                original = requirement.sha256;
            } else {
                const caps = [_]Capability{.{ .requirement_sha256 = &original, .supported = true, .future_ingestion = true, .query_embedding = true, .model_revision_verified = true, .preprocessing_verified = true, .vector_space_verified = true, .dimensions = 3 }};
                try std.testing.expect(!eq(&original, &requirement.sha256));
                try std.testing.expectEqual(Verdict.unknown, evaluate(requirement, .{ .version = 1, .configuration_id = "original-group", .requirements = &caps }).verdict);
            }
        }
    }
}

test "backup preflight template inventory limits preserve selector ownership" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{ .max_requirements = 1 } };
    defer inspector.deinit();
    try std.testing.expectError(error.PreflightLimitExceeded, inspector.metadataJson("{\"kind\":\"asset\",\"source_template\":\"{{remoteText url='https://one.example/a'}}{{remoteText url='https://two.example/b'}}\",\"producer\":{\"type\":\"copy\"}}"));
}

test "backup preflight opaque helper text cannot certify active media" {
    const a = std.testing.allocator;
    const source = "{{{remoteText url='data:text/plain,%3C%3C%3Cdotprompt%3Amedia%3A'}}}url {{{uri}}}>>>";
    const raw = "{\"uri\":\"file:///private/helper.pdf\"}";
    const parts = try @import("../../template_remote.zig").renderJsonToPartsWithConfig(a, source, raw, .{ .io = std.testing.io });
    defer template_runtime.freeContentParts(a, parts);
    try std.testing.expectEqual(@as(usize, 1), parts.len);
    try std.testing.expectEqualStrings("file:///private/helper.pdf", parts[0].media_url);
    const producer = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"mock\"}}";
    try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "summary", .kind = .asset, .template = source, .producer_json = producer });
    const metadata = try @import("../db/catalog/enrichment_catalog.zig").serializeCatalog(a, &.{.{ .name = "summary", .kind = .asset, .source_field = "", .source_template = source, .producer_json = producer }});
    defer a.free(metadata);
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(metadata);
    try inspector.document("doc", raw, "");
    const caps = try a.alloc(Capability, report.requirements.items.len);
    defer a.free(caps);
    for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .future_ingestion = true, .model_revision_verified = true };
    const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "model-only", .requirements = caps }, .{});
    defer a.free(evidence);
    const json = try report.jsonAlloc(evidence);
    defer a.free(json);
    try std.testing.expect(!report.analysis_complete);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"compatibility\":{\"verdict\":\"compatible\"") == null);
    try std.testing.expect(count(report, .dynamic_loader) > 0);
}

test "backup preflight whitespace control matches runtime" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "<<<dotprompt:media:\x0c{{~kind}} {{{uri}}}>>>",
        "<<<dotprompt:media:\x0b{{~kind}} {{{uri}}}>>>",
        "<<<dotprompt:media:{{prefix~}}\x0c{{kind}} {{{uri}}}>>>",
        "<<<dotprompt:media:{{prefix~}}\x0b{{kind}} {{{uri}}}>>>",
        "{{media url=uri}}",
    }) |source| {
        const raw = "{\"kind\":\"url\",\"uri\":\"file:///private/whitespace.pdf\"}";
        const rendered = try template_runtime.renderDocument(a, source, raw);
        defer a.free(rendered);
        try std.testing.expectEqualStrings("<<<dotprompt:media:url file:///private/whitespace.pdf>>>", rendered);
        const parts = try template_runtime.textToParts(a, rendered);
        defer template_runtime.freeContentParts(a, parts);
        try std.testing.expectEqualStrings("file:///private/whitespace.pdf", parts[0].media_url);
        const producer = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"mock\"}}";
        try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "summary", .kind = .asset, .template = source, .producer_json = producer });
        const metadata = try @import("../db/catalog/enrichment_catalog.zig").serializeCatalog(a, &.{.{ .name = "summary", .kind = .asset, .source_field = "", .source_template = source, .producer_json = producer }});
        defer a.free(metadata);
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.metadataJson(metadata);
        try inspector.document("doc", raw, "");
        const caps = try a.alloc(Capability, report.requirements.items.len);
        defer a.free(caps);
        for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .future_ingestion = true, .model_revision_verified = true };
        const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "model-only", .requirements = caps }, .{});
        defer a.free(evidence);
        const json = try report.jsonAlloc(evidence);
        defer a.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"verdict\":\"blocked\"") != null);
        try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    }
}

test "backup preflight reader credential aliases require separate destination evidence" {
    const a = std.testing.allocator;
    inline for (.{ "api_key", "bearer_token", "capability_token" }) |field_name| {
        const producer = try std.fmt.allocPrint(a, "{{\"type\":\"reader\",\"config\":{{\"provider\":\"antfly\",\"model\":\"mock\",\"api_url\":\"https://inference.example\",\"{s}\":\"TEST_TOKEN\"}}}}", .{field_name});
        defer a.free(producer);
        const parsed_producer = try std.json.parseFromSlice(Value, a, producer, .{});
        defer parsed_producer.deinit();
        const runtime_config = try std.json.parseFromValue(@import("antfly_readers").Config, a, get(parsed_producer.value, "config"), .{});
        defer runtime_config.deinit();
        try runtime_config.value.validate();
        try std.testing.expectEqualStrings("TEST_TOKEN", @field(runtime_config.value, field_name).?);
        try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "read", .kind = .asset, .field = "image", .producer_json = producer });
        const metadata = try @import("../db/catalog/enrichment_catalog.zig").serializeCatalog(a, &.{.{ .name = "read", .kind = .asset, .source_field = "image", .producer_json = producer }});
        defer a.free(metadata);
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.metadataJson(metadata);
        try inspector.document("doc", "{\"image\":\"data:image/png;base64,YQ==\"}", "");
        try std.testing.expectEqual(@as(u64, 1), count(report, .connection));
        var caps: std.ArrayListUnmanaged(Capability) = .empty;
        defer caps.deinit(a);
        for (report.requirements.items) |*r| if (r.kind != .connection) {
            try caps.append(a, .{ .requirement_sha256 = &r.sha256, .supported = true, .future_ingestion = true, .model_revision_verified = true });
        };
        const missing = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "no-credentials", .requirements = caps.items }, .{});
        defer a.free(missing);
        const unknown = try report.jsonAlloc(missing);
        defer a.free(unknown);
        try std.testing.expect(std.mem.indexOf(u8, unknown, "\"compatibility\":{\"verdict\":\"unknown\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, unknown, "TEST_TOKEN") == null);
        for (report.requirements.items) |*r| if (r.kind == .connection) {
            try caps.append(a, .{ .requirement_sha256 = &r.sha256, .supported = true });
        };
        const supplied = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "with-credentials", .requirements = caps.items }, .{});
        defer a.free(supplied);
        const verified = try report.jsonAlloc(supplied);
        defer a.free(verified);
        try std.testing.expect(std.mem.indexOf(u8, verified, "\"compatibility\":{\"verdict\":\"compatible\"") != null);
        caps.items[caps.items.len - 1].supported = false;
        for (report.requirements.items) |*r| if (r.kind == .connection) {
            try std.testing.expectEqual(Verdict.blocked, evaluate(r, .{ .version = 1, .configuration_id = "revoked", .requirements = caps.items }).verdict);
        };
    }
}

test "backup preflight copy output is inert text" {
    const a = std.testing.allocator;
    const text = "Documentation example: <<<dotprompt:media:url file:///private/example.pdf>>>";
    var client = @import("httpx").Client.initWithConfig(a, std.testing.io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = @import("../../asset_producer_runtime.zig").Runtime.init(a, &client);
    defer runtime.deinit();
    const output = try runtime.producer().produce(a, .{ .producer_type = .copy, .config_json = "", .source_text = text });
    defer a.free(output);
    try std.testing.expectEqualStrings(text, output);
    for ([_][]const u8{ "", "{{{body}}}", "{{media url=uri}}" }) |source_template| {
        const producer = "{\"type\":\"copy\"}";
        try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "verbatim", .kind = .asset, .field = "body", .template = source_template, .producer_json = producer });
        const metadata = try @import("../db/catalog/enrichment_catalog.zig").serializeCatalog(a, &.{.{ .name = "verbatim", .kind = .asset, .source_field = "body", .source_template = source_template, .producer_json = producer }});
        defer a.free(metadata);
        const raw = try std.json.Stringify.valueAlloc(a, .{ .body = text, .uri = "file:///private/example.pdf" }, .{});
        defer a.free(raw);
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.metadataJson(metadata);
        try inspector.document("doc", raw, "");
        const json = try report.jsonAlloc("{\"version\":1,\"configuration_id\":\"empty\"}");
        defer a.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"compatibility\":{\"verdict\":\"compatible\"") != null);
        try std.testing.expectEqual(@as(u64, 0), count(report, .source_local));
    }
}

test "backup preflight encoded query values are not credential parameters" {
    const a = std.testing.allocator;
    const Case = struct { query: []const u8, expiring: bool = false, page: bool = false, archive: bool = false };
    for ([_]Case{
        .{ .query = "note=literal%26token%3Dexample" },
        .{ .query = "note=literal%26page%3D2%26member%3Dfoo" },
        .{ .query = "note=literal%2526token%253Dexample" },
        .{ .query = "note%26token=literal" },
        .{ .query = "%74oken=example", .expiring = true },
        .{ .query = "note=literal&%58-Amz-%53ignature=example", .expiring = true },
        .{ .query = "%70age=2&%6dember=foo", .page = true, .archive = true },
    }) |case| {
        const source = try std.fmt.allocPrint(a, "https://cdn.example.org/image.png?{s}", .{case.query});
        defer a.free(source);
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.locator(source, "doc", "image");
        try std.testing.expectEqual(@as(u64, @intFromBool(case.expiring)), count(report, .source_expiring));
        try std.testing.expectEqual(@as(u64, @intFromBool(case.page)), count(report, .page_retrieval));
        try std.testing.expectEqual(@as(u64, @intFromBool(case.archive)), count(report, .archive_retrieval));
        const caps = try a.alloc(Capability, report.requirements.items.len);
        defer a.free(caps);
        for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true };
        const destination = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "public-cdn", .requirements = caps }, .{});
        defer a.free(destination);
        const json = try report.jsonAlloc(destination);
        defer a.free(json);
        const expected = if (case.expiring) "\"compatibility\":{\"verdict\":\"blocked\"" else "\"compatibility\":{\"verdict\":\"compatible\"";
        try std.testing.expect(std.mem.indexOf(u8, json, expected) != null);
    }
}

test "backup preflight literal copy still inventories executed template fetches and active consumers" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(
        \\[{"name":"verbatim","kind":"asset","source_template":"{{remoteText url=uri}}","producer":{"type":"copy"}}]
    );
    try inspector.document("doc", "{\"uri\":\"file:///private/fetched.txt\"}", "");
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    try std.testing.expect(report.analysis_complete);
    try inspector.metadataJson(
        \\[{"name":"prompt","kind":"asset","source_template":"{{{body}}}","producer":{"type":"generator","config":{"provider":"antfly","model":"mock"}}}]
    );
    try inspector.document("doc", "{\"body\":\"<<<dotprompt:media:url file:///private/active.pdf>>>\"}", "");
    try std.testing.expectEqual(@as(u64, 2), count(report, .source_local));
}

test "backup preflight credential URL aliases share the URL safety policy" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "https://media.example.org/doc.pdf?api_key=TEST_SECRET",
        "https://media.example.org/doc.pdf?%61pi_key=TEST_SECRET",
        "https://media.example.org/doc.pdf?bearer_token=TEST_SECRET",
        "https://media.example.org/doc.pdf#capability_token=TEST_SECRET",
    }) |source| {
        try std.testing.expect(@import("../../common/credential_safety.zig").urlContainsCredentials(source));
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.locator(source, "doc", "image");
        const caps = try a.alloc(Capability, report.requirements.items.len);
        defer a.free(caps);
        for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true };
        const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "public", .requirements = caps }, .{});
        defer a.free(evidence);
        const json = try report.jsonAlloc(evidence);
        defer a.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"compatibility\":{\"verdict\":\"blocked\"") != null);
        try std.testing.expect(count(report, .source_expiring) > 0);
        try std.testing.expect(std.mem.indexOf(u8, json, "TEST_SECRET") == null);
    }
}

test "backup preflight fixed chunk input is literal text" {
    const a = std.testing.allocator;
    const text = "Documentation example: <<<dotprompt:media:url file:///private/example.pdf>>>";
    const chunks = try @import("../db/enrichment/chunker_stub.zig").chunkText(a, text, 1024, 0);
    defer {
        for (chunks) |*chunk| chunk.deinit(a);
        a.free(chunks);
    }
    try std.testing.expectEqualStrings(text, chunks[0].text.?);
    for ([_][]const u8{ "", "{{{body}}}" }) |source_template| {
        try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .template = source_template, .name = "chunks", .kind = .chunk, .field = "body", .chunk_size = 1024 });
        const metadata = try @import("../db/catalog/enrichment_catalog.zig").serializeCatalog(a, &.{.{ .name = "chunks", .kind = .chunk, .source_template = source_template, .source_field = "body", .chunk_size = 1024 }});
        defer a.free(metadata);
        const raw = try std.json.Stringify.valueAlloc(a, .{ .body = text }, .{});
        defer a.free(raw);
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.metadataJson(metadata);
        try inspector.document("doc", raw, "");
        const json = try report.jsonAlloc("{\"version\":1,\"configuration_id\":\"empty\"}");
        defer a.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"compatibility\":{\"verdict\":\"compatible\"") != null);
        try std.testing.expectEqual(@as(u64, 0), count(report, .source_local));
    }
}

test "backup preflight compressed documents respect the logical materialization budget" {
    const a = std.testing.allocator;
    // Zstd frame without a content-size field: raw JSON delimiters around
    // 256 RLE blocks, each expanding to 128 KiB. No external encoder needed.
    var compressed: std.ArrayListUnmanaged(u8) = .empty;
    defer compressed.deinit(a);
    try compressed.appendSlice(a, &.{ 0x28, 0xb5, 0x2f, 0xfd, 0, 0x58, 0x48, 0, 0 });
    try compressed.appendSlice(a, "{\"body\":\"");
    for (0..256) |_| try compressed.appendSlice(a, &.{ 2, 0, 0x10, 'a' });
    try compressed.appendSlice(a, &.{ 0x11, 0, 0 });
    try compressed.appendSlice(a, "\"}");
    try std.testing.expect(compressed.items.len < 4096);
    const decoded = try codec.decompressZstd(a, compressed.items);
    defer a.free(decoded);
    try std.testing.expectEqual(@as(usize, 32 * 1024 * 1024 + 11), decoded.len);
    const payload = try codec.encodeDocumentBatch(a, &.{.{ .key = "doc", .value_flags = codec.doc_value_flag_compressed, .value = compressed.items, .timestamp_ns = 0 }});
    defer a.free(payload);
    const backing = try a.alloc(u8, 16 * 1024 * 1024);
    defer a.free(backing);
    var buffer = std.heap.FixedBufferAllocator.init(backing);
    const bounded = buffer.allocator();
    var report: Report = .{ .allocator = bounded };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = bounded, .report = &report, .options = .{ .max_document_bytes = 4096 } };
    defer inspector.deinit();
    try std.testing.expectError(error.PreflightLimitExceeded, inspector.visit(.document_batch, payload));
}

test "backup preflight model chunk text does not activate media markers" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "", "{{{body}}}" }) |source_template| {
        const metadata = try @import("../db/catalog/enrichment_catalog.zig").serializeCatalog(a, &.{.{ .name = "chunks", .kind = .chunk, .source_field = "body", .source_template = source_template, .chunker_json = "{\"provider\":\"antfly\",\"embedding\":{\"model\":\"media-model\",\"dimensions\":3}}" }});
        defer a.free(metadata);
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.metadataJson(metadata);
        try inspector.document("doc", "{\"body\":\"<<<dotprompt:media:url file:///private/example.pdf>>>\"}", "");
        try std.testing.expectEqual(@as(u64, 1), count(report, .model));
        try std.testing.expectEqual(@as(u64, 0), count(report, .source_local));
        try std.testing.expect(report.analysis_complete);
    }
}

test "backup preflight uncompressed document admission uses exact output boundaries" {
    const a = std.testing.allocator;
    var decoder: portable.InspectionDocumentDecoder = .{};
    defer decoder.deinit(a);
    const entry: codec.DocumentEntry = .{ .key = "doc", .value_flags = 0, .value = "{}", .timestamp_ns = 0 };
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.StreamTooLong, decoder.documentJsonLimitedAlloc(failing.allocator(), entry, 1));
    const json = try decoder.documentJsonLimitedAlloc(a, entry, 2);
    defer a.free(json);
    try std.testing.expectEqualStrings("{}", json);
}

test "backup preflight compressed output boundary is independent of frame overhead" {
    const a = std.testing.allocator;
    const frame = &[_]u8{ 0x28, 0xb5, 0x2f, 0xfd, 0, 0, 0x11, 0, 0, '{', '}' };
    const payload = try codec.encodeDocumentBatch(a, &.{.{ .key = "doc", .value_flags = codec.doc_value_flag_compressed, .value = frame, .timestamp_ns = 0 }});
    defer a.free(payload);
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{ .max_document_bytes = 1 } };
    defer inspector.deinit();
    try std.testing.expectError(error.PreflightLimitExceeded, inspector.visit(.document_batch, payload));
    inspector.options.max_document_bytes = 2;
    try inspector.visit(.document_batch, payload);
    try std.testing.expectEqual(@as(u64, 1), report.documents);
}

test "backup preflight oversized metadata is admitted before allocation" {
    const a = std.testing.allocator;
    const value = try a.alloc(u8, 32 * 1024 * 1024);
    defer a.free(value);
    @memset(value, ' ');
    value[0] = '[';
    value[value.len - 1] = ']';
    const payload = try codec.encodeKeyValueBatch(a, &.{.{ .key = "\x00\x00__metadata__:enrichments", .value = value }});
    defer a.free(payload);
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(a);
    try codec.writeHeader(&archive, a, .{ .format_version = codec.legacy_format_version, .flags = 0, .created_at_ns = 0, .backup_id = @splat(0), .table_count = 1, .shard_count = 1 });
    try codec.writeBlock(&archive, a, .metadata_batch, payload);
    try portable.validatePortable(a, archive.items);
    const backing = try a.alloc(u8, 16 * 1024 * 1024);
    defer a.free(backing);
    var buffer = std.heap.FixedBufferAllocator.init(backing);
    const bounded = buffer.allocator();
    var report: Report = .{ .allocator = bounded };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = bounded, .report = &report, .options = .{ .max_metadata_bytes = 4096 } };
    defer inspector.deinit();
    try std.testing.expectError(error.PreflightLimitExceeded, inspector.visit(.metadata_batch, payload));
}

test "backup preflight retained model chunk text follows the consumer source policy" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(
        \\[{"name":"chunks","kind":"chunk","source_artifact_name":"copied","source_field":"text","chunker_json":"{\"provider\":\"antfly\",\"embedding\":{\"model\":\"media-model\",\"dimensions\":3}}"}]
    );
    try inspector.document("doc", "{\"text\":\"<<<dotprompt:media:url file:///private/example.pdf>>>\"}", "copied");
    try inspector.document("doc", "<<<dotprompt:media:url file:///private/example.pdf>>>", "copied");
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_local));
    try std.testing.expectEqual(@as(u64, 1), count(report, .model));
    try std.testing.expect(report.analysis_complete);
}

test "backup preflight retained artifact admission precedes cloning" {
    const a = std.testing.allocator;
    const value = try a.alloc(u8, 32 * 1024 * 1024);
    defer a.free(value);
    @memset(value, 'a');
    const doc = try a.dupe(u8, "doc");
    defer a.free(doc);
    const name = try a.dupe(u8, "copy");
    defer a.free(name);
    const key = try artifact_ids.artifactPublicIdAlloc(a, .{ .document_id = doc, .name = name, .kind = .asset });
    defer a.free(key);
    const payload = try codec.encodeKeyValueBatch(a, &.{.{ .key = key, .value = value }});
    defer a.free(payload);
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(a);
    try codec.writeHeader(&archive, a, .{ .format_version = codec.legacy_format_version, .flags = 0, .created_at_ns = 0, .backup_id = @splat(0), .table_count = 1, .shard_count = 1 });
    try codec.writeBlock(&archive, a, .artifact_batch, payload);
    try portable.validatePortable(a, archive.items);
    const backing = try a.alloc(u8, 16 * 1024 * 1024);
    defer a.free(backing);
    var buffer = std.heap.FixedBufferAllocator.init(backing);
    const bounded = buffer.allocator();
    var report: Report = .{ .allocator = bounded };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = bounded, .report = &report, .options = .{ .max_document_bytes = 4096 } };
    defer inspector.deinit();
    try std.testing.expectError(error.PreflightLimitExceeded, inspector.visit(.artifact_batch, payload));
}

test "backup preflight rejects deeply nested unknown replication metadata without recursion" {
    const a = std.testing.allocator;
    const depth = 20000;
    const prefix = "[{\"type\":\"postgres\",\"dsn\":\"host=db.example dbname=app\",\"postgres_table\":\"public.docs\",\"metadata\":";
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(a);
    try raw.appendSlice(a, prefix);
    try raw.appendNTimes(a, '[', depth);
    try raw.appendSlice(a, "null");
    try raw.appendNTimes(a, ']', depth);
    try raw.appendSlice(a, "}]");
    // The same typed parsing policy used by replication source inspection
    // accepts the source and skips the forward-compatible metadata field.
    const parsed = try std.json.parseFromSlice(Value, a, raw.items, .{});
    defer parsed.deinit();
    const typed = try std.json.parseFromValue([]const @import("antfly_metadata_openapi").ReplicationSource, a, parsed.value, .{ .ignore_unknown_fields = true });
    defer typed.deinit();
    try std.testing.expectEqualStrings("public.docs", typed.value[0].postgres_table);
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    // Main's strict replication contract rejects unknown executable fields.
    try std.testing.expectError(error.UnknownField, inspector.replicationSourcesJson(raw.items));
    try std.testing.expectEqual(@as(usize, 0), report.requirements.items.len);
}

test "backup preflight replication binds credentials and CDC configuration without visiting inert transforms" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "${secret:private-pg}", "${REVIEW_PG_DSN}", "postgres://user:private-password@localhost/db", "host=localhost dbname=private-db password=private-password" }) |dsn| {
        const source = try std.json.Stringify.valueAlloc(a, .{.{ .type = "postgres", .dsn = dsn, .postgres_table = "public.users", .on_update = .{.{ .op = "$set", .path = "url", .value = "file:///inert-transform" }} }}, .{});
        defer a.free(source);
        var report: Report = .{ .allocator = a, .scan_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.replicationSourcesJson(source);
        try std.testing.expectEqual(@as(usize, 1), report.requirements.items.len);
        const requirement = &report.requirements.items[0];
        try std.testing.expectEqual(Kind.replication_source, requirement.kind);
        try std.testing.expectEqual(Verdict.unknown, evaluate(requirement, .{ .version = 1, .configuration_id = "empty" }).verdict);
        const digest = requirement.sha256;
        var cap: Capability = .{ .requirement_sha256 = &digest, .supported = true };
        try std.testing.expectEqual(Verdict.blocked, evaluate(requirement, .{ .version = 1, .configuration_id = "read-only", .requirements = &.{cap} }).verdict);
        cap.future_ingestion = true;
        try std.testing.expectEqual(Verdict.compatible, evaluate(requirement, .{ .version = 1, .configuration_id = "cdc-verified", .requirements = &.{cap} }).verdict);
        const output = try report.jsonAlloc(null);
        defer a.free(output);
        for ([_][]const u8{ dsn, "private-password", "private-db", "inert-transform" }) |secret| try std.testing.expect(std.mem.indexOf(u8, output, secret) == null);
        // Table/slot changes need new evidence even when the DSN is unchanged.
        const other = try std.json.Stringify.valueAlloc(a, .{.{ .type = "postgres", .dsn = dsn, .postgres_table = "public.other", .slot_name = "different-slot" }}, .{});
        defer a.free(other);
        try inspector.replicationSourcesJson(other);
        try std.testing.expectEqual(@as(usize, 2), report.requirements.items.len);
        try std.testing.expectEqual(Verdict.unknown, evaluate(&report.requirements.items[1], .{ .version = 1, .configuration_id = "old-source-only", .requirements = &.{cap} }).verdict);
    }
}

test "backup preflight replication rejects unknown providers malformed contracts and limits" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "[{\"type\":\"future-provider\",\"dsn\":\"opaque\",\"postgres_table\":\"users\"}]",
        "[{\"type\":\"postgres\",\"dsn\":\"\",\"postgres_table\":\"users\"}]",
        "[{\"type\":\"postgres\",\"postgres_table\":\"users\"}]",
        "[{\"type\":\"postgres\",\"dsn\":\"opaque\",\"postgres_table\":\"users\",\"future_connection\":{\"dsn\":\"other\"}}]",
        "{}",
    }) |source| {
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        if (inspector.replicationSourcesJson(source)) |_| return error.TestUnexpectedResult else |_| {}
        try std.testing.expectEqual(@as(usize, 0), report.requirements.items.len);
    }
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{ .max_definitions = 0 } };
    defer inspector.deinit();
    try std.testing.expectError(error.PreflightLimitExceeded, inspector.replicationSourcesJson("[{\"type\":\"postgres\",\"dsn\":\"opaque\",\"postgres_table\":\"users\"}]"));
}

test "backup preflight IP literals retain public and local scope" {
    const a = std.testing.allocator;
    for ([_]struct { url: []const u8, local: bool }{
        .{ .url = "https://[2606:4700:4700::1111]/file.txt", .local = false },
        .{ .url = "https://[2606:4700:4700:0:0:0:0:1111]/file.txt", .local = false },
        .{ .url = "https://[::ffff:808:808]/file.txt", .local = false },
        .{ .url = "https://[::ffff:8.8.8.8]/file.txt", .local = false },
        .{ .url = "https://[0:0:0:0:0:ffff:8.8.8.8]/file.txt", .local = false },
        .{ .url = "https://[64:ff9b::8.8.8.8]/file.txt", .local = false },
        .{ .url = "https://8.8.8.8/file.txt", .local = false },
        .{ .url = "https://[::]/file.txt", .local = true },
        .{ .url = "https://[::1]/file.txt", .local = true },
        .{ .url = "https://[fc00::1]/file.txt", .local = true },
        .{ .url = "https://[fe80::1]/file.txt", .local = true },
        .{ .url = "https://[::ffff:127.0.0.1]/file.txt", .local = true },
        .{ .url = "https://[0:0:0:0:0:ffff:192.168.1.1]/file.txt", .local = true },
        .{ .url = "https://[0:0:0:0:0:ffff:c0a8:101]/file.txt", .local = true },
        .{ .url = "https://192.168.1.1/file.txt", .local = true },
        .{ .url = "https://localhost/file.txt", .local = true },
    }) |case| {
        var report: Report = .{ .allocator = a, .scan_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.locator(case.url, "", "");
        try std.testing.expectEqual(@as(u64, if (case.local) 1 else 0), count(report, .source_local));
        try std.testing.expectEqual(@as(u64, 1), count(report, .source_external));
        var capabilities: std.ArrayListUnmanaged(Capability) = .empty;
        defer capabilities.deinit(a);
        for (report.requirements.items) |*requirement| try capabilities.append(a, .{ .requirement_sha256 = &requirement.sha256, .supported = true });
        const destination = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "verified", .requirements = capabilities.items }, .{});
        defer a.free(destination);
        const json = try report.jsonAlloc(destination);
        defer a.free(json);
        var parsed = try std.json.parseFromSlice(Value, a, json, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(if (case.local) "blocked" else "compatible", parsed.value.object.get("compatibility").?.object.get("verdict").?.string);
    }
}

test "backup preflight embedded inference identity retains model requirements without a media locator" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(
        \\{"kind":"embedding","name":"dense","field":"body","expected_dims":384,"producer":{"version":2,"provider":"antfly","model":"BAAI/bge-small-en-v1.5","endpoint":"antfly:embedded","sparse":false}}
    );
    try std.testing.expectEqual(@as(u64, 1), count(report, .model));
    try std.testing.expectEqual(@as(u64, 1), count(report, .producer_model));
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_unresolved));
    // The same token in actual source content is still an unsupported URI.
    try inspector.locator("antfly:embedded", "doc", "media");
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_unresolved));
}

test "backup preflight numeric template origin matches runtime" {
    const a = std.testing.allocator;
    const source_template = "https://{{tenant}}.example.org/file.pdf";
    for ([_][]const u8{ "1e3", "1.0", "18446744073709551615" }) |number_token| {
        const document_json = try std.fmt.allocPrint(a, "{{\"tenant\":{s}}}", .{number_token});
        defer a.free(document_json);
        const actual = try template_runtime.renderDocument(a, source_template, document_json);
        defer a.free(actual);
        const expected_url = try std.fmt.allocPrint(a, "https://{s}.example.org/file.pdf", .{number_token});
        defer a.free(expected_url);
        try std.testing.expectEqualStrings(expected_url, actual);
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        const catalog = try std.json.Stringify.valueAlloc(a, .{ .kind = "asset", .source_template = source_template, .producer = .{ .type = "reader" } }, .{});
        defer a.free(catalog);
        try inspector.metadataJson(catalog);
        try std.testing.expectEqual(@as(usize, 0), report.requirements.items.len);
        try inspector.document("doc", document_json, "");
        const origin = try std.fmt.allocPrint(a, "https://{s}.example.org:443", .{number_token});
        defer a.free(origin);
        const expected = requirementHash(.source_external, origin);
        try std.testing.expectEqual(@as(usize, 1), report.requirements.items.len);
        try std.testing.expectEqualStrings(&expected, &report.requirements.items[0].sha256);
    }
}

test "backup preflight hostname substitutions defer until documents" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    const catalog = try std.json.Stringify.valueAlloc(a, .{ .kind = "asset", .source_template = "https://{{host}}/file.pdf", .producer = .{ .type = "reader" } }, .{});
    defer a.free(catalog);
    try inspector.metadataJson(catalog);
    try inspector.document("doc", "{\"host\":\"example.org\"}", "");
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_external));
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_unresolved));
    try std.testing.expect(report.analysis_complete);
}

test "backup preflight definition substitutions retain independent static media" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    const catalog = try std.json.Stringify.valueAlloc(a, .{ .kind = "asset", .source_template = "{{media url=\"https://static.example.org/a\"}} <<<dotprompt:media:url https://{{host}}/b>>>", .producer = .{ .type = "generator" } }, .{});
    defer a.free(catalog);
    try inspector.metadataJson(catalog);
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_external));
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_unresolved));
    try std.testing.expect(report.analysis_complete);
    try inspector.document("doc", "{\"host\":\"dynamic.example.org\"}", "");
    try std.testing.expectEqual(@as(u64, 3), count(report, .source_external));
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_unresolved));
    try std.testing.expect(report.analysis_complete);
}

test "backup preflight definition substitutions may complete media delimiters" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    const catalog = try std.json.Stringify.valueAlloc(a, .{ .kind = "asset", .source_template = "<<<dotprompt:media:url https://{{host}}{{{tail}}}", .producer = .{ .type = "generator" } }, .{});
    defer a.free(catalog);
    try inspector.metadataJson(catalog);
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_external));
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_unresolved));
    try inspector.document("doc", "{\"host\":\"example.org\",\"tail\":\"/a>>>\"}", "");
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_external));
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_unresolved));
    try std.testing.expect(report.analysis_complete);
}

test "backup preflight immutable source contracts cannot be overridden by capabilities" {
    const a = std.testing.allocator;
    const Case = struct {
        uri: []const u8 = "https://example.org/clip.mp4",
        version: []const u8,
        version_kind: []const u8 = "strong_etag",
        valid: bool = false,
    };
    const long_version: [4097]u8 = @splat('x');
    for ([_]Case{
        .{ .version = "" },
        .{ .version = "v1" },
        .{ .version = "W/\"v1\"" },
        .{ .version = "\"bad\netag\"" },
        .{ .version = &long_version },
        .{ .version = "v1", .version_kind = "s3_version_id" },
        .{ .uri = "s3://bucket/clip.mp4", .version = "null", .version_kind = "s3_version_id" },
        .{ .uri = "data:video/mp4;base64,AQID", .version = "\"v1\"" },
        .{ .uri = "gs://bucket/clip.mp4", .version = "v1", .version_kind = "s3_version_id" },
        .{ .uri = "https://example.org/clip.mp4#page=2", .version = "\"v1\"" },
        .{ .version = "\"v1\"", .valid = true },
        .{ .uri = "s3://bucket/clip.mp4", .version = "v1", .version_kind = "s3_version_id", .valid = true },
        .{ .uri = "s3://bucket/clip.mp4", .version = "\"etag\"", .valid = true },
    }) |case| {
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.metadataJson(
            \\[{"name":"chunks","kind":"chunk","source_field":"body","chunker_json":"{\"provider\":\"antfly\",\"embedding\":{\"model\":\"media-model\",\"dimensions\":3}}"}]
        );
        const document_json = try std.json.Stringify.valueAlloc(a, .{ .body = .{ .type = "source", .uri = case.uri, .mime_type = "video/mp4", .version = case.version, .version_kind = case.version_kind, .length = 1024 } }, .{});
        defer a.free(document_json);
        try inspector.document("doc", document_json, "");
        try std.testing.expectEqual(case.valid, report.analysis_complete);
        try std.testing.expectEqual(@as(u64, if (case.valid) 0 else 1), count(report, .source_unresolved));
        const caps = try a.alloc(Capability, report.requirements.items.len);
        defer a.free(caps);
        for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .query_embedding = true, .future_ingestion = true, .model_revision_verified = true, .preprocessing_verified = true, .vector_space_verified = true, .dimensions = r.dimensions };
        const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "all-evidence", .requirements = caps }, .{});
        defer a.free(evidence);
        const json = try report.jsonAlloc(evidence);
        defer a.free(json);
        var parsed = try std.json.parseFromSlice(Value, a, json, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(if (case.valid) "compatible" else "blocked", parsed.value.object.get("compatibility").?.object.get("verdict").?.string);
    }
}

test "backup preflight retains known template origins for empty tables" {
    const a = std.testing.allocator;
    const Case = struct { producer: []const u8, template: []const u8 };
    for ([_]Case{
        .{ .producer = "reader", .template = "http://localhost/{{id}}" },
        .{ .producer = "generator", .template = "<<<dotprompt:media:url http://localhost/{{id}}>>>" },
    }) |case| {
        const producer = try std.json.Stringify.valueAlloc(a, .{ .type = case.producer, .config = .{ .provider = "antfly", .model = "mock" } }, .{});
        defer a.free(producer);
        try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "media", .kind = .asset, .template = case.template, .producer_json = producer });
        const metadata = try std.json.Stringify.valueAlloc(a, .{ .name = "media", .kind = "asset", .source_template = case.template, .producer_json = producer }, .{});
        defer a.free(metadata);
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.metadataJson(metadata);
        const caps = try a.alloc(Capability, report.requirements.items.len);
        defer a.free(caps);
        for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .query_embedding = true, .future_ingestion = true, .model_revision_verified = true, .preprocessing_verified = true, .vector_space_verified = true, .dimensions = r.dimensions };
        const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "all-model-evidence", .requirements = caps }, .{});
        defer a.free(evidence);
        const json = try report.jsonAlloc(evidence);
        defer a.free(json);
        var parsed = try std.json.parseFromSlice(Value, a, json, .{});
        defer parsed.deinit();
        const verdict = parsed.value.object.get("compatibility").?.object.get("verdict").?.string;
        if (!eq(verdict, "blocked")) std.debug.print("producer={s} local_requirements={d} verdict={s}\n", .{ case.producer, count(report, .source_local), verdict });
        try std.testing.expectEqualStrings("blocked", verdict);
    }
}

test "backup preflight partial locators preserve only guaranteed URI components" {
    const Case = struct {
        text: []const u8,
        external: u64 = 0,
        local: u64 = 0,
        connection: u64 = 0,
        embedded: u64 = 0,
        expiring: u64 = 0,
        page: u64 = 0,
        archive: u64 = 0,
        unresolved: u64 = 0,
    };
    const a = std.testing.allocator;
    for ([_]Case{
        .{ .text = "https://example.org/\x00", .external = 1 },
        .{ .text = "http://localhost/\x00", .external = 1, .local = 1 },
        .{ .text = "https://example.org:\x00/file" },
        .{ .text = "https://\x00.example.org/file" },
        .{ .text = "https://example.org\x00/file" },
        .{ .text = "https:\x00" },
        .{ .text = "\x00://example.org/file" },
        .{ .text = "s3://bucket/\x00", .connection = 1 },
        .{ .text = "file:///private/\x00", .local = 1 },
        .{ .text = "/private/https:\x00", .local = 1 },
        .{ .text = "data:text/plain,\x00", .embedded = 1 },
        .{ .text = "ftp://example.org/\x00", .unresolved = 1 },
        .{ .text = "https://user:pass@example.org/\x00", .external = 1, .expiring = 1 },
        .{ .text = "https://user:pass@\x00/file", .expiring = 1 },
        .{ .text = "https://example.org/file?token=\x00", .external = 1, .expiring = 1 },
        .{ .text = "https://example.org/file?token\x00name", .external = 1 },
        .{ .text = "https://example.org/file?keep=1&token\x00name", .external = 1 },
        .{ .text = "https://example.org/file?page=\x00", .external = 1, .page = 1 },
        .{ .text = "https://example.org/file?page\x00size", .external = 1 },
        .{ .text = "https://example.org/file?entry=\x00", .external = 1, .archive = 1 },
        .{ .text = "https://example.org/file?expires=\x00", .external = 1, .expiring = 1 },
        .{ .text = "https://example.org/file.zip\x00", .external = 1 },
        .{ .text = "https://example.org/file.zip/\x00", .external = 1, .archive = 1 },
        .{ .text = "https://example.org/file.zip?filename=\x00", .external = 1, .archive = 1 },
        .{ .text = "https://example.org/file%2\x00", .external = 1 },
        .{ .text = "https://example.org/file%2ezip\x00", .external = 1 },
        .{ .text = "https://example.org/file.zip#\x00", .external = 1, .page = 1, .archive = 1 },
        .{ .text = "https://example.org/file?q=token%26key\x00", .external = 1 },
        .{ .text = "https://example.org/file?x=1#token=\x00", .external = 1, .expiring = 1, .page = 1 },
        .{ .text = "https://example.org/file#token\x00name", .external = 1, .page = 1 },
        .{ .text = "https://example.org/file?x=literal?token\x00name", .external = 1 },
        .{ .text = "https://example.org/file#x=literal#token\x00name", .external = 1, .page = 1 },
        .{ .text = "https://example.org/file.zip!/\x00", .external = 1, .archive = 1 },
    }) |case| {
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        try inspector.templateLocator(case.text, "", "body", true);
        inline for (.{ .{ Kind.source_external, "external" }, .{ Kind.source_local, "local" }, .{ Kind.connection, "connection" }, .{ Kind.embedded, "embedded" }, .{ Kind.source_expiring, "expiring" }, .{ Kind.page_retrieval, "page" }, .{ Kind.archive_retrieval, "archive" }, .{ Kind.source_unresolved, "unresolved" } }) |expected| {
            const actual = count(report, expected[0]);
            if (actual != @field(case, expected[1])) std.debug.print("partial locator {s}: unexpected {s}\n", .{ case.text, expected[1] });
            try std.testing.expectEqual(@field(case, expected[1]), actual);
        }
        try std.testing.expectEqual(case.unresolved == 0, report.analysis_complete);
    }
}

test "backup preflight known remote template origins need separate destination evidence" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(
        \\{"name":"reader","kind":"asset","source_template":"https://example.org/{{id}}","producer":{"type":"reader","config":{"provider":"antfly","model":"mock"}}}
    );
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_external));
    const expected_origin = requirementHash(.source_external, "https://example.org:443");
    const caps = try a.alloc(Capability, report.requirements.items.len);
    defer a.free(caps);
    var used: usize = 0;
    var origin_cap: Capability = undefined;
    for (report.requirements.items) |*r| {
        const cap: Capability = .{ .requirement_sha256 = &r.sha256, .supported = true, .future_ingestion = true, .model_revision_verified = true };
        if (r.kind == .source_external) {
            try std.testing.expectEqualStrings(&expected_origin, &r.sha256);
            origin_cap = cap;
        } else {
            caps[used] = cap;
            used += 1;
        }
    }
    const model_only = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "model-only", .requirements = caps[0..used] }, .{});
    defer a.free(model_only);
    const unknown = try report.jsonAlloc(model_only);
    defer a.free(unknown);
    try std.testing.expect(std.mem.indexOf(u8, unknown, "\"compatibility\":{\"verdict\":\"unknown\"") != null);
    caps[used] = origin_cap;
    const all = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "model-and-origin", .requirements = caps }, .{});
    defer a.free(all);
    const compatible = try report.jsonAlloc(all);
    defer a.free(compatible);
    try std.testing.expect(std.mem.indexOf(u8, compatible, "\"compatibility\":{\"verdict\":\"compatible\"") != null);
}

test "backup preflight known media origins survive an unknown closing delimiter" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(
        \\{"name":"summary","kind":"asset","source_template":"<<<dotprompt:media:url https://example.org/{{{tail}}}","producer":{"type":"generator","config":{"provider":"antfly","model":"mock"}}}
    );
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_external));
    try inspector.document("doc", "{\"tail\":\"file.pdf>>>\"}", "");
    try std.testing.expectEqual(@as(u64, 2), count(report, .source_external));
    try std.testing.expect(report.analysis_complete);
}

test "backup preflight reader selection follows media precedence and JSON locator fallback" {
    const a = std.testing.allocator;
    const Case = struct {
        template: []const u8 = "",
        document: []const u8 = "{}",
        local: u64 = 0,
        external: u64 = 0,
        embedded: u64 = 0,
    };
    for ([_]Case{
        .{ .template = "https://example.org/prompt <<<dotprompt:media:url file:///private/page.png>>>", .local = 2 },
        .{ .template = "file:///prompt <<<dotprompt:media:url https://example.org/page.png>>>", .external = 2 },
        .{ .template = "{{media url=uri}}", .document = "{\"uri\":\"https://example.org/page.png\"}", .external = 1 },
        .{ .template = "{{media url=\"https://example.org/page.png\"}}", .external = 2 },
        .{ .template = "[\"https://example.org/page.png\",\"file:///private/page.png\"]", .local = 2, .external = 2 },
        .{ .template = "\"https://example.org/page.png\"", .external = 2 },
        .{ .template = "[\"https://{{host}}/page.png\"]", .document = "{\"host\":\"example.org\"}", .external = 1 },
        .{ .template = "<<<dotprompt:media:url https://example.org/{{{tail}}}", .document = "{\"tail\":\"page.png>>>\"}", .external = 2 },
        .{ .template = "<<<dotprompt:media:url data:image/png;base64,AQID>>>", .embedded = 2 },
        .{ .document = "{\"body\":[\"https://example.org/page.png\",\"file:///private/page.png\"]}", .local = 1, .external = 1 },
        .{ .document = "{\"body\":\"[\\\"https://example.org/page.png\\\",\\\"file:///private/page.png\\\"]\"}", .local = 1, .external = 1 },
        .{ .document = "{\"body\":\"\\\"https://example.org/page.png\\\"\"}", .external = 1 },
        .{ .document = "{\"body\":[null,123,{\"uri\":\"file:///ignored\"},[\"file:///ignored\"],\"https://example.org/page.png\"]}", .external = 1 },
        .{ .document = "{\"body\":[]}" },
    }) |case| {
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        const metadata = try std.json.Stringify.valueAlloc(a, .{ .name = "reader", .kind = "asset", .source_field = "body", .source_template = case.template, .producer = .{ .type = "reader" } }, .{});
        defer a.free(metadata);
        try inspector.metadataJson(metadata);
        try inspector.document("doc", case.document, "");
        try std.testing.expectEqual(case.local, count(report, .source_local));
        try std.testing.expectEqual(case.external, count(report, .source_external));
        try std.testing.expectEqual(case.embedded, count(report, .embedded));
        try std.testing.expect(report.analysis_complete);
    }
}

test "backup preflight partial reader lists retain static local sources" {
    const a = std.testing.allocator;
    const producer = "{\"type\":\"reader\",\"config\":{\"provider\":\"antfly\",\"model\":\"mock\"}}";
    const source_template = "[\"file:///private/static.png\",\"https://example.org/{{id}}\"]";
    try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "reader", .kind = .asset, .template = source_template, .producer_json = producer });
    var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    const metadata = try std.json.Stringify.valueAlloc(a, .{ .name = "reader", .kind = "asset", .source_template = source_template, .producer_json = producer }, .{});
    defer a.free(metadata);
    try inspector.metadataJson(metadata);
    const caps = try a.alloc(Capability, report.requirements.items.len);
    defer a.free(caps);
    for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .query_embedding = true, .future_ingestion = true, .model_revision_verified = true, .preprocessing_verified = true, .vector_space_verified = true, .dimensions = r.dimensions };
    const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "all-model-capabilities", .requirements = caps }, .{});
    defer a.free(evidence);
    const json = try report.jsonAlloc(evidence);
    defer a.free(json);
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    try std.testing.expect(std.mem.indexOf(u8, json, "\"compatibility\":{\"verdict\":\"blocked\"") != null);
}

test "backup preflight unsupported GCS helper schemes cannot be certified" {
    const a = std.testing.allocator;
    var incorrectly_compatible = false;
    for ([_][]const u8{ "gs", "gcs" }) |scheme| {
        const uri = try std.fmt.allocPrint(a, "{s}://bucket/source.txt", .{scheme});
        defer a.free(uri);
        try std.testing.expectError(error.UnsupportedUrlScheme, @import("antfly_scraping").downloadContentAlloc(a, uri, null, null));
        const source_template = try std.fmt.allocPrint(a, "{{{{remoteText url=\"{s}\"}}}}", .{uri});
        defer a.free(source_template);
        const producer = "{\"type\":\"copy\",\"config\":{}}";
        try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "copy", .kind = .asset, .template = source_template, .producer_json = producer });
        try std.testing.expectError(error.TransientPromptFailure, @import("../../template_remote.zig").renderJsonToValidatedTextWithConfig(a, source_template, "{}", .{}));
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        const metadata = try std.json.Stringify.valueAlloc(a, .{ .name = "copy", .kind = "asset", .source_template = source_template, .producer_json = producer }, .{});
        defer a.free(metadata);
        try inspector.metadataJson(metadata);
        try inspector.document("doc", "{}", "");
        const caps = try a.alloc(Capability, report.requirements.items.len);
        defer a.free(caps);
        for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true };
        const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "bucket-access", .requirements = caps }, .{});
        defer a.free(evidence);
        const json = try report.jsonAlloc(evidence);
        defer a.free(json);
        const compatible = std.mem.indexOf(u8, json, "\"compatibility\":{\"verdict\":\"compatible\"") != null;
        try std.testing.expect(!report.analysis_complete);
        incorrectly_compatible = incorrectly_compatible or compatible;
    }
    try std.testing.expect(!incorrectly_compatible);
}

test "backup preflight partial reader JSON definitions keep known origins and fail closed on unknown structure" {
    const a = std.testing.allocator;
    const Case = struct { text: []const u8, local: u64 = 0, external: u64 = 0, expiring: u64 = 0, dynamic: u64 = 0 };
    for ([_]Case{
        .{ .text = "[\"file:///private/{{id}}\"]", .local = 1 },
        .{ .text = "[\"https://example.org/{{id}}\"]", .external = 1 },
        .{ .text = "[\"{{host}}\",\"file:///private/fixed\"]", .local = 1 },
        .{ .text = "\"https://example.org/{{id}}?token=fixed\"", .external = 1 },
        .{ .text = "\"https://example.org/file?token={{token}}\"", .external = 1, .expiring = 1 },
        .{ .text = "[\"https://{{host}}/{{id}}\"]" },
        .{ .text = "[{{urls}}]", .dynamic = 1 },
        .{ .text = "\"file:///\\{{escape}}\"", .local = 1 },
        .{ .text = "\"file:///\\u00{{escape}}\"", .dynamic = 1 },
    }) |case| {
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        const metadata = try std.json.Stringify.valueAlloc(a, .{ .name = "reader", .kind = "asset", .source_template = case.text, .producer = .{ .type = "reader" } }, .{});
        defer a.free(metadata);
        try inspector.metadataJson(metadata);
        if (case.local != count(report, .source_local) or case.external != count(report, .source_external) or case.dynamic != count(report, .dynamic_loader))
            std.debug.print("partial reader definition mismatch: {s}\n", .{case.text});
        try std.testing.expectEqual(case.local, count(report, .source_local));
        try std.testing.expectEqual(case.external, count(report, .source_external));
        try std.testing.expectEqual(case.expiring, count(report, .source_expiring));
        try std.testing.expectEqual(case.dynamic, count(report, .dynamic_loader));
        try std.testing.expectEqual(case.dynamic == 0, report.analysis_complete);
    }
}

test "backup preflight static template error directives match execution for every input mode" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "copy", "reader", "generator" }) |producer_type| {
        const source_template = "<<<error:message=source unavailable>>>";
        try std.testing.expectError(error.TransientPromptFailure, @import("../../template_remote.zig").renderJsonToValidatedTextWithConfig(a, source_template, "{}", .{}));
        const producer = try std.json.Stringify.valueAlloc(a, .{ .type = producer_type, .config = .{ .provider = "antfly", .model = "mock" } }, .{});
        defer a.free(producer);
        try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "asset", .kind = .asset, .template = source_template, .producer_json = producer });
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        const metadata = try std.json.Stringify.valueAlloc(a, .{ .name = "asset", .kind = "asset", .source_template = source_template, .producer_json = producer }, .{});
        defer a.free(metadata);
        try inspector.metadataJson(metadata);
        try inspector.document("doc", "{}", "");
        const caps = try a.alloc(Capability, report.requirements.items.len);
        defer a.free(caps);
        for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .future_ingestion = true, .model_revision_verified = true };
        const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "all-capabilities", .requirements = caps }, .{});
        defer a.free(evidence);
        const json = try report.jsonAlloc(evidence);
        defer a.free(json);
        try std.testing.expectEqual(@as(u64, 2), count(report, .source_unresolved));
        try std.testing.expect(!report.analysis_complete);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"compatibility\":{\"verdict\":\"blocked\"") != null);
    }
}

test "backup preflight incomplete media syntax remains literal text like execution" {
    const a = std.testing.allocator;
    const source_template = "Explain <<<dotprompt:media:url file:///private/unfinished";
    const parts = try template_runtime.textToParts(a, source_template);
    defer template_runtime.freeContentParts(a, parts);
    for (parts) |part| try std.testing.expect(part == .text);
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    const metadata = try std.json.Stringify.valueAlloc(a, .{ .name = "generator", .kind = "asset", .source_template = source_template, .producer = .{ .type = "generator" } }, .{});
    defer a.free(metadata);
    try inspector.metadataJson(metadata);
    try inspector.document("doc", "{}", "");
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_unresolved));
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_local));
    try std.testing.expect(report.analysis_complete);
}

test "backup preflight known empty helper calls neither fetch nor invent opaque output" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "Hello {{remoteText}}",
        "Hello {{remoteText url=\"\"}}",
        "Hello {{remoteText url=\"\" credentials=\"\"}}",
        "Hello {{remoteMedia}}",
        "Hello {{remoteMedia url=\"\"}}",
        "Hello {{remotePDF}}",
        "Hello {{remotePDF credentials=\"\"}}",
        "Hello {{media}}",
        "Hello {{media url=\"\"}}",
    }) |source_template| {
        const rendered = try @import("../../template_remote.zig").renderJsonToValidatedTextWithConfig(a, source_template, "{}", .{});
        defer a.free(rendered);
        try std.testing.expectEqualStrings("Hello ", rendered);
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        const metadata = try std.json.Stringify.valueAlloc(a, .{ .name = "generator", .kind = "asset", .source_template = source_template, .producer = .{ .type = "generator", .config = .{ .provider = "antfly", .model = "mock" } } }, .{});
        defer a.free(metadata);
        try inspector.metadataJson(metadata);
        try inspector.document("doc", "{}", "");
        for (report.requirements.items) |r| try std.testing.expect(r.kind == .producer_model);
        try std.testing.expect(report.analysis_complete);
        const caps = try a.alloc(Capability, report.requirements.items.len);
        defer a.free(caps);
        for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true, .future_ingestion = true, .model_revision_verified = true };
        const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "all-models", .requirements = caps }, .{});
        defer a.free(evidence);
        const json = try report.jsonAlloc(evidence);
        defer a.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"compatibility\":{\"verdict\":\"compatible\"") != null);
    }
    // Empty helpers embedded in a locator must preserve its known text.
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(
        \\{"kind":"asset","source_template":"https://example.org/{{remoteText url=\"\"}}","producer":{"type":"document_extraction"}}
    );
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_external));
    try std.testing.expect(report.analysis_complete);
}

test "backup preflight malformed S3 sources cannot be certified" {
    const a = std.testing.allocator;
    var incorrectly_compatible = false;
    for ([_][]const u8{ "s3://bucket", "s3:relative", "s3:///key", "s3://bucket///" }) |uri| {
        // This is the same location parser called before the runtime creates
        // the S3 client. No credentials, allocator-backed I/O or network needed.
        try std.testing.expectError(error.InvalidS3Url, @import("antfly_scraping").s3BucketAlloc(a, try std.Uri.parse(uri), "objects.example.org", true));
        const source_template = try std.fmt.allocPrint(a, "{{{{remoteText url=\"{s}\"}}}}", .{uri});
        defer a.free(source_template);
        const producer = "{\"type\":\"copy\",\"config\":{}}";
        try @import("../db/enrichment/config_validation.zig").validatePublicConfig(a, .{ .name = "copy", .kind = .asset, .template = source_template, .producer_json = producer });
        var report: Report = .{ .allocator = a, .scan_complete = true, .counts_complete = true };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        const metadata = try std.json.Stringify.valueAlloc(a, .{ .name = "copy", .kind = "asset", .source_template = source_template, .producer_json = producer }, .{});
        defer a.free(metadata);
        try inspector.metadataJson(metadata);
        try inspector.document("doc", "{}", "");
        const caps = try a.alloc(Capability, report.requirements.items.len);
        defer a.free(caps);
        for (report.requirements.items, caps) |*r, *cap| cap.* = .{ .requirement_sha256 = &r.sha256, .supported = true };
        const evidence = try std.json.Stringify.valueAlloc(a, Destination{ .version = 1, .configuration_id = "bucket-access", .requirements = caps }, .{});
        defer a.free(evidence);
        const json = try report.jsonAlloc(evidence);
        defer a.free(json);
        const compatible = std.mem.indexOf(u8, json, "\"compatibility\":{\"verdict\":\"compatible\"") != null;
        try std.testing.expectEqual(@as(u64, 2), count(report, .source_unresolved));
        try std.testing.expectEqual(@as(u64, 0), count(report, .connection));
        try std.testing.expect(!report.analysis_complete);
        incorrectly_compatible = incorrectly_compatible or compatible;
    }
    try std.testing.expect(!incorrectly_compatible);
}

test "backup preflight S3 template prefixes defer only an open object key" {
    const a = std.testing.allocator;
    const Case = struct { text: []const u8, connection: u64, unresolved: u64 };
    for ([_]Case{
        .{ .text = "s3://bucket/{{id}}", .connection = 1, .unresolved = 0 },
        .{ .text = "s3://bucket/key", .connection = 1, .unresolved = 0 },
        .{ .text = "s3://bucket/", .connection = 0, .unresolved = 1 },
        .{ .text = "s3://bucket/?page={{id}}", .connection = 0, .unresolved = 1 },
        .{ .text = "s3:///{{id}}", .connection = 0, .unresolved = 1 },
        .{ .text = "s3://{{host}}/key", .connection = 0, .unresolved = 0 },
    }) |case| {
        var report: Report = .{ .allocator = a };
        defer report.deinit();
        var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
        defer inspector.deinit();
        const metadata = try std.json.Stringify.valueAlloc(a, .{ .name = "reader", .kind = "asset", .source_template = case.text, .producer = .{ .type = "reader" } }, .{});
        defer a.free(metadata);
        try inspector.metadataJson(metadata);
        try std.testing.expectEqual(case.connection, count(report, .connection));
        try std.testing.expectEqual(case.unresolved, count(report, .source_unresolved));
        try std.testing.expectEqual(@as(u64, 0), count(report, .source_local));
        try std.testing.expectEqual(case.unresolved == 0, report.analysis_complete);
    }
}
