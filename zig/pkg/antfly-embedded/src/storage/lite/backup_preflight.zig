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
const artifact_ids = @import("../db/artifact_ids.zig");
const canonical = @import("../db/document_content_hash.zig");
const staging = @import("restore_staging.zig");

pub const report_version = 1;
pub const inspector_version = "antfly-backup-preflight/1";
pub const Digest = [64]u8;
pub const Verdict = enum { compatible, blocked, unknown };
pub const Kind = enum { embedded, source_local, source_external, source_expiring, archive_retrieval, page_retrieval, connection, model, producer_model, dynamic_loader, source_unresolved };
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
        const supported = cap.supported and (r.kind != .producer_model or (cap.future_ingestion and cap.model_revision_verified)) and (r.kind != .model or (cap.query_embedding and cap.future_ingestion and cap.model_revision_verified and cap.preprocessing_verified and cap.vector_space_verified and r.dimensions > 0 and cap.dimensions == r.dimensions));
        e.verdict = if (supported) .compatible else .blocked;
        e.reason_code = if (supported) "capability_verified" else switch (r.kind) {
            .model, .producer_model => "model_incompatible",
            .connection => "connection_missing",
            else => "source_unresolved",
        };
        e.remediation = if (supported) "Revalidate this capability at restore admission." else if (r.kind == .model) "Verify the same model revision, preprocessing, dimensions and vector space for both query embedding and future ingestion." else "Configure durable access and the required retrieval semantics at the destination.";
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

const Selector = struct { field: []const u8, template: []const u8, artifact: []const u8, locator: bool, dynamic: bool };
pub const Inspector = struct {
    allocator: Allocator,
    report: *Report,
    options: Options,
    definitions: std.ArrayListUnmanaged(Selector) = .empty,
    definition_bytes: usize = 0,
    model_names: std.ArrayListUnmanaged(Digest) = .empty,
    decoder: portable.InspectionDocumentDecoder = .{},

    pub fn deinit(self: *@This()) void {
        for (self.definitions.items) |s| {
            self.allocator.free(s.field);
            self.allocator.free(s.template);
            self.allocator.free(s.artifact);
        }
        self.definitions.deinit(self.allocator);
        self.decoder.deinit(self.allocator);
        self.model_names.deinit(self.allocator);
    }

    pub fn visit(self: *@This(), kind: codec.BlockType, payload: []const u8) !void {
        try self.options.cancellation.check();
        switch (kind) {
            .shard_header => self.report.shards += 1,
            .metadata_batch => {
                try self.decoder.metadata(self.allocator, payload);
                const entries = try codec.decodeKeyValueBatch(self.allocator, payload);
                defer {
                    for (entries) |e| {
                        self.allocator.free(e.key);
                        self.allocator.free(e.value);
                    }
                    self.allocator.free(entries);
                }
                for (entries) |e| {
                    self.report.metadata_records += 1;
                    if (std.mem.endsWith(u8, e.key, ":indexes")) {
                        const json = try staging.tableIndexesJsonFromPortableMetadata(self.allocator, e.value, null);
                        defer self.allocator.free(json);
                        try self.metadataJson(json);
                    } else if (std.mem.endsWith(u8, e.key, ":enrichments") or std.mem.endsWith(u8, e.key, ":resolvers") or std.mem.endsWith(u8, e.key, ":replication_sources")) try self.metadataJson(e.value);
                }
            },
            .document_batch => {
                const entries = try codec.decodeDocumentBatchBorrowed(self.allocator, payload);
                defer self.allocator.free(entries);
                for (entries) |e| {
                    try self.options.cancellation.check();
                    if (e.value.len > self.options.max_document_bytes) return error.PreflightLimitExceeded;
                    const json = try self.decoder.documentJsonAlloc(self.allocator, e);
                    defer self.allocator.free(json);
                    try self.document(e.key, json, "");
                    self.report.documents += 1;
                }
            },
            .chunk_batch, .artifact_batch, .resolution_batch => {
                const entries = try codec.decodeKeyValueBatch(self.allocator, payload);
                defer {
                    for (entries) |e| {
                        self.allocator.free(e.key);
                        self.allocator.free(e.value);
                    }
                    self.allocator.free(entries);
                }
                for (entries) |e| {
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
        try self.config(parsed.value, 0, "");
    }

    fn config(self: *@This(), value: Value, depth: usize, catalog_name: []const u8) anyerror!void {
        if (depth > 32) return error.PreflightLimitExceeded;
        if (value == .array) {
            for (value.array.items) |v| try self.config(v, depth + 1, catalog_name);
            return;
        }
        if (value != .object) return;
        // Only executable catalog positions enter this walker. Document values
        // are examined later solely through these source selectors.
        const kind = str(get(value, "kind"));
        const typ = str(get(value, "type"));
        const producer_json = str(get(value, "producer_json"));
        var producer_parsed: ?std.json.Parsed(Value) = null;
        defer if (producer_parsed) |*p| p.deinit();
        if (producer_json.len > 0) producer_parsed = try std.json.parseFromSlice(Value, self.allocator, producer_json, .{});
        const producer = if (producer_parsed) |p| p.value else get(value, "producer");
        const producer_type = str(get(producer, "type"));
        const field = blk: {
            const f = str(get(value, "source_field"));
            break :blk if (f.len > 0) f else str(get(value, "field"));
        };
        const template = blk: {
            const t = str(get(value, "source_template"));
            break :blk if (t.len > 0) t else str(get(value, "template"));
        };
        const artifact = str(get(value, "source_artifact_name"));
        const is_embedding = eq(kind, "embedding") or eq(kind, "dense_vector") or eq(kind, "sparse_vector") or eq(typ, "embeddings") or get(value, "embedder") != .null or get(value, "semantic_producer") != .null;
        const is_definition = is_embedding or producer != .null or eq(kind, "asset") or eq(kind, "chunk") or get(value, "transcriber") != .null;
        const known = producer_type.len == 0 or eq(producer_type, "copy") or eq(producer_type, "reader") or eq(producer_type, "transcriber") or eq(producer_type, "document_extraction") or eq(producer_type, "generator") or eq(producer_type, "extractor");
        if (!known) try self.report.add(self.options, .dynamic_loader, "producer", "", field, 0);
        if (is_definition and (field.len > 0 or template.len > 0 or artifact.len > 0)) {
            const locator_mode = eq(producer_type, "reader") or eq(producer_type, "transcriber") or eq(producer_type, "document_extraction") or eq(str(get(value, "embedding_input")), "pdf_page_images") or eq(str(get(value, "input")), "pdf_page_images") or eq(str(get(value, "embedding_input")), "image") or eq(str(get(value, "embedding_input")), "audio") or eq(str(get(value, "embedding_input")), "video") or get(value, "transcriber") != .null;
            try self.selector(field, template, artifact, locator_mode, !known or (field.len > 0 and !simplePath(field)));
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
            const identity = try canonical.canonicalJsonValueAlloc(self.allocator, value);
            defer self.allocator.free(identity);
            try self.report.add(self.options, .model, identity, "", str(get(value, "name")), dims);
        }
        if (get(value, "model") == .string and str(get(value, "model")).len > 0) {
            const identity = try canonical.canonicalJsonValueAlloc(self.allocator, value);
            defer self.allocator.free(identity);
            try self.report.add(self.options, .producer_model, identity, "", "model", 0);
        }
        var it = value.object.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            const v = entry.value_ptr.*;
            if (eq(key, "api_url") or eq(key, "base_url") or eq(key, "endpoint") or eq(key, "url") or eq(key, "uri") or eq(key, "source_url") or eq(key, "model_path") or eq(key, "credentials_path")) {
                // This is a typed inference identity, not a media URI. Its
                // exact model contract still requires destination evidence.
                if (eq(key, "endpoint") and eq(str(v), "antfly:embedded") and
                    eq(str(get(value, "provider")), "antfly") and number(get(value, "version")) == 2 and
                    str(get(value, "model")).len > 0) continue;
                if (v == .string and v.string.len > 0) try self.locator(v.string, "", key);
            } else if (eq(key, "connection") or eq(key, "credential") or eq(key, "credential_name") or eq(key, "credentials") or eq(key, "connection_id") or eq(key, "connection_name")) {
                if (v == .string and v.string.len > 0) try self.report.add(self.options, .connection, v.string, "", key, 0);
            } else if (eq(key, "api_key") or eq(key, "api_token") or eq(key, "password") or eq(key, "authorization") or eq(key, "headers")) {
                if (v != .null and (v != .string or v.string.len > 0)) {
                    const binding = try canonical.canonicalJsonValueAlloc(self.allocator, v);
                    defer self.allocator.free(binding);
                    try self.report.add(self.options, .connection, binding, "", key, 0);
                }
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

    fn selector(self: *@This(), field: []const u8, template: []const u8, artifact: []const u8, locator_mode: bool, dynamic: bool) !void {
        if (self.definitions.items.len >= self.options.max_definitions or field.len + template.len + artifact.len > self.options.max_metadata_bytes -| self.definition_bytes) return error.PreflightLimitExceeded;
        for (self.definitions.items) |prior| if (eq(prior.field, field) and eq(prior.template, template) and eq(prior.artifact, artifact) and prior.locator == locator_mode and prior.dynamic == dynamic) return;
        const a = self.allocator;
        const f = try a.dupe(u8, field);
        errdefer a.free(f);
        const t = try a.dupe(u8, template);
        errdefer a.free(t);
        const ar = try a.dupe(u8, artifact);
        errdefer a.free(ar);
        try self.definitions.append(a, .{ .field = f, .template = t, .artifact = ar, .locator = locator_mode, .dynamic = dynamic });
        self.definition_bytes += field.len + template.len + artifact.len;
        // Empty tables still carry a future-ingestion dependency.
        if (dynamic) try self.report.add(self.options, .dynamic_loader, "loader", "", field, 0);
        if (template.len > 0) try self.templateSources(template, .null, "", field, locator_mode, true);
    }

    pub fn document(self: *@This(), id: []const u8, bytes: []const u8, artifact: []const u8) !void {
        if (bytes.len > self.options.max_document_bytes) return error.PreflightLimitExceeded;
        if (self.definitions.items.len == 0) return;
        var parsed = std.json.parseFromSlice(Value, self.allocator, bytes, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => {
                if (artifact.len > 0) {
                    for (self.definitions.items) |s| if (eq(s.artifact, artifact)) try self.consumeValue(.{ .string = bytes }, id, s.field, s.locator, 0);
                    return;
                }
                return err;
            },
        };
        defer parsed.deinit();
        for (self.definitions.items) |s| {
            if (!eq(s.artifact, artifact)) continue;
            if (s.dynamic) {
                try self.report.add(self.options, .dynamic_loader, "loader", id, s.field, 0);
                continue;
            }
            if (s.template.len > 0) {
                try self.templateSources(s.template, parsed.value, id, s.field, s.locator, false);
            } else if (artifact.len > 0 and eq(s.field, "text")) {
                // Reuse runtime chunk payload selection, including historical
                // _source_field provenance, instead of guessing stored layouts.
                if (try @import("../../chunking/chunk.zig").artifactTextAlloc(self.allocator, bytes, s.field)) |text| {
                    defer self.allocator.free(text);
                    try self.consumeValue(.{ .string = text }, id, s.field, s.locator, 0);
                }
            } else try self.consumeValue(if (s.field.len == 0) parsed.value else try self.selectedField(parsed.value, s.field, id), id, s.field, s.locator, 0);
        }
    }

    fn selectedField(self: *@This(), root: Value, path: []const u8, doc: []const u8) !Value {
        return fieldValue(root, path) catch {
            try self.report.add(self.options, .dynamic_loader, "array_field_path", doc, path, 0);
            return .null;
        };
    }

    fn fieldValue(root: Value, path: []const u8) !Value {
        if (path.len == 0 or eq(path, "this")) return root;
        var parts = std.mem.splitScalar(u8, path, '.');
        var value = root;
        while (parts.next()) |p| {
            if (value == .object) value = value.object.get(p) orelse return .null else if (value == .array) {
                const index = std.fmt.parseUnsigned(usize, p, 10) catch return error.UnsupportedFieldPath;
                if (index >= value.array.items.len) return .null;
                value = value.array.items[index];
            } else return .null;
        }
        return value;
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
        const prefix = "<<<dotprompt:media:url ";
        var pos: usize = 0;
        while (std.mem.indexOfPos(u8, text, pos, prefix)) |start| {
            const begin = start + prefix.len;
            const end = std.mem.indexOfPos(u8, text, begin, ">>>") orelse {
                try self.report.add(self.options, .source_unresolved, "media_directive", doc, field, 0);
                return;
            };
            try self.locator(text[begin..end], doc, field);
            pos = end + 3;
        }
    }

    fn templateSources(self: *@This(), text: []const u8, root: Value, doc: []const u8, field: []const u8, locator_mode: bool, definition: bool) !void {
        var pos: usize = 0;
        var expressions: usize = 0;
        while (std.mem.indexOfPos(u8, text, pos, "{{")) |start| {
            const end = std.mem.indexOfPos(u8, text, start + 2, "}}") orelse {
                try self.report.add(self.options, .dynamic_loader, "template", doc, field, 0);
                return;
            };
            const expr = std.mem.trim(u8, text[start + 2 .. end], " \t\r\n");
            expressions += 1;
            if (simplePath(expr)) {
                if (!definition) try self.consumeValue(try self.selectedField(root, expr, doc), doc, expr, locator_mode, 0);
            } else blk: {
                var tokens = std.mem.tokenizeAny(u8, expr, " \t\r\n");
                const helper = tokens.next() orelse break :blk;
                if (!(eq(helper, "media") or eq(helper, "remoteMedia") or eq(helper, "remoteText") or eq(helper, "remotePDF"))) {
                    try self.report.add(self.options, .dynamic_loader, "template", doc, field, 0);
                    break :blk;
                }
                const arg = tokens.next() orelse {
                    try self.report.add(self.options, .dynamic_loader, "template", doc, field, 0);
                    break :blk;
                };
                const credential = tokens.next();
                if (credential) |binding| {
                    if (!std.mem.startsWith(u8, binding, "credential=") or tokens.next() != null) {
                        try self.report.add(self.options, .dynamic_loader, "template_credential", doc, field, 0);
                        break :blk;
                    }
                    const ref = binding["credential=".len..];
                    if (ref.len < 2 or (ref[0] != '"' and ref[0] != '\'') or ref[ref.len - 1] != ref[0]) {
                        try self.report.add(self.options, .dynamic_loader, "template_credential", doc, field, 0);
                        break :blk;
                    }
                    try self.report.add(self.options, .connection, ref[1 .. ref.len - 1], doc, field, 0);
                }
                if (!std.mem.startsWith(u8, arg, "url=")) {
                    try self.report.add(self.options, .dynamic_loader, "template", doc, field, 0);
                    break :blk;
                }
                const source = arg[4..];
                if (source.len >= 2 and (source[0] == '"' or source[0] == '\'') and source[source.len - 1] == source[0]) {
                    try self.locator(source[1 .. source.len - 1], doc, field);
                } else if (simplePath(source)) {
                    if (!definition) try self.consumeValue(try self.selectedField(root, source, doc), doc, source, true, 0);
                } else try self.report.add(self.options, .dynamic_loader, "template", doc, field, 0);
            }
            pos = end + 2;
        }
        if (locator_mode and expressions > 0 and !(expressions == 1 and std.mem.startsWith(u8, text, "{{") and std.mem.endsWith(u8, text, "}}"))) try self.report.add(self.options, .dynamic_loader, "composed_locator", doc, field, 0);
        if (definition) {
            if (locator_mode and expressions == 0) try self.locator(text, doc, field) else try self.directives(text, doc, field);
        }
    }

    pub fn locator(self: *@This(), raw: []const u8, doc: []const u8, field: []const u8) !void {
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
        if (isLocalHost(host)) try self.report.add(self.options, .source_local, "local_gateway", doc, field, 0);
        if (uri.user != null or uri.password != null) try self.report.add(self.options, .source_expiring, "embedded_credentials", doc, field, 0);
        if (uri.query) |q| {
            const query = try std.ascii.allocLowerString(a, try q.toRawMaybeAlloc(a));
            var parts = std.mem.splitScalar(u8, query, '&');
            while (parts.next()) |part| {
                const key = part[0 .. std.mem.indexOfScalar(u8, part, '=') orelse part.len];
                if (eq(key, "sig") or eq(key, "signature") or eq(key, "token") or eq(key, "expires") or eq(key, "se") or eq(key, "x-amz-signature") or eq(key, "x-goog-signature") or eq(key, "x-amz-credential") or eq(key, "x-goog-credential") or eq(key, "access_token") or eq(key, "auth") or eq(key, "key") or eq(key, "expiry") or eq(key, "expires_at") or eq(key, "x-amz-expires") or eq(key, "x-goog-expires")) try self.report.add(self.options, .source_expiring, "temporary_access", doc, field, 0);
                if (eq(key, "page") or eq(key, "pages") or eq(key, "range") or eq(key, "page_number") or eq(key, "page_index") or eq(key, "page_range")) try self.report.add(self.options, .page_retrieval, "page_selection", doc, field, 0);
                if (eq(key, "member") or eq(key, "entry") or eq(key, "archive")) try self.report.add(self.options, .archive_retrieval, "archive_member", doc, field, 0);
            }
        }
        if (uri.fragment != null) try self.report.add(self.options, .page_retrieval, "fragment_selection", doc, field, 0);
        const path = try uri.path.toRawMaybeAlloc(a);
        if (eq(uri.scheme, "zip") or std.mem.endsWith(u8, path, ".zip") or std.mem.endsWith(u8, path, ".tar") or std.mem.endsWith(u8, path, ".tgz") or std.mem.indexOf(u8, path, ".zip/") != null or std.mem.indexOf(u8, path, ".zip!") != null or std.mem.indexOf(u8, path, "!/") != null) try self.report.add(self.options, .archive_retrieval, "archive_member", doc, field, 0);
        if (eq(uri.scheme, "http") or eq(uri.scheme, "https")) {
            if (host.len == 0) {
                try self.report.add(self.options, .source_unresolved, "missing_host", doc, field, 0);
                return;
            }
            // Origin scope is deliberately hashed; no path, query, userinfo or
            // credential value is ever included in an example or log.
            const origin = try std.fmt.allocPrint(a, "{s}://{s}:{d}", .{ uri.scheme, host, uri.port orelse if (eq(uri.scheme, "https")) @as(u16, 443) else @as(u16, 80) });
            try self.report.add(self.options, .source_external, origin, doc, field, 0);
        } else if (eq(uri.scheme, "s3") or eq(uri.scheme, "gs") or eq(uri.scheme, "gcs")) {
            const origin = try std.fmt.allocPrint(a, "{s}://{s}", .{ uri.scheme, host });
            try self.report.add(self.options, .connection, origin, doc, field, 0);
        } else try self.report.add(self.options, .source_unresolved, "unsupported_scheme", doc, field, 0);
    }
};

fn isLocalHost(host: []const u8) bool {
    if (host.len == 0) return false;
    if (host[0] == '[' and host[host.len - 1] == ']') {
        const address = std.Io.net.Ip6Address.parse(host[1 .. host.len - 1], 0) catch return true;
        const b = address.bytes;
        if ((std.mem.allEqual(u8, b[0..15], 0) and b[15] <= 1) or b[0] & 0xfe == 0xfc or (b[0] == 0xfe and b[1] & 0xc0 == 0x80) or (std.mem.allEqual(u8, b[0..10], 0) and b[10] == 255 and b[11] == 255)) return true;
    }
    if (eq(host, "localhost") or std.mem.endsWith(u8, host, ".localhost") or std.mem.endsWith(u8, host, ".local") or eq(host, "[::1]") or eq(host, "[::]") or std.mem.indexOf(u8, host, "::ffff:") != null) return true;
    if (std.mem.startsWith(u8, host, "[fc") or std.mem.startsWith(u8, host, "[fd") or std.mem.startsWith(u8, host, "[fe80:")) return true;
    var octets = std.mem.splitScalar(u8, host, '.');
    const first = std.fmt.parseUnsigned(u16, octets.next() orelse return false, 10) catch return std.mem.indexOfScalar(u8, host, '.') == null;
    const second = std.fmt.parseUnsigned(u16, octets.next() orelse return true, 10) catch return false;
    return first == 127 or first == 0 or first == 10 or (first == 169 and second == 254) or (first == 192 and second == 168) or (first == 172 and second >= 16 and second <= 31);
}

fn count(report: Report, kind: Kind) u64 {
    var n: u64 = 0;
    for (report.requirements.items) |r| if (r.kind == kind) {
        n += r.count;
    };
    return n;
}

test "backup preflight embedded inference identity retains model requirements without a media locator" {
    const a = std.testing.allocator;
    var report: Report = .{ .allocator = a };
    defer report.deinit();
    var inspector: Inspector = .{ .allocator = a, .report = &report, .options = .{} };
    defer inspector.deinit();
    try inspector.metadataJson(
        \\{"kind":"embedding","name":"dense","field":"body","expected_dims":384,"producer":{"version":2,"provider":"antfly","model":"BAAI/bge-small-en-v1.5","endpoint":"antfly:embedded"}}
    );
    try std.testing.expectEqual(@as(u64, 1), count(report, .model));
    try std.testing.expectEqual(@as(u64, 1), count(report, .producer_model));
    try std.testing.expectEqual(@as(u64, 0), count(report, .source_unresolved));
    // The same token in actual source content is still an unsupported URI.
    try inspector.locator("antfly:embedded", "doc", "media");
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_unresolved));
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
    try inspector.metadataJson("[{\"kind\":\"embedding\",\"source_template\":\"{{remoteMedia url=image credential=\\\"private-media\\\"}}\"}]");
    try std.testing.expectEqual(@as(u64, 1), count(report, .connection));
    try inspector.metadataJson("[{\"kind\":\"asset\",\"field\":\"media[*].url\",\"producer\":{\"type\":\"reader\"}}]");
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
    try inspector.metadataJson("{\"known\":{\"type\":\"embeddings\",\"dimension\":3,\"embedder\":{\"model\":\"exact-model\"}}}");
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
    try inspector.metadataJson("[{\"kind\":\"embedding\",\"field\":\"text\",\"source_artifact_name\":\"chunks\",\"expected_dims\":3,\"producer\":{\"model\":\"exact-model\"}}]");
    try inspector.document("d", "{\"body\":\"<<<dotprompt:media:url file:///private/active>>>\",\"_source_field\":\"body\",\"_provenance\":{\"url\":\"file:///private/historical\"}}", "chunks");
    try std.testing.expectEqual(@as(u64, 1), count(report, .source_local));
    try inspector.document("d", "{\"body\":\"plain text\",\"_source_field\":\"body\",\"_provenance\":{\"url\":\"file:///private/historical\"}}", "chunks");
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
