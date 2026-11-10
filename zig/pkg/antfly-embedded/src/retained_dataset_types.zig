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

//! Dataset meaning and retention are independent of placement. Values here are
//! persisted in the existing conditional-root/ordered-page metadata store.
const std = @import("std");
pub const Id = [32]u8;
pub const max_public_counter: u64 = 9_007_199_254_740_991;
pub const Kind = enum { records, summary };
pub const Access = enum { local, queryable, restore_required };
pub const Law = enum { count, sum, avg, min, max };
pub const Measure = struct { name: []const u8, law: Law, field: ?[]const u8 = null };
/// Initial history is explicit. Automatic producers start from source rows
/// still physically retained at activation; they cannot reconstruct old TTL
/// deletions. Certified history remains a separate, proof-bearing contract.
pub const InitialHistory = enum { certified, retained_source_rows };
pub const Summary = struct {
    source: Id,
    timestamp: []const u8,
    source_filter_json: []const u8 = "{\"match_all\":{}}",
    bucket_semantic_version: u32 = 1,
    timezone: []const u8 = "UTC",
    dimensions: []const []const u8 = &.{},
    interval_ns: u64,
    origin_ns: []const u8 = "0",
    mutable_for_ns: u64,
    measures: []const Measure,
    projection: ?@import("retained_dataset_projection.zig").Projection = null,
    initial_history: InitialHistory = .certified,

    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        if (self.projection == null and self.initial_history == .certified) return writeFieldsOmitting(self, .{ "interval_ns", "mutable_for_ns" }, .{ "projection", "initial_history" }, writer);
        if (self.projection == null) return writeFieldsOmitting(self, .{ "interval_ns", "mutable_for_ns" }, .{"projection"}, writer);
        if (self.initial_history == .certified) return writeFieldsOmitting(self, .{ "interval_ns", "mutable_for_ns" }, .{"initial_history"}, writer);
        try writeFields(self, .{ "interval_ns", "mutable_for_ns" }, writer);
    }
    pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!@This() {
        return fromWire(@This(), try std.json.innerParse(SummaryWire, a, source, options));
    }
    pub fn jsonParseFromValue(a: std.mem.Allocator, value: std.json.Value, options: std.json.ParseOptions) !@This() {
        return fromWire(@This(), try std.json.innerParseFromValue(SummaryWire, a, value, options));
    }
};
pub const Definition = struct {
    format: u8 = 1,
    id: Id,
    generation: u64,
    kind: Kind,
    schema: Id,
    /// Self-contained source-independent schema, checked against its digest.
    schema_json: []const u8,
    authorization: Id,
    /// Logical lifetime from the end of a finalized segment's event range.
    retention_ns: u64,
    /// Local residence can be shorter than logical lifetime.
    local_residence_ns: u64,
    summary: ?Summary = null,

    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writeFields(self, .{ "retention_ns", "local_residence_ns" }, writer);
    }
    pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!@This() {
        return fromWire(@This(), try std.json.innerParse(DefinitionWire, a, source, options));
    }
    pub fn jsonParseFromValue(a: std.mem.Allocator, value: std.json.Value, options: std.json.ParseOptions) !@This() {
        return fromWire(@This(), try std.json.innerParseFromValue(DefinitionWire, a, value, options));
    }

    pub fn fingerprint(self: Definition, a: std.mem.Allocator) !Id {
        try self.validate();
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        defer a.free(bytes);
        var digest: Id = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        return digest;
    }

    pub fn validate(self: Definition) !void {
        if ((self.format != 1 and self.format != 2) or zero(self.id) or zero(self.schema) or zero(self.authorization) or self.generation == 0 or self.generation > max_public_counter or
            self.retention_ns == 0 or self.local_residence_ns > self.retention_ns or
            (self.kind == .summary) != (self.summary != null)) return error.InvalidDatasetDefinition;
        if (self.format == 2 and (self.summary == null or self.summary.?.initial_history != .retained_source_rows)) return error.InvalidDatasetDefinition;
        if (self.format == 1 and self.summary != null and self.summary.?.initial_history != .certified) return error.InvalidDatasetDefinition;
        if (self.schema_json.len == 0 or self.schema_json.len > 16384) return error.InvalidDatasetDefinition;
        var digest: Id = undefined;
        std.crypto.hash.sha2.Sha256.hash(self.schema_json, &digest, .{});
        if (!std.mem.eql(u8, &digest, &self.schema)) return error.InvalidDatasetDefinition;
        if (!validObjectJSON(self.schema_json)) return error.InvalidDatasetDefinition;
        if (self.summary) |s| {
            _ = std.fmt.parseInt(i128, s.origin_ns, 10) catch return error.InvalidDatasetDefinition;
            if (s.bucket_semantic_version != 1 or !std.mem.eql(u8, s.timezone, "UTC") or !validObjectJSON(s.source_filter_json)) return error.InvalidDatasetDefinition;
            if (zero(s.source) or std.mem.eql(u8, &s.source, &self.id) or s.timestamp.len == 0 or s.interval_ns == 0 or s.measures.len == 0 or s.measures.len > 256 or s.dimensions.len > 32) return error.InvalidDatasetDefinition;
            for (s.dimensions, 0..) |name, i| {
                if (name.len == 0) return error.InvalidDatasetDefinition;
                for (s.dimensions[0..i]) |prior| if (std.mem.eql(u8, prior, name)) return error.InvalidDatasetDefinition;
            }
            if (s.projection) |projection| {
                projection.validate() catch return error.InvalidDatasetDefinition;
                if (projection.dimensions.len != s.dimensions.len or projection.measures.len != s.measures.len) return error.InvalidDatasetDefinition;
                for (projection.measures, s.measures) |reference, measure| if ((reference == null) != (measure.field == null)) return error.InvalidDatasetDefinition;
            }
            for (s.measures, 0..) |m, i| {
                if (m.name.len == 0 or (m.law != .count and (m.field == null or m.field.?.len == 0))) return error.InvalidDatasetDefinition;
                for (s.measures[0..i]) |prior| if (std.mem.eql(u8, prior.name, m.name)) return error.InvalidDatasetDefinition;
            }
        }
    }
};

/// Decimal strings keep signed nanoseconds exact across JSON/JavaScript hosts.
pub const Range = struct {
    start_ns: []const u8,
    end_ns: []const u8,
    pub fn start(self: Range) !i128 {
        return std.fmt.parseInt(i128, self.start_ns, 10) catch error.InvalidDatasetCoverage;
    }
    pub fn end(self: Range) !i128 {
        return std.fmt.parseInt(i128, self.end_ns, 10) catch error.InvalidDatasetCoverage;
    }
    pub fn validate(self: Range) !void {
        if (try self.start() >= try self.end()) return error.InvalidDatasetCoverage;
    }
    pub fn overlaps(self: Range, other: Range) !bool {
        return try self.start() < try other.end() and try other.start() < try self.end();
    }
};

/// Ordered summary row identity: sign-biased big-endian i128 bucket start,
/// followed by SHA256(canonical dimensions), encoded as lowercase hex. Spans
/// are half open and permit arbitrarily many bounded segments within a bucket.
pub const RowKey = [96]u8;
pub fn rowKey(timestamp: i128, dimensions: Id) RowKey {
    var bytes: [48]u8 = undefined;
    std.mem.writeInt(u128, bytes[0..16], @as(u128, @bitCast(timestamp)) ^ (@as(u128, 1) << 127), .big);
    bytes[16..].* = dimensions;
    return std.fmt.bytesToHex(bytes, .lower);
}
pub fn rowKeyTimestamp(key: RowKey) !i128 {
    const biased = std.fmt.parseInt(u128, key[0..32], 16) catch return error.InvalidDatasetCoverage;
    return @bitCast(biased ^ (@as(u128, 1) << 127));
}
pub const KeySpan = struct {
    start: RowKey,
    end: RowKey,
    pub fn fromRange(range: Range) !KeySpan {
        try range.validate();
        return .{ .start = rowKey(try range.start(), @splat(0)), .end = rowKey(try range.end(), @splat(0)) };
    }
    pub fn validate(self: KeySpan) !void {
        for ([_]RowKey{ self.start, self.end }) |key| for (key) |c| {
            if (!std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return error.InvalidDatasetCoverage;
        };
        if (std.mem.order(u8, &self.start, &self.end) != .lt) return error.InvalidDatasetCoverage;
    }
    pub fn contains(self: KeySpan, other: KeySpan) bool {
        return std.mem.order(u8, &self.start, &other.start) != .gt and std.mem.order(u8, &self.end, &other.end) != .lt;
    }
    pub fn overlaps(self: KeySpan, other: KeySpan) bool {
        return std.mem.order(u8, &self.start, &other.end) == .lt and std.mem.order(u8, &other.start, &self.end) == .lt;
    }
};

pub const SourcePosition = struct {
    partition: Id,
    incarnation: u64,
    sequence: u64,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writeFields(self, .{"sequence"}, writer);
    }
    pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!@This() {
        return fromWire(@This(), try std.json.innerParse(SourcePositionWire, a, source, options));
    }
    pub fn jsonParseFromValue(a: std.mem.Allocator, value: std.json.Value, options: std.json.ParseOptions) !@This() {
        return fromWire(@This(), try std.json.innerParseFromValue(SourcePositionWire, a, value, options));
    }
    pub fn covers(self: SourcePosition, other: SourcePosition) bool {
        return std.mem.eql(u8, &self.partition, &other.partition) and self.incarnation == other.incarnation and self.sequence >= other.sequence;
    }
};

/// A segment identifies one disjoint source partition/time extent. Replicas and
/// caches share this identity; they never become additional query inputs.
pub const Segment = struct {
    format: u8 = 1,
    id: Id,
    dataset: Id,
    definition_generation: u64,
    schema: Id,
    state_encoding: u32,
    coverage: Range,
    /// Absent in legacy whole-time extents. Explicit spans split large buckets.
    key_span: ?KeySpan = null,
    source: SourcePosition,
    finalized: bool,
    checksum: Id,
    byte_len: u64,
    placement_revision: u64 = 1,
    location: Id,
    access: Access = .local,
    /// A previous local copy awaiting reader-safe reclamation.
    previous_local: ?Id = null,
    /// Durable remote copy retained across restore; never an extra query input.
    archive: ?Id = null,
    location_collected: bool = false,
    archive_collected: bool = false,
    local_retiring: bool = false,
    retiring: bool = false,

    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writeFields(self, .{}, writer);
    }

    pub fn span(self: Segment) !KeySpan {
        return self.key_span orelse try KeySpan.fromRange(self.coverage);
    }

    pub fn expired(self: Segment, definition: Definition, now_ns: u64) !bool {
        if (now_ns == 0) return error.DatasetLeaseExpired;
        const deadline = std.math.add(i128, try self.coverage.end(), definition.retention_ns) catch return false;
        return deadline <= @as(i128, now_ns);
    }

    pub fn validate(self: Segment, definition: Definition) !void {
        try definition.validate();
        try self.coverage.validate();
        const extent = try self.span();
        try extent.validate();
        if (!(try KeySpan.fromRange(self.coverage)).contains(extent)) return error.InvalidDatasetCoverage;
        if (self.key_span != null) {
            if (try rowKeyTimestamp(extent.start) != try self.coverage.start()) return error.InvalidDatasetCoverage;
            const last = try rowKeyTimestamp(extent.end);
            const width: i128 = if (definition.summary) |summary| summary.interval_ns else 1;
            const end = if (std.mem.allEqual(u8, extent.end[32..], '0')) last else std.math.add(i128, last, width) catch return error.InvalidDatasetCoverage;
            if (end != try self.coverage.end()) return error.InvalidDatasetCoverage;
        }
        if (self.format != 1 or zero(self.id) or zero(self.location) or zero(self.checksum) or zero(self.source.partition) or
            self.source.incarnation == 0 or self.source.incarnation > max_public_counter or self.placement_revision > max_public_counter or self.state_encoding == 0 or self.byte_len == 0 or self.placement_revision == 0 or
            !std.mem.eql(u8, &self.dataset, &definition.id) or self.definition_generation != definition.generation or
            !std.mem.eql(u8, &self.schema, &definition.schema)) return error.InvalidDatasetSegment;
        if (self.previous_local) |previous| if (zero(previous) or std.mem.eql(u8, &previous, &self.location) or self.access == .local) return error.InvalidDatasetSegment;
        if (self.local_retiring and self.previous_local == null) return error.InvalidDatasetSegment;
        if ((self.location_collected or self.archive_collected) and !self.retiring) return error.InvalidDatasetSegment;
        if (self.archive) |archive| if (zero(archive)) return error.InvalidDatasetSegment;
        if (definition.summary) |s| {
            const start = try self.coverage.start();
            const end = try self.coverage.end();
            const width: i128 = s.interval_ns;
            if (@mod(start, width) != @mod(try std.fmt.parseInt(i128, s.origin_ns, 10), width) or @mod(end, width) != @mod(try std.fmt.parseInt(i128, s.origin_ns, 10), width)) return error.InvalidDatasetCoverage;
        }
    }
};

pub const RequiredSummary = struct {
    dataset: Id,
    definition_generation: u64,
    source: SourcePosition,
    coverage: Range,
    key_span: ?KeySpan = null,
};

/// Raw source reclamation requires durable incorporation, not an event-time
/// watermark. This proof does not authorize deleting an archive or summary.
pub fn requireSourceReclamation(required: RequiredSummary, definition: Definition, segment: Segment) !void {
    try segment.validate(definition);
    try required.coverage.validate();
    const needed = required.key_span orelse try KeySpan.fromRange(required.coverage);
    try needed.validate();
    if (!(try KeySpan.fromRange(required.coverage)).contains(needed)) return error.InvalidDatasetCoverage;
    if (!std.mem.eql(u8, &required.dataset, &definition.id) or required.definition_generation != definition.generation or
        definition.kind != .summary or segment.retiring or !segment.finalized or !segment.source.covers(required.source) or
        !(try segment.span()).contains(needed)) return error.DatasetDependencyPending;
}

pub fn zero(id: Id) bool {
    return std.mem.allEqual(u8, &id, 0);
}

fn validObjectJSON(bytes: []const u8) bool {
    if (bytes.len == 0 or bytes.len > 16384) return false;
    var memory: [8192]u8 = undefined;
    var allocator = std.heap.FixedBufferAllocator.init(&memory);
    const json = std.mem.trimStart(u8, bytes, " \t\r\n");
    return json.len != 0 and json[0] == '{' and (std.json.validate(allocator.allocator(), json) catch false);
}

const DecimalU64 = struct {
    value: u64,
    pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!@This() {
        const text = try std.json.innerParse([]const u8, a, source, options);
        return .{ .value = std.fmt.parseInt(u64, text, 10) catch return error.UnexpectedToken };
    }
    pub fn jsonParseFromValue(_: std.mem.Allocator, value: std.json.Value, _: std.json.ParseOptions) !@This() {
        if (value != .string) return error.UnexpectedToken;
        return .{ .value = std.fmt.parseInt(u64, value.string, 10) catch return error.UnexpectedToken };
    }
};
const SummaryWire = struct {
    source: Id,
    timestamp: []const u8,
    source_filter_json: []const u8 = "{\"match_all\":{}}",
    bucket_semantic_version: u32 = 1,
    timezone: []const u8 = "UTC",
    dimensions: []const []const u8 = &.{},
    interval_ns: DecimalU64,
    origin_ns: []const u8 = "0",
    mutable_for_ns: DecimalU64,
    measures: []const Measure,
    projection: ?@import("retained_dataset_projection.zig").Projection = null,
    initial_history: InitialHistory = .certified,
};
const DefinitionWire = struct {
    format: u8 = 1,
    id: Id,
    generation: u64,
    kind: Kind,
    schema: Id,
    schema_json: []const u8,
    authorization: Id,
    retention_ns: DecimalU64,
    local_residence_ns: DecimalU64,
    summary: ?Summary = null,
};
const SourcePositionWire = struct { partition: Id, incarnation: u64, sequence: DecimalU64 };
fn fromWire(comptime T: type, value: anytype) T {
    var result: T = undefined;
    inline for (@typeInfo(T).@"struct".field_names) |field| {
        const current = @field(value, field);
        @field(result, field) = if (@TypeOf(current) == DecimalU64) current.value else current;
    }
    return result;
}
pub fn writeFields(value: anytype, comptime names: anytype, writer: anytype) !void {
    return writeFieldsOmitting(value, names, .{}, writer);
}

pub fn writeFieldsOmitting(value: anytype, comptime names: anytype, comptime omitted: anytype, writer: anytype) !void {
    try writer.beginObject();
    inline for (@typeInfo(@TypeOf(value)).@"struct".field_names) |field| {
        const skip = comptime blk: {
            for (omitted) |name| if (std.mem.eql(u8, name, field)) break :blk true;
            break :blk false;
        };
        if (comptime skip) continue;
        try writer.objectField(field);
        const decimal = comptime blk: {
            for (names) |name| {
                if (std.mem.eql(u8, name, field)) break :blk true;
            }
            break :blk false;
        };
        if (decimal) {
            var buffer: [32]u8 = undefined;
            try writer.write(std.fmt.bufPrint(&buffer, "{d}", .{@field(value, field)}) catch unreachable);
        } else {
            const current = @field(value, field);
            if (@TypeOf(current) == Id) {
                try writeId(current, writer);
            } else if (@TypeOf(current) == ?Id) {
                if (current) |id| try writeId(id, writer) else try writer.write(null);
            } else try writer.write(current);
        }
    }
    try writer.endObject();
}

fn writeId(id: Id, writer: anytype) !void {
    try writer.beginArray();
    for (id) |byte| try writer.write(byte);
    try writer.endArray();
}
