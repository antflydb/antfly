// Copyright 2026 Antfly, Inc.
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
// limitations under the License.

//! Deterministic MAP hypervector primitives.
//!
//! This module deliberately stops at the enrichment boundary. It turns
//! canonical typed/path values and semantic embeddings into dense `f32`
//! artifacts; the existing dense index ranks those artifacts and the graph
//! executor remains the source of relational truth.
//!
//! Coordinate semantics are versioned and independent of Zig's standard random
//! implementation. Changing any algorithm below requires a new encoder version.

const std = @import("std");

pub const current_encoder_version: u16 = 1;
pub const current_canonicalization_version: u16 = 1;
pub const atomic_algorithm = "sha256-splitmix64-bipolar-v1";
pub const projection_algorithm = "splitmix64-rademacher-row-major-v1";
pub const fingerprint_algorithm = "sha256-hdc-encoder-identity-v1";

pub const default_dimensions: u32 = 10_000;
pub const default_atomic_seed: u64 = 13;
pub const max_dimensions: u32 = 65_536;
pub const max_embedding_dimensions: u32 = 65_536;
pub const max_projection_coordinates: u64 = 134_217_728;
pub const max_structural_paths: usize = 256;
pub const max_structural_path_bytes: usize = 1024;

const splitmix_increment: u64 = 0x9e37_79b9_7f4a_7c15;
const projection_row_domain: u64 = 0x4844_435f_5052_4f4a;
const projection_tie_domain: u64 = 0x4844_435f_5449_4521;

pub const ValueKind = enum(u8) {
    null = 0,
    boolean = 1,
    integer = 2,
    number = 3,
    string = 4,
    timestamp = 5,
    bytes = 6,
};

pub const CanonicalValue = struct {
    kind: ValueKind,
    bytes: []const u8,
};

/// User-facing HDC controls after defaults and path ordering are canonicalized.
///
/// The embedding provider remains configured by the ordinary embeddings index.
/// This structure only describes the deterministic HDC transform layered on it.
pub const UserConfig = struct {
    alloc: std.mem.Allocator,
    dimensions: u32 = default_dimensions,
    atomic_seed: u64 = default_atomic_seed,
    projection_seed: u64 = default_atomic_seed,
    semantic_weight: f32 = 8,
    structural_paths: []const []const u8 = &.{},

    pub fn parseValue(alloc: std.mem.Allocator, value: std.json.Value) !UserConfig {
        if (value != .object) return error.InvalidHdcConfig;
        var config = UserConfig{ .alloc = alloc };
        errdefer config.deinit();

        var saw_projection_seed = false;
        var paths_value: ?std.json.Value = null;
        var iterator = value.object.iterator();
        while (iterator.next()) |entry| {
            const name = entry.key_ptr.*;
            const field = entry.value_ptr.*;
            if (std.mem.eql(u8, name, "dimensions")) {
                config.dimensions = try jsonU32(field);
            } else if (std.mem.eql(u8, name, "seed")) {
                config.atomic_seed = try jsonU64(field);
            } else if (std.mem.eql(u8, name, "projection_seed")) {
                config.projection_seed = try jsonU64(field);
                saw_projection_seed = true;
            } else if (std.mem.eql(u8, name, "semantic_weight")) {
                config.semantic_weight = try jsonF32(field);
            } else if (std.mem.eql(u8, name, "structural_paths")) {
                paths_value = field;
            } else {
                return error.InvalidHdcConfig;
            }
        }
        if (!saw_projection_seed) config.projection_seed = config.atomic_seed;
        if (config.dimensions == 0 or config.dimensions > max_dimensions) {
            return error.InvalidHypervectorDimensions;
        }
        if (!std.math.isFinite(config.semantic_weight) or config.semantic_weight < 0) {
            return error.InvalidSemanticWeight;
        }

        if (paths_value) |paths| {
            if (paths != .array or paths.array.items.len > max_structural_paths) {
                return error.InvalidStructuralPaths;
            }
            const owned_paths = try alloc.alloc([]const u8, paths.array.items.len);
            var initialized: usize = 0;
            errdefer {
                for (owned_paths[0..initialized]) |path| alloc.free(@constCast(path));
                alloc.free(owned_paths);
            }
            for (paths.array.items, 0..) |path_value, index| {
                if (path_value != .string or
                    path_value.string.len == 0 or
                    path_value.string.len > max_structural_path_bytes or
                    !validStructuralPath(path_value.string))
                {
                    return error.InvalidStructuralPath;
                }
                owned_paths[index] = try alloc.dupe(u8, path_value.string);
                initialized += 1;
            }
            std.mem.sort([]const u8, owned_paths, {}, lessThanString);
            if (owned_paths.len > 1) {
                for (owned_paths[1..], owned_paths[0 .. owned_paths.len - 1]) |path, prior| {
                    if (std.mem.eql(u8, path, prior)) return error.DuplicateStructuralPath;
                }
            }
            config.structural_paths = owned_paths;
        }
        return config;
    }

    pub fn deinit(self: *UserConfig) void {
        for (self.structural_paths) |path| self.alloc.free(@constCast(path));
        if (self.structural_paths.len > 0) self.alloc.free(@constCast(self.structural_paths));
        self.* = undefined;
    }

    pub fn eql(left: UserConfig, right: UserConfig) bool {
        if (left.dimensions != right.dimensions or
            left.atomic_seed != right.atomic_seed or
            left.projection_seed != right.projection_seed or
            @as(u32, @bitCast(left.semantic_weight)) != @as(u32, @bitCast(right.semantic_weight)) or
            left.structural_paths.len != right.structural_paths.len)
        {
            return false;
        }
        for (left.structural_paths, right.structural_paths) |left_path, right_path| {
            if (!std.mem.eql(u8, left_path, right_path)) return false;
        }
        return true;
    }

    pub fn projection(self: UserConfig, input_dimensions: u32) Projection {
        return .{
            .input_dimensions = input_dimensions,
            .output_dimensions = self.dimensions,
            .seed = self.projection_seed,
        };
    }

    pub fn stringifyAlloc(self: UserConfig, alloc: std.mem.Allocator) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);
        try out.appendSlice(alloc, "{\"dimensions\":");
        try appendUnsigned(alloc, &out, self.dimensions);
        try out.appendSlice(alloc, ",\"seed\":");
        try appendUnsigned(alloc, &out, self.atomic_seed);
        try out.appendSlice(alloc, ",\"projection_seed\":");
        try appendUnsigned(alloc, &out, self.projection_seed);
        try out.appendSlice(alloc, ",\"semantic_weight\":");
        const weight = try std.fmt.allocPrint(alloc, "{d}", .{self.semantic_weight});
        defer alloc.free(weight);
        try out.appendSlice(alloc, weight);
        try out.appendSlice(alloc, ",\"structural_paths\":[");
        for (self.structural_paths, 0..) |path, index| {
            if (index > 0) try out.append(alloc, ',');
            const encoded = try std.json.Stringify.valueAlloc(alloc, path, .{});
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        }
        try out.appendSlice(alloc, "]}");
        return try out.toOwnedSlice(alloc);
    }

    /// Hashes the source inputs that can affect a complete node vector.
    ///
    /// Including the normalized HDC config and source document makes ordinary
    /// enrichment skip/rebuild logic invalidate stale structural vectors. The
    /// selected-path-only optimization can be added after churn is measured.
    pub fn sourceHash(self: UserConfig, raw_document: []const u8, semantic_text: []const u8) u64 {
        var hasher = std.hash.Wyhash.init(0x4844_435f_5352_4331);
        hashWyBytes(&hasher, raw_document);
        hashWyBytes(&hasher, semantic_text);
        hashWyU32(&hasher, self.dimensions);
        hashWyU64(&hasher, self.atomic_seed);
        hashWyU64(&hasher, self.projection_seed);
        hashWyU32(&hasher, @bitCast(self.semantic_weight));
        for (self.structural_paths) |path| hashWyBytes(&hasher, path);
        return hasher.final();
    }
};

pub const EmbeddingNormalization = enum(u8) {
    l2 = 1,
};

pub const OutputPrecision = enum(u8) {
    f32 = 1,
};

pub const SemanticIdentity = struct {
    source_path: []const u8,
    template_version: u32 = 1,
    embedding_provider: []const u8,
    embedding_model: []const u8,
    embedding_model_digest: []const u8,
    embedding_dimensions: u32,
    embedding_normalization: EmbeddingNormalization = .l2,
    projection_seed: u64,
    projection_checksum: [32]u8,
    semantic_weight: f32,
};

/// Complete coordinate-system identity for a persisted HDC artifact.
///
/// `structural_paths` must be strictly sorted and unique. Requiring a canonical
/// order prevents two equivalent configurations from acquiring different
/// fingerprints.
pub const Identity = struct {
    encoder_version: u16 = current_encoder_version,
    dimensions: u32 = default_dimensions,
    atomic_seed: u64 = default_atomic_seed,
    canonicalization_version: u16 = current_canonicalization_version,
    structural_paths: []const []const u8 = &.{},
    semantic: ?SemanticIdentity = null,
    output_precision: OutputPrecision = .f32,

    pub fn validate(self: Identity) !void {
        if (self.encoder_version != current_encoder_version) return error.UnsupportedEncoderVersion;
        if (self.canonicalization_version != current_canonicalization_version) {
            return error.UnsupportedCanonicalizationVersion;
        }
        if (self.dimensions == 0 or self.dimensions > max_dimensions) {
            return error.InvalidHypervectorDimensions;
        }

        var previous: ?[]const u8 = null;
        for (self.structural_paths) |path| {
            if (path.len == 0) return error.InvalidStructuralPath;
            if (previous) |prior| {
                if (std.mem.order(u8, prior, path) != .lt) return error.NonCanonicalStructuralPaths;
            }
            previous = path;
        }

        if (self.semantic) |semantic| {
            if (semantic.source_path.len == 0 or
                semantic.embedding_provider.len == 0 or
                semantic.embedding_model.len == 0 or
                semantic.embedding_model_digest.len == 0)
            {
                return error.InvalidSemanticIdentity;
            }
            if (semantic.embedding_dimensions == 0 or
                semantic.embedding_dimensions > max_embedding_dimensions)
            {
                return error.InvalidEmbeddingDimensions;
            }
            if (!std.math.isFinite(semantic.semantic_weight) or semantic.semantic_weight < 0) {
                return error.InvalidSemanticWeight;
            }

            const projection = Projection{
                .input_dimensions = semantic.embedding_dimensions,
                .output_dimensions = self.dimensions,
                .seed = semantic.projection_seed,
            };
            if (!std.mem.eql(u8, &semantic.projection_checksum, &projection.checksum())) {
                return error.ProjectionChecksumMismatch;
            }
        }
    }

    pub fn fingerprint(self: Identity) ![32]u8 {
        try self.validate();

        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hashBytes(&hasher, fingerprint_algorithm);
        hashU16(&hasher, self.encoder_version);
        hashU32(&hasher, self.dimensions);
        hashU64(&hasher, self.atomic_seed);
        hashBytes(&hasher, atomic_algorithm);
        hashU16(&hasher, self.canonicalization_version);
        hashU32(&hasher, @intCast(self.structural_paths.len));
        for (self.structural_paths) |path| hashBytes(&hasher, path);
        hashU8(&hasher, @intFromEnum(self.output_precision));

        if (self.semantic) |semantic| {
            hashU8(&hasher, 1);
            hashBytes(&hasher, semantic.source_path);
            hashU32(&hasher, semantic.template_version);
            hashBytes(&hasher, semantic.embedding_provider);
            hashBytes(&hasher, semantic.embedding_model);
            hashBytes(&hasher, semantic.embedding_model_digest);
            hashU32(&hasher, semantic.embedding_dimensions);
            hashU8(&hasher, @intFromEnum(semantic.embedding_normalization));
            hashBytes(&hasher, projection_algorithm);
            hashU64(&hasher, semantic.projection_seed);
            hashBytes(&hasher, &semantic.projection_checksum);
            hashU32(&hasher, @bitCast(semantic.semantic_weight));
        } else {
            hashU8(&hasher, 0);
        }

        var digest: [32]u8 = undefined;
        hasher.final(&digest);
        return digest;
    }

    pub fn fingerprintHex(self: Identity, out: *[64]u8) ![]const u8 {
        const digest = try self.fingerprint();
        return try std.fmt.bufPrint(out, "{x}", .{digest});
    }

    pub fn authoritativeBytesPerVector(self: Identity) !u64 {
        try self.validate();
        return @as(u64, self.dimensions) * @sizeOf(f32);
    }
};

/// Streaming Rademacher projection. The logical matrix is never allocated:
/// one deterministic row is generated at a time and accumulated into `out`.
pub const Projection = struct {
    input_dimensions: u32,
    output_dimensions: u32,
    seed: u64,

    pub fn validate(self: Projection) !void {
        if (self.input_dimensions == 0 or self.input_dimensions > max_embedding_dimensions) {
            return error.InvalidEmbeddingDimensions;
        }
        if (self.output_dimensions == 0 or self.output_dimensions > max_dimensions) {
            return error.InvalidHypervectorDimensions;
        }
        const coordinates = try std.math.mul(u64, self.input_dimensions, self.output_dimensions);
        if (coordinates > max_projection_coordinates) return error.ProjectionResourceLimitExceeded;
    }

    pub fn matrixBytes(self: Projection) !u64 {
        try self.validate();
        const coordinates = try std.math.mul(u64, self.input_dimensions, self.output_dimensions);
        return (coordinates + 7) / 8;
    }

    pub fn workingBytes(self: Projection) !u64 {
        try self.validate();
        return @as(u64, self.output_dimensions) * @sizeOf(f32);
    }

    /// Checksums the canonical packed matrix: input rows in ascending order,
    /// each row packed low-coordinate-first into little-endian `u64` words.
    /// Unused bits in the final word of a row are zero.
    pub fn checksum(self: Projection) [32]u8 {
        self.validate() catch unreachable;

        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hashBytes(&hasher, projection_algorithm);
        hashU32(&hasher, self.input_dimensions);
        hashU32(&hasher, self.output_dimensions);
        hashU64(&hasher, self.seed);

        var row: u32 = 0;
        while (row < self.input_dimensions) : (row += 1) {
            var state = projectionRowSeed(self.seed, row);
            var offset: u32 = 0;
            while (offset < self.output_dimensions) : (offset += 64) {
                var word = splitmix64(&state);
                const remaining = self.output_dimensions - offset;
                if (remaining < 64) {
                    word &= (@as(u64, 1) << @intCast(remaining)) - 1;
                }
                hashRawU64(&hasher, word);
            }
        }

        var digest: [32]u8 = undefined;
        hasher.final(&digest);
        return digest;
    }

    /// L2-normalizes `embedding`, projects it, and bipolarizes the result.
    ///
    /// The projection uses O(output dimensions) working memory and rejects
    /// non-finite or zero-norm inputs. `out` may be reused across calls.
    pub fn project(self: Projection, embedding: []const f32, out: []f32) !void {
        try self.validate();
        if (embedding.len != self.input_dimensions) return error.InvalidEmbeddingDimensions;
        if (out.len != self.output_dimensions) return error.InvalidHypervectorDimensions;

        var squared_norm: f64 = 0;
        for (embedding) |component| {
            if (!std.math.isFinite(component)) return error.NonFiniteEmbedding;
            const wide: f64 = component;
            squared_norm += wide * wide;
        }
        if (!(squared_norm > 0) or !std.math.isFinite(squared_norm)) {
            return error.ZeroNormEmbedding;
        }
        const inverse_norm: f64 = 1.0 / @sqrt(squared_norm);

        @memset(out, 0);
        for (embedding, 0..) |component, row_index| {
            const normalized: f32 = @floatCast(@as(f64, component) * inverse_norm);
            var state = projectionRowSeed(self.seed, @intCast(row_index));
            var coordinate: usize = 0;
            while (coordinate < out.len) {
                const signs = splitmix64(&state);
                const available = @min(@as(usize, 64), out.len - coordinate);
                for (0..available) |bit| {
                    const sign: f32 = if ((signs & (@as(u64, 1) << @intCast(bit))) != 0) 1 else -1;
                    out[coordinate + bit] += normalized * sign;
                }
                coordinate += available;
            }
        }

        for (out, 0..) |*coordinate, index| {
            coordinate.* = bipolarValue(coordinate.*, tieBreakBit(self.seed ^ projection_tie_domain, index));
        }
    }
};

pub const Encoder = struct {
    identity: Identity,

    pub fn init(identity: Identity) !Encoder {
        try identity.validate();
        return .{ .identity = identity };
    }

    pub fn atomic(self: Encoder, domain: []const u8, token: []const u8, out: []f32) !void {
        if (out.len != self.identity.dimensions) return error.InvalidHypervectorDimensions;
        var state = atomicSeed(self.identity.atomic_seed, domain, token);
        fillBipolar(&state, out);
    }

    /// Adds one typed key/value association to an unnormalized structural sum.
    /// `scratch` is caller-owned and makes batch memory use explicit.
    pub fn addAssociation(
        self: Encoder,
        accumulator: []f32,
        scratch: []f32,
        canonical_path: []const u8,
        canonical_value: CanonicalValue,
    ) !void {
        try self.updateAssociation(accumulator, scratch, canonical_path, canonical_value, 1);
    }

    pub fn removeAssociation(
        self: Encoder,
        accumulator: []f32,
        scratch: []f32,
        canonical_path: []const u8,
        canonical_value: CanonicalValue,
    ) !void {
        try self.updateAssociation(accumulator, scratch, canonical_path, canonical_value, -1);
    }

    fn updateAssociation(
        self: Encoder,
        accumulator: []f32,
        scratch: []f32,
        canonical_path: []const u8,
        canonical_value: CanonicalValue,
        direction: f32,
    ) !void {
        if (accumulator.len != self.identity.dimensions or scratch.len != self.identity.dimensions) {
            return error.InvalidHypervectorDimensions;
        }
        if (canonical_path.len == 0) return error.InvalidStructuralPath;

        try self.atomic("structural-key", canonical_path, scratch);

        var kind_buf = [_]u8{@intFromEnum(canonical_value.kind)};
        var value_state = atomicSeedParts(
            self.identity.atomic_seed,
            "structural-value",
            &kind_buf,
            canonical_value.bytes,
        );
        var coordinate: usize = 0;
        while (coordinate < accumulator.len) {
            const signs = splitmix64(&value_state);
            const available = @min(@as(usize, 64), accumulator.len - coordinate);
            for (0..available) |bit| {
                const value_sign: f32 = if ((signs & (@as(u64, 1) << @intCast(bit))) != 0) 1 else -1;
                accumulator[coordinate + bit] += direction * scratch[coordinate + bit] * value_sign;
            }
            coordinate += available;
        }
    }

    pub fn bipolarize(
        self: Encoder,
        raw: []const f32,
        context: []const u8,
        out: []f32,
    ) !void {
        if (raw.len != self.identity.dimensions or out.len != self.identity.dimensions) {
            return error.InvalidHypervectorDimensions;
        }
        var tie_state = atomicSeed(self.identity.atomic_seed, "bipolar-tie", context);
        var coordinate: usize = 0;
        while (coordinate < raw.len) {
            const tie_bits = splitmix64(&tie_state);
            const available = @min(@as(usize, 64), raw.len - coordinate);
            for (0..available) |bit| {
                if (!std.math.isFinite(raw[coordinate + bit])) return error.NonFiniteHypervector;
                out[coordinate + bit] = bipolarValue(
                    raw[coordinate + bit],
                    ((tie_bits >> @intCast(bit)) & 1) != 0,
                );
            }
            coordinate += available;
        }
    }

    pub fn combineSemantic(
        self: Encoder,
        structural: []const f32,
        semantic: []const f32,
        out: []f32,
    ) !void {
        const semantic_identity = self.identity.semantic orelse return error.SemanticChannelDisabled;
        if (structural.len != self.identity.dimensions or
            semantic.len != self.identity.dimensions or
            out.len != self.identity.dimensions)
        {
            return error.InvalidHypervectorDimensions;
        }
        for (structural, semantic, out) |structured, projected, *combined| {
            if (!std.math.isFinite(structured) or !std.math.isFinite(projected)) {
                return error.NonFiniteHypervector;
            }
            combined.* = structured + semantic_identity.semantic_weight * projected;
        }
    }
};

pub fn bind(lhs: []const f32, rhs: []const f32, out: []f32) !void {
    if (lhs.len != rhs.len or lhs.len != out.len) return error.InvalidHypervectorDimensions;
    for (lhs, rhs, out) |left, right, *product| {
        if (!std.math.isFinite(left) or !std.math.isFinite(right)) {
            return error.NonFiniteHypervector;
        }
        product.* = left * right;
    }
}

pub fn bundleAdd(accumulator: []f32, value: []const f32) !void {
    if (accumulator.len != value.len) return error.InvalidHypervectorDimensions;
    for (accumulator, value) |*sum, coordinate| {
        if (!std.math.isFinite(sum.*) or !std.math.isFinite(coordinate)) {
            return error.NonFiniteHypervector;
        }
        sum.* += coordinate;
    }
}

/// Combines a projected semantic hypervector with canonical structural values
/// selected from a JSON document.
///
/// Path syntax is dot-separated object keys for canonicalization version 1.
/// Arrays are treated as order-independent multisets (duplicates retain
/// multiplicity); configured objects recursively contribute typed leaf values.
pub fn composeJsonDocument(
    alloc: std.mem.Allocator,
    config: UserConfig,
    raw_document: []const u8,
    semantic: []const f32,
) ![]f32 {
    if (semantic.len != config.dimensions) return error.InvalidHypervectorDimensions;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_document, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidHdcDocument;

    const identity = Identity{
        .dimensions = config.dimensions,
        .atomic_seed = config.atomic_seed,
        .structural_paths = config.structural_paths,
    };
    const encoder = try Encoder.init(identity);
    const out = try alloc.alloc(f32, config.dimensions);
    errdefer alloc.free(out);
    @memset(out, 0);
    const scratch = try alloc.alloc(f32, config.dimensions);
    defer alloc.free(scratch);

    for (config.structural_paths) |path| {
        const selected = valueAtDotPath(parsed.value, path) orelse continue;
        try addJsonValue(alloc, encoder, out, scratch, path, selected);
    }
    for (out, semantic) |*coordinate, semantic_coordinate| {
        if (!std.math.isFinite(semantic_coordinate)) return error.NonFiniteHypervector;
        coordinate.* += config.semantic_weight * semantic_coordinate;
    }
    return out;
}

fn addJsonValue(
    alloc: std.mem.Allocator,
    encoder: Encoder,
    accumulator: []f32,
    scratch: []f32,
    path: []const u8,
    value: std.json.Value,
) !void {
    switch (value) {
        .null => try encoder.addAssociation(accumulator, scratch, path, .{ .kind = .null, .bytes = "" }),
        .bool => |item| try encoder.addAssociation(
            accumulator,
            scratch,
            path,
            .{ .kind = .boolean, .bytes = if (item) "true" else "false" },
        ),
        .integer => |item| {
            const encoded = try std.fmt.allocPrint(alloc, "{d}", .{item});
            defer alloc.free(encoded);
            try encoder.addAssociation(accumulator, scratch, path, .{ .kind = .integer, .bytes = encoded });
        },
        .float => |item| {
            if (!std.math.isFinite(item)) return error.NonFiniteStructuralValue;
            const encoded = try std.json.Stringify.valueAlloc(alloc, item, .{});
            defer alloc.free(encoded);
            try encoder.addAssociation(accumulator, scratch, path, .{ .kind = .number, .bytes = encoded });
        },
        .number_string => |item| try encoder.addAssociation(
            accumulator,
            scratch,
            path,
            .{ .kind = .number, .bytes = item },
        ),
        .string => |item| try encoder.addAssociation(
            accumulator,
            scratch,
            path,
            .{ .kind = .string, .bytes = item },
        ),
        .array => |array| {
            for (array.items) |item| try addJsonValue(alloc, encoder, accumulator, scratch, path, item);
        },
        .object => |object| {
            var iterator = object.iterator();
            while (iterator.next()) |entry| {
                const child_path = try std.fmt.allocPrint(
                    alloc,
                    "{s}\x1f{s}",
                    .{ path, entry.key_ptr.* },
                );
                defer alloc.free(child_path);
                try addJsonValue(alloc, encoder, accumulator, scratch, child_path, entry.value_ptr.*);
            }
        },
    }
}

fn valueAtDotPath(root: std.json.Value, path: []const u8) ?std.json.Value {
    var current = root;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (part.len == 0 or current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}

fn validStructuralPath(path: []const u8) bool {
    if (std.mem.indexOfScalar(u8, path, '\x1f') != null) return false;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (part.len == 0) return false;
    }
    return true;
}

fn lessThanString(_: void, left: []const u8, right: []const u8) bool {
    return std.mem.order(u8, left, right) == .lt;
}

fn jsonU32(value: std.json.Value) !u32 {
    const integer = if (value == .integer) value.integer else return error.InvalidHdcConfig;
    if (integer <= 0) return error.InvalidHdcConfig;
    return std.math.cast(u32, integer) orelse error.InvalidHdcConfig;
}

fn jsonU64(value: std.json.Value) !u64 {
    const integer = if (value == .integer) value.integer else return error.InvalidHdcConfig;
    if (integer < 0) return error.InvalidHdcConfig;
    return std.math.cast(u64, integer) orelse error.InvalidHdcConfig;
}

fn jsonF32(value: std.json.Value) !f32 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| @floatCast(float),
        .number_string => |text| std.fmt.parseFloat(f32, text) catch return error.InvalidHdcConfig,
        else => error.InvalidHdcConfig,
    };
}

fn appendUnsigned(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: anytype) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn hashWyBytes(hasher: *std.hash.Wyhash, value: []const u8) void {
    hashWyU64(hasher, value.len);
    hasher.update(value);
}

fn hashWyU32(hasher: *std.hash.Wyhash, value: u32) void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, value, .little);
    hasher.update(&bytes);
}

fn hashWyU64(hasher: *std.hash.Wyhash, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hasher.update(&bytes);
}

fn fillBipolar(state: *u64, out: []f32) void {
    var coordinate: usize = 0;
    while (coordinate < out.len) {
        const bits = splitmix64(state);
        const available = @min(@as(usize, 64), out.len - coordinate);
        for (0..available) |bit| {
            out[coordinate + bit] = if ((bits & (@as(u64, 1) << @intCast(bit))) != 0) 1 else -1;
        }
        coordinate += available;
    }
}

fn bipolarValue(value: f32, positive_tie: bool) f32 {
    if (value > 0) return 1;
    if (value < 0) return -1;
    return if (positive_tie) 1 else -1;
}

fn tieBreakBit(seed: u64, coordinate: usize) bool {
    var state = seed ^ (@as(u64, @intCast(coordinate)) *% splitmix_increment);
    return (splitmix64(&state) & 1) != 0;
}

fn projectionRowSeed(seed: u64, row: u32) u64 {
    var state = seed ^ projection_row_domain ^ (@as(u64, row) *% splitmix_increment);
    return splitmix64(&state);
}

fn atomicSeed(seed: u64, domain: []const u8, token: []const u8) u64 {
    return atomicSeedParts(seed, domain, "", token);
}

fn atomicSeedParts(seed: u64, domain: []const u8, prefix: []const u8, token: []const u8) u64 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hashBytes(&hasher, atomic_algorithm);
    hashU64(&hasher, seed);
    hashBytes(&hasher, domain);
    hashBytes(&hasher, prefix);
    hashBytes(&hasher, token);
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    return std.mem.readInt(u64, digest[0..8], .little);
}

fn splitmix64(state: *u64) u64 {
    state.* +%= splitmix_increment;
    var z = state.*;
    z = (z ^ (z >> 30)) *% 0xbf58_476d_1ce4_e5b9;
    z = (z ^ (z >> 27)) *% 0x94d0_49bb_1331_11eb;
    return z ^ (z >> 31);
}

fn hashBytes(hasher: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    hashRawU64(hasher, value.len);
    hasher.update(value);
}

fn hashU8(hasher: *std.crypto.hash.sha2.Sha256, value: u8) void {
    hasher.update(&.{value});
}

fn hashU16(hasher: *std.crypto.hash.sha2.Sha256, value: u16) void {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, value, .little);
    hasher.update(&buf);
}

fn hashU32(hasher: *std.crypto.hash.sha2.Sha256, value: u32) void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    hasher.update(&buf);
}

fn hashU64(hasher: *std.crypto.hash.sha2.Sha256, value: u64) void {
    hashRawU64(hasher, value);
}

fn hashRawU64(hasher: *std.crypto.hash.sha2.Sha256, value: u64) void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .little);
    hasher.update(&buf);
}

test "atomic vectors are deterministic and domain separated" {
    const identity = Identity{ .dimensions = 130, .atomic_seed = 13 };
    const encoder = try Encoder.init(identity);
    var first: [130]f32 = undefined;
    var replay: [130]f32 = undefined;
    var other: [130]f32 = undefined;

    try encoder.atomic("structural-key", "region", &first);
    try encoder.atomic("structural-key", "region", &replay);
    try encoder.atomic("structural-value", "region", &other);

    try std.testing.expectEqualSlices(f32, &first, &replay);
    try std.testing.expect(!std.mem.eql(f32, &first, &other));
    for (first) |coordinate| {
        try std.testing.expect(coordinate == -1 or coordinate == 1);
    }

    // Golden prefix protects persisted coordinate semantics from accidental
    // changes to hashing, byte order, or the local PRNG.
    try std.testing.expectEqualSlices(f32, &.{
        -1, 1, -1, 1,  -1, -1, 1, 1,
        1,  1, -1, -1, -1, -1, 1, -1,
    }, first[0..16]);
}

test "MAP binding with a bipolar factor is self inverse" {
    const encoder = try Encoder.init(.{ .dimensions = 64 });
    var key: [64]f32 = undefined;
    var value: [64]f32 = undefined;
    var bound: [64]f32 = undefined;
    var recovered: [64]f32 = undefined;
    try encoder.atomic("key", "region", &key);
    try encoder.atomic("value", "pacific_northwest", &value);
    try bind(&key, &value, &bound);
    try bind(&bound, &key, &recovered);
    try std.testing.expectEqualSlices(f32, &value, &recovered);
}

test "structural association add and remove preserve raw accumulator" {
    const encoder = try Encoder.init(.{
        .dimensions = 96,
        .structural_paths = &.{ "name", "region" },
    });
    var structural = [_]f32{0} ** 96;
    var scratch: [96]f32 = undefined;
    const initial = structural;

    try encoder.addAssociation(
        &structural,
        &scratch,
        "region",
        .{ .kind = .string, .bytes = "pacific_northwest" },
    );
    try std.testing.expect(!std.mem.eql(f32, &initial, &structural));
    try encoder.removeAssociation(
        &structural,
        &scratch,
        "region",
        .{ .kind = .string, .bytes = "pacific_northwest" },
    );
    try std.testing.expectEqualSlices(f32, &initial, &structural);
}

test "typed canonical values do not alias" {
    const encoder = try Encoder.init(.{ .dimensions = 96 });
    var as_integer = [_]f32{0} ** 96;
    var as_string = [_]f32{0} ** 96;
    var scratch: [96]f32 = undefined;
    try encoder.addAssociation(&as_integer, &scratch, "value", .{ .kind = .integer, .bytes = "1" });
    try encoder.addAssociation(&as_string, &scratch, "value", .{ .kind = .string, .bytes = "1" });
    try std.testing.expect(!std.mem.eql(f32, &as_integer, &as_string));
}

test "projection is deterministic scale invariant and bounded in memory" {
    const projection = Projection{
        .input_dimensions = 3,
        .output_dimensions = 130,
        .seed = 29,
    };
    const input = [_]f32{ 1, -2, 4 };
    const scaled = [_]f32{ 3, -6, 12 };
    var first: [130]f32 = undefined;
    var replay: [130]f32 = undefined;
    var scaled_result: [130]f32 = undefined;

    try projection.project(&input, &first);
    try projection.project(&input, &replay);
    try projection.project(&scaled, &scaled_result);

    try std.testing.expectEqualSlices(f32, &first, &replay);
    try std.testing.expectEqualSlices(f32, &first, &scaled_result);
    try std.testing.expectEqual(@as(u64, 520), try projection.workingBytes());
    try std.testing.expectEqual(@as(u64, 49), try projection.matrixBytes());
    for (first) |coordinate| {
        try std.testing.expect(coordinate == -1 or coordinate == 1);
    }
}

test "projection rejects malformed embeddings" {
    const projection = Projection{
        .input_dimensions = 2,
        .output_dimensions = 8,
        .seed = 1,
    };
    var out: [8]f32 = undefined;
    try std.testing.expectError(error.ZeroNormEmbedding, projection.project(&.{ 0, 0 }, &out));
    try std.testing.expectError(
        error.NonFiniteEmbedding,
        projection.project(&.{ std.math.nan(f32), 1 }, &out),
    );
    try std.testing.expectError(error.InvalidEmbeddingDimensions, projection.project(&.{1}, &out));
    try std.testing.expectError(
        error.ProjectionResourceLimitExceeded,
        (Projection{
            .input_dimensions = max_embedding_dimensions,
            .output_dimensions = max_dimensions,
            .seed = 1,
        }).validate(),
    );
}

test "identity validates projection checksum and fingerprints semantic drift" {
    const projection = Projection{
        .input_dimensions = 3,
        .output_dimensions = 64,
        .seed = 13,
    };
    const semantic = SemanticIdentity{
        .source_path = "description",
        .embedding_provider = "antfly",
        .embedding_model = "test-model",
        .embedding_model_digest = "sha256:model-v1",
        .embedding_dimensions = 3,
        .projection_seed = 13,
        .projection_checksum = projection.checksum(),
        .semantic_weight = 8,
    };
    try std.testing.expectEqualSlices(
        u8,
        &.{
            0x3c, 0xbf, 0x49, 0x40, 0x79, 0xfc, 0x64, 0x31,
            0xcc, 0xee, 0x87, 0xe8, 0x28, 0x6f, 0x86, 0xd4,
            0x4d, 0xb2, 0x30, 0x72, 0x57, 0x14, 0xea, 0xe5,
            0x8b, 0x6f, 0x45, 0xf1, 0x97, 0x83, 0xbe, 0xc4,
        },
        &semantic.projection_checksum,
    );
    const first = Identity{
        .dimensions = 64,
        .structural_paths = &.{ "name", "region" },
        .semantic = semantic,
    };
    var changed_semantic = semantic;
    changed_semantic.semantic_weight = 7;
    const changed = Identity{
        .dimensions = 64,
        .structural_paths = &.{ "name", "region" },
        .semantic = changed_semantic,
    };

    try first.validate();
    try std.testing.expect(!std.mem.eql(u8, &try first.fingerprint(), &try changed.fingerprint()));
    var fingerprint_hex: [64]u8 = undefined;
    const encoded_fingerprint = try first.fingerprintHex(&fingerprint_hex);
    try std.testing.expectEqual(@as(usize, 64), encoded_fingerprint.len);

    var invalid_semantic = semantic;
    invalid_semantic.projection_checksum[0] ^= 1;
    try std.testing.expectError(error.ProjectionChecksumMismatch, (Identity{
        .dimensions = 64,
        .semantic = invalid_semantic,
    }).validate());
}

test "identity requires canonical structural path ordering" {
    try std.testing.expectError(error.NonCanonicalStructuralPaths, (Identity{
        .dimensions = 64,
        .structural_paths = &.{ "region", "name" },
    }).validate());
    try std.testing.expectError(error.NonCanonicalStructuralPaths, (Identity{
        .dimensions = 64,
        .structural_paths = &.{ "name", "name" },
    }).validate());
}

test "semantic combination retains raw structural magnitude" {
    const projection = Projection{
        .input_dimensions = 2,
        .output_dimensions = 4,
        .seed = 5,
    };
    const identity = Identity{
        .dimensions = 4,
        .semantic = .{
            .source_path = "description",
            .embedding_provider = "antfly",
            .embedding_model = "test-model",
            .embedding_model_digest = "sha256:model-v1",
            .embedding_dimensions = 2,
            .projection_seed = 5,
            .projection_checksum = projection.checksum(),
            .semantic_weight = 2,
        },
    };
    const encoder = try Encoder.init(identity);
    const structural = [_]f32{ 3, -1, 0, 2 };
    const semantic = [_]f32{ 1, -1, 1, -1 };
    var combined: [4]f32 = undefined;
    try encoder.combineSemantic(&structural, &semantic, &combined);
    try std.testing.expectEqualSlices(f32, &.{ 5, -3, 2, 0 }, &combined);
}

test "user config canonicalizes paths and round trips" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        \\{"dimensions":64,"seed":17,"semantic_weight":3,"structural_paths":["region","name"]}
    ,
        .{},
    );
    defer parsed.deinit();
    var config = try UserConfig.parseValue(alloc, parsed.value);
    defer config.deinit();
    try std.testing.expectEqual(@as(u64, 17), config.projection_seed);
    try std.testing.expectEqualStrings("name", config.structural_paths[0]);
    try std.testing.expectEqualStrings("region", config.structural_paths[1]);

    const encoded = try config.stringifyAlloc(alloc);
    defer alloc.free(encoded);
    var reparsed_json = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    defer reparsed_json.deinit();
    var reparsed = try UserConfig.parseValue(alloc, reparsed_json.value);
    defer reparsed.deinit();
    try std.testing.expect(UserConfig.eql(config, reparsed));
}

test "user config rejects duplicate and unknown fields" {
    const alloc = std.testing.allocator;
    var duplicate = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        \\{"structural_paths":["region","region"]}
    ,
        .{},
    );
    defer duplicate.deinit();
    try std.testing.expectError(error.DuplicateStructuralPath, UserConfig.parseValue(alloc, duplicate.value));

    var unknown = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        \\{"dimensons":10000}
    ,
        .{},
    );
    defer unknown.deinit();
    try std.testing.expectError(error.InvalidHdcConfig, UserConfig.parseValue(alloc, unknown.value));

    var invalid_path = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        \\{"structural_paths":["region..name"]}
    ,
        .{},
    );
    defer invalid_path.deinit();
    try std.testing.expectError(error.InvalidStructuralPath, UserConfig.parseValue(alloc, invalid_path.value));
}

test "JSON document composition is deterministic and typed" {
    const alloc = std.testing.allocator;
    var config_json = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        \\{"dimensions":64,"seed":13,"semantic_weight":2,"structural_paths":["features","region"]}
    ,
        .{},
    );
    defer config_json.deinit();
    var config = try UserConfig.parseValue(alloc, config_json.value);
    defer config.deinit();

    var semantic = [_]f32{1} ** 64;
    const first = try composeJsonDocument(
        alloc,
        config,
        \\{"region":"pacific_northwest","features":["coast","rain"]}
    ,
        &semantic,
    );
    defer alloc.free(first);
    const reordered = try composeJsonDocument(
        alloc,
        config,
        \\{"features":["rain","coast"],"region":"pacific_northwest"}
    ,
        &semantic,
    );
    defer alloc.free(reordered);
    try std.testing.expectEqualSlices(f32, first, reordered);

    const typed_differently = try composeJsonDocument(
        alloc,
        config,
        \\{"features":["rain","coast"],"region":1}
    ,
        &semantic,
    );
    defer alloc.free(typed_differently);
    try std.testing.expect(!std.mem.eql(f32, first, typed_differently));
}
