// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Closed, allocation-free runtime qualification policy. Bundle integrity,
//! architecture recognition, and a qualified request are separate contracts.
//! Only the identity of bytes consumed by the live session may be checked here;
//! a pathname, listing manifest, or parsed bundle receipt is not a substitute.
//!
//! The production table is deliberately empty. This module does not consume
//! JSON evidence, expose a runtime override, or authenticate release evidence.
//! Adding a row is a reviewed release decision, not an inference from successful
//! loading or from the architecture-wide runtime_available flag.
const std = @import("std");
const bundle = @import("gliner_boundary_bundle.zig");

pub const policy_version: u32 = 1;
pub const max_entries: usize = 64;
pub const Backend = enum { native, metal };

/// Required features come from the complete compiled request, never from
/// capability strings supplied by a model or client. A row declares support
/// for every combination of its listed features within that row's lengths.
/// Separate rows must not be combined to manufacture support for a mixed task.
/// Normal schema, option, resource, and tokenizer validation still applies.
pub const Feature = enum {
    entities,
    entity_attributes,
    classification_single,
    classification_multi,
    classification_ordinal,
    classification_structured,
    classification_constraints,
    legacy_structures,
    records_natural,
    records_latent,
    records_anchorless,
    field_choices,
    field_rules,
    record_occurrence_policy,
    relations,
    relation_endpoints,
    joint_ie,
    joint_constraints,
    regex_validation,
    schema_descriptions,
    classification_context,
    word_whitespace,
    word_char,
    overlap_flat,
    overlap_allow,
    overlap_nested,
    overlap_longest,
    offset_utf8,
    offset_codepoints,
    offset_utf16,
    decoder_auto,
    decoder_exact,
    decoder_beam,
    best_effort,
    single_window,
    long_document,
    record_identity_occurrence,
    record_identity_semantic,
    confidence,
    spans,
};
pub const Features = std.EnumSet(Feature);

/// Both endpoints are inclusive. Comparisons do not add or multiply counts,
/// so an admitted upper endpoint of maxInt(u64) cannot wrap during matching.
pub const Range = struct {
    min: u64,
    max: u64,

    pub fn exact(value: u64) Range {
        return .{ .min = value, .max = value };
    }

    pub fn valid(self: Range) bool {
        return self.min <= self.max;
    }

    pub fn contains(self: Range, observed: Range) bool {
        return self.valid() and observed.valid() and self.min <= observed.min and observed.max <= self.max;
    }
};

/// A complete observed contract, or a row's explicitly admitted ranges.
/// All fields are required: an unknown token count must not default to zero.
/// For multiple observations, retain their minima AND maxima. Taking only
/// maxima could admit a short input through a row qualified only at long sizes.
pub const LengthContract = struct {
    /// Total input items in the public request, not the current execution batch.
    request_items: Range,
    /// Whole original document bytes, before any window slicing.
    document_bytes: Range,
    /// Whole original source words from the selected splitter. Excludes enum
    /// prefixes and synthetic terminal words; UTF-8 bytes are a different unit.
    document_words: Range,
    /// Number of windows for the whole document; one for the single-window path.
    window_count: Range,
    /// Actual prepared window text words: includes a synthetic terminal word,
    /// excludes the schema's enum prefix. It is not whole-document word count.
    window_words: Range,
    /// Actual padded encoder sequence dimension, including schema/prefix and
    /// special tokens. Never use document words or configured capacity here.
    padded_sequence_tokens: Range,

    pub fn valid(self: LengthContract) bool {
        inline for (@typeInfo(LengthContract).@"struct".fields) |field| {
            if (!@field(self, field.name).valid()) return false;
        }
        return true;
    }

    pub fn contains(self: LengthContract, observed: LengthContract) bool {
        inline for (@typeInfo(LengthContract).@"struct".fields) |field| {
            if (!@field(self, field.name).contains(@field(observed, field.name))) return false;
        }
        return true;
    }
};

/// Data only. Identity includes all five file sizes and digests, the variant,
/// and precision. No wildcard identity, implicit backend, or inherited limits.
pub const Entry = struct {
    identity: bundle.Identity,
    backend: Backend,
    features: Features,
    lengths: LengthContract,
};

const production_entries: []const Entry = &.{};

comptime {
    if (!validEntries(production_entries)) @compileError("invalid GLiNER2.5 runtime qualification policy");
}

/// Coarse closed gate only. True would still require an exact live-session
/// lookup; it must never authorize a listing that has no consumed identity.
pub fn hasPublishedProfiles() bool {
    return production_entries.len != 0;
}

/// A bounded selection of private production rows. Treat its representation as
/// opaque; callers obtain it only from start(). No row data or operation for
/// widening a selection is exposed. Reuse the SAME value for the whole request.
pub const Candidates = enum(u64) {
    rejected = 0,
    _,

    /// Intersect with one actual document/window observation. A failed check
    /// consumes all candidates, so retrying with shorter geometry cannot revive
    /// a rejected request. This mutates no global state and allocates nothing.
    pub fn narrow(self: *Candidates, observed: LengthContract) error{UnsupportedGlinerBoundaryRuntime}!void {
        return narrowEntries(production_entries, self, observed);
    }
};

/// Begin using the union of features from EVERY item in the public request.
/// This is feature preflight, not an execution permit. Narrow with all actual
/// prepared geometries before learned work; retain this same candidate set
/// across items and the long-document tokenizer/planning preflight.
pub fn start(consumed: bundle.Identity, backend: Backend, required: Features) error{UnsupportedGlinerBoundaryRuntime}!Candidates {
    return startEntries(production_entries, consumed, backend, required);
}

/// Post-load early rejection before tokenization. Success alone is NOT an
/// execution permit: require() must also check actual prepared geometry before
/// learned work. Advertisement requires a verified identity and actual backend.
pub fn supportsFeatures(consumed: bundle.Identity, backend: Backend, required: Features) error{UnsupportedGlinerBoundaryRuntime}!void {
    _ = try start(consumed, backend, required);
}

/// The same row must cover exact identity, backend, the full feature union, and
/// all supplied length ranges. For long documents, use actual tokenizer and
/// planner observations, and enforce before any window executes. For a batch,
/// accumulate observed ranges rather than joining independently matching rows.
pub fn require(consumed: bundle.Identity, backend: Backend, required: Features, observed: LengthContract) error{UnsupportedGlinerBoundaryRuntime}!void {
    var candidates = try start(consumed, backend, required);
    try candidates.narrow(observed);
}

fn equalDigest(expected: bundle.Digest, actual: bundle.Digest) bool {
    return expected.size_bytes == actual.size_bytes and std.mem.eql(u8, &expected.sha256, &actual.sha256);
}

fn equalIdentity(expected: bundle.Identity, actual: bundle.Identity) bool {
    if (expected.backbone != actual.backbone or expected.precision != actual.precision or !equalDigest(expected.weight, actual.weight)) return false;
    for (expected.sidecars, actual.sidecars) |left, right| if (!equalDigest(left, right)) return false;
    return true;
}

fn containsFeatures(supported: Features, required: Features) bool {
    inline for (@typeInfo(Feature).@"enum".fields) |field| {
        const feature: Feature = @enumFromInt(field.value);
        if (required.contains(feature) and !supported.contains(feature)) return false;
    }
    return true;
}

fn validDigest(digest: bundle.Digest) bool {
    if (digest.size_bytes == 0) return false;
    for (digest.sha256) |char| if (!((char >= '0' and char <= '9') or (char >= 'a' and char <= 'f'))) return false;
    return true;
}

fn validEntries(entries: []const Entry) bool {
    if (entries.len > max_entries) return false;
    for (entries) |entry| {
        if (!entry.lengths.valid() or !validDigest(entry.identity.weight)) return false;
        for (entry.identity.sidecars) |digest| if (!validDigest(digest)) return false;
    }
    return true;
}

fn startEntries(entries: []const Entry, consumed: bundle.Identity, backend: Backend, required: Features) error{UnsupportedGlinerBoundaryRuntime}!Candidates {
    if (!validEntries(entries)) return error.UnsupportedGlinerBoundaryRuntime;
    var remaining: u64 = 0;
    for (entries, 0..) |entry, index| {
        if (!equalIdentity(entry.identity, consumed) or entry.backend != backend or !containsFeatures(entry.features, required)) continue;
        remaining |= @as(u64, 1) << @intCast(index);
    }
    if (remaining == 0) return error.UnsupportedGlinerBoundaryRuntime;
    return @enumFromInt(remaining);
}

fn narrowEntries(entries: []const Entry, candidates: *Candidates, observed: LengthContract) error{UnsupportedGlinerBoundaryRuntime}!void {
    const previous = @intFromEnum(candidates.*);
    candidates.* = .rejected;
    if (!validEntries(entries) or !observed.valid()) return error.UnsupportedGlinerBoundaryRuntime;
    const valid_mask = if (entries.len == max_entries) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(entries.len)) - 1;
    if (previous & ~valid_mask != 0) return error.UnsupportedGlinerBoundaryRuntime;
    var remaining: u64 = 0;
    for (entries, 0..) |entry, index| {
        const bit = @as(u64, 1) << @intCast(index);
        if (previous & bit != 0 and entry.lengths.contains(observed)) remaining |= bit;
    }
    candidates.* = @enumFromInt(remaining);
    if (remaining == 0) return error.UnsupportedGlinerBoundaryRuntime;
}

/// Only this module's tests can supply synthetic rows. Runtime callers always
/// use the immutable production table; no allocation or external state occurs.
fn matchEntries(entries: []const Entry, consumed: bundle.Identity, backend: Backend, required: Features, observed: ?LengthContract) error{UnsupportedGlinerBoundaryRuntime}!void {
    var candidates = try startEntries(entries, consumed, backend, required);
    if (observed) |lengths| try narrowEntries(entries, &candidates, lengths);
}

fn testIdentity() bundle.Identity {
    return .{
        .backbone = .small,
        .precision = .fp32,
        .weight = bundle.Digest.of("synthetic tensor bytes"),
        .sidecars = .{
            bundle.Digest.of("synthetic config"),
            bundle.Digest.of("synthetic encoder config"),
            bundle.Digest.of("synthetic tokenizer"),
            bundle.Digest.of("synthetic tokenizer config"),
        },
    };
}

fn testLengths() LengthContract {
    return .{
        .request_items = .{ .min = 1, .max = 4 },
        .document_bytes = .{ .min = 0, .max = 4096 },
        .document_words = .{ .min = 0, .max = 256 },
        .window_count = .{ .min = 1, .max = 8 },
        .window_words = .{ .min = 1, .max = 128 },
        .padded_sequence_tokens = .{ .min = 16, .max = 512 },
    };
}

fn testEntry() Entry {
    return .{ .identity = testIdentity(), .backend = .native, .features = Features.initOne(.entities), .lengths = testLengths() };
}

test "boundary qualification production table denies every variant precision and backend" {
    try std.testing.expect(!@import("gliner_boundary.zig").runtime_available);
    try std.testing.expect(!hasPublishedProfiles());
    var identity = testIdentity();
    const features = Features.initFull();
    inline for (@typeInfo(@TypeOf(identity.backbone)).@"enum".fields) |variant| {
        identity.backbone = @enumFromInt(variant.value);
        inline for (@typeInfo(@TypeOf(identity.precision)).@"enum".fields) |precision| {
            identity.precision = @enumFromInt(precision.value);
            inline for (@typeInfo(Backend).@"enum".fields) |backend| {
                try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, supportsFeatures(identity, @enumFromInt(backend.value), features));
                try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, require(identity, @enumFromInt(backend.value), features, testLengths()));
            }
        }
    }
}

test "boundary qualification matches all five consumed file sizes and hashes" {
    const row = testEntry();
    try matchEntries(&.{row}, row.identity, row.backend, row.features, row.lengths);
    for (0..5) |file| {
        var actual = row.identity;
        const digest = if (file == 0) &actual.weight else &actual.sidecars[file - 1];
        digest.size_bytes += 1;
        // Identity.fingerprint currently omits sizes; the policy must not.
        try std.testing.expectEqual(row.identity.fingerprint(), actual.fingerprint());
        try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, actual, row.backend, row.features, row.lengths));
        digest.size_bytes -= 1;
        digest.sha256[0] = if (digest.sha256[0] == 'a') 'b' else 'a';
        try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, actual, row.backend, row.features, row.lengths));
    }
    var swapped = row.identity;
    std.mem.swap(bundle.Digest, &swapped.sidecars[0], &swapped.sidecars[1]);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, swapped, row.backend, row.features, row.lengths));
}

test "boundary qualification does not inherit variant precision or backend support" {
    const row = testEntry();
    var actual = row.identity;
    actual.backbone = .base;
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, actual, row.backend, row.features, row.lengths));
    actual = row.identity;
    actual.precision = .q8_0;
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, actual, row.backend, row.features, row.lengths));
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, row.identity, .metal, row.features, row.lengths));
}

test "boundary qualification requires one row for a mixed feature contract" {
    const entity = testEntry();
    var classification = entity;
    classification.features = Features.initOne(.classification_multi);
    var mixed = entity;
    mixed.features.insert(.classification_multi);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{ entity, classification }, entity.identity, .native, mixed.features, entity.lengths));
    try matchEntries(&.{mixed}, entity.identity, .native, mixed.features, entity.lengths);
    try matchEntries(&.{mixed}, entity.identity, .native, entity.features, entity.lengths);
    inline for (@typeInfo(Feature).@"enum".fields) |field| {
        const feature: Feature = @enumFromInt(field.value);
        if (feature != .entities) {
            var required = entity.features;
            required.insert(feature);
            try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{entity}, entity.identity, .native, required, entity.lengths));
        }
    }
}

test "boundary qualification checks every inclusive geometry endpoint and unit" {
    const row = testEntry();
    inline for (@typeInfo(LengthContract).@"struct".fields) |field| {
        var observed = row.lengths;
        const range = @field(row.lengths, field.name);
        @field(observed, field.name) = Range.exact(range.min);
        try matchEntries(&.{row}, row.identity, .native, row.features, observed);
        @field(observed, field.name) = Range.exact(range.max);
        try matchEntries(&.{row}, row.identity, .native, row.features, observed);
        @field(observed, field.name) = Range.exact(range.max + 1);
        try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, row.identity, .native, row.features, observed));
        if (range.min != 0) {
            @field(observed, field.name) = Range.exact(range.min - 1);
            try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, row.identity, .native, row.features, observed));
        }
        @field(observed, field.name) = .{ .min = 2, .max = 1 };
        try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, row.identity, .native, row.features, observed));
    }
    // Few words do not excuse an encoded sequence made large by schema tokens.
    var schema_heavy = row.lengths;
    schema_heavy.document_words = Range.exact(1);
    schema_heavy.padded_sequence_tokens = Range.exact(513);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, row.identity, .native, row.features, schema_heavy));
}

test "boundary qualification does not merge geometry or feature grants across rows" {
    var short = testEntry();
    var long = short;
    short.lengths.padded_sequence_tokens = .{ .min = 16, .max = 255 };
    long.lengths.padded_sequence_tokens = .{ .min = 256, .max = 512 };
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{ short, long }, short.identity, .native, short.features, testLengths()));
    // Nor may a feature match from one row borrow another row's larger lengths.
    long.features.insert(.joint_ie);
    var observed = short.lengths;
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{ short, long }, short.identity, .native, long.features, observed));
    observed.padded_sequence_tokens = Range.exact(512);
    try matchEntries(&.{ short, long }, short.identity, .native, long.features, observed);
}

test "boundary qualification intersects every window and cannot revive a failed request" {
    var short = testEntry();
    var long = short;
    short.lengths.padded_sequence_tokens = .{ .min = 16, .max = 255 };
    long.lengths.padded_sequence_tokens = .{ .min = 256, .max = 512 };
    const rows = [_]Entry{ short, long };
    var candidates = try startEntries(&rows, short.identity, .native, short.features);
    var observed = short.lengths;
    observed.padded_sequence_tokens = Range.exact(128);
    try narrowEntries(&rows, &candidates, observed);
    observed.padded_sequence_tokens = Range.exact(384);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, narrowEntries(&rows, &candidates, observed));
    observed.padded_sequence_tokens = Range.exact(128);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, narrowEntries(&rows, &candidates, observed));
    // A real covering row survives both observations in either order.
    const covering = testEntry();
    const covered_rows = [_]Entry{ short, long, covering };
    candidates = try startEntries(&covered_rows, short.identity, .native, short.features);
    try narrowEntries(&covered_rows, &candidates, observed);
    observed.padded_sequence_tokens = Range.exact(384);
    try narrowEntries(&covered_rows, &candidates, observed);
    // Total request count cannot be replaced by a smaller current batch size.
    observed.request_items = Range.exact(5);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, narrowEntries(&covered_rows, &candidates, observed));
}

test "boundary qualification feature preflight never grants unchecked geometry" {
    const row = testEntry();
    try matchEntries(&.{row}, row.identity, .native, row.features, null);
    var observed = row.lengths;
    observed.window_count = Range.exact(9);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, row.identity, .native, row.features, observed));
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, supportsFeatures(row.identity, .native, row.features));
}

test "boundary qualification rejects malformed or oversized tables before matching" {
    const row = testEntry();
    var malformed = row;
    malformed.identity.sidecars[3].sha256[63] = 'G';
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{ row, malformed }, row.identity, .native, row.features, row.lengths));
    malformed = row;
    malformed.identity.weight.size_bytes = 0;
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{malformed}, row.identity, .native, row.features, row.lengths));
    malformed = row;
    malformed.lengths.document_bytes = .{ .min = 3, .max = 2 };
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{malformed}, row.identity, .native, row.features, row.lengths));
    const rows: [max_entries + 1]Entry = @splat(row);
    try matchEntries(rows[0..max_entries], row.identity, .native, row.features, row.lengths);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&rows, row.identity, .native, row.features, row.lengths));
    var invalid_selection: Candidates = @enumFromInt(@as(u64, 1) << 63);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, narrowEntries(&.{row}, &invalid_selection, row.lengths));
    // Even test-supplied synthetic selections cannot access a production row.
    var synthetic = try startEntries(&.{row}, row.identity, .native, row.features);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, synthetic.narrow(row.lengths));
}

test "boundary qualification range matching has no saturating or overflow acceptance" {
    var row = testEntry();
    row.lengths.document_bytes = .{ .min = 0, .max = std.math.maxInt(u64) };
    var observed = row.lengths;
    observed.document_bytes = Range.exact(std.math.maxInt(u64));
    try matchEntries(&.{row}, row.identity, .native, row.features, observed);
    row.lengths.document_bytes.max -= 1;
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, row.identity, .native, row.features, observed));
}
