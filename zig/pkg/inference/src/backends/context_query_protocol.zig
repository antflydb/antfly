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

//! Frozen attention-matching query-source quotas shared by private replay,
//! capture, and serving. A branch allocates only its active sources, while the
//! quota is derived from the complete five-source protocol so branch order or
//! document length cannot give one source more influence.
const sampling = @import("context_query_sampling.zig");
const std = @import("std");
const policy = @import("../runtime/context_policy.zig");
const profiles = @import("../runtime/kv/compaction_profile.zig");

pub const max_sources = 5;
pub const max_training_rows: u32 = 3072;
pub const max_validation_rows: u32 = 1024;
pub const runtime_identity_version: u32 = 2;
const runtime_identity_domain = "antfly.context-query-runtime-protocol.v2\x00";

pub const Partition = enum { training, validation };
pub const Kind = enum {
    request_question_replay,
    fixed_study_prefill,
    fixed_study_generated_response,
};
pub const Source = struct {
    kind: Kind,
    partition: Partition,
    start: u32,
    end: u32,
    active: bool,
};
const BoundSource = struct {
    source: Source,
    bounds: sampling.Bounds,
    local_offset: u32,
};
pub const Selection = struct { partition: Partition, row: u32 };

fn hashInt(hash: *std.crypto.hash.sha2.Sha256, comptime T: type, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    hash.update(&bytes);
}

fn hashSource(hash: *std.crypto.hash.sha2.Sha256, index: u8, source: profiles.QuerySourceAuthority) void {
    hashInt(hash, u8, index);
    hashInt(hash, u8, @backingInt(source.kind));
    hashInt(hash, u8, @backingInt(source.partition));
    hashInt(hash, u8, source.physical_branch);
}

const canonical_sources = [max_sources]profiles.QuerySourceAuthority{
    .{ .kind = .request_question_replay, .partition = .training, .physical_branch = 0 },
    .{ .kind = .fixed_study_prefill, .partition = .training, .physical_branch = 1 },
    .{ .kind = .fixed_study_generated_response, .partition = .training, .physical_branch = 1 },
    .{ .kind = .fixed_study_prefill, .partition = .validation, .physical_branch = 2 },
    .{ .kind = .fixed_study_generated_response, .partition = .validation, .physical_branch = 2 },
};

pub const CaptureAuthorities = struct {
    allocator: std.mem.Allocator,
    values: []profiles.QueryProtocolAuthority,

    pub fn deinit(self: *CaptureAuthorities) void {
        for (self.values) |authority| self.allocator.free(authority.strata);
        self.allocator.free(self.values);
        self.* = undefined;
    }
};

/// Construct profile-free qualification authority from the actual loaded
/// layout and the separately validated compiled fit/gate studies. This is the
/// sole constructor used by capture tooling; production still consumes the
/// frozen profile authorities selected at the loaded-model boundary.
pub fn captureAuthorities(
    allocator: std.mem.Allocator,
    layout: policy.Layout,
    fit: profiles.QueryStudyAuthority,
    gate: profiles.QueryStudyAuthority,
) !CaptureAuthorities {
    if (layout.len == 0 or layout.len > layout.layers.len) return error.InvalidContextLayout;
    if (fit.response_tokens == 0 or gate.response_tokens == 0 or
        fit.generation_mode != .greedy_fixed_length_ignore_eos or gate.generation_mode != .greedy_fixed_length_ignore_eos)
        return error.CompactionQueryProtocolMismatch;
    var count: usize = 0;
    for (layout.layers[0..layout.len], 0..) |layer, index| {
        if (layer.owner > index or layer.owner >= layout.len or layer.kv_heads == 0 or
            layer.query_heads == 0 or layer.query_heads % layer.kv_heads != 0 or layer.head_dim == 0)
            return error.InvalidContextLayout;
        const owner = layout.layers[layer.owner];
        if (owner.owner != layer.owner or owner.kv_heads != layer.kv_heads or
            owner.head_dim != layer.head_dim or owner.sliding_window != layer.sliding_window)
            return error.InvalidContextLayout;
        count += @intFromBool(layer.owner == index and layer.sliding_window == 0);
    }
    if (count == 0) return error.InvalidContextLayout;
    const values = try allocator.alloc(profiles.QueryProtocolAuthority, count);
    errdefer allocator.free(values);
    var initialized: usize = 0;
    errdefer for (values[0..initialized]) |authority| allocator.free(authority.strata);
    for (layout.layers[0..layout.len], 0..) |owner, owner_index| {
        if (owner.owner != owner_index or owner.sliding_window != 0) continue;
        if (owner.kv_heads == 0 or owner.query_heads == 0 or owner.head_dim == 0)
            return error.InvalidContextLayout;
        var strata_count: usize = 0;
        for (layout.layers[0..layout.len]) |consumer| if (consumer.owner == owner_index) {
            if (consumer.kv_heads != owner.kv_heads or consumer.head_dim != owner.head_dim or
                consumer.query_heads == 0 or consumer.query_heads % consumer.kv_heads != 0)
                return error.InvalidContextLayout;
            strata_count = try std.math.add(usize, strata_count, consumer.query_heads);
        };
        if (strata_count == 0 or strata_count > 128) return error.InvalidContextLayout;
        const strata = try allocator.alloc(profiles.QueryStratum, strata_count);
        errdefer allocator.free(strata);
        var cursor: usize = 0;
        for (layout.layers[0..layout.len], 0..) |consumer, consumer_index| {
            if (consumer.owner != owner_index) continue;
            const groups = consumer.query_heads / consumer.kv_heads;
            for (0..consumer.query_heads) |query_head| {
                strata[cursor] = .{
                    .consumer_layer = @intCast(consumer_index),
                    .query_head = @intCast(query_head),
                    .kv_head = @intCast(query_head / groups),
                };
                cursor += 1;
            }
        }
        var authority = profiles.QueryProtocolAuthority{
            .schema_version = runtime_identity_version,
            .layer = @intCast(owner_index),
            .sha256 = @splat(0),
            .query_sampling = .source_uniform_partition_v3,
            .accounting = .serial_physical_branch_upper_bound_v2,
            .max_training_rows = max_training_rows,
            .max_validation_rows = max_validation_rows,
            .sources = canonical_sources,
            .owner_kv_heads = owner.kv_heads,
            .owner_query_heads = owner.query_heads,
            .owner_head_dim = owner.head_dim,
            .strata = strata,
            .fit = fit,
            .gate = gate,
        };
        authority.sha256 = try runtimeIdentity(authority);
        try validateRuntimeAuthority(authority, layout);
        values[initialized] = authority;
        initialized += 1;
    }
    return .{ .allocator = allocator, .values = values };
}

/// Canonical runtime authority shared with the Python sealer. Integer fields
/// are little endian; digest fields are their raw 32 bytes. Request question
/// bytes/length, generated output bytes, sampled counts/ranges and model load
/// generation are observations and are deliberately excluded.
pub fn runtimeIdentity(authority: profiles.QueryProtocolAuthority) ![32]u8 {
    if (authority.schema_version != runtime_identity_version or
        authority.query_sampling != .source_uniform_partition_v3 or
        authority.accounting != .serial_physical_branch_upper_bound_v2 or
        authority.max_training_rows != max_training_rows or authority.max_validation_rows != max_validation_rows or
        !std.meta.eql(authority.sources, canonical_sources) or authority.owner_kv_heads == 0 or
        authority.owner_query_heads == 0 or authority.owner_query_heads % authority.owner_kv_heads != 0 or
        authority.owner_head_dim == 0 or authority.strata.len == 0 or authority.strata.len > 128 or
        authority.fit.response_tokens == 0 or authority.gate.response_tokens == 0)
        return error.InvalidQueryProtocol;
    inline for (.{ authority.fit, authority.gate }) |study| {
        if (std.mem.allEqual(u8, &study.template_sha256, 0) or
            std.mem.allEqual(u8, &study.generation_contract_sha256, 0))
            return error.InvalidQueryProtocol;
    }

    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(runtime_identity_domain);
    hashInt(&hash, u32, authority.schema_version);
    hash.update("source_uniform_partition_v3\x00");
    hash.update("serial_physical_branch_upper_bound_v2\x00");
    hashInt(&hash, u32, authority.max_training_rows);
    hashInt(&hash, u32, authority.max_validation_rows);
    hashInt(&hash, u8, authority.sources.len);
    for (authority.sources, 0..) |source, index| hashSource(&hash, @intCast(index), source);
    hashInt(&hash, u32, authority.layer);
    hashInt(&hash, u32, authority.owner_kv_heads);
    hashInt(&hash, u32, authority.owner_query_heads);
    hashInt(&hash, u32, authority.owner_head_dim);
    for (authority.strata) |stratum| {
        hashInt(&hash, u32, stratum.consumer_layer);
        hashInt(&hash, u32, stratum.query_head);
        hashInt(&hash, u32, stratum.kv_head);
    }
    hashInt(&hash, u32, @intCast(authority.strata.len));
    inline for (.{ authority.fit, authority.gate }) |study| {
        hash.update(&study.template_sha256);
        hash.update(&study.generation_contract_sha256);
        hashInt(&hash, u32, study.response_tokens);
        hashInt(&hash, u8, @backingInt(study.generation_mode));
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

pub fn validateRuntimeAuthority(authority: profiles.QueryProtocolAuthority, layout: policy.Layout) !void {
    if (layout.len == 0 or layout.len > layout.layers.len or authority.layer >= layout.len)
        return error.CompactionQueryProtocolMismatch;
    const owner = layout.layers[authority.layer];
    if (owner.owner != authority.layer or owner.sliding_window != 0 or
        owner.kv_heads != authority.owner_kv_heads or owner.query_heads != authority.owner_query_heads or
        owner.head_dim != authority.owner_head_dim)
        return error.CompactionQueryProtocolMismatch;
    var cursor: usize = 0;
    for (layout.layers[0..layout.len], 0..) |consumer, layer_index| {
        if (consumer.owner != authority.layer) continue;
        if (consumer.kv_heads != owner.kv_heads or consumer.head_dim != owner.head_dim or
            consumer.query_heads == 0 or consumer.query_heads % consumer.kv_heads != 0)
            return error.CompactionQueryProtocolMismatch;
        for (0..consumer.query_heads) |query_head| {
            if (cursor >= authority.strata.len or !std.meta.eql(authority.strata[cursor], profiles.QueryStratum{
                .consumer_layer = @intCast(layer_index),
                .query_head = @intCast(query_head),
                .kv_head = @intCast(query_head / (consumer.query_heads / consumer.kv_heads)),
            })) return error.CompactionQueryProtocolMismatch;
            cursor += 1;
        }
    }
    if (cursor != authority.strata.len) return error.CompactionQueryProtocolMismatch;
    const actual = try runtimeIdentity(authority);
    if (!std.mem.eql(u8, &actual, &authority.sha256)) return error.CompactionQueryProtocolMismatch;
}

/// The runner must call this on the exact compiled/loaded bytes before starting
/// either private study branch. Artifact-declared digests alone are not proof
/// that serving executed the reviewed templates and generation contract.
pub fn validateStudyBytes(study: profiles.QueryStudyAuthority, template_bytes: []const u8, generation_contract_bytes: []const u8) !void {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(template_bytes, &digest, .{});
    if (!std.mem.eql(u8, &digest, &study.template_sha256)) return error.CompactionQueryProtocolMismatch;
    std.crypto.hash.sha2.Sha256.hash(generation_contract_bytes, &digest, .{});
    if (!std.mem.eql(u8, &digest, &study.generation_contract_sha256))
        return error.CompactionQueryProtocolMismatch;
}

test "context query runtime protocol v2 has cross-language golden identity" {
    var layout = policy.Layout{ .len = 3 };
    layout.layers[0] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 8, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    layout.layers[1] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 4, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    layout.layers[2] = .{ .owner = 2, .sliding_window = 0, .kv_heads = 1, .query_heads = 1, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    var authority = profiles.QueryProtocolAuthority{
        .schema_version = 2,
        .layer = 0,
        .sha256 = @splat(0),
        .query_sampling = .source_uniform_partition_v3,
        .accounting = .serial_physical_branch_upper_bound_v2,
        .max_training_rows = 3072,
        .max_validation_rows = 1024,
        .sources = canonical_sources,
        .owner_kv_heads = 2,
        .owner_query_heads = 8,
        .owner_head_dim = 64,
        .strata = &.{
            .{ .consumer_layer = 0, .query_head = 0, .kv_head = 0 },
            .{ .consumer_layer = 0, .query_head = 1, .kv_head = 0 },
            .{ .consumer_layer = 0, .query_head = 2, .kv_head = 0 },
            .{ .consumer_layer = 0, .query_head = 3, .kv_head = 0 },
            .{ .consumer_layer = 0, .query_head = 4, .kv_head = 1 },
            .{ .consumer_layer = 0, .query_head = 5, .kv_head = 1 },
            .{ .consumer_layer = 0, .query_head = 6, .kv_head = 1 },
            .{ .consumer_layer = 0, .query_head = 7, .kv_head = 1 },
            .{ .consumer_layer = 1, .query_head = 0, .kv_head = 0 },
            .{ .consumer_layer = 1, .query_head = 1, .kv_head = 0 },
            .{ .consumer_layer = 1, .query_head = 2, .kv_head = 1 },
            .{ .consumer_layer = 1, .query_head = 3, .kv_head = 1 },
        },
        .fit = .{ .template_sha256 = @splat(0x11), .generation_contract_sha256 = @splat(0x22), .response_tokens = 128, .generation_mode = .greedy_fixed_length_ignore_eos },
        .gate = .{ .template_sha256 = @splat(0x33), .generation_contract_sha256 = @splat(0x44), .response_tokens = 64, .generation_mode = .greedy_fixed_length_ignore_eos },
    };
    const expected = [_]u8{ 0xb3, 0x2e, 0x39, 0x16, 0xdc, 0xa3, 0xe5, 0x79, 0xd9, 0xae, 0xf2, 0x64, 0xc6, 0xb1, 0xce, 0xca, 0xd7, 0x1d, 0x42, 0xb8, 0x77, 0x31, 0xbf, 0x1f, 0xcd, 0x94, 0xc7, 0x5a, 0xcf, 0xfb, 0x33, 0x16 };
    try std.testing.expectEqual(expected, try runtimeIdentity(authority));
    authority.sha256 = expected;
    try validateRuntimeAuthority(authority, layout);
    authority.fit.response_tokens += 1;
    try std.testing.expectError(error.CompactionQueryProtocolMismatch, validateRuntimeAuthority(authority, layout));
}

test "context query capture authorities derive sorted heterogeneous consumer strata" {
    var layout = policy.Layout{ .len = 3 };
    layout.layers[0] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 8, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    layout.layers[1] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 4, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    layout.layers[2] = .{ .owner = 2, .sliding_window = 0, .kv_heads = 1, .query_heads = 1, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    const fit = profiles.QueryStudyAuthority{ .template_sha256 = @splat(0x11), .generation_contract_sha256 = @splat(0x22), .response_tokens = 256, .generation_mode = .greedy_fixed_length_ignore_eos };
    const gate = profiles.QueryStudyAuthority{ .template_sha256 = @splat(0x33), .generation_contract_sha256 = @splat(0x22), .response_tokens = 256, .generation_mode = .greedy_fixed_length_ignore_eos };
    var authorities = try captureAuthorities(std.testing.allocator, layout, fit, gate);
    defer authorities.deinit();
    try std.testing.expectEqual(@as(usize, 2), authorities.values.len);
    try std.testing.expectEqual(@as(u32, 0), authorities.values[0].layer);
    try std.testing.expectEqual(@as(usize, 12), authorities.values[0].strata.len);
    try std.testing.expectEqual(@as(u32, 2), authorities.values[1].layer);
    try std.testing.expectEqual(@as(usize, 1), authorities.values[1].strata.len);
    try std.testing.expectEqual(@as(u32, 1), authorities.values[0].strata[11].kv_head);
}

fn exerciseCaptureAuthoritiesAllocationFailure(allocator: std.mem.Allocator) !void {
    var layout = policy.Layout{ .len = 3 };
    layout.layers[0] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 8, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    layout.layers[1] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 4, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    layout.layers[2] = .{ .owner = 2, .sliding_window = 0, .kv_heads = 1, .query_heads = 1, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    const fit = profiles.QueryStudyAuthority{ .template_sha256 = @splat(0x11), .generation_contract_sha256 = @splat(0x22), .response_tokens = 256, .generation_mode = .greedy_fixed_length_ignore_eos };
    const gate = profiles.QueryStudyAuthority{ .template_sha256 = @splat(0x33), .generation_contract_sha256 = @splat(0x44), .response_tokens = 256, .generation_mode = .greedy_fixed_length_ignore_eos };
    var authorities = try captureAuthorities(allocator, layout, fit, gate);
    authorities.deinit();
}

test "context query capture authorities roll back every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseCaptureAuthoritiesAllocationFailure, .{});
}

test "context query capture authorities reject invalid loaded layouts before allocation" {
    var layout = policy.Layout{ .len = 2 };
    layout.layers[0] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 8, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    layout.layers[1] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 1, .query_heads = 4, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    const study = profiles.QueryStudyAuthority{ .template_sha256 = @splat(0x11), .generation_contract_sha256 = @splat(0x22), .response_tokens = 256, .generation_mode = .greedy_fixed_length_ignore_eos };
    try std.testing.expectError(error.InvalidContextLayout, captureAuthorities(std.testing.allocator, layout, study, study));
    layout.layers[1].kv_heads = 2;
    layout.layers[1].owner = 2;
    try std.testing.expectError(error.InvalidContextLayout, captureAuthorities(std.testing.allocator, layout, study, study));
}

test "context query runtime protocol validates actual study bytes" {
    const template = "sealed study template";
    const contract = "{\"mode\":\"greedy_fixed_length_ignore_eos\",\"tokens\":64}";
    var study = profiles.QueryStudyAuthority{
        .template_sha256 = undefined,
        .generation_contract_sha256 = undefined,
        .response_tokens = 64,
        .generation_mode = .greedy_fixed_length_ignore_eos,
    };
    std.crypto.hash.sha2.Sha256.hash(template, &study.template_sha256, .{});
    std.crypto.hash.sha2.Sha256.hash(contract, &study.generation_contract_sha256, .{});
    try validateStudyBytes(study, template, contract);
    try std.testing.expectError(error.CompactionQueryProtocolMismatch, validateStudyBytes(study, "drift", contract));
    try std.testing.expectError(error.CompactionQueryProtocolMismatch, validateStudyBytes(study, template, "drift"));
}

pub const Plan = struct {
    sources: [max_sources]BoundSource,
    training_rows: u32,
    validation_rows: u32,
    training_per_source_stratum: u32,
    validation_per_source_stratum: u32,
    active_training_sources: u32,
    active_validation_sources: u32,
    strata: u32,

    pub fn init(input: []const Source, strata: u32) !Plan {
        if (input.len != max_sources or strata == 0 or strata > 128) return error.InvalidQueryProtocol;
        const expected = [_]Kind{ .request_question_replay, .fixed_study_prefill, .fixed_study_generated_response, .fixed_study_prefill, .fixed_study_generated_response };
        var train_population: u32 = max_training_rows;
        var held_population: u32 = max_validation_rows;
        var training_sources: u32 = 0;
        var validation_sources: u32 = 0;
        var ranges: [max_sources]sampling.Bounds = undefined;
        for (input, 0..) |source, index| {
            const expected_partition: Partition = if (index < 3) .training else .validation;
            if (source.kind != expected[index] or source.partition != expected_partition or source.start >= source.end)
                return error.InvalidQueryProtocol;
            const held = source.partition == .validation;
            const bounds = try sampling.Bounds.init(source.start, source.end, held);
            ranges[index] = bounds;
            const population = bounds.last_ordinal - bounds.first_ordinal + 1;
            if (held) {
                validation_sources += 1;
                held_population = @min(held_population, population);
            } else {
                training_sources += 1;
                train_population = @min(train_population, population);
            }
        }
        const mask = [max_sources]bool{ input[0].active, input[1].active, input[2].active, input[3].active, input[4].active };
        if (!(@import("std").mem.eql(bool, &mask, &.{ true, false, false, false, false }) or
            @import("std").mem.eql(bool, &mask, &.{ false, true, true, false, false }) or
            @import("std").mem.eql(bool, &mask, &.{ false, false, false, true, true })) or
            input[0].start != input[1].start or input[0].start != input[3].start or
            input[1].end != input[2].start or input[3].end != input[4].start or
            training_sources != 3 or validation_sources != 2)
            return error.InvalidQueryProtocol;
        const train = @min(train_population, max_training_rows / (strata * training_sources));
        const held = @min(held_population, max_validation_rows / (strata * validation_sources));
        if (train == 0 or held == 0) return error.InsufficientContextQueries;
        var result = Plan{ .sources = undefined, .training_rows = 0, .validation_rows = 0, .training_per_source_stratum = train, .validation_per_source_stratum = held, .active_training_sources = 0, .active_validation_sources = 0, .strata = strata };
        for (input, ranges, 0..) |source, bounds, index| {
            const active = switch (source.partition) {
                .training => &result.active_training_sources,
                .validation => &result.active_validation_sources,
            };
            const count = if (source.partition == .training) train else held;
            result.sources[index] = .{ .source = source, .bounds = bounds, .local_offset = active.* * count };
            if (source.active) active.* += 1;
        }
        result.training_rows = result.active_training_sources * strata * train;
        result.validation_rows = result.active_validation_sources * strata * held;
        if (result.training_rows > max_training_rows or result.validation_rows > max_validation_rows or
            result.training_rows + result.validation_rows == 0) return error.InvalidQueryProtocol;
        return result;
    }

    pub fn select(self: Plan, position: u32, stratum: u32) !?Selection {
        if (stratum >= self.strata) return error.InvalidQueryProtocol;
        for (self.sources) |entry| {
            const source = entry.source;
            if (!source.active or position < source.start or position >= source.end) continue;
            const partition = sampling.partition(position);
            if (partition.held != (source.partition == .validation)) return null;
            const count = if (source.partition == .training) self.training_per_source_stratum else self.validation_per_source_stratum;
            const slot = (try entry.bounds.slot(partition.ordinal, count)) orelse return null;
            const active = if (source.partition == .training) self.active_training_sources else self.active_validation_sources;
            return .{ .partition = source.partition, .row = stratum * active * count + entry.local_offset + slot };
        }
        return null;
    }
};

test "context query protocol quotas are global while branch allocation is local" {
    const sources = [_]Source{
        .{ .kind = .request_question_replay, .partition = .training, .start = 100, .end = 159, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .training, .start = 100, .end = 157, .active = true },
        .{ .kind = .fixed_study_generated_response, .partition = .training, .start = 157, .end = 413, .active = true },
        .{ .kind = .fixed_study_prefill, .partition = .validation, .start = 100, .end = 157, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .validation, .start = 157, .end = 413, .active = false },
    };
    const plan = try Plan.init(&sources, 40);
    try @import("std").testing.expectEqual(@as(u32, 25), plan.training_per_source_stratum);
    try @import("std").testing.expectEqual(@as(u32, 12), plan.validation_per_source_stratum);
    try @import("std").testing.expectEqual(@as(u32, 2000), plan.training_rows);
    try @import("std").testing.expectEqual(@as(u32, 0), plan.validation_rows);
    // Position 100 is the selected training endpoint of the active fit-prefill
    // source. Position 101 is the held member of that quartet and is therefore
    // absent from this training-only branch.
    try @import("std").testing.expect((try plan.select(101, 0)) == null);
    var seen: [2000]bool = @splat(false);
    for (0..plan.strata) |stratum| for (100..413) |position| {
        const selection = (try plan.select(@intCast(position), @intCast(stratum))) orelse continue;
        try @import("std").testing.expectEqual(Partition.training, selection.partition);
        try @import("std").testing.expect(selection.row < seen.len and !seen[selection.row]);
        seen[selection.row] = true;
    };
    for (seen) |selected| try @import("std").testing.expect(selected);
}

test "context query protocol validation branch fills bounded source slots exactly once" {
    const sources = [_]Source{
        .{ .kind = .request_question_replay, .partition = .training, .start = 100, .end = 159, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .training, .start = 100, .end = 157, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .training, .start = 157, .end = 413, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .validation, .start = 100, .end = 157, .active = true },
        .{ .kind = .fixed_study_generated_response, .partition = .validation, .start = 157, .end = 413, .active = true },
    };
    const plan = try Plan.init(&sources, 40);
    try @import("std").testing.expectEqual(@as(u32, 960), plan.validation_rows);
    var seen: [960]bool = @splat(false);
    for (0..plan.strata) |stratum| for (100..413) |position| {
        const selection = (try plan.select(@intCast(position), @intCast(stratum))) orelse continue;
        try @import("std").testing.expectEqual(Partition.validation, selection.partition);
        try @import("std").testing.expect(selection.row < seen.len and !seen[selection.row]);
        seen[selection.row] = true;
    };
    for (seen) |selected| try @import("std").testing.expect(selected);
}

test "context query protocol rejects source reordering and incomplete populations" {
    var sources = [_]Source{
        .{ .kind = .request_question_replay, .partition = .training, .start = 1, .end = 10, .active = true },
        .{ .kind = .fixed_study_prefill, .partition = .training, .start = 1, .end = 10, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .training, .start = 10, .end = 20, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .validation, .start = 1, .end = 10, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .validation, .start = 10, .end = 20, .active = false },
    };
    sources[0].kind = .fixed_study_prefill;
    try @import("std").testing.expectError(error.InvalidQueryProtocol, Plan.init(&sources, 1));
    sources[0].kind = .request_question_replay;
    sources[0].partition = .validation;
    try @import("std").testing.expectError(error.InvalidQueryProtocol, Plan.init(&sources, 1));
    sources[0].partition = .training;
    sources[1].active = true;
    try @import("std").testing.expectError(error.InvalidQueryProtocol, Plan.init(&sources, 1));
    sources[1].active = false;
    sources[2].start += 1;
    try @import("std").testing.expectError(error.InvalidQueryProtocol, Plan.init(&sources, 1));
}
