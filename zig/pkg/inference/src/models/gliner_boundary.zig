// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Typed GLiNER2.5 boundary architecture contract. The encoder's relative
//! position table length and the extractor's input limit are independent.
//! Parsing this contract does not imply that a serving backend is qualified.

const std = @import("std");
const deberta = @import("deberta.zig");

pub const Architecture = enum { unknown, span, boundary };
pub const config_version: u32 = 3;
pub const architecture_version: u32 = 1;
pub const model_type = "gliner2.5";

/// Enable task advertisement only alongside a qualified boundary runtime.
pub const runtime_available = false;

pub const Backbone = enum {
    base,
    small,
    multi,

    pub fn modelName(self: Backbone) []const u8 {
        return switch (self) {
            .base => "microsoft/deberta-v3-base",
            .small => "microsoft/deberta-v3-xsmall",
            .multi => "microsoft/mdeberta-v3-base",
        };
    }
};

pub const CandidatePool = enum { per_query, shared };
pub const ExportMode = enum { auto, streaming, vectorized };
pub const MarginalLoss = enum { bce, asymmetric_focal };
pub const LossReduction = enum { global, per_query, sum };
pub const OverlapPolicy = enum { flat, nested, longest };

/// Defaults match the pinned upstream BoundaryHeadSettings for config v3.
/// Unknown keys are rejected: a new semantic switch must not be ignored.
pub const HeadConfig = struct {
    boundary_dim: u32 = 128,
    pair_dim: u32 = 128,
    boundary_refinement_layers: u32 = 1,
    boundary_ffn_multiplier: f32 = 2.0,
    start_top_k: u32 = 16,
    end_top_k: u32 = 16,
    ends_per_start: u32 = 8,
    starts_per_end: u32 = 8,
    candidate_budget: u32 = 128,
    training_candidate_budget: u32 = 160,
    max_gold_per_query: u32 = 32,
    end_block_size: u32 = 256,
    bidirectional_proposals: bool = true,
    use_inside_evidence: bool = true,
    dropout: f32 = 0.1,
    export_mode: ExportMode = .auto,
    vectorized_pair_elements: u32 = 16_777_216,
    boundary_negative_weight: f32 = 1.0,
    boundary_marginal_loss: MarginalLoss = .bce,
    loss_reduction: LossReduction = .global,
    boundary_focal_gamma_positive: f32 = 0.0,
    boundary_focal_gamma_negative: f32 = 2.0,
    boundary_focal_clip: f32 = 0.05,
    hard_negatives_per_positive: u32 = 5,
    minimum_hard_negatives: u32 = 8,
    hard_negative_keep_all_when_absent: bool = false,
    enable_span_content: bool = false,
    content_dim: u32 = 64,
    content_soft_max_pool: bool = false,
    enable_rotary_endpoints: bool = false,
    rotary_base: f32 = 10000.0,
    boundary_attention_layers: u32 = 0,
    boundary_attention_heads: u32 = 4,
    boundary_attention_window: u32 = 0,
    query_conditioned_inside_weight: bool = false,
    endpoint_difference_features: bool = false,
    reranker_endpoint_compat: bool = true,
    multihead_pair_compat_heads: u32 = 8,
    boundary_top_k_alpha: f32 = 0.0,
    boundary_top_k_max: u32 = 128,
    boundary_top_k_bucket: u32 = 8,
    candidate_pool: CandidatePool = .per_query,
    pool_boundary_top_k: u32 = 64,
    pool_size: u32 = 384,
    min_pool_per_query: u32 = 8,
    candidate_attention_layers: u32 = 2,
    candidate_attention_heads: u32 = 4,
    query_attention_layers: u32 = 1,
    enable_abstention: bool = true,
    abstention_threshold: f32 = 0.5,
    proposal_loss_weight: f32 = 0.3,
    consistency_loss_weight: f32 = 0.1,
    rerank_listwise_weight: f32 = 0.3,
    soft_iou_aux_weight: f32 = 0.2,
    soft_iou_anneal_steps: u32 = 20_000,
    abstention_loss_weight: f32 = 0.2,
    consistency_warmup_steps: u32 = 2000,
    enable_count_head: bool = true,
    count_loss_weight: f32 = 0.2,
    adaptive_threshold: bool = false,
    overlap_policy: OverlapPolicy = .flat,
    pair_temperature: f32 = 1.0,
    relation_temperature: f32 = 1.0,
    record_temperature: f32 = 1.0,
    classification_temperature: f32 = 1.0,
    negative_query_ratio: f32 = 0.5,
    max_negative_queries_per_batch: u32 = 64,
    classification_loss_weight: f32 = 1.0,
    enable_records: bool = true,
    record_dim: u32 = 128,
    record_instance_queries: u32 = 32,
    record_anchor_proposal_threshold: f32 = 0.2,
    record_anchor_threshold: f32 = 0.5,
    record_field_threshold: f32 = 0.5,
    record_loss_weight: f32 = 1.0,
    enable_relations: bool = true,
    relation_heads_per_type: u32 = 32,
    relation_tails_per_type: u32 = 32,
    relation_pair_cap: u32 = 128,
    relation_loss_weight: f32 = 1.0,
    relation_argument_proposal_threshold: f32 = 0.0,
    directional_relation_states: bool = false,
    relation_biaffine_content: bool = false,

    pub fn validate(self: HeadConfig) !void {
        inline for (@typeInfo(HeadConfig).@"struct".fields) |field| {
            const value = @field(self, field.name);
            if (field.type == f32) {
                if (!std.math.isFinite(value) or value < 0) return error.InvalidGlinerBoundaryConfig;
            } else if (field.type == u32) {
                if (value > std.math.maxInt(i32)) return error.InvalidGlinerBoundaryConfig;
            }
        }
        const positive = [_]u32{
            self.boundary_dim,             self.pair_dim,                    self.start_top_k,               self.end_top_k,
            self.ends_per_start,           self.starts_per_end,              self.candidate_budget,          self.training_candidate_budget,
            self.max_gold_per_query,       self.end_block_size,              self.vectorized_pair_elements,  self.content_dim,
            self.boundary_attention_heads, self.multihead_pair_compat_heads, self.boundary_top_k_max,        self.boundary_top_k_bucket,
            self.pool_boundary_top_k,      self.pool_size,                   self.candidate_attention_heads, self.record_dim,
            self.record_instance_queries,  self.relation_heads_per_type,     self.relation_tails_per_type,   self.relation_pair_cap,
        };
        for (positive) |value| if (value == 0) return error.InvalidGlinerBoundaryConfig;
        if (self.dropout >= 1 or self.boundary_focal_clip >= 1 or
            self.abstention_threshold > 1 or self.record_anchor_proposal_threshold > 1 or
            self.record_anchor_threshold > 1 or self.record_field_threshold > 1 or
            self.relation_argument_proposal_threshold > 1 or
            self.boundary_ffn_multiplier == 0 or self.rotary_base == 0 or
            self.pair_temperature == 0 or self.relation_temperature == 0 or
            self.record_temperature == 0 or self.classification_temperature == 0)
            return error.InvalidGlinerBoundaryConfig;
        if (self.boundary_dim % self.boundary_attention_heads != 0 or
            self.pair_dim % self.candidate_attention_heads != 0 or
            self.pair_dim % self.multihead_pair_compat_heads != 0 or
            (self.enable_rotary_endpoints and (self.pair_dim % 2 != 0 or self.boundary_dim % 2 != 0)) or
            self.min_pool_per_query > self.pool_size)
            return error.InvalidGlinerBoundaryConfig;
        const ffn_dim = @as(f64, @floatFromInt(self.boundary_dim)) * self.boundary_ffn_multiplier;
        if (ffn_dim < 1 or ffn_dim > std.math.maxInt(i32)) return error.InvalidGlinerBoundaryConfig;
    }
};

pub const EncoderConfig = struct {
    hidden_size: u32,
    intermediate_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    vocab_size: u32,
    max_position_embeddings: u32,
    position_buckets: u32,
    layer_norm_eps: f32,
    hidden_dropout_prob: f32,
    attention_probs_dropout_prob: f32,
    pad_token_id: u32,

    pub fn toDeberta(self: EncoderConfig) deberta.Config {
        return .{
            .hidden_size = self.hidden_size,
            .intermediate_size = self.intermediate_size,
            .num_hidden_layers = self.num_hidden_layers,
            .num_attention_heads = self.num_attention_heads,
            .vocab_size = self.vocab_size,
            .max_position_embeddings = self.max_position_embeddings,
            .position_buckets = self.position_buckets,
            .layer_norm_eps = self.layer_norm_eps,
            .use_exact_gelu = true,
        };
    }
};

pub const Config = struct {
    version: u32,
    architecture_version: u32,
    max_len: u32,
    backbone: Backbone,
    head: HeadConfig,
    encoder: EncoderConfig,
};

/// Detect from executable architecture metadata, never from repository names.
pub fn detectArchitecture(allocator: std.mem.Allocator, bytes: []const u8) !Architecture {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{}) catch |err| {
        if (err == error.OutOfMemory) return err;
        // Unrelated upstream metadata historically remains best-effort. A
        // malformed file carrying a boundary marker must still fail closed;
        // these strings only reject metadata and never select an architecture.
        for ([_][]const u8{ "\"boundary_head\"", "\"BoundaryExtractor\"", "\"boundary\"", "\"gliner2.5\"" }) |marker| {
            if (std.mem.indexOf(u8, bytes, marker) != null) return error.InvalidGlinerBoundaryConfig;
        }
        return .unknown;
    };
    defer parsed.deinit();
    if (parsed.value != .object) return .unknown;
    return detectArchitectureObject(parsed.value.object);
}

fn detectArchitectureObject(obj: std.json.ObjectMap) !Architecture {
    const model = obj.get("model_type");
    const is_extractor = model != null and model.? == .string and std.mem.eql(u8, model.?.string, "extractor");
    var boundary_marker = obj.contains("boundary_head") or
        (model != null and model.? == .string and std.mem.eql(u8, model.?.string, model_type));
    if (obj.get("architectures")) |architectures| {
        if (architectures == .array) for (architectures.array.items) |value| {
            if (value == .string and std.mem.eql(u8, value.string, "BoundaryExtractor")) boundary_marker = true;
        };
    }
    if (obj.get("architecture")) |value| {
        if (value == .string and std.mem.eql(u8, value.string, "boundary")) return .boundary;
        if (is_extractor or boundary_marker) {
            if (value != .string) return error.InvalidGlinerBoundaryConfig;
            if (!boundary_marker and std.mem.eql(u8, value.string, "span")) return .span;
            return error.UnsupportedGlinerArchitecture;
        }
    }
    if (boundary_marker) return error.InvalidGlinerBoundaryConfig;
    if (is_extractor) if (obj.get("config_version")) |version| {
        if (version == .integer and version.integer >= config_version) return error.InvalidGlinerBoundaryConfig;
    };
    return if (is_extractor) .span else .unknown;
}

pub fn parseConfig(allocator: std.mem.Allocator, bytes: []const u8, encoder_bytes: []const u8) !Config {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{}) catch |err| return configError(err);
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGlinerBoundaryConfig;
    const obj = parsed.value.object;
    if (try detectArchitectureObject(obj) != .boundary) return error.UnsupportedGlinerArchitecture;
    try requireString(obj, "model_type", "extractor");
    try requireString(obj, "token_pooling", "first");
    const version = try requiredU32(obj, "config_version", false);
    const arch_version = try requiredU32(obj, "architecture_version", false);
    if (version != config_version or arch_version != architecture_version) return error.UnsupportedGlinerBoundaryVersion;
    const max_len = try requiredU32(obj, "max_len", false);
    const model_name = obj.get("model_name") orelse return error.InvalidGlinerBoundaryConfig;
    if (model_name != .string) return error.InvalidGlinerBoundaryConfig;
    const backbone: Backbone = blk: {
        inline for (std.meta.fields(Backbone)) |field| {
            const candidate: Backbone = @enumFromInt(field.value);
            if (std.mem.eql(u8, model_name.string, candidate.modelName())) break :blk candidate;
        }
        return error.UnsupportedGlinerBoundaryEncoder;
    };
    const head_value = obj.get("boundary_head") orelse return error.InvalidGlinerBoundaryConfig;
    if (head_value != .object) return error.InvalidGlinerBoundaryConfig;
    const head = try parseHeadConfig(head_value.object);
    try head.validate();
    const encoder = try parseEncoderConfig(allocator, encoder_bytes, backbone);
    return .{ .version = version, .architecture_version = arch_version, .max_len = max_len, .backbone = backbone, .head = head, .encoder = encoder };
}

pub fn parseHeadConfig(obj: std.json.ObjectMap) !HeadConfig {
    var head = HeadConfig{};
    var iterator = obj.iterator();
    while (iterator.next()) |entry| {
        var known = false;
        inline for (@typeInfo(HeadConfig).@"struct".fields) |field| {
            if (std.mem.eql(u8, entry.key_ptr.*, field.name)) known = true;
        }
        if (!known) return error.UnsupportedGlinerBoundaryConfiguration;
    }
    inline for (@typeInfo(HeadConfig).@"struct".fields) |field| {
        if (obj.get(field.name)) |value| {
            if (field.type == u32) {
                @field(head, field.name) = try requiredU32(obj, field.name, true);
            } else if (field.type == f32) {
                @field(head, field.name) = try requiredF32(obj, field.name);
            } else if (field.type == bool) {
                if (value != .bool) return error.InvalidGlinerBoundaryConfig;
                @field(head, field.name) = value.bool;
            } else {
                if (value != .string) return error.InvalidGlinerBoundaryConfig;
                @field(head, field.name) = std.meta.stringToEnum(field.type, value.string) orelse
                    return error.UnsupportedGlinerBoundaryConfiguration;
            }
        }
    }
    try head.validate();
    return head;
}

fn parseEncoderConfig(allocator: std.mem.Allocator, bytes: []const u8, backbone: Backbone) !EncoderConfig {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{}) catch |err| return configError(err);
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGlinerBoundaryConfig;
    const obj = parsed.value.object;
    try requireString(obj, "model_type", "deberta-v2");
    try requireString(obj, "hidden_act", "gelu");
    try requireString(obj, "norm_rel_ebd", "layer_norm");
    try requireBool(obj, "relative_attention", true);
    try requireBool(obj, "share_att_key", true);
    try requireBool(obj, "position_biased_input", false);
    const relative_positions = obj.get("max_relative_positions") orelse return error.InvalidGlinerBoundaryConfig;
    if (relative_positions != .integer or relative_positions.integer != -1) return error.UnsupportedGlinerBoundaryEncoder;
    if (try requiredU32(obj, "type_vocab_size", true) != 0) return error.UnsupportedGlinerBoundaryEncoder;
    const pos_att_type = obj.get("pos_att_type") orelse return error.InvalidGlinerBoundaryConfig;
    if (pos_att_type != .array or pos_att_type.array.items.len != 2) return error.UnsupportedGlinerBoundaryEncoder;
    var has_p2c = false;
    var has_c2p = false;
    for (pos_att_type.array.items) |value| {
        if (value != .string) return error.InvalidGlinerBoundaryConfig;
        if (std.mem.eql(u8, value.string, "p2c")) has_p2c = true;
        if (std.mem.eql(u8, value.string, "c2p")) has_c2p = true;
    }
    if (!has_p2c or !has_c2p) return error.UnsupportedGlinerBoundaryEncoder;
    const result = EncoderConfig{
        .hidden_size = try requiredU32(obj, "hidden_size", false),
        .intermediate_size = try requiredU32(obj, "intermediate_size", false),
        .num_hidden_layers = try requiredU32(obj, "num_hidden_layers", false),
        .num_attention_heads = try requiredU32(obj, "num_attention_heads", false),
        .vocab_size = try requiredU32(obj, "vocab_size", false),
        .max_position_embeddings = try requiredU32(obj, "max_position_embeddings", false),
        .position_buckets = try requiredU32(obj, "position_buckets", false),
        .layer_norm_eps = try requiredF32(obj, "layer_norm_eps"),
        .hidden_dropout_prob = try requiredF32(obj, "hidden_dropout_prob"),
        .attention_probs_dropout_prob = try requiredF32(obj, "attention_probs_dropout_prob"),
        .pad_token_id = try requiredU32(obj, "pad_token_id", true),
    };
    if (result.hidden_size % result.num_attention_heads != 0 or result.pad_token_id >= result.vocab_size or
        result.layer_norm_eps <= 0 or result.hidden_dropout_prob >= 1 or result.attention_probs_dropout_prob >= 1)
        return error.InvalidGlinerBoundaryConfig;
    const expected_hidden: u32 = if (backbone == .small) 384 else 768;
    const expected_vocab: u32 = if (backbone == .multi) 250112 else 128011;
    if (result.hidden_size != expected_hidden or result.intermediate_size != expected_hidden * 4 or
        result.num_attention_heads != expected_hidden / 64 or result.num_hidden_layers != 12 or
        result.vocab_size != expected_vocab or result.max_position_embeddings != 512 or result.position_buckets != 256)
        return error.UnsupportedGlinerBoundaryEncoder;
    return result;
}

fn requiredU32(obj: std.json.ObjectMap, name: []const u8, allow_zero: bool) !u32 {
    const value = obj.get(name) orelse return error.InvalidGlinerBoundaryConfig;
    if (value != .integer or value.integer < 0) return error.InvalidGlinerBoundaryConfig;
    const result = std.math.cast(u32, value.integer) orelse return error.InvalidGlinerBoundaryConfig;
    if ((!allow_zero and result == 0) or result > std.math.maxInt(i32)) return error.InvalidGlinerBoundaryConfig;
    return result;
}

fn requiredF32(obj: std.json.ObjectMap, name: []const u8) !f32 {
    const value = obj.get(name) orelse return error.InvalidGlinerBoundaryConfig;
    const result: f32 = switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => @floatCast(value.float),
        else => return error.InvalidGlinerBoundaryConfig,
    };
    if (!std.math.isFinite(result) or result < 0) return error.InvalidGlinerBoundaryConfig;
    return result;
}

fn requireString(obj: std.json.ObjectMap, name: []const u8, expected: []const u8) !void {
    const value = obj.get(name) orelse return error.InvalidGlinerBoundaryConfig;
    if (value != .string) return error.InvalidGlinerBoundaryConfig;
    if (!std.mem.eql(u8, value.string, expected)) return error.UnsupportedGlinerBoundaryConfiguration;
}

fn requireBool(obj: std.json.ObjectMap, name: []const u8, expected: bool) !void {
    const value = obj.get(name) orelse return error.InvalidGlinerBoundaryConfig;
    if (value != .bool) return error.InvalidGlinerBoundaryConfig;
    if (value.bool != expected) return error.UnsupportedGlinerBoundaryEncoder;
}

fn configError(err: anyerror) anyerror {
    return if (err == error.OutOfMemory) err else error.InvalidGlinerBoundaryConfig;
}

fn loadFixture(allocator: std.mem.Allocator, backbone: Backbone, name: []const u8) ![]u8 {
    const c_file = @import("../util/c_file.zig");
    for ([_][]const u8{ "", "pkg/inference/", "zig/pkg/inference/" }) |prefix| {
        const path = try std.fmt.allocPrint(allocator, "{s}testdata/gliner25/models/{s}/{s}", .{ prefix, @tagName(backbone), name });
        defer allocator.free(path);
        const bytes = c_file.readFile(allocator, path) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        return bytes;
    }
    return error.FileNotFound;
}

test "gliner boundary parses the three pinned release configurations" {
    const fixtures = .{
        .{ Backbone.base, @as(u32, 768), @as(u32, 128011) },
        .{ Backbone.small, @as(u32, 384), @as(u32, 128011) },
        .{ Backbone.multi, @as(u32, 768), @as(u32, 250112) },
    };
    inline for (fixtures) |fixture| {
        const bytes = try loadFixture(std.testing.allocator, fixture[0], "config.json");
        defer std.testing.allocator.free(bytes);
        const encoder_bytes = try loadFixture(std.testing.allocator, fixture[0], "encoder_config.json");
        defer std.testing.allocator.free(encoder_bytes);
        const config = try parseConfig(std.testing.allocator, bytes, encoder_bytes);
        try std.testing.expectEqual(fixture[0], config.backbone);
        try std.testing.expectEqual(fixture[1], config.encoder.hidden_size);
        try std.testing.expectEqual(fixture[2], config.encoder.vocab_size);
        try std.testing.expectEqual(@as(u32, 4096), config.max_len);
        try std.testing.expectEqual(@as(u32, 512), config.encoder.max_position_embeddings);
        try std.testing.expect(config.encoder.toDeberta().use_exact_gelu);
        try std.testing.expectEqual(CandidatePool.shared, config.head.candidate_pool);
        try std.testing.expectEqual(@as(u32, 192), config.head.pool_size);
        try std.testing.expect(config.head.enable_records and config.head.enable_relations);
    }
}

test "gliner boundary rejects malformed values versions and unknown architecture switches" {
    const base_config = try loadFixture(std.testing.allocator, .base, "config.json");
    defer std.testing.allocator.free(base_config);
    const base_encoder = try loadFixture(std.testing.allocator, .base, "encoder_config.json");
    defer std.testing.allocator.free(base_encoder);
    const replacements = .{
        .{ "\"config_version\": 3", "\"config_version\": 4", error.UnsupportedGlinerBoundaryVersion },
        .{ "\"architecture_version\": 1", "\"architecture_version\": 2", error.UnsupportedGlinerBoundaryVersion },
        .{ "\"max_len\": 4096", "\"max_len\": -1", error.InvalidGlinerBoundaryConfig },
        .{ "\"max_len\": 4096", "\"max_len\": 4294967296", error.InvalidGlinerBoundaryConfig },
        .{ "\"candidate_budget\": 192", "\"candidate_budget\": 0", error.InvalidGlinerBoundaryConfig },
        .{ "\"candidate_budget\": 192", "\"candidate_budget\": -1", error.InvalidGlinerBoundaryConfig },
        .{ "\"dropout\": 0.1", "\"dropout\": 1.0", error.InvalidGlinerBoundaryConfig },
        .{ "\"pair_temperature\": 1.0", "\"pair_temperature\": 0.0", error.InvalidGlinerBoundaryConfig },
        .{ "\"enable_records\": true", "\"enable_records\": 1", error.InvalidGlinerBoundaryConfig },
        .{ "\"content_dim\": 64", "\"future_content_dim\": 64", error.UnsupportedGlinerBoundaryConfiguration },
        .{ "\"candidate_budget\": 192", "\"candidate_budget\": 4294967296", error.InvalidGlinerBoundaryConfig },
        .{ "\"candidate_budget\": 192", "\"candidate_budget\": 192.5", error.InvalidGlinerBoundaryConfig },
        .{ "\"candidate_pool\": \"shared\"", "\"candidate_pool\": \"future\"", error.UnsupportedGlinerBoundaryConfiguration },
    };
    inline for (replacements) |replacement| {
        const bytes = try std.mem.replaceOwned(u8, std.testing.allocator, base_config, replacement[0], replacement[1]);
        defer std.testing.allocator.free(bytes);
        try std.testing.expectError(replacement[2], parseConfig(std.testing.allocator, bytes, base_encoder));
    }
    try std.testing.expectEqual(Architecture.span, try detectArchitecture(std.testing.allocator, "{\"model_type\":\"extractor\"}"));
    try std.testing.expectEqual(Architecture.unknown, try detectArchitecture(std.testing.allocator, "{\"model_type\":\"bert\"}"));
    try std.testing.expectError(error.InvalidGlinerBoundaryConfig, detectArchitecture(std.testing.allocator, "{\"boundary_head\":{}}"));
    try std.testing.expectError(error.UnsupportedGlinerArchitecture, detectArchitecture(std.testing.allocator, "{\"model_type\":\"extractor\",\"architecture\":\"future\"}"));
    try std.testing.expectError(error.InvalidGlinerBoundaryConfig, detectArchitecture(std.testing.allocator, "{\"architecture\":\"boundary\","));
    try std.testing.expectError(error.InvalidGlinerBoundaryConfig, parseConfig(std.testing.allocator, base_config, "[]"));
}

test "gliner boundary parsing propagates and cleans up every allocation failure" {
    const base_config = try loadFixture(std.testing.allocator, .base, "config.json");
    defer std.testing.allocator.free(base_config);
    const base_encoder = try loadFixture(std.testing.allocator, .base, "encoder_config.json");
    defer std.testing.allocator.free(base_encoder);
    const Check = struct {
        fn run(allocator: std.mem.Allocator, bytes: []const u8, encoder_bytes: []const u8) !void {
            const config = try parseConfig(allocator, bytes, encoder_bytes);
            try std.testing.expectEqual(@as(u32, 768), config.encoder.hidden_size);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{ base_config, base_encoder });
}

test "gliner boundary encoder rejects negative overflowing and incompatible dimensions" {
    const base_config = try loadFixture(std.testing.allocator, .base, "config.json");
    defer std.testing.allocator.free(base_config);
    const base_encoder = try loadFixture(std.testing.allocator, .base, "encoder_config.json");
    defer std.testing.allocator.free(base_encoder);
    const replacements = .{
        .{ "\"hidden_size\": 768", "\"hidden_size\": -1", error.InvalidGlinerBoundaryConfig },
        .{ "\"vocab_size\": 128011", "\"vocab_size\": 4294967296", error.InvalidGlinerBoundaryConfig },
        .{ "\"num_attention_heads\": 12", "\"num_attention_heads\": 0", error.InvalidGlinerBoundaryConfig },
        .{ "\"hidden_size\": 768", "\"hidden_size\": 384", error.UnsupportedGlinerBoundaryEncoder },
        .{ "\"share_att_key\": true", "\"share_att_key\": false", error.UnsupportedGlinerBoundaryEncoder },
        .{ "\"hidden_dropout_prob\": 0.1", "\"hidden_dropout_prob\": 1.0", error.InvalidGlinerBoundaryConfig },
    };
    inline for (replacements) |replacement| {
        try std.testing.expect(std.mem.indexOf(u8, base_encoder, replacement[0]) != null);
        const bytes = try std.mem.replaceOwned(u8, std.testing.allocator, base_encoder, replacement[0], replacement[1]);
        defer std.testing.allocator.free(bytes);
        try std.testing.expectError(replacement[2], parseConfig(std.testing.allocator, base_config, bytes));
    }
}
