// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Legacy checkpoint adapter. New loaders supply Workload metadata directly.
const std = @import("std");
const Workload = @import("../ops/dense_projection_policy.zig").Workload;
fn any(comptime mode: enum { prefix, suffix, exact }, name: []const u8, comptime patterns: []const []const u8) bool {
    inline for (patterns) |pattern| if (switch (mode) {
        .prefix => std.mem.startsWith(u8, name, pattern),
        .suffix => std.mem.endsWith(u8, name, pattern),
        .exact => std.mem.eql(u8, name, pattern),
    }) return true;
    return false;
}
pub fn classify(name: []const u8) Workload {
    if (any(.prefix, name, &.{ "language_model.model.encoder.", "language_model.model.decoder.", "vision_tower.blocks." }) or
        any(.exact, name, &.{ "language_model.model.shared.weight", "language_model.lm_head.weight", "image_projection" })) return .reused;
    if (any(.prefix, name, &.{ "span_rep.", "count_embed." })) return .compact_head;
    if (std.mem.startsWith(u8, name, "encoder.layer.") and any(.suffix, name, &.{
        ".attention.self.query_proj.weight", ".attention.self.key_proj.weight", ".attention.self.value_proj.weight",
        ".attention.output.dense.weight",    ".intermediate.dense.weight",      ".output.dense.weight",
    })) return .encoder;
    if (any(.prefix, name, &.{ "text_projection.", "visual_projection.", "audio_projection.", "clap.text_projection.", "clap.audio_projection." }) or
        any(.exact, name, &.{ "clip.text_projection.weight", "clip.visual_projection.weight" })) return .optional_head;
    if (any(.prefix, name, &.{ "text_model.", "vision_model.", "audio_model." }) and any(.suffix, name, &.{
        ".self_attn.q_proj.weight",     ".self_attn.k_proj.weight",   ".self_attn.v_proj.weight",     ".self_attn.out_proj.weight",
        ".attention.self.query.weight", ".attention.self.key.weight", ".attention.self.value.weight", ".attention.output.dense.weight",
        ".mlp.fc1.weight",              ".mlp.fc2.weight",            ".intermediate.dense.weight",   ".output.dense.weight",
    })) return .optional_encoder;
    return .direct;
}
/// Unknown source namespaces leave canonical operation hints available.
pub fn registered(name: []const u8) ?Workload {
    const workload = classify(name);
    return if (workload == .direct) null else workload;
}
