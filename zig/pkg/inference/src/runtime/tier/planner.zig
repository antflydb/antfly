// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

pub const BackendClass = enum {
    cpu,
    gpu,
};

pub const ResidencyTier = enum {
    disk,
    host,
    backend,
};

pub const WeightClass = enum {
    embedding,
    norm,
    attention,
    router,
    output,
    expert,
    other,
};

pub const PlacementPlan = struct {
    class: WeightClass,
    preferred_tier: ResidencyTier,
    spill_tier: ResidencyTier,
};

pub const PlanContext = struct {
    backend: BackendClass,
    host_budget_bytes: usize = 0,
    backend_budget_bytes: usize = 0,
};

pub fn classifyWeight(name: []const u8) WeightClass {
    if (std.mem.indexOf(u8, name, ".block_sparse_moe.experts.") != null) return .expert;
    if (std.mem.endsWith(u8, name, ".block_sparse_moe.gate.weight")) return .router;
    if (std.mem.eql(u8, name, "model.embed_tokens.weight") or std.mem.eql(u8, name, "wte.weight")) return .embedding;
    if (std.mem.eql(u8, name, "lm_head.weight") or std.mem.eql(u8, name, "output.weight")) return .output;
    if (std.mem.indexOf(u8, name, ".self_attn.") != null or std.mem.indexOf(u8, name, ".attn.") != null) return .attention;
    if (std.mem.indexOf(u8, name, "layernorm") != null or std.mem.indexOf(u8, name, "layer_norm") != null or std.mem.indexOf(u8, name, ".norm.") != null or std.mem.endsWith(u8, name, ".norm.weight")) return .norm;
    return .other;
}

pub fn planForBackend(backend: BackendClass, name: []const u8) PlacementPlan {
    const class = classifyWeight(name);
    return switch (class) {
        .embedding, .norm, .attention, .router => .{
            .class = class,
            .preferred_tier = collapsePreferredTier(backend, .backend),
            .spill_tier = collapsePreferredTier(backend, .host),
        },
        .output, .other => .{
            .class = class,
            .preferred_tier = collapsePreferredTier(backend, .host),
            .spill_tier = .disk,
        },
        .expert => .{
            .class = .expert,
            .preferred_tier = collapsePreferredTier(backend, .backend),
            .spill_tier = switch (backend) {
                .cpu => .disk,
                .gpu => .host,
            },
        },
    };
}

pub fn planForContext(ctx: PlanContext, name: []const u8, tensor_bytes: usize) PlacementPlan {
    var plan = planForBackend(ctx.backend, name);
    if (tensor_bytes == 0) return plan;

    if (ctx.host_budget_bytes != 0 and tensor_bytes > ctx.host_budget_bytes / 4) {
        switch (plan.class) {
            .output, .other => {
                plan.preferred_tier = .disk;
                plan.spill_tier = .disk;
            },
            .expert => {
                plan.spill_tier = .disk;
            },
            else => {},
        }
    }

    if (ctx.backend_budget_bytes != 0 and tensor_bytes > ctx.backend_budget_bytes / 4) {
        switch (plan.class) {
            .expert => {
                plan.preferred_tier = collapsePreferredTier(ctx.backend, .host);
                plan.spill_tier = .disk;
            },
            .output, .other => {
                if (plan.preferred_tier != .disk) {
                    plan.preferred_tier = collapsePreferredTier(ctx.backend, .host);
                    plan.spill_tier = .disk;
                }
            },
            else => {},
        }
    }

    return plan;
}

fn collapsePreferredTier(backend: BackendClass, tier: ResidencyTier) ResidencyTier {
    return switch (backend) {
        .cpu => switch (tier) {
            .backend => .host,
            else => tier,
        },
        .gpu => tier,
    };
}

test "planner classifies mixtral roles" {
    try std.testing.expectEqual(WeightClass.expert, classifyWeight("model.layers.0.block_sparse_moe.experts.1.w1.weight"));
    try std.testing.expectEqual(WeightClass.router, classifyWeight("model.layers.0.block_sparse_moe.gate.weight"));
    try std.testing.expectEqual(WeightClass.attention, classifyWeight("model.layers.0.self_attn.q_proj.weight"));
    try std.testing.expectEqual(WeightClass.embedding, classifyWeight("model.embed_tokens.weight"));
}

test "planner uses host spill for gpu experts and disk spill for cpu experts" {
    const gpu_plan = planForBackend(.gpu, "model.layers.0.block_sparse_moe.experts.1.w1.weight");
    try std.testing.expectEqual(ResidencyTier.backend, gpu_plan.preferred_tier);
    try std.testing.expectEqual(ResidencyTier.host, gpu_plan.spill_tier);

    const cpu_plan = planForBackend(.cpu, "model.layers.0.block_sparse_moe.experts.1.w1.weight");
    try std.testing.expectEqual(ResidencyTier.host, cpu_plan.preferred_tier);
    try std.testing.expectEqual(ResidencyTier.disk, cpu_plan.spill_tier);
}

test "planner prefers disk for large cold tensors under tight budgets" {
    const ctx: PlanContext = .{
        .backend = .gpu,
        .host_budget_bytes = 256 * 1024 * 1024,
        .backend_budget_bytes = 256 * 1024 * 1024,
    };
    const plan = planForContext(ctx, "lm_head.weight", 128 * 1024 * 1024);
    try std.testing.expectEqual(ResidencyTier.disk, plan.preferred_tier);
    try std.testing.expectEqual(ResidencyTier.disk, plan.spill_tier);
}
