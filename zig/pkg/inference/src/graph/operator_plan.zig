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
const quant_matmul = @import("quant_matmul.zig");

pub const Operator = quant_matmul.Operator;
pub const QuantMatmulPlan = quant_matmul.Plan;
pub const QuantRowOpPlan = quant_matmul.RowOpPlan;
pub const QuantCopyOpPlan = quant_matmul.CopyOpPlan;
pub const AttentionOpPlan = quant_matmul.AttentionPlan;

pub const OperatorPlan = union(enum) {
    quant_matmul: QuantMatmulPlan,
    quant_row: QuantRowOpPlan,
    quant_copy: QuantCopyOpPlan,
    attention: AttentionOpPlan,

    pub fn operator(self: OperatorPlan) Operator {
        return switch (self) {
            .quant_matmul => |plan| plan.operator,
            .quant_row => |plan| plan.operator,
            .quant_copy => |plan| plan.operator,
            .attention => |plan| plan.operator,
        };
    }
};

pub const Stats = struct {
    total: usize = 0,
    fallback: usize = 0,
    mul_mv: usize = 0,
    mul_mv_ext: usize = 0,
    mul_mm: usize = 0,
    get_rows: usize = 0,
    set_rows: usize = 0,
    cpy_q_to_f32: usize = 0,
    cpy_f32_to_q: usize = 0,
    attention_flash: usize = 0,
    attention_paged: usize = 0,
    attention_quantized_kv: usize = 0,

    pub fn add(self: *Stats, operator: Operator) void {
        self.total += 1;
        switch (operator) {
            .fallback => self.fallback += 1,
            .mul_mv => self.mul_mv += 1,
            .mul_mv_ext => self.mul_mv_ext += 1,
            .mul_mm => self.mul_mm += 1,
            .get_rows => self.get_rows += 1,
            .set_rows => self.set_rows += 1,
            .cpy_q_to_f32 => self.cpy_q_to_f32 += 1,
            .cpy_f32_to_q => self.cpy_f32_to_q += 1,
            .attention_flash => self.attention_flash += 1,
            .attention_paged => self.attention_paged += 1,
            .attention_quantized_kv => self.attention_quantized_kv += 1,
        }
    }

    pub fn count(self: Stats, operator: Operator) usize {
        return switch (operator) {
            .fallback => self.fallback,
            .mul_mv => self.mul_mv,
            .mul_mv_ext => self.mul_mv_ext,
            .mul_mm => self.mul_mm,
            .get_rows => self.get_rows,
            .set_rows => self.set_rows,
            .cpy_q_to_f32 => self.cpy_q_to_f32,
            .cpy_f32_to_q => self.cpy_f32_to_q,
            .attention_flash => self.attention_flash,
            .attention_paged => self.attention_paged,
            .attention_quantized_kv => self.attention_quantized_kv,
        };
    }

    pub fn hasFallback(self: Stats) bool {
        return self.fallback != 0;
    }
};

test "operator plan reports concrete operator stats" {
    var stats = Stats{};
    stats.add(.mul_mv);
    stats.add(.mul_mv_ext);
    stats.add(.fallback);
    stats.add(.attention_quantized_kv);

    try std.testing.expectEqual(@as(usize, 4), stats.total);
    try std.testing.expectEqual(@as(usize, 1), stats.count(.mul_mv));
    try std.testing.expectEqual(@as(usize, 1), stats.count(.mul_mv_ext));
    try std.testing.expectEqual(@as(usize, 1), stats.count(.attention_quantized_kv));
    try std.testing.expect(stats.hasFallback());
}
