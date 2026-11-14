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

//! Dense promotion is a workload/layout decision, independent of checkpoint names.
const std = @import("std");
pub const Workload = enum { direct, reused, compact_head, encoder, optional_head, optional_encoder };
pub const Request = struct {
    workload: Workload,
    rows: usize,
    out_dim: usize,
    supported_quant: bool,
    low_bit_k: bool,
    packed_expert: bool,
    force_dense: bool = false,
    allow_optional: bool = false,
};
pub fn admit(request: Request) bool {
    if (request.rows == 0 or request.out_dim == 0 or request.packed_expert or !request.supported_quant) return false;
    if (request.force_dense) return true;
    return switch (request.workload) {
        .direct => false,
        .reused => true,
        .compact_head => request.low_bit_k,
        .encoder => request.low_bit_k and request.rows >= 16 and request.out_dim >= 512,
        .optional_head => request.allow_optional and request.rows <= 4 and request.out_dim >= 512,
        .optional_encoder => request.allow_optional and request.rows >= 16 and request.out_dim >= 512,
    };
}
pub fn preparedRows(rows: usize, input_width: usize, output_width: usize, blas: bool) bool {
    return !blas and rows >= 32 and input_width >= 64 and output_width >= 64;
}
test "dense projection admission is driven by workload shape and physical layout" {
    var request = Request{ .workload = .encoder, .rows = 16, .out_dim = 512, .supported_quant = true, .low_bit_k = true, .packed_expert = false };
    try std.testing.expect(admit(request));
    request.rows = 15;
    try std.testing.expect(!admit(request));
    request.rows = 16;
    request.out_dim = 511;
    try std.testing.expect(!admit(request));
    request.out_dim = 512;
    request.low_bit_k = false;
    try std.testing.expect(!admit(request));
    request.force_dense = true;
    try std.testing.expect(admit(request));
    request.packed_expert = true;
    try std.testing.expect(!admit(request));
    request.packed_expert = false;
    request.supported_quant = false;
    try std.testing.expect(!admit(request));
}
test "dense optional workloads and prepared panels retain row boundaries" {
    const head = Request{ .workload = .optional_head, .rows = 4, .out_dim = 512, .supported_quant = true, .low_bit_k = false, .packed_expert = false, .allow_optional = true };
    try std.testing.expect(admit(head));
    var next = head;
    next.rows = 5;
    try std.testing.expect(!admit(next));
    next = head;
    next.allow_optional = false;
    try std.testing.expect(!admit(next));
    try std.testing.expect(preparedRows(32, 64, 64, false));
    try std.testing.expect(!preparedRows(31, 64, 64, false));
    try std.testing.expect(!preparedRows(32, 63, 64, false));
    try std.testing.expect(!preparedRows(32, 64, 64, true));
}
