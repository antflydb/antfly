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

/// Tensor publication/storage contract. BF16 rounds operation outputs and keeps
/// normalization, softmax, pooling and dot partial reductions in F32. Public vectors
/// remain F32. This is immutable model policy, never inferred from a model name.
pub const Policy = enum { f32, bf16 };

/// Provider schedule is independent of tensor storage/publication precision.
pub const Gemm = enum { provider_default, vendor, vendor_deterministic };

/// Residual accumulation is independent of BF16 branch publication.
pub const ResidualStorage = enum { activation, f32 };

/// A resident BF16 CUDA boundary needs a stable vendor accumulation schedule.
/// Preserve explicit vendor choices; provider-default stage execution remains
/// unchanged outside resident programs. Callers report the resolved schedule.
pub fn residentGemm(cuda: bool, precision: Policy, requested: Gemm) Gemm {
    return if (cuda and precision == .bf16 and requested == .provider_default) .vendor_deterministic else requested;
}

test "resident GEMM resolution is local to the qualified arithmetic boundary" {
    const std = @import("std");
    try std.testing.expectEqual(Gemm.vendor_deterministic, residentGemm(true, .bf16, .provider_default));
    try std.testing.expectEqual(Gemm.provider_default, residentGemm(true, .f32, .provider_default));
    try std.testing.expectEqual(Gemm.provider_default, residentGemm(false, .bf16, .provider_default));
    try std.testing.expectEqual(Gemm.vendor, residentGemm(true, .bf16, .vendor));
    try std.testing.expectEqual(Gemm.vendor_deterministic, residentGemm(true, .bf16, .vendor_deterministic));
}
