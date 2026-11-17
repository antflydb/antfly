// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Tensor publication/storage contract. BF16 rounds operation outputs and keeps
/// normalization, softmax, pooling and dot partial reductions in F32. Public vectors
/// remain F32. This is immutable model policy, never inferred from a model name.
pub const Policy = enum { f32, bf16 };

/// Provider schedule is independent of tensor storage/publication precision.
pub const Gemm = enum { provider_default, vendor, vendor_deterministic };

/// Residual accumulation is independent of BF16 branch publication.
pub const ResidualStorage = enum { activation, f32 };
