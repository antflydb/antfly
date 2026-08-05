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
const platform = @import("antfly_platform");

pub const ResidencyPolicy = struct {
    tensor_core_packed_weights: bool = true,
    dequantize_all_weights: bool = false,
    dequantize_q4_0_to_bf16: bool = false,
    retain_q4_0_bf16_mirror: bool = false,
};

pub fn dequantizeAllWeights() bool {
    return platform.env.getenvBoolDefault(
        "TERMITE_CUDA_DEQUANTIZE_QUANT_WEIGHTS",
        false,
    );
}

pub fn dequantizeQ4_0ToBf16() bool {
    return platform.env.getenvBoolDefault(
        "ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q4_0_MATRIX_WEIGHTS_BF16",
        false,
    );
}

pub fn retainQ4_0Bf16Mirror() bool {
    return platform.env.getenvBoolDefault(
        "ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL",
        false,
    );
}

pub fn tensorCorePackedWeightsRequested() bool {
    const raw = platform.env.getenv("ANTFLY_CUDA_QMATMUL_VARIANT") orelse
        return true;
    return !(std.ascii.eqlIgnoreCase(raw, "legacy") or
        std.ascii.eqlIgnoreCase(raw, "fast_r2c4") or
        std.ascii.eqlIgnoreCase(raw, "r2c4") or
        std.ascii.eqlIgnoreCase(raw, "fast_r2c8") or
        std.ascii.eqlIgnoreCase(raw, "r2c8") or
        std.ascii.eqlIgnoreCase(raw, "fast_r4c4") or
        std.ascii.eqlIgnoreCase(raw, "r4c4"));
}

pub fn residencyPolicyFromEnv() ResidencyPolicy {
    return .{
        .tensor_core_packed_weights = tensorCorePackedWeightsRequested(),
        .dequantize_all_weights = dequantizeAllWeights(),
        .dequantize_q4_0_to_bf16 = dequantizeQ4_0ToBf16(),
        .retain_q4_0_bf16_mirror = retainQ4_0Bf16Mirror(),
    };
}

fn scaleBytes(bytes: usize, numerator: usize, denominator: usize) !usize {
    const product = std.math.mul(usize, bytes, numerator) catch
        return error.ResourceLimitExceeded;
    const rounded = std.math.add(usize, product, denominator - 1) catch
        return error.ResourceLimitExceeded;
    return rounded / denominator;
}

/// Conservatively estimate steady CUDA weight residency from the encoded
/// artifact size. The CUDA uploader may retain the source quantized buffer and
/// an execution-specific representation simultaneously:
///
/// - tensor-core Q8_0/Q4_K packing is approximately a second encoded copy;
/// - a Q4_0 BF16 prefill mirror adds about 3.6 encoded bytes per encoded byte;
/// - full dequantization can expand the lowest-bit supported formats by well
///   over an order of magnitude.
///
/// The estimate deliberately includes a small metadata/alignment margin. It is
/// evaluated from the same policy helpers used by the uploader, so operational
/// environment changes cannot silently bypass admission.
pub fn estimateEncodedArtifactDeviceBytesForPolicy(
    encoded_bytes: usize,
    policy: ResidencyPolicy,
) !usize {
    if (policy.dequantize_all_weights) {
        return scaleBytes(encoded_bytes, 32, 1);
    }
    if (policy.retain_q4_0_bf16_mirror and
        !policy.dequantize_q4_0_to_bf16)
    {
        return scaleBytes(encoded_bytes, 6, 1);
    }
    if (policy.dequantize_q4_0_to_bf16) {
        return scaleBytes(encoded_bytes, 4, 1);
    }
    // Even without tensor-core packing, F16 source tensors are materialized as
    // F32 by the current CUDA uploader. Use the same 2.25x floor for both dense
    // F16 and packed quantized artifacts; disabling tensor-core kernels must
    // not turn admission into an F16 under-estimate.
    _ = policy.tensor_core_packed_weights;
    return scaleBytes(encoded_bytes, 9, 4);
}

pub fn estimateEncodedArtifactDeviceBytes(encoded_bytes: usize) !usize {
    return estimateEncodedArtifactDeviceBytesForPolicy(
        encoded_bytes,
        residencyPolicyFromEnv(),
    );
}

test "CUDA load plan accounts for packed and expanded weight residency" {
    const encoded: usize = 1024;
    try std.testing.expectEqual(
        @as(usize, 2304),
        try estimateEncodedArtifactDeviceBytesForPolicy(encoded, .{}),
    );
    try std.testing.expectEqual(
        @as(usize, 2304),
        try estimateEncodedArtifactDeviceBytesForPolicy(encoded, .{
            .tensor_core_packed_weights = false,
        }),
    );
    try std.testing.expectEqual(
        @as(usize, 6144),
        try estimateEncodedArtifactDeviceBytesForPolicy(encoded, .{
            .retain_q4_0_bf16_mirror = true,
        }),
    );
    try std.testing.expectEqual(
        @as(usize, 32768),
        try estimateEncodedArtifactDeviceBytesForPolicy(encoded, .{
            .dequantize_all_weights = true,
        }),
    );
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        estimateEncodedArtifactDeviceBytesForPolicy(
            std.math.maxInt(usize),
            .{},
        ),
    );
}
