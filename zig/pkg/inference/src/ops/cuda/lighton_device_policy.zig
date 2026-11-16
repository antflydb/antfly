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

//! Hardware/artifact requirements, independent of benchmark coverage.
const std = @import("std");

pub const Device = struct {
    major: i32,
    minor: i32,
    driver_version: i32,

    pub fn tensorCores(self: Device) bool {
        return self.major >= 8;
    }

    // The portable activation/vision/recurrent artifacts use PTX 9.0.
    pub fn portableKernels(self: Device) bool {
        return self.tensorCores() and self.driver_version >= 13000;
    }

    // These artifacts contain architecture-specific sm_121a instructions.
    pub fn specializedKernels(self: Device) bool {
        return self.major == 12 and self.minor == 1;
    }

    pub fn automatic(self: Device, lighton: bool, cublaslt: bool, requested: ?bool) bool {
        return (requested orelse true) and lighton and cublaslt and self.portableKernels();
    }
};

pub const GraphDecision = enum {
    enabled,
    operator_disabled,
    model_unsupported,
    compute_unsupported,
    driver_unsupported,
    attention_disabled,
    kv_write_disabled,
    recurrent_batch_disabled,
    ffn_block_disabled,
    ffn_inputs_disabled,
    state_pool_disabled,
    arithmetic_not_capturable,
    cublaslt_unavailable,
    plan_cache_too_small,
    execution_not_capturable,
};

test "LightOn CUDA automatic policy follows capabilities and preserves explicit disable" {
    for ([_]Device{
        .{ .major = 8, .minor = 0, .driver_version = 13000 }, // A100
        .{ .major = 8, .minor = 9, .driver_version = 13000 }, // L4
        .{ .major = 9, .minor = 0, .driver_version = 13000 }, // Hopper
        .{ .major = 12, .minor = 1, .driver_version = 13020 }, // Spark
    }) |device| {
        try std.testing.expect(device.automatic(true, true, null));
        try std.testing.expect(device.automatic(true, true, true));
        try std.testing.expect(!device.automatic(true, true, false));
        try std.testing.expect(!device.automatic(false, true, true));
        try std.testing.expect(!device.automatic(true, false, true));
        try std.testing.expectEqual(device.major == 12, device.specializedKernels());
    }
    const old_driver = Device{ .major = 8, .minor = 0, .driver_version = 12090 };
    const old_compute = Device{ .major = 7, .minor = 5, .driver_version = 13000 };
    try std.testing.expect(!old_driver.automatic(true, true, true));
    try std.testing.expect(!old_compute.automatic(true, true, true));
}
