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

//! Common graph backend policy. Format adapters do not choose kernel legality.
const ml = @import("ml");
const BackendKind = @import("backend_contracts.zig").BackendKind;
const partition = @import("partition.zig");
const metal_capabilities = @import("metal_capabilities.zig");
const webgpu_capabilities = @import("webgpu_capabilities.zig");
const cuda_capabilities = @import("cuda_capabilities.zig");

pub fn supports(kind: BackendKind) *const fn (ml.graph.OpCode) bool {
    return switch (kind) {
        .metal => &metal_capabilities.supportsMetalEagerGraph,
        .webgpu => &webgpu_capabilities.supportsWebGpuGraph,
        .cuda => &cuda_capabilities.supports,
        else => &partition.supportsAll,
    };
}

pub fn decide(kind: BackendKind) ?*const fn (partition.CapabilityQuery) partition.CapabilityDecision {
    return switch (kind) {
        .metal => &metal_capabilities.decideMetalEagerGraph,
        .webgpu => &webgpu_capabilities.decideWebGpuGraph,
        .cuda => &cuda_capabilities.decide,
        .native => &partition.decideNative,
        else => null,
    };
}
