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

//! Every generated region in metal_kernels.m and inference_cuda_kernels.cu,
//! one module per kernel family. A family module exports
//! `pub const regions: []const Region`.

const r = @import("region.zig");
const rms_norm = @import("rms_norm.zig");
const argmax = @import("argmax.zig");

pub const Region = r.Region;

const core = [_]Region{
    r.metal("rms_norm", rms_norm.metalRegion(&rms_norm.kernels)),
    r.cuda("rms_norm", rms_norm.cudaRegion(&rms_norm.kernels)),
    r.metal("layer_norm", rms_norm.metalRegion(&rms_norm.layer_norm_kernels)),
    r.cuda("layer_norm", rms_norm.cudaRegion(&rms_norm.layer_norm_kernels)),
    r.metal("argmax", argmax.metalRegion()),
    r.cuda("argmax", argmax.cudaRegion()),
};

pub const regions: []const Region = &core ++
    @import("families/reduce.zig").regions ++
    @import("families/elementwise.zig").regions ++
    @import("families/quant_metal.zig").regions ++
    @import("families/quant_cuda.zig").regions ++
    @import("families/attention.zig").regions ++
    @import("families/model.zig").regions;
