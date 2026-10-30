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

//! Generated kernel regions of the `model` families.

const r = @import("../region.zig");
const lightonocr = @import("../model_lightonocr.zig");
const deberta = @import("../model_deberta_attention.zig");
const gliner_boundary = @import("../model_gliner_boundary.zig");
const dot_general = @import("../model_dot_general.zig");

const Region = r.Region;

pub const regions = [_]Region{
    r.metal("model.lightonocr3_vec4", r.metalLines(r.metal_indent, lightonocr.vec4Source())),
    r.metal("model.lightonocr3_tiled", r.metalLines(r.metal_indent, lightonocr.tiledSource())),
    r.region(r.metal_kernels_m, "    ", "model.deberta_attention", r.metalLines("    ", deberta.metalSource())),
    r.region("src/ops/cuda/kernels/gliner25_attention.cuh", "", "model.deberta_attention", deberta.cudaSource()),
    r.metal("model.gliner_boundary", r.metalLines(r.metal_indent, gliner_boundary.metalSource())),
    r.region("src/ops/cuda/kernels/gliner25_boundary.cuh", "", "model.gliner_boundary", gliner_boundary.cudaSource()),
    r.metal("model.dot_general_small_m", r.metalLines(r.metal_indent, dot_general.source())),
};
