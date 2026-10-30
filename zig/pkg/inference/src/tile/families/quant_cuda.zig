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

//! Generated kernel regions of the `quant_cuda` families.

const r = @import("../region.zig");
const q = @import("../quant_cuda.zig");

pub const regions = [_]r.Region{
    r.cuda("quant_cuda.dense_f32", q.dense_f32),
    r.cuda("quant_cuda.linear_f32_tile", q.wrappers(&q.linear_f32_tile)),
    r.cuda("quant_cuda.q8_0_tile", q.wrappers(&q.q8_0_tile)),
    r.cuda("quant_cuda.q4_0_tile", q.wrappers(&q.q4_0_tile)),
    r.cuda("quant_cuda.tc_hmma", q.wrappers(&q.tc_hmma)),
    r.cuda("quant_cuda.k_quant_tile4", q.k_quant_tile4),
    r.cuda("quant_cuda.q4_k_tile", q.wrappers(&q.q4_k_tile)),
    r.cuda("quant_cuda.q4_k_rows", q.q4_k_rows),
    r.cuda("quant_cuda.argmax_stage1", q.argmax_stage1),
    r.cuda("quant_cuda.q4_0_projections", q.wrappers(&q.q4_0_projections)),
    r.cuda("quant_cuda.embedding", q.embedding),
};
