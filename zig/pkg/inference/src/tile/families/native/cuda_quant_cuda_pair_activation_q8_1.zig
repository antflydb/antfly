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

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_f32_tile4(ANTFLY_CUDA_Q8_INPUT_PAIR_ACTIVATION_PARAMS) {
    \\    termite_q4_0_pair_activation_q8_1_tile4_impl<4u>(dst, q8_input, weight_gate, weight_up, rows, in_dim, out_dim, activation);
    \\}
    \\
    \\extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8(ANTFLY_CUDA_Q8_INPUT_PAIR_ACTIVATION_PARAMS) {
    \\    termite_q4_0_pair_activation_q8_1_tile4_impl<8u>(dst, q8_input, weight_gate, weight_up, rows, in_dim, out_dim, activation);
    \\}
    \\
;
