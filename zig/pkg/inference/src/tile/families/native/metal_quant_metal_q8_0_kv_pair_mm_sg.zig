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
    \\           TERMITE_MSL_ABI_Q8_KV_PAIR_MATRIX(termite_q8_0_kv_pair_mm_sg) " termite_q8_0_kv_pair_mm_sg_impl(input, weight_k, weight_v, output_k, output_v, p, shmem, tiitg, sgitg, tg, false); }\n"
    \\           TERMITE_MSL_ABI_Q8_KV_PAIR_MATRIX(termite_q8_0_kv_pair_mm_sg_tail) " termite_q8_0_kv_pair_mm_sg_impl(input, weight_k, weight_v, output_k, output_v, p, shmem, tiitg, sgitg, tg, true); }\n"
    \\
;
