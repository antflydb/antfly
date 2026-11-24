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

// native-program:entry termite_moe_route_select_tg
           TERMITE_MSL_ABI_MOE_ROUTE(termite_moe_route_select_tg) " threadgroup float shared_values[2]; threadgroup float expert_exps[128]; threadgroup float expert_probs[128]; threadgroup uint selected_ids_tg[8]; threadgroup float selected_probs_tg[8]; termite_moe_route_select_tg_impl(logits, route_ids, route_weights, p, route_ids, route_ids, route_ids, false, shared_values, expert_exps, expert_probs, selected_ids_tg, selected_probs_tg, lane, tg); }\n"
// native-program:entry termite_moe_route_select_tg_register
           TERMITE_MSL_ABI_MOE_ROUTE(termite_moe_route_select_tg_register) "\n"
           "    const uint row = tg.x; if (row >= p.rows || p.num_experts == 0u || p.num_experts > 128u || p.top_k == 0u || p.top_k > 8u) return; const uint base = row * p.num_experts;\n"
           "    threadgroup float shared_values[2]; threadgroup float expert_exps[128];\n"
           "    if (lane == 0u) { float row_max = -3.4028234663852886e+38f; for (uint expert = 0u; expert < p.num_experts; ++expert) row_max = max(row_max, logits[base + expert] * p.logit_scale); shared_values[0] = row_max; }\n"
           "    threadgroup_barrier(mem_flags::mem_threadgroup); const float row_max = shared_values[0];\n"
           "    for (uint expert = uint(lane); expert < p.num_experts; expert += 32u) expert_exps[expert] = termite_exp_approx(logits[base + expert] * p.logit_scale - row_max);\n"
           "    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    if (lane == 0u) { float denom = 0.0f; for (uint expert = 0u; expert < p.num_experts; ++expert) denom += expert_exps[expert]; shared_values[1] = max(denom, 1.0e-30f); }\n"
           "    threadgroup_barrier(mem_flags::mem_threadgroup); const float denom = shared_values[1];\n"
           TERMITE_MSL_MOE_ROUTE_SELECT
           TERMITE_MSL_MOE_ROUTE_PUBLISH
           "}\n"
// native-program:entry termite_moe_route_select_tg_register_v2
           TERMITE_MSL_ABI_MOE_ROUTE(termite_moe_route_select_tg_register_v2) "\n"
           "    const uint row = tg.x; if (row >= p.rows || p.num_experts == 0u || p.num_experts > 128u || p.top_k == 0u || p.top_k > 8u) return; const uint base = row * p.num_experts; threadgroup float expert_exps[128];\n"
           "    float row_max_lane = -3.4028234663852886e+38f; if (lane == 0u) for (uint expert = 0u; expert < p.num_experts; ++expert) row_max_lane = max(row_max_lane, logits[base + expert] * p.logit_scale); const float row_max = simd_broadcast(row_max_lane, 0u);\n"
           "    for (uint expert = uint(lane); expert < p.num_experts; expert += 32u) expert_exps[expert] = termite_exp_approx(logits[base + expert] * p.logit_scale - row_max);\n"
           "    simdgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    float denom_lane = 0.0f; if (lane == 0u) { for (uint expert = 0u; expert < p.num_experts; ++expert) denom_lane += expert_exps[expert]; denom_lane = max(denom_lane, 1.0e-30f); } const float denom = simd_broadcast(denom_lane, 0u);\n"
           TERMITE_MSL_MOE_ROUTE_SELECT
           TERMITE_MSL_MOE_ROUTE_PUBLISH
           "}\n"
