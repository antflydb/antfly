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

// native-program:entry termite_gemma4_audio_clamp
           TERMITE_MSL_ABI_CLAMP_ACTIVATION(termite_gemma4_audio_clamp) "\n"
           "    if (gid >= p.total) return;\n"
           "    float v = input[gid];\n"
           "    if ((p.flags & 1u) != 0u) v = max(v, p.min_value);\n"
           "    if ((p.flags & 2u) != 0u) v = min(v, p.max_value);\n"
           "    output[gid] = v;\n"
           "}\n"
// native-program:entry termite_gemma4_audio_glu_rows
           TERMITE_MSL_ABI_AUDIO_ROWS(termite_gemma4_audio_glu_rows) "\n"
           "    uint total = p.rows * p.dim; if (gid >= total) return;\n"
           "    uint row = gid / p.dim; uint col = gid - row * p.dim;\n"
           "    float a = input[row * 2u * p.dim + col]; float g = input[row * 2u * p.dim + p.dim + col];\n"
           "    float s = g >= 0.0f ? 1.0f / (1.0f + exp(-g)) : exp(g) / (1.0f + exp(g));\n"
           "    output[gid] = a * s;\n"
           "}\n"
// native-program:entry termite_gemma4_audio_flatten_ctf
           TERMITE_MSL_ABI_AUDIO_ROWS(termite_gemma4_audio_flatten_ctf) "\n"
           "    uint time_steps = p.rows; uint freq_bins = p.dim; uint channels = p.extra;\n"
           "    uint total = time_steps * freq_bins * channels; if (gid >= total) return;\n"
           "    uint per_step = freq_bins * channels;\n"
           "    uint t = gid / per_step; uint rem = gid - t * per_step; uint f = rem / channels; uint c = rem - f * channels;\n"
           "    output[gid] = input[(c * time_steps + t) * freq_bins + f];\n"
           "}\n"
