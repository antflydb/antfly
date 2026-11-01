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

// Generated from zig/lib/tile. Do not edit.
#define ANTFLY_TILE_DISTANCE_MSL \
    "inline float antfly_tile_distance(device const float* point_row, device const float* centroid_row, uint dims, int metric) {\n" \
    "        float dot = 0.0f;\n" \
    "        float point_norm_sq = 0.0f;\n" \
    "        float centroid_norm_sq = 0.0f;\n" \
    "        float l2 = 0.0f;\n" \
    "        for (uint dim = 0; dim < dims; dim++) {\n" \
    "            const float point = point_row[dim];\n" \
    "            const float centroid = centroid_row[dim];\n" \
    "            const float delta = point - centroid;\n" \
    "            dot += point * centroid;\n" \
    "            point_norm_sq += point * point;\n" \
    "            centroid_norm_sq += centroid * centroid;\n" \
    "            l2 += delta * delta;\n" \
    "        }\n" \
    "        float distance = l2;\n" \
    "        if (metric == 1) {\n" \
    "            distance = -dot;\n" \
    "        } else if (metric == 2) {\n" \
    "            if (point_norm_sq == 0.0f || centroid_norm_sq == 0.0f) {\n" \
    "                distance = 1.0f;\n" \
    "            } else {\n" \
    "                distance = 1.0f - dot * rsqrt(point_norm_sq * centroid_norm_sq);\n" \
    "            }\n" \
    "        }\n" \
    "    return distance;\n" \
    "}"
