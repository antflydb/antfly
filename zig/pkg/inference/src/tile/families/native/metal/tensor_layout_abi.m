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

// native-program:entry termite_cumulative_sum_i8
           TERMITE_MSL_ABI_CUMULATIVE_BYTE_SUM(termite_cumulative_sum_i8) "\n"
           "    threadgroup uchar totals[256];\n"
           "    termite_cumulative_sum_impl<uchar>(input, output, p, tid, row, totals);\n"
           "}\n"
// native-program:entry termite_cumulative_sum_u8
           TERMITE_MSL_ABI_CUMULATIVE_BYTE_SUM(termite_cumulative_sum_u8) "\n"
           "    threadgroup uchar totals[256];\n"
           "    termite_cumulative_sum_impl<uchar>(input, output, p, tid, row, totals);\n"
           "}\n"
// native-program:entry termite_florence_window_pack_f32
           TERMITE_MSL_ABI_FLORENCE_WINDOW(termite_florence_window_pack_f32) "\n"
           "    uint total = p.window_count * p.window_area * p.dim; if (gid >= total || p.dim == 0u || p.window_size == 0u) return;\n"
           "    uint c = gid % p.dim; uint token = gid / p.dim; uint pos = token % p.window_area; uint win = token / p.window_area;\n"
           "    uint windows_w = p.padded_w / p.window_size; uint windows_h = p.padded_h / p.window_size; uint windows_per_batch = windows_h * windows_w;\n"
           "    uint b = win / windows_per_batch; uint rem = win - b * windows_per_batch; uint wh = rem / windows_w; uint ww = rem - wh * windows_w;\n"
           "    uint dy = pos / p.window_size; uint dx = pos - dy * p.window_size; uint y = wh * p.window_size + dy; uint x = ww * p.window_size + dx;\n"
           "    output[gid] = (b < p.batch && y < p.height && x < p.width) ? input[((b * p.height + y) * p.width + x) * p.dim + c] : 0.0f;\n"
           "}\n"
// native-program:entry termite_florence_window_unpack_f32
           TERMITE_MSL_ABI_FLORENCE_WINDOW(termite_florence_window_unpack_f32) "\n"
           "    uint total = p.batch * p.height * p.width * p.dim; if (gid >= total || p.dim == 0u || p.window_size == 0u) return;\n"
           "    uint c = gid % p.dim; uint token = gid / p.dim; uint x = token % p.width; uint tmp = token / p.width; uint y = tmp % p.height; uint b = tmp / p.height;\n"
           "    uint windows_w = p.padded_w / p.window_size; uint windows_h = p.padded_h / p.window_size; uint wh = y / p.window_size; uint ww = x / p.window_size;\n"
           "    uint dy = y - wh * p.window_size; uint dx = x - ww * p.window_size; uint win = (b * windows_h + wh) * windows_w + ww; uint src_token = win * p.window_area + dy * p.window_size + dx;\n"
           "    output[gid] = input[src_token * p.dim + c];\n"
           "}\n"
