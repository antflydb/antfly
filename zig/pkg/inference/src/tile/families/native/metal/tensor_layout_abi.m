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
{{window_pack}}
           "}\n"
// native-program:entry termite_florence_window_unpack_f32
           TERMITE_MSL_ABI_FLORENCE_WINDOW(termite_florence_window_unpack_f32) "\n"
{{window_unpack}}
           "}\n"
