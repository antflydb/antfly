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

// native-program:entry termite_conv2d_f32
extern "C" __global__ void termite_conv2d_f32(ANTFLY_CUDA_CONV2D_F32_PARAMS) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = batch * out_channels * out_h * out_w;
    if (idx >= total) return;
    unsigned int ox = idx % out_w;
    unsigned int tmp = idx / out_w;
    unsigned int oy = tmp % out_h;
    tmp /= out_h;
    unsigned int oc = tmp % out_channels;
    unsigned int b = tmp / out_channels;
    unsigned int in_per_group = in_channels / groups;
    unsigned int out_per_group = out_channels / groups;
    unsigned int group = oc / out_per_group;
    float acc = bias[oc];
    for (unsigned int ig = 0; ig < in_per_group; ++ig) {
        unsigned int ic = group * in_per_group + ig;
        for (unsigned int ky = 0; ky < kernel_h; ++ky) {
            int iy = (int)(oy * stride_h + ky) - (int)padding_h;
            if (iy < 0 || iy >= (int)height) continue;
            for (unsigned int kx = 0; kx < kernel_w; ++kx) {
                int ix = (int)(ox * stride_w + kx) - (int)padding_w;
                if (ix < 0 || ix >= (int)width) continue;
                unsigned int x_idx = ((b * in_channels + ic) * height + (unsigned int)iy) * width + (unsigned int)ix;
                unsigned int w_idx = (((oc * in_per_group + ig) * kernel_h + ky) * kernel_w) + kx;
                acc += input[x_idx] * weight[w_idx];
            }
        }
    }
    dst[idx] = acc;
}
