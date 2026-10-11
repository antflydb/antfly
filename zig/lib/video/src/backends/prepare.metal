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

#include <metal_stdlib>
using namespace metal;
struct Params { uint sw, sh, tw, th, rotation, bt709, full, centered, depth, chroma, rounding, sample_shift; };
float3 rgb_q13(int y, int2 uv, constant Params &p) {
    int4 c = p.bt709 ? (p.full ? int4(12901, -1534, -3835, 15201) : int4(14686, -1747, -4366, 17305))
                    : (p.full ? int4(11485, -2819, -5850, 14516) : int4(13075, -3209, -6660, 16525));
    int luma = ((y - (p.full ? 0 : 16)) * (p.full ? 8192 : 9539)) >> 12;
    int3 value = int3(luma + ((uv.y * c.x) >> 12),
                     luma + ((uv.x * c.y) >> 12) + ((uv.y * c.z) >> 12),
                     luma + ((uv.x * c.w) >> 12));
    return float3(clamp((value + 1) >> 1, 0, 255));
}
float3 rgb(texture2d<float, access::read> yplane, texture2d<float, access::read> uvplane, uint2 point, constant Params &p) {
    uint2 xy = point;
    if (p.rotation == 1) xy = uint2(point.y, p.sh - 1 - point.x);
    else if (p.rotation == 2) xy = uint2(p.sw - 1 - point.x, p.sh - 1 - point.y);
    else if (p.rotation == 3) xy = uint2(p.sw - 1 - point.y, point.x);
    float scale = float(1u << (p.depth - 8));
    float storage_max = p.depth == 8 ? 255.0f : 65535.0f;
    float stored_scale = float(1u << p.sample_shift);
    float y = floor(round(yplane.read(xy).r * storage_max) / stored_scale) / scale;
    float2 uv = floor(round(uvplane.read(xy / 2).rg * storage_max) / stored_scale) / scale - 128.0f;
    if (p.rounding && p.depth == 8) return rgb_q13(int(y), int2(uv), p);
    if (!p.full) { y = (y - 16.0f) * (255.0f / 219.0f); uv *= (255.0f / 224.0f); }
    else if (p.depth != 8) { float factor = 255.0f * scale / float((1u << p.depth) - 1u); y *= factor; uv *= factor; }
    float3 value = p.bt709 ? float3(y + 1.5748f * uv.y, y - 0.187324f * uv.x - 0.468124f * uv.y, y + 1.8556f * uv.x)
                          : float3(y + 1.402f * uv.y, y - 0.344136f * uv.x - 0.714136f * uv.y, y + 1.772f * uv.x);
    return clamp(floor(value + 0.5f), 0.0f, 255.0f);
}
kernel void horizontal(texture2d<float, access::read> yplane [[texture(0)]], texture2d<float, access::read> uvplane [[texture(1)]],
    device uchar *out [[buffer(0)]], constant Params &p [[buffer(1)]], device const uint *axis [[buffer(2)]], device const int *weights [[buffer(3)]], uint2 gid [[thread_position_in_grid]]) {
    uint dh = (p.rotation & 1) ? p.sw : p.sh;
    if (gid.x >= p.tw || gid.y >= dh) return;
    uint start = axis[gid.x * 3], offset = axis[gid.x * 3 + 1], count = axis[gid.x * 3 + 2];
    int3 sum = int3(1 << 21);
    for (uint i = 0; i < count; ++i) sum += int3(rgb(yplane, uvplane, uint2(start + i, gid.y), p)) * weights[offset + i];
    uchar3 value = uchar3(clamp(sum >> 22, 0, 255));
    uint index = (gid.y * p.tw + gid.x) * 3;
    out[index] = value.r; out[index + 1] = value.g; out[index + 2] = value.b;
}
// Software-decoded RGBA is staged once as tightly packed bytes. Alpha is ignored
// exactly as in the opaque JPEG CPU reference; no NV12 conversion is involved.
kernel void horizontal_rgba(device const uchar *rgba [[buffer(4)]],
    device uchar *out [[buffer(0)]], constant Params &p [[buffer(1)]],
    device const uint *axis [[buffer(2)]], device const int *weights [[buffer(3)]],
    uint2 gid [[thread_position_in_grid]]) {
    uint dh = (p.rotation & 1) ? p.sw : p.sh;
    if (gid.x >= p.tw || gid.y >= dh) return;
    uint start = axis[gid.x * 3], offset = axis[gid.x * 3 + 1], count = axis[gid.x * 3 + 2];
    int3 sum = int3(1 << 21);
    for (uint i = 0; i < count; ++i) {
        uint2 xy = uint2(start + i, gid.y);
        if (p.rotation == 1) xy = uint2(xy.y, p.sh - 1 - xy.x);
        else if (p.rotation == 2) xy = uint2(p.sw - 1 - xy.x, p.sh - 1 - xy.y);
        else if (p.rotation == 3) xy = uint2(p.sw - 1 - xy.y, xy.x);
        uint pos = (xy.y * p.sw + xy.x) * 4;
        sum += int3(rgba[pos], rgba[pos + 1], rgba[pos + 2]) * weights[offset + i];
    }
    uchar3 value = uchar3(clamp(sum >> 22, 0, 255));
    uint index = (gid.y * p.tw + gid.x) * 3;
    out[index] = value.r; out[index + 1] = value.g; out[index + 2] = value.b;
}
kernel void vertical_patch(device const uchar *in [[buffer(0)]], device float *out [[buffer(1)]], constant Params &p [[buffer(2)]],
    device const uint *axis [[buffer(3)]], device const int *weights [[buffer(4)]], uint2 gid [[thread_position_in_grid]]) {
    if (gid.x >= p.tw || gid.y >= p.th) return;
    uint start = axis[gid.y * 3], offset = axis[gid.y * 3 + 1], count = axis[gid.y * 3 + 2];
    int3 sum = int3(1 << 21);
    for (uint i = 0; i < count; ++i) {
        uint pos = ((start + i) * p.tw + gid.x) * 3;
        sum += int3(in[pos], in[pos + 1], in[pos + 2]) * weights[offset + i];
    }
    float3 value = float3(clamp(sum >> 22, 0, 255)) * (1.0f / 255.0f);
    if (p.centered) value = 2.0f * (value - 0.5f);
    uint patch = (gid.y / 16) * (p.tw / 16) + gid.x / 16;
    uint index = patch * 768 + ((gid.y % 16) * 16 + gid.x % 16) * 3;
    out[index] = value.r; out[index + 1] = value.g; out[index + 2] = value.b;
}

float3 native_rgb(texture2d<uint, access::read> yplane, texture2d<uint, access::read> uvplane, uint2 point, constant Params &p) {
    uint2 xy = point;
    if (p.rotation == 1) xy = uint2(point.y, p.sh - 1 - point.x);
    else if (p.rotation == 2) xy = uint2(p.sw - 1 - point.x, p.sh - 1 - point.y);
    else if (p.rotation == 3) xy = uint2(p.sw - 1 - point.y, point.x);
    float scale = float(1u << (p.depth - 8));
    float y = float(yplane.read(xy).r >> p.sample_shift) / scale;
    float2 uv = float2(0.0f);
    if (p.chroma != 0) uv = float2(uvplane.read(xy / uint2(p.chroma == 3 ? 1 : 2, p.chroma == 1 ? 2 : 1)).rg >> p.sample_shift) / scale - 128.0f;
    if (p.rounding && p.depth == 8) return rgb_q13(int(y), int2(uv), p);
    if (!p.full) { y = (y - 16.0f) * (255.0f / 219.0f); uv *= (255.0f / 224.0f); }
    else if (p.depth != 8) { float factor = 255.0f * scale / float((1u << p.depth) - 1u); y *= factor; uv *= factor; }
    float3 value = p.bt709 ? float3(y + 1.5748f * uv.y, y - 0.187324f * uv.x - 0.468124f * uv.y, y + 1.8556f * uv.x)
                          : float3(y + 1.402f * uv.y, y - 0.344136f * uv.x - 0.714136f * uv.y, y + 1.772f * uv.x);
    return clamp(floor(value + 0.5f), 0.0f, 255.0f);
}
kernel void horizontal_native(texture2d<uint, access::read> yplane [[texture(0)]], texture2d<uint, access::read> uvplane [[texture(1)]],
    device uchar *out [[buffer(0)]], constant Params &p [[buffer(1)]], device const uint *axis [[buffer(2)]], device const int *weights [[buffer(3)]], uint2 gid [[thread_position_in_grid]]) {
    uint dh = (p.rotation & 1) ? p.sw : p.sh;
    if (gid.x >= p.tw || gid.y >= dh) return;
    uint start = axis[gid.x * 3], offset = axis[gid.x * 3 + 1], count = axis[gid.x * 3 + 2];
    int3 sum = int3(1 << 21);
    for (uint i = 0; i < count; ++i) sum += int3(native_rgb(yplane, uvplane, uint2(start + i, gid.y), p)) * weights[offset + i];
    uchar3 value = uchar3(clamp(sum >> 22, 0, 255));
    uint index = (gid.y * p.tw + gid.x) * 3;
    out[index] = value.r; out[index + 1] = value.g; out[index + 2] = value.b;
}
