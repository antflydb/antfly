// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#include <metal_stdlib>
using namespace metal;
struct Params { uint sw, sh, tw, th, rotation, bt709, full, centered; };
float3 rgb(texture2d<float, access::read> yplane, texture2d<float, access::read> uvplane, uint2 point, constant Params &p) {
    uint2 xy = point;
    if (p.rotation == 1) xy = uint2(point.y, p.sh - 1 - point.x);
    else if (p.rotation == 2) xy = uint2(p.sw - 1 - point.x, p.sh - 1 - point.y);
    else if (p.rotation == 3) xy = uint2(p.sw - 1 - point.y, point.x);
    float y = round(yplane.read(xy).r * 255.0f);
    float2 uv = round(uvplane.read(xy / 2).rg * 255.0f) - 128.0f;
    if (!p.full) { y = (y - 16.0f) * (255.0f / 219.0f); uv *= (255.0f / 224.0f); }
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
