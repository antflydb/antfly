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
struct ChromaTap { uint indices[5]; int weights[5]; int rounding_bias; };
struct Params { uint sw, sh, tw, th, rotation, bt709, full, centered, depth, chroma, rounding, sample_shift, tone, crop_left, crop_top, crop_right, crop_bottom; };
uint2 source_point(uint2 point, constant Params &p) {
    uint cw = p.sw - p.crop_left - p.crop_right, ch = p.sh - p.crop_top - p.crop_bottom;
    uint2 xy = point;
    if (p.rotation == 1) xy = uint2(point.y, ch - 1 - point.x);
    else if (p.rotation == 2) xy = uint2(cw - 1 - point.x, ch - 1 - point.y);
    else if (p.rotation == 3) xy = uint2(cw - 1 - point.y, point.x);
    return xy + uint2(p.crop_left, p.crop_top);
}
float3 tone_rgb(float3 encoded, constant Params &p, device const float *eotf) {
    float3 v = clamp(encoded / 255.0f, 0.0f, 1.0f), linear;
    float3 position = v * 16384.0f;
    uint3 index = min(uint3(position), uint3(16383));
    float3 a = float3(eotf[index.x], eotf[index.y], eotf[index.z]);
    float3 b = float3(eotf[index.x + 1], eotf[index.y + 1], eotf[index.z + 1]);
    linear = a + (b - a) * (position - float3(index));
    if (p.tone == 2) {
        float l = dot(linear, p.bt709 == 2 ? float3(0.2627f, 0.6780f, 0.0593f) : float3(0.2126f, 0.7152f, 0.0722f));
        linear *= 10.0f * pow(max(l, 0.0f), 0.2f);
    }
    if (p.bt709 == 2) {
        float3 r = linear;
        linear = float3(dot(r, float3(1.660491f, -0.587641f, -0.072850f)), dot(r, float3(-0.124550f, 1.132900f, -0.008350f)), dot(r, float3(-0.018151f, -0.100579f, 1.118730f)));
    }
    linear = clamp(linear / (1.0f + max(dot(linear, float3(0.2126f, 0.7152f, 0.0722f)), 0.0f)), 0.0f, 1.0f);
    float3 srgb = select(1.055f * pow(linear, float3(1.0f / 2.4f)) - 0.055f, 12.92f * linear, linear <= 0.0031308f);
    return clamp(floor(srgb * 255.0f + 0.5f), 0.0f, 255.0f);
}
float3 rgb_combined(int y, int2 uv, constant Params &p) {
    int scale = 1 << (p.depth - 8);
    int4 c = p.bt709 == 1 ? (p.full ? int4(12901, -1534, -3835, 15201) : int4(14686, -1747, -4366, 17305))
           : p.bt709 == 2 ? (p.full ? int4(12080, -1348, -4680, 15412) : int4(13752, -1535, -5328, 17545))
                          : (p.full ? int4(11485, -2819, -5850, 14516) : int4(13075, -3209, -6660, 16525));
    int luma = (y - (p.full ? 0 : 16 * scale)) * (p.full ? 8192 : 9539);
    int shift = p.depth + 5;
    int3 sums = int3(luma + uv.y * c.x, luma + uv.x * c.y + uv.y * c.z, luma + uv.x * c.w);
    int3 rounded = (sums + (1 << (shift - 1))) >> shift;
    rounded = select(rounded, int3(0), rounded >= 512);
    return float3(clamp(rounded, 0, 255));
}
float3 rgb_gbr(uint g, uint2 br, constant Params &p) {
    uint maximum = (1u << p.depth) - 1u;
    int gg = int(min(g, maximum)), bb = int(min(br.x, maximum)), rr = int(min(br.y, maximum));
    if (p.depth == 8) return float3(rr, gg, bb);
    if (p.rounding != 6) return float3((uint3(rr, gg, bb) * 255u + maximum / 2u) / maximum);
    int shift = p.depth + 1, bias = 1 << p.depth;
    int y = (8414 * rr + 16519 * gg + 3208 * bb + (16 << (p.depth + 7)) + bias) >> shift;
    int u = ((-4865 * rr - 9528 * gg + 14392 * bb + (128 << (p.depth + 7)) + bias) >> shift) - 8192;
    int v = ((14392 * rr - 12061 * gg - 2332 * bb + (128 << (p.depth + 7)) + bias) >> shift) - 8192;
    int yy = (y - 1024) * 9539;
    return float3(clamp((int3(yy + v * 13075, yy - u * 3209 - v * 6660, yy + u * 16525) + (1 << 18)) >> 19, 0, 255));
}
float3 rgb_q16(int y, int2 uv_delta, constant Params &p) {
    int4 c = p.bt709 == 1 ? (p.full ? int4(103206, -12275, -30678, 121608) : int4(100902, -12001, -29993, 118894))
           : p.bt709 == 2 ? (p.full ? int4(96638, -10783, -37443, 123299) : int4(94482, -10543, -36607, 120547))
                          : (p.full ? int4(91881, -22552, -46800, 116129) : int4(89830, -22049, -45756, 113537));
    int2 uv = uv_delta + 128;
    int4 displacement = (int4(uv.y, uv.x, uv.y, uv.x) * c >> 16) - (c >> 9);
    int3 indexes = y + int3(displacement.x, displacement.y + displacement.z, displacement.w);
    int3 value = p.full ? indexes : ((indexes + 326) * 76309 - 400 * 65536 + 32768) >> 16;
    return float3(clamp(value, 0, 255));
}
float3 rgb_q13(int y, int2 uv, constant Params &p) {
    int4 c = p.bt709 ? (p.full ? int4(12901, -1534, -3835, 15201) : int4(14686, -1747, -4366, 17305))
                    : (p.full ? int4(11485, -2819, -5850, 14516) : int4(13075, -3209, -6660, 16525));
    int luma = ((y - (p.full ? 0 : 16)) * (p.full ? 8192 : 9539)) >> 12;
    int3 value = int3(luma + ((uv.y * c.x) >> 12),
                     luma + ((uv.x * c.y) >> 12) + ((uv.y * c.z) >> 12),
                     luma + ((uv.x * c.w) >> 12));
    return float3(clamp((value + 1) >> 1, 0, 255));
}
float3 rgb_reconstructed(uint raw, uint2 xy, constant Params &p, device const short2 *horizontal, device const ChromaTap *vertical) {
    if (p.chroma == 0) {
        uint shift = p.depth - 8;
        return float3(min((raw + (shift == 0 ? 0u : 1u << (shift - 1))) >> shift, 255u));
    }
    bool full_chroma = (p.sw & 1) || p.chroma == 3;
    uint columns = full_chroma ? p.sw : (p.sw + 1) / 2;
    uint column = full_chroma ? xy.x : xy.x / 2;
    ChromaTap tap = vertical[xy.y];
    int2 uv = int2(0);
    for (uint i = 0; i < 5; ++i) {
        if (tap.weights[i] != 0) uv += int2(horizontal[tap.indices[i] * columns + column]) * tap.weights[i];
    }
    if (!full_chroma) {
        uint shift = p.depth - 8;
        int luma = int((raw + (shift == 0 ? 0u : 1u << (shift - 1))) >> shift);
        return rgb_q16(luma, clamp((uv + (1 << 18)) >> 19, 0, 255) - 128, p);
    }
    int4 c = p.bt709 == 1 ? (p.full ? int4(12901, -1534, -3835, 15201) : int4(14686, -1747, -4366, 17305))
           : p.bt709 == 2 ? (p.full ? int4(12080, -1348, -4680, 15412) : int4(13752, -1535, -5328, 17545))
                          : (p.full ? int4(11485, -2819, -5850, 14516) : int4(13075, -3209, -6660, 16525));
    int2 chroma = (uv + tap.rounding_bias - (128 << 19)) >> 10;
    uint luma = ((raw << (17 - p.depth)) - (p.full ? 0u : 16u << 9)) * (p.full ? 8192u : 9539u) + (1u << 21);
    uint3 sums = uint3(luma + uint(chroma.y) * uint(c.x), luma + uint(chroma.x) * uint(c.y) + uint(chroma.y) * uint(c.z), luma + uint(chroma.x) * uint(c.w));
    return float3(clamp(as_type<int3>(sums) >> 22, 0, 255));
}
kernel void chroma_horizontal_native(texture2d<uint, access::read> uv [[texture(1)]], device short2 *out [[buffer(0)]], constant Params &p [[buffer(1)]], device const ChromaTap *axis [[buffer(2)]], uint2 gid [[thread_position_in_grid]]) {
    uint columns = ((p.sw & 1) || p.chroma == 3) ? p.sw : (p.sw + 1) / 2;
    uint rows = p.chroma == 1 ? (p.sh + 1) / 2 : p.sh;
    if (gid.x >= columns || gid.y >= rows) return;
    ChromaTap tap = axis[gid.x]; int2 sum = int2(0);
    for (uint i = 0; i < 5; ++i) if (tap.weights[i] != 0) sum += int2(uv.read(uint2(tap.indices[i], gid.y)).rg >> p.sample_shift) * tap.weights[i];
    out[gid.y * columns + gid.x] = as_type<short2>(ushort2(min(sum >> (p.depth - 1), 32767)));
}
kernel void chroma_horizontal_float(texture2d<float, access::read> uv [[texture(1)]], device short2 *out [[buffer(0)]], constant Params &p [[buffer(1)]], device const ChromaTap *axis [[buffer(2)]], uint2 gid [[thread_position_in_grid]]) {
    uint columns = ((p.sw & 1) || p.chroma == 3) ? p.sw : (p.sw + 1) / 2;
    uint rows = p.chroma == 1 ? (p.sh + 1) / 2 : p.sh;
    if (gid.x >= columns || gid.y >= rows) return;
    ChromaTap tap = axis[gid.x]; int2 sum = int2(0);
    float maximum = p.depth == 8 ? 255.0f : 65535.0f;
    for (uint i = 0; i < 5; ++i) if (tap.weights[i] != 0) sum += int2(floor(round(uv.read(uint2(tap.indices[i], gid.y)).rg * maximum) / float(1u << p.sample_shift))) * tap.weights[i];
    out[gid.y * columns + gid.x] = as_type<short2>(ushort2(min(sum >> (p.depth - 1), 32767)));
}
float3 rgb(texture2d<float, access::read> yplane, texture2d<float, access::read> uvplane, uint2 point, constant Params &p, device const float *eotf, device const short2 *chroma, device const ChromaTap *chroma_y) {
    uint2 xy = source_point(point, p);
    if (p.rounding == 5 && p.tone == 0) return rgb_reconstructed(uint(round(yplane.read(xy).r * (p.depth == 8 ? 255.0f : 65535.0f))) >> p.sample_shift, xy, p, chroma, chroma_y);
    float scale = float(1u << (p.depth - 8));
    float storage_max = p.depth == 8 ? 255.0f : 65535.0f;
    float stored_scale = float(1u << p.sample_shift);
    float y = floor(round(yplane.read(xy).r * storage_max) / stored_scale) / scale;
    float2 uv = floor(round(uvplane.read(xy / 2).rg * storage_max) / stored_scale) / scale - 128.0f;
    if (p.rounding == 4 && p.tone == 0) return rgb_combined(int(y * scale), int2(uv * scale), p);
    if (p.rounding == 2 && p.depth == 8 && p.tone == 0) return rgb_q16(int(y), int2(uv), p);
    if (p.rounding == 1 && p.depth == 8 && p.tone == 0) return rgb_q13(int(y), int2(uv), p);
    if (!p.full) { y = (y - 16.0f) * (255.0f / 219.0f); uv *= (255.0f / 224.0f); }
    else if (p.depth != 8) { float factor = 255.0f * scale / float((1u << p.depth) - 1u); y *= factor; uv *= factor; }
    float3 value = p.bt709 == 1 ? float3(y + 1.5748f * uv.y, y - 0.187324f * uv.x - 0.468124f * uv.y, y + 1.8556f * uv.x)
                          : p.bt709 == 2 ? float3(y + 1.4746f * uv.y, y - 0.164553f * uv.x - 0.571353f * uv.y, y + 1.8814f * uv.x)
                          : float3(y + 1.402f * uv.y, y - 0.344136f * uv.x - 0.714136f * uv.y, y + 1.772f * uv.x);
    if (p.tone != 0) return tone_rgb(value, p, eotf);
    return clamp(floor(value + 0.5f), 0.0f, 255.0f);
}
kernel void horizontal(texture2d<float, access::read> yplane [[texture(0)]], texture2d<float, access::read> uvplane [[texture(1)]],
    device uchar *out [[buffer(0)]], constant Params &p [[buffer(1)]], device const uint *axis [[buffer(2)]], device const int *weights [[buffer(3)]], device const float *eotf [[buffer(5)]], device const short2 *chroma [[buffer(6)]], device const ChromaTap *chroma_y [[buffer(7)]], uint2 gid [[thread_position_in_grid]]) {
    uint dh = (p.rotation & 1) ? p.sw - p.crop_left - p.crop_right : p.sh - p.crop_top - p.crop_bottom;
    if (gid.x >= p.tw || gid.y >= dh) return;
    uint start = axis[gid.x * 3], offset = axis[gid.x * 3 + 1], count = axis[gid.x * 3 + 2];
    int3 sum = int3(1 << 21);
    for (uint i = 0; i < count; ++i) sum += int3(rgb(yplane, uvplane, uint2(start + i, gid.y), p, eotf, chroma, chroma_y)) * weights[offset + i];
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
    uint dh = (p.rotation & 1) ? p.sw - p.crop_left - p.crop_right : p.sh - p.crop_top - p.crop_bottom;
    if (gid.x >= p.tw || gid.y >= dh) return;
    uint start = axis[gid.x * 3], offset = axis[gid.x * 3 + 1], count = axis[gid.x * 3 + 2];
    int3 sum = int3(1 << 21);
    for (uint i = 0; i < count; ++i) {
        uint2 xy = source_point(uint2(start + i, gid.y), p);
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

float3 native_rgb(texture2d<uint, access::read> yplane, texture2d<uint, access::read> uvplane, uint2 point, constant Params &p, device const float *eotf, device const short2 *chroma, device const ChromaTap *chroma_y) {
    uint2 xy = source_point(point, p);
    if (p.bt709 == 3) return rgb_gbr(yplane.read(xy).r >> p.sample_shift, uvplane.read(xy).rg >> p.sample_shift, p);
    if (p.rounding == 5 && p.tone == 0) return rgb_reconstructed(yplane.read(xy).r >> p.sample_shift, xy, p, chroma, chroma_y);
    if (p.rounding == 3 && p.chroma == 0 && p.tone == 0) {
        uint shift = p.depth - 8;
        uint sample = yplane.read(xy).r >> p.sample_shift;
        uint rounded = (sample + (shift == 0 ? 0u : 1u << (shift - 1))) >> shift;
        return float3(min(rounded, 255u));
    }
    float scale = float(1u << (p.depth - 8));
    float y = float(yplane.read(xy).r >> p.sample_shift) / scale;
    float2 uv = float2(0.0f);
    if (p.chroma != 0) uv = float2(uvplane.read(xy / uint2(p.chroma == 3 ? 1 : 2, p.chroma == 1 ? 2 : 1)).rg >> p.sample_shift) / scale - 128.0f;
    if (p.rounding == 4 && p.tone == 0) return rgb_combined(int(y * scale), int2(uv * scale), p);
    if (p.rounding == 2 && p.depth == 8 && p.tone == 0) return rgb_q16(int(y), int2(uv), p);
    if (p.rounding == 1 && p.depth == 8 && p.tone == 0) return rgb_q13(int(y), int2(uv), p);
    if (!p.full) { y = (y - 16.0f) * (255.0f / 219.0f); uv *= (255.0f / 224.0f); }
    else if (p.depth != 8) { float factor = 255.0f * scale / float((1u << p.depth) - 1u); y *= factor; uv *= factor; }
    float3 value = p.bt709 == 1 ? float3(y + 1.5748f * uv.y, y - 0.187324f * uv.x - 0.468124f * uv.y, y + 1.8556f * uv.x)
                          : p.bt709 == 2 ? float3(y + 1.4746f * uv.y, y - 0.164553f * uv.x - 0.571353f * uv.y, y + 1.8814f * uv.x)
                          : float3(y + 1.402f * uv.y, y - 0.344136f * uv.x - 0.714136f * uv.y, y + 1.772f * uv.x);
    if (p.tone != 0) return tone_rgb(value, p, eotf);
    return clamp(floor(value + 0.5f), 0.0f, 255.0f);
}
kernel void horizontal_native(texture2d<uint, access::read> yplane [[texture(0)]], texture2d<uint, access::read> uvplane [[texture(1)]],
    device uchar *out [[buffer(0)]], constant Params &p [[buffer(1)]], device const uint *axis [[buffer(2)]], device const int *weights [[buffer(3)]], device const float *eotf [[buffer(5)]], device const short2 *chroma [[buffer(6)]], device const ChromaTap *chroma_y [[buffer(7)]], uint2 gid [[thread_position_in_grid]]) {
    uint dh = (p.rotation & 1) ? p.sw - p.crop_left - p.crop_right : p.sh - p.crop_top - p.crop_bottom;
    if (gid.x >= p.tw || gid.y >= dh) return;
    uint start = axis[gid.x * 3], offset = axis[gid.x * 3 + 1], count = axis[gid.x * 3 + 2];
    int3 sum = int3(1 << 21);
    for (uint i = 0; i < count; ++i) sum += int3(native_rgb(yplane, uvplane, uint2(start + i, gid.y), p, eotf, chroma, chroma_y)) * weights[offset + i];
    uchar3 value = uchar3(clamp(sum >> 22, 0, 255));
    uint index = (gid.y * p.tw + gid.x) * 3;
    out[index] = value.r; out[index + 1] = value.g; out[index + 2] = value.b;
}
