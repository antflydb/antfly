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

__device__ float luma_or_chroma(const unsigned char *p, unsigned high) {
    // P016 keeps 10-bit samples MSB-aligned; both depths land on the 8-bit scale.
    return high ? float(((unsigned)p[0] | ((unsigned)p[1] << 8)) >> 6) * 0.25f : float(p[0]);
}
__device__ float transfer_linear(float n, unsigned tone) {
    if (tone == 1) {
        float v = powf(fminf(1.0f, fmaxf(0.0f, n)), 32.0f / 2523.0f);
        return 100.0f * powf(fmaxf(v - 3424.0f / 4096.0f, 0.0f) / (2413.0f / 128.0f - 2392.0f / 128.0f * v), 16384.0f / 2610.0f);
    }
    float v = fminf(1.0f, fmaxf(0.0f, n));
    return v <= 0.5f ? v * v / 3.0f : (expf((v - 0.55991073f) / 0.17883277f) + 0.28466892f) / 12.0f;
}
// Fixed exposure, global Reinhard shoulder and sRGB: the portable HDR policy.
__device__ float render(float r, float g, float b, unsigned c, unsigned wide, unsigned tone) {
    float lr = transfer_linear(r / 255.0f, tone), lg = transfer_linear(g / 255.0f, tone), lb = transfer_linear(b / 255.0f, tone);
    if (tone == 2) {
        float l = wide ? lr * 0.2627f + lg * 0.6780f + lb * 0.0593f : lr * 0.2126f + lg * 0.7152f + lb * 0.0722f;
        float scale = 10.0f * powf(fmaxf(l, 0.0f), 0.2f);
        lr *= scale; lg *= scale; lb *= scale;
    }
    if (wide) {
        float wr = lr, wg = lg, wb = lb;
        lr = 1.660491f * wr - 0.587641f * wg - 0.072850f * wb;
        lg = -0.124550f * wr + 1.132900f * wg - 0.008350f * wb;
        lb = -0.018151f * wr - 0.100579f * wg + 1.118730f * wb;
    }
    float shoulder = 1.0f / (1.0f + fmaxf(lr * 0.2126f + lg * 0.7152f + lb * 0.0722f, 0.0f));
    float v = fminf(1.0f, fmaxf(0.0f, (c == 0 ? lr : c == 1 ? lg : lb) * shoulder));
    float encoded = v <= 0.0031308f ? 12.92f * v : 1.055f * powf(v, 1.0f / 2.4f) - 0.055f;
    return floorf(fminf(1.0f, fmaxf(0.0f, encoded)) * 255.0f + 0.5f);
}
__device__ float rgb(const unsigned char *p, unsigned pitch, unsigned h,
                     unsigned x, unsigned y, unsigned c, unsigned matrix, unsigned full, unsigned high, unsigned tone) {
    unsigned step = high ? 2 : 1;
    float yy = luma_or_chroma(p + y*pitch + x*step, high);
    const unsigned char *uv = p + pitch*h + (y/2)*pitch + (x/2)*2*step;
    float u = luma_or_chroma(uv, high)-128.0f, v = luma_or_chroma(uv + step, high)-128.0f;
    if (!full) { yy=(yy-16.0f)*(255.0f/219.0f); u*=255.0f/224.0f; v*=255.0f/224.0f; }
    float kr = matrix == 1 ? 0.2126f : matrix == 9 ? 0.2627f : 0.299f;
    float kb = matrix == 1 ? 0.0722f : matrix == 9 ? 0.0593f : 0.114f;
    float kg = 1.0f-kr-kb;
    float red = yy+2.0f*(1.0f-kr)*v, blue = yy+2.0f*(1.0f-kb)*u;
    float green = yy-2.0f*kb*(1.0f-kb)/kg*u-2.0f*kr*(1.0f-kr)/kg*v;
    if (tone) return render(fminf(255.0f,fmaxf(0.0f,red)), fminf(255.0f,fmaxf(0.0f,green)), fminf(255.0f,fmaxf(0.0f,blue)), c, matrix == 9, tone);
    float value = c==0 ? red : c==2 ? blue : green;
    return floorf(fminf(255.0f,fmaxf(0.0f,value))+0.5f);
}
// Rotation is clockwise quarter turns from coded to displayed pixels.
__device__ void coded_position(float dx, float dy, unsigned width, unsigned height, unsigned rotation, float *x, float *y) {
    if (rotation == 1) { *x = dy; *y = float(height)-1.0f-dx; }
    else if (rotation == 2) { *x = float(width)-1.0f-dx; *y = float(height)-1.0f-dy; }
    else if (rotation == 3) { *x = float(width)-1.0f-dy; *y = dx; }
    else { *x = dx; *y = dy; }
}
extern "C" __global__ void video_patches(const unsigned char *input, float *output,
    unsigned pitch, unsigned width, unsigned height, unsigned out_width, unsigned out_height,
    unsigned patch, unsigned matrix, unsigned full, unsigned rotation, unsigned high, unsigned tone,
    float mean0, float mean1, float mean2, float std0, float std1, float std2) {
    unsigned i = blockIdx.x*blockDim.x+threadIdx.x;
    if (i >= out_width*out_height*3) return;
    unsigned patch_area=patch*patch, token=i/(3*patch_area), within=i%(3*patch_area);
    unsigned c=within/patch_area, pixel=within%patch_area;
    unsigned dx=(token%(out_width/patch))*patch+pixel%patch;
    unsigned dy=(token/(out_width/patch))*patch+pixel/patch;
    unsigned raster_width = rotation & 1 ? height : width, raster_height = rotation & 1 ? width : height;
    float px=(float(dx)+0.5f)*float(raster_width)/float(out_width)-0.5f;
    float py=(float(dy)+0.5f)*float(raster_height)/float(out_height)-0.5f;
    float sx, sy;
    coded_position(px, py, width, height, rotation, &sx, &sy);
    unsigned x0=(unsigned)fmaxf(0.0f,floorf(sx)), y0=(unsigned)fmaxf(0.0f,floorf(sy)), x1=min(x0+1,width-1), y1=min(y0+1,height-1);
    float fx=sx-x0, fy=sy-y0;
    float a=rgb(input,pitch,height,x0,y0,c,matrix,full,high,tone), b=rgb(input,pitch,height,x1,y0,c,matrix,full,high,tone);
    float d=rgb(input,pitch,height,x0,y1,c,matrix,full,high,tone), e=rgb(input,pitch,height,x1,y1,c,matrix,full,high,tone);
    float value=(a*(1.0f-fx)+b*fx)*(1.0f-fy)+(d*(1.0f-fx)+e*fx)*fy;
    float mean=c==0?mean0:c==1?mean1:mean2, stdev=c==0?std0:c==1?std1:std2;
    output[i]=(value*(1.0f/255.0f)-mean)/stdev;
}

// Convert the selected surface once into its displayed orientation. EmbeddingGemma2
// then applies its antialiased bicubic resize and RGB-interleaved patch packing.
extern "C" __global__ void video_rgb8(const unsigned char *input, unsigned char *output,
    unsigned pitch, unsigned width, unsigned height, unsigned matrix, unsigned full,
    unsigned rotation, unsigned high, unsigned tone) {
    unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= width * height * 3) return;
    unsigned pixel = i / 3, raster_width = rotation & 1 ? height : width;
    float sx, sy;
    coded_position(float(pixel % raster_width), float(pixel / raster_width), width, height, rotation, &sx, &sy);
    output[i] = (unsigned char)rgb(input, pitch, height, (unsigned)sx, (unsigned)sy, i % 3, matrix, full, high, tone);
}
