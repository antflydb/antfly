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

__device__ float rgb(const unsigned char *p, unsigned pitch, unsigned h,
                     unsigned x, unsigned y, unsigned c, unsigned matrix, unsigned full) {
    float yy = p[y*pitch+x];
    const unsigned char *uv = p + pitch*h + (y/2)*pitch + (x/2)*2;
    float u = float(uv[0])-128.0f, v = float(uv[1])-128.0f;
    if (!full) { yy=(yy-16.0f)*(255.0f/219.0f); u*=255.0f/224.0f; v*=255.0f/224.0f; }
    float kr = matrix == 1 ? 0.2126f : 0.299f;
    float kb = matrix == 1 ? 0.0722f : 0.114f;
    float kg = 1.0f-kr-kb;
    float value = c==0 ? yy+2.0f*(1.0f-kr)*v : c==2 ? yy+2.0f*(1.0f-kb)*u : yy-2.0f*kb*(1.0f-kb)/kg*u-2.0f*kr*(1.0f-kr)/kg*v;
    return floorf(fminf(255.0f,fmaxf(0.0f,value))+0.5f);
}
extern "C" __global__ void video_patches(const unsigned char *input, float *output,
    unsigned pitch, unsigned width, unsigned height, unsigned out_width, unsigned out_height,
    unsigned patch, unsigned matrix, unsigned full,
    float mean0, float mean1, float mean2, float std0, float std1, float std2) {
    unsigned i = blockIdx.x*blockDim.x+threadIdx.x;
    if (i >= out_width*out_height*3) return;
    unsigned patch_area=patch*patch, token=i/(3*patch_area), within=i%(3*patch_area);
    unsigned c=within/patch_area, pixel=within%patch_area;
    unsigned dx=(token%(out_width/patch))*patch+pixel%patch;
    unsigned dy=(token/(out_width/patch))*patch+pixel/patch;
    float sx=(float(dx)+0.5f)*float(width)/float(out_width)-0.5f;
    float sy=(float(dy)+0.5f)*float(height)/float(out_height)-0.5f;
    unsigned x0=(unsigned)fmaxf(0.0f,floorf(sx)), y0=(unsigned)fmaxf(0.0f,floorf(sy)), x1=min(x0+1,width-1), y1=min(y0+1,height-1);
    float fx=sx-x0, fy=sy-y0;
    float a=rgb(input,pitch,height,x0,y0,c,matrix,full), b=rgb(input,pitch,height,x1,y0,c,matrix,full);
    float d=rgb(input,pitch,height,x0,y1,c,matrix,full), e=rgb(input,pitch,height,x1,y1,c,matrix,full);
    float value=(a*(1.0f-fx)+b*fx)*(1.0f-fy)+(d*(1.0f-fx)+e*fx)*fy;
    float mean=c==0?mean0:c==1?mean1:mean2, stdev=c==0?std0:c==1?std1:std2;
    output[i]=(value*(1.0f/255.0f)-mean)/stdev;
}

// Convert the selected NV12 surface once. EmbeddingGemma2 then applies its
// antialiased bicubic resize and RGB-interleaved patch packing on the device.
extern "C" __global__ void video_rgb8(const unsigned char *input, unsigned char *output,
    unsigned pitch, unsigned width, unsigned height, unsigned matrix, unsigned full) {
    unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= width * height * 3) return;
    unsigned pixel = i / 3;
    output[i] = (unsigned char)rgb(input, pitch, height, pixel % width, pixel / width, i % 3, matrix, full);
}
