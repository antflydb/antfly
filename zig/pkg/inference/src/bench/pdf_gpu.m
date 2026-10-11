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

// Benchmark-only Metal PDF painter; no global production renderer is changed.
#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#include <stdint.h>
#include <string.h>

typedef struct {
    uint32_t kind, offset, count, fill_rule;
    uint32_t color, antialias, image_width, image_height;
    float inverse[6], clip[4];
} PdfCommand;
typedef struct {
    const PdfCommand *commands;
    size_t command_count;
    const float *points;
    size_t point_count;
    const uint8_t *pixels;
    size_t pixel_count;
    uint8_t *output;
    uint32_t width, height;
} PdfPage;
@interface AntflyPdfGpu : NSObject
@property(nonatomic, strong) id<MTLDevice> device;
@property(nonatomic, strong) id<MTLCommandQueue> queue;
@property(nonatomic, strong) id<MTLComputePipelineState> pipeline;
@end
@implementation AntflyPdfGpu
@end

static NSString *const source =
@"#include <metal_stdlib>\n"
"using namespace metal;\n"
"struct Cmd { uint kind, offset, count, fill_rule; uint color, antialias, image_width, image_height; float inverse[6], clip[4]; };\n"
"struct Params { uint width, height, commands; };\n"
"bool inside(float2 p, device const Cmd& c, device const packed_float2* points) {\n"
" int winding=0; bool parity=false; float2 a=points[c.offset+c.count-1];\n"
" for(uint i=0;i<c.count;i++) { float2 b=points[c.offset+i];\n"
" if ((a.y<=p.y && b.y>p.y)||(b.y<=p.y && a.y>p.y)) {\n"
" float cross=a.x+(p.y-a.y)*(b.x-a.x)/(b.y-a.y);\n"
" if(cross>p.x) {parity=!parity; winding+=(b.y>a.y?1:-1);} } a=b; }\n"
" return c.fill_rule?parity:winding!=0; }\n"
"bool clipped(float2 p, device const Cmd& c) {return p.x<c.clip[0]||p.y<c.clip[1]||p.x>c.clip[2]||p.y>c.clip[3];}\n"
"kernel void paint(device const Cmd* cmds[[buffer(0)]],device const packed_float2* points[[buffer(1)]],\n"
" device const uchar* samples[[buffer(2)]],device uchar4* out[[buffer(3)]],constant Params& args[[buffer(4)]], uint2 tid[[thread_position_in_grid]]) {\n"
" if(tid.x>=args.width||tid.y>=args.height)return; uint4 dst=uint4(255); float2 p=float2(tid)+0.5f;\n"
" for(uint ci=0;ci<args.commands;ci++) {device const Cmd& c=cmds[ci];\n"
" if(c.kind==1) {if(clipped(p,c))continue;\n"
" int sx=int(points[c.count+tid.x].x),sy=int(points[c.fill_rule+tid.y].x);\n"
" if(sx<0||sy<0)continue; uint x=uint(sx),y=uint(sy);\n"
" uint off=c.offset+(y*c.image_width+x)*4; dst=uint4(samples[off],samples[off+1],samples[off+2],255);\n"
" }else {uint covered=0;\n"
" if(c.antialias) {for(uint y=0;y<2;y++)for(uint x=0;x<2;x++){float2 q=float2(tid)+float2(0.25f+0.5f*x,0.25f+0.5f*y); if(!clipped(q,c)&&inside(q,c,points))covered++;}}\n"
" else if(!clipped(p,c)&&inside(p,c,points))covered=4;\n"
" if(!covered)continue; uint alpha=(255*covered+2)/4;\n"
" uint3 color=uint3(c.color&255,(c.color>>8)&255,(c.color>>16)&255);\n"
" dst.xyz=(color*alpha+dst.xyz*(255-alpha)+127)/255;\n"
" }}out[tid.y*args.width+tid.x]=uchar4(dst); }\n";

void *antfly_pdf_gpu_create(void) {
    @autoreleasepool {
        AntflyPdfGpu *r = [AntflyPdfGpu new];
        r.device = MTLCreateSystemDefaultDevice();
        if (!r.device) return NULL;
        r.queue = [r.device newCommandQueue];
        NSError *error = nil;
        MTLCompileOptions *options = [MTLCompileOptions new];
        options.fastMathEnabled = NO;
        id<MTLLibrary> library = [r.device newLibraryWithSource:source options:options error:&error];
        if (!library) { NSLog(@"PDF GPU shader: %@", error); return NULL; }
        r.pipeline = [r.device newComputePipelineStateWithFunction:[library newFunctionWithName:@"paint"] error:&error];
        if (!r.pipeline || !r.queue) return NULL;
        return (__bridge_retained void *)r;
    }
}
void antfly_pdf_gpu_destroy(void *handle) {
    if (handle) { id released = (__bridge_transfer id)handle; (void)released; }
}
int antfly_pdf_gpu_render(void *handle, const PdfPage *pages, size_t count, double *gpu_seconds) {
    @autoreleasepool {
        if (!handle || !pages || count == 0 || count > 4) return -1;
        AntflyPdfGpu *r = (__bridge AntflyPdfGpu *)handle;
        id<MTLCommandBuffer> batch = [r.queue commandBuffer];
        if (!batch) return -2;
        NSMutableArray<id<MTLBuffer>> *outputs = [NSMutableArray arrayWithCapacity:count];
        for (size_t i=0;i<count;i++) {
            const PdfPage *p=&pages[i];
            size_t bytes=(size_t)p->width*p->height*4;
            id<MTLBuffer> commands = p->command_count ? [r.device newBufferWithBytes:p->commands length:p->command_count*sizeof(PdfCommand) options:MTLResourceStorageModeShared] : [r.device newBufferWithLength:sizeof(PdfCommand) options:MTLResourceStorageModeShared];
            id<MTLBuffer> points = p->point_count ? [r.device newBufferWithBytes:p->points length:p->point_count*2*sizeof(float) options:MTLResourceStorageModeShared] : [r.device newBufferWithLength:8 options:MTLResourceStorageModeShared];
            id<MTLBuffer> samples = p->pixel_count ? [r.device newBufferWithBytes:p->pixels length:p->pixel_count options:MTLResourceStorageModeShared] : [r.device newBufferWithLength:4 options:MTLResourceStorageModeShared];
            id<MTLBuffer> output = [r.device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
            if (!commands || !points || !samples || !output) return -3;
            [outputs addObject:output];
            id<MTLComputeCommandEncoder> e = [batch computeCommandEncoder];
            if (!e) return -4;
            [e setComputePipelineState:r.pipeline];
            [e setBuffer:commands offset:0 atIndex:0];
            [e setBuffer:points offset:0 atIndex:1];
            [e setBuffer:samples offset:0 atIndex:2];
            [e setBuffer:output offset:0 atIndex:3];
            uint32_t args[3]={p->width,p->height,(uint32_t)p->command_count};
            [e setBytes:args length:sizeof(args) atIndex:4];
            [e dispatchThreads:MTLSizeMake(p->width,p->height,1) threadsPerThreadgroup:MTLSizeMake(16,16,1)];
            [e endEncoding];
        }
        [batch commit];
        [batch waitUntilCompleted];
        if (batch.status != MTLCommandBufferStatusCompleted) return -5;
        *gpu_seconds = batch.GPUEndTime - batch.GPUStartTime;
        for(size_t i=0;i<count;i++) memcpy(pages[i].output, outputs[i].contents, (size_t)pages[i].width*pages[i].height*4);
        return 0;
    }
}
