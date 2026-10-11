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
#include <math.h>
#include <stdlib.h>

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
_Static_assert(sizeof(PdfCommand) == 72, "PDF command ABI");
// Calls are synchronous. Slots are reused only after waitUntilCompleted.
@interface AntflyPdfBuffers : NSObject
@property(nonatomic, strong) id<MTLBuffer> commands, points, samples, output, tileOffsets, tileIndices;
@end
@implementation AntflyPdfBuffers
@end
@interface AntflyPdfGpu : NSObject
@property(nonatomic, strong) id<MTLDevice> device;
@property(nonatomic, strong) id<MTLCommandQueue> queue;
@property(nonatomic, strong) id<MTLComputePipelineState> pipeline;
@property(nonatomic, strong) NSArray<AntflyPdfBuffers *> *slots;
@property(nonatomic) size_t cachedBytes;
@end
@implementation AntflyPdfGpu
@end

static NSString *const source =
@"#include <metal_stdlib>\n"
"using namespace metal;\n"
"struct Cmd { uint kind, offset, count, fill_rule; uint color, antialias, image_width, image_height; float inverse[6], clip[4]; };\n"
"struct Params { uint width, height, commands, tileCols, tiled; };\n"
"bool inside(float2 p, device const Cmd& c, device const packed_float2* points) {\n"
" int winding=0; bool parity=false; float2 a=points[c.offset+c.count-1];\n"
" for(uint i=0;i<c.count;i++) { float2 b=points[c.offset+i];\n"
" if ((a.y<=p.y && b.y>p.y)||(b.y<=p.y && a.y>p.y)) {\n"
" float cross=a.x+(p.y-a.y)*(b.x-a.x)/(b.y-a.y);\n"
" if(cross>p.x) {parity=!parity; winding+=(b.y>a.y?1:-1);} } a=b; }\n"
" return c.fill_rule?parity:winding!=0; }\n"
"bool clipped(float2 p, device const Cmd& c) {return p.x<c.clip[0]||p.y<c.clip[1]||p.x>c.clip[2]||p.y>c.clip[3];}\n"
"kernel void paint(device const Cmd* cmds[[buffer(0)]],device const packed_float2* points[[buffer(1)]],\n"
" device const uchar* samples[[buffer(2)]],device uchar4* out[[buffer(3)]],constant Params& args[[buffer(4)]],\n"
" device const uint* tileOffsets[[buffer(5)]],device const uint* tileIndices[[buffer(6)]], uint2 tid[[thread_position_in_grid]]) {\n"
" if(tid.x>=args.width||tid.y>=args.height)return; uint4 dst=uint4(255); float2 p=float2(tid)+0.5f;\n"
" uint tile=(tid.y/32)*args.tileCols+tid.x/32;\n"
" uint first=args.tiled?tileOffsets[tile]:0, last=args.tiled?tileOffsets[tile+1]:args.commands;\n"
" for(uint entry=first;entry<last;entry++) {uint ci=args.tiled?tileIndices[entry]:entry; device const Cmd& c=cmds[ci];\n"
// The unused inverse array carries conservative pixel-space paint bounds.
" if(float(tid.x+1)<=c.inverse[0]||float(tid.y+1)<=c.inverse[1]||float(tid.x)>=c.inverse[2]||float(tid.y)>=c.inverse[3])continue;\n"
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
        r.slots = @[[AntflyPdfBuffers new], [AntflyPdfBuffers new], [AntflyPdfBuffers new], [AntflyPdfBuffers new]];
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
size_t antfly_pdf_gpu_cached_bytes(void *handle) {
    return handle ? ((__bridge AntflyPdfGpu *)handle).cachedBytes : 0;
}
static id<MTLBuffer> reserveBuffer(AntflyPdfGpu *r, id<MTLBuffer> old, size_t needed) {
    needed = MAX(needed, (size_t)16);
    if (old.length >= needed) return old;
    const size_t limit = 512 * 1024 * 1024;
    size_t capacity = 16;
    while (capacity < needed && capacity < limit) capacity *= 2;
    size_t total = r.cachedBytes - old.length + capacity;
    if (capacity < needed || total > limit) return nil;
    id<MTLBuffer> buffer = [r.device newBufferWithLength:capacity options:MTLResourceStorageModeShared];
    if (buffer) r.cachedBytes = total;
    return buffer;
}
typedef struct { uint32_t x0, y0, x1, y1; } TileRect;
static TileRect paintBounds(PdfCommand *c, const PdfPage *p, uint32_t cols, uint32_t rows) {
    float x0=p->width, y0=p->height, x1=-1, y1=-1;
    if (c->kind == 1) {
        // O(width + height), independent of the page's pixel count.
        for (uint32_t x=0;x<p->width;x++) if (p->points[(c->count+x)*2] >= 0) { x0=MIN(x0,x); x1=x+1; }
        for (uint32_t y=0;y<p->height;y++) if (p->points[(c->fill_rule+y)*2] >= 0) { y0=MIN(y0,y); y1=y+1; }
    } else {
        for (uint32_t i=0;i<c->count;i++) {
            const float *v=p->points+(c->offset+i)*2;
            x0=MIN(x0,v[0]); y0=MIN(y0,v[1]); x1=MAX(x1,v[0]); y1=MAX(y1,v[1]);
        }
    }
    x0=MAX(x0,c->clip[0]); y0=MAX(y0,c->clip[1]);
    x1=MIN(x1,c->clip[2]); y1=MIN(y1,c->clip[3]);
    c->inverse[0]=x0; c->inverse[1]=y0; c->inverse[2]=x1; c->inverse[3]=y1;
    if (x0>x1 || y0>y1 || x1<0 || y1<0 || x0>p->width || y0>p->height)
        return (TileRect){1,1,0,0};
    return (TileRect){ MIN(cols-1,(uint32_t)MAX(0,floorf(x0/32))), MIN(rows-1,(uint32_t)MAX(0,floorf(y0/32))),
                       MIN(cols-1,(uint32_t)MAX(0,floorf(x1/32))), MIN(rows-1,(uint32_t)MAX(0,floorf(y1/32))) };
}
static int buildTiles(AntflyPdfGpu *r, AntflyPdfBuffers *slot, const PdfPage *p, uint32_t *tiled) {
    uint32_t cols=(p->width+31)/32, rows=(p->height+31)/32, tiles=cols*rows;
    slot.tileOffsets=reserveBuffer(r,slot.tileOffsets,((size_t)tiles+1)*4);
    if (!slot.tileOffsets) return -3;
    uint32_t *offsets=slot.tileOffsets.contents;
    memset(offsets,0,((size_t)tiles+1)*4);
    PdfCommand *commands=slot.commands.contents;
    TileRect rectangles[1024];
    for (uint32_t ci=0;ci<p->command_count;ci++) {
        TileRect rect=rectangles[ci]=paintBounds(&commands[ci],p,cols,rows);
        for(uint32_t y=rect.y0;y<=rect.y1;y++) for(uint32_t x=rect.x0;x<=rect.x1;x++) offsets[y*cols+x+1]++;
    }
    for (uint32_t i=0;i<tiles;i++) offsets[i+1]+=offsets[i];
    // Bound list storage to 16 MiB/page. Dense pages retain the bounds-culled
    // kernel without a tile list; this changes work, never paint semantics.
    *tiled=offsets[tiles]<=4*1024*1024;
    slot.tileIndices=reserveBuffer(r,slot.tileIndices,*tiled?(size_t)offsets[tiles]*4:4);
    if (!slot.tileIndices) return -3;
    if (!*tiled) return 0;
    uint32_t *cursor=malloc((size_t)tiles*4);
    if (!cursor) return -3;
    memcpy(cursor,offsets,(size_t)tiles*4);
    uint32_t *indices=slot.tileIndices.contents;
    // Append in original paint order; overlapping opaque and AA shapes must
    // observe the same composition sequence as the native renderer.
    for (uint32_t ci=0;ci<p->command_count;ci++) {
        TileRect rect=rectangles[ci];
        for(uint32_t y=rect.y0;y<=rect.y1;y++) for(uint32_t x=rect.x0;x<=rect.x1;x++) indices[cursor[y*cols+x]++]=ci;
    }
    free(cursor);
    return 0;
}
int antfly_pdf_gpu_render(void *handle, const PdfPage *pages, size_t count, double *gpu_seconds) {
    @autoreleasepool {
        if (!handle || !pages || count == 0 || count > 4) return -1;
        AntflyPdfGpu *r = (__bridge AntflyPdfGpu *)handle;
        id<MTLCommandBuffer> batch = [r.queue commandBuffer];
        if (!batch) return -2;
        for (size_t i=0;i<count;i++) {
            const PdfPage *p=&pages[i];
            if (!p->width || !p->height || p->width>4096 || p->height>4096 || p->command_count>1024 || p->point_count>65536 || p->pixel_count>64*1024*1024) return -1;
            size_t bytes=(size_t)p->width*p->height*4;
            AntflyPdfBuffers *slot=r.slots[i];
            slot.commands=reserveBuffer(r,slot.commands,p->command_count*sizeof(PdfCommand));
            slot.points=reserveBuffer(r,slot.points,p->point_count*2*sizeof(float));
            slot.samples=reserveBuffer(r,slot.samples,p->pixel_count);
            slot.output=reserveBuffer(r,slot.output,bytes);
            if (!slot.commands || !slot.points || !slot.samples || !slot.output) return -3;
            if(p->command_count)memcpy(slot.commands.contents,p->commands,p->command_count*sizeof(PdfCommand));
            if(p->point_count)memcpy(slot.points.contents,p->points,p->point_count*2*sizeof(float));
            if(p->pixel_count)memcpy(slot.samples.contents,p->pixels,p->pixel_count);
            uint32_t tiled;
            if(buildTiles(r,slot,p,&tiled)!=0)return -3;
            id<MTLComputeCommandEncoder> e = [batch computeCommandEncoder];
            if (!e) return -4;
            [e setComputePipelineState:r.pipeline];
            [e setBuffer:slot.commands offset:0 atIndex:0];
            [e setBuffer:slot.points offset:0 atIndex:1];
            [e setBuffer:slot.samples offset:0 atIndex:2];
            [e setBuffer:slot.output offset:0 atIndex:3];
            [e setBuffer:slot.tileOffsets offset:0 atIndex:5];
            [e setBuffer:slot.tileIndices offset:0 atIndex:6];
            uint32_t args[5]={p->width,p->height,(uint32_t)p->command_count,(p->width+31)/32,tiled};
            [e setBytes:args length:sizeof(args) atIndex:4];
            [e dispatchThreads:MTLSizeMake(p->width,p->height,1) threadsPerThreadgroup:MTLSizeMake(16,16,1)];
            [e endEncoding];
        }
        [batch commit];
        [batch waitUntilCompleted];
        if (batch.status != MTLCommandBufferStatusCompleted) return -5;
        *gpu_seconds = batch.GPUEndTime - batch.GPUStartTime;
        for(size_t i=0;i<count;i++) memcpy(pages[i].output, r.slots[i].output.contents, (size_t)pages[i].width*pages[i].height*4);
        return 0;
    }
}
