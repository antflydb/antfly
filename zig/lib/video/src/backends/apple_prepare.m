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

#include "apple_video.h"
#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#include <string.h>
#include <math.h>
// Embedded by the Zig build as a generated header, never loaded from cwd.
#include "video_prepare_shader.h"
@interface AVPreparer : NSObject
@property(nonatomic, strong) id<MTLDevice> device;
@property(nonatomic, strong) id<MTLCommandQueue> queue;
@property(nonatomic, strong) id<MTLComputePipelineState> horizontal;
@property(nonatomic, strong) id<MTLComputePipelineState> vertical;
@property(nonatomic, strong) id<MTLComputePipelineState> horizontalRGBA;
@property(nonatomic, strong) id<MTLComputePipelineState> horizontalNative;
@property(nonatomic, strong) id<MTLComputePipelineState> chromaNative;
@property(nonatomic, strong) id<MTLComputePipelineState> chromaFloat;
@property(nonatomic) void *cache;
// Single producer, append-only; never overwrite a span observed by inference.
@property(nonatomic, strong) id<MTLBuffer> outputSlab;
@property(nonatomic) size_t outputCapacity;
@property(nonatomic) size_t outputUsed;
@property(nonatomic, strong) id<MTLBuffer> pqLUT;
@property(nonatomic, strong) id<MTLBuffer> hlgLUT;
@end
@implementation AVPreparer
- (void)dealloc { av_metal_destroy(_cache); }
@end
@interface AVPrepared : NSObject
@property(nonatomic, strong) id<MTLCommandBuffer> command;
@property(nonatomic, strong) id<MTLBuffer> output;
@property(nonatomic) size_t outputOffset;
@property(nonatomic) size_t outputBytes;
@property(nonatomic) void *imported;
@property(nonatomic, strong) id<MTLBuffer> rgba;
@property(nonatomic, strong) id<MTLTexture> nativeY;
@property(nonatomic, strong) id<MTLTexture> nativeUV;
@property(nonatomic) BOOL completed;
@property(nonatomic) double gpuSeconds;
@end
@implementation AVPrepared
- (void)dealloc { if (_command.status >= MTLCommandBufferStatusCommitted) [_command waitUntilCompleted]; av_metal_import_release(_imported); }
@end
// One bounded device entry; request queues, texture imports, slabs and LUTs
// stay request-owned. Publication and compilation share a lock, preventing
// concurrent cold requests from compiling identical pipelines twice.
static BOOL preparationPipelines(AVPreparer *p) {
    static NSLock *lock;
    static AVPreparer *cached;
    static dispatch_once_t once;
    dispatch_once(&once, ^{ lock = [NSLock new]; });
    [lock lock];
    @try {
        if (cached.device == p.device) {
            p.horizontal = cached.horizontal; p.vertical = cached.vertical;
            p.horizontalRGBA = cached.horizontalRGBA; p.horizontalNative = cached.horizontalNative;
            p.chromaNative = cached.chromaNative; p.chromaFloat = cached.chromaFloat;
            return YES;
        }
        NSError *error = nil;
        id<MTLLibrary> library = [p.device newLibraryWithSource:@VIDEO_PREPARE_SHADER options:nil error:&error];
        if (!library) return NO;
        p.horizontal = [p.device newComputePipelineStateWithFunction:[library newFunctionWithName:@"horizontal"] error:&error];
        p.vertical = [p.device newComputePipelineStateWithFunction:[library newFunctionWithName:@"vertical_patch"] error:&error];
        p.horizontalRGBA = [p.device newComputePipelineStateWithFunction:[library newFunctionWithName:@"horizontal_rgba"] error:&error];
        p.horizontalNative = [p.device newComputePipelineStateWithFunction:[library newFunctionWithName:@"horizontal_native"] error:&error];
        p.chromaNative = [p.device newComputePipelineStateWithFunction:[library newFunctionWithName:@"chroma_horizontal_native"] error:&error];
        p.chromaFloat = [p.device newComputePipelineStateWithFunction:[library newFunctionWithName:@"chroma_horizontal_float"] error:&error];
        if (!p.horizontal || !p.vertical || !p.horizontalRGBA || !p.horizontalNative || !p.chromaNative || !p.chromaFloat) return NO;
        AVPreparer *entry = [AVPreparer new]; entry.device = p.device;
        entry.horizontal = p.horizontal; entry.vertical = p.vertical;
        entry.horizontalRGBA = p.horizontalRGBA; entry.horizontalNative = p.horizontalNative;
        entry.chromaNative = p.chromaNative; entry.chromaFloat = p.chromaFloat;
        cached = entry;
        return YES;
    } @finally { [lock unlock]; }
}
int32_t av_preparer_create(void *device, void **out) {
    *out = NULL;
    if (!device) return -1;
    @autoreleasepool {
        AVPreparer *p = [AVPreparer new]; p.device = (__bridge id<MTLDevice>)device;
        p.queue = [p.device newCommandQueue];
        void *cache = NULL;
        if (!p.queue || av_metal_create(device, &cache)) return -1;
        p.cache = cache;
        if (!preparationPipelines(p)) return -1;
        *out = (__bridge_retained void *)p; return 0;
    }
}
void av_preparer_destroy(void *p) {
    if (!p) return;
    @autoreleasepool {
        AVPreparer *owner = (__bridge_transfer AVPreparer *)p;
        // Fence all earlier submissions before releasing shared cache admission.
        id<MTLCommandBuffer> fence = [owner.queue commandBuffer];
        [fence commit]; [fence waitUntilCompleted];
    }
}
int32_t av_preparer_output_slab(void *handle, size_t bytes) {
    @autoreleasepool {
        AVPreparer *p = (__bridge AVPreparer *)handle;
        if (!bytes || bytes % sizeof(float) || p.outputCapacity) return -1;
        p.outputSlab = [p.device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
        if (!p.outputSlab) return -1;
        p.outputCapacity = bytes;
        return 0;
    }
}
static id<MTLBuffer> upload(id<MTLDevice> device, const void *bytes, size_t length) {
    return [device newBufferWithBytes:bytes length:length options:MTLResourceStorageModeShared];
}
@interface AVCoefficients : NSObject
@property(nonatomic, strong) id<MTLBuffer> xb;
@property(nonatomic, strong) id<MTLBuffer> xw;
@property(nonatomic, strong) id<MTLBuffer> yb;
@property(nonatomic, strong) id<MTLBuffer> yw;
@end
@implementation AVCoefficients
@end
int32_t av_coefficients_create(void *handle, const uint32_t *xaxis, size_t xsize, const int32_t *xweights, size_t xcount,
    const uint32_t *yaxis, size_t ysize, const int32_t *yweights, size_t ycount, void **out) {
    *out = NULL;
    @autoreleasepool {
        AVPreparer *p = (__bridge AVPreparer *)handle;
        AVCoefficients *c = [AVCoefficients new];
        c.xb = upload(p.device, xaxis, xsize * sizeof(uint32_t));
        c.xw = upload(p.device, xweights, xcount * sizeof(int32_t));
        c.yb = upload(p.device, yaxis, ysize * sizeof(uint32_t));
        c.yw = upload(p.device, yweights, ycount * sizeof(int32_t));
        if (!c.xb || !c.xw || !c.yb || !c.yw) return -1;
        *out = (__bridge_retained void *)c; return 0;
    }
}
void av_coefficients_destroy(void *p) { if (p) { id object = (__bridge_transfer id)p; (void)object; } }
typedef struct { uint32_t indices[5]; int32_t weights[5]; int32_t rounding_bias; } AVChromaTap;
_Static_assert(sizeof(AVChromaTap) == 44, "Metal chroma coefficient ABI changed");
@interface AVChromaCoefficients : NSObject
@property(nonatomic, strong) id<MTLBuffer> x;
@property(nonatomic, strong) id<MTLBuffer> y;
@end
@implementation AVChromaCoefficients
@end
int32_t av_chroma_create(void *handle, const void *x, size_t xcount, const void *y, size_t ycount, void **out) {
    *out = NULL;
    @autoreleasepool {
        AVPreparer *p = (__bridge AVPreparer *)handle;
        AVChromaCoefficients *c = [AVChromaCoefficients new];
        c.x = upload(p.device, x, xcount * sizeof(AVChromaTap)); c.y = upload(p.device, y, ycount * sizeof(AVChromaTap));
        if (!c.x || !c.y) return -1;
        *out = (__bridge_retained void *)c; return 0;
    }
}
void av_chroma_destroy(void *p) { if (p) { id object = (__bridge_transfer id)p; (void)object; } }
// Shared dispatch keeps resize/patch packing identical for both input formats.
static int32_t prepare(void *handle, void *surface, const uint8_t *rgba, size_t rgba_size, const uint32_t *params,
    void *coefficients, void *chromaCoefficients, void **out, id<MTLTexture> nativeY, id<MTLTexture> nativeUV, id<MTLBuffer> destination, size_t destinationOffset) {
    *out = NULL;
    @autoreleasepool {
        AVPreparer *p = (__bridge AVPreparer *)handle;
        AVCoefficients *c = (__bridge AVCoefficients *)coefficients;
        AVPrepared *result = [AVPrepared new];
        if (params[8] < 8 || params[8] > 14 || params[11] > 16 - params[8] || (params[8] == 8 && params[11] != 0)) return -1;
        if ((!rgba && ((params[5] == 3 && (params[9] != 3 || !params[6] || params[12])) || params[5] > 3 || (params[5] == 2 && params[12] == 0))) || params[12] > 2 || (rgba && params[12] != 0)) return -1;
        id<MTLBuffer> toneLUT = params[12] == 1 ? p.pqLUT : p.hlgLUT;
        if (params[12] && !toneLUT) {
            float table[16385];
            for (size_t i = 0; i <= 16384; ++i) {
                double n = (double)i / 16384.0, value;
                if (params[12] == 1) {
                    double v = pow(n, 32.0 / 2523.0);
                    value = 100.0 * pow(fmax(v - 3424.0 / 4096.0, 0.0) / (2413.0 / 128.0 - 2392.0 / 128.0 * v), 16384.0 / 2610.0);
                } else value = n <= 0.5 ? n * n / 3.0 : (exp((n - 0.55991073) / 0.17883277) + 0.28466892) / 12.0;
                table[i] = (float)value;
            }
            toneLUT = upload(p.device, table, sizeof(table));
            if (!toneLUT) return -1;
            if (params[12] == 1) p.pqLUT = toneLUT; else p.hlgLUT = toneLUT;
        }
        id<MTLTexture> y = nil, uv = nil;
        if (nativeY) {
            if (params[8] < 8 || params[8] > 14 || params[9] > 3 || ((nativeUV == nil) != (params[9] == 0))) return -1;
            MTLPixelFormat yf = params[8] == 8 ? MTLPixelFormatR8Uint : MTLPixelFormatR16Uint;
            MTLPixelFormat uvf = params[8] == 8 ? MTLPixelFormatRG8Uint : MTLPixelFormatRG16Uint;
            if (nativeY.device != p.device || nativeY.textureType != MTLTextureType2D ||
                nativeY.pixelFormat != yf || nativeY.width != params[0] || nativeY.height != params[1]) return -1;
            if (params[9] != 0) {
                if (nativeUV.device != p.device || nativeUV.textureType != MTLTextureType2D || nativeUV.pixelFormat != uvf ||
                    nativeUV.width != (params[0] + (params[9] == 3 ? 0 : 1)) / (params[9] == 3 ? 1 : 2) ||
                    nativeUV.height != (params[1] + (params[9] == 1 ? 1 : 0)) / (params[9] == 1 ? 2 : 1)) return -1;
            } else {
                // Bind a valid resource for Metal validation; the monochrome
                // shader branch never reads it or invents a chroma plane.
                MTLTextureDescriptor *dummy = [MTLTextureDescriptor texture2DDescriptorWithPixelFormat:uvf width:1 height:1 mipmapped:NO];
                dummy.storageMode = MTLStorageModePrivate; dummy.usage = MTLTextureUsageShaderRead;
                nativeUV = [p.device newTextureWithDescriptor:dummy];
                if (!nativeUV) return -1;
            }
            result.nativeY = y = nativeY; result.nativeUV = uv = nativeUV;
        } else if (rgba) {
            size_t channels = params[5], stride = params[6];
            if ((channels != 3 && channels != 4) || !params[0] || !params[1] ||
                stride < (size_t)params[0] * channels ||
                rgba_size != stride * (params[1] - 1) + (size_t)params[0] * channels) return -1;
            // Copies before returning; the producer can immediately free RGB/RGBA.
            result.rgba = upload(p.device, rgba, rgba_size);
            if (!result.rgba) return -1;
        } else {
            void *imported = NULL;
            if (av_metal_import(p.cache, surface, &imported)) return -1;
            result.imported = imported;
            y = (__bridge id<MTLTexture>)av_metal_plane_texture(imported, 0);
            uv = (__bridge id<MTLTexture>)av_metal_plane_texture(imported, 1);
        }
        size_t height = (params[4] & 1) ? params[0] - params[13] - params[15] : params[1] - params[14] - params[16];
        id<MTLBuffer> temp = [p.device newBufferWithLength:(size_t)params[2] * height * 3 options:MTLResourceStorageModePrivate];
        size_t outputBytes = (size_t)params[2] * params[3] * 3 * sizeof(float);
        BOOL slab = !destination && p.outputCapacity;
        if (slab) {
            if (p.outputUsed > p.outputCapacity || outputBytes > p.outputCapacity - p.outputUsed) return -1;
            destination = p.outputSlab;
            destinationOffset = p.outputUsed;
        }
        if (destination && (destination.device != p.device || destinationOffset % sizeof(float) || destinationOffset > destination.length || outputBytes > destination.length-destinationOffset)) return -1;
        result.output = destination ?: [p.device newBufferWithLength:outputBytes options:MTLResourceStorageModeShared];
        result.outputOffset = destination ? destinationOffset : 0;
        result.outputBytes = outputBytes;
        result.command = [p.queue commandBuffer];
        if (!temp || !result.output || !c.xb || !c.xw || !c.yb || !c.yw || !result.command) return -1;
        AVChromaCoefficients *chroma = (__bridge AVChromaCoefficients *)chromaCoefficients;
        id<MTLBuffer> chromaHorizontal = nil;
        BOOL reconstruct = !rgba && params[10] == 5 && params[12] == 0 && params[9] != 0;
        if (reconstruct) {
            if (!chroma.x || !chroma.y) return -1;
            size_t columns = ((params[0] & 1) || params[9] == 3) ? params[0] : (params[0] + 1) / 2;
            size_t rows = params[9] == 1 ? (params[1] + 1) / 2 : params[1];
            chromaHorizontal = [p.device newBufferWithLength:columns * rows * 4 options:MTLResourceStorageModePrivate];
            if (!chromaHorizontal) return -1;
            id<MTLComputeCommandEncoder> filter = [result.command computeCommandEncoder];
            if (!filter) return -1;
            [filter setComputePipelineState:nativeY ? p.chromaNative : p.chromaFloat];
            [filter setTexture:uv atIndex:1]; [filter setBuffer:chromaHorizontal offset:0 atIndex:0];
            [filter setBytes:params length:68 atIndex:1]; [filter setBuffer:chroma.x offset:0 atIndex:2];
            [filter dispatchThreads:MTLSizeMake(columns, rows, 1) threadsPerThreadgroup:MTLSizeMake(8, 8, 1)];
            [filter endEncoding];
        }
        id<MTLComputeCommandEncoder> encoder = [result.command computeCommandEncoder];
        if (!encoder) return -1;
        [encoder setComputePipelineState:nativeY ? p.horizontalNative : (rgba ? p.horizontalRGBA : p.horizontal)];
        if (rgba) [encoder setBuffer:result.rgba offset:0 atIndex:4];
        else { [encoder setTexture:y atIndex:0]; [encoder setTexture:uv atIndex:1]; [encoder setBuffer:params[12] ? toneLUT : c.xw offset:0 atIndex:5]; }
        [encoder setBuffer:temp offset:0 atIndex:0]; [encoder setBytes:params length:68 atIndex:1];
        [encoder setBuffer:c.xb offset:0 atIndex:2]; [encoder setBuffer:c.xw offset:0 atIndex:3];
        if (!rgba) { [encoder setBuffer:reconstruct ? chromaHorizontal : c.xw offset:0 atIndex:6]; [encoder setBuffer:reconstruct ? chroma.y : c.xw offset:0 atIndex:7]; }
        [encoder dispatchThreads:MTLSizeMake(params[2], height, 1) threadsPerThreadgroup:MTLSizeMake(8, 8, 1)]; [encoder endEncoding];
        encoder = [result.command computeCommandEncoder]; if (!encoder) return -1;
        [encoder setComputePipelineState:p.vertical]; [encoder setBuffer:temp offset:0 atIndex:0]; [encoder setBuffer:result.output offset:result.outputOffset atIndex:1];
        [encoder setBytes:params length:68 atIndex:2]; [encoder setBuffer:c.yb offset:0 atIndex:3]; [encoder setBuffer:c.yw offset:0 atIndex:4];
        [encoder dispatchThreads:MTLSizeMake(params[2], params[3], 1) threadsPerThreadgroup:MTLSizeMake(8, 8, 1)]; [encoder endEncoding];
        [result.command commit];
        if (slab) p.outputUsed += outputBytes;
        *out = (__bridge_retained void *)result; return 0;
    }
}
int32_t av_prepare_submit(void *handle, void *surface, const uint32_t *params, void *coefficients, void *chromaCoefficients, void **out) {
    return prepare(handle, surface, NULL, 0, params, coefficients, chromaCoefficients, out, nil, nil, nil, 0);
}
int32_t av_prepare_rgba_submit(void *handle, const uint8_t *rgba, size_t size, const uint32_t *params, void *coefficients, void **out) {
    if (!rgba) { *out = NULL; return -1; }
    return prepare(handle, NULL, rgba, size, params, coefficients, NULL, out, nil, nil, nil, 0);
}
int32_t av_prepare_rgba_into_submit(void *handle, const uint8_t *rgba, size_t size, const uint32_t *params, void *coefficients, void *destination, size_t offset, void **out) {
    if (!rgba || !destination) { *out = NULL; return -1; }
    return prepare(handle, NULL, rgba, size, params, coefficients, NULL, out, nil, nil, (__bridge id<MTLBuffer>)destination, offset);
}
int av_prepared_poll(void *p) {
    AVPrepared *result = (__bridge AVPrepared *)p;
    if (result.completed) return 1;
    MTLCommandBufferStatus status = result.command.status;
    return status == MTLCommandBufferStatusCompleted ? 1 : status == MTLCommandBufferStatusError ? -1 : 0;
}
int av_prepared_release_source(void *p) {
    if (av_prepared_poll(p) != 1) return -1;
    AVPrepared *result = (__bridge AVPrepared *)p;
    result.gpuSeconds = result.command.GPUEndTime - result.command.GPUStartTime;
    result.completed = YES;
    av_metal_import_release(result.imported); result.imported = NULL;
    result.command = nil;
    result.rgba = nil; result.nativeY = nil; result.nativeUV = nil;
    return 0;
}
size_t av_prepared_offset(void *p) { return ((__bridge AVPrepared *)p).outputOffset; }
void *av_prepared_buffer(void *p) { return (__bridge void *)((__bridge AVPrepared *)p).output; }
int32_t av_prepared_copy(void *p, float *out, size_t count) {
    AVPrepared *result = (__bridge AVPrepared *)p;
    if (av_prepared_poll(p) != 1 || count > result.outputBytes / sizeof(float)) return -1;
    id<MTLBuffer> readable = result.output;
    size_t offset = result.outputOffset, bytes = count * sizeof(float);
    if (readable.storageMode == MTLStorageModePrivate) {
        id<MTLCommandQueue> queue = [readable.device newCommandQueue];
        id<MTLBuffer> staging = [readable.device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
        id<MTLCommandBuffer> command = [queue commandBuffer];
        id<MTLBlitCommandEncoder> encoder = [command blitCommandEncoder];
        if (!staging || !command || !encoder) return -1;
        [encoder copyFromBuffer:readable sourceOffset:offset toBuffer:staging destinationOffset:0 size:bytes];
        [encoder endEncoding]; [command commit]; [command waitUntilCompleted];
        if (command.status != MTLCommandBufferStatusCompleted) return -1;
        readable = staging; offset = 0;
    }
    if (!readable.contents) return -1;
    memcpy(out, (const uint8_t *)readable.contents+offset, bytes); return 0;
}
void av_prepared_destroy(void *p) {
    if (p) {
        AVPrepared *result = (__bridge_transfer AVPrepared *)p;
        // Destruction fences even abandoned/canceled jobs. Their admission
        // must not be released while the command still owns staging/resources.
        [result.command waitUntilCompleted];
    }
}

double av_prepared_gpu_seconds(void *p) {
    AVPrepared *result = (__bridge AVPrepared *)p;
    if (av_prepared_poll(p) != 1) return -1;
    return result.completed ? result.gpuSeconds : result.command.GPUEndTime - result.command.GPUStartTime;
}
uint64_t av_preparer_device_bytes(void *p) { return ((__bridge AVPreparer *)p).device.currentAllocatedSize; }

int32_t av_prepare_native_submit(void *handle, void *y, void *uv, const uint32_t *params, void *coefficients, void *chromaCoefficients, void **out) {
    return prepare(handle, NULL, NULL, 0, params, coefficients, chromaCoefficients, out, (__bridge id<MTLTexture>)y, (__bridge id<MTLTexture>)uv, nil, 0);
}
int32_t av_prepare_host_submit(void *handle, const uint8_t *yb, size_t ys, const uint8_t *uvb, size_t uvs, const uint32_t *params, void *coefficients, void *chromaCoefficients, void **out) {
    *out = NULL;
    @autoreleasepool {
        AVPreparer *p = (__bridge AVPreparer *)handle;
        if (!yb || (params[9] != 0 && !uvb) || params[8] < 8 || params[8] > 14 || params[9] > 3) return -1;
        NSUInteger cw = (params[0] + (params[9] == 3 ? 0 : 1)) / (params[9] == 3 ? 1 : 2);
        NSUInteger ch = (params[1] + (params[9] == 1 ? 1 : 0)) / (params[9] == 1 ? 2 : 1);
        MTLTextureDescriptor *yd = [MTLTextureDescriptor texture2DDescriptorWithPixelFormat:params[8] == 8 ? MTLPixelFormatR8Uint : MTLPixelFormatR16Uint width:params[0] height:params[1] mipmapped:NO];
        MTLTextureDescriptor *uvd = [MTLTextureDescriptor texture2DDescriptorWithPixelFormat:params[8] == 8 ? MTLPixelFormatRG8Uint : MTLPixelFormatRG16Uint width:cw height:ch mipmapped:NO];
        yd.storageMode = uvd.storageMode = MTLStorageModeShared; yd.usage = uvd.usage = MTLTextureUsageShaderRead;
        id<MTLTexture> y = [p.device newTextureWithDescriptor:yd], uv = params[9] == 0 ? nil : [p.device newTextureWithDescriptor:uvd];
        if (!y || (params[9] != 0 && !uv)) return -1;
        [y replaceRegion:MTLRegionMake2D(0, 0, params[0], params[1]) mipmapLevel:0 withBytes:yb bytesPerRow:ys];
        if (params[9] != 0) [uv replaceRegion:MTLRegionMake2D(0, 0, cw, ch) mipmapLevel:0 withBytes:uvb bytesPerRow:uvs];
        return prepare(handle, NULL, NULL, 0, params, coefficients, chromaCoefficients, out, y, uv, nil, 0);
    }
}

uint32_t av_preparer_tone_mask(void *handle) {
    AVPreparer *p = (__bridge AVPreparer *)handle;
    return (p.pqLUT ? 1u : 0u) | (p.hlgLUT ? 2u : 0u);
}
