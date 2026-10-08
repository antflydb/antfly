// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#include "apple_video.h"
#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#include <string.h>
// Embedded by the Zig build as a generated header, never loaded from cwd.
#include "video_prepare_shader.h"
@interface AVPreparer : NSObject
@property(nonatomic, strong) id<MTLDevice> device;
@property(nonatomic, strong) id<MTLCommandQueue> queue;
@property(nonatomic, strong) id<MTLComputePipelineState> horizontal;
@property(nonatomic, strong) id<MTLComputePipelineState> vertical;
@property(nonatomic) void *cache;
@end
@implementation AVPreparer
- (void)dealloc { av_metal_destroy(_cache); }
@end
@interface AVPrepared : NSObject
@property(nonatomic, strong) id<MTLCommandBuffer> command;
@property(nonatomic, strong) id<MTLBuffer> output;
@property(nonatomic) void *imported;
@end
@implementation AVPrepared
- (void)dealloc { if (_command.status >= MTLCommandBufferStatusCommitted) [_command waitUntilCompleted]; av_metal_import_release(_imported); }
@end
int32_t av_preparer_create(void *device, void **out) {
    *out = NULL;
    if (!device) return -1;
    @autoreleasepool {
        AVPreparer *p = [AVPreparer new]; p.device = (__bridge id<MTLDevice>)device;
        p.queue = [p.device newCommandQueue];
        void *cache = NULL;
        if (!p.queue || av_metal_create(device, &cache)) return -1;
        p.cache = cache;
        NSError *error = nil;
        id<MTLLibrary> library = [p.device newLibraryWithSource:@VIDEO_PREPARE_SHADER options:nil error:&error];
        if (!library) return -1;
        p.horizontal = [p.device newComputePipelineStateWithFunction:[library newFunctionWithName:@"horizontal"] error:&error];
        p.vertical = [p.device newComputePipelineStateWithFunction:[library newFunctionWithName:@"vertical_patch"] error:&error];
        if (!p.horizontal || !p.vertical) return -1;
        *out = (__bridge_retained void *)p; return 0;
    }
}
void av_preparer_destroy(void *p) { if (p) { id object = (__bridge_transfer id)p; (void)object; } }
static id<MTLBuffer> upload(id<MTLDevice> device, const void *bytes, size_t length) {
    return [device newBufferWithBytes:bytes length:length options:MTLResourceStorageModeShared];
}
int32_t av_prepare_submit(void *handle, void *surface, const uint32_t *params,
    const uint32_t *xaxis, size_t xsize, const int32_t *xweights, size_t xcount,
    const uint32_t *yaxis, size_t ysize, const int32_t *yweights, size_t ycount, void **out) {
    *out = NULL;
    @autoreleasepool {
        AVPreparer *p = (__bridge AVPreparer *)handle;
        AVPrepared *result = [AVPrepared new];
        void *imported = NULL;
        if (av_metal_import(p.cache, surface, &imported)) return -1;
        result.imported = imported;
        id<MTLTexture> y = (__bridge id<MTLTexture>)av_metal_plane_texture(imported, 0);
        id<MTLTexture> uv = (__bridge id<MTLTexture>)av_metal_plane_texture(imported, 1);
        size_t height = (params[4] & 1) ? params[0] : params[1];
        id<MTLBuffer> temp = [p.device newBufferWithLength:(size_t)params[2] * height * 3 options:MTLResourceStorageModePrivate];
        result.output = [p.device newBufferWithLength:(size_t)params[2] * params[3] * 3 * sizeof(float) options:MTLResourceStorageModeShared];
        id<MTLBuffer> xb = upload(p.device, xaxis, xsize * sizeof(uint32_t)), xw = upload(p.device, xweights, xcount * sizeof(int32_t));
        id<MTLBuffer> yb = upload(p.device, yaxis, ysize * sizeof(uint32_t)), yw = upload(p.device, yweights, ycount * sizeof(int32_t));
        result.command = [p.queue commandBuffer];
        if (!temp || !result.output || !xb || !xw || !yb || !yw || !result.command) return -1;
        id<MTLComputeCommandEncoder> encoder = [result.command computeCommandEncoder];
        if (!encoder) return -1;
        [encoder setComputePipelineState:p.horizontal]; [encoder setTexture:y atIndex:0]; [encoder setTexture:uv atIndex:1];
        [encoder setBuffer:temp offset:0 atIndex:0]; [encoder setBytes:params length:32 atIndex:1];
        [encoder setBuffer:xb offset:0 atIndex:2]; [encoder setBuffer:xw offset:0 atIndex:3];
        [encoder dispatchThreads:MTLSizeMake(params[2], height, 1) threadsPerThreadgroup:MTLSizeMake(8, 8, 1)]; [encoder endEncoding];
        encoder = [result.command computeCommandEncoder]; if (!encoder) return -1;
        [encoder setComputePipelineState:p.vertical]; [encoder setBuffer:temp offset:0 atIndex:0]; [encoder setBuffer:result.output offset:0 atIndex:1];
        [encoder setBytes:params length:32 atIndex:2]; [encoder setBuffer:yb offset:0 atIndex:3]; [encoder setBuffer:yw offset:0 atIndex:4];
        [encoder dispatchThreads:MTLSizeMake(params[2], params[3], 1) threadsPerThreadgroup:MTLSizeMake(8, 8, 1)]; [encoder endEncoding];
        [result.command commit]; *out = (__bridge_retained void *)result; return 0;
    }
}
int av_prepared_poll(void *p) {
    MTLCommandBufferStatus status = ((__bridge AVPrepared *)p).command.status;
    return status == MTLCommandBufferStatusCompleted ? 1 : status == MTLCommandBufferStatusError ? -1 : 0;
}
void *av_prepared_buffer(void *p) { return (__bridge void *)((__bridge AVPrepared *)p).output; }
int32_t av_prepared_copy(void *p, float *out, size_t count) {
    AVPrepared *result = (__bridge AVPrepared *)p;
    if (av_prepared_poll(p) != 1 || count > result.output.length / sizeof(float)) return -1;
    memcpy(out, result.output.contents, count * sizeof(float)); return 0;
}
void av_prepared_destroy(void *p) { if (p) { id object = (__bridge_transfer id)p; (void)object; } }
