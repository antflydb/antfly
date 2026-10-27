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

// Test-process-only observer for GPU command-buffer timing in MLX benchmarks.
// Compile as a dylib and load before importing MLX. Never linked into Antfly.
// clang -O2 -dynamiclib -fobjc-arc -framework Foundation -framework Metal \
//   metal_command_timing.m -o /path/in/worktree/metal_command_timing.dylib
#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <objc/runtime.h>
#include <pthread.h>
static pthread_mutex_t guard = PTHREAD_MUTEX_INITIALIZER;
static NSMutableArray *pending;
static bool active;
static void (*original_commit)(id, SEL);
static id (*original_compute)(id, SEL);
static id (*original_compute_type)(id, SEL, MTLDispatchType);
static id (*original_blit)(id, SEL);
static char encoded_work;
static uint64_t empty_count;
static id measured_compute(id object, SEL selector) {
    objc_setAssociatedObject(object, &encoded_work, @YES, OBJC_ASSOCIATION_RETAIN_NONATOMIC);
    return original_compute(object, selector);
}
static id measured_compute_type(id object, SEL selector, MTLDispatchType type) {
    objc_setAssociatedObject(object, &encoded_work, @YES, OBJC_ASSOCIATION_RETAIN_NONATOMIC);
    return original_compute_type(object, selector, type);
}
static id measured_blit(id object, SEL selector) {
    objc_setAssociatedObject(object, &encoded_work, @YES, OBJC_ASSOCIATION_RETAIN_NONATOMIC);
    return original_blit(object, selector);
}
static void measured_commit(id object, SEL selector) {
    pthread_mutex_lock(&guard);
    if (active) [pending addObject:object];
    pthread_mutex_unlock(&guard);
    original_commit(object, selector);
}
int ocr_timing_init(void) {
    @autoreleasepool {
        id<MTLDevice> device=MTLCreateSystemDefaultDevice();
        id<MTLCommandQueue> queue=[device newCommandQueue];
        id<MTLCommandBuffer> cb=[queue commandBuffer];
        if (!cb) return -1;
        Method method=class_getInstanceMethod(object_getClass(cb), @selector(commit));
        if (!method || original_commit) return -2;
        original_commit=(void (*)(id, SEL))method_setImplementation(method, (IMP)measured_commit);
        Method compute=class_getInstanceMethod(object_getClass(cb), @selector(computeCommandEncoder));
        Method typed=class_getInstanceMethod(object_getClass(cb), @selector(computeCommandEncoderWithDispatchType:));
        Method blit=class_getInstanceMethod(object_getClass(cb), @selector(blitCommandEncoder));
        if (!compute || !typed || !blit) return -3;
        original_compute=(id (*)(id, SEL))method_setImplementation(compute, (IMP)measured_compute);
        original_compute_type=(id (*)(id, SEL, MTLDispatchType))method_setImplementation(typed, (IMP)measured_compute_type);
        original_blit=(id (*)(id, SEL))method_setImplementation(blit, (IMP)measured_blit);
        pending=[NSMutableArray new];
        return 0;
    }
}
void ocr_timing_begin(void) {
    pthread_mutex_lock(&guard);
    [pending removeAllObjects];
    empty_count=0;
    active=true;
    pthread_mutex_unlock(&guard);
}
uint64_t ocr_timing_end(uint64_t *count) {
    @autoreleasepool {
        pthread_mutex_lock(&guard);
        active=false;
        NSArray *buffers=[pending copy];
        [pending removeAllObjects];
        pthread_mutex_unlock(&guard);
        uint64_t nanos=0;
        *count=0;
        for (id<MTLCommandBuffer> cb in buffers) {
            [cb waitUntilCompleted];
            if (cb.status!=MTLCommandBufferStatusCompleted) return 0;
            if (cb.GPUEndTime<=cb.GPUStartTime) {
                // MLX residency can commit empty synchronization buffers. Only
                // skip equal timestamps when no compute/blit encoder was made;
                // missing timestamps on actual GPU work remain an error.
                if (cb.GPUEndTime==cb.GPUStartTime &&
                    objc_getAssociatedObject(cb, &encoded_work)==nil) { empty_count++; continue; }
                return 0;
            }
            nanos+=(uint64_t)((cb.GPUEndTime-cb.GPUStartTime)*1e9);
            *count+=1;
        }
        return nanos;
    }
}

uint64_t ocr_timing_empty_count(void) { return empty_count; }

// Qualify both compute-encoder entry points and blits before accepting timing.
int ocr_timing_self_test(void) {
    @autoreleasepool {
        id<MTLDevice> device=MTLCreateSystemDefaultDevice();
        id<MTLCommandQueue> queue=[device newCommandQueue];
        NSError *error=nil;
        NSString *source=@"#include <metal_stdlib>\nusing namespace metal;kernel void probe(device uint*y[[buffer(0)]],uint i[[thread_position_in_grid]]){y[i]=i+17;}";
        id<MTLLibrary> library=[device newLibraryWithSource:source options:nil error:&error];
        id<MTLComputePipelineState> pipeline=[device newComputePipelineStateWithFunction:[library newFunctionWithName:@"probe"] error:&error];
        if (!pipeline || error) return -1;
        id<MTLBuffer> first=[device newBufferWithLength:1024 options:MTLResourceStorageModeShared];
        id<MTLBuffer> second=[device newBufferWithLength:1024 options:MTLResourceStorageModeShared];
        id<MTLBuffer> third=[device newBufferWithLength:1024 options:MTLResourceStorageModeShared];
        if (!first || !second || !third) return -2;
        ocr_timing_begin();
        id<MTLCommandBuffer> empty=[queue commandBuffer]; [empty commit];
        for (size_t mode=0;mode<2;mode++) {
            id<MTLCommandBuffer> cb=[queue commandBuffer];
            id<MTLComputeCommandEncoder> enc=mode ? [cb computeCommandEncoderWithDispatchType:MTLDispatchTypeSerial] : [cb computeCommandEncoder];
            [enc setComputePipelineState:pipeline];[enc setBuffer:mode ? second : first offset:0 atIndex:0];
            [enc dispatchThreads:MTLSizeMake(256,1,1) threadsPerThreadgroup:MTLSizeMake(32,1,1)];
            [enc endEncoding];[cb commit];
        }
        id<MTLCommandBuffer> copy=[queue commandBuffer];
        id<MTLBlitCommandEncoder> blit=[copy blitCommandEncoder];
        [blit copyFromBuffer:first sourceOffset:0 toBuffer:third destinationOffset:0 size:1024];
        [blit endEncoding];[copy commit];
        uint64_t count=0,nanos=ocr_timing_end(&count);
        if (nanos==0 || count<3) return -3;
        for (size_t i=0;i<256;i++) if (((uint32_t*)first.contents)[i]!=i+17 ||
            ((uint32_t*)second.contents)[i]!=i+17 || ((uint32_t*)third.contents)[i]!=i+17) return -4;
        return 0;
    }
}
