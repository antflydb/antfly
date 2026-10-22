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
        pending=[NSMutableArray new];
        return 0;
    }
}
void ocr_timing_begin(void) {
    pthread_mutex_lock(&guard);
    [pending removeAllObjects];
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
            if (cb.status!=MTLCommandBufferStatusCompleted || cb.GPUEndTime<=cb.GPUStartTime) return 0;
            nanos+=(uint64_t)((cb.GPUEndTime-cb.GPUStartTime)*1e9);
            *count+=1;
        }
        return nanos;
    }
}
