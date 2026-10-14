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

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <objc/runtime.h>
#include <stdatomic.h>
#include <stdio.h>
#include <unistd.h>

// Diagnostic only: sample actual worker Metal allocations after waits. The
// original synchronization runs first; no buffers, kernels or choices change.
// Build: clang -dynamiclib -fobjc-arc -framework Foundation -framework Metal
//        metal_memory_probe.m -o /tmp/metal-memory-probe.dylib
static void (*original_wait)(id, SEL);
static _Atomic uint64_t wait_count;
static void sampled_wait(id object, SEL selector) {
    original_wait(object, selector);
    const uint64_t count = atomic_fetch_add(&wait_count, 1) + 1;
    if (count != 1 && count % 64 != 0) return;
    id<MTLCommandBuffer> command = object;
    fprintf(stderr, "metal-memory-probe pid=%d bytes=%llu waits=%llu\n", getpid(), (unsigned long long)command.device.currentAllocatedSize, (unsigned long long)count);
}

__attribute__((constructor)) static void install_memory_probe(void) {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        id<MTLCommandQueue> queue = [device newCommandQueue];
        id<MTLCommandBuffer> command = [queue commandBuffer];
        if (!command) return;
        Method method = class_getInstanceMethod(object_getClass(command), @selector(waitUntilCompleted));
        if (!method) return;
        original_wait = (void (*)(id, SEL))method_getImplementation(method);
        method_setImplementation(method, (IMP)sampled_wait);
    }
}
