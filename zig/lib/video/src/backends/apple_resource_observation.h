// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// Private, included once by apple_prepare.m. Authored video-owned buffers and
// textures only: CoreVideo cache/plane wrappers retain external backing and do
// not enter this ledger. Resource tokens never retain their resource/preparer.
#import <objc/runtime.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

enum { AV_OWNED_BUFFER, AV_OWNED_TEXTURE };
static pthread_mutex_t av_owned_resource_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint64_t av_owned_resource_live[3], av_owned_resource_peak[3];
static uint64_t av_owned_resource_events[4]; // buffer alloc/free, texture alloc/free
static uint64_t av_owned_resource_metadata_failures;
static char av_owned_resource_key;
typedef struct AVOwnedResourceInterval {
    struct AVOwnedResourceInterval *next;
    uint64_t peaks[3];
    uint64_t before[4];
    uint64_t metadata_failures_before;
} AVOwnedResourceInterval;
static AVOwnedResourceInterval *av_owned_resource_intervals;

static void av_owned_resource_update_peaks(void) {
    for (size_t i = 0; i < 3; ++i) {
        if (av_owned_resource_peak[i] < av_owned_resource_live[i])
            av_owned_resource_peak[i] = av_owned_resource_live[i];
        for (AVOwnedResourceInterval *node = av_owned_resource_intervals; node; node = node->next)
            if (node->peaks[i] < av_owned_resource_live[i]) node->peaks[i] = av_owned_resource_live[i];
    }
}
uintptr_t av_owned_resource_interval_begin(void *storage, size_t capacity) {
    if (!storage || capacity < sizeof(AVOwnedResourceInterval) ||
        (uintptr_t)storage % _Alignof(AVOwnedResourceInterval)) return 0;
    AVOwnedResourceInterval *node = storage;
    pthread_mutex_lock(&av_owned_resource_mutex);
    for (size_t i = 0; i < 3; ++i) node->peaks[i] = av_owned_resource_live[i];
    for (size_t i = 0; i < 4; ++i) node->before[i] = av_owned_resource_events[i];
    node->metadata_failures_before = av_owned_resource_metadata_failures;
    node->next = av_owned_resource_intervals; av_owned_resource_intervals = node;
    pthread_mutex_unlock(&av_owned_resource_mutex);
    return (uintptr_t)&av_owned_resource_live;
}
static void av_owned_resource_interval_copy(AVOwnedResourceInterval *node, uint64_t *values) {
    for (size_t i = 0; i < 3; ++i) {
        values[i] = av_owned_resource_live[i];
        values[3+i] = node->peaks[i];
        values[6+i] = av_owned_resource_peak[i];
    }
    for (size_t i = 0; i < 4; ++i) values[9+i] = av_owned_resource_events[i] - node->before[i];
    values[13] = av_owned_resource_metadata_failures;
    values[14] = av_owned_resource_metadata_failures - node->metadata_failures_before;
}
void av_owned_resource_interval_snapshot(void *storage, uint64_t *values) {
    pthread_mutex_lock(&av_owned_resource_mutex);
    av_owned_resource_interval_copy(storage, values);
    pthread_mutex_unlock(&av_owned_resource_mutex);
}
void av_owned_resource_interval_finish(void *storage, uint64_t *values) {
    AVOwnedResourceInterval *node = storage;
    pthread_mutex_lock(&av_owned_resource_mutex);
    AVOwnedResourceInterval **link = &av_owned_resource_intervals;
    while (*link != node) { if (!*link) abort(); link = &(*link)->next; }
    *link = node->next;
    av_owned_resource_interval_copy(node, values);
    pthread_mutex_unlock(&av_owned_resource_mutex);
}

@interface AVOwnedResourceObservation : NSObject {
@public
    uint64_t bytes;
    size_t kind;
}
@end
@implementation AVOwnedResourceObservation
- (void)dealloc {
    // No SDK property, caller allocator, callback or owner lock under this lock.
    pthread_mutex_lock(&av_owned_resource_mutex);
    av_owned_resource_live[0] -= bytes;
    av_owned_resource_live[1+kind] -= bytes;
    av_owned_resource_events[2*kind+1] += 1;
    pthread_mutex_unlock(&av_owned_resource_mutex);
}
@end
static id<MTLResource> av_observe_owned_resource(id<MTLResource> __attribute__((ns_consumed)) resource, size_t kind, bool refuse_metadata) NS_RETURNS_RETAINED;
static id<MTLResource> av_observe_owned_resource(id<MTLResource> __attribute__((ns_consumed)) resource, size_t kind, bool refuse_metadata) {
    if (!resource || objc_getAssociatedObject(resource, &av_owned_resource_key)) return resource;
    // Production passes false. The qualification oracle exercises the exact
    // optional-metadata failure branch with an actual retained SDK resource.
    AVOwnedResourceObservation *owner = refuse_metadata ? nil : [AVOwnedResourceObservation new];
    if (!owner) {
        pthread_mutex_lock(&av_owned_resource_mutex);
        av_owned_resource_metadata_failures += 1;
        pthread_mutex_unlock(&av_owned_resource_mutex);
        return resource; // preserve payload ownership; mark partial coverage
    }
    owner->bytes = resource.allocatedSize; owner->kind = kind;
    pthread_mutex_lock(&av_owned_resource_mutex);
    av_owned_resource_live[0] += owner->bytes;
    av_owned_resource_live[1+kind] += owner->bytes;
    av_owned_resource_events[2*kind] += 1;
    av_owned_resource_update_peaks();
    pthread_mutex_unlock(&av_owned_resource_mutex);
    // Both property query and association are outside the ledger lock. Token
    // association follows actual resource death, including command/view retains.
    objc_setAssociatedObject(resource, &av_owned_resource_key, owner, OBJC_ASSOCIATION_RETAIN_NONATOMIC);
    return resource;
}
static id<MTLBuffer> av_observe_owned_buffer(id<MTLBuffer> __attribute__((ns_consumed)) buffer) NS_RETURNS_RETAINED;
static id<MTLBuffer> av_observe_owned_buffer(id<MTLBuffer> __attribute__((ns_consumed)) buffer) {
    return (id<MTLBuffer>)av_observe_owned_resource(buffer, AV_OWNED_BUFFER, false);
}
static id<MTLTexture> av_observe_owned_texture(id<MTLTexture> __attribute__((ns_consumed)) texture) NS_RETURNS_RETAINED;
static id<MTLTexture> av_observe_owned_texture(id<MTLTexture> __attribute__((ns_consumed)) texture) {
    return (id<MTLTexture>)av_observe_owned_resource(texture, AV_OWNED_TEXTURE, false);
}

// Actual SDK resource lifetime oracle, never called by production. It covers
// nested subscriptions, independent strong owners, concurrent deallocation and
// optional metadata OOM without altering numerical preparation schedules.
int32_t av_owned_resource_observation_check(void *raw_device) {
    id<MTLDevice> device = (__bridge id<MTLDevice>)raw_device;
    if (!device) return -1;
    @autoreleasepool {
        AVOwnedResourceInterval outer, inner;
        uint64_t start[15], held[15], end[15], child[15];
        if (!av_owned_resource_interval_begin(&outer, sizeof(outer))) return -2;
        av_owned_resource_interval_snapshot(&outer, start);
        if (av_owned_resource_interval_begin(&inner, 0)) abort();
        if (!av_owned_resource_interval_begin(&inner, sizeof(inner))) abort();
        __attribute__((objc_precise_lifetime)) id<MTLBuffer> survivor = nil;
        __attribute__((objc_precise_lifetime)) id<MTLTexture> texture_survivor = nil;
        uint64_t buffer_bytes = 0, texture_bytes = 0;
        int32_t status = 0;
        @autoreleasepool {
            __attribute__((objc_precise_lifetime)) id<MTLBuffer> buffer = av_observe_owned_buffer([device newBufferWithLength:4096 options:MTLResourceStorageModeShared]);
            MTLTextureDescriptor *desc = [MTLTextureDescriptor texture2DDescriptorWithPixelFormat:MTLPixelFormatR8Uint width:32 height:24 mipmapped:NO];
            desc.storageMode = MTLStorageModePrivate; desc.usage = MTLTextureUsageShaderRead;
            __attribute__((objc_precise_lifetime)) id<MTLTexture> texture = av_observe_owned_texture([device newTextureWithDescriptor:desc]);
            if (!buffer || !texture) status = -3;
            buffer_bytes = buffer.allocatedSize; texture_bytes = texture.allocatedSize;
            survivor = buffer; texture_survivor = texture;
            // Duplicate attachment changes neither authority nor byte/event count.
            (void)av_observe_owned_buffer(buffer);
        }
        av_owned_resource_interval_finish(&inner, child);
        if (child[0] != start[0]+buffer_bytes+texture_bytes || child[9] != 1 || child[11] != 1 || child[10] || child[12]) status = -4;
        survivor = nil; texture_survivor = nil;
        av_owned_resource_interval_snapshot(&outer, held);
        if (held[0] != start[0] || held[10] != 1 || held[12] != 1) status = -5;
        dispatch_apply(16, dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^(size_t index) {
            @autoreleasepool {
                __attribute__((objc_precise_lifetime)) id<MTLBuffer> item = av_observe_owned_buffer([device newBufferWithLength:4096+index options:MTLResourceStorageModeShared]);
                if (!item) abort();
            }
        });
        @autoreleasepool {
            __attribute__((objc_precise_lifetime)) id<MTLBuffer> unobserved = (id<MTLBuffer>)av_observe_owned_resource([device newBufferWithLength:16 options:MTLResourceStorageModeShared], AV_OWNED_BUFFER, true);
            if (!unobserved) status = -6;
            // Metadata failure preserves the actual payload and its ownership.
            memset(unobserved.contents, 3, 16);
        }
        av_owned_resource_interval_finish(&outer, end);
        if (end[0] != start[0] || end[3] < start[0]+buffer_bytes+texture_bytes ||
            end[9] != 17 || end[10] != 17 || end[11] != 1 || end[12] != 1 || end[14] != 1) status = -7;
        return status;
    }
}
