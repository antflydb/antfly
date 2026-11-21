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

// Private implementation, included once by metal_kernels.m after its telemetry
// counters. Observe actual MTLBuffer lifetime, including command-buffer retains.
// Owned bytes use MTLResource.allocatedSize; no-copy mappings count logical
// aliased length separately. Neither measurement is physical resident memory.
enum { ANTFLY_BUFFER_OWNED, ANTFLY_BUFFER_INITIALIZED, ANTFLY_BUFFER_ALIAS };
enum {
    ANTFLY_BUFFER_OWNED_LIVE, ANTFLY_BUFFER_OWNED_PEAK,
    ANTFLY_BUFFER_PRIVATE_LIVE, ANTFLY_BUFFER_PRIVATE_PEAK,
    ANTFLY_BUFFER_VISIBLE_LIVE, ANTFLY_BUFFER_VISIBLE_PEAK,
    ANTFLY_BUFFER_ALIAS_LIVE, ANTFLY_BUFFER_ALIAS_PEAK,
    ANTFLY_BUFFER_OWNED_COUNT, ANTFLY_BUFFER_OWNED_COUNT_PEAK,
    ANTFLY_BUFFER_ALIAS_COUNT, ANTFLY_BUFFER_ALIAS_COUNT_PEAK,
    ANTFLY_BUFFER_MEMORY_COUNT
};
static _Atomic uint64_t antfly_buffer_memory[ANTFLY_BUFFER_MEMORY_COUNT];
static char antfly_buffer_observation_key;
// Same exact owning-purpose order as storage_accounting.OwnedPurpose. This is
// immutable producer intent at birth, never inferred from MTLStorageMode.
enum { ANTFLY_PURPOSE_UNCLASSIFIED, ANTFLY_PURPOSE_KV_STATE,
       ANTFLY_PURPOSE_MODEL_CACHE, ANTFLY_PURPOSE_COMMAND_WORKSPACE,
       ANTFLY_PURPOSE_COUNT };
static uint64_t antfly_buffer_purpose_live[ANTFLY_PURPOSE_COUNT];
// All root mutations and interval list updates share one lock. Allocation/free
// paths already operate once per real buffer, never per kernel/tile/element.
// No SDK call, allocator, callback or provider lock runs while this lock is held.
static pthread_mutex_t antfly_buffer_root_mutex = PTHREAD_MUTEX_INITIALIZER;
typedef struct AntflyMetalBufferInterval {
    struct AntflyMetalBufferInterval *next;
    uint64_t peaks[4];
    uint64_t before[4];
    uint64_t purpose_peaks[ANTFLY_PURPOSE_COUNT];
} AntflyMetalBufferInterval;
_Static_assert(sizeof(AntflyMetalBufferInterval) <= 128, "Zig final-address interval ABI capacity");
_Static_assert(_Alignof(AntflyMetalBufferInterval) <= 16, "Zig interval ABI alignment");
size_t termite_metal_buffer_purpose_count(void) { return ANTFLY_PURPOSE_COUNT; }
static AntflyMetalBufferInterval *antfly_buffer_intervals;
static uint64_t antfly_buffer_root_events[4]; // owned alloc/free, alias create/free
static void antfly_buffer_interval_update(void) {
    for (AntflyMetalBufferInterval *node = antfly_buffer_intervals; node != NULL; node = node->next) {
        for (size_t i = 0; i < ANTFLY_PURPOSE_COUNT; ++i)
            if (node->purpose_peaks[i] < antfly_buffer_purpose_live[i]) node->purpose_peaks[i] = antfly_buffer_purpose_live[i];
        for (size_t i = 0; i < 4; ++i) {
            const uint64_t live = atomic_load_explicit(&antfly_buffer_memory[2*i], memory_order_relaxed);
            if (node->peaks[i] < live) node->peaks[i] = live;
        }
    }
}
// Caller-provided final-address storage; no heap allocation or context borrow.
// The fixed ABI intentionally hides C pointer layout from Zig. A failed size
// check creates no subscription and changes no counters.
uintptr_t termite_metal_buffer_interval_begin(void *storage, size_t capacity) {
    if (storage == NULL || capacity < sizeof(AntflyMetalBufferInterval) ||
        (uintptr_t)storage % _Alignof(AntflyMetalBufferInterval) != 0) return 0;
    AntflyMetalBufferInterval *node = storage;
    pthread_mutex_lock(&antfly_buffer_root_mutex);
    for (size_t i = 0; i < 4; ++i) {
        node->peaks[i] = atomic_load_explicit(&antfly_buffer_memory[2*i], memory_order_relaxed);
        node->before[i] = antfly_buffer_root_events[i];
    }
    for (size_t i = 0; i < ANTFLY_PURPOSE_COUNT; ++i) node->purpose_peaks[i] = antfly_buffer_purpose_live[i];
    node->next = antfly_buffer_intervals;
    antfly_buffer_intervals = node;
    pthread_mutex_unlock(&antfly_buffer_root_mutex);
    return (uintptr_t)&antfly_buffer_memory;
}
static void antfly_buffer_interval_copy(AntflyMetalBufferInterval *node, uint64_t *values) {
    for (size_t i = 0; i < 4; ++i) {
        values[i] = atomic_load_explicit(&antfly_buffer_memory[2*i], memory_order_relaxed);
        values[4+i] = node->peaks[i];
        values[8+i] = antfly_buffer_root_events[i] - node->before[i];
    }
    for (size_t i = 0; i < ANTFLY_PURPOSE_COUNT; ++i) {
        values[12+i] = antfly_buffer_purpose_live[i];
        values[12+ANTFLY_PURPOSE_COUNT+i] = node->purpose_peaks[i];
    }
}
void termite_metal_buffer_interval_snapshot(void *storage, uint64_t *values) {
    pthread_mutex_lock(&antfly_buffer_root_mutex);
    antfly_buffer_interval_copy(storage, values);
    pthread_mutex_unlock(&antfly_buffer_root_mutex);
}
void termite_metal_buffer_interval_finish(void *storage, uint64_t *values) {
    AntflyMetalBufferInterval *node = storage;
    pthread_mutex_lock(&antfly_buffer_root_mutex);
    AntflyMetalBufferInterval **link = &antfly_buffer_intervals;
    while (*link != node) { if (*link == NULL) abort(); link = &(*link)->next; }
    *link = node->next;
    antfly_buffer_interval_copy(node, values);
    pthread_mutex_unlock(&antfly_buffer_root_mutex);
}

static void antfly_buffer_memory_add(size_t live, size_t peak, uint64_t bytes) {
    // Live gauges cannot exceed addressable resource memory. Unlike lifetime
    // cumulative counters they must decrease, so they do not saturate.
    const uint64_t current = atomic_fetch_add_explicit(&antfly_buffer_memory[live], bytes, memory_order_relaxed) + bytes;
    uint64_t previous = atomic_load_explicit(&antfly_buffer_memory[peak], memory_order_relaxed);
    while (previous < current && !atomic_compare_exchange_weak_explicit(
        &antfly_buffer_memory[peak], &previous, current, memory_order_relaxed, memory_order_relaxed)) {}
}

typedef struct { uint64_t bytes; size_t category; BOOL alias; } AntflyObservationLayoutBefore;
typedef struct { uint64_t bytes; size_t category; BOOL alias; uint8_t purpose; } AntflyObservationLayoutAfter;
_Static_assert(sizeof(AntflyObservationLayoutBefore) == sizeof(AntflyObservationLayoutAfter), "purpose uses existing observation ivar padding");

@interface AntflyMetalBufferObservation : NSObject {
@public
    uint64_t bytes;
    size_t category;
    BOOL alias;
    uint8_t purpose;
}
@end
@implementation AntflyMetalBufferObservation
- (void)dealloc {
    pthread_mutex_lock(&antfly_buffer_root_mutex);
    antfly_buffer_root_events[alias ? 3 : 1] += 1;
    atomic_fetch_sub_explicit(&antfly_buffer_memory[category], bytes, memory_order_relaxed);
    if (alias) {
        atomic_fetch_sub_explicit(&antfly_buffer_memory[ANTFLY_BUFFER_ALIAS_COUNT], 1, memory_order_relaxed);
        termite_metal_telemetry_add(TERMITE_METAL_ALIAS_RELEASES, 1);
        termite_metal_telemetry_add(TERMITE_METAL_ALIAS_BYTES_RELEASED, bytes);
    } else {
        if (antfly_buffer_purpose_live[purpose] < bytes) abort();
        antfly_buffer_purpose_live[purpose] -= bytes;
        atomic_fetch_sub_explicit(&antfly_buffer_memory[ANTFLY_BUFFER_OWNED_LIVE], bytes, memory_order_relaxed);
        atomic_fetch_sub_explicit(&antfly_buffer_memory[ANTFLY_BUFFER_OWNED_COUNT], 1, memory_order_relaxed);
        termite_metal_telemetry_add(TERMITE_METAL_BUFFER_RELEASES, 1);
        termite_metal_telemetry_add(TERMITE_METAL_BUFFER_BYTES_RELEASED, bytes);
    }
    antfly_buffer_interval_update();
    pthread_mutex_unlock(&antfly_buffer_root_mutex);
}
@end

// Preserve the +1 ownership of a newBuffer expression, including callers with
// no autorelease pool. Existing buffers passed for duplicate observation are
// retained/consumed by ARC in the same way.
static id<MTLBuffer> antfly_observe_metal_buffer_purpose(id<MTLBuffer> __attribute__((ns_consumed)) buffer, int kind, uint8_t purpose) NS_RETURNS_RETAINED;
static id<MTLBuffer> antfly_observe_metal_buffer(id<MTLBuffer> __attribute__((ns_consumed)) buffer, int kind) NS_RETURNS_RETAINED;
static id<MTLBuffer> antfly_observe_metal_buffer(id<MTLBuffer> __attribute__((ns_consumed)) buffer, int kind) {
    return antfly_observe_metal_buffer_purpose(buffer, kind, ANTFLY_PURPOSE_UNCLASSIFIED);
}
static id<MTLBuffer> antfly_observe_metal_buffer_purpose(id<MTLBuffer> __attribute__((ns_consumed)) buffer, int kind, uint8_t purpose) {
    if (buffer == nil || objc_getAssociatedObject(buffer, &antfly_buffer_observation_key) != nil) return buffer;
    AntflyMetalBufferObservation *owner = [AntflyMetalBufferObservation new];
    owner->alias = kind == ANTFLY_BUFFER_ALIAS;
    owner->purpose = purpose < ANTFLY_PURPOSE_COUNT ? purpose : ANTFLY_PURPOSE_UNCLASSIFIED;
    owner->bytes = owner->alias ? buffer.length : buffer.allocatedSize;
    owner->category = owner->alias ? ANTFLY_BUFFER_ALIAS_LIVE :
        (buffer.storageMode == MTLStorageModePrivate ? ANTFLY_BUFFER_PRIVATE_LIVE : ANTFLY_BUFFER_VISIBLE_LIVE);
    const uint64_t initialization_bytes = kind == ANTFLY_BUFFER_INITIALIZED ? buffer.length : 0;
    pthread_mutex_lock(&antfly_buffer_root_mutex);
    antfly_buffer_root_events[owner->alias ? 2 : 0] += 1;
    antfly_buffer_memory_add(owner->category, owner->category + 1, owner->bytes);
    if (owner->alias) {
        antfly_buffer_memory_add(ANTFLY_BUFFER_ALIAS_COUNT, ANTFLY_BUFFER_ALIAS_COUNT_PEAK, 1);
        termite_metal_telemetry_add(TERMITE_METAL_ALIAS_CREATES, 1);
        termite_metal_telemetry_add(TERMITE_METAL_ALIAS_BYTES_REGISTERED, owner->bytes);
    } else {
        antfly_buffer_purpose_live[owner->purpose] += owner->bytes;
        antfly_buffer_memory_add(ANTFLY_BUFFER_OWNED_LIVE, ANTFLY_BUFFER_OWNED_PEAK, owner->bytes);
        antfly_buffer_memory_add(ANTFLY_BUFFER_OWNED_COUNT, ANTFLY_BUFFER_OWNED_COUNT_PEAK, 1);
        termite_metal_telemetry_add(TERMITE_METAL_BUFFER_ALLOCATIONS, 1);
        termite_metal_telemetry_add(TERMITE_METAL_BUFFER_BYTES_ALLOCATED, owner->bytes);
        if (kind == ANTFLY_BUFFER_INITIALIZED) {
            termite_metal_telemetry_add(TERMITE_METAL_BUFFER_INITIALIZATION_BYTES, initialization_bytes);
            termite_metal_telemetry_add(TERMITE_METAL_BUFFER_INITIALIZATION_CALLS, 1);
        }
    }
    antfly_buffer_interval_update();
    pthread_mutex_unlock(&antfly_buffer_root_mutex);
    // The token has no reference to its buffer. Retained views/commands keep the
    // association alive; releasing one caller handle never decrements early.
    objc_setAssociatedObject(buffer, &antfly_buffer_observation_key, owner, OBJC_ASSOCIATION_RETAIN_NONATOMIC);
    return buffer;
}

void termite_metal_buffer_memory_snapshot(uint64_t *values) {
    if (values == NULL) return;
    pthread_mutex_lock(&antfly_buffer_root_mutex);
    for (size_t i = 0; i < ANTFLY_BUFFER_MEMORY_COUNT; ++i)
        values[i] = atomic_load_explicit(&antfly_buffer_memory[i], memory_order_relaxed);
    pthread_mutex_unlock(&antfly_buffer_root_mutex);
}

// Device oracle: actual rounded resources, independently retained views,
// no-copy mappings and concurrent destruction. Never called by production.
int termite_metal_buffer_observation_check(void) {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (device == nil) return 1;
        const size_t page = (size_t)getpagesize();
        void *mapped = NULL;
        if (posix_memalign(&mapped, page, page) != 0) return -1;
        memset(mapped, 7, page);
        uint64_t before[ANTFLY_BUFFER_MEMORY_COUNT], during[ANTFLY_BUFFER_MEMORY_COUNT], after[ANTFLY_BUFFER_MEMORY_COUNT];
        uint64_t counters_before[TERMITE_METAL_TELEMETRY_COUNT], counters_after[TERMITE_METAL_TELEMETRY_COUNT];
        termite_metal_buffer_memory_snapshot(before);
        termite_metal_telemetry_snapshot(counters_before);
        AntflyMetalBufferInterval outer_interval, inner_interval;
        uint64_t outer_values[12+2*ANTFLY_PURPOSE_COUNT], inner_values[12+2*ANTFLY_PURPOSE_COUNT];
        if (termite_metal_buffer_interval_begin(&outer_interval, sizeof(outer_interval)) == 0) return -9;
        // Size failure must not allocate/link a node, including with an active root.
        if (termite_metal_buffer_interval_begin(&inner_interval, 0) != 0) abort();
        if (termite_metal_buffer_interval_begin(&inner_interval, sizeof(inner_interval)) == 0) abort();
        uint64_t purpose_before[ANTFLY_PURPOSE_COUNT];
        termite_metal_buffer_interval_snapshot(&outer_interval, outer_values);
        for (size_t i = 0; i < ANTFLY_PURPOSE_COUNT; ++i) purpose_before[i] = outer_values[12+i];
        int status = 0;
        uint64_t owned = 0, visible = 0, private_bytes = 0;
        __attribute__((objc_precise_lifetime)) id<MTLBuffer> survivor = nil;
        @autoreleasepool {
            __attribute__((objc_precise_lifetime)) id<MTLBuffer> shared = antfly_observe_metal_buffer_purpose(
                [device newBufferWithBytes:mapped length:page options:MTLResourceStorageModeShared], ANTFLY_BUFFER_INITIALIZED, ANTFLY_PURPOSE_MODEL_CACHE);
            __attribute__((objc_precise_lifetime)) id<MTLBuffer> private_buffer = antfly_observe_metal_buffer_purpose(
                [device newBufferWithLength:3 * page options:MTLResourceStorageModePrivate], ANTFLY_BUFFER_OWNED, ANTFLY_PURPOSE_COMMAND_WORKSPACE);
            __attribute__((objc_precise_lifetime)) id<MTLBuffer> alias_buffer = antfly_observe_metal_buffer(
                [device newBufferWithBytesNoCopy:mapped length:page options:MTLResourceStorageModeShared deallocator:nil], ANTFLY_BUFFER_ALIAS);
            if (shared == nil || private_buffer == nil || alias_buffer == nil) status = -2;
            visible = shared.allocatedSize;
            private_bytes = private_buffer.allocatedSize;
            owned = visible + private_bytes;
            survivor = shared;
            // Duplicate observation must not count another allocation/owner.
            (void)antfly_observe_metal_buffer_purpose(shared, ANTFLY_BUFFER_OWNED, ANTFLY_PURPOSE_COMMAND_WORKSPACE);
            termite_metal_buffer_memory_snapshot(during);
            if (during[ANTFLY_BUFFER_OWNED_LIVE] != before[ANTFLY_BUFFER_OWNED_LIVE] + owned ||
                during[ANTFLY_BUFFER_PRIVATE_LIVE] != before[ANTFLY_BUFFER_PRIVATE_LIVE] + private_bytes ||
                during[ANTFLY_BUFFER_VISIBLE_LIVE] != before[ANTFLY_BUFFER_VISIBLE_LIVE] + visible ||
                during[ANTFLY_BUFFER_ALIAS_LIVE] != before[ANTFLY_BUFFER_ALIAS_LIVE] + page ||
                during[ANTFLY_BUFFER_OWNED_COUNT] != before[ANTFLY_BUFFER_OWNED_COUNT] + 2 ||
                during[ANTFLY_BUFFER_ALIAS_COUNT] != before[ANTFLY_BUFFER_ALIAS_COUNT] + 1 ||
                during[ANTFLY_BUFFER_OWNED_PEAK] < during[ANTFLY_BUFFER_OWNED_LIVE]) status = -3;
        }
        termite_metal_buffer_memory_snapshot(after);
        if (after[ANTFLY_BUFFER_OWNED_LIVE] != before[ANTFLY_BUFFER_OWNED_LIVE] + visible ||
            after[ANTFLY_BUFFER_ALIAS_LIVE] != before[ANTFLY_BUFFER_ALIAS_LIVE] ||
            after[ANTFLY_BUFFER_PRIVATE_LIVE] != before[ANTFLY_BUFFER_PRIVATE_LIVE]) status = -4;
        termite_metal_buffer_interval_finish(&inner_interval, inner_values);
        if (inner_values[0] != before[ANTFLY_BUFFER_OWNED_LIVE] + visible ||
            inner_values[4] != before[ANTFLY_BUFFER_OWNED_LIVE] + owned ||
            inner_values[8] != 2 || inner_values[9] != 1) status = -10;
        if (inner_values[12+ANTFLY_PURPOSE_MODEL_CACHE] != purpose_before[ANTFLY_PURPOSE_MODEL_CACHE] + visible ||
            inner_values[12+ANTFLY_PURPOSE_COMMAND_WORKSPACE] != purpose_before[ANTFLY_PURPOSE_COMMAND_WORKSPACE] ||
            inner_values[12+ANTFLY_PURPOSE_COUNT+ANTFLY_PURPOSE_MODEL_CACHE] < purpose_before[ANTFLY_PURPOSE_MODEL_CACHE] + visible ||
            inner_values[12+ANTFLY_PURPOSE_COUNT+ANTFLY_PURPOSE_COMMAND_WORKSPACE] < purpose_before[ANTFLY_PURPOSE_COMMAND_WORKSPACE] + private_bytes) status = -12;
        uint64_t partition = 0;
        for (size_t i = 0; i < ANTFLY_PURPOSE_COUNT; ++i) partition += inner_values[12+i];
        if (partition != inner_values[0]) status = -13;
        survivor = nil;
        free(mapped);
        termite_metal_buffer_memory_snapshot(after);
        for (size_t i = 0; i < ANTFLY_BUFFER_MEMORY_COUNT; i += 2)
            if (after[i] != before[i]) status = -5;
        termite_metal_telemetry_snapshot(counters_after);
        if (counters_after[TERMITE_METAL_BUFFER_ALLOCATIONS] - counters_before[TERMITE_METAL_BUFFER_ALLOCATIONS] != 2 ||
            counters_after[TERMITE_METAL_BUFFER_RELEASES] - counters_before[TERMITE_METAL_BUFFER_RELEASES] != 2 ||
            counters_after[TERMITE_METAL_BUFFER_BYTES_ALLOCATED] - counters_before[TERMITE_METAL_BUFFER_BYTES_ALLOCATED] != owned ||
            counters_after[TERMITE_METAL_BUFFER_BYTES_RELEASED] - counters_before[TERMITE_METAL_BUFFER_BYTES_RELEASED] != owned ||
            counters_after[TERMITE_METAL_ALIAS_CREATES] - counters_before[TERMITE_METAL_ALIAS_CREATES] != 1 ||
            counters_after[TERMITE_METAL_ALIAS_RELEASES] - counters_before[TERMITE_METAL_ALIAS_RELEASES] != 1 ||
            counters_after[TERMITE_METAL_ALIAS_BYTES_REGISTERED] - counters_before[TERMITE_METAL_ALIAS_BYTES_REGISTERED] != page ||
            counters_after[TERMITE_METAL_ALIAS_BYTES_RELEASED] - counters_before[TERMITE_METAL_ALIAS_BYTES_RELEASED] != page ||
            counters_after[TERMITE_METAL_BUFFER_INITIALIZATION_BYTES] - counters_before[TERMITE_METAL_BUFFER_INITIALIZATION_BYTES] != page ||
            counters_after[TERMITE_METAL_BUFFER_INITIALIZATION_CALLS] - counters_before[TERMITE_METAL_BUFFER_INITIALIZATION_CALLS] != 1) status = -6;
        if (status != 0) { termite_metal_buffer_interval_finish(&outer_interval, outer_values); return status; }
        dispatch_apply(32, dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^(size_t index) {
            @autoreleasepool {
                __attribute__((objc_precise_lifetime)) id<MTLBuffer> buffer = antfly_observe_metal_buffer(
                    [device newBufferWithLength:page + index options:MTLResourceStorageModeShared], ANTFLY_BUFFER_OWNED);
                if (buffer == nil) abort();
            }
        });
        termite_metal_buffer_interval_finish(&outer_interval, outer_values);
        for (size_t i = 0; i < ANTFLY_PURPOSE_COUNT; ++i)
            if (outer_values[12+i] != purpose_before[i]) return -14;
        if (outer_values[0] != before[ANTFLY_BUFFER_OWNED_LIVE] ||
            outer_values[4] < before[ANTFLY_BUFFER_OWNED_LIVE] + owned ||
            outer_values[8] != 34 || outer_values[9] != 34 ||
            outer_values[10] != 1 || outer_values[11] != 1) return -11;
        termite_metal_buffer_memory_snapshot(after);
        for (size_t i = 0; i < ANTFLY_BUFFER_MEMORY_COUNT; i += 2)
            if (after[i] != before[i]) return -7;
        termite_metal_telemetry_snapshot(counters_after);
        if (counters_after[TERMITE_METAL_BUFFER_ALLOCATIONS] - counters_before[TERMITE_METAL_BUFFER_ALLOCATIONS] != 34 ||
            counters_after[TERMITE_METAL_BUFFER_RELEASES] - counters_before[TERMITE_METAL_BUFFER_RELEASES] != 34) return -8;
        return 0;
    }
}
