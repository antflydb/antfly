// Copyright 2026 Antfly, Inc.
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
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct antfly_kmeans_metal_state {
    id<MTLDevice> device;
    id<MTLCommandQueue> queue;
    id<MTLComputePipelineState> assign_l2_pipeline;
    id<MTLComputePipelineState> update_partials_pipeline;
    id<MTLComputePipelineState> update_finalize_pipeline;
} antfly_kmeans_metal_state;

typedef struct antfly_kmeans_params {
    uint32_t point_count;
    uint32_t cluster_count;
    uint32_t dims;
    int32_t metric;
    uint32_t update_block_size;
    uint32_t update_block_count;
} antfly_kmeans_params;

static const uint32_t antfly_kmeans_update_block_size = 256;

@interface AntflyKmeansMetalContext : NSObject
@property(nonatomic, strong) id<MTLBuffer> pointsBuffer;
@property(nonatomic, strong) id<MTLBuffer> centroidsBuffer;
@property(nonatomic, strong) id<MTLBuffer> nextCentroidsBuffer;
@property(nonatomic, strong) id<MTLBuffer> assignmentsBuffer;
@property(nonatomic, strong) id<MTLBuffer> distancesBuffer;
@property(nonatomic, strong) id<MTLBuffer> countsBuffer;
@property(nonatomic, strong) id<MTLBuffer> partialSumsBuffer;
@property(nonatomic, strong) id<MTLBuffer> partialCountsBuffer;
@property(nonatomic, strong) id<MTLBuffer> paramsBuffer;
@property(nonatomic) uint32_t pointCount;
@property(nonatomic) uint32_t maxClusterCount;
@property(nonatomic) uint32_t dims;
@property(nonatomic) uint32_t updateBlockCount;
@end

@implementation AntflyKmeansMetalContext
@end

static antfly_kmeans_metal_state g_state;

static void antfly_kmeans_metal_debug_command_error(const char *phase, id<MTLCommandBuffer> command_buffer) {
    if (getenv("ANTFLY_KMEANS_METAL_DEBUG") == NULL || command_buffer == nil) return;
    NSError *error = command_buffer.error;
    if (error == nil) {
        fprintf(stderr, "antfly_kmeans_metal phase=%s status=%lu error=nil\n", phase, (unsigned long)command_buffer.status);
        return;
    }
    NSString *description = error.localizedDescription ?: @"";
    NSString *reason = error.localizedFailureReason ?: @"";
    fprintf(
        stderr,
        "antfly_kmeans_metal phase=%s status=%lu domain=%s code=%ld description=%s reason=%s\n",
        phase,
        (unsigned long)command_buffer.status,
        error.domain.UTF8String ?: "",
        (long)error.code,
        description.UTF8String ?: "",
        reason.UTF8String ?: ""
    );
}

static int antfly_kmeans_metal_debug_enabled(void) {
    return getenv("ANTFLY_KMEANS_METAL_DEBUG") != NULL;
}

static void antfly_kmeans_metal_debug_context_create_failure(
    const char *reason,
    uint32_t point_count,
    uint32_t max_cluster_count,
    uint32_t dims,
    NSUInteger points_bytes,
    NSUInteger centroids_bytes,
    NSUInteger partial_sums_bytes
) {
    if (!antfly_kmeans_metal_debug_enabled()) return;
    fprintf(
        stderr,
        "antfly_kmeans_metal context_create_failed reason=%s points=%u clusters=%u dims=%u points_bytes=%llu centroids_bytes=%llu partial_sums_bytes=%llu max_buffer_length=%llu\n",
        reason,
        point_count,
        max_cluster_count,
        dims,
        (unsigned long long)points_bytes,
        (unsigned long long)centroids_bytes,
        (unsigned long long)partial_sums_bytes,
        g_state.device == nil ? 0ULL : (unsigned long long)g_state.device.maxBufferLength
    );
}

static NSString *antfly_kmeans_metal_source(void) {
    return @"#include <metal_stdlib>\n"
           "using namespace metal;\n"
           "struct Params { uint point_count; uint cluster_count; uint dims; int metric; uint update_block_size; uint update_block_count; };\n"
           "kernel void kmeans_assign(device const float* points [[buffer(0)]],\n"
           "                         device const float* centroids [[buffer(1)]],\n"
           "                         device uint* assignments [[buffer(2)]],\n"
           "                         device float* distances [[buffer(3)]],\n"
           "                         constant Params& p [[buffer(4)]],\n"
           "                         uint gid [[thread_position_in_grid]]) {\n"
           "    if (gid >= p.point_count) return;\n"
           "    const uint point_base = gid * p.dims;\n"
           "    float best = 3.402823466e+38F;\n"
           "    uint best_cluster = 0;\n"
           "    for (uint cluster = 0; cluster < p.cluster_count; cluster++) {\n"
           "        const uint centroid_base = cluster * p.dims;\n"
           "        float dot = 0.0f;\n"
           "        float point_norm_sq = 0.0f;\n"
           "        float centroid_norm_sq = 0.0f;\n"
           "        float l2 = 0.0f;\n"
           "        for (uint dim = 0; dim < p.dims; dim++) {\n"
           "            const float point = points[point_base + dim];\n"
           "            const float centroid = centroids[centroid_base + dim];\n"
           "            const float delta = point - centroid;\n"
           "            dot += point * centroid;\n"
           "            point_norm_sq += point * point;\n"
           "            centroid_norm_sq += centroid * centroid;\n"
           "            l2 += delta * delta;\n"
           "        }\n"
           "        float distance = l2;\n"
           "        if (p.metric == 1) {\n"
           "            distance = -dot;\n"
           "        } else if (p.metric == 2) {\n"
           "            if (point_norm_sq == 0.0f || centroid_norm_sq == 0.0f) {\n"
           "                distance = 1.0f;\n"
           "            } else {\n"
           "                distance = 1.0f - dot * rsqrt(point_norm_sq * centroid_norm_sq);\n"
           "            }\n"
           "        }\n"
           "        if (distance < best) {\n"
           "            best = distance;\n"
           "            best_cluster = cluster;\n"
           "        }\n"
           "    }\n"
           "    assignments[gid] = best_cluster;\n"
           "    distances[gid] = best;\n"
           "}\n"
           "kernel void kmeans_update_partials(device const float* points [[buffer(0)]],\n"
           "                                   device const uint* assignments [[buffer(1)]],\n"
           "                                   device float* partial_sums [[buffer(2)]],\n"
           "                                   device uint* partial_counts [[buffer(3)]],\n"
           "                                   constant Params& p [[buffer(4)]],\n"
           "                                   uint gid [[thread_position_in_grid]]) {\n"
           "    const uint total = p.cluster_count * p.dims * p.update_block_count;\n"
           "    if (gid >= total) return;\n"
           "    const uint block = gid % p.update_block_count;\n"
           "    const uint cluster_dim = gid / p.update_block_count;\n"
           "    const uint cluster = cluster_dim / p.dims;\n"
           "    const uint dim = cluster_dim - cluster * p.dims;\n"
           "    const uint start = block * p.update_block_size;\n"
           "    const uint end = min(start + p.update_block_size, p.point_count);\n"
           "    float sum = 0.0f;\n"
           "    uint count = 0;\n"
           "    for (uint point_idx = start; point_idx < end; point_idx++) {\n"
           "        if (assignments[point_idx] != cluster) continue;\n"
           "        sum += points[point_idx * p.dims + dim];\n"
           "        count += 1;\n"
           "    }\n"
           "    partial_sums[cluster_dim * p.update_block_count + block] = sum;\n"
           "    if (dim == 0) partial_counts[cluster * p.update_block_count + block] = count;\n"
           "}\n"
           "kernel void kmeans_update_finalize(device const float* old_centroids [[buffer(0)]],\n"
           "                                   device const float* partial_sums [[buffer(1)]],\n"
           "                                   device const uint* partial_counts [[buffer(2)]],\n"
           "                                   device float* next_centroids [[buffer(3)]],\n"
           "                                   device uint* counts [[buffer(4)]],\n"
           "                                   constant Params& p [[buffer(5)]],\n"
           "                                   uint gid [[thread_position_in_grid]]) {\n"
           "    const uint total = p.cluster_count * p.dims;\n"
           "    if (gid >= total) return;\n"
           "    const uint cluster = gid / p.dims;\n"
           "    const uint dim = gid - cluster * p.dims;\n"
           "    float sum = 0.0f;\n"
           "    uint count = 0;\n"
           "    for (uint block = 0; block < p.update_block_count; block++) {\n"
           "        sum += partial_sums[gid * p.update_block_count + block];\n"
           "        count += partial_counts[cluster * p.update_block_count + block];\n"
           "    }\n"
           "    if (dim == 0) counts[cluster] = count;\n"
           "    if (count == 0) {\n"
           "        next_centroids[gid] = old_centroids[gid];\n"
           "    } else {\n"
           "        next_centroids[gid] = sum / float(count);\n"
           "    }\n"
           "}\n";
}

static int antfly_kmeans_metal_init(void) {
    if (g_state.assign_l2_pipeline != nil && g_state.update_partials_pipeline != nil && g_state.update_finalize_pipeline != nil) return 1;

    @synchronized([AntflyKmeansMetalContext class]) {
        if (g_state.assign_l2_pipeline != nil && g_state.update_partials_pipeline != nil && g_state.update_finalize_pipeline != nil) return 1;

        @autoreleasepool {
            id<MTLDevice> device = MTLCreateSystemDefaultDevice();
            if (device == nil) {
                NSArray<id<MTLDevice>> *devices = MTLCopyAllDevices();
                if (devices.count > 0) {
                    device = devices[0];
                }
            }
            if (device == nil) {
                if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal init_failed reason=no_device\n");
                return 0;
            }

            NSError *error = nil;
            id<MTLLibrary> library = [device newLibraryWithSource:antfly_kmeans_metal_source() options:nil error:&error];
            if (library == nil) {
                if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal init_failed reason=library error=%s\n", error.localizedDescription.UTF8String ?: "");
                return 0;
            }

            id<MTLFunction> function = [library newFunctionWithName:@"kmeans_assign"];
            if (function == nil) {
                if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal init_failed reason=assign_function\n");
                return 0;
            }
            id<MTLFunction> update_partials_function = [library newFunctionWithName:@"kmeans_update_partials"];
            if (update_partials_function == nil) {
                if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal init_failed reason=update_partials_function\n");
                return 0;
            }
            id<MTLFunction> update_finalize_function = [library newFunctionWithName:@"kmeans_update_finalize"];
            if (update_finalize_function == nil) {
                if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal init_failed reason=update_finalize_function\n");
                return 0;
            }

            id<MTLComputePipelineState> pipeline = [device newComputePipelineStateWithFunction:function error:&error];
            if (pipeline == nil) {
                if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal init_failed reason=assign_pipeline error=%s\n", error.localizedDescription.UTF8String ?: "");
                return 0;
            }
            id<MTLComputePipelineState> update_partials_pipeline = [device newComputePipelineStateWithFunction:update_partials_function error:&error];
            if (update_partials_pipeline == nil) {
                if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal init_failed reason=update_partials_pipeline error=%s\n", error.localizedDescription.UTF8String ?: "");
                return 0;
            }
            id<MTLComputePipelineState> update_finalize_pipeline = [device newComputePipelineStateWithFunction:update_finalize_function error:&error];
            if (update_finalize_pipeline == nil) {
                if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal init_failed reason=update_finalize_pipeline error=%s\n", error.localizedDescription.UTF8String ?: "");
                return 0;
            }

            id<MTLCommandQueue> queue = [device newCommandQueue];
            if (queue == nil) {
                if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal init_failed reason=queue\n");
                return 0;
            }

            g_state.device = device;
            g_state.queue = queue;
            g_state.assign_l2_pipeline = pipeline;
            g_state.update_partials_pipeline = update_partials_pipeline;
            g_state.update_finalize_pipeline = update_finalize_pipeline;
            return 1;
        }
    }
}

int antfly_kmeans_metal_available(void) {
    return antfly_kmeans_metal_init();
}

void *antfly_kmeans_metal_context_create(
    const float *points,
    uint32_t point_count,
    uint32_t max_cluster_count,
    uint32_t dims
) {
    if (points == NULL) {
        if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal context_create_failed reason=null_points\n");
        return NULL;
    }
    if (point_count == 0 || max_cluster_count == 0 || dims == 0) {
        if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal context_create_failed reason=zero_shape points=%u clusters=%u dims=%u\n", point_count, max_cluster_count, dims);
        return NULL;
    }
    if (!antfly_kmeans_metal_init()) {
        if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal context_create_failed reason=init_failed points=%u clusters=%u dims=%u\n", point_count, max_cluster_count, dims);
        return NULL;
    }

    @autoreleasepool {
        const NSUInteger points_bytes = (NSUInteger)point_count * (NSUInteger)dims * sizeof(float);
        const NSUInteger centroids_bytes = (NSUInteger)max_cluster_count * (NSUInteger)dims * sizeof(float);
        const NSUInteger assignments_bytes = (NSUInteger)point_count * sizeof(uint32_t);
        const NSUInteger distances_bytes = (NSUInteger)point_count * sizeof(float);
        const uint32_t update_block_count = (point_count + antfly_kmeans_update_block_size - 1) / antfly_kmeans_update_block_size;
        const NSUInteger partial_sums_bytes = (NSUInteger)max_cluster_count * (NSUInteger)dims * (NSUInteger)update_block_count * sizeof(float);
        const NSUInteger partial_counts_bytes = (NSUInteger)max_cluster_count * (NSUInteger)update_block_count * sizeof(uint32_t);

        AntflyKmeansMetalContext *context = [[AntflyKmeansMetalContext alloc] init];
        if (context == nil) {
            antfly_kmeans_metal_debug_context_create_failure("objc_context_nil", point_count, max_cluster_count, dims, points_bytes, centroids_bytes, partial_sums_bytes);
            return NULL;
        }

        context.pointsBuffer = [g_state.device newBufferWithBytes:points length:points_bytes options:MTLResourceStorageModeShared];
        context.centroidsBuffer = [g_state.device newBufferWithLength:centroids_bytes options:MTLResourceStorageModeShared];
        context.nextCentroidsBuffer = [g_state.device newBufferWithLength:centroids_bytes options:MTLResourceStorageModeShared];
        context.assignmentsBuffer = [g_state.device newBufferWithLength:assignments_bytes options:MTLResourceStorageModeShared];
        context.distancesBuffer = [g_state.device newBufferWithLength:distances_bytes options:MTLResourceStorageModeShared];
        context.countsBuffer = [g_state.device newBufferWithLength:(NSUInteger)max_cluster_count * sizeof(uint32_t) options:MTLResourceStorageModeShared];
        context.partialSumsBuffer = [g_state.device newBufferWithLength:partial_sums_bytes options:MTLResourceStorageModeShared];
        context.partialCountsBuffer = [g_state.device newBufferWithLength:partial_counts_bytes options:MTLResourceStorageModeShared];
        context.paramsBuffer = [g_state.device newBufferWithLength:sizeof(antfly_kmeans_params) options:MTLResourceStorageModeShared];
        if (context.pointsBuffer == nil || context.centroidsBuffer == nil || context.nextCentroidsBuffer == nil || context.assignmentsBuffer == nil || context.distancesBuffer == nil || context.countsBuffer == nil || context.partialSumsBuffer == nil || context.partialCountsBuffer == nil || context.paramsBuffer == nil) {
            antfly_kmeans_metal_debug_context_create_failure("buffer_nil", point_count, max_cluster_count, dims, points_bytes, centroids_bytes, partial_sums_bytes);
            if (antfly_kmeans_metal_debug_enabled()) {
                fprintf(
                    stderr,
                    "antfly_kmeans_metal buffers points=%d centroids=%d next=%d assignments=%d distances=%d counts=%d partial_sums=%d partial_counts=%d params=%d\n",
                    context.pointsBuffer != nil,
                    context.centroidsBuffer != nil,
                    context.nextCentroidsBuffer != nil,
                    context.assignmentsBuffer != nil,
                    context.distancesBuffer != nil,
                    context.countsBuffer != nil,
                    context.partialSumsBuffer != nil,
                    context.partialCountsBuffer != nil,
                    context.paramsBuffer != nil
                );
            }
            return NULL;
        }

        context.pointCount = point_count;
        context.maxClusterCount = max_cluster_count;
        context.dims = dims;
        context.updateBlockCount = update_block_count;
        return (__bridge_retained void *)context;
    }
}

void antfly_kmeans_metal_context_destroy(void *opaque_context) {
    if (opaque_context == NULL) return;
    AntflyKmeansMetalContext *context = (__bridge_transfer AntflyKmeansMetalContext *)opaque_context;
    (void)context;
}

int antfly_kmeans_metal_context_assign(
    void *opaque_context,
    const float *centroids,
    uint32_t cluster_count,
    int32_t metric,
    uint32_t *assignments,
    float *distances
) {
    if (opaque_context == NULL || centroids == NULL || assignments == NULL || distances == NULL) return 1;
    if (cluster_count == 0) return 1;
    if (metric < 0 || metric > 2) return 1;
    if (!antfly_kmeans_metal_init()) return 1;

    @autoreleasepool {
        AntflyKmeansMetalContext *context = (__bridge AntflyKmeansMetalContext *)opaque_context;
        if (cluster_count > context.maxClusterCount || context.pointCount == 0 || context.dims == 0) return 1;

        const NSUInteger centroids_bytes = (NSUInteger)cluster_count * (NSUInteger)context.dims * sizeof(float);
        const NSUInteger assignments_bytes = (NSUInteger)context.pointCount * sizeof(uint32_t);
        const NSUInteger distances_bytes = (NSUInteger)context.pointCount * sizeof(float);
        memcpy(context.centroidsBuffer.contents, centroids, centroids_bytes);

        antfly_kmeans_params params = {
            .point_count = context.pointCount,
            .cluster_count = cluster_count,
            .dims = context.dims,
            .metric = metric,
            .update_block_size = antfly_kmeans_update_block_size,
            .update_block_count = context.updateBlockCount,
        };
        memcpy(context.paramsBuffer.contents, &params, sizeof(params));

        id<MTLCommandBuffer> command_buffer = [g_state.queue commandBuffer];
        if (command_buffer == nil) return 1;
        id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
        if (encoder == nil) return 1;

        [encoder setComputePipelineState:g_state.assign_l2_pipeline];
        [encoder setBuffer:context.pointsBuffer offset:0 atIndex:0];
        [encoder setBuffer:context.centroidsBuffer offset:0 atIndex:1];
        [encoder setBuffer:context.assignmentsBuffer offset:0 atIndex:2];
        [encoder setBuffer:context.distancesBuffer offset:0 atIndex:3];
        [encoder setBuffer:context.paramsBuffer offset:0 atIndex:4];

        NSUInteger width = g_state.assign_l2_pipeline.maxTotalThreadsPerThreadgroup;
        if (width > 256) width = 256;
        if (width == 0) width = 1;
        MTLSize grid = MTLSizeMake(context.pointCount, 1, 1);
        MTLSize group = MTLSizeMake(width, 1, 1);
        [encoder dispatchThreads:grid threadsPerThreadgroup:group];
        [encoder endEncoding];
        [command_buffer commit];
        [command_buffer waitUntilCompleted];
        if (command_buffer.status != MTLCommandBufferStatusCompleted) {
            antfly_kmeans_metal_debug_command_error("assign", command_buffer);
            return 1;
        }

        memcpy(assignments, context.assignmentsBuffer.contents, assignments_bytes);
        memcpy(distances, context.distancesBuffer.contents, distances_bytes);
        return 0;
    }
}

int antfly_kmeans_metal_context_update_centroids(
    void *opaque_context,
    const float *old_centroids,
    uint32_t cluster_count,
    int32_t metric,
    float *next_centroids,
    uint32_t *counts
) {
    if (opaque_context == NULL || old_centroids == NULL || next_centroids == NULL || counts == NULL) {
        if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal update_failed reason=null_arg\n");
        return 1;
    }
    if (cluster_count == 0) {
        if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal update_failed reason=zero_clusters\n");
        return 1;
    }
    if (metric < 0 || metric > 2) {
        if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal update_failed reason=bad_metric metric=%d\n", metric);
        return 1;
    }
    if (!antfly_kmeans_metal_init()) {
        if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal update_failed reason=init_failed\n");
        return 1;
    }

    @autoreleasepool {
        AntflyKmeansMetalContext *context = (__bridge AntflyKmeansMetalContext *)opaque_context;
        if (cluster_count > context.maxClusterCount || context.pointCount == 0 || context.dims == 0) {
            if (antfly_kmeans_metal_debug_enabled()) fprintf(stderr, "antfly_kmeans_metal update_failed reason=bad_context clusters=%u max=%u points=%u dims=%u\n", cluster_count, context.maxClusterCount, context.pointCount, context.dims);
            return 1;
        }

        const NSUInteger centroids_bytes = (NSUInteger)cluster_count * (NSUInteger)context.dims * sizeof(float);
        const NSUInteger counts_bytes = (NSUInteger)cluster_count * sizeof(uint32_t);
        const NSUInteger partial_counts_bytes = (NSUInteger)cluster_count * (NSUInteger)context.updateBlockCount * sizeof(uint32_t);
        memcpy(context.centroidsBuffer.contents, old_centroids, centroids_bytes);
        memset(context.countsBuffer.contents, 0, counts_bytes);
        memset(context.partialCountsBuffer.contents, 0, partial_counts_bytes);

        antfly_kmeans_params params = {
            .point_count = context.pointCount,
            .cluster_count = cluster_count,
            .dims = context.dims,
            .metric = metric,
            .update_block_size = antfly_kmeans_update_block_size,
            .update_block_count = context.updateBlockCount,
        };
        memcpy(context.paramsBuffer.contents, &params, sizeof(params));

        id<MTLCommandBuffer> command_buffer = [g_state.queue commandBuffer];
        if (command_buffer == nil) return 1;
        id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
        if (encoder == nil) return 1;

        [encoder setComputePipelineState:g_state.update_partials_pipeline];
        [encoder setBuffer:context.pointsBuffer offset:0 atIndex:0];
        [encoder setBuffer:context.assignmentsBuffer offset:0 atIndex:1];
        [encoder setBuffer:context.partialSumsBuffer offset:0 atIndex:2];
        [encoder setBuffer:context.partialCountsBuffer offset:0 atIndex:3];
        [encoder setBuffer:context.paramsBuffer offset:0 atIndex:4];

        NSUInteger width = g_state.update_partials_pipeline.maxTotalThreadsPerThreadgroup;
        if (width > 256) width = 256;
        if (width == 0) width = 1;
        MTLSize grid = MTLSizeMake(cluster_count * context.dims * context.updateBlockCount, 1, 1);
        MTLSize group = MTLSizeMake(width, 1, 1);
        [encoder dispatchThreads:grid threadsPerThreadgroup:group];
        [encoder endEncoding];

        encoder = [command_buffer computeCommandEncoder];
        if (encoder == nil) return 1;
        [encoder setComputePipelineState:g_state.update_finalize_pipeline];
        [encoder setBuffer:context.centroidsBuffer offset:0 atIndex:0];
        [encoder setBuffer:context.partialSumsBuffer offset:0 atIndex:1];
        [encoder setBuffer:context.partialCountsBuffer offset:0 atIndex:2];
        [encoder setBuffer:context.nextCentroidsBuffer offset:0 atIndex:3];
        [encoder setBuffer:context.countsBuffer offset:0 atIndex:4];
        [encoder setBuffer:context.paramsBuffer offset:0 atIndex:5];

        width = g_state.update_finalize_pipeline.maxTotalThreadsPerThreadgroup;
        if (width > 256) width = 256;
        if (width == 0) width = 1;
        grid = MTLSizeMake(cluster_count * context.dims, 1, 1);
        group = MTLSizeMake(width, 1, 1);
        [encoder dispatchThreads:grid threadsPerThreadgroup:group];
        [encoder endEncoding];

        [command_buffer commit];
        [command_buffer waitUntilCompleted];
        if (command_buffer.status != MTLCommandBufferStatusCompleted) {
            antfly_kmeans_metal_debug_command_error("update", command_buffer);
            return 1;
        }

        memcpy(next_centroids, context.nextCentroidsBuffer.contents, centroids_bytes);
        memcpy(counts, context.countsBuffer.contents, counts_bytes);
        return 0;
    }
}

int antfly_kmeans_metal_assign(
    const float *points,
    const float *centroids,
    uint32_t point_count,
    uint32_t cluster_count,
    uint32_t dims,
    int32_t metric,
    uint32_t *assignments,
    float *distances
) {
    if (points == NULL || centroids == NULL || assignments == NULL || distances == NULL) return 1;
    if (point_count == 0 || cluster_count == 0 || dims == 0) return 1;
    if (metric < 0 || metric > 2) return 1;

    void *context = antfly_kmeans_metal_context_create(points, point_count, cluster_count, dims);
    if (context == NULL) return 1;
    int rc = antfly_kmeans_metal_context_assign(context, centroids, cluster_count, metric, assignments, distances);
    antfly_kmeans_metal_context_destroy(context);
    return rc;
}
