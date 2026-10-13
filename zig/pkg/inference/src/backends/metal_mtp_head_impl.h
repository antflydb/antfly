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

static const char *termite_mtp_head_source =
    "#include <metal_stdlib>\n"
    "using namespace metal;\n"
    "struct MtpHeadParams { uint hidden, vocab, clusters, topk, lm_type, centroid_type; };\n"
    "inline float mtp_weight(device const uchar *w, uint row, uint col, uint hidden, uint type) {\n"
    "    if (type == 4) {\n"
    "        device const uchar *b = w + ulong(row) * (hidden / 32) * 34 + ulong(col / 32) * 34;\n"
    "        return float(as_type<half>(*(device const ushort *)b)) * float(((device const char *)(b + 2))[col % 32]);\n"
    "    }\n"
    "    uint i = row * hidden + col;\n"
    "    if (type == 1) return as_type<float>(uint(((device const ushort *)w)[i]) << 16);\n"
    "    if (type == 3) return float(((device const half *)w)[i]);\n"
    "    return ((device const float *)w)[i];\n"
    "}\n"
    "kernel void mtp_centroids(device const float *x [[buffer(0)]], device const uchar *w [[buffer(1)]], device float *scores [[buffer(2)]], constant MtpHeadParams &p [[buffer(3)]], uint tg [[threadgroup_position_in_grid]], ushort sg [[simdgroup_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]]) {\n"
    "    uint row = tg * 8 + sg; if (row >= p.clusters) return;\n"
    "    float sum = 0; for (uint c = lane; c < p.hidden; c += 32) sum += x[c] * mtp_weight(w, row, c, p.hidden, p.centroid_type);\n"
    "    sum = simd_sum(sum); if (lane == 0) scores[row] = sum;\n"
    "}\n"
    "kernel void mtp_clusters(device const float *scores [[buffer(0)]], device uint *ids [[buffer(1)]], constant MtpHeadParams &p [[buffer(2)]], uint tg [[threadgroup_position_in_grid]], ushort sg [[simdgroup_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]]) {\n"
    "    uint row = tg * 8 + sg; if (row >= p.clusters) return;\n"
    "    float own = scores[row]; if (isnan(own)) own = -INFINITY;\n"
    "    uint rank = 0;\n"
    "    for (uint c = lane; c < p.clusters; c += 32) {\n"
    "        float other = scores[c]; if (isnan(other)) other = -INFINITY;\n"
    "        rank += uint(other > own || (other == own && c < row));\n"
    "    }\n"
    "    rank = simd_sum(rank); if (lane == 0 && rank < p.topk) ids[rank] = row;\n"
    "}\n"
    "kernel void mtp_candidates(device const float *x [[buffer(0)]], device const uchar *w [[buffer(1)]], device const float *ordering [[buffer(2)]], device const uint *ids [[buffer(3)]], device float *scores [[buffer(4)]], device uint *tokens [[buffer(5)]], constant MtpHeadParams &p [[buffer(6)]], uint tg [[threadgroup_position_in_grid]], ushort sg [[simdgroup_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]]) {\n"
    "    uint row = tg * 8 + sg; if (row >= p.topk * (p.vocab / p.clusters)) return;\n"
    "    uint size = p.vocab / p.clusters, pos = ids[row / size] * size + row % size;\n"
    "    float mapped = ordering[pos]; bool valid = isfinite(mapped) && mapped >= 0 && mapped < float(p.vocab) && floor(mapped) == mapped;\n"
    "    uint token = valid ? uint(mapped) : 0; float sum = 0;\n"
    "    if (valid) for (uint c = lane; c < p.hidden; c += 32) sum += x[c] * mtp_weight(w, token, c, p.hidden, p.lm_type);\n"
    "    sum = simd_sum(sum); if (lane == 0) { scores[row] = valid ? sum : -INFINITY; tokens[row] = token; }\n"
    "}\n"
    "kernel void mtp_select(device const float *scores [[buffer(0)]], device const uint *tokens [[buffer(1)]], device uint *result [[buffer(2)]], constant MtpHeadParams &p [[buffer(3)]], uint tid [[thread_position_in_grid]]) {\n"
    "    if (tid != 0) return; float best = -INFINITY; uint token = 0;\n"
    "    for (uint i = 0; i < p.topk * (p.vocab / p.clusters); ++i) if (scores[i] > best) { best = scores[i]; token = tokens[i]; }\n"
    "    result[0] = token;\n"
    "}\n"
;

@implementation TermiteMetalMtpHeadWorkspace
@end

static id<MTLComputePipelineState> termite_mtp_head_pipeline(id<MTLDevice> device, id<MTLLibrary> library, NSString *name) {
    MTLComputePipelineDescriptor *desc = [[MTLComputePipelineDescriptor alloc] init];
    desc.computeFunction = [library newFunctionWithName:name];
    desc.supportIndirectCommandBuffers = YES;
    return [device newComputePipelineStateWithDescriptor:desc options:MTLPipelineOptionNone reflection:nil error:nil];
}

int termite_metal_decode_runtime_mtp_cluster_head(
    termite_metal_decode_runtime *runtime, size_t lm_slot, size_t centroid_slot,
    void *input_handle, size_t input_offset, void *ordering_handle, size_t ordering_offset,
    size_t hidden, size_t vocab, size_t clusters, size_t topk, uint32_t *token
) {
    if (!runtime || !input_handle || !ordering_handle || !token) return -1;
    if (!hidden || hidden > 16384 || !vocab || vocab > 1048576 || !clusters || clusters > 4096 ||
        hidden > UINT32_MAX / vocab || !topk || topk > 128 || topk > clusters || vocab % clusters || lm_slot >= TERMITE_METAL_LINEAR_SLOT_CAPACITY ||
        centroid_slot >= TERMITE_METAL_LINEAR_SLOT_CAPACITY || topk * (vocab / clusters) > 65536) return -2;
    // Flush preserves the caller's frame by reopening an empty one. The
    // independent head command is ordered after that flush's wait; pending
    // work still requires a drain, but an empty frame is safe to preserve.
    const bool append_frame = runtime->active_frame_cb != nil &&
        termite_metal_env_flag_enabled(getenv("TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_FRAME"));
    if (runtime->submitted_frame_cb != nil || (!append_frame && runtime->active_frame_cb != nil && !termite_metal_decode_runtime_active_frame_empty(runtime))) return -3;
    id<MTLBuffer> weights[2]; NSUInteger offsets[2] = {0, 0}; uint32_t types[2];
    size_t slots[2] = {lm_slot, centroid_slot}, rows[2] = {vocab, clusters};
    for (unsigned i = 0; i < 2; ++i) {
        size_t slot = slots[i];
        if (runtime->quant_linear_slot_prepared[slot]) {
            if (runtime->quant_linear_formats[slot] != TERMITE_METAL_QUANT_FORMAT_Q8_0 || hidden % 32 ||
                runtime->quant_linear_in_dims[slot] != hidden || runtime->quant_linear_out_dims[slot] != rows[i] ||
                runtime->quant_linear_row_stride_bytes[slot] != (hidden / 32) * 34) return -4;
            weights[i] = runtime->quant_linear_weight_buffers[slot]; offsets[i] = runtime->quant_linear_weight_offsets[slot]; types[i] = 4;
        } else {
            if (!runtime->linear_slot_prepared[slot] || runtime->linear_in_dims[slot] != hidden || runtime->linear_out_dims[slot] != rows[i]) return -4;
            weights[i] = runtime->linear_weight_buffers[slot]; types[i] = runtime->linear_weight_dtypes[slot];
            if (types[i] == TERMITE_METAL_DENSE_LINEAR_DTYPE_F32_MPS) types[i] = 0;
            if (types[i] != 0 && types[i] != 1 && types[i] != 3) return -4;
        }
        size_t bytes = types[i] == 4 ? rows[i] * (hidden / 32) * 34 : rows[i] * hidden * (types[i] == 0 ? 4 : 2);
        if (weights[i] == nil || offsets[i] > weights[i].length || bytes > weights[i].length - offsets[i]) return -5;
    }
    id<MTLBuffer> source = (__bridge id<MTLBuffer>)input_handle, order = (__bridge id<MTLBuffer>)ordering_handle;
    if (input_offset > source.length || hidden * 4 > source.length - input_offset ||
        ordering_offset > order.length || vocab * 4 > order.length - ordering_offset) return -5;
    @autoreleasepool {
        TermiteMetalMtpHeadWorkspace *w = runtime->mtp_head_workspace;
        TermiteMtpHeadParams p = {(uint32_t)hidden, (uint32_t)vocab, (uint32_t)clusters, (uint32_t)topk, types[0], types[1]};
        BOOL rebuild = w == nil || memcmp(&w->geometry, &p, sizeof(p)) || w->lm_weight != weights[0] ||
            w->centroid_weight != weights[1] || w->ordering != order || w->lm_offset != offsets[0] ||
            w->centroid_offset != offsets[1] || w->ordering_offset != ordering_offset;
        if (rebuild) {
            w = [[TermiteMetalMtpHeadWorkspace alloc] init];
            MTLCompileOptions *options = [[MTLCompileOptions alloc] init]; options.fastMathEnabled = NO;
            NSError *error = nil;
            id<MTLLibrary> library = [runtime->device newLibraryWithSource:[NSString stringWithUTF8String:termite_mtp_head_source] options:options error:&error];
            if (!library) { fprintf(stderr, "mtp_cluster_head_compile: %s\n", error.localizedDescription.UTF8String); return -6; }
            NSString *names[4] = {@"mtp_centroids", @"mtp_clusters", @"mtp_candidates", @"mtp_select"};
            for (unsigned i = 0; i < 4; ++i) { w->pipelines[i] = termite_mtp_head_pipeline(runtime->device, library, names[i]); if (!w->pipelines[i] || w->pipelines[i].maxTotalThreadsPerThreadgroup < (i < 3 ? 256 : 1)) return -6; }
            size_t candidates = topk * (vocab / clusters);
            w->input = [runtime->device newBufferWithLength:hidden * 4 options:MTLResourceStorageModePrivate];
            w->centroids = [runtime->device newBufferWithLength:clusters * 4 options:MTLResourceStorageModePrivate];
            w->cluster_ids = [runtime->device newBufferWithLength:topk * 4 options:MTLResourceStorageModePrivate];
            w->scores = [runtime->device newBufferWithLength:candidates * 4 options:MTLResourceStorageModePrivate];
            w->tokens = [runtime->device newBufferWithLength:candidates * 4 options:MTLResourceStorageModePrivate];
            w->result = [runtime->device newBufferWithLength:4 options:MTLResourceStorageModeShared];
            w->params = [runtime->device newBufferWithBytes:&p length:sizeof(p) options:MTLResourceStorageModeShared];
            if (!w->input || !w->centroids || !w->cluster_ids || !w->scores || !w->tokens || !w->result || !w->params) return -7;
            w->geometry = p; w->lm_weight = weights[0]; w->centroid_weight = weights[1]; w->ordering = order;
            w->lm_offset = offsets[0]; w->centroid_offset = offsets[1]; w->ordering_offset = ordering_offset;
            runtime->mtp_head_workspace = w;
        }
        // Append before the scalar boundary: the hidden producer and head
        // share one submission. Flush below preserves the caller's frame.
        if (append_frame) termite_metal_decode_runtime_close_planned_compute_encoder_for_transition(runtime);
        id<MTLCommandBuffer> command = append_frame ? runtime->active_frame_cb : termite_metal_new_command_buffer(runtime->queue, __func__);
        if (!command) return -8;
        if (append_frame) { runtime->active_frame_blit_encoder_count += 1; runtime->active_frame_compute_encoder_count += 1; }
        id<MTLBlitCommandEncoder> blit = [command blitCommandEncoder]; if (!blit) return -8;
        [blit copyFromBuffer:source sourceOffset:input_offset toBuffer:w->input destinationOffset:0 size:hidden * 4]; [blit endEncoding];
        id<MTLComputeCommandEncoder> encoder = [command computeCommandEncoder]; if (!encoder) return -8;
        id<MTLBuffer> bindings[4][7] = {
            {w->input, w->centroid_weight, w->centroids, w->params},
            {w->centroids, w->cluster_ids, w->params},
            {w->input, w->lm_weight, w->ordering, w->cluster_ids, w->scores, w->tokens, w->params},
            {w->scores, w->tokens, w->result, w->params}
        };
        NSUInteger counts[4] = {4, 3, 7, 4}, grids[4] = {(clusters + 7) / 8, (clusters + 7) / 8, (topk * (vocab / clusters) + 7) / 8, 1}, threads[4] = {256, 256, 256, 1};
        BOOL replay = termite_metal_env_flag_enabled(getenv("TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_REPLAY"));
        if (replay && w->commands == nil) {
            MTLIndirectCommandBufferDescriptor *desc = [[MTLIndirectCommandBufferDescriptor alloc] init];
            desc.commandTypes = MTLIndirectCommandTypeConcurrentDispatch;
            desc.inheritBuffers = NO; desc.inheritPipelineState = NO; desc.maxKernelBufferBindCount = 7;
            w->commands = [runtime->device newIndirectCommandBufferWithDescriptor:desc maxCommandCount:4 options:MTLResourceStorageModePrivate];
            if (w->commands != nil) for (unsigned i = 0; i < 4; ++i) {
                id<MTLIndirectComputeCommand> item = [w->commands indirectComputeCommandAtIndex:i];
                [item setComputePipelineState:w->pipelines[i]];
                for (unsigned j = 0; j < counts[i]; ++j) {
                    NSUInteger offset = (i == 0 && j == 1) ? w->centroid_offset : (i == 2 && j == 1) ? w->lm_offset : (i == 2 && j == 2) ? w->ordering_offset : 0;
                    [item setKernelBuffer:bindings[i][j] offset:offset atIndex:j];
                }
                [item concurrentDispatchThreadgroups:MTLSizeMake(grids[i], 1, 1) threadsPerThreadgroup:MTLSizeMake(threads[i], 1, 1)];
                [item setBarrier];
            }
        }
        if (replay && w->commands != nil) {
            // ICB-bound buffers require explicit residency declarations. Cached
            // commands never borrow per-request input buffers; blit refreshes
            // the stable input before replay. Rebuild invalidates all bindings.
            for (unsigned i = 0; i < 4; ++i) for (unsigned j = 0; j < counts[i]; ++j)
                [encoder useResource:bindings[i][j] usage:MTLResourceUsageRead | MTLResourceUsageWrite];
            [encoder executeCommandsInBuffer:w->commands withRange:NSMakeRange(0, 4)];
        } else {
        for (unsigned i = 0; i < 4; ++i) {
            [encoder setComputePipelineState:w->pipelines[i]];
            for (unsigned j = 0; j < counts[i]; ++j) {
                NSUInteger offset = (i == 0 && j == 1) ? w->centroid_offset : (i == 2 && j == 1) ? w->lm_offset : (i == 2 && j == 2) ? w->ordering_offset : 0;
                [encoder setBuffer:bindings[i][j] offset:offset atIndex:j];
            }
            [encoder dispatchThreadgroups:MTLSizeMake(grids[i], 1, 1) threadsPerThreadgroup:MTLSizeMake(threads[i], 1, 1)];
            [encoder memoryBarrierWithScope:MTLBarrierScopeBuffers];
        }
        }
        [encoder endEncoding];
        if (append_frame) {
            if (termite_metal_decode_runtime_flush_active_frame(runtime) != 0) return -9;
        } else { [command commit]; [command waitUntilCompleted]; }
        if (command.status != MTLCommandBufferStatusCompleted) return -9;
        *token = *(uint32_t *)w->result.contents;
        return 0;
    }
}

// Dedicated fresh-runtime fixture; never called on a serving runtime. Exact
// integer scores isolate clustering/order/bindings from rounding tolerances.
int termite_metal_decode_runtime_mtp_cluster_head_test(termite_metal_decode_runtime *r) {
    if (!r) return -1;
    @autoreleasepool {
        float x[68] = {0}, lm[32 * 64] = {0}, cent[4 * 64] = {0}, ordering[36] = {0};
        for (unsigned i = 0; i < 32; ++i) { ordering[i + 4] = 31 - i; lm[i * 64] = i + 1; }
        for (unsigned i = 0; i < 4; ++i) cent[i * 64] = i + 1;
        id<MTLBuffer> input = [r->device newBufferWithBytes:x length:sizeof(x) options:MTLResourceStorageModeShared];
        id<MTLBuffer> order = [r->device newBufferWithBytes:ordering length:sizeof(ordering) options:MTLResourceStorageModeShared];
        if (!input || !order) return -2;
        for (unsigned type = 0; type < 4; ++type) {
            uint16_t packed[32 * 64] = {0}; unsigned char quant[32 * 2 * 34] = {0};
            for (unsigned i = 0; i < 32; ++i) {
                float value = i + 1; uint32_t bits; memcpy(&bits, &value, sizeof(bits));
                packed[i * 64] = type == 1 ? (uint16_t)(bits >> 16) : 0;
                if (type == 2) { __fp16 half = value; memcpy(&packed[i * 64], &half, 2); }
                for (unsigned b = 0; b < 2; ++b) { uint16_t scale = 0x3c00; memcpy(quant + i * 68 + b * 34, &scale, 2); }
                quant[i * 68 + 2] = (unsigned char)(i + 1);
            }
            r->quant_linear_slot_prepared[0] = type == 3;
            r->linear_slot_prepared[0] = type != 3; r->linear_in_dims[0] = 64; r->linear_out_dims[0] = 32;
            r->linear_weight_dtypes[0] = type == 2 ? 3 : type;
            r->linear_weight_buffers[0] = [r->device newBufferWithBytes:type == 0 ? (void *)lm : (void *)packed length:type == 0 ? sizeof(lm) : sizeof(packed) options:MTLResourceStorageModeShared];
            r->quant_linear_formats[0] = TERMITE_METAL_QUANT_FORMAT_Q8_0;
            r->quant_linear_in_dims[0] = 64; r->quant_linear_out_dims[0] = 32; r->quant_linear_row_stride_bytes[0] = 68;
            r->quant_linear_weight_buffers[0] = [r->device newBufferWithBytes:quant length:sizeof(quant) options:MTLResourceStorageModeShared];
            r->linear_slot_prepared[1] = 1; r->linear_in_dims[1] = 64; r->linear_out_dims[1] = 4; r->linear_weight_dtypes[1] = 0;
            r->linear_weight_buffers[1] = [r->device newBufferWithBytes:cent length:sizeof(cent) options:MTLResourceStorageModeShared];
            if (!r->linear_weight_buffers[0] || !r->linear_weight_buffers[1] || !r->quant_linear_weight_buffers[0]) return -2;
            for (unsigned pass = 0; pass < 3; ++pass) {
                ((float *)input.contents)[4] = pass == 1 ? -1 : 1;
                uint32_t token = UINT32_MAX;
                int rc = termite_metal_decode_runtime_mtp_cluster_head(r, 0, 1, (__bridge void *)input, 16, (__bridge void *)order, 16, 64, 32, 4, 2, &token);
                if (rc || token != (pass == 1 ? 16 : 15)) { fprintf(stderr, "mtp_head_test type=%u pass=%u rc=%d token=%u\n", type, pass, rc, token); return -3; }
            }
        }
        uint32_t token = UINT32_MAX;
        if (termite_metal_decode_runtime_mtp_cluster_head(r, 0, 1, (__bridge void *)input, 16, (__bridge void *)order, 16, 64, 32, 3, 2, &token) != -2) return -4;
        if (termite_metal_decode_runtime_mtp_cluster_head(r, 0, 1, (__bridge void *)input, 16, (__bridge void *)order, 16, 64, 32, 4, 5, &token) != -2) return -4;
        if (termite_metal_decode_runtime_begin_frame(r) != 0) return -6;
        if (termite_metal_decode_runtime_mtp_cluster_head(r, 0, 1, (__bridge void *)input, 16, (__bridge void *)order, 16, 64, 32, 4, 2, &token) != 0 || token != 15) return -6;
        if (!termite_metal_decode_runtime_active_frame_empty(r) || r->active_frame_cb == nil) return -6;
        id<MTLBlitCommandEncoder> pending = [r->active_frame_cb blitCommandEncoder];
        if (pending == nil) return -6;
        float pending_input[64] = {-1.0f};
        id<MTLBuffer> producer = [r->device newBufferWithBytes:pending_input length:sizeof(pending_input) options:MTLResourceStorageModeShared];
        if (producer == nil) return -6;
        [pending copyFromBuffer:producer sourceOffset:0 toBuffer:input destinationOffset:16 size:sizeof(pending_input)];
        [pending endEncoding]; r->active_frame_blit_encoder_count += 1;
        const bool head_frame = termite_metal_env_flag_enabled(getenv("TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_FRAME"));
        const uint64_t submits_before = r->frame_submit_count;
        const int pending_rc = termite_metal_decode_runtime_mtp_cluster_head(r, 0, 1, (__bridge void *)input, 16, (__bridge void *)order, 16, 64, 32, 4, 2, &token);
        if (pending_rc != (head_frame ? 0 : -3)) return -6;
        if (head_frame && (token != 16 || !termite_metal_decode_runtime_active_frame_empty(r) || r->frame_submit_count != submits_before + 1)) return -6;
        if (termite_metal_decode_runtime_cancel_frame(r) != 0) return -6;
        r->submitted_frame_cb = [r->queue commandBuffer];
        if (r->submitted_frame_cb == nil) return -6;
        [r->submitted_frame_cb commit];
        if (termite_metal_decode_runtime_mtp_cluster_head(r, 0, 1, (__bridge void *)input, 16, (__bridge void *)order, 16, 64, 32, 4, 2, &token) != -3) return -6;
        [r->submitted_frame_cb waitUntilCompleted]; r->submitted_frame_cb = nil;
        if (r->mtp_head_workspace == nil) return -5;
        // A serving frame may outlive its ambient thread-local association.
        // Copies must close the explicit runtime's compute encoder, and Shared
        // GPU producers must not be read by an immediate CPU memcpy.
        float guards[8] = {-77,-77,-77,-77,-77,-77,-77,-77};
        id<MTLBuffer> produced = [r->device newBufferWithLength:4 * sizeof(float) options:MTLResourceStorageModeShared];
        id<MTLBuffer> copied = [r->device newBufferWithBytes:guards length:sizeof(guards) options:MTLResourceStorageModeShared];
        id<MTLBuffer> copied_private = [r->device newBufferWithLength:4 * sizeof(float) options:MTLResourceStorageModePrivate];
        if (!produced || !copied || !copied_private) return -7;
        for (unsigned pair = 0; pair < 2; ++pair) {
            if (termite_metal_decode_runtime_begin_frame(r) != 0) return -7;
            TermiteMetalMtpHeadWorkspace *w = r->mtp_head_workspace;
            id<MTLComputeCommandEncoder> producer_encoder = [r->active_frame_cb computeCommandEncoder];
            if (!producer_encoder) return -7;
            r->active_planned_compute_encoder = producer_encoder;
            r->active_frame_compute_encoder_count += 1;
            [producer_encoder setComputePipelineState:w->pipelines[0]];
            [producer_encoder setBuffer:w->input offset:0 atIndex:0];
            [producer_encoder setBuffer:w->centroid_weight offset:w->centroid_offset atIndex:1];
            [producer_encoder setBuffer:produced offset:0 atIndex:2];
            [producer_encoder setBuffer:w->params offset:0 atIndex:3];
            [producer_encoder dispatchThreadgroups:MTLSizeMake(1,1,1) threadsPerThreadgroup:MTLSizeMake(256,1,1)];
            termite_metal_active_encoder_runtime = NULL;
            const int copy_rc = pair
                ? termite_metal_buffer_copy_pair(r, (__bridge void *)produced, 0, (__bridge void *)copied, 0, 16, (__bridge void *)produced, 0, (__bridge void *)copied_private, 0, 16)
                : termite_metal_buffer_copy(r, (__bridge void *)produced, 0, (__bridge void *)copied, 16, 16);
            if (copy_rc || r->active_planned_compute_encoder != nil || r->active_frame_blit_encoder_count != 1) return -7;
            if (termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r)) return -7;
            const float sign = head_frame ? -1.0f : 1.0f;
            const float *values = copied.contents;
            for (unsigned i = 0; i < 4; ++i) if (values[(pair ? 0 : 4) + i] != sign * (i + 1)) return -7;
            if (!pair) for (unsigned i = 0; i < 4; ++i) if (values[i] != -77.0f) return -7;
            if (pair) {
                float downloaded[4] = {0};
                if (termite_metal_buffer_download(r, (__bridge void *)copied_private, 0, downloaded, sizeof(downloaded))) return -7;
                for (unsigned i = 0; i < 4; ++i) if (downloaded[i] != sign * (i + 1)) return -7;
            }
        }
        if (termite_metal_decode_runtime_clear_linear_slot(r, 0) != 0 || r->mtp_head_workspace != nil) return -5;
        return 0;
    }
}
