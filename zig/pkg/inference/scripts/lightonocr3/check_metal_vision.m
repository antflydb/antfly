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

// Packed vision SDPA: precision, offsets, planned producers and frame leases.
#include "../../src/backends/metal_kernels.m"
#include <assert.h>

@interface VisionCheck : NSObject {
  @public
    size_t batch, seq, heads, count;
    id<MTLBuffer> q, k, v, out, reference;
}
@end
@implementation VisionCheck
@end

static VisionCheck *make_case_heads(termite_metal_decode_runtime *r, size_t batch, size_t seq, size_t heads) {
    VisionCheck *c = [VisionCheck new];
    c->batch = batch;
    c->seq = seq;
    c->heads = heads;
    c->count = batch * seq * heads * 64;
    size_t bytes = (c->count + 8) * 4;
    c->q = [r->device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
    c->k = [r->device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
    c->v = [r->device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
    c->out = [r->device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
    c->reference = [r->device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
    assert(c->q && c->k && c->v && c->out && c->reference);
    for (size_t i = 0; i < c->count + 8; i++) {
        ((float *)c->q.contents)[i] = sinf((float)(i % 4001) * 0.059f) * 0.2f;
        ((float *)c->k.contents)[i] = cosf((float)(i % 4019) * 0.071f) * 0.2f;
        ((float *)c->v.contents)[i] = sinf((float)(i % 4051) * 0.083f) * 0.2f;
        ((float *)c->out.contents)[i] = ((float *)c->reference.contents)[i] = 12345.0f;
    }
    return c;
}
static VisionCheck *make_case(termite_metal_decode_runtime *r, size_t batch, size_t seq) {
    return make_case_heads(r, batch, seq, 12);
}
static int encode_case(termite_metal_decode_runtime *r, VisionCheck *c, BOOL reference) {
    return termite_metal_decode_runtime_sdpa_f32_device(
        r, (__bridge void *)c->q, 16, (__bridge void *)c->k, 16, (__bridge void *)c->v, 16, NULL, 0, NULL, 0,
        c->batch, c->seq, c->heads, 64, 0, 0, 1, 1, 0, (__bridge void *)(reference ? c->reference : c->out),
        16);
}
static void compare_case(VisionCheck *c) {
    float *out = (float *)c->out.contents + 4, *ref = (float *)c->reference.contents + 4;
    float worst = 0;
    for (size_t i = 0; i < c->count; i++) {
        assert(isfinite(out[i]));
        worst = fmaxf(worst, fabsf(out[i] - ref[i]));
    }
    assert(worst < 2e-5f);
    for (size_t i = 0; i < 4; i++) {
        assert(((float *)c->out.contents)[i] == 12345.0f);
        assert(out[c->count + i] == 12345.0f);
    }
    // Independent F64 softmax samples at both tails and across batch/head lanes.
    for (size_t b = 0; b < c->batch; b++)
        for (size_t sample = 0; sample < 4; sample++) {
            size_t row = sample == 0   ? 0
                         : sample == 1 ? c->seq - 1
                                       : c->seq / (sample + 1),
                   h = (sample * 5 + b) % c->heads, d = (sample * 19 + b) % 64;
            double maxscore = -INFINITY;
            double *scores = malloc(c->seq * sizeof(double));
            assert(scores);
            float *q = (float *)c->q.contents + 4, *k = (float *)c->k.contents + 4,
                  *v = (float *)c->v.contents + 4;
            for (size_t j = 0; j < c->seq; j++) {
                double dot = 0;
                for (size_t t = 0; t < 64; t++)
                    dot += (double)q[((b * c->seq + row) * c->heads + h) * 64 + t] *
                           k[((b * c->seq + j) * c->heads + h) * 64 + t];
                scores[j] = isfinite(dot) ? dot * 0.125 : -INFINITY;
                maxscore = fmax(maxscore, scores[j]);
            }
            double sum = 0, value = 0;
            for (size_t j = 0; j < c->seq; j++) {
                double e = isfinite(scores[j]) ? exp(scores[j] - maxscore) : 0;
                sum += e;
                value += e * v[((b * c->seq + j) * c->heads + h) * 64 + d];
            }
            free(scores);
            assert(fabs((sum > 0 ? value / sum : 0) - out[((b * c->seq + row) * c->heads + h) * 64 + d]) <
                   2e-4);
        }
    fprintf(stderr, "vision heads=%zu batch=%zu seq=%zu full_output=pass cpu=pass canaries=pass max_abs=%g\n",
            c->heads, c->batch, c->seq, worst);
}
static void run_case(termite_metal_decode_runtime *r, VisionCheck *c, BOOL expected) {
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_VISION_MPP", "1", 1);
    assert(!encode_case(r, c, YES));
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_VISION_MPP");
    if (expected) {
        assert(!encode_case(r, c, NO));
        compare_case(c);
        assert(!r->lightonocr3_vision_pack_buffers[0] && !r->lightonocr3_vision_pack_buffers[1]);
    }
    uint64_t before = r->lightonocr3_vision_mpp_dispatches;
    assert(!termite_metal_decode_runtime_begin_frame(r));
    assert(!termite_metal_decode_runtime_begin_planned_compute_scope(
        r, TERMITE_METAL_COMPUTE_SOURCE_DENSE_LINEAR, TERMITE_METAL_COMPUTE_REGION_FFN));
    id<MTLBuffer> source = [r->device newBufferWithBytes:c->q.contents
                                                  length:c->q.length
                                                 options:MTLResourceStorageModeShared];
    id<MTLBuffer> zero = [r->device newBufferWithLength:c->q.length options:MTLResourceStorageModeShared];
    memset(zero.contents, 0, zero.length);
    memset(c->q.contents, 0, c->q.length);
    assert(!termite_metal_decode_runtime_apply_add_device(
        r, (__bridge void *)source, 16, (__bridge void *)zero, 16, c->count, (__bridge void *)c->q, 16));
    assert(!encode_case(r, c, NO));
    assert((r->lightonocr3_vision_mpp_dispatches != before) == expected);
    assert(!termite_metal_decode_runtime_end_planned_compute_scope(r));
    assert(termite_metal_decode_runtime_clear_lightonocr3_vision_workspace(r) != 0);
    assert(!termite_metal_decode_runtime_submit_frame(r));
    assert(!termite_metal_decode_runtime_wait_frame(r));
    compare_case(c);
    assert(!termite_metal_decode_runtime_clear_lightonocr3_vision_workspace(r));
    assert(!r->lightonocr3_vision_pack_buffers[0] && !r->lightonocr3_vision_pack_buffers[1]);
}
int main(void) {
    @autoreleasepool {
        termite_metal_decode_runtime *r = termite_metal_decode_runtime_create();
        assert(r);
        termite_metal_decode_runtime_set_lightonocr3_projections(r, 1);
        assert(r->lightonocr3_vision_mpp_pipeline && r->lightonocr3_vision_pack_pipeline);
        for (size_t b = 1; b <= 4; b++)
            run_case(r, make_case(r, b, b == 1 ? 257 : b == 2 ? 578 : b == 3 ? 1672 : 2160), YES);
        run_case(r, make_case(r, 1, 2592), YES);
        for (size_t b = 1; b <= 4; b++)
            run_case(r, make_case_heads(r, b, b == 1 ? 257 : b == 2 ? 578 : b == 3 ? 1672 : 2160, 16), YES);
        run_case(r, make_case_heads(r, 1, 2592, 16), YES);
        run_case(r, make_case_heads(r, 1, 4095, 16), YES);
        run_case(r, make_case_heads(r, 1, 4096, 16), YES);

        run_case(r, make_case(r, 1, 4095), YES);
        run_case(r, make_case(r, 1, 4096), YES);
        for (size_t heads = 12; heads <= 16; heads += 4) {
            run_case(r, make_case_heads(r, 1, 4097, heads), YES);
            run_case(r, make_case_heads(r, 1, 6528, heads), YES);
            run_case(r, make_case_heads(r, 1, 16384, heads), YES);
            VisionCheck *invalid = make_case_heads(r, 1, 578, heads);
            ((float *)invalid->q.contents)[4 + 3] = NAN;
            ((float *)invalid->k.contents)[4 + ((578 / 2) * heads + 5) * 64 + 31] = INFINITY;
            run_case(r, invalid, YES);
        }
        id<MTLComputePipelineState> pso = r->lightonocr3_vision_mpp_pipeline;
        r->lightonocr3_vision_mpp_pipeline = nil;
        run_case(r, make_case(r, 1, 578), NO);
        r->lightonocr3_vision_mpp_pipeline = pso;
        // Two encoded buffers cannot share a workspace; a cancelled peer must not
        // release a submitted owner's allocation. A larger call in one buffer
        // declines instead of retaining old and new allocations simultaneously.
        VisionCheck *a = make_case(r, 1, 578), *b = make_case(r, 1, 1672);
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_VISION_MPP", "1", 1);
        assert(!encode_case(r, a, YES));
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_VISION_MPP");
        assert(!termite_metal_decode_runtime_begin_frame(r));
        id<MTLSharedEvent> event = [r->device newSharedEvent];
        [r->active_frame_cb encodeWaitForEvent:event value:1];
        assert(!encode_case(r, a, NO));
        id<MTLBuffer> first = r->lightonocr3_vision_pack_buffers[0];
        assert(first);
        uint64_t before = r->lightonocr3_vision_mpp_dispatches;
        assert(!encode_case(r, b, NO));
        assert(r->lightonocr3_vision_mpp_dispatches == before);
        assert(r->lightonocr3_vision_pack_buffers[0] == first);
        assert(!termite_metal_decode_runtime_submit_frame(r));
        assert(!termite_metal_decode_runtime_begin_frame(r));
        assert(!encode_case(r, b, NO));
        assert(r->lightonocr3_vision_pack_buffers[1] && r->lightonocr3_vision_pack_buffers[1] != first);
        assert(termite_metal_decode_runtime_clear_lightonocr3_vision_workspace(r) != 0);
        assert(!termite_metal_decode_runtime_cancel_frame(r));
        event.signaledValue = 1;
        assert(!termite_metal_decode_runtime_wait_frame(r));
        compare_case(a);
        assert(!termite_metal_decode_runtime_clear_lightonocr3_vision_workspace(r));
        // GPU completion alone must not recycle a submitted frame's slot
        // before host frame retirement has released its resource bookkeeping.
        assert(!termite_metal_decode_runtime_begin_frame(r));
        assert(!encode_case(r, a, NO));
        id<MTLBuffer> completed_slot = r->lightonocr3_vision_pack_buffers[0];
        assert(completed_slot);
        assert(!termite_metal_decode_runtime_submit_frame(r));
        [r->submitted_frame_cb waitUntilCompleted];
        assert(!termite_metal_decode_runtime_begin_frame(r));
        assert(!encode_case(r, b, NO));
        assert(r->lightonocr3_vision_pack_buffers[0] == completed_slot &&
               r->lightonocr3_vision_pack_buffers[1] != nil);
        assert(!termite_metal_decode_runtime_cancel_frame(r));
        assert(!termite_metal_decode_runtime_wait_frame(r));
        assert(!r->lightonocr3_vision_pack_buffers[0] && !r->lightonocr3_vision_pack_buffers[1]);
        termite_metal_decode_runtime_destroy(r);
        fprintf(stderr, "All packed vision checks passed\n");
        return 0;
    }
}
