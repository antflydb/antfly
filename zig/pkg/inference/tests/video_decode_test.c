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

#include "../src/ops/cuda/video_decode.c"
#include <assert.h>
#include <string.h>

static unsigned creates, destroys, maps, unmaps, consumes;
static int selection = 1, consume_result = 1;
static unsigned reservations, releases, live_budgets;
static int admit_decoder = 1, fail_create = 0;
static int publish_failed_endpoint, fail_destroy, fail_parser_destroy;
static unsigned destroy_attempts, parser_destroy_attempts;
static cudaVideoCodec created_codec;
static int fake_reserve(void *user, unsigned width, unsigned height, unsigned surfaces) {
    (void)user; assert(width && height && surfaces);
    if (!admit_decoder) return 0;
    reservations++; live_budgets++; return 1;
}
static void fake_release(void *user) {
    (void)user; assert(live_budgets); live_budgets--; releases++;
}
static CUresult CUDAAPI fake_caps(CUVIDDECODECAPS *c) {
    c->bIsSupported = 1; c->nOutputFormatMask = 1;
    c->nMinWidth = c->nMinHeight = 48;
    c->nMaxWidth = c->nMaxHeight = 4096; c->nMaxMBCount = 65536;
    return CUDA_SUCCESS;
}
static CUresult CUDAAPI fake_create(CUvideodecoder *out, CUVIDDECODECREATEINFO *c) {
    assert(c->ulNumDecodeSurfaces <= 32 && c->ulNumOutputSurfaces == 1);
    if (fail_create) {
        if (publish_failed_endpoint) { *out = (CUvideodecoder)(uintptr_t)1; creates++; }
        return (CUresult)2; // SDK failure may still publish an owned endpoint
    }
    created_codec = c->CodecType;
    *out = (CUvideodecoder)(uintptr_t)1; creates++; return CUDA_SUCCESS;
}
static CUresult CUDAAPI fake_destroy(CUvideodecoder decoder) {
    assert(decoder); destroy_attempts++;
    if (fail_destroy) return (CUresult)2;
    destroys++; return CUDA_SUCCESS;
}
static CUresult CUDAAPI fake_destroy_parser(CUvideoparser parser) {
    assert(parser); parser_destroy_attempts++;
    return fail_parser_destroy ? (CUresult)2 : CUDA_SUCCESS;
}
static CUresult CUDAAPI fake_map(CUvideodecoder decoder, int index, unsigned long long *device, unsigned *pitch, CUVIDPROCPARAMS *p) {
    (void)index; (void)p; assert(decoder);
    *device = 1234; *pitch = 256; maps++; return CUDA_SUCCESS;
}
static CUresult CUDAAPI fake_unmap(CUvideodecoder decoder, unsigned long long device) {
    assert(decoder && device == 1234); unmaps++; return CUDA_SUCCESS;
}
static CUresult CUDAAPI fake_status(CUvideodecoder decoder, int index, CUVIDGETDECODESTATUS *status) {
    (void)index; assert(decoder); status->decodeStatus = cuvidDecodeStatus_Success; return CUDA_SUCCESS;
}
static int fake_frame(void *user, uint64_t device, unsigned pitch, unsigned w, unsigned h, int64_t pts, unsigned matrix, unsigned full) {
    (void)user; (void)pitch; (void)w; (void)h; (void)pts; (void)matrix; (void)full;
    if (!device) return selection;
    consumes++; return consume_result;
}
static CUVIDEOFORMAT format(void) {
    CUVIDEOFORMAT f = {0};
    f.codec = cudaVideoCodec_H264; f.progressive_sequence = 1;
    f.chroma_format = cudaVideoChromaFormat_420;
    f.coded_width = f.display_area.right = 160;
    f.coded_height = f.display_area.bottom = 96;
    f.display_aspect_ratio.x = 5; f.display_aspect_ratio.y = 3;
    f.video_signal_description.color_primaries = 2;
    f.video_signal_description.transfer_characteristics = 2;
    f.video_signal_description.matrix_coefficients = 2;
    f.min_num_decode_surfaces = 4;
    return f;
}
static Decoder decoder(void) {
    Decoder d = {0};
    d.expected_width = 160; d.expected_height = 96;
    d.expected_codec = cudaVideoCodec_H264;
    d.matrix_override = d.range_override = -1;
    d.reserve_budget = fake_reserve; d.release_budget = fake_release;
    d.caps = fake_caps; d.create_decoder = fake_create; d.destroy_decoder = fake_destroy;
    d.destroy_parser = fake_destroy_parser;
    d.map = fake_map; d.unmap = fake_unmap; d.status = fake_status; d.frame = fake_frame;
    return d;
}
// The same driver-death contract applies to legacy and prepared callers.
static Decoder *move_to_heap(Decoder *d) {
    Decoder *owned = malloc(sizeof(*owned)); assert(owned);
    *owned = *d; memset(d, 0, sizeof(*d)); return owned;
}
static void set_mode(Decoder *d, unsigned prepared, CUVIDEOFORMAT *f) {
    d->require_prepared_sequence = prepared;
    d->prepared_sequence = (AntflyNvdecSequence){160, 96, 160, 96, 4,
        (uint32_t)f->codec, 0, 0, 2, 0};
}
static void checked_death_failures(void) {
    for (unsigned mode = 0; mode != 2; ++mode) {
        Decoder d = decoder(); CUVIDEOFORMAT f = format(); set_mode(&d, mode, &f);
        unsigned old_releases = releases, old_destroys = destroys;
        // A failed create can return a live endpoint, and its failed destruction
        // must preserve both the exact endpoint and its budget for later close.
        fail_create = publish_failed_endpoint = fail_destroy = 1;
        assert(sequence(&d, &f) == 0 && d.failed == 3);
        assert(d.decoder && d.budget_live && live_budgets == 1);
        assert(releases == old_releases && destroys == old_destroys);
        fail_create = publish_failed_endpoint = fail_destroy = 0;
        assert(antfly_nvdec_close_checked(move_to_heap(&d)) == 0);
        assert(releases == old_releases + 1 && !live_budgets);

        d = decoder(); f = format(); set_mode(&d, mode, &f);
        assert(sequence(&d, &f) == 4);
        unsigned old_attempts = destroy_attempts, old_reservations = reservations;
        old_releases = releases;
        d.sequence_seen = 0; d.expected_codec = f.codec = cudaVideoCodec_HEVC;
        fail_destroy = 1;
        assert(sequence(&d, &f) == 0 && d.failed == (mode ? 2 : 3));
        // Prepared mode rejects a changed recipe before destruction. Legacy
        // mode attempts its original replacement, retaining a failed endpoint.
        assert(destroy_attempts == old_attempts + !mode);
        assert(d.decoder && d.budget_live && reservations == old_reservations);
        assert(releases == old_releases);
        fail_destroy = 0;
        assert(antfly_nvdec_close_checked(move_to_heap(&d)) == 0);

        d = decoder(); f = format(); set_mode(&d, mode, &f);
        assert(sequence(&d, &f) == 4);
        Decoder *owned = move_to_heap(&d);
        owned->parser = (CUvideoparser)(uintptr_t)2;
        old_releases = releases; old_attempts = destroy_attempts;
        fail_parser_destroy = 1;
        assert(antfly_nvdec_close_checked(owned) == 3);
        assert(owned->parser && owned->decoder && owned->budget_live);
        assert(releases == old_releases && destroy_attempts == old_attempts);
        fail_parser_destroy = 0; fail_destroy = 1;
        assert(antfly_nvdec_close_checked(owned) == 3);
        assert(!owned->parser && owned->decoder && owned->budget_live);
        assert(releases == old_releases);
        unsigned parser_attempts = parser_destroy_attempts;
        fail_destroy = 0;
        assert(antfly_nvdec_close_checked(owned) == 0);
        assert(parser_destroy_attempts == parser_attempts && releases == old_releases + 1);

        d = decoder(); f = format(); set_mode(&d, mode, &f);
        assert(sequence(&d, &f) == 4);
        d.parser = (CUvideoparser)(uintptr_t)2;
        old_releases = releases; fail_parser_destroy = 1;
        assert(antfly_nvdec_finish_checked(&d) == 3 && d.parser && d.decoder);
        assert(d.budget_live && releases == old_releases);
        assert(!antfly_nvdec_retirement_ready(&d, d.budget_user, d.release_budget));
        fail_parser_destroy = 0;
        assert(antfly_nvdec_finish_checked(&d) == 0 && !d.parser && d.decoder);
        assert(releases == old_releases);
        assert(antfly_nvdec_close_checked(move_to_heap(&d)) == 0);
        assert(!live_budgets && reservations == releases && creates == destroys);
    }
}
int main(void) {
    Decoder d = decoder(); CUVIDEOFORMAT f = format();
    assert(sequence(&d, &f) == 4 && creates == 1);
    assert(sequence(&d, &f) == 4 && creates == 1);
    d.sequence_seen = 0;
    assert(sequence(&d, &f) == 4 && creates == 1 && reservations == 1); // compatible clips retain one lease
    f.min_num_decode_surfaces = 5;
    assert(sequence(&d, &f) == 0 && d.failed == 2); // midclip change
    d.sequence_seen = 0; d.failed = 0;
    assert(sequence(&d, &f) == 5 && creates == 2 && destroys == 1);
    CUVIDPARSERDISPINFO picture = {0}; picture.progressive_frame = 1;
    selection = 0;
    assert(display(&d, &picture) == 1 && maps == 0);
    selection = 1;
    assert(display(&d, &picture) == 1 && consumes == 1 && maps == 1 && unmaps == 1);
    consume_result = -1;
    assert(display(&d, &picture) == 0 && d.failed == 4 && maps == 2 && unmaps == 2);
    picture.progressive_frame = 0;
    assert(display(&d, &picture) == 0 && d.failed == 2 && maps == 2);
    fake_destroy(d.decoder); release_budget(&d);
    assert(reservations == 2 && releases == 2 && live_budgets == 0);
    // Capacity denial must happen before decoder allocation.
    d = decoder(); f = format(); admit_decoder = 0;
    assert(sequence(&d, &f) == 0 && d.failed == 6 && creates == 2 && !live_budgets);
    admit_decoder = 1; fail_create = 1; d = decoder();
    assert(sequence(&d, &f) == 0 && d.failed == 3 && !live_budgets);
    fail_create = 0;
    assert(reservations == releases);
    // Teardown releases a retained decoder even without a live request/parser.
    Decoder *cached = malloc(sizeof(*cached)); assert(cached);
    *cached = decoder();
    assert(sequence(cached, &f) == 4 && live_budgets == 1);
    assert(antfly_nvdec_close_checked(cached) == 0);
    assert(!live_budgets && reservations == releases);
    d = decoder(); d.reserve_budget = NULL;
    assert(sequence(&d, &f) == 0 && d.failed == 6 && !live_budgets);

    for (unsigned i = 0; i < 9; i++) {
        d = decoder(); f = format();
        switch (i) {
            case 0: f.video_signal_description.transfer_characteristics = 16; break;
            case 1: f.video_signal_description.transfer_characteristics = 18; break;
            case 2: f.video_signal_description.color_primaries = 9; break;
            case 3: f.display_aspect_ratio.x = 16; f.display_aspect_ratio.y = 9; break;
            case 4: f.min_num_decode_surfaces = 33; break;
            case 5: f.min_num_decode_surfaces = 0; break;
            case 6: f.bit_depth_luma_minus8 = 2; break;
            case 7: f.display_area.right = 158; break;
            case 8: f.video_signal_description.matrix_coefficients = 1; d.matrix_override = 6; break;
        }
        assert(sequence(&d, &f) == 0 && d.failed == 2 && !d.decoder);
    }

    // Same-geometry clips may reuse surfaces only when their codecs match.
    const cudaVideoCodec codecs[] = {
        cudaVideoCodec_H264, cudaVideoCodec_HEVC, cudaVideoCodec_AV1, cudaVideoCodec_H264
    };
    d = decoder(); f = format();
    for (unsigned i = 0; i < sizeof(codecs) / sizeof(codecs[0]); i++) {
        unsigned before_creates = creates, before_destroys = destroys;
        unsigned before_reservations = reservations, before_releases = releases;
        d.sequence_seen = 0; d.failed = 0;
        d.expected_codec = f.codec = codecs[i];
        assert(sequence(&d, &f) == 4);
        assert(d.codec == codecs[i] && created_codec == codecs[i]);
        assert(creates == before_creates + 1 && destroys == before_destroys + (i != 0));
        assert(reservations == before_reservations + 1 && releases == before_releases + (i != 0));
        assert(live_budgets == 1);
        // A subsequent compatible clip retains exactly one decoder and lease.
        d.sequence_seen = 0;
        assert(sequence(&d, &f) == 4);
        assert(creates == before_creates + 1 && reservations == before_reservations + 1);
        // Reject unexpected codec signaling and midclip codec changes before
        // destroying the current decoder or acquiring another reservation.
        f.codec = codecs[i] == cudaVideoCodec_HEVC ? cudaVideoCodec_AV1 : cudaVideoCodec_HEVC;
        assert(sequence(&d, &f) == 0 && d.failed == 2);
        d.failed = 0; d.expected_codec = f.codec;
        assert(sequence(&d, &f) == 0 && d.failed == 2);
        assert(creates == before_creates + 1 && destroys == before_destroys + (i != 0));
        assert(reservations == before_reservations + 1 && releases == before_releases + (i != 0));
        assert(live_budgets == 1);
    }
    fake_destroy(d.decoder); release_budget(&d);
    assert(!live_budgets && reservations == releases && creates == destroys);
    checked_death_failures();
    return 0;
}
