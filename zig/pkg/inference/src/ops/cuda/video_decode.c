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

#include "video_headers/dynlink_cuda.h"
#include "video_headers/dynlink_nvcuvid.h"
#include <dlfcn.h>
#include <stdlib.h>
#include <stdint.h>

// The callback borrows an NV12 surface on the caller's CUDA context/stream.
// It must finish reading before returning. No pixels ever cross the host ABI.
typedef int (*frame_callback)(void *, uint64_t, unsigned, unsigned, unsigned,
                             int64_t, unsigned, unsigned);
typedef struct {
    void *library, *user;
    CUvideoparser parser;
    CUvideodecoder decoder;
    CUstream stream;
    frame_callback frame;
    unsigned width, height, matrix, full_range, coded_width, coded_height;
    int failed, sequence_seen;
    unsigned surfaces, expected_width, expected_height;
    int display_left, display_top, matrix_override, range_override;
    tcuvidCreateVideoParser *create_parser;
    tcuvidDestroyVideoParser *destroy_parser;
    tcuvidParseVideoData *parse;
    tcuvidGetDecoderCaps *caps;
    tcuvidCreateDecoder *create_decoder;
    tcuvidDestroyDecoder *destroy_decoder;
    tcuvidDecodePicture *decode;
    tcuvidGetDecodeStatus *status;
    tcuvidMapVideoFrame64 *map;
    tcuvidUnmapVideoFrame64 *unmap;
} Decoder;

static int sequence(void *opaque, CUVIDEOFORMAT *f) {
    Decoder *d = opaque;
    unsigned w = f->display_area.right - f->display_area.left;
    unsigned h = f->display_area.bottom - f->display_area.top;
    unsigned matrix = f->video_signal_description.matrix_coefficients;
    unsigned full_range = f->video_signal_description.video_full_range_flag;
    unsigned primaries = f->video_signal_description.color_primaries;
    unsigned transfer = f->video_signal_description.transfer_characteristics;
    if (d->matrix_override >= 0 && matrix != 2 && matrix != (unsigned)d->matrix_override) { d->failed = 2; return 0; }
    if (d->range_override >= 0 && matrix != 2 && full_range != (unsigned)d->range_override) { d->failed = 2; return 0; }
    if (d->matrix_override >= 0) matrix = d->matrix_override;
    if (d->range_override >= 0) full_range = d->range_override;
    if (f->codec != cudaVideoCodec_H264 || !f->progressive_sequence ||
        f->chroma_format != cudaVideoChromaFormat_420 || w != d->expected_width || h != d->expected_height || f->bit_depth_luma_minus8 ||
        f->bit_depth_chroma_minus8 || !w || !h || (w&1) || (h&1) ||
        f->coded_width > 4096 || f->coded_height > 4096 ||
        f->display_area.left < 0 || f->display_area.top < 0 ||
        f->display_area.right > (int)f->coded_width || f->display_area.bottom > (int)f->coded_height ||
        (f->display_aspect_ratio.x && f->display_aspect_ratio.y &&
         (uint64_t)f->display_aspect_ratio.x * h != (uint64_t)f->display_aspect_ratio.y * w) ||
        (matrix != 1 && matrix != 2 && matrix != 5 && matrix != 6) ||
        (primaries != 1 && primaries != 2 && primaries != 5 && primaries != 6) ||
        (transfer != 1 && transfer != 2 && transfer != 6 && transfer != 13) ||
        !f->min_num_decode_surfaces || f->min_num_decode_surfaces > 32) { d->failed = 2; return 0; }
    // Never silently reinterpret a stream whose geometry or color changes.
    int compatible = d->width == w && d->height == h && d->coded_width == f->coded_width &&
        d->coded_height == f->coded_height && d->surfaces >= f->min_num_decode_surfaces &&
        d->display_left == f->display_area.left && d->display_top == f->display_area.top;
    if (d->sequence_seen) {
        if (!compatible || d->matrix != matrix || d->full_range != full_range) {
            d->failed = 2; return 0;
        }
        return d->surfaces;
    }
    d->sequence_seen = 1;
    if (d->decoder && compatible) {
        d->matrix = matrix; d->full_range = full_range;
        return d->surfaces;
    }
    if (d->decoder) { d->destroy_decoder(d->decoder); d->decoder = NULL; }
    CUVIDDECODECAPS caps = {0};
    caps.eCodecType = f->codec; caps.eChromaFormat = f->chroma_format;
    if (d->caps(&caps) || !caps.bIsSupported || !(caps.nOutputFormatMask & 1) ||
        f->coded_width > caps.nMaxWidth || f->coded_height > caps.nMaxHeight ||
        f->coded_width < caps.nMinWidth || f->coded_height < caps.nMinHeight ||
        ((f->coded_width+15)/16)*((f->coded_height+15)/16) > caps.nMaxMBCount) {
        d->failed = 2; return 0;
    }
    CUVIDDECODECREATEINFO c = {0};
    c.CodecType = f->codec; c.ChromaFormat = f->chroma_format;
    c.ulWidth = f->coded_width; c.ulHeight = f->coded_height;
    c.ulNumDecodeSurfaces = f->min_num_decode_surfaces;
    c.ulCreationFlags = cudaVideoCreate_PreferCUVID;
    c.OutputFormat = cudaVideoSurfaceFormat_NV12;
    c.DeinterlaceMode = cudaVideoDeinterlaceMode_Weave;
    c.ulTargetWidth = w; c.ulTargetHeight = h; c.ulNumOutputSurfaces = 1;
    c.display_area.left = f->display_area.left; c.display_area.top = f->display_area.top;
    c.display_area.right = f->display_area.right; c.display_area.bottom = f->display_area.bottom;
    if (d->create_decoder(&d->decoder, &c)) { d->failed = 3; return 0; }
    d->width = w; d->height = h; d->coded_width = f->coded_width; d->coded_height = f->coded_height;
    d->matrix = matrix; d->full_range = full_range;
    d->display_left = f->display_area.left; d->display_top = f->display_area.top;
    d->surfaces = c.ulNumDecodeSurfaces;
    return d->surfaces;
}
static int decode(void *opaque, CUVIDPICPARAMS *picture) {
    Decoder *d = opaque;
    if (d->decode(d->decoder, picture)) { d->failed = 3; return 0; }
    return 1;
}
static int display(void *opaque, CUVIDPARSERDISPINFO *picture) {
    Decoder *d = opaque;
    if (!picture) return 1;
    if (!picture->progressive_frame) { d->failed = 2; return 0; }
    // Ask whether this presentation timestamp is wanted before mapping a surface.
    int wanted = d->frame(d->user, 0, 0, d->width, d->height, picture->timestamp, d->matrix, d->full_range);
    if (wanted < 0) { d->failed = 4; return 0; }
    if (!wanted) return 1;
    CUVIDPROCPARAMS p = {0};
    p.progressive_frame = picture->progressive_frame;
    p.top_field_first = picture->top_field_first; p.output_stream = d->stream;
    unsigned long long device = 0; unsigned pitch = 0;
    if (d->map(d->decoder, picture->picture_index, &device, &pitch, &p)) { d->failed = 3; return 0; }
    CUVIDGETDECODESTATUS status = {0};
    if (d->status(d->decoder, picture->picture_index, &status) ||
        status.decodeStatus == cuvidDecodeStatus_Error || status.decodeStatus == cuvidDecodeStatus_Error_Concealed) {
        d->unmap(d->decoder, device); d->failed = 3; return 0;
    }
    int ok = d->frame(d->user, device, pitch, d->width, d->height, picture->timestamp, d->matrix, d->full_range);
    int unmapped = d->unmap(d->decoder, device);
    if (ok < 0 || unmapped) { d->failed = 4; return 0; }
    return 1;
}
void antfly_nvdec_close(Decoder *d) {
    if (!d) return;
    if (d->parser) d->destroy_parser(d->parser);
    if (d->decoder) d->destroy_decoder(d->decoder);
    if (d->library) dlclose(d->library);
    free(d);
}
int antfly_nvdec_open(Decoder **out, void *stream, void *user, frame_callback callback) {
    Decoder *d = calloc(1, sizeof(*d));
    if (!d) return 5;
    d->stream = stream; d->user = user; d->frame = callback;
    // Zig's debug allocator caches DSO unwind metadata. Keep libnvcuvid's code
    // and .eh_frame mapped after dlclose so later stack captures cannot follow
    // stale metadata between independently owned CUDA contexts.
    d->library = dlopen("libnvcuvid.so.1", RTLD_NOW | RTLD_LOCAL | RTLD_NODELETE);
    if (!d->library) { antfly_nvdec_close(d); return 1; }
#define LOAD(field, symbol) if (!(d->field = dlsym(d->library, symbol))) { antfly_nvdec_close(d); return 1; }
    LOAD(create_parser,"cuvidCreateVideoParser") LOAD(destroy_parser,"cuvidDestroyVideoParser")
    LOAD(parse,"cuvidParseVideoData") LOAD(caps,"cuvidGetDecoderCaps")
    LOAD(create_decoder,"cuvidCreateDecoder") LOAD(destroy_decoder,"cuvidDestroyDecoder")
    LOAD(status,"cuvidGetDecodeStatus")
    LOAD(decode,"cuvidDecodePicture") LOAD(map,"cuvidMapVideoFrame64") LOAD(unmap,"cuvidUnmapVideoFrame64")
#undef LOAD
    *out = d; return 0;
}
// The previous request must have drained EOS before reuse. A new parser owns
// the new stream's reference-picture state; compatible decoder surfaces persist.
int antfly_nvdec_begin(Decoder *d, void *stream, void *user, frame_callback callback, unsigned width, unsigned height, int matrix, int range) {
    if (d->parser) return 3;
    d->stream = stream; d->user = user; d->frame = callback;
    d->failed = 0; d->sequence_seen = 0;
    d->expected_width = width; d->expected_height = height; d->matrix_override = matrix; d->range_override = range;
    CUVIDPARSERPARAMS p = {0};
    p.CodecType = cudaVideoCodec_H264; p.ulMaxNumDecodeSurfaces = 1;
    p.ulClockRate = 1000000; p.ulMaxDisplayDelay = 4;
    p.pUserData = d; p.pfnSequenceCallback = sequence; p.pfnDecodePicture = decode; p.pfnDisplayPicture = display;
    return d->create_parser(&d->parser, &p) ? 3 : 0;
}
void antfly_nvdec_finish(Decoder *d) {
    if (d->parser) d->destroy_parser(d->parser);
    d->parser = NULL; d->user = NULL; d->frame = NULL;
}

int antfly_nvdec_packet(Decoder *d, const unsigned char *bytes, unsigned size, int64_t pts, int eos) {
    CUVIDSOURCEDATAPACKET p = {0};
    p.flags = eos ? CUVID_PKT_ENDOFSTREAM : CUVID_PKT_TIMESTAMP;
    p.payload = bytes; p.payload_size = size; p.timestamp = pts;
    int result = d->parse(d->parser, &p);
    return d->failed ? d->failed : (result ? 3 : 0);
}
