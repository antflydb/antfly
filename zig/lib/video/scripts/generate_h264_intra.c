// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Offline fixture encoder only. The runtime does not link x264.
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <x264.h>
int main(int argc, char **argv) {
    if (argc != 8 && argc != 9 && argc != 10) return 2;
    FILE *in = fopen(argv[1], "rb"), *out = fopen(argv[2], "wb");
    if (!in || !out) return 3;
    int width = atoi(argv[3]), height = atoi(argv[4]), frames = atoi(argv[5]), qp = atoi(argv[6]);
    if (width < 16 || height < 16 || (width & 1) || (height & 1) || frames < 1 || qp < 1 || qp > 51) return 12;
    x264_param_t param;
    if (x264_param_default_preset(&param, "medium", "zerolatency")) return 4;
    param.i_width = width; param.i_height = height; param.i_csp = X264_CSP_I420;
    param.i_fps_num = 4; param.i_fps_den = 1;
    param.i_threads = 1; param.i_keyint_max = 1; param.i_bframe = 0;
    param.b_cabac = 0; param.b_deblocking_filter = argc >= 9 ? atoi(argv[8]) : 0;
    param.vui.b_fullrange = atoi(argv[7]);
    param.analyse.intra = argc == 10 && atoi(argv[9]) ? X264_ANALYSE_I4x4 : 0; param.analyse.inter = 0;
    param.analyse.b_transform_8x8 = 0; param.i_frame_reference = 1;
    param.rc.i_rc_method = X264_RC_CQP; param.rc.i_qp_constant = qp;
    param.b_repeat_headers = 1; param.b_annexb = 1;
    if (x264_param_apply_profile(&param, "baseline")) return 5;
    x264_t *encoder = x264_encoder_open(&param); if (!encoder) return 6;
    x264_picture_t picture, result;
    if (x264_picture_alloc(&picture, X264_CSP_I420, width, height)) return 7;
    for (int frame = 0; frame < frames; ++frame) {
        for (int p = 0; p < 3; ++p) {
            int plane_width = p == 0 ? width : width / 2, plane_height = p == 0 ? height : height / 2;
            for (int row = 0; row < plane_height; ++row)
                if (fread(picture.img.plane[p] + row * picture.img.i_stride[p], 1, plane_width, in) != (size_t)plane_width) return 8;
        }
        picture.i_pts = frame; picture.i_type = X264_TYPE_IDR;
        x264_nal_t *nals; int count;
        if (x264_encoder_encode(encoder, &nals, &count, &picture, &result) < 0) return 9;
        for (int n = 0; n < count; ++n)
            if (fwrite(nals[n].p_payload, 1, nals[n].i_payload, out) != (size_t)nals[n].i_payload) return 10;
    }
    if (x264_encoder_delayed_frames(encoder) != 0) return 11;
    x264_picture_clean(&picture); x264_encoder_close(encoder);
    fclose(in); fclose(out); return 0;
}
