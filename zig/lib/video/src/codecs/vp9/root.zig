// Copyright (c) 2010, The WebM Project authors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
// Redistributions of source code must retain the above copyright
//     notice, this list of conditions and the following disclaimer.
//
// Redistributions in binary form must reproduce the above copyright
//     notice, this list of conditions and the following disclaimer in
//     the documentation and/or other materials provided with the
//     distribution.
//
// Neither the name of Google, nor the WebM Project, nor the names
//     of its contributors may be used to endorse or promote products
//     derived from this software without specific prior written
//     permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
//
// Maintained Antfly Zig port; see LICENSE and PORTING.md.
comptime {
    _ = @import("bitreader.zig");
    _ = @import("bitreader_buffer.zig");
    _ = @import("common.zig");
    _ = @import("intrapred.zig");
    _ = @import("inv_txfm.zig");
    _ = @import("loopfilter.zig");
    _ = @import("prob.zig");
    _ = @import("vp9_alloccommon.zig");
    _ = @import("vp9_blockd.zig");
    _ = @import("vp9_common_data.zig");
    _ = @import("vp9_decodeframe.zig");
    _ = @import("vp9_decodemv.zig");
    _ = @import("vp9_decoder.zig");
    _ = @import("vp9_detokenize.zig");
    _ = @import("vp9_dsubexp.zig");
    _ = @import("vp9_dx_iface.zig");
    _ = @import("vp9_entropy.zig");
    _ = @import("vp9_entropymode.zig");
    _ = @import("vp9_entropymv.zig");
    _ = @import("vp9_filter.zig");
    _ = @import("vp9_frame_buffers.zig");
    _ = @import("vp9_idct.zig");
    _ = @import("vp9_iface_common.zig");
    _ = @import("vp9_job_queue.zig");
    _ = @import("vp9_loopfilter.zig");
    _ = @import("vp9_pred_common.zig");
    _ = @import("vp9_quant_common.zig");
    _ = @import("vp9_reconinter.zig");
    _ = @import("vp9_reconintra.zig");
    _ = @import("vp9_rtcd.zig");
    _ = @import("vp9_scale.zig");
    _ = @import("vp9_scan.zig");
    _ = @import("vp9_seg_common.zig");
    _ = @import("vp9_thread_common.zig");
    _ = @import("vp9_tile_common.zig");
    _ = @import("vpx_codec.zig");
    _ = @import("vpx_convolve.zig");
    _ = @import("vpx_decoder.zig");
    _ = @import("vpx_dsp_rtcd.zig");
    _ = @import("vpx_mem.zig");
    _ = @import("vpx_scale_rtcd.zig");
    _ = @import("vpx_thread.zig");
    _ = @import("yv12config.zig");
    _ = @import("../runtime.zig");
}
