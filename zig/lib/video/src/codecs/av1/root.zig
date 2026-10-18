// Copyright © 2018-2019, VideoLAN and dav1d authors
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// Maintained Antfly Zig port; see LICENSE and PORTING.md.
comptime {
    _ = @import("cdef_apply_tmpl_16.zig");
    _ = @import("cdef_apply_tmpl_8.zig");
    _ = @import("cdef_tmpl_16.zig");
    _ = @import("cdef_tmpl_8.zig");
    _ = @import("cdf.zig");
    _ = @import("common.zig");
    _ = @import("ctx.zig");
    _ = @import("data.zig");
    _ = @import("decode.zig");
    _ = @import("dequant_tables.zig");
    _ = @import("fg_apply_tmpl_16.zig");
    _ = @import("fg_apply_tmpl_8.zig");
    _ = @import("filmgrain_tmpl_16.zig");
    _ = @import("filmgrain_tmpl_8.zig");
    _ = @import("getbits.zig");
    _ = @import("intra_edge.zig");
    _ = @import("ipred_prepare_tmpl_16.zig");
    _ = @import("ipred_prepare_tmpl_8.zig");
    _ = @import("ipred_tmpl_16.zig");
    _ = @import("ipred_tmpl_8.zig");
    _ = @import("itx_1d.zig");
    _ = @import("itx_tmpl_16.zig");
    _ = @import("itx_tmpl_8.zig");
    _ = @import("lf_apply_tmpl_16.zig");
    _ = @import("lf_apply_tmpl_8.zig");
    _ = @import("lf_mask.zig");
    _ = @import("lib.zig");
    _ = @import("loopfilter_tmpl_16.zig");
    _ = @import("loopfilter_tmpl_8.zig");
    _ = @import("looprestoration_tmpl_16.zig");
    _ = @import("looprestoration_tmpl_8.zig");
    _ = @import("lr_apply_tmpl_16.zig");
    _ = @import("lr_apply_tmpl_8.zig");
    _ = @import("mc_tmpl_16.zig");
    _ = @import("mc_tmpl_8.zig");
    _ = @import("mem.zig");
    _ = @import("msac.zig");
    _ = @import("obu.zig");
    _ = @import("pal.zig");
    _ = @import("picture.zig");
    _ = @import("qm.zig");
    _ = @import("recon_tmpl_16.zig");
    _ = @import("recon_tmpl_8.zig");
    _ = @import("ref.zig");
    _ = @import("refmvs.zig");
    _ = @import("scan.zig");
    _ = @import("tables.zig");
    _ = @import("thread_task.zig");
    _ = @import("warpmv.zig");
    _ = @import("wedge.zig");
    _ = @import("../runtime.zig");
}
