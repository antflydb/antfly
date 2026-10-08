// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
/// Compile-time routes only. Hardware/profile availability is established by
/// session creation and the returned Batch.hardware receipt for each source.
pub const apple_decode_compiled = @import("builtin").os.tag == .macos;
pub const metal_preparation_compiled = apple_decode_compiled;
pub const portable_host_preparation = true;
pub const portable_h264_decode = false;
pub const nvdec_decode = false;
pub const cuda_preparation = false;
