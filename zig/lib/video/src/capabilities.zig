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

/// Compile-time routes only. Hardware/profile availability is established by
/// session creation and the returned Batch.hardware receipt for each source.
pub const apple_decode_compiled = @import("builtin").os.tag == .macos;
pub const metal_preparation_compiled = apple_decode_compiled;
pub const metal_rgba_preparation_compiled = metal_preparation_compiled;
pub const portable_host_preparation = true;
pub const portable_mjpeg_decode = true;
pub const portable_h264_decode = true;
pub const portable_vp8_decode = true;
pub const portable_vp8_subset = "qualified-raw-vp8-versions-0to3-stateful-reference-pictures";
pub const portable_vp9_decode = true;
pub const portable_av1_decode = true;
pub const portable_h264_subset = "qualified-avc1-avc3-8to14bit-mono-420-422-444-dynamic-epochs-separate-planes-sp-si-partitions-mbaff-paff";
pub const nvdec_decode = false;
pub const cuda_preparation = false;

pub const portable_vp9_subset = "qualified-profiles-0to3-8to12bit-420-422-444-interframes-superframes";
pub const portable_av1_subset = "qualified-profiles0to2-8to12bit-mono-420-422-444-low-overhead-annexb-operating-points-interframes-film-grain";
