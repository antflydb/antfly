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

const decode = @import("decode.zig");
const encode = @import("encode.zig");

pub const box = @import("box.zig");
pub const markers = @import("markers.zig");
pub const codestream = @import("codestream.zig");
pub const arithmetic = @import("arithmetic.zig");
pub const codeblock = @import("codeblock.zig");
pub const wavelet = @import("wavelet.zig");
pub const reconstruct = @import("reconstruct.zig");
pub const color = @import("color.zig");
pub const color_transform = @import("color_transform.zig");
pub const tagtree = @import("tagtree.zig");
pub const tile = @import("tile.zig");
pub const upsample = @import("upsample.zig");
pub const packet = @import("packet.zig");
pub const quantization = @import("quantization.zig");
pub const tier1_encode = @import("tier1_encode.zig");
pub const tier2_encode = @import("tier2_encode.zig");
pub const rate_control = @import("rate_control.zig");
pub const codestream_write = @import("codestream_write.zig");
pub const conformance = @import("../conformance.zig").jpeg2000;
pub const Format = decode.Format;
pub const Header = decode.Header;
pub const DecodedImage = decode.DecodedImage;
pub const DecodedImageU16 = decode.DecodedImageU16;
pub const DecodeBackend = decode.DecodeBackend;
pub const NativeDecodeSupport = decode.NativeDecodeSupport;

pub const decodeHeader = decode.decodeHeader;
pub const decodeHeaderBytes = decode.decodeHeaderBytes;
pub const decodeU8 = decode.decodeU8;
pub const decodeU8Bytes = decode.decodeU8Bytes;
pub const decodeU16Bytes = decode.decodeU16Bytes;
pub const nativeDecodeSupport = decode.nativeDecodeSupport;
pub const nativeDecodeSupportBytes = decode.nativeDecodeSupportBytes;

pub const encodeU8 = encode.encodeU8;
pub const encodeU8Bytes = encode.encodeU8Bytes;
pub const encodeU16 = encode.encodeU16;
pub const encodeU16Bytes = encode.encodeU16Bytes;
pub const EncodeParams = encode.EncodeParams;
pub const EncodeBackend = encode.EncodeBackend;

test {
    _ = box;
    _ = markers;
    _ = arithmetic;
    _ = codeblock;
    _ = wavelet;
    _ = tagtree;
    _ = tile;
    _ = upsample;
    _ = quantization;
    _ = color_transform;
    _ = tier1_encode;
    _ = tier2_encode;
    _ = encode;
    _ = decode;
    _ = rate_control;
    _ = codestream_write;
    _ = codestream;
    _ = reconstruct;
    _ = packet;
    _ = conformance;
    _ = @import("cross_validation.zig");
}
