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

pub const types = @import("types.zig");
pub const fixed_text = @import("fixed_text.zig");
pub const fixed_multimodal = @import("fixed_multimodal.zig");
pub const wav = @import("wav.zig");
pub const png = @import("png.zig");

pub const Chunk = types.Chunk;
pub const Input = types.Input;
pub const BinaryInput = types.BinaryInput;
pub const FixedTextConfig = types.FixedTextConfig;
pub const FixedChunkConfig = types.FixedChunkConfig;
pub const AudioChunkOptions = types.AudioChunkOptions;

test {
    _ = @import("types.zig");
    _ = @import("fixed_text.zig");
    _ = @import("fixed_multimodal.zig");
    _ = @import("wav.zig");
    _ = @import("png.zig");
}
