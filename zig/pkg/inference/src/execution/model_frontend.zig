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

//! Loaded models publish semantic stages independently of checkpoint format,
//! execution target and task orchestration. Adapters interpret configuration
//! and weight names; their forwards use the ordinary shared backend operations.
const capture = @import("capture.zig");

pub const Stage = enum {
    forward,
    embedding,
    encoder,
    decoder,
    image_projection,
    audio_projection,
    readout,
};

/// The loaded model retains context and the returned frontend's immutable ABI.
/// Resolving a stage performs no numerical execution or backend construction.
pub const Provider = struct {
    context: *anyopaque,
    resolve: *const fn (*anyopaque, Stage) anyerror!?capture.Forward,

    pub fn get(self: Provider, stage: Stage) !?capture.Forward {
        return self.resolve(self.context, stage);
    }
};
