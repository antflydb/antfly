// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

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
