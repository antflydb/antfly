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

//! Private Antfarm model-cache controls. These wire types are deliberately not
//! part of the public OpenAPI schema or generated client SDKs. A loaded cache
//! entry may stream its weights; it does not promise full device residency.
const api = @import("inference_api");

pub const ModelRuntime = struct {
    runtime_id: []const u8,
    state: enum { loading, loaded, in_use },
    runtime_kind: enum { primary, composite },
    active_requests: u64,
    startup_managed: bool,
    backend: ?[]const u8 = null,
    device: ?[]const u8 = null,
    residency_mode: enum { backend_managed, streamed, resident },
    // Elapsed age since last use, not a wall-clock timestamp.
    last_used_ms: u64,
};

pub const ModelRuntimeModel = struct {
    task: api.ModelKind,
    model: []const u8,
    load_state: []const u8,
    runtimes: []const ModelRuntime,
};

pub const ModelRuntimeResponse = struct {
    object: []const u8 = "model_runtime",
    models: []const ModelRuntimeModel,
};

pub const ModelRuntimeLoadRequest = struct {
    task: api.ModelKind,
    model: []const u8,
};

pub const ModelRuntimeLoadResponse = struct {
    outcome: []const u8 = "loaded",
    model: ModelRuntimeModel,
};

pub const ModelRuntimeUnloadRequest = struct { runtime_id: []const u8 };
pub const ModelRuntimeUnloadResponse = struct { outcome: []const u8 };
