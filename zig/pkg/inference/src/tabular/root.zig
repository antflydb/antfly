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

//! Inference-side predictor surface: registry, discovery, HTTP handlers,
//! CLI subcommand. Bridges `lib/ml/tabular` to the inference server.

pub const manifest = @import("manifest.zig");
pub const limits = @import("limits.zig");
pub const registry = @import("registry.zig");
pub const discovery = @import("discovery.zig");
pub const http = @import("http.zig");
pub const cli = @import("cli.zig");

test {
    _ = manifest;
    _ = limits;
    _ = registry;
    _ = discovery;
    _ = http;
    _ = cli;
}
