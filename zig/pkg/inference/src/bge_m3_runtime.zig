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

// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

/// One module owns both the internal runtime and the production Node boundary.
/// Keeping them under one root prevents Zig from compiling shared backend files
/// as members of two distinct modules in the managed BGE-M3 benchmark.
pub const internal = @import("inference_internal.zig");
pub const server = @import("server/server.zig");
pub const registry = @import("registry/registry.zig");
