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

//! Semantic loading, preparation and execution boundaries. Target backends
//! retain their specialized schedules behind these common owners.
pub const LoadedModel = @import("../architectures/model_definition.zig").LoadedModel;
pub const ModelProgram = @import("program.zig").ModelProgram;
pub const Binding = @import("program.zig").Binding;
pub const Executable = @import("executable.zig").Executable;
pub const prepare = @import("executable.zig").prepare;
pub const prepareWithOptions = @import("executable.zig").prepareWithOptions;
pub const PrepareOptions = @import("executable.zig").PrepareOptions;
pub const BackendContext = @import("../backends/compute_context.zig").BackendContext;
pub const kernels = @import("../ops/kernel_descriptor.zig");
pub const diagnostics = @import("diagnostics.zig");
pub const prepareAnalyzedGraph = @import("executable.zig").prepareAnalyzedGraph;
pub const model_candidate = @import("model_candidate.zig");
pub const ModelCandidate = model_candidate.Candidate;
pub const transfer_audit = @import("transfer_audit.zig");
