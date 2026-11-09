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

// Shared provider-neutral planning; the inference adapter supplies exact counts.
const shared = @import("antfly_generating").context_management.planner;
pub const Role = shared.Role;
pub const ToolCallView = shared.ToolCallView;
pub const MessageView = shared.MessageView;
pub const Limits = shared.Limits;
pub const ToolEvidence = shared.ToolEvidence;
pub const UnresolvedToolCall = shared.UnresolvedToolCall;
pub const StructuredMemory = shared.StructuredMemory;
pub const CountInput = shared.CountInput;
pub const ExactCounter = shared.ExactCounter;
pub const Unit = shared.Unit;
pub const Inspection = shared.Inspection;
pub const WindowOptions = shared.WindowOptions;
pub const WindowPlan = shared.WindowPlan;
pub const inspect = shared.inspect;
pub const planWindow = shared.planWindow;
pub const CompactionSelection = shared.CompactionSelection;
pub const CompactionBatchPlan = shared.CompactionBatchPlan;
pub const planCompactionBatch = shared.planCompactionBatch;
pub const selectCompactionPrefix = shared.selectCompactionPrefix;
pub const Digest = shared.Digest;
pub const Checkpoint = shared.Checkpoint;
pub const digestPrefix = shared.digestPrefix;
pub const digestBytes = shared.digestBytes;
pub const digestCheckpoint = shared.digestCheckpoint;
pub const validateCheckpointParent = shared.validateCheckpointParent;
pub const validateCheckpoint = shared.validateCheckpoint;
pub const validateCheckpointSource = shared.validateCheckpointSource;
pub const renderMemory = shared.renderMemory;

pub const summary_instruction = shared.summary_instruction;
pub const validateSummaryEvidence = shared.validateSummaryEvidence;
pub const retainSmallToolResults = shared.retainSmallToolResults;
pub const buildCompactionFrame = shared.buildCompactionFrame;

pub const parseSummary = shared.parseSummary;

pub const verbatim_tool_result_max_bytes = shared.verbatim_tool_result_max_bytes;
