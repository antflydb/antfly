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

const internal = @import("inference_internal");
test "GLiNER span objectives match finite difference gradients" {
    try internal.finetune.gliner_span_head.testLossGradients();
}
test "GLiNER span affine autodiff updates and resume agree" {
    try internal.finetune.gliner_span_head.testUpdateAndResume();
}
test "GLiNER span training rejects overlapping content and ids" {
    try internal.finetune.gliner_span_job.testDisjoint();
}

test "GLiNER span gradient matrix layouts match independent scalar contractions" {
    try internal.finetune.gliner_span_head.testDenseContractions();
}

test "GLiNER span soft-target evaluation retains human gold" {
    try internal.finetune.gliner_span_job.testSoftTargetGold();
}

test "GLiNER span resume binds each dataset partition" {
    try internal.finetune.gliner_span_job.testRunIdentity();
}
test "GLiNER span resume preserves best selection and rejects changed checkpoints" {
    try internal.finetune.gliner_span_job.testSelectionResume();
}
