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

// Shared with provider-backed gateway dictation.
pub const Style = @import("antfly_generating").dictation.Style;
pub const Options = @import("antfly_generating").dictation.Options;
pub const validate = @import("antfly_generating").dictation.validate;
pub const needsCleanup = @import("antfly_generating").dictation.needsCleanup;
pub const buildSystemPrompt = @import("antfly_generating").dictation.buildSystemPrompt;
pub const buildUserPrompt = @import("antfly_generating").dictation.buildUserPrompt;
pub const suggestedMaxTokens = @import("antfly_generating").dictation.suggestedMaxTokens;
pub const normalizeOutput = @import("antfly_generating").dictation.normalizeOutput;
pub const max_dictionary_entries = @import("antfly_generating").dictation.max_dictionary_entries;
pub const max_dictionary_entry_bytes = @import("antfly_generating").dictation.max_dictionary_entry_bytes;
pub const max_context_bytes = @import("antfly_generating").dictation.max_context_bytes;
pub const max_instructions_bytes = @import("antfly_generating").dictation.max_instructions_bytes;
