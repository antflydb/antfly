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

//! Provider capability declarations without task execution or transport owners.
pub const Support = enum { unknown, unsupported, supported };
pub const Capabilities = struct {
    streaming: Support = .unknown,
    tools: Support = .unknown,
    structured_output: Support = .unknown,
    images: Support = .unknown,
    audio: Support = .unknown,

    pub fn require(self: Capabilities, request: Requirements) !void {
        inline for (.{ "streaming", "tools", "structured_output", "images", "audio" }) |field| {
            if (@field(request, field) and @field(self, field) != .supported)
                return error.UnsupportedInferenceFeature;
        }
    }
};
pub const Requirements = struct {
    streaming: bool = false,
    tools: bool = false,
    structured_output: bool = false,
    images: bool = false,
    audio: bool = false,
};
