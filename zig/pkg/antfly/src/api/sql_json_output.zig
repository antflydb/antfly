// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! HTTP facade for the shared controlled SQL encoder.
const shared = @import("antfly_local_sources").sql_cooperative_json_output;
pub const Options = shared.Options;
pub const Buffer = shared.Buffer;
pub const Encoder = shared.Encoder;
pub const copyAlloc = shared.copyAlloc;
pub const valueAlloc = shared.valueAlloc;
