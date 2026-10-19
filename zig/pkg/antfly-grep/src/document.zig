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

//! Shared source contract; server and CLI validate exactly the same schema.
const shared = @import("antfly_grep_document");
pub const Provenance = shared.Provenance;
pub const Document = shared.Document;
pub const allowed = shared.allowed;
pub const field = shared.field;
pub const at = shared.at;
pub const integer = shared.integer;
pub const string = shared.string;
pub const localUri = shared.localUri;
pub const localUriAt = shared.localUriAt;
test {
    _ = shared;
}
