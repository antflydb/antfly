// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy
// of the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations.

/// Runtime-status records are embedded in unframed StoreRecord transitions.
/// A writer must continue emitting `legacy_record_version` until every
/// metadata replica that can apply the transition advertises support for the
/// current version.
pub const legacy_record_version: u16 = 12;
/// V13 is the first (unreleased) format carrying both compact repair state and
/// the store reporter-incarnation fence.
pub const repair_status_record_version: u16 = 13;
/// V14 carries per-artifact replay observations and the store-level protocol
/// capability used to keep artifact-index admission closed during rolling
/// upgrades.
pub const artifact_source_status_record_version: u16 = 14;
pub const current_record_version: u16 = artifact_source_status_record_version;
