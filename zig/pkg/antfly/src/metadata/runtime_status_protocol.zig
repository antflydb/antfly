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
/// V14 gates metadata transitions whose trailing native-restore identity is
/// mandatory and StoreRecord's data-plane native-generation capability.
pub const native_restore_identity_record_version: u16 = 14;
/// V15 carries per-artifact replay observations and the store-level protocol
/// capability used to keep artifact-index admission closed during rolling
/// upgrades.
pub const artifact_source_status_record_version: u16 = 15;
/// V16 carries source-local terminal failure state. This keeps one failed
/// enrichment stream from poisoning otherwise ready sources in a multi-source
/// index while retaining V15 decoding during rolling upgrades.
pub const artifact_source_failure_status_record_version: u16 = 16;
/// V17 carries readiness-critical native dense-vector projection debt across
/// the data-to-metadata status boundary.
pub const vector_projection_record_version: u16 = 17;
/// V18 carries the durable native dense-storage rollout phase. Enum ordinals
/// are fixed by common/dense_native_storage_phase.zig.
pub const dense_native_storage_record_version: u16 = 18;
/// V19 frames every index status and terminates its stable core with a
/// length-delimited TLV extension area. Unknown extensions can be skipped
/// during rolling upgrades without positional tail arithmetic.
pub const framed_index_status_record_version: u16 = 19;
/// V20 carries the data-plane native HBC authority capability in StoreRecord's
/// framed extension. Activation must precede publishing that capability so an
/// older metadata voter never encounters the new extension version.
pub const dense_native_capability_record_version: u16 = 20;
pub const current_record_version: u16 = dense_native_capability_record_version;
