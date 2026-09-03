// Copyright 2026 Antfly, Inc.
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

pub const types = @import("types.zig");
pub const codec = @import("codec.zig");
pub const source_binding = @import("source_binding.zig");
pub const sidecar_manifest = @import("sidecar_manifest.zig");

pub const Entry = types.Entry;
pub const SidecarKind = source_binding.SidecarKind;
pub const SidecarSourceBinding = source_binding.Binding;
pub const SidecarSourceRowRefKind = source_binding.RowRefKind;
pub const SidecarDeclaredArtifact = sidecar_manifest.DeclaredArtifact;
pub const SidecarManifest = sidecar_manifest.Manifest;
pub const freeEntries = types.freeEntries;
pub const encodeAlloc = codec.encodeAlloc;
pub const decodeAlloc = codec.decodeAlloc;
pub const header_len = codec.header_len;
pub const decodeHeader = codec.decodeHeader;
pub const sidecarBindingFromSnapshot = source_binding.bindingFromSnapshot;
pub const validateSidecarBatchAgainstBinding = source_binding.validateBatchAgainstBinding;
pub const validateSidecarBatchSnapshotAgainstBinding = source_binding.validateBatchSnapshotAgainstBinding;
pub const validateSidecarCandidateRowRefsAgainstBinding = source_binding.validateCandidateRowRefsAgainstBinding;
pub const sameSidecarSourceSnapshot = source_binding.sameSourceSnapshot;
pub const sidecarRowRefKeyAlloc = source_binding.rowRefKeyAlloc;
pub const sidecarRowRefFromKeyAlloc = source_binding.rowRefFromKeyAlloc;
pub const sidecarRowRefsFromKeysAlloc = source_binding.rowRefsFromKeysAlloc;
pub const freeOwnedSidecarRowRef = source_binding.freeOwnedRowRef;
pub const freeOwnedSidecarRowRefs = source_binding.freeOwnedRowRefs;
pub const sidecarRowRefKindForSourceKind = source_binding.rowRefKindForSourceKind;
pub const artifactKindForSidecarKind = sidecar_manifest.artifactKindForSidecarKind;
pub const sidecarKindForArtifactKind = sidecar_manifest.sidecarKindForArtifactKind;
pub const validateSidecarArtifactBinding = sidecar_manifest.validateArtifactBinding;
pub const validateSidecarBatchAgainstDeclaredArtifact = sidecar_manifest.validateBatchAgainstDeclaredArtifact;
pub const validateSidecarManifestAgainstBaseSource = sidecar_manifest.validateManifestAgainstBaseSource;

test "serverless segment module compiles" {
    _ = types;
    _ = codec;
    _ = source_binding;
    _ = sidecar_manifest;
    _ = Entry;
    _ = header_len;
    _ = decodeHeader;
    _ = SidecarSourceBinding;
    _ = SidecarDeclaredArtifact;
    _ = SidecarManifest;
    _ = freeEntries;
    _ = encodeAlloc;
    _ = decodeAlloc;
    _ = sidecarBindingFromSnapshot;
    _ = validateSidecarBatchAgainstBinding;
    _ = validateSidecarBatchSnapshotAgainstBinding;
    _ = validateSidecarCandidateRowRefsAgainstBinding;
    _ = sameSidecarSourceSnapshot;
    _ = sidecarRowRefKeyAlloc;
    _ = sidecarRowRefFromKeyAlloc;
    _ = sidecarRowRefsFromKeysAlloc;
    _ = freeOwnedSidecarRowRef;
    _ = freeOwnedSidecarRowRefs;
    _ = sidecarRowRefKindForSourceKind;
    _ = artifactKindForSidecarKind;
    _ = sidecarKindForArtifactKind;
    _ = validateSidecarArtifactBinding;
    _ = validateSidecarBatchAgainstDeclaredArtifact;
    _ = validateSidecarManifestAgainstBaseSource;
}
