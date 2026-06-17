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
pub const store = @import("store.zig");
pub const fs_store = @import("fs_store.zig");
pub const remote_store = @import("remote_store.zig");
pub const artifact_ref = @import("artifact_ref.zig");
pub const base_source = @import("base_source.zig");
pub const compatibility = @import("compatibility.zig");

pub const ArtifactKind = types.ArtifactKind;
pub const ArtifactRef = types.ArtifactRef;
pub const BaseSourceKind = types.BaseSourceKind;
pub const ExternalBaseFormat = types.ExternalBaseFormat;
pub const AntflyFragmentBaseSource = types.AntflyFragmentBaseSource;
pub const ExternalBaseSource = types.ExternalBaseSource;
pub const BaseSourceDescriptor = types.BaseSourceDescriptor;
pub const LakeManifestCompatibilityPolicy = compatibility.Policy;
pub const LakeManifestCompatibilityReport = compatibility.Report;
pub const PublishedGenerationStats = types.PublishedGenerationStats;
pub const ManifestStats = types.ManifestStats;
pub const PublishedGeneration = types.PublishedGeneration;
pub const Manifest = types.Manifest;
pub const cloneManifest = types.cloneManifest;
pub const freeManifest = types.freeManifest;
pub const encodeAlloc = codec.encodeAlloc;
pub const decodeAlloc = codec.decodeAlloc;
pub const PublishResult = store.PublishResult;
pub const ManifestStore = store.ManifestStore;
pub const FsStore = fs_store.FsStore;
pub const RemoteStore = remote_store.RemoteStore;
pub const checkLakeManifestCompatibility = compatibility.checkLakeBaseSource;

test "serverless manifest module compiles" {
    _ = types;
    _ = codec;
    _ = store;
    _ = fs_store;
    _ = remote_store;
    _ = artifact_ref;
    _ = base_source;
    _ = compatibility;
    _ = ArtifactKind;
    _ = ArtifactRef;
    _ = BaseSourceDescriptor;
    _ = LakeManifestCompatibilityPolicy;
    _ = LakeManifestCompatibilityReport;
    _ = PublishedGenerationStats;
    _ = ManifestStats;
    _ = PublishedGeneration;
    _ = Manifest;
    _ = cloneManifest;
    _ = freeManifest;
    _ = encodeAlloc;
    _ = decodeAlloc;
    _ = PublishResult;
    _ = ManifestStore;
    _ = FsStore;
    _ = RemoteStore;
    _ = checkLakeManifestCompatibility;
}
