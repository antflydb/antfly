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

pub const decoded_cache = @import("decoded_cache.zig");
pub const types = @import("types.zig");
pub const codec = @import("codec.zig");
pub const store = @import("store.zig");
pub const fs_store = @import("fs_store.zig");
pub const remote_store = @import("remote_store.zig");
pub const artifact_ref = @import("antfly_local_sources").serverless_manifest_artifact_ref;
pub const base_source = @import("antfly_local_sources").serverless_manifest_base_source;
pub const compatibility = @import("compatibility.zig");
pub const read_lease = @import("read_lease.zig");

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
    _ = read_lease;
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
