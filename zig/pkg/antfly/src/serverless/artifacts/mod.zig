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

pub const store = @import("store.zig");
pub const fs_store = @import("fs_store.zig");
pub const remote_store = @import("remote_store.zig");

pub const ArtifactMetadata = store.ArtifactMetadata;
pub const ArtifactStore = store.ArtifactStore;
pub const sha256ChecksumFromArtifactId = store.sha256ChecksumFromArtifactId;
pub const validateSha256ArtifactIdentity = store.validateSha256ArtifactIdentity;
pub const validateSha256Checksum = store.validateSha256Checksum;
pub const sha256DigestFromChecksum = store.sha256DigestFromChecksum;
pub const validatePayloadSha256WithCancellation = store.validatePayloadSha256WithCancellation;
pub const FsStore = fs_store.FsStore;
pub const RemoteStore = remote_store.RemoteStore;

test "serverless artifacts module compiles" {
    _ = store;
    _ = fs_store;
    _ = remote_store;
    _ = ArtifactMetadata;
    _ = ArtifactStore;
    _ = sha256ChecksumFromArtifactId;
    _ = validateSha256ArtifactIdentity;
    _ = validateSha256Checksum;
    _ = validatePayloadSha256WithCancellation;
    _ = FsStore;
    _ = RemoteStore;
}
