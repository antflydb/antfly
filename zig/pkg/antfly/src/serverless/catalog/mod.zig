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

pub const types = @import("types.zig");
pub const store = @import("store.zig");
pub const fs_store = @import("fs_store.zig");
pub const object_store = @import("object_store.zig");
pub const remote_store = @import("remote_store.zig");
pub const progress_store = @import("progress_store.zig");
pub const fs_progress_store = @import("fs_progress_store.zig");
pub const remote_progress_store = @import("remote_progress_store.zig");
pub const service = @import("service.zig");

pub const NamespaceRecord = types.NamespaceRecord;
pub const TableNamespaceRecord = types.TableNamespaceRecord;
pub const DefaultQueryView = types.DefaultQueryView;
pub const EnrichmentStage = types.EnrichmentStage;
pub const EnrichmentModelPreference = types.EnrichmentModelPreference;
pub const EnrichmentFailurePolicy = types.EnrichmentFailurePolicy;
pub const NamespacePolicy = types.NamespacePolicy;
pub const TablePublicationState = types.TablePublicationState;
pub const BuildStatus = types.BuildStatus;
pub const CatalogStore = store.CatalogStore;
pub const FsStore = fs_store.FsStore;
pub const ObjectStore = object_store.ObjectStore;
pub const RemoteStore = remote_store.RemoteStore;
pub const ProgressStore = progress_store.ProgressStore;
pub const EnrichmentStageProgress = progress_store.EnrichmentStageProgress;
pub const FsProgressStore = fs_progress_store.FsProgressStore;
pub const RemoteProgressStore = remote_progress_store.RemoteProgressStore;
pub const CatalogService = service.CatalogService;

test "serverless catalog module compiles" {
    _ = types;
    _ = store;
    _ = fs_store;
    _ = object_store;
    _ = remote_store;
    _ = progress_store;
    _ = fs_progress_store;
    _ = remote_progress_store;
    _ = service;
    _ = NamespaceRecord;
    _ = TableNamespaceRecord;
    _ = DefaultQueryView;
    _ = EnrichmentStage;
    _ = EnrichmentModelPreference;
    _ = EnrichmentFailurePolicy;
    _ = NamespacePolicy;
    _ = TablePublicationState;
    _ = BuildStatus;
    _ = CatalogStore;
    _ = FsStore;
    _ = ObjectStore;
    _ = RemoteStore;
    _ = ProgressStore;
    _ = EnrichmentStageProgress;
    _ = FsProgressStore;
    _ = RemoteProgressStore;
    _ = CatalogService;
}
