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
    _ = FsProgressStore;
    _ = RemoteProgressStore;
    _ = CatalogService;
}
