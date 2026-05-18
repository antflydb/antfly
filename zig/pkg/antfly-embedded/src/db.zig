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

const embedded = @import("embedded_db_surface");

pub const OpenOptions = embedded.OpenOptions;
pub const types = embedded.types;
pub const Profile = embedded.Profile;
pub const RemoteTemplateRenderConfig = embedded.RemoteTemplateRenderConfig;
pub const RemoteTemplateRenderer = embedded.RemoteTemplateRenderer;
pub const DB = embedded.DB;
pub const Storage = embedded.lsm_storage.Storage;
pub const MemoryStorage = embedded.lsm_storage.MemoryStorage;
pub const EnrichmentConfig = embedded.enrichment_runtime.Config;
pub const DenseEmbedder = embedded.enrichment_embedder.DenseEmbedder;
pub const setRemoteTemplateRenderer = embedded.setRemoteTemplateRenderer;
pub const renderRemoteTemplateText = embedded.renderRemoteTemplateText;
