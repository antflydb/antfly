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

const user_manager = @import("user_manager.zig");
const storage_adapter = @import("storage_adapter.zig");

pub const MemoryStore = user_manager.MemoryStore;
pub const AuthSubjectEntry = user_manager.AuthSubjectEntry;
pub const AuthSubjectKind = user_manager.AuthSubjectKind;
pub const ApiKey = user_manager.ApiKey;
pub const ApiKeyRecord = user_manager.ApiKeyRecord;
pub const CreatedApiKey = user_manager.CreatedApiKey;
pub const default_rbac_model_text = user_manager.default_rbac_model_text;
pub const ensureDefaultAdminUser = user_manager.ensureDefaultAdminUser;
pub const initDefaultEnforcer = user_manager.initDefaultEnforcer;
pub const Permission = user_manager.Permission;
pub const PermissionType = user_manager.PermissionType;
pub const ResourceType = user_manager.ResourceType;
pub const RowFilterEntry = user_manager.RowFilterEntry;
pub const User = user_manager.User;
pub const ValidatedApiKey = user_manager.ValidatedApiKey;
pub const UserManager = user_manager.UserManager;
pub const UserStore = user_manager.UserStore;
pub const StorageCasbinAdapter = storage_adapter.StorageCasbinAdapter;
pub const StorageUserStore = storage_adapter.StorageUserStore;

test {
    _ = storage_adapter;
}
