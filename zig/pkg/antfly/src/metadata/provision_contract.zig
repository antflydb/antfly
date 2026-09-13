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

//! Storage-free result contract for coarse local replica-root provisioning.

pub const ProvisionSummary = struct {
    groups_considered: usize = 0,
    dbs_opened: usize = 0,
    indexes_added: usize = 0,
    indexes_removed: usize = 0,
    indexes_pending: usize = 0,
    enrichments_added: usize = 0,
    enrichments_updated: usize = 0,
    enrichments_removed: usize = 0,
    resolvers_added: usize = 0,
    resolvers_updated: usize = 0,
    resolvers_removed: usize = 0,

    pub fn merge(self: *@This(), other: @This()) void {
        self.groups_considered += other.groups_considered;
        self.dbs_opened += other.dbs_opened;
        self.indexes_added += other.indexes_added;
        self.indexes_removed += other.indexes_removed;
        self.indexes_pending += other.indexes_pending;
        self.enrichments_added += other.enrichments_added;
        self.enrichments_updated += other.enrichments_updated;
        self.enrichments_removed += other.enrichments_removed;
        self.resolvers_added += other.resolvers_added;
        self.resolvers_updated += other.resolvers_updated;
        self.resolvers_removed += other.resolvers_removed;
    }

    pub fn indexManagerCatalogChanged(self: @This()) bool {
        return self.indexes_added > 0 or
            self.indexes_removed > 0 or
            self.resolvers_added > 0 or
            self.resolvers_updated > 0 or
            self.resolvers_removed > 0;
    }
};
