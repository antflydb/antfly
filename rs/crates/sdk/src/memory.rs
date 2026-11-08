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

//! Simple path for native agent memory: brief, recall, remember, outcome.
//!
//! Mutations take an optional idempotency key; when it is `None` a fresh key
//! is generated per call. Pass the same key when retrying so the server
//! applies the write once. The generated [`Client`] methods remain available
//! for the full memory API.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{Client, Error, types};
use sha2::{Digest, Sha256};

type MemoryResult<T> = Result<T, Error<types::Error>>;

/// A handle for one memory space, optionally scoped to a database, namespace
/// or branch.
#[derive(Debug, Clone)]
pub struct MemorySpace<'a> {
    client: &'a Client,
    space: String,
    database: Option<String>,
    namespace: Option<String>,
    branch: Option<String>,
}

impl Client {
    /// Returns a convenience handle for the named memory space.
    pub fn memory_space(&self, space: impl Into<String>) -> MemorySpace<'_> {
        MemorySpace {
            client: self,
            space: space.into(),
            database: None,
            namespace: None,
            branch: None,
        }
    }
}

/// Returns a new random idempotency key (32 hex characters).
pub fn new_idempotency_key() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    (0u64..2)
        .map(|salt| {
            // RandomState is seeded from the OS; the counter keeps keys unique
            // within this process.
            let mut hasher = RandomState::new().build_hasher();
            hasher.write_u64(count);
            hasher.write_u128(nanos);
            hasher.write_u64(salt);
            format!("{:016x}", hasher.finish())
        })
        .collect()
}

fn idempotency_key<K>(explicit: Option<&str>) -> MemoryResult<K>
where
    K: std::str::FromStr,
    K::Err: std::fmt::Display,
{
    let key = explicit.map_or_else(new_idempotency_key, str::to_owned);
    key.parse()
        .map_err(|error: K::Err| Error::InvalidRequest(format!("invalid Idempotency-Key: {error}")))
}

impl<'a> MemorySpace<'a> {
    /// Sends `X-Antfly-Database` on every request.
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Sends `X-Antfly-Namespace` on every request.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Sends `X-Antfly-Memory-Branch` on every request.
    pub fn branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    /// The memory space name.
    pub fn name(&self) -> &str {
        &self.space
    }

    fn scope(&self) -> (Option<&str>, Option<&str>, Option<&str>) {
        (
            self.database.as_deref(),
            self.branch.as_deref(),
            self.namespace.as_deref(),
        )
    }

    /// Returns the small context an agent should load at session start.
    pub async fn brief(
        &self,
        request: &types::MemoryBriefRequest,
    ) -> MemoryResult<types::MemoryBrief> {
        let (db, branch, ns) = self.scope();
        Ok(self
            .client
            .brief_memory(&self.space, db, branch, ns, request)
            .await?
            .into_inner())
    }

    /// Searches the space for memories relevant to `request`.
    pub async fn recall(
        &self,
        request: &types::MemoryRecallRequest,
    ) -> MemoryResult<types::MemoryRecallResult> {
        let (db, branch, ns) = self.scope();
        Ok(self
            .client
            .recall_memory(&self.space, db, branch, ns, request)
            .await?
            .into_inner())
    }

    /// Records evidence and the assertions that cite it.
    pub async fn observe(
        &self,
        request: &types::MemoryObservationRequest,
        idempotency_key: Option<&str>,
    ) -> MemoryResult<types::MemoryOperation> {
        let key: types::ObserveMemoryIdempotencyKey = self::idempotency_key(idempotency_key)?;
        let (db, branch, ns) = self.scope();
        Ok(self
            .client
            .observe_memory(&self.space, &key, db, branch, ns, request)
            .await?
            .into_inner())
    }

    /// Records `text` as one evidence item with a generated evidence id. Use
    /// [`MemorySpace::observe`] for assertions, provenance or several items.
    pub async fn remember(
        &self,
        text: &str,
        idempotency_key: Option<&str>,
    ) -> MemoryResult<types::MemoryOperation> {
        if text.trim().is_empty() {
            return Err(Error::InvalidRequest("remember text is empty".into()));
        }
        let key = idempotency_key.map_or_else(new_idempotency_key, str::to_owned);
        let evidence_id = format!(
            "{:x}",
            Sha256::digest(format!("antfly-memory-remember:{key}"))
        );
        let request: types::MemoryObservationRequest = serde_json::from_value(serde_json::json!({
            "evidence": [{"id": evidence_id, "text": text}],
        }))
        .map_err(|error| Error::InvalidRequest(error.to_string()))?;
        self.observe(&request, Some(&key)).await
    }

    /// Records whether a task that used recalled memories succeeded.
    pub async fn outcome(
        &self,
        outcome: &types::MemoryOutcome,
        idempotency_key: Option<&str>,
    ) -> MemoryResult<types::MemoryOperation> {
        let key: types::RecordMemoryOutcomeIdempotencyKey = self::idempotency_key(idempotency_key)?;
        let (db, branch, ns) = self.scope();
        Ok(self
            .client
            .record_memory_outcome(&self.space, &key, db, branch, ns, outcome)
            .await?
            .into_inner())
    }

    /// Runs a read-only health review of the space.
    pub async fn consolidate(
        &self,
        request: &types::MemoryConsolidationRequest,
    ) -> MemoryResult<types::MemoryConsolidation> {
        let (db, branch, ns) = self.scope();
        Ok(self
            .client
            .consolidate_memory(&self.space, db, branch, ns, request)
            .await?
            .into_inner())
    }

    /// Prioritizes memory `id` in the brief within its item and byte limits.
    pub async fn pin(
        &self,
        id: &str,
        reason: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> MemoryResult<types::MemoryOperation> {
        self.set_pin(id, true, reason, idempotency_key).await
    }

    /// Removes a pin from memory `id`.
    pub async fn unpin(
        &self,
        id: &str,
        reason: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> MemoryResult<types::MemoryOperation> {
        self.set_pin(id, false, reason, idempotency_key).await
    }

    async fn set_pin(
        &self,
        id: &str,
        pinned: bool,
        reason: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> MemoryResult<types::MemoryOperation> {
        let key: types::PinMemoryIdempotencyKey = self::idempotency_key(idempotency_key)?;
        let body = types::MemoryPin {
            id: id.to_owned(),
            pinned,
            reason: reason.map(str::to_owned),
        };
        let (db, branch, ns) = self.scope();
        Ok(self
            .client
            .pin_memory(&self.space, &key, db, branch, ns, &body)
            .await?
            .into_inner())
    }

    /// Returns one memory record by id.
    pub async fn get(&self, id: &str) -> MemoryResult<types::MemoryRecord> {
        let (db, branch, ns) = self.scope();
        Ok(self
            .client
            .get_memory(&self.space, id, db, branch, ns)
            .await?
            .into_inner())
    }
}
