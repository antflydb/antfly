// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

package management

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// SourceRevision returns "r1-" followed by the lowercase SHA-256 of the
// declaration's canonical JSON. Runtime observations and Revision are excluded.
// It returns "" if the declaration holds values JSON cannot encode.
func SourceRevision(s Source) string {
	declaration := struct {
		Id                   string                 `json:"id"`
		Name                 string                 `json:"name"`
		Connection           string                 `json:"connection"`
		Table                string                 `json:"table"`
		Config               map[string]any         `json:"config"`
		Permissions          Permissions            `json:"permissions"`
		PermissionTtlSeconds int                    `json:"permission_ttl_seconds"`
		Publication          SourcePublication      `json:"publication"`
		Identity             SourceIdentity         `json:"identity"`
		SearchVersion        int                    `json:"search_version"`
		Streams              []SearchStream         `json:"streams"`
		Artifacts            []ArtifactBinding      `json:"artifacts"`
		Processing           *ProcessingDeclaration `json:"processing"`
		Knowledge            *KnowledgeDeclaration  `json:"knowledge"`
	}{
		Id:                   s.Id,
		Name:                 s.Name,
		Connection:           s.Connection,
		Table:                s.Table,
		Config:               s.Config,
		Permissions:          s.Permissions,
		PermissionTtlSeconds: s.PermissionTtlSeconds,
		Publication:          s.Publication,
		Identity:             s.Identity,
		SearchVersion:        s.SearchVersion,
		Streams:              s.Streams,
		Artifacts:            s.Artifacts,
		Processing:           s.Processing,
		Knowledge:            s.Knowledge,
	}
	raw, err := json.Marshal(declaration)
	if err != nil {
		// Unreachable for JSON-decoded declarations. An empty revision never
		// satisfies a precondition, so conditional writes fail closed.
		return ""
	}
	sum := sha256.Sum256(raw)
	return "r1-" + hex.EncodeToString(sum[:])
}

// PreconditionBackend optionally supports atomic declaration updates and creates.
// PutSourceIfMatch must compare SourceRevision(current) with revision under the
// same lock that guards the write. A mismatch returns
// Failure(412, "source_revision_mismatch"); a missing source returns
// Failure(404, "source_not_found"). PutSourceIfMatch never creates a source.
// PutSourceIfAbsent atomically creates only if source.Id does not exist;
// an existing source returns Failure(412, "source_already_exists").
type PreconditionBackend interface {
	PutSourceIfMatch(ctx context.Context, source Source, revision string) (Source, error)
	PutSourceIfAbsent(ctx context.Context, source Source) (Source, error)
}
