// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

package commands

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

type Manifest struct {
	Version int              `json:"version"`
	Sources []ManifestSource `json:"sources"`
}

type ManifestSource struct {
	Key                  string                            `json:"key"`
	Name                 string                            `json:"name,omitempty"`
	Connection           string                            `json:"connection"`
	Table                string                            `json:"table"`
	Config               map[string]any                    `json:"config"`
	Publication          management.SourcePublication      `json:"publication,omitempty"`
	Permissions          *management.Permissions           `json:"permissions,omitempty"`
	PermissionTtlSeconds *int                              `json:"permission_ttl_seconds,omitempty"`
	SearchVersion        *int                              `json:"search_version,omitempty"`
	Streams              *[]management.SearchStream        `json:"streams,omitempty"`
	Artifacts            *[]management.ArtifactBinding     `json:"artifacts,omitempty"`
	Processing           *management.ProcessingDeclaration `json:"processing,omitempty"`
	Identity             *management.SourceIdentity        `json:"identity,omitempty"`
	Knowledge            *management.KnowledgeDeclaration  `json:"knowledge,omitempty"`
	Extensions           map[string]json.RawMessage        `json:"extensions,omitempty"`
}

func DecodeManifest(r io.Reader) (Manifest, error) {
	return Manifest{}, errors.New("not implemented")
}
