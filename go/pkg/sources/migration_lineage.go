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

package sources

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// LocalMigrationReceipt is committed in the same profile write as cutover.
// It records configuration bindings, never credentials or capability URLs.
type LocalMigrationReceipt struct {
	Version        int    `json:"version"`
	SourceID       string `json:"source_id"`
	Table          string `json:"table"`
	Namespace      string `json:"namespace"`
	FromConnection string `json:"from_connection"`
	FromPrefix     string `json:"from_prefix"`
	FromBinding    string `json:"from_binding"`
	ToConnection   string `json:"to_connection"`
	ToBucket       string `json:"to_bucket"`
	ToPrefix       string `json:"to_prefix"`
	ToBinding      string `json:"to_binding"`
}

func migrationReceipt(source Source, opts MigrateLocalOptions, from, to Connection) LocalMigrationReceipt {
	return LocalMigrationReceipt{Version: 1, SourceID: source.ID, Table: source.Table, Namespace: source.Identity.Namespace, FromConnection: opts.FromConnection, FromPrefix: opts.FromPrefix, FromBinding: connectionMediaBinding(from), ToConnection: opts.ToConnection, ToBucket: opts.ToBucket, ToPrefix: opts.ToPrefix, ToBinding: connectionMediaBinding(to)}
}

func migrationTargetMatches(source Source, receipt LocalMigrationReceipt, provider string) bool {
	config, err := migrateDestinationConfig(provider, receipt.ToBucket, receipt.ToPrefix)
	if err != nil {
		return false
	}
	var expected, actual any
	if json.Unmarshal(config, &expected) != nil || json.Unmarshal(source.Config, &actual) != nil {
		return false
	}
	a, _ := json.Marshal(expected)
	b, _ := json.Marshal(actual)
	return source.Connection == receipt.ToConnection && source.Table == receipt.Table && source.Identity.Namespace == receipt.Namespace && bytes.Equal(a, b)
}

func validateMigrationReceipt(profile Profile, source Source, receipt LocalMigrationReceipt) error {
	from, haveFrom := profile.Connections[receipt.FromConnection]
	to, haveTo := profile.Connections[receipt.ToConnection]
	if receipt.Version != 1 || receipt.SourceID != source.ID || receipt.Namespace == "" || !haveFrom || !haveTo || from.Provider != ProviderLocal || receipt.FromConnection == receipt.ToConnection || connectionMediaBinding(from) != receipt.FromBinding || connectionMediaBinding(to) != receipt.ToBinding || !migrationTargetMatches(source, receipt, to.Provider) {
		return fmt.Errorf("migration lineage differs from the selected source or transfer connections")
	}
	if _, err := localStoreDir(receipt.FromPrefix); err != nil {
		return err
	}
	if _, err := localStoreDir(receipt.ToPrefix); err != nil {
		return err
	}
	return nil
}

func applyMigrationReceipt(profile Profile, next *Profile, source Source, raw string) error {
	if len(raw) > 16384 {
		return fmt.Errorf("migration receipt exceeds budget")
	}
	var receipt LocalMigrationReceipt
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("invalid trailing migration receipt data")
	}
	if err := validateMigrationReceipt(profile, source, receipt); err != nil {
		return err
	}
	var previous *Source
	for i := range profile.Sources {
		if profile.Sources[i].ID == source.ID {
			previous = &profile.Sources[i]
			break
		}
	}
	if previous == nil {
		return fmt.Errorf("migration requires an existing source")
	}
	if previous.Connection == receipt.FromConnection {
		config, err := decodeSourceConfig[LocalConfig](previous.Config)
		if err != nil || config.Prefix != receipt.FromPrefix {
			return fmt.Errorf("original migration prefix differs from selected source")
		}
	} else {
		stored, exists := profile.LocalMigrations[source.ID]
		if !exists || stored != receipt || !migrationTargetMatches(*previous, stored, profile.Connections[stored.ToConnection].Provider) {
			return fmt.Errorf("migration retry has no matching durable lineage")
		}
	}
	if next.LocalMigrations == nil {
		next.LocalMigrations = make(map[string]LocalMigrationReceipt)
	}
	next.LocalMigrations[source.ID] = receipt
	return nil
}
