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
	"fmt"
	"strings"
)

// Restore source-relative identity in inventories written before relative keys
// existed. The config hash has already proved that these IDs were acquired from
// this configuration. Derive from the pinned IDs, never a new provider listing,
// so resumptions retain exactly their selected revisions and authority.
func restoreInventoryRelativeKeys(source Source, provider string, inventory *Inventory) error {
	if inventory == nil {
		return nil
	}
	var prefix string
	switch provider {
	case ProviderLocal:
		namespace, err := localIdentityNamespace(Request{SourceNamespace: "job/" + source.ID, Config: source.Config})
		if err != nil {
			return err
		}
		prefix = namespace + "/"
	case ProviderS3:
		config, err := decodeSourceConfig[S3Config](source.Config)
		if err != nil {
			return err
		}
		prefix = "s3://job/" + source.ID + "/" + config.Bucket + "/" + config.Prefix
	case ProviderGCS:
		config, err := decodeSourceConfig[GoogleConfig](source.Config)
		if err != nil {
			return err
		}
		prefix = "gcs://job/" + source.ID + "/" + config.Bucket + "/" + config.Prefix
	default:
		return nil
	}
	for i := range inventory.Objects {
		item := &inventory.Objects[i]
		if item.SourceKind != provider || !strings.HasPrefix(item.ID, prefix) {
			return fmt.Errorf("pinned inventory does not match configured source identity")
		}
		relative := strings.TrimPrefix(item.ID, prefix)
		if (item.RelativeKeySet || item.RelativeKey != "") && item.RelativeKey != relative {
			return fmt.Errorf("pinned inventory has inconsistent relative identity")
		}
		item.RelativeKey, item.RelativeKeySet = relative, true
	}
	return nil
}
