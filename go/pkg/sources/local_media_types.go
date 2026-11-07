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
	_ "embed"
	"encoding/json"
	"path/filepath"
	"strings"
)

// Shared with the AI SDK upload helper. Local stores encode MIME in the
// extension, so this mapping must not depend on the host's MIME database.
//
//go:embed local_media_types.json
var localMediaTypesJSON []byte

var localMediaMIMEs = func() map[string]string {
	var extensions map[string]string
	if err := json.Unmarshal(localMediaTypesJSON, &extensions); err != nil {
		panic(err)
	}
	result := make(map[string]string, len(extensions))
	for mediaType, extension := range extensions {
		result["."+extension] = mediaType
	}
	return result
}()

func localMediaMIMEForPath(path string) string {
	return localMediaMIMEs[strings.ToLower(filepath.Ext(path))]
}
