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

package oapi

import (
	"reflect"
	"testing"
)

func TestTableStorageEngineEmbeddedContract(t *testing.T) {
	doc, err := GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	engine := doc.Components.Schemas["TableStorageSettings"].Value.Properties["engine"]
	if engine == nil || engine.Value == nil {
		t.Fatal("embedded table storage schema is missing engine")
	}
	if !reflect.DeepEqual(engine.Value.Enum, []any{"local", "object"}) || engine.Value.Default != "local" {
		t.Fatalf("unexpected embedded engine contract: %#v", engine.Value)
	}
	if !TableStorageSettingsEngineLocal.Valid() || !TableStorageSettingsEngineObject.Valid() || TableStorageSettingsEngine("native").Valid() {
		t.Fatal("SDK table engine enum disagrees with embedded schema")
	}
}
