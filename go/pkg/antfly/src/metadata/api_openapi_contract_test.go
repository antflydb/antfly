// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package metadata

import "testing"

func TestEmbeddedOpenAPITransformOperationsMatchGeneratedModel(t *testing.T) {
	spec, err := GetSwagger()
	if err != nil {
		t.Fatalf("decode embedded OpenAPI document: %v", err)
	}

	transformOps, ok := spec.Components.Schemas["TransformOpType"]
	if !ok || transformOps == nil || transformOps.Value == nil {
		t.Fatal("embedded OpenAPI document is missing TransformOpType")
	}

	embedded := make(map[string]struct{}, len(transformOps.Value.Enum))
	for _, value := range transformOps.Value.Enum {
		op, ok := value.(string)
		if !ok {
			t.Fatalf("TransformOpType contains non-string enum value %T", value)
		}
		embedded[op] = struct{}{}
	}

	generated := []TransformOpType{
		TransformOpTypeSET,
		TransformOpTypeSETONINSERT,
		TransformOpTypeUNSET,
		TransformOpTypeINC,
		TransformOpTypePUSH,
		TransformOpTypeADDTOSET,
		TransformOpTypeMAX,
	}
	if len(embedded) != len(generated) {
		t.Fatalf("embedded TransformOpType has %d values; generated model has %d", len(embedded), len(generated))
	}
	for _, op := range generated {
		if _, ok := embedded[string(op)]; !ok {
			t.Errorf("embedded TransformOpType is missing generated operation %q", op)
		}
	}
}
