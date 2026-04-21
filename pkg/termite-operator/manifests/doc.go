// Copyright 2025 Antfly, Inc.
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

// Package manifests provides exportable Kubernetes manifests for the Termite operator.
//
// This package embeds the generated CRD and RBAC YAML files and provides
// type-safe Go accessors for use in infrastructure-as-code tools like Pulumi.
//
// # CRD Access
//
// The CRD manifests can be accessed as raw YAML or parsed into typed objects:
//
//	// Get raw YAML for kubectl apply
//	yaml := manifests.TermitePoolCRDYAML()
//
//	// Get parsed CRD object
//	crd, err := manifests.TermitePoolCRD()
//
// # RBAC Access
//
// RBAC resources are provided as typed Go objects:
//
//	// Get all RBAC resources needed for the operator
//	resources := manifests.AllRBACResources()
//
//	// Get individual resources
//	sa := manifests.ServiceAccount()
//	role := manifests.ClusterRole()
package manifests
