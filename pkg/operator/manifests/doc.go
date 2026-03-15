// Package manifests provides exportable Kubernetes manifests for the Antfly operator.
//
// This package embeds the generated CRD and RBAC YAML files and provides
// type-safe Go accessors for use in infrastructure-as-code tools like Pulumi.
//
// # CRD Access
//
// The CRD manifests can be accessed as raw YAML or parsed into typed objects:
//
//	// Get raw YAML for kubectl apply
//	yaml := manifests.AntflyClusterCRDYAML()
//
//	// Get parsed CRD object
//	crd, err := manifests.AntflyClusterCRD()
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
