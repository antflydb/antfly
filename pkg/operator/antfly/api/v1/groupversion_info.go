// Package v1 contains API Schema definitions for the antfly v1 API group
// +kubebuilder:object:generate=true
// +groupName=antfly.io
package v1

//go:generate go tool controller-gen object:headerFile="../../hack/boilerplate.go.txt" paths="."
//go:generate go tool controller-gen crd paths="." output:crd:artifacts:config=../../../manifests/crd

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is group version used to register these objects
	GroupVersion = schema.GroupVersion{Group: "antfly.io", Version: "v1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
