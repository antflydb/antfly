package bootstrap

import (
	"context"
	"fmt"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	sigyaml "sigs.k8s.io/yaml"

	"github.com/antflydb/antfly/pkg/operator/manifests"
)

const (
	fieldOwner      = "antfly-operator"
	crdTimeout      = 60 * time.Second
	crdPollInterval = 2 * time.Second
)

// EnsureCRDs applies all embedded CRDs using server-side apply
func EnsureCRDs(ctx context.Context, c client.Client) error {
	logger := log.FromContext(ctx)

	for _, crdYAML := range manifests.AllCRDYAMLBytes() {
		u := &unstructured.Unstructured{}
		if err := sigyaml.Unmarshal(crdYAML, &u.Object); err != nil {
			return fmt.Errorf("failed to unmarshal CRD YAML: %w", err)
		}

		logger.Info("Applying CRD", "name", u.GetName())

		// Server-side apply with force ownership
		if err := c.Apply(ctx, client.ApplyConfigurationFromUnstructured(u),
			client.ForceOwnership,
			client.FieldOwner(fieldOwner)); err != nil {
			return fmt.Errorf("failed to apply CRD %s: %w", u.GetName(), err)
		}
	}

	return nil
}

// WaitForCRDs waits for all CRDs to be established
func WaitForCRDs(ctx context.Context, c client.Client) error {
	logger := log.FromContext(ctx)
	crds, err := manifests.AllCRDs()
	if err != nil {
		return fmt.Errorf("failed to load embedded CRDs: %w", err)
	}

	for _, crd := range crds {
		logger.Info("Waiting for CRD to be established", "name", crd.Name)

		err := wait.PollUntilContextTimeout(ctx, crdPollInterval, crdTimeout, true,
			func(ctx context.Context) (bool, error) {
				current := &apiextensionsv1.CustomResourceDefinition{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(crd), current); err != nil {
					if apierrors.IsNotFound(err) {
						return false, nil
					}
					return false, err
				}

				for _, cond := range current.Status.Conditions {
					if cond.Type == apiextensionsv1.Established &&
						cond.Status == apiextensionsv1.ConditionTrue {
						return true, nil
					}
				}
				return false, nil
			})

		if err != nil {
			return fmt.Errorf("CRD %s not established: %w", crd.Name, err)
		}
	}

	return nil
}
