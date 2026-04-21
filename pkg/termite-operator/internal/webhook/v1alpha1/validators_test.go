package v1alpha1

import (
	"context"
	"strings"
	"testing"

	antflyaiv1alpha1 "github.com/antflydb/antfly/pkg/termite-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- TermitePoolValidator tests ---

func basePool() *antflyaiv1alpha1.TermitePool {
	return &antflyaiv1alpha1.TermitePool{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pool", Namespace: "default"},
		Spec: antflyaiv1alpha1.TermitePoolSpec{
			Models: antflyaiv1alpha1.ModelConfig{
				Preload: []antflyaiv1alpha1.ModelSpec{
					{Name: "bge-small-en-v1.5"},
				},
			},
			Replicas: antflyaiv1alpha1.ReplicaConfig{Min: 1, Max: 3},
		},
	}
}

func TestTermitePoolValidator_ValidateCreate_Valid(t *testing.T) {
	v := &TermitePoolValidator{}
	pool := basePool()

	warnings, err := v.ValidateCreate(context.Background(), pool)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
	if warnings != nil {
		t.Errorf("expected no warnings, got: %v", warnings)
	}
}

func TestTermitePoolValidator_ValidateCreate_InvalidReplicas(t *testing.T) {
	v := &TermitePoolValidator{}
	pool := basePool()
	pool.Spec.Replicas.Min = 5
	pool.Spec.Replicas.Max = 2 // min > max

	_, err := v.ValidateCreate(context.Background(), pool)
	if err == nil {
		t.Error("expected error for min > max replicas")
	}
}

func TestTermitePoolValidator_ValidateUpdate_ImmutableAutopilot(t *testing.T) {
	v := &TermitePoolValidator{}
	oldPool := basePool()
	oldPool.Spec.GKE = &antflyaiv1alpha1.GKEConfig{Autopilot: true}

	newPool := oldPool.DeepCopy()
	newPool.Spec.GKE.Autopilot = false

	_, err := v.ValidateUpdate(context.Background(), oldPool, newPool)
	if err == nil {
		t.Error("expected error for changing Autopilot mode")
	}
	if !strings.Contains(err.Error(), "immutable") {
		t.Errorf("expected 'immutable' in error, got: %v", err)
	}
}

func TestTermitePoolValidator_ValidateUpdate_ImmutableComputeClass(t *testing.T) {
	v := &TermitePoolValidator{}
	oldPool := basePool()
	oldPool.Spec.GKE = &antflyaiv1alpha1.GKEConfig{
		Autopilot:             true,
		AutopilotComputeClass: "Balanced",
	}

	newPool := oldPool.DeepCopy()
	newPool.Spec.GKE.AutopilotComputeClass = "Performance"

	_, err := v.ValidateUpdate(context.Background(), oldPool, newPool)
	if err == nil {
		t.Error("expected error for changing compute class")
	}
	if !strings.Contains(err.Error(), "immutable") {
		t.Errorf("expected 'immutable' in error, got: %v", err)
	}
}

func TestTermitePoolValidator_ValidateUpdate_MutableFields(t *testing.T) {
	v := &TermitePoolValidator{}
	oldPool := basePool()
	newPool := oldPool.DeepCopy()
	newPool.Spec.Replicas.Max = 5 // mutable field

	_, err := v.ValidateUpdate(context.Background(), oldPool, newPool)
	if err != nil {
		t.Errorf("expected no error for mutable field change, got: %v", err)
	}
}

func TestTermitePoolValidator_ValidateDelete(t *testing.T) {
	v := &TermitePoolValidator{}
	pool := basePool()

	warnings, err := v.ValidateDelete(context.Background(), pool)
	if err != nil {
		t.Errorf("expected no error on delete, got: %v", err)
	}
	if warnings != nil {
		t.Errorf("expected no warnings on delete, got: %v", warnings)
	}
}

// --- TermiteRouteValidator tests ---

func baseRoute() *antflyaiv1alpha1.TermiteRoute {
	return &antflyaiv1alpha1.TermiteRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "test-route", Namespace: "default"},
		Spec: antflyaiv1alpha1.TermiteRouteSpec{
			Route: []antflyaiv1alpha1.RouteDestination{
				{Pool: "pool-1", Weight: 100},
			},
		},
	}
}

func TestTermiteRouteValidator_ValidateCreate_Valid(t *testing.T) {
	v := &TermiteRouteValidator{}
	route := baseRoute()

	warnings, err := v.ValidateCreate(context.Background(), route)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
	if warnings != nil {
		t.Errorf("expected no warnings, got: %v", warnings)
	}
}

func TestTermiteRouteValidator_ValidateCreate_NoDestinations(t *testing.T) {
	v := &TermiteRouteValidator{}
	route := baseRoute()
	route.Spec.Route = nil

	_, err := v.ValidateCreate(context.Background(), route)
	if err == nil {
		t.Error("expected error for empty route destinations")
	}
}

func TestTermiteRouteValidator_ValidateUpdate_Valid(t *testing.T) {
	v := &TermiteRouteValidator{}
	oldRoute := baseRoute()
	newRoute := oldRoute.DeepCopy()
	newRoute.Spec.Route[0].Weight = 50

	_, err := v.ValidateUpdate(context.Background(), oldRoute, newRoute)
	if err != nil {
		t.Errorf("expected no error for route update, got: %v", err)
	}
}

func TestTermiteRouteValidator_ValidateCreate_HostedSourceMatchValid(t *testing.T) {
	v := &TermiteRouteValidator{}
	route := baseRoute()
	route.Spec.Match.Source = &antflyaiv1alpha1.SourceMatch{
		Organizations:  []string{"org-1"},
		Projects:       []string{"project-1"},
		APIKeyPrefixes: []string{"deadbeef"},
	}

	_, err := v.ValidateCreate(context.Background(), route)
	if err != nil {
		t.Fatalf("expected hosted source match to validate, got %v", err)
	}
}

func TestTermiteRouteValidator_ValidateCreate_RejectsNamespaceSourceMatch(t *testing.T) {
	v := &TermiteRouteValidator{}
	route := baseRoute()
	route.Spec.Match.Source = &antflyaiv1alpha1.SourceMatch{
		Namespaces: []string{"team-a"},
	}

	_, err := v.ValidateCreate(context.Background(), route)
	if err == nil {
		t.Fatal("expected namespace source match to be rejected")
	}
	if !strings.Contains(err.Error(), "spec.match.source.namespaces") {
		t.Fatalf("expected namespace validation error, got %v", err)
	}
}

func TestTermiteRouteValidator_ValidateDelete(t *testing.T) {
	v := &TermiteRouteValidator{}
	route := baseRoute()

	warnings, err := v.ValidateDelete(context.Background(), route)
	if err != nil {
		t.Errorf("expected no error on delete, got: %v", err)
	}
	if warnings != nil {
		t.Errorf("expected no warnings on delete, got: %v", warnings)
	}
}
