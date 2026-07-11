package v1

import (
	"fmt"
	"strings"
	"testing"

	inferencev1alpha1 "github.com/antflydb/antfly/go/pkg/operator/api/inference/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidateCreate_ValidBalanced(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot:             true,
				AutopilotComputeClass: "Balanced",
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				Resources: ResourceSpec{
					Limits: ResourceLimits{
						CPU:    "1",
						Memory: "1Gi",
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
				Resources: ResourceSpec{
					Limits: ResourceLimits{
						CPU:    "1",
						Memory: "1Gi",
					},
				},
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for valid Balanced compute class, got: %v", err)
	}
}

func TestValidateCreate_ValidAutopilotSpot(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot:             true,
				AutopilotComputeClass: "autopilot-spot",
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				Resources: ResourceSpec{
					Limits: ResourceLimits{
						CPU:    "1",
						Memory: "1Gi",
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
				Resources: ResourceSpec{
					Limits: ResourceLimits{
						CPU:    "1",
						Memory: "1Gi",
					},
				},
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for valid autopilot-spot compute class, got: %v", err)
	}
}

func TestValidateCreate_DefaultBalanced(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot:             true,
				AutopilotComputeClass: "", // Empty - should default to Balanced
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				Resources: ResourceSpec{
					Limits: ResourceLimits{
						CPU:    "1",
						Memory: "1Gi",
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
				Resources: ResourceSpec{
					Limits: ResourceLimits{
						CPU:    "1",
						Memory: "1Gi",
					},
				},
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for empty compute class (defaults to Balanced), got: %v", err)
	}
}

func TestValidateCreate_InvalidComputeClass(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot:             true,
				AutopilotComputeClass: "general-purpose", // INVALID
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for invalid compute class 'general-purpose', got nil")
	} else if !strings.Contains(err.Error(), "invalid GKE Autopilot compute class") {
		t.Errorf("Expected error to contain 'invalid GKE Autopilot compute class', got: %v", err)
	}
}

func TestValidateCreate_ConflictingSpotNodesData(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot:             true,
				AutopilotComputeClass: "autopilot-spot",
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: DataNodesSpec{
				Replicas:    3,
				UseSpotPods: true, // CONFLICT
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for conflicting useSpotNodes with Autopilot, got nil")
	} else if !strings.Contains(err.Error(), "useSpotPods") && !strings.Contains(err.Error(), "conflicts") {
		t.Errorf("Expected error about useSpotPods conflict, got: %v", err)
	}
}

func TestValidateCreate_ConflictingSpotNodesMetadata(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot:             true,
				AutopilotComputeClass: "autopilot-spot",
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas:    3,
				UseSpotPods: true, // CONFLICT
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for conflicting useSpotNodes with Autopilot, got nil")
	} else if !strings.Contains(err.Error(), "useSpotPods") && !strings.Contains(err.Error(), "conflicts") {
		t.Errorf("Expected error about useSpotPods conflict, got: %v", err)
	}
}

func TestValidateCreate_ComputeClassWithoutAutopilot(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot:             false,
				AutopilotComputeClass: "Balanced", // INVALID without Autopilot
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for compute class without Autopilot, got nil")
	} else if !strings.Contains(err.Error(), "autopilotComputeClass is set but") || !strings.Contains(err.Error(), "autopilot=false") {
		t.Errorf("Expected error about compute class requiring Autopilot, got: %v", err)
	}
}

func TestValidateCreate_AcceleratorWithoutGPUData(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot:             true,
				AutopilotComputeClass: "Accelerator",
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				Resources: ResourceSpec{
					Limits: ResourceLimits{
						CPU:    "1",
						Memory: "1Gi",
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
				Resources: ResourceSpec{
					Limits: ResourceLimits{
						CPU:    "1",
						Memory: "1Gi",
						// No GPU
					},
				},
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for Accelerator without GPU, got nil")
	} else if !strings.Contains(err.Error(), "Accelerator") || !strings.Contains(err.Error(), "GPU") {
		t.Errorf("Expected error about Accelerator requiring GPU, got: %v", err)
	}
}

func TestValidateCreate_AcceleratorWithoutGPUMetadata(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot:             true,
				AutopilotComputeClass: "Accelerator",
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				Resources: ResourceSpec{
					Limits: ResourceLimits{
						CPU:    "1",
						Memory: "1Gi",
						// No GPU
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
				Resources: ResourceSpec{
					Limits: ResourceLimits{
						CPU:    "1",
						Memory: "1Gi",
						// No GPU
					},
				},
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for Accelerator without GPU, got nil")
	} else if !strings.Contains(err.Error(), "Accelerator") || !strings.Contains(err.Error(), "GPU") {
		t.Errorf("Expected error about Accelerator requiring GPU, got: %v", err)
	}
}

func TestValidateCreate_AcceleratorWithGPU(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot:             true,
				AutopilotComputeClass: "Accelerator",
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				Resources: ResourceSpec{
					Limits: ResourceLimits{
						CPU:    "1",
						Memory: "1Gi",
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
				Resources: ResourceSpec{
					Limits: ResourceLimits{
						CPU:    "1",
						Memory: "1Gi",
						GPU:    "1",
					},
				},
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for Accelerator with GPU, got: %v", err)
	}
}

func TestValidateUpdate_ImmutableAutopilot(t *testing.T) {
	oldCluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot: false,
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	newCluster := oldCluster.DeepCopy()
	newCluster.Spec.GKE.Autopilot = true // Change Autopilot mode

	err := newCluster.ValidateUpdate(oldCluster)
	if err == nil {
		t.Error("Expected error for changing Autopilot mode, got nil")
	} else if !strings.Contains(err.Error(), "immutable") || !strings.Contains(err.Error(), "autopilot") {
		t.Errorf("Expected error about Autopilot being immutable, got: %v", err)
	}
}

func TestValidateUpdate_ImmutableComputeClass(t *testing.T) {
	oldCluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot:             true,
				AutopilotComputeClass: "Balanced",
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	newCluster := oldCluster.DeepCopy()
	newCluster.Spec.GKE.AutopilotComputeClass = "autopilot-spot" // Change compute class

	err := newCluster.ValidateUpdate(oldCluster)
	if err == nil {
		t.Error("Expected error for changing compute class, got nil")
	} else if !strings.Contains(err.Error(), "immutable") || !strings.Contains(err.Error(), "autopilotComputeClass") {
		t.Errorf("Expected error about compute class being immutable, got: %v", err)
	}
}

func TestValidateUpdate_MutableComputeClassNonAutopilot(t *testing.T) {
	oldCluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot:             false,
				AutopilotComputeClass: "",
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	newCluster := oldCluster.DeepCopy()
	newCluster.Spec.GKE.AutopilotComputeClass = "" // No change, still empty

	err := newCluster.ValidateUpdate(oldCluster)
	if err != nil {
		t.Errorf("Expected no error for unchanged compute class with Autopilot disabled, got: %v", err)
	}
}

func TestValidateCreate_BackwardCompatibilitySpotNodes(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot: false,
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: DataNodesSpec{
				Replicas:    3,
				UseSpotPods: true, // Valid for non-Autopilot
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for useSpotPods with Autopilot disabled (backward compatibility), got: %v", err)
	}
}

func TestValidateCreate_ValidEnvFromSecretRef(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				EnvFrom: []corev1.EnvFromSource{
					{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "backup-credentials",
							},
						},
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
				EnvFrom: []corev1.EnvFromSource{
					{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "backup-credentials",
							},
						},
					},
				},
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for valid envFrom with secretRef, got: %v", err)
	}
}

func TestValidateCreate_ValidEnvFromConfigMapRef(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				EnvFrom: []corev1.EnvFromSource{
					{
						ConfigMapRef: &corev1.ConfigMapEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "env-config",
							},
						},
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for valid envFrom with configMapRef, got: %v", err)
	}
}

func TestValidateCreate_InvalidEnvFromEmptySecretName(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				EnvFrom: []corev1.EnvFromSource{
					{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "", // Empty name
							},
						},
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for envFrom with empty secretRef name, got nil")
	} else if !strings.Contains(err.Error(), "secretRef.name") || !strings.Contains(err.Error(), "empty") {
		t.Errorf("Expected error about empty secretRef.name, got: %v", err)
	}
}

func TestValidateCreate_InvalidEnvFromEmptyConfigMapName(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
				EnvFrom: []corev1.EnvFromSource{
					{
						ConfigMapRef: &corev1.ConfigMapEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "", // Empty name
							},
						},
					},
				},
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for envFrom with empty configMapRef name, got nil")
	} else if !strings.Contains(err.Error(), "configMapRef.name") || !strings.Contains(err.Error(), "empty") {
		t.Errorf("Expected error about empty configMapRef.name, got: %v", err)
	}
}

func TestValidateCreate_InvalidEnvFromNoRef(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				EnvFrom: []corev1.EnvFromSource{
					{
						// Neither SecretRef nor ConfigMapRef specified
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for envFrom without secretRef or configMapRef, got nil")
	} else if !strings.Contains(err.Error(), "secretRef") || !strings.Contains(err.Error(), "configMapRef") {
		t.Errorf("Expected error about missing secretRef or configMapRef, got: %v", err)
	}
}

func TestValidateCreate_ValidEnvFromWithPrefix(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				EnvFrom: []corev1.EnvFromSource{
					{
						Prefix: "BACKUP_",
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "backup-credentials",
							},
						},
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for valid envFrom with prefix, got: %v", err)
	}
}

func TestValidateCreate_ValidTolerations(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				Tolerations: []corev1.Toleration{
					{
						Key:      "dedicated",
						Operator: corev1.TolerationOpEqual,
						Value:    "antfly",
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
				Tolerations: []corev1.Toleration{
					{
						Key:      "dedicated",
						Operator: corev1.TolerationOpEqual,
						Value:    "antfly",
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for valid tolerations, got: %v", err)
	}
}

func TestValidateCreate_ValidNodeSelector(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				NodeSelector: map[string]string{
					"node-pool": "antfly",
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
				NodeSelector: map[string]string{
					"node-pool": "antfly-data",
				},
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for valid nodeSelector, got: %v", err)
	}
}

func TestValidateCreate_NodeSelectorConflictsWithAutopilotMetadata(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot: true,
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				NodeSelector: map[string]string{
					"node-pool": "antfly",
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for nodeSelector with GKE Autopilot, got nil")
	} else if !strings.Contains(err.Error(), "nodeSelector") || !strings.Contains(err.Error(), "autopilot") {
		t.Errorf("Expected error about nodeSelector conflicting with Autopilot, got: %v", err)
	}
}

func TestValidateCreate_NodeSelectorConflictsWithAutopilotData(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot: true,
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
				NodeSelector: map[string]string{
					"node-pool": "antfly-data",
				},
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for nodeSelector with GKE Autopilot, got nil")
	} else if !strings.Contains(err.Error(), "nodeSelector") || !strings.Contains(err.Error(), "autopilot") {
		t.Errorf("Expected error about nodeSelector conflicting with Autopilot, got: %v", err)
	}
}

func TestValidateCreate_TolerationsWithAutopilotAllowed(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			GKE: &GKESpec{
				Autopilot: true,
			},
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
				Tolerations: []corev1.Toleration{
					{
						Key:      "example",
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for tolerations with GKE Autopilot, got: %v", err)
	}
}

func TestValidateCreate_ZeroMetadataReplicas(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.MetadataNodes.Replicas = 0

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for 0 metadata replicas, got nil")
	} else if !strings.Contains(err.Error(), "metadataNodes.replicas") {
		t.Errorf("Expected error about metadataNodes.replicas, got: %v", err)
	}
}

func TestValidateCreate_EvenMetadataReplicas(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.MetadataNodes.Replicas = 2

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for even metadata replicas, got nil")
	} else if !strings.Contains(err.Error(), "odd") {
		t.Errorf("Expected error about odd replica count, got: %v", err)
	}
}

func TestValidateCreate_EvenMetadataReplicas4(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.MetadataNodes.Replicas = 4

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for 4 metadata replicas, got nil")
	} else if !strings.Contains(err.Error(), "odd") {
		t.Errorf("Expected error about odd replica count, got: %v", err)
	}
}

func TestValidateCreate_ValidOddMetadataReplicas(t *testing.T) {
	for _, replicas := range []int32{1, 3, 5} {
		t.Run(fmt.Sprintf("replicas=%d", replicas), func(t *testing.T) {
			cluster := baseCluster()
			cluster.Spec.MetadataNodes.Replicas = replicas

			if err := cluster.ValidateCreate(); err != nil {
				t.Errorf("Expected no error for %d metadata replicas, got: %v", replicas, err)
			}
		})
	}
}

func TestValidateCreate_ZeroDataReplicas(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.DataNodes.Replicas = 0

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for 0 data replicas because it maps to the controller default, got: %v", err)
	}
}

func TestValidateCreate_NegativeMetadataReplicas(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: MetadataNodesSpec{
				Replicas: -1,
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for negative metadata replicas, got nil")
	} else if !strings.Contains(err.Error(), "metadataNodes.replicas") {
		t.Errorf("Expected error about metadataNodes.replicas, got: %v", err)
	}
}

func TestValidateCreate_NegativeDataReplicas(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: DataNodesSpec{
				Replicas: -1,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for negative data replicas, got nil")
	} else if !strings.Contains(err.Error(), "dataNodes.replicas") {
		t.Errorf("Expected error about dataNodes.replicas, got: %v", err)
	}
}

func TestValidateCreate_EnvFromBothRefsRejected(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.MetadataNodes.EnvFrom = []corev1.EnvFromSource{
		{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "my-secret"},
			},
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "my-configmap"},
			},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for envFrom with both secretRef and configMapRef, got nil")
	} else if !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("Expected error about 'exactly one', got: %v", err)
	}
}

func TestValidateCreate_EnvFromBothRefsRejectedDataNodes(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.DataNodes.EnvFrom = []corev1.EnvFromSource{
		{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "my-secret"},
			},
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "my-configmap"},
			},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for envFrom with both secretRef and configMapRef on dataNodes, got nil")
	} else if !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("Expected error about 'exactly one', got: %v", err)
	}
}

func TestValidateCreate_EKSThroughputOutOfRange(t *testing.T) {
	tests := []struct {
		name       string
		throughput int32
	}{
		{"too low", 50},
		{"too high", 1500},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := baseCluster()
			throughput := tt.throughput
			cluster.Spec.EKS = &EKSSpec{
				Enabled:       true,
				EBSVolumeType: "gp3",
				EBSThroughput: &throughput,
			}

			err := cluster.ValidateCreate()
			if err == nil {
				t.Errorf("Expected error for throughput %d, got nil", tt.throughput)
			} else if !strings.Contains(err.Error(), "ebsThroughput") {
				t.Errorf("Expected error about ebsThroughput, got: %v", err)
			}
		})
	}
}

func TestValidateCreate_EKSThroughputValid(t *testing.T) {
	cluster := baseCluster()
	throughput := int32(500)
	cluster.Spec.EKS = &EKSSpec{
		Enabled:       true,
		EBSVolumeType: "gp3",
		EBSThroughput: &throughput,
	}

	if err := cluster.ValidateCreate(); err != nil {
		t.Errorf("Expected no error for valid throughput, got: %v", err)
	}
}

func TestValidateCreate_EKSKmsKeyWithoutEncryption(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.EKS = &EKSSpec{
		Enabled:      true,
		EBSEncrypted: false,
		EBSKmsKeyId:  "arn:aws:kms:us-east-1:123456789012:key/my-key",
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for KMS key without encryption, got nil")
	} else if !strings.Contains(err.Error(), "ebsKmsKeyId") {
		t.Errorf("Expected error about ebsKmsKeyId, got: %v", err)
	}
}

func TestValidateCreate_EKSHyphenatedInstanceTypes(t *testing.T) {
	validTypes := []string{"u-6tb1.56xlarge", "mac2-m2.metal", "m5.large", "c5.xlarge", "r6i.2xlarge", "p3dn.24xlarge"}
	for _, it := range validTypes {
		t.Run(it, func(t *testing.T) {
			cluster := baseCluster()
			cluster.Spec.EKS = &EKSSpec{
				Enabled:       true,
				InstanceTypes: []string{it},
			}

			if err := cluster.ValidateCreate(); err != nil {
				t.Errorf("Expected no error for instance type %s, got: %v", it, err)
			}
		})
	}
}

func TestValidateCreate_EKSInvalidInstanceTypes(t *testing.T) {
	invalidTypes := []string{"INVALID", "m5", ".large", ""}
	for _, it := range invalidTypes {
		name := it
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			cluster := baseCluster()
			cluster.Spec.EKS = &EKSSpec{
				Enabled:       true,
				InstanceTypes: []string{it},
			}

			if err := cluster.ValidateCreate(); err == nil {
				t.Errorf("Expected error for instance type %q, got nil", it)
			}
		})
	}
}

func TestValidateCreate_PVCRetentionPolicyValid(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.Storage.PVCRetentionPolicy = &PVCRetentionPolicy{
		WhenDeleted: PVCRetentionDelete,
		WhenScaled:  PVCRetentionRetain,
	}

	err := cluster.ValidateCreate()
	if err != nil {
		t.Errorf("Expected no error for valid PVC retention policy, got: %v", err)
	}
}

func TestValidateCreate_PVCRetentionPolicyWithAutoscaling(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.Storage.PVCRetentionPolicy = &PVCRetentionPolicy{
		WhenDeleted: PVCRetentionRetain,
		WhenScaled:  PVCRetentionDelete,
	}
	cluster.Spec.DataNodes.AutoScaling = &AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 2,
		MaxReplicas: 5,
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Error("Expected error for WhenScaled=Delete with autoscaling enabled")
	}
}

func TestValidateUpdate_StorageClassImmutable(t *testing.T) {
	old := baseCluster()
	old.Spec.Storage.StorageClass = "gp2"

	new := baseCluster()
	new.Spec.Storage.StorageClass = "gp3"

	err := new.ValidateUpdate(old)
	if err == nil {
		t.Error("Expected error when changing storage class")
	}
}

func TestValidateUpdate_StorageSizeIncreaseAllowed(t *testing.T) {
	old := baseCluster()
	old.Spec.Storage.DataStorage = "1Gi"

	new := baseCluster()
	new.Spec.Storage.DataStorage = "2Gi"

	err := new.ValidateUpdate(old)
	if err != nil {
		t.Errorf("Expected no error for storage size increase, got: %v", err)
	}
}

func TestValidateUpdate_StorageSizeDecreaseRejected(t *testing.T) {
	old := baseCluster()
	old.Spec.Storage.DataStorage = "2Gi"

	new := baseCluster()
	new.Spec.Storage.DataStorage = "1Gi"

	err := new.ValidateUpdate(old)
	if err == nil {
		t.Error("Expected error when decreasing storage size")
	}
}

func TestValidateUpdate_StandaloneStorageSizeDecreaseRejected(t *testing.T) {
	old := baseStandaloneCluster()
	old.Spec.Storage.StandaloneStorage = "2Gi"

	new := baseStandaloneCluster()
	new.Spec.Storage.StandaloneStorage = "1Gi"

	err := new.ValidateUpdate(old)
	if err == nil {
		t.Fatal("expected error when decreasing standalone storage size")
	}
	if !strings.Contains(err.Error(), "standaloneStorage") {
		t.Fatalf("expected standaloneStorage error, got: %v", err)
	}
}

func TestValidateCreate_StorageAutoGrowRequiresMaxDataStorage(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.Storage.StorageAutoGrow = &StorageAutoGrowSpec{
		Enabled:              true,
		GrowThresholdPercent: 85,
		GrowIncrement:        "5Gi",
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected storage auto-grow max storage validation error")
	}
	if !strings.Contains(err.Error(), "maxDataStorage") {
		t.Fatalf("expected maxDataStorage error, got: %v", err)
	}
}

func TestValidateCreate_StorageAutoGrowValid(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.Storage.StorageAutoGrow = &StorageAutoGrowSpec{
		Enabled:              true,
		MaxDataStorage:       "20Gi",
		GrowThresholdPercent: 85,
		GrowIncrement:        "5Gi",
	}

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected valid storage auto-grow config, got: %v", err)
	}
}

func TestValidateCreate_ProductTierRequiresExplicitClusteredShape(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.ProductTier = &ProductTierSpec{
		Name:      "pro",
		Revision:  "2026-05",
		ManagedBy: "cloudaf",
		DataTier:  "data-large",
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected product tier validation error")
	}
	if !strings.Contains(err.Error(), "metadataNodes.resources") {
		t.Fatalf("expected metadata resources error, got: %v", err)
	}
}

func TestValidateCreate_ProductTierValidClusteredShape(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.ProductTier = &ProductTierSpec{
		Name:         "pro",
		Revision:     "2026-05",
		ManagedBy:    "cloudaf",
		MetadataTier: "metadata-small",
		DataTier:     "data-large",
	}
	cluster.Spec.MetadataNodes.Resources = ResourceSpec{CPU: "500m", Memory: "1Gi"}
	cluster.Spec.DataNodes.Resources = ResourceSpec{CPU: "2", Memory: "8Gi"}
	cluster.Spec.DataNodes.AutoScaling = &AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 3,
		MaxReplicas: 8,
	}

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected valid product tier shape, got: %v", err)
	}
}

func TestValidateCreate_ProductTierInferenceTierRequiresInferenceSpec(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.ProductTier = &ProductTierSpec{
		Name:          "pro",
		InferenceTier: "embed-small",
	}
	cluster.Spec.MetadataNodes.Resources = ResourceSpec{CPU: "500m", Memory: "1Gi"}
	cluster.Spec.DataNodes.Resources = ResourceSpec{CPU: "2", Memory: "8Gi"}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected inference tier validation error")
	}
	if !strings.Contains(err.Error(), "spec.inference is required") {
		t.Fatalf("expected inference spec error, got: %v", err)
	}
}

func TestValidateUpdate_MetadataScaleDownRejected(t *testing.T) {
	old := baseCluster()
	old.Spec.MetadataNodes.Replicas = 5

	new := baseCluster()
	new.Spec.MetadataNodes.Replicas = 3

	err := new.ValidateUpdate(old)
	if err == nil {
		t.Fatal("expected error when decreasing metadata replicas")
	}
	if !strings.Contains(err.Error(), "metadataNodes.replicas") {
		t.Fatalf("expected metadata replica error, got: %v", err)
	}
}

func TestValidateUpdate_DataScaleUpAllowed(t *testing.T) {
	old := baseCluster()
	old.Spec.DataNodes.Replicas = 3

	new := baseCluster()
	new.Spec.DataNodes.Replicas = 5

	if err := new.ValidateUpdate(old); err != nil {
		t.Fatalf("expected no error for data scale-up, got: %v", err)
	}
}

func TestValidateUpdate_DataScaleDownAllowed(t *testing.T) {
	old := baseCluster()
	old.Spec.DataNodes.Replicas = 5

	new := baseCluster()
	new.Spec.DataNodes.Replicas = 3

	if err := new.ValidateUpdate(old); err != nil {
		t.Fatalf("expected data scale-down to be admitted for controller-owned drain workflow, got: %v", err)
	}
}

func TestValidateCreate_DataSuspendAllowedWithRetainedPVCs(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.DataNodes.Suspend = true
	cluster.Spec.Storage.PVCRetentionPolicy = &PVCRetentionPolicy{
		WhenScaled: PVCRetentionRetain,
	}

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected data suspend with retained PVCs to be admitted, got: %v", err)
	}
}

func TestValidateCreate_DataSuspendRejectsDeleteOnScale(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.DataNodes.Suspend = true
	cluster.Spec.Storage.PVCRetentionPolicy = &PVCRetentionPolicy{
		WhenScaled: PVCRetentionDelete,
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected data suspend with WhenScaled=Delete to be rejected")
	}
	if !strings.Contains(err.Error(), "dataNodes.suspend") {
		t.Fatalf("expected data suspend error, got: %v", err)
	}
}

func TestValidateCreate_DataSuspendRejectsAutoscaling(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.DataNodes.Suspend = true
	cluster.Spec.DataNodes.AutoScaling = &AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 1,
		MaxReplicas: 3,
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected data suspend with autoscaling to be rejected")
	}
	if !strings.Contains(err.Error(), "dataNodes.suspend") {
		t.Fatalf("expected data suspend error, got: %v", err)
	}
}

func TestValidateCreate_AutoScalingBoundsRejected(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.DataNodes.AutoScaling = &AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 5,
		MaxReplicas: 3,
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected error for invalid autoscaling bounds")
	}
	if !strings.Contains(err.Error(), "minReplicas") {
		t.Fatalf("expected autoscaling bounds error, got: %v", err)
	}
}

func TestValidateCreate_AutoScalingMinReplicasZeroRejected(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.DataNodes.AutoScaling = &AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 0,
		MaxReplicas: 3,
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected error for autoscaling minReplicas zero")
	}
	if !strings.Contains(err.Error(), "minReplicas") {
		t.Fatalf("expected autoscaling minReplicas error, got: %v", err)
	}
}

func TestValidateCreate_DisabledAutoScalingIgnoresBounds(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.DataNodes.AutoScaling = &AutoScalingSpec{
		Enabled:     false,
		MinReplicas: 5,
		MaxReplicas: 0,
	}

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected disabled autoscaling bounds to be ignored, got: %v", err)
	}
}

func TestValidateUpdate_AutoScalingMaxBelowObservedReplicasAllowed(t *testing.T) {
	old := baseCluster()
	old.Spec.DataNodes.AutoScaling = &AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 3,
		MaxReplicas: 8,
	}
	old.Status.AutoScalingStatus = &AutoScalingStatus{
		CurrentReplicas: 7,
		DesiredReplicas: 7,
	}

	new := baseCluster()
	new.Spec.DataNodes.AutoScaling = &AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 2,
		MaxReplicas: 6,
	}

	if err := new.ValidateUpdate(old); err != nil {
		t.Fatalf("expected autoscaling maxReplicas below observed replicas to be admitted for controller-owned drain workflow, got: %v", err)
	}
}

func TestValidateUpdate_AutoScalingBoundsCanDecreaseAboveObservedReplicas(t *testing.T) {
	old := baseCluster()
	old.Spec.DataNodes.AutoScaling = &AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 3,
		MaxReplicas: 8,
	}
	old.Status.AutoScalingStatus = &AutoScalingStatus{
		CurrentReplicas: 5,
		DesiredReplicas: 5,
	}

	new := baseCluster()
	new.Spec.DataNodes.AutoScaling = &AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 2,
		MaxReplicas: 6,
	}

	if err := new.ValidateUpdate(old); err != nil {
		t.Fatalf("expected autoscaling bounds above observed replicas to be allowed, got: %v", err)
	}
}

func TestValidateUpdate_PVCRetentionPolicyMutable(t *testing.T) {
	old := baseCluster()
	old.Spec.Storage.PVCRetentionPolicy = &PVCRetentionPolicy{
		WhenDeleted: PVCRetentionRetain,
		WhenScaled:  PVCRetentionRetain,
	}

	new := baseCluster()
	new.Spec.Storage.PVCRetentionPolicy = &PVCRetentionPolicy{
		WhenDeleted: PVCRetentionDelete,
		WhenScaled:  PVCRetentionRetain,
	}

	err := new.ValidateUpdate(old)
	if err != nil {
		t.Errorf("Expected no error for PVC retention policy change, got: %v", err)
	}
}

func TestDefault_StandaloneDefaults(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "standalone-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Mode:       ClusterModeStandalone,
			Image:      "antfly:latest",
			Standalone: &StandaloneSpec{},
			Storage: StorageSpec{
				StorageClass:      "standard",
				StandaloneStorage: "1Gi",
			},
			Config: "{}",
		},
	}

	cluster.Default()

	if cluster.Spec.Mode != ClusterModeStandalone {
		t.Fatalf("expected standalone mode to remain set, got %q", cluster.Spec.Mode)
	}
	if cluster.Spec.Storage.Engine != "local" || cluster.Spec.Storage.LiteFileName != "" {
		t.Fatalf("expected local storage defaults without Lite-only fields, got engine=%q file=%q", cluster.Spec.Storage.Engine, cluster.Spec.Storage.LiteFileName)
	}
	if cluster.Spec.Standalone.Replicas != 1 {
		t.Fatalf("expected default standalone replicas=1, got %d", cluster.Spec.Standalone.Replicas)
	}
	if cluster.Spec.Standalone.NodeID != 1 {
		t.Fatalf("expected default standalone nodeID=1, got %d", cluster.Spec.Standalone.NodeID)
	}
	if cluster.Spec.Standalone.MetadataAPI.Port != 8080 {
		t.Fatalf("expected default standalone metadata API port 8080, got %d", cluster.Spec.Standalone.MetadataAPI.Port)
	}
	if cluster.Spec.Standalone.MetadataRaft.Port != 9017 {
		t.Fatalf("expected default standalone metadata raft port 9017, got %d", cluster.Spec.Standalone.MetadataRaft.Port)
	}
	if cluster.Spec.Standalone.StoreAPI.Port != 12380 {
		t.Fatalf("expected default standalone store API port 12380, got %d", cluster.Spec.Standalone.StoreAPI.Port)
	}
	if cluster.Spec.Standalone.StoreRaft.Port != 9021 {
		t.Fatalf("expected default standalone store raft port 9021, got %d", cluster.Spec.Standalone.StoreRaft.Port)
	}
	if cluster.Spec.Standalone.Health.Port != 4200 {
		t.Fatalf("expected default standalone health port 4200, got %d", cluster.Spec.Standalone.Health.Port)
	}
	if cluster.Spec.Standalone.Inference == nil {
		t.Fatal("expected default inference configuration to be populated")
	}
	if !cluster.Spec.Standalone.Inference.Enabled {
		t.Fatal("expected inference to default enabled for standalone mode")
	}
	if cluster.Spec.Standalone.Inference.APIURL != "http://0.0.0.0:11433" {
		t.Fatalf("expected default inference API URL, got %q", cluster.Spec.Standalone.Inference.APIURL)
	}
}

func TestDefault_StandaloneLiteDefaultsFileName(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.Storage.Engine = "lite"
	cluster.Spec.Storage.LiteFileName = ""
	cluster.Default()
	if cluster.Spec.Storage.LiteFileName != "antfly.aflite" {
		t.Fatalf("expected Lite filename default, got %q", cluster.Spec.Storage.LiteFileName)
	}
}

func TestValidateCreate_ValidStandalone(t *testing.T) {
	cluster := baseStandaloneCluster()

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected valid standalone cluster to pass validation, got: %v", err)
	}
}

func TestValidateCreate_StandaloneRequiresStorage(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.Storage.StandaloneStorage = ""

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected error when standalone storage is missing")
	}
	if !strings.Contains(err.Error(), "spec.storage.standaloneStorage") {
		t.Fatalf("expected standalone storage validation error, got: %v", err)
	}
}

func TestValidateCreate_StandaloneAcceptsTypedLiteStorage(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.Storage.Engine = "lite"
	cluster.Spec.Storage.LiteFileName = "data.aflite"

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected typed standalone Lite storage to pass admission, got: %v", err)
	}
}

func TestValidateCreate_StandaloneRejectsUnsafeLiteFileName(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.Storage.Engine = "lite"
	cluster.Spec.Storage.LiteFileName = "../data.aflite"
	if err := cluster.ValidateCreate(); err == nil || !strings.Contains(err.Error(), "liteFileName") {
		t.Fatalf("expected safe Lite basename validation error, got: %v", err)
	}
}

func TestValidateCreate_StandaloneLocalRejectsLiteFileName(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.Storage.Engine = "local"
	cluster.Spec.Storage.LiteFileName = "data.aflite"
	if err := cluster.ValidateCreate(); err == nil || !strings.Contains(err.Error(), "liteFileName") {
		t.Fatalf("expected tagged storage validation error, got: %v", err)
	}
}

func TestValidateCreate_ClusteredRejectsLiteStorage(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.Storage.Engine = "lite"
	cluster.Spec.Storage.LiteFileName = "data.aflite"
	if err := cluster.ValidateCreate(); err == nil || !strings.Contains(err.Error(), "spec.storage.engine") {
		t.Fatalf("expected clustered Lite storage validation error, got: %v", err)
	}
}

func TestValidateCreate_StandaloneRejectsClusteredFields(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.MetadataNodes.Replicas = 3

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected error when clustered fields are set in standalone mode")
	}
	if !strings.Contains(err.Error(), "spec.metadataNodes.replicas") {
		t.Fatalf("expected clustered field validation error, got: %v", err)
	}
}

func TestValidateCreate_StandaloneRejectsInvalidInferenceURL(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.Standalone.Inference = &StandaloneInferenceSpec{
		Enabled: true,
		APIURL:  "localhost:11433",
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected error for invalid inference API URL")
	}
	if !strings.Contains(err.Error(), "spec.standalone.inference.apiURL") {
		t.Fatalf("expected inference URL validation error, got: %v", err)
	}
}

func TestValidateCreate_RejectsInvalidManagedInferenceSpec(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.Inference = &AntflyInferenceSpec{
		Mode: AntflyInferenceModeManaged,
		ManagedPools: []ManagedInferencePoolSpec{{
			Spec: inferencev1alpha1.InferencePoolSpec{
				Models:   inferencev1alpha1.ModelConfig{},
				Replicas: inferencev1alpha1.ReplicaConfig{Min: 3, Max: 1},
				Hardware: inferencev1alpha1.HardwareConfig{},
			},
		}},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected error for invalid managed inference spec")
	}
	if !strings.Contains(err.Error(), "spec.inference.managedPools[0].spec is invalid") {
		t.Fatalf("expected managed inference validation error, got: %v", err)
	}
}

func TestValidateUpdate_ModeImmutable(t *testing.T) {
	oldCluster := baseCluster()

	newCluster := oldCluster.DeepCopy()
	newCluster.Spec.Mode = ClusterModeStandalone
	newCluster.Spec.Standalone = &StandaloneSpec{
		Replicas:     1,
		NodeID:       1,
		MetadataAPI:  APISpec{Port: 8080},
		MetadataRaft: APISpec{Port: 9017},
		StoreAPI:     APISpec{Port: 12380},
		StoreRaft:    APISpec{Port: 9021},
		Health:       APISpec{Port: 4200},
		Inference: &StandaloneInferenceSpec{
			Enabled: true,
			APIURL:  "http://0.0.0.0:11433",
		},
	}
	newCluster.Spec.MetadataNodes = MetadataNodesSpec{}
	newCluster.Spec.DataNodes = DataNodesSpec{}
	newCluster.Spec.Storage = StorageSpec{
		StorageClass:      "standard",
		StandaloneStorage: "1Gi",
	}

	err := newCluster.ValidateUpdate(oldCluster)
	if err == nil {
		t.Fatal("expected error when changing mode from Clustered to Standalone")
	}
	if !strings.Contains(err.Error(), "spec.mode") {
		t.Fatalf("expected immutable mode validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityHotStandbyValid(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Admin: &HAAdminSpec{
			PrimaryURL:            "http://primary-ha.default.svc:8081",
			ExecutePlannedActions: true,
		},
		Standbys: []HAStandbySpec{
			{Name: "standby-a", AdminURL: "http://standby-a-ha.default.svc:8081", RouteSelector: map[string]string{
				"app.kubernetes.io/name":      "antfly-database",
				"app.kubernetes.io/component": "standby-a",
			}},
			{Name: "standby-b", AdminURL: "http://standby-b-ha.default.svc:8081"},
		},
		SyncPolicy: &HASyncPolicy{
			Mode:          HADurabilityModeRemoteApply,
			Selection:     HAStandbySelectionAny,
			Required:      1,
			StandbyNames:  []string{"standby-a", "standby-b"},
			FailurePolicy: HAFailurePolicyBlock,
		},
		AutomaticFailover: &HAAutomaticFailoverPolicy{
			Enabled:          true,
			FencingAuthority: HAFencingAuthorityKubernetesLease,
		},
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			ShardID:          0,
			TableID:          0,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
	}

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected valid hot-standby HA config, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityAllowsEmptyDisabledConfig(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{}

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected empty disabled HA configuration to be accepted, got: %v", err)
	}

	cluster.Spec.HighAvailability.Mode = HAModeDisabled
	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected explicit disabled HA configuration to be accepted, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsManagedConfigWithoutHotStandbyMode(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Standbys: []HAStandbySpec{{Name: "standby-a"}},
		Admin: &HAAdminSpec{
			PrimaryURL: "http://primary-ha.default.svc:8081",
		},
		Retention: &HARetentionPolicy{MaxLagLSN: 5},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected HA configuration without HotStandby mode to be rejected")
	}
	if !strings.Contains(err.Error(), "mode must be HotStandby when HA configuration fields are set") {
		t.Fatalf("expected HA mode validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Mode = HAModeDisabled
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected disabled HA configuration with managed fields to be rejected")
	}
	if !strings.Contains(err.Error(), "mode must be HotStandby when HA configuration fields are set") {
		t.Fatalf("expected disabled HA mode validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityAllowsExecutableActionsWithoutEveryStandbyAdminURL(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Admin: &HAAdminSpec{
			PrimaryURL:            "http://primary-ha.default.svc:8081",
			ExecutePlannedActions: true,
		},
		Standbys: []HAStandbySpec{
			{Name: "standby-a", AdminURL: "http://standby-a-ha.default.svc:8081"},
			{Name: "standby-b"},
		},
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			ShardID:          10,
			TableID:          20,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
	}

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected partial HA admin endpoint configuration to be valid without automatic failover, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsAdminExecutionWithoutIdentity(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Admin: &HAAdminSpec{
			PrimaryURL:            "http://primary-ha.default.svc:8081",
			ExecutePlannedActions: true,
		},
		Standbys: []HAStandbySpec{
			{Name: "standby-a", AdminURL: "http://standby-a-ha.default.svc:8081"},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected HA admin execution without identity to be rejected")
	}
	if !strings.Contains(err.Error(), "admin.executePlannedActions requires spec.highAvailability.identity") {
		t.Fatalf("expected HA admin execution identity validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsInvalidAdminURLs(t *testing.T) {
	cluster := baseStandaloneCluster()
	backoffLimit := int32(-1)
	timeoutSeconds := int64(0)
	ttlSecondsAfterFinished := int32(-10)
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Admin: &HAAdminSpec{
			PrimaryURL:                 "primary-ha.default.svc:8081",
			ExecutePlannedActions:      true,
			JobBackoffLimit:            &backoffLimit,
			JobTimeoutSeconds:          &timeoutSeconds,
			JobTTLSecondsAfterFinished: &ttlSecondsAfterFinished,
		},
		Standbys: []HAStandbySpec{
			{Name: "standby-a", AdminURL: "grpc://standby-a-ha.default.svc:8081"},
			{Name: "standby-b"},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected invalid HA admin endpoint configuration to be rejected")
	}
	if !strings.Contains(err.Error(), "admin.primaryURL") ||
		!strings.Contains(err.Error(), "standbys[0].adminURL") ||
		!strings.Contains(err.Error(), "admin.jobBackoffLimit") ||
		!strings.Contains(err.Error(), "admin.jobTimeoutSeconds") ||
		!strings.Contains(err.Error(), "admin.jobTTLSecondsAfterFinished") {
		t.Fatalf("expected invalid HA admin endpoint errors, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsPaddedAdminURLs(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Admin: &HAAdminSpec{
			PrimaryURL: " http://primary-ha.default.svc:8081 ",
		},
		Standbys: []HAStandbySpec{{
			Name:     "standby-a",
			AdminURL: " http://standby-a-ha.default.svc:8081 ",
		}},
		Runtime: &HARuntimeSpec{
			Role:   HARuntimeRoleStandby,
			NodeID: "standby-a",
			Standby: &HAStandbyRuntimeSpec{
				SlotName:    "standby-a",
				UpstreamURL: " http://primary-ha.default.svc:8081 ",
			},
		},
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected padded HA admin URLs to be rejected")
	}
	for _, want := range []string{
		"admin.primaryURL must not have leading or trailing whitespace",
		"standbys[0].adminURL must not have leading or trailing whitespace",
		"runtime.standby.upstreamURL must not have leading or trailing whitespace",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected padded admin URL error %q, got: %v", want, err)
		}
	}
}

func TestValidateCreate_HighAvailabilityRejectsAdminURLsWithHiddenWhitespace(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Admin: &HAAdminSpec{
			PrimaryURL: "http://primary-ha.default.svc:8081/\tadmin",
		},
		Standbys: []HAStandbySpec{{
			Name:     "standby-a",
			AdminURL: "http://standby-a-ha.default.svc:8081/\vadmin",
		}},
		Runtime: &HARuntimeSpec{
			Role:   HARuntimeRoleStandby,
			NodeID: "standby-a",
			Standby: &HAStandbyRuntimeSpec{
				SlotName:    "standby-a",
				UpstreamURL: "http://primary-ha.default.svc:8081/\fadmin",
			},
		},
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected HA admin URLs with hidden whitespace to be rejected")
	}
	for _, want := range []string{
		"admin.primaryURL must not contain whitespace",
		"standbys[0].adminURL must not contain whitespace",
		"runtime.standby.upstreamURL must not contain whitespace",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected hidden whitespace admin URL error %q, got: %v", want, err)
		}
	}
}

func TestValidateCreate_HighAvailabilityRejectsInvalidAdminTokenEnvVar(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Admin: &HAAdminSpec{
			PrimaryURL:            "http://primary-ha.default.svc:8081",
			ExecutePlannedActions: true,
			TokenEnvVar:           "bad=token",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected invalid HA admin token environment variable to be rejected")
	}
	if !strings.Contains(err.Error(), "admin.tokenEnvVar") {
		t.Fatalf("expected admin.tokenEnvVar validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Admin.TokenEnvVar = "   "
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected whitespace HA admin token environment variable to be rejected")
	}
	if !strings.Contains(err.Error(), "admin.tokenEnvVar must not be whitespace") {
		t.Fatalf("expected admin.tokenEnvVar whitespace validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Admin.TokenEnvVar = " CUSTOM_HA_ADMIN_TOKEN "
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected HA admin token environment variable with surrounding whitespace to be rejected")
	}
	if !strings.Contains(err.Error(), "admin.tokenEnvVar must be a valid environment variable name") {
		t.Fatalf("expected admin.tokenEnvVar raw-name validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsWhitespacePaddedIdentityFields(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{{
			Name:     " standby-a ",
			SlotName: " slot-a ",
		}},
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: " primary-a ",
		},
		Runtime: &HARuntimeSpec{
			Role:   HARuntimeRolePrimary,
			NodeID: " primary-a ",
		},
		SyncPolicy: &HASyncPolicy{
			Mode:         HADurabilityModeRemoteWrite,
			Required:     1,
			StandbyNames: []string{" standby-a "},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected whitespace-padded HA identities to be rejected")
	}
	for _, want := range []string{
		"standbys[0].name must not have leading or trailing whitespace",
		"standbys[0].slotName must not have leading or trailing whitespace",
		"identity.currentPrimaryID must not have leading or trailing whitespace",
		"runtime.nodeID must not have leading or trailing whitespace",
		"syncPolicy.standbyNames[0] must not have leading or trailing whitespace",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected validation error containing %q, got: %v", want, err)
		}
	}
}

func TestValidateCreate_HighAvailabilityRejectsInvalidIdentifiers(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{{
			Name:     "standby a",
			SlotName: "slot a",
		}},
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary a",
		},
		Runtime: &HARuntimeSpec{
			Role:   HARuntimeRolePrimary,
			NodeID: "primary a",
		},
		SyncPolicy: &HASyncPolicy{
			Mode:         HADurabilityModeRemoteWrite,
			Required:     1,
			StandbyNames: []string{"standby a"},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected invalid HA identifiers to be rejected")
	}
	for _, want := range []string{
		"standbys[0].name must be a valid HA identifier",
		"standbys[0].slotName must be a valid HA identifier",
		"identity.currentPrimaryID must be a valid HA identifier",
		"runtime.nodeID must be a valid HA identifier",
		"syncPolicy.standbyNames[0] must be a valid HA identifier",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected validation error containing %q, got: %v", want, err)
		}
	}
}

func TestValidateCreate_HighAvailabilityRuntimeRequiresIdentityAndNodeID(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Runtime: &HARuntimeSpec{
			Role: HARuntimeRolePrimary,
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected incomplete HA runtime configuration to be rejected")
	}
	if !strings.Contains(err.Error(), "runtime.nodeID is required") ||
		!strings.Contains(err.Error(), "runtime requires spec.highAvailability.identity") {
		t.Fatalf("expected HA runtime identity/nodeID validation errors, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRuntimeRequiresStandaloneMode(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
		Runtime: &HARuntimeSpec{
			Role:   HARuntimeRolePrimary,
			NodeID: "primary-a",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected HA runtime configuration outside standalone mode to be rejected")
	}
	if !strings.Contains(err.Error(), "runtime is only supported when spec.mode=Standalone") {
		t.Fatalf("expected HA runtime standalone-mode validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRuntimeNodeIDMustMatchRoleIdentity(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
		Runtime: &HARuntimeSpec{
			Role:   HARuntimeRolePrimary,
			NodeID: "standby-a",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected primary runtime node identity mismatch to be rejected")
	}
	if !strings.Contains(err.Error(), "runtime.nodeID must match spec.highAvailability.identity.currentPrimaryID") {
		t.Fatalf("expected primary runtime node identity validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.Role = HARuntimeRoleStandby
	cluster.Spec.HighAvailability.Runtime.NodeID = "primary-a"
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected standby runtime using current primary node identity to be rejected")
	}
	if !strings.Contains(err.Error(), "runtime.nodeID must not match spec.highAvailability.identity.currentPrimaryID") {
		t.Fatalf("expected standby runtime node identity validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.NodeID = "standby-a"
	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected standby runtime with distinct node identity to be valid, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsWhitespaceFormerPrimaryLogPath(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
		Runtime: &HARuntimeSpec{
			Role:                 HARuntimeRolePrimary,
			NodeID:               "primary-a",
			FormerPrimaryLogPath: " \t ",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected whitespace former primary log path to be rejected")
	}
	if !strings.Contains(err.Error(), "runtime.formerPrimaryLogPath must not be whitespace") {
		t.Fatalf("expected formerPrimaryLogPath validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsPaddedRuntimePaths(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
		Runtime: &HARuntimeSpec{
			Role:                 HARuntimeRolePrimary,
			NodeID:               "primary-a",
			FencePath:            " /antflydb/ha/fence.wal ",
			FormerPrimaryLogPath: " /antflydb/ha/primary.wal ",
			Primary: &HAPrimaryRuntimeSpec{
				LogPath:   " /antflydb/ha/primary.wal ",
				SlotsPath: " /antflydb/ha/slots ",
			},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected padded HA primary runtime paths to be rejected")
	}
	for _, want := range []string{
		"runtime.primary.logPath must not have leading or trailing whitespace",
		"runtime.primary.slotsPath must not have leading or trailing whitespace",
		"runtime.fencePath must not have leading or trailing whitespace",
		"runtime.formerPrimaryLogPath must not have leading or trailing whitespace",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected validation error containing %q, got: %v", want, err)
		}
	}

	cluster.Spec.HighAvailability.Identity.CurrentPrimaryID = "primary-a"
	cluster.Spec.HighAvailability.Runtime = &HARuntimeSpec{
		Role:      HARuntimeRoleStandby,
		NodeID:    "standby-a",
		FencePath: "/antflydb/ha/fence.wal",
		Standby: &HAStandbyRuntimeSpec{
			LogPath:      " /antflydb/ha/standby.wal ",
			ProgressPath: " /antflydb/ha/progress.wal ",
		},
	}
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected padded HA standby runtime paths to be rejected")
	}
	for _, want := range []string{
		"runtime.standby.logPath must not have leading or trailing whitespace",
		"runtime.standby.progressPath must not have leading or trailing whitespace",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected validation error containing %q, got: %v", want, err)
		}
	}
}

func TestValidateCreate_HighAvailabilityRejectsRelativeOrNonNormalizedRuntimePaths(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
		Runtime: &HARuntimeSpec{
			Role:                 HARuntimeRolePrimary,
			NodeID:               "primary-a",
			FencePath:            "ha/fence.wal",
			FormerPrimaryLogPath: "/antflydb/ha/../primary.wal",
			Primary: &HAPrimaryRuntimeSpec{
				LogPath:   "ha/primary.wal",
				SlotsPath: "/antflydb/ha//slots",
			},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected relative and non-normalized HA primary runtime paths to be rejected")
	}
	for _, want := range []string{
		"runtime.primary.logPath must be an absolute normalized path",
		"runtime.primary.slotsPath must be an absolute normalized path",
		"runtime.fencePath must be an absolute normalized path",
		"runtime.formerPrimaryLogPath must be an absolute normalized path",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected validation error containing %q, got: %v", want, err)
		}
	}

	cluster.Spec.HighAvailability.Identity.CurrentPrimaryID = "primary-a"
	cluster.Spec.HighAvailability.Runtime = &HARuntimeSpec{
		Role:      HARuntimeRoleStandby,
		NodeID:    "standby-a",
		FencePath: "/antflydb/ha/fence.wal",
		Standby: &HAStandbyRuntimeSpec{
			LogPath:      "ha/standby.wal",
			ProgressPath: "/antflydb/ha/../progress.wal",
		},
	}
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected relative and non-normalized HA standby runtime paths to be rejected")
	}
	for _, want := range []string{
		"runtime.standby.logPath must be an absolute normalized path",
		"runtime.standby.progressPath must be an absolute normalized path",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected validation error containing %q, got: %v", want, err)
		}
	}
}

func TestValidateCreate_HighAvailabilityRejectsInvalidRuntimeAdminTokenEnvVar(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
		Runtime: &HARuntimeSpec{
			Role:             HARuntimeRolePrimary,
			NodeID:           "primary-a",
			AdminTokenEnvVar: "bad-token-env",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected invalid runtime admin token environment variable to be rejected")
	}
	if !strings.Contains(err.Error(), "runtime.adminTokenEnvVar") {
		t.Fatalf("expected runtime.adminTokenEnvVar validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.AdminTokenEnvVar = " ANTFLY_HA_ADMIN_TOKEN "
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected runtime admin token environment variable with surrounding whitespace to be rejected")
	}
	if !strings.Contains(err.Error(), "runtime.adminTokenEnvVar") {
		t.Fatalf("expected runtime.adminTokenEnvVar raw-name validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRuntimeAdminTokenRequiresPodEnvSource(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
		Runtime: &HARuntimeSpec{
			Role:             HARuntimeRolePrimary,
			NodeID:           "primary-a",
			AdminTokenEnvVar: "ANTFLY_HA_ADMIN_TOKEN",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected runtime admin token without pod env source to be rejected")
	}
	if !strings.Contains(err.Error(), "adminTokenSecretRef or spec.standalone.envFrom") {
		t.Fatalf("expected runtime admin token source validation error, got: %v", err)
	}

	cluster.Spec.Standalone.EnvFrom = []corev1.EnvFromSource{{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
		},
	}}
	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected runtime admin token to accept spec.standalone.envFrom token source, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRuntimeAdminTokenAcceptsSecretRef(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
		Runtime: &HARuntimeSpec{
			Role:             HARuntimeRolePrimary,
			NodeID:           "primary-a",
			AdminTokenEnvVar: "ANTFLY_HA_ADMIN_TOKEN",
			AdminTokenSecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
				Key:                  "token",
			},
		},
	}

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected runtime admin token secret ref to be valid, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.AdminTokenEnvVar = ""
	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected adminTokenSecretRef without adminTokenEnvVar to be rejected")
	}
	if !strings.Contains(err.Error(), "adminTokenEnvVar is required when adminTokenSecretRef is set") {
		t.Fatalf("expected adminTokenEnvVar-required validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.AdminTokenEnvVar = "ANTFLY_HA_ADMIN_TOKEN"
	cluster.Spec.HighAvailability.Runtime.AdminTokenSecretRef.Name = ""
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected runtime admin token secret ref without name to be rejected")
	}
	if !strings.Contains(err.Error(), "adminTokenSecretRef.name is required") {
		t.Fatalf("expected adminTokenSecretRef name-required validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.AdminTokenSecretRef.Name = " ha-admin-token "
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected runtime admin token secret ref name with whitespace to be rejected")
	}
	if !strings.Contains(err.Error(), "adminTokenSecretRef.name must not have leading or trailing whitespace") {
		t.Fatalf("expected adminTokenSecretRef name whitespace validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.AdminTokenSecretRef.Name = "HA_ADMIN_TOKEN"
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected runtime admin token secret ref invalid name to be rejected")
	}
	if !strings.Contains(err.Error(), "adminTokenSecretRef.name") ||
		!strings.Contains(err.Error(), "is invalid") {
		t.Fatalf("expected adminTokenSecretRef invalid-name validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.AdminTokenSecretRef.Name = "ha-admin-token"
	cluster.Spec.HighAvailability.Runtime.AdminTokenSecretRef.Key = ""
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected runtime admin token secret ref without key to be rejected")
	}
	if !strings.Contains(err.Error(), "adminTokenSecretRef.key is required") {
		t.Fatalf("expected adminTokenSecretRef key-required validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.AdminTokenSecretRef.Key = " token "
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected runtime admin token secret ref key with whitespace to be rejected")
	}
	if !strings.Contains(err.Error(), "adminTokenSecretRef.key must not have leading or trailing whitespace") {
		t.Fatalf("expected adminTokenSecretRef key whitespace validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.AdminTokenSecretRef.Key = "bad/key"
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected runtime admin token secret ref invalid key to be rejected")
	}
	if !strings.Contains(err.Error(), "adminTokenSecretRef.key") ||
		!strings.Contains(err.Error(), "is invalid") {
		t.Fatalf("expected adminTokenSecretRef invalid-key validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.AdminTokenSecretRef.Key = "token"
	optional := true
	cluster.Spec.HighAvailability.Runtime.AdminTokenSecretRef.Optional = &optional
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected optional runtime admin token secret ref to be rejected")
	}
	if !strings.Contains(err.Error(), "adminTokenSecretRef.optional must be false") {
		t.Fatalf("expected adminTokenSecretRef optional validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsInvalidStandbyRuntimeReplicationSource(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
		Runtime: &HARuntimeSpec{
			Role:   HARuntimeRoleStandby,
			NodeID: "standby-a",
			Standby: &HAStandbyRuntimeSpec{
				UpstreamURL: "grpc://primary.default.svc:8080",
				SlotName:    "standby-a",
			},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected invalid HA standby runtime upstream URL to be rejected")
	}
	if !strings.Contains(err.Error(), "runtime.standby.upstreamURL") {
		t.Fatalf("expected standby upstreamURL validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.Standby.UpstreamURL = "http://primary.default.svc:8080"
	cluster.Spec.HighAvailability.Runtime.Standby.SlotName = ""
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected HA standby runtime slotName to be required with upstreamURL")
	}
	if !strings.Contains(err.Error(), "runtime.standby.slotName is required when upstreamURL is set") {
		t.Fatalf("expected standby slotName validation error, got: %v", err)
	}

	cluster.Spec.HighAvailability.Runtime.Standby.SlotName = " standby-a "
	err = cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected HA standby runtime slotName with surrounding whitespace to be rejected")
	}
	if !strings.Contains(err.Error(), "runtime.standby.slotName must not have leading or trailing whitespace") {
		t.Fatalf("expected standby slotName whitespace validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsAutomaticFailoverWithoutDesiredStandbyAdminURL(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Admin: &HAAdminSpec{
			PrimaryURL:            "http://primary-ha.default.svc:8081",
			ExecutePlannedActions: true,
		},
		Standbys: []HAStandbySpec{
			{Name: "standby-a", RouteSelector: map[string]string{
				"app.kubernetes.io/component": "standby-a",
			}},
		},
		AutomaticFailover: &HAAutomaticFailoverPolicy{
			Enabled:          true,
			FencingAuthority: HAFencingAuthorityKubernetesLease,
		},
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			ShardID:          10,
			TableID:          20,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected automatic failover without standby admin URL to be rejected")
	}
	if !strings.Contains(err.Error(), "standbys[0].adminURL is required when automaticFailover is enabled") {
		t.Fatalf("expected automatic failover standby admin URL validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsInvalidRouteSelector(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{{
			Name: "standby-a",
			RouteSelector: map[string]string{
				"bad key": "standby-a",
			},
		}},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected invalid HA route selector to be rejected")
	}
	if !strings.Contains(err.Error(), "standbys[0].routeSelector") {
		t.Fatalf("expected route selector validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRequiresFencingForAutomaticFailover(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Admin: &HAAdminSpec{
			PrimaryURL:            "http://primary-ha.default.svc:8081",
			ExecutePlannedActions: true,
		},
		Standbys: []HAStandbySpec{
			{Name: "standby-a", AdminURL: "http://standby-a-ha.default.svc:8081"},
		},
		AutomaticFailover: &HAAutomaticFailoverPolicy{
			Enabled:          true,
			FencingAuthority: HAFencingAuthorityNone,
		},
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			ShardID:          10,
			TableID:          20,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected automatic failover without fencing to be rejected")
	}
	if !strings.Contains(err.Error(), "automaticFailover.fencingAuthority") {
		t.Fatalf("expected fencing validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsAutomaticFailoverWithoutRouteSelector(t *testing.T) {
	disabled := false
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Admin: &HAAdminSpec{
			PrimaryURL:            "http://primary-ha.default.svc:8081",
			ExecutePlannedActions: true,
		},
		Standbys: []HAStandbySpec{
			{Name: "standby-a", AdminURL: "http://standby-a-ha.default.svc:8081"},
			{Name: "standby-b", Desired: &disabled, AdminURL: "http://standby-b-ha.default.svc:8081", RouteSelector: map[string]string{
				"app.kubernetes.io/component": "standby-b",
			}},
		},
		AutomaticFailover: &HAAutomaticFailoverPolicy{
			Enabled:          true,
			FencingAuthority: HAFencingAuthorityKubernetesLease,
		},
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			ShardID:          10,
			TableID:          20,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected automatic failover without a desired routable standby to be rejected")
	}
	if !strings.Contains(err.Error(), "automaticFailover requires at least one desired standby with routeSelector") {
		t.Fatalf("expected automatic failover route selector validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsAutomaticFailoverWithoutExecutionPrerequisites(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{
			{Name: "standby-a", AdminURL: "http://standby-a-ha.default.svc:8081"},
		},
		AutomaticFailover: &HAAutomaticFailoverPolicy{
			Enabled:          true,
			FencingAuthority: HAFencingAuthorityKubernetesLease,
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected automatic failover without admin execution prerequisites to be rejected")
	}
	if !strings.Contains(err.Error(), "automaticFailover requires spec.highAvailability.admin.executePlannedActions=true") ||
		!strings.Contains(err.Error(), "automaticFailover requires spec.highAvailability.identity") {
		t.Fatalf("expected automatic failover execution prerequisite errors, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsUnsupportedAutomaticFencingAuthority(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Admin: &HAAdminSpec{
			PrimaryURL:            "http://primary-ha.default.svc:8081",
			ExecutePlannedActions: true,
		},
		Standbys: []HAStandbySpec{
			{Name: "standby-a", AdminURL: "http://standby-a-ha.default.svc:8081"},
		},
		AutomaticFailover: &HAAutomaticFailoverPolicy{
			Enabled:          true,
			FencingAuthority: HAFencingAuthorityExternal,
		},
		Identity: &HAReplicationIdentitySpec{
			ClusterID:        100,
			ShardID:          10,
			TableID:          20,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected unsupported automatic fencing authority to be rejected")
	}
	if !strings.Contains(err.Error(), "automaticFailover.fencingAuthority must be KubernetesLease") {
		t.Fatalf("expected unsupported fencing authority error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityAllowsDefaultAsyncSyncPolicy(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{
			{Name: "standby-a"},
		},
		SyncPolicy: &HASyncPolicy{
			Mode:          HADurabilityModeAsync,
			Selection:     HAStandbySelectionAny,
			FailurePolicy: HAFailurePolicyBlock,
		},
	}

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected explicit default async sync policy to be accepted, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsAsyncSyncOnlyFields(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{
			{Name: "standby-a"},
		},
		SyncPolicy: &HASyncPolicy{
			Mode:          HADurabilityModeAsync,
			Selection:     HAStandbySelectionFirst,
			Required:      1,
			StandbyNames:  []string{"standby-a"},
			FailurePolicy: HAFailurePolicyDegradeToAsync,
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected async sync policy with synchronous-only fields to be rejected")
	}
	for _, want := range []string{
		"syncPolicy.required must be omitted when mode is Async",
		"syncPolicy.standbyNames must be omitted when mode is Async",
		"syncPolicy.selection must be Any or omitted when mode is Async",
		"syncPolicy.failurePolicy must be Block or omitted when mode is Async",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected async sync policy error %q, got: %v", want, err)
		}
	}
}

func TestValidateCreate_HighAvailabilitySyncStandbysMustBeDeclared(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{
			{Name: "standby-a"},
			{Name: "standby-a"},
		},
		SyncPolicy: &HASyncPolicy{
			Mode:         HADurabilityModeRemoteWrite,
			Required:     1,
			StandbyNames: []string{"standby-missing"},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected duplicate and missing sync standby validation errors")
	}
	if !strings.Contains(err.Error(), "duplicated") || !strings.Contains(err.Error(), "standby-missing") {
		t.Fatalf("expected duplicate and undeclared standby errors, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilitySyncStandbysMustBeDesired(t *testing.T) {
	disabled := false
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{
			{Name: "standby-a"},
			{Name: "standby-b", Desired: &disabled},
		},
		SyncPolicy: &HASyncPolicy{
			Mode:         HADurabilityModeRemoteWrite,
			Required:     1,
			StandbyNames: []string{"standby-b"},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected sync policy naming an undesired standby to be rejected")
	}
	if !strings.Contains(err.Error(), "syncPolicy.standbyNames[0] \"standby-b\" must reference a desired standby") {
		t.Fatalf("expected undesired sync standby validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilitySyncAllRequiresNoExplicitRequiredCount(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{
			{Name: "standby-a"},
			{Name: "standby-b"},
		},
		SyncPolicy: &HASyncPolicy{
			Mode:         HADurabilityModeRemoteApply,
			Selection:    HAStandbySelectionAll,
			StandbyNames: []string{"standby-a", "standby-b"},
		},
	}

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected ALL sync policy without required count to be accepted, got: %v", err)
	}

	cluster.Spec.HighAvailability.SyncPolicy.Required = 1
	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected ALL sync policy with required count to be rejected")
	}
	if !strings.Contains(err.Error(), "syncPolicy.required must be omitted when selection is All") {
		t.Fatalf("expected ALL sync required-count validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsDuplicateSlotIdentities(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{
			{Name: "standby-a", SlotName: "shared-slot"},
			{Name: "standby-b", SlotName: "shared-slot"},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected duplicate slot identity validation error")
	}
	if !strings.Contains(err.Error(), "duplicates standby slot identity") {
		t.Fatalf("expected duplicate slot identity error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsUnsatisfiableSyncCardinality(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{
			{Name: "standby-a"},
			{Name: "standby-b"},
		},
		SyncPolicy: &HASyncPolicy{
			Mode:         HADurabilityModeRemoteApply,
			Selection:    HAStandbySelectionFirst,
			Required:     4,
			StandbyNames: []string{"standby-a", "standby-b", "standby-b"},
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected unsatisfiable sync policy validation errors")
	}
	if !strings.Contains(err.Error(), "required (4) cannot exceed standbyNames length (3)") {
		t.Fatalf("expected sync cardinality error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "duplicates standbyNames") {
		t.Fatalf("expected duplicate sync standby error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsAutomaticFailoverWithoutStandbys(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		AutomaticFailover: &HAAutomaticFailoverPolicy{
			Enabled:          true,
			FencingAuthority: HAFencingAuthorityKubernetesLease,
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected automatic failover without standbys to be rejected")
	}
	if !strings.Contains(err.Error(), "automaticFailover requires at least one declared standby") {
		t.Fatalf("expected automatic failover standby validation error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsIncompleteIdentity(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{
			{Name: "standby-a"},
		},
		Identity: &HAReplicationIdentitySpec{
			ClusterID: 100,
			ShardID:   10,
			TableID:   20,
		},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected incomplete identity validation errors")
	}
	if !strings.Contains(err.Error(), "identity.timelineID") ||
		!strings.Contains(err.Error(), "identity.epoch") ||
		!strings.Contains(err.Error(), "identity.currentPrimaryID") {
		t.Fatalf("expected incomplete identity errors, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsIncompleteSeedPaths(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{{
			Name:            "standby-a",
			SeedContentRoot: "/backup/base-standby-a",
		}},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected seed manifest path validation error")
	}
	if !strings.Contains(err.Error(), "seedManifestPath is required when seedContentRoot is set") {
		t.Fatalf("expected seed path dependency error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityRejectsPaddedSeedPaths(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{{
			Name:             "standby-a",
			SeedManifestPath: " /backup/base-standby-a/manifest.json ",
			SeedContentRoot:  " /backup/base-standby-a ",
		}},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected padded seed path validation errors")
	}
	for _, want := range []string{
		"standbys[0].seedManifestPath must not have leading or trailing whitespace",
		"standbys[0].seedContentRoot must not have leading or trailing whitespace",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected padded seed path error %q, got: %v", want, err)
		}
	}
}

func TestValidateCreate_HighAvailabilityRejectsRelativeOrNonNormalizedSeedPaths(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{{
			Name:             "standby-a",
			SeedManifestPath: "backup/base-standby-a/manifest.json",
			SeedContentRoot:  "/backup/base-standby-a/../base-standby-a",
		}},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected relative and non-normalized seed path validation errors")
	}
	for _, want := range []string{
		"standbys[0].seedManifestPath must be an absolute normalized path",
		"standbys[0].seedContentRoot must be an absolute normalized path",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected seed path validation error %q, got: %v", want, err)
		}
	}
}

func TestValidateCreate_HighAvailabilityRejectsArmedSlotDropForDesiredStandby(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Standbys: []HAStandbySpec{{
			Name:              "standby-a",
			DropSlotOnRemoval: true,
		}},
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected dropSlotOnRemoval validation error")
	}
	if !strings.Contains(err.Error(), "dropSlotOnRemoval requires desired=false") {
		t.Fatalf("expected dropSlotOnRemoval dependency error, got: %v", err)
	}
}

func TestValidateCreate_HighAvailabilityAdminJobPodSpecValidation(t *testing.T) {
	cluster := baseStandaloneCluster()
	cluster.Spec.HighAvailability = &HighAvailabilitySpec{
		Mode: HAModeHotStandby,
		Admin: &HAAdminSpec{
			EnvFrom: []corev1.EnvFromSource{{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "backup-credentials"},
				},
			}},
			Volumes: []corev1.Volume{{
				Name: "ha-seed",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			}},
			VolumeMounts: []corev1.VolumeMount{{
				Name:      "ha-seed",
				MountPath: "/backup",
			}},
		},
		Standbys: []HAStandbySpec{{Name: "standby-a"}},
	}
	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected valid HA admin pod spec, got: %v", err)
	}

	cluster.Spec.HighAvailability.Admin.VolumeMounts[0].Name = "missing"
	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected dangling volumeMount validation error")
	}
	if !strings.Contains(err.Error(), "volumeMounts[0].name \"missing\" must reference spec.highAvailability.admin.volumes") {
		t.Fatalf("expected dangling volumeMount error, got: %v", err)
	}
}

func baseCluster() *AntflyCluster {
	return &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: DataNodesSpec{
				Replicas: 3,
			},
			Storage: StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
	}
}

func baseStandaloneCluster() *AntflyCluster {
	return &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-standalone-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Mode:  ClusterModeStandalone,
			Image: "antfly:latest",
			Standalone: &StandaloneSpec{
				Replicas:     1,
				NodeID:       1,
				Resources:    ResourceSpec{CPU: "500m", Memory: "1Gi"},
				MetadataAPI:  APISpec{Port: 8080},
				MetadataRaft: APISpec{Port: 9017},
				StoreAPI:     APISpec{Port: 12380},
				StoreRaft:    APISpec{Port: 9021},
				Health:       APISpec{Port: 4200},
				Inference: &StandaloneInferenceSpec{
					Enabled: true,
					APIURL:  "http://0.0.0.0:11433",
				},
			},
			Storage: StorageSpec{
				StorageClass:      "standard",
				StandaloneStorage: "1Gi",
			},
			Config: "{}",
		},
	}
}
