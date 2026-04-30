package v1

import (
	"fmt"
	"strings"
	"testing"

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
		t.Errorf("Expected no error for 0 data replicas, got: %v", err)
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

func TestDefault_SwarmDefaults(t *testing.T) {
	cluster := &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "swarm-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Mode:  ClusterModeSwarm,
			Image: "antfly:latest",
			Swarm: &SwarmSpec{},
			Storage: StorageSpec{
				StorageClass: "standard",
				SwarmStorage: "1Gi",
			},
			Config: "{}",
		},
	}

	cluster.Default()

	if cluster.Spec.Mode != ClusterModeSwarm {
		t.Fatalf("expected swarm mode to remain set, got %q", cluster.Spec.Mode)
	}
	if cluster.Spec.Swarm.Replicas != 1 {
		t.Fatalf("expected default swarm replicas=1, got %d", cluster.Spec.Swarm.Replicas)
	}
	if cluster.Spec.Swarm.NodeID != 1 {
		t.Fatalf("expected default swarm nodeID=1, got %d", cluster.Spec.Swarm.NodeID)
	}
	if cluster.Spec.Swarm.MetadataAPI.Port != 8080 {
		t.Fatalf("expected default swarm metadata API port 8080, got %d", cluster.Spec.Swarm.MetadataAPI.Port)
	}
	if cluster.Spec.Swarm.MetadataRaft.Port != 9017 {
		t.Fatalf("expected default swarm metadata raft port 9017, got %d", cluster.Spec.Swarm.MetadataRaft.Port)
	}
	if cluster.Spec.Swarm.StoreAPI.Port != 12380 {
		t.Fatalf("expected default swarm store API port 12380, got %d", cluster.Spec.Swarm.StoreAPI.Port)
	}
	if cluster.Spec.Swarm.StoreRaft.Port != 9021 {
		t.Fatalf("expected default swarm store raft port 9021, got %d", cluster.Spec.Swarm.StoreRaft.Port)
	}
	if cluster.Spec.Swarm.Health.Port != 4200 {
		t.Fatalf("expected default swarm health port 4200, got %d", cluster.Spec.Swarm.Health.Port)
	}
	if cluster.Spec.Swarm.Termite == nil {
		t.Fatal("expected default termite configuration to be populated")
	}
	if !cluster.Spec.Swarm.Termite.Enabled {
		t.Fatal("expected termite to default enabled for swarm mode")
	}
	if cluster.Spec.Swarm.Termite.APIURL != "http://0.0.0.0:11433" {
		t.Fatalf("expected default termite API URL, got %q", cluster.Spec.Swarm.Termite.APIURL)
	}
}

func TestValidateCreate_ValidSwarm(t *testing.T) {
	cluster := baseSwarmCluster()

	if err := cluster.ValidateCreate(); err != nil {
		t.Fatalf("expected valid swarm cluster to pass validation, got: %v", err)
	}
}

func TestValidateCreate_SwarmRequiresStorage(t *testing.T) {
	cluster := baseSwarmCluster()
	cluster.Spec.Storage.SwarmStorage = ""

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected error when swarm storage is missing")
	}
	if !strings.Contains(err.Error(), "spec.storage.swarmStorage") {
		t.Fatalf("expected swarm storage validation error, got: %v", err)
	}
}

func TestValidateCreate_SwarmRejectsClusteredFields(t *testing.T) {
	cluster := baseSwarmCluster()
	cluster.Spec.MetadataNodes.Replicas = 3

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected error when clustered fields are set in swarm mode")
	}
	if !strings.Contains(err.Error(), "spec.metadataNodes.replicas") {
		t.Fatalf("expected clustered field validation error, got: %v", err)
	}
}

func TestValidateCreate_SwarmRejectsInvalidTermiteURL(t *testing.T) {
	cluster := baseSwarmCluster()
	cluster.Spec.Swarm.Termite = &SwarmTermiteSpec{
		Enabled: true,
		APIURL:  "localhost:11433",
	}

	err := cluster.ValidateCreate()
	if err == nil {
		t.Fatal("expected error for invalid termite API URL")
	}
	if !strings.Contains(err.Error(), "spec.swarm.termite.apiURL") {
		t.Fatalf("expected termite URL validation error, got: %v", err)
	}
}

func TestValidateUpdate_ModeImmutable(t *testing.T) {
	oldCluster := baseCluster()

	newCluster := oldCluster.DeepCopy()
	newCluster.Spec.Mode = ClusterModeSwarm
	newCluster.Spec.Swarm = &SwarmSpec{
		Replicas:     1,
		NodeID:       1,
		MetadataAPI:  APISpec{Port: 8080},
		MetadataRaft: APISpec{Port: 9017},
		StoreAPI:     APISpec{Port: 12380},
		StoreRaft:    APISpec{Port: 9021},
		Health:       APISpec{Port: 4200},
		Termite: &SwarmTermiteSpec{
			Enabled: true,
			APIURL:  "http://0.0.0.0:11433",
		},
	}
	newCluster.Spec.MetadataNodes = MetadataNodesSpec{}
	newCluster.Spec.DataNodes = DataNodesSpec{}
	newCluster.Spec.Storage = StorageSpec{
		StorageClass: "standard",
		SwarmStorage: "1Gi",
	}

	err := newCluster.ValidateUpdate(oldCluster)
	if err == nil {
		t.Fatal("expected error when changing mode from Clustered to Swarm")
	}
	if !strings.Contains(err.Error(), "spec.mode") {
		t.Fatalf("expected immutable mode validation error, got: %v", err)
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

func baseSwarmCluster() *AntflyCluster {
	return &AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-swarm-cluster",
			Namespace: "default",
		},
		Spec: AntflyClusterSpec{
			Mode:  ClusterModeSwarm,
			Image: "antfly:latest",
			Swarm: &SwarmSpec{
				Replicas:     1,
				NodeID:       1,
				Resources:    ResourceSpec{CPU: "500m", Memory: "1Gi"},
				MetadataAPI:  APISpec{Port: 8080},
				MetadataRaft: APISpec{Port: 9017},
				StoreAPI:     APISpec{Port: 12380},
				StoreRaft:    APISpec{Port: 9021},
				Health:       APISpec{Port: 4200},
				Termite: &SwarmTermiteSpec{
					Enabled: true,
					APIURL:  "http://0.0.0.0:11433",
				},
			},
			Storage: StorageSpec{
				StorageClass: "standard",
				SwarmStorage: "1Gi",
			},
			Config: "{}",
		},
	}
}
