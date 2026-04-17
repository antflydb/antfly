package controllers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// T004: Unit test for applyDefaults() setting ServiceMesh.Enabled=false
func TestApplyDefaults_ServiceMeshDefaults(t *testing.T) {
	g := NewWithT(t)

	// Setup scheme
	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	// Create reconciler
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme: s,
	}

	// Test Case 1: Cluster without ServiceMesh field (nil)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
			ServiceMesh: nil, // Explicitly nil
		},
	}

	// Apply defaults
	reconciler.applyDefaults(cluster)

	// Verify ServiceMesh is initialized with default Enabled=false
	g.Expect(cluster.Spec.ServiceMesh).ToNot(BeNil())
	g.Expect(cluster.Spec.ServiceMesh.Enabled).To(BeFalse())

	// Test Case 2: Cluster with ServiceMesh field but Enabled not set
	clusterWithMesh := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-mesh",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
			ServiceMesh: &antflyv1.ServiceMeshSpec{
				Annotations: map[string]string{
					"sidecar.istio.io/inject": "true",
				},
			},
		},
	}

	// Apply defaults
	reconciler.applyDefaults(clusterWithMesh)

	// Verify default Enabled is false (Go zero value)
	g.Expect(clusterWithMesh.Spec.ServiceMesh.Enabled).To(BeFalse())

	// Test Case 3: Cluster with ServiceMesh explicitly enabled
	clusterEnabled := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-enabled",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
			ServiceMesh: &antflyv1.ServiceMeshSpec{
				Enabled: true,
				Annotations: map[string]string{
					"sidecar.istio.io/inject": "true",
				},
			},
		},
	}

	// Apply defaults
	reconciler.applyDefaults(clusterEnabled)

	// Verify Enabled remains true
	g.Expect(clusterEnabled.Spec.ServiceMesh.Enabled).To(BeTrue())
}

// T005: Unit test for applyDefaults() setting PublicAPI.Enabled=false
func TestApplyDefaults_PublicAPIDefaultsFalse(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme: s,
	}

	// Test Case 1: Cluster without PublicAPI field (nil) — should default to enabled=false
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pubapi-default",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
		},
	}

	reconciler.applyDefaults(cluster)

	g.Expect(cluster.Spec.PublicAPI).ToNot(BeNil())
	g.Expect(cluster.Spec.PublicAPI.Enabled).ToNot(BeNil())
	g.Expect(*cluster.Spec.PublicAPI.Enabled).To(BeFalse(), "PublicAPI.Enabled should default to false")
	g.Expect(*cluster.Spec.PublicAPI.ServiceType).To(Equal(corev1.ServiceTypeLoadBalancer))
	g.Expect(cluster.Spec.PublicAPI.Port).To(Equal(int32(80)))

	// Test Case 2: Cluster with PublicAPI but Enabled=nil — should default to false
	clusterPartial := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pubapi-partial",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
			PublicAPI: &antflyv1.PublicAPIConfig{
				Port: 8080,
			},
		},
	}

	reconciler.applyDefaults(clusterPartial)

	g.Expect(clusterPartial.Spec.PublicAPI.Enabled).ToNot(BeNil())
	g.Expect(*clusterPartial.Spec.PublicAPI.Enabled).To(BeFalse(), "PublicAPI.Enabled should default to false when nil")

	// Test Case 3: Cluster with PublicAPI explicitly enabled — should remain true
	enabledTrue := true
	clusterEnabled := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pubapi-enabled",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
			PublicAPI: &antflyv1.PublicAPIConfig{
				Enabled: &enabledTrue,
			},
		},
	}

	reconciler.applyDefaults(clusterEnabled)

	g.Expect(*clusterEnabled.Spec.PublicAPI.Enabled).To(BeTrue(), "Explicitly enabled PublicAPI should remain true")
}

func TestApplyDefaults_SwarmDefaults(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme: s,
	}

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-swarm",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Mode:  antflyv1.ClusterModeSwarm,
			Image: "antfly:latest",
			Swarm: &antflyv1.SwarmSpec{},
			Storage: antflyv1.StorageSpec{
				StorageClass: "standard",
				SwarmStorage: "1Gi",
			},
		},
	}

	reconciler.applyDefaults(cluster)

	g.Expect(cluster.Spec.Swarm).ToNot(BeNil())
	g.Expect(cluster.Spec.Swarm.Replicas).To(Equal(int32(1)))
	g.Expect(cluster.Spec.Swarm.NodeID).To(Equal(int32(1)))
	g.Expect(cluster.Spec.Swarm.MetadataAPI.Port).To(Equal(int32(8080)))
	g.Expect(cluster.Spec.Swarm.MetadataRaft.Port).To(Equal(int32(9017)))
	g.Expect(cluster.Spec.Swarm.StoreAPI.Port).To(Equal(int32(12380)))
	g.Expect(cluster.Spec.Swarm.StoreRaft.Port).To(Equal(int32(9021)))
	g.Expect(cluster.Spec.Swarm.Health.Port).To(Equal(int32(4200)))
	g.Expect(cluster.Spec.Swarm.Termite).ToNot(BeNil())
	g.Expect(cluster.Spec.Swarm.Termite.Enabled).To(BeTrue())
	g.Expect(cluster.Spec.Swarm.Termite.APIURL).To(Equal("http://0.0.0.0:11433"))
}

// T006: Unit test for public API service deletion when disabled
func TestReconcileServices_DeletesPublicAPIWhenDisabled(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())
	err = corev1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	// Create a pre-existing public-api service to simulate the scenario
	existingSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-public-api",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{
				{Port: 80},
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(s).WithObjects(existingSvc).Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	enabled := false
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:     3,
				MetadataAPI:  antflyv1.APISpec{Port: 12377},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
				Health:       antflyv1.APISpec{Port: 4200},
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
				API:      antflyv1.APISpec{Port: 12380},
				Raft:     antflyv1.APISpec{Port: 9021},
				Health:   antflyv1.APISpec{Port: 4200},
			},
			PublicAPI: &antflyv1.PublicAPIConfig{
				Enabled: &enabled,
			},
		},
	}

	// Verify the service exists before reconciliation
	svc := &corev1.Service{}
	err = client.Get(context.Background(), types.NamespacedName{
		Name:      "test-cluster-public-api",
		Namespace: "default",
	}, svc)
	g.Expect(err).NotTo(HaveOccurred(), "Service should exist before reconciliation")

	// Run reconcileServices
	err = reconciler.reconcileServices(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())

	// Verify the public-api service has been deleted
	err = client.Get(context.Background(), types.NamespacedName{
		Name:      "test-cluster-public-api",
		Namespace: "default",
	}, svc)
	g.Expect(err).To(HaveOccurred(), "Service should be deleted")
	g.Expect(errors.IsNotFound(err)).To(BeTrue(), "Error should be NotFound")
}

// T007: Unit test for reconcileServices when no public-api service exists and publicAPI is disabled
func TestReconcileServices_NoErrorWhenPublicAPIDisabledAndNoService(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())
	err = corev1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	client := fake.NewClientBuilder().WithScheme(s).Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	enabled := false
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:     3,
				MetadataAPI:  antflyv1.APISpec{Port: 12377},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
				Health:       antflyv1.APISpec{Port: 4200},
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
				API:      antflyv1.APISpec{Port: 12380},
				Raft:     antflyv1.APISpec{Port: 9021},
				Health:   antflyv1.APISpec{Port: 4200},
			},
			PublicAPI: &antflyv1.PublicAPIConfig{
				Enabled: &enabled,
			},
		},
	}

	// Should not error even when no public-api service exists
	err = reconciler.reconcileServices(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())
}

// Integration tests using envtest
var _ = Describe("AntflyCluster Controller", func() {
	const (
		timeout  = time.Second * 30
		interval = time.Millisecond * 250
	)

	Context("When creating a basic AntflyCluster", func() {
		It("Should create StatefulSets, Services, and ConfigMap", func() {
			clusterName := "test-basic-cluster"
			namespace := "default"

			cluster := &antflyv1.AntflyCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: antflyv1.AntflyClusterSpec{
					Image: "antfly:latest",
					MetadataNodes: antflyv1.MetadataNodesSpec{
						Replicas: 3,
						Resources: antflyv1.ResourceSpec{
							CPU:    "500m",
							Memory: "512Mi",
						},
						MetadataAPI:  antflyv1.APISpec{Port: 12377},
						MetadataRaft: antflyv1.APISpec{Port: 9017},
						Health:       antflyv1.APISpec{Port: 4200},
					},
					DataNodes: antflyv1.DataNodesSpec{
						Replicas: 3,
						Resources: antflyv1.ResourceSpec{
							CPU:    "1000m",
							Memory: "2Gi",
						},
						API:    antflyv1.APISpec{Port: 12380},
						Raft:   antflyv1.APISpec{Port: 9021},
						Health: antflyv1.APISpec{Port: 4200},
					},
					Config: "{}",
					Storage: antflyv1.StorageSpec{
						StorageClass:    "standard",
						MetadataStorage: "1Gi",
						DataStorage:     "10Gi",
					},
				},
			}

			// Create the cluster
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			// Verify metadata StatefulSet is created
			metadataSts := &appsv1.StatefulSet{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      clusterName + "-metadata",
					Namespace: namespace,
				}, metadataSts)
			}, timeout, interval).Should(Succeed())
			Expect(*metadataSts.Spec.Replicas).To(Equal(int32(3)))

			// Verify data StatefulSet is created
			dataSts := &appsv1.StatefulSet{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      clusterName + "-data",
					Namespace: namespace,
				}, dataSts)
			}, timeout, interval).Should(Succeed())
			Expect(*dataSts.Spec.Replicas).To(Equal(int32(3)))

			// Verify ConfigMap is created
			configMap := &corev1.ConfigMap{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      clusterName + "-config",
					Namespace: namespace,
				}, configMap)
			}, timeout, interval).Should(Succeed())
			Expect(configMap.Data).To(HaveKey("config.json"))

			// Verify internal service is created
			internalSvc := &corev1.Service{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      clusterName + "-metadata",
					Namespace: namespace,
				}, internalSvc)
			}, timeout, interval).Should(Succeed())

			// Cleanup
			Expect(k8sClient.Delete(ctx, cluster)).To(Succeed())
		})
	})

	Context("When creating a cluster with service mesh enabled", func() {
		It("Should apply mesh annotations to pod templates", func() {
			clusterName := "mesh-cluster"
			namespace := "default"

			cluster := &antflyv1.AntflyCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: antflyv1.AntflyClusterSpec{
					Image: "antfly:latest",
					MetadataNodes: antflyv1.MetadataNodesSpec{
						Replicas: 3,
						Resources: antflyv1.ResourceSpec{
							CPU:    "500m",
							Memory: "512Mi",
						},
						MetadataAPI:  antflyv1.APISpec{Port: 12377},
						MetadataRaft: antflyv1.APISpec{Port: 9017},
						Health:       antflyv1.APISpec{Port: 4200},
					},
					DataNodes: antflyv1.DataNodesSpec{
						Replicas: 3,
						Resources: antflyv1.ResourceSpec{
							CPU:    "1000m",
							Memory: "2Gi",
						},
						API:    antflyv1.APISpec{Port: 12380},
						Raft:   antflyv1.APISpec{Port: 9021},
						Health: antflyv1.APISpec{Port: 4200},
					},
					Config: "{}",
					Storage: antflyv1.StorageSpec{
						StorageClass:    "standard",
						MetadataStorage: "1Gi",
						DataStorage:     "10Gi",
					},
					ServiceMesh: &antflyv1.ServiceMeshSpec{
						Enabled: true,
						Annotations: map[string]string{
							"sidecar.istio.io/inject": "true",
						},
					},
				},
			}

			// Create the cluster
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			// Verify metadata StatefulSet has mesh annotations
			metadataSts := &appsv1.StatefulSet{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      clusterName + "-metadata",
					Namespace: namespace,
				}, metadataSts)
			}, timeout, interval).Should(Succeed())

			// Check pod template annotations include mesh annotation
			Expect(metadataSts.Spec.Template.Annotations).To(HaveKeyWithValue(
				"sidecar.istio.io/inject", "true",
			))

			// Verify data StatefulSet has mesh annotations
			dataSts := &appsv1.StatefulSet{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      clusterName + "-data",
					Namespace: namespace,
				}, dataSts)
			}, timeout, interval).Should(Succeed())

			Expect(dataSts.Spec.Template.Annotations).To(HaveKeyWithValue(
				"sidecar.istio.io/inject", "true",
			))

			// Cleanup
			Expect(k8sClient.Delete(ctx, cluster)).To(Succeed())
		})
	})
})

// TestApplySchedulingConstraints verifies tolerations, nodeSelector, affinity, and topologySpreadConstraints
func TestApplySchedulingConstraints(t *testing.T) {
	g := NewWithT(t)

	t.Run("applies tolerations", func(t *testing.T) {
		g := NewWithT(t)
		podTemplate := &corev1.PodTemplateSpec{}
		tolerations := []corev1.Toleration{
			{
				Key:      "dedicated",
				Operator: corev1.TolerationOpEqual,
				Value:    "antfly",
				Effect:   corev1.TaintEffectNoSchedule,
			},
		}

		applySchedulingConstraints(podTemplate, tolerations, nil, nil, nil)

		g.Expect(podTemplate.Spec.Tolerations).To(HaveLen(1))
		g.Expect(podTemplate.Spec.Tolerations[0].Key).To(Equal("dedicated"))
		g.Expect(podTemplate.Spec.Tolerations[0].Value).To(Equal("antfly"))
	})

	t.Run("merges nodeSelector", func(t *testing.T) {
		g := NewWithT(t)
		podTemplate := &corev1.PodTemplateSpec{}
		nodeSelector := map[string]string{
			"node-pool": "antfly",
			"disk-type": "ssd",
		}

		applySchedulingConstraints(podTemplate, nil, nodeSelector, nil, nil)

		g.Expect(podTemplate.Spec.NodeSelector).To(HaveLen(2))
		g.Expect(podTemplate.Spec.NodeSelector["node-pool"]).To(Equal("antfly"))
		g.Expect(podTemplate.Spec.NodeSelector["disk-type"]).To(Equal("ssd"))
	})

	t.Run("applies affinity", func(t *testing.T) {
		g := NewWithT(t)
		podTemplate := &corev1.PodTemplateSpec{}
		affinity := &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "topology.kubernetes.io/zone",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"us-east-1a", "us-east-1b"},
								},
							},
						},
					},
				},
			},
			PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
					{
						Weight: 100,
						PodAffinityTerm: corev1.PodAffinityTerm{
							TopologyKey: "kubernetes.io/hostname",
						},
					},
				},
			},
		}

		applySchedulingConstraints(podTemplate, nil, nil, affinity, nil)

		g.Expect(podTemplate.Spec.Affinity).ToNot(BeNil())
		g.Expect(podTemplate.Spec.Affinity.NodeAffinity).ToNot(BeNil())
		g.Expect(podTemplate.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution).ToNot(BeNil())
		g.Expect(podTemplate.Spec.Affinity.PodAntiAffinity).ToNot(BeNil())
	})

	t.Run("applies topologySpreadConstraints", func(t *testing.T) {
		g := NewWithT(t)
		podTemplate := &corev1.PodTemplateSpec{}
		maxSkew := int32(1)
		constraints := []corev1.TopologySpreadConstraint{
			{
				MaxSkew:           maxSkew,
				TopologyKey:       "topology.kubernetes.io/zone",
				WhenUnsatisfiable: corev1.ScheduleAnyway,
			},
		}

		applySchedulingConstraints(podTemplate, nil, nil, nil, constraints)

		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(1))
		g.Expect(podTemplate.Spec.TopologySpreadConstraints[0].TopologyKey).To(Equal("topology.kubernetes.io/zone"))
	})

	t.Run("merges with existing affinity", func(t *testing.T) {
		g = NewWithT(t)
		// Simulate existing affinity (as EKS instance type would set)
		podTemplate := &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{
							{
								Weight: 100,
								Preference: corev1.NodeSelectorTerm{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      "node.kubernetes.io/instance-type",
											Operator: corev1.NodeSelectorOpIn,
											Values:   []string{"m5.large"},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		// User adds required node affinity
		userAffinity := &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "topology.kubernetes.io/zone",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"us-east-1a"},
								},
							},
						},
					},
				},
			},
		}

		applySchedulingConstraints(podTemplate, nil, nil, userAffinity, nil)

		// Both should be preserved
		g.Expect(podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))
		g.Expect(podTemplate.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution).ToNot(BeNil())
	})

	t.Run("combines all fields", func(t *testing.T) {
		g = NewWithT(t)
		podTemplate := &corev1.PodTemplateSpec{}
		tolerations := []corev1.Toleration{
			{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "antfly", Effect: corev1.TaintEffectNoSchedule},
		}
		nodeSelector := map[string]string{"node-pool": "antfly"}
		affinity := &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
					{Weight: 100, PodAffinityTerm: corev1.PodAffinityTerm{TopologyKey: "kubernetes.io/hostname"}},
				},
			},
		}
		maxSkew := int32(1)
		constraints := []corev1.TopologySpreadConstraint{
			{MaxSkew: maxSkew, TopologyKey: "topology.kubernetes.io/zone", WhenUnsatisfiable: corev1.ScheduleAnyway},
		}

		applySchedulingConstraints(podTemplate, tolerations, nodeSelector, affinity, constraints)

		g.Expect(podTemplate.Spec.Tolerations).To(HaveLen(1))
		g.Expect(podTemplate.Spec.NodeSelector).To(HaveLen(1))
		g.Expect(podTemplate.Spec.Affinity).ToNot(BeNil())
		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(1))
	})
}

// TestGenerateCompleteConfig verifies orchestration URLs use 0-based pod indexing
func TestGenerateCompleteConfig(t *testing.T) {
	g := NewWithT(t)

	// Setup scheme
	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	// Create reconciler
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme: s,
	}

	// Create cluster with 3 metadata replicas
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Config: "{}",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
				MetadataAPI: antflyv1.APISpec{
					Port: 12377,
				},
			},
		},
	}

	// Generate configuration
	configJSON, err := reconciler.generateCompleteConfig(cluster)
	g.Expect(err).NotTo(HaveOccurred())

	// Parse the generated config to verify orchestration URLs
	var config map[string]any
	err = json.Unmarshal([]byte(configJSON), &config)
	g.Expect(err).NotTo(HaveOccurred())

	// Verify metadata section exists with orchestration_urls
	metadata, ok := config["metadata"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "metadata section should exist")

	orchestrationURLs, ok := metadata["orchestration_urls"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "orchestration_urls should exist")

	// Verify correct 0-based pod indexing for StatefulSet pods
	// ID "1" should map to metadata-0
	url1, ok := orchestrationURLs["1"].(string)
	g.Expect(ok).To(BeTrue())
	g.Expect(url1).To(ContainSubstring("test-cluster-metadata-0"),
		"ID 1 should map to metadata-0 (0-based indexing)")

	// ID "2" should map to metadata-1
	url2, ok := orchestrationURLs["2"].(string)
	g.Expect(ok).To(BeTrue())
	g.Expect(url2).To(ContainSubstring("test-cluster-metadata-1"),
		"ID 2 should map to metadata-1 (0-based indexing)")

	// ID "3" should map to metadata-2
	url3, ok := orchestrationURLs["3"].(string)
	g.Expect(ok).To(BeTrue())
	g.Expect(url3).To(ContainSubstring("test-cluster-metadata-2"),
		"ID 3 should map to metadata-2 (0-based indexing)")

	// Verify the full URL format
	expectedURL := "http://test-cluster-metadata-0.test-cluster-metadata.default.svc.cluster.local:12377"
	g.Expect(url1).To(Equal(expectedURL))
}

func TestGenerateCompleteConfig_Swarm(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme: s,
	}

	cluster := baseSwarmControllerCluster()
	cluster.Spec.Config = `{
	  "replication_factor": 3,
	  "swarm_mode": false,
	  "storage": {
	    "s3": {
	      "bucket": "test-bucket"
	    }
	  }
	}`

	configJSON, err := reconciler.generateCompleteConfig(cluster)
	g.Expect(err).NotTo(HaveOccurred())

	var config map[string]any
	err = json.Unmarshal([]byte(configJSON), &config)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(config["swarm_mode"]).To(Equal(true))
	g.Expect(config["replication_factor"]).To(Equal(float64(1)))
	g.Expect(config["default_shards_per_table"]).To(Equal(float64(1)))
	g.Expect(config["disable_shard_alloc"]).To(Equal(true))

	storage, ok := config["storage"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	localStorage, ok := storage["local"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	g.Expect(localStorage["base_dir"]).To(Equal("/antflydb"))
	_, hasS3 := storage["s3"]
	g.Expect(hasS3).To(BeTrue(), "expected user-provided S3 storage config to be preserved")

	metadata, ok := config["metadata"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	orchestrationURLs, ok := metadata["orchestration_urls"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	g.Expect(orchestrationURLs["1"]).To(Equal("http://test-swarm-swarm.default.svc.cluster.local:8080"))
}

func TestReconcileServices_SwarmCreatesSwarmAndPublicAPI(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())
	err = corev1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	cluster := baseSwarmControllerCluster()
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	err = reconciler.reconcileServices(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())

	publicSvc := &corev1.Service{}
	err = client.Get(context.Background(), types.NamespacedName{Name: "test-swarm-public-api", Namespace: "default"}, publicSvc)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(publicSvc.Spec.Selector).To(HaveKeyWithValue("app.kubernetes.io/component", "swarm"))
	g.Expect(publicSvc.Spec.Ports).To(HaveLen(1))
	g.Expect(publicSvc.Spec.Ports[0].TargetPort.IntValue()).To(Equal(8080))

	swarmSvc := &corev1.Service{}
	err = client.Get(context.Background(), types.NamespacedName{Name: "test-swarm-swarm", Namespace: "default"}, swarmSvc)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(swarmSvc.Spec.Ports).To(HaveLen(5))

	err = client.Get(context.Background(), types.NamespacedName{Name: "test-swarm-metadata", Namespace: "default"}, &corev1.Service{})
	g.Expect(errors.IsNotFound(err)).To(BeTrue())
	err = client.Get(context.Background(), types.NamespacedName{Name: "test-swarm-data", Namespace: "default"}, &corev1.Service{})
	g.Expect(errors.IsNotFound(err)).To(BeTrue())
}

func TestUpdateStatus_Swarm(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())
	err = appsv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())
	err = corev1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	cluster := baseSwarmControllerCluster()
	swarmSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-swarm-swarm",
			Namespace: "default",
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: 1,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-swarm-swarm-0",
			Namespace: "default",
			Labels:    serviceSelectorLabels("test-swarm", "swarm"),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.10",
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(cluster).
		WithObjects(cluster, swarmSts, pod).
		Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	err = reconciler.updateStatus(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())

	updated := &antflyv1.AntflyCluster{}
	err = client.Get(context.Background(), types.NamespacedName{Name: "test-swarm", Namespace: "default"}, updated)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(updated.Status.Mode).To(Equal(antflyv1.ClusterModeSwarm))
	g.Expect(updated.Status.ReadyReplicas).To(Equal(int32(1)))
	g.Expect(updated.Status.SwarmNodesReady).To(Equal(int32(1)))
	g.Expect(updated.Status.Phase).To(Equal("Running"))
	g.Expect(updated.Status.SwarmStatus).ToNot(BeNil())
	g.Expect(updated.Status.SwarmStatus.Ready).To(BeTrue())
	g.Expect(updated.Status.SwarmStatus.PodName).To(Equal("test-swarm-swarm-0"))
	g.Expect(updated.Status.SwarmStatus.PodIP).To(Equal("10.0.0.10"))
	g.Expect(updated.Status.SwarmStatus.ObservedConfigHash).ToNot(BeEmpty())
}

func TestDetectSidecarInjectionStatus_ScopedToClusterInstance(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())
	err = corev1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	cluster := baseSwarmControllerCluster()
	clusterPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-swarm-swarm-0",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/name":     "antfly-database",
				"app.kubernetes.io/instance": "test-swarm",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "antfly"},
				{Name: "istio-proxy"},
			},
		},
	}
	otherClusterPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-cluster-swarm-0",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/name":     "antfly-database",
				"app.kubernetes.io/instance": "other-cluster",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "antfly"},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster, clusterPod, otherClusterPod).
		Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	podsWithSidecars, totalPods, err := reconciler.detectSidecarInjectionStatus(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(totalPods).To(Equal(int32(1)))
	g.Expect(podsWithSidecars).To(Equal(int32(1)))
}

// TestPodLabels tests that podLabels returns correct labels including instance
func TestPodLabels(t *testing.T) {
	g := NewWithT(t)

	labels := podLabels("my-cluster", "metadata")

	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/name", "antfly-database"))
	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/component", "metadata"))
	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/instance", "my-cluster"))
	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/managed-by", "antfly-operator"))
}

// TestSelectorLabels tests that selectorLabels includes instance but not managed-by
// TestServiceSelectorLabels tests that serviceSelectorLabels includes instance but not managed-by
func TestServiceSelectorLabels(t *testing.T) {
	g := NewWithT(t)

	labels := serviceSelectorLabels("my-cluster", "metadata")

	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/name", "antfly-database"))
	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/component", "metadata"))
	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/instance", "my-cluster"))
	g.Expect(labels).NotTo(HaveKey("app.kubernetes.io/managed-by"))
}

// TestBuildResourceRequirements tests resource conversion including GPU
func TestBuildResourceRequirements(t *testing.T) {
	g := NewWithT(t)
	r := &AntflyClusterReconciler{}

	t.Run("maps GPU to nvidia.com/gpu", func(t *testing.T) {
		g := NewWithT(t)
		reqs := r.buildResourceRequirements(antflyv1.ResourceSpec{
			CPU:    "500m",
			Memory: "1Gi",
			Limits: antflyv1.ResourceLimits{
				CPU:    "2",
				Memory: "4Gi",
				GPU:    "1",
			},
		})

		g.Expect(reqs.Requests[corev1.ResourceCPU]).To(Equal(resource.MustParse("500m")))
		g.Expect(reqs.Requests[corev1.ResourceMemory]).To(Equal(resource.MustParse("1Gi")))
		g.Expect(reqs.Limits[corev1.ResourceCPU]).To(Equal(resource.MustParse("2")))
		g.Expect(reqs.Limits[corev1.ResourceMemory]).To(Equal(resource.MustParse("4Gi")))
		g.Expect(reqs.Limits[corev1.ResourceName("nvidia.com/gpu")]).To(Equal(resource.MustParse("1")))
	})

	t.Run("omits GPU when empty", func(t *testing.T) {
		g := NewWithT(t)
		reqs := r.buildResourceRequirements(antflyv1.ResourceSpec{
			Limits: antflyv1.ResourceLimits{
				CPU:    "1",
				Memory: "2Gi",
			},
		})

		_, hasGPU := reqs.Limits[corev1.ResourceName("nvidia.com/gpu")]
		g.Expect(hasGPU).To(BeFalse())
	})

	_ = g // satisfy compiler
}

// TestBuildPVCRetentionPolicy tests PVC retention policy mapping
func TestBuildPVCRetentionPolicy(t *testing.T) {
	g := NewWithT(t)

	// nil policy
	result := buildPVCRetentionPolicy(nil)
	g.Expect(result).To(BeNil())

	// Retain/Retain (default)
	result = buildPVCRetentionPolicy(&antflyv1.PVCRetentionPolicy{
		WhenDeleted: antflyv1.PVCRetentionRetain,
		WhenScaled:  antflyv1.PVCRetentionRetain,
	})
	g.Expect(result).NotTo(BeNil())
	g.Expect(result.WhenDeleted).To(Equal(appsv1.RetainPersistentVolumeClaimRetentionPolicyType))
	g.Expect(result.WhenScaled).To(Equal(appsv1.RetainPersistentVolumeClaimRetentionPolicyType))

	// Delete/Retain
	result = buildPVCRetentionPolicy(&antflyv1.PVCRetentionPolicy{
		WhenDeleted: antflyv1.PVCRetentionDelete,
		WhenScaled:  antflyv1.PVCRetentionRetain,
	})
	g.Expect(result).NotTo(BeNil())
	g.Expect(result.WhenDeleted).To(Equal(appsv1.DeletePersistentVolumeClaimRetentionPolicyType))
	g.Expect(result.WhenScaled).To(Equal(appsv1.RetainPersistentVolumeClaimRetentionPolicyType))

	// Delete/Delete
	result = buildPVCRetentionPolicy(&antflyv1.PVCRetentionPolicy{
		WhenDeleted: antflyv1.PVCRetentionDelete,
		WhenScaled:  antflyv1.PVCRetentionDelete,
	})
	g.Expect(result).NotTo(BeNil())
	g.Expect(result.WhenDeleted).To(Equal(appsv1.DeletePersistentVolumeClaimRetentionPolicyType))
	g.Expect(result.WhenScaled).To(Equal(appsv1.DeletePersistentVolumeClaimRetentionPolicyType))

	// Empty strings default to Retain
	result = buildPVCRetentionPolicy(&antflyv1.PVCRetentionPolicy{})
	g.Expect(result).NotTo(BeNil())
	g.Expect(result.WhenDeleted).To(Equal(appsv1.RetainPersistentVolumeClaimRetentionPolicyType))
	g.Expect(result.WhenScaled).To(Equal(appsv1.RetainPersistentVolumeClaimRetentionPolicyType))
}

// TestApplyDefaultZoneTopologySpread tests zone topology spread behavior
func TestApplyDefaultZoneTopologySpread(t *testing.T) {
	t.Run("adds constraint to new StatefulSet", func(t *testing.T) {
		g := NewWithT(t)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{}, // CreationTimestamp is zero (new)
		}
		podTemplate := &corev1.PodTemplateSpec{}

		applyDefaultZoneTopologySpread(sts, podTemplate, "data", "my-cluster", nil, false)

		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(1))
		g.Expect(podTemplate.Spec.TopologySpreadConstraints[0].TopologyKey).To(Equal("topology.kubernetes.io/zone"))
		g.Expect(podTemplate.Spec.TopologySpreadConstraints[0].WhenUnsatisfiable).To(Equal(corev1.ScheduleAnyway))
		g.Expect(sts.Annotations[annotationDefaultTopologySpread]).To(Equal("true"))
	})

	t.Run("skips when user has explicit constraints", func(t *testing.T) {
		g := NewWithT(t)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{annotationDefaultTopologySpread: "true"},
			},
		}
		podTemplate := &corev1.PodTemplateSpec{}
		userConstraints := []corev1.TopologySpreadConstraint{
			{TopologyKey: "kubernetes.io/hostname", MaxSkew: 1},
		}

		applyDefaultZoneTopologySpread(sts, podTemplate, "data", "my-cluster", userConstraints, false)

		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(0))
		g.Expect(sts.Annotations).NotTo(HaveKey(annotationDefaultTopologySpread))
	})

	t.Run("skips for GKE Autopilot", func(t *testing.T) {
		g := NewWithT(t)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{},
		}
		podTemplate := &corev1.PodTemplateSpec{}

		applyDefaultZoneTopologySpread(sts, podTemplate, "data", "my-cluster", nil, true)

		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(0))
	})

	t.Run("re-adds if annotation present on existing StatefulSet", func(t *testing.T) {
		g := NewWithT(t)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				CreationTimestamp: metav1.Now(), // existing
				Annotations:       map[string]string{annotationDefaultTopologySpread: "true"},
			},
		}
		podTemplate := &corev1.PodTemplateSpec{}

		applyDefaultZoneTopologySpread(sts, podTemplate, "metadata", "my-cluster", nil, false)

		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(1))
	})

	t.Run("skips existing StatefulSet without annotation", func(t *testing.T) {
		g := NewWithT(t)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				CreationTimestamp: metav1.Now(), // existing
			},
		}
		podTemplate := &corev1.PodTemplateSpec{}

		applyDefaultZoneTopologySpread(sts, podTemplate, "data", "my-cluster", nil, false)

		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(0))
	})
}

// TestContainsVolumeAffinityMessage tests the helper function
func TestContainsVolumeAffinityMessage(t *testing.T) {
	g := NewWithT(t)

	g.Expect(containsVolumeAffinityMessage("0/3 nodes are available: 1 volume node affinity conflict, 2 node(s) didn't match")).To(BeTrue())
	g.Expect(containsVolumeAffinityMessage("no matching nodes")).To(BeFalse())
	g.Expect(containsVolumeAffinityMessage("")).To(BeFalse())
}

func baseSwarmControllerCluster() *antflyv1.AntflyCluster {
	enabled := true
	serviceType := corev1.ServiceTypeClusterIP

	return &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-swarm",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Mode:  antflyv1.ClusterModeSwarm,
			Image: "antfly:latest",
			Swarm: &antflyv1.SwarmSpec{
				Replicas:     1,
				NodeID:       1,
				Resources:    antflyv1.ResourceSpec{CPU: "500m", Memory: "1Gi"},
				MetadataAPI:  antflyv1.APISpec{Port: 8080},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
				StoreAPI:     antflyv1.APISpec{Port: 12380},
				StoreRaft:    antflyv1.APISpec{Port: 9021},
				Health:       antflyv1.APISpec{Port: 4200},
				Termite: &antflyv1.SwarmTermiteSpec{
					Enabled: true,
					APIURL:  "http://0.0.0.0:11433",
				},
			},
			Storage: antflyv1.StorageSpec{
				StorageClass: "standard",
				SwarmStorage: "1Gi",
			},
			PublicAPI: &antflyv1.PublicAPIConfig{
				Enabled:     &enabled,
				ServiceType: &serviceType,
				Port:        80,
			},
			Config: "{}",
		},
	}
}
