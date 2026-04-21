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

package controllers

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	antflyaiv1alpha1 "github.com/antflydb/antfly/pkg/termite-operator/api/v1alpha1"
)

var _ = Describe("TermitePool Controller", func() {
	const (
		poolName      = "test-pool"
		poolNamespace = "default"
	)

	Context("When creating a TermitePool", func() {
		It("Should create the associated Service, ConfigMap, and StatefulSet", func() {
			ctx := context.Background()

			pool := &antflyaiv1alpha1.TermitePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      poolName,
					Namespace: poolNamespace,
				},
				Spec: antflyaiv1alpha1.TermitePoolSpec{
					WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
					Models: antflyaiv1alpha1.ModelConfig{
						Preload: []antflyaiv1alpha1.ModelSpec{
							{
								Name:     "bge-small-en-v1.5",
								Priority: antflyaiv1alpha1.ModelPriorityHigh,
							},
						},
						LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
					},
					Replicas: antflyaiv1alpha1.ReplicaConfig{
						Min: 1,
						Max: 5,
					},
					Hardware: antflyaiv1alpha1.HardwareConfig{
						Accelerator: "tpu-v5-lite-podslice",
						Topology:    "2x2",
					},
				},
			}

			Expect(k8sClient.Create(ctx, pool)).Should(Succeed())

			// Verify the TermitePool was created
			poolLookupKey := types.NamespacedName{Name: poolName, Namespace: poolNamespace}
			createdPool := &antflyaiv1alpha1.TermitePool{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, poolLookupKey, createdPool)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// Verify the Service was created
			serviceLookupKey := types.NamespacedName{Name: poolName, Namespace: poolNamespace}
			createdService := &corev1.Service{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, serviceLookupKey, createdService)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(createdService.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			Expect(createdService.Spec.Ports).To(HaveLen(1))
			Expect(createdService.Spec.Ports[0].Name).To(Equal("http"))
			Expect(createdService.Spec.Ports[0].Port).To(Equal(int32(TermiteAPIPort)))

			// Verify the ConfigMap was created
			configMapLookupKey := types.NamespacedName{Name: poolName + "-config", Namespace: poolNamespace}
			createdConfigMap := &corev1.ConfigMap{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, configMapLookupKey, createdConfigMap)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(createdConfigMap.Data["TERMITE_MODELS"]).To(Equal("bge-small-en-v1.5"))
			Expect(createdConfigMap.Data["TERMITE_POOL"]).To(Equal(poolName))
			Expect(createdConfigMap.Data["TERMITE_WORKLOAD_TYPE"]).To(Equal("general"))
			Expect(createdConfigMap.Data["TERMITE_LOADING_STRATEGY"]).To(Equal("eager"))

			// Verify the StatefulSet was created
			stsLookupKey := types.NamespacedName{Name: poolName, Namespace: poolNamespace}
			createdSts := &appsv1.StatefulSet{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, stsLookupKey, createdSts)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(*createdSts.Spec.Replicas).To(Equal(int32(1)))
			Expect(createdSts.Spec.ServiceName).To(Equal(poolName))
			Expect(createdSts.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(createdSts.Spec.Template.Spec.Containers[0].Name).To(Equal("termite"))

			// Verify TPU node selector
			Expect(createdSts.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue(
				"cloud.google.com/gke-tpu-accelerator", "tpu-v5-lite-podslice"))
			Expect(createdSts.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue(
				"cloud.google.com/gke-tpu-topology", "2x2"))

			// Verify probes are configured
			container := createdSts.Spec.Template.Spec.Containers[0]
			Expect(container.StartupProbe).NotTo(BeNil())
			Expect(container.ReadinessProbe).NotTo(BeNil())
			Expect(container.LivenessProbe).NotTo(BeNil())

			// Cleanup
			Expect(k8sClient.Delete(ctx, pool)).Should(Succeed())
		})
	})

	Context("When updating a TermitePool", func() {
		It("Should update the StatefulSet replicas", func() {
			ctx := context.Background()

			pool := &antflyaiv1alpha1.TermitePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-test-pool",
					Namespace: poolNamespace,
				},
				Spec: antflyaiv1alpha1.TermitePoolSpec{
					WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
					Models: antflyaiv1alpha1.ModelConfig{
						Preload: []antflyaiv1alpha1.ModelSpec{
							{Name: "test-model"},
						},
						LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
					},
					Replicas: antflyaiv1alpha1.ReplicaConfig{
						Min: 1,
						Max: 5,
					},
					Hardware: antflyaiv1alpha1.HardwareConfig{},
				},
			}

			Expect(k8sClient.Create(ctx, pool)).Should(Succeed())

			// Wait for StatefulSet to be created
			stsLookupKey := types.NamespacedName{Name: "update-test-pool", Namespace: poolNamespace}
			createdSts := &appsv1.StatefulSet{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, stsLookupKey, createdSts)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(*createdSts.Spec.Replicas).To(Equal(int32(1)))

			// Update the pool
			poolLookupKey := types.NamespacedName{Name: "update-test-pool", Namespace: poolNamespace}
			Eventually(func() error {
				if err := k8sClient.Get(ctx, poolLookupKey, pool); err != nil {
					return err
				}
				pool.Spec.Replicas.Min = 3
				return k8sClient.Update(ctx, pool)
			}, timeout, interval).Should(Succeed())

			// Verify StatefulSet was updated
			Eventually(func() int32 {
				if err := k8sClient.Get(ctx, stsLookupKey, createdSts); err != nil {
					return 0
				}
				return *createdSts.Spec.Replicas
			}, timeout, interval).Should(Equal(int32(3)))

			// Cleanup
			Expect(k8sClient.Delete(ctx, pool)).Should(Succeed())
		})

		It("Should roll the StatefulSet when pod template labels change", func() {
			ctx := context.Background()

			pool := &antflyaiv1alpha1.TermitePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "workload-update-pool",
					Namespace: poolNamespace,
				},
				Spec: antflyaiv1alpha1.TermitePoolSpec{
					WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
					Models: antflyaiv1alpha1.ModelConfig{
						Preload:         []antflyaiv1alpha1.ModelSpec{{Name: "test-model"}},
						LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
					},
					Replicas: antflyaiv1alpha1.ReplicaConfig{
						Min: 1,
						Max: 3,
					},
					Hardware: antflyaiv1alpha1.HardwareConfig{},
				},
			}

			Expect(k8sClient.Create(ctx, pool)).Should(Succeed())

			stsLookupKey := types.NamespacedName{Name: "workload-update-pool", Namespace: poolNamespace}
			createdSts := &appsv1.StatefulSet{}
			Eventually(func() string {
				if err := k8sClient.Get(ctx, stsLookupKey, createdSts); err != nil {
					return ""
				}
				return createdSts.Spec.Template.Annotations["termite.antfly.io/template-hash"]
			}, timeout, interval).ShouldNot(BeEmpty())

			initialHash := createdSts.Spec.Template.Annotations["termite.antfly.io/template-hash"]
			Expect(createdSts.Spec.Template.Labels["antfly.io/workload-type"]).To(Equal("general"))

			poolLookupKey := types.NamespacedName{Name: "workload-update-pool", Namespace: poolNamespace}
			Eventually(func() error {
				if err := k8sClient.Get(ctx, poolLookupKey, pool); err != nil {
					return err
				}
				pool.Spec.WorkloadType = antflyaiv1alpha1.WorkloadTypeReadHeavy
				return k8sClient.Update(ctx, pool)
			}, timeout, interval).Should(Succeed())

			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, stsLookupKey, createdSts)).To(Succeed())
				g.Expect(createdSts.Spec.Template.Labels["antfly.io/workload-type"]).To(Equal("read-heavy"))
				g.Expect(createdSts.Spec.Template.Annotations["termite.antfly.io/template-hash"]).NotTo(Equal(initialHash))
			}, timeout, interval).Should(Succeed())

			Expect(k8sClient.Delete(ctx, pool)).Should(Succeed())
		})

		It("Should delete the PodDisruptionBudget when disabled", func() {
			ctx := context.Background()
			maxUnavailable := int32(1)

			pool := &antflyaiv1alpha1.TermitePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pdb-toggle-pool",
					Namespace: poolNamespace,
				},
				Spec: antflyaiv1alpha1.TermitePoolSpec{
					WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
					Models: antflyaiv1alpha1.ModelConfig{
						Preload:         []antflyaiv1alpha1.ModelSpec{{Name: "test-model"}},
						LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
					},
					Replicas: antflyaiv1alpha1.ReplicaConfig{
						Min: 2,
						Max: 3,
					},
					Hardware: antflyaiv1alpha1.HardwareConfig{},
					Availability: &antflyaiv1alpha1.AvailabilityConfig{
						PodDisruptionBudget: &antflyaiv1alpha1.PDBConfig{
							Enabled:        true,
							MaxUnavailable: &maxUnavailable,
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, pool)).Should(Succeed())

			pdbLookupKey := types.NamespacedName{Name: "pdb-toggle-pool-pdb", Namespace: poolNamespace}
			createdPDB := &policyv1.PodDisruptionBudget{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, pdbLookupKey, createdPDB)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			poolLookupKey := types.NamespacedName{Name: "pdb-toggle-pool", Namespace: poolNamespace}
			Eventually(func() error {
				if err := k8sClient.Get(ctx, poolLookupKey, pool); err != nil {
					return err
				}
				pool.Spec.Availability.PodDisruptionBudget.Enabled = false
				return k8sClient.Update(ctx, pool)
			}, timeout, interval).Should(Succeed())

			Eventually(func() bool {
				err := k8sClient.Get(ctx, pdbLookupKey, createdPDB)
				return apierrors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())

			Expect(k8sClient.Delete(ctx, pool)).Should(Succeed())
		})
	})

	Context("When creating a TermitePool with model variants", func() {
		It("Should include variant in the ConfigMap", func() {
			ctx := context.Background()

			pool := &antflyaiv1alpha1.TermitePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "variant-test-pool",
					Namespace: poolNamespace,
				},
				Spec: antflyaiv1alpha1.TermitePoolSpec{
					WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
					Models: antflyaiv1alpha1.ModelConfig{
						Preload: []antflyaiv1alpha1.ModelSpec{
							{
								Name:    "bge-small-en-v1.5",
								Variant: "quantized",
							},
						},
						LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
					},
					Replicas: antflyaiv1alpha1.ReplicaConfig{
						Min: 1,
						Max: 3,
					},
					Hardware: antflyaiv1alpha1.HardwareConfig{},
				},
			}

			Expect(k8sClient.Create(ctx, pool)).Should(Succeed())

			// Verify the ConfigMap includes the variant
			configMapLookupKey := types.NamespacedName{Name: "variant-test-pool-config", Namespace: poolNamespace}
			createdConfigMap := &corev1.ConfigMap{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, configMapLookupKey, createdConfigMap)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(createdConfigMap.Data["TERMITE_MODELS"]).To(Equal("bge-small-en-v1.5:quantized"))

			// Cleanup
			Expect(k8sClient.Delete(ctx, pool)).Should(Succeed())
		})
	})

	Context("When creating a TermitePool with custom image", func() {
		It("Should use the custom image in the StatefulSet", func() {
			ctx := context.Background()

			pool := &antflyaiv1alpha1.TermitePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "custom-image-pool",
					Namespace: poolNamespace,
				},
				Spec: antflyaiv1alpha1.TermitePoolSpec{
					WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
					Image:        "my-registry/termite:v1.0.0",
					Models: antflyaiv1alpha1.ModelConfig{
						Preload: []antflyaiv1alpha1.ModelSpec{
							{Name: "test-model"},
						},
						LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
					},
					Replicas: antflyaiv1alpha1.ReplicaConfig{
						Min: 1,
						Max: 3,
					},
					Hardware: antflyaiv1alpha1.HardwareConfig{},
				},
			}

			Expect(k8sClient.Create(ctx, pool)).Should(Succeed())

			// Verify the StatefulSet uses the custom image
			stsLookupKey := types.NamespacedName{Name: "custom-image-pool", Namespace: poolNamespace}
			createdSts := &appsv1.StatefulSet{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, stsLookupKey, createdSts)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(createdSts.Spec.Template.Spec.Containers[0].Image).To(Equal("my-registry/termite:v1.0.0"))

			// Cleanup
			Expect(k8sClient.Delete(ctx, pool)).Should(Succeed())
		})
	})
})
