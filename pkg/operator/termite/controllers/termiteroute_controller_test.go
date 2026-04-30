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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	antflyaiv1alpha1 "github.com/antflydb/antfly/pkg/operator/termite/api/v1alpha1"
)

var _ = Describe("TermiteRoute Controller", func() {
	const (
		routeNamespace = "default"
	)

	Context("When creating a TermiteRoute with valid pool references", func() {
		It("Should mark the route as active", func() {
			ctx := context.Background()

			// First create a TermitePool that the route will reference
			pool := &antflyaiv1alpha1.TermitePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "route-test-pool",
					Namespace: routeNamespace,
				},
				Spec: antflyaiv1alpha1.TermitePoolSpec{
					WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
					Models: antflyaiv1alpha1.ModelConfig{
						Preload: []antflyaiv1alpha1.ModelSpec{
							{Name: "bge-small-en-v1.5"},
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

			// Now create a route that references the pool
			route := &antflyaiv1alpha1.TermiteRoute{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "valid-route",
					Namespace: routeNamespace,
				},
				Spec: antflyaiv1alpha1.TermiteRouteSpec{
					Priority: 100,
					Match: antflyaiv1alpha1.RouteMatch{
						Operations: []antflyaiv1alpha1.OperationType{
							antflyaiv1alpha1.OperationEmbed,
						},
					},
					Route: []antflyaiv1alpha1.RouteDestination{
						{
							Pool:   "route-test-pool",
							Weight: 100,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, route)).Should(Succeed())

			// Verify the route becomes active
			routeLookupKey := types.NamespacedName{Name: "valid-route", Namespace: routeNamespace}
			createdRoute := &antflyaiv1alpha1.TermiteRoute{}
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, routeLookupKey, createdRoute); err != nil {
					return false
				}
				return createdRoute.Status.Active
			}, timeout, interval).Should(BeTrue())

			// Cleanup
			Expect(k8sClient.Delete(ctx, route)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, pool)).Should(Succeed())
		})
	})

	Context("When creating a TermiteRoute with invalid pool references", func() {
		It("Should mark the route as inactive", func() {
			ctx := context.Background()

			route := &antflyaiv1alpha1.TermiteRoute{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "invalid-route",
					Namespace: routeNamespace,
				},
				Spec: antflyaiv1alpha1.TermiteRouteSpec{
					Priority: 100,
					Match: antflyaiv1alpha1.RouteMatch{
						Operations: []antflyaiv1alpha1.OperationType{
							antflyaiv1alpha1.OperationEmbed,
						},
					},
					Route: []antflyaiv1alpha1.RouteDestination{
						{
							Pool:   "nonexistent-pool",
							Weight: 100,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, route)).Should(Succeed())

			// Verify the route is not active
			routeLookupKey := types.NamespacedName{Name: "invalid-route", Namespace: routeNamespace}
			createdRoute := &antflyaiv1alpha1.TermiteRoute{}
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, routeLookupKey, createdRoute); err != nil {
					return false
				}
				// Route should exist but not be active
				return !createdRoute.Status.Active
			}, timeout, interval).Should(BeTrue())

			// Cleanup
			Expect(k8sClient.Delete(ctx, route)).Should(Succeed())
		})
	})

	Context("When creating a TermiteRoute with multiple destinations", func() {
		It("Should validate all pool references", func() {
			ctx := context.Background()

			// Create two pools
			pool1 := &antflyaiv1alpha1.TermitePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "multi-dest-pool-1",
					Namespace: routeNamespace,
				},
				Spec: antflyaiv1alpha1.TermitePoolSpec{
					WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
					Models: antflyaiv1alpha1.ModelConfig{
						Preload:         []antflyaiv1alpha1.ModelSpec{{Name: "model-1"}},
						LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
					},
					Replicas: antflyaiv1alpha1.ReplicaConfig{Min: 1, Max: 3},
					Hardware: antflyaiv1alpha1.HardwareConfig{},
				},
			}
			Expect(k8sClient.Create(ctx, pool1)).Should(Succeed())

			pool2 := &antflyaiv1alpha1.TermitePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "multi-dest-pool-2",
					Namespace: routeNamespace,
				},
				Spec: antflyaiv1alpha1.TermitePoolSpec{
					WorkloadType: antflyaiv1alpha1.WorkloadTypeBurst,
					Models: antflyaiv1alpha1.ModelConfig{
						Preload:         []antflyaiv1alpha1.ModelSpec{{Name: "model-1"}},
						LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
					},
					Replicas: antflyaiv1alpha1.ReplicaConfig{Min: 1, Max: 10},
					Hardware: antflyaiv1alpha1.HardwareConfig{},
				},
			}
			Expect(k8sClient.Create(ctx, pool2)).Should(Succeed())

			// Create route with weighted traffic split
			route := &antflyaiv1alpha1.TermiteRoute{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "multi-dest-route",
					Namespace: routeNamespace,
				},
				Spec: antflyaiv1alpha1.TermiteRouteSpec{
					Priority: 100,
					Match: antflyaiv1alpha1.RouteMatch{
						Operations: []antflyaiv1alpha1.OperationType{
							antflyaiv1alpha1.OperationEmbed,
							antflyaiv1alpha1.OperationRerank,
						},
					},
					Route: []antflyaiv1alpha1.RouteDestination{
						{
							Pool:   "multi-dest-pool-1",
							Weight: 80,
						},
						{
							Pool:   "multi-dest-pool-2",
							Weight: 20,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, route)).Should(Succeed())

			// Verify the route becomes active
			routeLookupKey := types.NamespacedName{Name: "multi-dest-route", Namespace: routeNamespace}
			createdRoute := &antflyaiv1alpha1.TermiteRoute{}
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, routeLookupKey, createdRoute); err != nil {
					return false
				}
				return createdRoute.Status.Active
			}, timeout, interval).Should(BeTrue())

			// Cleanup
			Expect(k8sClient.Delete(ctx, route)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, pool1)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, pool2)).Should(Succeed())
		})
	})

	Context("When creating a TermiteRoute with model matching", func() {
		It("Should accept routes with model patterns", func() {
			ctx := context.Background()

			// Create a pool first
			pool := &antflyaiv1alpha1.TermitePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "model-match-pool",
					Namespace: routeNamespace,
				},
				Spec: antflyaiv1alpha1.TermitePoolSpec{
					WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
					Models: antflyaiv1alpha1.ModelConfig{
						Preload:         []antflyaiv1alpha1.ModelSpec{{Name: "bge-small-en-v1.5"}},
						LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
					},
					Replicas: antflyaiv1alpha1.ReplicaConfig{Min: 1, Max: 3},
					Hardware: antflyaiv1alpha1.HardwareConfig{},
				},
			}
			Expect(k8sClient.Create(ctx, pool)).Should(Succeed())

			// Create route with model pattern matching
			route := &antflyaiv1alpha1.TermiteRoute{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "model-match-route",
					Namespace: routeNamespace,
				},
				Spec: antflyaiv1alpha1.TermiteRouteSpec{
					Priority: 200,
					Match: antflyaiv1alpha1.RouteMatch{
						Models: []string{"bge-*", "e5-*"},
					},
					Route: []antflyaiv1alpha1.RouteDestination{
						{
							Pool:   "model-match-pool",
							Weight: 100,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, route)).Should(Succeed())

			// Verify the route becomes active
			routeLookupKey := types.NamespacedName{Name: "model-match-route", Namespace: routeNamespace}
			createdRoute := &antflyaiv1alpha1.TermiteRoute{}
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, routeLookupKey, createdRoute); err != nil {
					return false
				}
				return createdRoute.Status.Active
			}, timeout, interval).Should(BeTrue())

			// Verify spec was preserved
			Expect(createdRoute.Spec.Priority).To(Equal(int32(200)))
			Expect(createdRoute.Spec.Match.Models).To(ContainElements("bge-*", "e5-*"))

			// Cleanup
			Expect(k8sClient.Delete(ctx, route)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, pool)).Should(Succeed())
		})
	})

	Context("When creating a TermiteRoute with fallback configuration", func() {
		It("Should accept routes with fallback settings", func() {
			ctx := context.Background()

			// Create pools
			primaryPool := &antflyaiv1alpha1.TermitePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fallback-primary-pool",
					Namespace: routeNamespace,
				},
				Spec: antflyaiv1alpha1.TermitePoolSpec{
					WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
					Models: antflyaiv1alpha1.ModelConfig{
						Preload:         []antflyaiv1alpha1.ModelSpec{{Name: "model-1"}},
						LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
					},
					Replicas: antflyaiv1alpha1.ReplicaConfig{Min: 1, Max: 3},
					Hardware: antflyaiv1alpha1.HardwareConfig{},
				},
			}
			Expect(k8sClient.Create(ctx, primaryPool)).Should(Succeed())

			fallbackPool := &antflyaiv1alpha1.TermitePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fallback-backup-pool",
					Namespace: routeNamespace,
				},
				Spec: antflyaiv1alpha1.TermitePoolSpec{
					WorkloadType: antflyaiv1alpha1.WorkloadTypeBurst,
					Models: antflyaiv1alpha1.ModelConfig{
						Preload:         []antflyaiv1alpha1.ModelSpec{{Name: "model-1"}},
						LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
					},
					Replicas: antflyaiv1alpha1.ReplicaConfig{Min: 0, Max: 10},
					Hardware: antflyaiv1alpha1.HardwareConfig{},
				},
			}
			Expect(k8sClient.Create(ctx, fallbackPool)).Should(Succeed())

			// Create route with fallback
			route := &antflyaiv1alpha1.TermiteRoute{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fallback-route",
					Namespace: routeNamespace,
				},
				Spec: antflyaiv1alpha1.TermiteRouteSpec{
					Priority: 100,
					Match: antflyaiv1alpha1.RouteMatch{
						Operations: []antflyaiv1alpha1.OperationType{antflyaiv1alpha1.OperationEmbed},
					},
					Route: []antflyaiv1alpha1.RouteDestination{
						{
							Pool:   "fallback-primary-pool",
							Weight: 100,
						},
					},
					Fallback: &antflyaiv1alpha1.RouteFallback{
						Action:       antflyaiv1alpha1.FallbackActionRedirect,
						RedirectPool: "fallback-backup-pool",
					},
				},
			}
			Expect(k8sClient.Create(ctx, route)).Should(Succeed())

			// Verify the route becomes active
			routeLookupKey := types.NamespacedName{Name: "fallback-route", Namespace: routeNamespace}
			createdRoute := &antflyaiv1alpha1.TermiteRoute{}
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, routeLookupKey, createdRoute); err != nil {
					return false
				}
				return createdRoute.Status.Active
			}, timeout, interval).Should(BeTrue())

			// Verify fallback config was preserved
			Expect(createdRoute.Spec.Fallback).NotTo(BeNil())
			Expect(createdRoute.Spec.Fallback.Action).To(Equal(antflyaiv1alpha1.FallbackActionRedirect))
			Expect(createdRoute.Spec.Fallback.RedirectPool).To(Equal("fallback-backup-pool"))

			// Cleanup
			Expect(k8sClient.Delete(ctx, route)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, primaryPool)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, fallbackPool)).Should(Succeed())
		})
	})
})
