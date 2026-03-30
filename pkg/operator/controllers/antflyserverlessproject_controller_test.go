package controllers

import (
	"context"
	"testing"

	antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestAntflyServerlessProjectDefaultsAndValidation(t *testing.T) {
	g := NewWithT(t)

	project := &antflyv1.AntflyServerlessProject{
		Spec: antflyv1.AntflyServerlessProjectSpec{
			ObjectStore: antflyv1.ServerlessObjectStoreSpec{
				ArtifactsURI: "s3://bucket/artifacts",
				ManifestsURI: "s3://bucket/manifests",
				WALURI:       "s3://bucket/wal",
				CatalogURI:   "s3://bucket/catalog",
				ProgressURI:  "s3://bucket/progress",
			},
			Images: antflyv1.ServerlessImagesSpec{
				ZigImage: "ghcr.io/antflydb/antfly:zig",
			},
		},
	}

	project.Default()
	g.Expect(project.Spec.Query.Replicas).To(Equal(int32(1)))
	g.Expect(project.Spec.Maintenance.Replicas).To(Equal(int32(1)))
	g.Expect(project.Spec.Query.Port).To(Equal(int32(8080)))
	g.Expect(project.Spec.Proxy.ServiceType).To(Equal(corev1.ServiceTypeClusterIP))
	g.Expect(project.ValidateAntflyServerlessProject()).To(Succeed())

	project.Spec.Query.Replicas = -1
	g.Expect(project.ValidateAntflyServerlessProject()).ToNot(Succeed())
}

func TestAntflyServerlessProjectReconcileCreatesDeploymentsAndServices(t *testing.T) {
	g := NewWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(scheme)).To(Succeed())
	g.Expect(appsv1.AddToScheme(scheme)).To(Succeed())
	g.Expect(autoscalingv2.AddToScheme(scheme)).To(Succeed())
	g.Expect(corev1.AddToScheme(scheme)).To(Succeed())
	g.Expect(policyv1.AddToScheme(scheme)).To(Succeed())

	project := &antflyv1.AntflyServerlessProject{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyServerlessProjectSpec{
			ObjectStore: antflyv1.ServerlessObjectStoreSpec{
				ArtifactsURI: "s3://bucket/artifacts",
				ManifestsURI: "s3://bucket/manifests",
				WALURI:       "s3://bucket/wal",
				CatalogURI:   "s3://bucket/catalog",
				ProgressURI:  "s3://bucket/progress",
			},
			Images: antflyv1.ServerlessImagesSpec{
				ZigImage:   "ghcr.io/antflydb/antfly:zig",
				ProxyImage: "proxy:latest",
			},
			Proxy: antflyv1.ServerlessProxySpec{
				Enabled: true,
				AutoScaling: antflyv1.ServerlessAutoScalingSpec{
					Enabled:                        true,
					MinReplicas:                    1,
					MaxReplicas:                    3,
					TargetCPUUtilizationPercentage: 70,
				},
				Auth: antflyv1.ServerlessProxyAuthSpec{
					BearerTokenSecretRef: "proxy-auth",
				},
				RouteConfigMapRef: "docs-proxy-routes",
				Routes: []antflyv1.ServerlessProxyRouteSpec{
					{
						Tenant:             "t1",
						Namespace:          "docs",
						PreferredBackend:   "serverless",
						AllowStateful:      true,
						AllowServerless:    true,
						StatefulURL:        "http://stateful.default.svc:8080",
						ServerlessQueryURL: "http://docs-serverless-query.default.svc:8080",
						ServerlessAPIURL:   "http://docs-serverless-api.default.svc:8080",
					},
				},
			},
			Query: antflyv1.ServerlessQueryRuntimeSpec{
				AutoScaling: antflyv1.ServerlessAutoScalingSpec{
					Enabled:                        true,
					MinReplicas:                    1,
					MaxReplicas:                    4,
					TargetCPUUtilizationPercentage: 75,
				},
			},
			API: antflyv1.ServerlessAPIRuntimeSpec{
				AutoScaling: antflyv1.ServerlessAutoScalingSpec{
					Enabled:                        true,
					MinReplicas:                    1,
					MaxReplicas:                    3,
					TargetCPUUtilizationPercentage: 70,
				},
			},
			Embeddings: antflyv1.ServerlessEmbeddingSpec{
				IndexesSecretRef: "embedder-config",
			},
			ServiceAccountName: "serverless-sa",
		},
	}
	otherProject := &antflyv1.AntflyServerlessProject{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "analytics",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyServerlessProjectSpec{
			ObjectStore: antflyv1.ServerlessObjectStoreSpec{
				ArtifactsURI: "s3://bucket2/artifacts",
				ManifestsURI: "s3://bucket2/manifests",
				WALURI:       "s3://bucket2/wal",
				CatalogURI:   "s3://bucket2/catalog",
				ProgressURI:  "s3://bucket2/progress",
			},
			Images: antflyv1.ServerlessImagesSpec{
				ZigImage: "ghcr.io/antflydb/antfly:zig",
			},
			Query: antflyv1.ServerlessQueryRuntimeSpec{
				Port: 9090,
			},
			Proxy: antflyv1.ServerlessProxySpec{
				Routes: []antflyv1.ServerlessProxyRouteSpec{
					{
						Tenant:           "t2",
						Namespace:        "analytics",
						PreferredBackend: "serverless",
						AllowServerless:  true,
					},
				},
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "embedder-config",
			Namespace: "default",
		},
	}
	proxySecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "proxy-auth",
			Namespace: "default",
		},
	}
	routeConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-proxy-routes",
			Namespace: "default",
		},
		Data: map[string]string{
			"routes.json": `[{"tenant":"t3","table":"search","serving_namespace":"search-serving","preferred_backend":"stateful","allow_stateful":true,"stateful_url":"http://stateful-search.default.svc:8080"}]`,
		},
	}
	sharedRouteConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shared-proxy-routes",
			Namespace: "default",
			Labels: map[string]string{
				serverlessProxyRouteSourceLabel: "true",
			},
		},
		Data: map[string]string{
			"routes.json": `[{"tenant":"t4","table":"shared","serving_namespace":"shared-serving","preferred_backend":"serverless","allow_serverless":true,"serverless_query_url":"http://shared-serverless-query.default.svc:8080","serverless_api_url":"http://shared-serverless-api.default.svc:8080"}]`,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&antflyv1.AntflyServerlessProject{}).
		WithObjects(project, otherProject, secret, proxySecret, routeConfigMap, sharedRouteConfigMap).
		Build()

	reconciler := &AntflyServerlessProjectReconciler{
		Client: client,
		Scheme: scheme,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: project.Name, Namespace: project.Namespace},
	})
	g.Expect(err).NotTo(HaveOccurred())

	for _, name := range []string{
		"docs-serverless-api",
		"docs-serverless-query",
		"docs-serverless-maintenance",
		"docs-serverless-proxy",
	} {
		deployment := &appsv1.Deployment{}
		g.Expect(client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, deployment)).To(Succeed())
		if name == "docs-serverless-proxy" {
			g.Expect(deployment.Spec.Template.Spec.Volumes).To(ContainElement(HaveField("Name", Equal("proxy-config"))))
			g.Expect(deployment.Spec.Template.Spec.Volumes).To(ContainElement(HaveField("Name", Equal("proxy-auth"))))
			g.Expect(deployment.Spec.Template.Spec.Containers).To(HaveLen(1))
			g.Expect(deployment.Spec.Template.Spec.Containers[0].EnvFrom).To(HaveLen(1))
			g.Expect(deployment.Spec.Template.Spec.Containers[0].VolumeMounts).To(ContainElement(HaveField("MountPath", Equal("/etc/antfly-proxy"))))
			g.Expect(deployment.Spec.Template.Spec.Containers[0].VolumeMounts).To(ContainElement(HaveField("MountPath", Equal("/etc/antfly-proxy-secret"))))
		} else {
			g.Expect(deployment.Spec.Template.Spec.Containers).To(HaveLen(1))
			g.Expect(deployment.Spec.Template.Spec.Containers[0].Image).To(Equal("ghcr.io/antflydb/antfly:zig"))
			g.Expect(deployment.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{"antfly"}))
		}
	}

	for _, name := range []string{
		"docs-serverless-api",
		"docs-serverless-query",
		"docs-serverless-proxy",
	} {
		service := &corev1.Service{}
		g.Expect(client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, service)).To(Succeed())
	}

	configMap := &corev1.ConfigMap{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "docs-serverless-config", Namespace: "default"}, configMap)).To(Succeed())
	proxyConfigMap := &corev1.ConfigMap{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "docs-serverless-proxy-config", Namespace: "default"}, proxyConfigMap)).To(Succeed())
	g.Expect(proxyConfigMap.Data["ANTFLY_PROXY_REQUIRE_AUTH"]).To(Equal("true"))
	g.Expect(proxyConfigMap.Data["ANTFLY_PROXY_ROUTES_JSON"]).To(ContainSubstring(`"tenant":"t1"`))
	g.Expect(proxyConfigMap.Data["ANTFLY_PROXY_ROUTES_JSON"]).To(ContainSubstring(`"tenant":"t2"`))
	g.Expect(proxyConfigMap.Data["ANTFLY_PROXY_ROUTES_JSON"]).To(ContainSubstring(`"tenant":"t3"`))
	g.Expect(proxyConfigMap.Data["ANTFLY_PROXY_ROUTES_JSON"]).To(ContainSubstring(`"tenant":"t4"`))
	g.Expect(proxyConfigMap.Data["ANTFLY_PROXY_ROUTES_JSON"]).To(ContainSubstring(`"serverless_query_url":"http://analytics-serverless-query.default.svc:9090"`))
	g.Expect(proxyConfigMap.Data["ANTFLY_PROXY_ROUTES_JSON"]).To(ContainSubstring(`"serverless_api_url":"http://analytics-serverless-api.default.svc:9090"`))
	g.Expect(proxyConfigMap.Data["routes.json"]).To(ContainSubstring(`"tenant":"t1"`))
	g.Expect(proxyConfigMap.Data["ANTFLY_PROXY_ROUTES_FILE"]).To(Equal("/etc/antfly-proxy/routes.json"))
	g.Expect(proxyConfigMap.Data["ANTFLY_PROXY_BEARER_TOKENS_FILE"]).To(Equal("/etc/antfly-proxy-secret/bearer_tokens.json"))

	for _, name := range []string{
		"docs-serverless-api-pdb",
		"docs-serverless-query-pdb",
		"docs-serverless-maintenance-pdb",
		"docs-serverless-proxy-pdb",
	} {
		pdb := &policyv1.PodDisruptionBudget{}
		g.Expect(client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, pdb)).To(Succeed())
	}

	for _, name := range []string{
		"docs-serverless-api-hpa",
		"docs-serverless-query-hpa",
		"docs-serverless-proxy-hpa",
	} {
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		g.Expect(client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, hpa)).To(Succeed())
	}

	updated := &antflyv1.AntflyServerlessProject{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: project.Name, Namespace: project.Namespace}, updated)).To(Succeed())
	g.Expect(updated.Status.Validated).To(BeTrue())
	g.Expect(updated.Status.ConfigMapName).To(Equal("docs-serverless-config"))
	g.Expect(updated.Status.ProxyConfigMapName).To(Equal("docs-serverless-proxy-config"))
	g.Expect(updated.Status.APIServiceName).To(Equal("docs-serverless-api"))
	g.Expect(updated.Status.QueryServiceName).To(Equal("docs-serverless-query"))
	g.Expect(updated.Status.ProxyServiceName).To(Equal("docs-serverless-proxy"))
	g.Expect(updated.Status.Phase).To(Equal(antflyv1.ServerlessProjectPhaseReconciling))
	g.Expect(updated.Status.Conditions).To(ContainElement(HaveField("Type", Equal(antflyv1.TypeServerlessSecretsReady))))
}

func TestAntflyServerlessProjectReconcileRejectsInvalidConfig(t *testing.T) {
	g := NewWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(scheme)).To(Succeed())
	g.Expect(appsv1.AddToScheme(scheme)).To(Succeed())
	g.Expect(autoscalingv2.AddToScheme(scheme)).To(Succeed())
	g.Expect(corev1.AddToScheme(scheme)).To(Succeed())
	g.Expect(policyv1.AddToScheme(scheme)).To(Succeed())

	project := &antflyv1.AntflyServerlessProject{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "broken",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyServerlessProjectSpec{},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&antflyv1.AntflyServerlessProject{}).
		WithObjects(project).
		Build()

	reconciler := &AntflyServerlessProjectReconciler{
		Client: client,
		Scheme: scheme,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: project.Name, Namespace: project.Namespace},
	})
	g.Expect(err).NotTo(HaveOccurred())

	updated := &antflyv1.AntflyServerlessProject{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: project.Name, Namespace: project.Namespace}, updated)).To(Succeed())
	g.Expect(updated.Status.Validated).To(BeFalse())
	g.Expect(updated.Status.Phase).To(Equal(antflyv1.ServerlessProjectPhaseInvalidConfig))
}
