package controllers

import (
	"context"
	"encoding/json"
	"testing"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func TestStandaloneHACatalogScopeRequiresExplicitOptIn(t *testing.T) {
	for _, image := range []string{"antfly:v0.2.1", "antfly:v0.2.3"} {
		for _, role := range []antflyv1.HARuntimeRole{antflyv1.HARuntimeRolePrimary, antflyv1.HARuntimeRoleStandby} {
			t.Run(image+"/"+string(role), func(t *testing.T) {
				g := NewWithT(t)
				scheme := runtime.NewScheme()
				g.Expect(antflyv1.AddToScheme(scheme)).To(Succeed())
				g.Expect(appsv1.AddToScheme(scheme)).To(Succeed())
				g.Expect(corev1.AddToScheme(scheme)).To(Succeed())
				cluster := baseStandaloneControllerCluster()
				cluster.Spec.Image = image
				// Decode a persisted legacy identity with absent table/shard IDs.
				g.Expect(json.Unmarshal([]byte(`{"mode":"HotStandby","identity":{"clusterID":100,"timelineID":1,"epoch":1}}`), &cluster.Spec.HighAvailability)).To(Succeed())
				cluster.Spec.HighAvailability.Mode = antflyv1.HAModeHotStandby
				cluster.Spec.HighAvailability.Runtime = &antflyv1.HARuntimeSpec{Role: role, NodeID: "node-a"}
				client := newHAControllerTestClient(t, scheme, cluster)
				reconciler := &AntflyClusterReconciler{Client: client, Scheme: scheme}
				for _, mode := range []string{"", "false", "true"} {
					cluster.Annotations = map[string]string{}
					if mode != "" {
						cluster.Annotations[haCatalogReplicationAnnotation] = mode
					}
					g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
					sts := &appsv1.StatefulSet{}
					g.Expect(client.Get(context.Background(), types.NamespacedName{Name: cluster.Name + "-standalone", Namespace: cluster.Namespace}, sts)).To(Succeed())
					args := sts.Spec.Template.Spec.Containers[0].Args[0]
					if mode == "true" {
						g.Expect(args).To(ContainSubstring("--ha-shard-id 0"))
						g.Expect(args).To(ContainSubstring("--ha-table-id 0"))
					} else {
						g.Expect(args).NotTo(ContainSubstring("--ha-shard-id"))
						g.Expect(args).NotTo(ContainSubstring("--ha-table-id"))
					}
				}
				// Nonzero table-scoped identities keep their arguments even
				// without the new annotation, on either runtime version.
				cluster.Annotations = nil
				cluster.Spec.HighAvailability.Identity.ShardID = 10
				cluster.Spec.HighAvailability.Identity.TableID = 20
				g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
				sts := &appsv1.StatefulSet{}
				g.Expect(client.Get(context.Background(), types.NamespacedName{Name: cluster.Name + "-standalone", Namespace: cluster.Namespace}, sts)).To(Succeed())
				g.Expect(sts.Spec.Template.Spec.Containers[0].Args[0]).To(ContainSubstring("--ha-shard-id 10"))
				g.Expect(sts.Spec.Template.Spec.Containers[0].Args[0]).To(ContainSubstring("--ha-table-id 20"))
			})
		}
	}
}
