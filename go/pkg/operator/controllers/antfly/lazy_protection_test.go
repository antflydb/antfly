package controllers

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type catalogTransport func(*http.Request) (*http.Response, error)

func (f catalogTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestBackupScheduleWaitsForTablesAndRecovers(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{antflyv1.AddToScheme, batchv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cluster := &antflyv1.AntflyCluster{ObjectMeta: metav1.ObjectMeta{Name: "antfly", Namespace: "default"}}
	backup := &antflyv1.AntflyBackup{ObjectMeta: metav1.ObjectMeta{Name: "daily", Namespace: "default", UID: "backup-uid", Generation: 1}, Spec: antflyv1.AntflyBackupSpec{
		ClusterRef: antflyv1.ClusterReference{Name: "antfly"}, Schedule: "0 2 * * *", Destination: antflyv1.BackupDestination{Location: "s3://backups/daily", Connection: "archive"},
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(backup).WithObjects(cluster, backup).Build()
	body, code := "[]", 200
	calls := 0
	r := &AntflyBackupReconciler{Client: c, Scheme: scheme, HTTPClient: &http.Client{Transport: catalogTransport(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.URL.Path != "/db/v1/tables" {
			t.Fatalf("unexpected path %s", req.URL.Path)
		}
		return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}
	key := types.NamespacedName{Name: "daily", Namespace: "default"}
	for _, step := range []struct {
		name, body string
		code       int
		suspend    bool
		reason     string
	}{
		{"empty", "[]", 200, true, "WaitingForTables"},
		{"first table", `[{"name":"docs"}]`, 200, false, "CronJobCreated"},
		{"last table removed", "[]", 200, true, "WaitingForTables"},
		{"unavailable", "unavailable", 503, true, "CatalogUnavailable"},
		{"malformed", "null", 200, true, "CatalogUnavailable"},
		{"invalid table", "[{}]", 200, true, "CatalogUnavailable"},
		{"unauthorized", "unauthorized", 401, true, "CatalogUnavailable"},
		{"recovers", `[{"name":"docs"}]`, 200, false, "CronJobCreated"},
	} {
		t.Run(step.name, func(t *testing.T) {
			body, code = step.body, step.code
			result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			if err != nil {
				t.Fatal(err)
			}
			if result.RequeueAfter != 30*time.Second {
				t.Fatalf("missing catalog recheck: %+v", result)
			}
			var cron batchv1.CronJob
			if err := c.Get(context.Background(), types.NamespacedName{Name: "daily-backup", Namespace: "default"}, &cron); err != nil {
				t.Fatal(err)
			}
			if cron.Spec.Suspend == nil || *cron.Spec.Suspend != step.suspend {
				t.Fatalf("unexpected suspend: %v", cron.Spec.Suspend)
			}
			if err := c.Get(context.Background(), key, backup); err != nil {
				t.Fatal(err)
			}
			condition := meta.FindStatusCondition(backup.Status.Conditions, antflyv1.TypeBackupScheduleReady)
			if condition == nil || condition.Reason != step.reason {
				t.Fatalf("unexpected condition: %+v", condition)
			}
			if backup.Spec.Suspend || backup.Status.LastSuccessfulBackup != nil || backup.Status.LastFailedBackup != nil {
				t.Fatal("catalog gate changed intent or fabricated backup history")
			}
		})
	}
	// Explicit suspension wins even when tables exist and does not need a probe.
	backup.Spec.Suspend = true
	if err := c.Update(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	before := calls
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if calls != before {
		t.Fatal("suspended schedule queried catalog")
	}
}

func TestLazyHAWaitsBeforeFirstTableOnly(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.ActivationPolicy = "OnFirstTable"
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{Name: "standby-a"}}
	cluster.Status.HAStatus = &antflyv1.HAStatus{PrimaryLSN: 9, PrimaryAdminReachable: true, CatalogObserved: true, WaitingForTables: true}
	r := &AntflyClusterReconciler{}
	r.updateHAStatusAndConditions(cluster)
	if len(cluster.Status.HAStatus.PlannedActions) != 0 || cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("empty instance planned HA work")
	}
	condition := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAvailable)
	if condition == nil || condition.Reason != "WaitingForTables" || condition.Status != metav1.ConditionFalse {
		t.Fatalf("unexpected condition: %+v", condition)
	}
	for _, change := range []struct {
		name  string
		apply func(*antflyv1.HAStatus)
	}{
		{"table observed", func(s *antflyv1.HAStatus) { s.WaitingForTables = false; s.ActivationStarted = true }},
		{"table deleted after activation", func(s *antflyv1.HAStatus) { s.ActivationStarted = true }},
		{"existing slot on upgrade", func(s *antflyv1.HAStatus) { s.Standbys = []antflyv1.HAStandbyStatus{{Name: "standby-a", Active: true}} }},
	} {
		t.Run(change.name, func(t *testing.T) {
			c := cluster.DeepCopy()
			change.apply(c.Status.HAStatus)
			if planHA(c).WaitingForTables {
				t.Fatal("existing HA was deferred")
			}
		})
	}
	cluster.Status.HAStatus.CatalogObserved = false
	if !planHA(cluster).WaitingForTables {
		t.Fatal("old runtime without catalog evidence started lazy HA")
	}
	cluster.Spec.HighAvailability.ActivationPolicy = "Eager"
	if planHA(cluster).WaitingForTables {
		t.Fatal("eager HA unexpectedly deferred")
	}
}

func TestLazyHAActivationLatchesAcrossCatalogDeletionAndObservationFailure(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.ActivationPolicy = "OnFirstTable"
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary.test"}
	empty, available := true, true
	r := &AntflyClusterReconciler{HTTPClient: &http.Client{Transport: catalogTransport(func(req *http.Request) (*http.Response, error) {
		body, code := pathStylePrimaryStatus, 200
		if !available {
			body, code = "unavailable", 503
		} else {
			field := `"waiting_for_tables":false,`
			if empty {
				field = `"waiting_for_tables":true,`
			}
			body = strings.Replace(body, `"role":`, field+`"role":`, 1)
		}
		return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{"Content-Type": []string{"application/json"}}}, nil
	})}}
	if err := r.observeHAPrimaryAdminStatus(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if !planHA(cluster).WaitingForTables {
		t.Fatal("empty catalog did not defer activation")
	}
	empty = false
	if err := r.observeHAPrimaryAdminStatus(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if !cluster.Status.HAStatus.ActivationStarted || planHA(cluster).WaitingForTables {
		t.Fatal("first table did not activate HA")
	}
	// Simulate a controller restart using only the persisted API status.
	cluster = cluster.DeepCopy()
	empty = true
	if err := r.observeHAPrimaryAdminStatus(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if planHA(cluster).WaitingForTables {
		t.Fatal("last-table deletion reset activation")
	}
	available = false
	if err := r.observeHAPrimaryAdminStatus(context.Background(), cluster); err == nil {
		t.Fatal("expected observation error")
	}
	if planHA(cluster).WaitingForTables {
		t.Fatal("observation failure reset established HA")
	}
}
