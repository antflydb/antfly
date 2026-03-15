package main

import (
	"context"
	"flag"
	"os"
	"strings"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"go.uber.org/zap/zapcore"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
	"github.com/antflydb/antfly/pkg/operator/bootstrap"
	"github.com/antflydb/antfly/pkg/operator/controllers"
	webhookv1 "github.com/antflydb/antfly/pkg/operator/internal/webhook/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(antflyv1.AddToScheme(scheme))
	utilruntime.Must(metricsv1beta1.AddToScheme(scheme))
	utilruntime.Must(apiextensionsv1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var skipCRDInstall bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&skipCRDInstall, "skip-crd-install", false,
		"Skip automatic CRD installation (use if CRDs managed externally)")
	opts := zap.Options{
		Development: false,
		TimeEncoder: zapcore.RFC3339TimeEncoder,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgrOpts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "antfly-operator.antfly.io",
	}

	// Configure webhook server when webhooks are enabled
	if webhooksEnabled() {
		mgrOpts.WebhookServer = webhook.NewServer(webhook.Options{
			Port: 9443,
		})
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOpts)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Create Kubernetes clientset for metrics
	k8sClient, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "unable to create Kubernetes client")
		os.Exit(1)
	}

	// Create AutoScaler
	autoScaler := controllers.NewAutoScaler(mgr.GetClient(), k8sClient, mgr.GetClient())

	if err = (&controllers.AntflyClusterReconciler{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		AutoScaler: autoScaler,
		Recorder:   mgr.GetEventRecorder("antfly-operator"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AntflyCluster")
		os.Exit(1)
	}

	if err = (&controllers.AntflyBackupReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorder("antfly-backup"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AntflyBackup")
		os.Exit(1)
	}

	if err = (&controllers.AntflyRestoreReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorder("antfly-restore"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AntflyRestore")
		os.Exit(1)
	}

	// Setup webhooks
	if webhooksEnabled() {
		if err := webhookv1.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhooks")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	// Bootstrap CRDs before starting the manager
	// Use a direct client (not cached) since the manager cache isn't started yet
	if !skipCRDInstall {
		setupLog.Info("Ensuring CRDs are installed")
		ctx := context.Background()
		directClient, err := client.New(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme()})
		if err != nil {
			setupLog.Error(err, "Failed to create direct client for CRD bootstrap")
			os.Exit(1)
		}
		if err := bootstrap.EnsureCRDs(ctx, directClient); err != nil {
			setupLog.Error(err, "Failed to ensure CRDs")
			os.Exit(1)
		}
		if err := bootstrap.WaitForCRDs(ctx, directClient); err != nil {
			setupLog.Error(err, "Failed waiting for CRDs")
			os.Exit(1)
		}
		setupLog.Info("CRDs ready")
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// webhooksEnabled returns true only when ENABLE_WEBHOOKS is explicitly set to
// "true" or "1" (case-insensitive). Defaults to false so that local development
// with `make run` works without TLS certs. In-cluster deployments should set
// ENABLE_WEBHOOKS=true in the manager Deployment.
func webhooksEnabled() bool {
	v := strings.ToLower(os.Getenv("ENABLE_WEBHOOKS"))
	return v == "true" || v == "1"
}
