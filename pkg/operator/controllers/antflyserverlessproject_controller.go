package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
)

type AntflyServerlessProjectReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

//+kubebuilder:rbac:groups=antfly.io,resources=antflyserverlessprojects,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=antfly.io,resources=antflyserverlessprojects/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=antfly.io,resources=antflyserverlessprojects/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=configmaps;secrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

func (r *AntflyServerlessProjectReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	project := &antflyv1.AntflyServerlessProject{}
	if err := r.Get(ctx, req.NamespacedName, project); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get AntflyServerlessProject")
		return ctrl.Result{}, err
	}

	project.Default()

	validateErr := project.ValidateAntflyServerlessProject()
	validated := validateErr == nil
	message := ""
	if validateErr != nil {
		message = validateErr.Error()
	}
	project.Status.ObservedGeneration = project.Generation
	project.Status.Validated = validated
	project.Status.ConfigMapName = serverlessConfigMapName(project.Name)
	project.Status.ProxyConfigMapName = ""
	project.Status.APIServiceName = serverlessServiceName(project.Name, serverlessComponentAPI)
	project.Status.QueryServiceName = serverlessServiceName(project.Name, serverlessComponentQuery)
	project.Status.ProxyServiceName = ""
	project.Status.APIReadyReplicas = 0
	project.Status.QueryReadyReplicas = 0
	project.Status.MaintenanceReadyReplicas = 0
	project.Status.ProxyReadyReplicas = 0

	condition := metav1.Condition{
		Type:               antflyv1.TypeServerlessProjectReady,
		ObservedGeneration: project.Generation,
		LastTransitionTime: metav1.Now(),
	}

	if !validated {
		project.Status.Phase = antflyv1.ServerlessProjectPhaseInvalidConfig
		condition.Status = metav1.ConditionFalse
		condition.Reason = antflyv1.ReasonServerlessProjectInvalid
		condition.Message = message
		apiMeta.SetStatusCondition(&project.Status.Conditions, condition)
		if err := r.Status().Update(ctx, project); err != nil {
			log.Error(err, "Failed to update AntflyServerlessProject status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	secretsReady, secretsMessage, err := r.checkSecretsReady(ctx, project)
	if err != nil {
		return ctrl.Result{}, err
	}
	setServerlessSecretsCondition(project, secretsReady, secretsMessage)

	if err := r.reconcileConfigMap(ctx, project); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileDeployment(ctx, project, serverlessComponentAPI); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileDeployment(ctx, project, serverlessComponentQuery); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileDeployment(ctx, project, serverlessComponentMaintenance); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePDB(ctx, project, serverlessComponentAPI); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePDB(ctx, project, serverlessComponentQuery); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePDB(ctx, project, serverlessComponentMaintenance); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileHPA(ctx, project, serverlessComponentAPI); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileHPA(ctx, project, serverlessComponentQuery); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileService(ctx, project, serverlessComponentAPI); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileService(ctx, project, serverlessComponentQuery); err != nil {
		return ctrl.Result{}, err
	}

	if project.Spec.Proxy.Enabled {
		project.Status.ProxyConfigMapName = serverlessProxyConfigMapName(project.Name)
		project.Status.ProxyServiceName = serverlessServiceName(project.Name, serverlessComponentProxy)
		if err := r.reconcileProxyConfigMap(ctx, project); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileDeployment(ctx, project, serverlessComponentProxy); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcilePDB(ctx, project, serverlessComponentProxy); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileHPA(ctx, project, serverlessComponentProxy); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileService(ctx, project, serverlessComponentProxy); err != nil {
			return ctrl.Result{}, err
		}
	} else {
		if err := r.deleteProxyResources(ctx, project); err != nil {
			return ctrl.Result{}, err
		}
	}

	apiReady, err := r.deploymentReadyReplicas(ctx, project.Namespace, serverlessDeploymentName(project.Name, serverlessComponentAPI))
	if err != nil {
		return ctrl.Result{}, err
	}
	queryReady, err := r.deploymentReadyReplicas(ctx, project.Namespace, serverlessDeploymentName(project.Name, serverlessComponentQuery))
	if err != nil {
		return ctrl.Result{}, err
	}
	maintenanceReady, err := r.deploymentReadyReplicas(ctx, project.Namespace, serverlessDeploymentName(project.Name, serverlessComponentMaintenance))
	if err != nil {
		return ctrl.Result{}, err
	}
	project.Status.APIReadyReplicas = apiReady
	project.Status.QueryReadyReplicas = queryReady
	project.Status.MaintenanceReadyReplicas = maintenanceReady

	var proxyReady int32
	if project.Spec.Proxy.Enabled {
		proxyReady, err = r.deploymentReadyReplicas(ctx, project.Namespace, serverlessDeploymentName(project.Name, serverlessComponentProxy))
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	project.Status.ProxyReadyReplicas = proxyReady

	apiDesired := desiredReplicas(project.Spec.API.Replicas)
	queryDesired := desiredReplicas(project.Spec.Query.Replicas)
	maintenanceDesired := desiredReplicas(project.Spec.Maintenance.Replicas)
	proxyDesired := int32(0)
	if project.Spec.Proxy.Enabled {
		proxyDesired = desiredReplicas(project.Spec.Proxy.Replicas)
	}

	if apiReady >= apiDesired && queryReady >= queryDesired && maintenanceReady >= maintenanceDesired && proxyReady >= proxyDesired {
		project.Status.Phase = antflyv1.ServerlessProjectPhaseReady
		condition.Status = metav1.ConditionTrue
		condition.Reason = antflyv1.ReasonServerlessProjectReady
		condition.Message = "Serverless api, query, maintenance, and proxy workloads are ready"
	} else {
		project.Status.Phase = antflyv1.ServerlessProjectPhaseReconciling
		condition.Status = metav1.ConditionFalse
		condition.Reason = antflyv1.ReasonServerlessProjectReconciling
		condition.Message = "Serverless workloads are being reconciled"
	}
	apiMeta.SetStatusCondition(&project.Status.Conditions, condition)

	if err := r.Status().Update(ctx, project); err != nil {
		log.Error(err, "Failed to update AntflyServerlessProject status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *AntflyServerlessProjectReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&antflyv1.AntflyServerlessProject{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Complete(r)
}

type serverlessComponent string

const (
	serverlessComponentAPI          serverlessComponent = "api"
	serverlessComponentQuery        serverlessComponent = "query"
	serverlessComponentMaintenance  serverlessComponent = "maintenance"
	serverlessComponentProxy        serverlessComponent = "proxy"
	serverlessProxyRouteSourceLabel                     = "antfly.io/serverless-proxy-route-source"
)

func (r *AntflyServerlessProjectReconciler) reconcileDeployment(ctx context.Context, project *antflyv1.AntflyServerlessProject, component serverlessComponent) error {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serverlessDeploymentName(project.Name, component),
			Namespace: project.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		if err := controllerutil.SetControllerReference(project, deployment, r.Scheme); err != nil {
			return err
		}

		labels := serverlessPodLabels(project.Name, component)
		replicas := componentReplicas(project, component)
		port := componentPort(project, component)

		deployment.Labels = labels
		deployment.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: serverlessSelectorLabels(project.Name, component),
		}
		deployment.Spec.Replicas = &replicas
		deployment.Spec.Template.ObjectMeta.Labels = labels
		deployment.Spec.Template.Spec.ServiceAccountName = project.Spec.ServiceAccountName
		deployment.Spec.Template.Spec.Volumes = componentVolumes(project, component)
		deployment.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:            string(component),
				Image:           componentImage(project, component),
				ImagePullPolicy: imagePullPolicy(project.Spec.Images.ImagePullPolicy),
				Command:         componentCommand(component),
				Args:            componentArgs(component),
				Ports: []corev1.ContainerPort{
					{
						Name:          "http",
						ContainerPort: port,
					},
				},
				Env:          componentEnv(project, component),
				EnvFrom:      componentEnvFrom(project, component),
				VolumeMounts: componentVolumeMounts(project, component),
			},
		}
		return nil
	})

	return err
}

func (r *AntflyServerlessProjectReconciler) reconcileProxyConfigMap(ctx context.Context, project *antflyv1.AntflyServerlessProject) error {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serverlessProxyConfigMapName(project.Name),
			Namespace: project.Namespace,
		},
	}

	routesJSON, err := r.serverlessProxyRoutesJSON(ctx, project)
	if err != nil {
		return err
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		if err := controllerutil.SetControllerReference(project, configMap, r.Scheme); err != nil {
			return err
		}
		configMap.Labels = serverlessPodLabels(project.Name, serverlessComponentProxy)
		configMap.Data = map[string]string{
			"routes.json":                           routesJSON,
			"ANTFLY_PROXY_ROUTES_JSON":              routesJSON,
			"ANTFLY_PROXY_ROUTES_FILE":              "/etc/antfly-proxy/routes.json",
			"ANTFLY_PROXY_AUTH_BEARER_TOKEN_SECRET": project.Spec.Proxy.Auth.BearerTokenSecretRef,
			"ANTFLY_PROXY_BEARER_TOKENS_FILE":       "/etc/antfly-proxy-secret/bearer_tokens.json",
			"ANTFLY_PROXY_REQUIRE_AUTH":             strconv.FormatBool(project.Spec.Proxy.Auth.BearerTokenSecretRef != ""),
			"ANTFLY_PROXY_PUBLIC_ADDR":              fmt.Sprintf(":%d", project.Spec.Proxy.Port),
			"ANTFLY_PROXY_TENANT_HEADER":            "X-Antfly-Tenant",
			"ANTFLY_PROXY_TABLE_HEADER":             "X-Antfly-Table",
			"ANTFLY_PROXY_NAMESPACE_HEADER":         "X-Antfly-Namespace",
			"ANTFLY_PROXY_OPERATION_HEADER":         "X-Antfly-Operation",
			"ANTFLY_PROXY_VIEW_HEADER":              "X-Antfly-View",
		}
		return nil
	})

	return err
}

func (r *AntflyServerlessProjectReconciler) reconcileConfigMap(ctx context.Context, project *antflyv1.AntflyServerlessProject) error {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serverlessConfigMapName(project.Name),
			Namespace: project.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		if err := controllerutil.SetControllerReference(project, configMap, r.Scheme); err != nil {
			return err
		}
		configMap.Labels = serverlessPodLabels(project.Name, serverlessComponentQuery)
		configMap.Data = map[string]string{
			"ANTFLY_SERVERLESS_ARTIFACTS_URI":                 project.Spec.ObjectStore.ArtifactsURI,
			"ANTFLY_SERVERLESS_MANIFESTS_URI":                 project.Spec.ObjectStore.ManifestsURI,
			"ANTFLY_SERVERLESS_WAL_URI":                       project.Spec.ObjectStore.WALURI,
			"ANTFLY_SERVERLESS_CATALOG_URI":                   project.Spec.ObjectStore.CatalogURI,
			"ANTFLY_SERVERLESS_PROGRESS_URI":                  project.Spec.ObjectStore.ProgressURI,
			"ANTFLY_SERVERLESS_QUERY_CACHE_MAX_BYTES":         strconv.FormatInt(project.Spec.Query.CacheMaxBytes, 10),
			"ANTFLY_SERVERLESS_QUERY_CACHE_PAYLOAD_MAX_BYTES": strconv.FormatInt(project.Spec.Query.CachePayloadMaxBytes, 10),
			"ANTFLY_SERVERLESS_TICK_INTERVAL_MS":              strconv.FormatInt(int64(project.Spec.Maintenance.TickIntervalMS), 10),
			"ANTFLY_SERVERLESS_QUERY_PORT":                    strconv.FormatInt(int64(project.Spec.Query.Port), 10),
			"ANTFLY_SERVERLESS_PROXY_PORT":                    strconv.FormatInt(int64(project.Spec.Proxy.Port), 10),
			"ANTFLY_SERVERLESS_SERVICE_ACCOUNT_NAME":          project.Spec.ServiceAccountName,
			"ANTFLY_SERVERLESS_EMBEDDING_INDEXES_SECRET_REF":  project.Spec.Embeddings.IndexesSecretRef,
		}
		return nil
	})

	return err
}

func (r *AntflyServerlessProjectReconciler) reconcileService(ctx context.Context, project *antflyv1.AntflyServerlessProject, component serverlessComponent) error {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serverlessServiceName(project.Name, component),
			Namespace: project.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		if err := controllerutil.SetControllerReference(project, service, r.Scheme); err != nil {
			return err
		}

		service.Labels = serverlessSelectorLabels(project.Name, component)
		service.Spec.Selector = serverlessSelectorLabels(project.Name, component)
		service.Spec.Ports = []corev1.ServicePort{
			{
				Name:       "http",
				Port:       componentPort(project, component),
				TargetPort: intstr.FromInt32(componentPort(project, component)),
				Protocol:   corev1.ProtocolTCP,
			},
		}
		if component == serverlessComponentProxy {
			service.Spec.Type = project.Spec.Proxy.ServiceType
		} else {
			service.Spec.Type = corev1.ServiceTypeClusterIP
		}
		return nil
	})

	return err
}

func (r *AntflyServerlessProjectReconciler) reconcilePDB(ctx context.Context, project *antflyv1.AntflyServerlessProject, component serverlessComponent) error {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serverlessPDBName(project.Name, component),
			Namespace: project.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		if err := controllerutil.SetControllerReference(project, pdb, r.Scheme); err != nil {
			return err
		}
		pdb.Labels = serverlessSelectorLabels(project.Name, component)
		pdb.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: serverlessSelectorLabels(project.Name, component),
		}
		minAvailable := intstr.FromInt32(1)
		pdb.Spec.MinAvailable = &minAvailable
		return nil
	})

	return err
}

func (r *AntflyServerlessProjectReconciler) reconcileHPA(ctx context.Context, project *antflyv1.AntflyServerlessProject, component serverlessComponent) error {
	autoScaling := componentAutoScaling(project, component)
	name := serverlessHPAName(project.Name, component)

	if !autoScaling.Enabled {
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: project.Namespace}, hpa); err == nil {
			if err := r.Delete(ctx, hpa); err != nil && !errors.IsNotFound(err) {
				return err
			}
		} else if !errors.IsNotFound(err) {
			return err
		}
		return nil
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: project.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, hpa, func() error {
		if err := controllerutil.SetControllerReference(project, hpa, r.Scheme); err != nil {
			return err
		}
		hpa.Labels = serverlessSelectorLabels(project.Name, component)
		hpa.Spec.ScaleTargetRef = autoscalingv2.CrossVersionObjectReference{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Name:       serverlessDeploymentName(project.Name, component),
		}
		minReplicas := autoScaling.MinReplicas
		hpa.Spec.MinReplicas = &minReplicas
		hpa.Spec.MaxReplicas = autoScaling.MaxReplicas
		hpa.Spec.Metrics = []autoscalingv2.MetricSpec{
			{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: corev1.ResourceCPU,
					Target: autoscalingv2.MetricTarget{
						Type:               autoscalingv2.UtilizationMetricType,
						AverageUtilization: &autoScaling.TargetCPUUtilizationPercentage,
					},
				},
			},
		}
		return nil
	})

	return err
}

func (r *AntflyServerlessProjectReconciler) deleteProxyResources(ctx context.Context, project *antflyv1.AntflyServerlessProject) error {
	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Name: serverlessDeploymentName(project.Name, serverlessComponentProxy), Namespace: project.Namespace}, deployment); err == nil {
		if err := r.Delete(ctx, deployment); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	service := &corev1.Service{}
	if err := r.Get(ctx, client.ObjectKey{Name: serverlessServiceName(project.Name, serverlessComponentProxy), Namespace: project.Namespace}, service); err == nil {
		if err := r.Delete(ctx, service); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := r.Get(ctx, client.ObjectKey{Name: serverlessHPAName(project.Name, serverlessComponentProxy), Namespace: project.Namespace}, hpa); err == nil {
		if err := r.Delete(ctx, hpa); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	configMap := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Name: serverlessProxyConfigMapName(project.Name), Namespace: project.Namespace}, configMap); err == nil {
		if err := r.Delete(ctx, configMap); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	pdb := &policyv1.PodDisruptionBudget{}
	if err := r.Get(ctx, client.ObjectKey{Name: serverlessPDBName(project.Name, serverlessComponentProxy), Namespace: project.Namespace}, pdb); err == nil {
		if err := r.Delete(ctx, pdb); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	return nil
}

func (r *AntflyServerlessProjectReconciler) deploymentReadyReplicas(ctx context.Context, namespace, name string) (int32, error) {
	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, deployment); err != nil {
		return 0, err
	}
	return deployment.Status.ReadyReplicas, nil
}

func serverlessPodLabels(projectName string, component serverlessComponent) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "antfly-serverless",
		"app.kubernetes.io/component":  string(component),
		"app.kubernetes.io/instance":   projectName,
		"app.kubernetes.io/managed-by": "antfly-operator",
	}
}

func serverlessSelectorLabels(projectName string, component serverlessComponent) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "antfly-serverless",
		"app.kubernetes.io/component": string(component),
		"app.kubernetes.io/instance":  projectName,
	}
}

func serverlessDeploymentName(projectName string, component serverlessComponent) string {
	return fmt.Sprintf("%s-serverless-%s", projectName, component)
}

func serverlessServiceName(projectName string, component serverlessComponent) string {
	return fmt.Sprintf("%s-serverless-%s", projectName, component)
}

func serverlessConfigMapName(projectName string) string {
	return fmt.Sprintf("%s-serverless-config", projectName)
}

func serverlessProxyConfigMapName(projectName string) string {
	return fmt.Sprintf("%s-serverless-proxy-config", projectName)
}

func serverlessPDBName(projectName string, component serverlessComponent) string {
	return fmt.Sprintf("%s-serverless-%s-pdb", projectName, component)
}

func serverlessHPAName(projectName string, component serverlessComponent) string {
	return fmt.Sprintf("%s-serverless-%s-hpa", projectName, component)
}

func componentReplicas(project *antflyv1.AntflyServerlessProject, component serverlessComponent) int32 {
	switch component {
	case serverlessComponentAPI:
		if project.Spec.API.AutoScaling.Enabled {
			return desiredReplicas(project.Spec.API.AutoScaling.MinReplicas)
		}
		return desiredReplicas(project.Spec.API.Replicas)
	case serverlessComponentQuery:
		if project.Spec.Query.AutoScaling.Enabled {
			return desiredReplicas(project.Spec.Query.AutoScaling.MinReplicas)
		}
		return desiredReplicas(project.Spec.Query.Replicas)
	case serverlessComponentMaintenance:
		return desiredReplicas(project.Spec.Maintenance.Replicas)
	case serverlessComponentProxy:
		if project.Spec.Proxy.AutoScaling.Enabled {
			return desiredReplicas(project.Spec.Proxy.AutoScaling.MinReplicas)
		}
		return desiredReplicas(project.Spec.Proxy.Replicas)
	default:
		return 1
	}
}

func componentAutoScaling(project *antflyv1.AntflyServerlessProject, component serverlessComponent) antflyv1.ServerlessAutoScalingSpec {
	switch component {
	case serverlessComponentAPI:
		return project.Spec.API.AutoScaling
	case serverlessComponentQuery:
		return project.Spec.Query.AutoScaling
	case serverlessComponentProxy:
		return project.Spec.Proxy.AutoScaling
	default:
		return antflyv1.ServerlessAutoScalingSpec{}
	}
}

func componentImage(project *antflyv1.AntflyServerlessProject, component serverlessComponent) string {
	switch component {
	case serverlessComponentAPI, serverlessComponentQuery, serverlessComponentMaintenance:
		return project.Spec.Images.ZigImage
	case serverlessComponentProxy:
		return project.Spec.Images.ProxyImage
	default:
		return ""
	}
}

func componentPort(project *antflyv1.AntflyServerlessProject, component serverlessComponent) int32 {
	switch component {
	case serverlessComponentProxy:
		return project.Spec.Proxy.Port
	default:
		return project.Spec.Query.Port
	}
}

func componentCommand(component serverlessComponent) []string {
	switch component {
	case serverlessComponentAPI, serverlessComponentQuery, serverlessComponentMaintenance:
		return []string{"antfly"}
	default:
		return nil
	}
}

func componentArgs(component serverlessComponent) []string {
	switch component {
	case serverlessComponentAPI:
		return []string{"serverless", "api"}
	case serverlessComponentQuery:
		return []string{"serverless", "query"}
	case serverlessComponentMaintenance:
		return []string{"serverless", "maintenance"}
	default:
		return nil
	}
}

func componentEnv(project *antflyv1.AntflyServerlessProject, component serverlessComponent) []corev1.EnvVar {
	_ = project
	_ = component
	return nil
}

func componentVolumes(project *antflyv1.AntflyServerlessProject, component serverlessComponent) []corev1.Volume {
	if component != serverlessComponentProxy {
		return nil
	}

	volumes := []corev1.Volume{
		{
			Name: "proxy-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: serverlessProxyConfigMapName(project.Name),
					},
				},
			},
		},
	}

	if project.Spec.Proxy.Auth.BearerTokenSecretRef != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "proxy-auth",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: project.Spec.Proxy.Auth.BearerTokenSecretRef,
				},
			},
		})
	}

	return volumes
}

func componentVolumeMounts(project *antflyv1.AntflyServerlessProject, component serverlessComponent) []corev1.VolumeMount {
	if component != serverlessComponentProxy {
		return nil
	}

	mounts := []corev1.VolumeMount{
		{
			Name:      "proxy-config",
			MountPath: "/etc/antfly-proxy",
			ReadOnly:  true,
		},
	}

	if project.Spec.Proxy.Auth.BearerTokenSecretRef != "" {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "proxy-auth",
			MountPath: "/etc/antfly-proxy-secret",
			ReadOnly:  true,
		})
	}

	return mounts
}

func componentEnvFrom(project *antflyv1.AntflyServerlessProject, component serverlessComponent) []corev1.EnvFromSource {
	configMapName := serverlessConfigMapName(project.Name)
	if component == serverlessComponentProxy {
		configMapName = serverlessProxyConfigMapName(project.Name)
	}

	envFrom := []corev1.EnvFromSource{
		{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: configMapName,
				},
			},
		},
	}
	if component == serverlessComponentMaintenance && project.Spec.Embeddings.IndexesSecretRef != "" {
		envFrom = append(envFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: project.Spec.Embeddings.IndexesSecretRef,
				},
			},
		})
	}
	return envFrom
}

func desiredReplicas(raw int32) int32 {
	if raw <= 0 {
		return 1
	}
	return raw
}

func imagePullPolicy(raw string) corev1.PullPolicy {
	switch raw {
	case string(corev1.PullAlways):
		return corev1.PullAlways
	case string(corev1.PullNever):
		return corev1.PullNever
	default:
		return corev1.PullIfNotPresent
	}
}

func (r *AntflyServerlessProjectReconciler) checkSecretsReady(ctx context.Context, project *antflyv1.AntflyServerlessProject) (bool, string, error) {
	refs := []string{}
	if project.Spec.Embeddings.IndexesSecretRef != "" {
		refs = append(refs, project.Spec.Embeddings.IndexesSecretRef)
	}
	if project.Spec.Proxy.Auth.BearerTokenSecretRef != "" {
		refs = append(refs, project.Spec.Proxy.Auth.BearerTokenSecretRef)
	}
	if len(refs) == 0 {
		return true, "No external secrets referenced", nil
	}
	for _, ref := range refs {
		secret := &corev1.Secret{}
		if err := r.Get(ctx, client.ObjectKey{Name: ref, Namespace: project.Namespace}, secret); err != nil {
			if errors.IsNotFound(err) {
				return false, fmt.Sprintf("Referenced secret %q was not found", ref), nil
			}
			return false, "", err
		}
	}
	return true, fmt.Sprintf("All referenced secrets are present (%v)", refs), nil
}

func setServerlessSecretsCondition(project *antflyv1.AntflyServerlessProject, ready bool, message string) {
	condition := metav1.Condition{
		Type:               antflyv1.TypeServerlessSecretsReady,
		ObservedGeneration: project.Generation,
		LastTransitionTime: metav1.Now(),
		Message:            message,
	}
	if ready {
		condition.Status = metav1.ConditionTrue
		condition.Reason = antflyv1.ReasonServerlessAllSecretsFound
	} else {
		condition.Status = metav1.ConditionFalse
		condition.Reason = antflyv1.ReasonServerlessSecretNotFound
	}
	apiMeta.SetStatusCondition(&project.Status.Conditions, condition)
}

func (r *AntflyServerlessProjectReconciler) serverlessProxyRoutesJSON(ctx context.Context, project *antflyv1.AntflyServerlessProject) (string, error) {
	aggregated, err := r.aggregateProxyRoutes(ctx, project)
	if err != nil {
		return "", err
	}

	routes := make([]map[string]any, 0, len(aggregated))
	for _, route := range aggregated {
		routes = append(routes, map[string]any{
			"tenant":               route.Tenant,
			"table":                proxyRouteTableName(route),
			"serving_namespace":    proxyRouteServingNamespace(route),
			"preferred_backend":    route.PreferredBackend,
			"allow_stateful":       route.AllowStateful,
			"allow_serverless":     route.AllowServerless,
			"stateful_url":         route.StatefulURL,
			"serverless_query_url": route.ServerlessQueryURL,
			"serverless_api_url":   route.ServerlessAPIURL,
		})
	}
	encoded, err := json.Marshal(routes)
	if err != nil {
		return "", fmt.Errorf("marshal proxy routes: %w", err)
	}
	return string(encoded), nil
}

func (r *AntflyServerlessProjectReconciler) aggregateProxyRoutes(ctx context.Context, project *antflyv1.AntflyServerlessProject) ([]antflyv1.ServerlessProxyRouteSpec, error) {
	projects := &antflyv1.AntflyServerlessProjectList{}
	if err := r.List(ctx, projects, client.InNamespace(project.Namespace)); err != nil {
		return nil, err
	}

	sort.Slice(projects.Items, func(i, j int) bool {
		return projects.Items[i].Name < projects.Items[j].Name
	})

	indexed := map[string]antflyv1.ServerlessProxyRouteSpec{}
	sharedRoutes, err := r.namespaceSharedProxyRoutes(ctx, project.Namespace)
	if err != nil {
		return nil, err
	}
	for _, route := range sharedRoutes {
		key := route.Tenant + "/" + proxyRouteTableName(route)
		indexed[key] = route
	}
	for _, item := range projects.Items {
		projectRoutes, err := r.projectProxyRoutes(ctx, &item)
		if err != nil {
			return nil, err
		}
		for _, route := range projectRoutes {
			key := route.Tenant + "/" + proxyRouteTableName(route)
			if route.AllowServerless && route.ServerlessQueryURL == "" {
				route.ServerlessQueryURL = fmt.Sprintf("http://%s.%s.svc:%d",
					serverlessServiceName(item.Name, serverlessComponentQuery),
					item.Namespace,
					componentPort(&item, serverlessComponentQuery),
				)
			}
			if route.AllowServerless && route.ServerlessAPIURL == "" {
				route.ServerlessAPIURL = fmt.Sprintf("http://%s.%s.svc:%d",
					serverlessServiceName(item.Name, serverlessComponentAPI),
					item.Namespace,
					componentPort(&item, serverlessComponentAPI),
				)
			}
			indexed[key] = route
		}
	}

	keys := make([]string, 0, len(indexed))
	for key := range indexed {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	routes := make([]antflyv1.ServerlessProxyRouteSpec, 0, len(keys))
	for _, key := range keys {
		routes = append(routes, indexed[key])
	}
	return routes, nil
}

func (r *AntflyServerlessProjectReconciler) namespaceSharedProxyRoutes(ctx context.Context, namespace string) ([]antflyv1.ServerlessProxyRouteSpec, error) {
	configMaps := &corev1.ConfigMapList{}
	if err := r.List(ctx, configMaps, client.InNamespace(namespace), client.MatchingLabels{
		serverlessProxyRouteSourceLabel: "true",
	}); err != nil {
		return nil, err
	}

	sort.Slice(configMaps.Items, func(i, j int) bool {
		return configMaps.Items[i].Name < configMaps.Items[j].Name
	})

	var routes []antflyv1.ServerlessProxyRouteSpec
	for _, configMap := range configMaps.Items {
		raw, ok := configMap.Data["routes.json"]
		if !ok {
			continue
		}
		decoded, err := parseProxyRouteSpecsJSON(raw)
		if err != nil {
			return nil, fmt.Errorf("parse shared proxy routes from configmap %q: %w", configMap.Name, err)
		}
		routes = append(routes, decoded...)
	}
	return routes, nil
}

func (r *AntflyServerlessProjectReconciler) projectProxyRoutes(ctx context.Context, project *antflyv1.AntflyServerlessProject) ([]antflyv1.ServerlessProxyRouteSpec, error) {
	routes := append([]antflyv1.ServerlessProxyRouteSpec(nil), project.Spec.Proxy.Routes...)
	if project.Spec.Proxy.RouteConfigMapRef == "" {
		return routes, nil
	}

	configMap := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Name: project.Spec.Proxy.RouteConfigMapRef, Namespace: project.Namespace}, configMap); err != nil {
		return nil, fmt.Errorf("get proxy route configmap %q: %w", project.Spec.Proxy.RouteConfigMapRef, err)
	}

	key := project.Spec.Proxy.RouteConfigMapKey
	if key == "" {
		key = "routes.json"
	}
	raw, ok := configMap.Data[key]
	if !ok {
		return nil, fmt.Errorf("proxy route configmap %q is missing key %q", project.Spec.Proxy.RouteConfigMapRef, key)
	}

	decoded, err := parseProxyRouteSpecsJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("parse proxy routes from configmap %q: %w", project.Spec.Proxy.RouteConfigMapRef, err)
	}
	return append(routes, decoded...), nil
}

type proxyRouteConfig struct {
	Tenant             string `json:"tenant"`
	Table              string `json:"table"`
	Namespace          string `json:"namespace"`
	ServingNamespace   string `json:"serving_namespace"`
	PreferredBackend   string `json:"preferred_backend"`
	AllowStateful      bool   `json:"allow_stateful"`
	AllowServerless    bool   `json:"allow_serverless"`
	StatefulURL        string `json:"stateful_url"`
	ServerlessQueryURL string `json:"serverless_query_url"`
	ServerlessAPIURL   string `json:"serverless_api_url"`
}

func parseProxyRouteSpecsJSON(raw string) ([]antflyv1.ServerlessProxyRouteSpec, error) {
	if raw == "" {
		return nil, nil
	}
	var decoded []proxyRouteConfig
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, err
	}
	routes := make([]antflyv1.ServerlessProxyRouteSpec, 0, len(decoded))
	for _, route := range decoded {
		routes = append(routes, antflyv1.ServerlessProxyRouteSpec{
			Tenant:             route.Tenant,
			Table:              firstNonEmptyProxyString(route.Table, route.Namespace),
			Namespace:          route.Namespace,
			ServingNamespace:   firstNonEmptyProxyString(route.ServingNamespace, route.Namespace, route.Table),
			PreferredBackend:   route.PreferredBackend,
			AllowStateful:      route.AllowStateful,
			AllowServerless:    route.AllowServerless,
			StatefulURL:        route.StatefulURL,
			ServerlessQueryURL: route.ServerlessQueryURL,
			ServerlessAPIURL:   route.ServerlessAPIURL,
		})
	}
	return routes, nil
}

func proxyRouteTableName(route antflyv1.ServerlessProxyRouteSpec) string {
	return firstNonEmptyProxyString(route.Table, route.Namespace)
}

func proxyRouteServingNamespace(route antflyv1.ServerlessProxyRouteSpec) string {
	return firstNonEmptyProxyString(route.ServingNamespace, route.Namespace, route.Table)
}

func firstNonEmptyProxyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
