package v1

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	TypeServerlessProjectReady = "ServerlessProjectReady"
	TypeServerlessSecretsReady = "ServerlessSecretsReady"

	ReasonServerlessProjectPlanned      = "Planned"
	ReasonServerlessProjectValidated    = "Validated"
	ReasonServerlessProjectInvalid      = "Invalid"
	ReasonServerlessProjectReconciling  = "Reconciling"
	ReasonServerlessProjectReady        = "Ready"
	ReasonServerlessSecretNotFound      = "SecretNotFound"
	ReasonServerlessAllSecretsFound     = "AllSecretsFound"
	ServerlessProjectPhasePlanned       = "Planned"
	ServerlessProjectPhaseReconciling   = "Reconciling"
	ServerlessProjectPhaseInvalidConfig = "InvalidConfig"
	ServerlessProjectPhaseReady         = "Ready"
)

type ServerlessObjectStoreSpec struct {
	ArtifactsURI string `json:"artifactsURI"`
	ManifestsURI string `json:"manifestsURI"`
	WALURI       string `json:"walURI"`
	CatalogURI   string `json:"catalogURI"`
	ProgressURI  string `json:"progressURI"`
}

type ServerlessImagesSpec struct {
	ZigImage        string `json:"zigImage,omitempty"`
	ProxyImage      string `json:"proxyImage,omitempty"`
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`
}

type ServerlessAPIRuntimeSpec struct {
	Replicas    int32                     `json:"replicas,omitempty"`
	AutoScaling ServerlessAutoScalingSpec `json:"autoScaling,omitempty"`
}

type ServerlessQueryRuntimeSpec struct {
	Replicas             int32                     `json:"replicas,omitempty"`
	CacheMaxBytes        int64                     `json:"cacheMaxBytes,omitempty"`
	CachePayloadMaxBytes int64                     `json:"cachePayloadMaxBytes,omitempty"`
	Port                 int32                     `json:"port,omitempty"`
	AutoScaling          ServerlessAutoScalingSpec `json:"autoScaling,omitempty"`
}

type ServerlessMaintenanceRuntimeSpec struct {
	Replicas       int32 `json:"replicas,omitempty"`
	TickIntervalMS int32 `json:"tickIntervalMS,omitempty"`
}

type ServerlessAutoScalingSpec struct {
	Enabled                        bool  `json:"enabled,omitempty"`
	MinReplicas                    int32 `json:"minReplicas,omitempty"`
	MaxReplicas                    int32 `json:"maxReplicas,omitempty"`
	TargetCPUUtilizationPercentage int32 `json:"targetCPUUtilizationPercentage,omitempty"`
}

type ServerlessProxySpec struct {
	Enabled           bool                       `json:"enabled,omitempty"`
	Replicas          int32                      `json:"replicas,omitempty"`
	ServiceType       corev1.ServiceType         `json:"serviceType,omitempty"`
	Port              int32                      `json:"port,omitempty"`
	AutoScaling       ServerlessAutoScalingSpec  `json:"autoScaling,omitempty"`
	Auth              ServerlessProxyAuthSpec    `json:"auth,omitempty"`
	Routes            []ServerlessProxyRouteSpec `json:"routes,omitempty"`
	RouteConfigMapRef string                     `json:"routeConfigMapRef,omitempty"`
	RouteConfigMapKey string                     `json:"routeConfigMapKey,omitempty"`
}

type ServerlessEmbeddingSpec struct {
	IndexesSecretRef string `json:"indexesSecretRef,omitempty"`
}

type ServerlessProxyAuthSpec struct {
	BearerTokenSecretRef string `json:"bearerTokenSecretRef,omitempty"`
}

type ServerlessProxyRouteSpec struct {
	Tenant             string `json:"tenant"`
	Table              string `json:"table,omitempty"`
	Namespace          string `json:"namespace,omitempty"`
	ServingNamespace   string `json:"servingNamespace,omitempty"`
	PreferredBackend   string `json:"preferredBackend,omitempty"`
	AllowStateful      bool   `json:"allowStateful,omitempty"`
	AllowServerless    bool   `json:"allowServerless,omitempty"`
	StatefulURL        string `json:"statefulURL,omitempty"`
	ServerlessQueryURL string `json:"serverlessQueryURL,omitempty"`
	ServerlessAPIURL   string `json:"serverlessAPIURL,omitempty"`
}

type AntflyServerlessProjectSpec struct {
	ObjectStore ServerlessObjectStoreSpec        `json:"objectStore"`
	Images      ServerlessImagesSpec             `json:"images,omitempty"`
	API         ServerlessAPIRuntimeSpec         `json:"api,omitempty"`
	Query       ServerlessQueryRuntimeSpec       `json:"query,omitempty"`
	Maintenance ServerlessMaintenanceRuntimeSpec `json:"maintenance,omitempty"`
	Proxy       ServerlessProxySpec              `json:"proxy,omitempty"`
	Embeddings  ServerlessEmbeddingSpec          `json:"embeddings,omitempty"`
	// ServiceAccountName is the Kubernetes ServiceAccount to use for serverless pods.
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
}

type AntflyServerlessProjectStatus struct {
	ObservedGeneration       int64              `json:"observedGeneration,omitempty"`
	Phase                    string             `json:"phase,omitempty"`
	Validated                bool               `json:"validated,omitempty"`
	APIReadyReplicas         int32              `json:"apiReadyReplicas,omitempty"`
	QueryReadyReplicas       int32              `json:"queryReadyReplicas,omitempty"`
	MaintenanceReadyReplicas int32              `json:"maintenanceReadyReplicas,omitempty"`
	ProxyReadyReplicas       int32              `json:"proxyReadyReplicas,omitempty"`
	ConfigMapName            string             `json:"configMapName,omitempty"`
	ProxyConfigMapName       string             `json:"proxyConfigMapName,omitempty"`
	APIServiceName           string             `json:"apiServiceName,omitempty"`
	QueryServiceName         string             `json:"queryServiceName,omitempty"`
	ProxyServiceName         string             `json:"proxyServiceName,omitempty"`
	Conditions               []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Validated",type="boolean",JSONPath=".status.validated"
// +kubebuilder:printcolumn:name="API",type="integer",JSONPath=".status.apiReadyReplicas"
// +kubebuilder:printcolumn:name="Query",type="integer",JSONPath=".status.queryReadyReplicas"
// +kubebuilder:printcolumn:name="Maint",type="integer",JSONPath=".status.maintenanceReadyReplicas"
// +kubebuilder:printcolumn:name="Proxy",type="integer",JSONPath=".status.proxyReadyReplicas"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type AntflyServerlessProject struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   AntflyServerlessProjectSpec   `json:"spec"`
	Status AntflyServerlessProjectStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type AntflyServerlessProjectList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []AntflyServerlessProject `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AntflyServerlessProject{}, &AntflyServerlessProjectList{})
}

func (r *AntflyServerlessProject) Default() {
	if r.Spec.API.Replicas == 0 {
		r.Spec.API.Replicas = 1
	}
	if r.Spec.Query.Replicas == 0 {
		r.Spec.Query.Replicas = 1
	}
	if r.Spec.Maintenance.Replicas == 0 {
		r.Spec.Maintenance.Replicas = 1
	}
	if r.Spec.Query.Port == 0 {
		r.Spec.Query.Port = 8080
	}
	if r.Spec.Proxy.Port == 0 {
		r.Spec.Proxy.Port = 8080
	}
	if r.Spec.Proxy.Enabled && r.Spec.Proxy.Replicas == 0 {
		r.Spec.Proxy.Replicas = 1
	}
	if r.Spec.Proxy.ServiceType == "" {
		r.Spec.Proxy.ServiceType = corev1.ServiceTypeClusterIP
	}
	if r.Spec.Proxy.RouteConfigMapRef != "" && r.Spec.Proxy.RouteConfigMapKey == "" {
		r.Spec.Proxy.RouteConfigMapKey = "routes.json"
	}
	if r.Spec.Maintenance.TickIntervalMS == 0 {
		r.Spec.Maintenance.TickIntervalMS = 1000
	}
	if r.Spec.API.AutoScaling.Enabled {
		if r.Spec.API.AutoScaling.MinReplicas == 0 {
			r.Spec.API.AutoScaling.MinReplicas = 1
		}
		if r.Spec.API.AutoScaling.MaxReplicas == 0 {
			r.Spec.API.AutoScaling.MaxReplicas = 3
		}
		if r.Spec.API.AutoScaling.TargetCPUUtilizationPercentage == 0 {
			r.Spec.API.AutoScaling.TargetCPUUtilizationPercentage = 70
		}
	}
	if r.Spec.Query.AutoScaling.Enabled {
		if r.Spec.Query.AutoScaling.MinReplicas == 0 {
			r.Spec.Query.AutoScaling.MinReplicas = 1
		}
		if r.Spec.Query.AutoScaling.MaxReplicas == 0 {
			r.Spec.Query.AutoScaling.MaxReplicas = 3
		}
		if r.Spec.Query.AutoScaling.TargetCPUUtilizationPercentage == 0 {
			r.Spec.Query.AutoScaling.TargetCPUUtilizationPercentage = 70
		}
	}
	if r.Spec.Proxy.AutoScaling.Enabled {
		if r.Spec.Proxy.AutoScaling.MinReplicas == 0 {
			r.Spec.Proxy.AutoScaling.MinReplicas = 1
		}
		if r.Spec.Proxy.AutoScaling.MaxReplicas == 0 {
			r.Spec.Proxy.AutoScaling.MaxReplicas = 3
		}
		if r.Spec.Proxy.AutoScaling.TargetCPUUtilizationPercentage == 0 {
			r.Spec.Proxy.AutoScaling.TargetCPUUtilizationPercentage = 70
		}
	}
	for i := range r.Spec.Proxy.Routes {
		if strings.TrimSpace(r.Spec.Proxy.Routes[i].Table) == "" {
			r.Spec.Proxy.Routes[i].Table = strings.TrimSpace(r.Spec.Proxy.Routes[i].Namespace)
		}
		if strings.TrimSpace(r.Spec.Proxy.Routes[i].ServingNamespace) == "" {
			r.Spec.Proxy.Routes[i].ServingNamespace = firstNonEmptyString(r.Spec.Proxy.Routes[i].Namespace, r.Spec.Proxy.Routes[i].Table)
		}
	}
}

func (r *AntflyServerlessProject) ValidateCreate() error {
	return r.ValidateAntflyServerlessProject()
}

func (r *AntflyServerlessProject) ValidateUpdate(old runtime.Object) error {
	_, _ = old.(*AntflyServerlessProject)
	return r.ValidateAntflyServerlessProject()
}

func (r *AntflyServerlessProject) ValidateAntflyServerlessProject() error {
	var missing []string
	if strings.TrimSpace(r.Spec.ObjectStore.ArtifactsURI) == "" {
		missing = append(missing, "spec.objectStore.artifactsURI")
	}
	if strings.TrimSpace(r.Spec.ObjectStore.ManifestsURI) == "" {
		missing = append(missing, "spec.objectStore.manifestsURI")
	}
	if strings.TrimSpace(r.Spec.ObjectStore.WALURI) == "" {
		missing = append(missing, "spec.objectStore.walURI")
	}
	if strings.TrimSpace(r.Spec.ObjectStore.CatalogURI) == "" {
		missing = append(missing, "spec.objectStore.catalogURI")
	}
	if strings.TrimSpace(r.Spec.ObjectStore.ProgressURI) == "" {
		missing = append(missing, "spec.objectStore.progressURI")
	}
	if strings.TrimSpace(r.Spec.Images.ZigImage) == "" {
		missing = append(missing, "spec.images.zigImage")
	}
	if r.Spec.Proxy.Enabled && strings.TrimSpace(r.Spec.Images.ProxyImage) == "" {
		missing = append(missing, "spec.images.proxyImage")
	}
	if len(missing) > 0 {
		return fmt.Errorf("AntflyServerlessProject validation failed:\n  - missing required fields: %s", strings.Join(missing, ", "))
	}
	if r.Spec.API.Replicas < 0 {
		return fmt.Errorf("spec.api.replicas must be >= 0")
	}
	if r.Spec.Query.Replicas < 0 {
		return fmt.Errorf("spec.query.replicas must be >= 0")
	}
	if r.Spec.Maintenance.Replicas < 0 {
		return fmt.Errorf("spec.maintenance.replicas must be >= 0")
	}
	if r.Spec.Proxy.Replicas < 0 {
		return fmt.Errorf("spec.proxy.replicas must be >= 0")
	}
	if r.Spec.Query.Port < 0 || r.Spec.Query.Port > 65535 {
		return fmt.Errorf("spec.query.port must be between 0 and 65535")
	}
	if r.Spec.Proxy.Port < 0 || r.Spec.Proxy.Port > 65535 {
		return fmt.Errorf("spec.proxy.port must be between 0 and 65535")
	}
	if r.Spec.Maintenance.TickIntervalMS < 0 {
		return fmt.Errorf("spec.maintenance.tickIntervalMS must be >= 0")
	}
	if err := validateServerlessAutoScaling("spec.api.autoScaling", r.Spec.API.AutoScaling); err != nil {
		return err
	}
	if err := validateServerlessAutoScaling("spec.query.autoScaling", r.Spec.Query.AutoScaling); err != nil {
		return err
	}
	if err := validateServerlessAutoScaling("spec.proxy.autoScaling", r.Spec.Proxy.AutoScaling); err != nil {
		return err
	}
	for i, route := range r.Spec.Proxy.Routes {
		path := fmt.Sprintf("spec.proxy.routes[%d]", i)
		if strings.TrimSpace(route.Tenant) == "" {
			return fmt.Errorf("%s.tenant is required", path)
		}
		if strings.TrimSpace(route.Table) == "" && strings.TrimSpace(route.Namespace) == "" {
			return fmt.Errorf("%s.table or %s.namespace is required", path, path)
		}
		if !route.AllowStateful && !route.AllowServerless {
			return fmt.Errorf("%s must allow at least one backend", path)
		}
		if route.AllowStateful && strings.TrimSpace(route.StatefulURL) == "" {
			return fmt.Errorf("%s.statefulURL is required when allowStateful=true", path)
		}
		if route.AllowServerless && strings.TrimSpace(route.ServerlessQueryURL) == "" {
			return fmt.Errorf("%s.serverlessQueryURL is required when allowServerless=true", path)
		}
		switch strings.TrimSpace(route.PreferredBackend) {
		case "", "stateful", "serverless":
		default:
			return fmt.Errorf("%s.preferredBackend must be one of: stateful, serverless", path)
		}
	}
	if r.Spec.Proxy.RouteConfigMapKey != "" && strings.TrimSpace(r.Spec.Proxy.RouteConfigMapRef) == "" {
		return fmt.Errorf("spec.proxy.routeConfigMapKey requires spec.proxy.routeConfigMapRef")
	}
	return nil
}

func validateServerlessAutoScaling(path string, spec ServerlessAutoScalingSpec) error {
	if !spec.Enabled {
		return nil
	}
	if spec.MinReplicas <= 0 {
		return fmt.Errorf("%s.minReplicas must be > 0 when autoscaling is enabled", path)
	}
	if spec.MaxReplicas < spec.MinReplicas {
		return fmt.Errorf("%s.maxReplicas must be >= minReplicas", path)
	}
	if spec.TargetCPUUtilizationPercentage <= 0 || spec.TargetCPUUtilizationPercentage > 100 {
		return fmt.Errorf("%s.targetCPUUtilizationPercentage must be between 1 and 100", path)
	}
	return nil
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
