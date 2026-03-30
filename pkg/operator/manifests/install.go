package manifests

import (
	"fmt"
	"strings"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/yaml"
)

const (
	defaultOperatorImage = "ghcr.io/antflydb/antfly-operator:latest"
	webhookServiceName   = "antfly-operator-webhook-service"
	webhookCertName      = "antfly-operator-serving-cert"
	webhookIssuerName    = "selfsigned-issuer"
	webhookSecretName    = "webhook-server-cert"
)

// InstallOptions controls the generated install manifest bundle.
type InstallOptions struct {
	OperatorImage string
	IncludeCRDs   bool
}

// UninstallOptions controls the generated uninstall manifest bundle.
type UninstallOptions struct {
	IncludeCRDs bool
}

// OperatorInstallYAML returns a self-contained install manifest bundle for the
// operator, including CRDs, RBAC, the manager Deployment, and validating
// webhook resources.
func OperatorInstallYAML(opts InstallOptions) (string, error) {
	image := strings.TrimSpace(opts.OperatorImage)
	if image == "" {
		image = defaultOperatorImage
	}

	var docs []string
	if opts.IncludeCRDs {
		docs = append(docs, strings.TrimSpace(AllCRDsYAML()))
	}

	rbacDocs, err := marshalYAMLDocuments(
		Namespace(),
		ServiceAccount(),
		mustClusterRoleFromYAML(),
		ClusterRoleBinding(),
		LeaderElectionRole(),
		LeaderElectionRoleBinding(),
	)
	if err != nil {
		return "", err
	}
	docs = append(docs, rbacDocs...)

	installDocs, err := marshalYAMLDocuments(
		managerDeployment(image),
		webhookService(),
		validatingWebhookConfiguration(),
	)
	if err != nil {
		return "", err
	}
	docs = append(docs, installDocs...)
	docs = append(docs, certManagerIssuerYAML(), certManagerCertificateYAML())

	return strings.Join(filterEmpty(docs), "\n---\n") + "\n", nil
}

// OperatorUninstallYAML returns a manifest bundle suitable for deleting the
// operator installation. By default it preserves CRDs unless explicitly
// requested, which avoids deleting custom resources unexpectedly.
func OperatorUninstallYAML(opts UninstallOptions) (string, error) {
	rbacDocs, err := marshalYAMLDocuments(
		validatingWebhookConfiguration(),
		webhookService(),
		managerDeployment(defaultOperatorImage),
	)
	if err != nil {
		return "", err
	}

	docs := make([]string, 0, 12)
	docs = append(docs, rbacDocs...)
	docs = append(docs, certManagerCertificateYAML(), certManagerIssuerYAML())

	leaderElectionDocs, err := marshalYAMLDocuments(
		LeaderElectionRoleBinding(),
		LeaderElectionRole(),
		ClusterRoleBinding(),
		mustClusterRoleFromYAML(),
		ServiceAccount(),
		Namespace(),
	)
	if err != nil {
		return "", err
	}
	docs = append(docs, leaderElectionDocs...)

	if opts.IncludeCRDs {
		docs = append(docs, strings.TrimSpace(AllCRDsYAML()))
	}

	return strings.Join(filterEmpty(docs), "\n---\n") + "\n", nil
}

func mustClusterRoleFromYAML() any {
	role, err := ClusterRoleFromYAML()
	if err != nil {
		panic(err)
	}
	return role
}

func marshalYAMLDocuments(objects ...any) ([]string, error) {
	docs := make([]string, 0, len(objects))
	for _, obj := range objects {
		if obj == nil {
			continue
		}
		data, err := yaml.Marshal(obj)
		if err != nil {
			return nil, err
		}
		docs = append(docs, strings.TrimSpace(string(data)))
	}
	return docs, nil
}

func filterEmpty(docs []string) []string {
	filtered := make([]string, 0, len(docs))
	for _, doc := range docs {
		doc = strings.TrimSpace(doc)
		if doc == "" {
			continue
		}
		filtered = append(filtered, doc)
	}
	return filtered
}

func managerDeployment(image string) *appsv1.Deployment {
	labels := map[string]string{
		"app.kubernetes.io/name":       "antfly-operator",
		"app.kubernetes.io/component":  "controller",
		"app.kubernetes.io/part-of":    "antfly-operator",
		"app.kubernetes.io/managed-by": "antfly-operator",
	}

	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "antfly-operator",
			Namespace: OperatorNamespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name":      "antfly-operator",
					"app.kubernetes.io/component": "controller",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						"kubectl.kubernetes.io/default-container": "antfly-operator-manager",
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: ServiceAccountName,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: boolPtr(true),
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{
						{
							Name:            "antfly-operator-manager",
							Image:           image,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command:         []string{"/manager"},
							Args:            []string{"--leader-elect"},
							Env: []corev1.EnvVar{
								{
									Name:  "ENABLE_WEBHOOKS",
									Value: "true",
								},
							},
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    mustParseQuantity("500m"),
									corev1.ResourceMemory: mustParseQuantity("256Mi"),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    mustParseQuantity("100m"),
									corev1.ResourceMemory: mustParseQuantity("128Mi"),
								},
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 8080,
									Name:          "metrics",
									Protocol:      corev1.ProtocolTCP,
								},
								{
									ContainerPort: 8081,
									Name:          "health",
									Protocol:      corev1.ProtocolTCP,
								},
								{
									ContainerPort: 9443,
									Name:          "webhook-server",
									Protocol:      corev1.ProtocolTCP,
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/healthz",
										Port: intstr.FromString("health"),
									},
								},
								InitialDelaySeconds: 15,
								PeriodSeconds:       20,
								TimeoutSeconds:      5,
								FailureThreshold:    3,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/readyz",
										Port: intstr.FromString("health"),
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
								TimeoutSeconds:      5,
								FailureThreshold:    3,
							},
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: boolPtr(false),
								RunAsNonRoot:             boolPtr(true),
								RunAsUser:                int64Ptr(65532),
								ReadOnlyRootFilesystem:   boolPtr(true),
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "cert",
									MountPath: "/tmp/k8s-webhook-server/serving-certs",
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "cert",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									DefaultMode: int32Ptr(420),
									SecretName:  webhookSecretName,
								},
							},
						},
					},
					Tolerations: []corev1.Toleration{
						{
							Key:      "cloud.google.com/gke-spot",
							Operator: corev1.TolerationOpEqual,
							Value:    "true",
							Effect:   corev1.TaintEffectNoSchedule,
						},
						{
							Key:      "node.gke.io/balloon-pod-resize",
							Operator: corev1.TolerationOpExists,
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
					TerminationGracePeriodSeconds: int64Ptr(10),
				},
			},
		},
	}
}

func webhookService() *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      webhookServiceName,
			Namespace: OperatorNamespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Port:       443,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt(9443),
				},
			},
			Selector: map[string]string{
				"app.kubernetes.io/name":      "antfly-operator",
				"app.kubernetes.io/component": "controller",
			},
		},
	}
}

func validatingWebhookConfiguration() *admissionregistrationv1.ValidatingWebhookConfiguration {
	failurePolicy := admissionregistrationv1.Fail
	sideEffects := admissionregistrationv1.SideEffectClassNone
	matchPolicy := admissionregistrationv1.Equivalent
	scope := admissionregistrationv1.AllScopes

	return &admissionregistrationv1.ValidatingWebhookConfiguration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "admissionregistration.k8s.io/v1",
			Kind:       "ValidatingWebhookConfiguration",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "antfly-operator-validating-webhook-configuration",
			Annotations: map[string]string{
				"cert-manager.io/inject-ca-from": fmt.Sprintf("%s/%s", OperatorNamespace, webhookCertName),
			},
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			validatingWebhook("vantflycluster.kb.io", "/validate-antfly-io-v1-antflycluster", "antflyclusters", &failurePolicy, &sideEffects, &matchPolicy, &scope),
			validatingWebhook("vantflybackup.kb.io", "/validate-antfly-io-v1-antflybackup", "antflybackups", &failurePolicy, &sideEffects, &matchPolicy, &scope),
			validatingWebhook("vantflyrestore.kb.io", "/validate-antfly-io-v1-antflyrestore", "antflyrestores", &failurePolicy, &sideEffects, &matchPolicy, &scope),
			validatingWebhook("vantflyserverlessproject.kb.io", "/validate-antfly-io-v1-antflyserverlessproject", "antflyserverlessprojects", &failurePolicy, &sideEffects, &matchPolicy, &scope),
		},
	}
}

func validatingWebhook(name, path, resource string, failurePolicy *admissionregistrationv1.FailurePolicyType, sideEffects *admissionregistrationv1.SideEffectClass, matchPolicy *admissionregistrationv1.MatchPolicyType, scope *admissionregistrationv1.ScopeType) admissionregistrationv1.ValidatingWebhook {
	return admissionregistrationv1.ValidatingWebhook{
		Name:                    name,
		AdmissionReviewVersions: []string{"v1"},
		FailurePolicy:           failurePolicy,
		SideEffects:             sideEffects,
		MatchPolicy:             matchPolicy,
		ClientConfig: admissionregistrationv1.WebhookClientConfig{
			Service: &admissionregistrationv1.ServiceReference{
				Name:      webhookServiceName,
				Namespace: OperatorNamespace,
				Path:      &path,
			},
		},
		NamespaceSelector: &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{
					Key:      "kubernetes.io/metadata.name",
					Operator: metav1.LabelSelectorOpNotIn,
					Values:   []string{OperatorNamespace},
				},
			},
		},
		Rules: []admissionregistrationv1.RuleWithOperations{
			{
				Operations: []admissionregistrationv1.OperationType{
					admissionregistrationv1.Create,
					admissionregistrationv1.Update,
				},
				Rule: admissionregistrationv1.Rule{
					APIGroups:   []string{"antfly.io"},
					APIVersions: []string{"v1"},
					Resources:   []string{resource},
					Scope:       scope,
				},
			},
		},
	}
}

func certManagerIssuerYAML() string {
	return strings.TrimSpace(fmt.Sprintf(`
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: %s
  namespace: %s
spec:
  selfSigned: {}
`, webhookIssuerName, OperatorNamespace))
}

func certManagerCertificateYAML() string {
	return strings.TrimSpace(fmt.Sprintf(`
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: %s
  namespace: %s
spec:
  dnsNames:
    - %s.%s.svc
    - %s.%s.svc.cluster.local
  issuerRef:
    kind: Issuer
    name: %s
  secretName: %s
`, webhookCertName, OperatorNamespace, webhookServiceName, OperatorNamespace, webhookServiceName, OperatorNamespace, webhookIssuerName, webhookSecretName))
}

func mustParseQuantity(v string) resource.Quantity {
	return resource.MustParse(v)
}

func boolPtr(v bool) *bool {
	return &v
}

func int32Ptr(v int32) *int32 {
	return &v
}

func int64Ptr(v int64) *int64 {
	return &v
}
