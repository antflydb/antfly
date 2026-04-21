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

// Package controllers implements the Kubernetes controllers for Termite CRDs.
package controllers

//go:generate go tool controller-gen rbac:roleName=termite-operator-cluster-role paths="." output:rbac:artifacts:config=../manifests/rbac

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	antflyaiv1alpha1 "github.com/antflydb/antfly/pkg/termite-operator/api/v1alpha1"
)

const (
	// TermiteAPIPort is the port the Termite API server listens on.
	// This must match TERMITE_API_URL in the container image (default: http://0.0.0.0:8080).
	TermiteAPIPort = 8080
)

// TermitePoolReconciler reconciles a TermitePool object
type TermitePoolReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	TermiteImage string
	Recorder     events.EventRecorder

	// validationAttempts tracks consecutive validation failure counts per pool
	// (namespace/name -> int). Reset on successful validation.
	validationAttempts sync.Map
}

// +kubebuilder:rbac:groups=antfly.io,resources=termitepools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=antfly.io,resources=termitepools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=antfly.io,resources=termitepools/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles TermitePool reconciliation
func (r *TermitePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the TermitePool
	pool := &antflyaiv1alpha1.TermitePool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		if errors.IsNotFound(err) {
			r.validationAttempts.Delete(req.String())
			logger.Info("TermitePool not found, ignoring")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("Reconciling TermitePool", "name", pool.Name)

	poolKey := req.String()

	// 0. Validate configuration (fallback when webhook is disabled)
	// Generation guard: skip if spec unchanged since last successful validation.
	needsValidation := pool.Status.ObservedGeneration != pool.Generation ||
		pool.Status.Phase == antflyaiv1alpha1.TermitePoolPhaseDegraded
	if needsValidation {
		if err := r.validatePool(pool); err != nil {
			logger.Error(err, "TermitePool validation failed")
			meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
				Type:    antflyaiv1alpha1.TypeConfigurationValid,
				Status:  metav1.ConditionFalse,
				Reason:  antflyaiv1alpha1.ReasonValidationFailed,
				Message: err.Error(),
			})
			if pool.Status.Phase != antflyaiv1alpha1.TermitePoolPhaseDegraded {
				pool.Status.Phase = antflyaiv1alpha1.TermitePoolPhaseDegraded
			}
			if updateErr := r.Status().Update(ctx, pool); updateErr != nil {
				logger.Error(updateErr, "Failed to update status after validation error")
			}
			r.Recorder.Eventf(pool, nil, corev1.EventTypeWarning, antflyaiv1alpha1.ReasonValidationFailed, antflyaiv1alpha1.ReasonValidationFailed, "Validation failed: %s", err.Error())
			attempt := r.incrementValidationAttempts(poolKey)
			return ctrl.Result{RequeueAfter: calculateBackoff(attempt - 1)}, nil
		}
		r.resetValidationAttempts(poolKey)
		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:    antflyaiv1alpha1.TypeConfigurationValid,
			Status:  metav1.ConditionTrue,
			Reason:  antflyaiv1alpha1.ReasonValidationPassed,
			Message: "Configuration is valid",
		})
	}

	// 1. Create or update the headless Service
	if err := r.reconcileService(ctx, pool); err != nil {
		return ctrl.Result{}, err
	}

	// 2. Create or update the ConfigMap for model configuration
	if err := r.reconcileConfigMap(ctx, pool); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Create or update the StatefulSet
	if err := r.reconcileStatefulSet(ctx, pool); err != nil {
		return ctrl.Result{}, err
	}

	// 4. Create or update PodDisruptionBudget (from Availability config or GKE config)
	if err := r.reconcilePDB(ctx, pool); err != nil {
		return ctrl.Result{}, err
	}

	// 5. Update status
	if err := r.updateStatus(ctx, pool); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil // Requeue after 30 seconds
}

func (r *TermitePoolReconciler) reconcileService(ctx context.Context, pool *antflyaiv1alpha1.TermitePool) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pool.Name,
			Namespace: pool.Namespace,
			Labels:    r.labels(pool),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone, // Headless service
			Selector:  r.selectorLabels(pool),
			Ports: []corev1.ServicePort{
				{
					Name:     "http",
					Port:     TermiteAPIPort,
					Protocol: corev1.ProtocolTCP,
				},
			},
		},
	}

	// Set owner reference
	if err := ctrl.SetControllerReference(pool, svc, r.Scheme); err != nil {
		return err
	}

	// Create or update
	existing := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, existing); err != nil {
		if errors.IsNotFound(err) {
			return r.Create(ctx, svc)
		}
		return err
	}

	// Only update if ports changed
	if !reflect.DeepEqual(existing.Spec.Ports, svc.Spec.Ports) {
		existing.Spec.Ports = svc.Spec.Ports
		return r.Update(ctx, existing)
	}
	return nil
}

func (r *TermitePoolReconciler) reconcileConfigMap(ctx context.Context, pool *antflyaiv1alpha1.TermitePool) error {
	// Generate complete configuration
	completeConfig, err := r.generateCompleteConfig(pool)
	if err != nil {
		return fmt.Errorf("failed to generate complete config: %w", err)
	}

	// Build model list for environment variables (backward compatibility)
	models := make([]string, 0, len(pool.Spec.Models.Preload))
	for _, m := range pool.Spec.Models.Preload {
		name := m.Name
		if m.Variant != "" {
			name = name + ":" + m.Variant
		}
		models = append(models, name)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pool.Name + "-config",
			Namespace: pool.Namespace,
			Labels:    r.labels(pool),
		},
		Data: map[string]string{
			// Config file for --config flag
			"config.json": completeConfig,
			// Environment variables (backward compatibility)
			"TERMITE_MODELS":           strings.Join(models, ","),
			"TERMITE_POOL":             pool.Name,
			"TERMITE_WORKLOAD_TYPE":    string(pool.Spec.WorkloadType),
			"TERMITE_LOADING_STRATEGY": string(pool.Spec.Models.LoadingStrategy),
		},
	}

	if pool.Spec.Models.RegistryURL != "" {
		cm.Data["ANTFLY_REGISTRY_URL"] = pool.Spec.Models.RegistryURL
	}

	// Set owner reference
	if err := ctrl.SetControllerReference(pool, cm, r.Scheme); err != nil {
		return err
	}

	// Create or update
	existing := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: cm.Name, Namespace: cm.Namespace}, existing); err != nil {
		if errors.IsNotFound(err) {
			return r.Create(ctx, cm)
		}
		return err
	}

	// Only update if data changed
	if !reflect.DeepEqual(existing.Data, cm.Data) {
		existing.Data = cm.Data
		return r.Update(ctx, existing)
	}
	return nil
}

// generateCompleteConfig merges user-provided config with auto-generated settings
func (r *TermitePoolReconciler) generateCompleteConfig(pool *antflyaiv1alpha1.TermitePool) (string, error) {
	// Start with user config or empty object
	config := make(map[string]any)

	if pool.Spec.Config != "" {
		if err := json.Unmarshal([]byte(pool.Spec.Config), &config); err != nil {
			return "", fmt.Errorf("failed to parse spec.config: %w", err)
		}
	}

	// Build preload model list
	preload := make([]string, 0, len(pool.Spec.Models.Preload))
	for _, m := range pool.Spec.Models.Preload {
		name := m.Name
		if m.Variant != "" {
			name = name + ":" + m.Variant
		}
		preload = append(preload, name)
	}

	// Set auto-generated config (don't override if user specified)
	if _, exists := config["preload"]; !exists && len(preload) > 0 {
		config["preload"] = preload
	}

	// Build per-model loading strategies map
	// Only include models that have an explicit strategy override
	// Key format: "name" or "name-variant" (matches lazy registry naming)
	if _, exists := config["model_strategies"]; !exists {
		modelStrategies := make(map[string]string)
		for _, m := range pool.Spec.Models.Preload {
			if m.Strategy != "" {
				key := m.Name
				if m.Variant != "" {
					key = m.Name + "-" + m.Variant
				}
				modelStrategies[key] = string(m.Strategy)
			}
		}
		if len(modelStrategies) > 0 {
			config["model_strategies"] = modelStrategies
		}
	}

	// Set model directories based on models-dir default
	if _, exists := config["embedder_models_dir"]; !exists {
		config["embedder_models_dir"] = "/models/embedders"
	}
	if _, exists := config["chunker_models_dir"]; !exists {
		config["chunker_models_dir"] = "/models/chunkers"
	}
	if _, exists := config["reranker_models_dir"]; !exists {
		config["reranker_models_dir"] = "/models/rerankers"
	}

	// Set loading strategy config
	// Note: Termite defaults to lazy loading (5m keep_alive) like Ollama.
	// Eager loading must be explicitly set with keep_alive="0".
	if pool.Spec.Models.LoadingStrategy != "" {
		switch pool.Spec.Models.LoadingStrategy {
		case antflyaiv1alpha1.LoadingStrategyEager:
			// Eager loading: explicitly set keep_alive=0 to load all models at startup
			if _, exists := config["keep_alive"]; !exists {
				config["keep_alive"] = "0"
			}
		case antflyaiv1alpha1.LoadingStrategyLazy:
			// Lazy loading: set keep_alive if not specified (matches Termite default)
			if _, exists := config["keep_alive"]; !exists {
				if pool.Spec.Models.KeepAlive != nil {
					config["keep_alive"] = pool.Spec.Models.KeepAlive.Duration.String()
				} else {
					config["keep_alive"] = "5m" // Default 5 minutes
				}
			}
		case antflyaiv1alpha1.LoadingStrategyBounded:
			// Bounded loading: set max_loaded_models
			if _, exists := config["max_loaded_models"]; !exists {
				if pool.Spec.Models.MaxLoadedModels != nil {
					config["max_loaded_models"] = *pool.Spec.Models.MaxLoadedModels
				}
			}
			// Also set keep_alive for LRU eviction
			if _, exists := config["keep_alive"]; !exists {
				if pool.Spec.Models.KeepAlive != nil {
					config["keep_alive"] = pool.Spec.Models.KeepAlive.Duration.String()
				} else {
					config["keep_alive"] = "5m"
				}
			}
		}
	}

	// Set backend_priority based on accelerator type.
	// For CPU-only pools, the default from the container env var is sufficient.
	// This must be a JSON array (not a comma-separated string) so that
	// viper.GetStringSlice parses it correctly.
	if _, exists := config["backend_priority"]; !exists && pool.Spec.Hardware.Accelerator != "" {
		if strings.Contains(pool.Spec.Hardware.Accelerator, "tpu") {
			// TPU: prefer XLA backend
			config["backend_priority"] = []string{"xla", "onnx", "go"}
		} else {
			// GPU (nvidia, etc.): prefer ONNX backend (CUDA support)
			config["backend_priority"] = []string{"onnx", "xla", "go"}
		}
	}

	// Marshal to JSON
	configJSON, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal config: %w", err)
	}

	return string(configJSON), nil
}

func (r *TermitePoolReconciler) reconcileStatefulSet(ctx context.Context, pool *antflyaiv1alpha1.TermitePool) error {
	replicas := pool.Spec.Replicas.Min

	// Build model list for init container pull command
	// Group models by variant to use --variants flag (backward compatible with older images)
	// Example: /termite pull --models-dir /models --variants i8 bge-small-en-v1.5 mxbai-rerank-base-v1
	variantGroups := make(map[string][]string) // variant -> []model names
	for _, m := range pool.Spec.Models.Preload {
		variant := m.Variant
		if variant == "" {
			variant = "f32" // default variant
		}
		variantGroups[variant] = append(variantGroups[variant], m.Name)
	}

	// Build pull command(s) - one per variant group, sorted for deterministic ordering
	variants := make([]string, 0, len(variantGroups))
	for v := range variantGroups {
		variants = append(variants, v)
	}
	slices.Sort(variants)

	var pullCmds []string
	for _, variant := range variants {
		names := variantGroups[variant]
		slices.Sort(names) // Sort model names too for consistency
		pullCmds = append(pullCmds, fmt.Sprintf("/termite pull --models-dir /models --variants %s %s",
			variant, strings.Join(names, " ")))
	}
	pullCmd := strings.Join(pullCmds, " && ")

	// Determine image
	image := r.TermiteImage
	if pool.Spec.Image != "" {
		image = pool.Spec.Image
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pool.Name,
			Namespace: pool.Namespace,
			Labels:    r.labels(pool),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: pool.Name,
			Replicas:    &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: r.selectorLabels(pool),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: r.labels(pool),
				},
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Name:    "model-puller",
							Image:   image,
							Command: []string{"/bin/sh", "-c"},
							Args:    []string{pullCmd},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "models", MountPath: "/models"},
							},
							EnvFrom: []corev1.EnvFromSource{
								{ConfigMapRef: &corev1.ConfigMapEnvSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: pool.Name + "-config"},
								}},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:    "termite",
							Image:   image,
							Command: []string{"/termite"},
							Args:    []string{"run", "--config", "/config/config.json"},
							Ports: []corev1.ContainerPort{
								{Name: "http", ContainerPort: TermiteAPIPort, Protocol: corev1.ProtocolTCP},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "models", MountPath: "/models"},
								{Name: "config", MountPath: "/config", ReadOnly: true},
							},
							EnvFrom: []corev1.EnvFromSource{
								{ConfigMapRef: &corev1.ConfigMapEnvSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: pool.Name + "-config"},
								}},
							},
							Resources: r.buildResources(pool),
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "models",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: pool.Name + "-config",
									},
									Items: []corev1.KeyToPath{
										{Key: "config.json", Path: "config.json"},
									},
								},
							},
						},
					},
					ImagePullSecrets: pool.Spec.ImagePullSecrets,
				},
			},
		},
	}

	// Apply user-specified scheduling constraints first (tolerations, nodeSelector, affinity, topologySpreadConstraints)
	applySchedulingConstraints(&sts.Spec.Template, pool.Spec.Tolerations, pool.Spec.NodeSelector, pool.Spec.Affinity, pool.Spec.TopologySpreadConstraints)

	// Apply GKE-specific pod configuration (Autopilot compute classes, spot instances, etc.)
	r.applyGKEPodSpec(&sts.Spec.Template, pool)

	// Apply EKS-specific pod configuration (Spot instances, instance type affinity, etc.)
	r.applyEKSPodSpec(&sts.Spec.Template, pool)

	// Add TPU node selector and tolerations (works in both Standard and Autopilot modes)
	// In Autopilot, TPU provisioning is triggered by these selectors, not by compute class
	if pool.Spec.Hardware.Accelerator != "" {
		if sts.Spec.Template.Spec.NodeSelector == nil {
			sts.Spec.Template.Spec.NodeSelector = make(map[string]string)
		}
		sts.Spec.Template.Spec.NodeSelector["cloud.google.com/gke-tpu-accelerator"] = pool.Spec.Hardware.Accelerator
		sts.Spec.Template.Spec.NodeSelector["cloud.google.com/gke-tpu-topology"] = pool.Spec.Hardware.Topology

		sts.Spec.Template.Spec.Tolerations = append(sts.Spec.Template.Spec.Tolerations, corev1.Toleration{
			Key:      "google.com/tpu",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		})
	}

	// Add probes
	r.addProbes(sts, pool)

	// Set owner reference
	if err := ctrl.SetControllerReference(pool, sts, r.Scheme); err != nil {
		return err
	}

	// Add template hash annotation to trigger rolling updates when pod spec changes
	// This ensures pods are recreated when tolerations, resources, etc. change
	templateHash, err := computePodTemplateHash(&sts.Spec.Template)
	if err != nil {
		return fmt.Errorf("compute pod template hash: %w", err)
	}
	if sts.Spec.Template.Annotations == nil {
		sts.Spec.Template.Annotations = make(map[string]string)
	}
	sts.Spec.Template.Annotations["termite.antfly.io/template-hash"] = templateHash

	// Create or update
	existing := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}, existing); err != nil {
		if errors.IsNotFound(err) {
			return r.Create(ctx, sts)
		}
		return err
	}

	// Only update if replicas or template changed.
	// Compare template-hash annotations rather than full template specs to avoid
	// false positives from API server defaulting (e.g. added default fields).
	replicasChanged := !reflect.DeepEqual(existing.Spec.Replicas, sts.Spec.Replicas)
	existingHash := existing.Spec.Template.Annotations["termite.antfly.io/template-hash"]
	desiredHash := sts.Spec.Template.Annotations["termite.antfly.io/template-hash"]
	templateChanged := existingHash != desiredHash
	if replicasChanged || templateChanged {
		existing.Spec.Replicas = sts.Spec.Replicas
		existing.Spec.Template = sts.Spec.Template
		return r.Update(ctx, existing)
	}
	return nil
}

// computePodTemplateHash computes a hash of the pod template spec.
// This is used to trigger rolling updates when the template changes.
func computePodTemplateHash(template *corev1.PodTemplateSpec) (string, error) {
	// Create a copy without the hash annotation itself to avoid circular dependency
	templateCopy := template.DeepCopy()
	delete(templateCopy.Annotations, "termite.antfly.io/template-hash")

	hashInput := struct {
		Labels      map[string]string `json:"labels,omitempty"`
		Annotations map[string]string `json:"annotations,omitempty"`
		Spec        corev1.PodSpec    `json:"spec"`
	}{
		Labels:      templateCopy.Labels,
		Annotations: templateCopy.Annotations,
		Spec:        templateCopy.Spec,
	}

	// Marshal to JSON for consistent hashing
	data, err := json.Marshal(hashInput)
	if err != nil {
		return "", fmt.Errorf("marshal pod template spec: %w", err)
	}

	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:8]), nil // Use first 8 bytes (16 hex chars)
}

func (r *TermitePoolReconciler) reconcilePDB(ctx context.Context, pool *antflyaiv1alpha1.TermitePool) error {
	// Get PDB configuration from GKE, EKS, or Availability config
	var pdbConfig *antflyaiv1alpha1.PDBConfig
	pdbName := pool.Name + "-pdb"

	// Prefer cloud-provider PDB config, fall back to Availability config
	if pool.Spec.GKE != nil && pool.Spec.GKE.PodDisruptionBudget != nil {
		pdbConfig = pool.Spec.GKE.PodDisruptionBudget
	} else if pool.Spec.EKS != nil && pool.Spec.EKS.PodDisruptionBudget != nil {
		pdbConfig = pool.Spec.EKS.PodDisruptionBudget
	} else if pool.Spec.Availability != nil && pool.Spec.Availability.PodDisruptionBudget != nil {
		pdbConfig = pool.Spec.Availability.PodDisruptionBudget
	}

	// If no PDB config or not enabled, skip
	if pdbConfig == nil || !pdbConfig.Enabled {
		existing := &policyv1.PodDisruptionBudget{}
		err := r.Get(ctx, types.NamespacedName{Name: pdbName, Namespace: pool.Namespace}, existing)
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		return r.Delete(ctx, existing)
	}

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pdbName,
			Namespace: pool.Namespace,
		},
	}

	// Use CreateOrUpdate to ensure PDB is updated with latest configuration
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		// Set controller reference
		if err := ctrl.SetControllerReference(pool, pdb, r.Scheme); err != nil {
			return err
		}

		// Update PDB spec with labels
		pdb.Labels = r.labels(pool)

		// Set selector to match pool pods
		pdb.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: r.selectorLabels(pool),
		}

		// Set MaxUnavailable or MinAvailable (prefer MaxUnavailable as recommended)
		if pdbConfig.MaxUnavailable != nil {
			maxUnavailable := intstr.FromInt(int(*pdbConfig.MaxUnavailable))
			pdb.Spec.MaxUnavailable = &maxUnavailable
			pdb.Spec.MinAvailable = nil // Clear MinAvailable when MaxUnavailable is set
		} else if pdbConfig.MinAvailable != nil {
			minAvailable := intstr.FromInt(int(*pdbConfig.MinAvailable))
			pdb.Spec.MinAvailable = &minAvailable
			pdb.Spec.MaxUnavailable = nil // Clear MaxUnavailable when MinAvailable is set
		} else {
			// Default to MaxUnavailable=1
			maxUnavailable := intstr.FromInt(1)
			pdb.Spec.MaxUnavailable = &maxUnavailable
			pdb.Spec.MinAvailable = nil
		}

		return nil
	})

	return err
}

// applyGKEPodSpec applies GKE-specific configuration to pod template spec
func (r *TermitePoolReconciler) applyGKEPodSpec(podTemplate *corev1.PodTemplateSpec, pool *antflyaiv1alpha1.TermitePool) {
	// GKE Autopilot mode: use compute class annotations
	if pool.Spec.GKE != nil && pool.Spec.GKE.Autopilot {
		// Initialize annotations if nil
		if podTemplate.Annotations == nil {
			podTemplate.Annotations = make(map[string]string)
		}

		// Set termination grace period for graceful shutdown
		gracePeriod := int64(15)
		podTemplate.Spec.TerminationGracePeriodSeconds = &gracePeriod

		// Check if this is a TPU workload - TPU workloads should NOT have a compute class
		// annotation because the TPU node selectors (gke-tpu-accelerator, gke-tpu-topology)
		// drive node provisioning directly. Adding a compute class like "Balanced" prevents
		// the cluster autoscaler from creating TPU nodes.
		isTPUWorkload := strings.Contains(pool.Spec.Hardware.Accelerator, "tpu")

		if isTPUWorkload {
			// For TPU workloads: don't set compute-class, let node selectors drive provisioning
			// The TPU node selectors are set in ensureTPUResources()

			// But DO add spot toleration if spot is requested
			// TPU spot nodes still have the cloud.google.com/gke-spot taint
			if pool.Spec.Hardware.Spot || pool.Spec.GKE.AutopilotComputeClass == "autopilot-spot" {
				podTemplate.Spec.Tolerations = append(podTemplate.Spec.Tolerations, corev1.Toleration{
					Key:      "cloud.google.com/gke-spot",
					Operator: corev1.TolerationOpEqual,
					Value:    "true",
					Effect:   corev1.TaintEffectNoSchedule,
				})
			}
			return
		}

		// For non-TPU workloads: apply compute class annotation
		computeClass := pool.Spec.GKE.AutopilotComputeClass
		if computeClass == "" {
			computeClass = "Balanced"
		}

		// Apply compute class annotation (required for GKE Autopilot non-TPU workloads)
		podTemplate.Annotations["cloud.google.com/compute-class"] = computeClass

		// Add spot toleration if using autopilot-spot compute class
		// GKE Autopilot spot nodes have the taint cloud.google.com/gke-spot=true:NoSchedule
		if computeClass == "autopilot-spot" {
			podTemplate.Spec.Tolerations = append(podTemplate.Spec.Tolerations, corev1.Toleration{
				Key:      "cloud.google.com/gke-spot",
				Operator: corev1.TolerationOpEqual,
				Value:    "true",
				Effect:   corev1.TaintEffectNoSchedule,
			})
		}

		return
	}

	// Standard GKE mode (non-Autopilot): use node selectors for spot instances
	if pool.Spec.Hardware.Spot {
		// Initialize nodeSelector if nil
		if podTemplate.Spec.NodeSelector == nil {
			podTemplate.Spec.NodeSelector = make(map[string]string)
		}

		// Apply Spot Nodes configuration using node selector
		podTemplate.Spec.NodeSelector["cloud.google.com/gke-spot"] = "true"

		// Set termination grace period for graceful shutdown on eviction
		gracePeriod := int64(15)
		podTemplate.Spec.TerminationGracePeriodSeconds = &gracePeriod
	}
}

func (r *TermitePoolReconciler) updateStatus(ctx context.Context, pool *antflyaiv1alpha1.TermitePool) error {
	// Get StatefulSet to read replica status
	sts := &appsv1.StatefulSet{}

	newPhase := antflyaiv1alpha1.TermitePoolPhasePending
	var ready, total, desired int32

	if err := r.Get(ctx, types.NamespacedName{Name: pool.Name, Namespace: pool.Namespace}, sts); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
	} else {
		ready = sts.Status.ReadyReplicas
		total = sts.Status.Replicas
		desired = *sts.Spec.Replicas

		if ready == desired {
			newPhase = antflyaiv1alpha1.TermitePoolPhaseRunning
		} else if ready > 0 {
			newPhase = antflyaiv1alpha1.TermitePoolPhaseScaling
		}
	}

	// Skip update if nothing changed
	if pool.Status.Phase == newPhase &&
		pool.Status.Replicas.Ready == ready &&
		pool.Status.Replicas.Total == total &&
		pool.Status.Replicas.Desired == desired &&
		pool.Status.ObservedGeneration == pool.Generation {
		return nil
	}

	pool.Status.Phase = newPhase
	pool.Status.Replicas.Ready = ready
	pool.Status.Replicas.Total = total
	pool.Status.Replicas.Desired = desired
	pool.Status.ObservedGeneration = pool.Generation

	return r.Status().Update(ctx, pool)
}

// calculateBackoff calculates exponential backoff duration for validation failures.
// Schedule: 1s, 2s, 4s, 8s, 16s, 32s, 60s (max)
func calculateBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	// Cap to avoid int overflow (1<<63 wraps negative).
	if attempt > 6 {
		return 60 * time.Second
	}
	delay := time.Duration(1<<attempt) * time.Second
	if delay > 60*time.Second {
		return 60 * time.Second
	}
	return delay
}

func (r *TermitePoolReconciler) incrementValidationAttempts(key string) int {
	val, _ := r.validationAttempts.LoadOrStore(key, 0)
	count := val.(int) + 1
	r.validationAttempts.Store(key, count)
	return count
}

func (r *TermitePoolReconciler) resetValidationAttempts(key string) {
	r.validationAttempts.Delete(key)
}

func (r *TermitePoolReconciler) labels(pool *antflyaiv1alpha1.TermitePool) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "termite",
		"app.kubernetes.io/component": "termite-pool",
		"app.kubernetes.io/instance":  pool.Name,
		"antfly.io/pool":              pool.Name,
		"antfly.io/workload-type":     string(pool.Spec.WorkloadType),
	}
}

func (r *TermitePoolReconciler) selectorLabels(pool *antflyaiv1alpha1.TermitePool) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "termite",
		"app.kubernetes.io/instance": pool.Name,
		"antfly.io/pool":             pool.Name,
	}
}

func (r *TermitePoolReconciler) buildResources(pool *antflyaiv1alpha1.TermitePool) corev1.ResourceRequirements {
	// If user provided explicit resources, use those
	if pool.Spec.Resources != nil {
		resources := pool.Spec.Resources.DeepCopy()
		// Ensure TPU resources are set if accelerator is configured
		r.ensureTPUResources(resources, pool)
		return *resources
	}

	// Build default resources
	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("4Gi"),
			corev1.ResourceCPU:    resource.MustParse("1"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("8Gi"),
			corev1.ResourceCPU:    resource.MustParse("2"),
		},
	}

	// Add TPU resources if accelerator is configured
	r.ensureTPUResources(&resources, pool)

	return resources
}

// ensureTPUResources adds google.com/tpu resource requests/limits if an accelerator is configured
// and TPU resources are not already specified. This is required for GKE Autopilot.
func (r *TermitePoolReconciler) ensureTPUResources(resources *corev1.ResourceRequirements, pool *antflyaiv1alpha1.TermitePool) {
	// Only add TPU resources if accelerator is configured
	if pool.Spec.Hardware.Accelerator == "" {
		return
	}

	tpuResourceName := corev1.ResourceName("google.com/tpu")

	// Calculate TPU count from topology (e.g., "2x2" = 4, "2x4" = 8, "4x4" = 16)
	tpuCount := calculateTPUCountFromTopology(pool.Spec.Hardware.Topology)

	// Initialize maps if nil
	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{}
	}
	if resources.Limits == nil {
		resources.Limits = corev1.ResourceList{}
	}

	// Add TPU to requests if not already present
	if _, exists := resources.Requests[tpuResourceName]; !exists {
		resources.Requests[tpuResourceName] = *resource.NewQuantity(int64(tpuCount), resource.DecimalSI)
	}

	// Add TPU to limits if not already present
	if _, exists := resources.Limits[tpuResourceName]; !exists {
		resources.Limits[tpuResourceName] = *resource.NewQuantity(int64(tpuCount), resource.DecimalSI)
	}
}

// calculateTPUCountFromTopology parses a topology string like "2x2" and returns the TPU count
func calculateTPUCountFromTopology(topology string) int {
	if topology == "" {
		return 4 // Default to 2x2 = 4
	}

	// Parse "NxM" format
	parts := strings.Split(topology, "x")
	if len(parts) != 2 {
		return 4 // Default fallback
	}

	var rows, cols int
	if _, err := fmt.Sscanf(parts[0], "%d", &rows); err != nil {
		return 4
	}
	if _, err := fmt.Sscanf(parts[1], "%d", &cols); err != nil {
		return 4
	}

	return rows * cols
}

func (r *TermitePoolReconciler) addProbes(sts *appsv1.StatefulSet, pool *antflyaiv1alpha1.TermitePool) {
	container := &sts.Spec.Template.Spec.Containers[0]

	// Default startup probe (allows 5 min for model loading)
	failureThreshold := int32(30)
	periodSeconds := int32(10)

	if pool.Spec.Availability != nil && pool.Spec.Availability.StartupProbe != nil {
		if pool.Spec.Availability.StartupProbe.FailureThreshold != nil {
			failureThreshold = *pool.Spec.Availability.StartupProbe.FailureThreshold
		}
		if pool.Spec.Availability.StartupProbe.PeriodSeconds != nil {
			periodSeconds = *pool.Spec.Availability.StartupProbe.PeriodSeconds
		}
	}

	container.StartupProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/api/models",
				Port: intstr.FromString("http"),
			},
		},
		FailureThreshold: failureThreshold,
		PeriodSeconds:    periodSeconds,
	}

	container.ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/api/models",
				Port: intstr.FromString("http"),
			},
		},
		PeriodSeconds: 5,
	}

	container.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/api/models",
				Port: intstr.FromString("http"),
			},
		},
		PeriodSeconds: 30,
	}
}

// validatePool performs controller-level validation (fallback when webhook is disabled).
// Note: immutability checks require the old object and are only enforced by the
// admission webhook.
func (r *TermitePoolReconciler) validatePool(pool *antflyaiv1alpha1.TermitePool) error {
	return pool.ValidateTermitePool()
}

// applySchedulingConstraints applies user-specified scheduling constraints to the pod template.
// This is called before cloud-provider-specific functions so that their entries merge on top.
func applySchedulingConstraints(podTemplate *corev1.PodTemplateSpec, tolerations []corev1.Toleration, nodeSelector map[string]string, affinity *corev1.Affinity, topologySpreadConstraints []corev1.TopologySpreadConstraint) {
	// Apply tolerations
	podTemplate.Spec.Tolerations = append(podTemplate.Spec.Tolerations, tolerations...)

	// Apply node selector (merge into existing map)
	if len(nodeSelector) > 0 {
		if podTemplate.Spec.NodeSelector == nil {
			podTemplate.Spec.NodeSelector = make(map[string]string)
		}
		maps.Copy(podTemplate.Spec.NodeSelector, nodeSelector)
	}

	// Apply affinity (deep merge to coexist with cloud-provider entries)
	if affinity != nil {
		if podTemplate.Spec.Affinity == nil {
			podTemplate.Spec.Affinity = affinity.DeepCopy()
		} else {
			if affinity.NodeAffinity != nil {
				if podTemplate.Spec.Affinity.NodeAffinity == nil {
					podTemplate.Spec.Affinity.NodeAffinity = affinity.NodeAffinity.DeepCopy()
				} else {
					podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
						podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
						affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution...,
					)
					if affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
						podTemplate.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.DeepCopy()
					}
				}
			}
			if affinity.PodAffinity != nil {
				podTemplate.Spec.Affinity.PodAffinity = affinity.PodAffinity.DeepCopy()
			}
			if affinity.PodAntiAffinity != nil {
				podTemplate.Spec.Affinity.PodAntiAffinity = affinity.PodAntiAffinity.DeepCopy()
			}
		}
	}

	// Apply topology spread constraints
	if len(topologySpreadConstraints) > 0 {
		podTemplate.Spec.TopologySpreadConstraints = append(podTemplate.Spec.TopologySpreadConstraints, topologySpreadConstraints...)
	}
}

// applyEKSPodSpec applies AWS EKS-specific configuration to pod template spec
func (r *TermitePoolReconciler) applyEKSPodSpec(podTemplate *corev1.PodTemplateSpec, pool *antflyaiv1alpha1.TermitePool) {
	// Only apply EKS configuration if EKS is enabled
	if pool.Spec.EKS == nil || !pool.Spec.EKS.Enabled {
		return
	}

	eks := pool.Spec.EKS

	// Initialize annotations if nil
	if podTemplate.Annotations == nil {
		podTemplate.Annotations = make(map[string]string)
	}

	// Initialize nodeSelector if nil
	if podTemplate.Spec.NodeSelector == nil {
		podTemplate.Spec.NodeSelector = make(map[string]string)
	}

	// Apply Spot Instance configuration
	if eks.UseSpotInstances {
		// EKS Spot Instances use the capacity type label
		// This works with both managed node groups and Karpenter
		podTemplate.Spec.NodeSelector["eks.amazonaws.com/capacityType"] = "SPOT"

		// Set termination grace period for graceful shutdown on Spot interruption
		// AWS gives 2-minute warning before Spot termination
		gracePeriod := int64(25)
		podTemplate.Spec.TerminationGracePeriodSeconds = &gracePeriod

		// Add toleration for Spot Instance taint (common pattern)
		spotToleration := corev1.Toleration{
			Key:      "eks.amazonaws.com/capacityType",
			Operator: corev1.TolerationOpEqual,
			Value:    "SPOT",
			Effect:   corev1.TaintEffectNoSchedule,
		}
		podTemplate.Spec.Tolerations = appendTolerationIfNotExists(podTemplate.Spec.Tolerations, spotToleration)
	}

	// Apply instance type node affinity if specified
	if len(eks.InstanceTypes) > 0 {
		r.applyEKSInstanceTypeAffinity(podTemplate, eks.InstanceTypes)
	}
}

// applyEKSInstanceTypeAffinity adds node affinity to prefer specific EC2 instance types
func (r *TermitePoolReconciler) applyEKSInstanceTypeAffinity(podTemplate *corev1.PodTemplateSpec, instanceTypes []string) {
	if len(instanceTypes) == 0 {
		return
	}

	// Create node affinity for instance types
	instanceTypeRequirement := corev1.NodeSelectorRequirement{
		Key:      "node.kubernetes.io/instance-type",
		Operator: corev1.NodeSelectorOpIn,
		Values:   instanceTypes,
	}

	// Initialize affinity if nil
	if podTemplate.Spec.Affinity == nil {
		podTemplate.Spec.Affinity = &corev1.Affinity{}
	}
	if podTemplate.Spec.Affinity.NodeAffinity == nil {
		podTemplate.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}

	// Use preferred scheduling (soft affinity) to allow fallback to other instance types
	// This prevents pods from being unschedulable if preferred types aren't available
	weight := int32(100)
	preferredTerm := corev1.PreferredSchedulingTerm{
		Weight: weight,
		Preference: corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{instanceTypeRequirement},
		},
	}

	podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
		preferredTerm,
	)
}

// appendTolerationIfNotExists adds a toleration if it doesn't already exist
func appendTolerationIfNotExists(tolerations []corev1.Toleration, newToleration corev1.Toleration) []corev1.Toleration {
	for _, t := range tolerations {
		if t.Key == newToleration.Key && t.Operator == newToleration.Operator && t.Value == newToleration.Value {
			return tolerations
		}
	}
	return append(tolerations, newToleration)
}

// SetupWithManager sets up the controller with the Manager
func (r *TermitePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&antflyaiv1alpha1.TermitePool{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Complete(r)
}
