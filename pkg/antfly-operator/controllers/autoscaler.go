package controllers

import (
	"context"
	"fmt"
	"math"
	"time"

	antflyv1 "github.com/antflydb/antfly/pkg/antfly-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// AutoScaler handles autoscaling logic for AntflyCluster
type AutoScaler struct {
	client        client.Client
	k8sClient     kubernetes.Interface
	metricsClient client.Client
}

// NewAutoScaler creates a new AutoScaler instance
func NewAutoScaler(client client.Client, k8sClient kubernetes.Interface, metricsClient client.Client) *AutoScaler {
	return &AutoScaler{
		client:        client,
		k8sClient:     k8sClient,
		metricsClient: metricsClient,
	}
}

// MetricsData contains collected metrics for scaling decisions
type MetricsData struct {
	AverageCPUUsageMillicores   int64
	AverageMemoryUsageBytes     int64
	TotalCPURequestMillicores   int64
	TotalMemoryRequestBytes     int64
	CPUUtilizationPercentage    int32
	MemoryUtilizationPercentage int32
	PodCount                    int32
}

// EvaluateScaling evaluates if scaling is needed and returns desired replicas
func (a *AutoScaler) EvaluateScaling(ctx context.Context, cluster *antflyv1.AntflyCluster) (int32, error) {
	log := log.FromContext(ctx)

	// Check if autoscaling is enabled
	if cluster.Spec.DataNodes.AutoScaling == nil || !cluster.Spec.DataNodes.AutoScaling.Enabled {
		return cluster.Spec.DataNodes.Replicas, nil
	}

	autoScaling := cluster.Spec.DataNodes.AutoScaling
	currentReplicas := cluster.Spec.DataNodes.Replicas

	// Check cooldown periods
	if !a.canScale(cluster, autoScaling) {
		log.Info("Scaling is in cooldown period", "cluster", cluster.Name)
		return currentReplicas, nil
	}

	// Collect metrics
	metrics, err := a.collectMetrics(ctx, cluster)
	if err != nil {
		return currentReplicas, fmt.Errorf("failed to collect metrics: %w", err)
	}

	// Update current metrics in status
	if cluster.Status.AutoScalingStatus == nil {
		cluster.Status.AutoScalingStatus = &antflyv1.AutoScalingStatus{}
	}
	cluster.Status.AutoScalingStatus.CurrentCPUUtilizationPercentage = &metrics.CPUUtilizationPercentage
	cluster.Status.AutoScalingStatus.CurrentMemoryUtilizationPercentage = &metrics.MemoryUtilizationPercentage
	cluster.Status.AutoScalingStatus.CurrentReplicas = currentReplicas

	// Calculate desired replicas based on metrics
	desiredReplicas := min(
		max(
			a.calculateDesiredReplicas(currentReplicas, autoScaling, metrics),
			autoScaling.MinReplicas,
		),
		autoScaling.MaxReplicas,
	)

	// Log scaling decision
	if desiredReplicas != currentReplicas {
		log.Info("Scaling decision made",
			"cluster", cluster.Name,
			"currentReplicas", currentReplicas,
			"desiredReplicas", desiredReplicas,
			"cpuUtilization", metrics.CPUUtilizationPercentage,
			"memoryUtilization", metrics.MemoryUtilizationPercentage,
		)
	}

	return desiredReplicas, nil
}

// collectMetrics collects metrics from pods
func (a *AutoScaler) collectMetrics(ctx context.Context, cluster *antflyv1.AntflyCluster) (*MetricsData, error) {
	// Get data node pods
	podList := &corev1.PodList{}
	labelSelector := labels.SelectorFromSet(map[string]string{
		"app":       "antfly",
		"cluster":   cluster.Name,
		"component": "data",
	})

	if err := a.client.List(ctx, podList, &client.ListOptions{
		Namespace:     cluster.Namespace,
		LabelSelector: labelSelector,
	}); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	if len(podList.Items) == 0 {
		return &MetricsData{}, nil
	}

	// Get pod metrics
	podMetricsList := &metricsv1beta1.PodMetricsList{}
	if err := a.metricsClient.List(ctx, podMetricsList, &client.ListOptions{
		Namespace:     cluster.Namespace,
		LabelSelector: labelSelector,
	}); err != nil {
		// If metrics are not available yet, return zero metrics
		return &MetricsData{PodCount: int32(min(len(podList.Items), math.MaxInt32))}, nil //nolint:gosec // G115: pod count is bounded
	}

	// Calculate aggregate metrics
	metrics := &MetricsData{
		PodCount: int32(min(len(podList.Items), math.MaxInt32)), //nolint:gosec // G115: pod count is bounded
	}

	// Calculate total requested resources
	for _, pod := range podList.Items {
		for _, container := range pod.Spec.Containers {
			if cpu := container.Resources.Requests.Cpu(); cpu != nil {
				metrics.TotalCPURequestMillicores += cpu.MilliValue()
			}
			if memory := container.Resources.Requests.Memory(); memory != nil {
				metrics.TotalMemoryRequestBytes += memory.Value()
			}
		}
	}

	// Calculate actual usage from metrics
	for _, podMetrics := range podMetricsList.Items {
		for _, container := range podMetrics.Containers {
			metrics.AverageCPUUsageMillicores += container.Usage.Cpu().MilliValue()
			metrics.AverageMemoryUsageBytes += container.Usage.Memory().Value()
		}
	}

	// Calculate averages and percentages
	if metrics.PodCount > 0 {
		metrics.AverageCPUUsageMillicores = metrics.AverageCPUUsageMillicores / int64(metrics.PodCount)
		metrics.AverageMemoryUsageBytes = metrics.AverageMemoryUsageBytes / int64(metrics.PodCount)

		if metrics.TotalCPURequestMillicores > 0 {
			metrics.CPUUtilizationPercentage = int32(min((metrics.AverageCPUUsageMillicores*100*int64(metrics.PodCount))/metrics.TotalCPURequestMillicores, math.MaxInt32)) //nolint:gosec // G115: percentage value is bounded
		}
		if metrics.TotalMemoryRequestBytes > 0 {
			metrics.MemoryUtilizationPercentage = int32(min((metrics.AverageMemoryUsageBytes*100*int64(metrics.PodCount))/metrics.TotalMemoryRequestBytes, math.MaxInt32)) //nolint:gosec // G115: percentage value is bounded
		}
	}

	return metrics, nil
}

// calculateDesiredReplicas calculates the desired number of replicas based on metrics
// It considers both CPU and memory utilization, taking the maximum of the two
// to ensure adequate resources. For scale-down, it requires BOTH metrics (if configured)
// to be below target to prevent premature scaling.
func (a *AutoScaler) calculateDesiredReplicas(currentReplicas int32, autoScaling *antflyv1.AutoScalingSpec, metrics *MetricsData) int32 {
	if metrics.PodCount == 0 {
		return currentReplicas
	}

	var cpuDesiredReplicas, memoryDesiredReplicas int32
	hasCPUTarget := autoScaling.TargetCPUUtilizationPercentage != nil
	hasMemoryTarget := autoScaling.TargetMemoryUtilizationPercentage != nil

	// Calculate CPU-based desired replicas
	if hasCPUTarget {
		targetCPU := *autoScaling.TargetCPUUtilizationPercentage
		if targetCPU > 0 && metrics.CPUUtilizationPercentage > 0 {
			cpuRatio := float64(metrics.CPUUtilizationPercentage) / float64(targetCPU)
			cpuDesiredReplicas = int32(math.Ceil(float64(currentReplicas) * cpuRatio))
		} else {
			// No CPU metrics available, maintain current
			cpuDesiredReplicas = currentReplicas
		}
	}

	// Calculate memory-based desired replicas
	if hasMemoryTarget {
		targetMemory := *autoScaling.TargetMemoryUtilizationPercentage
		if targetMemory > 0 && metrics.MemoryUtilizationPercentage > 0 {
			memoryRatio := float64(metrics.MemoryUtilizationPercentage) / float64(targetMemory)
			memoryDesiredReplicas = int32(math.Ceil(float64(currentReplicas) * memoryRatio))
		} else {
			// No memory metrics available, maintain current
			memoryDesiredReplicas = currentReplicas
		}
	}

	// Determine final desired replicas
	var desiredReplicas int32
	if hasCPUTarget && hasMemoryTarget {
		// When both targets are configured:
		// - Scale UP based on the higher of the two (either resource being constrained triggers scale-up)
		// - Scale DOWN only when BOTH are below target (both resources have headroom)
		maxDesired := max(cpuDesiredReplicas, memoryDesiredReplicas)
		minDesired := min(cpuDesiredReplicas, memoryDesiredReplicas)

		if maxDesired > currentReplicas {
			// Scale up: use the higher value (resource that needs more capacity)
			desiredReplicas = maxDesired
		} else if maxDesired < currentReplicas {
			// Scale down: use the higher of the two (more conservative)
			// This ensures we don't scale down if either resource is still near target
			desiredReplicas = maxDesired
		} else {
			desiredReplicas = currentReplicas
		}
		// Avoid scaling down too aggressively - if one metric wants to stay, respect it
		if desiredReplicas < currentReplicas && minDesired < currentReplicas && maxDesired >= currentReplicas {
			desiredReplicas = currentReplicas
		}
	} else if hasCPUTarget {
		desiredReplicas = cpuDesiredReplicas
	} else if hasMemoryTarget {
		desiredReplicas = memoryDesiredReplicas
	} else {
		// No targets configured, maintain current
		return currentReplicas
	}

	// Apply scaling policies (gradual scaling)
	desiredReplicas = a.applyScalingLimits(currentReplicas, desiredReplicas)

	return desiredReplicas
}

// applyScalingLimits applies gradual scaling policies to prevent sudden resource changes
func (a *AutoScaler) applyScalingLimits(currentReplicas, desiredReplicas int32) int32 {
	if desiredReplicas > currentReplicas {
		// Scale up: limit to +50% or +2 replicas, whichever is larger
		maxScaleUp := int32(math.Max(2, float64(currentReplicas)*0.5))
		if desiredReplicas > currentReplicas+maxScaleUp {
			desiredReplicas = currentReplicas + maxScaleUp
		}
	} else if desiredReplicas < currentReplicas {
		// Scale down: limit to -25% or -1 replica, whichever is larger (more conservative)
		maxScaleDown := int32(math.Max(1, float64(currentReplicas)*0.25))
		if desiredReplicas < currentReplicas-maxScaleDown {
			desiredReplicas = currentReplicas - maxScaleDown
		}
	}
	return desiredReplicas
}

// ScaleDirection constants for tracking scaling direction
const (
	ScaleDirectionUp   = "up"
	ScaleDirectionDown = "down"
)

// canScale checks if scaling is allowed based on cooldown periods
// It uses the LastScaleDirection field to determine which cooldown to apply
func (a *AutoScaler) canScale(cluster *antflyv1.AntflyCluster, autoScaling *antflyv1.AutoScalingSpec) bool {
	if cluster.Status.AutoScalingStatus == nil || cluster.Status.AutoScalingStatus.LastScaleTime == nil {
		return true
	}

	lastScaleTime := cluster.Status.AutoScalingStatus.LastScaleTime.Time
	lastDirection := cluster.Status.AutoScalingStatus.LastScaleDirection
	now := time.Now()

	// Determine cooldown based on the last scaling direction
	var cooldownDuration time.Duration
	switch lastDirection {
	case ScaleDirectionUp:
		if autoScaling.ScaleUpCooldown != nil {
			cooldownDuration = autoScaling.ScaleUpCooldown.Duration
		} else {
			cooldownDuration = 60 * time.Second // default
		}
	case ScaleDirectionDown:
		if autoScaling.ScaleDownCooldown != nil {
			cooldownDuration = autoScaling.ScaleDownCooldown.Duration
		} else {
			cooldownDuration = 300 * time.Second // default
		}
	default:
		// No previous scaling direction recorded, allow scaling
		return true
	}

	return now.Sub(lastScaleTime) >= cooldownDuration
}

// UpdateScalingStatus updates the autoscaling status after a scaling decision
func (a *AutoScaler) UpdateScalingStatus(cluster *antflyv1.AntflyCluster, desiredReplicas int32) {
	if cluster.Status.AutoScalingStatus == nil {
		cluster.Status.AutoScalingStatus = &antflyv1.AutoScalingStatus{}
	}

	currentReplicas := cluster.Spec.DataNodes.Replicas
	cluster.Status.AutoScalingStatus.DesiredReplicas = desiredReplicas

	// Update last scale time and direction if replicas changed
	if desiredReplicas != currentReplicas {
		now := metav1.NewTime(time.Now())
		cluster.Status.AutoScalingStatus.LastScaleTime = &now

		// Record the scaling direction for cooldown tracking
		if desiredReplicas > currentReplicas {
			cluster.Status.AutoScalingStatus.LastScaleDirection = ScaleDirectionUp
		} else {
			cluster.Status.AutoScalingStatus.LastScaleDirection = ScaleDirectionDown
		}
	}
}
