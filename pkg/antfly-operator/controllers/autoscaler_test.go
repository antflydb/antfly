package controllers

import (
	"testing"
	"time"

	antflyv1 "github.com/antflydb/antfly/pkg/antfly-operator/api/v1"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Helper to create int32 pointers
//
//go:fix inline
func int32Ptr(i int32) *int32 {
	return new(i)
}

func TestCalculateDesiredReplicas_ScaleUp_CPUOnly(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:                        true,
		MinReplicas:                    3,
		MaxReplicas:                    10,
		TargetCPUUtilizationPercentage: int32Ptr(70),
	}

	// Current: 3 replicas at 100% CPU utilization
	// Expected: ceil(3 * 100/70) = ceil(4.28) = 5
	metrics := &MetricsData{
		PodCount:                 3,
		CPUUtilizationPercentage: 100,
	}

	result := as.calculateDesiredReplicas(3, autoScaling, metrics)
	g.Expect(result).To(Equal(int32(5)), "Should scale up from 3 to 5 replicas")
}

func TestCalculateDesiredReplicas_ScaleDown_CPUOnly(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:                        true,
		MinReplicas:                    3,
		MaxReplicas:                    10,
		TargetCPUUtilizationPercentage: int32Ptr(70),
	}

	// Current: 6 replicas at 30% CPU utilization
	// Expected: ceil(6 * 30/70) = ceil(2.57) = 3
	// But with gradual scaling limit: max scale down is 25% or 1, so 6 - 1 = 5
	metrics := &MetricsData{
		PodCount:                 6,
		CPUUtilizationPercentage: 30,
	}

	result := as.calculateDesiredReplicas(6, autoScaling, metrics)
	g.Expect(result).To(BeNumerically("<", 6), "Should scale down from 6 replicas")
	g.Expect(result).To(BeNumerically(">=", 5), "Should not scale down more than 1 replica at a time")
}

func TestCalculateDesiredReplicas_ScaleUp_MemoryOnly(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:                           true,
		MinReplicas:                       3,
		MaxReplicas:                       10,
		TargetMemoryUtilizationPercentage: int32Ptr(80),
	}

	// Current: 3 replicas at 120% memory utilization
	// Expected: ceil(3 * 120/80) = ceil(4.5) = 5
	metrics := &MetricsData{
		PodCount:                    3,
		MemoryUtilizationPercentage: 120,
	}

	result := as.calculateDesiredReplicas(3, autoScaling, metrics)
	g.Expect(result).To(Equal(int32(5)), "Should scale up from 3 to 5 replicas based on memory")
}

func TestCalculateDesiredReplicas_ScaleDown_MemoryOnly(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:                           true,
		MinReplicas:                       3,
		MaxReplicas:                       10,
		TargetMemoryUtilizationPercentage: int32Ptr(80),
	}

	// Current: 8 replicas at 40% memory utilization
	// Expected: ceil(8 * 40/80) = ceil(4) = 4
	// But with gradual scaling limit: max scale down is 25% of 8 = 2, so 8 - 2 = 6
	metrics := &MetricsData{
		PodCount:                    8,
		MemoryUtilizationPercentage: 40,
	}

	result := as.calculateDesiredReplicas(8, autoScaling, metrics)
	g.Expect(result).To(BeNumerically("<", 8), "Should scale down from 8 replicas")
	g.Expect(result).To(BeNumerically(">=", 6), "Should not scale down more than 25% at a time")
}

func TestCalculateDesiredReplicas_BothMetrics_ScaleUpOnHigherResource(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:                           true,
		MinReplicas:                       3,
		MaxReplicas:                       10,
		TargetCPUUtilizationPercentage:    int32Ptr(70),
		TargetMemoryUtilizationPercentage: int32Ptr(80),
	}

	// CPU wants to scale to ceil(3 * 100/70) = 5
	// Memory wants to scale to ceil(3 * 60/80) = 3 (stay same)
	// Should use CPU (higher) = 5
	metrics := &MetricsData{
		PodCount:                    3,
		CPUUtilizationPercentage:    100,
		MemoryUtilizationPercentage: 60,
	}

	result := as.calculateDesiredReplicas(3, autoScaling, metrics)
	g.Expect(result).To(Equal(int32(5)), "Should scale up based on CPU (the constrained resource)")
}

func TestCalculateDesiredReplicas_BothMetrics_ScaleDownWhenBothLow(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:                           true,
		MinReplicas:                       3,
		MaxReplicas:                       10,
		TargetCPUUtilizationPercentage:    int32Ptr(70),
		TargetMemoryUtilizationPercentage: int32Ptr(80),
	}

	// CPU wants to scale to ceil(6 * 30/70) = 3
	// Memory wants to scale to ceil(6 * 35/80) = 3
	// Both want to scale down, should scale down
	metrics := &MetricsData{
		PodCount:                    6,
		CPUUtilizationPercentage:    30,
		MemoryUtilizationPercentage: 35,
	}

	result := as.calculateDesiredReplicas(6, autoScaling, metrics)
	g.Expect(result).To(BeNumerically("<", 6), "Should scale down when both metrics are below target")
}

func TestCalculateDesiredReplicas_BothMetrics_NoScaleDownWhenOneHigh(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:                           true,
		MinReplicas:                       3,
		MaxReplicas:                       10,
		TargetCPUUtilizationPercentage:    int32Ptr(70),
		TargetMemoryUtilizationPercentage: int32Ptr(80),
	}

	// CPU wants to scale to ceil(6 * 30/70) = 3 (scale down)
	// Memory wants to scale to ceil(6 * 75/80) = 6 (stay same)
	// Should NOT scale down because memory is still near target
	metrics := &MetricsData{
		PodCount:                    6,
		CPUUtilizationPercentage:    30,
		MemoryUtilizationPercentage: 75,
	}

	result := as.calculateDesiredReplicas(6, autoScaling, metrics)
	g.Expect(result).To(Equal(int32(6)), "Should not scale down when memory is near target")
}

func TestCalculateDesiredReplicas_NoTargetsConfigured(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 3,
		MaxReplicas: 10,
		// No targets configured
	}

	metrics := &MetricsData{
		PodCount:                    5,
		CPUUtilizationPercentage:    100,
		MemoryUtilizationPercentage: 100,
	}

	result := as.calculateDesiredReplicas(5, autoScaling, metrics)
	g.Expect(result).To(Equal(int32(5)), "Should maintain current replicas when no targets configured")
}

func TestCalculateDesiredReplicas_ZeroPodCount(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:                        true,
		MinReplicas:                    3,
		MaxReplicas:                    10,
		TargetCPUUtilizationPercentage: int32Ptr(70),
	}

	metrics := &MetricsData{
		PodCount: 0,
	}

	result := as.calculateDesiredReplicas(3, autoScaling, metrics)
	g.Expect(result).To(Equal(int32(3)), "Should maintain current replicas when pod count is zero")
}

func TestApplyScalingLimits_ScaleUp(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	// Scale up from 4 to 10 should be limited to +50% = 4 + 2 = 6
	result := as.applyScalingLimits(4, 10)
	g.Expect(result).To(Equal(int32(6)), "Scale up should be limited to +50%")

	// Scale up from 2 to 10 should be limited to +2 (since 50% of 2 = 1, max(2,1) = 2)
	result = as.applyScalingLimits(2, 10)
	g.Expect(result).To(Equal(int32(4)), "Scale up should be limited to +2 minimum")
}

func TestApplyScalingLimits_ScaleDown(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	// Scale down from 8 to 2 should be limited to -25% = 8 - 2 = 6
	result := as.applyScalingLimits(8, 2)
	g.Expect(result).To(Equal(int32(6)), "Scale down should be limited to -25%")

	// Scale down from 3 to 1 should be limited to -1 (since 25% of 3 = 0.75, max(1, 0.75) = 1)
	result = as.applyScalingLimits(3, 1)
	g.Expect(result).To(Equal(int32(2)), "Scale down should be limited to -1 minimum")
}

func TestApplyScalingLimits_NoChange(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	result := as.applyScalingLimits(5, 5)
	g.Expect(result).To(Equal(int32(5)), "No change when current equals desired")
}

func TestCanScale_NoStatus(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			AutoScalingStatus: nil,
		},
	}

	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 3,
		MaxReplicas: 10,
	}

	result := as.canScale(cluster, autoScaling)
	g.Expect(result).To(BeTrue(), "Should allow scaling when no status exists")
}

func TestCanScale_NoLastScaleTime(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			AutoScalingStatus: &antflyv1.AutoScalingStatus{
				CurrentReplicas: 3,
				DesiredReplicas: 3,
				LastScaleTime:   nil,
			},
		},
	}

	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 3,
		MaxReplicas: 10,
	}

	result := as.canScale(cluster, autoScaling)
	g.Expect(result).To(BeTrue(), "Should allow scaling when no last scale time exists")
}

func TestCanScale_ScaleUpCooldown(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	// Last scale was 30 seconds ago, cooldown is 60 seconds
	lastScale := metav1.NewTime(time.Now().Add(-30 * time.Second))

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 5,
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			AutoScalingStatus: &antflyv1.AutoScalingStatus{
				CurrentReplicas:    5,
				DesiredReplicas:    5,
				LastScaleTime:      &lastScale,
				LastScaleDirection: ScaleDirectionUp,
			},
		},
	}

	cooldown := metav1.Duration{Duration: 60 * time.Second}
	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:         true,
		MinReplicas:     3,
		MaxReplicas:     10,
		ScaleUpCooldown: &cooldown,
	}

	result := as.canScale(cluster, autoScaling)
	g.Expect(result).To(BeFalse(), "Should not allow scaling during scale-up cooldown")
}

func TestCanScale_ScaleUpCooldownExpired(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	// Last scale was 90 seconds ago, cooldown is 60 seconds
	lastScale := metav1.NewTime(time.Now().Add(-90 * time.Second))

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 5,
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			AutoScalingStatus: &antflyv1.AutoScalingStatus{
				CurrentReplicas:    5,
				DesiredReplicas:    5,
				LastScaleTime:      &lastScale,
				LastScaleDirection: ScaleDirectionUp,
			},
		},
	}

	cooldown := metav1.Duration{Duration: 60 * time.Second}
	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:         true,
		MinReplicas:     3,
		MaxReplicas:     10,
		ScaleUpCooldown: &cooldown,
	}

	result := as.canScale(cluster, autoScaling)
	g.Expect(result).To(BeTrue(), "Should allow scaling after scale-up cooldown expires")
}

func TestCanScale_ScaleDownCooldown(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	// Last scale was 100 seconds ago, cooldown is 300 seconds
	lastScale := metav1.NewTime(time.Now().Add(-100 * time.Second))

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 5,
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			AutoScalingStatus: &antflyv1.AutoScalingStatus{
				CurrentReplicas:    5,
				DesiredReplicas:    5,
				LastScaleTime:      &lastScale,
				LastScaleDirection: ScaleDirectionDown,
			},
		},
	}

	cooldown := metav1.Duration{Duration: 300 * time.Second}
	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:           true,
		MinReplicas:       3,
		MaxReplicas:       10,
		ScaleDownCooldown: &cooldown,
	}

	result := as.canScale(cluster, autoScaling)
	g.Expect(result).To(BeFalse(), "Should not allow scaling during scale-down cooldown")
}

func TestCanScale_ScaleDownCooldownExpired(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	// Last scale was 400 seconds ago, cooldown is 300 seconds
	lastScale := metav1.NewTime(time.Now().Add(-400 * time.Second))

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 5,
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			AutoScalingStatus: &antflyv1.AutoScalingStatus{
				CurrentReplicas:    5,
				DesiredReplicas:    5,
				LastScaleTime:      &lastScale,
				LastScaleDirection: ScaleDirectionDown,
			},
		},
	}

	cooldown := metav1.Duration{Duration: 300 * time.Second}
	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:           true,
		MinReplicas:       3,
		MaxReplicas:       10,
		ScaleDownCooldown: &cooldown,
	}

	result := as.canScale(cluster, autoScaling)
	g.Expect(result).To(BeTrue(), "Should allow scaling after scale-down cooldown expires")
}

func TestCanScale_DefaultCooldowns(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	// Test default scale-up cooldown (60s)
	lastScale := metav1.NewTime(time.Now().Add(-30 * time.Second))

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 5,
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			AutoScalingStatus: &antflyv1.AutoScalingStatus{
				CurrentReplicas:    5,
				DesiredReplicas:    5,
				LastScaleTime:      &lastScale,
				LastScaleDirection: ScaleDirectionUp,
			},
		},
	}

	autoScaling := &antflyv1.AutoScalingSpec{
		Enabled:     true,
		MinReplicas: 3,
		MaxReplicas: 10,
		// No cooldowns specified, should use defaults
	}

	result := as.canScale(cluster, autoScaling)
	g.Expect(result).To(BeFalse(), "Should use default 60s scale-up cooldown")

	// Test default scale-down cooldown (300s)
	cluster.Status.AutoScalingStatus.LastScaleDirection = ScaleDirectionDown
	cluster.Status.AutoScalingStatus.LastScaleTime = &lastScale

	result = as.canScale(cluster, autoScaling)
	g.Expect(result).To(BeFalse(), "Should use default 300s scale-down cooldown")
}

func TestUpdateScalingStatus_ScaleUp(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
		},
		Status: antflyv1.AntflyClusterStatus{},
	}

	as.UpdateScalingStatus(cluster, 5)

	g.Expect(cluster.Status.AutoScalingStatus).NotTo(BeNil())
	g.Expect(cluster.Status.AutoScalingStatus.DesiredReplicas).To(Equal(int32(5)))
	g.Expect(cluster.Status.AutoScalingStatus.LastScaleTime).NotTo(BeNil())
	g.Expect(cluster.Status.AutoScalingStatus.LastScaleDirection).To(Equal(ScaleDirectionUp))
}

func TestUpdateScalingStatus_ScaleDown(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 5,
			},
		},
		Status: antflyv1.AntflyClusterStatus{},
	}

	as.UpdateScalingStatus(cluster, 3)

	g.Expect(cluster.Status.AutoScalingStatus).NotTo(BeNil())
	g.Expect(cluster.Status.AutoScalingStatus.DesiredReplicas).To(Equal(int32(3)))
	g.Expect(cluster.Status.AutoScalingStatus.LastScaleTime).NotTo(BeNil())
	g.Expect(cluster.Status.AutoScalingStatus.LastScaleDirection).To(Equal(ScaleDirectionDown))
}

func TestUpdateScalingStatus_NoChange(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 5,
			},
		},
		Status: antflyv1.AntflyClusterStatus{},
	}

	as.UpdateScalingStatus(cluster, 5)

	g.Expect(cluster.Status.AutoScalingStatus).NotTo(BeNil())
	g.Expect(cluster.Status.AutoScalingStatus.DesiredReplicas).To(Equal(int32(5)))
	g.Expect(cluster.Status.AutoScalingStatus.LastScaleTime).To(BeNil(), "Should not update scale time when no change")
	g.Expect(cluster.Status.AutoScalingStatus.LastScaleDirection).To(BeEmpty(), "Should not update direction when no change")
}

func TestUpdateScalingStatus_PreservesExistingStatus(t *testing.T) {
	g := NewWithT(t)
	as := &AutoScaler{}

	existingTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	cpuUtil := int32(50)
	memUtil := int32(60)

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 5,
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			AutoScalingStatus: &antflyv1.AutoScalingStatus{
				CurrentReplicas:                    5,
				DesiredReplicas:                    5,
				LastScaleTime:                      &existingTime,
				LastScaleDirection:                 ScaleDirectionUp,
				CurrentCPUUtilizationPercentage:    &cpuUtil,
				CurrentMemoryUtilizationPercentage: &memUtil,
			},
		},
	}

	// Update with new desired replicas
	as.UpdateScalingStatus(cluster, 7)

	// Should update scale time and direction
	g.Expect(cluster.Status.AutoScalingStatus.DesiredReplicas).To(Equal(int32(7)))
	g.Expect(cluster.Status.AutoScalingStatus.LastScaleDirection).To(Equal(ScaleDirectionUp))
	g.Expect(cluster.Status.AutoScalingStatus.LastScaleTime).NotTo(Equal(&existingTime))

	// Should preserve utilization percentages
	g.Expect(cluster.Status.AutoScalingStatus.CurrentCPUUtilizationPercentage).To(Equal(&cpuUtil))
	g.Expect(cluster.Status.AutoScalingStatus.CurrentMemoryUtilizationPercentage).To(Equal(&memUtil))
}
