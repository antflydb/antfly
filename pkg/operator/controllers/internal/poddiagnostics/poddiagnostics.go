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

package poddiagnostics

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

type FindingType string

const (
	FindingUnschedulable   FindingType = "Unschedulable"
	FindingImagePullFailed FindingType = "ImagePullFailed"
	FindingInitFailed      FindingType = "InitContainerFailed"
	FindingModelPullFailed FindingType = "ModelPullFailed"
	FindingCrashLooping    FindingType = "CrashLooping"
	FindingProbeFailed     FindingType = "ProbeFailed"
	FindingNotReady        FindingType = "NotReady"
)

type Finding struct {
	Type      FindingType
	Pod       string
	Container string
	Reason    string
	Message   string
}

func DiagnosePods(pods []corev1.Pod) []Finding {
	var findings []Finding
	for _, pod := range pods {
		findings = append(findings, diagnosePod(pod)...)
	}
	return findings
}

func Has(findings []Finding, findingTypes ...FindingType) bool {
	for _, finding := range findings {
		for _, findingType := range findingTypes {
			if finding.Type == findingType {
				return true
			}
		}
	}
	return false
}

func First(findings []Finding, findingTypes ...FindingType) (Finding, bool) {
	for _, finding := range findings {
		for _, findingType := range findingTypes {
			if finding.Type == findingType {
				return finding, true
			}
		}
	}
	return Finding{}, false
}

func Message(finding Finding) string {
	var parts []string
	if finding.Pod != "" {
		parts = append(parts, "pod "+finding.Pod)
	}
	if finding.Container != "" {
		parts = append(parts, "container "+finding.Container)
	}
	if finding.Reason != "" {
		parts = append(parts, "reason "+finding.Reason)
	}
	if finding.Message != "" {
		parts = append(parts, finding.Message)
	}
	if len(parts) == 0 {
		return string(finding.Type)
	}
	return strings.Join(parts, ": ")
}

func Summary(findings []Finding) string {
	if len(findings) == 0 {
		return ""
	}
	return Message(findings[0])
}

func diagnosePod(pod corev1.Pod) []Finding {
	var findings []Finding

	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled &&
			condition.Status == corev1.ConditionFalse &&
			condition.Reason == corev1.PodReasonUnschedulable {
			findings = append(findings, Finding{
				Type:    FindingUnschedulable,
				Pod:     pod.Name,
				Reason:  condition.Reason,
				Message: condition.Message,
			})
		}
		if condition.Type == corev1.PodReady &&
			condition.Status == corev1.ConditionFalse &&
			isProbeMessage(condition.Message) {
			findings = append(findings, Finding{
				Type:    FindingProbeFailed,
				Pod:     pod.Name,
				Reason:  condition.Reason,
				Message: condition.Message,
			})
		}
	}

	for _, status := range pod.Status.InitContainerStatuses {
		findings = append(findings, diagnoseContainerStatus(pod.Name, status, true)...)
	}
	for _, status := range pod.Status.ContainerStatuses {
		findings = append(findings, diagnoseContainerStatus(pod.Name, status, false)...)
	}

	if pod.Status.Phase == corev1.PodFailed {
		findings = append(findings, Finding{
			Type:    FindingNotReady,
			Pod:     pod.Name,
			Reason:  string(pod.Status.Phase),
			Message: pod.Status.Message,
		})
	}

	return findings
}

func diagnoseContainerStatus(podName string, status corev1.ContainerStatus, init bool) []Finding {
	var findings []Finding
	if status.State.Waiting != nil {
		reason := status.State.Waiting.Reason
		message := status.State.Waiting.Message
		switch {
		case isImagePullReason(reason):
			findings = append(findings, Finding{
				Type:      FindingImagePullFailed,
				Pod:       podName,
				Container: status.Name,
				Reason:    reason,
				Message:   message,
			})
		case reason == "CrashLoopBackOff":
			findingType := FindingCrashLooping
			if init {
				findingType = FindingInitFailed
				if isModelPuller(status.Name) {
					findingType = FindingModelPullFailed
				}
			}
			findings = append(findings, Finding{
				Type:      findingType,
				Pod:       podName,
				Container: status.Name,
				Reason:    reason,
				Message:   message,
			})
		case init && isContainerFailureReason(reason):
			findingType := FindingInitFailed
			if isModelPuller(status.Name) {
				findingType = FindingModelPullFailed
			}
			findings = append(findings, Finding{
				Type:      findingType,
				Pod:       podName,
				Container: status.Name,
				Reason:    reason,
				Message:   message,
			})
		}
	}

	if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 {
		findingType := FindingCrashLooping
		if init {
			findingType = FindingInitFailed
			if isModelPuller(status.Name) {
				findingType = FindingModelPullFailed
			}
		}
		findings = append(findings, Finding{
			Type:      findingType,
			Pod:       podName,
			Container: status.Name,
			Reason:    status.State.Terminated.Reason,
			Message:   terminatedMessage(status.State.Terminated),
		})
	}

	return findings
}

func isImagePullReason(reason string) bool {
	return reason == "ErrImagePull" || reason == "ImagePullBackOff" || reason == "InvalidImageName"
}

func isContainerFailureReason(reason string) bool {
	switch reason {
	case "CreateContainerConfigError",
		"CreateContainerError",
		"RunContainerError",
		"StartError",
		"Error":
		return true
	default:
		return false
	}
}

func isModelPuller(name string) bool {
	return strings.HasPrefix(name, "model-puller")
}

func isProbeMessage(message string) bool {
	message = strings.ToLower(message)
	return strings.Contains(message, "probe failed") ||
		strings.Contains(message, "readiness probe") ||
		strings.Contains(message, "liveness probe") ||
		strings.Contains(message, "startup probe")
}

func terminatedMessage(state *corev1.ContainerStateTerminated) string {
	if state == nil {
		return ""
	}
	if state.Message != "" {
		return state.Message
	}
	if state.Signal != 0 {
		return fmt.Sprintf("exit code %d, signal %d", state.ExitCode, state.Signal)
	}
	return fmt.Sprintf("exit code %d", state.ExitCode)
}
