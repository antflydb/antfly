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

// Package v1alpha1 contains API Schema definitions for the antfly v1alpha1 API group
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TermiteRouteSpec defines the desired state of TermiteRoute
type TermiteRouteSpec struct {
	// Priority determines the order routes are evaluated (higher = first)
	// Routes with the same priority are evaluated in alphabetical order
	// +kubebuilder:default=100
	Priority int32 `json:"priority,omitempty"`

	// Match defines when this route applies
	Match RouteMatch `json:"match"`

	// Route defines where to send matching requests
	Route []RouteDestination `json:"route"`

	// Fallback defines behavior when all destinations are unavailable
	// +optional
	Fallback *RouteFallback `json:"fallback,omitempty"`

	// RateLimiting applies rate limits to this route
	// +optional
	RateLimiting *RouteRateLimiting `json:"rateLimiting,omitempty"`

	// Retry configures retry behavior for this route
	// +optional
	Retry *RouteRetry `json:"retry,omitempty"`
}

// RouteMatch defines the conditions for a route to match
type RouteMatch struct {
	// Operations matches specific API operations
	// +optional
	Operations []OperationType `json:"operations,omitempty"`

	// Models matches model names (supports wildcards: "bge-*", "*-rerank-*")
	// +optional
	Models []string `json:"models,omitempty"`

	// Headers matches request headers
	// +optional
	Headers map[string]StringMatch `json:"headers,omitempty"`

	// Source matches the source of the request (e.g., specific Antfly tables)
	// +optional
	Source *SourceMatch `json:"source,omitempty"`

	// TimeWindow restricts when this route is active
	// +optional
	TimeWindow *TimeWindowMatch `json:"timeWindow,omitempty"`
}

// OperationType represents a Termite API operation
type OperationType string

const (
	OperationEmbed  OperationType = "embed"
	OperationChunk  OperationType = "chunk"
	OperationRerank OperationType = "rerank"
)

// StringMatch defines how to match a string value
type StringMatch struct {
	// Exact matches the exact value
	// +optional
	Exact string `json:"exact,omitempty"`

	// Prefix matches a prefix
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// Regex matches a regular expression
	// +optional
	Regex string `json:"regex,omitempty"`
}

// SourceMatch matches the request source
type SourceMatch struct {
	// Tables matches requests from specific Antfly tables
	// +optional
	Tables []string `json:"tables,omitempty"`

	// Organizations matches requests authenticated for specific hosted organizations.
	// +optional
	Organizations []string `json:"organizations,omitempty"`

	// Projects matches requests authenticated for specific hosted projects.
	// +optional
	Projects []string `json:"projects,omitempty"`

	// APIKeyPrefixes matches requests authenticated with specific hosted API key prefixes.
	// +optional
	APIKeyPrefixes []string `json:"apiKeyPrefixes,omitempty"`

	// Namespaces is reserved for future authenticated source identity support.
	// Requests that set this field are currently rejected.
	// +optional
	Namespaces []string `json:"namespaces,omitempty"`

	// ServiceAccounts is reserved for future authenticated source identity support.
	// Requests that set this field are currently rejected.
	// +optional
	ServiceAccounts []string `json:"serviceAccounts,omitempty"`
}

// TimeWindowMatch restricts when a route is active
type TimeWindowMatch struct {
	// Start is the start time in HH:MM format (UTC)
	Start string `json:"start"`

	// End is the end time in HH:MM format (UTC)
	End string `json:"end"`

	// Days restricts to specific days (0=Sunday, 6=Saturday)
	// +optional
	Days []int `json:"days,omitempty"`
}

// RouteDestination defines a destination for requests
type RouteDestination struct {
	// Pool is the TermitePool to route to
	Pool string `json:"pool"`

	// Weight is the relative weight for this destination (0-100)
	// Used for traffic splitting between multiple destinations
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=100
	Weight int32 `json:"weight,omitempty"`

	// Condition makes this destination conditional
	// +optional
	Condition *RouteCondition `json:"condition,omitempty"`
}

// RouteCondition defines when a destination is eligible
type RouteCondition struct {
	// QueueDepth activates when queue depth matches
	// Supports operators: ">50", "<10", ">=100"
	// +optional
	QueueDepth string `json:"queueDepth,omitempty"`

	// AvailableReplicas activates when replica count matches
	// +optional
	AvailableReplicas string `json:"availableReplicas,omitempty"`

	// Latency activates when the rolling P99 latency matches (e.g., ">100ms")
	// +optional
	Latency string `json:"latency,omitempty"`

	// ModelLoaded activates only if the model is loaded on this pool
	// +optional
	ModelLoaded *bool `json:"modelLoaded,omitempty"`

	// TimeOfDay activates during specific hours
	// +optional
	TimeOfDay *TimeWindowMatch `json:"timeOfDay,omitempty"`
}

// RouteFallback defines fallback behavior
type RouteFallback struct {
	// Action is what to do when all destinations fail
	// +kubebuilder:validation:Enum=queue;reject;redirect
	Action FallbackAction `json:"action"`

	// MaxQueueTime is max time to queue before rejecting (for action=queue)
	// +optional
	MaxQueueTime *metav1.Duration `json:"maxQueueTime,omitempty"`

	// RedirectPool is the pool to redirect to (for action=redirect)
	// +optional
	RedirectPool string `json:"redirectPool,omitempty"`

	// ErrorResponse customizes the error response (for action=reject)
	// +optional
	ErrorResponse *ErrorResponseConfig `json:"errorResponse,omitempty"`
}

// FallbackAction defines fallback actions
type FallbackAction string

const (
	FallbackActionQueue    FallbackAction = "queue"
	FallbackActionReject   FallbackAction = "reject"
	FallbackActionRedirect FallbackAction = "redirect"
)

// ErrorResponseConfig customizes error responses
type ErrorResponseConfig struct {
	// StatusCode is the HTTP status code
	// +kubebuilder:default=503
	StatusCode int32 `json:"statusCode,omitempty"`

	// Message is the error message
	// +optional
	Message string `json:"message,omitempty"`

	// RetryAfter suggests when to retry (seconds)
	// +optional
	RetryAfter *int32 `json:"retryAfter,omitempty"`
}

// RouteRateLimiting configures rate limiting
type RouteRateLimiting struct {
	// RequestsPerSecond limits requests per second
	RequestsPerSecond int32 `json:"requestsPerSecond"`

	// BurstSize allows temporary bursts
	// +optional
	BurstSize *int32 `json:"burstSize,omitempty"`

	// PerModel applies limits per model (vs global)
	// +optional
	PerModel bool `json:"perModel,omitempty"`
}

// RetryCondition defines a condition that triggers a retry
type RetryCondition string

const (
	RetryOn5xx               RetryCondition = "5xx"
	RetryOnReset             RetryCondition = "reset"
	RetryOnConnectFailure    RetryCondition = "connect-failure"
	RetryOnRetriable4xx      RetryCondition = "retriable-4xx"
	RetryOnRefusedStream     RetryCondition = "refused-stream"
	RetryOnCancelled         RetryCondition = "cancelled"
	RetryOnDeadlineExceeded  RetryCondition = "deadline-exceeded"
	RetryOnResourceExhausted RetryCondition = "resource-exhausted"
)

// ValidRetryConditions contains all valid retry condition values.
var ValidRetryConditions = []RetryCondition{
	RetryOn5xx, RetryOnReset, RetryOnConnectFailure, RetryOnRetriable4xx,
	RetryOnRefusedStream, RetryOnCancelled, RetryOnDeadlineExceeded, RetryOnResourceExhausted,
}

// RouteRetry configures retry behavior
type RouteRetry struct {
	// Attempts is the max retry attempts
	// +kubebuilder:default=3
	Attempts int32 `json:"attempts,omitempty"`

	// PerTryTimeout is the timeout per attempt
	// +optional
	PerTryTimeout *metav1.Duration `json:"perTryTimeout,omitempty"`

	// RetryOn specifies which errors trigger retries
	// +optional
	RetryOn []RetryCondition `json:"retryOn,omitempty"`
}

// TermiteRouteStatus defines the observed state of TermiteRoute
type TermiteRouteStatus struct {
	// Active indicates if the route is currently active
	Active bool `json:"active,omitempty"`

	// ObservedGeneration is the most recent generation observed by the controller.
	// Used to skip validation when the spec has not changed.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Priority",type=integer,JSONPath=`.spec.priority`
// +kubebuilder:printcolumn:name="Active",type=boolean,JSONPath=`.status.active`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TermiteRoute is the Schema for the termiteroutes API
type TermiteRoute struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec TermiteRouteSpec `json:"spec,omitempty"`
	// +optional
	Status TermiteRouteStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TermiteRouteList contains a list of TermiteRoute
type TermiteRouteList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TermiteRoute `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TermiteRoute{}, &TermiteRouteList{})
}
