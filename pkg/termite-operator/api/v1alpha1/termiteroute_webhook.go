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

package v1alpha1

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
)

// timeFormatPattern matches HH:MM time format (00:00 - 23:59).
var timeFormatPattern = regexp.MustCompile(`^([01]?[0-9]|2[0-3]):([0-5][0-9])$`)

// ValidateCreate validates the TermiteRoute configuration when creating a new route.
// Called by controller fallback when webhooks are disabled.
func (r *TermiteRoute) ValidateCreate() error {
	return r.ValidateTermiteRoute()
}

// ValidateUpdate validates the TermiteRoute configuration when updating an existing route.
// Called by controller fallback when webhooks are disabled (note: controllers cannot
// provide the old object, so this is only called by the deprecated webhook interface).
func (r *TermiteRoute) ValidateUpdate(old runtime.Object) error {
	return r.ValidateTermiteRoute()
}

// ValidateTermiteRoute performs all validation checks
func (r *TermiteRoute) ValidateTermiteRoute() error {
	var allErrors []string

	if err := r.validateRouteDestinations(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateMatch(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateFallback(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateRateLimiting(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateRetry(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("TermiteRoute validation failed:\n  - %s",
			strings.Join(allErrors, "\n  - "))
	}

	return nil
}

// validateRouteDestinations validates that route destinations are properly configured
func (r *TermiteRoute) validateRouteDestinations() error {
	if len(r.Spec.Route) == 0 {
		return fmt.Errorf("spec.route must have at least one destination")
	}

	poolNames := make(map[string]bool)

	for i, dest := range r.Spec.Route {
		if dest.Pool == "" {
			return fmt.Errorf("spec.route[%d].pool is required", i)
		}

		// Check for duplicate pools without conditions
		if dest.Condition == nil {
			if poolNames[dest.Pool] {
				return fmt.Errorf("duplicate pool '%s' in route destinations without conditions", dest.Pool)
			}
			poolNames[dest.Pool] = true
		}

		// Validate weight range
		if dest.Weight < 0 || dest.Weight > 100 {
			return fmt.Errorf("spec.route[%d].weight must be between 0 and 100, got %d", i, dest.Weight)
		}
	}

	return nil
}

// validateMatch validates the route match configuration
func (r *TermiteRoute) validateMatch() error {
	match := r.Spec.Match

	// Validate operations
	validOps := map[OperationType]bool{
		OperationEmbed:  true,
		OperationChunk:  true,
		OperationRerank: true,
	}
	for _, op := range match.Operations {
		if !validOps[op] {
			return fmt.Errorf("invalid operation '%s'. Must be one of: embed, chunk, rerank", op)
		}
	}

	// Validate model patterns (wildcards)
	for i, pattern := range match.Models {
		if pattern == "" {
			return fmt.Errorf("spec.match.models[%d] cannot be empty", i)
		}
		// Validate wildcard patterns are valid glob patterns
		if strings.Contains(pattern, "*") {
			// Convert glob to regex to validate
			regexPattern := strings.ReplaceAll(pattern, "*", ".*")
			if _, err := regexp.Compile("^" + regexPattern + "$"); err != nil {
				return fmt.Errorf("invalid model pattern '%s': %v", pattern, err)
			}
		}
	}

	// Validate time window
	if match.TimeWindow != nil {
		if err := validateTimeWindow(match.TimeWindow); err != nil {
			return fmt.Errorf("spec.match.timeWindow: %w", err)
		}
	}

	// Validate header matchers
	for header, matcher := range match.Headers {
		if header == "" {
			return fmt.Errorf("header name cannot be empty in spec.match.headers")
		}
		// Ensure at least one match type is specified
		if matcher.Exact == "" && matcher.Prefix == "" && matcher.Regex == "" {
			return fmt.Errorf("header matcher for '%s' must specify at least one of: exact, prefix, or regex", header)
		}
		// Validate regex if specified
		if matcher.Regex != "" {
			if _, err := regexp.Compile(matcher.Regex); err != nil {
				return fmt.Errorf("invalid regex for header '%s': %v", header, err)
			}
		}
	}

	return nil
}

// validateTimeWindow validates time window configuration
func validateTimeWindow(tw *TimeWindowMatch) error {
	if tw.Start != "" && !timeFormatPattern.MatchString(tw.Start) {
		return fmt.Errorf("start time '%s' is not in HH:MM format", tw.Start)
	}

	if tw.End != "" && !timeFormatPattern.MatchString(tw.End) {
		return fmt.Errorf("end time '%s' is not in HH:MM format", tw.End)
	}

	// Validate days (0-6)
	for _, day := range tw.Days {
		if day < 0 || day > 6 {
			return fmt.Errorf("invalid day %d. Days must be 0 (Sunday) through 6 (Saturday)", day)
		}
	}

	return nil
}

// validateFallback validates fallback configuration
func (r *TermiteRoute) validateFallback() error {
	if r.Spec.Fallback == nil {
		return nil
	}

	fb := r.Spec.Fallback

	// Validate action
	validActions := map[FallbackAction]bool{
		FallbackActionQueue:    true,
		FallbackActionReject:   true,
		FallbackActionRedirect: true,
	}
	if !validActions[fb.Action] {
		return fmt.Errorf("invalid fallback action '%s'. Must be one of: queue, reject, redirect", fb.Action)
	}

	// Validate redirect pool is specified when action is redirect
	if fb.Action == FallbackActionRedirect && fb.RedirectPool == "" {
		return fmt.Errorf("spec.fallback.redirectPool is required when action is 'redirect'")
	}

	// Note: maxQueueTime is optional when action is queue - proxy will use default if not specified.

	return nil
}

// validateRateLimiting validates rate limiting configuration
func (r *TermiteRoute) validateRateLimiting() error {
	if r.Spec.RateLimiting == nil {
		return nil
	}

	rl := r.Spec.RateLimiting

	if rl.RequestsPerSecond <= 0 {
		return fmt.Errorf("spec.rateLimiting.requestsPerSecond must be > 0, got %d", rl.RequestsPerSecond)
	}

	if rl.BurstSize != nil && *rl.BurstSize < 0 {
		return fmt.Errorf("spec.rateLimiting.burstSize must be >= 0, got %d", *rl.BurstSize)
	}

	return nil
}

// validateRetry validates retry configuration
func (r *TermiteRoute) validateRetry() error {
	if r.Spec.Retry == nil {
		return nil
	}

	retry := r.Spec.Retry

	if retry.Attempts < 0 {
		return fmt.Errorf("spec.retry.attempts must be >= 0, got %d", retry.Attempts)
	}

	// Validate retryOn values
	for _, condition := range retry.RetryOn {
		if !slices.Contains(ValidRetryConditions, condition) {
			names := make([]string, len(ValidRetryConditions))
			for i, c := range ValidRetryConditions {
				names[i] = string(c)
			}
			return fmt.Errorf("invalid retry condition '%s'. Valid values: %s", condition, strings.Join(names, ", "))
		}
	}

	return nil
}
