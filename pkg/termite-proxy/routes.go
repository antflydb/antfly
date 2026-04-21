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

// Package proxy implements route matching for TermiteRoute CRDs.
package proxy

import (
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Route represents a compiled TermiteRoute for fast matching
type Route struct {
	Name     string
	Priority int32

	// Compiled matchers
	Operations     map[OperationType]bool
	ModelPatterns  []*regexp.Regexp
	HeaderMatchers map[string]*StringMatcher
	SourceTables   map[string]bool
	TimeWindow     *TimeWindow

	// Destinations
	Destinations []Destination

	// Fallback
	Fallback *Fallback

	// Rate limiting state
	RateLimiter *RateLimiter

	// Retry config
	RetryAttempts   int32
	RetryTimeout    time.Duration
	RetryOnStatuses map[int]bool

	// Stats
	MatchedRequests int64
	LastMatchTime   time.Time
}

// OperationType for matching
type OperationType string

// StringMatcher for header matching
type StringMatcher struct {
	Exact  string
	Prefix string
	Regex  *regexp.Regexp
}

func (m *StringMatcher) Matches(value string) bool {
	if m.Exact != "" && value == m.Exact {
		return true
	}
	if m.Prefix != "" && strings.HasPrefix(value, m.Prefix) {
		return true
	}
	if m.Regex != nil && m.Regex.MatchString(value) {
		return true
	}
	return false
}

// TimeWindow for time-based matching
type TimeWindow struct {
	StartHour   int
	StartMinute int
	EndHour     int
	EndMinute   int
	Days        map[int]bool // 0=Sunday, 6=Saturday
}

func (tw *TimeWindow) IsActive(t time.Time) bool {
	t = t.UTC()

	// Check day of week
	if len(tw.Days) > 0 {
		if !tw.Days[int(t.Weekday())] {
			return false
		}
	}

	// Check time of day
	currentMinutes := t.Hour()*60 + t.Minute()
	startMinutes := tw.StartHour*60 + tw.StartMinute
	endMinutes := tw.EndHour*60 + tw.EndMinute

	if startMinutes <= endMinutes {
		// Normal case: start before end (e.g., 09:00-17:00)
		return currentMinutes >= startMinutes && currentMinutes < endMinutes
	}
	// Overnight case: end before start (e.g., 22:00-06:00)
	return currentMinutes >= startMinutes || currentMinutes < endMinutes
}

// Destination represents a route destination
type Destination struct {
	Pool   string
	Weight int32

	// Conditions
	QueueDepthCondition *ThresholdCondition
	ReplicaCondition    *ThresholdCondition
	LatencyCondition    *ThresholdCondition
	RequireModelLoaded  bool
	TimeCondition       *TimeWindow
}

// ThresholdCondition for numeric comparisons
type ThresholdCondition struct {
	Operator string // ">", "<", ">=", "<=", "=="
	Value    float64
}

func (c *ThresholdCondition) Evaluate(value float64) bool {
	switch c.Operator {
	case ">":
		return value > c.Value
	case "<":
		return value < c.Value
	case ">=":
		return value >= c.Value
	case "<=":
		return value <= c.Value
	case "==":
		return value == c.Value
	}
	return false
}

// Fallback defines fallback behavior
type Fallback struct {
	Action       string // "queue", "reject", "redirect"
	MaxQueueTime time.Duration
	RedirectPool string
	StatusCode   int
	Message      string
	RetryAfter   int
}

// RateLimiter implements token bucket rate limiting
type RateLimiter struct {
	rate        float64
	burstSize   int
	tokens      float64
	lastUpdate  time.Time
	perModel    bool
	modelLimits map[string]*modelLimit

	mu sync.Mutex
}

type modelLimit struct {
	tokens     float64
	lastUpdate time.Time
}

func NewRateLimiter(rps int32, burst int32, perModel bool) *RateLimiter {
	return &RateLimiter{
		rate:        float64(rps),
		burstSize:   int(burst),
		tokens:      float64(burst),
		lastUpdate:  time.Now(),
		perModel:    perModel,
		modelLimits: make(map[string]*modelLimit),
	}
}

func (rl *RateLimiter) Allow(model string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	var tokens *float64
	var lastUpdate *time.Time

	if rl.perModel {
		ml, exists := rl.modelLimits[model]
		if !exists {
			ml = &modelLimit{
				tokens:     float64(rl.burstSize),
				lastUpdate: now,
			}
			rl.modelLimits[model] = ml
		}
		tokens = &ml.tokens
		lastUpdate = &ml.lastUpdate
	} else {
		tokens = &rl.tokens
		lastUpdate = &rl.lastUpdate
	}

	// Refill tokens
	elapsed := now.Sub(*lastUpdate).Seconds()
	*tokens += elapsed * rl.rate
	if *tokens > float64(rl.burstSize) {
		*tokens = float64(rl.burstSize)
	}
	*lastUpdate = now

	// Check if we have a token
	if *tokens >= 1 {
		*tokens--
		return true
	}
	return false
}

// RouteRequest contains information about a request for routing
type RouteRequest struct {
	Operation   OperationType
	Model       string
	Headers     map[string]string
	SourceTable string
	Timestamp   time.Time
}

// RouteManager manages all routes and performs matching
type RouteManager struct {
	routes []*Route // Sorted by priority (descending)
	mu     sync.RWMutex
}

// NewRouteManager creates a new RouteManager
func NewRouteManager() *RouteManager {
	return &RouteManager{
		routes: make([]*Route, 0),
	}
}

// AddRoute adds a route (routes are re-sorted by priority)
func (rm *RouteManager) AddRoute(route *Route) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Remove existing route with same name
	newRoutes := make([]*Route, 0, len(rm.routes)+1)
	for _, r := range rm.routes {
		if r.Name != route.Name {
			newRoutes = append(newRoutes, r)
		}
	}
	newRoutes = append(newRoutes, route)

	// Sort by priority (descending), then by name (ascending) for stable ordering
	sort.Slice(newRoutes, func(i, j int) bool {
		if newRoutes[i].Priority != newRoutes[j].Priority {
			return newRoutes[i].Priority > newRoutes[j].Priority
		}
		return newRoutes[i].Name < newRoutes[j].Name
	})

	rm.routes = newRoutes
}

// RemoveRoute removes a route by name
func (rm *RouteManager) RemoveRoute(name string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	newRoutes := make([]*Route, 0, len(rm.routes))
	for _, r := range rm.routes {
		if r.Name != name {
			newRoutes = append(newRoutes, r)
		}
	}
	rm.routes = newRoutes
}

// Match finds the first matching route for a request
func (rm *RouteManager) Match(req *RouteRequest) *Route {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	for _, route := range rm.routes {
		if rm.matchRoute(route, req) {
			// Update stats
			atomic.AddInt64(&route.MatchedRequests, 1)
			route.LastMatchTime = req.Timestamp
			return route
		}
	}
	return nil
}

func (rm *RouteManager) matchRoute(route *Route, req *RouteRequest) bool {
	// Match operations (if specified)
	if len(route.Operations) > 0 {
		if !route.Operations[req.Operation] {
			return false
		}
	}

	// Match models (if specified)
	if len(route.ModelPatterns) > 0 {
		matched := false
		for _, pattern := range route.ModelPatterns {
			if pattern.MatchString(req.Model) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Match headers (if specified)
	for headerName, matcher := range route.HeaderMatchers {
		value, exists := req.Headers[headerName]
		if !exists || !matcher.Matches(value) {
			return false
		}
	}

	// Match source tables (if specified)
	if len(route.SourceTables) > 0 {
		if !route.SourceTables[req.SourceTable] {
			return false
		}
	}

	// Match time window (if specified)
	if route.TimeWindow != nil {
		if !route.TimeWindow.IsActive(req.Timestamp) {
			return false
		}
	}

	return true
}

// SelectDestination chooses a destination from a matched route
// based on weights and conditions
func (rm *RouteManager) SelectDestination(route *Route, req *RouteRequest, registry *ModelRegistry) (*Destination, error) {
	// Collect eligible destinations
	eligible := make([]Destination, 0)
	totalWeight := int32(0)

	for _, dest := range route.Destinations {
		// Check conditions
		if !rm.evaluateConditions(&dest, req, registry) {
			continue
		}

		eligible = append(eligible, dest)
		totalWeight += dest.Weight
	}

	if len(eligible) == 0 {
		return nil, nil // No eligible destinations
	}

	// Weighted random selection
	if len(eligible) == 1 {
		return &eligible[0], nil
	}

	// Simple weighted selection (could use random for true distribution)
	// For now, pick highest weight that's eligible
	var best *Destination
	for i := range eligible {
		if best == nil || eligible[i].Weight > best.Weight {
			best = &eligible[i]
		}
	}
	return best, nil
}

func (rm *RouteManager) evaluateConditions(dest *Destination, req *RouteRequest, registry *ModelRegistry) bool {
	// Get pool stats
	endpoints := registry.GetEndpointsForPool(dest.Pool)
	if len(endpoints) == 0 {
		return false // Pool has no healthy endpoints
	}

	// Calculate aggregate stats
	var totalQueueDepth int32
	var modelLoaded bool
	for _, ep := range endpoints {
		totalQueueDepth += atomic.LoadInt32(&ep.QueueDepth)
		if _, exists := ep.Models[req.Model]; exists {
			modelLoaded = true
		}
	}
	avgQueueDepth := float64(totalQueueDepth) / float64(len(endpoints))

	// Check queue depth condition
	if dest.QueueDepthCondition != nil {
		if !dest.QueueDepthCondition.Evaluate(avgQueueDepth) {
			return false
		}
	}

	// Check replica condition
	if dest.ReplicaCondition != nil {
		if !dest.ReplicaCondition.Evaluate(float64(len(endpoints))) {
			return false
		}
	}

	// Check model loaded condition
	if dest.RequireModelLoaded && !modelLoaded {
		return false
	}

	// Check time condition
	if dest.TimeCondition != nil {
		if !dest.TimeCondition.IsActive(req.Timestamp) {
			return false
		}
	}

	return true
}

// CompileModelPattern compiles a model pattern with wildcards to a regex
func CompileModelPattern(pattern string) (*regexp.Regexp, error) {
	// Escape regex special chars except *
	escaped := regexp.QuoteMeta(pattern)
	// Convert * to .*
	regexPattern := strings.ReplaceAll(escaped, `\*`, `.*`)
	// Anchor the pattern
	regexPattern = "^" + regexPattern + "$"
	return regexp.Compile(regexPattern)
}

// ParseThresholdCondition parses conditions like ">50", ">=100", "<10"
func ParseThresholdCondition(s string) (*ThresholdCondition, error) {
	s = strings.TrimSpace(s)

	var operator string
	var valueStr string

	if strings.HasPrefix(s, ">=") {
		operator = ">="
		valueStr = strings.TrimPrefix(s, ">=")
	} else if strings.HasPrefix(s, "<=") {
		operator = "<="
		valueStr = strings.TrimPrefix(s, "<=")
	} else if strings.HasPrefix(s, ">") {
		operator = ">"
		valueStr = strings.TrimPrefix(s, ">")
	} else if strings.HasPrefix(s, "<") {
		operator = "<"
		valueStr = strings.TrimPrefix(s, "<")
	} else if strings.HasPrefix(s, "==") {
		operator = "=="
		valueStr = strings.TrimPrefix(s, "==")
	} else {
		operator = "=="
		valueStr = s
	}

	// Parse value (handle duration suffixes like "100ms")
	valueStr = strings.TrimSpace(valueStr)
	var value float64

	if before, ok := strings.CutSuffix(valueStr, "ms"); ok {
		// Milliseconds
		valueStr = before
		var v float64
		_, err := parseFloat(valueStr, &v)
		if err != nil {
			return nil, err
		}
		value = v / 1000 // Convert to seconds
	} else if before, ok := strings.CutSuffix(valueStr, "s"); ok {
		valueStr = before
		_, err := parseFloat(valueStr, &value)
		if err != nil {
			return nil, err
		}
	} else {
		_, err := parseFloat(valueStr, &value)
		if err != nil {
			return nil, err
		}
	}

	return &ThresholdCondition{
		Operator: operator,
		Value:    value,
	}, nil
}

func parseFloat(s string, v *float64) (int, error) {
	var n int
	_, err := parseFloatInternal(s, v, &n)
	return n, err
}

func parseFloatInternal(s string, v *float64, n *int) (bool, error) {
	// Simple float parser
	var result float64
	var decimal float64 = 1
	inDecimal := false
	negative := false

	for i, c := range s {
		if c == '-' && i == 0 {
			negative = true
			continue
		}
		if c == '.' {
			inDecimal = true
			continue
		}
		if c >= '0' && c <= '9' {
			digit := float64(c - '0')
			if inDecimal {
				decimal *= 10
				result += digit / decimal
			} else {
				result = result*10 + digit
			}
			*n++
		}
	}

	if negative {
		result = -result
	}
	*v = result
	return true, nil
}
