// Copyright 2026 Antfly, Inc.
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

// Package proxy implements route matching for InferenceProxy CRDs.
package proxy

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Route represents a compiled InferenceProxy for fast matching
type Route struct {
	Name     string
	Priority int32

	// Declarative matchers. The manager compiles private programs when it owns
	// an installed route snapshot.
	Operations          map[OperationType]bool
	ModelPatterns       []*RegexPattern
	HeaderMatchers      map[string]*StringMatcher
	SourceTables        map[string]bool
	SourceOrganizations map[string]bool
	SourceProjects      map[string]bool
	SourceAPIKeys       map[string]bool
	TimeWindow          *TimeWindow

	// Destinations
	Destinations []Destination

	// Fallback
	Fallback *Fallback

	// Rate limiting state
	RateLimiter *RateLimiter

	// Retry config
	RetryAttempts      int32
	RetryTimeout       time.Duration
	RetryOnStatuses    map[int]bool
	RetryOnRequestErrs bool
	RetryOnCanceled    bool
}

// OperationType for matching
type OperationType string

// RegexSyntax is part of route policy identity. Go's regexp.String omits
// whether a program was compiled as POSIX or switched to leftmost-longest, so
// retaining only *regexp.Regexp cannot support authoritative change detection.
type RegexSyntax string

const (
	RegexLeftmostFirst   RegexSyntax = "leftmost-first"
	RegexLeftmostLongest RegexSyntax = "leftmost-longest"
	RegexPOSIX           RegexSyntax = "posix"
)

// RegexPattern is the declarative route representation. compiled is owned by
// the installed immutable snapshot and never exposed as mutable policy state.
type RegexPattern struct {
	Expression string
	Syntax     RegexSyntax
	compiled   *regexp.Regexp
}

func CompileRegexPattern(expression string, syntax RegexSyntax) (*RegexPattern, error) {
	pattern := &RegexPattern{Expression: expression, Syntax: syntax}
	compiled, err := pattern.compile()
	if err != nil {
		return nil, err
	}
	pattern.compiled = compiled
	return pattern, nil
}

func MustRegexPattern(expression string) *RegexPattern {
	pattern, err := CompileRegexPattern(expression, RegexLeftmostFirst)
	if err != nil {
		panic(err)
	}
	return pattern
}

func (p *RegexPattern) compile() (*regexp.Regexp, error) {
	if p == nil {
		return nil, nil
	}
	switch p.Syntax {
	case "", RegexLeftmostFirst:
		return regexp.Compile(p.Expression)
	case RegexLeftmostLongest:
		compiled, err := regexp.Compile(p.Expression)
		if err != nil {
			return nil, err
		}
		compiled.Longest()
		return compiled, nil
	case RegexPOSIX:
		return regexp.CompilePOSIX(p.Expression)
	default:
		return nil, fmt.Errorf("unsupported regex syntax %q", p.Syntax)
	}
}

func (p *RegexPattern) Matches(value string) bool {
	return p != nil && p.compiled != nil && p.compiled.MatchString(value)
}

// StringMatcher for header matching
type StringMatcher struct {
	Exact  string
	Prefix string
	Regex  *RegexPattern
}

func (m *StringMatcher) Matches(value string) bool {
	if m.Exact != "" && value == m.Exact {
		return true
	}
	if m.Prefix != "" && strings.HasPrefix(value, m.Prefix) {
		return true
	}
	if m.Regex != nil && m.Regex.Matches(value) {
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
	Operation          OperationType
	Model              string
	Headers            map[string]string
	SourceTable        string
	SourceOrganization string
	SourceProject      string
	SourceAPIKey       string
	Timestamp          time.Time
}

// RouteManager manages all routes and performs matching
type RouteManager struct {
	routes     []*Route // Sorted by priority (descending)
	generation uint64
	mu         sync.RWMutex
}

// NewRouteManager creates a new RouteManager
func NewRouteManager() *RouteManager {
	return &RouteManager{
		routes: make([]*Route, 0),
	}
}

// UpsertRoute adds or replaces a route (routes are re-sorted by priority). It
// deliberately owns an immutable copy of the declarative policy: informer
// resyncs and equivalent updates preserve both the routing generation and live
// rate-limiter state, while later caller mutations cannot alter installed
// routing policy behind the generation fence.
func (rm *RouteManager) UpsertRoute(route *Route) (bool, error) {
	if route == nil {
		return false, nil
	}
	candidate, err := prepareRoute(route, true)
	if err != nil {
		return false, err
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Remove existing route with same name
	newRoutes := make([]*Route, 0, len(rm.routes)+1)
	for _, r := range rm.routes {
		if r.Name == candidate.Name {
			if routePolicyEqual(r, candidate) {
				return false, nil
			}
			// The token bucket is mutable runtime state, not declarative policy.
			// Preserve it across unrelated policy changes when its configuration
			// is unchanged.
			if rateLimiterPolicyEqual(r.RateLimiter, candidate.RateLimiter) {
				candidate.RateLimiter = r.RateLimiter
			}
			continue
		}
		newRoutes = append(newRoutes, r)
	}
	newRoutes = append(newRoutes, candidate)

	// Sort by priority (descending), then by name (ascending) for stable ordering
	sort.Slice(newRoutes, func(i, j int) bool {
		if newRoutes[i].Priority != newRoutes[j].Priority {
			return newRoutes[i].Priority > newRoutes[j].Priority
		}
		return newRoutes[i].Name < newRoutes[j].Name
	})

	rm.routes = newRoutes
	rm.generation++
	return true, nil
}

func prepareRoute(route *Route, ownRateLimiter bool) (*Route, error) {
	cloned := cloneRoute(route, ownRateLimiter)
	for index, pattern := range cloned.ModelPatterns {
		if pattern == nil {
			return nil, fmt.Errorf("model pattern %d must not be nil", index)
		}
		if pattern.Syntax == "" {
			pattern.Syntax = RegexLeftmostFirst
		}
		compiled, err := pattern.compile()
		if err != nil {
			return nil, fmt.Errorf("invalid model pattern %q: %w", pattern.Expression, err)
		}
		pattern.compiled = compiled
	}
	for name, matcher := range cloned.HeaderMatchers {
		if matcher == nil {
			return nil, fmt.Errorf("header matcher for %q must not be nil", name)
		}
		if matcher.Regex == nil {
			continue
		}
		if matcher.Regex.Syntax == "" {
			matcher.Regex.Syntax = RegexLeftmostFirst
		}
		compiled, err := matcher.Regex.compile()
		if err != nil {
			return nil, fmt.Errorf("invalid header pattern %q for %q: %w", matcher.Regex.Expression, name, err)
		}
		matcher.Regex.compiled = compiled
	}
	return cloned, nil
}

func cloneRoute(route *Route, ownRateLimiter bool) *Route {
	if route == nil {
		return nil
	}
	cloned := *route
	cloned.Operations = cloneBoolMap(route.Operations)
	cloned.ModelPatterns = cloneRegexps(route.ModelPatterns)
	cloned.HeaderMatchers = cloneStringMatchers(route.HeaderMatchers)
	cloned.SourceTables = cloneBoolMap(route.SourceTables)
	cloned.SourceOrganizations = cloneBoolMap(route.SourceOrganizations)
	cloned.SourceProjects = cloneBoolMap(route.SourceProjects)
	cloned.SourceAPIKeys = cloneBoolMap(route.SourceAPIKeys)
	cloned.TimeWindow = cloneTimeWindow(route.TimeWindow)
	cloned.Destinations = cloneDestinations(route.Destinations)
	cloned.Fallback = cloneFallback(route.Fallback)
	cloned.RetryOnStatuses = cloneBoolMap(route.RetryOnStatuses)
	if ownRateLimiter && route.RateLimiter != nil {
		cloned.RateLimiter = cloneRateLimiterPolicy(route.RateLimiter)
	}
	// Match snapshots share only the synchronized token-bucket runtime. Every
	// declarative map, slice, matcher, and condition is independently owned.
	return &cloned
}

func cloneRateLimiterPolicy(source *RateLimiter) *RateLimiter {
	if source == nil {
		return nil
	}
	return &RateLimiter{
		rate:        source.rate,
		burstSize:   source.burstSize,
		tokens:      float64(source.burstSize),
		lastUpdate:  time.Now(),
		perModel:    source.perModel,
		modelLimits: make(map[string]*modelLimit),
	}
}

func cloneBoolMap[K comparable](source map[K]bool) map[K]bool {
	if source == nil {
		return nil
	}
	cloned := make(map[K]bool, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func cloneRegexps(source []*RegexPattern) []*RegexPattern {
	if source == nil {
		return nil
	}
	cloned := make([]*RegexPattern, len(source))
	for i, pattern := range source {
		if pattern != nil {
			patternCopy := *pattern
			cloned[i] = &patternCopy
		}
	}
	return cloned
}

func cloneStringMatchers(source map[string]*StringMatcher) map[string]*StringMatcher {
	if source == nil {
		return nil
	}
	cloned := make(map[string]*StringMatcher, len(source))
	for name, matcher := range source {
		if matcher == nil {
			cloned[name] = nil
			continue
		}
		matcherCopy := *matcher
		if matcher.Regex != nil {
			regexCopy := *matcher.Regex
			matcherCopy.Regex = &regexCopy
		}
		cloned[name] = &matcherCopy
	}
	return cloned
}

func cloneTimeWindow(source *TimeWindow) *TimeWindow {
	if source == nil {
		return nil
	}
	cloned := *source
	cloned.Days = cloneBoolMap(source.Days)
	return &cloned
}

func cloneDestinations(source []Destination) []Destination {
	if source == nil {
		return nil
	}
	cloned := make([]Destination, len(source))
	for i := range source {
		cloned[i] = source[i]
		cloned[i].QueueDepthCondition = cloneThreshold(source[i].QueueDepthCondition)
		cloned[i].ReplicaCondition = cloneThreshold(source[i].ReplicaCondition)
		cloned[i].LatencyCondition = cloneThreshold(source[i].LatencyCondition)
		cloned[i].TimeCondition = cloneTimeWindow(source[i].TimeCondition)
	}
	return cloned
}

func cloneThreshold(source *ThresholdCondition) *ThresholdCondition {
	if source == nil {
		return nil
	}
	cloned := *source
	return &cloned
}

func cloneFallback(source *Fallback) *Fallback {
	if source == nil {
		return nil
	}
	cloned := *source
	return &cloned
}

func routePolicyEqual(left, right *Route) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Name != right.Name || left.Priority != right.Priority ||
		left.RetryAttempts != right.RetryAttempts || left.RetryTimeout != right.RetryTimeout ||
		left.RetryOnRequestErrs != right.RetryOnRequestErrs || left.RetryOnCanceled != right.RetryOnCanceled ||
		!boolMapEqual(left.Operations, right.Operations) ||
		!boolMapEqual(left.SourceTables, right.SourceTables) ||
		!boolMapEqual(left.SourceOrganizations, right.SourceOrganizations) ||
		!boolMapEqual(left.SourceProjects, right.SourceProjects) ||
		!boolMapEqual(left.SourceAPIKeys, right.SourceAPIKeys) ||
		!boolMapEqual(left.RetryOnStatuses, right.RetryOnStatuses) ||
		!regexpSliceEqual(left.ModelPatterns, right.ModelPatterns) ||
		!stringMatcherMapEqual(left.HeaderMatchers, right.HeaderMatchers) ||
		!timeWindowEqual(left.TimeWindow, right.TimeWindow) ||
		!destinationSliceEqual(left.Destinations, right.Destinations) ||
		!fallbackEqual(left.Fallback, right.Fallback) ||
		!rateLimiterPolicyEqual(left.RateLimiter, right.RateLimiter) {
		return false
	}
	return true
}

func boolMapEqual[K comparable](left, right map[K]bool) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if other, ok := right[key]; !ok || other != value {
			return false
		}
	}
	return true
}

func regexpSliceEqual(left, right []*RegexPattern) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] == nil || right[i] == nil {
			if left[i] != right[i] {
				return false
			}
			continue
		}
		if left[i].Expression != right[i].Expression || left[i].Syntax != right[i].Syntax {
			return false
		}
	}
	return true
}

func stringMatcherMapEqual(left, right map[string]*StringMatcher) bool {
	if len(left) != len(right) {
		return false
	}
	for name, matcher := range left {
		other, ok := right[name]
		if !ok || !stringMatcherEqual(matcher, other) {
			return false
		}
	}
	return true
}

func stringMatcherEqual(left, right *StringMatcher) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Exact != right.Exact || left.Prefix != right.Prefix {
		return false
	}
	if left.Regex == nil || right.Regex == nil {
		return left.Regex == right.Regex
	}
	return left.Regex.Expression == right.Regex.Expression && left.Regex.Syntax == right.Regex.Syntax
}

func timeWindowEqual(left, right *TimeWindow) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.StartHour == right.StartHour && left.StartMinute == right.StartMinute &&
		left.EndHour == right.EndHour && left.EndMinute == right.EndMinute &&
		boolMapEqual(left.Days, right.Days)
}

func thresholdEqual(left, right *ThresholdCondition) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Operator == right.Operator && left.Value == right.Value
}

func destinationSliceEqual(left, right []Destination) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Pool != right[i].Pool || left[i].Weight != right[i].Weight ||
			left[i].RequireModelLoaded != right[i].RequireModelLoaded ||
			!thresholdEqual(left[i].QueueDepthCondition, right[i].QueueDepthCondition) ||
			!thresholdEqual(left[i].ReplicaCondition, right[i].ReplicaCondition) ||
			!thresholdEqual(left[i].LatencyCondition, right[i].LatencyCondition) ||
			!timeWindowEqual(left[i].TimeCondition, right[i].TimeCondition) {
			return false
		}
	}
	return true
}

func fallbackEqual(left, right *Fallback) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func rateLimiterPolicyEqual(left, right *RateLimiter) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.rate == right.rate && left.burstSize == right.burstSize && left.perModel == right.perModel
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
	if len(newRoutes) != len(rm.routes) {
		rm.routes = newRoutes
		rm.generation++
	}
}

// Generation identifies the immutable routing policy snapshot currently
// installed in this manager. Capability leases bind to this value so a policy
// update cannot silently reinterpret an already planned invocation.
func (rm *RouteManager) Generation() uint64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.generation
}

// Match finds the first matching route for a request
func (rm *RouteManager) Match(req *RouteRequest) *Route {
	return cloneRoute(rm.matchInstalled(req), false)
}

// matchInstalled returns a manager-owned immutable snapshot for the internal
// proxy hot path. It must never escape through an exported result.
func (rm *RouteManager) matchInstalled(req *RouteRequest) *Route {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	for _, route := range rm.routes {
		if rm.matchRoute(route, req) {
			return route
		}
	}
	return nil
}

// MatchAtGeneration linearizes a routing decision against the policy snapshot
// used for capability discovery. A false result means the caller must refresh
// its capability lease before executing.
func (rm *RouteManager) MatchAtGeneration(req *RouteRequest, generation uint64) (*Route, bool) {
	route, current := rm.matchInstalledAtGeneration(req, generation)
	return cloneRoute(route, false), current
}

func (rm *RouteManager) matchInstalledAtGeneration(req *RouteRequest, generation uint64) (*Route, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if rm.generation != generation {
		return nil, false
	}
	for _, route := range rm.routes {
		if rm.matchRoute(route, req) {
			return route, true
		}
	}
	return nil, true
}

// RouteCohort describes the pools that a future execution may reach. Matched
// distinguishes a route cohort from the absence of any applicable route, and
// Terminal records that a definite route makes the default pool and every
// lower-priority route unreachable. A terminal route may intentionally expose
// no pools (for example, a reject fallback).
type RouteCohort struct {
	Pools      []string
	Matched    bool
	Terminal   bool
	Generation uint64
}

// PotentialCohortFor returns the route cohort that can handle an operation and
// model. Missing source/header context is treated as unknown rather than as a
// negative match. Routes stay priority ordered: all possibly matching routes
// are included until a definite match makes every lower-priority route
// unreachable. Time windows remain possible for the lifetime of a capability
// lease because discovery and execution may straddle a window boundary.
func (rm *RouteManager) PotentialCohortFor(req *RouteRequest) RouteCohort {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	seen := make(map[string]bool)
	appendRoutePools := func(out []string, route *Route) []string {
		for _, destination := range route.Destinations {
			if destination.Pool != "" && !seen[destination.Pool] {
				seen[destination.Pool] = true
				out = append(out, destination.Pool)
			}
		}
		if route.Fallback != nil && route.Fallback.Action == "redirect" && route.Fallback.RedirectPool != "" && !seen[route.Fallback.RedirectPool] {
			seen[route.Fallback.RedirectPool] = true
			out = append(out, route.Fallback.RedirectPool)
		}
		return out
	}
	cohort := RouteCohort{Generation: rm.generation}
	for _, route := range rm.routes {
		if len(route.Operations) > 0 && !route.Operations[req.Operation] {
			continue
		}
		if len(route.ModelPatterns) > 0 {
			matched := false
			for _, pattern := range route.ModelPatterns {
				if pattern.Matches(req.Model) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		possible := false
		impossible := false
		for headerName, matcher := range route.HeaderMatchers {
			value, present := lookupHeader(req.Headers, headerName)
			if !present {
				possible = true
				continue
			}
			if !matcher.Matches(value) {
				impossible = true
				break
			}
		}
		if impossible {
			continue
		}
		matchSource := func(value string, accepted map[string]bool) bool {
			if len(accepted) == 0 {
				return true
			}
			if value == "" {
				possible = true
				return true
			}
			return accepted[value]
		}
		if !matchSource(req.SourceTable, route.SourceTables) ||
			!matchSource(req.SourceOrganization, route.SourceOrganizations) ||
			!matchSource(req.SourceProject, route.SourceProjects) ||
			!matchSource(req.SourceAPIKey, route.SourceAPIKeys) {
			continue
		}
		if route.TimeWindow != nil {
			// Even a currently exact time match can change while the issued lease
			// remains live, so retain both this route and the next definite route.
			possible = true
		}
		cohort.Matched = true
		cohort.Pools = appendRoutePools(cohort.Pools, route)
		if !possible {
			cohort.Terminal = true
			return cohort
		}
	}
	return cohort
}

// PotentialPoolsFor is retained for source compatibility.
// Deprecated: use PotentialCohortFor so a terminal route cannot be confused
// with the absence of a matching route.
func (rm *RouteManager) PotentialPoolsFor(req *RouteRequest) []string {
	return rm.PotentialCohortFor(req).Pools
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
			if pattern.Matches(req.Model) {
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
		value, exists := lookupHeader(req.Headers, headerName)
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

	if len(route.SourceOrganizations) > 0 {
		if !route.SourceOrganizations[req.SourceOrganization] {
			return false
		}
	}

	if len(route.SourceProjects) > 0 {
		if !route.SourceProjects[req.SourceProject] {
			return false
		}
	}

	if len(route.SourceAPIKeys) > 0 {
		if !route.SourceAPIKeys[req.SourceAPIKey] {
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
	return rm.selectDestinationWithin(route, req, registry, nil)
}

// selectDestinationWithin applies route weights and conditions only to pools
// containing a currently healthy endpoint from the immutable capability lease.
func (rm *RouteManager) selectDestinationWithin(route *Route, req *RouteRequest, registry *ModelRegistry, allowed map[string]endpointRef) (*Destination, error) {
	// Collect eligible destinations
	eligible := make([]Destination, 0)
	totalWeight := int32(0)

	for _, dest := range route.Destinations {
		// Check conditions
		if !rm.evaluateConditionsWithin(&dest, req, registry, allowed) {
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

	if totalWeight <= 0 {
		return &eligible[0], nil
	}

	// Use a deterministic weighted selection so repeated traffic splits
	// remain stable without shared mutable RNG state.
	selection := weightedSelectionValue(route, req, totalWeight)
	cumulative := int32(0)
	for i := range eligible {
		cumulative += eligible[i].Weight
		if selection < cumulative {
			return &eligible[i], nil
		}
	}

	return &eligible[len(eligible)-1], nil
}

func (rm *RouteManager) evaluateConditions(dest *Destination, req *RouteRequest, registry *ModelRegistry) bool {
	return rm.evaluateConditionsWithin(dest, req, registry, nil)
}

func (rm *RouteManager) evaluateConditionsWithin(dest *Destination, req *RouteRequest, registry *ModelRegistry, allowed map[string]endpointRef) bool {
	stats := registry.poolConditionStatsWithin(dest.Pool, req.Model, allowed)
	if stats.HealthyEndpoints == 0 {
		return false // Pool has no healthy endpoints
	}

	// Check queue depth condition
	if dest.QueueDepthCondition != nil {
		if !dest.QueueDepthCondition.Evaluate(stats.AvgQueueDepth) {
			return false
		}
	}

	// Check replica condition
	if dest.ReplicaCondition != nil {
		if !dest.ReplicaCondition.Evaluate(float64(stats.HealthyEndpoints)) {
			return false
		}
	}

	if dest.LatencyCondition != nil {
		if !stats.HasLatency {
			return false
		}
		if !dest.LatencyCondition.Evaluate(stats.P99Latency.Seconds()) {
			return false
		}
	}

	// Check model loaded condition
	if dest.RequireModelLoaded && !stats.ModelLoaded {
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
func CompileModelPattern(pattern string) (*RegexPattern, error) {
	// Escape regex special chars except *
	escaped := regexp.QuoteMeta(pattern)
	// Convert * to .*
	regexPattern := strings.ReplaceAll(escaped, `\*`, `.*`)
	// Anchor the pattern
	regexPattern = "^" + regexPattern + "$"
	return CompileRegexPattern(regexPattern, RegexLeftmostFirst)
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

func weightedSelectionValue(route *Route, req *RouteRequest, totalWeight int32) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(route.Name))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(req.Model))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(req.Operation))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(req.SourceTable))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(req.SourceOrganization))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(req.SourceProject))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(req.SourceAPIKey))
	_, _ = h.Write([]byte{0})

	headerNames := make([]string, 0, len(req.Headers))
	for name := range req.Headers {
		headerNames = append(headerNames, name)
	}
	sort.Strings(headerNames)
	for _, name := range headerNames {
		_, _ = h.Write([]byte(name))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(req.Headers[name]))
		_, _ = h.Write([]byte{0})
	}

	_, _ = h.Write([]byte(req.Timestamp.UTC().Format(time.RFC3339Nano)))
	return int32(h.Sum32() % uint32(totalWeight))
}
