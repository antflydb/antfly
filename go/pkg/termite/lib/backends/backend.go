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

package backends

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Backend represents an inference backend that can load models.
// Backends self-register via init() functions in their respective files.
type Backend interface {
	// Type returns the backend type identifier
	Type() BackendType

	// Name returns a human-readable name (e.g., "ONNX Runtime (CUDA)")
	Name() string

	// Available returns true if this backend can be used in the current environment.
	// This checks for required libraries, hardware, etc.
	Available() bool

	// Priority returns the default priority (lower = higher priority).
	// Used when no explicit priority is configured.
	// Recommended values: 10 for ONNX, 20 for XLA, 30 for GoMLX, 100 for Go (fallback)
	Priority() int

	// Loader returns the ModelLoader for this backend.
	Loader() ModelLoader
}

var (
	// registry holds all registered backends
	registry   = make(map[BackendType]Backend)
	registryMu sync.RWMutex

	// priority defines the order to try backends when selecting default.
	// Configurable via SetPriority(). Default: ONNX > XLA > CoreML > Go
	defaultPriority = []BackendType{BackendONNX, BackendXLA, BackendCoreML, BackendGo}
	configPriority  []BackendType
	priorityMu      sync.RWMutex
)

// RegisterBackend registers a backend. Called by backend implementations in init().
// Thread-safe. Later registrations for the same type overwrite earlier ones.
func RegisterBackend(b Backend) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[b.Type()] = b
}

// GetBackend returns the backend for the given type, if registered.
func GetBackend(t BackendType) (Backend, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	b, ok := registry[t]
	return b, ok
}

// ListRegistered returns all registered backends (available or not).
// Sorted by priority (lowest priority number first).
func ListRegistered() []Backend {
	registryMu.RLock()
	defer registryMu.RUnlock()

	backends := make([]Backend, 0, len(registry))
	for _, b := range registry {
		backends = append(backends, b)
	}

	// Sort by priority for consistent ordering
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].Priority() < backends[j].Priority()
	})

	return backends
}

// ListAvailable returns all backends that are currently available for use.
// Sorted by configured priority order.
func ListAvailable() []Backend {
	priority := GetPriority()

	registryMu.RLock()
	defer registryMu.RUnlock()

	// First add backends in priority order
	result := make([]Backend, 0, len(registry))
	seen := make(map[BackendType]bool)

	for _, t := range priority {
		if b, ok := registry[t]; ok && b.Available() {
			result = append(result, b)
			seen[t] = true
		}
	}

	// Then add any remaining available backends not in priority list
	for t, b := range registry {
		if !seen[t] && b.Available() {
			result = append(result, b)
		}
	}

	return result
}

// SetPriority sets the backend selection priority order.
// When selecting a default backend, the first available backend in this order is used.
// Call before creating any sessions to take effect.
func SetPriority(order []BackendType) {
	priorityMu.Lock()
	defer priorityMu.Unlock()
	configPriority = make([]BackendType, len(order))
	copy(configPriority, order)
}

type gpuModeConfigurableBackend interface {
	SetGPUMode(GPUMode)
}

// ConfigurePriorityDevices applies the first device requirement for each
// concrete backend before any model sessions are created. SessionManager keeps
// device metadata for selection and diagnostics, while backends such as ONNX
// use this hook to configure their execution provider globally.
func ConfigurePriorityDevices(priority []BackendSpec) {
	seen := make(map[BackendType]struct{}, len(priority))
	for _, spec := range priority {
		if _, exists := seen[spec.Backend]; exists {
			continue
		}
		seen[spec.Backend] = struct{}{}
		backend, exists := GetBackend(spec.Backend)
		if !exists {
			continue
		}
		configurable, ok := backend.(gpuModeConfigurableBackend)
		if !ok {
			continue
		}
		configurable.SetGPUMode(spec.Device.ToGPUMode())
	}
}

// GetPriority returns the current backend priority order.
// Returns the configured priority if set, otherwise the default.
func GetPriority() []BackendType {
	priorityMu.RLock()
	defer priorityMu.RUnlock()
	if len(configPriority) > 0 {
		result := make([]BackendType, len(configPriority))
		copy(result, configPriority)
		return result
	}
	result := make([]BackendType, len(defaultPriority))
	copy(result, defaultPriority)
	return result
}

// GetDefaultBackend returns the first available backend according to priority order.
// Returns nil if no backends are available.
func GetDefaultBackend() Backend {
	priority := GetPriority()

	registryMu.RLock()
	defer registryMu.RUnlock()

	// Try backends in priority order
	for _, t := range priority {
		if b, ok := registry[t]; ok && b.Available() {
			return b
		}
	}

	// Fallback: any available backend
	for _, b := range registry {
		if b.Available() {
			return b
		}
	}

	return nil
}

// GetBackendWithFallback attempts to get the preferred backend, falling back to
// alternatives if unavailable. Returns the backend and its type.
func GetBackendWithFallback(preferred BackendType) (Backend, BackendType, error) {
	// Try preferred first
	if b, ok := GetBackend(preferred); ok && b.Available() {
		return b, preferred, nil
	}

	// Fallback to default
	b := GetDefaultBackend()
	if b == nil {
		return nil, "", fmt.Errorf("no available backends (preferred: %s)", preferred)
	}

	return b, b.Type(), nil
}

// ParseBackendType translates the public inference backend vocabulary into the
// implementation-specific backend used by the Go runtime. Public config must
// not expose GoMLX/CoreML naming: those are implementation details and differ
// from the shared Antfly API used by the Zig runtime.
func ParseBackendType(s string) (BackendType, error) {
	switch strings.ToLower(s) {
	case "onnx":
		return BackendONNX, nil
	case "pjrt":
		return BackendXLA, nil
	case "metal":
		return BackendCoreML, nil
	case "native":
		return BackendGo, nil
	case "cuda":
		// CUDA is realized by the ONNX backend in the Go runtime. ParseBackendSpec
		// supplies DeviceCUDA so loaders that consume device preferences can make
		// the execution-provider requirement explicit.
		return BackendONNX, nil
	case "webgpu":
		return "", fmt.Errorf("backend %q is not supported by the Go inference runtime", s)
	default:
		return "", fmt.Errorf("unknown backend type: %q (valid: native, onnx, metal, cuda, pjrt, webgpu)", s)
	}
}

// BackendTypeStrings returns valid backend type strings for documentation/validation.
func BackendTypeStrings() []string {
	return []string{"native", "onnx", "metal", "cuda", "pjrt", "webgpu"}
}

// ParseDeviceType parses a string into DeviceType.
func ParseDeviceType(s string) (DeviceType, error) {
	switch strings.ToLower(s) {
	case "auto", "":
		return DeviceAuto, nil
	case "cuda", "gpu":
		return DeviceCUDA, nil
	case "coreml":
		return DeviceCoreML, nil
	case "tpu":
		return DeviceTPU, nil
	case "cpu", "off":
		return DeviceCPU, nil
	default:
		return "", fmt.Errorf("unknown device type: %q (valid: auto, cuda, coreml, tpu, cpu)", s)
	}
}

// ParseBackendSpec parses a public "backend" or "backend:device" string and
// translates it to the Go runtime's internal backend/device pair.
func ParseBackendSpec(s string) (BackendSpec, error) {
	parts := strings.SplitN(s, ":", 2)
	publicBackend := strings.ToLower(strings.TrimSpace(parts[0]))

	backend, err := ParseBackendType(publicBackend)
	if err != nil {
		return BackendSpec{}, err
	}

	defaultDevice := DeviceAuto
	switch publicBackend {
	case "native":
		defaultDevice = DeviceCPU
	case "metal":
		defaultDevice = DeviceCoreML
	case "cuda":
		defaultDevice = DeviceCUDA
	}
	spec := BackendSpec{Backend: backend, Device: defaultDevice}

	if len(parts) == 2 {
		device, err := ParseDeviceType(parts[1])
		if err != nil {
			return BackendSpec{}, err
		}
		spec.Device = device
	}

	return spec, nil
}

// ParseBackendPriority parses a list of backend:device strings into BackendSpecs.
// Each element can be a single spec ("onnx", "pjrt:tpu") or comma-separated
// ("onnx,pjrt,native") to handle environment variables where viper returns the
// entire value as a single string slice element.
func ParseBackendPriority(priority []string) ([]BackendSpec, error) {
	specs := make([]BackendSpec, 0, len(priority))
	for _, s := range priority {
		// Split on commas to handle env vars like TERMITE_BACKEND_PRIORITY="onnx,pjrt,native"
		for part := range strings.SplitSeq(s, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			spec, err := ParseBackendSpec(part)
			if err != nil {
				return nil, fmt.Errorf("invalid backend priority %q: %w", part, err)
			}
			specs = append(specs, spec)
		}
	}
	return specs, nil
}

// ParseGPUMode parses a string into GPUMode.
func ParseGPUMode(s string) GPUMode {
	switch strings.ToLower(s) {
	case "auto", "":
		return GPUModeAuto
	case "tpu":
		return GPUModeTpu
	case "cuda":
		return GPUModeCuda
	case "coreml":
		return GPUModeCoreML
	case "off":
		return GPUModeOff
	default:
		return GPUModeAuto
	}
}
