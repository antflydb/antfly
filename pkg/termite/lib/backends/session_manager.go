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

package backends

import (
	"fmt"
	"sync"
)

// SessionManager manages model loaders across multiple backends.
// It maintains at most one loader per backend type (lazy-created).
//
// Usage:
//
//	manager := backends.NewSessionManager()
//	defer manager.Close()
//
//	// Configure backend priority with device preferences
//	manager.SetPriority([]BackendSpec{
//	    {Backend: BackendONNX, Device: DeviceCUDA},
//	    {Backend: BackendGoMLX, Device: DeviceAuto},  // auto-selects xla or simplego engine
//	    {Backend: BackendONNX, Device: DeviceCPU},
//	})
//
//	// Load a model respecting backend restrictions
//	model, backend, err := manager.LoadModel(modelPath, []string{"onnx", "gomlx"})
type SessionManager struct {
	loaders  map[BackendType]ModelLoader
	priority []BackendSpec // Configured priority with device preferences
	mu       sync.RWMutex
	closed   bool
}

// NewSessionManager creates a new session manager.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		loaders: make(map[BackendType]ModelLoader),
	}
}

// SetPriority configures the backend priority order with device preferences.
// This is used when selecting backends for model loading.
func (sm *SessionManager) SetPriority(priority []BackendSpec) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.priority = make([]BackendSpec, len(priority))
	copy(sm.priority, priority)
}

// getPriority returns the configured priority or the global default.
func (sm *SessionManager) getPriority() []BackendSpec {
	// If we have a configured priority, use it
	if len(sm.priority) > 0 {
		result := make([]BackendSpec, len(sm.priority))
		copy(result, sm.priority)
		return result
	}

	// Fall back to global priority (BackendType only, DeviceAuto)
	globalPriority := GetPriority()
	result := make([]BackendSpec, len(globalPriority))
	for i, bt := range globalPriority {
		result[i] = BackendSpec{Backend: bt, Device: DeviceAuto}
	}
	return result
}

// GetLoader returns a model loader for the specified backend.
// Creates a new loader if one doesn't exist for this backend.
// Returns an error if the backend is unavailable.
func (sm *SessionManager) GetLoader(backend BackendType) (ModelLoader, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return nil, fmt.Errorf("session manager is closed")
	}

	// Return existing loader if available
	if loader, ok := sm.loaders[backend]; ok {
		return loader, nil
	}

	// Get backend
	b, ok := GetBackend(backend)
	if !ok {
		return nil, fmt.Errorf("backend %q not registered", backend)
	}

	if !b.Available() {
		return nil, fmt.Errorf("backend %q not available", backend)
	}

	// Get loader from backend
	loader := b.Loader()
	sm.loaders[backend] = loader
	return loader, nil
}

// GetLoaderWithFallback attempts to get a loader for the preferred backend,
// falling back to alternatives if unavailable.
// Returns the loader and the actual backend type used.
func (sm *SessionManager) GetLoaderWithFallback(preferred BackendType) (ModelLoader, BackendType, error) {
	// Try preferred backend first
	loader, err := sm.GetLoader(preferred)
	if err == nil {
		return loader, preferred, nil
	}

	// Get fallback backend
	b, actualType, err := GetBackendWithFallback(preferred)
	if err != nil {
		return nil, "", fmt.Errorf("no available backends (preferred: %s): %w", preferred, err)
	}

	// Get loader for fallback backend
	loader, err = sm.GetLoader(actualType)
	if err != nil {
		return nil, "", fmt.Errorf("get fallback loader (%s): %w", b.Name(), err)
	}

	return loader, actualType, nil
}

// GetLoaderForModel returns a loader for a model, respecting its backend restrictions.
// If modelBackends is empty, the model supports all backends and the default priority is used.
// Returns the loader and the backend type that was used.
func (sm *SessionManager) GetLoaderForModel(modelBackends []string) (ModelLoader, BackendType, error) {
	sm.mu.RLock()
	priority := sm.getPriority()
	sm.mu.RUnlock()

	// Build model backend set for quick lookup
	modelBackendSet := make(map[BackendType]bool)
	for _, b := range modelBackends {
		modelBackendSet[BackendType(b)] = true
	}

	// Try each backend spec in priority order
	var lastErr error
	for _, spec := range priority {
		// Skip if model doesn't support this backend (unless model has no restrictions)
		if len(modelBackends) > 0 && !modelBackendSet[spec.Backend] {
			continue
		}

		loader, err := sm.GetLoader(spec.Backend)
		if err == nil {
			return loader, spec.Backend, nil
		}
		lastErr = err
	}

	// Include the last error in the returned error for debugging
	if lastErr != nil {
		if len(modelBackends) > 0 {
			return nil, "", fmt.Errorf("no available backends matching model requirements %v: last error: %w", modelBackends, lastErr)
		}
		return nil, "", fmt.Errorf("no available backends: last error: %w", lastErr)
	}

	if len(modelBackends) > 0 {
		return nil, "", fmt.Errorf("no available backends matching model requirements %v", modelBackends)
	}
	return nil, "", fmt.Errorf("no available backends")
}

// LoadModel loads a model using the best available backend.
// If modelBackends is non-empty, only those backends are considered.
// Returns the model and the backend type that was used.
func (sm *SessionManager) LoadModel(path string, modelBackends []string, opts ...LoadOption) (Model, BackendType, error) {
	loader, backendType, err := sm.GetLoaderForModel(modelBackends)
	if err != nil {
		return nil, "", err
	}

	model, err := loader.Load(path, opts...)
	if err != nil {
		return nil, "", fmt.Errorf("loading model with %s backend: %w", backendType, err)
	}

	return model, backendType, nil
}

// GetDefaultLoader returns a loader using the default backend (first available by priority).
func (sm *SessionManager) GetDefaultLoader() (ModelLoader, BackendType, error) {
	// Use GetLoaderForModel with empty model backends to get first available
	return sm.GetLoaderForModel(nil)
}

// HasLoader returns true if a loader exists for the given backend.
func (sm *SessionManager) HasLoader(backend BackendType) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, ok := sm.loaders[backend]
	return ok
}

// ActiveBackends returns the list of backends with active loaders.
func (sm *SessionManager) ActiveBackends() []BackendType {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	backends := make([]BackendType, 0, len(sm.loaders))
	for t := range sm.loaders {
		backends = append(backends, t)
	}
	return backends
}

// GetSessionFactory returns a SessionFactory for the specified backend.
// Returns an error if the backend doesn't support session creation.
func (sm *SessionManager) GetSessionFactory(backend BackendType) (SessionFactory, error) {
	sm.mu.RLock()
	if sm.closed {
		sm.mu.RUnlock()
		return nil, fmt.Errorf("session manager is closed")
	}
	sm.mu.RUnlock()

	// Get the backend
	b, ok := GetBackend(backend)
	if !ok {
		return nil, fmt.Errorf("backend %q not registered", backend)
	}

	if !b.Available() {
		return nil, fmt.Errorf("backend %q not available", backend)
	}

	// Check if the backend provides a SessionFactory
	provider, ok := b.(SessionFactoryProvider)
	if !ok {
		return nil, fmt.Errorf("backend %q does not support session factory", backend)
	}

	return provider.SessionFactory(), nil
}

// GetSessionFactoryForModel returns a SessionFactory for loading a model,
// respecting backend restrictions. Tries backends in priority order.
// Returns the factory and the backend type that was used.
func (sm *SessionManager) GetSessionFactoryForModel(modelBackends []string) (SessionFactory, BackendType, error) {
	sm.mu.RLock()
	priority := sm.getPriority()
	sm.mu.RUnlock()

	// Build model backend set for quick lookup
	modelBackendSet := make(map[BackendType]bool)
	for _, b := range modelBackends {
		modelBackendSet[BackendType(b)] = true
	}

	// Try each backend spec in priority order
	var lastErr error
	for _, spec := range priority {
		// Skip if model doesn't support this backend (unless model has no restrictions)
		if len(modelBackends) > 0 && !modelBackendSet[spec.Backend] {
			continue
		}

		factory, err := sm.GetSessionFactory(spec.Backend)
		if err == nil {
			return factory, spec.Backend, nil
		}
		lastErr = err
	}

	if lastErr != nil {
		if len(modelBackends) > 0 {
			return nil, "", fmt.Errorf("no session factory for backends %v: %w", modelBackends, lastErr)
		}
		return nil, "", fmt.Errorf("no session factory available: %w", lastErr)
	}

	if len(modelBackends) > 0 {
		return nil, "", fmt.Errorf("no session factory for backends %v", modelBackends)
	}
	return nil, "", fmt.Errorf("no session factory available")
}

// Close releases all managed resources.
// After Close, the SessionManager cannot be reused.
func (sm *SessionManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return nil
	}

	// Note: ModelLoader doesn't have a Close method in the interface,
	// but individual backends may implement one if needed.
	sm.loaders = nil
	sm.closed = true

	return nil
}

// GetGenerativeSessionFactory returns a GenerativeSessionFactory for the specified backend.
// Returns an error if the backend doesn't support generative session creation.
func (sm *SessionManager) GetGenerativeSessionFactory(backend BackendType) (GenerativeSessionFactory, error) {
	sm.mu.RLock()
	if sm.closed {
		sm.mu.RUnlock()
		return nil, fmt.Errorf("session manager is closed")
	}
	sm.mu.RUnlock()

	// Get the backend
	b, ok := GetBackend(backend)
	if !ok {
		return nil, fmt.Errorf("backend %q not registered", backend)
	}

	if !b.Available() {
		return nil, fmt.Errorf("backend %q not available", backend)
	}

	// Check if the backend provides a GenerativeSessionFactory
	provider, ok := b.(GenerativeSessionFactoryProvider)
	if !ok {
		return nil, fmt.Errorf("backend %q does not support generative session factory", backend)
	}

	return provider.GenerativeSessionFactory(), nil
}

// GetGenerativeSessionFactoryForModel returns a GenerativeSessionFactory for loading a model,
// respecting backend restrictions. Tries backends in priority order.
// Returns the factory and the backend type that was used.
func (sm *SessionManager) GetGenerativeSessionFactoryForModel(modelBackends []string) (GenerativeSessionFactory, BackendType, error) {
	sm.mu.RLock()
	priority := sm.getPriority()
	sm.mu.RUnlock()

	// Build model backend set for quick lookup
	modelBackendSet := make(map[BackendType]bool)
	for _, b := range modelBackends {
		modelBackendSet[BackendType(b)] = true
	}

	// Try each backend spec in priority order
	var lastErr error
	for _, spec := range priority {
		// Skip if model doesn't support this backend (unless model has no restrictions)
		if len(modelBackends) > 0 && !modelBackendSet[spec.Backend] {
			continue
		}

		factory, err := sm.GetGenerativeSessionFactory(spec.Backend)
		if err == nil {
			return factory, spec.Backend, nil
		}
		lastErr = err
	}

	if lastErr != nil {
		if len(modelBackends) > 0 {
			return nil, "", fmt.Errorf("no generative session factory for backends %v: %w", modelBackends, lastErr)
		}
		return nil, "", fmt.Errorf("no generative session factory available: %w", lastErr)
	}

	if len(modelBackends) > 0 {
		return nil, "", fmt.Errorf("no generative session factory for backends %v", modelBackends)
	}
	return nil, "", fmt.Errorf("no generative session factory available")
}
