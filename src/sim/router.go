package sim

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
)

type routeEntry struct {
	handler   http.Handler
	available bool
}

// Router is an in-memory HTTP transport for simulator scenarios.
type Router struct {
	mu     sync.RWMutex
	routes map[string]routeEntry
}

func NewRouter() *Router {
	return &Router{
		routes: make(map[string]routeEntry),
	}
}

func (r *Router) Register(rawURL string, handler http.Handler) error {
	key, err := routeKey(rawURL)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes[key] = routeEntry{handler: handler, available: true}
	return nil
}

func (r *Router) Unregister(rawURL string) error {
	key, err := routeKey(rawURL)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.routes, key)
	return nil
}

func (r *Router) SetAvailable(rawURL string, available bool) error {
	key, err := routeKey(rawURL)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.routes[key]
	if !ok {
		return fmt.Errorf("route %s not found", rawURL)
	}
	entry.available = available
	r.routes[key] = entry
	return nil
}

func (r *Router) RoundTrip(req *http.Request) (*http.Response, error) {
	key := req.URL.Scheme + "://" + req.URL.Host

	r.mu.RLock()
	entry, ok := r.routes[key]
	r.mu.RUnlock()
	if !ok || !entry.available || entry.handler == nil {
		return nil, fmt.Errorf("route %s unavailable", key)
	}

	clone := req.Clone(req.Context())
	clone.RequestURI = req.URL.RequestURI()
	clone.Host = req.URL.Host

	recorder := httptest.NewRecorder()
	entry.handler.ServeHTTP(recorder, clone)
	resp := recorder.Result()
	resp.Request = req
	return resp, nil
}

func routeKey(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse route URL %q: %w", rawURL, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("route URL %q must include scheme and host", rawURL)
	}
	return parsed.Scheme + "://" + parsed.Host, nil
}
