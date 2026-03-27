package memoryaf

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/antflydb/antfly/pkg/client/query"
)

const (
	serverName        = "memoryaf"
	serverVersion     = "0.1.0"
	serverDescription = "Shared team memory for AI agents"
)

//go:embed dashboard/*
var dashboardFiles embed.FS

// RequestUserContextFunc extracts a UserContext from an HTTP request.
type RequestUserContextFunc func(r *http.Request) (UserContext, error)

// HTTPHandler exposes memoryaf over a small JSON API and embedded dashboard.
type HTTPHandler struct {
	handler       *Handler
	userContextFn RequestUserContextFunc
	assets        fs.FS
}

type availabilityChecker interface {
	isAvailable(ctx context.Context) bool
}

// NewHTTPHandler creates a combined REST API and dashboard handler.
// If userContextFn is nil, request identity is taken from headers with safe defaults.
func NewHTTPHandler(handler *Handler, userContextFn RequestUserContextFunc) *HTTPHandler {
	if userContextFn == nil {
		userContextFn = DefaultRequestUserContext
	}
	assets, err := fs.Sub(dashboardFiles, "dashboard")
	if err != nil {
		panic(fmt.Sprintf("memoryaf: load embedded dashboard assets: %v", err))
	}
	return &HTTPHandler{
		handler:       handler,
		userContextFn: userContextFn,
		assets:        assets,
	}
}

// DefaultRequestUserContext reads identity from request headers.
func DefaultRequestUserContext(r *http.Request) (UserContext, error) {
	return UserContext{
		UserID:    firstHeader(r, "X-User-ID", "dashboard"),
		Namespace: firstHeader(r, "X-Namespace", "default"),
		Role:      firstHeader(r, "X-Role", "member"),
		AgentID:   r.Header.Get("X-Agent-ID"),
		DeviceID:  r.Header.Get("X-Device-ID"),
		SessionID: r.Header.Get("X-Session-ID"),
	}, nil
}

// ServeHTTP implements http.Handler.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/health" {
		setCORSHeaders(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}

	switch {
	case r.URL.Path == "/health":
		h.handleHealth(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/v1/"):
		h.serveAPI(w, r)
	default:
		h.serveDashboard(w, r)
	}
}

func (h *HTTPHandler) serveAPI(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/api/v1/info":
		h.handleInfo(w, r)
	case r.URL.Path == "/api/v1/stats":
		h.handleStats(w, r)
	case r.URL.Path == "/api/v1/sessions":
		h.handleSessions(w, r)
	case r.URL.Path == "/api/v1/memories":
		h.handleMemories(w, r)
	case r.URL.Path == "/api/v1/memories/search":
		h.handleSearchMemories(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/v1/memories/"):
		h.handleMemoryByPath(w, r)
	case r.URL.Path == "/api/v1/entities":
		h.handleEntities(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/v1/entities/"):
		h.handleEntityByPath(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/v1/sessions/") && strings.HasSuffix(r.URL.Path, "/end"):
		h.handleEndSession(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/") && strings.HasSuffix(r.URL.Path, "/init"):
		h.handleNamespaceInit(w, r)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (h *HTTPHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	uctx, _ := h.userContextFn(r)
	antflyAvailable := h.checkAntfly(r.Context(), uctx.Namespace)
	termiteAvailable := h.checkExtractor(r.Context())
	level := "ok"
	if !antflyAvailable {
		level = "unhealthy"
	} else if h.handler != nil && h.handler.extractor != nil && !termiteAvailable {
		level = "degraded"
	}
	status := HealthStatus{
		Status:  level,
		Antfly:  antflyAvailable,
		Termite: termiteAvailable,
	}
	writeJSON(w, http.StatusOK, status)
}

func (h *HTTPHandler) handleInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, ServerInfo{
		Name:        serverName,
		Version:     serverVersion,
		Description: serverDescription,
	})
}

func (h *HTTPHandler) handleNamespaceInit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	uctx, err := h.userContextFn(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if uctx.Role != "admin" {
		writeError(w, http.StatusForbidden, "forbidden: admin role required")
		return
	}

	raw := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/"), "/init")
	namespace, err := url.PathUnescape(strings.Trim(raw, "/"))
	if err != nil || namespace == "" {
		writeError(w, http.StatusBadRequest, "invalid namespace")
		return
	}

	if err := h.handler.InitNamespace(r.Context(), namespace); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"namespace": namespace, "status": "initialized"})
}

func (h *HTTPHandler) handleMemories(w http.ResponseWriter, r *http.Request) {
	uctx, err := h.userContextFn(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	switch r.Method {
	case http.MethodGet:
		args := ListMemoriesArgs{
			Project:       r.URL.Query().Get("project"),
			Tags:          splitCSV(r.URL.Query().Get("tags")),
			MemoryType:    r.URL.Query().Get("memory_type"),
			CreatedBy:     r.URL.Query().Get("created_by"),
			Visibility:    r.URL.Query().Get("visibility"),
			SourceBackend: r.URL.Query().Get("source_backend"),
			SourceID:      r.URL.Query().Get("source_id"),
			Limit:         parseInt(r.URL.Query().Get("limit"), 0),
			Offset:        parseInt(r.URL.Query().Get("offset"), 0),
			SessionID:     r.URL.Query().Get("session_id"),
			AgentID:       r.URL.Query().Get("agent_id"),
			DeviceID:      r.URL.Query().Get("device_id"),
			Ephemeral:     parseBool(r.URL.Query().Get("ephemeral")),
		}
		memories, err := h.handler.ListMemories(r.Context(), args, uctx)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, memories)
	case http.MethodPost:
		var args StoreMemoryArgs
		if err := decodeJSONBody(r, &args); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		memory, err := h.handler.StoreMemory(r.Context(), args, uctx)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, memory)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *HTTPHandler) handleMemoryByPath(w http.ResponseWriter, r *http.Request) {
	uctx, err := h.userContextFn(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/memories/")
	rest = strings.Trim(rest, "/")
	if rest == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	if strings.HasSuffix(rest, "/related") {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		id := strings.TrimSuffix(rest, "/related")
		id = strings.Trim(id, "/")
		results, err := h.handler.FindRelated(r.Context(), FindRelatedArgs{
			ID:        id,
			Depth:     parseInt(r.URL.Query().Get("depth"), 0),
			Limit:     parseInt(r.URL.Query().Get("limit"), 0),
			Ephemeral: parseBool(r.URL.Query().Get("ephemeral")),
		}, uctx)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, results)
		return
	}

	id, err := url.PathUnescape(rest)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid memory id")
		return
	}

	switch r.Method {
	case http.MethodGet:
		memory, err := h.handler.GetMemory(r.Context(), id, uctx)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, memory)
	case http.MethodPut:
		var args UpdateMemoryArgs
		if err := decodeJSONBody(r, &args); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		args.ID = id
		memory, err := h.handler.UpdateMemory(r.Context(), args, uctx)
		if err != nil {
			status := http.StatusBadRequest
			if strings.Contains(err.Error(), "forbidden") {
				status = http.StatusForbidden
			}
			writeError(w, status, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, memory)
	case http.MethodDelete:
		if err := h.handler.DeleteMemory(r.Context(), id, uctx); err != nil {
			status := http.StatusBadRequest
			if strings.Contains(err.Error(), "forbidden") {
				status = http.StatusForbidden
			}
			writeError(w, status, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "deleted"})
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *HTTPHandler) handleSearchMemories(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	uctx, err := h.userContextFn(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	var args SearchMemoriesArgs
	if err := decodeJSONBody(r, &args); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	results, err := h.handler.SearchMemories(r.Context(), args, uctx)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, results)
}

func (h *HTTPHandler) handleEntities(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	uctx, err := h.userContextFn(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	entities, err := h.handler.ListEntities(r.Context(), ListEntitiesArgs{
		Label:     r.URL.Query().Get("label"),
		Limit:     parseInt(r.URL.Query().Get("limit"), 0),
		Ephemeral: parseBool(r.URL.Query().Get("ephemeral")),
	}, uctx)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, entities)
}

func (h *HTTPHandler) handleEntityByPath(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	uctx, err := h.userContextFn(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/entities/")
	rest = strings.Trim(rest, "/")
	if !strings.HasSuffix(rest, "/memories") {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	entityRef := strings.Trim(strings.TrimSuffix(rest, "/memories"), "/")
	entityRef, err = url.PathUnescape(entityRef)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid entity identifier")
		return
	}
	parts := strings.SplitN(entityRef, ":", 2)
	if len(parts) != 2 {
		writeError(w, http.StatusBadRequest, "entity path must be <label>:<text>")
		return
	}

	results, err := h.handler.GetEntityMemories(r.Context(), EntityMemoriesArgs{
		EntityLabel: parts[0],
		EntityText:  parts[1],
		Limit:       parseInt(r.URL.Query().Get("limit"), 0),
		Ephemeral:   parseBool(r.URL.Query().Get("ephemeral")),
	}, uctx)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, results)
}

func (h *HTTPHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	uctx, err := h.userContextFn(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	stats, err := h.handler.GetStats(r.Context(), MemoryStatsArgs{
		Project:       r.URL.Query().Get("project"),
		SourceBackend: r.URL.Query().Get("source_backend"),
		SessionID:     r.URL.Query().Get("session_id"),
		AgentID:       r.URL.Query().Get("agent_id"),
		DeviceID:      r.URL.Query().Get("device_id"),
		Ephemeral:     parseBool(r.URL.Query().Get("ephemeral")),
	}, uctx)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (h *HTTPHandler) handleSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	uctx, err := h.userContextFn(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	sessions, err := h.handler.ListSessions(r.Context(), ListSessionsArgs{
		AgentID: r.URL.Query().Get("agent_id"),
		Limit:   parseInt(r.URL.Query().Get("limit"), 0),
	}, uctx)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sessions)
}

func (h *HTTPHandler) handleEndSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	uctx, err := h.userContextFn(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/sessions/")
	sessionID := strings.Trim(strings.TrimSuffix(rest, "/end"), "/")
	sessionID, err = url.PathUnescape(sessionID)
	if err != nil || sessionID == "" {
		writeError(w, http.StatusBadRequest, "invalid session id")
		return
	}

	if err := h.handler.EndSession(r.Context(), EndSessionArgs{SessionID: sessionID}, uctx); err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "forbidden") {
			status = http.StatusForbidden
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"session_id": sessionID, "status": "ended"})
}

func (h *HTTPHandler) serveDashboard(w http.ResponseWriter, r *http.Request) {
	assetPath := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if assetPath == "" || assetPath == "." {
		assetPath = "index.html"
	}

	data, err := fs.ReadFile(h.assets, assetPath)
	if err != nil {
		data, err = fs.ReadFile(h.assets, "index.html")
		if err != nil {
			writeError(w, http.StatusInternalServerError, "dashboard assets unavailable")
			return
		}
		assetPath = "index.html"
	}

	contentType := mime.TypeByExtension(path.Ext(assetPath))
	if contentType == "" {
		contentType = "text/html; charset=utf-8"
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write(data)
}

func decodeJSONBody(r *http.Request, dst any) error {
	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return fmt.Errorf("request body is required")
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return fmt.Errorf("decode json: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

func setCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-User-ID, X-Namespace, X-Role, X-Agent-ID, X-Device-ID, X-Session-ID")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseInt(raw string, def int) int {
	if strings.TrimSpace(raw) == "" {
		return def
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return value
}

func parseBool(raw string) bool {
	value, err := strconv.ParseBool(raw)
	return err == nil && value
}

func firstHeader(r *http.Request, name, def string) string {
	if value := strings.TrimSpace(r.Header.Get(name)); value != "" {
		return value
	}
	return def
}

func (h *HTTPHandler) checkAntfly(ctx context.Context, namespace string) bool {
	if h.handler == nil || h.handler.client == nil {
		return false
	}
	if namespace == "" {
		namespace = "default"
	}
	resp, err := h.handler.client.QueryWithBody(ctx, mustMarshal(map[string]any{
		"table":            tableName(namespace),
		"full_text_search": json.RawMessage(mustMarshal(query.NewMatchAll())),
		"limit":            0,
		"count":            true,
	}))
	if err != nil {
		return false
	}
	_, err = parseResponse(resp)
	return err == nil
}

func (h *HTTPHandler) checkExtractor(ctx context.Context) bool {
	if h.handler == nil || h.handler.extractor == nil {
		return false
	}
	if checker, ok := h.handler.extractor.(availabilityChecker); ok {
		return checker.isAvailable(ctx)
	}
	return true
}
