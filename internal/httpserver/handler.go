package httpserver

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

// Handler exposes operational endpoints (/healthz, /readyz) for the
// control-panel-service. Business APIs are added in later sections.
type Handler struct {
	ready atomic.Bool
}

func NewHandler() *Handler {
	return &Handler{}
}

// MarkReady flips /readyz to 200. Call once startup deps (DB, etc.) are wired.
func (h *Handler) MarkReady()    { h.ready.Store(true) }
func (h *Handler) MarkNotReady() { h.ready.Store(false) }

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", h.handleHealthz)
	mux.HandleFunc("/readyz", h.handleReadyz)
}

func (h *Handler) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !h.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func NewMux(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
