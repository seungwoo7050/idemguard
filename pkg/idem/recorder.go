package idem

import (
	"bytes"
	"net/http"
	"strings"

	"github.com/felixge/httpsnoop"
)

// DefaultMaxCaptureBytes limits response body buffering to 1MB.
const DefaultMaxCaptureBytes = 1024 * 1024

// ResponseRecorder intercepts and captures response status code, headers, and body.
type ResponseRecorder struct {
	http.ResponseWriter
	underlying      http.ResponseWriter
	statusCode      int
	writtenHeader   bool
	body            bytes.Buffer
	maxCaptureBytes int
	overflow        bool
}

// NewResponseRecorder wraps w and limits captured body bytes to maxCaptureBytes.
func NewResponseRecorder(w http.ResponseWriter, maxCaptureBytes int) *ResponseRecorder {
	if maxCaptureBytes <= 0 {
		maxCaptureBytes = DefaultMaxCaptureBytes
	}
	rec := &ResponseRecorder{
		underlying:      w,
		statusCode:      http.StatusOK,
		maxCaptureBytes: maxCaptureBytes,
	}
	rec.ResponseWriter = httpsnoop.Wrap(w, httpsnoop.Hooks{
		WriteHeader: func(next httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
			return func(code int) {
				if !rec.writtenHeader {
					rec.statusCode = code
					rec.writtenHeader = true
				}
				next(code)
			}
		},
		Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(p []byte) (int, error) {
				if !rec.writtenHeader {
					rec.writtenHeader = true
				}
				if !rec.overflow {
					remaining := rec.maxCaptureBytes - rec.body.Len()
					if remaining >= len(p) {
						rec.body.Write(p)
					} else {
						if remaining > 0 {
							rec.body.Write(p[:remaining])
						}
						rec.overflow = true
					}
				}
				return next(p)
			}
		},
	})
	return rec
}

// Flush forwards flush requests to the underlying writer if it implements http.Flusher.
func (r *ResponseRecorder) Flush() {
	if flusher, ok := r.underlying.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Unwrap returns the underlying ResponseWriter.
func (r *ResponseRecorder) Unwrap() http.ResponseWriter {
	return r.underlying
}

// StatusCode returns the captured HTTP status code.
func (r *ResponseRecorder) StatusCode() int {
	return r.statusCode
}

// CapturedBody returns a copy of the captured response body bytes.
func (r *ResponseRecorder) CapturedBody() []byte {
	return r.body.Bytes()
}

// IsOverflow reports whether the response body exceeded MaxCaptureBytes.
func (r *ResponseRecorder) IsOverflow() bool {
	return r.overflow
}

// CapturedHeaders extracts single-value headers suitable for idempotency persistence,
// filtering out hop-by-hop transport headers.
func (r *ResponseRecorder) CapturedHeaders() map[string]string {
	h := r.Header()
	out := make(map[string]string, len(h))
	for k, vals := range h {
		lower := strings.ToLower(k)
		if lower == "connection" || lower == "transfer-encoding" || lower == "trailer" || lower == "upgrade" {
			continue
		}
		if len(vals) > 0 {
			out[k] = strings.Join(vals, ", ")
		}
	}
	return out
}

// ReplayTo writes the captured response to another http.ResponseWriter.
func (r *ResponseRecorder) ReplayTo(w http.ResponseWriter) {
	for k, v := range r.CapturedHeaders() {
		w.Header().Set(k, v)
	}
	w.Header().Set("X-Idempotent-Replayed", "true")
	w.WriteHeader(r.statusCode)
	body := r.CapturedBody()
	if len(body) > 0 {
		_, _ = w.Write(body)
	}
}
