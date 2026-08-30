package httpapi

import "net/http"

// statusResponseWriter observes the effective response status without changing
// the normal ResponseWriter contract. net/http defaults a response with no
// header or body to 200, and Write defaults a response with a body to 200.
type statusResponseWriter struct {
	http.ResponseWriter
	statusCode    int
	headerWritten bool
}

func (w *statusResponseWriter) WriteHeader(statusCode int) {
	// Informational responses do not determine the final response status. The
	// underlying writer still receives them, including repeated 1xx headers.
	if statusCode >= 100 && statusCode < 200 && statusCode != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(statusCode)
		return
	}
	if !w.headerWritten {
		w.statusCode = statusCode
		w.headerWritten = true
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *statusResponseWriter) Write(body []byte) (int, error) {
	if !w.headerWritten {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

// StatusCode returns the status that net/http will use when the handler has
// not written a response. Unmatched requests never reach this writer.
func (w *statusResponseWriter) StatusCode() int {
	if !w.headerWritten {
		return http.StatusOK
	}
	return w.statusCode
}

// Unwrap lets http.NewResponseController reach optional capabilities of the
// underlying writer without making unsupported capabilities appear available.
func (w *statusResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
