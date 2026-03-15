// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml

package middleware

import (
	"net/http"
	"strconv"
	"strings"
)

// CORSMiddleware returns a middleware that adds CORS headers with default configuration
// (allows all origins). Use CORSMiddlewareWithConfig for custom configuration.
func CORSMiddleware(next http.Handler) http.Handler {
	return CORSMiddlewareWithConfig(next, nil)
}

// CORSMiddlewareWithConfig returns a middleware that adds CORS headers based on the provided configuration.
// If corsConfig is nil or fields are empty, sensible defaults are used:
//   - AllowedOrigins: ["*"]
//   - AllowedMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]
//   - AllowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "Accept", "Origin"]
//   - MaxAge: 3600 seconds
func CORSMiddlewareWithConfig(next http.Handler, corsConfig *CORSConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip CORS if explicitly disabled
		if corsConfig != nil && !corsConfig.Enabled {
			next.ServeHTTP(w, r)
			return
		}

		// Set default values if corsConfig is nil or fields are empty
		allowedOrigins := []string{"*"}
		allowedMethods := []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"}
		allowedHeaders := []string{
			"Content-Type",
			"Authorization",
			"X-Requested-With",
			"Accept",
			"Origin",
		}
		exposedHeaders := []string{}
		allowCredentials := false
		maxAge := 3600

		// Override with config values if provided
		if corsConfig != nil {
			if len(corsConfig.AllowedOrigins) > 0 {
				allowedOrigins = corsConfig.AllowedOrigins
			}
			if len(corsConfig.AllowedMethods) > 0 {
				allowedMethods = corsConfig.AllowedMethods
			}
			if len(corsConfig.AllowedHeaders) > 0 {
				allowedHeaders = corsConfig.AllowedHeaders
			}
			if len(corsConfig.ExposedHeaders) > 0 {
				exposedHeaders = corsConfig.ExposedHeaders
			}
			allowCredentials = corsConfig.AllowCredentials
			if corsConfig.MaxAge > 0 {
				maxAge = corsConfig.MaxAge
			}
		}

		// Handle origin
		origin := r.Header.Get("Origin")
		if origin != "" {
			// Check if origin is allowed
			originAllowed := false
			for _, allowedOrigin := range allowedOrigins {
				if allowedOrigin == "*" || allowedOrigin == origin {
					originAllowed = true
					if allowedOrigin == "*" {
						w.Header().Set("Access-Control-Allow-Origin", "*")
					} else {
						w.Header().Set("Access-Control-Allow-Origin", origin)
						// When using specific origins, also set Vary header
						w.Header().Set("Vary", "Origin")
					}
					break
				}
			}

			if !originAllowed {
				// Origin not allowed, but continue without CORS headers
				next.ServeHTTP(w, r)
				return
			}
		}

		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Methods", strings.Join(allowedMethods, ", "))
		w.Header().Set("Access-Control-Allow-Headers", strings.Join(allowedHeaders, ", "))

		if len(exposedHeaders) > 0 {
			w.Header().Set("Access-Control-Expose-Headers", strings.Join(exposedHeaders, ", "))
		}

		if allowCredentials {
			// Credentials cannot be used with wildcard origin
			if len(allowedOrigins) == 1 && allowedOrigins[0] == "*" {
				// Log warning but continue
				// In production, this should be caught by config validation
			} else {
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}
		}

		if maxAge > 0 {
			w.Header().Set("Access-Control-Max-Age", strconv.Itoa(maxAge))
		}

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
