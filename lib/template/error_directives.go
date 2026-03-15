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

package template

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Error directives are structured markers embedded in rendered template output
// by helpers (e.g., remoteMedia) when they encounter errors. The format follows
// the existing <<<...>>> directive pattern used by dotprompt media markers.
//
// Format:
//   <<<error:status=404 message=Not Found>>>
//   <<<error:message=connection refused>>>
//
// Helpers report facts (status code, message). Callers decide semantics
// (permanent vs transient) based on the status code.

// errorDirectiveRe matches <<<error:status=NNN message=...>>> or <<<error:message=...>>>
var errorDirectiveRe = regexp.MustCompile(`<<<error:(?:status=(\d+) )?message=(.+?)>>>`)

// ErrorDirective represents a parsed error directive from rendered template output.
type ErrorDirective struct {
	Status  int // HTTP status code, or 0 for non-HTTP errors
	Message string
}

// IsPermanent returns true if the directive's HTTP status indicates the resource
// is permanently unavailable and should not be retried.
func (d ErrorDirective) IsPermanent() bool {
	return d.Status == 401 || d.Status == 403 || d.Status == 404 || d.Status == 410
}

// sanitizeDirectiveMessage removes sequences that would break directive parsing.
func sanitizeDirectiveMessage(msg string) string {
	return strings.ReplaceAll(msg, ">>>", ">>\\>")
}

// FormatErrorDirective formats an error directive for embedding in rendered output.
// If status is 0, the status field is omitted.
func FormatErrorDirective(status int, message string) string {
	message = sanitizeDirectiveMessage(message)
	if status > 0 {
		return fmt.Sprintf("<<<error:status=%d message=%s>>>", status, message)
	}
	return fmt.Sprintf("<<<error:message=%s>>>", message)
}

// ParseErrorDirectives extracts all error directives from a string.
func ParseErrorDirectives(s string) []ErrorDirective {
	matches := errorDirectiveRe.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return nil
	}

	directives := make([]ErrorDirective, 0, len(matches))
	for _, m := range matches {
		var status int
		if m[1] != "" {
			status, _ = strconv.Atoi(m[1])
		}
		directives = append(directives, ErrorDirective{
			Status:  status,
			Message: m[2],
		})
	}
	return directives
}

// ContainsErrorDirective returns true if the string contains any error directive.
func ContainsErrorDirective(s string) bool {
	return errorDirectiveRe.MatchString(s)
}

// StripErrorDirectives removes all error directives from a string.
func StripErrorDirectives(s string) string {
	return errorDirectiveRe.ReplaceAllString(s, "")
}
