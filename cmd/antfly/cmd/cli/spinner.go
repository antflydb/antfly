/*
Copyright 2025 The Antfly Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cli

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/term"
)

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// spinner renders an animated spinner with a message on stderr.
// It gracefully degrades to static text when stderr is not a terminal.
type spinner struct {
	w             io.Writer
	isTTY         bool
	mu            sync.Mutex
	message       string
	detail        string // optional secondary line (e.g. classification result)
	renderedLines int    // number of physical terminal lines currently rendered
	running       bool
	done          chan struct{}
	frame         int
}

func newSpinner(w io.Writer) *spinner {
	f, ok := w.(*os.File)
	isTTY := ok && term.IsTerminal(int(f.Fd()))
	return &spinner{
		w:     w,
		isTTY: isTTY,
		done:  make(chan struct{}),
	}
}

// start begins the spinner animation with the given message.
func (s *spinner) start(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.message = msg
	s.detail = ""

	if s.running {
		// Already running, just update message
		if s.isTTY {
			s.clear()
			s.renderedLines = 0
			s.render()
		} else {
			fmt.Fprintf(s.w, "%s\n", msg)
		}
		return
	}

	s.running = true
	s.renderedLines = 0
	s.done = make(chan struct{})

	if !s.isTTY {
		fmt.Fprintf(s.w, "%s\n", msg)
		return
	}

	s.render()
	go s.animate()
}

// setDetail sets a secondary detail line below the spinner.
func (s *spinner) setDetail(detail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.detail = detail
	if !s.isTTY && detail != "" {
		fmt.Fprintf(s.w, "  %s\n", detail)
	}
}

// log prints a line of text above the spinner without stopping it.
func (s *spinner) log(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isTTY && s.running {
		s.clear()
		s.renderedLines = 0
		fmt.Fprintln(s.w, msg)
		s.render()
	} else {
		fmt.Fprintln(s.w, msg)
	}
}

// stop stops the spinner and shows a completed message with a green checkmark.
func (s *spinner) stop(msg string) {
	s.finish("32", "✓", msg)
}

// stopError stops the spinner and shows an error message with a red cross.
func (s *spinner) stopError(msg string) {
	s.finish("31", "✗", msg)
}

// finish stops the spinner and prints a final status line. Must not hold s.mu.
func (s *spinner) finish(colorCode, icon, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return
	}

	s.running = false
	close(s.done)

	if s.isTTY {
		s.clear()
		fmt.Fprintf(s.w, "\033[%sm%s\033[0m %s\n", colorCode, icon, msg)
	} else {
		fmt.Fprintf(s.w, "%s %s\n", icon, msg)
	}
	s.renderedLines = 0
}

// clear erases all rendered physical lines and moves cursor to the start. Must hold s.mu.
func (s *spinner) clear() {
	// Clear current line, then move up and clear each previous line
	fmt.Fprint(s.w, "\r\033[K")
	for i := 1; i < s.renderedLines; i++ {
		fmt.Fprint(s.w, "\033[A\r\033[K")
	}
}

// termWidth returns the terminal width, or 80 if it cannot be determined.
func (s *spinner) termWidth() int {
	if f, ok := s.w.(*os.File); ok {
		if w, _, err := term.GetSize(int(f.Fd())); err == nil && w > 0 {
			return w
		}
	}
	return 80
}

// visibleLen returns the number of visible characters in a string,
// ignoring ANSI escape sequences.
func visibleLen(s string) int {
	n := 0
	inEsc := false
	for _, r := range s {
		if inEsc {
			if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
				inEsc = false
			}
			continue
		}
		if r == '\033' {
			inEsc = true
			continue
		}
		n++
	}
	return n
}

// physicalLines returns the number of physical terminal lines a string
// occupies, accounting for wrapping at the given terminal width.
func physicalLines(s string, width int) int {
	if width <= 0 {
		return 1
	}
	vLen := visibleLen(s)
	if vLen == 0 {
		return 1
	}
	return (vLen-1)/width + 1
}

// render writes the current spinner frame. Must hold s.mu.
func (s *spinner) render() {
	s.clear()
	w := s.termWidth()
	frame := spinnerFrames[s.frame%len(spinnerFrames)]
	line := fmt.Sprintf("\033[33m%s\033[0m %s", frame, s.message)
	fmt.Fprint(s.w, line)
	lines := physicalLines(line, w)
	if s.detail != "" {
		detail := fmt.Sprintf("  \033[2m%s\033[0m", s.detail)
		fmt.Fprintf(s.w, "\n%s", detail)
		lines += physicalLines(detail, w)
	}
	s.renderedLines = lines
}

func (s *spinner) animate() {
	ticker := time.NewTicker(80 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.mu.Lock()
			s.frame++
			s.render()
			s.mu.Unlock()
		}
	}
}
