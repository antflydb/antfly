// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"archive/zip"
	"bufio"
	"container/heap"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ajroetker/pdf"
	"github.com/pdfcpu/pdfcpu/pkg/api"
)

const corpusMaxBytes = 128 << 20

type corpusSource struct {
	Dataset string `json:"dataset"`
	Path    string `json:"path"`
	Zip     bool   `json:"zip,omitempty"`
}
type corpusConfig struct {
	Sources        []corpusSource `json:"sources"`
	Key            string         `json:"key"`
	BaseURL        string         `json:"base_url"`
	PagesPerRecord int            `json:"pages_per_record"`
	LimitPages     int            `json:"limit_pages"`
	LimitFiles     int            `json:"limit_files"`
}
type sourceLocator struct {
	Source   int    `json:"source"`
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Modified int64  `json:"modified"`
	CRC      uint32 `json:"crc,omitempty"`
	First    int    `json:"first,omitempty"`
	Last     int    `json:"last,omitempty"`
}
type corpusEntry struct {
	ID      string        `json:"id"`
	Dataset string        `json:"dataset"`
	MIME    string        `json:"mime_type"`
	Locator sourceLocator `json:"locator"`
}
type corpusRecord struct {
	ID       string         `json:"id"`
	Document map[string]any `json:"document"`
}

func corpusMIME(name string) string {
	switch strings.ToLower(path.Ext(name)) {
	case ".pdf":
		return "application/pdf"
	case ".wav":
		return "audio/wav"
	case ".mp3":
		return "audio/mpeg"
	case ".m4a":
		return "audio/mp4"
	case ".aif", ".aiff":
		return "audio/aiff"
	case ".caf":
		return "audio/x-caf"
	case ".flac":
		return "audio/flac"
	}
	return ""
}

var eftaName = regexp.MustCompile(`(?i)^EFTA[0-9]+$`)
var datasetName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func corpusID(dataset, name string) string {
	base := path.Base(name)
	stem := strings.TrimSuffix(base, path.Ext(base))
	if eftaName.MatchString(stem) {
		return dataset + ":" + strings.ToUpper(stem) + strings.ToLower(path.Ext(base))
	}
	sum := sha256.Sum256([]byte(name))
	return dataset + ":" + hex.EncodeToString(sum[:16])
}
func includedSource(name string) bool {
	if corpusMIME(name) == "" || strings.HasPrefix(path.Base(name), "._") {
		return false
	}
	for _, part := range strings.Split(filepath.ToSlash(name), "/") {
		if part == "pages" || part == "__MACOSX" || strings.HasPrefix(part, ".") {
			return false
		}
	}
	return true
}

// A bounded external merge sort deduplicates source IDs without keeping the corpus
// in a map. Only duplicate candidates require a full content comparison.
func discoverCorpus(cfg corpusConfig, output string) error {
	scratch, err := os.MkdirTemp(filepath.Dir(output), "inventory-sort-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	var runs []string
	entries := make([]corpusEntry, 0, 4096)
	flush := func() error {
		if len(entries) == 0 {
			return nil
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].ID == entries[j].ID {
				return entries[i].Locator.Source < entries[j].Locator.Source
			}
			return entries[i].ID < entries[j].ID
		})
		f, err := os.CreateTemp(scratch, "run-")
		if err != nil {
			return err
		}
		enc := json.NewEncoder(f)
		for _, entry := range entries {
			if err = enc.Encode(entry); err != nil {
				f.Close()
				return err
			}
		}
		err = f.Close()
		runs = append(runs, f.Name())
		entries = entries[:0]
		return err
	}
	add := func(i int, name string, size, modified int64, crc uint32) error {
		if !includedSource(name) {
			return nil
		}
		entries = append(entries, corpusEntry{ID: corpusID(cfg.Sources[i].Dataset, name), Dataset: cfg.Sources[i].Dataset, MIME: corpusMIME(name), Locator: sourceLocator{Source: i, Name: name, Size: size, Modified: modified, CRC: crc}})
		if len(entries) == cap(entries) {
			return flush()
		}
		return nil
	}
	for i, s := range cfg.Sources {
		if s.Zip {
			z, err := zip.OpenReader(s.Path)
			if err != nil {
				return err
			}
			info, err := os.Stat(s.Path)
			if err != nil {
				z.Close()
				return err
			}
			for _, f := range z.File {
				if !f.FileInfo().IsDir() {
					if err = add(i, f.Name, int64(f.UncompressedSize64), info.ModTime().UnixNano(), f.CRC32); err != nil {
						z.Close()
						return err
					}
				}
			}
			if err = z.Close(); err != nil {
				return err
			}
		} else {
			err = filepath.WalkDir(s.Path, func(p string, d fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				rel, e := filepath.Rel(s.Path, p)
				if e != nil {
					return e
				}
				if d.IsDir() {
					if rel != "." && (d.Name() == "pages" || strings.HasPrefix(d.Name(), ".")) {
						return filepath.SkipDir
					}
					return nil
				}
				if !d.Type().IsRegular() {
					return nil
				}
				info, e := d.Info()
				if e != nil {
					return e
				}
				return add(i, filepath.ToSlash(rel), info.Size(), info.ModTime().UnixNano(), 0)
			})
			if err != nil {
				return err
			}
		}
	}
	if err = flush(); err != nil {
		return err
	}
	// Bound the merge fan-in, including file descriptors, for million-file inventories.
	for len(runs) > 64 {
		var next []string
		for start := 0; start < len(runs); start += 64 {
			end := min(start+64, len(runs))
			out := filepath.Join(scratch, fmt.Sprintf("merge-%d-%d", len(runs), start))
			if err = mergeCorpusRuns(cfg, runs[start:end], out, false); err != nil {
				return err
			}
			next = append(next, out)
		}
		for _, r := range runs {
			os.Remove(r)
		}
		runs = next
	}
	return mergeCorpusRuns(cfg, runs, output, true)
}

type corpusRun struct {
	f      *os.File
	reader *bufio.Reader
	entry  corpusEntry
}
type corpusHeap []*corpusRun

func (h corpusHeap) Len() int           { return len(h) }
func (h corpusHeap) Less(i, j int) bool { return h[i].entry.ID < h[j].entry.ID }
func (h corpusHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *corpusHeap) Push(v any)        { *h = append(*h, v.(*corpusRun)) }
func (h *corpusHeap) Pop() any          { old := *h; v := old[len(old)-1]; *h = old[:len(old)-1]; return v }
func readCorpusLine[T any](r *bufio.Reader, v *T) (int64, error) {
	var line []byte
	for {
		fragment, err := r.ReadSlice('\n')
		if len(line)+len(fragment) > 1<<20 {
			return 0, fmt.Errorf("manifest line exceeds 1 MiB")
		}
		line = append(line, fragment...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			if len(line) > 0 && errors.Is(err, io.EOF) {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, err
		}
		break
	}
	err := error(nil)
	if err = json.Unmarshal(line, v); err != nil {
		return 0, err
	}
	return int64(len(line)), nil
}
func mergeCorpusRuns(cfg corpusConfig, paths []string, output string, dedup bool) error {
	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()
	var h corpusHeap
	var opened []*os.File
	defer func() {
		for _, file := range opened {
			file.Close()
		}
	}()
	for _, p := range paths {
		input, e := os.Open(p)
		if e != nil {
			return e
		}
		opened = append(opened, input)
		run := &corpusRun{f: input, reader: bufio.NewReader(input)}
		if _, e = readCorpusLine(run.reader, &run.entry); e == nil {
			heap.Push(&h, run)
		} else if !errors.Is(e, io.EOF) {
			return e
		}
	}
	store := newCorpusStore(cfg)
	defer store.Close()
	enc := json.NewEncoder(f)
	var previous *corpusEntry
	for len(h) > 0 {
		run := heap.Pop(&h).(*corpusRun)
		entry := run.entry
		if dedup && previous != nil && previous.ID == entry.ID {
			a, e := store.digest(previous.Locator)
			if e != nil {
				return e
			}
			b, e := store.digest(entry.Locator)
			if e != nil {
				return e
			}
			if a != b {
				return fmt.Errorf("conflicting copies of %s; use distinct dataset labels", entry.ID)
			}
		} else {
			if err = enc.Encode(entry); err != nil {
				return err
			}
			copy := entry
			previous = &copy
		}
		if _, err = readCorpusLine(run.reader, &run.entry); err == nil {
			heap.Push(&h, run)
		} else if !errors.Is(err, io.EOF) {
			return err
		}
	}
	return f.Sync()
}

// Keep one ZIP central directory open. This avoids repeatedly parsing enormous
// archives while bounding cache memory. Active readers retain their archive when
// another request switches the cached source; transfers never hold the cache lock.
type corpusStore struct {
	cfg corpusConfig
	mu  sync.Mutex
	zip *corpusArchive
}

type corpusArchive struct {
	reader  *zip.ReadCloser
	source  int
	files   map[string]*zip.File
	readers int
}

type corpusArchiveReader struct {
	io.ReadCloser
	store   *corpusStore
	archive *corpusArchive
	once    sync.Once
	err     error
}

func (r *corpusArchiveReader) Close() error {
	r.once.Do(func() {
		r.err = r.ReadCloser.Close()
		r.store.mu.Lock()
		defer r.store.mu.Unlock()
		r.archive.readers--
		if r.archive != r.store.zip && r.archive.readers == 0 {
			r.archive.reader.Close()
		}
	})
	return r.err
}

func newCorpusStore(cfg corpusConfig) *corpusStore { return &corpusStore{cfg: cfg} }
func (s *corpusStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeArchive()
}

// Caller holds mu. Readers of an evicted archive close it when the last finishes.
func (s *corpusStore) closeArchive() {
	if s.zip != nil {
		if s.zip.readers == 0 {
			s.zip.reader.Close()
		}
		s.zip = nil
	}
}
func (s *corpusStore) open(loc sourceLocator) (io.ReadCloser, error) {
	if loc.Source < 0 || loc.Source >= len(s.cfg.Sources) || !includedSource(loc.Name) || !fs.ValidPath(loc.Name) {
		return nil, fmt.Errorf("invalid source locator")
	}
	source := s.cfg.Sources[loc.Source]
	if source.Zip {
		s.mu.Lock()
		defer s.mu.Unlock()
		stat, err := os.Stat(source.Path)
		if err != nil {
			return nil, err
		}
		if stat.ModTime().UnixNano() != loc.Modified {
			return nil, fmt.Errorf("archive changed; prepare a new corpus state")
		}
		if s.zip == nil || s.zip.source != loc.Source {
			reader, err := zip.OpenReader(source.Path)
			if err != nil {
				return nil, err
			}
			files := make(map[string]*zip.File, len(reader.File))
			for _, f := range reader.File {
				if _, exists := files[f.Name]; exists {
					reader.Close()
					return nil, fmt.Errorf("duplicate ZIP member %q", f.Name)
				}
				files[f.Name] = f
			}
			s.closeArchive()
			s.zip = &corpusArchive{reader: reader, source: loc.Source, files: files}
		}
		f := s.zip.files[loc.Name]
		if f == nil || int64(f.UncompressedSize64) != loc.Size || f.CRC32 != loc.CRC {
			return nil, fmt.Errorf("ZIP member changed")
		}
		reader, err := f.Open()
		if err != nil {
			return nil, err
		}
		s.zip.readers++
		return &corpusArchiveReader{ReadCloser: reader, store: s, archive: s.zip}, nil
	}
	root, err := os.OpenRoot(source.Path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	f, err := root.Open(loc.Name)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != loc.Size || info.ModTime().UnixNano() != loc.Modified {
		f.Close()
		return nil, fmt.Errorf("source changed; prepare a new corpus state")
	}
	return f, nil
}
func (s *corpusStore) digest(loc sourceLocator) (string, error) {
	r, err := s.open(loc)
	if err != nil {
		return "", err
	}
	defer r.Close()
	h := sha256.New()
	_, err = io.Copy(h, r)
	return hex.EncodeToString(h.Sum(nil)), err
}
func (s *corpusStore) seekable(loc sourceLocator) (*os.File, func(), error) {
	r, err := s.open(loc)
	if err != nil {
		return nil, nil, err
	}
	if f, ok := r.(*os.File); ok {
		return f, func() { f.Close() }, nil
	}
	f, err := os.CreateTemp("", "epstein-source-*.pdf")
	if err != nil {
		r.Close()
		return nil, nil, err
	}
	cleanup := func() { f.Close(); os.Remove(f.Name()) }
	_, err = io.Copy(f, io.LimitReader(r, loc.Size+1))
	closeErr := r.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, nil, err
	}
	return f, cleanup, nil
}
func (s *corpusStore) pages(loc sourceLocator) (int, error) {
	f, cleanup, err := s.seekable(loc)
	if err != nil {
		return 0, err
	}
	defer cleanup()
	return corpusPDFPages(f, loc.Size)
}

func corpusPDFPages(f *os.File, size int64) (int, error) {
	doc, err := pdf.NewReader(f, size)
	if err == nil {
		return doc.NumPage(), nil
	}
	// The fast reader rejects headers with trailing whitespace found in DOJ PDFs.
	// Fall back to the parser already used for page-range extraction, preserving
	// the original bytes and their xref offsets.
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	defer f.Seek(0, io.SeekStart)
	return api.PageCount(f, nil)
}
func sourceToken(cfg corpusConfig, loc sourceLocator) string {
	data, _ := json.Marshal(loc)
	payload := base64.RawURLEncoding.EncodeToString(data)
	mac := hmac.New(sha256.New, []byte(cfg.Key))
	mac.Write([]byte(payload))
	return payload + "." + hex.EncodeToString(mac.Sum(nil))
}
func sourceURL(cfg corpusConfig, loc sourceLocator) string {
	return strings.TrimRight(cfg.BaseURL, "/") + "/sources/" + sourceToken(cfg, loc)
}
func (s *corpusStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/sources/"), ".")
	if len(parts) != 2 {
		http.Error(w, "invalid source token", http.StatusForbidden)
		return
	}
	mac := hmac.New(sha256.New, []byte(s.cfg.Key))
	mac.Write([]byte(parts[0]))
	sig, err := hex.DecodeString(parts[1])
	if err != nil || !hmac.Equal(sig, mac.Sum(nil)) {
		http.Error(w, "invalid source token", http.StatusForbidden)
		return
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[0])
	var loc sourceLocator
	if err != nil || json.Unmarshal(data, &loc) != nil {
		http.Error(w, "invalid source token", http.StatusForbidden)
		return
	}
	if loc.First < 0 || loc.Last < loc.First || ((loc.First == 0) != (loc.Last == 0)) {
		http.Error(w, "invalid page range", 400)
		return
	}
	if loc.First > 0 && corpusMIME(loc.Name) != "application/pdf" {
		http.Error(w, "invalid page range", 400)
		return
	}
	w.Header().Set("Content-Type", corpusMIME(loc.Name))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", path.Base(loc.Name)))
	if loc.First > 0 {
		source, cleanup, e := s.seekable(loc)
		if e != nil {
			http.Error(w, e.Error(), 409)
			return
		}
		defer cleanup()
		if loc.First == 1 && loc.Size <= corpusMaxBytes {
			pages, parseErr := corpusPDFPages(source, loc.Size)
			if parseErr == nil && pages == loc.Last {
				http.ServeContent(w, r, path.Base(loc.Name), time.Time{}, source)
				return
			}
		}
		output, e := os.CreateTemp("", "epstein-range-*.pdf")
		if e != nil {
			http.Error(w, "temporary file failed", 500)
			return
		}
		defer func() { output.Close(); os.Remove(output.Name()) }()
		if e = api.Trim(source, output, []string{fmt.Sprintf("%d-%d", loc.First, loc.Last)}, nil); e != nil {
			http.Error(w, "PDF page extraction failed", 422)
			return
		}
		info, e := output.Stat()
		if e != nil || info.Size() > corpusMaxBytes {
			http.Error(w, "PDF range exceeds 128 MiB; reduce --pages-per-record", 413)
			return
		}
		http.ServeContent(w, r, path.Base(loc.Name), time.Time{}, output)
		return
	}
	reader, err := s.open(loc)
	if err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	defer reader.Close()
	if f, ok := reader.(*os.File); ok {
		http.ServeContent(w, r, path.Base(loc.Name), time.Time{}, f)
		return
	}
	// ZIP streams do not offer random access. Audio is bounded and spooled to disk
	// for browser range requests; original PDFs can be streamed without splitting.
	if strings.HasPrefix(corpusMIME(loc.Name), "audio/") {
		f, e := os.CreateTemp("", "epstein-audio-*")
		if e != nil {
			http.Error(w, "temporary file failed", 500)
			return
		}
		defer func() { f.Close(); os.Remove(f.Name()) }()
		if loc.Size > corpusMaxBytes {
			http.Error(w, "audio exceeds 128 MiB", 413)
			return
		}
		if _, e = io.Copy(f, io.LimitReader(reader, corpusMaxBytes+1)); e != nil {
			http.Error(w, "audio read failed", 500)
			return
		}
		http.ServeContent(w, r, path.Base(loc.Name), time.Time{}, f)
		return
	}
	w.Header().Set("Content-Length", fmt.Sprint(loc.Size))
	if r.Method == http.MethodGet {
		io.Copy(w, reader)
	}
}
func newCorpusKey() (string, error) {
	var b [32]byte
	_, err := rand.Read(b[:])
	return hex.EncodeToString(b[:]), err
}
