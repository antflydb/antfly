// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

// Raw preservation is deliberately independent of corpus records and source
// jobs. No stage-originals path contacts Antfly or triggers enrichment.
type originalRecord struct {
	Dataset    string          `json:"dataset"`
	Name       string          `json:"name"`
	Container  string          `json:"container"`
	MIME       string          `json:"mime_type"`
	Bytes      int64           `json:"bytes"`
	SHA256     string          `json:"sha256"`
	MD5        []byte          `json:"md5"`
	CRC32C     uint32          `json:"crc32c"`
	Object     string          `json:"object"`
	Generation string          `json:"generation,omitempty"`
	Archive    *archiveReceipt `json:"archive_source,omitempty"`
}

type originalPut func(context.Context, string, string, io.ReadSeeker, originalRecord) (string, error)

func originalMIME(name string) string {
	if mime := corpusMIME(name); mime != "" {
		return mime
	}
	switch strings.ToLower(path.Ext(name)) {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".tif", ".tiff":
		return "image/tiff"
	case ".webp":
		return "image/webp"
	case ".bmp":
		return "image/bmp"
	case ".mp4", ".m4v":
		return "video/mp4"
	case ".mov":
		return "video/quicktime"
	case ".avi":
		return "video/x-msvideo"
	case ".mkv":
		return "video/x-matroska"
	case ".webm":
		return "video/webm"
	case ".mpg", ".mpeg":
		return "video/mpeg"
	default:
		return "application/octet-stream"
	}
}

func stageOriginalsCmd(args []string) error {
	f := flag.NewFlagSet("stage-originals", flag.ContinueOnError)
	state := f.String("state", "./grepstein-originals", "Raw catalog and bounded spool directory")
	bucket := f.String("bucket", "", "Private originals bucket; required for uploads")
	inventory := f.Bool("inventory-only", false, "Hash and catalog originals without uploading")
	budget := f.Int64("max-bytes", 10<<30, "Maximum total uncompressed source bytes, including existing objects")
	maxObject := f.Int64("max-object-bytes", 8<<30, "Maximum bytes in one original/spooled ZIP member")
	var selected StringSliceFlag
	f.Var(&selected, "source", "Repeatable dataset=directory or dataset=archive.zip; any release may be stored")
	if err := f.Parse(args); err != nil {
		return err
	}
	if len(selected) == 0 || *budget < 1 || *maxObject < 1 {
		return fmt.Errorf("sources and positive byte budgets are required")
	}
	if !*inventory && (*bucket == "" || strings.ContainsAny(*bucket, "/?#:")) {
		return fmt.Errorf("a private GCS bucket is required")
	}
	var sources []corpusSource
	for _, arg := range selected {
		dataset, source, ok := strings.Cut(arg, "=")
		if !ok || !datasetName.MatchString(dataset) {
			return fmt.Errorf("use dataset=directory or dataset=archive.zip")
		}
		absolute, err := filepath.Abs(source)
		if err != nil {
			return err
		}
		info, err := os.Stat(absolute)
		if err != nil {
			return err
		}
		isZip := !info.IsDir() && strings.EqualFold(filepath.Ext(absolute), ".zip")
		if !info.IsDir() && !isZip {
			return fmt.Errorf("source must be a directory or ZIP")
		}
		sources = append(sources, corpusSource{Dataset: dataset, Path: absolute, Zip: isZip})
	}
	if err := os.MkdirAll(*state, 0700); err != nil {
		return err
	}
	statePath, err := filepath.EvalSymlinks(*state)
	if err != nil {
		return err
	}
	statePath, err = filepath.Abs(statePath)
	if err != nil {
		return err
	}
	for _, source := range sources {
		if source.Zip {
			continue
		}
		root, err := filepath.EvalSymlinks(source.Path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, statePath)
		if err != nil {
			return err
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("staging state must be outside source directories")
		}
	}
	unlock, err := corpusLock(*state)
	if err != nil {
		return err
	}
	defer unlock()
	ctx := context.Background()
	var put originalPut
	if !*inventory {
		client, err := storage.NewClient(ctx)
		if err != nil {
			return err
		}
		defer client.Close()
		put = originalGCSWriter(client)
	}
	return stageOriginals(ctx, *state, *bucket, sources, *budget, *maxObject, put)
}

func stageOriginals(ctx context.Context, state, bucket string, sources []corpusSource, budget, maxObject int64, put originalPut) error {
	output, err := os.CreateTemp(state, ".originals-*")
	if err != nil {
		return err
	}
	defer os.Remove(output.Name())
	defer output.Close()
	hash := sha256.New()
	encoder := json.NewEncoder(io.MultiWriter(output, hash))
	var total int64
	count := 0
	types := map[string]int{}
	seen := map[string]bool{}
	for _, source := range sources {
		// Known acquisition receipts are verified before using the source. Other
		// explicitly supplied releases are retained without asserting authority.
		provenance, err := archiveProvenance(source)
		if err != nil {
			return err
		}
		visit := func(name string, size int64, open func() (io.ReadCloser, error)) error {
			if count >= 2_000_000 {
				return fmt.Errorf("raw staging exceeds the two-million-file inventory budget; use separate source batches")
			}
			if !fs.ValidPath(name) || strings.Contains(name, "\\") {
				return fmt.Errorf("unsafe source member %q", name)
			}
			id := source.Dataset + "/" + name
			if seen[id] {
				return fmt.Errorf("duplicate original path %s", id)
			}
			seen[id] = true
			if size < 0 || size > maxObject || size > budget-total {
				return fmt.Errorf("original %s exceeds staging byte budget", id)
			}
			reader, err := open()
			if err != nil {
				return err
			}
			defer reader.Close()
			record := originalRecord{Dataset: source.Dataset, Name: name, Container: filepath.Base(source.Path), MIME: originalMIME(name), Bytes: size, Archive: provenance}
			sha, md, crc := sha256.New(), md5.New(), crc32.New(crc32.MakeTable(crc32.Castagnoli))
			writers := []io.Writer{sha, md, crc}
			var spool *os.File
			if put != nil {
				spool, err = os.CreateTemp(state, ".original-spool-*")
				if err != nil {
					return err
				}
				defer os.Remove(spool.Name())
				defer spool.Close()
				writers = append(writers, spool)
			}
			n, err := io.Copy(io.MultiWriter(writers...), io.LimitReader(reader, size+1))
			if err != nil {
				return err
			}
			if n != size {
				return fmt.Errorf("original size changed: %s", id)
			}
			if err := reader.Close(); err != nil {
				return err
			}
			record.SHA256 = hex.EncodeToString(sha.Sum(nil))
			record.MD5 = md.Sum(nil)
			record.CRC32C = crc.Sum32()
			record.Object = "objects/sha256/" + record.SHA256 + strings.ToLower(path.Ext(name))
			if put != nil {
				if _, err = spool.Seek(0, io.SeekStart); err != nil {
					return err
				}
				record.Generation, err = put(ctx, bucket, record.Object, spool, record)
				if err != nil {
					return fmt.Errorf("%s: %w", id, err)
				}
				if record.Generation == "" {
					return fmt.Errorf("missing original object generation")
				}
			}
			if err := encoder.Encode(record); err != nil {
				return err
			}
			total += n
			count++
			types[record.MIME]++
			return nil
		}
		if source.Zip {
			z, err := zip.OpenReader(source.Path)
			if err != nil {
				return err
			}
			err = func() error {
				defer z.Close()
				for _, member := range z.File {
					if member.FileInfo().IsDir() {
						continue
					}
					if !member.Mode().IsRegular() || member.UncompressedSize64 > uint64(maxObject) {
						return fmt.Errorf("unsupported or oversized ZIP member %s", member.Name)
					}
					if err := visit(member.Name, int64(member.UncompressedSize64), member.Open); err != nil {
						return err
					}
				}
				return nil
			}()
			if err != nil {
				return err
			}
		} else {
			root, err := os.OpenRoot(source.Path)
			if err != nil {
				return err
			}
			err = filepath.WalkDir(source.Path, func(p string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.IsDir() {
					return nil
				}
				info, err := entry.Info()
				if err != nil {
					return err
				}
				if !info.Mode().IsRegular() {
					return fmt.Errorf("non-regular original %s", p)
				}
				name, err := filepath.Rel(source.Path, p)
				if err != nil {
					return err
				}
				name = filepath.ToSlash(name)
				return visit(name, info.Size(), func() (io.ReadCloser, error) { return root.Open(name) })
			})
			root.Close()
			if err != nil {
				return err
			}
		}
	}
	if count == 0 {
		return fmt.Errorf("no originals")
	}
	if err := output.Sync(); err != nil {
		return err
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	receipt := map[string]any{"kind": "originals_catalog_v1", "records": count, "bytes": total, "mime_types": types, "sha256": digest, "uploaded": put != nil, "antfly_loaded": false}
	if put != nil {
		if _, err := output.Seek(0, io.SeekStart); err != nil {
			return err
		}
		raw, err := originalFileRecord(output, "application/x-ndjson")
		if err != nil {
			return err
		}
		key := "originals-catalogs/sha256/" + digest + ".ndjson"
		generation, err := put(ctx, bucket, key, output, raw)
		if err != nil {
			return err
		}
		receipt["manifest"] = "gs://" + bucket + "/" + key
		receipt["generation"] = generation
	}
	if err := output.Close(); err != nil {
		return err
	}
	if err := os.Rename(output.Name(), filepath.Join(state, "originals.ndjson")); err != nil {
		return err
	}
	if err := atomicCorpusJSON(filepath.Join(state, "originals.json"), receipt); err != nil {
		return err
	}
	return printCorpusJSON(receipt)
}

func originalFileRecord(source io.ReadSeeker, mime string) (originalRecord, error) {
	var record originalRecord
	sha, md, crc := sha256.New(), md5.New(), crc32.New(crc32.MakeTable(crc32.Castagnoli))
	n, err := io.Copy(io.MultiWriter(sha, md, crc), source)
	if err != nil {
		return record, err
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return record, err
	}
	return originalRecord{MIME: mime, Bytes: n, SHA256: hex.EncodeToString(sha.Sum(nil)), MD5: md.Sum(nil), CRC32C: crc.Sum32()}, nil
}

func originalGCSWriter(client *storage.Client) originalPut {
	return func(ctx context.Context, bucket, key string, source io.ReadSeeker, record originalRecord) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Hour)
		defer cancel()
		object := client.Bucket(bucket).Object(key)
		verify := func(attrs *storage.ObjectAttrs) (string, error) {
			if asserted := attrs.Metadata["sha256"]; asserted != "" && asserted != record.SHA256 {
				return "", fmt.Errorf("original object SHA-256 metadata mismatch")
			}
			if attrs.Size != record.Bytes || !bytes.Equal(attrs.MD5, record.MD5) || attrs.CRC32C != record.CRC32C {
				return "", fmt.Errorf("original object integrity mismatch")
			}
			return strconv.FormatInt(attrs.Generation, 10), nil
		}
		attrs, err := object.Attrs(ctx)
		if err == nil {
			return verify(attrs)
		}
		if !errors.Is(err, storage.ErrObjectNotExist) {
			return "", err
		}
		writer := object.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
		writer.ChunkSize = 8 << 20
		writer.ContentType = record.MIME
		writer.CRC32C = record.CRC32C
		writer.SendCRC32C = true
		writer.Metadata = map[string]string{"sha256": record.SHA256}
		n, copyErr := io.Copy(writer, io.LimitReader(source, record.Bytes+1))
		if copyErr != nil || n != record.Bytes {
			cancel()
			writer.Close()
			return "", fmt.Errorf("original upload interrupted or source changed")
		}
		if err := writer.Close(); err != nil {
			var apiErr *googleapi.Error
			if errors.As(err, &apiErr) && apiErr.Code == 412 {
				attrs, err := object.Attrs(ctx)
				if err != nil {
					return "", err
				}
				return verify(attrs)
			}
			return "", err
		}
		return verify(writer.Attrs())
	}
}
