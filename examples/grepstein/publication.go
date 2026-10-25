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
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Publication is a complete, sealed source inventory. Derived stages cannot
// silently interpret a lost/truncated local manifest as the full release set.
func validatePublication(state string) error {
	var receipt struct {
		SHA256  string `json:"sha256"`
		Records int    `json:"records"`
	}
	if err := readCorpusJSON(filepath.Join(state, "publication.json"), &receipt); err != nil {
		return err
	}
	if len(receipt.SHA256) != 64 || receipt.Records < 1 {
		return fmt.Errorf("invalid publication receipt")
	}
	file, err := os.Open(filepath.Join(state, "published.ndjson"))
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	reader := bufio.NewReader(io.TeeReader(file, hash))
	count := 0
	seen := map[string]bool{}
	for {
		var record corpusRecord
		if _, err = readCorpusLine(reader, &record); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if record.ID == "" || record.Document == nil || seen[record.ID] {
			return fmt.Errorf("invalid or duplicate publication record")
		}
		seen[record.ID] = true
		count++
	}
	if count != receipt.Records || hex.EncodeToString(hash.Sum(nil)) != receipt.SHA256 {
		return fmt.Errorf("published source inventory changed; recover the sealed GCS manifest")
	}
	return nil
}
