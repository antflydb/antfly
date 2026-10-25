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
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/sources"
	"golang.org/x/oauth2/google"
)

type cloudSourceJob struct {
	ID               string         `json:"id,omitempty"`
	Name             string         `json:"name"`
	ProviderAuthID   string         `json:"provider_auth_id"`
	TargetTable      string         `json:"target_table"`
	TableName        string         `json:"table_name,omitempty"`
	Mode             string         `json:"mode"`
	SourceConfig     map[string]any `json:"source_config"`
	RunPolicy        string         `json:"run_policy"`
	ScheduleInterval int            `json:"schedule_interval_seconds"`
}

func publishGrepProjection(ctx context.Context, snapshot, revision, bucket, connection, table, planFile string) error {
	client, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/devstorage.read_write")
	if err != nil {
		return err
	}
	client.Timeout = 15 * time.Minute
	return publishGrepProjectionWithClient(client, snapshot, revision, bucket, connection, table, planFile)
}
func publishGrepProjectionWithClient(client *http.Client, snapshot, revision, bucket, connection, table, planFile string) error {
	partitions, err := os.ReadDir(snapshot)
	if err != nil {
		return err
	}
	jobs := []cloudSourceJob{}
	for _, partition := range partitions {
		if !partition.IsDir() {
			return fmt.Errorf("unexpected snapshot entry")
		}
		prefix := "grep/v1/" + revision + "/" + partition.Name() + "/"
		pages, err := os.ReadDir(filepath.Join(snapshot, partition.Name()))
		if err != nil {
			return err
		}
		if len(pages) > 10000 {
			return fmt.Errorf("canonical page partition exceeds source job budget")
		}
		for _, file := range pages {
			page, err := readGrepPage(filepath.Join(snapshot, partition.Name()), file.Name())
			if err != nil {
				return err
			}
			digest := md5.Sum([]byte(page.Content))
			if _, err = putGCS(client, bucket, prefix+page.Item.Path, "text/plain; charset=utf-8", strings.NewReader(page.Content), int64(len(page.Content)), base64.StdEncoding.EncodeToString(digest[:])); err != nil {
				return err
			}
		}
		jobs = append(jobs, cloudSourceJob{Name: "grepstein-" + partition.Name(), ProviderAuthID: connection, TargetTable: table, Mode: "source", RunPolicy: "scheduled", ScheduleInterval: 3600, SourceConfig: map[string]any{"bucket": bucket, "prefix": prefix, "suffixes": []string{".txt"}, "max_objects": 10000, "permissions": sources.Permissions{Public: true}, "permission_ttl_seconds": 86400}})
	}
	// No jobs are created until the entire immutable projection is durable.
	// Colony owns their publication/revocation, source IDs, and catalog freshness.
	if err = atomicCorpusJSON(planFile, jobs); err != nil {
		return err
	}
	fmt.Printf("Cloud GCS source job plan: %s\nApply with grepstein cloud-jobs --plan %s --jobs-url <instance import-jobs API URL>\n", planFile, planFile)
	return nil
}

func cloudJobsCmd(args []string) error {
	f := flag.NewFlagSet("cloud-jobs", flag.ContinueOnError)
	planFile := f.String("plan", "./grepstein-corpus/cloud-jobs.json", "Reviewed Cloud job plan")
	endpoint := f.String("jobs-url", "", "Colony organization/instance import-jobs API URL")
	tokenEnv := f.String("token-env", "GREPSTEIN_CLOUD_TOKEN", "Environment variable holding a Colony user access token")
	if err := f.Parse(args); err != nil {
		return err
	}
	parsed, err := url.Parse(*endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || !strings.HasSuffix(parsed.Path, "/import-jobs") {
		return fmt.Errorf("jobs-url must be the HTTPS instance import-jobs endpoint")
	}
	token := os.Getenv(*tokenEnv)
	if token == "" {
		return fmt.Errorf("%s is required", *tokenEnv)
	}
	var jobs []cloudSourceJob
	if err = readCorpusJSON(*planFile, &jobs); err != nil {
		return err
	}
	if len(jobs) == 0 || len(jobs) > 1000 {
		return fmt.Errorf("invalid job plan")
	}
	unlock, err := corpusLock(filepath.Dir(*planFile))
	if err != nil {
		return err
	}
	defer unlock()
	client := &http.Client{Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return reconcileCloudJobs(client, strings.TrimRight(*endpoint, "/"), token, jobs)
}

func cloudJobRequest(client *http.Client, method, endpoint, token string, body any) (json.RawMessage, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, endpoint, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("Cloud job request HTTP %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (16<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 16<<20 || !json.Valid(raw) {
		return nil, fmt.Errorf("invalid Cloud job response")
	}
	return raw, nil
}

func reconcileCloudJobs(client *http.Client, endpoint, token string, jobs []cloudSourceJob) error {
	wanted := map[string]bool{}
	for _, job := range jobs {
		if !strings.HasPrefix(job.Name, "grepstein-") || wanted[job.Name] || job.Mode != "source" || job.ProviderAuthID == "" || job.TargetTable == "" || job.RunPolicy != "scheduled" || job.ScheduleInterval < 60 {
			return fmt.Errorf("invalid or duplicate Grepstein job")
		}
		wanted[job.Name] = true
	}
	raw, err := cloudJobRequest(client, "GET", endpoint, token, nil)
	if err != nil {
		return err
	}
	var existing struct {
		Data []cloudSourceJob `json:"data"`
	}
	if err = json.Unmarshal(raw, &existing); err != nil {
		return err
	}
	byName := map[string]cloudSourceJob{}
	for _, job := range existing.Data {
		if !wanted[job.Name] {
			continue
		}
		if _, ok := byName[job.Name]; ok {
			return fmt.Errorf("ambiguous existing job name: %s", job.Name)
		}
		byName[job.Name] = job
	}
	seen := map[string]bool{}
	for _, job := range jobs {
		if !strings.HasPrefix(job.Name, "grepstein-") || seen[job.Name] || job.Mode != "source" || job.ProviderAuthID == "" || job.TargetTable == "" {
			return fmt.Errorf("invalid or duplicate Grepstein job")
		}
		seen[job.Name] = true
		if current, ok := byName[job.Name]; ok {
			// Normalize JSON numbers and explicit permissions structs before comparing.
			a, _ := json.Marshal(current.SourceConfig)
			b, _ := json.Marshal(job.SourceConfig)
			var ac, bc any
			json.Unmarshal(a, &ac)
			json.Unmarshal(b, &bc)
			target := current.TargetTable
			if target == "" {
				target = current.TableName
			}
			if current.ID == "" || current.ProviderAuthID != job.ProviderAuthID || target != job.TargetTable || current.Mode != job.Mode || current.RunPolicy != job.RunPolicy || current.ScheduleInterval != job.ScheduleInterval || !reflect.DeepEqual(ac, bc) {
				return fmt.Errorf("existing job %s differs; review its lifecycle before replacing it", job.Name)
			}
			continue
		}
		raw, err = cloudJobRequest(client, "POST", endpoint, token, job)
		if err != nil {
			return err
		}
		var created cloudSourceJob
		if json.Unmarshal(raw, &created) != nil || created.ID == "" {
			return fmt.Errorf("Cloud returned no created job identity")
		}
		// Scheduled jobs are admitted and run by Colony. Never publish their catalog
		// directly or infer query readiness from this create response.
		fmt.Printf("Created %s (%s)\n", job.Name, created.ID)
	}
	return nil
}
