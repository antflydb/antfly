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
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	sources "github.com/antflydb/antfly/go/pkg/sources"
)

// migrateLocal moves one local-provider source's content-addressed objects
// to an S3-compatible or GCS bucket and repoints its catalog there. Copying
// and verifying never touch serve's lock, so pass --server to repoint
// against a live serve instance over its own admin API, with no delivery
// downtime; without --server, it repoints in-process against --state-dir,
// exactly like serve would, which requires serve to be stopped first.
func migrateLocal(args []string) error {
	flags := flag.NewFlagSet("antfly-source migrate-local", flag.ContinueOnError)
	profile := flags.String("config", "", "initial source profile JSON (persisted runtime changes take precedence); ignored when --server is set")
	server := flags.String("server", "", "a live \"serve\" instance's base URL; repoints over its admin API instead of in-process (no delivery downtime)")
	state := flags.String("state-dir", "", "private durable state directory (the same one passed to serve); required without --server")
	publicURL := flags.String("public-url", "", "stable source-service URL reachable by Antfly (must match serve); required without --server")
	adminEnv := flags.String("admin-token-env", "ANTFLY_SOURCE_ADMIN_TOKEN", "admin bearer token environment reference")
	mediaEnv := flags.String("media-key-env", "ANTFLY_SOURCE_MEDIA_KEY", "stable media signing key environment reference; ignored when --server is set")
	sourceID := flags.String("source", "", "source UUID to migrate")
	fromConnection := flags.String("from-connection", "", "name of the source's existing provider \"local\" connection")
	fromPrefix := flags.String("from-prefix", "", "the local store's existing prefix (its LocalConfig \"prefix\")")
	toConnection := flags.String("to-connection", "", "name of an existing provider \"s3\" or \"gcs\" connection to copy objects to")
	toBucket := flags.String("to-bucket", "", "destination bucket")
	toPrefix := flags.String("to-prefix", "", "destination key prefix; objects land at <to-prefix>/objects/sha256/<digest>[.ext]")
	dryRun := flags.Bool("dry-run", false, "report counts and bytes without copying, writing or deleting anything")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *sourceID == "" || *fromConnection == "" || *toConnection == "" || *toBucket == "" {
		return fmt.Errorf("migrate-local requires source, from-connection, to-connection and to-bucket")
	}
	var configuration sources.Profile
	if *server == "" {
		if *profile == "" || *state == "" || *publicURL == "" {
			return fmt.Errorf("migrate-local without --server requires config, state-dir and public-url")
		}
		var err error
		configuration, err = loadProfile(*profile)
		if err != nil {
			return err
		}
	}
	fmt.Fprintln(os.Stderr, "migrate-local: "+sources.MigrateLocalRekeyNotice)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	result, err := sources.MigrateLocal(ctx, sources.MigrateLocalOptions{
		Profile:        configuration,
		Server:         *server,
		StateDir:       *state,
		PublicURL:      *publicURL,
		AdminToken:     os.Getenv(*adminEnv),
		MediaKey:       []byte(os.Getenv(*mediaEnv)),
		SourceID:       *sourceID,
		FromConnection: *fromConnection,
		FromPrefix:     *fromPrefix,
		ToConnection:   *toConnection,
		ToBucket:       *toBucket,
		ToPrefix:       *toPrefix,
		DryRun:         *dryRun,
		Progress:       printMigrateProgress,
	})
	printMigrateSummary(result)
	if err != nil {
		return err
	}
	if result.Failed > 0 {
		return fmt.Errorf("migrate-local: %d object(s) failed", result.Failed)
	}
	return nil
}

func printMigrateProgress(o sources.MigrateLocalObject) {
	if o.Error != "" {
		fmt.Fprintf(os.Stderr, "migrate-local: %-11s %s (%s)\n", o.Status, o.Path, o.Error)
		return
	}
	fmt.Fprintf(os.Stderr, "migrate-local: %-11s %s -> %s\n", o.Status, o.Path, o.DestinationKey)
}

func printMigrateSummary(r sources.MigrateLocalResult) {
	mode := "migrate"
	if r.DryRun {
		mode = "dry run"
	}
	fmt.Fprintf(os.Stderr, "migrate-local (%s): copied=%d (%d bytes) skipped=%d (%d bytes) failed=%d\n",
		mode, r.Copied, r.BytesCopied, r.Skipped, r.BytesSkipped, r.Failed)
	if r.DryRun {
		fmt.Fprintln(os.Stderr, "migrate-local: dry run; nothing was copied, written or deleted")
		return
	}
	if r.Failed > 0 {
		fmt.Fprintln(os.Stderr, "migrate-local: one or more objects failed verification; the catalog was not repointed")
		return
	}
	if r.CatalogRepointed {
		if r.RunID != "" {
			fmt.Fprintf(os.Stderr, "migrate-local: catalog repointed to connection %q bucket %q prefix %q (run %s, %d reference(s) updated)\n",
				r.ToConnection, r.Bucket, r.ToPrefix, r.RunID, r.ReferencesUpdated)
		} else {
			fmt.Fprintf(os.Stderr, "migrate-local: already pointed at connection %q bucket %q prefix %q; nothing to repoint\n", r.ToConnection, r.Bucket, r.ToPrefix)
		}
	}
	fmt.Fprintln(os.Stderr, "migrate-local: local copies were left in place. Once search results against the bucket look right, remove them yourself; this tool never deletes local content.")
}
