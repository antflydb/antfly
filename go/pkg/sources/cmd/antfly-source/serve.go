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
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	sources "github.com/antflydb/antfly/go/pkg/sources"
)

func serve(args []string) error {
	flags := flag.NewFlagSet("antfly-source serve", flag.ContinueOnError)
	profile := flags.String("config", "", "initial source profile JSON (persisted runtime changes take precedence)")
	state := flags.String("state-dir", "", "private durable state directory")
	listen := flags.String("listen", "127.0.0.1:8082", "HTTP listen address (put non-loopback listeners behind TLS)")
	publicURL := flags.String("public-url", "", "stable source-service URL reachable by Antfly")
	adminEnv := flags.String("admin-token-env", "ANTFLY_SOURCE_ADMIN_TOKEN", "admin bearer token environment reference")
	mediaEnv := flags.String("media-key-env", "ANTFLY_SOURCE_MEDIA_KEY", "stable media signing key environment reference")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *profile == "" || *state == "" || *publicURL == "" {
		return fmt.Errorf("serve requires config, state-dir and public-url")
	}
	configuration, err := loadProfile(*profile)
	if err != nil {
		return err
	}
	service, err := sources.NewService(sources.ServiceOptions{Profile: configuration, StateDir: *state, PublicURL: *publicURL, AdminToken: os.Getenv(*adminEnv), MediaKey: []byte(os.Getenv(*mediaEnv))})
	if err != nil {
		return err
	}
	defer service.Close()
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: service, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 60 * time.Second, WriteTimeout: 120 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	done := make(chan struct{})
	shutdownDone := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err := server.Shutdown(shutdown)
			if err != nil {
				_ = server.Close()
			}
			shutdownDone <- err
		case <-done:
			shutdownDone <- nil
		}
	}()
	err = server.Serve(listener)
	close(done)
	shutdownErr := <-shutdownDone
	if errors.Is(err, http.ErrServerClosed) {
		return shutdownErr
	}
	return err
}
