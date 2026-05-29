// Copyright 2026 Antfly, Inc.
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

package metadata

import (
	"embed"
	"io/fs"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

//go:embed antfarm
var staticFS embed.FS

// spaFileSystem is a custom file system that serves 'index.html' for any path
// that doesn't correspond to an existing file. This is needed for Single-Page
// Applications where routing is handled on the client side.
type spaFileSystem struct {
	root http.FileSystem
}

func (fs spaFileSystem) Open(name string) (http.File, error) {
	f, err := fs.root.Open(name)
	if os.IsNotExist(err) {
		// If the file is not found, serve index.html instead.
		return fs.root.Open("index.html")
	}
	return f, err
}

func addAntfarmRoutes(mux *http.ServeMux) {
	// Get a filesystem rooted at the "static" directory.
	subFS, err := fs.Sub(staticFS, "antfarm")
	if err != nil {
		panic("could not find static directory in embedded files")
	}

	// Serve the static files for the frontend application.
	mux.Handle("/", http.FileServer(spaFileSystem{http.FS(subFS)}))
}

// addInferenceProxy adds a reverse proxy that forwards /ai/v1/* requests
// to the Antfly inference API.
func addInferenceProxy(mux *http.ServeMux, inferenceURL string) {
	target, err := url.Parse(inferenceURL)
	if err != nil {
		return
	}

	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			incomingPath := r.In.URL.Path
			targetPath := strings.TrimRight(target.Path, "/")
			if strings.HasSuffix(targetPath, "/ai/v1") {
				incomingPath = strings.TrimPrefix(incomingPath, "/ai/v1")
				if incomingPath == "" {
					incomingPath = "/"
				}
			}
			r.Out.URL.Path = path.Join(targetPath, incomingPath)
			r.Out.URL.RawPath = ""
			r.Out.Host = target.Host
			// Don't leak Antfly auth credentials to the inference service.
			r.Out.Header.Del("Authorization")
			r.Out.Header.Del("Cookie")
			r.Out.Header.Del("Accept-Encoding")
		},
		Transport: &http.Transport{
			ResponseHeaderTimeout: 10 * time.Second,
		},
	}

	mux.Handle("/ai/v1/", proxy)
}
