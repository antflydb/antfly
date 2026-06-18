package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
)

const googleDriveAuthTimeout = 5 * time.Minute

var googleDriveADCTokenSource = loadGoogleDriveADCTokenSource

type googleDriveTokenCache struct {
	ClientID     string          `json:"client_id"`
	ClientSecret string          `json:"client_secret"`
	Endpoint     oauth2.Endpoint `json:"endpoint"`
	Scopes       []string        `json:"scopes"`
	Token        *oauth2.Token   `json:"token"`
}

func defaultGoogleDriveTokenFile() string {
	if dir, err := os.UserConfigDir(); err == nil && dir != "" {
		return filepath.Join(dir, "antfly", "docsaf", "google-drive-token.json")
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".config", "antfly", "docsaf", "google-drive-token.json")
	}
	return "google-drive-token.json"
}

func authCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: docsaf auth google-drive [flags]")
	}
	switch args[0] {
	case "google-drive":
		return authGoogleDriveCmd(args[1:])
	default:
		return fmt.Errorf("unknown auth provider %q; expected google-drive", args[0])
	}
}

func authGoogleDriveCmd(args []string) error {
	fs := flag.NewFlagSet("auth google-drive", flag.ExitOnError)
	clientSecretPath := fs.String("client-secret", "", "Google OAuth client secret JSON file for an installed/desktop app")
	tokenFile := fs.String("token-file", defaultGoogleDriveTokenFile(), "Path to write the Google Drive OAuth token cache")
	port := fs.Int("port", 0, "Local callback port; 0 chooses a free port")
	timeout := fs.Duration("timeout", googleDriveAuthTimeout, "Time to wait for browser authorization")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}
	if strings.TrimSpace(*clientSecretPath) == "" {
		return fmt.Errorf("--client-secret is required")
	}

	clientSecretJSON, err := os.ReadFile(*clientSecretPath)
	if err != nil {
		return fmt.Errorf("read client secret: %w", err)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		return fmt.Errorf("listen for OAuth callback: %w", err)
	}
	defer listener.Close() //nolint:errcheck // best-effort close during command cleanup

	redirectURL := "http://" + listener.Addr().String() + "/callback"
	config, err := google.ConfigFromJSON(clientSecretJSON, drive.DriveReadonlyScope)
	if err != nil {
		return fmt.Errorf("parse OAuth client secret: %w", err)
	}
	config.RedirectURL = redirectURL

	state, err := randomState()
	if err != nil {
		return err
	}

	codeCh := make(chan string, 1)
	errCh := make(chan error, 1)
	server := &http.Server{
		Handler: googleDriveCallbackHandler(state, codeCh, errCh),
	}
	defer server.Shutdown(context.Background()) //nolint:errcheck // command is exiting
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	authURL := config.AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.ApprovalForce)
	fmt.Printf("Open this URL in your browser to authorize docsaf Google Drive access:\n\n%s\n\n", authURL)
	fmt.Printf("Waiting for Google OAuth callback on %s...\n", redirectURL)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var code string
	select {
	case code = <-codeCh:
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for Google OAuth callback after %s", timeout.String())
	}

	token, err := config.Exchange(ctx, code)
	if err != nil {
		return fmt.Errorf("exchange OAuth code: %w", err)
	}
	if token.RefreshToken == "" {
		fmt.Printf("Warning: Google did not return a refresh token. Revoke the app grant or use a new client if future syncs fail after the access token expires.\n")
	}

	cache := googleDriveTokenCache{
		ClientID:     config.ClientID,
		ClientSecret: config.ClientSecret,
		Endpoint:     config.Endpoint,
		Scopes:       config.Scopes,
		Token:        token,
	}
	if err := writeGoogleDriveTokenCache(*tokenFile, cache); err != nil {
		return err
	}

	fmt.Printf("Google Drive authorization saved to %s\n", *tokenFile)
	return nil
}

func googleDriveCallbackHandler(expectedState string, codeCh chan<- string, errCh chan<- error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/callback" {
			http.NotFound(w, r)
			return
		}
		if got := r.URL.Query().Get("state"); got != expectedState {
			http.Error(w, "invalid OAuth state", http.StatusBadRequest)
			select {
			case errCh <- fmt.Errorf("invalid OAuth state"):
			default:
			}
			return
		}
		if errText := r.URL.Query().Get("error"); errText != "" {
			http.Error(w, "authorization failed", http.StatusBadRequest)
			select {
			case errCh <- fmt.Errorf("Google authorization failed: %s", errText):
			default:
			}
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			http.Error(w, "missing OAuth code", http.StatusBadRequest)
			select {
			case errCh <- fmt.Errorf("missing OAuth code"):
			default:
			}
			return
		}
		fmt.Fprintln(w, "docsaf Google Drive authorization complete. You can close this window.")
		select {
		case codeCh <- code:
		default:
		}
	})
}

func randomState() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate OAuth state: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func writeGoogleDriveTokenCache(path string, cache googleDriveTokenCache) error {
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal token cache: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("create token cache directory: %w", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("write token cache: %w", err)
	}
	return nil
}

func loadGoogleDriveTokenSource(ctx context.Context, path string) (oauth2.TokenSource, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("Google Drive token file path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read Google Drive token file %q: %w", path, err)
	}
	var cache googleDriveTokenCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return nil, fmt.Errorf("parse Google Drive token file %q: %w", path, err)
	}
	if cache.ClientID == "" || cache.ClientSecret == "" || cache.Token == nil {
		return nil, fmt.Errorf("Google Drive token file %q is missing OAuth client or token data", path)
	}
	endpoint := cache.Endpoint
	if endpoint.AuthURL == "" || endpoint.TokenURL == "" {
		endpoint = google.Endpoint
	}
	scopes := cache.Scopes
	if len(scopes) == 0 {
		scopes = []string{drive.DriveReadonlyScope}
	}
	config := &oauth2.Config{
		ClientID:     cache.ClientID,
		ClientSecret: cache.ClientSecret,
		Endpoint:     endpoint,
		Scopes:       scopes,
	}
	return config.TokenSource(ctx, cache.Token), nil
}

func resolveGoogleDriveTokenSource(ctx context.Context, tokenFile string) (oauth2.TokenSource, error) {
	if strings.TrimSpace(tokenFile) != "" {
		if tokenSource, err := loadGoogleDriveTokenSource(ctx, tokenFile); err == nil {
			return tokenSource, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(os.Stderr, "Warning: ignoring unusable Google Drive token file %q: %v\n", tokenFile, err)
		}
	}
	if tokenSource, err := googleDriveADCTokenSource(ctx); err == nil {
		return tokenSource, nil
	}
	return nil, missingGoogleDriveAuthError()
}

func loadGoogleDriveADCTokenSource(ctx context.Context) (oauth2.TokenSource, error) {
	credentials, err := google.FindDefaultCredentials(ctx, drive.DriveReadonlyScope)
	if err != nil {
		return nil, err
	}
	if credentials.TokenSource == nil {
		return nil, fmt.Errorf("application default credentials did not include a token source")
	}
	return credentials.TokenSource, nil
}

func missingGoogleDriveAuthError() error {
	return fmt.Errorf("no Google Drive auth found\n\nRun one of:\n  docsaf auth google-drive --client-secret ./client_secret.json\n  gcloud auth application-default login --scopes=%s\n\nThen retry with:\n  docsaf sync --source google-drive --drive-folder <folder-url>", drive.DriveReadonlyScope)
}
