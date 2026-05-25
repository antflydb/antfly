package docsaf

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/bmatcuk/doublestar/v4"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/time/rate"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

// maxFileDownloadSize is the maximum size for a single file download (100MB).
const maxFileDownloadSize = 100 * 1024 * 1024

// Workspace MIME types and the format to export them as.
// Google Docs export as HTML to preserve heading structure for HTMLProcessor.
// Google Slides export as PDF to preserve slide layout for PDFProcessor.
var workspaceExportFormats = map[string]string{
	"application/vnd.google-apps.document":     "text/html",
	"application/vnd.google-apps.presentation": "application/pdf",
	"application/vnd.google-apps.form":         "text/plain",
}

// Workspace MIME types that should be skipped entirely.
var workspaceSkipTypes = map[string]bool{
	"application/vnd.google-apps.spreadsheet": true,
	"application/vnd.google-apps.drawing":     true,
	"application/vnd.google-apps.map":         true,
	"application/vnd.google-apps.site":        true,
	"application/vnd.google-apps.shortcut":    true,
	"application/vnd.google-apps.folder":      true,
}

// folderIDRegexp extracts the folder ID from various Google Drive URL formats.
var folderIDRegexp = regexp.MustCompile(`/folders/([a-zA-Z0-9_-]+)`)

// GoogleDriveSourceConfig holds configuration for a GoogleDriveSource.
type GoogleDriveSourceConfig struct {
	// CredentialsJSON is a service account key JSON string or file path.
	// Either CredentialsJSON or AccessToken must be provided.
	CredentialsJSON string

	// AccessToken is a pre-obtained OAuth2 access token.
	// Either CredentialsJSON or AccessToken must be provided.
	AccessToken string

	// FolderID is the Google Drive folder ID or full folder URL (required).
	// Supports URLs like https://drive.google.com/drive/folders/<ID> or
	// https://drive.google.com/drive/u/0/folders/<ID>.
	FolderID string

	// BaseURL is the base URL for generating document links (optional).
	// If empty, defaults to "https://drive.google.com".
	BaseURL string

	// IncludePatterns is a list of glob patterns to include.
	// If empty, all files are included (subject to exclude patterns).
	// Supports ** wildcards for recursive matching.
	IncludePatterns []string

	// ExcludePatterns is a list of glob patterns to exclude.
	// Supports ** wildcards for recursive matching.
	ExcludePatterns []string

	// Concurrency controls how many parallel downloads run at once.
	// Default: 5
	Concurrency int

	// IncludeSharedDrives enables listing files from shared/team drives.
	// Default: true (when nil).
	IncludeSharedDrives *bool
}

// GoogleDriveSource traverses files in a Google Drive folder and yields content items.
type GoogleDriveSource struct {
	config    GoogleDriveSourceConfig
	service   *drive.Service
	semaphore chan struct{}
	limiter   *rate.Limiter
}

// NewGoogleDriveSource creates a new Google Drive content source.
func NewGoogleDriveSource(ctx context.Context, config GoogleDriveSourceConfig) (*GoogleDriveSource, error) {
	if config.CredentialsJSON == "" && config.AccessToken == "" {
		return nil, fmt.Errorf("either CredentialsJSON or AccessToken is required")
	}
	if config.FolderID == "" {
		return nil, fmt.Errorf("FolderID is required")
	}

	// Parse folder ID from URL if needed
	config.FolderID = parseFolderID(config.FolderID)

	// Set defaults
	if config.Concurrency <= 0 {
		config.Concurrency = 5
	}

	// Build Drive service
	var opts []option.ClientOption
	if config.AccessToken != "" {
		ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: config.AccessToken})
		opts = append(opts, option.WithTokenSource(ts))
	} else {
		// Try as file path first, fall back to inline JSON
		credJSON := []byte(config.CredentialsJSON)
		if data, err := os.ReadFile(config.CredentialsJSON); err == nil {
			credJSON = data
		}
		creds, err := google.CredentialsFromJSON(ctx, credJSON, drive.DriveReadonlyScope) //nolint:staticcheck // no alternative available for service account credentials
		if err != nil {
			return nil, fmt.Errorf("parsing credentials: %w", err)
		}
		opts = append(opts, option.WithCredentials(creds))
	}

	service, err := drive.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Drive service: %w", err)
	}

	// Rate limiter: 8 qps with burst of 2 (Google limit is 10 qps)
	limiter := rate.NewLimiter(8.0, 2)

	return &GoogleDriveSource{
		config:    config,
		service:   service,
		semaphore: make(chan struct{}, config.Concurrency),
		limiter:   limiter,
	}, nil
}

// includeSharedDrives returns whether shared drives should be included.
// Defaults to true when IncludeSharedDrives is nil.
func (s *GoogleDriveSource) includeSharedDrives() bool {
	if s.config.IncludeSharedDrives == nil {
		return true
	}
	return *s.config.IncludeSharedDrives
}

// Type returns "google_drive" as the source type.
func (s *GoogleDriveSource) Type() string {
	return "google_drive"
}

// BaseURL returns the base URL for this source.
func (s *GoogleDriveSource) BaseURL() string {
	if s.config.BaseURL != "" {
		return s.config.BaseURL
	}
	return "https://drive.google.com"
}

// Traverse lists files in the Google Drive folder and yields content items
// in lexicographic path order (depth-first, sorted by name within each folder).
func (s *GoogleDriveSource) Traverse(ctx context.Context) (<-chan ContentItem, <-chan error) {
	items := make(chan ContentItem)
	errs := make(chan error, 1)

	go func() {
		defer close(items)
		defer close(errs)

		if err := s.traverseFolder(ctx, s.config.FolderID, "", items); err != nil {
			errs <- err
		}
	}()

	return items, errs
}

// downloadSlot holds the result channel for a single concurrent download,
// allowing ordered emission after concurrent downloads complete.
type downloadSlot struct {
	ch chan ContentItem
}

// traverseFolder recursively lists files in a folder and sends them to the items channel.
// Files are emitted in name-sorted order within each folder (depth-first traversal).
// Downloads happen concurrently for performance, but results are emitted in order.
func (s *GoogleDriveSource) traverseFolder(ctx context.Context, folderID, pathPrefix string, items chan<- ContentItem) error {
	var pageToken string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Rate limit list calls
		if err := s.limiter.Wait(ctx); err != nil {
			return err
		}

		query := fmt.Sprintf("'%s' in parents and trashed = false", folderID)
		call := s.service.Files.List().
			Q(query).
			OrderBy("name").
			Fields("nextPageToken, files(id, name, mimeType, size, modifiedTime, md5Checksum, webViewLink)").
			PageSize(1000)

		if s.includeSharedDrives() {
			call = call.SupportsAllDrives(true).IncludeItemsFromAllDrives(true)
		}

		if pageToken != "" {
			call = call.PageToken(pageToken)
		}

		fileList, err := call.Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("listing folder %s: %w", folderID, err)
		}

		// Separate folders from files so we can recurse folders in sorted order
		// before emitting files. This produces depth-first, name-sorted traversal.
		type fileEntry struct {
			file    *drive.File
			relPath string
		}
		var folders []fileEntry
		var files []fileEntry

		for _, file := range fileList.Files {
			relPath := file.Name
			if pathPrefix != "" {
				relPath = pathPrefix + "/" + file.Name
			}

			if file.MimeType == "application/vnd.google-apps.folder" {
				folders = append(folders, fileEntry{file: file, relPath: relPath})
				continue
			}

			// Skip non-exportable Workspace types
			if workspaceSkipTypes[file.MimeType] {
				continue
			}
			if strings.HasPrefix(file.MimeType, "application/vnd.google-apps.") {
				if _, ok := workspaceExportFormats[file.MimeType]; !ok {
					continue
				}
			}

			// Check exclude/include patterns
			if s.shouldExclude(relPath) {
				continue
			}
			if !s.shouldInclude(relPath) {
				continue
			}

			files = append(files, fileEntry{file: file, relPath: relPath})
		}

		// Sort folders by name (API returns sorted, but be defensive)
		sort.Slice(folders, func(i, j int) bool {
			return folders[i].file.Name < folders[j].file.Name
		})

		// Recurse into subfolders first (depth-first, sorted by name)
		for _, folder := range folders {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := s.traverseFolder(ctx, folder.file.Id, folder.relPath, items); err != nil {
				return err
			}
		}

		// Download files concurrently but emit in name-sorted order.
		// Each file gets an ordered result channel; downloads fill them
		// in parallel, but we read them sequentially to maintain order.
		slots := make([]downloadSlot, len(files))
		var wg sync.WaitGroup
		for i, fe := range files {
			slots[i] = downloadSlot{ch: make(chan ContentItem, 1)}

			wg.Add(1)
			s.semaphore <- struct{}{}
			go func(f *drive.File, relPath string, slot chan<- ContentItem) {
				defer wg.Done()
				defer func() { <-s.semaphore }()
				defer close(slot)

				content, contentType, err := s.downloadFile(ctx, f)
				if err != nil {
					log.Printf("Warning: Failed to download %s: %v", relPath, err)
					return // slot closed empty — skipped in emission
				}

				driveURL := f.WebViewLink
				if driveURL == "" {
					driveURL = fmt.Sprintf("https://drive.google.com/file/d/%s/view", f.Id)
				}

				select {
				case slot <- ContentItem{
					Path:        relPath,
					SourceURL:   driveURL,
					Content:     content,
					ContentType: contentType,
					Metadata: map[string]any{
						"source_type":   "google_drive",
						"drive_file_id": f.Id,
						"mime_type":     f.MimeType,
						"modified_time": f.ModifiedTime,
						"md5_checksum":  f.Md5Checksum,
						"size":          f.Size,
					},
				}:
				case <-ctx.Done():
				}
			}(fe.file, fe.relPath, slots[i].ch)
		}

		// Emit items in name-sorted order by reading slots sequentially.
		// Each slot blocks until that file's download completes.
		for _, slot := range slots {
			if item, ok := <-slot.ch; ok {
				select {
				case items <- item:
				case <-ctx.Done():
					wg.Wait()
					return ctx.Err()
				}
			}
		}

		// Wait for any remaining goroutines (e.g., failed downloads
		// whose slots were already closed empty).
		wg.Wait()

		pageToken = fileList.NextPageToken
		if pageToken == "" {
			break
		}
	}

	return nil
}

// downloadFile downloads a file from Google Drive, handling Google Workspace exports.
func (s *GoogleDriveSource) downloadFile(ctx context.Context, file *drive.File) ([]byte, string, error) {
	// Rate limit download calls
	if err := s.limiter.Wait(ctx); err != nil {
		return nil, "", err
	}

	// Check if this is an exportable Google Workspace file
	if exportMIME, ok := workspaceExportFormats[file.MimeType]; ok {
		resp, err := s.service.Files.Export(file.Id, exportMIME).Context(ctx).Download()
		if err != nil {
			return nil, "", fmt.Errorf("exporting file %s: %w", file.Name, err)
		}
		defer resp.Body.Close() //nolint:errcheck // best-effort close in deferred cleanup

		data, err := io.ReadAll(io.LimitReader(resp.Body, maxFileDownloadSize))
		if err != nil {
			return nil, "", fmt.Errorf("reading exported file %s: %w", file.Name, err)
		}
		return data, exportMIME, nil
	}

	// Regular file download
	resp, err := s.service.Files.Get(file.Id).Context(ctx).Download()
	if err != nil {
		return nil, "", fmt.Errorf("downloading file %s: %w", file.Name, err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort close in deferred cleanup

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxFileDownloadSize))
	if err != nil {
		return nil, "", fmt.Errorf("reading file %s: %w", file.Name, err)
	}

	contentType := DetectContentType(file.Name, data)
	return data, contentType, nil
}

// parseFolderID extracts a folder ID from a Google Drive URL or returns the input as-is.
func parseFolderID(input string) string {
	if matches := folderIDRegexp.FindStringSubmatch(input); len(matches) == 2 {
		return matches[1]
	}
	return input
}

// shouldExclude checks if a path matches any exclude pattern.
func (s *GoogleDriveSource) shouldExclude(path string) bool {
	for _, pattern := range s.config.ExcludePatterns {
		matched, err := doublestar.Match(pattern, path)
		if err != nil {
			log.Printf("Warning: Invalid exclude pattern %s: %v", pattern, err)
			continue
		}
		if matched {
			return true
		}
	}
	return false
}

// shouldInclude checks if a path matches include patterns.
// If no include patterns are configured, returns true.
func (s *GoogleDriveSource) shouldInclude(path string) bool {
	if len(s.config.IncludePatterns) == 0 {
		return true
	}
	for _, pattern := range s.config.IncludePatterns {
		matched, err := doublestar.Match(pattern, path)
		if err != nil {
			log.Printf("Warning: Invalid include pattern %s: %v", pattern, err)
			continue
		}
		if matched {
			return true
		}
	}
	return false
}
