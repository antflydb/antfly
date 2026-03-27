package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/docsaf"
	"github.com/antflydb/antfly/pkg/libaf/s3"
	"github.com/antflydb/antfly/pkg/memoryaf"
	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
)

const defaultUserID = "docsaf-sync"

type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	return strings.Join(*s, ", ")
}

func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type config struct {
	antflyURL       string
	sourceType      string
	dir             string
	baseURL         string
	namespace       string
	project         string
	userID          string
	visibility      string
	memoryType      string
	debounce        time.Duration
	includePatterns []string
	excludePatterns []string
	gitURL          string
	gitRef          string
	gitSubPath      string
	gitUsername     string
	gitPassword     string
	gitSSHKeyPath   string
	s3Bucket        string
	s3Prefix        string
	s3Endpoint      string
	s3AccessKeyID   string
	s3SecretKey     string
	s3SessionToken  string
	s3UseSSL        bool
	driveFolderID   string
	driveCredsJSON  string
	driveToken      string
	driveShared     bool
	webStartURL     string
	webMaxDepth     int
	webMaxPages     int
}

type sectionRecord struct {
	Section       docsaf.DocumentSection
	SourceBackend string
	SourceID      string
	SourcePath    string
	SourceURL     string
	SourceVersion string
}

type desiredMemory struct {
	Identity string
	Args     memoryaf.StoreMemoryArgs
}

type syncStats struct {
	Created   int
	Updated   int
	Replaced  int
	Deleted   int
	Unchanged int
}

type antflyMemoryClient struct {
	api        *client.AntflyClient
	baseURL    string
	httpClient *http.Client
}

type sourceHandle struct {
	source  docsaf.ContentSource
	cleanup func()
}

func newAntflyMemoryClient(baseURL string) (*antflyMemoryClient, error) {
	httpClient := &http.Client{Timeout: 30 * time.Second}
	api, err := client.NewAntflyClient(baseURL, httpClient)
	if err != nil {
		return nil, err
	}
	return &antflyMemoryClient{
		api:        api,
		baseURL:    strings.TrimSuffix(baseURL, "/"),
		httpClient: httpClient,
	}, nil
}

func (c *antflyMemoryClient) CreateTable(ctx context.Context, tableName string, req *client.CreateTableRequest) error {
	return c.api.CreateTable(ctx, tableName, *req)
}

func (c *antflyMemoryClient) Batch(ctx context.Context, tableID string, req client.BatchRequest) (*client.BatchResult, error) {
	return c.api.Batch(ctx, tableID, req)
}

func (c *antflyMemoryClient) QueryWithBody(ctx context.Context, requestBody []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/query", bytes.NewReader(requestBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("query failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "sync":
		err = runSync(os.Args[2:])
	case "watch":
		err = runWatch(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s <sync|watch> [flags]\n", filepath.Base(os.Args[0]))
}

func runSync(args []string) error {
	cfg, err := parseConfig("sync", args)
	if err != nil {
		return err
	}

	return executeSync(context.Background(), cfg)
}

func runWatch(args []string) error {
	cfg, err := parseConfig("watch", args)
	if err != nil {
		return err
	}

	if err := executeSync(context.Background(), cfg); err != nil {
		return err
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	defer watcher.Close()

	if err := addRecursiveWatches(watcher, cfg.dir); err != nil {
		return err
	}

	log.Printf("watching %s", cfg.dir)

	signalCh := make(chan struct{}, 1)
	timer := time.NewTimer(cfg.debounce)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if ignoreWatchPath(event.Name) {
				continue
			}
			if event.Op&fsnotify.Create != 0 {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					if err := addRecursiveWatches(watcher, event.Name); err != nil {
						log.Printf("watch add failed for %s: %v", event.Name, err)
					}
				}
			}
			if !isRelevantEvent(event) {
				continue
			}
			log.Printf("change detected: %s %s", event.Op.String(), event.Name)
			resetTimer(timer, cfg.debounce)
		case <-timer.C:
			select {
			case signalCh <- struct{}{}:
			default:
			}
		case <-signalCh:
			if err := executeSync(context.Background(), cfg); err != nil {
				log.Printf("sync failed: %v", err)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			log.Printf("watcher error: %v", err)
		}
	}
}

func parseConfig(name string, args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	fs.StringVar(&cfg.antflyURL, "url", "http://localhost:8080/api/v1", "Antfly API URL")
	fs.StringVar(&cfg.sourceType, "source", "filesystem", "docsaf source backend: filesystem, git, s3, google-drive, or web")
	fs.StringVar(&cfg.dir, "dir", "", "Directory to process")
	fs.StringVar(&cfg.baseURL, "base-url", "", "Base URL for generated section links")
	fs.StringVar(&cfg.namespace, "namespace", "default", "memoryaf namespace")
	fs.StringVar(&cfg.project, "project", "", "memoryaf project identifier")
	fs.StringVar(&cfg.userID, "user-id", defaultUserID, "created_by value used for synced memories")
	fs.StringVar(&cfg.visibility, "visibility", memoryaf.VisibilityTeam, "memory visibility")
	fs.StringVar(&cfg.memoryType, "memory-type", memoryaf.MemoryTypeSemantic, "memory type to store for each section")
	fs.DurationVar(&cfg.debounce, "debounce", 750*time.Millisecond, "watch debounce duration")
	fs.StringVar(&cfg.gitURL, "git-url", "", "Git repository URL or owner/repo shorthand")
	fs.StringVar(&cfg.gitRef, "git-ref", "", "Git branch, tag, or commit")
	fs.StringVar(&cfg.gitSubPath, "git-subpath", "", "Subdirectory within the Git repository")
	fs.StringVar(&cfg.gitUsername, "git-username", "", "Git HTTPS username")
	fs.StringVar(&cfg.gitPassword, "git-password", "", "Git HTTPS password or token")
	fs.StringVar(&cfg.gitSSHKeyPath, "git-ssh-key", "", "Path to Git SSH private key")
	fs.StringVar(&cfg.s3Bucket, "s3-bucket", "", "S3 bucket name")
	fs.StringVar(&cfg.s3Prefix, "s3-prefix", "", "S3 key prefix")
	fs.StringVar(&cfg.s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint")
	fs.StringVar(&cfg.s3AccessKeyID, "s3-access-key-id", "", "S3 access key ID")
	fs.StringVar(&cfg.s3SecretKey, "s3-secret-access-key", "", "S3 secret access key")
	fs.StringVar(&cfg.s3SessionToken, "s3-session-token", "", "S3 session token")
	fs.BoolVar(&cfg.s3UseSSL, "s3-use-ssl", false, "Use SSL/TLS for S3 connections")
	fs.StringVar(&cfg.driveFolderID, "drive-folder-id", "", "Google Drive folder ID or folder URL")
	fs.StringVar(&cfg.driveCredsJSON, "drive-credentials-json", "", "Google Drive service account JSON string or file path")
	fs.StringVar(&cfg.driveToken, "drive-access-token", "", "Google Drive OAuth access token")
	fs.BoolVar(&cfg.driveShared, "drive-include-shared-drives", true, "Include shared drives for Google Drive sync")
	fs.StringVar(&cfg.webStartURL, "web-start-url", "", "Starting URL for web sync")
	fs.IntVar(&cfg.webMaxDepth, "web-max-depth", 2, "Maximum web crawl depth")
	fs.IntVar(&cfg.webMaxPages, "web-max-pages", 200, "Maximum web pages to crawl")

	var includePatterns stringSliceFlag
	var excludePatterns stringSliceFlag
	fs.Var(&includePatterns, "include", "Include glob pattern (repeatable)")
	fs.Var(&excludePatterns, "exclude", "Exclude glob pattern (repeatable)")

	if err := fs.Parse(args); err != nil {
		return cfg, err
	}

	cfg.sourceType = normalizeSourceType(cfg.sourceType)
	cfg.includePatterns = includePatterns
	cfg.excludePatterns = excludePatterns

	switch cfg.sourceType {
	case "filesystem":
		if cfg.dir == "" {
			return cfg, errors.New("--dir is required for --source=filesystem")
		}
		info, err := os.Stat(cfg.dir)
		if err != nil {
			return cfg, fmt.Errorf("stat %s: %w", cfg.dir, err)
		}
		if !info.IsDir() {
			return cfg, fmt.Errorf("%s is not a directory", cfg.dir)
		}
	case "git":
		if cfg.gitURL == "" {
			return cfg, errors.New("--git-url is required for --source=git")
		}
	case "s3":
		if cfg.s3Bucket == "" {
			return cfg, errors.New("--s3-bucket is required for --source=s3")
		}
	case "google_drive":
		if cfg.driveFolderID == "" {
			return cfg, errors.New("--drive-folder-id is required for --source=google-drive")
		}
		if cfg.driveCredsJSON == "" && cfg.driveToken == "" {
			return cfg, errors.New("either --drive-credentials-json or --drive-access-token is required for --source=google-drive")
		}
	case "web":
		if cfg.webStartURL == "" {
			return cfg, errors.New("--web-start-url is required for --source=web")
		}
	default:
		return cfg, fmt.Errorf("unsupported --source %q", cfg.sourceType)
	}

	if name == "watch" && cfg.sourceType != "filesystem" {
		return cfg, errors.New("watch only supports --source=filesystem")
	}
	if cfg.project == "" {
		cfg.project = defaultProject(cfg)
	}

	return cfg, nil
}

func executeSync(ctx context.Context, cfg config) error {
	mc, err := newAntflyMemoryClient(cfg.antflyURL)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}

	handler := memoryaf.NewHandler(mc, nil, zap.NewNop())
	defer handler.Close()

	handle, err := buildSource(ctx, cfg)
	if err != nil {
		return err
	}
	if handle.cleanup != nil {
		defer handle.cleanup()
	}

	records, err := collectSections(ctx, handle.source)
	if err != nil {
		return err
	}
	desired := buildDesiredMemories(records, cfg)

	stats, err := syncMemories(ctx, handler, cfg, desired)
	if err != nil {
		return err
	}

	log.Printf("sync complete: created=%d updated=%d replaced=%d deleted=%d unchanged=%d",
		stats.Created, stats.Updated, stats.Replaced, stats.Deleted, stats.Unchanged)
	return nil
}

func buildSource(ctx context.Context, cfg config) (sourceHandle, error) {
	switch cfg.sourceType {
	case "filesystem":
		return sourceHandle{
			source: docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
				BaseDir:         cfg.dir,
				BaseURL:         cfg.baseURL,
				IncludePatterns: cfg.includePatterns,
				ExcludePatterns: cfg.excludePatterns,
			}),
		}, nil
	case "git":
		var auth *docsaf.GitAuth
		if cfg.gitUsername != "" || cfg.gitPassword != "" || cfg.gitSSHKeyPath != "" {
			auth = &docsaf.GitAuth{
				Username:   cfg.gitUsername,
				Password:   cfg.gitPassword,
				SSHKeyPath: cfg.gitSSHKeyPath,
			}
		}
		source, err := docsaf.NewGitSource(docsaf.GitSourceConfig{
			URL:             cfg.gitURL,
			Ref:             cfg.gitRef,
			BaseURL:         cfg.baseURL,
			SubPath:         cfg.gitSubPath,
			IncludePatterns: cfg.includePatterns,
			ExcludePatterns: cfg.excludePatterns,
			Auth:            auth,
		})
		if err != nil {
			return sourceHandle{}, fmt.Errorf("create git source: %w", err)
		}
		return sourceHandle{
			source:  source,
			cleanup: source.Cleanup,
		}, nil
	case "s3":
		source, err := docsaf.NewS3Source(docsaf.S3SourceConfig{
			Credentials: s3.Credentials{
				AccessKeyId:     cfg.s3AccessKeyID,
				SecretAccessKey: cfg.s3SecretKey,
				SessionToken:    cfg.s3SessionToken,
				Endpoint:        cfg.s3Endpoint,
				UseSsl:          cfg.s3UseSSL,
			},
			Bucket:          cfg.s3Bucket,
			Prefix:          cfg.s3Prefix,
			BaseURL:         cfg.baseURL,
			IncludePatterns: cfg.includePatterns,
			ExcludePatterns: cfg.excludePatterns,
		})
		if err != nil {
			return sourceHandle{}, fmt.Errorf("create s3 source: %w", err)
		}
		return sourceHandle{source: source}, nil
	case "google_drive":
		source, err := docsaf.NewGoogleDriveSource(ctx, docsaf.GoogleDriveSourceConfig{
			CredentialsJSON:     cfg.driveCredsJSON,
			AccessToken:         cfg.driveToken,
			FolderID:            cfg.driveFolderID,
			BaseURL:             cfg.baseURL,
			IncludePatterns:     cfg.includePatterns,
			ExcludePatterns:     cfg.excludePatterns,
			IncludeSharedDrives: &cfg.driveShared,
		})
		if err != nil {
			return sourceHandle{}, fmt.Errorf("create google drive source: %w", err)
		}
		return sourceHandle{source: source}, nil
	case "web":
		source, err := docsaf.NewWebSource(docsaf.WebSourceConfig{
			StartURL:        cfg.webStartURL,
			BaseURL:         cfg.baseURL,
			IncludePatterns: cfg.includePatterns,
			ExcludePatterns: cfg.excludePatterns,
			MaxDepth:        cfg.webMaxDepth,
			MaxPages:        cfg.webMaxPages,
		})
		if err != nil {
			return sourceHandle{}, fmt.Errorf("create web source: %w", err)
		}
		return sourceHandle{source: source}, nil
	default:
		return sourceHandle{}, fmt.Errorf("unsupported source type %q", cfg.sourceType)
	}
}

func collectSections(ctx context.Context, source docsaf.ContentSource) ([]sectionRecord, error) {
	registry := docsaf.DefaultRegistry()

	items, errs := source.Traverse(ctx)
	var out []sectionRecord
	for items != nil || errs != nil {
		select {
		case item, ok := <-items:
			if !ok {
				items = nil
				continue
			}
			processor := registry.GetProcessor(item.ContentType, item.Path)
			if processor == nil {
				continue
			}
			sections, err := processor.Process(item.Path, item.SourceURL, source.BaseURL(), item.Content)
			if err != nil {
				return nil, fmt.Errorf("process %s: %w", item.Path, err)
			}
			for _, section := range sections {
				out = append(out, newSectionRecord(source.Type(), item, section))
			}
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			return nil, err
		}
	}

	slices.SortFunc(out, func(a, b sectionRecord) int {
		if cmp := strings.Compare(a.SourcePath, b.SourcePath); cmp != 0 {
			return cmp
		}
		return strings.Compare(
			sectionIdentity(a.SourceID, a.Section.SectionPath, classifyMemoryType(a.Section)),
			sectionIdentity(b.SourceID, b.Section.SectionPath, classifyMemoryType(b.Section)),
		)
	})
	return out, nil
}

func newSectionRecord(sourceType string, item docsaf.ContentItem, section docsaf.DocumentSection) sectionRecord {
	record := sectionRecord{
		Section:       section,
		SourceBackend: sourceType,
		SourcePath:    item.Path,
		SourceURL:     firstNonEmpty(section.URL, item.SourceURL),
	}

	switch sourceType {
	case "filesystem":
		record.SourceID = "filesystem:" + item.Path
		record.SourceVersion = metadataString(item.Metadata, "mod_time")
	case "git":
		repo := metadataString(item.Metadata, "repository")
		ref := metadataString(item.Metadata, "ref")
		record.SourceID = "git:" + repo + ":" + item.Path
		if ref != "" {
			record.SourceID = "git:" + repo + "@" + ref + ":" + item.Path
			record.SourceVersion = ref
		}
	case "s3":
		bucket := metadataString(item.Metadata, "bucket")
		key := metadataString(item.Metadata, "key")
		record.SourceID = fmt.Sprintf("s3://%s/%s", bucket, key)
		record.SourceVersion = metadataString(item.Metadata, "last_modified")
		record.SourceURL = firstNonEmpty(section.URL, item.SourceURL, record.SourceID)
	case "google_drive":
		fileID := metadataString(item.Metadata, "drive_file_id")
		if fileID != "" {
			record.SourceID = "gdrive:" + fileID
		}
		record.SourceVersion = firstNonEmpty(
			metadataString(item.Metadata, "md5_checksum"),
			metadataString(item.Metadata, "modified_time"),
		)
	case "web":
		record.SourceID = firstNonEmpty(item.SourceURL, section.URL)
	default:
		record.SourceID = sourceType + ":" + item.Path
	}

	if record.SourceID == "" {
		record.SourceID = sourceType + ":" + item.Path
	}
	if record.SourceURL == "" {
		record.SourceURL = firstNonEmpty(section.URL, item.SourceURL)
	}
	return record
}

func buildDesiredMemories(records []sectionRecord, cfg config) []desiredMemory {
	out := make([]desiredMemory, 0, len(records))
	for _, record := range records {
		memType := cfg.memoryType
		if memType == "auto" {
			memType = classifyMemoryType(record.Section)
		}

		args := memoryaf.StoreMemoryArgs{
			Content:       sectionContent(record.Section),
			MemoryType:    memType,
			Tags:          sectionTags(record.Section),
			Project:       cfg.project,
			Source:        "docsaf sync",
			Visibility:    cfg.visibility,
			SourceBackend: record.SourceBackend,
			SourceID:      record.SourceID,
			SourcePath:    record.SourcePath,
			SourceURL:     record.SourceURL,
			SourceVersion: record.SourceVersion,
			SectionPath:   append([]string(nil), record.Section.SectionPath...),
		}

		out = append(out, desiredMemory{
			Identity: sectionIdentity(args.SourceID, args.SectionPath, args.MemoryType),
			Args:     args,
		})
	}
	return out
}

func syncMemories(ctx context.Context, handler *memoryaf.Handler, cfg config, desired []desiredMemory) (syncStats, error) {
	stats := syncStats{}
	uctx := memoryaf.UserContext{
		UserID:    cfg.userID,
		Namespace: cfg.namespace,
		Role:      "member",
	}

	existing, err := listManagedMemories(ctx, handler, cfg, uctx)
	if err != nil {
		return stats, err
	}

	existingByIdentity := make(map[string]memoryaf.Memory, len(existing))
	for _, mem := range existing {
		existingByIdentity[sectionIdentity(mem.SourceID, mem.SectionPath, mem.MemoryType)] = mem
	}

	for _, want := range desired {
		have, ok := existingByIdentity[want.Identity]
		if !ok {
			if _, err := handler.StoreMemory(ctx, want.Args, uctx); err != nil {
				return stats, fmt.Errorf("store %s: %w", want.Identity, err)
			}
			stats.Created++
			continue
		}

		if needsReplace(have, want.Args) {
			if err := handler.DeleteMemory(ctx, have.ID, uctx); err != nil {
				return stats, fmt.Errorf("delete before replace %s: %w", want.Identity, err)
			}
			if _, err := handler.StoreMemory(ctx, want.Args, uctx); err != nil {
				return stats, fmt.Errorf("replace %s: %w", want.Identity, err)
			}
			stats.Replaced++
			delete(existingByIdentity, want.Identity)
			continue
		}

		if needsUpdate(have, want.Args) {
			_, err := handler.UpdateMemory(ctx, memoryaf.UpdateMemoryArgs{
				ID:         have.ID,
				Content:    want.Args.Content,
				MemoryType: want.Args.MemoryType,
				Tags:       append([]string(nil), want.Args.Tags...),
				Project:    want.Args.Project,
				Source:     want.Args.Source,
				Visibility: want.Args.Visibility,
			}, uctx)
			if err != nil {
				return stats, fmt.Errorf("update %s: %w", want.Identity, err)
			}
			stats.Updated++
		} else {
			stats.Unchanged++
		}
		delete(existingByIdentity, want.Identity)
	}

	for _, stale := range existingByIdentity {
		if err := handler.DeleteMemory(ctx, stale.ID, uctx); err != nil {
			return stats, fmt.Errorf("delete stale %s: %w", stale.ID, err)
		}
		stats.Deleted++
	}

	return stats, nil
}

func listManagedMemories(ctx context.Context, handler *memoryaf.Handler, cfg config, uctx memoryaf.UserContext) ([]memoryaf.Memory, error) {
	var out []memoryaf.Memory
	const pageSize = 200

	for offset := 0; ; offset += pageSize {
		page, err := handler.ListMemories(ctx, memoryaf.ListMemoriesArgs{
			Project:   cfg.project,
			CreatedBy: cfg.userID,
			Limit:     pageSize,
			Offset:    offset,
		}, uctx)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < pageSize {
			return out, nil
		}
	}
}

func needsReplace(existing memoryaf.Memory, args memoryaf.StoreMemoryArgs) bool {
	return existing.SourceBackend != args.SourceBackend ||
		existing.SourceID != args.SourceID ||
		existing.SourcePath != args.SourcePath ||
		existing.SourceURL != args.SourceURL ||
		existing.SourceVersion != args.SourceVersion ||
		!slices.Equal(existing.SectionPath, args.SectionPath)
}

func needsUpdate(existing memoryaf.Memory, args memoryaf.StoreMemoryArgs) bool {
	return existing.Content != args.Content ||
		existing.Project != args.Project ||
		existing.Source != args.Source ||
		existing.Visibility != args.Visibility ||
		!slices.Equal(existing.Tags, args.Tags)
}

func sectionIdentity(sourceID string, sectionPath []string, memoryType string) string {
	return sourceID + "|" + strings.Join(sectionPath, "\x1f") + "|" + memoryType
}

func sectionContent(section docsaf.DocumentSection) string {
	var sb strings.Builder
	sb.WriteString(section.Title)
	if len(section.SectionPath) > 0 {
		sb.WriteString("\nSection Path: ")
		sb.WriteString(strings.Join(section.SectionPath, " > "))
	}
	sb.WriteString("\n\n")
	sb.WriteString(strings.TrimSpace(section.Content))
	if len(section.Questions) > 0 {
		sb.WriteString("\n\nQuestions:\n")
		for _, q := range section.Questions {
			sb.WriteString("- ")
			sb.WriteString(q)
			sb.WriteString("\n")
		}
	}
	return strings.TrimSpace(sb.String())
}

func sectionTags(section docsaf.DocumentSection) []string {
	tags := []string{"docsaf", section.Type}
	if len(section.SectionPath) > 0 {
		tags = append(tags, slugify(section.SectionPath[0]))
	}
	return tags
}

func classifyMemoryType(section docsaf.DocumentSection) string {
	lowerPath := strings.ToLower(section.FilePath)
	lowerTitle := strings.ToLower(section.Title)
	if strings.Contains(lowerPath, "runbook") || strings.Contains(lowerPath, "playbook") || strings.HasPrefix(lowerTitle, "how to") {
		return memoryaf.MemoryTypeProcedural
	}
	return memoryaf.MemoryTypeSemantic
}

func metadataString(metadata map[string]any, key string) string {
	if metadata == nil {
		return ""
	}
	v, ok := metadata[key]
	if !ok || v == nil {
		return ""
	}
	switch vv := v.(type) {
	case string:
		return vv
	case time.Time:
		return vv.UTC().Format(time.RFC3339)
	default:
		return fmt.Sprint(vv)
	}
}

func normalizeSourceType(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "_", "-")
	switch normalized {
	case "filesystem", "git", "s3", "web":
		return normalized
	case "google-drive":
		return "google_drive"
	default:
		return normalized
	}
}

func defaultProject(cfg config) string {
	switch cfg.sourceType {
	case "filesystem":
		return filepath.Base(filepath.Clean(cfg.dir))
	case "git":
		return defaultProjectToken(cfg.gitURL)
	case "s3":
		return defaultProjectToken(cfg.s3Bucket + "-" + strings.Trim(cfg.s3Prefix, "/"))
	case "google_drive":
		return defaultProjectToken("drive-" + cfg.driveFolderID)
	case "web":
		return defaultProjectToken(cfg.webStartURL)
	default:
		return "docsaf-memory"
	}
}

func defaultProjectToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "docsaf-memory"
	}
	value = strings.TrimSuffix(value, ".git")
	value = filepath.Base(value)
	if value == "." || value == "/" || value == "" {
		value = "docsaf-memory"
	}
	return slugify(value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func slugify(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, " ", "-")
	value = strings.ReplaceAll(value, "/", "-")
	return value
}

func addRecursiveWatches(watcher *fsnotify.Watcher, root string) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if ignoreWatchPath(path) {
			if path == root {
				return nil
			}
			return filepath.SkipDir
		}
		if err := watcher.Add(path); err != nil {
			return fmt.Errorf("watch %s: %w", path, err)
		}
		return nil
	})
}

func ignoreWatchPath(path string) bool {
	clean := filepath.Clean(path)
	if filepath.Base(clean) == ".DS_Store" {
		return true
	}
	parts := strings.Split(clean, string(filepath.Separator))
	return slices.Contains(parts, ".git")
}

func isRelevantEvent(event fsnotify.Event) bool {
	return event.Has(fsnotify.Create) ||
		event.Has(fsnotify.Write) ||
		event.Has(fsnotify.Remove) ||
		event.Has(fsnotify.Rename)
}

func resetTimer(timer *time.Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}
