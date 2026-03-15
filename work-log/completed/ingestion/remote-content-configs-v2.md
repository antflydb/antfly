# Implementation Plan: Remote Content Configuration System

## Overview

Consolidate S3 credentials and content security settings into a new `remote_content` configuration section. This separates remote content fetching credentials from backup storage (`storage.s3`) and enables multiple named credential sets with bucket-based auto-selection.

## Target Configuration

```yaml
remote_content:
  security:  # Global defaults (replaces top-level content_security)
    block_private_ips: true
    max_download_size_mb: 100
    download_timeout_seconds: 30
    max_image_dimension: 2048

  default_s3: "primary"

  s3:
    primary:
      endpoint: "s3.amazonaws.com"
      access_key_id: "${secret:aws.key}"
      secret_access_key: "${secret:aws.secret}"

    untrusted:
      endpoint: "s3.amazonaws.com"
      buckets: ["user-uploads-*", "public-*"]  # Glob patterns
      access_key_id: "${secret:uploads.key}"
      secret_access_key: "${secret:uploads.secret}"
      security:  # Per-credential override
        max_download_size_mb: 10

  http:  # Future extension
    internal-api:
      base_url: "https://docs.internal.com"
      headers:
        Authorization: "Bearer ${secret:token}"
```

## Template Usage

```handlebars
{{!-- Auto-selects credential by bucket pattern --}}
{{remotePDF url="s3://user-uploads-prod/doc.pdf"}}

{{!-- Explicit credential selection --}}
{{remotePDF url="s3://bucket/doc.pdf" credentials="primary"}}
```

## Credential Resolution Order

1. Explicit `credentials="name"` parameter in template
2. First credential where `buckets` glob pattern matches URL's bucket
3. `default_s3` credential
4. Legacy fallback: `storage.s3` credentials (backward compatibility)
5. Error if nothing matches

---

## Implementation Steps

### Step 1: Add OpenAPI Schemas

**File: `antfly-go/libaf/scraping/openapi.yaml`**

Add after `ContentSecurityConfig`:

```yaml
S3CredentialConfig:
  type: object
  description: S3 credential with optional bucket patterns and security overrides.
  allOf:
    - $ref: '../s3/openapi.yaml#/components/schemas/Credentials'
    - type: object
      properties:
        buckets:
          type: array
          description: Glob patterns for bucket names this credential handles.
          items:
            type: string
          example: ["user-uploads-*", "media-*"]
        security:
          $ref: '#/components/schemas/ContentSecurityConfig'
          description: Security overrides for this credential.

HTTPCredentialConfig:
  type: object
  description: HTTP credential for authenticated endpoints.
  properties:
    base_url:
      type: string
      format: uri
      description: Base URL prefix this credential applies to.
    headers:
      type: object
      description: HTTP headers to include. Supports keystore syntax.
      additionalProperties:
        type: string
    security:
      $ref: '#/components/schemas/ContentSecurityConfig'

RemoteContentConfig:
  type: object
  description: Configuration for remote content fetching (remotePDF, remoteMedia, remoteText).
  properties:
    security:
      $ref: '#/components/schemas/ContentSecurityConfig'
      description: Global security defaults for remote content operations.
    default_s3:
      type: string
      description: Default S3 credential name when no pattern matches.
    s3:
      type: object
      description: Named S3 credentials.
      additionalProperties:
        $ref: '#/components/schemas/S3CredentialConfig'
    http:
      type: object
      description: Named HTTP credentials.
      additionalProperties:
        $ref: '#/components/schemas/HTTPCredentialConfig'
```

### Step 2: Reference in Main Config

**File: `src/common/openapi.yaml`**

Add to `Config` properties (after `content_security`):

```yaml
remote_content:
  $ref: "../../antfly-go/libaf/scraping/openapi.yaml#/components/schemas/RemoteContentConfig"
```

### Step 3: Run Code Generation

```bash
make generate
```

### Step 4: Create Credential Resolution Logic

**New file: `lib/scraping/remote_content.go`**

```go
package scraping

import (
    "fmt"
    "net/url"
    "path"
    "strings"
    "sync"

    "github.com/antflydb/antfly-go/libaf/s3"
    libscraping "github.com/antflydb/antfly-go/libaf/scraping"
)

type RemoteContentManager struct {
    mu              sync.RWMutex
    globalSecurity  *ContentSecurityConfig
    s3Creds         map[string]*s3CredentialEntry
    httpCreds       map[string]*httpCredentialEntry
    defaultS3       string
    fallbackS3      *S3Credentials
    fallbackSecurity *ContentSecurityConfig
}

type s3CredentialEntry struct {
    creds    *s3.Credentials
    buckets  []string
    security *ContentSecurityConfig
}

type httpCredentialEntry struct {
    baseURL  string
    headers  map[string]string
    security *ContentSecurityConfig
}

var (
    manager   *RemoteContentManager
    managerMu sync.RWMutex
)

func InitRemoteContentConfig(cfg *libscraping.RemoteContentConfig, fallbackS3 *S3Credentials, fallbackSecurity *ContentSecurityConfig) {
    managerMu.Lock()
    defer managerMu.Unlock()

    m := &RemoteContentManager{
        s3Creds:          make(map[string]*s3CredentialEntry),
        httpCreds:        make(map[string]*httpCredentialEntry),
        fallbackS3:       fallbackS3,
        fallbackSecurity: fallbackSecurity,
    }

    if cfg == nil {
        manager = m
        return
    }

    if cfg.Security != nil {
        m.globalSecurity = cfg.Security
    } else {
        m.globalSecurity = fallbackSecurity
    }

    m.defaultS3 = cfg.DefaultS3

    for name, s3cfg := range cfg.S3 {
        m.s3Creds[name] = &s3CredentialEntry{
            creds: &s3.Credentials{
                Endpoint:        s3cfg.Endpoint,
                UseSsl:          s3cfg.UseSsl,
                AccessKeyId:     s3cfg.AccessKeyId,
                SecretAccessKey: s3cfg.SecretAccessKey,
                SessionToken:    s3cfg.SessionToken,
            },
            buckets:  s3cfg.Buckets,
            security: s3cfg.Security,
        }
    }

    for name, httpcfg := range cfg.Http {
        m.httpCreds[name] = &httpCredentialEntry{
            baseURL:  httpcfg.BaseUrl,
            headers:  httpcfg.Headers,
            security: httpcfg.Security,
        }
    }

    manager = m
}

func ResolveS3Credentials(s3URL, explicitCred string) (*S3Credentials, *ContentSecurityConfig, error) {
    managerMu.RLock()
    m := manager
    managerMu.RUnlock()

    if m == nil {
        return GetDefaultS3Credentials(), GetDefaultSecurityConfig(), nil
    }

    m.mu.RLock()
    defer m.mu.RUnlock()

    // 1. Explicit credential
    if explicitCred != "" {
        entry, ok := m.s3Creds[explicitCred]
        if !ok {
            return nil, nil, fmt.Errorf("unknown S3 credential: %s", explicitCred)
        }
        return toS3Creds(entry.creds), m.mergeSecurity(entry.security), nil
    }

    // 2. Extract bucket and match patterns
    bucket, err := extractBucket(s3URL)
    if err == nil && bucket != "" {
        for _, entry := range m.s3Creds {
            if matchesBucket(entry.buckets, bucket) {
                return toS3Creds(entry.creds), m.mergeSecurity(entry.security), nil
            }
        }
    }

    // 3. Default S3 credential
    if m.defaultS3 != "" {
        if entry, ok := m.s3Creds[m.defaultS3]; ok {
            return toS3Creds(entry.creds), m.mergeSecurity(entry.security), nil
        }
    }

    // 4. Legacy fallback
    if m.fallbackS3 != nil {
        return m.fallbackS3, m.fallbackSecurity, nil
    }

    return nil, nil, fmt.Errorf("no S3 credentials for: %s", s3URL)
}

func (m *RemoteContentManager) mergeSecurity(override *ContentSecurityConfig) *ContentSecurityConfig {
    base := m.globalSecurity
    if base == nil {
        base = m.fallbackSecurity
    }
    if base == nil {
        base = GetDefaultSecurityConfig()
    }
    if override == nil {
        return base
    }
    merged := *base
    if override.MaxDownloadSizeBytes > 0 {
        merged.MaxDownloadSizeBytes = override.MaxDownloadSizeBytes
    }
    if override.DownloadTimeoutSeconds > 0 {
        merged.DownloadTimeoutSeconds = override.DownloadTimeoutSeconds
    }
    if override.MaxImageDimension > 0 {
        merged.MaxImageDimension = override.MaxImageDimension
    }
    if len(override.AllowedPaths) > 0 {
        merged.AllowedPaths = override.AllowedPaths
    }
    return &merged
}

func extractBucket(s3URL string) (string, error) {
    u, err := url.Parse(s3URL)
    if err != nil {
        return "", err
    }
    parts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
    if len(parts) == 0 {
        return "", fmt.Errorf("no bucket in path")
    }
    return parts[0], nil
}

func matchesBucket(patterns []string, bucket string) bool {
    for _, p := range patterns {
        if matched, _ := path.Match(p, bucket); matched {
            return true
        }
    }
    return false
}

func toS3Creds(c *s3.Credentials) *S3Credentials {
    return &S3Credentials{
        Endpoint:        c.Endpoint,
        UseSsl:          c.UseSsl,
        AccessKeyId:     c.AccessKeyId,
        SecretAccessKey: c.SecretAccessKey,
        SessionToken:    c.SessionToken,
    }
}
```

### Step 5: Update Template Helpers

**File: `lib/template/remotehelpers.go`**

Modify each helper to accept `credentials` parameter:

```go
func RemotePDFFn(options *raymond.Options) raymond.SafeString {
    url := options.HashStr("url")
    output := options.HashStr("output")
    credentials := options.HashStr("credentials")  // NEW

    if url == "" {
        return raymond.SafeString("")
    }
    if output == "" {
        output = "text"
    }

    // Resolve credentials based on URL and explicit name
    s3Creds, securityConfig, err := scraping.ResolveS3Credentials(url, credentials)
    if err != nil {
        log.Printf("RemotePDFFn: %v", err)
        return raymond.SafeString("")
    }

    ctx, cancel := context.WithTimeout(context.Background(),
        time.Duration(securityConfig.DownloadTimeoutSeconds)*time.Second)
    defer cancel()

    result, err := scraping.DownloadAndProcessLink(
        ctx, url, securityConfig, s3Creds, &scraping.PDFProcessor{})
    // ... rest unchanged
}
```

Apply same pattern to `RemoteMediaFn` and `RemoteTextFn`.

### Step 6: Update Initialization

**File: `cmd/antfly/cmd/utils.go`**

In `parseConfig()`, after existing S3 setup:

```go
// Initialize remote content configuration
var fallbackS3 *scraping.S3Credentials
if config.Storage.S3.Endpoint != "" {
    creds := config.Storage.S3.GetS3Credentials()
    fallbackS3 = &scraping.S3Credentials{
        Endpoint:        creds.Endpoint,
        UseSsl:          creds.UseSsl,
        AccessKeyId:     creds.AccessKeyId,
        SecretAccessKey: creds.SecretAccessKey,
        SessionToken:    creds.SessionToken,
    }
}

var fallbackSecurity *scraping.ContentSecurityConfig
if config.ContentSecurity != (scraping.ContentSecurityConfig{}) {
    fallbackSecurity = &config.ContentSecurity
}

scraping.InitRemoteContentConfig(
    config.RemoteContent,
    fallbackS3,
    fallbackSecurity,
)
```

### Step 7: Add Tests

**New file: `lib/scraping/remote_content_test.go`**

```go
func TestResolveS3Credentials_ExplicitCredential(t *testing.T) {
    // Test explicit credential selection
}

func TestResolveS3Credentials_BucketPatternMatch(t *testing.T) {
    // Test glob pattern matching: "user-*" matches "user-uploads"
}

func TestResolveS3Credentials_DefaultFallback(t *testing.T) {
    // Test default_s3 fallback
}

func TestResolveS3Credentials_LegacyFallback(t *testing.T) {
    // Test backward compatibility with storage.s3
}

func TestSecurityConfigMerge(t *testing.T) {
    // Test per-credential security overrides
}
```

**Update: `src/common/config_test.go`**

Add test case for parsing `remote_content` config.

---

## Files to Modify

| File | Change |
|------|--------|
| `antfly-go/libaf/scraping/openapi.yaml` | Add S3CredentialConfig, HTTPCredentialConfig, RemoteContentConfig schemas |
| `src/common/openapi.yaml` | Add remote_content property to Config |
| `lib/scraping/remote_content.go` | NEW: Credential resolution logic |
| `lib/scraping/remote_content_test.go` | NEW: Unit tests |
| `lib/template/remotehelpers.go` | Add credentials parameter to helpers |
| `cmd/antfly/cmd/utils.go` | Initialize RemoteContentConfig at startup |
| `src/common/config_test.go` | Add test for remote_content parsing |

---

## Backward Compatibility

- If `remote_content` is not configured, system uses `storage.s3` and `content_security`
- Existing configs continue to work unchanged
- Template helpers without `credentials` parameter work as before

---

## Verification

1. Run `make generate` - should generate new types
2. Run `go build ./...` - should compile
3. Run `go test ./lib/scraping/...` - new tests pass
4. Run `go test ./src/common/...` - config parsing works
5. Manual test with config:
   ```yaml
   remote_content:
     default_s3: "test"
     s3:
       test:
         endpoint: "localhost:9000"
         access_key_id: "minioadmin"
         secret_access_key: "minioadmin"
         use_ssl: false
   ```
6. Test template: `{{remotePDF url="s3://localhost:9000/bucket/doc.pdf"}}`
