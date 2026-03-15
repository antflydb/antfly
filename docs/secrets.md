# Secrets Management

Antfly supports secure secrets management through an encrypted keystore system, similar to Elasticsearch's keystore approach. This allows you to store sensitive credentials (API keys, AWS credentials, etc.) in an encrypted file instead of plain text configuration files or environment variables.

## Overview

Secrets can be stored in three ways (in priority order):

1. **Encrypted Keystore** - Local encrypted file (recommended for production)
2. **Environment Variables** - Standard env vars (good for development)
3. **Config Files** - Direct values in config (deprecated, not recommended)

## Quick Start

### 1. Create a Keystore

```bash
# Create encrypted keystore with password
antfly keystore create
# Enter keystore password: ****

# Create passwordless keystore (less secure but simpler)
echo "" | antfly keystore create --stdin
```

The keystore is created at `/etc/antfly/keystore` by default.

### 2. Add Secrets

```bash
# Add secrets interactively
antfly keystore add aws.access_key_id
antfly keystore add aws.secret_access_key
antfly keystore add openai.api_key

# Add from stdin (for automation)
echo "sk-..." | antfly keystore add openai.api_key --stdin

# Add file contents (for service account JSONs, etc)
antfly keystore add-file gcp.credentials /path/to/service-account.json
```

### 3. Reference in Configuration

Use `${secret:key.name}` syntax in your config files:

```yaml
# config.yaml
storage:
  s3:
    bucket: my-bucket
    region: us-east-1
    access_key_id: ${secret:aws.access_key_id}
    secret_access_key: ${secret:aws.secret_access_key}

indexes:
  - type: embedding_enricher
    config:
      provider: openai
      api_key: ${secret:openai.api_key}
```

### 4. Start Antfly

```bash
# With password-protected keystore
ANTFLY_KEYSTORE_PASSWORD="your-password" antfly swarm --config config.yaml

# With passwordless keystore
antfly swarm --config config.yaml

# Custom keystore path
antfly swarm --config config.yaml --keystore-path /path/to/keystore
```

## CLI Commands

### `keystore create`

Creates a new encrypted keystore file.

```bash
antfly keystore create [flags]

Flags:
  --force           Overwrite existing keystore
  -p, --path PATH   Keystore file path (default: /etc/antfly/keystore)
```

### `keystore add`

Adds or updates a secret in the keystore.

```bash
antfly keystore add <key> [flags]

Flags:
  --stdin           Read value from stdin instead of prompting
  -p, --path PATH   Keystore file path

Examples:
  antfly keystore add aws.access_key_id
  echo "value" | antfly keystore add openai.api_key --stdin
```

### `keystore add-file`

Adds a file's contents as a secret.

```bash
antfly keystore add-file <key> <file-path> [flags]

Example:
  antfly keystore add-file gcp.credentials /path/to/service-account.json
```

### `keystore list`

Lists all secret keys (not values) in the keystore.

```bash
antfly keystore list [flags]

Example output:
  Keystore contains 3 secret(s):
    - aws.access_key_id
    - aws.secret_access_key
    - openai.api_key
```

### `keystore show`

Shows a secret value (prints plain text to terminal).

```bash
antfly keystore show <key> [flags]

Example:
  antfly keystore show openai.api_key
```

### `keystore remove`

Removes a secret from the keystore permanently.

```bash
antfly keystore remove <key> [flags]

Example:
  antfly keystore remove aws.access_key_id
```

## Environment Variables

### Keystore Configuration

- `ANTFLY_KEYSTORE_PATH` - Path to keystore file (default: `/etc/antfly/keystore`)
- `ANTFLY_KEYSTORE_PASSWORD` - Keystore password (if password-protected)

### Fallback Credentials

If a secret is not found in the keystore, Antfly will fall back to these environment variables:

- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` - AWS credentials
- `OPENAI_API_KEY` - OpenAI API key
- `ANTHROPIC_API_KEY` - Anthropic Claude API key
- `GEMINI_API_KEY` - Google Gemini API key
- `GOOGLE_APPLICATION_CREDENTIALS` - Google Cloud service account path
- `VERTEXAI_PROJECT` / `VERTEXAI_LOCATION` - Google Vertex AI config

## Kubernetes Deployment

### Option 1: Pre-Created Keystore (Recommended)

Create the keystore locally, then mount it as a Kubernetes Secret:

```bash
# 1. Create keystore locally
antfly keystore create
antfly keystore add aws.access_key_id
antfly keystore add aws.secret_access_key
antfly keystore add openai.api_key

# 2. Create K8s Secret from keystore file
kubectl create secret generic antfly-keystore \
  --from-file=keystore=/etc/antfly/keystore
```

Mount in your deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: antfly-metadata
spec:
  template:
    spec:
      containers:
      - name: antfly
        image: antfly:latest
        env:
        - name: ANTFLY_KEYSTORE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: antfly-keystore-password
              key: password
        volumeMounts:
        - name: keystore
          mountPath: /etc/antfly
          readOnly: true
      volumes:
      - name: keystore
        secret:
          secretName: antfly-keystore
```

### Option 2: Init Container (Kubernetes-Native)

Build the keystore from individual Kubernetes Secrets using an init container:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: aws-credentials
type: Opaque
stringData:
  aws.access_key_id: AKIAIOSFODNN7EXAMPLE
  aws.secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
---
apiVersion: v1
kind: Secret
metadata:
  name: api-keys
type: Opaque
stringData:
  openai.api_key: sk-...
  gemini.api_key: AIza...
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: antfly-metadata
spec:
  template:
    spec:
      initContainers:
      - name: keystore-builder
        image: antfly:latest
        command: ["/bin/sh", "-c"]
        args:
          - |
            antfly keystore create --force
            for secret_file in /secrets/*; do
              while IFS='=' read -r key value; do
                echo "$value" | antfly keystore add "$key" --stdin
              done < "$secret_file"
            done
        volumeMounts:
        - name: keystore
          mountPath: /etc/antfly
        - name: secrets
          mountPath: /secrets
      containers:
      - name: antfly
        image: antfly:latest
        volumeMounts:
        - name: keystore
          mountPath: /etc/antfly
          readOnly: true
      volumes:
      - name: keystore
        emptyDir: {}
      - name: secrets
        projected:
          sources:
          - secret:
              name: aws-credentials
          - secret:
              name: api-keys
```

## Security Best Practices

1. **Use Password Protection**: Always create keystores with strong passwords in production
2. **Restrict File Permissions**: Keystore files are automatically created with `0600` permissions
3. **Rotate Secrets Regularly**: Update secrets periodically using `antfly keystore add`
4. **Never Commit Keystores**: Add keystore files to `.gitignore`
5. **Use K8s Secrets in Production**: Leverage your orchestration platform's secrets management
6. **Limit Secret Access**: Only give keystore passwords to processes that need them

## Common Secret Keys

Use these conventional key names for consistency:

**AWS:**
- `aws.access_key_id`
- `aws.secret_access_key`
- `aws.session_token`

**OpenAI:**
- `openai.api_key`
- `openai.base_url` (for custom endpoints)

**Anthropic:**
- `anthropic.api_key`

**Google Cloud:**
- `gcp.credentials` (service account JSON)
- `google.project`
- `google.location`
- `gemini.api_key`
- `vertexai.project`
- `vertexai.location`

## Troubleshooting

### Keystore not loading

```bash
# Check if keystore exists
ls -l /etc/antfly/keystore

# Verify password is correct
antfly keystore list
```

### Secret not resolving

```bash
# List all secrets in keystore
antfly keystore list

# Check secret value
antfly keystore show <key>

# Verify config syntax
grep -r "secret:" config.yaml
```

### Wrong password error

```
Error: failed to decrypt value: cipher: message authentication failed
```

Solution: Ensure `ANTFLY_KEYSTORE_PASSWORD` environment variable matches the password used during keystore creation.

## Migration from Environment Variables

If you're currently using environment variables, migrate to keystore:

```bash
# 1. Create keystore
antfly keystore create

# 2. Add secrets from environment
echo "$AWS_ACCESS_KEY_ID" | antfly keystore add aws.access_key_id --stdin
echo "$AWS_SECRET_ACCESS_KEY" | antfly keystore add aws.secret_access_key --stdin
echo "$OPENAI_API_KEY" | antfly keystore add openai.api_key --stdin

# 3. Update config to use ${secret:...} references
# 4. Remove sensitive env vars
# 5. Keep ANTFLY_KEYSTORE_PASSWORD only
```

## Technical Details

### Encryption

- **Algorithm**: AES-256-GCM (authenticated encryption)
- **Key Derivation**: PBKDF2 with SHA-256 (100,000 iterations)
- **Salt**: 32 bytes (randomly generated per keystore)
- **Nonce**: 12 bytes (randomly generated per secret)

### File Format

Keystore files are JSON with base64-encoded encrypted values:

```json
{
  "version": 1,
  "salt": "base64-encoded-salt",
  "entries": {
    "aws.access_key_id": {
      "key": "aws.access_key_id",
      "value": "base64-encoded-encrypted-value",
      "created_at": "2025-01-15T10:30:00Z",
      "updated_at": "2025-01-15T10:30:00Z"
    }
  }
}
```

### Resolution Order

When resolving `${secret:key.name}`:

1. Check keystore for exact key match
2. Check environment variable with dots replaced by underscores (e.g., `AWS_ACCESS_KEY_ID`)
3. Check common environment variable mappings (see table above)
4. Return error if not found

## Examples

### Full-Text Search with OpenAI Embeddings

```yaml
# config.yaml
tables:
  - name: documents
    indexes:
      - type: embedding_enricher
        config:
          provider: openai
          api_key: ${secret:openai.api_key}
          model: text-embedding-3-small
          field: content
```

```bash
# Setup
antfly keystore create
echo "sk-..." | antfly keystore add openai.api_key --stdin

# Run
ANTFLY_KEYSTORE_PASSWORD="..." antfly swarm --config config.yaml
```

### S3 Storage with AWS Credentials

```yaml
# config.yaml
storage:
  keyvalue: s3
  s3:
    endpoint: s3.amazonaws.com
    region: us-east-1
    bucket: antfly-data
    use_ssl: true
    access_key_id: ${secret:aws.access_key_id}
    secret_access_key: ${secret:aws.secret_access_key}
```

```bash
# Setup
antfly keystore create
antfly keystore add aws.access_key_id
antfly keystore add aws.secret_access_key

# Run
ANTFLY_KEYSTORE_PASSWORD="..." antfly swarm --config config.yaml
```

### Multi-Provider AI with Multiple Keys

```yaml
# config.yaml
tables:
  - name: articles
    indexes:
      - type: embedding_enricher
        config:
          provider: openai
          api_key: ${secret:openai.api_key}
      - type: summarize_enricher
        config:
          provider: anthropic
          api_key: ${secret:anthropic.api_key}
```

```bash
# Setup
antfly keystore create
echo "$OPENAI_API_KEY" | antfly keystore add openai.api_key --stdin
echo "$ANTHROPIC_API_KEY" | antfly keystore add anthropic.api_key --stdin

# Run
ANTFLY_KEYSTORE_PASSWORD="..." antfly swarm --config config.yaml
```
