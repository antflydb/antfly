# AntflyRestore API Reference

Complete API reference for the AntflyRestore custom resource.

## Overview

AntflyRestore defines on-demand restore operations for Antfly clusters. The operator creates a Kubernetes Job to execute the restore.

```yaml
apiVersion: antfly.io/v1
kind: AntflyRestore
metadata:
  name: restore-operation
  namespace: default
spec:
  # ... spec fields
status:
  # ... status fields (read-only)
```

## Spec

### Top-Level Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `clusterRef` | [ClusterReference](#clusterreference) | Yes | - | Target cluster for restore |
| `source` | [RestoreSource](#restoresource) | Yes | - | Backup source location |
| `tables` | []string | No | all | Specific tables to restore |
| `restoreMode` | [RestoreMode](#restoremode) | No | fail_if_exists | Behavior when tables exist |
| `restoreTimeout` | *duration | No | 2h | Maximum restore duration |
| `backoffLimit` | *int32 | No | 3 | Retry attempts before failure |

### ClusterReference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | - | Name of the target AntflyCluster |
| `namespace` | string | No | same as restore | Namespace of the AntflyCluster |

### RestoreSource

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `backupId` | string | Yes | Backup identifier to restore |
| `location` | string | Yes | Backup URL (s3:// or file://) |
| `credentialsSecret` | *[SecretReference](#secretreference) | No | Secret with storage credentials |

**Location format**:
- S3: `s3://bucket-name/path/to/backups`
- GCS (via S3 API): `s3://bucket-name/path` with endpoint in credentials
- Local: `file:///path/to/backups` (testing only)

### SecretReference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Name of the Secret |

**Expected Secret keys**:
| Key | Required | Description |
|-----|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | Access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | Secret key |
| `AWS_REGION` | No | AWS region |
| `AWS_ENDPOINT_URL` | No | Custom S3 endpoint |

### RestoreMode

| Mode | Description |
|------|-------------|
| `fail_if_exists` | Abort if any target table exists (default, safest) |
| `skip_if_exists` | Skip existing tables, restore others |
| `overwrite` | Drop and recreate existing tables (destructive) |

## Status

### Top-Level Status Fields

| Field | Type | Description |
|-------|------|-------------|
| `phase` | [RestorePhase](#restorephase) | Current phase |
| `startTime` | *Time | When restore started |
| `completionTime` | *Time | When restore finished |
| `tables` | [][TableRestoreStatus](#tablerestorestatus) | Per-table status |
| `message` | string | Status message |
| `conditions` | []Condition | Current conditions |
| `jobName` | string | Name of executing Job |

### RestorePhase

| Phase | Description |
|-------|-------------|
| `Pending` | Restore has not started |
| `Running` | Restore is in progress |
| `Completed` | Restore completed successfully |
| `Failed` | Restore failed |

### TableRestoreStatus

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Table name |
| `status` | string | "Pending", "Restoring", "Completed", "Failed", "Skipped" |
| `error` | string | Error message if failed |

### Conditions

| Type | Description |
|------|-------------|
| `JobReady` | Restore Job is created and ready |
| `ClusterReady` | Target cluster exists and is ready |

**Condition reasons**:
- `JobCreated` - Job created successfully
- `JobRunning` - Job is running
- `JobCompleted` - Job completed successfully
- `JobFailed` - Job failed
- `ClusterNotFound` - Target cluster not found
- `InvalidSource` - Backup source is invalid

## Validation Rules

- `source.location` must start with `s3://` or `file://`
- `source.backupId` must be non-empty
- `clusterRef.name` must reference an existing AntflyCluster
- `restoreMode` must be valid enum value
- `backoffLimit` must be >= 0

## Examples

### Basic Restore

```yaml
apiVersion: antfly.io/v1
kind: AntflyRestore
metadata:
  name: restore-latest
  namespace: production
spec:
  clusterRef:
    name: my-cluster
  source:
    backupId: "backup-20250115-020000"
    location: s3://my-bucket/antfly-backups
    credentialsSecret:
      name: backup-credentials
```

### Restore Specific Tables

```yaml
apiVersion: antfly.io/v1
kind: AntflyRestore
metadata:
  name: restore-users-table
  namespace: production
spec:
  clusterRef:
    name: my-cluster
  source:
    backupId: "backup-20250115-020000"
    location: s3://my-bucket/antfly-backups
    credentialsSecret:
      name: backup-credentials
  tables:
    - users
  restoreMode: overwrite  # Replace existing table
```

### Restore with Skip Mode

```yaml
apiVersion: antfly.io/v1
kind: AntflyRestore
metadata:
  name: restore-missing-tables
  namespace: production
spec:
  clusterRef:
    name: my-cluster
  source:
    backupId: "backup-20250115-020000"
    location: s3://my-bucket/antfly-backups
    credentialsSecret:
      name: backup-credentials
  restoreMode: skip_if_exists  # Only restore tables that don't exist
```

### Cross-Namespace Restore

Restore to a different namespace (e.g., staging from production backup):

```yaml
apiVersion: antfly.io/v1
kind: AntflyRestore
metadata:
  name: restore-to-staging
  namespace: staging
spec:
  clusterRef:
    name: staging-cluster
    namespace: staging
  source:
    backupId: "backup-20250115-020000"
    location: s3://my-bucket/production-backups
    credentialsSecret:
      name: backup-credentials
  restoreMode: overwrite
```

### GCS Restore

```yaml
apiVersion: antfly.io/v1
kind: AntflyRestore
metadata:
  name: restore-from-gcs
  namespace: production
spec:
  clusterRef:
    name: my-cluster
  source:
    backupId: "backup-20250115-020000"
    location: s3://my-gcs-bucket/antfly-backups
    credentialsSecret:
      name: gcs-hmac-credentials
---
apiVersion: v1
kind: Secret
metadata:
  name: gcs-hmac-credentials
  namespace: production
stringData:
  AWS_ACCESS_KEY_ID: "GOOGABC123DEF456"
  AWS_SECRET_ACCESS_KEY: "your-hmac-secret"
  AWS_ENDPOINT_URL: "https://storage.googleapis.com"
  AWS_REGION: "auto"
```

### Long-Running Restore

For large datasets:

```yaml
apiVersion: antfly.io/v1
kind: AntflyRestore
metadata:
  name: restore-large-dataset
  namespace: production
spec:
  clusterRef:
    name: my-cluster
  source:
    backupId: "backup-20250115-020000"
    location: s3://my-bucket/large-backups
    credentialsSecret:
      name: backup-credentials
  restoreTimeout: 8h  # Extended timeout
  backoffLimit: 5      # More retries
```

## Managing Restores

### List Restores

```bash
kubectl get antflyrestore -n production
```

### View Restore Status

```bash
kubectl get antflyrestore restore-latest -n production -o yaml
```

### Check Restore Progress

```bash
# View phase
kubectl get antflyrestore restore-latest -n production \
  -o jsonpath='{.status.phase}'

# View per-table status
kubectl get antflyrestore restore-latest -n production \
  -o jsonpath='{.status.tables}' | jq
```

### View Restore Job

```bash
# Get job name
kubectl get antflyrestore restore-latest -n production \
  -o jsonpath='{.status.jobName}'

# View job
kubectl get job <job-name> -n production

# View job logs
kubectl logs -l job-name=<job-name> -n production
```

### Cancel Restore

Delete the AntflyRestore resource to cancel:

```bash
kubectl delete antflyrestore restore-latest -n production
```

This will also delete the associated Job.

### Re-run Failed Restore

Delete and recreate the restore:

```bash
# Delete failed restore
kubectl delete antflyrestore restore-latest -n production

# Recreate
kubectl apply -f restore.yaml
```

## Troubleshooting

### Restore Stuck in Pending

```bash
# Check conditions
kubectl get antflyrestore restore-latest -o jsonpath='{.status.conditions}'

# Check if cluster exists
kubectl get antflycluster my-cluster -n production
```

### Restore Failed

```bash
# View error message
kubectl get antflyrestore restore-latest -o jsonpath='{.status.message}'

# Check job logs
kubectl logs -l job-name=<job-name> -n production

# Check per-table errors
kubectl get antflyrestore restore-latest -o jsonpath='{.status.tables}' | jq '.[] | select(.status=="Failed")'
```

### Table Already Exists

If using `fail_if_exists` mode and tables exist:

1. Use `skip_if_exists` to restore other tables
2. Use `overwrite` to replace existing tables (destructive)
3. Manually drop tables before restore

## See Also

- [Backup & Restore Guide](../operations/backup-restore.md): Operational guide
- [AntflyBackup API](antflybackup-api.md): Backup API reference
- [Secrets Management](../security/secrets-management.md): Credential management
