# AntflyBackup API Reference

Complete API reference for the AntflyBackup custom resource.

## Overview

AntflyBackup defines scheduled backup operations for Antfly clusters. The operator creates a Kubernetes CronJob to execute backups on schedule.

```yaml
apiVersion: antfly.io/v1
kind: AntflyBackup
metadata:
  name: daily-backup
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
| `clusterRef` | [ClusterReference](#clusterreference) | Yes | - | Reference to the cluster to backup |
| `schedule` | string | Yes | - | Cron schedule expression |
| `destination` | [BackupDestination](#backupdestination) | Yes | - | Backup storage location |
| `tables` | []string | No | all | Specific tables to backup |
| `suspend` | bool | No | false | Suspend scheduled backups |
| `backupTimeout` | *duration | No | 1h | Maximum backup duration |
| `successfulJobsHistoryLimit` | *int32 | No | 3 | Successful job history to retain |
| `failedJobsHistoryLimit` | *int32 | No | 1 | Failed job history to retain |

### ClusterReference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | - | Name of the AntflyCluster |
| `namespace` | string | No | same as backup | Namespace of the AntflyCluster |

### BackupDestination

| Field | Type | Required | Description |
|-------|------|----------|-------------|
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

### Schedule Syntax

Standard cron format: `minute hour day-of-month month day-of-week`

| Field | Values |
|-------|--------|
| Minute | 0-59 |
| Hour | 0-23 |
| Day of Month | 1-31 |
| Month | 1-12 |
| Day of Week | 0-6 (0=Sunday) |

**Examples**:
| Schedule | Cron Expression |
|----------|-----------------|
| Every hour | `0 * * * *` |
| Daily at 2am | `0 2 * * *` |
| Weekly Sunday 3am | `0 3 * * 0` |
| Monthly 1st at 4am | `0 4 1 * *` |
| Every 6 hours | `0 */6 * * *` |

## Status

### Top-Level Status Fields

| Field | Type | Description |
|-------|------|-------------|
| `phase` | [BackupPhase](#backupphase) | Current phase |
| `lastScheduledTime` | *Time | Last backup scheduled time |
| `lastSuccessfulBackup` | *[BackupRecord](#backuprecord) | Most recent successful backup |
| `recentBackups` | [][BackupRecord](#backuprecord) | Recent backup records |
| `conditions` | []Condition | Current conditions |
| `nextScheduledBackup` | *Time | Next planned backup time |
| `cronJobName` | string | Name of managing CronJob |

### BackupPhase

| Phase | Description |
|-------|-------------|
| `Active` | Backup schedule is active |
| `Suspended` | Backup schedule is suspended |
| `Failed` | Backup schedule has failed |

### BackupRecord

| Field | Type | Description |
|-------|------|-------------|
| `backupId` | string | Unique backup identifier |
| `startTime` | Time | When backup started |
| `completionTime` | *Time | When backup completed |
| `status` | string | "Running", "Completed", "Failed" |
| `tables` | []string | Tables that were backed up |
| `error` | string | Error message if failed |

### Conditions

| Type | Description |
|------|-------------|
| `ScheduleReady` | CronJob is created and ready |
| `ClusterReady` | Referenced cluster exists and is ready |

**Condition reasons**:
- `CronJobCreated` - CronJob created successfully
- `ClusterNotFound` - Referenced cluster not found
- `InvalidSchedule` - Cron schedule is invalid
- `InvalidDestination` - Backup destination is invalid

## Validation Rules

- `schedule` must be valid cron syntax
- `destination.location` must start with `s3://` or `file://`
- `clusterRef.name` must reference an existing AntflyCluster
- `successfulJobsHistoryLimit` must be >= 0
- `failedJobsHistoryLimit` must be >= 0

## Examples

### Basic Daily Backup

```yaml
apiVersion: antfly.io/v1
kind: AntflyBackup
metadata:
  name: daily-backup
  namespace: production
spec:
  clusterRef:
    name: my-cluster
  schedule: "0 2 * * *"
  destination:
    location: s3://my-bucket/antfly-backups
    credentialsSecret:
      name: backup-credentials
```

### Backup Specific Tables

```yaml
apiVersion: antfly.io/v1
kind: AntflyBackup
metadata:
  name: critical-tables-backup
  namespace: production
spec:
  clusterRef:
    name: my-cluster
  schedule: "0 * * * *"  # Hourly
  destination:
    location: s3://my-bucket/critical-backups
    credentialsSecret:
      name: backup-credentials
  tables:
    - users
    - transactions
    - audit_log
  backupTimeout: 30m
  successfulJobsHistoryLimit: 24  # Keep 24 hours of hourly backups
```

### GCS Backup with HMAC

```yaml
apiVersion: antfly.io/v1
kind: AntflyBackup
metadata:
  name: gcs-backup
  namespace: production
spec:
  clusterRef:
    name: my-cluster
  schedule: "0 2 * * *"
  destination:
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

### Cross-Namespace Backup

```yaml
apiVersion: antfly.io/v1
kind: AntflyBackup
metadata:
  name: prod-backup
  namespace: backup-jobs
spec:
  clusterRef:
    name: production-cluster
    namespace: production
  schedule: "0 */6 * * *"
  destination:
    location: s3://backup-bucket/production
    credentialsSecret:
      name: backup-credentials
```

### Suspended Backup

```yaml
apiVersion: antfly.io/v1
kind: AntflyBackup
metadata:
  name: suspended-backup
  namespace: production
spec:
  clusterRef:
    name: my-cluster
  schedule: "0 2 * * *"
  destination:
    location: s3://my-bucket/backups
    credentialsSecret:
      name: backup-credentials
  suspend: true  # Temporarily disabled
```

## Managing Backups

### List Backups

```bash
kubectl get antflybackup -n production
```

### View Backup Status

```bash
kubectl get antflybackup daily-backup -n production -o yaml
```

### Check Last Successful Backup

```bash
kubectl get antflybackup daily-backup -n production \
  -o jsonpath='{.status.lastSuccessfulBackup}'
```

### View CronJob

```bash
kubectl get cronjob -l antfly.io/backup=daily-backup -n production
```

### Suspend Backup

```bash
kubectl patch antflybackup daily-backup -n production \
  --type='merge' -p='{"spec":{"suspend":true}}'
```

### Resume Backup

```bash
kubectl patch antflybackup daily-backup -n production \
  --type='merge' -p='{"spec":{"suspend":false}}'
```

### Trigger Immediate Backup

```bash
# Create a job from the CronJob
kubectl create job --from=cronjob/daily-backup-cron manual-backup-$(date +%s) -n production
```

## See Also

- [Backup & Restore Guide](../operations/backup-restore.md): Operational guide
- [AntflyRestore API](antflyrestore-api.md): Restore API reference
- [Secrets Management](../security/secrets-management.md): Credential management
