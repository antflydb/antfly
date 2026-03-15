# Backup and Restore

This guide covers backing up and restoring Antfly database clusters using the AntflyBackup and AntflyRestore CRDs.

## Overview

The Antfly Operator provides two CRDs for data protection:

- **AntflyBackup**: Scheduled backups using Kubernetes CronJobs
- **AntflyRestore**: On-demand restore operations using Kubernetes Jobs

Backups can be stored on:
- Amazon S3
- Google Cloud Storage (S3-compatible API)
- Local filesystem (for testing)

## Quick Start

### Create a Backup Schedule

```yaml
apiVersion: antfly.io/v1
kind: AntflyBackup
metadata:
  name: daily-backup
  namespace: default
spec:
  clusterRef:
    name: my-cluster
  schedule: "0 2 * * *"  # Daily at 2am UTC
  destination:
    location: s3://my-bucket/antfly-backups
    credentialsSecret:
      name: backup-credentials
```

### Create Credentials Secret

```bash
kubectl create secret generic backup-credentials \
  --from-literal=AWS_ACCESS_KEY_ID='your-access-key' \
  --from-literal=AWS_SECRET_ACCESS_KEY='your-secret-key' \
  --from-literal=AWS_REGION='us-east-2'
```

### Restore from Backup

```yaml
apiVersion: antfly.io/v1
kind: AntflyRestore
metadata:
  name: restore-from-backup
  namespace: default
spec:
  clusterRef:
    name: my-cluster
  source:
    backupId: "backup-20250101-020000"
    location: s3://my-bucket/antfly-backups
    credentialsSecret:
      name: backup-credentials
```

## AntflyBackup Configuration

### Full Specification

```yaml
apiVersion: antfly.io/v1
kind: AntflyBackup
metadata:
  name: my-backup
  namespace: default
spec:
  # Reference to the cluster to backup
  clusterRef:
    name: my-cluster
    namespace: default  # Optional, defaults to same namespace

  # Cron schedule (required)
  schedule: "0 2 * * *"

  # Backup destination (required)
  destination:
    location: s3://bucket/path  # or file:///path
    credentialsSecret:
      name: backup-credentials  # Optional for IRSA/Workload Identity

  # Tables to backup (optional, defaults to all)
  tables:
    - users
    - orders

  # Suspend backups (optional)
  suspend: false

  # Backup timeout (optional, default: 1h)
  backupTimeout: 2h

  # History limits (optional)
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 1
```

### Schedule Syntax

Use standard cron syntax:

| Field | Allowed Values |
|-------|----------------|
| Minute | 0-59 |
| Hour | 0-23 |
| Day of Month | 1-31 |
| Month | 1-12 |
| Day of Week | 0-6 (Sunday=0) |

Common schedules:

| Schedule | Cron Expression |
|----------|-----------------|
| Every hour | `0 * * * *` |
| Daily at 2am | `0 2 * * *` |
| Weekly on Sunday | `0 2 * * 0` |
| Monthly on 1st | `0 2 1 * *` |

### Backup Status

Check backup status:

```bash
# List backups
kubectl get antflybackup

# NAME           CLUSTER      SCHEDULE    PHASE    LAST SUCCESS         AGE
# daily-backup   my-cluster   0 2 * * *   Active   2025-01-15T02:00:00Z 5d

# Detailed status
kubectl get antflybackup daily-backup -o yaml
```

Status fields:

```yaml
status:
  phase: Active  # Active, Suspended, or Failed
  lastScheduledTime: "2025-01-15T02:00:00Z"
  lastSuccessfulBackup:
    backupId: "backup-20250115-020000"
    startTime: "2025-01-15T02:00:00Z"
    completionTime: "2025-01-15T02:05:30Z"
    status: "Completed"
  nextScheduledBackup: "2025-01-16T02:00:00Z"
  cronJobName: "daily-backup-cron"
  conditions:
    - type: ScheduleReady
      status: "True"
    - type: ClusterReady
      status: "True"
```

## AntflyRestore Configuration

### Full Specification

```yaml
apiVersion: antfly.io/v1
kind: AntflyRestore
metadata:
  name: my-restore
  namespace: default
spec:
  # Target cluster
  clusterRef:
    name: my-cluster
    namespace: default

  # Backup source (required)
  source:
    backupId: "backup-20250115-020000"
    location: s3://bucket/path
    credentialsSecret:
      name: backup-credentials

  # Tables to restore (optional, defaults to all from backup)
  tables:
    - users
    - orders

  # Behavior when tables exist (optional)
  restoreMode: fail_if_exists  # fail_if_exists, skip_if_exists, or overwrite

  # Restore timeout (optional, default: 2h)
  restoreTimeout: 4h

  # Retry limit (optional, default: 3)
  backoffLimit: 3
```

### Restore Modes

| Mode | Behavior |
|------|----------|
| `fail_if_exists` | Abort if any target table exists (default) |
| `skip_if_exists` | Skip existing tables, restore others |
| `overwrite` | Drop and recreate existing tables |

### Restore Status

```bash
# List restores
kubectl get antflyrestore

# NAME                 CLUSTER      BACKUP ID               PHASE       AGE
# restore-from-backup  my-cluster   backup-20250115-020000  Completed   1h

# Detailed status
kubectl get antflyrestore restore-from-backup -o yaml
```

Status fields:

```yaml
status:
  phase: Completed  # Pending, Running, Completed, or Failed
  startTime: "2025-01-15T10:00:00Z"
  completionTime: "2025-01-15T10:15:30Z"
  tables:
    - name: users
      status: Completed
    - name: orders
      status: Completed
  jobName: "restore-from-backup-job"
  conditions:
    - type: JobReady
      status: "True"
    - type: ClusterReady
      status: "True"
```

## Storage Backends

### Amazon S3

```yaml
spec:
  destination:
    location: s3://my-bucket/antfly-backups
    credentialsSecret:
      name: s3-credentials
```

Create credentials:

```bash
kubectl create secret generic s3-credentials \
  --from-literal=AWS_ACCESS_KEY_ID='AKIAIOSFODNN7EXAMPLE' \
  --from-literal=AWS_SECRET_ACCESS_KEY='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY' \
  --from-literal=AWS_REGION='us-east-2'
```

### Google Cloud Storage (S3-Compatible)

GCS supports S3-compatible access with HMAC credentials:

```yaml
spec:
  destination:
    location: s3://my-gcs-bucket/antfly-backups
    credentialsSecret:
      name: gcs-credentials
```

Create HMAC credentials in GCP Console, then:

```bash
kubectl create secret generic gcs-credentials \
  --from-literal=AWS_ACCESS_KEY_ID='GOOGABC123DEF456' \
  --from-literal=AWS_SECRET_ACCESS_KEY='your-hmac-secret' \
  --from-literal=AWS_ENDPOINT_URL='https://storage.googleapis.com' \
  --from-literal=AWS_REGION='auto'
```

### Local Filesystem (Testing Only)

```yaml
spec:
  destination:
    location: file:///backups/antfly
```

**Warning**: Local filesystem backups are stored on the pod's filesystem and are lost when the pod is deleted. Use only for testing.

## Credentials Management

### Using Secrets

The most common approach for static credentials:

```yaml
spec:
  destination:
    credentialsSecret:
      name: backup-credentials
```

Secret must contain AWS SDK-compatible keys:

| Key | Required | Description |
|-----|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | Access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | Secret key |
| `AWS_REGION` | No | AWS region (default: us-east-1) |
| `AWS_ENDPOINT_URL` | No | Custom endpoint for S3-compatible storage |

### Using IRSA (AWS)

For EKS clusters with IRSA, omit the credentialsSecret:

```yaml
spec:
  destination:
    location: s3://my-bucket/backups
    # No credentialsSecret - uses IRSA
```

Ensure:
1. The cluster's `serviceAccountName` is set
2. The ServiceAccount has the IRSA annotation
3. The IAM role has S3 permissions

### Using Workload Identity (GCP)

For GKE clusters, omit the credentialsSecret:

```yaml
spec:
  destination:
    location: s3://my-gcs-bucket/backups
    # No credentialsSecret - uses Workload Identity
```

Ensure:
1. The cluster's `serviceAccountName` is set
2. The Kubernetes SA is bound to a GCP SA
3. The GCP SA has Storage permissions

## Injecting Credentials via envFrom

For clusters that need backup credentials in their pods (for direct backup commands):

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: my-cluster
spec:
  metadataNodes:
    envFrom:
      - secretRef:
          name: backup-credentials
  dataNodes:
    envFrom:
      - secretRef:
          name: backup-credentials
```

See [Secrets Management](../security/secrets-management.md) for details.

## Monitoring Backups

### View Backup Jobs

```bash
# List CronJobs
kubectl get cronjob -l antfly.io/backup=daily-backup

# List Jobs from recent backups
kubectl get jobs -l antfly.io/backup=daily-backup

# View backup pod logs
kubectl logs -l job-name=daily-backup-cron-xxxxx
```

### Check Conditions

```bash
# Check backup schedule status
kubectl get antflybackup daily-backup -o jsonpath='{.status.conditions}' | jq

# Check for failures
kubectl get antflybackup -o jsonpath='{range .items[*]}{.metadata.name}: {.status.phase}{"\n"}{end}'
```

### Set Up Alerts

Monitor the `ScheduleReady` and `ClusterReady` conditions for backup health.

## Cross-Namespace Restore

Restore to a cluster in a different namespace:

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
    location: s3://my-bucket/production-backups  # From production
    credentialsSecret:
      name: backup-credentials
```

## On-Demand Backups

Trigger an immediate backup by creating an AntflyBackup with a schedule in the past:

```yaml
apiVersion: antfly.io/v1
kind: AntflyBackup
metadata:
  name: manual-backup-$(date +%s)
spec:
  clusterRef:
    name: my-cluster
  schedule: "* * * * *"  # Run every minute (will run once immediately)
  destination:
    location: s3://my-bucket/manual-backups
  suspend: true  # Suspend after first run
```

Or use the Antfly CLI directly:

```bash
kubectl exec -it my-cluster-metadata-0 -- antfly backup create s3://my-bucket/backups
```

## Best Practices

1. **Schedule Backups During Low Traffic**: Run backups during off-peak hours
2. **Test Restores Regularly**: Verify backups can be restored successfully
3. **Use Multiple Regions**: Store backups in a different region for disaster recovery
4. **Retain Sufficient History**: Keep enough backups for your recovery point objectives
5. **Monitor Backup Health**: Set up alerts for failed backups
6. **Encrypt Backups**: Use S3 server-side encryption or EBS encryption
7. **Document Restore Procedures**: Have runbooks for different recovery scenarios

## Troubleshooting

### Backup Not Running

```bash
# Check CronJob
kubectl describe cronjob <backup-name>-cron

# Check backup conditions
kubectl get antflybackup <name> -o jsonpath='{.status.conditions}'

# Check operator logs
kubectl logs -n antfly-operator-namespace -l app.kubernetes.io/name=antfly-operator | grep -i backup
```

### Credentials Issues

```bash
# Verify secret exists
kubectl get secret backup-credentials

# Check secret keys
kubectl get secret backup-credentials -o jsonpath='{.data}' | jq -r 'keys[]'

# Test credentials (from backup pod)
kubectl exec -it <backup-pod> -- aws s3 ls s3://my-bucket/
```

### Restore Failing

```bash
# Check restore status
kubectl describe antflyrestore <name>

# Check job logs
kubectl logs -l job-name=<restore-job-name>

# Check restore conditions
kubectl get antflyrestore <name> -o jsonpath='{.status.conditions}'
```

### Timeout Issues

Increase timeouts for large datasets:

```yaml
spec:
  backupTimeout: 4h   # For AntflyBackup
  restoreTimeout: 8h  # For AntflyRestore
```

## See Also

- [Secrets Management](../security/secrets-management.md): Credential management
- [AntflyBackup API Reference](../reference/antflybackup-api.md): Complete API reference
- [AntflyRestore API Reference](../reference/antflyrestore-api.md): Complete API reference
