# Secrets Management

This guide covers managing credentials and secrets for Antfly clusters.

## Overview

Antfly clusters may need credentials for:

- **Backup/Restore**: S3 or GCS storage access
- **Cloud Provider APIs**: AWS, GCP service access
- **Custom Integrations**: External service connections

The operator supports multiple credential management approaches:

- Kubernetes Secrets with `envFrom`
- IRSA (IAM Roles for Service Accounts) on AWS
- Workload Identity on GCP
- External secret managers (via external-secrets operator)

## Using Kubernetes Secrets

### Basic Configuration

Inject secrets into pods using `envFrom`:

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

### Creating Secrets

For S3-compatible storage (AWS S3, GCS, MinIO):

```bash
kubectl create secret generic backup-credentials \
  --from-literal=AWS_ACCESS_KEY_ID='your-access-key' \
  --from-literal=AWS_SECRET_ACCESS_KEY='your-secret-key' \
  --from-literal=AWS_REGION='us-east-2'
```

For GCS via S3 API:

```bash
kubectl create secret generic backup-credentials \
  --from-literal=AWS_ACCESS_KEY_ID='GOOGABC123DEF456' \
  --from-literal=AWS_SECRET_ACCESS_KEY='your-hmac-secret' \
  --from-literal=AWS_ENDPOINT_URL='https://storage.googleapis.com' \
  --from-literal=AWS_REGION='auto'
```

### Expected Secret Keys

The Antfly database uses AWS SDK-compatible environment variables:

| Key | Required | Description |
|-----|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | Access key ID |
| `AWS_SECRET_ACCESS_KEY` | Yes | Secret access key |
| `AWS_REGION` | No | AWS region (default: us-east-1) |
| `AWS_ENDPOINT_URL` | No | Custom endpoint for S3-compatible storage |

### Using Prefix

Namespace environment variables with a prefix:

```yaml
spec:
  dataNodes:
    envFrom:
      - secretRef:
          name: backup-credentials
        prefix: "BACKUP_"  # Results in BACKUP_AWS_ACCESS_KEY_ID, etc.
```

### Multiple Secrets

Combine multiple secrets:

```yaml
spec:
  dataNodes:
    envFrom:
      - secretRef:
          name: backup-credentials
      - secretRef:
          name: monitoring-credentials
      - configMapRef:
          name: feature-flags
```

## Secret Rotation

### Automatic Rolling Updates

The operator computes a hash of secret data and adds it as a pod annotation (`antfly.io/envfrom-hash`). When secret content changes, pods are automatically restarted.

**How it works**:
1. Operator reads secret data
2. Computes SHA256 hash of contents
3. Adds hash to pod template annotation
4. When hash changes, pods roll automatically

### Manual Rotation

For immediate rotation, manually restart pods:

```bash
# Restart metadata nodes
kubectl rollout restart statefulset/my-cluster-metadata -n production

# Restart data nodes
kubectl rollout restart statefulset/my-cluster-data -n production
```

### Best Practices for Rotation

1. **Use short-lived credentials** when possible
2. **Rotate regularly** (at least every 90 days)
3. **Monitor for failures** after rotation
4. **Test in staging** before production rotation

## SecretsReady Condition

The operator tracks secret availability via the `SecretsReady` status condition.

### Check Status

```bash
# View SecretsReady condition
kubectl get antflycluster my-cluster -o jsonpath='{.status.conditions}' | \
  jq '.[] | select(.type=="SecretsReady")'
```

### Status Values

| Status | Reason | Description |
|--------|--------|-------------|
| `True` | `AllSecretsFound` | All referenced secrets exist |
| `False` | `SecretNotFound` | One or more secrets missing |

### Common Issues

**Secret not found**:
```
Type: SecretsReady
Status: False
Reason: SecretNotFound
Message: Secret "backup-credentials" not found in namespace "production"
```

**Fix**: Create the missing secret before the cluster or pods will be stuck.

## IRSA (AWS)

For EKS clusters, use IRSA instead of static credentials.

### Setup

1. **Create IAM Role** with trust policy for OIDC:
   ```bash
   OIDC_PROVIDER=$(aws eks describe-cluster --name my-cluster \
     --query "cluster.identity.oidc.issuer" --output text | sed 's|https://||')

   cat > trust-policy.json << EOF
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Principal": {
         "Federated": "arn:aws:iam::123456789012:oidc-provider/${OIDC_PROVIDER}"
       },
       "Action": "sts:AssumeRoleWithWebIdentity",
       "Condition": {
         "StringEquals": {
           "${OIDC_PROVIDER}:sub": "system:serviceaccount:production:antfly-sa"
         }
       }
     }]
   }
   EOF

   aws iam create-role --role-name antfly-backup-role \
     --assume-role-policy-document file://trust-policy.json
   ```

2. **Attach permissions**:
   ```bash
   aws iam put-role-policy --role-name antfly-backup-role \
     --policy-name s3-backup-access \
     --policy-document '{
       "Version": "2012-10-17",
       "Statement": [{
         "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
         "Resource": ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"]
       }]
     }'
   ```

3. **Create Kubernetes ServiceAccount**:
   ```yaml
   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: antfly-sa
     namespace: production
     annotations:
       eks.amazonaws.com/role-arn: "arn:aws:iam::123456789012:role/antfly-backup-role"
   ```

4. **Reference in AntflyCluster**:
   ```yaml
   spec:
     serviceAccountName: antfly-sa
     eks:
       enabled: true
       irsaRoleARN: "arn:aws:iam::123456789012:role/antfly-backup-role"
     # No envFrom needed - credentials are automatic
   ```

### IRSA vs Static Credentials

| Aspect | IRSA | Static Credentials |
|--------|------|-------------------|
| Security | More secure | Less secure |
| Rotation | Automatic | Manual |
| Audit | CloudTrail integration | Limited |
| Setup | More complex | Simple |
| Cross-account | Supported | Limited |

**Recommendation**: Use IRSA for production AWS workloads.

## Workload Identity (GCP)

For GKE clusters, use Workload Identity instead of static credentials.

### Setup

1. **Enable Workload Identity on cluster** (if not already):
   ```bash
   gcloud container clusters update my-cluster \
     --workload-pool=PROJECT_ID.svc.id.goog
   ```

2. **Create GCP Service Account**:
   ```bash
   gcloud iam service-accounts create antfly-backup-sa \
     --project=PROJECT_ID
   ```

3. **Grant permissions**:
   ```bash
   gcloud projects add-iam-policy-binding PROJECT_ID \
     --member="serviceAccount:antfly-backup-sa@PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/storage.objectAdmin"
   ```

4. **Bind to Kubernetes ServiceAccount**:
   ```bash
   gcloud iam service-accounts add-iam-policy-binding \
     antfly-backup-sa@PROJECT_ID.iam.gserviceaccount.com \
     --role="roles/iam.workloadIdentityUser" \
     --member="serviceAccount:PROJECT_ID.svc.id.goog[production/antfly-sa]"
   ```

5. **Create Kubernetes ServiceAccount**:
   ```yaml
   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: antfly-sa
     namespace: production
     annotations:
       iam.gke.io/gcp-service-account: antfly-backup-sa@PROJECT_ID.iam.gserviceaccount.com
   ```

6. **Reference in AntflyCluster**:
   ```yaml
   spec:
     serviceAccountName: antfly-sa
     # No envFrom needed - credentials are automatic
   ```

## External Secrets Operator

For organizations using external-secrets operator with HashiCorp Vault, AWS Secrets Manager, etc.

### Example with AWS Secrets Manager

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: backup-credentials
  namespace: production
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secrets-manager
    kind: ClusterSecretStore
  target:
    name: backup-credentials
    creationPolicy: Owner
  data:
    - secretKey: AWS_ACCESS_KEY_ID
      remoteRef:
        key: antfly/backup-credentials
        property: access_key_id
    - secretKey: AWS_SECRET_ACCESS_KEY
      remoteRef:
        key: antfly/backup-credentials
        property: secret_access_key
```

Then reference the secret normally:

```yaml
spec:
  dataNodes:
    envFrom:
      - secretRef:
          name: backup-credentials
```

## Security Best Practices

### Do

- **Use IRSA/Workload Identity** for cloud-native credential management
- **Rotate credentials regularly** (every 90 days minimum)
- **Use least privilege** - grant only necessary permissions
- **Encrypt secrets at rest** - enable Kubernetes secret encryption
- **Audit access** - enable audit logging for secret access
- **Use namespaces** - isolate secrets per environment

### Don't

- **Don't commit secrets** to version control
- **Don't use root/admin credentials** in applications
- **Don't share credentials** across environments
- **Don't hardcode credentials** in manifests
- **Don't use default ServiceAccount** for workloads needing cloud access

### Encryption at Rest

Enable Kubernetes secret encryption:

```yaml
# EncryptionConfiguration for kube-apiserver
apiVersion: apiserver.config.k8s.io/v1
kind: EncryptionConfiguration
resources:
  - resources:
      - secrets
    providers:
      - aescbc:
          keys:
            - name: key1
              secret: <base64-encoded-secret>
      - identity: {}
```

## Troubleshooting

### Secret Not Found

**Symptoms**: Pods stuck in `CreateContainerConfigError`

**Solution**:
```bash
# Check secret exists
kubectl get secret backup-credentials -n production

# Create if missing
kubectl create secret generic backup-credentials \
  --from-literal=AWS_ACCESS_KEY_ID='...' \
  --from-literal=AWS_SECRET_ACCESS_KEY='...'
```

### Invalid Credentials

**Symptoms**: Backup/restore operations fail with authentication errors

**Solution**:
```bash
# Verify credentials are correct
kubectl get secret backup-credentials -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d

# Test credentials (from a debug pod)
kubectl run debug --rm -it --image=amazon/aws-cli -- \
  aws s3 ls s3://my-bucket/
```

### IRSA Not Working

**Symptoms**: AWS API calls fail with "unable to assume role"

**Solution**:
```bash
# Verify ServiceAccount annotation
kubectl get sa antfly-sa -o yaml | grep eks.amazonaws.com

# Check trust policy
aws iam get-role --role-name antfly-backup-role \
  --query "Role.AssumeRolePolicyDocument"

# Verify OIDC provider
aws eks describe-cluster --name my-cluster \
  --query "cluster.identity.oidc"
```

## See Also

- [Backup & Restore](../operations/backup-restore.md): Using credentials for backups
- [AWS EKS](../cloud-platforms/aws-eks.md): IRSA configuration
- [GCP GKE](../cloud-platforms/gcp-gke.md): Workload Identity configuration
- [RBAC](rbac.md): Operator permissions
