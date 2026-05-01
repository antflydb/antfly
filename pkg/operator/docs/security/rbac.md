# RBAC Configuration

This guide covers Role-Based Access Control (RBAC) for the Antfly Operator.

## Overview

The Antfly Operator requires specific Kubernetes RBAC permissions to manage cluster resources. The installation includes all necessary RBAC configuration.

## Operator RBAC

### Service Account

The operator uses the ServiceAccount `antfly-operator-service-account` in the `antfly-operator-namespace`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: antfly-operator-service-account
  namespace: antfly-operator-namespace
```

**Important**: The exact name `antfly-operator-service-account` is required. Mismatches cause permission errors.

### ClusterRole

The operator's ClusterRole (`antfly-operator-cluster-role`) grants permissions for:

| Resource | Permissions |
|----------|-------------|
| StatefulSets | create, delete, get, list, patch, update, watch |
| Services | create, delete, get, list, patch, update, watch |
| ConfigMaps | create, delete, get, list, patch, update, watch |
| PersistentVolumeClaims | create, delete, get, list, patch, update, watch |
| Pods | get, list, watch |
| Secrets | get, list, watch |
| Events | create, patch |
| PodDisruptionBudgets | create, delete, get, list, patch, update, watch |
| AntflyClusters | get, list, watch, create, update, patch, delete |
| AntflyBackups | get, list, watch, create, update, patch, delete |
| AntflyRestores | get, list, watch, create, update, patch, delete |
| Metrics API | get, list |

Full ClusterRole definition:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: antfly-operator-cluster-role
rules:
  # Core resources for cluster management
  - apiGroups: ["apps"]
    resources: ["statefulsets"]
    verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]
  - apiGroups: [""]
    resources: ["services", "configmaps", "persistentvolumeclaims"]
    verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]

  # Pod Disruption Budgets
  - apiGroups: ["policy"]
    resources: ["poddisruptionbudgets"]
    verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]

  # Antfly CRDs
  - apiGroups: ["antfly.io"]
    resources: ["antflyclusters", "antflybackups", "antflyrestores"]
    verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]
  - apiGroups: ["antfly.io"]
    resources: ["antflyclusters/status", "antflybackups/status", "antflyrestores/status"]
    verbs: ["get", "patch", "update"]
  - apiGroups: ["antfly.io"]
    resources: ["antflyclusters/finalizers", "antflybackups/finalizers", "antflyrestores/finalizers"]
    verbs: ["update"]

  # Metrics for autoscaling
  - apiGroups: ["metrics.k8s.io"]
    resources: ["pods"]
    verbs: ["get", "list"]
```

### ClusterRoleBinding

The ClusterRoleBinding connects the ClusterRole to the ServiceAccount:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: antfly-operator-cluster-role-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: antfly-operator-cluster-role
subjects:
  - kind: ServiceAccount
    name: antfly-operator-service-account
    namespace: antfly-operator-namespace
```

## Verifying RBAC Configuration

### Check ServiceAccount

```bash
# Verify ServiceAccount exists
kubectl get serviceaccount -n antfly-operator-namespace antfly-operator-service-account

# Check ServiceAccount details
kubectl describe serviceaccount -n antfly-operator-namespace antfly-operator-service-account
```

### Check Deployment ServiceAccount

```bash
# Verify deployment uses correct ServiceAccount
kubectl get deployment antfly-operator -n antfly-operator-namespace \
  -o jsonpath='{.spec.template.spec.serviceAccountName}'
```

### Check ClusterRole

```bash
# Verify ClusterRole exists
kubectl get clusterrole antfly-operator-cluster-role

# View ClusterRole permissions
kubectl describe clusterrole antfly-operator-cluster-role

# Check for PDB permissions specifically
kubectl get clusterrole antfly-operator-cluster-role -o yaml | grep -A5 poddisruptionbudgets
```

### Check ClusterRoleBinding

```bash
# Verify ClusterRoleBinding exists
kubectl get clusterrolebinding antfly-operator-cluster-role-binding

# View binding details
kubectl describe clusterrolebinding antfly-operator-cluster-role-binding
```

### Test Permissions

```bash
# Test if operator can create StatefulSets
kubectl auth can-i create statefulsets \
  --as=system:serviceaccount:antfly-operator-namespace:antfly-operator-service-account

# Test if operator can list PodDisruptionBudgets
kubectl auth can-i list poddisruptionbudgets \
  --as=system:serviceaccount:antfly-operator-namespace:antfly-operator-service-account

# Test if operator can watch AntflyClusters
kubectl auth can-i watch antflyclusters.antfly.io \
  --as=system:serviceaccount:antfly-operator-namespace:antfly-operator-service-account
```

## Namespace-Scoped RBAC

By default, the operator watches all namespaces. To restrict to specific namespaces:

### 1. Use Role Instead of ClusterRole

Create a Role in each target namespace:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: antfly-operator-role
  namespace: production
rules:
  # Same rules as ClusterRole, but namespaced
  - apiGroups: ["apps"]
    resources: ["statefulsets"]
    verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]
  # ... (remaining rules)
```

### 2. Create RoleBinding

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: antfly-operator-rolebinding
  namespace: production
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: antfly-operator-role
subjects:
  - kind: ServiceAccount
    name: antfly-operator-service-account
    namespace: antfly-operator-namespace
```

### 3. Configure Operator Watch Namespace

Set the `WATCH_NAMESPACE` environment variable:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: antfly-operator
  namespace: antfly-operator-namespace
spec:
  template:
    spec:
      containers:
        - name: manager
          env:
            - name: WATCH_NAMESPACE
              value: "production,staging"  # Comma-separated namespaces
```

## User RBAC

### Cluster Administrator Role

Grant users permission to manage Antfly clusters:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: antfly-admin
rules:
  - apiGroups: ["antfly.io"]
    resources: ["antflyclusters", "antflybackups", "antflyrestores"]
    verbs: ["*"]
  - apiGroups: ["antfly.io"]
    resources: ["antflyclusters/status", "antflybackups/status", "antflyrestores/status"]
    verbs: ["get"]
```

### Read-Only Role

Grant users read-only access to Antfly resources:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: antfly-viewer
rules:
  - apiGroups: ["antfly.io"]
    resources: ["antflyclusters", "antflybackups", "antflyrestores"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["antfly.io"]
    resources: ["antflyclusters/status", "antflybackups/status", "antflyrestores/status"]
    verbs: ["get"]
```

### Backup Operator Role

Grant users permission to manage backups only:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: antfly-backup-operator
rules:
  - apiGroups: ["antfly.io"]
    resources: ["antflybackups", "antflyrestores"]
    verbs: ["*"]
  - apiGroups: ["antfly.io"]
    resources: ["antflyclusters"]
    verbs: ["get", "list", "watch"]
```

### Bind to User

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: user-antfly-admin
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: antfly-admin
subjects:
  - kind: User
    name: alice@example.com
    apiGroup: rbac.authorization.k8s.io
```

## Pod ServiceAccount

### Custom ServiceAccount for Pods

Configure a custom ServiceAccount for Antfly pods (used for Workload Identity):

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: antfly-workload-sa
  namespace: production
  annotations:
    # For GKE Workload Identity
    iam.gke.io/gcp-service-account: antfly@myproject.iam.gserviceaccount.com
    # OR for AWS IRSA
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/antfly-role
---
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: my-cluster
  namespace: production
spec:
  serviceAccountName: antfly-workload-sa
  # ... rest of spec
```

## Troubleshooting

### Permission Denied Errors

If you see errors like:
```
poddisruptionbudgets.policy is forbidden: User "system:serviceaccount:antfly-operator-namespace:WRONG-NAME" cannot list resource "poddisruptionbudgets"
```

1. **Check ServiceAccount name matches**:
   ```bash
   kubectl get deployment antfly-operator -n antfly-operator-namespace \
     -o jsonpath='{.spec.template.spec.serviceAccountName}'
   ```

2. **Verify ClusterRoleBinding**:
   ```bash
   kubectl get clusterrolebinding antfly-operator-cluster-role-binding -o yaml
   ```

3. **Ensure ServiceAccount exists**:
   ```bash
   kubectl get sa -n antfly-operator-namespace
   ```

### Missing Permissions

If the operator can't perform certain actions:

1. **Check the ClusterRole has the permission**:
   ```bash
   kubectl get clusterrole antfly-operator-cluster-role -o yaml | grep -A 10 "apiGroups"
   ```

2. **Test the permission**:
   ```bash
   kubectl auth can-i <verb> <resource> \
     --as=system:serviceaccount:antfly-operator-namespace:antfly-operator-service-account
   ```

3. **Update ClusterRole if needed**:
   ```bash
   kubectl edit clusterrole antfly-operator-cluster-role
   ```

### Infrastructure-as-Code Issues

When using Pulumi, Terraform, or Helm:

1. **Verify resource names match exactly**:
   - ServiceAccount: `antfly-operator-service-account`
   - ClusterRole: `antfly-operator-cluster-role`
   - ClusterRoleBinding: `antfly-operator-cluster-role-binding`

2. **Check for naming conventions that append suffixes**:
   - Helm may add release names
   - Pulumi/Terraform may add random suffixes

3. **Use the exact names from the install manifest**:
   ```bash
   curl -s https://antfly.io/antfly-operator-install.yaml | grep "name:" | head -20
   ```

## Best Practices

1. **Use Least Privilege**: Grant only necessary permissions
2. **Namespace Isolation**: Consider namespace-scoped RBAC for multi-tenant clusters
3. **Audit RBAC**: Regularly review permissions granted
4. **Separate Roles**: Create different roles for admins, operators, and viewers
5. **Document Custom Roles**: Keep track of custom RBAC configurations
6. **Test Permissions**: Verify permissions with `kubectl auth can-i`

## See Also

- [Installation](../getting-started/installation.md): Initial RBAC setup
- [Secrets Management](secrets-management.md): Managing secrets access
- [Troubleshooting](../troubleshooting.md): Common RBAC issues
