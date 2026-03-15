package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
)

// AntflyBackupReconciler reconciles an AntflyBackup object
type AntflyBackupReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

//+kubebuilder:rbac:groups=antfly.io,resources=antflybackups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=antfly.io,resources=antflybackups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=antfly.io,resources=antflybackups/finalizers,verbs=update
//+kubebuilder:rbac:groups=antfly.io,resources=antflyclusters,verbs=get;list;watch
//+kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile handles AntflyBackup reconciliation
func (r *AntflyBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the AntflyBackup resource
	backup := &antflyv1.AntflyBackup{}
	if err := r.Get(ctx, req.NamespacedName, backup); err != nil {
		if errors.IsNotFound(err) {
			// Resource deleted, nothing to do
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get AntflyBackup")
		return ctrl.Result{}, err
	}

	// If already Failed and the spec hasn't changed since we last observed it,
	// skip reconciliation. If the user edits the spec (bumping Generation),
	// re-run validation to allow recovery without delete/recreate.
	if backup.Status.Phase == antflyv1.BackupPhaseFailed &&
		backup.Status.ObservedGeneration >= backup.Generation {
		return ctrl.Result{}, nil
	}

	// Validate configuration (fallback when webhook is disabled).
	// Note: immutability checks (e.g. clusterRef changes) require the old object
	// and are only enforced by the admission webhook.
	if err := backup.ValidateAntflyBackup(); err != nil {
		log.Error(err, "AntflyBackup validation failed")
		r.updateStatusWithError(ctx, backup, antflyv1.BackupPhaseFailed, antflyv1.TypeBackupScheduleReady, antflyv1.ReasonBackupValidationFailed, err.Error())
		return ctrl.Result{}, nil
	}

	// Fetch the referenced AntflyCluster.
	// Use BackupPhasePending (not Failed) for cluster-not-found since this is a
	// transient condition — the cluster may not be ready yet. Requeue to retry.
	// Using Pending avoids tripping the early-exit guard (which checks
	// Phase == Failed && ObservedGeneration >= Generation).
	cluster, err := r.getReferencedCluster(ctx, backup)
	if err != nil {
		log.Error(err, "Failed to get referenced AntflyCluster")
		r.updateStatusWithError(ctx, backup, antflyv1.BackupPhasePending, antflyv1.TypeBackupClusterReady, antflyv1.ReasonClusterNotFound, err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Reconcile the CronJob
	if err := r.reconcileCronJob(ctx, backup, cluster); err != nil {
		log.Error(err, "Failed to reconcile CronJob")
		r.updateStatusWithError(ctx, backup, antflyv1.BackupPhaseFailed, antflyv1.TypeBackupScheduleReady, antflyv1.ReasonCronJobFailed, err.Error())
		return ctrl.Result{}, err
	}

	// Update status
	if err := r.updateStatus(ctx, backup); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// getReferencedCluster fetches the AntflyCluster referenced by the backup
func (r *AntflyBackupReconciler) getReferencedCluster(ctx context.Context, backup *antflyv1.AntflyBackup) (*antflyv1.AntflyCluster, error) {
	namespace := backup.Spec.ClusterRef.Namespace
	if namespace == "" {
		namespace = backup.Namespace
	}

	cluster := &antflyv1.AntflyCluster{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      backup.Spec.ClusterRef.Name,
		Namespace: namespace,
	}, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get AntflyCluster %s/%s: %w",
			namespace, backup.Spec.ClusterRef.Name, err)
	}

	return cluster, nil
}

// reconcileCronJob creates or updates the CronJob for scheduled backups
func (r *AntflyBackupReconciler) reconcileCronJob(ctx context.Context, backup *antflyv1.AntflyBackup, cluster *antflyv1.AntflyCluster) error {
	log := log.FromContext(ctx)

	cronJobName := backup.Name + "-backup"
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cronJobName,
			Namespace: backup.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cronJob, func() error {
		// Set controller reference for garbage collection
		if err := controllerutil.SetControllerReference(backup, cronJob, r.Scheme); err != nil {
			return err
		}

		// Build CronJob spec
		cronJob.Spec = r.buildCronJobSpec(backup, cluster)

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to create/update CronJob: %w", err)
	}

	log.Info("Reconciled CronJob", "name", cronJobName)
	return nil
}

// buildCronJobSpec creates the CronJob spec for backup operations
func (r *AntflyBackupReconciler) buildCronJobSpec(backup *antflyv1.AntflyBackup, cluster *antflyv1.AntflyCluster) batchv1.CronJobSpec {
	// Determine cluster namespace for service URL
	clusterNamespace := backup.Spec.ClusterRef.Namespace
	if clusterNamespace == "" {
		clusterNamespace = backup.Namespace
	}

	// Build the cluster API URL using the public-api service
	clusterURL := fmt.Sprintf("http://%s-public-api.%s.svc.cluster.local",
		cluster.Name, clusterNamespace)

	// Build shell command. All user-controlled values are shell-quoted to
	// prevent injection. backup.Name is additionally safe because Kubernetes
	// enforces RFC 1123 naming (alphanumeric and hyphens only).
	// The $(date ...) suffix is intentionally unquoted so the shell expands it
	// at runtime to produce a timestamped backup ID.
	cmd := "/antfly backup" +
		" --url " + shellQuote(clusterURL) +
		" --backup-id " + shellQuote(backup.Name) + "-$(date +%Y%m%d%H%M%S)" +
		" --location " + shellQuote(backup.Spec.Destination.Location)

	// Add table filter if specified
	if len(backup.Spec.Tables) > 0 {
		cmd += " --tables " + shellQuote(strings.Join(backup.Spec.Tables, ","))
	}

	// Build environment from secret if provided
	var envFrom []corev1.EnvFromSource
	if backup.Spec.Destination.CredentialsSecret != nil {
		envFrom = []corev1.EnvFromSource{
			{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: backup.Spec.Destination.CredentialsSecret.Name,
					},
				},
			},
		}
	}

	// Calculate timeout (default: 1 hour)
	timeoutSeconds := int64(3600)
	if backup.Spec.BackupTimeout != nil {
		timeoutSeconds = int64(backup.Spec.BackupTimeout.Seconds())
	}

	// Job history limits
	successfulJobsHistoryLimit := new(int32(3))
	failedJobsHistoryLimit := new(int32(1))
	if backup.Spec.SuccessfulJobsHistoryLimit != nil {
		successfulJobsHistoryLimit = backup.Spec.SuccessfulJobsHistoryLimit
	}
	if backup.Spec.FailedJobsHistoryLimit != nil {
		failedJobsHistoryLimit = backup.Spec.FailedJobsHistoryLimit
	}

	return batchv1.CronJobSpec{
		Schedule:          backup.Spec.Schedule,
		Suspend:           new(backup.Spec.Suspend),
		ConcurrencyPolicy: batchv1.ForbidConcurrent,
		// StartingDeadlineSeconds: If a backup is delayed more than 3 hours (e.g., scheduler
		// downtime), skip it entirely. This prevents "catch-up storms" where many missed
		// backups run simultaneously after an outage. Set to ~half the typical 6-hour
		// backup interval.
		StartingDeadlineSeconds:    new(int64(10800)),
		SuccessfulJobsHistoryLimit: successfulJobsHistoryLimit,
		FailedJobsHistoryLimit:     failedJobsHistoryLimit,
		JobTemplate: batchv1.JobTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					"app.kubernetes.io/name":       "antfly-backup",
					"app.kubernetes.io/component":  "backup",
					"app.kubernetes.io/managed-by": "antfly-operator",
					"antfly.io/backup":             backup.Name,
				},
			},
			Spec: batchv1.JobSpec{
				ActiveDeadlineSeconds: new(timeoutSeconds),
				BackoffLimit:          new(int32(3)),
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app.kubernetes.io/name":       "antfly-backup",
							"app.kubernetes.io/component":  "backup",
							"app.kubernetes.io/managed-by": "antfly-operator",
							"antfly.io/backup":             backup.Name,
						},
					},
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyOnFailure,
						Containers: []corev1.Container{
							{
								Name:  "backup",
								Image: cluster.Spec.Image,
								// Use shell to enable command substitution for backup ID
								Command: []string{"/bin/sh", "-c"},
								Args:    []string{cmd},
								EnvFrom: envFrom,
							},
						},
					},
				},
			},
		},
	}
}

// shellQuote quotes a string for safe use as a shell argument,
// preventing injection via metacharacters like $(), backticks, etc.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// updateStatus updates the AntflyBackup status
func (r *AntflyBackupReconciler) updateStatus(ctx context.Context, backup *antflyv1.AntflyBackup) error {
	// Record the generation we've successfully reconciled
	backup.Status.ObservedGeneration = backup.Generation

	// Determine phase based on suspend state
	if backup.Spec.Suspend {
		backup.Status.Phase = antflyv1.BackupPhaseSuspended
	} else {
		backup.Status.Phase = antflyv1.BackupPhaseActive
	}

	// Store CronJob name in status
	backup.Status.CronJobName = backup.Name + "-backup"

	// Update status conditions
	condition := metav1.Condition{
		Type:               antflyv1.TypeBackupScheduleReady,
		Status:             metav1.ConditionTrue,
		Reason:             antflyv1.ReasonCronJobCreated,
		Message:            "CronJob created successfully",
		LastTransitionTime: metav1.Now(),
	}
	r.setCondition(backup, condition)

	// Check for recent job completions to update backup history
	if err := r.updateBackupHistory(ctx, backup); err != nil {
		log.FromContext(ctx).Error(err, "Failed to update backup history")
		// Don't fail the reconciliation for history update errors
	}

	return r.Status().Update(ctx, backup)
}

// updateBackupHistory checks for completed jobs and updates the backup history
func (r *AntflyBackupReconciler) updateBackupHistory(ctx context.Context, backup *antflyv1.AntflyBackup) error {
	// List jobs created by the CronJob
	jobList := &batchv1.JobList{}
	if err := r.List(ctx, jobList,
		client.InNamespace(backup.Namespace),
		client.MatchingLabels{"antfly.io/backup": backup.Name},
	); err != nil {
		return err
	}

	// Find the most recent successful job
	var lastSuccessfulJob *batchv1.Job
	for i := range jobList.Items {
		job := &jobList.Items[i]
		if isJobSuccessful(job) {
			if lastSuccessfulJob == nil || job.Status.CompletionTime.After(lastSuccessfulJob.Status.CompletionTime.Time) {
				lastSuccessfulJob = job
			}
		}
	}

	// Update last successful backup if found
	if lastSuccessfulJob != nil {
		backup.Status.LastSuccessfulBackup = &antflyv1.BackupRecord{
			BackupID:       lastSuccessfulJob.Name,
			StartTime:      *lastSuccessfulJob.Status.StartTime,
			CompletionTime: lastSuccessfulJob.Status.CompletionTime,
			Status:         "Completed",
			Tables:         backup.Spec.Tables,
		}
		backup.Status.LastScheduledTime = lastSuccessfulJob.Status.CompletionTime
	}

	return nil
}

// isJobSuccessful checks if a job completed successfully
func isJobSuccessful(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// updateStatusWithError updates the status with an error message.
// Only sets ObservedGeneration for terminal phases (Failed) to allow the
// early-exit guard to prevent infinite reconciliation. Transient phases
// (Pending) intentionally skip it so requeues continue retrying.
func (r *AntflyBackupReconciler) updateStatusWithError(ctx context.Context, backup *antflyv1.AntflyBackup, phase antflyv1.BackupPhase, conditionType, reason, message string) {
	backup.Status.Phase = phase
	if phase == antflyv1.BackupPhaseFailed {
		backup.Status.ObservedGeneration = backup.Generation
	}

	condition := metav1.Condition{
		Type:               conditionType,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}
	r.setCondition(backup, condition)

	if err := r.Status().Update(ctx, backup); err != nil {
		log.FromContext(ctx).Error(err, "Failed to update status with error", "phase", phase, "reason", reason)
	}
}

// setCondition updates or adds a condition to the backup status
func (r *AntflyBackupReconciler) setCondition(backup *antflyv1.AntflyBackup, condition metav1.Condition) {
	for i, existing := range backup.Status.Conditions {
		if existing.Type == condition.Type {
			backup.Status.Conditions[i] = condition
			return
		}
	}
	backup.Status.Conditions = append(backup.Status.Conditions, condition)
}

// SetupWithManager sets up the controller with the Manager
func (r *AntflyBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&antflyv1.AntflyBackup{}).
		Owns(&batchv1.CronJob{}).
		Complete(r)
}
