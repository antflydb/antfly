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

	antflyv1 "github.com/antflydb/antfly/pkg/operator/antfly/api/v1"
)

// AntflyRestoreReconciler reconciles an AntflyRestore object
type AntflyRestoreReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

//+kubebuilder:rbac:groups=antfly.io,resources=antflyrestores,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=antfly.io,resources=antflyrestores/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=antfly.io,resources=antflyrestores/finalizers,verbs=update
//+kubebuilder:rbac:groups=antfly.io,resources=antflyclusters,verbs=get;list;watch
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile handles AntflyRestore reconciliation
func (r *AntflyRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the AntflyRestore resource
	restore := &antflyv1.AntflyRestore{}
	if err := r.Get(ctx, req.NamespacedName, restore); err != nil {
		if errors.IsNotFound(err) {
			// Resource deleted, nothing to do
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get AntflyRestore")
		return ctrl.Result{}, err
	}

	// If restore is already completed or failed, skip reconciliation
	if restore.Status.Phase == antflyv1.RestorePhaseCompleted ||
		restore.Status.Phase == antflyv1.RestorePhaseFailed {
		log.Info("Restore already finished", "phase", restore.Status.Phase)
		return ctrl.Result{}, nil
	}

	// Validate configuration (fallback when webhook is disabled).
	// Note: immutability and phase-based guards require the old object
	// and are only enforced by the admission webhook.
	if err := restore.ValidateAntflyRestore(); err != nil {
		log.Error(err, "AntflyRestore validation failed")
		r.updateStatusWithError(ctx, restore, antflyv1.RestorePhaseFailed, antflyv1.TypeRestoreJobReady, antflyv1.ReasonRestoreValidationFailed, err.Error())
		return ctrl.Result{}, nil
	}

	// Fetch the referenced AntflyCluster.
	// Use Pending phase (not Failed) for cluster-not-found since this is a
	// transient condition — the cluster may not be ready yet. Requeue to retry.
	cluster, err := r.getReferencedCluster(ctx, restore)
	if err != nil {
		log.Error(err, "Failed to get referenced AntflyCluster")
		r.updateStatusWithError(ctx, restore, antflyv1.RestorePhasePending, antflyv1.TypeRestoreClusterReady, antflyv1.ReasonRestoreClusterNotFound, err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Check if Job already exists
	job := &batchv1.Job{}
	jobName := restore.Name + "-restore"
	err = r.Get(ctx, types.NamespacedName{
		Name:      jobName,
		Namespace: restore.Namespace,
	}, job)

	if errors.IsNotFound(err) {
		// Create the Job
		if err := r.createRestoreJob(ctx, restore, cluster); err != nil {
			log.Error(err, "Failed to create restore Job")
			r.updateStatusWithError(ctx, restore, antflyv1.RestorePhaseFailed, antflyv1.TypeRestoreJobReady, antflyv1.ReasonRestoreJobCreationFailed, err.Error())
			return ctrl.Result{}, err
		}

		// Update status to Running
		restore.Status.Phase = antflyv1.RestorePhaseRunning
		restore.Status.StartTime = new(metav1.Now())
		restore.Status.JobName = jobName
		r.setCondition(restore, metav1.Condition{
			Type:               antflyv1.TypeRestoreJobReady,
			Status:             metav1.ConditionTrue,
			Reason:             antflyv1.ReasonRestoreJobCreated,
			Message:            "Restore job created successfully",
			LastTransitionTime: metav1.Now(),
		})
		if err := r.Status().Update(ctx, restore); err != nil {
			log.Error(err, "Failed to update status")
			return ctrl.Result{}, err
		}

		log.Info("Created restore Job", "name", jobName)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil

	} else if err != nil {
		log.Error(err, "Failed to get Job")
		return ctrl.Result{}, err
	}

	// Job exists, check its status
	if err := r.updateStatusFromJob(ctx, restore, job); err != nil {
		log.Error(err, "Failed to update status from job")
		return ctrl.Result{}, err
	}

	// Requeue if still running
	if restore.Status.Phase == antflyv1.RestorePhaseRunning {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

// getReferencedCluster fetches the AntflyCluster referenced by the restore
func (r *AntflyRestoreReconciler) getReferencedCluster(ctx context.Context, restore *antflyv1.AntflyRestore) (*antflyv1.AntflyCluster, error) {
	namespace := restore.Spec.ClusterRef.Namespace
	if namespace == "" {
		namespace = restore.Namespace
	}

	cluster := &antflyv1.AntflyCluster{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      restore.Spec.ClusterRef.Name,
		Namespace: namespace,
	}, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get AntflyCluster %s/%s: %w",
			namespace, restore.Spec.ClusterRef.Name, err)
	}

	return cluster, nil
}

// createRestoreJob creates the Job for the restore operation
func (r *AntflyRestoreReconciler) createRestoreJob(ctx context.Context, restore *antflyv1.AntflyRestore, cluster *antflyv1.AntflyCluster) error {
	job := r.buildRestoreJob(restore, cluster)

	// Set controller reference for garbage collection
	if err := controllerutil.SetControllerReference(restore, job, r.Scheme); err != nil {
		return err
	}

	if err := r.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create Job: %w", err)
	}

	return nil
}

// buildRestoreJob creates the Job spec for restore operations
func (r *AntflyRestoreReconciler) buildRestoreJob(restore *antflyv1.AntflyRestore, cluster *antflyv1.AntflyCluster) *batchv1.Job {
	// Determine cluster namespace for service URL
	clusterNamespace := restore.Spec.ClusterRef.Namespace
	if clusterNamespace == "" {
		clusterNamespace = restore.Namespace
	}

	// Build the cluster API URL using the public-api service
	clusterURL := fmt.Sprintf("http://%s-public-api.%s.svc.cluster.local",
		cluster.Name, clusterNamespace)

	// Build CLI arguments
	args := []string{
		"restore",
		"--url", clusterURL,
		"--backup-id", restore.Spec.Source.BackupID,
		"--location", restore.Spec.Source.Location,
	}

	// Add restore mode
	restoreMode := string(restore.Spec.RestoreMode)
	if restoreMode == "" {
		restoreMode = string(antflyv1.RestoreModeFailIfExists)
	}
	args = append(args, "--mode", restoreMode)

	// Add table filter if specified
	if len(restore.Spec.Tables) > 0 {
		args = append(args, "--tables", strings.Join(restore.Spec.Tables, ","))
	}

	// Build environment from secret if provided
	var envFrom []corev1.EnvFromSource
	if restore.Spec.Source.CredentialsSecret != nil {
		envFrom = []corev1.EnvFromSource{
			{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: restore.Spec.Source.CredentialsSecret.Name,
					},
				},
			},
		}
	}

	// Calculate timeout (default: 2 hours)
	timeoutSeconds := int64(7200)
	if restore.Spec.RestoreTimeout != nil {
		timeoutSeconds = int64(restore.Spec.RestoreTimeout.Seconds())
	}

	// Backoff limit
	backoffLimit := new(int32(3))
	if restore.Spec.BackoffLimit != nil {
		backoffLimit = restore.Spec.BackoffLimit
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restore.Name + "-restore",
			Namespace: restore.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "antfly-restore",
				"app.kubernetes.io/component":  "restore",
				"app.kubernetes.io/managed-by": "antfly-operator",
				"antfly.io/restore":            restore.Name,
			},
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: new(timeoutSeconds),
			BackoffLimit:          backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":       "antfly-restore",
						"app.kubernetes.io/component":  "restore",
						"app.kubernetes.io/managed-by": "antfly-operator",
						"antfly.io/restore":            restore.Name,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:  "restore",
							Image: cluster.Spec.Image,
							// Uses exec form (not shell), so args are passed
							// directly as argv without shell interpretation —
							// no quoting needed (unlike the backup controller
							// which uses shell form for $(date) expansion).
							Command: []string{"/antfly", "cli"},
							Args:    args,
							EnvFrom: envFrom,
						},
					},
				},
			},
		},
	}
}

// updateStatusFromJob updates the restore status based on the Job status
func (r *AntflyRestoreReconciler) updateStatusFromJob(ctx context.Context, restore *antflyv1.AntflyRestore, job *batchv1.Job) error {
	// Check for completion
	for _, condition := range job.Status.Conditions {
		switch condition.Type {
		case batchv1.JobComplete:
			if condition.Status == corev1.ConditionTrue {
				restore.Status.Phase = antflyv1.RestorePhaseCompleted
				restore.Status.CompletionTime = job.Status.CompletionTime
				restore.Status.Message = "Restore completed successfully"
				r.setCondition(restore, metav1.Condition{
					Type:               antflyv1.TypeRestoreJobReady,
					Status:             metav1.ConditionTrue,
					Reason:             antflyv1.ReasonRestoreJobCompleted,
					Message:            "Restore job completed successfully",
					LastTransitionTime: metav1.Now(),
				})
				r.Recorder.Eventf(restore, nil, corev1.EventTypeNormal, "RestoreCompleted", "RestoreCompleted",
					"Restore from backup %s completed successfully", restore.Spec.Source.BackupID)
			}

		case batchv1.JobFailed:
			if condition.Status == corev1.ConditionTrue {
				restore.Status.Phase = antflyv1.RestorePhaseFailed
				restore.Status.CompletionTime = new(metav1.Now())
				restore.Status.Message = fmt.Sprintf("Restore failed: %s", condition.Message)
				r.setCondition(restore, metav1.Condition{
					Type:               antflyv1.TypeRestoreJobReady,
					Status:             metav1.ConditionFalse,
					Reason:             antflyv1.ReasonRestoreJobFailed,
					Message:            condition.Message,
					LastTransitionTime: metav1.Now(),
				})
				r.Recorder.Eventf(restore, nil, corev1.EventTypeWarning, "RestoreFailed", "RestoreFailed",
					"Restore from backup %s failed: %s", restore.Spec.Source.BackupID, condition.Message)
			}
		}
	}

	// Still running if no terminal condition
	if restore.Status.Phase != antflyv1.RestorePhaseCompleted &&
		restore.Status.Phase != antflyv1.RestorePhaseFailed {
		restore.Status.Phase = antflyv1.RestorePhaseRunning
		r.setCondition(restore, metav1.Condition{
			Type:               antflyv1.TypeRestoreJobReady,
			Status:             metav1.ConditionTrue,
			Reason:             antflyv1.ReasonRestoreJobRunning,
			Message:            "Restore job is running",
			LastTransitionTime: metav1.Now(),
		})
	}

	return r.Status().Update(ctx, restore)
}

// updateStatusWithError updates the status with an error message.
// Only sets CompletionTime and emits failure events for terminal phases.
func (r *AntflyRestoreReconciler) updateStatusWithError(ctx context.Context, restore *antflyv1.AntflyRestore, phase antflyv1.RestorePhase, conditionType, reason, message string) {
	restore.Status.Phase = phase
	restore.Status.Message = message

	// Only set CompletionTime for terminal phases
	if phase == antflyv1.RestorePhaseFailed || phase == antflyv1.RestorePhaseCompleted {
		restore.Status.CompletionTime = new(metav1.Now())
	}

	r.setCondition(restore, metav1.Condition{
		Type:               conditionType,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	})

	if err := r.Status().Update(ctx, restore); err != nil {
		log.FromContext(ctx).Error(err, "Failed to update status with error")
	}

	// Emit appropriate event based on phase
	if phase == antflyv1.RestorePhaseFailed {
		r.Recorder.Eventf(restore, nil, corev1.EventTypeWarning, "RestoreFailed", "RestoreFailed", "%s", message)
	} else {
		r.Recorder.Eventf(restore, nil, corev1.EventTypeWarning, reason, reason, "%s", message)
	}
}

// setCondition updates or adds a condition to the restore status
func (r *AntflyRestoreReconciler) setCondition(restore *antflyv1.AntflyRestore, condition metav1.Condition) {
	for i, existing := range restore.Status.Conditions {
		if existing.Type == condition.Type {
			restore.Status.Conditions[i] = condition
			return
		}
	}
	restore.Status.Conditions = append(restore.Status.Conditions, condition)
}

// SetupWithManager sets up the controller with the Manager
func (r *AntflyRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&antflyv1.AntflyRestore{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
