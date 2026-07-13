/*
Copyright (c) Amazon Web Services
Distributed under the terms of the MIT license
*/

package controller

import (
	"context"
	"fmt"
	"time"

	workspacev1alpha1 "github.com/jupyter-infra/jupyter-k8s/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Integration readiness probe reasons (machine-readable, CamelCase).
const (
	IntegrationReasonReady       = "Ready"
	IntegrationReasonProbeFailed = "ProbeFailed"
	IntegrationReasonPodNotFound = "PodNotFound"
	IntegrationReasonProbeError  = "ProbeError"
	// IntegrationReasonNotResolved is reported on status.integrationStatuses[] for an attached
	// integration that has no frozen resolution yet -- e.g. its first-attach capture failed because the
	// referenced resource does not exist or the template is broken. Surfacing it (rather than logging
	// only) lets an admin see an unresolved integration on the Workspace status. The detailed cause is
	// in the operator logs (reconcileIntegrationFreeze); the status message points there.
	IntegrationReasonNotResolved = "NotResolved"

	DefaultIntegrationProbeTimeoutSeconds = 5
	// DefaultIntegrationProbePeriod is the base re-probe cadence, and the sole source of it: the period
	// is an operator-level setting, not author-selectable per template. The Running reconcile has no
	// watch on the referenced resource, so it requeues on this period to refresh report-only status --
	// kept coarse (5m) because integration health is reported, not gating, so it need not be tight.
	// Mirrors DefaultIdleCheckInterval.
	DefaultIntegrationProbePeriod = 5 * time.Minute
	// MaxIntegrationProbeBackoff caps the exponential backoff a continuously-failing probe backs off to,
	// so a perpetually-broken integration re-execs at most this often rather than every base period.
	MaxIntegrationProbeBackoff = 30 * time.Minute
)

// The probe execs into the primary workspace container (PrimaryContainerName, defined in
// constants.go) so it observes the pod's real network/auth context -- mirroring the idle detector --
// regardless of which sidecars an integration injects. Not author-selectable.

// integrationProbeRequeueAfter is the re-probe interval for a verdict: the base period
// (DefaultIntegrationProbePeriod, the sole operator-level cadence) while ready, and an exponential
// backoff (base doubled per base-period of continuous failure, capped at MaxIntegrationProbeBackoff)
// while failing. Backoff is derived statelessly from how long the verdict has been failing --
// now - LastTransitionTime -- which preserveConditionTimestamp holds stable across a streak, so no
// failure counter is stored.
func integrationProbeRequeueAfter(status *workspacev1alpha1.IntegrationStatus, now time.Time) time.Duration {
	base := DefaultIntegrationProbePeriod
	if status == nil || status.State == IntegrationStateReady || len(status.Conditions) == 0 {
		return base
	}
	failingFor := now.Sub(status.Conditions[0].LastTransitionTime.Time)
	interval := base
	for elapsed := base; elapsed <= failingFor && interval < MaxIntegrationProbeBackoff; elapsed += base {
		interval *= 2
	}
	if interval > MaxIntegrationProbeBackoff {
		interval = MaxIntegrationProbeBackoff
	}
	return interval
}

// PodExecWithStderr is the exec dependency (satisfied by *PodExecUtil), injectable for tests.
type PodExecWithStderr interface {
	ExecInPodWithStderr(ctx context.Context, pod *corev1.Pod, containerName string, cmd []string, stdin string) (string, string, error)
}

// IntegrationProberInterface allows mocking in tests. FindRunningPod is called once per reconcile and
// the pod is passed into each Probe, so N integrations cost one pod lookup, not N.
type IntegrationProberInterface interface {
	FindRunningPod(ctx context.Context, workspace *workspacev1alpha1.Workspace) (*corev1.Pod, error)
	Probe(ctx context.Context, pod *corev1.Pod, integrationName string, probe *workspacev1alpha1.IntegrationStatusProbe) workspacev1alpha1.IntegrationStatus
}

// IntegrationProber execs the integration's statusProbe in the workspace container and returns a
// report-only verdict. No WorkspaceIntegration object is involved -- the probe spec is resolved
// inline from the template (same source as the sidecar) and passed in directly.
type IntegrationProber struct {
	client   client.Client
	execUtil PodExecWithStderr
}

// NewIntegrationProber creates a new IntegrationProber.
func NewIntegrationProber(c client.Client, execUtil PodExecWithStderr) *IntegrationProber {
	return &IntegrationProber{client: c, execUtil: execUtil}
}

var _ IntegrationProberInterface = &IntegrationProber{}

// Probe execs the integration's statusProbe command in the given workspace pod and returns the verdict.
// The caller resolves the pod once (FindRunningPod) and invokes this only for an integration that
// declares an Exec statusProbe (see probeIntegrationStatus). The probe is report-only: it never gates
// pod readiness or restarts the pod.
func (p *IntegrationProber) Probe(
	ctx context.Context,
	pod *corev1.Pod,
	integrationName string,
	probe *workspacev1alpha1.IntegrationStatusProbe,
) workspacev1alpha1.IntegrationStatus {
	logger := logf.FromContext(ctx).WithValues("pod", pod.Name, "integration", integrationName)

	timeout := time.Duration(resolveIntegrationProbeTimeout(probe)) * time.Second
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	stdout, stderr, execErr := p.execUtil.ExecInPodWithStderr(
		probeCtx, pod, PrimaryContainerName, probe.Exec.Command, "")
	if execErr != nil {
		msg := firstNonEmpty(stderr, stdout, execErr.Error())
		logger.V(1).Info("Integration status probe failed", "reason", IntegrationReasonProbeFailed, "message", msg)
		return buildIntegrationStatus(integrationName, false, IntegrationReasonProbeFailed, msg)
	}

	return buildIntegrationStatus(integrationName, true, IntegrationReasonReady, "")
}

// buildIntegrationStatus builds a KRO-style IntegrationStatus: a coarse State plus a single "Ready"
// condition carrying the machine-readable reason and human-readable message. LastTransitionTime is
// stamped now (the CRD schema requires it); callers that re-probe on a cadence should preserve the
// prior timestamp when the condition has not materially changed (see preserveConditionTimestamp) so
// an unchanged verdict does not churn the status on every probe.
func buildIntegrationStatus(name string, ready bool, reason, message string) workspacev1alpha1.IntegrationStatus {
	condStatus := metav1.ConditionFalse
	state := IntegrationStateDegraded
	if ready {
		condStatus = metav1.ConditionTrue
		state = IntegrationStateReady
	}
	return workspacev1alpha1.IntegrationStatus{
		Name:  name,
		State: state,
		Conditions: []metav1.Condition{{
			Type:               IntegrationConditionTypeReady,
			Status:             condStatus,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: metav1.Now(),
		}},
	}
}

// preserveConditionTimestamp copies the prior Ready-condition LastTransitionTime onto next when the
// verdict is unchanged (same Status and Reason), so a report-only re-probe does not churn the timestamp
// every cadence. Message is deliberately EXCLUDED from the comparison: a failing probe's message carries
// stderr, which can vary run-to-run (e.g. a timestamped error) without the verdict changing -- and a
// stable timestamp across a failing streak is also what integrationProbeRequeueAfter uses to back off.
func preserveConditionTimestamp(prior, next *workspacev1alpha1.IntegrationStatus) {
	if prior == nil || next == nil || len(prior.Conditions) == 0 || len(next.Conditions) == 0 {
		return
	}
	p, n := prior.Conditions[0], &next.Conditions[0]
	if p.Status == n.Status && p.Reason == n.Reason {
		n.LastTransitionTime = p.LastTransitionTime
	}
}

// FindRunningPod locates a running pod for the workspace by its labels (mirrors the idle checker).
func (p *IntegrationProber) FindRunningPod(ctx context.Context, workspace *workspacev1alpha1.Workspace) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := p.client.List(ctx, podList,
		client.InNamespace(workspace.Namespace),
		client.MatchingLabels(GenerateLabels(workspace.Name)),
	); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}
	for i := range podList.Items {
		// Skip a terminating pod: during a Recreate rollout the old pod can still report Running while it
		// is being torn down, and probing it would yield a stale verdict for the incoming pod.
		if podList.Items[i].Status.Phase == corev1.PodRunning && podList.Items[i].DeletionTimestamp == nil {
			return &podList.Items[i], nil
		}
	}
	return nil, fmt.Errorf("no running pod found for workspace")
}

func resolveIntegrationProbeTimeout(probe *workspacev1alpha1.IntegrationStatusProbe) int32 {
	if probe.TimeoutSeconds > 0 {
		return probe.TimeoutSeconds
	}
	return DefaultIntegrationProbeTimeoutSeconds
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// getIntegrationStatusProbeSpec returns the template's statusProbe with its exec command resolved from
// the FROZEN values (nil if the template declares none). Reads the template, not the referenced resource,
// so the freeze contract holds. The command carries the same {{ resource }} / {{ .Parameters }}
// expressions as the sidecar fields, so it must be rendered from the frozen record before it is exec'd --
// otherwise the probe runs against literal "{{ resource ... }}" text. A missing template errors so the
// caller reports not-ready rather than skip.
func (rm *ResourceManager) getIntegrationStatusProbeSpec(
	ctx context.Context,
	workspace *workspacev1alpha1.Workspace,
	ref *workspacev1alpha1.IntegrationTemplateRef,
) (*workspacev1alpha1.IntegrationStatusProbe, error) {
	template, err := getIntegrationTemplate(ctx, rm.client, rm.integrationTemplateNamespace, workspace, ref)
	if err != nil {
		return nil, err
	}
	if template.Spec.StatusProbe == nil {
		return nil, nil
	}
	frozen := findResolvedIntegration(&workspace.Status, ref.Name)
	if frozen == nil {
		// Caller gates on this: an unresolved integration is reported NotResolved before we get here.
		return nil, fmt.Errorf("integration %q has no frozen values to resolve its statusProbe command", ref.Name)
	}
	resolver := NewIntegrationTemplateResolver(NewFrozenResourceValueProvider(frozen.Values))
	return resolveStatusProbeCommand(resolver, template.Spec.StatusProbe, buildIntegrationTemplateData(workspace, ref))
}
