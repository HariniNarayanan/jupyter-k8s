/*
Copyright (c) Amazon Web Services
Distributed under the terms of the MIT license
*/

package controller

import (
	"context"
	"fmt"

	workspacev1alpha1 "github.com/jupyter-infra/jupyter-k8s/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// This file is the build side of integrations: it renders each attached integration onto the pod template
// from the frozen values (status.resolvedIntegrations), loading the template for pod SHAPE but never
// reading the referenced resource (that is the capture side's job). Mirrors deployment_builder_access.go.
// Functions are ordered entry points first, then the helpers they call.

// applyIntegrationsToDeployment merges every attached integration's frozen values into the pod template
// -- same values yield the same template, so an unchanged integration never rolls the pod. Fail-closed:
// no frozen record yet -> skip (base-only); render error -> abort the build, leaving the running pod.
func (db *DeploymentBuilder) applyIntegrationsToDeployment(
	ctx context.Context,
	deployment *appsv1.Deployment,
	workspace *workspacev1alpha1.Workspace,
) error {
	logger := logf.FromContext(ctx)

	for i := range workspace.Spec.IntegrationTemplateRefs {
		ref := &workspace.Spec.IntegrationTemplateRefs[i]
		frozen := findResolvedIntegration(&workspace.Status, ref.Name)
		if frozen == nil {
			logger.V(1).Info("integration has no frozen values yet; building base pod without its overlay",
				"integration", ref.Name)
			continue
		}
		mods, shareProcessNamespace, err := db.buildIntegrationPodModifications(ctx, workspace, ref, frozen)
		if err != nil {
			return fmt.Errorf("integration %q overlay failed; preserving running deployment: %w", ref.Name, err)
		}
		applyPodModifications(&deployment.Spec.Template.Spec, mods)
		// OR-reduce: any integration requesting a shared PID namespace enables it; none can disable it.
		if shareProcessNamespace != nil && *shareProcessNamespace {
			deployment.Spec.Template.Spec.ShareProcessNamespace = shareProcessNamespace
		}
	}
	return nil
}

// buildIntegrationPodModifications loads the template for pod SHAPE but resolves {{ resource }} values
// from the frozen set (a missing frozen value is a hard error). shareProcessNamespace is carried through.
func (db *DeploymentBuilder) buildIntegrationPodModifications(
	ctx context.Context,
	workspace *workspacev1alpha1.Workspace,
	ref *workspacev1alpha1.IntegrationTemplateRef,
	frozen *workspacev1alpha1.ResolvedIntegration,
) (*workspacev1alpha1.PodModifications, *bool, error) {
	template, err := getIntegrationTemplate(ctx, db.client, db.options.DefaultTemplateNamespace, workspace, ref)
	if err != nil {
		return nil, nil, err
	}
	shareProcessNamespace := template.Spec.ShareProcessNamespace

	if template.Spec.DeploymentModifications == nil || template.Spec.DeploymentModifications.PodModifications == nil {
		return &workspacev1alpha1.PodModifications{}, shareProcessNamespace, nil
	}

	data := buildIntegrationTemplateData(workspace, ref)
	resolver := NewIntegrationTemplateResolver(NewFrozenResourceValueProvider(frozen.Values))
	mods, err := resolver.ResolvePodModifications(template.Spec.DeploymentModifications.PodModifications, data)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to render frozen pod modifications for integration %q: %w", ref.Name, err)
	}
	if mods == nil {
		mods = &workspacev1alpha1.PodModifications{}
	}
	return mods, shareProcessNamespace, nil
}

// applyPodModifications appends the integration's containers/volumes and merges its primary-container env
// (integration-owned key wins). Mirrors the AccessStrategy merge.
func applyPodModifications(ps *corev1.PodSpec, pm *workspacev1alpha1.PodModifications) {
	if pm == nil {
		return
	}
	ps.Containers = append(ps.Containers, pm.AdditionalContainers...)
	ps.InitContainers = append(ps.InitContainers, pm.InitContainers...)
	ps.Volumes = append(ps.Volumes, pm.Volumes...)

	if pm.PrimaryContainerModifications == nil {
		return
	}
	primary := findPrimaryContainer(ps)
	if primary == nil {
		// Should never happen -- buildPodSpec always creates the primary container. Log rather than
		// silently drop the primary-container env/volumeMounts if the pod template is ever malformed.
		logf.Log.Error(nil, "primary container not found in pod spec; dropping integration primary-container modifications",
			"expectedContainerName", PrimaryContainerName)
		return
	}
	for _, env := range pm.PrimaryContainerModifications.MergeEnv {
		// MergeEnv[].ValueTemplate has already been resolved to a literal by the resolver.
		setEnv(primary, env.Name, env.ValueTemplate)
	}
	primary.VolumeMounts = append(primary.VolumeMounts, pm.PrimaryContainerModifications.VolumeMounts...)
}

// findPrimaryContainer returns the workspace's primary container by its well-known name.
func findPrimaryContainer(ps *corev1.PodSpec) *corev1.Container {
	for i := range ps.Containers {
		if ps.Containers[i].Name == PrimaryContainerName {
			return &ps.Containers[i]
		}
	}
	return nil
}

// setEnv sets (overwriting an existing same-named entry) an env var on a container.
func setEnv(c *corev1.Container, name, value string) {
	for i := range c.Env {
		if c.Env[i].Name == name {
			c.Env[i].Value = value
			return
		}
	}
	c.Env = append(c.Env, corev1.EnvVar{Name: name, Value: value})
}
