/*
Copyright (c) Amazon Web Services
Distributed under the terms of the MIT license
*/

package v1alpha1

import (
	"context"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workspacev1alpha1 "github.com/jupyter-infra/jupyter-k8s/api/v1alpha1"
)

// IntegrationTemplateRefValidator validates a Workspace's integrationTemplateRefs at admission on two
// axes, in this order: (1) namespace scope -- a ref may only target the workspace's own namespace or the
// configured shared namespace (checked first, before any read, so a cross-namespace ref never triggers a
// read of another team's template); and (2) parameter completeness -- the workspace supplies every
// parameter the referenced WorkspaceIntegrationTemplate declares. Catching both at the USER's write --
// rather than at controller resolve time -- gives immediate, actionable feedback instead of a degraded
// workspace.
type IntegrationTemplateRefValidator struct {
	client           client.Client
	defaultNamespace string
}

// NewIntegrationTemplateRefValidator creates the validator. defaultNamespace is the shared namespace an
// integrationTemplateRef may additionally target (beyond the workspace's own namespace); it is also used
// to resolve a ref that omits its own namespace (the workspace's namespace is used per-workspace). This
// is the SAME value the WorkspaceTemplate guard uses (--default-template-namespace), so integration
// templates and workspace templates share one shared-namespace setting.
func NewIntegrationTemplateRefValidator(c client.Client, defaultNamespace string) *IntegrationTemplateRefValidator {
	return &IntegrationTemplateRefValidator{client: c, defaultNamespace: defaultNamespace}
}

// Validate checks each integrationTemplateRef the workspace attaches (see validateRef), rejecting the
// workspace on the first invalid one. It also returns non-blocking warnings (supplied parameters the
// template does not declare) aggregated across all refs -- surfaced to the user without failing admission.
func (v *IntegrationTemplateRefValidator) Validate(ctx context.Context, workspace *workspacev1alpha1.Workspace) (admission.Warnings, error) {
	log := logf.FromContext(ctx).WithName("integration-template-ref-validator")
	var warnings admission.Warnings
	for i := range workspace.Spec.IntegrationTemplateRefs {
		ref := &workspace.Spec.IntegrationTemplateRefs[i]
		refWarnings, err := v.validateRef(ctx, workspace, ref)
		if err != nil {
			log.Info("Rejected Workspace: invalid integrationTemplateRef",
				"workspace", workspace.GetName(), "namespace", workspace.GetNamespace(),
				"integrationTemplateRef", ref.Name, "error", err.Error())
			return warnings, err
		}
		warnings = append(warnings, refWarnings...)
	}
	return warnings, nil
}

// validateRef validates a single integrationTemplateRef, in order:
//  1. namespace scope -- the ref may only target the workspace's own or the configured shared namespace,
//     checked BEFORE the template read so a cross-namespace ref never triggers a read of another team's
//     template;
//  2. template existence -- the referenced template must exist (rejected if not), matching the
//     WorkspaceTemplate precedent (TemplateValidator.ValidateCreateWorkspace fetches and rejects a
//     missing template);
//  3. parameter completeness -- the workspace supplies every parameter the referenced template declares.
//
// Supplied-but-undeclared parameters are returned as a warning (they are ignored at resolve time, so
// they do not break the workspace, but often signal a typo).
func (v *IntegrationTemplateRefValidator) validateRef(
	ctx context.Context, workspace *workspacev1alpha1.Workspace, ref *workspacev1alpha1.IntegrationTemplateRef,
) (admission.Warnings, error) {
	if err := v.validateNamespaceScope(ref, workspace.Namespace); err != nil {
		return nil, err
	}

	ns := ref.Namespace
	if ns == "" {
		ns = workspace.Namespace
	}
	tmpl := &workspacev1alpha1.WorkspaceIntegrationTemplate{}
	err := v.client.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ns}, tmpl)
	// Fall back to the shared namespace, mirroring the controller's template resolution order
	// (ref-ns -> workspace-ns -> shared-ns). Without this the webhook would reject a by-name reference to
	// an admin-installed shared template that the controller would happily resolve -- so a chart-shipped
	// template in the shared namespace could never be referenced by name.
	if errors.IsNotFound(err) && v.defaultNamespace != "" && ns != v.defaultNamespace {
		if fallbackErr := v.client.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: v.defaultNamespace}, tmpl); fallbackErr == nil {
			err = nil
		} else if !errors.IsNotFound(fallbackErr) {
			return nil, fmt.Errorf("failed to load integration template %q from shared namespace %q: %w", ref.Name, v.defaultNamespace, fallbackErr)
		} else {
			// Reject a reference to a non-existent template at the user's write -- same as a Workspace
			// referencing a missing WorkspaceTemplate. Catching the typo here beats a degraded workspace.
			return nil, fmt.Errorf("integrationTemplateRefs[%q]: WorkspaceIntegrationTemplate %q not found in namespace %q or shared namespace %q", ref.Name, ref.Name, ns, v.defaultNamespace)
		}
	}
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("integrationTemplateRefs[%q]: WorkspaceIntegrationTemplate %q not found in namespace %q", ref.Name, ref.Name, ns)
		}
		return nil, fmt.Errorf("failed to load integration template %q: %w", ref.Name, err)
	}

	if err := validateWorkspaceIntegrationParameters(ref, tmpl); err != nil {
		return nil, err
	}
	return unusedParameterWarnings(ref, tmpl), nil
}

// validateNamespaceScope rejects an integrationTemplateRefs[].namespace that targets a namespace other
// than the workspace's own or the configured shared (default) namespace. This mirrors the workspace
// templateRef guard (TemplateValidator.validateTemplateNamespace) and the AccessStrategy guard: without
// it, a user could point an integrationTemplateRef at ANY namespace and have the operator -- which holds
// cluster-wide read on WorkspaceIntegrationTemplate -- resolve a template from another team's namespace
// (a confused-deputy cross-namespace read). Enforcing it at admission fails the write closed BEFORE any
// such read happens; the admission webhook is the single enforcement point (the controller resolver
// reads from the already-validated stored spec).
func (v *IntegrationTemplateRefValidator) validateNamespaceScope(ref *workspacev1alpha1.IntegrationTemplateRef, workspaceNamespace string) error {
	ns := ref.Namespace
	// Allowed: unset (defaults to the workspace namespace), the workspace's own namespace, or the
	// configured shared namespace.
	if ns == "" || ns == workspaceNamespace || (v.defaultNamespace != "" && ns == v.defaultNamespace) {
		return nil
	}
	// Rejected: name the shared namespace in the message only when one is configured.
	if v.defaultNamespace == "" {
		return fmt.Errorf(
			"integrationTemplateRefs[%q].namespace %q is not allowed: integration templates must be in the workspace namespace %q",
			ref.Name, ns, workspaceNamespace,
		)
	}
	return fmt.Errorf(
		"integrationTemplateRefs[%q].namespace %q is not allowed: integration templates must be in the workspace namespace %q or the shared namespace %q",
		ref.Name, ns, workspaceNamespace, v.defaultNamespace,
	)
}

// validateWorkspaceIntegrationParameters checks that the ref supplies every parameter the template
// declares (spec.parameters). Pure (no reads) so it is unit-tested directly. spec.parameters is
// list-map-keyed on name (unique) and in declaration order, so the first missing one yields a
// deterministic message.
func validateWorkspaceIntegrationParameters(
	ref *workspacev1alpha1.IntegrationTemplateRef,
	tmpl *workspacev1alpha1.WorkspaceIntegrationTemplate,
) error {
	supplied := ref.ParametersMap()
	for _, param := range tmpl.Spec.Parameters {
		if _, present := supplied[param.Name]; !present {
			return fmt.Errorf("integration %q requires parameter %q but the workspace does not supply it", ref.Name, param.Name)
		}
	}
	return nil
}

// unusedParameterWarnings returns a warning for each supplied parameter the template does not declare.
// Undeclared parameters are ignored at resolve time (so this is not a rejection), but a supplied name
// that no declared parameter matches is usually a typo of a real one -- surfacing it at the user's write
// turns a silently-ignored value into an actionable warning. Names are sorted for a deterministic message.
func unusedParameterWarnings(
	ref *workspacev1alpha1.IntegrationTemplateRef,
	tmpl *workspacev1alpha1.WorkspaceIntegrationTemplate,
) admission.Warnings {
	declared := make(map[string]struct{}, len(tmpl.Spec.Parameters))
	for _, param := range tmpl.Spec.Parameters {
		declared[param.Name] = struct{}{}
	}
	var unused []string
	for name := range ref.ParametersMap() {
		if _, ok := declared[name]; !ok {
			unused = append(unused, name)
		}
	}
	if len(unused) == 0 {
		return nil
	}
	sort.Strings(unused)
	var warnings admission.Warnings
	for _, name := range unused {
		warnings = append(warnings, fmt.Sprintf(
			"integrationTemplateRefs[%q]: parameter %q is not declared by the template and will be ignored (typo?)",
			ref.Name, name))
	}
	return warnings
}
