/*
Copyright (c) Amazon Web Services
Distributed under the terms of the MIT license
*/

package v1alpha1

import (
	"context"
	"fmt"
	"sort"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workspacev1alpha1 "github.com/jupyter-infra/jupyter-k8s/api/v1alpha1"
	"github.com/jupyter-infra/jupyter-k8s/internal/controller"
)

// IntegrationTemplateRefValidator validates a Workspace's integrationTemplateRefs at admission. It splits
// into two passes with deliberately different audiences and call sites:
//
//   - Validate: correctness checks -- namespace scope, template existence, and parameter completeness.
//     On CREATE it runs before the controller/admin bypass, so a dangling reference or a missing parameter
//     is reported even for privileged writers (a user error, not a permission concern). On UPDATE it runs
//     after the bypass and only when the refs change (like the other update checks), so a controller
//     finalizer/label write never re-validates a live workspace against a since-deleted template.
//   - ValidateGetResourceRefs: a PERMISSION check that the requesting user may get every resource the
//     referenced templates name. The webhook runs it only for non-admin users, after the bypass, because
//     it authorizes the human user -- not the operator -- against the objects the operator would otherwise
//     read on their behalf (the confused-deputy escalation it defends against).
//
// Catching all of these at the USER's write -- rather than at controller resolve time -- gives immediate,
// actionable feedback instead of a degraded workspace.
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
//
// It returns the resolved templates, parallel to workspace.Spec.IntegrationTemplateRefs, so the
// authorization pass (ValidateGetResourceRefs) reuses them instead of re-resolving: on both paths the
// webhook runs correctness before authorization under the same conditions, so the templates it loads are
// exactly the ones authorization needs. On error the returned slice is nil (the webhook rejects before
// authorizing).
func (v *IntegrationTemplateRefValidator) Validate(ctx context.Context, workspace *workspacev1alpha1.Workspace) ([]*workspacev1alpha1.WorkspaceIntegrationTemplate, admission.Warnings, error) {
	log := logf.FromContext(ctx).WithName("integration-template-ref-validator")
	var warnings admission.Warnings
	templates := make([]*workspacev1alpha1.WorkspaceIntegrationTemplate, len(workspace.Spec.IntegrationTemplateRefs))
	for i := range workspace.Spec.IntegrationTemplateRefs {
		ref := &workspace.Spec.IntegrationTemplateRefs[i]
		tmpl, refWarnings, err := v.validateRef(ctx, workspace, ref)
		if err != nil {
			log.Info("Rejected Workspace: invalid integrationTemplateRef",
				"workspace", workspace.GetName(), "namespace", workspace.GetNamespace(),
				"integrationTemplateRef", ref.Name, "error", err.Error())
			return nil, warnings, err
		}
		templates[i] = tmpl
		warnings = append(warnings, refWarnings...)
	}
	return templates, warnings, nil
}

// validateRef runs the correctness checks for a single integrationTemplateRef, in order:
//  1. namespace scope -- the ref may only target the workspace's own or the configured shared namespace,
//     checked BEFORE the template read so a cross-namespace ref never triggers a read of another team's
//     template;
//  2. template existence -- the referenced template must exist (rejected if not), matching the
//     WorkspaceTemplate precedent (TemplateValidator.ValidateCreateWorkspace fetches and rejects a
//     missing template);
//  3. parameter completeness -- the workspace supplies every parameter the referenced template declares.
//
// Per-resource authorization (whether the user may get the referenced objects) is NOT here: it is a
// permission check the webhook applies only to non-admin callers, in ValidateGetResourceRefs.
//
// It returns the resolved template so the caller can hand it to the authorization pass without a second
// load. Supplied-but-undeclared parameters are returned as a warning (they are ignored at resolve time,
// so they do not break the workspace, but often signal a typo).
func (v *IntegrationTemplateRefValidator) validateRef(
	ctx context.Context, workspace *workspacev1alpha1.Workspace, ref *workspacev1alpha1.IntegrationTemplateRef,
) (*workspacev1alpha1.WorkspaceIntegrationTemplate, admission.Warnings, error) {
	if err := v.validateNamespaceScope(ref, workspace.Namespace); err != nil {
		return nil, nil, err
	}
	tmpl, err := v.getTemplate(ctx, ref, workspace.Namespace)
	if err != nil {
		return nil, nil, err
	}
	if err := validateWorkspaceIntegrationParameters(ref, tmpl); err != nil {
		return nil, nil, err
	}
	return tmpl, unusedParameterWarnings(ref, tmpl), nil
}

// getTemplate loads the WorkspaceIntegrationTemplate a ref names, mirroring the controller's
// getIntegrationTemplate resolution: try the ref's own namespace (defaulting to the workspace's when
// unset), then fall back to the configured shared namespace. Without the fallback the webhook would
// reject a by-name reference to an admin-installed shared template that the controller would happily
// resolve -- so a chart-shipped template in the shared namespace could never be referenced by name. A
// template that resolves in neither namespace is rejected at the user's write, matching the
// WorkspaceTemplate precedent (a Workspace referencing a missing WorkspaceTemplate is rejected too).
func (v *IntegrationTemplateRefValidator) getTemplate(
	ctx context.Context, ref *workspacev1alpha1.IntegrationTemplateRef, workspaceNamespace string,
) (*workspacev1alpha1.WorkspaceIntegrationTemplate, error) {
	ns := ref.Namespace
	if ns == "" {
		ns = workspaceNamespace
	}

	tmpl := &workspacev1alpha1.WorkspaceIntegrationTemplate{}
	err := v.client.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ns}, tmpl)
	if err == nil {
		return tmpl, nil
	}
	if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to load integration template %q: %w", ref.Name, err)
	}

	// Not in the ref's namespace: fall back to the shared namespace when one is configured and distinct.
	if v.defaultNamespace == "" || ns == v.defaultNamespace {
		return nil, fmt.Errorf("integrationTemplateRefs[%q]: WorkspaceIntegrationTemplate %q not found in namespace %q", ref.Name, ref.Name, ns)
	}
	err = v.client.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: v.defaultNamespace}, tmpl)
	if err == nil {
		return tmpl, nil
	}
	if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to load integration template %q from shared namespace %q: %w", ref.Name, v.defaultNamespace, err)
	}
	return nil, fmt.Errorf("integrationTemplateRefs[%q]: WorkspaceIntegrationTemplate %q not found in namespace %q or shared namespace %q", ref.Name, ref.Name, ns, v.defaultNamespace)
}

// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create

// ValidateGetResourceRefs authorizes the requesting user for every resource the workspace's integration
// templates reference. A WorkspaceIntegrationTemplate may name other resources (e.g. a RayCluster) whose
// name and namespace come from user-supplied parameters; the operator holds cluster-wide read on those
// resources and reads them at reconcile time to inject their values into the workspace pod. Without this
// check a user who cannot get a referenced RayCluster could reference it and have the operator read it on
// their behalf -- a confused-deputy escalation.
//
// This is a PERMISSION check, so the webhook calls it only for non-admin users (the controller and admins
// are exempt, exactly as they are for the other permission checks) and, on update, only when the
// integration references actually change (see integrationRefsChanged). It fails closed: the first denied
// or unevaluable review rejects the workspace.
//
// templates are the resolved templates from Validate, parallel to workspace.Spec.IntegrationTemplateRefs;
// reusing them avoids re-resolving each template a second time in the same admission request. The webhook
// runs Validate before this pass under the same conditions (on create for every caller; on update, for
// non-admins when the refs change), so the templates are already loaded whenever this pass runs.
func (v *IntegrationTemplateRefValidator) ValidateGetResourceRefs(
	ctx context.Context,
	workspace *workspacev1alpha1.Workspace,
	templates []*workspacev1alpha1.WorkspaceIntegrationTemplate,
) error {
	if len(workspace.Spec.IntegrationTemplateRefs) == 0 {
		return nil
	}

	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return fmt.Errorf("cannot authorize integration resources without admission request context: %w", err)
	}

	for i := range workspace.Spec.IntegrationTemplateRefs {
		ref := &workspace.Spec.IntegrationTemplateRefs[i]
		if err := v.authorizeReferencedResources(ctx, workspace, ref, templates[i], req.UserInfo); err != nil {
			return err
		}
	}
	return nil
}

// authorizeReferencedResources issues a get SubjectAccessReview, as the requesting user, for each
// resource the template references. It resolves each resourceRef's name/namespace from the workspace and
// the ref's parameters using the SAME resolver the controller uses at reconcile, so admission authorizes
// exactly the object reconcile will fetch (including the resolver's empty-value guards).
func (v *IntegrationTemplateRefValidator) authorizeReferencedResources(
	ctx context.Context,
	workspace *workspacev1alpha1.Workspace,
	ref *workspacev1alpha1.IntegrationTemplateRef,
	tmpl *workspacev1alpha1.WorkspaceIntegrationTemplate,
	user authenticationv1.UserInfo,
) error {
	// Metadata expressions may reference only .Workspace and .Parameters ({{ resource }} is rejected on
	// metadata by template validation), so a resolver with no live-value provider is sufficient.
	resolver := controller.NewIntegrationTemplateResolver(nil)
	data := controller.IntegrationTemplateData{
		Workspace:  controller.IntegrationWorkspaceData{Name: workspace.Name, Namespace: workspace.Namespace},
		Parameters: ref.ParametersMap(),
	}

	for i := range tmpl.Spec.ResourceRefs {
		resourceRef := &tmpl.Spec.ResourceRefs[i]

		// The controller defaults an omitted resourceRef namespace to the workspace's namespace; resolve
		// against that copy so the review targets the same object reconcile will fetch.
		effectiveRef := *resourceRef
		if effectiveRef.Metadata.Namespace == "" {
			effectiveRef.Metadata.Namespace = workspace.Namespace
		}
		name, namespace, err := resolver.ResolveResourceRef(&effectiveRef, data)
		if err != nil {
			return fmt.Errorf("integrationTemplateRefs[%q]: %w", ref.Name, err)
		}

		resource, err := v.resourceForGVK(resourceRef.APIVersion, resourceRef.Kind)
		if err != nil {
			return fmt.Errorf("integrationTemplateRefs[%q]: %w", ref.Name, err)
		}

		review := &authorizationv1.SubjectAccessReview{
			Spec: authorizationv1.SubjectAccessReviewSpec{
				User:   user.Username,
				Groups: user.Groups,
				UID:    user.UID,
				Extra:  subjectAccessReviewExtra(user.Extra),
				ResourceAttributes: &authorizationv1.ResourceAttributes{
					Verb:      "get",
					Group:     resource.Group,
					Resource:  resource.Resource,
					Namespace: namespace,
					Name:      name,
				},
			},
		}
		if err := v.client.Create(ctx, review); err != nil {
			return fmt.Errorf("integrationTemplateRefs[%q]: authorizing access to %s %q in namespace %q: %w",
				ref.Name, resourceRef.Kind, name, namespace, err)
		}
		if !review.Status.Allowed {
			return fmt.Errorf("integrationTemplateRefs[%q]: user %q may not get %s %q in namespace %q",
				ref.Name, user.Username, resourceRef.Kind, name, namespace)
		}
	}
	return nil
}

// resourceForGVK maps a resourceRef's apiVersion/kind to its API resource using the RESTMapper.
func (v *IntegrationTemplateRefValidator) resourceForGVK(apiVersion, kind string) (schema.GroupVersionResource, error) {
	gvk := schema.FromAPIVersionAndKind(apiVersion, kind)
	mapping, err := v.client.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("cannot map %s to an API resource: %w", gvk, err)
	}
	return mapping.Resource, nil
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

// subjectAccessReviewExtra converts admission UserInfo extra attributes into the SubjectAccessReview
// shape so the review carries the same identity the API server would see on a direct request.
func subjectAccessReviewExtra(extra map[string]authenticationv1.ExtraValue) map[string]authorizationv1.ExtraValue {
	if len(extra) == 0 {
		return nil
	}
	out := make(map[string]authorizationv1.ExtraValue, len(extra))
	for key, value := range extra {
		out[key] = authorizationv1.ExtraValue(value)
	}
	return out
}
