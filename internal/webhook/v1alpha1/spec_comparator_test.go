/*
Copyright (c) Amazon Web Services
Distributed under the terms of the MIT license
*/

package v1alpha1

import (
	"testing"

	workspacev1alpha1 "github.com/jupyter-infra/jupyter-k8s/api/v1alpha1"
)

// ref builds an integrationTemplateRef with the given name and parameters, so the table below reads as
// the reference set a workspace carries.
func ref(name string, params ...workspacev1alpha1.IntegrationParameter) workspacev1alpha1.IntegrationTemplateRef {
	return workspacev1alpha1.IntegrationTemplateRef{Name: name, Parameters: params}
}

// param builds a rayClusterName parameter with the given value (the only parameter these cases vary).
func param(value string) workspacev1alpha1.IntegrationParameter {
	return workspacev1alpha1.IntegrationParameter{Name: "rayClusterName", Value: value}
}

// TestIntegrationRefsChanged pins the update-time skip predicate: the per-resource authorization runs
// only when the referenced set actually changes, so a no-op or unrelated update issues no
// SubjectAccessReview. Semantic equality treats a nil and an empty slice as unchanged.
func TestIntegrationRefsChanged(t *testing.T) {
	cases := []struct {
		name string
		old  []workspacev1alpha1.IntegrationTemplateRef
		new  []workspacev1alpha1.IntegrationTemplateRef
		want bool
	}{
		{
			name: "identical single ref is unchanged",
			old:  []workspacev1alpha1.IntegrationTemplateRef{ref("ray-integration", param("c1"))},
			new:  []workspacev1alpha1.IntegrationTemplateRef{ref("ray-integration", param("c1"))},
			want: false,
		},
		{
			name: "nil and empty are unchanged",
			old:  nil,
			new:  []workspacev1alpha1.IntegrationTemplateRef{},
			want: false,
		},
		{
			name: "both empty is unchanged",
			old:  []workspacev1alpha1.IntegrationTemplateRef{},
			new:  []workspacev1alpha1.IntegrationTemplateRef{},
			want: false,
		},
		{
			name: "a changed parameter value is a change",
			old:  []workspacev1alpha1.IntegrationTemplateRef{ref("ray-integration", param("c1"))},
			new:  []workspacev1alpha1.IntegrationTemplateRef{ref("ray-integration", param("c2"))},
			want: true,
		},
		{
			name: "pointing at a different template is a change",
			old:  []workspacev1alpha1.IntegrationTemplateRef{ref("ray-integration")},
			new:  []workspacev1alpha1.IntegrationTemplateRef{ref("other-integration")},
			want: true,
		},
		{
			name: "adding a ref is a change",
			old:  nil,
			new:  []workspacev1alpha1.IntegrationTemplateRef{ref("ray-integration")},
			want: true,
		},
		{
			name: "removing a ref is a change",
			old:  []workspacev1alpha1.IntegrationTemplateRef{ref("ray-integration")},
			new:  nil,
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oldSpec := &workspacev1alpha1.WorkspaceSpec{IntegrationTemplateRefs: tc.old}
			newSpec := &workspacev1alpha1.WorkspaceSpec{IntegrationTemplateRefs: tc.new}
			if got := integrationRefsChanged(oldSpec, newSpec); got != tc.want {
				t.Errorf("integrationRefsChanged() = %v, want %v", got, tc.want)
			}
		})
	}
}
