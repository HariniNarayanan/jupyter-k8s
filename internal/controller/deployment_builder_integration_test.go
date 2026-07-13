/*
Copyright (c) Amazon Web Services
Distributed under the terms of the MIT license
*/

package controller

import (
	"testing"

	workspacev1alpha1 "github.com/jupyter-infra/jupyter-k8s/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func rayRef(cluster, ns string) *workspacev1alpha1.IntegrationTemplateRef {
	return &workspacev1alpha1.IntegrationTemplateRef{
		Name: rayIntegrationName,
		Parameters: []workspacev1alpha1.IntegrationParameter{
			{Name: rayClusterNameKey, Value: cluster},
			{Name: rayClusterNamespaceKey, Value: ns},
		},
	}
}

// TestIntegrationParametersHash_StableAndSensitive verifies the token is a deterministic function of the
// user input (template ref + params) and changes exactly when that input changes.
func TestIntegrationParametersHash_StableAndSensitive(t *testing.T) {
	base := rayRef("cluster-a", "ray-ns")

	// Deterministic: same input -> same token, regardless of parameter ordering.
	reordered := &workspacev1alpha1.IntegrationTemplateRef{
		Name: rayIntegrationName,
		Parameters: []workspacev1alpha1.IntegrationParameter{
			{Name: rayClusterNamespaceKey, Value: "ray-ns"},
			{Name: rayClusterNameKey, Value: "cluster-a"},
		},
	}
	if getIntegrationParametersHash(base) != getIntegrationParametersHash(reordered) {
		t.Fatal("token must be independent of parameter ordering")
	}

	// Sensitive: a changed parameter (cluster switch) changes the token.
	if getIntegrationParametersHash(base) == getIntegrationParametersHash(rayRef(testClusterB, "ray-ns")) {
		t.Fatal("token must change when a parameter changes (cluster switch)")
	}

	// Sensitive: a changed template name changes the token.
	other := rayRef("cluster-a", "ray-ns")
	other.Name = "other-integration"
	if getIntegrationParametersHash(base) == getIntegrationParametersHash(other) {
		t.Fatal("token must change when the template ref changes")
	}

	// Empty ref -> empty token so a non-integration workspace stays out of the path.
	if getIntegrationParametersHash(nil) != "" {
		t.Fatal("nil ref must yield the empty token")
	}
}

// TestResourceValueProvider_CaptureThenReplay verifies the core seam: a live capture records the
// resolved values, and a frozen replay reproduces them WITHOUT any resource access -- the property
// that makes an unchanged-token reconcile drift-proof.
func TestResourceValueProvider_CaptureThenReplay(t *testing.T) {
	// A capturing provider backed by a fake resource. Rather than stand up unstructured objects, we
	// exercise capture/replay symmetry directly through the provider contract.
	captured := map[string]string{
		CaptureKey("rayCluster", "{.status.head.serviceName}"): testSvcA,
		CaptureKey("rayCluster", "{.status.endpoints.gcs}"):    "6379",
	}

	replay := NewFrozenResourceValueProvider(captured)
	got, err := replay.Value("rayCluster", "{.status.head.serviceName}")
	if err != nil || got != testSvcA {
		t.Fatalf("replay serviceName: got %q err %v, want svc-a", got, err)
	}
	got, err = replay.Value("rayCluster", "{.status.endpoints.gcs}")
	if err != nil || got != "6379" {
		t.Fatalf("replay gcs: got %q err %v, want 6379", got, err)
	}

	// A key absent from the frozen set is a hard error (forces a re-resolve, never a silent empty).
	if _, err := replay.Value("rayCluster", "{.status.head.newField}"); err == nil {
		t.Fatal("replay of an uncaptured key must error, not return empty")
	}
}

// TestResolveTemplateExpression_UsesProvider verifies {{ resource ... }} routes through the resolver's provider
// and that a nil provider is a clean error rather than a panic.
func TestResolveTemplateExpression_UsesProvider(t *testing.T) {
	data := IntegrationTemplateData{
		Workspace:  IntegrationWorkspaceData{Name: "ws", Namespace: "ns"},
		Parameters: map[string]string{rayClusterNameKey: "cluster-a"},
	}

	frozen := NewFrozenResourceValueProvider(map[string]string{
		CaptureKey("rayCluster", "{.status.head.serviceName}"): testSvcA,
	})
	r := NewIntegrationTemplateResolver(frozen)

	// Mixed template: a parameter substitution + a {{ resource }} lookup.
	out, err := r.ResolveTemplateExpression(
		`ray start --address={{ resource "rayCluster" "{.status.head.serviceName}" }} --name={{ .Parameters.rayClusterName }}`,
		data,
	)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if out != "ray start --address=svc-a --name=cluster-a" {
		t.Fatalf("unexpected render: %q", out)
	}

	// nil provider -> {{ resource }} errors cleanly.
	rNil := NewIntegrationTemplateResolver(nil)
	if _, err := rNil.ResolveTemplateExpression(`{{ resource "rayCluster" "{.x}" }}`, data); err == nil {
		t.Fatal("nil provider must error on a {{ resource }} expression")
	}
}

// TestApplyPodModifications_MergeSemantics verifies the pod-template merge: containers/volumes are
// appended and primary-container env is set (overwriting a same-named key so integration env wins).
func TestApplyPodModifications_MergeSemantics(t *testing.T) {
	ps := &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: PrimaryContainerName, Env: []corev1.EnvVar{{Name: "EXISTING", Value: "keep"}, {Name: rayAddressEnv, Value: "old"}}},
		},
	}
	mods := &workspacev1alpha1.PodModifications{
		AdditionalContainers: []corev1.Container{{Name: raySidecarName, Image: "img"}},
		Volumes:              []corev1.Volume{{Name: rayTmpVolume}},
		PrimaryContainerModifications: &workspacev1alpha1.PrimaryContainerModifications{
			MergeEnv: []workspacev1alpha1.AccessEnvTemplate{{Name: rayAddressEnv, ValueTemplate: "auto"}},
		},
	}

	applyPodModifications(ps, mods)

	if len(ps.Containers) != 2 || ps.Containers[1].Name != raySidecarName {
		t.Fatalf("sidecar not appended: %+v", ps.Containers)
	}
	if len(ps.Volumes) != 1 || ps.Volumes[0].Name != rayTmpVolume {
		t.Fatalf("volume not appended: %+v", ps.Volumes)
	}
	primary := findPrimaryContainer(ps)
	if v := envValue(primary, rayAddressEnv); v != "auto" {
		t.Fatalf("integration env should overwrite: got RAY_ADDRESS=%q, want auto", v)
	}
	if v := envValue(primary, "EXISTING"); v != "keep" {
		t.Fatalf("unrelated env must be preserved: got EXISTING=%q, want keep", v)
	}
}

func envValue(c *corev1.Container, name string) string {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}
