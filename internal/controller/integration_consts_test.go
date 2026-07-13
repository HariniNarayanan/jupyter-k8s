/*
Copyright (c) Amazon Web Services
Distributed under the terms of the MIT license
*/

package controller

// Repeated integration test literals, extracted to satisfy goconst. These are shared across the
// integration test files in this package (deployment/resource-manager/prober). Kind and the
// rayClusterName parameter key already have consts in integration_template_resolver_test.go
// (rayClusterKind, rayClusterNameKey); the default namespace reuses testNamespace.
const (
	rayIntegrationName     = "ray-integration"
	raySidecarName         = "ray-sidecar"
	rayTmpVolume           = "ray-tmp"
	rayClusterNamespaceKey = "rayClusterNamespace"
	rayGroup               = "ray.io"
	rayAPIVersion          = "ray.io/v1"
	rayAddressEnv          = "RAY_ADDRESS"
	rayClusterNameExpr     = "{{ .Parameters.rayClusterName }}"
	testSvcA               = "svc-a"
	testClusterB           = "cluster-b"
	testReasonReady        = "Ready"
	testReadyMessage       = "ready"
)
