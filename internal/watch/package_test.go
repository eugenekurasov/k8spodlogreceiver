// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package watch // import "github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/watch"

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
