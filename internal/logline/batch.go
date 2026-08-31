// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package logline

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/eugenekurasov/k8spodlogreceiver/internal/metadata"
)

type Meta struct {
	Namespace     string
	PodName       string
	PodUID        string
	ContainerName string
	NodeName      string
	RestartCount  int32
}

type Batch struct {
	logs    plog.Logs
	records plog.LogRecordSlice
	count   int
}

func NewBatch(m Meta) *Batch {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()

	// Attribute names come from the semantic conventions rather than string
	// literals, so a typo cannot ship and the version they follow is a single
	// import to bump. rl.SchemaUrl states that version to whatever reads these
	// records — that is what makes a schema processor able to translate them.
	rl.SetSchemaUrl(semconv.SchemaURL)
	attrs := rl.Resource().Attributes()
	attrs.PutStr(string(semconv.K8SNamespaceNameKey), m.Namespace)
	attrs.PutStr(string(semconv.K8SPodNameKey), m.PodName)
	attrs.PutStr(string(semconv.K8SPodUIDKey), m.PodUID)
	attrs.PutStr(string(semconv.K8SContainerNameKey), m.ContainerName)
	attrs.PutStr(string(semconv.K8SNodeNameKey), m.NodeName)
	attrs.PutInt(string(semconv.K8SContainerRestartCountKey), int64(m.RestartCount))

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(metadata.ScopeName)

	return &Batch{logs: logs, records: sl.LogRecords()}
}

func (b *Batch) Append(body string, ts time.Time) {
	lr := b.records.AppendEmpty()
	now := time.Now()
	if ts.IsZero() {
		ts = now
	}
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(now))
	lr.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	lr.Body().SetStr(body)
	b.count++
}

func (b *Batch) Count() int { return b.count }

func (b *Batch) Logs() plog.Logs { return b.logs }
