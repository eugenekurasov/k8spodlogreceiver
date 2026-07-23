package logline

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
)

type Meta struct {
	Namespace     string
	PodName       string
	PodUID        string
	ContainerName string
}

type Batch struct {
	logs    plog.Logs
	records plog.LogRecordSlice
	count   int
}

func NewBatch(m Meta) *Batch {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()

	res := rl.Resource()
	res.Attributes().PutStr("k8s.namespace.name", m.Namespace)
	res.Attributes().PutStr("k8s.pod.name", m.PodName)
	res.Attributes().PutStr("k8s.pod.uid", m.PodUID)
	res.Attributes().PutStr("k8s.container.name", m.ContainerName)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(metadata.ScopeName)

	return &Batch{logs: logs, records: sl.LogRecords()}
}

func (b *Batch) Append(body string, ts time.Time) {
	lr := b.records.AppendEmpty()
	if ts.IsZero() {
		ts = time.Now()
	}
	lr.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	lr.Body().SetStr(body)
	b.count++
}

func (b *Batch) Count() int { return b.count }

func (b *Batch) Logs() plog.Logs { return b.logs }
