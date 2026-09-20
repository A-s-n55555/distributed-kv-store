package store

import (
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

// Snapshot returns an independent copy of all retained key versions.
// It includes tombstones and legacy unversioned records.
func (m *Map) Snapshot() map[int64][]Record {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := make(map[int64][]Record, len(m.data))

	for key, records := range m.data {
		copied := make([]Record, len(records))

		for i, record := range records {
			copied[i] = record

			if record.Clock != nil {
				copied[i].Clock = version.Clone(record.Clock)
			}
		}

		snapshot[key] = copied
	}

	return snapshot
}
