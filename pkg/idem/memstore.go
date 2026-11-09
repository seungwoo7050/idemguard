package idem

import (
	"context"
	"sync"
	"time"
)

type memEntry struct {
	record *Record
	done   chan struct{}
	closed bool
}

func (e *memEntry) closeDone() {
	if !e.closed {
		close(e.done)
		e.closed = true
	}
}

// MemoryStore provides a thread-safe in-memory Store with in-flight channel broadcasting.
type MemoryStore struct {
	mu        sync.Mutex
	entries   map[string]*memEntry
	stopCh    chan struct{}
	closeOnce sync.Once
}

// NewMemoryStore initializes MemoryStore and starts background sweeping if reaperInterval > 0.
func NewMemoryStore(reaperInterval time.Duration) *MemoryStore {
	ms := &MemoryStore{
		entries: make(map[string]*memEntry),
		stopCh:  make(chan struct{}),
	}
	if reaperInterval > 0 {
		go ms.reaperLoop(reaperInterval)
	}
	return ms
}

func (m *MemoryStore) reaperLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case now := <-ticker.C:
			m.SweepExpired(now)
		}
	}
}

// TryAcquire atomically attempts to register an IN_FLIGHT record.
func (m *MemoryStore) TryAcquire(ctx context.Context, key, fingerprint string, ttl time.Duration) (AcquireResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	entry, exists := m.entries[key]
	if exists && entry.record.IsExpired(now) {
		entry.closeDone()
		delete(m.entries, key)
		exists = false
	}

	if !exists {
		rec := &Record{
			Key:         key,
			Fingerprint: fingerprint,
			Status:      StatusInFlight,
			CreatedAt:   now,
			ExpiresAt:   now.Add(ttl),
		}
		m.entries[key] = &memEntry{record: rec, done: make(chan struct{})}
		return AcquireResult{Acquired: true, Record: rec.Clone()}, nil
	}

	if entry.record.Fingerprint != fingerprint {
		return AcquireResult{Record: entry.record.Clone()}, ErrFingerprintMismatch
	}

	res := AcquireResult{Record: entry.record.Clone()}
	if entry.record.Status == StatusInFlight {
		res.WaitCh = entry.done
	}
	return res, nil
}

// Complete transitions an IN_FLIGHT record to COMPLETED and unblocks waiters.
func (m *MemoryStore) Complete(ctx context.Context, key string, statusCode int, headers map[string]string, body []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, exists := m.entries[key]
	if !exists {
		return ErrNotFound
	}

	entry.record.Status = StatusCompleted
	entry.record.StatusCode = statusCode
	entry.record.Headers = headers
	entry.record.Body = body
	entry.record.ExpiresAt = time.Now().Add(ttl)
	entry.closeDone()
	return nil
}

// Delete removes a record and unblocks in-flight waiters so followers do not hang.
func (m *MemoryStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, exists := m.entries[key]; exists {
		entry.closeDone()
		delete(m.entries, key)
	}
	return nil
}

// Get retrieves an unexpired record for key.
func (m *MemoryStore) Get(ctx context.Context, key string) (*Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, exists := m.entries[key]
	if !exists || entry.record.IsExpired(time.Now()) {
		return nil, ErrNotFound
	}
	return entry.record.Clone(), nil
}

// SweepExpired removes all expired records and returns the swept count.
func (m *MemoryStore) SweepExpired(now time.Time) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	swept := 0
	for key, entry := range m.entries {
		if entry.record.IsExpired(now) {
			entry.closeDone()
			delete(m.entries, key)
			swept++
		}
	}
	return swept
}

// Close stops the background reaper goroutine.
func (m *MemoryStore) Close() error {
	m.closeOnce.Do(func() { close(m.stopCh) })
	return nil
}
