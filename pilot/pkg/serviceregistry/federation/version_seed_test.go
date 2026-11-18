// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package federation

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/test/util/assert"
)

func TestVersionVector_SeedNoOpIfHigher(t *testing.T) {
	t.Parallel()
	v := newVersionVector()
	v.set("c1", 100)
	v.seed("c1", 50)
	assert.Equal(t, v.get("c1"), uint64(100))
}

func TestVersionVector_SeedRaisesCounter(t *testing.T) {
	t.Parallel()
	v := newVersionVector()
	v.set("c1", 100)
	v.seed("c1", 500)
	assert.Equal(t, v.get("c1"), uint64(500))
	// Subsequent increment must exceed the seeded value.
	next := v.increment("c1")
	if next <= 500 {
		t.Fatalf("expected increment > 500 after seed, got %d", next)
	}
}

// fakeTransport is a minimal Transport for SyncProtocol tests. It records
// broadcasts and exposes a manual BootstrapDone trigger.
type fakeTransport struct {
	handler       Handler
	bootstrapDone chan struct{}
	mu            sync.Mutex
	broadcasts    []*SyncMessage
	started       atomic.Bool
}

func newFakeTransport(h Handler) *fakeTransport {
	return &fakeTransport{handler: h, bootstrapDone: make(chan struct{})}
}

func (f *fakeTransport) Start() error                   { f.started.Store(true); return nil }
func (f *fakeTransport) BootstrapDone() <-chan struct{} { return f.bootstrapDone }
func (f *fakeTransport) Shutdown() error                { return nil }
func (f *fakeTransport) Broadcast(msg *SyncMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.broadcasts = append(f.broadcasts, msg)
	return nil
}

func (f *fakeTransport) snapshot() []*SyncMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*SyncMessage, len(f.broadcasts))
	copy(out, f.broadcasts)
	return out
}

func newTestSyncProtocol(t *testing.T, localID cluster.ID) (*SyncProtocol, *fakeTransport) {
	t.Helper()
	var ft *fakeTransport
	sp, err := NewSyncProtocol(SyncProtocolConfig{
		LocalClusterID:     localID,
		SnapshotInterval:   100 * time.Millisecond,
		Sink:               noopSink{},
		AmbientIndexGetter: func() model.FederationAmbientIndex { return noopAmbientIndex{} },
		NewTransport: func(h Handler) (Transport, error) {
			ft = newFakeTransport(h)
			return ft, nil
		},
		StopCh: make(chan struct{}),
	})
	if err != nil {
		t.Fatalf("NewSyncProtocol: %v", err)
	}
	return sp, ft
}

func TestSyncProtocol_SeedSelfVersionRaisesCounter(t *testing.T) {
	t.Parallel()
	sp, _ := newTestSyncProtocol(t, "local")
	sp.SeedSelfVersion(0) // no-op
	assert.Equal(t, sp.versionVector.get("local"), uint64(0))
	sp.SeedSelfVersion(9999)
	assert.Equal(t, sp.versionVector.get("local"), uint64(9999))
}

func TestSyncProtocol_OutboundWaitsForBootstrap(t *testing.T) {
	t.Parallel()
	sp, ft := newTestSyncProtocol(t, "local")
	sp.leaderStopCh = make(chan struct{})
	sp.isLeader.Store(true)

	done := make(chan struct{})
	go func() {
		sp.processOutgoingDebounced()
		close(done)
	}()

	// Without bootstrap done, outbound loop must be parked — no broadcasts.
	time.Sleep(50 * time.Millisecond)
	if got := len(ft.snapshot()); got != 0 {
		t.Fatalf("unexpected broadcasts before bootstrap: %d", got)
	}

	// Seed the self-version, then close bootstrap. The first increment after
	// release must reflect the seed.
	sp.SeedSelfVersion(7777)
	close(ft.bootstrapDone)

	// Stop the leader to exit the goroutine — we don't need it to broadcast,
	// only to verify it was parked on bootstrap.
	close(sp.leaderStopCh)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processOutgoingDebounced did not exit after leaderStop")
	}

	// Confirm the seed survived.
	if got := sp.versionVector.get("local"); got < 7777 {
		t.Fatalf("seed lost: got %d, want >= 7777", got)
	}
}

func TestSyncProtocol_BootstrapEmpty_StillProgresses(t *testing.T) {
	t.Parallel()
	sp, ft := newTestSyncProtocol(t, "local")
	sp.leaderStopCh = make(chan struct{})
	sp.isLeader.Store(true)

	// Bootstrap closes immediately with no self-version seed (empty bootstrap).
	close(ft.bootstrapDone)

	done := make(chan struct{})
	go func() {
		sp.processOutgoingDebounced()
		close(done)
	}()

	close(sp.leaderStopCh)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processOutgoingDebounced did not exit after empty-bootstrap release")
	}
}
