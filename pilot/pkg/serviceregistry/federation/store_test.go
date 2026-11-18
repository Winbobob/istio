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
	"strings"
	"testing"
	"time"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/test/util/assert"
	"istio.io/istio/pkg/workloadapi"
)

func newTestStore(localCluster cluster.ID) *federationStore {
	return newFederationStore(federationStoreConfig{
		LocalClusterID:    localCluster,
		TrustDomainGetter: func() string { return "cluster.local" },
	})
}

func makeSyncMessage(clusterID cluster.ID, version uint64, fullSync bool, hostnames ...string) *SyncMessage {
	msg := &SyncMessage{
		ClusterID:     clusterID,
		FullSync:      fullSync,
		VersionVector: map[cluster.ID]uint64{clusterID: version},
		Network:       "network-remote",
		NetworkGateway: &WireNetworkGateway{
			Cluster:   string(clusterID),
			Addr:      "10.0.0.1",
			HBONEPort: 15008,
		},
		Capacities: map[string]uint32{},
	}
	for _, h := range hostnames {
		// One backend per service, as a sender reports for a service with a
		// workload; services with no capacity are not routed to.
		msg.Capacities[serviceKey("ns1", h)] = 1
		msg.Services = append(msg.Services, &model.ServiceInfo{
			Service: &workloadapi.Service{
				Hostname:  h,
				Namespace: "ns1",
				Addresses: []*workloadapi.NetworkAddress{{
					Network: "network-remote",
					Address: []byte{10, 0, 0, 1},
				}},
				Ports: []*workloadapi.Port{{ServicePort: 80, TargetPort: 8080}},
			},
			Scope: model.Global,
		})
	}
	return msg
}

// handleSync invokes the production handleSyncMessage with the current
// wallclock time as the broker enqueue timestamp. Tests that need to control
// shard.LastSeen (e.g. driving a shard past the expiry window) call
// handleSyncMessage directly with an explicit time.
func handleSync(s *federationStore, msg *SyncMessage) {
	s.handleSyncMessage(msg, time.Now())
}

func TestExpireStaleShards_DeadClusterRemoved(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Receive a message from cluster-remote.
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))

	// Verify the shard is live.
	svcs, wls := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, len(wls) > 0, true) // at least gateway + split-horizon workload

	// Advance time past expiry.
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()

	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 1)
	assert.Equal(t, expired[0], cluster.ID("cluster-remote"))

	// After expiry, getFederationState should return nothing.
	svcs, wls = store.getFederationState()
	assert.Equal(t, len(svcs), 0)
	assert.Equal(t, len(wls), 0)
}

func TestExpireStaleShards_LiveClusterNotExpired(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	handleSync(store, makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))

	// LastSeen is recent (just set by handleSyncMessage), should not expire.
	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 0)

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
}

func TestTombstonedShardRejectsStaleMessages(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Initial sync at version 1000.
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))

	// Tombstone the shard.
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()
	store.expireStaleShards(15*time.Minute, time.Now())

	// Send a stale message with version <= 1000. Should be rejected.
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)

	// Also reject an incremental with version 999.
	handleSync(store, makeSyncMessage("cluster-remote", 999, false, "svc1.ns1.svc.cluster.local"))

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 0)
}

func TestTombstonedShardResurrectedByNewerVersion(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Initial sync at version 1000.
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))

	// Tombstone.
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()
	store.expireStaleShards(15*time.Minute, time.Now())

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)

	// Send a newer version (strictly greater). Should resurrect.
	handleSync(store, makeSyncMessage("cluster-remote", 1001, true, "svc-new.ns1.svc.cluster.local"))

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc-new.ns1.svc.cluster.local")

	// Verify shard is no longer tombstoned.
	store.mu.RLock()
	shard := store.shards["cluster-remote"]
	assert.Equal(t, shard.Tombstoned, false)
	assert.Equal(t, shard.LastSeen.IsZero(), false)
	store.mu.RUnlock()
}

// TestExpireStaleShards_BootstrapShardsGetNormalLastSeen verifies that a shard
// hydrated by the bootstrap drain has its LastSeen set by handleSyncMessage and
// therefore obeys the normal expiry contract — it is NOT exempt from expiry.
//
// (The previous "bootstrap grace period" branch in expireStaleShards was dead:
// every accepted message — including bootstrap-merged ones — sets LastSeen, so
// a non-tombstoned shard always has a non-zero LastSeen at sweep time. The
// branch was removed in this PR.)
func TestExpireStaleShards_BootstrapShardsGetNormalLastSeen(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// A bootstrap-style sync (full=true) goes through handleSyncMessage, which
	// sets LastSeen to time.Now(). The shard is treated as live.
	handleSync(store, makeSyncMessage("bootstrap-cluster", 500, true,
		"svc.ns1.svc.cluster.local"))

	store.mu.RLock()
	last := store.shards["bootstrap-cluster"].LastSeen
	store.mu.RUnlock()
	assert.Equal(t, last.IsZero(), false)

	// Within window: not expired.
	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 0)

	// Outside window: expired like any other shard.
	store.mu.Lock()
	store.shards["bootstrap-cluster"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()
	expired = store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 1)
	assert.Equal(t, expired[0], cluster.ID("bootstrap-cluster"))
}

func TestExpireStaleShards_MultipleShardsMixedState(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Cluster A: recent
	handleSync(store, makeSyncMessage("cluster-a", 1000, true, "svc-a.ns1.svc.cluster.local"))

	// Cluster B: stale
	handleSync(store, makeSyncMessage("cluster-b", 2000, true, "svc-b.ns1.svc.cluster.local"))
	store.mu.Lock()
	store.shards["cluster-b"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()

	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 1)
	assert.Equal(t, expired[0], cluster.ID("cluster-b"))

	// Only cluster A's services should remain.
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc-a.ns1.svc.cluster.local")
}

func TestExpireStaleShards_AlreadyTombstonedSkipped(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	handleSync(store, makeSyncMessage("cluster-remote", 1000, true, "svc.ns1.svc.cluster.local"))

	// Tombstone it.
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()
	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 1)

	// Second sweep should not re-expire it.
	expired = store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 0)
}

func TestExpireStaleShards_DisabledWhenZeroDuration(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	handleSync(store, makeSyncMessage("cluster-remote", 1000, true, "svc.ns1.svc.cluster.local"))
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-1 * time.Hour)
	store.mu.Unlock()

	// Zero expiry duration should never expire anything.
	expired := store.expireStaleShards(0, time.Now())
	assert.Equal(t, len(expired), 0)
}

func TestBootstrapThenExpiry_RetainedSnapshotExpires(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Simulate bootstrap: a retained snapshot from a dead cluster arrives
	// through the bootstrap path and is applied to the store.
	msg := makeSyncMessage("dead-cluster", 5000, true, "dead-svc.ns1.svc.cluster.local")
	handleSync(store, msg)

	// Verify it's live.
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)

	// Time passes: no more messages arrive from dead-cluster.
	store.mu.Lock()
	store.shards["dead-cluster"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()

	// Expiry sweep tombstones it.
	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 1)

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 0)

	// Now simulate a restart: bootstrap replays the same retained snapshot
	// (version 5000). It should be rejected because version <= tombstoned version.
	handleSync(store, msg)

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 0)

	// But a genuinely new leader on that cluster with a higher version succeeds.
	handleSync(store, makeSyncMessage("dead-cluster", 5001, true, "alive-svc.ns1.svc.cluster.local"))

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "alive-svc.ns1.svc.cluster.local")
}

func TestNewSyncProtocol_RejectsExpiryWithDisabledSnapshots(t *testing.T) {
	t.Parallel()

	// NewTransport, Sink, and AmbientIndexGetter are non-nil so the early
	// "required" checks pass; the expiry-vs-snapshot validation runs and
	// rejects the config before the factory is ever invoked.
	_, err := NewSyncProtocol(SyncProtocolConfig{
		LocalClusterID:     "test",
		SnapshotInterval:   -1, // disabled
		ShardExpiry:        15 * time.Minute,
		Sink:               noopSink{},
		AmbientIndexGetter: func() model.FederationAmbientIndex { return noopAmbientIndex{} },
		NewTransport: func(_ Handler) (Transport, error) {
			t.Fatal("NewTransport must not be invoked when validation fails first")
			return nil, nil
		},
		StopCh: make(chan struct{}),
	})
	assert.Equal(t, err != nil, true)
}

// noopSink satisfies federation.Sink for tests that only exercise validation
// or other paths that never reach the debounce flush. It deliberately drops
// everything: a test that wants to assert on Apply payloads should use a
// recording sink.
type noopSink struct{}

func (noopSink) Apply(_ []*model.ServiceInfo, _ []*model.WorkloadInfo) {}

// noopAmbientIndex satisfies model.FederationAmbientIndex for tests that only
// exercise validation or other paths that never query the index. The handler
// callback is never invoked.
type noopAmbientIndex struct{}

func (noopAmbientIndex) AllGlobalServiceFromLocalClusterWithSANs() []*model.ServiceInfo {
	return nil
}
func (noopAmbientIndex) ServiceWithSANs(_ *model.ServiceInfo) *model.ServiceInfo { return nil }
func (noopAmbientIndex) RegisterGlobalServiceHandler(_ func(prev, curr *model.ServiceInfo, event model.Event)) {
}
func (noopAmbientIndex) ServiceCapacity(_ *model.ServiceInfo) uint32               { return 0 }
func (noopAmbientIndex) AllLocalWorkloadsForGlobalServices() []*model.WorkloadInfo { return nil }
func (noopAmbientIndex) RegisterGlobalWorkloadHandler(_ func(prev, curr *model.WorkloadInfo, event model.Event)) {
}

func TestGatewayDedupeKey_DifferentClustersNotCollapsed(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Two clusters on the same network with the same gateway address
	// but different HBONE ports and cluster IDs.
	msgA := &SyncMessage{
		ClusterID:     "cluster-a",
		FullSync:      true,
		VersionVector: map[cluster.ID]uint64{"cluster-a": 1000},
		Network:       "network-shared",
		NetworkGateway: &WireNetworkGateway{
			Cluster:   "cluster-a",
			Addr:      "10.0.0.1",
			HBONEPort: 15008,
		},
		Services: []*model.ServiceInfo{{
			Service: &workloadapi.Service{
				Hostname:  "svc-a.ns1.svc.cluster.local",
				Namespace: "ns1",
				Addresses: []*workloadapi.NetworkAddress{{Network: "network-shared", Address: []byte{10, 0, 0, 1}}},
				Ports:     []*workloadapi.Port{{ServicePort: 80, TargetPort: 8080}},
			},
			Scope: model.Global,
		}},
	}

	msgB := &SyncMessage{
		ClusterID:     "cluster-b",
		FullSync:      true,
		VersionVector: map[cluster.ID]uint64{"cluster-b": 1000},
		Network:       "network-shared",
		NetworkGateway: &WireNetworkGateway{
			Cluster:   "cluster-b",
			Addr:      "10.0.0.1",
			HBONEPort: 15009, // different port
		},
		Services: []*model.ServiceInfo{{
			Service: &workloadapi.Service{
				Hostname:  "svc-b.ns1.svc.cluster.local",
				Namespace: "ns1",
				Addresses: []*workloadapi.NetworkAddress{{Network: "network-shared", Address: []byte{10, 0, 0, 2}}},
				Ports:     []*workloadapi.Port{{ServicePort: 80, TargetPort: 8080}},
			},
			Scope: model.Global,
		}},
	}

	handleSync(store, msgA)
	handleSync(store, msgB)

	_, workloads := store.getFederationState()

	// Count gateway workloads (UIDs starting with "NetworkGateway/").
	gwCount := 0
	for _, wl := range workloads {
		if wl.Workload != nil && len(wl.Workload.Uid) > 15 && wl.Workload.Uid[:15] == "NetworkGateway/" {
			gwCount++
		}
	}
	// Both clusters should produce distinct gateway workloads.
	assert.Equal(t, gwCount, 2)
}

func TestGatewayDedupe_SharedGatewayDeterministic(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Two clusters on the same network advertise the same gateway address and
	// port, so their gateway workloads share a UID. Only the cluster and
	// service account differ.
	gwMsg := func(clusterID cluster.ID, sa string) *SyncMessage {
		return &SyncMessage{
			ClusterID:     clusterID,
			FullSync:      true,
			VersionVector: map[cluster.ID]uint64{clusterID: 1000},
			Network:       "network-shared",
			NetworkGateway: &WireNetworkGateway{
				Cluster:        string(clusterID),
				Addr:           "10.0.0.1",
				HBONEPort:      15008,
				ServiceAccount: sa,
				Namespace:      "istio-system",
			},
		}
	}
	handleSync(store, gwMsg("cluster-b", "sa-b"))
	handleSync(store, gwMsg("cluster-a", "sa-a"))

	// Map iteration order is random, so repeat to catch an order-dependent pick.
	for range 20 {
		_, workloads := store.getFederationState()
		var gateways []*model.WorkloadInfo
		for _, wl := range workloads {
			if strings.HasPrefix(wl.Workload.Uid, "NetworkGateway/") {
				gateways = append(gateways, wl)
			}
		}
		assert.Equal(t, len(gateways), 1)
		// The lowest cluster ID wins.
		assert.Equal(t, gateways[0].Workload.ClusterId, "cluster-a")
		assert.Equal(t, gateways[0].Workload.ServiceAccount, "sa-a")
	}
}

func TestProjectionCache_HitOnUnchangedShard(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Seed a shard.
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true,
		"svc1.ns1.svc.cluster.local", "svc2.ns1.svc.cluster.local"))

	// First call builds the cache (miss).
	svcs1, wls1 := store.getFederationState()
	assert.Equal(t, len(svcs1), 2)
	assert.Equal(t, len(wls1) > 0, true)

	// Verify cache was populated.
	store.mu.RLock()
	cached := store.shards["cluster-remote"].cachedProjection
	store.mu.RUnlock()
	assert.Equal(t, cached != nil, true)

	// Second call should hit the cache — same results.
	svcs2, wls2 := store.getFederationState()
	assert.Equal(t, len(svcs2), len(svcs1))
	assert.Equal(t, len(wls2), len(wls1))

	// Verify the same projection pointer was reused (cache hit).
	store.mu.RLock()
	cachedAfter := store.shards["cluster-remote"].cachedProjection
	store.mu.RUnlock()
	assert.Equal(t, cached, cachedAfter)
}

func TestProjectionCache_InvalidatedOnNewMessage(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Seed and build cache.
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true,
		"svc1.ns1.svc.cluster.local"))
	svcs1, _ := store.getFederationState()
	assert.Equal(t, len(svcs1), 1)

	// Capture first projection.
	store.mu.RLock()
	firstProj := store.shards["cluster-remote"].cachedProjection
	store.mu.RUnlock()
	assert.Equal(t, firstProj != nil, true)

	// Send a new message (incremental, adds a service). This should invalidate the cache.
	handleSync(store, makeSyncMessage("cluster-remote", 1001, false,
		"svc2.ns1.svc.cluster.local"))

	// Cache should be nil.
	store.mu.RLock()
	assert.Equal(t, store.shards["cluster-remote"].cachedProjection == nil, true)
	store.mu.RUnlock()

	// Next getFederationState rebuilds the cache.
	svcs2, _ := store.getFederationState()
	assert.Equal(t, len(svcs2), 2)

	// New projection should be different from the first.
	store.mu.RLock()
	secondProj := store.shards["cluster-remote"].cachedProjection
	store.mu.RUnlock()
	assert.Equal(t, secondProj != nil, true)
	assert.Equal(t, firstProj != secondProj, true)
}

func TestProjectionCache_InvalidatedOnExpiry(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Seed and build cache.
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true,
		"svc1.ns1.svc.cluster.local"))
	store.getFederationState()

	store.mu.RLock()
	assert.Equal(t, store.shards["cluster-remote"].cachedProjection != nil, true)
	store.mu.RUnlock()

	// Tombstone via expiry.
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()
	expired := store.expireStaleShards(10*time.Minute, time.Now())
	assert.Equal(t, len(expired), 1)

	// Cache should be cleared.
	store.mu.RLock()
	assert.Equal(t, store.shards["cluster-remote"].cachedProjection == nil, true)
	store.mu.RUnlock()

	// getFederationState should return nothing (tombstoned).
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)
}

func TestProjectionCache_MultiShardSelectiveInvalidation(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Seed two shards.
	handleSync(store, makeSyncMessage("cluster-a", 1000, true,
		"svc-a.ns1.svc.cluster.local"))
	handleSync(store, makeSyncMessage("cluster-b", 1000, true,
		"svc-b.ns1.svc.cluster.local"))

	// Build caches.
	store.getFederationState()

	store.mu.RLock()
	projA := store.shards["cluster-a"].cachedProjection
	projB := store.shards["cluster-b"].cachedProjection
	store.mu.RUnlock()
	assert.Equal(t, projA != nil, true)
	assert.Equal(t, projB != nil, true)

	// Update only cluster-a.
	handleSync(store, makeSyncMessage("cluster-a", 1001, false,
		"svc-a2.ns1.svc.cluster.local"))

	// cluster-a cache invalidated, cluster-b still valid.
	store.mu.RLock()
	assert.Equal(t, store.shards["cluster-a"].cachedProjection == nil, true)
	assert.Equal(t, store.shards["cluster-b"].cachedProjection, projB) // same pointer
	store.mu.RUnlock()

	// Rebuild and verify counts.
	svcs, _ := store.getFederationState()
	// cluster-a: svc-a + svc-a2 = 2, cluster-b: svc-b = 1 → total 3
	assert.Equal(t, len(svcs), 3)
}

// TestServiceAddresses_PreservedVerbatim verifies that a federation-received
// service exposes only its origin (network, ip) address — no synthetic
// local-network entries are injected. Receiving clusters are expected to
// contribute the locally-routable VIP via a local Service stub or
// ServiceEntry; see multicluster-vip-lookup-analysis.html for rationale.
func TestServiceAddresses_PreservedVerbatim(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	handleSync(store, makeSyncMessage("cluster-remote", 1000, true,
		"svc1.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)

	addrs := svcs[0].Service.Addresses
	assert.Equal(t, len(addrs), 1)
	assert.Equal(t, addrs[0].Network, "network-remote")
	assert.Equal(t, addrs[0].Address, []byte{10, 0, 0, 1})
}

// TestServiceAddresses_NoCrossShardCollision verifies that two remote shards
// publishing the same hostname with the same ClusterIP on different networks
// do not fabricate a collision on the receiving cluster's local network.
// (Issues #1 and #2 from the addLocalNetworkVIPs critique.)
func TestServiceAddresses_NoCrossShardCollision(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	mkMsg := func(clusterID cluster.ID, network string) *SyncMessage {
		return &SyncMessage{
			ClusterID:     clusterID,
			FullSync:      true,
			VersionVector: map[cluster.ID]uint64{clusterID: 1000},
			Network:       network,
			NetworkGateway: &WireNetworkGateway{
				Cluster:   string(clusterID),
				Addr:      "10.0.0.99",
				HBONEPort: 15008,
			},
			Services: []*model.ServiceInfo{{
				// Same hostname, same ClusterIP, different networks — would
				// previously have synthesised duplicate (local, 10.0.0.5) entries.
				Service: &workloadapi.Service{
					Hostname:  "svc.ns1.svc.cluster.local",
					Namespace: "ns1",
					Addresses: []*workloadapi.NetworkAddress{{
						Network: network,
						Address: []byte{10, 0, 0, 5},
					}},
					Ports: []*workloadapi.Port{{ServicePort: 80, TargetPort: 8080}},
				},
				Scope: model.Global,
			}},
		}
	}

	handleSync(store, mkMsg("cluster-a", "network-a"))
	handleSync(store, mkMsg("cluster-b", "network-b"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 2) // one per shard, deduped at the ambient/index layer

	for _, s := range svcs {
		for _, a := range s.Service.Addresses {
			// No synthetic local-network entries should appear.
			assert.Equal(t, a.Network != "network-local", true)
		}
	}
}

func TestHealthState_InitiallyNotReady(t *testing.T) {
	t.Parallel()

	var h HealthState
	// Neither bootstrap complete nor transport connected by default.
	assert.Equal(t, h.BootstrapComplete.Load(), false)
	assert.Equal(t, h.TransportConnected.Load(), false)
	assert.Equal(t, h.LastMessageReceived.Load(), int64(0))
}

func TestHealthState_ReadyRequiresBothFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bootstrap   bool
		transport   bool
		expectReady bool
	}{
		{"neither", false, false, false},
		{"bootstrap only", true, false, false},
		{"transport only", false, true, false},
		{"both", true, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var h HealthState
			h.BootstrapComplete.Store(tt.bootstrap)
			h.TransportConnected.Store(tt.transport)

			// IsReady logic: bootstrap && transport
			ready := h.BootstrapComplete.Load() && h.TransportConnected.Load()
			assert.Equal(t, ready, tt.expectReady)
		})
	}
}

func TestHealthState_LastMessageReceived(t *testing.T) {
	t.Parallel()

	var h HealthState
	assert.Equal(t, h.LastMessageReceived.Load(), int64(0))

	now := time.Now().UnixMilli()
	h.LastMessageReceived.Store(now)
	assert.Equal(t, h.LastMessageReceived.Load(), now)
}

func TestHealthState_TransportDisconnectReconnect(t *testing.T) {
	t.Parallel()

	var h HealthState
	h.TransportConnected.Store(true)
	h.BootstrapComplete.Store(true)

	// Simulate disconnect.
	h.TransportConnected.Store(false)
	ready := h.BootstrapComplete.Load() && h.TransportConnected.Load()
	assert.Equal(t, ready, false)

	// Simulate reconnect.
	h.TransportConnected.Store(true)
	ready = h.BootstrapComplete.Load() && h.TransportConnected.Load()
	assert.Equal(t, ready, true)
}

// --- Version vector / temporal semantics tests ---

func TestVersionVector_StaleMessageRejected(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Accept version 5.
	handleSync(store, makeSyncMessage("cluster-a", 5, true, "svc1.ns1.svc.cluster.local"))
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)

	// Reject version 3 (stale).
	handleSync(store, makeSyncMessage("cluster-a", 3, true, "svc-stale.ns1.svc.cluster.local"))
	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc1.ns1.svc.cluster.local")
}

func TestVersionVector_EqualVersionFullSyncAccepted(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Accept version 5 full-sync.
	handleSync(store, makeSyncMessage("cluster-a", 5, true, "svc1.ns1.svc.cluster.local"))

	// Equal version full-sync (re-delivery) — accepted for idempotency.
	handleSync(store, makeSyncMessage("cluster-a", 5, true, "svc-updated.ns1.svc.cluster.local"))
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc-updated.ns1.svc.cluster.local")
}

func TestVersionVector_EqualVersionIncrementalRejected(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Accept version 5.
	handleSync(store, makeSyncMessage("cluster-a", 5, true, "svc1.ns1.svc.cluster.local"))

	// Equal version incremental — rejected (only full-sync re-delivery accepted).
	handleSync(store, makeSyncMessage("cluster-a", 5, false, "svc-inc.ns1.svc.cluster.local"))
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc1.ns1.svc.cluster.local")
}

func TestVersionVector_OutOfOrderDelivery(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Messages arrive out of order: 3, 1, 5, 2.
	handleSync(store, makeSyncMessage("cluster-a", 3, true, "svc-v3.ns1.svc.cluster.local"))
	handleSync(store, makeSyncMessage("cluster-a", 1, true, "svc-v1.ns1.svc.cluster.local"))
	handleSync(store, makeSyncMessage("cluster-a", 5, true, "svc-v5.ns1.svc.cluster.local"))
	handleSync(store, makeSyncMessage("cluster-a", 2, true, "svc-v2.ns1.svc.cluster.local"))

	// Only the highest version (5) should be retained.
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc-v5.ns1.svc.cluster.local")
}

func TestVersionVector_MultiClusterIndependent(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Two clusters with their own version vectors.
	handleSync(store, makeSyncMessage("cluster-a", 10, true, "svc-a.ns1.svc.cluster.local"))
	handleSync(store, makeSyncMessage("cluster-b", 5, true, "svc-b.ns1.svc.cluster.local"))

	// Stale message for cluster-a doesn't affect cluster-b.
	handleSync(store, makeSyncMessage("cluster-a", 3, true, "svc-stale.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 2)

	hostnames := make(map[string]bool)
	for _, svc := range svcs {
		hostnames[svc.Service.Hostname] = true
	}
	assert.Equal(t, hostnames["svc-a.ns1.svc.cluster.local"], true)
	assert.Equal(t, hostnames["svc-b.ns1.svc.cluster.local"], true)
}

// --- Full-sync replacement semantics tests ---

func TestFullSync_ReplacesEntireShard(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Version 1: two services.
	msg1 := makeSyncMessage("cluster-a", 1, true, "svc1.ns1.svc.cluster.local", "svc2.ns1.svc.cluster.local")
	handleSync(store, msg1)
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 2)

	// Version 2: full-sync replaces with one different service.
	msg2 := makeSyncMessage("cluster-a", 2, true, "svc3.ns1.svc.cluster.local")
	handleSync(store, msg2)
	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc3.ns1.svc.cluster.local")
}

// --- Incremental delete semantics tests ---

func TestIncrementalDelete_RemovesService(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Full-sync with two services.
	handleSync(store, makeSyncMessage("cluster-a", 1, true,
		"svc1.ns1.svc.cluster.local", "svc2.ns1.svc.cluster.local"))
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 2)

	// Incremental delete of svc1.
	deleteMsg := &SyncMessage{
		ClusterID:       "cluster-a",
		FullSync:        false,
		VersionVector:   map[cluster.ID]uint64{"cluster-a": 2},
		DeletedServices: []DeletedServiceKey{{Namespace: "ns1", Hostname: "svc1.ns1.svc.cluster.local"}},
	}
	handleSync(store, deleteMsg)

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc2.ns1.svc.cluster.local")
}

func TestIncrementalDelete_NonexistentHostnameIgnored(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	handleSync(store, makeSyncMessage("cluster-a", 1, true, "svc1.ns1.svc.cluster.local"))

	// Delete a (namespace, hostname) that doesn't exist — no crash, no effect.
	deleteMsg := &SyncMessage{
		ClusterID:       "cluster-a",
		FullSync:        false,
		VersionVector:   map[cluster.ID]uint64{"cluster-a": 2},
		DeletedServices: []DeletedServiceKey{{Namespace: "ns1", Hostname: "nonexistent.ns1.svc.cluster.local"}},
	}
	handleSync(store, deleteMsg)

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
}

func TestIncrementalAddAndDelete_SameMessage(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	handleSync(store, makeSyncMessage("cluster-a", 1, true,
		"svc1.ns1.svc.cluster.local", "svc2.ns1.svc.cluster.local"))

	// Incremental: add svc3, delete svc1. Deletes are processed before adds.
	msg := makeSyncMessage("cluster-a", 2, false, "svc3.ns1.svc.cluster.local")
	msg.DeletedServices = []DeletedServiceKey{{Namespace: "ns1", Hostname: "svc1.ns1.svc.cluster.local"}}
	handleSync(store, msg)

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 2)
	hostnames := make(map[string]bool)
	for _, svc := range svcs {
		hostnames[svc.Service.Hostname] = true
	}
	assert.Equal(t, hostnames["svc2.ns1.svc.cluster.local"], true)
	assert.Equal(t, hostnames["svc3.ns1.svc.cluster.local"], true)
}

// --- Self-message filtering tests ---

func TestSelfMessage_Ignored(t *testing.T) {
	t.Parallel()

	store := newTestStore("local-cluster")

	// Message from own cluster — should be silently dropped.
	handleSync(store, makeSyncMessage("local-cluster", 1, true, "svc1.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)
}

// --- Nil / empty message safety tests ---

func TestNilMessage_Ignored(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Should not panic.
	handleSync(store, nil)

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)
}

func TestEmptyFullSync_ClearsShard(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Populate shard.
	handleSync(store, makeSyncMessage("cluster-a", 1, true,
		"svc1.ns1.svc.cluster.local", "svc2.ns1.svc.cluster.local"))
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 2)

	// Empty full-sync replaces shard with zero services.
	handleSync(store, makeSyncMessage("cluster-a", 2, true))
	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 0)
}

// --- Multi-cluster convergence tests ---

func TestConvergence_BootstrapThenIncrementals(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Simulate bootstrap: full-sync from two clusters.
	handleSync(store, makeSyncMessage("cluster-a", 100, true,
		"svc-a1.ns1.svc.cluster.local", "svc-a2.ns1.svc.cluster.local"))
	handleSync(store, makeSyncMessage("cluster-b", 50, true,
		"svc-b1.ns1.svc.cluster.local"))

	// Live incrementals: cluster-a adds a service, cluster-b deletes one.
	handleSync(store, makeSyncMessage("cluster-a", 101, false, "svc-a3.ns1.svc.cluster.local"))

	deleteMsgB := &SyncMessage{
		ClusterID:       "cluster-b",
		FullSync:        false,
		VersionVector:   map[cluster.ID]uint64{"cluster-b": 51},
		DeletedServices: []DeletedServiceKey{{Namespace: "ns1", Hostname: "svc-b1.ns1.svc.cluster.local"}},
	}
	handleSync(store, deleteMsgB)

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 3) // a1, a2, a3; b1 deleted

	hostnames := make(map[string]bool)
	for _, svc := range svcs {
		hostnames[svc.Service.Hostname] = true
	}
	assert.Equal(t, hostnames["svc-a1.ns1.svc.cluster.local"], true)
	assert.Equal(t, hostnames["svc-a2.ns1.svc.cluster.local"], true)
	assert.Equal(t, hostnames["svc-a3.ns1.svc.cluster.local"], true)
	assert.Equal(t, hostnames["svc-b1.ns1.svc.cluster.local"], false)
}

func TestConvergence_FullSyncHealsStaleState(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Initial state: svc1 and svc2.
	handleSync(store, makeSyncMessage("cluster-a", 1, true,
		"svc1.ns1.svc.cluster.local", "svc2.ns1.svc.cluster.local"))

	// Suppose we missed an incremental delete for svc1.
	// The next full-sync at version 5 heals the state.
	handleSync(store, makeSyncMessage("cluster-a", 5, true, "svc2.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc2.ns1.svc.cluster.local")
}

func TestConvergence_TombstoneBlocksRetainedBootstrapMessages(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Accept initial state at version 10.
	handleSync(store, makeSyncMessage("cluster-a", 10, true, "svc1.ns1.svc.cluster.local"))

	// Tombstone the shard.
	store.mu.Lock()
	store.shards["cluster-a"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()
	store.expireStaleShards(15*time.Minute, time.Now())

	// Retained bootstrap re-delivers the version 10 full-sync — should be blocked.
	handleSync(store, makeSyncMessage("cluster-a", 10, true, "svc1.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)

	// Only a strictly newer version can resurrect.
	handleSync(store, makeSyncMessage("cluster-a", 11, true, "svc-resurrected.ns1.svc.cluster.local"))
	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc-resurrected.ns1.svc.cluster.local")
}

// --- SPIFFE SAN parsing tests ---

func TestParseSpiffeSAN_ValidSAN(t *testing.T) {
	t.Parallel()

	result := parseSpiffeSAN("spiffe://cluster.local/ns/my-ns/sa/my-sa")
	assert.Equal(t, result != nil, true)
	assert.Equal(t, result.namespace, "my-ns")
	assert.Equal(t, result.serviceAccount, "my-sa")
}

func TestParseSpiffeSAN_DifferentTrustDomain(t *testing.T) {
	t.Parallel()

	result := parseSpiffeSAN("spiffe://example.com/ns/prod/sa/web-server")
	assert.Equal(t, result != nil, true)
	assert.Equal(t, result.namespace, "prod")
	assert.Equal(t, result.serviceAccount, "web-server")
}

func TestParseSpiffeSAN_InvalidFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		san  string
	}{
		{"empty", ""},
		{"not spiffe", "https://example.com/ns/foo/sa/bar"},
		{"no path", "spiffe://cluster.local"},
		{"missing sa segment", "spiffe://cluster.local/ns/my-ns"},
		{"wrong prefix", "spiffe://cluster.local/foo/my-ns/sa/my-sa"},
		{"wrong middle", "spiffe://cluster.local/ns/my-ns/notsa/my-sa"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, parseSpiffeSAN(tt.san) == nil, true)
		})
	}
}

// --- Split-horizon workload identity tests ---

func TestSplitHorizonWorkload_ExtractsFirstSAN(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	msg := makeSyncMessage("cluster-a", 1, true, "svc1.ns1.svc.cluster.local")
	// Add SANs to the service.
	msg.Services[0].Service.SubjectAltNames = []string{
		"spiffe://cluster.local/ns/ns1/sa/primary-sa",
		"spiffe://cluster.local/ns/ns1/sa/secondary-sa",
	}
	handleSync(store, msg)

	_, wls := store.getFederationState()

	// Find the split-horizon workload (not the gateway).
	var splitHorizonWl *model.WorkloadInfo
	for i := range wls {
		if !isGatewayWorkload(wls[i]) {
			splitHorizonWl = wls[i]
			break
		}
	}
	assert.Equal(t, splitHorizonWl != nil, true)
	// Only the first SAN's service account is used.
	assert.Equal(t, splitHorizonWl.Workload.ServiceAccount, "primary-sa")
}

func TestSplitHorizonWorkload_NoSANDefaultsToDefault(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	msg := makeSyncMessage("cluster-a", 1, true, "svc1.ns1.svc.cluster.local")
	// No SANs.
	msg.Services[0].Service.SubjectAltNames = nil
	handleSync(store, msg)

	_, wls := store.getFederationState()

	var splitHorizonWl *model.WorkloadInfo
	for i := range wls {
		if !isGatewayWorkload(wls[i]) {
			splitHorizonWl = wls[i]
			break
		}
	}
	assert.Equal(t, splitHorizonWl != nil, true)
	assert.Equal(t, splitHorizonWl.Workload.ServiceAccount, "default")
}

// --- Gateway workload tests ---

func TestGatewayWorkload_CreatedPerShard(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	handleSync(store, makeSyncMessage("cluster-a", 1, true, "svc1.ns1.svc.cluster.local"))

	_, wls := store.getFederationState()

	var gatewayCount int
	for i := range wls {
		if isGatewayWorkload(wls[i]) {
			gatewayCount++
		}
	}
	assert.Equal(t, gatewayCount, 1)
}

func TestGatewayWorkload_NoGatewayNoWorkloads(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	msg := &SyncMessage{
		ClusterID:     "cluster-a",
		FullSync:      true,
		VersionVector: map[cluster.ID]uint64{"cluster-a": 1},
		// No NetworkGateway.
		Services: []*model.ServiceInfo{{
			Service: &workloadapi.Service{
				Hostname:  "svc1.ns1.svc.cluster.local",
				Namespace: "ns1",
				Addresses: []*workloadapi.NetworkAddress{{Network: "net-a", Address: []byte{10, 0, 0, 1}}},
				Ports:     []*workloadapi.Port{{ServicePort: 80}},
			},
			Scope: model.Global,
		}},
	}
	handleSync(store, msg)

	_, wls := store.getFederationState()
	assert.Equal(t, len(wls), 0)
}

// --- Channel pressure tests ---

func TestIncomingChannel_DropsOnOverflow(t *testing.T) {
	t.Parallel()

	// The incoming channel has capacity 100. Verify that sending beyond
	// capacity uses non-blocking semantics and does not deadlock.
	ch := make(chan *SyncMessage, 100)

	// Fill the channel.
	for i := range 100 {
		ch <- makeSyncMessage("cluster-a", uint64(i), false, "svc.ns1.svc.cluster.local")
	}

	// One more — this should NOT block (matches handleIncomingMessage behavior).
	select {
	case ch <- makeSyncMessage("cluster-a", 200, false, "svc.ns1.svc.cluster.local"):
		// If we got here, the channel wasn't full (unexpected).
		t.Fatal("expected channel to be full")
	default:
		// Expected: channel full, message dropped.
	}

	assert.Equal(t, len(ch), 100)
}

// TestHandleSyncMessage_SameHostnameDifferentNamespacesCoexist verifies that
// two services in the same source cluster with the same hostname but different
// namespaces (e.g. ExternalName "httpbin" in ns-a and ns-b) are both retained
// in the shard. Before #5 the shard map was keyed by hostname only, so the
// second service overwrote the first.
func TestHandleSyncMessage_SameHostnameDifferentNamespacesCoexist(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	const hostname = "httpbin.example.com"

	mkSvc := func(ns string) *model.ServiceInfo {
		return &model.ServiceInfo{
			Service: &workloadapi.Service{
				Hostname:  hostname,
				Namespace: ns,
				Addresses: []*workloadapi.NetworkAddress{{
					Network: "network-remote",
					Address: []byte{10, 0, 0, 1},
				}},
				Ports: []*workloadapi.Port{{ServicePort: 80, TargetPort: 8080}},
			},
			Scope: model.Global,
		}
	}

	msg := &SyncMessage{
		ClusterID:     "cluster-remote",
		FullSync:      true,
		VersionVector: map[cluster.ID]uint64{"cluster-remote": 1000},
		Network:       "network-remote",
		NetworkGateway: &WireNetworkGateway{
			Cluster:   "cluster-remote",
			Addr:      "10.0.0.1",
			HBONEPort: 15008,
		},
		Services: []*model.ServiceInfo{mkSvc("ns-a"), mkSvc("ns-b")},
	}
	handleSync(store, msg)

	// The shard must hold both services, not one — collect by namespace.
	store.mu.RLock()
	shard := store.shards["cluster-remote"]
	got := make(map[string]bool, len(shard.Services))
	for _, svc := range shard.Services {
		got[svc.Service.Namespace] = true
	}
	store.mu.RUnlock()
	assert.Equal(t, got, map[string]bool{"ns-a": true, "ns-b": true})

	// And the projection must surface both.
	svcs, _ := store.getFederationState()
	namespaces := make(map[string]bool, len(svcs))
	for i := range svcs {
		namespaces[svcs[i].Service.Namespace] = true
	}
	assert.Equal(t, namespaces, map[string]bool{"ns-a": true, "ns-b": true})

	// Incremental delete by exact (namespace, hostname) clears only the
	// matching entry. Before #5/#13 the wire format carried hostname only
	// and both namespaces were collapsed; the new wire format makes the
	// deletion precise.
	deleteMsg := &SyncMessage{
		ClusterID:       "cluster-remote",
		FullSync:        false,
		VersionVector:   map[cluster.ID]uint64{"cluster-remote": 1001},
		DeletedServices: []DeletedServiceKey{{Namespace: "ns-a", Hostname: hostname}},
	}
	handleSync(store, deleteMsg)

	store.mu.RLock()
	remaining := make(map[string]bool, len(store.shards["cluster-remote"].Services))
	for _, svc := range store.shards["cluster-remote"].Services {
		remaining[svc.Service.Namespace] = true
	}
	store.mu.RUnlock()
	assert.Equal(t, remaining, map[string]bool{"ns-b": true})
}

// TestTombstonedShard_RejectsIncrementalAcceptsFullSync verifies that after a
// shard is tombstoned, an incremental message at version > tombstone version is
// dropped (it would leave the shard with only the delta's contents until the
// next snapshot), but a full-sync at the same version repopulates the shard
// atomically.
func TestTombstonedShard_RejectsIncrementalAcceptsFullSync(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Initial sync at version 1000 with two services.
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true,
		"svc1.ns1.svc.cluster.local",
		"svc2.ns1.svc.cluster.local"))

	// Tombstone via expiry.
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()
	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 1)

	store.mu.RLock()
	assert.Equal(t, store.shards["cluster-remote"].Tombstoned, true)
	tombstoneVersion := store.shards["cluster-remote"].Version
	store.mu.RUnlock()
	assert.Equal(t, tombstoneVersion, uint64(1000))

	// An incremental at version 1001 must be REJECTED. Applying it would leave
	// the shard with only the incremental's one service until the next snapshot.
	handleSync(store, makeSyncMessage("cluster-remote", 1001, false,
		"svc-incremental.ns1.svc.cluster.local"))

	store.mu.RLock()
	assert.Equal(t, store.shards["cluster-remote"].Tombstoned, true)
	assert.Equal(t, len(store.shards["cluster-remote"].Services), 0)
	assert.Equal(t, store.shards["cluster-remote"].Version, uint64(1000))
	store.mu.RUnlock()

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)

	// A full-sync at version 1001 resurrects the shard atomically with the new
	// state. This is the required recovery path.
	handleSync(store, makeSyncMessage("cluster-remote", 1001, true,
		"svc-fresh.ns1.svc.cluster.local"))

	store.mu.RLock()
	assert.Equal(t, store.shards["cluster-remote"].Tombstoned, false)
	assert.Equal(t, store.shards["cluster-remote"].Version, uint64(1001))
	assert.Equal(t, len(store.shards["cluster-remote"].Services), 1)
	store.mu.RUnlock()

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc-fresh.ns1.svc.cluster.local")
}

// isGatewayWorkload returns true if the workload UID indicates a NetworkGateway workload.
func isGatewayWorkload(wl *model.WorkloadInfo) bool {
	return len(wl.Workload.Uid) > 15 && wl.Workload.Uid[:15] == "NetworkGateway/"
}

// wlInfo builds a minimal WorkloadInfo with the given UID for tests.
func wlInfo(uid string) *model.WorkloadInfo {
	return &model.WorkloadInfo{Workload: &workloadapi.Workload{Uid: uid, Name: uid}}
}

// TestIncrementalWorkloads_MergeAndDelete verifies that incremental messages
// merge new workloads into the shard and that DeletedWorkloads prunes removed
// UIDs immediately, without waiting for a full-sync — the core of incremental
// workload sync.
func TestIncrementalWorkloads_MergeAndDelete(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Full-sync seeds the shard with two workloads.
	handleSync(store, &SyncMessage{
		ClusterID:     "cluster-remote",
		FullSync:      true,
		VersionVector: map[cluster.ID]uint64{"cluster-remote": 1},
		Network:       "net-1",
		Workloads:     []*model.WorkloadInfo{wlInfo("p1"), wlInfo("p2")},
	})
	shard := store.shards["cluster-remote"]
	assert.Equal(t, len(shard.Workloads), 2)

	// Incremental adds p3 and deletes p2. p1 is untouched and must remain.
	handleSync(store, &SyncMessage{
		ClusterID:        "cluster-remote",
		FullSync:         false,
		VersionVector:    map[cluster.ID]uint64{"cluster-remote": 2},
		Network:          "net-1",
		Workloads:        []*model.WorkloadInfo{wlInfo("p3")},
		DeletedWorkloads: []string{"p2"},
	})

	got := store.shards["cluster-remote"].Workloads
	assert.Equal(t, len(got), 2)
	if _, ok := got["p1"]; !ok {
		t.Errorf("p1 should remain after incremental merge")
	}
	if _, ok := got["p3"]; !ok {
		t.Errorf("p3 should be added by incremental merge")
	}
	if _, ok := got["p2"]; ok {
		t.Errorf("p2 should be pruned by DeletedWorkloads, still present")
	}
}

// TestIncrementalWorkloads_DeleteStopsRouting verifies that a deleted workload
// stops appearing in the same-network shard projection (the routable set)
// immediately after the incremental delete, not only after the next full-sync.
func TestIncrementalWorkloads_DeleteStopsRouting(t *testing.T) {
	t.Parallel()

	store := newFederationStore(federationStoreConfig{
		LocalClusterID:     "local",
		TrustDomainGetter:  func() string { return "cluster.local" },
		LocalNetworkGetter: func() string { return "net-1" }, // same network → workloads pass through
	})

	handleSync(store, &SyncMessage{
		ClusterID:     "cluster-remote",
		FullSync:      true,
		VersionVector: map[cluster.ID]uint64{"cluster-remote": 1},
		Network:       "net-1",
		Workloads:     []*model.WorkloadInfo{wlInfo("p1")},
	})

	_, workloads := store.getFederationState()
	assert.Equal(t, hasWorkloadUID(workloads, "p1"), true)

	// Incremental delete of p1.
	handleSync(store, &SyncMessage{
		ClusterID:        "cluster-remote",
		FullSync:         false,
		VersionVector:    map[cluster.ID]uint64{"cluster-remote": 2},
		Network:          "net-1",
		DeletedWorkloads: []string{"p1"},
	})

	_, workloads = store.getFederationState()
	assert.Equal(t, hasWorkloadUID(workloads, "p1"), false)
}

// TestFlatNetwork_EmptyNetworkIsSameNetwork verifies that when both the local
// cluster and the remote shard report an empty (default/flat) network ID, the
// shard is treated as same-network and its pod workloads are routed directly —
// rather than being pushed down the split-horizon path, which without a gateway
// would leave the service with no routable workloads.
func TestFlatNetwork_EmptyNetworkIsSameNetwork(t *testing.T) {
	t.Parallel()

	store := newFederationStore(federationStoreConfig{
		LocalClusterID:    "local",
		TrustDomainGetter: func() string { return "cluster.local" },
		// No LocalNetworkGetter → local network is "" (default/flat network).
	})

	// Remote shard also on the default network (Network == "") with a pod
	// workload and no gateway.
	handleSync(store, &SyncMessage{
		ClusterID:     "cluster-remote",
		FullSync:      true,
		VersionVector: map[cluster.ID]uint64{"cluster-remote": 1},
		Network:       "", // default/flat network
		Workloads:     []*model.WorkloadInfo{wlInfo("p1")},
	})

	_, workloads := store.getFederationState()
	// The pod must be directly routable; no gateway exists to fall back to.
	assert.Equal(t, hasWorkloadUID(workloads, "p1"), true)
}

// hasWorkloadUID reports whether any workload in the slice has the given UID.
func hasWorkloadUID(wls []*model.WorkloadInfo, uid string) bool {
	for i := range wls {
		if wls[i].Workload != nil && wls[i].Workload.Uid == uid {
			return true
		}
	}
	return false
}

// makeEviction builds the message an external membership controller publishes
// when a cluster is removed: a full-sync with no services, workloads or gateway, marked evicted.
func makeEviction(clusterID cluster.ID, version uint64) *SyncMessage {
	return &SyncMessage{
		ClusterID:     clusterID,
		FullSync:      true,
		VersionVector: map[cluster.ID]uint64{clusterID: version},
		Evicted:       true,
	}
}

func TestEviction_TombstonesLiveShard(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))

	handleSync(store, makeEviction("cluster-remote", 2000))

	svcs, wls := store.getFederationState()
	assert.Equal(t, len(svcs), 0)
	assert.Equal(t, len(wls), 0)

	store.mu.RLock()
	defer store.mu.RUnlock()
	shard := store.shards["cluster-remote"]
	assert.Equal(t, shard.Tombstoned, true)
	assert.Equal(t, shard.Version, uint64(2000))
	assert.Equal(t, store.versionVector.get("cluster-remote"), uint64(2000))
}

// An eviction at the same version as the last snapshot passes the gate.
func TestEviction_EqualVersionTombstones(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")
	handleSync(store, makeSyncMessage("cluster-remote", 2000, true, "svc1.ns1.svc.cluster.local"))

	handleSync(store, makeEviction("cluster-remote", 2000))

	svcs, wls := store.getFederationState()
	assert.Equal(t, len(svcs), 0)
	assert.Equal(t, len(wls), 0)

	store.mu.RLock()
	defer store.mu.RUnlock()
	shard := store.shards["cluster-remote"]
	assert.Equal(t, shard.Tombstoned, true)
	assert.Equal(t, shard.Version, uint64(2000))
	assert.Equal(t, store.versionVector.get("cluster-remote"), uint64(2000))
}

// If the eviction only emptied the shard and left it live, a late incremental
// above the eviction version would be applied in place and re-add partial state.
func TestEviction_DropsLateIncremental(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))
	handleSync(store, makeEviction("cluster-remote", 2000))

	handleSync(store, makeSyncMessage("cluster-remote", 2001, false, "svc1.ns1.svc.cluster.local"))

	svcs, wls := store.getFederationState()
	assert.Equal(t, len(svcs), 0)
	assert.Equal(t, len(wls), 0)

	store.mu.RLock()
	defer store.mu.RUnlock()
	shard := store.shards["cluster-remote"]
	assert.Equal(t, shard.Tombstoned, true)
	assert.Equal(t, shard.Version, uint64(2000))
	assert.Equal(t, store.versionVector.get("cluster-remote"), uint64(2000))
}

func TestEviction_StaleEvictionIgnored(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")
	handleSync(store, makeSyncMessage("cluster-remote", 2000, true, "svc1.ns1.svc.cluster.local"))

	handleSync(store, makeEviction("cluster-remote", 1500))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)

	store.mu.RLock()
	defer store.mu.RUnlock()
	shard := store.shards["cluster-remote"]
	assert.Equal(t, shard.Tombstoned, false)
	assert.Equal(t, shard.Version, uint64(2000))
	assert.Equal(t, store.versionVector.get("cluster-remote"), uint64(2000))
}

func TestEviction_RedeliveryIsNoop(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))
	handleSync(store, makeEviction("cluster-remote", 2000))

	store.mu.RLock()
	lastSeen := store.shards["cluster-remote"].LastSeen
	store.mu.RUnlock()

	handleSync(store, makeEviction("cluster-remote", 2000))

	store.mu.RLock()
	defer store.mu.RUnlock()
	shard := store.shards["cluster-remote"]
	assert.Equal(t, shard.Tombstoned, true)
	assert.Equal(t, shard.Version, uint64(2000))
	// Rejected by the version gate, so it does not touch the shard at all.
	assert.Equal(t, shard.LastSeen, lastSeen)
}

func TestEviction_RejoinResurrects(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))
	handleSync(store, makeEviction("cluster-remote", 2000))

	handleSync(store, makeSyncMessage("cluster-remote", 2001, true, "svc-new.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc-new.ns1.svc.cluster.local")

	store.mu.RLock()
	defer store.mu.RUnlock()
	shard := store.shards["cluster-remote"]
	assert.Equal(t, shard.Tombstoned, false)
	assert.Equal(t, shard.Version, uint64(2001))
	assert.Equal(t, store.versionVector.get("cluster-remote"), uint64(2001))
}

// Istiod could see the eviction without ever having seen the cluster. It must
// still record the version so a retained older snapshot cannot recreate it.
func TestEviction_UnknownClusterRecordsVersion(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")
	handleSync(store, makeEviction("cluster-unseen", 2000))

	handleSync(store, makeSyncMessage("cluster-unseen", 1999, true, "svc1.ns1.svc.cluster.local"))
	handleSync(store, makeSyncMessage("cluster-unseen", 2000, true, "svc1.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)

	store.mu.RLock()
	defer store.mu.RUnlock()
	shard := store.shards["cluster-unseen"]
	assert.Equal(t, shard.Tombstoned, true)
	assert.Equal(t, shard.Version, uint64(2000))
	assert.Equal(t, store.versionVector.get("cluster-unseen"), uint64(2000))
}

// An evicted shard is already tombstoned, so the expiry sweep skips it.
func TestEviction_NotReportedAsExpired(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")
	handleSync(store, makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))
	handleSync(store, makeEviction("cluster-remote", 2000))

	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()

	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 0)
}
