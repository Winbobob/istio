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
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"istio.io/istio/pilot/pkg/model"
	labelutil "istio.io/istio/pilot/pkg/serviceregistry/util/label"
	"istio.io/istio/pilot/pkg/util/protoconv"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/config/schema/kind"
	"istio.io/istio/pkg/log"
	"istio.io/istio/pkg/maps"
	"istio.io/istio/pkg/network"
	"istio.io/istio/pkg/slices"
	"istio.io/istio/pkg/workloadapi"
	"k8s.io/apimachinery/pkg/types"
)

var storeLog = log.RegisterScope("federation-store", "Federation data store")

// federationStore is an internal data store for federation-synced services.
// It stores the synchronized state from remote clusters and provides
// methods to extract data for KRT collections.
type federationStore struct {
	mu sync.RWMutex

	// shards contains the synchronized state from remote clusters.
	shards map[cluster.ID]*clusterShard

	// versionVector tracks versions for conflict resolution.
	versionVector *versionVector

	// localClusterID is the local cluster's ID.
	localClusterID cluster.ID

	// localNetworkGetter returns the local cluster's Istio network ID.
	// Used by buildShardProjection to determine whether a remote shard
	// is on the same network (use workloads directly) or different network
	// (use split-horizon via gateway).
	localNetworkGetter func() string

	// trustDomainGetter returns the mesh trust domain.
	// Used to set the trust domain on split-horizon workloads.
	trustDomainGetter func() string
}

// federationStoreConfig contains configuration for the federation store.
type federationStoreConfig struct {
	// LocalClusterID is the local cluster's identifier.
	LocalClusterID cluster.ID

	// LocalNetworkGetter returns the local cluster's Istio network ID.
	LocalNetworkGetter func() string

	// TrustDomainGetter returns the mesh trust domain.
	TrustDomainGetter func() string
}

// newFederationStore creates a new federation data store.
func newFederationStore(cfg federationStoreConfig) *federationStore { // checked
	return &federationStore{
		shards:             make(map[cluster.ID]*clusterShard),
		versionVector:      newVersionVector(),
		localClusterID:     cfg.LocalClusterID,
		localNetworkGetter: cfg.LocalNetworkGetter,
		trustDomainGetter:  cfg.TrustDomainGetter,
	}
}

// handleSyncMessage processes a sync message from a peer.
//
// enqueuedAt is the broker-side timestamp at which the source published the
// message, used as
// shard.LastSeen so freshness is measured at the source clock. A zero value
// is substituted with time.Now() defensively.
func (s *federationStore) handleSyncMessage(msg *SyncMessage, enqueuedAt time.Time) { // checked
	if msg == nil {
		return
	}
	if enqueuedAt.IsZero() {
		enqueuedAt = time.Now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	clusterID := msg.ClusterID
	if clusterID == s.localClusterID {
		// Don't process our own messages
		return
	}

	// Check if this is newer than what we have
	remoteVersion, ok := msg.VersionVector[clusterID]
	if !ok {
		remoteVersion = 0
	}
	localVersion := s.versionVector.get(clusterID)

	// Reject messages with version <= the recorded version. This covers both
	// normal stale-message filtering and tombstone protection: a tombstoned
	// shard retains its version, so retained snapshots from a dead cluster
	// cannot resurrect it. Only a strictly newer version can clear a tombstone.
	if remoteVersion <= localVersion {
		// Exception: allow full-sync with equal version (re-delivery of same snapshot),
		// but NOT when the shard is tombstoned — a tombstoned shard can only be
		// resurrected by a strictly newer version.
		if !(msg.FullSync && remoteVersion == localVersion && !s.isShardTombstoned(clusterID)) {
			storeLog.Debugf("Ignoring stale message from cluster %s (remote=%d, local=%d, tombstoned=%v)",
				clusterID, remoteVersion, localVersion, s.isShardTombstoned(clusterID))
			messagesDroppedStale.Increment()
			return
		}
	}

	if msg.Evicted {
		s.evictShard(clusterID, remoteVersion, enqueuedAt)
		return
	}

	storeLog.Infof("Processing sync message from cluster %s (full=%v, version=%d, gateway=%v)",
		clusterID, msg.FullSync, remoteVersion, msg.NetworkGateway)

	// Get or create shard for this cluster.
	// A tombstoned shard can ONLY be resurrected by a full-sync. Incrementals at
	// version > tombstone version are rejected: applying them would leave the
	// shard with whatever subset that one delta carries (the rest of the cluster's
	// state is gone), producing partial data until the next 5-minute snapshot.
	// Drop and wait for the snapshot to repopulate the shard atomically.
	shard, exists := s.shards[clusterID]
	if exists && shard.Tombstoned && !msg.FullSync {
		storeLog.Infof("Dropping incremental from tombstoned cluster %s (version=%d, tombstone=%d) — waiting for full-sync snapshot",
			clusterID, remoteVersion, shard.Version)
		messagesDroppedTombstonedIncremental.Increment()
		return
	}
	// Create a fresh shard when:
	//   - !exists:        first message from this cluster
	//   - msg.FullSync:   full-sync replaces all state atomically, regardless of
	//                     whether the shard is tombstoned or not.
	//
	// Truth table over (exists, tombstoned, fullSync).
	//   exists | tomb | full | outcome
	//   -------+------+------+---------------------------------------------
	//   false  |  n/a | false| fresh (!exists)      — first message, incremental
	//   false  |  n/a | true | fresh (!exists)      — first message, full-sync
	//   true   | false| false| mutate in place      — incremental on live shard
	//   true   | false| true | fresh (fullSync)     — full replace
	//   true   | true | false| dropped before       — never reaches here
	//   true   | true | true | fresh (fullSync)     — resurrection
	if !exists || msg.FullSync {
		if shard != nil && shard.Tombstoned {
			// Reaching here with a tombstoned shard implies msg.FullSync (a
			// tombstoned incremental was already dropped above), i.e. resurrection.
			storeLog.Infof("Resurrecting tombstoned cluster %s with version %d (was %d)",
				clusterID, remoteVersion, shard.Version)
			shardsResurrected.Increment()
		}
		shard = &clusterShard{
			ClusterID:  clusterID,
			Services:   make(map[string]*model.ServiceInfo),
			Capacities: map[string]uint32{},
		}
		s.shards[clusterID] = shard
	}

	// Invalidate projection cache — the shard is about to be mutated.
	shard.cachedProjection = nil

	// Refresh liveness on every accepted message using the broker enqueue
	// time. Two effects: (1) cross-cluster clock skew does not influence
	// shard expiry; (2) a backlog drained after a broker outage does
	// not artificially keep dead shards alive.
	shard.LastSeen = enqueuedAt

	// Store the sender's network ID.
	shard.Network = msg.Network

	// Store the network gateway info from this cluster
	if msg.NetworkGateway != nil {
		shard.NetworkGateway = msg.NetworkGateway
		storeLog.Debugf("Stored network gateway for cluster %s: network=%s, addr=%s, hbonePort=%d",
			clusterID, shard.Network, msg.NetworkGateway.Addr, msg.NetworkGateway.HBONEPort)
	}

	// Delete services listed in incremental messages by exact (namespace,
	// hostname) key. The previous wire format carried hostname only and the
	// receive side scanned all keys with a matching hostname suffix — a
	// shard-internal collision when two namespaces shared a hostname. The
	// new wire format makes the deletion exact.
	for _, d := range msg.DeletedServices {
		key := serviceKey(d.Namespace, d.Hostname)
		if _, ok := shard.Services[key]; ok {
			delete(shard.Services, key)
			delete(shard.Capacities, key)
			storeLog.Debugf("Deleted service %s from cluster %s", key, clusterID)
		}
	}

	// Process services. Key by namespace/hostname so two global services with
	// the same hostname in different namespaces do not overwrite each other.
	for _, svc := range msg.Services {
		if svc == nil || svc.Service == nil {
			continue
		}
		key := serviceKey(svc.Service.Namespace, svc.Service.Hostname)
		shard.Services[key] = svc
		if cap, ok := msg.Capacities[key]; ok {
			shard.Capacities[key] = cap
		}
	}

	// Apply explicit workload deletions, then merge received workloads into the
	// shard, in the same order as DeletedServices / Services above, so an upsert
	// wins if a malformed message lists a UID in both. Incremental messages
	// carry only changed workloads (msg.Workloads) plus the UIDs of removed ones
	// (msg.DeletedWorkloads); full-sync messages carry the complete set and no
	// deletions (and arrive on a fresh shard, so the merge rebuilds from empty).
	// This keeps deleted pods / no-longer-global workloads from staying routable
	// until the next full sync, since buildShardProjection emits every entry in
	// shard.Workloads for same-network shards. The projection side decides whether
	// to use these (same network → direct routing) or ignore them (different
	// network → split-horizon via gateway).
	for _, uid := range msg.DeletedWorkloads {
		if _, ok := shard.Workloads[uid]; ok {
			delete(shard.Workloads, uid)
			storeLog.Debugf("Deleted workload %s from cluster %s", uid, clusterID)
		}
	}
	if len(msg.Workloads) > 0 {
		if shard.Workloads == nil {
			shard.Workloads = make(map[string]*model.WorkloadInfo, len(msg.Workloads))
		}
		for _, wl := range msg.Workloads {
			if wl != nil && wl.Workload != nil {
				shard.Workloads[wl.Workload.Uid] = wl
			}
		}
	}

	// Update version vector
	s.versionVector.set(clusterID, remoteVersion)
	shard.Version = remoteVersion

	// Log current state of all shards
	var shardSummary []string
	for cid, sh := range s.shards {
		if sh.Tombstoned {
			shardSummary = append(shardSummary, fmt.Sprintf("%s:tombstoned", cid))
		} else {
			shardSummary = append(shardSummary, fmt.Sprintf("%s:%d", cid, len(sh.Services)))
		}
	}
	storeLog.Debugf("After sync: shards=%v", shardSummary)
}

// evictShard tombstones a cluster's shard on an external eviction. The caller has
// already passed the version gate, so the eviction is newer than anything held
// for the cluster.
func (s *federationStore) evictShard(clusterID cluster.ID, version uint64, enqueuedAt time.Time) { // checked
	shard, exists := s.shards[clusterID]
	// Create the shard if the one named in the eviction is missing. A full-sync
	// older than the eviction can still arrive later. Without the shard, the
	// cluster's version vector entry starts from 0, so that full-sync would pass
	// the version gate and create a live shard, which is not what we want.
	if !exists {
		shard = &clusterShard{ClusterID: clusterID}
		s.shards[clusterID] = shard
	}

	storeLog.Infof("Evicting shard for cluster %s: evicted (version=%d, previous=%d, tombstoned=%v)",
		clusterID, version, shard.Version, shard.Tombstoned)

	tombstoneShard(shard)
	shard.LastSeen = enqueuedAt
	shard.Version = version
	s.versionVector.set(clusterID, version)
	shardsEvicted.Increment()
}

// tombstoneShard marks a shard tombstoned and drops its state. The cluster's
// version vector entry is left in place, so the version gate still rejects stale
// messages; resurrection replaces the whole shard, so nothing cleared here is
// ever read again.
func tombstoneShard(shard *clusterShard) { // checked
	shard.Tombstoned = true
	shard.Services = nil
	shard.Capacities = nil
	shard.Workloads = nil
	shard.NetworkGateway = nil
	shard.cachedProjection = nil
}

// isShardTombstoned returns whether a cluster's shard is currently tombstoned.
// Caller must already hold s.mu (read or write); this method does not acquire it.
func (s *federationStore) isShardTombstoned(clusterID cluster.ID) bool { // checked
	if shard, ok := s.shards[clusterID]; ok {
		return shard.Tombstoned
	}
	return false
}

// expireStaleShards tombstones shards with no accepted message within the
// expiry duration. Tombstoned shards retain their Version so stale messages
// cannot resurrect them. Returns the cluster IDs newly tombstoned.
func (s *federationStore) expireStaleShards(expiry time.Duration, now time.Time) []cluster.ID { // checked
	if expiry <= 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var expired []cluster.ID
	for cid, shard := range s.shards {
		if shard.Tombstoned {
			continue
		}
		if now.Sub(shard.LastSeen) > expiry {
			storeLog.Warnf("Tombstoning shard for cluster %s: no message in %v (last seen %v)",
				cid, now.Sub(shard.LastSeen).Round(time.Second), shard.LastSeen.Format(time.RFC3339))
			tombstoneShard(shard)
			expired = append(expired, cid)
			shardsExpired.Increment()
		}
	}
	return expired
}

// recordShardGauges updates the live/tombstoned shard gauge metrics.
func (s *federationStore) recordShardGauges() { // checked
	s.mu.RLock()
	defer s.mu.RUnlock()
	var live, tombstoned int
	for _, shard := range s.shards {
		if shard.Tombstoned {
			tombstoned++
		} else {
			live++
		}
	}
	shardsLive.Record(float64(live))
	shardsTombstoned.Record(float64(tombstoned))
}

// getFederationState returns all services and workloads from the federation store.
// This is used to populate KRT collections for ambient index integration.
//
// Each shard's derived services and workloads are cached in a per-shard projection.
// Only shards whose data changed since the last call are recomputed. The final
// aggregation (concatenation + cross-shard gateway deduplication) is always done,
// but it is cheap compared to the per-service proto cloning and workload construction
// that the projection cache avoids.
func (s *federationStore) getFederationState() ([]*model.ServiceInfo, []*model.WorkloadInfo) { // checked
	s.mu.Lock()
	defer s.mu.Unlock()

	var services []*model.ServiceInfo
	var workloads []*model.WorkloadInfo

	// Track which gateway workloads have already been emitted, keyed by UID
	// ("NetworkGateway/<network>/<addr>/<hbonePort>"). The UID has no cluster, so
	// remote clusters sharing a network and gateway address collapse into one
	// gateway workload. Shards are visited in cluster-ID order so the surviving
	// copy (and its ClusterId and service account) is the same on every flush
	// and every replica.
	seenGateways := make(map[string]bool)

	for _, clusterID := range slices.Sort(maps.Keys(s.shards)) {
		shard := s.shards[clusterID]
		// Skip tombstoned shards — their services and gateways have been cleared.
		if shard.Tombstoned {
			continue
		}

		// Use cached projection if available, otherwise rebuild it.
		proj := shard.cachedProjection
		if proj == nil {
			projectionCacheMisses.Increment()
			proj = s.buildShardProjection(shard)
			shard.cachedProjection = proj
		} else {
			projectionCacheHits.Increment()
		}

		services = append(services, proj.Services...)

		// Append workloads with cross-shard gateway deduplication.
		for _, wl := range proj.Workloads {
			// Gateway workloads have UIDs like "NetworkGateway/network/addr/port".
			// Split-horizon workloads are never duplicated across shards.
			if strings.HasPrefix(wl.Workload.Uid, "NetworkGateway/") {
				if seenGateways[wl.Workload.Uid] {
					continue
				}
				seenGateways[wl.Workload.Uid] = true
			}
			workloads = append(workloads, wl)
		}
	}

	return services, workloads
}

// buildShardProjection computes the derived services and workloads for a single
// shard. This is the expensive path: it constructs split-horizon and gateway
// workloads from the shard's raw services.
//
// Services are passed through verbatim — federation publishes Service.Addresses
// with the origin cluster's (network, ip) only. Receiving clusters are expected
// to provide their own locally-routable VIP via a local Service stub or
// ServiceEntry, which the ambient index merges with the federation entry.
// See multicluster-vip-lookup-analysis.html for the rationale.
//
// TODO(#12, istio/istio#56960): when WireNetworkGateway carries multiple
// addresses, fan out one NetworkGateway workload per address and one
// split-horizon workload per (service, address) pair so multi-replica
// east-west gateway HA survives federation.
func (s *federationStore) buildShardProjection(shard *clusterShard) *shardProjection { // checked
	proj := &shardProjection{}

	// Determine whether the remote shard is on the same Istio network.
	// Same network → use workloads directly (pods are reachable).
	// Different network → use split-horizon via gateway.
	localNetwork := ""
	if s.localNetworkGetter != nil {
		localNetwork = s.localNetworkGetter()
	}
	sameNetwork := localNetwork == shard.Network

	// Create a NetworkGateway workload for this shard's gateway (multi-network path).
	if !sameNetwork && shard.NetworkGateway != nil {
		gwWorkload := s.createNetworkGatewayWorkload(shard.NetworkGateway, shard.Network)
		if gwWorkload != nil {
			proj.Workloads = append(proj.Workloads, gwWorkload)
		}
	}

	for _, svc := range shard.Services {
		if svc == nil || svc.Service == nil {
			continue
		}

		proj.Services = append(proj.Services, svc)

		// Create split-horizon workloads for cross-network services.
		if !sameNetwork && shard.NetworkGateway != nil {
			gw := model.NetworkGateway{
				Network:   network.ID(shard.Network),
				Cluster:   cluster.ID(shard.NetworkGateway.Cluster),
				Addr:      shard.NetworkGateway.Addr,
				HBONEPort: shard.NetworkGateway.HBONEPort,
				ServiceAccount: types.NamespacedName{
					Name:      shard.NetworkGateway.ServiceAccount,
					Namespace: shard.NetworkGateway.Namespace,
				},
			}
			svcKey := serviceKey(svc.Service.Namespace, svc.Service.Hostname)
			// Senders omit capacity for services with no workloads, so 0 (or
			// no entry in Capacities) means no backends in that cluster: do
			// not route to it. The service itself stays so its name and VIP
			// still resolve.
			capacity := shard.Capacities[svcKey]
			if capacity == 0 {
				continue
			}
			wlInfo := s.createSplitHorizonWorkload(svc, gw, capacity)
			if wlInfo != nil {
				proj.Workloads = append(proj.Workloads, wlInfo)
			}
		}
	}

	// Same network: pass through received workloads directly (no split-horizon).
	if sameNetwork {
		for _, wl := range shard.Workloads {
			if wl != nil {
				proj.Workloads = append(proj.Workloads, wl)
			}
		}
	}

	return proj
}

// createSplitHorizonWorkload creates a synthetic workload that routes to a network gateway.
// This enables cross-network traffic routing for services discovered via federation.
func (s *federationStore) createSplitHorizonWorkload(svcInfo *model.ServiceInfo, gateway model.NetworkGateway, capacity uint32) *model.WorkloadInfo { // checked
	if svcInfo == nil || svcInfo.Service == nil {
		return nil
	}
	svc := svcInfo.Service
	svcNamespacedName := svc.Namespace + "/" + svc.Hostname

	uid := generateSplitHorizonWorkloadUID(gateway.Network.String(), gatewayResourceName(gateway), svcNamespacedName)

	hboneMtlsPort := gateway.HBONEPort
	if hboneMtlsPort == 0 {
		hboneMtlsPort = 15008
	}

	// Get trust domain for workload identity (defaults to "cluster.local")
	trustDomain := pickTrustDomain(s.trustDomainGetter)

	// Extract service account from the service's SubjectAltNames for mTLS identity verification.
	// The SANs are SPIFFE URIs like "spiffe://cluster.local/ns/<ns>/sa/<sa>".
	// The ztunnel uses the workload's ServiceAccount+Namespace to construct the expected SAN.
	serviceAccount := "default"
	if len(svc.SubjectAltNames) > 0 {
		// Parse the first SAN to extract the service account.
		// Format: spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>
		if parts := parseSpiffeSAN(svc.SubjectAltNames[0]); parts != nil {
			serviceAccount = parts.serviceAccount
		}
	}

	wl := &workloadapi.Workload{
		Uid:            uid,
		Name:           uid,
		Namespace:      svc.Namespace,
		Network:        gateway.Network.String(),
		TrustDomain:    trustDomain,
		ServiceAccount: serviceAccount,
		Capacity:       &wrapperspb.UInt32Value{Value: capacity},
		WorkloadType:   workloadapi.WorkloadType_POD,
		TunnelProtocol: workloadapi.TunnelProtocol_HBONE,
		NetworkGateway: &workloadapi.GatewayAddress{
			Destination:   &workloadapi.GatewayAddress_Address{},
			HboneMtlsPort: hboneMtlsPort,
		},
		ClusterId: gateway.Cluster.String(),
		Services: map[string]*workloadapi.PortList{
			svcNamespacedName: {
				Ports: svc.Ports,
			},
		},
	}

	// Set up the network gateway address
	address, err := netip.ParseAddr(gateway.Addr)
	if err != nil {
		// Assume address is a hostname
		wl.NetworkGateway.Destination = &workloadapi.GatewayAddress_Hostname{
			Hostname: &workloadapi.NamespacedHostname{
				Namespace: gateway.ServiceAccount.Namespace,
				Hostname:  gateway.Addr,
			},
		}
	} else {
		wl.NetworkGateway.Destination = &workloadapi.GatewayAddress_Address{
			Address: &workloadapi.NetworkAddress{
				Network: gateway.Network.String(),
				Address: address.AsSlice(),
			},
		}
	}

	addr := &workloadapi.Address{Type: &workloadapi.Address_Workload{Workload: wl}}
	marshaled := protoconv.MessageToAny(addr)

	storeLog.Debugf("Created split-horizon workload: uid=%s, gateway=%s:%d, svc=%s",
		uid, gateway.Addr, hboneMtlsPort, svcNamespacedName)

	return &model.WorkloadInfo{
		Workload:         wl,
		Source:           model.TypedObject{Kind: kind.KubernetesGateway},
		Labels:           labelutil.AugmentLabels(nil, gateway.Cluster, "", "", gateway.Network),
		MarshaledAddress: marshaled,
		AsAddress:        model.AddressInfo{Address: addr, Marshaled: marshaled},
	}
}

// createNetworkGatewayWorkload creates a workload representing the remote network gateway.
// This is needed so ztunnel knows how to reach the gateway when routing to split-horizon workloads.
// The UID format matches what ztunnel expects: NetworkGateway/network/addr/port
func (s *federationStore) createNetworkGatewayWorkload(wireGw *WireNetworkGateway, networkID string) *model.WorkloadInfo { // checked
	if wireGw == nil {
		return nil
	}

	hbonePort := wireGw.HBONEPort
	if hbonePort == 0 {
		hbonePort = 15008
	}

	uid := fmt.Sprintf("NetworkGateway/%s/%s/%d", networkID, wireGw.Addr, hbonePort)
	trustDomain := pickTrustDomain(s.trustDomainGetter)

	// Use service account from wire gateway, default to "istio-eastwestgateway"
	serviceAccount := wireGw.ServiceAccount
	if serviceAccount == "" {
		serviceAccount = "istio-eastwestgateway"
	}

	// Use namespace from wire gateway, default to "istio-system"
	namespace := wireGw.Namespace
	if namespace == "" {
		namespace = "istio-system"
	}

	wl := &workloadapi.Workload{
		Uid:            uid,
		Name:           uid,
		Namespace:      namespace,
		Network:        networkID,
		TrustDomain:    trustDomain,
		ClusterId:      wireGw.Cluster,
		ServiceAccount: serviceAccount,
	}

	// Set the gateway address
	address, err := netip.ParseAddr(wireGw.Addr)
	if err != nil {
		// If it's a hostname, set it as the hostname
		wl.Hostname = wireGw.Addr
	} else {
		// If it's an IP, add it to addresses
		wl.Addresses = append(wl.Addresses, address.AsSlice())
	}

	addr := &workloadapi.Address{Type: &workloadapi.Address_Workload{Workload: wl}}
	marshaled := protoconv.MessageToAny(addr)

	storeLog.Debugf("Created network gateway workload: uid=%s, addr=%s:%d, sa=%s/%s",
		uid, wireGw.Addr, hbonePort, namespace, serviceAccount)

	return &model.WorkloadInfo{
		Workload:         wl,
		Source:           model.TypedObject{Kind: kind.KubernetesGateway},
		Labels:           labelutil.AugmentLabels(nil, cluster.ID(wireGw.Cluster), "", "", network.ID(networkID)),
		MarshaledAddress: marshaled,
		AsAddress:        model.AddressInfo{Address: addr, Marshaled: marshaled},
	}
}

func gatewayResourceName(gateway model.NetworkGateway) string { // checked
	if gateway.Cluster != "" {
		return gateway.Cluster.String() + "/" + gateway.Addr
	}
	return gateway.Addr
}

func generateSplitHorizonWorkloadUID(networkID, gatewayName, service string) string { // checked
	return networkID + "/SplitHorizonWorkload/" + gatewayName + "/" + service
}

func pickTrustDomain(trustDomainGetter func() string) string { // checked
	if trustDomainGetter != nil {
		if td := trustDomainGetter(); td != "" {
			return td
		}
	}
	return "cluster.local"
}

// spiffeParts holds the components parsed from a SPIFFE URI.
type spiffeParts struct {
	namespace      string
	serviceAccount string
}

// parseSpiffeSAN extracts namespace and service account from a SPIFFE URI.
// Format: spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>
func parseSpiffeSAN(san string) *spiffeParts { // checked
	const prefix = "spiffe://"
	if !strings.HasPrefix(san, prefix) {
		return nil
	}
	// Strip "spiffe://<trust-domain>/"
	rest := san[len(prefix):]
	idx := strings.Index(rest, "/")
	if idx < 0 {
		return nil
	}
	rest = rest[idx+1:] // "ns/<namespace>/sa/<service-account>"
	parts := strings.SplitN(rest, "/", 4)
	if len(parts) != 4 || parts[0] != "ns" || parts[2] != "sa" {
		return nil
	}
	return &spiffeParts{
		namespace:      parts[1],
		serviceAccount: parts[3],
	}
}

// serviceKey produces the in-shard map key for a service. Including the
// namespace prevents two services with the same hostname in different
// namespaces from overwriting each other in shard.Services.
func serviceKey(namespace, hostname string) string { // checked
	return namespace + "/" + hostname
}
