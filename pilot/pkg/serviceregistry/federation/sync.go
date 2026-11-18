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
	"sync/atomic"
	"time"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/util/runtime"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/log"
)

const (
	// debounceAfter is the quiet period after receiving a message before
	// flushing accumulated state to KRT collections and triggering an XDS push.
	// If more messages arrive within this window, the timer resets.
	debounceAfter = 200 * time.Millisecond

	// debounceMax is the maximum time to wait before flushing, regardless of
	// whether messages are still arriving. This bounds worst-case latency
	// during sustained high-throughput updates.
	debounceMax = 2 * time.Second
)

var syncLog = log.RegisterScope("federation-sync", "Federation sync protocol")

// runWithRecovery runs fn, recovering from panics and restarting with a brief
// backoff. This ensures critical federation goroutines are not silently lost.
// If fn returns normally (e.g. stopCh closed), runWithRecovery returns.
func runWithRecovery(name string, fn func()) {
	for {
		recovered := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					runtime.LogPanic(r)
					syncLog.Errorf("federation goroutine %q panicked, restarting after backoff", name)
					panicsRecovered.With(goroutineTag.Value(name)).Increment()
					recovered = true
				}
			}()
			fn()
		}()
		if !recovered {
			return
		}
		time.Sleep(1 * time.Second)
	}
}

// HealthState tracks the health of the federation subsystem.
// All fields are safe for concurrent access.
type HealthState struct {
	// BootstrapComplete is true once the bootstrap drain finishes.
	BootstrapComplete atomic.Bool

	// TransportConnected is true while the receive loop is actively
	// receiving messages. Set false after consecutive receive errors
	// exceed the threshold; set true on first successful receive.
	TransportConnected atomic.Bool

	// LastMessageReceived is the wallclock time of the last successfully
	// processed inbound message (unix millis). Zero means none received.
	LastMessageReceived atomic.Int64
}

// outgoingEvent represents a local change to be broadcast to peers — either a
// service change or a workload change (diffientiated by kind). For services,
// hostname/namespace are set for delete events and service is set for upsert.
// For workloads, workload is set for upsert and workloadUID is set for delete.
// Carrying namespace explicitly (services) / UID (workloads) lets the receive
// side delete the exact entry.
type outgoingEvent struct {
	kind outgoingKind

	// service-kind fields
	service   *model.ServiceInfo // non-nil for add/update, nil for delete
	hostname  string             // set for delete events
	namespace string             // set for delete events

	// workload-kind fields
	workload    *model.WorkloadInfo // non-nil for add/update, nil for delete
	workloadUID string              // set for delete events

	event model.Event
}

// outgoingKind diffientiates the two payload types carried on outgoingCh.
type outgoingKind int

const (
	outgoingService outgoingKind = iota
	outgoingWorkload
)

// incomingEnvelope pairs a received SyncMessage with the broker enqueue
// timestamp; the timestamp flows into shard.LastSeen so freshness reflects
// the source clock, not this replica's delivery clock.
type incomingEnvelope struct {
	msg        *SyncMessage
	enqueuedAt time.Time
}

// SyncProtocol orchestrates federation: it broadcasts local service changes
// through the broker transport and applies inbound messages to the federation store. It
// implements Handler so the transport receives directly into this struct
// without a back-reference.
type SyncProtocol struct {
	// store holds synced remote services from other clusters.
	store *federationStore

	// transport is built inside NewSyncProtocol via cfg.NewTransport with
	// this *SyncProtocol bound as its Handler.
	transport Transport

	// health tracks the federation subsystem's health for IsReady() and metrics.
	health HealthState

	// localClusterID is the local cluster's ID.
	localClusterID cluster.ID

	// versionVector tracks local versions for outbound sync messages.
	versionVector *versionVector

	// ambientIndexGetter resolves the FederationAmbientIndex; see
	// SyncProtocolConfig.AmbientIndexGetter for why this is a getter.
	ambientIndexGetter func() model.FederationAmbientIndex

	// ambientIndex is the resolved ambient index. Populated and validated
	// non-nil in Start(); accessed everywhere else.
	ambientIndex model.FederationAmbientIndex

	// sink replaces the union of all live remote shards at the end of every
	// debounce window. The same krt collections are wired into the ambient
	// index, so a single Apply propagates through to XDS.
	sink Sink

	// localNetworkGatewayGetter returns the local network gateway.
	localNetworkGatewayGetter func() *model.NetworkGateway

	// snapshotInterval controls how often a full-sync snapshot is published.
	snapshotInterval time.Duration

	// incomingCh buffers received messages for debounced apply. Each envelope
	// carries the broker enqueue timestamp for use as shard.LastSeen.
	incomingCh chan incomingEnvelope

	// outgoingCh buffers local service change events for debounced broadcasting.
	// Events are deduplicated per hostname and batched into a single SyncMessage.
	outgoingCh chan outgoingEvent

	// isLeader is true when this replica holds the federation leader lease.
	// Only the leader publishes outbound messages; all replicas process inbound.
	isLeader atomic.Bool

	// leaderStopCh is closed when this replica loses leader status.
	// Re-created each time leadership is acquired.
	leaderStopCh chan struct{}

	// maintenanceCh signals the inbound debounce loop to run a shard expiry check.
	// Capacity 1 with non-blocking sends so the sweep loop never blocks.
	maintenanceCh chan struct{}

	// shardExpiry is the duration after which a shard with no accepted
	// messages is tombstoned. Zero disables expiry.
	shardExpiry time.Duration

	// shardSweepInterval is how often to check for expired shards.
	shardSweepInterval time.Duration

	// stopCh signals shutdown.
	stopCh chan struct{}
}

// SyncProtocolConfig contains configuration for the sync protocol.
type SyncProtocolConfig struct {
	LocalClusterID cluster.ID

	// SnapshotInterval controls how often a full-sync snapshot is published
	// to the broker. New istiods bootstrap from these retained snapshots.
	// Defaults to 5m if zero. Set negative to disable.
	SnapshotInterval time.Duration

	// AmbientIndexGetter resolves the FederationAmbientIndex this protocol
	// integrates with. Required: NewSyncProtocol rejects a nil getter, and
	// Start rejects a getter that resolves to nil — federation supports
	// ambient mode only.
	//
	// A getter (rather than a value) is required because the ambient index is
	// constructed by the multicluster controller's startFunc, which runs after
	// initFederationSync registers this config. The getter resolves successfully
	// by the time Start() runs.
	AmbientIndexGetter func() model.FederationAmbientIndex

	// LocalNetworkGatewayGetter returns the local cluster's network gateway.
	// This is synced to remote clusters so they can route traffic back.
	//
	// TODO(#12, istio/istio#56960): widen to []model.NetworkGateway so federation
	// publishes every replica/AZ of the local east-west gateway. Held back from
	// PR 4 to keep wire shape close to upstream's current single-gateway model.
	LocalNetworkGatewayGetter func() *model.NetworkGateway

	// LocalNetworkGetter returns the local cluster's Istio network ID.
	// Populated on SyncMessage.Network so receivers can determine whether
	// the sender is on the same network (single-network: use workloads
	// directly) or a different network (multi-network: use gateway).
	LocalNetworkGetter func() string

	// Sink owns the federated services/workloads collections (typically
	// *ambient.FederationSource) and is the only path through which the
	// protocol mutates ambient-index-visible state. The collection is shared
	// with the ambient index, so a single Sink.Apply propagates to XDS.
	Sink Sink

	// TrustDomainGetter returns the mesh trust domain.
	// Used to set the trust domain on split-horizon workloads for HBONE mTLS.
	TrustDomainGetter func() string

	// NewTransport constructs the federation transport with this SyncProtocol
	// bound as the Handler. Invoked exactly once during NewSyncProtocol after
	// the *SyncProtocol shell exists, so the Handler is non-nil at construction.
	NewTransport TransportFactory

	// ShardExpiry is the duration after which a shard with no accepted
	// messages is tombstoned and its services/workloads removed.
	// Defaults to max(3*SnapshotInterval, 15m) if zero. Set negative to disable.
	ShardExpiry time.Duration

	// ShardSweepInterval is how often to check for expired shards.
	// Defaults to 1m if zero.
	ShardSweepInterval time.Duration

	StopCh chan struct{}
}

// NewSyncProtocol creates a new sync protocol handler.
// Note: Call Start() to begin receiving/publishing messages.
func NewSyncProtocol(cfg SyncProtocolConfig) (*SyncProtocol, error) {
	if cfg.NewTransport == nil {
		return nil, fmt.Errorf("NewTransport is required")
	}
	if cfg.Sink == nil {
		return nil, fmt.Errorf("Sink is required")
	}
	if cfg.AmbientIndexGetter == nil {
		return nil, fmt.Errorf("AmbientIndexGetter is required (federation supports ambient mode only)")
	}

	// Create internal components for tracking outbound sync state
	versionVector := newVersionVector()

	// Create the federation store for holding synced remote services
	store := newFederationStore(federationStoreConfig{
		LocalClusterID:     cfg.LocalClusterID,
		LocalNetworkGetter: cfg.LocalNetworkGetter,
		TrustDomainGetter:  cfg.TrustDomainGetter,
	})

	snapshotInterval := cfg.SnapshotInterval
	if snapshotInterval == 0 {
		snapshotInterval = 5 * time.Minute
	}

	// Compute shard expiry. Default: max(3*snapshotInterval, 15m).
	// This gives headroom for publish jitter, election turnover, and
	// transient broker hiccups.
	shardExpiry := cfg.ShardExpiry
	if shardExpiry == 0 {
		shardExpiry = 3 * snapshotInterval
		if shardExpiry < 15*time.Minute {
			shardExpiry = 15 * time.Minute
		}
	}
	if shardExpiry < 0 {
		shardExpiry = 0 // disabled
	}

	// If shard expiry is enabled, a positive snapshot interval is required.
	// Without periodic snapshots, remote clusters have no heartbeat signal
	// and every peer will eventually tombstone this cluster's shard.
	if shardExpiry > 0 && snapshotInterval < 0 {
		return nil, fmt.Errorf("shard expiry (%v) requires a positive SnapshotInterval; got %v", shardExpiry, snapshotInterval)
	}

	shardSweepInterval := cfg.ShardSweepInterval
	if shardSweepInterval == 0 {
		shardSweepInterval = 1 * time.Minute
	}

	sp := &SyncProtocol{
		store:                     store,
		localClusterID:            cfg.LocalClusterID,
		versionVector:             versionVector,
		ambientIndexGetter:        cfg.AmbientIndexGetter,
		sink:                      cfg.Sink,
		localNetworkGatewayGetter: cfg.LocalNetworkGatewayGetter,
		snapshotInterval:          snapshotInterval,
		shardExpiry:               shardExpiry,
		shardSweepInterval:        shardSweepInterval,
		incomingCh:                make(chan incomingEnvelope, 100),
		outgoingCh:                make(chan outgoingEvent, 100),
		maintenanceCh:             make(chan struct{}, 1),
		stopCh:                    cfg.StopCh,
	}

	// Build the transport with this *SyncProtocol bound as its Handler.
	transport, err := cfg.NewTransport(sp)
	if err != nil {
		return nil, fmt.Errorf("build transport: %w", err)
	}
	sp.transport = transport

	return sp, nil
}

// IsReady reports BootstrapComplete && TransportConnected.
//
// NOT a Kubernetes readiness probe — federation is best-effort by design (a
// broker outage must not flip istiods to NotReady). Used by /debug/federation
// and dashboards; alerting should use the bootstrap_complete and
// transport_connected gauges.
func (sp *SyncProtocol) IsReady() bool {
	return sp.health.BootstrapComplete.Load() && sp.health.TransportConnected.Load()
}

// Start begins federation messaging and registers for service change events.
//
// All replicas:
//   - Start transport (receive inbound messages from remote clusters)
//   - Process incoming messages → store → KRT → XDS push
//   - Register federation collections with ambient index
//   - Register for global service change events (enqueue to outgoingCh)
//
// Leader only (activated via BecomeLeader):
//   - Outgoing debounce loop (drain outgoingCh → broadcast through the transport)
//   - Snapshot loop (periodic full-sync publish)
//
// This separation ensures all replicas have identical federation state for
// serving ztunnel/envoy, while only one replica publishes to the broker.
func (sp *SyncProtocol) Start() error {
	// Resolve the ambient index. See SyncProtocolConfig.AmbientIndexGetter
	// for why resolution is deferred to Start.
	sp.ambientIndex = sp.ambientIndexGetter()
	if sp.ambientIndex == nil {
		return fmt.Errorf("federation is enabled but ambient mode is not: " +
			"either enable ambient (PILOT_ENABLE_AMBIENT=true) " +
			"or disable federation (PILOT_ENABLE_FEDERATION=false)")
	}

	// 1. Start transport first — must be running before we can receive.
	// Mark TransportConnected eagerly; the receive loop flips it false after
	// consecutiveErrorThreshold failures and back true on the next success.
	if err := sp.transport.Start(); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}
	sp.health.TransportConnected.Store(true)
	transportConnected.Record(1)
	syncLog.Info("Transport started")

	// 2. Start inbound message processor (all replicas)
	go runWithRecovery("processIncomingDebounced", sp.processIncomingDebounced)

	// 3. Start shard expiry sweep (all replicas — every replica serves
	// federated state to ztunnel/envoy and must independently expire dead shards)
	if sp.shardExpiry > 0 {
		go runWithRecovery("shardExpiryLoop", sp.shardExpiryLoop)
	}

	// 4. Register for push-based global service change events.
	// Events are enqueued to outgoingCh on all replicas but only
	// broadcast by processOutgoingDebounced when isLeader is true.
	sp.ambientIndex.RegisterGlobalServiceHandler(func(prev, curr *model.ServiceInfo, event model.Event) {
		switch event {
		case model.EventAdd, model.EventUpdate:
			sp.handleServiceUpsert(curr)
		case model.EventDelete:
			if prev != nil && prev.Service != nil {
				sp.handleServiceDelete(prev.Service.Namespace, prev.Service.Hostname)
			}
		}
	})
	syncLog.Info("Registered for push-based global service events")

	// 5. Register for push-based workload change events so incremental messages
	// carry only changed workloads (plus explicit deletions) rather than the
	// full snapshot. Same leader-gated, enqueue-to-outgoingCh model as services.
	sp.ambientIndex.RegisterGlobalWorkloadHandler(func(prev, curr *model.WorkloadInfo, event model.Event) {
		switch event {
		case model.EventAdd, model.EventUpdate:
			sp.handleWorkloadUpsert(curr)
		case model.EventDelete:
			if prev != nil && prev.Workload != nil {
				sp.handleWorkloadDelete(prev.Workload.Uid)
			}
		}
	})
	syncLog.Info("Registered for push-based global workload events")

	syncLog.Info("Sync protocol started (awaiting leader election for outbound publishing)")

	// Wait for bootstrap to mark the health flag. This is an observability
	// signal only — federation does not gate readiness on it. (Outbound
	// publishing gates on bootstrap separately: both snapshotLoop and
	// processOutgoingDebounced wait on BootstrapDone so SeedSelfVersion has
	// raised the version counter before the leader publishes.)
	go func() {
		select {
		case <-sp.transport.BootstrapDone():
			sp.health.BootstrapComplete.Store(true)
			bootstrapComplete.Record(1)
			syncLog.Info("Health: bootstrap complete")
		case <-sp.stopCh:
		}
	}()

	return nil
}

// Stop gracefully stops the sync protocol.
func (sp *SyncProtocol) Stop() error {
	sp.StopLeading() // stop outbound if leading
	return sp.transport.Shutdown()
}

// BecomeLeader activates outbound publishing on this replica.
// Called by the leader election callback when this istiod wins the lease.
// It starts the outgoing debounce loop and the snapshot loop, and publishes
// an immediate full-sync snapshot so remote clusters see the new leader's state.
func (sp *SyncProtocol) BecomeLeader(leaderStop <-chan struct{}) {
	// Idempotency guard: only run the setup on the not-leader → leader transition.
	// If already leading, re-entering would orphan leaderStopCh and start duplicate
	// debounce/snapshot goroutines publishing to the same outgoingCh.
	if sp.isLeader.Load() {
		return
	}

	sp.leaderStopCh = make(chan struct{})
	sp.isLeader.Store(true)
	syncLog.Info("This replica is now the federation leader — starting outbound publishing")
	isLeader.Record(1)

	// Start the outgoing debounce loop (drains outgoingCh → broadcast)
	go runWithRecovery("processOutgoingDebounced", sp.processOutgoingDebounced)

	// Start the snapshot loop (periodic full sync publish)
	if sp.snapshotInterval > 0 {
		go runWithRecovery("snapshotLoop", sp.snapshotLoop)
	}

	// Bridge the external leaderStop to our internal leaderStopCh
	go func() {
		defer runtime.HandleCrash(runtime.LogPanic)
		select {
		case <-leaderStop:
			sp.StopLeading()
		case <-sp.stopCh:
		}
	}()
}

// StopLeading deactivates outbound publishing on this replica.
// Called when this replica loses the leader lease.
func (sp *SyncProtocol) StopLeading() {
	if !sp.isLeader.CompareAndSwap(true, false) {
		return
	}
	syncLog.Info("This replica lost federation leadership — stopping outbound publishing")
	isLeader.Record(0)
	close(sp.leaderStopCh)
}

// snapshotLoop publishes a full-sync snapshot immediately on leader election
// and every snapshotInterval thereafter. Outbound carries only local data, but
// we wait for inbound bootstrap so the version-vector seed from retained
// self-messages (Handler.SeedSelfVersion) is in place before any outbound
// increment — otherwise a new leader with a clock behind the previous leader
// would publish stale-looking versions.
func (sp *SyncProtocol) snapshotLoop() {
	// Wait for bootstrap so SeedSelfVersion has had a chance to raise the
	// local counter past the previous leader's last published version.
	select {
	case <-sp.transport.BootstrapDone():
	case <-sp.leaderStopCh:
		return
	case <-sp.stopCh:
		return
	}

	// Publish initial snapshot immediately on leader election (best-effort).
	syncLog.Info("Leader publishing initial full sync")
	sp.publishSnapshot()

	ticker := time.NewTicker(sp.snapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sp.publishSnapshot()
		case <-sp.leaderStopCh:
			syncLog.Info("Snapshot loop stopping (lost leadership)")
			return
		case <-sp.stopCh:
			syncLog.Info("Snapshot loop stopping (shutdown)")
			return
		}
	}
}

// publishSnapshot builds and publishes a full-sync snapshot message.
func (sp *SyncProtocol) publishSnapshot() {
	msg := sp.buildFullSyncMessage()
	if msg == nil {
		return
	}
	sp.broadcast(msg)
	if msg.FullSync {
		messagesSentFullSync.Increment()
	}
	syncLog.Infof("Published full-sync snapshot: %d services", len(msg.Services))
}

// broadcast sends a sync message to all peers through the transport.
func (sp *SyncProtocol) broadcast(msg *SyncMessage) {
	if err := sp.transport.Broadcast(msg); err != nil {
		syncLog.Warnf("Failed to broadcast message: %v", err)
		broadcastErrors.Increment()
	}
}

// getLocalNetworkGateway returns the local network gateway info for syncing.
func (sp *SyncProtocol) getLocalNetworkGateway() *WireNetworkGateway {
	if sp.localNetworkGatewayGetter == nil {
		return nil
	}
	gw := sp.localNetworkGatewayGetter()
	if gw == nil {
		return nil
	}
	wireGw := &WireNetworkGateway{
		Cluster:        gw.Cluster.String(),
		Addr:           gw.Addr,
		HBONEPort:      gw.HBONEPort,
		ServiceAccount: gw.ServiceAccount.Name,
		Namespace:      gw.ServiceAccount.Namespace,
	}
	return wireGw
}

// getLocalNetwork returns the local cluster's Istio network ID.
func (sp *SyncProtocol) getLocalNetwork() string {
	if sp.store.localNetworkGetter == nil {
		return ""
	}
	return sp.store.localNetworkGetter()
}

// buildFullSyncMessage creates a full sync message with all local global services.
func (sp *SyncProtocol) buildFullSyncMessage() *SyncMessage {
	// AllGlobalServiceFromLocalClusterWithSANs includes SPIFFE identities so
	// remote clusters know what identity to expect when connecting.
	services := sp.ambientIndex.AllGlobalServiceFromLocalClusterWithSANs()

	// Increment version for this sync
	version := sp.versionVector.increment(sp.localClusterID)

	msg := &SyncMessage{
		VersionVector:  sp.versionVector.copy(),
		FullSync:       true,
		ClusterID:      sp.localClusterID,
		Network:        sp.getLocalNetwork(),
		NetworkGateway: sp.getLocalNetworkGateway(),
		Services:       services,
		Capacities:     sp.computeServiceCapacities(services),
		// Always include local workloads backing global services. The receiver
		// decides whether to use them (same network → direct routing) or ignore
		// them (different network → split-horizon via gateway).
		Workloads: sp.ambientIndex.AllLocalWorkloadsForGlobalServices(),
	}

	syncLog.Infof("Built full sync message: %d services, %d workloads, version=%d",
		len(services), len(msg.Workloads), version)

	return msg
}

// computeServiceCapacities returns a map of service key → total backend
// capacity for each service, using the ambient index to sum workload
// capacities. This matches upstream's multicluster.go coalesced workload logic.
func (sp *SyncProtocol) computeServiceCapacities(serviceInfos []*model.ServiceInfo) map[string]uint32 {
	caps := make(map[string]uint32, len(serviceInfos))
	for _, svc := range serviceInfos {
		if svc == nil || svc.Service == nil {
			continue
		}
		cap := sp.ambientIndex.ServiceCapacity(svc)
		if cap > 0 {
			caps[serviceKey(svc.Service.Namespace, svc.Service.Hostname)] = cap
		}
	}
	return caps
}

// handleServiceUpsert enqueues a service add/update for debounced broadcast
// (leader-only). Non-blocking: a full outgoingCh drops the event and the next
// snapshot reconciles. Blocking would back-pressure into the krt event
// callback and stall unrelated XDS pushes.
func (sp *SyncProtocol) handleServiceUpsert(svc *model.ServiceInfo) {
	if svc == nil || svc.Scope != model.Global {
		return
	}
	if !sp.isLeader.Load() {
		return
	}

	syncLog.Debugf("Enqueuing service upsert: %s", svc.Service.Hostname)

	select {
	case sp.outgoingCh <- outgoingEvent{kind: outgoingService, service: svc, event: model.EventUpdate}:
	case <-sp.stopCh:
	default:
		messagesDroppedOutgoingOverflow.Increment()
		syncLog.Warnf("Outgoing federation channel full, dropping upsert for %s; next snapshot will reconcile",
			svc.Service.Hostname)
	}
}

// handleServiceDelete enqueues a service deletion for debounced broadcast
// (leader-only). Non-blocking: see handleServiceUpsert. Carries namespace so
// the receive side can delete the exact (namespace, hostname) entry.
func (sp *SyncProtocol) handleServiceDelete(namespace, hostname string) {
	if !sp.isLeader.Load() {
		return
	}

	syncLog.Debugf("Enqueuing service delete: %s/%s", namespace, hostname)

	select {
	case sp.outgoingCh <- outgoingEvent{kind: outgoingService, namespace: namespace, hostname: hostname, event: model.EventDelete}:
	case <-sp.stopCh:
	default:
		messagesDroppedOutgoingOverflow.Increment()
		syncLog.Warnf("Outgoing federation channel full, dropping delete for %s/%s; next snapshot will reconcile",
			namespace, hostname)
	}
}

// handleWorkloadUpsert enqueues a workload add/update for debounced broadcast
// (leader-only). Non-blocking: see handleServiceUpsert. The workload is already
// filtered to global service keys by the ambient index before this is called.
func (sp *SyncProtocol) handleWorkloadUpsert(wl *model.WorkloadInfo) {
	if wl == nil || wl.Workload == nil {
		return
	}
	if !sp.isLeader.Load() {
		return
	}

	syncLog.Debugf("Enqueuing workload upsert: %s", wl.Workload.Uid)

	select {
	case sp.outgoingCh <- outgoingEvent{kind: outgoingWorkload, workload: wl, event: model.EventUpdate}:
	case <-sp.stopCh:
	default:
		messagesDroppedOutgoingOverflow.Increment()
		syncLog.Warnf("Outgoing federation channel full, dropping workload upsert for %s; next snapshot will reconcile",
			wl.Workload.Uid)
	}
}

// handleWorkloadDelete enqueues a workload deletion for debounced broadcast
// (leader-only). Non-blocking: see handleServiceUpsert. Carries the UID so the
// receive side can delete the exact workload from the shard.
func (sp *SyncProtocol) handleWorkloadDelete(uid string) {
	if uid == "" {
		return
	}
	if !sp.isLeader.Load() {
		return
	}

	syncLog.Debugf("Enqueuing workload delete: %s", uid)

	select {
	case sp.outgoingCh <- outgoingEvent{kind: outgoingWorkload, workloadUID: uid, event: model.EventDelete}:
	case <-sp.stopCh:
	default:
		messagesDroppedOutgoingOverflow.Increment()
		syncLog.Warnf("Outgoing federation channel full, dropping workload delete for %s; next snapshot will reconcile",
			uid)
	}
}

// processOutgoingDebounced batches local service change events and broadcasts
// them as a single SyncMessage. Events are deduplicated per hostname (last
// event wins), so rapid-fire updates from deployments produce one message.
// Only runs on the leader replica; exits when leadership is lost.
//
// Waits for bootstrap before draining outgoingCh so the version-vector seed
// from retained self-messages (Handler.SeedSelfVersion) is in place before
// any outbound increment. Without this gate, a new leader with a wall clock
// behind the previous leader would publish messages with versions <= the
// previous leader's last published version and receivers would reject them
// as stale until the clock catches up.
func (sp *SyncProtocol) processOutgoingDebounced() {
	select {
	case <-sp.transport.BootstrapDone():
	case <-sp.leaderStopCh:
		return
	case <-sp.stopCh:
		return
	}

	for {
		var ev outgoingEvent
		select {
		case ev = <-sp.outgoingCh:
		case <-sp.leaderStopCh:
			return
		case <-sp.stopCh:
			return
		}

		// Accumulate events, deduplicating by (namespace, hostname) so that
		// two services with the same hostname in different namespaces don't
		// overwrite each other in the pending-batch maps. Workloads are
		// accumulated separately, deduplicated by UID.
		pendingServices := make(map[string]*model.ServiceInfo)
		pendingDeletes := make(map[string]DeletedServiceKey)
		pendingWorkloads := make(map[string]*model.WorkloadInfo)
		pendingWorkloadDeletes := make(map[string]struct{})

		applyEvent := func(e outgoingEvent) {
			switch e.kind {
			case outgoingWorkload:
				if e.event == model.EventDelete {
					delete(pendingWorkloads, e.workloadUID)
					pendingWorkloadDeletes[e.workloadUID] = struct{}{}
				} else if e.workload != nil && e.workload.Workload != nil {
					uid := e.workload.Workload.Uid
					delete(pendingWorkloadDeletes, uid)
					pendingWorkloads[uid] = e.workload
				}
			default: // outgoingService
				if e.event == model.EventDelete {
					key := serviceKey(e.namespace, e.hostname)
					delete(pendingServices, key)
					pendingDeletes[key] = DeletedServiceKey{Namespace: e.namespace, Hostname: e.hostname}
				} else {
					svc := e.service
					if enriched := sp.ambientIndex.ServiceWithSANs(svc); enriched != nil {
						svc = enriched
					}
					key := serviceKey(svc.Service.Namespace, svc.Service.Hostname)
					delete(pendingDeletes, key)
					pendingServices[key] = svc
				}
			}
		}

		applyEvent(ev)

		quietTimer := time.NewTimer(debounceAfter)
		maxTimer := time.NewTimer(debounceMax)

	drain:
		for {
			select {
			case ev = <-sp.outgoingCh:
				applyEvent(ev)
				// Go 1.23+ guarantees no stale value after Stop() (unbuffered
				// timer channel), so draining <-quietTimer.C is unnecessary
				// and would block.
				quietTimer.Stop()
				quietTimer.Reset(debounceAfter)
			case <-quietTimer.C:
				break drain
			case <-maxTimer.C:
				break drain
			case <-sp.leaderStopCh:
				quietTimer.Stop()
				maxTimer.Stop()
				return
			case <-sp.stopCh:
				quietTimer.Stop()
				maxTimer.Stop()
				return
			}
		}
		quietTimer.Stop()
		maxTimer.Stop()

		if len(pendingServices) == 0 && len(pendingDeletes) == 0 &&
			len(pendingWorkloads) == 0 && len(pendingWorkloadDeletes) == 0 {
			continue
		}

		version := sp.versionVector.increment(sp.localClusterID)

		services := make([]*model.ServiceInfo, 0, len(pendingServices))
		for _, svc := range pendingServices {
			services = append(services, svc)
		}

		deletedServices := make([]DeletedServiceKey, 0, len(pendingDeletes))
		for _, d := range pendingDeletes {
			deletedServices = append(deletedServices, d)
		}

		workloads := make([]*model.WorkloadInfo, 0, len(pendingWorkloads))
		for _, wl := range pendingWorkloads {
			workloads = append(workloads, wl)
		}

		deletedWorkloads := make([]string, 0, len(pendingWorkloadDeletes))
		for uid := range pendingWorkloadDeletes {
			deletedWorkloads = append(deletedWorkloads, uid)
		}

		msg := &SyncMessage{
			VersionVector:   sp.versionVector.copy(),
			FullSync:        false,
			ClusterID:       sp.localClusterID,
			Network:         sp.getLocalNetwork(),
			NetworkGateway:  sp.getLocalNetworkGateway(),
			Services:        services,
			DeletedServices: deletedServices,
			Capacities:      sp.computeServiceCapacities(services),
			// Incremental workload delta: only the workloads that changed
			// (pendingWorkloads) plus the UIDs that were removed. The full set is
			// reconciled by the periodic full-sync snapshot. The receiver decides
			// whether to use them based on network match.
			Workloads:        workloads,
			DeletedWorkloads: deletedWorkloads,
		}

		sp.broadcast(msg)
		if msg.FullSync {
			messagesSentFullSync.Increment()
		} else {
			messagesSentIncremental.Increment()
		}
		channelDepthOutgoing.Record(float64(len(sp.outgoingCh)))
		syncLog.Infof("Flushed debounced outgoing broadcast: %d services, %d service deletes, %d workloads, %d workload deletes, version=%d",
			len(services), len(deletedServices), len(workloads), len(deletedWorkloads), version)
	}
}

// processIncomingDebounced is the main loop for debounced message processing.
// It accumulates incoming messages and applies them to the store immediately
// (so version vectors stay current), but defers the expensive KRT collection
// reset and XDS push until a quiet period or max wait is reached.
//
// Maintenance events (shard expiry sweeps) are also handled here to ensure
// a single serialized path owns store mutation and KRT Reset() calls.
//
// This provides two layers of batching:
//  1. Federation debounce (200ms quiet / 2s max) — batches store→KRT→push
//  2. Istiod's built-in XDS debounce (100ms/10s) — batches pushes to proxies
func (sp *SyncProtocol) processIncomingDebounced() {
	for {
		// Block until first message or maintenance signal arrives.
		var gotMessage bool
		select {
		case env := <-sp.incomingCh:
			sp.store.handleSyncMessage(env.msg, env.enqueuedAt)
			gotMessage = true
		case <-sp.maintenanceCh:
			// Maintenance signal: run expiry check and flush.
		case <-sp.stopCh:
			return
		}

		count := 0
		if gotMessage {
			count = 1
			sp.health.LastMessageReceived.Store(time.Now().UnixMilli())
		}

		// Start debounce timers.
		quietTimer := time.NewTimer(debounceAfter)
		maxTimer := time.NewTimer(debounceMax)

		// Accumulate more messages within the debounce window.
	drain:
		for {
			select {
			case env := <-sp.incomingCh:
				sp.store.handleSyncMessage(env.msg, env.enqueuedAt)
				count++
				sp.health.LastMessageReceived.Store(time.Now().UnixMilli())
				// Go 1.23+ guarantees no stale value after Stop() (unbuffered
				// timer channel), so draining <-quietTimer.C is unnecessary
				// and would block.
				quietTimer.Stop()
				quietTimer.Reset(debounceAfter)
			case <-sp.maintenanceCh:
				// Coalesce maintenance signals into the current debounce window.
			case <-quietTimer.C:
				break drain
			case <-maxTimer.C:
				break drain
			case <-sp.stopCh:
				quietTimer.Stop()
				maxTimer.Stop()
				return
			}
		}
		quietTimer.Stop()
		maxTimer.Stop()

		// Run shard expiry check. This is cheap when nothing expires.
		if sp.shardExpiry > 0 {
			if expired := sp.store.expireStaleShards(sp.shardExpiry, time.Now()); len(expired) > 0 {
				syncLog.Infof("Expired %d shard(s): %v", len(expired), expired)
			}
		}

		// Single batch update: rebuild KRT collections and trigger one XDS push.
		flushStart := time.Now()
		services, workloads := sp.store.getFederationState()
		sp.sink.Apply(services, workloads)
		flushDurationInbound.Record(time.Since(flushStart).Seconds())

		// Update state gauges.
		federatedServices.Record(float64(len(services)))
		federatedWorkloads.Record(float64(len(workloads)))
		channelDepthIncoming.Record(float64(len(sp.incomingCh)))
		sp.store.recordShardGauges()
		// No manual XDS push needed: krt's reactive pipeline propagates
		// collection changes through JoinCollection → indexes → RegisterBatch
		// → XDS push automatically.

		syncLog.Infof("Flushed debounced federation update: %d messages, %d services, %d workloads",
			count, len(services), len(workloads))
	}
}

// shardExpiryLoop periodically signals the debounce loop to run an expiry
// check. It runs on all replicas because every replica independently serves
// federated state to proxies and must withdraw dead clusters' contributions.
func (sp *SyncProtocol) shardExpiryLoop() {
	ticker := time.NewTicker(sp.shardSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Non-blocking send: if the debounce loop hasn't consumed the
			// previous signal yet, skip — it will run expiry on its next flush.
			select {
			case sp.maintenanceCh <- struct{}{}:
			default:
			}
		case <-sp.stopCh:
			return
		}
	}
}

// HandleMessage is the Handler entry point for inbound messages (live and
// bootstrap-replayed). The message is enqueued for debounced apply.
//
// Non-blocking: a full incomingCh drops the message; processMessage in the
// transport acks unconditionally afterward, so a dropped message is removed
// from the subscription and recovered at the next snapshot. Blocking would
// stall the transport's receiver for the entire subscription.
//
// enqueuedAt is the broker enqueue timestamp — forwarded to the store as
// shard.LastSeen so freshness uses the source clock.
func (sp *SyncProtocol) HandleMessage(msg *SyncMessage, enqueuedAt time.Time) {
	if msg == nil {
		return
	}

	syncLog.Debugf("Received sync from cluster %s: %d services, fullSync=%v — enqueuing",
		msg.ClusterID, len(msg.Services), msg.FullSync)

	select {
	case sp.incomingCh <- incomingEnvelope{msg: msg, enqueuedAt: enqueuedAt}:
	case <-sp.stopCh:
	default:
		messagesDroppedIncomingOverflow.Increment()
		syncLog.Warnf("Incoming federation channel full, dropping message from cluster %s (services=%d, fullSync=%v); next snapshot will reconcile",
			msg.ClusterID, len(msg.Services), msg.FullSync)
	}
}

// SeedSelfVersion is the Handler entry point for the bootstrap self-version
// recovery. The transport surfaces the maximum VersionVector[self] observed
// in retained bootstrap messages so the next versionVector.increment returns
// a value strictly greater than the previous leader's last published version.
// Called before BootstrapDone() closes; outbound publishing waits on bootstrap
// so this seed is in place before any increment happens.
func (sp *SyncProtocol) SeedSelfVersion(version uint64) {
	if version == 0 {
		return
	}
	sp.versionVector.seed(sp.localClusterID, version)
	syncLog.Infof("Seeded local version vector from bootstrap: cluster=%s version=%d",
		sp.localClusterID, version)
}

// ReportConnectivity is the Handler entry point for transport connectivity
// transitions; mirrored into the pilot_federation_transport_connected gauge.
func (sp *SyncProtocol) ReportConnectivity(connected bool) {
	sp.health.TransportConnected.Store(connected)
	if connected {
		transportConnected.Record(1)
	} else {
		transportConnected.Record(0)
	}
}
