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

package bootstrap

import (
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"

	"istio.io/api/label"
	"istio.io/istio/pilot/pkg/features"
	"istio.io/istio/pilot/pkg/leaderelection"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/serviceregistry/ambient"
	"istio.io/istio/pilot/pkg/serviceregistry/federation"
	"istio.io/istio/pkg/kube/kclient"
	"istio.io/istio/pkg/log"
)

// initFederationSync initializes pub/sub federation. Ambient global services
// are synchronized with peer istiods through the broker transport returned by
// federation.NewTransport.
//
// Multi-replica behavior:
//   - Every replica receives all inbound messages and keeps complete federation
//     state. How that is achieved (for example one subscription per replica) is
//     up to the transport.
//   - Leader election gates outbound publishing: only the leader publishes snapshots
//     and service change events. All replicas process inbound messages.
//   - Version vectors use max(counter+1, time.Now().UnixMilli()) to ensure a new
//     leader always produces versions higher than the previous leader.
func (s *Server) initFederationSync(args *PilotArgs, fedSource *ambient.FederationSource) error {
	log.Infof("Initializing federation with broker %q", features.FederationBrokerAddress)

	stopCh := make(chan struct{})
	// closeStop wraps close(stopCh) with sync.OnceFunc so it is safe to call
	// from both the server-stop goroutine and the Start() failure path without
	// panicking on double close.
	closeStop := sync.OnceFunc(func() { close(stopCh) })

	// Informer-backed namespace client for reading the local cluster's Istio
	// network ID from the system namespace label. kclient.New shares the
	// process-wide namespace SharedInformer (already created in NewServer for
	// the discovery-namespaces filter), so this adds no extra API watch, and
	// Get() is a cache lookup rather than a live API call. The informer is
	// registered before s.kubeClient.RunAndWait, so it is started and synced
	// as part of normal server startup.
	namespaces := kclient.New[*corev1.Namespace](s.kubeClient)

	// Federation collections are owned by FederationSource and shared with
	// the ambient index. The transport is built inside NewSyncProtocol via
	// the NewTransport closure, with the protocol bound as its Handler.
	syncProtocol, err := federation.NewSyncProtocol(federation.SyncProtocolConfig{
		LocalClusterID: s.clusterID,
		AmbientIndexGetter: func() model.FederationAmbientIndex {
			return s.ambientIndex
		},
		LocalNetworkGatewayGetter: func() *model.NetworkGateway {
			// Find the local cluster's network gateway by matching cluster ID.
			//
			// TODO(#12, istio/istio#56960): when the federation wire format
			// learns to carry multiple addresses, return every gateway whose
			// Cluster matches s.clusterID so multi-replica / multi-AZ
			// east-west gateway HA is preserved end-to-end. Held back to keep
			// the wire shape close to upstream's current single-gateway model.
			if s.environment.NetworkManager != nil {
				gateways := s.environment.NetworkManager.AllGateways()
				for i := range gateways {
					if gateways[i].Cluster == s.clusterID {
						return &gateways[i]
					}
				}
			}
			return nil
		},
		LocalNetworkGetter: func() string {
			// Read the system namespace's topology.istio.io/network label.
			// This is the canonical source for the local cluster's Istio network ID.
			// Served from the namespace informer cache (no API round-trip), and
			// tracks label changes live so a network reassignment is picked up
			// without restarting istiod.
			ns := namespaces.Get(args.Namespace, "")
			if ns == nil {
				return ""
			}
			return ns.Labels[label.TopologyNetwork.Name]
		},
		TrustDomainGetter: func() string {
			return s.environment.Mesh().GetTrustDomain()
		},
		Sink:               fedSource,
		SnapshotInterval:   features.FederationSnapshotInterval,
		ShardExpiry:        features.FederationShardExpiry,
		ShardSweepInterval: features.FederationShardSweepInterval,
		NewTransport: func(handler federation.Handler) (federation.Transport, error) {
			return federation.NewTransport(federation.TransportConfig{
				LocalClusterID: s.clusterID,
				ReplicaID:      args.PodName,
				BrokerAddress:  features.FederationBrokerAddress,
				Handler:        handler,
				StopCh:         stopCh,
			})
		},
		StopCh: stopCh,
	})
	if err != nil {
		return fmt.Errorf("failed to create sync protocol: %w", err)
	}

	// Federation is best-effort, so it is not wired to the readiness probe.
	// Health is exposed via metrics and SyncProtocol.IsReady().

	// Register the debug endpoint on the internal (auth-disabled) mux. The mux
	// is initialized in initIstiodAdminServer, which runs before
	// initServiceControllers, so it is non-nil here.
	if s.internalDebugMux != nil {
		s.internalDebugMux.HandleFunc("/debug/federation/shards", syncProtocol.HandleDebugShards)
	}

	// Start the sync protocol (transport + inbound processing on all replicas)
	s.addStartFunc("federation registry", func(stop <-chan struct{}) error {
		go func() {
			<-stop
			closeStop()
			if err := syncProtocol.Stop(); err != nil {
				log.Warnf("Error stopping sync protocol: %v", err)
			}
		}()
		if err := syncProtocol.Start(); err != nil {
			closeStop()
			return err
		}
		return nil
	})

	// Use leader election to gate outbound publishing.
	// Only the leader replica publishes snapshots and service change events.
	// All replicas process inbound messages independently.
	s.addStartFunc("federation leader election", func(stop <-chan struct{}) error {
		go leaderelection.
			NewLeaseLeaderElection(args.Namespace, args.PodName, leaderelection.FederationController, args.Revision, s.kubeClient).
			AddRunFunction(func(leaderStop <-chan struct{}) {
				syncProtocol.BecomeLeader(leaderStop)
				<-leaderStop
			}).
			Run(stop)
		return nil
	})

	log.Info("Federation initialized")
	return nil
}
