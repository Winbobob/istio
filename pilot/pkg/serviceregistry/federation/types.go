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
	"time"

	"google.golang.org/protobuf/proto"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/util/protoconv"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/workloadapi"
)

// SyncMessage represents a federation synchronization message between istiod peers.
// Note: We use a wire-friendly format (WireServiceInfo) for serialization because
// model.ServiceInfo contains protobuf types that don't serialize cleanly with JSON.
type SyncMessage struct {
	// VersionVector tracks versions per cluster for conflict resolution.
	// Key is cluster ID, value is the monotonically increasing version.
	VersionVector map[cluster.ID]uint64

	// FullSync indicates whether this is a full synchronization (on connect)
	// or an incremental delta update.
	FullSync bool

	// ClusterID identifies the originating cluster for this message.
	ClusterID cluster.ID

	// Network is the Istio network ID of the originating cluster.
	// Used by the receiver to determine whether the sender is on the same
	// network (single-network: use workloads directly) or a different network
	// (multi-network: use services + gateway for split-horizon).
	Network string `json:"network,omitempty"`

	// NetworkGateway contains the originating cluster's east-west gateway info.
	// This is used by remote clusters to create split-horizon workloads.
	NetworkGateway *WireNetworkGateway `json:"networkGateway,omitempty"`

	// Services contains the service information being synchronized.
	// Note: This field is used internally but not for wire serialization.
	Services []*model.ServiceInfo `json:"-"`

	// WireServices contains the wire-friendly service representations.
	// This is populated during serialization from Services.
	WireServices []WireServiceInfo `json:"services,omitempty"`

	// DeletedServices contains the (namespace, hostname) pairs of services
	// that were deleted. Used in incremental (non-full-sync) messages.
	// Carries namespace alongside hostname so two services with the same
	// hostname in different namespaces can be removed independently —
	// the previous DeletedHostnames []string conflated them.
	DeletedServices []DeletedServiceKey `json:"deletedServices,omitempty"`

	// Capacities maps service key (namespace/hostname) to total backend
	// capacity from the source cluster. Populated during wire-to-model
	// conversion alongside Services; not serialized (capacity lives on
	// WireServiceInfo on the wire).
	Capacities map[string]uint32 `json:"-"`

	// Workloads contains the workload information being synchronized.
	// Used for single-network federation where pods are directly reachable
	// and no split-horizon routing via E/W gateway is needed.
	// Not serialized directly — converted to/from WireWorkloads for wire transport.
	Workloads []*model.WorkloadInfo `json:"-"`

	// WireWorkloads contains the wire-friendly workload representations.
	// Populated during serialization from Workloads.
	WireWorkloads []WireWorkloadInfo `json:"workloads,omitempty"`

	// DeletedWorkloads contains the UIDs of workloads that were removed (the pod
	// was deleted, or it no longer backs any global service). Used in incremental
	// (non-full-sync) messages so the receiver can prune them immediately rather
	// than waiting for the next full-sync snapshot. Plain JSON like DeletedServices
	// — workloads are keyed by Workload.Uid everywhere, so the UID is the key.
	DeletedWorkloads []string `json:"deletedWorkloads,omitempty"`

	// Evicted marks an eviction published by an external membership controller:
	// the cluster was removed from the mesh and its state must be withdrawn
	// without waiting for shard expiry. Never set by istiod.
	Evicted bool `json:"evicted,omitempty"`
}

// DeletedServiceKey identifies a single service to remove from a shard.
type DeletedServiceKey struct {
	Namespace string `json:"namespace"`
	Hostname  string `json:"hostname"`
}

// WireServiceInfo is a wire-friendly representation of ServiceInfo.
// It uses base64-encoded protobuf for the Service field to avoid JSON marshaling issues.
//
// Hostname and Namespace are intentionally NOT carried at this level —
// they are inside ServiceProto and ToServiceInfo never reads a duplicate.
type WireServiceInfo struct {
	// ServiceProto is the base64-encoded protobuf of workloadapi.Service
	ServiceProto []byte `json:"serviceProto,omitempty"`

	// Scope indicates if the service is local or global
	Scope model.ServiceScope `json:"scope,omitempty"`

	// LabelSelector preserves the service selector metadata needed by ambient internals.
	LabelSelector model.LabelSelector `json:"labelSelector,omitempty"`

	// PortNames preserves the service-port to target-port name mapping.
	PortNames map[int32]model.ServicePortName `json:"portNames,omitempty"`

	// Source identifies the originating resource for status and conflict handling.
	Source model.TypedObject `json:"source,omitempty"`

	// Waypoint preserves service waypoint binding status used by ambient and status code.
	Waypoint model.WaypointBindingStatus `json:"waypoint,omitempty"`

	// CreationTime preserves deterministic merge ordering metadata.
	CreationTime time.Time `json:"creationTime,omitempty"`

	// Capacity is the total backend capacity for this service (sum of workload
	// capacities on the source cluster). Used to set the capacity on synthetic
	// split-horizon workloads so ztunnel weights cross-cluster traffic
	// proportionally. Senders omit it (0) for services with no workloads;
	// receivers do not route to that cluster for the service.
	Capacity uint32 `json:"capacity,omitempty"`
}

// WireNetworkGateway represents network gateway information synced via federation.
//
// TODO(#12, istio/istio#56960): widen Addr to Addresses []string and fan out a
// gateway workload per address on receive so multi-replica / multi-AZ
// east-west gateway HA is preserved across federation. Held back from PR 4
// to keep the wire shape close to upstream's current single-gateway model.
type WireNetworkGateway struct {
	// Cluster is the cluster ID where this gateway resides.
	Cluster string `json:"cluster"`
	// Addr is the gateway address (IP or hostname).
	Addr string `json:"addr"`
	// HBONEPort is the HBONE mTLS port for the gateway.
	HBONEPort uint32 `json:"hbonePort"`
	// ServiceAccount is the service account name used by the gateway.
	// This is used for mTLS identity verification.
	ServiceAccount string `json:"serviceAccount,omitempty"`
	// Namespace is the namespace where the gateway pod resides.
	Namespace string `json:"namespace,omitempty"`
}

// WireWorkloadInfo is a wire-friendly representation of WorkloadInfo.
// It uses proto-marshaled bytes for the Workload field, same pattern as WireServiceInfo.
// Used for single-network federation where pods are directly reachable.
type WireWorkloadInfo struct {
	// WorkloadProto is the proto-marshaled workloadapi.Workload
	WorkloadProto []byte `json:"workloadProto,omitempty"`

	// Labels preserves the workload labels needed by ambient internals.
	Labels map[string]string `json:"labels,omitempty"`

	// Source identifies the originating resource for status and conflict handling.
	Source model.TypedObject `json:"source,omitempty"`

	// CreationTime preserves deterministic ordering metadata.
	CreationTime time.Time `json:"creationTime,omitempty"`
}

// ToWireWorkloadInfo converts a model.WorkloadInfo to wire format.
func ToWireWorkloadInfo(wl *model.WorkloadInfo) (WireWorkloadInfo, error) {
	if wl == nil || wl.Workload == nil {
		return WireWorkloadInfo{}, nil
	}

	data, err := proto.Marshal(wl.Workload)
	if err != nil {
		return WireWorkloadInfo{}, fmt.Errorf("marshal workload %s: %w", wl.Workload.Uid, err)
	}

	return WireWorkloadInfo{
		WorkloadProto: data,
		Labels:        wl.Labels,
		Source:        wl.Source,
		CreationTime:  wl.CreationTime,
	}, nil
}

// ToWorkloadInfo converts wire format back to model.WorkloadInfo.
func (w *WireWorkloadInfo) ToWorkloadInfo() *model.WorkloadInfo {
	if w == nil || len(w.WorkloadProto) == 0 {
		return nil
	}

	wl := &workloadapi.Workload{}
	if err := proto.Unmarshal(w.WorkloadProto, wl); err != nil {
		return nil
	}

	addr := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{Workload: wl},
	}
	marshaled := protoconv.MessageToAny(addr)

	return &model.WorkloadInfo{
		Workload:         wl,
		Labels:           w.Labels,
		Source:           w.Source,
		CreationTime:     w.CreationTime,
		MarshaledAddress: marshaled,
		AsAddress:        model.AddressInfo{Address: addr, Marshaled: marshaled},
	}
}

// shardProjection holds the derived services and workloads for a single
// cluster shard. It is computed once from the shard's raw data and cached
// until the shard is mutated (new sync message, expiry, or resurrection).
type shardProjection struct {
	// Services are the localized services derived from this shard.
	Services []*model.ServiceInfo

	// Workloads are the split-horizon + gateway workloads derived from this shard.
	Workloads []*model.WorkloadInfo
}

// clusterShard holds the synchronized state from a remote cluster.
type clusterShard struct {
	// ClusterID identifies the remote cluster.
	ClusterID cluster.ID

	// Network is the Istio network ID of the remote cluster.
	Network string

	// Version is the current version for this cluster's data.
	Version uint64

	// Services maps service hostname to service info.
	Services map[string]*model.ServiceInfo

	// Capacities maps service key (namespace/hostname) to total backend capacity
	// from the source cluster. Used to set the capacity on synthetic split-horizon
	// workloads so ztunnel can weight cross-cluster traffic proportionally.
	Capacities map[string]uint32

	// Workloads maps workload UID to workload info. Used for single-network
	// federation where pods are directly reachable (no split-horizon needed).
	Workloads map[string]*model.WorkloadInfo

	// NetworkGateway is the network gateway info for this cluster.
	NetworkGateway *WireNetworkGateway

	// LastSeen is the time the last accepted message from this cluster was processed.
	// Used by the expiry sweep to detect dead clusters.
	LastSeen time.Time

	// Tombstoned is true when this shard has been expired due to prolonged silence.
	// A tombstoned shard retains its Version so that stale messages (including
	// retained full-sync snapshots) cannot resurrect a dead cluster. Only a
	// message with a strictly newer version can clear the tombstone.
	// Resurrection depends on the invariant that version values are time-based
	// (millis since epoch), so a new leader always produces higher versions.
	Tombstoned bool

	// cachedProjection holds the pre-computed derived services and workloads
	// for this shard. Nil means the cache is invalidated and must be rebuilt.
	// Set to nil on every mutation (sync message, expiry, resurrection).
	cachedProjection *shardProjection
}

// ToWireServiceInfo converts a model.ServiceInfo to wire format.
// Returns an error if the inner workloadapi.Service cannot be proto-marshaled.
// Callers MUST handle the error: a silently-empty ServiceProto is dropped on the
// receive side without diagnostics. nil/empty inputs are not errors.
func ToWireServiceInfo(svc *model.ServiceInfo) (WireServiceInfo, error) {
	if svc == nil || svc.Service == nil {
		return WireServiceInfo{}, nil
	}

	wire := WireServiceInfo{
		Scope:         svc.Scope,
		LabelSelector: svc.LabelSelector,
		PortNames:     svc.PortNames,
		Source:        svc.Source,
		Waypoint:      svc.Waypoint,
		CreationTime:  svc.CreationTime,
	}

	// Serialize the protobuf Service.
	data, err := proto.Marshal(svc.Service)
	if err != nil {
		return WireServiceInfo{}, fmt.Errorf("marshal service %s/%s: %w", svc.Service.Namespace, svc.Service.Hostname, err)
	}
	wire.ServiceProto = data

	return wire, nil
}

// ToServiceInfo converts wire format back to model.ServiceInfo.
// It also precomputes the AsAddress field for use by AddressInformation.
func (w *WireServiceInfo) ToServiceInfo() *model.ServiceInfo {
	if w == nil || len(w.ServiceProto) == 0 {
		return nil
	}

	svc := &workloadapi.Service{}
	if err := proto.Unmarshal(w.ServiceProto, svc); err != nil {
		return nil
	}

	// Precompute the AddressInfo for use by AddressInformation()
	addr := &workloadapi.Address{
		Type: &workloadapi.Address_Service{
			Service: svc,
		},
	}
	marshaled := protoconv.MessageToAny(addr)

	return &model.ServiceInfo{
		Service:          svc,
		LabelSelector:    w.LabelSelector,
		PortNames:        w.PortNames,
		Source:           w.Source,
		Scope:            w.Scope,
		Waypoint:         w.Waypoint,
		CreationTime:     w.CreationTime,
		MarshaledAddress: marshaled,
		AsAddress: model.AddressInfo{
			Address:   addr,
			Marshaled: marshaled,
		},
	}
}
