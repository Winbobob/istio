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
	"time"

	"istio.io/istio/pkg/cluster"
)

// Transport is the byte-pipe contract between SyncProtocol and a pub/sub
// broker. A Transport is built with its Handler already bound; methods on
// the Handler are safe to call concurrently from transport goroutines.
//
// The only broker requirement is pub/sub fan-out: every message published by
// one istiod must reach every other subscribed istiod. Delivery may be
// at-least-once and may include the publisher's own messages; SyncProtocol
// drops self-messages and version vectors make duplicates harmless. Retaining
// recent messages for cold-start bootstrap is optional.
type Transport interface {
	// Start connects to the broker and starts receiving. Every message must be
	// passed to Handler.HandleMessage, on every istiod replica of every cluster
	// (for example one subscription per replica, keyed on ReplicaID). Start
	// returns once receiving has begun; the receive loop runs in its own
	// goroutine. Called once on every replica, before any other method. A
	// returned error fails istiod startup.
	Start() error
	// Broadcast publishes msg to all peers through the broker. Only the leader
	// calls it. At-least-once delivery is fine. The transport is responsible for
	// encoding msg; Services, Capacities and Workloads are not serialized
	// directly and must be carried as WireServices / WireWorkloads.
	Broadcast(msg *SyncMessage) error
	// BootstrapDone returns a channel that closes once retained messages have
	// been replayed to the Handler (and Handler.SeedSelfVersion called).
	// Transports that do not retain messages return an already closed channel.
	// It does not gate readiness, but the leader does not publish until it
	// closes, so it must close.
	BootstrapDone() <-chan struct{}
	// Shutdown stops receiving and releases broker connections. Called once on
	// istiod shutdown.
	Shutdown() error
}

// Handler is the receive side that SyncProtocol implements for the transport.
type Handler interface {
	// HandleMessage delivers a decoded SyncMessage. enqueuedAt is the broker
	// timestamp (the time the broker accepted the message) used as
	// shard.LastSeen so freshness ignores cross-cluster clock skew. Transport
	// MUST substitute time.Now() when the broker did not stamp a timestamp;
	// the store also substitutes for a zero value defensively.
	HandleMessage(msg *SyncMessage, enqueuedAt time.Time)

	// ReportConnectivity flips the pilot_federation_transport_connected gauge.
	ReportConnectivity(connected bool)

	// SeedSelfVersion raises the local cluster's version-vector counter to at
	// least the given value. Transports that retain messages call it once during bootstrap,
	// after peeking retained self-messages and computing the maximum
	// VersionVector[self] observed. This restores monotonicity across istiod
	// leader handovers when the new leader's wall clock is behind the old
	// leader's last-published version. Called before BootstrapDone() closes.
	SeedSelfVersion(version uint64)
}

// TransportFactory builds a Transport with the Handler bound. SyncProtocol
// invokes it inside NewSyncProtocol once the *SyncProtocol shell exists.
type TransportFactory func(handler Handler) (Transport, error)

// TransportConfig carries the settings istiod passes to a transport. Anything
// a transport needs beyond these (topic or subject names, tuning) is owned and
// read by the transport implementation.
//
// Do not put secrets in BrokerAddress. Credentials should
// come from a mounted Secret file (exposed here as a path) or from workload
// identity.
type TransportConfig struct {
	// LocalClusterID identifies this istiod's cluster.
	LocalClusterID cluster.ID
	// ReplicaID uniquely identifies this istiod replica (the pod name).
	// Transports that need per-consumer state, such as a subscription or a
	// consumer group, can key on it.
	ReplicaID string
	// BrokerAddress is where the broker is, in the transport's own format
	// (for example a URL, a host:port list, or a namespace FQDN). It must not
	// contain credentials.
	BrokerAddress string
	// Handler receives decoded messages. Required.
	Handler Handler
	// StopCh is closed on shutdown.
	StopCh chan struct{}
}

// NewTransport builds the transport SyncProtocol uses to reach the broker.
//
// TODO: no broker implementation exists yet. This returns a no-op transport:
// Broadcast drops messages and nothing is ever delivered to the Handler, so
// istiod runs the sync protocol but exchanges no data with peers.
func NewTransport(cfg TransportConfig) (Transport, error) {
	syncLog.Warnf("No federation transport implementation is available; federation messages are not sent or received")
	bootstrapDone := make(chan struct{})
	// There is no retained state to replay.
	close(bootstrapDone)
	return &noopTransport{bootstrapDone: bootstrapDone}, nil
}

type noopTransport struct {
	bootstrapDone chan struct{}
}

func (t *noopTransport) Start() error                   { return nil }
func (t *noopTransport) Broadcast(*SyncMessage) error   { return nil }
func (t *noopTransport) BootstrapDone() <-chan struct{} { return t.bootstrapDone }
func (t *noopTransport) Shutdown() error                { return nil }
