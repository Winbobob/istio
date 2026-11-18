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
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"istio.io/istio/pkg/cluster"
)

// DebugShardInfo is the JSON shape of a single shard for /debug/federation/shards.
type DebugShardInfo struct {
	ClusterID      cluster.ID          `json:"clusterID"`
	Version        uint64              `json:"version"`
	LastSeen       time.Time           `json:"lastSeen"`
	Tombstoned     bool                `json:"tombstoned"`
	Services       int                 `json:"services"`
	NetworkGateway *WireNetworkGateway `json:"networkGateway,omitempty"`
}

// DebugInfo is the JSON shape of the /debug/federation/shards endpoint.
type DebugInfo struct {
	LocalClusterID      cluster.ID `json:"localClusterID"`
	IsLeader            bool       `json:"isLeader"`
	BootstrapComplete   bool       `json:"bootstrapComplete"`
	TransportConnected  bool       `json:"transportConnected"`
	LastMessageReceived time.Time  `json:"lastMessageReceived"`
	// LocalVersion is the local cluster's outbound version counter (next broadcast +1).
	LocalVersion uint64 `json:"localVersion"`
	// AcceptedVersions is the highest version accepted from each remote cluster
	// (i.e. what would be filtered as stale on the next inbound message).
	AcceptedVersions   map[cluster.ID]uint64 `json:"acceptedVersions"`
	Shards             []DebugShardInfo      `json:"shards"`
	IncomingChannelLen int                   `json:"incomingChannelLen"`
	IncomingChannelCap int                   `json:"incomingChannelCap"`
	OutgoingChannelLen int                   `json:"outgoingChannelLen"`
	OutgoingChannelCap int                   `json:"outgoingChannelCap"`
}

// debugInfo returns a snapshot of federation state for /debug/federation/shards.
func (sp *SyncProtocol) debugInfo() DebugInfo {
	lastMsgMillis := sp.health.LastMessageReceived.Load()
	var lastMsg time.Time
	if lastMsgMillis > 0 {
		lastMsg = time.UnixMilli(lastMsgMillis)
	}

	info := DebugInfo{
		LocalClusterID:      sp.localClusterID,
		IsLeader:            sp.isLeader.Load(),
		BootstrapComplete:   sp.health.BootstrapComplete.Load(),
		TransportConnected:  sp.health.TransportConnected.Load(),
		LastMessageReceived: lastMsg,
		LocalVersion:        sp.versionVector.get(sp.localClusterID),
		AcceptedVersions:    sp.store.versionVector.copy(),
		IncomingChannelLen:  len(sp.incomingCh),
		IncomingChannelCap:  cap(sp.incomingCh),
		OutgoingChannelLen:  len(sp.outgoingCh),
		OutgoingChannelCap:  cap(sp.outgoingCh),
		Shards:              sp.store.debugShards(),
	}
	return info
}

// debugShards returns a snapshot of all shards in the store.
func (s *federationStore) debugShards() []DebugShardInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]DebugShardInfo, 0, len(s.shards))
	for cid, shard := range s.shards {
		out = append(out, DebugShardInfo{
			ClusterID:      cid,
			Version:        shard.Version,
			LastSeen:       shard.LastSeen,
			Tombstoned:     shard.Tombstoned,
			Services:       len(shard.Services),
			NetworkGateway: shard.NetworkGateway,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClusterID < out[j].ClusterID })
	return out
}

// HandleDebugShards is the HTTP handler for /debug/federation/shards. It
// returns the federation store snapshot as JSON. Read-only.
func (sp *SyncProtocol) HandleDebugShards(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(sp.debugInfo()); err != nil {
		// JSON encoding cannot fail here for our shape; fall back to a text error.
		http.Error(w, fmt.Sprintf("encode: %v", err), http.StatusInternalServerError)
	}
}
