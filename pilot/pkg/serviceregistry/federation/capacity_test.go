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
	"testing"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/test/util/assert"
)

// splitHorizonCapacities returns, per service key, the capacity of the
// split-horizon workload projected for it.
func splitHorizonCapacities(wls []*model.WorkloadInfo) map[string]uint32 {
	out := map[string]uint32{}
	for _, wl := range wls {
		if isGatewayWorkload(wl) {
			continue
		}
		for svcKey := range wl.Workload.Services {
			out[svcKey] = wl.Workload.GetCapacity().GetValue()
		}
	}
	return out
}

func TestProjection_SkipsServiceWithNoBackends(t *testing.T) {
	store := newTestStore("local")
	msg := makeSyncMessage("cluster-a", 1, true,
		"empty.ns1.svc.cluster.local", "busy.ns1.svc.cluster.local", "nocap.ns1.svc.cluster.local")
	msg.Capacities = map[string]uint32{
		"ns1/empty.ns1.svc.cluster.local": 0, // received with no capacity on the wire
		"ns1/busy.ns1.svc.cluster.local":  3,
		// nocap.ns1: no entry
	}
	handleSync(store, msg)

	svcs, wls := store.getFederationState()
	// Every service is still published, so the name and VIP keep resolving.
	assert.Equal(t, len(svcs), 3)

	caps := splitHorizonCapacities(wls)
	for _, k := range []string{"ns1/empty.ns1.svc.cluster.local", "ns1/nocap.ns1.svc.cluster.local"} {
		if _, ok := caps[k]; ok {
			t.Fatalf("%s has no backends and must not get a split-horizon workload: %v", k, caps)
		}
	}
	assert.Equal(t, caps["ns1/busy.ns1.svc.cluster.local"], uint32(3))
}

func TestProjection_BackendsComeAndGo(t *testing.T) {
	store := newTestStore("local")
	host := "svc.ns1.svc.cluster.local"
	key := "ns1/" + host

	for i, tc := range []struct {
		capacity uint32
		routed   bool
	}{
		{0, false},
		{2, true},
		{0, false},
	} {
		msg := makeSyncMessage("cluster-a", uint64(i+1), true, host)
		msg.Capacities = map[string]uint32{key: tc.capacity}
		handleSync(store, msg)
		_, wls := store.getFederationState()
		got, ok := splitHorizonCapacities(wls)[key]
		assert.Equal(t, ok, tc.routed)
		if tc.routed {
			assert.Equal(t, got, tc.capacity)
		}
	}
}
