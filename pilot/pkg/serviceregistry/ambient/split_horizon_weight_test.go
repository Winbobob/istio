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

package ambient

import (
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/test/util/assert"
	"istio.io/istio/pkg/workloadapi"
)

// Waypoint endpoints for remote clusters carry the split-horizon workload's
// capacity as their LB weight, so a cluster with more backends gets more
// traffic instead of an equal share.
func TestBuildSyntheticSplitHorizonEndpoints_WeightFromCapacity(t *testing.T) {
	svc := &model.ServiceInfo{
		Service: &workloadapi.Service{
			Hostname:  "svc.ns1.svc.cluster.local",
			Namespace: "ns1",
			Ports:     []*workloadapi.Port{{ServicePort: 80, TargetPort: 8080}},
		},
		PortNames: map[int32]model.ServicePortName{80: {PortName: "http"}},
	}
	ports := &workloadapi.PortList{Ports: []*workloadapi.Port{{ServicePort: 80, TargetPort: 8080}}}
	wl := func(name string, addr byte, capacity *wrapperspb.UInt32Value) *model.WorkloadInfo {
		return &model.WorkloadInfo{Workload: &workloadapi.Workload{
			Name:      name,
			Network:   "net-" + name,
			ClusterId: "c-" + name,
			Addresses: [][]byte{{10, 0, 0, addr}},
			Services:  map[string]*workloadapi.PortList{svc.ResourceName(): ports},
			Capacity:  capacity,
		}}
	}

	eps := buildSyntheticSplitHorizonEndpoints([]*model.WorkloadInfo{
		wl("big", 1, wrapperspb.UInt32(8)),
		wl("small", 2, wrapperspb.UInt32(2)),
	}, svc)

	weights := map[string]uint32{}
	for _, ep := range eps {
		weights[ep.WorkloadName] = ep.GetLoadBalancingWeight()
	}
	assert.Equal(t, weights, map[string]uint32{"big": 8, "small": 2})
}
