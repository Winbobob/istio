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
	"fmt"
	"testing"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/test"
	"istio.io/istio/pkg/test/util/assert"
	"istio.io/istio/pkg/workloadapi"
)

func gatewayWorkload(nw, addr string) *model.WorkloadInfo {
	return &model.WorkloadInfo{Workload: &workloadapi.Workload{
		Uid:       fmt.Sprintf("NetworkGateway/%s/%s/15008", nw, addr),
		Name:      "gw-" + nw,
		Namespace: "istio-system",
		Network:   nw,
		ClusterId: "c-" + nw,
		Hostname:  addr,
	}}
}

func podWorkload(name string) *model.WorkloadInfo {
	return &model.WorkloadInfo{Workload: &workloadapi.Workload{
		Uid:       "cluster/Pod/default/" + name,
		Name:      name,
		Namespace: "default",
		Network:   "net-local",
	}}
}

// AmbientNetworkGateways reads the gateway index instead of listing every
// workload. It must return exactly what the full scan returned, including
// after gateways are added and removed.
func TestAmbientNetworkGateways_IndexMatchesFullScan(t *testing.T) {
	stop := test.NewStop(t)
	wls := krt.NewMutableCollection(nil, []*model.WorkloadInfo{
		podWorkload("a"), podWorkload("b"), gatewayWorkload("net-1", "1.1.1.1"), podWorkload("c"),
	}, krt.WithStop(stop), krt.WithName("TestWorkloads"))
	gws := krt.NewStaticCollection[NetworkGateway](nil, nil, krt.WithStop(stop), krt.WithName("TestNetworkGateways"))

	a := &index{
		workloads: workloadsCollection{Collection: wls.AsCollection(), NetworkGateways: newNetworkGatewayWorkloadIndex(wls.AsCollection())},
		networks:  NetworkCollections{NetworkGateways: gws},
	}
	// Reference result: scan every workload.
	fullScan := func() []model.NetworkGateway {
		var out []model.NetworkGateway
		for _, w := range wls.List() {
			if gw, ok := ambientNetworkGatewayFromWorkload(w); ok {
				out = append(out, gw)
			}
		}
		return model.SortGateways(out)
	}

	check := func(wantAddrs ...string) {
		t.Helper()
		got := a.AmbientNetworkGateways()
		assert.Equal(t, got, fullScan())
		var addrs []string
		for _, g := range got {
			addrs = append(addrs, g.Addr)
		}
		assert.Equal(t, addrs, wantAddrs)
	}

	check("1.1.1.1")

	wls.UpdateObject(gatewayWorkload("net-2", "2.2.2.2"))
	check("1.1.1.1", "2.2.2.2")

	wls.DeleteObject(gatewayWorkload("net-1", "1.1.1.1").ResourceName())
	check("2.2.2.2")

	wls.UpdateObject(podWorkload("d"))
	check("2.2.2.2")
}
