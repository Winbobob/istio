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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"istio.io/api/annotation"
	"istio.io/api/label"
	meshconfig "istio.io/api/mesh/v1alpha1"
	"istio.io/istio/pilot/pkg/features"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/serviceregistry/memory"
	"istio.io/istio/pilot/pkg/serviceregistry/provider"
	"istio.io/istio/pilot/pkg/serviceregistry/util/xdsfake"
	"istio.io/istio/pilot/pkg/util/protoconv"
	xdsendpoints "istio.io/istio/pilot/pkg/xds/endpoints"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/config/constants"
	"istio.io/istio/pkg/config/host"
	configlabels "istio.io/istio/pkg/config/labels"
	"istio.io/istio/pkg/config/mesh/meshwatcher"
	"istio.io/istio/pkg/config/protocol"
	"istio.io/istio/pkg/config/schema/kind"
	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/maps"
	"istio.io/istio/pkg/network"
	"istio.io/istio/pkg/slices"
	"istio.io/istio/pkg/test"
	"istio.io/istio/pkg/test/util/assert"
	"istio.io/istio/pkg/workloadapi"
)

type federatedEndpointBuilderDiscovery struct {
	*memory.ServiceDiscovery
	services        []*model.Service
	serviceInfos    map[string]*model.ServiceInfo
	ambientGateways []model.NetworkGateway
}

func (f *federatedEndpointBuilderDiscovery) Services() []*model.Service {
	return f.services
}

func (f *federatedEndpointBuilderDiscovery) GetService(hostname host.Name) *model.Service {
	for _, svc := range f.services {
		if svc.Hostname == hostname {
			return svc
		}
	}
	return nil
}

func (f *federatedEndpointBuilderDiscovery) GetProxyServiceTargets(*model.Proxy) []model.ServiceTarget {
	res := make([]model.ServiceTarget, 0, len(f.services))
	for _, svc := range f.services {
		res = append(res, model.ServiceTarget{Service: svc})
	}
	return res
}

func (f *federatedEndpointBuilderDiscovery) GetProxyWorkloadLabels(*model.Proxy) configlabels.Instance {
	return nil
}

func (f *federatedEndpointBuilderDiscovery) NetworkGateways() []model.NetworkGateway {
	return nil
}

func (f *federatedEndpointBuilderDiscovery) MCSServices() []model.MCSServiceInfo {
	return nil
}

func (f *federatedEndpointBuilderDiscovery) ServiceInfo(key string) *model.ServiceInfo {
	return f.serviceInfos[key]
}

func (f *federatedEndpointBuilderDiscovery) AmbientNetworkGateways() []model.NetworkGateway {
	return f.ambientGateways
}

func TestWaypointInterop(t *testing.T) {
	for _, tt := range []struct {
		name          string
		enableFeature *bool
		serviceLabels map[string]string
	}{
		{
			name:          "IngressUseWaypoint",
			enableFeature: nil,
			serviceLabels: map[string]string{"istio.io/ingress-use-waypoint": "true"},
		},
		{
			name:          "AmbientMultiNetwork",
			enableFeature: &features.EnableAmbientMultiNetwork,
			serviceLabels: map[string]string{"istio.io/global": "true"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.enableFeature != nil {
				test.SetForTest(t, tt.enableFeature, true)
			}
			// Test that we can get updates for EDS when we have service bound waypoints.
			s := newAmbientTestServer(t, testC, testNW, "")
			// the two types return different keys.. rename to make it more clear
			addressUpdate := s.svcXdsName
			edsUpdate := s.hostnameForService
			assertServicesWithWaypoint := func(want ...string) {
				t.Helper()
				fetch := func() []string {
					got := s.ServicesWithWaypoint(s.svcXdsName("svc1"))
					return slices.Map(got, func(e model.ServiceWaypointInfo) string {
						return e.Service.Hostname + "/" + e.WaypointHostname
					})
				}
				assert.EventuallyEqual(t, fetch, want)
			}

			s.addService(t, "svc1",
				maps.MergeCopy(map[string]string{label.IoIstioUseWaypoint.Name: "wp-svc"}, tt.serviceLabels),
				map[string]string{},
				[]int32{80}, map[string]string{"app": "a"}, "10.0.0.2")
			s.assertEvent(t, addressUpdate("svc1"))
			assertServicesWithWaypoint()

			// Add waypoint...
			// We should get a service update for EDS to update
			// First we will test an IP-based waypoint...
			s.addWaypointSpecificAddress(t, "10.0.0.1", "", "wp-svc", constants.AllTraffic, true)
			s.addService(t, "wp-svc",
				map[string]string{},
				map[string]string{},
				[]int32{80}, map[string]string{"app": "waypoint"}, "10.0.0.1")
			s.assertEvent(t, addressUpdate("wp-svc"), addressUpdate("svc1"), edsUpdate("svc1"))
			assertServicesWithWaypoint(s.hostnameForService("svc1") + "/" + s.hostnameForService("wp-svc"))

			// add a waypoint instance... we should get an EDS update
			s.addPods(t, "127.0.0.4", "wp-pod1", "wp-sa", map[string]string{"app": "waypoint"}, nil, true, corev1.PodRunning)
			s.assertEvent(t, s.podXdsName("wp-pod1"), edsUpdate("svc1"))
			s.addPods(t, "127.0.0.5", "wp-pod2", "wp-sa", map[string]string{"app": "waypoint"}, nil, true, corev1.PodRunning)
			s.assertEvent(t, s.podXdsName("wp-pod2"), edsUpdate("svc1"))
			assertServicesWithWaypoint(s.hostnameForService("svc1") + "/" + s.hostnameForService("wp-svc"))

			// now we are going to change to a different waypoint, this will be hostname based
			s.addWaypointSpecificAddress(t, "", "example.com", "wp-svc-host", constants.AllTraffic, true)
			s.addService(t, "svc1",
				maps.MergeCopy(map[string]string{label.IoIstioUseWaypoint.Name: "wp-svc-host"}, tt.serviceLabels),
				map[string]string{},
				[]int32{80}, map[string]string{"app": "a"}, "10.0.0.2")
			s.assertEvent(t, addressUpdate("svc1"), edsUpdate("svc1"))
			assertServicesWithWaypoint(s.hostnameForService("svc1") + "/" + "example.com")
		})
	}
}

// TestWaypointInteropCanary verifies canary waypoint pod changes trigger EDS for the bound service.
func TestWaypointInteropCanary(t *testing.T) {
	s := newAmbientTestServer(t, testC, testNW, "")
	addressUpdate := s.svcXdsName
	edsUpdate := s.hostnameForService

	// Primary and canary waypoint services (IP based), no pods yet.
	s.addWaypointSpecificAddress(t, "10.0.0.1", "", "wp-primary", constants.AllTraffic, true)
	s.addService(t, "wp-primary",
		map[string]string{},
		map[string]string{},
		[]int32{80}, map[string]string{"app": "primary"}, "10.0.0.1")
	s.assertEvent(t, addressUpdate("wp-primary"))
	s.addWaypointSpecificAddress(t, "10.0.0.3", "", "wp-canary", constants.AllTraffic, true)
	s.addService(t, "wp-canary",
		map[string]string{},
		map[string]string{},
		[]int32{80}, map[string]string{"app": "canary"}, "10.0.0.3")
	s.assertEvent(t, addressUpdate("wp-canary"))

	// Bind svc1 to the primary waypoint with a weighted canary.
	s.addService(t, "svc1",
		map[string]string{
			label.IoIstioUseWaypoint.Name:       "wp-primary",
			label.IoIstioUseWaypointCanary.Name: "wp-canary",
			"istio.io/ingress-use-waypoint":     "true",
		},
		map[string]string{annotation.IoIstioUseWaypointCanaryWeight.Name: "20"},
		[]int32{80}, map[string]string{"app": "a"}, "10.0.0.2")
	s.assertEvent(t, addressUpdate("svc1"), edsUpdate("svc1"))

	// The weighted set should resolve to both waypoints.
	assert.EventuallyEqual(t, func() int {
		got := s.ServicesWithWaypoint(s.svcXdsName("svc1"))
		if len(got) == 0 {
			return 0
		}
		return len(got[0].WeightedWaypoints)
	}, 2)

	// Primary waypoint pod changes still push EDS for svc1.
	s.addPods(t, "127.0.0.10", "primary-pod", "primary-sa", map[string]string{"app": "primary"}, nil, true, corev1.PodRunning)
	s.assertEvent(t, s.podXdsName("primary-pod"), edsUpdate("svc1"))

	// Canary waypoint pod changes must also push EDS for svc1.
	s.addPods(t, "127.0.0.11", "canary-pod", "canary-sa", map[string]string{"app": "canary"}, nil, true, corev1.PodRunning)
	s.assertEvent(t, s.podXdsName("canary-pod"), edsUpdate("svc1"))

	// A weight change must push EDS for svc1.
	s.addService(t, "svc1",
		map[string]string{
			label.IoIstioUseWaypoint.Name:       "wp-primary",
			label.IoIstioUseWaypointCanary.Name: "wp-canary",
			"istio.io/ingress-use-waypoint":     "true",
		},
		map[string]string{annotation.IoIstioUseWaypointCanaryWeight.Name: "50"},
		[]int32{80}, map[string]string{"app": "a"}, "10.0.0.2")
	s.assertEvent(t, addressUpdate("svc1"), edsUpdate("svc1"))
}

func TestFederatedAmbientNetworkGatewayDiscovery(t *testing.T) {
	test.SetForTest(t, &features.EnableAmbient, true)
	test.SetForTest(t, &features.EnableAmbientMultiNetwork, true)
	test.SetForTest(t, &features.EnableAmbientWaypointMultiNetwork, true)

	stopCh := make(chan struct{})
	t.Cleanup(func() {
		close(stopCh)
	})

	fedCluster := cluster.ID("remote-cluster")
	fedNetwork := "network-remote"
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	fedSource := NewFederationSource(fedCluster, stopCh)
	s := newAmbientTestServerFromOptions(t, testNW, Options{
		ClusterID:        testC,
		FederationSource: fedSource,
	}, true)

	service := &workloadapi.Service{
		Name:      "svc1",
		Namespace: testNS,
		Hostname:  s.hostnameForService("svc1"),
		Addresses: []*workloadapi.NetworkAddress{{
			Network: fedNetwork,
			Address: []byte{10, 0, 0, 11},
		}},
		Ports: []*workloadapi.Port{{
			ServicePort: 80,
			TargetPort:  8080,
		}},
		Waypoint: &workloadapi.GatewayAddress{
			Destination: &workloadapi.GatewayAddress_Hostname{
				Hostname: &workloadapi.NamespacedHostname{
					Namespace: testNS,
					Hostname:  s.hostnameForService("wp-svc"),
				},
			},
			HboneMtlsPort: 15008,
		},
	}
	serviceAddr := &workloadapi.Address{Type: &workloadapi.Address_Service{Service: service}}
	serviceMarshaled := protoconv.MessageToAny(serviceAddr)
	fedSource.UpsertService(&model.ServiceInfo{
		Service:          service,
		Scope:            model.Global,
		CreationTime:     createdAt,
		MarshaledAddress: serviceMarshaled,
		AsAddress: model.AddressInfo{
			Address:   serviceAddr,
			Marshaled: serviceMarshaled,
		},
	})

	workload := &workloadapi.Workload{
		Uid:            "NetworkGateway/network-remote/172.18.0.10/15008",
		Name:           "NetworkGateway/network-remote/172.18.0.10/15008",
		Namespace:      constants.IstioSystemNamespace,
		Network:        fedNetwork,
		ClusterId:      fedCluster.String(),
		ServiceAccount: "istio-eastwestgateway",
		Addresses:      [][]byte{parseIP("172.18.0.10")},
	}
	addr := &workloadapi.Address{Type: &workloadapi.Address_Workload{Workload: workload}}
	marshaled := protoconv.MessageToAny(addr)

	fedSource.UpsertWorkload(&model.WorkloadInfo{
		Workload:         workload,
		Source:           model.TypedObject{Kind: kind.KubernetesGateway},
		CreationTime:     createdAt,
		MarshaledAddress: marshaled,
		AsAddress: model.AddressInfo{
			Address:   addr,
			Marshaled: marshaled,
		},
	})
	s.assertEvent(t, workload.Uid)

	assert.EventuallyEqual(t, func() []model.NetworkGateway {
		return s.AmbientNetworkGateways()
	}, []model.NetworkGateway{{
		Network:   network.ID(fedNetwork),
		Cluster:   fedCluster,
		Addr:      "172.18.0.10",
		HBONEPort: 15008,
		ServiceAccount: types.NamespacedName{
			Namespace: constants.IstioSystemNamespace,
			Name:      "istio-eastwestgateway",
		},
	}})

	serviceKey := fmt.Sprintf("%s/%s", testNS, s.hostnameForService("svc1"))
	assert.EventuallyEqual(t, func() bool {
		return s.ServiceInfo(serviceKey) != nil
	}, true)

	endpointIndex := model.NewEndpointIndex(model.NewXdsCache())
	shards, _ := endpointIndex.GetOrCreateEndpointShard(string(service.Hostname), testNS)
	shards.Lock()
	shards.Shards[model.ShardKey{Cluster: fedCluster}] = []*model.IstioEndpoint{{
		Addresses:       []string{"240.0.0.1"},
		Network:         network.ID(fedNetwork),
		Locality:        model.Locality{ClusterID: fedCluster},
		ServicePortName: "http",
		EndpointPort:    8080,
		HostName:        string(service.Hostname),
		Namespace:       testNS,
		TLSMode:         model.IstioMutualTLSModeLabel,
	}}
	shards.Unlock()

	modelSvc := &model.Service{
		Hostname: host.Name(service.Hostname),
		Attributes: model.ServiceAttributes{
			Name:      service.Name,
			Namespace: service.Namespace,
		},
		Ports: model.PortList{{Port: 80, Protocol: protocol.HTTP, Name: "http"}},
	}
	env := model.NewEnvironment()
	configStore := model.NewFakeStore()
	env.ConfigStore = configStore
	env.Watcher = meshwatcher.NewTestWatcher(&meshconfig.MeshConfig{RootNamespace: constants.IstioSystemNamespace})
	env.NetworksWatcher = meshwatcher.NewFixedNetworksWatcher(nil)
	env.AmbientIndexes = s.index
	env.VirtualServiceController = model.NewVirtualServiceController(
		configStore,
		model.VSControllerOptions{KrtDebugger: krt.GlobalDebugHandler},
		env.Watcher,
	)
	stop := test.NewStop(t)
	go configStore.Run(stop)
	go env.VirtualServiceController.Run(stop)
	kube.WaitForCacheSync("test", stop, configStore.HasSynced)
	kube.WaitForCacheSync("test", stop, env.VirtualServiceController.HasSynced)
	env.ServiceDiscovery = &federatedEndpointBuilderDiscovery{
		ServiceDiscovery: memory.NewServiceDiscovery(),
		services:         []*model.Service{modelSvc},
		serviceInfos: map[string]*model.ServiceInfo{
			serviceKey: s.ServiceInfo(serviceKey),
		},
		ambientGateways: s.AmbientNetworkGateways(),
	}
	if err := env.InitNetworksManager(xdsfake.NewFakeXDS()); err != nil {
		t.Fatal(err)
	}
	env.Init()

	push := model.NewPushContext()
	push.InitContext(env, nil, nil)
	env.SetPushContext(push)
	proxy := &model.Proxy{
		Type: model.Waypoint,
		Metadata: &model.NodeMetadata{
			Namespace: testNS,
			Network:   testNW,
			ClusterID: testC,
		},
		Labels: map[string]string{label.GatewayManaged.Name: constants.ManagedGatewayMeshControllerLabel},
	}
	proxy.SetSidecarScope(push)
	clusterName := model.BuildSubsetKey(model.TrafficDirectionInboundVIP, "http", host.Name(service.Hostname), 80)
	builder := xdsendpoints.NewCDSEndpointBuilder(
		proxy,
		push,
		clusterName,
		model.TrafficDirectionInboundVIP,
		"http",
		host.Name(service.Hostname),
		80,
		modelSvc,
		nil,
	)
	cla := builder.BuildClusterLoadAssignment(endpointIndex)
	totalEndpoints := 0
	for _, locality := range cla.Endpoints {
		totalEndpoints += len(locality.LbEndpoints)
	}
	if totalEndpoints == 0 {
		t.Fatalf("expected non-empty inbound-vip EDS for federated service, got %#v", cla.Endpoints)
	}
}

func TestFederatedWaypointInteropPublishesSyntheticShardEndpoints(t *testing.T) {
	test.SetForTest(t, &features.EnableAmbientMultiNetwork, true)

	stopCh := make(chan struct{})
	t.Cleanup(func() {
		close(stopCh)
	})

	fedCluster := cluster.ID("remote-cluster")
	fedSource := NewFederationSource(fedCluster, stopCh)
	endpointIndex := model.NewEndpointIndex(model.NewXdsCache())
	updater := xdsfake.NewWithDelegate(model.NewEndpointIndexUpdater(endpointIndex))
	s := newAmbientTestServerFromOptions(t, testNW, Options{
		ClusterID:        testC,
		FederationSource: fedSource,
		XDSUpdater:       updater,
	}, true)

	s.addWaypointSpecificAddress(t, "10.0.0.1", "", "wp-svc", constants.AllTraffic, true)
	s.addService(t, "wp-svc",
		map[string]string{},
		map[string]string{},
		[]int32{80}, map[string]string{"app": "waypoint"}, "10.0.0.1")
	s.addService(t, "svc1",
		map[string]string{
			label.IoIstioUseWaypoint.Name: "wp-svc",
			"istio.io/global":             "true",
		},
		map[string]string{},
		[]int32{80}, map[string]string{"app": "a"}, "10.0.0.2")

	serviceKey := fmt.Sprintf("%s/%s", testNS, s.hostnameForService("svc1"))
	fedSource.UpsertWorkload(precomputeWorkload(&model.WorkloadInfo{
		Workload: &workloadapi.Workload{
			Uid:            fmt.Sprintf("%s/SplitHorizonWorkload/%s/east-west/%s", fedCluster, testNS, s.hostnameForService("svc1")),
			Name:           "remote-split-horizon",
			Namespace:      testNS,
			Network:        "network-remote",
			ClusterId:      fedCluster.String(),
			ServiceAccount: "remote-sa",
			TunnelProtocol: workloadapi.TunnelProtocol_HBONE,
			NetworkGateway: &workloadapi.GatewayAddress{
				Destination: &workloadapi.GatewayAddress_Address{
					Address: &workloadapi.NetworkAddress{
						Network: "network-remote",
						Address: []byte{172, 18, 0, 10},
					},
				},
				HboneMtlsPort: 15008,
			},
			Services: map[string]*workloadapi.PortList{
				serviceKey: {
					Ports: []*workloadapi.Port{{
						ServicePort: 80,
						TargetPort:  8080,
					}},
				},
			},
		},
		Source: model.TypedObject{Kind: kind.KubernetesGateway},
		Labels: map[string]string{},
	}))

	assert.EventuallyEqual(t, func() []*model.IstioEndpoint {
		shards, found := endpointIndex.ShardsForService(s.hostnameForService("svc1"), testNS)
		if !found {
			return nil
		}
		shards.RLock()
		defer shards.RUnlock()
		return slices.Map(shards.Shards[model.ShardKey{Cluster: fedCluster, Provider: provider.Kubernetes}], func(ep *model.IstioEndpoint) *model.IstioEndpoint {
			return ep.ShallowCopy()
		})
	}, []*model.IstioEndpoint{{
		Addresses:       []string{"10.0.0.2"},
		EndpointPort:    8080,
		ServicePortName: "tcp-port",
		Network:         network.ID("network-remote"),
		Locality: model.Locality{
			ClusterID: fedCluster,
		},
		Labels:         map[string]string{},
		ServiceAccount: "remote-sa",
		TLSMode:        model.IstioMutualTLSModeLabel,
		WorkloadName:   "remote-split-horizon",
		Namespace:      testNS,
		HostName:       s.hostnameForService("svc1"),
		HealthStatus:   model.Healthy,
	}})
}

func TestWaypointAddressSharedByTwoServices(t *testing.T) {
	const collideHost = "collide.istio.io"

	bindServiceToWaypoint := func(t *testing.T, alsoReportHostname bool) *ambientTestServer {
		t.Helper()
		s := newAmbientTestServer(t, testC, testNW, "")
		waypointHostname := ""
		if alsoReportHostname {
			waypointHostname = s.hostnameForService("wp-svc")
		}
		s.addWaypointSpecificAddress(t, "10.0.0.1", waypointHostname, "wp-svc", constants.AllTraffic, true)
		s.addService(t, "wp-svc",
			map[string]string{},
			map[string]string{},
			[]int32{80}, map[string]string{"app": "waypoint"}, "10.0.0.1")
		s.assertEvent(t, s.svcXdsName("wp-svc"))

		s.addService(t, "svc1",
			map[string]string{
				label.IoIstioUseWaypoint.Name:   "wp-svc",
				"istio.io/ingress-use-waypoint": "true",
			},
			map[string]string{},
			[]int32{80}, map[string]string{"app": "a"}, "10.0.0.2")
		s.assertEvent(t, s.svcXdsName("svc1"), s.hostnameForService("svc1"))

		s.addPods(t, "127.0.0.4", "wp-pod1", "wp-sa", map[string]string{"app": "waypoint"}, nil, true, corev1.PodRunning)
		s.assertEvent(t, s.podXdsName("wp-pod1"), s.hostnameForService("svc1"))
		return s
	}

	t.Run("waypoint addressed by IP only", func(t *testing.T) {
		s := bindServiceToWaypoint(t, false)

		s.addServiceEntry(t, collideHost, []string{"10.0.0.1"}, "collide", testNS,
			map[string]string{"app": "collide"}, nil)
		s.assertEvent(t, s.xdsNamespacedHostname(collideHost), s.hostnameForService("svc1"))

		s.addPods(t, "127.0.0.5", "wp-pod2", "wp-sa", map[string]string{"app": "waypoint"}, nil, true, corev1.PodRunning)
		s.fx.StrictMatchOrFail(t, xdsfake.Event{Type: "xds", ID: s.podXdsName("wp-pod2")})
		s.assertNoMatchingEvent(t, s.hostnameForService("svc1"))

		s.deleteServiceEntry(t, "collide")
		s.assertEvent(t, s.xdsNamespacedHostname(collideHost), s.hostnameForService("svc1"))

		s.addPods(t, "127.0.0.6", "wp-pod3", "wp-sa", map[string]string{"app": "waypoint"}, nil, true, corev1.PodRunning)
		s.assertEvent(t, s.podXdsName("wp-pod3"), s.hostnameForService("svc1"))
	})

	t.Run("waypoint also reporting hostname", func(t *testing.T) {
		s := bindServiceToWaypoint(t, true)

		s.addServiceEntry(t, collideHost, []string{"10.0.0.1"}, "collide", testNS,
			map[string]string{"app": "collide"}, nil)
		s.fx.StrictMatchOrFail(t, xdsfake.Event{Type: "xds", ID: s.xdsNamespacedHostname(collideHost)})
		s.assertNoMatchingEvent(t, s.hostnameForService("svc1"))

		s.addPods(t, "127.0.0.5", "wp-pod2", "wp-sa", map[string]string{"app": "waypoint"}, nil, true, corev1.PodRunning)
		s.assertEvent(t, s.podXdsName("wp-pod2"), s.hostnameForService("svc1"))
	})
}
