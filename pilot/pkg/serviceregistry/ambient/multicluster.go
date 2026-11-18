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
	"net/netip"
	"strings"

	"google.golang.org/protobuf/types/known/wrapperspb"
	v1 "k8s.io/api/core/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"istio.io/api/label"
	networkingclient "istio.io/client-go/pkg/apis/networking/v1"
	securityclient "istio.io/client-go/pkg/apis/security/v1"
	"istio.io/istio/pilot/pkg/features"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/serviceregistry/ambient/statusqueue"
	"istio.io/istio/pilot/pkg/serviceregistry/provider"
	labelutil "istio.io/istio/pilot/pkg/serviceregistry/util/label"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/config/constants"
	"istio.io/istio/pkg/config/schema/kind"
	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/kclient"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/kube/multicluster"
	"istio.io/istio/pkg/log"
	"istio.io/istio/pkg/maps"
	"istio.io/istio/pkg/network"
	"istio.io/istio/pkg/ptr"
	"istio.io/istio/pkg/slices"
	"istio.io/istio/pkg/spiffe"
	"istio.io/istio/pkg/util/protomarshal"
	"istio.io/istio/pkg/util/sets"
	"istio.io/istio/pkg/workloadapi"
)

func (a *index) buildGlobalCollections(
	localCluster multicluster.ClusterCollections,
	localAuthzPolicies krt.Collection[*securityclient.AuthorizationPolicy],
	localPeerAuths krt.Collection[*securityclient.PeerAuthentication],
	localGatewayClasses krt.Collection[*gatewayv1.GatewayClass],
	localWorkloadEntries krt.Collection[*networkingclient.WorkloadEntry],
	localServiceEntries krt.Collection[*networkingclient.ServiceEntry],
	localServiceEntryInformers kclient.Informer[*networkingclient.ServiceEntry],
	localServiceInformers kclient.Informer[*v1.Service],
	localAuthzInformers kclient.Informer[*securityclient.AuthorizationPolicy],
	localWorkloadEntryInformers kclient.Informer[*networkingclient.WorkloadEntry],
	options Options,
	opts krt.OptionsBuilder,
) {
	LocalPods := localCluster.Pods()
	LocalNamespaces := localCluster.Namespaces()
	LocalNodes := localCluster.Nodes()
	LocalGateways := localCluster.Gateways()

	LocalMeshConfig := options.MeshConfig

	LocalGatewaysWithCluster := krt.MapCollection(LocalGateways, func(obj *gatewayv1.Gateway) krt.ObjectWithCluster[*gatewayv1.Gateway] {
		return krt.ObjectWithCluster[*gatewayv1.Gateway]{
			ClusterID: localCluster.ID(),
			Object:    &obj,
		}
	}, opts.WithName("LocalGatewaysWithCluster")...)
	GlobalGatewaysWithCluster := multicluster.NestedCollectionFromLocalAndRemote(
		a.mcController,
		LocalGatewaysWithCluster,
		func(ctx krt.HandlerContext, c multicluster.ClusterCollections) *krt.Collection[krt.ObjectWithCluster[*gatewayv1.Gateway]] {
			if !kube.WaitForCacheSync(fmt.Sprintf("ambient/informer/gateways[%s]", c.ID()), a.stop, c.Gateways().HasSynced) {
				log.Warnf("Failed to sync gateways informer for cluster %s", c.ID())
				return nil
			}

			// N.B we're not using the opts.WithXXX pattern here since we want to be very obvious about which
			// stop is being used to shutdown the collection (it should always be the cluster stop, NEVER
			// the top-level stop associated with the ambient controller)
			opts := []krt.CollectionOption{
				krt.WithName(fmt.Sprintf("ambient/GatewaysWithCluster[%s]", c.ID())),
				krt.WithDebugging(opts.Debugger()),
				krt.WithStop(c.GetStop()),
			}
			return ptr.Of(krt.MapCollection(c.Gateways(), func(obj *gatewayv1.Gateway) krt.ObjectWithCluster[*gatewayv1.Gateway] {
				return krt.ObjectWithCluster[*gatewayv1.Gateway]{
					ClusterID: c.ID(),
					Object:    &obj,
				}
			}, opts...))
		}, "GatewaysWithCluster", opts,
	)

	globalGatewaysByCluster := multicluster.NestedCollectionIndexByCluster(GlobalGatewaysWithCluster)

	// Set up collections for remote clusters
	GlobalNetworks := buildGlobalNetworkCollections(
		a.mcController,
		LocalNamespaces,
		LocalGateways,
		GlobalGatewaysWithCluster,
		globalGatewaysByCluster,
		options,
		opts,
	)
	a.networks = GlobalNetworks
	builder := Builder{
		DomainSuffix: a.DomainSuffix,
		ClusterID:    a.ClusterID,
		Flags:        a.Flags,
		Networks:     GlobalNetworks,
	}

	// we need networks to be initialized before we can build waypoints
	LocalWaypoints := builder.WaypointsCollection(options.ClusterID, LocalGateways, localGatewayClasses, LocalPods, opts)

	// We need this because there may be services and waypoints on remote clusters that aren't represented
	// in our local config cluster
	GlobalWaypoints := GlobalWaypointsCollection(
		localCluster,
		LocalWaypoints,
		a.mcController,
		localGatewayClasses,
		GlobalNetworks,
		opts,
	)

	WaypointsByCluster := multicluster.NestedCollectionIndexByCluster(GlobalWaypoints)
	// AllPolicies includes peer-authentication converted policies
	AuthorizationPolicies, AllPolicies := a.buildAndRegisterPolicyCollections(
		localAuthzPolicies,
		localPeerAuths,
		LocalWaypoints,
		opts,
	)
	authPoliciesByNs := selectingWorkloadAuthzByNs(AuthorizationPolicies)
	LocalServiceEntryVisibility := model.ServiceEntryVisibilityCollection(LocalMeshConfig.AsCollection(), opts)

	LocalWorkloadServices := builder.ServicesCollection(
		localCluster.ID(),
		localCluster.Services(),
		localServiceEntries,
		LocalWaypoints,
		LocalNamespaces,
		LocalMeshConfig,
		LocalServiceEntryVisibility,
		opts,
		false, // Don't precompute here; these will just get merged into the global collection later
	)
	// Store the local-only services collection for outbound federation sync.
	// This prevents federation data from being re-broadcast.
	a.localServices = LocalWorkloadServices

	// All of this is local only, but we need to do it here so we don't have to rebuild collections in ambientindex
	if features.EnableAmbientStatus {
		serviceEntriesWriter := kclient.NewWriteClient[*networkingclient.ServiceEntry](localCluster.Client())
		servicesWriter := kclient.NewWriteClient[*v1.Service](localCluster.Client())
		authorizationPoliciesWriter := kclient.NewWriteClient[*securityclient.AuthorizationPolicy](localCluster.Client())

		WaypointPolicyStatus := WaypointPolicyStatusCollection(
			localAuthzPolicies,
			LocalWaypoints,
			localCluster.Services(),
			localServiceEntries,
			localGatewayClasses,
			LocalMeshConfig,
			LocalServiceEntryVisibility,
			localCluster.Namespaces(),
			opts,
		)
		statusQueue := statusqueue.NewQueue(options.StatusNotifier)
		statusqueue.Register(statusQueue, "istio-ambient-service", LocalWorkloadServices,
			func(info *model.ServiceInfo) (kclient.Patcher, map[string]model.Condition) {
				// Since we have 1 collection for multiple types, we need to split these out
				if info.Source.Kind == kind.ServiceEntry {
					return kclient.ToPatcher(serviceEntriesWriter), getConditions(info.Source.NamespacedName, localServiceEntryInformers)
				}
				return kclient.ToPatcher(servicesWriter), getConditions(info.Source.NamespacedName, localServiceInformers)
			})
		statusqueue.Register(statusQueue, "istio-ambient-ztunnel-policy", AuthorizationPolicies,
			func(pol model.WorkloadAuthorization) (kclient.Patcher, map[string]model.Condition) {
				return kclient.ToPatcher(authorizationPoliciesWriter), getConditions(pol.Source.NamespacedName, localAuthzInformers)
			})
		statusqueue.Register(statusQueue, "istio-ambient-waypoint-policy", WaypointPolicyStatus,
			func(pol model.WaypointPolicyStatus) (kclient.Patcher, map[string]model.Condition) {
				return kclient.ToPatcher(authorizationPoliciesWriter), getConditions(pol.Source.NamespacedName, localAuthzInformers)
			})
		a.statusQueue = statusQueue
	}
	// Now we get to collections where we can actually merge duplicate keys, so we can use nested collections
	GlobalWorkloadServicesWithCluster := GlobalNestedWorkloadServicesCollection(
		localCluster,
		LocalWorkloadServices,
		LocalWaypoints,
		a.mcController,
		localServiceEntries,
		GlobalWaypoints,
		WaypointsByCluster,
		LocalMeshConfig,
		GlobalNetworks,
		options.DomainSuffix,
		opts,
	)

	GlobalMergedWorkloadServicesWithCluster := krt.NestedJoinWithMergeCollection(
		GlobalWorkloadServicesWithCluster,
		mergeServiceInfosWithCluster(localCluster.ID()),
		opts.WithName("GlobalMergedServiceInfosWithCluster")...,
	)

	GlobalMergedWorkloadServices := krt.MapCollection(
		GlobalMergedWorkloadServicesWithCluster,
		func(obj krt.ObjectWithCluster[model.ServiceInfo]) *model.ServiceInfo {
			return obj.Object
		},
		opts.WithName("GlobalMergedServiceInfos")...,
	)

	// Merge federation services into the global merged services.
	// Federation services represent services discovered from remote clusters through federation.
	// They must be merged here (before SplitHorizonServices) so that SAN augmentation
	// picks up federation workloads from different networks.
	if options.FederationSource != nil {
		fedSvcs := options.FederationSource.Services()

		// Merge global + federation services. Global wins on key conflict.
		GlobalMergedWorkloadServices = krt.JoinCollection(
			[]krt.Collection[*model.ServiceInfo]{GlobalMergedWorkloadServices, fedSvcs},
			opts.WithName("GlobalMergedWithFederationServices")...,
		)
	}

	GlobalWorkloadServicesWithClusterByCluster := multicluster.NestedCollectionIndexByCluster(GlobalWorkloadServicesWithCluster)

	LocalNamespacesInfo := krt.NewCollection(LocalNamespaces, func(ctx krt.HandlerContext, ns *v1.Namespace) *model.NamespaceInfo {
		return &model.NamespaceInfo{
			Name:               ns.Name,
			IngressUseWaypoint: strings.EqualFold(ns.Labels["istio.io/ingress-use-waypoint"], "true"),
		}
	}, opts.WithName("LocalNamespacesInfo")...)

	LocalNodeLocality := NodesCollection(
		LocalNodes,
		opts.WithName("LocalNodeLocality")...,
	)
	GlobalNodeLocality := GlobalNodesCollection(localCluster, LocalNodeLocality, a.mcController, opts)
	GlobalNodeLocalityByCluster := multicluster.NestedCollectionIndexByCluster(GlobalNodeLocality)

	localPeerAuthsByNs := krt.NewNamespaceIndex(localPeerAuths)
	GlobalWorkloads := MergedGlobalWorkloadsCollection(
		localCluster,
		LocalWaypoints,
		LocalNodeLocality,
		a.mcController,
		localWorkloadEntries,
		localServiceEntries,
		GlobalNodeLocality,
		GlobalNodeLocalityByCluster,
		options.MeshConfig,
		authPoliciesByNs,
		localPeerAuthsByNs,
		GlobalWaypoints,
		WaypointsByCluster,
		LocalWorkloadServices,
		GlobalWorkloadServicesWithCluster,
		GlobalWorkloadServicesWithClusterByCluster,
		GlobalNetworks,
		options.ClusterID,
		options.Flags,
		options.DomainSuffix,
		opts,
	)
	if features.EnableAmbientStatus {
		workloadEntriesWriter := kclient.NewWriteClient[*networkingclient.WorkloadEntry](localCluster.Client())
		statusqueue.Register(a.statusQueue, "istio-ambient-workloadentry", GlobalWorkloads,
			func(info *model.WorkloadInfo) (kclient.Patcher, map[string]model.Condition) {
				if info.Source.Kind != kind.WorkloadEntry {
					return nil, nil
				}
				return kclient.ToPatcher(workloadEntriesWriter), getConditions(info.Source.NamespacedName, localWorkloadEntryInformers)
			})
	}

	// Index local+remote k8s-watched workloads by service key BEFORE merging federation workloads.
	// Used by outbound federation (ServiceWithSANs) to avoid re-broadcasting federation SANs.
	a.localWorkloadsByServiceKey = krt.NewIndex(GlobalWorkloads, "localService", func(o *model.WorkloadInfo) []string {
		return maps.Keys(o.Workload.Services)
	})
	// Capture the pre-federation-merge workloads collection so outbound federation
	// can register a per-workload change handler for incremental sync. Must be the
	// same (pre-fedWls-join) collection localWorkloadsByServiceKey is built from.
	a.localWorkloads = GlobalWorkloads

	// Merge federation workloads into GlobalWorkloads so SplitHorizonServices can
	// discover their SANs for mTLS identity verification. The coalescedWorkloads
	// pipeline will log warnings about missing network gateways for federation
	// networks (harmless: federation provides its own split-horizon workloads).
	var fedWls krt.Collection[*model.WorkloadInfo]
	if options.FederationSource != nil {
		fedWls = options.FederationSource.Workloads()

		GlobalWorkloads = krt.JoinCollection(
			[]krt.Collection[*model.WorkloadInfo]{GlobalWorkloads, fedWls},
			opts.With(krt.WithName("GlobalWithFederationWorkloads"), krt.WithJoinUnchecked())...,
		)
	}

	GlobalWorkloadServiceIndex := krt.NewIndex(GlobalWorkloads, "service", func(o *model.WorkloadInfo) []string {
		return maps.Keys(o.Workload.Services)
	})

	// This allows us to find all the workloads that correspond to a service, network pair.
	// This will allow us to build coalesced workloads that represent all the workloads in a remote network,
	// for a given service. This helps reduce the number of XDS objects we to proxies.
	workloadNetworkServiceIndex := krt.NewIndex(GlobalWorkloads, "network;service", func(o *model.WorkloadInfo) []string {
		res := make([]string, 0, len(o.Workload.Services))
		for svc := range o.Workload.Services {
			res = append(res, strings.Join([]string{o.Workload.Network, svc}, ";"))
		}
		return res
	})

	coalescedWorkloads := krt.NewPointerCollection(
		workloadNetworkServiceIndex.AsCollection(
			opts.WithName("workloadNetworkServiceIndex")...,
		),
		func(ctx krt.HandlerContext, i krt.IndexObject[string, *model.WorkloadInfo]) *model.WorkloadInfo {
			parts := strings.Split(i.Key, ";")
			if len(parts) != 2 {
				log.Errorf("Invalid key %s for SplitHorizonWorkloads, expected <network>;<service>", i.Key)
				return nil
			}
			networkID := network.ID(parts[0])
			localNetwork := GlobalNetworks.FetchLocalNetworkID(ctx).String()
			if networkID.String() == localNetwork {
				// We don't coalesce workloads for the local network
				return nil
			}

			svcName := parts[1]
			svc := krt.FetchOne(ctx, GlobalMergedWorkloadServices, krt.FilterKey(svcName))

			if svc == nil || *svc == nil {
				log.Errorf("Failed to find service %s to coalesce workloads", svcName)
				return nil
			}
			if (*svc).Scope != model.Global {
				return nil
			}

			capacity := uint32(0)
			for _, wl := range i.Objects {
				if wl.Workload.GetCapacity().GetValue() == 0 {
					capacity++
				} else {
					capacity += wl.Workload.Capacity.GetValue()
				}
			}
			gws := LookupNetworkGateway(ctx, networkID, GlobalNetworks.GatewaysByNetwork)
			meshCfg := krt.FetchOne(ctx, a.meshConfig.AsCollection())
			if meshCfg == nil {
				log.Errorf("Failed to find mesh config for network %s", networkID)
				return nil
			}
			if len(gws) == 0 {
				log.Warnf("No network gateway found for network %s", networkID)
				return nil
			}
			if len(gws) > 1 {
				// See https://github.com/istio/istio/issues/56960
				log.Warnf("Multiple gateways found for network %s, using the first one", networkID)
			}
			gw := gws[0]
			return a.createSplitHorizonWorkload(svcName, (*svc).Service, &gw, capacity, meshCfg)
		}, opts.WithName("CoalesedWorkloads")...,
	)
	networkLocalWorkloads := krt.NewPointerCollection(GlobalWorkloads, func(ctx krt.HandlerContext, wi *model.WorkloadInfo) *model.WorkloadInfo {
		if strings.HasPrefix(wi.Workload.Uid, "NetworkGateway/") {
			return wi
		}
		if wi.Workload.Network != GlobalNetworks.FetchLocalNetworkID(ctx).String() {
			return nil
		}
		return wi
	}, opts.WithName("NetworkLocalWorkloads")...)

	splitHorizonComponents := []krt.Collection[*model.WorkloadInfo]{
		coalescedWorkloads,
		networkLocalWorkloads,
	}

	// Merge federation workloads into splitHorizonComponents with dedup: only add the ones the other
	// sub-collections do not already contribute. networkLocalWorkloads already contributes every
	// NetworkGateway workload in any network plus every workload on the local network (in
	// single-network that includes the federated remote workloads that share the local network id).
	//
	// Without the dedup, in multi-network the NetworkGateway workload is double-added, and in
	// single-network the remote workloads that share the local network id are double-added. In the
	// checked SplitHorizonWorkloads join that creates a race:
	//   - if the workload is already held by both networkLocalWorkloads and fedWls when the join
	//     processes the events, both events are neutralized: the lower-priority fedWls add is dropped
	//     and the higher-priority networkLocalWorkloads add is converted to a no-op update
	//     (pkg/kube/krt/join.go), which the batched event filter then skips (pkg/kube/krt/util.go),
	//     so proxies like ztunnel never receive the workload.
	//   - if only one collection has converged when the add is processed, the add is forwarded and
	//     reaches ztunnel. Which case happens depends on goroutine scheduling, hence the race and its
	//     per-replica behavior.
	if fedWls != nil {
		federationCrossNetworkWorkloads := krt.NewPointerCollection(fedWls, func(ctx krt.HandlerContext, wi *model.WorkloadInfo) *model.WorkloadInfo {
			// NetworkGateway workloads and same-network workloads already arrive via
			// networkLocalWorkloads; only cross-network non-gateway workloads need the direct path.
			if strings.HasPrefix(wi.Workload.Uid, "NetworkGateway/") {
				return nil
			}
			if wi.Workload.Network == GlobalNetworks.FetchLocalNetworkID(ctx).String() {
				return nil
			}
			return wi
		}, opts.WithName("FederationCrossNetworkWorkloads")...)
		splitHorizonComponents = append(splitHorizonComponents, federationCrossNetworkWorkloads)
	}

	SplitHorizonWorkloads := krt.JoinCollection(
		splitHorizonComponents,
		opts.WithName("SplitHorizonWorkloads")...,
	)
	SplitHorizonWorkloads.RegisterBatch(krt.BatchedEventFilter(
		func(a *model.WorkloadInfo) *workloadapi.Workload {
			// Only trigger push if the XDS object changed; the rest is just for computation of others
			return a.Workload
		},
		PushXdsAddress(a.XDSUpdater, (*model.WorkloadInfo).ResourceName, (*model.WorkloadInfo).WaypointRef),
	), false)

	SplitHorizonWorkloadAddressIndex := krt.NewIndex(SplitHorizonWorkloads, "networkAddress", networkAddressFromWorkload)
	SplitHorizonWorkloadServiceIndex := krt.NewIndex(SplitHorizonWorkloads, "service", func(o *model.WorkloadInfo) []string {
		return maps.Keys(o.Workload.Services)
	})
	SyntheticSplitHorizonWorkloads := krt.NewPointerCollection(
		SplitHorizonWorkloads,
		func(ctx krt.HandlerContext, wi *model.WorkloadInfo) *model.WorkloadInfo {
			if wi.Workload == nil || len(wi.Workload.Services) == 0 {
				return nil
			}
			if strings.HasPrefix(wi.Workload.Uid, "NetworkGateway/") {
				return nil
			}
			if wi.Workload.Network == GlobalNetworks.FetchLocalNetworkID(ctx).String() {
				return nil
			}
			return wi
		},
		opts.WithName("SyntheticSplitHorizonWorkloads")...,
	)
	SyntheticSplitHorizonWorkloadServiceShardIndex := krt.NewIndex(
		SyntheticSplitHorizonWorkloads,
		"service;shard",
		func(o *model.WorkloadInfo) []string {
			if o.Workload == nil {
				return nil
			}
			res := make([]string, 0, len(o.Workload.Services))
			shard := syntheticSplitHorizonShardKey(o)
			for svc := range o.Workload.Services {
				res = append(res, syntheticSplitHorizonServiceShardIndexKey(svc, shard))
			}
			return res
		},
	)
	SplitHorizonWorkloadWaypointIndexHostname := krt.NewIndex(SplitHorizonWorkloads, "namespaceHostname", func(w *model.WorkloadInfo) []NamespaceHostname {
		// Filter out waypoints.
		if w.Labels[label.GatewayManaged.Name] == constants.ManagedGatewayMeshControllerLabel {
			return nil
		}
		waypoint := w.Workload.Waypoint
		if waypoint == nil {
			return nil
		}
		waypointAddress := waypoint.GetHostname()
		if waypointAddress == nil {
			return nil
		}

		return []NamespaceHostname{{
			Namespace: waypointAddress.Namespace,
			Hostname:  waypointAddress.Hostname,
		}}
	})
	SplitHorizonWorkloadWaypointIndexIP := krt.NewIndex(SplitHorizonWorkloads, "waypointIp", func(w *model.WorkloadInfo) []networkAddress {
		// Filter out waypoints.
		if w.Labels[label.GatewayManaged.Name] == constants.ManagedGatewayMeshControllerLabel {
			return nil
		}
		waypoint := w.Workload.Waypoint
		if waypoint == nil {
			return nil
		}

		waypointAddress := waypoint.GetAddress()
		if waypointAddress == nil {
			return nil
		}
		netip, _ := netip.AddrFromSlice(waypointAddress.Address)
		netaddr := networkAddress{
			network: waypointAddress.Network,
			ip:      netip.String(),
		}

		return []networkAddress{netaddr}
	})

	SplitHorizonServices := krt.NewPointerCollection(
		GlobalMergedWorkloadServices,
		func(ctx krt.HandlerContext, svc *model.ServiceInfo) *model.ServiceInfo {
			if svc.Scope != model.Global {
				return svc
			}

			wls := GlobalWorkloadServiceIndex.Fetch(ctx, svc.ResourceName())
			if len(wls) == 0 {
				return svc
			}

			// Since we merge the workloads in the remote cluster, we need to input the
			// service account sans of the remote workloads through the SubjectAlNames field.
			meshCfg := krt.FetchOne(ctx, a.meshConfig.AsCollection())
			if meshCfg == nil {
				log.Errorf("Failed to find mesh config")
				return nil
			}
			localNetwork := GlobalNetworks.FetchLocalNetworkID(ctx).String()
			sans := sets.String{}

			for _, wl := range wls {
				if wl.Workload.Network == localNetwork {
					continue
				}
				sans.Insert(spiffe.MustGenSpiffeURI(meshCfg.MeshConfig, wl.Workload.Namespace, wl.Workload.ServiceAccount))
			}
			if sans.IsEmpty() {
				return svc
			}
			sans = sans.Union(sets.New(svc.Service.SubjectAltNames...))

			newSvcInfo := cloneServiceInfoPreservingMetadata(svc)
			newSvcInfo.Service.SubjectAltNames = sans.UnsortedList()
			return precomputeService(newSvcInfo)
		},
		opts.WithName("SplitHorizonServices")...,
	)

	SplitHorizonServices.RegisterBatch(krt.BatchedEventFilter(
		func(a *model.ServiceInfo) *model.XDSServiceInfo {
			// Only trigger push if the XDS object changed; the rest is just for computation of others
			return &model.XDSServiceInfo{
				Service:            a.Service,
				DNSConnectStrategy: a.DNSConnectStrategy,
			}
		},
		PushXdsAddress(a.XDSUpdater, (*model.ServiceInfo).ResourceName, (*model.ServiceInfo).WaypointRef),
	), false)
	SyntheticSplitHorizonServiceShardEndpoints := krt.NewCollection(
		SyntheticSplitHorizonWorkloadServiceShardIndex.AsCollection(opts.WithName("SyntheticSplitHorizonWorkloadServiceShardIndex")...),
		func(ctx krt.HandlerContext, i krt.IndexObject[string, *model.WorkloadInfo]) *syntheticServiceShardEndpoints {
			serviceKey, shard, ok := parseSyntheticSplitHorizonServiceShardIndexKey(i.Key)
			if !ok {
				log.Errorf("Invalid key %s for SyntheticSplitHorizonWorkloadServiceShardIndex", i.Key)
				return nil
			}
			svc := krt.FetchOne(ctx, SplitHorizonServices, krt.FilterKey(serviceKey))
			if svc == nil || *svc == nil || !shouldPublishSyntheticSplitHorizonEDS(*svc) {
				return nil
			}
			namespace, hostname, found := strings.Cut(serviceKey, "/")
			if !found {
				log.Errorf("Invalid service key %s for SyntheticSplitHorizonWorkloadServiceShardIndex", serviceKey)
				return nil
			}
			return &syntheticServiceShardEndpoints{
				ServiceHostname: hostname,
				Namespace:       namespace,
				Shard:           shard,
				Endpoints:       buildSyntheticSplitHorizonEndpoints(i.Objects, *svc),
			}
		},
		opts.WithName("SyntheticSplitHorizonServiceShardEndpoints")...,
	)
	SyntheticSplitHorizonServiceShardEndpoints.RegisterBatch(func(events []krt.Event[syntheticServiceShardEndpoints]) {
		for _, event := range events {
			latest := event.Latest()
			endpoints := latest.Endpoints
			if event.Event == controllers.EventDelete {
				endpoints = nil
			}
			a.XDSUpdater.EDSUpdate(latest.Shard, latest.ServiceHostname, latest.Namespace, endpoints)
		}
	}, false)

	SplitHorizonServiceAddressIndex := krt.NewIndex(SplitHorizonServices, "serviceAddress", networkAddressFromService)
	SplitHorizonServiceInfosByOwningWaypointHostname := krt.NewIndex(SplitHorizonServices, "namespaceHostname", func(s *model.ServiceInfo) []NamespaceHostname {
		// Filter out waypoint services
		// TODO: we are looking at the *selector* -- we should be looking the labels themselves or something equivalent.
		if s.LabelSelector.Labels[label.GatewayManaged.Name] == constants.ManagedGatewayMeshControllerLabel {
			return nil
		}
		return serviceOwningWaypointHostnames(s)
	})
	SplitHorizonServiceInfosByOwningWaypointIP := krt.NewIndex(SplitHorizonServices, "owningWaypointIp", func(s *model.ServiceInfo) []networkAddress {
		// Filter out waypoint services
		if s.LabelSelector.Labels[label.GatewayManaged.Name] == constants.ManagedGatewayMeshControllerLabel {
			return nil
		}
		return serviceOwningWaypointAddresses(s)
	})

	if features.EnableIngressWaypointRouting {
		RegisterEdsShim(
			a.XDSUpdater,
			SplitHorizonWorkloads,
			LocalNamespacesInfo,
			SplitHorizonWorkloadServiceIndex,
			SplitHorizonServices,
			SplitHorizonServiceAddressIndex, // TODO: should we consider allowing ingress -> remote services?
			opts,
		)
	}
	a.namespaces = LocalNamespacesInfo

	a.workloads = workloadsCollection{
		Collection:               SplitHorizonWorkloads,
		ByAddress:                SplitHorizonWorkloadAddressIndex,
		ByServiceKey:             SplitHorizonWorkloadServiceIndex,
		ByOwningWaypointHostname: SplitHorizonWorkloadWaypointIndexHostname,
		ByOwningWaypointIP:       SplitHorizonWorkloadWaypointIndexIP,
		NetworkGateways:          newNetworkGatewayWorkloadIndex(SplitHorizonWorkloads),
	}

	a.services = servicesCollection{
		Collection:               SplitHorizonServices,
		ByAddress:                SplitHorizonServiceAddressIndex,
		ByOwningWaypointHostname: SplitHorizonServiceInfosByOwningWaypointHostname,
		ByOwningWaypointIP:       SplitHorizonServiceInfosByOwningWaypointIP,
	}
	a.authorizationPolicies = AllPolicies
	// TODO: Should this be the set of global waypoints?
	// Probably yes, but coming back to it in a follow up
	a.waypoints = waypointsCollection{
		Collection: LocalWaypoints,
	}
}

type simplePort struct {
	servicePort uint32
	targetPort  uint32
}
type portKey struct {
	source    string
	clusterID cluster.ID
}

type simpleNetworkAddress struct {
	network string
	ip      netip.Prefix
}

func networkAddressToSimple(a *workloadapi.NetworkAddress) simpleNetworkAddress {
	addr, _ := netip.AddrFromSlice(a.Address)
	prefix := netip.PrefixFrom(addr, addr.BitLen())
	if a.Length != nil {
		prefix = netip.PrefixFrom(addr, int(*a.Length))
	}
	return simpleNetworkAddress{
		network: a.Network,
		ip:      prefix,
	}
}

func mergeServiceInfosWithCluster(
	localClusterID cluster.ID,
) func(serviceInfos []krt.ObjectWithCluster[model.ServiceInfo]) *krt.ObjectWithCluster[model.ServiceInfo] {
	return func(serviceInfos []krt.ObjectWithCluster[model.ServiceInfo]) *krt.ObjectWithCluster[model.ServiceInfo] {
		svcInfosLen := len(serviceInfos)
		if svcInfosLen == 0 {
			return nil
		}

		// Precompute the svc info here
		if svcInfosLen == 1 {
			obj := serviceInfos[0]
			merged := *obj.Object
			return &krt.ObjectWithCluster[model.ServiceInfo]{
				ClusterID: obj.ClusterID,
				Object:    precomputeService(&merged),
			}
		}

		// If we can't find a local serviceinfo, we just take the first one.
		base := serviceInfos[0]
		for _, obj := range serviceInfos {
			if obj.ClusterID == localClusterID {
				base = obj
				// If there's a service entry that's considered external to the mesh, we
				// prioritize that
				if obj.Object.Source.Kind == kind.ServiceEntry {
					break
				}
			}
		}

		// If we have a locally scoped service
		if base.Object != nil && base.Object.Scope != model.Global {
			// and we did not find one in the local cluster, skip it
			if base.ClusterID != localClusterID {
				return nil
			}
			// otherwise, skip merging
			merged := *base.Object
			return &krt.ObjectWithCluster[model.ServiceInfo]{
				ClusterID: base.ClusterID,
				Object:    precomputeService(&merged),
			}
		}

		vips := sets.NewWithLength[simpleNetworkAddress](svcInfosLen)
		sans := sets.NewWithLength[string](svcInfosLen)
		ports := map[portKey]sets.Set[simplePort]{}
		workloadPortsToSimplePort := func(p *workloadapi.Port) simplePort {
			return simplePort{
				servicePort: p.ServicePort,
				targetPort:  p.TargetPort,
			}
		}
		for _, obj := range serviceInfos {
			if obj.Object == nil {
				continue
			}

			ports[portKey{
				source:    obj.Object.Source.String(),
				clusterID: obj.ClusterID,
			}] = sets.New(slices.Map(obj.Object.Service.Ports, workloadPortsToSimplePort)...)
			// This flat merge is ok because the VIPs themselves are per-network and we require
			// VIP uniqueness within a network
			vips.InsertAll(slices.Map(obj.Object.Service.GetAddresses(), networkAddressToSimple)...)
			sans.InsertAll(obj.Object.Service.GetSubjectAltNames()...)

		}

		basePorts := sets.New(slices.Map(base.Object.Service.Ports, workloadPortsToSimplePort)...)
		for source, portSet := range ports {
			if !basePorts.Equals(portSet) {
				log.Warnf("ServiceInfo derived from %s has mismatched ports. Base ports %v != %v", source, basePorts, portSet)
			}
		}

		// Never modify a ServiceInfo published by an input collection.
		merged := *base.Object
		merged.Service = protomarshal.Clone(base.Object.Service)

		// TODO: Do we need to merge anything else?

		// VIP order needs to be stable for comparison purposes.
		orderedVips := slices.SortBy(vips.UnsortedList(), func(a simpleNetworkAddress) string {
			return a.network + "/" + a.ip.String()
		})
		merged.Service.Addresses = slices.Map(orderedVips, func(a simpleNetworkAddress) *workloadapi.NetworkAddress {
			na := &workloadapi.NetworkAddress{
				Network: a.network,
				Address: a.ip.Addr().AsSlice(),
			}
			if !a.ip.IsSingleIP() {
				na.Length = ptr.Of(uint32(a.ip.Bits()))
			}
			return na
		})
		merged.Service.SubjectAltNames = sans.UnsortedList()

		// Remember, we have to re-precompute the serviceinfo since we changed it
		return &krt.ObjectWithCluster[model.ServiceInfo]{
			ClusterID: base.ClusterID,
			Object:    precomputeService(&merged),
		}
	}
}

func mergeWorkloadInfosWithCluster(
	localClusterID cluster.ID,
) func(workloadInfos []krt.ObjectWithCluster[model.WorkloadInfo]) *krt.ObjectWithCluster[model.WorkloadInfo] {
	return func(workloadInfos []krt.ObjectWithCluster[model.WorkloadInfo]) *krt.ObjectWithCluster[model.WorkloadInfo] {
		if len(workloadInfos) == 0 {
			return nil
		}
		if len(workloadInfos) == 1 {
			return &workloadInfos[0]
		}

		// TODO: We should adjust the remote cluster store logic to seamlessly swap out clusters without there
		// being duplicates in our collections. Tracked by https://github.com/istio/istio/issues/49349
		log.Warnf("Duplicate workloadinfos found for key %s. Trying to take local and then the first if we can't find it", krt.GetKey(workloadInfos[0]))
		for _, obj := range workloadInfos {
			if obj.Object == nil {
				continue
			}
			if obj.ClusterID == localClusterID {
				return &obj
			}
		}

		log.Warnf("No local workloadinfo found for %v. Taking the first", workloadInfos)
		return &workloadInfos[0]
	}
}

func unwrapObjectWithCluster[T any](obj krt.ObjectWithCluster[T]) T {
	if obj.Object == nil {
		return ptr.Empty[T]()
	}
	return *obj.Object
}

func wrapObjectWithCluster[T any](clusterID cluster.ID) func(obj T) krt.ObjectWithCluster[T] {
	return func(obj T) krt.ObjectWithCluster[T] {
		return krt.ObjectWithCluster[T]{ClusterID: clusterID, Object: &obj}
	}
}

func wrapPointerObjectWithCluster[T any](clusterID cluster.ID) func(obj *T) krt.ObjectWithCluster[T] {
	return func(obj *T) krt.ObjectWithCluster[T] {
		return krt.ObjectWithCluster[T]{ClusterID: clusterID, Object: obj}
	}
}

type syntheticServiceShardEndpoints struct {
	ServiceHostname string
	Namespace       string
	Shard           model.ShardKey
	Endpoints       []*model.IstioEndpoint
}

func (s syntheticServiceShardEndpoints) ResourceName() string {
	return syntheticSplitHorizonServiceShardIndexKey(s.Namespace+"/"+s.ServiceHostname, s.Shard)
}

func (s syntheticServiceShardEndpoints) Equals(other syntheticServiceShardEndpoints) bool {
	if s.ServiceHostname != other.ServiceHostname || s.Namespace != other.Namespace || s.Shard != other.Shard {
		return false
	}
	if len(s.Endpoints) != len(other.Endpoints) {
		return false
	}
	for i := range s.Endpoints {
		if !s.Endpoints[i].Equals(other.Endpoints[i]) {
			return false
		}
	}
	return true
}

func shouldPublishSyntheticSplitHorizonEDS(svc *model.ServiceInfo) bool {
	return svc.Service != nil && svc.Service.Waypoint != nil
}

func syntheticSplitHorizonShardKey(wl *model.WorkloadInfo) model.ShardKey {
	return model.ShardKey{Cluster: cluster.ID(wl.Workload.ClusterId), Provider: provider.Kubernetes}
}

func syntheticSplitHorizonServiceShardIndexKey(serviceKey string, shard model.ShardKey) string {
	return serviceKey + ";" + shard.Cluster.String()
}

func parseSyntheticSplitHorizonServiceShardIndexKey(key string) (string, model.ShardKey, bool) {
	serviceKey, clusterName, found := strings.Cut(key, ";")
	if !found {
		return "", model.ShardKey{}, false
	}
	return serviceKey, model.ShardKey{Cluster: cluster.ID(clusterName), Provider: provider.Kubernetes}, true
}

func buildSyntheticSplitHorizonEndpoints(workloads []*model.WorkloadInfo, svc *model.ServiceInfo) []*model.IstioEndpoint {
	serviceKey := svc.ResourceName()
	endpoints := make([]*model.IstioEndpoint, 0, len(workloads))
	for _, wl := range workloads {
		if wl.Workload == nil {
			continue
		}
		ports := wl.Workload.Services[serviceKey]
		if ports == nil {
			continue
		}
		address := syntheticSplitHorizonEndpointAddress(wl, svc)
		for _, port := range ports.Ports {
			portName, ok := svc.PortNames[int32(port.ServicePort)]
			if !ok || portName.PortName == "" {
				continue
			}
			endpointPort := port.TargetPort
			if endpointPort == 0 {
				endpointPort = port.ServicePort
			}
			endpoints = append(endpoints, &model.IstioEndpoint{
				Addresses:       []string{address},
				EndpointPort:    endpointPort,
				ServicePortName: portName.PortName,
				Network:         network.ID(wl.Workload.Network),
				Locality: model.Locality{
					ClusterID: cluster.ID(wl.Workload.ClusterId),
				},
				Labels:         maps.Clone(wl.Labels),
				ServiceAccount: wl.Workload.ServiceAccount,
				TLSMode:        model.IstioMutualTLSModeLabel,
				WorkloadName:   wl.Workload.Name,
				Namespace:      wl.Workload.Namespace,
				HostName:       svc.Service.Hostname,
				HealthStatus:   model.Healthy,
				// LbWeight for waypoints (Envoy) is the split-horizon
				// workload's Capacity, the remote cluster's backend count for
				// this service, which ztunnel already weights by.
				LbWeight: wl.Workload.GetCapacity().GetValue(),
			})
		}
	}
	return slices.SortBy(endpoints, func(ep *model.IstioEndpoint) string {
		return ep.ServicePortName + ";" + ep.FirstAddressOrNil() + ";" + ep.Locality.ClusterID.String()
	})
}

func syntheticSplitHorizonEndpointAddress(wl *model.WorkloadInfo, svc *model.ServiceInfo) string {
	for _, raw := range wl.Workload.Addresses {
		if addr, ok := netip.AddrFromSlice(raw); ok {
			return addr.String()
		}
	}
	for _, addr := range svc.Service.Addresses {
		if addr.Network != wl.Workload.Network {
			continue
		}
		if parsed, ok := netip.AddrFromSlice(addr.Address); ok {
			return parsed.String()
		}
	}
	for _, addr := range svc.Service.Addresses {
		if parsed, ok := netip.AddrFromSlice(addr.Address); ok {
			return parsed.String()
		}
	}
	if gateway := wl.Workload.GetNetworkGateway(); gateway != nil {
		if addr := gateway.GetAddress(); addr != nil {
			if parsed, ok := netip.AddrFromSlice(addr.Address); ok {
				return parsed.String()
			}
		}
	}
	// Class E reserved range (240.0.0.0/4) — never routed on the internet.
	// EDS requires an address, but ztunnel routes via NetworkGateway, so this is never connected to.
	return "240.0.0.1"
}

func (a *index) createSplitHorizonWorkload(
	svcNamespacedName string, svc *workloadapi.Service, networkGateway *NetworkGateway, capacity uint32, meshCfg *MeshConfig,
) *model.WorkloadInfo {
	hboneMtlsPort := networkGateway.HBONEPort
	if hboneMtlsPort == 0 {
		hboneMtlsPort = 15008
	}
	uid := generateSplitHorizonWorkloadUID(networkGateway.Network.String(), networkGateway.ResourceName(), svcNamespacedName)
	wl := workloadapi.Workload{
		Uid:            uid,
		Name:           uid,
		Namespace:      svc.Namespace,
		Network:        networkGateway.Network.String(),
		TrustDomain:    pickTrustDomain(meshCfg),
		Capacity:       &wrapperspb.UInt32Value{Value: capacity},
		WorkloadType:   workloadapi.WorkloadType_POD, // TODO(stevenjin8): What is the correct type here?
		TunnelProtocol: workloadapi.TunnelProtocol_HBONE,
		NetworkGateway: &workloadapi.GatewayAddress{
			Destination:   &workloadapi.GatewayAddress_Address{},
			HboneMtlsPort: hboneMtlsPort,
		},
		ClusterId: networkGateway.Cluster.String(),

		Services: map[string]*workloadapi.PortList{
			svcNamespacedName: {
				Ports: svc.Ports,
			},
		},
	}
	address, err := netip.ParseAddr(networkGateway.Addr)
	if err != nil {
		// Assume address is a hostname
		wl.NetworkGateway.Destination = &workloadapi.GatewayAddress_Hostname{
			Hostname: &workloadapi.NamespacedHostname{
				Namespace: networkGateway.Source.Namespace,
				Hostname:  networkGateway.Addr,
			},
		}
	} else {
		wl.NetworkGateway.Destination = &workloadapi.GatewayAddress_Address{
			Address: &workloadapi.NetworkAddress{
				Network: networkGateway.Network.String(),
				Address: address.AsSlice(),
			},
		}
	}

	wi := &model.WorkloadInfo{
		Workload: &wl,
		Source:   model.TypedObject{Kind: kind.KubernetesGateway},
		Labels:   labelutil.AugmentLabels(nil, networkGateway.Cluster, "", "", networkGateway.Network),
	}
	return precomputeWorkload(wi)
}
