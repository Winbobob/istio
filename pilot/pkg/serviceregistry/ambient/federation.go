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
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/log"
	"istio.io/istio/pkg/spiffe"
	"istio.io/istio/pkg/util/protomarshal"
	"istio.io/istio/pkg/util/sets"
)

// AllGlobalServiceFromLocalClusterWithSANs returns all known globally scoped services with
// SubjectAltNames populated based on the local workloads backing them. This is used for
// federation sync so remote clusters know what identities to expect when connecting.
// It uses localServices (pre-merge) to avoid including federated services.
func (a *index) AllGlobalServiceFromLocalClusterWithSANs() []*model.ServiceInfo {
	if a.localServices == nil {
		return nil
	}
	services := a.allLocalNetworkGlobalServicesFromCollection(a.localServices)

	// Get mesh config for trust domain
	meshCfg := a.meshConfig.Get()
	if meshCfg == nil {
		log.Warnf("Mesh config not available, returning services without SANs")
		return services
	}

	result := make([]*model.ServiceInfo, 0, len(services))
	for _, svc := range services {
		if enriched := a.enrichServiceWithSANs(svc, meshCfg); enriched != nil {
			svc = enriched
		}
		result = append(result, svc)
	}

	return result
}

// allLocalNetworkGlobalServicesFromCollection returns global-scoped services from the given
// collection. This is factored out from AllGlobalServiceFromLocalClusterWithSANs so that the outbound
// federation path can operate on localServices (pre-merge) without including federated data.
func (a *index) allLocalNetworkGlobalServicesFromCollection(col krt.Collection[*model.ServiceInfo]) []*model.ServiceInfo {
	var res []*model.ServiceInfo
	for _, svc := range col.List() {
		if svc.Scope != model.Global {
			// Check if the service is a waypoint containing global services
			wpSvcs := a.services.ByOwningWaypointHostname.Lookup(NamespaceHostname{
				Namespace: svc.Service.Namespace,
				Hostname:  svc.Service.Hostname,
			})
			if len(wpSvcs) == 0 {
				continue
			}
			for _, resp := range wpSvcs {
				if resp.Scope == model.Global {
					res = append(res, svc)
					break
				}
			}
		} else {
			res = append(res, svc)
		}
	}
	return res
}

// ServiceWithSANs returns a copy of the service with SubjectAltNames populated
// based on local workloads backing it. Uses localWorkloadsByServiceKey to avoid
// including federation workloads in the SAN computation.
func (a *index) ServiceWithSANs(svc *model.ServiceInfo) *model.ServiceInfo {
	if svc == nil || svc.Service == nil {
		return svc
	}

	// Get mesh config for trust domain
	meshCfg := a.meshConfig.Get()
	if meshCfg == nil {
		return svc
	}

	return a.enrichServiceWithSANs(svc, meshCfg)
}

// enrichServiceWithSANs looks up local workloads backing a service,
// collects their SPIFFE identities, and returns a cloned service with
// SubjectAltNames populated. Returns nil if no SANs were added.
func (a *index) enrichServiceWithSANs(svc *model.ServiceInfo, meshCfg *MeshConfig) *model.ServiceInfo {
	// Look up local workloads backing this service
	svcKey := svc.Service.Namespace + "/" + svc.Service.Hostname
	var wls []*model.WorkloadInfo
	if a.localWorkloadsByServiceKey != nil {
		wls = a.localWorkloadsByServiceKey.Lookup(svcKey)
	} else {
		wls = a.workloads.ByServiceKey.Lookup(svcKey)
	}

	if len(wls) == 0 {
		return nil
	}

	// Collect SANs from all workloads backing this service
	sans := sets.String{}
	for _, wl := range wls {
		san := spiffe.MustGenSpiffeURI(meshCfg.MeshConfig, wl.Workload.Namespace, wl.Workload.ServiceAccount)
		sans.Insert(san)
	}

	if sans.IsEmpty() {
		return nil
	}

	// Merge with any existing SANs
	sans = sans.Union(sets.New(svc.Service.SubjectAltNames...))

	// Clone and update the service
	newSvcInfo := cloneServiceInfoPreservingMetadata(svc)
	newSvcInfo.Service.SubjectAltNames = sets.SortedList(sans)

	log.Debugf("Added SANs for service %s: %v", svc.Service.Hostname, newSvcInfo.Service.SubjectAltNames)

	return newSvcInfo
}

// ServiceCapacity returns the total backend capacity for a service, computed
// as the sum of workload capacities (workloads with zero capacity count as 1).
// Same logic as upstream multicluster.go coalesced workload capacity.
func (a *index) ServiceCapacity(serviceInfo *model.ServiceInfo) uint32 {
	if serviceInfo == nil || serviceInfo.Service == nil {
		return 0
	}
	service := serviceInfo.Service
	svcKey := service.Namespace + "/" + service.Hostname
	var workloadInfos []*model.WorkloadInfo
	if a.localWorkloadsByServiceKey != nil {
		workloadInfos = a.localWorkloadsByServiceKey.Lookup(svcKey)
	} else {
		workloadInfos = a.workloads.ByServiceKey.Lookup(svcKey)
	}
	capacity := uint32(0)
	for _, wl := range workloadInfos {
		if wl.Workload.GetCapacity().GetValue() == 0 {
			capacity++
		} else {
			capacity += wl.Workload.Capacity.GetValue()
		}
	}
	return capacity
}

// globalServiceKeySet returns the namespace/hostname keys of all local
// global-scoped services. This is the single source of truth for federation
// eligibility, shared by AllLocalWorkloadsForGlobalServices (full-sync snapshot)
// and RegisterGlobalWorkloadHandler (incremental event stream) so the two never
// disagree about which workloads are federated.
func (a *index) globalServiceKeySet() sets.String {
	keys := sets.String{}
	if a.localServices == nil {
		return keys
	}
	for _, svc := range a.allLocalNetworkGlobalServicesFromCollection(a.localServices) {
		if svc.Service == nil {
			continue
		}
		keys.Insert(svc.Service.Namespace + "/" + svc.Service.Hostname)
	}
	return keys
}

// workloadBacksGlobalService reports whether the workload backs at least one
// global-scoped service — i.e. whether it is federation-eligible.
func workloadBacksGlobalService(wl *model.WorkloadInfo, globalServiceKeys sets.String) bool {
	if wl.Workload == nil {
		return false
	}
	for k := range wl.Workload.Services {
		if globalServiceKeys.Contains(k) {
			return true
		}
	}
	return false
}

// AllLocalWorkloadsForGlobalServices returns all local workloads backing
// global-scoped services. Used by single-network federation to send actual
// pod workloads instead of split-horizon data. Workloads are deduped by UID
// since a workload may back multiple services.
func (a *index) AllLocalWorkloadsForGlobalServices() []*model.WorkloadInfo {
	if a.localServices == nil || a.localWorkloadsByServiceKey == nil {
		return nil
	}

	// Single source of truth for which services are federated. A workload's
	// Workload.Services map may also contain local-only service keys (a pod can
	// back both global and local services); those must not leak to remote
	// clusters, so we filter each workload's Services map down to this set.
	globalServiceKeys := a.globalServiceKeySet()
	if globalServiceKeys.IsEmpty() {
		return nil
	}

	seen := sets.String{}
	var result []*model.WorkloadInfo
	for svcKey := range globalServiceKeys {
		wls := a.localWorkloadsByServiceKey.Lookup(svcKey)
		for _, wl := range wls {
			if wl.Workload == nil {
				continue
			}
			if !seen.InsertContains(wl.Workload.Uid) {
				result = append(result, filterWorkloadToGlobalServices(wl, globalServiceKeys))
			}
		}
	}

	log.Debugf("AllLocalWorkloadsForGlobalServices: %d global services, %d unique workloads",
		len(globalServiceKeys), len(result))

	return result
}

// filterWorkloadToGlobalServices returns a copy of wl whose Workload.Services
// map is trimmed to only the keys in globalServiceKeys. This prevents leaking
// local-only service associations to remote clusters when the workload is
// federated. The workload proto is deep-copied via protomarshal.Clone so the shared
// index entry is never mutated; if no filtering is needed, the original pointer
// is returned unchanged.
func filterWorkloadToGlobalServices(wl *model.WorkloadInfo, globalServiceKeys sets.String) *model.WorkloadInfo {
	if wl == nil || wl.Workload == nil || len(wl.Workload.Services) == 0 {
		return wl
	}

	// Fast path: nothing to strip if every service key is already global.
	needsFilter := false
	for k := range wl.Workload.Services {
		if !globalServiceKeys.Contains(k) {
			needsFilter = true
			break
		}
	}
	if !needsFilter {
		return wl
	}

	cloned := protomarshal.Clone(wl.Workload)
	for k := range cloned.Services {
		if !globalServiceKeys.Contains(k) {
			delete(cloned.Services, k)
		}
	}
	// Copy the WorkloadInfo so we do not mutate the caller's (shared) struct.
	out := *wl
	out.Workload = cloned
	return &out
}

// RegisterGlobalServiceHandler registers a callback that is invoked when
// local global-scoped services change. Uses localServices (pre-merge) so that
// federation data changes don't trigger outbound sync loops.
func (a *index) RegisterGlobalServiceHandler(f func(prev, curr *model.ServiceInfo, event model.Event)) {
	if a.localServices == nil {
		return
	}
	// Service scope transition table:
	//   Old Scope | New Scope | Federation Event
	//   --------- | --------- | ----------------
	//   —         | Global    | EventAdd
	//   Global    | —         | EventDelete
	//   Global    | Global    | EventUpdate
	//   Local     | Global    | EventAdd  (new to federation)
	//   Global    | Local     | EventDelete (removed from federation)
	//   Local     | Local     | skip
	a.localServices.Register(func(e krt.Event[*model.ServiceInfo]) {
		var prev, curr *model.ServiceInfo
		var event model.Event

		oldSvc, newSvc := eventObjects(e)
		oldIsGlobal := oldSvc != nil && oldSvc.Scope == model.Global
		newIsGlobal := newSvc != nil && newSvc.Scope == model.Global

		switch {
		case e.Event == controllers.EventAdd && newIsGlobal:
			// New global service added.
			curr = newSvc
			event = model.EventAdd
		case e.Event == controllers.EventDelete && oldIsGlobal:
			// Global service deleted.
			prev = oldSvc
			event = model.EventDelete
		case e.Event == controllers.EventUpdate && oldIsGlobal && newIsGlobal:
			// Global service updated, still global.
			prev = oldSvc
			curr = newSvc
			event = model.EventUpdate
		case e.Event == controllers.EventUpdate && !oldIsGlobal && newIsGlobal:
			// Scope changed from local to global — treat as a new addition to federation.
			curr = newSvc
			event = model.EventAdd
		case e.Event == controllers.EventUpdate && oldIsGlobal && !newIsGlobal:
			// Scope changed from global to local — treat as a deletion from federation.
			prev = oldSvc
			event = model.EventDelete
		default:
			// Non-global service that remained non-global, or irrelevant event — skip.
			return
		}

		f(prev, curr, event)
	})
}

// RegisterGlobalWorkloadHandler registers a callback invoked when a local
// workload's federation eligibility changes. A workload is eligible iff it backs
// at least one global-scoped service. Eligibility changes through two independent
// triggers, both handled here:
//
//	Trigger 1 — the workload itself changes (pod added / deleted / edited). Handled
//	  by registering on a.localWorkloads (the pre-federation-merge collection).
//	Trigger 2 — a service's scope flips global<->local (or is deleted), changing
//	  eligibility of workloads that did not themselves change. Handled by fanning
//	  out to a.localWorkloadsByServiceKey.Lookup(svcKey).
//
// Emitted workloads are filtered to global service keys (filterWorkloadToGlobalServices)
// so local-only associations never leak. Eligibility uses globalServiceKeySet, the
// same source of truth as AllLocalWorkloadsForGlobalServices, so the incremental
// stream and the full-sync snapshot never disagree.
func (a *index) RegisterGlobalWorkloadHandler(f func(prev, curr *model.WorkloadInfo, event model.Event)) {
	if a.localWorkloads == nil || a.localServices == nil || a.localWorkloadsByServiceKey == nil {
		return
	}

	// emitUpsert fires the handler for a federation-eligible workload. The
	// workload is trimmed to its global-service keys (filterWorkloadToGlobalServices)
	// so local-only associations never leave the cluster. prev is the pre-change
	// revision (nil on add); the trimmed workload is passed as curr.
	emitUpsert := func(prev, wl *model.WorkloadInfo, globalKeys sets.String, event model.Event) {
		f(prev, filterWorkloadToGlobalServices(wl, globalKeys), event)
	}

	// Trigger 1: per-workload changes.
	a.localWorkloads.Register(func(e krt.Event[*model.WorkloadInfo]) {
		globalKeys := a.globalServiceKeySet()
		oldWl, newWl := eventObjects(e)
		// A workload-only event does not change service scope, so the same global
		// key set applies to both the old and new revision.
		wasEligible := oldWl != nil && workloadBacksGlobalService(oldWl, globalKeys)
		isEligible := newWl != nil && workloadBacksGlobalService(newWl, globalKeys)

		switch {
		case !wasEligible && isEligible:
			emitUpsert(nil, newWl, globalKeys, model.EventAdd)
		case wasEligible && isEligible:
			emitUpsert(oldWl, newWl, globalKeys, model.EventUpdate)
		case wasEligible && !isEligible:
			// Pod deleted, or edited so it no longer backs any global service.
			f(oldWl, nil, model.EventDelete)
		default:
			// Never eligible — not federated, nothing to do.
		}
	})

	// Trigger 2: service scope flips fan out to the workloads backing that service.
	a.localServices.Register(func(e krt.Event[*model.ServiceInfo]) {
		oldSvc, newSvc := eventObjects(e)
		oldIsGlobal := oldSvc != nil && oldSvc.Scope == model.Global
		newIsGlobal := newSvc != nil && newSvc.Scope == model.Global
		if oldIsGlobal == newIsGlobal {
			// Scope did not cross the global boundary — Trigger 1 (if the backing
			// set changed) or nothing covers it. Avoid redundant fan-out.
			return
		}

		svc := newSvc
		if svc == nil {
			svc = oldSvc
		}
		if svc == nil || svc.Service == nil {
			return
		}
		svcKey := svc.Service.Namespace + "/" + svc.Service.Hostname

		// globalServiceKeySet reflects state at the time this handler runs. For a
		// scope flip the service still exists, so the index Lookup is stable and
		// this correctly re-publishes or withdraws each backing workload.
		globalKeys := a.globalServiceKeySet()
		for _, wl := range a.localWorkloadsByServiceKey.Lookup(svcKey) {
			if wl.Workload == nil {
				continue
			}
			if workloadBacksGlobalService(wl, globalKeys) {
				// Still (or now) eligible: re-publish with the updated filtered view.
				emitUpsert(wl, wl, globalKeys, model.EventUpdate)
			} else {
				// No longer backs any global service: withdraw it.
				f(wl, nil, model.EventDelete)
			}
		}
	})
}

// eventObjects unwraps the old and new objects of an event on a pointer collection.
func eventObjects[T any](e krt.Event[*T]) (oldObj, newObj *T) {
	if e.Old != nil {
		oldObj = *e.Old
	}
	if e.New != nil {
		newObj = *e.New
	}
	return oldObj, newObj
}
