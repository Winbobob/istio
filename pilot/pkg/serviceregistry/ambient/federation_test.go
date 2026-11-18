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
	"sync"
	"testing"
	"time"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/maps"
	"istio.io/istio/pkg/ptr"
	"istio.io/istio/pkg/test"
	"istio.io/istio/pkg/test/util/assert"
	"istio.io/istio/pkg/test/util/retry"
	"istio.io/istio/pkg/workloadapi"
)

// serviceInfo builds a minimal ServiceInfo with the given hostname and scope.
func serviceInfo(hostname string, scope model.ServiceScope) *model.ServiceInfo {
	return &model.ServiceInfo{
		Service: &workloadapi.Service{
			Name:      hostname,
			Namespace: "default",
			Hostname:  hostname,
		},
		Scope: scope,
	}
}

// TestRegisterGlobalServiceHandler_NilLocalServices verifies that the handler
// registration is a no-op (and does not panic) when localServices is nil.
func TestRegisterGlobalServiceHandler_NilLocalServices(t *testing.T) {
	a := &index{}
	called := false
	a.RegisterGlobalServiceHandler(func(prev, curr *model.ServiceInfo, event model.Event) {
		called = true
	})
	assert.Equal(t, called, false)
}

// TestRegisterGlobalServiceHandler_ScopeTransitions exercises the scope
// transition table in RegisterGlobalServiceHandler.
//
// Each step describes a state transition for a single hostname:
//
//	prevScope -> currScope, performed by add / update / delete on the
//	underlying StaticCollection. expectEvent describes what (if anything)
//	the handler should observe.
func TestRegisterGlobalServiceHandler_ScopeTransitions(t *testing.T) {
	type step struct {
		serviceEvent   model.Event // EventAdd / EventUpdate / EventDelete
		prevScope      *model.ServiceScope
		currScope      *model.ServiceScope
		expectEvent    *model.Event // nil = no event expected
		expectPrevFunc func(*model.ServiceInfo) bool
		expectCurrFunc func(*model.ServiceInfo) bool
	}

	isNil := func(s *model.ServiceInfo) bool { return s == nil }
	isGlobal := func(s *model.ServiceInfo) bool { return s != nil && s.Scope == model.Global }

	cases := []struct {
		name string
		step step
	}{
		{
			name: "Global_to_Global_emits_Update",
			step: step{
				serviceEvent:   model.EventUpdate,
				prevScope:      ptr.Of(model.Global),
				currScope:      ptr.Of(model.Global),
				expectEvent:    ptr.Of(model.EventUpdate),
				expectPrevFunc: isGlobal,
				expectCurrFunc: isGlobal,
			},
		},
		{
			name: "Global_to_Local_emits_Delete",
			step: step{
				serviceEvent:   model.EventUpdate,
				prevScope:      ptr.Of(model.Global),
				currScope:      ptr.Of(model.Local),
				expectEvent:    ptr.Of(model.EventDelete),
				expectPrevFunc: isGlobal,
				expectCurrFunc: isNil,
			},
		},
		{
			name: "Global_to_nil_emits_Delete",
			step: step{
				serviceEvent:   model.EventDelete,
				prevScope:      ptr.Of(model.Global),
				currScope:      nil,
				expectEvent:    ptr.Of(model.EventDelete),
				expectPrevFunc: isGlobal,
				expectCurrFunc: isNil,
			},
		},
		{
			name: "Local_to_Global_emits_Add",
			step: step{
				serviceEvent:   model.EventUpdate,
				prevScope:      ptr.Of(model.Local),
				currScope:      ptr.Of(model.Global),
				expectEvent:    ptr.Of(model.EventAdd),
				expectPrevFunc: isNil,
				expectCurrFunc: isGlobal,
			},
		},
		{
			name: "Local_to_Local_skips",
			step: step{
				serviceEvent: model.EventUpdate,
				prevScope:    ptr.Of(model.Local),
				currScope:    ptr.Of(model.Local),
				expectEvent:  nil,
			},
		},
		{
			name: "Local_to_nil_skips",
			step: step{
				serviceEvent: model.EventDelete,
				prevScope:    ptr.Of(model.Local),
				currScope:    nil,
				expectEvent:  nil,
			},
		},
		{
			name: "nil_to_Global_emits_Add",
			step: step{
				serviceEvent:   model.EventAdd,
				prevScope:      nil,
				currScope:      ptr.Of(model.Global),
				expectEvent:    ptr.Of(model.EventAdd),
				expectPrevFunc: isNil,
				expectCurrFunc: isGlobal,
			},
		},
		{
			name: "nil_to_Local_skips",
			step: step{
				serviceEvent: model.EventAdd,
				prevScope:    nil,
				currScope:    ptr.Of(model.Local),
				expectEvent:  nil,
			},
		},
	}

	stop := test.NewStop(t)
	col := krt.NewMutableCollection[*model.ServiceInfo](nil, nil, krt.WithStop(stop), krt.WithName("TestLocalServices"))
	a := &index{localServices: col.AsCollection()}

	var (
		mu     sync.Mutex
		events []krt.Event[model.ServiceInfo]
	)
	a.RegisterGlobalServiceHandler(func(prev, curr *model.ServiceInfo, event model.Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, krt.Event[model.ServiceInfo]{Old: prev, New: curr, Event: controllers.EventType(event)})
	})

	getEvents := func() []krt.Event[model.ServiceInfo] {
		mu.Lock()
		defer mu.Unlock()
		out := make([]krt.Event[model.ServiceInfo], len(events))
		copy(out, events)
		return out
	}
	resetEvents := func() {
		mu.Lock()
		defer mu.Unlock()
		events = nil
	}

	// Each subtest uses a unique hostname so cases are independent.
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hostname := "svc-" + tc.name + ".example.com"

			// Set up "previous" state if needed: any case with a non-nil prevScope
			// requires pre-populating the collection so the handler observes the
			// transition from that prior scope.
			if tc.step.prevScope != nil {
				col.UpdateObject(serviceInfo(hostname, *tc.step.prevScope))
				// Wait for setup event to be dispatched (async) before resetting.
				time.Sleep(50 * time.Millisecond)
			}
			// Drain any setup events.
			resetEvents()

			// Apply the operation under test.
			switch tc.step.serviceEvent {
			case model.EventAdd, model.EventUpdate:
				col.UpdateObject(serviceInfo(hostname, *tc.step.currScope))
			case model.EventDelete:
				// Scope does not matter here: ServiceInfo's keyFunc (ResourceName)
				// only considers namespace and hostname.
				col.DeleteObject(krt.GetKey(serviceInfo(hostname, model.Local)))
			default:
				t.Fatalf("unknown op %q", tc.step.serviceEvent)
			}

			if tc.step.expectEvent == nil {
				// No event expected — give the handler a moment, then assert nothing fired.
				time.Sleep(20 * time.Millisecond)
				assert.Equal(t, len(getEvents()), 0)
				return
			}

			// Wait for exactly one event.
			retry.UntilOrFail(t, func() bool { return len(getEvents()) == 1 },
				retry.Delay(5*time.Millisecond), retry.Timeout(2*time.Second))

			got := getEvents()[0]
			assert.Equal(t, model.Event(got.Event), *tc.step.expectEvent)
			assert.Equal(t, tc.step.expectPrevFunc(got.Old), true)
			assert.Equal(t, tc.step.expectCurrFunc(got.New), true)
		})
	}
}

// workloadInfoBacking builds a WorkloadInfo whose Services map contains the
// given service keys (namespace/hostname), i.e. the workload "backs" them.
func workloadInfoBacking(uid string, serviceKeys ...string) *model.WorkloadInfo {
	svcs := map[string]*workloadapi.PortList{}
	for _, k := range serviceKeys {
		svcs[k] = &workloadapi.PortList{}
	}
	return &model.WorkloadInfo{Workload: &workloadapi.Workload{Uid: uid, Name: uid, Services: svcs}}
}

// TestRegisterGlobalWorkloadHandler_Trigger1_Deletion verifies the load-bearing
// P2 claim: when a workload stops backing any global service (pod deleted, or its
// last global backing removed), the per-workload trigger emits EventDelete —
// independently of the service-scope-flip trigger. This is what guarantees a
// service deletion does not leave stale direct pod routes even though the two
// collections have no cross-ordering guarantee.
func TestRegisterGlobalWorkloadHandler_Trigger1_Deletion(t *testing.T) {
	stop := test.NewStop(t)

	// One global service "default/gsvc" so the workload is federation-eligible.
	svcCol := krt.NewStaticCollection(nil,
		[]*model.ServiceInfo{serviceInfo("gsvc", model.Global)},
		krt.WithStop(stop), krt.WithName("TestLocalServices"))

	wlCol := krt.NewMutableCollection[*model.WorkloadInfo](nil, nil,
		krt.WithStop(stop), krt.WithName("TestLocalWorkloads"))
	wlIndex := krt.NewIndex(wlCol.AsCollection(), "svc", func(o *model.WorkloadInfo) []string {
		return maps.Keys(o.Workload.Services)
	})

	a := &index{
		localServices:              svcCol,
		localWorkloads:             wlCol.AsCollection(),
		localWorkloadsByServiceKey: wlIndex,
	}

	var (
		mu     sync.Mutex
		events []krt.Event[model.WorkloadInfo]
	)
	a.RegisterGlobalWorkloadHandler(func(prev, curr *model.WorkloadInfo, event model.Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, krt.Event[model.WorkloadInfo]{Old: prev, New: curr, Event: controllers.EventType(event)})
	})
	getEvents := func() []krt.Event[model.WorkloadInfo] {
		mu.Lock()
		defer mu.Unlock()
		out := make([]krt.Event[model.WorkloadInfo], len(events))
		copy(out, events)
		return out
	}
	reset := func() { mu.Lock(); events = nil; mu.Unlock() }

	// hasEvent reports whether any captured event matches the predicate.
	hasEvent := func(pred func(krt.Event[model.WorkloadInfo]) bool) bool {
		for _, e := range getEvents() {
			if pred(e) {
				return true
			}
		}
		return false
	}

	// Add a workload backing the global service → expect an eligible upsert
	// (EventAdd from the per-workload trigger). Other events (e.g. a scope-flip
	// trigger replaying initial state) may also fire; we only require the add.
	wl := workloadInfoBacking("pod-1", "default/gsvc")
	wlCol.UpdateObject(wl)
	retry.UntilOrFail(t, func() bool {
		return hasEvent(func(e krt.Event[model.WorkloadInfo]) bool {
			return model.Event(e.Event) == model.EventAdd &&
				e.New != nil && e.New.Workload != nil && e.New.Workload.Uid == "pod-1"
		})
	}, retry.Delay(5*time.Millisecond), retry.Timeout(2*time.Second))
	reset()

	// Delete the workload (pod deleted) → expect a Delete carrying the old UID.
	// This is the load-bearing assertion: the per-workload trigger withdraws the
	// workload independently of the service-scope trigger.
	wlCol.DeleteObject(krt.GetKey(wl))
	retry.UntilOrFail(t, func() bool {
		return hasEvent(func(e krt.Event[model.WorkloadInfo]) bool {
			return model.Event(e.Event) == model.EventDelete &&
				e.Old != nil && e.Old.Workload != nil && e.Old.Workload.Uid == "pod-1"
		})
	}, retry.Delay(5*time.Millisecond), retry.Timeout(2*time.Second))
}
