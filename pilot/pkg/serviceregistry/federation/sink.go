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

import "istio.io/istio/pilot/pkg/model"

// Sink is the receive-side contract between SyncProtocol and the ambient
// index integration. SyncProtocol calls Apply once per debounce window with
// the merged union of all live remote shards. Nil and empty slices are
// equivalent — both mean "no remote services/workloads exist".
type Sink interface {
	Apply(services []*model.ServiceInfo, workloads []*model.WorkloadInfo)
}
