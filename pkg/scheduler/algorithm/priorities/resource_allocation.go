/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package priorities

import (
	"fmt"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	priorityutil "k8s.io/kubernetes/pkg/scheduler/algorithm/priorities/util"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	"k8s.io/kubernetes/pkg/scheduler/schedulercache"
)

const NvidiaGPU v1.ResourceName = "nvidia.com/gpu"

// ResourceAllocationPriority contains information to calculate resource allocation priority.
type ResourceAllocationPriority struct {
	Name   string
	scorer func(requested, allocable *schedulercache.Resource) int64
}

// PriorityMap priorities nodes according to the resource allocations on the node.
// It will use `scorer` function to calculate the score.
func (r *ResourceAllocationPriority) PriorityMap(
	pod *v1.Pod,
	meta interface{},
	nodeInfo *schedulercache.NodeInfo) (schedulerapi.HostPriority, error) {
	glog.V(10).Infof("%v, priorityMap for pod %v and nodeInfo %v", r.Name, pod, nodeInfo)
	node := nodeInfo.Node()
	if node == nil {
		return schedulerapi.HostPriority{}, fmt.Errorf("node not found")
	}
	allocatable := nodeInfo.AllocatableResource()

	var requested = schedulercache.Resource{}
	if priorityMeta, ok := meta.(*priorityMetadata); ok {
		requested = *priorityMeta.nonZeroRequest
		glog.V(8).Infof("%v, %v got requested from Meta, %v", r.Name, node.Name, requested)
	} else {
		// We couldn't parse metadata - fallback to computing it.
		requested = *getNonZeroRequests(pod)
		glog.V(8).Infof("%v, %v got requested from NonZeroRequest, %v", r.Name, node.Name, requested)
	}

	requested.MilliCPU += nodeInfo.NonZeroRequest().MilliCPU
	requested.Memory += nodeInfo.NonZeroRequest().Memory

	// Add GPU requests in nodeInfo
	requested.NvidiaGPU += nodeInfo.RequestedResource().NvidiaGPU
	if v, ok := nodeInfo.RequestedResource().ScalarResources[NvidiaGPU]; ok && v > 0 {
		glog.V(8).Infof("%v, %v got requested from nodeInfo, %v", r.Name, node.Name, nodeInfo.RequestedResource())
		requested.AddScalar(NvidiaGPU, v)
	}

	glog.V(10).Infof(
		"%v -> %v: %v, total request %d millicores %d memory bytes %d v1GPU %d v2GPU",
		pod.Name, node.Name, r.Name,
		requested.MilliCPU, requested.Memory,
		requested.NvidiaGPU, requested.ScalarResources[NvidiaGPU],
	)
	score := r.scorer(&requested, &allocatable)

	glog.V(10).Infof(
		"%v -> %v: %v, capacity %d millicores %d memory bytes %d v1GPU %d v2GPU, total request %d millicores %d memory bytes %d v1GPU %d v2GPU, score %d",
		pod.Name, node.Name, r.Name,
		allocatable.MilliCPU, allocatable.Memory, allocatable.NvidiaGPU, allocatable.ScalarResources[NvidiaGPU],
		requested.MilliCPU, requested.Memory,
		requested.NvidiaGPU, requested.ScalarResources[NvidiaGPU],
		score,
	)

	return schedulerapi.HostPriority{
		Host:  node.Name,
		Score: int(score),
	}, nil
}

func getNonZeroRequests(pod *v1.Pod) *schedulercache.Resource {
	result := &schedulercache.Resource{}
	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		cpu, memory := priorityutil.GetNonzeroRequests(&container.Resources.Requests)
		result.MilliCPU += cpu
		result.Memory += memory

		if quantity, found := (container.Resources.Requests)[v1.ResourceNvidiaGPU]; found {
			result.NvidiaGPU += quantity.Value()
		}
		if quantity, found := (container.Resources.Requests)[NvidiaGPU]; found {
			result.AddScalar(NvidiaGPU, quantity.Value())
		}
	}
	return result

}
