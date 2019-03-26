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

	"k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/features"
	priorityutil "k8s.io/kubernetes/pkg/scheduler/algorithm/priorities/util"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

const NvidiaGPU v1.ResourceName = "nvidia.com/gpu"

// ResourceAllocationPriority contains information to calculate resource allocation priority.
type ResourceAllocationPriority struct {
	Name   string
	scorer func(requested, allocable *schedulernodeinfo.Resource, includeVolumes bool, requestedVolumes int, allocatableVolumes int) int64
}

// PriorityMap priorities nodes according to the resource allocations on the node.
// It will use `scorer` function to calculate the score.
func (r *ResourceAllocationPriority) PriorityMap(
	pod *v1.Pod,
	meta interface{},
	nodeInfo *schedulernodeinfo.NodeInfo) (schedulerapi.HostPriority, error) {
	klog.V(10).Infof("xychu %v, priorityMap for pod %v and nodeInfo %v", r.Name, pod, nodeInfo)
	node := nodeInfo.Node()
	if node == nil {
		return schedulerapi.HostPriority{}, fmt.Errorf("node not found")
	}
	allocatable := nodeInfo.AllocatableResource()

	var requested schedulernodeinfo.Resource
	if priorityMeta, ok := meta.(*priorityMetadata); ok {
		//requested = *priorityMeta.nonZeroRequest
		requested = *priorityMeta.nonZeroRequest.Clone()
		klog.V(8).Infof("xychu %v, %v got requested from Meta, %v", r.Name, node.Name, requested)
	} else {
		// We couldn't parse metadata - fallback to computing it.
		requested = *getNonZeroRequests(pod)
		klog.V(8).Infof("xychu %v, %v got requested from NonZeroRequest, %v", r.Name, node.Name, requested)
	}

	requested.MilliCPU += nodeInfo.NonZeroRequest().MilliCPU
	requested.Memory += nodeInfo.NonZeroRequest().Memory
	// Add GPU requests in nodeInfo
	if v, ok := nodeInfo.RequestedResource().ScalarResources[NvidiaGPU]; ok && v > 0 {
		klog.V(8).Infof("xychu %v, %v got requested from nodeInfo, %v", r.Name, node.Name, nodeInfo.RequestedResource())
		requested.AddScalar(NvidiaGPU, v)
	}

	klog.V(10).Infof(
		"xychu %v -> %v: %v, total request %d millicores %d memory bytes %d GPU",
		pod.Name, node.Name, r.Name,
		requested.MilliCPU, requested.Memory,
		requested.ScalarResources[NvidiaGPU],
	)
	var score int64
	// Check if the pod has volumes and this could be added to scorer function for balanced resource allocation.
	if len(pod.Spec.Volumes) >= 0 && utilfeature.DefaultFeatureGate.Enabled(features.BalanceAttachedNodeVolumes) && nodeInfo.TransientInfo != nil {
		score = r.scorer(&requested, &allocatable, true, nodeInfo.TransientInfo.TransNodeInfo.RequestedVolumes, nodeInfo.TransientInfo.TransNodeInfo.AllocatableVolumesCount)
	} else {
		score = r.scorer(&requested, &allocatable, false, 0, 0)
	}

	if klog.V(10) {
		if len(pod.Spec.Volumes) >= 0 && utilfeature.DefaultFeatureGate.Enabled(features.BalanceAttachedNodeVolumes) && nodeInfo.TransientInfo != nil {
			klog.Infof(
				"%v -> %v: %v, capacity %d millicores %d memory bytes, %d volumes, total request %d millicores %d memory bytes %d volumes, score %d",
				pod.Name, node.Name, r.Name,
				allocatable.MilliCPU, allocatable.Memory, nodeInfo.TransientInfo.TransNodeInfo.AllocatableVolumesCount,
				requested.MilliCPU, requested.Memory,
				nodeInfo.TransientInfo.TransNodeInfo.RequestedVolumes,
				score,
			)
		} else {
			klog.Infof(
				"%v -> %v: %v, capacity %d millicores %d memory bytes, total request %d millicores %d memory bytes, score %d",
				pod.Name, node.Name, r.Name,
				allocatable.MilliCPU, allocatable.Memory,
				requested.MilliCPU, requested.Memory,
				score,
			)
		}
	}

	klog.V(10).Infof(
		"xychu %v -> %v: %v, capacity %d millicores %d memory bytes %d GPU, total request %d millicores %d memory bytes %d GPU, score %d",
		pod.Name, node.Name, r.Name,
		allocatable.MilliCPU, allocatable.Memory, allocatable.ScalarResources[NvidiaGPU],
		requested.MilliCPU, requested.Memory,
		requested.ScalarResources[NvidiaGPU],
		score,
	)

	return schedulerapi.HostPriority{
		Host:  node.Name,
		Score: int(score),
	}, nil
}

func getNonZeroRequests(pod *v1.Pod) *schedulernodeinfo.Resource {
	klog.V(10).Infof("xychu start pod %v GetNonzeroRequests", pod.Name)
	result := &schedulernodeinfo.Resource{}
	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		cpu, memory := priorityutil.GetNonzeroRequests(&container.Resources.Requests)
		result.MilliCPU += cpu
		result.Memory += memory

		if quantity, found := (container.Resources.Requests)[NvidiaGPU]; found {
			result.AddScalar(NvidiaGPU, quantity.Value())
		}
	}

	klog.V(10).Infof("xychu end pod %v GetNonzeroRequests %v", pod.Name, result)
	return result

}
