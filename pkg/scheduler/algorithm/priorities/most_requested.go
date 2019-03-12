/*
Copyright 2016 The Kubernetes Authors.

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
	//	"github.com/golang/glog"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	"k8s.io/kubernetes/pkg/scheduler/schedulercache"
)

var (
	mostResourcePriority = &ResourceAllocationPriority{"MostResourceAllocation", mostResourceScorer}

	// MostRequestedPriorityMap is a priority function that favors nodes with most requested resources.
	// It calculates the percentage of memory and CPU requested by pods scheduled on the node, and prioritizes
	// based on the maximum of the average of the fraction of requested to capacity.
	// Details: (cpu(10 * sum(requested) / capacity) + memory(10 * sum(requested) / capacity)) / 2
	MostRequestedPriorityMap = mostResourcePriority.PriorityMap
)

func mostResourceScorer(requested, allocable *schedulercache.Resource) int64 {
	if requested.NvidiaGPU > 0 {
		return mostRequestedScore(requested.NvidiaGPU, allocable.NvidiaGPU) +
			(mostRequestedScore(requested.MilliCPU, allocable.MilliCPU)+
				mostRequestedScore(requested.Memory, allocable.Memory))/2
	}
	if v, ok := requested.ScalarResources[NvidiaGPU]; ok && v > 0 {
		// for GPU node with GPU requeted, use gpuScore * 10 + (cpuScore + memScore)/2
		// which means that even one GPU is used, the score will still dominate over cpu+mem
		return mostRequestedScore(v, allocable.ScalarResources[NvidiaGPU])*10 +
			(mostRequestedScore(requested.MilliCPU, allocable.MilliCPU)+
				mostRequestedScore(requested.Memory, allocable.Memory))/2
	} else if v, ok := allocable.ScalarResources[NvidiaGPU]; !ok || v == 0 {
		// for CPU node and CPU pods, use 9*10 + (cpuScore + memScore)/2
		// so it's score will only lower than:
		// all GPU have been requetsed:  10*10 + (cpuScore + memScore)/2
		// and higher than other GPU node which has gpu avaliable.
		return 90 +
			(mostRequestedScore(requested.MilliCPU, allocable.MilliCPU)+
				mostRequestedScore(requested.Memory, allocable.Memory))/2
	}
	// for GPU node without GPU requeted
	return (mostRequestedScore(requested.MilliCPU, allocable.MilliCPU) +
		mostRequestedScore(requested.Memory, allocable.Memory)) / 2
}

// The used capacity is calculated on a scale of 0-10
// 0 being the lowest priority and 10 being the highest.
// The more resources are used the higher the score is. This function
// is almost a reversed version of least_requested_priority.calculatUnusedScore
// (10 - calculateUnusedScore). The main difference is in rounding. It was added to
// keep the final formula clean and not to modify the widely used (by users
// in their default scheduling policies) calculateUSedScore.
func mostRequestedScore(requested, capacity int64) int64 {
	if capacity == 0 {
		return 0
	}
	if requested > capacity {
		return 0
	}

	return (requested * schedulerapi.MaxPriority) / capacity
}
