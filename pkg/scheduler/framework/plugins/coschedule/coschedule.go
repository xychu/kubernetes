/*
Copyright 2019 The Kubernetes Authors.

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

package coschedule

import (
	"fmt"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
)

// CoschedulingPlugin is a plugin that implements coscheduling a group of pods
type CoschedulingPlugin struct {
	handle framework.FrameworkHandle
}

var _ = framework.ReservePlugin(CoschedulingPlugin{})
var _ = framework.PermitPlugin(CoschedulingPlugin{})

// Name is the name of the plug used in Registry and configurations.
const Name = "coscheduling-plugin"

// Name returns name of the plugin. It is used in logs, etc.
func (mc CoschedulingPlugin) Name() string {
	return Name
}

type stateData struct {
	readyCount int32
	readyPods  []string
}

func (f *stateData) Clone() framework.StateData {
	copy := &stateData{
		readyCount: f.readyCount,
		readyPods:  append(f.readyPods[:0:0], f.readyPods...),
	}
	return copy
}

// Reserve is the functions invoked by the framework at "reserve" extension point.
func (mc CoschedulingPlugin) Reserve(state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	msg := fmt.Sprintf("xychu: in reserve %s", pod.Name)
	klog.V(0).Infof(msg)
	if pod == nil {
		return framework.NewStatus(framework.Error, "pod cannot be nil")
	}
	if name, okay := pod.ObjectMeta.Annotations["coschedule-name"]; okay {
		state.Lock()
		if v, e := state.Read(framework.StateKey(name)); e == nil {
			if value, ok := v.(*stateData); ok {
				value.readyCount += 1
				value.readyPods = append(value.readyPods, pod.Name)
				state.Write(framework.StateKey(name), value)
				msg := fmt.Sprintf("xychu: in reserve %s update state %v", pod.Name, value)
				klog.V(0).Infof(msg)
			}
		} else {
			state.Write(framework.StateKey(name), &stateData{
				readyCount: 1,
				readyPods:  []string{pod.Name}})
			msg := fmt.Sprintf("xychu: in reserve %s init state", pod.Name)
			klog.V(0).Infof(msg)
		}
		state.Unlock()
	}
	return nil
}

// Permit is the functions invoked by the framework at "premit" extension point.
func (mc CoschedulingPlugin) Permit(state *framework.CycleState, pod *v1.Pod, nodeName string) (*framework.Status, time.Duration) {
	msg := fmt.Sprintf("xychu: in Permit %s", pod.Name)
	klog.V(0).Infof(msg)
	if pod == nil {
		return framework.NewStatus(framework.Error, "pod cannot be nil"), 0 * time.Second
	}

	if name, okay := pod.ObjectMeta.Annotations["coschedule-name"]; okay {
		targetCount, _ := pod.ObjectMeta.Annotations["coschedule-count"]
		targetCountInt32, _ := strconv.ParseInt(targetCount, 10, 32)
		count := int32(1)
		search := func(p framework.WaitingPod) {
			// TODO: add more checks for these pods, e.g. whether it has been deleted
			if p.GetPod().Annotations["coschedule-name"] == name {
				count++
			}
		}
		mc.handle.IterateOverWaitingPods(search)

		if count < int32(targetCountInt32) {
			msg := fmt.Sprintf("xychu: in Permit %s and %s/%s pods ready.", pod.Name, count, targetCount)
			klog.V(0).Infof(msg)
			return framework.NewStatus(framework.Wait, ""), 5 * time.Minute
		}

		allow := func(p framework.WaitingPod) {
			if p.GetPod().Annotations["coschedule-name"] == name {
				p.Allow()
			}
		}
		mc.handle.IterateOverWaitingPods(allow)
		return nil, 0 * time.Second
	}

	//if name, okay := pod.ObjectMeta.Annotations["coschedule-name"]; okay {
	//	targetCount, _ := pod.ObjectMeta.Annotations["coschedule-count"]
	//	state.RLock()
	//	defer state.RUnlock()
	//	if v, e := state.Read(framework.StateKey(name)); e == nil {
	//		if value, ok := v.(*stateData); ok && fmt.Sprint(value.readyCount) == targetCount {
	//			msg := fmt.Sprintf("xychu: in Permit %s all pods are ready %v", pod.Name, value.readyPods)
	//			klog.V(0).Infof(msg)
	//			return framework.NewStatus(framework.Success, "All pods are ready."), 0 * time.Second
	//		} else {
	//			msg := fmt.Sprintf("xychu: in Peuurmit %s and %s/%s pods ready.", pod.Name, value.readyCount, targetCount)
	//			klog.V(0).Infof(msg)
	//			return framework.NewStatus(framework.Wait, "{}/{} pod(s) not ready"), 300 * time.Second
	//		}
	//	}
	//}
	msg = fmt.Sprintf("xychu: in Permit %s has no coschedule annotation", pod.Name)
	klog.V(0).Infof(msg)
	return framework.NewStatus(framework.Success, "No coscheduling annotation found."), 0 * time.Second
}

// New initializes a new plugin and returns it.
func New(_ *runtime.Unknown, h framework.FrameworkHandle) (framework.Plugin, error) {
	return &CoschedulingPlugin{handle: h}, nil
}
