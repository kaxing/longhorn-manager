package node

import (
	"fmt"
	"math"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
)

type nodeValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &nodeValidator{ds: ds}
}

func (n *nodeValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "nodes",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Node{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Update,
			admissionregv1.Delete,
		},
	}
}

func (n *nodeValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	node := newObj.(*longhorn.Node)

	return n.validateNodeOnUpdate(node)
}

func (n *nodeValidator) Delete(request *admission.Request, oldObj runtime.Object) error {
	node := oldObj.(*longhorn.Node)

	return n.validateNodeOnDelete(node)
}

func (n *nodeValidator) validateNodeOnUpdate(node *longhorn.Node) error {
	// Only scheduling disabled node can be evicted
	// Can not enable scheduling on an evicting node
	if node.Spec.EvictionRequested && node.Spec.AllowScheduling {
		return fmt.Errorf("need to disable scheduling on node %v for node eviction, or cancel eviction to enable scheduling on this node", node.Name)
	}

	_, err := util.ValidateTags(node.Spec.Tags)
	if err != nil {
		return err
	}

	if node.Spec.EngineManagerCPURequest < 0 || node.Spec.ReplicaManagerCPURequest < 0 {
		return fmt.Errorf("found invalid EngineManagerCPURequest %v or ReplicaManagerCPURequest %v during node %v update", node.Spec.EngineManagerCPURequest, node.Spec.ReplicaManagerCPURequest, node.Name)
	}

	if node.Spec.EngineManagerCPURequest != 0 || node.Spec.ReplicaManagerCPURequest != 0 {
		kubeNode, err := n.ds.GetKubernetesNode(node.Name)
		if err != nil {
			return err
		}
		allocatableCPU := float64(kubeNode.Status.Allocatable.Cpu().MilliValue())
		engineManagerCPUSetting, err := n.ds.GetSetting(types.SettingNameGuaranteedEngineManagerCPU)
		if err != nil {
			return err
		}
		engineManagerCPUInPercentage := engineManagerCPUSetting.Value
		if node.Spec.EngineManagerCPURequest > 0 {
			engineManagerCPUInPercentage = fmt.Sprintf("%.0f", math.Round(float64(node.Spec.EngineManagerCPURequest)/allocatableCPU*100.0))
		}
		replicaManagerCPUSetting, err := n.ds.GetSetting(types.SettingNameGuaranteedReplicaManagerCPU)
		if err != nil {
			return err
		}
		replicaManagerCPUInPercentage := replicaManagerCPUSetting.Value
		if node.Spec.ReplicaManagerCPURequest > 0 {
			replicaManagerCPUInPercentage = fmt.Sprintf("%.0f", math.Round(float64(node.Spec.ReplicaManagerCPURequest)/allocatableCPU*100.0))
		}
		if err := types.ValidateCPUReservationValues(engineManagerCPUInPercentage, replicaManagerCPUInPercentage); err != nil {
			return err
		}
	}

	// Validate Spec.Disks
	for name, disk := range node.Spec.Disks {
		if disk.StorageReserved < 0 {
			return fmt.Errorf("update disk on node %v error: The storageReserved setting of disk %v(%v) is not valid, should be positive and no more than storageMaximum and storageAvailable", name, name, disk.Path)
		}
		_, err := util.ValidateTags(disk.Tags)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *nodeValidator) validateNodeOnDelete(node *longhorn.Node) error {
	// Only remove node from longhorn without any volumes on it
	replicas, err := n.ds.ListReplicasByNodeRO(node.Name)
	if err != nil {
		return err
	}
	engines, err := n.ds.ListEnginesByNodeRO(node.Name)
	if err != nil {
		return err
	}

	condition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady)
	// Only could delete node from longhorn if kubernetes node missing or manager pod is missing
	if condition.Status == longhorn.ConditionStatusTrue ||
		(condition.Reason != longhorn.NodeConditionReasonKubernetesNodeGone &&
			condition.Reason != longhorn.NodeConditionReasonManagerPodMissing) ||
		node.Spec.AllowScheduling || len(replicas) > 0 || len(engines) > 0 {
		return fmt.Errorf("could not delete node %v with node ready condition is %v, reason is %v, node schedulable %v, and %v replica, %v engine running on it", node.Name,
			condition.Status, condition.Reason, node.Spec.AllowScheduling, len(replicas), len(engines))
	}

	return nil
}
