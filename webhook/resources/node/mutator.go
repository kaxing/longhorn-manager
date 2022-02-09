package node

import (
	"encoding/json"
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
)

type nodeMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &nodeMutator{ds: ds}
}

func (n *nodeMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "nodes",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Node{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (n *nodeMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateNode(newObj)
}

func (n *nodeMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateNode(newObj)
}

func mutateNode(newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	node := newObj.(*longhorn.Node)

	if node.Spec.Tags == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/tags", "value": []}`)
	} else {
		if len(node.Spec.Tags) > 0 {
			tags, err := util.ValidateTags(node.Spec.Tags)
			if err != nil {
				return nil, err
			}
			bytes, err := json.Marshal(tags)
			if err != nil {
				return nil, err
			}
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/tags", "value": %s}`, string(bytes)))
		}
	}

	if node.Spec.Disks == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/disks", "value": {}}`)
	} else {
		for name, disk := range node.Spec.Disks {
			if disk.Tags == nil {
				patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/disks/%s/tags", "value": []}`, name))
			} else {
				if len(disk.Tags) > 0 {
					tags, err := util.ValidateTags(disk.Tags)
					if err != nil {
						return nil, err
					}
					bytes, err := json.Marshal(tags)
					if err != nil {
						return nil, err
					}
					patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/disks/%s/tags", "value": %s}`, name, string(bytes)))
				}
			}
		}
	}

	return patchOps, nil
}
