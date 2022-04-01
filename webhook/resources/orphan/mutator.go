package orphan

import (
	"encoding/json"
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
	"github.com/pkg/errors"
)

var (
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group
)

type orphanMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &orphanMutator{ds: ds}
}

func (o *orphanMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "orphans",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Orphan{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (o *orphanMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	orphan := newObj.(*longhorn.Orphan)

	// Merge the user created and longhorn specific labels
	labels := orphan.Labels
	if labels == nil {
		labels = map[string]string{}
	}

	longhornLabels := types.GetOrphanLabels(orphan.Spec.NodeID, orphan.Spec.Parameters[longhorn.OrphanDiskUUID], orphan.Name)
	for k, v := range longhornLabels {
		labels[k] = v
	}
	bytes, err := json.Marshal(labels)
	if err != nil {
		err = errors.Wrapf(err, "failed to get JSON encoding for orphan %v labels", orphan.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/labels", "value": %v}`, string(bytes)))

	if err := util.AddFinalizer(longhornFinalizerKey, orphan); err != nil {
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	bytes, err = json.Marshal(orphan.Finalizers)
	if err != nil {
		err = errors.Wrapf(err, "failed to get JSON encoding for orphan %v finalizers", orphan.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/finalizers", "value": %v}`, string(bytes)))

	return patchOps, nil
}
