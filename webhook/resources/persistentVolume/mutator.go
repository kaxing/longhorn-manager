package persistentvolume

import (
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
)

type persistentVolumeMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &persistentVolumeMutator{ds: ds}
}

func (p *persistentVolumeMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "persistentvolumes",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &corev1.PersistentVolume{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (p *persistentVolumeMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutatePersistentVolumeOnCreate(newObj)
}

func mutatePersistentVolumeOnCreate(newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	return patchOps, nil
}
