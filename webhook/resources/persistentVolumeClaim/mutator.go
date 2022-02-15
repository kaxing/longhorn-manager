package persistentvolumeclaim

import (
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
)

type persistentVolumeClaimMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &persistentVolumeClaimMutator{ds: ds}
}

func (p *persistentVolumeClaimMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "persistentvolumeclaims",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &corev1.PersistentVolumeClaim{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (p *persistentVolumeClaimMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutatePersistentVolumeClaim(newObj)
}

func (p *persistentVolumeClaimMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutatePersistentVolumeClaim(newObj)
}

func mutatePersistentVolumeClaim(newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	return patchOps, nil
}
