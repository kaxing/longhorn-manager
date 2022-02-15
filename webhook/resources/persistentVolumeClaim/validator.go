package persistentvolumeclaim

import (
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
)

type persistentVolumeClaimValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &persistentVolumeClaimValidator{ds: ds}
}

func (p *persistentVolumeClaimValidator) Resource() admission.Resource {
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

func (p *persistentVolumeClaimValidator) Create(request *admission.Request, newObj runtime.Object) error {
	newPV := newObj.(*corev1.PersistentVolumeClaim)

	return p.validateOnCreate(newPV)
}

func (p *persistentVolumeClaimValidator) validateOnCreate(newPV *corev1.PersistentVolumeClaim) error {
	return nil
}
