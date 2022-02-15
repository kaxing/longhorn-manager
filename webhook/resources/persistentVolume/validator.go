package persistentvolume

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type persistentVolumeValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &persistentVolumeValidator{ds: ds}
}

func (p *persistentVolumeValidator) Resource() admission.Resource {
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

func (p *persistentVolumeValidator) Create(request *admission.Request, newObj runtime.Object) error {
	newPV := newObj.(*corev1.PersistentVolume)

	return p.validateOnCreate(newPV)
}

func (p *persistentVolumeValidator) validateOnCreate(newPV *corev1.PersistentVolume) error {
	v, err := p.ds.GetVolume(newPV.Spec.PersistentVolumeSource.CSI.VolumeHandle)
	if err != nil {
		return err
	}

	if v.Status.IsStandby {
		return werror.NewInvalidError(fmt.Sprintf("cannot create PV for standby volume %v", v.Name), "")
	}

	if v.Status.KubernetesStatus.PVName != "" {
		return werror.NewInvalidError(fmt.Sprintf("volume already had PV %v", v.Status.KubernetesStatus.PVName), "")
	}

	return nil
}
