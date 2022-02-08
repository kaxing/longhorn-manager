package engineimage

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/pkg/errors"
)

type engineImageValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &engineImageValidator{ds: ds}
}

func (e *engineImageValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "engineimages",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.EngineImage{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Delete,
		},
	}
}

func (e *engineImageValidator) Delete(request *admission.Request, oldObj runtime.Object) error {
	engineImage := oldObj.(*longhorn.EngineImage)

	return e.validateEngineImageOnDelete(engineImage)
}

func (e *engineImageValidator) validateEngineImageOnDelete(engineImage *longhorn.EngineImage) error {
	defaultImage, err := e.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return errors.Wrap(err, "unable to delete engine image")
	}
	if engineImage.Spec.Image == defaultImage {
		return fmt.Errorf("unable to delete the default engine image")
	}
	if engineImage.Status.RefCount != 0 {
		return fmt.Errorf("unable to delete the engine image while being used")
	}
	return nil
}
