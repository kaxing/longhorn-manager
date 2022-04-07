package orphan

import (
	"fmt"
	"reflect"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type orphanValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &orphanValidator{ds: ds}
}

func (o *orphanValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "orphans",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Orphan{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Update,
		},
	}
}

func (o *orphanValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldOrphan := oldObj.(*longhorn.Orphan)
	newOrphan := newObj.(*longhorn.Orphan)

	if !reflect.DeepEqual(oldOrphan.Spec, newOrphan.Spec) {
		return werror.NewInvalidError(fmt.Sprintf("orphan %v spec fileds are immutable", oldOrphan.Name), "")
	}

	return nil
}
