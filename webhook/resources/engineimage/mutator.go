package engineimage

import (
	"encoding/json"
	"fmt"
	"strings"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/util"
)

type engineImageMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &engineImageMutator{ds: ds}
}

func (e *engineImageMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "engineimages",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.EngineImage{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (e *engineImageMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateEngineImageOnCreate(newObj)
}

func mutateEngineImageOnCreate(newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	engineImage := newObj.(*longhorn.EngineImage)

	image := strings.TrimSpace(engineImage.Spec.Image)
	if image != engineImage.Spec.Image {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/image", "value": "%s"}`, image))
	}

	name := types.GetEngineImageChecksumName(image)
	if name != engineImage.Name {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/name", "value": "%s"}`, name))
	}

	labels := types.GetEngineImageLabels(name)
	bytes, err := json.Marshal(labels)
	if err != nil {
		return nil, err
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/labels", "value": %v}`, string(bytes)))

	finalizers := engineImage.Finalizers
	patchOp, err := util.AddLonghornFinalizer(finalizers)
	if err != nil {
		return nil, err
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
