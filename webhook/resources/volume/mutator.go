package volume

import (
	"fmt"
	"strconv"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type volumeMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &volumeMutator{ds: ds}
}

func (v *volumeMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "volumes",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Volume{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (v *volumeMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	volume := newObj.(*longhorn.Volume)

	if volume.Spec.ReplicaAutoBalance == "" {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/replicaAutoBalance", "value": "ignored"}`)
	}
	if volume.Spec.DiskSelector == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/diskSelector", "value": []}`)
	}
	if volume.Spec.NodeSelector == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/nodeSelector", "value": []}`)
	}
	if volume.Spec.RecurringJobs == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/recurringJobs", "value": []}`)
	}
	for id, job := range volume.Spec.RecurringJobs {
		if job.Groups == nil {
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/recurringJobs/%d/groups", "value": []}`, id))
		}
		if job.Labels == nil {
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/recurringJobs/%d/labels", "value": {}}`, id))
		}
	}

	size := util.RoundUpSize(volume.Spec.Size)
	if size != volume.Spec.Size {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/size", "value": "%s"}`, strconv.FormatInt(size, 10)))
	}

	if volume.Spec.NumberOfReplicas == 0 {
		numberOfReplicas, err := v.getDefaultReplicaCount()
		if err != nil {
			return nil, errors.Wrap(err, "BUG: cannot get valid number for setting default replica count")
		}

		logrus.Infof("Use the default number of replicas %v", numberOfReplicas)
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/numberOfReplicas", "value": %v}`, numberOfReplicas))
	}

	if string(volume.Spec.ReplicaAutoBalance) == "" {
		replicaAutoBalance := longhorn.ReplicaAutoBalanceIgnored
		logrus.Infof("Use the %v to inherit global replicas auto-balance setting", replicaAutoBalance)

		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/replicaAutoBalance", "value": "%s"}`, string(replicaAutoBalance)))
	}

	if string(volume.Spec.DataLocality) == "" {
		defaultDataLocality, err := v.ds.GetSettingValueExisted(types.SettingNameDefaultDataLocality)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot get valid mode for setting default data locality for volume: %v", volume.Name)
		}

		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/dataLocality", "value": "%s"}`, defaultDataLocality))
	}

	if string(volume.Spec.AccessMode) == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/accessMode", "value": "%s"}`, string(longhorn.AccessModeReadWriteOnce)))
	}

	return patchOps, nil
}

func (v *volumeMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	volume := newObj.(*longhorn.Volume)

	if volume.Spec.ReplicaAutoBalance == "" {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/replicaAutoBalance", "value": "ignored"}`)
	}
	if volume.Spec.DiskSelector == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/diskSelector", "value": []}`)
	}
	if volume.Spec.NodeSelector == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/nodeSelector", "value": []}`)
	}
	if volume.Spec.RecurringJobs == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/recurringJobs", "value": []}`)
	}
	for id, job := range volume.Spec.RecurringJobs {
		if job.Groups == nil {
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/recurringJobs/%d/groups", "value": []}`, id))
		}
		if job.Labels == nil {
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/recurringJobs/%d/labels", "value": {}}`, id))
		}
	}

	size := util.RoundUpSize(volume.Spec.Size)
	if size != volume.Spec.Size {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/size", "value": "%s"}`, strconv.FormatInt(size, 10)))
	}
	return patchOps, nil
}

func (v *volumeMutator) getDefaultReplicaCount() (int, error) {
	c, err := v.ds.GetSettingAsInt(types.SettingNameDefaultReplicaCount)
	if err != nil {
		return 0, err
	}
	return int(c), nil
}
