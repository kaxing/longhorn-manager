package recurringjob

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type recurringJobValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &recurringJobValidator{ds: ds}
}

func (r *recurringJobValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "recurringjobs",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.RecurringJob{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (r *recurringJobValidator) Create(request *admission.Request, newObj runtime.Object) error {
	recurringJob := newObj.(*longhorn.RecurringJob)

	if recurringJob.Spec.Task != longhorn.RecurringJobTypeBackup && recurringJob.Spec.Task != longhorn.RecurringJobTypeSnapshot {
		return werror.NewInvalidError(fmt.Sprintf("recurring job type %v is not valid", recurringJob.Spec.Task), "")
	}

	if !util.ValidateName(recurringJob.Name) {
		return werror.NewInvalidError(fmt.Sprintf("invalid name %v", recurringJob.Name), "")
	}

	jobs := []longhorn.RecurringJobSpec{
		{
			Name:        recurringJob.Spec.Name,
			Groups:      recurringJob.Spec.Groups,
			Task:        recurringJob.Spec.Task,
			Cron:        recurringJob.Spec.Cron,
			Retain:      recurringJob.Spec.Retain,
			Concurrency: recurringJob.Spec.Concurrency,
			Labels:      recurringJob.Spec.Labels,
		},
	}

	if err := datastore.ValidateRecurringJobs(jobs); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil

}

func (r *recurringJobValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	newRecurringJob := newObj.(*longhorn.RecurringJob)

	if newRecurringJob.Spec.Task != longhorn.RecurringJobTypeBackup && newRecurringJob.Spec.Task != longhorn.RecurringJobTypeSnapshot {
		return werror.NewInvalidError(fmt.Sprintf("recurring job type %v is not valid", newRecurringJob.Spec.Task), "")
	}

	jobs := []longhorn.RecurringJobSpec{
		{
			Name:        newRecurringJob.Spec.Name,
			Groups:      newRecurringJob.Spec.Groups,
			Task:        newRecurringJob.Spec.Task,
			Cron:        newRecurringJob.Spec.Cron,
			Retain:      newRecurringJob.Spec.Retain,
			Concurrency: newRecurringJob.Spec.Concurrency,
			Labels:      newRecurringJob.Spec.Labels,
		},
	}
	if err := datastore.ValidateRecurringJobs(jobs); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}
