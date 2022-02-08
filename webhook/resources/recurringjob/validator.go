package recurringjob

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
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

	return r.validateRecurringJobOnCreate(recurringJob)
}

func (r *recurringJobValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	recurringJob := oldObj.(*longhorn.RecurringJob)

	return r.validateRecurringJobOnUpdate(recurringJob)
}

func (r *recurringJobValidator) validateRecurringJobOnCreate(recurringJob *longhorn.RecurringJob) error {
	if recurringJob.Spec.Task != longhorn.RecurringJobTypeBackup && recurringJob.Spec.Task != longhorn.RecurringJobTypeSnapshot {
		return fmt.Errorf("recurring job type %v is not valid", recurringJob.Spec.Task)
	}

	if !util.ValidateName(recurringJob.Name) {
		return fmt.Errorf("invalid name %v", recurringJob.Name)
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
		return err
	}

	return nil
}

func (r *recurringJobValidator) validateRecurringJobOnUpdate(recurringJob *longhorn.RecurringJob) error {
	if recurringJob.Spec.Task != longhorn.RecurringJobTypeBackup && recurringJob.Spec.Task != longhorn.RecurringJobTypeSnapshot {
		return fmt.Errorf("recurring job type %v is not valid", recurringJob.Spec.Task)
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
		return err
	}

	return nil
}
