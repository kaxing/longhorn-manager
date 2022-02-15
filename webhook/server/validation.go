package server

import (
	"net/http"

	"github.com/rancher/wrangler/pkg/webhook"

	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/client"
	"github.com/longhorn/longhorn-manager/webhook/resources/backingimage"
	"github.com/longhorn/longhorn-manager/webhook/resources/engineimage"
	"github.com/longhorn/longhorn-manager/webhook/resources/node"
	persistentvolume "github.com/longhorn/longhorn-manager/webhook/resources/persistentVolume"
	persistentvolumeclaim "github.com/longhorn/longhorn-manager/webhook/resources/persistentVolumeClaim"
	"github.com/longhorn/longhorn-manager/webhook/resources/recurringjob"
	"github.com/longhorn/longhorn-manager/webhook/resources/setting"
	"github.com/longhorn/longhorn-manager/webhook/resources/volume"
)

func Validation(client *client.Client) (http.Handler, []admission.Resource, error) {
	resources := []admission.Resource{}
	validators := []admission.Validator{
		persistentvolume.NewValidator(client.Datastore),
		persistentvolumeclaim.NewValidator(client.Datastore),
		node.NewValidator(client.Datastore),
		setting.NewValidator(client.Datastore),
		engineimage.NewValidator(client.Datastore),
		backingimage.NewValidator(client.Datastore),
		recurringjob.NewValidator(client.Datastore),
		volume.NewValidator(client.Datastore),
	}

	router := webhook.NewRouter()
	for _, v := range validators {
		addHandler(router, admission.AdmissionTypeValidation, admission.NewValidatorAdapter(v))
		resources = append(resources, v.Resource())
	}

	return router, resources, nil
}
