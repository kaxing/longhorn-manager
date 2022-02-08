package backingimage

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type backingImageValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &backingImageValidator{ds: ds}
}

func (b *backingImageValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backingimages",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.BackingImage{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Delete,
		},
	}
}

func (b *backingImageValidator) Create(request *admission.Request, newObj runtime.Object) error {
	backingImage := newObj.(*longhorn.BackingImage)

	return b.validateBackingImageOnCreate(backingImage)
}

func (b *backingImageValidator) Delete(request *admission.Request, oldObj runtime.Object) error {
	backingImage := oldObj.(*longhorn.BackingImage)

	return b.validateBackingImageOnDelete(backingImage)
}

func (b *backingImageValidator) validateBackingImageOnCreate(backingImage *longhorn.BackingImage) error {
	if !util.ValidateName(backingImage.Name) {
		return fmt.Errorf("invalid name %v", backingImage.Name)
	}

	if len(backingImage.Spec.Checksum) != 0 {
		if !util.ValidateChecksumSHA512(backingImage.Spec.Checksum) {
			return fmt.Errorf("invalid checksum %v", backingImage.Spec.Checksum)
		}
	}

	switch longhorn.BackingImageDataSourceType(backingImage.Spec.SourceType) {
	case longhorn.BackingImageDataSourceTypeDownload:
		if backingImage.Spec.SourceParameters[longhorn.DataSourceTypeDownloadParameterURL] == "" {
			return fmt.Errorf("invalid parameter %+v for source type %v", backingImage.Spec.SourceParameters, backingImage.Spec.SourceType)
		}
	case longhorn.BackingImageDataSourceTypeUpload:
	case longhorn.BackingImageDataSourceTypeExportFromVolume:
		volumeName := backingImage.Spec.SourceParameters[controller.DataSourceTypeExportFromVolumeParameterVolumeName]
		if volumeName == "" {
			return fmt.Errorf("invalid parameter %+v for source type %v", backingImage.Spec.SourceParameters, backingImage.Spec.SourceType)
		}
		v, err := b.ds.GetVolume(volumeName)
		if err != nil {
			return fmt.Errorf("failed to get volume %v before exporting backing image", volumeName)
		}
		if v.Status.Robustness == longhorn.VolumeRobustnessFaulted {
			return fmt.Errorf("cannot export a backing image from faulted volume %v", volumeName)
		}
		eiName := types.GetEngineImageChecksumName(v.Status.CurrentImage)
		ei, err := b.ds.GetEngineImage(eiName)
		if err != nil {
			return errors.Wrapf(err, "failed to get then check engine image %v for volume %v before exporting backing image", eiName, volumeName)
		}
		if ei.Status.CLIAPIVersion < engineapi.CLIVersionFive {
			return fmt.Errorf("engine image %v CLI version %v doesn't support this feature, please upgrade engine for volume %v before exporting backing image from the volume", eiName, ei.Status.CLIAPIVersion, volumeName)
		}
		// By default the exported file type is raw.
		if backingImage.Spec.SourceParameters[manager.DataSourceTypeExportFromVolumeParameterExportType] == "" {
			backingImage.Spec.SourceParameters[manager.DataSourceTypeExportFromVolumeParameterExportType] = manager.DataSourceTypeExportFromVolumeParameterExportTypeRAW
		}
		if backingImage.Spec.SourceParameters[manager.DataSourceTypeExportFromVolumeParameterExportType] != manager.DataSourceTypeExportFromVolumeParameterExportTypeRAW &&
			backingImage.Spec.SourceParameters[manager.DataSourceTypeExportFromVolumeParameterExportType] != manager.DataSourceTypeExportFromVolumeParameterExportTypeQCOW2 {
			return fmt.Errorf("unsupported export type %v", backingImage.Spec.SourceParameters[manager.DataSourceTypeExportFromVolumeParameterExportType])
		}
	default:
		return fmt.Errorf("unknown backing image source type %v", backingImage.Spec.SourceType)
	}

	if _, err := b.ds.GetBackingImage(backingImage.Name); err == nil {
		return fmt.Errorf("backing image already exists")
	} else if !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "failed to check backing image existence before creation")
	}

	return nil
}

func (b *backingImageValidator) validateBackingImageOnDelete(backingImage *longhorn.BackingImage) error {
	replicas, err := b.ds.ListReplicasByBackingImage(backingImage.Name)
	if err != nil {
		return err
	}
	if len(replicas) != 0 {
		return fmt.Errorf("cannot delete backing image %v since there are replicas using it", backingImage.Name)
	}
	return nil
}
