package volume

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
	"github.com/sirupsen/logrus"
)

type volumeValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &volumeValidator{ds: ds}
}

func (e *volumeValidator) Resource() admission.Resource {
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

func (v *volumeValidator) Create(request *admission.Request, newObj runtime.Object) error {
	volume := newObj.(*longhorn.Volume)

	return v.validateVolumeOnCreate(volume)
}

func (v *volumeValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldVolume := oldObj.(*longhorn.Volume)
	newVolume := newObj.(*longhorn.Volume)

	return v.validateVolumeOnUpdate(oldVolume, newVolume)
}

func (v *volumeValidator) validateVolumeOnCreate(volume *longhorn.Volume) error {
	return nil
}

func (v *volumeValidator) validateVolumeOnUpdate(oldVolume, newVolume *longhorn.Volume) error {
	if err := v.validateAttach(oldVolume, newVolume); err != nil {
		return err
	}

	if err := v.validateDetach(oldVolume, newVolume); err != nil {
		return err
	}

	if err := v.validateUpdateDataLocality(oldVolume, newVolume); err != nil {
		return err
	}

	if err := v.validateUpdateAccessMode(oldVolume, newVolume); err != nil {
		return err
	}

	if err := v.validateActivate(oldVolume, newVolume); err != nil {
		return err
	}

	if err := v.validateUpdateReplicaCount(oldVolume, newVolume); err != nil {
		return err
	}

	if err := v.validateUpdateReplicaAutoBalance(oldVolume, newVolume); err != nil {
		return err
	}

	if err := v.validateExpand(oldVolume, newVolume); err != nil {
		return err
	}

	if err := v.validateCancelExpansion(oldVolume, newVolume); err != nil {
		return err
	}

	if err := datastore.CheckVolume(newVolume); err != nil {
		return err
	}

	return nil
}

func (v *volumeValidator) validateAttach(oldVolume, newVolume *longhorn.Volume) error {
	if oldVolume.Spec.NodeID == newVolume.Spec.NodeID {
		logrus.Debugf("Volume %v is already attached to node %v", oldVolume.Name, oldVolume.Spec.NodeID)
		return nil
	}

	if oldVolume.Spec.MigrationNodeID == newVolume.Spec.NodeID {
		logrus.Debugf("Volume %v is already migrating to node %v from node %v", oldVolume.Name, newVolume.Spec.NodeID, oldVolume.Spec.NodeID)
		return nil
	}

	if isReady, err := v.ds.CheckEngineImageReadyOnAtLeastOneVolumeReplica(oldVolume.Spec.EngineImage, oldVolume.Name, newVolume.Spec.NodeID); !isReady {
		if err != nil {
			return werror.NewInvalidError(fmt.Sprintf("cannot attach volume %v with image %v: %v", oldVolume.Name, oldVolume.Spec.EngineImage, err), "")
		}
		return werror.NewInvalidError(fmt.Sprintf("cannot attach volume %v because the engine image %v is not deployed on at least one of the the replicas' nodes or the node that the volume is going to attach to", oldVolume.Name, oldVolume.Spec.EngineImage), "")
	}

	restoreCondition := types.GetCondition(oldVolume.Status.Conditions, longhorn.VolumeConditionTypeRestore)
	if restoreCondition.Status == longhorn.ConditionStatusTrue {
		return werror.NewInvalidError(fmt.Sprintf("volume %v is restoring data", newVolume.Name), "")
	}

	if oldVolume.Status.RestoreRequired {
		return werror.NewInvalidError(fmt.Sprintf("volume %v is pending restoring", newVolume.Name), "")
	}

	if oldVolume.Spec.AccessMode != longhorn.AccessModeReadWriteMany && oldVolume.Status.State != longhorn.VolumeStateDetached {
		return werror.NewInvalidError(fmt.Sprintf("invalid state %v to attach RWO volume %v", oldVolume.Status.State, newVolume.Name), "")
	}

	isVolumeShared := oldVolume.Spec.AccessMode == longhorn.AccessModeReadWriteMany && !oldVolume.Spec.Migratable
	isVolumeDetached := oldVolume.Spec.NodeID == ""

	if isVolumeDetached {
		if !isVolumeShared || newVolume.Spec.DisableFrontend {
			logrus.Infof("Volume %v attachment to %v with disableFrontend %v requested", oldVolume.Name, oldVolume.Spec.NodeID, newVolume.Spec.DisableFrontend)
		}
	} else if isVolumeShared {
		// shared volumes only need to be attached if maintenance mode is requested
		// otherwise we just set the disabled frontend and last attached by states
		logrus.Debugf("No need to attach volume %v since it's shared via %v", oldVolume.Name, oldVolume.Status.ShareEndpoint)
	} else {
		// non shared volume that is already attached needs to be migratable
		// to be able to attach to a new node, without detaching from the previous node
		if !oldVolume.Spec.Migratable {
			return werror.NewInvalidError(fmt.Sprintf("non migratable volume %v cannot attach to node %v is already attached to node %v", oldVolume.Name, newVolume.Spec.NodeID, oldVolume.Spec.NodeID), "")
		}
		if oldVolume.Spec.MigrationNodeID != "" && oldVolume.Spec.MigrationNodeID != newVolume.Spec.NodeID {
			return werror.NewInvalidError(fmt.Sprintf("unable to migrate volume %v from %v to %v since it's already migrating to %v",
				oldVolume.Name, oldVolume.Spec.NodeID, newVolume.Spec.NodeID, oldVolume.Spec.MigrationNodeID), "")
		}
		if oldVolume.Status.State != longhorn.VolumeStateAttached {
			return werror.NewInvalidError(fmt.Sprintf("invalid volume state to start migration %v", oldVolume.Status.State), "")
		}
		if oldVolume.Status.Robustness != longhorn.VolumeRobustnessHealthy && oldVolume.Status.Robustness != longhorn.VolumeRobustnessDegraded {
			return werror.NewInvalidError("volume must be healthy or degraded to start migration", "")
		}
		if oldVolume.Spec.EngineImage != oldVolume.Status.CurrentImage {
			return werror.NewInvalidError("upgrading in process for volume, cannot start migration", "")
		}
		if oldVolume.Spec.Standby || oldVolume.Status.IsStandby {
			return werror.NewInvalidError("dr volume migration is not supported", "")
		}
		if oldVolume.Status.ExpansionRequired {
			return werror.NewInvalidError("cannot migrate volume while an expansion is required", "")
		}

		if oldVolume.Spec.MigrationNodeID != newVolume.Spec.NodeID {
			return werror.NewInvalidError(fmt.Sprintf("invalid migrationNodeID of volume %v", oldVolume.Name), "")
		}
	}

	return nil
}

func (v *volumeValidator) validateDetach(oldVolume, newVolume *longhorn.Volume) error {
	if oldVolume.Status.IsStandby {
		return werror.NewInvalidError(fmt.Sprintf("cannot detach standby volume %v", oldVolume.Name), "")
	}

	if oldVolume.Spec.NodeID == "" && oldVolume.Spec.MigrationNodeID == "" {
		logrus.Infof("No need to detach volume %v is already detached from all nodes", oldVolume.Name)
		return nil
	}

	// shared volumes only need to be detached if they are attached in maintenance mode
	if oldVolume.Spec.AccessMode == longhorn.AccessModeReadWriteMany && !oldVolume.Spec.Migratable && !oldVolume.Spec.DisableFrontend {
		logrus.Infof("No need to detach volume %v since it's shared via %v", oldVolume.Name, oldVolume.Status.ShareEndpoint)
		return nil
	}

	if newVolume.Spec.NodeID != "" && newVolume.Spec.NodeID != oldVolume.Spec.NodeID && newVolume.Spec.NodeID != oldVolume.Spec.MigrationNodeID {
		logrus.Infof("no need to detach volume %v since it's not attached to node %v", oldVolume.Name, newVolume.Spec.NodeID)
		return nil
	}

	isMigratingVolume := oldVolume.Spec.Migratable && oldVolume.Spec.MigrationNodeID != "" && oldVolume.Spec.NodeID != ""
	isMigrationConfirmation := isMigratingVolume && newVolume.Spec.NodeID == oldVolume.Spec.NodeID
	isMigrationRollback := isMigratingVolume && newVolume.Spec.NodeID == oldVolume.Spec.MigrationNodeID

	// Since all invalid/unexcepted cases have been handled above, we only need to take care of regular detach or migration here.
	if isMigrationConfirmation {
		// Need to make sure both engines are running.
		// If the old one crashes, the volume will fall into the auto reattachment flow. Then allowing migration confirmation will mess up the volume.
		if !v.isVolumeAvailableOnNode(newVolume.Name, oldVolume.Spec.MigrationNodeID) || !v.isVolumeAvailableOnNode(newVolume.Name, oldVolume.Spec.NodeID) {
			return werror.NewInvalidError("migration is not ready yet", "")
		}

		if newVolume.Spec.NodeID != newVolume.Spec.MigrationNodeID || newVolume.Spec.MigrationNodeID != "" {
			return werror.NewInvalidError(fmt.Sprintf("invalid nodeID and migrationNodeID for volume %v migration confirmation", newVolume.Name), "")
		}
	} else if isMigrationRollback {
		if newVolume.Spec.MigrationNodeID != "" {
			return werror.NewInvalidError(fmt.Sprintf("invalid migrationNodeID for volume %v migration rollback", newVolume.Name), "")
		}
	} else {
		if newVolume.Spec.NodeID != "" || newVolume.Spec.MigrationNodeID != "" {
			return werror.NewInvalidError(fmt.Sprintf("invalid nodeID or migrationNodeID for volume %v detach", newVolume.Name), "")
		}
	}

	return nil
}

func (v *volumeValidator) validateUpdateDataLocality(oldVolume, newVolume *longhorn.Volume) error {
	if oldVolume.Spec.DataLocality == newVolume.Spec.DataLocality {
		logrus.Debugf("Volume %v already has data locality %v", oldVolume.Spec.DataLocality, newVolume.Spec.DataLocality)
		return nil
	}

	if err := types.ValidateDataLocality(newVolume.Spec.DataLocality); err != nil {
		return err
	}

	return nil
}

func (v *volumeValidator) validateUpdateAccessMode(oldVolume, newVolume *longhorn.Volume) error {
	if oldVolume.Spec.AccessMode == newVolume.Spec.AccessMode {
		return nil
	}

	if oldVolume.Spec.NodeID != "" || oldVolume.Status.State != longhorn.VolumeStateDetached {
		return werror.NewInvalidError("can only update volume access mode while volume is detached", "")
	}

	return nil
}

func (v *volumeValidator) validateUpdateReplicaCount(oldVolume, newVolume *longhorn.Volume) error {
	if oldVolume.Spec.NumberOfReplicas == newVolume.Spec.NumberOfReplicas {
		return nil
	}

	if err := types.ValidateReplicaCount(newVolume.Spec.NumberOfReplicas); err != nil {
		return err
	}

	if oldVolume.Spec.NodeID == "" || oldVolume.Status.State != longhorn.VolumeStateAttached {
		return werror.NewInvalidError(fmt.Sprintf("invalid volume state to update replica count%v", oldVolume.Status.State), "")
	}
	if oldVolume.Spec.EngineImage != oldVolume.Status.CurrentImage {
		return werror.NewInvalidError("upgrading in process, cannot update replica count", "")
	}
	if oldVolume.Spec.MigrationNodeID != "" {
		return werror.NewInvalidError("migration in process, cannot update replica count", "")
	}

	return nil
}

func (v *volumeValidator) validateUpdateReplicaAutoBalance(oldVolume, newVolume *longhorn.Volume) error {
	if oldVolume.Spec.ReplicaAutoBalance == newVolume.Spec.ReplicaAutoBalance {
		return nil
	}
	if err := types.ValidateReplicaAutoBalance(newVolume.Spec.ReplicaAutoBalance); err != nil {
		return err
	}
	return nil
}

func (v *volumeValidator) validateActivate(oldVolume, newVolume *longhorn.Volume) error {
	if !oldVolume.Status.IsStandby {
		logrus.Infof("volume %v is already in active mode", oldVolume.Name)
		return nil
	}
	if !oldVolume.Spec.Standby {
		logrus.Infof("volume %v is being activated", oldVolume.Name)
		return nil
	}

	if newVolume.Spec.Frontend != longhorn.VolumeFrontendBlockDev && newVolume.Spec.Frontend != longhorn.VolumeFrontendISCSI {
		return werror.NewInvalidError(fmt.Sprintf("invalid frontend %v", newVolume.Spec.Frontend), "")
	}

	var engine *longhorn.Engine
	es, err := v.ds.ListVolumeEngines(oldVolume.Name)
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to list engines for volume %v: %v", oldVolume.Name, err), "")
	}
	if len(es) != 1 {
		return werror.NewInvalidError(fmt.Sprintf("found more than 1 engines for volume %v", oldVolume.Name), "")
	}
	for _, e := range es {
		engine = e
	}

	if oldVolume.Status.LastBackup != engine.Status.LastRestoredBackup || engine.Spec.RequestedBackupRestore != engine.Status.LastRestoredBackup {
		logrus.Infof("Standby volume %v will be activated after finishing restoration, "+
			"backup volume's latest backup: %v, "+
			"engine requested backup restore: %v, engine last restored backup: %v",
			oldVolume.Name, oldVolume.Status.LastBackup, engine.Spec.RequestedBackupRestore, engine.Status.LastRestoredBackup)
	}

	return nil
}

func (v *volumeValidator) validateExpand(oldVolume, newVolume *longhorn.Volume) error {
	return nil
}

func (v *volumeValidator) validateCancelExpansion(oldVolume, newVolume *longhorn.Volume) error {
	return nil
}

func (v *volumeValidator) validateEngineUpgrade(oldVolume, newVolume *longhorn.Volume) error {
	if oldVolume.Spec.EngineImage == newVolume.Spec.EngineImage {
		logrus.Infof("upgrading in process for volume %v engine image from %v to %v already",
			oldVolume.Name, oldVolume.Status.CurrentImage, oldVolume.Spec.EngineImage)
		return nil
	}

	// Only allow to upgrade to the default engine image if the setting `Automatically upgrade volumes' engine to the default engine image` is enabled
	concurrentAutomaticEngineUpgradePerNodeLimit, err := v.ds.GetSettingAsInt(types.SettingNameConcurrentAutomaticEngineUpgradePerNodeLimit)
	if err != nil {
		return err
	}
	if concurrentAutomaticEngineUpgradePerNodeLimit > 0 {
		defaultEngineImage, err := v.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
		if err != nil {
			return err
		}
		if newVolume.Spec.EngineImage != defaultEngineImage {
			return werror.NewInvalidError(fmt.Sprintf("updrading to %v is not allowed. "+
				"Only allow to upgrade to the default engine image %v because the setting "+
				"`Concurrent Automatic Engine Upgrade Per Node Limit` is greater than 0",
				newVolume.Spec.EngineImage, defaultEngineImage), "")
		}
	}

	if oldVolume.Spec.EngineImage != oldVolume.Status.CurrentImage && newVolume.Spec.EngineImage != oldVolume.Status.CurrentImage {
		return werror.NewInvalidError(fmt.Sprintf("upgrading in process for volume %v engine image from %v to %v, cannot upgrade to another engine image",
			oldVolume.Name, oldVolume.Status.CurrentImage, oldVolume.Spec.EngineImage), "")
	}

	if isReady, err := v.ds.CheckEngineImageReadyOnAllVolumeReplicas(newVolume.Spec.EngineImage, oldVolume.Name, oldVolume.Status.CurrentNodeID); !isReady {
		if err != nil {
			return werror.NewInvalidError(fmt.Sprintf("cannot upgrade engine image for volume %v from image %v to image %v: %v", oldVolume.Name, oldVolume.Spec.EngineImage, newVolume.Spec.EngineImage, err), "")
		}
		return werror.NewInvalidError(fmt.Sprintf("cannot upgrade engine image for volume %v from image %v to image %v because the engine image %v is not deployed on the replicas' nodes or the node that the volume is attached to", oldVolume.Name, oldVolume.Spec.EngineImage, newVolume.Spec.EngineImage, newVolume.Spec.EngineImage), "")
	}

	if isReady, err := v.ds.CheckEngineImageReadyOnAllVolumeReplicas(oldVolume.Status.CurrentImage, oldVolume.Name, oldVolume.Status.CurrentNodeID); !isReady {
		if err != nil {
			return werror.NewInvalidError(fmt.Sprintf("cannot upgrade engine image for volume %v from image %v to image %v: %v", oldVolume.Name, oldVolume.Spec.EngineImage, newVolume.Spec.EngineImage, err), "")
		}
		return werror.NewInvalidError(fmt.Sprintf("cannot upgrade engine image for volume %v from image %v to image %v because the volume's current engine image %v is not deployed on the replicas' nodes or the node that the volume is attached to", oldVolume.Name, oldVolume.Spec.EngineImage, newVolume.Spec.EngineImage, oldVolume.Status.CurrentImage), "")
	}

	if oldVolume.Spec.MigrationNodeID != "" {
		return werror.NewInvalidError("cannot upgrade during migration", "")
	}

	// Note: Rebuild is not supported for old DR volumes and the handling of a degraded DR volume live upgrade will get stuck.
	// Hence if you modify this part, the live upgrade should be prevented in API level for all old DR volumes.
	if oldVolume.Status.State == longhorn.VolumeStateAttached && oldVolume.Status.Robustness != longhorn.VolumeRobustnessHealthy {
		return werror.NewInvalidError(fmt.Sprintf("cannot do live upgrade for a unhealthy volume %v", oldVolume.Name), "")
	}

	return nil
}

func (v *volumeValidator) isVolumeAvailableOnNode(volume, node string) bool {
	es, _ := v.ds.ListVolumeEngines(volume)
	for _, e := range es {
		if e.Spec.NodeID != node {
			continue
		}
		if e.DeletionTimestamp != nil {
			continue
		}
		if e.Spec.DesireState != longhorn.InstanceStateRunning || e.Status.CurrentState != longhorn.InstanceStateRunning {
			continue
		}
		hasAvailableReplica := false
		for _, mode := range e.Status.ReplicaModeMap {
			hasAvailableReplica = hasAvailableReplica || mode == longhorn.ReplicaModeRW
		}
		if !hasAvailableReplica {
			continue
		}
		return true
	}

	return false
}
