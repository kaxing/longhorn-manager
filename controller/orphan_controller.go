package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

type OrphanController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewOrphanController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) *OrphanController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	oc := &OrphanController{
		baseController: newBaseController("longhorn-orphan", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-orphan-controller"}),
	}

	ds.OrphanInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    oc.enqueueOrphan,
		UpdateFunc: func(old, cur interface{}) { oc.enqueueOrphan(cur) },
		DeleteFunc: oc.enqueueOrphan,
	})
	oc.cacheSyncs = append(oc.cacheSyncs, ds.OrphanInformer.HasSynced)

	ds.NodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(cur interface{}) { oc.enqueueForLonghornNode(cur) },
		UpdateFunc: func(old, cur interface{}) { oc.enqueueForLonghornNode(cur) },
		DeleteFunc: func(cur interface{}) { oc.enqueueForLonghornNode(cur) },
	})
	oc.cacheSyncs = append(oc.cacheSyncs, ds.NodeInformer.HasSynced)

	return oc
}

func (oc *OrphanController) enqueueOrphan(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	oc.queue.Add(key)
}

func (oc *OrphanController) enqueueForLonghornNode(obj interface{}) {
	node, ok := obj.(*longhorn.Node)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		// use the last known state, to enqueue, dependent objects
		node, ok = deletedState.Obj.(*longhorn.Node)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	orphans, err := oc.ds.ListOrphansByNodeRO(node.Name)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to list orphans with label %v=%v since %v",
			types.GetLonghornLabelKey(types.LonghornLabelNode), node.Name, err))
		return
	}

	for _, orphan := range orphans {
		oc.enqueueOrphan(orphan)
	}
}

func (oc *OrphanController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer oc.queue.ShutDown()

	oc.logger.Infof("Start Longhorn Orphan controller")
	defer oc.logger.Infof("Shutting down Longhorn Orphan controller")

	if !cache.WaitForNamedCacheSync(oc.name, stopCh, oc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(oc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (oc *OrphanController) worker() {
	for oc.processNextWorkItem() {
	}
}

func (oc *OrphanController) processNextWorkItem() bool {
	key, quit := oc.queue.Get()
	if quit {
		return false
	}
	defer oc.queue.Done(key)
	err := oc.syncOrphan(key.(string))
	oc.handleErr(err, key)
	return true
}

func (oc *OrphanController) handleErr(err error, key interface{}) {
	if err == nil {
		oc.queue.Forget(key)
		return
	}

	log := oc.logger.WithField("orphan", key)

	if oc.queue.NumRequeues(key) < maxRetries {
		log.WithError(err).Warnf("Error syncing Longhorn orphan %v: %v", key, err)

		oc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	log.WithError(err).Warnf("Dropping Longhorn orphan %v out of the queue: %v", key, err)
	oc.queue.Forget(key)
}

func (oc *OrphanController) syncOrphan(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: fail to sync orphan %v", oc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != oc.namespace {
		return nil
	}
	return oc.reconcile(name)
}

func (oc *OrphanController) reconcile(orphanName string) (err error) {
	orphan, err := oc.ds.GetOrphan(orphanName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	log := getLoggerForOrphan(oc.logger, orphan)

	if !oc.isResponsibleFor(orphan) {
		return nil
	}

	if orphan.Status.OwnerID != oc.controllerID {
		orphan.Status.OwnerID = oc.controllerID
		orphan, err = oc.ds.UpdateOrphanStatus(orphan)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Orphan Controller %v picked up %v", oc.controllerID, orphan.Name)
	}

	if !orphan.DeletionTimestamp.IsZero() {
		defer func() {
			if err == nil {
				err = oc.ds.RemoveFinalizerForOrphan(orphan)
			}
		}()

		// Make sure if the orphan node and controller ID are same.
		// If NO, just delete the orphan resource object.
		if orphan.Spec.NodeID != oc.controllerID {
			return nil
		}

		node, err := oc.ds.GetNode(orphan.Spec.NodeID)
		if err != nil {
			if apierrors.IsNotFound(err) {
				log.Debugf("Only delete the orphan resource object since node %v does not exist", node.Name)
				return nil
			}
			return err
		}

		id := orphan.Spec.Parameters[longhorn.OrphanDiskFsid]
		if node.Status.DiskStatus[id].DiskUUID == orphan.Spec.Parameters[longhorn.OrphanDiskUUID] &&
			types.GetCondition(orphan.Status.Conditions, longhorn.OrphanConditionTypeUnavailable).Status == longhorn.ConditionStatusFalse {
			err := oc.deleteOrphanedData(orphan)
			if err != nil && !apierrors.IsNotFound(err) {
				orphan.Status.Conditions = types.SetCondition(orphan.Status.Conditions,
					longhorn.OrphanConditionTypeError, longhorn.ConditionStatusTrue, "", err.Error())
				log.WithError(err).Errorf("error deleting orphan %v data", orphan.Name)
				return err
			}
		}
		return nil
	}

	existingOrphan := orphan.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingOrphan.Status, orphan.Status) {
			return
		}
		if _, err := oc.ds.UpdateOrphanStatus(orphan); err != nil && apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", orphanName)
			oc.enqueueOrphan(orphan)
		}
	}()

	if err := oc.updateConditions(orphan); err != nil {
		log.WithError(err).Errorf("failed to update conditions for orphan %v", orphan.Name)
		return err
	}

	return nil
}

func getLoggerForOrphan(logger logrus.FieldLogger, orphan *longhorn.Orphan) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"orphan": orphan.Name,
		},
	)
}

func (oc *OrphanController) isResponsibleFor(orphan *longhorn.Orphan) bool {
	return isControllerResponsibleFor(oc.controllerID, oc.ds, orphan.Name, orphan.Spec.NodeID, orphan.Status.OwnerID)
}

func (oc *OrphanController) deleteOrphanedData(orphan *longhorn.Orphan) error {
	oc.logger.Infof("Deleting orphan %v replica directory %v in disk %v on node %v",
		orphan.Name, orphan.Spec.Parameters[longhorn.OrphanDataName],
		orphan.Spec.Parameters[longhorn.OrphanDiskPath], orphan.Status.OwnerID)

	return util.DeleteReplicaDirectoryName(orphan.Spec.Parameters[longhorn.OrphanDiskPath], orphan.Spec.Parameters[longhorn.OrphanDataName])
}

func (oc *OrphanController) updateConditions(orphan *longhorn.Orphan) error {
	if err := oc.updateUnavailableCondition(orphan); err != nil {
		return err
	}

	if types.GetCondition(orphan.Status.Conditions, longhorn.OrphanConditionTypeError).Status != longhorn.ConditionStatusTrue {
		orphan.Status.Conditions = types.SetCondition(orphan.Status.Conditions, longhorn.OrphanConditionTypeError, longhorn.ConditionStatusFalse, "", "")
	}

	return nil
}

func (oc *OrphanController) updateUnavailableCondition(orphan *longhorn.Orphan) (err error) {
	reason := ""

	defer func() {
		if err != nil {
			return
		}

		status := longhorn.ConditionStatusFalse
		if reason != "" {
			status = longhorn.ConditionStatusTrue
		}

		orphan.Status.Conditions = types.SetCondition(orphan.Status.Conditions, longhorn.OrphanConditionTypeUnavailable, status, reason, "")
	}()

	if autoDeletion, err := oc.ds.GetSettingAsBool(types.SettingNameOrphanAutoDeletion); err == nil {
		if autoDeletion {
			reason = longhorn.OrphanConditionTypeUnavailableReasonAutoDeletionEnabled
			return nil
		}
	} else {
		return errors.Wrapf(err, "failed to get %v setting", types.SettingNameOrphanAutoDeletion)
	}

	if orphan.Spec.NodeID != orphan.Status.OwnerID {
		reason = longhorn.OrphanConditionTypeUnavailableReasonNodeDown
		return nil
	}

	node, err := oc.ds.GetNodeRO(orphan.Status.OwnerID)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get node %v", orphan.Spec.NodeID)
		}
		reason = longhorn.OrphanConditionTypeUnavailableReasonNodeNotFound
		return nil
	}

	if node.Spec.EvictionRequested {
		reason = longhorn.OrphanConditionTypeUnavailableReasonNodeEvicted
		return nil
	}

	id := orphan.Spec.Parameters[longhorn.OrphanDiskFsid]
	if disk, ok := node.Spec.Disks[id]; ok {
		if disk.EvictionRequested {
			reason = longhorn.OrphanConditionTypeUnavailableReasonDiskEvicted
		}
	} else {
		reason = longhorn.OrphanConditionTypeUnavailableReasonDiskNotFound
	}

	return nil
}
