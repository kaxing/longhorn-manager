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

	if oc.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn orphan %v: %v", key, err)
		oc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn orphan %v out of the queue: %v", key, err)
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

func getLoggerForOrphan(logger logrus.FieldLogger, orphan *longhorn.Orphan) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"orphan": orphan.Name,
		},
	)
}

func (oc *OrphanController) reconcile(orphanName string) (err error) {
	orphan, err := oc.ds.GetOrphan(orphanName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	if !oc.isResponsibleFor(orphan) {
		return nil
	}

	log := getLoggerForOrphan(oc.logger, orphan)

	if !orphan.DeletionTimestamp.IsZero() {
		err := oc.deleteOrphanedData(orphan)
		if err != nil && !apierrors.IsNotFound(err) {
			orphan.Status.Conditions = types.SetCondition(orphan.Status.Conditions,
				longhorn.OrphanConditionTypeDeletable, longhorn.ConditionStatusFalse,
				"", err.Error())
			log.WithError(err).Errorf("error deleting orphan %v data", orphan.Name)
			return err
		}
		return oc.ds.RemoveFinalizerForOrphan(orphan)
	}

	existingOrphan := orphan.DeepCopy()

	orphan.Status.Conditions = types.SetCondition(orphan.Status.Conditions,
		longhorn.OrphanConditionTypeDeletable, longhorn.ConditionStatusTrue,
		"", "")
	if reflect.DeepEqual(existingOrphan.Status, orphan.Status) {
		return nil
	}
	if _, err := oc.ds.UpdateOrphanStatus(orphan); err != nil && apierrors.IsConflict(errors.Cause(err)) {
		log.WithError(err).Debugf("Requeue %v due to conflict", orphanName)
		oc.enqueueOrphan(orphan)
	}

	return nil
}

func (oc *OrphanController) isResponsibleFor(orphan *longhorn.Orphan) bool {
	return isControllerResponsibleFor(oc.controllerID, oc.ds, orphan.Name, "", orphan.Spec.NodeID)
}

func (oc *OrphanController) deleteOrphanedData(orphan *longhorn.Orphan) error {
	return errors.New("TODO: delete on-disk orphaned data")
}
