package controller

import (
	"fmt"
	"reflect"
	"strings"
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
)

/*
var (
	// maxRetriesOnAcquireLockError should guarantee the cumulative retry time
	// is larger than 150 seconds.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the times
	// a deployment is going to be requeued:
	//
	// 5ms, 10ms, 20ms, ... , 81.92s, 163.84s
	maxRetriesOnAcquireLockError = 16
)
*/
type HousekeepingController struct {
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

func NewHousekeepingController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) *HousekeepingController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	hc := &HousekeepingController{
		baseController: newBaseController("longhorn-housekeeping", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-housekeeping-controller"}),
	}

	ds.HousekeepingInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    hc.enqueueHousekeeping,
		UpdateFunc: func(old, cur interface{}) { hc.enqueueHousekeeping(cur) },
		DeleteFunc: hc.enqueueHousekeeping,
	})

	hc.cacheSyncs = append(hc.cacheSyncs, ds.HousekeepingInformer.HasSynced)

	return hc
}

func (hc *HousekeepingController) enqueueHousekeeping(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	hc.queue.Add(key)
}

func (hc *HousekeepingController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer hc.queue.ShutDown()

	hc.logger.Infof("Start Longhorn Housekeeping controller")
	defer hc.logger.Infof("Shutting down Longhorn Housekeeping controller")

	if !cache.WaitForNamedCacheSync(hc.name, stopCh, hc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(hc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (hc *HousekeepingController) worker() {
	for hc.processNextWorkItem() {
	}
}

func (hc *HousekeepingController) processNextWorkItem() bool {
	key, quit := hc.queue.Get()
	if quit {
		return false
	}
	defer hc.queue.Done(key)
	err := hc.syncHousekeeping(key.(string))
	hc.handleErr(err, key)
	return true
}

func (hc *HousekeepingController) handleErr(err error, key interface{}) {
	if err == nil {
		hc.queue.Forget(key)
		return
	}

	// The resync period of the backup is one hour and the maxRetries is 3.
	// Thus, the deletion failure of the backup in error state is caused by the shutdown of the replica during backing up,
	// if the lock hold by the backup job is not released.
	// The workaround is to increase the maxRetries number and to retry the deletion until the lock acquisition
	// of the housekeeping is timeout after 150 seconds.
	if strings.Contains(err.Error(), "failed lock") {
		if hc.queue.NumRequeues(key) < maxRetriesOnAcquireLockError {
			hc.logger.WithError(err).Warnf("Error syncing Longhorn housekeeping %v because of the failure of lock acquisition", key)
			hc.queue.AddRateLimited(key)
			return
		}
	} else {
		if hc.queue.NumRequeues(key) < maxRetries {
			hc.logger.WithError(err).Warnf("Error syncing Longhorn housekeeping %v", key)
			hc.queue.AddRateLimited(key)
			return
		}
	}

	utilruntime.HandleError(err)
	hc.logger.WithError(err).Warnf("Dropping Longhorn housekeeping %v out of the queue", key)
	hc.queue.Forget(key)
}

func (hc *HousekeepingController) syncHousekeeping(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: fail to sync housekeeping %v", hc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != hc.namespace {
		return nil
	}
	return hc.reconcile(name)
}

func getLoggerForHousekeeping(logger logrus.FieldLogger, housekeeping *longhorn.Housekeeping) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"housekeeping": housekeeping.Name,
		},
	)
}

func (hc *HousekeepingController) reconcile(housekeepingName string) (err error) {
	logrus.Infof("Debug ===> reconcile %v", housekeepingName)
	housekeeping, err := hc.ds.GetHousekeeping(housekeepingName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	// Check the responsible node
	if !hc.isResponsibleFor(housekeeping) {
		return nil
	}

	if housekeeping.Status.OwnerID != hc.controllerID {
		housekeeping.Status.OwnerID = hc.controllerID
		housekeeping, err = hc.ds.UpdateHousekeepingStatus(housekeeping)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
	}

	log := getLoggerForHousekeeping(hc.logger, housekeeping)

	// Examine DeletionTimestamp to determine if object is under deletion
	if !housekeeping.DeletionTimestamp.IsZero() {
		return hc.ds.RemoveFinalizerForHousekeeping(housekeeping)
	}

	//syncTime := metav1.Time{Time: time.Now().UTC()}
	existingHousekeeping := housekeeping.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingHousekeeping.Status, housekeeping.Status) {
			return
		}
		if _, err := hc.ds.UpdateHousekeepingStatus(housekeeping); err != nil && apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", housekeepingName)
			hc.enqueueHousekeeping(housekeeping)
		}
	}()

	if housekeeping.Status.LastSyncedAt.IsZero() {
	}

	// The housekeeping config had synced
	if !housekeeping.Status.LastSyncedAt.IsZero() &&
		!housekeeping.Spec.SyncRequestedAt.After(housekeeping.Status.LastSyncedAt.Time) {
		return nil
	}

	// TODO: Update Hosekeeping CR status
	logrus.Infof("Debug ===> finish")
	return nil
}

func (hc *HousekeepingController) isResponsibleFor(housekeeping *longhorn.Housekeeping) bool {
	return isControllerResponsibleFor(hc.controllerID, hc.ds, housekeeping.Name, "", housekeeping.Status.OwnerID)
}
