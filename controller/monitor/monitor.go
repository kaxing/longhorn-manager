package monitor

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/client-go/tools/record"

	"github.com/longhorn/longhorn-manager/datastore"
)

type Monitor interface {
	Start()
	Close()

	SyncState() error
	GetState() interface{}
}

type baseMonitor struct {
	logger logrus.FieldLogger

	eventRecorder record.EventRecorder
	ds            *datastore.DataStore

	syncPeriod time.Duration

	ctx  context.Context
	quit context.CancelFunc
}

func newBaseMonitor(logger logrus.FieldLogger, eventRecorder record.EventRecorder, ds *datastore.DataStore, syncPeriod time.Duration, ctx context.Context, quit context.CancelFunc) *baseMonitor {
	m := &baseMonitor{
		logger: logger,

		eventRecorder: eventRecorder,
		ds:            ds,

		syncPeriod: syncPeriod,

		ctx:  ctx,
		quit: quit,
	}

	return m
}
