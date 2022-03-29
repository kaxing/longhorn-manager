package monitor

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	NodeMonitorSyncPeriod = 30 * time.Second
)

type NodeMonitor struct {
	logger logrus.FieldLogger

	ds *datastore.DataStore

	node longhorn.Node
	lock sync.RWMutex

	onDiskReplicaDirectoryNames map[string]map[string]string

	syncCallback func(key string)

	ctx  context.Context
	quit context.CancelFunc
}

func NewNodeMonitor(logger logrus.FieldLogger, ds *datastore.DataStore,
	node *longhorn.Node, syncCallback func(key string)) (*NodeMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &NodeMonitor{
		logger: logger,

		ds: ds,

		node: *node,
		lock: sync.RWMutex{},

		onDiskReplicaDirectoryNames: make(map[string]map[string]string, 0),

		syncCallback: syncCallback,

		ctx:  ctx,
		quit: quit,
	}

	// Create a goroutine to monitor the node's disks status
	go m.monitorNode()

	return m, nil
}

func (m *NodeMonitor) monitorNode() {
	wait.PollUntil(NodeMonitorSyncPeriod, func() (done bool, err error) {
		if err := m.syncNode(); err != nil {
			m.logger.Errorf("Stop monitoring because of %v", err)
			m.Close()
		}
		return false, nil
	}, m.ctx.Done())
}

func (m *NodeMonitor) syncNode() error {
	node, err := m.ds.GetNode(m.node.Name)
	if err != nil {
		err = errors.Wrapf(err, "longhorn node %v has been deleted", m.node.Name)
		return err
	}

	onDiskReplicaDirectoryNames := m.getOnDiskReplicaDirectoryNames(node)
	orphanedReplicaDirectoryNames := m.getOnDiskOrphanedReplicaDirectoryNames(node, onDiskReplicaDirectoryNames)

	m.onDiskReplicaDirectoryNames = onDiskReplicaDirectoryNames

	m.lock.Lock()
	defer m.lock.Unlock()
	for id, names := range orphanedReplicaDirectoryNames {
		node.Status.DiskStatus[id].OrphanedReplicaDirectoryNames = names
	}

	m.node = *node

	return nil
}

func (m *NodeMonitor) Close() {
	m.quit()
}

func (m *NodeMonitor) getOnDiskReplicaDirectoryNames(node *longhorn.Node) map[string]map[string]string {
	result := make(map[string]map[string]string, 0)

	for id, disk := range node.Spec.Disks {
		replicaDirectoryNames, err := util.GetReplicaDirectoryNames(disk.Path)
		if err != nil {
			logrus.Warnf("Unable to get replica directories in disk %v on node %v since %v", disk.Path, node.Name, err.Error())
		}

		result[id] = replicaDirectoryNames
	}

	return result
}

func (m *NodeMonitor) getOnDiskOrphanedReplicaDirectoryNames(node *longhorn.Node, onDiskReplicaDirectoryNames map[string]map[string]string) map[string]map[string]string {
	orphanedReplicaDirectoryNames := make(map[string]map[string]string, 0)

	for diskID, names := range onDiskReplicaDirectoryNames {
		if reflect.DeepEqual(names, m.onDiskReplicaDirectoryNames[diskID]) {
			logrus.Infof("Replica directories in disk %v on node %v is not changed", diskID, node.Name)
			if _, exist := node.Status.DiskStatus[diskID]; exist {
				orphanedReplicaDirectoryNames[diskID] = copyMap(node.Status.DiskStatus[diskID].OrphanedReplicaDirectoryNames)
			}
		} else {
			logrus.Infof("Replica directories in disk %v on node %v is changed", diskID, node.Name)
			orphanedReplicaDirectoryNames[diskID] = copyMap(names)
			for replicaName := range node.Status.DiskStatus[diskID].ScheduledReplica {
				replica, err := m.ds.GetReplica(replicaName)
				if err != nil {
					continue
				}
				delete(orphanedReplicaDirectoryNames[diskID], replica.Spec.DataDirectoryName)
			}
		}
	}

	return orphanedReplicaDirectoryNames
}

func (m *NodeMonitor) GetNode() *longhorn.Node {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.node.DeepCopy()
}

func copyMap(m map[string]string) map[string]string {
	n := make(map[string]string)
	for k, v := range m {
		n[k] = v
	}
	return n
}
