package monitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	FakeNodeMonitorSyncPeriod = 30 * time.Second

	TestDiskID1 = "fsid"

	TestOrphanedReplicaDirectoryName = "test-volume-r-000000000"
)

type FakeNodeMonitor struct {
	*baseMonitor

	nodeName string

	collectedDataLock sync.RWMutex
	collectedData     *NodeMonitorCollectedData

	syncCallback func(key string)

	getDiskStatHandler GetDiskStatHandler
	getDiskConfig      GetDiskConfig
	generateDiskConfig GenerateDiskConfig
}

func NewFakeNodeMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, node *longhorn.Node, syncCallback func(key string)) (*FakeNodeMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &FakeNodeMonitor{
		baseMonitor: newBaseMonitor(ctx, quit, logger, ds, FakeNodeMonitorSyncPeriod),

		nodeName: node.Name,

		collectedDataLock: sync.RWMutex{},
		collectedData:     &NodeMonitorCollectedData{},

		syncCallback: syncCallback,

		getDiskStatHandler: fakeGetDiskStat,
		getDiskConfig:      fakeGetDiskConfig,
		generateDiskConfig: fakeGenerateDiskConfig,
	}

	return m, nil
}

func (m *FakeNodeMonitor) Start() {
	wait.PollImmediateUntil(m.syncPeriod, func() (done bool, err error) {
		if err := m.SyncCollectedData(); err != nil {
			m.logger.Errorf("Stop monitoring because of %v", err)
		}
		return false, nil
	}, m.ctx.Done())
}

func (m *FakeNodeMonitor) Close() {
	m.quit()
}

func (m *FakeNodeMonitor) GetCollectedData() (interface{}, error) {
	m.collectedDataLock.RLock()
	defer m.collectedDataLock.RUnlock()

	data := &NodeMonitorCollectedData{}
	if err := copier.Copy(data, m.collectedData); err != nil {
		return nil, errors.Wrapf(err, "failed to copy node monitor collected data")
	}

	return data, nil
}

func (m *FakeNodeMonitor) SyncCollectedData() error {
	node, err := m.ds.GetNode(m.nodeName)
	if err != nil {
		err = errors.Wrapf(err, "failed to get longhorn node %v", m.nodeName)
		return err
	}

	healthyDiskInfo, failedDiskInfo := m.collectDiskData(node)
	onDiksReplicaDirectoryNames := m.getOnDiskReplicaDirectoryNames(node)

	m.updateCollectedData(healthyDiskInfo, failedDiskInfo, onDiksReplicaDirectoryNames)
	key := node.Namespace + "/" + m.nodeName
	m.syncCallback(key)

	return nil
}

func (m *FakeNodeMonitor) collectDiskData(node *longhorn.Node) (map[string]map[string]*DiskInfo, map[string]*DiskInfo) {
	healthyDiskInfo := make(map[string]map[string]*DiskInfo, 0)
	failedDiskInfo := make(map[string]*DiskInfo, 0)

	for diskName, disk := range node.Spec.Disks {
		stat, err := m.getDiskStatHandler(disk.Path)
		if err != nil {
			failedDiskInfo[diskName] =
				newDiskInfoForFailedDisk(fmt.Sprintf("Disk %v(%v) on node %v is not ready: Get disk information error: %v",
					diskName, node.Spec.Disks[diskName].Path, node.Name, err))
			continue
		}

		if healthyDiskInfo[stat.Fsid] == nil {
			healthyDiskInfo[stat.Fsid] = make(map[string]*DiskInfo, 0)
		}

		diskConfig, err := m.getDiskConfig(disk.Path)
		if err != nil {
			if types.ErrorIsNotFound(err) {
				diskConfig, err = m.generateDiskConfig(node.Spec.Disks[diskName].Path)
				if err != nil {
					failedDiskInfo[diskName] =
						newDiskInfoForFailedDisk(fmt.Sprintf("Disk %v(%v) on node %v is not ready: failed to generate disk config: error: %v",
							diskName, disk.Path, node.Name, err))
					continue
				}
			} else {
				failedDiskInfo[diskName] =
					newDiskInfoForFailedDisk(fmt.Sprintf("Disk %v(%v) on node %v is not ready: failed to get disk config: error: %v",
						diskName, disk.Path, node.Name, err))
				continue
			}
		}

		healthyDiskInfo[stat.Fsid][diskName] = &DiskInfo{
			Path:     disk.Path,
			DiskStat: stat,
			DiskUUID: diskConfig.DiskUUID,
		}
	}

	return healthyDiskInfo, failedDiskInfo
}

func (m *FakeNodeMonitor) getOnDiskReplicaDirectoryNames(node *longhorn.Node) map[string]map[string]string {
	result := make(map[string]map[string]string, 0)

	// Add orphaned replica directory in each disk
	for id := range node.Spec.Disks {
		if result[id] == nil {
			result[id] = make(map[string]string)
		}
		result[id][TestOrphanedReplicaDirectoryName] = ""
	}

	return result
}

func fakeGetDiskStat(directory string) (*util.DiskStat, error) {
	return &util.DiskStat{
		Fsid:       "fsid",
		Path:       directory,
		Type:       "ext4",
		FreeBlock:  0,
		TotalBlock: 0,
		BlockSize:  0,

		StorageMaximum:   0,
		StorageAvailable: 0,
	}, nil
}

func fakeGetDiskConfig(path string) (*util.DiskConfig, error) {
	return &util.DiskConfig{
		DiskUUID: TestDiskID1,
	}, nil
}

func fakeGenerateDiskConfig(path string) (*util.DiskConfig, error) {
	return &util.DiskConfig{
		DiskUUID: TestDiskID1,
	}, nil
}

func (m *FakeNodeMonitor) updateCollectedData(healthyDiskInfo map[string]map[string]*DiskInfo, failedDiskInfo map[string]*DiskInfo, onDiskReplicaDirectoryNames map[string]map[string]string) {
	m.collectedDataLock.Lock()
	defer m.collectedDataLock.Unlock()

	m.collectedData.HealthyDiskInfo = healthyDiskInfo
	m.collectedData.FailedDiskInfo = failedDiskInfo
	m.collectedData.OnDiskReplicaDirectoryNames = onDiskReplicaDirectoryNames
}
