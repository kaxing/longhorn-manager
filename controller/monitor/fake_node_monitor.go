package monitor

import (
	"context"
	"fmt"
	"reflect"
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
		err = errors.Wrapf(err, "longhorn node %v has been deleted", m.nodeName)
		return err
	}

	pruneDiskStatus(node)
	diskStatusMap, diskStatMap := m.syncDiskStatus(node)
	onDiksReplicaDirectoryNames := m.getOnDiskReplicaDirectoryNames(node)

	if isDiskStatusChanged(m.collectedData.DiskStatusMap, diskStatusMap) ||
		!reflect.DeepEqual(m.collectedData.OnDiskReplicaDirectoryNames, onDiksReplicaDirectoryNames) {
		m.updateCollectedData(diskStatusMap, diskStatMap, onDiksReplicaDirectoryNames)
	}

	return nil
}

func (m *FakeNodeMonitor) updateCollectedData(diskStatusMap map[string]*longhorn.DiskStatus, diskStatMap map[string]*DiskStat, onDiskReplicaDirectoryNames map[string]map[string]string) {
	m.collectedDataLock.Lock()
	defer m.collectedDataLock.Unlock()

	m.collectedData.DiskStatusMap = diskStatusMap
	m.collectedData.DiskStatMap = diskStatMap
	m.collectedData.OnDiskReplicaDirectoryNames = onDiskReplicaDirectoryNames
}

func (m *FakeNodeMonitor) syncDiskStatus(node *longhorn.Node) (diskStatusMap map[string]*longhorn.DiskStatus, diskStatMap map[string]*DiskStat) {
	diskStatusMap = copyDiskStatus(node.Status.DiskStatus)
	diskStatMap = m.getDiskStatMap(node)

	fsid2Disks := map[string][]string{}
	for id, info := range diskStatMap {
		if info.err != nil {
			diskStatusMap[id].Conditions = types.SetCondition(diskStatusMap[id].Conditions,
				longhorn.DiskConditionTypeReady, longhorn.ConditionStatusFalse,
				string(longhorn.DiskConditionReasonNoDiskInfo),
				fmt.Sprintf("Disk %v(%v) on node %v is not ready: Get disk information error: %v",
					id, node.Spec.Disks[id].Path, node.Name, info.err))
		} else {
			if fsid2Disks[info.Entry.Fsid] == nil {
				fsid2Disks[info.Entry.Fsid] = []string{}
			}
			fsid2Disks[info.Entry.Fsid] = append(fsid2Disks[info.Entry.Fsid], id)
		}
	}

	for fsid, disks := range fsid2Disks {
		for _, id := range disks {
			diskStatus := diskStatusMap[id]
			disk := node.Spec.Disks[id]
			diskUUID := ""
			diskConfig, err := m.getDiskConfig(node.Spec.Disks[id].Path)
			if err != nil {
				if !types.ErrorIsNotFound(err) {
					diskStatusMap[id].Conditions = types.SetCondition(diskStatusMap[id].Conditions,
						longhorn.DiskConditionTypeReady,
						longhorn.ConditionStatusFalse,
						string(longhorn.DiskConditionReasonNoDiskInfo),
						fmt.Sprintf("Disk %v(%v) on node %v is not ready: failed to get disk config: error: %v",
							id, disk.Path, node.Name, err))
					continue
				}
			} else {
				diskUUID = diskConfig.DiskUUID
			}

			if diskStatusMap[id].DiskUUID == "" {
				// Check disks in the same filesystem
				if m.isFSIDDuplicatedWithExistingReadyDisk(
					id, disks, diskStatusMap) {
					// Found multiple disks in the same Fsid
					diskStatusMap[id].Conditions = types.SetCondition(diskStatusMap[id].Conditions,
						longhorn.DiskConditionTypeReady,
						longhorn.ConditionStatusFalse,
						string(longhorn.DiskConditionReasonDiskFilesystemChanged),
						fmt.Sprintf("Disk %v(%v) on node %v is not ready: disk has same file system ID %v as other disks %+v",
							id, disk.Path, node.Name, fsid, disks))
					continue
				}

				if diskUUID == "" {
					diskConfig, err := m.generateDiskConfig(node.Spec.Disks[id].Path)
					if err != nil {
						diskStatusMap[id].Conditions = types.SetCondition(diskStatusMap[id].Conditions,
							longhorn.DiskConditionTypeReady,
							longhorn.ConditionStatusFalse,
							string(longhorn.DiskConditionReasonNoDiskInfo),
							fmt.Sprintf("Disk %v(%v) on node %v is not ready: failed to generate disk config: error: %v",
								id, disk.Path, node.Name, err))
						continue
					}
					diskUUID = diskConfig.DiskUUID
				}
				diskStatus.DiskUUID = diskUUID
			} else { // diskStatusMap[id].DiskUUID != ""
				if diskUUID == "" {
					diskStatusMap[id].Conditions = types.SetCondition(diskStatusMap[id].Conditions,
						longhorn.DiskConditionTypeReady,
						longhorn.ConditionStatusFalse,
						string(longhorn.DiskConditionReasonDiskFilesystemChanged),
						fmt.Sprintf("Disk %v(%v) on node %v is not ready: cannot find disk config file, maybe due to a mount error",
							id, disk.Path, node.Name))
				} else if diskStatusMap[id].DiskUUID != diskUUID {
					diskStatusMap[id].Conditions = types.SetCondition(diskStatusMap[id].Conditions,
						longhorn.DiskConditionTypeReady,
						longhorn.ConditionStatusFalse,
						string(longhorn.DiskConditionReasonDiskFilesystemChanged),
						fmt.Sprintf("Disk %v(%v) on node %v is not ready: record diskUUID doesn't match the one on the disk ",
							id, disk.Path, node.Name))
				}
			}

			if diskStatus.DiskUUID == diskUUID {
				diskStatusMap[id].Conditions = types.SetCondition(diskStatusMap[id].Conditions,
					longhorn.DiskConditionTypeReady,
					longhorn.ConditionStatusTrue,
					"",
					fmt.Sprintf("Disk %v(%v) on node %v is ready", id, disk.Path, node.Name))
			}
			diskStatusMap[id] = diskStatus
		}
	}

	return diskStatusMap, diskStatMap
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

func (m *FakeNodeMonitor) getDiskStatMap(node *longhorn.Node) map[string]*DiskStat {
	result := map[string]*DiskStat{}

	for id, disk := range node.Spec.Disks {
		info, err := m.getDiskStatHandler(disk.Path)
		result[id] = &DiskStat{
			Entry: info,
			err:   err,
		}
	}
	return result
}

// Check all disks in the same filesystem ID are in ready status
func (m *FakeNodeMonitor) isFSIDDuplicatedWithExistingReadyDisk(name string, disks []string, diskStatusMap map[string]*longhorn.DiskStatus) bool {
	if len(disks) > 1 {
		for _, otherName := range disks {
			diskReady :=
				types.GetCondition(
					diskStatusMap[otherName].Conditions,
					longhorn.DiskConditionTypeReady)

			if (otherName != name) && (diskReady.Status == longhorn.ConditionStatusTrue) {
				return true
			}
		}
	}

	return false
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
