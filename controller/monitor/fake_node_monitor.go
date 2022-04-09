package monitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"

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

	stateLock sync.RWMutex
	state     *NodeMonitorState

	syncCallback func(key string)

	getDiskInfoHandler GetDiskInfoHandler
	getDiskConfig      GetDiskConfig
	generateDiskConfig GenerateDiskConfig
}

func NewFakeNodeMonitor(logger logrus.FieldLogger, eventRecorder record.EventRecorder,
	ds *datastore.DataStore, node *longhorn.Node, syncCallback func(key string)) (*FakeNodeMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &FakeNodeMonitor{
		baseMonitor: newBaseMonitor(ctx, quit, logger, eventRecorder, ds, FakeNodeMonitorSyncPeriod),

		stateLock: sync.RWMutex{},
		state: &NodeMonitorState{
			Node:                        node.DeepCopy(),
			OnDiskReplicaDirectoryNames: make(map[string]map[string]*util.Entry, 0),
		},

		syncCallback: syncCallback,

		getDiskInfoHandler: fakeGetDiskInfo,
		getDiskConfig:      fakeGetDiskConfig,
		generateDiskConfig: fakeGenerateDiskConfig,
	}

	return m, nil
}

func (m *FakeNodeMonitor) Start() {
	wait.PollImmediateUntil(m.syncPeriod, func() (done bool, err error) {
		if err := m.SyncState(); err != nil {
			m.logger.Errorf("Stop monitoring because of %v", err)
		}
		return false, nil
	}, m.ctx.Done())
}

func (m *FakeNodeMonitor) Close() {
	m.quit()
}

func (m *FakeNodeMonitor) GetState() (interface{}, error) {
	m.stateLock.RLock()
	defer m.stateLock.RUnlock()

	state := &NodeMonitorState{}
	if err := copier.Copy(state, m.state); err != nil {
		return nil, errors.Wrapf(err, "failed to copy node monitor state")
	}

	return state, nil
}

func (m *FakeNodeMonitor) SyncState() error {
	node, err := m.ds.GetNode(m.state.Node.Name)
	if err != nil {
		err = errors.Wrapf(err, "longhorn node %v has been deleted", m.state.Node.Name)
		return err
	}

	m.syncDiskStatus(node)

	onDiksDiskReplicaDirectoryNames, err := m.getOnDiskReplicaDirectoryNames(node)
	if err != nil {
		return err
	}

	m.updateState(node, onDiksDiskReplicaDirectoryNames)

	return nil
}

func (m *FakeNodeMonitor) updateState(node *longhorn.Node, onDiskReplicaDirectoryNames map[string]map[string]*util.Entry) {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()

	m.state.Node = node.DeepCopy()
	m.state.OnDiskReplicaDirectoryNames = onDiskReplicaDirectoryNames
}

func (m *FakeNodeMonitor) syncDiskStatus(node *longhorn.Node) {
	// Sync the disks between node.Spec.Disks and node.Status.DiskStatus.
	if node.Status.DiskStatus == nil {
		node.Status.DiskStatus = map[string]*longhorn.DiskStatus{}
	}

	// Aligh node.Spec.Disks and node.Status.DiskStatus.
	for id := range node.Spec.Disks {
		if node.Status.DiskStatus[id] == nil {
			node.Status.DiskStatus[id] = &longhorn.DiskStatus{}
		}
		diskStatus := node.Status.DiskStatus[id]
		if diskStatus.Conditions == nil {
			diskStatus.Conditions = []longhorn.Condition{}
		}
		if diskStatus.ScheduledReplica == nil {
			diskStatus.ScheduledReplica = map[string]int64{}
		}
		// when condition are not ready, the old storage data should be cleaned
		diskStatus.StorageMaximum = 0
		diskStatus.StorageAvailable = 0
		node.Status.DiskStatus[id] = diskStatus
	}

	for id := range node.Status.DiskStatus {
		if _, exists := node.Spec.Disks[id]; !exists {
			delete(node.Status.DiskStatus, id)
		}
	}

	m.updateDiskStatusReadyCondition(node)
}

func (m *FakeNodeMonitor) updateDiskStatusReadyCondition(node *longhorn.Node) {
	diskStatusMap := node.Status.DiskStatus
	diskInfoMap := m.getDiskInfoMap(node)

	fsid2Disks := map[string][]string{}
	for id, info := range diskInfoMap {
		if info.err != nil {
			diskStatusMap[id].Conditions = types.SetConditionAndRecord(diskStatusMap[id].Conditions,
				longhorn.DiskConditionTypeReady, longhorn.ConditionStatusFalse,
				string(longhorn.DiskConditionReasonNoDiskInfo),
				fmt.Sprintf("Disk %v(%v) on node %v is not ready: Get disk information error: %v",
					id, node.Spec.Disks[id].Path, node.Name, info.err),
				m.eventRecorder, node, v1.EventTypeWarning)
		} else {
			if fsid2Disks[info.entry.Fsid] == nil {
				fsid2Disks[info.entry.Fsid] = []string{}
			}
			fsid2Disks[info.entry.Fsid] = append(fsid2Disks[info.entry.Fsid], id)
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
					diskStatusMap[id].Conditions = types.SetConditionAndRecord(diskStatusMap[id].Conditions,
						longhorn.DiskConditionTypeReady, longhorn.ConditionStatusFalse,
						string(longhorn.DiskConditionReasonNoDiskInfo),
						fmt.Sprintf("Disk %v(%v) on node %v is not ready: failed to get disk config: error: %v",
							id, disk.Path, node.Name, err),
						m.eventRecorder, node, v1.EventTypeWarning)
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
					diskStatusMap[id].Conditions =
						types.SetConditionAndRecord(
							diskStatusMap[id].Conditions,
							longhorn.DiskConditionTypeReady,
							longhorn.ConditionStatusFalse,
							string(longhorn.DiskConditionReasonDiskFilesystemChanged),
							fmt.Sprintf("Disk %v(%v) on node %v is not ready: disk has same file system ID %v as other disks %+v",
								id, disk.Path, node.Name, fsid, disks),
							m.eventRecorder, node,
							v1.EventTypeWarning)
					continue
				}

				if diskUUID == "" {
					diskConfig, err := m.generateDiskConfig(node.Spec.Disks[id].Path)
					if err != nil {
						diskStatusMap[id].Conditions = types.SetConditionAndRecord(diskStatusMap[id].Conditions,
							longhorn.DiskConditionTypeReady, longhorn.ConditionStatusFalse,
							string(longhorn.DiskConditionReasonNoDiskInfo),
							fmt.Sprintf("Disk %v(%v) on node %v is not ready: failed to generate disk config: error: %v",
								id, disk.Path, node.Name, err),
							m.eventRecorder, node, v1.EventTypeWarning)
						continue
					}
					diskUUID = diskConfig.DiskUUID
				}
				diskStatus.DiskUUID = diskUUID
			} else { // diskStatusMap[id].DiskUUID != ""
				if diskUUID == "" {
					diskStatusMap[id].Conditions = types.SetConditionAndRecord(diskStatusMap[id].Conditions,
						longhorn.DiskConditionTypeReady, longhorn.ConditionStatusFalse,
						string(longhorn.DiskConditionReasonDiskFilesystemChanged),
						fmt.Sprintf("Disk %v(%v) on node %v is not ready: cannot find disk config file, maybe due to a mount error",
							id, disk.Path, node.Name),
						m.eventRecorder, node, v1.EventTypeWarning)
				} else if diskStatusMap[id].DiskUUID != diskUUID {
					diskStatusMap[id].Conditions = types.SetConditionAndRecord(diskStatusMap[id].Conditions,
						longhorn.DiskConditionTypeReady, longhorn.ConditionStatusFalse,
						string(longhorn.DiskConditionReasonDiskFilesystemChanged),
						fmt.Sprintf("Disk %v(%v) on node %v is not ready: record diskUUID doesn't match the one on the disk ",
							id, disk.Path, node.Name),
						m.eventRecorder, node, v1.EventTypeWarning)
				}
			}

			if diskStatus.DiskUUID == diskUUID {
				// on the default disks this will be updated constantly since there is always something generating new disk usage (logs, etc)
				// We also don't need byte/block precisions for this instead we can round down to the next 10/100mb
				const truncateTo = 100 * 1024 * 1024
				usableStorage := (diskInfoMap[id].entry.StorageAvailable / truncateTo) * truncateTo
				diskStatus.StorageAvailable = usableStorage
				diskStatus.StorageMaximum = diskInfoMap[id].entry.StorageMaximum
				diskStatusMap[id].Conditions = types.SetConditionAndRecord(diskStatusMap[id].Conditions,
					longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue,
					"", fmt.Sprintf("Disk %v(%v) on node %v is ready", id, disk.Path, node.Name),
					m.eventRecorder, node, v1.EventTypeNormal)
			}
			diskStatusMap[id] = diskStatus
		}
	}
}

func (m *FakeNodeMonitor) getOnDiskReplicaDirectoryNames(node *longhorn.Node) (map[string]map[string]*util.Entry, error) {
	result := make(map[string]map[string]*util.Entry, 0)

	// Add active and orphaned replica directories
	replicas, err := m.ds.ListReplicasByNodeRO(node.Name)
	if err != nil {
		return nil, err
	}
	for _, replica := range replicas {
		if result[replica.Spec.DiskID] == nil {
			result[replica.Spec.DiskID] = make(map[string]*util.Entry)
		}

		entry := &util.Entry{
			Parameters: map[string]string{
				util.EntryChangedTime: "0",
			},
		}
		result[replica.Spec.DiskID][replica.Spec.DataDirectoryName] = entry
	}

	// Add orphaned replica directory in each disk
	for id := range node.Spec.Disks {
		if result[id] == nil {
			result[id] = make(map[string]*util.Entry)
		}
		entry := &util.Entry{
			Parameters: map[string]string{
				util.EntryChangedTime: "0",
			},
		}
		result[id][TestOrphanedReplicaDirectoryName] = entry
	}

	return result, nil
}

func (m *FakeNodeMonitor) getDiskInfoMap(node *longhorn.Node) map[string]*diskInfo {
	result := map[string]*diskInfo{}

	for id, disk := range node.Spec.Disks {
		info, err := m.getDiskInfoHandler(disk.Path)
		result[id] = &diskInfo{
			entry: info,
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

func fakeGetDiskInfo(directory string) (*util.DiskInfo, error) {
	return &util.DiskInfo{
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
