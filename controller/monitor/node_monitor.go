package monitor

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

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
	NodeMonitorSyncPeriod = 30 * time.Second
)

type NodeMonitor struct {
	logger logrus.FieldLogger

	eventRecorder record.EventRecorder
	ds            *datastore.DataStore

	node longhorn.Node
	lock sync.RWMutex

	onDiskReplicaDirectoryNames map[string]map[string]string

	syncCallback       func(key string)
	getDiskInfoHandler GetDiskInfoHandler
	getDiskConfig      GetDiskConfig
	generateDiskConfig GenerateDiskConfig

	ctx  context.Context
	quit context.CancelFunc
}

type GetDiskInfoHandler func(string) (*util.DiskInfo, error)
type GetDiskConfig func(string) (*util.DiskConfig, error)
type GenerateDiskConfig func(string) (*util.DiskConfig, error)

func NewNodeMonitor(logger logrus.FieldLogger, eventRecorder record.EventRecorder,
	ds *datastore.DataStore, node *longhorn.Node, syncCallback func(key string)) (*NodeMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &NodeMonitor{
		logger: logger,

		eventRecorder: eventRecorder,
		ds:            ds,

		node: *node,
		lock: sync.RWMutex{},

		onDiskReplicaDirectoryNames: make(map[string]map[string]string, 0),

		syncCallback: syncCallback,

		getDiskInfoHandler: util.GetDiskInfo,
		getDiskConfig:      util.GetDiskConfig,
		generateDiskConfig: util.GenerateDiskConfig,

		ctx:  ctx,
		quit: quit,
	}

	// Create a goroutine to monitor the node's disks status
	go m.monitorNode()

	return m, nil
}

type diskInfo struct {
	entry *util.DiskInfo
	err   error
}

func (m *NodeMonitor) monitorNode() {
	wait.PollImmediateUntil(NodeMonitorSyncPeriod, func() (done bool, err error) {
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

	m.syncDiskStatus(node)

	onDiskReplicaDirectoryNames := m.getOnDiskReplicaDirectoryNames(node)
	orphanedReplicaDirectoryNames := m.getOnDiskOrphanedReplicaDirectoryNames(node, onDiskReplicaDirectoryNames)

	m.onDiskReplicaDirectoryNames = onDiskReplicaDirectoryNames

	m.lock.Lock()
	defer m.lock.Unlock()
	for id, names := range orphanedReplicaDirectoryNames {
		node.Status.DiskStatus[id].OrphanedReplicaDirectoryNames = names
	}

	m.node = *node

	key := m.node.Namespace + "/" + m.node.Name
	m.syncCallback(key)

	return nil
}

func (m *NodeMonitor) Close() {
	m.quit()
}

func (m *NodeMonitor) syncDiskStatus(node *longhorn.Node) {
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

func (m *NodeMonitor) updateDiskStatusReadyCondition(node *longhorn.Node) {
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

func (m *NodeMonitor) getDiskInfoMap(node *longhorn.Node) map[string]*diskInfo {
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
func (m *NodeMonitor) isFSIDDuplicatedWithExistingReadyDisk(name string, disks []string, diskStatusMap map[string]*longhorn.DiskStatus) bool {
	if len(disks) > 1 {
		for _, otherName := range disks {
			diskReady :=
				types.GetCondition(
					diskStatusMap[otherName].Conditions,
					longhorn.DiskConditionTypeReady)

			if (otherName != name) && (diskReady.Status ==
				longhorn.ConditionStatusTrue) {
				return true
			}
		}
	}

	return false
}
