package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	NodeMonitorSyncPeriod = 30 * time.Second

	volumeMetaData = "volume.meta"
)

type NodeMonitor struct {
	*baseMonitor

	nodeName string

	collectedDataLock sync.RWMutex
	collectedData     *NodeMonitorCollectedData

	syncCallback func(key string)

	getDiskInfoHandler GetDiskInfoHandler
	getDiskConfig      GetDiskConfig
	generateDiskConfig GenerateDiskConfig
}

type NodeMonitorCollectedData struct {
	DiskStatusMap               map[string]*longhorn.DiskStatus
	DiskInfoMap                 map[string]*DiskInfo
	OnDiskReplicaDirectoryNames map[string]map[string]string
}

type VolumeMeta struct {
	Size            int64
	Head            string
	Dirty           bool
	Rebuilding      bool
	Error           string
	Parent          string
	SectorSize      int64
	BackingFilePath string
	BackingFile     interface{}
}

type GetDiskInfoHandler func(string) (*util.DiskInfo, error)
type GetDiskConfig func(string) (*util.DiskConfig, error)
type GenerateDiskConfig func(string) (*util.DiskConfig, error)

type DiskInfo struct {
	Entry *util.DiskInfo
	err   error
}

func NewNodeMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, node *longhorn.Node, syncCallback func(key string)) (*NodeMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &NodeMonitor{
		baseMonitor: newBaseMonitor(ctx, quit, logger, ds, NodeMonitorSyncPeriod),

		nodeName: node.Name,

		collectedDataLock: sync.RWMutex{},
		collectedData:     &NodeMonitorCollectedData{},

		syncCallback: syncCallback,

		getDiskInfoHandler: util.GetDiskInfo,
		getDiskConfig:      util.GetDiskConfig,
		generateDiskConfig: util.GenerateDiskConfig,
	}

	go m.Start()

	return m, nil
}

func (m *NodeMonitor) Start() {
	wait.PollImmediateUntil(m.syncPeriod, func() (done bool, err error) {
		if err := m.SyncCollectedData(); err != nil {
			m.logger.Errorf("failed to sync data because of %v", err)
		}
		return false, nil
	}, m.ctx.Done())
}

func (m *NodeMonitor) Close() {
	m.quit()
}

func (m *NodeMonitor) GetCollectedData() (interface{}, error) {
	m.collectedDataLock.RLock()
	defer m.collectedDataLock.RUnlock()

	data := &NodeMonitorCollectedData{}
	if err := copier.Copy(data, m.collectedData); err != nil {
		return nil, errors.Wrapf(err, "failed to copy node monitor collected data")
	}

	return data, nil
}

func (m *NodeMonitor) SyncCollectedData() error {
	node, err := m.ds.GetNode(m.nodeName)
	if err != nil {
		err = errors.Wrapf(err, "failed to get longhorn node %v", m.nodeName)
		return err
	}

	pruneDiskStatus(node)

	diskStatusMap, diskInfoMap := m.syncDiskStatus(node)
	onDiksReplicaDirectoryNames := m.getOnDiskReplicaDirectoryNames(node)

	if isDiskStatusChanged(m.collectedData.DiskStatusMap, diskStatusMap) ||
		!reflect.DeepEqual(m.collectedData.OnDiskReplicaDirectoryNames, onDiksReplicaDirectoryNames) {
		m.updateCollectedData(diskStatusMap, diskInfoMap, onDiksReplicaDirectoryNames)
		key := node.Namespace + "/" + m.nodeName
		m.syncCallback(key)
	}

	return nil
}

func (m *NodeMonitor) updateCollectedData(diskStatusMap map[string]*longhorn.DiskStatus, diskInfoMap map[string]*DiskInfo, onDiskReplicaDirectoryNames map[string]map[string]string) {
	m.collectedDataLock.Lock()
	defer m.collectedDataLock.Unlock()

	m.collectedData.DiskStatusMap = diskStatusMap
	m.collectedData.DiskInfoMap = diskInfoMap
	m.collectedData.OnDiskReplicaDirectoryNames = onDiskReplicaDirectoryNames
}

func (m *NodeMonitor) syncDiskStatus(node *longhorn.Node) (diskStatusMap map[string]*longhorn.DiskStatus, diskInfoMap map[string]*DiskInfo) {
	diskStatusMap = copyDiskStatus(node.Status.DiskStatus)
	diskInfoMap = m.getDiskInfoMap(node)

	fsid2Disks := map[string][]string{}
	for id, info := range diskInfoMap {
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

	return diskStatusMap, diskInfoMap
}

func (m *NodeMonitor) getOnDiskReplicaDirectoryNames(node *longhorn.Node) map[string]map[string]string {
	result := make(map[string]map[string]string, 0)

	for id, disk := range node.Spec.Disks {
		possibleNames, err := util.GetPossibleReplicaDirectoryNames(disk.Path)
		if err != nil {
			logrus.Errorf("unable to get possible replica directories in disk %v on node %v since %v", disk.Path, node.Name, err.Error())
			continue
		}

		prunedPossibleNames, err := m.prunePossibleReplicaDirectoryNames(node, disk.Path, possibleNames)
		if err != nil {
			logrus.Errorf("unable to prune possible replica directories in disk %v on node %v since %v", disk.Path, node.Name, err.Error())
			continue
		}

		result[id] = prunedPossibleNames
	}

	return result
}

func (m *NodeMonitor) prunePossibleReplicaDirectoryNames(node *longhorn.Node, diskPath string, replicaDirectoryNames map[string]string) (map[string]string, error) {
	// Find out the orphaned directories by checking with replica CRs
	replicas, err := m.ds.ListReplicasByNodeRO(node.Name)
	if err != nil {
		return nil, err
	}

	for _, replica := range replicas {
		delete(replicaDirectoryNames, replica.Spec.DataDirectoryName)
	}

	// Check volume metafile
	for name := range replicaDirectoryNames {
		if ok, err := isVolumeMetaFileExist(diskPath, name); err != nil || !ok {
			delete(replicaDirectoryNames, name)
		}
	}

	return replicaDirectoryNames, nil
}

func (m *NodeMonitor) getDiskInfoMap(node *longhorn.Node) map[string]*DiskInfo {
	result := map[string]*DiskInfo{}

	for id, disk := range node.Spec.Disks {
		info, err := m.getDiskInfoHandler(disk.Path)
		result[id] = &DiskInfo{
			Entry: info,
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

func isVolumeMetaFileExist(diskPath, replicaDirectoryName string) (bool, error) {
	var meta VolumeMeta

	path := filepath.Join(diskPath, "replicas", replicaDirectoryName, volumeMetaData)

	if err := unmarshalFile(path, &meta); err != nil {
		return false, err
	}

	return true, nil
}

func unmarshalFile(path string, obj interface{}) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	return dec.Decode(obj)
}

func pruneDiskStatus(node *longhorn.Node) {
	if node.Status.DiskStatus == nil {
		node.Status.DiskStatus = map[string]*longhorn.DiskStatus{}
	}

	// Align node.Spec.Disks and node.Status.DiskStatus.
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
		// When condition are not ready, the old storage data should be cleaned.
		diskStatus.StorageMaximum = 0
		diskStatus.StorageAvailable = 0
		node.Status.DiskStatus[id] = diskStatus
	}

	for id := range node.Status.DiskStatus {
		if _, exists := node.Spec.Disks[id]; !exists {
			delete(node.Status.DiskStatus, id)
		}
	}
}

func isDiskStatusChanged(oldDiskStatus, newDiskStatus map[string]*longhorn.DiskStatus) bool {
	if len(oldDiskStatus) != len(newDiskStatus) {
		return true
	}

	for id, oldStatus := range oldDiskStatus {
		newStatus, ok := newDiskStatus[id]
		if !ok {
			return true
		}
		oldReadyCondition := types.GetCondition(oldStatus.Conditions, longhorn.DiskConditionTypeReady)
		newReadyCondition := types.GetCondition(newStatus.Conditions, longhorn.DiskConditionTypeReady)
		if !reflect.DeepEqual(&oldReadyCondition, &newReadyCondition) {
			return true
		}
	}
	return false
}

func copyDiskStatus(diskStatus map[string]*longhorn.DiskStatus) map[string]*longhorn.DiskStatus {
	diskStatusCopy := make(map[string]*longhorn.DiskStatus, 0)

	for name, status := range diskStatus {
		statusCopy := &longhorn.DiskStatus{}
		status.DeepCopyInto(statusCopy)
		diskStatusCopy[name] = statusCopy
	}

	return diskStatusCopy
}
