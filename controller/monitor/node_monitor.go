package monitor

import (
	"context"
	"fmt"
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

	getDiskStatHandler GetDiskStatHandler
	getDiskConfig      GetDiskConfig
	generateDiskConfig GenerateDiskConfig
}

type NodeMonitorCollectedData struct {
	HealthyDiskInfo             map[string]map[string]*DiskInfo
	FailedDiskInfo              map[string]*DiskInfo
	OnDiskReplicaDirectoryNames map[string]map[string]string
}

type DiskInfo struct {
	Path                        string
	DiskStat                    *util.DiskStat
	DiskUUID                    string
	Condition                   *longhorn.Condition
	OnDiskReplicaDirectoryNames map[string]string
}

type GetDiskStatHandler func(string) (*util.DiskStat, error)
type GetDiskConfig func(string) (*util.DiskConfig, error)
type GenerateDiskConfig func(string) (*util.DiskConfig, error)

func NewNodeMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, node *longhorn.Node, syncCallback func(key string)) (*NodeMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &NodeMonitor{
		baseMonitor: newBaseMonitor(ctx, quit, logger, ds, NodeMonitorSyncPeriod),

		nodeName: node.Name,

		collectedDataLock: sync.RWMutex{},
		collectedData: &NodeMonitorCollectedData{
			HealthyDiskInfo:             make(map[string]map[string]*DiskInfo, 0),
			FailedDiskInfo:              make(map[string]*DiskInfo, 0),
			OnDiskReplicaDirectoryNames: make(map[string]map[string]string, 0),
		},

		syncCallback: syncCallback,

		getDiskStatHandler: util.GetDiskStat,
		getDiskConfig:      util.GetDiskConfig,
		generateDiskConfig: util.GenerateDiskConfig,
	}

	go m.Start()

	return m, nil
}

func (m *NodeMonitor) Start() {
	wait.PollImmediateUntil(m.syncPeriod, func() (done bool, err error) {
		if err := m.SyncCollectedData(); err != nil {
			m.logger.Errorf("Stop monitoring because of %v", err)
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

	healthyDiskInfo, failedDiskInfo := m.collectDiskData(node)
	onDiskReplicaDirectoryNames := m.collectOnDiskReplicaDirectoryNames(node)

	if !reflect.DeepEqual(m.collectedData.HealthyDiskInfo, healthyDiskInfo) ||
		!reflect.DeepEqual(m.collectedData.FailedDiskInfo, failedDiskInfo) ||
		!reflect.DeepEqual(m.collectedData.OnDiskReplicaDirectoryNames, onDiskReplicaDirectoryNames) {
		m.updateCollectedData(healthyDiskInfo, failedDiskInfo, onDiskReplicaDirectoryNames)
		key := node.Namespace + "/" + m.nodeName
		m.syncCallback(key)
	}

	return nil
}

// Collect Disk data. The disks are divided into two groups, the healthy and failed disks.
// A failed disk means it encounters stat, getDiskConfig or generateDiskConfig failure.
// healthyDiskInfo layout is map[fsid][diskName]*DiskInfo.
// failedDiskInfo layout is map[diskName]*DiskInfo.
func (m *NodeMonitor) collectDiskData(node *longhorn.Node) (map[string]map[string]*DiskInfo, map[string]*DiskInfo) {
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

func (m *NodeMonitor) collectOnDiskReplicaDirectoryNames(node *longhorn.Node) map[string]map[string]string {
	result := make(map[string]map[string]string, 0)

	for diskName, disk := range node.Spec.Disks {
		/*
			if node.Status.DiskStatus == nil || node.Status.DiskStatus[diskName] == nil {
				continue
			}

			readyCondition := types.GetCondition(node.Status.DiskStatus[diskName].Conditions, longhorn.DiskConditionTypeReady)
			if readyCondition.Status != longhorn.ConditionStatusTrue {
				continue
			}
		*/
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

		result[diskName] = prunedPossibleNames
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
		if err := isVolumeMetaFileExist(diskPath, name); err != nil {
			delete(replicaDirectoryNames, name)
		}
	}

	return replicaDirectoryNames, nil
}

func (m *NodeMonitor) updateCollectedData(healthyDiskInfo map[string]map[string]*DiskInfo, failedDiskInfo map[string]*DiskInfo, onDiskReplicaDirectoryNames map[string]map[string]string) {
	m.collectedDataLock.Lock()
	defer m.collectedDataLock.Unlock()

	m.collectedData.HealthyDiskInfo = healthyDiskInfo
	m.collectedData.FailedDiskInfo = failedDiskInfo
	m.collectedData.OnDiskReplicaDirectoryNames = onDiskReplicaDirectoryNames
}

func isVolumeMetaFileExist(diskPath, replicaDirectoryName string) error {
	path := filepath.Join(diskPath, "replicas", replicaDirectoryName, volumeMetaData)
	_, err := util.GetVolumeMeta(path)
	return err
}

func newDiskInfoForFailedDisk(message string) *DiskInfo {
	return &DiskInfo{
		Condition: &longhorn.Condition{
			Type:    longhorn.DiskConditionTypeReady,
			Status:  longhorn.ConditionStatusFalse,
			Reason:  string(longhorn.DiskConditionReasonNoDiskInfo),
			Message: message,
		},
	}
}
