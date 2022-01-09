package scheduler

import (
	"fmt"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type CacheScheduler struct {
	ds *datastore.DataStore
}

type CacheDisk struct {
	longhorn.CacheDiskSpec
	*longhorn.CacheDiskStatus
	NodeID string
}

type CacheDiskSchedulingInfo struct {
	StorageAvailable           int64
	StorageMaximum             int64
	StorageReserved            int64
	StorageScheduled           int64
	MinimalAvailablePercentage int64
}

func NewCacheScheduler(ds *datastore.DataStore) *CacheScheduler {
	cacheScheduler := &CacheScheduler{
		ds: ds,
	}
	return cacheScheduler
}

func (cs *CacheScheduler) ScheduleCache(engine *longhorn.Engine, volume *longhorn.Volume) error {
	if volume.Spec.CacheSize == 0 {
		return nil
	}

	node, err := cs.ds.GetNode(engine.Spec.NodeID)
	if err != nil {
		return fmt.Errorf("unable to get node %v for engine", engine.Spec.NodeID)
	}

	var cacheDiskSpec longhorn.CacheDiskSpec
	var cacheDiskStatus *longhorn.CacheDiskStatus

	cacheDiskFound := false
	fsid := ""
	for fsid, cacheDiskStatus = range node.Status.CacheDiskStatus {
		if types.GetCondition(cacheDiskStatus.Conditions, longhorn.DiskConditionTypeSchedulable).Status == longhorn.ConditionStatusTrue {
			cacheDiskFound = true
			cacheDiskSpec = node.Spec.CacheDisks[fsid]
			break
		}
	}
	if !cacheDiskFound {
		return fmt.Errorf("cannot find the spec or the status for cache disk when scheduling cache")
	}

	info, err := cs.GetCacheDiskSchedulingInfo(cacheDiskSpec, cacheDiskStatus)
	if err != nil {
		return fmt.Errorf("unable to get settings when scheduling engine: %v", err)
	}

	if !cs.IsSchedulableToCacheDisk(volume.Spec.CacheSize, info) {
		return fmt.Errorf("unable to schedule disk to engine: %v", err)
	}

	for diskName, cacheDiskStatus := range node.Status.CacheDiskStatus {
		if types.GetCondition(cacheDiskStatus.Conditions, longhorn.DiskConditionTypeReady).Status == longhorn.ConditionStatusTrue {
			engine.Spec.CacheDiskPath = node.Spec.CacheDisks[diskName].Path
			break
		}
	}

	return nil
}

func (cs *CacheScheduler) GetCacheDiskSchedulingInfo(disk longhorn.CacheDiskSpec, diskStatus *longhorn.CacheDiskStatus) (*CacheDiskSchedulingInfo, error) {
	minimalAvailablePercentage, err := cs.ds.GetSettingAsInt(types.SettingNameStorageMinimalAvailablePercentage)
	if err != nil {
		return nil, err
	}
	info := &CacheDiskSchedulingInfo{
		StorageAvailable:           diskStatus.StorageAvailable,
		StorageScheduled:           diskStatus.StorageScheduled,
		StorageReserved:            disk.StorageReserved,
		StorageMaximum:             diskStatus.StorageMaximum,
		MinimalAvailablePercentage: minimalAvailablePercentage,
	}
	return info, nil
}

func (cs *CacheScheduler) IsSchedulableToCacheDisk(size int64, info *CacheDiskSchedulingInfo) bool {
	// StorageReserved = the space is already used by 3rd party + the space will be used by 3rd party.
	// StorageAvailable = the space can be used by 3rd party or Longhorn system.
	// There is no (direct) relationship between StorageReserved and StorageAvailable.
	return info.StorageMaximum > 0 && info.StorageAvailable > 0 &&
		info.StorageAvailable-size > int64(float64(info.StorageMaximum)*float64(info.MinimalAvailablePercentage)/100) &&
		(size+info.StorageScheduled) <= int64(info.StorageMaximum-info.StorageReserved)
}
