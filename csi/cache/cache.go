package cache

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/longhorn/longhorn-manager/util"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

const (
	mapperFilePathPrefix = "/dev/mapper"
	mapperMetadataCache  = ".cache.metadata"
	mapperDataCache      = ".cache.data"
	mapperDevWithCache   = ".cached"
)

func VolumeMapper(volume string) string {
	return path.Join(mapperFilePathPrefix, volume+mapperDevWithCache)
}

func DeactivateCacheDevice(volume string) error {
	if err := deactivateCacheDevice(volume); err != nil {
		return err
	}
	if err := deactivateMetadataCacheDevice(volume); err != nil {
		return err
	}
	if err := deactivateDataCacheDevice(volume); err != nil {
		return err
	}
	return nil
}

func ActivateCacheDevice(volume, devPath string, cacheBlockSize int64) error {
	loopDevPath := getloopDevPath(volume)

	metadataCacheDevPath := metadataCacheVolumeMapper(volume)
	dataCacheDevPath := dataCacheVolumeMapper(volume)

	sectorSize, err := getSectorSize(loopDevPath)
	if err != nil {
		return err
	}
	logrus.Infof("loopback device sector size = %v", sectorSize)

	cacheDevSize, err := getDevSize(loopDevPath)
	if err != nil {
		return err
	}
	logrus.Infof("loopback device size = %v", cacheDevSize)

	if err := activateMetadataCacheDevice(loopDevPath, volume+mapperMetadataCache, cacheDevSize, cacheBlockSize, sectorSize); err != nil {
		return err
	}
	if err := activateDataCacheDevice(loopDevPath, volume+mapperDataCache, cacheDevSize, cacheBlockSize, sectorSize); err != nil {
		return err
	}
	if err := activateCacheDevice(volume, devPath, metadataCacheDevPath, dataCacheDevPath, sectorSize); err != nil {
		return err
	}

	return nil
}

func getloopDevPath(volume string) string {
	return path.Join(util.DeviceDirectory, "cache", volume)
}

func metadataCacheVolumeMapper(volume string) string {
	return path.Join(mapperFilePathPrefix, volume+mapperMetadataCache)
}

func dataCacheVolumeMapper(volume string) string {
	return path.Join(mapperFilePathPrefix, volume+mapperDataCache)
}

func activateMetadataCacheDevice(loopDevPath, metadataDev string, cacheDevSize, cacheBlockSize, sectorSize int64) error {
	metadataSectors := getMetadataDevSectors(cacheDevSize, cacheBlockSize, sectorSize)

	mapping := makeMetadataCacheMapping(loopDevPath, metadataSectors)
	if _, err := dmsetup("create", metadataDev, "--table", mapping); err != nil {
		return err
	}
	return nil
}

func deactivateMetadataCacheDevice(volume string) error {
	if _, err := dmsetup("remove", volume+mapperMetadataCache); err != nil {
		return err
	}
	return nil
}

func activateDataCacheDevice(loopDevPath, dataCacheDev string, cacheDevSize, cacheBlockSize, sectorSize int64) error {
	metadataSectors := getMetadataDevSectors(cacheDevSize, cacheBlockSize, sectorSize)
	dataSectors := getDataDevSectors(cacheDevSize, cacheBlockSize, sectorSize)

	mapping := makeDataCacheMapping(loopDevPath, dataSectors, metadataSectors)
	if _, err := dmsetup("create", dataCacheDev, "--table", mapping); err != nil {
		return err
	}
	return nil
}

func deactivateDataCacheDevice(volume string) error {
	if _, err := dmsetup("remove", volume+mapperDataCache); err != nil {
		return err
	}
	return nil
}

func activateCacheDevice(volume, devPath, metadataCacheDevPath, dataCacheDevPath string, sectorSize int64) error {
	devSectors, err := getDevSectors(devPath)
	if err != nil {
		return err
	}

	mapping := makeCachedDevMapping(devPath, metadataCacheDevPath, dataCacheDevPath, devSectors, sectorSize)
	if _, err := dmsetup("create", volume+mapperDevWithCache, "--table", mapping); err != nil {
		return err
	}
	return nil
}

func deactivateCacheDevice(volume string) error {
	if _, err := dmsetup("remove", volume+mapperDevWithCache); err != nil {
		return err
	}
	return nil
}

func makeMetadataCacheMapping(devPath string, metadataSectors int64) string {
	mapping := fmt.Sprintf("0 %v linear %v 0", metadataSectors, devPath)
	return strings.TrimSpace(mapping)
}

func makeDataCacheMapping(devPath string, dataSectors, metadataSectors int64) string {
	mapping := fmt.Sprintf("0 %v linear %v %v", dataSectors, devPath, metadataSectors)
	return strings.TrimSpace(mapping)
}

func makeCachedDevMapping(devPath, metadataCacheDevPath, dataDevCechePath string, devSectors, sectorSize int64) string {
	mapping := fmt.Sprintf("0 %v cache %v %v %v %v 1 writethrough default 0", devSectors, metadataCacheDevPath, dataDevCechePath, devPath, sectorSize)
	return strings.TrimSpace(mapping)
}

func getDevSize(devPath string) (int64, error) {
	out, err := util.Execute([]string{}, "blockdev", "--getsize64", devPath)
	if err != nil {
		return -1, err
	}

	trimmed := strings.TrimSpace(out)
	size, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return -1, err
	}

	return size, err
}

func getDevSectors(devPath string) (int64, error) {
	out, err := util.Execute([]string{}, "blockdev", "--getsz", devPath)
	if err != nil {
		return -1, err
	}

	trimmed := strings.TrimSpace(out)
	size, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return -1, err
	}

	return size, err
}

func getMetadataDevSectors(cacheDevSize, cacheBlockSize, sectorSize int64) int64 {
	logrus.Infof("Debug ===> cacheDevSize = %v, cacheBlockSize=%v", cacheDevSize, cacheBlockSize)
	metadataSize := 4194304 + (16 * cacheDevSize / cacheBlockSize)
	metadataSectors := metadataSize / sectorSize
	logrus.Infof("Debug ===> cacheDevSize = %v, cacheBlockSize=%v", cacheDevSize, cacheBlockSize)
	return metadataSectors
}

func getDataDevSectors(cacheDevSize, cacheBlockSize, sectorSize int64) int64 {
	metadataSectors := getMetadataDevSectors(cacheDevSize, cacheBlockSize, sectorSize)
	return (cacheDevSize / sectorSize) - metadataSectors
}

func getSectorSize(filePath string) (int64, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}

	if fi.Mode().IsRegular() {
		return 512, nil
	}

	if fi.Mode()&os.ModeDevice == 0 {
		return 0, errors.New("not a regular file or device")
	}

	sz, err := unix.IoctlGetInt(int(f.Fd()), unix.BLKSSZGET)
	if err != nil {
		return 0, err
	}
	return int64(sz), nil
}

func mknod(device string, major, minor int) error {
	var fileMode os.FileMode = 0660
	fileMode |= unix.S_IFBLK
	dev := int(unix.Mkdev(uint32(major), uint32(minor)))

	logrus.Infof("Creating device %s %d:%d", device, major, minor)
	return unix.Mknod(device, uint32(fileMode), dev)
}
