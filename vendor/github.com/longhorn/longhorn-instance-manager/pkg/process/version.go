package process

import (
	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"

	"github.com/longhorn/longhorn-instance-manager/pkg/meta"
	rpc "github.com/longhorn/longhorn-instance-manager/pkg/lhrpc"
)

func (pm *Manager) VersionGet(ctx context.Context, empty *empty.Empty) (*rpc.VersionResponse, error) {
	v := meta.GetVersion()
	return &rpc.VersionResponse{
		Version:   v.Version,
		GitCommit: v.GitCommit,
		BuildDate: v.BuildDate,

		InstanceManagerAPIVersion:    int64(v.InstanceManagerAPIVersion),
		InstanceManagerAPIMinVersion: int64(v.InstanceManagerAPIMinVersion),
	}, nil
}
