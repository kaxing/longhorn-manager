package cache

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"

	iscsi_util "github.com/longhorn/go-iscsi-helper/util"
)

func losetup(args ...string) (stdout string, err error) {
	// NOTE: losetup needs to be run in the host IPC/MNT
	// if you only use MNT the binary will not return but still do the appropriate action.
	ns := iscsi_util.GetHostNamespacePath(hostProcPath)
	nsArgs := prepareCommandArgs(ns, "losetup", args)
	ctx, cancel := context.WithTimeout(context.TODO(), dmsetupTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "nsenter", nsArgs...)

	var stdoutBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf

	output := stdoutBuf.String()
	if err := cmd.Run(); err != nil {
		return output, fmt.Errorf("failed to run losetup args: %v output: %v error: %v", args, output, err)
	}

	return stdoutBuf.String(), nil
}
