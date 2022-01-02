package cache

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"time"

	iscsi_util "github.com/longhorn/go-iscsi-helper/util"
)

const (
	hostProcPath   = "/proc" // we use hostPID for the csi plugin
	dmsetupTimeout = time.Minute
)

// dmsetup runs dmsetup via nsenter inside of the host namespaces
// The util dmsetup returns 0 on success and a non-zero value on error.
// 1 wrong parameters, 2 no permission (bad passphrase),
// 3 out of memory, 4 wrong device specified,
// 5 device already exists or device is busy.
func dmsetup(args ...string) (stdout string, err error) {
	// NOTE: dmsetup needs to be run in the host IPC/MNT
	// if you only use MNT the binary will not return but still do the appropriate action.
	ns := iscsi_util.GetHostNamespacePath(hostProcPath)
	nsArgs := prepareCommandArgs(ns, "dmsetup", args)
	ctx, cancel := context.WithTimeout(context.TODO(), dmsetupTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "nsenter", nsArgs...)

	var stdoutBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf

	output := stdoutBuf.String()
	if err := cmd.Run(); err != nil {
		return output, fmt.Errorf("failed to run dmsetup args: %v output: %v error: %v", args, output, err)
	}

	return stdoutBuf.String(), nil
}

func prepareCommandArgs(ns, cmd string, args []string) []string {
	cmdArgs := []string{
		"--mount=" + filepath.Join(ns, "mnt"),
		"--ipc=" + filepath.Join(ns, "ipc"),
		cmd,
	}
	return append(cmdArgs, args...)
}
