package util

import (
	"encoding/json"
	"fmt"
)

const (
	longhornFinalizerKey = "longhorn.io"
)

func Contains(elems []string, v string) bool {
	for _, s := range elems {
		if v == s {
			return true
		}
	}
	return false
}

func AddForegroundDeletionFinalizer(finalizers []string) (string, error) {
	if Contains(finalizers, "foregroundDeletion") {
		return "", nil
	}

	finalizers = append(finalizers, "foregroundDeletion")

	bytes, err := json.Marshal(finalizers)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`{"op": "replace", "path": "/metadata/finalizers", "value": %v}`, string(bytes)), nil
}

func AddLonghornFinalizer(finalizers []string) (string, error) {
	if Contains(finalizers, longhornFinalizerKey) {
		return "", nil
	}

	finalizers = append(finalizers, longhornFinalizerKey)

	bytes, err := json.Marshal(finalizers)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`{"op": "replace", "path": "/metadata/finalizers", "value": %v}`, string(bytes)), nil
}
