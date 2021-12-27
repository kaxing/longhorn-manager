package v1beta1

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	restclient "k8s.io/client-go/rest"

	"github.com/longhorn/longhorn-manager/types"

	longhornV1beta1 "github.com/longhorn/longhorn-manager/upgrade/v1beta1/k8s/pkg/apis/longhorn/v1beta1"

	lhclientsetV1beta1 "github.com/longhorn/longhorn-manager/upgrade/v1beta1/k8s/pkg/client/clientset/versioned"
)

func CanUpgrade(config *restclient.Config, namespace string) (bool, error) {
	lhClientV1beta1, err := lhclientsetV1beta1.NewForConfig(config)
	if err != nil {
		return false, errors.Wrap(err, "unable to get clientset for v1beta1")
	}

	scheme := runtime.NewScheme()
	if err := longhornV1beta1.SchemeBuilder.AddToScheme(scheme); err != nil {
		return false, errors.Wrap(err, "unable to create scheme for v1beta1")
	}

	_, err = lhClientV1beta1.LonghornV1beta1().Settings(namespace).Get(context.TODO(), string(types.SettingNameDefaultEngineImage), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("setting %v not found", string(types.SettingNameDefaultEngineImage))
			return true, nil
		}

		return false, errors.Wrap(err, fmt.Sprintf("unable to get setting %v", string(types.SettingNameDefaultEngineImage)))
	}

	// The CRD API version is v1alpha1 if SettingNameCRDAPIVersion is "" and SettingNameDefaultEngineImage is set.
	// Longhorn no longer supports the upgrade from v1alpha1 to v1beta2 directly.
	return false, errors.Wrapf(err, "unable to upgrade from v1alpha1 directly")
}
