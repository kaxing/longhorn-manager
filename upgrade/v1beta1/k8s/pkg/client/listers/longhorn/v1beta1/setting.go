/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by lister-gen. DO NOT EDIT.

package v1beta1

import (
	v1beta1 "github.com/longhorn/longhorn-manager/upgrade/v1beta1/k8s/pkg/apis/longhorn/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// SettingLister helps list Settings.
type SettingLister interface {
	// List lists all Settings in the indexer.
	List(selector labels.Selector) (ret []*v1beta1.Setting, err error)
	// Settings returns an object that can list and get Settings.
	Settings(namespace string) SettingNamespaceLister
	SettingListerExpansion
}

// settingLister implements the SettingLister interface.
type settingLister struct {
	indexer cache.Indexer
}

// NewSettingLister returns a new SettingLister.
func NewSettingLister(indexer cache.Indexer) SettingLister {
	return &settingLister{indexer: indexer}
}

// List lists all Settings in the indexer.
func (s *settingLister) List(selector labels.Selector) (ret []*v1beta1.Setting, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1beta1.Setting))
	})
	return ret, err
}

// Settings returns an object that can list and get Settings.
func (s *settingLister) Settings(namespace string) SettingNamespaceLister {
	return settingNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// SettingNamespaceLister helps list and get Settings.
type SettingNamespaceLister interface {
	// List lists all Settings in the indexer for a given namespace.
	List(selector labels.Selector) (ret []*v1beta1.Setting, err error)
	// Get retrieves the Setting from the indexer for a given namespace and name.
	Get(name string) (*v1beta1.Setting, error)
	SettingNamespaceListerExpansion
}

// settingNamespaceLister implements the SettingNamespaceLister
// interface.
type settingNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all Settings in the indexer for a given namespace.
func (s settingNamespaceLister) List(selector labels.Selector) (ret []*v1beta1.Setting, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1beta1.Setting))
	})
	return ret, err
}

// Get retrieves the Setting from the indexer for a given namespace and name.
func (s settingNamespaceLister) Get(name string) (*v1beta1.Setting, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1beta1.Resource("setting"), name)
	}
	return obj.(*v1beta1.Setting), nil
}
