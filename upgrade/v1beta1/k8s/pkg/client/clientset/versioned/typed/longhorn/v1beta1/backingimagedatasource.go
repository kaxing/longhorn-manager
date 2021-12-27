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

// Code generated by client-gen. DO NOT EDIT.

package v1beta1

import (
	"context"
	"time"

	v1beta1 "github.com/longhorn/longhorn-manager/upgrade/v1beta1/k8s/pkg/apis/longhorn/v1beta1"
	scheme "github.com/longhorn/longhorn-manager/upgrade/v1beta1/k8s/pkg/client/clientset/versioned/scheme"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// BackingImageDataSourcesGetter has a method to return a BackingImageDataSourceInterface.
// A group's client should implement this interface.
type BackingImageDataSourcesGetter interface {
	BackingImageDataSources(namespace string) BackingImageDataSourceInterface
}

// BackingImageDataSourceInterface has methods to work with BackingImageDataSource resources.
type BackingImageDataSourceInterface interface {
	Create(ctx context.Context, backingImageDataSource *v1beta1.BackingImageDataSource, opts v1.CreateOptions) (*v1beta1.BackingImageDataSource, error)
	Update(ctx context.Context, backingImageDataSource *v1beta1.BackingImageDataSource, opts v1.UpdateOptions) (*v1beta1.BackingImageDataSource, error)
	UpdateStatus(ctx context.Context, backingImageDataSource *v1beta1.BackingImageDataSource, opts v1.UpdateOptions) (*v1beta1.BackingImageDataSource, error)
	Delete(ctx context.Context, name string, opts v1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error
	Get(ctx context.Context, name string, opts v1.GetOptions) (*v1beta1.BackingImageDataSource, error)
	List(ctx context.Context, opts v1.ListOptions) (*v1beta1.BackingImageDataSourceList, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1beta1.BackingImageDataSource, err error)
	BackingImageDataSourceExpansion
}

// backingImageDataSources implements BackingImageDataSourceInterface
type backingImageDataSources struct {
	client rest.Interface
	ns     string
}

// newBackingImageDataSources returns a BackingImageDataSources
func newBackingImageDataSources(c *LonghornV1beta1Client, namespace string) *backingImageDataSources {
	return &backingImageDataSources{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the backingImageDataSource, and returns the corresponding backingImageDataSource object, and an error if there is any.
func (c *backingImageDataSources) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1beta1.BackingImageDataSource, err error) {
	result = &v1beta1.BackingImageDataSource{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("backingimagedatasources").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of BackingImageDataSources that match those selectors.
func (c *backingImageDataSources) List(ctx context.Context, opts v1.ListOptions) (result *v1beta1.BackingImageDataSourceList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1beta1.BackingImageDataSourceList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("backingimagedatasources").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested backingImageDataSources.
func (c *backingImageDataSources) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("backingimagedatasources").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a backingImageDataSource and creates it.  Returns the server's representation of the backingImageDataSource, and an error, if there is any.
func (c *backingImageDataSources) Create(ctx context.Context, backingImageDataSource *v1beta1.BackingImageDataSource, opts v1.CreateOptions) (result *v1beta1.BackingImageDataSource, err error) {
	result = &v1beta1.BackingImageDataSource{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("backingimagedatasources").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(backingImageDataSource).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a backingImageDataSource and updates it. Returns the server's representation of the backingImageDataSource, and an error, if there is any.
func (c *backingImageDataSources) Update(ctx context.Context, backingImageDataSource *v1beta1.BackingImageDataSource, opts v1.UpdateOptions) (result *v1beta1.BackingImageDataSource, err error) {
	result = &v1beta1.BackingImageDataSource{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("backingimagedatasources").
		Name(backingImageDataSource.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(backingImageDataSource).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *backingImageDataSources) UpdateStatus(ctx context.Context, backingImageDataSource *v1beta1.BackingImageDataSource, opts v1.UpdateOptions) (result *v1beta1.BackingImageDataSource, err error) {
	result = &v1beta1.BackingImageDataSource{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("backingimagedatasources").
		Name(backingImageDataSource.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(backingImageDataSource).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the backingImageDataSource and deletes it. Returns an error if one occurs.
func (c *backingImageDataSources) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("backingimagedatasources").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *backingImageDataSources) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("backingimagedatasources").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched backingImageDataSource.
func (c *backingImageDataSources) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1beta1.BackingImageDataSource, err error) {
	result = &v1beta1.BackingImageDataSource{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("backingimagedatasources").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}
