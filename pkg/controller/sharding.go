/*
Copyright 2024 The Crossplane Authors.

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

package controller

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"

	"github.com/crossplane/crossplane-runtime/v2/pkg/xcrd"
)

// ShardOptions configure shard-aware filtering of a provider's reconciliation
// cache. Providers that opt into sharding build these and pass the resulting
// cache.Options to their controller-runtime manager.
//
// Sharding partitions managed resources across multiple provider instances
// using the xcrd.LabelKeyShard ("crossplane.io/shard") label. Each instance's
// informer cache is filtered to a single shard so that exactly one instance
// reconciles any given resource. See the Provider Sharding design for details.
type ShardOptions struct {
	// Enabled reports whether sharding is configured for this provider at all.
	// When false, ShardCacheOptions returns a zero-value cache.Options that
	// preserves today's unfiltered behavior byte-for-byte.
	Enabled bool

	// ShardID is the shard this provider instance serves. An empty ShardID with
	// Enabled true denotes the *default* instance, which serves resources that
	// carry no shard label (selector "!crossplane.io/shard").
	ShardID string

	// ExemptObjects are object types that must NOT be filtered by the shard
	// selector, even though Enabled is true. The shard selector is overridden
	// with labels.Everything() for each, so every instance caches all of them.
	//
	// Core, secret, and configmap types are exempted automatically (see
	// ShardCacheOptions). Providers pass their own ProviderConfig,
	// ProviderConfigUsage, and StoreConfig API types here, since the runtime
	// library does not know a provider's specific API types.
	ExemptObjects []client.Object
}

// ShardCacheOptions returns the controller-runtime cache.Options that implement
// the shard provider's reconciliation watches for the given ShardOptions.
//
// The returned options behave as follows:
//
//   - Sharding disabled (o.Enabled false): a zero-value cache.Options with no
//     DefaultLabelSelector and no ByObject overrides. This is identical to a
//     provider that never heard of sharding — today's behavior, unchanged.
//   - Shard instance (o.Enabled true, o.ShardID != ""): DefaultLabelSelector of
//     "crossplane.io/shard=<ShardID>". Only matching resources are cached and
//     trigger reconciles.
//   - Default instance (o.Enabled true, o.ShardID == ""): DefaultLabelSelector
//     of "!crossplane.io/shard" (label does not exist). The default instance
//     serves only unlabeled resources, so each resource is reconciled by
//     exactly one instance and there is no dual reconciliation (design Option A).
//
// When sharding is enabled, the following types are exempted from the shard
// selector via ByObject (their Label is set to labels.Everything()), so every
// instance can see them regardless of shard: corev1.Secret, corev1.ConfigMap,
// and every object in o.ExemptObjects (the provider's ProviderConfig,
// ProviderConfigUsage, and StoreConfig types).
//
// The shard selector applies only to the reconciliation cache. Cross-shard
// reference resolution must read through an unfiltered client; see
// NewUncachedClient.
func ShardCacheOptions(o ShardOptions) cache.Options {
	// Sharding off: no selector and no ByObject. Populating ByObject here would
	// diverge from today's behavior even though there is no default selector to
	// override, so we deliberately return the zero value.
	if !o.Enabled {
		return cache.Options{}
	}

	var selector labels.Selector
	if o.ShardID == "" {
		// Default instance: resources with no shard label. Built explicitly
		// rather than via labels.Parse so a malformed label key can never make
		// this panic or return an error at provider startup.
		req, _ := labels.NewRequirement(xcrd.LabelKeyShard, selection.DoesNotExist, nil)
		selector = labels.NewSelector().Add(*req)
	} else {
		selector = labels.SelectorFromSet(labels.Set{xcrd.LabelKeyShard: o.ShardID})
	}

	// Exempt non-sharded types from the selector. Core types are referenced
	// directly; provider-specific types come from the caller.
	exempt := append([]client.Object{
		&corev1.Secret{},
		&corev1.ConfigMap{},
	}, o.ExemptObjects...)

	byObject := make(map[client.Object]cache.ByObject, len(exempt))
	for _, obj := range exempt {
		byObject[obj] = cache.ByObject{Label: labels.Everything()}
	}

	return cache.Options{
		DefaultLabelSelector: selector,
		ByObject:             byObject,
	}
}

// NewUncachedClient returns a client.Client that reads directly from the API
// server, bypassing the manager's (shard-filtered) cache.
//
// Sharded providers wire this into the managed reconciler's reference resolver
// via managed.WithReferenceResolver(managed.NewAPISimpleReferenceResolver(c)),
// so that a resource in shard A can resolve a reference to a resource in shard
// B even though shard B's resources are absent from shard A's filtered cache.
//
// This is the design's recommended approach (1) for cross-shard reference
// resolution: an uncached client is simple and correct. Reference resolution is
// infrequent relative to reconciliation, so the per-lookup API server round
// trip is acceptable.
//
// Callers should reserve this for the reference resolver, not for general
// reconciliation: it does not benefit from informer caching and is not shard
// filtered. Non-sharded providers do not need it and should keep using the
// manager's cached client, leaving their reconcile behavior unchanged.
func NewUncachedClient(c cluster.Cluster) (client.Client, error) {
	// Omitting Options.Cache makes client.New read straight from the API
	// server. Scheme and Mapper are taken from the cluster so the uncached
	// client understands the same types as the manager.
	return client.New(c.GetConfig(), client.Options{
		Scheme: c.GetScheme(),
		Mapper: c.GetRESTMapper(),
	})
}
