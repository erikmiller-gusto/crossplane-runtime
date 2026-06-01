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
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane/crossplane-runtime/v2/pkg/xcrd"
)

// A stand-in for a provider-specific ProviderConfig API type. Providers pass
// their own such types into ShardOptions.ExemptObjects.
type fakeProviderConfig struct{ corev1.ConfigMap }

func TestShardCacheOptions(t *testing.T) {
	type want struct {
		// selector is the expected DefaultLabelSelector, by String(). The empty
		// string means "no selector" (a nil selector).
		selector string
		// exemptKinds is the set of types we expect to be exempted from the
		// shard selector (i.e. present in ByObject with Label=Everything()).
		exemptKinds []client.Object
		// byObjectNil asserts ByObject is nil (the sharding-off path).
		byObjectNil bool
	}

	pc := &fakeProviderConfig{}

	cases := map[string]struct {
		reason string
		opts   ShardOptions
		want   want
	}{
		"ShardingDisabled": {
			reason: "With sharding disabled, options must be byte-for-byte today's behavior: no selector and no ByObject.",
			opts:   ShardOptions{Enabled: false, ShardID: "shard-a"},
			want: want{
				selector:    "",
				byObjectNil: true,
			},
		},
		"ShardInstance": {
			reason: "A shard instance must select only its own shard's resources.",
			opts:   ShardOptions{Enabled: true, ShardID: "shard-a", ExemptObjects: []client.Object{pc}},
			want: want{
				selector:    "crossplane.io/shard=shard-a",
				exemptKinds: []client.Object{&corev1.Secret{}, &corev1.ConfigMap{}, pc},
			},
		},
		"DefaultInstance": {
			reason: "The default instance (sharding on, empty shard ID) must select only unlabeled resources.",
			opts:   ShardOptions{Enabled: true, ShardID: "", ExemptObjects: []client.Object{pc}},
			want: want{
				selector:    "!crossplane.io/shard",
				exemptKinds: []client.Object{&corev1.Secret{}, &corev1.ConfigMap{}, pc},
			},
		},
		"NoExemptObjects": {
			reason: "Core/secret/configmap types are exempted even when the provider passes no types of its own.",
			opts:   ShardOptions{Enabled: true, ShardID: "shard-b"},
			want: want{
				selector:    "crossplane.io/shard=shard-b",
				exemptKinds: []client.Object{&corev1.Secret{}, &corev1.ConfigMap{}},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := ShardCacheOptions(tc.opts)

			// DefaultLabelSelector: compare by String(), since labels.Selector
			// is an interface with unexported fields that go-cmp cannot diff.
			gotSelector := ""
			if got.DefaultLabelSelector != nil {
				gotSelector = got.DefaultLabelSelector.String()
			}
			if diff := cmp.Diff(tc.want.selector, gotSelector); diff != "" {
				t.Errorf("\n%s\nShardCacheOptions(...) DefaultLabelSelector: -want, +got:\n%s", tc.reason, diff)
			}

			if tc.want.byObjectNil {
				if got.ByObject != nil {
					t.Errorf("\n%s\nShardCacheOptions(...): expected nil ByObject when sharding disabled, got %d entries", tc.reason, len(got.ByObject))
				}
				return
			}

			// Every exempt type must be present and exempted with
			// labels.Everything() (String() == ""). Asserting the Label value,
			// not just key presence, catches a silently-wrong exemption.
			//
			// ByObject is keyed by client.Object (interface). The helper builds
			// the core-type keys itself, so we cannot look up by our own
			// pointer (interface map lookup is pointer identity for pointers);
			// match on the key's reflect.Type instead.
			byType := make(map[reflect.Type]cache.ByObject, len(got.ByObject))
			for k, v := range got.ByObject {
				byType[reflect.TypeOf(k)] = v
			}
			if len(got.ByObject) != len(tc.want.exemptKinds) {
				t.Errorf("\n%s\nShardCacheOptions(...): want %d ByObject entries, got %d", tc.reason, len(tc.want.exemptKinds), len(got.ByObject))
			}
			for _, k := range tc.want.exemptKinds {
				bo, ok := byType[reflect.TypeOf(k)]
				if !ok {
					t.Errorf("\n%s\nShardCacheOptions(...): missing ByObject exemption for %T", tc.reason, k)
					continue
				}
				if bo.Label == nil || bo.Label.String() != labels.Everything().String() {
					t.Errorf("\n%s\nShardCacheOptions(...): exemption for %T must be labels.Everything(), got %v", tc.reason, k, bo.Label)
				}
			}
		})
	}
}

// Guard against the shard-label key drifting away from core's value: the wire
// string is the cross-repo contract.
func TestShardLabelKeyContract(t *testing.T) {
	if xcrd.LabelKeyShard != "crossplane.io/shard" {
		t.Errorf("LabelKeyShard must stay identical to core's value; got %q", xcrd.LabelKeyShard)
	}
}

// Compile-time assurance the helper returns the controller-runtime type
// providers feed into a manager.
var _ = func() cache.Options { return ShardCacheOptions(ShardOptions{}) }
