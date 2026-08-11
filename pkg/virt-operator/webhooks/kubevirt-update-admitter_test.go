/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors.
 *
 */

package webhooks

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"

	v1 "kubevirt.io/api/core/v1"

	"kubevirt.io/kubevirt/pkg/pointer"
	"kubevirt.io/kubevirt/pkg/testutils"
	"kubevirt.io/kubevirt/pkg/virt-config/featuregate"
)

var _ = Describe("Validating KubeVirtUpdate Admitter", func() {
	test := field.NewPath("test")
	vmProfileField := test.Child("virtualMachineInstanceProfile")

	DescribeTable("validateVirtTemplateDeployment", func(kvSpec v1.KubeVirtSpec, expectError bool) {
		causes := validateVirtTemplateDeployment(&kvSpec.Configuration)
		if expectError {
			Expect(causes).To(HaveLen(1))
			Expect(causes[0].Type).To(Equal(metav1.CauseTypeFieldValueInvalid))
			Expect(causes[0].Field).To(Equal("spec.configuration.virtTemplateDeployment.enabled"))
		} else {
			Expect(causes).To(BeEmpty())
		}
	},
		Entry("should reject when VirtTemplateDeployment enabled without Template feature gate",
			v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						DisabledFeatureGates: []string{featuregate.Template},
					},
					VirtTemplateDeployment: &v1.VirtTemplateDeployment{
						Enabled: pointer.P(true),
					},
				},
			},
			true,
		),
		Entry("should allow when VirtTemplateDeployment enabled with Template feature gate",
			v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.Template},
					},
					VirtTemplateDeployment: &v1.VirtTemplateDeployment{
						Enabled: pointer.P(true),
					},
				},
			},
			false,
		),
		Entry("should allow when VirtTemplateDeployment is nil",
			v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{},
			},
			false,
		),
		Entry("should allow when VirtTemplateDeployment.Enabled is nil",
			v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					VirtTemplateDeployment: &v1.VirtTemplateDeployment{
						Enabled: nil,
					},
				},
			},
			false,
		),
		Entry("should allow when VirtTemplateDeployment.Enabled is false",
			v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					VirtTemplateDeployment: &v1.VirtTemplateDeployment{
						Enabled: pointer.P(false),
					},
				},
			},
			false,
		),
	)

	DescribeTable("validateRoleAggregationStrategy", func(kvSpec v1.KubeVirtSpec, expectError bool) {
		causes := validateRoleAggregationStrategy(&kvSpec.Configuration)
		if expectError {
			Expect(causes).To(HaveLen(1))
			Expect(causes[0].Type).To(Equal(metav1.CauseTypeFieldValueInvalid))
			Expect(causes[0].Field).To(Equal("spec.configuration.roleAggregationStrategy"))
		} else {
			Expect(causes).To(BeEmpty())
		}
	},
		Entry("should reject when RoleAggregationStrategy is Manual with OptOutRoleAggregation feature gate disabled",
			v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						DisabledFeatureGates: []string{featuregate.OptOutRoleAggregation},
					},
					RoleAggregationStrategy: pointer.P(v1.RoleAggregationStrategyManual),
				},
			},
			true,
		),
		Entry("should allow when RoleAggregationStrategy is Manual with OptOutRoleAggregation enabled by default (Beta)",
			v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					RoleAggregationStrategy: pointer.P(v1.RoleAggregationStrategyManual),
				},
			},
			false,
		),
		Entry("should allow when RoleAggregationStrategy is nil",
			v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{},
			},
			false,
		),
		Entry("should allow when RoleAggregationStrategy is AggregateToDefault without feature gate",
			v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					RoleAggregationStrategy: pointer.P(v1.RoleAggregationStrategyAggregateToDefault),
				},
			},
			false,
		),
		Entry("should allow when RoleAggregationStrategy is AggregateToDefault and OptOutRoleAggregation is disabled",
			v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						DisabledFeatureGates: []string{featuregate.OptOutRoleAggregation},
					},
					RoleAggregationStrategy: pointer.P(v1.RoleAggregationStrategyAggregateToDefault),
				},
			},
			false,
		),
	)

	DescribeTable("validateMigrationConfiguration", func(oldConfig, newConfig *v1.KubeVirtConfiguration, expectError bool) {
		causes := validateMigrationConfiguration(oldConfig, newConfig)
		if expectError {
			Expect(causes).To(HaveLen(1))
			Expect(causes[0].Type).To(Equal(metav1.CauseTypeFieldValueInvalid))
			Expect(causes[0].Field).To(Equal("spec.configuration.migrationConfiguration.maxDowntimeMs"))
		} else {
			Expect(causes).To(BeEmpty())
		}
	},
		Entry("should reject when MaxDowntimeMs is newly set without feature gate",
			&v1.KubeVirtConfiguration{},
			&v1.KubeVirtConfiguration{
				MigrationConfiguration: &v1.MigrationConfiguration{MaxDowntimeMs: pointer.P(uint64(900))},
			},
			true,
		),
		Entry("should allow when MaxDowntimeMs is set with MigrationStallDetection gate",
			&v1.KubeVirtConfiguration{},
			&v1.KubeVirtConfiguration{
				MigrationConfiguration: &v1.MigrationConfiguration{MaxDowntimeMs: pointer.P(uint64(900))},
				DeveloperConfiguration: &v1.DeveloperConfiguration{
					FeatureGates: []string{featuregate.MigrationStallDetection},
				},
			},
			false,
		),
		Entry("should allow when MaxDowntimeMs is set with MigrationDowntimeTuning gate",
			&v1.KubeVirtConfiguration{},
			&v1.KubeVirtConfiguration{
				MigrationConfiguration: &v1.MigrationConfiguration{MaxDowntimeMs: pointer.P(uint64(900))},
				DeveloperConfiguration: &v1.DeveloperConfiguration{
					FeatureGates: []string{featuregate.MigrationDowntimeTuning},
				},
			},
			false,
		),
		Entry("should allow unrelated update when MaxDowntimeMs is unchanged and feature gate is disabled",
			&v1.KubeVirtConfiguration{
				MigrationConfiguration: &v1.MigrationConfiguration{MaxDowntimeMs: pointer.P(uint64(900))},
			},
			&v1.KubeVirtConfiguration{
				MigrationConfiguration: &v1.MigrationConfiguration{MaxDowntimeMs: pointer.P(uint64(900))},
			},
			false,
		),
		Entry("should reject changing MaxDowntimeMs when feature gate is disabled",
			&v1.KubeVirtConfiguration{
				MigrationConfiguration: &v1.MigrationConfiguration{MaxDowntimeMs: pointer.P(uint64(500))},
			},
			&v1.KubeVirtConfiguration{
				MigrationConfiguration: &v1.MigrationConfiguration{MaxDowntimeMs: pointer.P(uint64(900))},
			},
			true,
		),
	)

	DescribeTable("validateSeccompConfiguration", func(seccompConfiguration *v1.SeccompConfiguration, expectedFields []string) {
		causes := validateSeccompConfiguration(test, seccompConfiguration)
		Expect(causes).To(HaveLen(len(expectedFields)))
		for _, cause := range causes {
			Expect(cause.Field).To(BeElementOf(expectedFields))
		}
	},
		Entry("don't specifying custom ", &v1.SeccompConfiguration{
			VirtualMachineInstanceProfile: &v1.VirtualMachineInstanceProfile{
				CustomProfile: nil,
			},
		}, []string{vmProfileField.Child("customProfile").String()}),

		Entry("having custom local and runtimeDefault Profile", &v1.SeccompConfiguration{
			VirtualMachineInstanceProfile: &v1.VirtualMachineInstanceProfile{
				CustomProfile: &v1.CustomProfile{
					RuntimeDefaultProfile: true,
					LocalhostProfile:      pointer.P("somethingNotImportant"),
				},
			},
		}, []string{vmProfileField.Child("customProfile", "runtimeDefaultProfile").String(), vmProfileField.Child("customProfile", "localhostProfile").String()}),
	)

	DescribeTable("test validateCustomizeComponents", func(cc v1.CustomizeComponents, expectedCauses int) {
		causes := validateCustomizeComponents(cc)
		Expect(causes).To(HaveLen(expectedCauses))
	},
		Entry("invalid values rejected", v1.CustomizeComponents{
			Patches: []v1.CustomizeComponentsPatch{
				{
					ResourceName: "virt-api",
					ResourceType: "Deployment",
					Type:         v1.StrategicMergePatchType,
					Patch:        `{"json: "not valid"}`,
				},
			},
		}, 1),
		Entry("empty patch field rejected", v1.CustomizeComponents{
			Patches: []v1.CustomizeComponentsPatch{
				{
					ResourceName: "virt-api",
					ResourceType: "Deployment",
					Type:         v1.StrategicMergePatchType,
					Patch:        "",
				},
			},
		}, 1),
		Entry("valid values accepted", v1.CustomizeComponents{
			Patches: []v1.CustomizeComponentsPatch{
				{
					ResourceName: "virt-api",
					ResourceType: "Deployment",
					Type:         v1.StrategicMergePatchType,
					Patch:        `{}`,
				},
			},
		}, 0),
	)

	DescribeTable("test validateHandlerPools", func(kv *v1.KubeVirt, expectedCauses []metav1.StatusCause) {
		causes := validateHandlerPools(kv)
		Expect(causes).To(Equal(expectedCauses))
	},
		Entry("empty pools", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: nil,
			},
		}, nil),
		Entry("pools not enabled", &v1.KubeVirt{Spec: v1.KubeVirtSpec{}}, nil),
		Entry("partition keys with no pools", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{"some-key-1"},
					Pools:         nil,
				},
			},
		}, nil),
		Entry("malformed partition key", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{"bad label for a node"},
				},
			},
		}, []metav1.StatusCause{
			{
				Type:    "FieldValueInvalid",
				Message: "partitionKeys should be valid qualified names: bad label for a node",
				Field:   "spec.handlerPools.partitionKeys",
			},
		}),
		Entry("too many partition keys", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{
						"some-key-1",
						"some-key-2",
						"some-key-3",
						"some-key-4",
						"some-key-5",
						"some-key-6",
						"some-key-7",
						"some-key-8",
						"some-key-9",
						"some-key-10",
						"some-key-11",
						"some-key-12",
						"some-key-13",
						"some-key-14",
						"some-key-15",
						"some-key-16",
						"some-key-17",
					},
					Pools: []v1.HandlerPoolConfig{
						{
							Name:         "pool-1",
							NodeSelector: nil,
						},
					},
				},
			},
		}, []metav1.StatusCause{
			{
				Type:    metav1.CauseTypeFieldValueRequired,
				Message: "partitionKeys length should be between 1 and 16, but it is 17",
				Field:   "spec.handlerPools.partitionKeys",
			},
		}),
		Entry("not enough partition keys", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{},
					Pools: []v1.HandlerPoolConfig{
						{
							Name:         "pool-1",
							NodeSelector: nil,
						},
					},
				},
			},
		}, []metav1.StatusCause{
			{
				Type:    metav1.CauseTypeFieldValueRequired,
				Message: "partitionKeys length should be between 1 and 16, but it is 0",
				Field:   "spec.handlerPools.partitionKeys",
			},
		}),
		Entry("partition keys with duplicates", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{
						"some-key-1",
						"some-key-2",
						"some-key-1",
						"some-key-4",
						"some-key-1",
					},
					Pools: []v1.HandlerPoolConfig{
						{
							Name:         "pool-1",
							NodeSelector: nil,
						},
					},
				},
			},
		}, []metav1.StatusCause{
			{
				Type:    metav1.CauseTypeFieldValueDuplicate,
				Message: "partitionKeys should be unique, but there are two duplicate keys: some-key-1",
				Field:   "spec.handlerPools.partitionKeys",
			},
			{
				Type:    metav1.CauseTypeFieldValueDuplicate,
				Message: "partitionKeys should be unique, but there are two duplicate keys: some-key-1",
				Field:   "spec.handlerPools.partitionKeys",
			},
		}),
		Entry("a pool with no selectors", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{
						"some-key-1",
						"some-key-2",
					},
					Pools: []v1.HandlerPoolConfig{
						{
							Name:         "pool-1",
							NodeSelector: nil,
						},
					},
				},
			},
		}, []metav1.StatusCause{
			{
				Type:    "FieldValueNotFound",
				Message: "pool pool-1 doesn't specify all partition keys [some-key-1 some-key-2]",
				Field:   "spec.handlerPools.pools.nodeSelector",
			},
		}),
		Entry("pools with non-unique names", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{
						"some-key-1",
					},
					Pools: []v1.HandlerPoolConfig{
						{
							Name: "pool-1",
							NodeSelector: map[string]string{
								"some-key-1": "1",
							},
						},
						{
							Name: "pool-2",
							NodeSelector: map[string]string{
								"some-key-1": "2",
							},
						},
						{
							Name: "pool-1",
							NodeSelector: map[string]string{
								"some-key-1": "3",
							},
						},
						{
							Name: "pool-3",
							NodeSelector: map[string]string{
								"some-key-1": "4",
							},
						},
						{
							Name: "pool-1",
							NodeSelector: map[string]string{
								"some-key-1": "5",
							},
						},
					},
				},
			},
		}, []metav1.StatusCause{
			{
				Type:    metav1.CauseTypeFieldValueDuplicate,
				Message: "pools names should be unique, but there are two duplicates: pool-1",
				Field:   "spec.handlerPools.pools.name",
			},
			{
				Type:    metav1.CauseTypeFieldValueDuplicate,
				Message: "pools names should be unique, but there are two duplicates: pool-1",
				Field:   "spec.handlerPools.pools.name",
			},
		}),
		Entry("pools with incorrect labels", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{
						"some-key-1",
					},
					Pools: []v1.HandlerPoolConfig{
						{
							Name: "pool-1",
							NodeSelector: map[string]string{
								"some-key-1": "1",
							},
						},
						{
							Name: "pool-2",
							NodeSelector: map[string]string{
								"some-key-1": "2",
							},
						},
						{
							Name: "pool-3",
							NodeSelector: map[string]string{
								"some-key-1":    "3",
								"bad key value": "4",
							},
						},
					},
				},
			},
		}, []metav1.StatusCause{
			{
				Type:    "FieldValueInvalid",
				Message: "node selector label should be valid qualified names: bad key value",
				Field:   "spec.handlerPools.pools.nodeSelector",
			},
			{
				Type:    "FieldValueNotFound",
				Message: "node selectors should specify all partition keys, but pool-3 has an additional label: bad key value",
				Field:   "spec.handlerPools.pools.nodeSelector",
			},
			{
				Type:    "FieldValueNotFound",
				Message: "pool pool-3 doesn't specify all partition keys [some-key-1]",
				Field:   "spec.handlerPools.pools.nodeSelector",
			},
		}),
		Entry("pools with intersections", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{
						"some-key-1",
					},
					Pools: []v1.HandlerPoolConfig{
						{
							Name: "pool-1",
							NodeSelector: map[string]string{
								"some-key-1": "1",
							},
						},
						{
							Name: "pool-2",
							NodeSelector: map[string]string{
								"some-key-1": "2",
							},
						},
						{
							Name: "pool-3",
							NodeSelector: map[string]string{
								"some-key-1": "3",
							},
						},
						{
							Name: "pool-4",
							NodeSelector: map[string]string{
								"some-key-1": "1",
							},
						},
						{
							Name: "pool-5",
							NodeSelector: map[string]string{
								"some-key-1": "2",
							},
						},
					},
				},
			},
		}, []metav1.StatusCause{
			{
				Type:    "FieldValueDuplicate",
				Message: "pools selectors should be unique, but we have non unique selectors between pool-1 and pool-4",
				Field:   "spec.handlerPools.pools.nodeSelector",
			},
			{
				Type:    "FieldValueDuplicate",
				Message: "pools selectors should be unique, but we have non unique selectors between pool-2 and pool-5",
				Field:   "spec.handlerPools.pools.nodeSelector",
			},
		}),
		Entry("pools with several partition keys", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{
						"some-key-1",
						"some-key-2",
					},
					Pools: []v1.HandlerPoolConfig{
						{
							Name: "pool-1",
							NodeSelector: map[string]string{
								"some-key-1": "1",
								"some-key-2": "a",
							},
						},
						{
							Name: "pool-2",
							NodeSelector: map[string]string{
								"some-key-1": "2",
								"some-key-2": "b",
							},
						},
						{
							Name: "pool-3",
							NodeSelector: map[string]string{
								"some-key-1": "3",
								"some-key-2": "c",
							},
						},
						{
							Name: "pool-4",
							NodeSelector: map[string]string{
								"some-key-1": "1",
								"some-key-2": "d",
							},
						},
						{
							Name: "pool-5",
							NodeSelector: map[string]string{
								"some-key-1": "2",
								"some-key-2": "e",
							},
						},
					},
				},
			},
		}, nil),
		Entry("pools with omitted partition keys", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{
						"some-key-1",
						"some-key-2",
					},
					Pools: []v1.HandlerPoolConfig{
						{
							Name: "pool-1",
							NodeSelector: map[string]string{
								"some-key-1": "1",
								"some-key-2": "a",
							},
						},
						{
							Name: "pool-2",
							NodeSelector: map[string]string{
								"some-key-1": "2",
							},
						},
						{
							Name: "pool-3",
							NodeSelector: map[string]string{
								"some-key-1": "3",
								"some-key-2": "c",
							},
						},
						{
							Name: "pool-4",
							NodeSelector: map[string]string{
								"some-key-2": "d",
							},
						},
						{
							Name: "pool-5",
							NodeSelector: map[string]string{
								"some-key-1": "2",
								"some-key-2": "e",
							},
						},
					},
				},
			},
		}, []metav1.StatusCause{
			{
				Type:    "FieldValueNotFound",
				Message: "pool pool-2 doesn't specify all partition keys [some-key-1 some-key-2]",
				Field:   "spec.handlerPools.pools.nodeSelector",
			},
			{
				Type:    "FieldValueNotFound",
				Message: "pool pool-4 doesn't specify all partition keys [some-key-1 some-key-2]",
				Field:   "spec.handlerPools.pools.nodeSelector",
			},
		}),
		Entry("pools with not listed partition key selectors", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{
						"some-key-1",
						"some-key-2",
					},
					Pools: []v1.HandlerPoolConfig{
						{
							Name: "pool-1",
							NodeSelector: map[string]string{
								"some-key-1": "1",
								"some-key-2": "a",
							},
						},
						{
							Name: "pool-2",
							NodeSelector: map[string]string{
								"some-key-1": "2",
								"some-key-2": "b",
							},
						},
						{
							Name: "pool-3",
							NodeSelector: map[string]string{
								"some-key-1": "3",
								"some-key-2": "c",
							},
						},
						{
							Name: "pool-4",
							NodeSelector: map[string]string{
								"some-key-1": "4",
								"some-key-2": "d",
								"some-key-3": "4a",
							},
						},
						{
							Name: "pool-5",
							NodeSelector: map[string]string{
								"some-key-1": "2",
								"some-key-2": "e",
							},
						},
					},
				},
			},
		}, []metav1.StatusCause{
			{
				Type:    "FieldValueNotFound",
				Message: "node selectors should specify all partition keys, but pool-4 has an additional label: some-key-3",
				Field:   "spec.handlerPools.pools.nodeSelector",
			},
			{
				Type:    "FieldValueNotFound",
				Message: "pool pool-4 doesn't specify all partition keys [some-key-1 some-key-2]",
				Field:   "spec.handlerPools.pools.nodeSelector",
			},
		}),
		Entry("node selectors and workload selectors conflict", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				Workloads: &v1.ComponentConfig{
					NodePlacement: &v1.NodePlacement{
						NodeSelector: map[string]string{
							"some-key-1": "5",
						},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{
						"some-key-1",
					},
					Pools: []v1.HandlerPoolConfig{
						{
							Name: "pool-1",
							NodeSelector: map[string]string{
								"some-key-1": "1",
							},
						},
					},
				},
			},
		}, []metav1.StatusCause{
			{
				Type:    "FieldValueInvalid",
				Message: "node selectors has a conflict with the workload selectors: a handler pool pool-1 has a conflicting selector some-key-1 with workloads selector",
				Field:   "spec.handlerPools.pools.nodeSelector",
			},
		}),
		Entry("node selectors and workload selectors no conflict on empty map", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				Workloads: &v1.ComponentConfig{
					NodePlacement: &v1.NodePlacement{
						NodeSelector: map[string]string{},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{
						"some-key-1",
					},
					Pools: []v1.HandlerPoolConfig{
						{
							Name: "pool-1",
							NodeSelector: map[string]string{
								"some-key-1": "1",
							},
						},
					},
				},
			},
		}, nil),
		Entry("pools with concatenated selectors", &v1.KubeVirt{
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{"a", "b"},
					Pools: []v1.HandlerPoolConfig{
						{
							Name: "pool-1",
							NodeSelector: map[string]string{
								"a": "x",
								"b": "by",
							},
						},
						{
							Name: "pool-2",
							NodeSelector: map[string]string{
								"a": "xb",
								"b": "y",
							},
						},
					},
				},
			},
		}, nil),
	)

	Context("with TLSConfiguration", func() {
		DescribeTable("should reject", func(tlsConfiguration *v1.TLSConfiguration, expectedErrorMessage string, indexInField int) {
			causes := validateTLSConfiguration(tlsConfiguration)

			Expect(causes).To(HaveLen(1))
			Expect(causes[0].Message).To(Equal(expectedErrorMessage))
			field := "spec.configuration.tlsConfiguration.ciphers"
			if indexInField != -1 {
				field = fmt.Sprintf("%s#%d", field, indexInField)
			}
			Expect(causes[0].Field).To(Equal(field))
		},
			Entry("with unspecified minTLSVersion but non empty ciphers",
				&v1.TLSConfiguration{Ciphers: []string{tls.CipherSuiteName(tls.TLS_AES_256_GCM_SHA384)}},
				"You cannot specify ciphers when spec.configuration.tlsConfiguration.minTLSVersion is empty or VersionTLS13",
				-1,
			),
			Entry("with specified ciphers and minTLSVersion = 1.3",
				&v1.TLSConfiguration{Ciphers: []string{tls.CipherSuiteName(tls.TLS_AES_256_GCM_SHA384)}, MinTLSVersion: v1.VersionTLS13},
				"You cannot specify ciphers when spec.configuration.tlsConfiguration.minTLSVersion is empty or VersionTLS13",
				-1,
			),
			Entry("with unknown cipher in the list",
				&v1.TLSConfiguration{
					MinTLSVersion: v1.VersionTLS12,
					Ciphers:       []string{tls.CipherSuiteName(tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256), "NOT_VALID_CIPHER"},
				},
				"NOT_VALID_CIPHER is not a valid cipher",
				1,
			),
		)
	})

	Context("with AdditionalGuestMemoryOverheadRatio", func() {
		DescribeTable("the ratio must be parsable to float", func(unparsableRatio string) {
			causes := validateGuestToRequestHeadroom(&unparsableRatio)
			Expect(causes).To(HaveLen(1))
		},
			Entry("not a number", "abcdefg"),
			Entry("number with bad formatting", "1.fd3ggx"),
		)

		DescribeTable("the ratio must be larger than 1", func(lessThanOneRatio string) {
			causes := validateGuestToRequestHeadroom(&lessThanOneRatio)
			Expect(causes).ToNot(BeEmpty())
		},
			Entry("0.999", "0.999"),
			Entry("negative number", "-1.3"),
		)

		DescribeTable("valid values", func(validRatio string) {
		},
			Entry("1.0", "1.0"),
			Entry("5", "5"),
			Entry("1.123", "1.123"),
		)
	})

	Context("deprecations", func() {
		var admitter *KubeVirtUpdateAdmitter

		BeforeEach(func() {
			clusterConfig, _, _ := testutils.NewFakeClusterConfigUsingKVConfig(&v1.KubeVirtConfiguration{})
			admitter = NewKubeVirtUpdateAdmitter(nil, clusterConfig)
		})

		admit := func(kubevirt v1.KubeVirt) *admissionv1.AdmissionResponse {
			return admitKVUpdate(admitter, &kubevirt, &kubevirt)
		}

		const warn = true
		const warnNotExpected = false

		DescribeTable("usage of mediatedDevicesTypes", func(shouldWarn bool, conf *v1.MediatedDevicesConfiguration) {
			kvObject := v1.KubeVirt{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
				},
				Spec: v1.KubeVirtSpec{
					Configuration: v1.KubeVirtConfiguration{
						MediatedDevicesConfiguration: conf,
					},
				},
			}

			response := admit(kvObject)
			Expect(response).NotTo(BeNil())
			if shouldWarn {
				Expect(response.Warnings).NotTo(BeEmpty())
				Expect(response.Warnings).To(ContainElement("spec.configuration.mediatedDevicesConfiguration.mediatedDevicesTypes is deprecated, use mediatedDeviceTypes"))
			} else {
				Expect(response.Warnings).To(BeEmpty())
			}
		},
			Entry("should warn if used", warn, &v1.MediatedDevicesConfiguration{
				MediatedDevicesTypes: []string{"test1", "test2"},
			}),

			Entry("should not warn if empty", warnNotExpected, &v1.MediatedDevicesConfiguration{
				MediatedDevicesTypes: []string{},
			}),
			Entry("should not warn if nil", warnNotExpected, &v1.MediatedDevicesConfiguration{
				MediatedDevicesTypes: nil,
			}),
			Entry("should not warn if configuration is nil", warnNotExpected, nil),
		)

		DescribeTable("usage of nodeMediatedDeviceTypes.mediatedDevicesTypes", func(shouldWarn bool, conf *v1.MediatedDevicesConfiguration) {
			kvObject := v1.KubeVirt{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
				},
				Spec: v1.KubeVirtSpec{
					Configuration: v1.KubeVirtConfiguration{
						MediatedDevicesConfiguration: conf,
					},
				},
			}

			response := admit(kvObject)
			Expect(response).NotTo(BeNil())
			if shouldWarn {
				Expect(response.Warnings).NotTo(BeEmpty())
				Expect(response.Warnings).To(ContainElement("spec.configuration.mediatedDevicesConfiguration.nodeMediatedDeviceTypes[0].mediatedDevicesTypes is deprecated, use mediatedDeviceTypes"))
			} else {
				Expect(response.Warnings).To(BeEmpty())
			}
		}, Entry("should warn if used", warn, &v1.MediatedDevicesConfiguration{
			NodeMediatedDeviceTypes: []v1.NodeMediatedDeviceTypesConfig{
				{
					NodeSelector:         map[string]string{},
					MediatedDevicesTypes: []string{"test1", "test2"},
					MediatedDeviceTypes:  []string{},
				},
			},
		}),
			Entry("should not warn if empty", warnNotExpected, &v1.MediatedDevicesConfiguration{
				NodeMediatedDeviceTypes: []v1.NodeMediatedDeviceTypesConfig{
					{
						NodeSelector:         map[string]string{},
						MediatedDevicesTypes: []string{},
						MediatedDeviceTypes:  []string{},
					},
				},
			}),
			Entry("should not warn if nil", warnNotExpected, &v1.MediatedDevicesConfiguration{
				NodeMediatedDeviceTypes: []v1.NodeMediatedDeviceTypesConfig{
					{
						NodeSelector:         map[string]string{},
						MediatedDevicesTypes: nil,
						MediatedDeviceTypes:  []string{},
					},
				},
			}),

			Entry("should not warn if configuration nil", warnNotExpected, nil),
		)

		DescribeTable("should raise warning when a deprecated feature-gate is enabled", func(featureGate, expectedWarning string) {
			kv := v1.KubeVirt{}
			kvBytes, err := json.Marshal(kv)
			Expect(err).ToNot(HaveOccurred())

			kv.Spec.Configuration.DeveloperConfiguration = &v1.DeveloperConfiguration{FeatureGates: []string{featureGate}}
			kvUpdatedBytes, err := json.Marshal(kv)
			Expect(err).ToNot(HaveOccurred())

			request := &admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Resource:  KubeVirtGroupVersionResource,
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{Raw: kvBytes},
					Object:    runtime.RawExtension{Raw: kvUpdatedBytes},
				},
			}

			Expect(admitter.Admit(context.Background(), request)).To(Equal(&admissionv1.AdmissionResponse{
				Allowed: true,
				Warnings: []string{
					expectedWarning,
				},
			}))
		},
			Entry("with LiveMigration", featuregate.LiveMigrationGate, fmt.Sprintf(featuregate.WarningPattern, featuregate.LiveMigrationGate, featuregate.GA)),
			Entry("with SRIOVLiveMigration", featuregate.SRIOVLiveMigrationGate, fmt.Sprintf(featuregate.WarningPattern, featuregate.SRIOVLiveMigrationGate, featuregate.GA)),
			Entry("with NonRoot", featuregate.NonRoot, fmt.Sprintf(featuregate.WarningPattern, featuregate.NonRoot, featuregate.GA)),
			Entry("with PSA", featuregate.PSA, fmt.Sprintf(featuregate.WarningPattern, featuregate.PSA, featuregate.GA)),
			Entry("with CPUNodeDiscoveryGate", featuregate.CPUNodeDiscoveryGate, fmt.Sprintf(featuregate.WarningPattern, featuregate.CPUNodeDiscoveryGate, featuregate.GA)),
			Entry("with HotplugNICs", featuregate.HotplugNetworkIfacesGate, fmt.Sprintf(featuregate.WarningPattern, featuregate.HotplugNetworkIfacesGate, featuregate.GA)),
			Entry("with Passt", featuregate.PasstGate, featuregate.PasstDiscontinueMessage),
			Entry("with MacvtapGate", featuregate.MacvtapGate, featuregate.MacvtapDiscontinueMessage),
			Entry("with ExperimentalVirtiofsSupport", featuregate.VirtIOFSGate, featuregate.VirtioFsFeatureGateDiscontinueMessage),
			Entry("with DisableMediatedDevicesHandling", featuregate.DisableMediatedDevicesHandling, "DisableMDEVConfiguration has been deprecated since v1.8.0"),
		)

		DescribeTable("should raise warning when archConfig is set for ppc64le", func(shouldWarn bool, archConfig *v1.ArchConfiguration) {
			kv := v1.KubeVirt{
				Spec: v1.KubeVirtSpec{
					Configuration: v1.KubeVirtConfiguration{
						ArchitectureConfiguration: archConfig,
					},
				},
			}

			response := admit(kv)
			Expect(response).NotTo(BeNil())

			if shouldWarn {
				Expect(response.Warnings).NotTo(BeEmpty())
				Expect(response.Warnings).To(ContainElement("spec.configuration.architectureConfiguration.ppc64le is deprecated and no longer supported."))
			} else {
				Expect(response.Warnings).To(BeEmpty())
			}
		},
			Entry("should warn when archConfig is set for ppc64le", true, &v1.ArchConfiguration{Ppc64le: &v1.ArchSpecificConfiguration{}}),
			Entry("should not warn when archConfig is not set for ppc64le", false, &v1.ArchConfiguration{}),
		)
	})

	Context("Feature Gate Validation", func() {
		var admitter *KubeVirtUpdateAdmitter

		BeforeEach(func() {
			clusterConfig, _, _ := testutils.NewFakeClusterConfigUsingKVConfig(&v1.KubeVirtConfiguration{})
			admitter = NewKubeVirtUpdateAdmitter(nil, clusterConfig)
		})

		admitUpdate := func(devConfig *v1.DeveloperConfiguration) *admissionv1.AdmissionResponse {
			oldKV := &v1.KubeVirt{ObjectMeta: metav1.ObjectMeta{Name: "test"}}
			newKV := oldKV.DeepCopy()
			newKV.Spec.Configuration.DeveloperConfiguration = devConfig
			return admitKVUpdate(admitter, oldKV, newKV)
		}

		DescribeTable("should reject conflicting feature gates", func(enabledGates, disabledGates []string, expectedConflictingGates ...string) {
			var devConfig *v1.DeveloperConfiguration
			if enabledGates != nil || disabledGates != nil {
				devConfig = &v1.DeveloperConfiguration{
					FeatureGates:         enabledGates,
					DisabledFeatureGates: disabledGates,
				}
			}

			response := admitUpdate(devConfig)

			if len(expectedConflictingGates) == 0 {
				Expect(response.Allowed).To(BeTrue())
			} else {
				Expect(response.Allowed).To(BeFalse())
				Expect(response.Result.Details.Causes).To(HaveLen(len(expectedConflictingGates)))
				for _, gate := range expectedConflictingGates {
					Expect(response.Result.Details.Causes).To(ContainElement(And(
						HaveField("Message", fmt.Sprintf(`feature gate "%s" exists on both "FeatureGates" and "DisabledFeatureGates"`, gate)),
						HaveField("Type", metav1.CauseTypeForbidden),
						HaveField("Field", field.NewPath("spec", "configuration", "developerConfiguration", "featureGates").String()),
					)), `Expected to find conflict for gate: %s`, gate)
				}
			}
		},
			Entry("no conflict - both lists empty",
				[]string{},
				[]string{}),

			Entry("no conflict - only enabled gates",
				[]string{"Gate1", "Gate2"},
				[]string{}),

			Entry("no conflict - only disabled gates",
				[]string{},
				[]string{"Gate1", "Gate2"}),

			Entry("no conflict - different gates",
				[]string{"EnabledGate1", "EnabledGate2"},
				[]string{"DisabledGate1", "DisabledGate2"}),

			Entry("no conflict - nil DeveloperConfiguration",
				nil, nil),

			Entry("single conflict - same gate in both lists",
				[]string{"ConflictGate", "ValidGate1"},
				[]string{"ConflictGate", "ValidGate2"},
				"ConflictGate"),

			Entry("multiple conflicts",
				[]string{"Conflict1", "Conflict2", "ValidGate"},
				[]string{"Conflict1", "Conflict2", "AnotherValid"},
				"Conflict1", "Conflict2"),

			Entry("all gates conflict",
				[]string{"Gate1", "Gate2", "Gate3"},
				[]string{"Gate1", "Gate2", "Gate3"},
				"Gate1", "Gate2", "Gate3"),
		)
	})
})

func admitKVUpdate(admitter *KubeVirtUpdateAdmitter, oldKV, newKV *v1.KubeVirt) *admissionv1.AdmissionResponse {
	oldKVBytes, err := json.Marshal(oldKV)
	ExpectWithOffset(1, err).ToNot(HaveOccurred())
	newKVBytes, err := json.Marshal(newKV)
	ExpectWithOffset(1, err).ToNot(HaveOccurred())

	request := &admissionv1.AdmissionReview{
		Request: &admissionv1.AdmissionRequest{
			Resource:  KubeVirtGroupVersionResource,
			Operation: admissionv1.Update,
			OldObject: runtime.RawExtension{Raw: oldKVBytes},
			Object:    runtime.RawExtension{Raw: newKVBytes},
		},
	}
	return admitter.Admit(context.Background(), request)
}
