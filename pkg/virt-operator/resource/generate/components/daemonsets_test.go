package components

import (
	"errors"
	"math/rand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"

	operatorutil "kubevirt.io/kubevirt/pkg/virt-operator/util"
)

var _ = Describe("Handler DaemonSet", func() {
	var config *operatorutil.KubeVirtDeploymentConfig

	BeforeEach(func() {
		config = &operatorutil.KubeVirtDeploymentConfig{}
	})

	DescribeTable("should propagate imagePullPolicy to",
		func(additionalProperties map[string]string, containerName string, isInitContainer bool, expectedPolicy corev1.PullPolicy) {
			config.AdditionalProperties = additionalProperties
			ds, err := NewHandlerDaemonSet(config, "", "", "", nil)
			Expect(err).ToNot(HaveOccurred())

			containers := ds.Spec.Template.Spec.Containers
			if isInitContainer {
				containers = ds.Spec.Template.Spec.InitContainers
			}

			var target *corev1.Container
			for i := range containers {
				if containers[i].Name == containerName {
					target = &containers[i]
					break
				}
			}
			Expect(target).NotTo(BeNil(), "container %s should exist", containerName)
			Expect(target.ImagePullPolicy).To(Equal(expectedPolicy))
		},
		Entry("the virt-launcher init container when configured",
			map[string]string{
				operatorutil.AdditionalPropertiesNamePullPolicy: string(corev1.PullAlways),
			},
			"virt-launcher", true, corev1.PullAlways,
		),
		Entry("the virt-launcher init container by default",
			map[string]string(nil),
			"virt-launcher", true, corev1.PullIfNotPresent,
		),
		Entry("the virt-launcher-image-holder container when configured",
			map[string]string{
				operatorutil.AdditionalPropertiesNamePullPolicy: string(corev1.PullAlways),
				operatorutil.AdditionalPropertiesPullSecrets:    `[{"name":"test-secret"}]`,
			},
			"virt-launcher-image-holder", false, corev1.PullAlways,
		),
		Entry("the virt-launcher-image-holder container by default",
			map[string]string{
				operatorutil.AdditionalPropertiesPullSecrets: `[{"name":"test-secret"}]`,
			},
			"virt-launcher-image-holder", false, corev1.PullIfNotPresent,
		),
	)

	It("should not use bidirectional mount propagation for the kubelet volume", func() {
		ds, err := NewHandlerDaemonSet(config, "", "", "", nil)
		Expect(err).ToNot(HaveOccurred())
		container := ds.Spec.Template.Spec.Containers[0]

		var kubeletMount *corev1.VolumeMount
		for i := range container.VolumeMounts {
			if container.VolumeMounts[i].Name == "kubelet" {
				kubeletMount = &container.VolumeMounts[i]
				break
			}
		}
		Expect(kubeletMount).NotTo(BeNil(), "kubelet volume mount should exist")
		Expect(kubeletMount.MountPropagation).NotTo(BeNil())
		Expect(*kubeletMount.MountPropagation).To(Equal(corev1.MountPropagationHostToContainer))
	})

	DescribeTable("should correctly serialize node selector terms for the default virt-handler",
		func(partitionKeys []string, poolsTerms []map[string]string, expectedTerms []corev1.NodeSelectorTerm, expectedError error) {
			// The generated terms should be stable and independent of orders of pools and partition keys.
			// Randomize the order of the keys.
			rand.Shuffle(len(partitionKeys), func(i, j int) {
				partitionKeys[i], partitionKeys[j] = partitionKeys[j], partitionKeys[i]
			})

			// Randomize the order of the pools.
			rand.Shuffle(len(poolsTerms), func(i, j int) {
				poolsTerms[i], poolsTerms[j] = poolsTerms[j], poolsTerms[i]
			})

			tree, err := newNodeSelectorTermsTree(partitionKeys)
			if err != nil && expectedError != nil {
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(expectedError))
				return
			}
			Expect(err).ToNot(HaveOccurred())
			for _, t := range poolsTerms {
				err = tree.Insert(t)
				if err != nil && expectedError != nil {
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(expectedError))
					return
				}
			}

			Expect(tree.Terms()).To(Equal(expectedTerms))
		},
		Entry("no pools",
			[]string{"key1", "key2", "key3"},
			[]map[string]string{},
			nil,
			nil,
		),
		Entry("empty partition keys",
			[]string{},
			[]map[string]string{},
			nil,
			errors.New("partition keys shouldn't be empty"),
		),
		Entry("duplicate partition keys",
			[]string{"key1", "key2", "key3", "key4", "key1", "key5"},
			[]map[string]string{},
			nil,
			errors.New("duplicate partition key key1"),
		),
		Entry("missing a partition key",
			[]string{"key1", "key2", "key3"},
			[]map[string]string{
				{
					"key1": "1",
					"key2": "1a",
					"key3": "1aa",
				},
				{
					"key1": "2",
					"key2": "1a",
					"key3": "3aa",
				},
				{
					"key1": "1",
					"key3": "1aa",
				},
			},
			nil,
			errors.New("node selector terms don't conform with partition keys"),
		),
		Entry("an extra partition key",
			[]string{"key1", "key2", "key3"},
			[]map[string]string{
				{
					"key1": "1",
					"key2": "1a",
					"key3": "1aa",
				},
				{
					"key1": "2",
					"key2": "1a",
					"key3": "3aa",
				},
				{
					"key1": "1",
					"key2": "1a",
					"key3": "1aa",
					"key4": "1aa",
				},
			},
			nil,
			errors.New("node selector terms don't conform with partition keys"),
		),
		Entry("an extra partition key and one missed key",
			[]string{"key1", "key2", "key3"},
			[]map[string]string{
				{
					"key1": "1",
					"key2": "1a",
					"key3": "1aa",
				},
				{
					"key1": "2",
					"key2": "1a",
					"key3": "3aa",
				},
				{
					"key1": "1",
					"key2": "1a",
					"key4": "1aa",
				},
			},
			nil,
			errors.New("terms doesn't contain key3 key"),
		),
		Entry("one partition key",
			[]string{"key1"},
			[]map[string]string{
				{
					"key1": "1",
				},
				{
					"key1": "2",
				},
				{
					"key1": "3",
				},
			},
			[]corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key1", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1", "2", "3"}},
					},
				},
			},
			nil,
		),
		Entry("one pool with three keys",
			[]string{"key1", "key2", "key3"},
			[]map[string]string{
				{
					"key1": "1",
					"key2": "1a",
					"key3": "1aa",
				},
			},
			[]corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key1", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key2", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1a"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key3", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1aa"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"1a"}},
					},
				},
			},
			nil,
		),
		Entry("three pools with three partition keys",
			[]string{"key1", "key2", "key3"},
			[]map[string]string{
				{
					"key1": "1",
					"key2": "1a",
					"key3": "1aa",
				},
				{
					"key1": "2",
					"key2": "1a",
					"key3": "3aa",
				},
				{
					"key1": "1",
					"key2": "12a",
					"key3": "1aa",
				},
			},
			[]corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key1", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1", "2"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key2", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1a"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"2"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key3", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"3aa"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"2"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"1a"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key2", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"12a", "1a"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key3", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1aa"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"1a"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key3", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1aa"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"12a"}},
					},
				},
			},
			nil,
		),
		Entry("full cartesian product",
			[]string{"key1", "key2"},
			[]map[string]string{
				{
					"key1": "a",
					"key2": "b",
				},
				{
					"key1": "b",
					"key2": "a",
				},
				{
					"key1": "a",
					"key2": "a",
				},
				{
					"key1": "b",
					"key2": "b",
				},
			},
			[]corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key1", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"a", "b"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key2", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"a", "b"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"b"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key2", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"a", "b"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}},
					},
				},
			},
			nil,
		),
		Entry("maximum divergence",
			[]string{"key1", "key2", "key3"},
			[]map[string]string{
				{
					"key1": "1",
					"key2": "1a",
					"key3": "1aa",
				},
				{
					"key1": "1",
					"key2": "2a",
					"key3": "1aa",
				},
				{
					"key1": "1",
					"key2": "2a",
					"key3": "1aa",
				},
				{
					"key1": "1",
					"key2": "2a",
					"key3": "2aa",
				},
				{
					"key1": "1",
					"key2": "2a",
					"key3": "3aa",
				},
				{
					"key1": "1",
					"key2": "3a",
					"key3": "1aa",
				},
				{
					"key1": "1",
					"key2": "3a",
					"key3": "1aa",
				},
				{
					"key1": "1",
					"key2": "3a",
					"key3": "2aa",
				},
				{
					"key1": "2",
					"key2": "1a",
					"key3": "3aa",
				},
				{
					"key1": "3",
					"key2": "12a",
					"key3": "1aa",
				},
			},
			[]corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key1", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1", "2", "3"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key2", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"12a"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"3"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key3", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1aa"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"3"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"12a"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key2", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1a"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"2"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key3", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"3aa"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"2"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"1a"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key2", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1a", "2a", "3a"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key3", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1aa", "2aa"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"3a"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key3", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1aa", "2aa", "3aa"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"2a"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key3", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1aa"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"1a"}},
					},
				},
			},
			nil,
		),
		Entry("duplicate pools",
			[]string{"key1", "key2"},
			[]map[string]string{
				{
					"key1": "1",
					"key2": "a",
				},
				{
					"key1": "1",
					"key2": "b",
				},
				{
					"key1": "1",
					"key2": "a",
				},
			},
			[]corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key1", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key2", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"a", "b"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
					},
				},
			},
			nil,
		),
		Entry("five keys with deep branching",
			[]string{"key1", "key2", "key3", "key4", "key5"},
			[]map[string]string{
				{
					"key1": "1",
					"key2": "2",
					"key3": "3",
					"key4": "4a",
					"key5": "5a",
				},
				{
					"key1": "1",
					"key2": "2",
					"key3": "3",
					"key4": "4b",
					"key5": "5b",
				},
			},
			[]corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key1", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"1"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key2", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"2"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key3", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"3"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"2"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key4", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"4a", "4b"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"2"}},
						{Key: "key3", Operator: corev1.NodeSelectorOpIn, Values: []string{"3"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key5", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"5b"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"2"}},
						{Key: "key3", Operator: corev1.NodeSelectorOpIn, Values: []string{"3"}},
						{Key: "key4", Operator: corev1.NodeSelectorOpIn, Values: []string{"4b"}},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "key5", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"5a"}},
						{Key: "key1", Operator: corev1.NodeSelectorOpIn, Values: []string{"1"}},
						{Key: "key2", Operator: corev1.NodeSelectorOpIn, Values: []string{"2"}},
						{Key: "key3", Operator: corev1.NodeSelectorOpIn, Values: []string{"3"}},
						{Key: "key4", Operator: corev1.NodeSelectorOpIn, Values: []string{"4a"}},
					},
				},
			},
			nil,
		),
	)
})
