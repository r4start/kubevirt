package components

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	virtv1 "kubevirt.io/api/core/v1"

	operatorutil "kubevirt.io/kubevirt/pkg/virt-operator/util"
)

var _ = Describe("virt-handler pools", func() {
	newConfig := func() *operatorutil.KubeVirtDeploymentConfig {
		return &operatorutil.KubeVirtDeploymentConfig{
			Namespace:       "kubevirt",
			ComponentImages: operatorutil.ComponentImages{VirtHandlerImage: "default-handler"},
			AdditionalProperties: map[string]string{
				operatorutil.AdditionalPropertiesHandlerPoolsEnabled: "",
			},
			HandlerPools: operatorutil.HandlersPoolConfig{
				PartitionKeys: []string{"hardware.example.com/family"},
				Pools: []operatorutil.HandlerPoolConfig{
					{
						Name:             "gpu",
						VirtHandlerImage: "gpu-handler",
						NodeSelector: map[string]string{
							"hardware.example.com/family": "gpu",
						},
					},
					{
						Name:             "cpu",
						VirtHandlerImage: "cpu-handler",
						NodeSelector: map[string]string{
							"hardware.example.com/family": "cpu",
						},
					},
				},
			},
		}
	}

	It("renders a pool-specific DaemonSet", func() {
		config := newConfig()
		daemonSet, err := NewHandlerDaemonSet(config, "", "", "", &config.HandlerPools.Pools[0])
		Expect(err).NotTo(HaveOccurred())
		Expect(daemonSet.Name).To(Equal("virt-handler-gpu"))
		Expect(daemonSet.Spec.Selector.MatchLabels).To(HaveKeyWithValue(handlerPoolLabel, "gpu"))
		Expect(daemonSet.Spec.Template.Labels).To(HaveKeyWithValue(handlerPoolLabel, "gpu"))
		Expect(daemonSet.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue("hardware.example.com/family", "gpu"))
		Expect(daemonSet.Spec.Template.Spec.Containers[0].Image).To(Equal("gpu-handler"))
		Expect(daemonSet.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution).To(ConsistOf(corev1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
				virtv1.AppLabel: VirtHandlerName,
			}},
			TopologyKey: corev1.LabelHostname,
		}))
	})

	It("excludes pool nodes from the default DaemonSet", func() {
		config := newConfig()
		daemonSet, err := NewHandlerDaemonSet(config, "", "", "", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(daemonSet.Name).To(Equal(VirtHandlerName))

		required := daemonSet.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		Expect(required.NodeSelectorTerms).To(Equal([]corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      "hardware.example.com/family",
				Operator: corev1.NodeSelectorOpNotIn,
				Values:   []string{"cpu", "gpu"},
			}},
		}}))
		Expect(daemonSet.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution).To(ConsistOf(corev1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
				virtv1.AppLabel: VirtHandlerName,
			}},
			TopologyKey: corev1.LabelHostname,
		}))
	})

	It("rejects selectors that do not conform to partition keys", func() {
		config := newConfig()
		config.HandlerPools.Pools[0].NodeSelector = map[string]string{"other": "gpu"}
		_, err := NewHandlerDaemonSet(config, "", "", "", nil)
		Expect(err).To(MatchError("terms doesn't contain hardware.example.com/family key"))
	})
})
