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

package tests_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"

	k6tv1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"

	"kubevirt.io/kubevirt/pkg/virt-config/featuregate"

	"kubevirt.io/kubevirt/tests/console"
	"kubevirt.io/kubevirt/tests/decorators"
	"kubevirt.io/kubevirt/tests/flags"
	"kubevirt.io/kubevirt/tests/framework/kubevirt"
	"kubevirt.io/kubevirt/tests/framework/matcher"
	"kubevirt.io/kubevirt/tests/libkubevirt"
	"kubevirt.io/kubevirt/tests/libvmifact"
	"kubevirt.io/kubevirt/tests/libvmops"
	"kubevirt.io/kubevirt/tests/libwait"
	"kubevirt.io/kubevirt/tests/testsuite"
)

var _ = Describe("[sig-operator] virt-handler pools", Serial, decorators.SigOperator, func() {
	const (
		poolSelectorLabelName = "handler-pool"
		virtHandlerName       = "virt-handler"
		poolNamePrefix        = "pool"
	)

	var (
		originalSpec *k6tv1.KubeVirtSpec

		client kubecli.KubevirtClient
		ctx    context.Context
	)

	waitForPoolsBeReady := func(expectedPools int) {
		Eventually(func(g Gomega) {
			daemonSets, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).List(ctx, metav1.ListOptions{})
			g.Expect(err).ToNot(HaveOccurred())

			poolsCount := 0
			prefix := fmt.Sprintf("%s-%s", virtHandlerName, poolNamePrefix)
			for _, ds := range daemonSets.Items {
				if !strings.HasPrefix(ds.Name, prefix) {
					continue
				}
				g.Expect(ds.Status.DesiredNumberScheduled).To(BeNumerically(">", 0))
				g.Expect(ds.Status.DesiredNumberScheduled).To(Equal(ds.Status.NumberReady))
				poolsCount += 1
			}

			g.Expect(poolsCount).To(BeNumerically("==", expectedPools))
		}, 240*time.Second, 1*time.Second).Should(Succeed(), "waiting for virt-handler pools to be ready")
	}

	deployPools := func(ctx context.Context, client kubecli.KubevirtClient) ([]k6tv1.HandlerPoolConfig, error) {
		kv := libkubevirt.GetCurrentKv(client)
		poolsCount, err := getMaxPossiblePoolsCount(ctx, client, virtHandlerName)
		if err != nil {
			return nil, err
		}
		if poolsCount < 1 {
			return nil, errors.New("not enough nodes for virt-handler pools")
		}

		kv.Spec.HandlerPools, err = generatePools(poolsCount, poolNamePrefix, poolSelectorLabelName)
		if err != nil {
			return nil, err
		}

		nodes, err := getEligibleNodes(ctx, client, virtHandlerName)
		if err != nil {
			return nil, err
		}

		if len(nodes) == 0 {
			return nil, errors.New("the cluster doesn't have nodes with running virt-handler pods")
		}

		if len(nodes) < poolsCount {
			return nil, fmt.Errorf("not enough nodes with running virt-handler for full rollout; have %d; required %d", len(nodes), poolsCount)
		}

		nodesLabels, err := setNodesLabels(ctx, client, kv, nodes)
		DeferCleanup(func() {
			for nodeName, labelKeys := range nodesLabels {
				var labelStr []string
				for _, k := range labelKeys {
					labelStr = append(labelStr, fmt.Sprintf(`"%s":null`, k))
				}
				patch := []byte(fmt.Sprintf(`{"metadata":{"labels":{%s}}}`, strings.Join(labelStr, ",")))
				_, err := client.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, patch, metav1.PatchOptions{})
				Expect(err).ToNot(HaveOccurred())
			}

			nodesLabels = nil
		})

		if err != nil {
			return nil, err
		}

		enableHandlerPools(kv)

		kv, err = client.KubeVirt(flags.KubeVirtInstallNamespace).Update(ctx, kv, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}

		return kv.Spec.HandlerPools, nil
	}

	removePools := func(ctx context.Context, client kubecli.KubevirtClient) {
		var kv *k6tv1.KubeVirt

		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			kv = libkubevirt.GetCurrentKv(client).DeepCopy()
			kv.Spec = *originalSpec.DeepCopy()
			_, err := client.KubeVirt(kv.Namespace).Update(ctx, kv, metav1.UpdateOptions{})
			return err
		})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			daemonSets, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).List(ctx, metav1.ListOptions{})
			g.Expect(err).ToNot(HaveOccurred())

			prefix := fmt.Sprintf("%s-%s", virtHandlerName, poolNamePrefix)
			hasPool := slices.ContainsFunc(daemonSets.Items, func(ds appsv1.DaemonSet) bool {
				return strings.HasPrefix(ds.Name, prefix)
			})
			g.Expect(hasPool).To(BeFalse())
		}, 240*time.Second, 1*time.Second).Should(Succeed(), "waiting for virt-handler pools to be ready")
	}

	runVMI := func(ctx context.Context, client kubecli.KubevirtClient) (*k6tv1.VirtualMachineInstance, string) {
		vmi := libvmops.RunVMIAndExpectLaunch(libvmifact.NewAlpine(), libvmops.StartupTimeoutSecondsSmall)
		vmi = libwait.WaitUntilVMIReady(vmi, console.LoginToAlpine)

		DeferCleanup(func() {
			err := client.VirtualMachineInstance(testsuite.GetTestNamespace(vmi)).Delete(ctx, vmi.Name, metav1.DeleteOptions{})
			if err != nil && !k8serrors.IsNotFound(err) {
				Expect(err).ToNot(HaveOccurred())
			}
			Expect(libwait.WaitForVirtualMachineToDisappearWithTimeout(vmi, 120*time.Second)).To(Succeed())
		})

		bootID, err := console.RunCommandAndStoreOutput(vmi, "cat /proc/sys/kernel/random/boot_id", 15*time.Second)
		Expect(err).ToNot(HaveOccurred())

		return vmi, bootID
	}

	checkVMIOperational := func(ctx context.Context, client kubecli.KubevirtClient, vmi *k6tv1.VirtualMachineInstance) {
		Consistently(matcher.ThisVMI(vmi), 2*time.Minute, 5*time.Second).
			Should(matcher.BeInPhase(k6tv1.Running))

		Expect(console.LoginToAlpine(vmi)).To(Succeed())
		Expect(console.RunCommand(vmi, "echo ok", 15*time.Second)).To(Succeed())
	}

	checkBootID := func(vmi *k6tv1.VirtualMachineInstance, bootID string) {
		bootIDAfter, err := console.RunCommandAndStoreOutput(vmi, "cat /proc/sys/kernel/random/boot_id", 15*time.Second)
		Expect(err).ToNot(HaveOccurred())
		Expect(strings.TrimSpace(bootIDAfter)).To(Equal(strings.TrimSpace(bootID)))
	}

	BeforeEach(func() {
		ctx = context.Background()
		client = kubevirt.Client()
		originalSpec = &libkubevirt.GetCurrentKv(client).Spec

		ds, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, virtHandlerName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(ds.Status.DesiredNumberScheduled).To(BeNumerically(">", 0))
		Expect(ds.Status.DesiredNumberScheduled).To(Equal(ds.Status.NumberReady))

		if ds.Status.NumberReady < 2 {
			Skip("virt-handler pools test requires at least two nodes suitable for virt-handler")
		}
	})

	AfterEach(func() {
		removePools(ctx, client)
		testsuite.EnsureKubevirtReadyWithTimeout(libkubevirt.GetCurrentKv(client), 420*time.Second)

		ds, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, virtHandlerName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(ds.Status.DesiredNumberScheduled).To(BeNumerically(">", 0))
		Expect(ds.Status.DesiredNumberScheduled).To(Equal(ds.Status.NumberReady))
		Expect(ds.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable.IntValue()).To(Equal(1))

		daemonSets, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).List(ctx, metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(len(daemonSets.Items)).To(BeNumerically(">", 0))

		prefix := fmt.Sprintf("%s-%s", virtHandlerName, poolNamePrefix)
		hasPool := slices.ContainsFunc(daemonSets.Items, func(ds appsv1.DaemonSet) bool {
			return strings.HasPrefix(ds.Name, prefix)
		})
		Expect(hasPool).To(BeFalse())
	})

	It("should successfully deploy", func() {
		By("running a VMI")
		vmiBeforePools, bootIDBeforePools := runVMI(ctx, client)
		checkVMIOperational(ctx, client, vmiBeforePools)
		checkBootID(vmiBeforePools, bootIDBeforePools)

		By("deploying virt-handler pools")
		pools, err := deployPools(ctx, client)
		Expect(err).ToNot(HaveOccurred())

		waitForPoolsBeReady(len(pools))

		Eventually(func(g Gomega) {
			ds, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, virtHandlerName, metav1.GetOptions{})
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(ds.Status.DesiredNumberScheduled).To(BeZero())
			g.Expect(ds.Status.NumberReady).To(BeZero())
		}, 240*time.Second, 1*time.Second).Should(Succeed(), "waiting for virt-handler daemonset to be updated")

		testsuite.EnsureKubevirtReadyWithTimeout(libkubevirt.GetCurrentKv(client), 420*time.Second)

		By("running a VMI after pools deployment")
		vmiWithPools, bootIDWithPools := runVMI(ctx, client)
		checkVMIOperational(ctx, client, vmiWithPools)
		checkBootID(vmiWithPools, bootIDWithPools)

		By("checking VMI started before virt-handler pools had been deployed")
		checkVMIOperational(ctx, client, vmiBeforePools)
		checkBootID(vmiBeforePools, bootIDBeforePools)

		By("downgrading to default virt-handler deployment scheme")
		removePools(ctx, client)

		testsuite.EnsureKubevirtReadyWithTimeout(libkubevirt.GetCurrentKv(client), 420*time.Second)

		By("checking VMIs after downgrade")
		checkVMIOperational(ctx, client, vmiBeforePools)
		checkBootID(vmiBeforePools, bootIDBeforePools)

		checkVMIOperational(ctx, client, vmiWithPools)
		checkBootID(vmiWithPools, bootIDWithPools)
	})

	It("should correctly use several labels", func() {
		const secondLabel = "some-second-label"

		By("deploying pools")
		maxPoolsCount, err := getMaxPossiblePoolsCount(ctx, client, virtHandlerName)
		Expect(err).ToNot(HaveOccurred())
		Expect(maxPoolsCount).To(BeNumerically(">", 1))

		poolsCount := maxPoolsCount - 1
		pools, err := generatePools(poolsCount, poolNamePrefix, poolSelectorLabelName)
		Expect(err).ToNot(HaveOccurred())

		pools[0].NodeSelector[secondLabel] = ""

		nodes, err := getEligibleNodes(ctx, client, virtHandlerName)
		Expect(err).ToNot(HaveOccurred())
		Expect(len(nodes)).To(BeNumerically(">=", len(pools)))

		kv := libkubevirt.GetCurrentKv(client)
		enableHandlerPools(kv)
		kv.Spec.HandlerPools = pools

		nodesLabels, err := setNodesLabels(ctx, client, kv, nodes)
		DeferCleanup(func() {
			for nodeName, labelKeys := range nodesLabels {
				var labelStr []string
				for _, k := range labelKeys {
					labelStr = append(labelStr, fmt.Sprintf(`"%s":null`, k))
				}
				patch := []byte(fmt.Sprintf(`{"metadata":{"labels":{%s}}}`, strings.Join(labelStr, ",")))
				_, err := client.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, patch, metav1.PatchOptions{})
				Expect(err).ToNot(HaveOccurred())
			}

			nodesLabels = nil
		})
		Expect(err).ToNot(HaveOccurred())

		kv, err = client.KubeVirt(flags.KubeVirtInstallNamespace).Update(ctx, kv, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		testsuite.EnsureKubevirtReadyWithTimeout(kv, 420*time.Second)

		By("checking pools expectations")
		daemonSets, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).List(ctx, metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())

		virtHandlers := slices.DeleteFunc(daemonSets.Items, func(ds appsv1.DaemonSet) bool {
			return !strings.HasPrefix(ds.Name, virtHandlerName)
		})
		Expect(poolsCount + 1).To(BeNumerically("==", len(virtHandlers))) // pools + one default

		for _, ds := range virtHandlers {
			Expect(ds.Status.DesiredNumberScheduled).ToNot(BeZero())
			Expect(ds.Status.NumberReady).To(BeNumerically("==", ds.Status.DesiredNumberScheduled))
		}

		By("checking correct placement")
		clusterNodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())

		seenVirtHandlers := 0
		for _, node := range clusterNodes.Items {
			pods, err := listRunningPodsOnNode(ctx, client, node.Name)
			Expect(err).ToNot(HaveOccurred())

			ownerName := ""
			ownerNamespace := ""
			for _, pod := range pods {
				for _, ownerRef := range pod.OwnerReferences {
					if ownerRef.Kind == "DaemonSet" && strings.HasPrefix(ownerRef.Name, virtHandlerName) {
						Expect(ownerName).To(BeEmpty())
						ownerNamespace = pod.Namespace
						ownerName = ownerRef.Name
					}
				}
			}

			if ownerName == "" {
				continue
			}

			ds, err := client.AppsV1().DaemonSets(ownerNamespace).Get(ctx, ownerName, metav1.GetOptions{})
			Expect(err).ToNot(HaveOccurred())

			prefix := fmt.Sprintf("%s-%s", virtHandlerName, poolNamePrefix)
			if strings.HasPrefix(ds.Name, prefix) {
				// All selectors AND, so a node must contain them all.
				for key, value := range ds.Spec.Template.Spec.NodeSelector {
					valueNode, found := node.Labels[key]
					Expect(found).To(BeTrue(), fmt.Sprintf("node %s missing selector key %q for daemonset %s", node.Name, key, ds.Name))
					Expect(valueNode).To(BeIdenticalTo(value),
						fmt.Sprintf("node %s has wrong value for key %q for daemonset %s", node.Name, key, ds.Name))
				}
				seenVirtHandlers += 1
			} else if ds.Name == virtHandlerName {
				for _, pool := range pools {
					matchesPool := true
					for key, expected := range pool.NodeSelector {
						actual, found := node.Labels[key]
						if !found || actual != expected {
							matchesPool = false
							break
						}
					}

					Expect(matchesPool).To(BeFalse(),
						fmt.Sprintf("default virt-handler runs on node %s, which matches pool selector %v", node.Name, pool.NodeSelector))
				}

				seenVirtHandlers += 1
			}
		}

		Expect(poolsCount + 1).To(BeNumerically("==", seenVirtHandlers)) // pools + one default
	})
})

func generatePools(
	poolsCount int,
	poolNamePrefix string,
	poolSelectorLabelName string,
) ([]k6tv1.HandlerPoolConfig, error) {
	handlerPools := make([]k6tv1.HandlerPoolConfig, 0, poolsCount)
	for i := range poolsCount {
		poolName := fmt.Sprintf("%s-%d", poolNamePrefix, i)
		handlerPools = append(handlerPools,
			k6tv1.HandlerPoolConfig{
				Name: poolName,
				NodeSelector: map[string]string{
					poolSelectorLabelName: poolName,
				},
			})
	}

	return handlerPools, nil
}

func setNodesLabels(
	ctx context.Context,
	client kubecli.KubevirtClient,
	kv *k6tv1.KubeVirt,
	nodes []*corev1.Node,
) (map[string][]string, error) {
	nodesLabels := make(map[string][]string)
	for i, pool := range kv.Spec.HandlerPools {
		var labels []string
		keys := make(map[string]struct{})
		for k, v := range pool.NodeSelector {
			labels = append(labels, fmt.Sprintf(`"%s": "%s"`, k, v))
			keys[k] = struct{}{}
		}

		patch := []byte(fmt.Sprintf(`{"metadata": {"labels":{%s}}}`, strings.Join(labels, ",")))
		_, err := client.CoreV1().Nodes().Patch(ctx, nodes[i].Name, types.MergePatchType, patch, metav1.PatchOptions{})
		if err != nil {
			return nodesLabels, err
		}

		for k := range keys {
			nodesLabels[nodes[i].Name] = append(nodesLabels[nodes[i].Name], k)
		}
	}
	return nodesLabels, nil
}

func getMaxPossiblePoolsCount(ctx context.Context, client kubecli.KubevirtClient, dsName string) (int, error) {
	ds, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, dsName, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}

	if ds.Status.NumberReady <= 1 {
		return 0, errors.New("can't deploy virt-handler pools. not enough capacity")
	}

	return int(ds.Status.DesiredNumberScheduled), nil
}

func listPodsOnNode(ctx context.Context, client kubecli.KubevirtClient, nodeName string, selectors ...fields.Selector) ([]corev1.Pod, error) {
	podsSelectors := []fields.Selector{
		fields.OneTermEqualSelector("spec.nodeName", nodeName),
	}
	podsSelectors = append(podsSelectors, selectors...)
	podsSelector := fields.AndSelectors(podsSelectors...)

	podList, err := client.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: podsSelector.String(),
	})
	if err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func listRunningPodsOnNode(ctx context.Context, client kubecli.KubevirtClient, nodeName string) ([]corev1.Pod, error) {
	pods, err := listPodsOnNode(ctx, client, nodeName, fields.OneTermEqualSelector("status.phase", string(corev1.PodRunning)))
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(pods, func(pod corev1.Pod) bool {
		return pod.DeletionTimestamp != nil
	}), nil
}

func getEligibleNodes(ctx context.Context, client kubecli.KubevirtClient, dsName string) ([]*corev1.Node, error) {
	nodesList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	// Get eligible nodes for virt-handlers
	nodes := make([]*corev1.Node, 0, len(nodesList.Items))
	seen := make(map[string]struct{}, len(nodesList.Items))

	for i := range nodesList.Items {
		node := &nodesList.Items[i]
		podList, err := listRunningPodsOnNode(ctx, client, node.Name)
		if err != nil {
			return nil, err
		}

		hasVirtHandler := false
		for _, pod := range podList {
			if strings.HasPrefix(pod.Name, dsName) {
				hasVirtHandler = true
				break
			}
		}

		if _, found := seen[node.Name]; hasVirtHandler && !found {
			seen[node.Name] = struct{}{}
			nodes = append(nodes, node)
		}
	}
	return nodes, nil
}

func enableHandlerPools(kv *k6tv1.KubeVirt) {
	if kv.Spec.Configuration.DeveloperConfiguration == nil {
		kv.Spec.Configuration.DeveloperConfiguration = &k6tv1.DeveloperConfiguration{}
	}

	kv.Spec.Configuration.DeveloperConfiguration.FeatureGates =
		append(kv.Spec.Configuration.DeveloperConfiguration.FeatureGates, featuregate.HandlerPoolsGate)
}
