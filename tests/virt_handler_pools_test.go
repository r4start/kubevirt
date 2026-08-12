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
	"encoding/json"
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

	waitForPoolsBeRemoved := func(ctx context.Context, client kubecli.KubevirtClient) {
		Eventually(func(g Gomega) {
			daemonSets, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).List(ctx, metav1.ListOptions{})
			g.Expect(err).ToNot(HaveOccurred())

			prefix := fmt.Sprintf("%s-%s", virtHandlerName, poolNamePrefix)
			hasPool := slices.ContainsFunc(daemonSets.Items, func(ds appsv1.DaemonSet) bool {
				return strings.HasPrefix(ds.Name, prefix)
			})
			g.Expect(hasPool).To(BeFalse())
		}, 240*time.Second, 1*time.Second).Should(Succeed())
	}

	deployPools := func(ctx context.Context, client kubecli.KubevirtClient) (*k6tv1.HandlerPoolsConfig, error) {
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

		if err := setNodesLabels(ctx, client, kv, nodes); err != nil {
			return nil, err
		}

		enableHandlerPools(kv)

		kv, err = client.KubeVirt(flags.KubeVirtInstallNamespace).Update(ctx, kv, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}

		return kv.Spec.HandlerPools, nil
	}

	deploySpecificPools := func(ctx context.Context, client kubecli.KubevirtClient, pools *k6tv1.HandlerPoolsConfig) (*k6tv1.KubeVirt, error) {
		kv := libkubevirt.GetCurrentKv(client)
		kv.Spec.HandlerPools = pools

		nodes, err := getEligibleNodes(ctx, client, virtHandlerName)
		if err != nil {
			return nil, err
		}
		if len(nodes) == 0 {
			return nil, errors.New("the cluster doesn't have nodes with running virt-handler pods")
		}
		if len(nodes) < len(kv.Spec.HandlerPools.Pools) {
			return nil, fmt.Errorf("not enough nodes with running virt-handler for full rollout; have %d; required %d", len(nodes), len(kv.Spec.HandlerPools.Pools))
		}

		if err := setNodesLabels(ctx, client, kv, nodes); err != nil {
			return nil, err
		}

		enableHandlerPools(kv)

		kv, err = client.KubeVirt(flags.KubeVirtInstallNamespace).Update(ctx, kv, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}

		return kv, nil
	}

	deploySpecificPoolsWithExpectations := func(ctx context.Context, client kubecli.KubevirtClient, pools *k6tv1.HandlerPoolsConfig, expectedVirtHandlersCount int) (*k6tv1.KubeVirt, []appsv1.DaemonSet) {
		kv, err := deploySpecificPools(ctx, client, pools)
		Expect(err).ToNot(HaveOccurred())

		testsuite.EnsureKubevirtReadyWithTimeout(kv, 420*time.Second)

		By("checking pools expectations")
		daemonSets, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).List(ctx, metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())

		virtHandlers := slices.DeleteFunc(daemonSets.Items, func(ds appsv1.DaemonSet) bool {
			return !strings.HasPrefix(ds.Name, virtHandlerName)
		})
		Expect(len(virtHandlers)).To(Equal(expectedVirtHandlersCount))
		return kv, virtHandlers
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

		waitForPoolsBeRemoved(ctx, client)
	}

	runVMI := func(ctx context.Context, client kubecli.KubevirtClient) (*k6tv1.VirtualMachineInstance, string) {
		vmi := libvmops.RunVMIAndExpectLaunch(libvmifact.NewAlpine(), flags.StartupTimeoutSecondsSmall())
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

	checkCorrectPlacement := func(ctx context.Context, client kubecli.KubevirtClient, nodes []corev1.Node, pools *k6tv1.HandlerPoolsConfig) (int, int) {
		seenVirtHandlers := 0
		poolsSeen := 0
		for _, node := range nodes {
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
				poolsSeen += 1
			} else if ds.Name == virtHandlerName {
				for _, pool := range pools.Pools {
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
		return seenVirtHandlers, poolsSeen
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

		waitForPoolsBeReady(len(pools.Pools))

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

	It("should deploy multiple partition keys with an empty label value", func() {
		const secondLabel = "some-second-label"

		By("deploying pools")
		maxPoolsCount, err := getMaxPossiblePoolsCount(ctx, client, virtHandlerName)
		Expect(err).ToNot(HaveOccurred())
		Expect(maxPoolsCount).To(BeNumerically(">", 1))

		poolsCount := maxPoolsCount - 1
		pools, err := generatePools(poolsCount, poolNamePrefix, poolSelectorLabelName)
		Expect(err).ToNot(HaveOccurred())

		pools.PartitionKeys = append(pools.PartitionKeys, secondLabel)

		for _, pool := range pools.Pools {
			pool.NodeSelector[secondLabel] = ""
		}

		_, virtHandlers := deploySpecificPoolsWithExpectations(ctx, client, pools, poolsCount+1)
		for _, ds := range virtHandlers {
			Expect(ds.Status.DesiredNumberScheduled).ToNot(BeZero())
			Expect(ds.Status.NumberReady).To(BeNumerically("==", ds.Status.DesiredNumberScheduled))
		}

		By("checking correct placement")
		clusterNodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())

		seenVirtHandlers, _ := checkCorrectPlacement(ctx, client, clusterNodes.Items, pools)
		Expect(poolsCount + 1).To(BeNumerically("==", seenVirtHandlers)) // pools + one default
	})

	It("should use custom image and tag", func() {
		if flags.KubeVirtVersionTagAlt == "" {
			Skip("alt tag is not specified, but required for this test")
		}

		By("deploying pools")
		maxPoolsCount, err := getMaxPossiblePoolsCount(ctx, client, virtHandlerName)
		Expect(err).ToNot(HaveOccurred())
		Expect(maxPoolsCount).To(BeNumerically(">", 1))

		pools, err := generatePools(maxPoolsCount, poolNamePrefix, poolSelectorLabelName)
		Expect(err).ToNot(HaveOccurred())

		ds, err := client.AppsV1().
			DaemonSets(flags.KubeVirtInstallNamespace).
			Get(ctx, virtHandlerName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		virtHandlerContainer := findContainerByName(ds.Spec.Template.Spec.Containers, virtHandlerName)
		Expect(virtHandlerContainer).ToNot(BeNil())

		// Container image format: [registry/]image[:tag][@digest]
		// Example: some-cr.com/image:tag@sha256:abc
		// All parts are optional except the image name.
		// Steps to modify the image tag:
		// 1. Split by '/' to separate a registry path from repository and version info (e.g., "image:tag@sha256:abc").
		// 2. Split by '@' to separate the tag part from digest (e.g., "image:tag" and "sha256:abc"). Discard digest since we're changing the tag.
		// 3. Split by ':' to separate the repository from the tag (e.g., "image" and "tag"), then replace with the alternate tag.
		// 4. Reconstruct the full image reference.
		defaultImage := virtHandlerContainer.Image
		imageParts := strings.Split(virtHandlerContainer.Image, "/")
		partsCount := len(imageParts)
		Expect(partsCount).To(BeNumerically(">", 0))

		// Hashed version.
		image := imageParts[partsCount-1]
		imageParts = imageParts[:partsCount-1]

		imageTagVersionParts := strings.Split(image, "@")
		imageTagVersionPartsLen := len(imageTagVersionParts)
		Expect(imageTagVersionPartsLen).To(BeElementOf([]int{1, 2}))

		tagVersion := strings.Split(imageTagVersionParts[0], ":")
		tagVersionLen := len(tagVersion)
		Expect(tagVersionLen).To(BeElementOf([]int{1, 2}))

		// There is only image name.
		if tagVersionLen == 2 {
			tagVersion = tagVersion[:1]
		}
		tagVersion = append(tagVersion, flags.KubeVirtVersionTagAlt)

		imageWithAltTag := strings.Join(tagVersion, ":")
		imageParts = append(imageParts, imageWithAltTag)

		pools.Pools[0].VirtHandlerImage = strings.Join(imageParts, "/")

		kv, err := deploySpecificPools(ctx, client, pools)
		Expect(err).ToNot(HaveOccurred())

		testsuite.EnsureKubevirtReadyWithTimeout(kv, 420*time.Second)

		ds, err = client.AppsV1().
			DaemonSets(flags.KubeVirtInstallNamespace).
			Get(ctx, fmt.Sprintf("%s-%s", virtHandlerName, pools.Pools[0].Name), metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())

		virtHandlerContainer = findContainerByName(ds.Spec.Template.Spec.Containers, virtHandlerName)
		Expect(virtHandlerContainer).ToNot(BeNil())
		Expect(virtHandlerContainer.Image).To(BeIdenticalTo(pools.Pools[0].VirtHandlerImage))

		dsList, err := client.AppsV1().
			DaemonSets(flags.KubeVirtInstallNamespace).
			List(
				ctx,
				metav1.ListOptions{
					LabelSelector: "kubevirt.io=" + virtHandlerName,
				},
			)
		Expect(err).ToNot(HaveOccurred())
		Expect(dsList.Items).To(HaveLen(maxPoolsCount + 1))

		customImagePoolName := fmt.Sprintf("%s-%s", virtHandlerName, pools.Pools[0].Name)
		for _, ds := range dsList.Items {
			if ds.Name == customImagePoolName {
				continue
			}
			virtHandlerContainer = findContainerByName(ds.Spec.Template.Spec.Containers, virtHandlerName)
			Expect(virtHandlerContainer).ToNot(BeNil())
			Expect(virtHandlerContainer.Image).To(BeIdenticalTo(defaultImage))
		}
	})

	It("should add a pool to an existing pool configuration", func() {
		nodes, err := getEligibleNodes(ctx, client, virtHandlerName)
		Expect(err).ToNot(HaveOccurred())
		if len(nodes) < 3 {
			Skip("test requires at least 3 nodes with virt-handler")
		}

		By("deploying initial pools leaving one node for default handler")
		initialPoolCount := len(nodes) - 1
		pools, err := generatePools(initialPoolCount, poolNamePrefix, poolSelectorLabelName)
		Expect(err).ToNot(HaveOccurred())

		kv, err := deploySpecificPools(ctx, client, pools)
		Expect(err).ToNot(HaveOccurred())
		testsuite.EnsureKubevirtReadyWithTimeout(kv, 420*time.Second)
		waitForPoolsBeReady(initialPoolCount)
		Eventually(func() error {
			return checkDaemonSetStatus(ctx, client, virtHandlerName, 1)
		}, 240*time.Second, 1*time.Second).Should(Succeed())

		By("running a VMI before adding new pool")
		vmi, bootID := runVMI(ctx, client)
		checkVMIOperational(ctx, client, vmi)

		By("finding the default handler node")
		defaultNodeName, err := findDefaultHandlerNode(ctx, client, virtHandlerName)
		Expect(err).ToNot(HaveOccurred())
		Expect(defaultNodeName).ToNot(BeEmpty(), "could not find a node running default virt-handler")

		By("adding a new pool targeting the default handler's node")
		newPoolName := fmt.Sprintf("%s-%d", poolNamePrefix, initialPoolCount)
		newPool := k6tv1.HandlerPoolConfig{
			Name: newPoolName,
			NodeSelector: map[string]string{
				poolSelectorLabelName: newPoolName,
			},
		}

		err = patchNodeLabelsWithCleanup(ctx, client, defaultNodeName, map[string]string{poolSelectorLabelName: newPoolName})
		Expect(err).ToNot(HaveOccurred())

		Expect(updateKvPools(ctx, client, func(kv *k6tv1.KubeVirt) {
			if kv.Spec.HandlerPools == nil {
				kv.Spec.HandlerPools = &k6tv1.HandlerPoolsConfig{}
			}
			kv.Spec.HandlerPools.Pools = append(kv.Spec.HandlerPools.Pools, newPool)
		})).To(Succeed())

		By("waiting for all pools including the new one to be ready")
		waitForPoolsBeReady(initialPoolCount + 1)
		Eventually(func() error {
			return checkDaemonSetStatus(ctx, client, virtHandlerName, 0)
		}, 240*time.Second, 1*time.Second).Should(Succeed())

		testsuite.EnsureKubevirtReadyWithTimeout(libkubevirt.GetCurrentKv(client), 420*time.Second)

		By("verifying VMI survived pool addition")
		checkVMIOperational(ctx, client, vmi)
		checkBootID(vmi, bootID)
	})

	It("should remove a single pool while preserving others", func() {
		nodes, err := getEligibleNodes(ctx, client, virtHandlerName)
		Expect(err).ToNot(HaveOccurred())
		if len(nodes) < 3 {
			Skip("test requires at least 3 nodes with virt-handler")
		}

		By("deploying pools for all nodes")
		pools, err := deployPools(ctx, client)
		Expect(err).ToNot(HaveOccurred())
		waitForPoolsBeReady(len(pools.Pools))
		Eventually(func() error {
			return checkDaemonSetStatus(ctx, client, virtHandlerName, 0)
		}, 240*time.Second, 1*time.Second).Should(Succeed())

		By("running a VMI")
		vmi, bootID := runVMI(ctx, client)
		checkVMIOperational(ctx, client, vmi)

		By("removing the first pool from configuration")
		removedPoolName := pools.Pools[0].Name
		removedPoolDSName := fmt.Sprintf("%s-%s", virtHandlerName, removedPoolName)

		Expect(updateKvPools(ctx, client, func(kv *k6tv1.KubeVirt) {
			kv.Spec.HandlerPools.Pools = slices.DeleteFunc(kv.Spec.HandlerPools.Pools, func(p k6tv1.HandlerPoolConfig) bool {
				return p.Name == removedPoolName
			})
		})).To(Succeed())

		By("waiting for removed pool's daemonset to be deleted")
		Eventually(func(g Gomega) bool {
			deleted, err := isDaemonSetDeleted(ctx, client, removedPoolDSName)
			g.Expect(err).ToNot(HaveOccurred())
			return deleted
		}, 240*time.Second, 1*time.Second).Should(BeTrue())

		By("verifying default handler picked up the freed node")
		Eventually(func() error {
			return checkDaemonSetStatus(ctx, client, virtHandlerName, 1)
		}, 240*time.Second, 1*time.Second).Should(Succeed())

		By("verifying remaining pools are still operational")
		waitForPoolsBeReady(len(pools.Pools) - 1)
		testsuite.EnsureKubevirtReadyWithTimeout(libkubevirt.GetCurrentKv(client), 420*time.Second)

		By("verifying VMI survived pool removal")
		checkVMIOperational(ctx, client, vmi)
		checkBootID(vmi, bootID)
	})

	It("should not expose any node to dual virt-handler coverage during pool removal", func() {
		var (
			violations []string
			monitorErr error
		)

		By("deploying pools for all nodes")
		pools, err := deployPools(ctx, client)
		Expect(err).ToNot(HaveOccurred())
		waitForPoolsBeReady(len(pools.Pools))
		Eventually(func() error {
			return checkDaemonSetStatus(ctx, client, virtHandlerName, 0)
		}, 240*time.Second, 1*time.Second).Should(Succeed())

		By("running a VMI")
		vmi, bootID := runVMI(ctx, client)
		checkVMIOperational(ctx, client, vmi)

		By("monitoring for dual virt-handler coverage while removing pools")
		monitorCtx, cancelMonitor := context.WithCancel(ctx)
		monitorDone := make(chan struct{})
		go func() {
			defer close(monitorDone)
			defer GinkgoRecover()
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-monitorCtx.Done():
					return
				case <-ticker.C:
					node, err := findNodeWithDualHandlerCoverage(monitorCtx, client, virtHandlerName)
					if err != nil {
						if monitorCtx.Err() != nil {
							return
						}
						monitorErr = fmt.Errorf("monitoring virt-handler coverage: %w", err)
						return
					}
					if node == "" {
						continue
					}
					violations = append(violations, node)
				}
			}
		}()
		stopMonitoring := func() {
			cancelMonitor()
			<-monitorDone
		}
		DeferCleanup(stopMonitoring)

		By("triggering pool removal")
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			kv := libkubevirt.GetCurrentKv(client).DeepCopy()
			kv.Spec = *originalSpec.DeepCopy()
			_, err := client.KubeVirt(kv.Namespace).Update(ctx, kv, metav1.UpdateOptions{})
			return err
		})
		Expect(err).ToNot(HaveOccurred())

		By("waiting for all pool daemonsets to be deleted")
		waitForPoolsBeRemoved(ctx, client)

		stopMonitoring()

		testsuite.EnsureKubevirtReadyWithTimeout(libkubevirt.GetCurrentKv(client), 420*time.Second)

		By("asserting no dual coverage was observed on any node")
		Expect(monitorErr).ToNot(HaveOccurred())
		Expect(violations).To(BeEmpty(),
			"nodes with dual virt-handler coverage during pool removal: %v", violations)

		By("verifying VMI survived pool removal without restart")
		checkVMIOperational(ctx, client, vmi)
		checkBootID(vmi, bootID)
	})

	It("should handle node re-labeling between pools", func() {
		nodes, err := getEligibleNodes(ctx, client, virtHandlerName)
		Expect(err).ToNot(HaveOccurred())
		if len(nodes) < 3 {
			Skip("test requires at least 3 nodes with virt-handler")
		}

		By("deploying pools leaving one node for default handler")
		poolsCount := len(nodes) - 1
		pools, err := generatePools(poolsCount, poolNamePrefix, poolSelectorLabelName)
		Expect(err).ToNot(HaveOccurred())

		kv, err := deploySpecificPools(ctx, client, pools)
		Expect(err).ToNot(HaveOccurred())
		testsuite.EnsureKubevirtReadyWithTimeout(kv, 420*time.Second)
		waitForPoolsBeReady(poolsCount)

		By("running a VMI")
		vmi, bootID := runVMI(ctx, client)
		checkVMIOperational(ctx, client, vmi)

		By("finding the node assigned to pool-0")
		pool0LabelValue := pools.Pools[0].NodeSelector[poolSelectorLabelName]
		pool1LabelValue := pools.Pools[1].NodeSelector[poolSelectorLabelName]

		pool0NodeName, err := findNodeWithLabel(ctx, client, poolSelectorLabelName, pool0LabelValue)
		Expect(err).ToNot(HaveOccurred())
		Expect(pool0NodeName).ToNot(BeEmpty(), "could not find node assigned to pool-0")

		By("re-labeling node from pool-0 to pool-1")
		err = patchNodeLabelsWithCleanup(ctx, client, pool0NodeName, map[string]string{poolSelectorLabelName: pool1LabelValue})
		Expect(err).ToNot(HaveOccurred())

		pool0DSName := fmt.Sprintf("%s-%s", virtHandlerName, pools.Pools[0].Name)
		pool1DSName := fmt.Sprintf("%s-%s", virtHandlerName, pools.Pools[1].Name)

		By("waiting for pool-0 to scale down and pool-1 to scale up")
		Eventually(func() error {
			return checkDaemonSetStatus(ctx, client, pool0DSName, 0)
		}, 420*time.Second, 1*time.Second).Should(Succeed())
		By("waiting for the old pool-0 pod to leave the re-labeled node")
		Eventually(func() (bool, error) {
			return isDaemonSetPodRunningOnNode(ctx, client, pool0NodeName, pool0DSName)
		}, 420*time.Second, 1*time.Second).Should(BeFalse())
		Eventually(func() error {
			return checkDaemonSetStatus(ctx, client, pool1DSName, 2)
		}, 420*time.Second, 1*time.Second).Should(Succeed())

		testsuite.EnsureKubevirtReadyWithTimeout(libkubevirt.GetCurrentKv(client), 420*time.Second)

		By("verifying VMI survived re-labeling")
		checkVMIOperational(ctx, client, vmi)
		checkBootID(vmi, bootID)
	})

	It("should update a pool's image while VMIs are running", func() {
		if flags.KubeVirtVersionTagAlt == "" {
			Skip("alt tag is not specified, but required for this test")
		}

		By("deploying pools")
		pools, err := deployPools(ctx, client)
		Expect(err).ToNot(HaveOccurred())
		waitForPoolsBeReady(len(pools.Pools))

		By("running a VMI")
		vmi, bootID := runVMI(ctx, client)
		checkVMIOperational(ctx, client, vmi)

		By("getting current virt-handler image from pool-0")
		targetPoolDSName := fmt.Sprintf("%s-%s", virtHandlerName, pools.Pools[0].Name)
		ds, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, targetPoolDSName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())

		virtHandlerContainer := findContainerByName(ds.Spec.Template.Spec.Containers, virtHandlerName)
		Expect(virtHandlerContainer).ToNot(BeNil())
		originalImage := virtHandlerContainer.Image
		altImage := replaceImageTag(originalImage, flags.KubeVirtVersionTagAlt)

		By("updating pool-0 with custom image")
		Expect(updateKvPools(ctx, client, func(kv *k6tv1.KubeVirt) {
			for i := range kv.Spec.HandlerPools.Pools {
				if kv.Spec.HandlerPools.Pools[i].Name == pools.Pools[0].Name {
					kv.Spec.HandlerPools.Pools[i].VirtHandlerImage = altImage
					break
				}
			}
		})).To(Succeed())

		By("waiting for pool-0 DS to roll out with new image")
		Eventually(func(g Gomega) {
			ds, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, targetPoolDSName, metav1.GetOptions{})
			g.Expect(err).ToNot(HaveOccurred())
			container := findContainerByName(ds.Spec.Template.Spec.Containers, virtHandlerName)
			g.Expect(container).ToNot(BeNil())
			g.Expect(container.Image).To(Equal(altImage))
			g.Expect(ds.Status.DesiredNumberScheduled).To(Equal(ds.Status.NumberReady))
			g.Expect(ds.Status.DesiredNumberScheduled).To(BeNumerically(">", 0))
		}, 240*time.Second, 1*time.Second).Should(Succeed(), "waiting for pool-0 DS to update image")

		testsuite.EnsureKubevirtReadyWithTimeout(libkubevirt.GetCurrentKv(client), 420*time.Second)

		By("verifying other pool DSes still use default image")
		for _, pool := range pools.Pools[1:] {
			poolDSName := fmt.Sprintf("%s-%s", virtHandlerName, pool.Name)
			ds, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, poolDSName, metav1.GetOptions{})
			Expect(err).ToNot(HaveOccurred())
			container := findContainerByName(ds.Spec.Template.Spec.Containers, virtHandlerName)
			Expect(container).ToNot(BeNil())
			Expect(container.Image).To(Equal(originalImage))
		}

		By("verifying VMI survived image update")
		checkVMIOperational(ctx, client, vmi)
		checkBootID(vmi, bootID)
	})

	It("should update a pool's node selector", func() {
		nodes, err := getEligibleNodes(ctx, client, virtHandlerName)
		Expect(err).ToNot(HaveOccurred())
		if len(nodes) < 3 {
			Skip("test requires at least 3 nodes with virt-handler")
		}

		By("deploying pools")
		pools, err := deployPools(ctx, client)
		Expect(err).ToNot(HaveOccurred())
		waitForPoolsBeReady(len(pools.Pools))

		By("running a VMI")
		vmi, bootID := runVMI(ctx, client)
		checkVMIOperational(ctx, client, vmi)

		By("finding the node assigned to pool-0")
		pool0LabelValue := pools.Pools[0].NodeSelector[poolSelectorLabelName]
		pool0NodeName, err := findNodeWithLabel(ctx, client, poolSelectorLabelName, pool0LabelValue)
		Expect(err).ToNot(HaveOccurred())
		Expect(pool0NodeName).ToNot(BeEmpty(), "could not find node assigned to pool-0")

		By("re-labeling node with new selector value")
		const newSelectorValue = "pool-0-updated"
		err = patchNodeLabelsWithCleanup(ctx, client, pool0NodeName, map[string]string{poolSelectorLabelName: newSelectorValue})
		Expect(err).ToNot(HaveOccurred())

		By("updating pool-0's node selector in KubeVirt spec")
		Expect(updateKvPools(ctx, client, func(kv *k6tv1.KubeVirt) {
			for i := range kv.Spec.HandlerPools.Pools {
				if kv.Spec.HandlerPools.Pools[i].Name == pools.Pools[0].Name {
					kv.Spec.HandlerPools.Pools[i].NodeSelector = map[string]string{
						poolSelectorLabelName: newSelectorValue,
					}
					break
				}
			}
		})).To(Succeed())

		By("waiting for pool-0 DS to update with new selector and become ready")
		pool0DSName := fmt.Sprintf("%s-%s", virtHandlerName, pools.Pools[0].Name)
		Eventually(func(g Gomega) {
			ds, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, pool0DSName, metav1.GetOptions{})
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(ds.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue(poolSelectorLabelName, newSelectorValue))
			g.Expect(ds.Status.DesiredNumberScheduled).To(BeNumerically(">", 0))
			g.Expect(ds.Status.DesiredNumberScheduled).To(Equal(ds.Status.NumberReady))
		}, 240*time.Second, 1*time.Second).Should(Succeed(), "waiting for pool-0 DS to update selector")

		testsuite.EnsureKubevirtReadyWithTimeout(libkubevirt.GetCurrentKv(client), 420*time.Second)

		By("verifying VMI survived selector change")
		checkVMIOperational(ctx, client, vmi)
		checkBootID(vmi, bootID)
	})

	It("should correctly place pools", func() {
		const (
			poolsCount       = 2
			groupLabel       = "group"
			groupValue       = "default"
			workloadsLabel   = "workloads-label"
			workloadsValue   = "enabled"
			defaultPoolValue = "unmatched"
		)

		nodes, err := getEligibleNodes(ctx, client, virtHandlerName)
		Expect(err).ToNot(HaveOccurred())
		if len(nodes) < 4 {
			Skip("test requires at least 4 nodes with virt-handler")
		}

		By("labeling the eligible nodes")
		newLabels := map[string]string{groupLabel: groupValue, poolSelectorLabelName: defaultPoolValue}
		for _, node := range nodes {
			err = patchNodeLabelsWithCleanup(ctx, client, node.Name, newLabels)
			Expect(err).ToNot(HaveOccurred())
		}

		By("deploying pools")
		maxPoolsCount, err := getMaxPossiblePoolsCount(ctx, client, virtHandlerName)
		Expect(err).ToNot(HaveOccurred())
		Expect(maxPoolsCount).To(BeNumerically(">", poolsCount))

		pools, err := generatePools(poolsCount, poolNamePrefix, poolSelectorLabelName)
		Expect(err).ToNot(HaveOccurred())

		pools.PartitionKeys = append(pools.PartitionKeys, groupLabel)

		for _, pool := range pools.Pools {
			pool.NodeSelector[groupLabel] = groupValue
		}

		kv, virtHandlers := deploySpecificPoolsWithExpectations(ctx, client, pools, poolsCount+1)
		for _, ds := range virtHandlers {
			if ds.Name == virtHandlerName {
				Expect(len(nodes) - poolsCount).To(Equal(int(ds.Status.NumberReady)))
			} else {
				Expect(int(ds.Status.NumberReady)).To(Equal(1))
			}
		}

		By("checking correct placement")
		clusterNodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())

		seenVirtHandlers, poolHandlers := checkCorrectPlacement(ctx, client, clusterNodes.Items, pools)
		Expect(seenVirtHandlers).To(Equal(len(nodes)))
		Expect(poolHandlers).To(Equal(poolsCount))

		By("checking incorrect workload node selectors being rejected")
		kv, err = client.KubeVirt(flags.KubeVirtInstallNamespace).Get(ctx, kv.Name, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(kv.Spec.Workloads).To(BeNil(), "kv.Spec.Workloads shouldn't be specified")

		kv.Spec.Workloads = &k6tv1.ComponentConfig{
			NodePlacement: &k6tv1.NodePlacement{
				NodeSelector: map[string]string{
					groupLabel: "bad-value",
				},
			},
		}
		kv, err = client.KubeVirt(flags.KubeVirtInstallNamespace).Update(ctx, kv, metav1.UpdateOptions{})
		Expect(err).To(HaveOccurred())
		Expect(k8serrors.IsInvalid(err)).To(BeTrue(), fmt.Sprintf("unexpected error was returned: %+v", err))

		statusError, ok := errors.AsType[*k8serrors.StatusError](err)
		Expect(ok).To(BeTrue())
		foundNodeSelectorInCauses := false
		for _, cause := range statusError.Status().Details.Causes {
			if cause.Field == "spec.handlerPools.pools.nodeSelector" {
				foundNodeSelectorInCauses = true
				break
			}
		}
		Expect(foundNodeSelectorInCauses).To(BeTrue())

		By("checking correct workload affinity being applied")
		clusterNodes, err = client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())

		excludedNodeName := ""
		for _, node := range clusterNodes.Items {
			// Skip labeling one default node
			if val, found := node.Labels[poolSelectorLabelName]; found && val == defaultPoolValue && excludedNodeName == "" {
				excludedNodeName = node.Name
				continue
			}

			err = patchNodeLabelsWithCleanup(ctx, client, node.Name, map[string]string{workloadsLabel: workloadsValue})
			Expect(err).ToNot(HaveOccurred())
		}
		Expect(excludedNodeName).ToNot(BeEmpty())

		kv = libkubevirt.GetCurrentKv(client)
		Expect(kv.Spec.Workloads).To(BeNil(), "kv.Spec.Workloads shouldn't be specified")

		kv.Spec.Workloads = &k6tv1.ComponentConfig{
			NodePlacement: &k6tv1.NodePlacement{
				NodeSelector: map[string]string{
					groupLabel: groupValue,
				},
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{{
								MatchExpressions: []corev1.NodeSelectorRequirement{{
									Key:      workloadsLabel,
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{workloadsValue},
								}},
							}},
						},
					},
				},
			},
		}

		kv, err = client.KubeVirt(flags.KubeVirtInstallNamespace).Update(ctx, kv, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		testsuite.EnsureKubevirtReadyWithTimeout(kv, 420*time.Second)

		By("checking correct placement")
		clusterNodes, err = client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())

		seenVirtHandlers, poolHandlers = checkCorrectPlacement(ctx, client, clusterNodes.Items, pools)
		Expect(poolHandlers).To(Equal(poolsCount))
		Expect(seenVirtHandlers).To(Equal(len(nodes) - 1))
	})
})

func generatePools(
	poolsCount int,
	poolNamePrefix string,
	poolSelectorLabelName string,
) (*k6tv1.HandlerPoolsConfig, error) {
	handlerPoolsConfig := &k6tv1.HandlerPoolsConfig{
		PartitionKeys: []string{poolSelectorLabelName},
		Pools:         make([]k6tv1.HandlerPoolConfig, 0, poolsCount),
	}
	for i := range poolsCount {
		poolName := fmt.Sprintf("%s-%d", poolNamePrefix, i)
		handlerPoolsConfig.Pools = append(handlerPoolsConfig.Pools,
			k6tv1.HandlerPoolConfig{
				Name: poolName,
				NodeSelector: map[string]string{
					poolSelectorLabelName: poolName,
				},
			})
	}

	return handlerPoolsConfig, nil
}

func setNodesLabels(
	ctx context.Context,
	client kubecli.KubevirtClient,
	kv *k6tv1.KubeVirt,
	nodes []*corev1.Node,
) error {
	for i, pool := range kv.Spec.HandlerPools.Pools {
		if err := patchNodeLabelsWithCleanup(ctx, client, nodes[i].Name, pool.NodeSelector); err != nil {
			return err
		}
	}
	return nil
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

func findContainerByName(containers []corev1.Container, name string) *corev1.Container {
	for i, container := range containers {
		if container.Name != name {
			continue
		}
		return &containers[i]
	}
	return nil
}

func updateKvPools(ctx context.Context, client kubecli.KubevirtClient, updateFn func(kv *k6tv1.KubeVirt)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		kv := libkubevirt.GetCurrentKv(client).DeepCopy()
		updateFn(kv)
		_, err := client.KubeVirt(kv.Namespace).Update(ctx, kv, metav1.UpdateOptions{})
		return err
	})
}

func patchNodeLabels(ctx context.Context, client kubecli.KubevirtClient, nodeName string, labels map[string]string) error {
	labelValues := make(map[string]any, len(labels))
	for k, v := range labels {
		labelValues[k] = v
	}
	return patchNodeLabelValues(ctx, client, nodeName, labelValues)
}

func patchNodeLabelsWithCleanup(ctx context.Context, client kubecli.KubevirtClient, nodeName string, labels map[string]string) error {
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	originalLabelValues := make(map[string]any, len(labels))
	for key := range labels {
		if value, exists := node.Labels[key]; exists {
			originalLabelValues[key] = value
		} else {
			originalLabelValues[key] = nil
		}
	}

	if err := patchNodeLabels(ctx, client, nodeName, labels); err != nil {
		return err
	}

	DeferCleanup(func() {
		Expect(patchNodeLabelValues(ctx, client, nodeName, originalLabelValues)).To(Succeed())
	})
	return nil
}

func patchNodeLabelValues(ctx context.Context, client kubecli.KubevirtClient, nodeName string, labels map[string]any) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"labels": labels,
		},
	})
	if err != nil {
		return err
	}

	_, err = client.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

func checkDaemonSetStatus(ctx context.Context, client kubecli.KubevirtClient, dsName string, expectedDesired int32) error {
	ds, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, dsName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if ds.Status.DesiredNumberScheduled != expectedDesired {
		return fmt.Errorf("daemonset %s: expected desired=%d, got desired=%d", dsName, expectedDesired, ds.Status.DesiredNumberScheduled)
	}
	if ds.Status.NumberReady != expectedDesired {
		return fmt.Errorf("daemonset %s: expected ready=%d, got ready=%d", dsName, expectedDesired, ds.Status.NumberReady)
	}
	return nil
}

func isDaemonSetDeleted(ctx context.Context, client kubecli.KubevirtClient, dsName string) (bool, error) {
	_, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, dsName, metav1.GetOptions{})
	if err == nil {
		return false, nil
	}
	if k8serrors.IsNotFound(err) {
		return true, nil
	}
	return false, err
}

func isDaemonSetPodRunningOnNode(ctx context.Context, client kubecli.KubevirtClient, nodeName, dsName string) (bool, error) {
	pods, err := listRunningPodsOnNode(ctx, client, nodeName)
	if err != nil {
		return false, err
	}

	for _, pod := range pods {
		for _, ref := range pod.OwnerReferences {
			if ref.Kind == "DaemonSet" && ref.Name == dsName {
				return true, nil
			}
		}
	}

	return false, nil
}

func findDefaultHandlerNode(ctx context.Context, client kubecli.KubevirtClient, virtHandlerName string) (string, error) {
	nodesList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	for _, node := range nodesList.Items {
		pods, err := listRunningPodsOnNode(ctx, client, node.Name)
		if err != nil {
			return "", err
		}
		for _, pod := range pods {
			for _, ref := range pod.OwnerReferences {
				if ref.Kind == "DaemonSet" && ref.Name == virtHandlerName {
					return node.Name, nil
				}
			}
		}
	}
	return "", nil
}

func findNodeWithLabel(ctx context.Context, client kubecli.KubevirtClient, labelKey, labelValue string) (string, error) {
	nodesList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	for _, node := range nodesList.Items {
		if node.Labels[labelKey] == labelValue {
			return node.Name, nil
		}
	}
	return "", nil
}

// findNodeWithDualHandlerCoverage returns the name of the first node that has
// running pods from two or more virt-handler DaemonSets (e.g. the default DS
// and a pool DS running simultaneously). An empty string means no such node exists.
func findNodeWithDualHandlerCoverage(ctx context.Context, client kubecli.KubevirtClient, virtHandlerName string) (string, error) {
	nodesList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	for _, node := range nodesList.Items {
		pods, err := listRunningPodsOnNode(ctx, client, node.Name)
		if err != nil {
			return "", err
		}

		owningDSes := map[string]struct{}{}
		for _, pod := range pods {
			for _, ref := range pod.OwnerReferences {
				if ref.Kind == "DaemonSet" && strings.HasPrefix(ref.Name, virtHandlerName) {
					owningDSes[ref.Name] = struct{}{}
				}
			}
		}

		if len(owningDSes) > 1 {
			return node.Name, nil
		}
	}
	return "", nil
}

func replaceImageTag(image string, newTag string) string {
	imageParts := strings.Split(image, "/")
	partsCount := len(imageParts)

	nameAndTag := imageParts[partsCount-1]
	registryParts := imageParts[:partsCount-1]

	// Strip digest if present (format: name:tag@sha256:abc)
	digestParts := strings.Split(nameAndTag, "@")
	nameAndTag = digestParts[0]

	// Replace or add tag
	tagParts := strings.Split(nameAndTag, ":")
	tagParts = tagParts[:1]
	tagParts = append(tagParts, newTag)

	nameAndTag = strings.Join(tagParts, ":")
	registryParts = append(registryParts, nameAndTag)

	return strings.Join(registryParts, "/")
}
