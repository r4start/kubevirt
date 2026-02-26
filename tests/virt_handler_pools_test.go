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
	kvconfig "kubevirt.io/kubevirt/tests/libkubevirt/config"
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

	generatePools := func(ctx context.Context, client kubecli.KubevirtClient) ([]k6tv1.HandlerPoolConfig, error) {
		ds, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, virtHandlerName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		handlerPools := make([]k6tv1.HandlerPoolConfig, 0, ds.Status.NumberReady)
		for i := range ds.Status.NumberReady {
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
		var err error

		kb := libkubevirt.GetCurrentKv(client).DeepCopy()
		kb.Spec.HandlerPools, err = generatePools(ctx, client)
		if err != nil {
			return nil, err
		}

		nodesList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}

		// Get eligible nodes for virt-handlers
		nodes := make([]*corev1.Node, 0, len(nodesList.Items))
		for _, node := range nodesList.Items {
			podsSelector := fields.AndSelectors(
				fields.OneTermEqualSelector("spec.nodeName", node.Name),
				fields.OneTermEqualSelector("status.phase", string(corev1.PodRunning)),
			)
			podList, err := client.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{
				FieldSelector: podsSelector.String(),
			})
			if err != nil {
				return nil, err
			}

			for _, pod := range podList.Items {
				if strings.HasPrefix(pod.Name, "virt-handler") {
					nodes = append(nodes, &node)
				}
			}
		}

		if len(nodes) == 0 {
			return nil, errors.New("the cluster doesn't have nodes with running virt-handler pods")
		}

		if len(nodes) < len(kb.Spec.HandlerPools) {
			return nil, fmt.Errorf("not enough nodes with running virt-handler for the test; have %d; required %d",
				len(nodesList.Items), len(kb.Spec.HandlerPools))
		}

		for i, pool := range kb.Spec.HandlerPools {
			patch := []byte(fmt.Sprintf(`[{"op": "add", "path":"/metadata/labels/%s", "value": "%s"}]`, poolSelectorLabelName, pool.Name))
			_, err = client.CoreV1().Nodes().Patch(ctx, nodes[i].Name, types.JSONPatchType, patch, metav1.PatchOptions{})
			if err != nil {
				return nil, err
			}
		}

		_, err = client.KubeVirt(flags.KubeVirtInstallNamespace).Update(ctx, kb, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}

		return kb.Spec.HandlerPools, nil
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

		testsuite.EnsureKubevirtReadyWithTimeout(kv, 420*time.Second)
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

		kv := libkubevirt.GetCurrentKv(client)
		originalSpec = kv.Spec.DeepCopy()

		kvconfig.EnableFeatureGate(featuregate.HandlerPoolsGate)

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

		patch := []byte(fmt.Sprintf(`{"metadata":{"labels":{"%s":null}}}`, poolSelectorLabelName))
		nodesList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())

		for _, node := range nodesList.Items {
			_, err = client.CoreV1().Nodes().Patch(ctx, node.Name, types.MergePatchType, patch, metav1.PatchOptions{})
			Expect(err).ToNot(HaveOccurred())
		}

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
		pools, err := deployPools(ctx, client)
		Expect(err).ToNot(HaveOccurred())

		waitForPoolsBeReady(len(pools))

		Eventually(func(g Gomega) {
			_, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, virtHandlerName, metav1.GetOptions{})
			g.Expect(err).To(HaveOccurred())
			g.Expect(k8serrors.IsNotFound(err)).To(BeTrue(), fmt.Sprintf("expected daemonset %q to be deleted, got err=%v", virtHandlerName, err))
		}, 240*time.Second, 1*time.Second).Should(Succeed(), "waiting for virt-handler daemonset to be deleted")
	})

	It("should not break a running VMI", func() {
		vmi, bootIDBefore := runVMI(ctx, client)

		pools, err := deployPools(ctx, client)
		Expect(err).ToNot(HaveOccurred())

		waitForPoolsBeReady(len(pools))
		testsuite.EnsureKubevirtReadyWithTimeout(libkubevirt.GetCurrentKv(client), 420*time.Second)

		checkVMIOperational(ctx, client, vmi)
		checkBootID(vmi, bootIDBefore)
	})

	It("downgrade should not break a running VMI", func() {
		pools, err := deployPools(ctx, client)
		Expect(err).ToNot(HaveOccurred())

		waitForPoolsBeReady(len(pools))
		testsuite.EnsureKubevirtReadyWithTimeout(libkubevirt.GetCurrentKv(client), 420*time.Second)

		vmi, bootIDBefore := runVMI(ctx, client)

		removePools(ctx, client)

		checkVMIOperational(ctx, client, vmi)
		checkBootID(vmi, bootIDBefore)
	})
})
