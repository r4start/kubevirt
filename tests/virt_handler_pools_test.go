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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	k6tv1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/kubevirt/pkg/virt-config/featuregate"
	kvconfig "kubevirt.io/kubevirt/tests/libkubevirt/config"

	"kubevirt.io/kubevirt/tests/decorators"
	"kubevirt.io/kubevirt/tests/flags"
	"kubevirt.io/kubevirt/tests/framework/kubevirt"
	"kubevirt.io/kubevirt/tests/libkubevirt"
)

var _ = Describe("[sig-operator] virt-handler pools", Serial, decorators.SigOperator, func() {
	const (
		poolSelectorLabelName = "handler-pool"
		virtHandlerName       = "virt-handler"
	)

	var (
		originalKV *k6tv1.KubeVirt
		client     kubecli.KubevirtClient
		ctx        context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		client = kubevirt.Client()
		originalKV = libkubevirt.GetCurrentKv(client)

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
		_, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, virtHandlerName, metav1.GetOptions{})
		if err == nil {
			// We have skipped the test. Do nothing.
			return
		}

		_, err = client.KubeVirt(flags.KubeVirtInstallNamespace).Update(ctx, originalKV, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		patch := []byte(fmt.Sprintf(`[{"op": "remove", "path":"/metadata/labels/%s"}]`, poolSelectorLabelName))
		nodesList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())
		for _, node := range nodesList.Items {
			_, err = client.CoreV1().Nodes().Patch(ctx, node.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
			Expect(err).ToNot(HaveOccurred())
		}

		Eventually(func(g Gomega) {
			ds, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, virtHandlerName, metav1.GetOptions{})
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(ds.Status.DesiredNumberScheduled).To(Equal(ds.Status.NumberReady))
			g.Expect(ds.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable.IntValue()).To(Equal(1))
		}, 240*time.Second, 1*time.Second).Should(Succeed(), "waiting for virt-handler to be ready")
	})

	deployPools := func(ctx context.Context, client kubecli.KubevirtClient, kb *k6tv1.KubeVirt) error {
		nodesList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
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
				return err
			}

			for _, pod := range podList.Items {
				if strings.HasPrefix(pod.Name, "virt-handler") {
					nodes = append(nodes, &node)
				}
			}
		}

		if len(nodes) == 0 {
			return errors.New("the cluster doesn't have nodes with running virt-handler pods")
		}

		if len(nodes) < len(kb.Spec.HandlerPools) {
			return fmt.Errorf("not enough nodes with running virt-handler for the test; have %d; required %d",
				len(nodesList.Items), len(kb.Spec.HandlerPools))
		}

		for i, pool := range kb.Spec.HandlerPools {
			n := nodes[i].DeepCopy()
			if n.Labels == nil {
				n.Labels = make(map[string]string)
			}

			n.Labels[poolSelectorLabelName] = pool.Name
			n, err = client.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{})
			if err != nil {
				return err
			}

			n, err := client.CoreV1().Nodes().Get(ctx, n.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}

			if value, found := n.Labels[poolSelectorLabelName]; !found || value != pool.Name {
				return fmt.Errorf("failed to update %s labels", n.Name)
			}

			nodes[i] = n
		}

		_, err = client.KubeVirt(flags.KubeVirtInstallNamespace).Update(ctx, kb, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		return nil
	}

	It("should successfully deploy several virt-handler pools", decorators.RequiresTwoSchedulableNodes, func() {
		ds, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(ctx, virtHandlerName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())

		poolsKv := originalKV.DeepCopy()
		poolsKv.Spec.HandlerPools = make([]k6tv1.HandlerPoolConfig, 0, ds.Status.NumberReady)

		for i := range ds.Status.NumberReady {
			poolName := fmt.Sprintf("pool-%d", i)
			poolsKv.Spec.HandlerPools = append(poolsKv.Spec.HandlerPools,
				k6tv1.HandlerPoolConfig{
					Name: poolName,
					NodeSelector: map[string]string{
						poolSelectorLabelName: poolName,
					},
				})
		}

		err = deployPools(ctx, client, poolsKv)
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			daemonSets, err := client.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).List(ctx, metav1.ListOptions{})
			g.Expect(err).ToNot(HaveOccurred())

			poolsCount := 0
			prefix := fmt.Sprintf("%s-%s", virtHandlerName, "pool")
			for _, ds := range daemonSets.Items {
				if !strings.HasPrefix(ds.Name, prefix) {
					continue
				}
				g.Expect(ds.Status.DesiredNumberScheduled).To(Equal(ds.Status.NumberReady))
				poolsCount += 1
			}

			g.Expect(poolsCount).To(BeNumerically("==", len(poolsKv.Spec.HandlerPools)))
		}, 240*time.Second, 1*time.Second).Should(Succeed(), "waiting for virt-handler pools to be ready")
	})
})
