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
	. "github.com/onsi/ginkgo/v2"

	"kubevirt.io/kubevirt/tests/decorators"
)

var _ = Describe("[sig-operator] virt-handler pools", Serial, decorators.SigOperator, func() {
	//var originalKV *v1.KubeVirt
	//var virtCli kubecli.KubevirtClient
	//var dsInformer cache.SharedIndexInformer
	//var stopCh chan struct{}
	//var updateTimeout time.Duration
	//
	//BeforeEach(func() {
	//	virtCli = kubevirt.Client()
	//
	//	originalKV = libkubevirt.GetCurrentKv(virtCli)
	//
	//	stopCh = make(chan struct{})
	//
	//	informerFactory := informers.NewSharedInformerFactoryWithOptions(
	//		virtCli,
	//		0,
	//		informers.WithNamespace(flags.KubeVirtInstallNamespace),
	//		informers.WithTweakListOptions(
	//			func(opts *metav1.ListOptions) {
	//				opts.LabelSelector = "kubevirt.io=virt-handler"
	//			},
	//		),
	//	)
	//
	//	dsInformer = informerFactory.Apps().V1().DaemonSets().Informer()
	//
	//	ds, err := virtCli.AppsV1().DaemonSets(flags.KubeVirtInstallNamespace).Get(context.Background(), "virt-handler", metav1.GetOptions{})
	//	Expect(err).ToNot(HaveOccurred())
	//	nodesToUpdate := ds.Status.DesiredNumberScheduled
	//	Expect(nodesToUpdate).To(BeNumerically(">", 0))
	//	updateTimeout = time.Duration(canaryTestNodeTimeout * nodesToUpdate)
	//})
	//
	//AfterEach(func() {
	//	close(stopCh)
	//	patchPayload, err := patch.New(patch.WithReplace("/spec/customizeComponents", originalKV.Spec.CustomizeComponents)).GeneratePayload()
	//	Expect(err).ToNot(HaveOccurred())
	//	_, err = virtCli.KubeVirt(flags.KubeVirtInstallNamespace).Patch(context.Background(), originalKV.Name, types.JSONPatchType, patchPayload, metav1.PatchOptions{})
	//	Expect(err).ToNot(HaveOccurred())
	//
	//	Eventually(func(g Gomega) {
	//		ds, err := virtCli.AppsV1().DaemonSets(originalKV.Namespace).Get(context.Background(), "virt-handler", metav1.GetOptions{})
	//		g.Expect(err).ToNot(HaveOccurred())
	//		g.Expect(ds.Status.DesiredNumberScheduled).To(Equal(ds.Status.NumberReady))
	//		g.Expect(ds.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable.IntValue()).To(Equal(1))
	//	}, updateTimeout*time.Second, 1*time.Second).Should(Succeed(), "waiting for virt-handler to be ready")
	//})

})
