package node

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	appv1 "k8s.io/api/apps/v1"
	k8sv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	framework "k8s.io/client-go/tools/cache/testing"
	"k8s.io/client-go/tools/record"

	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	kubevirtfake "kubevirt.io/client-go/kubevirt/fake"
	"kubevirt.io/client-go/log"
	"kubevirt.io/client-go/testing"

	controllertesting "kubevirt.io/kubevirt/pkg/controller/testing"
	"kubevirt.io/kubevirt/pkg/testutils"
	"kubevirt.io/kubevirt/pkg/virt-config/featuregate"
	watchtesting "kubevirt.io/kubevirt/pkg/virt-controller/watch/testing"
)

var _ = Describe("Node controller with", func() {

	var ctrl *gomock.Controller
	var fakeVirtClient *kubevirtfake.Clientset
	var nodeSource *framework.FakeControllerSource
	var nodeInformer cache.SharedIndexInformer
	var vmiSource *framework.FakeControllerSource
	var vmiInformer cache.SharedIndexInformer
	var stop chan struct{}
	var controller *Controller
	var recorder *record.FakeRecorder
	var mockQueue *testutils.MockWorkQueue[string]
	var virtClient *kubecli.MockKubevirtClient
	var kubeClient *fake.Clientset
	var vmiFeeder *testutils.VirtualMachineFeeder[string]
	var kubeVirtStore cache.Store

	syncCaches := func(stop chan struct{}) {
		go nodeInformer.Run(stop)
		go vmiInformer.Run(stop)
		Expect(cache.WaitForCacheSync(stop, nodeInformer.HasSynced, vmiInformer.HasSynced)).To(BeTrue())
	}

	BeforeEach(func() {
		stop = make(chan struct{})
		ctrl = gomock.NewController(GinkgoT())
		virtClient = kubecli.NewMockKubevirtClient(ctrl)
		fakeVirtClient = kubevirtfake.NewSimpleClientset()

		nodeInformer, nodeSource = testutils.NewFakeInformerFor(&k8sv1.Node{})
		vmiInformer, vmiSource = testutils.NewFakeInformerFor(&v1.VirtualMachineInstance{})
		recorder = record.NewFakeRecorder(100)
		recorder.IncludeObject = true

		config, _, kvStore := testutils.NewFakeClusterConfigUsingKV(&v1.KubeVirt{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "kubevirt",
				Namespace: "kubevirt",
			},
			Spec: v1.KubeVirtSpec{
				Configuration: v1.KubeVirtConfiguration{
					DeveloperConfiguration: &v1.DeveloperConfiguration{
						FeatureGates: []string{featuregate.HandlerPoolsGate},
					},
				},
				HandlerPools: &v1.HandlerPoolsConfig{
					PartitionKeys: []string{"pool"},
					Pools: []v1.HandlerPoolConfig{{
						Name:         "test",
						NodeSelector: map[string]string{"pool": "test"},
					}},
				},
			},
		})
		kubeVirtStore = kvStore
		controller, _ = NewController(virtClient, nodeInformer, vmiInformer, recorder, config)
		// Wrap our workqueue to have a way to detect when we are done processing updates
		mockQueue = testutils.NewMockWorkQueue(controller.Queue)
		controller.Queue = mockQueue
		vmiFeeder = testutils.NewVirtualMachineFeeder(mockQueue, vmiSource)

		// Set up mock client
		virtClient.EXPECT().VirtualMachineInstance(metav1.NamespaceAll).Return(fakeVirtClient.KubevirtV1().VirtualMachineInstances(metav1.NamespaceAll)).AnyTimes()
		virtClient.EXPECT().VirtualMachineInstance(metav1.NamespaceDefault).Return(fakeVirtClient.KubevirtV1().VirtualMachineInstances(metav1.NamespaceDefault)).AnyTimes()
		kubeClient = fake.NewSimpleClientset()
		virtClient.EXPECT().CoreV1().Return(kubeClient.CoreV1()).AnyTimes()
		virtClient.EXPECT().AppsV1().Return(kubeClient.AppsV1()).AnyTimes()

		// Make sure that all unexpected calls to kubeClient will fail
		kubeClient.Fake.PrependReactor("*", "*", func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
			Expect(action).To(BeNil())
			return true, nil, nil
		})
		syncCaches(stop)
	})

	addNode := func(node *k8sv1.Node) {
		mockQueue.ExpectAdds(1)
		nodeSource.Add(node)
		mockQueue.Wait()
	}

	addVMI := func(vmi *v1.VirtualMachineInstance) {
		_, err := fakeVirtClient.KubevirtV1().VirtualMachineInstances(vmi.Namespace).Create(context.TODO(), vmi, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
	}

	modifyNode := func(node *k8sv1.Node) {
		mockQueue.ExpectAdds(1)
		nodeSource.Modify(node)
		mockQueue.Wait()
	}

	deleteNode := func(node *k8sv1.Node) {
		mockQueue.ExpectAdds(1)
		nodeSource.Delete(node)
		mockQueue.Wait()
	}

	expectVMIToFailedStatus := func(vmiName string) {
		updatedVMI, err := fakeVirtClient.KubevirtV1().VirtualMachineInstances(metav1.NamespaceDefault).Get(context.TODO(), vmiName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(updatedVMI.Status.Phase).To(Equal(v1.Failed))
		Expect(updatedVMI.Status.Reason).To(Equal(NodeUnresponsiveReason))
	}

	sanityExecute := func() {
		controllertesting.SanityExecute(controller, []cache.Store{
			controller.vmiStore, controller.nodeStore,
		}, Default)
	}

	Context("pods and vmis given", func() {
		It("should only select stuck vmis", func() {
			node := NewHealthyNode("test")
			vmiWithPod := watchtesting.NewRunningVirtualMachine("vmiWithPod", node)
			podForVMI := NewHealthyPodForVirtualMachine("podForVMI", vmiWithPod)
			vmiWithPodInDifferentNamespace := watchtesting.NewRunningVirtualMachine("vmiWithPodInDifferentNamespace", node)
			podInDifferentNamespace := NewHealthyPodForVirtualMachine("podInDifferentnamespace", vmiWithPodInDifferentNamespace)
			podInDifferentNamespace.Namespace = "wrong"
			vmiWithoutPod := watchtesting.NewRunningVirtualMachine("vmiWithoutPod", node)

			vmis := filterStuckVirtualMachinesWithoutPods([]*v1.VirtualMachineInstance{
				vmiWithPod,
				vmiWithPodInDifferentNamespace,
				vmiWithoutPod,
			}, []*k8sv1.Pod{
				podForVMI,
				podInDifferentNamespace,
			})

			By("filtering out vmis which have a pod")
			Expect(vmis).ToNot(ContainElement(vmiWithPod))

			By("keeping vmis which are running and have no pod in their namespace")
			Expect(vmis).To(ContainElement(vmiWithoutPod))
			Expect(vmis).To(ContainElement(vmiWithPodInDifferentNamespace))
		})
	})

	Context("responsive virt-handler given", func() {
		It("should do nothing", func() {
			node := NewHealthyNode("testnode")

			addNode(node)

			sanityExecute()
		})
	})

	Context("unresponsive virt-handler given", func() {
		It("should set the node to unschedulable", func() {
			node := NewHealthyNode("testnode")
			node.Annotations[v1.VirtHandlerHeartbeat] = nowAsJSONWithOffset(-10 * time.Minute)

			addNode(node)

			kubeClient.Fake.PrependReactor("patch", "nodes", func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
				patch, ok := action.(k8stesting.PatchAction)
				Expect(ok).To(BeTrue())
				Expect(string(patch.GetPatch())).To(Equal(`{"metadata": { "labels": {"kubevirt.io/schedulable": "false"}}}`))
				return true, nil, nil
			})

			sanityExecute()
			testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
		})
		DescribeTable("should set a vmi without a pod to failed state if the vmi is in ", func(phase v1.VirtualMachineInstancePhase) {
			node := NewUnhealthyNode("testnode")
			vmi := watchtesting.NewRunningVirtualMachine("vmi1", node)
			vmi.Status.Phase = phase
			addVMI(vmi)
			kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
				return true, &k8sv1.PodList{}, nil
			})

			Expect(controller.checkVirtLauncherPodsAndUpdateVMIStatus(node.Name, []*v1.VirtualMachineInstance{vmi}, log.DefaultLogger())).To(Succeed())
			testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
			expectVMIToFailedStatus(vmi.Name)
		},
			Entry("running state", v1.Running),
			Entry("scheduled state", v1.Scheduled),
		)
		It("should set multiple vmis to failed in one go, even if some updates fail", func() {
			node := NewUnhealthyNode("testnode")
			vmi := watchtesting.NewRunningVirtualMachine("vmi", node)
			addVMI(vmi)
			vmi1 := watchtesting.NewRunningVirtualMachine("vmi1", node)
			addVMI(vmi1)
			vmi2 := watchtesting.NewRunningVirtualMachine("vmi2", node)
			addVMI(vmi2)

			fakeVirtClient.Fake.PrependReactor("patch", "virtualmachineinstances", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
				patchAction := action.(k8stesting.PatchAction)
				if patchAction.GetName() == vmi1.Name {
					return true, &v1.VirtualMachineInstance{}, fmt.Errorf("some error")
				}
				return
			})

			Expect(controller.updateVMIWithFailedStatus([]*v1.VirtualMachineInstance{vmi, vmi1, vmi2}, log.DefaultLogger())).To(HaveOccurred())
			testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
			testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
			testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
			Expect(testing.FilterActions(&fakeVirtClient.Fake, "patch", "virtualmachineinstances")).To(HaveLen(3))
		})
		It("should set a vmi without a pod to failed state, triggered by vmi add event", func() {
			node := NewUnhealthyNode("testnode")
			vmi := watchtesting.NewRunningVirtualMachine("vmi1", node)
			addVMI(vmi)

			vmiFeeder.Add(vmi)
			kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
				a, _ := action.(k8stesting.ListAction)
				if strings.Contains(a.GetListRestrictions().Labels.String(), "virt-handler") {
					return true, &k8sv1.PodList{Items: []k8sv1.Pod{*NewVirtHandlerPod(node.Name)}}, nil
				}

				return true, &k8sv1.PodList{}, nil
			})

			sanityExecute()
			testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
			expectVMIToFailedStatus(vmi.Name)
		})
		It("should set a vmi without a pod containing all terminated containers in a failed state", func() {
			node := NewUnhealthyNode("testnode")
			vmi := watchtesting.NewRunningVirtualMachine("vmi1", node)
			addVMI(vmi)

			kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
				return true, &k8sv1.PodList{Items: []k8sv1.Pod{*NewUnhealthyStuckTerminatingPodForVirtualMachine("whatever", vmi)}}, nil
			})

			Expect(controller.checkVirtLauncherPodsAndUpdateVMIStatus("testnode", []*v1.VirtualMachineInstance{vmi}, log.DefaultLogger())).To(Succeed())
			testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
			expectVMIToFailedStatus(vmi.Name)
		})
		It("should set a vmi without a pod to failed state, triggered by node update", func() {
			node := NewUnhealthyNode("testnode")
			vmi := watchtesting.NewRunningVirtualMachine("vmi1", node)
			addVMI(vmi)

			Expect(nodeInformer.GetStore().Add(node)).To(Succeed())
			modifyNode(node.DeepCopy())
			kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
				a, _ := action.(k8stesting.ListAction)
				if strings.Contains(a.GetListRestrictions().Labels.String(), "virt-handler") {
					return true, &k8sv1.PodList{Items: []k8sv1.Pod{*NewVirtHandlerPod(node.Name)}}, nil
				}

				return true, &k8sv1.PodList{}, nil
			})

			sanityExecute()
			testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
			expectVMIToFailedStatus(vmi.Name)
		})
		It("should set a vmi without a pod to failed state, triggered by node delete", func() {
			node := NewUnhealthyNode("testnode")
			vmi := watchtesting.NewRunningVirtualMachine("vmi1", node)
			addVMI(vmi)

			Expect(nodeInformer.GetStore().Add(node)).To(Succeed())
			deleteNode(node.DeepCopy())
			kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
				a, _ := action.(k8stesting.ListAction)
				if strings.Contains(a.GetListRestrictions().Labels.String(), "virt-handler") {
					return true, &k8sv1.PodList{Items: []k8sv1.Pod{*NewVirtHandlerPod(node.Name)}}, nil
				}

				return true, &k8sv1.PodList{}, nil
			})

			sanityExecute()
			testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
			expectVMIToFailedStatus(vmi.Name)
		})
		It("should set a vmi without a pod to failed state, triggered by vmi modify event", func() {
			node := NewUnhealthyNode("testnode")
			vmi := watchtesting.NewRunningVirtualMachine("vmi1", node)
			addVMI(vmi)

			Expect(vmiInformer.GetStore().Add(vmi)).To(Succeed())
			vmiFeeder.Modify(vmi)
			kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
				a, _ := action.(k8stesting.ListAction)
				if strings.Contains(a.GetListRestrictions().Labels.String(), "virt-handler") {
					return true, &k8sv1.PodList{Items: []k8sv1.Pod{*NewVirtHandlerPod(node.Name)}}, nil
				}

				return true, &k8sv1.PodList{}, nil
			})

			sanityExecute()
			testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
			expectVMIToFailedStatus(vmi.Name)
		})
		It("should set a vmi with an unhealthy pod to failed state, triggered by vmi modify event", func() {
			node := NewUnhealthyNode("testnode")
			vmi := watchtesting.NewRunningVirtualMachine("vmi1", node)
			addVMI(vmi)

			Expect(vmiInformer.GetStore().Add(vmi)).To(Succeed())
			vmiFeeder.Modify(vmi)
			kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
				a, _ := action.(k8stesting.ListAction)
				if strings.Contains(a.GetListRestrictions().Labels.String(), "virt-handler") {
					return true, &k8sv1.PodList{Items: []k8sv1.Pod{*NewVirtHandlerPod(node.Name)}}, nil
				}

				return true, &k8sv1.PodList{Items: []k8sv1.Pod{*NewUnhealthyPodForVirtualMachine("whatever", vmi)}}, nil
			})

			sanityExecute()
			testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
			expectVMIToFailedStatus(vmi.Name)
		})

		DescribeTable("should ignore a vmi which still has a healthy pod in", func(phase v1.VirtualMachineInstancePhase) {
			node := NewUnhealthyNode("testnode")
			vmi := watchtesting.NewRunningVirtualMachine("vmi", node)
			vmi.Status.Phase = phase
			addVMI(vmi)
			vmi1 := watchtesting.NewRunningVirtualMachine("vmi1", node)
			vmi1.Status.Phase = phase
			addVMI(vmi1)

			kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
				return true, &k8sv1.PodList{Items: []k8sv1.Pod{*NewHealthyPodForVirtualMachine("whatever", vmi1)}}, nil
			})

			Expect(controller.checkVirtLauncherPodsAndUpdateVMIStatus(node.Name, []*v1.VirtualMachineInstance{vmi}, log.DefaultLogger())).To(Succeed())
			testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
			By("checking that only a vmi with a pod gets removed")
			expectVMIToFailedStatus(vmi.Name)
			updatedVMI, err := fakeVirtClient.KubevirtV1().VirtualMachineInstances(metav1.NamespaceDefault).Get(context.TODO(), vmi1.Name, metav1.GetOptions{})
			Expect(err).ToNot(HaveOccurred())
			Expect(updatedVMI.Status.Phase).To(Equal(phase))
		},
			Entry("running state", v1.Running),
			Entry("scheduled state", v1.Scheduled),
		)
	})

	Context("check for orphaned vmis", func() {

		var node *k8sv1.Node
		var vmi *v1.VirtualMachineInstance

		BeforeEach(func() {
			node = NewHealthyNode("testnode")
			vmi = watchtesting.NewRunningVirtualMachine("vmi", node)
		})

		DescribeTable("should account for automatic DaemonSet tolerations when the handler pod is missing",
			func(key string, effect k8sv1.TaintEffect, hostNetwork bool, eligible bool) {
				node.Spec.Taints = []k8sv1.Taint{{Key: key, Effect: effect}}
				ds := HealthVirtHandlerDS()
				ds.Spec.Template.Spec.HostNetwork = hostNetwork
				original := ds.DeepCopy()

				kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, &k8sv1.PodList{}, nil
				})
				kubeClient.Fake.PrependReactor("list", "daemonsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, &appv1.DaemonSetList{Items: []appv1.DaemonSet{*ds}}, nil
				})

				err := controller.createEventIfNodeHasOrphanedVMIs(node, []*v1.VirtualMachineInstance{vmi})
				if eligible {
					Expect(err).ToNot(HaveOccurred())
					testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
				} else {
					Expect(err).To(MatchError(ContainSubstring("virt-handler DaemonSet isn't running on the node")))
				}
				Expect(ds).To(Equal(original))
			},
			Entry("not-ready NoExecute", k8sv1.TaintNodeNotReady, k8sv1.TaintEffectNoExecute, false, true),
			Entry("unreachable NoExecute", k8sv1.TaintNodeUnreachable, k8sv1.TaintEffectNoExecute, false, true),
			Entry("disk-pressure NoSchedule", k8sv1.TaintNodeDiskPressure, k8sv1.TaintEffectNoSchedule, false, true),
			Entry("memory-pressure NoSchedule", k8sv1.TaintNodeMemoryPressure, k8sv1.TaintEffectNoSchedule, false, true),
			Entry("pid-pressure NoSchedule", k8sv1.TaintNodePIDPressure, k8sv1.TaintEffectNoSchedule, false, true),
			Entry("unschedulable NoSchedule", k8sv1.TaintNodeUnschedulable, k8sv1.TaintEffectNoSchedule, false, true),
			Entry("network-unavailable with host networking", k8sv1.TaintNodeNetworkUnavailable, k8sv1.TaintEffectNoSchedule, true, true),
			Entry("network-unavailable without host networking", k8sv1.TaintNodeNetworkUnavailable, k8sv1.TaintEffectNoSchedule, false, false),
			Entry("network-unavailable NoExecute with host networking", k8sv1.TaintNodeNetworkUnavailable, k8sv1.TaintEffectNoExecute, true, false),
			Entry("not-ready NoSchedule", k8sv1.TaintNodeNotReady, k8sv1.TaintEffectNoSchedule, false, false),
			Entry("custom NoSchedule taint", "example.com/dedicated", k8sv1.TaintEffectNoSchedule, false, false),
			Entry("custom NoExecute taint", "example.com/dedicated", k8sv1.TaintEffectNoExecute, false, false),
			Entry("custom PreferNoSchedule taint", "example.com/dedicated", k8sv1.TaintEffectPreferNoSchedule, false, true),
		)

		DescribeTable("should reconcile a terminated launcher according to the handler pool configuration",
			func(gates []string, pools *v1.HandlerPoolsConfig, expectError bool) {
				kv := testutils.GetFakeKubeVirtClusterConfig(kubeVirtStore).DeepCopy()
				kv.Spec.Configuration.DeveloperConfiguration.FeatureGates = gates
				kv.Spec.HandlerPools = pools
				testutils.UpdateFakeKubeVirtClusterConfig(kubeVirtStore, kv)

				node.Annotations[v1.VirtHandlerHeartbeat] = nowAsJSONWithOffset(-10 * time.Minute)
				node.Labels[v1.NodeSchedulable] = "false"
				Expect(controller.nodeStore.Add(node)).To(Succeed())
				addVMI(vmi)

				ds := HealthVirtHandlerDS()
				ds.Spec.Template.Spec.NodeSelector = map[string]string{"pool": "test"}
				pod := NewUnhealthyPodForVirtualMachine("virt-launcher", vmi)
				pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(vmi, v1.VirtualMachineInstanceGroupVersionKind)}
				kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
					listAction := action.(k8stesting.ListAction)
					if strings.Contains(listAction.GetListRestrictions().Labels.String(), "virt-handler") {
						return true, &k8sv1.PodList{}, nil
					}
					return true, &k8sv1.PodList{Items: []k8sv1.Pod{*pod}}, nil
				})
				kubeClient.Fake.PrependReactor("list", "daemonsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, &appv1.DaemonSetList{Items: []appv1.DaemonSet{*ds}}, nil
				})

				err := controller.execute(node.Name)
				if expectError {
					Expect(err).To(MatchError(ContainSubstring("virt-handler DaemonSet isn't running on the node")))
					updatedVMI, err := fakeVirtClient.KubevirtV1().VirtualMachineInstances(vmi.Namespace).Get(context.Background(), vmi.Name, metav1.GetOptions{})
					Expect(err).ToNot(HaveOccurred())
					Expect(updatedVMI.Status.Phase).To(Equal(v1.Running))
					return
				}

				Expect(err).ToNot(HaveOccurred())
				Expect(recorder.Events).To(Receive(And(
					HavePrefix(k8sv1.EventTypeWarning+" "+NodeUnresponsiveReason+" "),
					ContainSubstring("virt-handler is not present, there are orphaned vmis on this node."),
				)))
				Expect(recorder.Events).To(Receive(And(
					HavePrefix(k8sv1.EventTypeNormal+" "+NodeUnresponsiveReason+" "),
					ContainSubstring(fmt.Sprintf("virt-handler on node %s is not responsive, marking VMI as failed", node.Name)),
				)))
				expectVMIToFailedStatus(vmi.Name)
			},
			Entry("with the feature gate disabled", []string{}, &v1.HandlerPoolsConfig{
				PartitionKeys: []string{"pool"},
				Pools:         []v1.HandlerPoolConfig{{Name: "test", NodeSelector: map[string]string{"pool": "test"}}},
			}, false),
			Entry("with the feature gate enabled and nil pools", []string{featuregate.HandlerPoolsGate}, (*v1.HandlerPoolsConfig)(nil), false),
			Entry("with the feature gate enabled and empty pools", []string{featuregate.HandlerPoolsGate}, &v1.HandlerPoolsConfig{}, false),
			Entry("with the feature gate enabled and configured pools", []string{featuregate.HandlerPoolsGate}, &v1.HandlerPoolsConfig{
				PartitionKeys: []string{"pool"},
				Pools:         []v1.HandlerPoolConfig{{Name: "test", NodeSelector: map[string]string{"pool": "test"}}},
			}, true),
		)

		DescribeTable("testing orphaned event", func(returnVirtHandler bool, ds *appv1.DaemonSet, hasrunningvmi bool, expectEvent bool) {

			kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
				if returnVirtHandler {
					return true, &k8sv1.PodList{Items: []k8sv1.Pod{*NewVirtHandlerPod(node.Name)}}, nil
				}

				return true, &k8sv1.PodList{}, nil
			})

			kubeClient.Fake.PrependReactor("list", "daemonsets", func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
				return true, &appv1.DaemonSetList{Items: []appv1.DaemonSet{*ds}}, nil
			})

			vmis := []*v1.VirtualMachineInstance{}
			if hasrunningvmi {
				vmis = []*v1.VirtualMachineInstance{vmi}
			}

			err := controller.createEventIfNodeHasOrphanedVMIs(node, vmis)
			Expect(err).ToNot(HaveOccurred())
			if expectEvent {
				testutils.ExpectEvent(recorder, NodeUnresponsiveReason)
			}
		},
			Entry("is not created when no vmis", true, &appv1.DaemonSet{}, false, false),
			Entry("is not created when virt-handler is running", true, &appv1.DaemonSet{}, true, false),
			Entry("is not created when daemonSet is not stable", false, UnHealthVirtHandlerDS(), true, false),
			Entry("is created when virt-handler is missing on node with vmis", false, HealthVirtHandlerDS(), true, true),
		)
	})

	Context("check daemonset status without pools", func() {
		DescribeTable("should require one daemonset and check its status",
			func(daemonsets []appv1.DaemonSet, expectedRunning bool, expectError bool) {
				kubeClient.Fake.PrependReactor("list", "daemonsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, &appv1.DaemonSetList{Items: daemonsets}, nil
				})

				running, err := checkDaemonSetStatus(virtClient, fields.ParseSelectorOrDie("kubevirt.io=virt-handler"))
				if expectError {
					Expect(err).To(MatchError("should only be running one virt-handler DaemonSet"))
				} else {
					Expect(err).ToNot(HaveOccurred())
				}
				Expect(running).To(Equal(expectedRunning))
			},
			Entry("with no daemonsets", []appv1.DaemonSet{}, false, true),
			Entry("with multiple daemonsets", []appv1.DaemonSet{*HealthVirtHandlerDS(), *HealthVirtHandlerDS()}, false, true),
			Entry("with one healthy daemonset", []appv1.DaemonSet{*HealthVirtHandlerDS()}, true, false),
			Entry("with one unhealthy daemonset", []appv1.DaemonSet{*UnHealthVirtHandlerDS()}, false, false),
		)
	})

	Context("check handler pools status", func() {
		var selector fields.Selector

		BeforeEach(func() {
			selector = fields.ParseSelectorOrDie("kubevirt.io=virt-handler")
		})

		It("should evaluate status using only the daemonset eligible for the node", func() {
			node := NewHealthyNode("testnode")
			node.Labels["pool"] = "a"

			eligible := HealthVirtHandlerDS()
			eligible.Spec.Template.Spec.NodeSelector = map[string]string{"pool": "a"}

			ineligible := UnHealthVirtHandlerDS()
			ineligible.Spec.Template.Spec.NodeSelector = map[string]string{"pool": "b"}

			kubeClient.Fake.PrependReactor("list", "daemonsets",
				func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
					return true, &appv1.DaemonSetList{Items: []appv1.DaemonSet{*eligible, *ineligible}}, nil
				},
			)

			running, err := checkHandlerPoolsStatus(virtClient, selector, node)
			Expect(err).ToNot(HaveOccurred())
			Expect(running).To(BeTrue())
		})

		It("should evaluate required node affinity when determining daemonset eligibility", func() {
			node := NewHealthyNode("testnode")
			node.Labels["pool"] = "a"

			eligible := HealthVirtHandlerDS()
			eligible.Spec.Template.Spec.NodeSelector = map[string]string{"pool": "a"}

			ineligible := UnHealthVirtHandlerDS()
			ineligible.Spec.Template.Spec.Affinity = &k8sv1.Affinity{
				NodeAffinity: &k8sv1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &k8sv1.NodeSelector{
						NodeSelectorTerms: []k8sv1.NodeSelectorTerm{
							{
								MatchExpressions: []k8sv1.NodeSelectorRequirement{
									{
										Key:      "pool",
										Operator: k8sv1.NodeSelectorOpNotIn,
										Values:   []string{"a"},
									},
								},
							},
						},
					},
				},
			}

			kubeClient.Fake.PrependReactor("list", "daemonsets",
				func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
					return true, &appv1.DaemonSetList{Items: []appv1.DaemonSet{*eligible, *ineligible}}, nil
				},
			)

			running, err := checkHandlerPoolsStatus(virtClient, selector, node)
			Expect(err).ToNot(HaveOccurred())
			Expect(running).To(BeTrue())
		})

		It("should return an error when multiple daemonsets are eligible on the node", func() {
			node := NewHealthyNode("testnode")
			node.Labels["pool"] = "a"

			first := HealthVirtHandlerDS()
			first.Spec.Template.Spec.NodeSelector = map[string]string{"pool": "a"}

			second := HealthVirtHandlerDS()
			second.Spec.Template.Spec.NodeSelector = map[string]string{"pool": "a"}

			kubeClient.Fake.PrependReactor("list", "daemonsets",
				func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
					return true, &appv1.DaemonSetList{Items: []appv1.DaemonSet{*first, *second}}, nil
				},
			)

			_, err := checkHandlerPoolsStatus(virtClient, selector, node)
			Expect(err).To(MatchError(ContainSubstring("several virt-handler DaemonSets are running on the node")))
		})

		It("should return an error when no daemonset is eligible on the node", func() {
			node := NewHealthyNode("testnode")
			node.Labels["pool"] = "a"

			first := HealthVirtHandlerDS()
			first.Spec.Template.Spec.NodeSelector = map[string]string{"pool": "b"}

			second := HealthVirtHandlerDS()
			second.Spec.Template.Spec.NodeSelector = map[string]string{"pool": "c"}

			kubeClient.Fake.PrependReactor("list", "daemonsets",
				func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
					return true, &appv1.DaemonSetList{Items: []appv1.DaemonSet{*first, *second}}, nil
				},
			)

			_, err := checkHandlerPoolsStatus(virtClient, selector, node)
			Expect(err).To(MatchError(ContainSubstring("virt-handler DaemonSet isn't running on the node")))
		})

		It("should consider NoSchedule taints and tolerations for eligibility", func() {
			node := NewHealthyNode("testnode")
			node.Labels["pool"] = "a"
			node.Spec.Taints = []k8sv1.Taint{
				{
					Key:    "dedicated",
					Value:  "infra",
					Effect: k8sv1.TaintEffectNoSchedule,
				},
			}

			untolerated := HealthVirtHandlerDS()
			untolerated.Spec.Template.Spec.NodeSelector = map[string]string{"pool": "b"}

			tolerated := HealthVirtHandlerDS()
			tolerated.Spec.Template.Spec.Tolerations = []k8sv1.Toleration{
				{
					Key:      "dedicated",
					Operator: k8sv1.TolerationOpEqual,
					Value:    "infra",
					Effect:   k8sv1.TaintEffectNoSchedule,
				},
			}

			kubeClient.Fake.PrependReactor("list", "daemonsets",
				func(action k8stesting.Action) (handled bool, obj runtime.Object, err error) {
					return true, &appv1.DaemonSetList{Items: []appv1.DaemonSet{*untolerated, *tolerated}}, nil
				},
			)

			running, err := checkHandlerPoolsStatus(virtClient, selector, node)
			Expect(err).ToNot(HaveOccurred())
			Expect(running).To(BeTrue())
		})
	})

	Context("daemonset pod tolerations", func() {
		DescribeTable("should replace finite eviction tolerations while preserving custom tolerations", func(key string) {
			seconds := int64(300)
			finite := k8sv1.Toleration{
				Key:               key,
				Operator:          k8sv1.TolerationOpExists,
				Effect:            k8sv1.TaintEffectNoExecute,
				TolerationSeconds: &seconds,
			}
			custom := k8sv1.Toleration{
				Key:      "example.com/dedicated",
				Operator: k8sv1.TolerationOpEqual,
				Value:    "infra",
				Effect:   k8sv1.TaintEffectNoSchedule,
			}
			spec := &k8sv1.PodSpec{Tolerations: []k8sv1.Toleration{custom, finite}}

			addOrUpdateDaemonPodTolerations(spec)

			Expect(spec.Tolerations).To(ContainElement(k8sv1.Toleration{
				Key:      key,
				Operator: k8sv1.TolerationOpExists,
				Effect:   k8sv1.TaintEffectNoExecute,
			}))
			Expect(spec.Tolerations).ToNot(ContainElement(finite))
			Expect(spec.Tolerations).To(ContainElement(custom))
			Expect(spec.Tolerations).To(HaveLen(7))

			By("leaving the pod spec unchanged when applying the defaults again")
			expected := spec.DeepCopy()
			addOrUpdateDaemonPodTolerations(spec)
			Expect(spec).To(Equal(expected))
		},
			Entry("not-ready", k8sv1.TaintNodeNotReady),
			Entry("unreachable", k8sv1.TaintNodeUnreachable),
		)

		It("should report no change when a toleration is already identical", func() {
			toleration := k8sv1.Toleration{
				Key:      k8sv1.TaintNodeNotReady,
				Operator: k8sv1.TolerationOpExists,
				Effect:   k8sv1.TaintEffectNoExecute,
			}
			spec := &k8sv1.PodSpec{Tolerations: []k8sv1.Toleration{toleration}}
			expected := spec.DeepCopy()

			Expect(addOrUpdateTolerationInPodSpec(spec, &toleration)).To(BeFalse())
			Expect(spec).To(Equal(expected))
		})
	})

	AfterEach(func() {
		close(stop)
		// Ensure that we add checks for expected events to every test
		Expect(recorder.Events).To(BeEmpty())
	})

})

func NewHealthyNode(nodeName string) *k8sv1.Node {
	return &k8sv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Annotations: map[string]string{
				v1.VirtHandlerHeartbeat: nowAsJSONWithOffset(0),
			},
			Labels: map[string]string{
				v1.NodeSchedulable: "true",
			},
		},
	}
}

func HealthVirtHandlerDS() *appv1.DaemonSet {
	ds := newVirtHanderDS()
	ds.Status = appv1.DaemonSetStatus{
		CurrentNumberScheduled: 1,
		DesiredNumberScheduled: 1,
		NumberReady:            1,
	}

	return ds
}

func UnHealthVirtHandlerDS() *appv1.DaemonSet {
	ds := newVirtHanderDS()
	ds.Status = appv1.DaemonSetStatus{
		CurrentNumberScheduled: 0,
		DesiredNumberScheduled: 1,
		NumberReady:            0,
	}

	return ds
}

func newVirtHanderDS() *appv1.DaemonSet {
	return &appv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "virt-handler",
			Labels: map[string]string{
				"kubevirt.io": "virt-handler",
			},
		},
	}
}

func NewUnhealthyNode(nodeName string) *k8sv1.Node {
	node := NewHealthyNode(nodeName)
	node.Annotations[v1.VirtHandlerHeartbeat] = nowAsJSONWithOffset(-10 * time.Minute)
	node.Labels[v1.NodeSchedulable] = "false"
	return node
}

func nowAsJSONWithOffset(offset time.Duration) string {
	now := metav1.Now()
	now = metav1.NewTime(now.Add(offset))

	data, err := json.Marshal(now)
	Expect(err).ToNot(HaveOccurred())
	return strings.Trim(string(data), `"`)
}

func NewVirtHandlerPod(nodeName string) *k8sv1.Pod {
	return &k8sv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "virt-handler",
			Labels: map[string]string{
				"kubevirt.io": "virt-handler",
			},
		},
		Spec: k8sv1.PodSpec{
			NodeName: nodeName,
		},
	}
}

func NewUnhealthyStuckTerminatingPodForVirtualMachine(podName string, vmi *v1.VirtualMachineInstance) *k8sv1.Pod {
	pod := NewHealthyPodForVirtualMachine(podName, vmi)
	pod.Status.Phase = k8sv1.PodPending
	pod.Status.ContainerStatuses = []k8sv1.ContainerStatus{
		{
			State: k8sv1.ContainerState{
				Terminated: &k8sv1.ContainerStateTerminated{},
			},
		},
	}
	return pod
}

func NewUnhealthyPodForVirtualMachine(podName string, vmi *v1.VirtualMachineInstance) *k8sv1.Pod {
	pod := NewHealthyPodForVirtualMachine(podName, vmi)
	pod.Status.Phase = k8sv1.PodFailed
	return pod
}

func NewHealthyPodForVirtualMachine(podName string, vmi *v1.VirtualMachineInstance) *k8sv1.Pod {
	return &k8sv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: k8sv1.NamespaceDefault,
			Labels: map[string]string{
				v1.CreatedByLabel: string(vmi.UID),
				v1.AppLabel:       "virt-launcher",
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(vmi, v1.VirtualMachineInstanceGroupVersionKind)},
		},
		Spec: k8sv1.PodSpec{NodeName: vmi.Status.NodeName},
		Status: k8sv1.PodStatus{
			Phase: k8sv1.PodRunning,
		},
	}
}
