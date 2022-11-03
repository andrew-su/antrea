// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package e2e

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	antreaIDPSYML string = "idps.yml"

	antreaIDPSDaemonSet string = "antrea-idps-agent"

	idpsControllerContainerName = "antrea-idps-controller"
	idpsAgentContainerName      = "antrea-idps-agent"
	idpsSuricataContainerName   = "suricata"
)

// deployAntreaIDPS deploys Antrea IDPS.
func (data *TestData) deployAntreaIDPS() error {
	antreaIDPSYaml := antreaIDPSYML
	rc, _, _, err := data.provider.RunCommandOnNode(controlPlaneNodeName(), fmt.Sprintf("kubectl apply -f %s", antreaIDPSYaml))
	if err != nil || rc != 0 {
		return fmt.Errorf("error when deploying the Antrea IDPS; %s not available on the control-plane Node", antreaIDPSYaml)
	}

	log.Println("Waiting for all Antrea DaemonSet Pods")
	if err = data.waitForAntreaIDPSDaemonSetPods(defaultTimeout); err != nil {
		return err
	}

	log.Println("Waiting for Antrea IDPS controller Pod")
	idpsControllerPod, err := data.getAntreaIDPSController()
	if err != nil {
		return fmt.Errorf("error when getting Antrea IDPS controller Pod: %v", err)
	}
	if err = data.podWaitForReady(defaultTimeout, idpsControllerPod.Name, antreaNamespace); err != nil {
		return err
	}

	return nil
}

// waitForAntreaIDPSDaemonSetPods waits for the K8s apiserver to report that all the Antrea IDPS Pods are
// available, i.e. all the Nodes have one or more of the Antrea IDPS daemon Pod running and available.
func (data *TestData) waitForAntreaIDPSDaemonSetPods(timeout time.Duration) error {
	err := wait.Poll(defaultInterval, timeout, func() (bool, error) {
		getDS := func(dsName string, os string) (*appsv1.DaemonSet, error) {
			ds, err := data.clientset.AppsV1().DaemonSets(antreaNamespace).Get(context.TODO(), dsName, metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("error when getting Antrea IDPS %s daemonset: %v", os, err)
			}
			return ds, nil
		}
		var dsLinux *appsv1.DaemonSet
		var err error
		if dsLinux, err = getDS(antreaIDPSDaemonSet, "Linux"); err != nil {
			return false, err
		}
		currentNumAvailable := dsLinux.Status.NumberAvailable
		UpdatedNumberScheduled := dsLinux.Status.UpdatedNumberScheduled

		// Make sure that all Daemon Pods are available.
		// We use clusterInfo.numNodes instead of DesiredNumberScheduled because
		// DesiredNumberScheduled may not be updated right away. If it is still set to 0 the
		// first time we get the DaemonSet's Status, we would return immediately instead of
		// waiting.
		desiredNumber := int32(clusterInfo.numNodes - len(clusterInfo.windowsNodes))
		if currentNumAvailable != desiredNumber || UpdatedNumberScheduled != desiredNumber {
			return false, nil
		}

		// Make sure that all antrea-idps-agent Pods are not terminating. This is required because NumberAvailable of
		// DaemonSet counts Pods even if they are terminating. Deleting antrea-agent Pods directly does not cause the
		// number to decrease if the process doesn't quit immediately, e.g. when the signal is caught by bincover
		// program and triggers coverage calculation.
		pods, err := data.clientset.CoreV1().Pods(antreaNamespace).List(context.TODO(), metav1.ListOptions{
			LabelSelector: "app=antrea-idps,component=antrea-idps-agent",
		})
		if err != nil {
			return false, fmt.Errorf("failed to list antrea-idps-agent Pods: %v", err)
		}
		if len(pods.Items) != (clusterInfo.numNodes - len(clusterInfo.windowsNodes)) {
			return false, nil
		}
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp != nil {
				return false, nil
			}
		}
		return true, nil
	})
	if err == wait.ErrWaitTimeout {
		_, stdout, _, _ := data.provider.RunCommandOnNode(controlPlaneNodeName(), fmt.Sprintf("kubectl -n %s describe pod", antreaNamespace))
		return fmt.Errorf("antrea-idps-agent DaemonSet not ready within %v; kubectl describe pod output: %v", defaultTimeout, stdout)
	} else if err != nil {
		return err
	}

	return nil
}

// getAntreaIDPSAgentOnNode retrieves the name of the Antrea Pod (antrea-idps-agent-*) running on a specific Node.
func (data *TestData) getAntreaIDPSAgentOnNode(nodeName string) (*corev1.Pod, error) {
	listOptions := metav1.ListOptions{
		LabelSelector: "app=antrea-idps,component=antrea-idps-agent",
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	}
	pods, err := data.clientset.CoreV1().Pods(antreaNamespace).List(context.TODO(), listOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to list Antrea IDPS Pods: %v", err)
	}
	if len(pods.Items) != 1 {
		return nil, fmt.Errorf("expected *exactly* one Pod")
	}
	return &pods.Items[0], nil
}

// getAntreaIDPSController retrieves the name of the Antrea IDPS Controller (antrea-idps-controller-*) running in the k8s cluster.
func (data *TestData) getAntreaIDPSController() (*corev1.Pod, error) {
	listOptions := metav1.ListOptions{
		LabelSelector: "app=antrea-idps,component=antrea-idps-controller",
	}
	pods, err := data.clientset.CoreV1().Pods(antreaNamespace).List(context.TODO(), listOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to list Antrea IDPS Controller: %v", err)
	}
	if len(pods.Items) != 1 {
		return nil, fmt.Errorf("expected *exactly* one Pod")
	}
	return &pods.Items[0], nil
}
