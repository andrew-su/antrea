# Copyright (C) 2024 VMware, Inc. All rights reserved.
# -- VMware Confidential

from .k8s_client import K8sClient


def get_antrea_controller_pod():
    k8sclient = K8sClient()
    pods = k8sclient.list_namespaced_pod(namespace='kube-system', selector="component=antrea-controller")
    if len(pods.items) == 0:
        return None
    return pods.items[0]
