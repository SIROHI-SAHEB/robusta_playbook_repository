import logging
from kubernetes import client, config
from hikaru.meta import KubernetesException
from hikaru.model.rel_1_26 import PersistentVolumeClaim
from robusta.api import *

@action
def my_action(event: PodEvent):
    # we have full access to the pod on which the alert fired
    pod = event.get_pod()
    pod_name = pod.metadata.name
    pod_logs = pod.get_logs()
    pod_processes = pod.exec("ps aux")

    # this is how you send data to slack or other destinations
    event.add_enrichment([
        MarkdownBlock("*Oh no!* An alert occurred on " + pod_name),
        FileBlock("crashing-pod.log", pod_logs)
    ])

class ResizePVCParams(ActionParams):
    """
    :var name: name of the PersistentVolumeClaim to resize
    :var namespace: namespace of the PersistentVolumeClaim to resize
    :example name: some_pvc_name
    """
    name: str
    namespace: str = "default"

@action
def resize_persistent_volume(event: ExecutionBaseEvent, params: ResizePVCParams):
    """
    Resize the PersistentVolumeClaim by increasing its size by 5%.
    """
    try:
        pvc = PersistentVolumeClaim().read(name=params.name, namespace=params.namespace)
    except KubernetesException:
        event.add_finding(
            Finding(
                title=f"Error resizing PersistentVolumeClaim {params.name}",
                aggregation_key="PVCResizeError",
                finding_type=FindingType.ISSUE,
                failure=True,
            )
        )
        event.add_enrichment(
            [
                MarkdownBlock(
                    f"Resize failed because PersistentVolumeClaim {params.name} in namespace {params.namespace} doesn't exist"
                )
            ]
        )
        return

    current_size = pvc.spec.resources.requests['storage']
    new_size = int(current_size.strip('Gi')) * 1.05
    new_size_str = f"{int(new_size)}Gi"

    pvc.spec.resources.requests['storage'] = new_size_str
    try:
        pvc.patch()
        logging.info(f"Resized PVC {params.name} in namespace {params.namespace} from {current_size} to {new_size_str}")
        event.add_finding(
            Finding(
                title=f"Resized PersistentVolumeClaim {params.name}",
                aggregation_key="PVCResizeSuccess",
                finding_type=FindingType.REPORT,
                failure=False,
            )
        )
        event.add_enrichment(
            [
                MarkdownBlock(
                    f"Resized PVC {params.name} in namespace {params.namespace} from {current_size} to {new_size_str}"
                )
            ]
        )
    except KubernetesException as e:
        logging.error(f"Failed to resize PVC {params.name}: {e}")
        event.add_finding(
            Finding(
                title=f"Error resizing PersistentVolumeClaim {params.name}",
                aggregation_key="PVCResizeError",
                finding_type=FindingType.ISSUE,
                failure=True,
            )
        )
        event.add_enrichment(
            [
                MarkdownBlock(
                    f"Resize failed for PersistentVolumeClaim {params.name} in namespace {params.namespace} due to {e}"
                )
            ]
        )
