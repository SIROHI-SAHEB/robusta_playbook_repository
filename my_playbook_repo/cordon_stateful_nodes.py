import logging
from kubernetes import client, config
from robusta.api import *

class CordonStatefulNodesParams(ActionParams):
    """
    :var node_name: name of the node to cordon
    :example node_name: some-node-name
    """
    node_name: Optional[str] = "ip-172-21-7-117.ap-south-1.compute.internal"

@action
def cordon_stateful_nodes(event: ExecutionBaseEvent, params: CordonStatefulNodesParams):
    """
    Cordon the node where the label node.paytm.com/group contains the value stateful.
    """
    logging.info(f"Received parameters: {params}")

    node_name = params.node_name

    if not node_name:
        logging.error("Node name is missing in the parameters.")
        raise ActionException(ErrorCodes.ACTION_VALIDATION_ERROR, "Node name is missing in the parameters.")

    logging.info(f"Node name to cordon: {node_name}")

    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

    v1 = client.CoreV1Api()

    # Get the node
    try:
        node = v1.read_node(name=node_name)
    except client.exceptions.ApiException as e:
        logging.error(f"Failed to read node {node_name}: {e}")
        raise ActionException(ErrorCodes.ACTION_UNEXPECTED_ERROR, f"Failed to read node {node_name} {e}")

    # Check if the node has the required label and is not already cordoned
    labels = node.metadata.labels
    if any("stateful" in value for value in labels.values()):
        if node.spec.unschedulable:
            event.add_enrichment([MarkdownBlock(f"Node {node.metadata.name} already cordoned")])
        else:
            try:
                v1.patch_node(node.metadata.name, {"spec": {"unschedulable": True}})
                event.add_enrichment([MarkdownBlock(f"Node {node.metadata.name} cordoned")])
                logging.info(f"Node {node.metadata.name} cordoned")
            except Exception as e:
                logging.error(f"Failed to cordon node {node.metadata.name}: {e}")
                raise ActionException(ErrorCodes.ACTION_UNEXPECTED_ERROR, f"Failed to cordon node {node.metadata.name} {e}")
    else:
        logging.info(f"Node {node.metadata.name} does not have the required label, skipping cordon action.")

    # Add findings and enrichment
    event.add_finding(
        Finding(
            title="Cordoned Stateful Node",
            aggregation_key="CordonStatefulNodes",
            finding_type=FindingType.REPORT,
            failure=False,
        )
    )
    event.add_enrichment(
        [
            MarkdownBlock(f"Node {node.metadata.name} cordoned")
        ]
    )