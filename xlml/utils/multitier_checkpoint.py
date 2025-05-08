from absl import logging
from kubernetes import client as k8s_client
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from xlml.utils import gke
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
from typing import Union, List
import re

def _get_core_api_client(
    project: str, region: str, cluster_name: str
) -> k8s_client.CoreV1Api:
  """Create a core API client for the given cluster."""
  client = gke.get_authenticated_client(project, region, cluster_name)

  # Initilize the client
  core_api = k8s_client.CoreV1Api(client)
  logging.info("Successful initilize k8s client from cluster response.")
  return core_api


def _list_workload_pods(
    core_api: k8s_client.CoreV1Api, workload_id: str
) -> k8s_client.V1PodList:
  """List all pods for the given workload."""
  logging.info(f"Getting pods for workload_id: {workload_id}")
  pods = core_api.list_namespaced_pod(
      label_selector=f"jobset.sigs.k8s.io/jobset-name={workload_id}",
      namespace="default",
  )
  return pods

def _list_multitier_pods(core_api: k8s_client.CoreV1Api) -> k8s_client.V1PodList:
  """
  List all high-scale-checkpointing pods in the 'gke-managed-checkpointing' namespace.

  This function retrieves all pods from the 'gke-managed-checkpointing' namespace that are labeled
  with 'k8s-app=high-scale-checkpointing' using the provided Kubernetes CoreV1Api client. It logs
  relevant information, including whether no pods were found or if multiple pods were returned.

  Returns:
      k8s_client.V1PodList: A list of pod objects that match the specified label. If no pods are
      found, the returned list will be empty.

  Note:
      Ensure that the supplied CoreV1Api client is correctly configured with the necessary context
      and permissions to access the Kubernetes cluster.
  """
  logging.info("Listing high-scale-checkpointing pods")
  pods = core_api.list_namespaced_pod(
    namespace="gke-managed-checkpointing",
    label_selector="k8s-app=high-scale-checkpointing"
  )
  if not pods.items:
    logging.info("No pods found in 'gke-managed-checkpointing' with label 'k8s-app=high-scale-checkpointing'")
    return pods
  if len(pods.items) > 1:
    logging.info("Got more than one pod")
  for i, pod in enumerate(pods.items):
    logging.info(f"Pod {i}: {pod.metadata.name}")
  return pods

def _execute_command_in_pod(
  core_api: k8s_client.CoreV1Api,
  pod: k8s_client.V1Pod,
  command: Union[str, List[str]],
  container: str = None,  # optional container name
) -> str:
  """Execute a command in the given pod using stream for an upgradeable connection."""
  logging.info(f"Executing command in pod {pod.metadata.name}: {command}")
  try:
    cmd = command if isinstance(command, list) else command.split()
    response = stream(
      core_api.connect_get_namespaced_pod_exec,
      name=pod.metadata.name,
      namespace=pod.metadata.namespace,
      command=cmd,
      container=container,
      stderr=True,
      stdin=False,
      stdout=True,
      tty=False,
    )
    return response
  except k8s_client.rest.ApiException as e:
    logging.error(f"Error executing command in pod {pod.metadata.name}: {e}")
    raise

@task
def prepare_verification_targets(
  project: str,
  region: str,
  cluster_name: str,
  workload_id: str,
  workload_namespace: str,
  driver_namespace: str,
  driver_label_selector: str,
) -> dict:
  """
  Prepare verification targets for RAM disk checkpoint verification.

  This task performs the following steps:
    1. Lists all workload pods for the specified workload ID in the given namespace.
    2. Selects the last training pod from the list.
    3. Finds the corresponding CSI driver pod on the same node as the selected training pod
     using the provided driver namespace and label selector.

  Args:
    project (str): GCP project ID.
    region (str): GCP region.
    cluster_name (str): Kubernetes cluster name.
    workload_id (str): Unique identifier for the training workload.
    workload_namespace (str): Namespace where the training workload pods are running.
    driver_namespace (str): Namespace where the CSI driver pods are running.
    driver_label_selector (str): Label selector to identify the CSI driver pods.

  Returns:
    dict: A dictionary with keys:
      - "target_pod": The selected training pod object.
      - "node_name": The node on which the training pod is scheduled.
      - "driver_pod": The CSI driver pod object found on the target node.

  Raises:
    AirflowException: If no workload pods are found, the target pod is not scheduled on a node,
            or if no CSI driver pod is found on the target node.
  """
  logging.info(f"Preparing verification targets for workload_id: '{workload_id}'")
  core_api = _get_core_api_client(project, region, cluster_name)

  # Step 1: List workload pods.
  logging.info(
    f"Listing pods for workload_id '{workload_id}' in namespace '{workload_namespace}' using selector 'jobset.sigs.k8s.io/jobset-name={workload_id}'"
  )
  workload_pods = _list_workload_pods(core_api, workload_id)
  if not workload_pods.items:
    logging.error(
      f"No pods found for workload_id '{workload_id}' in namespace '{workload_namespace}'."
    )
    raise AirflowException(f"No pods found for workload_id '{workload_id}'")

  # Step 2: Select the last training pod.
  target_training_pod = workload_pods.items[-1]
  target_pod_name = target_training_pod.metadata.name
  target_pod_namespace = target_training_pod.metadata.namespace
  node_name = target_training_pod.spec.node_name
  if not node_name:
    logging.error(
      f"Target pod '{target_pod_name}' is not yet scheduled on any node."
    )
    raise AirflowException("Target pod is not scheduled on a node.")
  logging.info(
    f"Selected training pod '{target_pod_name}' in namespace '{target_pod_namespace}' on node '{node_name}'."
  )

  # Step 3: Find the CSI driver pod on the same node.
  logging.info(
    f"Searching for CSI driver pod on node '{node_name}' in namespace '{driver_namespace}' using label '{driver_label_selector}'."
  )
  all_driver_pods = _list_multitier_pods(core_api)
  driver_pods_on_node = [pod for pod in all_driver_pods.items if pod.spec.node_name == node_name]
  if not driver_pods_on_node:
    logging.error(
      f"No CSI driver pod found on node '{node_name}' in namespace '{driver_namespace}'."
    )
    raise AirflowException(f"No CSI driver pod found on node '{node_name}'")
  if len(driver_pods_on_node) > 1:
    logging.warning(
      f"Multiple CSI driver pods found on node '{node_name}'. Using the first one: '{driver_pods_on_node[0].metadata.name}'."
    )
  driver_pod = driver_pods_on_node[0]
  logging.info(
    f"Identified CSI driver pod '{driver_pod.metadata.name}' in namespace '{driver_pod.metadata.namespace}' on node '{node_name}'."
  )

  return {
    "target_pod_name": target_training_pod.metadata.name,
    "target_pod_namespace": target_training_pod.metadata.namespace,
    "node_name": node_name,
    "driver_pod_name": driver_pod.metadata.name,
    "driver_pod_namespace": driver_pod.metadata.namespace,
  }

@task
def verify_checkpoint_files(
  verification_targets: dict,
  ramdisk_directory: str,
  driver_container_name: str,
  workload_id: str,
  project: str,
  region: str,
  cluster_name: str,
  expected_steps: List[int]
) -> bool:
  """
  Verify the existence of checkpoint files in the specified RAM disk directory and check
  for the expected file which should follow the pattern:
      workload_id-s<digits>-n<digits>-g<digits>.meta

  In addition, the digits following 's' must be present in the expected_steps list.

  This task performs the following steps:
    1. Defines and executes a command to list the contents of the RAM disk directory
       on the identified CSI driver pod.
    2. Parses the command output to determine if checkpoint files are present and if the
       expected file (with expected step number) is found.

  Args:
    verification_targets (dict): Dictionary returned by the preparation task containing the
                                 CSI driver pod.
    ramdisk_directory (str): The RAM disk directory path inside the CSI driver container.
    driver_container_name (str): Name of the container within the driver pod.
    workload_id (str): Workload identifier used as the file prefix.
    project (str): GCP project ID.
    region (str): GCP region.
    cluster_name (str): Kubernetes cluster name.
    expected_steps (List[int]): Expected step numbers for the 's' part of the filename.

  Returns:
    bool: True if checkpoint files and the expected file are detected;
          otherwise, raises an AirflowException.

  Raises:
    AirflowException: If no checkpoint files or the expected file (with expected s-digit) are detected.
  """
  # Rehydrate driver pod from name/namespace
  driver_pod_name = verification_targets["driver_pod_name"]
  driver_pod_namespace = verification_targets["driver_pod_namespace"]
  logging.info(
    f"Executing checkpoint verification command in CSI driver pod '{driver_pod_name}' using container '{driver_container_name}'."
  )
  core_api = _get_core_api_client(project, region, cluster_name)
  driver_pod = core_api.read_namespaced_pod(
    name=driver_pod_name, namespace=driver_pod_namespace
  )
  check_command = ['ls', '-lA', ramdisk_directory]
  command_output = _execute_command_in_pod(core_api, driver_pod, check_command, driver_container_name)

  logging.info("\n--- Check Command Output ---")
  print(command_output)
  logging.info("--- Output End ---")

  # Split the output into lines and filter out the header (e.g. "total ...").
  output_lines = command_output.strip().split('\n')
  file_lines = []
  for line in output_lines:
    if line and not line.startswith('total '):
      tokens = line.split()
      file_name = tokens[-1] if len(tokens) > 1 else line.strip()
      file_lines.append(file_name)

  if not file_lines:
    raise AirflowException(f"No checkpoint files detected in RAM disk directory '{ramdisk_directory}'")

  # Compile the expected pattern:
  # Expected pattern: "workload_id-s<digits>-n<digits>-g<digits>.meta"
  expected_pattern = re.compile(
    r"^" + re.escape(workload_id) + r"-s(\d+)-n\d+-g\d+\.meta$"
  )

  matched_files = []
  for fname in file_lines:
    match = expected_pattern.match(fname)
    if match:
      step_number = int(match.group(1))
      if step_number in expected_steps:
        matched_files.append(fname)

  if matched_files:
    logging.info(f"Expected file found: {matched_files[0]}")
    return True
  else:
    raise AirflowException(
      f"Expected file matching pattern with expected step (s<number>) not found in RAM disk directory '{ramdisk_directory}'"
    )

def verify_last_workload_pod_ramdisk_checkpoint(
  project: str,
  region: str,
  cluster_name: str,
  workload_id: str,
  expected_steps: List[int],  # Expected step numbers passed externally
  workload_namespace: str = "default",  # Namespace of the training workload pods
  ramdisk_directory: str = "",            # RAM disk directory path inside the CSI driver container
  driver_namespace: str = "gke-managed-checkpointing",  # Namespace where CSI driver pods run
  driver_label_selector: str = "k8s-app=high-scale-checkpointing",  # Label selector for CSI driver pods
  driver_container_name: str = "csi",     # Container name within the driver pod for command execution
) -> TaskGroup:
  """
  Orchestrate RAM disk checkpoint verification for the last workload pod using a TaskGroup.

  The TaskGroup chains two subtasks:
    a. Preparation Task: Prepares verification targets by listing workload pods, selecting the last
       training pod, and identifying the corresponding CSI driver pod on the same node.
    b. Verification Task: Executes a command on the identified CSI driver pod to check the contents
       of the RAM disk directory and verifies the presence of checkpoint files.

  Args:
    project (str): GCP project ID.
    region (str): GCP region.
    cluster_name (str): Kubernetes cluster name.
    workload_id (str): Unique training workload identifier.
    workload_namespace (str): Namespace where training workload pods are deployed.
    ramdisk_directory (str): Path to the RAM disk directory inside the CSI driver container.
    driver_namespace (str): Namespace where the CSI driver pods are deployed.
    driver_label_selector (str): Label selector to identify the CSI driver pods.
    driver_container_name (str): Container name in the CSI driver pod to execute the command.
    expected_steps (List[int]): Allowed step numbers for the expected checkpoint file.

  Returns:
    TaskGroup: A task group containing the chained tasks.
  """
  with TaskGroup("verify_checkpoint_group", tooltip="Checkpoint verification for last workload pod") as group:
    # Preparation Task: Get verification targets (training pod and driver pod)
    verification_targets = prepare_verification_targets(
      project, region, cluster_name, workload_id, workload_namespace, driver_namespace, driver_label_selector
    )
    # Verification Task: Check for checkpoint files in the RAM disk directory on the driver pod
    verify = verify_checkpoint_files(
      verification_targets,
      ramdisk_directory,
      driver_container_name,
      workload_id,
      project,
      region,
      cluster_name,
      expected_steps
    )

  return group
