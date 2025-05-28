# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utilities to run workloads with xpk (https://github.com/AI-Hypercomputer/xpk)."""

import os
import tempfile
import uuid
from absl import logging
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.hooks.subprocess import SubprocessHook
from kubernetes import client as k8s_client
from xlml.apis import metric_config
from xlml.utils import gke
from xlml.utils import parser
from dags.common.vm_resource import GpuVersion
from kubernetes.stream import stream
import time


# b/411426745 - Setting branch to 0.4.1 till the depdency issue is resolved.
MAIN_BRANCH = "main"
# Duration = past 7 days
LOGGING_URL_FORMAT = (
    "https://pantheon.corp.google.com/logs/query;"
    + "query=resource.type%3D%22k8s_container%22%0A"
    + "resource.labels.project_id%3D%22{project}%22%0A"
    + "resource.labels.location%3D%22{region}%22%0A"
    + "resource.labels.cluster_name%3D%22{cluster}%22%0A"
    + "resource.labels.namespace_name%3D%22default%22%0A"
    + "labels.k8s-pod%2Fjobset_sigs_k8s_io%2F"
    + "jobset-name%3D%22{workload_id}%22%20severity%3E%3DDEFAULT;"
    + "storageScope=project;duration=P7D?e=13803378&"
    + "mods=allow_workbench_image_override&project={project}"
)


def get_xpk_setup_cmd(tmpdir, branch: str = MAIN_BRANCH):
  clone_branch = (
      f"git clone --branch {branch} https://github.com/AI-Hypercomputer/xpk"
      f" {tmpdir}/xpk"
  )
  cmds = [
      "set -xue",
      clone_branch,
      f"cd {tmpdir}/xpk/src/xpk",
      "sed -i '/validate_dependencies()/s/^/#/' main.py",
      "cat main.py",
      "pip install ruamel.yaml docker",
  ]
  return cmds


def is_valid_gpu_version(accelerator_type: str):
  if accelerator_type in [member.value for member in GpuVersion]:
    return True
  return False


@task
def generate_workload_id(benchmark_id: str) -> str:
  """Generate a valid workload ID."""
  import re

  short_id = str(uuid.uuid4())[:8]
  # Remove all non-alphanumeric characters, and truncate to ensure the result
  # is less than 40 characters.
  short_benchmark = re.sub(r"[^a-zA-Z0-9-]+", "", benchmark_id)[:32]
  return f"{short_benchmark}{short_id}"


@task
def run_workload(
    task_id: str,
    cluster_project: str,
    zone: str,
    cluster_name: str,
    benchmark_id: str,
    workload_id: str,
    gcs_path: str,
    docker_image: str,
    accelerator_type: str,
    run_cmds: str,
    num_slices: int = 1,
    use_vertex_tensorboard: bool = False,
    use_pathways: bool = False,
    ramdisk_directory: str = "",  # Directory for enabling emergency checkpointing
    mtc_enabled: bool = False,  # It enables MTC phase-2 drivers
    xpk_branch: str = MAIN_BRANCH,
):
  """Run workload through xpk tool."""

  with tempfile.TemporaryDirectory() as tmpdir:
    if accelerator_type in [
        GpuVersion.XPK_H100.value,
        GpuVersion.XPK_H100_MEGA.value,
    ]:
      multi_keyword = "num-nodes"
    else:
      multi_keyword = "num-slices"

    create_field = "create-pathways" if use_pathways else "create"
    type_field = "tpu-type" if use_pathways else "device-type"

    workload_create_cmd = (
        f"python {tmpdir}/xpk/xpk.py workload {create_field}"
        f" --cluster={cluster_name} --workload={workload_id}"
        f" --command='{run_cmds}' --{type_field}={accelerator_type}"
        f" --{multi_keyword}={num_slices} --docker-image={docker_image}"
        f" --project={cluster_project} --zone={zone}"
        f" --env {metric_config.SshEnvVars.GCS_OUTPUT.name}={gcs_path}"
        " --restart-on-user-code-failure"
    )
    if ramdisk_directory:
      workload_create_cmd += f" --ramdisk-directory={ramdisk_directory}"
    if mtc_enabled:
      workload_create_cmd += " --mtc-enabled"
    # For Orbax DAG
    if ramdisk_directory and mtc_enabled:
      workload_create_cmd = workload_create_cmd.replace(
          " --restart-on-user-code-failure", ""
      )
      workload_create_cmd += " --max-restarts=50"

    # If using a valid GPU and the XPK branch is set to "main", then branch is switch to "v0.4.1".
    if is_valid_gpu_version(accelerator_type) and xpk_branch == MAIN_BRANCH:
      xpk_branch = "v0.4.1"

    cmds = get_xpk_setup_cmd(tmpdir, xpk_branch)
    if accelerator_type == GpuVersion.XPK_H100_MEGA.value:
      workload_create_cmd += " --scheduler=gke.io/topology-aware-auto"
    if use_vertex_tensorboard:
      workload_create_cmd += " --use-vertex-tensorboard"
      vertex_ai_dependency = (
          "pip install -U google-cloud-aiplatform cloud-accelerator-diagnostics"
      )
      cmds.append(vertex_ai_dependency)
    cmds.append(workload_create_cmd)
    hook = SubprocessHook()
    result = hook.run_command(
        ["bash", "-c", ";".join(cmds)],
        env={**os.environ, "KUBECONFIG": os.path.join(tmpdir, "xpk.conf")},
    )
    assert (
        result.exit_code == 0
    ), f"XPK command failed with code {result.exit_code}"


def _get_core_api_client(
    project_id: str, region: str, cluster_name: str
) -> k8s_client.CoreV1Api:
  """Create a core API client for the given cluster."""
  client = gke.get_authenticated_client(project_id, region, cluster_name)

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

def _list_mtc_pods(
    core_api: k8s_client.CoreV1Api
) -> k8s_client.V1PodList:
  """List all pods for thecsi Driver"""
  logging.info(f"Getting pods from CSI MTC Driver")
  pods = core_api.list_namespaced_pod(
      label_selector=f"k8s-app=high-scale-checkpointing",
      namespace="gke-managed-checkpointing",
  )
  return pods

def _get_batch_api_client(
    project_id: str, region: str, cluster_name: str
) -> k8s_client.BatchV1Api:
  """Create a batch API client for the given cluster."""
  client = gke.get_authenticated_client(project_id, region, cluster_name)

  # Initilize the client
  batch_api = k8s_client.BatchV1Api(client)
  logging.info(
      "Successful initilize k8s batch api client from cluster response."
  )
  return batch_api


def _get_workload_job(
    batch_api: k8s_client.BatchV1Api, workload_id: str
) -> k8s_client.V1Job:
  """Get the job for a given workload."""
  logging.info(f"Getting job for workload_id: {workload_id}")
  jobs = batch_api.list_namespaced_job(
      label_selector=f"jobset.sigs.k8s.io/jobset-name={workload_id}",
      namespace="default",
  )
  if len(jobs.items) == 0:
    logging.info(f"Getting job for workload_id: {workload_id}")
    return None

  if len(jobs.items) > 1:
    logging.info(f"Got more than one job for workload_id: {workload_id}")
    for i, job in enumerate(jobs.items):
      logging.info(f"Job {i=}")
      logging.info(f"{job}")

  return jobs.items[0]


def _execute_command_in_pod(
    core_api: k8s_client.CoreV1Api,
    pod: k8s_client.V1Pod,
    command,
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


@task.sensor(poke_interval=30, timeout=1200, mode="reschedule")
def validate_restoring_from_gcs_or_local(
    task_id: str,
    project_id: str,
    region: str,
    cluster_name: str,
    ramdisk_dir: str,
) -> bool:
  """Check the EMC is restoring from Bucket or from local Dir """

  # Just give a little more time for testing only.
  time.sleep(30)
  core_api = _get_core_api_client(project_id, region, cluster_name)
  pods_mtc = _list_mtc_pods(core_api)
  if any(pod.status.phase in ["Pending","Terminating"] for pod in pods_mtc.items):
    logging.info("Some of the pods is still pending. Waiting to start")
    return False
  try:
    if any(pod.status.phase in ["Failed"] for pod in pods_mtc.items):
      raise AirflowFailException(f"Bad pod phase. One of the pods Is in Failed")
  finally:
    # Checking MTC  pod event when restoring pod
    logging.info("PODS",pods_mtc.items)
    if all(pod.status.phase in ["Running"] for pod in pods_mtc.items):
      logging.info("HERE")

      #Select randomly one pod to check the local ramdisk
      for i in range(len(pods_mtc.items)-1):
        logs_mtc = core_api.read_namespaced_pod_log(
            name=pods_mtc.items[i].metadata.name, namespace=pods_mtc.items[i].metadata.namespace,
            container="replication-worker"
        )

        # Check is restoring from Bucket
        parser_util = parser.Parser()
        check_mtc_pods = []
        result = parser_util.check_restore_from_mtc(logs_from_mtc_pod=logs_mtc)
        check_mtc_pods.append(result)
        logging.info("MTC pods ==========>",check_mtc_pods)

        # If the result contains 'steps' means that worker pod and MTC pod restore step match.
        if all(check_mtc_pods):
          return True
    return False

@task.sensor(poke_interval=120, timeout=1200, mode="reschedule")
def check_local_ramdisk(
    project_id: str,
    region: str,
    cluster_name: str,
    workload_id: str,
    ramdisk_dir: str,
) -> bool:
  """Check locally in the working pod while executing the ramdisk to see if follow HASH format"""

  # Need time sleep so it can let the pods to recover after injecting failure
  time.sleep(30)

  core_api = _get_core_api_client(project_id, region, cluster_name)
  pods = _list_workload_pods(core_api, workload_id)

  if any(pod.status.phase in ["Pending"] for pod in pods.items):
    logging.info("Some of the pods is still pending. Waiting to start")
    return False

  cmd = ["bash", "-c", f"ls /{ramdisk_dir}"]
  for pod in pods.items:
    # Need to be imporved so it can compare steps with csid driver
    if pod.status.phase == "Running":
      response = _execute_command_in_pod(core_api=core_api, pod=pod, command=cmd)
      files = response.strip().split("\n")
      logging.info("Files ===> ", files)
      if len(files) > 0:
        for file in files:
          if file.endswith(".data"):
            return True


@task.sensor(poke_interval=120, timeout=1200, mode="reschedule")
def run_interruption_cmd(
    task_id: str,
    project_id: str,
    region: str,
    cluster_name: str,
    workload_id: str,
) -> bool:
  """Run command to interrupt pod ."""
  core_api = _get_core_api_client(project_id, region, cluster_name)
  pods = _list_workload_pods(core_api, workload_id)

  if any(pod.status.phase in ["Pending"] for pod in pods.items):
    logging.info("Some of the pods is still pending. Waiting to start")
    return False

  try:
    for pod in pods.items:
      if pod.status.phase == "Failed":
        # Don't keep retrying if the pod has failed
        raise AirflowFailException(f"Bad pod phase: {pod.status.phase}")
      elif pod.status.phase in ["Unknown"]:
        raise RuntimeError(f"Bad pod phase: {pod.status.phase}")
  finally:
    if all(pod.status.phase in ["Running"] for pod in pods.items):

      # Pick last one running pod
      pod = pods.items[len(pods.items) - 1]
      logs = core_api.read_namespaced_pod_log(
          name=pod.metadata.name, namespace=pod.metadata.namespace
      )
      logging.info(f"Logs for pod {pod.metadata.name}:")
      for line in logs.split("\n"):
        logging.info(line)

      # TODO --> More sophisticated way to know when the pod start training.
      if "completed step:" in logs:
        # Here where regex expression to kill pod will go. First test with killing the pod
        result = core_api.delete_namespaced_pod(
            name=pod.metadata.name, namespace=pod.metadata.namespace
        )
        logging.info("The {pod.metadata.name} pod was sucessfully deleted.")
        return True
  return False


@task.sensor(poke_interval=60, timeout=600, mode="reschedule")
def wait_for_workload_start(
    workload_id: str, project_id: str, region: str, cluster_name: str
) -> bool:
  """Check if the workload has started."""
  core_api = _get_core_api_client(project_id, region, cluster_name)
  pods = _list_workload_pods(core_api, workload_id)
  print(f"Found {len(pods.items)} pods for workload {workload_id}")
  return len(pods.items) > 0


@task.sensor(poke_interval=60, timeout=600, mode="reschedule")
def wait_for_workload_resume(
    task_id: str,
    project_id: str,
    region: str,
    cluster_name: str,
    workload_id: str,
) -> bool:
  """Check if the running pods are running"""
  core_api = _get_core_api_client(project_id, region, cluster_name)
  pods = _list_workload_pods(core_api, workload_id)

  if any(pod.status.phase in ["Pending", "Terminating"] for pod in pods.items):
    logging.info("Some of the pods is still pending.Or are terminating")
    return False

  if all(pod.status.phase in ["Running"] for pod in pods.items):
    logging.info("All pods are in running state. Can continue")
    return True


@task.sensor(poke_interval=60, timeout=600, mode="reschedule")
def wait_for_workload_completion(
    workload_id: str, project_id: str, region: str, cluster_name: str
) -> bool:
  """Check the workload status."""
  core_api = _get_core_api_client(project_id, region, cluster_name)
  pods = _list_workload_pods(core_api, workload_id)

  if not pods.items:
    logging.info(f"No pods found for workload selector: {workload_id}.")

    # Pathways jobs delete all pods on failure so we must also check if the job
    # is complete
    batch_api = _get_batch_api_client(project_id, region, cluster_name)
    job = _get_workload_job(batch_api, workload_id)
    if job is None:
      logging.info(
          f"No pods or jobs were found for workload selector: {workload_id}"
      )
      return False

    if any(condition.type == "Failed" for condition in job.status.conditions):
      # Don't keep retrying if the job has failed
      raise AirflowFailException('Job has condition type: "Failed"')

    if any(condition.type == "Complete" for condition in job.status.conditions):
      logging.info(
          "No pods found but job is complete for workload selector:"
          f" {workload_id}"
      )
      return True

    return False

  if any(pod.status.phase in ["Pending", "Running"] for pod in pods.items):
    logging.info("At least one pod has yet to complete.")
    return False

  try:
    for pod in pods.items:
      if pod.status.phase == "Failed":
        # Don't keep retrying if the pod has failed
        raise AirflowFailException(f"Bad pod phase: {pod.status.phase}")
      elif pod.status.phase in ["Unknown"]:
        raise RuntimeError(f"Bad pod phase: {pod.status.phase}")
  finally:
    # TODO(jonbolin): log printing for GPUs, which have multiple containers
    if len(pod.spec.containers) == 1:
      # Print the logs of the last pod checked - either the first failed pod or
      # the last successful one.
      logs = core_api.read_namespaced_pod_log(
          name=pod.metadata.name, namespace=pod.metadata.namespace
      )
      logging.info(f"Logs for pod {pod.metadata.name}:")
      for line in logs.split("\n"):
        logging.info(line)
    url = LOGGING_URL_FORMAT.format(
        project=project_id,
        region=region,
        cluster=cluster_name,
        workload_id=workload_id,
    )
    logging.info(f"Link to logs: {url}")

  logging.info("All pod(s) phase are succeeded.")
  return True


@task(trigger_rule="all_done")
def clean_up_workload(
    workload_id: str,
    project_id: str,
    zone: str,
    cluster_name: str,
    xpk_branch: str = MAIN_BRANCH,
) -> bool:
  """Delete workload."""
  with tempfile.TemporaryDirectory() as tmpdir:
    workload_delete_cmd = (
        f"python {tmpdir}/xpk/xpk.py workload delete"
        f" --cluster={cluster_name} --workload={workload_id}"
        f" --project={project_id} --zone={zone}"
    )

    cmds = get_xpk_setup_cmd(tmpdir, xpk_branch)
    cmds.append(workload_delete_cmd)
    hook = SubprocessHook()
    result = hook.run_command(
        ["bash", "-c", ";".join(cmds)],
        env={**os.environ, "KUBECONFIG": os.path.join(tmpdir, "xpk.conf")},
    )
    assert (
        result.exit_code == 0
    ), f"XPK clean-up failed with code {result.exit_code}"
