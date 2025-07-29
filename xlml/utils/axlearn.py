# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0 #
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utilities to run workloads with xpk (https://github.com/AI-Hypercomputer/xpk)."""

from datetime import timedelta
import re
import uuid
from absl import logging
from airflow.decorators import task
from airflow.hooks.subprocess import SubprocessHook

LALITAH_BRANCH = "lkolluru-orbax-fuji-v2"
SAM_BRANCH = "orbax-fuji-v2"


# This function do some hacks to get Axlearn working with Airlfow
# One of them is deleting some unuseful packages in [dev] dependencies. We only need to run axlearn CLI
@task(execution_timeout=timedelta(hours=1))
def set_up_axlearn_dpd(branch: str):
  """setup axlearn dependencies."""
  if branch == LALITAH_BRANCH or branch == SAM_BRANCH:
    logging.info(f"Using custom branch  ==> {branch}")
    clone_branch = (
        f"git clone --branch {branch} https://github.com/lkolluru05/axlearn.git"
        f" $HOME/axlearn"
    )

  # Maybe add these lines
  install_python3_cmd = [
      "rm -rf ~/.pyenv",
      "rm -rf ~/my_venv",
      "curl https://pyenv.run | bash",
      f"echo 'export PYENV_ROOT=\"$HOME/.pyenv\"' >> ~/.bashrc ",
      f"echo 'export PYENV_ROOT=\"$HOME/.pyenv\"' >> ~/.profile ",
      f"echo '[[ -d $PYENV_ROOT/bin ]] && export PATH=\"$PYENV_ROOT/bin:$PATH\"' >> ~/.bashrc ",
      f"echo '[[ -d $PYENV_ROOT/bin ]] && export PATH=\"$PYENV_ROOT/bin:$PATH\"' >> ~/.profile",
      f"echo 'eval \"$(pyenv init -)\"' >> ~/.bashrc ",
      f"echo 'eval \"$(pyenv init -)\"' >> ~/.profile",
      f"source ~/.bashrc ",
      f"source ~/.profile",
      f"pyenv install 3.10.12 && pyenv global 3.10.12",
      "python -m venv ~/my_venv",
      f"source ~/my_venv/bin/activate",
  ]

  # TODO: Need to think a better way to do this.
  hack_dpndcies_cmd = [
      "sed -i '/^dev = \[/,/^\]/ { /^dev = \[/b; /\[core\]/ { s/^\s*#\s*//; b; }; /\[orbax\]/ { s/^\s*#\s*//; b; }; /^\s*\"/ { s/^\s*#\?\s*//; s/^/#/; }; }' pyproject.toml"
  ]

  cmds = [
      "set -xue",
      "rm -rf $HOME/axlearn",
      clone_branch,
      *install_python3_cmd,
      "python --version",
      f"cd ~/axlearn/ ",
      *hack_dpndcies_cmd,
      f"pip  install -e '.[core,gcp,dev]'",
      "pip list",
      "pyenv rehash",
      "which axlearn",
  ]
  hook = SubprocessHook()
  result = hook.run_command(["bash", "-c", ";".join(cmds)])

  assert (
      result.exit_code == 0
  ), f"Set up axlearn dependencies command failed with code {result.exit_code}"


@task
def activate_axlearn(
    cluster_name: str,
    project_id: str,
    region: str,
):
  """Activate axlearn."""

  cmds = [
      "set -xue",
      "cat ~/.bashrc",
      "cd ~/axlearn",
      "source ~/my_venv/bin/activate",
      "python --version",
      "which axlearn",
      "axlearn gcp config activate",
      f"gcloud container clusters get-credentials {cluster_name} \
        --region {region} --project {project_id}",
  ]
  hook = SubprocessHook()
  result = hook.run_command(["bash", "-c", ";".join(cmds)])
  assert (
      result.exit_code == 0
  ), f"Set up axlearn dependencies command failed with code {result.exit_code}"


@task
def generate_workload_id(benchmark_id: str) -> str:
  """Generate a valid workload ID."""

  # Remove all non-alphanumeric characters, and truncate to ensure the result
  # is less than 40 characters.
  short_benchmark = re.sub(r"[^a-zA-Z0-9-]+", "", benchmark_id)[:32]
  short_id = str(uuid.uuid4())[:8]
  return f"{short_benchmark}{short_id}"


@task(execution_timeout=timedelta(hours=1))
def create_conf_axlearn(
    cluster_name: str,
    project_id: str,
    zone: str,
):
  """create axlearn config file."""
  command_string = f'cat << \'CONFIG_EOF\' > ~/axlearn/.axlearn/axlearn.default.config\n    [gcp]\n_active = "{project_id}:{zone}"\n\n[gcp."{project_id}:{zone}"]\nproject = "{project_id}"\nregion = "{zone[:-2]}"\nzone = "{zone}"\ngke_cluster = "{cluster_name}"\ncluster = "{cluster_name}"\nlabels = "tpu-v5p"\ndocker_repo = "us-docker.pkg.dev/{project_id}/axlearn"\ndefault_dockerfile = "Dockerfile"\npermanent_bucket = "{project_id}-axlearn"\nprivate_bucket = "{project_id}-axlearn"\nttl_bucket = "{project_id}-axlearn"\nCONFIG_EOF\n'
  # command_string = """cat << 'CONFIG_EOF' > ~/axlearn/.axlearn/axlearn.default.config
  #   ["gcp.cienet-cmcs:us-east5-a"]
  #   project = "cienet-cmcs"
  #   region = "us-east5"
  #   zone = "us-east5-a"
  #   gke_cluster = "camiloquinones-axlearn"
  #   cluster = "camiloquinones-axlearn"
  #   labels = "tpu-v5p"
  #   docker_repo = "us-docker.pkg.dev/cienet-cmcs/axlearn"
  #   default_dockerfile = "Dockerfile"
  #   permanent_bucket = "cienet-cmcs-axlearn"
  #   private_bucket = "cienet-cmcs-axlearn"
  #   ttl_bucket = "cienet-cmcs-axlearn"CONFIG_EOF"""
  create_axlearn_conf = [command_string.rstrip("\n")]
  cmds = [*create_axlearn_conf]
  hook = SubprocessHook()
  result = hook.run_command(["bash", "-c", ";".join(cmds)])

  assert (
      result.exit_code == 0
  ), f"Config Axlearn file command failed with code {result.exit_code}"


@task(execution_timeout=timedelta(hours=1))
def run_workload_axlearn(
    cluster_project: str,
    zone: str,
    cluster_name: str,
    run_name: str,
    accelerator_type: str = "",
    module: str = "",
    model_config: str = "",
    trainer_dir: str = "",
    num_replicas: int = 1,
    trace_steps: list[str] = None,
):
  """Run workload through axlearn tool."""

  trace_list = (
      ("--trace_at_steps=" + ", ".join(map(str, trace_steps)))
      if trace_steps
      else " "
  )
  export_var = [
      f"export CLUSTER={cluster_name}",
      f"export NAME={run_name}",
      f"export BASTION_TIER=disabled",
      f"export DEFAULT_PROJECT_ID=$(gcloud config get project)",
      "export PROJECT_ID=${PROJECT_ID:-$DEFAULT_PROJECT_ID}",
      f"export INSTANCE_TYPE={accelerator_type}",
      f"export NUM_REPLICAS={num_replicas}",
      f"export MODULE={module}",
      f"export MODEL_CONFIG={model_config}",
      f"export TRAIN_DIR={trainer_dir}",
  ]
  logging.info(
    f" Cluster: {cluster_name} \
    -- num-replicas={num_replicas} \
    --run_name={run_name} \
    --project={cluster_project} \
    --zone={zone} \
    --instance-type={accelerator_type} \
    --module={module} \
    --config={model_config} \
    --trainer_dir={trainer_dir} \
    --data_dir=gs://axlearn-public/tensorflow_datasets \
    --jax_backend=tpu \
    --mesh_selector={accelerator_type} \
    --initialization_timeout=1200 Trace: {trace_list}"
  )
  workload_create_cmd = (
      f"axlearn gcp launch run --cluster=$CLUSTER    "
      f"--runner_name gke_tpu_single    "
      f" --name=$NAME   --instance_type=$INSTANCE_TYPE   "
      f"--num_replicas=$NUM_REPLICAS         --bundler_spec=allow_dirty=True "
      f"--bundler_type=artifactregistry --bundler_spec=image=tpu         "
      f"--bundler_spec=dockerfile=Dockerfile  --bundler_spec=target=tpu       "
      f"-- \"ulimit -n 1048576; ulimit -c 0; python3 -c 'import jax; jax.devices()'; python3 -m axlearn.common.launch_trainer_main\"     "
      f"--module=$MODULE    --config=$MODEL_CONFIG           "
      f"--trainer_dir=$TRAIN_DIR       "
      f"--data_dir=gs://axlearn-public/tensorflow_datasets            "
      f"--jax_backend=tpu           --mesh_selector=$INSTANCE_TYPE   "
      f"--initialization_timeout=1200      {trace_list}     "
  )

  cmds = [
      "set -xue",
      "source ~/my_venv/bin/activate",
      "cd ~/axlearn",
      "axlearn gcp config activate",
      f"gcloud container clusters get-credentials {cluster_name} \
        --region {zone[:-2]} --project {cluster_project}",
      *export_var,
      workload_create_cmd,
  ]

  hook = SubprocessHook()
  result = hook.run_command(["bash", "-c", ";".join(cmds)])

  assert (
      result.exit_code == 0
  ), f"Axlearn command failed with code {result.exit_code}"


#
#
# def _get_core_api_client(
#     project_id: str, region: str, cluster_name: str
# ) -> k8s_client.CoreV1Api:
#   """Create a core API client for the given cluster."""
#   client = gke.get_authenticated_client(project_id, region, cluster_name)
#
#   # Initilize the client
#   core_api = k8s_client.CoreV1Api(client)
#   logging.info("Successful initilize k8s client from cluster response.")
#   return core_api
#
#
# def _list_workload_pods(
#     core_api: k8s_client.CoreV1Api, workload_id: str
# ) -> k8s_client.V1PodList:
#   """List all pods for the given workload."""
#   logging.info(f"Getting pods for workload_id: {workload_id}")
#   pods = core_api.list_namespaced_pod(
#       label_selector=f"jobset.sigs.k8s.io/jobset-name={workload_id}",
#       namespace="default",
#   )
#   return pods
#
#
# def _get_batch_api_client(
#     project_id: str, region: str, cluster_name: str
# ) -> k8s_client.BatchV1Api:
#   """Create a batch API client for the given cluster."""
#   client = gke.get_authenticated_client(project_id, region, cluster_name)
#
#   # Initilize the client
#   bat h_api = k8s_client.BatchV1Api(client)
#   logging.info(
#       "Successful initilize k8s batch api client from cluster response."
#   )
#   return batch_api
#
#
# def _get_workload_job(
#     batch_api: k8s_client.BatchV1Api, workload_id: str
# ) -> k8s_client.V1Job:
#   """Get the job for a given workload."""
#   logging.info(f"Getting job for workload_id: {workload_id}")
#   job  = batch_api.list_namespaced_job(
#       label_selector=f"jobset.sigs.k8s.io/jobset-name={workload_id}",
#       namespace="default",
#   )
#   if len(jobs.items) == 0:
#     logging.info(f"Getting job for workload_id: {workload_id}")
#     return None
#
#   if len(jobs.items) > 1:
#     logging.info(f"Got more than one job for workload_id: {workload_id}")
#     f r i, job in enumerate(jobs.items):
#       logging.info(f"Job {i=}")
#       logging.info(f"{job}")

#   return jobs.items[0]
#
#
# def extract_numbers(pod_name: str) -> Tuple[int, int]:
#   """Extract slice and pod numbers from pod name."""
#   m tch = re.search(r"slice-job-(\d+)-(\d+)-", pod_name)
#   if match:
#     return int(match.group(1)), int(match.group(2))
#   return (0, 0)
#
#
# def _find_target_pod_node(
#     project_id: str,
#     region: str,
#     cluster_name: str,
#     workload_id: str,
#     last_node: bool = False,
# ) -> str:
#   """find the node name for the workload."""
#   core_api = _get_core_api_client(project_id, region, cluster_name)
#   pods = _list_workload_pods(core_api, workload_id)
#   pod_node_pairs = []
#   pattern = re.compile(r".*slice-job-(\d+)-(\d+)-\w+")
#
#   for pod in pods.items:
#     if pod.status.phase == "Running" and pod.spec.node_name:
#       if pattern.match(pod.metadata.name):
#         pod_node_pairs.append((pod.metadata.name, pod.spec.node_name))

#   # Find the pod with the highest slice and pod numbers.
#
#   # Sort by slice number, then by pod number, and get the last (highest) one
#   sorted_pairs = sorted(pod_node_pairs, key=lambda x: extract_numbers(x[0]))
#   target_pod, target_node = sorted_pairs[0]
#   if last_node:
#     target_pod, target_node = sorted_pairs[-1]
#
#   logging.info("Identified Pod for node deletion:")
#   logging.info(f"  Pod Name:   {target_pod}")
#   logging.info(f"  Node Name:  {target_node}")
#   logging.info("-" * 72)

#   return target_node
#
#
# @task.sensor(poke_interval=60, timeout=600, mode="reschedule")
# def wait_for_workload_start(
#     workload_id: str, project_id: str, region: str, cluster_name: str
# ) -> bool:
#   """Check if the workload has started."""
#   core_api = _get_core_api_client(project_id, region, cluster_name)
#   pods = _list_workload_pods(core_api, workload_id)
#   print(f"Found {len(pods.items)} pods for workload {workload_id}")
#   return len(pods.items) > 0
#
#
# @ta k.sensor(poke_interval=120, timeout=3600, mode="reschedule")
# def wait_for_reach_step_to_interrupt(
#     task_id: str,
#     project_id: str,
#     region: str,
#     cluster_name: str,
#     workload_id: str,
#     step_to_interrupt: str,
# ) -> bool:
#   """
#   Watch any given training pod, check the given step is already reach before
#   deleting a node
#   """
#   core_api = _get_core_api_client(project_id, region, cluster_name)
#   pods = _list_workload_pods(core_api, workload_id)
#
#   i  any(pod.status.phase in ["Pending"] for pod in pods.items):
#     logging.info("Some of the pods is still pending. Waiting to start")
#     return False
#
#   try:
#     for pod in pods.items:
#       i  pod.status.phase == "Failed":
#         # Don't keep retrying if the pod has failed
#         raise AirflowFailException(f"Bad pod phase: {pod.status.phase}")
#       elif pod.status.phase in ["Unknown"]:
#         raise RuntimeError(f"Bad pod phase: {pod.status.phase}")
#   finally:
#     i  all(pod.status.phase in ["Running"] for pod in pods.items):
#
#       # Pick last one running pod
#       pod = pods.items[len(pods.items) - 1]
#       logs = core_api.read_namespaced_pod_log(
#           name=pod.metadata.name, namespace=pod.metadata.namespace
#       )
#
#       # Just to debug TO BE DELETED
#       logging.info(f"Logs for pod {pod.metadata.name}:")
#       for line in logs.split("\n"):
#         logging.info(line)
#       if f"completed step: {step_to_interrupt}" in logs:
#         # Here we return true because we are sure the step "step_to_interrupt" is already save
#         logging.info("The step to be interrupt is {step_to_interrupt}")
#         return True
#   return False
#
#
# @task.sensor(poke_interval=60, timeout=600, mode="reschedule")
# def wait_for_workload_completion(
#     workload_id: str, project_id: str, region: str, cluster_name: str
# ) -> bool:
#   """Check the workload status."""
#   core_api = _get_core_api_client(project_id, region, cluster_name)
#   pods = _list_workload_pods(core_api, workload_id)
#
#   if not pods.items:
#     logging.info(f"No pods found for workload selector: {workload_id}.")
#
#     # Pathways jobs delete all pods on failure so we must also check if the job
#     # is complete
#     batch_api = _get_batch_api_client(project_id, region, cluster_name)
#     job = _get_workload_job(batch_api, workload_id)
#     if job is None:
#       logging.info(
#           f"No pods or jobs were found for workload selector: {workload_id}"
#       )
#       return False
#
#     if any(condition.type == "Failed" for condition in job.status.conditions):
#       # Don't keep retrying if the job has failed
#       raise AirflowFailException('Job has condition type: "Failed"')
#
#     if any(condition.type == "Complete" for condition in job.status.conditions):
#       logging.info(
#           "No pods found but job is complete for workload selector:"
#           f" {workload_id}"
#       )
#       return True

#     return False
#
#   if any(pod.status.phase in ["Pending", "Running"] for pod in pods.items):
#     logging.info("At least one pod has yet to complete.")
#     return False
#
#   try:
#     for pod in pods.items:
#       if pod.status.phase == "Failed":
#         # Don't keep retrying if the pod has failed
#         raise AirflowFailException(f"Bad pod phase: {pod.status.phase}")
#       elif pod.status.phase in ["Unknown"]:
#         raise RuntimeError(f"Bad pod phase: {pod.status.phase}")
#   finally:
#     # TODO(jonbolin): log printing for GPUs, which have multiple containers
#     if len(pod.spec.containers) == 1:
#       # Print the logs of the last pod checked - either the first failed pod or
#       # the last successful one.
#       logs = core_api.read_namespaced_pod_log(
#           name=pod.metadata.name, namespace=pod.metadata.namespace
#       )
#       logging.info(f"Logs for pod {pod.metadata.name}:")
#       for line in logs.split("\n"):
#         logging.info(line)
#     url = LOGGING_URL_FORMAT.format(
#         project=project_id,
#         region=region,
#         cluster=cluster_name,
#         workload_id=workload_id,
#     )
#     logging.info(f"Link to logs: {url}")
#
#   logging.info("All pod(s) phase are succeeded.")
#   return True
#
#
# @ta k(trigger_rule="all_done")
# def clean_up_workload(
#     workload_id: str,
#     project_id: str,
#     zone: str,
#     cluster_name: str,
#     xpk_branch: str = MAIN_BRANCH,
# ) -> bool:
#   """Delete workload."""
#   with tempfile.TemporaryDirectory() as tmpdir:
#     workload_delete_cmd = (
#         f"python {tmpdir}/xpk/xpk.py workload delete"
#         f" --cluster={cluster_name} --workload={workload_id}"
#         f" --project={project_id} --zone={zone}"
#     )
#
#     cmds = get_xpk_setup_cmd(tmpdir, xpk_branch)
#     cmds.append(workload_delete_cmd)
#     hook = SubprocessHook()
#     result = hook.run_command(
#         ["bash", "-c", ";".join(cmds)],
#         env={**os.environ, "KUBECONFIG": os.path.join(tmpdir, "xpk.conf")},
#     )
#     assert (
#         result.exit_code == 0
#     ), f"XPK clean-up failed with code {result.exit_code}"
#
#
# @task
# def delete_node(
#     cluster_name: str,
#     workload_id: str,
#     zone: str,
#     project: str,
#     dry_run: bool = False,
#     last_node: bool = False,
# ) -> None:
#   """Delete node."""
#   nod _name = _find_target_pod_node(
#       project,
#       zone[:-2],
#       cluster_name,
#       workload_id,
#       last_node,
#   )
#   # Delete the specified compute instance.
#   if dry_run:
#     logging.info(
#         f"DRY RUN: Would delete node: {node_name}"
#         f"in zone: {zone} (project: {project})"
#     )
#     return
#
#   logging.info(f"Proceeding to delete node: {node_name}")
#   try:
#     # Initialize the Compute Engine client
#     instances_client = compute_v1.InstancesClient()
#
#     # Delete the instance
#     operation = instances_client.delete(
#         project=project, zone=zone, instance=node_name
#     )
#
#     logging.info(f"Deletion operation started for node: {node_name}")
#     logging.info(f"Operation: {operation.name}")
#     logging.info(f"Deletion command executed for node: {node_name}")
#   except Exception as e:
#     logging.info(f"Error deleting node {node_name}: {e}", file=sys.stderr)
#     sys.exit(1)
#
#
# @task
# def simple_sleep(sleep_seconds: int) -> None:
#   """
#   A simple task that pauses execution for a specified number of seconds
#   using time.sleep().
#
#   Note: This task occupies a worker slot for the entire sleep duration.
#   It is not a sensor and does not use the 'poke' or 'reschedule' mechanism.
#
#   Args:
#       sleep_seconds: The number of seconds the task should sleep.
#   """
#   if sleep_seconds < 0:
#     logging.warning(
#         f"Requested sleep time is negative: {sleep_seconds}. Skipping sleep."
#     )
#     return  # Or raise an error, depending on desired behavior
#   logging.info(
#       f"Simple Sleep Task: Starting sleep for {sleep_seconds} seconds."
#   )
#   # --- The sleep happens here ---
#   time.sleep(sleep_seconds)
#   # -----------------------------
#   logging.info(
#       f"Simple Sleep Task: Finished sleeping after {sleep_seconds} seconds."
#   )
#   return
#
#
# @task.sensor(poke_interval=120, timeout=1200, mode="reschedule")
# def wait_for_training_step_complete(
#     project_id: str,
#     region: str,
#     cluster_name: str,
#     workload_id: str,
#     step: str,
#     polling_time: int = 20,
# ) -> bool:
#   """Restore workload from specific step and calculate elapsed time."""
#   start_time = time.monotonic()  # Record the start time

#   core_api = _get_core_api_client(project_id, region, cluster_name)
#
#   try:
#     w ile True:
#       pods = _list_workload_pods(core_api, workload_id)
#       if any(pod.status.phase in ["Failed", "Unknown"] for pod in pods.items):
#         elapsed_time = time.monotonic() - start_time
#         logging.error(
#             f"Bad pod phase. Sensor failed after {elapsed_time:.2f} seconds."
#         )
#         raise AirflowFailException("Bad pod phase")
#
#       if all(pod.status.phase in ["Running"] for pod in pods.items):
#         # Pick last one running pod
#         pod = pods.items[len(pods.items) - 1]
#         logs = core_api.read_namespaced_pod_log(
#             name=pod.metadata.name, namespace=pod.metadata.namespace
#         )
#         logging.info(f"Logs for pod {pod.metadata.name}:")
#         for line in logs.split("\n"):
#           logging.info(line)
#
#         if f"completed step: {step}" in logs:
#           elapsed_time = time.monotonic() - start_time
#           logging.info(
#               f"Stop training at step {step}. Sensor completed successfully in {elapsed_time:.2f} seconds."
#           )
#           return True
#       time.sleep(polling_time)
#   except Exception as e:
#     elapsed_time = time.monotonic() - start_time
#     logging.error(
#         f"An unexpected error occurred: {e}. Sensor failed after {elapsed_time:.2f} seconds."
#     )
#     raise
