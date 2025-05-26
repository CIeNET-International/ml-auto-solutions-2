# Copyright 2025 Google LLC
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

"""
A DAG to run MaxText multi-tier checkpointing tests.
"""
import datetime
from airflow import models
from dags import composer_env, gcs_bucket
from dags.common.vm_resource import DockerImage, XpkClusters
from dags.multipod.configs import gke_config
from dags.multipod.configs.common import SetupMode  # Run once a day at 10 am UTC (2 am PST)

SCHEDULED_TIME = "0 10 * * *" if composer_env.is_prod_env() else None

with models.DAG(
    dag_id="maxtext_multi_tier_checkpointing_restore_local",
    schedule=None,
    tags=[
        "multipod_team",
        "maxtext",
        "multi_tier_checkpointing_restore_local",
    ],
    start_date=datetime.datetime(2025, 2, 27),
    catchup=False,
    concurrency=2,
) as dag:
  base_output_directory = (
      f"{gcs_bucket.CAMILO_BASE_OUTPUT_DIR}/maxtext_multi_tier_checkpointing_phase2"
  )
  dataset_path = gcs_bucket.CAMILO_BASE_C4
  docker_images = [
      (SetupMode.STABLE, DockerImage.CAMILO_RUNNER),
  ]
  test_configs = {
      # accelerator: list of slices to test
      "v5litepod-32": [2],
      # "v6e-64":[2],
  }
  clusters = {
      # accelerator: cluster name
      "v5litepod-32": XpkClusters.TPU_V5E_32_CLUSTER_CIENET ,
      # "v6e-64": XpkClusters.TPU_V6E_64_CLUSTER_CUSTOM,
  }
  params = {
    "ramdisk": "/local",
    "steps": 20,
    "chk_period": 200,
    "chk_local": 10,
    "repl_backup_min": 30,
    "ici_tensor_parall": 1,
    "use_replicator":True,
    "model_to_run":'llama2-7b'
  }

  for mode, image in docker_images:
    for accelerator, slices in test_configs.items():
      for slice_num in slices:
        command = (
            "bash end_to_end/save_mtc/test_mtc_phase_2_save_path.sh"
            f" multitiercheckpointing-{slice_num}x-{accelerator}"
            f" {base_output_directory} {dataset_path}"
            f" {params['ramdisk']} {params['steps']} "
            f" {params['chk_period']} {params['chk_local']}"
            f" {params['repl_backup_min']} {params['ici_tensor_parall']}"
            f" {params['use_replicator']} {params['model_to_run']}",
        )

        maxtext_v5e32_interruption = gke_config.get_gke_config(
            num_slices=slice_num,
            cluster=clusters[accelerator],
            time_out_in_min=60,
            test_name="maxtext-multi-tier-checkpointing-phase2-initial-run-local",
            run_model_cmds=command,
            docker_image=image.value,
            test_owner="Camilo Quinones",
        ).run(ramdisk_directory="local", xpk_branch="main", skip_post_process=True, mtc_enabled=True)

        params['steps']=50
        command = (
            "bash end_to_end/save_mtc/test_mtc_phase_2_save_path.sh"
            f" multitiercheckpointing-{slice_num}x-{accelerator}"
            f" {base_output_directory} {dataset_path}"
            f" {params['ramdisk']} {params['steps']} "
            f" {params['chk_period']} {params['chk_local']}"
            f" {params['repl_backup_min']} {params['ici_tensor_parall']}"
            f" {params['use_replicator']} {params['model_to_run']}",
        )

        maxtext_v5e32_restore = gke_config.get_gke_config(
            num_slices=slice_num,
            cluster=clusters[accelerator],
            time_out_in_min=60,
            test_name="maxtext-multi-tier-checkpointing-phase2-restore-local",
            run_model_cmds=command,
            docker_image=image.value,
            test_owner="Camilo Quinones",
        ).run_check_restore(ramdisk_directory="local", xpk_branch="main", skip_post_process=True, mtc_enabled=True)

        (
            maxtext_v5e32_interruption
            >> maxtext_v5e32_restore
        )
