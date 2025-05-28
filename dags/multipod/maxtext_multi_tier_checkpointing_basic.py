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
    dag_id="maxtext_multi_tier_checkpointing_sav01",
    schedule=None,
    tags=[
        "multipod_team",
        "maxtext",
        "multi_tier_checkpointing_res01",
    ],
    start_date=datetime.datetime(2025, 2, 27),
    catchup=False,
    concurrency=2,
) as dag:
  base_output_directory = (
      f"{gcs_bucket.BASE_OUTPUT_DIR}/maxtext_multi_tier_checkpointing_phase2"  # Fill your output dir
  )
  dataset_path = gcs_bucket.TRAIN_DATA_C4
  docker_images = [
      (SetupMode.STABLE, DockerImage.ORBAX_STABLE_TEMPLATED_RUNNER),  # Fill your Maxtext runner
  ]
  test_configs = {
      # accelerator: list of slices to test
      "v5p-8": [2],
  }
  clusters = {
      # accelerator: cluster name
      "v5p-8": XpkClusters.TPU_V5P_8_CLUSTER_CIENET,
  }
  params = {
    "ramdisk": "/local",
    "steps": 100,
    "chk_period": 200,
    "chk_local": 25,
    "repl_backup_min": 2,
    "checkpoint_storage_target_data_file_size_bytes": 2147483648,
    "model_to_run": "default"
  }

  for mode, image in docker_images:
    for accelerator, slices in test_configs.items():
      for slice_num in slices:
        command = (
            # "sleep 3600s", # For login into pod to debug
            "bash end_to_end/test_mtc_phase_2_save_path.sh"
            f" multitiercheckpointing-{slice_num}x-{accelerator}"
            f" {base_output_directory} {dataset_path}"
            f" {params['ramdisk']} {params['steps']} "
            f" {params['chk_period']} {params['chk_local']}"
            f" {params['repl_backup_min']} {params['model_to_run']}",
        )

        maxtext_v5p8_save_checkpoint = gke_config.get_gke_config(
            num_slices=slice_num,
            cluster=clusters[accelerator],
            time_out_in_min=60,
            test_name="maxtext-multi-tier-checkpointing-phase2-save",
            run_model_cmds=command,
            docker_image=image.value,
            test_owner="Camilo Quinones",
        ).run(ramdisk_directory="local", xpk_branch="main", skip_post_process=True, mtc_enabled=True)

        clean_cmd = (f"rm -rf {params['ramdisk']}/*",)
        clean_ramdisk_one = gke_config.get_gke_config(
              num_slices=slice_num,
              cluster=clusters[accelerator],
              time_out_in_min=60,
              test_name="clean-ramdisk",
              run_model_cmds=clean_cmd,
              docker_image=image.value,
              test_owner="Camilo Quinones",
        ).run(ramdisk_directory="local", xpk_branch="main", skip_post_process=True, mtc_enabled=True)

        (
            maxtext_v5p8_save_checkpoint
            >> clean_ramdisk_one
        )
