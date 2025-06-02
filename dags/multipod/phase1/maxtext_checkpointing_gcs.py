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
from dags.common import test_owner
from dags.common.vm_resource import DockerImage, XpkClusters
from dags.multipod.configs import gke_config
from dags.multipod.configs.common import SetupMode  # Run once a day at 10 am UTC (2 am PST)
from xlml.utils import xpk

SCHEDULED_TIME = "0 10 * * *" if composer_env.is_prod_env() else None


with models.DAG(
    dag_id="maxtext_checkpointing_sav01",
    schedule=None,
    tags=[
        "multipod_team",
        "maxtext",
    ],
    start_date=datetime.datetime(2025, 2, 27),
    catchup=False,
    concurrency=2,
) as dag:
  base_output_directory = (
      # Fill your output dir
      f"{gcs_bucket.BASE_OUTPUT_DIR}/maxtext_checkpointing_phase1"
  )
  dataset_path = gcs_bucket.MLPERF_LLM_DIR
  docker_images = [
      # Fill your Maxtext runner
      (SetupMode.STABLE, DockerImage.ORBAX_STABLE_PURE_RUNNER),
  ]
  test_configs = {
      # accelerator: list of slices to test
      "v5p-8": [2],
  }
  clusters = {
      # accelerator: cluster name
      "v5p-8": XpkClusters.TPU_V5P_8_CLUSTER_CIENET,
  }
  ramdisk_directory = "/local"
  test_name = "sav-01"  # TBD
  current_datetime = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

  for mode, image in docker_images:
    for accelerator, slices in test_configs.items():
      for slice_num in slices:
        run_name = f"{test_name}-{slice_num}x{accelerator}-{current_datetime}"
        gcs_path = xpk.create_output_path(
            base_output_directory=base_output_directory,
            run_name=run_name,
        )

        run_model_commands = (
            # "sleep 3600s", # For login into pod to debug
            "export TPU_PREMAPPED_BUFFER_SIZE=52428800000",
            "export TPU_PREMAPPED_BUFFER_TRANSFER_THRESHOLD_BYTES=52428800000",
            " ".join([
                "python3 -m MaxText.train",
                "MaxText/configs/base.yml",
                "remat_policy=full",
                "remat_policy=full",
                "global_parameter_scale=1",
                f"base_output_directory={base_output_directory}",
                "dataset_type=synthetic",
                "steps=100",
                "per_device_batch_size=1",
                "max_target_length=256",
                "enable_emergency_checkpoint=true",
                "checkpoint_period=20",
                f"local_checkpoint_directory={ramdisk_directory}",
                "local_checkpoint_period=10",
                f"run_name={run_name}",
            ])
        )
        maxtext_v5p8_save_checkpoint = gke_config.get_gke_config(
            num_slices=slice_num,
            cluster=clusters[accelerator],
            time_out_in_min=60,
            test_name=test_name,
            run_model_cmds=run_model_commands,
            docker_image=image.value,
            test_owner=test_owner.SEVERUS_H,
        ).run(
            ramdisk_directory="local",
            xpk_branch="main",
            skip_post_process=True,
            mtc_enabled=True
        )

        clean_cmd = (f"rm -rf /local/*",)
        clean_ramdisk_one = gke_config.get_gke_config(
              num_slices=slice_num,
              cluster=clusters[accelerator],
              time_out_in_min=60,
              test_name="clean-ramdisk",
              run_model_cmds=clean_cmd,
              docker_image=image.value,
              test_owner=test_owner.SEVERUS_H,
        ).run(
            ramdisk_directory="local",
            xpk_branch="main",
            skip_post_process=True,
            mtc_enabled=True
        )

        validate = xpk.validate_saving_checkpoint(gcs_path)

        (
            [gcs_path, maxtext_v5p8_save_checkpoint]
            >> clean_ramdisk_one
            >> validate
        )
