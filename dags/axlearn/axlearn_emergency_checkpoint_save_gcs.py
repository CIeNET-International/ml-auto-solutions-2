# Copyright 2024 Google LLC
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

"""A DAG to run all supported ML models with the latest JAX/FLAX version."""

import datetime
from airflow import models
from dags import composer_env
from dags.multipod.configs.common import SetupMode
from dags.common.vm_resource import TpuVersion, Zone, RuntimeVersion, Project,DockerImage
from dags.axlearn.configs import axlearn_config as config
from airflow.utils.task_group import TaskGroup


# Run once a day at 6 pm UTC (11 am PST)
SCHEDULED_TIME = '0 18 * * *' if composer_env.is_prod_env() else None

v5p_conf = {
    'tpu_version': TpuVersion.V5P,
    'tpu_cores': 8,
    'tpu_zone': Zone.US_CENTRAL1_A.value,
    'is_tpu_reserved': False,
    'project_name': "cienet-cmcs",
    'network': 'camiloquinones-net',
    'subnetwork': 'camiloquinones-subnet',
}

common = {
    'time_out_in_min': 180,
    'task_owner': "Camilo",
}

with models.DAG(
    dag_id='project_bite_tpu_e2e',
    schedule=SCHEDULED_TIME,
    tags=[
        'multipod_team',
        'tpu',
        'axlearn',
    ],
    start_date=datetime.datetime(2024, 4, 4),
    catchup=False,
) as dag:
  with TaskGroup(
      group_id='axlearn_tpu_training', prefix_group_id=False
  ) as axlearn_training:

    docker_images = [(
        SetupMode.NIGHTLY,
        DockerImage.AXLEARN_ORBAX_STABLE_JAX,
    )]

    test_configs = {
        'v5p-128': [2],
    }
    for mode, image in docker_images:
      for accelerator, slices in test_configs.items():
        for slice_num in slices:

          # AXLearn head against JAX head
          # Runs Fuji training on v5p-128 in the provided GCP Project
          jax_main_fuji_v5p_8 = config.get_axlearn_tpu_config(
              cluster_name="camiloquinones-axlearn",
              docker_image=image.value,
              tpu_version=TpuVersion.V5P,
              tpu_cores=128,
              tpu_zone=Zone.US_EAST5_A.value,
              runtime_version=RuntimeVersion.V2_ALPHA_TPUV5.value,
              project_name="cienet-cmcs",
              network='camiloquinones-net',
              subnetwork='camiloquinones-subnet',
              is_tpu_reserved=False,
              num_slices=2,
              model_config='fuji-test-v1',
              time_out_in_min=180,
              task_owner="Camilo",
          ).run()

