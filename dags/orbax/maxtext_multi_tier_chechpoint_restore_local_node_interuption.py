"""Add commentMore actions
A DAG to run MaxText multi-tier checkpointing tests (phase2: save & validate).
"""

import datetime
from airflow import models
from dags import composer_env, gcs_bucket
from dags.common import test_owner
from dags.common.vm_resource import DockerImage, XpkClusters
from dags.multipod.configs import gke_config
from dags.multipod.configs.common import SetupMode
from xlml.utils import log_explorer
from xlml.utils import orbax

SCHEDULE = "0 10 * * *" if composer_env.is_prod_env() else None

with models.DAG(
    dag_id="maxtext_multi_tier_res09_restore_local_node_interuption_phase2",
    schedule_interval=SCHEDULE,
    tags=[
        "multipod_team",
        "maxtext",
        "multi_tier_p2_chkpt_restore_local_node_interuption",
        "nightly",
    ],
    start_date=datetime.datetime(2025, 6, 18),
    catchup=False,
    concurrency=2,
) as dag:
  base_output_directory = f"{gcs_bucket.CAMILO_OUTPUT_DIR}/maxtext_multi_tier_res09_node_interuption_phase2"
  docker_images = [
      (
          SetupMode.NIGHTLY,
          DockerImage.MAXTEXT_STABLE_SEVERUS,
      )
  ]
  ram_disk = "/local"
  test_configs = {"v5p-64": [4]}
  clusters = {"v5p-64": XpkClusters.TPU_V5P_128_CLUSTER_CIENET}
  step = 500
  local_checkpoint_period = 40
  replicator_backup_interval_minutes = 30
  use_replicator = "true"
  name_prefix = "mxtx-ph2"
  model_name = "llama2-7b"

  for mode, image in docker_images:
    for accelerator, slices in test_configs.items():
      for slice_num in slices:
        delete_cpc = orbax.delete_cpc(
            clusters[accelerator].project,
            clusters[accelerator].zone[:-2],
            clusters[accelerator].name,
            gcs_bucket.CAMILO_OUTPUT_DIR.split("gs://")[1],
            "ct5p-hightpu-4t",
            "google.com/tpu",
            "200000Mi",
        )
        apply_cpc = orbax.apply_cpc(
            clusters[accelerator].project,
            clusters[accelerator].zone[:-2],
            clusters[accelerator].name,
            gcs_bucket.CAMILO_OUTPUT_DIR.split("gs://")[1],
            "ct5p-hightpu-4t",
            "google.com/tpu",
            "200000Mi",
        )
        run_time = datetime.datetime.now().strftime("%Y-%m-%d-%H")
        run_name = (
            f"{name_prefix}-{model_name}-{slice_num}x-{accelerator}-{run_time}"
        )
        workload_command = (
            "export TPU_PREMAPPED_BUFFER_SIZE=26843545600 && "
            "export TPU_PREMAPPED_BUFFER_TRANSFER_THRESHOLD_BYTES=26843545600&& "
            "python3 -m MaxText.train MaxText/configs/base.yml remat_policy=full "
            f"global_parameter_scale=1 base_output_directory={base_output_directory} "
            f"dataset_type=synthetic steps={step} per_device_batch_size=1 "
            f"max_target_length=256 model_name={model_name} per_device_batch_size=2 "
            "reuse_example_batch=1 enable_emergency_checkpoint=true "
            f"local_checkpoint_directory={ram_disk} local_checkpoint_period={local_checkpoint_period} "
            f"use_replicator_service={use_replicator} replicator_backup_interval_minutes={replicator_backup_interval_minutes} "
            f"run_name={run_name}",
        )

        start_time = log_explorer.generate_timestamp()

        # make launch test_name unique
        maxtext_phase2_chkpt_test = gke_config.get_gke_config(
            num_slices=slice_num,
            cluster=clusters[accelerator],
            time_out_in_min=60,
            test_name=f"{name_prefix}-{model_name}",
            run_model_cmds=workload_command,
            docker_image=image.value,
            test_owner=test_owner.ERNIE_C,
        ).run_with_node_interruption(
            ramdisk_directory=ram_disk,
            mtc_enabled=True,
            xpk_branch="develop",
            skip_post_process=True,
        )

        # cleanup run: unique test_name
        cleanup_command = (f"rm -rf {ram_disk}/*",)
        ram_disk_cleanup = gke_config.get_gke_config(
            num_slices=slice_num,
            cluster=clusters[accelerator],
            time_out_in_min=60,
            test_name=f"{name_prefix}-cleanup",
            run_model_cmds=cleanup_command,
            docker_image=image.value,
            test_owner=test_owner.ERNIE_C,
        ).run(
            ramdisk_directory=ram_disk,
            mtc_enabled=True,
            xpk_branch="develop",
            skip_post_process=True,
        )

        end_time = log_explorer.generate_timestamp()
        vali_step = step - 1
        vali_step_list = [
            i for i in range(0, vali_step, local_checkpoint_period)
        ]
        vali_step_list.append(vali_step)

        validate_log_by_steps = log_explorer.validate_log_with_step(
            project_id=clusters[accelerator].project,
            location=clusters[accelerator].zone[:-2],
            cluster_name=clusters[accelerator].name,
            text_filter="Finished asynchronous save `(blocking` `+` `background)` in",
            start_time=start_time,
            end_time=end_time,
            vali_step_list=vali_step_list,
        )
        validate_is_restoring = log_explorer.validate_log_exist(
            project_id=clusters[accelerator].project,
            location=clusters[accelerator].zone[:-2],
            cluster_name=clusters[accelerator].name,
            text_filter="'event_type': 'restore'",
            start_time=start_time,
            end_time=end_time,
        )
        validate_log_by_file_extension = (
            log_explorer.validate_by_file_extension(
                project_id=clusters[accelerator].project,
                location=clusters[accelerator].zone[:-2],
                cluster_name=clusters[accelerator].name,
                start_time=start_time,
                end_time=end_time,
                text_filter="lrwxrwxrwx",
                expected_phase="phase2",
            )
        )

        (
            delete_cpc
            >> apply_cpc
            >> start_time
            >> maxtext_phase2_chkpt_test
            >> ram_disk_cleanup
            >> end_time
            >> validate_log_by_steps
            >> validate_is_restoring
            >> validate_log_by_file_extension
        )
