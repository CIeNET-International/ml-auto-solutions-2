"""
A DAG to run MaxText multi-tier checkpointing tests (phase1: save & validate).
"""

import datetime
from airflow import models
from dags import composer_env, gcs_bucket
from dags.common import test_owner
from dags.common.vm_resource import TpuVersion, Zone, DockerImage, XpkClusters
from dags.multipod.configs import gke_config
from dags.multipod.configs.common import SetupMode
from xlml.utils.multitier_checkpoint import verify_last_workload_pod_ramdisk_checkpoint

SCHEDULE = None if not composer_env.is_prod_env() else "0 10 * * *"

with models.DAG(
    dag_id="maxtext_multi_tier_p2_chkpt_validate",
    schedule_interval=SCHEDULE,
    start_date=datetime.datetime(2025, 5, 7),
    catchup=False,
    concurrency=2,
) as dag:
  base_output_directory = (
      f"{gcs_bucket.CIENET_MTC_BUCKET}/maxtext_multi_tier_checkpointing"
  )
  dataset_path = gcs_bucket.MLPERF_LLM_DIR
  docker_images = [
      (SetupMode.JAX_STABLE_STACK, DockerImage.XPK_JAX_TEST_CUSTOM)
  ]
  ram_disk = "/local"
  test_configs = {"v5p-8": [2]}
  clusters = {"v5p-8": XpkClusters.TPU_V5P_8_CLUSTER_CIENET}

  for mode, image in docker_images:
    for accelerator, slices in test_configs.items():
      for slice_num in slices:
        workload_command = (
          f"export TPU_PREMAPPED_BUFFER_SIZE=52428800000 && "
          f"export TPU_PREMAPPED_BUFFER_TRANSFER_THRESHOLD_BYTES=52428800000 && "
          f"python3 -m MaxText.train MaxText/configs/base.yml remat_policy=full "
          f"global_parameter_scale=1 base_output_directory={base_output_directory} "
          f"dataset_type=synthetic steps=100 per_device_batch_size=1 "
          f"ici_fsdp_parallelism=-1 ici_tensor_parallelism=4 max_target_length=256 && "
          f"reuse_example_batch=1 enable_emergency_checkpoint=true "
          f"local_checkpoint_directory={ram_disk} local_checkpoint_period=10 && "
          f"use_replicator_service=True replicator_backup_interval_minutes=1 "
          f"run_name=$WORKLOAD dataset_path={dataset_path}"
        )
        maxtext_v5p_configs_test = gke_config.get_gke_config(
          num_slices=slice_num,
          cluster=clusters[accelerator],
          time_out_in_min=60,
          test_name="maxtext-multi-tier-checkpointing",
          run_model_cmds=workload_command,
          docker_image=image.value,
          test_owner=test_owner.JACKY_F,
        ).launch_workload(
          ramdisk_directory=ram_disk,
          workload_id="maxtext-multi-tier-checkpointing",
        )
        # phase2: save results (in GCS via existing post_process) then validate

        verify_ramdisk_task=gke_config.get_gke_config(
          num_slices=slice_num,
          cluster=clusters[accelerator],
          time_out_in_min=60,
          test_name="maxtext-multi-tier-checkpointing",
          run_model_cmds=cleanup_command,
          docker_image=image.value,
          test_owner=test_owner.JACKY_F,
        ).verify_last_workload_pod_ramdisk_checkpoint(
          expected_steps=[0, 99],
          workload_id=maxtext_v5p_configs_test,
          emc_directory="/local/tmpfs/client",
        )

        cleanup_command = "rm -rf /local/*"
        ram_disk_cleanup = gke_config.get_gke_config(
          num_slices=slice_num,
          cluster=clusters[accelerator],
          time_out_in_min=60,
          test_name="maxtext-multi-tier-checkpointing",
          run_model_cmds=cleanup_command,
          docker_image=image.value,
          test_owner=test_owner.JACKY_F,
        ).run(ramdisk_directory=ram_disk)

        clean_up_workload=gke_config.get_gke_config(
          num_slices=slice_num,
          cluster=clusters[accelerator],
          time_out_in_min=60,
          test_name="maxtext-multi-tier-checkpointing",
          run_model_cmds=cleanup_command,
          docker_image=image.value,
          test_owner=test_owner.JACKY_F,
        ).clean_up_workload(
          workload_id="maxtext-multi-tier-checkpointing",
        )

        maxtext_v5p_configs_test >> verify_ramdisk_task >> ram_disk_cleanup
