
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

"""Utilities to parse the logs of different log pods for MTC """

import re
from absl import logging


class Parser():
  """
  This class is mainly use to parse specifically for Phase2 Emergency Multitier Checkpointer
  """
  def __init__(self):
    """
    When running collect some important info related to restore , save. Locally and in GCS
    """
    self.last_step_restore_local = None
    self.last_step_restore_gcs = None
    self.time_restore_gcs = None
    self.time_restore_emc = None
    self.time_restore_local = None

  def check_restore_from_mtc(self, logs_from_mtc_pod: str=None) -> bool:
    """
    Parse the logs for phase2 emergency multitier checkpointer
    Check that is restoring from gcs bucket
    """

    # Create a dict with all regular exprecions to catch important info for restoring
    regx_restore = {
    'regx_restore_steps_mtc_gcs':r"Restoring from backup \'(.*?)\', checkpoint (\d+)",
    'regx_restore_steps_mtc_incluster':r"Restoring from in-cluster checkpoint (\d+)"
    }

    # Check worker pod is restoring and MTC pod is restoring from gcs bucket
    if "Restoring from backup" in logs_from_mtc_pod:
      result = self.extract_steps_from_logs(regx_restore.get('regx_restore_steps_mtc_gcs',""),logs_from_mtc_pod, "gcs")
      logging.info(f"Restored from GCS Bucket with info: {result}")
    if "Restoring from in-cluster" in logs_from_mtc_pod:
      result = self.extract_steps_from_logs(regx_restore.get('regx_restore_steps_mtc_incluster',""),logs_from_mtc_pod)
      logging.info(f"Restored from local directory with info: {result}")
    return result

  def extract_steps_from_logs(self, regx_restore_event:str, logs_to_exrtact:str, src_restore:str=None):
    step_restore_mtc = int(re.search(regx_restore_event, logs_to_exrtact).group(2)) if src_restore == 'gcs' else int(re.search(regx_restore_event, logs_to_exrtact).group(1))
    if type(step_restore_mtc) == int:
      self.last_step_restore_gcs =step_restore_mtc
      return True
    logging.info("Steps mistmach from Pod Workload and from MTC Driver")
    return False
