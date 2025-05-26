
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
import json
from absl import logging
import json


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

  def get_info_restore(self, logs_from_worker_pod: str = None, logs_from_mtc_pod: str=None) -> dict:
    """
    Parse the logs for phase2 emergency multitier checkpointer
    Check that is restoring from gcs bucket
    """

    # Create a dict with all regular exprecions to catch important info for restoring
    regx_restore = {
    'regx_restore_steps_pod':r"standard_logger\.py:34\] \{(.*?)\}",
    'regx_restore_steps_mtc_gcs':r"Restoring from backup \'(.*?)\', checkpoint (\d+)",
    'regx_restore_steps_mtc_incluster':r"Restoring from in-cluster checkpoint (\d+)"
    }

    # Check worker pod is restoring and MTC pod is restoring from gcs bucket
    if "restoring from this run\'s" in logs_from_worker_pod:
      event = "{"+ re.search(regx_restore.get('regx_restore_steps_pod',""), logs_from_worker_pod).group(1).replace("'",'"')+ "}"
      try:
        json_event = json.loads(event)
      except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON : {e.msg}", e.doc, e.pos)
      step_restore_pod = json_event.get('step',None)
      if "Restoring from backup" in logs_from_mtc_pod:
        result = self.extract_info_from_logs_mtc(step_restore_pod,json_event,regx_restore.get('regx_restore_steps_mtc_gcs',""),logs_from_mtc_pod, "gcs")
        logging.info(f"Restored from GCS Bucket with info: {result}")
      if "Restoring from in-cluster" in logs_from_mtc_pod:
        result = self.extract_info_from_logs_mtc(step_restore_pod,json_event,regx_restore.get('regx_restore_steps_mtc_incluster',""),logs_from_mtc_pod)
        logging.info(f"Restored from local directory with info: {result}")
      return result
    logging.info("The workload is not restoring neither locally or from GCS",)
    return {}


  def extract_info_from_logs_mtc(self, step_restore_pod:str, json_event:dict, regx_restore_event:str, logs_to_exrtact:str, src_restore:str=None):
    step_restore_mtc = int(re.search(regx_restore_event, logs_to_exrtact).group(2)) if src_restore == 'gcs' else int(re.search(regx_restore_event, logs_to_exrtact).group(1))
    if step_restore_pod == step_restore_mtc:
      self.last_step_restore_gcs = step_restore_pod
      self.time_restore_emc = json_event.get('checkpoint_manager_duration_secs',None)
      return {'steps':self.last_step_restore_gcs,'source':'gcs','emc_time':self.time_restore_emc}
    logging.info("Steps mistmach from Pod Workload and from MTC Driver")
    return {}
