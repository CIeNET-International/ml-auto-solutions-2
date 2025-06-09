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

"""Tests for log_explorer.py."""

import datetime
import sys
from unittest import mock
from absl import flags
from absl.testing import absltest
from absl.testing import parameterized

# Mocking Airflow decorators and exceptions as 
# they are not needed for unit logic tests
# and might cause import errors outside an Airflow environment.
# For actual Airflow DAG testing, you'd use Airflow's test utilities.
mock_task = mock.MagicMock(return_value=lambda f: f)
mock_airflow_fail_exception = type('AirflowFailException', (Exception,), {})

sys.modules['airflow.decorators'] = mock.MagicMock(task=mock_task)
sys.modules['airflow.exceptions'] = mock.MagicMock(
  AirflowFailException=mock_airflow_fail_exception
)

# Mocking Google Cloud Logging and xlml.utils.gcs
mock_gcp_logging_client = mock.MagicMock()
mock_gcp_logging_client.list_entries.return_value = [] # Default to empty list
sys.modules['google.cloud.logging'] = mock.MagicMock(Client=mock.MagicMock(return_value=mock_gcp_logging_client))

mock_gcs = mock.MagicMock()
mock_gcs.get_gcs_files.return_value = [] # Default to empty list
sys.modules['xlml.utils.gcs'] = mock_gcs

# Mocking absl.logging to capture or suppress logs during tests
mock_absl_logging = mock.MagicMock()
sys.modules['absl.logging'] = mock_absl_logging

# Now import the functions from your log_explorer.py
# Assuming log_explorer.py is in the same directory or accessible via PYTHONPATH
# If log_explorer.py is in a subdirectory (e.g., 'dags/utils/log_explorer.py'),
# adjust the import path accordingly.
from xlml.utils.log_explorer import (
    validate_gcs_checkpoint_save,
    validate_log_with_step,
    list_log_entries,
)


# Helper Mock Class to simulate a LogEntry object from google-cloud-logging
class MockLogEntry:
  """A mock object to simulate a Google Cloud LogEntry."""

  def __init__(self, payload: str, timestamp: datetime.datetime = None):
    self.payload = payload
    self.timestamp = timestamp if timestamp else datetime.datetime.now(
        datetime.timezone.utc
    )
    # Add other attributes if your functions access them (e.g., severity, resource)
    self.severity = "INFO"
    self.resource = mock.MagicMock(
        labels={
            "project_id": "test-project",
            "location": "test-location",
            "cluster_name": "test-cluster",
            "namespace_name": "test-namespace",
            "pod_name": "test-pod",
            "container_name": "test-container",
        }
    )

  def __str__(self):
    return self.payload


class LogExplorerTest(parameterized.TestCase, absltest.TestCase):

  # Helper to manage temp directories (from your example)
  def get_tempdir(self):
    try:
      flags.FLAGS.test_tmpdir
    except flags.UnparsedFlagAccessError:
      flags.FLAGS(sys.argv)
    return self.create_tempdir().full_path

  def setUp(self):
    super().setUp()
    # Reset mocks before each test to ensure isolation
    mock_gcp_logging_client.reset_mock()
    mock_gcs.reset_mock()
    mock_absl_logging.reset_mock()
    # Ensure list_entries returns an empty list by default
    mock_gcp_logging_client.list_entries.return_value = []
    mock_gcs.get_gcs_files.return_value = []

  # --- Tests for list_log_entries ---
  @parameterized.named_parameters(
      ("default_params", None, None, None),
      ("with_container", "my-container", None, None),
      ("with_text_filter", None, "some_text", None),
      (
          "with_time_range",
          None,
          None,
          (
              datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
              datetime.datetime(2025, 1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
          ),
      ),
      (
          "only_start_time",
          None,
          None,
          (datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc), None),
      ),
  )
  def test_list_log_entries_filter_construction(
      self, container_name, text_filter, time_range
  ):
    project_id = "test-project"
    location = "us-central1"
    cluster_name = "test-cluster"
    namespace = "test-namespace"
    pod_pattern = "test-pod-*"

    start_time = time_range[0] if time_range else None
    end_time = time_range[1] if time_range else None

    # Set mock return value
    mock_gcp_logging_client.list_entries.return_value = [
        MockLogEntry("log1"),
        MockLogEntry("log2"),
    ]

    result = list_log_entries(
        project_id=project_id,
        location=location,
        cluster_name=cluster_name,
        namespace=namespace,
        pod_pattern=pod_pattern,
        container_name=container_name,
        text_filter=text_filter,
        start_time=start_time,
        end_time=end_time,
    )

    # Assert that list_entries was called
    mock_gcp_logging_client.list_entries.assert_called_once()
    actual_filter = mock_gcp_logging_client.list_entries.call_args[1]["filter_"]

    self.assertIn(f'resource.labels.project_id="{project_id}"', actual_filter)
    self.assertIn(f'resource.labels.location="{location}"', actual_filter)
    self.assertIn(f'resource.labels.cluster_name="{cluster_name}"', actual_filter)
    self.assertIn(f'resource.labels.namespace_name="{namespace}"', actual_filter)
    self.assertIn(f'resource.labels.pod_name:"{pod_pattern}"', actual_filter)
    self.assertIn("severity>=DEFAULT", actual_filter)

    if container_name:
      self.assertIn(f'resource.labels.container_name="{container_name}"', actual_filter)
    if text_filter:
      self.assertIn(f'SEARCH("{text_filter}")', actual_filter)

    # Check time range in filter
    if start_time and end_time:
      self.assertIn(f'timestamp>="{start_time.strftime("%Y-%m-%dT%H:%M:%SZ")}"', actual_filter)
      self.assertIn(f'timestamp<="{end_time.strftime("%Y-%m-%dT%H:%M:%SZ")}"', actual_filter)
    elif start_time and not end_time:
      # If only start_time is provided, end_time defaults to now.
      # We can't assert the exact 'now' string, but check start_time is there.
      self.assertIn(f'timestamp>="{start_time.strftime("%Y-%m-%dT%H:%M:%SZ")}"', actual_filter)
      self.assertIn('timestamp<="', actual_filter) # Should still have an end_time part

    self.assertIsInstance(result, list)
    self.assertEqual(len(result), 2)
    self.assertEqual(result[0].payload, "log1")

  def test_list_log_entries_no_entries_found(self):
    mock_gcp_logging_client.list_entries.return_value = []
    result = list_log_entries("p", "l", "c")
    self.assertEqual(len(result), 0)
    mock_absl_logging.info.assert_called_with("Log filter constructed: %s", mock.ANY)
    mock_absl_logging.info.assert_called_with(mock.ANY, 0) # Asserts the count info log

  def test_list_log_entries_api_error(self):
    mock_gcp_logging_client.list_entries.side_effect = Exception("API error")
    result = list_log_entries("p", "l", "c")
    self.assertEqual(len(result), 0)
    mock_absl_logging.error.assert_called_with("Error fetching log entries in list_log_entries: %s", mock.ANY)

  # --- Tests for validate_gcs_checkpoint_save ---
  @parameterized.named_parameters(
      (
          "success_single_file",
          [
              MockLogEntry(
                  "Successful: backup for step 100 to backup/gcs/2025-06-05_03-07"
              )
          ],
          ["file.data", "other.txt"],
          True,
          None,
      ),
      (
          "failure_no_data_file",
          [
              MockLogEntry(
                  "Successful: backup for step 100 to backup/gcs/2025-06-05_03-07"
              )
          ],
          ["only.txt", "another.log"],
          False,
          "Checkpoint files can not found in 2025-06-05_03-07",
      ),
      (
          "failure_empty_bucket_files",
          [
              MockLogEntry(
                  "Successful: backup for step 100 to backup/gcs/2025-06-05_03-07"
              )
          ],
          [],
          False,
          "Checkpoint files can not found in 2025-06-05_03-07",
      ),
      (
          "success_no_gcs_log",
          [MockLogEntry("Just a regular log line")],
          [],
          True, # Should return True as no GCS paths were found to validate
          None,
      ),
      (
          "success_multiple_lines_one_path",
          [
              MockLogEntry(
                  "Line 1\nSuccessful: backup for step 100 to backup/gcs/2025-06-05_03-07\nLine 3"
              )
          ],
          ["file.data"],
          True,
          None,
      ),
      (
          "failure_list_log_entries_error",
          [],
          [],
          False,
          "Error fetching logs", # Expecting AirflowFailException from list_log_entries error
          True, # Indicates list_log_entries should throw an error
      ),
  )
  @mock.patch('log_explorer.list_log_entries')
  def test_validate_gcs_checkpoint_save(
      self,
      mock_entries,
      mock_gcs_files_return,
      expected_result,
      expected_exception_message,
      mock_list_log_entries,
      simulate_list_log_entries_error=False,
  ):
    project_id = "test-project"
    location = "us-central1"
    cluster_name = "test-cluster"
    bucket_name = "test-bucket"

    if simulate_list_log_entries_error:
      mock_list_log_entries.side_effect = Exception("Error fetching logs")
    else:
      mock_list_log_entries.return_value = mock_entries

    mock_gcs.get_gcs_files.return_value = mock_gcs_files_return

    if expected_exception_message:
      with self.assertRaisesRegex(
          mock_airflow_fail_exception, expected_exception_message
      ):
        validate_gcs_checkpoint_save(
            project_id=project_id,
            location=location,
            cluster_name=cluster_name,
            bucket_name=bucket_name,
        )
    else:
      result = validate_gcs_checkpoint_save(
          project_id=project_id,
          location=location,
          cluster_name=cluster_name,
          bucket_name=bucket_name,
      )
      self.assertEqual(result, expected_result)
      if "backup/gcs/" in str(mock_entries): # Check get_gcs_files only if path is expected to be found
        # Assert get_gcs_files was called with the correct path
        mock_gcs.get_gcs_files.assert_called_once_with(
            f"{bucket_name}/2025-06-05_03-07/"
        )
      else:
        # If no GCS log string is present, get_gcs_files should not be called
        mock_gcs.get_gcs_files.assert_not_called()

  # --- Tests for validate_log_with_step ---
  @parameterized.named_parameters(
      (
          "success_all_steps_found",
          [
              MockLogEntry("10 seconds to /local/1000"),
              MockLogEntry("5 seconds to /local/2000"),
          ],
          [1000, 2000],
          True,
          None,
      ),
      (
          "failure_some_steps_missing",
          [MockLogEntry("10 seconds to /local/1000")],
          [1000, 2000],
          False,
          ".*", # Expecting AirflowFailException, message doesn't matter much here
      ),
      (
          "success_no_steps_to_validate",
          [],
          None,
          False, # As per `if vali_step_list is None: return False`
          None,
      ),
      (
          "success_no_relevant_logs",
          [MockLogEntry("Irrelevant log line")],
          [1000],
          False,
          ".*", # Expecting AirflowFailException if steps are defined but not found
      ),
      (
          "failure_list_log_entries_error",
          [],
          [1000], # Vali step list is present
          False,
          "Error fetching logs", # Expecting AirflowFailException from list_log_entries error
          True, # Indicates list_log_entries should throw an error
      ),
  )
  @mock.patch('log_explorer.list_log_entries')
  def test_validate_log_with_step(
      self,
      mock_entries,
      vali_step_list,
      expected_result,
      expected_exception_message,
      mock_list_log_entries,
      simulate_list_log_entries_error=False,
  ):
    project_id = "p"
    location = "l"
    cluster_name = "c"

    if simulate_list_log_entries_error:
      mock_list_log_entries.side_effect = Exception("Error fetching logs")
    else:
      mock_list_log_entries.return_value = mock_entries

    if expected_exception_message:
      with self.assertRaisesRegex(
          mock_airflow_fail_exception, expected_exception_message
      ):
        validate_log_with_step(
            project_id=project_id,
            location=location,
            cluster_name=cluster_name,
            vali_step_list=vali_step_list,
        )
    else:
      result = validate_log_with_step(
          project_id=project_id,
          location=location,
          cluster_name=cluster_name,
          vali_step_list=vali_step_list,
      )
      self.assertEqual(result, expected_result)
      if vali_step_list is not None and expected_result is True:
        mock_absl_logging.info.assert_called_with("Validate success")
      elif vali_step_list is None:
        pass # No specific log message to assert here, returns False
      else: # Expecting a failure for missing steps
        mock_absl_logging.info.assert_called_with("Validate success")
        # Assert not called, because an exception is raised before this log
        # For parameterized tests, the assertRaisesRegex already verifies the failure.


if __name__ == "__main__":
  absltest.main()
