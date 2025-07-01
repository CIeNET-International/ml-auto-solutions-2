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

from unittest import mock
from absl.testing import absltest
from absl.testing import parameterized
from xlml.utils import log_explorer


class LogExplorerTest(parameterized.TestCase, absltest.TestCase):

  @mock.patch("google.cloud.logging.Client")
  def test_list_log_entries_get(self, mock_client):
    mock_log_1 = mock.MagicMock()
    mock_log_1.payload = "save checkpoint"
    mock_log_2 = mock.MagicMock()
    mock_log_2.payload = "test"
    mock_client_instance = mock_client.return_value
    mock_client_instance.list_entries.return_value = [mock_log_1, mock_log_2]
    result = log_explorer.list_log_entries("p", "l", "c")
    mock_client.assert_called_once_with(project="p")
    mock_client_instance.list_entries.assert_called_once()
    self.assertEqual(len(result), len([mock_log_1, mock_log_2]))
    self.assertEqual(result, [mock_log_1, mock_log_2])

  @mock.patch("google.cloud.logging.Client")
  def test_list_log_entries_no_entries_found(self, mock_client):
    mock_client_instance = mock_client.return_value
    mock_client_instance.list_entries.return_value = []
    result = log_explorer.list_log_entries("p", "l", "c")
    mock_client.assert_called_once_with(project="p")
    mock_client_instance.list_entries.assert_called_once()
    self.assertEqual(len(result), 0)
    self.assertEqual(result, [])

  @mock.patch("google.cloud.logging.Client")
  def test_list_log_entries_api_error(self, mock_client):
    mock_client_instance = mock_client.return_value
    mock_client_instance.list_entries.side_effect = Exception("API error")
    with self.assertRaisesRegex(Exception, "API error"):
      log_explorer.list_log_entries("p", "l", "c")
    mock_client.assert_called_once_with(project="p")
    mock_client_instance.list_entries.assert_called_once()


if __name__ == "__main__":
  absltest.main()
