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

"""GCS bucket for data."""

# GCS bucket for training data
CRITEO_DIR = "gs://ml-auto-solutions/data/criteo/terabyte_processed_shuffled"
IMAGENET_DIR = "gs://ml-auto-solutions/data/imagenet"
TFDS_DATA_DIR = "gs://ml-auto-solutions/data/tfds-data"
MAXTEXT_DIR = "gs://max-datasets-rogue"
AXLEARN_DIR = "gs://axlearn-public/tensorflow_datasets"
# TRAIN_DATA_C4 = "gs://camilo-bucket-orbax"
TRAIN_DATA_C4 = "gs://severus-maxtext-c4-dataset"

# GCS bucket for output
# Multi-tier checkpointing need special permission for GCS Bucket
# For further question reach out to  Multi-tier Checkpointing Owners.
MTC_BUCKET = "gs://mtc-bucket-us-east5/output"
# BASE_OUTPUT_DIR = "gs://ml-auto-solutions/output"
BASE_OUTPUT_DIR = "gs://severus-maxtext-c4-dataset/output_airflow"

# Test camilo DELETE LATER
BASE_OUTPUT_DIR_TEST = "gs://camilo-bucket-orbax"
