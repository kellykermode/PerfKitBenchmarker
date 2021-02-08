# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License
"""Runs a python script on gcsfuse data."""

import logging
import os
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

flags.DEFINE_string(
    'gcsfuse_data_dir', '',
    'The GCS directory storing the files for the benchmark, such as '
    'gs://bucket/dir/')

BENCHMARK_NAME = 'gcsfuse'
BENCHMARK_CONFIG = """
gcsfuse:
  description: >
    Read GCS data via gcsfuse. Specify the number of VMs with --num_vms.
  vm_groups:
    default:
      vm_count: null
      vm_spec:
        GCP:
          machine_type: n1-standard-96
          zone: us-central1-c
          image_family: tf-latest-gpu
          image_project: deeplearning-platform-release
"""

DLVM_PYTHON = '/opt/conda/bin/python'
REMOTE_SCRIPTS_DIR = 'gcsfuse_scripts'
REMOTE_SCRIPT = 'read.py'
UNIT = 'MB/s'

FLAGS = flags.FLAGS


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install and set up gcsfuse on all the VMs."""
  vms = benchmark_spec.vms
  vm_util.RunThreaded(_Prepare, vms)


def _Prepare(vm):
  """Mount gcsfuse at /gcs and set up the test script."""
  # Mount gcsfuse at /gcs
  vm.Install('gcsfuse')

  # Set up the test script
  path = data.ResourcePath(os.path.join(REMOTE_SCRIPTS_DIR, REMOTE_SCRIPT))
  logging.info('Uploading %s to %s', path, vm)
  vm.PushFile(path, REMOTE_SCRIPT)
  vm.RemoteCommand(f'sudo chmod 777 {REMOTE_SCRIPT}')


def Run(benchmark_spec):
  """Run a python script to read the files concurrently."""
  vms = benchmark_spec.vms
  gfile_sample_lists = vm_util.RunThreaded(_ReadThroughputTestViaGfile, vms)
  gcsfuse_sample_lists = vm_util.RunThreaded(_ReadThroughputTestViaGcsfuse, vms)
  samples = []
  for sample_list in gfile_sample_lists + gcsfuse_sample_lists:
    samples.extend(sample_list)
  return samples


def _ReadThroughputTestViaGcsfuse(vm):
  metrics = _ReadThroughputTest(vm, '/gcs/')
  metadata = {
      'gcsfuse_version': FLAGS.gcsfuse_version,
      'gcsfuse_options': FLAGS.gcsfuse_options,
  }
  return [
      sample.Sample('gcsfuse read throughput', x, UNIT, metadata)
      for x in metrics
  ]


def _ReadThroughputTestViaGfile(vm):
  metrics = _ReadThroughputTest(vm, '')
  return [sample.Sample('gfile read throughput', x, UNIT) for x in metrics]


def _ReadThroughputTest(vm, mountpoint):
  """Read the files in the directory via tf.io.gfile or gcsfuse."""
  data_dir = FLAGS.gcsfuse_data_dir
  options = f'--mountpoint="{mountpoint}"'
  cmd = f'gsutil ls "{data_dir}" | {DLVM_PYTHON} {REMOTE_SCRIPT} {options}'
  logging.info(cmd)
  stdout, stderr = vm.RemoteCommand(cmd)
  logging.info(stdout)
  logging.info(stderr)
  return [float(line) for line in stdout.split('\n') if line]


def Cleanup(benchmark_spec):
  """Cleanup gcsfuse on the VM."""
  vm = benchmark_spec.vms[0]
  vm.Uninstall('gcsfuse')  # Unmount gcsfuse
