# Lint as: python3
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
# limitations under the License.
"""Script that concurrently reads files to test the max throughput.

Example usages:

(1) Specify `--mountpoint` to read from gcsfuse.
> gsutil ls gs://gcsfuse-benchmark/10M/ | python read.py --mountpoint=/gcs/

(2) Omit `--mountpoint` to read from GCS using tf.io.gfile; specify
`--iterations` to run it multiple times.
> gsutil ls gs://gcsfuse-benchmark/10M/ | python read.py --iterations=3
"""
import concurrent.futures
import sys
import time

from absl import flags
from absl import logging
import tensorflow as tf

FLAGS = flags.FLAGS

flags.DEFINE_integer(
    "iterations", 1, "Number of iterations this benchmark should repeated run.")

flags.DEFINE_integer("workers", 16,
                     "Number of workers this benchmark runs concurrently.")

flags.DEFINE_string(
    "mountpoint", None,
    "The directory where all the GCS buckets are mounted. If "
    "omitted, the benchmark reads the objects with tf.io.gfile "
    "instead.")

flags.DEFINE_bool("verbose", False, "Print the results with extra information.")


class ObjectReader():
  """Provides a function to open and read an object as a file from GCS."""

  def __init__(self, object_name, mountpoint):
    self.object_name = object_name
    self.mountpoint = mountpoint

  def Open(self):
    """Opens the file with tf.io.gfile or from local FS using Gcsfuse."""
    if self.mountpoint:
      file_name = self.object_name.replace("gs://", self.mountpoint)
      return open(file_name, "rb")
    else:
      return tf.io.gfile.GFile(self.object_name, "rb")

  def ReadFull(self):
    """Reads the entire file and returns the bytes read."""
    f = self.Open()
    bytes_read = 0
    while True:
      data = f.read(2 * 1024 * 1024)
      if data:
        bytes_read += len(data)
      else:
        break
    f.close()
    return bytes_read


class ReadBenchmark():
  """Runs a benchmark by reading files and measure the throughput."""

  def __init__(self):
    self.iterations = FLAGS.iterations
    self.executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=FLAGS.workers)

    objects = sys.stdin.read().split("\n")
    self.readers = [ObjectReader(o, FLAGS.mountpoint) for o in objects if o]

  def Run(self):
    """Run the benchmark N times, printing all the metrics per iteration."""
    for it in range(self.iterations):
      total_mb, duration_sec = self.RunAllReaders()
      self.PrintResult(it, total_mb, duration_sec)
    self.executor.shutdown()

  def RunAllReaders(self):
    """Read all files, returning bytes read and duration."""
    start = time.time()
    size_list = list(self.executor.map(lambda r: r.ReadFull(), self.readers))
    total_mb = sum(size_list) * 1.0 / (1024 * 1024)
    duration_sec = time.time() - start
    return total_mb, duration_sec

  def PrintResult(self, iteration, total_mb, duration_sec):
    throughput = total_mb / duration_sec
    if FLAGS.verbose:
      info = "#{}: {} MB, {:.1f} seconds, {:.1f} MB/s".format(
          iteration, total_mb, duration_sec, throughput)
      print(info)
    else:
      print(throughput)


if __name__ == "__main__":
  # Parse command-line flags
  try:
    FLAGS(sys.argv)
  except flags.Error as e:
    logging.exception("%s\nUsage: %s ARGS\n%s", e, sys.argv[0], FLAGS)
    sys.exit(1)
  ReadBenchmark().Run()
