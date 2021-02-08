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
"""Module installing, mounting and unmounting gcsfuse."""

from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_string('gcsfuse_version', '0.35.0', 'The version of the gcsfuse.')
flags.DEFINE_string('gcsfuse_options', '--implicit-dirs',
                    'The options used to mount gcsfuse.')

PACKAGE_LOCAL = '/tmp/gcsfuse.deb'
MNT = '/gcs'


def _PackageUrl():
  return 'https://github.com/GoogleCloudPlatform/gcsfuse/releases/download/v{v}/gcsfuse_{v}_amd64.deb'.format(
      v=FLAGS.gcsfuse_version)


def AptInstall(vm):
  """Installs the gcsfuse package and mounts gcsfuse."""
  vm.InstallPackages('wget')
  vm.RemoteCommand('wget -O {local} {url}'.format(
      local=PACKAGE_LOCAL, url=_PackageUrl()))

  vm.InstallPackages(PACKAGE_LOCAL)

  vm.RemoteCommand(
      'sudo mkdir -p {mnt} && sudo chmod a+w {mnt}'.format(mnt=MNT))
  vm.RemoteCommand('gcsfuse {opts} {mnt}'.format(
      opts=FLAGS.gcsfuse_options, mnt=MNT))


def Uninstall(vm):
  vm.RemoteCommand('sudo umount {mnt}'.format(mnt=MNT))
