# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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


from abc import abstractmethod
import random
import re
import string
import uuid

from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
import six

# TODO(ferneyhough): change to enum
flags.DEFINE_string('managed_db_engine', None,
                    'Managed database flavor to use (mysql, postgres)')
flags.DEFINE_string('managed_db_engine_version', None,
                    'Version of the database flavor selected, e.g. 5.7')
flags.DEFINE_string('managed_db_database_name', None,
                    'Name of the database to create. Defaults to '
                    'pkb-db-[run-uri]')
flags.DEFINE_string('managed_db_database_username', None,
                    'Database username. Defaults to '
                    'pkb-db-user-[run-uri]')
flags.DEFINE_string('managed_db_database_password', None,
                    'Database password. Defaults to '
                    'a random 10-character alpha-numeric string')
flags.DEFINE_boolean('managed_db_high_availability', False,
                     'Specifies if the database should be high availability')
flags.DEFINE_boolean('managed_db_backup_enabled', True,
                     'Whether or not to enable automated backups')
flags.DEFINE_string('managed_db_backup_start_time', '07:00',
                    'Time in UTC that automated backups (if enabled) '
                    'will be scheduled. In the form HH:MM UTC. '
                    'Defaults to 07:00 UTC')
flags.DEFINE_list('managed_db_zone', None,
                  'zone or region to launch the database in. '
                  'Defaults to the client vm\'s zone.')
flags.DEFINE_string('client_vm_zone', None,
                    'zone or region to launch the client in. ')
flags.DEFINE_string('managed_db_machine_type', None,
                    'Machine type of the database.')
flags.DEFINE_integer('managed_db_cpus', None,
                     'Number of Cpus in the database.')
flags.DEFINE_string('managed_db_memory', None,
                    'Amount of Memory in the database.  Uses the same format '
                    'string as custom machine memory type.')
flags.DEFINE_integer('managed_db_disk_size', None,
                     'Size of the database disk in GB.')
flags.DEFINE_string('managed_db_disk_type', None, 'Disk type of the database.')
flags.DEFINE_integer('managed_db_disk_iops', None,
                     'Disk iops of the database on AWS io1 disks.')

flags.DEFINE_integer('managed_db_azure_compute_units', None,
                     'Number of Dtus in the database.')
flags.DEFINE_string('managed_db_tier', None,
                    'Tier in azure. (Basic, Standard, Premium).')
flags.DEFINE_string('client_vm_machine_type', None,
                    'Machine type of the client vm.')
flags.DEFINE_integer('client_vm_cpus', None, 'Number of Cpus in the client vm.')
flags.DEFINE_string(
    'client_vm_memory', None,
    'Amount of Memory in the vm.  Uses the same format '
    'string as custom machine memory type.')
flags.DEFINE_integer('client_vm_disk_size', None,
                     'Size of the client vm disk in GB.')
flags.DEFINE_string('client_vm_disk_type', None, 'Disk type of the client vm.')
flags.DEFINE_integer('client_vm_disk_iops', None,
                     'Disk iops of the database on AWS for client vm.')
flags.DEFINE_boolean(
    'use_managed_db', True, 'If true, uses the managed MySql '
    'service for the requested cloud provider. If false, uses '
    'MySql installed on a VM.')
flags.DEFINE_list(
    'db_flags', '', 'Flags to apply to the implementation of '
    'MySQL on the cloud that\'s being used. Example: '
    'binlog_cache_size=4096,innodb_log_buffer_size=4294967295')
flags.DEFINE_integer(
    'innodb_buffer_pool_size', None,
    'Size of the innodb buffer pool size in GB. '
    'Defaults to #CPUs G if unset')
flags.DEFINE_integer('innodb_log_file_size', 1000,
                     'Size of the log file in MB. Defaults to 1000M.')


BACKUP_TIME_REGULAR_EXPRESSION = '^\d\d\:\d\d$'
flags.register_validator(
    'managed_db_backup_start_time',
    lambda value: re.search(BACKUP_TIME_REGULAR_EXPRESSION, value) is not None,
    message=('--database_backup_start_time must be in the form HH:MM'))

MYSQL = 'mysql'
POSTGRES = 'postgres'
AURORA_POSTGRES = 'aurora-postgresql'
AURORA_MYSQL = 'aurora-mysql'
AURORA_MYSQL56 = 'aurora'
SQLSERVER = 'sqlserver'
SQLSERVER_EXPRESS = 'sqlserver-ex'
SQLSERVER_ENTERPRISE = 'sqlserver-ee'
SQLSERVER_STANDARD = 'sqlserver-se'

ALL_ENGINES = [
    MYSQL,
    POSTGRES,
    AURORA_POSTGRES,
    AURORA_MYSQL,
    AURORA_MYSQL56,
    SQLSERVER,
    SQLSERVER_EXPRESS,
    SQLSERVER_ENTERPRISE,
    SQLSERVER_STANDARD
]

FLAGS = flags.FLAGS

# TODO: Implement DEFAULT BACKUP_START_TIME for instances.


class RelationalDbPropertyNotSet(Exception):
  pass


class RelationalDbEngineNotFoundException(Exception):
  pass


class UnsupportedError(Exception):
  pass


def GenerateRandomDbPassword():
  """Generate a strong random password.

   # pylint: disable=line-too-long
  Reference: https://docs.microsoft.com/en-us/sql/relational-databases/security/password-policy?view=sql-server-ver15
  # pylint: enable=line-too-long

  Returns:
    A random database password.
  """
  prefix = [random.choice(string.ascii_lowercase),
            random.choice(string.ascii_uppercase),
            random.choice(string.digits)]
  return ''.join(prefix) + str(uuid.uuid4())[:10]


def GetRelationalDbClass(cloud):
  """Get the RelationalDb class corresponding to 'cloud'.

  Args:
    cloud: name of cloud to get the class for

  Returns:
    BaseRelationalDb class with the cloud attribute of 'cloud'.
  """
  return resource.GetResourceClass(BaseRelationalDb, CLOUD=cloud)


def VmsToBoot(vm_groups):
  # TODO(jerlawson): Enable replications.
  return {
      name: spec  # pylint: disable=g-complex-comprehension
      for name, spec in six.iteritems(vm_groups)
      if name == 'clients' or name == 'default' or
      (not FLAGS.use_managed_db and name == 'servers')
  }


class BaseRelationalDb(resource.BaseResource):
  """Object representing a relational database Service."""

  RESOURCE_TYPE = 'BaseRelationalDb'

  def __init__(self, relational_db_spec):
    """Initialize the managed relational database object.

    Args:
      relational_db_spec: spec of the managed database.

    Raises:
      UnsupportedError: if high availability is requested for an unmanaged db.
    """
    super(BaseRelationalDb, self).__init__()
    self.spec = relational_db_spec
    if not FLAGS.use_managed_db:
      if self.spec.high_availability:
        raise UnsupportedError('High availability is unsupported for unmanaged '
                               'databases.')
      self.endpoint = ''
      self.spec.database_username = 'root'
      self.spec.database_password = 'perfkitbenchmarker'
      self.innodb_buffer_pool_size = FLAGS.innodb_buffer_pool_size
      self.innodb_log_file_size = FLAGS.innodb_log_file_size
      self.is_managed_db = False
    else:
      self.is_managed_db = True

  @property
  def client_vm(self):
    """Client VM which will drive the database test.

    This is required by subclasses to perform client-vm
    network-specific tasks, such as getting information about
    the VPC, IP address, etc.

    Raises:
      RelationalDbPropertyNotSet: if the client_vm is missing.

    Returns:
      The client_vm.
    """
    if not hasattr(self, '_client_vm'):
      raise RelationalDbPropertyNotSet('client_vm is not set')
    return self._client_vm

  @client_vm.setter
  def client_vm(self, client_vm):
    self._client_vm = client_vm

  @property
  def server_vm(self):
    """Server VM for hosting a managed database.

    Raises:
      RelationalDbPropertyNotSet: if the server_vm is missing.

    Returns:
      The server_vm.
    """
    if not hasattr(self, '_server_vm'):
      raise RelationalDbPropertyNotSet('server_vm is not set')
    return self._server_vm

  @server_vm.setter
  def server_vm(self, server_vm):
    self._server_vm = server_vm

  def SetVms(self, vm_groups):
    self.client_vm = vm_groups['clients' if 'clients' in
                               vm_groups else 'default'][0]
    if not self.is_managed_db and 'servers' in vm_groups:
      self.server_vm = vm_groups['servers'][0]
      if not self.innodb_buffer_pool_size:
        self.innodb_buffer_pool_size = self.server_vm.NumCpusForBenchmark()
    # TODO(jerlawson): Enable replications.

  def MakePsqlConnectionString(self, database_name):
    return '\'host={0} user={1} password={2} dbname={3}\''.format(
        self.endpoint,
        self.spec.database_username,
        self.spec.database_password,
        database_name)

  def MakeMysqlConnectionString(self, use_localhost=False):
    return '-h {0}{1} -u {2} -p{3}'.format(
        self.endpoint if not use_localhost else 'localhost',
        ' -P 3306' if not self.is_managed_db else '',
        self.spec.database_username, self.spec.database_password)

  def MakeSysbenchConnectionString(self):
    return (
        '--mysql-host={0}{1} --mysql-user={2} --mysql-password="{3}" ').format(
            self.endpoint,
            ' --mysql-port=3306' if not self.is_managed_db else '',
            self.spec.database_username, self.spec.database_password)

  @property
  def endpoint(self):
    """Endpoint of the database server (exclusing port)."""
    if not hasattr(self, '_endpoint'):
      raise RelationalDbPropertyNotSet('endpoint not set')
    return self._endpoint

  @endpoint.setter
  def endpoint(self, endpoint):
    self._endpoint = endpoint

  @property
  def port(self):
    """Port (int) on which the database server is listening."""
    if not hasattr(self, '_port'):
      raise RelationalDbPropertyNotSet('port not set')
    return self._port

  @port.setter
  def port(self, port):
    self._port = int(port)

  def GetResourceMetadata(self):
    """Returns a dictionary of metadata.

    Child classes can extend this if needed.

    Raises:
       RelationalDbPropertyNotSet: if any expected metadata is missing.
    """
    metadata = {
        'zone': self.spec.db_spec.zone,
        'disk_type': self.spec.db_disk_spec.disk_type,
        'disk_size': self.spec.db_disk_spec.disk_size,
        'engine': self.spec.engine,
        'high_availability': self.spec.high_availability,
        'backup_enabled': self.spec.backup_enabled,
        'backup_start_time': self.spec.backup_start_time,
        'engine_version': self.spec.engine_version,
        'client_vm_zone': self.spec.vm_groups['clients'].vm_spec.zone,
        'use_managed_db': self.is_managed_db,
        'instance_id': self.instance_id,
        'client_vm_disk_type':
            self.spec.vm_groups['clients'].disk_spec.disk_type,
        'client_vm_disk_size':
            self.spec.vm_groups['clients'].disk_spec.disk_size,
    }

    if not self.is_managed_db:
      metadata.update({
          'unmanaged_db_innodb_buffer_pool_size_gb':
              self.innodb_buffer_pool_size,
          'unmanaged_db_innodb_log_file_size_mb':
              self.innodb_log_file_size,
      })

    if (hasattr(self.spec.db_spec, 'machine_type') and
        self.spec.db_spec.machine_type):
      metadata.update({
          'machine_type': self.spec.db_spec.machine_type,
      })
    elif hasattr(self.spec.db_spec, 'cpus') and (hasattr(
        self.spec.db_spec, 'memory')):
      metadata.update({
          'cpus': self.spec.db_spec.cpus,
      })
      metadata.update({
          'memory': self.spec.db_spec.memory,
      })
    elif hasattr(self.spec.db_spec, 'tier') and (hasattr(
        self.spec.db_spec, 'compute_units')):
      metadata.update({
          'tier': self.spec.db_spec.tier,
      })
      metadata.update({
          'compute_units': self.spec.db_spec.compute_units,
      })
    else:
      raise RelationalDbPropertyNotSet(
          'Machine type of the database must be set.')

    if (hasattr(self.spec.vm_groups['clients'].vm_spec, 'machine_type') and
        self.spec.vm_groups['clients'].vm_spec.machine_type):
      metadata.update({
          'client_vm_machine_type':
              self.spec.vm_groups['clients'].vm_spec.machine_type,
      })
    elif hasattr(self.spec.vm_groups['clients'].vm_spec, 'cpus') and (hasattr(
        self.spec.vm_groups['clients'].vm_spec, 'memory')):
      metadata.update({
          'client_vm_cpus': self.spec.vm_groups['clients'].vm_spec.cpus,
      })
      metadata.update({
          'client_vm_memory': self.spec.vm_groups['clients'].vm_spec.memory,
      })
    else:
      raise RelationalDbPropertyNotSet(
          'Machine type of the client VM must be set.')

    if FLAGS.db_flags:
      metadata.update({
          'db_flags': FLAGS.db_flags,
      })

    return metadata

  @abstractmethod
  def GetDefaultEngineVersion(self, engine):
    """Return the default version (for PKB) for the given database engine.

    Args:
      engine: name of the database engine

    Returns: default version as a string for the given engine.
    """

  def _PostCreate(self):
    self._ApplyDbFlags()

  def _IsReadyUnmanaged(self):
    """Return true if the underlying resource is ready.

    Returns:
      True if MySQL was installed successfully, False if not.

    Raises:
      Exception: If this method is called when the database is a managed one.
        Shouldn't happen.
    """
    if self.is_managed_db:
      raise Exception('Checking state of unmanaged database when the database '
                      'is managed.')
    if (self.spec.engine_version == '5.6' or
        self.spec.engine_version.startswith('5.6.')):
      mysql_name = 'mysql56'
    elif (self.spec.engine_version == '5.7' or
          self.spec.engine_version.startswith('5.7.')):
      mysql_name = 'mysql57'
    elif (self.spec.engine_version == '8.0' or
          self.spec.engine_version.startswith('8.0.')):
      mysql_name = 'mysql80'
    else:
      raise Exception('Invalid database engine version: %s. Only 5.6 and 5.7 '
                      'and 8.0 are supported.' % self.spec.engine_version)
    stdout, stderr = self.server_vm.RemoteCommand(
        'sudo service %s status' % self.server_vm.GetServiceName(mysql_name))
    return stdout and not stderr

  def _InstallMySQLClient(self):
    """Installs MySQL Client on the client vm.

    Raises:
      Exception: If the requested engine version is unsupported.
    """
    if (self.spec.engine_version == '5.6' or
        self.spec.engine_version.startswith('5.6.')):
      mysql_name = 'mysqlclient56'
    elif (self.spec.engine_version == '5.7' or
          self.spec.engine_version.startswith('5.7') or
          self.spec.engine_version == '8.0' or
          self.spec.engine_version.startswith('8.0')):
      mysql_name = 'mysqlclient'
    else:
      raise Exception('Invalid database engine version: %s. Only 5.6, 5.7 '
                      'and 8.0 are supported.' % self.spec.engine_version)
    self.client_vm.Install(mysql_name)
    self.client_vm.RemoteCommand(
        'sudo sed -i '
        '"s/max_allowed_packet\t= 16M/max_allowed_packet\t= 1024M/g" %s' %
        self.client_vm.GetPathToConfig(mysql_name))
    self.client_vm.RemoteCommand(
        'sudo cat %s' % self.client_vm.GetPathToConfig(mysql_name),
        should_log=True)

  def _PrepareDataDirectories(self, mysql_name):
    # Make the data directories in case they don't already exist.
    self.server_vm.RemoteCommand('sudo mkdir -p /scratch/mysql')
    self.server_vm.RemoteCommand('sudo mkdir -p /scratch/tmp')
    self.server_vm.RemoteCommand('sudo chown mysql:mysql /scratch/mysql')
    self.server_vm.RemoteCommand('sudo chown mysql:mysql /scratch/tmp')
    # Copy all the contents of the default data directories to the new ones.
    self.server_vm.RemoteCommand(
        'sudo rsync -avzh /var/lib/mysql/ /scratch/mysql')
    self.server_vm.RemoteCommand('sudo rsync -avzh /tmp/ /scratch/tmp')
    self.server_vm.RemoteCommand('df', should_log=True)
    # Configure AppArmor.
    self.server_vm.RemoteCommand(
        'echo "alias /var/lib/mysql -> /scratch/mysql," | sudo tee -a '
        '/etc/apparmor.d/tunables/alias')
    self.server_vm.RemoteCommand(
        'echo "alias /tmp -> /scratch/tmp," | sudo tee -a '
        '/etc/apparmor.d/tunables/alias')
    self.server_vm.RemoteCommand(
        'sudo sed -i '
        '"s|# Allow data files dir access|'
        '  /scratch/mysql/ r, /scratch/mysql/** rwk, /scratch/tmp/ r, '
        '/scratch/tmp/** rwk, /proc/*/status r, '
        '/sys/devices/system/node/ r, /sys/devices/system/node/node*/meminfo r,'
        ' /sys/devices/system/node/*/* r, /sys/devices/system/node/* r, '
        '# Allow data files dir access|g" /etc/apparmor.d/usr.sbin.mysqld')
    self.server_vm.RemoteCommand(
        'sudo apparmor_parser -r /etc/apparmor.d/usr.sbin.mysqld')
    self.server_vm.RemoteCommand('sudo systemctl restart apparmor')
    # Finally, change the MySQL data directory.
    self.server_vm.RemoteCommand(
        'sudo sed -i '
        '"s|datadir\t\t= /var/lib/mysql|datadir\t\t= /scratch/mysql|g" '
        '%s' % self.server_vm.GetPathToConfig(mysql_name))
    self.server_vm.RemoteCommand(
        'sudo sed -i '
        '"s|tmpdir\t\t= /tmp|tmpdir\t\t= /scratch/tmp|g" '
        '%s' % self.server_vm.GetPathToConfig(mysql_name))

  def _InstallMySQLServer(self):
    """Installs MySQL Server on the server vm.

    https://d0.awsstatic.com/whitepapers/Database/optimizing-mysql-running-on-amazon-ec2-using-amazon-ebs.pdf
    for minimal tuning parameters.

    Raises:
      Exception: If the requested engine version is unsupported, or if this
        method is called when the database is a managed one. The latter
        shouldn't happen.
    """
    if self.is_managed_db:
      raise Exception('Can\'t install MySQL Server when using a managed '
                      'database.')
    if self.client_vm.IS_REBOOTABLE:
      self.client_vm.ApplySysctlPersistent({
          'net.ipv4.tcp_keepalive_time': 100,
          'net.ipv4.tcp_keepalive_intvl': 100,
          'net.ipv4.tcp_keepalive_probes': 10
      })
    if self.server_vm.IS_REBOOTABLE:
      self.server_vm.ApplySysctlPersistent({
          'net.ipv4.tcp_keepalive_time': 100,
          'net.ipv4.tcp_keepalive_intvl': 100,
          'net.ipv4.tcp_keepalive_probes': 10
      })
    if (self.spec.engine_version == '5.6' or
        self.spec.engine_version.startswith('5.6.')):
      mysql_name = 'mysql56'
    elif (self.spec.engine_version == '5.7' or
          self.spec.engine_version.startswith('5.7.')):
      mysql_name = 'mysql57'
    elif (self.spec.engine_version == '8.0' or
          self.spec.engine_version.startswith('8.0.')):
      mysql_name = 'mysql80'
    else:
      raise Exception('Invalid database engine version: %s. Only 5.6 and 5.7 '
                      'and 8.0 are supported.' % self.spec.engine_version)
    self.server_vm.Install(mysql_name)
    self.server_vm.RemoteCommand('chmod 777 %s' %
                                 self.server_vm.GetScratchDir())
    self.server_vm.RemoteCommand('sudo service %s stop' %
                                 self.server_vm.GetServiceName(mysql_name))
    self._PrepareDataDirectories(mysql_name)

    # Minimal MySQL tuning; see AWS whitepaper in docstring.
    innodb_buffer_pool_gb = self.innodb_buffer_pool_size
    innodb_log_file_mb = self.innodb_log_file_size

    self.server_vm.RemoteCommand(
        'echo "\n'
        f'innodb_buffer_pool_size = {innodb_buffer_pool_gb}G\n'
        'innodb_flush_method = O_DIRECT\n'
        'innodb_flush_neighbors = 0\n'
        f'innodb_log_file_size = {innodb_log_file_mb}M'
        '" | sudo tee -a %s' % self.server_vm.GetPathToConfig(mysql_name))

    # These (and max_connections after restarting) help avoid losing connection.
    self.server_vm.RemoteCommand(
        'echo "\nskip-name-resolve\n'
        'connect_timeout        = 86400\n'
        'wait_timeout        = 86400\n'
        'interactive_timeout        = 86400" | sudo tee -a %s' %
        self.server_vm.GetPathToConfig(mysql_name))
    self.server_vm.RemoteCommand('sudo sed -i "s/bind-address/#bind-address/g" '
                                 '%s' %
                                 self.server_vm.GetPathToConfig(mysql_name))
    self.server_vm.RemoteCommand(
        'sudo sed -i '
        '"s/max_allowed_packet\t= 16M/max_allowed_packet\t= 1024M/g" %s' %
        self.server_vm.GetPathToConfig(mysql_name))

    # Configure logging (/var/log/mysql/error.log will print upon db deletion).
    self.server_vm.RemoteCommand(
        'echo "\nlog_error_verbosity        = 3" | sudo tee -a %s' %
        self.server_vm.GetPathToConfig(mysql_name))
    self.server_vm.RemoteCommand(
        'sudo cat /etc/mysql/mysql.conf.d/mysql.sock',
        should_log=True,
        ignore_failure=True)
    # Restart.
    self.server_vm.RemoteCommand('sudo service %s restart' %
                                 self.server_vm.GetServiceName(mysql_name))
    self.server_vm.RemoteCommand(
        'sudo cat %s' % self.server_vm.GetPathToConfig(mysql_name),
        should_log=True)

    self.server_vm.RemoteCommand(
        'mysql %s -e "SET GLOBAL max_connections=8000;"' %
        self.MakeMysqlConnectionString(use_localhost=True))
    if FLAGS.ip_addresses == vm_util.IpAddressSubset.INTERNAL:
      client_ip = self.client_vm.internal_ip
    else:
      client_ip = self.client_vm.ip_address
    self.server_vm.RemoteCommand(
        ('mysql %s -e "CREATE USER \'%s\'@\'%s\' IDENTIFIED BY \'%s\';"') %
        (self.MakeMysqlConnectionString(use_localhost=True),
         self.spec.database_username, client_ip, self.spec.database_password))
    self.server_vm.RemoteCommand(
        ('mysql %s -e "GRANT ALL PRIVILEGES ON *.* TO \'%s\'@\'%s\';"') %
        (self.MakeMysqlConnectionString(use_localhost=True),
         self.spec.database_username, client_ip))
    self.server_vm.RemoteCommand(
        'mysql %s -e "FLUSH PRIVILEGES;"' %
        self.MakeMysqlConnectionString(use_localhost=True))

  def _ApplyDbFlags(self):
    """Apply Flags on the database."""
    if FLAGS.db_flags:
      if self.is_managed_db:
        self._ApplyManagedDbFlags()
      else:
        if self.spec.engine == MYSQL:
          self._ApplyMySqlFlags()
        else:
          raise NotImplementedError('Flags is not supported on %s' %
                                    self.spec.engine)

  def _ApplyManagedDbFlags(self):
    """Apply flags on the managed database."""
    raise NotImplementedError('Managed Db flags is not supported for %s' %
                              type(self).__name__)

  def _ApplyMySqlFlags(self):
    if FLAGS.db_flags:
      for flag in FLAGS.db_flags:
        cmd = 'mysql %s -e \'SET %s;\'' % self.MakeMysqlConnectionString(), flag
        _, stderr, _ = vm_util.IssueCommand(cmd, raise_on_failure=False)
        if stderr:
          raise Exception('Invalid MySQL flags: %s' % stderr)

  def Failover(self):
    """Fail over the database.  Throws exception if not high available."""
    if not self.spec.high_availability:
      raise Exception('Attempt to fail over a database that isn\'t marked '
                      'as high available')
    self._FailoverHA()

  @abstractmethod
  def _FailoverHA(self):
    """Fail over from master to replica."""
    pass
