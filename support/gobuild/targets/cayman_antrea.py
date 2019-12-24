# Copyright (c) 2012-2019 VMware, Inc.  All rights reserved.
# -- VMware Confidential

"""
cayman_antrea gobuild product module.
"""

import os
import helpers.env
import helpers.python
import helpers.target
import helpers.msvc
import specs.cayman_antrea


class _CaymanAntrea(helpers.target.Target, helpers.python.PythonHelper):
    """
    CaymanAntrea Open Source component
    """

    def GetRepositories(self, hosttype):
        repos = [{
            'rcs': 'git',
            'src': 'core-build/cayman_antrea;%(branch);',
            'dst': 'cayman_antrea',
        }]
        return repos

    def _Environment(self, hosttype):
        env = helpers.env.SafeEnvironment(hosttype)

        if hosttype.startswith('windows'):
            return env  # done

        tcroot = os.environ.get('TCROOT', '/build/toolchain')

        paths = [os.path.join(tcroot, 'lin32', path)
                 for path in ['python-2.7.1/bin',
                              'coreutils-5.97/bin',
                              'findutils-4.2.27/bin',
                              'grep-2.5.1a/bin']]
        paths.append(env['PATH'])
        env['PATH'] = os.pathsep.join(paths)

        return env

    def _Command(self, hosttype, product, target='install', args={}):
        entry = 'cayman_antrea/antrea/bootstrap.py'

        return {'desc': 'Compiling target CaymanAntrea %s' % product,
                'root': '%(buildroot)/cayman_antrea/antrea',
                'log': 'antrea.log',
                'command': self._PythonCommand(hosttype, product,
                                               entry, target,
                                               arguments=args),
                'env': self._Environment(hosttype),
                }

    def GetStorageInfo(self, hosttype):
        storages = []
        if hosttype.startswith('linux'):
            # Linux side is responsible for copying the source files to storage
            storages.append({'type': 'source', 'src': 'cayman_antrea'})
        storages.append({'type': 'build', 'src': 'cayman_antrea/build'})
        return storages

    def GetBuildProductVersion(self, hosttype):
        # If the boostrap.py file sets PROVENANCE_FILE pass
        # "provenance_file='foo.yaml'" here.
        return self.ExtractVersionFromProvenanceMetadata()

    def GetComponentDependencies(self):
        buildtype = self.options.get('buildtype')
        comps = {}
        comps['cayman'] = {
            'branch':    specs.cayman_antrea.CAYMAN_BRANCH,
            'change':    specs.cayman_antrea.CAYMAN_CLN,
            'buildtype': specs.cayman_antrea.CAYMAN_BUILDTYPE,
            'hosttypes': specs.cayman_antrea.CAYMAN_HOSTTYPES,
        }

        return comps


class CaymanAntrea(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    product_map = {
        specs.cayman_antrea.LINUX_HOSTTYPE: ['lin64'],
    }

    def GetClusterRequirements(self):
        return CaymanAntrea.product_map.keys()

    def GetBuildProductNames(self):
        return {'name':     'cayman_antrea',
                'longname': 'cayman_antrea'}

    def GetCommands(self, hosttype):
        products = CaymanAntrea.product_map[hosttype]
        return [self._Command(hosttype, product) for product in products]

    def GetComponentPath(self):
        return '%(buildroot)/publish'
