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

        comps['cayman_cmake'] = {
            'branch':    specs.cayman_antrea.CAYMAN_CMAKE_BRANCH,
            'change':    specs.cayman_antrea.CAYMAN_CMAKE_CLN,
            'buildtype': specs.cayman_antrea.CAYMAN_CMAKE_BUILDTYPE,
            'hosttypes': specs.cayman_antrea.CAYMAN_CMAKE_HOSTTYPES,
        }

        comps['cayman_ninja'] = {
            'branch':    specs.cayman_antrea.CAYMAN_NINJA_BRANCH,
            'change':    specs.cayman_antrea.CAYMAN_NINJA_CLN,
            'buildtype': specs.cayman_antrea.CAYMAN_NINJA_BUILDTYPE,
            'hosttypes': specs.cayman_antrea.CAYMAN_NINJA_HOSTTYPES,
        }

        comps['cayman_llvm'] = {
            'branch':    specs.cayman_antrea.CAYMAN_LLVM_BRANCH,
            'change':    specs.cayman_antrea.CAYMAN_LLVM_CLN,
            'buildtype': specs.cayman_antrea.CAYMAN_LLVM_BUILDTYPE,
            'hosttypes': specs.cayman_antrea.CAYMAN_LLVM_HOSTTYPES,
        }

        comps['cayman_esx_glibc'] = [{
            'branch':    specs.cayman_antrea.CAYMAN_ESX_GLIBC_BRANCH,
            'change':    specs.cayman_antrea.CAYMAN_ESX_GLIBC_CLN,
            'buildtype': specs.cayman_antrea.CAYMAN_ESX_GLIBC_BUILDTYPE,
            'hosttypes': specs.cayman_antrea.CAYMAN_ESX_GLIBC_HOSTTYPES,
        }, {
            'alias': 'cayman_esx_glibc_2_17',
            'branch':    specs.cayman_antrea.CAYMAN_ESX_GLIBC_2_17_BRANCH,
            'change':    specs.cayman_antrea.CAYMAN_ESX_GLIBC_2_17_CLN,
            'buildtype': specs.cayman_antrea.CAYMAN_ESX_GLIBC_2_17_BUILDTYPE,
            'hosttypes': specs.cayman_antrea.CAYMAN_ESX_GLIBC_2_17_HOSTTYPES,
        }]

        comps['cayman_glibc'] = [{
            'alias': 'cayman_glibc_2_17',
            'branch':    specs.cayman_antrea.CAYMAN_GLIBC_2_17_BRANCH,
            'change':    specs.cayman_antrea.CAYMAN_GLIBC_2_17_CLN,
            'buildtype': specs.cayman_antrea.CAYMAN_GLIBC_2_17_BUILDTYPE,
            'hosttypes': specs.cayman_antrea.CAYMAN_GLIBC_2_17_HOSTTYPES,
        }]

        comps['cayman_esx_toolchain'] = [{
            'branch':    specs.cayman_antrea.CAYMAN_ESX_TOOLCHAIN_BRANCH,
            'change':    specs.cayman_antrea.CAYMAN_ESX_TOOLCHAIN_CLN,
            'buildtype': specs.cayman_antrea.CAYMAN_ESX_TOOLCHAIN_BUILDTYPE,
            'hosttypes': specs.cayman_antrea.CAYMAN_ESX_TOOLCHAIN_HOSTTYPES,
        }, {
            'alias': 'cayman_esx_toolchain_gcc6',
            'branch':    specs.cayman_antrea.CAYMAN_ESX_TOOLCHAIN_GCC6_BRANCH,     # nopep8
            'change':    specs.cayman_antrea.CAYMAN_ESX_TOOLCHAIN_GCC6_CLN,        # nopep8
            'buildtype': specs.cayman_antrea.CAYMAN_ESX_TOOLCHAIN_GCC6_BUILDTYPE,  # nopep8
            'hosttypes': specs.cayman_antrea.CAYMAN_ESX_TOOLCHAIN_GCC6_HOSTTYPES,  # nopep8
        }]

        comps['cayman_python'] = [{
            'alias': 'cayman_python',
            'branch': specs.cayman_antrea.CAYMAN_PYTHON3_BRANCH,
            'change': specs.cayman_antrea.CAYMAN_PYTHON3_CLN,
            'buildtype': specs.cayman_antrea.CAYMAN_PYTHON3_BUILDTYPE,
            'hosttypes': specs.cayman_antrea.CAYMAN_PYTHON3_HOSTTYPES,
            'cacheflags': specs.cayman_antrea.CAYMAN_PYTHON3_CACHEFLAGS,
        },
            {
            'alias': 'cayman_python2',
            'branch': specs.cayman_antrea.CAYMAN_PYTHON_BRANCH,
            'change': specs.cayman_antrea.CAYMAN_PYTHON_CLN,
            'buildtype': specs.cayman_antrea.CAYMAN_PYTHON_BUILDTYPE,
            'hosttypes': specs.cayman_antrea.CAYMAN_PYTHON_HOSTTYPES,
            'cacheflags': specs.cayman_antrea.CAYMAN_PYTHON_CACHEFLAGS,
        }]

        # MSVC components
        helpers.msvc.AddCaymanMsvcComponents(comps, buildtype)

        return comps


class CaymanAntrea(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    product_map = {
        'linux64': [
            'lin32',             'lin64',
            'lin32+gcc48',       'lin64+gcc48',
            'lin32+gcc48+clang', 'lin64+gcc48+clang',
            'lin64+gcc48+clang+asan',
            'lin32+gcc6',        'lin64+gcc6',
            'lin32+gcc6+clang',  'lin64+gcc6+clang',
            'esx32+gcc48',       'esx64+gcc48',
            'esx32+gcc6',        'esx64+gcc6',   'esx-aarch64+gcc6',

            # 32-bit armv7, hardfloat. eg. Cortex A5, A7, A8, A9, A15, A17...
            'lin-arm7a-hf',      'lin-arm7a-hf+glibc217+gcc6',

            # Deprecated. Use +gcc48 instead of -cayman.
            'lin32-cayman',      'lin64-cayman',
            'esx-cayman',        'esx64-cayman',
        ],

        'macosx-elcapitan': ['apple_mac64'],

        'windows-2008': [
            'win32_vc90sp1', 'win64_vc90sp1',
            # deprecated
            'win32', 'win64',
        ],
        'windows-2016-vs2013-U5': ['win32_vc120', 'win64_vc120'],
        'windows2016-clean': ['win32_vc140', 'win64_vc140', ],
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
