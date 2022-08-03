# Copyright (c) 2012-2019 VMware, Inc.  All rights reserved.
# -- VMware Confidential

"""
cayman_antrea gobuild product module.
"""

import os
import helpers.env
import helpers.python
import helpers.target
import specs.cayman_antrea


class _CaymanAntrea(helpers.target.Target, helpers.python.CaymanPythonHelper):
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

        paths = [
            "%(gobuild_component_cayman_python_root)/lin64/bin",
            "%(gobuild_component_cayman_openssl_root)/lin64/bin",
        ]

        tcroot = os.environ.get('TCROOT', '/build/toolchain')

        paths.extend([os.path.join(tcroot, 'lin64', path)
                      for path in ['coreutils-5.97/bin',
                                   'findutils-4.2.27/bin',
                                   "git-1.8.3-1/bin",
                                   'grep-2.5.1a/bin',]])
        paths.append("/build/toolchain/noarch/vmware/gpgsign/")
        paths.append(env['PATH'])
        env['PATH'] = os.pathsep.join(paths)
        env['LD_LIBRARY_PATH'] = os.pathsep.join([
            "%(gobuild_component_cayman_python_root)/lin64/lib",
            "%(gobuild_component_cayman_openssl_root)/lin64/lib64",
        ])

        return env

    def _Command(self, hosttype, product, target='install', args={}):
        entry = 'cayman_antrea/antrea/bootstrap.py'
        print ("_Command cayman_antrea/antrea/bootstrap.py")
        print ("%(gobuild_component_docker_tool_root)")

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
        comps = {
            'cayman': {
                'branch': specs.cayman_antrea.CAYMAN_BRANCH,
                'change': specs.cayman_antrea.CAYMAN_CLN,
                'buildtype': specs.cayman_antrea.CAYMAN_BUILDTYPE,
                'hosttypes': specs.cayman_antrea.CAYMAN_HOSTTYPES},
            "cayman_python": {
                "branch": specs.cayman_antrea.CAYMAN_PYTHON_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_PYTHON_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_PYTHON_BUILDTYPE,
                "hosttypes": specs.cayman_antrea.CAYMAN_PYTHON_HOSTTYPES},
            "cayman_openssl": {
                "branch": specs.cayman_antrea.CAYMAN_OPENSSL_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_OPENSSL_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_OPENSSL_BUILDTYPE,
                "hosttypes": specs.cayman_antrea.CAYMAN_OPENSSL_HOSTTYPES},
            "csc-photon": {
                "branch": specs.cayman_antrea.CSC_PHOTON_BRANCH,
                "change": specs.cayman_antrea.CSC_PHOTON_CLN,
                "buildtype": specs.cayman_antrea.CSC_PHOTON_BUILDTYPE,
                "files": specs.cayman_antrea.CSC_PHOTON_FILES},
            "cayman_cni_plugins": {
                "branch": specs.cayman_antrea.CAYMAN_CNI_PLUGINS_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_CNI_PLUGINS_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_CNI_PLUGINS_BUILDTYPE,
                "files": specs.cayman_antrea.CAYMAN_CNI_PLUGINS_FILES},
            "cayman_go": {
                "branch": specs.cayman_antrea.CAYMAN_GO_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_GO_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_GO_BUILDTYPE,
                "files": specs.cayman_antrea.CAYMAN_GO_FILES},
            "nsx-ovs-build": {
                "branch": specs.cayman_antrea.NSX_OVS_BUILD_BRANCH,
                "change": specs.cayman_antrea.NSX_OVS_BUILD_CLN,
                "buildtype": specs.cayman_antrea.NSX_OVS_BUILD_BUILDTYPE,
                "files": specs.cayman_antrea.NSX_OVS_BUILD_FILES},
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
        return {'name': 'cayman_antrea',
                'longname': 'cayman_antrea'}

    def GetCommands(self, hosttype):
        products = CaymanAntrea.product_map[hosttype]
        return [self._Command(hosttype=hosttype, product=product, args={"BUILD_PRODUCT":"cayman_antrea"}) for product in products]

    def GetComponentPath(self):
        return '%(buildroot)/publish'


class CaymanAntreaTKGMAdv(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    product_map = {
        specs.cayman_antrea.LINUX_HOSTTYPE: ['lin64'],
    }

    def GetClusterRequirements(self):
        return CaymanAntrea.product_map.keys()

    def GetBuildProductNames(self):
        return {'name': 'cayman_antrea_tkgm-advanced',
                'longname': 'cayman_antrea_tkgm-advanced'}

    def GetCommands(self, hosttype):
        products = CaymanAntrea.product_map[hosttype]
        #hosttype, product
        return [self._Command(hosttype=hosttype, product=product, args={"BUILD_PRODUCT":"cayman_antrea_tkgm-advanced"}) for product in products]

    def GetComponentPath(self):
        return '%(buildroot)/publish'


class CaymanAntreaTKGMStd(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    product_map = {
        specs.cayman_antrea.LINUX_HOSTTYPE: ['lin64'],
    }

    def GetClusterRequirements(self):
        return CaymanAntrea.product_map.keys()

    def GetBuildProductNames(self):
        return {'name': 'cayman_antrea_tkgm-standard',
                'longname': 'cayman_antrea_tkgm-standard'}

    def GetCommands(self, hosttype):
        products = CaymanAntrea.product_map[hosttype]
        return [self._Command(hosttype=hosttype, product=product, args={"BUILD_PRODUCT":"cayman_antrea_tkgm-standard"}) for product in products]

    def GetComponentPath(self):
        return '%(buildroot)/publish'


class CaymanAntreaTKGSAdv(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    product_map = {
        specs.cayman_antrea.LINUX_HOSTTYPE: ['lin64'],
    }

    def GetClusterRequirements(self):
        return CaymanAntrea.product_map.keys()

    def GetBuildProductNames(self):
        return {'name': 'cayman_antrea_tkgs-advanced',
                'longname': 'cayman_antrea_tkgs-advanced'}

    def GetCommands(self, hosttype):
        products = CaymanAntrea.product_map[hosttype]
        return [self._Command(hosttype=hosttype, product=product, args={"BUILD_PRODUCT":"cayman_antrea_tkgs-advanced"}) for product in products]

    def GetComponentPath(self):
        return '%(buildroot)/publish'


class CaymanAntreaMultiCluster(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    product_map = {
        specs.cayman_antrea.LINUX_HOSTTYPE: ['lin64'],
    }

    def GetClusterRequirements(self):
        return CaymanAntrea.product_map.keys()

    def GetBuildProductNames(self):
        return {'name': 'cayman_antrea_multi-cluster',
                'longname': 'cayman_antrea_multi-cluster'}

    def GetCommands(self, hosttype):
        products = CaymanAntrea.product_map[hosttype]
        return [self._Command(hosttype=hosttype, product=product, args={"BUILD_PRODUCT":"cayman_antrea_multi-cluster"}) for product in products]

    def GetComponentPath(self):
        return '%(buildroot)/publish'

    def GetComponentDependencies(self):
        buildtype = self.options.get('buildtype')
        comps = {
            'cayman': {
                'branch': specs.cayman_antrea.CAYMAN_BRANCH,
                'change': specs.cayman_antrea.CAYMAN_CLN,
                'buildtype': specs.cayman_antrea.CAYMAN_BUILDTYPE,
                'hosttypes': specs.cayman_antrea.CAYMAN_HOSTTYPES},
            "cayman_python": {
                "branch": specs.cayman_antrea.CAYMAN_PYTHON_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_PYTHON_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_PYTHON_BUILDTYPE,
                "hosttypes": specs.cayman_antrea.CAYMAN_PYTHON_HOSTTYPES},
            "cayman_go": {
                "branch": specs.cayman_antrea.CAYMAN_GO_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_GO_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_GO_BUILDTYPE,
                "files": specs.cayman_antrea.CAYMAN_GO_FILES},
            "cayman_openssl": {
                "branch": specs.cayman_antrea.CAYMAN_OPENSSL_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_OPENSSL_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_OPENSSL_BUILDTYPE,
                "hosttypes": specs.cayman_antrea.CAYMAN_OPENSSL_HOSTTYPES},
        }

        return comps

class CaymanAntreaIPsec(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    product_map = {
        specs.cayman_antrea.LINUX_HOSTTYPE: ['lin64'],
    }

    def GetClusterRequirements(self):
        return CaymanAntrea.product_map.keys()

    def GetBuildProductNames(self):
        return {'name': 'cayman_antrea_ipsec',
                'longname': 'cayman_antrea_ipsec'}

    def GetCommands(self, hosttype):
        products = CaymanAntrea.product_map[hosttype]
        return [self._Command(hosttype=hosttype, product=product, args={"BUILD_PRODUCT":"cayman_antrea_ipsec"}) for product in products]

    def GetComponentPath(self):
        return '%(buildroot)/publish'