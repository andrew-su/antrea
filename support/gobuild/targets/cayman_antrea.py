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

BRANCH_NAME = "%(branch)"
BUILDROOT = "%(buildroot)"
PROJECT_DIR = "%s/cayman_antrea" % (BUILDROOT)
PUBLISH_DIR = "%s/publish" %(BUILDROOT)
BUILDNUMBER = "%(buildnumber)"

# sudoers_extra_permissions defines extra sudo permissions for builds.
# This is required when 'root_needed' is false in 'default_protections' flag.
sudoers_extra_permissions = [
    {
        'root_needed': False,
        'sudoers_file': '%(buildroot)/cayman_antrea/support/gobuild/root/sudoers_extra_permissions',
        'sudoers_rank': 20,
    },
]

common_product_flags = {
                'srp_observer': {
                    'debug_logging': True
                },
                'https_observer': {
                    'enabled': True,
                },
                'fs_observer': {
                    'enabled': True
                },
                'git_observer': {
                    'enabled': True
                },
                'default_protections': [
                    {
                        'root_needed': False
                    },
                    {
                        'sysctls_key': 'kernel.yama.ptrace_scope',
                        'sysctls_value': '3'
                    }
                ]
        }

product_map = {
    specs.cayman_antrea.LINUX_HOSTTYPE: common_product_flags,
}

dist_map = {
    specs.cayman_antrea.LINUX_HOSTTYPE: ['lin64'],
}

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
        env["SHELL"] = "/build/toolchain/lin64/bash-4.1/bin/bash"
        paths = [
            "%(gobuild_component_cayman_python_root)/lin64+gcc6/bin",
            "%(gobuild_component_cayman_openssl_root)/lin64+gcc6/usr/bin",
        ]

        paths += ["/build/toolchain/lin64/coreutils-8.6/bin"]
        paths += ["/usr/bin", "/bin", "/usr/local/sbin", "/usr/local/bin",
                  "/usr/sbin", "/sbin"]
        paths += ['/build/toolchain/lin64/wget-1.19.2-openssl1.0.2l/bin']
        paths += ['/build/toolchain/noarch/vmware/gpgsign/']

        tcroot = os.environ.get('TCROOT', '/build/toolchain')
        paths.extend([os.path.join(tcroot, 'lin64', path)
                      for path in ['coreutils-5.97/bin',
                                   'findutils-4.2.27/bin',
                                   "git-1.8.3-1/bin",
                                   'grep-2.5.1a/bin',
                                   'bash-4.1/bin']])
        env["PATH"] = os.pathsep.join(paths + [env["PATH"]])

        env['LD_LIBRARY_PATH'] = os.pathsep.join([
            "%(gobuild_component_cayman_python_root)/lin64+gcc6/lib",
            "%(gobuild_component_cayman_openssl_root)/lin64+gcc6/usr/lib64",
        ])

        for d in self.GetComponentDependencies():
            d = d.replace('-', '_')
            env['GOBUILD_%s_ROOT' % d.upper()] = '%%(gobuild_component_%s_root)' % d
        env["host_alias"] = "x86_64-linux"
        # Have to disable ssl verification since gobuild machine doesn't have
        # embedded cert bundle.
        env["GIT_SSL_NO_VERIFY"] = "false"
        del env["PYTHONDONTWRITEBYTECODE"]
        env["PYTHONIOENCODING"] = "UTF-8"
        env["LANG"] = "en_US.UTF-8"
        env["PROJECT_DIR"] = PROJECT_DIR
        env["PUBLISH_DIR"] = PUBLISH_DIR
        env["BRANCH_NAME"] = BRANCH_NAME
        env["BUILD_NUMBER"] = BUILDNUMBER
        env["BUILDROOT"] = BUILDROOT

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
                'extra_protections': sudoers_extra_permissions
                }

    def _WrapCommands(self, hosttype, commands):
        commands.insert(0, {
            "desc": "Configure Docker to use overlay storage and install tools",
            "root": "%(buildroot)",
            "log": "configure-buildenv.log",
            "command":'cd %(buildroot)/cayman_antrea; /bin/bash ./antrea/configure_buildenv.sh',
            "env": self._Environment(hosttype),
            'extra_protections': sudoers_extra_permissions
        })
        commands.append({
            "desc": "Cleanup Docker storage and local yum files",
            "root": "%(buildroot)",
            "log": "cleanup.log",
            "command":'cd %(buildroot)/cayman_antrea; /bin/bash ./antrea/cleanup.sh',
            "env": self._Environment(hosttype),
            'extra_protections': sudoers_extra_permissions
        })
        return commands

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
            "cayman_helm": {
                "branch": specs.cayman_antrea.CAYMAN_HELM_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_HELM_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_HELM_BUILDTYPE,
                "files": specs.cayman_antrea.CAYMAN_HELM_FILES},
            "cayman_kubernetes-sigs_kustomize": {
                "branch": specs.cayman_antrea.CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_BUILDTYPE,
                "files": specs.cayman_antrea.CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_FILES},
            "cayman_suricata":{
                "branch": specs.cayman_antrea.CAYMAN_SURICATA_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_SURICATA_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_SURICATA_BUILDTYPE,
                "files": specs.cayman_antrea.CAYMAN_SURICATA_FILES},
            "cayman_msvc_redists": {
                "branch": specs.cayman_antrea.CAYMAN_MSVC_REDISTS_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_MSVC_REDISTS_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_MSVC_REDISTS_BUILDTYPE,
                "files": specs.cayman_antrea.CAYMAN_MSVC_REDISTS_FILES},
        }

        return comps

    def GetProvenanceSchematics(self, hosttype):
        # Make sure to update the schematic file when you overwrite GetComponentDependencies function
        # with more components for this target.
        return [
            'cayman_antrea/support/gobuild/provenance/cayman_antrea.schematic.json',
            'cayman_antrea/support/gobuild/provenance/build.schematic.json'
        ]

class CaymanAntrea(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    def GetClusterRequirements(self):
        return product_map

    def GetBuildProductNames(self):
        return {'name': 'cayman_antrea',
                'longname': 'cayman_antrea'}

    def GetCommands(self, hosttype):
        products = dist_map[hosttype]
        commands = [self._Command(hosttype=hosttype, product=product, args={"BUILD_PRODUCT":"cayman_antrea"}) for product in products]
        return self._WrapCommands(hosttype, commands)

    def GetComponentPath(self):
        return '%(buildroot)/publish'

class CaymanAntreaTKGMAdv(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    def GetClusterRequirements(self):
        return product_map

    def GetBuildProductNames(self):
        return {'name': 'cayman_antrea_tkgm-advanced',
                'longname': 'cayman_antrea_tkgm-advanced'}

    def GetCommands(self, hosttype):
        products = dist_map[hosttype]
        commands = [self._Command(hosttype=hosttype, product=product, args={"BUILD_PRODUCT":"cayman_antrea_tkgm-advanced"}) for product in products]
        return self._WrapCommands(hosttype, commands)

    def GetComponentPath(self):
        return '%(buildroot)/publish'

class CaymanAntreaTKGSAdv(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    def GetClusterRequirements(self):
        return product_map

    def GetBuildProductNames(self):
        return {'name': 'cayman_antrea_tkgs-advanced',
                'longname': 'cayman_antrea_tkgs-advanced'}

    def GetCommands(self, hosttype):
        products = dist_map[hosttype]
        commands = [self._Command(hosttype=hosttype, product=product, args={"BUILD_PRODUCT":"cayman_antrea_tkgs-advanced"}) for product in products]
        return self._WrapCommands(hosttype, commands)

    def GetComponentPath(self):
        return '%(buildroot)/publish'

class CaymanAntreaMultiCluster(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    def GetClusterRequirements(self):
        return product_map

    def GetBuildProductNames(self):
        return {'name': 'cayman_antrea_multi-cluster',
                'longname': 'cayman_antrea_multi-cluster'}

    def GetCommands(self, hosttype):
        products = dist_map[hosttype]
        commands = [self._Command(hosttype=hosttype, product=product, args={"BUILD_PRODUCT":"cayman_antrea_multi-cluster"}) for product in products]
        return self._WrapCommands(hosttype, commands)

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
            "cayman_helm": {
                "branch": specs.cayman_antrea.CAYMAN_HELM_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_HELM_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_HELM_BUILDTYPE,
                "files": specs.cayman_antrea.CAYMAN_HELM_FILES},
            "cayman_kubernetes-sigs_kustomize": {
                "branch": specs.cayman_antrea.CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_BUILDTYPE,
                "files": specs.cayman_antrea.CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_FILES},
        }

        return comps

    def GetProvenanceSchematics(self, hosttype):
        return [
            'cayman_antrea/support/gobuild/provenance/cayman_antrea_multi-cluster.schematic.json',
            'cayman_antrea/support/gobuild/provenance/build.schematic.json'
        ]

class CaymanAntreaIPsec(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    def GetClusterRequirements(self):
        return product_map

    def GetBuildProductNames(self):
        return {'name': 'cayman_antrea_ipsec',
                'longname': 'cayman_antrea_ipsec'}

    def GetCommands(self, hosttype):
        products = dist_map[hosttype]
        commands = [self._Command(hosttype=hosttype, product=product, args={"BUILD_PRODUCT":"cayman_antrea_ipsec"}) for product in products]
        return self._WrapCommands(hosttype, commands)

    def GetComponentPath(self):
        return '%(buildroot)/publish'

class CaymanAntreaIDPS(_CaymanAntrea):
    """
    CaymanAntrea Open Source component
    """

    def GetClusterRequirements(self):
        return product_map

    def GetBuildProductNames(self):
        return {'name': 'cayman_antrea_idps',
                'longname': 'cayman_antrea_idps'}

    def GetCommands(self, hosttype):
        products = dist_map[hosttype]
        commands = [self._Command(hosttype=hosttype, product=product, args={"BUILD_PRODUCT":"cayman_antrea_idps"}) for product in products]
        return self._WrapCommands(hosttype, commands)

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
            "cayman_helm": {
                "branch": specs.cayman_antrea.CAYMAN_HELM_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_HELM_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_HELM_BUILDTYPE,
                "files": specs.cayman_antrea.CAYMAN_HELM_FILES},
            "cayman_kubernetes-sigs_kustomize": {
                "branch": specs.cayman_antrea.CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_BRANCH,
                "change": specs.cayman_antrea.CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_CLN,
                "buildtype": specs.cayman_antrea.CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_BUILDTYPE,
                "files": specs.cayman_antrea.CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_FILES},
        }

        return comps

    def GetProvenanceSchematics(self, hosttype):
        return [
            'cayman_antrea/support/gobuild/provenance/cayman_antrea_idps.schematic.json',
            'cayman_antrea/support/gobuild/provenance/build.schematic.json'
        ]
