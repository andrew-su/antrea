# Copyright 2008 Vmware Inc. All rights reserved. -- VMware Confidential
import os
import helpers.buildapi
import helpers.target
import helpers.env
import specs.antrea_release

BRANCH_NAME = "%(branch)"
BUILDROOT = "%(buildroot)"
PROJECT_DIR = "%s/antrea-release" % (BUILDROOT)
PUBLISH_DIR = "%s/publish" %(BUILDROOT)
BUILDNUMBER = "%(buildnumber)"

class AntreaRelease(helpers.target.Target):
    def GetBuildProductNames(self):
        return {
            "name": "antrea-release",
            "longname": "Build for antrea-release"
        }

    def GetRepositories(self, hostname):
        return [{
            "rcs": "git",
            "src": "core-build/cayman_antrea;%(branch);",
            "dst": "antrea-release"
        }]

    def GetClusterRequirements(self):
        return ["linux-centos72-gc32"]

    def GetStorageInfo(self, hosttype):
        return []

    def GetBuildProductVersion(self, hosttype):
        if BRANCH_NAME.startswith("vmware-"):
            return BRANCH_NAME[len("vmware-"):]
        else:
            return BRANCH_NAME

    def _GetEnvironment(self, hosttype):
        return self._GetEnvironmentLinux(hosttype)

    def _GetEnvironmentLinux(self, hosttype):
        env = helpers.env.SafeEnvironment(hosttype)
        env["SHELL"] = "/build/toolchain/lin64/bash-4.1/bin/bash"
        paths = map((lambda x: "/build/toolchain/lin64/" + x + "/bin"), [
            "make-3.81", "gawk-3.1.5",
            "findutils-4.2.27", "grep-2.5.1a", "autoconf-2.69",
            "automake-1.13", "sed-4.1.5", "tar-1.23", "gzip-1.5",
            "bash-4.1", "gcc-4.4.3-1", "git-1.8.3-1", "procps-3.2.7-1"
        ])
        paths += ["/build/toolchain/lin64/coreutils-8.6/bin"]
        paths += ["/usr/bin", "/bin", "/usr/local/sbin", "/usr/local/bin",
                  "/usr/sbin", "/sbin"]
        paths += ['/build/toolchain/lin64/wget-1.19.2-openssl1.0.2l/bin']
        paths += ['/build/toolchain/noarch/vmware/gpgsign/']
        env["PATH"] = os.pathsep.join(paths + [env["PATH"]])
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

    def GetCommands(self, hosttype):
        return self._GetCommandsLinux(hosttype)

    def _GetCommandsLinux(self, hosttype):
        commands = []

        cmd = "cd %(buildroot)/antrea-release; ./scripts/antrea_release_build.sh"
        commands.append({
            "desc": "build and publish antrea-release deliverables",
            "root": "%(buildroot)",
            "log": "gobuild_publish_deliverables.log",
            "command": cmd,
            "env": self._GetEnvironment(hosttype)
        })

        return commands

    def GetComponentDependencies(self):
        components = {
            "antrea-interworking": {
                "branch": specs.antrea_release.ANTREA_INTERWORKING_BRANCH,
                "buildtype": specs.antrea_release.ANTREA_INTERWORKING_BUILDTYPE,
                "files": specs.antrea_release.ANTREA_INTERWORKING_FILES,
                },
            "cayman_antrea": {
                "branch": specs.antrea_release.CAYMAN_ANTREA_BRANCH,
                "buildtype": specs.antrea_release.CAYMAN_ANTREA_BUILDTYPE,
                "files": specs.antrea_release.CAYMAN_ANTREA_FILES,
            },
            "cayman_antrea_tkgm-advanced":{
                "branch": specs.antrea_release.CAYMAN_ANTREA_BRANCH,
                "buildtype": specs.antrea_release.CAYMAN_ANTREA_BUILDTYPE,
                "files": specs.antrea_release.CAYMAN_ANTREA_FILES,
            },
            "cayman_antrea_tkgm-standard": {
                "branch": specs.antrea_release.CAYMAN_ANTREA_BRANCH,
                "buildtype": specs.antrea_release.CAYMAN_ANTREA_BUILDTYPE,
                "files": specs.antrea_release.CAYMAN_ANTREA_FILES,
            },
            "cayman_antrea_tkgs-advanced": {
                "branch": specs.antrea_release.CAYMAN_ANTREA_BRANCH,
                "buildtype": specs.antrea_release.CAYMAN_ANTREA_BUILDTYPE,
                "files": specs.antrea_release.CAYMAN_ANTREA_FILES,
            },
            "cayman_antrea_multi-cluster": {
                "branch": specs.antrea_release.CAYMAN_ANTREA_BRANCH,
                "buildtype": specs.antrea_release.CAYMAN_ANTREA_BUILDTYPE,
                "files": specs.antrea_release.CAYMAN_ANTREA_FILES,
            },
            "cayman_antrea-operator-for-kubernetes": {
                "branch": specs.antrea_release.CAYMAN_ANTREA_OPERATOR_FOR_KUBERNETES_BRANCH,
                "buildtype": specs.antrea_release.CAYMAN_ANTREA_OPERATOR_FOR_KUBERNETES_BUILDTYPE,
                "files": specs.antrea_release.CAYMAN_ANTREA_OPERATOR_FOR_KUBERNETES_FILES,
            },
        }
        # Automatically uses latest builds from the specified branch
        return helpers.buildapi.update_component_commits(
            components, requested_buildtype=self.options.get('buildtype'))
