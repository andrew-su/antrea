# Copyright (c) 2013-2017 VMware, Inc.  All rights reserved.
# -- VMware Confidential

"""
Antrea build definition.
"""

import os

import py.log
from os.path import join
from py.build.vos import Vos
import py.utils.sh_tools as sh_tools
import py.utils.flags as Flags
import py.profiles.common.tools.gcc_common as gccCommon
import py.profiles.common.tools.msvc_common as msvcCommon
import py.profiles.apple_common.tools as apple_tools
import py.build.noarchbuilder as noarchbuilder

import py.build.utils.cmake as cmake
import py.build.utils.ninja as ninja

logger = py.log.getLogger()

logger.debug('antrea_defs.py')


class Strip(object):

    def __init__(self, env):
        self._env = env

    def __call__(self, src, dst):
        import subprocess
        return subprocess.call(
            [self._env['STRIP'], '--strip-all', '-o', dst, src])


# Note: inherit from the closest of the stock builder classes available in
# py/build and override any method needed

class CaymanAntreaBuilder(Vos):

    @py.log.traced
    def setEnv(self, env):
        super(CaymanAntreaBuilder, self).setEnv(env)

    @py.log.traced
    def install(self, env):
        super(CaymanAntreaBuilder, self).install(env)
        # Manually copy any files not copied by the install step.

        # rootDir = os.path.join(env['PROJECT_SRC_DIR'], 'include')
        # installDir = os.path.join(env['DESTDIR'], 'include')

        # dirfilter = lambda root, x: 'test' not in root and 'test' not in x
        # filefilter = lambda root, x: x.endswith('.h') and 'test' not in root

        # sh_tools.DeepCopy(rootDir, installDir,
        # dir_filter = dirfilter,
        # file_filter = file_filter)


class CaymanAntreaBuilderLin(CaymanAntreaBuilder):
    """
    Linux builder
    """

    @py.log.traced
    def setEnv(self, env):
        super(CaymanAntreaBuilderLin, self).setEnv(env)

        # install directly in the publish/$PRODUCT dir
        env['DESTDIR'] = os.path.join(env['PUBLISH_DIR'], env['PRODUCT'])

        gccCommon.useWarningsAsErrors(env)

        # Disable specific warnings
        Flags.Append(env,
                     CFLAGS=[
                         # '-Wno-unused-parameter',
                     ],)

        ninja.useNinjaLin(env)

    @py.log.traced
    def configure(self, env):
        gccCommon.useCCwrapper(env)
        gccCommon.useCXXwrapper(env)

        customFlags = [
            # config
        ]
        cmd, cmdEnv = cmake.cmakeConfigCommandLin(env,
                                                  customFlags=customFlags)

        return sh_tools.Execute(env['BUILDDIR'], cmd, cmdEnv, env)

    @py.log.traced
    def build(self, env):

        customFlags = [
        ]

        command, cmdEnv = ninja.ninjaCommandLin(env,
                                                customFlags=customFlags)

        return sh_tools.Execute(env['BUILDDIR'], command, cmdEnv, env)

    @py.log.traced
    def install(self, env):
        super(CaymanAntreaBuilderLin, self).install(env)

        # Copy or rename any build artefacts
        files = []

        # files = [
        # (sh_tools.Copy,
        #  '%(BUILDDIR)s/Bin/antrea/config.hpp',
        #  '%(DESTDIR)s/include/antrea/config.hpp'),
        # (sh_tools.Copy,
        #  '%(BUILDDIR)s/Bin/%(BUILDTYPE)s/libantrea.so',
        #  '%(DESTDIR)s/%(SODIR_DEBUG)s/libantrea.so'),
        # (Strip(env),
        #  '%(DESTDIR)s/%(SODIR_DEBUG)s/libantrea.so',
        #  '%(DESTDIR)s/%(SODIR_RELEASE)s/libantrea.so'),
        # ]

        # Expand environment variables.
        files = [(cmd, src % env, dst % env) for cmd, src, dst in files]
        return sh_tools.Install(files)


class CaymanAntreaBuilderWin(CaymanAntreaBuilder):
    """
    Windows builder
    """

    @py.log.traced
    def setEnv(self, env):
        super(CaymanAntreaBuilderWin, self).setEnv(env)

        # install directly in the publish/$PRODUCT dir
        env['DESTDIR'] = os.path.join(env['PUBLISH_DIR'], env['PRODUCT'])

        msvcCommon.useWarningsAsErrors(env)

        # Disable specific warnings
        Flags.Append(env,
                     CFLAGS=[
                         # '/wd1111', # some warning
                     ],)

        ninja.useNinjaLin(env)

    @py.log.traced
    def configure(self, env):

        customFlags = [
            # config
        ]
        cmd, cmdEnv = cmake.cmakeConfigCommandWin(env,
                                                  customFlags=customFlags)

        return sh_tools.Execute(env['BUILDDIR'], cmd, cmdEnv, env)

    @py.log.traced
    def build(self, env):

        customFlags = [
        ]

        command, cmdEnv = ninja.ninjaCommandWin(env,
                                                customFlags=customFlags)

        return sh_tools.Execute(env['BUILDDIR'], command, cmdEnv, env)

    @py.log.traced
    def install(self, env):
        super(CaymanAntreaBuilderWin, self).install(env)

        # Copy or rename any build artefacts
        files = []

        # files = [
        #    (sh_tools.Copy,
        #     '%(BUILDDIR)s/Bin/antrea/config.hpp',
        #     '%(DESTDIR)s/include/antrea/config.hpp'),
        #    (sh_tools.Copy,
        #     '%(BUILDDIR)s/Bin/%(BUILDTYPE)s/antrea.lib',
        #     '%(DESTDIR)s/bin/antrea.lib'),
        #    (sh_tools.Copy,
        #     '%(BUILDDIR)s/Bin/%(BUILDTYPE)s/antrea.dll',
        #     '%(DESTDIR)s/bin/antrea.dll'),
        # ]
        # if env['BUILDTYPE'] == 'Debug':
        #    files.append((sh_tools.Copy,
        #                  '%(BUILDDIR)s/Bin/%(BUILDTYPE)s/antrea.pdb',
        #                  '%(DESTDIR)s/bin/antrea.pdb'))

        # Expand environment variables.
        files = [(cmd, src % env, dst % env) for cmd, src, dst in files]
        return sh_tools.Install(files)


class CaymanAntreaBuilderMac(CaymanAntreaBuilder):
    """
    Apple Mac C++11 + libc++ builder
    """

    @py.log.traced
    def prep(self, env):
        super(CaymanAntreaBuilderMac, self).prep(env)

        apple_tools.xcode.generate(env,
                                   xcodeVersion='7.3',
                                   sdkVersion='10.11',
                                   minVersion='10.9')

    @py.log.traced
    def setEnv(self, env):
        super(CaymanAntreaBuilderMac, self).setEnv(env)

        gccCommon.useWarningsAsErrors(env)

        # Disable specific warnings
        Flags.Append(env,
                     CFLAGS=[
                         # '-Wno-unused-parameter',
                     ],)

        ninja.useNinjaMac(env)

    @py.log.traced
    def configure(self, env):
        gccCommon.useCCwrapper(env)
        gccCommon.useCXXwrapper(env)

        customFlags = [
            # config
        ]
        cmd, cmdEnv = cmake.cmakeConfigCommandMac(env,
                                                  customFlags=customFlags)

        return sh_tools.Execute(env['BUILDDIR'], cmd, cmdEnv, env)

    @py.log.traced
    def build(self, env):

        customFlags = [
        ]

        command, cmdEnv = ninja.ninjaCommandMac(env,
                                                customFlags=customFlags)

        return sh_tools.Execute(env['BUILDDIR'], command, cmdEnv, env)

    @py.log.traced
    def install(self, env):
        super(CaymanAntreaBuilderMac, self).install(env)

        # Copy or rename any build artefacts
        files = []

        # files = [
        # (sh_tools.Copy,
        #  '%(BUILDDIR)s/Bin/antrea/config.hpp',
        #  '%(DESTDIR)s/include/antrea/config.hpp'),
        # (sh_tools.Copy,
        #  '%(BUILDDIR)s/Bin/%(BUILDTYPE)s/libantrea.so',
        #  '%(DESTDIR)s/%(SODIR_DEBUG)s/libantrea.so'),
        # (Strip(env),
        #  '%(DESTDIR)s/%(SODIR_DEBUG)s/libantrea.so',
        #  '%(DESTDIR)s/%(SODIR_RELEASE)s/libantrea.so'),
        # ]

        # Expand environment variables.
        files = [(cmd, src % env, dst % env) for cmd, src, dst in files]
        return sh_tools.Install(files)

    @py.log.traced
    def postInstall(self, env):
        super(CaymanAntreaBuilderMac, self).postInstall(env)

        apple_tools.gobuild.Publish(
            env['BUILDDIR'],
            env,
            [
                join(env['DESTDIR'], 'bin'),
                join(env['DESTDIR'], 'include'),
                join(env['DESTDIR'], 'lib'),
            ],
            join(env['PUBLISH_DIR'],  env['PRODUCT']),
            env['GOBUILD_TARGET'],
            publishRoot=env['PUBLISH_DIR'])


class CaymanAntreaBuilderNoarch(noarchbuilder.PythonBuilderNoarch):
    """Linux builder"""

    @py.log.traced
    def setEnv(self, env):

        super(CaymanAntreaBuilderNoarch, self).setEnv(env)
        # install directly in the publish/$PRODUCT dir
        env['PYTHON3'] = os.path.join(env['GOBUILD_CAYMAN_PYTHON_ROOT'],
                                      'lin64', 'bin', 'python3')
        env['PYTHON'] = os.path.join(env['GOBUILD_CAYMAN_PYTHON2_ROOT'],
                                     'lin64', 'bin', 'python')

    @py.log.traced
    def configure(self, env):

        # Copy the source files into the build dir
        sh_tools.DeepCopy(env['PROJECT_SRC_DIR'], env['BUILDDIR'])

    @py.log.traced
    def install(self, env):

        cmdEnv = {}
        cmdEnv['DESTDIR'] = env['DESTDIR']

        command = [
            env['PYTHON3'],
            'setup.py',
            'install',
            '--compile',
            '--optimize=1',
            '--prefix=%s' % env['DESTDIR']
            ]

        return sh_tools.Execute(env['BUILDDIR'], command, cmdEnv, env)

    @py.log.traced
    def _publishPkg(self, env):
        # TODO publish egg/wheel packages
        pass


def getBuilder(product, env):
    """
    Entry point

    Get the builder object for the specified target platform
    """

    logger.debug('antrea_defs.py getbuilder, product: %s' % product)

    logger.debug('=== Builder Env Begin===')
    for k, v in sorted(env.items()):
        logger.debug('%s = %s' % (k, v))
    logger.debug('=== Builder Env End===')

# if package is pure python then use CaymanAntreaBuilderNoarch
#   return CaymanAntreaBuilderNoarch()

    if 'IS_WINDOWS' in env:
        return CaymanAntreaBuilderWin()

    if 'IS_LINUX' in env:
        return CaymanAntreaBuilderLin()

    if 'IS_ESX' in env:
        return CaymanAntreaBuilderLin()

    if 'XCODE_PLATFORM' in env:
        return CaymanAntreaBuilderMac()

    raise Exception('unknown product: %s' % product)
