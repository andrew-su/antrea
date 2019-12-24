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

    @py.log.traced
    def configure(self, env):
        pass

    @py.log.traced
    def build(self, env):
        src = os.path.join(env['VOSROOT'], 'antrea')
        command = ['chmod', '+x', './antrea_build.sh']
        sh_tools.Execute(src, command, env, env)
        return sh_tools.Execute(src, ['./antrea_build.sh'], env, env)

    @py.log.traced
    def install(self, env):
        super(CaymanAntreaBuilderLin, self).install(env)

        # Expand environment variables.
        sh_tools.DeepCopy(os.path.join(env['BUILDROOT'], 'output', 'scripts'),
                          os.path.join(env['DESTDIR'], 'antrea', 'scripts'))


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

    if 'IS_LINUX' in env:
        return CaymanAntreaBuilderLin()

    raise Exception('unknown product: %s' % product)
