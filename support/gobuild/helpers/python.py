# Copyright (c) 2013 VMware, Inc.  All rights reserved. -- VMware Confidential

"""
Helpers for Python-based targets.
"""

import os
import platform


def GetTCArch():
    sys_map = {'Linux': 'lin32', 'Windows': 'win32', 'Darwin': 'mac32'}
    return sys_map[platform.system()]


class PythonHelper:
    """
    Helper class for targets that build with Python.
    """

    def _PythonCommand(self, hosttype, product, script, target, arguments={}):

        arguments.update({
            'OBJDIR': '"%(buildtype)"',  # still in use
            'BUILDTYPE': '"%(buildtype)"',
            'BUILD_NUMBER': '"%(buildnumber)"',
            'PRODUCT_BUILD_NUMBER': '"%(productbuildnumber)"',
            'CHANGE_NUMBER': '"%(changenumber)"',
            'BRANCH_NAME': '"%(branch)"',
            'RELEASETYPE': '"%(releasetype)"',
            'PRODUCT': product,
            'GOBUILD_AUTO_COMPONENTS': '0',
        })

        if 'PUBLISH_DIR' not in arguments:
            arguments['PUBLISH_DIR'] = '%(buildroot)/publish'

        # Handle verbosity
        if 'VERBOSE' not in arguments:
            arguments['VERBOSE'] = True

        for d in self.GetComponentDependencyAliases():
            d_no_hyphens = d.replace('-', '_')
            root_key = 'GOBUILD_%s_ROOT' % d_no_hyphens.upper()
            root_value = '%%(gobuild_component_%s_root)' % d_no_hyphens
            arguments[root_key] = root_value

        tcroot = os.environ.get('TCROOT', '/build/toolchain')
        pythonver = 'python-2.7.1'
        pythondir = ''
        if not hosttype.startswith('windows'):
            # posix installs of python keep their binaries in bin/
            pythondir = 'bin'

        cmd = [
            os.path.join(tcroot, GetTCArch(), pythonver, pythondir, 'python'),
            os.path.join('%(buildroot)', script),
        ]

        cmd.extend(['%s=%s' % (k, v) for k, v in arguments.iteritems()])
        cmd.extend([target])

        return ' '.join(cmd)

    def _GetStoreSourceRule(self, hosttype, tree):
        """
        Return the standard storage rules for a python based build.  The
        Linux side is responsible for copying the source files to storage.
        """
        return [{'type': 'source',
                 'src': '%s/' % tree
                 }]

    def _GetStoreBuildRule(self, hosttype, tree):
        """
        Return the standard storage rules for a python based build.  The
        Linux side is responsible for copying the source files to storage.
        """
        return [{'type': 'build',
                 'src': 'build'
                 }]
