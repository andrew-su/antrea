# Copyright (c) 2008 VMware, Inc.  All rights reserved. -- VMware Confidential

"""
Helpers for SCons-based targets.
"""

import os


class SConsHelper:
    """
    Helper class for targets that build with SCons.
    """

    def _Command(self, hosttype, target, **options):
        """
        Return a dictionary representing a command to invoke scons in bora
        with the standard flags required for it to succeed.
        """

        def q(s):
            return '"%s"' % s

        defaults = {
            'BUILDTYPE': q('%(buildtype)'),
            'RELTYPE': q('%(releasetype)'),
            'BUILD_NUMBER': q('%(buildnumber)'),
            'PRODUCT_BUILD_NUMBER': q('%(productbuildnumber)'),
            'CHANGE_NUMBER': q('%(changenumber)'),
            'BRANCH_NAME': q('%(branch)'),
            'RELEASE_PACKAGES_DIR': q('%(buildroot)/publish'),
            'PUBLISH_DIR': q('%(buildroot)/publish'),
            'REMOTE_COPY_SCRIPT': q('%(gobuildc) %(buildid)'),
        }

        # Handle verbosity
        if self.options.get('verbose') and 'VERBOSE' not in defaults:
            defaults['VERBOSE'] = True

        defaults.update(options)

        # Create the command line to invoke scons
        cmd = os.path.join('scons', 'bin', 'scons')
        cmd += ' %s' % target

        for k in sorted(defaults.keys()):
            v = defaults[k]
            cmd += ' ' + str(k)
            if v is not None:
                cmd += '=' + str(v)

        return cmd

    def _GetStoreSourceRule(self, hosttype, tree):
        """
        Return the standard storage rules for a scons based build.  The
        Linux side is responsible for copying the source files to storage.
        """
        return [{'type': 'source',
                 'src': '%s/' % tree
                 }]

    def _GetStoreBuildRule(self, hosttype, tree):
        """
        Return the standard storage rules for a scons based build.  The
        Linux side is responsible for copying the source files to storage.
        """
        return [{'type': 'build',
                 'src': 'build'
                 }]
