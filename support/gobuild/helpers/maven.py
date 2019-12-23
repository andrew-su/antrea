# Copyright (c) 2008 VMware, Inc.  All rights reserved. -- VMware Confidential

"""
Helpers for maven-based targets.
"""

import os


class MavenHelper:
    """
    Helper class for targets that build with maven.
    """

    def _Command(self, hosttype, target, mavenversion='2.0.8', **flags):
        """
        Return a dictionary representing a command to invoke maven with
        standard mavenflags.
        """

        def q(s):
            return '"%s"' % s

        defaults = {
            'GOBUILD_OFFICIAL_BUILD': '1',
            'GOBUILD_AUTO_COMPONENTS': '',
            'OBJDIR': q('%(buildtype)'),
            'RELTYPE': q('%(releasetype)'),
            'BUILD_NUMBER': q('%(buildnumber)'),
            'PRODUCT_BUILD_NUMBER': q('%(productbuildnumber)'),
            'CHANGE_NUMBER': q('%(changenumber)'),
            'BRANCH_NAME': q('%(branch)'),
            'PUBLISH_DIR': q('%(buildroot)/publish'),
            'REMOTE_COPY_SCRIPT': q('%(gobuildc) %(buildid)'),
        }

        # Add a GOBUILD_*_ROOT flag for every component we depend on.
        if hasattr(self, 'GetComponentDependencyAliases'):
            for d in self.GetComponentDependencyAliases():
                d = d.replace('-', '_')
                defaults['GOBUILD_%s_ROOT' % d.upper()] = \
                    '%%(gobuild_component_%s_root)' % d

        # Override the defaults above with the options passed in by
        # the client.
        defaults.update(flags)

        # Choose maven
        if hosttype.startswith('windows'):
            tcroot = os.environ.get('TCROOT', 'C:/TCROOT-not-set')
            mavencmd = '%s/noarch/apache-maven-%s/bin/mvn.bat' % (tcroot,
                                                                  mavenversion)
        else:
            tcroot = os.environ.get('TCROOT', '/build/toolchain')
            mavencmd = '%s/noarch/apache-maven-%s/bin/mvn' % (tcroot,
                                                              mavenversion)

        # Create the command line to invoke maven
        cmd = '%s %s ' % (mavencmd, target)
        for k in sorted(defaults.keys()):
            v = defaults[k]
            cmd += ' -D' + str(k)
            if v is not None:
                cmd += '=' + str(v)

        return cmd
