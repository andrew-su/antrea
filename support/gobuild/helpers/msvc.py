# Copyright (c) 2018-2019 VMware, Inc.  All rights reserved.
# VMware Confidential

"""Contains Microsoft Visual C++-related helpers.
"""

from helpers.target import TargetException

import specs.visualstudio


def AddCaymanMsvcComponents(comps, buildtype):
    """
    adds Windows and MSVC (non)redists and the MSVC compiler to the gobuild
    component dependencies. For obj builds, the nonredists are added. For
    all other build types, the redists are added. This function is intended
    to keep you on the "latest and greatest" version of the compiler that you
    specify.
    """
    # Add debug nonredists only to obj builds.
    if buildtype == 'obj':
        # Consumers should not already have these in comps.
        assert('cayman_msvc_nonredists' not in comps)
        assert('cayman_windows_nonredists' not in comps)

        comps['cayman_msvc_nonredists'] = [
            {
                'alias': 'cayman_msvc_nonredists_vc140',
                'branch': specs.visualstudio.CAYMAN_MSVC_NONREDISTS_VC140_BRANCH,
                'change': specs.visualstudio.CAYMAN_MSVC_NONREDISTS_VC140_CLN,
                'hosttypes': specs.visualstudio.CAYMAN_MSVC_NONREDISTS_VC140_HOSTTYPES,
                'buildtype': specs.visualstudio.CAYMAN_MSVC_NONREDISTS_VC140_BUILDTYPE,
                'cacheflags': specs.visualstudio.CAYMAN_MSVC_NONREDISTS_VC140_CACHEFLAGS,
            }
        ]

        comps['cayman_windows_nonredists'] = {
            'branch': specs.visualstudio.CAYMAN_WINDOWS_NONREDISTS_BRANCH,
            'change': specs.visualstudio.CAYMAN_WINDOWS_NONREDISTS_CLN,
            'hosttypes': specs.visualstudio.CAYMAN_WINDOWS_NONREDISTS_HOSTTYPES,
            'buildtype': specs.visualstudio.CAYMAN_WINDOWS_NONREDISTS_BUILDTYPE,
            'cacheflags': specs.visualstudio.CAYMAN_WINDOWS_NONREDISTS_CACHEFLAGS,
        }

    # Consumers should not already have these in comps.
    assert('cayman_msvc_redists' not in comps)
    assert('cayman_windows_redists' not in comps)
    assert('cayman_msvc_desktop' not in comps)

    # Add redists and the compiler for all build types.
    comps['cayman_msvc_redists'] = [
        {
            'alias': 'cayman_msvc_redists_vc140',
            'branch': specs.visualstudio.CAYMAN_MSVC_REDISTS_VC140_BRANCH,
            'change': specs.visualstudio.CAYMAN_MSVC_REDISTS_VC140_CLN,
            'hosttypes': specs.visualstudio.CAYMAN_MSVC_REDISTS_VC140_HOSTTYPES,
            'buildtype': specs.visualstudio.CAYMAN_MSVC_REDISTS_VC140_BUILDTYPE,
            'cacheflags': specs.visualstudio.CAYMAN_MSVC_REDISTS_VC140_CACHEFLAGS,
        }
    ]
    comps['cayman_msvc_desktop'] = [
        {
            'alias': 'cayman_msvc_desktop_vc140',
            'branch': specs.visualstudio.CAYMAN_MSVC_DESKTOP_VC140_BRANCH,
            'change': specs.visualstudio.CAYMAN_MSVC_DESKTOP_VC140_CLN,
            'hosttypes': specs.visualstudio.CAYMAN_MSVC_DESKTOP_VC140_HOSTTYPES,
            'buildtype': specs.visualstudio.CAYMAN_MSVC_DESKTOP_VC140_BUILDTYPE,
            'cacheflags': specs.visualstudio.CAYMAN_MSVC_DESKTOP_VC140_CACHEFLAGS,
        }
    ]
    comps['cayman_windows_redists'] = {
        'branch': specs.visualstudio.CAYMAN_WINDOWS_REDISTS_BRANCH,
        'change': specs.visualstudio.CAYMAN_WINDOWS_REDISTS_CLN,
        'hosttypes': specs.visualstudio.CAYMAN_WINDOWS_REDISTS_HOSTTYPES,
        'buildtype': specs.visualstudio.CAYMAN_WINDOWS_REDISTS_BUILDTYPE,
        'cacheflags': specs.visualstudio.CAYMAN_WINDOWS_REDISTS_CACHEFLAGS,
    }
