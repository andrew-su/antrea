# Copyright (c) 2018-2019 VMware, Inc.  All rights reserved.
# -- VMware Confidential

"""visualstudio.py

This file contains the specfile definitions needed to consume the latest
version of the vc14 (non)redists as well as the latest version of Visual Studio
that is compatible with those redists.

The recommended way of consuming these definitions is using helpers/msvc.py's
AddCaymanMsvcComponents function.
"""

CAYMAN_MSVC_REDISTS_VC140_BRANCH = 'vc140-redists-latest'
CAYMAN_MSVC_REDISTS_VC140_CLN = '9f1340a27dd6031bb697248dfdfb9dbf7e5c98cc'
CAYMAN_MSVC_REDISTS_VC140_BUILDTYPE = 'release'
CAYMAN_MSVC_REDISTS_VC140_HOSTTYPES = {
    'windows2016-clean': 'windows2016-clean',
}
CAYMAN_MSVC_REDISTS_VC140_CACHEFLAGS = {'shareable': True}

CAYMAN_MSVC_NONREDISTS_VC140_BRANCH = 'vc140-redists-latest'
CAYMAN_MSVC_NONREDISTS_VC140_CLN = '9f1340a27dd6031bb697248dfdfb9dbf7e5c98cc'
CAYMAN_MSVC_NONREDISTS_VC140_HOSTTYPES = {
    'windows2016-clean': 'windows2016-clean',
}
CAYMAN_MSVC_NONREDISTS_VC140_BUILDTYPE = 'release'
CAYMAN_MSVC_NONREDISTS_VC140_CACHEFLAGS = {'shareable': True}

CAYMAN_MSVC_DESKTOP_VC140_BRANCH = 'vc140-toolset-latest'
CAYMAN_MSVC_DESKTOP_VC140_CLN = 'e8ffdb75ddea083d57e80d10a3cc14e95e8b9e56'
CAYMAN_MSVC_DESKTOP_VC140_HOSTTYPES = {
    'windows2016-clean': 'windows2016-clean',
}
CAYMAN_MSVC_DESKTOP_VC140_BUILDTYPE = 'release'
CAYMAN_MSVC_DESKTOP_VC140_CACHEFLAGS = {'shareable': True}

CAYMAN_WINDOWS_REDISTS_BRANCH = 'win-redists-latest'
CAYMAN_WINDOWS_REDISTS_CLN = '9f1340a27dd6031bb697248dfdfb9dbf7e5c98cc'
CAYMAN_WINDOWS_REDISTS_BUILDTYPE = 'release'
CAYMAN_WINDOWS_REDISTS_HOSTTYPES = {
    'windows2016-clean': 'windows2016-clean',
}
CAYMAN_WINDOWS_REDISTS_CACHEFLAGS = {'shareable': True}

CAYMAN_WINDOWS_NONREDISTS_BRANCH = 'win-redists-latest'
CAYMAN_WINDOWS_NONREDISTS_CLN = '9f1340a27dd6031bb697248dfdfb9dbf7e5c98cc'
CAYMAN_WINDOWS_NONREDISTS_HOSTTYPES = {
   'windows2016-clean': 'windows2016-clean',
}
CAYMAN_WINDOWS_NONREDISTS_BUILDTYPE = 'release'
CAYMAN_WINDOWS_NONREDISTS_CACHEFLAGS = {'shareable': True}
