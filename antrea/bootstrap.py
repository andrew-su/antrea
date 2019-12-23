# Copyright (c) 2013-2016 VMware, Inc.  All rights reserved.
# -- VMware Confidential

"""
antrea build bootstrap.
"""

import os
import sys

sys.dont_write_bytecode = True

CWD = os.path.dirname(os.path.realpath(__file__))
VOSROOT, PROJECT = os.path.split(CWD)

sys.path.append(os.path.join(VOSROOT, 'support', 'gobuild'))

from py.autotools import autoTools, useCmdArgs, execWithArgs
from py.autotools import DefaultProdArchHostType


prod, arch, hostType = DefaultProdArchHostType()

env = {
    'CWD': CWD,
    'VOSROOT': VOSROOT,
    'PROJECT': PROJECT,
    'BUILDROOT': os.path.join(VOSROOT, 'build'),
    'COMPCACHE': os.environ.get(
        'GOBUILD_LOCALCACHE_DIR',
        os.path.join(VOSROOT, 'build', 'gobuild', 'compcache')),
    'LOGDIR': os.path.join(VOSROOT, 'build', 'gobuild', 'log'),
    'MAINSRCROOT': VOSROOT,
    'GOBUILD_AUTO_COMPONENTS': 1,
    'GOBUILD_AUTO_COMPONENTS_HOSTTYPE': hostType,
    'GOBUILD_TARGET': 'cayman_antrea',
    'OBJDIR': 'obj',
    'PRODUCT': prod,
    'ARCH': arch,

    # PROVENANCE_FILE defaults to GOBUILD_TARGET + '.yaml'. See vos.py in the
    # cayman component. In rare cases a project may need to overried the
    # filename. Typically this also requires passing a provenance_file
    # parameter to ExtractVersionFromProvenanceMetadata in the gobuild target.
    # See GetBuildProductVersion in support/gobuild/targets/cayman_antrea.py.
    #
    # env['PROVENANCE_FILE']: 'cayman_antrea.yaml',
}


# Pass through certain environment variables.
for v in ['SystemRoot', 'SystemDrive',
          'PROCESSOR_ARCHITECTURE',
          'TMP', 'TEMP', 'TMPDIR', 'windir']:
    if v in os.environ:
        env[v] = os.environ[v]

# Main.
print('CWD=%s' % env['CWD'])
print('VOSROOT=%s' % env['VOSROOT'])
print('BUILDROOT=%s' % env['BUILDROOT'])

target = useCmdArgs(sys.argv, env)

autoTools(env)

# Allow override of gobuild locations.
useCmdArgs(sys.argv, env)

# Dump the environment.
print('module_path:', sys.path)
for key, val in sorted(env.items()):
    print('%s=%s' % (key, val))

print('target = %s' % target)
print('global environment: %s' % os.environ)

env['TARGET'] = target

# Pass to next stage. '-B' suppresses .pyc file creation.
typical = os.path.join(env['GOBUILD_CAYMAN_ROOT'], 'typical.py')
execWithArgs([sys.executable, '-B', typical], env)
