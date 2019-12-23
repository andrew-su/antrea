CAYMAN_BRANCH = 'master'
CAYMAN_CLN = '71c887b9ee67bd2298b85464ff14401b978ff885'
CAYMAN_BUILDTYPE = 'release'
CAYMAN_HOSTTYPES = {
    'linux64': 'linux',
    'linux': 'linux',
    'windows2016-clean': 'linux',
    'windows': 'linux',
    'windows-2008': 'linux',
    'windows-2016-vs2013-U5': 'linux',
    'macosx-elcapitan': 'linux',
}

CAYMAN_CMAKE_BRANCH = 'vmware-latest'
CAYMAN_CMAKE_CLN = 'dc8cddcec503519b8c73c360fb73354ff5dd330b'
CAYMAN_CMAKE_BUILDTYPE = 'release'
CAYMAN_CMAKE_HOSTTYPES = {
    'linux': 'linux64',
    'linux64': 'linux64',
    'windows2016-clean': 'windows-2008',
    'windows': 'windows-2008',
    'windows-2008': 'windows-2008',
    'windows-2016-vs2013-U5': 'windows-2008',
    'macosx-elcapitan': 'macosx-lion',
}

CAYMAN_NINJA_BRANCH = 'vmware-latest'
CAYMAN_NINJA_CLN = 'ffb8b65c850703dafa39b3d98cfa3d22d55e077c'
CAYMAN_NINJA_BUILDTYPE = 'release'
CAYMAN_NINJA_HOSTTYPES = {
    'linux': 'linux64',
    'linux64': 'linux64',
    'windows2016-clean': 'windows2012r2-vs2013',
    'windows': 'windows2012r2-vs2013',
    'windows-2008': 'windows2012r2-vs2013',
    'windows-2016-vs2013-U5': 'windows2012r2-vs2013',
    'macosx-elcapitan': 'macosx-elcapitan',
}

CAYMAN_LLVM_BRANCH = 'vmware-release_39'
CAYMAN_LLVM_CLN = '1f04882c1a1fd3f665a0f644878ebe4838e19940'
CAYMAN_LLVM_BUILDTYPE = 'release'
CAYMAN_LLVM_HOSTTYPES = {
    'linux64': 'linux64',
}

CAYMAN_ESX_GLIBC_BRANCH = 'vmkernel-main'
CAYMAN_ESX_GLIBC_CLN = 'b64510ac42fd2dc9daa8863dd55c9d1d78dcdfe4'
CAYMAN_ESX_GLIBC_HOSTTYPES = {'linux64': 'linux64'}
CAYMAN_ESX_GLIBC_BUILDTYPE = 'release'
CAYMAN_ESX_GLIBC_CACHEFLAGS = {'shareable': True, 'group': True}

CAYMAN_ESX_TOOLCHAIN_BRANCH = 'vmware-gcc48'
CAYMAN_ESX_TOOLCHAIN_BUILDTYPE = 'release'
CAYMAN_ESX_TOOLCHAIN_CLN = '746e5a3f11a81b7e0ccd0c7e48f58bfb99b23818'
CAYMAN_ESX_TOOLCHAIN_HOSTTYPES = {'linux64': 'linux64'}
CAYMAN_ESX_TOOLCHAIN_CACHEFLAGS = {'shareable': True, 'group': True}

CAYMAN_ESX_GLIBC_2_17_BRANCH = 'vmware-glibc-2.17'
CAYMAN_ESX_GLIBC_2_17_CLN = '8da846cdfffa1ce0a450d82e5d60140c753ab937'
CAYMAN_ESX_GLIBC_2_17_HOSTTYPES = {'linux64': 'linux64'}
CAYMAN_ESX_GLIBC_2_17_BUILDTYPE = 'release'
CAYMAN_ESX_GLIBC_2_17_CACHEFLAGS = {'shareable': True, 'group': True}

CAYMAN_GLIBC_2_17_BRANCH = 'vmware-glibc-2.17'
CAYMAN_GLIBC_2_17_CLN = '37fe6d3ad0bf5cf38bb7ef2b3232f9f7c56ed531'
CAYMAN_GLIBC_2_17_HOSTTYPES = {'linux64': 'linux64'}
CAYMAN_GLIBC_2_17_BUILDTYPE = 'release'
CAYMAN_GLIBC_2_17_CACHEFLAGS = {'shareable': True, 'group': True}

CAYMAN_ESX_TOOLCHAIN_GCC6_BRANCH = 'vmware-gcc6'
CAYMAN_ESX_TOOLCHAIN_GCC6_BUILDTYPE = 'release'
CAYMAN_ESX_TOOLCHAIN_GCC6_CLN = '20292133d8d6444deefa46c286c3f74ec52badc8'
CAYMAN_ESX_TOOLCHAIN_GCC6_HOSTTYPES = {'linux64': 'linux64'}
CAYMAN_ESX_TOOLCHAIN_GCC6_CACHEFLAGS = {'shareable': True, 'group': True}

CAYMAN_PYTHON_BRANCH = 'vmware-python-2.7-latest-openssl-1.0.2'
CAYMAN_PYTHON_BUILDTYPE = 'release'
CAYMAN_PYTHON_CLN = '604ab44a3f039be252b8ace7fb737585577f43b9'
CAYMAN_PYTHON_HOSTTYPES = {'linux64': 'linux64'}
CAYMAN_PYTHON_CACHEFLAGS = {'shareable': True}

CAYMAN_PYTHON3_BRANCH = 'vmware-python-3.5-latest-openssl-1.0.2'
CAYMAN_PYTHON3_BUILDTYPE = 'release'
CAYMAN_PYTHON3_CLN = '996fe31415ad7d8b2db37ee9b013b3b8944a900d'
CAYMAN_PYTHON3_HOSTTYPES = {'linux64': 'linux64'}
CAYMAN_PYTHON3_CACHEFLAGS = {'shareable': True}
