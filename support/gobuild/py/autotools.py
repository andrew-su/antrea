from __future__ import print_function
import subprocess
import sys
import platform


def downloadTools(GOBUILD_TARGET,
                  VOSROOT,
                  OBJDIR,
                  COMPCACHE,
                  LOGDIR,
                  GOBUILD_AUTO_COMPONENTS_HOSTTYPE,
                  GOBUILD_AUTO_COMPONENTS_SKIP='',
                  GOBUILD_AUTO_COMPONENTS_ONLY=None,
                  GOBUILD_AUTO_COMPONENTS_BUILDS=None,
                  **kwargs):

    cmd = [
        'gobuild',
        'deps',
        GOBUILD_TARGET,
        '--srcroot=%s' % VOSROOT,
        '--buildtype=%s' % OBJDIR,
        '--localcache=%s' % COMPCACHE,
        '--skip.components=%s' % GOBUILD_AUTO_COMPONENTS_SKIP,
        '--logdir=%s' % LOGDIR,
        '--hosttype=%s' % GOBUILD_AUTO_COMPONENTS_HOSTTYPE,
        '--download',
    ]

    if GOBUILD_AUTO_COMPONENTS_ONLY is not None:
        cmd.append(
            '--only.components=%s' % GOBUILD_AUTO_COMPONENTS_ONLY,
        )

    if GOBUILD_AUTO_COMPONENTS_BUILDS is not None:
        cmd.append(
            '--component.builds=%s' % GOBUILD_AUTO_COMPONENTS_BUILDS,
        )

    print('downloadTools cmd: %s' % cmd)

    useShell = sys.platform.startswith('win')

    p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT,
                         shell=useShell)

    stdout, stderr = p.communicate()
    if p.returncode != 0:
        print('*** gobuild deps failed!')
        print(stdout)
        print('***')
        raise subprocess.CalledProcessError(p.returncode, cmd, stderr)
    return stdout.decode()


def parseComponentSpec(spec):
    components = []
    for line in spec.splitlines():
        if line.startswith('@'):
            data = line[1:].strip()
        else:
            print(line)
            continue
        try:
            name, path = data.split(',', 1)
        except ValueError:
            print('parseComponentSpec: invalid input line: %s' % line,
                  file=sys.stderr)
            return components, 1
        components.append((name, path))

    return components, 0


def useCmdArgs(args, env):
    target = ''
    for a in args:
        try:
            key, val = a.split('=', 1)
        except:
            target = a
            continue
        env[key] = val

    return target


def autoTools(env):
    autoCmpKey = 'GOBUILD_AUTO_COMPONENTS'
    autoCmp = env.get(autoCmpKey, None)

    print('%s=%s' % (autoCmpKey, autoCmp))
    if autoCmp and autoCmp == '0':
        print("Auto components disabled")
        return []

    print('Downloading components...')
    spec = downloadTools(**env)
    comps, err = parseComponentSpec(spec)

    # update env with the component locations
    for (name, path) in comps:
        env['GOBUILD_%s_ROOT' % name.replace('-', '_').upper()] = path

    return comps


def execWithArgs(cmd, env):
    _cmd = cmd[:]

    for key in env:
        _cmd.append('%s=%s' % (key, env[key]))

    sys.stdout.flush()
    subprocess.check_call(_cmd)


def DefaultProdArchHostType():
    s = platform.system()

    if s == 'Linux':
        return 'lin64', 'lin64', 'linux64'
    elif s == 'Windows':
        return 'win64', 'win64', 'windows-2016-vs2015-U3'
    elif s == 'Darwin':
        return 'mac64', 'mac64', 'macosx-lion'
