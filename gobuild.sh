#!/bin/bash

GOBUILDCMD="/bldmnt/apps/bin/gobuild sandbox queue"
REPO=cayman_antrea.git
HEAD_REF=$(git symbolic-ref HEAD)
BRANCH=${HEAD_REF#refs/heads/}
LOCALCOMMITS="--changeset=False"
PYTHON="/build/toolchain/lin64/python-2.7.5/bin/python2.7"
REPO_ROOT="$( dirname "${BASH_SOURCE[0]}" )"
TARGETS=$(cd ${REPO_ROOT}/support/gobuild && ${PYTHON} -c "execfile('__init__.py'); \
    print ' '.join([x for x in TARGETS.keys() if not x.endswith('-access')])")

show_usage() {
    cat << EOF
usage: $(basename $0) <target> <options>

Available targets:
$(echo $TARGETS | tr ' ' '\n' | sort)

Options
    --localcommits        Apply local changeset to sandbox build
    --private             Use private git repo on git-eng:
                          git-eng:private/$USER/$REPO
    --                    Pass all remaining params to gobuild-sandbox-queue.
                          Get detailed help by $0 <target> -- --help
EOF

}
## Parse the parameters passed to the script
if [ "$1" = "--help" ] || [ "$1" = "-h" ] || [ "$1" = "" ]; then
    show_usage
    exit 0
fi

## Check if  target is in target list
if [[ $TARGETS =~ (^| )$1($| ) ]]; then
    TARGET=$1
    shift
else
    echo "$(basename $0): invalid target '$1'"
    echo "Available targets:"
    echo $TARGETS | tr ' ' '\n'
    exit 1
fi

while test $# != 0;
do
    case $1 in
        --help)
            show_usage
            exit 0
            ;;
        --localcommits)
            LOCALCOMMITS="--changeset=HEAD"
            ;;
        --private)
            REPO="private/$USER/$REPO"
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "$(basename $0): invalid argument '$1'"
            show_usage
            exit 1
            ;;
    esac
    shift
done

cmd="$GOBUILDCMD $TARGET \
--bootstrap=\"$TARGET=git-eng:core-build/$REPO;%(branch);\" \
--branch $BRANCH \
--accept-defaults \
--no-send-email \
$LOCALCOMMITS \
$@
"
echo "Running: $cmd"
eval $cmd
rc=$?
if [ $rc -ne 0 ]; then
    echo "INFO: If you log in as other user, append \"-- --user <userid>\" to override."
    echo "INFO: For detailed help, run \"$0 $TARGET -- --help\""
    exit $rc
fi

