#!/usr/bin/env bash

BINDIR="$(dirname $( readlink -f "$0" ) )"
exec "${BINDIR}/java-run.sh" org.apache.hadoop.fs.FsShell $@
exit 1
