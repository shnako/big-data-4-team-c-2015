#!/usr/bin/env bash

BINDIR="$(dirname $( readlink -f "$0" ) )"
TOPDIR="${BINDIR}/.."
_CP="${TOPDIR}/conf"
for f in "${TOPDIR}/lib/"*.jar; do _CP="${_CP}:$f"; done
if [ ! -z "$HADOOP_CLASSPATH" ]; then _CP="${_CP}:${HADOOP_CLASSPATH}"; fi
exec java -classpath "$_CP" $@
exit 1
