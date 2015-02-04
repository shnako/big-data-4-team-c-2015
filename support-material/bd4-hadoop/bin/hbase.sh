#! /usr/bin/env bash
#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# 
# The hbase command script.  Based on the hadoop command script putting
# in hbase classes, libs and configurations ahead of hadoop's.
#
# TODO: Narrow the amount of duplicated code.
#
# Environment Variables:
#
#   JAVA_HOME        The java implementation to use.  Overrides JAVA_HOME.
#
#   HBASE_CLASSPATH  Extra Java CLASSPATH entries.
#
#   HBASE_CLASSPATH_PREFIX Extra Java CLASSPATH entries that should be
#                    prefixed to the system classpath.
#
#   HBASE_HEAPSIZE   The maximum amount of heap to use, in MB. 
#                    Default is 1000.
#
#   HBASE_LIBRARY_PATH  HBase additions to JAVA_LIBRARY_PATH for adding
#                    native libraries.
#
#   HBASE_OPTS       Extra Java runtime options.
#
#   HBASE_CONF_DIR   Alternate conf dir. Default is ${HBASE_HOME}/conf.
#
#   HBASE_ROOT_LOGGER The root appender. Default is INFO,console
#
#   JRUBY_HOME       JRuby path: $JRUBY_HOME/lib/jruby.jar should exist.
#                    Defaults to the jar packaged with HBase.
#
#   JRUBY_OPTS       Extra options (eg '--1.9') passed to the hbase shell.
#                    Empty by default.
#
bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# This will set HBASE_HOME, etc.
#. "$bin"/hbase-config.sh
HBASE_HOME="${bin}/.."
HADOOP_HOME="${HBASE_HOME}"
HBASE_CONF_DIR="${HBASE_HOME}/conf"
JAVA_HOME="${JAVA_HOME:-/usr}"

if [ -z "$JAVA_HOME" -o ! -d "$JAVA_HOME" ]; then
    echo "Error: JAVA_HOME is not set or doesn't point to a valid directory"
    exit 1
fi

cygwin=false
case "`uname`" in
CYGWIN*) cygwin=true;;
esac

# Detect if we are in hbase sources dir
in_dev_env=false
if [ -d "${HBASE_HOME}/target" ]; then
  in_dev_env=true
fi

# if no args specified, show usage
if [ $# = 0 ]; then
  echo "Usage: hbase [<options>] <command> [<args>]"
  echo "Options:"
  echo ""
  echo "Commands:"
  echo "Some commands take arguments. Pass no args or -h for usage."
  echo "  shell           Run the HBase shell"
  echo "  classpath       Dump hbase CLASSPATH"
  echo "  CLASSNAME       Run the class named CLASSNAME"
  exit 1
fi

# get arguments
COMMAND=$1
shift

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx1000m 

# check envvars which might override default args
if [ "$HBASE_HEAPSIZE" != "" ]; then
  SUFFIX="m"
  if [ "${HBASE_HEAPSIZE: -1}" == "m" ] || [ "${HBASE_HEAPSIZE: -1}" == "M" ]; then
    SUFFIX=""
  fi
  if [ "${HBASE_HEAPSIZE: -1}" == "g" ] || [ "${HBASE_HEAPSIZE: -1}" == "G" ]; then
    SUFFIX=""
  fi
  #echo "run with heapsize $HBASE_HEAPSIZE"
  JAVA_HEAP_MAX="-Xmx""$HBASE_HEAPSIZE""$SUFFIX"
  #echo $JAVA_HEAP_MAX
fi

# so that filenames w/ spaces are handled correctly in loops below
ORIG_IFS=$IFS
IFS=

# CLASSPATH initially contains $HBASE_CONF_DIR
CLASSPATH="${HBASE_CONF_DIR}"
CLASSPATH=${CLASSPATH}:${JAVA_HOME}/lib/tools.jar

add_to_cp_if_exists() {
  if [ -d "$@" ]; then
    CLASSPATH=${CLASSPATH}:"$@"
  fi
}

# Add libs to CLASSPATH
for f in $HBASE_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# default log directory & file
if [ "$HBASE_LOG_DIR" = "" ]; then
  HBASE_LOG_DIR="$HBASE_HOME/logs"
fi
if [ "$HBASE_LOGFILE" = "" ]; then
  HBASE_LOGFILE='hbase.log'
fi

function append_path() {
  if [ -z "$1" ]; then
    echo $2
  else
    echo $1:$2
  fi
}

JAVA_PLATFORM=""

# if HBASE_LIBRARY_PATH is defined lets use it as first or second option
if [ "$HBASE_LIBRARY_PATH" != "" ]; then
  JAVA_LIBRARY_PATH=$(append_path "$JAVA_LIBRARY_PATH" "$HBASE_LIBRARY_PATH")
fi

# Add user-specified CLASSPATH last
if [ "$HBASE_CLASSPATH" != "" ]; then
  CLASSPATH=${CLASSPATH}:${HBASE_CLASSPATH}
fi

# Add user-specified CLASSPATH prefix first
if [ "$HBASE_CLASSPATH_PREFIX" != "" ]; then
  CLASSPATH=${HBASE_CLASSPATH_PREFIX}:${CLASSPATH}
fi

# cygwin path translation
if $cygwin; then
  CLASSPATH=`cygpath -p -w "$CLASSPATH"`
  HBASE_HOME=`cygpath -d "$HBASE_HOME"`
  HBASE_LOG_DIR=`cygpath -d "$HBASE_LOG_DIR"`
fi

# restore ordinary behaviour
unset IFS

HBASE_OPTS="$HBASE_OPTS $CLIENT_GC_OPTS"

# figure out which class to run
if [ "$COMMAND" = "shell" ] ; then
  #find the hbase ruby sources
  if [ -d "$HBASE_HOME/lib/ruby" ]; then
    HBASE_OPTS="$HBASE_OPTS -Dhbase.ruby.sources=$HBASE_HOME/lib/ruby"
  else
    HBASE_OPTS="$HBASE_OPTS -Dhbase.ruby.sources=$HBASE_HOME/hbase-shell/src/main/ruby"
  fi
  HBASE_OPTS="$HBASE_OPTS $HBASE_SHELL_OPTS"
  CLASS="org.jruby.Main -X+O ${JRUBY_OPTS} ${HBASE_HOME}/lib/ruby/hirb.rb"
elif [ "$COMMAND" = "classpath" ] ; then
  echo $CLASSPATH
  exit 0
else
  CLASS=$COMMAND
fi

# Have JVM dump heap if we run out of memory.  Files will be 'launch directory'
# and are named like the following: java_pid21612.hprof. Apparently it doesn't
# 'cost' to have this flag enabled. Its a 1.6 flag only. See:
# http://blogs.sun.com/alanb/entry/outofmemoryerror_looks_a_bit_better
HBASE_OPTS="$HBASE_OPTS -Dhbase.log.dir=$HBASE_LOG_DIR"
HBASE_OPTS="$HBASE_OPTS -Dhbase.log.file=$HBASE_LOGFILE"
HBASE_OPTS="$HBASE_OPTS -Dhbase.home.dir=$HBASE_HOME"
HBASE_OPTS="$HBASE_OPTS -Dhbase.root.logger=${HBASE_ROOT_LOGGER:-INFO,console}"
if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
  HBASE_OPTS="$HBASE_OPTS -Djava.library.path=$JAVA_LIBRARY_PATH"
  export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$JAVA_LIBRARY_PATH"
fi

HBASE_OPTS="$HBASE_OPTS -Dhbase.security.logger=${HBASE_SECURITY_LOGGER:-INFO,NullAppender}"

# Exec unless HBASE_NOEXEC is set.
export CLASSPATH
if [ "${HBASE_NOEXEC}" != "" ]; then
  "$JAVA" -Dproc_$COMMAND -XX:OnOutOfMemoryError="kill -9 %p" $JAVA_HEAP_MAX $HBASE_OPTS $CLASS "$@"
else
  exec "$JAVA" -Dproc_$COMMAND -XX:OnOutOfMemoryError="kill -9 %p" $JAVA_HEAP_MAX $HBASE_OPTS $CLASS "$@"
fi
