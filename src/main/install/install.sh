#!/bin/sh

# Copyright 2011 Quest Software, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# NAME
#   install.sh
#
# SYNOPSIS
#   /bin/sh install.sh [-v] [-h] [-s <sqoop_home_dir>] [-c <sqoop_conf_dir>] [-d <oraoop_doc_dir>] 
#
# DESCRIPTION
#   Installs the Quest® Data Connector for Oracle and Hadoop manager for Cloudera Sqoop.  Quest® Data Connector for Oracle and Hadoop is a Sqoop plug-in
#   that will copy data between Oracle and Hadoop with performance that has linear scalability
#   compared with the number of Hadoop mappers. 
#
# OPTIONS
#   -v                  Verbose mode that prints progress and context information to stdout.
#
#   -h                  Print this help page to stdout and exit without running the script.
#
#   -s <sqoop_home_dir> The directory in which Cloudera Sqoop has been installed.  This option
#                       overides $SQOOP_HOME in the environment, but either the option or the
#                       environment variable must be set.
#
#   -c <sqoop_conf_dir> The directory in which the site configuration for Cloudera Sqoop is stored.
#                       This option overides $SQOOP_CONF_DIR from the environment, and if neither
#                       is set the configuration directory defaults to "conf" in the Sqoop home.
#
#   -d <oraoop_doc_dir> The directory in which the Quest® Data Connector for Oracle and Hadoop documentation directory will be created.
#                       This sub-directory contains the Quest® Data Connector for Oracle and Hadoop User Guide. If this option is not
#                       specified then /usr/share/doc will be used.    
#

###
### Process command line options 
###

set -- `getopt 's:c:d:vh' $@`
while [ "$1" ]; do
	case "$1" in
		-s)
			SQOOP_HOME=$2
			shift 2;;
		-c)
			SQOOP_CONF_DIR=$2
			shift 2;;
		-d)
			DOC_BASEDIR=$2
			shift 2;;
		-v)
			VERBOSE="true"
			shift;;
		--)
			shift
			break;;
		*)
			cat $0 | head -46 | tail -n +16
			exit -1;;
	esac
done

###
### Set up the required parameters and verify that we have a viable installation environment 
###

# a shorthand road to dusty death
die() { echo "${0}: ERROR: ${1}" >&2; exit ${2:--1}; }

# ensure we have a valid install directory
INSTALL_DIR=`dirname $0`
[ "${VERBOSE}" ] && echo "-> Installing Quest® Data Connector for Oracle and Hadoop from directory \"${INSTALL_DIR}\""
[ -f "${INSTALL_DIR}/install.sh" ] || \
	die "installer script missing from install directory: ${INSTALL_DIR}/install.sh" 2
[ -f "${INSTALL_DIR}/conf/oraoop-site-template.xml" ] || \
	die "Quest® Data Connector for Oracle and Hadoop configuration file missing from install directory: ${INSTALL_DIR}/conf/oraoop-site-template.xml" 2

# get the version from version file
VERSION=`cat version.txt`
[ -z "${VERSION}" ] && die "Version file is corrupt: ${INSTALL_DIR}/version.txt"
[ "${VERBOSE}" ] && echo "-> Installing Quest® Data Connector for Oracle and Hadoop version \"${VERSION}\""

# ensure we have a viable SQOOP_HOME
[ -z "${SQOOP_HOME}" ] && \
	die "\$SQOOP_HOME environment variable must be set or use \"-s <sqoop_home>\" option"
[ -d "${SQOOP_HOME}" ] || \
	die "Sqoop home \"${SQOOP_HOME}\" is not a valid directory"

# default the Sqoop conf dir if it is not explicity set
if [ -z "${SQOOP_CONF_DIR}" ]; then
	[ "${VERBOSE}" ] && echo "-> Sqoop conf directory not set - using \$SQOOP_HOME/conf as the Sqoop configuration directory"
	SQOOP_CONF_DIR="${SQOOP_HOME}/conf"
fi

# make sure the Sqoop conf directory looks kosher 
[ -d "${SQOOP_CONF_DIR}" ] || \
	die "Sqoop configuration directory not found: ${SQOOP_CONF_DIR}"
[ -f "${SQOOP_CONF_DIR}/sqoop-site-template.xml" ] || \
	die "Sqoop configuration directory \"${SQOOP_CONF_DIR}\" must contain the file sqoop-site-template.xml."

# ensure we have a managers.d file in the Sqoop conf directory
MANAGER_DIR="${SQOOP_CONF_DIR}/managers.d"
[ -d $MANAGER_DIR ] || mkdir -p ${MANAGER_DIR} || \
	die "failed to create the Sqoop managers.d directory" $?

# set the directory where we will put the user guide
DOC_TARGET="${DOC_BASEDIR:-${SQOOP_HOME}/doc}/oraoop-${VERSION}"

# dump the parameters if we're being garrulous
if [ "${VERBOSE}" ]; then
	echo "-> Installation parameters:"
	echo "   Sqoop home directory  = ${SQOOP_HOME}"
	echo "   Sqoop conf directory  = ${SQOOP_CONF_DIR}"
	echo "   User guide directory  = ${DOC_TARGET}"
	echo "   Quest® Data Connector for Oracle and Hadoop version number = ${VERSION}"
fi

###
### Force user to accept license agreement
###

head -n 176 docs/LICENSE.txt | less -EXF
if [ "$$" = "$BASHPID" ]; then
	READOPTS="-n 1 -s"
fi
while [ "${license_accepted}" = "" ]; do
	echo
	printf "Do you accept these terms? (Y/N): "
	read $READOPTS license_accepted
done
echo
case "${license_accepted}" in
	[yY1]*) ;;
	*) die "License agreement not accepted" 3
esac

###
### Set up complete!  Now we need to install the Quest® Data Connector for Oracle and Hadoop files into the Sqoop directory.
###

# Check for other oraoop jar files in the target directory
EXISTING_JARS=()
for file in ${SQOOP_HOME}/lib/oraoop*.jar
do
        if [ -e "$file" ]; then
                EXISTING_JARS+=("$file")
        fi
done

if [ ${#EXISTING_JARS[@]} -gt 0 ]; then
        echo "Found existing Quest® Data Connector for Oracle and Hadoop files, they will be removed."
        for file in "${EXISTING_JARS[@]}"
        do
                echo "Removing ${file}"
                rm -f "$file"
        done
fi

# install the jar file into the Sqoop lib directory
JAR_TARGET="${SQOOP_HOME}/lib/oraoop-${VERSION}.jar"
install -m 444 ${INSTALL_DIR}/bin/oraoop.jar $JAR_TARGET || \
	die "failed to install the Quest® Data Connector for Oracle and Hadoop manager jar file" $?
[ "${VERBOSE}" ] && echo "-> Installed Quest® Data Connector for Oracle and Hadoop jar as \"${JAR_TARGET}\""

# create the Quest® Data Connector for Oracle and Hadoop manager configuration for the jar file we just installed
echo "com.quest.oraoop.OraOopManagerFactory=${JAR_TARGET}" > ${MANAGER_DIR}/oraoop || \
	die "failed to create the Quest® Data Connector for Oracle and Hadoop manager configuration file" $?
chmod 444 ${MANAGER_DIR}/oraoop
[ "${VERBOSE}" ] && ( echo "-> Created Quest® Data Connector for Oracle and Hadoop manager conf:"; echo "   `cat ${MANAGER_DIR}/oraoop`" )

# install the site configuration template and make a copy as the site configuration
install -m 444 ${INSTALL_DIR}/conf/oraoop-site-template.xml ${SQOOP_CONF_DIR} || \
	die "failed to install the Quest® Data Connector for Oracle and Hadoop site configuration" $?

# install the Quest® Data Connector for Oracle and Hadoop help
[ "${VERBOSE}" ] && echo "-> Installing user guide in directory \"${DOC_TARGET}\""
mkdir -p ${DOC_TARGET}
if [ "$?" != "0" ]; then
	echo "Failed to create documentation directory \"${DOC_TARGET}\". This probably needs root permission."
	echo "See \"${INSTALL_DIR}/docs/oraoopuserguide.pdf\" for the Quest® Data Connector for Oracle and Hadoop user guide."
else
	install -m 444 ${INSTALL_DIR}/docs/oraoopuserguide.pdf ${DOC_TARGET} || \
		die "failed to install the Quest® Data Connector for Oracle and Hadoop user guide" $?
	install -m 444 ${INSTALL_DIR}/docs/LICENSE.txt ${DOC_TARGET} || \
		die "failed to install the Quest® Data Connector for Oracle and Hadoop license" $?
fi

###
### Installation complete
###
[ "${VERBOSE}" ] && echo "Installation complete"
