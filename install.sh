#!/bin/bash

## THIS DIRECTORY

SOURCE=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)

## DYNAMO SOURCE

DYNAMO=$1

if [ -z "$DYNAMO" ] || ! [ -d $DYNAMO ]
then
  echo "Usage: install.sh DYNAMO"
  exit 1
fi

## DYNAMO CONFIGURATION

INSTALL_CONF=$2
[ -z "$INSTALL_CONF" ] && INSTALL_CONF=$DYNAMO/dynamo.cfg

if ! [ -e $INSTALL_CONF ]
then
  echo
  echo "$INSTALL_CONF does not exist."
  exit 1
fi

## PLACE ITEMS REQUIRED BEFORE INSTALLATION

rm -rf $SOURCE/.tmp
mkdir $SOURCE/.tmp
CLEANUP_LIST=

for OBJ in mysql config
do
  # List of files we are adding to $DYNAMO
  CLEANUP_LIST=$CLEANUP_LIST" "$(diff -rq $SOURCE/$OBJ $DYNAMO/$OBJ | sed -n "s|^Only in $SOURCE/\(.*\): \(.*\)|\1/\2|p")
  REPLACE_LIST=$(diff -rq $SOURCE/$OBJ $DYNAMO/$OBJ | sed -n "s|^Files .* and $DYNAMO/\(.*\) differ|\1|p")

  for FILE in $REPLACE_LIST
  do
    mkdir -p $SOURCE/.tmp/$(dirname $FILE)
    mv $DYNAMO/$FILE $SOURCE/.tmp/$FILE
  done

  cp -rf $SOURCE/$OBJ $DYNAMO/
done

CLEANUP_LIST=$CLEANUP_LIST" defaults.json"
mv $DYNAMO/defaults.json $SOURCE/.tmp/defaults.json
$DYNAMO/utilities/combine-json $DYNAMO/defaults.json $SOURCE/defaults.json > defaults.json.$$
mv defaults.json.$$ $DYNAMO/defaults.json

## INSTALL STANDARD DYNAMO

$DYNAMO/install.sh $INSTALL_CONF
RC=$?

## RESTORE THE STANDARD INSTALLATION DIRECTORY

for FILE in $CLEANUP_LIST
do
  rm -f $DYNAMO/$FILE
done

cp -rf $SOURCE/.tmp/* $DYNAMO/
rm -rf $SOURCE/.tmp

## ABORT IF FAILED

[ $RC -eq 0 ] || exit $RC

## COPY POST-INSTALL ITEMS

source $DYNAMO/utilities/shellutils.sh

READCONF="$DYNAMO/utilities/readconf -I $INSTALL_CONF"

INSTALL_PATH=$($READCONF paths.dynamo_base)
SYSBIN_PATH=$($READCONF paths.sysbin_path)

# Libraries
for PYPATH in $(python -c 'import sys; print " ".join(sys.path)')
do
  if [[ $PYPATH =~ ^/usr/lib/python.*/site-packages$ ]]
  then
    cp -rf $SOURCE/lib/* $PYPATH/dynamo/
    python -m compileall $PYPATH/dynamo > /dev/null
    break
  fi
done

# Dynamo components
for OBJ in exec utilities web
do
  cp -rf $SOURCE/$OBJ $INSTALL_PATH/
done

# CLIs
for FILE in $(ls $SOURCE/sbin)
do
  sed "s|_PYTHON_|$(which python)|" $SOURCE/sbin/$FILE > $SYSBIN_PATH/$FILE
  chmod 744 $SYSBIN_PATH/$FILE
done

# Schedule

cp -f $SOURCE/cms.seq /etc/dynamo/schedule.seq
