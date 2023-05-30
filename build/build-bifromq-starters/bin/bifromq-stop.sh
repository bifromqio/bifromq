#!/bin/bash

#
# Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#    http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.
#

if [ $# -lt 1 ];
then
  echo "USAGE: $0 servicename"
  exit 1
fi

NAME=$1

SIGNAL=${SIGNAL:-TERM}

PIDS=$(ps -ef | grep $NAME | grep java | grep -v grep | awk '{print $2}')

if [ -z "$PIDS" ]; then
  echo "No $NAME to stop"
  exit 1
else
  echo "Find $NAME process $PIDS, and stopping it"
  kill -s $SIGNAL $PIDS
fi

ret=1
for i in {1..300}
do
    ps -ef | grep $NAME | grep java | grep -v grep >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "$NAME process $PIDS was stopped"
        ret=0
        break
    fi
    sleep 1
done

if [ $ret -eq 1 ]; then
    echo "Wait process stop timeout: $NAME does not stop for more than 300s!"
fi
exit $ret
