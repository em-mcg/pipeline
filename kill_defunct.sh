#!/bin/bash

PID=$(ps aux | grep '\[python\] <defunct>' | awk '{ print $2 }')
PID=`echo $PID $(ps aux | grep 'pipeline_daemon' | awk '{ print $2 }' | tr "\n" " ")`

if [ ! -z "$PID" ]; then

  echo "Killing PIDs: " $PID
  kill $PID

else

  echo "Nothing to kill"

fi
