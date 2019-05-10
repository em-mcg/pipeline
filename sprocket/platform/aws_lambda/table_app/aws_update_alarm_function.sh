#!/bin/bash

# go to script directory
cd "$(dirname "$0")"

# remove old file zip if it exists
if [ -f function.zip ]; then
  rm function.zip
fi

# make new file zip
zip -r function.zip *

# update function on aws
# assumes that function already exists
aws lambda update-function-code --function-name UpdateAlarmTable --zip-file fileb://function.zip
