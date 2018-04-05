#!/usr/bin/env bash

pip  install -t python_modules -r requirements.txt
ENV= production

if [ -n "$1" ];
then
    ENV=$1
fi

if [ -z "$2" ];
then
    echo "No AWS Profile set using default"
    sls deploy --stage $ENV --verbose
else
    echo "Using giving AWS Profile"
    echo $ENV
    sls deploy --stage $ENV --verbose --aws-profile $2
fi
