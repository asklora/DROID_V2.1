#!/bin/bash
if ! [ -x "$(command -v jq)" ]; then
  echo 'Error: jq is not installed.' >&2
  apt-get install -yq jq
fi
if ! [ -x "$(command -v aws)" ]; then
  
  apt-get install -yq awscli
fi
apt update
tar -xzf ./installer/ta-lib-0.4.0-src.tar.gz
cd ./ta-lib
./configure --prefix=/usr
make
make install
if [ -x "$(command -v aws)" ]; then
aws configure set aws_access_key_id AKIA2XEOTUNGWEQ43TB6
aws configure set aws_secret_access_key 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
aws configure set default.region ap-east-1
else
  echo 'Error: aws is not installed.' >&2
  echo 'Error: aws credentials not installed' >&2
fi
