#!/bin/bash
if ! [ -x "$(command -v jq)" ]; then
  echo 'Error: jq is not installed.' >&2
  sudo apt-get install jq
fi
if ! [ -x "$(command -v jq)" ]; then
  echo 'Error: aws is not installed.' >&2
  sudo apt-get install awscli
fi
tar -xzf ta-lib-0.4.0-src.tar.gz
cd ta-lib/
./configure --prefix=/usr
make
sudo make install
export AWS_ACCESS_KEY_ID=AKIA2XEOTUNGWEQ43TB6
export AWS_SECRET_ACCESS_KEY=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
export AWS_DEFAULT_REGION=ap-east-1
