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
