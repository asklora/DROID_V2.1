#!/bin/bash
if [ -x "$(command -v aws)" ]; then
	aws configure set aws_access_key_id AKIA2XEOTUNGWEQ43TB6
	aws configure set aws_secret_access_key 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
	aws configure set default.region ap-east-1
else
	echo 'Error: aws is not installed.' >&2
	echo 'Error: aws credentials not installed' >&2
	apt-get install -yq awscli
	aws configure set aws_access_key_id AKIA2XEOTUNGWEQ43TB6
	aws configure set aws_secret_access_key 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
	aws configure set default.region ap-east-1
fi
aws ecr get-login-password --region ap-east-1 | docker login --username AWS --password-stdin 736885973837.dkr.ecr.ap-east-1.amazonaws.com
sudo rm -rf /home/ubuntu/DROID_V2.1