if [ -x "$(command -v aws)" ]; then
	aws configure set aws_access_key_id AKIA2XEOTUNG3F3FQ2NW
	aws configure set aws_secret_access_key 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
	aws configure set default.region ap-east-1
else
	echo 'Error: aws is not installed.' >&2
	echo 'Error: aws credentials not installed' >&2
	apt-get install -yq awscli
	aws configure set aws_access_key_id AKIA2XEOTUNG3F3FQ2NW
	aws configure set aws_secret_access_key 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
	aws configure set default.region ap-east-1
fi
aws ecr get-login-password --region ap-east-1