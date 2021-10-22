#!/bin/bash
login(){
if [ -x "$(command -v aws)" ]; then
	aws configure set aws_access_key_id AKIA2XEOTUNGWEQ43TB6
	aws configure set aws_secret_access_key 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
	aws configure set default.region ap-east-1
else
	echo 'Error: aws is not installed.' >&2
	echo 'Error: aws credentials not installed' >&2
    if [ "$(uname)" == "Darwin" ]; then
    cd /tmp
    curl -o awscli.zip https://s3.amazonaws.com/aws-cli/awscli-bundle.zip
    unzip awscli.zip
    sudo ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws
    # Do something under Mac OS X platform        
    elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    # Do something under GNU/Linux platform
	apt-get install -yq awscli
    fi
	aws configure set aws_access_key_id AKIA2XEOTUNGWEQ43TB6
	aws configure set aws_secret_access_key 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
	aws configure set default.region ap-east-1
fi
aws ecr get-login-password --region ap-east-1 | docker login --username AWS --password-stdin 736885973837.dkr.ecr.ap-east-1.amazonaws.com

}

up(){
login
docker-compose -f local.yml up -d --force-recreate
echo "droid url: http://127.0.0.1:8000"
echo "asklora url: http://127.0.0.1:9000"

}

checkapps(){
    droidresponse=$(curl --write-out '%{http_code}' --silent --output /dev/null http://0.0.0.0:8000)
    askloraresponse=$(curl --write-out '%{http_code}' --silent --output /dev/null http://0.0.0.0:9000)
    if [ $droidresponse == 200 ]; then
    echo "droid response: $droidresponse Ready"
    else
    echo "droid response: $droidresponse app not running"
    fi

    if [ $askloraresponse == 200 ]; then
    echo "asklora response: $askloraresponse Ready"
    else
    echo "asklora response: $askloraresponse app not running"
    fi
}



down(){
docker-compose -f local.yml down

}

djangologs(){
    docker logs --follow django
}
celerylogs(){
    docker logs --follow Celery
}
askloralogs(){
    docker logs --follow asklora
}


help(){
    echo "./sandbox up -> run aplication"
    echo "./sandbox down -> stop aplication and destroy"
    echo "./sandbox djangologs -> log django app"
    echo "./sandbox celerylogs -> log celery app"
    echo "./sandbox askloralogs -> log asklora app"

}
$1