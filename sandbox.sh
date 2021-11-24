#!/bin/bash
dockercheck(){
if [ -x "$(command -v docker)" ]; then
echo "Docker installed"
else
echo "Docker not installed"
if [ "$(uname)" == "Darwin" ]; then
echo "Please install docker here https://docs.docker.com/desktop/mac/install/"
exit 1
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
	./installer/install-docker.sh
fi
fi
}
composecheck(){
if [ -x "$(command -v docker-compose)" ]; then
echo "Docker Compose installed"
else
echo "ERROR: Docker Compose not installed"
exit 1
fi
}


login(){
if [ -x "$(command -v aws)" ]; then
	aws configure set aws_access_key_id AKIA2XEOTUNGWEQ43TB6
	aws configure set aws_secret_access_key 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
	aws configure set default.region ap-east-1
else
	echo 'Error: aws is not installed.' >&2
	echo 'Error: aws credentials not installed' >&2
    if [ "$(uname)" == "Darwin" ]; then
    curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
    sudo installer -pkg AWSCLIV2.pkg -target /
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
appurl(){
    echo "droid url: http://127.0.0.1:8000"
}
up(){
dockercheck
composecheck
login
docker-compose -f local.yml up -d --force-recreate
appurl

}

checkapps(){
    droidresponse=$(curl --write-out '%{http_code}' --silent --output /dev/null http://0.0.0.0:8000)
    if [ $droidresponse == 200 ]; then
    echo "droid response: $droidresponse Ready"
    else
    echo "droid response: $droidresponse app not running"
    fi
}



down(){
docker-compose -f local.yml down

}

djangologs(){
    docker logs --follow droid
}
celerylogs(){
    docker logs --follow Celery
}


droidrun(){
    docker exec -it django bash -c "$commands"
}



help(){
    echo "./sandbox.sh up -> run aplication"
    echo "./sandbox.sh down -> stop aplication and destroy"
    echo "./sandbox.sh djangologs -> log django app"
    echo "./sandbox.sh celerylogs -> log celery app"
    echo "./sandbox.sh checkapps -> Check status app"
    echo "./sandbox.sh appurl -> log asklora app"
    echo "./sandbox.sh droidrun 'your command' -> run command inside container droid"

}
commands=$2
$1