## Run locally
- set `./sandbox help` see available commands

## Commands don't delete

- set `testbuild` as commit message to trigger aws test
- set `dev-run` as commit message in branch dev to trigger dev server update
- set `test-noimage` as commit message in branch master to prod server update
- set `'build-base-image` as commit message to trigger build base image and push to ecr

### docker login
- aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin 736885973837.dkr.ecr.ap-northeast-2.amazonaws.com

### docker build and push to ecr

- docker build . -t celery-portfolio-job -f deployment/batch/Dockerfile.celery
- docker tag celery-portfolio-job:latest 736885973837.dkr.ecr.ap-northeast-2.amazonaws.com/celery-portfolio-job:latest
- docker push 736885973837.dkr.ecr.ap-northeast-2.amazonaws.com/celery-portfolio-job:latest

# asklora install

### requirements need to meet

- python 3.8
- os ubuntu >= 20

### install step

1. Run talib and aws dependency
   - `sudo ./installer/requirement.sh`
2. Run install python requirements in activated environment
   - `pip install -r installer/requirements_no_AI.txt`

### requirements need to meet

- python 3.6
- os ubuntu >= 18.0

### install step
1. Run talib and aws dependency
   - `sudo ./installer/requirement.sh`
2. Run install python requirements in activated env
   - `pip install -r installer/requirements_no_AI_36.txt`
