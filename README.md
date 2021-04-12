## Command dont delete
### docker login
- aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin 736885973837.dkr.ecr.ap-northeast-2.amazonaws.com
### docker build and push to ecr
- docker build . -t celery-portfolio-job -f deployment/batch/Dockerfile.celery 
- docker tag celery-portfolio-job:latest 736885973837.dkr.ecr.ap-northeast-2.amazonaws.com/celery-portfolio-job:latest
- docker push 736885973837.dkr.ecr.ap-northeast-2.amazonaws.com/celery-portfolio-job:latest 