## Flow
![flow](/svg/TPDDJyCm38Rl-HMcvoAD7P66fcc22q8xm7bobvH5IvEIEDluzpZzMDD9t1h7x-l7TheEX9vYO-0defL62D9a51HR98JQk0ku6hRUQNbU5a8yq54r2iwwycX9Y73IjaMF3OGZIdVHzXjgD45bDrBL60szEaiVoEAlAbgAMSAE4VH1j-ouGocJntAHtM_STOm_BOIetBNdgp2eGhIJ6gBLP-MvUXui9EVkPGWb35-NK006IFezsnWLve860oc_TqTpE9ClOJWJ7ZfTRDb7oL6gh0Hccbjqymik-eFgYmtVGgnMi4LjVg7rZWC1T00VBQnMmbj72XotSEkad56-wq1f8LpdnBT9IdXI5chlcgdxH9Iin1Y1nvSH09xXJH6bOV8kXrvwVJgRUGMHCsKavQsVJnQbiOAh2BLLilSQgp5X6mcq9TVf8HjQXpWcCc7cpvIRAHlGZwHI_8zqjvV-2IUOwivgc3oRyPjdjaJ8VDCyP9yzKUPeJ3glnB1ThTJhwCoktLYwqjv57dXQ_feT-M_x0m00 "flow")

## Commands don't delete

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
