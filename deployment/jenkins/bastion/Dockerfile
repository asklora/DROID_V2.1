FROM 736885973837.dkr.ecr.ap-east-1.amazonaws.com/app-test-env:latest
ENV PYTHONUNBUFFERED 1
RUN mkdir code/
COPY . code/
WORKDIR code/
RUN ls -l
# RUN installer/requirement.sh
# RUN pip install -r installer/requirements_no_AI.txt
EXPOSE 8000