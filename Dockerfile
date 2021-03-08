FROM python:3.8
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
COPY . /code/
RUN apt update
RUN installer/requirement.sh
RUN pip3 install -r requirements_no_AI.txt
EXPOSE 8000