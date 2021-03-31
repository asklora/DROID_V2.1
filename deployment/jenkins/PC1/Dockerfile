FROM python:3.8
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
COPY installer/requirements_AI.txt /code/
RUN pip install -r requirements_AI.txt
COPY . /code/