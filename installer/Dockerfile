FROM python:3.8
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
COPY . /code/
RUN apt update
RUN ./requirement.sh
RUN installer/python_dependency.sh
EXPOSE 8000