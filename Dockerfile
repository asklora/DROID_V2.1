FROM python:3.8
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
RUN ls -la *
WORKDIR /code
COPY . /code/
RUN pwd
RUN installer/requirement.sh
RUN installer/python_dependency.sh
EXPOSE 8000