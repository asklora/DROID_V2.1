FROM python:3.8
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
COPY . /code/
CMD $PWD
RUN ../../installer/requirement.sh
RUN ../../installer/python_dependency.sh
EXPOSE 8000