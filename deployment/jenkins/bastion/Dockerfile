FROM python:3.8
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
COPY . /code/
CMD echo $PWD
RUN /code/installer/requirement.sh
RUN /code/installer/python_dependency.sh
EXPOSE 8000