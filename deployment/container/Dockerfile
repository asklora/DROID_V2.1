
FROM python:3.6
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
# COPY requirements.txt /code/
COPY . /code/
RUN apt update
RUN installer/requirement.sh
# RUN pip install -r requirements.txt
# ENV DOCKERVERSION=19.03.12
# RUN curl -fsSLO https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKERVERSION}.tgz \
#     && tar xzvf docker-${DOCKERVERSION}.tgz --strip 1 -C /usr/local/bin docker/docker \
#     && rm docker-${DOCKERVERSION}.tgz

# EXPOSE 8000