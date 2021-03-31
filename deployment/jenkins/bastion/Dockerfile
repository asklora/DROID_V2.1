FROM python:3.8
ENV PYTHONUNBUFFERED 1
RUN mkdir code/
COPY . code/
WORKDIR code/
RUN ls -l
# RUN installer/requirement.sh
RUN pip install -r installer/requirements_AI.txt
EXPOSE 8000