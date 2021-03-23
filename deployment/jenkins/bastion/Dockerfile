FROM python:3.8
ENV PYTHONUNBUFFERED 1
RUN mkdir code/
COPY . code/
WORKDIR code/
RUN ls -l
RUN installer/requirement.sh
RUN installer/python_dependency.sh
EXPOSE 8000
CMD python manage.py runserver 0.0.0.0:8000