FROM python:3.6
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
COPY . /code/
RUN apt update
RUN installer/requirement.sh
RUN pip3 install -r requirements_no_AI.txt
EXPOSE 8000
RUN echo $PWD
RUN ./run-django.sh
#ENTRYPOINT [ "python manage.py runserver" ]
#CMD [ "python ./manage.py runserver" ]
#RUN cd code && python manage.py runserver