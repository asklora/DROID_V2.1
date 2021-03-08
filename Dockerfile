FROM python:3.8
#<<<<<<< HEAD
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
COPY . /code/
RUN apt update
RUN installer/requirement.sh
RUN pip3 install -r requirements.txt
RUN pip3 uninstall numpy -y
RUN pip3 install numpy
#RUN pip3 install -r requirements_no_AI.txt
EXPOSE 8000
#=======
#COPY . /droid-v2
#WORKDIR /droid-v2
#RUN pip install --upgrade pip
#RUN installer/requirement.sh
#RUN pip install -r requirements_no_AI.txt
#EXPOSE 8000
#ENTRYPOINT [ "python" ]
#CMD [ "./manage.py runserver" ]
#>>>>>>> cf560d4c6e990a80afbe24c65cd253716c17aec5
