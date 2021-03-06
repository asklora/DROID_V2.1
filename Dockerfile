FROM python:3
COPY . /droid-v2
WORKDIR /droid-v2
RUN pip3 install --upgrade pip
RUN pip3 install --no-binary --allow-all-external -r requirements_no_AI.txt
EXPOSE 8000
ENTRYPOINT [ "python" ]
CMD [ "./manage.py runserver" ]
