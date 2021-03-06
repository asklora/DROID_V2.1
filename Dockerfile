FROM python:3.8
COPY . /droid-v2
WORKDIR /droid-v2
RUN pip install --upgrade pip
RUN pip install --no-binary --allow-all-external -r requirements_no_AI.txt
EXPOSE 8000
ENTRYPOINT [ "python" ]
CMD [ "./manage.py runserver" ]
