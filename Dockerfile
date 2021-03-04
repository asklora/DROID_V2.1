FROM django
COPY . /droid-v2
WORKDIR /droid-v2
RUN pip3 install --no-use-wheel --allow-all-external -r requirements.txt
EXPOSE 8000
ENTRYPOINT [ "python" ]
CMD [ "./manage.py runserver" ]
