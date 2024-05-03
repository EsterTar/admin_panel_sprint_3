FROM python:3.10

WORKDIR /opt/app

ENV DJANGO_SETTINGS_MODULE 'config.settings'

COPY entrypoint.sh entrypoint.sh
RUN chmod +x entrypoint.sh

COPY requirements.txt requirements.txt
COPY uwsgi/uwsgi.ini uwsgi.ini

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt --no-cache-dir

COPY . .

EXPOSE 8000

RUN adduser --disabled-password devops
USER devops

ENTRYPOINT ["bash", "entrypoint.sh"]
