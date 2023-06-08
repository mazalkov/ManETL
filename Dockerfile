# FIXME: Perms issuse with running python in an airflow image

FROM apache/airflow:2.6.1

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

RUN sudo python setup.py install

