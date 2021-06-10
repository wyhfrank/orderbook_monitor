FROM python:3-slim-buster

# Dependency for psycopg2
# https://stackoverflow.com/a/67468970/1938012
RUN apt-get update && apt-get -y install libpq-dev gcc
WORKDIR /work
COPY . .
RUN pip install -r requirements.txt
