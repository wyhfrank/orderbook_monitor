FROM python:3-slim-buster
WORKDIR /work
COPY . .
RUN pip install -r requirements.txt
