FROM python:3.12-slim

RUN pip install pika

ENV PYTHONUNBUFFERED=1

WORKDIR /home/flask-app

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000
