FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "app.py"]
