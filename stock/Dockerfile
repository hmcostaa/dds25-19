FROM python:3.12-slim

WORKDIR /usr/src/app/stock

ENV PYTHONPATH=/usr/src/app

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY *.py ./

CMD ["python", "app.py"]
