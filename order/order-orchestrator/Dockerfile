FROM python:3.12-slim

WORKDIR /usr/src/app/order-orchestrator

COPY ./requirements.txt .

RUN pip install -r requirements.txt

# Copy the order directory into the container
COPY ./order /usr/src/app/order

COPY ./order/order-orchestrator/*.py ./

CMD ["python", "app.py"]
