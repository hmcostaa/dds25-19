FROM python:3.13-alpine
WORKDIR /usr/src/app/gateway
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 5000
CMD ["python", "app.py"]
RUN apk add --no-cache curl && \
    pip install --no-cache-dir -r requirements.txt

