FROM python:3-slim
WORKDIR /programas/etl
RUN pip3 install mysql-connector-python pandas boto3
COPY . .
CMD ["python3", "main.py"]