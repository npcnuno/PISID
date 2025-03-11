
FROM python:3.9-slim
WORKDIR /app
COPY mongodb_to_mysql.py .
RUN pip install pymongo mysql-connector-python
CMD ["python", "mongodb_to_mysql.py"]
