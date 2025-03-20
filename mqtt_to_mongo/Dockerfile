FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .


CMD ["python", "mqtt_to_mongodb.py", "add_to_sessions.py","delete_processed_messages.py", "mark_messages_as_sent.py" ]
