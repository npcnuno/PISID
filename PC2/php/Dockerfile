# Use an official lightweight Python image
FROM python:3.9-slim

# set workdir
WORKDIR /app

# copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy the rest of your application code
COPY . .

# expose Flask’s default port
EXPOSE 5000

# run the app — in production you might swap this for gunicorn
CMD ["python", "app.py"]
