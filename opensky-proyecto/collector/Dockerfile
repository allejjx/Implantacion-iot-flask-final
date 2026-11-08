FROM python:3.11

WORKDIR /app

COPY collector.py .

RUN pip install requests paho-mqtt

CMD ["python", "-u", "collector.py"]
