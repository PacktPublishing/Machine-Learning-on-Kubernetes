#FROM python:3.8-slim-buster
FROM registry.access.redhat.com/ubi8/python-38:1-77

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY model_event_listener.py .

CMD [ "python3", "model_event_listener.py" ]