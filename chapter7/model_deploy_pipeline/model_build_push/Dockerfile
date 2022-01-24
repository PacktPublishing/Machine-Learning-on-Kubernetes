FROM registry.access.redhat.com/ubi8/python-38:1-77

WORKDIR /microservice
COPY base_requirements.txt /microservice/
RUN pip install -r base_requirements.txt

COPY requirements.txt /microservice/
RUN pip install -r requirements.txt

COPY Predictor.py   model.pkl /microservice/

CMD seldon-core-microservice $MODEL_NAME --service-type $SERVICE_TYPE --grpc-port ${GRPC_PORT} --metrics-port ${METRICS_PORT} --http-port ${HTTP_PORT}


#FROM quay.io/ml-aml-workshop/python:3.7-slimv2
