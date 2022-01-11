import string
import subprocess
import os
import os
import mlflow
from minio import Minio
from jinja2 import Template
import time


"""
    This script assumes that the /kaniko/.docker/config.json has the correct repo and associated credentials mounted
    It also expects the these env variables has been set
    CONTAINER_REGISTRY is the resitry server like quay.io
    CONTAINER_DETAILS is the container coordinates like ml-on-k8s/containermodel:1.0.0
    AWS_SECRET_ACCESS_KEY is the password for the S3 store
    MODEL_NAME is hte name of the model in mlflow
    MODEL_VERSION is the version of the model in mlflow
"""



os.environ['MLFLOW_S3_ENDPOINT_URL']='http://minio-ml-workshop:9000'
os.environ['AWS_ACCESS_KEY_ID']='minio'
os.environ['AWS_REGION']='us-east-1'
os.environ['AWS_BUCKET_NAME']='mlflow'

HOST = "http://mlflow:5500"

model_name = os.environ["MODEL_NAME"]
model_version = os.environ["MODEL_VERSION"]
build_name = f"seldon-model-{model_name}-v{model_version}"


def get_s3_server():
    minioClient = Minio('minio-ml-workshop:9000',
                        access_key=os.environ['AWS_ACCESS_KEY_ID'],
                        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                        secure=False)

    return minioClient


def init():
    mlflow.set_tracking_uri(HOST)


def download_artifacts():
    print("retrieving model metadata from mlflow...")
    model = mlflow.pyfunc.load_model(
        model_uri=f"models:/{model_name}/{model_version}"
    )
    print(model)

    run_id = model.metadata.run_id
    experiment_id = mlflow.get_run(run_id).info.experiment_id

    print("initializing connection to s3 server...")
    minioClient = get_s3_server()

    #     artifact_location = mlflow.get_experiment_by_name('rossdemo').artifact_location
    #     print("downloading artifacts from s3 bucket " + artifact_location)

    data_file_model = minioClient.fget_object("mlflow", f"/{experiment_id}/{run_id}/artifacts/model/model.pkl", "model.pkl")
    data_file_requirements = minioClient.fget_object("mlflow", f"/{experiment_id}/{run_id}/artifacts/model/requirements.txt", "requirements.txt")

    #Using boto3 Download the files from mlflow, the file path is in the model meta
    #write the files to the file system
    print("download successful")

    return run_id

def build_push_image():
    container_location = string.Template("$CONTAINER_REGISTRY/$CONTAINER_DETAILS").substitute(os.environ)
    print(subprocess.check_output(['/kaniko/executor', '--context', '/workspace',  '--dockerfile', 'Dockerfile', '--destination', container_location]))


init()
download_artifacts()
build_push_image()

