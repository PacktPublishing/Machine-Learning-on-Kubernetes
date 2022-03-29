import string
import subprocess
import os
import base64
import mlflow
from minio import Minio
from mlflow.tracking import MlflowClient


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

auth_encoded = string.Template("$CONTAINER_REGISTRY_USER:$CONTAINER_REGISTRY_PASSWORD").substitute(os.environ)
os.environ["CONTAINER_REGISTRY_CREDS"] = base64.b64encode(auth_encoded.encode("ascii")).decode("ascii")

docker_auth = string.Template('{"auths":{"$CONTAINER_REGISTRY":{"auth":"$CONTAINER_REGISTRY_CREDS"}}}').substitute(os.environ)
print(docker_auth)
f = open("/kaniko/.docker/config.json", "w")
f.write(docker_auth)
f.close()

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
    # model = mlflow.pyfunc.load_model(
    #     model_uri=f"models:/{model_name}/{model_version}"
    # )
    client = MlflowClient()

    model = client.get_registered_model(model_name)

    print(model)
    
    for latest_version in model.latest_versions:
        if latest_version.version != model_version:
            continue

        run_id = latest_version.run_id
        source = latest_version.source
        experiment_id = latest_version.source.split("/")[3]
        print(latest_version)    

    # run_id = model._latest_version[0].run_id
    # source = model._latest_version[0].source
    # experiment_id = "1" # to be calculated from the source which is source='s3://mlflow/1/bf721e5641394ed6866baf20131fca20/artifacts/model'

    print("initializing connection to s3 server...")
    minioClient = get_s3_server()

    #     artifact_location = mlflow.get_experiment_by_name('rossdemo').artifact_location
    #     print("downloading artifacts from s3 bucket " + artifact_location)

    data_file_model = minioClient.fget_object("mlflow", f"/{experiment_id}/{run_id}/artifacts/model/model.pkl", "model.pkl")
    data_file_requirements = minioClient.fget_object("mlflow", f"/{experiment_id}/{run_id}/artifacts/model/requirements.txt", "requirements.txt")
    
    #download all the pkl files from this location
    pkl_objects = minioClient.list_objects("mlflow", recursive=True, prefix=f"/{experiment_id}/{run_id}")
    for pkl_object in pkl_objects:
        print(pkl_object)
        pkl_object_name = pkl_object.object_name
        if pkl_object_name.endswith('pkl'):
            minioClient.fget_object('mlflow', pkl_object_name, pkl_object_name.split("/")[-1])
      
    #Using boto3 Download the files from mlflow, the file path is in the model meta
    #write the files to the file system
    print("download successful")

    return run_id

def build_push_image():
    container_location = string.Template("$CONTAINER_REGISTRY/$CONTAINER_DETAILS").substitute(os.environ)
    
    #For docker repo, do not include the registry domain name in container location
    if os.environ["CONTAINER_REGISTRY"].find("docker.io") != -1:
        container_location= os.environ["CONTAINER_DETAILS"]
        
    full_command = "/kaniko/executor --context=" + os.getcwd() + " --dockerfile=Dockerfile --verbosity=debug --cache=true --single-snapshot=true --destination=" + container_location
    print(full_command)
    process = subprocess.run(full_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(process.stdout)
    print(process.stderr)

    # print(subprocess.check_output(['/kaniko/executor', '--context', '/workspace',  '--dockerfile', 'Dockerfile', '--destination', container_location]))


init()
download_artifacts()
build_push_image()

