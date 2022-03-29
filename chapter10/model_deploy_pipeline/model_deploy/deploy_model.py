import openshift as oc
import os, time
from jinja2 import Template
from mlflow.tracking import MlflowClient
import mlflow

HOST = "http://mlflow:5500"

model_name = os.environ["MODEL_NAME"]
model_version = os.environ["MODEL_VERSION"]
cluster_dns = os.environ["CLUSTER_DOMAIN_NAME"]

ingress_host = f"{model_name}.{cluster_dns}"

build_name = f"seldon-model-{model_name}-v{model_version}"

mlflow.set_tracking_uri(HOST)

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


model_container_location = os.environ["CONTAINER_DETAILS"]

if os.environ.get("CONTAINER_REGISTRY") is not None and os.environ["CONTAINER_REGISTRY"].find("docker.io") == -1:
    model_container_location = os.environ["CONTAINER_REGISTRY"] + "/" + os.environ["CONTAINER_DETAILS"]

server = "https://" + os.environ["KUBERNETES_SERVICE_HOST"] + ":" + os.environ["KUBERNETES_SERVICE_PORT"]
print(server)

#build from source Docker file
with open('/var/run/secrets/kubernetes.io/serviceaccount/token', 'r') as file:
    token = file.read()
print(f"Access Token{token}")

#/var/run/secrets/kubernetes.io/serviceaccount
with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as namespace:
    project = namespace.read()
print(f"Namespace: {project}")

with oc.api_server(server):
    with oc.token(token):
        with oc.project(project), oc.timeout(10*60):
            template_data = {"experiment_id": run_id, "model_name": model_name, "model_coordinates": model_container_location, "ingress_host": ingress_host}
            applied_template = Template(open("SeldonDeploy.yaml").read())
            
            rendered_template = applied_template.render(template_data)
            print('Rendered Template: \r' + rendered_template)
            oc.apply(rendered_template)
            
            service_name = "model-" + run_id + "-" + model_name
            ingress_count = oc.selector(f"ingress/{service_name}").count_existing()
            print(ingress_count)
            if ingress_count == 0:
                service_name = "model-" + run_id + "-" + model_name
                while True:
                    service_count = oc.selector(f"service/{service_name}").count_existing()
                    if service_count > 0:
                        service = oc.selector(f"service/{service_name}").object()
                        print(service.name())
                        applied_template = Template(open("Ingress.yaml").read())
                        print(applied_template.render(template_data))
                        oc.apply(applied_template.render(template_data))
                        break
                    else:
                        print(f"Service name does not exist {service_name}")
                        time.sleep(10)
            else:
                print(f"Ingress already exists for {service_name}")