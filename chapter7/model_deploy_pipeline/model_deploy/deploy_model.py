import openshift as oc
import os, time
from jinja2 import Template



model_name = os.environ["MODEL_NAME"]
model_version = os.environ["MODEL_VERSION"]
build_name = f"seldon-model-{model_name}-v{model_version}"

#temp
run_id = "1"

model_container_location = os.environ["CONTAINER_REGISTRY"] + "/" + os.environ["CONTAINER_DETAILS"]



server = "https://" + os.environ["KUBERNETES_SERVICE_HOST"] + ":" + os.environ["KUBERNETES_SERVICE_PORT"]
print(server)

#build from source Docker file
with open('/var/run/secrets/kubernetes.io/serviceaccount/token', 'r') as file:
    token = file.read()
print(f"Openshift Token{token}")

#/var/run/secrets/kubernetes.io/serviceaccount
with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as namespace:
    project = namespace.read()
print(f"Project name: {project}")

with oc.api_server(server):
    with oc.token(token):
        with oc.project(project), oc.timeout(10*60):
            template_data = {"experiment_id": run_id, "model_name": model_name, "image_name": build_name, "model_coordinates": model_container_location}
            applied_template = Template(open("SeldonDeploy.yaml").read())
            print(applied_template.render(template_data))

            oc.apply(applied_template.render(template_data))

            ingress_count = oc.selector(f"ingress/{build_name}").count_existing()
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
                print(f"Route already exists for {build_name}")