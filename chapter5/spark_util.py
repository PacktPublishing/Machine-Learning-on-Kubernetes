import os
import socket
import sys
import time
import subprocess


# get all installed packages as installed_packages
reqs = subprocess.check_output([sys.executable, '-m', 'pip', 'freeze'])
installed_packages = [r.decode().split('==')[0] for r in reqs.split()]


def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


def get_cluster_name(index = 0, from_env=False):
    # Get the cluster name from env var or from spark-info.txt file

    if os.path.exists('spark-info.txt') and not from_env:
        print("Retrieving spark cluster name from spark-info.txt file...")
        with open('spark-info.txt', 'r') as sparkinfo:
            spark_cluster_name = sparkinfo.readlines()[0 + (index * 2)].replace("\n", "").strip()
        os.environ['SPARK_CLUSTER'] = spark_cluster_name
        print(f"Found new cluster name: {spark_cluster_name}")
        return spark_cluster_name

    if os.environ['SPARK_CLUSTER'].startswith('spark-cluster-'):
        return os.environ['SPARK_CLUSTER']

    return f"spark-cluster-{os.environ['SPARK_CLUSTER']}"


def get_driver_host_ip():
    # Returns the IP address where this process is running on
    ip_address = socket.gethostbyname(socket.gethostname())
    print(f"Driver IP address: {ip_address}")
    return ip_address


def init_environment(submit_args):
    # Prepares Spark related configuration through PYSPARK_SUBMIT_ARGS env variable
    cluster_name = get_cluster_name()
    print(f"Cluter name: {cluster_name}")

    os.environ['PYSPARK_SUBMIT_ARGS'] = f"{submit_args} --master spark://{cluster_name}:7077 pyspark-shell "
#     print(f"PYSPARK_SUBMIT_ARGS: {os.environ['PYSPARK_SUBMIT_ARGS']}")


def getOrCreateSparkSession(application_name, submit_args = "", log_level="INFO"):
    # Creates a Spark session from a given Spark application name and optional submit_arg arguments

    if ("pyspark") not in installed_packages:
        install("pyspark")

    from pyspark import SparkConf
    from pyspark.sql import SparkSession

    print("Initializing environment variables for Spark")
    init_environment(submit_args)
    ip_address = get_driver_host_ip()

    # Tell the executors where the driver is located using IP address instead of domain/pod name
    sparkSessionBuilder = SparkSession\
        .builder\
        .config("spark.local.ip", ip_address)\
        .config("spark.driver.host", ip_address)\
        .config("spark.driver.bindAddress", ip_address)\
        .appName(application_name)

    print("Creating a spark session...")
    spark = sparkSessionBuilder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    print("Spark session created")
    return spark


def wait(predicate, timeout):
    # Evaluates the given predicate function every 5 seconds until it returns true

    mustend = time.time() + timeout
    time.sleep(5)
    while time.time() < mustend:
        try:
            if predicate(1) : return True
        except Exception as ex:
            print(ex)
        time.sleep(5)
    return False


def get_openshift_info():
    # Returns spark API server URL, Token information and current project/namespace

    print("Retrieving Openshift info...")
    # Get the Openshift API URL
    server = "https://" + os.environ["KUBERNETES_SERVICE_HOST"] + ":" + os.environ["KUBERNETES_SERVICE_PORT"] 

    print(f"Kubernetes API server found at: {server}")

    print("Retrieving Openshift token information...")
    # Get the Openshift auth token
    with open('/var/run/secrets/kubernetes.io/serviceaccount/token', 'r') as file:
        token = file.read()
    # print(f"Openshift Token{token}")

    print("Retrieving Openshift project information...")
    # Get the current Openshift project name
    with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as namespace:
        project = namespace.read()
    print(f"Project name: {project}")

    return server, token, project


def start_spark_cluster(cluster_name = "default", worker_nodes = "2", timeout_seconds = 300):
    # Creates a new Spark Cluster kubernetes resource from a given name and number of worker nodes.
    # This fuinction fails if the Spark Cluster does not become ready after the given timeout

    if ("openshift-client") not in installed_packages:
        install("openshift-client")

    if ("jinja2") not in installed_packages:
        install("jinja2")

    import openshift as oc
    from jinja2 import Template

    server, token, project = get_openshift_info()
    spark_crd_name = "SparkCluster"
    cluster_prefix = "spark-cluster"

    # Connect to Openshift
    with oc.api_server(server):
        with oc.token(token):
            with oc.project(project), oc.timeout(10*60):
                print('OpenShift client version: {}'.format(oc.get_client_version()))

                print(f"Searching for SparkCluster with name {cluster_prefix}-{cluster_name}...")
                cluster_count = oc.selector(f"{spark_crd_name}/{cluster_prefix}-{cluster_name}").count_existing()
                print(f"SparkCluster found: {cluster_count}")

                with open('spark-info.txt', 'w') as file:
                    file.write(f"{cluster_prefix}-{cluster_name}\n")
                    file.write(f"{worker_nodes}")

                # Only create Spark cluster if it doesn't exist
                if cluster_count > 0:
                    print(f"Spark Cluster already exists {cluster_prefix}-{cluster_name}")
                    sys.exit(0)

                template_data = {"clustername": f"{cluster_prefix}-{cluster_name}", "workernodes": f"{worker_nodes}", "project": project}
                applied_template = Template(open("spark-cluster.yaml").read())

                print(f"Creating SparkCluster {cluster_prefix}-{cluster_name} ...")
                # print(applied_template.render(template_data))
                oc.create(applied_template.render(template_data))

                route_template = Template(open("spark-cluster-route.yaml").read())
                print("Creating or updating route for Spark UI... ")
                oc.apply(route_template.render(template_data))

                # predicate function to check if the master node and all worker nodes are ready
                cluster_ready = lambda _: \
                oc.selector(f"replicationcontroller/{cluster_prefix}-{cluster_name}-m")\
                .object().model.status.can_match({ 'readyReplicas': 1 }) &\
                oc.selector(f"replicationcontroller/{cluster_prefix}-{cluster_name}-w")\
                .object().model.status.can_match({ 'readyReplicas': int(worker_nodes) })

                # wait for cluster to be ready
                print("Waiting for spark cluster to be ready...")
                ready = wait(cluster_ready, timeout_seconds)

                if not ready:
                    print(f"Cluster was not ready after a given timeout {timeout}s")
                    sys.exit(1)

                print(f"SparkCluster {cluster_prefix}-{cluster_name} is ready.")


def stop_spark_cluster(cluster_name = "default"):
    # Deletes SparkCluster Kubernetes resource with the given name

    if ("openshift-client") not in installed_packages:
        install("openshift-client")

    import openshift as oc

    server, token, project = get_openshift_info()
    spark_crd_name = "SparkCluster"

    # Connect to openshift API server
    with oc.api_server(server):
        with oc.token(token):
            with oc.project(project), oc.timeout(10*60):
                print('OpenShift client version: {}'.format(oc.get_client_version()))

                print(f"Searching for SparkCluster with name {cluster_name}...")
                cluster_count = oc.selector(f"{spark_crd_name}/{cluster_name}").count_existing()
                print(f"SparkCluster found: {cluster_count}")

                print(cluster_count)
                if cluster_count > 0:
                    print(f"Deleting cluster {cluster_name}")
                    oc.oc_action(oc.cur_context(), "delete", cmd_args=[spark_crd_name, f"{cluster_name}"])
                    print("SparkCluster deleted")
                else:
                    print(f"Spark Cluster does not exists {cluster_name}")