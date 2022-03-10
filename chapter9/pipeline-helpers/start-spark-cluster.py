import os
import spark_util

cluster_name = os.environ["SPARK_CLUSTER"]
worker_nodes = os.environ["WORKER_NODES"]

if os.getenv("worker_nodes") is None:
    worker_nodes = "2"
    
spark_util.start_spark_cluster(cluster_name, worker_nodes)
