import spark_util
import os

if 'SPARK_CLUSTER' in os.environ:
    cluster_name = spark_util.get_cluster_name(from_env=True)
else:
    cluster_name = spark_util.get_cluster_name()

spark_util.stop_spark_cluster(cluster_name)
