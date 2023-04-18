import os
import sys
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, IntegerType

try:
    import sys
    import ast
    import datetime
    from ast import literal_eval
    import re
    import boto3
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    import os
    import json
    from dataclasses import dataclass
    from pyspark.sql.functions import from_json, col
    import pyspark.sql.functions as F
except Exception as e:
    pass


def main():
    SUBMIT_ARGS = "--packages org.postgresql:postgresql:42.5.4 pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder.appName('IncrementalJDBC') \
        .getOrCreate()

    # set up connection parameters
    jdbc_url = 'jdbc:postgresql://localhost:5432/postgres'
    table_name = 'public.sales'
    user = 'postgres'
    password = 'postgres'
    pk = "salesid"

    # read the maximum value of the primary key from the last extraction
    max_id_checkpoint_dir = "./checkpoint/max_id"
    if os.path.exists(max_id_checkpoint_dir):
        max_id = spark.read.csv(max_id_checkpoint_dir).collect()[0][0]
        print(f"Checkpoint found, resuming from {max_id}")
    else:
        print("Checkpoint not found, starting from scratch.")
        max_id = 0

    incremental_query = f"SELECT * FROM {table_name} WHERE {pk} > {max_id}"

    # read the incremental data from the database
    incremental_data = spark.read.format('jdbc').options(
        url=jdbc_url,
        query=incremental_query,
        user=user,
        password=password,
        driver='org.postgresql.Driver'
    ).load()

    if incremental_data.count() > 0:
        max_id = incremental_data.agg({pk: "max"}).collect()[0][0]
        spark_df = spark.createDataFrame(data=[(str(max_id), table_name)], schema=['max_id', "table_name"])
        print("spark_df", spark_df.show())
        spark_df.write.mode("overwrite").csv(max_id_checkpoint_dir)
    else:
        print("No data to Process ")

    print(incremental_data.show())


if __name__ == "__main__":
    main()
