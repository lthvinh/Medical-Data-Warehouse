from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import types as T
from pyspark.sql import functions as F
from delta import *
import yaml

def main():
    with open('/opt/etl/config.yaml') as config_file: 
        configs = yaml.safe_load(config_file)

    spark_configs = configs['spark']
    spark_config =  spark_configs['config']
    spark_packages = spark_configs['packages']

    spark_session_builder = (
        SparkSession
        .builder
        .config(map = spark_config)
    )

    spark = configure_spark_with_delta_pip(
        spark_session_builder = spark_session_builder
        , extra_packages = spark_packages
    ).getOrCreate()

    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])


    df.write.mode("overwrite").format('delta').option('path', "s3a://medical-bucket/temp/spark_output5").saveAsTable('spark_output5')

    spark.stop()

if __name__ == "__main__":
    main()
