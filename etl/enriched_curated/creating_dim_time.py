from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from delta import *
import yaml

def main():
    with open('/opt/etl/config.yaml') as config_file:
        configs = yaml.safe_load(config_file)

    paths_configs = configs['paths']
    raw_path = paths_configs['raw']
    enriched_path = paths_configs['enriched']
    curated_path= paths_configs['curated']
    
    metastore_configs = configs['metastore']
    enriched_database = metastore_configs['enriched']
    curated_database = metastore_configs['curated']
    
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

    spark.catalog.setCurrentDatabase(curated_database)

    dim_time__table_name = 'dim_time'
    dim_time__full_path = f'{curated_path}/{dim_time__table_name}'
    dim_time__is_existing = DeltaTable.isDeltaTable(spark, dim_time__full_path)
    
    if not dim_time__is_existing:
        creating_dim_time__sql = '''
            WITH hours AS (
                SELECT explode(sequence(0, 23, 1)) as Hour
            )
            , minutes AS (
                SELECT explode(sequence(0, 59, 1)) as Minute
            )
            , seconds AS (
                SELECT explode(sequence(0, 59, 1)) as Second
            )
            , dim_time as (
                SELECT
                    CAST(CONCAT_WS('', Hour, Minute, Second) AS INT) AS Time_Key
                    , CONCAT_WS(':', LPAD(Hour, 2, 0), LPAD(Minute, 2, 0), LPAD(Second, 2, 0)) as Time
                    , Hour
                    , Minute
                    , Second
                FROM hours AS h
                CROSS JOIN minutes AS m
                CROSS JOIN seconds AS s
            )
            select * from dim_time
        '''
        dim_time__df = spark.sql(creating_dim_time__sql)
        dim_time__df.write.mode('overwrite').format('delta').option('path', dim_time__full_path).saveAsTable(dim_time__table_name)

if __name__ == '__main__':
    main()