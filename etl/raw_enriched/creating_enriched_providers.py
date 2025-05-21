from pyspark.sql import SparkSession
from pyspark import SparkConf
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

    spark.catalog.setCurrentDatabase(enriched_database)

    raw_providers__file_name = 'providers.csv'
    enriched_providers__table_name = 'enriched_providers'
    raw_providers__full_path = f'{raw_path}/{raw_providers__file_name}'
    enriched_providers__full_path = f'{enriched_path}/{enriched_providers__table_name}'
    
    raw_providers__schema = T.StructType([
        T.StructField('ID', T.StringType(), True)
        , T.StructField('ORGANIZATION', T.StringType(), True)
        , T.StructField('NAME', T.StringType(), True)
        , T.StructField('GENDER', T.StringType(), True)
        , T.StructField('SPECIALITY', T.StringType(), True)
        , T.StructField('ADDRESS', T.StringType(), True)
        , T.StructField('CITY', T.StringType(), True)
        , T.StructField('STATE', T.StringType(), True)
        , T.StructField('ZIP', T.LongType(), True)
        , T.StructField('LAT', T.DoubleType(), True)
        , T.StructField('LON', T.DoubleType(), True)
        , T.StructField('ENCOUNTERS', T.LongType(), True)
        , T.StructField('PROCEDURES', T.LongType(), True)
    ])
    
    raw_providers__df = (
        spark.read.format('csv')
        .options(**{
            'path': raw_providers__full_path
            , 'header': True
            , 'inferSchema': False
        })
        .schema(raw_providers__schema)
        .load()
        .dropDuplicates()
    )
    
    enriched_providers__df = raw_providers__df.withColumn(
        'HASH', F.concat_ws('|', *[F.col(column_name) for column_name in raw_providers__df.columns])
    )
    
    enriched_providers__is_existing = DeltaTable.isDeltaTable(spark, enriched_providers__full_path)
    if not enriched_providers__is_existing:
        enriched_providers__df.write.format('delta').mode('overwrite').option('path', enriched_providers__full_path).saveAsTable(enriched_providers__table_name)
    else:
        enriched_providers__delta_table = DeltaTable.forName(spark, enriched_providers__table_name)
        (
            enriched_providers__delta_table.alias('target').merge(
                source = enriched_providers__df.alias('source')
                , condition = (F.col('source.HASH') == F.col('target.HASH'))
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        
if __name__ == '__main__':
    main()