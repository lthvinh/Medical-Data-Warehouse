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

    raw_immunizations__file_name = 'immunizations.csv'
    enriched_immunizations__table_name = 'enriched_immunizations'
    raw_immunizations__full_path = f'{raw_path}/{raw_immunizations__file_name}'
    enriched_immunizations__full_path = f'{enriched_path}/{enriched_immunizations__table_name}'
    
    raw_immunizations__schema = T.StructType([
        T.StructField('DATE', T.TimestampType(), True)
        , T.StructField('PATIENT', T.StringType(), True)
        , T.StructField('ENCOUNTER', T.StringType(), True)
        , T.StructField('CODE', T.LongType(), True)
        , T.StructField('DESCRIPTION', T.StringType(), True)
        , T.StructField('BASE_COST', T.DoubleType(), True)
    ])
    
    raw_immunizations__df = (
        spark.read.format('csv')
        .options(**{
            'path': raw_immunizations__full_path
            , 'header': True
            , 'inferSchema': False
        })
        .schema(raw_immunizations__schema)
        .load()
        .dropDuplicates()
    )
    
    enriched_immunizations__df = raw_immunizations__df.withColumn(
        'HASH', F.concat_ws('|', *[F.col(column_name) for column_name in raw_immunizations__df.columns])
    )
    
    enriched_immunizations__is_existing = DeltaTable.isDeltaTable(spark, enriched_immunizations__full_path)
    if not enriched_immunizations__is_existing:
        enriched_immunizations__df.write.format('delta').mode('overwrite').option('path', enriched_immunizations__full_path).saveAsTable(enriched_immunizations__table_name)
    else:
        enriched_immunizations__delta_table = DeltaTable.forName(spark, enriched_immunizations__table_name)
        (
            enriched_immunizations__delta_table.alias('target').merge(
                source = enriched_immunizations__df.alias('source')
                , condition = (F.col('source.HASH') == F.col('target.HASH'))
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

if __name__ == '__main__':
    main()