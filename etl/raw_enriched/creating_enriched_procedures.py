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
    
    raw_procedures__file_name = 'procedures.csv'
    enriched_procedures__table_name = 'enriched_procedures'
    raw_procedures__full_path = f'{raw_path}/{raw_procedures__file_name}'
    enriched_procedures__full_path = f'{enriched_path}/{enriched_procedures__table_name}'
    
    raw_procedures__schema = T.StructType([
        T.StructField('START', T.TimestampType(), True)
        , T.StructField('STOP', T.TimestampType(), True)
        , T.StructField('PATIENT', T.StringType(), True)
        , T.StructField('ENCOUNTER', T.StringType(), True)
        , T.StructField('SYSTEM', T.StringType(), True)
        , T.StructField('CODE', T.LongType(), True)
        , T.StructField('DESCRIPTION', T.StringType(), True)
        , T.StructField('BASE_COST', T.DoubleType(), True)
        , T.StructField('REASONCODE', T.LongType(), True)
        , T.StructField('REASONDESCRIPTION', T.StringType(), True)
    ])
    
    raw_procedures__df = (
        spark.read.format('csv')
        .options(**{
            'path': raw_procedures__full_path
            , 'header': True
            , 'inferSchema': False
        })
        .schema(raw_procedures__schema)
        .load()
        .dropDuplicates()
    )
    
    enriched_procedures__df = raw_procedures__df.withColumn(
        'HASH', F.concat_ws('|', *[F.col(column_name) for column_name in raw_procedures__df.columns])
    )
    
    enriched_procedures__is_existing = DeltaTable.isDeltaTable(spark, enriched_procedures__full_path)
    if not enriched_procedures__is_existing:
        enriched_procedures__df.write.format('delta').mode('overwrite').option('path', enriched_procedures__full_path).saveAsTable(enriched_procedures__table_name)
    else:
        enriched_procedures__delta_table = DeltaTable.forName(spark, enriched_procedures__table_name)
        (
            enriched_procedures__delta_table.alias('target').merge(
                source = enriched_procedures__df.alias('source')
                , condition = (F.col('source.HASH') == F.col('target.HASH'))
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        
if __name__ == '__main__':
    main()